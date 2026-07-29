# VEMB V16 Aeron/UB 跨机模式阶段总结

更新时间：2026-07-28

## 1. 文档目的

本文记录当前 Aeron/UB 数据面的实现状态、最近一次有效跨机测试、已确认的问题和下一阶段的演进方向，作为后续继续开发和回归测试的基线。

本文描述的是当前工作树状态。相关代码尚未形成一个干净提交，工作树中还包含本轮功能开发、性能实验和脚本修改；后续提交前需要继续逐项审查这些改动。

## 2. 结论摘要

当前已经完成以下主线改造：

- TCP 和 Aeron 在 transport 层分开处理，后面的 proxy、SuperNode、TLC 和请求语义保持共用。
- Aeron 请求/响应数据面使用 UB-backed SPSC ring，不再依赖混杂 TCP I/O thread 的 `epoll_wait` 等待。
- Aeron 控制面支持 TCP attach，跨机 CLI 不需要 UDS；UDS 代码仍保留为兼容路径。
- 跨机 ATTACH 返回服务端 UB path，客户端将服务端 path 映射为本地 UB view 后再 mmap ring 和 warm region。
- 同机 UB 映射使用 `O_RDWR`；跨机 NC 映射当前必须使用 `O_RDWR|O_SYNC`，否则已复现 `mmap(...)=EPERM`。
- proxy 的 Aeron channel poll 使用 active channel snapshot；控制面更新 snapshot，poller 只读取原子快照，不在热路径加 mutex。
- 同一个 UB device 可以通过不同 offset 为多个 channel 分配 ring，因此 4 个 UB path 不限制为 4 个连接。

当前还没有解决性能差距：有效跨机测试约为 5.22--5.23M QPS，低于历史同机 Aeron 的约 48--59M QPS，也低于用户报告的 TCP 约 11M QPS。2026-07-29 的同参数对照已经确认，主要瓶颈是 client 侧跨机 warm-region handle 解引用和 1200B vector copy；server attach、ring 发布和 server worker 不是第一瓶颈。跨机 UB 元数据访问、重复 mmap/TLB 压力仍是后续需要继续拆分的次级成本。

## 3. 当前架构

### 3.1 传输和处理边界

请求处理链路保持一致：

```text
CLI
  -> transport-specific ingress
  -> proxy channel poll
  -> proxy batch / job ring
  -> SuperNode worker
  -> TLC/storage lookup and execution
  -> completion
  -> proxy response publish
  -> CLI
```

TCP 模式的 ingress 是 TCP socket；Aeron 模式的 ingress 是客户端 UB request ring。proxy 后面的请求解析、批处理、job/completion ring、SuperNode 执行和 response 语义共用，Aeron 读请求返回 handle，TCP 读请求返回 inline payload。

### 3.2 Aeron 控制面和数据面

跨机 Aeron 使用两条不同链路：

```text
TCP control/ATTACH:
  CLI -> server Redis TCP listener -> ATTACH response

UB data plane:
  CLI -> request ring -> proxy
  proxy -> response ring -> CLI
```

TCP ATTACH 只负责分配 channel、返回 channel id、ring offset、slot size 和 warm region 描述；真正的 req/resp 数据不经过 TCP。ATTACH 完成后 TCP 连接关闭，客户端直接访问本地 UB view。

当前 `aeron` runner 默认构造 `tcp://HOST:PORT` 控制端点。UDS listener 和 UDS alloc/close 代码仍存在，主要用于兼容旧调用方式；测试目标不依赖 UDS，也不要求 CLI 与 server 同机。

### 3.3 Channel 和 UB path 映射

一个 channel 包含：

- 一个 request ring；
- 一个 response ring；
- 一个 channel id 和 ring offset；
- 一个由 server ATTACH 返回的 warm region 描述。

ring 不按 channel 独占一个 device，而是在指定 UB path 的地址空间中按 offset 分配。因此：

```text
总 channel 数 = threads * clients
例：64 * 4 = 256 channels
4 个 UB path = 4 个可分配的 UB 地址空间/设备入口
每个 channel = 同一 path 上的一对不同 offset 的 req/resp ring
```

当前两节点 path 约定如下。服务端返回服务端视角的 path，客户端按固定实验拓扑将编号加 4：

| 逻辑资源 | server 视角 | client 本地 view |
|---|---|---|
| Aeron ring/warm 资源 1 | `/dev/obmm_shmdev1` | `/dev/obmm_shmdev5` |
| Aeron ring/warm 资源 2 | `/dev/obmm_shmdev2` | `/dev/obmm_shmdev6` |
| Aeron ring/warm 资源 3 | `/dev/obmm_shmdev3` | `/dev/obmm_shmdev7` |
| Aeron ring/warm 资源 4 | `/dev/obmm_shmdev4` | `/dev/obmm_shmdev8` |

当前映射逻辑是实验环境专用的 `1..4 -> 5..8` 规则。它不是通用的 locality 发现机制，后续跨节点部署应改为显式的 server-path 到 client-path 配置或由设备拓扑提供映射。

### 3.4 Ring 容量和 poll

客户端 ring 定义在 `src/vemb_v16_client_ring.h`：

- 最大 pipeline/batch 参数：128；
- ring slot 数：256，即 `128 * 2`；
- request slot 按 `dim * sizeof(float)` 加请求头计算；
- response slot 使用固定 `vemb_v16_resp_t` 大小；
- server proxy 单次 request poll batch 当前为 `PROXY_REQUEST_BATCH=32`。

空 ring 时 poll 返回 0，不阻塞在 Aeron ring 上。满 ring 或 response 暂时不可发布时使用 `cpu_relax()`/自旋退避；客户端 ring helper 还有 spin、ARM `yield`/x86 `pause` 和最终 1 微秒 `nanosleep` 的自适应退避。

proxy 的 Aeron channel 列表使用双 buffer active snapshot：

1. 控制面新增/关闭 channel 时复制当前 channel index 列表；
2. 原子交换 snapshot 指针；
3. poller 读取当前 snapshot，依次 poll 活跃 channel；
4. poller 不扫描固定的 `VEMB_V16_MAX_CHANNELS`，也不在每个 channel 上调用 epoll 等待。

proxy 与 SuperNode 的 worker 数可以分别配置。1:1 配置可以让 queue/shard 配对更直接，但当前仍需通过对比测试确认它能否降低调度和 poll 开销，不能仅凭线程数相等推断 QPS 会提升。

## 4. 已完成的代码范围

本轮主要涉及：

- `src/vemb_v16_proxy.c`、`src/vemb_v16_proxy_types.h`：Aeron active channel snapshot、channel ownership、proxy poll、job/completion queue 和线程配置；
- `src/vemb_v16_aeron_attach.c/.h`：TCP ATTACH、server-side ring path/offset 和 warm region 描述返回；
- `src/vemb_v16_aeron_transport.c/.h`：UB ring request poll 和 response publish；
- `src/vemb_v16_client_ring.h`：固定 slot ring、batch、容量和退避策略；
- `clients/c/vemb_v16_client_sdk.c/.h`：TCP control、UB ring mmap、server path 到 client path 映射、warm region mmap 和 handle 解引用；
- `src/vemb_v16_server.c`、`src/vemb_v16_server_integration.c`、`src/config.c`、`src/server.h`：transport/control/worker 参数接入；
- `memtier_benchmark/vemb_v16_aeron_runner.cpp/.h`、`memtier_benchmark/memtier_benchmark.cpp`、`benchmark/vemb_v16_bench.c`：Aeron runner、pipeline/channel 分配、handle read 开关和跨节点路径处理；
- `run_aeron_best.sh`：manifest 保留、UB path 收集、path 占用检查、server/client 角色和测试参数；
- `scripts/test_host_mt.md` 及相关架构文档：测试说明和架构说明同步。

本轮工作树还包含 `scripts/run_host_mt_server_flamegraph.sh` 等测试辅助脚本，提交前需要将功能改动与实验产物分开审查。

## 5. 最近一次有效跨机测试

### 5.1 测试配置
编译的时 redis-server，不是 vemb_server
- server：`192.168.90.112`；
- client：`192.168.90.111`；
- control：TCP；
- server req/resp UB path：`/dev/obmm_shmdev3`；
- server warm UB path：`/dev/obmm_shmdev4`；
- client req/resp UB path：`/dev/obmm_shmdev7`；
- client warm UB path：`/dev/obmm_shmdev8`；
- channel 数：`TS * CS = 64 * 4 = 256`；
- pipeline：32；
- `NUM_KEYS=100000`，`DIM=300`；
- proxy IO：21；
- SuperNode worker：21。

### 5.2 结果

```text
prefill: 214092.42 sets/sec
QPS:     5051887.06
p50:     0.103 ms
p99:     1.775 ms
hits:    5040657.95
misses:  11200.83
```

结果目录：

```text
/root/szz/codespace/hpc-redis/benchmark/results/aeron_sweep/20260728_214954/
```

日志确认了实际 path 映射：

```text
server_path=/dev/obmm_shmdev4 local_path=/dev/obmm_shmdev8
shmdev=/dev/obmm_shmdev3
```

该测试的 warm region 已被 server 广告并由 client 映射。runner 中的日志仍输出 `cross-node, no warm region`，这是状态文字错误，不代表 warm region 没有映射，后续应修正。

### 5.3 无效结果说明

此前约 `5.282M` 的一次结果不能作为对比基线：当时脚本覆盖了外部 `MANIFEST`，并意外使用了 `/dev/obmm_shmdev2` 作为 warm path。当前脚本已经改为尊重外部 `MANIFEST`，并在启动 server 前检查配置 UB path 是否被其他进程占用。

如果 `lsof` 发现相同 UB path 已被进程持有，脚本返回退出码 3，不能直接启动第二个使用相同 path 的 server，也不应杀掉无关进程。

### 5.4 2026-07-29 重跑及瓶颈对照

使用用户指定目录 `/root/szz/codespace/hpc-redis`，server 为 `192.168.90.112`，client 为
`192.168.90.111`。server transport 使用 `aeron`，client transport 使用
`aeron-cross-node`；server ring/warm path 分别为 `/dev/obmm_shmdev3` 和
`/dev/obmm_shmdev4`，client 映射为 `/dev/obmm_shmdev7` 和 `/dev/obmm_shmdev8`。
参数为 `TS=64`、`CS=4`、`PIPELINE=32`、`NUM_KEYS=100000`、`DIM=300`、
`PIO=21`、`SNW=21`、`TEST_TIME=10`。

| 组别 | QPS | p50 | p99 | 说明 |
|---|---:|---:|---:|---|
| handle read enabled | 5.234M | 0.127 ms | 1.783 ms | 第一次有效重跑 |
| handle read enabled | 5.222M | 0.095 ms | 0.511 ms | 第二次复跑 |
| `VEMB_AERON_SKIP_HANDLE_READ=1` | 12.747M | 0.151 ms | 0.679 ms | 只验证 response/handle metadata，不复制 vector |

两次 enabled 结果稳定在约 `5.23M QPS`；跳过 vector 解引用后提升到 `12.75M QPS`，约
`2.44x`。enabled 组的 256 个 channel 均完成 ATTACH，`publish_fail=0`，
`handle_deref` 全部成功，因此瓶颈不是 channel 建立、ring 满或 warm 映射失败。

每个成功 GET 都会在 client 侧执行 `vemb_v16_aeron_read_vector()`，从
`/dev/obmm_shmdev8` 的跨机 warm mapping 读取并复制 1200B。按约 `5.22M` hits/sec
计算，client 每秒需要处理约 `6.27 GB` 的 vector read/copy；这解释了 5M 级别的上限。
server 侧对照 perf 也没有出现与 QPS 差距相称的压力变化：enabled 采样约为
`464B cycles / 8.67B cache-misses / 8.29M context-switches`，skip 采样约为
`408B cycles / 8.94B cache-misses / 8.74M context-switches`（每组 15 秒采样窗口，
包含相邻 prefill，数值用于方向判断）。

结果目录：

```text
/root/szz/codespace/hpc-redis/benchmark/results/aeron_cross_20260729_full/20260729_102257/
/root/szz/codespace/hpc-redis/benchmark/results/aeron_cross_20260729_full2/20260729_102515/
/root/szz/codespace/hpc-redis/benchmark/results/aeron_cross_20260729_skip/20260729_102359/
```

## 6. 当前性能判断

已经排除的主要误因：

- 不是 pipeline 没有建立：测试使用 pipeline=32；
- 不是 channel 没有建立：256 个 channel 均完成 ATTACH；
- 不是命中率不足：有效测试大部分请求为 hit；
- 不是 CLI 没有执行 handle read：默认 `VEMB_AERON_SKIP_HANDLE_READ` 未开启时会调用 `vemb_v16_aeron_read_vector()`。

2026-07-29 对照后，当前成本来源按优先级排序：

1. client 每个命中请求都通过 warm-region handle 读取并复制 1200B vector；跨机 mapping 使用 `O_SYNC`，这是当前 5M 上限的主要来源；
2. 每个 channel 目前独立建立 warm-region mmap，即使这些 channel 指向同一 warm region，也可能造成重复虚拟映射、页表和 TLB 压力；
3. 跨机 client 对 req/resp ring 的 head/tail 和 slot metadata 进行 NC UB 访问，每次 poll 都可能产生较高的远端可见性成本；
4. 多 channel 轮询的访问顺序、batch 上限和 ring head/tail 发布频率仍可能放大元数据访问次数；
5. proxy/SuperNode 的跨 worker job/completion 通知仍有 eventfd 路径，之前火焰图中还观察到 `eventfd_read/write` 和 `__sched_yield` 开销。

`O_RDWR` 不能作为跨机优化直接替代 `O_RDWR|O_SYNC`：在当前 UB 环境下曾复现 `mmap(...)=EPERM`。同机 client/server 使用相同 path 时可去掉 `O_SYNC`，但跨机仍需保留，除非设备驱动和可见性语义得到新的验证。

## 7. 当前远端状态和操作注意事项

最近检查时，6398 测试 server 已退出；另有一个 6391 的 `redis-server` 进程仍在运行，并持有至少 `/dev/obmm_shmdev4`。启动新测试前先检查：

```bash
ssh root@192.168.90.112 \
  'lsof /dev/obmm_shmdev3 /dev/obmm_shmdev4'
```

只清理确认属于本轮测试的进程和 path。`run_aeron_best.sh` 的 UB ownership guard 是有意保留的保护逻辑。

## 8. 后续演进计划

### P0：先修正状态和建立对照组

- 修正 runner 的 cross-node warm-region 状态日志；
- 确认 server/client 两端 manifest、UB path owner 和端口状态；
- 在相同 `TS/CS/PIPELINE/NUM_KEYS` 下分别测试：实际 handle read、`VEMB_AERON_SKIP_HANDLE_READ=1`、同机 `O_RDWR`、跨机 `O_RDWR|O_SYNC`；
- 每组至少保存 client、server 日志和参数快照，避免再次混淆 manifest 或 warm path。

### P1：采集可分离的性能证据

- server-only perf/火焰图：区分 Aeron poll、proxy batch、job/completion、TLC stable-read 和 warm access；
- client-only perf/火焰图：区分 ring metadata、response poll、handle 解引用、SVE/vector copy 和退避；
- 同时包含用户态和内核态，重点确认 `eventfd_read/write`、`__sched_yield`、page fault、mmap/UB driver 路径占比；
- 记录每秒 QPS、CPU core、ops/core、p50/p99、hit/miss 和 ring full/empty 计数。

### P2：降低跨机 UB 元数据访问

- warm region 做按 path/region 的共享 mmap，channel 只保存 region id 到共享映射的引用；
- 在一次 poll 中尽量批量读取/发布，减少每个 request 的 head/tail 原子操作和远端 metadata 访问；
- 评估 active snapshot 的轮询粒度、每 channel 最大 drain 数和空 channel 跳过策略；
- 在保持 SPSC 约束的前提下，减少 eventfd 通知频率，优先使用 non-sleep poll/批量唤醒；
- 继续验证 proxy/SuperNode 1:1 worker pairing，比较其与 active snapshot 的收益，不能预先假定二者可互相替代。

### P3：去除实验拓扑硬编码

- 将 `server /dev/obmm_shmdev1..4 -> client /dev/obmm_shmdev5..8` 从 SDK 内置规则改为显式配置；
- ATTACH 返回 stable region identity、server path、offset 和 bytes；
- client 依据本地 path map 或拓扑配置完成映射，不在 warm-region manifest 中写死 client path；
- 对非 OBMM path、同机 path 和多节点 path 分别保留明确测试覆盖。

## 9. 复现命令模板

本机或 server 端启动前，先确认 path 没有被占用：

```bash
lsof /dev/obmm_shmdev3 /dev/obmm_shmdev4
```

跨机测试的基本参数形态：

```bash
SERVER_HOST=192.168.90.112 \
AERON_TRANSPORT=aeron-cross-node \
AERON_CONTROL=tcp \
MANIFEST=/path/to/server-manifest.yaml \
NUM_KEYS=100000 TEST_TIME=3 TS=64 CS=4 PS=32 \
PIO=21 SNW=21 \
bash run_aeron_best.sh
```

handle 解引用对照：

```bash
VEMB_AERON_SKIP_HANDLE_READ=1 \
SERVER_HOST=192.168.90.112 \
AERON_TRANSPORT=aeron-cross-node \
AERON_CONTROL=tcp \
NUM_KEYS=100000 TEST_TIME=3 TS=64 CS=4 PS=32 \
PIO=21 SNW=21 \
bash run_aeron_best.sh
```

## 10. 关联文件

- `run_aeron_best.sh`
- `clients/c/vemb_v16_client_sdk.c`
- `src/vemb_v16_aeron_attach.c`
- `src/vemb_v16_aeron_transport.c`
- `src/vemb_v16_proxy.c`
- `src/vemb_v16_client_ring.h`
- `memtier_benchmark/vemb_v16_aeron_runner.cpp`
- `scripts/test_host_mt.md`
- `docs/VEMB_V16_PHASE2_AERON_RING_DESIGN.md`
- `docs/VEMB_V16_VSIM_UB_CC_NC_VISIBILITY_REPRO.md`

## 11. 2026-07-29 batch/SVE 实现

针对上述瓶颈和 ring 轮询开销，已将 Aeron UB path 的 request/response 热路径改为 batch：

- server request ingress 使用 `poll_batch`，先复制到本地 request batch，再交给 proxy；server response batch 保持一次发布；
- memtier Aeron CLI 每个 channel 最多批量 publish 32 个可变长度 request，并批量 poll response；
- ring slot 的 publish/poll copy 在 server 侧调用 `src/sve_operation.c` 的 `sve_streaming_load_f32`，在 CLI 侧调用 `clients/c/vemb_v16_client_sdk.c` 的同名实现；共享 ring header 只提供调用适配，不重复实现 SVE intrinsic。

编译说明：跨机 memtier Aeron CLI 不需要手工设置 `USE_SVE=yes`。`clients/c/Makefile` 在 `aarch64` 上默认加入 `-DUSE_ARM_SVE -march=armv8.2-a+sve`；`USE_SVE=yes` 仍适用于 server 的 `src` target 或 standalone SVE benchmark/UT。已验证 `make -C clients/c static install-headers`、`make -C memtier_benchmark` 和 `make -C src vemb_v16_server` 通过。

## 12. 2026-07-29 Redis target batch/SVE 复测

本轮按 Redis 正式 target 编译和启动，未使用独立的 `vemb_v16_server`：

```text
make -B -C src redis-server USE_UB=yes \
  PROXY_REQUEST_BATCH=32 PROXY_RESPONSE_BATCH=32 PROXY_QUEUE_BATCH=32 \
  VEMB_V16_PROXY_AFFINITY_MODE=0
```

启动测试前，两台机器的目标 UB 设备均通过权限和空闲检查，并使用 `O_RDWR|O_SYNC` 对设备做一页 mmap/读取探针：server `/dev/obmm_shmdev3`、`/dev/obmm_shmdev4`，client `/dev/obmm_shmdev7`、`/dev/obmm_shmdev8` 全部成功。测试结束后 Redis 已停止，目标 path 无残留 holder。

参数为 `TS=64`、`CS=4`、`PIPELINE=32`、`NUM_KEYS=100000`、`DIM=300`、`TEST_TIME=10`，server 使用 TCP control `192.168.90.112:6395`，ring/warm path 为 `/dev/obmm_shmdev3`、`/dev/obmm_shmdev4`，client 使用 `aeron-cross-node`。

```text
QPS:  6.646851M
p50:  0.543 ms
p99:  1.271 ms
```

结果目录：

```text
/root/szz/codespace/hpc-redis/benchmark/results/aeron_cross_20260729_batch/20260729_111550/
```

相对前一轮 handle-read enabled 的 `5.222M QPS`，本轮提升约 `1.273x`（`+27.3%`）。client 日志中 `publish_fail=0`，每个 worker 的 `publish_ok` 与 `poll_got` 一致，`handle_deref` 全部成功，说明 batch ring 流程没有丢请求或 warm read 失败。

容量调整：`MAX_VECTORS=131072` 仍足够覆盖 `NUM_KEYS=100000`；实际使用的
`tmp_aeron_path4_manifest.yaml` 已将 warm region 从 1G 调整为 4G
（`bytes: 4294967296`），供下一轮启动使用。上面的 `6.646851M QPS` 仍对应调整前的 1G 测试。
