# VEMB V16 Aeron/UB 跨机模式阶段总结

更新时间：2026-07-28

## 1. 文档目的

本文记录当前 Aeron/UB 数据面的实现状态、最近一次有效跨机测试、已确认的问题和下一阶段的演进方向，作为后续继续开发和回归测试的基线。

本文描述的是当前已提交状态。基线改造提交为 `a84d4ff`，compact frame 和双 UB path 改造提交为 `c6ea39c`；远端实验结果目录位于两台机器的 `/root/szz/codespace/hpc-redis` 下。

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

## 13. 2026-07-29 compact frame / 双 UB path 复测

为进一步降低 CLI 与 proxy 之间的跨机数据量，本轮将 Aeron ring 中的 request/response
从内部完整结构改为按 op/status 编码的 compact frame：GET request 只携带 key 和必要
字段，response 只携带 status、request id 以及 handle metadata，不在 req/resp ring 中
携带 1200B vector payload。server 和 CLI 两侧都保留各自的
`sve_streaming_load_f32` 实现，ring payload copy 统一通过该函数；编译跨机 CLI 时不需
额外设置 `USE_SVE=yes`，clients/c 的 AArch64 默认编译选项已启用 SVE。`USE_SVE=yes`
仍可用于 server 或 standalone SVE benchmark/UT 的显式构建。

ring layout 采用 64B cacheline：ring header、slot metadata、payload 起始位置和 slot
stride 均按 64B 对齐；每个 slot 的 metadata 独立保存实际 frame 长度，避免固定大 payload
复制。request/response 使用独立 UB path，避免双向流量争用同一地址空间：

| 角色 | request path | response path | warm path |
|---|---|---|---|
| server `redis-server` | `/dev/obmm_shmdev3` | `/dev/obmm_shmdev2` | `/dev/obmm_shmdev4` |
| client / CLI | `/dev/obmm_shmdev7` | `/dev/obmm_shmdev6` | `/dev/obmm_shmdev8` |

server 使用 `redis-server` target，关键参数为 `transport=aeron`、TCP control、
`proxy_io_threads=21`、`supernode_workers=21`、`PROXY_REQUEST_BATCH=32` 和
`PROXY_RESPONSE_BATCH=32`；CLI 使用 `aeron-cross-node`、`TS=64`、`CS=4`、
`PIPELINE=32`。`NUM_KEYS=100000`、`DIM=300`，warm region manifest 使用 4G，
`MAX_VECTORS=131072` 足够覆盖 key 数量。

结果：

```text
QPS:  9.96317788M
avg:  0.38016 ms
p50:  0.37500 ms
p99:  0.82300 ms
```

结果目录：

```text
/root/szz/codespace/hpc-redis/benchmark/results/aeron_cross_20260729_slim_dualpath/20260729_133321/
```

相对上一轮 Redis batch/SVE 的 `6.646851M QPS`，本轮提升约 `49.9%`。结合前面的
`VEMB_AERON_SKIP_HANDLE_READ=1` 对照（`12.747M QPS`），当前剩余瓶颈仍是 client 从
跨机 warm mapping 解引用并复制 1200B vector；compact req/resp 和双 path 已明显降低
CLI/proxy 控制流成本，但没有消除 warm-region 的远端读取与 copy 成本。后续优先级仍是
共享 warm mmap、降低每请求 head/tail metadata 访问，以及继续拆分 client 侧 ring poll、
handle dereference 和 SVE copy 的 CPU 采样。

测试结束后的清理检查确认 server 的 `/dev/obmm_shmdev2`、`/dev/obmm_shmdev3`、
`/dev/obmm_shmdev4` 和 client 的 `/dev/obmm_shmdev6`、`/dev/obmm_shmdev7`、
`/dev/obmm_shmdev8` 均无残留 holder；远端日志中的 ATTACH 控制帧正常处理，未发现
protocol error。

## 14. 2026-07-29 warm path 反向对照：server 8 -> client 4

按设备拓扑做反向 warm-region 对照：server manifest 使用
`/dev/obmm_shmdev8`，CLI 显式设置
`VEMB_V16_AERON_CLIENT_WARM_UB_PATH=/dev/obmm_shmdev4`；request/response ring
仍为 server `/dev/obmm_shmdev3`、`/dev/obmm_shmdev2` 对应 client
`/dev/obmm_shmdev7`、`/dev/obmm_shmdev6`。参数保持与上一轮一致：
`TS=64`、`CS=4`、`PIPELINE=32`、`NUM_KEYS=100000`、`DIM=300`、
`TEST_TIME=10`，warm region 为 4G，`MAX_VECTORS=131072`。

这组方向暴露了 path-specific 的 mmap flags 要求：server `/dev8` 映射约 229MB
warm layout 时，纯 `O_RDWR` 返回 `EPERM`，`O_RDWR|O_SYNC` 才能成功；client
`/dev4` 按要求使用纯 `O_RDWR`。因此新增了按 UB device view 选择 flags 的逻辑：
1-4 不加 `O_SYNC`，5-8 保留 `O_SYNC`。显式 warm client path 和该 flags 修正分别
记录在提交 `38c6f52`、`023fcd7`、`23e7988`。

最终测试成功，256 个 channel 完成 ATTACH，`publish_fail=0`，所有
`handle_deref` 成功：

```text
QPS:  7.335260M
avg:  1.05687 ms
p50:  1.04700 ms
p99:  1.51100 ms
```

结果目录：

```text
/root/szz/codespace/hpc-redis/benchmark/results/aeron_cross_20260729_warm8to4/20260729_135602/
```

相同 request/response ring 配置下，server warm4 -> client warm8 为 `9.963178M QPS`，
反向 server warm8 -> client warm4 为 `7.335260M QPS`，下降约 `26.4%`。因此当前
设备方向不是等价交换；反向方向的 warm 读取路径明显更慢，瓶颈仍集中在 client
warm-region handle 解引用和 1200B vector copy，而不是 ATTACH 或 ring 丢包。

## 15. 2026-07-29 c6 模式 cycles 火焰图

恢复到 c6 模式后，以 `TS=64`、`CS=4`、`PIPELINE=32`、`NUM_KEYS=100000`、
`DIM=300`、`MAX_VECTORS=131072`、`TEST_TIME=60` 重跑。最终结果为：

```text
QPS:  9.912978M
avg:  0.38705 ms
p50:  0.38300 ms
p99:  0.83100 ms
```

结果目录：

```text
/root/szz/codespace/hpc-redis/benchmark/results/aeron_cross_20260729_flamegraph/20260729_141302/
```

server 和 CLI 均使用 `perf record -F 99 -e cycles -g`，即同时包含用户态和内核态：

- server：[redis_server_1451597_20260729_141338.svg](/Users/szza/codespace/work/hpc-redis/perf/aeron_cross_20260729_flamegraph/server_cycles/redis_server_1451597_20260729_141338.svg)，采集约 95,112 samples；
- CLI：[memtier_3223595_20260729_141359.svg](/Users/szza/codespace/work/hpc-redis/perf/aeron_cross_20260729_flamegraph/client_cycles/memtier_3223595_20260729_141359.svg)，采集约 22,303 samples。

热点判断：

1. CLI 最大栈是 `account_response -> vemb_v16_aeron_read_vector -> sve_streaming_load_f32`，约占 CLI 加权栈样本的 44.8%；其次是 request batch publish、response batch poll 以及两侧 ring copy 的 `sve_streaming_load_f32`。
2. server 主要消耗在 `proxy_io_pool_thread_main`、`vemb_v16_aeron_poll_shm_requests`、`drain_completions`，以及 `supernode_pool_thread_main -> drain_shard_queues -> vemb_v16_supernode_handle_vemb_job -> vemb_v16_tlc_get_handle_stable_read -> tlc_core_get_warm_location_raw`。
3. 内核态栈没有出现占主导的 UB mmap 或 page fault 路径；可见内核热点主要是 `nanosleep`/`schedule`，CLI 另有少量 `munmap`。因此当前 9M 模式的第一瓶颈仍是 CLI warm-region handle 解引用后的 1200B vector copy，server 第二瓶颈是 proxy/SuperNode 队列调度和 warm stable read。

CLI 采样启动较晚，目标进程结束后只留下约 22,303 个样本，因此两张图的样本量不能直接比较；但 CLI 的主热点栈与此前 skip-handle-read 对照一致，结论方向稳定。

## 16. Aeron SDK 与 proxy/SuperNode 的轮询和唤醒策略

本节固定目前两个场景的策略结论，避免将 `clients/c` SDK 的非阻塞 API、跨节点
UB ring 和 proxy 进程内 queue 混用讨论。

### 16.1 `clients/c SDK <-> proxy`：保留 ring polling

`clients/c` 的 Aeron API 是非阻塞接口：

```text
vemb_v16_aeron_publish_request()
vemb_v16_aeron_publish_request_batch()
vemb_v16_aeron_poll_response()
vemb_v16_aeron_poll_response_batch()
```

SDK 的 `vemb_v16_aeron_open_remote()` 只通过 TCP 完成 ATTACH 握手，随后关闭 TCP，
request/response 数据通过映射的 UB ring 传输。因此跨节点数据面没有可由远端 SDK
直接触发的本机 futex 或 eventfd：

1. futex 需要本机内核能够观察到共享等待字的变化；远端 UB/PCIe 写入不能可靠地
   作为本机 futex 唤醒事件。
2. eventfd 需要持有本机 fd 的线程执行 `eventfd_write()`；远端 SDK 不能直接写入
   proxy 的 eventfd。
3. 在每次 request publish 时增加 sideband eventfd 或 socket 通知，会把零系统调用
   数据面重新变成系统调用路径，通常不适合高吞吐 pipeline。

因此当前结论是：

```text
SDK <-> proxy 数据面 = SPSC ring + batch publish/poll
跨节点唤醒          = 不依赖 futex/eventfd
低负载等待          = 由 SDK 调用方选择 backoff 或超时策略
```

如果需要阻塞式使用方式，应新增可选的 `wait_response()` 辅助接口，不能改变现有
non-blocking API 的语义。推荐实现为按时间预算的自适应等待：

```text
收到 response        -> 立即返回，并重置 idle 状态
空轮询 0~5 us        -> 纯 spin/cpu_relax
超过 spin budget     -> nanosleep 1 us
持续为空             -> 1, 2, 4, ... us 指数退避，设置最大 sleep
达到调用方 deadline  -> 返回 timeout
```

时间预算应使用 monotonic clock，而不是固定空轮询次数。SDK 的 batch poll 应优先于
单条 poll；没有 outstanding request 时不应进入等待。跨节点场景还应控制空轮询时对
远端 `head/tail` metadata 的读取频率，因为该读取成本可能高于本地 `cpu_relax()`。

### 16.2 `proxy <-> SuperNode`：进程内 queue 使用 hybrid wait

proxy 到 SuperNode 的 job shard queue 位于同一进程的 heap 内存，每个
`proxy worker -> SuperNode worker` 队列是独立 SPSC ring。当前 Linux 实现为每个
SuperNode worker 配置一个 eventfd，并通过 `job_notify_armed` 合并通知：只有 worker
处于可唤醒状态时，producer 才执行 `eventfd_write()`。

这个 eventfd 方案语义正确，且高负载时不会为每个 job 触发系统调用。它的主要成本
发生在队列短暂为空、worker 进入睡眠以及下一批任务到达的边界。因此不应预期仅把
eventfd 替换成 futex 就能显著提高持续高负载吞吐。

推荐的策略顺序是：

```text
drain batch
-> 空队列时 spin 2~10 us
-> 仍为空才阻塞等待
-> 有任务后立即恢复 polling
```

阻塞原语的选择：

| 场景 | 推荐 |
|---|---|
| 当前实现、需要快速落地 | 保留 eventfd |
| 同进程线程间、追求更低等待开销 | `FUTEX_WAIT_PRIVATE` / `FUTEX_WAKE_PRIVATE` |
| 需要接入 epoll 或统一 fd 事件循环 | eventfd |
| 跨节点共享 UB ring | 不使用普通 futex/eventfd 做数据到达通知 |

如果后续切换到 futex，建议使用每个 SuperNode worker 一个 `notify_seq`，producer
在空转 worker 可能睡眠时递增 sequence，worker 在“设置等待状态、重新检查所有
shard queue、再 `FUTEX_WAIT_PRIVATE`”的顺序中避免 lost wakeup。当前
`job_notify_armed` 的二次检查语义必须保留。

### 16.3 验证指标

后续比较 eventfd、futex 和不同时间预算时，至少同时记录：

```text
QPS / throughput
queue latency p50/p99/p999
proxy_io empty poll、cpu_relax、nanosleep
SuperNode empty poll、eventfd/futex wait 次数
CPU cycles、context switches、system time
```

判断标准是：持续高负载优先看 throughput 和 tail latency；稀疏或 burst workload
优先看 CPU 占用、唤醒延迟和 p99。当前火焰图已经显示 `nanosleep`/`schedule` 是
可见内核热点，因此下一步应先验证时间预算和 hybrid wait，再决定是否将进程内
eventfd 替换成 futex。

## 17. 降低 SDK 与 proxy 之间 Aeron ring metadata 读取

当前 request/response ring 的 `head` 和 `tail` 位于共享 UB/SHM mapping 中。每次
poll 或 publish 都需要读取对端 cursor；跨节点场景下，这些读取可能比本地原子操作
更昂贵。当前 batch API 已经做到每个 batch 读取一次对端 cursor，但每次调用仍会
重新加载共享字段。

### 17.1 进程本地 cursor

共享 ring layout 不增加进程私有字段。SDK 和 proxy 分别在自己的 channel/worker
对象中保存 local cursor：

```text
local_head       consumer 自己维护
local_tail       producer 自己维护
cached_tail      consumer 最近一次观察到的 producer tail
cached_head      producer 最近一次观察到的 consumer head
```

consumer 只有在本地已知数据耗尽时才重新加载共享 `tail`：

```text
local_head < cached_tail -> 继续消费，不读取共享 tail
local_head == cached_tail -> acquire-load shared tail
```

producer 只有在剩余空间不足以容纳下一批时才重新加载共享 `head`：

```text
remaining >= batch_size -> 使用 cached_head
remaining < batch_size  -> acquire-load shared head
```

旧的对端 cursor 只能造成保守等待，不能导致覆盖未消费数据或读取未发布数据；
因此该方案可以在不改变 ring 内存布局和跨节点协议的情况下实现。

batch 结束后再发布本端 cursor：

```text
consume batch -> 一次 release-store shared head
publish batch -> 一次 release-store shared tail
```

### 17.2 HOT/WARM/COLD channel poll 调度

local cursor 只能减少有数据时的重复读取，不能解决长期空闲 channel 每轮读取远端
`tail` 的问题。proxy Aeron worker 应对 channel 做分级调度：

```text
HOT   上次 poll 有数据，下一轮立即 poll
WARM  最近有数据，间隔 1~5 us poll
COLD  长期为空，间隔 10~100 us poll
```

状态转换规则：

```text
poll 到数据 -> HOT
HOT 连续为空 -> WARM
WARM 连续为空 -> COLD
COLD poll 到数据 -> HOT
```

worker 每轮只读取一次 monotonic time，并根据 channel 的 `next_poll_ns` 判断是否
到期，不能对每个 channel 单独调用时钟。COLD channel 的首包检测延迟最多增加一个
poll interval；高负载 channel 应在收到数据后立即恢复 HOT 状态。

同时避免以下重复读取模式：

```text
available() -> poll_batch()
```

应直接调用 `poll_batch()`，因为 `available()` 和 `poll_batch()` 可能各自读取一次
共享 `head/tail`。

### 17.3 收益预期

以下是设计阶段的工程估算，不替代实测：

| 工作负载 | 预期结果 |
|---|---|
| 高负载、`PIPELINE=32`、大多数 channel 活跃 | 总 QPS 约提升 `0~5%`，transport CPU 约下降 `5~15%` |
| burst 流量、ring 经常积累多个 batch | QPS 约提升 `5~15%`，P99 可能改善 |
| 256 channel、但大部分长期空闲 | 远端 metadata 读取约减少 `10~100x`，poll worker 空闲 CPU 约下降 `50~95%` |
| `PIPELINE=1` | local cursor 收益有限，batch 化收益更重要 |
| 远端 UB metadata 读取延迟很高 | 收益可能高于上述范围，需要结合设备实测 |

当前 9M 模式的首要瓶颈仍是 client warm-region handle 解引用和 1200B vector copy，
因此不应预期仅通过减少 ring metadata load 获得倍数级吞吐提升。该设计的主要目标
是降低 transport CPU、system time 和空闲 channel 的无效远端访问。

### 17.4 A/B 验证

实现前后至少记录以下计数器：

```text
request_shared_tail_loads
request_shared_head_loads
response_shared_tail_loads
response_shared_head_loads
batch_count
empty_poll_count
hot/warm/cold poll count
```

测试矩阵：

```text
PIPELINE=1/32/128
CHANNELS=1/16/256
持续满载、burst、长期空闲
baseline
baseline + local cursor
baseline + HOT/WARM/COLD
local cursor + HOT/WARM/COLD
```

比较 QPS、queue latency p50/p99/p999、CPU cycles、context switches 和 system time。
如果 `shared_*_loads / request_count` 已接近 `1 / batch_size`，local cursor 对吞吐
的影响会很小；如果空闲扫描占比高，应优先观察 HOT/WARM/COLD 调度的 CPU 和远端
metadata 读取下降幅度。

### 17.5 当前落地状态

本轮已先落地第 17 节的核心路径：

- `src/vemb_v16_client_ring.h` 增加进程私有 producer/consumer cursor；共享 ring
  layout 和旧 API 保持不变。server proxy 的 request ring 使用 consumer cursor，
  response ring 使用 producer cursor。
- `clients/c/vemb_v16_client_sdk.c` 和 `benchmark/vemb_v16_bench.c` 分别保存
  request producer、response consumer cursor，跨机 CLI 的实际压测路径也已接入。
- Aeron proxy IO worker 对 channel 保存本地 HOT/WARM/COLD 状态：首次空 poll 进入
  WARM，连续 4 次空 poll 后进入 COLD；WARM 间隔为 1 us，COLD 间隔为 10 us，poll
  到 request 后立即恢复 HOT。每轮只读取一次 monotonic time。
- completion drain 没有降频，仍在每轮执行；因此该优化只改变 request ring 的空闲
  poll 频率，不改变 response 完成处理和请求语义。

本轮已验证 `make -C src redis-server USE_UB=yes`、`make -C benchmark
vemb_v16_bench vemb_v16_aeron_ring_ut` 和 `make -C clients/c` 通过。shared cursor
load 的细分计数和 HOT/WARM/COLD 分状态计数仍待下一轮压测补齐；当前代码中的 worker
idle、batch 和 item counters 可先用于确认 proxy CPU 是否下降。

## 18. 2026-07-29 跨机 batch/perf 复测

### 18.1 配置与同步校验

本轮使用 `RUN_ID=20260729_effect_flame5`，server 为 `192.168.90.111`，client
为 `192.168.90.112`，两端工作目录均为 `/root/szz/codespace/hpc-redis`。测试前
对实际分配的六个 UB path 均执行了 `O_RDWR|O_SYNC` 的 4 KiB mmap 校验，全部通过：

```text
server request/response/warm: /dev/obmm_shmdev3 /dev/obmm_shmdev2 /dev/obmm_shmdev4
client request/response/warm: /dev/obmm_shmdev7 /dev/obmm_shmdev6 /dev/obmm_shmdev8
```

本地、111、112 三方的协议、proxy、server、stats、benchmark 和 client SDK 源码
SHA-256 完全一致。111 强制编译的是 `redis-server`；112 的 client 编译命令包含
`-DUSE_ARM_SVE -march=armv8.2-a+sve -DVEMB_V16_CLIENT_RING_USE_SVE`。

运行参数为 `DIM=300`、`MAX_VECTORS=131072`、warm region 4 GiB、`PIO=21`、
`SNW=21`、`t=64`、`c=4`、`PIPELINE=32`、`NUM_KEYS=100000`，GET-only workload
持续 35 秒。

### 18.2 吞吐与 batch 效果

client 结果为：

```text
QPS       9,980,173.97
p50       0.383 ms
p99       0.743 ms
p99.9     0.823 ms
GET       349,306,089 左右，全部命中，handle_deref fail=0
```

server 负载快照相对 prefill baseline 的增量为：

```text
proxy_io_requests             349,339,712
proxy_io_request_batch_count   10,916,866
supernode_job_count           349,339,712
supernode_job_batch_count      10,916,866
```

因此 request/job 平均为 `32.0 items/batch`，客户端读写 batch 已经生效；本轮
主要瓶颈不是单条协议处理，而是 batch 内每个 vector 的读取和解引用成本。

proxy worker 的增量空转计数为 `782,961,289`，其中 `cpu_relax=703,678,765`、
`nanosleep=79,282,524`，约 89.9% 是 relax、10.1% 进入 nanosleep。supernode
的 `empty_poll=eventfd_wait=8,643,812`，说明 supernode 没有继续用 Aeron 空转，
主要处于 eventfd wait；server 侧 CPU 浪费主要来自 21 个 proxy IO worker 对
256 个 channel 的空闲扫描。

### 18.3 用户态+内核态火焰图

两端均使用 `perf record -F 99 -e cycles -g`，没有使用 `cycles:u`，也没有使用
系统范围 `-a`。server 捕获约 70,322 samples，client 捕获约 220,784 samples：

- [server SVG](../perf/aeron_cross_20260729_flame_stats/20260729_effect_flame5/flame/server.svg)
- [client SVG](../perf/aeron_cross_20260729_flame_stats/20260729_effect_flame5/flame/client.svg)
- [server collapsed](../perf/aeron_cross_20260729_flame_stats/20260729_effect_flame5/flame/server.collapsed.txt)
- [client collapsed](../perf/aeron_cross_20260729_flame_stats/20260729_effect_flame5/flame/client.collapsed.txt)

client 火焰图的主要栈为：

```text
worker_main -> account_response -> vemb_v16_aeron_read_vector -> sve_streaming_load_f32
```

按 collapsed 栈权重，`sve_streaming_load_f32` 约 63.3%、`account_response` 约 56.3%、
`vemb_v16_aeron_read_vector` 约 45.4%；response batch poll 和 request batch publish
各约 19.2%。这些比例有父子栈重叠，不能相加，但热点排序清楚地表明 CLI 瓶颈仍是
warm-region vector copy/read，而不是 key 生成或 Redis 命令解析。

server 火焰图的主要 inclusive 栈为 `proxy_io_pool_thread_main` 约 82.3%、
`supernode_pool_thread_main` 约 16.7%；有效工作链中 `drain_shard_queues` 约 14.1%、
`vemb_v16_aeron_poll_shm_requests` 约 12.8%、`vemb_v16_supernode_handle_vemb_job`
约 11.9%、`vemb_v16_tlc_get_handle_stable_read` 约 11.2%、
`tlc_core_get_warm_location_raw` 约 10.8%。这解释了 server 之前“多数 CPU 没在做任务”
的现象：大量时间在 proxy IO worker 的轮询主循环，supernode 侧已经通过 eventfd
wait 降低了空转。

### 18.4 采样缺口与结论

本轮火焰图有效。server thread/core sampler 的第一次启动因远端 `scp` 不保留
`sample_server_threads.sh` 的执行权限而失败；随后改用 `bash` 启动时 server 已
退出，未得到可用于本轮 workload 的 core 汇总，不能据此填写 CPU core 分布。server
退出日志没有正常 shutdown、OOM 或 kernel crash 记录；这属于实验收尾诊断缺口，
不影响已经完成的 35 秒 client 结果和 perf 数据。

当前可执行的优化优先级是：

1. CLI 继续优化 `account_response`/`vemb_v16_aeron_read_vector` 的 1200-byte
   vector 读取路径，评估按 batch 复用 handle、减少 payload copy 和预取距离。
2. server 优先优化 256 channel 的 proxy IO 空闲扫描，使用 local cursor 以及
   HOT/WARM/COLD poll 调度；supernode 的 eventfd wait 已经是相对合理的 idle path。
3. 保持 request/job `32 items/batch`，再对比 `PIPELINE=1/32/128`，避免把 batch
   已经解决的 cursor/publish 成本误判为主瓶颈。

## 19. 2026-07-29 第 17 节优化复测结论

本轮重新同步到两端正确目录：server 源码位于 `src/`，SDK 位于 `clients/c/`，并在
111 上使用 `redis-server` target 编译。server manifest 更新为 4 GiB
`/dev/obmm_shmdev4`；启动前以 `O_RDWR|O_SYNC` 的 4 KiB mmap 校验六个 UB path，
均可用。

干净启动 server 后，以 `DIM=300`、`NUM_KEYS=100000`、`MAX_VECTORS=131072`、
`PIO=21`、`SNW=21`、`t=64`、`c=4`、`PIPELINE=32` 进行 35 秒 GET-only 测试，得到：

```text
QPS       9,896,383.74
p50       0.375 ms
p99       0.743 ms
p99.9     0.839 ms
handle_deref fail=0
```

相对第 13 节可比的约 `9.963M QPS` 为 `-0.67%`，相对第 18 节的 `9.980M QPS`
为 `-0.84%`。这不足以证明进程本地 cursor 与 HOT/WARM/COLD 调度提升了满载吞吐；
当前只能判定其没有造成明显回归，收益若存在则低于本次压测噪声和主要瓶颈的影响。

server 相对 prefill 的计数增量为：

```text
proxy_io_requests             346,341,248
proxy_io_request_batch_count   10,823,164
supernode_jobs                346,341,248
supernode_job_batch_count      10,823,164
```

request/job 仍为 `32.0 items/batch`。proxy 的空轮询为 `752,240,804`，其中
`cpu_relax=715,906,004`、`nanosleep=36,334,800`；第 17 节尚未增加
shared-cursor load 与 HOT/WARM/COLD 分状态计数，因此本轮不能量化 metadata load
实际减少量。

server 在首轮 workload 中使用 `perf record -F 99 -e cycles -g` 成功获得 76,025
samples，包含用户态和内核态：

- [server SVG](../perf/aeron_cross_20260729_flame_stats/20260729_effect_flame4/server.svg)
- [server collapsed](../perf/aeron_cross_20260729_flame_stats/20260729_effect_flame4/server.collapsed.txt)

为补齐 client 火焰图而复用了仍在运行的 server，第二次 ATTACH 分配了新的 channel，
旧 channel 尚未回收，QPS 降为 `3.451M`。该结果不进入 A/B 对比，只用于保留热点
样本：

- [client SVG](../perf/aeron_cross_20260729_flame_stats/20260729_effect_flame4/client.svg)
- [client collapsed](../perf/aeron_cross_20260729_flame_stats/20260729_effect_flame4/client.collapsed.txt)

因此下一步应先补齐第 17.4 节的分项计数并做 fresh-server A/B，而不是把复用 server
后的第二次吞吐视作优化退化。当前优先级仍是 CLI warm-region vector copy/read，以及
server 对失活 Aeron channel 的回收与扫描集合收缩。

### 19.1 回退决定

2026-07-30 已回退第 17 节的进程本地 cursor 和 proxy HOT/WARM/COLD 调度：满载
`PIPELINE=32` 未得到可测吞吐收益，且当时没有 shared cursor load 或 poll-state
分项计数能够证明其收益。ring publish/poll 恢复为直接读取共享 head/tail，proxy
恢复为每轮轮询全部 active Aeron channel。batch、SVE 和协议瘦身保留；worker 统计
计数已在同日删除。线程名和 SuperNode eventfd 操作保持不变。
