# VEMB V16 Aeron/UB 跨机模式阶段总结

更新时间：2026-07-28

## 0. 提交定位索引

下表中的“代码/实验基线”是对应设计或测试运行时最近可定位的源码提交；“文档提交”是该段内容写入本文的提交。章节中若明确写有“工作区未提交”，说明测试还包含该提交之后的本地改动，不能仅凭基线 hash 复现，必须同时保留当时的工作区 diff 和结果目录。

**后续测试记录规范：** 所有新增的机器测试结果表，无论是性能、正确性、火焰图还是 UT，均须逐行包含
`测试代码提交` 和 `工作区状态` 两列。`测试代码提交` 填写运行时的 commit hash；存在未提交改动时，
`工作区状态` 必须填写 `基线 <hash> + 未提交 diff`，并在结果目录归档对应 diff。即使整张表使用同一个提交，
也不得仅在标题或上级章节中间接说明。

| 内容范围 | 代码/实验基线 | 文档提交 | 定位说明 |
|---|---|---|---|
| 第 1--12 节：跨机基线、架构、batch/SVE 与 Redis target 复测 | `a84d4ffcf89173421c67a783a26df27a1514b6fb` | `a84d4ffcf89173421c67a783a26df27a1514b6fb` | Aeron/UB 跨机基线及 batch/SVE 实现。 |
| 第 13--15 节：compact frame、双 UB path、warm path 反向对照与 cycles 火焰图 | `c6ea39c19dfe5650ba812776b64123ef7feee751` | `9639bfe16f84aaf1da4812e8760de1b62cdea938` | 代码基线为 compact frame/双 path；测试与分析记录在 futex 提交中写入。 |
| 第 16--19 节：轮询/唤醒设计、ring metadata 设计及 batch/perf 对照 | `c6ea39c19dfe5650ba812776b64123ef7feee751` + `9639bfe16f84aaf1da4812e8760de1b62cdea938` | `9639bfe16f84aaf1da4812e8760de1b62cdea938` | 第 17 节的收益表是设计估算；第 18--19 节为同一代码演进链上的实测。 |
| 第 20--21.3 节：CLI/Proxy 两级缓存、单帧 batch_request 设计及最小闭环 | `6249f284be128f87a74b98c1ebed69a4fa302c16` | `6249f284be128f87a74b98c1ebed69a4fa302c16` | v2 batch 数据面设计、ATTACH 修复和最小闭环。 |
| 第 21.3.4--21.14 节：CLI L0/shared-L0 checkpoint 及跨机 baseline 数据 | `6249f284be128f87a74b98c1ebed69a4fa302c16` | `6249f284be128f87a74b98c1ebed69a4fa302c16` | 各完成记录已标注“工作区未提交”；该 hash 是最近提交的源码基线，不是这些实验的完整源码快照。 |
| 第 21.15--21.18 节：火焰图 runner、load gate、ready bitmap 实验及 lane snapshot | `b98ffaec9ce04fd441ce6e4b5623e0cdde29d4b8` | `b98ffaec9ce04fd441ce6e4b5623e0cdde29d4b8` | lane snapshot 优化已提交；ready bitmap 等实验仍按正文标注的工作区状态定位。 |
| 第 21.19--21.20 节：response frame 可见性复现、cacheline 隔离尝试及 L1 规划 | `b98ffaec9ce04fd441ce6e4b5623e0cdde29d4b8`（基线） | 工作区未提交 | 本节当前新增内容及对应实验代码尚未形成 commit；复现必须结合当前工作区 diff、实验目录和该基线。 |

短 hash 可直接用于 `git show`；表中的完整 hash 用于避免后续对象缩写碰撞。结果目录中的原始日志、perf 数据和 manifest 是测试数据本身的补充定位信息。

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

测试代码提交：`a84d4ff`。

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

构建验证代码提交：`a84d4ff`。

针对上述瓶颈和 ring 轮询开销，已将 Aeron UB path 的 request/response 热路径改为 batch：

- server request ingress 使用 `poll_batch`，先复制到本地 request batch，再交给 proxy；server response batch 保持一次发布；
- memtier Aeron CLI 每个 channel 最多批量 publish 32 个可变长度 request，并批量 poll response；
- ring slot 的 publish/poll copy 在 server 侧调用 `src/sve_operation.c` 的 `sve_streaming_load_f32`，在 CLI 侧调用 `clients/c/vemb_v16_client_sdk.c` 的同名实现；共享 ring header 只提供调用适配，不重复实现 SVE intrinsic。

编译说明：跨机 memtier Aeron CLI 不需要手工设置 `USE_SVE=yes`。`clients/c/Makefile` 在 `aarch64` 上默认加入 `-DUSE_ARM_SVE -march=armv8.2-a+sve`；`USE_SVE=yes` 仍适用于 server 的 `src` target 或 standalone SVE benchmark/UT。已验证 `make -C clients/c static install-headers`、`make -C memtier_benchmark` 和 `make -C src vemb_v16_server` 通过。

## 12. 2026-07-29 Redis target batch/SVE 复测

测试代码提交：`a84d4ff`。

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

测试代码提交：`c6ea39c`。

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

测试代码提交：`c6ea39c`；关联的 UB view flags 修正提交见正文。

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

测试代码提交：`c6ea39c`。

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

测试代码提交：`c6ea39c` + `9639bfe`。

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

测试代码提交：`c6ea39c` + `9639bfe`。

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

## 20. 提议架构：CLI/Proxy 两级缓存和单帧 `batch_request`

本节是后续实现的目标设计，不表示当前 wire protocol 已经变更。目标是同时消除三类
重复工作：CLI 对已读向量的重复跨机读取、同一批中相同 key 的重复 server 请求，以及
SuperNode 对 proxy 已能判断的 cache hit 的重复调度。本设计的 batch 永久限定为拓扑稳定
期的 `VEMB_HANDLE` 读路径；写、删除、`VEMB_INLINE`、`VSIM_*` 和迁移仍走现有权威
存储路径。扩容期间不创建、发送、解析或执行该 batch。

### 20.1 目标和非目标

- CLI 对同一逻辑读的重复项只向 server 发送一次，并可复用已物化的向量结果；
- server 的热 key location cache 前置到 proxy。proxy 命中时直接构造 response，
  不创建 job、不进入 SuperNode queue；
- CLI 到 server 的每个 flush 是一个逻辑 `batch_request` 帧，server 对该帧一次解码、
  一次排队和一次发布对应的 `batch_response`，不再把 batch 表示为多个独立 request
  ring slot；
- SuperNode 只接收 cache miss 的按 shard 子批，继续拥有 TLC/storage、写入和迁移的
  权威执行权；
- 不把 vector payload 复制进 server proxy cache。proxy cache 保存可验证的 location
  和 generation，命中后返回 handle；CLI cache 才保存已物化 vector，避免重复远端
  warm-region read/copy。

batch 的准入条件固定为 `op == VEMB_V16_OP_VEMB_HANDLE`、channel 已协商 batch
capability、请求 `topology_epoch` 等于 server 已发布 epoch，且 topology state 为
`STABLE`。`BATCH_REQUEST_SIZE` 是每个 frame 中去重后的最大 item 数，必须通过 ATTACH
协商。
任一条件不满足时，CLI 和 server 均使用当前逐项 Aeron/TCP 路径；这不是后续阶段才补齐的
限制。

不在本轮改变 Redis 命令语义、控制面 ATTACH、UB path 映射或跨 owner 转发语义。单个
batch 必须有协商的 item/byte 上限；“一个包”表示一个有边界的逻辑帧，不表示把无界的
2K/3K 请求塞进固定大小 ring slot。

### 20.2 新请求路径

以下例子对应 `docs/cross_node.jpeg`：CLI 收到 3K 个逻辑读，其中 1K 命中本地 cache，
剩余 2K 经去重后封装为若干受上限约束的 `batch_request`。每一个 batch 在 UB 数据面
中只有一个 descriptor 和一段连续的 frame payload。

```text
logical requests (N)
        |
        v
CLI L0: in-flight coalescing, same (VEMB_HANDLE, key, dim, epoch) -> one leader
        |
        +-- completed CLI L1 hit --> materialized response for all followers
        |
        v
one batch_request frame per flush (unique misses U)
        |
        v
UB batch descriptor ring + byte arena
        |
        v
server proxy L2: validate location cache
        |                         |
        | hit H                   | miss M
        v                         v
direct response entries     shard-grouped miss sub-batches
        |                         |
        |                         v
        |                    SuperNode -> TLC/storage
        |                         |
        +----------- batch response aggregator <---+
                            |
                            v
                  one batch_response frame (U entries)
                            |
                            v
             CLI fills L1 and fans out result to N callers
```

`req_id` 仍是每个逻辑请求的 completion identity；batch 只降低传输和调度次数，不能把
多个调用压缩成一个不可追踪的请求。response entry 使用原始 item index 恢复 caller
顺序，因此 cache hit、不同 shard 的 miss 和错误可以任意完成顺序，而 CLI 观察到的结果
顺序保持不变。

### 20.3 CLI 两层去重和缓存

CLI cache 分成必须实现的 L0 和可配置的 L1，避免把一致性要求不明的持久 cache 当作
正确性优化。

| 层 | key | value | 行为和一致性 |
|---|---|---|---|
| L0 in-flight | `(VEMB_HANDLE, key bytes, dim, topology_epoch)` | leader 的 batch item 和 follower 列表 | 同一未完成读只发送一次；所有 follower 等待同一 response。这一层不复用旧值，因此保持现有严格读语义。 |
| L1 completed | `(VEMB_HANDLE, key bytes, dim, owner_generation, value_generation)` | `vemb_v16_resp` 和已物化 vector | read-through LRU；命中不访问 server，也不做 warm-region handle dereference。默认仅在显式一致性策略允许时使用。 |

`VEMB_HANDLE` 的 L1 entry 存储完整 vector snapshot，而不是仅保存
`vector_offset`。仅缓存 handle 仍会在每次命中执行跨机 mmap/1200-byte copy，无法解决
当前 CLI 的主要热点。除 `VEMB_HANDLE` 外，所有操作均不进入 L0/L1 batch cache，也不从
L1 返回。

在一个 flush 内，CLI 先命中 L1，再为余项建立 L0。每个 L0 leader 对应 batch 中一个
item；所有 follower 的 `req_id` 保存在 CLI 本地 fan-out 表中，不发送重复 wire entry。
server response 返回后，leader 将结果复制给 follower，并移除 L0 entry。leader 失败、
超时或收到 `MOVED`/`ASK` 时必须唤醒全部 follower，且不得填充 L1。

跨 client 写入会让 completed cache 失效，因此 L1 必须显式提供以下模式：

- `strict`：只启用 L0；L1 不在普通读路径服务请求。这是默认模式，语义与当前实现一致。
- `session`：本 CLI 的 `VADD`/`VREM` 同步失效本地 key；其他 client 的写可能在 TTL 内
  可见为旧值。适合 benchmark 或明确接受 bounded-stale 的业务。
- `leased`：后续可选扩展。server 发放携带 `value_generation` 的只读 lease，并在写入
  之前回收或等待 lease 失效；只有完成该协议后，L1 才能在跨 client 写入下保持严格语义。

`owner_generation` 不能单独作为 completed cache version，因为它描述 owner/location
代际，不能证明同一 owner 下的覆盖写未发生。wire response 需要增加每 key 单调递增的
`value_generation`；写成功、删除、迁移 cutover 和 tombstone 均推进或使该 generation
不可用。

扩容开始是 completed cache 的硬失效点：CLI 收到 topology state 从 `STABLE` 迁出或
epoch 变更后，必须停止 batch flush、清空 L0/L1，并将尚未发布的 leader 改走当前逐项
路径。扩容完成且新的 epoch 已通过 ATTACH/控制面发布前，不得恢复 L0/L1 或 batch。

### 20.4 Proxy 前置 location cache

现有 `tlc_core` location cache 的命中发生在 SuperNode/TLC 路径。新 L2 cache 仅服务
拓扑稳定期的 `VEMB_HANDLE`，归
`vemb_v16_proxy_t` 所有，并使用 shard 分片、seqlock entry 和固定 probe 数，避免多个
proxy IO worker 使用互斥锁。entry 至少包含：

```text
key_hash + key fingerprint/key bytes
topology_epoch + owner_generation + value_generation
region_id + region_index + local_slot + offset + bytes
key-meta/fence generation
```

proxy 命中不等于直接信任旧 offset。命中流程必须是：

1. 以 acquire 读取 cache entry 的 seqlock，确认完整 key、`topology_epoch` 和
   `owner_generation`；
2. 通过只读的 key-meta/fence view 验证 entry generation，确认该 key 不在 `CUTOVER`、
   `SOURCE_GC` 或 tombstone 状态；
3. 再次读取 fence generation。两次检查相同才允许从 location 组装
   `VEMB_HANDLE` response；
4. 任一步失败均视为 miss，交给 SuperNode 的 stable-read 路径，不能从旧 warm/cache
   location 返回数据；
5. SuperNode 对 stable-read miss/命中得到的权威结果，在完成 response 前回填或更新
   proxy L2 entry。

这保留了当前迁移 fence 的“查找前后都验证”要求。`region_id` 只作为稳定外部身份，
`region_index` 仅用于本进程访问，两者必须一起缓存，不能假定相等。

写入、删除、迁移控制面按照下列时序处理：

1. proxy 收到 `VADD`/`VREM`、迁移 begin/cutover/source-GC 时，先使对应 key 或 range
   的 L2 entry 不可命中；
2. 仍由 SuperNode/TLC 执行权威操作；
3. 成功 completion 携带新的 `value_generation` 和 location；需要继续可读时再回填 L2，
   删除和 tombstone 只保留短期 negative entry 或不缓存；
4. topology epoch 变化时按 owner/range 批量失效，不允许旧 epoch entry 服务请求。

扩容状态下 L2 整体视为不可用，而不只依赖单 key fence。proxy 在开始扩容时关闭 batch
admission、停止 L2 hit 回包并清空/隔离当前 epoch 的 L2 entries；所有读回到既有
SuperNode `stable_read` 路径。只有扩容完成、所有新 owner view 发布且 topology state
重新为 `STABLE` 后，才为新 epoch 新建空的 L2 cache view。

为避免引入“只做 NULL 检查再转发”的薄包装，TLC 应暴露一个有明确语义的
`stable_read` 结果或 cache-view 发布接口；proxy 不应自行访问 warm region 原始数组，也
不应复制 TLC 的完整 location 查找决策树。

### 20.5 单帧 batch wire 和 ring

当前 `vemb_v16_client_publish_request_batch()` 一次提交多个可变长度 slot，server 仍在
`vemb_v16_aeron_poll_shm_requests()` 中逐 slot decode。本设计改为一条 batch descriptor
ring 和一块 byte arena：descriptor 仅包含 `(batch_id, offset, bytes, item_count)`；payload
是一个连续的 `batch_request` frame。producer 先在 arena 保留全部字节、写满 frame，最后
release-store descriptor tail；consumer acquire-load 一次 descriptor tail 后直接解析该 frame。

batch 使用独立的 v2 ATTACH 和新的 v2 logical channel，不替换现有 v1 request/response
channel。CLI 在同一 worker 内同时保留两套资源：v1 常驻，用于所有非 `VEMB_HANDLE`、
扩容期请求和 `BATCH_FALLBACK_LEGACY` 重放；v2 只在稳定期发送 batch。一个 v2 方向由
descriptor ring 和 byte arena 组成，因此 v2 channel 包含 request descriptor/arena 与
response descriptor/arena 四段 UB 资源。这样 server 关闭 batch admission 时，CLI 可立即
切回已存在的 v1 channel，不需要在扩容窗口内新建 channel。

```text
batch_request
  header: magic, version, flags, channel_id, batch_id, topology_epoch,
          item_count, entry_table_bytes, payload_bytes
  entries[item_count]: original_index, req_id, op, flags, body_offset, body_bytes
  bodies: compact request bodies

batch_response
  header: magic, version, flags, channel_id, batch_id, topology_epoch,
          item_count, entry_table_bytes, payload_bytes
  entries[item_count]: original_index, req_id, status, op, body_offset, body_bytes
  bodies: compact response bodies
```

公共字段从 item 中提升到 header；每个 body 只编码 `VEMB_HANDLE` 所需字段，复用
现有 `vemb_v16_req_encode`/decode 的该操作语义，但 channel 和 batch 共有字段不重复编码。
需要在 `vemb_v16_protocol.h` 中为 frame
增加独立的有界 encode/decode API，不应让 server 把 wire bytes 强转为 C struct。所有
`offset + bytes`、`item_count`、entry table 长度及单 item 操作形状均在 proxy 的一次早期
校验中验证；非法 batch 为每个已声明 item 返回 `ERR`，避免 CLI waiter 永久挂起。

#### 20.5.1 可配置 `BATCH_REQUEST_SIZE`

`BATCH_REQUEST_SIZE` 是 CLI 的 batch flush 阈值，单位为已去重的 `VEMB_HANDLE` item，不是原始
调用数，也不是 SuperNode miss 数。例如 64 个逻辑调用在 L1 命中 16 个、L0 去重后剩余
32 个 leader 时，只形成一个 32-item frame；即使 `BATCH_REQUEST_SIZE=64` 也不会填充虚拟 item。

CLI 在 ATTACH 请求中发送 `requested_batch_size`；server 以运行时配置
`VEMB_V16_BATCH_REQUEST_SIZE` 和安全硬上限 `VEMB_V16_BATCH_REQUEST_SIZE_MAX` 裁决，返回
`effective_batch_size`、`max_batch_bytes` 和 batch protocol version：

```text
effective_batch_size = min(requested_batch_size,
                           server VEMB_V16_BATCH_REQUEST_SIZE,
                           VEMB_V16_BATCH_REQUEST_SIZE_MAX)
```

`BATCH_REQUEST_SIZE` 必须为正整数；建议默认值沿用当前有效批量的 `32`。`1` 是正确性和协议回退
对照，较大值需受 byte arena、aggregation context pool 和 P99 延迟预算约束。server 以
`VEMB_V16_BATCH_REQUEST_SIZE_MAX` 预分配并限制 descriptor、response entry 和 aggregation context
容量，不能按客户端声明临时扩容或在热路径 malloc。

实际 frame item 数还受 `max_batch_bytes` 限制：CLI 选择不超过 `effective_batch_size` 且
完整编码后不超过 byte 上限的最大前缀；第一个 item 已超过 byte 上限时，该 item 直接走
legacy 逐项路径。到达 `effective_batch_size`、byte 上限或 flush deadline 任一条件即发布。

配置变更只影响后续 channel。CLI 修改 `BATCH_REQUEST_SIZE` 或 server reload
`VEMB_V16_BATCH_REQUEST_SIZE` 后，必须先 drain 已发布 batch，再关闭并重新 ATTACH channel；不得
让同一 channel 的 in-flight frame 使用两个 item 上限。扩容期间该协商值无效，因为 batch
admission 已关闭。

frame 上限通过 ATTACH 协商。CLI 到达 `effective_batch_size`、`max_batch_bytes` 或 flush
deadline 时发布一个 frame；key 或 item 超过上限时直接走当前逐项路径，绝不扩张固定 slot，
也不发布半个 batch。arena 环绕使用 padding descriptor 或双映射保证单个 frame 连续；
response arena 采用同样规则。

### 20.6 Server 端批处理和聚合

server 的数据面入口变为：

```text
vemb_v16_aeron_poll_vemb_batch_frames()
  -> vemb_v16_batch_request_decode()
  -> vemb_v16_proxy_handle_batch_request()
  -> L2 hit response fill + miss shard grouping
  -> vemb_v16_supernode_handle_job_batch() for miss sub-batches
  -> vemb_v16_batch_response_publish()
```

proxy 为每个 batch 分配一个只在本地可见的 aggregation context，含 `pending_count`、
`response[item_count]`、原始 index 和一次性发布状态。L2 hit 直接填 response；miss 仅按
目标 shard 形成 `vemb_v16_job_batch_t`，而不是重新经过 client ring 或逐条 response
publish。所有 miss completion 写回同一 context；`pending_count` 归零后只发布一次
`batch_response` frame。

同一 batch 内只允许合并完全相同的 `VEMB_HANDLE` 读
`(key, dim, topology_epoch)`。任何非 `VEMB_HANDLE` item、混合读写 batch、epoch 不匹配
或 topology 非 `STABLE` 的 batch 都不进入此入口。server 返回整个 frame 的
`BATCH_FALLBACK_LEGACY` 结果，CLI 保持原始 `req_id`，以当前逐项路径重放；不得部分执行
该 batch 后再切换路径，以免破坏调用方的完成语义。

### 20.7 落地顺序和验收

1. 先增加 protocol version/ATTACH capability，保留现有 per-slot ring 作为运行期 v1 回退；
   普通 v1 client/server 组合可以继续协商共同版本，但 batch session 必须完成 v2 ATTACH。
2. 实现 CLI L0 去重和仅限 `VEMB_HANDLE` 的 `batch_request`/`batch_response`，验证同批
   重复 key 的 server 请求数恰为唯一 key 数，并保持 response 顺序和错误 fan-out。
3. 增加 proxy L2 location cache view 与迁移 fence 双检；cache hit 不得创建
   SuperNode job。通过 migration、tombstone 和并发 VADD/VREM 单测验证不返回旧 location。
4. 接入 CLI L1 `session` 模式和 metrics；`strict` 只启用 L0。leased 模式必须在租约和
   失效协议完成后单独上线。
5. 增加扩容门禁测试：扩容开始立即停用 batch 与 L1/L2、所有未发送项走 legacy、server
   拒绝 topology 非 `STABLE` 的 batch；扩容完成并发布新 epoch 后才能重新协商 batch。
6. 以 `BATCH_REQUEST_SIZE=1/8/32/128` 做协议和压力测试，验证 ATTACH 返回的
   `effective_batch_size` 一致、byte 上限能截断 frame、配置变更只在 re-ATTACH 后生效，
   并同时报告吞吐、P99 和实际 item-size 分布。

至少新增以下计数并在 A/B 中报告：

- CLI：`l0_coalesced`, `l1_hit`, `l1_miss`, `batch_frames`, `batch_items`,
  `batch_unique_items`, `batch_size_configured`, `batch_size_effective`,
  `batch_byte_limited`, `vector_copy_bytes`；
- proxy：`batch_frames`, `batch_items`, `l2_hit`, `l2_miss`, `l2_fence_reject`,
  `supernode_miss_items`, `response_frames`；
- 一致性：`cache_invalidate`, `stale_generation_reject`, `cutover_reject`,
  `tombstone_reject`。

验收不只看 QPS：对重复 key `VEMB_HANDLE` workload，L0/L1/L2 命中必须分别降低 server
items、SuperNode jobs 和 CLI vector copy bytes；扩容期间 batch item、L1 hit 和 L2 hit 必须
为零，且所有请求走 legacy stable-read。对 migration/cutover/source-GC/VREM 并发压力，
严禁从缓存返回 tombstoned 或旧 source location。兼容模式下现有
`vemb_v16_aeron_ring_ut`、migration control、remote-meta 和跨节点 read-verify 必须保持
通过。

## 21. 代码落地计划和验收

本节将第 20 节的目标映射到当前代码。第一版只实现稳定期 `VEMB_HANDLE`；v1 协议和
现有 per-slot request/response ring 是常驻回退路径，不删除、不复用为 v2 batch arena。

### 21.1 双通道运行模型

| 通道 | 资源 | 使用范围 | 生命周期 |
|---|---|---|---|
| v1 legacy | 现有 request ring + response ring | `VADD`、`VREM`、`VEMB_INLINE`、`VSIM_*`、扩容期 `VEMB_HANDLE` 与 batch fallback | CLI worker 启动时 ATTACH，始终保留 |
| v2 batch | request descriptor ring + request byte arena；response descriptor ring + response byte arena | topology 为 `STABLE` 的 `VEMB_HANDLE` | v2 ATTACH 成功后启用；epoch 变化或关闭时 drain 后回收 |

v2 ATTACH 分配新的 `channel_id`，并在 response 中返回四段 UB 资源、protocol version、
`effective_batch_size` 与 `max_batch_bytes`。使用 batch session 的 SDK 必须成功建立 v2；旧 SDK
或不支持 v2 的 server 可以继续只建立 v1，但 batch session 在 v2 ATTACH、四段资源映射或 L0
初始化失败时必须直接 open 失败，不能退回 v1-only session。不得在原固定长度 ATTACH struct 尾部追加字段，
必须使用新的 ATTACH magic 和固定长度 v2 request/response struct，避免旧 SDK 读错 response。

### 21.2 模块和职责

| 模块 | 文件 | 主要改动 |
|---|---|---|
| v2 ATTACH | `src/vemb_v16_aeron_attach.h/.c`、`src/vemb_v16_server_integration.c`、`clients/c/vemb_v16_client_sdk.c` | 新 magic、v2 request/response、feature negotiation 和运行期 v1 fallback；batch session 的 v2 ATTACH 失败为 open failure；sniff 路由按 magic 分派。 |
| UB 资源 | `src/vemb_v16_storage.h/.c`、`src/vemb_v16_proxy_types.h` | 新增 v2 channel 的四段映射分配、关闭和失败回滚；channel 保存 descriptor/arena map、bytes 与协商参数。 |
| batch ring/codec | 新增 `src/vemb_v16_batch_ring.h/.c`、`src/vemb_v16_protocol.h` | SPSC descriptor/arena reserve、publish、poll、consume；只编码 `batch_id`、epoch、item count、key length 和 packed key，response 用 item index 对应 handle/status。 |
| server ingress | `src/vemb_v16_aeron_transport.c`、`src/vemb_v16_proxy.c` | `poll_vemb_batch_frame -> decode -> proxy_handle_vemb_batch -> batch_response`；不能经由旧 client ring 再拆成 wire request。 |
| miss completion | `src/vemb_v16_proxy.c`、`src/vemb_v16_supernode.h/.c` | aggregation context pool；job/completion 增加 batch context id、generation 和 item index，miss 完成后回填同一 batch response。第一版复用既有 job pool/shard queue，不先重写调度器。 |
| CLI | `clients/c/vemb_v16_client_sdk.h/.c`、`memtier_benchmark/vemb_v16_aeron_runner.cpp` | v2 open/close/publish/poll API；worker 选择 v1/v2，维护 L0 去重与 follower fan-out。 |
| 配置和统计 | `src/server.h`、`src/config.c`、`src/vemb_v16_server_integration.c`、memtier option parser、`src/vemb_v16_stats.*` | server `vemb-v16-batch-request-size`、standalone `--batch-request-size`、memtier `--vemb-v16-batch-request-size`；新增协商、cache、frame 与 fallback 指标。 |

### 21.3 分阶段实现

1. **门禁和协商。** 增加 `BATCH_REQUEST_SIZE` 配置、硬上限
   `VEMB_V16_BATCH_REQUEST_SIZE_MAX`、强制 v2 ATTACH 和运行期 v1 fallback。server 为每个 v2 channel
   固化 `effective_batch_size`，配置变更只影响 re-ATTACH 后的新 channel。
2. **数据面基础。** 实现 batch descriptor ring、byte arena 和精简 frame codec，先完成单进程
   producer/consumer UT、arena wrap、backpressure 和关闭回收；这里不接入 proxy cache。
3. **server 直接批处理。** 在 proxy 创建固定容量 aggregation context pool。L2 未实现时，
   batch 的全部 item 都以现有 job ref 进入 shard queue；completion 汇总为一个
   `batch_response`。这一步已消除 CLI/server 间逐 slot wire decode 和逐条 response publish。
4. **CLI L0，先于 proxy cache。** 在 memtier worker 中对同一 batch 的相同
   `(VEMB_HANDLE, key, dim, epoch)` 建立一个 leader 和多个 follower；仅发送 leader，按
   `(batch_id, item_index)` fan-out。L0 不复用已完成值，因此不引入 stale read。
5. **proxy L2 location cache。** 新建 proxy-owned cache view，而不是把 TLC 的 raw lookup
   代码复制到 proxy。SuperNode stable-read completion 回填 location；proxy hit 做 key-meta
   fence 前后双检，命中直接写 aggregation response，miss 才进 SuperNode。`VADD`/`VREM`、
   tombstone、cutover、source-GC 和 epoch 变化均使 entry 不可命中。
6. **CLI L1 completed cache。** 默认关闭；先以显式 `session` 模式保存已经 materialize 的
   vector，减少重复 warm-region copy。严格跨 client 一致性需要额外的失效/lease 协议，未完成
   前不作为默认行为。
7. **扩容和性能收尾。** 在所有 `vemb_v16_proxy_migration_*`、epoch/topology set 入口先关闭
   batch admission，再等待已接收 aggregation context drain、清空 L2，最后切换 storage
   migration state。扩容中 v2 frame 返回 `BATCH_FALLBACK_LEGACY`，CLI 以保留的 v1 channel
   重放；新 epoch 发布并重新 ATTACH v2 后才恢复 batch。

proxy 是 batch admission、cache 和 quiesce 的唯一决策层；storage/TLC 继续负责权威
stable-read、写入、fence 和迁移状态。这避免 proxy 与 storage 重复实现同一套拓扑决策。

### 21.3.1 当前落地状态

已完成 v2 ATTACH 控制面：server sniff 按 v1/v2 magic 分派；v2 handler 读取协商字段，
在 migration active 时拒绝，调用四段 UB allocator 和 proxy v2 channel attach，并在 response
返回四段 `(backend, path, offset, bytes)`、固定的 descriptor slot/ring 参数、当前 epoch、
`effective_batch_size` 与 `max_batch_bytes`。SDK 有独立的 v2 open/close/resource API，已完成
四段 UB path 映射、mmap、错误回滚和 close notification；v1 channel 和其 publish/poll API
保持不变。

跨机 OBMM 设备的 client 视图不得把 resource 的非零 `offset` 直接作为 `mmap` offset 使用。
SDK 对 v1 ring 和已映射到 peer 视图的 v2 UB resource 都从 offset 0 映射覆盖
`offset + bytes` 的范围，再以 `mapping_base + offset` 取得资源视图；同机和非 OBMM path 仍按
普通 offset mmap。这样连续 ATTACH 的后续 channel 不会映射回设备首段，v1 remote open 也会验证
request/response ring header 后才返回 channel。

所有 channel resource 的对外 bytes 和 UB pool reserve 均以
`CACHELINE_SIZE`（当前 64B）对齐。v1 slot 在 ATTACH 时先对齐，ring layout 的
header、stride 和总 bytes 因此天然对齐；v2 descriptor slot 固定为 64B，client 请求的
`max_batch_bytes` 由 server 向上对齐后协商并同时作为 request/response arena 的 resource bytes。
SDK 拒绝非 64B 对齐的 v2 response，避免 peer 对同一 arena 使用不同边界。

server 在写零、初始化 ring header 和 arena 后才返回 ATTACH response。UB 负责本地 CC 与对端
NC view 的初始化可见性，代码不执行 `dc cvac`、`dc ivac` 或 `dsb sy`；不得把 CPU cache
maintenance 当作 OBMM 跨机 ATTACH 的正确性条件。

#### 21.3.2 跨机 ATTACH non-zero-offset 缺陷（2026-08-03）

测试代码提交：`6249f28`。

复现条件是 111 server 使用 CC UB view（`/dev/obmm_shmdev3` request、
`/dev/obmm_shmdev2` response），112 client 使用对应 NC view（`/dev/obmm_shmdev7`、
`/dev/obmm_shmdev6`），连续创建 channel。第一条通常位于 offset 0；第二条 v1 曾出现 request
ring 正确而 response ring header 为零，v2 曾出现 descriptor header 读取为零或上一条 channel 的
值。根因是 SDK 把 server 返回的非零 UB offset 直接传给远端 `mmap`，没有取得可靠的 resource
view；旧 v1 open 也没有验证 response ring header。

修复为 v1/v2 共用“从 peer UB view 的 offset 0 映射 `offset + bytes`，再使用
`mapping_base + offset`”的资源映射，并保存 mapping base/bytes 供 close 正确释放。v1 remote
open 对 request/response header 做完整 layout 校验；server 仅在四段资源初始化完成后写 ATTACH
response，跨机可见性继续由 UB 的 CC/NC mapping 保证。

新增 `benchmark/vemb_v16_attach_smoke` 连续创建两条 v1 channel；既有
`benchmark/vemb_v16_batch_attach_smoke` 连续创建两条 v2 channel 并校验两侧 descriptor header。
跨机已验证 v1 第二条的 request/response 非零 offset、v2 第二条的四段非零 offset，以及 v1
分配后再创建 v2 的共享 UB pool 场景。`benchmark/ub_cc_nc_visibility_ut` 还验证了
`3 -> 7` 和 `2 -> 6` 的 CC writer / NC reader 基础可见性。

#### 21.3.3 v2 batch 数据面最小闭环（2026-08-03）

测试代码提交：`6249f28`。

v2 descriptor/arena 现已接入数据面。client 将一个只含 `VEMB_HANDLE` key 的 request frame
写入 request arena，再向 descriptor ring 发布 `(start, bytes, item_count)`；proxy 从 descriptor
ring 取 frame，逐 item 创建既有 read job，SuperNode completion 按原始 item index 聚合为一帧
`batch_response` 后写入 response arena 和 response descriptor ring。response 中对外的 `req_id`
固定重写为 item index，因此后续 CLI L0 可用 `(batch_id, item_index)` 完成 follower fan-out，
而不依赖 server 内部 job id。

arena 不另设共享 head/tail ABI：producer 保存本地 monotonic arena tail，并通过读取已由 peer
推进的 descriptor-ring head 与被消费 descriptor 的 `(start, bytes)` 推导可回收边界。consumer
在读取/解码 arena frame 后才推进 descriptor head，故 producer 不会覆盖正在读取的 frame。单测
覆盖 descriptor backpressure 和 arena wrap；跨机 smoke 在同一 channel 连续发送两帧，覆盖
request/response 两侧的回收复用。

aggregation context pool 现已允许同一 v2 channel 同时保留多个 in-flight batch，并按 shard 聚合。
CLI L0/L1 和 proxy L2 仍未接入。数据面会在每帧检查 epoch 和 migration active，失败项返回
`STALE_TOPOLOGY`；CLI 的 legacy replay 与完整 topology-STABLE admission 仍是后续阶段。UB pool
的物理空间仍是进程生命周期管理，当前 close 释放 proxy/SDK
ownership，但不回收 bump allocation；动态 UB path 映射和可复用 allocator 是后续工作。

**当前完成清单：**

| 范围 | 当前状态 | 代码入口 |
|---|---|---|
| v2 ATTACH 与四段 UB resource | 已完成；64B 对齐、远端 non-zero offset 映射和 close notification 已覆盖；batch session 必须完成 v2 ATTACH、资源映射和 L0 初始化，否则 open 失败 | `vemb_v16_aeron_attach.*`、`vemb_v16_storage.*`、SDK `open_remote_batch()` |
| request/response frame | 已完成；request 只允许 packed `VEMB_HANDLE` key，response 以 item 顺序携带 compact `vemb_v16_resp` | `src/vemb_v16_batch_ring.h` |
| descriptor/arena | 已完成；descriptor 使用现有 SPSC client ring，arena 回收由已消费 descriptor 推导 | `vemb_v16_batch_arena_publish()` |
| server ingress | 已完成；proxy 从 request descriptor 解码，沿用 read job/SuperNode 路径，completion 聚合后一次返回 | `vemb_v16_aeron_poll_shm_requests()`、`vemb_v16_proxy_handle_batch_request()` |
| SDK 数据面 API | 已完成；`vemb_v16_aeron_batch_publish_handle()` 与 `vemb_v16_aeron_batch_poll_response()` | `clients/c/vemb_v16_client_sdk.*` |
| aggregation context pool | 已完成；每个 proxy IO worker 固定 4 个 context，v2 channel 可同时接收至多 4 个 in-flight batch | `vemb_v16_proxy_handle_batch_request()`、`batch_context_complete()` |
| CLI L0 | 已完成首版；SDK batch session、key-only coalescing、强制 v2 准入、运行期 v1 fallback 和 memtier pure-handle consumer 已覆盖 | `clients/c/vemb_v16_aeron_batch_client.c`、`clients/c/vemb_v16_cli_l0.c` |
| CLI L1、proxy L2 | 未开始 | 第 6/5 阶段 |
| 扩容 fallback/replay | 未开始；当前仅每帧返回 `STALE_TOPOLOGY` | 第 7 阶段 |

**当前行为与限制：**

- 一个 v2 channel 固定归属 `channel_index % proxy_io_worker_count` 对应的 proxy IO worker；该
  worker 串行消费该 channel 的 SPSC request descriptor ring、更新其 aggregation context，并消费
  completion ring。因此不需要 context 锁，也不会在 `pending_count` 初始化前处理 completion。
- 每个 proxy IO worker 有 `VEMB_V16_BATCH_CONTEXTS_PER_PROXY_WORKER=4` 个固定 context；同一
  v2 channel 可有最多 4 个已接收但未发布 response 的 batch。子池耗尽时不消费 request descriptor，
  由 client 侧 descriptor backpressure 保持 frame，直到一个 context 发布 response 后复用。
- `batch_id` 是 response frame 的 identity；server 内部为每个 item 分配临时 `req_id`，response
  对 SDK 重写为 `item_index`。该约定是后续 L0 leader/follower fan-out 的接口，不能更改为内部
  req id。
- 当前 frame admission 已检查 request epoch 与 migration active；尚未实现显式 topology
  `STABLE` 状态检查、`BATCH_FALLBACK_LEGACY` 或 client v1 replay。不要让业务 client 使用 v2
  作为扩容期间的通用回退路径。
- 当前 smoke 使用不存在 key 验证 request -> job -> completion -> response 的完整跨机链路；
  仍需补充已存在 vector 的 `VEMB_HANDLE` metadata 与 warm-region dereference read-verify。

**已完成验证（2026-08-03）：**

- 本地：`vemb_v16_batch_request_config_ut`、`vemb_v16_batch_ring_ut`、
  `vemb_v16_batch_context_ut`、`vemb_v16_batch_close_drain_ut`、
  `make -C src vemb_v16_server USE_UB=yes`、
  `make -C clients/c static`；
- 跨机：111 为 CC server view（`/dev/obmm_shmdev3`、`/dev/obmm_shmdev2`），112 为 NC client
  view（`/dev/obmm_shmdev7`、`/dev/obmm_shmdev6`）；同一 v2 channel 在首次 poll 前连续发布
  四个 batch，并接收四个 batch response，`32769` bytes 协商为 `32832` bytes；
- 回归：随后 v1 连续两条 ATTACH 的 non-zero-offset request/response header smoke 通过；
  两端 `sync_changed_code_to_peer.sh --dry-run` 均为 `candidates=23 different=0`。

**第 4 阶段 CLI L0 已完成首版；下一个实现入口为 proxy L2 location cache。**

目标数据流程如下。context 是 proxy IO worker 私有的 server 内存，不映射到 UB；一个 worker
持有固定容量子池，避免按 channel 预分配大量 `response[128]`，也避免多个 worker 在热路径争用锁。

```text
CLI / SDK
  batch_id=101                 batch_id=102
       |                            |
       +-- request frame + descriptor
                    |
                    v
 request descriptor ring + request arena
                    |
                    v
 proxy IO worker (channel pinned to this worker)
   peek descriptor -> request one free context
                    |
          +---------+---------+
          |                   |
       context free        pool exhausted
          |                   |
          v                   v
 decode + consume       keep descriptor unconsumed
 assign batch_token     (natural request-ring backpressure)
          |
          v
 existing VEMB_HANDLE read jobs -> shard queues -> SuperNode/TLC/storage
          |                                      |
          +---------- completion(batch_token) ---+
                    |
                    v
 proxy IO worker validates context slot + generation + item index
   -> fill response[item_index] -> pending_count--
                    |
                    v
 pending_count == 0 -> one batch_response -> response arena + descriptor ring
   -> release context (the next acquire increments generation)
                    |
                    v
 SDK poll by batch_id/item_index
```

`batch_token` 仅在 server 的 job/completion 生命周期中传递，至少编码 context slot、generation 和
item index；不得借用对 SDK 可见的 `req_id` 区间推导 context。completion 到达时必须同时校验 token
generation、channel identity 和 context state，旧、重复或 close 后的 completion 直接释放 payload 并
丢弃。

在 `vemb_v16_proxy_io_worker_t` 中使用固定容量子池替代 channel 单份聚合状态。每个 context 保存
`batch_id`、epoch、item count、pending count、completion bitmap、response entries 和 generation；
descriptor 被消费前分配 context，completion 以 `(worker_id, context_slot, generation, item_index)`
回填。pool 耗尽时保留 request descriptor 不消费，形成背压，不能 malloc 或覆盖未完成 context。
`vemb_v16_batch_context_ut` 覆盖四 context 耗尽、乱序 completion、重复 completion、generation
拒绝旧 completion 和按 channel abort。现有跨机 smoke 覆盖四个 frame 先发后收和 v1 回归；完整
channel close 与 in-flight SuperNode job 的并发 drain 由
`vemb_v16_batch_close_drain_ut` 覆盖：close 等待已取得 SuperNode 引用的 job；该 job 在关闭期间
发布 completion，释放引用后 channel 才回收 completion ring 和四段资源。该 UT 直接调用真实的
SuperNode completion handler，但不覆盖 shard queue 的调度延迟。第 4 阶段 CLI L0 可以继续。

#### 21.3.4 下一阶段：CLI L0 in-flight coalescer 设计

本节定义第 4 阶段的实现边界；目前尚未接入代码。此处的 L0 不是 completed-data cache，
也不保存 vector、handle 或任意可在未来请求中复用的结果。它是 CLI worker 私有的动态
`group by`：将相同且尚未完成的 `VEMB_HANDLE` 读合并为一个 leader 和零到多个 follower。
为避免与后续 L1 混淆，代码和指标应优先使用 `inflight_group`、`inflight_group_index`、
`coalesced_follower` 和 `new_leader_group`，而非泛称的 cache hit/miss。

v2 batch 的 item 固定为 `VEMB_HANDLE`，dim 已由 v2 ATTACH/channel 协商，epoch 已由 frame
header 表示。因此这三者是 admission 和 L0 namespace 的约束，而不是 bucket 的 key。通过
admission 后，L0 只按 **final VEMB key bytes** 聚合；hash/fingerprint 只用于定位，最终仍以
`key_len + memcmp` 判等。epoch、dim、batch channel 集合或 channel generation 改变时，当前
worker 的 L0 namespace 整体 detach，禁止新旧条件下的 group 共存。

**适用场景和硬边界：**

- 只在一个 CLI worker 内工作，不跨 worker、进程或 client 合并；worker 是 L0 索引、entry
  pool、follower pool 和 v2 channel 的唯一写者，因此热路径不加锁；
- 只处理 `op == VEMB_V16_OP_VEMB_HANDLE`、topology 为 `STABLE`、请求 epoch 等于该 worker
  当前已协商 epoch，且 v2 channel 已 ATTACH 成功、channel dim 等于 worker 配置的请求；通过
  该 admission 后，相同 final key bytes 才属于同一 group；
- `VADD`、`VREM`、`VEMB_INLINE`、`VSIM_*`、未协商 v2、任何扩容/迁移窗口、epoch 切换和
  `BATCH_FALLBACK_LEGACY` 重放均绕过 L0，保持现有 v1 逐项路径；它们既不能成为 leader，
  也不能作为 follower 加入已有 group；
- 同一 key 的并发读会被合并，已完成的读绝不命中 L0。收到终态 response 后 group 在 fan-out
  前从索引移除，所以 completion callback 中出现的新读必然建立新 leader，不会加入已完成结果；
- L0 不改变请求完成身份。leader 和每个 follower 都保留各自 caller 的 `req_id`、回调/等待者
  引用和请求次序；仅 leader 占用一个 `(batch_id, item_index)` wire item，response 返回后再
  对全部 caller fan-out 相同的 status/handle。

典型收益场景是同一 worker 在 batch flush deadline 内收到热点 key 的多个并发
`VEMB_HANDLE`。例如 64 个逻辑调用中 20 个为 key A、12 个为 key B、32 个为不同 key，L0
建立 34 个 leader 和 30 个 follower；server 只收到 34 个 item。它不优化不同 worker 的重复
请求，也不减少独立 key 的 warm-region vector copy；后者属于未来 L1 completed cache 的范围。

**请求和生命周期：**

```text
logical VEMB_HANDLE request
        |
        +-- v2 admission failed / migration / epoch mismatch --> v1 legacy request
        |
        v
hash + exact lookup in inflight_group_index
        |                                |
        | existing same key              | no existing key
        v                                v
append follower                      allocate leader group + key slab
                                         |
                                         v
                               PENDING_SEND (keeps index membership)
                                         |
                     flush/space         | ring or byte backpressure
                               v          v
                          PUBLISHED    retain and retry next poll/flush
                               |
                               v
             batch_inflight[batch_id][item_index] = (entry_id, generation)
                               |
                       v2 response / terminal error / timeout
                               |
                               v
       remove group from index -> fan out leader and followers -> recycle all pools
```

`PENDING_SEND` 必须继续留在索引中，因此同 key 的后续调用仍会合并，而不会因暂时的
descriptor/arena 背压重复创建 leader。v2 item 尚未发布前若确定不能准入，可将该 group
转为 `FALLBACK_V1`，但在其 v1 leader 未终态完成前仍保留索引以继续合并；v1 completion
沿用相同的“先 remove、后 fan-out”顺序。发布后的 response 一律以
`(batch_id, item_index)` 找到 `(entry_id, generation)`；generation 不匹配、重复 response、
未知 batch 或已 detach 的 group 只计数并丢弃，不能唤醒已复用的 entry。

**固定容量索引和内存布局：**

L0 不是通用字典，不采用 SDS、动态分配、rehash、overflow chain 或 Robin-Hood 搬迁。建议复用
Valkey 的紧凑 bucket 思想，但只保留固定探测和完整 key 比较。初始默认容量为
`L0_MAX_ENTRIES=1024`，`256` 个 bucket，每 bucket `12` 个 slot，共 `3072` slot；entry pool
上限仍为 1024，较低装载率为热点 burst、删除和无链表查找保留余量。bucket 可定义为：

```c
typedef struct l0_bucket {
    uint8_t fingerprint[12];
    uint16_t occupied;
    uint16_t reserved;
    uint32_t entry_id[12];
} l0_bucket_t; /* 64 bytes */
```

hash 使用带进程 seed 的 64-bit hash 计算完整 final VEMB key bytes；bucket index 为
`hash & mask`，fingerprint 取 `hash >> 56`。slot candidate 的比较顺序固定为：occupied bit、
fingerprint、entry 中的完整 64-bit hash、key length，最后 `memcmp(key bytes)`。fingerprint
只用于减少无效比较，完整 hash 也不能替代 `memcmp`；不同 key 的 hash collision 绝不能被
合并。entry 在创建时记录 `(bucket_index, bucket_slot)`，终态删除仅清 occupied bit，因而是
O(1) 且不影响其他 slot。

entry 不应包含 `char key[VEMB_V16_MAX_KEY_LEN]`，以免每个未完成 group 都按最大 key 长度
浪费内存。初始化时预分配 segregated key slab，例如 `16/32/64/128/MAX_KEY_LEN` 五类；entry
仅保存 `key_len`、`key_class`、`key_slot` 和完整 hash。key slab、entry pool、follower pool、
pending-send queue 与 `batch_inflight` 均在 worker/channel 初始化时分配，热路径不得 malloc。
不能使用 FIFO key arena，因为 batch 可以乱序完成，先分配的 key 未必先释放。

bucket、entry、key slab 或 follower pool 满不是正确性错误，而是优化容量不足：本次逻辑请求
直接走 v1，计入对应 `l0_*_exhausted`/`l0_fallback_v1` 计数；不得临时扩容、创建链表或将不同
key 强行合并。follower 数也需要硬上限；达到上限时新的调用独立走 v1，以免一个热点 key
耗尽整个 worker 的等待者资源。具体容量应与每 worker 最大 pipeline、协商
`effective_batch_size`、最大 in-flight batch 数和延迟预算一起配置，而不是从
`BATCH_REQUEST_SIZE` 推导出无界内存。

**epoch、扩容和关闭：**

- topology 离开 `STABLE` 或 epoch 改变时，立即禁止新 v2 admission；`PENDING_SEND` group
  从 active index detach 并改走 v1，已 `PUBLISHED` group 保留其 allocation，只能由
  `batch_inflight` 的终态 response、timeout 或 channel teardown 回收；
- 不得把旧 epoch 的 follower 绑定到新 epoch leader。detach 与 response 路径均校验
  `(entry_id, generation, topology_epoch)`，避免延迟 response 误完成新 group；
- v2 channel close 时先停止 publish/poll 和新 admission，失效所有 index membership，取消或
  唤醒 followers；随后等待已发布 batch 的响应/关闭同步完成，再回收 entry、follower 与 key
  slab。该生命周期必须与现有 server aggregation context close/drain 对齐；
- L0 不填 L1，也不读取 L1。未来 L1 引入时应在 L0 之前做 completed-result 查找，只有 L1
  miss 才进入本节流程；strict 模式仍只启用 L0。

**第 4 阶段验收和可观测性：**

- 同 key 的 leader/follower 合并、不同 key（含相同 fingerprint 或强制 hash collision）不合并、
  key slab 复用、bucket/entry/follower 耗尽回退、ring backpressure 下 `PENDING_SEND` 继续合并；
- 乱序 batch response、重复 response、旧 generation、epoch detach、v1 fallback leader 和
  channel close/drain 都不会遗漏或重复完成 caller；
- 同一 batch 的 N 个相同 key 只产生一个 server item，所有 caller 获得 leader 的终态
  status/handle，且 caller 次序和 `req_id` 不改变；
- 增加至少 `l0_new_leader_groups`、`l0_coalesced_followers`、`l0_exact_key_mismatch`、
  `l0_bucket_full`、`l0_entry_exhausted`、`l0_follower_exhausted`、`l0_key_slab_exhausted`、
  `l0_fallback_v1`、`l0_stale_response` 和 `l0_active_groups` 指标。性能报告同时给出逻辑
  调用数、unique leader item 数、coalescing ratio 和 v1 fallback 数，不能只以 QPS 判断收益。

#### 21.3.5 CLI L0 实施计划（key-only namespace）

本计划按阶段验证，不自动提交任何代码。第一版只接入 `aeron-cross-node`：现有 v2 SDK 仅提供
`vemb_v16_aeron_open_remote_batch()`，本地 `aeron` 和全部非 batch 场景保持 v1。

1. **CLI batch session 和独立聚合器。** 在 `clients/c` 新建公开的
   `vemb_v16_aeron_batch_client_t`，其实现拥有一对 v1/v2 channel；任意 C/C++ CLI 都可 include
   `vemb_v16_client_sdk.h` 并以 `(final_key_bytes, caller_cookie)` submit、flush、poll completion。
   L0 是该 session 内部的 C 实现，而不是 memtier 私有对象。模块使用 64B bucket、固定
   entry/follower/key-slab pool，以及 `(batch_id, item_index)` 到 `(entry_id, generation)` 的 fixed
   batch-record map。第一版一个 session 绑定一个 v2 channel；需要多个 channel 的 CLI 创建多个
   session，因此 SPSC ownership、dim 和 epoch namespace 天然隔离。UT 先覆盖同 key 合并、hash
   collision 不合并、所有 pool 耗尽、backpressure、乱序/重复/旧 response 和 entry 复用。
2. **双通道提交。** batch session 在 remote open 时同时建立 v1 和 v2；v2 ATTACH、四段资源映射
   或 L0 初始化失败时 open 直接失败，绝不产生 v1-only batch session。v1 永久保留给非
   `VEMB_HANDLE`、扩容和已成功建立 v2 后的运行期 fallback。`BATCH_REQUEST_SIZE` 是 open 参数，以 ATTACH 返回的
   `effective_batch_size`、`max_batch_bytes` 为准；L0 leader 在 item/byte/deadline 任一阈值到达时
   组 frame。deadline 的创建、比较和重试一律使用 `vemb_v16_monotonic_ns()`（经
   `vemb_v16_util.h` 使用 `monotonic.h` 的 `getMonotonicNs()`）；不得使用 wall clock 或本地时间封装。
   `max_batch_delay_us=0` 保持现有 eager `poll()` flush；非零时首个 pending leader 固定 deadline，
   follower 不延后它。每次成功发布后若仍有 pending leader，下一 frame 立即 eligible，避免已排队的
   leader 再等待一个完整 delay。SDK 不创建 timer 线程；busy-spin CLI 在每轮 `poll()` 检查 deadline，
   其他事件循环型 CLI 可通过 `next_flush_deadline_ns()` 安排自己的 wakeup。
   v2 ring/arena 暂满时保持 `PENDING_SEND`，不重复发 key；bucket 或任一 pool 耗尽时
   内部提交单次 v1 leader。
3. **response fan-out。** session 的 poll 同时处理 v1 和 v2；v2 response 通过 batch-record
   定位 group，v1 fallback response 通过内部 req-id map 定位 group。对每个逻辑 caller 调用同一
   completion callback，返回原 caller cookie 与 `vemb_v16_resp`；SDK 不保存 completed vector，
   也不替 CLI 调用回调后的 vector 生命周期负责。SDK 提供 session-scoped warm-region read helper，
   CLI 可在一个 group 的 callback 中只 dereference 一次后自行同步 fan-out，不构成 L1。
4. **fallback、epoch 和关闭。** `STALE_TOPOLOGY`、response epoch mismatch 或 v2 error 将已发布
   group 转为单次 v1 leader；epoch/dim/channel generation 改变时停止 admission、detach L0
   namespace。session close 先停止 submit，再 drain 或以明确错误回调终止 v1 pending、v2 batch
   records 与 group，不遗留 caller cookie。
5. **CLI consumer 和验收。** memtier 只作为第一个 consumer：创建一个 session 并把其逻辑请求
   /完成统计接到 submit/poll callback，不再实现自己的 L0。每阶段先跑 SDK UT，再构建 `clients/c`
   与 `memtier_benchmark`。最终同步到 111/112 进行跨机 read-verify：N 个相同 key 只发一个 server
   item、所有 caller completion 均到达；如 CLI 选择在 callback 内共享临时向量，每 group 只发生
   一次 handle dereference。报告逻辑调用数、unique item 数、coalescing ratio、frame bytes 和 v1
   fallback。

#### 21.3.6 CLI L0 batch session 实现状态（2026-08-04）

测试代码基线：`6249f28`；本节的后续实现和测试包含工作区未提交变更。

第 4 阶段首版已实现为 `clients/c` 的 C API，而非 `memtier_benchmark` 私有逻辑。公开的
`vemb_v16_aeron_batch_client_t` 在 remote open 时创建一个永久 v1 fallback channel 和一个必需的
v2 batch channel；v2 ATTACH、四段 UB 资源映射或 L0 初始化失败时 session open 直接失败，不会创建
v1-only session。CLI 以 final key
bytes 和 caller cookie 调用 `submit_handle()`，再通过 `flush()`/`poll()` 接收原 cookie 对应的
completion。原 `poll()` 保持 SDK 不持久化 vector 的 response-only 语义；单独的
`read_vector()` 使用 session 的 v1 warm mapping。需要共享的 consumer 选择新的 shared poll
接口，而不是将 vector 变成已完成缓存。

共享 dereference 已在 SDK 落地：原 `poll()` callback ABI 保持不变，新增
`poll_shared_vector()`。session create 时预分配一个 `dim` 大小的 group scratch 和一个 direct-v1
scratch；收到 `OK` group response 时 SDK 在移除 L0 index 前只执行一次 warm-region copy，再把同一个
只读 `vector_view` 同步 fan-out 给 leader/follower。view 只在 callback 期间有效，下一 response 才能
复用 scratch，调用方不能保留它；它不是 L1、不会跨请求服务，也不会引入热路径分配。v1 fallback group
走同一共享路径，pool/direct fallback 仍保持逐项 v1 语义。统计新增
`shared_vector_group_reads`、`shared_vector_group_bytes`、read failures 和 fan-out 数。

deadline-aware flush 已完成：`vemb_v16_aeron_batch_client_options_t` 的
`max_batch_delay_us` 控制 underfilled frame 的最大聚合时间，零值保持 eager flush。SDK 将 canonical
`monotonic.c` 纳入独立库，并在首次非零 delay session open 时初始化一次；全部 deadline 路径使用
`vemb_v16_monotonic_ns()`。`submit_handle()` 在新 leader 使 item 或编码 byte 上限达到时立即发布；
否则只记录首 leader deadline。`poll()` 检查到期后发布，v2 ring/arena 背压保留 deadline 并在下一轮
重试。`next_flush_deadline_ns()` 仅提供给非 busy-spin consumer 作为 wakeup hint，不创建 SDK thread。
batch session 统计分别记录 `flush_eager`、`flush_full`、`flush_deadline` 与
`flush_backpressure`，避免零 delay 的兼容路径被误报为 deadline flush。

内部 `vemb_v16_cli_l0.c` 是固定容量的 C 实现：256 个 64B bucket，每 bucket 12 个 slot；1024
entry、4096 follower、`16/32/64/128` segregated key slab 和 64 个 fixed batch record。所有 pool
在 session create 时分配；submit、flush 和 completion 热路径不分配内存。index 只以 final key
bytes 判等，比较顺序为 occupied、fingerprint、64-bit hash、length 和 `memcmp`。bucket、entry、
key slab 或 follower 耗尽时该 caller 直接走 v1；batch ring/arena 满时 leader 保持
`PENDING_SEND`，继续接收同 key follower。

v2 response 按 `(batch_id, item_index)` 找到 `(entry_id, generation)`，随后从 index 删除 group 并
fan-out 给 leader/follower。`STALE_TOPOLOGY`、epoch mismatch 或 v2 poll error 停止新 batch admission，
对 live group 单次 v1 重放；v1 同样无法发布时以 `ERR` callback 完成，避免 caller 挂起。close 将
所有未完成 caller 以 `ERR` callback 收敛后释放 v1/v2 和 L0 资源。

memtier 已作为第一个 consumer 接入，但为控制本阶段风险，仅在
`aeron-cross-node + pure VEMB_HANDLE` workload 使用 batch session；混合读写、`VSIM_*`、`VREM` 和
本地 `aeron` 继续走原 v1 runner。新增 `--vemb-v16-batch-request-size=N`，默认 32，实际 item/byte
限制仍以 ATTACH 协商值为准；`--vemb-v16-batch-max-delay-us=N` 默认 0，非零时启用 deadline-aware
flush。worker 退出日志输出 leader、follower、frame、unique item、frame bytes、v1 fallback 和
eager/full/deadline/backpressure flush 原因计数。

跨机性能 A/B 使用 `--vemb-v16-batch-disable` 建立 v1 基线。该开关只禁止创建 v2 batch
session，仍以完全相同的 `VEMB_HANDLE` 请求、key distribution、pipeline 和 warm-region
dereference 执行；它不能替代或与 `--vemb-v16-vrem` 混用，后者会将读操作改为 `VREM`，不具备
可比性。基线和 candidate 均须先用 v1 `VADD` 预填充同一 key 范围，分别覆盖热点聚合和唯一 key
场景，并记录 QPS、P50/P99、leader/follower、frame、fallback 与 backpressure。

**已完成验证：**

- 本地与 111 Linux：`make -B -C benchmark vemb_v16_cli_l0_ut` 通过。UT 覆盖 exact collision、
  leader/follower、bucket/follower/key slab/entry pool 耗尽、prepared draft 背压、乱序和重复
  response、generation rejection、fallback 和 abort fan-out；
- 本地：`vemb_v16_cli_deadline_ut` 覆盖首 leader deadline、follower 不延后、eager zero-delay、
  publish 后 residual pending 的立即 eligible，以及 L0 pending item/frame-byte 计数；
- 本地：`make -C clients/c all`、`make -C memtier_benchmark` 和 `git diff --check` 通过；
- 跨机：111 以专用 6395 `vemb_v16_server` 使用 CC `3/2/4`，112 使用 NC `7/6/8`。1 worker、1
  client、pipeline 8、32 次相同不存在 key 的 `VEMB_HANDLE` 请求，日志为
  `leaders=4 followers=28 frames=4 unique_items=4 fallback_v1=0`，32 个 `NOT_FOUND` completion
  全部返回。测试结束后专用 server 已停止。
- 跨机 deadline：同一专用 6395 server、`BATCH_REQUEST_SIZE=32`、pipeline 8、32 次相同不存在 key。
  `--vemb-v16-batch-max-delay-us=100` 得到 `frames=4, flush_deadline=4, flush_eager=0,
  flush_backpressure=0`；零值回归得到 `frames=4, flush_eager=4, flush_deadline=0`。两轮均为
  `leaders=4, followers=28, fallback_v1=0` 且 32 个 `NOT_FOUND` completion 全部返回；专用 server
  已停止。
- 跨机 batch A/B（2026-08-04）：111 专用 6395 server 使用 CC `3/2/4`，112 使用 NC
  `7/6/8`；`DIM=300`、实际 handle dereference、`threads=8`、`clients=4`、`pipeline=32`、
  `BATCH_REQUEST_SIZE=32`、`max-delay-us=0`，每组连续 3 次、每次 10 秒。v1 基线使用
  `--vemb-v16-batch-disable`，因此与 candidate 的请求、key 分布和 warm read 完全相同。
  server 为隔离测试配置 `proxy-io=1, supernode=1`，这些数值用于验证 batch 收益，不是整机峰值。

  | workload | v1 QPS (3 samples) | v2 QPS (3 samples) | QPS median delta | v1/v2 P99 median | v2 aggregation |
  |---|---:|---:|---:|---:|---|
  | 4 个已预填充热点 key，`R:R` | 2.307M, 2.298M, 2.318M | 4.131M, 4.095M, 4.103M | +77.8% | 0.399 / 0.255 ms | sample 2: 5,118,001 leaders, 35,830,223 followers, 1,279,632 frames |
  | 512 个已预填充顺序 key，`S:S` | 2.181M, 2.173M, 2.161M | 2.459M, 2.458M, 2.455M | +13.1% | 0.415 / 0.455 ms | sample 2: 24,583,456 leaders, 0 followers, 768,233 full frames |

  两个 v2 workload 的所有 worker 均为 `fallback_v1=0`、`flush_backpressure=0`，且 response
  status 为 `OK` 并完成 vector handle dereference。热点收益同时包含 CLI L0 coalescing 和
  v2 frame；唯一 key 收益只归因于 v2 frame，P99 略升说明满 32 item frame 的排队成本仍需结合
  低延迟参数（例如较小 batch size 或非零 deadline）单独取舍。

  高并行确认（单次 15 秒，不计入上述 3-sample median）：server 使用
  `proxy-io=21, supernode=21`，client 使用 `threads=16, clients=4, pipeline=32`。v1 为
  `4.542M QPS`、P99 `0.335ms`；v2 为 `7.787M QPS`、P99 `0.263ms`，QPS 提升 `71.4%`，
  `fallback_v1=0`、`flush_backpressure=0`。batch session 为每个 client channel 同时建立 v1/v2，
  该并发度占用 128 个 server channel，保持在当前 `VEMB_V16_MAX_CHANNELS=192` 上限内；因此
  不能将历史 `64x4` 配置直接用于这版双通道 batch A/B。

  该 A/B 的 standalone `vemb_v16_server` 和 1GiB warm manifest 不可与历史 v1 峰值直接
  比较。为校准该差异，随后以当前同步源码重建 `redis-server`，在 111/112 使用 4GiB warm
  manifest、server `taskset 0-47`、client `taskset 96-191`、`PIO=21`、`SNW=21`、
  `t=64,c=4,pipeline=32`、100,000 个已预填充 key，执行 30 秒 v1
  `--vemb-v16-batch-disable` workload，得到 `9.937M QPS`、P99 `0.735ms`、全部 handle
  dereference 成功且 `publish_fail=0`。这与文档此前的约 `9.9M` 基线一致；此前的
  `2.3M/4.5M/6.35M` 不是 v1 回归，分别受 `1/1` worker、64 channel 和 1GiB warm region
  的测试配置限制。

  以该 `9.937M` v1 场景评估 v2 时，不得将低并发热点 A/B 的 `+77.8%` 直接外推为
  `17M+`。第一版 CLI L0 只合并 wire item 和 server job；memtier 对每个 follower 仍执行一次
  1200B handle dereference，因此高并发下仍受 CLI warm-region copy 限制。未共享 vector 时，
  唯一 key 的实测 `+13.1%` 给出约 `10.9--11.4M QPS` 的合理目标，热点 key 也应先按
  `10.5--12M QPS` 评估。下一步在 SDK 为每个 L0 group 只执行一次 dereference 并把只读临时
  vector view fan-out 给 leader/follower；这会同时降低 CLI copy bytes 和 SVE load。共享后热点
  场景的收益必须以同一 `redis-server + 4GiB + 256 channel + taskset` A/B 实测，当前不承诺
  固定 QPS 上界。

  SDK shared-vector 校准（2026-08-04）已完成，全部使用上文的 `redis-server + 4GiB + taskset`
  基线、`PIO=21`、`SNW=21`、`t=64,c=4,pipeline=32`、30 秒真实 handle dereference：

  | workload | v1 QPS | v2 shared QPS | QPS delta | P99 v1/v2 | vector sharing |
  |---|---:|---:|---:|---:|---|
  | 100,000 随机已预填充 key | 9.937M | 10.761M | +8.3% | 0.735 / 0.751 ms | 322,743,689 leader、50,103 follower；shared reads 322,743,689，几乎无可合并 read |
  | 4 个随机热点 key | 12.631M | 51.728M, 51.507M | +309.5%, +307.8% | 0.535 / 0.191 ms | first sample: 193,925,678 reads 对 1,551,558,496 logical fan-out，约 8:1 copy 压缩 |

  32-caller same-key cross-node smoke 进一步验证 `leaders=1, followers=31,
  shared_vector_reads=1, shared_vector_bytes=1200, shared_vector_fanout=32`。三次 shared workload
  日志均未出现非零 `fallback_v1`、`flush_backpressure`、shared read failure 或非 `OK` completion。
  热点 QPS 不能推广到唯一 key 或一般业务分布，它反映的是 L0 同 key group 的物理 vector copy 被
  消除后的上限；唯一 key 结果仍是常规端到端预期。

  最终实现将早期的每 L0 entry vector buffer 收敛为每 SDK session 一个 `dim` 大小的 shared
  scratch 和一个 direct-v1 scratch，view 只在同步 callback 内有效。以该最终分配方案重新同步、
  重建并复测：4-key 热点为 `51.458M QPS`、P99 `0.191ms`，累计 `192,942,836` shared reads
  向 `1,543,695,104` logical callers fan-out，`read_failures=fallback_v1=flush_backpressure=0`；
  100,000 随机 key 为 `10.695M QPS`、P99 `0.759ms`，累计 `320,767,513` shared reads、仅
  `49,703` followers，错误和回退计数同样为零。该结果与 scratch 优化前的 `51.5--51.7M` 热点、
  `10.761M` 随机 key 结果一致，确认不再随 L0 capacity 线性预分配 vector 内存，且正常读路径
  没有实质吞吐回归。

  10,000 随机已预填充 key 的中等热点测试使用相同的 256 logical client channel、30 秒和
  `BATCH_REQUEST_SIZE=32`：v1 为 `13.859M QPS`、P99 `0.479ms`；v2 shared 为
  `15.298M QPS`、P99 `0.559ms`，吞吐 `+10.4%`。v2 记录 `457,899,272` leaders、
  `710,552` followers（约 0.155% logical reads 可合并），所有 shared read、fallback 和
  backpressure error 计数为零。因此该 key cardinality 的主收益是 v2 frame/proxy 调度，
  而不是 shared vector copy；P99 增加表明延迟敏感场景仍应另行调整 batch size/deadline。

  注意当前 memtier runner 在创建 batch session 前仍创建一组原 v1 runner channel，batch session
  自身再创建 v1+v2，故 v2 benchmark 暂时占用每 logical client 三个 server channel。连续在同一
  server 实例先跑 256-channel v1 再跑 v2，会触及当前 1024 channel 容量并使后者无法开始；本次
  A/B 通过在两次 workload 间重启专用 server 完成。这个额外 channel 占用是 runner 资源效率问题，
  不影响 SDK session 的双通道模型，但应在后续将 batch 模式下未使用的 `all_channels` 创建移除。

**当前限制和遗漏：**

- 尚无 session 层的可控 v2 poll error、`STALE_TOPOLOGY` 和 v1 ring-full fault-injection UT；现有
  L0 UT 覆盖状态机，但这些 transport failure 分支仅由代码审查和正常跨机 smoke 覆盖；
- 尚无“v1 ATTACH 成功、v2 ATTACH 被拒绝”的 session-open fault-injection UT；当前实现由
  `vemb_v16_aeron_batch_client_open_remote()` 的统一失败路径保证关闭 v1 并返回 `NULL`，需要在
  ATTACH transport 增加可控拒绝 seam 后补齐该断言；
- deadline 的调度精度受 owner worker 的 `poll()` 周期约束；当前 memtier busy-spin 每轮检查，其他
  consumer 必须以 `poll()` 或 `next_flush_deadline_ns()` 驱动自己的事件循环；
- 本阶段不包含 L1 completed vector cache、proxy L2、mixed workload batch admission、local aeron
  v2 open，亦未进行扩容期间的真实迁移回归。

### 21.4 验收标准

**协议和资源：**

- v1 client/v1 server、v1 client/v2 server、v2 client/v1 server 都可用 v1 完成请求；
- v2 成功后，`BATCH_REQUEST_SIZE=1/8/32/128` 的 `effective_batch_size` 与 ATTACH response
  一致；item/byte 任一上限达到即 flush；
- 跨机 smoke 至少连续建立两个 v2 channel，第二个必须验证非零 UB offset 的 request/response
  descriptor header；
- 连续两个 v1 channel 的 request/response ring header 必须有效；在 v1 分配后建立 v2 channel
  也必须通过，覆盖两种 allocator 共享 UB pool 的非零 offset；
- v1 ring、v2 descriptor ring 与 request/response arena 的 resource bytes 必须为 64B 倍数；
  `max_batch_bytes` 的非对齐请求必须在 ATTACH response 中返回向上对齐后的协商值；
- channel 关闭和 ATTACH 任一失败路径均释放四段 v2 UB 资源；热路径无 malloc，也不因 client
  声明的 size 临时扩大 pool 或 arena。

**正确性：**

- 仅 `VEMB_HANDLE + STABLE + epoch match` 进入 v2；其他 op 一律走 v1；
- 同一 batch 中 N 个相同 key 经 L0 后 server 只看到一个 item，所有 follower 都得到 leader
  的 status/handle，且 caller 原始顺序不变；
- L2 hit 不创建 SuperNode job；L2 miss 的 batch response 与逐项 v1 response 完全一致；
- 非法 length、offset、item count、descriptor generation 或已关闭 channel 必须得到可观测的
  error/fallback，不得让 waiter 永久挂起。

**扩容和缓存：**

- 扩容开始前 proxy 完成 quiesce；扩容窗口的 v2 batch frame、L1 hit、L2 hit 计数均为零；
- cutover、source-GC、tombstone 和并发 `VADD`/`VREM` 压力下，cache 不得返回旧 source
  location 或已删除 key；
- `BATCH_FALLBACK_LEGACY` 保留每个 caller 的完成身份并经 v1 成功重放；扩容完成、新 epoch
  发布和 re-ATTACH 之前，v2 不得恢复。

**构建、回归和性能：**

- 单测：新增 `vemb_v16_batch_request_ut`，并扩展 `vemb_v16_aeron_ring_ut`、migration control、
  remote-meta 和 cross-node read-verify；
- 构建：`make -C src vemb_v16_server`、`make -C clients/c`、`make -C memtier_benchmark`；
- A/B：对 `BATCH_REQUEST_SIZE=1/8/32/128` 记录 QPS、P50/P99、实际 frame item/byte 分布、
  L0/L1/L2 hit、SuperNode miss item、fallback、CLI vector copy bytes 和每 channel UB 内存；
- 只有在 read-verify、迁移回归和资源泄漏检查均通过后，才把 v2 batch 用于跨机性能结论。

### 21.5 后续调优与跨 session shared-L0 checkpoint

本节是 shared-vector SDK 完成后的下一轮实施顺序。每完成一个 checkpoint，必须将标题中的
`[ ]` 改为 `[x]`，并在该 checkpoint 末尾补充日期、提交前工作区状态、实际命令、结果、未通过
项目及后续阻塞；这样从文档恢复时可直接从首个未完成 checkpoint 继续。不得只依据代码已写入
标记完成，必须先通过本节定义的 UT 和跨机验收。除非另有明确要求，整个过程不自动提交代码。

#### CP-0 `[x]` 修正 batch benchmark 的冗余 channel

测试代码基线：`6249f28`；验收结果包含工作区未提交变更。

**目的：** 当前 memtier 在 batch session 建立前先创建 `all_channels`，随后每个 batch session
又创建自己的 v1+v2 channel。因此 batch benchmark 暂时为每个 logical client 分配三个 server
channel，其中第一组在 batch 路径不参与 publish/poll。它浪费 UB，且连续先跑 256-channel v1、
再跑 v2 时会触及 1024 channel 容量。

**实施：** 在 `aeron-cross-node + pure VEMB_HANDLE + batch enabled` 分支中，跳过未被 batch
session 使用的 `all_channels` 创建、warm mapping 和 teardown；batch session 内的永久 v1 fallback
及 v2 batch channel 保持不变。v1 baseline、mixed workload、`VREM`、`VSIM_*` 和 local aeron
继续使用原 runner channel。

**验收：**

- 新增或扩展 runner UT，断言 batch 模式每 logical client 只 ATTACH 一次 v1 和一次 v2；
- `t=64,c=4` 的 v2 实际 server channel 占用为 512，而不是 768；同一专用 server 中连续运行
  v1 256-channel 与 v2 512-channel 不再需要重启；
- 4-key smoke、100k random read、`--vemb-v16-batch-disable` baseline 及现有 SDK/L0 UT 通过；
- 记录修改前后的 attach 数、UB bytes、QPS 和 P99，确认移除冗余 channel 不改变请求语义。

**完成记录（2026-08-04，工作区未提交）：**

- 实现：新增 `vemb_v16_aeron_runner_plan_channels()`，batch admission 后不再创建
  `all_channels`；worker 在 batch 模式按 `batch_clients` 驱动逻辑 channel，非 batch 模式仍只使用
  legacy channel。新增 `vemb_v16_aeron_runner_plan_ut`，覆盖 pure cross-node batch、local、mixed、
  `VSIM`、`VREM` 与 `--vemb-v16-batch-disable` 的 resource plan。
- 本地 UT/build：`make -B -C benchmark vemb_v16_aeron_runner_plan_ut`、
  `vemb_v16_cli_l0_ut`、`vemb_v16_cli_deadline_ut` 和 `make -C memtier_benchmark` 均通过；111/112
  已通过 `sync_changed_code_to_peer.sh` 同步，112 也完成 runner-plan UT 与 memtier 重建。
- 跨机连续验证：fresh 111 6395 server 上先运行 4-key、`t=64,c=4,pipeline=32` 的 256-channel
  v1 baseline，再不重启运行同配置 v2。server 日志为 `ATTACH v1=513`（预填充 1 + v1 baseline
  256 + v2 session 256）、`ATTACH v2=256`，故 v2 自身准确占用 256 对 v1/v2 channel，即 512 个
  server channel，而不是旧路径的 768 个。v1 为 `12.197M QPS`、P99 `0.503ms`；v2 为
  `49.121M QPS`、P99 `0.199ms`，worker completion 均为 `OK`、handle dereference failure 为零。
- fresh server 的 100k 已预填充 random v2 read 为 `10.324M QPS`、P99 `0.783ms`，
  `fallback_v1=flush_backpressure=shared_vector_read_failures=0`。该 5 秒功能样本只验证资源和
  非热点读路径；30 秒、多重复的性能结论留给 CP-1 矩阵。
- UB 节省：DIM=300 的冗余 v1 request/response ring 原始 mapping 分别为 `376,960B` 和
  `32,896B`；allocator 按 4KiB 向上对齐后分别占用 `380,928B` 和 `36,864B`，每 pair
  `417,792B`。移除 256 个未使用 pair 后，request/response UB 合计少用 `106,954,752B`（102MiB）。
- 未通过项/阻塞：无。CP-1 之前不应将上述 5 秒样本当作参数最优或长期吞吐结论。

#### CP-1 `[ ]` `BATCH_REQUEST_SIZE` 与 pipeline 参数矩阵

**前置：** CP-0 完成。固定 `max-delay-us=0`、真实 `VEMB_HANDLE` dereference、4GiB warm manifest、
server `taskset 0-47`、client `taskset 96-191`、`PIO=21`、`SNW=21` 和 30 秒运行时间。每个新候选
默认只运行一次 v1/v2 对照；只有样本失败、非全命中、fallback/backpressure 非零或明显偏离已建立
稳定范围时才重跑。此前已有的重复结果继续保留为稳定性证据。

**矩阵：**

```text
BATCH_REQUEST_SIZE: 1, 8, 32, 64, 128
pipeline:           8, 32, 64, 128
key distribution:   10k uniform, 100k uniform
```

每个 key range 均先以 v1 `VADD` 顺序预填充；每个 candidate 对照同参数
`--vemb-v16-batch-disable` v1。若资源或时间限制需要裁剪，先完整覆盖 `32/64/128` pipeline 和
`8/32/64/128` batch size，再补边界组合。

**记录与验收：**

- 记录 QPS、P50/P99、实际 frame item/byte histogram、leaders、followers、follower/logical ratio、
  shared reads、physical/logical vector bytes、fallback、backpressure 和 non-OK completion；
- 明确区分 frame 降本与 shared-vector 降本：额外运行 `VEMB_AERON_SKIP_HANDLE_READ=1` 对照只用于
  归因，不可替代真实 handle read 结论；
- 验证当 `pipeline < BATCH_REQUEST_SIZE` 且 delay 为零时，实际聚合窗口主要受 pipeline 限制；
- 选择每个 workload 的吞吐优选点和满足延迟预算的优选点，不能只报告单一最高 QPS。

**范围更新（2026-08-04）：** 后续 CP-1、CP-2 和性能复测不再新增 4-key 场景，只覆盖 10k 与
100k key range。此前 4-key 数据只作为极端热点历史样本保留，不能参与参数选择或对一般 workload
的外推。

**执行策略更新（2026-08-04）：** 为控制实验耗时，撤销“每个候选固定三重复”的要求。10k v2 r3、
100k v1/v2 r3 不再执行；已完成的 r1/r2 与受控 r3 仅用于验证 `batch=32,pipeline=32` 的稳定性。
其余参数扫描采用一次有效的 30 秒 v1/v2 对照，并遵守 fresh server 的停止后/启动后各 5 秒稳定等待。

**进行中记录（2026-08-04，工作区未提交）：**

- 开始本 checkpoint 前已重新通过 `make -B -C benchmark vemb_v16_cli_l0_ut
  vemb_v16_cli_deadline_ut vemb_v16_aeron_runner_plan_ut`、三个对应 UT、`make -C memtier_benchmark`
  与 `git diff --check`。随后使用 `scripts/sync_changed_code_to_peer.sh` 同步 111/112；41 个候选
  文件均为 `same`，两端执行版本一致。
- 先完成 10k uniform、真实 `VEMB_HANDLE` read、`t=64,c=4`、5 秒的筛选。`pipeline=32` 下，
  batch `8/32/64/128` 的 v2 分别为 `16.978/17.743/17.734/17.704M QPS`，对应 v1
  `18.579/18.549/18.579/18.482M QPS`；batch 8 的 P99 为 `0.839ms`，其余为
  `0.519-0.527ms`。固定 batch 32 时，pipeline `8/32/64/128` 的 v2 分别为
  `17.657/17.743/17.471/17.501M QPS`，P99 为 `0.143/0.519/4.863/33.535ms`。所有筛选
  样本 `fallback_v1=0`、`flush_backpressure=0`；10k uniform follower 很低，不能从该场景期待
  batch 聚合收益。该筛选只用于缩小 30 秒矩阵范围，不能作为长期性能结论。
- 完成一个 clean 4-key uniform v2 样本：fresh 111 `6395` server、预填充后在 112 运行
  `t=64,c=4,pipeline=32,BATCH_REQUEST_SIZE=32,max-delay-us=0` 30 秒，真实 read 全部命中，得到
  `51.013M QPS`、P99 `0.191ms`。汇总为 `leaders=191,267,864`、
  `followers=1,339,025,672`、`frames=47,821,673`、`unique_items=191,267,864`、
  `shared_reads=191,267,864`、`shared_fanout=1,530,293,536`，且 fallback/backpressure 均为零。
- 测试编排限制已确认：server 的 channel slot 在 client close 后不会在同一进程内复用。因此同一
  server 不能连续承载多次 `256` batch-session（每次使用 `512` channel）的样本；后续每一个
  v1/v2 repeat 都必须 fresh 6395 server 并重新顺序预填充，完成后立即将结果写入本节。此前一个
  自动循环出现 server lifecycle 重叠，相关 TSV 已判无效，不纳入任何结论。
- 未完成：每个 v1/v2 candidate 的 30 秒三重复、完整 `10k/100k` 矩阵、frame item/byte
  histogram、`VEMB_AERON_SKIP_HANDLE_READ=1` 归因对照和 workload 优选点；因此 CP-1 保持 `[ ]`。

**增量样本（2026-08-04）：** 按更新后的范围完成 fresh 6395 server、顺序预填充 10k key、
`t=64,c=4,pipeline=32` 的 v1 30 秒 baseline。真实 `VEMB_HANDLE` read 全部命中且所有 64 个
worker 已 join，结果为 `13.859M QPS`、P99 `0.479ms`，handle dereference failure 为零。下一步在
fresh server 上运行同参数 `BATCH_REQUEST_SIZE=32,max-delay-us=0` v2，完成即追加记录。

**增量样本（2026-08-04，10k v2 repeat 1）：** fresh server 再预填充 10k key 后，同一
`t=64,c=4,pipeline=32,BATCH_REQUEST_SIZE=32,max-delay-us=0` v2 得到 `15.166M QPS`、P99
`0.575ms`，相比上述单次 v1 baseline 吞吐 `+9.4%`、P99 `+0.096ms`。全部 read 命中、64 个 worker
均已 join，`fallback_v1=0`、`flush_backpressure=0`、shared-vector read failure 为零；汇总
`leaders=454,024,949`、`followers=704,331`、`frames=14,210,290`、
`shared_reads=454,024,949`、`shared_fanout=454,729,280`。该结果仍需 repeat 2/3 验证。

**增量样本（2026-08-04，100k v1 repeat 1）：** fresh 6395 server、顺序预填充 100k key 后，
同一 `t=64,c=4,pipeline=32` v1 baseline 得到 `9.772M QPS`、P99 `0.743ms`。真实 read 全部命中，
64 个 worker 全部 join，handle dereference failure 为零；下一步用 fresh server 执行对应 v2。

**增量样本（2026-08-04，100k v2 repeat 1）：** fresh server 重预填充后，`BATCH_REQUEST_SIZE=32,
max-delay-us=0` 的 v2 得到 `10.513M QPS`、P99 `0.775ms`，相对该单次 v1 baseline 吞吐 `+7.6%`、
P99 `+0.032ms`。全部 read 命中、64 个 worker 已 join、handle dereference failure、fallback、
backpressure 及 shared-vector read failure 均为零；汇总 `leaders=315,317,828`、
`followers=48,988`、`frames=9,855,213`、`shared_reads=315,317,796`、
`shared_fanout=315,366,784`。该结果仍需 repeat 2/3 验证。

**增量样本（2026-08-04，10k v1 repeat 2）：** fresh server 重预填充后，同一 v1 baseline 为
`13.903M QPS`、P99 `0.479ms`，所有 read 命中且 64 worker 已 join，handle dereference failure
为零。与 repeat 1 的 `13.859M QPS` 相差 `0.3%`，当前 baseline 稳定；下一步运行对应 v2 repeat 2。

**增量样本（2026-08-04，10k v2 repeat 2）：** fresh server 重预填充后，同一 v2 candidate 为
`15.224M QPS`、P99 `0.575ms`，全部 read 命中且 64 worker 已 join；fallback、backpressure 与
shared-vector read failure 为零，`followers=706,913`。与 v2 repeat 1 的 `15.166M QPS` 相差
`0.4%`，当前 v2 吞吐稳定；下一步继续 100k repeat 2。

**增量样本（2026-08-04，100k v1 repeat 2）：** fresh server 重预填充后，v1 baseline 为
`9.798M QPS`、P99 `0.743ms`，全部 read 命中、64 worker 已 join，handle dereference failure
为零。与 repeat 1 的 `9.772M QPS` 相差 `0.3%`，当前 100k v1 baseline 稳定。

**增量样本（2026-08-04，100k v2 repeat 2）：** fresh server 重预填充后，v2 candidate 为
`10.568M QPS`、P99 `0.783ms`，全部 read 命中且 64 worker 已 join，fallback、backpressure 与
shared-vector read failure 均为零。与 v2 repeat 1 的 `10.513M QPS` 相差 `0.5%`；当前 100k v2
吞吐稳定，完成 r3 后可汇总与 v1 的 A/B 结果。

**无效样本（2026-08-04，10k v1 r3 首次尝试）：** 预填充的 10k VADD 正常完成，但随后 read
仅 publish `128` 个请求，各 worker `ops_done=0`，却持续轮询到无法匹配的 response，未生成
`Totals`。该样本不计入 r3，也不作为性能或正确性结论。重试前必须确认旧 6395 pid 已退出、启动
新 server 后额外等待并检查 pidfile，再重新预填充和运行；若复发，转入 v1 response-ring/req-id
生命周期专项定位。

**根因确认与有效样本（2026-08-04，10k v1 r3）：** 检查确认 111 没有第二个 6395 server，且
allocator 对每个 ATTACH ring 执行清零和 header 初始化。以“停止旧 server 后等待 5 秒、启动新
server 后再等待 5 秒”的受控生命周期重试后恢复正常：`13.780M QPS`、P99 `0.479ms`、全部 read
命中、64 worker 已 join、handle dereference failure 为零。该值与 r1/r2 相差不超过 `0.9%`，计为
有效 r3。现有证据表明首次异常是测试 harness 未等待 server proxy/UB 生命周期完全稳定，而不是可
复现的数据面 bug；后续所有 fresh-server sample 必须使用上述两段稳定等待。若该条件下复发，再按
response ring/req-id 生命周期 bug 处理。

**参数筛选（2026-08-04，10k，batch 8/pipeline 32）：** 按单次 30 秒策略、fresh server 与
两段 5 秒稳定等待完成真实 read，结果 `10.345M QPS`、P99 `1.559ms`，全部命中且 64 worker join，
fallback/backpressure 均为零。相对同 workload 的 batch 32（约 `15.2M QPS`、P99 `0.575ms`）显著
更差，因此 batch 8 已淘汰，不进入后续 delay/Zipf 候选。

**协商限制（2026-08-04）：** v2 ATTACH 的实际大小为
`min(client BATCH_REQUEST_SIZE, server vemb-v16-batch-request-size)`。当前 6395 server 未显式设置
后者，默认 `32`；因此 client `batch=64,pipeline=32` 的单次结果 `15.305M QPS`、P99 `0.567ms` 实际
仍是 effective batch 32，不能作为 batch 64 结论。同时 `pipeline=32` 也限制单 channel 在 zero-delay
下最多形成 32-item 聚合。后续测试真实 batch 64/128 时，必须将 server 上限同步提高到 64/128，且
选择不小于该值的 pipeline；重新 ATTACH 后以 response 的 `effective_batch_size` 作为唯一判据。

**参数筛选（2026-08-04，10k，真实 batch 64/pipeline 64）：** 111 server 设为
`vemb-v16-batch-request-size=64` 后，112 `vemb_v16_batch_attach_smoke` 确认
`effective_batch_size=64`；fresh server、两段 5 秒稳定等待、重预填充后，真实 read 为
`15.759M QPS`、P99 `1.271ms`，全命中且 64 worker join。相对 batch 32/pipeline 32 的约
`15.2M QPS`，吞吐仅小幅提高但 P99 明显变差；暂不作为低延迟默认值，继续验证真实 batch 128。

**参数筛选（2026-08-04，10k，真实 batch 128/pipeline 128）：** 111 server 设为 128 后，
ATTACH smoke 确认 `effective_batch_size=128`。单次真实 read 为 `15.371M QPS`、P99 `2.431ms`，
全命中、64 worker join、fallback/backpressure/read failure 均为零。吞吐未超过真实 batch64 的
`15.759M QPS`，且 P99 进一步恶化，故 batch128/pipeline128 不作为 10k 低延迟或吞吐优选点。

#### CP-2 `[ ]` deadline-aware `max-delay-us` 扫描

**前置：** CP-1 完成，并从其结果选择有代表性的 batch size/pipeline 组合。固定其它参数，扫描：

```text
max-delay-us: 0, 2, 5, 10, 20, 50, 100
```

覆盖 10k 和 100k uniform。每个值运行 3 次 30 秒；继续使用
`vemb_v16_monotonic_ns()`，不得用 wall clock 或 sleep 改变 SDK 的 deadline 语义。

**验收：**

- `flush_deadline` 随非零 delay 出现，`flush_eager`、`flush_full`、deadline 和 backpressure
  分类准确；新增 UT 覆盖候选 delay 边界和 ring pressure retry；
- 报告 follower ratio、平均 frame items、QPS、P99，找出 P99 预算内的最大有效 delay；
- 若 delay 仅增加排队、未提高 follower ratio 或降低 frame 成本，则不作为默认值；
- 0us 回归保持当前 eager 行为和结果范围。

#### CP-3 `[ ]` 可复现 Zipf/热点基准

**前置：** CP-1、CP-2 完成。先为 memtier key generator 增加分布 UT，验证 Zipf 频率随 rank
单调下降、给定 seed 可重复、key range 边界正确；只有 UT 通过才运行性能测试。

**矩阵：**

```text
key count: 10k, 100k
key pattern: Z:Z
Zipf s: 0.8, 0.99, 1.1, 1.2
```

通过 `--key-pattern=Z:Z --key-zipfian-s=<s>` 驱动；所有样本使用相同 seed、预填充数据和阶段 1
选定的 batch/pipeline/delay 策略。

**验收：**

- 报告 top-key request share、leaders、followers、平均/最大 group fan-out、shared vector copy ratio、
  QPS、P99 和 v1 baseline delta；
- 以热点度曲线解释收益，禁止用 4-key 极端结果外推一般业务；
- 若 parser/generator 尚不接受 `Z`，该 checkpoint 阻塞并记录错误与修复点，不能静默退化为 uniform。

#### CP-4 `[ ]` 分片 single-owner shared-L0 聚合器

**前置：** CP-0 至 CP-3 完成，调参结果用于确定 shard 数、ring 深度、vector pool 容量和默认
batch/deadline。第一版不实现多个 worker 直接 CAS 操作同一全局 L0 hash table：follower 追加、
entry 回收、vector 生命周期和 callback thread affinity 使严格 wait-free 的共享 hash 复杂且难以
验证。

**目标架构：** 按 `hash(final_key) -> shard` 路由。每个 shard 由一个 pinned aggregator owner
thread 独占其 L0、v1 fallback channel 和 v2 batch channel；producer worker 通过固定容量的
per-worker/per-shard SPSC ingress ring 提交，聚合器通过 per-shard/per-worker SPSC egress ring
将 completion 送回原 worker。这样 ingress/egress 操作固定步数、无锁且可作为 wait-free 的
bounded fast path；L0 本身是单 owner 数据结构，无并发 hash/reclamation 问题。ring 满、vector pool
耗尽、topology 非稳定、epoch 变化或不支持操作均立即走现有 local v1 路径。

```text
CLI worker -- SPSC ingress --> hash shard aggregator -- v1/v2 --> server
    ^                                  |
    +--------- SPSC completion --------+  (original worker runs callback)
```

聚合 group 在创建时从固定 `shared_vector_pool` 获取一个 `dim` buffer；leader response 只 copy 一次，
completion 描述符引用 `(vector_slot, generation)`，所有 worker callback 消费完后以 refcount 回收。
不能把当前 per-session scratch view 跨线程保存，也不能由 leader thread 直接调用其它 worker 的 callback。

**建议首版配置：**

```text
VEMB_V16_SHARED_L0_MODE=off|aggregator   (default off)
VEMB_V16_SHARED_L0_SHARDS=4 or 8
VEMB_V16_SHARED_L0_INGRESS_DEPTH=32
VEMB_V16_SHARED_L0_EGRESS_DEPTH=64
VEMB_V16_SHARED_VECTOR_SLOTS=256 per shard
```

**无锁 first-version 细化（2026-08-04）：** shared-L0 不引入全局并发 hash table。每个 shard owner
独占一个现有 C `vemb_v16_cli_l0`、一个 v1 fallback channel、一个 v2 batch channel 和一个固定
vector pool；同 key 经 `hash(final_key) & (shards - 1)` 始终到同一 owner，故不会因分片损失
coalescing。worker 到 shard、shard 到 worker、worker 到 shard 的 vector ack 分别为固定容量 SPSC
ring；ring 满、topology 非 stable、epoch 变化、非 `VEMB_HANDLE`、pool/L0 耗尽均立即走原 worker
v1 路径。每个 ring 的 head/tail 使用 acquire/release 原子操作，无 mutex、无 MPSC CAS。

completion 只在原 worker 上执行 callback。shard 只做一次 handle dereference 并分配
`(vector_slot,generation)`；worker callback 返回后经自己的 SPSC ack ring 归还 slot。owner 汇总 ack
后回收 slot，避免多个 worker 对同一 refcount 做原子减法和 cacheline 争用。vector view 仅在 callback
期间有效，禁止跨 callback 保存。first-version 从 8 shard、ingress depth 64、egress depth 128、每 shard
512 vector slots起步；高负载 busy-poll，空闲才由 non-empty 通知进入等待。

**实施顺序（2026-08-04）：**

1. 先实现 C 的 SPSC ingress/egress/ack ring 与 owner-only vector pool，新增 UT 覆盖 wrap、满/空、
   generation ABA、跨 worker ack、close/drain；此阶段不改变 runner。
2. 在 SDK 增加 shared aggregator manager，创建/关闭 shard 的 v1/v2 channel，复用现有 L0；保持 feature
   default off，并增加 topology/容量 fallback 计数。
3. runner 开启时将 stable `VEMB_HANDLE` 送 ingress、从原 worker completion ring 取回并更新原 pending；
   所有其它 op 和 fallback 继续旧路径。
4. 通过单元测试、cross-node read-verify 后，用 10k/100k 比较 local-L0 与 shared-L0 的 leader/follower、
   physical vector read、QPS/P99、server channel 和 UB bytes。

**Phase 1 完成记录（2026-08-04，工作区未提交）：** 新增
`clients/c/internal/vemb_v16_shared_l0.h` 与 `clients/c/vemb_v16_shared_l0.c`。实现固定容量、
单 producer/single consumer 的 ingress/completion/ack ring，以及仅由 shard owner 操作的
generation-tagged vector pool；worker ack 后才回收 vector slot，过期 generation 和重复 ack 被拒绝。
新增 `benchmark/vemb_v16_shared_l0_ut.c`，覆盖 ring 满/空/FIFO、vector 多 consumer ack、回收和
generation ABA。`make -B -C benchmark vemb_v16_shared_l0_ut vemb_v16_cli_l0_ut
vemb_v16_cli_deadline_ut`、三个 UT、`make -C clients/c`、`make -C memtier_benchmark` 与
`git diff --check` 均通过。

该阶段只提供无锁 transport/vector 生命周期基础，尚未创建 aggregator thread、尚未接入 runner，
feature 未开启且当前跨机行为不变。Phase 2 需实现 shard manager 和 close/drain，再进行其专门 UT；
不得将本记录视为 CP-4 完成。

**Phase 2 完成记录（2026-08-04，工作区未提交）：** shared-L0 manager 现为每个 shard 创建一个
single-owner L0、每 worker 的 ingress/egress SPSC ring，并提供 worker submit、owner drain、
prepare/publish/complete 和原 worker completion poll API。UT 增加两个 worker 对相同 key 路由到同一
shard 的断言：owner drain 两个 ingress 后只形成一个 batch item，完成后两个 caller 分别从自己的
egress ring 收到原 response。再次通过 `make -B -C benchmark vemb_v16_shared_l0_ut`、UT、
`make -C clients/c`、`make -C memtier_benchmark` 与 `git diff --check`。

该 manager 尚无 owner pthread、未管理 v1/v2 channel，egress overflow 仅记数且不允许接入生产
runner；因此 cross-node 行为仍未改变。下一阶段必须先实现 egress capacity/close-drain 的无丢失
契约和 owner channel lifecycle UT，之后才接 runner。

**Phase 3 完成记录（2026-08-04，工作区未提交）：** 每个 shard 增加 owner-only、固定容量的
completion outbox（`1024 + 4096` 个 logical completion）。`owner_complete` 现在先以不消费
response identity 的 L0 peek 获得 group fanout；outbox 空间不足时返回 `0`，调用方必须保留同一
server response，待 egress flush 后重试。空间足够时才 resolve/finish L0 group，并把 leader/follower
全部复制到 outbox；成功返回 `1`。`owner_flush_completions` 再将 outbox FIFO 投递到原 worker 的
SPSC egress，目标 egress 满时停止，不再丢弃 completion。L0 admission error 同样经 outbox 返回；
outbox 已满时 owner 不再消费 ingress，避免消费后无处保存 error completion。

`vemb_v16_shared_l0_ut` 现验证正常 coalesced completion 必须显式 flush 才对 worker 可见，以及
egress depth 为 4 时五个 completion 先投递四个、第五个保留，worker 取走一个后继续 flush 并按
`300..304` FIFO 完整返回。`make -B -C benchmark vemb_v16_shared_l0_ut vemb_v16_cli_l0_ut
vemb_v16_cli_deadline_ut`、三个 UT、`make -C clients/c`、`make -C memtier_benchmark` 与
`git diff --check` 均通过。

该版刻意使用单个 shard-wide FIFO outbox，因此一个已满 worker 的 completion 会暂时阻塞该 shard
后续其它 worker 的 outbox 项；这是无丢失优先的 first-version tradeoff，后续可改为每 worker 的
owner-only outbox。

**Phase 4 完成记录（2026-08-04，工作区未提交）：** manager 增加 release/acquire 的
`stop_submissions` gate，关闭开始后 `submit` 立即拒绝新 ingress，但不取消已接受工作。
`shard_drained` 仅在该 shard 的所有 ingress/egress SPSC ring、L0 active group 和 owner outbox 都为空时
返回真；调用方必须先 stop submissions、持续 drain/poll/flush，之后才能 destroy manager。UT 额外覆盖
一个带在途 group 的关闭：stop 后拒绝新请求，完成和 outbox flush 后仍因 worker egress 未消费而未 drain，
worker 取走 completion 后才成为 drained。完整构建/UT 命令与 Phase 3 相同，均通过。

尚未实现 owner pthread、v1/v2 channel 生命周期、vector slot 实际映射或 runner 接入，不能用于跨机实验。

**Phase 5 完成记录（2026-08-04，工作区未提交）：** 每个 shard 现在可由 owner 直接创建永久
v1 Aeron channel（含 warm-region mapping）与 v2 batch channel，不复用会自行拥有另一套 L0 的
`vemb_v16_aeron_batch_client`。v2 ATTACH、四段资源映射或 resource 查询失败都会关闭已开的 v1，
不允许产生 v1-only shared shard；返回值区分 v1 ATTACH、warm mapping、v2 ATTACH/mapping 与
resource-query 失败，供后续 runner 统计。关闭要求先 `stop_submissions` 且 `shard_drained`。

新增 `benchmark/vemb_v16_shared_l0_attach_smoke.c`。本地通过 `make -B -C benchmark
vemb_v16_shared_l0_ut vemb_v16_shared_l0_attach_smoke vemb_v16_cli_l0_ut
vemb_v16_cli_deadline_ut`、三个 UT、`make -C clients/c`、`make -C memtier_benchmark` 与
`git diff --check`。随后以 `scripts/sync_changed_code_to_peer.sh` 同步 111/112；在 112 编译并运行
`./benchmark/vemb_v16_shared_l0_attach_smoke 192.168.90.111 6395`，成功创建 v1 channel `518` 和
v2 batch channel `519`，输出 `effective_batch_size=32 max_batch_bytes=65536`，确认四段 UB 资源创建、
warm mapping 与 stop/drain/close 均可用。

该 smoke 首次失败暴露 `shard_count=1` 被错误沿用 ring depth 的“至少 2”幂次校验。已改为 shard
允许任意正 2 的幂（包含 1），ring capacity 仍要求至少 2；UT 增加单 shard manager 创建验证。当前仍
未让 owner 使用 channel publish/poll 数据面，未实现 v1 fallback submit/response、deadline flush、
owner pthread、实际 shared vector slot/ack 或 runner 接入；不得据此运行 shared-L0 性能结论。

**Phase 6 完成记录（2026-08-04，工作区未提交）：** shard owner 现在可显式调用
`owner_flush_batch` 从其独占 L0 draft 发布一帧 v2 request，并用 `owner_poll_batch` 读取一帧 response。
response 先保存在 shard 固定 pending-response buffer；若 completion outbox 空间不足，当前 item 不消费、
整帧保留到下次 poll，保持无丢失。topology epoch 不匹配时返回 `STALE_TOPOLOGY` response，当前不会
偷偷改走 v1；扩容期间 shared-L0 本就不得启用，后续 runner 负责停止 admission 和 drain。

cross-node smoke 增加两个相同 final key 的 logical request：同一 worker ingress 后 owner drain 得到
一个 L0 leader，发布一个 physical v2 item，poll 后向 caller cookie `11/22` fanout，最后 stop/drain/close。
同步 112 并运行 `./benchmark/vemb_v16_shared_l0_attach_smoke 192.168.90.111 6395` 成功，协商结果仍为
`effective_batch_size=32 max_batch_bytes=65536`，且实际建立 v1 channel `520` 与 v2 channel `521`。
本地完整 UT/build 命令与 Phase 5 相同，均通过。

本阶段没有 deadline-aware owner flush、v1 fallback submit/poll、pinned owner pthread、vector pool 实际
handle dereference/ack，也没有 runner 接入；因此还不能进行 shared-L0 性能实验。下一阶段先实现 owner
thread 的 stop/drain loop 和其生命周期 UT，再把 stable `VEMB_HANDLE` 从 runner 路由进 ingress。

**Phase 7 完成记录（2026-08-04，工作区未提交）：** 实现每 shard 一个 owner pthread。启动前仍由
控制线程完成 channel ATTACH；启动后只有 owner thread 调用 drain、v2 flush、v2 poll 和 completion
outbox flush，worker 仍只操作各自的 SPSC ingress/egress。loop 使用 bounded `max_drain_items`、
`max_completion_items`，无工作时按配置的微秒级 sleep 让出 CPU。`stop_owner_threads` 先禁止 submit，
再等待 owner 在 ingress/L0/outbox/worker-egress 全部 drain 后退出；调用方在 join 期间必须继续 poll
worker completion，避免带未消费 egress 的错误关闭。

`vemb_v16_shared_l0_ut` 新增单 shard 的 idle start/stop/join 覆盖。cross-node smoke 改为不直接调用
owner 数据面：启动 owner thread 后仅提交两个同 key request，并从 worker egress 收到两个 completion，
再 stop/join/close。同步 112 后通过 `make -B -C benchmark vemb_v16_shared_l0_ut
vemb_v16_shared_l0_attach_smoke`、UT 和 `./benchmark/vemb_v16_shared_l0_attach_smoke
192.168.90.111 6395`；实际建立 v1 channel `522`、v2 channel `523`，协商
`effective_batch_size=32 max_batch_bytes=65536`。本地完整 UT、`make -C clients/c`、
`make -C memtier_benchmark` 与 `git diff --check` 均通过。

该 owner loop 尚未 pin CPU、没有 idle notification/epoll、没有 deadline-aware flush，且 v2 transport
failure、stale topology 和 oversized key 目前只保留已有 error/stale 语义，未接 v1 fallback。下一阶段应先
在 runner 增加 default-off 的 `VEMB_V16_SHARED_L0_MODE=aggregator` 路由，仅接纳 stable
`VEMB_HANDLE`；完成 worker pending/completion 关联和 close drain UT 后，再补 fallback 与 shared vector。

**Phase 8 完成记录（2026-08-04，工作区未提交）：** memtier Aeron runner 现支持默认关闭的
`VEMB_V16_SHARED_L0_MODE=aggregator`。channel plan 新增 shared-L0 分支，只有 cross-node、纯
`VEMB_HANDLE` read、非 VSIM/VREM、未 batch-disable 时才允许；其它 workload 显式报错，不会静默切换。
当前还要求 `VEMB_AERON_SKIP_HANDLE_READ=1`，因为 shared vector slot/ack 尚未接入，禁止在没有合法
worker vector view 时运行实际 handle dereference。

runner 创建一个跨全部 worker 的 8-shard manager，在启动 worker 前完成 v1/v2 attach 并启动 owner
threads。worker cookie 编码为 `(logical_channel << 32) | req_id`，shared completion 回原 worker 后精确
恢复原 pending ring；worker join 后再 stop/join owners、关闭各 shard channel。修复了 manager `submit`
成功返回 `1`、batch session 成功返回 `0` 的差异，避免 shared request 被误记为 publish failure。

本地已通过 `vemb_v16_aeron_runner_plan_ut`、`vemb_v16_shared_l0_ut`、`make -C clients/c`、
`make -C memtier_benchmark` 和 `git diff --check`。同步 111/112、重启 111 并等待 5 秒后，从 112 运行
2 秒 `t=2,c=2,pipeline=8`、10k uniform metadata-only shared smoke：两 worker 分别
`publish_ok/poll_got/ops_done=144120/143997`、`publish_fail=0`，总计 `144031.71 QPS`，p99 `0.343ms`，
正常完成 worker join。新 server 未预热 key，全部为 `NOT_FOUND`，故此结果仅验证 runner 路由、pending
关联和 close lifecycle，不作为 shared-L0 性能结论。

尚缺 runner 的 shared completion 专门 UT、预热后的 read-verify、v2 transport failure/stale topology/
oversized-key 的明确 fallback-or-fail 策略、deadline flush、CPU pinning，以及 shared vector pool/ack。
在这些完成前，shared-L0 mode 仍是 metadata-only 实验功能，不能参与正式 QPS 比较。

**Phase 8.1 完成记录（2026-08-04，工作区未提交）：** 将 runner completion cookie 的
`(logical_channel, req_id)` encode/decode 抽到 channel-plan header，`vemb_v16_aeron_runner_plan_ut`
覆盖非零 high/low 32-bit round-trip；与 shared-L0 UT 和 memtier build 一并通过。随后从 112 使用普通
cross-node VADD（`t=1,c=1,pipeline=16,requests=10000`）预热，写入 `10000` 次、`publish_fail=0`。

预热后运行 shared-L0 metadata-only read-verify（`t=2,c=2,pipeline=8`、10k uniform、2 秒）：worker 0
为 `publish_ok=poll_got=ops_done=157992`、`OK=104038`、`NOT_FOUND=53954`；worker 1 为
`158147/158147/158147`、`OK=100311`、`NOT_FOUND=57836`，无 `ERR`。总计 `158043.66 QPS`，
`102157.80 hits/sec`、`55885.86 misses/sec`、p99 `0.343ms`。随机预热覆盖约 64.6% key，故 hit/miss
比例符合预期；本结果验证 shared completion 到原 worker pending 的正确性和 close lifecycle，仍不用于
正式性能结论。

**Phase 8.2 完成记录（2026-08-04，工作区未提交）：** shared owner thread 已接入与普通 batch
client 一致的 deadline-aware flush。`vemb_v16_shared_l0_owner_options_t` 新增
`max_batch_delay_us`，runner 将 `--vemb-v16-batch-max-delay-us` 原样传入；零值保持每轮 eager
publish。非零值在启动 owner thread 前一次性初始化 `monotonic.h` 时钟，失败则 shared-L0 启动失败，
不退回 eager 或 v1。每个 shard 在首个 pending leader 时固定 deadline；在协商的
`effective_batch_size`、`max_batch_bytes` 任一上限到达时立即发布，否则仅在 deadline 到期后发布。
成功发布后若仍有 pending leader，新的 deadline 是 `now + delay`，使下一帧等待完整 delay；v2
pressure 时不消费 L0 draft，deadline 保留并在下一轮重试。

同时修正公共 deadline helper 的 progress 语义：此前仍有 pending leader 时错误地把 deadline 设为
`now`，会导致发布后下一轮立即 flush；现改为 `now + delay`。`vemb_v16_cli_deadline_ut` 新增此
边界的到期前/到期时断言。本地通过 `make -B -C benchmark vemb_v16_cli_deadline_ut
vemb_v16_shared_l0_ut vemb_v16_shared_l0_attach_smoke`、三个 UT、`make -C clients/c`、
`make -C memtier_benchmark` 与 `git diff --check`。

使用 `scripts/sync_changed_code_to_peer.sh` 同步 111/112 后，112 以
`./benchmark/vemb_v16_shared_l0_attach_smoke 192.168.90.111 6395 100` 建立 v1 channel `50`、v2
channel `51` 并完成 coalesced response、drain/join/close，输出
`effective_batch_size=32 max_batch_bytes=65536 max_batch_delay_us=100`。随后在 112 对现有 10k
预热集运行 metadata-only runner 验证（`t=2,c=2,pipeline=8,batch=32,max-delay-us=100`）：
`publish_ok/poll_got/ops_done=86760/86760/86760` 与 `86492/86492/86492`、无 publish failure 和 error，
总计 `173208.01 QPS`、p99 `0.335ms`。该次只证明 runner 参数传递和真实 owner deadline 路径，不与
eager 的 QPS 作性能结论。

当前 shared-L0 仍是 metadata-only：尚未将 owner 映射的 handle 解引用至 shared vector slot，也没有
worker ack 回收，因此 `VEMB_AERON_SKIP_HANDLE_READ=1` 仍是强制条件。尚缺运行期 v2 failure/
oversized-key 的明确 fail 语义、CPU pinning/idle notification，以及 per-worker outbox 消除单 shard FIFO
head-of-line blocking；这些完成前禁止正式 10k/100k QPS A/B。

**Phase 8.3 代码完成、跨机验证阻塞（2026-08-04，工作区未提交）：** shared-L0 manager 已接入
group-owned shared vector 生命周期。每个 shard 在 v1 warm mapping 与 v2 batch channel ATTACH 均成功后
创建固定 `512 x dim` vector pool；owner 在一个 `OK VEMB_HANDLE` group 上只调用一次
`vemb_v16_aeron_read_vector()`，并将相同 `(slot,generation)` 写入 leader/follower completion。worker 在
本线程完成 pending/accounting 后查看只读 slot 并向所属 shard 的 SPSC ack ring 回传；owner loop 每轮先
drain ack，最后一个 ack 才回收该 slot。`shard_drained()` 现在还要求 ack ring 为空且所有 vector slot 已
回收，防止 close/unmap 时 vector view 仍被 worker 使用。

每个 worker/shard ack ring 固定为 `8192` entries，覆盖当前 L0 的最大 follower fanout，避免 egress
ring（默认 128）与 ack 背压相互放大；若 `vemb_v16_cli_l0_finish()` 异常，已分配 slot 会同步归还。
runner 已解除 `VEMB_AERON_SKIP_HANDLE_READ=1` 的 shared-L0 强制限制：正常 shared mode 会以 owner
slot 构造临时 vector view，metadata-only 仍可显式用该环境变量运行。vector pool 建立失败使 shard
ATTACH 返回 `-6`，不会形成 v1-only shard。

本地通过 `make -B -C benchmark vemb_v16_shared_l0_ut vemb_v16_cli_deadline_ut
vemb_v16_aeron_runner_plan_ut`、三个 UT、`make -C clients/c`、`make -C memtier_benchmark` 与
`git diff --check`。代码已通过 `scripts/sync_changed_code_to_peer.sh` 同步 111/112，但跨机实际 vector
read 尚未完成：111 的 `/tmp/vemb_perf_warm_111.yaml` 一度缺失，已恢复并重启后 dataplane 正常；随后
确认 112 不存在 `/dev/obmm_shmdev5`、`6`、`7`、`8`。当前 SDK 的既定拓扑要求把 111 server 的
`/dev/obmm_shmdev1-4` 映射为 112 的 `5-8`，故 client 无法 mmap ATTACH 返回的 ring/warm resource，
单 shard smoke 返回 `rc=-2`。这是 UB peer-view/设备环境阻塞，并非 vector slot/ack 逻辑失败。

恢复 112 的 `5-8` UB view 后，依次执行 `make -C clients/c && make -C memtier_benchmark`，然后从
112 运行 `./benchmark/vemb_v16_shared_l0_attach_smoke 192.168.90.111 6395`，预热 10k，再运行不带
`VEMB_AERON_SKIP_HANDLE_READ=1` 的 `VEMB_V16_SHARED_L0_MODE=aggregator` 10k read-verify。验收要求
`handle_deref[ok]>0`、`fail=0`、无 ack failure、worker drain/join/close 成功；达到前 CP-4 仍不得作
性能 A/B 结论。

**Phase 8.4 完成记录（2026-08-05，工作区未提交）：** shared-L0 manager 新增 owner-side
`vemb_v16_shared_l0_stats_t` 与停止 owner thread 后的聚合 snapshot API。runner 在 owner stop 后输出：
`leaders`、`followers`、`avg_fanout`、`frames`、`unique_items`、`frame_bytes`、shared vector group
read/failure/bytes/fanout、pool exhausted、ack failures、v2 backpressure、non-OK 与 stale response。
其中 leader/follower 在 owner drain L0 submit 结果处累计；frame/item/byte 仅在 v2 publish 成功后累计；
non-OK/stale 统计物理 v2 response item，shared-vector fanout 统计收到同一有效 vector view 的 logical
completion 数。平均 fanout 为 `(leaders + followers) / leaders`。

正常计数均是 shard owner 私有写入。唯一例外是 worker 将 vector ack 写入自己的 SPSC ring 失败时，使用
异常路径原子计数；owner 发现 generation/slot 无效 ack 也会累计 failure。UT 增加 snapshot 的
leader/follower 断言；本地通过 `make -B -C benchmark vemb_v16_shared_l0_ut
vemb_v16_cli_deadline_ut vemb_v16_aeron_runner_plan_ut`、三个 UT、`make -C clients/c`、
`make -C memtier_benchmark` 与 `git diff --check`。当前仍没有 server/proxy 的 request/response UB byte
累计，不能将 CLI `frame_bytes` 等同于全链路 UB traffic；该数据面指标需要后续独立实现。

**实施子步骤：**

1. 定义公共 API、配置和固定 ring/vector-pool 生命周期；默认关闭，保持现有 session L0 行为；
2. 完成 SPSC ingress/egress、worker affinity completion 和 bounded fallback，增加 ring-full、close/drain、
   callback-on-owner-thread UT；
3. 将 current L0 挂接到 shard owner，验证跨 worker 同 key 仅一个 leader，hash collision 不合并，
   epoch/非 `VEMB_HANDLE` 不进入 shared path；
4. 接入 group-owned vector slot/refcount，验证一次 handle copy、多 worker fan-out、generation 防 ABA、
   vector pool 耗尽 fallback 和 close/reclaim；
5. 以 CP-1 至 CP-3 的 workload 重跑 local-L0 与 shared-L0 A/B，报告 server channel/UB bytes 降幅、
   shared ratio、QPS/P99 以及所有 fallback/error 计数。

**完成门槛：**

- SPSC ring、shared L0、vector pool 和 owner-affinity callback 的专门 UT 均通过；
- 扩容/epoch detach、channel close/drain、错误 response、ring/vector pool 耗尽不会漏回调、重复回调或
  跨 generation 使用 vector；
- stable `VEMB_HANDLE` 外的请求永远不进入 shared L0；
- 真实跨机读验证与性能 A/B 均通过后，才将 `VEMB_V16_SHARED_L0_MODE=aggregator` 作为可选功能保留。

### 21.6 UB 全双工 CC/NC 方向修正 checkpoint（2026-08-05，工作区未提交）

测试代码基线：`6249f28` + 工作区未提交变更。

此前文档中把 server `/dev2` 与 client `/dev6` 的旧单向实验关系沿用到 batch
response，和真实双机拓扑不一致。统一规则是每个节点的 `1-4` 均为本节点 local CC
region，`5-8` 均为对端 `1-4` 的 imported NC view。因此当前 112 client 到 111
server 的跨机 channel 固定为：

```text
request:  112 dev7 (NC writer) -> 111 dev3 (local CC reader)
response: 111 dev6 (NC writer) -> 112 dev2 (local CC reader)
warm:     111 dev4 (local CC writer) -> 112 dev8 (NC reader)
```

server 必须以 `--vemb-v16-aeron-ub-path /dev/obmm_shmdev3` 和
`--vemb-v16-aeron-response-ub-path /dev/obmm_shmdev6` 启动。storage 的 request
pool 按 CC 打开，response pool 按 NC (`O_SYNC`) 打开；v1 与 v2 的 descriptor/arena
均复用这一方向。UB 自身负责 NC write 到对端 CC read 的可见性，数据面不额外用
`dc ivac`/`dc cvac` 进行同步。

SDK 的实验映射更新为双向 `1-4 <-> 5-8`：ATTACH 返回 server local path `1-4` 时，
client 映射对应 imported path `5-8`，从 offset 0 覆盖 `offset + bytes` 后取 resource
view；ATTACH 返回 server imported path `5-8` 时，client 映射自身 local path `1-4`，
直接以 resource 的实际 offset mmap。这样 response 的 client CC mapping 不会错误地
按 peer-view 映射回首段。cacheability 由最终 mapping 方向决定，不能由“path 是否发生
字符串翻译”推断。

**验证完成：** 本地完成 `make -C clients/c`、v1/v2/shared-L0 attach smoke、
`vemb_v16_shared_l0_ut`、`vemb_v16_cli_deadline_ut`、`make -C memtier_benchmark`
和 `git diff --check`。同步 111/112 后，111 6395 server 日志确认：
`/dev/obmm_shmdev3 mode=CC`、`/dev/obmm_shmdev6 mode=NC`。112 的
`vemb_v16_shared_l0_attach_smoke` 成功建立 v1/v2 channel；v2 response descriptor
和 arena 的非零 offset 分别为 `36864`、`73728`。

随后预填充 10k key，并在 112 执行 `VEMB_V16_SHARED_L0_MODE=aggregator`、
`t=2,c=2,pipeline=8`、2 秒的真实 handle read-verify。结果为 `258202` logical GET，
`handle_deref ok=258202 fail=0`；owner stats 为 `leaders=257478`、`followers=724`、
`shared_vector_reads=257478`、`shared_vector_read_failures=0`、
`vector_pool_exhausted=0`、`ack_failures=0`、`v2_backpressure=0`、
`non_ok=0`、`stale=0`，QPS `129078.35`、p99 `0.335ms`。该样本验证 CC/NC 方向、
非零 response offset 与 shared vector/ack 数据面正确性；因并发和 pipeline 很小，不能
作为 CP-4 性能结论。

**10k Zipf 聚合验证（2026-08-05）：** 112 使用 `Z:Z`、`s=1.5`、10k 已预填充
keyspace、`batch=32,max-delay=50us`。`t=8,c=4,pipeline=16,test-time=5s` 的
shared-L0 样本完成 `8938795` logical completion，其中 `8532130` leaders 和
`406665` followers，聚合率为 `4.55%`，平均 fanout `1.05`；物理 batch 为 `288912`，
shared vector reads 为 `8532130`，无 read/ack/v2 backpressure/non-OK/stale error。
这证明不同 worker 进入同一 shard 后可以发生真实 coalescing，而不是仅记录 metadata。
worker 侧另有 `112` 个 fixed L0 pool admission 产生的 synthetic `STATUS_ERR`，该问题
不来自 server response，单独按下述容量缺口处理。

相同 workload 的 local-L0 baseline 完成 `16119376` logical completion，累计
`13605` followers（约 `0.08%`），QPS `3225386.62`、p99 `0.215ms`；shared-L0 为
QPS `1787598.71`、p99 `0.503ms`。因此 shared-L0 将可观测 coalescing 比例提高约
54 倍，但当前 single-owner SPSC ingress/egress 扫描、跨 worker completion 和 vector
ack 的成本仍使吞吐下降约 `44.6%`，不得声称已有性能收益。

`t=16,c=4,pipeline=32` 的同类压力样本进一步得到 `4177789` followers、平均 fanout
`1.23`（聚合率 `18.37%`），证明热点下聚合空间更大；但约 `0.87M` logical request 被
fixed L0 pool admission 转成 `STATUS_ERR`。当前 owner stats 未细分
bucket/entry/follower/key-slab exhaustion，且 admission failure 没有走 v1 fallback，
所以该压力样本只能说明上限，不能作为 correctness 或性能样本。下一阶段须增加这些
exhaustion counters，并将 capacity failure 改为 bounded fallback 或明确 backpressure，
使正常 benchmark 不返回 synthetic `STATUS_ERR`。

### 21.7 local-v2 基线恢复与热日志根因 checkpoint（2026-08-05，工作区未提交）

测试代码基线：`6249f28` + 工作区未提交变更。

在正确的全双工 UB 方向下，首次 local-v2 30 秒样本只有 `3.832M QPS`，而相同 v1
样本仍有 `11.543M QPS`；因此不能将问题归因为 shared-L0。定位发现
`proxy_io_aeron_poll_thread_main()` 在每次发现 request ring 非空时调用
`serverLog(LL_NOTICE, "vemb_v16 aeron poll ready...")`。一次 30 秒 v1 压测实际写入
`10,819,285` 行、约 `920MB` 日志，poll/log I/O 成为数据面热路径的主导开销。删除该
调试日志后，111 server log 在完整样本中仅 `1,579` 行/约 `163KB`，且没有 poll-ready
记录。

**统一基线口径：** 111 fresh 6395 server 使用 request `dev3` CC、response `dev6` NC，
`21` proxy-IO/`21` SuperNode worker 并绑定 `0-47`；112 绑定 `96-191`。每个样本先以
v1 VADD 顺序预填充
10k key，再执行真实 `VEMB_HANDLE` 的 10k uniform `R:R` read：
`t=64,c=4,pipeline=32,BATCH_REQUEST_SIZE=32,max-delay-us=0,test-time=30s`。

删除日志后的第一个有效样本为 `20.145M QPS`、P99 `1.399ms`，handle dereference
failure、fallback 与 flush backpressure 均为零。后续将 batch arena bulk copy 直接调用
`sve_operation::sve_streaming_load_f32`、ring slot stride 改为已对齐的
`64 + slot_size`、通用 align helper 改为 2 的幂 mask 运算；descriptor 保持 16-byte
`memcpy`。在 111/112 以显式 `USE_SVE=yes` 重建后的两个 fresh screening sample 为
`18.375M`（P99 `3.743ms`）和 `22.607M QPS`（P99 `0.391ms`）。后者仅有
`1.54 misses/sec`，其余为 hit，所有 handle dereference 均成功。两者单样本波动较大，
不能将差值直接归因于 direct SVE call；但 `22.607M` 是当前正确 UB 方向下的最高
local-v2 基线。

**结论：** 当前 `1.788M` shared-L0 结果不能再与修复前的 `3.832M` local-v2 错误基线
比较。其已被本节约 `20-22.6M` 的 local-v2 基线取代；后续 shared-L0 实验也未达到
`>=2.5` fanout、零 synthetic error 和不低于 local-L0 QPS 的门槛，实验代码随后已删除。

**shared-L0 停止结论（2026-08-05，工作区未提交）：** 不再为当前 shared-L0 实现继续
执行远端调参或性能测试。已有 10k Zipf 有效样本将 coalescing 从 local-L0 的约 `0.08%`
提高到 `4.55%`，但 logical QPS 从 `3.225M` 降到 `1.788M`；提高并发后虽达到 `18.37%`
聚合率和 `1.23` 平均 fanout，却触发 fixed L0 pool admission exhaustion 并返回 synthetic
`STATUS_ERR`。这说明瓶颈是单 owner shard 的 SPSC ingress/egress 扫描、跨 worker
completion、vector ack 和固定容量管理，而不是 batch size、pipeline 或 delay 参数。

修复 server poll 热日志后，正确 UB 方向上的 local-v2 10k uniform 基线已为
`20.145M QPS`，SVE 重建后单样本最高 `22.607M QPS`。即使暂不把旧 shared-L0 样本和
该新基线作严格 A/B，现有 `1.05-1.23` 的 fanout 也远低于达到 `40M` logical QPS 所需的
物理请求削减比例。因此当前 shared-L0 不应进入默认数据面；后续若重新考虑跨 worker
聚合，必须采用 key-to-owner affinity 或无锁 MPSC ingress，并先消除 admission error，
再以 `>=2.5` fanout、零 synthetic error 和不低于 local-L0 QPS 作为重新启用实验的门槛。

**代码清理（2026-08-05，工作区未提交）：** 已删除 shared-L0 的 SDK 实现、internal
header、UT、attach smoke、`VEMB_V16_SHARED_L0_MODE` 开关，以及 memtier runner 中的
owner thread、shard channel、completion/ack 分派和统计代码。普通 v2 batch 的
channel-plan 保留并已简化为“符合 batch admission 即创建每 worker/channel 的 batch
session”；worker-local L0、deadline-aware flush 和 shared-vector read 均继续保留。
本地已通过 `vemb_v16_aeron_runner_plan_ut`、`vemb_v16_cli_l0_ut`、
`vemb_v16_cli_deadline_ut`、`vemb_v16_batch_ring_ut`、`make -C clients/c USE_SVE=yes`
与 `make -C memtier_benchmark USE_SVE=yes`。本节此前的 shared-L0 数据仅作历史实验
记录，不能视为当前可用功能。

### 21.8 v2 baseline fallback 筛选（2026-08-05，工作区未提交）

测试代码基线：`6249f28` + 工作区未提交变更。

在 112 -> 111 的正确 CC/NC UB 方向、fresh 6395 server、顺序预填充 10k key、真实
`VEMB_HANDLE` 读取、`batch=32,max-delay=0,pipeline=32,test-time=30s` 下，v2 worker-local
L0 的当前最高吞吐样本为 `t=64,c=4`：`22.973M QPS`、P99 `0.383ms`。请求/响应 channel 和
handle dereference 均正常，累计 `415` 个 group v1 fallback（约 `0.00006%` logical request）、
`51` 个 `NOT_FOUND`、`2` 个 shared-vector read failure。因此该值可作为当前吞吐结果，不能
标为零 fallback correctness baseline。

为验证 fallback 是否由 `256` 个 batch session 的资源压力造成，固定其它参数分别降低
`c`，每组一轮 fresh-server 样本：

| t | c | QPS | P99 | v1 fallback | NOT_FOUND | vector read failure |
|---|---|---:|---:|---:|---:|---:|
| 64 | 4 | 22.973M | 0.383ms | 415 | 51 | 2 |
| 64 | 2 | 18.812M | 0.791ms | 1,658 | 532 | 2 |
| 64 | 1 | 14.102M | 0.231ms | 863 | 833 | 8 |

`c=2`、`c=1` 均没有消除 fallback，且吞吐下降，故 session 数、`BATCH_REQUEST_SIZE`、
pipeline 与 max-delay 不应被当作保证零 fallback 的手段。`flush_backpressure=0` 也排除了
普通 v2 request arena/ring 饱和。当前 SDK 只会在 `vemb_v16_aeron_batch_poll_response()`
返回负值，或 response epoch/status stale 时关闭该 session 的 v2 并将未完成 group 转到 v1；
下一步应给该函数区分 descriptor peek、descriptor bounds、response decode、item-count
mismatch 与 stale-epoch 的原因计数及 channel/batch id，再用该计数修复 response 数据面。
修复前不再做更多参数扫描。

### 21.9 v2 reused-arena payload 可见性修复与零 fallback 验收（2026-08-05，工作区未提交）

测试代码基线：`6249f28` + 工作区未提交变更。

21.8 的 fallback 不是 batch session 数、`BATCH_REQUEST_SIZE`、pipeline 或普通 request
ring/arena 满导致。单 worker 的 10 秒样本表明 v2 可先持续工作，再因一次 response poll
失败关闭 session；该 group 改走 v1 后，后续请求均直接走 v1。因此必须修复 response
descriptor 与其复用 arena payload 的配对，而不是继续调整并发参数。

**根因：** batch descriptor 的 `sequence` 只证明当前 descriptor slot 已发布，无法证明
相同 arena 位置上本轮 payload 已在 client 的 CC 映射中可读。arena 回绕复用时，client
可能先读到当前 `start/bytes/sequence`，但读到上一轮残留 frame。旧代码将 decode、
item-count 或 batch-id 不匹配作为 poll error，runner 随即关闭该 v2 session 并触发 v1
fallback。server request 侧存在同一类风险：不能因为当前 descriptor 对应的 arena frame
尚未可读而消费该 descriptor。

**修复：** `vemb_v16_batch_desc_t` 增加 `batch_id`，每次 request/response publish 均将
frame 的 batch id 写入 descriptor。server request poll 与 client response poll 都先验证
descriptor sequence 和边界，再 decode frame 并要求 `frame.batch_id == desc.batch_id`（request
还要求 item count 一致）。对于 decode 失败、边界暂不可见或 id 不匹配，保留 ring head 并
返回 `0` 供后续 poll 重试；只有完成合法处理后才 consume descriptor。此为 UB CC/NC
可见性等待，不是错误 fallback。曾尝试以 `dc ivac/cvac` 强制 cache maintenance，但 112
用户态触发 `SIGILL`，且违背 UB 负责 NC writer 到 CC reader 可见性的约束，已明确不用；此前
ATTACH 初始化中的 `dc cvac + dsb sy` 也已删除。

**本地验收：** 已通过
`make -C clients/c USE_SVE=yes`、
`make -B -C benchmark vemb_v16_batch_ring_ut vemb_v16_batch_context_ut USE_SVE=yes`、
`./benchmark/vemb_v16_batch_ring_ut`、`./benchmark/vemb_v16_batch_context_ut`、
`make -C src redis-server USE_SVE=yes`、`make -C memtier_benchmark USE_SVE=yes` 与
`git diff --check`。ring UT 同时断言 descriptor 的 batch id；仍应补一个专门的 stale-but-
valid reused response frame 注入 UT，验证 batch-id mismatch 不 consume。当前对于永久损坏
payload 的 retry 没有 response visibility deadline，属于后续故障隔离设计项，不能误解为
已具备有界失败语义。

**跨机验收：** 使用已确认的全双工路径（112 `dev7` NC -> 111 `dev3` CC request，111
`dev6` NC -> 112 `dev2` CC response），预填充 10k key 后执行真实 `VEMB_HANDLE` uniform
read。`t=1,c=1,batch=32,pipeline=32,max-delay=0,test-time=10s` 得到 `407,854 QPS`，
`4,072,310` leaders、`127,456` frames，v1 fallback、vector read failure 与 handle
dereference failure 均为零。显式执行的 `t=64,c=4,batch=32,pipeline=32,test-time=30s`
样本得到 `23,153,627.54 QPS`、P99 `0.367ms`，所有 worker 的 `fallback_v1=0`，
`shared_vector_read_failures=0`，以及全部 handle dereference 成功。这取代 21.8 的
“带 fallback 吞吐样本”，是当前 v2 local-L0 的零 fallback correctness/performance baseline。

**实验环境说明：** 修复代码已同步到 111/112；112 client SDK 与 memtier 使用
`USE_SVE=yes` 强制重建。111 常规 LTO 重建被 linker OOM kill，原因是生成的
`src/.make-settings` 覆盖优化选项并保留 `-O3 -flto`；将该测试机的生成配置改为 `-O2`
后，以 `USE_SVE=yes` 成功构建并运行 server。上述 23.154M 结果的 111 server 是 O2/SVE、
非 LTO，不能宣称为 LTO 成绩。测试辅助脚本还修复了 server 启动等待 port/PID ready 和
shutdown 等待 pid/port 释放的竞态；现有总控脚本仍偶发在 prefill 后提前结束，性能验收
改用显式 start/prefill/read 命令完成，后续应继续加固该编排脚本。

### 21.10 跨机联调可复现流程与 build-stamp gate（2026-08-05，工作区未提交）

测试代码基线：`6249f28` + 工作区未提交变更。

本轮跨机 v2 联调反复遇到的问题已归纳为四类，后续性能数据必须同时通过以下 gate，不能只以
`Totals` QPS 判断成功。

| Gate | 已遇问题 | 固化检查/修复 |
|---|---|---|
| 协议与 UB 映射 | ATTACH 对非零 offset 直接 mmap 回首段；将旧 arena payload 当作当前 response；错误沿用单向 UB 路径 | peer-view 从 offset 0 覆盖 `offset + bytes` 后取 resource view；descriptor 绑定 `batch_id` 且 payload 未就绪时不 consume；固定 `112 dev7 NC -> 111 dev3 CC` request 与 `111 dev6 NC -> 112 dev2 CC` response。 |
| UB 可见性 | 尝试 `dc ivac/cvac` 后 112 用户态 `SIGILL`；误把 CPU cache 操作当作 UB 同步 | UB 保证 NC writer 到 CC reader 可见性，v2 poll 以保留 head/retry 等待 frame；删除初始化路径的 `dc cvac + dsb sy`。NC 写端仍以 `O_SYNC` 打开，这是 UB mapping 模式要求，不是 CPU cache-maintenance。 |
| 构建与同步 | `sync_changed_code_to_peer.sh` 原先只复制源码，远端 `redis-server`/SDK/memtier 可继续使用旧二进制；`--all-code` 按文件新建 SSH 会话，易在大量文件时中断且日志淹没构建错误；重复同步可并发启动多个 `make -B`；111 的 `-O3 -flto` 链接曾被 OOM kill | 新增 `scripts/vemb_v16_build_stamp.sh`，记录源码指纹、`src/.make-settings`、构建策略与 artifact SHA-256；同步脚本复用 SSH control connection，默认只输出 changed（`--verbose` 才输出 same），支持远端强制 `-B` 构建和验证；build-stamp 对每个 target 使用非阻塞 `flock`，拒绝并发构建；构建失败不会产生有效 stamp。当前固定策略是 111 server、112 SDK/memtier 全部 `-O3 -flto` + SVE；111 即使再次 LTO OOM 也不得回落为 O2 或 non-LTO 并将其当作当前基线。 |
| 进程、资源与结果 | 旧 server/port/UB path holder 未退出；启动脚本只固定 sleep；总控脚本偶发在 prefill 后结束；poll-ready 调试日志写入 920MB | 启动前检查每条 UB path holder 和目标 port；启动后同时等待 pid 与 listener；停止后等待 pid/port 释放；数据面禁止逐 poll 日志；prefill、read workload 和 worker stats 均单独检查，wrapper 提前退出时改用显式 start/prefill/read 并保留日志。 |

**源码与二进制一致性：** 从现在开始，跨机实验必须使用以下顺序。`--all-code` 先将整个可执行
代码集合校验为一致；`--build` 强制 `-B` 构建并写入 target stamp；`--verify-build` 再校验当前
源码、构建设置、O3/LTO/SVE 策略和二进制 hash。111 仅构建 server，112 仅构建 SDK/memtier client：

```sh
NODE=192.168.90.111 bash scripts/sync_changed_code_to_peer.sh \
  --all-code --build server --verify-build server
NODE=192.168.90.112 bash scripts/sync_changed_code_to_peer.sh \
  --all-code --build client --verify-build client
```

`run_aeron_best.sh` 默认执行相应 `build-stamp.sh verify`：`ROLE=server` 校验 server，
`ROLE=client` 校验 client，`ROLE=both` 校验两者。源码或 `src/.make-settings` 在构建后变化、
artifact 被替换、stamp 缺失时，runner 在启动 Redis 前直接失败。手工构建是唯一允许的例外，
但必须显式运行 `bash scripts/vemb_v16_build_stamp.sh write server|client` 重新记录 artifact
hash，且必须采用相同的 O3/LTO/SVE 策略，不能通过关闭验证或以 O2/non-LTO artifact 掩盖旧二进制。

**标准联调验收：** 每个阶段先运行该阶段最小 UT 和受影响 target 的本地构建；同步/构建/验证通过
后，再执行 remote UB/port preflight、fresh server start、顺序 v1 VADD prefill、真实
`VEMB_HANDLE` read。结果至少归档 `Totals` QPS/P99、每 worker `fallback_v1`、
`flush_backpressure`、shared-vector read failure、handle dereference failure、server log 与
build stamp。零 fallback 样本还须确认所有上述 failure counter 均为零。任何 prefill 失败、
脚本提前退出、stamp 不匹配、UB path holder 或非零 failure 都使该样本无效，不得进入性能对比。

当前使用的 `/private/tmp/start_vemb_redis_111.sh`、`run_v2_baseline_repeats.sh`、
`run_vemb_cp1_candidate.sh` 与 `run_vemb_cp1_screen.sh` 已在启动或发压前验证相应 server/client
stamp。它们仍是临时文件，后续应迁入仓库的统一 cross-node runner；在此之前，新临时脚本不得
绕过同一 gate。旧 `start_vemb_deadline_server_111.sh` 直接启动历史 `vemb_v16_server` artifact，
不属于当前 `redis-server` 基线，禁止用于本节的性能结果。

尚未自动化的两项保留为下一步：为 stale-but-valid reused frame 增加故障注入 UT；为永久错误的
descriptor/payload 定义有界 visibility timeout 和明确的 channel failure 语义。它们完成前，当前
retry 仅能证明正常 UB 传播延迟不触发 v1 fallback，不能证明损坏 channel 的故障隔离能力。

### 21.11 O3/LTO/SVE 跨机 v2 baseline（2026-08-05，工作区未提交）

测试代码基线：`6249f28` + 工作区未提交变更。

111 server 与 112 SDK/memtier 均通过新的 build-stamp 策略强制重建：server 使用
`-O3 -flto -fno-omit-frame-pointer` 和 `USE_SVE=yes`；SDK 与 memtier 的 C/C++ 编译及链接
使用 `-O3 -flto -DUSE_ARM_SVE -march=armv8.2-a+sve`。111 的 LTO link 成功完成（49 个 LTRANS
job 串行执行），不再使用 O2/non-LTO 回退。

按统一口径执行单轮 fresh-server 10k uniform、真实 `VEMB_HANDLE` 读：111 fresh 6395、正确
的 `112 dev7 NC -> 111 dev3 CC` request 与 `111 dev6 NC -> 112 dev2 CC` response，顺序 v1
VADD prefill 后，在 112 运行
`t=64,c=4,pipeline=32,BATCH_REQUEST_SIZE=32,max-delay-us=0,test-time=30s`。结果：

| QPS | P99 | leaders | followers | frames | fallback | backpressure | vector read failure | NOT_FOUND/server error/handle failure |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 23.566M | 0.359ms | 705,836,799 | 1,094,817 | 22,091,613 | 0 | 0 | 0 | 0 / 0 / 0 |

该样本是当前明确以 O3/LTO/SVE 构建得到的零 fallback correctness/performance 结果。相对 21.9
记录的 O2/SVE 样本 `23.154M QPS` 高约 `1.8%`，但两者均为单轮跨机样本，不能将差值直接归因于
LTO；后续性能结论应继续以 fresh-server 单轮有效样本为最小单位，并在需要作编译器 A/B 结论时再
补受控重复。

### 21.12 Zipf key generator 修复与 local-L0 聚合验证（2026-08-05，工作区未提交）

测试代码基线：`6249f28` + 工作区未提交变更。

原 `zipfian_distribution()` 使用 `pow(u, 1/s)` 再取模。它不是 Zipf inverse-CDF：尤其在
`s=1` 时退化为 uniform，取模还会破坏尾部概率。因此此前标为 Zipf 的样本不能用于判断
worker-local L0 聚合收益。实现现改为连续 Zipf inverse-CDF：`s=1` 使用 `exp(u * log(n))`，
其它 `s` 使用幂律积分的反函数；rank 最后钳制到 `[1, n]`，不再取模回绕。

新增 `obj_gen_zipf_ut` 使用固定 seed 对 10k keyspace 抽样 100 万次，验证边界和 rank
`1 > 10 > 100 > 1000` 的频率单调关系。本地结果为
`top=75284, rank10=10327, rank100=1056, rank1000=101`，UT 通过。

使用修复后的 generator 在相同 O3/LTO/SVE、UB 路径和真实 `VEMB_HANDLE` 条件下执行 10k
`Z:Z,s=1.5`：`t=64,c=4,pipeline=32,BATCH_REQUEST_SIZE=32,max-delay-us=0,test-time=10s`。

| QPS | P99 | leaders | followers | frames | 平均 fanout | fallback/backpressure/vector failure/NOT_FOUND/server error/handle failure |
|---:|---:|---:|---:|---:|---:|---:|
| 38.493M | 0.239ms | 190,636,791 | 193,613,801 | 12,007,831 | 2.016 | 0 / 0 / 0 / 0 / 0 / 0 |

该有效短样本中，followers 占逻辑请求约 `50.4%`，说明正确 Zipf 热点下 local-L0 能实质减少
物理请求，QPS 高于 21.11 的 uniform 样本。

**间歇 `NOT_FOUND` 根因与修复：** 早期 30 秒 Zipf 样本分别出现 2 次和 9 次 `NOT_FOUND`。
永久 server 日志证实这不是预填充遗漏：`stable_read` 返回 `rc=-1` 时收到的 key 已被拼接损坏，
例如期望 `v2-zipf-s15:<rank>` 却收到 `v2-zv2-zipf-s1`、`v2-zip2-zipf-s`。根因是重用 batch
arena 的 CC 读端可先观察到 descriptor 和 frame header，而中间 key-length/key bytes 仍是上一轮
内容；仅校验 header `batch_id` 不能证明完整 payload 已可见。

request/response frame 现带末尾 8-byte `commit batch_id`。第一版将 marker 包含在 SVE bulk copy
中，仍复现 1 次错误，说明尾部可见也不能约束中间 body 的可见性。最终发布流程为：bulk copy
header/body，`release` barrier，单独以 volatile byte store 写 commit marker，最后 publish descriptor。
读端要求 descriptor、header、尾部 commit 三处 batch id 一致，否则保持 ring head 并重试。
`vemb_v16_batch_ring_ut` 覆盖 request/response trailer 损坏拒绝，以及 bulk-copy source trailer 被
发布函数覆盖为正确 marker；本地 SDK/server 构建通过。

两端重新全量 O3/LTO/SVE 构建并通过 build-stamp 后，fresh 6395、10k `Z:Z,s=1.5`、真实
`VEMB_HANDLE`、`t=64,c=4,pipeline=32,BATCH_REQUEST_SIZE=32,max-delay-us=0,test-time=30s` 的
严格样本为：

| QPS | P99 | leaders | followers | frames | 平均 fanout | fallback/backpressure/vector failure/NOT_FOUND/server error/handle failure |
|---:|---:|---:|---:|---:|---:|---:|
| 39.567M | 0.239ms | 588,648,173 | 597,869,811 | 37,078,687 | 2.016 | 0 / 0 / 0 / 0 / 0 / 0 |

111 的 `/tmp/vemb_redis_6395.log` 在该样本中没有 `vemb_v16 handle miss`。临时
`run_v2_baseline_repeats.sh` 未追加 TSV 行，但对原始 runner log 直接执行同一 strict predicate
返回 0，且所有 64 worker 已 join；因此上述数据为有效样本。后续应修复该临时脚本的 TSV
解析/追加路径，但不得以此误判数据面失败。

**100k Zipf 验证（2026-08-06）：** 保持相同的 O3/LTO/SVE 构建、UB 路径、真实
`VEMB_HANDLE` 和读压参数，仅将 keyspace 提升为 100k：`Z:Z,s=1.5`、顺序 v1 VADD 预填充
100k key 后运行 `t=64,c=4,pipeline=32,BATCH_REQUEST_SIZE=32,max-delay-us=0,test-time=30s`。

| QPS | P99 | leaders | followers | frames | 平均 fanout | fallback/backpressure/vector failure/NOT_FOUND/server error/handle failure |
|---:|---:|---:|---:|---:|---:|---:|
| 39.590M | 0.239ms | 594,497,692 | 593,127,140 | 37,113,276 | 1.998 | 0 / 0 / 0 / 0 / 0 / 0 |

111 server log 没有 `vemb_v16 handle miss`。相对上述 10k Zipf 的 `39.567M`，吞吐基本持平；
`s=1.5` 的请求质量集中在极少数头部 rank，故总 keyspace 从 10k 扩至 100k 没有明显降低当前
worker-local L0 的聚合收益。

**较低热点强度（2026-08-06）：** 100k keyspace 将 Zipf 指数降为 `s=1.2`。该分布的累计请求
覆盖为 Top 1/10/100/1k/10k = `19.64% / 48.47% / 70.76% / 85.16% / 94.26%`，明显低于
`s=1.5` 的头部集中度。保持同一预填充和读压参数，30 秒 strict 样本为：

| QPS | P99 | leaders | followers | frames | 平均 fanout | fallback/backpressure/vector failure/NOT_FOUND/server error/handle failure |
|---:|---:|---:|---:|---:|---:|---:|
| 28.943M | 0.335ms | 653,052,763 | 215,217,093 | 27,133,433 | 1.330 | 0 / 0 / 0 / 0 / 0 / 0 |

111 server log 没有 `vemb_v16 handle miss`。followers 占逻辑请求约 `24.8%`，低于 `s=1.5` 的
约 `50%`，物理 VEMB 请求增多使 QPS 从约 `39.59M` 降至约 `28.94M`；该差异符合 L0 聚合窗口对
热点集中度敏感的预期。

为保留异常现场，server 在 `vemb_v16_tlc_get_handle_stable_read()` 返回非零并产生
`NOT_FOUND` 时永久记录 `vemb_v16 handle miss`，包含 `req_id`、`batch_token`、key hash、key 与
返回码。该日志只运行在 `NOT_FOUND` 异常路径，不属于热点路径，必须保留。

### 21.13 100k uniform 与 Zipf s=1.0 边界样本（2026-08-06）

测试代码基线：`6249f28` + 工作区未提交变更。

为量化 worker-local L0 对热点强度的敏感性，在相同 O3/LTO/SVE 构建、UB 双向路径、fresh 6395、
顺序 v1 VADD 预填充、真实 `VEMB_HANDLE` 和
`t=64,c=4,pipeline=32,BATCH_REQUEST_SIZE=32,max-delay-us=0,test-time=30s` 下，补充 100k
uniform 与 Zipf `s=1.0` 的单轮 strict 样本。`s=0` 在此表示 uniform，因此使用 `R:R` random
key pattern；不能传 `Z:Z,s=0`，因为 Zipf 参数要求严格大于零，非正默认值会被配置层改为 `0.99`。

| distribution | QPS | P99 | leaders | followers | frames | 平均 fanout | follower 占 logical request | fallback/backpressure/vector failure/NOT_FOUND/server error/handle failure |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 100k uniform (`R:R`, s=0) | 14.127M | 0.567ms | 423,722,022 | 65,914 | 13,243,373 | 1.000 | 0.016% | 0 / 0 / 0 / 0 / 0 / 0 |
| 100k Zipf (`Z:Z,s=1.0`) | 23.635M | 0.431ms | 651,489,412 | 57,499,740 | 22,155,911 | 1.088 | 8.11% | 0 / 0 / 0 / 0 / 0 / 0 |
| 100k Zipf (`Z:Z,s=1.2`) | 28.943M | 0.335ms | 653,052,763 | 215,217,093 | 27,133,433 | 1.330 | 24.79% | 0 / 0 / 0 / 0 / 0 / 0 |
| 100k Zipf (`Z:Z,s=1.5`) | 39.590M | 0.239ms | 594,497,692 | 593,127,140 | 37,113,276 | 1.998 | 49.95% | 0 / 0 / 0 / 0 / 0 / 0 |

最新 `s=1.0` 样本的 111 `/tmp/vemb_redis_6395.log` 没有 `vemb_v16 handle miss`，且 runner
确认 64 worker 全部 join。uniform 原始 runner 统计同样为零 failure；该轮 server 日志在随后
fresh `s=1.0` server 启动时被同一路径覆盖，故不把它单独作为 server-log 证据。

结论是：修复后的 `s=1.0` 并非 uniform，已经使 100k workload 从约 `14.13M` 提升到
`23.64M QPS`；但其 `1.088` fanout 仍明显低于 `s=1.2` 的 `1.330` 和 `s=1.5` 的 `1.998`。
local-L0 只在同一 worker 的当前聚合窗口内合并相同 key，因此热点不足时绝大多数 logical request
仍对应一个 physical batch item，不能期望仅靠增大 keyspace、batch 或 pipeline 获得 `s=1.5` 的
吞吐。

### 21.14 ring 优化快照的 100k uniform 回归（2026-08-06，工作区未提交）

测试代码基线：`6249f28` + 工作区未提交变更。

当前 ring 优化快照重新同步到 111/112 后，两端均以 O3/LTO/SVE 强制重建并通过 build stamp。
构建过程发现并修复两项 build gate 问题：ring UT 不应继续调用已内联删除的 slot-stride helper，
改为直接断言当前 `RING_SLOT_META_BYTES + slot_size` 的 cacheline 对齐；build-stamp 强制覆盖
memtier `CXXFLAGS` 时必须保留 `-Drestrict=__restrict__`，使公共 C header 可被 GNU C++ 编译。
112 上 `vemb_v16_aeron_ring_ut` 重新编译并通过。

随后运行 fresh 6395、顺序 v1 VADD 预填充 100k key、真实 `VEMB_HANDLE` 的 30 秒 `R:R` uniform
read：`t=64,c=4,pipeline=32,BATCH_REQUEST_SIZE=32,max-delay-us=0`。

| QPS | P99 | leaders | followers | frames | vector reads | vector bytes | 平均 fanout | fallback/backpressure/vector failure/NOT_FOUND/server error/handle failure |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 14.023M | 0.567ms | 420,564,342 | 65,354 | 13,144,678 | 420,564,342 | 504,677,210,400 | 1.00016 | 0 / 0 / 0 / 0 / 0 / 0 |

`shared_vector_group_reads == leaders`，且 `shared_vector_group_bytes == leaders * 1200B`，说明每个
physical leader 均执行一次 DIM=300 warm-region vector materialize；followers 没有额外读取。
111 server log 没有 `vemb_v16 handle miss`，64 worker 均 join。期间记录了两次
`v2 batch response frame not ready or inconsistent`，均由 poll 保留 ring head 后重试成功，未造成
fallback、非 OK completion 或 handle failure。这是正常 UB 可见性重试计数之外的告警现场，后续应
继续保留并在需要降低告警噪声时增加独立 retry counter，不能将其解释为 response 数据面错误。

相对 21.13 的 `14.127M QPS` uniform 单轮样本，本轮低约 `0.73%`，处于跨机单轮波动范围内；当前
ring 优化没有带来可辨识的 100k uniform 吞吐提升，不能据此宣称性能收益。

### 21.15 100k uniform 用户态+内核态火焰图（2026-08-07，工作区未提交）

测试代码基线：`b98ffae` + 工作区未提交变更。

基于 21.14 的 current v2 local-L0 基线，在 fresh 111 server 上重新执行一次 100k uniform
跨机读取，并在两台机器各自生成火焰图。两端均先通过 O3/LTO/SVE build stamp 验证；111 为
`redis-server`，112 为 SDK/memtier。request 为 `112 dev7 NC -> 111 dev3 CC`，response 为
`111 dev6 NC -> 112 dev2 CC`，client warm read 为本地映射的 `dev8`。

运行参数为：顺序 v1 VADD 预填充 100k key（`S:S`）；随后真实 `VEMB_HANDLE` read 使用
`R:R`、`t=64,c=4,pipeline=32,BATCH_REQUEST_SIZE=32,max-delay-us=0,test-time=30s`。
111 server 在读压开始前运行 `perf record -F 99 -g -e cycles -p <redis-server-pid> -- sleep 25`；
112 直接以 `perf record -F 99 -g -e cycles -- taskset ... memtier_benchmark ...` 启动 30 秒读压。
没有使用 `cycles:u` 或 `-a`，故两张图均包含目标进程的用户态和内核态调用栈，但不混入系统中
无关进程。

| QPS | P99 | leaders | followers | frames | vector reads | vector bytes | 平均 fanout | fallback/backpressure/vector read failure/NOT_FOUND/server error/handle failure |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 14.044M | 0.567ms | 421,414,988 | 65,428 | 13,171,263 | 421,414,988 | 505,697,985,600 | 1.00016 | 0 / 0 / 0 / 0 / 0 / 0 |

64 个 batch session 和 64 个 worker 全部完成；111 server log 的 `vemb_v16 handle miss` 为 0。
server 采集到 1,825 samples，CLI 采集到 187,193 samples。两侧 raw script 都包含
`[kernel.kallsyms]` frame（server 20,118 行、CLI 15,714 行），并分别包含 `redis-server` 和
`memtier_benchmark` 用户态 frame，确认火焰图覆盖了两种特权级。

server SVG 使用 `server.process.collapsed.txt` 重绘：将原 folded stack 的第一个 frame（worker
thread comm，例如 `vemb-io-*`）统一替换为 `redis-server`，并增加 `all` 根。最终唯一根为
`all -> redis-server`，覆盖 100% server samples；后续的 `proxy_io_pool_thread_main`、
`supernode_pool_thread_main` 等保留为真实调用链入口。因而图中不会按单个 server thread 分组，
但仍完整显示不同 server role 的工作及其用户态、内核态后续栈。

**采样覆盖更正：** 上述 SVG 的进程级折叠语义正确，但原始 `server.perf.data` 没有捕获任何
`vemb-sn-*` 或 `supernode_pool_thread_main` sample；`batch_processor_thread` 属于独立的 legacy
batch processor，不能替代 VEMB pooled SuperNode。因此它只能用于已采集到的 main/proxy/legacy
batch 线程分析，**不能作为完整 server 火焰图使用**。后续必须重跑同一 workload：在采样开始前通过
`ps -T -p <redis-server-pid>` 枚举全部 server TID，并以 `perf record -F 99 -g -e cycles --tid
<comma-separated-tids>` 显式绑定 main、`vemb-io-*` 与 `vemb-sn-*`；生成 SVG 前要求 raw script
至少各含一个 `vemb-sn-*` 和 `supernode_pool_thread_main` sample，否则本轮 server 图无效。

**全 TID replacement server sample：** 随后 fresh 111 server 重预填充并重跑相同 100k uniform
workload。采样前的 `ps -T` 确认有 21 个 `vemb-io-*` 与 21 个 `vemb-sn-*`；将全部 server TID
显式传入 `perf record -F 99 -g -e cycles --tid <comma-separated-tids> -- sleep 25`。这份 trace
采集到 29,677 samples，其中 `vemb-sn-*` 为 7,328、`supernode_pool_thread_main` 为 7,203、
`proxy_io_pool_thread_main` 为 21,982，且有 22,846 个 kernel frame。由此生成的 SVG 为唯一
`all -> redis-server` 根：proxy IO 占 `74.77%`、pooled SuperNode 占 `24.52%`，两者均保留完整
用户态和内核态后续调用链。

对应驱动 workload 为 `14.122M QPS`、P99 `0.567ms`；64 worker 全部 join，fallback、backpressure、
vector read failure、NOT_FOUND、server error、handle dereference failure 和 handle miss 均为 0。
这份 replacement SVG 才是本节可用于 server 分析的火焰图：
[server all-TID SVG](../perf/aeron_cross_20260807_100k_uniform_flame_101854/server_resample/server.svg)，
其 [raw perf script](../perf/aeron_cross_20260807_100k_uniform_flame_101854/server_resample/server.alltid.perf.script)
和 [process-level folded stacks](../perf/aeron_cross_20260807_100k_uniform_flame_101854/server_resample/server.process.collapsed.txt)
也已归档。

本地归档目录为
[`perf/aeron_cross_20260807_100k_uniform_flame_101854`](../perf/aeron_cross_20260807_100k_uniform_flame_101854/)。
核心可视化产物：

- [initial server SVG (incomplete: no pooled SuperNode samples)](../perf/aeron_cross_20260807_100k_uniform_flame_101854/server/server.svg) 和 [server metadata](../perf/aeron_cross_20260807_100k_uniform_flame_101854/server/server.meta.txt)
- [process-level server collapsed stacks](../perf/aeron_cross_20260807_100k_uniform_flame_101854/server/server.process.collapsed.txt)
- [CLI SVG](../perf/aeron_cross_20260807_100k_uniform_flame_101854/client/client.svg) 和 [CLI metadata](../perf/aeron_cross_20260807_100k_uniform_flame_101854/client/client.meta.txt)
- [server raw perf script](../perf/aeron_cross_20260807_100k_uniform_flame_101854/server/server.perf.script) 和 [CLI raw perf script](../perf/aeron_cross_20260807_100k_uniform_flame_101854/client/client.perf.script)

归档还保留两端 `perf.data`、collapsed stacks、server log、prefill log 与完整 workload log。四份
原始 `perf.data`/`perf.script` 已以 SHA-256 和远端产物逐一核对一致。测试结束后，111 的 6395
实例已经停止，端口和测试 UB 设备无残留 holder。

### 21.16 跨节点火焰图 runner（2026-08-07）

测试代码提交：`b98ffae`。

`scripts/run_aeron_cross_node_flamegraph.sh` 固化了本节的 fresh-server 跨节点流程：两端 build
stamp 门禁、UB/port preflight、111 server 启动、112 顺序 v1 VADD prefill、真实 v2
`VEMB_HANDLE` read、两端本机 `perf`/FlameGraph 渲染和归档拉回 controller 本地。

默认运行 100k uniform：

```bash
bash scripts/run_aeron_cross_node_flamegraph.sh
```

文档中的 Zipf 热点场景直接指定 key pattern 和指数，例如：

```bash
KEY_PATTERN=Z:Z ZIPF_S=1.5 NUM_KEYS=100000 \
  bash scripts/run_aeron_cross_node_flamegraph.sh
```

`NUM_KEYS`、`DIM`、`THREADS`、`CLIENTS`、`PIPELINE`、`BATCH_REQUEST_SIZE`、
`BATCH_MAX_DELAY_US`、`PIO`、`SNW`、CPU mask、UB path、
manifest、采样 event/frequency/duration、节点和输出目录均可由同名环境变量覆盖。`BUILD=verify`
（默认）要求两端当前 O3/LTO/SVE stamp；源码同步后用 `BUILD=build` 强制重建。

server 不能再以仅 process leader 的采样替代全线程采样。runner 在 prefill 后枚举
`ps -T -p <redis-server-pid>` 的全部 TID，以 `perf --tid` 采集；随后把 `vemb-io-*`、
`vemb-sn-*` 等 thread comm 统一归并为 `all -> redis-server` 根，但保留 proxy/SuperNode 的调用链。
默认硬性要求 raw script 同时含 `vemb-sn-*`、`supernode_pool_thread_main` 和
`proxy_io_pool_thread_main`，否则拒绝产出有效 server 图。`REQUIRE_VEMB_THREAD_SAMPLES=0` 仅用于
明确知道 SuperNode 应无 CPU 样本的诊断场景。

每次运行的完整 server/client `perf.data`、`perf.script`、collapsed stacks、SVG、build metadata、
thread manifest、prefill/workload/server log 分别打包在远端临时 run directory，并解包到本地
`LOCAL_ROOT/server` 与 `LOCAL_ROOT/client`。默认完成或失败时停止 fresh 6395 server；使用
`KEEP_SERVER=1` 才保留它。`DRY_RUN=1` 只验证参数与输出布局，不执行 SSH、构建、测试或采样。

**runner smoke 验证（2026-08-07）：** 实际执行了 15 秒 `R:R` 和 15 秒
`KEY_PATTERN=Z:Z ZIPF_S=1.2` 的 100k smoke。两种场景均完成远端归档、本地拉回和 server 自动
清理；Zipf 场景为 `28.111M QPS`、P99 `0.343ms`，64 worker 全部 join，fallback、backpressure、
vector read failure、non-OK response 与 handle dereference failure 均为 0。其 server raw trace
分别含 8,276 个 `vemb-sn-*`、8,207 个 `supernode_pool_thread_main`、18,341 个
`proxy_io_pool_thread_main` 和 19,580 个 kernel frame，满足完整 server 图门禁。

runner 向 SSH 传递可选的 `ZIPF_S` 时使用非空哨兵值，并在远端还原为空字符串。原因是 OpenSSH
会丢弃空的位置参数；未处理时 uniform `R:R` 的后续参数会左移，导致远端 shell 以错误字段作为
线程数或 CPU mask。该修复已由上述 uniform 与 Zipf smoke 同时覆盖。

**server CPU 归档（2026-08-07）：** runner 仅对 111 server 采集 CPU，不对 client 增加 CPU
统计。`server.cpu.process.tsv` 使用 Redis 进程 jiffies，在固定 `TEST_TIME` 窗口给出 `user`、
`system` 和 `total=user+system` 的秒数及等效核数。`server.cpu.cpuset.mpstat.txt` 保留
`SERVER_CPU_MASK` 内每个 CPU 的原始 `mpstat` 数据；`server.cpu.cpuset.summary.tsv` 汇总
`usr/nice/sys/iowait/irq/soft/steal/guest/gnice/total` 的平均百分比和等效核数，其中
`soft` 即 `si`，`total=100-idle`。`soft/irq` 属于该 CPU set 的系统 CPU 时间，不能错误归因成
Redis 进程自身时间。`server.cpu.summary.txt` 将这些数据渲染为固定列宽的可读表，并在 runner 成功
结束时输出；该表不显示 `idle` 行，只保留 `total`。

**跨机测试环境门禁（2026-08-07）：** 在一次无效 smoke 中，client 仅有 `5.21M QPS`、P99
`15.7ms`，而 111 的 CPU set 却显示 `42.093` 核。复核发现该 host 上已有另一 `redis-server`
在 1 秒 `pidstat` 样本中占用约 38 核；因此 CPU-set 的系统总量不能归因给 fresh server。runner
现在在 111/112 启动任一测试角色前强制执行 load gate：默认拒绝单进程超过 `10%` CPU、全进程合计
超过 `20%` CPU，或单进程 RSS 超过 `256MiB` 的情况，并打印 blocker 的 PID、CPU/RSS 与 command。
原始 `pidstat` 和 blocker 清单归档随样本保存。阈值可用
`MAX_FOREIGN_CPU_PCT`、`MAX_FOREIGN_TOTAL_CPU_PCT`、`MAX_FOREIGN_RSS_MB` 调整。
当确认 `opencode` 与本轮无关时，可显式设定 `KILL_OPENCODE=1`：runner 在两端 gate 前只匹配
comm 精确为 `opencode` 的进程，发送 `SIGKILL` 并确认退出后再继续；默认 `0` 不执行任何杀进程操作。

**火焰图参数标签（2026-08-07）：** server/client SVG 的顶部 title 使用 compact run label：key 数和
分布、dim、`PIO/SNW`、`t/c`、pipeline、batch、batch delay、测试/采样时长和 `RUN_ID`；subtitle
额外标注 role、CPU mask、event 与采样频率。这样从独立 SVG 即可辨别样本参数，无需依赖目录名。

**load-gated 100k distribution 复测（2026-08-07）：** 清理与本轮无关的 `opencode` 和 `mutagen-agent` 后，runner
在两端通过 CPU/RSS gate（server/client 的 CPU/RSS blocker 文件均为空）并运行 100k、dim=300、
`PIO=21,SNW=21,t=64,c=4,pipeline=32,batch=32,max-delay-us=0` 的 30 秒 V2 `VEMB_HANDLE` read。
三轮都完成 64 worker join，fallback、backpressure、vector-read failure、non-OK response、handle
failure 与 server handle miss 均为 0；25 秒 server all-TID `perf` 与 client process `perf` 均已归档，
测试结束后 6395 已释放。

| key distribution | QPS | P99 | server process total cores | server CPU-set total cores | artifacts | server SVG | client SVG |
|---|---:|---:|---:|---:|---|---|---|
| Uniform `R:R` | 13.873M | 0.583ms | 27.641 | 28.091 | [`aeron_cross_uniform_100k_20260807_1220`](../perf/aeron_cross_uniform_100k_20260807_1220/) | [server](../perf/aeron_cross_uniform_100k_20260807_1220/server/server.svg) | [client](../perf/aeron_cross_uniform_100k_20260807_1220/client/client.svg) |
| Zipf `s=1.2` | 28.359M | 0.343ms | 28.955 | 29.725 | [`aeron_cross_zipf12_20260807_1215_r2`](../perf/aeron_cross_zipf12_20260807_1215_r2/) | [server](../perf/aeron_cross_zipf12_20260807_1215_r2/server/server.svg) | [client](../perf/aeron_cross_zipf12_20260807_1215_r2/client/client.svg) |
| Zipf `s=1.5` | 39.321M | 0.247ms | 29.226 | 30.183 | [`aeron_cross_zipf15_20260807_1215`](../perf/aeron_cross_zipf15_20260807_1215/) | [server](../perf/aeron_cross_zipf15_20260807_1215/server/server.svg) | [client](../perf/aeron_cross_zipf15_20260807_1215/client/client.svg) |

三组 server/client SVG title 分别含 `keys100000_RR_...`、`keys100000_Z1.2_...` 和
`keys100000_Z1.5_...` 的完整 compact run label，可在不依赖目录名的情况下识别参数。

15 秒 100k uniform smoke 验证中，Redis 进程采样窗口为 `15.004s`，user/system/total 分别为
`25.632/0.592/26.224` 核；绑定的 48 核 CPU set 的 user/sys/irq/soft/total 分别为
`25.727/0.612/0.274/0.017/26.630` 核。CPU 采样与 client workload 同时开始，独立 sampler 在
固定窗口结束时写入进程结果，因而不包含随后的 client `perf script` 和 SVG 渲染时间。

### 21.17 v2 request ready bitmap 落地（2026-08-07，工作区未提交）

测试代码基线：`b98ffae` + 工作区未提交变更。

针对 14:14、16:16、18:18 的 CPU/QPS 对照，v2 batch channel 已加入 request/completion 共用的
ready bitmap：client 在 descriptor 发布后置 bit，PIO 以 acquire exchange 取走 bit 并只 poll 对应
channel；SuperNode completion 发布后也置相同 bit，避免最后一个 request 的 response 停留在 completion
ring。旧 v1 与没有协商 bitmap 的通道保留周期 snapshot probe，以兼容永久 fallback channel。

布局、内存序的无丢通知证明、close/reuse 语义、epoch 不在热路径递增的原因、分阶段范围与性能验收
要求见 [ready bitmap 设计](VEMB_V16_AERON_READY_BITMAP_DESIGN_20260807.md)。该变更尚未写入性能结论；
必须完成 fresh-server 跨节点复测后才可比较 CPU cores 与 QPS。

**首次实现复测：** 在 `PIO=14,SNW=14`、30 秒、load gate 全通过的 fresh 样本
[`aeron_cross_20260807_164350`](../perf/aeron_cross_20260807_164350/) 中，启用 bitmap 的 QPS 仅
`4.840M`、P99 `0.279ms`、server process `9.565` cores。虽然相对之前 14:14 的 `13.548` cores
下降，但 QPS 相对 `13.477M` 下降约 `64.1%`，不能作为优化接受。ready-map 创建日志为
`/dev/obmm_shmdev3@761856`、`14 lanes x 10 words`，64 worker 均 join，所有 publish failure、fallback、
backpressure、non-OK status 和 handle dereference failure 均为零；因此这不是错误或回退导致的假下降。

该实验代码随后已撤回：当前 attach ABI、SDK 和 server 均不再包含 ready bitmap，v2 默认恢复完整 PIO
poll；设计文档和失败样本仅作为实验记录保留。后续若重做通知机制，须先以低成本 doorbell/ack 替代跨机
shared-word 原子 RMW，并通过同参数 A/B 后再更新默认策略。

### 21.18 Aeron lane snapshot 热路径优化（2026-08-07）

测试代码提交：`b98ffae`。

检查发现现有双缓冲 snapshot 已在控制面按 `channel_index % PIO` 分配到对应 worker 的独立列表，因而
PIO 热循环中再次执行同一个 modulo 过滤是重复工作。代码撤回 ready bitmap 后，删除该重复判断，保留
snapshot 内部的 channel-index 边界检查；v1 channel（包括 VADD/VSIM）和 v2 channel 的扫描语义不变。

在两台机器同步并重建后，使用相同的 100k workload（`t=64,c=4,pipeline=32,batch=32`、30 秒、
`PIO=16,SNW=16`）进行 uniform 和 Zipf fresh-server 测试。uniform 两轮中按 QPS 选取较好的一组，
与两个 Zipf 场景放在同一张表中：

| distribution | run | QPS | P99 | server process cores | artifacts |
|---|---|---:|---:|---:|---|
| Uniform `R:R` | `aeron_cross_20260807_171001` | 13.8276M | 0.655ms | 14.633 | [perf](../perf/aeron_cross_20260807_171001/) |
| Zipf `s=1.2` | `aeron_lane_snapshot_p16_s16_zipf12_frame_retry_r2_20260810_095505` | 18.474646M | 0.487ms | 13.176 | [perf](../perf/aeron_lane_snapshot_p16_s16_zipf12_frame_retry_r2_20260810_095505/) |
| Zipf `s=1.5` | `aeron_lane_snapshot_p16_s16_zipf15_frame_retry_20260810_095815` | 23.252459M | 0.383ms | 12.314 | [perf](../perf/aeron_lane_snapshot_p16_s16_zipf15_frame_retry_20260810_095815/) |

两轮均为 64 worker join，publish、fallback、backpressure、non-OK response 和 handle dereference failure
均为 0。两轮均值为 `13.8219M QPS / 14.623 cores`，相对旧 `16:16` 结果
（`13.723M / 14.864 cores`）约提升 `0.7%` QPS、降低 `1.6%` server cores；收益较小，但没有吞吐回退。

三组均为 `100K, PIO=16, SNW=16, t=64, c=4, pipeline=32, batch=32, 30s`；Zipf 两轮完成 v2 response
arena 临时可见性重试后，均为 64 worker join，最终 `fallback_v1`、publish failure、backpressure、
stale/non-OK/unmatched response、vector read failure 和 handle dereference failure 均为 0。response retry
warning 仅表示 client 读到尚未完整可见的 response arena 并重试 descriptor，最终不会消费该帧的部分内容；
其不计入数据面失败。

### 21.19 batch response body 可见性根因与完整 frame 提交（2026-08-10，工作区未提交）

测试代码基线：`b98ffae` + 工作区未提交变更。

**当前 SSH 入口：** 本文仍按逻辑角色称为 `111`（server）和 `112`（client），分别直接登录
`ssh -p 22 root@192.168.90.111` 与 `ssh -p 22 root@192.168.90.112`。这些地址用于 VEMB TCP
服务端点时，仍按具体服务端口区分。

`PIO=18,SNW=18,100K Zipf s=1.2,batch=32` 的一次样本曾在 client 读到
`sequence=68511,batch_id=68511,index=3` 时出现混合的 `region_id/offset`，同一 descriptor
稍后重试后恢复为合法 handle。该 response arena 段在 descriptor 尚未 consume 时不会被 server
复用，且 server completion 到 response entry 的字段复制已核对正确。因此根因不是 handle 构造或
复用旧 frame，而是跨机 UB 上 descriptor/final marker 已可见而 response body 尚未完整可见。

**出现过程与证据：** v2 response 的 writer 是 server 对 import UB 的 NC 映射，reader 是 client
对同一 UB 的本地 CC 映射。server 先在 response arena 写 frame body，再写尾部 commit，最后 release
发布 response descriptor。release 保证 server CPU 的写入程序顺序，但它不把多个 UB cache line 合并成
client 一次原子可见的整帧。arena 复用或传播延迟时，client 可以先获得新 descriptor、新 trailer 和当前
`batch_id`，而 entry body 的部分 cache line 仍是上一帧或未完全传播的内容。

时序如下，其中 `body line A/B` 是同一 response frame 的不同 cache line。`release` 约束的是
server 发出写入的先后关系；它不是跨机 UB 的“整帧到达确认”，不会令 client 的 CC 映射在一个原子时刻
同时更新 A、B、trailer 与 descriptor。

```text
server: import UB / NC writer       UB propagation          client: local UB / CC reader
-----------------------------       --------------          ----------------------------
write body line A (new)  ---------> A becomes visible
write body line B (new)  ---------> B propagation delayed
release fence
write checksum (new)     ---------> checksum becomes visible
release fence
write batch_id (new)     ---------> batch_id becomes visible
publish descriptor       ---------> descriptor/tail visible
                                                            acquire descriptor
                                                            read batch_id == descriptor id
                                                            read A == new, B == old

current protocol:                                           only batch_id matched: decode/consume
                                                            -> mixed handle can escape

checksum experiment:                                        hash(A,new + B,old) != trailer
                                                            -> retain ring head and retry

                                  B becomes visible  -----> read A,B all new
                                                            hash(body) == trailer
                                                            -> decode and consume
```

因此这里的 `release` 仍然必要：它确保 server 不会在程序顺序上先发布 descriptor 再写 body 或 trailer。
但在 UB 的 NC-to-CC 最终可见性模型中，它不等价于“client 已经取得完整 frame”的 ack，也不为多个独立
cache line 提供 transaction/atomic-snapshot 语义。checksum experiment 只能隔离错误消费，不能改变
UB 的可见性；该实验已撤回，不能作为当前修复方案。

这次异常按以下链路首次被定位：

1. 在 `PIO=18,SNW=18,100K Zipf s=1.2,batch=32` 下，client 已看到 `sequence=68511` 与
   `batch_id=68511`，所以不是 descriptor slot 复用、顺序错乱或旧 frame 的 batch-id mismatch。
2. decoder 可通过旧的 magic、长度和 `batch_id` 校验，但 `index=3` 的 `VEMB_HANDLE` 出现
   `region_id=3823584000, offset=2364844800, bytes=1200`。`bytes=1200` 仍等于 `dim=300` 的正确长度，
   但 region id 不属于 client 已映射 warm region；该组合不可能由合法 completion 产生。
3. 定位期间，client 在 consume descriptor 前临时增加 warm-region span 校验，拒绝该 entry 并保持 ring
   head；同一 `sequence/batch_id/index` 的后续 poll 读到合法 handle，输出 `visibility recovered`。期间
   server 不能复用该 arena 段，排除了 server 后续覆盖导致的读写竞争。该临时 validator 已随实验撤回。
4. server 侧 `completion_set_vector_handle()` 和 `vemb_v16_make_response_from()` 已核对为从同一个
   completion 一致复制 `region_id/vector_offset/vector_bytes`，因此排除 response 构造的字段赋值错误。
5. 结论是旧 frame body、当前 frame body 与当前 control/trailer 在 client CC 映射的可见时间不同。
   这不需要、也不能以 `dc cvac`、`dc ivac` 或 `dsb sy` 修复：UB 负责 NC writer 到 CC reader 的最终
   可见性；应用协议无法把这种范围级部分可见修复成原子整帧可见。

**已撤回的应用层隔离实验：** 曾把 request/response trailer 从 8-byte `batch_id` 扩为
`u64 batch_id + u64 body_hash`，并在 consume 前校验完整 body。首次采用逐字节 FNV-1a 的 P18 回归
[`aeron_frame_checksum_p18_s18_zipf12_b32_20260810_1130`](../perf/aeron_frame_checksum_p18_s18_zipf12_b32_20260810_1130/)
证明完整性语义正确：`fallback_v1=0`、`shared_vector_read_failures=0`、handle dereference failure=0，
且 22 次未完整 response 都被 `decode_rc=-1` 重试而没有消费。该实现仅得到 `7.445M QPS`，远低于
旧 P18 样本，原因是 FNV-1a 对每个 frame body 的串行逐字节乘法成为双端热点；该数据不得用于性能对比。
后续曾改为 `XXH3-64`，但尚未以它建立有效的跨机性能样本。该实验以及临时 span validator 均已撤回：
当前代码恢复原始 8-byte `batch_id` trailer，未改变 frame ABI。原因是它们只是防止 client 错误消费的
应用层补偿，不是 UB 连续 range 部分可见的根因修复。

**当前待解决项：** UB 必须明确并实现范围级 publish 语义：server 在写完一个连续 response frame 后，
通过 UB 提供的完成/doorbell/fence 原语发布 descriptor；client 在观察到该发布后必须看到该范围内全部
cache line 的本轮内容。若 UB 的现有 contract 已承诺这一点，则本现象是 UB 实现或 driver 的可见性 bug，
应以最小 UB 可见性 UT 稳定复现并在 UB 侧修复；不得继续以 checksum、`dc cvac`、`dc ivac` 或客户端
retry 伪装为根治。

本轮还修复了 `sync_changed_code_to_peer.sh --all-code` 的候选文件枚举：旧实现使用
`git ls-files -co`，会遗漏已跟踪但未暂存的工作区修改，因而可能只同步部分 frame ABI 源码。
现在 `--all-code` 显式合并全部 tracked 文件与 untracked 文件，再按工作区内容 hash 比较；所有跨机
重建前都应使用该模式，不能依赖 staging 状态。

**完整 frame UT（本地与跨机复现完成）：** `benchmark/ub_cc_nc_visibility_ut` 已增加
`frame-writer-nc` 和 `frame-reader-cc`。它复用同一 4KiB（可配置）frame，frame 首尾都写入 generation，
body 每个 word 写入 `(seed, generation, index)` 的确定性 pattern，descriptor 在 body 后发布。反向 UB ack
强制 writer 等待 reader 检查完整 frame 后才开始下一代，所以检测到混合 body 时可排除 writer 复用覆盖。

本地普通共享文件已经验证：1,000 次无注入复用不误报；第 2 代通过 UT 专用
`--inject-mixed-at 2` 保留一个旧 body word 时，reader 的 `--expect-mixed` 正确输出
`FRAME_VISIBILITY_FAILURE type=mixed generation=2 descriptor=2 index=3`，且 header/trailer 都为当前 generation。这只验证
检测器，不能替代真实 UB 路径。两机恢复后应使用无注入版本稳定复现：

```text
111 frame-writer-nc: data=/dev/obmm_shmdev6 (NC write), ack=/dev/obmm_shmdev3 (CC read)
112 frame-reader-cc: data=/dev/obmm_shmdev2 (CC read), ack=/dev/obmm_shmdev7 (NC write)
```

新的 SSH 入口下，按该路径执行无注入 `frame_bytes=4096, iterations=1000000` 已在第 2 代复现：
`FRAME_VISIBILITY_FAILURE type=marker generation=2 descriptor=2 header=2 trailer=1`。这说明当前
descriptor 与 header 已在 112 client CC 映射可见，但 trailer 仍为上一代；writer 等待本代 ack，故不能归因于
arena 提前复用。该次 reader 使用 `--expect-visibility-failure` 并返回 `0`。常规复现中该开关对任一
marker/body 部分可见失败均返回 `0`；
`NOT_REPRODUCED` 返回 `3`，明确表示该次未触发，不能记录为通过。`--expect-mixed` 只用于严格要求 body
混合且 frame 首尾 marker 均为当前 generation 的场景。

**cacheline 隔离尝试（2026-08-10，已撤回）：** response frame codec 曾调整为 `64B header | entries
body（每个 entry 固定 64B）| padding | 64B commit line`，commit `batch_id` 保持该 frame 的最后 8B，
frame 总长度也对齐至 64B，因而 header、entries、commit 以及相邻 frame 不再共用 cacheline。32 个
`VEMB_HANDLE + OK` entry 由紧凑布局的 `1184B` 增加为 `2176B`。本地与 111/112
`vemb_v16_batch_ring_ut` 均通过。本地 UT 已固定断言 32-entry frame 的 `2176B` 长度、64B 对齐和
encode/decode round-trip。

按相同 `111 dev6 NC -> 112 dev2 CC`、`111 dev3 CC <- 112 dev7 NC` ack 路径，以精确 `2176B`、32 个固定
64B entry 的 layout 无注入复测，仍在 generation `2` 复现
`FRAME_VISIBILITY_FAILURE type=marker descriptor=2 header=2 trailer=1`，reader 使用
`--expect-visibility-failure` 返回 `0`。因此这不是 header/body/trailer 共享 cacheline 导致的局部布局
问题，也不是 entry 跨 cacheline 导致，而是连续 cacheline 的 UB 范围可见性仍可跨行混代。因此该 ABI 已撤回，
当前代码恢复为 24B header、可变长 entry 和末尾 8B commit 的紧凑布局。本 checkpoint 未用新 ABI 重建完整 Redis/client
并运行数据面 workload，不能当作端到端回归结果。

### 21.20 CLI worker-owned L1 completed-vector cache 落地与验证

本 checkpoint 的目标是先落地 CLI L1，而不是 proxy L2：在已经预填充、无写入、纯
`VEMB_HANDLE` 读取的 benchmark 中，让 completed-vector hit 完全绕开 channel、server 和 UB
warm-region dereference。这样不要求 server、proxy、ATTACH 或 wire ABI 改造，直接削减 UB 读取和
client 端 vector copy；L1 默认关闭，只有显式配置 cache entry 数时才启用。

21.19 记录的 UB 连续 frame 范围级 publish 可见性问题已经确认属于硬件问题并完成修复，
不再作为 L1 实现的前置门禁。L1 仍只缓存一次已经完整 materialize 的成功 vector；端到端测试
必须继续验证 response/frame 完整性、handle dereference 成功率和 stale/unmatched response 为零，
不能用缓存结果掩盖数据面错误。

#### 21.20.1 首版语义和边界

- 仅接纳成功的 `VEMB_HANDLE` response，且 workload 必须是 prefill 后无 `VADD`、`VREM`、迁移、
  扩容或 topology epoch 变化的 immutable pure-read 阶段；`NOT_FOUND`、非 `OK`、fallback、超时、
  `MOVED`/`ASK` 和任何 handle dereference failure 都不得填入 L1。
- 首版没有 TTL，也不宣称跨 client 写入下的 strict 或 bounded-stale 语义。当前阶段不存在写入，
  因此 entry 在 worker 生命周期内保持有效，直到显式 clear、worker 退出或容量淘汰。
- key 是完整 final VEMB key bytes；worker 的 L1 在创建时固定 `dim`，entry 不重复保存 `dim`。
  hash/fingerprint 仅用于定位，命中必须比较完整 key，不得 hash-only 命中。value 保存已经从 UB
  物化出的完整 vector snapshot，以及后续完成所需的最小成功 metadata，不保存仅含 `offset` 的 handle。
- 首版 entry 固定为 32B；显式保留 4B `reserved`，后续新增 metadata 时优先消耗该空间：

```c
typedef struct vemb_v16_cli_l1_entry {
    uint64_t key_hash;      /* Hash used to select the cache set. */
    uint16_t key_len;       /* Exact final-key length in bytes. */
    uint16_t vector_bytes;  /* Materialized vector size in bytes. */
    uint32_t key_slot;      /* Index into the preallocated key slab. */
    uint32_t vector_slot;   /* Index into the preallocated vector pool. */
    uint32_t generation;    /* L1 slot generation used to reject stale refs. */
    uint16_t pin_count;     /* Number of local completions holding this entry. */
    uint8_t  valid;         /* Entry contains a completed vector. */
    uint8_t  clock_ref;     /* CLOCK replacement reference bit. */
    uint8_t  reserved[4];   /* Reserved for future metadata fields. */
} vemb_v16_cli_l1_entry_t;

_Static_assert(sizeof(vemb_v16_cli_l1_entry_t) == 32,
               "L1 entry must remain 32 bytes");
```

  `generation` 是 L1 slot 重用代际，用于拒绝旧 completion 对新 entry 的 stale release；它不能
  与远端 warm location 的 `owner_generation` 合并。`vector_bytes` 保留用于 materialize 后的
  shape 校验（当前最大 `DIM=4096` 时不超过 `16384` bytes）。
- `slot_meta`、key hash/fingerprint、`owner_generation` 和 `write_seq` 的 warm-state 校验本轮不实现。
  后续在允许写入或迁移前必须补齐如下语义，届时不通过则 invalidate 并重新从 server 解析：

```c
/*
 * TODO(vemb-v16): Before serving an L1 vector, validate the cached
 * region/slot against warm slot_meta state, key hash/fingerprint,
 * owner_generation, and write_seq. A same-owner write_seq change
 * should refresh from UB; owner/key mismatch must invalidate the
 * entry and resolve a new handle through the server.
 */
```

#### 21.20.2 所有权、并发和容量

memtier 的一个 worker 是 L1 的唯一 owner；该 worker 所持有的全部 batch session 共享一个 L1。
现有 L0 保持 batch-session-owned：它只合并同一 session 中尚未完成的相同 key，不保存 completed
value。L1 位于其之前并跨该 worker 的 session 复用 completed vector，二者不可合并为同一个 table。

所有 L1 lookup、insert、evict、pin/release 和统计均只由 owner worker 执行，首版不使用 mutex、
原子 hash table 或跨 worker 共享缓存。每 worker 在启动时一次性预分配固定 entry pool、key storage 和
vector storage；热路径不得 malloc、rehash、创建链表或按 entry 分配 vector。建议首版使用固定容量、
4-way set-associative table 和 CLOCK replacement；entry 被 local completion 引用期间必须 pin，CLOCK
不得淘汰，完成回调处理后 release。pool/set 全部 pinned 或满时视为运行期资源压力，直接 miss 并走现有
L0/batch 路径，不临时扩容。

L1 的索引遵循 Valkey/L0 已验证的局部模式：`key_hash -> set`、高 8 位 `h2` 指纹预筛、完整 hash/长度/
key bytes 确认；`h2` 永远不是命中依据。L1 固定为 `N/4` 个 set、每 set 4 个 way，set metadata 保持
一条 cache line；它不采用 Valkey 的 child bucket、动态扩缩容或 incremental rehash。**TODO(vemb-v16):**
当 L0、L1 和 server 侧出现足够多相同的 worker-local fixed-hash 使用者时，统一沉淀 hash、`h2`、
exact-key compare、generation 和 bucket layout 的设计规范或窄索引底座；不得把 L0 的 in-flight 生命周期、
L1 的 pin-aware CLOCK 或 server 的 authoritative/rehash 语义强行合并到同一个通用表。

初始默认建议为每 worker `4096` entries。`dim=300` 时 vector payload 约 `4.7 MiB/worker`，64 worker
约 `300 MiB`，不含 key 和 entry metadata；最终值必须由实际 worker 数、DIM、RSS 预算和热点分布复核。
100K uniform 在此容量下的 hit ratio 可能很低，4-key hotspot 与 Zipf 才是首要收益场景。

CLI 打开 server 广告的 warm region 时只允许读权限：使用 `O_RDONLY` 和 `PROT_READ` 的映射。
request/response ring 以及 batch descriptor/arena 仍必须使用 `O_RDWR` 和可读写映射，因为 CLI
需要发布请求并推进/消费 ring metadata。warm region 映射不得成为修改 server warm 数据的路径。

#### 21.20.3 请求、填充和本地完成路径

```text
logical VEMB_HANDLE read (worker-owned)
        |
        v
L1 exact-key lookup
   | hit                         | miss
   v                             v
enqueue local completion     current session L0 admission
                                  | follower: wait for leader
                                  v
                              batch publish -> server -> response
                                  |
                                  v
                         materialize UB vector exactly once per L0 group
                                  |
                                  v
                         L1 put exactly once, then L0 leader/follower fan-out
```

L1 hit 不能伪造 Aeron response，也不能直接重入现有 submit 栈。每个 worker 维护固定容量的 local
completion queue，容量至少覆盖该 worker 的 `batch sessions * pipeline` 最大未完成量。queue item 保存
原逻辑请求的 caller cookie/`req_id`、完成所需的 response 信息，以及被 pin 的 L1 entry index 和
generation；owner worker 在正常 poll/accounting 阶段消费 queue、回调并 release pin。因此 hit 和 miss
走相同的逻辑 completion 记账、延迟统计和 pipeline credit 归还，而命中不触发 channel/server/UB 访问。

响应回填必须挂在 batch client 的“一个 L0 group 已获得成功 response、尚未释放该 group”这一点：先取得
group 的精确 key，materialize 一次 UB vector，调用 worker-owned L1 fill hook 一次，再使用同一 materialized
结果完成 leader/follower fan-out。不得在每个 follower 回调中重复填 L1，也不得在 `vemb_v16_cli_l0_finish()`
释放 group/key 后再尝试回填。

推荐 batch client 提供一次-per-group 的 materialization hook（可作为现有
`poll_shared_vector()` 的扩展 API），回调参数包含 borrowed 的完整 key、成功 response metadata
和临时 vector view。worker 在回调中把 vector 复制到预分配的 L1 vector pool，然后才调用 L0 finish。
该临时 view 只在 callback 期间有效，L1 不得保存其指针。

严格时序为：

```text
response item
  -> resolve L0 group
  -> borrow group key
  -> materialize vector
  -> L1 put exactly once
  -> l0_finish()
  -> leader/follower fan-out
```

只有 `status == OK` 且 handle dereference 成功的正常 VEMB_HANDLE response 才能触发该 hook；
`NOT_FOUND`、非 OK、fallback v1、stale topology、超时、关闭错误和 dereference failure 均不得填充 L1。

#### 21.20.4 channel 路由与 batch-agg 的关系

首版保留当前 session/channel round-robin：worker 仍按现有 `next_ch` 选择 channel，key 在该选择之后生成。
这样可以把 L1 A/B 与 channel 调度、per-channel batch 聚合率的变化隔离开。L1 hit 根本不进入 channel；
只有 L1 miss 继续使用当前 L0 和 batch-agg，故不会破坏已有 channel ownership 或 SPSC 假设。

后续可单独实验“仅对 L1 miss 按 key 固定路由 channel”，以集中热点并提高单 session L0/batch-agg 的
相同 key 合并概率。但它会改变 channel 负载、tail latency 和 backpressure 分布，不能与第一版 L1 收益
混在同一次比较中；应在 L1 自身 A/B 正确后再以独立 checkpoint 评估，且仍不得跨 worker 共享 L1。

#### 21.20.5 分阶段实现、checkpoint 和验收

L1 代码按以下阶段落地。每个阶段完成后单独形成 checkpoint，不把多个阶段的性能收益混在一个
提交或一张结果表中。每个 checkpoint 必须记录：测试代码提交、工作区状态、改动文件、UT/build
命令、关键计数和未覆盖风险。存在未提交改动时，结果目录必须归档对应的 `git diff`。

**L1-P0：权限和基线冻结。 DONE（2026-08-12）**

- 保留已完成的 warm-region 只读约束：CLI warm mapping 使用 `O_RDONLY + PROT_READ`；req/resp
  ring 和 batch descriptor/arena 继续使用 `O_RDWR + PROT_READ|PROT_WRITE`。
- 建立 L1 disabled 基线，不改变请求、L0、batch 或 completion 语义；确认 21.19 硬件修复后的
  response/frame 完整性、handle dereference、stale/unmatched response 均正常。
- 验收完成：`git diff --check`、`make -C clients/c`、`make -C memtier_benchmark -j2`。
  两个构建均成功；构建输出中的既有 unused-variable、uninitialized-warning 和 macOS linker
  deprecation warning 不属于本阶段改动。4-key hotspot 的 disabled 运行参数和结果尚未在本阶段
  归档，待跨节点测试环境可用后作为独立基线 checkpoint 补充，不阻塞权限边界和构建验收。
- checkpoint：当前工作区包含 warm-region 只读映射改动；工作区状态为基线提交加未提交 diff，
  后续归档测试结果时必须同时保存该 diff。P0 未实现 L1 entry、lookup、local completion 或回填路径。

**L1-P1：独立固定池模块。 DONE（2026-08-12）**

- 新建 `clients/c/internal/vemb_v16_cli_l1.h` 和 `clients/c/vemb_v16_cli_l1.c`；entry 固定为
  32B（含 4B `reserved`），worker 创建时固定 `dim`，不在 entry 重复保存 `dim`。
- API 只负责 worker-local cache：`create/destroy`、exact-key lookup、put、pin/release、clear
  和 stats。固定 key slab、vector pool、4-way set table 和 CLOCK replacement；热路径禁止 malloc、
  rehash 和按 entry 分配。
- UT 覆盖完整 key collision、CLOCK eviction、all-pinned miss、generation ABA、key/vector storage
  exhaustion，以及 hit/insert/evict 计数。此阶段不接入 memtier，不改变线上路径。
- 验收：L1 UT、`make -C clients/c`、`git diff --check`。
- 已完成：新增固定池模块及 `vemb_v16_cli_l1_ut`。L1 使用 `N/4 x 4-way`、一条 cache line 的 set
  metadata、`h2` 预筛和 full-key confirm；每次 lookup 自动 pin，返回 `entry_id + generation`，只有
  release 后才允许 CLOCK 淘汰。UT 已覆盖相同 hash/h2 的完整 key collision、CLOCK replacement、
  all-pinned resource pressure、stale generation ABA、key/vector pool exhaustion、clear invalidation
  及 hit/insert/evict 计数。
- 验收完成：`make -B -C benchmark vemb_v16_cli_l1_ut`、`make -B -C benchmark
  vemb_v16_cli_l0_ut`、`make -C clients/c`、`git diff --check` 均通过。测试代码提交：
  `5ab79d39babd79549492b44cfb1e32596ef99f15`；工作区状态：基线 `5ab79d39` + 未提交 diff。P1
  仅构建独立模块，未改 memtier、batch-agg、server、proxy 或 wire path；端到端填充/本地完成仍留给 P2/P3。

**L1-P2：worker 生命周期和本地完成队列。 DONE（2026-08-12）**

- 增加 `--vemb-v16-l1-entries=N`，默认 `0`（关闭）；在每个 memtier worker 创建/销毁一个 L1，
  所有该 worker 的 batch session 共享它。
- 增加固定容量 local completion queue，容量至少覆盖 `batch sessions * pipeline`。queue item
  保存 caller cookie/req_id、sent time、L1 entry index 和 generation；命中只入队，不直接重入
  submit 或 callback。
- 仅接入纯 `VEMB_HANDLE` consumer 的 submit 前 lookup；poll/accounting 阶段消费 local queue，
  复用现有 latency、pipeline credit 和 completion 记账，完成后 release pin。
- 验收：L1 disabled 行为与基线一致；enabled 的 synthetic local-hit UT 覆盖队列满、pin/release
  和 generation mismatch。此阶段暂不填充 L1，所有请求仍可走现有 server miss 路径。
- 已完成：新增 `--vemb-v16-l1-entries=N`（默认 `0`），只允许 pure cross-node `VEMB_HANDLE`
  batch session 使用；entry 数必须是 `4 * 2^k`，与固定 `N/4 x 4-way` L1 set layout 对齐。每个
  worker 创建一个 C SDK L1，旗下 batch session 共用；worker join 后、batch session teardown 前销毁。
- runner 的 local completion queue 只是 C++ accounting 适配：worker 启动前一次性分配
  `clients * pipeline` 个固定槽，保存 channel、caller cookie、sent time、pinned vector view 和
  C SDK `entry_id + generation` ref。命中占用原有 pending slot，只入队，不向 batch client、channel、
  server 或 UB publish；poll/drain 时构造本地 OK completion，复用现有 latency、pipeline credit 和
  get accounting，然后 release L1 pin。queue 满时 release pin 并走原有 batch submit path。
- cache 的 hash table、key/vector pool、pin/release 和 CLOCK 策略仍完全在 `clients/c` 的 C SDK；
  runner 没有复制或扩展 cache 模型。P2 没有 L1 fill/materialization hook，也没有 P4 的 L1 汇总指标，
  因而真实端到端运行仍是 L1 miss，不应据此报告 cache 性能收益。
- 验收完成：`make -B -C benchmark vemb_v16_cli_l1_ut`、`make -B -C benchmark
  vemb_v16_cli_local_completion_ut`、`make -B -C benchmark vemb_v16_cli_l0_ut`、
  `make -B -C clients/c`、`make -B -C memtier_benchmark -j2`、`git diff --check` 均通过。local
  completion UT 覆盖 FIFO/queue-full、L1 lookup pin/release 和 clear/reuse 后 stale generation ref
  rejection。P2 测试代码尚未独立提交；改动为 `memtier_benchmark/memtier_benchmark.{h,cpp}`、
  `memtier_benchmark/vemb_v16_aeron_runner.cpp`、`memtier_benchmark/vemb_v16_aeron_local_completion.h`、
  `memtier_benchmark/Makefile.am`、`benchmark/Makefile` 与
  `benchmark/vemb_v16_cli_local_completion_ut.cpp`，另更新本进度文档。构建中的既有 unused-variable、
  uninitialized-variable、C++ VLA extension 及 macOS linker deprecation warning 不属于本阶段改动；
  工作区仍是基线 `5ab79d39` 加未提交 diff。

**L1-P3：L0 group materialization hook 和回填。 DONE（2026-08-12）**

- 为 batch client 增加一次-per-L0-group 的 materialization hook，扩展现有
  `poll_shared_vector()`；hook 在 `l0_finish()` 前取得 borrowed key，并传递成功 response metadata
  和临时 vector view。
- worker 在 hook 中将 vector 复制到预分配 L1 pool，成功 `VEMB_HANDLE` 且 dereference 成功时
  `put` exactly once，然后执行原有 leader/follower fan-out。不得在 follower callback 重复填充，
  不得保存临时 view 指针。
- fallback v1、NOT_FOUND、非 OK、stale topology、超时、关闭错误和 dereference failure 不填 L1。
- 验收：batch/L0 UT 验证一个 group 只产生一次 insert，重复 follower 不增加 insert；错误收敛和
  stale response 行为保持不变。
- 已完成：C SDK 新增
  `vemb_v16_aeron_batch_client_poll_shared_vector_with_materialization_hook()`；既有
  `poll_shared_vector()` ABI 保持不变，只是以空 hook 转发到新 API。SDK 仅在一个 v2、`PUBLISHED`
  L0 group 收到 `OK VEMB_HANDLE`、`vector_bytes == dim * sizeof(float)` 且完整 warm-region read
  成功后，在 `vemb_v16_cli_l0_finish()` 前同步交付 borrowed key/vector view。v1 direct、v1 fallback
  和所有非 OK、错误 shape 或 read failure 均不触发 hook。
- worker 只在 L1 已启用且未使用 `VEMB_AERON_SKIP_HANDLE_READ` 时注册 hook，并在其中立即调用 C SDK
  `vemb_v16_cli_l1_put()` 复制 vector；固定池资源压力的 put 失败不会改变原有远端 completion 或
  leader/follower fan-out。runner 不保留 scratch view，也没有实现第二套 cache 模型。
- 新增 `vemb_v16_cli_l1_materialization_ut`，通过真实 L0 `submit -> publish -> resolve -> finish`
  状态机验证 hook 早于 fan-out、leader/follower 只插入一次；另覆盖 invalid vector、非 OK 和
  `FALLBACK_V1` 均不插入。验收完成：`make -B -C benchmark vemb_v16_cli_l1_materialization_ut`、
  `make -B -C benchmark vemb_v16_cli_l1_ut`、`make -B -C benchmark
  vemb_v16_cli_local_completion_ut`、`make -B -C benchmark vemb_v16_cli_l0_ut`、`make -B -C clients/c`、
  `make -B -C memtier_benchmark -j2` 与 `git diff --check` 均通过。P3 测试和实现仍是基线
  `5ab79d39` 上的未提交 diff；P4 的汇总指标、日志和功能 A/B 尚未开始。

**L1-P4：统计、日志和功能场景验证。 DONE（enabled-only，2026-08-12）**

- 增加并汇总 `l1_hit`、`l1_miss`、`l1_insert`、`l1_evict`、`l1_hit_ratio`、`l1_vector_bytes`、
  `l1_ub_read_bytes_saved` 和 `local_completion_count`。后者只统计本地命中完成；saved bytes
  只按实际 L1 hit 计算，不能混入 L0 coalescing 收益。
- 本轮仅执行 100K uniform、Zipf `s=1.0`、Zipf `s=1.2` 和 Zipf `s=1.5` 的 enabled 场景；按本 checkpoint 的
  约定，未开发 CLI L1 的版本才是 disabled 版本。用户明确要求不跑 4-key hotspot，也不以本轮数据
  声称 A/B 增益。每组记录 QPS、P99、L1 hit ratio、server items、UB read bytes、client RSS 和
  server process CPU core-equivalent。
- 验收：enabled 命中不访问 channel/server/warm region；所有 fallback、backpressure、non-OK、
  stale/unmatched response 和 handle dereference failure 为零或有明确归因。
- 已完成：C SDK L1 stats 增加 `live_vector_bytes`；runner 在销毁每 worker L1 前汇总并输出一行
  `[aeron] l1-summary`，包括 `l1_hit`、`l1_miss`、`l1_insert`、`l1_evict`、`l1_hit_ratio`、
  `l1_vector_bytes`、`l1_ub_read_bytes_saved`、`local_completion_count`，以及资源压力、stale ref、
  queue-full fallback、`server_items` 和 `ub_read_bytes`。`l1_vector_bytes` 是运行末尾 live vector
  payload，不等同于 `l1_vector_capacity_bytes` 这个预分配 RSS 预算上限。
- `local_completion_count` 与 `l1_ub_read_bytes_saved` 仅在 local queue 的命中被实际消费并完成
  accounting 后增长；queue 满时 pin 被释放并走远端，因而不会虚报本地完成或节省的 UB read。`server_items`
  是 v2 unique batch item 加实际 v1 fallback/direct request，`ub_read_bytes` 是成功 materialize 的
  shared-vector read；两者不混入 L0 follower coalescing 收益。
- `scripts/run_aeron_cross_node_flamegraph.sh` 增加 `L1_ENTRIES`（默认 0）并严格检查 `0` 或
  `4 * 2^k`。每次样本将该值写进 run label，采集 client max RSS，并在
  `client.l1_summary.tsv` 归档 QPS/P99、全部 L1 指标、server items、UB read bytes 与 RSS；异常
  fallback、backpressure、read failure、非 OK、queue-full、all-pinned、storage exhaustion 或 stale ref
  直接使样本失败。脚本语法、dry-run、容量拒绝和 TSV 合成解析均已验证。
- batch-wave 修复：第一次在 `PIO=12, SNW=12, server CPU=0-15` 的 L1 enabled uniform 样本中发现
  local L1 hit 先归还一个 pipeline slot，会令该 channel 的未完成远端 L0 group 在
  `max_batch_delay_us=0` 下被 eager flush；下一轮仅补入一个 miss，导致远端 batch 退化为单项 frame。
  runner 现为 L1 enabled channel 增加 remote batch-wave gate：一个 publish wave 有远端 submit 后，
  只有该 wave 的全部远端 logical completion 已 account，才允许该 channel refill。local completion
  仍在正常 poll/accounting 阶段完成。disabled 路径不启用该 gate。新增 local-completion UT 覆盖
  wave 的 seal/complete 边界。
- 为防止外部 workload 再次污染结果，`run_aeron_cross_node_flamegraph.sh` 的启动 gate 在原有全主机
  `pidstat` 检查外，增加对目标 CPU set 的两次 `/proc/stat` snapshot 采样；target busy 超过
  `MAX_FOREIGN_CPUSET_BUSY_PCT`（默认 10%）即拒绝运行。采样用 CPU id 映射解析，避免早期
  positional `paste` 计算的列错位。
- 受控跨机验证配置：`L1_ENTRIES=4096`，100K prefill 后 immutable pure read，`t=64 c=4`
  `pipeline=32 batch=32 max_delay=0`，30s；server `PIO=12 SNW=12 taskset=0-15`，client
  `taskset=96-191`，`KILL_OPENCODE=0 KILL_MUTAGEN=0`。四场启动前 build stamp、全主机 load gate
  和 target CPU-set gate 均通过；server thread inventory 均为 12 `vemb-io-*` + 12 `vemb-sn-*`。

  | workload | QPS | P99 | L1 hit ratio | server items | UB read bytes | client max RSS | redis-server cores | CPU-set total cores (0-15) |
  |---|---:|---:|---:|---:|---:|---:|---:|---:|
  | uniform `R:R` | 7,944,863.20 | 1.391 ms | 4.0925% | 228,757,328 | 274,508,793,600 | 430,480 KiB | 10.291 | 10.833 |
  | Zipf `Z:Z`, `s=1.0` | 13,190,123.63 | 0.703 ms | 59.3575% | 160,666,648 | 192,799,977,600 | 423,252 KiB | 8.973 | 9.413 |
  | Zipf `Z:Z`, `s=1.2` | 17,628,362.45 | 0.511 ms | 83.9782% | 84,662,643 | 101,595,171,600 | 425,784 KiB | 8.397 | 8.870 |
  | Zipf `Z:Z`, `s=1.5` | 39,550,067.31 | 0.383 ms | 97.6277% | 28,091,517 | 33,709,820,400 | 411,824 KiB | 7.841 | 8.262 |

  `redis-server cores` 是 process user+system CPU time / 30s wall time；CPU-set total 还包含
  IRQ 和该 set 内其他极小系统活动，故略高于进程值。此前不具可比性的 `PIO=21/SNW=21`、server
  `0-47` 样本（25.002 cores）不纳入本表。

  uniform 的 L0 聚合为 7,454,866 frames / 228,757,328 unique items，即 30.686 items/frame，已恢复
  接近 32-item batch，证明修复消除了此前约 1.18 items/frame 的 local-hit refill 退化。Zipf 1.0/1.2/1.5
  分别为 12.987/5.138/1.415 items/frame；这是 L1 先剔除热点命中、剩余远端 key 又被 L0 合并后的实际 unique
  miss 分布，不能与 uniform 的独立 key frame size 直接比较。

  四场均为 enabled-only 功能与资源验证，不构成 disabled 对照或性能收益声明。所有样本的
  `fallback_v1`、`flush_backpressure`、shared-vector read failure、non-OK status、unmatched response、
  handle dereference failure、`l1_queue_full_fallbacks`、`l1_all_pinned`、L1 key/vector storage
  exhaustion 与 `l1_stale_ref` 均为零。完整工件位于：
  `perf/l1p4_uniform_enabled_p12_s12_svr0_15_wavefix_20260812_1732/`、
  `perf/l1p4_zipf10_enabled_p12_s12_svr0_15_wavefix_20260812_1753/`、
  `perf/l1p4_zipf12_enabled_p12_s12_svr0_15_wavefix_20260812_1735/`、
  `perf/l1p4_zipf15_enabled_p12_s12_svr0_15_wavefix_20260812_1739/`；每目录均保留 server/client
  perf data、SVG、CPU sampling、workload log 和 `client.l1_summary.tsv`。本次代码仍在基线
  `5ab79d39` 上的未提交工作区，结果不能作为已提交源码快照的 A/B 基线。

**L1-P5：扩展前的封存 checkpoint。**

- 汇总 P1-P4 的代码、UT 和 A/B 结果，冻结首版 immutable pure-read 语义，明确 entry 容量与 RSS
  预算建议。
- 在此 checkpoint 之前不实现 proxy L2、跨 worker shared cache、key-affine channel routing、
  `slot_meta/write_seq` 校验，以及可写 workload 的 TTL/lease/invalidation。
- P5 之后如需支持迁移或写入，必须单独设计 generation/失效协议和新的 checkpoint，不得把它们
  作为首版 L1 的隐式扩展。

本节只定义 worker-local completed-vector cache。proxy L2 location cache、跨 worker shared cache、
key-affine channel routing、`slot_meta` 校验，以及可写 workload 的 TTL/lease/invalidation 仍是后续独立
阶段，不能随首版 L1 一起启用或宣称完成。
