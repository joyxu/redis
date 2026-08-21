# VEMB V16 Scaleout Throughput Topology-Aware Memtier Plan

本文记录 `benchmark/hpc_redis_scaleout_throughput.sh` 中 `during_scaleout` 与
`scaleout_after` 两个阶段的正确压测口径，以及 memtier 需要补齐的
topology-aware 能力。

## 背景

VEMB V16 客户端必须按 server 发布的 topology 路由，而不是按本地 endpoint
列表 hash 分流。它读取的 topology 为：

- `current_topology_epoch`
- `min_write_epoch`
- `active_owners`
- `standby_owners`
- `owner_id -> endpoint`
- `DUAL_WRITE_REQUIRED / AUTO_SCALEOUT / COORDINATED_SCALEOUT`
- `STALE_TOPOLOGY / MOVED / ASK` 等响应状态

因此在扩容场景中会出现两个问题：

1. `scaleout_after` 阶段已经是 `active={0,1}`，但 memtier 仍可能把属于
   owner1 的 key 打到 node0，导致 miss/error，低估扩容后稳态吞吐。
2. `during_scaleout` 阶段 topology 正在从 `active={0}` 过渡到
   `active={0,1}`。如果 memtier 不处理 `STALE_TOPOLOGY / MOVED / ASK`，
   会把正常迁移窗口状态计成 miss/error，低估扩容期间可用吞吐。

正确方向是将 memtier 从 endpoint-hash client 升级为 server-topology client。

## 目标口径

### baseline

`baseline` 保持现有口径：

- 初始 topology 为 `active={0}`
- 数据预填到 node0
- memtier 只连 node0
- 统计单 owner 本地读吞吐

### during_scaleout

`during_scaleout` 的目标是测扩容窗口内业务可用吞吐：

- memtier 从 node0 bootstrap
- 启动时 fetch 当前 topology，初始应为 `active={0}`
- 后台持续读已有 key
- 脚本发布 candidate topology：
  - `active={0}`
  - `standby={0,1}`
  - `DUAL_WRITE_REQUIRED | AUTO_SCALEOUT | COORDINATED_SCALEOUT`
- coordinator 发布 full active 后 topology 变为：
  - `active={0,1}`
  - `standby={0,1}`
- memtier 在运行中遇到 topology 状态变化时 refresh 并 retry

`STALE_TOPOLOGY / MOVED / ASK` 是可恢复中间态，不应直接计为 miss/error。
只有最终重试失败才计入 error。

### scaleout_after

`scaleout_after` 的目标是测扩容完成后的两 owner 稳态吞吐：

- final topology 必须为 `active={0,1}`
- memtier 使用 server topology 将 key 路由到 owner0/node0 或 owner1/node1
- 读请求必须按 active owner 路由
- 统计两台 active owner 合计成功 QPS
- `hits ~= ops`，`misses/errors` 应接近 0

不能再用“只连 node0”或“endpoint 数组 hash”代表扩容后稳态吞吐。

## Memtier 路由契约

### 自动 topology

```text
--vemb-v16-endpoints=HOST:PORT[,HOST:PORT...]
--vemb-v16-topology-refresh-ms=N
--vemb-v16-topology-retry-limit=N
```

语义：

- `--protocol vemb_v16` 总是启动时 fetch topology、按 active owner ring
  路由，并在运行中 refresh/retry；不存在 endpoint-hash 回退。
- `--vemb-v16-endpoints` 未指定时，`-s/-p` 是唯一 TCP bootstrap endpoint。
- 当前 TCP memtier 实现复用该列表的连接，因此指定列表时必须包含 topology
  中每个 active owner 的 TCP endpoint；动态连接广告 endpoint 属于后续 common
  transport 接入范围。
- `--cluster-mode` 是 Redis Cluster 兼容模式，不参与 VEMB 路由；它与
  `--protocol vemb_v16` 不能组合。

### Topology 获取

memtier 启动时应从 bootstrap endpoint 获取 topology，缓存以下信息：

```text
current_topology_epoch
min_write_epoch
flags
active_ring
standby_ring
owner_endpoints
```

可复用 hpc-redis 中已有逻辑：

```text
src/vemb_v16_client_topology.c
src/vemb_v16_client_topology.h
src/vemb_v16_topology.c
src/vemb_v16_topology.h
```

需要注意：`benchmark/vemb_v16_bench` 默认通过 SDK common core 发送原生
`VEMB_V16_NET_TOPOLOGY_GET` frame；集成 `redis-server` 下的 memtier
路径如果不能直接发送该 frame，需要补一个 Redis/VEMB 协议可访问的 topology
control 入口，或让 memtier 的 VEMB transport 能复用原生 topology control。

### 读路径

每次 VEMB read 前：

```text
key -> key_hash
owner = vemb_v16_topology_ring_owner(active_ring, key_hash)
endpoint = topology endpoint for owner
request.topology_epoch = current_topology_epoch
send to endpoint
```

响应处理：

```text
OK               -> count hit
NOT_FOUND        -> count miss
STALE_TOPOLOGY   -> refresh topology, retry same key
MOVED            -> refresh topology, recompute owner, retry same key
ASK              -> send one ASK retry to redirect_owner
network failure  -> reconnect endpoint, bounded retry
other ERR        -> count final error
```

### 写路径

写路径分两期落地。

第一期只支持 after 稳态写入：

```text
key -> active owner -> endpoint -> VADD
```

第二期支持 during live write：

```text
plan = vemb_v16_client_topology_plan_write(topology, key_hash)
send active write
if topology/server contract requires client-side dual write:
    send standby write
handle STALE/MOVED/ASK with refresh/retry
```

是否由客户端显式 dual-write 需要与 server 当前语义对齐，不能仅根据 flag
盲目发送第二份写。

### 并发模型

memtier 多线程下 topology cache 建议采用读多写少模型：

- 每个 worker 读取 topology 快照。
- refresh 时构建完整新 topology，再整体替换。
- 使用 RW lock 或 atomic pointer 保护 topology。
- endpoint connection pool 按 owner 维护，topology 更新后按 owner 重连。

避免 worker 读到半更新 topology。

### 统计项

新增统计项，用于区分业务 miss 与迁移重试：

```text
TopologyRefresh/sec
StaleRetry/sec
MovedRetry/sec
AskRetry/sec
ReconnectRetry/sec
FinalErrors/sec
```

`STALE/MOVED/ASK` 中间态不计入 memtier `Misses/sec`。只有最终
`NOT_FOUND` 才计入 miss，最终不可恢复错误才计入 error。

## hpc_redis_scaleout_throughput.sh 调整

memtier 修复后，脚本三段建议如下。

### baseline

保持现状：

```bash
$MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
  -s $NODE0_HOST -p $PORT \
  --ratio=0:1 --key-pattern=R:R \
  --key-prefix=item: --key-minimum=1 --key-maximum=$PREFILL_KEYS
```

### during_scaleout

后台 memtier 改为 topology-aware：

```bash
$MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
  --vemb-v16-endpoints=$NODE0_HOST:$PORT,$NODE1_HOST:$PORT \
  --vemb-v16-topology-retry-limit=8 \
  --ratio=0:1 --key-pattern=R:R \
  --key-prefix=item: --key-minimum=1 --key-maximum=$PREFILL_KEYS \
  --test-time=$BG_TIME_SCALEOUT
```

注意：

- memtier 启动时 topology 可能只有 `active={0}`。
- node1 endpoint 只作为可连接 bootstrap/未来 owner endpoint。
- 实际路由必须等 server topology 发布后由 refresh 得到。
- during 输出中 retry 统计应单独记录，不能混入 miss。

### scaleout_after

扩容完成后使用 final topology 压稳态读：

```bash
$MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
  --vemb-v16-endpoints=$NODE0_HOST:$PORT,$NODE1_HOST:$PORT \
  --ratio=0:1 --key-pattern=R:R \
  --key-prefix=item: --key-minimum=1 --key-maximum=$PREFILL_KEYS \
  --test-time=$TEST_TIME
```

如果需要避免历史迁移 key 对吞吐口径的影响，可以在 cutover 后新增一段
post-cutover steady keyspace：

```text
STEADY_KEY_MIN=PREFILL_KEYS+1
STEADY_KEY_MAX=PREFILL_KEYS+STEADY_KEYS
```

先用 topology-aware VADD 写入该 keyspace，再用同样 topology-aware read
测稳态吞吐。

## 验收标准

### after 稳态

必须满足：

```text
scaleout_after Misses/sec 接近 0
scaleout_after FinalErrors/sec 接近 0
scaleout_after Hits/sec ~= Ops/sec
node0/node1 都有请求和成功响应
```

如果 `hits/ops` 仍接近某个 owner 占比，例如 `784/1000`，说明客户端仍未按
server topology 路由。

### during 窗口

必须满足：

```text
during_scaleout 有 topology refresh/retry 统计
STALE/MOVED/ASK 不直接计入 miss/error
coordinator full active publish 成功
memtier 后台自然结束并输出 Totals
FinalErrors/sec 接近 0
```

允许 during QPS 低于 baseline/after，因为迁移、refresh、retry、UB RPC
都会引入额外开销；但不能因为可恢复 topology 状态被误判而产生大量 miss/error。

## 推荐 PR 拆分

1. `feat(memtier): route vemb v16 by server topology`
   - fetch topology
   - read/write 按 active owner endpoint 路由
   - after 稳态通过

2. `feat(memtier): retry vemb v16 topology transitions`
   - 支持 `STALE_TOPOLOGY / MOVED / ASK`
   - 增加 refresh/retry 统计
   - during_scaleout 通过

3. `feat(memtier): support scaleout write semantics`
   - during live write
   - dual-write/candidate topology 语义
   - 与 server 写路径契约对齐

## 当前注意事项

在 memtier 修复前，以下结果不能作为扩容后稳态吞吐结论：

- after 只连接 node0
- during 将 `STALE/MOVED/ASK` 计为 miss/error

这些结果只能用于暴露客户端路由问题，不能代表 VEMB V16 scaleout 后真实吞吐。

## 当前落地进度断点

更新时间：2026-07-24

### 已完成代码改动

1. 服务端 topology GET 修正：
   - 文件：`src/vemb_v16_proxy.c`
   - 新增 `topology_resp_has_endpoint_for_owner()`
   - `topology_resp_add_local_endpoint()` 在 storage/published topology 已包含
     local owner endpoint 时不再覆盖它。
   - 目的：避免集成 `redis-server` 在本地只有 UDS/Aeron enabled 时，把已经发布的
     TCP endpoint 覆盖成 UDS/Aeron endpoint，导致 TCP client topology fetch 失败。

2. memtier VEMB topology routing：
   - 文件：`memtier_benchmark/memtier_benchmark.h`
   - 文件：`memtier_benchmark/memtier_benchmark.cpp`
   - `--protocol vemb_v16` 自动启用 server topology routing；没有
     endpoint-hash 回退。

3. memtier VEMB 多 endpoint 路由第一版：
   - 文件：`memtier_benchmark/cluster_client.h`
   - 文件：`memtier_benchmark/cluster_client.cpp`
   - 新增成员：
     - `m_topology_valid`
     - `m_topology`
     - `m_owner_to_conn[]`
   - 新增流程：
     - `fetch_topology()`：从 `--vemb-v16-endpoints` 第一个 endpoint 拉取 topology。
     - `build_topology_owner_map()`：把 topology 中 owner endpoint 映射到现有连接下标。
     - `route_key_to_backend()`：按 `active_ring + xxh3(key)` 选择 owner，
       再映射到连接。
     - `apply_topology_epoch()`：把 fetch 到的 `current_topology_epoch` 写入每个
       `vemb_v16_protocol`。
   - 当前限制：第一版要求 topology 返回的每个 active owner endpoint 都已经出现在
     `--vemb-v16-endpoints` 中；暂未动态新增连接。

4. memtier VEMB request epoch：
   - 文件：`memtier_benchmark/protocol.h`
   - 文件：`memtier_benchmark/protocol.cpp`
   - 新增 `m_topology_epoch` 和 `set_topology_epoch()`
   - TCP VADD/VREM/VEMB_HANDLE/VEMB_INLINE 请求改为直接构造 `vemb_v16_req_t`
     并调用 `vemb_v16_req_encode()`，从而写入 `req.topology_epoch`。
   - Aeron request 构造也同步写入 `req.topology_epoch`。
   - VSIM template patch 路径同步设置 `topology_epoch`，并把 key hash 修正为
     `vemb_v16_xxh3_64_str()`。

5. SDK 构建修正：
   - 文件：`clients/c/vemb_v16_client_sdk.c`
   - 新增 `sve_streaming_load_f32()` 前置声明。
   - 目的：修复当前 clang/C99 下函数先使用后定义导致的 SDK 编译失败。

6. redis-server 构建修正：
   - 文件：`src/vector_engine_ub_impl.c`
   - 新增 `#include "sve_similarity.h"`，修复
     `sve_cosine_similarity_f32()` 未声明。
   - 文件：`src/sve_similarity.c`
   - `svwhilelt_b32()` 参数显式转换为 `uint64_t`，修复 clang 对
     SVE intrinsic 重载选择的歧义。

7. scaleout throughput 脚本接入 memtier topology-aware：
   - 文件：`benchmark/hpc_redis_scaleout_throughput.sh`
   - `during_scaleout` 后台 memtier 指定：
     - `--vemb-v16-endpoints=$NODE0_HOST:$PORT,$NODE1_HOST:$PORT`
   - `scaleout_after` 从 `vemb_v16_bench` 的 common-core 验证路径切回
     topology-aware memtier：
     - cutover 后先用 topology-aware memtier 预填 steady keyspace
     - 再用 topology-aware memtier 读 steady keyspace
   - 新增：
     - `STEADY_KEY_MIN`
     - `STEADY_KEY_MAX`

8. memtier topology 运行中刷新第一版：
   - 文件：`memtier_benchmark/cluster_client.h`
   - 文件：`memtier_benchmark/cluster_client.cpp`
   - 新增 `m_topology_refresh_event`
   - 新增周期刷新：
     - 默认每 500ms 调用 `refresh_topology()`
     - topology epoch 变化后重新 `apply_topology_epoch()`
     - epoch 变化后唤醒各连接 pipeline
   - 新增 topology-aware key 生成约束：
     - VEMB client 某连接只生成当前 active ring 会路由到该连接 owner 的 key。
     - 避免 active={0} 启动阶段把属于 node0 的 key 排到 node1
       连接上，导致 cross-connection queueing 后无进展。

### 已完成远端同步/验证

远端环境：

```text
node0 = 192.168.90.111
node1 = 192.168.90.112
REMOTE_DIR = /root/szz/codespace/hpc-redis
MEMTIER = /root/szz/codespace/hpc-redis/memtier_benchmark/memtier_benchmark
```

同步方式：

- 远端缺少 `rsync`。
- 用户要求使用“源码覆盖变更文件”的方式同步，而不是整体目录同步。
- 已用小 tar 包只覆盖本次变更涉及的源码文件。
- 远端 memtier 已经编译好；后续若只改 memtier 源码，需要按需只同步相关文件并在远端执行
  `make -j`。

远端 smoke 结果：

1. 初始 smoke 前发现 node0 有遗留 `redis-server` 占用 UB path：

```text
node0 redis-server PID 389058 on port 6395 held /dev/obmm_shmdev2
```

已杀掉该精确 PID 后继续测试。

2. 第一版 topology-aware memtier：
   - baseline 约 `11M ops/sec`。
   - during 阶段从开始就是 `0 ops` 并最终无 `Totals`。
   - 原因：active={0} 启动时 memtier 同时连接 node0/node1，node1 连接生成的 key
     实际应该路由到 node0，造成 cross-connection queueing，pipeline 无法推进。

3. full-active 手工验证：
   - node0/node1 topology GET 均显示 `active={0,1}`。
   - topology 中 owner endpoint 均为 TCP endpoint。
   - topology-aware memtier steady keyspace 写入 1000 key 成功。
   - topology-aware memtier 读 3s 成功：

```text
Totals 2606731.51 ops/sec
Hits/sec = 2606731.51
Misses/sec = 0
```

结论：`scaleout_after` 的“两 owner 稳态 topology-aware 路由”已验证可用。

4. 加入 topology-aware key 生成约束后：
   - during 第一秒约 `26k ops/sec`。
   - 随后变为 `0 ops`，memtier 没有自然输出 `Totals`。

5. 加入 500ms 周期 topology refresh 后：
   - during 第一秒约 `14.8k ops/sec`。
   - 随后仍变为 `0 ops`，memtier 没有自然输出 `Totals`。

   结论：周期 refresh 只能减少“拓扑变了但客户端不知道”的窗口，不能替代
   `ASK / MOVED / STALE_TOPOLOGY` 的请求级处理。during 阶段仍未完成。

9. memtier 请求级 topology retry 第一版：
   - 文件：`memtier_benchmark/protocol.h`
   - 文件：`memtier_benchmark/protocol.cpp`
   - `protocol_response` 暴露 VEMB response status 与 `redirect_owner`。
   - `vemb_v16_protocol` 支持设置下一次 request flags，用于 ASK redirect
     重发时带 `VEMB_V16_REQ_F_ASK_REDIRECT`。
   - 文件：`memtier_benchmark/shard_connection.h`
   - 文件：`memtier_benchmark/shard_connection.cpp`
   - 新增 `vemb_v16_request` 保存 key/value/offset/retry_count，支持重发同一个请求。
   - 文件：`memtier_benchmark/cluster_client.h`
   - 文件：`memtier_benchmark/cluster_client.cpp`
   - `vemb_v16_multi_client::handle_response()` 识别：
     - `STALE_TOPOLOGY`：refresh topology 后按 active ring 重路由重发。
     - `MOVED`：refresh topology 后按 active ring 重路由重发。
     - `ASK`：按 `redirect_owner` 找连接，并带 ASK flag 单次重发。
   - retry 受 `--vemb-v16-topology-retry-limit` 控制，默认 8。
   - 文件：`memtier_benchmark/memtier_benchmark.cpp`
   - 新增参数：
     - `--vemb-v16-topology-refresh-ms=N`
     - `--vemb-v16-topology-retry-limit=N`
   - 文件：`memtier_benchmark/run_stats.cpp`
   - topology-aware VEMB 输出复用 `MOVED/sec` / `ASK/sec` 列；其中
     `STALE_TOPOLOGY` 归入 refresh/MOVED 类 retry 统计，不进入 miss/error。
   - 文件：`benchmark/hpc_redis_scaleout_throughput.sh`
   - `during_scaleout` memtier 增加
     `--vemb-v16-topology-retry-limit=8`，note 改为
     `memtier-client-topology-retry`。

10. memtier during 收尾与 key_hash 修正：
   - 文件：`memtier_benchmark/protocol.cpp`
   - TCP 直构 `vemb_v16_req_t` 的 VADD/VREM/VEMB_HANDLE 路径补齐
     `req.key_hash = vemb_v16_xxh3_64_str(...)`，避免扩容迁移/重定向阶段
     server 看到错误 key_hash。
   - 文件：`memtier_benchmark/client.cpp`
   - `client::finished()` 改用 wall-clock `get_duration_usec()` 判断
     `--test-time`，避免迁移窗口 0 ops 后统计秒不再滚动导致永不结束。
   - 文件：`memtier_benchmark/cluster_client.cpp`
   - topology-aware refresh timer 在 finished 后不再重排，并断开 shard
     connections 清理 pending response，让 during 阶段能自然输出 Totals。

### 当前构建状态

按用户指定方式执行：

```bash
cd clients/c && make -j
```

结果：已通过。

```bash
cd memtier_benchmark
autoreconf -ivf
./configure
make -j
```

结果：

- `autoreconf -ivf` 已通过，仅有老 autoconf 宏 warning。
- `./configure` 已通过。
- `make -j` 已通过，`memtier_benchmark/memtier_benchmark` 已成功链接。
- `./memtier_benchmark --help` 已显示 VEMB endpoint、refresh 与 retry 参数。
- 再次执行 `make -j` 显示 `Nothing to be done for all-am`。

曾遇到并已修复的链接错误：

```text
Undefined symbols:
  vemb_v16_topology_ring_owner(...)
  vemb_v16_client_topology_fetch_tcp(...)
NOTE: found symbol in libvemb_v16_client.a, declaration possibly missing extern "C"
```

修复方式：

- 文件：`memtier_benchmark/cluster_client.h`
- 把 `#include "vemb_v16_client_sdk.h"` 用 `extern "C"` 包裹，避免 C SDK 符号
  在 C++ 编译单元中被 name mangling。

### 当前已知问题

1. `during_scaleout` 的 `ASK / MOVED / STALE_TOPOLOGY` 请求级 retry 已完成，
   并已在远端 scaleout smoke 中验证能输出 Totals。

当前 memtier 做到了：

```text
startup topology fetch
active-ring owner routing
request.topology_epoch
periodic topology refresh
parse VEMB response status
识别 STALE_TOPOLOGY / MOVED / ASK
按 redirect_owner 或刷新后的 active owner 重发同一个 request
bounded retry
把 topology 中间态从 miss/error 中剥离出来统计
```

远端 smoke（2026-07-24，run_id=`20260724_032552`）结果：

```text
scaleout_baseline ops=10736461.78 hits=10736461.78 misses=0
during_scaleout  ops=164.43      hits=164.43      misses=0 moved=0 ask=0
scaleout_after   ops=12354456.90 hits=12354456.90 misses=0 moved=0 ask=0
```

注意：during 已能自然输出 Totals，coordinator full-active publish 成功，
after 稳态吞吐正常；但 during QPS 仍很低且 p99 很高，说明迁移窗口仍有
in-flight 请求长尾/悬挂被 test-time 收尾断开，后续还需要继续追 server
迁移窗口回包或客户端超时重试语义。

2. during 卡住不只是“缺少定时刷新”。

远端 smoke 已证明：

- startup fetch 后无 refresh：during 卡住。
- 加 500ms refresh：during 仍卡住。

因此下一步必须查清两件事：

- 服务端在 migration active / min_write_epoch 更新后，对旧 epoch 的读请求是否及时返回
  `STALE_TOPOLOGY / MOVED / ASK`。
- 如果服务端已经返回这些状态，memtier 是否因为 `protocol.cpp::parse_response()`
  把非 OK/NOT_FOUND 都折叠成 generic error，导致 client 层完全拿不到 redirect
  owner / topology epoch。

3. 读路径可能缺少 stale topology 早返回。

目前已观察到写路径有 stale topology 处理，读路径需要重点检查：

```text
src/vemb_v16_supernode.c
vemb_v16_supernode_handle_vemb_job()
```

如果 read request 在 migration active 后因为旧 epoch 进入了可能阻塞的 remote
lookup / fence 路径，而不是及时返回可恢复状态，memtier 端即使实现 retry 也无法解挂。

4. `parse_bg_out()` 与 `set -e` 有诊断风险。

后台 memtier 没有输出 `Totals` 时，`result=$(parse_bg_out ...)` 可能在
`set -e` 下提前退出脚本，影响后续日志采集。主问题仍是 during 阶段 memtier
卡住，但后续可以把该解析改成“记录 0 + 保留 raw log + 继续收集诊断”。

5. 清理远端进程时避免使用会匹配自身命令行的 `pkill -f`。

曾出现 SSH 返回 255 的情况，怀疑是远端 `pkill -f` 匹配到了当前 shell/ssh
命令行。后续清理建议：

```bash
ssh -p 22 root@192.168.90.111 \
  'kill -9 <exact_pid> 2>/dev/null || true; sleep 1; ps -ef | grep redis-server | grep -v grep || true'
```

当前记录到的遗留状态：

```text
node0 可能仍有 redis-server PID 440547 on 0.0.0.0:6391
node1 当时未见 redis/memtier
```

继续远端测试前先重新确认并清理 UB path 占用。

### 待继续事项

1. 已执行：

```bash
git diff --check
```

结果：通过。

2. 注意 `autoreconf` 生成了构建备份文件：

```text
memtier_benchmark/config.guess~
memtier_benchmark/config.sub~
```

后续需要判断是否清理。它们是构建副产物，不属于本次功能代码。

3. `make -C src redis-server` 已通过。

构建过程中仍有若干既有 warning，包括 SVE fallback unused parameter、format
pedantic、macro redefined 等，但没有 error。

4. `benchmark/hpc_redis_scaleout_throughput.sh` 已通过：

```bash
bash -n benchmark/hpc_redis_scaleout_throughput.sh
```

5. `benchmark/hpc_redis_scaleout_throughput.sh` 仍有前序实验改动需要最终复核：
   - 已有 UB manifest 生成、remote dir、prereq/UB idle 检查等修改。
   - `during_scaleout` 已切到 topology-aware memtier，但当前 memtier
     只支持启动时 fetch、按 active ring 路由和周期 refresh，尚不支持
     `ASK / MOVED / STALE_TOPOLOGY` 请求级 refresh/retry。
   - `scaleout_after` 已切到 topology-aware memtier steady keyspace 口径。

### 仍未完成的功能

1. `during_scaleout` 的请求级 refresh/retry 尚未实现：
   - `STALE_TOPOLOGY`
   - `MOVED`
   - `ASK`
   - retry 统计

2. memtier 暂未新增：
   - `--vemb-v16-topology-refresh-ms`
   - `--vemb-v16-topology-retry-limit`
   - topology refresh/retry 统计项

3. 当前代码主要覆盖 `scaleout_after` 稳态拓扑路由；`during_scaleout`
   还需要在 response path 暴露 VEMB status，再做 bounded refresh/retry。

## 下一步落地 TODO

### P0: 先确认 during 卡点

1. 本地阅读协议状态定义：
   - `src/vemb_v16_protocol.h`
   - 确认 `VEMB_V16_STATUS_STALE_TOPOLOGY`
   - 确认 `VEMB_V16_STATUS_MOVED`
   - 确认 `VEMB_V16_STATUS_ASK`
   - 确认 response 中的 `redirect_owner` / `topology_epoch` 字段编码。

2. 阅读 memtier VEMB response parse：
   - `memtier_benchmark/protocol.cpp::parse_response()`
   - 目标：确认当前是否丢弃了 VEMB status、redirect owner、topology epoch。

3. 阅读 memtier request/response 生命周期：
   - `memtier_benchmark/cluster_client.cpp`
   - 参考 Redis cluster `MOVED/ASK` 处理方式。
   - 目标：找出 VEMB multi client 做同 key bounded retry 的最小改动点。

4. 阅读服务端 read path：
   - `src/vemb_v16_supernode.c`
   - 重点看 `vemb_v16_supernode_handle_vemb_job()`。
   - 目标：确认 migration active + stale request epoch 下，read 是否会及时返回
     `STALE_TOPOLOGY / MOVED / ASK`，还是可能阻塞。

### P1: memtier 请求级状态暴露

1. 在 `vemb_v16_protocol` 中保存最近一次 VEMB response metadata：

```text
last_vemb_status
last_vemb_redirect_owner
last_vemb_topology_epoch
```

2. `parse_response()` 不再把所有非 OK/NOT_FOUND VEMB status 都折叠成普通错误；
   至少要让 client 层能区分：

```text
STALE_TOPOLOGY
MOVED
ASK
ERR
```

3. 保持默认 memtier 统计兼容：只有最终不可恢复错误才计入 error。

### P2: memtier bounded retry

1. 给 `vemb_v16_multi_client` 增加 topology retry 参数：

```text
--vemb-v16-topology-refresh-ms=N
--vemb-v16-topology-retry-limit=N
```

2. 对可恢复状态执行：

```text
STALE_TOPOLOGY -> refresh topology -> recompute owner -> retry same key
MOVED          -> refresh topology -> recompute owner or use redirect_owner -> retry same key
ASK            -> one-shot send to redirect_owner -> retry same key
```

3. retry 必须有上限，超过后才计入 final error。

4. 增加统计：

```text
TopologyRefresh
StaleRetry
MovedRetry
AskRetry
FinalTopologyErrors
```

### P3: 服务端 read stale/redirect 兜底

如果 P0 发现服务端 read path 对旧 epoch during 请求没有及时返回可恢复状态，则补齐：

1. read 请求进入可能阻塞路径前检查 topology epoch。
2. migration active 且 request epoch stale 时返回 `STALE_TOPOLOGY`。
3. owner 已切换且当前节点不是 owner 时返回 `MOVED`，并带 `redirect_owner`。
4. cutover/repair 只允许临时转发时返回 `ASK`，并带 `redirect_owner`。
5. 保持 source-side migration fence：不能为了避免卡住而返回旧位置的 stale data。

### P4: 远端复测

1. 只同步本次变更文件到 node0/node1：

```bash
COPYFILE_DISABLE=1 tar -czf - <changed-files> | \
  ssh -p 22 root@192.168.90.111 'tar -xzf - -C /root/szz/codespace/hpc-redis'
COPYFILE_DISABLE=1 tar -czf - <changed-files> | \
  ssh -p 22 root@192.168.90.112 'tar -xzf - -C /root/szz/codespace/hpc-redis'
```

2. 若改了 memtier：

```bash
ssh -p 22 root@192.168.90.111 \
  'cd /root/szz/codespace/hpc-redis/memtier_benchmark && make -j'
ssh -p 22 root@192.168.90.112 \
  'cd /root/szz/codespace/hpc-redis/memtier_benchmark && make -j'
```

3. 若改了 server：

```bash
ssh -p 22 root@192.168.90.111 \
  'cd /root/szz/codespace/hpc-redis && make -C src redis-server'
ssh -p 22 root@192.168.90.112 \
  'cd /root/szz/codespace/hpc-redis && make -C src redis-server'
```

4. smoke：

```bash
TEST_TIME=3 PREFILL_KEYS=1000 STEADY_KEYS=1000 BG_TIME_SCALEOUT=20 \
MEMTIER=/root/szz/codespace/hpc-redis/memtier_benchmark/memtier_benchmark \
bash benchmark/hpc_redis_scaleout_throughput.sh
```

验收：

```text
during_scaleout memtier 自然输出 Totals
during_scaleout 有 topology retry 统计
during_scaleout FinalErrors/sec 接近 0
scaleout_after Hits/sec ~= Ops/sec
scaleout_after Misses/sec = 0 或接近 0
```

## 2026-07-24 during_scaleout root cause

本轮定位确认 `during_scaleout` QPS 很低不是 TSV 解析问题。旧 run 的 raw
日志显示第 1 秒后瞬时 ops/sec 长时间为 0，Totals 是真实完成请求数。

实际有两个问题：

1. 服务端 read path 没有在 migration active 且 request epoch stale 时早返回
   `STALE_TOPOLOGY`。旧 epoch 读请求会进入迁移 fence/lookup 路径，形成
   test-time 收尾长尾。已在 `src/vemb_v16_supernode.c` 的 VEMB read 与
   VSIM key-key read 入口补齐早返回。
2. memtier topology-aware 模式下，candidate topology 为
   `active={0}, standby={0,1}` 时，node1 连接已存在但不是 active owner。
   该连接被 wake 后会反复随机生成 key，发现都不属于 conn1 后返回
   `not_available`，而 `fill_pipeline()` 会继续重试，造成 worker 忙等并阻塞
   libevent 处理。已在 `vemb_v16_multi_client::hold_pipeline()` 中让
   standby-only connection 在不拥有 active owner 时保持空闲，等 final
   topology 刷新后再填 pipeline。

远端验证 run_id=`20260724_034306`：

```text
scaleout_baseline  ops=11788476.33 hits=11788476.33 p99=0.90300
during_scaleout   ops=11983093.30 hits=11982096.16 p99=0.68453
scaleout_after    ops=12454806.12 hits=12454806.12 p99=1.33373
```

同口径 `BG_TIME_SCALEOUT=30` 复测 run_id=`20260724_034425`：

```text
scaleout_baseline  ops=11494851.96 hits=11494851.96 p99=0.91900
during_scaleout   ops=12219758.70 hits=12219360.20 p99=0.66964
scaleout_after    ops=12345285.28 hits=12345285.28 p99=1.33402
```

## 测试执行边界（强制）

真实 UB cluster/扩容测试永不在开发机或其他本地机器上启动。测试进程、Redis
server、coordinator、topology_ctl、CLI/memtier 以及 UB device 映射都必须运行在
远端 111/112 节点；本机只允许执行源码同步、远端编译触发、脚本语法检查和结果
拉取/分析。所有远端操作统一经跳板机 `43.154.145.18`（SSH 端口 `8111` 对应
node0，`8112` 对应 node1）完成。

每次重测前必须确认远端没有残留 runner、topology_ctl、coordinator、Redis 或
memtier 进程，并且同一时刻只启动一个扩容 runner 实例。这里的“一个 runner”
是一个扩容脚本实例，不限制该实例内部启动的 CLI/memtier worker、thread 或
client 数量。
