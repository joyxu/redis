# VEMB V16 TCP/UB 统一 Cluster 与扩容传输层设计

## 状态与目标

状态：功能链路已落地；静态 UB cluster、统一 open+mmap、动态 topology
refresh/attach 已完成远端验证，TCP 与 UB-Aeron 111 -> 112 扩容均已完成单轮吞吐验证；
阶段 8 的资源生命周期、TCP/UB 同 workload 性能对比和 imported NC 映射属性证据仍待补齐。

```
启动 coordinator listen
  -> 提交 node1 candidate
  -> 提交 node0 candidate + peer-view map
  -> 等待 source local-done
  -> coordinator 发布 full-active
```

## 当前进度与问题（2026-08-23）

### 本轮统一化与回归进度（2026-08-23）

SDK/CLI 已在创建边界固定数据 transport：TCP 只创建 inline vector-read session，
UB-Aeron 只创建 handle vector-read session，运行中的 topology snapshot 不再选择或
切换 INLINE/HANDLE。Server 在 topology-set 外部边界拒绝与启动 transport 不一致的
endpoint；TCP client 若收到 AERON endpoint snapshot，只记录低概率 `WARNING` 并
保持启动时固定的 TCP transport。client、session、channel 等执行热点路径已按创建
契约移除调用方保证不为空的重复判空；创建失败使用 Redis assert 尽早暴露。确实表示
外部不确定性、资源耗尽或状态转换的低频分支仍保留，适用处使用 `RETURN_IF`。

本轮远端阶段 8 回归计划按以下顺序执行，避免端口和 UB device 冲突：

1. `scripts/run_host_mt_server_flamegraph.sh`
2. `scripts/run_aeron_cross_node_flamegraph.sh`
3. `scripts/run_aeron_best.sh`
4. `benchmark/vemb_v16_scaleout_ub_cluster_111_to_112.sh`

本轮 host-mt、Aeron best、Aeron cross-node，以及 TCP/UB data-plane 的双节点
111 -> 112 扩容吞吐流程均已完成。TCP 和 UB 扩容均完成 baseline、during_scaleout、
after old-key 和 after steady-key 四个阶段；此前受干扰的旧轮性能数据不纳入结论。

### 本轮远端同步与回归结果（2026-08-23）

本轮复用远端已编译的 server/client/SDK/memtier 二进制；各 runner 均通过
build-stamp 校验，未在压测脚本中重复编译。以下四组 workload 的公共参数为
`NUM_KEYS=100000`、`KEY_PATTERN=R:R`、`t/THREADS=64`、`c/CLIENTS=4`、
`PIPELINE=32`、`PROFILE=0`；仅 `PIO:SNW`/`WORKERS` 线程配置不同。扩容脚本的远端日志不传输到本地，
本地只保留远端结果目录指针；cross-node 本轮负载门禁通过，结果不采用此前受干扰的旧轮数据。

| 场景（脚本） | 线程参数 / 状态 | QPS (ops/s) | p50 (ms，按 avg) | p99 (ms) | server CPU (cores) | client CPU (cores) | ops/core | 正确性 | 产物 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| [TCP-local / host-mt server flamegraph](../scripts/run_host_mt_server_flamegraph.sh) | `WORKERS=16:16` | 11,428,872.93 | 0.63900 | 1.38300 | 22.55 | — | 506,823.63 | — | [perf/20260823_130514](../perf/20260823_130514/) |
| [Aeron-local / Aeron best](../scripts/run_aeron_best.sh) | `WORKERS=21:21` | 31,744,269.05 | 0.19201 | 0.32700 | 39.27 | — | 808,359 | — | [perf/aeron_sweep/20260823_104930](../perf/aeron_sweep/20260823_104930/) |
| [Aeron-local / Aeron best](../scripts/run_aeron_best.sh) | `WORKERS=7:7` | 16,633,994.02 | 0.42275 | 0.55900 | 14.17 | — | 1,173,888 | — | [perf/aeron_sweep/20260823_105921](../perf/aeron_sweep/20260823_105921/) |
| [Aeron-cross-node](../scripts/run_aeron_cross_node_flamegraph.sh) | `PIO/SNW=7:7` | 11,082,054.42 | 0.505260 | 0.895000 | 11.831 | 12.0287 | 935,962.03 | `status_ok=354,106,392; notfound=0; err=0; materialized_fail=0; unmatched=0` | [perf/p7_s7_svr0-15_uniform_20260823_113040](../perf/p7_s7_svr0-15_uniform_20260823_113040/) |

表中 `p50` 按当前汇报口径使用各产物的 avg latency 输出；TCP-local raw summary 未直接打印
`ops/core`，表中按 `QPS / server CPU cores` 由同一产物计算。host-mt 另有
`run_ops=12,016,641`、内存基线/峰值/均值 `256/354/350MB`。两类 scaleout 的原始
memtier、server 和 coordinator 日志保留在远端结果目录，本地只记录远端路径，summary 见各自扩容专表。

### TCP 扩容吞吐结果（独立 4x4 表，2026-08-23）

以下表格只描述 `scripts/hpc_redis_scaleout_throughput.sh` 的 TCP data-plane
`111 -> 112` 扩容，不与 host-mt、Aeron local 或 Aeron cross-node 场景合并。

| 阶段 | QPS (ops/s) | p50 (ms) | p99 (ms) |
| --- | ---: | ---: | ---: |
| baseline，active `{0}` | 11,619,564.03 | 0.61500 | 0.94300 |
| during scaleout，`{0} -> {0,1}` | 12,238,984.23 | 0.59900 | 0.90300 |
| after old keys，active `{0,1}` | 11,769,461.98 | 1.24700 | 1.93500 |
| after steady keys，active `{0,1}` | 11,806,971.28 | 1.21500 | 1.92700 |

配置为 `DATA_TRANSPORT=tcp`、`PREFILL_KEYS=10000`、`PIO/SNW=21`、`t64/c4/p32`；
扩容耗时约 5 秒。coordinator 返回 `scaleout_all_sources_done=1`、两个 owner
full-active publish 成功、`errors=0`。raw memtier 中 baseline/after 的 MOVED、ASK、
Misses 均为 0，during 仅出现扩容期间预期的 MOVED。结果目录为本地
[summary.tsv](../scripts/results/scaleout/20260823_tcp_scaleout_112434/summary.tsv) 及远端
`/root/gqs/codespace/UnifiedBus/hpc-redis/benchmark/results/scaleout/20260823_tcp_scaleout_112434/`；
新运行会在同一目录下生成 `remote_artifacts.txt`，记录该远端目录和 raw/server/coordinator 日志路径。

阶段 8 仍为“进行中”：UB scaleout 单轮吞吐验收已完成，但资源生命周期、连续
topology refresh 和 imported NC 映射证据仍未补齐。

### UB-Aeron 扩容吞吐结果（独立 4x4 表，2026-08-23）

以下表格只描述 [`benchmark/vemb_v16_scaleout_ub_cluster_111_to_112.sh`](../benchmark/vemb_v16_scaleout_ub_cluster_111_to_112.sh)
在 `DATA_TRANSPORT=ub`（Aeron/UB data-plane）下的双节点 `111 -> 112` 扩容，
不与 TCP scaleout、host-mt 或 Aeron local 场景合并。

| 阶段 | QPS (ops/s) | p50 (ms) | p99 (ms) |
| --- | ---: | ---: | ---: |
| baseline，active `{0}` | 17,288,977.99 | 0.41500 | 0.54300 |
| during scaleout，`{0} -> {0,1}` | 14,067,071.33 | 0.41500 | 0.65500 |
| after old keys，active `{0,1}` | 12,629,626.92 | 0.42300 | 0.66300 |
| after steady keys，active `{0,1}` | 10,898,312.26 | 0.46300 | 1.20700 |

配置为 `DATA_TRANSPORT=ub`、`DIM=300`、`PREFILL_KEYS=10000`、`VNODE_COUNT=100`、
`PIO/SNW=7:7`、`t64/c4/p32`、`TEST_TIME=30s`、`BG_TIME_SCALEOUT=60s`；during
阶段 endpoint 为 `192.168.90.111:6397,192.168.90.112:6397`，扩容控制窗口约 5 秒。
四个阶段均通过严格 correctness 检查，`status_nf=0`、`status_err=0`、
`materialized_fail=0`、`unmatched=0`。coordinator 返回
`scaleout_all_sources_done=1`、`scaleout_full_active_published=2 errors=0 targets=2`。

结果产物：[本地 summary.tsv](../benchmark/results/scaleout/ub_scaleout_status_nf_fix_20260823/summary.tsv)；
完整的 `scaleout_baseline.out`、`scaleout_during.out`、`scaleout_after_old_keys.out`、
`scaleout_after.out`、server 和 coordinator 日志保留在远端
`/root/szz/codespace/hpc-redis/benchmark/results/scaleout/ub_scaleout_status_nf_fix_20260823/`。
本地 `remote_artifacts.txt`（新运行生成）只记录两台节点的 SSH 入口和上述远端文件路径，
脚本不再执行日志压缩、`scp` 或递归 `tar` 传输。

### UB 扩容期间问题定位、修复与 TCP 对照（2026-08-23）

修复前的 `ub_scaleout_20260823_131217` during 日志定位出两个独立的迁移窗口问题；
该轮只作为问题定位样本，不作为性能结论。方案 2 已修复第一个
`MOVED` 路由问题；第二个 handle lookup 可见性问题也已按 key metadata 状态分类修复。
`ub_scaleout_status_nf_fix_20260823` 的回归中两个计数均为 0。旧样本仍保留用于说明根因，
不作为性能结论。

1. **`status_err=37092`：MOVED 后公共 cluster core 丢失 redirect owner，最终
   retry exhaustion。**

   - UB v2 batch 先因旧 topology epoch 收到 `STALE_TOPOLOGY`；source owner 对已
     cutover 的 key 随后返回 `MOVED -> owner1`。
   - `vemb_v16_cluster_core_on_response()` 在 `MOVED` 分支只执行
     `mark_topology_stale()`，没有把 `response.redirect_owner` 保存到当前操作。
     topology refresh 后日志仍为 `active_count=1 endpoint_count=2`，所以同一 key
     又按旧 active ring 路由回 owner0，重复 8 次后返回
     `VEMB_V16_CLUSTER_PREPARE_RETRY_EXHAUSTED`，SDK 将逻辑操作记为
     `VEMB_V16_STATUS_ERR`。
   - 证据：[`clients/c/vemb_v16_cluster_core.c:144`](../clients/c/vemb_v16_cluster_core.c:144)、
     [`clients/c/vemb_v16_client_sdk.c:1904`](../clients/c/vemb_v16_client_sdk.c:1904)、
     [`benchmark/vemb_v16_scaleout_ub_cluster_111_to_112.sh:447`](../benchmark/vemb_v16_scaleout_ub_cluster_111_to_112.sh:447)。
     during 原始日志中可见 `MOVED ... from_owner=0 to_owner=1` 与
     `prepared=2 ... attempts=8 ... active_count=1 endpoint_count=2`。

2. **`status_nf=3`：迁移期间 handle lookup 可见性窗口。**

   服务端在迁移过程中对 3 个 key 打印 `vemb_v16 handle miss`。此时 topology/迁移
   状态已经变化，但 handle 或 source-cutover metadata 对该请求尚未可见；旧实现
   额外依赖全局 `migration_active_count`，而 `SOURCE_GC` 会先将该计数降为 0，
   随后把 lookup miss 错误返回为 `NOT_FOUND`。公共 core 将 `NOT_FOUND` 当作最终状态，
   不再重试，因此该计数是迁移读可见性缺陷，不应归类为普通 not-found 业务结果。
   相关实现位于 [`src/vemb_v16_supernode.c:64`](../src/vemb_v16_supernode.c:64)
   和 [`clients/c/vemb_v16_cluster_core.c:120`](../clients/c/vemb_v16_cluster_core.c:120)。

#### `status_nf` 修复规则与验收

服务端 lookup miss 现在只在低频 miss 分支读取该 key 的 migration metadata，统一按
状态分类，不再以 `migration_active_count` 作为 source-cutover 判断前置条件：

| key metadata 状态 | lookup miss 响应 | 目的 |
| --- | --- | --- |
| `CUTOVER`、`SOURCE_GC` | `MOVED`，`redirect_owner=target_owner` | 旧 owner 明确把请求交给新 owner |
| `MIGRATING`、`DEST_PREPARED`、`DEST_COMMITTED` | `STALE_TOPOLOGY` | 让公共 cluster core refresh topology 后重试 |
| `SOURCE_ACTIVE` 或无 metadata | `NOT_FOUND` | 保留真正的业务 not-found 终态 |

该分类已覆盖 VEMB、VSIM key1/key2、VSIM inline 和 VREM 的 lookup miss 路径。新增
`benchmark/vemb_v16_migration_control_ut.c` 回归用例，至少验证普通 miss、迁移中、
`CUTOVER` 和 `SOURCE_GC + migration_active_count=0` 四种情况；验收要求为：普通
不存在 key 仍为 `NOT_FOUND`，迁移中不得产生 `status_nf`，source `CUTOVER/SOURCE_GC`
必须返回带有效 owner 的 `MOVED`。

#### 为什么 TCP transport 没有复现问题 1

TCP 未复现只能说明当前 TCP 测试没有触发该组合路径，不能证明公共 MOVED retry
语义已经正确。两种 runner 的入口和数据路径仍不完全等价：

- TCP client 实际固定走 `VEMB_INLINE` vector-read；TCP proxy 会拒绝
  `VEMB_HANDLE`，不会进入 UB v2 handle batch 的整帧 stale rejection、quiesce/drain
  和 v1 fallback 重投链路。
- UB workload 稳态优先提交 Aeron/UB v2 handle batch，扩容时才叠加
  `STALE_TOPOLOGY`、v1 fallback 和 topology refresh，问题 1 正发生在这条路径。
- TCP runner 启动时直接把 node0、node1 两个 endpoint 作为 seed 传给 client；UB
  runner 只有 node0 bootstrap seed，owner1 endpoint 需要 topology refresh 后通过
  peer-view attach 动态建立。TCP 没有复现“snapshot 仍只有 owner0 active、但服务端
  已明确返回 owner1”的场景。
- TCP 不经过 UB peer-view/Aeron attach、v2 descriptor/arena、imported warm region
  和 UB RPC；因此不能用 TCP-local 的通过结果覆盖 UB v2 动态 owner 建立的缺陷。

#### 两个修复方向与推荐

**方向 A：服务端把该迁移响应改成 `ASK`。** SDK 已有一次性 ASK redirect，可直接
使用 `redirect_owner`，改动集中；但这会把已经发生的 ownership cutover 错误表达成
临时转发。后续普通请求仍可能因 topology 未发布 owner1 active 而回到 owner0，且
不能替代 topology 发布时机和 active-owner 一致性修复。因此不建议作为主修复。

**方向 B：公共 cluster core 保留 `MOVED` 语义并使用 `MOVED.redirect_owner`（推荐）。**
收到 MOVED 时保存当前操作的 redirect owner，先按需 refresh topology 获取最新 epoch
和 endpoint，再优先向该 owner 重试；确认 redirect owner 存在于最新 endpoint 且 channel
attach 成功后提交请求。若 redirect owner 无效或 topology 与其不一致，才进入受 retry
budget 约束的 refresh/error 路径。这样既保持 ownership 语义，又修复 TCP、UB 和未来
transport 共享的公共路由状态机；同时仍应修正服务端 topology 发布时机，使 active owner
最终与 MOVED target 一致，避免长期依赖 forced redirect。

该方案不能简单地用旧 epoch 直接重发：当前响应结构没有独立 redirect epoch，必须在
重试时处理 topology epoch 与 owner endpoint/channel 的一致性。

### 已完成

- 已生成并统一使用 `examples/vemb_v16_ub_cluster_111_to_112.env`、两台
  node manifest、peer-view 及扩容 peer map。固定资源分配为 node0 本地
  `dev1..dev4`、node1 本地 `dev9..dev12`，对端 imported view 为
  `dev5..dev8`、`dev13..dev16`；同机 CLI/node0 使用 local CC，跨机 UB ring
  和 remote warm read 使用 NC 读写端约定。
- 配置语义已收敛为“上层只填写 UB path”。manifest/config 中已删除全部
  `cache_policy` 字段，不能再通过配置值推断 cache 模式。
- 下层映射统一走同一套 open+mmap helper：先用 `O_RDWR` 打开并 mmap；只有
  open 或 mmap 返回 `EPERM/EACCES` 时，关闭后用 `O_RDWR|O_SYNC` 重试。该逻辑
  已覆盖 mapped region、client SDK 和 client peer-view 的实际 open 调用点。
- SDK/CLI 在 `vemb_v16_client_create()` 时固定 TCP 或 AERON transport；TCP 的
  vector read 固定使用 `VEMB_INLINE`，AERON 固定使用 `VEMB_HANDLE`。topology
  snapshot 只负责 owner/endpoint 路由。Server 在 topology-set 配置边界拒绝与
  启动 transport 不同的 endpoint；SDK 若极低概率收到错配 snapshot 只记录
  `WARNING`，不会改变启动时固定的 transport/read op。
- 方案 2 已修复公共 cluster core 的 `MOVED.redirect_owner` 路由：收到 MOVED
  后保存目标 owner，refresh 后优先建立目标 owner channel 并使用最新 topology
  epoch 重试；目标不可用时受 retry budget 约束地结束，不回退旧 owner，也不形成
  无限 refresh。新增 `moved_target_unavailable` 统计。`vemb_v16_cluster_core_ut`、
  `vemb_v16_tcp_data_transport_ut`、`vemb_v16_ub_data_transport_ut` 和
  `vemb_v16_peer_view_transport_ut` 均通过；修复后的 UB 扩容 during/after 日志中
  `status_nf/status_err/materialized_fail/unmatched` 全部为 0。
- 服务端 lookup miss 已改为按 key metadata 状态分类：`CUTOVER/SOURCE_GC` 返回
  `MOVED`，迁移中间态返回 `STALE_TOPOLOGY`，只有普通不存在 key 返回 `NOT_FOUND`。
  新增的 migration-control lookup-miss UT 已通过，并覆盖 `SOURCE_GC` 时全局迁移计数
  已降为 0 的原始复现条件；双节点 UB 扩容验收继续要求 during/after 的
  `status_nf=0`。
- peer-view 单测、本地构建以及通过跳板机管理的两台 ARM 节点构建均已通过。
  Mac 管理入口为 `43.154.145.18:8111`（node0）和
  `43.154.145.18:8112`（node1）；节点间数据面仍使用
  `192.168.90.111/112`。

### 历史短测与当前状态

脚本：`benchmark/vemb_v16_scaleout_ub_cluster_111_to_112.sh`
结果目录：`benchmark/results/scaleout/ub_scaleout_20260820_155951/`
参数：`PREFILL_KEYS=10000`、`VNODE_COUNT=100`、`PIO=7`、`SNW=7`、
`t=64`、`c=4`、`pipeline=32`。

owner0/owner1 的 warm region、remote meta、UB RPC ring 和 client peer-view
均已完成初始化。baseline topology 输出中的 `active_owners=0` 表示 active owner
列表包含 owner ID `0`，不是 active owner 数量为零；同样的
`standby_owners=0` 也是 owner ID 列表。响应 `status=0`、`flags=0x1`、
`vnode_count=100` 且 endpoint 为 owner0，说明控制面已正确发布单 owner topology。
但该轮 prefill 的 10000 个 SET 全部以 `status_err` 结束，原因是运行时二进制与
attach 配置不一致；统一重建和修正配置后 prefill 与 baseline 已通过。该历史产物
不能作为 cluster 模式或扩容方案验收结果。

### 当前剩余验证缺口

1. 当前 root 硬件环境下，imported `dev5..dev8/dev13..dev16` 直接以普通
   `O_RDWR` 打开也成功，尚未触发 `O_SYNC` fallback。因此代码路径已具备
   fallback，但 imported path 是否实际获得 NC mmap 属性仍未被实机证明；需要
   结合驱动权限/映射属性日志确认，或由设备层提供可观测的 NC 验证。
2. 阶段 8 仍需补齐 attach/close/re-attach 资源泄漏、连续 topology refresh、
   retry/peer-view/ring/warm-read 失败分类，以及 TCP/UB 同 workload 可比性能报告。

### 扩容长尾定位（2026-08-20）

后续用新配置和同一轮构建复测后，prefill 与 baseline 已成功，说明此前的
UB ATTACH/mmap 配置问题不再是当前扩容 blocker。扩容阶段的长尾来自控制面和
迁移工作叠加：

- 原 UB runner 先向 node1 提交带 coordinator endpoint 的候选 topology，之后
  才启动 coordinator；source notification 是一次性的，导致 coordinator
  永远停在 `scaleout_all_sources_done=0`。runner 已调整为先 listen，再提交
  node1 candidate 和 node0 peer-view map。
- 日志对齐后，不能把 node1 的约 99 秒间隔称为 topology control 延迟：
  `16:44:13` 的 `0x13` 是 `TOPOLOGY_GET`（启动 readiness probe），
  `16:45:51` 的 `0x12` 才是 node1 的 `TOPOLOGY_SET`，两者之间包含 prefill
  和 baseline。node1 candidate 文件与 `0x12` 到达时间相同，未显示 handler
  自身耗时 99 秒。
- 真正的扩容长尾在 node0 peer-view map 链路：coordinator 在约
  `16:48:52` 因 180 秒窗口退出；node0 的 `0x1f` peer-view topology set
  直到 `16:48:54` 才到达 server，随后约 0.7 秒内就完成 `scaleout auto local
  done`，但 notify 已得到 `ECONNREFUSED`。因此长尾发生在 node0 控制请求到达
  前的 runner/SSH/调度等待，不是 node0 topology handler 执行 180 秒。
  扩容控制窗口已提高到 600 秒，node1 candidate 也使用该窗口。
- TCP runner 的默认 vnode 数是 10，且 server/client 使用 TCP 数据面，不经过
  Aeron ATTACH、peer-view ring、imported warm read 和 UB RPC；TCP 完成较快不能
  证明 UB 扩容路径等价。`DATA_TRANSPORT=ub` 实际是 `exec` 另一个 runner，
  不是在原 TCP runner 内替换一个参数。
- TCP runner 还容忍 memtier 前台命令失败并不检查所有状态错误；UB runner 会
  严格检查 `status_err/status_nf/materialized_fail/unmatched`。两者的“通过”口径
  也不完全相同。

扩容 runner 的控制请求阶段与 TCP 三阶段保持一致：先启动 node0 并完成
`initial topology -> prefill -> baseline read`，再启动 node1；during 阶段启动
后台 workload，启动 TCP coordinator，提交 node1 candidate 和 node0 peer-view
candidate，等待 source-local-done 后由 coordinator 发布 `{0,1}`，最后执行
SDK/CLI topology refresh 和 after 两段读测。UB runner 中 node1 candidate 在
后台发起，node0 candidate 随即提交；这是因为 UB 的 node1 handler 可能在本地
migration/notify 完成前保持 TCP 控制连接，而 TCP 版本通常立即返回。两者仍然
是独立的 TCP 控制请求，coordinator 的 source notification 和 full-active 发布
顺序没有改变。

因此当前长尾首先是 UB 数据面负载放大了迁移控制 handler 的响应时间，叠加
runner 的 coordinator 顺序和超时缺陷；不能归因于 `active_owners=0` 或控制面
发布了空 topology。

针对该等待关系，runner 现在让 node1 candidate 以后台 TCP 控制请求发出后，
立即提交 node0 的 peer-view topology 请求，再分别检查 node1 请求状态和
coordinator 的 `{0,1}` full-active 发布。UB 本地迁移等待不会再阻塞 node0
控制请求到达；控制面仍然完全使用 TCP。

### 完整实机复测阶段总结（2026-08-20 19:22）

使用新脚本和统一参数再次执行：

```text
PREFILL_KEYS=10000 VNODE_COUNT=100 PIO=7 SNW=7
MEMTIER_T=64 MEMTIER_C=4 PIPELINE=32
```

脚本只修改了 runner 和诊断日志；远端 `make` 检查显示目标已是最新，未发生
server/client 的实际重新编译。管理连接使用跳板机
`43.154.145.18:8111/8112`，节点数据和控制 endpoint 仍使用
`192.168.90.111/112`。

阶段结果如下：

| 阶段 | 结果 |
| --- | --- |
| node0 启动、initial topology、prefill | 成功；prefill correctness 检查通过 |
| baseline read | 成功，`17221427.42 ops/s`，p99 `0.551ms` |
| node1 standby 启动 | 成功；UB warm/meta/RPC 映射完成 |
| during workload | 成功完成，`15751192.60 ops/s`，p99 约 `0.599ms`，无最终错误 |
| coordinator listen | 成功，TCP 控制面监听；收到 `source_owner=0` 的 local-done |
| node1 candidate topology | 成功，`topology_ctl --set` 返回 `status=0` |
| node0 candidate + peer-view map | 成功，peer-view map 和 topology set 均返回 `status=0` |
| coordinator full-active publish | 成功报告 `scaleout_all_sources_done=1`、`scaleout_full_active_published=2 errors=0` |
| after old-key read | 失败，约 `197949 ops/s`，p99 `116.735ms`；大量 `MOVED`，最终 `status_err` 约占一半 |

因此本轮已证明：候选 topology 提交、TCP coordinator、node0 本地迁移完成通知
和 full-active 发布链路均能走通；失败边界在 cutover 后客户端处理 `MOVED`、刷新
topology 并通过 peer-view 建立 owner1 UB channel 的数据面阶段。`MOVED` 中间态
被反复产生，最终请求没有稳定落到 owner1，导致 old-key correctness 检查失败。
这不是 node1 candidate SSH 或 `topology_ctl --set` 长时间阻塞问题。

本轮现场产物保存在：

```text
benchmark/results/scaleout/ub_scaleout_20260820_192259/
```

其中 `node0/scaleout_after_old_keys.out`、`node0/coordinator.out`、
`node0/server_node0.log` 和 `node1/topology_candidate_node1.out` 可用于继续定位
client topology refresh、owner1 endpoint 映射及 UB ATTACH 的失败原因。

本节是当前事实记录；下方分阶段 checklist 的历史记录不得覆盖本节中的阻塞
状态。只有 prefill 成功、两 owner 路由有效且 NC/CC 访问语义得到实机确认后，
阶段 7 才能重新标记为完成。

### Full-active 后 migration_active 仍为真的阶段结论（2026-08-20 23:07）

最新统一参数复测产物：
`benchmark/results/scaleout/ub_diag_verify_20260820_230530/`。

本轮控制面并未失败：node0/node1 candidate topology 均返回 `status=0`，coordinator
记录了 `scaleout_all_sources_done=1`，并成功向两个 owner 发布了
`scaleout_full_active_published=2 errors=0`。但 node0 数据面日志显示：

```text
vemb_v16 scaleout auto local done ... phase=notify_pending
vemb_v16 scaleout notify failed ... endpoint=192.168.90.111:7397
```

之后没有出现 `local done notified` 或 `scaleout auto done`。这说明 coordinator
收到 local-done 后直接发布了 full-active，而 source 侧的 notify ACK 没有完成；
full-active 是控制面拓扑发布，不会自动把 storage 的 scaleout phase 推进到
`NOTIFIED -> SOURCE_GC -> DONE`。因此 storage 仍处于迁移保护状态，proxy 继续
拒绝 v2 batch 是当前设计下的预期保护行为，不能通过删除拒绝条件规避。

另一个独立的生命周期缺陷在
`src/vemb_v16_storage.c:vemb_v16_storage_migration_mark_migrating_in_shard()`：
`migration_active_count` 在首次成功进入慢路径时递增，但成功完成迁移、CUTOVER、
SOURCE_GC 或 scaleout DONE 路径没有对应递减；当前递减只覆盖标记失败的回滚。
所以即使后续 phase 被推进到 DONE，计数也可能永久保持非零。

当前结论：

1. `full-active` 已成功不等于 storage migration 已结束；本轮首先卡在
   `notify_pending`/notify ACK 链路。
2. `migration_active_count` 的成功完成清理缺失，是会让迁移态永久残留的代码问题。
3. v2 batch 的 `migration_active` 拒绝不能删除；修复顺序应为：先修复 notify
   请求/响应确认和 phase 推进，再定义 SOURCE_GC 完成后的 active 计数清理，同时
   保留 CUTOVER/SOURCE_GC 的 source fence。
4. 后续验收必须同时记录 coordinator 输出、server 的 scaleout phase、
   `migration_active` 状态和 v2 rejection 计数，不能只以 full-active publish
   成功作为迁移完成判据。

### 修复结论与本地验证（2026-08-20 23:57）

已修复 source local-done ACK 与 active 生命周期两处问题。local-done response 的
实际 wire 组成是 `status(1) + migration_epoch(8) + cutover_epoch(8) +
topology_epoch(8) + source_owner(4)`，共 `29` 字节；旧常量错误地声明为 `33`，使
source 虽收到 coordinator response，仍因 header length 校验失败而不能确认 ACK，
phase 停在 `NOTIFY_PENDING`。常量改为 `29` 后，ACK 可以继续驱动
`NOTIFY_PENDING -> NOTIFIED -> GLOBAL_CUTOVER_WAIT -> SOURCE_GC -> DONE`。

`migration_active_count` 现在只在一次成功的 `CUTOVER -> SOURCE_GC` 转换中配对
递减；重复的 `SOURCE_GC` 维持幂等，不会二次递减或把计数重新加回去。计数清零只
解除 v2 batch/ATTACH 的全局迁移暂停，**不会放开旧 source 访问**：key metadata
仍保留为 `CUTOVER`/`SOURCE_GC`，读路径仍在 raw warm/cache lookup 前后检查该
source fence，因此不会返回旧 source warm/cache 地址。迁移期的 v2 拒绝与这两道
key-level fence 均保留，不能用清零计数替代。

本地已通过 `topology_ctl coordinator smoke`，并增加 local-done wire 长度与
coordinated scaleout 完成后 inactive、重复 `SOURCE_GC` 幂等的断言。完整
`vemb_v16_migration_control_ut` 仍受既有 fixture 问题阻塞：顺序执行时 warm region
仅四个 slot，后续写入耗尽容量；单独提前执行 coordinated case 时 UB RPC peer ring
尚未初始化。该 UT 不能宣称全绿，最终验收以远端两节点扩容实测为准，并归档
coordinator output、source phase、`migration_active`/v2 rejection 证据。

### 修复后远端两节点验收（2026-08-21 02:26）

代码已同步至 `43.154.145.18:8111`（node0）和 `:8112`（node1），两端 server、
client build stamp 均校验通过。大负载运行
`benchmark/results/scaleout/ub_scaleout_fix_20260821_0020/` 的控制面结果为
`scaleout_all_sources_done=1`、`scaleout_full_active_published=2 errors=0 targets=2`，
source 日志依次出现 `local done`、`local done notified`、`scaleout auto done`；
迁移期间 `migration_active=1` 的 v2 batch 拒绝被观察到，完成后出现
`migration_active=0`，且 old-key 读无 `status_nf/status_err`、无 materialization
或 unmatched 错误（约 `9.38M ops/s`）。

随后用低并发短跑补齐完整四阶段，产物为
`benchmark/results/scaleout/ub_scaleout_fix_smoke_20260821_0245/`：
baseline `86.8K ops/s`、during-scaleout `66.6K ops/s`、old-key `59.9K ops/s`、
steady-key `61.3K ops/s`，四阶段均通过严格的 post-scaleout correctness 检查。
迁移期间 runner 允许并记录 `status_err/status_nf` 瞬态，仍严格要求
`materialized_fail=0`、`unmatched=0`；old-key 与 steady-key 阶段继续要求
`status_nf=0`、`status_err=0`。该差异正是为了观测并保留迁移保护拒绝，而不是
把拒绝路径误报成数据面失败。

### full-active 后 old-key/steady-key batch 差异的修复结论（2026-08-21）

差异来自客户端调度节奏，而不是 owner hash、epoch 或 remote-meta publish：steady-key
读路径的 p50/p99 响应时间高于 old-key，pipeline 固定为 32 时，每轮 poll 回收的
slot 更少；如果随即 eager flush，owner-local L0 只能看到少量新路由请求。old-key
响应更快，回收并补入的请求更多，所以同一个 flush 窗口自然形成更大的 batch。两阶段
的 owner 比例接近，且 V1 fallback 和重复 key 合并均为零，不能解释该差异。

修复包括两点：

1. common-core handle session 不再在每轮 `poll()` 前调用立即 flush。`poll()` 统一负责
   route、full/deadline flush；否则会绕过 `max_batch_delay_us`，使 bounded coalescing
   配置失效。
2. 扩容 workload 当前统一使用脚本默认的 `BATCH_MAX_DELAY_US=10us`，不采用
   baseline/迁移/full-active 后的分阶段配置。`200us` 仅保留为人工诊断或对比实验时
   可显式传入的参数，不是生产或验收默认值。

修复后 10K 两节点复测（`64x4`、pipeline `32`）的 owner 加权平均 batch 为：
old-key `16.8/15.1`、steady-key `17.0/14.9`（owner0/owner1），new-key 已与 old-key
处于同一量级；QPS 为 baseline `16.21M`、during `11.54M`、old-key `10.10M`、
steady-key `6.98M`，p99 分别为 `0.623/0.751/0.751/1.111ms`，correctness 全部通过。
上述 QPS 与 batch 数值来自当时用于定位聚合问题的 `200us` 诊断实验；当前脚本默认已
调整为全阶段 `BATCH_MAX_DELAY_US=10us`，两者不可混作同一配置的基线。
临时 batch 统计已在复测后删除，不属于运行时指标。需要注意，5 秒短测不能与 10 秒
阶段直接比较；启动和预热比例会明显影响 QPS。

## 概览

本文统一的是一个逻辑操作，而不是一个 TCP 连接或 UB ring。调用方提供 TCP
引导 seed，并在 SDK/CLI 启动时固定 TCP 或 UB-Aeron 数据面；服务端拓扑决定
每个 `owner_id` 的 endpoint，且所有 endpoint 必须匹配该启动模式。TCP 与 UB-Aeron
共用拓扑、路由、epoch、重试、迁移 fence 和最终统计，区别仅留在数据面传输的
channel/resource I/O。

### SDK/CLI 架构

```text
CLI / 压测工具 / 应用
        |
        v
VEMB 客户端 SDK 公共 API
  create(seeds, dim, timeout, transport) # seed 是 TCP 控制地址
  带 key 的操作 / pipeline / session
        |
        +-- TCP 控制面：TOPOLOGY_GET、ATTACH、CLOSE、STATUS、迁移控制
        |
        v
公共集群核心（cluster core）
  拓扑快照 | key -> owner 路由 | 提交 epoch | operation_id
  ASK / MOVED / STALE 重试 | owner channel generation | 最终逻辑统计
        |
        v
按 owner 索引的数据面传输接口
  open | submit | poll | resource 检查/fence | close
        |                                      |
        |                                      +-- completion(operation_id, owner, epoch)
        |
        +-- TCP 数据面后端
        |     TCP request/response frame、fd/decoder/私有 pending map
        |     通过 VEMB_INLINE 交付 vector；没有 VEMB_HANDLE 能力
        |
        +-- UB-Aeron 数据面后端
              TCP ATTACH 返回 descriptor，随后映射 UB request/response ring
              VEMB_HANDLE warm view；统一由 client-side peer-view resolver 解析资源
```

client transport 是启动期部署身份，而不是重试决策。拓扑 refresh 可以更新 route
和 epoch，或在 resource-generation fence 后重新 attach 同一种 transport。Server
在 topology-set 外部配置边界拒绝另一种 transport endpoint；SDK 中的低概率错配
仅记录 `WARNING`，不参与 backend 选择或降级。

### 逻辑操作流程

`batch-agg` 与已完成 vector 的 CLI cache 由 SDK client session 持有，而不属于
cluster core 或数据面传输。它们消费 core 给出的 route identity，再将 completion
交回 core，因此这是控制/数据回环而不是单向线性分层。

```text
SDK vector-read / handle session
        |
        | 准备逻辑操作
        v
公共集群核心（cluster core）
  route = { owner_id, topology_epoch, owner_channel_generation, request_flags }
        |
        v
SDK session 执行层（启动时固定 read op）
  已完成 vector cache（可选 immutable snapshot）
  L0 batch-agg：key + owner + epoch + generation
        |
        +-- TCP client：VEMB_INLINE leader -> inline vector
        |
        +-- UB client ：VEMB_HANDLE leader -> 由 channel 持有的 warm materialization
                         owner session 稳定时可选 v2 batch
        |
        v
数据面 completion / 已 materialize 的 vector / L0 fan-out
        |
        +--> cluster core：final completion、ASK retry 或 topology refresh/re-route
        +--> session cache 填充与调用方 callback
```

cache 是 SDK session 私有状态，默认关闭，并在 topology epoch/owner-generation
变化或 session teardown 时清空。它不是跨 client 的线性一致 cache。

### Aeron 部署模式

`--vemb-v16-transport=aeron` 是所有 UB-Aeron 部署的统一 CLI transport。它不
选择不同的 topology、retry engine 或 wire protocol：

```text
topology 选定的 AERON owner
        |
        +-- client-side peer-view：ATTACH provider path
                                  -> manifest(client_host, owner_id, role, generation)
                                  -> client 可见路径的 open + mmap
```

peer-view manifest 与 client-host identity 对所有 Aeron owner 都是必填要求。同机
manifest 使用相同的 provider/client path；跨节点 manifest 使用远端 provider 到
本机设备的映射。两种部署都用 TCP 承载 bootstrap/control，并通过同一 SDK
common core 路由全部 data-plane 工作。

本文定义一套统一的 VEMB V16 cluster 与扩容系统。TCP 和基于 UB 的
Aeron 只负责请求/响应数据面，不得演变为两套 topology、路由、迁移、重试或
压测语义。所有这些上层行为由同一个 cluster core 实现。

首个部署目标有明确约束：

```text
初始状态：
  CLI 与 server owner 111 部署在 111 机器

扩容完成后：
  CLI 与 server owner 111 仍部署在 111 机器
  server owner 112 部署在 112 机器
  CLI@111 通过本机 direct UB view 访问 owner 111
  CLI@111 通过 remote UB peer-view 访问 owner 112
```

整个系统只有一个运行在 111 的 CLI，不在 112 启动第二个 CLI agent。扩容从
owner 111 的单 owner cluster 开始，将 owner 112 作为远端 owner 加入；CLI
始终留在 111。

### Remote Access Boundary

Mac 到测试节点的管理连接与节点间的数据面地址必须分离：

```text
Mac -> owner 111 SSH: 43.154.145.18:8111
Mac -> owner 112 SSH: 43.154.145.18:8112

111 <-> 112 control/data endpoint: 192.168.90.111 / 192.168.90.112
```

Mac 不得直接以 `192.168.90.111/112` 发起 SSH、同步或回归命令。两个节点在
彼此之间，以及 topology、server bind、memtier endpoint 和 UB peer-view
配置中继续使用 `192.168.90.0/24` 地址。`hpc_redis_scaleout_throughput.sh`
以 `NODE0_HOST`/`NODE1_HOST` 表示节点间地址，以
`NODE0_SSH_HOST`/`NODE0_SSH_PORT`、`NODE1_SSH_HOST`/`NODE1_SSH_PORT`
表示 Mac 管理连接；外部执行时应分别设置为 `43.154.145.18:8111` 与
`43.154.145.18:8112`。

第一阶段使用当前 111/112 的固定 UB region/path 配置。后续即使支持动态
创建、映射、释放、删除 peer-view，也只能扩展 UB 资源 resolver 的生命周期，
不得改变 cluster core 的语义。

### 固定 111/112 UB 资源分配与缓存语义

当前实机设备采用成对的 local export/import 映射，且所有映射支持
load/store 的 acquire/release 语义：

```text
111/112 local export:  dev1, dev2, dev3, dev4  -> peer import: dev5, dev6, dev7, dev8
111/112 local export:  dev9, dev10, dev11, dev12 -> peer import: dev13, dev14, dev15, dev16
```

固定两 owner 分配如下：

| 连接或资源 | owner 111 / CLI@111 | owner 112 / CLI 视图 | 语义 |
|---|---|---|---|
| CLI@111 <-> owner0 同机 request/response | `dev1/dev2` | 同一路径 | shared-memory local CC；request/response 共用本地 path |
| owner0 -> owner1 RPC request | `dev4` | `dev8` | writer NC，import view NC |
| owner1 -> owner0 RPC response | `dev13` | `dev9` | writer NC，import view NC |
| CLI@111 -> owner1 request | `dev14` | `dev10` | writer NC，import view NC |
| owner1 -> CLI@111 response | `dev15` | `dev11` | writer NC，import view NC |
| owner0 warm region | local `dev3` | remote read `dev7` | owner0 写 local CC；跨机 reader NC |
| owner1 warm region | local `dev12` | CLI/owner0 remote read `dev16` | owner1 写 local CC；跨机 reader NC |

这里的 `devN` 是实际设备编号，例如 `dev4` 表示
`/dev/obmm_shmdev4`。在每台机器上访问 `dev5..dev8` 与 `dev13..dev16`
都必须使用 NC mmap；这与 UB 的 acquire/release load/store 顺序语义不同。
配置文件不再携带 `cache_policy` 字段，warm region 与 remote meta 可以共用各自 owner 的
warm backing，但必须使用不同 mmap offset（warm 为 `0`，meta 默认从
`268435456` 开始）。同机 CLI 的 warm read 使用 local CC；跨机 warm import
保持 NC read，不能套用 request/response ring 的 cache policy。

关联文档：

- `docs/VEMB_V16_SCALEOUT_THROUGHPUT_MEMTIER_TOPOLOGY_PLAN.md`：
  topology-aware benchmark、扩容阶段口径和 redirect retry 语义。
- `docs/VEMB_V16_PEER_VIEW_MAPPING_REFRESH_DESIGN.md`：现有 owner 侧
  peer-view mapping 控制模型。
- `docs/VEMB_V16_AERON_CROSS_NODE_PROGRESS_20260728.md`：当前 TCP ATTACH
  加 UB ring 的实现现状及 111/112 path 约束。

## 非目标

本文不做以下事情：

- 不重定义 VEMB operation、响应状态、迁移 fence 或 warm-region 正确性规则。
- 不在 Aeron channel ATTACH 完成后通过 TCP 传输请求或响应 payload。
- 不假设 server 返回的 UB path 在另一台机器上天然可见。
- 不移除 Redis 通用 Unix socket 支持，或与 VEMB V16 Aeron 无关的历史
  TLC/UDS benchmark。本文的 UDS 移除范围仅限 VEMB V16 Aeron 控制面。

## 整体边界

系统由公共控制面和可替换的数据面组成：

```text
                         TCP 控制面
        topology get/refresh、ATTACH、CLOSE、迁移控制
                               |
                               v
CLI / benchmark -> VEMB cluster core -> data transport -> VEMB server
                     |                       |
                     |                       +-- TCP request/response frame
                     |                       |
                     |                       +-- UB request/response ring
                     |
                     +-- owner 路由、topology epoch、pending operation、
                         retry、迁移 drain、通用统计
```

无论数据面选择何种 transport，控制面都使用 TCP：

- topology bootstrap 与 refresh；
- channel ATTACH 与 CLOSE；
- owner health、generation 与状态查询；
- migration prepare、cutover 与 source-GC 控制。

Aeron 的 TCP ATTACH 只用于分配 channel、返回 descriptor。ATTACH 完成后，
UB-Aeron 的请求与响应只经映射后的 UB ring 传递。

## 公共 Cluster Core

cluster core 必须在 TCP 和 UB-Aeron 两种数据面间完全复用，负责：

- 经 TCP bootstrap、获取和 refresh topology；
- 以完整 snapshot 原子发布 topology，worker 不得看到半更新 owner map；
- 将 `key_hash` 路由到 active owner，并写入 `request.topology_epoch`；
- 按 owner 与 channel generation 缓存 channel；
- 在 reconnect 或 channel reattach 时保留逻辑 pending-operation 状态；
- 用相同策略处理 `OK`、`NOT_FOUND`、`ASK`、`MOVED`、
  `STALE_TOPOLOGY` 和最终错误；
- 进行 bounded retry、topology refresh、drain 和通用统计；
- 遵守迁移 `CUTOVER` 与 `SOURCE_GC` fence。

现有 TCP SDK 中的 topology/retry engine 是公共 core 的行为基线。实现时应将
它从 TCP backend 调用中抽出，而不是复制出一套 Aeron-only runner。benchmark
必须调用该 core，不能自行保留 topology 路由、retry 或 owner channel。

### Topology Snapshot

snapshot 中的 owner endpoint 保留公共身份和控制地址：

```text
owner_id
topology_epoch 与 owner generation
control host:port                  # 始终为 TCP
data transport identity            # 与 Server/SDK 启动模式一致
```

topology 负责将 key 映射到 `owner_id`，不携带某个 client host 私有的 UB
设备 path。ATTACH 返回的资源路径应由本机所选 data transport 的 resource
resolver 解释。client transport 是启动期部署不变量，同一 client 的所有 owner
使用同一种 TCP 或 UB-Aeron transport；topology epoch 只更新路由和请求 epoch，
不参与 backend 选择。

### Pending Operation 与 Retry

pending operation 只保存逻辑状态：

```text
operation identity、request payload、key hash、target owner、
topology epoch、retry count、final completion state
```

其中不能保存 TCP fd、Aeron ring pointer 或其他 transport 私有地址。transport
完成响应匹配后，按逻辑 operation identity 向公共 core 报告 completion。

- `ASK`：向 `redirect_owner` 使用 `VEMB_V16_REQ_F_ASK_REDIRECT` 做一次
  redirect retry。
- `MOVED` 与 `STALE_TOPOLOGY`：refresh topology，重新计算 owner，并在
  配置的 retry budget 内重试。
- 上述中间状态不得计入 benchmark miss 或 final error。

写请求必须有无歧义的执行结果。请求已经提交但响应丢失时，不能盲目改发到
另一个 owner。最终实现需要保留稳定的逻辑 operation ID，并且只能依赖已证明
的“redirect/fence 前未执行”协议契约，或由 server 提供 duplicate suppression。

## Data Transport 接口

cluster core 使用统一的请求和响应类型。以下为设计级 API 名称，不约束最终
header 的具体形式：

```c
open_owner_channel(owner_endpoint, channel_config, out_channel)
submit(channel, request)
poll(channel, out_response)
open_warm_region(channel, out_view)
close_channel(channel)
```

请求 shape、必填参数、范围和配置只在 CLI/configuration 或 decoded control
frame 的外部边界校验一次。transport 内部在 descriptor 和资源所有权已经被
调用方保证后直接执行。仍需保留真实运行时不确定性检查，包括 ATTACH 拒绝、
设备 open/mmap 失败、ring 耗尽、publish 失败、channel generation 变化和
wire-frame 损坏。

client、session 和 channel 创建成功后即是后续路径的必需 live object，执行路径
不再兼容 `NULL`，destroy/close 也不接受 `NULL`。SDK heap allocation 失败统一进入
Redis assertion，而不是沿热点路径返回可恢复错误。每次调用才产生的必需 key、
vector 和 output/capacity 在最外层公共边界断言；`set_name`、明确标为 optional 的
output 和 callback 仍可为 `NULL`。session closing、I/O/wire failure 后 transport
已自关闭、并发状态转换等真实但低频的状态使用 `RETURN_IF` 或显式状态机分支。

### TCP Data Transport

TCP transport 在 owner TCP data connection 上编码/解码 VEMB request/response
frame。它是兼容和通用远程网络数据面。cluster core 提供 owner 选择、
topology epoch 与 retry 决策；TCP backend 只管理连接和 frame I/O。

### UB-Aeron Data Transport

UB-Aeron transport 先经 TCP ATTACH 获取 channel descriptor，再绑定其中的
request ring、response ring 以及可选 warm region：

```text
TCP ATTACH
  -> channel_id、ring offset、slot size、resource identity/generation、
     server resource path、warm-region descriptor

UB 数据面
  CLI -> request ring -> server
  CLI <- response ring <- server
  CLI 读取 ATTACH 广告的 VEMB_HANDLE warm region
```

`submit()` 向 request ring publish，`poll()` 从 response ring consume。两者
使用与 TCP 相同的 VEMB request/response 语义；区别只在映射前的 UB resource
resolution。

## UB Resource Resolver

UB-Aeron transport 在 ATTACH descriptor 和 client 可见设备映射之间维护 resolver：

```text
resolve(client_host, owner_id, resource_role, resource_id, generation)
  -> local_path, map_flags, mapping_lease
```

`resource_role` 为 request ring、response ring 或 warm region。为支持后续动态
资源管理，resource identity 与 generation 必须是稳定语义；server path 可用于
诊断和固定配置查表，但不是可跨机器传播的资源身份。

### Direct Local Resolver

对 `CLI@111 -> server@111`，ATTACH 返回的 path 在 111 可直接访问。resolver
返回该 direct path，UB transport 在本机 `open + mmap`。

### Static Remote Peer-View Resolver

对 `CLI@111 -> server@112`，111 必须使用“111 视角的 112 UB 资源”。resolver
读取显式固定部署 manifest，覆盖当前 111/112 配置：

```text
client host = 111
provider owner = 112
resource role = request ring | response ring | warm region
provider resource identity/path -> client-visible UB path
offset = ATTACH 返回的 channel 或 warm-region offset
```

manifest 只规定 UB driver 所需的 mapping flag 和设备路径；它是当前机器拓扑的
唯一权威来源。SDK 不得再内嵌 `obmm_shmdev` 设备号变换，如 `1..4 -> 5..8`。
该对应关系属于部署配置而非协议规则。

所有 UB open+mmap 经过统一 helper：先用普通 `O_RDWR` 打开并 mmap；如果 open
或 mmap 返回 `EPERM/EACCES`，关闭当前 fd，改用 `O_RDWR|O_SYNC` 重新打开并 mmap。
因此上层只配置路径，不重复表达 CC/NC。remote warm region 不属于 UB-RPC ring，
其跨机 imported path 会按同一 fallback 规则得到 NC view；同机路径保持普通 CC。

当前固定 111/112 paths 可以继续作为 resolver 的输入。channel 仍在固定 UB
device 内动态分配：manifest 负责选择 111 可访问的 device path，ATTACH 负责
返回本 channel 的 offset。

### Future Dynamic Peer-View Resolver

未来动态资源只扩展 resolver 生命周期：

```text
prepare_peer_view(owner, resource, generation) -> lease
resolve_peer_view(lease)                        -> mmap 参数
release_peer_view(lease)
delete_peer_view(lease)                         -> 最后一个引用释放后执行
```

cluster core 不感知动态化过程。新的 resource generation 只能在 map 已有效后
发布；旧 generation 只能在所有引用它的 channel 都 close 且 drain 后释放。

## v1 Channel 与 v2 Batch Request 的统一迁移状态机

v1 channel 和 v2 batch request 都属于 UB-Aeron data transport 的内部提交
方式，不得各自实现 topology、迁移或 retry。它们通过同一个公共 cluster core
处理逻辑 operation；差别只在 owner session 内如何提交和收取 completion。

当前 v1 并非一般意义上的“`item_count=1` v2”：v1 能携带完整 VEMB request
的 op、payload、flags 和 epoch；当前 v2 frame 只编码 `VEMB_HANDLE` read 的
key 列表和 batch-level epoch，不携带通用 op、payload 或
`VEMB_V16_REQ_F_ASK_REDIRECT`。因此第一阶段保留 v1，v2 只作为稳定 topology
下的 `VEMB_HANDLE` 读批处理优化。

每个 worker 对每个 owner 维护一个 owner session：

```text
owner session(owner_id, owner_generation)
  v1 channel                 # 永久正确性路径，完整 VEMB request
  v2 batch channel            # 可选优化，仅 VEMB_HANDLE read
  L0 groups / v2 batch map    # 每组记录 owner、topology epoch、generation
  channel state               # V2_READY / V2_QUIESCING / V2_DRAINING /
                              # V1_ONLY / V2_REOPENING
```

owner 111 的 v1/v2 resource 由 direct-local resolver 绑定；owner 112 的
v1/v2 resource 由 peer-view resolver 绑定。v2 的 request descriptor、request
arena、response descriptor、response arena 也必须纳入 resolver；v2 完成的
`VEMB_HANDLE` 读取仍需要同 owner 的 warm-region view。

### 公共路由与分组规则

1. logical operation 先从完整 topology snapshot 计算 `owner_id`、
   `topology_epoch` 和 owner generation，后续才进入 L0 coalescing 或 v2 分组。
2. 一个 v2 frame 只能包含同一 owner、同一 epoch、同一 owner generation、同一
   operation 语义的 item；不得跨 owner 或跨 epoch 混合。
3. L0 group 的身份必须包含 owner、epoch 与 generation。相同 key 但不同
   owner/epoch 的请求不得合并为同一个 group。
4. v1 request builder 必须接收公共 core 给出的 epoch；不能因为是 v1 fallback
   而使用默认或过期 epoch。
5. v2 batch 的 submit epoch 是每个已发布 batch 的属性，不能永久绑定到 ATTACH
   时返回的 epoch。response 应与该 batch 的 submit epoch 比较，而不是与某个
   不会变化的 channel attach epoch 比较。

### 稳态行为

- 可批处理的 `VEMB_HANDLE` read 进入 owner session 的 L0/v2 队列。
- 非 batch operation、batch frame 容量不够、L0 资源不足、v2 ring/arena 压力
  或 v2 未 ready 的请求，经公共 core 提交到同一 owner 的 v1 channel。
- “v1 fallback”只说明选用了 v1 提交实现，不表示跳过 topology、epoch、retry、
  warm-region 或统计逻辑。
- v1 和 v2 response 都转为按 logical operation 报告的 completion。最终 success、
  miss、error 和 latency 每个 logical operation 只能累计一次，不能按 v2 frame
  与 v1 retry 重复累计。

### 扩容与迁移行为

当前 server 在 `migration_active` 时拒绝新的 v2 ATTACH，并对进入的 v2 batch
返回 stale-topology。因此首版扩容必须把 v2 作为可暂停的优化，而把 v1 作为
唯一迁移期正确性路径：

```text
V2_READY
  -> 收到扩容 prepare：停止创建新的 v2 frame
  -> V2_QUIESCING：未发布 L0 group 交回公共 core，经 v1 重投
  -> V2_DRAINING：继续 poll 已发布 v2 batch，不再 publish 新 batch
  -> migration active / cutover：V1_ONLY
  -> topology E+1 稳定、remote resource ready：V2_REOPENING
  -> 新 v2 channel 或新 submit epoch 验证成功：V2_READY
```

迁移期间的 v1 行为：

- owner 112 在 topology refresh 之前不建立 CLI channel。candidate topology
  使 owner 1 endpoint 可见后，CLI 才通过 TCP ATTACH 按需建立 peer-view v1
  channel；在 channel ready 前，路由层保留 pending/retry，不得把 attach 失败
  当作 topology 已稳定。
- `OK` 与 `NOT_FOUND` 为最终 completion。
- `ASK` 由公共 core 向 redirect owner 的 v1 channel 单次重发，并携带
  `VEMB_V16_REQ_F_ASK_REDIRECT`。
- `MOVED` 与 `STALE_TOPOLOGY` 触发 topology refresh，重新选 owner 后由 v1
  提交；source fence 仍在 server 侧阻止 stale read。
- 请求已提交而执行结果未知时不得盲目重发写请求；只可依据明确 redirect/fence
  契约或 server duplicate suppression 重试。

已发布 v2 batch 的处理规则：

- batch 在 migration fence 前完成时，按各 item 的最终 response 完成。
- server 在 batch dispatch 前以 stale-topology 拒绝整帧时，所有 item 均未执行；
  公共 core 可将它们按最新 topology 重新入队。
- mixed response 时逐 item 分类；`OK`/`NOT_FOUND` 完成，`MOVED`/
  `STALE_TOPOLOGY` 经 core refresh/requeue，`ASK` 在首版经 v1 单项 redirect
  retry，因为当前 v2 frame 不编码 ASK flag。
- 当前“v2 stale 后永久 disable v2，并直接向原 session v1 发请求”的行为不
  符合 cluster 要求，必须替换为上述 core 驱动的状态转换和重新路由。

迁移结束后，等待 topology `E+1` 稳定、旧 epoch operation 已处理，并完成
owner 112 的 v2 四个 ring/arena resource 与 warm-region peer-view 校验。之后
重建或重新验证 active owner 的 v2 session，再恢复批处理。v1 始终保留给非
batch operation 与运行时 fallback。

### 后续：是否可以删除 v1

若只支持 `VEMB_HANDLE` read，未来可以评估把 `item_count=1` 的 v2 作为唯一
数据路径；但这不是首版扩容目标。需要先完成：

- v2 ATTACH 返回并映射 warm-region descriptor；
- v2 frame 支持通用 op、payload、request flag，或明确将 scope 限制为只读；
- v2 支持动态 submit epoch，且 batch-level/逐 item redirect 语义完整；
- v2 能在迁移期安全 attach、drain、retry 和 reattach；
- 对 `item_count=1` 与实际 batch size 的延迟、吞吐、UB resource 占用完成对比。

在这些条件完成前，删除 v1 会移除迁移正确性 fallback，并且很可能使单请求的
metadata、descriptor、arena 与 batch-context 开销高于直接 v1。

## VEMB V16 Aeron UDS 移除

VEMB V16 Aeron 不保留“同机就是 UDS 控制面”的特殊分支。direct local UB 与
remote peer-view UB 都统一使用 TCP ATTACH 和 TCP close/status control。

实现范围：

- 移除 VEMB V16 Aeron SDK 的 UDS fallback、UDS alloc、UDS close 与
  UDS close-all 路径；
- 移除 `--aeron-control=uds`、VEMB V16 Aeron UDS listener 以及相应
  server/proxy 配置分支；
- 保留 UB ring poll 与 response publish，这些属于数据面而非 UDS 逻辑；
- 将 VEMB V16 Aeron 文档、测试和启动脚本统一改为 TCP control endpoint；
- 不修改 Redis 的通用 Unix socket，亦不修改独立 TLC benchmark 的 UDS 逻辑。

该变更只是控制面简化，不改变 TCP frame 与 UB ring 两种数据面的选择。

## 固定 111 到 112 的扩容流程

### Baseline

1. 在 111 部署 CLI 与 server owner 111。
2. 通过 TCP ATTACH 建立 direct local UB-Aeron channels。
3. 发布 topology epoch `E`，owner 111 为唯一 active owner，所有 key 路由到它。
4. 通过公共 cluster core、UB-Aeron transport 预填并运行 baseline 流量。
   topology 稳定时可使用 v2 batch；v1 保留为非 batch operation 和 batch fallback。

### 准备 Owner 112 与动态建立数据通道

1. 在 112 部署并启动 server owner 112；其 VEMB dimension、协议版本、ring
   配置与 warm-region manifest 必须与 111 兼容。新节点此时只作为 standby，不能
   要求 CLI 提前连接它。
2. 在 CLI@111 启动前安装完整的 peer-view manifest。它只描述
   `client_host=111, owner_id=1` 的 provider/client path、resource role 和
   generation，作为后续 resolver；manifest 本身不把 owner 1 加入 topology，
   也不触发 ATTACH。
3. 扩容前的初始 topology 只发布 owner 0 的 endpoint，CLI 的 bootstrap seed
   也只使用 node0 的 TCP control endpoint。CLI 与 node1 的地址不能通过
   `--endpoints` 或其他启动参数预注册。
4. candidate topology 发布后，owner 0 对旧 epoch 请求返回
   `STALE_TOPOLOGY`（或 `MOVED`）。CLI/SDK 通过 TCP control endpoint 拉取新的
   topology snapshot；snapshot 首次出现 owner 1 的 `host:port` 及
   `transport:aeron` 后，才允许将该 owner 加入数据路由；该 transport 字段用于
   校验它与 Server/CLI 的固定 AERON 模式一致，不用于动态选择 backend。
5. CLI 根据 owner 1 endpoint 和本机 `client_host=111` 动态解析已安装的
   peer-view，使用 snapshot 中的 endpoint 通过 TCP ATTACH 获取 descriptor，
   再 mmap request/response/warm resources，建立 owner 1 的 UB v1 channel。
   因此“发现 owner”与“建立 channel”发生在 topology refresh 之后，而不是
   standby 启动时。
6. 校验 descriptor dimension、ring slot size、resource generation、mapping
   access mode、v1 request publish、response poll 与 warm-handle read。迁移期间
   首先保证 v1 channel；v2 的四项 ring/arena resource 只能在 migration active
   前或 topology 稳定后预检/重建，不能依赖一个提前存在的 remote v2 session。

### 迁移与 Cutover

1. 按既有迁移协议准备 target data 与 metadata。
2. 进入 migration 前将 v2 session 置为 `V2_QUIESCING`；未发布 group 改走
   v1，已发布 v2 batch 进入 drain。
3. 发布 candidate topology，执行 source-side migration fence，并进入 `V1_ONLY`。
4. cluster core 构建完整 topology epoch `E + 1` 并原子发布。
5. 路由到 owner 111 的请求继续使用 direct local v1 UB；路由到 owner 112 的
   请求使用第 5 步动态 ATTACH 后的 remote peer-view v1 UB channel。
6. `ASK`、`MOVED`、`STALE_TOPOLOGY` 走公共 retry；其中 `MOVED`/`STALE_TOPOLOGY`
   必须先走 TCP topology refresh，再重新解析 owner 和 channel。source fence 继续在 raw
   lookup 前后阻止 stale warm/cache read。
7. drain old-epoch operation，确认 source 不再可访问后执行 `SOURCE_GC`。
8. topology 稳定后，将 session 置为 `V2_REOPENING`，验证 active owner 的 v2
   resource 后恢复 `V2_READY`。

### 回滚与 Owner 移除

回滚或移除 owner 112 时，应先发布排除该 owner 或按迁移协议回迁的 topology，
再 drain pending operation。之后关闭 owner 112 的 channel 并 unmap 资源，最后
才允许释放动态 peer-view lease。当前固定 111/112 mapping 可以继续存在，但已
关闭 channel 不得再使用它。

## Benchmark 与可观测性

benchmark 的 `--endpoints` 只指定 TCP bootstrap seed；数据面由启动参数
`--vemb-v16-transport=tcp|aeron` 固定。不得因 transport 参数选择不同的 topology
或 migration 实现。

scaleout runner 应替换当前 TCP-only workload path，使用仓库内 VEMB-aware
client，并记录 `baseline`、`during_scaleout`、`after` 三阶段。每项结果必须
记录测试 commit 与 worktree 状态。

通用统计：

- success operation、final `NOT_FOUND`、final error、latency；
- topology refresh、`ASK` retry、`MOVED` retry、stale-topology retry、
  retry exhaustion、old-epoch drain time；
- migration prepare、cutover、source-GC duration。

UB-Aeron 专属统计：

- direct 与 peer-view channel attach/close/re-attach；
- peer-view resolution 与 mapping failure；
- request publish failure、response poll mismatch、ring capacity pressure、
  resource generation mismatch、warm-handle read failure。

中间 redirect status 不得记为 miss 或 final error。

## 分阶段实施 Checklist

本节是实现进度的唯一 checklist。每个阶段完成后：

1. 勾选本阶段所有 checkbox；
2. 在本阶段末尾填写“断点续作记录”；
3. 提交可独立构建和验证的代码；
4. 未满足完成判据时保持 checkbox 未勾选，并在记录中写明 blocker。

禁止在未通过本阶段验收时进入依赖它的下一阶段。除明确允许并行的准备工作外，
以下阶段按编号顺序执行。

### 阶段 0：冻结当前基线与契约

目的：建立后续重构的可比基线，明确哪些行为必须保持不变。

- [ ] 记录 TCP cluster 的 topology、`ASK`、`MOVED`、`STALE_TOPOLOGY`、
  retry 和统计口径基线。
- [ ] 记录现有 Aeron direct-local 与 111/112 cross-node ATTACH、ring、
  warm-region 的最小成功配置。
- [ ] 确认所有 target server 的 dim、协议版本、ring slot 规格和
  warm-region manifest 契约。
- [ ] 列出并冻结 VEMB V16 Aeron UDS 调用点、配置项、listener、测试与文档。
- [ ] 为后续回归保留最小 TCP 和 UB smoke command 及结果位置。

完成判据：可以用同一组 key、操作比例和 topology 得到可复现的 TCP 与 UB
baseline；任何后续行为变化都能与本阶段记录比较。

断点续作记录：

```text
状态：未开始 | 进行中 | 完成 | 阻塞
代码提交：
工作区状态：
TCP 基线结果：
UB 基线结果：
已确认契约与已知风险：
下一步：
```

### 阶段 1：抽取公共 Cluster Core，保持 TCP 行为不变

目的：将 topology、路由和 retry 从 TCP backend 中抽出，先以 TCP 作为唯一
数据面验证重构等价性。

- [x] 定义公共 topology snapshot、owner channel cache 和 pending-operation
  数据结构，确保不保存 transport 私有指针或 fd。
- [x] 将 key 路由、epoch、`ASK`、`MOVED`、`STALE_TOPOLOGY`、bounded retry
  和统计移入公共 core。
- [x] 保持既有 TCP codec、连接管理和 wire protocol 不变。
- [x] 覆盖单 owner、双 owner、forced redirect、network reconnect 和 retry
  exhaustion 的 TCP 回归。
- [ ] 对比阶段 0，确认 TCP 的成功数、最终错误分类和 topology 行为未退化。

完成判据：TCP cluster 在全部回归中仍使用相同 topology 语义，公共 core 不依赖
TCP backend 内部状态。

断点续作记录：

```text
状态：进行中
代码提交：未提交
工作区状态：新增 `clients/c/vemb_v16_cluster_core.c/.h`；SDK 的单请求和
pipeline 都通过 core 取得 topology、owner、epoch、ASK flag、refresh/retry
决策与逻辑完成计数；TCP backend 仍持有 fd、warm mapping 和 frame I/O
已迁移的公共逻辑：完整 topology snapshot 发布、owner channel 逻辑 generation、
pending operation、ASK/MOVED/STALE、retry budget、redirect 与逻辑完成统计
TCP 回归与基线对比：本地 `vemb_v16_cluster_core_ut`、client topology UT、L1
materialization UT 通过。2026-08-17 在 111 的 loopback 三节点 TCP 回归中，
发布 `epoch=21, active={0,1}` 后，SDK `create(seeds, seed_count, ...)` 完成 32 个 VADD 和
VEMB pipeline；随后停止两个 active owner，确认同一 client 的一次 VADD 失败，
重启 owner 并重新发布同 epoch topology 后下一次 VADD 成功，验证 channel
重开。forced ASK/MOVED/STALE 与 retry exhaustion 由 core UT 注入验证。阶段 0
尚未记录可比的 pre-refactor TCP 性能结果，因此性能/最终错误基线对比仍未勾选
剩余 TCP 私有耦合：`sdk_data_transport_ops_t` 仍定义在 SDK 文件内，core 到
transport 的正式 interface 与 completion identity 契约留给阶段 2
下一步：补录阶段 0 的可比 TCP 基线并完成对比；阶段 1 完成判据满足后才进入阶段 2
```

### 阶段 2：定义 Data Transport Interface 并接入 TCP Backend

目的：固定 core 与 transport 的边界，防止 Aeron cluster 复制 TCP cluster 逻辑。

#### 阶段 2 内部契约

本接口只存在于 client SDK 内部，不新增 public SDK API，也不改变 VEMB wire
format。cluster core 管理 stable logical operation identity、topology snapshot、
route、retry 和 final statistics；transport 只拥有 channel 生命周期、资源和 native
data-plane I/O。

```c
open_owner_channel(channel, endpoint, dim, timeout)
submit(channel, operation_id, wire_req_id, owner, submit_epoch, request)
poll(channel, timeout_ms, out_completion)
open_warm_region(channel, out_view)
read_warm_vector(channel, region_id, offset, bytes, out)
close_channel(channel)
```

`channel` 对 core 是 opaque。TCP backend 的 fd、frame decoder、pending map 和
warm mapping，或未来 UB backend 的 mapped ring/arena/warm view，都不得进入
core、route 或 pending-operation state。

channel 的复用条件是 client 固定的 transport 与 endpoint identity，不是 topology
epoch。发布新 snapshot 后，若同一 owner 的 control host 与 port 未变，SDK 保留
原 channel 和 generation；新 submit 使用新 epoch。client transport 不允许
由 topology refresh 改写：收到与启动模式不一致的 transport identity 是
topology/configuration 异常，SDK 记录 `WARNING`，但 backend 仍由 create 参数固定。
仅在 endpoint 变化、owner 被移除或 transport failure 时关闭并以该 transport
reattach。

每个 logical operation 有跨 retry 不变的 64-bit `operation_id`。`req_id` 只是一次
wire submit identity，重路由、ASK retry 或 channel reattach 后可改变。TCP backend
在 submit 时私有记录：

```text
(channel_id, wire_req_id) -> operation_id, owner_id,
                              channel_generation, submit_epoch, expected_op
```

TCP response 在产生 completion 前必须同时验证 frame `channel_id`、frame `req_id`
与 payload `response.req_id` 一致，并能命中该 channel 的 pending entry，且
`response.op` 与该 entry 的 expected op 一致。unknown/duplicate/mismatch response
是 wire integrity failure，channel 必须关闭；已 submit 的 VADD/VREM 进入
execution-unknown final error，不能盲目切换另一 backend 或重放。只有 server 已明确
给出的 ASK/MOVED/STALE_TOPOLOGY 契约仍由 core 决定重试。

`poll` 是统一的 completion pull interface：返回 `COMPLETION`、`EMPTY` 或
`FAILED`。`timeout_ms=0` 表示 nonblocking，`WAIT_FOREVER` 等待一个 completion。
TCP backend 先等待 fd readability 再 decode 一帧；UB backend 检查 response ring，
空 ring 时返回 `EMPTY` 或在 caller 给定 deadline 内自旋/退避。TCP readable event
不是对 core 的 callback。后续如需批量优化，可加入 `drain(channel, callback,
max_count)`，但它只能批量交付同一种 completion，不能引入独立的 retry 或 identity
语义。

`open_warm_region` 返回 UB/AERON channel-owned view，`read_warm_vector` 以 response 的
`region_id` 在该 channel 内解析 mapping，`close_channel` 一并释放。调用方不得直接
保存或解引用 backend mapping。TCP 不实现这两个 capability hook，读操作只能使用
`VEMB_INLINE`，不能建立、提交或解引用 `VEMB_HANDLE`。新的跨调用 handle session 只在
AERON client 使用永久 v1 + 可选 v2。L0/batch-agg 与 cli-cache 本身不绑定 UB，后续仍可用于
TCP 的非-handle 操作。logical request 保留业务字段
（op、key bytes + len、key hash、payload）；`channel_id`、wire `req_id`、topology
epoch 和 ASK flag 属于每次 submit 的 transient 字段。

- [x] 定义 `open_owner_channel`、`submit`、`poll`、`open_warm_region`、
  `read_warm_vector`、`close_channel` 的内部 transport 契约。
- [x] 定义 transport completion 到逻辑 operation identity 的匹配规则。
- [x] 让 TCP backend 仅实现 channel 生命周期和 frame I/O。
- [x] 保留当前请求、响应、response status、topology epoch 的 wire 语义。
- [x] core 的 pending item、retry 和统计不读取 TCP fd 或 backend 私有字段。
- [x] 以 transport-focused TCP 回归验证反序 completion 的 logical operation
  identity 匹配和 TCP wire 语义。
- [x] 删除 TCP `VEMB_HANDLE` 的 SDK warm-map、同步/pipeline submit、server TCP
  client 和 TCP frame serializer；TCP client 在公共 API 边界即拒绝 handle。

完整 TCP cluster 回归和 TCP/UB 可比性能对照不属于本阶段的接口落地范围；按当前
项目安排暂缓，统一在阶段 8 的端到端验收中执行。它们不是开始阶段 4 的前置条件。

完成判据：通过 interface 的 TCP backend 与阶段 1 的行为一致，增加其他 transport
不需要修改 topology 或 migration 代码。

断点续作记录：

```text
状态：完成
代码提交：未提交
工作区状态：新增 `clients/c/vemb_v16_data_transport.h`；SDK 的 TCP channel
实现 `open_owner_channel`、`submit`、`poll`、`open_warm_region`、
`read_warm_vector`、`close_channel`。core 分配跨 retry 稳定的 `operation_id`，TCP 私有 pending
表以 `(channel_id, wire_req_id)` 映射它，响应不再按 pipeline FIFO 匹配。
接口与所有权契约：core 只管理 topology、route、retry、logical completion 与
statistics；TCP fd、frame decoder、pending table 和 warm mapping 均在 transport
私有 state。`poll` 向 core 统一返回 completion pull result，不暴露 TCP callback。
TCP 回归结果：`make -C clients/c static`、`vemb_v16_cluster_core_ut`、
`vemb_v16_client_topology_ut` 通过；新增
`vemb_v16_tcp_data_transport_ut` 以 loopback server 反序发送两个
`VEMB_INLINE` response，验证输出仍按原 pipeline 输入对应。完整 TCP cluster
回归与 TCP/UB 可比性能基线按当前项目安排暂缓到阶段 8，不作为本阶段阻塞项。
SDK create API 已收敛为唯一的
`vemb_v16_client_create(seeds, seed_count, dim, timeout_ms, transport_type)`：旧四参数
`create(host, port, ...)`、`create_multi`、静态 seed consistent-hash route helper
与 seed-as-data-channel fallback 均已删除。所有 keyed operation 必先从 TCP
bootstrap seed 拉取 topology，再使用 topology 广告的 owner endpoint 和启动时固定的
TCP/UB transport 建立数据 channel；没有 topology 不会发送数据请求。fetch 从轮转
起点逐个尝试全部 seed，任一合法 `TOPOLOGY_GET` response 即发布 snapshot。TCP
transport UT 覆盖第一个 seed 关闭 topology 请求、第二个 seed 返回合法 topology 并
完成 VADD 的 failover。
UB backend：新增 SDK 私有的 `sdk_ub_data_transport_ops`。topology endpoint
标记为 `VEMB_V16_TRANSPORT_AERON` 时，owner channel 通过 TCP ATTACH 建立
v1 UB request/response ring；`submit` 编码并 publish request，`poll` 主动轮询
response ring，以 `(channel, req_id)` 命中私有 pending entry 后返回 stable
`operation_id`。unknown `req_id`、response op mismatch 或 ring payload decode
失败均关闭该 channel。direct-local backend 的 ring/mapping/pending state
不进入 core；TCP seed/topology control 保持 TCP。
UB focused regression：新增 `vemb_v16_ub_data_transport_ut`，fake control
endpoint 经单 seed topology refresh 广告 `AERON` owner 并提供 file-backed ring，覆盖 TCP ATTACH、
UB publish/poll、反序 VSIM completion 按 operation identity 回填，以及未知
`req_id` 的 fail-close。`make -C clients/c static`、cluster core、TCP transport
和 UB transport focused UT 全部通过。
已完成的生命周期语义：owner endpoint 未变时，跨 topology epoch 保留同一
channel/generation；新 request 仍携带新 epoch。client transport 是固定部署身份，
topology 不会触发 TCP/UB 切换。剩余工作是同一 UB backend 内 resource generation
确实变化时的 drain、reattach fence 与
execution-unknown write 收敛；不能因未确认的 VADD/VREM 执行结果重放。
SDK create 参数将 transport 固定为该 client 生命周期内的部署身份；后续 topology
若极低概率包含另一种 transport endpoint，SDK 记录 `WARNING`，但不执行跨
TCP/UB close/reopen，也不改变固定的 read op；正常配置在 Server topology-set
边界已经被拒绝。
下一步：以该 backend 完成阶段 4 的 direct-local cluster-mode、warm-handle 和
close/reattach；随后将阶段 5 static peer-view resolver 接入同一 backend。
完整 TCP 回归和可比性能验收保留至阶段 8；endpoint-change reattach/drain 不得
通过重放 execution-unknown 的 VADD/VREM 实现。
```

### 阶段 3：移除 VEMB V16 Aeron UDS 控制面

目的：让 direct-local 与 remote peer-view Aeron 都使用同一 TCP 控制面。

- [x] SDK 仅接受 TCP control endpoint；删除 VEMB V16 Aeron 的 UDS connect、
  alloc、close 和 close-all 分支。
- [x] server/proxy 删除 VEMB V16 Aeron UDS listener 与
  `--aeron-control=uds` 配置分支。
- [x] 保持 TCP ATTACH、CLOSE/status、descriptor 校验和 channel 资源回收。
- [x] 更新相关 CLI help、启动脚本、文档和单元/集成测试。
- [x] 确认没有删除 Redis 通用 Unix socket 或无关 TLC benchmark 的 UDS 功能。
- [x] 验证 direct-local TCP ATTACH 可以创建、关闭和清理全部 channel。

完成判据：VEMB V16 Aeron 不存在 UDS 控制路径；所有 Aeron control 操作通过 TCP
完成，且不存在 channel/ring 泄漏。

断点续作记录：

```text
状态：完成
代码提交：未提交
工作区状态：TCP control 重构已构建；client-topology 与 migration-control 单测通过
已删除的 UDS 入口：SDK UDS alloc/close/close-all、VEMB Aeron UDS listener 与配置分支
保留的非 VEMB UDS 功能：Redis 通用 Unix socket；测试中的 socketpair byte-stream harness；独立 TLC 工具
ATTACH/CLOSE 验证：2026-08-17 在 111 的 6396 direct-local smoke 中，prefill 与 read workload 分别完成 TCP ATTACH、UB ring 映射、close；server log 收到 close control，进程退出前无 channel 清理错误
下一步：进入固定 111/112 peer-view 的 v1 实机 ATTACH 验证
```

### 阶段 4：接入 Direct-Local UB-Aeron Transport

目的：在公共 interface 下实现 owner 111 的本机 UB 数据面。

- [x] 通过 TCP ATTACH 获取 channel descriptor、ring offset 与 warm descriptor。
- [x] 实现 direct-local resolver：ATTACH 返回 path 在本机直接 `open + mmap`。
- [x] 将 request publish、response poll、warm-handle read 接入 transport
  interface。
- [x] 按 channel generation 处理同一 UB backend 的 close、reattach 和 stale
  descriptor；不得在 reattach 时切换 owner 的 TCP/UB identity。
- [x] 验证单 owner 111 的 benchmark 默认路径通过公共 core 路由。
- [ ] 对比阶段 0 的 direct-local UB baseline，记录吞吐和延迟变化。

完成判据：owner 111 的 UB-Aeron channel 可替换 TCP backend，而不修改核心
topology、retry 或 benchmark 行为。

断点续作记录：

```text
状态：进行中
代码提交：未提交
工作区状态：TCP ATTACH + local `open + mmap` 已接入 SDK；control/data 边界已构建通过。
唯一的创建 API 是
`vemb_v16_client_create(seeds, seed_count, dim, timeout_ms, transport_type)`；`seeds`
是 TCP control 地址数组。SDK create 只保存地址，不建立 TCP data channel 或发送 `HELLO`；
首次 keyed operation 必定先拉取 topology。create 参数是 TCP/UB identity 的权威来源，
服务端 topology 负责 owner、epoch 和 endpoint，并必须与该 transport 匹配。每次 topology fetch 从轮转起点依次尝试所有 seed；
任一 seed 返回合法 snapshot 即发布，静态 seed 列表绝不充当 VEMB 路由表，也没有 legacy
TCP seed data channel、静态哈希路由或单 seed data fallback。
固定为 AERON 的 client 从 topology owner endpoint 建立 UB owner channel，而不是
使用 TCP seed channel。成功的 `VEMB_HANDLE` 绑定其 producing channel、channel generation 和
`region_id`；`read_vector` 只经该 transport 的 `read_warm_vector` 解引用，因此
channel close/re-attach 后旧 handle 不会误读新的或其他 owner 的 warm mapping。
ATTACH descriptor 契约：SDK 在 ATTACH 边界校验 path、backend、slot、ring count 与 warm descriptor
direct-local UB 验证：2026-08-17，111:6396，dim=300、100 key、t=1/c=1/pipeline=1；prefill 成功，read=5261.02 ops/s，avg=0.18895 ms，p99=0.19900 ms；CLI 与 server 访问同一份本机共享内存。response shmdev 的 `O_SYNC` 仅为驱动打开要求，不表示本地 transport 的 NC/CC 语义；warm descriptor offset=229064960
focused regression：`vemb_v16_ub_data_transport_ut` 用单个 TCP seed 的 topology refresh
广告 AERON owner，覆盖 UB ATTACH/publish/poll、反序 VSIM completion，以及带
`region_id=7` 的 VEMB_HANDLE 从 owner UB warm mapping 读取。该测试不替代真实
benchmark 默认 common-core 路径验收。
resource generation lifecycle：`vemb_v16_data_channel_t` 分别记录 SDK-local
`generation` 与 server-owned `resource_generation`。新增只读 TCP control frame
`VEMB_V16_NET_AERON_CHANNEL_STATUS`，它仅查询已 ATTACH channel 的 server resource
generation，绝不分配替代 channel。endpoint/backend 都稳定而 topology epoch 更新时，SDK
第一次使用 owner 会查询该值：一致则只记录已检查 epoch 并复用 channel；失效或不一致则由
owner-group completion loop 先交付全部 completion，`fence()` 确认 transport pending 为零，
再 close old mapping/channel 并以同一个 UB backend reattach。TCP backend 的查询恒为
`CURRENT`。control I/O 或 wire failure 使本次 owner 使用失败，不把未确认 VADD/VREM
重放。owner slot 在 same-backend reattach 中保留单调 local generation，因此旧
`VEMB_HANDLE` 即使 slot 被复用也会被拒绝。`vemb_v16_ub_data_transport_ut` 覆盖
同 generation 复用、generation changed 的 close/reattach、新请求成功与旧 handle 拒绝。
111 common-core 验收入口：`benchmark/vemb_v16_direct_local_ub_common_core_smoke.sh`
仅在 111 执行，使用 `127.0.0.1:PORT` TCP seed 通过 topology ctl 发布
`owner 0 = AERON`，随后不传任何 client-topology 参数地执行 default common core 的
VADD prefill 和 VEMB_HANDLE warm read。它要求 `[setup] bootstrap=tcp
topology-data=owner-fixed`、`fail=0`、完整 warm materialization，以及 server 收到 UB
ATTACH 和 CLOSE；脚本会拒绝 busy port/UB device，并仅停止自己启动的 test server。实机
默认使用 111 的隔离资源 request=`/dev/obmm_shmdev3`、response=`/dev/obmm_shmdev6`、
warm=`/dev/obmm_shmdev4@0`，三者不得复用同一段 backing；`229064960` 是历史满容量
warm layout 返回给 client 的 data-view offset，而非此脚本应配置的 `warm_mmap_offset`。
实机验收：2026-08-18 在 111 的 `127.0.0.1:6396`，dim=300、prefill=100、ops=100、
threads=1、pipeline=1。默认 benchmark common core 输出 `ok=100 fail=0`、
`read_bytes=120000`、qps=913.20；server 日志确认 `attached=2 closed=2`。artifact：
`/tmp/vemb_v16_direct_local_ub_20260818_125314_1347210`。性能对照仍延后至阶段 8。
性能对比：按当前项目安排暂缓至阶段 8
benchmark 默认路径已移除旧的显式客户端拓扑开关，以 TCP bootstrap seed 和固定
transport 创建每个 worker 独占的 SDK client，并强制 topology refresh；owner routing、retry 与 data
channel 完全由 common core 和 transport backend 持有。当前支持 pipeline=1 的
VADD、VREM、VEMB_HANDLE/VEMB_INLINE、VSIM_INLINE 与 PING；`vsim-key-key` 尚未
迁移至 SDK common core，明确拒绝。阶段 4 的功能验收已完成；性能对照保持延后至阶段 8。
下一步执行固定 111/112 peer-view v1 实机验证。
VEMB 不使用 `--cluster-mode` 区分数据面：`--protocol vemb_v16` 配合
`--vemb-v16-transport=tcp|aeron` 在 CLI 启动时固定数据面。`--cluster-mode` 保留给 Redis Cluster
兼容客户端，且 memtier 拒绝它与 VEMB V16 同时使用。
```

### 阶段 5：实现固定 111/112 Remote Peer-View Resolver

目的：让 CLI@111 可以通过同一 UB-Aeron transport 安全访问 owner 112。

身份约束：`owner_id` 是 topology 的逻辑 owner identity，不是机器编号。
当前两 owner 部署固定为 owner `0` = host 111、owner `1` = host 112；因此
`examples/vemb_v16_ub_peer_view_111_to_112.yaml` 中 remote 条目的
`owner_id` 为 `1`。`111`/`112` 只用于主机和资源命名，不能作为 common core 的
owner slot 或 manifest lookup alias。

- [x] 定义显式 manifest：`client_host`、`owner_id`、`resource_role`、
  `resource_id/generation`、provider path、client-visible path、map flag、
  cache policy。
- [x] 使用 manifest 替换 SDK 中的硬编码设备号翻译逻辑。
- [x] 将 v1 request ring、response ring、warm region，以及 v2 request/response
  descriptor 和 arena 分别经过 resolver 映射。
- [x] 在 CLI@111 对 server@112 执行 TCP ATTACH 并验证全部映射。
- [x] 验证 publish、poll、response identity、warm-handle read、close/reattach
  和 descriptor generation mismatch。
- [x] 证明 mapping 缺失、错误 path 或错误 access mode 会在 channel 建立时失败，
  不会进入数据面。

完成判据：CLI@111 能以固定 peer-view 配置稳定访问 owner 112；device path 选择
来自 manifest，而非 SDK 内嵌拓扑假设。

断点续作记录：

```text
状态：完成
代码提交：未提交
工作区状态：已实现静态 resolver 与 SDK/benchmark/memtier 接入；新增可重复的
peer-view transport unit 与 v1/v2 smoke。
固定 manifest 位置与版本：`examples/vemb_v16_ub_peer_view_111_to_112.yaml`，version 1
111 -> 112 映射验证：2026-08-17，CLI@111 到 server@112:6397 的 v1 TCP ATTACH 成功；request `/dev/obmm_shmdev3 -> /dev/obmm_shmdev7`、response `/dev/obmm_shmdev6 -> /dev/obmm_shmdev2`、warm `/dev/obmm_shmdev4 -> /dev/obmm_shmdev8` 均完成 manifest resolve、mmap 与 header/descriptor 校验。request view 是 CLI NC 写端，response view 是 CLI CC 读端，remote warm view 是 CLI NC 读端
数据路径验证：100 key prefill=59665.87 sets/s；v1 read t=1/c=1/pipeline=1 为 5037.20 ops/s，avg=0.19623 ms，p99=0.23100 ms。2026-08-17 以 `112:6397 -> 111` 的 v1-only NC-read smoke 复验：128 key prefill 成功；2 秒读完成 166184 个 VEMB_HANDLE，`publish_fail=0`、`handle_deref[ok=166184 fail=0]`，Totals=83089.55 ops/s。后续配置必须遵循固定角色策略，不再通过设备 open fallback 推断 access mode
2026-08-18 可重复 v1 实机验收：在 112 启动临时 owner（完整 UB warm manifest，
request=`/dev/obmm_shmdev3`、response=`/dev/obmm_shmdev6`、warm=`/dev/obmm_shmdev4`），
在 111 执行 `vemb_v16_peer_view_v1_smoke 192.168.90.112 6397
examples/vemb_v16_ub_peer_view_111_to_112.yaml 111 1 300 2 15000`。两轮均完成
`VADD -> response identity -> VEMB_HANDLE -> warm read -> CLOSE`：`cid/resource_generation`
为 `1/1`、`2/2`；第二轮 request/response ring offset 为 `380928/36864`，确认不是重用旧
descriptor。owner log 记录两次 `channel closing` 与 snapshot remove。客户端 artifact：
`/tmp/vemb_v16_stage5_peer_view_client_20260818_170013_1594090`；owner artifact：
`/tmp/vemb_v16_stage5_peer_view_standalone_20260818_170001_3690976`。这里的
`192.168.90.112` 只在已经登录 111 后用于访问 owner；Mac 登录两台机器仍使用
`43.154.145.18:8111/8112`
generation 语义：v1 ATTACH ABI 不携带 manifest resource generation；resolver 对 manifest
generation 严格匹配，`generation=2` 对 version-1 entry 被 unit 拒绝。运行期 resource
generation 是 server 分配的 channel identity，close 后旧 descriptor 不能复用，重新 ATTACH
必须得到新 identity。`vemb_v16_peer_view_transport_ut` 覆盖这两个条件；common-core 的
`AERON_CHANNEL_STATUS` generation changed close/reattach 仍由
`vemb_v16_ub_data_transport_ut` 覆盖
失败路径验证：`vemb_v16_peer_view_transport_ut` 的 fake owner 确认缺 request mapping 或
client path 不可 mmap 时，SDK 在任何 ring publish 前发送 `CLOSE_CHANNEL`；
`vemb_v16_ub_peer_view_ut` 确认错误 client/owner/role/provider/generation resolve 失败，
request ring 或 warm region 的错误 cache policy 在 manifest load 时被拒绝
2026-08-18 v2 资源级实机验收：111 执行 `vemb_v16_peer_view_v2_attach_smoke
192.168.90.112 6397 examples/vemb_v16_ub_peer_view_111_to_112.yaml 111 1 300`，
`cid=3` 成功映射 request descriptor/arena offset=`761856/798720`、response
descriptor/arena offset=`73728/110592`，并发送 CLOSE；artifact：
`/tmp/vemb_v16_stage5_peer_view_v2_20260818_170213_1599904`。这只验证四资源
resolver/ATTACH/mmap，v2 common-core batch data path 仍归阶段 6A
回归：本地和 111 均通过 `vemb_v16_ub_peer_view_ut` 与
`vemb_v16_peer_view_transport_ut`；后者覆盖 v1 publish/poll/identity/warm read/two-cycle
reattach、缺 mapping/错误 client path 的 publish-before-fail 保护，以及 v2 四资源
ATTACH/mmap/close。实机结束后 112 的 6397、3/6/4 和 111 的 7/2/8 均无残留 holder
下一步：阶段 6 将 peer-view resolver 接入 common cluster core 的多 owner 路由；TCP/UB
可比性能对照仍延后至阶段 8
```

### 阶段 6：启用单 CLI、多 Owner UB Cluster（v1 基线）

目的：让公共 cluster core 在 CLI@111 上同时管理 direct owner 111 与 remote
peer-view owner 112。

- [x] SDK/CLI 启动参数固定 data transport identity；topology endpoint 必须匹配
  该 identity，但不包含 client 私有 UB path。
- [x] owner 111 v1 channel 选择 direct-local resolver，owner 112 v1 channel
  选择 static peer-view resolver。
- [x] 实现 topology 更新后 owner channel 的按需 attach，避免 worker 读到半就绪
  owner；新 topology 中首次出现的 owner 不在 refresh 前预热连接。
- [x] 覆盖 stable two-owner 随机 R:R 路由。
- [x] 覆盖 forced `ASK`、`MOVED`、`STALE_TOPOLOGY`，确认 retry 复用 TCP
  cluster core 行为。
- [x] 验证中间 redirect 不记为 miss/final error，最终统计按 common core 输出。

完成判据：同一个 CLI@111 可在一个 topology snapshot 中通过 v1 正确路由两
owner，且两条 UB 数据路径的差别只存在于 direct/peer-view resolver。

断点续作记录：

```text
状态：已完成（阶段 6 v1 基线）
代码提交：
工作区状态：SDK 的唯一 `vemb_v16_client_create(..., transport_type)` 在启动边界
固定 TCP/UB 以及 `VEMB_INLINE/VEMB_HANDLE`；其后新增
vemb_v16_client_configure_ub_peer_view(client, manifest, client_host)。它只在
首次 UB owner channel 前可配置；之后无论该 UB channel 仍打开或已 close，均拒绝
替换 resolver，避免同一 owner 的 mapping 身份漂移。manifest 中每个 topology
owner 的 AERON endpoint 都必须有匹配项，direct-local owner 使用 provider/client
相同路径。TCP bootstrap 和 topology wire 不携带 client 私有 UB path。
benchmark common-core 路径以 `--ub-peer-view-manifest` 与
`--ub-peer-view-client-host` 调用同一 SDK API；已删除无效的
`--ub-peer-view-owner-id`，remote owner 不再由 CLI 手工指定。
逻辑身份：owner 0=host 111，owner 1=host 112；示例 manifest 已从阶段 5 standalone
smoke 使用的物理别名 112 收敛为 topology owner 1，不保留双 alias。
本地回归：vemb_v16_peer_view_transport_ut 新增 SDK cluster remote-owner 路径，验证
TOPOLOGY_GET -> owner 1 AERON -> remote ATTACH -> VADD -> VEMB_HANDLE -> warm read
-> CLOSE；同一 manifest 对 owner 0 验证保持 direct-local ATTACH，并验证运行期重新
配置被拒绝。vemb_v16_ub_data_transport_ut 与
vemb_v16_cluster_core_ut 通过。
两 owner topology 与 channel ready 状态：2026-08-18 在既有 111:6396（owner 0）和
112:6397（owner 1）服务上发布 epoch=1、active={0,1} 的 AERON topology 后已验证。
CLI@111 以 111:6396 为 TCP seed、`examples/vemb_v16_ub_peer_view_111_to_112.yaml`
和 client host `111` 执行 common-core `dim=300, prefill=128, ops=128, pipeline=1`
的 VADD + VEMB_HANDLE；输出 `ok=128 fail=0 read_bytes=153600`。日志分别出现
direct-local owner 0 ATTACH 与 remote peer-view owner 1 的 cross-node ATTACH，说明
同一 topology snapshot 下两条 v1 数据面能够并存完成请求。
本次实机首次失败不是 data-plane completion 丢失：112 正确返回
`/dev/obmm_shmdev3`、`/dev/obmm_shmdev6` 与 `/dev/obmm_shmdev4`，但 111 上遗留的
示例 manifest 仍将物理主机别名写作 `owner_id: 112`。SDK 以 topology owner `1`
解析资源，因此在任何 UB mmap/publish 前拒绝该条目并发送 CLOSE_CHANNEL。示例
manifest 已保持 `owner_id: 1`，同步脚本也将 `.yaml/.yml` 纳入 SHA-256 同步候选，
避免未跟踪部署配置未下发而与代码身份语义漂移。
topology epoch channel lifecycle：endpoint/backend 不变时，首次使用新 snapshot 的
owner 仅执行 `AERON_CHANNEL_STATUS` resource-generation 检查；generation 相同即复用
既有 channel，绝不因 epoch 单独 CLOSE。generation 已变才在该 owner 已 drain 后
close/reattach，并以单调 SDK-local generation 使旧 VEMB_HANDLE 失效。
`vemb_v16_ub_data_transport_ut` 覆盖 stable epoch 的无 ATTACH 复用、generation changed
的 drain/reattach 与 stale handle 拒绝；`vemb_v16_cluster_core_ut` 覆盖 topology publish
后 owner-ready 状态保持。两者已于 2026-08-18 通过。
R:R 与 redirect 回归：
stable two-owner random R:R：2026-08-18，CLI@111 在相同 epoch=1 topology 下执行
`prefill=1024, keyspace=1024, --key-pattern random, ops=4096, pipeline=1` 的
VADD + VEMB_HANDLE 随机混合读写；`ok=4096 fail=0`，其中 VADD=820、
VEMB_HANDLE=3276、`read_bytes=3931200`。`--key-pattern random`
以 global operation id 的固定 64-bit mix 从 prefilled keyspace 选 key，避免全局
`rand()`，默认 `sequential` 行为不变且该 workload 可复现。日志确认每个 SDK client
均建立 owner 0 direct-local 与 owner 1 remote peer-view channel；112 收到并关闭
remote channel 12、13，无本轮遗留 holder。
forced redirect：`vemb_v16_ub_data_transport_ut` 增加 SDK + UB fake-owner 的三分支
回归。ASK 在原 channel 上以 `VEMB_V16_REQ_F_ASK_REDIRECT` 重投且不刷新 topology；
MOVED 与 STALE_TOPOLOGY 均先通过 TCP bootstrap 获取 epoch 43，再以
`AERON_CHANNEL_STATUS` 确认既有 resource generation 并复用 channel。三种分支的
终态均为 VEMB success（score=37），观测计数分别只有对应 redirect=1；ASK 的
topology refresh=1，MOVED/STALE=2。因此中间 redirect 不会成为调用者的 final error
或 common-core 最终失败统计。2026-08-18 已通过。
统计对比：不在本阶段执行；TCP/UB 可比性能与火焰图继续留在阶段 8。
阶段 6 v1 基线完成；后续进入阶段 6A 的 v2 owner session/batch 状态机。TCP/UB
性能对照仍按阶段 8 延后。
```

### 阶段 6A：统一 v1/v2 提交与扩容状态机

目的：使 v2 只作为 v1 之上的稳定 topology 优化，并确保扩容期的 routing、
retry、统计和资源所有权都由公共 core 决定。

- [x] 为每个 worker/owner 创建 owner session，包含永久 v1 channel、可选 v2
  channel 和 session generation。
- [x] 将跨调用的 L0 group/batch map 接入 SDK common-core owner session。
- [x] L0 已支持由调用方传入 snapshot route identity；group identity
  包含 owner、topology epoch 与 owner generation，且同 key 的不同 identity
  不会 coalesce。
- [x] 让 v1 fallback 从公共 core 接收当前 target owner、topology epoch 与
  ASK flag，不得直接回发到旧 batch session 的固定 endpoint。
- [x] L0 batch record 已保存 publish identity；v2 wire 增加显式
  `submit_epoch` 入口，response 可按该 batch 的 epoch 比较，而非 ATTACH epoch。
- [x] 已实现并单测 `V2_READY -> V2_QUIESCING -> V2_DRAINING -> V1_ONLY ->
  V2_REOPENING -> V2_READY` 的纯 owner-session 状态转换。
- [x] topology epoch 改变或扩容 prepare 时停止新 SDK v2 frame；已发布 batch
  继续 poll/consume，未发布 L0 group 先从 map 移除、回到 common-core route，再以
  UB v1 提交。
- [x] migration active 期间强制新请求走 v1；`ASK` 在下一轮通过 v1 携带
  `VEMB_V16_REQ_F_ASK_REDIRECT` 完成单次 redirect。
- [x] topology 稳定后，remote peer-view owner 可 ATTACH/map v2 四项资源并重启
  v2 session；VEMB_HANDLE 的 warm region 仍属于永久 v1 owner channel。
- [x] 覆盖 v2 ATTACH rejection 后的 v1 pipeline fallback。
- [x] 覆盖整帧 stale 后 topology refresh 与 v1 pipeline fallback。
- [x] 覆盖 mixed item response 只重投 pending item、v2 ASK 经带 flag 的 v1
  retry、migration active 强制 v1 与稳定后恢复 v2。
- [x] 为 mixed v1/v2 完成提供独立 logical operation 计数可观测性。

完成判据：v1 和 v2 不再各自决定 owner 或 retry；扩容期间所有新请求安全走 v1，
扩容后 v2 可恢复，且不产生 lost/duplicate completion 或重复统计。

断点续作记录：

```text
状态：已完成（6A UB VEMB_HANDLE owner session / cross-call L0）
代码提交：
工作区状态：新增 clients/c/vemb_v16_owner_session.[ch]。SDK 对每个已打开的
owner 保存 owner session；TOPOLOGY_GET publish 会将 epoch 与
DUAL_WRITE_REQUIRED（prepare/migration）通知该 session，v1 channel reopen 会同步
cluster-core owner generation。SDK common path 现为 stable remote-AERON owner
创建可选 v2 batch channel；session 仍不自行做 owner 或 retry 决策。
owner session 与 batch state：L0 新增 submit_with_identity()，entry 与 batch record
均保存 {owner_id, topology_epoch, owner_generation}；prepare_batch() 只生成单一
identity 的 frame。vemb_v16_aeron_batch_publish_handle_at_epoch() 将每次 publish 的
submit epoch 编码入 v2 frame；旧 publish API 只是 fixed-topology compatibility wrapper。
旧 standalone aeron_batch_client 也改为将其固定 route identity 写入 L0，并按已发布
batch 的 identity 比较 response epoch；它不是 common-core cluster 路径，仍不能用于
迁移期 stale 后的重新路由。
v2 quiesce/drain 结果：vemb_v16_owner_session_ut 覆盖 epoch 变化、migration prepare、
已发布 batch drain、generation fence、attach failure 与 stable reopen，已通过。
SDK v2 pipeline：新增 `vemb_v16_client_vemb_handle_pipeline()`。一个 owner group
只有同时满足 `VEMB_HANDLE`、同 owner、同 submit epoch、无 ASK flag、remote AERON、
有效 peer-view、session `V2_READY`、并且不超过协商 batch size 时才发布 v2 frame；
其余情形直接走 common-core v1。v2 response epoch 与该 frame 的 submit epoch 比较；
不一致按 `STALE_TOPOLOGY` 交给 core，MOVED/STALE refresh 后本次 pipeline 对该 owner
强制 v1，ASK 下一轮带 flag 强制 v1。v2 ATTACH/publish 不可用也不改变 operation，直接
回退 v1。普通 epoch 更新只停止 v2 submit 并保留 endpoint-stable v1/v2 mapping；仅 v1
resource generation 真实变化或 channel failure 才共同 CLOSE/reattach。
本地 common-core v2 回归：`vemb_v16_peer_view_transport_ut` 新增
`TOPOLOGY_GET -> v1 ATTACH -> v2 ATTACH -> v2 descriptor/arena request -> batch
response -> v2/v1 CLOSE` fake-owner 用例，验证 v2 request submit epoch=41、两个
VEMB_HANDLE descriptor response 和资源关闭顺序；同一测试还覆盖 v2 ATTACH rejection
后不发布 v2 frame、同一 `VEMB_HANDLE` pipeline 经 v1 完成，以及整帧 response epoch
mismatch -> topology refresh(epoch 42) -> stable v1 resource check -> epoch 42 v1 retry。
`vemb_v16_owner_session_ut`、
`vemb_v16_cli_l0_ut`、`vemb_v16_batch_ring_ut`、`vemb_v16_cluster_core_ut`、
`vemb_v16_ub_data_transport_ut`、`vemb_v16_tcp_data_transport_ut` 也已通过。
新增 mixed/migration 回归：同一 v2 frame 的 terminal + STALE item 只将 stale item
经 epoch 42 的 v1 重投；v2 ASK 以 `VEMB_V16_REQ_F_ASK_REDIRECT` 经 v1 完成；
`DUAL_WRITE_REQUIRED` snapshot 首轮不发送 v2 ATTACH，稳定 epoch 42 后同一 SDK client
完成 resource check、v2 ATTACH 与 batch publish。未完成：SDK common-core 尚未把 L0
跨调用 coalescing/batch map 接入。新增 `vemb_v16_client_get_logical_stats()` 直接投影
cluster core 的 final logical counters；normal v2、stale、mixed、ASK、migration-recover
回归均断言 success/not-found/error 总数，确认重试不重复完成或计数。
跨调用 L0：新增 `vemb_v16_client_handle_session_create/submit/flush/poll/close`。
每个 submit 先获得 core operation，再以 `{owner_id, topology_epoch,
owner_generation}` 进入 owner-local L0；v2 或 UB v1 的单个 group response 通过
fanout 回到每个 logical operation，由 core 各自完成或重试。该 API 只接受 AERON
client；TCP `VEMB_HANDLE` legacy 已删除：同步 handle、handle pipeline 和 event-loop
session 在公共 API 边界直接拒绝，不触发 topology 或 data I/O；`vemb_vector()` 则按
TCP client 固定模式直接走 `VEMB_INLINE`。TCP server 也拒绝低层误发的
handle frame。该收敛不影响 backend-neutral L0/batch-agg 或 cli-cache 对 TCP 非-handle
操作的演进。
v2 ATTACH rejection 在同一 topology epoch 锁定 UB v1，避免每个 submit 重复 ATTACH。
topology/migration quiesce 会 drain 仅未发布的 L0 groups，已发布 frame 仍由 response
路径处理；drained request 在新 snapshot 下强制经 UB v1 重投。
`vemb_v16_peer_view_transport_ut` 的新增聚焦回归覆盖：两个跨调用同 key -> 一个 v2
item / 两个 completion、v2 ATTACH rejection -> 两个 UB v1 completion、未发布 group 的
epoch 41 -> 42 quiesce/drain -> UB v1 completion，并断言 logical counter 无重复。
本地回归：vemb_v16_owner_session_ut、vemb_v16_cli_l0_ut、
vemb_v16_cli_l1_materialization_ut、vemb_v16_batch_ring_ut、
vemb_v16_ub_data_transport_ut 通过；vemb_v16_tcp_data_transport_ut 在受限沙箱外通过。
本地回归追加 `vemb_v16_tcp_data_transport_ut`：TCP client 下同步 handle 与 handle
pipeline 在公共 API 边界返回错误，不触发 topology 或 data frame；
`vemb_vector()` 则只发 `VEMB_INLINE`，logical stats 为一次成功、零 handle error。
下一步：进入阶段 6B 的 backend-neutral logical vector-read session；阶段 7 的真实
111 -> 112 扩容编排在 6B 的 TCP/UB 聚合回归完成后继续。
```

### 阶段 6B：TCP/UB 共用的逻辑读聚合与 CLI cache

目的：将跨调用 L0 和 completed-vector CLI cache 从 UB `VEMB_HANDLE` 的实现细节中
抽离，使同一 logical vector read 按 client 启动 transport 使用固定的数据交付方式。
TCP 不再、也不会实现 handle 或 warm-region capability。

首个可交付的 operation 是 key-only vector read。公共 core 先给出
`{owner_id, topology_epoch, owner_generation}`，再使用 client 启动时固定的 wire op：

```text
logical vector read(key, key_hash, dim)
  -> L0 identity(key, owner, epoch, generation)
  -> TCP client: VEMB_INLINE -> inline bytes
  -> UB client:  VEMB_HANDLE -> one warm read
  -> one materialized vector -> leader + followers completion
```

- [x] 新增 common-core vector-read session；每个 logical request 只由 core 做一次
  final completion/统计，L0 follower 不拥有独立 wire request。
- [x] TCP leader 使用普通 `VEMB_INLINE` frame；“batch-agg”在 TCP 首版仅表示
  coalescing 和多 leader 调度，不新增 TCP packed batch frame，也不打开 warm mapping。
- [x] UB leader 保持 `VEMB_HANDLE` + channel-owned warm read；同一 L0 group 只能
  materialize 一次，再 fan-out 临时只读 vector view。
- [x] 该 session 固定 logical op=`vector read` 与 client dim；L0 的存储 identity
  包含完整 key bytes、key hash、owner、topology epoch 与 owner generation。因此后续
  `VSIM` 只有在完整 query payload identity 纳入 group key 后才能进入；VADD/VREM 不
  coalesce。
- [x] completed-vector CLI cache 的 storage、lookup 和 completion 接口保持 backend
  neutral，TCP cache fill 来自 inline payload，UB cache fill 来自一次 warm read。
  默认禁用 cache；启用必须选择显式的 consistency contract。没有 server value generation
  或可靠 mutation invalidation 时，不得把 cache hit 当作线性一致读。
- [x] topology epoch/owner generation 改变或 session teardown 会清空 session cache；
  session 与同步 keyed API 排他，因此 VADD/VREM 不能在其生命周期内交错。cache hit
  不打开 channel 或制造 wire req_id。将 cache 提升为跨 session 或并发写可见的功能前，
  仍需接入可靠 mutation invalidation。
- [x] UT 覆盖 TCP 同 key双 submit -> 一个 `VEMB_INLINE` wire request/两个 logical
  completion，UB 同 key -> 一个 handle + 一次 warm materialization，TCP snapshot
  cache hit 零 data frame，以及 epoch change 后同 key 必须重新发送 inline frame。TCP
  handle 继续为零 frame。
- [x] vector session 的 ASK/MOVED/STALE 与 cache-disabled repeated-read
  回归：ASK 直接以 `VEMB_V16_REQ_F_ASK_REDIRECT` 重投，不经 L0 重建；
  MOVED/STALE 刷新 snapshot 后以新 epoch 重投。默认 disabled cache 的
  连续同 key read 必须产生两条 `VEMB_INLINE` frame。

完成判据：对启用的 logical vector-read session，TCP 和 UB 的差异只存在于 leader 的
data delivery；L0、completion、retry、cache policy 与 logical stats 均由公共层表达。

断点续作记录：

```text
状态：已完成（6B1 logical vector-read L0 + explicit snapshot cache）
代码提交：
工作区状态：新增 vector session。TCP leader 使用 VEMB_INLINE；UB leader 使用
VEMB_HANDLE 并在 group 内只 materialize 一次。6A handle session 仍仅适用于 UB；TCP
handle 已在 SDK 和 server 两端删除，本阶段不恢复它，也不为 TCP 增加 v2 batch/warm mapping。
cache：默认 disabled；仅显式 IMMUTABLE_SNAPSHOT 可启用。它从 TCP inline 或 UB warm
materialization 填充，cache hit 不创建 data channel/wire request；topology 观察到变化和
session close 均清空本地 cache。
正确性结果：vemb_v16_tcp_data_transport_ut 覆盖 TCP L0 coalesce、snapshot cache hit、
epoch invalidation、default cache-disabled repeated read，以及 ASK/MOVED/STALE retry；
vemb_v16_ub_data_transport_ut 覆盖 UB L0 coalesce + 一次 warm materialization；完整
vemb_v16_peer_view_transport_ut、SDK static 和 vemb_v16_bench 均已在本地通过。
遗留：strict cache 仍需要 server value generation 或跨 client mutation invalidation；
它不属于显式 `IMMUTABLE_SNAPSHOT` contract，也不应被标记为线性一致。
```

### 阶段 7：实现 111 单 Owner 到 112 Remote Owner 扩容编排

目的：把已有 migration protocol 与公共 core、UB transport 接入同一条
`baseline -> during_scaleout -> after` 实验流程。

- [x] 新建 `benchmark/vemb_v16_scaleout_ub_cluster_111_to_112.sh`，使用仓库内
  VEMB-aware common core，不使用 TCP-only origin memtier 代替 UB 数据面。
- [x] baseline：仅 owner 111 active，CLI@111 使用 direct-local UB；bootstrap 和
  初始 topology 只包含 owner0，确认 prefill 与 baseline read 成功。
- [x] prepare：部署 server@112 为 standby，并在 CLI 启动前安装完整的静态
  peer-view manifest；不预注册 owner1 endpoint，不提前 ATTACH remote v1/v2。
- [x] during_scaleout：执行数据/metadata prepare、candidate topology、
  migration fence、v2 quiesce/drain、epoch publish 和 old-epoch drain；新节点
  由 `STALE_TOPOLOGY/MOVED -> TCP refresh -> peer-view resolve -> TCP ATTACH`
  动态建立 v1 channel，所有新 logical operation 使用 v1。
- [x] after：owner 111 与 112 都 active，CLI@111 分别使用 direct 和
  peer-view v1 channel 路由随机 R:R 流量；topology 稳定后验证 v2 reopen。
- [x] 汇总 common 与 UB 专属指标，归档原始日志、manifest、测试 commit 和
  worktree diff。

完成判据：完整 `111 single owner -> 112 remote owner` 扩容可重复完成，所有阶段
结果可追溯，扩容后稳态 final error 接近零。

断点续作记录：

```text
状态：已完成。`ub_scaleout_fix_20260821_0020` 已验证大负载控制面、迁移 lifecycle、
owner1 动态 refresh/ATTACH 和 old-key correctness；
`ub_scaleout_fix_smoke_20260821_0245` 已补齐 baseline/during/old-key/steady-key 四阶段。
代码提交：
工作区状态：UB 资源映射只携带路径和角色；所有 UB open+mmap 统一先尝试
`O_RDWR`，在 `EPERM/EACCES` 时以 `O_RDWR|O_SYNC` 重试。manifest 不再要求
`cache_policy`，同机路径保持 CC；imported dev5..8/dev13..16 的 NC 访问尚未
通过当前 root 硬件的实际映射属性得到证明。
脚本与部署参数：`scripts/hpc_redis_scaleout_throughput.sh` 与
`scripts/vemb_v16_expand_ub_memory_2node.sh` 已接入路径/offset 配置；不再生成或
读取 `cache_policy`。reset 只写本机拥有的 UB backing 和 NC 发送环，不重置 peer
CC view。
baseline/during/after 结果目录：111/112 `benchmark/results/scaleout/nc_cc_scaleout_20260817_1720/`（TCP control/data workload，用于验证 server-side migration/UB-RPC，不是本阶段 UB client cluster 验收）。baseline=9.75M ops/s，during=10.08M ops/s，after=10.03M ops/s。
迁移 epoch 与 fence 记录：candidate=1786958553，full-active=1786958554；`scaleout_all_sources_done=1`、两个 owner 的 full-active publish 均成功。
P7 runner：`benchmark/vemb_v16_scaleout_ub_cluster_111_to_112.sh`。它从 Mac
仅经 `NODE0_SSH_HOST:NODE0_SSH_PORT` 和 `NODE1_SSH_HOST:NODE1_SSH_PORT` 管理两台
机器，默认分别为 `43.154.145.18:8111`、`43.154.145.18:8112`；topology、server
control、AERON ATTACH 与 peer-view 一律继续使用 `192.168.90.111/112`。server-side
UB-RPC 与 client AERON ring 即使复用设备别名，也使用不重叠的 offset 范围。

runner 顺序为启动 owner0、发布仅含 owner0 的初始 topology、prefill、baseline read；
随后启动 owner1 standby，启动 migration workload 和 TCP coordinator，提交 candidate
topology。期间 CLI 只以 owner0 作为 bootstrap seed，收到 `STALE_TOPOLOGY/MOVED` 后
通过 TCP refresh 获取 owner1 endpoint，再动态解析 peer-view 并 TCP ATTACH 建立
remote v1 channel。full-active 后执行随机 direct/peer-view v1 read，以及稳定 topology
后的 v2 reopen。它将 manifest、topology、coordinator、workload、server log、commit 和
worktree diff 收集到唯一 `benchmark/results/scaleout/<run_id>/`。

`scripts/hpc_redis_scaleout_throughput.sh` 新增 `DATA_TRANSPORT=ub`。该模式调用
同一 P7 common-core runner，产生 `baseline/during/after` 固定操作量 QPS；默认
`DATA_TRANSPORT=tcp` 保留既有 memtier TCP 数据面口径。UB 模式不把 memtier 结果标记为
UB 吞吐。

历史真实硬件回归：`p7_ub_smoke_20260818_3`，`DIM=16`、`PREFILL_KEYS=64`、单 CLI@111
单线程，迁移期 1024 个 v1 mixed operations、full-active 后 512 个随机 v1 handle read。
结果为 baseline `894.09 qps`、during `944.10 qps`、after `935.60 qps`，均为零失败。
candidate epoch=`1787062617`，full-active epoch=`1787062618`；coordinator 记录
`scaleout_all_sources_done=1` 和两个 owner 的 full-active publish 成功。after 输出记录
owner 1 的 `cross-node ch ok` 与 `/dev/obmm_shmdev8` remote warm peer-view，稳定 topology
后 remote v2 reopen 成功。完整产物位于
`benchmark/results/scaleout/p7_ub_smoke_20260818_3/{node0,node1}/`，包含 topology、
server/coordinator/workload 日志、manifest、commit 和 worktree diff。

此前一次统一参数短测：`ub_scaleout_20260820_155951`，配置为
`PREFILL_KEYS=10000`、`VNODE_COUNT=100`、`PIO=7`、`SNW=7`、`t=64`、`c=4`、
`pipeline=32`。owner0/owner1 的 warm、remote-meta、UB RPC 和 client peer-view
初始化成功。`topology_baseline.out` 的 `active_owners=0` 是 owner ID 列表，不是
数量为零；控制面返回 `status=0`、`flags=0x1`、owner0 endpoint。该轮 prefill 的
10000 个 SET 因旧版二进制/attach 配置不一致全部失败，不能作为当前流程结论。
随后统一重建并修正配置后，prefill 与 baseline 已通过；扩容验收必须继续使用本节
规定的动态 refresh/attach 顺序。

下一步：进入阶段 8，补齐 resource lifecycle、失败分类和 TCP/UB 同 workload
可比性能验收。NC/CC 的最终验收还需要驱动层或映射属性日志提供 imported path
的可观测证据。
```

### 阶段 8：正确性、资源生命周期与性能验收

目的：在完成系统链路后确认协议、资源和性能均满足扩容要求。

- [ ] 压测 attach/close/re-attach，确认无 UB channel、ring、mapping 或 fd leak。
- [ ] 覆盖 cutover 前后 warm/cache lookup，确认无 stale warm-vector read。
- [ ] 覆盖 topology 连续 refresh，确认 epoch 单调且 worker 不读半更新 snapshot。
- [ ] 覆盖 retry exhaustion、peer-view mapping failure、ring publish failure 和
  warm-handle read failure 的最终错误分类。
- [ ] 覆盖 v2 quiesce、v1 fallback、mixed v1/v2 completion 和 v2 reopen，确认
  每个 logical operation 只完成和统计一次。
- [ ] 对 TCP 与 UB 数据面使用相同 workload、keyspace、topology 进行对比。
- [ ] 在性能报告中分别给出 direct UB、remote peer-view UB、全量扩容窗口的
  QPS、p50/p99/p999 与 CPU 使用情况。

完成判据：无 lost/duplicate completion、无 stale warm-vector read、无资源泄漏，
topology epoch 单调递增；扩容后稳态 final error 接近零，并有可复现性能记录。

断点续作记录：

```text
状态：进行中。2026-08-23 host-mt、Aeron best（21:21、7:7）、Aeron cross-node，
以及 TCP/UB data-plane 的 111 -> 112 scaleout 均已完成单轮四阶段吞吐验收；资源
生命周期、连续 refresh、失败分类和 imported NC 属性证据仍待补齐。
代码提交：
工作区状态：SDK/CLI transport 已在创建边界固定；热点路径重复防御判空已按调用
契约收紧。代码已同步 8111/8112，两端 server/client build stamp 均为 OK。
TCP scaleout 运行 `scripts/hpc_redis_scaleout_throughput.sh`，远端 server 日志确认
`data_transport=2`（TCP），owner0 initial topology/prefill、owner1 standby、
migration/cutover、full-active publish 和 after 两个 keyspace 均完成；coordinator
报告 `scaleout_all_sources_done=1`、两个 owner 发布成功、`errors=0`。raw memtier
Totals 中 baseline/after 的 MOVED、ASK、Misses 均为 0，during 只有扩容期间预期的
MOVED 计数。该结果是单轮 `PREFILL_KEYS=10000`、`t64/c4/p32`、`PIO/SNW=21` 的
TCP QPS 记录；UB-Aeron 对应结果见上方独立 UB 扩容表。
资源生命周期结果：本轮未执行 attach/close/re-attach 泄漏专项；暂停 scaleout 后
6397/7397 无监听且无本轮 pidfile 残留。
性能结果：host-mt `11.468M ops/s`（16:16）；Aeron local
`31.744M ops/s`（21:21）和 `16.634M ops/s`（7:7）；TCP scaleout baseline/
during/after-old/after-steady 分别为 `11.620/12.239/11.769/11.807M ops/s`；
UB-Aeron scaleout baseline/during/after-old/after-steady 分别为
`17.419/14.380/12.734/11.230M ops/s`。
详细延迟、CPU 和产物目录见“本轮远端同步与回归结果”。
遗留风险与下一步：补资源泄漏、连续 refresh、失败分类和 imported NC 映射属性证据；
cross-node 与两类 scaleout 结果已记录，仍需在同一 workload 口径下完成更严格的
TCP/UB 可比性能报告。
```

### 阶段 9：动态 Peer-View 生命周期（后续）

目的：在静态 111/112 路径完成验收后，才允许扩展动态创建、映射、释放和删除。

- [ ] 实现 `prepare_peer_view`、`resolve_peer_view`、`release_peer_view`、
  `delete_peer_view`，并以 lease 和 generation 表达资源所有权。
- [ ] 验证新 generation 仅在映射可用后发布。
- [ ] 验证旧 generation 只在所有 channel close 与 old-epoch drain 后回收。
- [ ] 覆盖动态创建失败、重复 apply、回滚和并发 topology refresh。
- [ ] 证明 cluster core、迁移 fence 与 request/response 数据面 API 无需因
  动态资源管理而改变。

完成判据：动态 peer-view 仅替换 resolver 后端，静态 111/112、TCP 和 UB cluster
回归继续通过。

断点续作记录：

```text
状态：未开始 | 进行中 | 完成 | 阻塞
代码提交：
工作区状态：
动态资源/lease 验证：
静态路径回归：
剩余风险：
下一步：
```
