# TLC HA 跨节点 Checkpoint 与 AOF Tail Resync 设计

## 目标与边界

本文定义 Follower 无法继续增量复制时的跨节点恢复流程：Leader 将一个已验证的
checkpoint generation 传给 Follower，随后传输该 checkpoint 之后所需的 AOF tail，
使 Follower 恢复为连续、durable、可 apply 的状态。

目标状态为：

```text
Leader durable prefix = H
Follower durable prefix = H
Follower applied prefix = H
Follower 从 H + 1 继续接收普通 EVENTS
```

该流程服务于 AOF 已 compact、无法恢复的 GAP/checksum conflict、Follower 本地
checkpoint 损坏和显式全量重建。它不实现自动 reconnect、故障检测、切主、fencing
决策或多副本 quorum；这些机制在需要恢复时调用本状态机，而不是内嵌在 checkpoint
传输协议中。

本文细化 [TLC HA 数据同步设计](./TLC_HA_DATA_SYNC_DESIGN.md) 的 Snapshot resync
章节。现有 `tlc_cold_export_checkpoint()`、`tlc_cold_import_checkpoint()`、
`tlc_cold_replay_after()` 与 `tlc_cold_replay_range()` 是 COLD 格式基础，但尚未由
Replica channel 编排为跨机协议。

## 现有前提

当前 checkpoint 是带 generation、`ha_term`、每个 meta shard `captured_seq` 和全量
checksum 的不可变 blob。`tlc_cold_import_checkpoint()` 只接受空的、fenced 的 COLD
runtime：目标不得已有 manifest、AOF append 或并发 lifecycle 变化。这个约束必须
保留，不能为了 resync 在活跃 Follower COLD 上原地覆盖文件。

checkpoint 的 `checkpoint_seq` 是所有 shard `captured_seq` 的最小值，并不表示每个
shard 都在同一个 seq 拍下快照。因此 tail replay 不能简单按一个全局 `N + 1` 对
每个 shard 盲目 apply；必须保留并使用 checkpoint 的 per-shard `captured_seq`，按
`tlc_cold_replay_after()` 的语义重建逻辑状态。同时，为维持全局连续 AOF 前缀，
Follower 仍须持久化从 checkpoint 最小 seq 之后的完整有序 event 流。

## 当前状态与下一步

截至当前实现，以下基础边界已完成并有本地与 111/112 UB 回归：

- fuzzy checkpoint replay：`captured_seq[shard]` 覆盖的 event 不改 WARM，但推进全局
  apply prefix；
- Leader 普通发送队列满时不阻塞写入，记录当前未入队 event 的最早
  `replay_from_seq`，由 sender 从 Leader AOF 补读；
- Follower apply worker 在初始空队列时保持存活，可等待稍后到达的 replay；
- `tlc_core_resync_from()` 先导出并验证 Leader checkpoint blob，随后对已 fenced 的
  非空 Follower 在同一 `tlc_core_t`/`tlc_cold_t` 对象中清空 COLD-owned AOF/checkpoint
  文件、清理 WARM/key-meta，并 import blob 后 replay tail。这里没有 staging runtime、
  目录切换或 active runtime 双写；reset 后失败由上层保持 FENCED。
- M1 的 COLD snapshot session 已完成：`tlc_cold_begin_resync_snapshot()` 在同一
  checkpoint/io 锁序内导出已验证 blob、复制 `captured_seq[]`、固定 durable boundary
  `B` 并注册 AOF retention pin；`tlc_cold_read_resync_snapshot()` 分批连续读取固定
  `[checkpoint_seq + 1, B]`，起点不再保留时返回
  `TLC_COLD_RESYNC_REQUIRED`；`tlc_cold_end_resync_snapshot()` 释放 blob 与 pin。
- M3 已完成：`SNAPSHOT_BEGIN/CHUNK/END` 使用固定网络字节序 codec 接入既有 STREAM 与
  UB listener，保留旧 `EVENTS/ACK/HEARTBEAT` header/layout；Follower-only assembler
  对 term、identity、session、连续 offset、chunk checksum 与总 blob checksum 做无副作用
  校验，错误只进入 assembler `FAILED`，不会修改 active COLD/Core。成功 artifact 通过
  `tlc_ha_replica_take_resync_snapshot()` 转交给安装阶段。
- M3 本地 UT 覆盖成功组装、identity/offset/session、chunk checksum 和总 checksum 拒绝，
  以及 STREAM 上 artifact 后普通 EVENTS 仍可复制。111/112 UB 回归已验证 200KB checkpoint
  按 32KB chunk 穿过真实 ring（超过单 slot 的普通 event payload），并同时保留正常 10,000
  event 复制、Follower COLD recovery 和重启后手工 AOF replay 回归。
- M4 的安装边界已完成：`tlc_core_install_resync_checkpoint()` 将已验证 blob 安装到唯一
  active runtime，顺序执行 fenced reset、COLD import、Core/WARM recovery 和
  `replica_applied_seq = checkpoint_seq`。`tlc_core_resync_from()` 已复用该边界；UT 从含有
  旧数据的 Follower 安装 artifact，确认旧 key 清除、checkpoint 状态恢复且 durable prefix
  回到 checkpoint seq。
- M4 的 Follower 协调已接线：`tlc_ha_replica_install_resync_snapshot()` 先置 FENCED，
  等待 ingress/apply in-flight 计数归零，丢弃旧 apply queue 后调用 Core install。完成
  artifact 由 listener 用 release-CAS 发布、installer 用 acquire-exchange 领取，
  `resync_mutex` 已删除；正常 EVENTS/COLD/WARM 路径没有新增 mutex。
- M4 的文件化 artifact 已完成：Follower 的 assembler 将每个校验通过的 chunk 用
  `pwrite` 写入 `<cold-dir>.resync/<session>/checkpoint.blob.part`，不累计完整 blob。
  `SNAPSHOT_END` 先 `fsync(part)`，用固定 64KB `pread` 缓冲全量校验 hash，再原子 rename
  为 `checkpoint.blob` 并 fsync session directory。installer 重新 `open/fstat`，以同样的
  流式校验确认协议 hash、checkpoint layout、generation hash 和各 shard state hash；随后将
  `.blob` 原子 move 为 COLD checkpoint generation 并发布 manifest，不创建完整映射或 blob
  缓冲。UT 覆盖 `.part` 错误清理、`.blob` 内容和 install 后 artifact 路径消失，111/112 UB
  回归覆盖同一 200KB artifact 的真实跨机传输和安装。
- M7 的 Leader AOF retention window 已接线：COLD 配置的 `retention_events` 是目标
  retained AOF event 数 `R`，不是 index 大小；精确 cursor ring 固定为
  `C = ceil_pow2(ceil(1.5 * R))`。生产配置 `R=1048576`，因此 `C=2097152`。
  Leader resync controller 每 10ms 检查一次窗口，在 `used >= R` 时发布新 checkpoint、
  seal 当前 segment 并 compact；active resync pin 阻止删除所需 tail 时，`used >= 85% * C`
  会 abort 该 session、释放 pin 后再次 compact。该控制器只使用已有 COLD I/O 一致性边界，
  不给 normal COLD append、sender 或 WARM apply 添加 mutex。`tlc_replica_cold_ut` 覆盖
  小 `R` 的容量推导与 wrap，`tlc_ha_replica_ut` 覆盖 soft compact 和 pin 压力 abort/compact；
  111/112 UB 的 M7 小窗口专项已加入脚本参数：`TLC_HA_UB_M7_ONLY=1` 可只跑
  `R=8/C=16` 的 soft compact 与 active pin hard-pressure abort。

### M3-M7 状态总表（当前实现基线）

| 阶段 | 当前状态 | 实际内容 |
| --- | --- | --- |
| M3 | 已完成 | `SNAPSHOT_BEGIN/CHUNK/END`、固定网络字节序 codec、连续 offset/chunk/整体 checksum、STREAM 与 UB listener 接入、文件化 artifact assembler。 |
| M4 | 已完成 | Follower 单 active runtime 的 fenced reset、checkpoint `.part -> .blob`、流式校验、COLD import、Core/WARM recovery、artifact release/acquire 交接。 |
| M5 | 已完成 | `SNAPSHOT_INSTALLED`、tail rounds、`RESYNC_ACK`、final emission gate、`HANDOFF_COMMIT/ACK`、pin/gate 释放和 `H + 1` 普通复制。 |
| M6 | 已完成 | 固定 Leader/Follower 角色、自动 GAP 检测、Leader AOF 严格 replay、retention 缺失后自动 snapshot resync、install/tail/handoff 闭环。 |
| M7 | 已完成（数据面） | 固定容量 cursor ring、soft compact、hard pressure abort、retention pin 维护及 111/112 UB 专项回归。 |

上述 M3-M7 不包含 HA control plane、自动 reconnect、failover、角色晋升、owner fencing
和 lineage transition；这些仍是后续独立阶段。心跳检测、退避、动态角色、Leader 通知和
切主恢复设计见 [TLC HA 心跳检测与切主设计](./TLC_HA_FAILOVER_HEARTBEAT_DESIGN.md)。当前待验证项包括失败注入矩阵（import/reset、
AOF append/fsync、WARM apply、ACK/handoff、进程中断）、重复/过期 frame 幂等性、长时间
queue saturation/gate 压测，以及 v2 descriptor ring/payload arena 变更后的全量回归。

## HA 线程模型

下面是当前固定 Leader/Follower 部署的线程和队列关系。箭头表示数据或控制流，
`producer_admission_gate` 只保护 Leader outbound queue 的准入判断和 `queue_tail`
发布，不覆盖 AOF 读取、网络发送或 WARM apply 等长路径。

```text
                              TLC HA THREAD MODEL
================================================================================

  Leader node
  ----------

  Redis/TLC write thread(s)
          |
          v
  +-----------------------+
  | COLD request queue    |  queue_mu + condition variable
  +-----------------------+
          |
          v
  +-----------------------+        +-----------------------+
  | COLD writer thread    |------->| COLD AOF / segments   |
  | assign seq, append    |        +-----------------------+
  +-----------------------+                    |
          | append_sink(seq,event)            v
          v                           +-----------------------+
  +-----------------------+            | COLD flush thread    |
  | replica_event_sink()  |            | group fsync           |
  +-----------------------+            +-----------------------+
          |
          v
  +-------------------------------+
  | producer_admission_gate       |
  | check replay_mode + publish   |
  +-------------------------------+
          |
          v
  +-----------------------+       +-----------------------+
  | HA outbound queue     |<------| sender replay producer|
  | atomic head/tail      |       | bounded AOF replay    |
  +-----------------------+       +-----------------------+
          |
          v
  +-----------------------+
  | Leader sender thread  |
  | batch + send EVENTS   |
  +-----------------------+
          |
          v
  +-----------------------+       UB TX ring / TCP stream
  | Leader TX             |======================================>
  +-----------------------+

  +-----------------------+       <======================================
  | Leader receiver       |       UB RX ring / TCP stream
  | ACK / heartbeat /     |
  | resync control        |
  +-----------------------+

  +-----------------------+       +-----------------------+
  | Heartbeat thread      |------>| heartbeat frames      |
  | health + timeout      |       +-----------------------+
  +-----------------------+

  +-----------------------+
  | Resync controller     |
  | retention, checkpoint |
  | begin_resync          |
  +-----------------------+


  Follower node
  ------------

  UB RX ring / TCP stream
          |
          v
  +-----------------------+
  | Follower listener     |
  | decode/checksum       |
  | EVENTS/control frames |
  +-----------------------+
          |
          +------------------------------+
          |                              |
          v                              v
  +-----------------------+      +-----------------------+
  | COLD replica request  |      | Follower apply queue  |
  | submit batch          |      | ordered event copies  |
  +-----------------------+      +-----------------------+
          |                              |
          v                              v
  +-----------------------+      +-----------------------+
  | COLD request queue    |      | Follower apply thread |
  +-----------------------+      | strict contiguous seq |
          |                      | apply to WARM         |
          v                      +-----------------------+
  +-----------------------+                  |
  | COLD writer thread    |                  v
  | append local AOF      |      +-----------------------+
  +-----------------------+      | WARM / applied_seq    |
          |                      +-----------------------+
          v
  +-----------------------+
  | COLD flush thread     |
  | group fsync           |
  +-----------------------+

  +-----------------------+       =======================================>
  | Follower heartbeat    |------> Leader
  | durable/applied       |
  +-----------------------+

  +-----------------------+
  | Follower resync       |
  | controller            |
  | artifact install,    |
  | tail replay, fencing  |
  +-----------------------+


  Automatic GAP repair
  --------------------

  Follower apply thread
          -> WARM GAP
          -> Follower resync controller sends RESYNC_REQUIRED
          -> Leader receiver accepts request
          -> Leader resync controller schedules AOF replay or snapshot

  During automatic AOF replay:

  COLD append -> replica_event_sink()
                        |
                        +-> replay_mode == true
                        +-> do not allocate/publish outbound event
                        +-> atomically record earliest replay_from_seq

  Leader sender -> read bounded [replay_from_seq, appended_seq] from AOF
                -> publish only up to outbound queue free capacity
                -> repeat until replay_from_seq is empty

  Manual tlc_ha_replica_replay_from() is a test/controlled-recovery entry;
  it must not enumerate an AOF range concurrently with new Leader appends.
================================================================================
```

M5 已完成 `SNAPSHOT_INSTALLED`/`RESYNC_ABORT`、`TAIL_REQUEST/TAIL_END`、catch-up rounds、
final emission gate、`RESYNC_ACK` 和 `HANDOFF_COMMIT`。M6 已完成 Follower 的自动恢复入口：
GAP 先以 Follower `durable_seq + 1` 为起点从 Leader AOF 连续补读；只有该范围不再完整
保留时才转 checkpoint snapshot。内容冲突和 lineage 不匹配不接受 AOF 覆盖，前者走 snapshot，
后者保持拒绝并等待控制面重配。自动 reconnect、故障检测、切主和晋升仍是独立后续工作项。

## 一致性模型

一次 resync session 固定以下不可变身份：

```text
session_id                 随机且单调唯一的本次传输身份
leader_node_id/peer_node_id 固定 Node group 身份
ha_term                    当前 Leader owner term
topology_epoch             checkpoint 所属拓扑上下文
generation                 checkpoint generation
captured_seq[shard]        checkpoint 中每个 shard 的状态边界
checkpoint_seq             min(captured_seq[])
checkpoint_bytes/hash      完整 blob 身份
tail_start                 checkpoint_seq + 1
```

Leader 在已验证的 checkpoint generation 上取一个 durable tail 边界 `B`。快照传输
期间 Leader 可以继续正常写入，`B` 之后的 event 不属于第一轮 tail。Follower 安装
checkpoint 后，Leader 发送完整连续 AOF 范围 `[checkpoint_seq + 1, B]`；Follower
将其 durable append 到当前 active COLD，并根据 `captured_seq[shard]` 只 apply 该 shard
仍缺失的部分。

由于 Leader 可以持续写入，`B` 不是最终切换边界。Follower 通过若干轮
`TAIL_REQUEST(next_seq)` 追赶，最后在一个短暂的 Leader emission gate 内固定 handoff
边界 `H`。gate 阻止新的 Leader event 进入当前副本的普通发送序列，Leader 发送
`[next_seq, H]` 并等待 Follower durable/applied 确认；随后释放 gate，`H + 1` 之后的
新 event 走普通 EVENTS 流。这样不会丢弃或并发混发 resync tail 与实时 event。

第一版允许 final gate 对写入形成短暂背压。它必须记录持续时间和原因；不能为了避免
背压而丢弃 sender queue 中的 event，或在没有顺序屏障的情况下猜测 Follower 已追平。

## 状态机

Leader 和 Follower 各自维护一个 session；任一时刻一个 Node group 只允许一个活动
resync session。

```text
Follower                         Leader
--------                         ------
NORMAL / DEGRADED
    -> REQUESTED  -- RESYNC_REQUEST -->  PREPARING
                                         -> SNAPSHOT_SENDING
RECEIVING_SNAPSHOT <-- SNAPSHOT_BEGIN/CHUNK/END --+
    -> STAGING_IMPORT                              |
    -> TAIL_CATCHUP -- TAIL_REQUEST ------------> TAIL_SENDING
    <- EVENTS tail / TAIL_END -------------------+
    -> FINALIZING <-- HANDOFF_BEGIN ------------- FINAL_HANDOFF
    -- RESYNC_ACK(H) ---------------------------> NORMAL
NORMAL <------- HANDOFF_COMMIT ------------------ NORMAL
```

本地状态含义：

- `REQUESTED`：Follower 已停止普通 apply，等待本 session 的 checkpoint；
- `RECEIVING_SNAPSHOT`：只接受连续且校验通过的 checkpoint chunk；
- `STAGING_IMPORT`：checkpoint 已完整落盘，Follower 正在同进程 reset 后的 empty/fenced
  COLD 中导入；
- `TAIL_CATCHUP`：唯一 active runtime 按顺序 append tail，并根据 captured seq 重建状态；
- `FINALIZING`：Leader 已关闭 session 的实时发送入口，等待到 `H` 的 durable/apply
  ACK；
- `NORMAL`：同一 active runtime 解除 FENCED，普通 EVENTS 从 `H + 1` 开始。

收到错误 session、错误 sender identity、旧 `ha_term`、不匹配 `topology_epoch`、不连续
chunk 或冲突 checksum 时，接收方拒绝该 frame、将 session 置为 `FAILED`。在尚未执行
同进程 reset 前保留原 active generation；reset 一旦开始，旧 COLD/WARM 已按本次 resync
决策清空，后续失败只能保持 `FENCED/UNAVAILABLE`，不能回退到旧 generation。只有显式新
request 或上层 reconnect 策略可以开始新 session。

## Replica 控制帧

在现有 `EVENTS`、`ACK`、`HEARTBEAT` 之外新增以下 `kind`。所有 payload 使用现有
网络字节序编码，完整 Replica frame checksum 继续覆盖 header 和 payload。

| kind | 方向 | 关键字段 | 语义 |
|---|---|---|---|
| `RESYNC_REQUEST` | Follower -> Leader | session id、reason、local durable/applied、expected seq | 请求 snapshot 或恢复失败 |
| `SNAPSHOT_BEGIN` | Leader -> Follower | session id、generation、term、epoch、shard count、checkpoint seq、blob bytes、blob hash、first tail boundary | 宣布不可变 blob 与 session 几何 |
| `SNAPSHOT_CHUNK` | Leader -> Follower | session id、offset、bytes、chunk checksum、data | 连续传输 checkpoint blob |
| `SNAPSHOT_END` | Leader -> Follower | session id、blob bytes、blob hash | 结束传输，触发全量校验和 import |
| `SNAPSHOT_INSTALLED` | Follower -> Leader | session id、generation、checkpoint seq、captured-seq digest | 同进程 reset 后唯一 active COLD 的 checkpoint import 成功 |
| `TAIL_REQUEST` | Follower -> Leader | session id、next seq、target boundary | 请求下一段连续 AOF |
| `TAIL_END` | Leader -> Follower | session id、last seq、durable boundary | 标记本轮 tail 结束 |
| `HANDOFF_ACK` | Follower -> Leader | session id、handoff seq H | Follower 已 commit，Leader 可释放 gate 和 pin |
| `RESYNC_ACK` | Follower -> Leader | session id、durable seq、applied seq | 确认 snapshot/tail/handoff 的连续前缀 |
| `HANDOFF_COMMIT` | Leader -> Follower | session id、H | 确认 H，清理 resync artifact，解除 FENCED，切回普通复制 |
| `RESYNC_ABORT` | 双向 | session id、stage、reason code | 显式结束失败 session |

`SNAPSHOT_CHUNK` 的 payload 上限必须不超过当前 `max_batch_bytes` 与 UB ring frame
容量，且传输端严格按 offset 递增。checkpoint 不能被当成一个大 frame 一次塞入固定
slot 或未来 arena。初版不引入压缩、乱序 chunk、并行 session 或跨 session dedup；这些
都应在正确的单 session 顺序流稳定后再评估。

## Leader 实现

### 选择 checkpoint 与 pin AOF

Leader 新增 COLD 边界 API，由 COLD 自己按既有 `checkpoint_mu`、`io_mu` 和 append
序列管理锁顺序完成：

```c
int tlc_cold_begin_resync_snapshot(
    tlc_cold_t *cold,
    uint32_t expected_meta_shard_count,
    tlc_cold_resync_snapshot_t *snapshot);
void tlc_cold_end_resync_snapshot(
    tlc_cold_t *cold,
    tlc_cold_resync_snapshot_t *snapshot);
```

`snapshot` 包含已验证 checkpoint blob、checkpoint result、per-shard captured seq、
其 digest、固定 durable boundary `B` 和 resync retention token。不要让
`tlc_ha_replica.c` 自己解析 checkpoint 文件取得 captured seq，也不要在外层拼接
checkpoint/IO mutex。

开始 session 时 COLD 记录 `aof_pin_from_seq = checkpoint_seq + 1`。compact 计算
保留边界时必须同时考虑所有 active resync pin 和 retained generation：

```text
effective_aof_retain_from = min(retained_checkpoint_floor + 1,
                                every_active_resync_pin)
```

session 成功 `HANDOFF_COMMIT` 或 `RESYNC_ABORT` 后才释放 pin。Leader 的进度诊断应
记录 generation、checkpoint seq、tail boundary、pin seq、已发送 seq、session state、
chunk bytes 和 final gate 耗时。

### 发送和切换

当前 `replica_send_frame()` 的 mutex 只保证帧写入不交错，不能保证 resync tail 与
sender queue 的 seq ownership。实现时为每个 Follower 增加明确的发送模式和 emission
gate：

```text
NORMAL        append sink 的 event 进入普通 sender queue
RESYNC        sender 消费但不向该 Follower 发送实时 event；历史范围只由 resync sender 发送
FINAL_GATE    禁止新的 event 跨过 handoff；发送最后 [next_seq, H]
NORMAL        从 H + 1 恢复普通 sender queue
```

在 `RESYNC` 期间，实时 event 仍已存在 Leader AOF；实现可以释放该 Follower 的旧队列
副本，但不可让 event sink 因队列满停止 Leader 服务。最终 handoff 前必须以 COLD AOF
为权威重放遗漏范围。若没有能证明 seq 唯一发送权的 emission gate，不能切回 NORMAL。

## Follower artifact 与同进程 reset

Follower 不在正在服务的 COLD 目录上调用 `tlc_cold_import_checkpoint()`。收到
`SNAPSHOT_BEGIN` 后只创建 session 专属临时 artifact；Follower 进入
`RECOVERING/FENCED`，不创建第二套在线 Core/WARM runtime：

```text
active Follower COLD/Core/WARM     standby，停止普通 EVENTS/apply，本次 resync 允许清空
resync artifact                    独立的已校验 checkpoint 文件
同进程 reset 后的唯一 runtime         import checkpoint -> append tail -> replay/apply
```

临时 artifact 不是第二套数据库，只是 checkpoint 传输的组装和校验载体，例如：

```text
<cold-directory>.resync/<session_id>/checkpoint.blob.part
<cold-directory>.resync/<session_id>/checkpoint.blob
```

`checkpoint.blob.part` 只接收 `SNAPSHOT_CHUNK`。每个已验证 chunk 用顺序 offset 写入该
文件；`SNAPSHOT_END` 重新校验总长度/hash 后，必须 `fsync(part)`、原子 rename 为
`checkpoint.blob`，并 fsync artifact directory。只有 `.blob` 可以进入 import；`.part`
在启动或新 request 时直接删除。该版本不写 bootstrap/session 状态标记，也不尝试从半安装
状态恢复。artifact 文件不能被客户端读取，也不能被 COLD recovery 当作 active manifest。

chunk 不能在内存中累计完整 checkpoint；内存只保留当前 chunk 和连续 offset。`SNAPSHOT_END`
与 install 均用固定 64KB `pread` 缓冲做流式 checksum/layout 校验。校验成功后 install
将 `.blob` rename 到 COLD 的 generation 路径并发布 manifest，因此不会创建完整 `mmap` 或
完整 blob heap buffer；artifact 与 checkpoint generation 位于同一 COLD 文件系统是该原子
move 的前置条件，installer 在 FENCED/reset 前以 `st_dev` 强制校验该条件。worker 随后停止
普通 EVENTS/apply，并在同一进程内关闭 COLD writer、清空 COLD 文件和 Core/WARM 状态，再
重新打开空的 fenced COLD。该过程不切换两套 runtime，也不要求重启进程。

重启时先只恢复完整 active COLD checkpoint+AOF；若 COLD recovery 失败，则 Follower 保持
`FENCED/UNAVAILABLE` 并重新请求 snapshot。对于已完整接收但尚未 import 的 `.blob`，启动时
可重新校验后使用它完成 import，或直接删除并重新请求 snapshot，两者都不依赖 bootstrap
marker。完成本地 COLD recovery 后，Follower 必须与 Leader 比较 identity/term 和连续前缀：
本地 `durable_seq < leader_durable_seq` 本身不是过期，Leader 仍保留
`[local_durable_seq + 1, leader_durable_seq]` 时走普通增量 replay；只有该起点早于 Leader
retention floor、term/lineage 不匹配或 replay 检出 GAP，才视为整体进度过期并触发新的
snapshot resync。

**性能约束**：不得为正常 Follower 的每个 EVENTS batch、COLD append 或 WARM apply
获取 resync/gate mutex，也不得用 resync 安装锁串行化 ingress 与异步 apply。正常路径只做
两次 `fenced` 原子读和一次对应的 in-flight 原子加/减：首次读通过后增加 ingress 或 apply
计数，再次读确认未进入 FENCED 后才访问 COLD/Core。安装路径先以 release 语义置
`fenced=true`，仅在这一低频状态转换中等待两个计数归零并丢弃未 apply 的旧队列 event；
第二次读到 FENCED 的并发 worker 只能撤销计数，不能再触碰 COLD/Core。心跳和 resync
控制帧不走该 gate。

**锁预算（强制）**：新增 resync 代码必须优先使用单写者状态机、原子状态位、原子
in-flight 计数和有界无锁 ring；不得为了方便给 event、COLD 或 WARM 热路径添加 mutex。
完成 checkpoint artifact 的 listener -> orchestrator 交接为单生产者/单消费者：生产者在
完整校验后以 release-CAS 发布一个不可变 artifact，消费者以 acquire-exchange 取得所有权，
不使用 mutex。唯一允许的 `stream_write_mutex` 仅用于 STREAM transport 的多个发送者写同一
字节流时防止 header/payload 交错；它不保护 COLD/Core，不得用于 UB ring，也不得成为
FENCED 的 gate。任何新 mutex 必须在设计和代码注释中说明资源、竞争方、热/冷路径归属及
为何原子协议不足；没有这个证明不得引入。

**性能验收**：代码审查必须确认 Follower 普通 `EVENTS -> COLD append -> WARM apply`
路径不调用 `pthread_mutex_lock`；UT 必须在 artifact 已发布而普通复制仍进行时完成一次原子
领取/install；111/112 UB 回归必须继续通过，以确认 resync control traffic 不退化普通
无锁 ring 的发送与接收。

导入成功后，Follower 将 tail 以全局连续 seq 写入唯一 active COLD。Core 从 checkpoint
记录取得 `captured_seq[shard]`，只将每个 shard 缺失的 tail 应用到 WARM；不得因某个
shard 已包含较晚 snapshot state 而把旧 event 覆盖回去。每一轮 tail 先完成 COLD
durable append，再推进 applied progress 并发出 `RESYNC_ACK`。

收到 `HANDOFF_COMMIT` 后，Follower 在当前 active runtime 的 durable/applied 都达到
`H` 时清理临时 artifact、解除 FENCED、恢复普通复制 listener。失败、断电或
`RESYNC_ABORT` 时删除 artifact；由于旧 COLD 已按 resync 决策清空，Follower 保持
`FENCED/UNAVAILABLE`，重新发起新的 request。不允许半安装的 checkpoint 被标记
为正常副本或用于对外服务。

## 进度、ACK 与诊断

普通 `peer_accepted_seq`、`peer_durable_seq`、`peer_applied_seq` 只在有效普通复制或
`RESYNC_ACK` 已证实连续前缀时推进。`SNAPSHOT_INSTALLED` 只说明 checkpoint 已在
reset 后的 active COLD 落盘，不能冒充 `applied_seq`；`HANDOFF_COMMIT` 前不将 checkpoint/tail 的临时结果暴露为
对外正常 Follower state。

增加 `tlc_ha_replica_resync_progress_t` 或扩展现有 progress 诊断，至少输出：

```text
resync_state / session_id / reason
checkpoint_generation / checkpoint_seq / captured_seq_digest
snapshot_bytes_total / snapshot_bytes_received
tail_next_seq / tail_target_seq / handoff_seq
aof_pin_from_seq / final_gate_elapsed_ms
last_error_stage / last_error_code
```

耗时使用 `monotonic.h` 的 `elapsedMs`/`elapsedUs`，不重新定义 clock helper。日志按
状态转换和周期采样输出，避免为每个 chunk 创建高频格式化日志。

## 落地前置条件

本功能不是在现有 `tlc_core_resync_from()` 外面包一层网络发送即可完成。开始跨节点
开发前必须先解决以下三个边界，否则后续状态机无法证明正确：

1. **fuzzy checkpoint replay 边界**：COLD AOF 必须保存全局连续的
   `[checkpoint_seq + 1, B]`，但 Core/WARM 只能对每个 shard 的
   `seq > captured_seq[shard]` 的 event 执行状态 apply。当前
   `tlc_core_resync_from()` 直接按全局范围 apply，必须改为 resync 专用 apply/replay
   路径，并单独维护全局 durable/apply 连续前缀。
2. **普通复制不能被队列满阻塞**：当前 Replica append sink 可能同步等待 sender queue
   空间。正常模式必须允许丢弃队列副本并从 Leader AOF 重建发送范围；只有 final gate
   才允许有上限、可观测的短暂背压。
3. **Follower 安装边界**：`tlc_cold_import_checkpoint()` 只接受空的 fenced COLD。
   第一版不实现两个在线 COLD/Core/WARM runtime 的进程内切换；Follower 在 resync
   期间进入 `FENCED/UNAVAILABLE`，checkpoint 先落到 session 临时文件，校验完成后
   允许清空本地旧 COLD/WARM，并在同一进程内 reset 后将其作为唯一 active COLD 启动。

上述前置条件完成后，所有阶段都必须保留现有 EVENTS/ACK/HEARTBEAT 回归；任何阶段
失败都不得发布半完成 checkpoint，Follower 保持 `FENCED/UNAVAILABLE`，重新发起 resync。

## 分阶段实施计划

### P0：冻结协议和错误语义

**代码落点**：`src/tlc_ha_replica.h`、`src/tlc_ha_replica.c`、`src/tlc_cold.h`、
`src/tlc_core.h`。

冻结以下内容：

- session 粒度为一个 Node group 的一次完整 resync；同组同时只能有一个 active session；
- frame header、网络字节序、checksum 覆盖范围、payload 上限和版本号；
- session identity：node、peer、`ha_term`、`topology_epoch`、generation、blob hash；
- 状态转换、错误 stage/reason、`FAILED` 后只能由新 request 建立新 session；
- `durable_seq`、`applied_seq`、`peer_*_seq` 的推进条件；
- `CHECKPOINTED`（事件已由 checkpoint 覆盖、跳过 WARM apply）与
  `APPLIED/DUPLICATE/GAP/ERROR` 的区别。

**验收**：增加纯编解码/错误码 UT；旧 EVENTS/ACK/HEARTBEAT frame 的行为和字节布局
不变。

### M1：COLD snapshot session、AOF reader 和 retention pin

**代码落点**：`src/tlc_cold.[ch]`、`src/tlc_core.[ch]`。

新增由 COLD 管理锁顺序的边界 API：

```c
int tlc_cold_begin_resync_snapshot(
    tlc_cold_t *cold,
    uint32_t expected_meta_shard_count,
    tlc_cold_resync_snapshot_t *snapshot);
void tlc_cold_end_resync_snapshot(
    tlc_cold_t *cold,
    const tlc_cold_resync_snapshot_t *snapshot);
```

`snapshot` 至少包含已验证 blob、长度/hash、generation、term/epoch、
`captured_seq[]` digest、`checkpoint_seq`、固定 Leader durable 边界 `B` 和 pin token。
同时新增只读、有界的连续 AOF batch reader；起点已被 compact 时返回明确的
resync-required 错误。

compact 必须将 active pin 纳入保留边界，session 成功 commit 或 abort 后才释放 pin。
补充并发 checkpoint/export/compact、pin 存在时回收受阻、释放后可回收的 UT。

**出口条件**：在没有 Replica channel 的情况下，能够稳定导出一个 snapshot session，
并从 reader 读取 `[checkpoint_seq + 1, B]` 的完整有序 event。

**当前实现**：出口条件已由 `tlc_resync_ut` 覆盖。测试在 session 创建后继续写入，确认
reader 不越过固定 `B`；随后发布更新 checkpoint 并 compact，确认 pin 存活时旧 tail
不能被回收，而 session 结束后相同 compact 可以回收 sealed AOF 段。

### M2：修复 fuzzy replay 和 resync Core 最小能力

**代码落点**：`src/tlc_core.c`、`src/tlc_core.h`、`benchmark/tlc_resync_ut.c`。

实现 resync 专用 replay：

- COLD 按全局 seq 连续 append 和 durable；
- Core 根据 event 的 `meta_shard_id` 查 `captured_seq[shard]`；
- `seq <= captured_seq[shard]` 时跳过状态 apply，但仍推进全局连续进度；
- `seq > captured_seq[shard]` 时才 apply 到 WARM；
- 任何 GAP、冲突、term/epoch 不匹配都停止该 session。

不要复用要求 `seq == replica_applied_seq + 1` 且不理解 fuzzy 边界的普通在线 apply
路径。新增测试覆盖不同 shard 的 captured seq 交错、旧 event 不覆盖新 checkpoint
状态、tail 缺失和 apply 失败。

### M3：Replica resync frame 和 chunk assembler

**代码落点**：`src/tlc_ha_replica.[ch]`、`benchmark/tlc_ha_replica_ut.c`。

本阶段只实现 checkpoint artifact 的三个 data/control frame，并使用独立 payload 结构：

- `SNAPSHOT_BEGIN/CHUNK/END`：有序 offset、分块 checksum、总长度/hash；
- 既有 `EVENTS/ACK/HEARTBEAT` frame 的 header/layout 不变；
- 剩余 `RESYNC_REQUEST`、`SNAPSHOT_INSTALLED`、`RESYNC_ABORT` 属于 M4 的安装边界，
  `TAIL_REQUEST/TAIL_END`、`RESYNC_ACK/HANDOFF_COMMIT/HANDOFF_ACK` 属于 M5。

frame ingress 在最高外部边界一次性校验长度、kind、session、sender identity、term、
epoch 和 checksum；内部 handler 只处理已满足前置条件的 frame。

**出口条件（已完成）**：大于单个 UB slot 的 checkpoint 可以拆成多 chunk；重复/跳跃
offset、块 checksum、整体 hash、身份和 session 不匹配全部拒绝，且不改变 active
generation。STREAM UT 覆盖 artifact 后普通 EVENTS 继续复制；111/112 UB 回归覆盖 200KB
artifact 的 32KB 分块传输。

### M4：Follower 单 active 安装和同进程 reset

**代码落点**：`src/tlc_ha_replica.c`、`src/tlc_core.c`、
`src/vemb_v16_storage.[ch]`。

第一版不创建第二套在线 Core/WARM，也不在进程内切换 `storage->tlc`、COLD 或 WARM
provider。Follower 的生命周期改为：

```text
active COLD/Core/WARM       standby，接收普通复制
FENCED/UNAVAILABLE           停止普通 EVENTS apply，不参与服务/晋升
resync artifact              独立临时 checkpoint 文件
in-process reset              清空并重新初始化唯一 active COLD/Core/WARM
```

收到 `SNAPSHOT_BEGIN` 后，Follower 只在 session 临时目录创建 `checkpoint.blob.part`，并将
连续 offset 的已校验 chunk 写入文件；`SNAPSHOT_END` 校验整体 hash 后 fsync 并原子 rename
为 `checkpoint.blob`。此阶段不修改 active COLD。这里的临时 artifact 不是第二套数据库，
只是 checkpoint 传输的组装和校验载体，例如：

```text
<cold-directory>.resync/<session_id>/checkpoint.blob.part
<cold-directory>.resync/<session_id>/checkpoint.blob
```

`.part` 在进程重启或新 session 时直接清理；本阶段不持久化 bootstrap/session marker，也不
恢复半安装 COLD。`.blob` 只可作为重新校验后 import 的输入，不能被客户端读取，也不能被
COLD recovery 当作 active manifest。

`SNAPSHOT_END` 校验并完成 `.blob` rename 后，停止普通 EVENTS/apply；在同一进程内关闭
COLD writer、清空旧 COLD 文件、清理 Core metadata/WARM allocation，然后重新打开空的
fenced COLD，调用 `tlc_cold_import_checkpoint()`，再接收和 durable append AOF tail。
Replica channel 和 session 保持不变，不需要重启进程。

当前已实现上述过程的 Core 安装边界
`tlc_core_install_resync_checkpoint_file()`，以及 Replica 的
`tlc_ha_replica_install_resync_snapshot()` FENCED/quiesce 协调；file install 只接收独占的
完整验证 artifact fd/path，调用方必须已经停止 Follower writes、Replica ingress/apply 和
COLD lifecycle users。listener 的 `SNAPSHOT_END` handler 只发布 artifact，不能直接调用
Core install。

本次 resync 明确允许丢弃旧 generation，不需要保留 backup，也不实现半安装状态恢复。
进程异常退出时，启动流程丢弃 `.part`；本地 active COLD 的 checkpoint+AOF 能完整恢复时
先走增量追赶，不能恢复或增量起点已不被 Leader 保留时重新发起 snapshot request。

每轮 tail 仍必须先完成 active COLD durable append，再由同一个 active Core apply 并
发送 `RESYNC_ACK`。`HANDOFF_COMMIT` 不再表示 runtime 指针切换，只表示 H 已确认、
清理 resync artifact、释放 pin 并解除 FENCED，恢复普通复制 listener。

失败、断电或 abort 时删除 `.part`；已完成但未 import 的 `.blob` 可重新校验后复用或删除。
由于旧 COLD 已按 resync 决策清空，Follower 保持 `FENCED/UNAVAILABLE`，重新发起新的
request。不得让半安装 checkpoint 被标记为正常副本或用于对外服务。增加 import 失败、
进程异常中断、`.part` 清理、`.blob` 重校验、tail append 失败和 WARM recovery 失败测试。

**出口条件**：进程内始终只有一套 active runtime；成功 reset 后 active 的逻辑状态和
全局 durable/applied prefix 一致，失败时保持 FENCED 并可重新发起 resync。

### M5：Leader orchestrator、catch-up rounds 和 final handoff

**代码落点**：`src/tlc_ha_replica.c`，必要时扩展 `src/tlc_cold.[ch]`。

为每个 Follower 增加 session object 和发送模式：

```text
NORMAL -> RESYNC -> FINAL_GATE -> NORMAL
```

实现顺序：

1. 收到受控 resync request，调用 COLD begin API，固定 `B` 并建立 pin；
2. 发送 begin/chunk/end，等待 `SNAPSHOT_INSTALLED`；
3. 按 `next_seq` 发送多轮 `[next_seq, target]`，每轮等待 durable/apply ACK；
4. 进入 `FINAL_GATE`，禁止该 Follower 的普通 EVENTS 跨过最终边界，固定 `H`；
5. 发送 `[next_seq, H]`，确认 `durable_seq == H && applied_seq == H`；
6. 发送 `HANDOFF_COMMIT`，收到确认后释放 gate 和 pin；
7. 恢复普通 sender，第一条普通 EVENTS 必须是 `H + 1`。

`RESYNC` 期间实时 event 仍以 Leader AOF 为权威，不得因队列满停止 Leader 服务；
`FINAL_GATE` 的写入背压必须记录原因和 monotonic 耗时。

**出口条件**：高频写入下无 seq gap、无 tail/ordinary event 交叉 ownership，成功、
abort、超时都能释放 session 资源。

**当前实现（已完成）**：`tlc_ha_replica_begin_resync()` 是唯一的受控 Leader 入口。
它建立 COLD retention pin、发送 `RESYNC_REQUEST` 和 checkpoint artifact；Follower import
后发送 `SNAPSHOT_INSTALLED` 与首个 `TAIL_REQUEST`。每轮 `TAIL_END` 后 Follower 先发送
durable/applied `RESYNC_ACK`，再预取下一轮起点。Leader 可以扩展 pin 的 boundary，最终以
原子 emission gate 固定 `H`；gate 期间只记录最早 `replay_from_seq`，不阻塞 COLD/WARM
写入。Follower 收到 `HANDOFF_COMMIT` 后解除 FENCED 并回送 `HANDOFF_ACK`；Leader 只有在
收到该 ACK 后才释放 pin/gate、清空旧 sender queue。final gate 期间有实际 COLD append 时，
从记录的最早 `seq > H` 补读；若无新写，下一条普通 event 直接进入 sender queue，仍然是
`H + 1`。
普通 `EVENTS -> COLD append -> WARM apply` 热路径没有新增 mutex。

`benchmark/tlc_ha_replica_ut` 已覆盖 STREAM 的 checkpoint 后 tail、`B` 扩展为 `H`、
`HANDOFF_ACK`、`H + 1` 普通复制、inactivity timeout 后重新建 session，以及显式 abort。
超时默认 30 秒，只在 heartbeat 冷路径检查；抢到 abort 所有权的 Leader 释放 pin 但保持
emission gate，Follower 删除 artifact 并保持 FENCED。111/112 UB 已通过成功 handoff（`H=3`
后 seq `4` 同时达到 durable 和 applied），并通过 timeout/abort cleanup 跨机用例。
server 管理入口属于 M6，不阻塞 M5 的代码实现完成。

### M6：固定角色与自动 resync 触发

**代码落点**：`src/vemb_v16_server_integration.c`、`src/vemb_v16_storage.[ch]`、
`src/tlc_ha_replica.[ch]`。

当前版本只有两个部署时固定的副本：一个 `Leader` 和一个 `Follower`。角色、对端
node id、`ha_term` 与 `topology_epoch` 在启动配置中一次确定；server 使用
`HPC_REDIS_HA_ROLE=leader|follower` 启动对应 runtime。没有运行期角色切换，也不需要
外部 `TLC.HA` 工具干预。

TODO(HA control plane)：在具备持久 term、角色切换 fencing、peer 重连和晋升约束后，再
增加 `TLC.HA` 管理命令；它不能成为当前 resync 的前置条件。完整控制面方案见
[TLC HA 心跳检测与切主设计](./TLC_HA_FAILOVER_HEARTBEAT_DESIGN.md)。

TODO(lineage transition)：切主导致 `ha_term` 变化、扩缩容或迁移导致
`topology_epoch` 变化后，control plane 必须证明新 Leader 包含 Follower 的 durable
prefix，才能从 `durable_seq + 1` 增量追赶；证明失败或该 AOF 起点已回收时才选择
snapshot。若 shard 映射或 event 语义不兼容，必须进入显式迁移协议，不能将两条历史直接
拼接。当前固定角色版本对 term/epoch 不匹配一律拒绝自动 resync。

自动触发链路为：

```text
Follower normal EVENTS 检出 COLD GAP
    -> 记录 first_seq、expected_seq、durable_seq，发送 RESYNC_REQUIRED(GAP, durable_seq, applied_seq)
    -> Leader 校验固定 peer + ha_term + topology_epoch，并以 [durable_seq + 1, leader_appended]
       调用严格连续 AOF replay
    -> Follower 从缺失起点重新连续 durable append 后解除 GAP recovery 状态

Follower normal EVENTS 检出同 lineage 内容 CONFLICT，或 WARM apply GAP / STALE
    -> Follower FENCED，发送 RESYNC_REQUIRED(CONFLICT, durable_seq, applied_seq)
    -> Leader controller 自动调用 begin_resync()
    -> snapshot -> install -> tail -> HANDOFF_ACK -> H + 1 normal EVENTS

Leader sender 发现 replay_from_seq 对应 AOF 区间不再完整保留
    -> TLC_COLD_RESYNC_REQUIRED
    -> gate 普通 emission，Leader controller 自动调用 begin_resync()
```

**当前验收状态（2026-09-04）**：本地 `tlc_ha_replica_ut` 已覆盖并通过两条自动路径：
保留 AOF 的 GAP 从 `durable_seq + 1` 补齐，及 compact 使前缀缺失后自动选择 checkpoint
snapshot、install、tail 和 `H=98` handoff。111/112 UB 的 10k 压测已通过，Leader/Follower
均达到 `10000`，queue 饱和只触发区间级 AOF 补读。111/112 UB 的 M6 GAP 用例使用明确的
111 `tx=134217728/rx=201326592`、112 反向参数复测：Leader 收到
`RESYNC_REQUIRED(GAP)`、完成 AOF `[1,2]` replay，并确认 `accepted=3`。跨机自动
**retention -> snapshot** 专项也已通过：Leader 在 checkpoint `96` 后 compact AOF 前缀，
Follower 收到孤立的 `seq=98` 后请求 GAP recovery；Leader 的严格 AOF `[1,98]` 在
`expected_seq=91` 返回 retention missing，自动导出 checkpoint `96`，发送 tail `97,98`，
最终双方完成 `H=98` handoff（Follower `durable=98`、Leader `accepted=98`）。该用例证明
snapshot 只在严格 AOF replay 无法满足起点时使用，不作为普通 GAP 的兜底。

`tlc_cold_replay_range()` 必须验证完整的 `[start_seq, end_seq]` 连续区间；缺失首条或
中间 seq 一律返回 `TLC_COLD_RESYNC_REQUIRED`，不能把“没有重放任何记录”当作成功。

真正的 lineage 不匹配不是自动 resync 原因：不匹配的 `peer_id`、`ha_term` 或
`topology_epoch` 控制请求被拒绝，Follower 保持 FENCED，必须由后续 control plane 重配
角色/term 后才能建新 session。它不允许从该 peer 自动 import snapshot。

当前实现的低频 controller 使用原子 pending bit 和短轮询；正常 EVENTS、COLD append、
WARM apply 不增加 mutex。sender queue 饱和每个待补读区间只记录一次最早 `replay_from_seq`；
GAP 日志必须记录 Follower durable/applied、缺失起点和 Leader AOF repair 的 `[start,end]`；
AOF replay 必须记录 start/complete/retention-missing。snapshot
日志必须记录选择原因、session、checkpoint、chunk、tail、H、pin、gate 耗时和最后错误字段；
状态转换按采样频率记录。

Leader COLD 为 retained AOF 维护固定容量、2 的幂的精确
`seq -> (segment_id, segment_offset)` cursor ring。正常 GAP replay 以
`slot = seq & (capacity - 1)` O(1) 取得 cursor，并从该 offset 直接严格重放；slot 的
`seq` tag 不匹配时不能读取旧位置。配置值 `R=retention_events` 是目标 retained AOF
window，ring 容量固定为 `C = ceil_pow2(ceil(1.5 * R))`；生产默认 `R=1048576`、
`C=2097152`。令 `used = appended_seq - retained_floor_seq + 1`：Leader 的低频
resync controller 在 `used >= R` 时发布下一 generation checkpoint、seal 当前非空
segment、并以 checkpoint seq compact；同一 retained floor 只做一次软阈值 checkpoint，
避免每个 event 重复做 I/O。compact 只能删除 sealed segment，且仍受 active resync pin
保护，因此 pin 存活时 soft compact 可以成功但 floor 不前移。

若 `used >= floor(0.85 * C)` 且存在 active session，controller 必须记录
`floor/appended/used/R/C`、abort session 释放 pin，并立即按已发布 checkpoint 再 compact。
硬阈值不能受 checkpoint/compact 失败的重试退避限制；普通失败重试可以退避。正常 append、
sender、WARM apply 不读取 controller 状态、不获取 retention mutex；controller 是唯一写入
它的低频状态机，只通过已有 `io_mu` 获取 COLD 文件窗口快照。索引未命中时当前实现保守回退到
完整严格扫描，以兼容自动 retention 启用前留下的历史 AOF；扫描发现起点不可得才转 snapshot。
该 fallback 不在正常、已对齐 compact window 的 replay 路径上。

Leader sender queue 保持单消费者（sender thread）所有权；正常 COLD append 和 AOF replay
是两个可能的 producer，不能并发发布 `queue_tail`。两者必须经过短时
`producer_admission_gate`：正常 append 在 gate 内完成“检查 `replay_mode`/发布 queue”决策，
sender 在 gate 内切换 `replay_mode`；AOF replay 的文件读取和批量处理都在 gate 外执行。补读
期间新 append 只原子更新最早 `replay_from_seq`，不再分配或填充 outbound queue。handoff 后
control thread 只发布 `normal_min_seq = H + 1`，由 sender 自己丢弃已被 tail 覆盖的旧 queue
event；abort 只设置原子 `sender_discard_pending`，由 sender 在 gate 内自行清理。control
thread 不得直接 pop 无锁 sender queue。`tlc_ha_replica_replay_from()` 是测试/受控恢复接口，
调用方不得在其枚举 AOF 范围期间并发追加 Leader event；它不属于正常自动 GAP repair
生产路径。

**启动恢复时序约束**：`tlc_ha_replica_replay_from()` 只用于已完成本地恢复后的测试或
受控 replay，不代表生产启动状态机。Follower 启动时必须先打开本地 COLD，完整恢复
checkpoint/AOF 到 WARM；恢复失败则保持 `FENCED/UNAVAILABLE` 并重新请求 snapshot，
不得开始普通增量同步或对外提供服务。只有本地恢复完成后，Follower 才能与 Leader 比较
连续 seq，选择 AOF 增量或 snapshot resync；达到连续同步边界 `H` 后才解除 FENCED、进入
`NORMAL`。因此，测试中手工调用 `tlc_ha_replica_replay_from()` 不得被解释为允许生产路径
与 COLD append 并发调用该接口。

server 接线必须保证：

- Follower 在 artifact 接收和同进程 reset 期间是 `RECOVERING/FENCED/UNAVAILABLE`；
- reset/import 完成并达到 H 前不将 Follower 标记为 `NORMAL` 或允许其晋升；
- shutdown、重复 request、peer disconnect 都能清理 `.part` 或已废弃的 artifact 文件；
- 同一进程不同时创建两套 Core/WARM runtime；
- `redis-server` 正常启动时没有 HA 配置则不改变现有行为。

真实机 TCP 闭环已于 2026-09-04 通过：`sdk_ha_replica_tcp` 从 Leader TCP 写入 `10004`
events，经 UB Replica 后在 Follower TCP 校验成功；随后 Follower 使用同一 COLD 目录重启，
`VERIFY PASS events=10004`。这验证固定角色部署的 Redis TCP -> Leader -> Follower 数据路径，
不代表 lineage transition 或 HA control plane 已实现。

### M7：真实 STREAM、UB 和回归闭环

验证顺序固定为：

1. `benchmark/tlc_ha_replica_ut`：frame/session/chunk/state-machine；
2. `benchmark/tlc_resync_ut`：fuzzy replay、pin、compact、失败恢复；
3. `benchmark/tlc_ha_replica_process_ut`：跨进程 STREAM，覆盖中断和重新 request；
4. 111/112 STREAM 或 TCP 控制面：先验证大 checkpoint 多 chunk；
5. 111/112 UB Replica：验证 slot 上限、方向 offset、真实 COLD/WARM reset 后状态；
6. Redis TCP/SDK 端到端：验证 resync 前状态、tail 更新、tombstone 和 `H + 1` 后新写入。

每次真实机回归记录 generation、`checkpoint_seq`、`B`、`H`、chunk bytes、pin、各阶段
耗时以及 Leader/Follower durable/applied progress。

**当前验收状态（2026-09-04）**：`benchmark/tlc_ha_replica_ub_111_to_112.sh`
已完成 111/112 全量 UB 回归，覆盖双向 visibility、10k 普通复制、sender queue 饱和后的
AOF 补读、snapshot chunks、checkpoint install、M5 handoff + `H + 1` applied、M5
timeout/abort cleanup、M6 GAP -> AOF repair、M6 retention -> snapshot、M7 soft compact、
M7 hard pressure abort、Follower 持久 COLD recovery，以及重启后手工 AOF replay。最后
restart-replay 阶段需要从持久 COLD 将 10k event 重新 apply 到 WARM，实测 60s 接近边界
（约 applied=9833/10000 时超时），因此脚本单独提供
`TLC_HA_RESTART_REPLAY_TIMEOUT_SECONDS`，默认 120s；其他阶段仍使用
`TLC_HA_TEST_TIMEOUT_SECONDS` 默认 60s。

自动 reconnect、failover、owner fencing 和角色提升仍属于后续阶段，不能由本功能回归
隐含宣称完成。v2 descriptor ring/payload arena 也是独立优化；resync frame 不依赖其完成，
但 v2 落地后必须重新执行本节的跨机回归。

`producer_admission_gate` 修改后的 Redis TCP/SDK 端到端回归已于 2026-09-04 重新通过：
`benchmark/tlc_ha_redis_tcp_111_to_112.sh` 完成 Leader -> Replica UB -> Follower TCP
写入 `10004` events，随后重启 Follower 并从同一 COLD 目录恢复，SDK 校验输出
`VERIFY PASS events=10004`。该回归确认 gate 没有改变普通 TCP 写入、复制、WARM apply 或
重启恢复语义；远端构建中的宏重定义和 huge-TLB fallback 仅为环境警告，不影响测试结果。

## 验收矩阵

### 正确性不变量

- `Follower durable_seq` 始终是无 gap 的连续 AOF 前缀；
- `Follower applied_seq` 不超过 durable 前缀，且不超过已确认的 session 边界；
- fuzzy checkpoint 下，每个 shard 只 apply `seq > captured_seq[shard]` 的 event；
- 成功 handoff 后：`Leader durable prefix = Follower durable prefix = H`，普通流首个
  seq 为 `H + 1`；
- 任意失败路径不发布半完成 checkpoint；reset 失败时保持 `FENCED/UNAVAILABLE`，可重新
  发起 resync。

### 故障和边界用例

- compact 后起点不可得时转 snapshot，pin 存在时不得删除所需 tail；
- `used == R` 自动 publish/seal/compact 后 AOF floor 前移，且 ring 容量为
  `ceil_pow2(ceil(1.5 * R))`；pin 阻塞 soft compact 后，在 `used >= floor(0.85 * C)`
  自动 abort session、释放 pin 并推进 floor；
- 大 checkpoint 多 chunk、最后不足一 chunk、offset 重复/跳跃、块或整体 checksum 错误；
- session、peer identity、term、epoch、generation 不匹配；
- import/reset、AOF append、per-shard apply、ACK、handoff commit 失败；
- Leader 高频写入和 final gate 背压；
- Follower 在接收 snapshot、tail、final gate、commit 前后断电或重启；
- 重复 request、旧 session frame、重复 tail、重复 commit 和 abort 幂等性。

### 真实 111/112 最小流程

```text
正常写入并确认普通复制
    -> 让 Follower 进入受控 REQUESTED/FENCED
    -> Leader 持续写入制造 B 之后的 tail
    -> 显式 start_resync，接收并校验 checkpoint artifact
    -> 停止普通 EVENTS/apply、清空 COLD/WARM 并同进程 reset
    -> import checkpoint，接收 tail，完成 final handoff
    -> 校验 checkpoint 前数据、tail 更新、更新值、tombstone 和 H + 1 新 event
    -> 再次校验 durable prefix 和逻辑状态
```

通过上述测试只能证明跨节点 checkpoint + AOF tail resync。自动 reconnect、failover、
fencing 和角色提升必须使用独立故障模型与回归。

## M8/M9 当前状态

M8 heartbeat 故障检测和已完成的 M9 动态角色/term 基础能力、线程模型、owner/term
metadata 持久化，以及两个关键问题（Leader apply 与 sender 队列所有权冲突、已定位修复的
post-handoff resync gate/replay 问题）统一记录在
`docs/TLC_HA_FAILOVER_HEARTBEAT_DESIGN.md` 的“当前落地状态”章节。本文件中的
M10 `LEADER_ANNOUNCE`/`ROLE_ACK`、connection epoch 和常驻线程 reconnect 基础也已接入；
自动 failover、严格 UB 控制帧 reservation 优先级、reconnect 后 replay/snapshot 闭环和
角色提升的跨进程恢复验收仍未完成。M11 第一项 heartbeat failure 驱动的
`CANDIDATE -> MASTER` controller 状态机已实现并由单测覆盖；新 Leader reconnect 后
announce、旧 Leader 高 term 降级和 STREAM `SIGPIPE` 防护也已加入测试。replay/snapshot
跨进程收敛仍未完成。
