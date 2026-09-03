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
将其 durable append 到 staging COLD，并根据 `captured_seq[shard]` 只 apply 该 shard
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
- `STAGING_IMPORT`：checkpoint 已完整落盘并在独立 empty/fenced runtime 中导入；
- `TAIL_CATCHUP`：staging runtime 按顺序 append tail，并根据 captured seq 重建状态；
- `FINALIZING`：Leader 已关闭 session 的实时发送入口，等待到 `H` 的 durable/apply
  ACK；
- `NORMAL`：新的 active Follower runtime 已发布，普通 EVENTS 从 `H + 1` 开始。

收到错误 session、错误 sender identity、旧 `ha_term`、不匹配 `topology_epoch`、不连续
chunk 或冲突 checksum 时，接收方拒绝该 frame、将 session 置为 `FAILED`，并保留原 active
generation。只有显式新 request 或上层 reconnect 策略可以开始新 session。

## Replica 控制帧

在现有 `EVENTS`、`ACK`、`HEARTBEAT` 之外新增以下 `kind`。所有 payload 使用现有
网络字节序编码，完整 Replica frame checksum 继续覆盖 header 和 payload。

| kind | 方向 | 关键字段 | 语义 |
|---|---|---|---|
| `RESYNC_REQUEST` | Follower -> Leader | session id、reason、local durable/applied、expected seq | 请求 snapshot 或恢复失败 |
| `SNAPSHOT_BEGIN` | Leader -> Follower | session id、generation、term、epoch、shard count、checkpoint seq、blob bytes、blob hash、first tail boundary | 宣布不可变 blob 与 session 几何 |
| `SNAPSHOT_CHUNK` | Leader -> Follower | session id、offset、bytes、chunk checksum、data | 连续传输 checkpoint blob |
| `SNAPSHOT_END` | Leader -> Follower | session id、blob bytes、blob hash | 结束传输，触发全量校验和 import |
| `SNAPSHOT_INSTALLED` | Follower -> Leader | session id、generation、checkpoint seq、captured-seq digest | staging import 成功 |
| `TAIL_REQUEST` | Follower -> Leader | session id、next seq、target boundary | 请求下一段连续 AOF |
| `TAIL_END` | Leader -> Follower | session id、last seq、durable boundary | 标记本轮 tail 结束 |
| `HANDOFF_BEGIN` | Leader -> Follower | session id、handoff seq H | final gate 已建立 |
| `RESYNC_ACK` | Follower -> Leader | session id、durable seq、applied seq | 确认 snapshot/tail/handoff 的连续前缀 |
| `HANDOFF_COMMIT` | Leader -> Follower | session id、H | 发布 staging runtime，切回普通复制 |
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
    const tlc_cold_resync_snapshot_t *snapshot);
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

## Follower staging 与发布

Follower 不在正在服务的 COLD 目录上调用 `tlc_cold_import_checkpoint()`。收到
`SNAPSHOT_BEGIN` 后创建 session 专属 staging directory、empty/fenced COLD runtime 和
对应的 fenced core/WARM runtime：

```text
active Follower COLD/Core/WARM     保持可回退，不接收本 session 的新数据
staging Follower COLD/Core/WARM    import checkpoint -> append tail -> replay/apply
```

chunk 可写入 staging blob 文件或有上限的内存 buffer；两种实现都必须在
`SNAPSHOT_END` 前验证连续 offset、总字节数和每块 checksum，在结束时验证完整 blob
hash。验证通过后才调用 `tlc_cold_import_checkpoint()`。

导入成功后，Follower 将 tail 以全局连续 seq 写入 staging AOF。staging core 从 checkpoint
记录取得 `captured_seq[shard]`，只将每个 shard 缺失的 tail 应用到 WARM；不得因某个
shard 已包含较晚 snapshot state 而把旧 event 覆盖回去。每一轮 tail 先完成 COLD
durable append，再推进 staging applied progress 并发出 `RESYNC_ACK`。

收到 `HANDOFF_COMMIT` 后，Follower 在 staging durable/applied 都达到 `H` 时原子发布
新 runtime：停止旧服务入口、切换 active directory/runtime、恢复读服务和普通 listener。
失败、断电或 `RESYNC_ABORT` 时销毁 staging runtime/临时文件，旧 generation 保持可用；
不允许半安装的 checkpoint 对外提供读服务。

## 进度、ACK 与诊断

普通 `peer_accepted_seq`、`peer_durable_seq`、`peer_applied_seq` 只在有效普通复制或
`RESYNC_ACK` 已证实连续前缀时推进。`SNAPSHOT_INSTALLED` 只说明 checkpoint 已落盘，
不能冒充 `applied_seq`；`HANDOFF_COMMIT` 前不将 checkpoint/tail 的临时结果暴露为
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

## 实施顺序

1. COLD snapshot session：增加 snapshot metadata/captured-seq 导出和 retention pin；
   补充 compact 对 active pin 的约束与 UT。
2. Replica wire：新增 resync frame 编解码、session 身份/term/epoch 校验，以及 chunk
   assembler UT；保留原 EVENTS/ACK/HEARTBEAT 行为。
3. Follower staging：创建 empty fenced COLD/Core/WARM runtime，完成 import、tail
   append、per-shard replay/apply 与成功/失败清理。
4. Leader orchestrator：实现 checkpoint export、连续 tail、catch-up rounds、final
   emission gate、handoff 与 pin release。
5. 触发策略：先提供受控管理/UT 入口；自动 reconnect、GAP 检测和健康状态触发在该
   状态机稳定后接入。
6. server 接线：在 `redis-server` 生命周期中接入诊断和受控 resync 入口，再进行真实
   111/112 回归。

每步都保留现有普通复制回归。v2 descriptor ring + payload arena 是独立优化；resync
frame 不依赖其完成，但 v2 落地后必须重新运行本设计的跨机回归。

## 验收矩阵

本地 UT：

- 已 compact 到增量范围不可得时，Follower 请求 snapshot 后恢复连续 prefix；
- 大 checkpoint 多 chunk、arena/slot 边界、最后不足一 chunk、chunk offset 重复或跳跃；
- chunk checksum、全量 hash、term、epoch、peer identity、session id 不匹配全部拒绝；
- checkpoint import 失败、staging AOF append 失败、tail checksum conflict、进程中断后
  旧 active generation 仍可恢复；
- compact 在 session pin 存在时不得删除所需 tail，成功/abort 后可以释放 pin；
- 高频 Leader 写入下 final gate 保持全局 seq 连续，切换后第一条普通 EVENTS 为 `H + 1`。

真实 111/112：

1. 启动 Leader/Follower，完成一段正常 Redis TCP/SDK 写入。
2. 停止或破坏 Follower 的可恢复 AOF/WARM，Leader 持续写入以制造 tail。
3. 显式触发 resync，记录 checkpoint generation、`B`、`H`、chunk 字节数、pin 和阶段耗时。
4. 通过 Follower TCP SDK 验证 checkpoint 前数据、tail 更新、更新值和 tombstone。
5. 停止 Follower、reset WARM 后以其新 COLD 目录重启，再次验证 durable prefix。

通过上述测试只能证明跨节点 checkpoint + AOF tail resync。自动 reconnect、failover、
fencing 和角色提升必须单独故障模型与回归，不能由本测试隐含宣称完成。
