# TLC HA 心跳检测与切主设计

> **历史方案说明**：本文记录早期 heartbeat-driven 自动晋升方案。当前实现已经采用
> [TLC HA/LVS/Keepalived 设计](./TLC_HA_LVS_KEEPALIVED_DESIGN.md)：keepalived 负责
> 故障检测、VRRP 和 VIP，HPC-Redis 只通过外部 `HA PROMOTE/DEMOTE/FENCE` 执行角色
> 转换。本文中关于 `SUSPECT/CANDIDATE`、heartbeat 自动晋升、双通道 heartbeat、
> `heartbeat_failure_threshold` 和 `heartbeat_suspect_hold_down_ms` 的内容均为历史
> 设计记录，不是当前运行时契约；当前代码和验收以 keepalived 设计及回归方案为准。

## 1. 目标与范围

本文定义固定双机 Node group 中的故障检测、切主、角色通知、动态线程模型和旧节点
恢复机制，作为 [TLC HA 跨节点 resync 设计](./TLC_HA_CROSS_NODE_RESYNC_DESIGN.md)
中 M3-M7 数据面之上的控制面设计。

本文只覆盖一个 `hpc_node_id` 对应的双副本 Node group。多个 Node group 的 owner、
状态和切换相互独立，不因某个 Node group 故障而阻塞其他 Node group。

heartbeat 只负责 liveness 检测和进度上报，不改变现有 `EVENTS`、`ACK`、checkpoint、
AOF tail 和 handoff 的连续性约束。

## 2. 故障模型

第一版采用以下假设：

1. 两台机器不会同时宕机；
2. 不考虑双向网络隔离，不会出现两台机器都长期看不到对方但仍继续运行；
3. 故障期间至少有一台机器可以继续运行，并最终向另一台机器发送控制通知；
4. 心跳只能说明一段时间内没有收到合法消息，不能单独证明对端已经停止写入；
5. 旧 Leader 恢复后必须接受更高 `ha_term`，转换为 Follower，不能继续作为旧 owner 写入。

在这个故障模型下，Follower 可以在连续超时并完成本地检查后自动晋升。若未来允许
网络分区，必须增加 lease、witness、STONITH 或其他外部 fencing，不能只依赖本地
heartbeat 和自增 term 保证无脑裂。

## 3. 现有实现基线

当前 `tlc_ha_replica` 已实现：

- 固定大小、带 checksum 的 HEARTBEAT frame；
- `hpc_node_id`、`peer_node_id`、`ha_term` 和 role 校验；
- `last_heartbeat_received_ns`、`peer_health`、`peer_durable_seq`、
  `peer_progress_seq` 观测；
- Leader/Follower 的 durable/applied 进度上报；
- heartbeat 超时后将对端标记为 `UNAVAILABLE`。

当前缺少的是：

- `SUSPECT/CANDIDATE/MASTER/FAULT` 状态转换；
- owner/term 持久化；
- 角色晋升、降级和 fencing；
- Leader 通知及对端强制降级；
- 连接断开后的 reconnect；
- 旧 Leader 恢复后的 FENCED/resync 流程。

因此现有 heartbeat 是健康监控，不是完整的 failover 机制。

## 4. role 与 HA 控制状态

`role` 和 HA 控制状态是两个不同维度。

`role` 表示复制方向：

```text
Leader / Follower
```

控制状态表示 owner 生命周期：

```text
INIT -> BACKUP -> SUSPECT -> CANDIDATE -> RECOVERING -> MASTER
                                  |             |
                                  +-----------> FAULT
```

`FENCED` 是独立安全闸门，可以叠加到任意状态。

例如：

```text
role=Follower, state=BACKUP,     fenced=false  正常备份
role=Follower, state=SUSPECT,    fenced=false  多个周期未收到主节点
role=Follower, state=CANDIDATE,  fenced=true   正在申请接管，不可写
role=Follower, state=RECOVERING, fenced=true   正在恢复 COLD/WARM
role=Leader,   state=MASTER,     fenced=false  已拥有写权
role=Leader,   state=FAULT,      fenced=true   失去 owner，禁止写入
```

建议增加：

```c
atomic_uint role;
atomic_uint ha_state;
atomic_bool write_fenced;
atomic_uint_fast64_t current_term;
atomic_uint_fast64_t last_valid_heartbeat_ns;
atomic_uint missed_heartbeat_count;
atomic_uint healthy_heartbeat_count;
atomic_uint_fast64_t connection_epoch;
```

当前 `role` 是启动配置字段，线程集合也按启动 role 创建。这是固定角色版本的实现
限制，不是 HA 协议的要求。

## 5. 心跳协议与检测

### 5.1 心跳字段

第一版继续复用现有固定 heartbeat schema：

```text
hpc_node_id
peer_node_id
ha_term
role
health
sent_at_ns
durable_seq
progress_seq
checksum
```

`sent_at_ns` 只用于诊断；超时必须使用接收端本地 `last_valid_heartbeat_ns`。只有
通过 frame checksum、Node group identity、peer identity 和 term 单调性校验的 heartbeat
才能刷新 liveness 或更新进度。

### 5.2 liveness 与 health 分离

需要分开保存：

```text
peer_liveness        是否在超时窗口内收到合法 heartbeat
peer_reported_health 对端报告的 COLD/复制健康状态
```

对端报告 `FAILED` 不等价于传输链路失联；链路仍可用于发送切主通知和恢复控制帧。
反过来，链路失联时也不能因为最后一次 health 为 `HEALTHY` 就继续开放依赖对端的操作。

### 5.3 退避和滞后确认

单次超时不能触发切主。建议默认：

```text
heartbeat_interval = 1s
heartbeat_timeout  = 3s
连续失败次数       = 3
suspect_hold_down  = 2~5s
重试退避            = 1s, 2s, 4s，最大 5s
```

流程为：

```text
收到合法 heartbeat
    -> missed_count = 0，清除 SUSPECT

第一次超过 timeout
    -> state = SUSPECT
    -> 记录故障开始时间
    -> 发起探测或 reconnect

后续连续 timeout
    -> missed_count++
    -> 按 backoff 再次确认

达到连续失败次数且超过 hold-down
    -> state = CANDIDATE
```

heartbeat 线程只负责检测并发布故障事件；HA controller thread 负责状态转换、恢复和
角色切换，避免 heartbeat 线程执行长时间 COLD I/O。

## 6. CANDIDATE 到 MASTER

进入 `CANDIDATE` 后，controller 检查：

1. 仍未收到合法 peer heartbeat；
2. 本地 COLD manifest/AOF 可读取；
3. 本地 `durable_seq` 连续且可信；
4. 没有未完成的 snapshot install 或 resync reset；
5. 没有更高的本地或 peer term；
6. 本地已进入写 fencing transition。

接管边界使用本地 Follower 的 durable prefix：

```text
takeover_seq = local_follower_durable_seq
```

不能使用 `applied_seq` 代替 durable_seq。若需要重建 WARM，节点先进入 `RECOVERING`，
恢复成功后才能成为 `MASTER`。未同步到 Follower 的旧 Leader 尾部必须作为可能丢失范围
暴露，不能伪造为已复制。

term 处理顺序：

```text
new_term = max(local_persisted_term, last_seen_peer_term) + 1
    -> 持久化 owner/term metadata
    -> fsync
    -> 完成本地 fencing transition
    -> role=Leader, state=MASTER
    -> 打开写入口
```

## 7. Leader 通知与对端降级

成为 Leader 后发送高优先级 `LEADER_ANNOUNCE` 控制帧，建议字段为：

```text
hpc_node_id
old_owner_id
new_owner_id
new_ha_term
topology_epoch
takeover_seq
connection_epoch
reason
checksum
```

发送顺序：

```text
1. 关闭普通 event emission
2. 持久化新 owner/term
3. 切换为 MASTER
4. 发送 LEADER_ANNOUNCE
5. 异步接收 ROLE_ACK；控制面可查询确认状态，对端不可达时保持断线/故障策略
6. 恢复普通 EVENTS 发送
```

对端收到 `new_ha_term > local_term` 的通知后：

```text
收到 LEADER_ANNOUNCE
    -> write_fenced = true
    -> 停止本地写入口
    -> 中止旧 term 的 resync/session
    -> 更新 owner/term 并 fsync
    -> role=Follower
    -> state=BACKUP 或 RECOVERING
    -> 返回 ROLE_ACK
    -> 连接新 Leader，执行 AOF replay 或 snapshot resync
```

控制帧优先级不能只依赖 listener 的 `switch` 顺序。STREAM/UB 当前是 FIFO 通道，建议
为控制帧保留独立优先级队列或预留 ring slot；至少在发送通知前设置
`resync_emission_gate` 并停止普通 sender。

## 8. 动态线程模型

切主不应通过销毁并重建线程实现。所有副本启动时创建完整基础线程：

```text
receiver_thread
sender_thread
apply_thread
heartbeat_thread
HA controller_thread
```

线程根据运行时 `role/state` 选择行为：

```text
sender:
    Leader   -> EVENTS、AOF replay、resync tail
    Follower -> ACK、heartbeat、ROLE_ACK、RESYNC_REQUIRED

receiver:
    所有角色都接收 EVENTS、ACK、heartbeat 和控制帧

apply:
    Follower apply 复制事件
    Leader 可处理本地提交或晋升后的恢复队列
```

角色转换顺序：

```text
1. controller 设置 write_fenced=true
2. 关闭普通 emission
3. 等待 ingress/sender/apply in-flight 清零
4. 持久化新 owner/term
5. 原子更新 role/state
6. 绑定或解绑 event sink
7. 唤醒 sender/apply 线程
8. 发送 LEADER_ANNOUNCE 或恢复复制
9. 完成条件满足后解除 fencing
```

因此 `role` 必须改为原子状态，或由单一 controller 发布不可变状态快照，不能让多个
线程并发读写普通非原子 `role`。

## 9. 连接断开与重连

需要区分协议故障和网络断开：

```text
checksum/身份/版本/term 错误
    -> FAULT/FENCED

EOF、连接关闭、对端进程退出
    -> DISCONNECTED
    -> reconnect backoff
    -> HANDSHAKE
```

重连握手至少交换：

```text
hpc_node_id
peer_node_id
role
ha_term
topology_epoch
connection_epoch
durable_seq
```

重连后只有当前 term、当前 owner 和连续 durable prefix 都通过校验，才恢复 EVENTS 复制。

## 10. 旧 Leader 恢复

旧 Leader 发现 peer term 更高时：

```text
立即 write_fenced=true
    -> state=RECOVERING
    -> 不接受客户端写入
    -> role=Follower
    -> 从新 Leader durable prefix 追赶
    -> AOF 起点保留时使用 strict replay
    -> 起点已 compact 时使用 checkpoint + tail resync
    -> WARM 恢复完成后进入 BACKUP
```

现有 `SNAPSHOT_BEGIN/CHUNK/END`、tail rounds、`HANDOFF_COMMIT` 和 `HANDOFF_ACK` 可以
继续作为恢复数据面，但必须将固定 Leader/Follower、固定 term 的限制改成由新 owner term
和 connection handshake 驱动。

## 11. 一致性不变量

```text
Follower durable_seq 始终是连续前缀
Follower applied_seq 不能代替 durable_seq
旧 term 的 EVENT/ACK/heartbeat 不得更新状态
同一 seq 的 payload/checksum 必须稳定
ha_safe_point_seq = min(local_durable_seq, peer_durable_seq)
未复制的 Leader 尾部必须报告为可能丢失
compact 不能删除新 owner 恢复所需的 AOF/checkpoint
```

heartbeat 中的 durable_seq 可以补足丢失的 ACK，并推进 Leader 的 replicated_seq 观测，
但 heartbeat 本身不触发 Follower flush，也不构成新的 durable ACK。

## 12. 实现阶段

### M8：故障检测

- 增加 `ha_state`、miss counter、SUSPECT 和 backoff；
- 分离 peer liveness 与 peer health；
- heartbeat timeout 只发布 controller 事件；
- 增加单次丢包、连续丢包、恢复和抖动测试。

### M9：动态角色与 term

- 所有副本创建完整 sender/apply/receiver 线程；
- role/state 原子化；
- 增加 owner/term metadata 和 fsync；
- 实现 FENCED、CANDIDATE、RECOVERING、MASTER 状态转换。

### M10：Leader 通知与重连

- 增加 `LEADER_ANNOUNCE`、`ROLE_ACK`；
- 实现控制帧优先级和 connection epoch；
- 网络断开进入 reconnect，而不是立即停止整个 Replica runtime。

### M11：切主与恢复闭环

- Follower 自动晋升；
- 旧 Leader 接收高 term 后自动降级；
- 复用 AOF replay/snapshot resync 完成新旧 owner 收敛；
- 完成单个 `hpc_node_id` 的故障注入和跨节点回归。

### 历史落地状态（2026-09-06）

以下记录对应 heartbeat-driven 方案的历史实现阶段。后续 keepalived 方案移除了自主
晋升和双通道 liveness 判断，不能将本节的自动晋升描述当作当前行为。

M8 已落地。当前实现包含 `ha_state`、连续 timeout 计数、SUSPECT、退避探测、
SUSPECT hold-down、peer liveness/peer health 分离，以及 heartbeat failure pending
事件。单次 heartbeat timeout 只进入 SUSPECT，不执行角色切换；达到连续失败阈值后只
通知 controller，自动切主仍属于 M11。

M9 已落地基础能力。`role` 和 `ha_term` 使用原子运行时状态；Leader/Follower 两端
都会创建 sender、receiver、apply、heartbeat 和 controller 线程。sender 只消费
Leader EVENTS 队列，Follower apply 只消费复制队列，角色变化不销毁或重建线程。新增
`FENCED` 状态、角色/term 查询接口和受 fencing 约束的本地角色转换接口。term metadata
使用 checksum、临时文件、`fsync(file)`、原子 rename 和 `fsync(directory)` 持久化。

M9 当前实现已达到退出条件；保留一个需要在后续扩展中维护的线程/队列设计约束：

1. **Leader 的 apply 线程会抢走本应由 sender 消费的发送队列**

   统一创建完整线程后，如果 apply 线程不检查运行时 role，Leader 的 event sink 会把
   event 放入发送队列，但 Leader apply 线程也会从同一个队列 pop。event 可能被本地
   apply 或释放，sender 因而无法发送给 Follower。当前实现用 role 门控规避该问题：
   Leader apply 线程对该复制队列空转，Follower apply 线程才消费它。未来若要让 Leader
   处理晋升后的恢复队列，必须使用独立队列，不能重新共享 EVENTS sender queue。

2. **post-handoff 的 resync gate/replay 状态曾未完全闭环，现已修复**

   final handoff 期间新写入会被 gate 拦截，并记录最早待补发位置 `replay_from_seq`。
   收到 `HANDOFF_ACK` 后，预期顺序是关闭 gate、设置 `normal_min_seq = H + 1`、由
   sender 从 AOF replay `replay_from_seq` 并发送补发事件，最后清除 replay 状态。
   原因是 sender 循环中残留了一条无 body 的条件语句，将
   `replay_from_seq -> replay_mode -> replica_replay_pending()` 错误嵌套在
   `queue_peek() != NULL` 分支内。handoff 后普通队列为空时，sender 永远不会启动
   AOF replay，导致 `replay_from_seq=H+1` 保持不变。删除该错误嵌套后，sender 可以在
   队列为空时先 claim replay cursor、补读 AOF，再发送 `H+1`。新增的 handoff/replay、
   follower durable/apply 诊断日志用于持续验证这条链路。

### Queue 与 AOF replay 的顺序不变量

`sender queue` 和 AOF 是两个不同层次的对象：queue 是发送调度队列，AOF 是持久化
补发数据源。handoff 后的典型状态为：

```text
普通 queue 中可能残留 seq <= H 的旧事件
normal_min_seq = H + 1
replay_from_seq = H + 1
```

sender 先排空 queue：小于 `normal_min_seq` 的事件直接丢弃，因为已经被 handoff 边界
`H` 覆盖；queue 为空后，才从 AOF 的 `replay_from_seq` 开始读取，并把 replay event
重新放入 queue，由同一个 sender 发出。`replay_mode` 是执行门控，不是新的 seq 来源：
它保证 replay 过程中到达的新写入也只更新最早的 `replay_from_seq`，不会绕过 replay
流直接进入普通发送顺序。

当前 handoff 协议依赖以下不变量来避免重复发送：final gate 建立后停止普通 queue
admission，等待 sender in-flight 清零，并在 commit 前把 Leader 的最新 durable boundary
纳入 `H`。因此 handoff 完成后 queue 不应出现 `H + 1` 或更大的有效事件；如果未来放宽
quiescence，必须增加 queue/replay 游标推进或显式去重，不能直接从 `H + 1` 重放。

### M9 退出判断与 M10 准入

M9 可以结束并推进 M10。M9 已满足：

- 全角色基础线程常驻，切换不销毁/重建 sender、receiver、apply；
- role、HA state、term 原子发布；
- fencing、in-flight drain、owner/term metadata fsync；
- heartbeat 检测结果可被 controller 消费；
- 普通复制和 post-handoff `H + 1` replay 回归通过。

M10 的工作边界从这里开始：增加 `LEADER_ANNOUNCE`、`ROLE_ACK`、控制帧优先级、
connection epoch 和 reconnect。M10 不应重新改变 queue/AOF replay 的上述顺序不变量。

### M10 当前落地状态（2026-09-06）

M10 已完成协议和运行时基础接入：

- 增加 `LEADER_ANNOUNCE` 和 `ROLE_ACK` 帧，listener 在普通 ACK/EVENTS 分支前处理；
- Leader 的 `MASTER` 角色转换先保持 emission fence，发送 announce，成功后才解除 fence；
  announce 失败保持 `FAULT/FENCED`；
- Follower 只接受 peer identity、topology、term、connection epoch 合法的 announce，
  高 term announce 会先降为 `FOLLOWER/BACKUP`，随后发送 ACK；Leader 只接受当前 term、
  当前 epoch 且确认 `FOLLOWER` 的 ACK，并记录 announce 已确认状态；
- `connection_epoch` 使用单调递增连接世代。STREAM 断开只清除 `connected` 并保留
  sender/receiver/apply/heartbeat/controller 线程；控制线程自动替换内部活动 fd、
  提升 epoch、清理心跳失败计数并复用原线程，不重建线程；
- fd 替换和关闭受现有 stream 写锁保护，避免读线程关闭旧连接与写线程并发使用同一个 fd。
- 控制面可通过 `tlc_ha_replica_leader_announce_acked()` 查询当前 term 的 announce
  是否已被 Follower 确认；单测覆盖 announce/ACK 和 TCP 断线重连。
- 新 Leader 在 reconnect 后会自动重发 `LEADER_ANNOUNCE`；重连握手期间抵达的旧 role
  heartbeat 会被忽略而不会终止 listener，旧 Leader 随后降级为 `FOLLOWER/BACKUP`。
- STREAM 写路径使用 `MSG_NOSIGNAL`；断线期间的写失败转为 reconnect 状态，不再因
  `SIGPIPE` 终止整个进程。

当前仍未宣称完成的部分：M11 的 replay/snapshot 自动选择与跨进程收敛仍需继续实现；UB
ring 上多个 producer 的严格控制帧优先级还需要单独的 reservation 串行化方案。现有
`tlc_ha_replica_ut`、`tlc_ha_replica_process_ut` 在 M10/M11 改动后均通过。

当前验证结果：`tlc_ha_replica_process_ut` 和完整 `tlc_ha_replica_ut` 均通过，覆盖普通
复制、post-handoff `H+1` 补发、heartbeat 检测和 resync 场景。M9 仍不包含完整切主恢复
闭环；M10 已接入 `LEADER_ANNOUNCE`、`ROLE_ACK` 和基础 reconnect，但自动晋升及跨进程
重连后的 replay/snapshot 闭环仍是后续工作。

### M11 当前落地状态（2026-09-07）

M11 第一项“heartbeat failure -> CANDIDATE -> MASTER”已接入
`replica_resync_controller_main()`：

```text
heartbeat_failure_pending = true
    -> 仅 Follower 响应
    -> CAS BACKUP/SUSPECT -> CANDIDATE
    -> 再次确认 pending=true、peer_liveness=false
    -> resync 非活动且 COLD progress 可读
    -> ha_term + 1
    -> transition_role(LEADER, MASTER, new_term)
```

晋升继续复用 M9 fencing、in-flight drain、term metadata 持久化和 M10 announce。若旧
Leader 已断线，`LEADER_ANNOUNCE` 发送失败会记录为 deferred，但本地晋升仍会完成；旧
Leader 恢复并 reconnect 后，必须通过更高 term 的 announce 降级。若是 UB publish 失败或
term/COLD 状态不可用，则进入 `FAULT`，不发布 `MASTER`。合法 heartbeat 在候选阶段到达
时会清除 pending 并将状态恢复为本地正常状态，避免一次延迟 heartbeat 触发接管。

新增 `run_automatic_promotion_test` 覆盖断开对端、连续超时、hold-down、term 递增、
`CANDIDATE -> MASTER` 和断线 announce deferred；该测试与现有普通复制、resync、
heartbeat、reconnect 测试均通过。新增双实例 `run_reconnect_leader_announce_test` 覆盖
新 Leader 重连重发 announce、旧 Leader 收到高 term 后降级为 `FOLLOWER/BACKUP`，以及
重连握手期间旧 role heartbeat 被忽略。该测试还覆盖对端关闭导致的 STREAM 写失败，
确认不会出现 `SIGPIPE` 进程退出。

### M11 剩余工作与执行顺序

除控制面自动晋升和 STREAM reconnect 基础外，M11 仍有以下工作：

1. **旧 term 事件处理**：Follower 收到 `event.ha_term < local_term` 的 UB/STREAM
   事件时必须记录 `WARNING`，丢弃该批次，并触发从当前 durable 边界开始的 replay 或
   snapshot resync；`event.ha_term > local_term` 则说明控制面尚未先于数据面生效，必须
   拒绝并记录顺序违规。
2. **Core/Replica term 同步**：`core->ha_term` 是普通业务写入写入 COLD 时的 term 来源，
   `replica->ha_term` 是心跳、announce 和 controller 的 term 来源。两者必须在启动恢复、
   `CANDIDATE -> MASTER` 以及收到高 term announce 时原子地发布同一个 term。
3. **M11-1 自动恢复选择**：切主或重连后以 `H + 1` 为起点自动尝试 AOF replay；当 AOF
   保留窗口不包含该起点时，自动切换到 snapshot resync。普通 queue、replay cursor 和
   `normal_min_seq` 必须保持单调，不能重复或跳过有效事件。
4. **重连后的跨进程收敛**：旧 Leader 恢复后完成高 term 降级、数据通道重建，并通过
   replay/snapshot 追平 durable/applied seq。
5. **UB 控制帧与故障恢复**：补齐多 producer reservation 下的控制帧优先级、UB ring
   reset/rebind 和对应的断线恢复测试。
6. **真实故障回归**：增加真实 TCP 断线/重连、单节点宕机恢复、旧 Leader 降级和 UB
   数据面故障注入测试。
7. **耗时 TODO**：继续拆分现有单测约 1 秒耗时，定位固定 sleep、COLD fsync、ACK 轮询
   和线程调度延迟。

建议先完成第 1、2 项，再完成第 3 项；第 4-6 项依赖恢复协议闭环后执行。

截至 2026-09-07，第 1、2 项已完成：Follower ingress 会对旧/未来 term EVENTS 做顺序
校验，旧 term 以 `WARNING` 记录并安排 GAP resync，未来 term 记录顺序违规并拒绝；Core
的 HA term 已改为原子值，并在 Replica 启动恢复和角色转换时同步发布。第 3 项尚未完成
切主后的自动触发：现有代码已支持收到 GAP 后的 AOF replay 及 retention 缺失时的 snapshot
fallback，但晋升/重连后的 peer progress 握手和自动选择仍需补齐。

### M11-1 自动恢复选择与跨进程收敛（#3 + #4）讨论稿

#### 现有基础能力

已有的 replay/snapshot 基础能力：

| 能力 | 状态 |
|------|------|
| Follower 收到 GAP → 发 RESYNC_REQUIRED(GAP) → Leader AOF replay | 已有 |
| AOF retention 不足 → 自动 fallback 到 snapshot resync | 已有 |
| CONFLICT → snapshot resync 全量闭环 | 已有 |
| sender 检查 `role == LEADER` 后发送 EVENTS | 已有 |
| `transition_role` 到 BACKUP 时解除 fenced | 已有 |
| apply 检查 `role == FOLLOWER` | 已有 |

**结论**：已有 replay/snapshot 基础能力，但仍需补齐 takeover 边界语义、data-plane
generation 隔离、显式 recovery session 生命周期和 transition 并发清理。以下逐项
设计。

#### 问题 1：takeover_seq 语义不正确（协议字段修正）

**现状**：`replica_send_leader_announce()` 每次从实时 COLD 进度取
`takeover_seq = progress.durable_seq`。新 Leader 晋升后继续写入，发送给恢复中的
旧 Leader 的 announce 携带的是当前 `durable_seq`（例如 15），而不是晋升时的固定
边界（例如 10）。

**后果**：旧 Leader 有未复制尾部（durable=13）时，announce 携带 takeover_seq=15，
`13 < 15` 判定为 GAP 而不是 CONFLICT，旧 term 的脏尾部（seq 11~13）不会被
snapshot 清除。

**修正**：

1. 在 Follower 晋升为 Leader 时，固定保存晋升边界：

   ```c
   replica->promotion_takeover_seq = local_durable_seq_at_promotion;
   ```

   该值在 `replica_controller_try_promote()` 成功路径中、`transition_role` 调用
   之前获取 COLD progress 并保存。此后直到下一次角色转换，该值不变。

2. `LEADER_ANNOUNCE` 帧扩展一个字段：

   ```text
   takeover_seq         固定晋升边界，用于旧 owner 判断 GAP/CONFLICT
   current_durable_seq  当前 Leader 已 durable 的最新 seq，用于恢复目标
   ```

   `replica_send_leader_announce()` 改为：
   - `announce.takeover_seq = replica->promotion_takeover_seq`
   - `announce.current_durable_seq = progress.durable_seq`（实时值）

3. Follower（旧 Leader）收到 announce 后的恢复判断：

   ```text
   local_durable < takeover_seq
       → 晋升前就落后，需要补齐 [local_durable+1, takeover_seq]
       → 选择 GAP → AOF replay
       → 追平后继续接收 takeover_seq+1 之后的新数据

   local_durable > takeover_seq
       → 旧 owner 有未复制尾部（旧 term uncommitted suffix）
       → 选择 CONFLICT → snapshot resync
       → 清除旧尾部，从新 Leader 的 checkpoint 重建

   local_durable == takeover_seq
       → 晋升边界完全一致
       → 需要追赶 takeover_seq+1 之后的新数据
       → 进入 RECOVERING，等待显式 recovery session
   ```

4. 初始启动时（非晋升），`promotion_takeover_seq` 初始化为 0。从配置启动的 Leader
   不经过 `try_promote`，announce 中 `takeover_seq` 直接使用实时 `durable_seq`——
   这对于固定角色部署是正确的，因为不存在旧 owner 脏尾部问题。

#### 问题 2：transition_role 未清理通路遗留状态

**现状**：`transition_role` 只做 fencing、in-flight drain、term 持久化和
role/state 发布。以下状态未清理：

| 遗留状态 | 影响 |
|----------|------|
| sender queue | 晋升后旧 Follower EVENTS 会被发回对端 |
| `replay_from_seq` / `replay_mode` | 旧 session 的 replay seq 被新 term sender 使用 |
| `resync_required_pending` / `resync_session_state` | 旧 Follower resync 请求残留 |
| `normal_min_seq` | 晋升后 sender 沿用旧值 |
| resync assembler artifact | `.part` 文件残留 |

**这些是功能正确性的一部分，不是优化。**

**修正**：在 `transition_role` 的 in-flight drain 完成、term 持久化之后、role/state
发布之前，统一执行以下清理（不区分方向，两个方向操作对称）：

```text
1. replica_discard_sender_queue()
2. atomic_store(replay_from_seq, 0)
3. atomic_store(replay_mode, false)
4. atomic_store(replay_log_active, false)
5. atomic_store(replay_producer_active, false)
6. atomic_store(resync_required_pending, false)
7. atomic_store(resync_session_state, IDLE)
8. replica_reset_resync_artifact()
9. atomic_store(sender_discard_pending, false)
10. atomic_store(normal_min_seq, 0)
```

#### 问题 3：snapshot assembler 清理存在并发风险

**现状**：`transition_role` 的 drain 只等待 `ingress_inflight`、`apply_inflight`、
`sender_inflight`。snapshot assembler 由 data listener 使用，未包含在 drain 中。

**并发场景**：

```text
control listener 收到 LEADER_ANNOUNCE
    → transition_role()
    → 清理 snapshot assembler（步骤 8）

data listener
    → 同时正在写 SNAPSHOT_CHUNK → pwrite .part 文件
```

可能导致 `.part` 文件被删除后继续写入、assembler 状态被重置、新旧 session 混用。

**修正方案**：

增加 `snapshot_assembler_inflight` 原子计数，data listener 在处理
`SNAPSHOT_BEGIN/CHUNK/END` 前加 1、处理完减 1。transition_role 的 drain 阶段需要
同时等待该计数归零。

另外，assembler 增加 `recovery_generation` 字段：每次 transition 递增该代际号。
data listener 在 assembler 操作前检查 generation 是否匹配，不匹配时丢弃该帧。
这保证旧 session 的迟到 UB 帧不会污染新 session。

```text
transition_role drain 阶段：
    等待 ingress_inflight == 0
    等待 apply_inflight == 0
    等待 sender_inflight == 0
    等待 snapshot_assembler_inflight == 0  ← 新增

清理阶段：
    replica->recovery_generation++         ← 新增
    replica_reset_resync_artifact()

data listener SNAPSHOT_* handler：
    if (assembler->recovery_generation != replica->recovery_generation)
        discard + WARNING
```

#### 问题 4：收敛判断不能只用 durable_seq

**现状**：announce handler 只比较 `local_durable_seq vs takeover_seq`。

**不足**：HA 状态还包含 `applied_seq`、term、connection epoch、UB data-plane
generation。如果 `local_durable == takeover_seq` 但 `applied_seq < takeover_seq`，
COLD 有数据但 WARM 尚未恢复完成，不能直接开始接收新数据。

**修正**：announce 触发的恢复判断增加 applied_seq 检查：

```text
local_durable == takeover_seq 时：
    if (applied_seq < durable_seq)
        → 进入 RECOVERING（不是 BACKUP）
        → 等待 WARM apply 追平
        → applied_seq == durable_seq 后才解除 RECOVERING → BACKUP
    else
        → 直接 BACKUP
```

同时，新 Follower 在 `RECOVERING` 状态下不接受普通 EVENTS（ingress 拒绝），只有
达到 BACKUP 后才允许。这防止 WARM 未恢复完就混入新事件。

#### 问题 5：TCP/UB 跨通道无全局顺序 — data-plane generation

**现状**：TCP announce 和 UB 数据帧是两个独立传输。reconnect 后 UB ring 中可能
残留旧 term 事件、旧 session 事件、announce 前已 publish 但未消费的事件。

**修正**：增加 data-plane generation 机制：

1. `LEADER_ANNOUNCE` 和 `ROLE_ACK` 交换后，双方建立新的 `data_generation`。
2. 数据帧（EVENTS、ACK、SNAPSHOT_*、TAIL_*）携带或关联 `data_generation`。
3. 旧 generation 的 UB 帧记录 WARNING 后丢弃。

具体实现：

```text
announce/ROLE_ACK 完成后：
    Leader: atomic_store(data_generation, connection_epoch)
    Follower: atomic_store(data_generation, announce.connection_epoch)

data listener (UB ring)：
    读到帧后检查帧的 ha_term 和 data_generation
    ha_term < local_term → WARNING + 丢弃
    data_generation < local_data_generation → WARNING + 丢弃
```

由于现有帧 header 中已有 `ha_term` 校验（Follower ingress line 4206），旧 term
事件已被拒绝。对于同 term 但旧 connection epoch 的事件，需要新增
`data_generation` 校验。

**帧 header 不扩展**：`data_generation` 不写入 wire frame。利用现有
`connection_epoch` 原子值：reconnect 后 epoch 递增，Follower ingress 在已有的
`ha_term` 检查之后增加 `connection_epoch` 检查即可区分旧帧。UB ring 中残留的
旧帧没有 `connection_epoch` 字段（它在 handshake 层面），但它们的 `ha_term` 可以
区分——旧 term 帧被 term 校验拒绝。同 term 同 connection_epoch 的旧帧实际上是
断线前的正常帧，ingress 可以按 seq 幂等处理。

如果需要更严格的隔离（例如同 term 下 UB ring reset），可以在 reconnect 后执行
UB ring consumer cursor 重置，丢弃 ring 中所有残留帧，从空 ring 开始接收。

#### 问题 6：恢复应为显式 recovery session，不只依赖隐式 GAP

**现状**：切主后的恢复入口依赖 ingress 收到 EVENTS 时检测 COLD GAP，触发
`RESYNC_REQUIRED`。这对普通运行时缺口可行，但不适合作为完整 failover recovery
的唯一入口。

**风险**：

- replay 和 snapshot 可能同时启动（GAP 和 CONFLICT 请求交叉）；
- 旧 GAP 请求污染新 term；
- recovery 完成但 sender 已提前发送普通事件；
- 旧 session 的 `replay_from_seq` 残留。

**修正**：announce 触发的恢复改为显式 recovery session：

```text
LEADER_ANNOUNCE
    → Follower 降级为 FOLLOWER/RECOVERING
    → Follower 上报 RECOVERY_REQUEST(durable_seq, applied_seq, term)
    → Leader 保存 promotion_takeover_seq 和 current_durable_seq
    → Leader 比较 Follower durable vs takeover_seq
    → Leader 明确选择 REPLAY 或 SNAPSHOT
    → REPLAY: Leader 发送 RECOVERY_BEGIN(session_id, replay, start, end)
              → 从 AOF 补发 → Follower 追平
              → Leader 发送 RECOVERY_DONE(durable, applied, term)
    → SNAPSHOT: Leader 发送 RECOVERY_BEGIN(session_id, snapshot, ...)
              → 复用现有 snapshot resync 流程
              → 完成后发送 RECOVERY_DONE
    → 双方确认后：
        Follower: RECOVERING → BACKUP，解除 ingress gate
        Leader: 解除 emission gate，开始普通 EVENTS

普通运行时的 GAP 检测和 RESYNC_REQUIRED 保留不变，用于非 failover 场景。
```

**与现有 resync session 的关系**：recovery session 可以复用现有的
`RESYNC_REQUEST/SNAPSHOT_BEGIN/TAIL_REQUEST/HANDOFF_COMMIT` 流程，但入口从
announce handler 明确发起，而不是等待 GAP 隐式触发。recovery session 的
`session_id` 与 `recovery_generation` 绑定，旧 generation 的请求自动拒绝。

**简化方案**：如果不引入新的 `RECOVERY_BEGIN/RECOVERY_DONE` 帧，可以在现有
`RESYNC_REQUIRED` 帧中增加 `recovery_generation` 字段，并在 announce handler
中直接调用 `replica_follower_schedule_resync()` 时绑定 generation。Leader 收到
`RESYNC_REQUIRED` 后检查 generation，旧 generation 的请求静默丢弃。这样保持
帧协议的最小变更。

#### 问题 7：测试设计

M11-1 测试应使用与生产部署一致的传输基础设施：

**传输层**：loopback TCP bind + connect 作为控制面，POSIX shm UB ring 作为数据面。
socketpair 仅用于现有回归测试的向后兼容，不作为新 failover 测试的传输。

**进程模型**：至少两个独立进程（或 fork），各自独立维护 Core/COLD/Replica 生命周期，
通过 TCP 长连接和 UB ring 通信。这验证跨进程的 fd 替换、reconnect、term 持久化
和 COLD 恢复。

**场景覆盖**：

场景 A：正常切主 → AOF replay 收敛

```text
1. Leader A 和 Follower B 正常运行，双方 durable=N
2. A 断线（kill / close fd）
3. B heartbeat 超时 → CANDIDATE → MASTER → term+1
4. B 继续写入 M 条 → B durable=N+M
5. A 恢复并 TCP reconnect
6. B 发送 LEADER_ANNOUNCE(takeover_seq=N, current_durable=N+M)
7. A 降级 → RECOVERING → RECOVERY_REQUEST
8. B 选择 AOF replay → 补发 [N+1, N+M]
9. A 追平 → RECOVERY_DONE → BACKUP
10. 验证 A Core 数据、ha_term、role
```

场景 B：旧 Leader 有未复制尾部 → snapshot

```text
1. A/B 双方 durable=10
2. A 继续写入 3 条（A durable=13，B durable=10）
3. 断开，B 晋升 → takeover_seq=10
4. B 写入 5 条 → B durable=15
5. A reconnect → announce → A durable=13 > takeover_seq=10 → CONFLICT
6. snapshot resync → A 旧尾部被覆盖 → A durable=15
7. 验证 A 的 seq 11~13 旧数据不存在
```

场景 C：AOF retention 不足 → 自动 fallback snapshot

```text
1. 使用小 retention window (R=8, C=16)
2. A/B durable=10，断开，B 晋升
3. B 写 M=20 条 → retention compact 回收旧 prefix
4. A reconnect → GAP → RESYNC_REQUIRED(GAP)
5. B 尝试 AOF replay → TLC_COLD_RESYNC_REQUIRED
6. 自动 fallback → snapshot → A 追平
```

场景 D：UB ring 残留帧隔离

```text
1. 正常复制中，A 在 UB ring 中 publish 若干帧
2. 断开 TCP（UB ring 残留未消费帧）
3. B 晋升 → term+1
4. A reconnect → 降级
5. B 的 data listener 消费 UB ring 中旧帧
6. 验证旧 term 帧被 WARNING 丢弃，不影响新 session
```

场景 E：announce 与 UB EVENTS 交叉到达

```text
1. B 晋升后写入新事件并 publish 到 UB ring
2. 同时 TCP 发送 LEADER_ANNOUNCE
3. A 先收到 UB EVENTS（新 term）→ future term 拒绝
4. A 随后收到 TCP announce → 降级 → 更新 term
5. 后续 UB EVENTS 正常接收
```

#### 落地顺序

```text
Step 1: 增加并持久化 promotion_takeover_seq，修正 announce 帧语义
Step 2: 扩展 LEADER_ANNOUNCE 帧增加 current_durable_seq 字段
Step 3: 在 transition_role 中实现完整通路清理
Step 4: 增加 snapshot_assembler_inflight drain 和 recovery_generation
Step 5: announce handler 中增加 applied_seq 检查和 RECOVERING 状态
Step 6: 恢复流程改为显式 recovery session（复用 RESYNC_REQUIRED + generation）
Step 7: 增加 data_generation / connection_epoch 校验隔离旧 UB 帧
Step 8: 编译并运行现有 tlc_ha_replica_ut 确认不引入回归
Step 9: 新增 TCP loopback / 跨进程 场景 A ~ E 测试
Step 10: 全量本地 UT 通过
Step 11: 同步到 111/112 跑 UB 回归
Step 12: 真实 111/112 故障注入（kill -9 + 恢复）
Step 13: 更新本文档的 M11 落地状态
```

### M11-1 最终可落实设计结论（2026-09-07）

本节覆盖并取代上面的讨论稿约束，作为 M11-1 实现和验收的唯一依据。目标是同时完成
切主后的 owner 收敛、旧 Leader 恢复降级、durable/applied 追平、TCP/UB 残留帧隔离、
严格单主和客户端路由切换。

#### 1. Owner term 与 takeover boundary

每个 owner term 建立时固定一个 `takeover_seq`：

```text
takeover_seq = owner/term 建立瞬间的 durable_seq
```

该值在当前 term 内不变化。`LEADER_ANNOUNCE` 必须同时携带：

```text
takeover_seq         固定接管边界
current_durable_seq  发送 announce 时的实时 durable 目标
ha_term
connection_epoch
```

owner metadata 直接使用新的唯一格式（version=2）：

```text
owner_node_id
ha_term
takeover_seq
checksum
```

不保留 v1 兼容分支，不使用 `UINT64_MAX` 或其他 takeover 哨兵。旧格式 metadata 在
启动时拒绝加载，由部署或测试清理后重新生成。`ha_term` 和 `takeover_seq` 必须在同一
个 metadata 文件中一起 fsync、rename 和提交。

Follower 收到 announce 后按三分支判断：

```text
local_durable < takeover_seq
    -> GAP
    -> replay [local_durable + 1, takeover_seq]

local_durable > takeover_seq
    -> 旧 owner 存在未提交尾部
    -> CONFLICT
    -> snapshot 覆盖旧尾部

local_durable == takeover_seq
    -> 检查 current_durable_seq 和 applied_seq
    -> 仍有数据未追平时进入 RECOVERING
    -> durable/applied 都追平后进入 BACKUP
```

配置启动的 Leader 也在当前 owner term 建立时固定初始 boundary；后续 announce 使用
该 term 的固定 boundary，同时用 `current_durable_seq` 表示实时进度。

#### 2. transition 与 apply drain

所有角色转换由 `transition_mutex` 串行保护。加锁后必须重新检查 role、term 和目标
状态，后进入者发现状态已被更高 term 或前一个转换覆盖时直接结束。

统一转换顺序如下：

```text
lock transition_mutex
    -> 重新检查 role/term/state
    -> resync_fenced = true
    -> write_fenced = true（需要停止本地写入时）
    -> resync_emission_gate = true
    -> 等待 ingress_inflight == 0
    -> 等待 apply_inflight == 0
    -> Follower -> Leader 时等待 applied_seq == durable_seq
    -> 确认 apply queue 为空
    -> 等待 sender/replay/snapshot/data-listener inflight 清零
    -> 清理旧 queue、replay、resync 状态
    -> 持久化 term 和 takeover_seq
    -> 发布 role/state
unlock transition_mutex
```

`applied_seq == durable_seq` 的等待必须发生在 fence 之后，且有界超时；超时进入
`FAULT`，不能继续晋升。不能在 Follower 尚有未 apply 数据时无条件 discard queue。

#### 3. Recovery session 与 gate

旧 Leader 收到新 owner announce 后必须先进入：

```text
FOLLOWER / RECOVERING / fenced
```

随后发送 `ROLE_ACK(state=RECOVERING)`。ROLE_ACK 只表示已经接受新 owner，不表示数据
已经恢复完成。恢复完成必须由 `RECOVERY_DONE`（或等价的最终 handoff 确认）表示。

Recovery session 至少绑定以下信息：

```text
session_id
recovery_generation
ha_term
takeover_seq
target_durable_seq
follower durable_seq
follower applied_seq
recovery method: REPLAY 或 SNAPSHOT
```

流程为：

```text
LEADER_ANNOUNCE
    -> Follower RECOVERING + fenced
    -> ROLE_ACK(RECOVERING)
    -> RECOVERY_REQUEST
    -> Leader 选择 replay 或 snapshot
    -> recovery 数据传输
    -> 检查 durable/applied
    -> 必要时扩展 target 并继续补发
    -> RECOVERY_DONE
    -> Follower RECOVERING -> BACKUP
    -> Leader 解除 emission gate
```

恢复期间 Leader 不发送普通 EVENTS。新增写入只能纳入 recovery target 的 extend/tail
流程，不能绕过 recovery gate。Replay 边界必须满足：

```text
replay_start >= retained_floor_seq
replay_end <= leader durable_seq
```

#### 4. UB reset 与 producer barrier

不扩展普通 UB frame header，使用 recovery session 和 reset barrier 隔离旧帧。区分：

```text
data_reset_gate      仅在 reset 窗口暂停 data listener
ordinary_event_gate  recovery 完成前禁止普通 EVENTS
```

reset 顺序如下：

```text
建立 recovery session 和远端 emission gate
    -> 关闭本地 producer admission
    -> 等待 producer reservation 完成
    -> 等待 data_listener_inflight == 0
    -> reset RX ring consumer cursor
    -> recovery_generation++
    -> 清除 data_reset_gate
    -> 允许 recovery 数据消费
    -> 保持 ordinary_event_gate
```

`producer_inflight` 必须覆盖完整的 `reserve -> write -> publish` 区间。对端 fail-stop 时
可依据进程已停止简化 barrier；在线降级时必须先通过 `ROLE_ACK(RECOVERING)` 让远端停止
普通 producer。reset 丢弃的旧 session 帧由 recovery replay/snapshot 重新投递。

UB 中先到达的 future-term EVENTS 属于可预期的跨通道乱序：记录 `WARNING`、丢弃该批次
并等待 TCP `LEADER_ANNOUNCE`，不能因此直接终止 Replica。持续收到 future-term 帧但始终
没有合法 announce 时，才进入有界的重连或 `FAULT` 处理。

#### 5. 双通道 heartbeat 与严格单主

Heartbeat 同时使用 TCP 控制面和独立 UB liveness 通道，并且都采用 round-trip：

```text
TCP HEARTBEAT + HEARTBEAT_ACK
UB_HEARTBEAT + UB_HEARTBEAT_ACK
```

UB heartbeat 使用独立 ring 或预留高优先级槽位，不能和普通 EVENTS 共用易阻塞队列。
分别维护 `tcp_liveness` 和 `ub_liveness`。

```text
TCP、UB round-trip 都正常
    -> 正常运行

仅 TCP 失联、UB 仍正常
    -> 不允许 Follower 晋升
    -> 当前 Leader 继续作为唯一可写 owner

两条通道都失联或 owner lease 失效
    -> Leader write_fenced + emission fenced
    -> Leader 进入 FAULT/FENCED
    -> Follower 经连续超时和 apply drain 后晋升
```

`write_fenced` 必须在 COLD append 之前生效，不能只停止 sender。完整网络分区在没有
witness、lease 或外部 fencing 时不保证严格单主，应明确列为安全边界之外。

#### 6. 客户端 owner 切换

Replica 的 owner 变化必须同步给 Proxy/Supernode，至少包含：

```text
owner_node_id
current_owner
ha_term
peer endpoint
```

旧 Leader 被 fencing 后，对客户端返回 `HA_NOT_OWNER`、`HA_FENCED` 或 `HA_REDIRECT`。
Proxy 刷新 owner 后将后续请求路由到新 Leader。发送后超时的非幂等写请求不能盲目重试，
需要 `client_request_id` 或等价去重机制。

#### 7. M11-1 当前落地状态（2026-09-08）

已落地的运行时基础设施：

- owner metadata 使用 version=2，并持久化固定 `takeover_seq`；`LEADER_ANNOUNCE`
  同时携带固定边界和发送时的 `current_durable_seq`。
- 角色转换由 `transition_mutex` 串行化。转换先关闭 ingress、普通 emission、Core
  写入和 data producer admission，再等待 ingress/apply/sender/replay/data-listener
  清空；Follower 晋升前必须满足 `applied_seq == durable_seq`，超时进入 `FAULT`。
- data producer 的计数覆盖 `reserve -> write -> publish`。故障晋升且控制连接已断开时，
  在 drain 后重置 RX ring consumer cursor，并递增 recovery generation，隔离旧 UB 帧。
- TCP 控制面发送 heartbeat 和复制进度用于诊断；UB ring 只承载数据帧，不再发送 heartbeat。
  超时只记录带 backoff 的诊断日志，不改变 `ha_state`、不自动晋升，也不 self-fence。
  keepalived 通过 RESP `HA PROMOTE/DEMOTE/FENCE` 显式触发角色切换和紧急 fencing。
- `local_durable < takeover_seq` 的 GAP replay 在恢复状态下允许接收恢复 EVENTS；
  replay/apply 追平后恢复到 BACKUP。snapshot handoff 仍沿用现有
  `HANDOFF_COMMIT/HANDOFF_ACK` 闭环。

尚未完成、不能宣称验收通过的部分：

- 真实双机 keepalived/VIP 漂移和 notify 脚本联调仍需在 111/112 环境完成。
- 完整的 `RECOVERY_REQUEST/RECOVERY_DONE` 独立协议和 target 动态扩展；当前 GAP 和
  snapshot 继续复用既有 resync 控制帧。
- Proxy/Supernode 的 `HA_NOT_OWNER`/`HA_REDIRECT` 客户端路由和非幂等请求去重。
- 真实双机 TCP-only 诊断、VIP 漂移和外部 promote/demote/fence 切换测试。

验证记录：相关 Replica、process、UB node 三个单元目标均可编译通过，只有既有的
`_GNU_SOURCE` 重定义警告。当前沙箱禁止创建 POSIX shared memory，运行时测试在创建
`/tlc-ha-*-tx-*` 区域时收到 `Operation not permitted`，因此跨进程/UB 协议断言需在
具备 shm 权限的环境（111/112 真机或等效 CI runner）执行。

#### 8. 最终验收场景

至少覆盖：

```text
A. 正常切主 -> AOF replay
B. 旧 Leader 脏尾部 -> CONFLICT + snapshot
C. retention 不足 -> snapshot fallback
D. UB ring 残留帧 -> reset 后隔离
E. announce 与 UB EVENTS 交叉到达
F. TCP-only 故障 -> 不发生双写
G. Leader self-fence 后 Proxy/CLI 切换到新 Leader
H. recovery 未完成时禁止普通 EVENTS
I. applied_seq 未追平时不能晋升或解除 gate
```

最终实现闭环为：

```text
固定 takeover boundary
    -> transition 原子 fence/drain
    -> recovery session 选择 replay/snapshot
    -> UB reset barrier 隔离旧帧
    -> 双通道 heartbeat 检测 owner 可达性
    -> Leader self-fence 保证单一可写 owner
    -> Proxy/CLI 路由到新 Leader
```

### HA TCP 控制面 + UB 数据面（提前至 M11）

原计划将 TCP 控制面 + UB 数据面作为 M12 独立阶段。经评估，该拆分与 M11 的切主恢复
闭环存在强耦合：控制帧优先级、UB ring reset/rebind 和断线恢复均需要双通道基础设施。
因此将此项提前至 M11，与剩余工作同步落地。

#### 决策记录

1. **不复用 proxy 一次性 TCP 控制 Loop**：`vemb_v16_control_inject_fd()` 采用
   detached pthread + 单次 read-dispatch-reply-close 模型，协议头为
   `vemb_v16_net_hdr_t`（32 字节，magic/version/type/flags/payload_len），
   与 Replica 的长连接、多轮帧交互和 `tlc_ha_replica_frame_wire_t` 不兼容。
   **复用 `vemb_v16_net.h` 网络工具函数**（`_connect`、`_listen`、`_read_full`、
   `_write_full`、`_set_tcp_nodelay`、`_set_timeouts`）和 6379 sniff routing
   入口（`vemb_async_peek_handler()` 中新增 Replica ATTACH magic）。

2. **去除 STREAM transport**：现有 `TLC_HA_REPLICA_TRANSPORT_STREAM` 等价于
   "所有帧走 TCP fd"，是 TCP+UB 设计的退化形式。去除后统一为 TCP 控制面 + UB
   数据面，减少 `replica_send_frame()` 和 `replica_read_frame()` 的条件分支。

3. **Heartbeat 归入控制面**：现有 `replica_frame_is_control()` 将 HEARTBEAT 归为
   数据帧。新设计中 HEARTBEAT 改走 TCP 控制面，保证心跳不受 UB ring 故障阻塞。

#### 通道分类

```text
TCP 控制面（blocking fd，长连接）:
    HEARTBEAT、LEADER_ANNOUNCE、ROLE_ACK、
    RESYNC_REQUEST、SNAPSHOT_INSTALLED、RESYNC_ABORT、
    TAIL_REQUEST、RESYNC_ACK、
    HANDOFF_COMMIT、HANDOFF_ACK、RESYNC_REQUIRED

UB 数据面（共享内存 MPSC ring）:
    EVENTS、ACK、
    SNAPSHOT_BEGIN、SNAPSHOT_CHUNK、SNAPSHOT_END、
    TAIL_EVENTS、TAIL_END
```

TAIL_END 与 TAIL_EVENTS 走同一个 UB 通道。TAIL_END 在最后一个 TAIL_EVENTS 之后
发送，Follower 收到后校验 applied_seq/durable_seq 是否追平。若拆到不同通道，TCP
上的 TAIL_END 可能先于 UB 上的 TAIL_EVENTS 到达，导致校验失败。

TCP 控制面握手成功、term 和 epoch 校验通过后，才允许数据面 UB ring 活动。UB ring
故障只影响数据面，不阻塞切主通知和心跳。

#### 实现步骤

**Step 1：Config/Struct 重构**

- 删除 `tlc_ha_replica_transport_t` 枚举和 `config.transport` 字段
- 控制面 endpoint 由 Replica 内部管理：每个节点 bind 自己的
  `control_bind_host:control_bind_port`，主动节点连接
  `peer_advertised_host:peer_control_port`
- `config.tx_ring` / `config.rx_ring` 变为必选项（不再条件初始化）
- `struct tlc_ha_replica` 保留内部活动 `control_fd`，新增 bind/peer endpoint 状态
- 删除 `stream_write_mutex`（TCP 控制面为单 writer；数据面由 UB ring CAS 保护）

**Step 2：发送路径拆分**

- `replica_send_frame()` 根据帧类型决定通道：
  - 控制帧 → `replica_send_control_frame()` → blocking TCP write via 内部 `control_fd`
  - 数据帧 → `replica_send_data_frame()` → UB ring reserve/publish
- `replica_frame_is_control()` 更新：HEARTBEAT 改为 true
- `control_send_pending` 机制保留，用于 TCP 控制帧优先级
- 控制帧 TCP 写失败触发 `replica_mark_disconnected()`；数据帧 UB 失败触发
  `replica_stop_signal()`

**Step 3：接收路径拆分**

- `replica_listener_main()` 拆为两个线程：
  - `replica_control_listener_main()` — blocking TCP read，处理控制帧
  - `replica_data_listener_main()` — UB ring poll，处理 EVENTS/ACK/snapshot 数据帧
- 控制 listener 的 TCP EOF/错误 → `replica_mark_disconnected()`，等待 reconnect
- 数据 listener 的 UB poll 失败 → `replica_stop_signal()`
- 控制线程在断线后自动 accept/connect 并替换内部活动 fd，不影响 UB ring
- 初始建连或重连期间 `connected=false` 不计入 heartbeat timeout；只有已建立
  控制长连接后的连续超时样本才推进 SUSPECT/CANDIDATE。

**Step 4：清理与简化**

- 删除所有 `replica->transport == TLC_HA_REPLICA_TRANSPORT_STREAM` 条件分支
- 删除 `stream_write_mutex` 及其初始化/销毁
- `connected` 状态仅反映 TCP 控制面连接状态
- 线程数从 5 增至 6：sender、control_listener、data_listener、apply、heartbeat、
  controller

### TODO：定位单测约 1 秒耗时

当前 `tlc_ha_replica_ut` 实测总耗时约 1.1 秒，但 ACK 轮询本身不是固定 10 秒等待。
需要继续拆分以下耗时来源并记录每段实际耗时：

- `run_network_test()` 中用于 abort/cleanup 场景的显式 `usleep(500000)`；
- 各类 `wait_for_value()`、`wait_for_cold_durable()` 和 ACK 轮询的实际重试次数；
- COLD append/fsync、snapshot chunk 发送、tail apply、handoff ACK 的处理时间；
- heartbeat timeout 测试中的 100ms 等待，以及 reconnect/promotion 的线程调度延迟。

后续应给测试增加单调时钟分段统计，输出 snapshot、tail、ACK、handoff、heartbeat 和
cleanup 各阶段耗时；在确认协议已完成后，删除不必要的固定 sleep，保留有界条件等待。

## 13. 验收标准

至少覆盖：

1. 单次 heartbeat 丢失不会切主；
2. 连续超时经过 backoff 后才进入 CANDIDATE；
3. 合法 heartbeat 可以清除 SUSPECT；
4. 新 Leader 先持久化 term，再开放写入；
5. `LEADER_ANNOUNCE` 可以使恢复中的旧 Leader 进入 FENCED/Follower；
6. 旧 term EVENT、ACK、heartbeat 不得回退状态或刷新 liveness；
7. 切主不需要重新创建 sender/apply/receiver 线程；
8. 旧 Leader 恢复后只能通过 replay 或 snapshot resync 追平；
9. 未复制 AOF 尾部被明确记录为可能丢失；
10. 单个 Node group 切换不改变其他 Node group owner；
11. 在既定无双向网络故障模型下不会出现两个 MASTER；
12. 若未来允许网络分区，系统进入 FENCED/FAULT 或要求外部 fencing，不假设本地
    heartbeat 可以提供 quorum 安全。
