# HPC-Redis HA 数据同步设计

本文独立描述 HPC-Redis 双副本 HA 的数据同步设计。它补充
`TLC_2X3_HA_ARCHITECTURE.md` 中的同步边界，但不改变当前的基本定位：

```text
HA      = 两个在线副本
COLD    = 每个副本本地的 AOF/RDB 持久化层
同步    = Leader COLD AOF event -> Follower COLD AOF -> Follower apply
```

本设计采用异步复制，语义接近原生 Redis 的普通主从复制：Leader 可以在
Follower 暂时不可用或落后时继续接受本地写入；Leader 整机或存储不可恢复时，
尚未在 Follower 上持久化的尾部 event 可能丢失。

## 1. 设计边界

### 1.1 复制拓扑

每个 `HPC-Redis Node` 对应一个固定的副本对：

```text
Node group n:

    replica-A <-----------------> replica-B
                  fixed pair
```

一个 Node group 只包含一个 HPC-Redis Leader 和一个 HPC-Redis Follower，服务一个
独立的 key 数据集。一个 HPC-Redis 实例只属于一个 Node group，不在同一实例内
复用或承载其他 Node group 的复制状态。

Replica 的 UB mapped path 可以与 Server 的 UB RPC 使用同一个固定 path，但每个
Node group 的两个复制方向必须使用独立且不重叠的 `mmap_offset` 区域。offset、
区域大小和方向由启动配置固定，不能在运行时把一个 Node group 的 ring 映射到
另一个 Node group 或 RPC region。

复制通道是一对一的专用通道，不是由第三方在每次同步时动态选择
`source_node` 和 `destination_node` 的总线。当前 HA role 决定方向：

```text
Leader -> Follower
```

发生角色变化后，方向才反转。每个副本的 COLD、AOF、复制游标、传输队列和
进度状态均按 `hpc_node_id` 独立维护；不同 Node group 之间不共享 seq、ACK 或
队列。

不同 Node group 对应互斥的 key 数据集。HA 数据同步覆盖所有 Node group，但每个
Node group 独立执行自己的 Leader -> Follower 复制，多个 Node group 可以并行
同步；不需要跨 Node group 共享复制日志、seq、ACK 或 safe point。

这里的“独立”是资源和状态上的完整隔离：每个 Node group 都有自己的一对
Leader/Follower、COLD AOF、seq 空间、复制队列、UB data channel/ring、durable ACK、
heartbeat/liveness、resync 状态和 HA role/term。任何一个 Node group 的 event、
连接、队列、进度或故障状态都不能被另一个 Node group 消费、合并或推进；一个组的
断链、重连、resync 或 failover 也不应暂停或改变其他组的复制。

`hpc_node_id` 是副本实例和固定 Node group 的上下文，不是 AOF event 的业务字段，
也不是每条复制消息由外部控制器选择的路由字段。固定通道仍应校验对端身份，
以防止配置错误把一个 Node group 的日志接入另一个 Node group；该校验不改变
复制拓扑的固定一一对应关系。

### 1.2 非目标

本设计不提供：

- 跨不同 key 数据集/Node group 的事务原子性（这不影响各 Node group 独立完成 HA 复制）；
- 跨 metadata/key shard 的事务原子性；
- 两副本 quorum commit；
- Leader 故障时的零数据丢失保证；
- WARM slot、物理地址或本地 location 布局的复制一致性。

Follower 可以为同一个逻辑 key 选择不同的本地 slot 和 WARM placement，但逻辑
key/value/version 状态必须按相同 event 顺序重建。

### 1.3 扩容和 ownership 迁移

扩容采用类似 Redis Cluster 的分层模型：复制只负责一个 Node group 内的
Leader -> Follower，ownership/slot 迁移由拓扑控制面单独负责。新增 Node group
不会修改已有 Node group 的 seq、ACK、复制队列或 AOF 历史：

```text
Node group A:  Leader A -> Follower A'
Node group B:  Leader B -> Follower B'
```

当 key/slot 从 Node group A 迁移到 Node group B 时，不能把两个 Node group
合并成一条复制日志。迁移数据流为：

```text
1. Leader A 将逻辑 key/value/version 导入 Leader B。
2. Leader B 将导入结果写入自己的 COLD AOF，并传播给 Follower B。
3. Target Group 完成导入确认后，拓扑控制面执行 CUTOVER，Source Leader
   不再接受该 key 的旧 owner 请求。
4. Source Leader 对迁出 key 追加普通 `DEL` event，并按正常 HA 复制流传播给
   Follower A，使 Source Group 的两个副本收敛到相同 key-set。
5. 拓扑控制面发布新的 ownership/topology_epoch；Source 后台再执行 SOURCE_GC。
```

迁移完成后的收敛状态是：

```text
Node group A 的 Leader/Follower：不再持有迁移出的 key/slot
Node group B 的 Leader/Follower：持有迁移后的 key/slot
```

源组的 Replica 不直接从目标组接收迁移数据；它跟随源 Leader 的普通 `DEL` 事件
收敛。迁移控制阶段不会编码为 HA AOF/Replica 控制事件。
目标组的 Replica 不直接从源组接收复制日志；它跟随目标 Leader 的导入/写入
事件收敛。源组和目标组分别维护自己的 seq、durable ACK、replicated_seq 和
safe point，不共享这些状态。

迁移期间，source/importing 状态、请求转发、CUTOVER 顺序和 SOURCE_GC 时机由
拓扑控制面保证。CUTOVER 后 Source Leader 对旧拓扑请求返回 `MOVE`（或由路由
层直接转发到新 owner），不再接受该 key 的旧 owner 写入；复制层只负责持久化和
传播各自 Leader 已接受的普通逻辑 event。迁移失败时不得跳过
源或目标组的 event gap，应按迁移协议重试、回滚未确认的导入，或重新执行
snapshot + delta。迁移完成后，新的 ownership 必须绑定新的
`topology_epoch`；`ha_term` 仍只表示各 Node group 的 owner 任期，不表示
跨 Node group 的迁移进度。

## 2. 同步数据模型

### 2.1 COLD AOF event

Leader 的本地写路径将客户端操作转换成规范化的内部逻辑 event。复制的不是
CLI 原始请求，也不是 Primary 的 WARM slot 操作。

```text
ha_term
topology_epoch
seq
op
meta_shard_id
key
value
version
checksum
```

字段语义：

| 字段 | 语义 |
|---|---|
| `ha_term` | 当前 owner 任期/fencing 标识；旧 `ha_term` 的 event 必须拒绝 |
| `topology_epoch` | 数据/迁移拓扑版本，用于路由和拓扑上下文校验；不承担 owner lease/fencing 职责 |
| `seq` | 该 Node group 内永久单调递增的逻辑事件序号 |
| `op` | 逻辑操作，例如 `PUT`、`DEL` 或规范化后的 `VADD` |
| `meta_shard_id` | event 所属的本地 metadata 分区 |
| `key` | 规范化 key 编码 |
| `value` | 完整逻辑 payload；删除 event 可为空并携带 tombstone 语义 |
| `version` | key/object 逻辑版本，用于旧更新和冲突检查 |
| `checksum` | 覆盖规范化 header 与 payload 的完整性校验值 |

event 不包含 `hpc_node_id`。event 已经写入某个固定 Node group 的 COLD AOF，
其 Node 身份由本地 COLD 实例、AOF 路径和固定复制通道上下文确定。

### 2.2 seq 规则

每个 Node group 的 `seq` 满足：

```text
seq 从 1 开始（或由已有日志恢复出的下一个值开始）
跨 ha_term 永久单调递增
ha_term 切换不重置 seq
不允许 gap，不允许复用已使用的 seq
```

`ha_term` 用于拒绝旧 owner 的 event，`topology_epoch` 用于校验数据所属的拓扑
上下文，`seq` 用于顺序、连续性和幂等。由于 seq 跨 `ha_term` 不重置，日志排序
只需要按 seq 进行；实现仍必须校验 event 的 `ha_term` 和 `topology_epoch` 是否
符合当前复制上下文。

`ha_term` 与 `topology_epoch` 必须分开持久化和校验。旧实现曾将 AOF event header
的 `term` 字段复用为 `topology_epoch`；采用本设计后，AOF event header 不再用一个
字段同时表达这两个概念。

### 2.3 进度定义

每个 Node group 独立维护以下进度：

```text
leader_appended_seq
leader_durable_seq
follower_durable_seq
replicated_seq
applied_seq
ha_safe_point_seq
```

```text
leader_appended_seq = Leader AOF 已成功 append 的最高连续 seq
leader_durable_seq  = Leader AOF 已刷盘的最高连续 seq
follower_durable_seq= Follower AOF 已刷盘的最高连续 seq
replicated_seq      = Leader 根据有效的 Follower durable ACK 或 heartbeat durable
                      报告记录的最高连续 seq
applied_seq         = Follower HA/WARM state 已应用的最高连续 seq
ha_safe_point_seq   = min(leader_durable_seq, follower_durable_seq)
```

`replicated_seq` 是 Leader 对 Follower durable 前缀的观测记录，不是另一个独立的
日志来源；该观测可以由经过校验的 durable ACK 和 heartbeat durable 报告共同推进。
`applied_seq` 只表示在线状态应用进度，不能冒充 AOF 刷盘确认。

### 2.4 `ha_term` 持久化

`ha_term` 是 Node group 的 owner 任期/fencing 版本，必须与数据拓扑版本
`topology_epoch` 分开处理。为避免重启或切主后复用旧任期，至少持久化在：

```text
owner/term metadata:
    current_ha_term、current owner

AOF event header:
    ha_term、topology_epoch、seq

checkpoint generation header:
    ha_term、checkpoint_seq
```

独立 owner/term metadata 保存当前最新任期；AOF event 的 `ha_term` 用于拒绝
旧 owner 的延迟 event；checkpoint 的 `ha_term` 用于确认 snapshot 属于哪个
任期。checkpoint manifest 也必须能关联到 generation 的 `ha_term`，不能只依赖
内存状态。

切主时，新 Leader 必须先计算并持久化新任期，再开放写入口：

```text
new_ha_term = max(local_persisted_ha_term, last_seen_peer_ha_term) + 1
    -> 持久化并 fsync owner/term metadata
    -> 完成 fencing
    -> 成为 Leader 并接受写入
```

当前故障模型假设不存在网络分区和双副本同时故障，因此 `ha_term` 不承担
quorum 或故障检测职责；它仍用于切主、重连、重启后隔离旧 owner 的 event 和
heartbeat。若未来允许网络分区，仅依靠本地自增 `ha_term` 不能保证绝不双主，
仍需要 lease、witness 或外部 fencing。

## 3. Leader 写入路径

单条写入的逻辑顺序如下：

```text
Client
    -> Proxy/SuperNode/TLC
    -> Leader 写入协调逻辑校验 owner/ha_term/topology_epoch/route
    -> 构造规范化 event 和不可见 WARM staging
    -> COLD AOF append queue
    -> 唯一 AOF writer 分配 seq 并 append
    -> 推进 leader_appended_seq
    -> local flush coordinator 执行 group commit/fsync
    -> 推进 leader_durable_seq
    -> 发布本地 HA/WARM state
    -> 返回 LOCAL_DURABLE_ACK
```

请求线程不等待 Follower。`LOCAL_DURABLE_ACK` 只承诺 Leader 本地 COLD AOF 已
达到持久化点；Follower 的接收、刷盘和 apply 不属于客户端成功响应的前置条件。

AOF append 失败或本地刷盘失败时，不能返回成功。WARM 发布失败时，已持久化的
AOF event 必须保留，后续通过 replay 或幂等重试重建 WARM/HA state。

## 4. 正常增量同步

### 4.1 复制来源

复制 cursor 从 Leader 的 COLD AOF 读取 event。内存 replication queue 只是有界
传输缓冲，不是复制的权威存储：

```text
Leader AOF
    -> replication cursor
    -> bounded replication queue
    -> fixed Follower channel
```

正常增量复制的上界可以使用 Leader 当前已 append 的最高连续 seq：

```text
[follower_durable_seq + 1, leader_appended_seq]
```

这允许复制已经 append、但尚未在 Leader 上 fsync 的 event，从而降低复制延迟。
这些 event 在 Leader 失效时可能只存在于 Follower；这不违反异步复制语义，但
客户端不能将其误解为已经达到 Leader 本地 durable 边界。

### 4.2 batch 规则

复制允许按 event 条数、总字节数或发送窗口组装 batch：

```text
Follower durable_seq = 100
Leader appended_seq  = 1000

batch-1 = [101, 200]
batch-2 = [201, 500]
batch-3 = [501, 1000]
```

batch 只改变传输粒度，不改变 event 顺序。每个 batch 必须表示连续区间，
Follower 不得把未来 seq 当作已提交，也不得跳过中间缺口。

### 4.3 Follower 接收顺序

Follower 对 batch 执行：

```text
校验 event 长度、ha_term、topology_epoch、seq、版本和 checksum
    -> 检查 first_seq 是否为 local_durable_seq + 1
    -> 重复的已持久化区间按 checksum 幂等确认
    -> 中间有缺口则暂停并请求 expected_seq
    -> 进入 Follower AOF append queue
    -> Follower 唯一 AOF writer 按原 seq append
    -> local flush coordinator 刷盘
    -> 推进 follower_durable_seq
    -> 返回 durable ACK
    -> 后台 apply 到本地 HA/WARM state
    -> 推进 applied_seq
```

Follower 不为复制 event 重新分配 seq。相同 seq 再次到达时，payload/checksum
必须一致；同一 seq 出现不同内容属于数据冲突，必须停止该 Node group 的同步并
报警。已发送 durable ACK 的 event 如果后续 apply 失败，仍须保留在 AOF 中，
进入恢复或重试流程，不能伪造回滚 ACK。

### 4.4 ACK

Follower ACK 只表示：

```text
Follower AOF 已刷盘到 durable_seq
```

ACK 只确认连续前缀，不确认“最后收到的网络包”。Leader 收到 ACK 后推进其
durable 观测进度；Follower 的 `applied_seq` 可作为服务就绪和恢复进度指标，
但不能代替 durable ACK。

ACK 丢失时，Leader 从 Follower 最近确认的连续 seq 重新发送。Follower 使用
seq 和 checksum 幂等处理，不生成新的 event。

### 4.5 Leader/Follower 心跳

复制通道除 event batch 和 durable ACK 外，必须周期性交换固定大小的 heartbeat
帧。心跳用于在没有写入或 ACK 的安静窗口中确认固定副本通道仍然可用，并传播
当前 ha_term、角色、健康状态和同步进度。心跳本身不触发 Follower flush，也不产生
新的 AOF 持久化确认；合法 heartbeat 中的 durable 报告可以与 ACK 合并，补足
丢失或延迟的 ACK。

Leader 和 Follower 使用同一种 heartbeat 帧格式，帧内的 `role` 决定
`durable_seq` 和 `progress_seq` 的具体语义：

```text
Heartbeat:
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

以上字段构成本文 TLC 数据同步的规范 heartbeat schema；`leader_appended_seq`、
`ha_safe_point_seq` 以及各类 lag/health 细分项属于本地统计或可选诊断信息，不是
heartbeat 的必需 wire 字段。

字段语义：

| 字段 | 语义 |
|---|---|
| `hpc_node_id` | 发送方所属的固定 HPC-Redis Node group；不是 AOF event 字段，用于防止跨 Node group 串线 |
| `peer_node_id` | 固定副本对中的目标节点，用于校验心跳是否发给正确对端 |
| `ha_term` | 当前 owner 任期/fencing 标识；旧 `ha_term` 的心跳不能更新 peer 状态或恢复复制 |
| `role` | 发送方当前角色（`Leader`/`Follower`），也决定 `progress_seq` 的含义 |
| `health` | 发送方 COLD、复制和在线状态的健康结果，例如 `HEALTHY`、`DEGRADED`、`RECOVERING`、`FAILED` |
| `sent_at_ns` | 发送方使用单调时钟记录的发送时间，仅用于诊断；超时使用接收方本地时间 |
| `durable_seq` | 发送方本地 COLD AOF 已刷盘的最高连续 seq；Leader 时等于 `leader_durable_seq`，Follower 时等于 `follower_durable_seq` |
| `progress_seq` | 角色相关的复制进度；Leader 时等于 Leader 已记录的 Follower durable ACK 进度（`replicated_seq`），Follower 时等于已应用到本地 HA/WARM 状态的进度（`applied_seq`） |
| `checksum` | 覆盖固定帧 header 和 payload 的完整性校验值；校验失败的帧不得更新 liveness 或进度 |

`leader_appended_seq` 不放入 heartbeat。它是 Leader 的本地 AOF append 上界，
Follower 不需要依赖它；Leader 可通过复制 batch 的 `first_seq/last_seq` 和本地
统计获得该值。`ha_safe_point_seq` 也不放入 heartbeat，它是派生值：

```text
ha_safe_point_seq = min(leader_durable_seq, follower_durable_seq)
```

两端在收到对方 durable 进度后本地计算 safe point，避免把派生值变成额外的
协议状态。Follower 的 durable ACK 和 heartbeat 中的 `durable_seq` 都是对同一
个本地 durable 前缀的报告；在身份、checksum 和 ha_term 校验通过后，Leader 可以
用两者共同更新观测值：

```text
replicated_seq = max(last_valid_durable_ack_seq,
                     last_valid_follower_heartbeat_durable_seq)
```

ACK 与 heartbeat 乱序或重复到达时不得回退进度。`progress_seq` 仍不能替代
durable 进度：Leader 发送的 `replicated_seq` 仅用于诊断，Follower 不能据此
伪造自己的 `follower_durable_seq`；Follower 发送的 `applied_seq` 不能代替
durable ACK 或 `durable_seq`。

Heartbeat 由双方按固定周期发送（建议默认 1 秒），接收端使用本地单调时钟记录
`last_heartbeat_received_ns`。`sent_at_ns` 只用于诊断，不能用于跨机器计算超时或
延迟。超过 `heartbeat_timeout`（建议为 3 个周期）没有收到合法帧时，将对端标记为
`UNAVAILABLE`，停止依赖该副本推进 `replicated_seq/ha_safe_point_seq`，并触发
复制重连或告警。恢复后必须先收到合法
当前 ha_term 的 heartbeat，再恢复增量复制。

接收端必须校验固定副本身份、帧版本/长度、checksum、ha_term 不能回退，以及同一
ha_term 下 `durable_seq/progress_seq` 不能回退。ha_term 不用于提供 quorum 或故障检测；
在“最多一个副本故障且不存在网络分区”的前提下，它仍用于切主、重连和重启后
隔离旧 owner 的延迟 event/heartbeat。ha_term 发生切换时允许角色改变，但仍须
执行 fencing 和 event ha_term/topology_epoch 校验。心跳超时不能删除 AOF、跳过 GAP
或改变 event 的 ha_term/topology_epoch/seq。

## 5. 从 AOF 读取和 `seq -> offset` 索引

### 5.1 AOF 是复制重放的权威来源

“重新读取 AOF”是指从 Leader 本地 AOF segment 中按 seq 定位并读取 event，
不是从头扫描全部日志，也不是只依赖内存中的 Raft-like storage。

```text
AOF segment（持久化 event）
    -> segment 元数据
    -> seq/offset 索引
    -> replication cursor
    -> batch [seq_start, seq_end]
```

内存中可以保留：

```text
segment_id、first_seq、last_seq
seq -> file offset 索引
read-ahead batch/cache
```

这些结构只是读取加速层。复制 queue 满、连接断开或进程重启后，仍必须能够从
AOF 重建待发送 batch。

### 5.2 定位和读取

当 Follower 请求从 `requested_seq` 继续时：

```text
1. 根据 seq 范围定位目标 AOF segment。
2. 使用 seq -> offset（或最近的稀疏索引 offset）定位起始位置。
3. 使用 pread 或等价顺序读取 event。
4. 校验 event framing、ha_term、topology_epoch、seq 和 checksum。
5. 组装有界 batch 并发送。
6. 等待 Follower durable ACK 后继续下一连续区间。
```

索引不必为每条 event 保存完整 offset。可以使用稀疏索引：

```text
每 N 条 event 保存一个 seq -> offset
```

请求未命中精确索引时，从最近的稀疏索引开始顺序扫描少量 event。这样索引内存
开销与 segment 数和索引密度相关，而不是与历史 event 总数线性增长。

### 5.3 segment 生命周期和并发保护

AOF segment 的生命周期至少包含：

```text
ACTIVE -> SEALED -> RETAINED -> COMPACTED/DELETED
```

- `ACTIVE` 由 AOF writer 追加，不能被 compactor 原地重写。
- `SEALED` 后才能参与 compact 和离线读取索引构建。
- replication cursor 正在读取的 segment 必须被 pin/ref 保护。
- 被 pin 的 segment 不能删除或替换；cursor 完成读取后释放引用。
- 删除或重写 segment 时，关联 index 必须同步替换或释放。

## 6. 断线续传、队列背压和 GAP

### 6.1 断线续传

Follower 断线后保留自己的 `follower_durable_seq`。重新建立固定副本通道后，
从以下位置继续：

```text
requested_seq = follower_durable_seq + 1
```

如果该 seq 仍在 Leader AOF 的保留范围内，Leader 通过索引定位并增量补发。
网络重试不能改变 event 的 ha_term/topology_epoch/seq，也不能从 AOF 删除未 ACK 的 event。

### 6.2 队列满的含义

复制 queue 满表示网络发送或 Follower 消费速度低于 Leader 产生 event 的速度，
不表示 event 可以被丢弃：

```text
暂停或降低网络发送
    -> 保留 Leader AOF
    -> queue 空间释放后继续发送
    -> 或从 follower_durable_seq + 1 重新从 AOF 构造 batch
```

queue 可以清空和重建，因为它不是日志存储；AOF 必须保留复制和本地恢复所需的
连续区间。不能覆盖未发送 event，不能伪造 ACK，也不能因为 queue 满而删除 AOF。

Follower 长时间落后时，复制 lag 和 AOF 占用持续增长。达到资源保护阈值后，
可以暂停该 Node group 的新写入或进入降级，但不能静默丢弃已经返回
`LOCAL_DURABLE_ACK` 的 event。

### 6.3 GAP

Follower 收到的 `first_seq` 大于期望值时，必须返回缺口：

```text
expected_seq = follower_durable_seq + 1
received_seq  > expected_seq
    -> GAP(expected_seq)
    -> 暂停后续 apply
    -> Leader 从 expected_seq 重发
```

如果收到的 seq 早于当前连续前缀，必须检查 checksum 后幂等确认；如果 checksum
不一致，视为数据冲突，停止自动复制。

## 7. Snapshot resync

### 7.1 触发条件

出现以下任一情况时，Follower 需要全量重同步：

- 请求的 seq 早于 Leader 的 `aof_retain_from_seq`；
- Leader 已 compact 掉 Follower 所需的 AOF 区间；
- Follower 本地 checkpoint/manifest 校验失败；
- Follower AOF 存在不可恢复的 gap 或 checksum 冲突；
- Follower 需要重新建立完整状态。

不能通过跳过缺失 seq 的方式继续增量复制。

### 7.2 固定同步边界

Snapshot resync 选择一个已经发布且校验有效的 checkpoint generation，并固定：

```text
B = snapshot 开始时 Leader 的 leader_durable_seq
```

`B` 是一次 resync 的固定边界，不是持续变化的最新 seq。snapshot 传输期间
Leader 可以继续写入，新增 event 的 seq 会超过 `B`，但不混入本次 snapshot。

### 7.3 resync 流程

```text
1. Leader 选择 active checkpoint generation。
2. 记录每个 meta/key shard 的 captured_seq。
3. 固定 B = leader_durable_seq。
4. Leader 通过 Replica channel 搬运完整 checkpoint blob；Follower 校验文件长度、
   分区数量、摘要和 checksum。当前代码以 `tlc_cold_export_checkpoint()` /
   `tlc_cold_import_checkpoint()` 提供同一格式 blob 的导出/导入抽象。
5. Follower 仅在空的 `RECOVERING/FENCED` COLD runtime 上安装 generation；导入校验
   或发布失败时保留已有有效 generation，不覆盖当前 manifest。
6. Follower 对每个 shard replay：
       [captured_seq_of(shard) + 1, B]
7. Follower 将 replay 的 AOF tail 刷盘，并报告 durable_seq = B。
8. Follower 从 B + 1 开始恢复普通增量复制。
```

其中 AOF tail 是逻辑范围，不一定对应单个物理文件：

```text
tail(shard) = [captured_seq_of(shard) + 1, B]
```

resync 未完成前，目标副本处于 `RECOVERING/FENCED`，不能使用半完成的
checkpoint 提供服务。snapshot、AOF tail 和 manifest 都必须完成校验后再发布；
失败时保留旧 generation，不覆盖正在使用的有效状态。

## 8. AOF compact 与索引同步回收

### 8.1 compact 边界

本设计依靠 checkpoint + AOF tail 进行 snapshot resync，因此 AOF 保留边界主要
由本地可恢复 checkpoint 决定，而不是无限等待 Follower ACK：

```text
retained_generations = 保留的有效 checkpoint generation
checkpoint_floor_seq  = min(保留 generation 的 checkpoint_seq)
aof_retain_from_seq   = checkpoint_floor_seq + 1
```

Follower 落后到保留范围之外时，转为 snapshot resync。`ha_safe_point_seq` 仍然
用于表示双副本持久化安全边界、故障丢失窗口和监控状态，但不阻塞本地 AOF compact；
compact 由保留的 checkpoint generation 决定，超出保留范围的 Follower 必须走
snapshot resync。

### 8.2 完整 segment 回收

当完整 segment 被 compact 删除时，必须同步处理其索引：

```text
1. 计算新的保留 generation 和 AOF 边界。
2. 原子发布新的 compact/catalog 元数据。
3. 删除超出边界的完整 AOF segment。
4. 删除对应的 segment index，释放内存。
5. fsync 目录并记录回收结果。
```

不能出现 segment 已删除、但 `seq -> offset` 仍指向旧文件的状态。

### 8.3 segment 内部重写

如果 compact 边界落在 segment 内部，必须生成新的 segment 和新的 index：

```text
旧 segment: [1, 1000]
保留范围:   [700, 1000]

新临时 segment: [700, 1000]
新 index:       seq -> 新 segment offset
```

发布顺序为：

```text
写临时 segment
    -> 写新 index
    -> fsync segment/index
    -> 原子发布 catalog
    -> 删除旧 segment 和旧 index
    -> fsync 目录
```

compactor 不得原地重写 AOF writer 当前使用的 ACTIVE segment，也不得替换仍被
replication cursor pin 的 SEALED segment。

### 8.4 重启重建

进程重启后，内存索引可以从已发布的 AOF segment 元数据和 segment 内容重建：

```text
读取 manifest/catalog
    -> 扫描保留 segment header 和 event framing
    -> 校验 seq 连续性和 checksum
    -> 重建 segment seq range
    -> 重建稀疏 seq -> offset index
```

如果发现尾部 partial event，可以按 AOF 恢复规则截断；中间的 gap、坏 checksum
或 seq 冲突必须停止恢复并报警，不能跳过后继续复制。

## 9. 失败和数据一致性边界

### 9.1 可接受的数据丢失

异步复制下，Leader 已本地持久化但尚未被 Follower 持久化的尾部可能在 Leader
整体不可恢复时丢失：

```text
Leader durable_seq   = 1000
Follower durable_seq = 980

可能丢失范围：[981, 1000]
```

这与原生 Redis 普通异步 replication 的切主数据边界相同。系统必须暴露 lag，
不能宣称默认两副本无损提交。

### 9.2 必须长期成立的不变量

```text
Follower durable_seq 不得跨越 seq gap
Follower 不能接受旧 ha_term event
同一 seq 的 payload/checksum 必须稳定
Follower durable ACK 只能确认本地 AOF 已刷盘
replication queue 不是可靠日志
compact 不能删除本地恢复所需的 AOF 区间
compact 必须同步替换/释放 seq -> offset index
```

### 9.3 可观测性

每个 Node group 至少应暴露：

```text
leader_appended_seq
leader_durable_seq
follower_durable_seq
replicated_seq
applied_seq
ha_safe_point_seq
replication_lag_events
replication_lag_bytes
last_sent_seq
last_ack_seq
last_gap_seq
last_checksum_error_seq
aof_retain_from_seq
checkpoint_floor_seq
segment_count
index_bytes
indexed_event_count
```

告警应区分复制断线、GAP、checksum/conflict、ha_term 过期、Follower 存储失败、
队列背压、AOF 空间不足和 snapshot resync，而不能只报告一个 lag 数值。

## 10. 设计结论

```text
1. 同步源是 Leader COLD AOF event。
2. event 不包含 hpc_node_id；Node identity 来自固定副本实例和复制通道上下文。
3. seq 在 Node group 内跨 ha_term 永久单调递增。
4. 增量同步严格连续，但允许 batch，范围为
       [follower_durable_seq + 1, leader_appended_seq]。
5. Follower 先写 AOF、刷盘并 ACK durable_seq，再 apply 在线状态。
6. 复制 queue 是可重建的传输缓冲，AOF 才是复制重放的权威来源。
7. 断线时通过 seq -> offset 从 AOF 重读并补发。
8. seq -> offset index 必须随 AOF segment compact 同步替换或释放，支持稀疏索引。
9. 增量起点已被 compact 时，使用 checkpoint + AOF tail 做 snapshot resync。
10. snapshot resync 的固定边界为 B = snapshot 开始时的 leader_durable_seq。
11. 异步复制允许 Leader 故障时丢失尚未在 Follower 上 durable 的尾部 event。
12. Leader/Follower 使用统一 heartbeat 帧；`durable_seq` 表示发送方本地持久化进度，
    `progress_seq` 按角色表示 `replicated_seq` 或 `applied_seq`；
    `leader_appended_seq` 和 `ha_safe_point_seq` 不进入 heartbeat；Leader 在校验
    通过后可将 durable ACK 与 Follower heartbeat 的 durable 进度取最大值，心跳
    超时只使用本地接收时间。
13. `ha_term` 是独立于 `topology_epoch` 的 owner 任期；它持久化在 owner/term
    metadata、AOF event 和 checkpoint generation 中，切主时递增并在开放写入前
    完成持久化。
14. 扩容和 ownership 迁移采用 Redis Cluster 式分层模型：迁移由拓扑控制面负责，
    每个 Node group 只复制自己的 Leader event；源组通过 DEL/tombstone 收敛，
    目标组通过导入/写入 event 收敛，两个 Node group 不共享 seq、ACK 或 safe point。
```
