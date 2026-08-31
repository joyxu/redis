# TLC 2xN HA 架构设计：HA 双副本与 COLD 备份层

本文重新定义 TLC 的 `COLD` 与 `HA` 边界。设计参考
`MySQL_InnoDB_log_Doublewrite.md` 中的 WAL、checkpoint 和 redo 思路，
但不照搬 InnoDB 的实现。这里的 `redo` 仅指 InnoDB 参考机制；TLC COLD
使用的是逻辑 AOF，两者职责相近但日志语义和格式不同。

核心结论：

```text
HA = 两个在线数据副本，负责服务和异步复制
COLD = 每个副本本地的 RDB/AOF 持久化层，主要负责故障恢复和灾难容错
WARM/HOT = 在线计算和索引缓存，可由 HA 或 COLD 重建
```

`COLD` 不参与普通读和计算，也不等待 Follower 响应。SuperNode/TLC 将已
接受的写入先交给本地 COLD AOF，并按成功返回策略完成本地持久化，随后由
后台复制线程异步发送到 Follower。除必要的本地 AOF append/fsync 外，COLD
的 checkpoint、压缩、原子发布和恢复扫描不运行在请求线程；Follower
尚未追平时只能反映为复制 lag。

## 1. 范围、拓扑和不变量

本文中的 `2xN` 表示两个 IDC 副本、每个 IDC 部署 `N` 个 HPC-Redis
Node。`N` 不是架构常量，由 HPC-Redis 的实际部署配置和容量规划
确定；文件名中的 `2X3` 仅保留历史命名，不代表固定的节点数。

```text
IDC 副本数 x HPC-Redis Node 数 = 2 x N = 2N 个节点 placement
```

每个 IDC 都部署全部 `N` 个 HPC-Redis Node；同一序号的节点跨 IDC
组成一组 HA primary/replica，实际数量按 `hpc_node_count` 创建：

```text
IDC-A                                      IDC-B
-------                                    -------
HPC-Redis Node[0 .. N-1]: primary == async replicate ==> replica
          |                                      |
          v                                      v
       COLD-A (async backup)                 COLD-B (async backup)
```

正常情况下 IDC-A 的 `N` 个 HPC-Redis Node 负责各自请求，IDC-B 的对应 Node
接收异步复制并保持可接管状态。也可以按 HPC-Redis Node 分散 primary，但每个
HPC-Redis Node 始终只有一个写 owner。

### 1.1 容错与切换粒度

容错粒度是 `HPC-Redis Node`，不是 IDC。每个 Node 内的 HA Agent worker 为对应
Node group 独立维护：

```text
owner_idc[hpc_node_id]
term/epoch[hpc_node_id]
appended_seq、durable_seq、replicated_seq、ha_safe_point_seq、applied_seq
MASTER/BACKUP/FAULT/RECOVERING/FENCED
master_down_timer 和 takeover 条件
```

每个 HPC-Redis Node 进程只服务一个 Node，因此进程内部和本文的 Node 流程均使用
无下标的标量 `appended_seq`、`durable_seq`、`replicated_seq` 等状态，不需要维护以
`hpc_node_id` 为索引的数组。Proxy、控制器或运维汇总视图通过外部的
`hpc_node_id -> Node progress` 关联多个 Node 的状态。

因此，某个 HPC-Redis Node 在 IDC-A 上故障时，只把该 HPC-Redis Node 的 owner 切换到
IDC-B 对应副本：

```text
HPC-Redis Node n:  IDC-A FAULT -> IDC-B CANDIDATE -> IDC-B MASTER
HPC-Redis Node m:  IDC-A MASTER                         （不受影响）
HPC-Redis Node k:  IDC-B BACKUP                         （不受影响）
```

路由表应表达为 `hpc_node_owner[hpc_node_count]` 或等价的动态结构。IDC 级故障
只是同时使该 IDC 承载的多个 HPC-Redis Node 分别进入故障处理流程，不得把健康 HPC-Redis Node
 强制切换或暂停。

fencing 也应优先按 HPC-Redis Node owner 或对应服务进程实施；只有无法进行细粒度
隔离时，才使用 IDC 级 STONITH。切换发布必须原子更新目标 HPC-Redis Node 的 owner
和 epoch，不能因为一个 HPC-Redis Node 切换而覆盖其他 HPC-Redis Node 的路由。

### 1.2 HPC-Redis Node 的具体粒度

本文中的 `HPC-Redis Node` 是一个 HPC-Redis **节点级服务单元**，由部署配置的
`hpc_node_count` 和节点放置规则确定。`2x3` 中的 `3` 就是每个 IDC 的三个
HPC-Redis Node；实际部署可以是 `2xN`。它与代码中的 `key shard` 不是同一层：

```text
key
  -> 代码内部 key_shard_id = key route(hash(key))
  -> hpc_node_id = placement(key_shard_id, hpc_node_count)
  -> hpc_node_owner[hpc_node_id]（HPC-Redis Node）
  -> 该节点在当前 owner IDC 的 WARM/HA/COLD
```

上述目标选择由 Proxy 依据已发布的路由元数据完成，并随转发请求携带
`hpc_node_id`、owner/term 和路由版本。SuperNode/TLC 不重新选择目标 Node，
而是将请求交给该 Node primary 进程内的写入协调逻辑；该逻辑负责校验路由和
当前写 owner，发现路由过期或本地不具备写权时返回刷新/重试。CLI 只连接统一
的 Proxy 地址，不需要直接感知后端 IDC 或 HPC-Redis Node 的物理地址。

一个 HPC-Redis Node 承载一组 key 的 metadata、WARM 位置和向量数据；它不是：

```text
单个 key                 （HPC-Redis Node 包含很多 key）
单个 WARM slot           （slot 是 HPC-Redis Node 内的物理缓存位置）
整个 IDC                 （一个 IDC 承载多个 HPC-Redis Node）
```

因此每个 HPC-Redis Node 独立维护 COLD 的 `AOF`、`RDB checkpoint`、`appended_seq`、
`durable_seq`、`replicated_seq`、`ha_safe_point_seq`、`owner` 和恢复状态，都按 `hpc_node_id` 独立维护。
故障只影响该 HPC-Redis Node 的 key 集合；故障切换、日志追赶和 COLD 恢复也只处理
该 HPC-Redis Node，不要求整个 IDC 或所有 HPC-Redis Node 同时停顿。

必须长期成立的约束：

1. 成功返回的写入至少已经达到本地 COLD AOF 的约定持久化点；Follower
   的复制确认是异步的，不是客户端提交前置条件。
2. 普通读只读 HA/WARM，不读 COLD；COLD 读只用于恢复、校验和运维导出。
3. COLD checkpoint/AOF lag 和 Follower replication lag 不改变当前 owner；
   它们决定故障切换时可保留的数据边界。
4. WARM/HOT 不是持久性权威，丢失后可以从本地 COLD AOF/RDB 或 peer 重建。
5. 没有 fencing/lease 时，网络分区只能停止写入，不能安全地自动双主。

## 2. COLD 层设计（组件和流程）

COLD 是每个 HPC-Redis Node 上的本地恢复子系统，按 `hpc_node_id` 保存 RDB checkpoint 和
AOF 追加日志。它不提供普通请求读或计算；写入只作为持久化事件接收，
不参与选主；它的本地 AOF 是
该节点写入的持久化边界，Follower 复制则完全异步。COLD-A 和 COLD-B
分别维护自己的恢复文件；两者不能替代 HA 在线副本或异步复制协议。

Proxy 是 CLI 的统一接入和转发层；它依据已发布的路由元数据把请求转给对应的
SuperNode/TLC。SuperNode/TLC 收到 Proxy 转发的请求后，进入目标 HPC-Redis
Node 的本地写路径。待写 event payload 由目标 HPC-Redis Node primary 的本地
写入路径构造，不是 CLI 原始请求，也不是 Follower 重新生成的事件。COLD
ingress 校验 payload 后交给 AOF append queue；唯一的 AOF writer 分配连续
`seq` 并封装完整的 `cold_backup_event`。AOF append 成功后，事件才被标记为
accepted，随后由 replication cursor 从本地 AOF 读取并异步排入复制队列：

```text
CLI
    -> Proxy（统一接入、按路由元数据转发）
    -> SuperNode/TLC worker (VADD/PUT/DEL，已携带目标 hpc_node_id)
    -> 目标 HPC-Redis Node primary 内的写入协调逻辑校验 owner/term/route_version，构造 event payload 和 WARM staging
    -> COLD ingress 校验待写 event payload
    -> 放入 per-node AOF append queue
    -> Node 内唯一 AOF writer 分配 seq、封装 immutable cold_backup_event 并执行本地 COLD AOF append（按 ack 策略持久化）
    -> AOF append 成功后标记 accepted，持久化完成后标记 durable
    -> replication cursor 从本地 AOF 读取 accepted event
    -> 放入 per-node replication queue，后台发送到 Follower COLD/HA state
```

这里的“写入协调逻辑”是目标 HPC-Redis Node primary 内的一项处理职责，不是
额外部署的进程或独立 HA 组件。它负责校验写权、协调 WARM 发布与 COLD AOF
持久化，并把 entry 交给异步复制队列；实现上可以由 SuperNode/TLC 请求处理
线程调用 Node 内的 HA Agent/COLD 接口完成。Proxy 只负责接入、路由和连接级
故障转发，不构造 COLD event，也不推进 COLD seq。

这样 WARM 负责在线读写和计算，COLD 负责异步备份。COLD 不得为了取得
payload 而延迟读取可复用的 WARM slot；事件必须携带独立 payload，或持有
在 COLD 完成前不会失效的引用。COLD event 的生命周期由队列所有权管理，
不能引用已经允许淘汰或覆盖的 WARM 内存。

SuperNode/TLC 在请求路径中对每个已校验写入只构造一次待写 event payload。
该 payload 的 key/value、序列化内存和处理时间受入口
请求上限约束；超限请求在边界处拒绝，或先按协议分块后为每个块构造 event。
请求路径只执行 event 构造和有界的本地 AOF append queue 入队，不做无界聚合，
也不等待 Follower；由 Node 内唯一的 AOF writer 串行完成实际 append。
COLD 的 RDB checkpoint、压缩、原子发布和跨节点补段全部由后台线程完成。复制队列满时不得丢弃 event：
保留本地 AOF 区间，由 replication cursor 稍后重放；SuperNode 不等待队列腾空。

### 2.1 COLD 组件

```text
请求路径（primary）
CLI -> Proxy -> SuperNode/TLC -> Node primary 写入协调逻辑
                                      |
                                      +-> WARM staging（暂不可见）
                                      +-> cold_backup_event payload
                                      |
                                      v
                                  COLD ingress
                                  校验 event
                                      |
                                      v
                         per-node AOF append queue
                              （多生产者入队）
                                      |
                                      v
                              AOF segment writer
                         （单消费者分配 seq、append）
                                      |
                                      v
             segment store / local AOF
                    |
          +---------+-------------------------+
          |                                   |
          v                                   v
 appended seq/progress cursor          append accepted
 （旁路元数据：记录 seq/offset）       （以下两条支路并行）
                                          |
                    +---------------------+---------------------+
                    |                                           |
                    v                                           v
        本地 durable 路径                    异步复制路径
               |                                  |
               v                                  v
  local flush coordinator                 replication cursor
   （group commit 批量策略）                       |
               |                                  v
               v                           replication queue
          durable_seq                             |
               |                                  v
               v                         网络发送到 Follower
  发布 WARM metadata/HA state                     |
               |                                  |
               v                                  v
  CLI LOCAL_DURABLE_ACK                     Follower COLD ingress
                                                  |
                                                  v
                                         Follower AOF append queue
                                                  |
                                                  v
                                         Follower AOF segment writer
                                                  |
                                                  v
                                         Follower local flush coordinator
                                                  |
                                                  v
                                  Follower durable ACK -> replicated_seq
                                                  |
                                                  v
                                          ha_safe_point_seq

后台 checkpoint / compact 路径
HA/WARM state + durable_seq -> checkpoint builder
                                      |
                                      v
                         checkpoint.<generation>.tmp
                                      |
                                      v
                             fsync + atomic rename
                                      |
                                      v
                            manifest / checkpoint catalog
                                      |
                                      v
                        retention / AOF compaction
```

图中 AOF append 是主节点进入后续处理的分界点：只有 append 成功的 event 才会
进入 replication cursor。复制支路与本地 group commit 支路并行，Follower 不在
本地 CLI 成功响应的等待链上；CLI 只等待本地 durable 分支完成。Follower 收到
event 后仍需在自己的 AOF 上完成持久化，再返回 durable ACK。

`appended seq/progress cursor` 在图中是元数据旁路，不是 event 数据分支：AOF writer
在 append 后更新 `appended_seq` 和 segment offset，flush coordinator 在 fsync 后
更新 `durable_seq`。这些进度供 CLI ACK、replication cursor、checkpoint、compact
和恢复校验使用；cursor 本身不写 AOF、不复制到 Follower，也不产生数据副本。

`cold_backup_event` 和 `appended seq/progress cursor` 是逻辑职责，不是额外部署的
进程：

- `cold_backup_event`：由目标 primary 的本地写入路径准备 payload，进入 COLD
  ingress 校验和 AOF append queue 后，由 Node 内唯一的 AOF writer 分配连续
  `seq`、封装完整 immutable event 并执行 append。AOF append 失败时不能标记为
  accepted，也不能进入复制队列。
  Follower 接收 primary 序列化后的同一个 `(term, hpc_node_id, seq)`，只做
  校验和幂等应用，不重新生成新的事件。
- `appended seq/progress cursor`：跟踪该 HPC-Redis Node 本地 AOF 的进度，至少
  对应 `appended_seq`（AOF 已接受的最高连续 seq）和 `durable_seq`（已达到本地
  持久化点的最高连续 seq）；实现上也可以记录当前 AOF segment 的物理 offset。
  它不负责发送 Follower，也不等同于 `replication cursor`。
- `replication cursor`：从本地 AOF 读取连续 entry，向 Follower 发送，并根据
  Follower 的 durable ACK 推进 `replicated_seq` 和 `ha_safe_point_seq`。

Primary 和 Follower 都运行完整的本地 COLD 管线。区别只在于入口来源：primary
从本地写入 worker 接收 payload，Follower 从 HA Agent 的复制接收器接收已经封装
的 event。Follower 也必须经过自己的 COLD ingress、append queue、单一 AOF writer
和 flush coordinator；不能由网络接收线程直接并发写 AOF，也不能在写入 page cache
后立即发送 durable ACK。

组件职责：

| 组件 | 职责 | 运行约束 |
|---|---|---|
| SuperNode/TLC ingress | 接收目标 primary 写路径构造的待写 event payload，校验并交给 COLD append queue | 不接收客户端原始请求，不等待 Follower |
| Follower COLD ingress | 接收 HA Agent 复制的完整 event，校验 term/seq/checksum 后交给本地 append queue | 不重新构造 seq；不绕过本地 writer |
| WARM payload adapter | 从 WARM 更新生成事件 payload 或受保护引用 | 不能引用已可淘汰/覆盖的 slot |
| per-node AOF append queue | 接收本地写 worker（primary）或复制接收器（Follower）提交的 event，并把所有权交给 AOF writer | 有界 MPSC；满时触发背压，不丢 event |
| local AOF writer | 每个副本各自作为唯一消费者：primary 分配连续 `seq`，Follower 沿用原 `seq` 幂等追加；两者都封装/校验完整 event | 独占本副本 append segment；不等待远端 |
| local flush coordinator | 按条数、字节数或时间窗口组织 group commit，执行 `fdatasync/fsync` 并推进本副本 `durable_seq` | 刷盘失败必须报告；不等待远端 |
| appended seq/progress cursor | 跟踪 `appended_seq`、`durable_seq` 和可选的 AOF segment offset | 只表示本地进度，不发送 Follower |
| replication cursor | 从本地 AOF 维护每 HPC-Redis Node 的 `replicated_seq`，向 Follower 补发 | 只发送连续 entry，支持幂等重放 |
| per-node replication queue | 将 Follower 复制与 COLD 磁盘解耦，保存待发送 entry | 有界队列；满时依靠本地 AOF 重放，不丢 entry |
| RDB/checkpoint builder | 从已应用状态生成恢复快照 | 后台批量执行，不阻塞请求 |
| AOF segment/compaction writer | 按 checkpoint floor 与 HA safe point 分段、压缩和回收本地 AOF | 独立线程和 I/O 预算 |
| segment store | 保存带 checksum、版本和 seq 范围的归档段 | 临时文件不得视为已发布 |
| checkpoint generation store | 保存带 checksum 的完整 checkpoint generation | 临时文件 fsync 后才能发布，不原地覆盖 |
| checkpoint publisher | fsync 临时文件、原子更新 generation/manifest | 发布失败保留旧 generation |
| manifest/catalog | 原子发布 RDB checkpoint、AOF 范围和格式元数据 | 只发布完整且校验通过的版本 |
| recovery manager | 扫描、修复、重放并重建 HA state/WARM | 恢复期间保持 `FENCED` |
| retention/compaction | 协调 checkpoint generation 和 AOF segment 回收 | 不得破坏任何保留 generation 的连续 replay 区间 |
| validator/metrics | 检测 gap、旧 term、坏 checksum、空间和 lag | 错误必须可观测、不可静默跳过 |

每个组件都按 `hpc_node_id` 隔离状态。一个 HPC-Redis Node 的 COLD 队列或 checkpoint
失败，不得锁住其他 HPC-Redis Node 的备份和在线请求。

### 2.2 AOF 与 RDB 的职责和组合

两者解决不同问题：

| 机制 | 保存内容 | 主要职责 | 典型代价 |
|---|---|---|---|
| AOF | 按 `seq` 顺序追加的 `PUT/DEL` 变更 | 至少保留最旧可回退 checkpoint 之后的变更，并受 HA safe point 约束；也是异步复制来源 | 文件持续增长，需要分段 compact；恢复需 replay |
| RDB | 各 key-shard 在不同捕获序号上的完整状态（fuzzy checkpoint） | 提供紧凑恢复基线，缩短 AOF replay；便于校验和离线备份 | 周期性生成有 I/O 和 CPU 开销，快照间隔内不能单独覆盖最新变更 |

推荐的 COLD 配置是 **AOF + RDB 同时存在**：

```text
写入：WARM staging -> COLD ingress -> append queue -> AOF writer
      -> 本地 durable 分支返回 local ACK；复制分支从本地 AOF 异步发送到 Follower
快照：后台直接序列化各 key-shard 的已提交 state，生成 fuzzy RDB checkpoint
维护：按保留的 checkpoint 集合回收旧 generation 和过期 AOF segment
恢复：加载最近有效 RDB，再 replay AOF 尾部到目标 seq
```

不要求每次写入都生成 RDB；RDB 是后台周期性或按水位触发的快照。AOF
追加和本地持久化点由 `local_ack_policy` 控制，Follower ACK 不在这条路径上。
故障恢复优先推荐 `LOCAL_DURABLE_ACK`；只有在业务明确接受本机故障丢失窗口
时，才使用 `LOCAL_ACCEPTED_ACK`。

可以只启用一种模式，但必须显式接受其边界：

```text
AOF-only：恢复完整性较好，但日志会增长，重启 replay 时间不可控。
RDB-only：实现简单、恢复快，但只能恢复到最近快照，快照后的写入可能丢失；
          不适合作为需要较小数据丢失窗口的默认模式。
```

因此 COLD 默认同时保留 AOF 和 RDB；AOF 保证增量恢复，RDB 控制恢复时间
和存储增长。RDB 使用不可变 generation 文件和 manifest 原子发布，不原地
覆盖已发布的 checkpoint；该发布协议不依赖 Doublewrite。AOF 仍独立使用
checksum 和 append 发布协议。

### 2.3 写入、持久化与异步复制流程

COLD 接收 SuperNode/TLC worker 生成的待写 event payload。单条写入的逻辑顺序为：

```text
CLI -> Proxy -> SuperNode/TLC
    -> 目标 HPC-Redis Node primary 写入协调逻辑校验 hpc_node_id、owner/term 和 route_version
    -> 构造 event payload 和 WARM staging（只保存待应用变更，不对读可见）
    -> COLD ingress 校验待写 event payload
    -> 放入 per-node AOF append queue
    -> Node 内唯一 local AOF writer 分配 seq、封装 cold_backup_event、追加 AOF，推进 appended_seq
       ├─ 本地 durable 分支：group commit/fsync 完成，推进 durable_seq
       │    -> 原子发布 WARM metadata/HA state
       │    -> 返回 CLI 成功（LOCAL_DURABLE_ACK）
       └─ 异步复制分支：replication cursor 读取已接受 entry
            -> replication queue -> Follower COLD ingress
            -> Follower append queue -> Follower AOF writer（沿用原 seq 幂等追加）
            -> Follower flush coordinator 完成本地持久化后 ACK
            -> primary 推进 replicated_seq / ha_safe_point_seq
```

多个 worker 可以并发构造 payload 并入队；有界 MPSC append queue 将 event
所有权转交给 Node 内唯一的 AOF writer。writer 串行分配连续 `seq`、封装完整
event、追加 AOF，并负责 append segment 的 rotate；worker 不直接并发写同一个
AOF 文件。队列满时触发背压，不能丢弃 event，也不能绕过 writer 伪造成功。

本地 `appended_seq` 只在本地 AOF 接受后推进，`durable_seq` 只在本地持久化
点完成后推进。进程在 AOF append 完成前崩溃时，entry 不算已接受；不能把“写入
page cache”或“收到网络包”当作本地 COLD 已持久化。

成功返回给 CLI 的写入使用上面的同一顺序：staging 只是待应用变更，真正的
WARM/HA state 只有在 AOF append 和本地持久化确认完成后才发布。AOF 失败时
必须丢弃 staging，不得返回成功；WARM 发布失败时保留 AOF entry，通过恢复
或幂等重试重建 WARM。replication cursor 可以在 AOF append 成功后读取 accepted
entry，与本地 group commit 并行；Follower ACK 不在 CLI 成功返回的前置条件中。

### 2.4 本地刷盘策略

`append` 只表示 entry 已写入 COLD AOF 的受保护写入路径，不必等价于每条
请求都执行一次物理 `fsync`。为了在性能和持久性之间取舍，`local_ack_policy`
至少应支持以下模式：

当前推荐默认配置为：

```text
cold_flush_mode          = group_commit
cold_group_max_entries   = deployment-defined
cold_group_max_bytes     = deployment-defined
cold_group_max_delay_us  = deployment-defined
local_ack_policy         = LOCAL_DURABLE_ACK
```

批次达到任一上限即触发刷盘；批次为空时新请求不需要等待其他请求到来，
可以立即建立单请求批次。上述阈值由实际介质、吞吐和尾延迟目标确定，不在
架构层固定具体数值。

| 模式 | 刷盘方式 | 成功返回语义 | 风险/代价 |
|---|---|---|---|
| `LOCAL_DURABLE_ACK` + group commit（默认） | 多个 entry 先由 AOF writer 顺序 append，由 local flush coordinator 批量 `fdatasync/fsync`；刷盘完成后一起推进 `durable_seq` | 仅确认已进入本次完成的本地持久化批次 | 一批请求共享一次 fsync，延迟受批量窗口影响 |
| `LOCAL_DURABLE_ACK` + per-write flush | 每条 entry 追加后立即 `fdatasync/fsync` | 每条成功写都达到本地持久化点 | 持久性最直接，但 IOPS、尾延迟和写放大最高 |
| `LOCAL_ACCEPTED_ACK` | 只 append 到进程/OS 缓冲或受保护队列，不等待持久化 | 只能表示已接受，不能承诺崩溃或断电不丢 | 进程、主机或存储故障时存在未落盘窗口；不作为默认成功语义 |

group commit 下，某个请求仍不能在包含它的 fsync 完成前返回
`LOCAL_DURABLE_ACK`；只是多个请求共享一次刷盘。批次应由条数、字节数或
最大等待时间触发，并设置有界窗口；如果当前没有可合并的请求，单个请求
也可以立即触发一次刷盘，不需要等待“下一个请求”。`fsync` 失败、AOF 写入
失败或本地持久化介质不可用时，必须拒绝成功返回并保留/报告明确的错误状态。

### 2.5 `LOCAL_DURABLE_ACK` 的返回时点

本设计区分两个时点：AOF 刷盘完成后产生内部的 `durable confirmation`；
对外的 `LOCAL_DURABLE_ACK` 成功响应则严格发生在以下条件全部满足之后：

```text
entry 已完整追加到本地 AOF（含长度、seq 和 checksum）
    -> 覆盖该 entry 的 fdatasync/fsync 或等价持久化屏障完成
    -> durable_seq 原子推进到不小于该 entry 的 seq
    -> WARM metadata / HA state 原子发布成功
    -> 向 CLI 返回成功（标记为 LOCAL_DURABLE_ACK）
```

前两步完成后，该写入路径可以先收到内部 durable confirmation；但在 WARM
发布失败时，不能向 CLI 返回成功。此时 AOF entry 必须保留，由恢复或幂等
重试重新应用 WARM。Follower 的接收、AOF append 和 apply 都不属于
`LOCAL_DURABLE_ACK` 的前置条件；它们只影响本地 `replicated_seq` 和异步复制 lag。

InnoDB redo 的 group commit 和 Redis AOF 的批量 append 可作为实现参考；
但 Redis `appendfsync everysec` 允许客户端在 fsync 前成功返回，不能满足
本设计的无损成功语义。COLD 使用有界 group commit，只有覆盖该 entry 的
批次持久化完成后才返回 `LOCAL_DURABLE_ACK`。

`LOCAL_DURABLE_ACK` 能保证已返回数据在进程崩溃、WARM 丢失或重启后可由本地
COLD RDB+AOF 恢复。若还要保证整台 Server 或本地磁盘物理损坏时不丢数据，
primary 的 COLD 存储必须使用独立冗余（例如 RAID/复制卷或同步持久化存储）。
仅依赖异步 Follower 时，primary 已返回但尚未达到 `replicated_seq` 的尾部
在 Server 整体损坏后仍可能丢失；此时不能宣称跨 Server 无损。
因此“CLI 成功且任何单 Server 故障都不丢”要求 COLD 在返回成功前得到
独立冗余介质确认；如果系统只提供本地单盘 AOF，则只能承诺进程重启恢复，
不能承诺物理 Server 故障下零丢失。

### 2.6 Checkpoint 和刷盘流程

RDB checkpoint 是每个 key-shard 在各自捕获序号上的完整状态。由于不同
key-shard 不在同一个全局时刻完成捕获，这种 Checkpoint 称为 `fuzzy checkpoint`：
它不是事务级的全局瞬时快照，但每个 key-shard 都有明确的状态边界，并且可以
通过 AOF 增量恢复到一致状态。

`checkpoint_seq` 表示 generation 的保守全局边界；generation 另外保存每个
key-shard 的 `captured_seq`。下文的 `checkpoint_seq_of(g)` 只是 generation
`g` 的全局边界记号，`captured_seq_of(g, key_shard_id)` 表示该 generation 中
指定 key-shard 的捕获序号；这些都是 Checkpoint 元数据，不是 HPC-Redis Node
级别的序号数组。

以下 checkpoint 和 compact 变量均在单个 HPC-Redis Node 内计算；不同 Node
分别维护各自的标量状态，不共享 seq。

```text
创建新的 checkpoint generation
    -> 读取当前 HPC-Redis Node 的 key-shard 列表
    -> 依次对单个 key-shard 获取短暂写保护
    -> 直接序列化该 key-shard 的稳定 HA state
    -> 记录 captured_seq_of(g, key_shard_id)
    -> 释放该 key-shard 写保护，继续处理下一个 key-shard
    -> 计算每个 key-shard 的 header/payload checksum
    -> 计算 generation 的保守 checkpoint_seq
    -> 顺序写 checkpoint.<generation>.tmp
    -> fsync 临时 checkpoint 文件
    -> 校验 key-shard 数量、captured_seq、generation 和全量摘要
    -> 原子 rename 为 checkpoint.<generation> 并 fsync 目录
    -> 选择保留 generation 集合和 AOF 保留边界
    -> 写 manifest.tmp，包含 active/retained generation 与 AOF 保留边界
    -> fsync manifest.tmp，原子 rename manifest 并 fsync 目录
    -> 后台 compact/回收旧 generation 与过期 AOF segment
```

每个 Checkpoint generation 至少保存以下内容：

```text
generation header:
    format_version, generation, hpc_node_id, term
    checkpoint_seq（所有 key-shard captured_seq 的保守最小值）
    key_shard_count, generation_checksum

key-shard record:
    key_shard_id
    captured_seq
    state_length, state_checksum
    serialized key/value/vector/metadata state
```

Checkpoint builder 只在当前 key-shard 的短暂写保护内生成该分区的稳定记录；
不同 key-shard 的 `captured_seq` 可以不同，这正是 fuzzy checkpoint 的语义。
跨 key-shard 的逻辑写操作必须在进入 COLD 前拆分为可独立恢复的 event，或由
上层保证其不会要求事务级的跨分区原子性。

Checkpoint builder 直接从 key-shard 状态生成数据，不通过重放全部 AOF 生成
Checkpoint，也不要求 HPC-Redis 支持 MVCC。AOF writer 在 Checkpoint 期间继续
接收新写入；新事件不会进入当前 generation，而是作为该 generation 之后的
恢复增量保留。key-shard 的短暂写保护只覆盖当前分区的序列化，不暂停整个
HPC-Redis Node。

每个 key-shard 写保护期间捕获的状态必须已经达到本地持久化边界：若该分区的
内存状态领先于 `durable_seq`，builder 必须等待对应的 local flush，或只捕获
最后一个已持久化的状态，并将 `captured_seq` 记录为不大于 `durable_seq` 的值。
因此 Checkpoint 不会把尚未完成本地持久化的 event 当作恢复基线。

临时文件在 manifest 原子发布前不可作为有效 checkpoint。任何一步失败都保留
旧 manifest 和旧 generation，新的不完整文件进入临时/废弃状态。只有新
generation 已 fsync、manifest 已发布且恢复校验通过后，才允许后台回收旧
generation；至少保留一个可回退版本。当前 COLD 不做已发布 checkpoint
文件的原地覆盖，因此不需要 Doublewrite。

### 2.7 Compact 和保留策略

compact 按 HPC-Redis Node 独立执行，使用 `checkpoint_seq`，不使用 InnoDB
的 LSN。`checkpoint_retention_count` 是每个 Node 保留的 checkpoint
generation 数量，与 `2xN` 拓扑中的 Node 数 `N` 无关；默认应不小于 `2`，
以保留当前 generation 和至少一个可回退 generation。

```text
checkpoint_retention_count = deployment-defined, >= 2
retained_generations       = 最近 checkpoint_retention_count 个已发布且校验有效的 generation
checkpoint_seq_of(g)    = min(captured_seq_of(g, key_shard_id) for all key-shards)
checkpoint_floor_seq    = min(checkpoint_seq_of(g) for g in retained_generations)
ha_safe_point_seq       = min(durable_seq, replicated_seq)，且仅在当前 term 连续有效
aof_compact_through_seq = min(checkpoint_floor_seq, ha_safe_point_seq)
aof_retain_from_seq     = aof_compact_through_seq + 1
```

`ha_safe_point_seq` 表示 primary 和 Follower 都已经本地持久化的最高连续
`seq`。Follower 只有在自身 AOF 的 `durable_seq` 推进后才能发送可推进
`replicated_seq` 的 ACK；primary 不等待该 ACK 返回 CLI 成功，但 compact
必须等待它。不能简单地在最新 checkpoint 发布后删除所有 `checkpoint_seq`
之前的 AOF：若保留旧 generation 作为回退点，就必须保留从该旧 generation
的最小 `captured_seq + 1` 开始的连续 AOF；同时删除上限不能超过 HA safe point。

`ha_safe_point_seq` 是当前 `term` 下的 Node group 元数据，由 primary
随 HA/replication 状态发布。两个副本只能在本地 `durable_seq` 已覆盖该值、
term 一致且连续性校验通过后，用它更新各自的 compact catalog；Follower 不能
自行依据未验证的 primary 进度越过 safe point 回收本地 AOF。

例如，保留两个 generation：

```text
generation 41: checkpoint_seq = 1,000,000
generation 42: checkpoint_seq = 1,200,000
generation 43: checkpoint_seq = 1,500,000

保留集合 = {42, 43}
checkpoint_floor_seq = 1,200,000
ha_safe_point_seq = 1,350,000
AOF compact 上限 = min(1,200,000, 1,350,000) = 1,200,000
可回收 AOF = end_seq <= 1,200,000 的完整 segment
```

每次新 generation 发布并验证后，后台 compactor 按以下顺序运行：

```text
选择最近 checkpoint_retention_count 个有效 generation
    -> 计算 checkpoint_floor_seq / ha_safe_point_seq / aof_compact_through_seq
    -> 原子发布 retained_generations 与全部 compact 边界 catalog
    -> 删除超出保留集的旧 checkpoint generation
    -> 删除 end_seq <= aof_compact_through_seq 的完整 AOF segment
    -> 若边界落在某个 segment 内，重写其保留后缀到临时 segment，fsync 后原子发布
```

catalog 必须先于文件删除发布。进程在 compact 任一步崩溃时，最多遗留可回收
文件或重复 AOF，不得出现 catalog 仍允许选择某 generation、但恢复它所需
AOF 已被删除的状态。compactor 只处理已 seal 的 AOF segment；当前 append
segment 必须先 rotate 成 seal segment，不能与 local AOF writer 原地重写同一文件。

Follower 不可用或其本地 AOF 未达到 durable point 时，`ha_safe_point_seq` 不会
前进，AOF compact 也不能越过它。这不会让单条在线写入等待 Follower，但会增加
primary 的 AOF 保留量；容量接近上限时必须告警并按资源策略暂停新写入或启用
独立归档，不能以 snapshot resync 为理由越过 safe point 删除 AOF。

Follower 的增量复制仅在所需起点仍位于 AOF 保留范围内时进行：

```text
replicated_seq + 1 >= aof_retain_from_seq
    -> 从 AOF 做增量追赶

replicated_seq + 1 < aof_retain_from_seq
    -> 发送 active generation + 按各 key-shard captured_seq 补发 AOF tail
```

### 2.8 COLD 数据同步流程

同步是“primary 本地 COLD AOF -> Follower COLD/HA state”，不是同步提交：

```text
当前 HPC-Redis Node owner
    -> 本地 COLD AOF 追加 entry
    -> replication cursor 从 AOF 读取 entry
    -> 异步发送到 Follower
    -> Follower COLD ingress 校验 term、seq 和 checksum
    -> Follower append queue -> Follower AOF writer 按原 seq 幂等追加
    -> Follower flush coordinator 达到本地 durable point 后 ACK durable_seq
    -> Follower 再应用 WARM/HA state（不可见 staging -> durable 后发布）
    -> primary 推进 replicated_seq / ha_safe_point_seq
    -> 检测 gap 后请求补段，不能跳过序号
```

数据源切换规则：

1. 正常时 Follower 从 primary 的本地 COLD AOF 消费；所需起点已被 compact 时，
   改为接收 active generation + AOF tail 的 snapshot resync。
2. primary 故障时，从可访问的旧 primary COLD AOF、Follower 或新 owner 补读
   已知连续区间。
3. HPC-Redis Node 切主只切换该 HPC-Redis Node 的 COLD consumer source，其他 HPC-Redis Node 不变。
4. 同一 `(hpc_node_id, term, seq)` 重复到达必须幂等；不同 payload 或 checksum
   冲突必须报警并停止该 HPC-Redis Node 的 COLD 发布。
5. COLD-A 与 COLD-B 独立落盘；可选的异地归档用于更大故障域，不替代 HA
   两个在线副本的异步复制。

同步指标为每 HPC-Redis Node 的 `appended_seq`、`durable_seq`、`replicated_seq`、
`checkpoint_seq` 和 `replication_lag = appended_seq - replicated_seq`。lag 只
影响 Follower 新鲜度和故障切换可保留的数据边界。

### 2.9 COLD 恢复流程

```text
启动 recovery manager（保持 FENCED）
    -> 读取并校验 manifest/catalog
    -> 先选择 active generation；失败时只在 retained_generations 内回退
    -> 扫描正式 checkpoint 中各 key-shard 状态并校验全量摘要
    -> 校验失败：回退到上一个有效 generation，并确认 AOF 覆盖其 replay 起点
    -> 加载各 key-shard checkpoint 状态到临时 HA state
    -> 按 captured_seq_of(selected_generation, key_shard_id) 过滤并 replay 后续 AOF event
    -> 校验 term、seq、version、key/value checksum 和删除 tombstone
    -> 与可用 HA 副本比较 durable/replicated boundary
    -> 原子切换恢复出的 HA state
    -> 重建 WARM/HOT，追平 primary AOF/复制日志
    -> fencing/lease 校验通过后才允许 BACKUP 或 MASTER
```

发现 AOF gap、旧 term、manifest 摘要不匹配或保留的 generation 均损坏时，恢复
必须停止并报警，不能跳过 entry 继续生成看似完整的数据。

### 2.10 COLD 背压和故障边界

```text
AOF writer/local flush coordinator 慢或不可写
    -> AOF append queue 累积，aof_lag 上升并告警
Follower 慢或不可用
    -> replication queue 累积，replication_lag 上升并告警
checkpoint builder/publisher 慢或失败
    -> checkpoint_lag 上升或 checkpoint_publish_failure 告警
以上任一情况
    -> primary 按 local_ack_policy 继续或暂停本地写入；不等待 Follower
```

本地 AOF 的最低保留边界由 `aof_retain_from_seq` 决定，不能为了降低磁盘
占用而越过该边界。只有 AOF 保留空间耗尽、且无法继续追加或 compact 时，才触发
primary 自身的资源保护策略；不能因为 COLD 或 Follower lag 而丢弃已返回
`LOCAL_DURABLE_ACK` 的 entry。Follower lag 会停止 `ha_safe_point_seq` 推进，
因而阻止 AOF 删除越过该 safe point；Follower 重置或所需起点已被删除时必须走
snapshot resync，但 snapshot resync 不解除当前 Follower 对 safe point 的约束。

## 3. 分层职责

| 层 | 主要内容 | 是否在请求热路径 | 故障后的作用 |
|---|---|---:|---|
| HOT | key、版本和位置索引 | 是 | 可丢失，由 WARM/HA 重建 |
| WARM | UB/shm 中的向量工作集 | 是 | 可丢失，lazy rebuild |
| HA replication stream | primary 到 Follower 的异步操作复制 | 是（仅排队） | Follower 追平和切主 |
| HA state | 每个副本的在线逻辑状态（属于 HA/WARM 在线层） | 是 | 直接提供服务 |
| COLD AOF | 本地追加式持久化日志 | 是（本地 append/ack） | 最近写入恢复、Follower 异步复制来源 |
| COLD RDB checkpoint | 不可变 generation 状态快照 | 否 | 快速恢复基线、压缩 AOF replay 范围 |
| COLD checkpoint publisher | fsync 临时文件并原子发布 manifest | 否 | 防止不完整 generation 被采用 |

primary COLD AOF 必须有明确的保留和截断策略；Follower 通过 replication
cursor 消费 AOF，不能把 COLD 文件当作在线读索引或在线锁的延伸。

### 3.1 与 InnoDB 机制的对应关系

```text
HA replication log   ~= 复制流：把 primary 的变更异步发送到 Follower
HA state              ~= 内存/数据页：承载当前可服务的逻辑状态
COLD AOF             -> 逻辑变更日志：记录本地接受的变更，供崩溃恢复和复制；职责上类似 InnoDB redo，但不是同一格式
COLD RDB             ~= RDB/checkpoint：提供恢复基线
COLD checkpoint publisher ~= 不可变 generation + 原子 manifest：防止不完整 RDB 被采用
```

TLC 不要求把 InnoDB 的 Undo、Redo 和 Binlog 逐一实现为同名组件。关键是
保持职责关系：本地 AOF 保护已接受变更，RDB 保存一致快照，generation/manifest
原子发布防止不完整快照被采用，复制流把 AOF entry 异步送到 Follower。COLD 不承担普通
读请求和计算，也不等待远端 ACK。

InnoDB 的 Doublewrite 保护的是原地刷回表空间的数据页，并不保护 redo；
TLC COLD 当前使用完整 generation 文件和 manifest 原子发布，因此不需要
Doublewrite。AOF 通过 entry framing、seq 和 checksum 处理追加日志的崩溃恢复：
尾部不完整 entry 可截断，中间 checksum/seq 损坏必须停止恢复并切换到健康
副本或灾备文件，不能跳过损坏区间继续提供服务。

## 4. Keepalived 风格控制面

### 4.1 HPC-Redis Node group 的部署模型

每个 HPC-Redis Node 是一个独立的 hpc-redis 服务进程，并部署在独立的
Server 上；同一 Server 不承载多个用于生产 HA 的 HPC-Redis Node。两个 IDC
中具有相同 `hpc_node_id` 的两个进程组成一个
HPC-Redis Node group：

```text
HPC-Redis Node group n

IDC-A / Server-A-n                         IDC-B / Server-B-n
-------------------                        -------------------
hpc-redis process                          hpc-redis process
  ├─ TLC + WARM                              ├─ TLC + WARM
  ├─ local COLD AOF/RDB                      ├─ local COLD AOF/RDB
  └─ HA Agent worker(n)  <== async ==>       └─ HA Agent worker(n)
```

HA Agent worker 应嵌入每个 HPC-Redis Node 进程，或作为与该进程一一对应的
sidecar；它不是一个代表整个 IDC 的单一 worker。每个 worker 只管理本地
`hpc_node_id`，负责：

```text
heartbeat 收发和本地状态机
与对端 Node 的 AOF replication sender/receiver
本地 fencing、term/lease 和 owner 检查
本地 COLD AOF/RDB 恢复、追赶和健康上报
```

可以额外部署 IDC 级 controller 汇总指标、下发配置或执行运维操作，但它
不能替代 Node 内 HA worker，也不能把多个 Node 合并成一个故障切换单元。
一 Node 一 Server 是故障域要求：Server 故障只影响一个 HPC-Redis Node；
多个 Node 共用 Server 仅适用于开发或测试，不满足生产 HA 的独立故障隔离。

### 4.2 身份和心跳

每个 HPC-Redis Node 进程内的 HA Agent worker 管理一个 Node group。同一
HPC-Redis Node group 使用稳定的 `group_id + hpc_node_id`，例如：

```text
group_id = TLC-HA
hpc_node_id = n
IDC-A: idc_id = 0, priority = 200
IDC-B: idc_id = 1, priority = 100
```

priority 只在双方可通信时用于确定性排序，不能代表数据更新程度。

heartbeat 至少携带并校验：

```text
protocol_version, group_id, idc_id
hpc_node_id
role（MASTER/BACKUP，仅作报告）
term/epoch、appended_seq、durable_seq、replicated_seq、ha_safe_point_seq、applied_seq（均属于该 HPC-Redis Node）
ha_replica_health、warm_health、cold_aof_health、checkpoint_health、
aof_lag、replication_lag
advertisement interval、checksum/authentication
```

`cold_aof_health`、`aof_lag` 和 `checkpoint_health` 只用于可观测性、备份告警和恢复排程，
不能直接授予或撤销 HA 写权。故障判断使用本地接收时间，并校验心跳身份、
版本、group、完整性和 term 新旧关系。

### 4.3 每个 HPC-Redis Node 的状态机

```text
INIT -> BACKUP -> CANDIDATE -> MASTER
  |       ^          |            |
  +----> FAULT <-----+------------+
             |
             v
        RECOVERING -> BACKUP
```

每个 HPC-Redis Node group 独立拥有 `BACKUP`、`MASTER`、`FAULT`、`RECOVERING` 状态；
`CANDIDATE` 是该 HPC-Redis Node 的切主瞬态，`INIT` 是该 HPC-Redis Node 的启动瞬态。
`FENCED` 是独立安全闸门，可叠加在任何 HPC-Redis Node 状态上。一个 HPC-Redis Node 的状态
变化不能直接改变其他 HPC-Redis Node 的 owner 或状态。

关键转换：

```text
INIT -> BACKUP
    完成本地 COLD RDB/AOF 和复制状态检查；默认关闭写入口。

INIT -> MASTER
    仅 bootstrap 明确指定初始 owner，或已取得有效 lease 时允许。

BACKUP -> CANDIDATE
    master_down_timer 连续超时后触发；单次丢包不能触发。

CANDIDATE -> MASTER
    该 HPC-Redis Node 的本地状态和恢复文件健康、取得更高 term/lease，且该 HPC-Redis Node 的
    旧 Leader 已被 fencing。候选副本只能接管自身 replicated_seq 以内的数据；
    COLD/AOF lag 决定可恢复边界，不得伪造缺失的 entry。

MASTER -> FAULT
    该 HPC-Redis Node 的 HA 状态、复制协议或 lease 失败；只关闭该 HPC-Redis Node 写入口并
    设置该 HPC-Redis Node 的 FENCED。

FAULT/RECOVERING -> BACKUP
    COLD AOF/HA state 追平、fencing 确认和健康稳定期完成。
```

数据面状态单独表示：

```text
NORMAL    = primary 本地持久化正常，Follower 异步追赶
DEGRADED  = Follower 不可用或 lag 超阈值；仍可按本地 ack 策略写入
RECOVERING= 正在从 COLD RDB+AOF 或 peer 恢复，不能接管或提供写服务
FENCED    = 失去写权威，禁止提交和竞选
```

COLD 故障只会在受影响的 HPC-Redis Node 上附加 `COLD_DEGRADED` 备份标志，不应把
健康的 HA HPC-Redis Node 转成 `FAULT`：

```text
HPC-Redis Node n MASTER + NORMAL + COLD_DEGRADED = 仍可提供在线 HA 服务，备份告警
HPC-Redis Node n BACKUP + NORMAL + COLD_DEGRADED = 可接管，但需标记该 HPC-Redis Node 备份落后
```

### 4.4 状态回调

```text
on_master:
    对目标 HPC-Redis Node 校验 term/lease、本地 COLD durable_seq 和 fencing；通过后
    打开该 HPC-Redis Node 写入口

on_backup:
    关闭该 HPC-Redis Node 的本地客户端写入口；保留异步复制接收和 COLD 恢复线程

on_fault:
    关闭该 HPC-Redis Node 写入口；若曾持有该 HPC-Redis Node owner 或 lease，设置该 HPC-Redis Node FENCED

on_recover:
    进入 RECOVERING；先恢复 HA 状态，再重建 WARM/HOT；COLD 备份可在后台追赶
```

健康检查必须区分在线服务与备份服务：

```text
HA 进程、复制线程、local/durable/applied lag、lease/fencing
COLD AOF 可追加、RDB generation/manifest 可用、checkpoint 和 replication lag
```

其中在线状态和本地 COLD AOF 可写性按 HPC-Redis Node 决定是否可以接受写入；Follower
可达性只决定复制状态和切主候选资格；RDB generation/manifest 状态决定恢复优先级。

## 5. HA 双副本异步复制写路径

`VADD/PUT/DEL` 的在线写入先落到 primary 本地 COLD AOF，再异步复制到
Follower。HA 仍保留两个副本，但不使用同步 `quorum=2` 提交：

```text
CLI
  -> Proxy（统一接入并转发至 SuperNode/TLC）
  -> 当前 HPC-Redis Node primary
  -> 构造 event payload 和 primary WARM staging（不可对普通读可见）
  -> COLD ingress 校验待写 event payload
  -> per-node AOF append queue
  -> Node 内唯一 AOF writer 分配 seq、封装并追加 (term, hpc_node_id, seq, op, key, value, version, checksum)
       ├─ local flush coordinator 按 local_ack_policy 执行 group commit/fsync
       │    -> durable_seq
       │    -> 原子发布 WARM metadata/HA state
       │    -> 返回 CLI 成功（LOCAL_DURABLE_ACK）
       └─ replication cursor 读取 accepted entry
            -> replication queue -> Follower COLD ingress
            -> Follower append queue -> Follower AOF writer
            -> Follower flush coordinator -> durable ACK
            -> Follower apply WARM/HA state
            -> replicated_seq / ha_safe_point_seq 推进
```

进度字段按 HPC-Redis Node 定义：

```text
appended_seq    = primary 已接受并写入 AOF 的最高 seq
durable_seq     = primary COLD AOF 已达到本地持久化点的最高 seq
replicated_seq  = Follower AOF 已本地持久化并确认的最高连续 seq
ha_safe_point_seq = primary 与 Follower 均已本地持久化的最高连续 seq
applied_seq     = 本地 HA state/WARM 已应用的最高连续 seq
```

`LOCAL_DURABLE_ACK` 只承诺 primary 本地 COLD 已持久化；它不承诺
Follower 已收到。`LOCAL_ACCEPTED_ACK` 甚至只承诺写入已进入本地受保护队列，
进程和存储同时故障时可能丢失；它只能作为显式的 pending/降级响应，不能
作为“成功”返回，也不能称为 `QUORUM_ACK`。

### 5.1 某个 HPC-Redis Node 的 Follower 不可用

Follower 不可用时的默认策略：

```text
HPC-Redis Node n 的 IDC-B 副本不可用
    -> IDC-A 仍是 HPC-Redis Node n 的 MASTER
    -> 仅 HPC-Redis Node n 标记为 REPLICATION_DEGRADED
    -> A 继续写本地 COLD AOF（按 local_ack_policy 返回）
    -> 其他 HPC-Redis Node 继续按各自状态服务
```

Follower 恢复后，先以 `RECOVERING` 从 primary COLD AOF 补齐，再恢复正常的
异步复制。`replicated_seq` 追不上时，不能把本地 `LOCAL_ACK` 伪称为两副本
确认；其他 HPC-Redis Node 不需要等待该恢复过程。

### 5.2 在线读路径

普通 `VEMB/VSIM/GET` 只访问当前 HA owner 的 HOT/WARM/HA state：

```text
WARM hit -> 直接读取并校验 generation/version
WARM miss -> 从本地 HA state 加载并 promote 到 WARM
HA state miss -> 按一致性策略从 peer 读取或返回 NOT_FOUND
```

COLD 不参与正常读延迟。只有恢复、审计、离线导出或 HA state 双失时，才
允许从 COLD RDB + AOF 重建 HA state。

## 6. HA-COLD 接口契约

详细的 COLD 组件和生命周期流程见第 2 节。本节只规定 HA 与 COLD 的
边界，防止实现重新把 COLD 放回在线提交路径。

### 6.1 SuperNode/TLC 向 COLD 发布

SuperNode/TLC 向 primary COLD ingress 提交由本地写入路径构造的待写 event
payload；AOF writer 为其分配 Node 内连续 `seq`，封装完整
`cold_backup_event` 并追加到 AOF。只有 AOF append 成功后才推进该
HPC-Redis Node 的 `appended_seq`。未提交的 WARM 内容和 speculative 更新不得
进入 COLD AOF。复制和恢复接口中的完整 event 至少包含：

```text
(hpc_node_id, term, seq, op, key, value, version, checksum)
```

字段语义如下：

| 字段 | 含义 |
|---|---|
| `hpc_node_id` | 目标 HPC-Redis Node；是 HA/COLD 故障和切换粒度，不是 `key_shard_id` |
| `term` | 当前 HA owner 任期/fencing epoch；旧 owner 的 event 必须拒绝 |
| `seq` | 由该 Node 唯一 AOF writer 分配的单调递增逻辑事件序号；用于顺序、连续性和幂等，不能当作 InnoDB LSN |
| `op` | 逻辑操作类型，例如 `VADD`、`PUT`、`DEL`；删除保留 tombstone 语义 |
| `key` | 被修改对象的规范化 key，复制和恢复必须使用相同编码 |
| `value` | `VADD/PUT` 的完整 payload；`DEL` 可为空或携带 tombstone 信息 |
| `version` | key/对象的逻辑版本，用于拒绝旧更新和冲突检测 |
| `checksum` | 覆盖规范化 event header 与 payload 的完整性校验值 |

事件状态必须区分 `constructed`、`accepted` 和 `durable`：payload 构造完成不
表示 AOF 已接受；AOF writer 分配 `seq` 并 append 成功后才可标记 `accepted`，覆盖该 entry 的本地
fsync/group commit 完成后才可标记 `durable`。重试必须复用相同的
`(term, hpc_node_id, seq)`，不能生成不同事件。

COLD 写入流水线向本地写入路径返回持久化进度和错误状态；写入路径据此决定
是否可以向 CLI 返回成功。该结果不是 Follower 提交确认：

```text
appended_seq、durable_seq、replicated_seq、ha_safe_point_seq、checkpoint_seq
```

这些字段永远不能伪装成 Follower 提交，也不能改变 HPC-Redis Node owner。

### 6.2 COLD 对 HA 的恢复接口

恢复接口按 HPC-Redis Node 请求一个连续边界：

```text
load checkpoint generation
replay AOF [selected_generation.checkpoint_seq + 1, target_seq]
return validated state + recovered_seq
```

返回前必须完成 checksum、term、seq、版本和 tombstone 校验；出现 gap 或
冲突时返回恢复失败。HA 在收到恢复状态后仍需从在线副本追平，并保持
`FENCED/RECOVERING`，直到 fencing/lease 和 owner 条件全部满足。

### 6.3 故障隔离

```text
COLD flush coordinator/checkpoint builder 失败
    -> 仅对应 HPC-Redis Node 的 COLD 备份进入 COLD_DEGRADED
    -> primary 按 local_ack_policy 继续或暂停本地写入
    -> 记录 lag、空间和错误指标
```

COLD append queue、AOF writer、flush coordinator、checkpoint builder、replication
cursor 和 recovery manager 均不得占用 HA 请求线程；一个 HPC-Redis Node 的 COLD
阻塞不得锁住其他 HPC-Redis Node。只有 primary AOF 保留空间耗尽时，才按
HA 的资源保护策略处理，不能把 COLD 失败伪装成本地持久化成功。

## 7. Leader 故障、按 HPC-Redis Node 切主和旧节点恢复

以下只描述 HPC-Redis Node n；其他 HPC-Redis Node 的控制面和数据面并行运行，不被该 HPC-Redis Node
的切换阻塞。假设 HPC-Redis Node n 的 A 是 primary、B 是 replica：

```text
A MASTER                         B BACKUP
   | heartbeat/HA replication       |
   X 故障                           |
                                    v
                         timeout -> CANDIDATE
                                    |
                         检查 B replicated_seq / COLD AOF
                         取得更高 term/lease
                         确认 A 已 fencing
                                    |
                                    v
                                  MASTER
```

切主依赖 HPC-Redis Node n 的 B `replicated_seq`、B 本地 COLD 恢复状态和 fencing。
A 在本地 AOF 中已有、但尚未复制到 B 的尾部 entry，不能在 B 上凭空出现；
这些 entry 在异步复制模型下可能丢失。已经返回 `LOCAL_DURABLE_ACK` 的写入
若未达到 B 的 `replicated_seq`，必须按产品策略报告为 failover data loss，
不能声称两副本无损接管。

旧 A 的 HPC-Redis Node n 恢复时：

```text
发现更高 term -> 保持 FENCED
             -> 作为 BACKUP 连接 B
             -> 从 B 的 HPC-Redis Node n COLD AOF/复制日志追平
             -> 重建 HPC-Redis Node n 的 WARM/HOT
             -> 后台补齐 HPC-Redis Node n 的 COLD backup
             -> 健康稳定后重新作为 HPC-Redis Node n 的异步 Follower
```

没有 lease、外部仲裁、witness 或 STONITH 时，两副本无法同时保证持续可写
和绝不脑裂。网络分区期间的安全默认行为是两边停止写入，或只允许拥有
外部 fencing 证明的一边继续服务。

## 8. 启动、重建和运维策略

推荐启动顺序不应作为安全依据；初始 owner 由持久化 epoch/lease 或明确的
bootstrap 配置确定：

```text
启动 -> INIT/FENCED
     -> 检查本地 COLD RDB/AOF 和 HA state
     -> 从 primary COLD AOF 追平 replicated_seq
     -> 重建 WARM/HOT
     -> 加入 BACKUP 或按 lease 成为 MASTER
     -> COLD 备份在后台继续追赶
```

建议关闭自动 preemption，或设置足够长的恢复 hold-down，避免网络抖动造成
主备来回切换。运维指标至少包括：

```text
appended_seq、durable_seq、replicated_seq、applied_seq、aof_lag、replication_lag
checkpoint_seq、checkpoint_lag、retained_generation_count、checkpoint_floor_seq
ha_safe_point_seq、aof_compact_through_seq、aof_retain_from_seq、aof_segment_bytes
checkpoint_publish_failure、checkpoint_fallback_count、manifest_checksum_error、snapshot_resync_count、retention_pressure
warm_rebuild_count、fencing_count、split_brain_reject
```

## 9. 故障矩阵和验收标准

| 场景 | HA 行为 | COLD 行为 |
|---|---|---|
| WARM 丢失 | 从 HA state lazy rebuild | 不受影响 |
| 某 HPC-Redis Node 的 HA-B 副本故障 | 仅该 HPC-Redis Node 在 A 保持 MASTER；继续本地 AOF 写入 | `ha_safe_point_seq` 停止推进，AOF 保留量上升 |
| 某 HPC-Redis Node 的 HA-A 副本故障 | 仅该 HPC-Redis Node 由 B 经 fencing 接管 `replicated_seq` 内数据 | 未同步 AOF 尾部可能丢失 |
| primary COLD 不可用 | 按 local_ack_policy 拒绝或降级本地写入 | 标记 COLD_DEGRADED，不等待 Follower |
| checkpoint generation 不完整或校验失败 | 不影响在线 HA | 回退旧 generation 后 replay AOF |
| 某 HPC-Redis Node 的两个 HA 副本均丢失 | 仅该 HPC-Redis Node 停止服务，等待恢复 | 从该 HPC-Redis Node 的 COLD RDB + AOF 灾备重建 |
| 网络分区 | 无 fencing 时停止写入 | 仅记录备份状态 |

实现验收至少应证明：

1. 正常时每个 HPC-Redis Node 只有一个 MASTER，Follower 通过异步 AOF 流持续追赶。
2. `LOCAL_DURABLE_ACK` 只证明 primary COLD AOF，本身不代表 Follower 已确认。
3. RDB fuzzy checkpoint、压缩、checkpoint 发布和恢复扫描不运行在 HA 请求线程；
   Checkpoint 直接序列化 key-shard 状态，不通过重放全部 AOF 生成；本地 AOF
   append 只承担配置的 local ack 边界。
4. 某 HPC-Redis Node 的 COLD 故障只产生该 HPC-Redis Node 的 `COLD_DEGRADED` 和告警，不自动
   触发 HA 切主。
5. COLD checkpoint generation 经 checksum、manifest 和旧版本回退校验，损坏或不完整版本不会被采用。
6. COLD 恢复按 RDB + 连续 AOF 重放，拒绝 gap、旧 term 和坏校验。
7. 某 HPC-Redis Node Leader 故障后，仅该 HPC-Redis Node 的候选副本从 `replicated_seq` 接管，
   明确报告未同步 AOF 尾部。
8. 旧 Leader 的该 HPC-Redis Node 恢复先 `FENCED/RECOVERING`，从新 owner AOF 追平后
   才重新作为异步 Follower。
9. 无 fencing 的网络分区不会自动允许双主写入。
10. 所有 `N` 个 HPC-Redis Node 的 owner 独立发布，与按 `hpc_node_count` 分配的
    `hpc_node_owner[hpc_node_count]` 保持一致；单 HPC-Redis Node 切换不修改其他 HPC-Redis Node owner。
11. AOF compact 不得删除 `seq > ha_safe_point_seq` 的 entry，也不得删除任一
    `retained_generations` replay 所需的连续区间；Follower durable ACK 才能推进 safe point。
