# VEMB V16 COLD Layer 跨集群 HA 与 LVS/F5 控制面借鉴设计

日期：2026-08-24

状态：设计草案

## 1. 结论

本设计明确区分两个层次：

```text
集群内 scale-out：同一个集群内增加/移除 owner，改变 key 的归属。
集群间 HA：两个独立集群之间复制 COLD 权威数据，并在故障时切换服务集群。
```

当前 `active_ring`、`standby_ring`、`topology_epoch`、`min_write_epoch`、
迁移状态、final fence 和 owner lease，属于**集群内扩容迁移协议**，不是现成的
跨集群 HA 协议。

它们可以复用的只是协议思想和部分状态机结构。跨集群 HA 必须新增独立的：

```text
HA cluster role
Paxos group / ballot
durable commit index
cross-cluster authority epoch
write fencing
replication catch-up state
```

不能因为一次 scale-out 已经完成，就认为另一个集群已经具备接管写入的条件。

## 2. LVS/F5 能借鉴什么

`LVS-master` 下没有找到题目所述中文标题的独立文件。这里按 Keepalived/VRRP
源码和配置样例归纳其可借鉴部分。

### 2.1 借鉴入口漂移，不借鉴其数据权威语义

VRRP/Keepalived 负责选出一个 MASTER，并在状态变化时添加或删除 VIP、路由，
再通过 gratuitous ARP 更新邻居缓存。[vrrp.c](/Users/szza/codespace/work/LVS-master/tools/keepalived/keepalived/vrrp/vrrp.c:736)

对应到本系统：

```text
VRRP VIP                  -> 对外服务集群 endpoint / traffic group
VRRP MASTER/BACKUP        -> HA_ACTIVE / HA_STANDBY
advertisement timeout     -> 健康度与租约超时
gratuitous ARP            -> 发布新的 HA topology snapshot
notify_master/backup      -> 触发服务入口、读写门和监控状态变化
```

这只能决定“请求应该送到哪个集群”，不能决定“哪个节点可以修改 COLD”。

### 2.2 借鉴显式状态机、健康检查和抖动抑制

Keepalived 根据接口和脚本检查结果调整 effective priority，避免单次探测失败
立即切换。[vrrp_scheduler.c](/Users/szza/codespace/work/LVS-master/tools/keepalived/keepalived/vrrp/vrrp_scheduler.c:692)

LVS 中的 `quorum/hysteresis` 会控制 virtual server 是否继续挂载后端，
但这个 quorum 是后端服务池存活数，不是 Paxos 投票 quorum。[ipwrapper.c](/Users/szza/codespace/work/LVS-master/tools/keepalived/keepalived/check/ipwrapper.c:721)

HA 可以借鉴：

```text
多探针
失败/恢复 hysteresis
显式 FAULT、READONLY、FENCED 状态
切换期间 drain 和入口撤销顺序
```

不能借鉴：

```text
VRRP priority 作为 COLD 写权限
收到更高 priority 就直接允许写入
仅靠 VRRP advertisement 防止网络分区脑裂
```

### 2.3 借鉴数据面和控制面分离

LVS DR/TUN 说明了转发数据面可以很轻：

```text
DR：修改目的 MAC，后端直接返回客户端。
TUN：IPIP 封装，后端解封装后直接返回客户端。
```

对应到 VEMB：

```text
UB/shared payload 的直接读       -> 类似 DR 的快速数据面
跨机器 RPC / fallback             -> 类似 TUN 的封装路径
owner private metadata            -> 数据权威目录
HA/Paxos control plane            -> 决定哪个集群可以提交写入
```

快速读路径不能反向推导写 owner。远程 WARM handle 也必须继续经过
`owner_generation`、key fingerprint 和发布版本校验。

### 2.4 借鉴同步临时状态的边界

IPVS syncd 在 MASTER/BACKUP 切换时改变同步方向，主要同步连接状态；它不是
业务数据的持久权威。[vrrp_scheduler.c](/Users/szza/codespace/work/LVS-master/tools/keepalived/keepalived/vrrp/vrrp_scheduler.c:230)

对应到本系统：

```text
Paxos log / COLD                -> 权威数据复制
WARM payload                    -> 可重建缓存
WARM handle / remote meta       -> 可失效的定位信息
连接、请求去重、session 状态    -> 可选的临时状态同步
```

## 3. 两层拓扑

### 3.1 集群内 scale-out

每个集群独立维护自己的 owner ring：

```text
Cluster A
  owner A0, A1, A2, ...
  active_ring_A
  standby_ring_A
  topology_epoch_A

Cluster B
  owner B0, B1, B2, ...
  active_ring_B
  standby_ring_B
  topology_epoch_B
```

集群内扩容只改变本集群的 key owner：

```text
candidate topology
  -> baseline snapshot
  -> delta outbox
  -> checkpoint barrier
  -> final fence
  -> target owner lease
  -> publish full active topology
```

它解决的是同一集群内部的 key 迁移和旧 owner 拒绝访问，不解决 Cluster A
和 Cluster B 谁对外服务。

### 3.2 集群间 HA

推荐部署为两个独立数据集群，加一个第三故障域的 witness：

```text
Cluster A（当前 ACTIVE）
  多个 scale-out owner
  每个 shard 的 COLD 数据
  本集群 WARM
  HA/Paxos voter

Cluster B（当前 STANDBY）
  多个 scale-out owner
  同一 shard 的 COLD 数据副本
  本集群 WARM
  HA/Paxos voter

Witness W
  Paxos voter
  不保存 vector payload
```

最小的跨集群 Paxos group 是：

```text
cluster-level voter A + cluster-level voter B + witness W
quorum = 2 / 3
```

集群内部可以继续有本地副本和本地故障转移，但跨集群 Paxos 不应把所有
scale-out owner 直接混成一个无边界的 voter 集合。建议每个 shard 由一个
集群级 replica endpoint 代表该集群参与跨集群 Paxos；集群内部再负责把 chosen
entry 复制到本集群的 COLD 副本。

如果只有 Cluster A 和 Cluster B 两个 voter：

```text
quorum = 2
```

安全性仍然成立，但任意一个集群失效后无法继续强一致写入。若要求一个集群
故障后另一个集群仍可写，必须增加 witness 或第三个数据故障域。

## 4. 命名空间和版本边界

下列字段必须分开，不能把 scale-out 版本当成 HA 版本：

| 字段 | 作用 | 所属层次 |
|---|---|---|
| `topology_epoch` | 集群内 owner ring 和迁移版本 | scale-out |
| `min_write_epoch` | 集群内拒绝旧 topology 的写请求 | scale-out |
| `key_version` | 单个 key 的逻辑数据版本 | 数据记录 |
| `owner_epoch` | 迁移过程中 target owner 的 key lease/fence | scale-out，需持久化后才能用于 HA |
| `ha_authority_epoch` | 当前服务集群的跨集群写 fencing 版本 | HA |
| `paxos_ballot` | Paxos 提案和 leader 版本 | HA |
| `commit_index` | 已 chosen 的权威写入位置 | HA/COLD |
| `applied_index` | 本地 COLD 已 apply 的位置 | COLD |
| `owner_generation` | WARM slot 物理位置复用版本 | WARM |
| `resource_generation` | channel/transport 资源生命周期版本 | transport |

重要规则：

```text
topology_epoch_A != topology_epoch_B 不代表发生 HA 切换。
scale-out final fence 成功不代表 Cluster B 获得写权限。
owner_generation 变化不代表 COLD 数据版本变化。
```

## 5. HA 状态机

每个集群对每个 shard 维护独立的 HA 状态：

```text
HA_FAULT
  -> HA_FOLLOWER
  -> HA_PROPOSER
  -> HA_LEADER
  -> HA_FENCED / HA_READONLY
```

状态约束：

```text
HA_FOLLOWER：可以 apply chosen entry，可以提供明确标记的本地读，不接受普通写。
HA_PROPOSER：正在通过 Paxos prepare/accept 获取更高 ballot，不接受普通写。
HA_LEADER：拥有未过期的 ha_authority_epoch，并持续证明 quorum 存在。
HA_FENCED：拒绝所有写，撤销本地 active endpoint，等待人工或控制面恢复。
HA_READONLY：可以服务允许陈旧的读，但不产生新的权威写入。
```

健康检查只能触发候选状态变化，不能直接把节点变成 `HA_LEADER`。成为
`HA_LEADER` 必须满足：

```text
1. 获得更高的 paxos_ballot。
2. 从 quorum 获得当前 chosen/commit 水位。
3. 提交新的 ha_authority_epoch。
4. 本地 COLD 至少 apply 到要求的 commit_index。
5. 写门打开前完成旧 active endpoint 的撤销或 fencing。
```

## 6. 跨集群权威写路径

`VADD` 的跨集群强 HA 路径：

```text
1. 客户端根据 HA topology 找到 ACTIVE Cluster。
2. ACTIVE Cluster 根据本地 active_ring 路由到 shard owner。
3. owner 校验：
     ha_authority_epoch
     paxos_ballot
     topology_epoch
     request_id
     key_version
4. 生成 Paxos log entry：
     cluster_id
     shard_id
     key / key_hash
     key_version
     vector payload
     ha_authority_epoch
     paxos_ballot
     log_index
     payload_crc
5. 本地 append 并发送到 Cluster B replica 和 witness。
6. quorum chosen 后，entry 才成为权威写入。
7. Cluster A 和 Cluster B 各自 apply 到本地 COLD。
8. ACTIVE Cluster 确认本地 apply 成功后更新 WARM。
9. 返回 VADD OK。
```

推荐第一版的确认边界：

```text
quorum chosen
  -> ACTIVE COLD apply
  -> return OK
```

Cluster B 的 apply 可以在 chosen 后异步追平，但必须保证：

```text
chosen entry 永久保存在可 replay 的 Paxos log 中；
Cluster B 未追平时不能声称自己具备完整强一致读能力；
Cluster B 接管前必须等待 apply_index >= committed barrier。
```

WARM-first 不能作为跨集群强 HA 的确认路径：

```text
WARM update -> return OK
```

否则 WARM 丢失、进程崩溃或集群切换时，已确认写入可能没有任何权威副本。

## 7. HA 切换顺序

### 7.1 正常切换

```text
1. 控制面停止 Cluster A 接收新写，进入 WRITE_DRAIN。
2. 等待 in-flight 写完成或返回 retry。
3. Cluster A 撤销 HA active endpoint，关闭本地写门。
4. Cluster B 通过 Paxos 获取更高 ballot 和 ha_authority_epoch。
5. Cluster B 等待本地 applied_index 追上 commit barrier。
6. Cluster B 打开写门，发布 HA topology snapshot。
7. 客户端收到旧 epoch 后 refresh，并重新路由到 Cluster B。
```

### 7.2 Cluster A 故障

```text
1. Cluster B 只根据健康检查进入候选状态。
2. Cluster B 必须从 B + W 或 A + B 的 quorum 获得 Paxos 权限。
3. B 提交新的 ha_authority_epoch。
4. B 拒绝所有低于该 epoch 的旧请求。
5. B apply chosen log，再发布 ACTIVE。
```

如果 B 只有自己可见、无法联系 quorum：

```text
B 只能 READONLY 或 FAULT，不能自动接管写入。
```

### 7.3 旧 Cluster 恢复

恢复的 Cluster A 必须：

```text
1. 以 FOLLOWER 启动，不能使用本地旧 active 状态自升主。
2. replay Paxos log，校验 ha_authority_epoch 和 commit_index。
3. 追平 COLD。
4. 清空或标记旧 WARM 为不可见，重新 lazy promote。
5. 通过 control plane 明确 rejoin，才可成为 standby voter。
```

## 8. 网络分区与脑裂矩阵

| 场景 | Cluster A | Cluster B | 结果 |
|---|---|---|---|
| A、B、W 全部可达 | 可写 | follower | 正常 |
| A-B 可达，W 故障 | 可写 | follower | A+B 形成 quorum |
| A 故障，B+W 可达 | 不可写 | 可写 | B 接管 |
| B 故障，A+W 可达 | 可写 | 不可写 | A 继续 |
| A 只连 W，B 只连自己 | 不可写或等待 lease | 不可写 | 无合法 quorum |
| A、B 完全分区，W 只连 A | 可写 | 只读 | A+W 形成 quorum |
| 旧 A 仍有本地请求但 lease 过期 | 拒写 | 新 owner 可写 | fencing 生效 |

这里的关键不是谁先看到故障，而是每一个写请求都必须通过：

```text
当前 ha_authority_epoch
当前 Paxos ballot
未过期 lease
quorum proof
```

VRRP advertisement 或客户端 topology refresh 只能加速收敛，不能替代这些检查。

## 9. 读路径和 WARM 恢复

### 9.1 ACTIVE 集群本地读

```text
WARM hit
  -> 校验 owner_generation、commit_index
  -> 返回

WARM miss
  -> 本地 COLD Get
  -> 校验 version/CRC
  -> promote 到 WARM
```

### 9.2 STANDBY 集群读

默认只允许 local-read 语义，并显式标记可能落后：

```text
applied_index < cluster commit barrier
  -> 不提供 strong read
```

需要强一致读时：

```text
向当前 HA leader 获取 read barrier
等待本地 applied_index >= barrier
再读 COLD 或满足版本要求的 WARM row
```

### 9.3 WARM 丢失

WARM 不是跨集群复制目标。集群切换或进程重启时：

```text
1. 读取本地 COLD 和 committed Paxos log。
2. replay applied_index 之后的 chosen entry。
3. 清空 HOT/WARM metadata。
4. 用 COLD lazy promote 重建 WARM。
```

## 10. 集群内扩容与跨集群 HA 的交互

两种操作必须分开编排：

```text
集群内扩容：改变 Cluster A 自己的 active/standby ring。
跨集群复制：复制逻辑写入，不复制某个集群的物理 WARM handle。
跨集群切换：改变 HA active cluster，不直接重算另一个集群的 ring。
```

### 10.1 Scale-out 期间的写入

Cluster A 内部扩容时：

```text
客户端仍写 active owner；
source 通过 delta outbox 追平 target；
final fence 后提交 target owner lease；
top_ctl 发布新的 Cluster A full active topology。
```

这些迁移 delta 需要作为逻辑写入进入跨集群 Paxos 顺序，或者至少带有明确的
`key_version` 并由 HA replication adapter 复制。不能只复制 target 的最终
WARM slot，因为 slot 是集群本地物理位置。

### 10.2 HA 切换期间的扩容

不允许同时进行未经协调的两个 owner 变更：

```text
禁止：Cluster A 正在 cutover，同时 Cluster B 依据旧 ring 接管写入。
```

推荐顺序：

```text
1. 先让 scale-out migration 达到可复制的 barrier。
2. 将新的 key owner、key_version、tombstone 纳入 Paxos/COLD 状态。
3. 完成跨集群 apply/catch-up。
4. 再执行 HA cluster handoff。
```

如果故障迫使 B 在 A 的 scale-out 未完成时接管，B 必须以 Paxos committed
状态为准，并将未完成迁移的 key 保持在旧 owner 视图，不能根据单方面的
candidate topology 直接接收写入。

## 11. 现有机制与 HA 的关系

| 现有机制 | 直接语义 | HA 中的使用方式 |
|---|---|---|
| `active_ring/standby_ring` | 集群内路由和迁移候选视图 | 每个集群各自维护，不作为跨集群 owner |
| `topology_epoch` | 集群内拓扑版本 | 不替代 `ha_authority_epoch` |
| `min_write_epoch` | 拒绝迁移期间的旧写请求 | 继续用于集群内 scale-out；HA 另设写门 |
| `owner_epoch` | key migration lease/fence | 可复用单调拒旧思想；HA 需要持久化、按 shard 纳入 Paxos |
| final fence | 防止 source 在 cutover 后继续写 | 可作为 cluster handoff 的本地 drain/fence 子步骤 |
| `needs_dual_write` | scale-out 迁移期的双目标写规划 | 不是 Paxos quorum，也不是跨集群 ACK |
| `owner_generation` | WARM slot 复用和 stale handle 拒绝 | 只保护物理缓存定位，不保护 HA 写权限 |
| `DEST_COMMITTED` | target 已应用迁移快照 | 不能等同于 Paxos chosen/cluster committed |

因此，现有代码可以提供：

```text
key 级迁移状态机
stale request 拒绝模式
final fence + lease 的本地实现经验
topology snapshot 发布和客户端 refresh 机制
```

但 HA 还需要新增：

```text
跨集群 Paxos replication adapter
持久化 ballot / commit_index / applied_index
per-shard ha_authority_epoch
cluster-level write gate
失去 quorum 后的 FENCED 处理
跨集群 snapshot、replay、catch-up 和 rejoin
```

## 12. 推荐数据对象

跨集群 Paxos entry 的逻辑字段：

```text
cluster_group_id
shard_id
request_id
key / key_hash
key_version
operation: PUT | DELETE
topology_epoch_of_origin
source_owner
target_owner_if_migrating
paxos_ballot
log_index
payload_crc
vector payload or durable payload reference
```

HA 写门的运行时状态：

```text
cluster_id
shard_id
role
ha_authority_epoch
paxos_ballot
chosen_index
applied_index
lease_expiry
quorum_healthy
fenced
```

跨集群 snapshot 必须至少包含：

```text
snapshot_id
shard_id
last_included_index
ha_authority_epoch
topology metadata
key_version/tombstone state
payload checksum
```

## 13. 实施阶段

### P0：先完成边界隔离

1. 在代码和协议命名中区分 `topology_epoch` 与 `ha_authority_epoch`。
2. 明确 `needs_dual_write` 不是 HA replication ACK。
3. 将 COLD/Paxos 设计成唯一权威写入路径。
4. WARM 只作为可丢失、可恢复缓存。

### P1：单 shard 跨集群 Paxos

1. 建立 Cluster A、Cluster B、Witness 三个 voter endpoint。
2. 实现 proposal、accept、chosen、commit_index 持久化。
3. 实现 COLD apply 和 replay。
4. 写请求必须带 authority epoch 和 request_id。

### P2：HA write gate 和切换

1. 增加 `HA_FOLLOWER/PROPOSER/LEADER/FENCED` 状态机。
2. quorum 丢失后关闭写门，而不是只撤销 endpoint。
3. 新 leader 提交更高 `ha_authority_epoch`。
4. 增加旧 epoch、旧 ballot 和过期 lease 的拒绝测试。

### P3：与 scale-out 对接

1. 将迁移 delta 转成可复制的逻辑 mutation。
2. 保持每个集群独立的 active/standby ring。
3. 在 cutover 前验证 Paxos/COLD barrier 已追平。
4. 禁止 candidate topology 直接成为跨集群写权限。

### P4：snapshot、catch-up、rejoin

1. 增加 COLD checkpoint 和 Paxos log compaction。
2. 新集群从 snapshot + log replay 加入。
3. WARM 统一走 lazy recovery，避免复制物理 slot。
4. rejoin 前完成 authority epoch、commit index 和 topology 对账。

## 14. 必须验证的故障场景

```text
1. A/B/W 全可达：A 写入，B apply，chosen 后返回 OK。
2. A 故障：B+W 接管，旧 A epoch 写入全部拒绝。
3. W 故障：A+B 仍可写，记录 quorum 降级。
4. A/B 分区且无 witness：两边都不能写。
5. A/B 分区但 W 只连 A：A 写，B 只读。
6. A 在 quorum commit 后、COLD apply 前崩溃：B replay 后保留写入。
7. WARM region 丢失：COLD 保留数据，读路径 lazy promote。
8. Cluster A scale-out 进行中时故障：B 以 Paxos committed state 接管，
   不接受未完成 candidate topology 的写入。
9. 旧客户端携带旧 topology_epoch 和旧 ha_authority_epoch：收到明确 stale
   错误并 refresh，不得静默写入。
10. 旧 owner 进程暂停超过 lease TTL 后恢复：fencing token 拒绝旧写。
```

## 15. 指标和可观测性

HA/Paxos：

```text
ha_role
ha_authority_epoch
paxos_ballot
chosen_index
applied_index
quorum_healthy
replication_lag
leader_stepdown_count
fence_reject_count
stale_authority_reject_count
```

集群内 scale-out：

```text
topology_epoch
min_write_epoch
migration_state
outbox_depth
final_fence_count
owner_lease_commit_count
source_fence_slow_path_count
```

COLD/WARM：

```text
cold_apply_ns
cold_replay_entries
cold_snapshot_index
warm_hit
warm_promote
warm_stale_handle_reject
```

## 16. 参考实现位置

集群内 scale-out 现有实现：

- [vemb_v16_client_topology.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_client_topology.c:97)：active/standby ring 与写计划。
- [vemb_v16_storage.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_storage.c:2142)：topology epoch 和旧写请求判断。
- [vemb_v16_storage.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_storage.c:2517)：final cutover 后发布 full active topology。
- [vemb_v16_storage.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_storage.c:3419)：target owner lease commit。
- [tlc_core.c](/Users/szza/codespace/work/hpc-redis/src/tlc_core.c:515)：stale migration snapshot 拒绝。

已有 COLD/Paxos/WARM 总体原则：

- [VEMB_V16_COLD_PAXOS_WARM_HA_DESIGN.md](/Users/szza/codespace/work/hpc-redis/docs/VEMB_V16_COLD_PAXOS_WARM_HA_DESIGN.md:11)：COLD/Paxos 是权威，WARM 是可恢复缓存。
- [VEMB_V16_SCALE_OUT_NODE_MIGRATION_DESIGN.md](/Users/szza/codespace/work/hpc-redis/docs/VEMB_V16_SCALE_OUT_NODE_MIGRATION_DESIGN.md:48)：集群内 migration epoch、final fence 和 target lease。

LVS/Keepalived 参考：

- [vrrp.c](/Users/szza/codespace/work/LVS-master/tools/keepalived/keepalived/vrrp/vrrp.c:736)：VIP 接管、退主和优先级选举。
- [vrrp_scheduler.c](/Users/szza/codespace/work/LVS-master/tools/keepalived/keepalived/vrrp/vrrp_scheduler.c:692)：健康检查影响 effective priority。
- [ipwrapper.c](/Users/szza/codespace/work/LVS-master/tools/keepalived/keepalived/check/ipwrapper.c:721)：real server quorum/hysteresis。

## 17. 最终边界

```text
LVS/F5 控制面：谁接收入口流量。
集群内 scale-out：这个集群内部哪个 owner 负责 key。
跨集群 Paxos：哪个集群拥有 COLD 的当前写权威。
COLD：已确认数据的持久状态机。
WARM/UB：本地或远程高速缓存，不作为 HA 写权威。
```

三者必须按这个顺序判断：

```text
入口是否指向本集群？
本集群是否是当前 HA authority？
本地 shard 是否持有有效 owner/lease？
Paxos 是否达到 quorum？
最后才进入 COLD/WARM 数据路径。
```
