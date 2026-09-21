# TLC HA 数据同步代码落地计划

本文将 [TLC HA 数据同步设计](./TLC_HA_DATA_SYNC_DESIGN.md) 转换为代码实现和
测试计划。本文只描述落地顺序，不重新定义数据同步协议。

## 当前完成情况

截至当前版本，M0-M6 的核心数据同步、持久化、复制协议、heartbeat、同机 resync
以及固定 UB 数据面验证已经完成；M8 中 Source 侧迁移 cutover 的普通 `DEL` 复制
语义也已经落地。M7 的切主、fencing 和自动角色转换，以及 M4 的完整生产启动流程
和自动重连仍未完成。

跨节点 checkpoint/AOF tail resync 的最新实现状态以
`TLC_HA_CROSS_NODE_RESYNC_DESIGN.md` 的 M3-M7 状态总表为准；本文早期阶段描述中
仍出现“跨节点 blob 搬运待接入”等历史措辞时，均视为已被当前 Replica channel、文件化
artifact、自动 GAP repair 和 retention->snapshot 闭环实现所替代。

已完成的代码与测试包括：

- 新 COLD/AOF/checkpoint 格式、`ha_term`/`topology_epoch` 分离、单机无副本路径；
- Leader event、Follower Replica AOF、append ACK、异步 apply、连续 seq 和
  checksum/GAP/CONFLICT 处理；
- STREAM/TCP 同机双进程复制、heartbeat 进度单调合并和 liveness/timeout；
- checkpoint export/import、固定边界 tail replay、损坏 blob 拒绝和 generation
  保留；
- 真实机脚本固定 COLD 目录、Follower 重启 recovery、Leader AOF replay range 和
  重启后重复 event 的 append ACK 校验；
- 固定 UB Replica ring、cacheable/non-cacheable 映射约定，以及 111/112 双节点 HA
  数据面直接回归；独立双向可见性前置仍受设备可映射范围限制；
- `redis-server` 内部 HA Replica 生命周期接线，以及 TCP SDK 写入 Leader、通过
  Follower TCP SDK 读取最终状态的 111/112 端到端回归；
- migration `CUTOVER` 后 Source Leader 追加普通 `DEL` 并通过 Replica 流发送给
  Source Follower，`SOURCE_GC` 仅做 Source 本地拓扑清理；
- migration-control UT 的容量、transport、ring cache policy、实际 shard 和
  epoch 初始值修正；proxy 销毁时跳过未创建 channel，避免无效 channel close。

已完成的本地/真实机回归结果：

```text
tlc_ha_replica_ut: PASS
tlc_replica_cold_ut: PASS
tlc_resync_ut: PASS
tlc_ha_replica_process_ut: PASS
benchmark/vemb_v16_migration_control_ut: all tests passed
111/112 direct UB Replica: PASS (10K events, Follower apply, COLD recovery durable=10000)
111/112 Redis TCP/SDK HA: PASS (10004 events, Follower apply, reset-WARM COLD recovery)
make -C src vemb_v16_server: PASS
```

本地 macOS 沙箱没有 `/dev/shm`，因此共享内存型 UT 只能在 111/112 真实环境执行；
本地已完成上述非共享内存目标编译和 `git diff --check`；共享内存相关 UT 已在
111 机器执行并通过。

### 真实 UB 验证结论（2026-09-03）

111/112 使用实际 Replica ring 已完成直接回归：111 Leader 的
`dev4@134217728 -> dev8@134217728` 数据路径和 112 Follower 的
`dev9@201326592 -> dev13@201326592` ACK/反向路径共同完成 10,000 个 event；
Leader 输出 `accepted=10000`，Follower 输出 `PASS applied`，随后以同一 Follower
COLD 目录运行 `follower-recover`，确认 `durable=10000`。Follower 的本地 TX/RX
offset 必须与 Leader 相反，脚本已按此修正。

独立 `ub_cc_nc_visibility_ut` 的单一 offset 前置不等同于上述 Replica ring 回归。
默认值已从 7 GiB 降为 `268435456`（256 MiB）：111->112 在该地址完成 1,000 次并输出
`NOT_REPRODUCED`；但 112->111 在同一地址上，111 的 `/dev/obmm_shmdev4` ACK 映射
返回 `EPERM`。64 MiB 虽可完成映射，却无法完成跨机 data/ACK 握手。历史上 7 GiB
仅验证过 112->111，111->112 同样因 `dev4` 映射 `EPERM` 失败。

因此当前不得宣称完整脚本的双向独立可见性前置已通过。要恢复该结论，需由 UB 环境
提供四个设备均可访问的低地址双向共享范围，或将可见性工具扩展为独立 data/ack
offset；本轮不继续验证该项。

### Redis TCP/SDK 端到端验证结论（2026-09-03）

新增 `benchmark/tlc_ha_redis_tcp_111_to_112.sh`：它以环境变量为两台原有
`redis-server` 配置 HA role、peer、term、COLD 目录和固定 UB Replica ring，而不改变
客户端协议。111 Leader 与 112 Follower 分别使用独立 local-SHM WARM region；客户端
`clients/c/sdk_ha_replica_tcp` 只向 111 的 TCP endpoint 写入，并从 112 的 TCP
endpoint 读取。

真实机运行在 `dim=16`、`max_vectors=32768` 下完成 10,000 个初始 VADD、对 key 0
执行 VADD update、VREM、VADD，以及删除最后一个 key，共 10,004 个有序 COLD/Replica
event。SDK 输出 `PASS events=10004`；Leader progress 为 `appended=durable=
peer_accepted=peer_durable=peer_applied=10004`，Follower progress 为
`appended=durable=applied=peer_durable=peer_applied=10004`，两端 `peer_health=1`。
Follower TCP SDK 验证所有保留向量、key 0 的更新值以及最后 key 的 tombstone。

随后脚本停止两端、reset Replica ring 与 Follower WARM region，以原 COLD 目录只重启
Follower；只读 SDK 输出 `VERIFY PASS events=10004`。这验证的是 Follower group commit
最终 durable 前缀和 COLD 自动恢复，不将其扩大解释为 checkpoint resync、自动重连或
failover。

## 真实机测试脚本规划

截至 2026-09-03，真实机回归分为两条独立脚本。两者都使用 111 Leader、112
Follower、固定的双向 UB Replica ring path/offset，且 Follower 本地 TX/RX offset
与 Leader 方向相反。它们分别验证传输/COLD 协议和正常 Redis server 请求入口，
不能相互替代。

| 脚本 | 入口与目的 | 已覆盖的主要结果 |
|---|---|---|
| `benchmark/tlc_ha_replica_ub_111_to_112.sh` | 独立 Replica node UT；定位 UB ring、协议和 COLD 行为 | event 复制、append ACK、heartbeat、async apply、Follower COLD recovery、Follower 重启后的手工 AOF range replay |
| `benchmark/tlc_ha_redis_tcp_111_to_112.sh` | 正常 `redis-server` + TCP SDK；验证生产写路径已接入 Replica | SDK VADD/VREM -> Leader COLD -> UB Replica -> Follower COLD/apply -> Follower TCP SDK read，以及 reset WARM 后的 Follower COLD recovery |

### 直接 UB Replica 回归

`benchmark/tlc_ha_replica_ub_111_to_112.sh` 是底层数据面基线。它同步并构建 node
UT 和 visibility UT，检查设备权限、path、offset 对齐和 TX/RX 不重叠，然后按以下
顺序执行：

```text
远端环境/设备检查
    -> 编译 Replica UB UT
    -> 执行独立 UB CC/NC path/offset 可见性前置
    -> reset 指定 Replica ring
    -> 启动 112 Follower
    -> 启动 111 Leader
    -> 多批次 PUT/UPDATE/DEL 数据同步
    -> append ACK、heartbeat、peer health 和 progress 校验
    -> 以同一 Follower COLD 目录执行 recovery
    -> reset ring、重启 Follower、执行 Leader AOF range replay
    -> 输出两端日志并清理进程
```

脚本的 Replica regression PASS 证明的是底层事件流已经在实际 UB 设备上完成；它不经
`redis-server` 请求分发，因此适合定位 ring、frame、ACK、COLD append 与恢复问题，
不用于替代生产入口的端到端结论。

已完成的直接 UB 回归为 10K events，确认 Leader `accepted`、Follower async apply、
append ACK、heartbeat 和 progress；随后以同一 Follower COLD 目录运行
`follower-recover`，并在 reset ring 后重启 Follower、运行手工 AOF range replay。
该 replay 是受控 UT 场景，不等同于生产自动重连或自动 tail resync。

独立 `ub_cc_nc_visibility_ut` 是 Replica ring 回归的环境前置，不得与其混为一个
正确性结论。当前默认单一 visibility offset 已降为 256 MiB：111 -> 112 可完成
1,000 次并输出 `NOT_REPRODUCED`，但 112 -> 111 的 ACK 映射在 111
`/dev/obmm_shmdev4` 返回 `EPERM`。64 MiB 可映射却不能完成 data/ACK 握手；历史 7 GiB
offset 也只曾验证单方向。因此，目前不能宣称同一低地址 offset 的双向独立可见性
preflight 通过。该环境限制不否定已经通过的固定 Replica ring 回归。

### Redis TCP/SDK 端到端回归

`benchmark/tlc_ha_redis_tcp_111_to_112.sh` 使用相同 UB 配置，但将写入和读取放回
正常 server/SDK 路径。脚本同步所需源码，在两台机器构建 `redis-server` 与
`clients/c/build/sdk_ha_replica_tcp`，创建独立 COLD/WARM/manifest 目录，reset 两个
Replica ring，然后先启动 112 Follower、再启动 111 Leader：

```text
TCP SDK VADD/VREM
    -> Redis TCP proxy
    -> SuperNode worker
    -> key hash，选择并锁定 key-meta shard
    -> Leader COLD append
    -> Leader COLD event sink
    -> UB Replica ring
    -> Follower COLD append / append ACK
    -> Follower async apply
    -> Follower TCP SDK 读取验证
```

当前默认 workload 为 `dim=16`、`max_vectors=32768`：10,000 个唯一初始 VADD，随后
对 `ha-tcp:0` 执行 VADD update、VREM、VADD，并对 `ha-tcp:9999` 执行 VREM，共
10,004 个有序 COLD/Replica event。SDK 在 112 验证所有保留向量、key 0 的最终更新值
及最后 key 的 tombstone；两端日志同时检查启动成功、progress 达到 10,004 和
`peer_health=1`。

写入回归结束后脚本停止两端，reset Replica ring 与 Follower WARM region，以相同
Follower COLD 目录只重启 Follower，并用 SDK `verify` 模式读取数据。`VERIFY PASS`
证明 Follower group commit 的最终 durable 前缀可用于 COLD recovery；它不扩大为
checkpoint resync、自动重连或 failover 的结论。

真实机成功记录位于：

```text
benchmark/results/tlc_ha_redis_tcp/20260903_170511-36050
```

### 执行与扩展规则

代码改动先运行相应本地 UT 和目标构建；修改 Replica frame、ring layout、UB 可见性或
COLD/ACK 协议时，运行直接 UB 脚本；修改 Redis 生命周期、proxy、worker、TLC 写入
接线或 SDK 时，额外运行 TCP/SDK 脚本。两个脚本均保留远端 artifacts 和两端日志，以
便区分客户端请求、Leader local COLD、Replica transport、Follower durable 与 apply
失败。

未来的 descriptor ring + payload arena v2 不能静默复用 version 1 的共享映射。必须
为 v2 增加独立 layout/commit/reclaim UT，并在两条真实机脚本上重新执行上述回归；具体
布局、唯一 publisher 前提和验收要求见
[`TLC_HA_REPLICA_RING_ARENA_OPTIMIZATION.md`](./TLC_HA_REPLICA_RING_ARENA_OPTIMIZATION.md)。

当前仍未覆盖或未完成的边界如下，脚本 PASS 不得替代这些结论：

```text
PENDING: single low shared offset 的双向独立 UB visibility preflight
NOT_IMPLEMENTED: cross-node checkpoint + AOF tail resync
NOT_IMPLEMENTED: automatic reconnect / failover / fencing / role promotion
NOT_IMPLEMENTED: v2 descriptor ring + payload arena layout
```

`NOT_IMPLEMENTED: cross-node checkpoint + AOF tail resync` 的协议、COLD staging、
AOF retention pin 与验收设计见
[`TLC_HA_CROSS_NODE_RESYNC_DESIGN.md`](./TLC_HA_CROSS_NODE_RESYNC_DESIGN.md)。

## 1. 已确认的实现边界

### 1.1 复制单元

每个 Node group 是一个独立复制单元：

```text
Leader(group N) -> Follower(group N)
```

一个 Node group 只包含一个 HPC-Redis Leader 和一个 HPC-Redis Follower，对应一个
独立 key 数据集；一个 HPC-Redis 实例只属于一个 Node group。

不同 Node group 不共享 `seq`、AOF、replication queue、ACK、heartbeat 进度或
safe point。跨 Node group 的 ownership migration 由 topology control plane 负责，
不由复制层合并日志。

每个 Node group 的隔离边界还包括独立的 Leader/Follower runtime、UB data
channel/ring、COLD/AOF cursor、durable/apply 进度、heartbeat/liveness、resync 状态和
HA role/term。任一 Node group 的 event、连接、队列、故障或 failover 状态都不能被
其他 Node group 使用、合并或推进；一个组的断链、重连或 resync 不影响其他组并行
复制。

### 1.2 新持久化格式

当前项目仍在开发中，AOF 和 checkpoint 直接切换到设计中的新格式，不保留旧格式
解析、兼容或迁移分支。非新格式直接拒绝加载。

AOF event 固定保存：

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

`ha_term` 是 Node group owner 任期，`topology_epoch` 是数据/迁移拓扑版本；两者
必须独立校验和持久化。

### 1.3 无副本模式

没有 peer 配置时，Node group 运行在 `STANDALONE` 模式：

```text
不建立 replica channel
不启动 heartbeat
不等待 Follower ACK
不执行 Follower apply
```

本地 COLD append、flush、recovery 和读路径保持现有语义。建议诊断值为：

```text
role                 = STANDALONE
ha_term              = 0
replicated_seq       = 0
follower_durable_seq = 0
ha_safe_point_seq    = leader_durable_seq
```

## 2. 模块职责

### 2.1 现有模块改造

```text
src/tlc_cold.h / src/tlc_cold.c
    新 event header、AOF append/replay、checkpoint header/manifest
    本地进度和 Follower replica AOF 写入

src/tlc_core.h / src/tlc_core.c
    Leader event 构造、Follower event apply、HA state 接入
    topology_epoch 与 ha_term 的分离校验

src/vemb_v16_tlc.c
    外部 HA/COLD 生命周期和参数形状校验边界
```

### 2.2 新 HA 模块

第一阶段新增一个独立模块：

```text
src/tlc_ha_replica.h
src/tlc_ha_replica.c
```

负责 Node group HA 状态、角色转换、进度合并、heartbeat 编解码和 owner/term
metadata。只有当传输逻辑复杂到需要独立生命周期时，才拆出
`tlc_ha_transport.[ch]`。

HA 状态至少包含：

```text
hpc_node_id
peer_node_id
role
health
ha_term
leader_appended_seq
leader_durable_seq
follower_durable_seq
replicated_seq
applied_seq
last_heartbeat_received_ns
peer_health
connection_state
```

### 2.3 固定配置的 UB 数据面

当前阶段借鉴 UB RPC Channel 的启动方式：每个 Node group 在进程启动时固定配置
Leader/Follower 的 UB endpoint、tx/rx ring、slot 布局、`hpc_node_id`、
`peer_node_id` 和 role。启动时完成映射、布局和固定副本身份校验，随后直接进行
数据通信，不引入动态 channel 生命周期状态机：

```text
进程启动
    -> 打开并校验固定 UB tx/rx ring
    -> 启动 Replica runtime
    Leader tx_ring     -> Follower rx_ring   EVENTS
    Follower tx_ring   -> Leader rx_ring     ACK/heartbeat
```

Replica channel 可以与 Server/UB RPC 使用同一个固定 mapped path，但必须为每个
Node group 的两个方向预留不重叠的 `mmap_offset` 区域：

```text
shared UB path
    -> RPC region
    -> Node group N: Leader -> Follower Replica ring @ offset A
    -> Node group N: Follower -> Leader Replica ring @ offset B
    -> Node group N+1: 两个 Replica ring @ 其他 offset
```

每个 offset 必须满足映射对齐要求，且对应区域大小覆盖 ring header、slot 和必要的
保留空间；配置加载时要验证同一路径上的区域不重叠，也不能覆盖已有 RPC region。
ring reset 只允许作用于指定的 `(path, offset, channel direction)`，不能重置整条
path 上的其他 channel。

当前不在 M4 引入 TCP 控制面握手、动态 `CHANNEL_OPEN/CLOSE` 或自动重连。已有
epoll TCP listener 后续可复用为低带宽控制面，用于 channel 关闭、重建和恢复协调；
这些操作必须在 Replica runtime 停止且没有 ring producer/consumer 后执行。

当前 Replica frame 和 UB mapped SPSC ring 实现位于：

```text
src/tlc_ha_replica.h
src/tlc_ha_replica.c
```

实现可以复用现有 UB Channel 的 mapped ring、内存映射和固定配置约定，但不复用
lookup/migration RPC 的 pending `request_id`、固定 request/response union 或业务
handler。HA 需要连续 event 流、异步累计 ACK、heartbeat 和按 seq 重建，因此必须
使用独立 Replica ring 和 frame kind。

实现采用分层方式：

```text
tlc_ha replica protocol
    -> dedicated replica frame
    -> UB mapped ring transport
```

建议先将 ring open/close/reset、publish/poll、slot 生命周期和 ring-full 状态抽取
为通用模块：

```text
src/vemb_v16_ub_ring.h
src/vemb_v16_ub_ring.c
```

现有 `vemb_v16_ub_rpc` 和 HA replica channel 共同使用该模块，但不共享 lookup/
migration wire、pending 表或业务 handler。

Replica 必须使用独立 ring 配置，至少包含：

```text
shared path
Node group id
channel direction
mmap_offset
slot_count
slot_bytes
Leader -> Follower: replica event/data ring
Follower -> Leader: ACK/heartbeat/control ring
```

如果多个 frame kind 共用一条 ring，header 必须明确区分 event、ACK、heartbeat 和
resync control，并保证 event 的连续 seq 处理顺序。key/value 超过单个 slot 的
稳定容量时，使用固定 descriptor 加专用 shared payload area 或有界分片，不能截断
event。

UB ring 只是传输缓冲，不是可靠日志。ring 满、reset 或进程重启时必须背压或重建，
不能丢弃已接受 event；Leader 始终从 AOF 按 `follower_durable_seq + 1` 补发。
UB channel 不负责持久化、term fencing 或故障恢复，这些职责仍属于 COLD 和
`tlc_ha`。

无完整固定 UB 配置时不启动 Replica data channel。当前阶段不定义控制连接断开后的
自动状态转换；数据面恢复由后续 channel 生命周期阶段负责，不能把未确认 event
当作已复制。

## 3. Replica API 约定

复制数据面相关 API 的名称显式包含 `replica`；通用 checkpoint blob 生命周期 API
沿用 `checkpoint` 语义命名。所有 API 都在头文件中写明前置条件、所有权、并发
限制和错误语义。

| API | 语义 |
|---|---|
| `tlc_cold_read_replica_batch()` | Leader 从 AOF 读取指定连续 seq 区间，不修改 COLD 状态；起点超出保留范围时返回 resync 所需错误 |
| `tlc_cold_submit_replica_batch()` | Follower 使用 Leader 原始 seq 写入本地 AOF，不重新分配 seq；在线复制以 append ACK 返回，resync 保持 fsync durable ACK |
| `tlc_cold_export_checkpoint()` | Leader 导出已校验 active checkpoint 的完整文件 blob，调用方负责释放 blob |
| `tlc_cold_import_checkpoint()` | Follower 校验并原子发布 checkpoint blob；仅接受空的 fenced COLD runtime |
| `tlc_cold_free_checkpoint_blob()` | 释放 checkpoint export 返回的 blob |
| `tlc_cold_get_replica_progress()` | 获取 Follower 本地 `follower_durable_seq` 和 `applied_seq` |
| `tlc_core_apply_replica_event()` | 将已经 durable 的 event 应用到 Follower 的 HA/WARM state；成功后推进 `applied_seq` |
| `tlc_ha_build_replica_heartbeat()` | 构造统一 heartbeat frame |
| `tlc_ha_handle_replica_heartbeat()` | 校验身份、term、checksum 后更新 liveness 和进度 |
| `tlc_ha_handle_replica_ack()` | Leader 校验 Follower durable ACK，并以最大值更新 `replicated_seq` |
| `tlc_ha_start_replica_sync()` | 根据 Follower 进度选择增量复制或 checkpoint + AOF tail |
| `tlc_ha_stop_replica_sync()` | 停止同步发送并进入断线/恢复状态 |
| `tlc_ha_persist_replica_term()` | 原子持久化 owner、角色和 `ha_term` |
| `tlc_ha_promote_replica()` | 在控制面完成 fencing 后提升 Follower 为 Leader |

`tlc_cold_submit()` 只用于本地 Leader/Standalone 写入，不与
`tlc_cold_submit_replica_batch()` 混用。

## 4. 分阶段实现

### M0：冻结代码契约

内容：

- 冻结 event、checkpoint、ACK 和 heartbeat 字段及整数宽度；
- 冻结 frame 长度、checksum 覆盖范围和网络字节序；
- 冻结 role、health、错误码和状态转换；
- 明确本地 replay 允许历史 term，网络 replica ingress 拒绝过期 term；
- 明确 `ha_safe_point_seq` 只在本地派生，不进入 heartbeat。

验收：头文件契约和本文设计文档中的字段、错误和状态定义一致。

### M1：COLD 新格式和单机路径

修改 `tlc_cold.[ch]`、`tlc_core.[ch]`：

- event 增加 `ha_term/topology_epoch`；
- AOF 和 checkpoint 使用新 header；
- checksum 覆盖新字段；
- recovery 分别校验两个字段；
- 保持本地 append、flush、replay 和读路径不变。

测试：

- 新 event 编解码和 checksum；
- AOF 重启恢复；
- checkpoint 发布、加载和校验；
- 无副本写入、恢复和 compact；
- 新字段不改变单机读写行为。

### M2：基本的内存数据同步

实现不依赖网络和 Replica AOF 的最小闭环：

```text
Leader event
    -> Follower 接收
    -> seq/版本校验
    -> Follower apply
```

测试 `PUT`、`DEL/tombstone`、多 event 顺序、重复 event、版本更新、不同 WARM
placement，以及多个 Node group 的隔离。

验收：Leader 与 Follower 的逻辑 key/value/version/tombstone 最终一致。

### M3：Follower Replica 持久化、网络复制和异步 apply

实现 `tlc_cold_submit_replica_batch()`：

- Follower 使用 Leader seq；
- 连续 seq 才能进入 AOF；
- 相同 checksum 的重复 event 幂等；
- 不同 checksum 返回冲突；
- append 后返回 append ACK，后台 group commit 定时 fsync；
- append 后异步调用 `tlc_core_apply_replica_event()`。

已实现的网络和调度部分：

- Leader 本地 COLD append 成功后由 `tlc_core` event sink 投递到 Replica sender queue；
- 独立 stream 传输 event batch frame 和累计 append ACK；
- Follower receiver 先写 Replica AOF 并等待 append，再投递有界 apply queue；
- apply worker 按原 seq 调用 `tlc_core_apply_replica_event()`；
- Leader/Follower 各自的复制队列采用 cache-line 对齐的 SPSC 无锁环形队列：
  Leader 由唯一 COLD writer 生产、sender 消费，Follower 由 receiver 生产、apply
  worker 消费；满/空时采用自适应退避，短暂 `cpu_relax` 后进入 `nanosleep(1us)`，
  不在正常读路径增加锁或同步；
- queue 满、frame 校验失败、GAP、CONFLICT 或 apply 错误会停止该 Replica runtime，
  由后续 resync 流程接管。

测试 Follower 重启恢复、durable-before-apply、存储失败、GAP、duplicate 和
checksum conflict。上述基础路径已由 `tlc_replica_cold_ut`、
`tlc_ha_replica_ut` 和 `tlc_ha_replica_process_ut` 覆盖。

网络集成测试覆盖 Leader -> Follower event、append ACK 和异步 WARM apply；固定
UB channel 的启动、ring reset、断线后的手动重建和 AOF 补发属于 M4。

### M4：固定配置 UB channel、cursor 和数据复制

按启动时固定配置打开独立 UB replica channel，先跑通数据面；传输层不改变复制协议
语义：

- event batch frame；
- append ACK frame；
- frame 长度和 checksum；
- event、ACK、heartbeat、resync control 的 frame kind；
- Leader AOF cursor 和 seq/index 定位；
- queue 背压；
- 断线重连和 `follower_durable_seq + 1` 续传。

当前已实现：独立 Replica UB mapped SPSC ring、固定 slot frame 的长度/checksum
校验、COLD 稀疏 seq cursor、按连续 seq 的 AOF range replay，以及
`tlc_ha_replica_replay_from()` 补发入口。TCP/Unix blocking stream 保留为兼容和
测试传输，不作为生产数据面主通道。

固定配置启动、ring reset、双节点 UB ring 数据通路和手动补发已由
`tlc_ha_replica_ub_111_to_112.sh` 在 111/112 完成验证。剩余工作是将该数据面接入
正式服务启动流程，并补齐 TCP 控制面复用、HELLO 身份握手、channel
open/close/reconnect 状态机和自动恢复。

`benchmark/tlc_ha_replica_process_ut` 已先覆盖同机双进程 STREAM 基线：Leader 和
Follower 使用独立 COLD/WARM runtime，通过 socketpair 完成 event 持久化、异步
apply、累计 durable ACK 及 heartbeat 健康状态校验。该测试不等同于 UB ring
跨进程测试。

真实机脚本 `benchmark/tlc_ha_replica_ub_111_to_112.sh` 已接入 111/112 的源码同步、
远端编译、ring reset 和双节点启动。当前脚本使用 scale-out 对应的本地设备映射：
111 默认 `TX=/dev/obmm_shmdev4,RX=/dev/obmm_shmdev13`，112 默认
`TX=/dev/obmm_shmdev9,RX=/dev/obmm_shmdev8`。其中 111->112 使用
`dev4@134217728 -> dev8@134217728`，112->111 使用
`dev9@201326592 -> dev13@201326592`；112 端启动和 reset 都必须按该反向 offset
初始化。该两条 Replica ring 路径已通过直接 10K HA 回归和 Follower COLD recovery。

`ub_cc_nc_visibility_ut` 的独立单一 offset 前置当前不能作为双向 `VISIBLE` 结论：
256 MiB 仅完成 111->112，112->111 的 ACK 映射在 `dev4` 返回 `EPERM`；64 MiB 虽
可映射但不能完成握手。默认配置使用 256 MiB，避免原 7 GiB 地址；完整脚本在环境
提供统一低地址双向共享范围前，不应输出整体 `PASS`。这不影响已经完成的 Replica
ring frame/ACK/heartbeat/apply 和持久化恢复回归。

不得使用现有 UB RPC 的 pending request/response 机制承载 event 流；Replica ACK
是独立的异步累计确认。当前阶段测试 UB frame 编解码、ACK 丢失/重复、GAP、ring
满、ring reset、固定配置重启和 AOF 重读补发；TCP stream 作为兼容传输保留。

已有 UB ring 基础测试继续保留；新增 Replica 专用测试验证 event descriptor、
shared payload 生命周期、ring-full 背压、固定配置启动和手动 channel 重建。UB 与
TCP 的吞吐/延迟对比属于性能测试，不替代正确性测试。

### M5：Heartbeat 和健康状态

当前已实现统一 heartbeat frame、固定副本身份/role/ha_term 校验、durable/progress
进度单调合并、接收端本地 monotonic liveness 记录和超时标记，并提供 peer health 与
最近 heartbeat 时间查询 API。heartbeat 发送与现有 Replica UB/STREAM 数据面复用，
 不触发 COLD flush；发送路径对并发控制帧与 event frame 做串行化。

Replica heartbeat worker 每秒输出一次 `appended_seq`、`durable_seq`、`applied_seq`、
`peer_accepted_seq`、peer durable/applied 进度和 peer health，用于区分 AOF append、
后台 group commit 与异步 apply 的背压位置。

实现统一 heartbeat：

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

处理要求：

- 合法身份、term、长度和 checksum 才能更新 liveness；
- append ACK 与 heartbeat durable 进度分别单调合并，不能混用语义；
- 同一 term 下进度不得回退；
- timeout 只使用接收端本地 monotonic 时间；
- `sent_at_ns` 仅用于诊断；
- heartbeat 不触发 flush。

测试空闲 heartbeat、乱序/重复 heartbeat、旧 term、错误 peer、checksum 错误、
timeout 和恢复重连。`benchmark/tlc_ha_replica_ut` 已覆盖上述 heartbeat 正常、
进度不回退、stale term、错误身份、checksum 错误及 timeout 恢复场景。

### M6：Snapshot resync 和 compact

当前已实现 `tlc_cold_clone_checkpoint()`、Replica COLD base seq 初始化，以及
`tlc_core_resync_from()` 的同步编排：从 Leader active checkpoint 固定边界，向空的
`RECOVERING/FENCED` Follower 安装已校验 generation，恢复 checkpoint 状态后按连续
AOF tail 补到固定边界。`tlc_cold_export_checkpoint()` 和
`tlc_cold_import_checkpoint()` 提供完整 checkpoint 文件 blob 的导出/导入，复用现有
header、record 和 generation checksum；导入只接受空的 fenced Replica COLD，校验或
发布失败不会覆盖已有有效 generation。历史版本的 clone/resync 仅在同一进程内编排；
当前实现已在 Replica channel 增加受校验的 checkpoint 传输流程，并完成文件化 artifact、
自动 GAP repair 和 retention->snapshot 闭环。

实现 checkpoint + AOF tail：

```text
选择 checkpoint
    -> 固定 Leader durable 边界 B
    -> Follower 下载、校验并原子安装
    -> replay AOF tail
    -> durable_seq 到达 B
    -> 恢复增量复制
```

Follower 在 resync 期间保持 `RECOVERING/FENCED`。

`benchmark/tlc_resync_ut` 已覆盖 checkpoint seq 边界、tail replay、Follower WARM
收敛和 durable_seq 到达固定边界，以及 checkpoint export/import、损坏 blob 拒绝、
已有 generation 保留和 compact 后 checkpoint 继续可用。

compact 由 checkpoint retention/floor 决定，不再由 `ha_safe_point_seq` 阻塞；
replication cursor pin 的 segment 不能删除，segment/index/catalog 必须同步替换。

后续仍需补充 compact 后增量起点失效、安装中断、tail 缺失、并发 cursor、index
重建和 resync 完成后的增量复制；当前 UT 已覆盖 snapshot 校验失败和 compact 后
active checkpoint 可导出。

### M6 后续：真实 Follower 恢复与跨节点 resync

#### Follower 持久化 COLD 目录重启恢复

该测试已扩展 `tlc_ha_replica_ub_node_ut`：Leader 和 Follower 接收固定的
`TLC_HA_UB_COLD_DIR`，并提供 `follower-recover` 阶段重新打开已有目录；脚本默认
为每次运行生成独立的 `/tmp` 目录，也可通过 env 文件固定目录。Leader 还可通过
`TLC_HA_REPLAY_START`/`TLC_HA_REPLAY_END` 显式调用 `tlc_ha_replica_replay_from()`。

```text
leader_init  -> follower_init -> 写入并同步一批 event
             -> 停止 Follower（保留 COLD 目录和 Replica ring）
             -> Leader 继续追加 event
follower_restart -> 从本地 AOF 恢复 durable 状态
                  -> 由 Leader 按 durable_seq + 1 手动 replay
verify           -> 校验 key/value/version/tombstone 和 progress
```

实现要点：

- Follower 重启时只能打开已有的 Replica AOF，不得清空目录或重置有效 generation；
- 恢复顺序必须是 COLD recovery、Replica runtime 启动、再执行缺口 replay；
- 已经 durable 的 event 必须幂等，未 durable 的 event 只能从 Leader AOF 补发；
- 测试 harness 需要暴露 `start_seq/end_seq` replay 参数，调用
  `tlc_ha_replica_replay_from()` 时禁止并发追加新的 Leader event，满足 API 的
  AOF 枚举前置条件；sender 在空闲时保持运行以消费补发范围；
- 该阶段验证的是“持久化恢复 + 手动补发”，不宣称自动 reconnect。

当前真实机脚本已完成固定 COLD 目录传递、首轮复制后的 Leader replay range，以及
停止后的 Follower AOF recovery 校验；跨节点 checkpoint/AOF tail resync 已由 Replica
channel 接入并在 111/112 脚本中验证，脚本当前仍明确输出自动 reconnect/failover、HA
control plane 和 lineage transition 的 `TODO`。

#### 真实跨节点 checkpoint + AOF tail resync

历史版本的 `tlc_cold_export_checkpoint()`/`tlc_cold_import_checkpoint()` 只完成同进程
blob 生命周期；当前实现已在 Replica channel 接入受校验的 checkpoint 传输流程：

```text
Leader 选择 active checkpoint，固定 durable 边界 B
    -> RESYNC_BEGIN(generation, checkpoint_seq, blob_bytes, checksum)
    -> RESYNC_CHUNK(有序 chunk，逐 chunk checksum)
    -> RESYNC_END(校验完整 blob)
    -> Follower 写临时文件并原子安装 checkpoint
    -> Follower recovery/import，保持 RECOVERING/FENCED
    -> Leader 按 checkpoint_seq + 1 replay AOF tail 至 B
    -> Follower durable/apply 到 B
    -> 恢复普通 event 增量复制
```

落地要求：

- 新增独立 resync frame kind 或等价 control frame，不能复用 event/ACK 的字段语义；
- chunk 必须有序、有界，不能因为单个 slot 容量截断 checkpoint blob；
- Follower 只能在临时文件完整校验、generation 和整体 checksum 通过后原子替换；
- 安装期间拒绝普通 apply，完成后再按连续 seq 重建尾部；
- Leader 固定边界 B 后，checkpoint 和 tail 使用同一 generation/term 约束；
- 传输失败、chunk 缺失、checksum 错误或 tail GAP 时保留旧 generation，并重新进入
  resync，不能部分发布；
- resync 完成后必须验证 `durable_seq == B`、`applied_seq == B`，再解除 FENCED。

该流程已扩展 `tlc_ha_replica` 的 wire/frame、发送队列和 Follower 状态机，并接入真实机
脚本；`tlc_resync_ut` 继续负责同进程 checkpoint 边界和损坏 blob 测试，真实 111/112 脚本
已覆盖 UB event/ACK/heartbeat、snapshot chunks、install、tail、handoff 及
retention->snapshot。

### M7：owner/term、fencing 和 failover

实现 owner/term metadata 的加载、原子持久化和角色状态机：

```text
读取 local/peer term
    -> 计算递增 ha_term
    -> 持久化并 fsync
    -> 完成 fencing
    -> Follower 晋升 Leader
    -> 恢复写入口
```

不实现自动 quorum 选主，角色切换由控制面触发。

测试 term 重启恢复、旧 event/heartbeat 拒绝、fencing 前禁止写入、seq 不重置、
晋升后重新复制以及故障恢复。

### M8：ownership migration 接入

由 topology control plane 编排：

```text
Source Leader 推送 baseline + delta outbox
    -> Target Leader 导入并写入目标 AOF
    -> Target group 内部复制
    -> checkpoint barrier
    -> final fence + target lease commit
    -> topology control plane mark CUTOVER，Source 拒绝旧 owner 请求
    -> Source Leader 追加普通 DEL，并复制到 Source Follower
    -> 发布新的 topology_epoch
    -> topology control plane 执行 SOURCE_GC（Source 本地清理）
```

迁移中的 `VREM` 才产生带版本 tombstone delta，并通过 outbox 发送到 Target；
CUTOVER 完成后 Source Leader 追加普通 source `DEL`，并通过 HA 复制流发送到
Source Follower。测试迁移失败重试、snapshot + delta、
source/target seq 隔离和 cutover 期间不返回旧 owner 数据。

`CUTOVER` 和 `SOURCE_GC` 仅由 topology control plane 维护，不进入 HA AOF 或
Replica wire event。CUTOVER 完成且 Source 不再接受旧 owner 请求后，Source Leader
对迁出 key 追加普通 `DEL`，Follower 按正常 seq/ACK/apply 流程接收该 `DEL`，从而
保持 Source Leader/Follower 的 key-set 一致。SOURCE_GC 只执行 Source 本地的
旧 metadata/location/warm 引用清理，不产生额外复制事件。HA event 因此无需新增
迁移操作，也无需携带 `target_owner`、`migration_id` 或迁移范围。相关单 key、range、
coordinated scaleout 和 source DEL 行为已纳入 `vemb_v16_migration_control_ut`，并
在 111 机器完整通过。

## 5. 后续性能优化 TODO

当前先保持固定容量 SPSC 队列和现有自适应退避，不因阻塞等待机制引入额外运行时
复杂度。后续根据性能测试结果评估 futex 通知优化：

```text
进程内 HA SPSC queue：使用 FUTEX_PRIVATE 做空/满通知
跨进程 host shared-memory ring：使用共享 futex 做空/满通知
UB device ring：仅在确认设备映射支持 futex 后启用，否则保留设备完成事件或轮询退避
STREAM/TCP：继续使用 socket blocking/epoll，不使用 futex
```

实施时需要验证：共享 futex 的映射和对齐约束、head/tail 的内存序、丢失唤醒、
进程退出和 ring reset 时的等待者唤醒，以及 futex 相比当前 `cpu_relax()` +
`nanosleep(1us)` 的吞吐、尾延迟和 CPU 占用。该优化不得改变 SPSC 队列所有权、
复制顺序、背压和 durable-before-apply 语义。

## 6. 测试执行顺序

测试必须按以下顺序推进，前一阶段通过后再引入后一阶段：

```text
基本内存同步
    -> Replica AOF 持久化
    -> 复制 frame/ACK/GAP 协议
    -> Heartbeat/liveness
    -> Snapshot resync/compact
    -> Failover/fencing
    -> Ownership migration
```

建议测试目标：

```text
benchmark/tlc_sync_ut.c
benchmark/tlc_cold_ut.c
benchmark/tlc_replica_cold_ut.c
benchmark/tlc_ha_replica_ut.c
benchmark/tlc_ha_replica_process_ut.c
benchmark/tlc_ha_replica_ub_node_ut.c
benchmark/tlc_resync_ut.c
benchmark/vemb_v16_migration_control_ut.c
benchmark/tlc_ha_replica_ub_111_to_112.sh
```

每层同时覆盖成功路径、重复消息、乱序消息、资源不足和 I/O 故障。双节点集成
测试使用本地 TCP 或 `socketpair`，验证真实 frame、断线、重连和状态转换。

## 7. 最终验收标准

- 无副本模式的写入、恢复和读取行为与当前单机逻辑一致；
- Leader/Follower 的逻辑状态按 event 顺序收敛；
- Follower append ACK 只表示本地 AOF append 已完成；durable 进度由 heartbeat 和诊断值暴露。
- `replicated_seq`、`applied_seq` 和 heartbeat 进度只增不减；
- 复制 queue 丢失后可以从 AOF 重建；
- 增量范围不足时只能进入 snapshot resync，不能跳过 GAP；
- heartbeat 超时不依赖跨机器时间；
- 切主前完成 term 持久化和 fencing；
- compact 不删除本地恢复或复制 cursor 仍需要的数据；
- 不同 Node group 始终保持独立复制状态；
- 异步复制的尾部数据丢失边界通过监控暴露，不能宣称零数据丢失。

## 8. 构建和回归入口

实现对应阶段后执行最小相关测试，并在跨模块修改后执行完整 TLC 构建：

```text
make -C benchmark tlc_cold_ut
make -C benchmark tlc_sync_ut
make -C benchmark tlc_replica_cold_ut
make -C benchmark tlc_ha_replica_ut
make -C benchmark tlc_ha_replica_process_ut
make -C benchmark tlc_ha_replica_ub_node_ut
make -C benchmark tlc_resync_ut
make -C benchmark vemb_v16_migration_control_ut
make -C benchmark vemb_v16_tlc_ut
make -C src vemb_v16_server
```

所有代码提交前执行：

```text
git diff --check
```
