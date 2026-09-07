# TLC HA 111/112 真机测试场景

## 1. 文档目的

本文记录当前 111/112 双机真机回归中的两条测试脚本、各自的数据流和验收边界：

- `benchmark/tlc_ha_replica_ub_111_to_112.sh`
- `benchmark/tlc_ha_redis_tcp_111_to_112.sh`

两条脚本使用相同的双向 UB Replica ring 配置，默认部署为 111 Leader、112
Follower，且 Follower 的 TX/RX offset 与 Leader 方向相反。它们测试的层次不同，
不能相互替代。

本文描述的是固定角色的数据同步回归，不覆盖自动切主故障注入。HA TCP 控制面与 UB
数据面拆分已在 M11 落地（见
[心跳设计文档](./TLC_HA_FAILOVER_HEARTBEAT_DESIGN.md) M11 部分），但两条脚本
Redis TCP 场景已接入 Replica 内部 TCP 控制面：节点自行 bind
`HPC_REDIS_HA_CONTROL_BIND_HOST:HPC_REDIS_HA_CONTROL_PORT`，并连接
`HPC_REDIS_HA_PEER_HOST:HPC_REDIS_HA_PEER_PORT`；UB 继续承载数据帧。

## 2. 场景对比

| 脚本 | 测试入口 | 主要目的 | 典型问题定位范围 |
| --- | --- | --- | --- |
| `tlc_ha_replica_ub_111_to_112.sh` | 直接启动 `tlc_ha_replica_ub_node_ut` | 验证 Replica UB 数据面、协议状态机和 COLD/resync 行为 | ring、frame、ACK、heartbeat、snapshot、AOF、replay、COLD |
| `tlc_ha_redis_tcp_111_to_112.sh` | 启动真实 `redis-server`，使用 `sdk_ha_replica_tcp` | 验证生产 Redis 请求入口已经接入 Replica UB | Redis TCP、proxy、SuperNode、COLD append、WARM apply、TCP read |

脚本名中的 `redis_tcp` 指 Redis 客户端访问协议，不是 HA 控制面 TCP。当前两条脚本
中的跨节点复制数据仍然通过 UB Replica ring 传输。

## 3. 公共前置条件

两条脚本都需要：

1. 可通过 SSH 访问 Node 111 和 Node 112；
2. 环境文件 `examples/tlc_ha_replica_ub_111_to_112.env`，或由
   `TLC_HA_UB_CONFIG` 指定的配置文件；
3. 两台机器上的 UB data/ACK 路径可读写；
4. 非零、64 字节对齐且互不重叠的 Replica ring offset；
5. 脚本可以同步源码并在远端构建所需二进制。

两条脚本都会先 reset Replica ring。真机测试使用独立的 COLD/WARM 目录或 COLD
目录，测试结束时通过 trap 停止由脚本启动的进程。

## 4. 直接 UB Replica 回归

### 4.1 数据流

该脚本不启动 Redis server，不经过 Redis 请求分发路径：

```text
111: tlc_ha_replica_ub_node_ut leader
        |
        |  Replica UB TX/RX ring
        v
112: tlc_ha_replica_ub_node_ut follower
```

Leader 和 Follower 直接通过 node UT 生成和消费 HA event、ACK、heartbeat、snapshot
以及 replay 控制帧。该路径适合隔离底层复制和恢复问题。

### 4.2 执行阶段

脚本按以下顺序执行：

```text
远端环境、设备权限、path 和 offset 检查
    -> 同步源码并构建 node UT/visibility UT
    -> 独立 UB CC/NC visibility 前置
    -> reset 111/112 Replica ring
    -> 普通 event 复制、append ACK、heartbeat、async apply
    -> snapshot chunks
    -> checkpoint install
    -> M5 handoff、final ACK、H + 1 replay
    -> M5 timeout/abort cleanup
    -> M6 自动 GAP -> AOF repair
    -> M6 retention 不足 -> snapshot fallback
    -> M7 soft compact、hard pressure abort
    -> Follower COLD recovery
    -> reset ring、Follower 重启、手工 AOF range replay
```

### 4.3 主要验收点

- Follower 的 `durable_seq` 是无 gap 的连续前缀；
- Leader 能收到 append ACK，并观察到 peer durable/applied progress；
- heartbeat 能更新 peer health 和进度；
- checkpoint、snapshot chunk、checksum、generation 和 handoff 边界正确；
- GAP 能选择 AOF replay；AOF retention 不足时能回退到 snapshot；
- M5 完成后普通流从 `H + 1` 开始；
- timeout/abort 能释放 pin、清理 artifact 并解除发送侧 fence；
- Follower 能从持久 COLD 恢复 WARM；
- Follower 重启后可以在受控测试中执行指定 AOF range replay。

### 4.4 结论边界

该脚本证明底层 Replica event 流已经在真实 UB 设备上工作，但不经过
`redis-server` 请求分发。因此它不能单独证明：

- Redis TCP VADD/VREM 已接入复制；
- 生产 proxy/SuperNode 路径正确；
- 自动 reconnect、自动 failover 或 owner fencing 已完成；
- HA TCP 控制面已经建立并能按顺序重建 UB channel。

其中 `ub_cc_nc_visibility_ut` 是独立的 UB 可见性环境前置。它的结果不能直接等同于
Replica ring 的完整双向正确性结论；当前真机环境仍可能受到特定设备映射权限和低地址
共享范围限制。

## 5. Redis TCP/SDK 端到端回归

### 5.1 数据流

该脚本启动真实 `redis-server`，客户端只使用 Redis TCP endpoint：

```text
SDK TCP write
    -> 111 Redis Leader
    -> Redis proxy
    -> SuperNode worker
    -> key hash / shard
    -> Leader COLD append
    -> Leader event sink
    -> Replica UB ring
    -> 112 Follower COLD append / ACK
    -> Follower async apply
    -> 112 Redis TCP read
    -> SDK verify
```

脚本使用独立的 local-SHM WARM region、manifest 和 COLD 目录，避免与其他回归的运行
状态互相污染。启动顺序是先启动 112 Follower，再启动 111 Leader。

### 5.2 写入和读取场景

默认 workload 为 `dim=16`、`max_vectors=32768`：

1. SDK 向 111 Leader 写入 10,000 个唯一初始 VADD；
2. 对 `ha-tcp:0` 执行 VADD update、VREM、再次 VADD；
3. 对 `ha-tcp:9999` 执行 VREM，形成 tombstone；
4. 由 112 Follower 的 Redis TCP endpoint 读取并校验所有保留向量、更新值和删除结果；
5. 检查两端 `appended`、`durable`、`applied`、`peer_*` progress，以及
   `peer_health=1`。

因此默认总 event 数是 `10004`，SDK 应输出：

```text
sdk_ha_replica_tcp: PASS events=10004
```

### 5.3 COLD 重启恢复场景

首轮写入和读取完成后，脚本：

```text
停止 111/112
    -> reset Replica ring
    -> 使用相同 Follower COLD 目录只重启 112
    -> SDK verify 读取 112
```

预期输出：

```text
sdk_ha_replica_tcp: VERIFY PASS events=10004
```

该阶段验证 Follower group commit 形成的 durable COLD 前缀可以在 WARM reset 后恢复，
并不验证 checkpoint resync、自动 reconnect 或角色提升。

### 5.4 结论边界

该脚本证明生产 Redis 请求入口能够完成：

```text
Redis TCP request -> Leader COLD -> UB Replica -> Follower COLD/WARM -> Redis TCP read
```

它不替代底层 UB 回归，也不能单独证明：

- snapshot、M5/M6/M7 所有边界状态机都已覆盖；
- heartbeat 超时会自动触发 `SUSPECT -> CANDIDATE -> MASTER`；
- Leader 通知旧节点降级和 owner fencing 已完成；
- HA TCP 控制面与 UB 数据面的先后建链屏障已完成。

## 6. 两条脚本的互补关系

推荐先运行底层 UB 回归，再运行 Redis TCP/SDK 回归：

```text
UB Replica 基线通过
    -> Redis server/SDK 集成验证
    -> 根据失败层次定位 ring/protocol 或 Redis 业务入口
```

如果 UB 脚本失败，应优先检查 ring、frame、ACK、COLD、replay 或 snapshot；如果 UB
脚本通过而 Redis TCP 脚本失败，应优先检查 Redis proxy、SuperNode、shard、COLD event
sink、WARM apply 或 SDK 校验逻辑。

两条脚本共同验证的是固定角色下的数据同步和恢复。自动切主、真实 TCP reconnect、
旧 Leader 恢复后的 lineage transition，以及”先 HA TCP、后重建 UB channel”的控制面
生命周期，应在 M11 后续故障回归中单独测试。

## 7. 相关文档

- [TLC HA 跨节点 resync 设计](./TLC_HA_CROSS_NODE_RESYNC_DESIGN.md)
- [TLC HA 心跳检测与切主设计](./TLC_HA_FAILOVER_HEARTBEAT_DESIGN.md)
- [TLC HA 数据同步实现计划](./TLC_HA_DATA_SYNC_IMPLEMENTATION_PLAN.md)
