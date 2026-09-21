# TLC HA 基于 keepalived VRRP 的故障检测与切主设计

## 1. 目标与范围

将故障检测和客户端路由从 HPC-Redis 内部移交给 keepalived VRRP：

```text
keepalived VRRP        → 故障检测 + VIP 漂移（替代 M8 双通道 heartbeat 状态机）
VIP 直接绑定网卡       → 客户端路由（替代未实现的 HA_NOT_OWNER/HA_REDIRECT）
keepalived notify 脚本 → 触发 HPC-Redis 内部 promote/demote
HPC-Redis 内部         → 仅保留数据面切换（fencing/term/drain/recovery）
```

### 1.0 为什么不需要 LVS 负载均衡

双机 active-passive 场景下，任意时刻只有一个 Master 接受写入。LVS ipvs
内核模块的核心能力（L4 负载均衡、权重分发、多后端轮询）在此场景没有意义。
只需要 keepalived 的 VRRP 协议管理 VIP 漂移即可：

```text
需要:  keepalived VRRP → VIP 在网卡之间漂移 → 客户端透明切换
不需要: LVS ipvs → 负载均衡 → 分发到多个 active 后端（没有多个 active）
不需要: ipvsadm → real_server weight 调整（不存在 virtual_server）
不需要: FullNAT/DR/TUN 转发模式（VIP 直接绑在网卡上，零转发开销）
```

VIP 直接绑定在当前 VRRP MASTER 的网卡上，客户端连接 VIP 等价于直连该机器，
数据路径无代理、无 NAT、无额外跳数。

本文覆盖一个 `hpc_node_id` 的双副本 Node group。多个 Node group 的 owner、
VIP 和切换相互独立。

### 1.1 故障模型

沿用现有假设：

1. 两台机器不会同时宕机；
2. 不考虑双向网络隔离（keepalived VRRP 同样依赖此假设）；
3. keepalived 进程随 OS 启动、停止时等价于节点不可达；
4. HPC-Redis 进程独立于 keepalived，redis-server 崩溃不等于节点崩溃。

## 2. 架构概览

```text
                      CLI / SDK Client
                            │
                            ▼
                      ┌───────────┐
                      │ VIP:6379  │  keepalived VRRP 管理
                      └─────┬─────┘  (直接绑在网卡上，无中间转发)
                            │
              ┌─────────────┴─────────────┐
              ▼                           ▼
     ┌─────────────────┐        ┌─────────────────┐
     │  Node A (111)   │        │  Node B (112)    │
     │  redis-server   │        │  redis-server    │
     │  keepalived     │  VRRP  │  keepalived      │
     │  COLD/WARM      │◄──────►│  COLD/WARM       │
     │                 │ 组播/   │                  │
     │     UB TX ring ─┼─单播──┼→ UB RX ring      │
     │     UB RX ring ←┼────────┼─ UB TX ring      │
     │                 │        │                  │
     │  TCP control ───┼────────┼─ TCP control     │
     │  (direct, 非 VIP)│       │  (direct, 非 VIP) │
     └─────────────────┘        └─────────────────┘
```

三条独立通信路径：

```text
Client ─── VIP:6379 ──── redis-server   客户端数据（VIP 直接绑网卡，零转发）
Node A ─── UB ring ──── Node B          Replica 数据面（共享内存，直连）
Node A ─── TCP:port ─── Node B          Replica 控制面（TCP，直连，非 VIP）
```

VIP 绑定在当前 VRRP MASTER 的网卡上，客户端连接 VIP 等价于直连该机器。
Replica 控制面和 UB 数据面始终是两节点直连，不经过 VIP。

## 3. keepalived 配置

### 3.1 VRRP 实例

两台机器的配置几乎相同，只有 `priority` 不同：

111 机器 `/etc/keepalived/keepalived.conf`：

```text
vrrp_script chk_hpc_redis {
    script "/opt/hpc-redis/keepalived/scripts/ha_healthcheck.sh"
    interval 2                    # 每 2 秒检查一次
    weight 0                      # 1.2.2: 失败时进入 FAULT 并释放 VIP
    fall 3                        # 连续 3 次失败才判定不健康
    rise 2                        # 连续 2 次成功恢复健康
}

vrrp_instance HPC_REDIS_HA {
    state BACKUP                  # 两台都初始为 BACKUP
    interface eth0
    virtual_router_id 60
    nopreempt                     # 恢复后不抢回 VIP，避免双切
    priority 100                  # 111 高优先级 → 初始 Master
    advert_int 1                  # 1 秒 VRRP 广播间隔

    authentication {
        auth_type PASS
        auth_pass hpcredis
    }

    virtual_ipaddress {
        10.0.0.100/24             # CLI/SDK 连接的统一入口
    }

    track_script {
        chk_hpc_redis weight 0    # 健康失败必须撤销 MASTER 广播
    }

    notify_master "/opt/hpc-redis/keepalived/scripts/ha_notify.sh MASTER"
    notify_backup "/opt/hpc-redis/keepalived/scripts/ha_notify.sh BACKUP"
    notify_fault  "/opt/hpc-redis/keepalived/scripts/ha_notify.sh FAULT"
}
```

112 机器只改一行：`priority 90`。

### 3.2 VIP 工作原理

VIP 不经过任何代理或转发层，直接绑在网卡上：

```text
正常运行时：
  111 eth0:  10.0.0.1  +  10.0.0.100 (VIP)    ← ip addr add
  112 eth0:  10.0.0.2

  客户端 → 10.0.0.100:6379 → 直接到 111 的 redis-server
  零转发开销

故障切换后：
  111 eth0:  10.0.0.1                          ← VIP 被移除
  112 eth0:  10.0.0.2  +  10.0.0.100 (VIP)     ← VIP 绑上来

  客户端 → 10.0.0.100:6379 → 直接到 112 的 redis-server
  keepalived 发送 gratuitous ARP 刷新交换机 MAC 表
```

### 3.3 网络要求

```text
要求：111 和 112 在同一个 L2 广播域（同 VLAN/同子网）
原因：VRRP 组播 224.0.0.18 + gratuitous ARP 刷新交换机 MAC 表

如果 111/112 跨 VLAN：
  现代 keepalived 可使用 unicast_peer 单播模式替代组播；但本项目固定使用
  1.2.2，该版本源码不解析以下关键字，因此跨 VLAN 单播配置不能直接使用：

  vrrp_instance HPC_REDIS_HA {
      ...
      unicast_src_ip 10.0.0.1
      unicast_peer {
          10.0.0.2
      }
  }
```

111/112 已经通过 UB 共享内存直连，大概率满足同 L2 要求。

### 3.3 健康检查脚本

`/opt/hpc-redis/keepalived/scripts/ha_healthcheck.sh`：

```bash
#!/bin/bash
# 检查 redis-server 是否存活且内部状态正常
# 返回 0 = 健康，非 0 = 不健康

REDIS_CLI="/opt/hpc-redis/redis-cli"
PORT=6379
TIMEOUT=2

# 1. 进程存活检查
result=$($REDIS_CLI -p $PORT PING 2>/dev/null)
if [ "$result" != "PONG" ]; then
    exit 1
fi

# 2. HA 状态检查（非 FAULT）
ha_state=$($REDIS_CLI -p $PORT HA.STATE 2>/dev/null)
if [ "$ha_state" = "FAULT" ]; then
    exit 1
fi

exit 0
```

健康检查只验证进程存活和非 FAULT 状态。不检查 role 是否为 MASTER——
Follower 也是健康的，只是不接收客户端流量（VIP 不在 Follower 侧）。

### 3.5 notify 脚本

`/opt/hpc-redis/keepalived/scripts/ha_notify.sh`：

```bash
#!/bin/bash
# keepalived VRRP 状态变化时调用
# 参数: MASTER | BACKUP | FAULT

REDIS_CLI="/opt/hpc-redis/redis-cli"
PORT=6379
LOG="/var/log/hpc-redis-ha.log"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $1" >> "$LOG"; }

case "$1" in
    MASTER)
        log "keepalived -> MASTER, promoting HPC-Redis"

        # 同步调用 promote，阻塞直到完成或超时
        result=$($REDIS_CLI -p $PORT HA.PROMOTE 2>&1)
        rc=$?

        if [ $rc -eq 0 ]; then
            log "HA.PROMOTE succeeded: $result"
        else
            log "HA.PROMOTE failed (rc=$rc): $result"
        fi
        ;;

    BACKUP)
        log "keepalived -> BACKUP, demoting HPC-Redis"

        result=$($REDIS_CLI -p $PORT HA.DEMOTE 2>&1)
        log "HA.DEMOTE result (rc=$?): $result"
        ;;

    FAULT)
        log "keepalived -> FAULT, fencing HPC-Redis"

        $REDIS_CLI -p $PORT HA.FENCE 2>&1
        ;;
esac
```

脚本会把最近一次 notify 的结果原子写入
`/run/hpc-redis-keepalived/notify.status`。运维可以直接查询：

```sh
/opt/hpc-redis/keepalived/scripts/ha_notify.sh status
redis-cli HA NOTIFY STATUS
```

状态至少包含 `state`、`operation`、`result`、`rc`、`attempt`、时间戳和
命令输出。失败后可在确认 Redis 已恢复、VIP 归属正确后重试：

```sh
/opt/hpc-redis/keepalived/scripts/ha_notify.sh retry
```

重试只重放最近一次失败的 `MASTER`/`BACKUP`/`FAULT` 操作；`PROMOTE` 返回
“already leader”会按幂等成功记录。`ERR_APPLY_LAG` 是可恢复的 promote
就绪条件失败：节点保持 `FOLLOWER/BACKUP` 和写 fencing，复制继续进行，追平后
可以直接 retry。重试不能修复 Redis 已退出、仍处于 `FAULT/FENCED`、COLD progress
读取失败或 VIP 错误归属等问题，这些情况必须先恢复服务或人工介入。keepalived
不会因为 notify 失败自动回滚 VIP。

## 4. HPC-Redis 外部控制接口

### 4.1 新增 Redis 命令

在 `vemb_v16_tlc.c` 或等价的命令注册位置新增三个管理命令：

```text
HA.PROMOTE    同步晋升为 Leader/Master
HA.DEMOTE     同步降级为 Follower/Backup
HA.FENCE      紧急写 fencing
HA.STATE      查询当前 HA 状态（role, state, term, fenced）
HA.PROGRESS   查询复制进度
HA NOTIFY STATUS 查询最近一次 keepalived notify 结果
```

### 4.2 HA.PROMOTE 实现

```c
int tlc_ha_replica_external_promote(tlc_ha_replica_t *replica);
```

执行顺序：

```text
1. 检查当前 role == FOLLOWER
       否则返回 ERR_ALREADY_LEADER 或 ERR_INVALID_STATE

2. 检查当前 ha_state != FAULT
       FAULT 状态需要先人工恢复

3. 获取 COLD progress
       durable_seq 和 applied_seq 必须可读

4. 等待 applied_seq == durable_seq（有界超时 5s）
       WARM 未追平不能晋升
       超时返回 ERR_APPLY_LAG，保持 FOLLOWER/BACKUP 和写 fencing，允许后续 retry

5. 计算 new_term = max(local_term, last_seen_peer_term) + 1

6. 固定 promotion_takeover_seq = local_durable_seq

7. 调用 transition_role(LEADER, MASTER, new_term)
       内部执行：
         lock transition_mutex
         resync_fenced = true
         write_fenced = true（如果原来不是）
         resync_emission_gate = true
         drain inflight（ingress/apply/sender/producer/data_listener/replay_producer）
         持久化 term + takeover_seq（owner metadata v2）
         Core ha_term 同步
         发布 role=LEADER, state=MASTER
         unlock transition_mutex

8. 发送 LEADER_ANNOUNCE
       成功 → 等待 ROLE_ACK（非阻塞，后台处理）
       失败 → deferred，记录日志，不阻塞晋升

9. 解除 write_fenced（Core 开始接受写入）

10. 返回 OK + new_term
```

### 4.3 HA.DEMOTE 实现

```c
int tlc_ha_replica_external_demote(tlc_ha_replica_t *replica);
```

同步部分（函数返回前完成）：

```text
1. 检查当前 role == LEADER
       已经是 FOLLOWER 则直接返回 OK

2. 设置 Core write_fenced = true（立即停止新写入）

3. 设置 resync_fenced = true

4. 调用 transition_role(FOLLOWER, BACKUP, current_term)
       内部执行完整 drain 和状态清理

5. 确保 Core write_fenced = true（Follower 始终不可写）
   释放 resync_fenced（允许复制数据面恢复入站）

6. 返回 OK
```

异步部分（由 control_listener / heartbeat 线程驱动）：

```text
7. control_listener 线程自动重连新 Master TCP 控制面

8. 收到新 Master 的 LEADER_ANNOUNCE
       按正常 announce handler 处理
       包括三分支恢复判断（GAP/CONFLICT/追平）

9. 如果有数据落差，进入 RECOVERING
       由现有 resync 机制完成追平
       追平后 apply 线程自动 transition → BACKUP
```

设计说明：`HA.DEMOTE` 在 Redis 主线程执行（RESP 命令），同步阻塞等待
TCP 重连和 LEADER_ANNOUNCE 会阻塞所有客户端命令（包括 keepalived
healthcheck 的 PING），可能触发误切。安全不变量（write_fenced +
role=FOLLOWER）在同步部分已经建立，恢复是活性问题而非安全性问题。

### 4.4 HA.FENCE 实现

```c
int tlc_ha_replica_external_fence(tlc_ha_replica_t *replica);
```

紧急操作，只做两件事：

```text
1. atomic_store(Core->write_fenced, true)
2. atomic_store(replica->ha_state, FENCED)
```

不执行 drain 或角色转换，仅防止新写入。用于 keepalived FAULT 状态的快速
安全停止。

### 4.5 HA.STATE 和 HA.PROGRESS

复用现有查询 API：

```text
HA.STATE 返回：
    role:      LEADER | FOLLOWER | STANDALONE
    ha_state:  INIT | BACKUP | RECOVERING | MASTER | FAULT | FENCED
    ha_term:   <uint64>
    fenced:    true | false

HA.PROGRESS 返回：
    appended_seq:      <uint64>
    durable_seq:       <uint64>
    applied_seq:       <uint64>
    peer_durable_seq:  <uint64>
    peer_applied_seq:  <uint64>
    peer_health:       HEALTHY | FAILED | UNKNOWN
```

## 5. 故障切换完整流程

### 5.1 场景 A：Master 进程崩溃（节点存活）

```text
时间线：

T+0s    Node A redis-server 崩溃
T+2s    keepalived(A) chk_hpc_redis 第 1 次失败
T+4s    keepalived(A) chk_hpc_redis 第 2 次失败
T+6s    keepalived(A) chk_hpc_redis 第 3 次失败（fall=3 达标）
        weight=0 使 A 进入 FAULT，停止 VRRP 广播并释放 VIP
T+7s    keepalived(B) 检测到 A 的 MASTER 广播消失
        VRRP 状态转换：B 成为 MASTER
        VIP 10.0.0.100 漂移到 Node B
T+7s    keepalived(B) 执行 notify_master
          → ha_notify.sh MASTER
          → redis-cli HA.PROMOTE
T+7.5s  HPC-Redis(B) 完成内部晋升：
          applied_seq == durable_seq ✓
          ha_term++ → fsync
          transition_role(LEADER, MASTER)
          LEADER_ANNOUNCE 发送失败（A 进程不在）→ deferred
          write_fenced 解除
T+7.5s  VIP 生效，新连接到达 Node B
        （T+7 ~ T+7.5 的 0.5 秒窗口内，新连接收到短暂错误）

后续：
T+?     Node A redis-server 被重启
        keepalived(A) chk_hpc_redis 恢复健康
        keepalived(A) VRRP priority 恢复为 100
        但 nopreempt → A 保持 BACKUP，不抢回 VIP
        keepalived(A) 调用 notify_backup → ha_notify.sh BACKUP
          → redis-cli HA.DEMOTE
        HPC-Redis(A) 降级为 FOLLOWER
          → 连接 Node B TCP 控制面
          → 收到 LEADER_ANNOUNCE（B 重发 deferred announce）
          → 三分支恢复判断
          → replay/snapshot 追平
          → RECOVERING → BACKUP
```

### 5.2 场景 B：整机宕机

```text
T+0s    Node A 整机宕机（keepalived + redis-server 同时不可用）
T+3s    keepalived(B) 连续 3 个 VRRP 周期未收到 A 的广播（advert_int=1s）
        B 成为 VRRP MASTER
        VIP 漂移，notify_master 执行
        流程与场景 A 相同

后续：
T+?     Node A 整机恢复
        keepalived(A) 启动，state=BACKUP，nopreempt
        redis-server(A) 启动
        keepalived(A) 保持 BACKUP（nopreempt）
        notify_backup → HA.DEMOTE → 降级和恢复
```

### 5.3 场景 C：网络分区（受限保证）

```text
T+0s    A 和 B 之间网络断开
T+3s    keepalived(B) 未收到 A 的 VRRP → B 成为 MASTER → VIP 在 B
        keepalived(A) 未收到 B 的 VRRP → A 保持 MASTER（如果当前是 MASTER）

        ⚠️ 此时 A 和 B 都认为自己是 MASTER → 双写风险
```

**已有保护与安全边界**：

```text
1. VIP 漂移后，新客户端只能连到 B
   A 侧的影响仅限于已有长连接（不会有新连接到 A 的 VIP）

2. 网络恢复后，term 较低的一方收到 LEADER_ANNOUNCE 后自动降级
   ha_term 比较确保收敛

3. 与现有设计一致：完整网络分区在没有 witness/lease/STONITH 时
   不保证严格单主，应明确列为安全边界之外
```

HPC-Redis **不做** self-fence 来应对网络分区。原因见 §6.1。

### 5.4 VIP 切换期间的客户端行为

```text
切换窗口 ≈ VRRP 检测时间 + promote 内部耗时
         ≈ 3~7 秒（健康检查）或 3 秒（整机宕机）+ < 1 秒

窗口内行为：
  - 已建立连接：旧 Master 连接断开（进程崩溃）或返回错误（fenced）
  - 新建连接：VIP 已漂移但 promote 未完成时，短暂连接超时或错误
  - promote 完成后：所有新连接正常

客户端策略：
- 连接失败或收到 HA_FENCED → 短暂退避后重连 VIP
- VIP 地址不变，客户端无需更改配置

UB/AERON 客户端不能在原进程内迁移共享内存 ring。应用应销毁旧 owner channel，
调用 `vemb_v16_client_reconnect()`，并在 topology 广播节点直连地址时预先配置
`vemb_v16_client_set_ha_endpoint(client, <VIP>, <port>)`，使新的 AERON_ATTACH
经 VIP 到达当前 Master。复制控制连接仍使用节点直连地址；多 owner topology
不能把所有 owner 复用同一个 VIP。
```

## 6. 轻量心跳（保留）与 self-fence 决策

### 6.1 不做 self-fence

HPC-Redis **不** 基于 TCP 心跳超时自主执行 write_fenced。原因如下：

**场景分析**：

```text
场景 1：Follower 挂了（最常见故障）
    TCP 控制面超时 → 如果 self-fence → Master 停写 → 全局不可用
    但 Master 完全健康，是唯一存活节点
    → self-fence 把"备机故障"变成"服务中断" ❌

场景 2：Master 进程崩溃
    进程已死，没有进程可以 fence
    → self-fence 无作用 —

场景 3：Master keepalived 挂了，redis-server 还在
    112 keepalived 检测 VRRP 超时 → VIP 漂移 → HA.PROMOTE → 112 成为新 Master
    112 发送 LEADER_ANNOUNCE(term+1) → 111 收到后降级
    → LEADER_ANNOUNCE 已经提供保护 ✓，不需要 self-fence

场景 4：TCP 控制面断了，VRRP 正常
    如果 self-fence → Master 停写，但 keepalived 认为一切正常 → 不触发切主
    → 死锁：写被 fence，无人解除 ❌

场景 5：网络分区
    self-fence 也无法解决（两端都看不到对方）
    需要 witness/lease → 不在当前故障模型内 —
```

**三层已有保护已经足够**：

```text
保护层 1: VIP 漂移
    VIP 移走后，没有新客户端连到旧 Master
    影响范围仅限于已有长连接

保护层 2: LEADER_ANNOUNCE
    新 Master 发送高 term announce → 旧 Master 收到后 fence + 降级
    reactive（收到明确信号），不是 proactive（猜测超时）

保护层 3: keepalived notify_backup
    旧 Master 的 keepalived 恢复后 → BACKUP → notify_backup → HA.DEMOTE
    即使 LEADER_ANNOUNCE 送不到，keepalived 最终也会触发降级
```

**结论**：self-fence 重新引入了基于心跳超时的自主决策，与委托 keepalived 的
设计目标矛盾，且在最常见故障场景（Follower 挂了）中会误伤健康 Master。

### 6.2 保留心跳的用途

故障检测委托给 keepalived 后，HPC-Redis 内部仍保留轻量 TCP 心跳，但只用于
可观测性和进度交换，**不做任何 fence 或状态转换决策**：

```text
✅ 复制进度上报（durable_seq / applied_seq 交换）
✅ peer 连通性诊断日志（运维可观测，不影响行为）
✅ 运维命令 HA.PROGRESS 的数据来源
✅ heartbeat worker 每秒输出进度日志

❌ 不触发 write_fenced
❌ 不触发 ha_state 变化
❌ 不做切主决策
❌ 不做双通道 liveness 判断
```

### 6.3 心跳帧

继续使用现有 heartbeat frame，但移除 UB heartbeat（UB 只传数据帧）：

```text
TCP 控制面 HEARTBEAT:
    hpc_node_id
    peer_node_id
    ha_term
    role
    health
    sent_at_ns
    durable_seq
    applied_seq
    checksum

发送间隔: 1s
超时处理: 仅记录诊断日志，不改变任何状态
```

## 7. 代码变更清单

### 7.1 移除

| 模块 | 移除内容 |
|------|----------|
| ha_state 状态机 | 移除 `SUSPECT`、`CANDIDATE` 状态及其转换逻辑，重新编号（§13.3） |
| 故障检测 | 移除 `missed_heartbeat_count` 的阈值决策、`suspect_hold_down`、内部 `heartbeat_failure_pending` 状态和 `heartbeat_failure_threshold`；missed count 仅保留诊断用途（§13.1） |
| 自动晋升 | 移除 `replica_controller_try_promote()` 及 controller 线程中的自动晋升分支 |
| 双通道 heartbeat | 移除 `UB_HEARTBEAT`/`UB_HEARTBEAT_ACK` 帧种类；移除 `last_ub_heartbeat_ack_ns`、`ub_heartbeat_nonce`、`replica_dual_liveness_ok()` |
| UB heartbeat 发送 | heartbeat 线程不再向 UB ring 发送心跳 |
| self-fence | 移除心跳超时触发的 `write_fenced` 和 `FENCED` 状态转换；心跳超时只记录诊断日志（§6.1） |

### 7.2 简化

| 模块 | 变更 |
|------|------|
| ha_state 枚举 | `INIT=0 → BACKUP=1 → RECOVERING=2 → MASTER=3 → FAULT=4 → FENCED=5`（§13.2, §13.3） |
| controller 线程 | 保留并简化为低频 resync/retention maintenance；不执行自动晋升或其他 HA 决策 |
| heartbeat 线程 | 只发送 TCP heartbeat + 进度上报 + 诊断日志，不改变任何状态 |
| 线程数 | 保持 6 个：sender、control_listener、data_listener、apply、heartbeat、resync_controller |

### 7.3 新增

| 模块 | 内容 |
|------|------|
| `tlc_ha_replica_external_promote()` | 外部晋升入口（§4.2） |
| `tlc_ha_replica_external_demote()` | 外部降级入口（§4.3） |
| `tlc_ha_replica_external_fence()` | 紧急 fencing（§4.4） |
| Redis 命令注册 | `HA.PROMOTE`、`HA.DEMOTE`、`HA.FENCE`、`HA.STATE`、`HA.PROGRESS` |

### 7.4 保留不变

| 模块 | 原因 |
|------|------|
| `transition_role()` + `transition_mutex` | 角色转换原子性，promote/demote 都需要 |
| `write_fenced` / Core `ha_term` | 防止双写 |
| owner metadata v2 + fsync | term 持久化 |
| `LEADER_ANNOUNCE` / `ROLE_ACK` | 对端降级通知 |
| 三分支恢复判断 | `durable < takeover_seq` → GAP，`>` → CONFLICT，`==` → 检查 applied |
| Recovery（replay/snapshot） | 旧 Master 追平 |
| UB ring reset / `recovery_generation` | 数据面隔离 |
| `promotion_takeover_seq` | 固定晋升边界 |
| TCP 控制面 listener | 接收 announce、ROLE_ACK、resync 控制帧 |

### 7.5 仍需落地的遗留项

以下是现有设计中尚未完成的功能正确性问题，不受架构变更影响，仍需
按原计划落地：

| 项目 | 说明 |
|------|------|
| transition 通路清理 | `transition_role()` 未清理 `replay_from_seq`/`replay_mode`/`resync_session_state`/`normal_min_seq`/sender queue |
| `snapshot_assembler_inflight` drain | transition 期间 snapshot chunk handler 并发安全 |
| `RECOVERY_REQUEST`/`RECOVERY_DONE` 协议 | 显式 recovery session（复用 RESYNC_REQUIRED + generation） |
| `data_generation` 隔离 | 同 term 旧 UB 帧过滤 |

## 8. 部署拓扑

### 8.1 单 Node group 双机（111/112）

```text
安装清单：
  111: redis-server + keepalived    ← 2 个进程
  112: redis-server + keepalived    ← 2 个进程
  额外机器: 0
  LVS ipvs 内核模块: 不需要
  ipvsadm: 不需要

111 (初始 Master):
    redis-server  (port 6379)
    keepalived    (priority 100, VRRP ID 60)
    eth0:  10.0.0.1 + 10.0.0.100 (VIP)

112 (初始 Follower):
    redis-server  (port 6379)
    keepalived    (priority 90, VRRP ID 60)
    eth0:  10.0.0.2
```

### 8.1.1 真实机 UB Export/Import 映射

111/112 上的 `/dev/obmm_shmdev*` 编号是**本地视角**：本地 Export
设备在对端必须使用对应的 Import 设备，不能把同一个编号直接写到两端。
两组设备的完整双向关系如下：

| 本地 Export 设备 | 对端 Import 设备 |
|---|---|
| 111 `/dev/obmm_shmdev1` | 112 `/dev/obmm_shmdev5` |
| 111 `/dev/obmm_shmdev2` | 112 `/dev/obmm_shmdev6` |
| 111 `/dev/obmm_shmdev3` | 112 `/dev/obmm_shmdev7` |
| 111 `/dev/obmm_shmdev4` | 112 `/dev/obmm_shmdev8` |
| 112 `/dev/obmm_shmdev1` | 111 `/dev/obmm_shmdev5` |
| 112 `/dev/obmm_shmdev2` | 111 `/dev/obmm_shmdev6` |
| 112 `/dev/obmm_shmdev3` | 111 `/dev/obmm_shmdev7` |
| 112 `/dev/obmm_shmdev4` | 111 `/dev/obmm_shmdev8` |
| 111 `/dev/obmm_shmdev9` | 112 `/dev/obmm_shmdev13` |
| 111 `/dev/obmm_shmdev10` | 112 `/dev/obmm_shmdev14` |
| 111 `/dev/obmm_shmdev11` | 112 `/dev/obmm_shmdev15` |
| 111 `/dev/obmm_shmdev12` | 112 `/dev/obmm_shmdev16` |
| 112 `/dev/obmm_shmdev9` | 111 `/dev/obmm_shmdev13` |
| 112 `/dev/obmm_shmdev10` | 111 `/dev/obmm_shmdev14` |
| 112 `/dev/obmm_shmdev11` | 111 `/dev/obmm_shmdev15` |
| 112 `/dev/obmm_shmdev12` | 111 `/dev/obmm_shmdev16` |

当前 HA Replica 回归只使用以下两条数据方向，且同一条方向的两端必须配置相同
的 mmap offset：

```text
111 TX=/dev/obmm_shmdev4  -> 112 RX=/dev/obmm_shmdev8
112 TX=/dev/obmm_shmdev9  -> 111 RX=/dev/obmm_shmdev13
```

其余设备保留给其他 UB lane/扩容测试。启动前必须在两台机器上确认设备存在、
读写权限正确且没有被其他进程占用；peer-view manifest 中的 provider/client
path 也必须按上表生成。

同机 CLI 必须避让 Replica 保留设备：111 上不要使用 `dev4/dev13`，112 上不要
使用 `dev8/dev9`。VIP 漂移后 owner 可能从远端变成本机，客户端必须按新 owner
重新选择 path：CLI@111 对 owner111 使用本地 `dev1/dev2/dev3`，对 owner112
使用远端 `dev14/dev11/dev16`；CLI@112 对 owner112 使用本地 `dev1/dev2/dev3`，
对 owner111 使用远端 `dev7/dev2/dev5`。两个 peer-view manifest 都包含这两套
owner view，并按此避让规则配置。两套 view 是按 active owner 选择的替代项，
不是同一 CLI 的并行映射；切主必须先销毁旧 owner channel/mmap，再用新
`owner_id` 执行 AERON_ATTACH。这样即使不同 owner 的 client path 编号相同，
也不会与旧 ring 同时占用。

### 8.2 多 Node group

每个 Node group 使用独立的 VRRP ID 和 VIP：

```text
Node group 0:  VIP 10.0.0.100, VRRP ID 60
Node group 1:  VIP 10.0.0.101, VRRP ID 61
Node group 2:  VIP 10.0.0.102, VRRP ID 62
```

keepalived 配置多个 `vrrp_instance`，每个 Node group 一个。不同 Node group
的 VIP 可以分布在不同节点上（一台机器同时是某些 group 的 Master 和另一些
group 的 Follower）。各 Node group 的切换独立，不相互阻塞。

### 8.3 CLI 接入

```text
客户端只需配置 VIP 地址：
    redis-cli -h 10.0.0.100 -p 6379

切换后 VIP 自动指向新 Master，客户端无需更改配置。

运维命令也通过 VIP：
    redis-cli -h 10.0.0.100 -p 6379 HA.STATE
    redis-cli -h 10.0.0.100 -p 6379 HA.PROGRESS

直连特定节点用于调试：
    redis-cli -h 10.0.0.1 -p 6379 HA.STATE    # 看 Node A
    redis-cli -h 10.0.0.2 -p 6379 HA.STATE    # 看 Node B
```

## 9. 安全约束

### 9.1 不变量

```text
1. HA.PROMOTE 必须在 applied_seq == durable_seq 之后才能完成
2. promote 完成前 Core write_fenced 不解除
3. ha_term 递增且持久化在角色发布之前
4. 旧 term 的 EVENT/ACK/heartbeat 不回退状态
5. VIP 漂移不改变 Replica 数据面的连续性约束
6. keepalived notify 脚本失败不影响 VRRP 状态（VIP 已漂移）
7. promote 失败时保持 write_fenced，不开放写入；其中 ERR_APPLY_LAG 保持
   FOLLOWER/BACKUP 并继续复制，只有 COLD progress 读取失败、term 溢出或
   transition 内部错误进入 FAULT/FENCED
```

### 9.2 非目标（明确不保证）

```text
1. 双向网络分区下的严格单主（需要 witness/lease）
2. 零数据丢失（异步复制的尾部数据可能丢失）
3. keepalived 本身的高可用（keepalived 崩溃 = 故障检测失效）
4. 基于心跳超时的 self-fence（见 §6.1 详细分析）
```

## 10. 测试方案

### 10.0 第一阶段：COLD 启动边界

keepalived/VIP failover 回归必须以 COLD 矩阵为前置门禁。先用独立目录验证：

1. fresh 0 字节 AOF、无 checkpoint 可以正常启动；
2. 有 AOF、无 checkpoint 时可以依靠 AOF 增量恢复；
3. 有效 `checkpoint.manifest` 和 checkpoint 文件可通过 checksum、generation、
   term、seq 校验；
4. manifest 缺失/损坏、checkpoint 缺失或 checksum 不匹配时必须拒绝无效 checkpoint，
   不能静默继续。

目录快照必须在启动前后保存，特别确认 0 字节 AOF 是本轮 fresh-start 产生的，
而不是上次测试残留。COLD 阶段未通过时，不进入 VIP 漂移和 UB 客户端重建测试。

### 10.1 本地单元测试

```text
test_external_promote:
    创建 Follower replica → HA.PROMOTE → 验证 role=LEADER, state=MASTER,
    term 递增, write_fenced=false

test_external_demote:
    创建 Master replica → HA.DEMOTE → 验证 role=FOLLOWER, fenced=true

test_external_fence:
    创建 Master replica → HA.FENCE → 验证 write_fenced=true, state=FENCED,
    role 不变

test_promote_with_apply_lag:
    Follower applied < durable → HA.PROMOTE → 等待追平 → 成功
    超时 → 返回 ERR_APPLY_LAG，保持 FOLLOWER/BACKUP，继续 apply；随后 retry 成功

test_promote_already_leader:
    Master → HA.PROMOTE → 返回 ERR_ALREADY_LEADER

test_promote_in_fault:
    FAULT 状态 → HA.PROMOTE → 返回 ERR_INVALID_STATE

test_heartbeat_timeout_no_fence:
    Master → TCP 心跳超时 → 验证 write_fenced 仍为 false
    → 验证 ha_state 不变（只有诊断日志，无状态转换）
```

### 10.2 双进程集成测试

```text
test_keepalived_promote_demote:
    进程 A (Master) + 进程 B (Follower)
    → 模拟 keepalived: 向 B 发送 HA.PROMOTE
    → 验证 B 成为 MASTER, A 收到 LEADER_ANNOUNCE 后降级
    → 验证 A 通过 replay/snapshot 追平

test_promote_with_peer_down:
    进程 A (Master) + 进程 B (Follower)
    → 停止 A
    → 向 B 发送 HA.PROMOTE
    → 验证 B 成为 MASTER, LEADER_ANNOUNCE deferred
    → 重启 A，向 A 发送 HA.DEMOTE
    → 验证 A 降级并恢复

test_concurrent_promote:
    同时向 A 和 B 发送 HA.PROMOTE
    → 只有一个成功（term 较高的胜出）
    → 另一个检测到更高 term 后降级
```

### 10.3 真实机 keepalived 集成

```text
test_redis_crash_failover:
    111 Master + 112 Follower + keepalived
    → kill -9 111 redis-server
    → 验证 VIP 漂移到 112
    → 验证 112 HA.STATE = LEADER/MASTER
    → 重启 111 redis-server
    → 验证 111 HA.STATE = FOLLOWER/BACKUP
    → 验证 111 数据追平

test_keepalived_stop_restart:
    111 持有 VIP，112 为 Backup
    → 使用部署脚本 stop 111 keepalived（TERM + 显式删除 VIP + PID 清理）
    → 验证 111 VIP=0，112 接管 VIP 且成为 LEADER/MASTER
    → 使用部署脚本 restart 111 keepalived
    → 验证 111 仍为 BACKUP，不抢回 VIP（nopreempt）
    → 每个阶段都验证两台机器合计恰好一个 VIP

test_machine_reboot_failover:
    → reboot 111
    → 验证 VIP 漂移、112 晋升、111 恢复后降级追平

test_nopreempt:
    → 111 崩溃 → 112 晋升 → 111 恢复
    → 验证 VIP 不回到 111（nopreempt）
```

## 11. 与现有设计文档的关系

```text
TLC_HA_FAILOVER_HEARTBEAT_DESIGN.md
    M8  故障检测              → 被 keepalived 替代，移除
    M9  动态角色与 term       → 保留
    M10 Leader 通知与重连     → 保留
    M11 切主与恢复闭环        → 触发方式改为外部，内部逻辑保留
    M11-1 自动恢复/跨进程收敛 → 保留，不受影响

TLC_HA_DATA_SYNC_IMPLEMENTATION_PLAN.md
    M0-M6                    → 已完成，不受影响
    M7 owner/term/fencing    → promote 入口改为外部，内部保留
    M8 migration             → 不受影响
```

## 12. 落地步骤

```text
Step 1: 新增 HA.PROMOTE / HA.DEMOTE / HA.FENCE / HA.STATE / HA.PROGRESS
        Redis 命令注册和 tlc_ha_replica_external_*() 实现
        → 可独立测试，不影响现有内部逻辑

Step 2: 编写 keepalived 配置模板和 notify/healthcheck 脚本
        → 可在测试环境验证 VIP 漂移，不需要 HPC-Redis 参与

Step 3: 111/112 真实机部署 keepalived + 配置
        → 先手工验证 VIP 漂移 + notify 脚本调用

Step 4: 联调：keepalived notify → HA.PROMOTE/DEMOTE → 验证切主流程
        → kill redis-server → 观察自动切换

Step 5: 已完成：移除 M8 故障检测状态机、双通道 heartbeat、自动晋升
        → ha_state 已简化，heartbeat timeout 仅记录诊断日志

Step 6: 落地 transition 通路清理等遗留项（与 LVS 无关）

Step 7: 完整回归：
        make -C benchmark tlc_ha_replica_ut
        make -C benchmark tlc_ha_replica_process_ut
        111/112 keepalived 集成测试
```

Step 1-4 的外部 HA 命令、keepalived 部署和真实机联调已完成。当前 keepalived
触发 `HA.PROMOTE/DEMOTE/FENCE` 走外部入口，controller 线程只负责低频
resync/retention maintenance，不再作为自动晋升 fallback。

## 13. 设计决策记录

### 13.1 内部 heartbeat failure 状态删除，backoff 保留

**结论**：删除内部 `heartbeat_failure_pending` 状态、
`heartbeat_failure_threshold` 和 `heartbeat_suspect_hold_down_ms` 配置字段。

`tlc_ha_replica_heartbeat_failure_pending()` 仅作为旧调用方的兼容查询保留，固定返回
false；它不再代表运行时状态，也不参与任何角色转换。

故障检测已委托 keepalived，内部心跳超时不再触发自动晋升、状态转换或 fencing。
`resync_controller_main` 只处理 resync/retention maintenance。

保留 heartbeat 线程中的 **backoff 探测间隔**，用途改为控制诊断日志频率——
避免 TCP 心跳持续超时时每秒刷屏。

### 13.2 `STATE_FENCED` 保留

**结论**：`STATE_FENCED` 保留在 `ha_state` 枚举中。

触发方式：keepalived 检测到 FAULT 状态 → 执行 `notify_fault` →
`ha_notify.sh FAULT` → `redis-cli HA FENCE` → `external_fence()` →
`atomic_store(ha_state, FENCED)`。

与其他状态的区别：

```text
STATE_FENCED   紧急停写，由 keepalived FAULT 外部触发
               不执行 drain 或角色转换，仅 atomic_store(write_fenced, true)
               需要人工介入恢复（重启 keepalived/redis-server）

STATE_FAULT    内部错误（如 COLD 写入失败、线程异常）
               由代码内部 transition_role() 设置

write_fenced   临时 fencing，transition_role() 期间短暂置位
(transition)   角色切换完成后自动解除
```

如果删除 `STATE_FENCED`，keepalived FAULT 只能复用 `STATE_FAULT`，但
`STATE_FAULT` 的语义是"内部错误"，运维无法区分是代码 bug 还是 keepalived
主动 fence。保留 `STATE_FENCED` 提供明确的运维诊断信号。

### 13.3 `SUSPECT` / `CANDIDATE` 删除，枚举重新编号

**结论**：从 `ha_state` 枚举中删除 `STATE_SUSPECT` 和 `STATE_CANDIDATE`，
直接重新编号。

当前版本未发布，不存在旧版本兼容性问题。新编号：

```c
enum ha_state {
    STATE_INIT       = 0,
    STATE_BACKUP     = 1,
    STATE_RECOVERING = 2,
    STATE_MASTER     = 3,
    STATE_FAULT      = 4,
    STATE_FENCED     = 5,
};
```

`SUSPECT`（心跳超时疑似故障）和 `CANDIDATE`（自动晋升候选）都是内部
故障检测状态机的产物。故障检测委托 keepalived 后，这两个状态没有触发入口。

### 13.4 Term 计算：`max(local_term, last_seen_peer_term) + 1`

**结论**：promote 时 `new_term = max(local_term, last_seen_peer_term) + 1`，
在当前故障模型内充分。

故障模型假设（§1.1）：两台机器不会同时宕机。在此假设下，每次切主时故障
节点恢复后总能通过 TCP 控制面收到对端的 `LEADER_ANNOUNCE`（携带最新 term），
或通过 owner metadata v2 读取本地持久化的 term。`max()` 确保新 term 严格
大于双方已知的最大 term。

不需要全局 term 分配器或 witness 节点——双机场景下 local + peer 就是全局。

### 13.5 命令接入方式：Redis RESP 命令

**结论**：采用方案 A，注册为 Redis RESP 命令。

`HA` 作为父命令，`PROMOTE`/`DEMOTE`/`FENCE`/`STATE`/`PROGRESS` 作为子命令，
参照 `CLUSTER` 命令模式。keepalived 脚本直接使用 `redis-cli -p 6379 HA PROMOTE`。

不需要额外 `ha_ctl` 工具。原因：

```text
1. 当前 redis-server 进程已同时处理 VEMB 和 RESP 两套协议
   port 6379 → sniff_and_handoff → 非 VEMB 流量 → acceptCommonFinalize
   → 标准 Redis 命令处理，redis-cli 天然可用

2. C SDK 走 VEMB 二进制协议（VEMB_V16_MAGIC + NET_HELLO → proxy 注入），
   与 RESP 是不同协议栈，复用为管理工具需要新增帧类型 + sniff 分支，更复杂

3. 方案 A 只需：commands.def + commands/*.json 注册 + haCommand() 处理函数
   方案 B 需要：ha_ctl.c + NET_HA_MGMT 帧类型 + sniff 新分支 + Makefile 目标
```

实现要点：
- `src/commands/ha-*.json` + `src/commands.def`：注册 HA 父命令 + 5 个子命令
- `src/vemb_v16_server_integration.c`：`haCommand(client *c)` 统一入口，按 `argv[1]` 分发
- `src/server.h`：`void haCommand(client *c);` 声明

### 13.6 UB/Aeron 模式与 keepalived 的关系

keepalived 管理的是**谁是 Master、谁是 Backup**，与客户端数据面传输方式
（TCP 还是 UB/Aeron）正交。

```text
                    TCP 模式                    UB/Aeron 模式
                    --------                    -------------
HA 控制面           keepalived VRRP             keepalived VRRP（完全一样）
  故障检测          keepalived chk_hpc_redis    keepalived chk_hpc_redis
  主备切换触发      notify → redis-cli HA       notify → redis-cli HA
  命令传输          TCP RESP (port 6379)        TCP RESP (port 6379)

客户端数据面        TCP socket (VIP:6379)       UB ring (shm)
  路由/发现         VIP 漂移                    AERON_ATTACH 握手(TCP, 经VIP)
  failover 感知     VIP 自动切换                write_fenced 拒绝写入

节点间复制          UB TX/RX ring + TCP 控制面   同（不受客户端模式影响）
```

#### UB 客户端 failover 流程

UB 数据通道绑定的是共享内存路径，不是 TCP 连接。主备切换后客户端需要
重新与新 Master 建立 UB 通道：

```text
故障前:
  Client ──AERON_ATTACH──→ Node A (Master) TCP:6379   (握手，一次性)
  Client ←─────UB ring────→ Node A                     (数据面，shm)

Node A 故障 → keepalived VIP 漂移到 Node B → HA PROMOTE:
  Client ←── UB ring ──→ Node A (Follower, write_fenced=true)
                          写入被拒绝，客户端感知到错误

客户端重建:
  Client ──AERON_ATTACH──→ VIP:6379 → Node B (新 Master)  (重新握手)
  Client ←─────UB ring────→ Node B                         (新 UB 通道)
```

AERON_ATTACH 握手始终走 TCP 且经过 VIP，所以 VIP 漂移对 UB 客户端同样有效：
客户端重连 VIP:6379 发起 AERON_ATTACH 时自动连到新 Master，获取新的 UB ring
shm 路径，建立新数据通道。

客户端侧 failover 逻辑：

```text
1. UB ring 写入收到 write_fenced 错误（或连接断开）
2. 断开旧 UB ring
3. 通过 VIP:6379 TCP 重新发起 AERON_ATTACH
4. 拿到新 Master 的 UB ring 参数
5. 建立新 UB 数据通道，恢复读写
```

SDK 提供 `vemb_v16_client_reconnect()` 作为显式 failover 边界。它会关闭
所有旧 UB 映射、清理旧 channel，并刷新拓扑；下一次操作重新执行
`AERON_ATTACH`。应用应在 ring publish/poll 失败或收到 fenced 错误后调用它，
不要自动重放无法确认结果的写请求。
