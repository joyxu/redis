# TLC HA Keepalived/Failover 回归方案

## 1. 目的和范围

本文将 [TLC_HA_LVS_KEEPALIVED_DESIGN.md](./TLC_HA_LVS_KEEPALIVED_DESIGN.md)
中的测试结论落到 111/112 真机回归流程，说明现有两个 runner 的覆盖范围、需要
调整的断言，以及 keepalived、VIP 和 UB 客户端 failover 的验收方法。

本文只覆盖一个 `hpc_node_id` 对应的双副本 Node group。LVS/ipvs 不属于本方案的
数据路径，也不需要上传或配置；LVS 已存在于两台机器的
`/root/szz/codespace/LVS`，但本回归不使用它。

## 2. 设计语义

最新设计的职责划分如下：

```text
keepalived        故障检测、VRRP、VIP 漂移
notify 脚本       通过 Redis RESP 触发 HA PROMOTE/DEMOTE/FENCE
HPC-Redis         fencing、term、drain、recovery、LEADER_ANNOUNCE
TCP 控制面        111/112 节点直连，不经过 VIP
客户端入口       通过 VIP；UB 客户端的 AERON_ATTACH 也经 VIP
```

HPC-Redis 内部 heartbeat 只用于 liveness、进度和诊断日志：

- heartbeat timeout 不得自动 promote；
- heartbeat timeout 不得自动 fence；
- heartbeat timeout 不得改变 `ha_state`；
- Follower 报告健康不代表它拥有写权。

### 2.1 远端 UB Export/Import 映射

HA Replica ring 的设备必须按 Export -> 对端 Import 配对，且两端使用相同 offset。
当前 111/112 的设备关系为：

| 本地 Export | 对端 Import |
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

当前 HA Replica 方向是：111 TX=`shmdev4` -> 112 RX=`shmdev8`，112 TX=`shmdev9`
-> 111 RX=`shmdev13`。设备、权限、offset 或 peer-view manifest 任一不匹配，均应
先按此表排查。

### 2.2 同机 CLI 的 UB 设备避让

CLI 与 Redis/Replica 同机运行时，CLI 使用的 `client_path` 不能复用该主机上
Replica 的设备，即使 mmap offset 不同也视为冲突。当前保留设备和推荐的 CLI
设备如下：

| CLI 所在主机 | HA Replica 保留设备 | CLI@111 推荐设备 | CLI@112 推荐设备 |
|---|---|---|---|
| 111 | `dev4` (TX), `dev13` (RX) | owner111 本地 `dev1/dev2/dev3`；owner112 远端 `dev14/dev11/dev16` | 不适用 |
| 112 | `dev9` (TX), `dev8` (RX) | 不适用 | owner111 远端 `dev7/dev2/dev5`；owner112 本地 `dev1/dev2/dev3` |

这里的 `dev1/dev2/dev3` 依次表示 request ring、response ring、warm region；
远端 owner 的三项按对应 peer-view manifest 的 request/response/warm 顺序解释。
每个 CLI manifest 都应同时包含本地 owner 和远端 owner 两套 view：failover
前后 `owner_id` 可能变化，reconnect 时按新 owner 重新解析 path，不能继续使用
旧 owner 的 Import ring。
这些 view 是同一个 CLI 在不同 active owner 下的替代配置，不是并行映射配置；
同一时刻只允许保留当前 owner 的 channel/mmap。尤其 CLI@112 的 owner111 远端
response 使用 `dev2`，与 owner112 本地 response 编号相同，必须在切主时先关闭
旧 channel，才能重新映射该 path。
因此：

- CLI@111 使用 `examples/vemb_v16_ub_peer_view_111_to_112.yaml` 时，owner112
  的远端路径为 `14/11/16`，owner111 同机路径为 `1/2/3`，均不占用 `4/13`；
- CLI@112 使用 `examples/vemb_v16_ub_peer_view_112_to_111.yaml` 时，owner111
  的远端路径为 `7/2/5`，owner112 同机路径为 `1/2/3`，均不占用 `8/9`。

启动 CLI 前仍需在其所在主机执行 `lsof` 或等价检查，确认推荐设备没有被其他
CLI/benchmark 占用；多个 CLI 实例必须再分配不同的 mmap offset，并在测试结束后
销毁 owner channel。Replica 的 Export/Import 设备不能作为临时 CLI path 候选。

当前 Redis 命令采用父命令加子命令的 RESP 形式：

```bash
redis-cli -h <host> -p <port> HA STATE
redis-cli -h <host> -p <port> HA PROGRESS
redis-cli -h <host> -p <port> HA PROMOTE
redis-cli -h <host> -p <port> HA DEMOTE
redis-cli -h <host> -p <port> HA FENCE
```

设计文档中的 `HA.PROMOTE`、`HA.STATE` 等写法是概念名称，不是当前实际的命令
解析形式。

## 3. 两个现有 runner 的覆盖范围

### 3.1 UB runner

脚本：[benchmark/tlc_ha_replica_ub_111_to_112.sh](../benchmark/tlc_ha_replica_ub_111_to_112.sh)

当前覆盖：

```text
UB CC/NC visibility 双向检查
普通 EVENTS、append ACK、async apply、heartbeat 进度
snapshot chunks、checkpoint install
M5 tail/handoff/ACK、M5 timeout/abort cleanup
M6 AOF GAP repair、retention 不足时 snapshot fallback
M7 soft compact、hard pressure abort
Follower COLD recovery、重启后的手工 AOF replay
```

这些测试验证 UB ring、frame、ACK、COLD、snapshot 和 resync 数据面，应继续在真实
UB 设备上回归。它们不经过 `redis-server`，因此不能证明 Redis TCP 请求入口、VIP
或 keepalived 已经正确工作。

### 3.2 Redis TCP/SDK runner

脚本：[benchmark/tlc_ha_redis_tcp_111_to_112.sh](../benchmark/tlc_ha_redis_tcp_111_to_112.sh)

当前覆盖：

```text
真实 redis-server 启动
TCP SDK -> 111 Leader 写入
Leader COLD -> UB Replica -> 112 Follower COLD/WARM
112 TCP 读取校验 VADD/VREM/update/tombstone
检查 appended/durable/applied/peer progress
使用相同 COLD 目录重启 Follower 并验证恢复
```

当前不覆盖：

```text
HA STATE/PROGRESS 的控制面验收
HA PROMOTE/DEMOTE/FENCE
keepalived notify、VRRP、VIP 漂移
Redis crash、机器 reboot、keepalived 进程故障
旧 Leader fencing、降级和恢复后的 lineage transition
客户端经 VIP 重连
UB 客户端重新 AERON_ATTACH
```

### 3.3 COLD 启动边界（同步构建后的第一测试项）

在任何 keepalived、VIP 或客户端 failover 测试前，先完成 COLD 启动矩阵；
COLD 结果不合格时不得进入后续切主测试。每个 case 使用独立的新目录，启动前
记录目录清单、inode、size、mtime，启动后保存 Redis 日志和 COLD 进度。

| Case | 启动目录 | 预期行为 | 判定 |
|---|---|---|---|
| COLD-1 fresh empty | 只有新建的 0 字节 AOF，无 manifest | 正常启动，`generation=0` 只表示没有 checkpoint，依靠后续 AOF 增量 | PASS/FAIL |
| COLD-2 AOF only | 有有效 AOF，无 checkpoint manifest | 正常启动并 replay AOF，`durable_seq/applied_seq` 可追平 | PASS/FAIL |
| COLD-3 valid checkpoint | manifest、checkpoint 文件和 checksum 均有效 | 校验通过，从 checkpoint 继续 replay 增量 AOF | PASS/FAIL |
| COLD-4 invalid checkpoint | manifest 存在但文件缺失、checksum/generation/term/seq 任一不匹配 | 拒绝无效 checkpoint并记录明确错误；不得静默当作有效恢复 | PASS/FAIL |

另加一项 AOF 全空确认：空 AOF 必须由本轮 fresh-start 的目录和时间戳证明，不能
把上次测试残留的 `aof-000...log` 当作 fresh 状态。若目录只有空 AOF 且无
checkpoint，允许启动；若启动后产生事件，必须确认 AOF size/mtime 发生变化并能
正常 replay。

本阶段的输出至少包括：四个 case 的目录快照、启动/退出码、COLD validation 日志、
`generation/checkpoint_seq/durable_seq/applied_seq`，以及“无 checkpoint 时 AOF
增量可恢复”的实际证据。

### 3.4 keepalived 1.2.2 手册结论与自动化部署

本回归使用的本地手册是
`/Users/szza/codespace/work/LVS/docs/keepalived_user_manual.md`，内容与远端
`LVS/tools/keepalived` 的 1.2.2 源码一致。对本方案有影响的约束如下：

```text
启动/停止/重载：service keepalived start|stop|restart|reload，或给主进程发送
                 SIGTERM/SIGHUP
命令行配置文件：-f <file>（默认 /etc/keepalived/keepalived.conf）
VRRP 专用模式： -P；配置诊断输出：-d；前台运行：-n；控制台日志：-l
```

1.2.2 的配置语法支持 `vrrp_script`、`track_script`、`notify_master`、
`notify_backup`、`notify_fault` 和 `nopreempt`。源码中没有
`unicast_peer`/`unicast_src_ip` 解析关键字，因此本版本必须使用同一二层广播域
上的 VRRP 组播；跨 VLAN 不能直接套用现代 keepalived 的单播配置。

构建时远端默认只有 popt 运行库，必须显式安装 `popt-devel`。部署脚本使用
`--disable-lvs` 构建 VRRP-only 二进制，不上传或修改远端已有的
`/root/szz/codespace/LVS` 源码之外的 LVS/ipvs 配置。配置、脚本和生命周期操作
统一由以下脚本完成：

```bash
# 显式指定已经做过 ARP duplicate 检查的测试 VIP
scripts/deploy_keepalived_ha.sh --all \
  --vip 192.168.90.202/24 --install-deps deploy

scripts/deploy_keepalived_ha.sh --all \
  --vip 192.168.90.202/24 check
scripts/deploy_keepalived_ha.sh --all \
  --vip 192.168.90.202/24 start
scripts/deploy_keepalived_ha.sh --all \
  --vip 192.168.90.202/24 status
scripts/deploy_keepalived_ha.sh --all \
  --vip 192.168.90.202/24 reload
scripts/deploy_keepalived_ha.sh --all \
  --vip 192.168.90.202/24 stop
```

脚本在 111/112 自动选择 `eth1/eth0` 和优先级 `100/90`，生成
`/etc/keepalived/keepalived.conf`，将 hook 安装到
`/opt/hpc-redis/keepalived/scripts/`，二进制位于
`/opt/hpc-redis/keepalived/sbin/keepalived`，PID 位于
`/run/hpc-redis-keepalived/`。1.2.2 没有现代 `-t` 选项，`check`/`deploy` 使用
`-n -l -d -P` 短时解析启动并检查错误日志；这一步可能短暂执行 VRRP 初始化，
所以部署动作完成后保持 daemon 停止，必须由回归人员显式执行 `start`。

## 4. 必须修改或新增的回归内容

### 4.1 远程同步方式

两个 runner 的 `sync_node()` 已统一调用
`scripts/sync_changed_code_to_peer.sh`；该脚本负责按 hash 只同步差异文件。手工
执行或在 runner 外准备远端环境时，也必须对每台机器先完成同步和构建：

```bash
NODE=43.154.145.18 SSH_PORT=8111 \
REMOTE_ROOT=/root/szz/codespace/hpc-redis \
bash scripts/sync_changed_code_to_peer.sh --all-code --build all --verify-build all

NODE=43.154.145.18 SSH_PORT=8112 \
REMOTE_ROOT=/root/szz/codespace/hpc-redis \
bash scripts/sync_changed_code_to_peer.sh --all-code --build all --verify-build all
```

`tlc_ha_replica_ub_111_to_112.sh` 同步后还会针对 UB runner 执行远端目标构建；
`tlc_ha_redis_tcp_111_to_112.sh` 使用 `--build all --verify-build all`。UB 测试只能
在支持 UB 的远程 111/112 硬件上运行，不得用本地 POSIX shm 测试结果替代 UB 真机
结果。除非已保存同一提交的远端 hash/build-stamp 证据，否则不得设置
`TLC_HA_UB_SKIP_SYNC=1` 或 `TLC_HA_REDIS_TCP_SKIP_SYNC=1`。LVS 不需要同步。

### 4.2 UB runner 的调整

保留所有数据面和 M5/M6/M7 测试，但调整以下语义：

1. 普通复制阶段的 heartbeat 断言只能检查 peer health/progress 和诊断日志。
2. 任何“heartbeat timeout 自动切主”的断言都应删除，改为“状态和 fencing 不变”。
3. `M6 automatic GAP` 继续保留；这里的 automatic 指 AOF/snapshot 数据修复，
   不是自动 failover。
4. 不要把 HA RESP 命令硬塞进 `tlc_ha_replica_ub_node_ut`；该 UT 不启动
   `redis-server`，应由 Redis TCP/keepalived 专项测试覆盖控制面。

### 4.3 Redis TCP runner 的新增阶段

在现有固定角色写入和 COLD recovery 之后，增加独立的 HA 阶段：

1. 轮询两端 `HA STATE` 和 `HA PROGRESS`，确认 Follower `applied_seq == durable_seq`。
2. 对 Follower 发送 `HA PROMOTE`，验证 term 增加、状态为 `LEADER/MASTER`、
   `fenced=false`。
3. 验证旧 Leader 收到 `LEADER_ANNOUNCE` 后成为 `FOLLOWER/BACKUP`，旧写入被
   fencing。
4. 验证 `HA DEMOTE`、`HA FENCE`、重复 promote 和 FAULT/FENCED 状态错误码。
5. 增加 Redis crash、机器 reboot 和 `nopreempt` 场景。
6. 增加旧节点恢复后的 AOF replay/snapshot resync 和进度追平检查。
7. failover 阶段的客户端入口改用 VIP；固定角色基线仍可使用节点直连地址，
   以便隔离数据面和路由问题。
8. 检查 `HA NOTIFY STATUS`；notify 失败时确认状态为 `failed`，修复故障后执行
   `ha_notify.sh retry`，验证目标状态和状态文件均恢复为 `success`。

## 5. `M7_ONLY` 专项

执行：

```bash
TLC_HA_UB_M7_ONLY=1 \
bash benchmark/tlc_ha_replica_ub_111_to_112.sh
```

该模式仍执行远程环境检查、源码同步、构建和 ring reset，然后只运行：

- M7 retention soft compact；
- M7 retention hard pressure abort。

它适合快速回归 M7 retention 边界，不包括普通复制、M5/M6、COLD recovery、
HA 命令、keepalived、VIP 或 failover。因此 `M7_ONLY PASS` 不能作为“双机 HA
切主通过”的结论。

## 6. 切主和 failover 测试流程

### 6.1 控制面联调（不启动 keepalived）

原测试场景 10（并发 promote、重复 promote 和异常状态错误码组合）仍纳入完整
回归，且已在 COLD 矩阵之后完成；早期“先解决场景 11 的 COLD 启动门禁”仅是
历史执行顺序，不删除或豁免场景 10。

使用节点直连地址启动 111 Leader、112 Follower，复制控制面使用直连 TCP 端口。
客户端端口以测试配置为准；当前 TCP runner 默认为 `6399`，HA 控制面默认为
`9737`，UB runner 的控制面默认为 `9736`。

正常状态检查：

```text
111: role=LEADER, ha_state=MASTER, fenced=false
112: role=FOLLOWER, ha_state=BACKUP
112: applied_seq == durable_seq
```

故障切主：

```text
停止或 kill -9 111 redis-server
在 112 执行 HA PROMOTE
```

验收：

- 112 的 promote 只有在 apply drain 完成后成功；
- 112 term 大于旧 term，状态为 `LEADER/MASTER`；
- 112 `fenced=false` 并可以接受新写入；
- 旧 111 不再接受写入；
- 111 重启后不能自动恢复为 Master；
- 111 经 `HA DEMOTE` 或 `LEADER_ANNOUNCE` 变为 `FOLLOWER/BACKUP`，随后
  通过 AOF replay 或 snapshot 追平。

控制命令边界也要验证：

```text
Master -> HA PROMOTE       => ERR already leader
FAULT/FENCED -> HA PROMOTE => ERR invalid HA state
Master -> HA FENCE         => fenced=true，写入被拒绝
```

### 6.2 keepalived 真机 failover

两台机器分别配置：

```text
111 priority=100
112 priority=90
nopreempt
相同 virtual_router_id
相同 VIP
```

同一二层网络使用 VRRP 组播；VIP 只绑定当前 VRRP MASTER。keepalived 1.2.2
源码不支持 `unicast_peer`，跨 VLAN 的单播配置不属于本次部署范围。节点间复制
控制面继续使用 111/112 直连地址，不能使用 VIP。

健康检查必须只检查 Redis `PING` 和 `HA STATE` 非 `FAULT/FENCED`，不能要求
Follower 必须是 Master。notify 脚本的返回值和日志也必须检查，不能只观察 VIP。

正常启动验收：

```text
111 持有 VIP，状态为 MASTER
112 不持有 VIP，状态为 BACKUP
```

Redis 崩溃场景：

```text
kill -9 111 redis-server
等待 keepalived healthcheck/VRRP 判定
```

验收顺序：

1. VIP 从 111 漂移到 112；
2. 112 `notify_master` 调用 `HA PROMOTE`；
3. 112 为 `LEADER/MASTER`；
4. VIP 新连接在短暂切换窗口后成功；
5. 旧连接断开或收到 fenced 错误；
6. 111 恢复后因 `nopreempt` 不抢回 VIP；
7. 111 的 `notify_backup` 调用 `HA DEMOTE`，然后完成 replay/snapshot recovery。

notify 结果验收：

```text
redis-cli HA NOTIFY STATUS
  -> state/operation/result/rc/attempt/timestamp/detail
notify 失败 -> result=failed，VIP 不回滚
修复 Redis 或路由后执行 ha_notify.sh retry
  -> 目标 HA 命令重新执行，result=success
```

相同流程还要覆盖：

- 111 整机 reboot；
- 111 keepalived 进程停止但 Redis 仍存活；
- 112 故障后的反向切换。

本轮不执行双向网络分区测试。该场景仍是设计文档中的安全边界和非目标，除非
后续引入 witness/lease/STONITH，否则不能把它当作严格单主能力验收。

### 6.3 UB 客户端 failover

#### 6.3.1 当前 owner identity 约定

本轮 UB/Aeron HA 切主测试使用紧凑的 topology owner ID，不直接使用物理主机编号：

```text
111 -> owner_id=0
112 -> owner_id=1
```

该命名空间必须在三处保持一致：

- server manifest：`local_ub_node_id`、`home_ub_node_id`、`remote_meta_views.owner_id`
  和 `ub_rpc_peers.owner_id`；
- topology control：`active_owners`、`standby_owners` 和 `endpoint.owner_id`；
- CLI peer-view manifest：按 `client_host + owner_id + resource_role` 配置本机可见
  的 `provider_path -> client_path` 映射。

例如 CLI@111 的 manifest 同时包含 `client_host=111, owner_id=0` 的本机 view 和
`client_host=111, owner_id=1` 的远端 view；CLI@112 对称配置 owner 1/0。CLI 不把
物理主机号 111/112 当作 owner ID，也不要求 server 与 CLI 共用同一个 manifest 文件。
CLI 通过 TCP topology 获取当前 active owner，再用该 owner ID 选择静态 peer-view
映射。

本专项是“一主一备”模型，不是 `vemb_v16_ub_active_2node_111_to_112.sh` 的
cluster 模型。owner 0/1 仅表示固定身份：111 为 owner 0、112 为 owner 1；任意
时刻 topology 的 `active_owners` 必须恰好包含一个 owner，另一个只能出现在
`standby_owners`。初始状态为 `active=0, standby=0,1`，111 故障切换后为
`active=1, standby=0,1`（反向测试则相反）。不得用 `active=0,1` 作为本回归的
切主通过条件，因为那代表两个 owner 同时可写。

专项 runner 为
`benchmark/tlc_ha_aeron_failover_111_to_112.sh`。它与基础
`tlc_ha_replica_ub_111_to_112.sh` 的职责不同：前者验证 Redis Aeron 数据面经
VIP 的 ATTACH、唯一 active owner、kill leader 后 topology 更新、客户端重新
ATTACH 以及旧节点恢复；后者只验证 HA Replica UB 控制/数据帧和 COLD/recovery。

当前 SDK/common core 将 owner ID 作为内部 owner slot 使用，支持范围是
`0..VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS-1`；benchmark 还有更小的节点数组限制。
因此本轮禁止将 owner ID 改成 `100/101`。任意 owner ID 需要后续增加逻辑 owner 到
内部 slot 的映射后再单独回归。

UB 客户端的共享内存 ring 不会因 VIP 漂移自动变更。客户端必须执行：

```text
旧 UB ring 写入失败或连接断开
销毁旧 ring
通过 VIP:客户端端口重新获取 TCP topology
根据新的 active owner（111 为 0，112 为 1）选择对应 peer-view
经 VIP 重新发起 AERON_ATTACH，取得新 Master 的 UB ring 参数
建立新 ring 并恢复读写
```

验收：

- 旧 Master 的 ring 被 fencing 或失效；
- 客户端不会继续使用旧 ring；
- 切主前后 owner ID 与物理节点的对应关系符合 `0@111/1@112`；
- CLI 选择的 `client_path` 不复用本机 HA Replica 保留设备；
- AERON_ATTACH 通过 VIP 到达新 Master；
- 新 ring 上读写恢复；
- 只保证 Follower 已持久化的 durable prefix，不宣称异步尾部零丢失。

### 6.4 测试隔离、数据保存与清理

不同故障场景不应通过删除 COLD/AOF 或重置 UB ring 来“清场”。测试前只清理上一
个场景遗留的运行时状态：使用部署脚本停止 keepalived（脚本会发送 TERM、显式
删除 VIP、清理 PID 文件），然后验证本机 VIP=0。Redis 进程重启场景保留原 COLD
目录、AOF、RDB 和 UB ring 内容；只有专门的 fresh-start/协议 UT 才使用新目录或
显式 reset。

VIP 验证使用地址字段精确匹配，不使用可能误匹配前缀的普通 `grep`。先在目标节点
上取 VIP 的地址部分（例如 `192.168.90.202/24` 的地址是 `192.168.90.202`）：

```bash
VIP_ADDR=192.168.90.202

# 本机必须没有 VIP；命令返回 0 表示仍存在，返回 1 表示已清理。
ip -o -4 addr show |
  awk -v vip="$VIP_ADDR" '{addr=$4; sub("/.*", "", addr); if (addr == vip) found=1}
       END {exit found ? 0 : 1}'

# 两台机器分别执行上面的检查后，汇总结果必须恰好为 1。
ip -o -4 addr show |
  awk -v vip="$VIP_ADDR" '{addr=$4; sub("/.*", "", addr); if (addr == vip) count++}
       END {print "vip_count=" count; exit count == 1 ? 0 : 1}'
```

若停止的是 keepalived 当前 MASTER，必须先确认旧节点 VIP=0，再确认 BACKUP
接管；直接 `kill -TERM` 或 `kill -9` 不能代替清理，因为 keepalived 1.2.2 退出
时可能来不及删除 VIP。推荐的停止/重启流程为：

```bash
scripts/deploy_keepalived_ha.sh --node 111 \
  --vip 192.168.90.202/24 stop
# 验证 111 VIP=0，再启动或等待 112 接管
scripts/deploy_keepalived_ha.sh --node 111 \
  --vip 192.168.90.202/24 restart
```

每个场景的通用步骤是：

1. 记录两端 `HA STATE`、`HA PROGRESS`、notify status、Redis PID、keepalived PID
   和 VIP 归属。
2. 完成运行时清理，确认目标节点 VIP=0；按场景启动 Redis/keepalived，并等待 topology、
   peer health 和 `applied_seq == durable_seq`。
3. 执行单一故障动作：只 kill Redis、只停止 keepalived，或同时停止两者；不要在
   同一轮混入 COLD reset。
4. 等待 VRRP 收敛和 notify 完成，验证 VIP 恰好在一个节点、该节点为
   `LEADER/MASTER`，另一节点未持有写权，并保存日志。
5. 恢复被杀进程时使用原 PID 配置和原数据目录，验证 AOF replay/snapshot resync
   后再进入下一场景。

已完成的真机流程、未完成流程及其当前结论见第 7.3 节；“仅停止 keepalived”和
“UB 经 VIP 重建”和跨节点 HANDLE read 已在后续真机矩阵完成，剩余未覆盖项仅限
独立故障域 reboot。

SDK 客户端在 UB publish/poll 失败后必须销毁旧 ring，并调用
`vemb_v16_client_reconnect()`；该 API 刷新拓扑并让下一次操作重新执行
`AERON_ATTACH`。不自动重放结果未知的写请求。

当 topology 返回的是节点直连地址、而客户端入口必须固定经过 VIP 时，应用应在
创建客户端且尚未打开 owner channel 前调用：

```c
vemb_v16_client_set_ha_endpoint(client, "192.168.90.202", 6379);
```

该 override 只作用于数据面 owner channel 的 TCP handshake/AERON_ATTACH，复制控制面
仍使用节点直连地址。VIP 漂移后调用 `vemb_v16_client_reconnect()` 会关闭旧映射，
刷新 topology，并用 VIP 对当前 active owner 重新执行 ATTACH。此模式要求一个 HA
Node group 在 topology 中只有一个 active owner；多 owner/多分片场景必须为每个 owner
提供可达的独立 endpoint，不能把所有 owner 强行指向同一个 VIP。

`vemb_v16_bench --ha-endpoint HOST:PORT` 是上述 API 的 smoke-test 入口。

## 7. 回归通过条件和非目标

### 7.1 必须通过

```text
固定角色 UB 数据面和 COLD/resync 回归通过
固定角色 Redis TCP/SDK 回归通过
HA STATE/PROGRESS 与 durable/apply 边界正确
promote/demote/fence 角色和写权限正确
VIP 任意时刻只在一个节点上
客户端经 VIP 重连后恢复服务
旧 Leader 恢复为 Backup 并完成追平
heartbeat timeout 不触发自主切主或 self-fence
```

### 7.3 当前真机回归记录（2026-09-09/10）

已完成：111/112 keepalived 配置部署、启动选主、`nopreempt` 行为、notify status
查询、notify 失败注入与 `retry`。已验证 112 先启动时持有 VIP，111 后启动不会因
高 priority 抢占。

早期未通过、现已由后续真机矩阵取代：仅停止 keepalived 的切换回归。测试中 112 的 COLD 目录没有
有效 checkpoint，导致 `notify_master -> HA PROMOTE` 返回 `invalid HA state`；修复
COLD/复制初始状态后才能评价 promote。checkpoint 并非启动必需：没有 checkpoint
时应从连续 AOF 增量恢复；但只要目录中存在 `checkpoint.manifest`，就必须通过完整
校验，不能静默使用无效 checkpoint。另发现 keepalived 1.2.2 手工 TERM 后 111
残留 VIP，112 启动后可观察到双 VIP，后续必须由部署脚本显式执行 VIP 清理并检查
“恰好一个节点持有 VIP”。

未执行：整机 reboot。当前 111/112 SSH 端口映射到同一系统实例，无法证明 reboot
只影响一个 HA 节点；在确认独立故障域前禁止执行。

112 的实际目录只有一个 0 字节的
`aof-00000000000000000000.log`，没有 `checkpoint.manifest` 或
`checkpoint-<generation>` 文件；启动日志中的 `generation=0, EINVAL` 是“尝试
校验 active checkpoint 但不存在有效 manifest”的结果，不是检测到一份完整但
checksum 错误的 checkpoint。0 字节 AOF 在 fresh-start 时是合法的初始日志文件，
也可能是上一次测试创建目录后尚未产生事件留下的残留；当前时间戳和内容不足以单独
证明是哪一种。后续应在启动前记录目录清单和 inode/mtime，测试结束后确认是否有
AOF 事件及 checkpoint manifest，避免把残留目录误当作恢复数据。

此前 smoke 曾记录 `EAGAIN (Resource temporarily unavailable)`；该结果已被后续
重启、重建 benchmark 并打开分阶段诊断后的结果取代，不能再作为当前实现结论。

当前诊断已分别记录 ATTACH 响应、path/offset、peer-view resolve、UB mmap 和 ring
header。111 的实际结果为 ATTACH/ring/VADD 全部成功；112 的失败发生在
peer-view resolve（`req_rc=-1/resp_rc=-1`），服务端返回 `/dev/obmm_shmdev1`、
`/dev/obmm_shmdev2`，而 CLI@112 owner111 manifest 当前声明的 provider path 是
`dev3/dev6`。这属于配置不匹配，不是 EAGAIN、HA fenced 或 ring 满。该段是正式
peer-view 修正前的历史诊断；当前 HA Aeron manifest 已使用 UB warm region，并由
下述 `aeron_handle_20260910_1` 完成 `VEMB_HANDLE` warm read。

服务端保留低频关键路径日志：
`aeron ATTACH warm candidate: region=%u backend=%u path=%s bytes=%llu offset=%llu`。
该日志位于 ATTACH 候选选择路径，不在请求热点路径，可用于区分无候选、SHM 候选和
合法 UB 候选。

### 7.2 明确不保证

```text
双向网络分区下的严格单主
异步复制的零数据丢失
keepalived 进程自身的高可用
未实现的 HA_NOT_OWNER/HA_REDIRECT 自动路由和非幂等请求去重
```

## 8. 推荐执行顺序

远程测试每一轮都必须先同步源码并构建，不能用本地构建结果或远端旧二进制代替。
以下 11 项是完整回归清单；状态依据第 7.3 节及已有真机记录，已完成项不再作为
当前阻塞项，但源码或配置变化后仍应按需抽样重跑。

| 顺序 | 场景 | 当前状态 | 本轮动作与进入条件 |
|---:|---|---|---|
| 1 | 远程同步并构建 | **已完成（2026-09-09，本轮）** | 111、112 均完成 `scripts/sync_changed_code_to_peer.sh --all-code --build all --verify-build all`；两端 server/client build-stamp 均为 `OK`。后续每轮仍必须重复此项。 |
| 2 | COLD 启动矩阵（此前称“场景 11”，COLD-1..4） | **已完成（2026-09-09）** | 111/112 `tlc_cold_ut` 均通过；真实 Redis+UB 1000-event smoke 写入 1004 个事件、112 follower 读取校验通过，并从同一 COLD 目录重启 follower 后再次校验通过。 |
| 3 | M7_ONLY retention 专项 | **已有通过记录** | 仅在本轮 M7/retention 代码或远端二进制变化时重跑；不把 `M7_ONLY PASS` 当作 HA 切主通过。 |
| 4 | 完整 UB 数据面 runner | **已有覆盖记录，需同步后确认** | 在真实 UB 设备上复核 CC/NC、EVENT、snapshot、M5/M6/M7 和 follower recovery；若失败先定位 UB 数据面，不进入 VIP 测试。 |
| 5 | Redis TCP/SDK 固定角色 runner | **已完成（2026-09-09）** | 1000-event smoke 实际写入 1004 个事件，112 Follower 读取校验和 COLD 重启恢复校验均通过；固定角色结果不等价于 failover 结果。 |
| 6 | 无 keepalived 的 HA 控制面（场景 10） | **已完成（2026-09-09）** | 111 重复 promote 返回 `ERR already leader`；停止 111 后 112 promote 成功（term `3 -> 4`）；112 重复 promote 返回 `ERR already leader`；fenced follower promote 返回 `ERR invalid HA state`。 |
| 7 | keepalived + VIP 的 Redis crash | **已完成（2026-09-09）** | kill 111 Redis 后 112 接管唯一 VIP、PROMOTE 成功（term `5 -> 6`）；111 恢复后保持 BACKUP 且 DEMOTE 成功。故障节点的 FENCE notify 因 Redis 已停返回失败，但状态可查询。 |
| 8 | 仅停止 keepalived，以及 Redis + keepalived 组合故障 | **已完成（2026-09-09）** | keepalived-only：停止 112 keepalived 后 111 接管 VIP 并 promote（term `6 -> 7`），未形成双主；组合故障：同时停止 111 Redis/keepalived 后 112 接管并 promote（term `7 -> 8`）。发现被停止节点的 notify.status 可能保留旧 MASTER 结果。 |
| 9 | 112 -> 111 反向切换与 nopreempt | **已完成（2026-09-09）** | kill 112 Redis 后 111 接管唯一 VIP 并 promote（term `8 -> 9`）；112 恢复后为 BACKUP、DEMOTE 成功且不抢占 VIP。 |
| 10 | UB 客户端经 VIP 重建 AERON_ATTACH（CLI@111、CLI@112） | **已完成（2026-09-09/10）** | 正式 owner peer-view 已统一；CLI@111 的 owner `0 -> 1` 和 CLI@112 的 owner `1 -> 0` 均完成重新 ATTACH/VADD，两个方向均 `fail=0`；同一 runner 的初始/切主 `VEMB_HANDLE` warm read 也均 `ok=4 fail=0`、`read_bytes=256`。 |
| 11 | 旧 TCP 连接行为、旧 Leader recovery 与数据追平 | **已完成（2026-09-10）** | 旧连接先收到 `PONG`，kill 旧 Leader 后对旧地址返回 `Connection refused`；新 Leader 接管后，旧节点 AOF repair 完成，`follower durable=applied=16` 且新 Leader `appended=peer_durable=peer_applied=16`。 |

建议的实际推进顺序为：第 1~2 项和第 6~11 项已完成，第 3~5 项是已有
基线的同步后确认。整机 reboot 仍不包含在这 11 项内，
待确认 111/112 是独立故障域后再单独安排。

只有第 2 项的真实 Redis+UB 矩阵以及第 4、5、6、7、8、9、10、11 项均有可复核证据，才能给出
“111/112 keepalived failover 回归通过”的结论；第 3 项已有记录可作为基线，
但若本轮代码涉及 M7/retention 则必须重新执行。

## 9. 2026-09-09 真机部署与最小回归记录

本次部署使用 `192.168.90.202/24` 作为测试 VIP。111/112 的二层网卡分别为
`eth1`/`eth0`，VRRP ID 为 60，优先级为 100/90。两端均从已有的
`/root/szz/codespace/LVS/tools/keepalived` 构建 1.2.2 VRRP-only 二进制，未上传或
修改 LVS/ipvs；构建依赖通过显式的 `--install-deps` 安装 `popt-devel`。

已执行并通过：

```text
deploy -> config check：111/112 均 valid，daemon 保持 stopped
start：111 持有 VIP，112 为 BACKUP
kill -9 111 redis-server：111 进入 FAULT 并释放 VIP，112 接管 VIP
                   notify_master -> HA.PROMOTE rc=0，112 term=2->3
恢复 111 Follower：111 不抢回 VIP，notify_backup -> HA.DEMOTE rc=0
                   111=FOLLOWER/BACKUP，112=LEADER/MASTER
```

初次故障演练还暴露了两个 1.2.2 兼容性要点，已固化到模板和脚本：

1. `vrrp_script weight -20` 只降低优先级；在 `nopreempt` 双机配置下不会让
   Backup 接管。模板改为 `weight 0` 并在 `track_script` 显式指定 `weight 0`，
   失败经过 `fall=3` 后进入 FAULT、停止广播并删除 VIP。
2. 该版本 reload 可能保留旧进程留下的 VIP。修改配置后应执行脚本的
   `stop`（脚本会额外清理 VIP）再 `start`；不要只依赖 `reload` 作为 VIP 清理。

完整回归仍需覆盖整机 reboot（当前明确不执行）。本节后续记录已补齐 keepalived
进程故障、UB 客户端经 VIP 重新 `AERON_ATTACH`、旧 TCP 连接行为以及故障期间数据
追平；本节结果已证明部署、VIP 漂移、notify 控制命令、`nopreempt`
恢复路径以及无 keepalived 控制面边界在 111/112 真机上工作。

执行顺序调整后的首轮 COLD 尝试（历史记录，已由后续 run 取代）：远端 111 Linux 上 `make -C benchmark
tlc_ha_replica_ut` 构建成功，测试完成 COLD 初始化和临时 ring 映射，但随后在
`run_network_test` 的尾事件等待断言失败；因此本次只能记录为“COLD 代码路径可执行、
整套 UT 未通过”；该结论已由后续 COLD 协议级 UT 和 Redis+UB smoke 取代，不能作为当前阻塞项。

本轮同步构建后的 COLD 协议级复核：111、112 分别执行
`make -C benchmark tlc_cold_ut && ./benchmark/tlc_cold_ut`，结果均为
`tlc_cold_ut: PASS`。日志中包含预期的空目录、AOF 序列 gap、checkpoint
`EINVAL`、损坏 AOF 和非末段文件拒绝信息。完整输出已分别保存于远端
`/tmp/tlc_cold_ut_111.log` 和 `/tmp/tlc_cold_ut_112.log`。该结果已封存为
COLD-1..4 的协议级证据；此前因两端 6379 实例占用 HA Replica 设备（111
`dev4/dev13`、112 `dev9/dev8`）而暂缓的进程级 COLD case，已由下述真实
Redis+UB smoke 补齐。

本轮真实 Redis+UB COLD smoke（run id `20260909_163522-99018`）已完成：
`TLC_HA_REDIS_TCP_EVENT_COUNT=1000`、`MAX_VECTORS=4096`，111 Leader 经
`dev4 -> dev8` Replica 映射向 112 Follower 发布，SDK 报告 `PASS events=1004`；
随后使用同一 COLD 目录重启 112 Follower，SDK 报告 `VERIFY PASS events=1004`，
runner 最终报告 `PASS: Follower COLD recovery after reset WARM region`。远端产物目录为
`/root/szz/codespace/hpc-redis/benchmark/results/tlc_ha_redis_tcp/20260909_163522-99018`，
两端启动日志保存在该目录对应的 `redis111.log`/`redis112.log`。runner 自身的 6399
实例已退出；本轮随后以保留的 `/tmp/ha-keepalive-20260909` COLD/RDB 状态恢复 6379
双节点，未删除任何持久化数据。

无 keepalived 控制面场景 10 也已完成：初始 111 为 `LEADER/MASTER`、112 为
`FOLLOWER/BACKUP`，重复 promote 返回 `ERR already leader`；停止 111 后在 112
执行 promote 返回 `OK`，term `3 -> 4` 且状态为 `LEADER/MASTER`；将 112 降为
follower 后 FENCE，再 promote 返回 `ERR invalid HA state`。

随后完成 keepalived 故障矩阵：

* Redis crash（111 -> 112）：111 kill 后释放 VIP，112 `PROMOTE success`、term
  `5 -> 6`；111 重启为 follower，`DEMOTE success`，VIP 未回抢。
* keepalived-only（停止 112 keepalived，Redis 保持存活）：111 接管 VIP 并
  promote 到 term=7，112 通过 Leader announce 降为 follower，未观察到双 VIP/双主。
* Redis+keepalived 组合故障（111）：112 接管 VIP 并 promote 到 term=8；111
  恢复后保持 follower，VIP 仍在 112。
* 反向 Redis crash（112 -> 111）：111 接管 VIP 并 promote 到 term=9；112
  恢复后 `BACKUP/DEMOTE success`，`nopreempt` 生效。

以上每次均通过部署脚本清理旧 VIP，并用 `ip addr` 确认任意时刻只有一个节点
持有 `192.168.90.202`。在 Redis 已停止时，故障节点的 `notify_fault` 无法连接
本机 Redis，`notify.status` 记录 `result=failed`；VRRP 不回滚，另一节点仍可
正常接管。这是预期的可观测行为。另发现停止 keepalived 本身不一定更新
`notify.status`，被停止节点可能保留旧的 `MASTER/PROMOTE` 记录，后续应通过
状态查询时同时读取 HA STATE 和 keepalived 进程状态，不能单独依赖旧 notify 文件。

当前环境已恢复为 111 `LEADER/MASTER`、112 `FOLLOWER/BACKUP`，VIP 唯一位于 111，
两端 keepalived 和 Redis 均运行，peer health 为 `HEALTHY`。

上述环境快照只描述当时的手工矩阵结束状态。专项 runner 默认在退出时停止两端
Redis/keepalived 并显式删除 VIP，因此后续判断环境是否可复用必须重新查询进程、
`HA STATE` 和 VIP，不能沿用该快照。

2026-09-09 ATTACH 复核补充：两端先通过 `scripts/sync_changed_code_to_peer.sh
--all-code --build all --verify-build all` 并重启 6379 Redis。CLI@111 使用
`vemb_v16_bench --ha-endpoint 192.168.90.202:6379 --mode vadd` 成功完成 2 次
VADD；CLI@112 同命令经 VIP 连接到 111，但在 owner111 的 peer-view path 解析处
失败。该失败已由日志明确定位到 manifest，不应标记为服务端 ATTACH 失败。后续应
先统一服务端 AERON request/response/warm 的 provider path 与 CLI@111/112 两份
manifest，再进行旧 ring 销毁、VIP 漂移和新 owner 重建测试。

本轮重启顺序中，112 follower 首次重启后其 9738 控制监听在 111 重启后消失，
111 的 `peer_health` 一度变为 `UNKNOWN`，新增 VADD 未被接收；再次仅重启 112
后监听恢复，双方 `appended_seq=11/durable_seq=11/applied_seq=11` 且
`peer_health=HEALTHY`。因此后续故障切换前必须把控制端口监听和 seq 追平作为门禁，
不能只看 Redis 6379 可连接或旧的 `HA PROGRESS` 快照。

为隔离配置因素，CLI@112 使用临时 manifest（实际 leader dev1/dev2 -> CLI@112
Import dev5/dev6）复测同一命令，结果为 `ok=2 fail=0`。这证明 SDK 的
`--ha-endpoint` VIP override、ATTACH、UB mmap 和 VADD 数据面均可工作；临时文件
未纳入正式配置。正式 failover 仍不能直接通过该临时映射，因为 VIP 漂移到 112
后同一个 owner 的服务端 provider path/本地 path 组合会改变，必须先定义并统一
按 owner/active-host 选择的 peer-view 方案。

### 9.1 双向 Aeron/UB failover 专项结果

2026-09-09/10 已用独立 runner
`benchmark/tlc_ha_aeron_failover_111_to_112.sh` 完成正式映射回归。测试前两端均执行
`scripts/sync_changed_code_to_peer.sh --all-code --build all --verify-build all`，随后
再次执行 `--all-code --verify-build all`，结果均为 `different=0`，server/client
build stamp 均为 `OK`。

真实 keepalived/VIP 方向 run id 为 `aeron_keepalived_20260909_5`：初始
`active=0`，CLI@111 通过 VIP 建立 owner0 Aeron channel，`VADD ok=4 fail=0`；kill
111 Redis 后 VIP 唯一漂移到 112，112 的 notify 状态为 `MASTER/PROMOTE success`，
topology 更新为 `active=1`。同一 CLI 关闭旧 channel 后按 owner1 的
`dev14/dev11/dev16` view 重新经 VIP ATTACH，`VADD ok=4 fail=0`。112 日志保留
`aeron ATTACH warm candidate: ... path=/dev/obmm_shmdev12`，111 恢复后接受
`start=9 durable=16` 的 AOF repair prefix 并回到 `FOLLOWER/BACKUP`。runner 最终输出：

```text
tlc_ha_aeron_failover_111_to_112: PASS active_owner=1 standby_owner=0 run=aeron_keepalived_20260909_5
```

对称 CLI@112 方向 run id 为 `aeron_cli112_20260910_1`。该轮不重复 VRRP 矩阵，
使用直连 HA control 验证 owner/path 重选：初始 `active=1`，CLI@112 通过 owner1
本机 view ATTACH/VADD；kill 112 并 promote 111 后更新为 `active=0`，CLI@112 按
owner0 的 `dev7/dev2/dev5` view 重新 ATTACH/VADD，两个阶段均 `fail=0`；112 最后
以 follower 恢复。runner 最终输出：

```text
tlc_ha_aeron_failover_111_to_112: PASS active_owner=0 standby_owner=1 run=aeron_cli112_20260910_1
```

因此场景 10 的双向 owner 重选、旧 Aeron channel 销毁、新 channel ATTACH/VADD 和
跨节点 `VEMB_HANDLE` warm payload read 均已完成。补充 run id 为
`aeron_handle_20260910_1`：初始 owner0 与切主后 owner1 的 HANDLE 读均为
`ok=4 fail=0 read_bytes=256`；CLI 日志分别确认 UB warm region
`dev1 -> dev1`（本地）和 `dev12 -> dev16`（远端 Import），112 服务端保留
`aeron ATTACH warm candidate` 日志。

场景 11 的正式 run id 为 `aeron_tcp11_20260910_4`。旧 TCP probe 在切主前收到
`PONG`，kill 111 后对旧地址记录连续 `Connection refused`；这表示旧连接已失效，
而不是把请求重定向到新 owner。切主后的 owner1 Aeron/VADD 仍为 `fail=0`。P8
显式 `HA PROGRESS` 门禁确认恢复后的 111 follower `durable_seq=applied_seq=16`，
112 leader `appended_seq=peer_durable_seq=peer_applied_seq=16`，随后 runner 输出：

```text
tlc_ha_aeron_failover_111_to_112: PASS active_owner=1 standby_owner=0 run=aeron_tcp11_20260910_4
```

因此场景 11 的旧 TCP 连接失效、旧 Leader AOF repair 和 seq 追平已完成。剩余边界
是因 111/112 当前 SSH 端口映射到同一系统实例而暂不执行的整机 reboot。
