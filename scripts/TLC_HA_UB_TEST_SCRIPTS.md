# TLC HA/UB 验证脚本使用手册

本文是 TLC HA、HA Replica UB 和 Aeron/UB failover 验证脚本的操作入口。以下命令
均在本地 `hpc-redis` 根目录执行。除明确标记为诊断模式外，测试 runner 会自动调用
`scripts/sync_changed_code_to_peer.sh`，在 111 和 112 同步、构建并验证远端产物。

## 1. 前置条件

执行测试前必须完成可靠性文档中的 preflight：

- SSH 端口确实对应预期的 111/112，且需要 reboot 的场景具有独立故障域；
- `/dev/obmm_shmdev*` 设备存在、可读写且没有被其他进程占用；
- Redis、keepalived、旧 runner 和 VIP 残留已清理；
- `ssh`、`scp`、`timeout`、`redis-cli`、`ss`、`ip` 等工具可用；
- 111/112 的代码 hash、server/client build stamp 一致。

默认共享配置为：

```text
examples/tlc_ha_replica_ub_111_to_112.env
```

当前 HA Replica 映射必须保持：

```text
111 TX=/dev/obmm_shmdev4  RX=/dev/obmm_shmdev13
112 TX=/dev/obmm_shmdev9  RX=/dev/obmm_shmdev8
```

需要临时配置时使用独立 env 文件：

```bash
TLC_HA_UB_CONFIG=/path/to/test.env \
  bash benchmark/tlc_ha_replica_ub_111_to_112.sh
```

## 2. 每轮同步和构建

### 运行前提

- 本地当前目录必须是 hpc-redis git 工作树，且待同步的文件位于仓库内；
- 本机有 `ssh`、`scp`、`sha256sum` 或 `shasum`；
- 可以免密登录目标节点，远端用户对 `REMOTE_ROOT` 具有读写和构建权限；
- `NODE`、`SSH_PORT`、`REMOTE_ROOT` 指向正确的节点和源码目录；
- 若执行构建，远端具备编译器、make 和目标依赖，且没有其他 runner 并发覆盖构建目录。

手工准备远端环境时，对每台机器分别执行：

```bash
NODE=43.154.145.18 SSH_PORT=8111 \
REMOTE_ROOT=/root/szz/codespace/hpc-redis \
bash scripts/sync_changed_code_to_peer.sh \
  --all-code --build all --verify-build all

NODE=43.154.145.18 SSH_PORT=8112 \
REMOTE_ROOT=/root/szz/codespace/hpc-redis \
bash scripts/sync_changed_code_to_peer.sh \
  --all-code --build all --verify-build all
```

脚本必须输出并保存 source hash、server/client build stamp 和关键 binary 校验信息。
`SKIP_SYNC` 只能用于已有同一提交远端产物的诊断，不得作为正式回归默认路径。

## 3. HA Replica UB 基础回归

### 运行前提

- 必须在支持 UB 的真实 111/112 硬件上运行，不能用本地 POSIX shm 结果替代；
- 已按第 1 节完成远端身份、设备权限、设备占用和控制面连通性检查；
- 共享 env 中的 Replica TX/RX path 和 offset 与实际设备一致；
- `tlc_ha_replica_ub_node_ut`、`ub_cc_nc_visibility_ut` 可执行；
- 使用 fresh COLD 目录时目录尚未被其他 case 使用；使用 recovery 时必须保留原目录。

执行完整 UB 数据面、COLD、M5、M6、M7 和 Follower recovery：

```bash
bash benchmark/tlc_ha_replica_ub_111_to_112.sh
```

覆盖内容包括 visibility、普通 EVENTS、append ACK、snapshot/checkpoint、M5 handoff、
M5 timeout/abort、M6 GAP repair、retention -> snapshot、M7 compact/pressure 和
重启后的 AOF replay。该 runner 不启动 `redis-server`，不能单独证明 Redis TCP、VIP、
keepalived 或客户端 failover。

只运行 M7 retention 专项：

```bash
TLC_HA_UB_M7_ONLY=1 \
  bash benchmark/tlc_ha_replica_ub_111_to_112.sh
```

`M7_ONLY PASS` 只表示 M7 retention 用例通过。smoke 或延长 replay 超时示例：

```bash
TLC_HA_EXPECTED_EVENTS=1000 TLC_HA_TEST_TIMEOUT_SECONDS=90 \
  bash benchmark/tlc_ha_replica_ub_111_to_112.sh

TLC_HA_RESTART_REPLAY_TIMEOUT_SECONDS=120 \
  bash benchmark/tlc_ha_replica_ub_111_to_112.sh
```

## 4. Redis TCP/SDK 固定角色回归

### 运行前提

- 已完成本轮源码同步和 server/client 构建验证；
- 111/112 的 Replica UB 设备可读写且未被其他进程占用；
- `src/redis-server`、`benchmark/tlc_ha_replica_ub_node_ut` 和
  `clients/c/build/sdk_ha_replica_tcp` 已构建；
- TCP 端口 `6399` 和控制面端口 `9737` 未被其他服务占用；
- 测试结果目录和两端 COLD 目录可创建；
- 固定角色 recovery 测试需要保留原 COLD/AOF，不能提前删除。

执行 Redis TCP/SDK -> Leader -> Replica UB -> Follower TCP/SDK，以及 Follower COLD
重启恢复：

```bash
bash benchmark/tlc_ha_redis_tcp_111_to_112.sh
```

默认配置为 Redis TCP `6399`、HA control `9737`、`10000` 个事件、`dim=16`。缩小
规模进行 smoke：

```bash
TLC_HA_REDIS_TCP_EVENT_COUNT=1000 \
TLC_HA_REDIS_TCP_RUN_ID=tcp_smoke_1000 \
  bash benchmark/tlc_ha_redis_tcp_111_to_112.sh
```

该 runner 验证固定 Leader/Follower 数据链路和 COLD recovery，不验证 keepalived、
VIP 漂移或 Aeron 客户端重新 ATTACH。

## 5. keepalived 部署和生命周期

### 运行前提

- VIP 已完成 ARP/地址冲突检查，且没有其他服务使用；
- 111/112 位于同一二层广播域，当前 keepalived 1.2.2 使用 VRRP 组播；
- SSH 端口分别能访问目标节点，且远端具有 root 权限；
- 远端存在 `/root/szz/codespace/LVS/tools/keepalived`；
- 远端具备 gcc、make，首次构建时可安装 `popt-devel`；
- `HA` Redis 命令、`redis-cli`、`timeout` 和 `/var/log`、`/run` 相关目录可用；
- 部署或切换前两端没有残留 VIP，或允许脚本显式清理残留 VIP。

LVS 不需要同步或上传；keepalived 从远端已有的
`/root/szz/codespace/LVS/tools/keepalived` 构建。先确认测试 VIP 未被其他服务使用：

```bash
scripts/deploy_keepalived_ha.sh --all \
  --vip 192.168.90.202/24 --install-deps deploy
scripts/deploy_keepalived_ha.sh --all \
  --vip 192.168.90.202/24 check
scripts/deploy_keepalived_ha.sh --all \
  --vip 192.168.90.202/24 start
scripts/deploy_keepalived_ha.sh --all \
  --vip 192.168.90.202/24 status
```

`deploy` 完成构建、安装配置和 hook 后保持 daemon 停止；必须显式 `start`。停止
或切换场景前清理两端：

```bash
scripts/deploy_keepalived_ha.sh --all \
  --vip 192.168.90.202/24 stop
```

单节点操作使用 `--node 111` 或 `--node 112`。`restart` 是保留数据和配置的进程
级重启，不等价于整机 reboot。

notify 状态和失败重试在对应远端执行：

```bash
/opt/hpc-redis/keepalived/scripts/ha_notify.sh status
/opt/hpc-redis/keepalived/scripts/ha_notify.sh retry
```

重试后仍必须检查 VIP、keepalived PID、HA STATE 和 notify 状态；notify 失败不会
回滚 VRRP/VIP。

`scripts/ha_notify.sh` 和 `scripts/ha_healthcheck.sh` 不是独立测试 runner，而是
keepalived hook。它们的运行前提是：

- 已安装到 `/opt/hpc-redis/keepalived/scripts/` 并具有可执行权限；
- `redis-cli` 能连接本机 Redis `6379`；
- Redis 已注册 `HA STATE`、`HA PROMOTE`、`HA DEMOTE` 和 `HA FENCE` 命令；
- `ha_notify.sh` 的 status 目录和日志文件可写；
- `ha_healthcheck.sh` 只能用于判断本机 Redis 是否可服务，不能用来判断 Follower
  必须是 Master。

## 6. Aeron/UB 一主一备 failover

### 运行前提

- 已完成 keepalived 部署和配置检查；正式模式不要求提前启动 daemon；
- `src/redis-server`、`benchmark/vemb_v16_bench`、
  `benchmark/vemb_v16_topology_ctl` 和 `benchmark/tlc_ha_replica_ub_node_ut` 已构建；
- 111/112 的 Aeron request/response/warm path 与 CLI peer-view manifest 已按
  owner0/owner1 规则配置，并避开本机 Replica 设备；
- VIP、Redis TCP 端口 `6379` 和 HA control 端口 `9738` 未被其他测试占用；
- 两端可以互通控制面地址 `192.168.90.111`/`192.168.90.112`；
- 正式切主测试需要保留 notify、Redis、keepalived 和 topology 的远端结果目录；
- 运行反向场景前，必须先完成上一轮的进程停止、VIP 清理和结果保存。

正式模式（默认 `TLC_HA_AERON_SKIP_KEEPALIVED=0`）要求 keepalived 已经完成部署，
但启动状态应由 runner 管理。Aeron runner 在 P4 只执行 keepalived 的 `check` 和
`start`，不会自动执行 `deploy`；因此第一次运行前必须先执行：

```bash
scripts/deploy_keepalived_ha.sh --all \
  --vip 192.168.90.202/24 --install-deps deploy
scripts/deploy_keepalived_ha.sh --all \
  --vip 192.168.90.202/24 check
```

部署后不要手工启动 keepalived。runner 的 P2 会停止残留进程、删除 VIP 并验证
两端 VIP 为 0，P4 再启动 keepalived 并等待初始 owner 持有唯一 VIP。后续每轮
Aeron 测试仍应保留这一步骤，以便检查配置是否被修改；若脚本报告 binary 或配置
不存在，应先重新执行 `deploy`，不能设置 `SKIP_KEEPALIVED=1` 来绕过正式 VIP 测试。

默认方向为 111 owner0 Leader -> 112 owner1 Follower：

```bash
bash benchmark/tlc_ha_aeron_failover_111_to_112.sh
```

脚本流程为同步构建、清理 Redis/keepalived/VIP、重置 Replica ring、启动一主一备、
初始 Aeron ATTACH/VADD、kill 当前 Leader、等待 VIP 和 topology 切换、销毁旧 ring
后重新 ATTACH/VADD 和 warm read，最后重启旧节点并检查 recovery 追平。

反向验证 112 owner1 -> 111 owner0：

```bash
INITIAL_NODE=112 CLIENT_NODE=112 \
  bash benchmark/tlc_ha_aeron_failover_111_to_112.sh
```

常用诊断变量：

```text
TLC_HA_AERON_RUN_ID=name       固定远端结果目录
TLC_HA_AERON_WAIT_SECONDS=45   VIP/HA/recovery 等待上限
TLC_HA_AERON_TCP_PROBE=0       关闭旧 TCP 连接探针
TLC_HA_AERON_KEEP_SERVERS=1    失败后保留 Redis 进程
TLC_HA_AERON_RESET_REPLICA=0   诊断时跳过 Replica ring reset
```

只验证 HA 控制面、不启用 keepalived：

```bash
TLC_HA_AERON_SKIP_KEEPALIVED=1 \
  bash benchmark/tlc_ha_aeron_failover_111_to_112.sh
```

该模式使用 `HA PROMOTE`，不能作为 VRRP/VIP 漂移通过的证据。`SKIP_SYNC`、
`SKIP_KEEPALIVED` 和 `RESET_REPLICA=0` 都必须在测试报告中标注。

## 7. 推荐执行顺序

前一项失败时停止，不得跳过门禁进入后续故障场景：

```text
1. 远端身份、故障域、UB 设备、工具和 VIP preflight
2. 两端同步源码、构建并验证 server/client build stamp
3. tlc_ha_replica_ub_111_to_112.sh
4. tlc_ha_redis_tcp_111_to_112.sh
5. keepalived deploy/check/start，确认 VIP 恰好由一个节点持有
6. tlc_ha_aeron_failover_111_to_112.sh
7. 反向 Aeron failover
8. 停止 keepalived，清理 VIP，保存远端结果和日志
```

每次运行都要记录 `run_id`、远端结果目录、配置 hash、build stamp、故障动作、VIP
漂移时间、notify 结果、term、owner、durable/applied seq 和最终 PASS/FAIL。
