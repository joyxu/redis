# TLC HA/UB 测试可靠性与环境防错原则

## 1. 目的

本文记录 TLC HA、HA Replica UB 和 Aeron/UB failover 回归的测试治理原则，
用于避免将测试脚本、远端环境、构建产物、配置映射或历史残留误判为产品故障。

本文是后续 runner 和部署脚本实现的约束，不替代具体测试场景文档。场景、通过条件
和 111/112 的设备映射以以下文档为准：

- [TLC HA Keepalived/Failover 回归方案](./TLC_HA_KEEPALIVED_FAILOVER_REGRESSION_PLAN.md)
- [TLC HA/LVS/Keepalived 设计](./TLC_HA_LVS_KEEPALIVED_DESIGN.md)
- [TLC HA 跨节点 resync 设计](./TLC_HA_CROSS_NODE_RESYNC_DESIGN.md)

## 2. 总原则：失败即停止，结果必须可归因

测试 runner 必须采用 fail-closed 语义：

1. SSH 未执行、远端命令失败、构建产物不一致、配置无法解析或环境不完整时，立即
   停止测试。
2. 测试结果必须能区分脚本错误、环境错误、配置错误和产品行为错误，不能只依赖
   一个退出码或一行 PASS 输出。
3. 每个测试必须有唯一 `run_id`，保存故障前、故障中和恢复后的状态与日志。
4. 未通过前置门禁时，不得进入下一阶段，也不得把后续失败解释为 UB/HA 协议问题。
5. 未经明确授权，不得通过删除 COLD/AOF、重置数据或强制修改 VIP 来掩盖恢复问题。

## 3. 测试前强制 preflight

每次远程测试开始前，必须在 111 和 112 执行环境检查，并保存输出。

### 3.1 远端身份和故障域

必须确认：

- SSH 端口分别指向预期的 111 和 112；
- `hostname`、节点 IP、`/etc/machine-id` 与测试配置一致；
- 111 和 112 不是同一个系统实例、容器或网络命名空间；
- 需要独立故障域的测试（特别是整机 reboot）在身份检查失败时直接禁止执行。

当前 SSH 端口若映射到同一系统实例，只允许执行进程级、keepalived 级或协议级测试，
不能据此宣称整机故障隔离已经通过。

### 3.2 工具、权限和端口

必须检查以下工具和资源：

```text
ssh、scp、timeout、awk、ss、ip、redis-cli
Redis server/client binary
UB 设备节点及其读写权限
Redis TCP 端口和 HA 控制面端口
keepalived binary、配置目录和 PID 目录
```

检查失败时输出明确的节点、路径、端口和权限信息。

### 3.3 运行时残留

启动新场景前，必须确认：

- 旧 Redis 进程已经停止；
- Redis 测试端口没有监听；
- 旧 keepalived 进程已经停止；
- 两端 VIP 均为 0；
- 旧 PID 文件已清理；
- 旧 runner 子进程和 SSH 控制连接已退出；
- UB 设备没有被其他 CLI、benchmark 或 Replica 进程占用。

进程判断必须使用 `pidfile + kill -0 + ss`，必要时再检查
`/proc/<pid>/cmdline`。禁止使用可能匹配 SSH 命令或 runner 自身的 `pgrep -f`
或 `pkill -f` 作为唯一判断依据。

## 4. 同步、构建和产物验证

### 4.1 每轮必须同步

除非是明确记录的本地诊断，UB 真机和 HA 真机测试每一轮都必须先执行：

```bash
scripts/sync_changed_code_to_peer.sh \
  --all-code --build all --verify-build all
```

LVS 不属于本测试数据路径，不需要同步；远端已有 LVS 位于
`/root/szz/codespace/LVS`。

### 4.2 必须保存的构建证据

每台机器至少保存：

```text
本地 git HEAD 和远端 source hash
server build stamp
client build stamp
关键 binary 的 size、mtime、sha256 和可执行状态
构建命令、退出码和完整日志
```

以下任一情况都必须阻止测试：

- server/client build stamp 不一致；
- binary 为 0 字节、不可执行或 hash 与预期不符；
- 构建被中断但仍继续使用产物；
- 代码已同步但没有重新构建；
- 两个 runner 同时覆盖同一远端构建目录。

构建应使用锁或等价机制避免并发覆盖。`SKIP_SYNC` 只能用于诊断，并且必须提供
同一提交的远端 hash 和 build-stamp 证据。

## 5. UB 设备和 peer-view 配置防错

### 5.1 固定的 Replica 设备

当前 HA Replica 只使用以下方向：

```text
111 TX=/dev/obmm_shmdev4  -> 112 RX=/dev/obmm_shmdev8
112 TX=/dev/obmm_shmdev9  -> 111 RX=/dev/obmm_shmdev13
```

两端必须使用同一 mmap offset；设备路径、方向、offset、权限或 peer identity 任一
不匹配，都必须在启动前报错。

### 5.2 CLI 同机设备避让

CLI 与 Redis/Replica 同机运行时，CLI path 不能复用本机 Replica 设备，即使 offset
不同也视为冲突：

```text
CLI@111 owner0: dev1/dev2/dev3
CLI@111 owner1: dev14/dev11/dev16
CLI@112 owner1: dev1/dev2/dev3
CLI@112 owner0: dev7/dev2/dev5
```

CLI failover 后可能从远端 owner 变为本地 owner，必须重新检查新 path 是否与本机
Replica 冲突。旧 owner channel 必须先销毁，不能在同一时刻保留两套 mmap。

### 5.3 owner 和 manifest 校验

本轮一主一备模型固定为：

```text
111 -> owner_id=0
112 -> owner_id=1
active owner 数量必须恰好为 1
```

启动前应静态检查：

- server manifest、topology 和 CLI peer-view 的 owner ID 一致；
- provider path 能被 client peer-view 解析；
- request、response、warm 顺序一致；
- active/standby 配置不是 cluster 模式的 `active=0,1`；
- `provider_path`、`client_path`、offset 和 bytes 均可验证。

最好在启动 Redis 前执行一次 ATTACH/manifest resolve preflight，提前发现 provider
path 不匹配，而不是把配置错误表现为 `EAGAIN`、mmap 失败或 ring 空转。

## 6. COLD、AOF 和测试目录隔离

测试必须区分 fresh-start 和 recovery/restart：

```text
fresh-start：每个 case 使用全新的目录
restart/recovery：保留原 COLD、AOF、checkpoint 和 UB ring
```

每个 case 启动前保存目录清单、inode、size、mtime、AOF hash 和 checkpoint manifest。

必须遵守以下语义：

- 没有 checkpoint 时允许从连续 AOF 增量启动；
- 有 `checkpoint.manifest` 时必须完整校验 generation、term、seq、长度和 checksum；
- 只有 fresh 目录中的 0 字节 AOF 才能直接解释为初始空日志；
- 不能把上一个场景遗留的 AOF 或 checkpoint 当作 fresh 输入；
- 重启恢复测试不能通过删除持久化数据来清理状态。

如果启动目录只有空 AOF，报告中必须同时说明目录是否为本轮新建，以及启动后 AOF
是否因新事件发生了 size/mtime 变化。

## 7. Redis 和 keepalived 生命周期

### 7.1 Redis

Redis 启动后必须验证：

- PID 文件内容对应实际 Redis 进程；
- 目标端口由该 PID 监听；
- `PING` 和 `HA STATE` 返回预期结果；
- `HA PROGRESS` 的字段可以解析。

停止 Redis 后必须再次确认 PID 消失、端口关闭，并保存最后的 HA 状态和日志。

### 7.2 keepalived 和 VIP

每个 keepalived 场景开始前固定执行：

1. 停止两端 keepalived；
2. 显式删除两端 VIP；
3. 删除旧 PID 文件；
4. 验证两端 VIP 数量均为 0；
5. 启动指定节点并等待 VRRP 收敛；
6. 验证 VIP 恰好由一个节点持有。

VIP 检查必须按地址字段精确匹配，不能依赖模糊前缀匹配。`weight 0` 用于保证
健康检查失败时进入 FAULT 并释放 VIP；不能依赖 `nopreempt + weight -20` 保证
Backup 接管。

notify 失败不会回滚 VRRP 状态，因此必须同时检查：

```text
keepalived PID
VIP 归属
HA STATE
notify.status
```

旧 notify 文件不能单独证明当前状态。Redis 恢复且路由可用后，应执行 notify retry，
并确认结果从 `failed` 变为 `success`。

## 8. 故障动作、超时和结果留证

每一轮只执行一个明确故障动作：

```text
只 kill Redis
只停止 keepalived
同时停止 Redis 和 keepalived
```

不要在同一轮混入 COLD reset、修改 manifest、改变 UB path 或重新编译。

每个阶段都必须有有界 timeout。timeout 时至少输出：

```text
最后一次 HA STATE
最后一次 HA PROGRESS
VIP count 和 owner
Redis/keepalived PID
notify status
ingress/apply/sender/producer/replay 计数
当前 term、generation、owner 和 seq 边界
```

runner 应保存以下时间点和结果：

```text
故障前状态
故障动作时间
VIP 漂移时间
notify 完成时间
新 owner 可写时间
旧连接失败时间
recovery 完成时间
最终 durable_seq/applied_seq
```

远端命令应通过明确的 shell 参数或 `ssh ... bash -s` 执行，并输出唯一完成标记，
例如：

```text
REMOTE_STEP_OK step=stop_redis node=111
```

本地必须同时检查 SSH 返回码、完成标记和远端状态查询，不能只相信 stdout。

## 9. Aeron/UB failover 的专门约束

VIP 漂移不会自动迁移已经建立的 UB ring。客户端 failover 必须验证完整流程：

```text
旧 ring publish/poll 失败
-> 销毁旧 ring
-> 通过 VIP 刷新 topology
-> 根据新 active owner 选择 peer-view
-> 重新 AERON_ATTACH
-> 建立新 ring 并恢复读写
```

客户端不能继续使用旧 owner 的 Import ring，也不能自动重放结果未知的非幂等写请求。

新的 active owner 必须满足：

- owner ID 与物理节点关系仍为 `0@111/1@112`；
- topology 中只有一个 active owner；
- 新 path 不占用本机 Replica 设备；
- ATTACH 返回的 provider path、offset 和 bytes 与 peer-view resolve 结果一致。

## 10. 当前已具备和后续需要补强的措施

当前已有：

- sync 脚本的 SHA-256 比较、复制后校验和 build-stamp 校验；
- runner 的 UB path、offset、对齐和范围检查；
- 每轮唯一结果目录和阶段 timeout；
- keepalived 显式 VIP 清理；
- VIP 恰好一个节点持有的检查；
- Aeron failover 后旧 ring 销毁和重新 ATTACH。

后续实现应继续补强：

1. 111/112 独立故障域自动检测；
2. 统一 manifest 生成与 ATTACH/peer-view resolve preflight；
3. 远端进程身份的精确校验；
4. 构建锁和中断构建保护；
5. 每个 COLD/AOF case 的快照留证；
6. 远端命令完成标记和结构化结果文件；
7. 将“命令未执行”“环境不满足”“配置不匹配”和“产品断言失败”分成不同错误码。

## 11. 验证脚本使用方法

脚本的完整命令、参数、诊断模式和推荐执行顺序见
[scripts/TLC_HA_UB_TEST_SCRIPTS.md](../scripts/TLC_HA_UB_TEST_SCRIPTS.md)。本节保留
测试可靠性文档中的使用索引，脚本细节以该手册为准。

以下命令均在本地 hpc-redis 根目录执行。除明确标记为诊断模式外，脚本会自动调用
`scripts/sync_changed_code_to_peer.sh`，在 111 和 112 同步、构建并验证远端产物。

### 11.1 共享配置

默认配置文件为：

```text
examples/tlc_ha_replica_ub_111_to_112.env
```

需要临时覆盖配置时，使用独立 env 文件，不要直接修改测试脚本：

```bash
TLC_HA_UB_CONFIG=/path/to/test.env \
  bash benchmark/tlc_ha_replica_ub_111_to_112.sh
```

测试开始前应确认 env 中的 Replica 映射仍为：

```text
111 TX=dev4, RX=dev13
112 TX=dev9, RX=dev8
```

### 11.2 HA Replica UB 基础回归

执行完整 UB 数据面、COLD、M5、M6、M7 和 Follower recovery：

```bash
bash benchmark/tlc_ha_replica_ub_111_to_112.sh
```

该脚本覆盖 visibility、普通 EVENTS、append ACK、snapshot/checkpoint、M5 handoff、
M5 timeout/abort、M6 GAP repair、retention -> snapshot、M7 compact/pressure 和
重启后的 AOF replay。它不启动 `redis-server`，不能单独证明 Redis TCP、VIP、
keepalived 或客户端 failover。

只回归 M7 retention 边界：

```bash
TLC_HA_UB_M7_ONLY=1 \
  bash benchmark/tlc_ha_replica_ub_111_to_112.sh
```

`M7_ONLY PASS` 只表示 M7 retention 用例通过，不代表完整 HA 切主通过。

常用规模和超时覆盖：

```bash
TLC_HA_EXPECTED_EVENTS=1000 \
TLC_HA_TEST_TIMEOUT_SECONDS=90 \
  bash benchmark/tlc_ha_replica_ub_111_to_112.sh
```

重启 replay 使用单独的超时变量：

```bash
TLC_HA_RESTART_REPLAY_TIMEOUT_SECONDS=120 \
  bash benchmark/tlc_ha_replica_ub_111_to_112.sh
```

只有在已有同一提交的远端 binary 并且正在做诊断时，才允许：

```bash
TLC_HA_UB_SKIP_SYNC=1 \
  bash benchmark/tlc_ha_replica_ub_111_to_112.sh
```

### 11.3 Redis TCP/SDK 固定角色回归

执行 Redis TCP/SDK -> Leader -> Replica UB -> Follower TCP/SDK，以及 Follower COLD
重启恢复：

```bash
bash benchmark/tlc_ha_redis_tcp_111_to_112.sh
```

默认端口和规模为：

```text
Redis TCP: 6399
HA control: 9737
事件数: 10000
dim: 16
```

常用覆盖方式：

```bash
TLC_HA_REDIS_TCP_EVENT_COUNT=1000 \
TLC_HA_REDIS_TCP_RUN_ID=tcp_smoke_1000 \
  bash benchmark/tlc_ha_redis_tcp_111_to_112.sh
```

该脚本验证固定 Leader/Follower 数据链路和 COLD recovery，不验证 keepalived、VIP
漂移或 Aeron 客户端重新 ATTACH。

### 11.4 keepalived 部署和生命周期

先确认 VIP 没有被其他测试使用，再在两台机器部署 VRRP-only keepalived：

```bash
scripts/deploy_keepalived_ha.sh --all \
  --vip 192.168.90.202/24 --install-deps deploy
```

部署动作会从远端已有的 `/root/szz/codespace/LVS/tools/keepalived` 构建 1.2.2，
安装配置和 notify/healthcheck hook，并保持 daemon 停止。配置检查和启动必须显式执行：

```bash
scripts/deploy_keepalived_ha.sh --all \
  --vip 192.168.90.202/24 check
scripts/deploy_keepalived_ha.sh --all \
  --vip 192.168.90.202/24 start
scripts/deploy_keepalived_ha.sh --all \
  --vip 192.168.90.202/24 status
```

停止或切换场景前，显式清理两端并确认 VIP 数量为 0：

```bash
scripts/deploy_keepalived_ha.sh --all \
  --vip 192.168.90.202/24 stop
```

单节点操作使用 `--node 111` 或 `--node 112`。`restart` 只用于保留该节点数据和
配置的进程级重启；它不等价于整机 reboot。

notify 状态在对应远端执行：

```bash
/opt/hpc-redis/keepalived/scripts/ha_notify.sh status
/opt/hpc-redis/keepalived/scripts/ha_notify.sh retry
```

`retry` 只能重放最近一次失败的 MASTER/BACKUP/FAULT 操作。执行后仍需检查 VIP、
keepalived PID 和 `HA STATE`，不能只看 notify 文件。

### 11.5 Aeron/UB 一主一备 failover

正式的 Aeron/UB HA 切主使用独立 runner：

```bash
bash benchmark/tlc_ha_aeron_failover_111_to_112.sh
```

默认流程为：

```text
同步并构建
-> 停止旧 Redis/keepalived，清理 VIP
-> 重置 Replica ring
-> 启动 111 owner0 Leader、112 owner1 Follower
-> topology 设置为 active owner=0
-> CLI@111 通过 VIP 执行 Aeron ATTACH/VADD
-> kill 111 Redis，等待 112 接管 VIP 并 PROMOTE
-> topology 切换为 active owner=1
-> CLI 销毁旧 ring，经 VIP 重新 ATTACH/VADD 和 VEMB_HANDLE warm read
-> 重启 111 为 Follower，验证 recovery 和 durable/applied 追平
```

反向验证 112 owner1 -> 111 owner0：

```bash
INITIAL_NODE=112 CLIENT_NODE=112 \
  bash benchmark/tlc_ha_aeron_failover_111_to_112.sh
```

常用诊断变量：

```text
TLC_HA_AERON_RUN_ID=name       固定远端结果目录名
TLC_HA_AERON_WAIT_SECONDS=45   VIP/HA/recovery 等待上限
TLC_HA_AERON_TCP_PROBE=0       关闭旧 TCP 连接探针
TLC_HA_AERON_KEEP_SERVERS=1    失败后保留 Redis 进程以便检查
TLC_HA_AERON_RESET_REPLICA=0   仅用于诊断，跳过 ring reset
```

不启用 keepalived、只验证控制面切换时：

```bash
TLC_HA_AERON_SKIP_KEEPALIVED=1 \
  bash benchmark/tlc_ha_aeron_failover_111_to_112.sh
```

该模式使用 `HA PROMOTE`，不能作为 VRRP/VIP 漂移通过的证据。`SKIP_SYNC`、
`SKIP_KEEPALIVED` 和 `RESET_REPLICA=0` 都必须在报告中注明，因为它们会减少正式
回归覆盖范围。

### 11.6 推荐执行顺序

正式回归按以下顺序执行，前一项失败时停止：

```text
1. 远端身份、UB 设备、VIP 和工具 preflight
2. 每台机器同步源码并构建，验证 server/client build stamp
3. tlc_ha_replica_ub_111_to_112.sh
4. tlc_ha_redis_tcp_111_to_112.sh
5. keepalived deploy/check/start，验证初始 VIP=1
6. tlc_ha_aeron_failover_111_to_112.sh
7. 反向 Aeron failover
8. 停止 keepalived、清理 VIP、保存结果和日志
```

每个脚本的 `run_id`、远端结果目录、配置 hash、build stamp 和最终 PASS/FAIL 输出
都必须写入回归记录。

## 12. 测试前检查清单

在启动正式测试前，runner 必须能够回答以下问题：

```text
[ ] SSH 端口确实对应 111/112，且故障域符合当前场景
[ ] 本地和远端代码 hash、server/client build stamp 一致
[ ] Redis、keepalived、旧 runner 和旧 VIP 已清理
[ ] UB 设备存在、权限正确、未被占用
[ ] Replica TX/RX path、offset 和 peer-view 映射正确
[ ] CLI path 避开本机 Replica 设备
[ ] owner ID、active/standby 和 manifest 语义一致
[ ] COLD/AOF 目录符合 fresh 或 recovery 测试语义
[ ] Redis TCP、HA 控制面和 redis-cli 可用
[ ] timeout、日志目录和结果目录可写
[ ] 故障动作、通过条件和恢复步骤已经固定
```

任一项无法确认，都应停止并修复测试前置条件，而不是继续运行并归因于 UB/HA
实现。
