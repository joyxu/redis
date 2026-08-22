# VEMB V16 扩容节点迁移设计

更新日期：2026-07-08

本文描述当前 P0 扩容设计和实现口径。核心路径已经从“外部工具逐 key
驱动迁移”收敛为：

```text
top_ctl 只发布新节点加入 / candidate topology
-> source SuperNode 自动计算本机迁出 key
-> source 自动 push baseline + delta outbox 追平
-> per-range checkpoint barrier
-> per-range final fence + target lease commit
-> proxy 回调 top_ctl 本机 local done
-> top_ctl 收齐所有 source 后统一发布 full active topology
-> source SOURCE_GC / DONE
```

外部 `vemb_v16_topology_ctl` 仍保留手动 range barrier/cutover/source-gc
命令，但在当前推荐路径里主要作为 debug、观测和人工介入工具，不是
server/server 扩容闭环的必需依赖。

## 设计目标

P0 的目标是 normal-path no-loss cutover：

```text
无 source / target / top_ctl 进程崩溃、无永久网络分区时：
  已返回 OK 的 VADD/VREM 在 cutover 后不丢失。
  DELETE tombstone 不会被旧 baseline 复活。
  旧 snapshot / 旧 delta 不会覆盖更高 key_version。
  source 和 target 不会同时成为同一 key 的权威写 owner。
  非迁移场景不进入扩容 slow path。
```

P0 不承诺：

```text
source 返回 OK 后立刻崩溃时，内存 outbox 中未 ack delta 不丢。
target 重启后自动恢复 applied_seq / owner lease。
top_ctl 崩溃后恢复 coordinator progress。
client retry 在缺少 request_id 去重表时严格幂等。
COLD 层全量迁移和生产级 SOURCE_GC 回收水位。
```

这些属于 P1 的 WAL、持久 lease、持久 applied_seq、request 去重和
crash recovery。

## 关键概念

```text
active ring:
  当前客户端普通读写使用的 owner ring。

standby ring:
  扩容 candidate ring。source 根据 active/standby 差异计算哪些 key
  要迁到新 owner。

topology_epoch:
  拓扑版本。candidate topology 和 full active topology 都必须递增。

min_write_epoch:
  写入请求允许的最小 topology_epoch。P0 fast path 要避免在没有 active
  migration 时把它扩大成全库写 fence。

key_version:
  key 级单调版本，用于拒绝旧 snapshot、旧 delta 和乱序写。

tombstone:
  删除也是带版本的写。target 上更高版本 tombstone 不能被旧 snapshot 覆盖。

range:
  当前实现中的迁移范围为
  (migration_topology_epoch, target_owner, shard_id)。

outbox:
  source 侧迁移 delta 队列。MIGRATING key 的本地写提交后 append delta，
  target ack 后推进 acked_seq。

checkpoint barrier:
  只记录 checkpoint_seq = last_assigned_seq，并尝试 drain 到 target。
  它不冻结 source 写。

final fence:
  cutover 前的短窗口。outbox 进入 FENCING，不再接受新的 delta append；
  drain 到 final_barrier_seq 后提交 target lease。

owner lease / owner_epoch:
  target 接受写权威前必须提交的 fence token。source 只有在 target lease
  commit 成功后才能本地 mark CUTOVER。
```

## 拓扑模型

扩容前：

```text
active  = {0,1}
standby = {0,1}
```

发布 candidate topology：

```text
active  = {0,1}
standby = {0,1,2}
flags   = DUAL_WRITE_REQUIRED | AUTO_SCALEOUT | COORDINATED_SCALEOUT
epoch   = migration_epoch
```

完成后发布 full active topology：

```text
active  = {0,1,2}
standby = {0,1,2}
epoch   = cutover_epoch
```

设计边界：

```text
CLI/SDK 不做扩容期双写。
CLI/SDK 只写 active owner。
source SuperNode 负责 baseline + delta 追平 target。
target owner 只接受自己提交过 payload/meta/lease 的 ASK redirect 写。
```

## 角色分工

```text
top_ctl:
  发布 candidate topology。
  coordinated 模式下监听 source local done callback。
  收齐所有 source 后统一发布 full active topology。

proxy:
  负责 control plane 网络入口。
  周期观察 storage scaleout status。
  phase == NOTIFY_PENDING 时回调 top_ctl。
  callback 失败可重试，不进入普通读写 fast path。

source SuperNode / storage:
  收到 candidate topology 后自动计算本机 migrate plan。
  标记 MIGRATING。
  push baseline snapshot。
  对迁移 key 的 VADD/VREM 追加 delta outbox。
  推进 range checkpoint barrier、final fence、target lease commit。
  coordinated 模式下 local done 后等待 top_ctl global publish。

target SuperNode / storage:
  apply baseline / delta。
  按 topology_epoch + key_version + tombstone 幂等合并。
  keyed BARRIER_REQ 校验 baseline/tombstone 已 ready。
  LEASE_COMMIT_REQ 成功后允许 ASK redirect 写进入本地提交。

benchmark client / SDK helper:
  通过 TOPOLOGY_GET 拉取 active/standby ring 和 owner endpoint table。
  收到 STALE_TOPOLOGY/MOVED 后刷新 topology 并重试。
  收到 ASK(target_owner) 后按 redirect_owner 做一次 ASK redirect 短重试，
  target gate 仍不 ready 时回退 refresh topology。
```

## 端到端流程

### 1. 新节点启动

新节点先完成本地存储和通信初始化：

```text
parse manifest
open/mmap local WARM payload region
open/mmap remote meta views
build UB RPC peers
start SuperNode/proxy worker
expose TCP/UDS control endpoint
```

此时新节点可以被 control plane 发现，但还不是客户端 active owner。

### 2. 发布 candidate topology

`top_ctl` 发布：

```text
active  = old owners
standby = old owners + new owners
flags   = DUAL_WRITE_REQUIRED | AUTO_SCALEOUT
```

多 source 扩容再加：

```text
flags += COORDINATED_SCALEOUT
coordinator_endpoint = top_ctl callback endpoint
```

`owner_endpoints` 必须包含所有 active/standby owner 的 endpoint。ASK/MOVED
只携带 `redirect_owner`，CLI/SDK 需要通过 topology endpoint table 找到
target 的 host/port 或 UDS path。

### 3. source 自动计算 migrate plan

source 收到 candidate topology 后，枚举本机 TLC key_meta：

```text
old_owner = hash(key, active ring)
new_owner = hash(key, standby ring)

if old_owner == local_owner && new_owner != local_owner:
  mark key MIGRATING
  target_owner = new_owner
  shard_id = key_hash % shard_count
```

当前实现会自动按 shard 拆 range，避免所有 key 堆在单个 range：

```text
range = (migration_epoch, target_owner, shard_id)
shard_count <= 16
range_count <= VEMB_V16_STORAGE_MAX_SCALEOUT_RANGES
```

手动/debug `MARK_MIGRATING` 仍允许调用方显式指定 `shard_id`。

### 4. baseline snapshot

source 在 mark MIGRATING 后生成 baseline descriptor，并通过 UB migration RPC
发送给 target：

```text
MIGRATE_BASELINE_PUT:
  key / key_hash
  topology_epoch
  key_version
  tombstone
  payload descriptor
  target_owner
  shard_id
```

target apply 规则：

```text
payload 写入完成后才能提交 DEST_COMMITTED meta。
topology_epoch 更旧则拒绝。
key_version 更旧则拒绝。
key_version 相等按 duplicate 处理。
tombstone 版本高于 snapshot 时，snapshot 不能复活旧值。
```

如果 target 暂不可达或返回非终态，source 不回滚 candidate topology，而是把
baseline 放入进程内 retry queue，由 migration retry worker 后台重发。后续
keyed barrier / lease commit 会挡住缺失 baseline 的 cutover。

### 5. 迁移期间写入

CLI/SDK 仍写 active owner，也就是 source：

```text
client -> source active owner
source 本地提交 VADD/VREM
source bump key_version / 写 tombstone
source append delta outbox
source 尝试 push MIGRATE_DELTA_PUT / MIGRATE_DELTA_DELETE
source 收到 target OK/DUPLICATE/STALE_REJECTED 后 ack outbox
```

source 返回用户 OK 的边界：

```text
local write success + delta append success
```

即时 push timeout/busy 不让用户写失败；pending delta 留在 outbox，由 retry
worker 或后续 barrier drain 追平。

### 6. range checkpoint barrier

range barrier 当前是 checkpoint，不是停写点：

```text
checkpoint_seq = outbox.last_assigned_seq
outbox 保持 OPEN
source 后续 VADD/VREM 继续本地提交并 append delta
storage 尝试 drain 当前 pending delta
response.range_ready 表示 source acked_seq >= checkpoint_seq
```

因此，checkpoint barrier 到 final cutover 之间不会因为 barrier 本身让
MIGRATING 写长期失败。

### 7. final fence + cutover

range cutover 内部执行 final fence：

```text
begin_final_fence:
  outbox OPEN -> FENCING
  final_barrier_seq = last_assigned_seq
  新的 source delta append 被拒

drain:
  push pending delta
  等 source acked_seq >= final_barrier_seq

target ready check:
  对每个 key 发送 keyed BARRIER_REQ
  要求 target applied_seq >= final_barrier_seq
  要求 key 已 DEST_COMMITTED 或 tombstone ready
  要求 target_owner / topology_epoch / shard_id 匹配

lease commit:
  对每个 key 发送 LEASE_COMMIT_REQ
  target 只在 key ready 且 owner_epoch 不倒退时接受

source mark cutover:
  target lease 成功后 source 本地 mark CUTOVER
  source 不再作为该 key 权威 owner
```

如果 drain、target ready 或 lease commit 失败：

```text
abort_final_fence
outbox 恢复 OPEN
source 写入恢复正常
下一轮 auto step / control retry 重新尝试 cutover
```

final fence 窗口内的 source 写会返回 `ASK(target_owner)` 或 retry 语义。
target-side ASK gate 不会盲目接收写，必须满足：

```text
key 已 DEST_COMMITTED 或 tombstone ready
target_owner 是本机
owner lease 已 commit
target applied_seq >= final_barrier_seq
```

否则 target 继续返回 ASK，客户端再 refresh topology / retry。

### 8. coordinated local done callback

多 source 场景下，单个 source 的 local done 不等于 global done。例如：

```text
active={0,1} -> active={0,1,2}
```

owner 0 和 owner 1 都可能有 key 迁给 owner 2。任意一个 source 先完成，都不能
自行发布 full active。

coordinated 模式状态：

```text
LOCAL_DONE:
  本机 migration_epoch 下无 MIGRATING key。
  baseline retry pending 为 0。
  outgoing outbox 均 cutover_ready。
  target keyed BARRIER_REQ / LEASE_COMMIT_REQ 已通过。

NOTIFY_PENDING:
  本机 local done，等待 proxy 成功通知 top_ctl。

NOTIFIED / GLOBAL_CUTOVER_WAIT:
  top_ctl 已 ack，本机等待 global full active topology。

DONE:
  收到 full active topology，SOURCE_GC 完成。
```

proxy callback payload：

```text
notify_seq
migration_topology_epoch
cutover_topology_epoch
source_owner
phase
error_code
pending_delta
baseline_retry_pending
migrating_key_count
range_count
```

top_ctl 以 `(migration_epoch, source_owner, notify_seq)` 幂等 ack。

### 9. top_ctl 发布 full active

top_ctl 收齐所有 old active source 的 `SCALEOUT_LOCAL_DONE` 后，统一发布：

```text
active  = {old owners + new owners}
standby = {old owners + new owners}
epoch   = cutover_epoch
flags   = 0 或不再包含 DUAL_WRITE_REQUIRED
```

source 收到 full active 后，coordinated source 才推进 SOURCE_GC / DONE。

### 10. SOURCE_GC

P0 的 SOURCE_GC 是安全 fence 状态，不是激进删除：

```text
CUTOVER -> SOURCE_GC
保留 source meta/tombstone/fence 信息
旧 source 请求返回 MOVED(target_owner)
不做 P1 级持久恢复和激进 payload 回收
```

## 读写路径语义

### 非迁移 fast path

扩容代码对普通读写路径的影响必须可控。当前实现已加入两个 fast path counter：

```text
storage.migration_active_count:
  为 0 时，SuperNode VADD/VREM/read miss 跳过 stale epoch、ASK/MOVED、
  outbox、barrier、migration delta 判断。

tlc.source_fence_active_count:
  为 0 且没有 tombstone 时，read 跳过 source-fence lookup。
```

普通 VADD/VREM 仍会为了正常 key_version/tombstone 元数据获取 key_meta lock；
这不属于扩容 slow path。进一步 lock-free 需要单独的 key_meta RCU/seqlock
或分片锁设计。

### MIGRATING source 写

```text
outbox OPEN:
  source 本地提交
  append delta
  返回 OK

outbox FENCING:
  source 不再 append delta
  返回 ASK(target_owner) / retry
```

### CUTOVER / SOURCE_GC source 写

```text
source 不再是权威 owner。
返回 MOVED(target_owner)。
```

### ASK redirect 写

ASK 不是让客户端永久刷新 topology，而是一次临时 redirect：

```text
source -> client: ASK(target_owner)
client 根据 topology endpoint table 找到 target endpoint
client 带 VEMB_V16_REQ_F_ASK_REDIRECT 直连 target 重试一次
target gate ready: commit
target gate not ready: 继续 ASK，client refresh topology / retry
```

### 读路径

```text
cutover 前:
  普通读仍走 active source。

target 未 DEST_COMMITTED:
  不能把未完成 baseline 当成本地命中返回。

source CUTOVER/SOURCE_GC:
  source 对旧请求返回 MOVED(target_owner)。
```

## 正确性条件

cutover 必须同时满足：

```text
baseline ready:
  target 已有 key 的 DEST_COMMITTED meta，或更高版本 tombstone。

delta ready:
  source acked_seq >= final_barrier_seq。
  target keyed BARRIER_REQ 返回 applied_seq >= final_barrier_seq。

lease ready:
  target LEASE_COMMIT_REQ 成功。
  owner_epoch/topology_epoch 不倒退。

publish guard:
  source 本机无未提交 MIGRATING key。
  outbox 均 cutover_ready。
  coordinated 模式必须等待 top_ctl global publish。
```

冲突处理：

```text
旧 snapshot 覆盖新 delta:
  target 用 key_version 拒绝旧 snapshot。

删除与迁移并发:
  VREM 生成 tombstone delta，旧 snapshot 不能复活。

客户端旧 topology 写:
  source/target 返回 STALE_TOPOLOGY、MOVED 或 ASK，引导 refresh/retry。

payload 尚未写完:
  target 只发布 DEST_COMMITTED meta，读路径不读取 DEST_PREPARED。

source 清理过早:
  P0 SOURCE_GC 保留 fence/meta，不激进释放。
```

## range 分页

单次 range control response 的 key 数上限为
`VEMB_V16_MIGRATION_CONTROL_MAX_RANGE_KEYS`。当前实现已经把 barrier 和
cutover/source-gc 拆开：

```text
range barrier:
  只 count range 内 MIGRATING key + checkpoint/drain outbox。
  不把所有 key 塞进 response。

range cutover/source-gc:
  每次处理一页 key。
  request.page_limit = 0 使用默认页大小。
  page_limit 超过默认值会 cap 到默认值。
  response.remaining_keys 表示剩余未处理 key。
  response.range_done 表示本 action 已完成。
```

`vemb_v16_topology_ctl --range-wait-cutover`、`--range-cutover`、
`--range-source-gc` 会循环到 `range_done` 或超时。

## 执行脚本

推荐直接运行 server smoke，覆盖真实三节点、两个 source、coordinated callback、
range 分页和 cutover 后 Node2/owner2 直连写入：

```bash
cd /Users/szza/codespace/work/hpc-redis
make -C benchmark vemb_v16_scaleout_coordinated_server_smoke
```

可显式固定端口、数据规模和 epoch：

```bash
cd /Users/szza/codespace/work/hpc-redis

export VEMB_V16_SCALEOUT_SMOKE_BASE_PORT=29600
export VEMB_V16_SCALEOUT_SMOKE_DIM=4
export VEMB_V16_SCALEOUT_SMOKE_PREFILL=256
export VEMB_V16_SCALEOUT_SMOKE_OPS=96
export VEMB_V16_SCALEOUT_SMOKE_MIGRATION_EPOCH=23

make -C benchmark vemb_v16_scaleout_coordinated_server_smoke
```

脚本内部流程：

```text
1. build vemb_v16_server / vemb_v16_bench / vemb_v16_topology_ctl。
2. 启动 owner 0、1、2 三个 SuperNode。
3. 发布初始 active={0,1}, standby={0,1} 到 old sources。
4. benchmark client-topology 只 bootstrap owner 0 endpoint，并通过 topology
   discovery prefill 数据。
5. 启动 top_ctl coordinator-listen，等待 source 0/1 callback。
6. 发布 candidate:
   active={0,1}, standby={0,1,2},
   flags=DUAL_WRITE_REQUIRED | AUTO_SCALEOUT | COORDINATED_SCALEOUT。
7. source 0/1 自动 mark MIGRATING、push baseline、drain delta、
   checkpoint barrier、final fence、lease commit。
8. proxy 回调 top_ctl local done。
9. top_ctl 收齐 source 0/1 后发布 full active={0,1,2} 到三台 server。
10. source 收到 full active 后 SOURCE_GC/DONE。
11. benchmark 再跑普通 client-topology 写入，验证 owner 2 已接收 direct VADD。
```

成功输出应包含：

```text
[ok] coordinator collected both sources and published full-active topology
[ok] all nodes converged to active owners 0,1,2
[ok] coordinated scaleout server smoke passed node2_vadd=...
```

## 两机真实执行指令：`{0} -> {0,1}`

以下步骤用于真实两机环境验证单 source 扩容：`node0 -> node0,node1`。

环境约定：

- `192.168.90.111` = `node0` = owner `0`
- `192.168.90.112` = `node1` = owner `1`
- 代码目录：`/root/szz/codespace/hpc-redis`
- server 监听端口：`6391`
- top_ctl coordinator 监听端口：`7391`
- UB 设备映射：

| 语义 | node0 (`192.168.90.111`) | node1 (`192.168.90.112`) |
| --- | --- | --- |
| 本地 payload export | `/dev/obmm_shmdev1` | `/dev/obmm_shmdev1` |
| 对端 payload import | `/dev/obmm_shmdev5` | `/dev/obmm_shmdev5` |
| 本地 request export | `/dev/obmm_shmdev2` | `/dev/obmm_shmdev2` |
| 对端 request import | `/dev/obmm_shmdev6` | `/dev/obmm_shmdev6` |
| 本地 response export | `/dev/obmm_shmdev4` | `/dev/obmm_shmdev4` |
| 对端 response import | `/dev/obmm_shmdev8` | `/dev/obmm_shmdev8` |
| 预留扩展 payload export | `/dev/obmm_shmdev3` | `/dev/obmm_shmdev3` |
| 预留扩展 payload import | `/dev/obmm_shmdev7` | `/dev/obmm_shmdev7` |

对应关系：

| 本地设备 | 对端导入设备 | 用途 |
| --- | --- | --- |
| `1` | `5` | payload / remote meta view |
| `2` | `6` | request lane |
| `3` | `7` | extra payload region |
| `4` | `8` | response lane |
- payload UB path：
  - 本地 CC：`/dev/obmm_shmdev1`
  - 远端 NC view：`/dev/obmm_shmdev5`
- remote meta UB path：
  - 本地 meta owner view：`/dev/obmm_shmdev1@268435456`
  - 对端 meta view：`/dev/obmm_shmdev5@268435456`
- UB RPC ring path：
  - request lane：`/dev/obmm_shmdev2`
  - inbound request lane：`/dev/obmm_shmdev6`
  - response lane：`/dev/obmm_shmdev8`
  - outbound response lane：`/dev/obmm_shmdev4`

注意：

- `--tcp-host` 必须使用真实 IP，不能使用 `0.0.0.0`，否则 client topology 会返回不可路由 endpoint。
- 当前 baseline/delta payload 仍要求 target 能看到 source warm payload，因此 manifest 中仍需同时保留 `region100` 与 `region101` 的 `warm_regions` 映射，不能把 `/dev/obmm_shmdev5` 仅配置成 meta-only。
- 当前 `--reset-warm-regions` 会 reset manifest 中列出的 region/meta backing。实际执行时仅让 `node0` 首次启动带 `--reset-warm-regions`。

### 1. 两台机器编译

`192.168.90.111` 和 `192.168.90.112` 均执行：

```bash
cd /root/szz/codespace/hpc-redis

make -C src vemb_v16_server
make -C benchmark vemb_v16_bench
make -C benchmark vemb_v16_topology_ctl
```

### 2. 准备 node0 manifest

在 `192.168.90.111` 上：

```bash
cat >/tmp/v16_node0.yaml <<'YAML'
local_ub_node_id: 0
local_region_weight: 4

remote_meta_provider: ub
remote_meta_path: /dev/obmm_shmdev1
remote_meta_mmap_offset: 268435456
remote_meta_entries: 8192
remote_meta_buckets: 16384
ub_rpc_timeout_ms: 200

warm_regions:
  - region_id: 100
    provider: ub
    path: /dev/obmm_shmdev1
    mmap_offset: 0
    bytes: 67108864
    value_size: 64
    home_ub_node_id: 0
    weight: 1
  - region_id: 101
    provider: ub
    path: /dev/obmm_shmdev5
    mmap_offset: 0
    bytes: 67108864
    value_size: 64
    home_ub_node_id: 1
    weight: 1

remote_meta_views:
  - owner_id: 1
    provider: ub
    path: /dev/obmm_shmdev5
    mmap_offset: 268435456
    entries: 8192
    buckets: 16384

ub_rpc_peers:
  - owner_id: 1
    provider: ub
    request_path: /dev/obmm_shmdev2
    request_mmap_offset: 8388608
    response_path: /dev/obmm_shmdev8
    response_mmap_offset: 16777216
    inbound_request_path: /dev/obmm_shmdev6
    inbound_request_mmap_offset: 8388608
    outbound_response_path: /dev/obmm_shmdev4
    outbound_response_mmap_offset: 16777216
YAML
```

### 3. 准备 node1 manifest

在 `192.168.90.112` 上：

```bash
cat >/tmp/v16_node1.yaml <<'YAML'
local_ub_node_id: 1
local_region_weight: 4

remote_meta_provider: ub
remote_meta_path: /dev/obmm_shmdev1
remote_meta_mmap_offset: 268435456
remote_meta_entries: 8192
remote_meta_buckets: 16384
ub_rpc_timeout_ms: 200

warm_regions:
  - region_id: 101
    provider: ub
    path: /dev/obmm_shmdev1
    mmap_offset: 0
    bytes: 67108864
    value_size: 64
    home_ub_node_id: 1
    weight: 1
  - region_id: 100
    provider: ub
    path: /dev/obmm_shmdev5
    mmap_offset: 0
    bytes: 67108864
    value_size: 64
    home_ub_node_id: 0
    weight: 1

remote_meta_views:
  - owner_id: 0
    provider: ub
    path: /dev/obmm_shmdev5
    mmap_offset: 268435456
    entries: 8192
    buckets: 16384

ub_rpc_peers:
  - owner_id: 0
    provider: ub
    request_path: /dev/obmm_shmdev2
    request_mmap_offset: 8388608
    response_path: /dev/obmm_shmdev8
    response_mmap_offset: 16777216
    inbound_request_path: /dev/obmm_shmdev6
    inbound_request_mmap_offset: 8388608
    outbound_response_path: /dev/obmm_shmdev4
    outbound_response_mmap_offset: 16777216
YAML
```

### 4. 启动 node0 / node1

先清理旧进程，两台机器均执行：

```bash
pids=$(pgrep -f "[v]emb_v16_server|[v]emb_v16_topology_ctl|[v]emb_v16_bench" || true)
[ -n "$pids" ] && kill $pids 2>/dev/null || true
```

在 `192.168.90.111` 上启动 `node0`：

```bash
cd /root/szz/codespace/hpc-redis

./src/vemb_v16_server \
  --transport tcp \
  --tcp-host 192.168.90.111 \
  --tcp-port 6391 \
  --proxy-io-threads 1 \
  --supernode-workers 1 \
  --warm-regions-manifest /tmp/v16_node0.yaml \
  --reset-warm-regions \
  --dim 16 \
  --max-vectors 8192 \
  --loglevel notice \
  >/tmp/v16_node0.log 2>&1 &
```

在 `192.168.90.112` 上启动 `node1`：

```bash
cd /root/szz/codespace/hpc-redis

./src/vemb_v16_server \
  --transport tcp \
  --tcp-host 192.168.90.112 \
  --tcp-port 6391 \
  --proxy-io-threads 1 \
  --supernode-workers 1 \
  --warm-regions-manifest /tmp/v16_node1.yaml \
  --dim 16 \
  --max-vectors 8192 \
  --loglevel notice \
  >/tmp/v16_node1.log 2>&1 &
```

### 5. 发布初始 topology：`active={0}`

在 `192.168.90.111` 上：

```bash
cd /root/szz/codespace/hpc-redis

./benchmark/vemb_v16_topology_ctl \
  --set \
  --transport tcp \
  --host 192.168.90.111 \
  --port 6391 \
  --epoch 1 \
  --min-write-epoch 1 \
  --active 0 \
  --standby 0 \
  --owner-endpoints 0=192.168.90.111:6391 \
  --timeout-ms 5000
```

校验返回应包含：

```text
status=0
active_owners=0
standby_owners=0
endpoint_count=1
endpoint[0]=owner:0 transport:tcp host:192.168.90.111 port:6391
```

### 6. prefill 业务数据

在 `192.168.90.111` 上：

```bash
./benchmark/vemb_v16_bench \
  --transport tcp \
  --endpoints 192.168.90.111:6391 \
  --dim 16 \
  --prefill 1024 \
  --ops 0 \
  --threads 1 \
  --pipeline 1 \
  --mode vadd \
  --timeout-ms 10000 \
  >/tmp/v16_prefill.out 2>&1

cat /tmp/v16_prefill.out
```

预期包含：

```text
[topology] epoch=1 ...
[prefill] inserted=1024
```

### 7. 启动持续写压

在 `192.168.90.111` 上：

```bash
nohup ./benchmark/vemb_v16_bench \
  --transport tcp \
  --endpoints 192.168.90.111:6391 \
  --dim 16 \
  --prefill 0 \
  --keyspace 1024 \
  --ops 50000 \
  --threads 2 \
  --pipeline 1 \
  --mode vadd \
  --timeout-ms 180000 \
  >/tmp/v16_live_write.out 2>&1 &
```

### 8. 启动 coordinator

在 `192.168.90.111` 上：

```bash
nohup ./benchmark/vemb_v16_topology_ctl \
  --coordinator-listen \
  --transport tcp \
  --host 192.168.90.111 \
  --port 7391 \
  --expected-sources 0 \
  --migration-epoch 23 \
  --cutover-epoch 24 \
  --standby 0,1 \
  --owner-endpoints 0=192.168.90.111:6391,1=192.168.90.112:6391 \
  --wait-ms 60000 \
  --timeout-ms 5000 \
  >/tmp/v16_coordinator.out 2>/tmp/v16_coordinator.err &
```

### 9. 发布 candidate topology：`{0} -> {0,1}`

先发给 `node1`：

```bash
./benchmark/vemb_v16_topology_ctl \
  --set \
  --transport tcp \
  --host 192.168.90.112 \
  --port 6391 \
  --epoch 23 \
  --min-write-epoch 23 \
  --active 0 \
  --standby 0,1 \
  --dual-write \
  --auto-scaleout \
  --coordinated-scaleout \
  --owner-endpoints 0=192.168.90.111:6391,1=192.168.90.112:6391 \
  --coordinator-endpoint 192.168.90.111:7391 \
  --timeout-ms 5000
```

再发给 `node0`：

```bash
./benchmark/vemb_v16_topology_ctl \
  --set \
  --transport tcp \
  --host 192.168.90.111 \
  --port 6391 \
  --epoch 23 \
  --min-write-epoch 23 \
  --active 0 \
  --standby 0,1 \
  --dual-write \
  --auto-scaleout \
  --coordinated-scaleout \
  --owner-endpoints 0=192.168.90.111:6391,1=192.168.90.112:6391 \
  --coordinator-endpoint 192.168.90.111:7391 \
  --timeout-ms 5000
```

### 10. 验证 cutover 完成

查看 coordinator 输出：

```bash
cat /tmp/v16_coordinator.out
```

预期包含：

```text
scaleout_all_sources_done=1
scaleout_full_active_published=2 errors=0 targets=2
```

查看 `node0` 日志：

```bash
grep -E "migration auto plan|scaleout auto done|local done" /tmp/v16_node0.log
```

预期包含：

```text
vemb_v16 migration auto plan: local_owner=0 ...
vemb_v16 scaleout auto done: local_owner=0 migration_epoch=23 cutover_epoch=24
```

最终两边 topology 都应为：

```bash
./benchmark/vemb_v16_topology_ctl \
  --get \
  --transport tcp \
  --host 192.168.90.111 \
  --port 6391 \
  --timeout-ms 5000

./benchmark/vemb_v16_topology_ctl \
  --get \
  --transport tcp \
  --host 192.168.90.112 \
  --port 6391 \
  --timeout-ms 5000
```

预期包含：

```text
current_topology_epoch=24
min_write_epoch=24
active_owners=0,1
standby_owners=0,1
endpoint_count=2
endpoint[0]=owner:0 transport:tcp host:192.168.90.111 port:6391
endpoint[1]=owner:1 transport:tcp host:192.168.90.112 port:6391
```

### 11. cutover 后再跑一次 bench

在 `192.168.90.111` 上：

```bash
./benchmark/vemb_v16_bench \
  --transport tcp \
  --endpoints 192.168.90.111:6391,192.168.90.112:6391 \
  --dim 16 \
  --prefill 0 \
  --keyspace 2000 \
  --ops 2000 \
  --threads 2 \
  --pipeline 1 \
  --mode vadd \
  --timeout-ms 10000 \
  >/tmp/v16_post_cutover.out 2>&1

cat /tmp/v16_post_cutover.out
```

预期：

- `fail=0`
- 输出中能看到 `stats node=1`，表示 node1 已接管部分写流量
- 不要省略 `--prefill 0`；bench 默认会先 prefill `65536` 个 key，和 `--max-vectors 8192` 组合时会因为容量不匹配而返回 `status=2`

### 12. cutover 后读验证

在 `192.168.90.111` 上：

```bash
./benchmark/vemb_v16_bench \
  --transport tcp \
  --endpoints 192.168.90.111:6391,192.168.90.112:6391 \
  --dim 16 \
  --prefill 0 \
  --keyspace 2000 \
  --ops 2000 \
  --threads 2 \
  --pipeline 1 \
  --mode vemb-supernode-read \
  --timeout-ms 10000 \
  >/tmp/v16_post_read.out 2>&1

cat /tmp/v16_post_read.out
```

预期：

- `fail=0`
- 输出中能看到 `stats node=0` 与 `stats node=1`

### 13. 一键脚本

仓库内提供了可从 `node0` 直接发起的完整脚本。

当前推荐配置策略：

- 启动阶段使用 `manifest`
- 扩容阶段使用 `topology_ctl --apply-peer-view-map` 刷新增 peer-view 配置

也就是：

- `node0` 先按旧 manifest 启动
- `node1` 按自己的 startup manifest 启动
- 再由 `node0` 用 `topology_ctl` 刷入 owner1 的本机视角 UB 映射
- attach 成功后再发布 candidate topology

执行：

```bash
cd /root/szz/codespace/hpc-redis
chmod +x benchmark/vemb_v16_scaleout_real_2node.sh
./benchmark/vemb_v16_scaleout_real_2node.sh
```

可选环境变量：

- `RUN_LIVE_WRITE=1`：迁移期间开启持续写压
- `LIVE_MODE=mixed-80r20w`：迁移期间同时持续查询和写入
- `LIVE_THREADS=2`：live workload 线程数
- `LIVE_TIMEOUT_MS=180000`：live workload 超时时间
- `RUN_POST_READ=0`：跳过 cutover 后读验证
- `MAX_VECTORS=65536`：提升容量，便于更大 keyspace 压测
- `PREFILL_KEYS=2048`：调整初始灌数
- `NODE0_HOST` / `NODE1_HOST` / `REMOTE_DIR` / `SSH_USER`：覆盖默认双机环境

### 14. 常用排查命令

```bash
tail -200 /tmp/v16_node0.log
tail -200 /tmp/v16_node1.log
cat /tmp/v16_prefill.out
cat /tmp/v16_live_write.out
cat /tmp/v16_post_cutover.out
cat /tmp/v16_post_read.out
cat /tmp/v16_coordinator.out
cat /tmp/v16_coordinator.err
```

### 15. 2026-07-08 远端验证结果

基于上面的两机环境，按当前仓库脚本和 bench 代码在远端复测：

#### 15.1 基本扩容流程

执行：

```bash
cd /root/szz/codespace/hpc-redis
./benchmark/vemb_v16_scaleout_real_2node.sh
```

结果：

```text
scaleout_all_sources_done=1
scaleout_full_active_published=2 errors=0 targets=2
current_topology_epoch=24
min_write_epoch=24
active_owners=0,1
standby_owners=0,1
```

post-cutover 校验：

```text
[done] mode=vadd threads=2 ok=4000 fail=0
[done] mode=vemb-inline threads=2 ok=4000 fail=0
```

说明当前单 source `node0 -> node0,node1` 扩容主流程已经可以稳定完成：

- source 自动生成 migration plan
- coordinator 收到 `local done`
- full active topology 成功发布
- cutover 后 node1 可以接管部分写流量和读流量
- 新增 owner1 的 UB 可见性通过 `--apply-peer-view-map` 在 scaleout 前完成刷新

#### 15.2 扩容期间持续写入和查询

执行：

```bash
cd /root/szz/codespace/hpc-redis
RUN_LIVE_WRITE=1 \
LIVE_MODE=mixed-80r20w \
LIVE_THREADS=2 \
LIVE_TIMEOUT_MS=180000 \
PREFILL_KEYS=4000 \
LIVE_WRITE_OPS=50000 \
./benchmark/vemb_v16_scaleout_real_2node.sh
```

结果：

```text
[done] mode=mixed-80r20w threads=2 ok=100000 fail=0
[client-topology] dual_write_sent=0 stale_refreshes=0
```

本轮验证覆盖了：

- 扩容前已有 4000 条存量数据
- candidate topology 发布后 workload 持续运行
- cutover 过程中同时有读和写
- workload 在 full active 发布后继续跑完，不依赖人工停压

#### 15.3 本轮踩坑与修正

这次真实双机验证里，主要补了三类问题：

1. bench read/write 的 client-topology TCP 容错：
   - 读路径补齐 `MOVED/ASK/STALE` refresh/retry
   - 读写路径都补齐 send/recv 失败后的 channel reconnect
   - topology refresh 后重建 client TCP channels，避免继续复用 cutover 前 channel

2. bench 进程级稳定性：
   - `benchmark/vemb_v16_bench` 现在忽略 `SIGPIPE`
   - 避免 cutover/reconnect 时对端先关连接，bench 因写死连接被信号直接打死

3. 真实两机脚本校验闭环：
   - live workload 不再只是在后台启动，而是会等待结束并检查退出码
   - live timeout 参数化为 `LIVE_TIMEOUT_MS`
   - coordinator `--expected-sources` 改为 owner 列表语义下的 `0`

其中第 3 点非常重要：如果脚本不等待 live workload，只看到扩容主流程结束，
会出现“cutover 成功但后台压测其实已经失败退出”的假阳性。

## 手动控制命令

手动命令主要用于 debug：

```text
--set / --get:
  发布或读取 topology，包括 active/standby、flags、owner endpoints、
  coordinator endpoint。

--range-barrier:
  对 (migration_epoch,target_owner,shard_id) 做 checkpoint barrier。

--range-wait-ready:
  循环 range-barrier，直到 response.range_ready=1。

--range-cutover:
  对 range 执行 final fence + keyed target barrier + lease commit + source CUTOVER。
  自动按 page_limit 分页。

--range-wait-cutover:
  先 wait-ready，再循环 cutover 到 range_done。

--range-source-gc:
  对 CUTOVER key 分页推进 SOURCE_GC。

--coordinator-listen:
  作为本轮扩容 coordinator，监听 SCALEOUT_LOCAL_DONE callback，
  收齐 expected sources 后发布 full active topology。
```

## 代码阅读路线

建议按这条路径读扩容代码：

```text
1. src/vemb_v16_protocol.h
   状态码、topology flags、endpoint、range control、local done callback wire struct。

2. src/vemb_v16_topology.c / src/vemb_v16_client_topology.c
   active/standby ring、owner endpoint discovery、client routing plan。

3. src/vemb_v16_storage.c
   topology_set、auto scaleout state machine、source auto plan、
   baseline retry、delta outbox drain、range barrier/cutover/source-gc。

4. src/vemb_v16_migration_outbox.c
   OPEN -> checkpoint -> FENCING -> CUTOVER_READY，
   append/ack/checkpoint/begin_final_fence/abort_final_fence。

5. src/vemb_v16_tlc.c
   key_migration_state、DEST_COMMITTED、CUTOVER/SOURCE_GC fence、
   key_version/tombstone 合并规则。

6. src/vemb_v16_ub_rpc.c
   BASELINE_PUT、DELTA_PUT、DELTA_DELETE、BARRIER_REQ、LEASE_COMMIT_REQ。

7. src/vemb_v16_proxy.c
   topology/range control 入口、scaleout notify worker。

8. benchmark/vemb_v16_topology_ctl.c
   发布 topology、range debug 命令、coordinator-listen。

9. benchmark/vemb_v16_bench.c
   client-topology、STALE/MOVED/ASK redirect retry。

10. benchmark/vemb_v16_scaleout_coordinated_server_smoke.sh
    当前最完整的 `{0,1} -> {0,1,2}` server smoke。
```

## 当前 P0 状态

已落地：

```text
topology active/standby + owner endpoint table。
AUTO_SCALEOUT source-local migrate plan。
source push baseline + retry queue。
VADD/VREM delta outbox。
MIGRATE_BASELINE_PUT / DELTA_PUT / DELTA_DELETE / BARRIER_REQ /
LEASE_COMMIT_REQ。
checkpoint barrier + final fence 短窗口。
target-side ASK gate + client redirect_owner retry。
per-range barrier/cutover/source-gc。
range 分页和自动拆 shard。
coordinated scaleout callback。
top_ctl 收齐多个 source 后发布 full active。
normal-path isolation fast path。
真实 server smoke: active {0,1} -> active {0,1,2}。
```

P0 仍可补强但不阻塞当前 normal-path 闭环：

```text
1. [done] 迁移期间持续业务写压 UT。
   在 migration_control_ut 中构造 MIGRATING range：
   - baseline 已 apply；
   - range checkpoint barrier 后继续对 source 执行 VADD/VREM；
   - 断言 source 写返回 OK，不是 ASK；
   - 断言 delta_seq 继续增长，target 最终 apply 到最新 key_version/tombstone；
   - range cutover 后 source 返回 MOVED，target lease 后允许 ASK redirect 写。

2. [done] 迁移期间持续业务写压 server smoke。
   新增 vemb_v16_scaleout_coordinated_live_write_smoke.sh 真实 server smoke：
   - 启动 node0/node1/node2，初始 active={0,1}；
   - prefill 后启动后台 benchmark live-write workload；
   - 在 workload 运行中发布 candidate active={0,1}, standby={0,1,2}；
   - 等 top_ctl coordinator 收齐 source0/source1 done 并发布 full active；
   - 等后台 workload 结束；
   - 断言 bench fail=0、source 完成迁移、node2 cutover 后可接 direct VADD。
   当前默认 live mode 为 vadd，VEMB_V16_SCALEOUT_LIVE_MODE 可配置；
   为避免 live workload 重复 prefill，vemb_v16_bench 已增加 --keyspace N，
   将“已有 key 空间”和“本次是否 prefill”两个语义拆开。

proxy callback 失败重试的独立 UT。
normal_path_fast_count / migration_slow_path_count / source_fence_slow_path_count
观测 counter。
active_count 从 0->1->0 后恢复 fast path 的回归测试。
正式 Redis CLI/SDK 的 endpoint discovery / ASK/MOVED 集成。
request_id/client_id 去重表。
```

P1：

```text
outbox WAL 或可靠队列。
target applied_seq 持久化。
owner lease / topology publish 状态持久化。
top_ctl coordinator crash recovery。
COLD 层迁移和 SOURCE_GC 生产安全水位。
连接池替换、owner 移除、endpoint 变更和延迟关闭策略。
```

## 结论

当前扩容主线是 server 内自动化状态机，而不是外部 migration tool 逐 key 驱动。
外部只需要发布 candidate topology；source SuperNode 自动计算迁出 key，使用
baseline + delta outbox 追平 target，通过 checkpoint barrier 降低写停顿，再在
final fence 短窗口内完成 target lease commit。多 source 场景由 proxy 回调
top_ctl 汇聚 local done，top_ctl 统一发布 full active topology，保证 global
cutover 不被单个 source 提前触发。
