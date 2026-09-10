# VEMB V16 Scaleout Test Host

本文记录基于 [docs/VEMB_V16_SCALE_OUT_NODE_MIGRATION_DESIGN.md](/Users/szza/codespace/work/hpc-redis/docs/VEMB_V16_SCALE_OUT_NODE_MIGRATION_DESIGN.md)
执行 `benchmark/vemb_v16_scaleout_real_2node.sh` 时使用的实际测试机器、代码路径、
设备前提、脚本配置和结果，便于后续复现远端扩容验证。

当前真实双机的 UB 映射关系已经确认应为：

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

即第一组按 `1..4 -> 5..8` 偏移 4，第二组按 `9..12 -> 13..16` 偏移 4。
扩容脚本当前使用第一组的四条方向；HA Replica 回归使用 `111:4 -> 112:8`
和 `112:9 -> 111:13`，不要将两组编号混用。

## 测试机器

- `node0`: `192.168.90.111`
- `node1`: `192.168.90.112`
- `SSH_USER`: `root`
- 两台机器均可通过 `ssh -p 22 root@<host>` 访问

## 远端代码路径

- 两台机器代码目录一致：
  `REMOTE_DIR=/root/szz/codespace/hpc-redis`

## 设备与运行前提

两台机器都需要具备以下 UB/共享内存设备：

- `/dev/obmm_shmdev1`
- `/dev/obmm_shmdev2`
- `/dev/obmm_shmdev3`
- `/dev/obmm_shmdev4`
- `/dev/obmm_shmdev5`
- `/dev/obmm_shmdev6`
- `/dev/obmm_shmdev7`
- `/dev/obmm_shmdev8`

脚本启动前已验证：

```bash
ssh -p 22 root@192.168.90.111 'test -d /root/szz/codespace/hpc-redis && echo repo_ok'
ssh -p 22 root@192.168.90.112 'test -d /root/szz/codespace/hpc-redis && echo repo_ok'

ssh -p 22 root@192.168.90.111 'ls /dev/obmm_shmdev1 /dev/obmm_shmdev2 /dev/obmm_shmdev3 /dev/obmm_shmdev4 /dev/obmm_shmdev5 /dev/obmm_shmdev6 /dev/obmm_shmdev7 /dev/obmm_shmdev8'
ssh -p 22 root@192.168.90.112 'ls /dev/obmm_shmdev1 /dev/obmm_shmdev2 /dev/obmm_shmdev3 /dev/obmm_shmdev4 /dev/obmm_shmdev5 /dev/obmm_shmdev6 /dev/obmm_shmdev7 /dev/obmm_shmdev8'
```

还需要保证扩容测试开始前，这组 UB RPC path 没有被其他现场占用：

- `/dev/obmm_shmdev2`
- `/dev/obmm_shmdev4`
- `/dev/obmm_shmdev6`
- `/dev/obmm_shmdev8`

建议在两台机器上执行：

```bash
ssh -p 22 root@192.168.90.111 'lsof /dev/obmm_shmdev2 /dev/obmm_shmdev4 /dev/obmm_shmdev6 /dev/obmm_shmdev8 2>/dev/null || true'
ssh -p 22 root@192.168.90.112 'lsof /dev/obmm_shmdev2 /dev/obmm_shmdev4 /dev/obmm_shmdev6 /dev/obmm_shmdev8 2>/dev/null || true'
```

若存在其他业务进程占用这四个设备，必须先清场再跑扩容；否则会出现：

- UB RPC ring 数据被其他现场覆盖
- baseline timeout / duplicate response
- scaleout 卡在 draining 或 cutover 前

## 脚本默认机器配置

`benchmark/vemb_v16_scaleout_real_2node.sh` 中当前使用的机器与目录默认值：

```bash
NODE0_HOST=192.168.90.111
NODE1_HOST=192.168.90.112
SSH_USER=root
REMOTE_DIR=/root/szz/codespace/hpc-redis
```

## 本次测试使用的扩容配置

### 网络与 epoch

```bash
SERVER_PORT=6391
COORD_PORT=7391
MIGRATION_EPOCH=23
CUTOVER_EPOCH=24
```

### 数据与运行参数

```bash
DIM=16
MAX_VECTORS=8192
PREFILL_KEYS=1024
POST_KEYSPACE=2000
POST_OPS=2000
POST_THREADS=2
LIVE_WRITE_OPS=50000
LIVE_MODE=vadd
LIVE_THREADS=2
LIVE_TIMEOUT_MS=180000
RUN_LIVE_WRITE=1
RUN_POST_READ=1
VERIFY_MIGRATED_DATA=1
VERIFY_SAMPLE_COUNT=8
RESET_NODE0=1
RESET_NODE1=1
```

## 执行命令

在本地工作目录 `/Users/szza/codespace/work/hpc-redis` 下执行：

```bash
RUN_LIVE_WRITE=1 RUN_POST_READ=1 VERIFY_MIGRATED_DATA=1 VERIFY_SAMPLE_COUNT=8 \
bash ./benchmark/vemb_v16_scaleout_real_2node.sh
```

默认同时覆盖两个场景：

- 带预写入历史数据的扩容
- 扩容窗口内持续 live write 的扩容

## 预期执行流程

脚本会在远端执行以下关键步骤：

1. 在两台机器上编译：
   - `src/vemb_v16_server`
   - `benchmark/vemb_v16_bench`
   - `benchmark/vemb_v16_topology_ctl`
   - `benchmark/vemb_v16_read_verify`
2. 写入 node0/node1 启动 manifest
3. 启动两台 `vemb_v16_server`
4. 由 node0 写入并应用 peer-view map：
5. 由 node0 发布初始 topology：
   - `active={0}`
   - `standby={0}`
6. 预写入 `1024` 个 key
7. 启动 coordinator listener
8. 发布 candidate topology：
   - `epoch=23`
   - `active={0}`
   - `standby={0,1}`
   - flags:
     `DUAL_WRITE_REQUIRED | AUTO_SCALEOUT | COORDINATED_SCALEOUT`
   - node1 仍使用普通 `--set`
   - node0 使用组合命令：
     `./benchmark/vemb_v16_topology_ctl --set-with-peer-view-map /tmp/v16_node0_peer_map.yaml ...`
9. 校验 node0 runtime attach 日志
10. 等待 source local done 与 full active publish
11. 校验最终 topology：
    - `epoch=24`
    - `active={0,1}`
    - `standby={0,1}`
12. 在 node1 上校验迁移后的旧数据
13. 做 cutover 后写验证与读验证

## 本次测试结果摘要

2026-07-09 实测通过，关键结果如下：

- node0 使用 `--set-with-peer-view-map` 返回成功：
  - `status=0`
  - `peer_view_map_status=0`
  - `topology_status=0`
- node0 runtime attach 三类资源全部成功：
  - warm region
  - remote meta owner view
  - UB RPC peer
- source 自动迁移统计：
  `marked=627 skipped=397`
- coordinator 收齐 local done 后成功发布 full active topology
- 最终 topology 为：
  `epoch=24 active={0,1}`
- 迁移旧数据校验通过：
  `verified=8/627 migrated keys on owner=1`
- cutover 后写验证通过：
  `4000/4000 ok`
- cutover 后读验证通过：
  `4000/4000 ok`

## 常用排查命令

```bash
ssh root@192.168.90.111 'tail -200 /tmp/v16_node0_scaleout.log'
ssh root@192.168.90.112 'tail -200 /tmp/v16_node1_scaleout.log'
ssh root@192.168.90.111 'cat /tmp/v16_coordinator_scaleout.out'
```
