# HPC-Redis 性能回归测试指导

---

## 1. 目的

在代码变更后，用固定的脚本和参数验证:
1. 功能路径可跑通（server 起、prefill 成、数据面无 0-ops 空转）
2. 输出指标完整（TSV 列对齐、core_ut/core_st/si/rss 有合理值）
3. 吞吐/延迟相对基线无异常回退

## 2. 环境前置

### 2.1 节点与路径
| 节点 | 角色 | 代码路径 |
|---|---|---|
| HW01 (192.168.90.111) | server / 大部分脚本执行点 | /root/gqs/codespace/UnifiedBus/hpc-redis |
| HW02 (192.168.90.112) | client (memtier) / 双机 server | 同上 |
| HW04(192.168.90.21)/HW05(192.168.90.20) | 4 节点 cluster 用 (脚本 4/7) | 同上 |

### 2.2 编译与 stamp
```bash
# 三件套 (两台都要)
make -C src -j$(nproc) redis-server
make -C clients/c -j$(nproc)
make -C memtier_benchmark -j$(nproc)
# stamp (脚本 8/13 的 BUILD=verify 依赖; HW01 写 server+client, HW02 同)
bash scripts/vemb_v16_build_stamp.sh write server
bash scripts/vemb_v16_build_stamp.sh write client
```

### 2.3 UB 环境
- 跨节点/scaleout 类脚本依赖 /dev/obmm_shmdev* 已安装 (ubme_tool)
- **只读检查**: `dd if=/dev/obmm_shmdevN of=/dev/null bs=4096 count=1` 应能 open
  (open 报 EPERM = 该设备模式与使用方不匹配, 见 6.1 已知问题)

## 3. 执行纪律 (必须遵守)

1. **跑前空闲检查** (两台): 无 redis-server/memtier/perf/bw_mem 进程 且 idle≥70%。
   有人用 → 每 30s 重查, 空闲才跑。
2. **UB 测试严格串行**: 上一条完全退出 (server 停、shmdev 释放) 再起下一条。
3. **跑后清理**: 每条结束确认无残留进程 (脚本自身 pkill 兜底 + 人工复查)。
   服务器残留会污染下一条的负载门禁和对照。
4. **数值合理性校验** (不只是列对齐):
   - ops 高时 core_ut/st 不可能为 0
   - core_ut + core_st ≈ cores
   - 跨节点场景 si 应显著非零
5. 全部脚本在 **HW01 上执行** (跨节点类由脚本 ssh 到对端)。
6. hpc 相关的脚本注意绑核数>pio+snw

## 4. 测试项

### 单实例本地回环对比

#### 1. 单实例 baseline(标准 redis) VS 单实例 HPC TCP

```
# VEMB 读
  OP_TYPE=VEMB bash benchmark/run_vemb_local_loopback_sweep.sh

# VSIM 相似度
  OP_TYPE=VSIM_2KEY bash benchmark/run_vemb_local_loopback_sweep.sh

# VADD 写入
  OP_TYPE=VADD bash benchmark/run_vemb_local_loopback_sweep.sh

# VREM 删除
  OP_TYPE=VREM bash benchmark/run_vemb_local_loopback_sweep.sh
```

#### 2. 单实例 hpc aeron
```
# OP_TYPE 支持 VEMB(默认)/VSIM_2KEY/VADD/VREM (VADD 免 prefill, 非 VEMB 每档重启 server)
# 对齐核数口径用 WORKERS=pio:snw SERVER_MASK=... 覆盖默认 8:8 / 0-47
  RUN_LOCAL=1 OP_TYPE=VEMB bash scripts/run_aeron_best.sh
  RUN_LOCAL=1 OP_TYPE=VSIM_2KEY bash scripts/run_aeron_best.sh
```

对每个 OP_TYPE 输出单实例 baseline VS HPC TCP 汇总表，表头包含：T, C, P, 原生 ops/core,	hpc ops/core,	加速比
对于 baseline VS hpc aeron  汇总表，表头包含：T, C, P, 原生 ops/core,	hpc ops/core,	加速比

#### 3. 单实例 baseline DIM=
#### 2. 单实例跨节点 sweep (SERVER=HW01, CLIENT=HW02 memtier)
```
# VEMB 读
  OP_TYPE=VEMB bash benchmark/run_vemb_cross_node_sweep.sh

# VSIM 相似度
  OP_TYPE=VSIM_2KEY bash benchmark/run_vemb_cross_node_sweep.sh

# VADD 写入
  OP_TYPE=VADD bash benchmark/run_vemb_cross_node_sweep.sh

# VREM 删除
  OP_TYPE=VREM bash benchmark/run_vemb_cross_node_sweep.sh
```

对每个 OP_TYPE 输出汇总表，表头包含：T, C, P, 原生 ops/core,	hpc ops/core,	加速比

#### 3. 多实例 baseline 和 多实例hpc 对比
```
# 集成 server 绑 NUMA0；memtier 绑 NUMA1

  # (1) 本地回环 DIM=300 hpc 17 档

  SERVERS_ONLY=hpc OP_TYPE=VEMB HPC_PIO=21 HPC_SNW=21 \
    HPC_SERVER_CPUSET=0-47 DIM=300 TEST_TIME=30 \
    bash benchmark/run_vemb_local_loopback_sweep.sh

  # (2) 本地回环 baseline 12 实例 17 档

  LOCAL_BENCH=1 RAW=1 NUM_INSTANCES=12 IO_THREADS=4 \
    DIM=300 bash run_multi_instance_redis_baseline.sh

  # (3) 跨节点 DIM=300 hpc
  SERVERS_ONLY=hpc OP_TYPE=VEMB HPC_PIO=21 HPC_SNW=21 \
    HPC_SERVER_CPUSET=0-47 DIM=300 TEST_TIME=30 \
    bash benchmark/run_vemb_cross_node_sweep.sh

  # (4) 跨节点 DIM=300 baseline 12 实例

  RAW=1 NUM_INSTANCES=12 IO_THREADS=4 \
    DIM=300 NIC_IFACE=eth4 bash run_multi_instance_redis_baseline.sh

  # (5) 跨节点 DIM=8 hpc 17 档

  SERVERS_ONLY=hpc OP_TYPE=VEMB HPC_PIO=21 HPC_SNW=21 \
    HPC_SERVER_CPUSET=0-47 DIM=8 TEST_TIME=30 \
    bash benchmark/run_vemb_cross_node_sweep.sh

  # (6) 跨节点 DIM=8 baseline 12 实例
  RAW=1 NUM_INSTANCES=12 IO_THREADS=4 \
  DIM=8 NIC_IFACE=eth4 bash run_multi_instance_redis_baseline.sh
```
对比 （1）（2） 的数据，输出汇总表，表头包含：T, C, P, 原生 ops/core,	hpc ops/core,	加速比
对比 （3）（4） 的数据，输出汇总表，表头包含：T, C, P, 原生 ops/core,	hpc ops/core,	加速比
对比 （5）（6） 的数据，输出汇总表，表头包含：T, C, P, 原生 ops/core,	hpc ops/core,	加速比

#### 4. Redis 集群场景对比
```
# 原生 Redis cluster 
bash benchmark/run_redis_cluster_vemb.sh

# 集成Redis TCP cluster
bash run_cluster_2node_sweep.sh

# 集成Redis Aeron 模式 local
RUN_LOCAL=1 bash scripts/run_aeron_best.sh

# 集成Redis Aeron 模式 crossnode
# (OP_TYPE=VEMB/VSIM_2KEY/VADD/VREM; warm manifest 用 xnode_aeron 系列, warm 区在 shmdev4)
SERVER_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis \
CLIENT_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis \
BUILD=verify PROFILE=0 \
PIO=6 SNW=6 \
SERVER_MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_xnode_aeron.yaml \
bash scripts/run_aeron_cross_node_flamegraph.sh

# 集成Redis Aeron 双节点 (默认 9 档; SWEEP=0 TS="64" CS="4" PS="32" 回单档;
# OP_TYPE=VSIM_2KEY 覆盖 t8 报告的 HPC Aeron 行)
REMOTE_DIR=/root/gqs/codespace/UnifiedBus/hpc-redis \
    TEST_TIME=30 \
    bash benchmark/vemb_v16_aeron_cluster_tput.sh
```
输出汇总表，表头包含：t, c, pipeline,	原生 ops/sec/core,	hpc TCP cluster ops/sec/core,	TCP/原生,	Aeron ops/sec/core,	Aeron/原生

#### 5. 扩容场景下 VEMB 性能

```
# hpc-redis扩容全过程

bash benchmark/hpc_redis_scaleout_throughput.sh

# 原生redis扩容全过程

N_INIT=3 N_FINAL=4 RAW=1 TEST_TIME=60 \
    CLIENTS=200 THREADS=16 IO_THREADS=4 \
    VECTORS_PER_VSET=6250 \
    bash benchmark/run_redis_cluster_scaleout_vemb.sh
```

#### 6. 混合读与写/删操作
```
bash benchmark/hpc_redis_mixed_workload.sh
```

#### 7. 本地远端UB 7:1拓扑读吞吐对比
```
# 4 组实验 = 2 拓扑 × 2 transport，每组先 TCP prefill 再 bench

# 拓扑名对应 examples/vemb_v16_warm_regions_<name>.yaml

bash bench_aeron_3scenarios.sh pure_local_100k tcp    100000 30

bash bench_aeron_3scenarios.sh pure_local_100k aeron  100000 30

bash bench_aeron_3scenarios.sh small_7to1_100k tcp    100000 30

bash bench_aeron_3scenarios.sh small_7to1_100k aeron  100000 30
```

#### 8. VSIM_2KEY 双节点性能对比
```
TEST_TIME=30 NUM_KEYS=100000 bash benchmark/run_vsim_2key_2node.sh
```
对每个 OP_TYPE 输出汇总表，表头包含：T, C, P, 原生 ops/core,	hpc ops/core,	加速比

## 7. 结果归档

- 每次回归建目录: `benchmark/results/regression/<date>_<branch>/`
- 存: 各脚本 TSV 副本 + 脚本 13 的 RUN_ID 列表 + 本机 git commit + 环境快照 (UB 设备列表)
- 对比上一版基线, 记录差异 >5% 的项及归因
