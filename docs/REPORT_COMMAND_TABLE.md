# 全量数据采集命令表（对应 docs/REPORT_SKELETON.md）

> 版本: v0.2 (20260826)
> 约定: 未注明均在 **HW01** (`/root/gqs/codespace/UnifiedBus/hpc-redis`) 上执行；跨节点/双机类脚本由脚本自身 ssh 到对端。

## 0. 前置（每次回归一次性）【HW01 与 HW02 各执行一次】

```bash
cd /root/gqs/codespace/UnifiedBus/hpc-redis
make -C src -j$(nproc) redis-server && make -C clients/c -j$(nproc) && make -C memtier_benchmark -j$(nproc)
bash scripts/vemb_v16_build_stamp.sh write server
bash scripts/vemb_v16_build_stamp.sh write client
```

- baseline 默认 NOQUANT（`BASELINE_NOQUANT=0` 关闭）；baseline RDB 预填充自动加载（`BASELINE_RDB=0` 强制 prefill）。
- UB 用例严格串行；跑前两台空闲检查（无 redis-server/memtier/perf 且 idle≥70%）。

---

> 执行位置: 除注明"在本地 Mac 执行"外均在 HW01。以下三类脚本在**本地 Mac** 执行（管理面走 frpc 转发口，数据面走 111/112 内网）: `scripts/run_aeron_cross_node_flamegraph.sh`、`benchmark/vemb_v16_aeron_cluster_tput.sh`、`benchmark/vemb_v16_scaleout_ub_cluster_111_to_112.sh`。

## 1. 3.2.1 单实例性能

### 1.1 本地回环 VEMB / VSIM_2KEY / VADD / VREM（9 档）

baseline【HW01 执行】（4 条）：

```bash
SERVERS_ONLY=baseline OP_TYPE=VEMB      bash benchmark/run_vemb_local_loopback_sweep.sh
SERVERS_ONLY=baseline OP_TYPE=VSIM_2KEY bash benchmark/run_vemb_local_loopback_sweep.sh
SERVERS_ONLY=baseline OP_TYPE=VADD      bash benchmark/run_vemb_local_loopback_sweep.sh
SERVERS_ONLY=baseline OP_TYPE=VREM      bash benchmark/run_vemb_local_loopback_sweep.sh
```

hpc TCP【HW01 执行】（4 条）：

```bash
SERVERS_ONLY=hpc HPC_PIO=2 HPC_SNW=2 OP_TYPE=VEMB      bash benchmark/run_vemb_local_loopback_sweep.sh
SERVERS_ONLY=hpc HPC_PIO=2 HPC_SNW=2 OP_TYPE=VSIM_2KEY bash benchmark/run_vemb_local_loopback_sweep.sh
SERVERS_ONLY=hpc HPC_PIO=2 HPC_SNW=2 OP_TYPE=VADD      bash benchmark/run_vemb_local_loopback_sweep.sh
SERVERS_ONLY=hpc HPC_PIO=2 HPC_SNW=2 OP_TYPE=VREM      bash benchmark/run_vemb_local_loopback_sweep.sh
```

hpc Aeron【HW01 执行，RUN_LOCAL=1】（4 条；VADD 免 prefill，非 VEMB 每档重启 server + prefill）：

```bash
RUN_LOCAL=1 WORKERS=2:2 TEST_TIME=20 OP_TYPE=VEMB      bash scripts/run_aeron_best.sh
RUN_LOCAL=1 WORKERS=2:2 TEST_TIME=20 OP_TYPE=VSIM_2KEY bash scripts/run_aeron_best.sh
RUN_LOCAL=1 WORKERS=2:2 TEST_TIME=20 OP_TYPE=VADD      bash scripts/run_aeron_best.sh
RUN_LOCAL=1 WORKERS=2:2 TEST_TIME=20 OP_TYPE=VREM      bash scripts/run_aeron_best.sh
```

产物 TSV：TCP/baseline 在 `benchmark/results/<op小写>_loopback_sweep/<时间戳>/summary.tsv`；Aeron 在 `perf/aeron_sweep/<run-id>/summary.tsv`。

### 1.2 本地回环 64x1x32 高维（DIM=1024/2048/3072）

baseline【HW01 执行】：

```bash
SERVERS_ONLY=baseline OP_TYPE=VEMB DIM=1024 TS="64" CS="1" PS="32" MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim1024.yaml bash benchmark/run_vemb_local_loopback_sweep.sh
SERVERS_ONLY=baseline OP_TYPE=VEMB DIM=2048 TS="64" CS="1" PS="32" MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim2048.yaml bash benchmark/run_vemb_local_loopback_sweep.sh
SERVERS_ONLY=baseline OP_TYPE=VEMB DIM=3072 TS="64" CS="1" PS="32" MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim3072.yaml bash benchmark/run_vemb_local_loopback_sweep.sh
```

hpc TCP【HW01 执行】：

```bash
SERVERS_ONLY=hpc HPC_PIO=2 HPC_SNW=2 OP_TYPE=VEMB DIM=1024 TS="64" CS="1" PS="32" MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim1024.yaml bash benchmark/run_vemb_local_loopback_sweep.sh
SERVERS_ONLY=hpc HPC_PIO=2 HPC_SNW=2 OP_TYPE=VEMB DIM=2048 TS="64" CS="1" PS="32" MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim2048.yaml bash benchmark/run_vemb_local_loopback_sweep.sh
SERVERS_ONLY=hpc HPC_PIO=2 HPC_SNW=2 OP_TYPE=VEMB DIM=3072 TS="64" CS="1" PS="32" MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim3072.yaml bash benchmark/run_vemb_local_loopback_sweep.sh
```

hpc Aeron【HW01 执行，RUN_LOCAL=1】：

```bash
RUN_LOCAL=1 WORKERS=2:2 DIM=1024 TS=64 CS=1 PIPELINE=32 TEST_TIME=30 MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim1024.yaml bash scripts/run_aeron_best.sh
RUN_LOCAL=1 WORKERS=2:2 DIM=2048 TS=64 CS=1 PIPELINE=32 TEST_TIME=30 MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim2048.yaml bash scripts/run_aeron_best.sh
RUN_LOCAL=1 WORKERS=2:2 DIM=3072 TS=64 CS=1 PIPELINE=32 TEST_TIME=30 MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim3072.yaml bash scripts/run_aeron_best.sh
```

### 1.3 跨节点 VEMB / VSIM_2KEY

baseline【HW01 执行，脚本 ssh 到 HW02 起 memtier】：

```bash
SERVERS_ONLY=baseline OP_TYPE=VEMB bash benchmark/run_vemb_cross_node_sweep.sh
SERVERS_ONLY=baseline OP_TYPE=VSIM_2KEY bash benchmark/run_vemb_cross_node_sweep.sh
```

hpc TCP【HW01 执行，脚本 ssh 到 HW02 起 memtier】：

```bash
SERVERS_ONLY=hpc HPC_PIO=2 HPC_SNW=2 OP_TYPE=VEMB bash benchmark/run_vemb_cross_node_sweep.sh
SERVERS_ONLY=hpc HPC_PIO=2 HPC_SNW=2 OP_TYPE=VSIM_2KEY bash benchmark/run_vemb_cross_node_sweep.sh
```

hpc Aeron【本地 Mac 执行】（单次单档、外层 7 档循环）：

```bash
TS_l=( 1  1  4 16 64 64 64); CS_l=( 1  1  1  1  1  4 16); PS_l=( 1 32 32 32 32 32 32)
for OP in VEMB VSIM_2KEY; do
    for i in $(seq 0 6); do
        OP_TYPE=$OP THREADS=${TS_l[$i]} CLIENTS=${CS_l[$i]} PIPELINE=${PS_l[$i]} \
        PROFILE=0 TEST_TIME=30 PIO=2 SNW=2 \
        SERVER_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis \
        CLIENT_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis \
        SERVER_MANIFEST=/root/gqs/codespace/UnifiedBus/hpc-redis/examples/vemb_v16_warm_regions_111_xnode_aeron.yaml \
            bash scripts/run_aeron_cross_node_flamegraph.sh
    done
done
```

产物：TCP/baseline 在 `benchmark/results/<op小写>_cross_node_sweep/<时间戳>/summary.tsv`（含 nic_util_pct）；Aeron 每档在 `benchmark/results/.../<run-id>/client.workload.summary.tsv`（ops/avg/p50/p99）+ `server.cpu.cpuset.summary.tsv`（ut/st/si 由 %usr/%sys/%softirq 换算）。Aeron 数据面走 UB 不走网卡，报告 nic_util 列记 NA。

### 1.4 跨节点 64x1x32 高维（DIM=1024/2048/3072）

baseline【HW01 执行】：

```bash
SERVERS_ONLY=baseline OP_TYPE=VEMB DIM=1024 TS="64" CS="1" PS="32" MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim1024.yaml bash benchmark/run_vemb_cross_node_sweep.sh
SERVERS_ONLY=baseline OP_TYPE=VEMB DIM=2048 TS="64" CS="1" PS="32" MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim2048.yaml bash benchmark/run_vemb_cross_node_sweep.sh
SERVERS_ONLY=baseline OP_TYPE=VEMB DIM=3072 TS="64" CS="1" PS="32" MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim3072.yaml bash benchmark/run_vemb_cross_node_sweep.sh
```

hpc TCP【HW01 执行】：

```bash
SERVERS_ONLY=hpc HPC_PIO=2 HPC_SNW=2 OP_TYPE=VEMB DIM=1024 TS="64" CS="1" PS="32" MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim1024.yaml bash benchmark/run_vemb_cross_node_sweep.sh
SERVERS_ONLY=hpc HPC_PIO=2 HPC_SNW=2 OP_TYPE=VEMB DIM=2048 TS="64" CS="1" PS="32" MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim2048.yaml bash benchmark/run_vemb_cross_node_sweep.sh
SERVERS_ONLY=hpc HPC_PIO=2 HPC_SNW=2 OP_TYPE=VEMB DIM=3072 TS="64" CS="1" PS="32" MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim3072.yaml bash benchmark/run_vemb_cross_node_sweep.sh
```

hpc Aeron【本地 Mac 执行】：

```bash
OP_TYPE=VEMB DIM=1024 THREADS=64 CLIENTS=1 PIPELINE=32 PROFILE=0 TEST_TIME=30 PIO=2 SNW=2 SERVER_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis CLIENT_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis SERVER_MANIFEST=/root/gqs/codespace/UnifiedBus/hpc-redis/examples/vemb_v16_warm_regions_111_xnode_aeron_dim1024.yaml bash scripts/run_aeron_cross_node_flamegraph.sh
OP_TYPE=VEMB DIM=2048 THREADS=64 CLIENTS=1 PIPELINE=32 PROFILE=0 TEST_TIME=30 PIO=2 SNW=2 SERVER_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis CLIENT_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis SERVER_MANIFEST=/root/gqs/codespace/UnifiedBus/hpc-redis/examples/vemb_v16_warm_regions_111_xnode_aeron_dim2048.yaml bash scripts/run_aeron_cross_node_flamegraph.sh
OP_TYPE=VEMB DIM=3072 THREADS=64 CLIENTS=1 PIPELINE=32 PROFILE=0 TEST_TIME=30 PIO=2 SNW=2 SERVER_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis CLIENT_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis SERVER_MANIFEST=/root/gqs/codespace/UnifiedBus/hpc-redis/examples/vemb_v16_warm_regions_111_xnode_aeron_dim3072.yaml bash scripts/run_aeron_cross_node_flamegraph.sh
```

---

## 2. 3.2.2 同核数对比

### 2.1 本地回环 DIM=300（17:17 vs baseline 12 实例）【块内注释标注执行位置】

```bash
# hpc TCP 17:17【HW01 执行】
SERVERS_ONLY=hpc OP_TYPE=VEMB HPC_PIO=17 HPC_SNW=17 HPC_SERVER_CPUSET=0-47 DIM=300 TEST_TIME=30 bash benchmark/run_vemb_local_loopback_sweep.sh
# hpc Aeron 17:17【HW01 执行，RUN_LOCAL=1】（默认 9 档）
RUN_LOCAL=1 WORKERS=17:17 SERVER_MASK=0-47 TEST_TIME=30 bash scripts/run_aeron_best.sh
# baseline 12 实例【HW01 执行】
LOCAL_BENCH=1 RAW=1 NUM_INSTANCES=12 IO_THREADS=4 DIM=300 bash run_multi_instance_redis_baseline.sh
```

### 2.2 跨节点 DIM=300【块内注释标注执行位置】

```bash
# hpc TCP 17:17【HW01 执行】
SERVERS_ONLY=hpc OP_TYPE=VEMB HPC_PIO=17 HPC_SNW=17 HPC_SERVER_CPUSET=0-47 DIM=300 TEST_TIME=30 bash benchmark/run_vemb_cross_node_sweep.sh
# hpc Aeron 17:17【本地 Mac 执行】（7 档循环）
TS_l=( 1  1  4 16 64 64 64); CS_l=( 1  1  1  1  1  4 16); PS_l=( 1 32 32 32 32 32 32)
for i in $(seq 0 6); do
    THREADS=${TS_l[$i]} CLIENTS=${CS_l[$i]} PIPELINE=${PS_l[$i]} \
    PROFILE=0 TEST_TIME=30 PIO=17 SNW=17 \
    SERVER_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis \
    CLIENT_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis \
    SERVER_MANIFEST=/root/gqs/codespace/UnifiedBus/hpc-redis/examples/vemb_v16_warm_regions_111_xnode_aeron.yaml \
        bash scripts/run_aeron_cross_node_flamegraph.sh
done
# baseline 12 实例【HW01 执行】
RAW=1 NUM_INSTANCES=12 IO_THREADS=4 DIM=300 NIC_IFACE=eth4 bash run_multi_instance_redis_baseline.sh
```

### 2.3 跨节点 DIM=8【块内注释标注执行位置】

```bash
# hpc TCP 17:17【HW01 执行】（脚本自动切 dim8 manifest）
SERVERS_ONLY=hpc OP_TYPE=VEMB HPC_PIO=17 HPC_SNW=17 HPC_SERVER_CPUSET=0-47 DIM=8 TEST_TIME=30 bash benchmark/run_vemb_cross_node_sweep.sh
# hpc Aeron 17:17【本地 Mac 执行】（7 档循环）
TS_l=( 1  1  4 16 64 64 64); CS_l=( 1  1  1  1  1  4 16); PS_l=( 1 32 32 32 32 32 32)
for i in $(seq 0 6); do
    DIM=8 THREADS=${TS_l[$i]} CLIENTS=${CS_l[$i]} PIPELINE=${PS_l[$i]} \
    PROFILE=0 TEST_TIME=30 PIO=17 SNW=17 \
    SERVER_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis \
    CLIENT_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis \
    SERVER_MANIFEST=/root/gqs/codespace/UnifiedBus/hpc-redis/examples/vemb_v16_warm_regions_111_xnode_aeron_dim8.yaml \
        bash scripts/run_aeron_cross_node_flamegraph.sh
done
# baseline 12 实例【HW01 执行】
RAW=1 NUM_INSTANCES=12 IO_THREADS=4 DIM=8 NIC_IFACE=eth4 bash run_multi_instance_redis_baseline.sh
```

### 2.4 本地回环 64x1x32 高维（DIM=1024/2048/3072）【全部 HW01 执行】

```bash
# baseline 12 实例【HW01 执行】
LOCAL_BENCH=1 RAW=1 NUM_INSTANCES=12 IO_THREADS=4 DIM=1024 bash run_multi_instance_redis_baseline.sh
LOCAL_BENCH=1 RAW=1 NUM_INSTANCES=12 IO_THREADS=4 DIM=2048 bash run_multi_instance_redis_baseline.sh
LOCAL_BENCH=1 RAW=1 NUM_INSTANCES=12 IO_THREADS=4 DIM=3072 bash run_multi_instance_redis_baseline.sh
# hpc TCP 17:17【HW01 执行】
SERVERS_ONLY=hpc HPC_PIO=17 HPC_SNW=17 HPC_SERVER_CPUSET=0-47 OP_TYPE=VEMB DIM=1024 TS="64" CS="1" PS="32" MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim1024.yaml TEST_TIME=30 bash benchmark/run_vemb_local_loopback_sweep.sh
SERVERS_ONLY=hpc HPC_PIO=17 HPC_SNW=17 HPC_SERVER_CPUSET=0-47 OP_TYPE=VEMB DIM=2048 TS="64" CS="1" PS="32" MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim2048.yaml TEST_TIME=30 bash benchmark/run_vemb_local_loopback_sweep.sh
SERVERS_ONLY=hpc HPC_PIO=17 HPC_SNW=17 HPC_SERVER_CPUSET=0-47 OP_TYPE=VEMB DIM=3072 TS="64" CS="1" PS="32" MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim3072.yaml TEST_TIME=30 bash benchmark/run_vemb_local_loopback_sweep.sh
# hpc Aeron 17:17【HW01 执行，RUN_LOCAL=1】
RUN_LOCAL=1 WORKERS=17:17 SERVER_MASK=0-47 DIM=1024 TS=64 CS=1 PIPELINE=32 TEST_TIME=30 MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim1024.yaml bash scripts/run_aeron_best.sh
RUN_LOCAL=1 WORKERS=17:17 SERVER_MASK=0-47 DIM=2048 TS=64 CS=1 PIPELINE=32 TEST_TIME=30 MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim2048.yaml bash scripts/run_aeron_best.sh
RUN_LOCAL=1 WORKERS=17:17 SERVER_MASK=0-47 DIM=3072 TS=64 CS=1 PIPELINE=32 TEST_TIME=30 MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim3072.yaml bash scripts/run_aeron_best.sh
```

### 2.5 跨节点 64x1x32 高维（DIM=1024/2048/3072）【块内注释标注执行位置】

```bash
# baseline 12 实例【HW01 执行】
RAW=1 NUM_INSTANCES=12 IO_THREADS=4 DIM=1024 NIC_IFACE=eth4 bash run_multi_instance_redis_baseline.sh
RAW=1 NUM_INSTANCES=12 IO_THREADS=4 DIM=2048 NIC_IFACE=eth4 bash run_multi_instance_redis_baseline.sh
RAW=1 NUM_INSTANCES=12 IO_THREADS=4 DIM=3072 NIC_IFACE=eth4 bash run_multi_instance_redis_baseline.sh
# hpc TCP 17:17【HW01 执行】
SERVERS_ONLY=hpc HPC_PIO=17 HPC_SNW=17 HPC_SERVER_CPUSET=0-47 OP_TYPE=VEMB DIM=1024 TS="64" CS="1" PS="32" MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim1024.yaml TEST_TIME=30 bash benchmark/run_vemb_cross_node_sweep.sh
SERVERS_ONLY=hpc HPC_PIO=17 HPC_SNW=17 HPC_SERVER_CPUSET=0-47 OP_TYPE=VEMB DIM=2048 TS="64" CS="1" PS="32" MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim2048.yaml TEST_TIME=30 bash benchmark/run_vemb_cross_node_sweep.sh
SERVERS_ONLY=hpc HPC_PIO=17 HPC_SNW=17 HPC_SERVER_CPUSET=0-47 OP_TYPE=VEMB DIM=3072 TS="64" CS="1" PS="32" MANIFEST=$PWD/examples/vemb_v16_warm_regions_111_dim3072.yaml TEST_TIME=30 bash benchmark/run_vemb_cross_node_sweep.sh
# hpc Aeron 17:17【本地 Mac 执行】
OP_TYPE=VEMB DIM=1024 THREADS=64 CLIENTS=1 PIPELINE=32 PROFILE=0 TEST_TIME=30 PIO=17 SNW=17 SERVER_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis CLIENT_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis SERVER_MANIFEST=/root/gqs/codespace/UnifiedBus/hpc-redis/examples/vemb_v16_warm_regions_111_xnode_aeron_dim1024.yaml bash scripts/run_aeron_cross_node_flamegraph.sh
OP_TYPE=VEMB DIM=2048 THREADS=64 CLIENTS=1 PIPELINE=32 PROFILE=0 TEST_TIME=30 PIO=17 SNW=17 SERVER_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis CLIENT_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis SERVER_MANIFEST=/root/gqs/codespace/UnifiedBus/hpc-redis/examples/vemb_v16_warm_regions_111_xnode_aeron_dim2048.yaml bash scripts/run_aeron_cross_node_flamegraph.sh
OP_TYPE=VEMB DIM=3072 THREADS=64 CLIENTS=1 PIPELINE=32 PROFILE=0 TEST_TIME=30 PIO=17 SNW=17 SERVER_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis CLIENT_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis SERVER_MANIFEST=/root/gqs/codespace/UnifiedBus/hpc-redis/examples/vemb_v16_warm_regions_111_xnode_aeron_dim3072.yaml bash scripts/run_aeron_cross_node_flamegraph.sh
```

---

## 3. 3.2.3 Redis 集群场景对比

### 3.1 DIM=300 VEMB（9 档）【块内注释标注执行位置】

```bash
# 原生 redis cluster 4 节点【HW01 执行】⚠️ HW05 失联，待恢复
bash benchmark/run_redis_cluster_vemb.sh
# hpc TCP 双机 cluster【HW01 执行】（默认 HPC_PIO=4 HPC_SNW=4）
HPC_PIO=4 HPC_SNW=4 bash run_cluster_2node_sweep.sh
# hpc Aeron 双机 cluster【本地 Mac 执行】（默认 9 档）
TEST_TIME=30 PIO=4 SNW=4 bash benchmark/vemb_v16_aeron_cluster_tput.sh
```

产物：`benchmark/results/cluster_2node_sweep/<时间戳>/summary.tsv`、`benchmark/results/aeron_cluster/<run-id>/summary.tsv`、原生 `benchmark/results/cluster_vemb/...`。三方案对比表（原生/TCP/Aeron ops/sec/core）由三个 TSV 汇总。

### 3.2 64x1x32 高维（DIM=1024/2048/3072）【块内注释标注执行位置】

```bash
# 原生 cluster【HW01 执行】⚠️ HW05 失联，待恢复
DIM=1024 TS="64" CS="1" PS="32" bash benchmark/run_redis_cluster_vemb.sh
DIM=2048 TS="64" CS="1" PS="32" bash benchmark/run_redis_cluster_vemb.sh
DIM=3072 TS="64" CS="1" PS="32" bash benchmark/run_redis_cluster_vemb.sh
# hpc TCP cluster【HW01 执行】
HPC_PIO=4 HPC_SNW=4 DIM=1024 TS="64" CS="1" PS="32" bash run_cluster_2node_sweep.sh
HPC_PIO=4 HPC_SNW=4 DIM=2048 TS="64" CS="1" PS="32" bash run_cluster_2node_sweep.sh
HPC_PIO=4 HPC_SNW=4 DIM=3072 TS="64" CS="1" PS="32" bash run_cluster_2node_sweep.sh
# hpc Aeron cluster【本地 Mac 执行】（单档）
DIM=1024 SWEEP=0 TS=64 CS=1 PS=32 TEST_TIME=30 PIO=4 SNW=4 bash benchmark/vemb_v16_aeron_cluster_tput.sh
DIM=2048 SWEEP=0 TS=64 CS=1 PS=32 TEST_TIME=30 PIO=4 SNW=4 bash benchmark/vemb_v16_aeron_cluster_tput.sh
DIM=3072 SWEEP=0 TS=64 CS=1 PS=32 TEST_TIME=30 PIO=4 SNW=4 bash benchmark/vemb_v16_aeron_cluster_tput.sh
```

---

## 4. 3.2.4 扩容场景下 VEMB 性能【块内注释标注执行位置】

```bash
# hpc TCP 扩容【HW01 执行】(脚本需内网直连 192.168.1.x:22, Mac 只走 frp 转发口不可达；默认 PIO=21 SNW=21)
PIO=21 SNW=21 bash scripts/hpc_redis_scaleout_throughput.sh
# hpc Aeron 扩容【本地 Mac 执行】
REMOTE_DIR=/root/gqs/codespace/UnifiedBus/hpc-redis PIO=21 SNW=21 bash benchmark/vemb_v16_scaleout_ub_cluster_111_to_112.sh
# 原生 redis cluster 扩容【HW01 执行】⚠️ HW05 失联，待恢复
N_INIT=3 N_FINAL=4 RAW=1 TEST_TIME=60 CLIENTS=200 THREADS=16 IO_THREADS=4 VECTORS_PER_VSET=6250 bash benchmark/run_redis_cluster_scaleout_vemb.sh
```

产物均为 phase 表（baseline/during/after）：`benchmark/results/.../summary.tsv`。

---

## 5. 3.2.5 混合读与写/删【HW01 执行】

```bash
PIO=21 SNW=21 bash benchmark/hpc_redis_mixed_workload.sh
```

（报告此节无 Aeron 行）

## 6. 3.2.6 本地远端 UB 7:1 拓扑读吞吐对比【HW01 执行】（脚本内置 server pio=21 snw=21，TCP/aeron 场景同参）

```bash
bash bench_aeron_3scenarios.sh pure_local_100k tcp   100000 30
bash bench_aeron_3scenarios.sh pure_local_100k aeron 100000 30
bash bench_aeron_3scenarios.sh small_7to1_100k tcp   100000 30
bash bench_aeron_3scenarios.sh small_7to1_100k aeron 100000 30
```

## 7. 3.2.7 VSIM_2KEY 双节点性能对比【块内注释标注执行位置】

```bash
# baseline + hpc TCP【HW01 执行】（一次跑出两组）
HPC_PIO=2 HPC_SNW=2 TEST_TIME=30 NUM_KEYS=100000 bash benchmark/run_vsim_2key_2node.sh
# HPC Aeron【本地 Mac 执行】（双 owner cluster 拓扑 + vsim-key-key，默认 9 档）
OP_TYPE=VSIM_2KEY TEST_TIME=30 NUM_KEYS=100000 PIO=2 SNW=2 bash benchmark/vemb_v16_aeron_cluster_tput.sh
```

产物：`benchmark/results/vsim_2key/.../summary.tsv`、`benchmark/results/aeron_cluster/<run-id>/summary.tsv`。

---

## 8. 环境备注

- HW05 失联影响：原生 4 节点 cluster（§3）与原生扩容（§4），待管理员恢复后执行对应标注命令。