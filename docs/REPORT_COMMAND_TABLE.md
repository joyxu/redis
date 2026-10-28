# 全量数据采集命令表（对应 docs/REPORT_SKELETON.md）

> 版本: v0.5 (20261021)；环境: HW01=192.168.90.161 / HW02=192.168.90.162；100G 数据面 192.168.1.161/162/163/164（HW04/HW05 的 enp33s0f1np1，已配置恢复）
> 约定: 未注明均在 **HW01** (`/root/gqs/codespace/UnifiedBus/hpc-redis`) 上执行；跨节点/双机类脚本由脚本自身 ssh 到对端。

## 0. 前置（每次回归一次性）【HW01 与 HW02 各执行一次】

```bash
cd /root/gqs/codespace/UnifiedBus/hpc-redis
make -C src -j$(nproc) redis-server && make -C clients/c -j$(nproc) && make -C memtier_benchmark -j$(nproc)
bash scripts/vemb_v16_build_stamp.sh write server
bash scripts/vemb_v16_build_stamp.sh write client
```

- baseline 默认 NOQUANT（`BASELINE_NOQUANT=0` 关闭）；baseline RDB 预填充自动加载（`BASELINE_RDB=0` 强制 prefill）。
- **改过 memtier/客户端源码并重编后，必须重跑 `vemb_v16_build_stamp.sh write client`（server 同理）**——否则所有 Aeron 用例在 stamp 校验处直接失败（`VEMB client sources changed`）。
- UB 用例严格串行；跑前两台空闲检查（无 redis-server/memtier/perf 且 idle≥70%）。

---

> 执行位置: 除注明"在本地 Mac 执行"外均在 HW01。以下脚本在**本地 Mac** 执行（ssh 直连 161/162/163:22 管理网，数据面走 100G 内网 192.168.1.x，脚本默认值已按新环境写死，仅需按需覆盖并发/manifest）: `scripts/run_aeron_cross_node_flamegraph.sh`、`benchmark/vemb_v16_aeron_cluster_tput_cli163.sh`（cluster 稳态，client 在 163，20261022 起替代旧 `_tput.sh`）、`scripts/vemb_v16_scaleout_ub_cluster_111_to_112_cli163.sh`（Aeron 扩容）、`scripts/vemb_v16_scaleout_ub_local_111.sh`（单机扩容）。
> 这三类脚本带节点负载预检门（`MAX_FOREIGN_CPU_PCT/MAX_FOREIGN_TOTAL_CPU_PCT/MAX_FOREIGN_CPUSET_BUSY_PCT/MAX_FOREIGN_RSS_MB`，默认 10/20/10/256）。节点上有常驻进程（如 opencode/pidstat）时按需放宽，例如 `MAX_FOREIGN_TOTAL_CPU_PCT=40 MAX_FOREIGN_CPUSET_BUSY_PCT=20 MAX_FOREIGN_RSS_MB=2048`。

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
        CLIENT_PEER_VIEW_MANIFEST=/root/gqs/codespace/UnifiedBus/hpc-redis/examples/vemb_v16_ub_peer_view_112_to_111_xnode_aeron.yaml \
            bash scripts/run_aeron_cross_node_flamegraph.sh
    done
done
```

产物：TCP/baseline 在 `benchmark/results/<op小写>_cross_node_sweep/<时间戳>/summary.tsv`（含 nic_util_pct）；Aeron 每档在 `benchmark/results/.../<run-id>/client.workload.summary.tsv`（ops/avg/p50/p99）+ `server.cpu.cpuset.summary.tsv`（ut/st/si 由 %usr/%sys/%softirq 换算）。Aeron 数据面走 UB 不走网卡，报告 nic_util 列记 NA。

### 1.3.1 跨节点 VEMB Zipf 分布（64x1x32，baseline vs hpc Aeron）

```bash
# baseline【HW01 执行】（memtier_benchmark_origin 已打 zipf 补丁，zipf_port.patch 可追溯）
SERVERS_ONLY=baseline OP_TYPE=VEMB KEY_PATTERN=Z ZIPF_S=1.2 TS="64" CS="1" PS="32" TEST_TIME=30 \
  bash benchmark/run_vemb_cross_node_sweep.sh

# hpc Aeron【本地 Mac 执行】
OP_TYPE=VEMB THREADS=64 CLIENTS=1 PIPELINE=32 \
KEY_PATTERN=Z:Z ZIPF_S=1.2 \
PROFILE=0 TEST_TIME=30 PIO=3 SNW=3 \
SERVER_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis \
CLIENT_ROOT=/root/gqs/codespace/UnifiedBus/hpc-redis \
SERVER_MANIFEST=/root/gqs/codespace/UnifiedBus/hpc-redis/examples/vemb_v16_warm_regions_111_xnode_aeron.yaml \
CLIENT_PEER_VIEW_MANIFEST=/root/gqs/codespace/UnifiedBus/hpc-redis/examples/vemb_v16_ub_peer_view_112_to_111_xnode_aeron.yaml \
    bash scripts/run_aeron_cross_node_flamegraph.sh
```

两侧同 keyspace（item:1..100000）、同指数 1.2、同并发；zipf 生成器为同一份代码（origin 补丁自 fork 原样移植）。产物：baseline 在 `benchmark/results/vemb_cross_node_sweep/<时间戳>/summary.tsv`，Aeron run-id 中段标记 `zipf1.2`。

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
    CLIENT_PEER_VIEW_MANIFEST=/root/gqs/codespace/UnifiedBus/hpc-redis/examples/vemb_v16_ub_peer_view_112_to_111_xnode_aeron.yaml \
        bash scripts/run_aeron_cross_node_flamegraph.sh
done
# baseline 12 实例【HW01 执行】
RAW=1 NUM_INSTANCES=12 IO_THREADS=4 DIM=300 bash run_multi_instance_redis_baseline.sh
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
RAW=1 NUM_INSTANCES=12 IO_THREADS=4 DIM=8 bash run_multi_instance_redis_baseline.sh
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
RAW=1 NUM_INSTANCES=12 IO_THREADS=4 DIM=1024 bash run_multi_instance_redis_baseline.sh
RAW=1 NUM_INSTANCES=12 IO_THREADS=4 DIM=2048 bash run_multi_instance_redis_baseline.sh
RAW=1 NUM_INSTANCES=12 IO_THREADS=4 DIM=3072 bash run_multi_instance_redis_baseline.sh
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

### 3.1 DIM=300 VEMB（7 档 + 64x1x64 单档）【块内注释标注执行位置】

```bash
# 原生 redis cluster 4 节点【HW01 执行】（IPS=192.168.1.161-164 走 100G；前提: HW01 ~/.ssh/config 含 HW01/HW02/HW04/HW05 别名，已配）
bash benchmark/run_redis_cluster_vemb.sh
# 原生 64x1x64 深流水单档【HW01 执行】
TS="64" CS="1" PS="64" bash benchmark/run_redis_cluster_vemb.sh
# hpc TCP 双机 cluster【HW01 执行】（默认 HPC_PIO=4 HPC_SNW=4；memtier 在 HW04/163 —— 20261022 起 cluster 稳态 client 统一放 163，脚本 MEMTIER_HOST 变量）
MEMTIER_HOST=root@192.168.90.163 HPC_PIO=4 HPC_SNW=4 bash run_cluster_2node_sweep.sh
# hpc Aeron 双机 cluster【本地 Mac 执行】（默认 7 档；client 在 HW04/163，经 UB 导入窗直达两 owner；前置: 163 已装 7 个导入窗 dev9-15 且 client stamp 通过，见脚本头）
TEST_TIME=30 PIO=2 SNW=2 bash benchmark/vemb_v16_aeron_cluster_tput_cli163.sh
# 64x1x64 深流水单档【本地 Mac 执行】（PS=64 管 client pipeline+batch，PIPELINE=64 管 server batch，两者都显式传）
SWEEP=0 TS=64 CS=1 PS=64 PIPELINE=64 TEST_TIME=30 PIO=2 SNW=2 bash benchmark/vemb_v16_aeron_cluster_tput_cli163.sh

```

产物：`benchmark/results/cluster_2node_sweep/<时间戳>/summary.tsv`、`benchmark/results/aeron_cluster/<run-id>/summary.tsv`、原生 `benchmark/results/redis_cluster_vemb/...`。汇总为两张表（原生 vs hpc TCP / 原生 vs hpc Aeron，T·C·P + ops/core + 加速比），由三个 TSV 汇总。

### 3.2 64x1x32 高维（DIM=1024/2048/3072）【块内注释标注执行位置】

```bash
# 原生 cluster【HW01 执行】
DIM=1024 TS="64" CS="1" PS="32" bash benchmark/run_redis_cluster_vemb.sh
DIM=2048 TS="64" CS="1" PS="32" bash benchmark/run_redis_cluster_vemb.sh
DIM=3072 TS="64" CS="1" PS="32" bash benchmark/run_redis_cluster_vemb.sh
# hpc TCP cluster【HW01 执行】（memtier 在 163；DIM!=300 自动用 cluster_111/112_dim<D>.yaml，需先跑一轮 warm 布局预热——脚本已内置）
MEMTIER_HOST=root@192.168.90.163 HPC_PIO=4 HPC_SNW=4 DIM=1024 TS="64" CS="1" PS="32" bash run_cluster_2node_sweep.sh
MEMTIER_HOST=root@192.168.90.163 HPC_PIO=4 HPC_SNW=4 DIM=2048 TS="64" CS="1" PS="32" bash run_cluster_2node_sweep.sh
MEMTIER_HOST=root@192.168.90.163 HPC_PIO=4 HPC_SNW=4 DIM=3072 TS="64" CS="1" PS="32" bash run_cluster_2node_sweep.sh
# hpc Aeron cluster【本地 Mac 执行】（单档；client 在 163，同 3.1 前置）
DIM=1024 SWEEP=0 TS=64 CS=1 PS=32 TEST_TIME=30 PIO=2 SNW=2 bash benchmark/vemb_v16_aeron_cluster_tput_cli163.sh
DIM=2048 SWEEP=0 TS=64 CS=1 PS=32 TEST_TIME=30 PIO=2 SNW=2 bash benchmark/vemb_v16_aeron_cluster_tput_cli163.sh
DIM=3072 SWEEP=0 TS=64 CS=1 PS=32 TEST_TIME=30 PIO=2 SNW=2 bash benchmark/vemb_v16_aeron_cluster_tput_cli163.sh
```

---

## 4. 3.2.4 扩容场景下 VEMB 性能【块内注释标注执行位置】

```bash
# hpc TCP 扩容【HW01 执行】（memtier 在 163=192.168.1.163，走 100G；默认 PIO=21 SNW=21）
PIO=21 SNW=21 bash scripts/hpc_redis_scaleout_throughput.sh
# hpc Aeron 扩容【本地 Mac 执行】（client 在 HW04/163 上，经 UB 导入窗 dev9-14 直达两 owner 数据面，绑 96-191 核；PIO/SNW 默认 7:7，可覆盖）
# 前置（一次性）: 163 已装 6 个 UB 导入窗（/root/jxw/ubme 的 ubme_tool import，163 dev9-14，回滚 state: 163:/tmp/state_163_scaleout.txt）且 163 上 `vemb_v16_build_stamp.sh verify client` 通过
REMOTE_DIR=/root/gqs/codespace/UnifiedBus/hpc-redis bash scripts/vemb_v16_scaleout_ub_cluster_111_112_cli163.sh
# 原生 redis cluster 扩容【HW01 执行】（memtier 默认在 HW04）
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
# HPC Aeron【本地 Mac 执行】（双 owner cluster 拓扑 + vsim-key-key，默认 7 档；client 在 HW01，同 §3 前置）
OP_TYPE=VSIM_2KEY TEST_TIME=30 NUM_KEYS=100000 PIO=2 SNW=2 bash benchmark/vemb_v16_aeron_cluster_tput.sh
```

产物：`benchmark/results/vsim_2key/.../summary.tsv`、`benchmark/results/aeron_cluster/<run-id>/summary.tsv`。

---

## 8. COLD 层影响对比（VEMB 读 64x1x32）

```bash
# 本地回环 cold on【HW01 执行】
RUN_LOCAL=1 WORKERS=2:2 TEST_TIME=20 OP_TYPE=VEMB TS=64 CS=1 PIPELINE=32 COLD_DIR=/root/gqs/cold_data/vemb64 bash scripts/run_aeron_best.sh

# 跨节点 cluster cold on【本地 Mac 执行】（client 在 HW04/163，同 §3 前置）
REMOTE_DIR=/root/gqs/codespace/UnifiedBus/hpc-redis COLD_DIR=/root/gqs/cold_data/xnode300 SWEEP=0 TS=64 CS=1 PS=32 TEST_TIME=30 bash benchmark/vemb_v16_aeron_cluster_tput_cli163.sh
```

产物：`perf/aeron_sweep/<run-id>/summary.tsv`、`benchmark/results/aeron_cluster/<run-id>/summary.tsv`。

## 9. HA 功能性回归（双机主备）【本地 Mac 执行】

runner 自动同步远端并构建；要求双节点空闲、VIP 未被占用。按 9.1 → 9.4 顺序执行，前一项失败不进入下一项。

### 9.1 keepalived 一次性部署（仅首次或配置变更）

```bash
scripts/deploy_keepalived_ha.sh --all --vip 192.168.90.202/24 --install-deps deploy
scripts/deploy_keepalived_ha.sh --all --vip 192.168.90.202/24 check
# 部署后保持 stopped，由 9.4 的 runner 启动，不要手工 start
```

成功输出：

```text
node=111 config=valid
node=112 config=valid
```

### 9.2 HA Replica UB 数据面回归

```bash
HA_REMOTE_DIR=/root/gqs/codespace/UnifiedBus/hpc-redis bash benchmark/tlc_ha_replica_ub_111_to_112.sh
```

成功输出（末尾 `TODO: ...` 为脚本固定提示，非失败）：

```text
visibility 111_to_112: PASS
visibility 112_to_111: PASS
tlc_ha_replica_ub_111_to_112: PASS (visibility, replication, append ACK, heartbeat, async apply)
tlc_ha_replica_ub_111_to_112: PASS (111 -> 112 snapshot chunks)
tlc_ha_replica_ub_111_to_112: PASS (111 -> 112 checkpoint install)
tlc_ha_replica_ub_111_to_112: PASS (M5 resync tail, handoff, H + 1)
tlc_ha_replica_ub_111_to_112: PASS (M5 timeout and abort cleanup)
tlc_ha_replica_ub_111_to_112: PASS (M6 automatic GAP -> AOF repair)
tlc_ha_replica_ub_111_to_112: PASS (M6 automatic retention -> snapshot)
tlc_ha_replica_ub_111_to_112: PASS (M7 retention soft compact)
tlc_ha_replica_ub_111_to_112: PASS (M7 retention hard pressure abort)
tlc_ha_replica_ub_111_to_112: PASS (persistent follower COLD recovery)
tlc_ha_replica_ub_111_to_112: PASS (manual AOF replay after follower restart)
```

### 9.3 Redis TCP/SDK 固定角色回归

```bash
HA_REMOTE_DIR=/root/gqs/codespace/UnifiedBus/hpc-redis bash benchmark/tlc_ha_redis_tcp_111_to_112.sh
```

成功输出（默认 10000 事件 → events=10004）：

```text
PASS: Redis TCP SDK -> Leader -> Replica UB -> Follower TCP SDK (10004 events)
PASS: Follower COLD recovery after reset WARM region
artifacts: <HA_REMOTE_DIR>/benchmark/results/tlc_ha_redis_tcp/<RUN_ID> (on 111 and 112)
```

### 9.4 Aeron/UB 一主一备 failover（keepalived/VIP 真实切主）

```bash
# 正向（须先完成 9.1；反向等正向 runner 退出后再执行）
HA_REMOTE_DIR=/root/gqs/codespace/UnifiedBus/hpc-redis bash benchmark/tlc_ha_aeron_failover_111_to_112.sh

# 反向
INITIAL_NODE=112 CLIENT_NODE=112 \
  HA_REMOTE_DIR=/root/gqs/codespace/UnifiedBus/hpc-redis \
  bash benchmark/tlc_ha_aeron_failover_111_to_112.sh
```

成功输出（最后一行；反向时 active/standby 对调）：

```text
tlc_ha_aeron_failover_111_to_112: PASS active_owner=1 standby_owner=0 run=<RUN_ID>
```

## 10. KVC 原生基准（Mooncake TE 传输层）【块内注释标注执行位置】

> 独立于 vemb 回归的测试族；20261022 已执行，结果在 REPORT_20261021_full.md §3.3。环境口径: HW01=192.168.90.161（owner） / HW02=192.168.90.162（远端），ssh 22；shmdev **1-8 本地池、9-16 对端导入**（161 dev1-8 ↔ 162 dev9-16，+8 配对；161:dev2 在 HW02=shmdev10）。二进制布局: HW01 仓库根 /root/gqs/codespace/UnifiedBus/hpc-redis（$MOON=Mooncake 子仓、kvc-sdk）；HW02 部署位 /root/gqs/nvsync（bench/库）。
> 通用坑: store_kv_bench 取稳态行（duration=60s）不取含预热总均值行；老环境（111/112，+4 配对）时代的实测数字仅历史参考。

### 10.0 公共准备（起一次全程复用）【HW01 执行】

```bash
MOON=/root/gqs/codespace/UnifiedBus/hpc-redis/Mooncake
sysctl -w net.ipv4.tcp_tw_reuse=1; sysctl -w "net.ipv4.ip_local_port_range=10240 65535"
export MC_TCP_ENABLE_CONNECTION_POOL=1 MC_TCP_LANES_PER_PEER=16
# metadata server 依赖 aiohttp（dnf install -y python3-aiohttp），包源在 mooncake-wheel:
dnf install -y python3-aiohttp
PYTHONPATH=$MOON/mooncake-wheel nohup python3 -m mooncake.http_metadata_server --port 8080 > /tmp/meta.log 2>&1 &
sleep 2
nohup $MOON/build/mooncake-store/src/mooncake_master \
    -default_kv_lease_ttl=600000 -http_metadata_server_port=8081 -metrics_port=9004 -logtostderr > /tmp/master.log 2>&1 &
sleep 4; ss -tln | grep -cE ":8080 |:50051 "    # 应为 2
export LD_LIBRARY_PATH=$MOON/build/mooncake-common
export COMMON="--master_server=127.0.0.1:50051 --metadata_server=http://127.0.0.1:8080/metadata \
  --num_objects=100000 --value_size=4096 --batch_size=64 --local_buffer_size=$((256*1024*1024))"
BIN=$MOON/build/mooncake-store/benchmarks/store_kv_bench_native
NUMA="numactl --cpunodebind=0 --membind=0"    # owner/bench 一律钉到设备窗所在 NUMA 节点（161/162 实测 node0）
```

纪律（每条都吃过亏）: ① 每次 bench 用唯一 `--local_hostname` 端口（rpc_meta 键重复会 400）② 跑前 `pgrep -f store_kv_bench_native` 确认无残留 ③ fill 永远串行单写者（`--read_threads=1`，写完进程退出再起下一组）④ 结果行在 stderr: `RESULT threads=N kv/s=X MiB/s=Y failed=.. verify_fail=.. p50_ms=..` ⑤ owner/bench 一律 `$NUMA` 钉扎——未钉扎成绩对调度器摆布敏感（实测 4 节点机同二进制 4890 vs 8151 MiB/s，±67%），不钉扎不可比 ⑥ **fill 后梯度必须在 600s 租约窗口内跑完**（master `-default_kv_lease_ttl=600000`；过期后全量返回 OBJECT_NOT_FOUND(-704)，现象是 kv/s=0 全 failed——不是并发压挂，重新 fill 即恢复）。

### 10.1 传输层基准（store_kv_bench_native: TE over TCP / URMA / ubmem）

编译【HW01 执行，一次】:

```bash
dnf install -y rdma-core-devel glog-devel gflags-devel jsoncpp-devel libunwind-devel numactl-devel \
  python3-devel boost-devel protobuf-devel yaml-cpp-devel libcurl-devel hiredis-devel liburing-devel \
  jemalloc-devel libzstd-devel elfutils-libelf-devel patchelf xxhash-devel libbsd-devel
# msgpack-devel 仓里没有、mooncake-store 又必须（源码装头文件）:
git clone --depth 1 -b cpp_master https://github.com/msgpack/msgpack-c.git
cmake -B msgpack-c/build -S msgpack-c -DMSGPACK_CXX=ON -DMSGPACK_BUILD_EXAMPLES=OFF \
      -DMSGPACK_BUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX=/usr/local
cmake --build msgpack-c/build --target install -j32
# Mooncake/extern/{yalantinglibs,pybind11} 为子模块: git submodule update --init 后
# yalantinglibs（v0.5.7）按 dependencies.sh 口径 cmake --build --target install 到 /usr/local
cd Mooncake
cmake -B build -DCMAKE_BUILD_TYPE=Release -DUSE_TCP=ON -DUSE_UB=ON -DUSE_UBMEM=ON \
      -DBUILD_BENCHMARK=ON [-DURMA_SYSTEM_INCLUDE_DIR=/usr/include/ub/umdk/urma] \
      [-DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=<googletest-1.17.0 源码目录>]
cmake --build build --target mooncake_master mooncake_client store_kv_bench_native -j32
```

方括号开关: urma 头在 /usr/include/ub/umdk/urma 时必传第一项（否则走 atomgit fallback，无外网必挂）；无外网时用本地 googletest-1.17.0 源码目录传第二项。
增量重编: 缓存指向的 /tmp/UnifiedBus 或 /tmp/deps/googletest-v1.17.0 若已失，先 `ln -sfn /root/gqs/codespace/UnifiedBus /tmp/UnifiedBus`、`git clone --depth 1 -b v1.17.0 https://github.com/google/googletest.git /tmp/deps/googletest-v1.17.0`。

本地三组【HW01 执行】（每组: 起 owner → fill → 梯度 → 收尾 `pkill -x mooncake_client; sleep 6; pkill -x mooncake_master; sleep 2`）:

```bash
# ---- 1) TE over TCP ----
export MC_TCP_ENABLE_CONNECTION_POOL=1 MC_TCP_LANES_PER_PEER=16; unset MC_CUSTOM_TOPO_JSON MC_UBMEM_DEV_PATH MC_UBMEM_DEV_SIZE
nohup $NUMA $MOON/build/mooncake-store/src/mooncake_client --host=127.0.0.1 --global_segment_size=4GB \
  --master_server_address=localhost:50051 --metadata_server=http://127.0.0.1:8080/metadata \
  --protocol=tcp --port=50052 --threads=32 --logtostderr > /tmp/client.log 2>&1 &
sleep 8 && ss -tln | grep -q ':50052 ' && echo CLIENT_UP
$NUMA $BIN $COMMON --key_prefix=te1 --protocol=tcp --local_hostname=127.0.0.1:21001 --read_threads=1 --runtime=1
port=21011
for T in 8 16 32 64 128; do
  $NUMA $BIN $COMMON --key_prefix=te1 --protocol=tcp --skip_fill \
    --read_threads=$T --runtime=20 --local_hostname=127.0.0.1:$port
  port=$((port+1))
done
pkill -x mooncake_client; sleep 6; pkill -x mooncake_master; sleep 2

# ---- 2) TE over URMA（client 与 bench 同机, topo json 一致; 勿设 MC_MS_AUTO_DISC=1）----
unset MC_TCP_ENABLE_CONNECTION_POOL MC_TCP_LANES_PER_PEER MC_UBMEM_DEV_PATH MC_UBMEM_DEV_SIZE
# 设备名以实查为准（本轮 161/162 均为 udmac0d1e2 等 7 个）: ls /dev/uburma*
echo '{"cpu:0": [["udmac0d1e2"], []], "cpu:1": [["udmac0d1e2"], []]}' > /tmp/topo.json
export MC_CUSTOM_TOPO_JSON=/tmp/topo.json
nohup $NUMA $MOON/build/mooncake-store/src/mooncake_client --host=127.0.0.1 --global_segment_size=4GB \
  --master_server_address=localhost:50051 --metadata_server=http://127.0.0.1:8080/metadata \
  --protocol=ub --port=50052 --threads=32 --logtostderr > /tmp/client_ub.log 2>&1 &
sleep 8 && ss -tln | grep -q ':50052 ' && echo CLIENT_UP
grep -c "udmac0d1e2 add to list" /tmp/client_ub.log    # 应为 1; 见 mock_urma_device 即 topo 未生效
$NUMA $BIN $COMMON --key_prefix=ur --protocol=ub --local_hostname=127.0.0.1:21101 --read_threads=1 --runtime=1
port=21111
for T in 8 16 32 64 128; do
  $NUMA $BIN $COMMON --key_prefix=ur --protocol=ub --skip_fill \
    --read_threads=$T --runtime=20 --local_hostname=127.0.0.1:$port
  port=$((port+1))
done
pkill -x mooncake_client; sleep 6; pkill -x mooncake_master; sleep 2

# ---- 3) TE over ubmem（段占窗底 4GB、本端缓冲占窗顶; MC_UBMEM_DEV_SIZE=完整窗口 8GB）----
unset MC_CUSTOM_TOPO_JSON
export MC_UBMEM_DEV_PATH=/dev/obmm_shmdev2 MC_UBMEM_DEV_SIZE=$((8*1024*1024*1024))
fuser /dev/obmm_shmdev2 && echo BUSY    # 必须无占用
nohup $NUMA $MOON/build/mooncake-store/src/mooncake_client --host=127.0.0.1 --global_segment_size=4GB \
  --master_server_address=localhost:50051 --metadata_server=http://127.0.0.1:8080/metadata \
  --protocol=ubmem --port=50052 --threads=32 --logtostderr > /tmp/client_ubm.log 2>&1 &
sleep 8 && grep "Mounting ubmem segment" /tmp/client_ubm.log && grep "inline_memcpy=1" /tmp/client_ubm.log
UBM_COMMON="--protocol=ubmem --key_prefix=um"
$NUMA $BIN $COMMON $UBM_COMMON --local_hostname=127.0.0.1:23001 --read_threads=1 --runtime=1
port=23011
for T in 8 16 32 64 128; do
  $NUMA $BIN $COMMON $UBM_COMMON --skip_fill --read_threads=$T \
    --runtime=20 --local_hostname=127.0.0.1:$port
  port=$((port+1))
done
pkill -x mooncake_client; sleep 6; pkill -x mooncake_master; sleep 2
```

跨节点三组【owner=HW01 / bench=HW02】:

```bash
# ---- 一次性: 【HW01→HW02】同步三件 ----
ssh root@192.168.90.162 'mkdir -p /root/gqs/nvsync'
scp $MOON/build/mooncake-store/benchmarks/store_kv_bench_native root@192.168.90.162:/root/gqs/nvsync/
scp $MOON/../kvc-sdk/build/libkvc_server.so root@192.168.90.162:/root/gqs/nvsync/
scp $MOON/build/mooncake-common/libasio.so root@192.168.90.162:/root/gqs/nvsync/
# ---- 【HW02】侧公共 ----
# 注意 cpu:1 也必须带第二个空列表，矩阵不完整会被 bench 拒绝（topology matrix invalid）
ssh root@192.168.90.162 'mkdir -p /root/gqs/nvsync && echo "{\"cpu:0\": [[\"udmac0d1e2\"], []], \"cpu:1\": [[\"udmac0d1e2\"], []]}" > /root/gqs/nvsync/topo.json'
grep -oE "cna\[[0-9]+\]" /proc/mami/udm/udevice | sort -u    # 【HW01】cna 实查勿背（十进制）; HW02 侧同理
```

```bash
# ---- 组1 TE over TCP ----
# 【HW01】owner（--host 必须真实 IP; master/metadata 同 10.0）:
unset MC_CUSTOM_TOPO_JSON MC_UBMEM_DEV_PATH MC_UBMEM_DEV_SIZE
nohup $NUMA $MOON/build/mooncake-store/src/mooncake_client --host=192.168.90.161 --global_segment_size=4GB \
  --master_server_address=127.0.0.1:50051 --metadata_server=http://127.0.0.1:8080/metadata \
  --protocol=tcp --port=50052 --threads=32 --logtostderr > /tmp/client.log 2>&1 &
sleep 8 && ss -tln | grep -q ':50052 ' && echo OWNER_UP
# 【HW02】bench:
export LD_LIBRARY_PATH=/root/gqs/nvsync
export COMMON="--master_server=192.168.90.161:50051 --metadata_server=http://192.168.90.161:8080/metadata \
  --num_objects=100000 --value_size=4096 --batch_size=64 --local_buffer_size=$((256*1024*1024))"
export MC_TCP_ENABLE_CONNECTION_POOL=1 MC_TCP_LANES_PER_PEER=16
unset MC_UBMEM_DEV_PATH MC_UBMEM_DEV_SIZE
BIN=/root/gqs/nvsync/store_kv_bench_native
$NUMA $BIN $COMMON --protocol=tcp --key_prefix=ctcp \
  --local_hostname=192.168.90.162:22001 --read_threads=1 --runtime=1    # fill（跨网 TE 写）
port=22011
for T in 8 16 32 64 128; do
  $NUMA $BIN $COMMON --protocol=tcp --key_prefix=ctcp --skip_fill \
    --read_threads=$T --runtime=20 --local_hostname=192.168.90.162:$port
  port=$((port+1))
done
# 【HW01】收尾: pkill -x mooncake_client; sleep 6; pkill -x mooncake_master; sleep 2
```

```bash
# ---- 组2 TE over URMA（先按组1收尾再起）----
# 【HW01】owner:
export MC_CUSTOM_TOPO_JSON=/tmp/topo.json
nohup $NUMA $MOON/build/mooncake-store/src/mooncake_client --host=192.168.90.161 --global_segment_size=4GB \
  --master_server_address=127.0.0.1:50051 --metadata_server=http://127.0.0.1:8080/metadata \
  --protocol=ub --port=50052 --threads=32 --logtostderr > /tmp/client.log 2>&1 &
sleep 8 && ss -tln | grep -q ':50052 ' && echo OWNER_UP
# 【HW02】bench:
export LD_LIBRARY_PATH=/root/gqs/nvsync
export COMMON="--master_server=192.168.90.161:50051 --metadata_server=http://192.168.90.161:8080/metadata \
  --num_objects=100000 --value_size=4096 --batch_size=64 --local_buffer_size=$((256*1024*1024))"
export MC_CUSTOM_TOPO_JSON=/root/gqs/nvsync/topo.json
BIN=/root/gqs/nvsync/store_kv_bench_native
$NUMA $BIN $COMMON --protocol=ub --key_prefix=cur \
  --local_hostname=192.168.90.162:22101 --read_threads=1 --runtime=1
port=22111
for T in 8 16 32 64 128; do
  $NUMA $BIN $COMMON --protocol=ub --key_prefix=cur --skip_fill \
    --read_threads=$T --runtime=20 --local_hostname=192.168.90.162:$port
  port=$((port+1))
done
# 【HW01】收尾同组1
```

```bash
# ---- 组3 TE over ubmem（镜像窗 + PEER_DEV_MAP; master 单起 rpc 50061 避让）----
# 【HW01】起 master(50061) + owner:
nohup $MOON/build/mooncake-store/src/mooncake_master \
  -default_kv_lease_ttl=600000 -rpc_port=50061 -http_metadata_server_port=8081 \
  -metrics_port=9004 -logtostderr > /tmp/master_61.log 2>&1 &
sleep 4; ss -tln | grep -c ':50061 '
export MC_UBMEM_DEV_PATH=/dev/obmm_shmdev2 MC_UBMEM_DEV_SIZE=$((8*1024*1024*1024))
nohup $NUMA $MOON/build/mooncake-store/src/mooncake_client --host=192.168.90.161 --global_segment_size=4GB \
  --master_server_address=127.0.0.1:50061 --metadata_server=http://127.0.0.1:8080/metadata \
  --protocol=ubmem --port=50052 --threads=32 --logtostderr > /tmp/client_ubx.log 2>&1 &
sleep 8 && grep "Mounting ubmem segment" /tmp/client_ubx.log    # 段挂载成功标志
# 【HW02】sweep（本地池 shmdev3 作本端缓冲, 读经镜像窗 shmdev10）:
export LD_LIBRARY_PATH=/root/gqs/nvsync
export MC_UBMEM_DEV_PATH=/dev/obmm_shmdev3 MC_UBMEM_DEV_SIZE=$((8*1024*1024*1024))
export MC_UBMEM_PEER_DEV_MAP="192.168.90.161:/dev/obmm_shmdev2=/dev/obmm_shmdev10"
BIN=/root/gqs/nvsync/store_kv_bench_native
UBX_COMMON="--master_server=192.168.90.161:50061 \
  --metadata_server=http://192.168.90.161:8080/metadata \
  --num_objects=100000 --value_size=4096 --batch_size=64 \
  --local_buffer_size=$((256*1024*1024)) --protocol=ubmem --key_prefix=xxu"
$NUMA $BIN $UBX_COMMON --local_hostname=192.168.90.162:24101 --read_threads=1 --runtime=1     # fill（经镜像窗串行写）
$NUMA $BIN $UBX_COMMON --skip_fill --read_threads=1 --runtime=5 \
  --local_hostname=192.168.90.162:24110    # smoke: verify_fail=0 字节级跨窗校验门，过了才继续
port=24111
for T in 8 16 32 64 128 192 256; do
  $NUMA $NUMA $BIN $UBX_COMMON --skip_fill --read_threads=$T --runtime=20 \
    --local_hostname=192.168.90.162:$port
  dmesg | grep -iE "ucmi|mami" | grep -icE "error|fail"    # 每档后应无增量
  port=$((port+1))
done
# 收尾（master/owner 按 10.0 起的 pid 文件 kill，勿用 pkill -f）:
# kill $(cat /tmp/ubx_owner.pid) $(cat /tmp/ubx_master.pid) 2>/dev/null; rm -f /tmp/ubx_owner.pid /tmp/ubx_master.pid
```

镜像映射除 `MC_UBMEM_PEER_DEV_MAP` 外，还支持集群级 JSON 拓扑文件 `MC_UBMEM_PEER_DEV_MAP_FILE`（全部节点读同一份，结构为 源host → 源设备 → {读取方host: 本地镜像设备}，各节点只取"自己是读取方"的条目；纯导出方读后零映射不报错；env 已设时文件被忽略）:

```bash
# /root/gqs/ubmem_peer_dev_map.json（两节点同内容同 md5，161 放 /root/gqs/，162 放 /root/gqs/nvsync/）:
#   { "192.168.90.161": { "/dev/obmm_shmdev2": { "192.168.90.162": "/dev/obmm_shmdev10" } } }
unset MC_UBMEM_PEER_DEV_MAP
export MC_UBMEM_PEER_DEV_MAP_FILE=/root/gqs/nvsync/ubmem_peer_dev_map.json   # owner 侧同样设（/root/gqs/ 路径）
```
20261026 实测（新格式，owner+bench 双侧同文件）: owner 纯导出零映射零报错；fill/smoke(verify_fail=0)/32T 均过；文件中 163/164 无关节点块被正确忽略；env+file 同设时 env 优先（bogus 设备即时报 cannot open）。
```

### 10.2 大块读取口径（每键 1 MiB，TCP vs 共享内存窗，评估大块带宽）

```bash
# 复用 10.0 的 master（分配与键值大小无关）
export COMMON1M="--master_server=127.0.0.1:50051 \
  --metadata_server=http://127.0.0.1:8080/metadata \
  --num_objects=3072 --value_size=1048576 --batch_size=16 \
  --local_buffer_size=$((2560*1024*1024))"

# ---- ubmem 路径（共享内存窗; owner 起段同 10.1 本地 ubmem 组: MC_UBMEM_DEV_PATH=shmdev2, --protocol=ubmem）----
UBM1M="--protocol=ubmem --key_prefix=um1"
$NUMA $BIN $COMMON1M $UBM1M --local_hostname=127.0.0.1:24001 --read_threads=1 --runtime=1    # fill（串行）
for T in 1 8 16 32 64 128; do    # 读梯度·关闭拷贝池
  MC_UBMEM_COPY_THREADS=0 $NUMA $BIN $COMMON1M $UBM1M --skip_fill \
    --read_threads=$T --runtime=20 --local_hostname=127.0.0.1:$((24500+T))
done

# ---- TCP 对照（owner 起段同 10.1 本地 TCP 组: --protocol=tcp）----
TCP1M="--protocol=tcp --key_prefix=tcp1"
$NUMA $BIN $COMMON1M $TCP1M --local_hostname=127.0.0.1:24201 --read_threads=1 --runtime=1    # fill
for T in 1 8 16 32 64 128; do
  $NUMA $BIN $COMMON1M $TCP1M --skip_fill --read_threads=$T --runtime=20 --local_hostname=127.0.0.1:$((24600+T))
done
# 收尾: pkill -x mooncake_client; sleep 6; pkill -x mooncake_master; sleep 2（下次跑 10.0 前按 10.0 重起 4K master）
```

### 10.3 收尾清理

```bash
pkill -x mooncake_client; pkill -x mooncake_master; pkill -f "mooncake.http_metadata"
# 测试段清零（mmap 直写，flush EINVAL 可忽略; 勿动基线设备）
python3 -c "import mmap; f=open('/dev/obmm_shmdev1','r+b',buffering=0); m=mmap.mmap(f.fileno(),1048576); m[0:1048576]=b'\0'*1048576; m.close(); f.close()"
rm -rf /home/ub_bench /tmp/ub_bench /root/ub_bench
```

---

## 11. 环境备注

- HW04(ub3)/HW05(ub4) 已部署本仓库与 redis-8.6.3（/root/gqs/codespace）；数据面 192.168.1.163/164 已配置恢复（enp33s0f1np1，100G）。
- memtier_benchmark_origin（含 zipf 补丁）已自 /ssd/root112/root/gqs/codespace/UnifiedBus/memtier_benchmark_origin 同步恢复至 HW01/HW02 同路径。
- 管理网偶发瞬时断连（ssh 255/预检失败）：直接重跑；但瞬断可能遗留未清理的 redis-server/memtier，会把后续命令的负载预检门拦掉——重跑前先清掉本次测试的残留进程（勿动他人/用户常驻进程）。HA Aeron runner 重复跑可加 TLC_HA_AERON_SKIP_SYNC=1 跳过远端重编。
