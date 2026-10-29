#!/bin/bash
# ============================================================================
# run_cluster_2node_sweep.sh
# 双节点集群 VEMB 吞吐 sweep
#   HW01 + HW02 各起一个 hpc-redis (不同 cluster manifest)
#   memtier 在 MEMTIER_HOST (默认 HW04/163) 多端点 (--vemb-v16-endpoints), taskset 绑核
#   SAR 同时采集两台 server 网卡利用率
#
# 用法 (在 HW01 上执行):
#   bash run_cluster_2node_sweep.sh                          # 默认 9 档 DIM=300
#   DIM=300 TEST_TIME=10 bash run_cluster_2node_sweep.sh       # smoke
#   TS="64" CS="8" PS="32" bash run_cluster_2node_sweep.sh   # 单档
#   MEMTIER_HOST=root@192.168.90.163 bash run_cluster_2node_sweep.sh   # 指定压测机
# ============================================================================
set -uo pipefail

# === 拓扑 ===
HW01_IP=${HW01_IP:-192.168.1.161}
HW02_IP=${HW02_IP:-192.168.1.162}
SERVER_PORT=${SERVER_PORT:-6390}
# 第二台 server (HW02) 的 ssh 目标
HW02_SSH=${HW02_SSH:-root@192.168.90.162}
# memtier 执行机 (20261021 起 cluster 稳态 client 统一放 HW04/163, 与两台 server 分离)
MEMTIER_HOST=${MEMTIER_HOST:-root@192.168.90.163}
NIC_IFACE=${NIC_IFACE:-enp33s0f1np1}
HW02_NIC_IFACE=${HW02_NIC_IFACE:-enp38s0f1np1}

# === memtier 绑核 (压测机侧, 与 server 无关) ===
MEMTIER_CPUSET=${MEMTIER_CPUSET:-96-191}
MEMTIER_TASKSET="taskset -c $MEMTIER_CPUSET"

# === 路径 (SERVER 本地) ===
HPC_DIR=${HPC_DIR:-/root/gqs/codespace/UnifiedBus/hpc-redis}
# 高维 (DIM!=300) 默认使用对应 dim 变体 manifest (value_size=dim*4, 4GB region)
DIM_MANIFEST_SUFFIX=""
if [ "${DIM:-300}" != "300" ]; then
    DIM_MANIFEST_SUFFIX="_dim${DIM}"
fi
HW01_MANIFEST=${HW01_MANIFEST:-$HPC_DIR/examples/cluster_111${DIM_MANIFEST_SUFFIX}.yaml}
HW02_MANIFEST=${HW02_MANIFEST:-$HPC_DIR/examples/cluster_112${DIM_MANIFEST_SUFFIX}.yaml}
MEMTIER=${MEMTIER:-$HPC_DIR/memtier_benchmark/memtier_benchmark}
DATA_DIR=${DATA_DIR:-/tmp/redis-cluster-sweep}

# === 测试参数 ===
TEST_TIME=${TEST_TIME:-30}
DIM=${DIM:-300}
NUM_KEYS=${NUM_KEYS:-100000}

# === server 参数 ===
HPC_PIO=${HPC_PIO:-4}
HPC_SNW=${HPC_SNW:-4}
BASELINE_IO_THREADS=${BASELINE_IO_THREADS:-4}

# === 9 档配置矩阵 ===
# 7 档精简矩阵 (原 9 档, 20260826 削减: 保留低并发斜率 + 高并发饱和 + 两条 c 扫描)
TS_DEFAULT=( 1  1  4 16 64 64 64)
CS_DEFAULT=( 1  1  1  1  1  4 16)
PS_DEFAULT=( 1 32 32 32 32 32 32)
TS=( ${TS:-${TS_DEFAULT[*]}} )
CS=( ${CS:-${CS_DEFAULT[*]}} )
PS=( ${PS:-${PS_DEFAULT[*]}} )
NCONFIGS=${#TS[@]}

# === 输出 ===
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR=${OUTDIR:-benchmark/results/cluster_2node_sweep/${TIMESTAMP}}
RAWDIR="$OUTDIR/raw"
TSV="$OUTDIR/summary.tsv"

ulimit -n 200000
mkdir -p "$RAWDIR" "$DATA_DIR"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# ============================================================================
# 客户端环境准备: 同步源码 + 编译 memtier
# ============================================================================
setup_client() {
    log "=== $MEMTIER_HOST 环境检查 ==="
    local client_memtier="$HPC_DIR/memtier_benchmark/memtier_benchmark"
    if ssh "$MEMTIER_HOST" "test -x $client_memtier" 2>/dev/null; then
        log "  $MEMTIER_HOST memtier 已存在"
        return 0
    fi
    log "  $MEMTIER_HOST 缺少 memtier, 同步源码编译..."
    ssh "$MEMTIER_HOST" "mkdir -p /root/gqs/codespace/UnifiedBus" 2>/dev/null
    cd /root/gqs/codespace/UnifiedBus
    tar czf /tmp/hpc_redis_src.tgz --exclude='.git' --exclude='benchmark/results' hpc-redis/
    scp /tmp/hpc_redis_src.tgz "$MEMTIER_HOST:/tmp/" 2>/dev/null
    ssh "$MEMTIER_HOST" "cd /root/gqs/codespace/UnifiedBus && tar xzf /tmp/hpc_redis_src.tgz" 2>/dev/null
    ssh "$MEMTIER_HOST" "cd /root/gqs/codespace/UnifiedBus/hpc-redis/clients/c && make clean 2>/dev/null && make -j\$(nproc)" 2>&1 | tail -5
    local build_log=$(ssh "$MEMTIER_HOST" "cd /root/gqs/codespace/UnifiedBus/hpc-redis/memtier_benchmark && make clean 2>/dev/null && make -j\$(nproc) 2>&1" 2>&1)
    if ! ssh "$MEMTIER_HOST" "test -x /root/gqs/codespace/UnifiedBus/hpc-redis/memtier_benchmark/memtier_benchmark" 2>/dev/null; then
        log "ERROR: $MEMTIER_HOST memtier 编译失败"
        echo "$build_log" | tail -20
        return 1
    fi
    log "  $MEMTIER_HOST memtier 编译完成"
}

# ============================================================================
# 部署 manifest 到 HW02
# ============================================================================
deploy_hw02_manifest() {
    log "=== 部署 manifest + 环境到 HW02 ==="
    ssh "$HW02_SSH" "mkdir -p $HPC_DIR/examples $DATA_DIR" 2>/dev/null
    scp "$HW02_MANIFEST" "$HW02_SSH:$HPC_DIR/examples/cluster_112.yaml" 2>/dev/null
    log "  cluster_112.yaml -> HW02:$HPC_DIR/examples/"
}

# ============================================================================
# 启停 server
# ============================================================================
start_server_hw01() {
    log "  启动 HW01 hpc-redis (pio=$HPC_PIO snw=$HPC_SNW)..."
    pkill -9 -f "redis-server.*:$SERVER_PORT" 2>/dev/null || true
    sleep 1
    local max_vec=$((NUM_KEYS + 1000))
    taskset -c 0-47 \
        $HPC_DIR/src/redis-server \
            --port $SERVER_PORT --bind 0.0.0.0 --protected-mode no \
            --tcp-backlog 16384 --tcp-keepalive 1800 --timeout 0 \
            --io-threads $BASELINE_IO_THREADS --io-threads-do-reads yes \
            --vemb-v16-enabled yes \
            --vemb-v16-tcp-host $HW01_IP \
            --vemb-v16-dim $DIM \
            --vemb-v16-proxy-io-threads $HPC_PIO \
            --vemb-v16-supernode-workers $HPC_SNW \
            --vemb-v16-warm-regions-manifest $HW01_MANIFEST \
            --vemb-v16-reset-warm-regions yes \
            --appendonly no --save '' \
            --dir $DATA_DIR --logfile $DATA_DIR/hw01.log \
            --daemonize yes
    for _ in $(seq 1 60); do
        ss -tln | grep -q ":$SERVER_PORT " && break
        sleep 0.25
    done
    ss -tln | grep -q ":$SERVER_PORT " || { log "FAIL: HW01 server not listening"; return 1; }
    log "  HW01 server ready"
}

start_server_hw02() {
    log "  启动 HW02 hpc-redis (pio=$HPC_PIO snw=$HPC_SNW)..."
    ssh "$HW02_SSH" "pkill -9 -f 'redis-server.*:$SERVER_PORT' 2>/dev/null; sleep 1; mkdir -p $DATA_DIR"
    local max_vec=$((NUM_KEYS + 1000))
    ssh "$HW02_SSH" "taskset -c 0-47 \
        $HPC_DIR/src/redis-server \
            --port $SERVER_PORT --bind 0.0.0.0 --protected-mode no \
            --tcp-backlog 16384 --tcp-keepalive 1800 --timeout 0 \
            --io-threads $BASELINE_IO_THREADS --io-threads-do-reads yes \
            --vemb-v16-enabled yes \
            --vemb-v16-tcp-host $HW02_IP \
            --vemb-v16-dim $DIM \
            --vemb-v16-proxy-io-threads $HPC_PIO \
            --vemb-v16-supernode-workers $HPC_SNW \
            --vemb-v16-warm-regions-manifest $HW02_MANIFEST \
            --vemb-v16-reset-warm-regions yes \
            --appendonly no --save '' \
            --dir $DATA_DIR --logfile $DATA_DIR/hw02.log \
            --daemonize yes"
    for _ in $(seq 1 60); do
        ssh "$HW02_SSH" "ss -tln | grep -q ':$SERVER_PORT '" 2>/dev/null && break
        sleep 0.25
    done
    ssh "$HW02_SSH" "ss -tln | grep -q ':$SERVER_PORT '" 2>/dev/null || { log "FAIL: HW02 server not listening"; return 1; }
    log "  HW02 server ready"
}

stop_servers() {
    log "  关闭所有 server..."
    pkill -9 -f "redis-server.*:$SERVER_PORT" 2>/dev/null || true
    ssh "$HW02_SSH" "pkill -9 -f 'redis-server.*:$SERVER_PORT'" 2>/dev/null || true
    sleep 2
}

# ============================================================================
# 发布双 owner 集群拓扑
#   warm-region manifest 不含对端 owner 声明, 服务端 owner ring 退化成单
#   owner, 客户端拉到的拓扑也就一个 owner, 全部请求落在 HW01 单入口
#   (HW02 只出 UB 内存, 零 CPU 零 TCP 流量)。server 起来后用 topology_ctl
#   显式发布 ring={0,1} + 双 endpoint, 客户端即按 key hash 双入口路由。
# ============================================================================
publish_topology() {
    log "  发布双 owner 拓扑 (active={0,1})..."
    (cd $HPC_DIR && ./benchmark/vemb_v16_topology_ctl --set --transport tcp \
        --host $HW01_IP --port $SERVER_PORT \
        --epoch 1 --min-write-epoch 1 --vnode-count 10 \
        --active 0,1 --standby 0,1 \
        --owner-endpoints 0=${HW01_IP}:${SERVER_PORT},1=${HW02_IP}:${SERVER_PORT} \
        --timeout-ms 5000) || { log "FAIL: HW01 topology publish"; return 1; }
    ssh "$HW02_SSH" "cd $HPC_DIR && ./benchmark/vemb_v16_topology_ctl --set --transport tcp \
        --host $HW02_IP --port $SERVER_PORT \
        --epoch 1 --min-write-epoch 1 --vnode-count 10 \
        --active 0,1 --standby 0,1 \
        --owner-endpoints 0=${HW01_IP}:${SERVER_PORT},1=${HW02_IP}:${SERVER_PORT} \
        --timeout-ms 5000" || { log "FAIL: HW02 topology publish"; return 1; }
    log "  双 owner 拓扑已发布"
}

# ============================================================================
# jiffies helper (sum utime+stime across all TIDs of redis-server on port)
# ============================================================================
snapshot_jiffies() {
    local port=$1 ut=0 st=0
    for pid in $(pgrep -x redis-server); do
        if tr '\0' ' ' < /proc/$pid/cmdline 2>/dev/null | grep -Eq ":${port}\$|:${port} "; then
            read u s < <(awk '{u+=$14; s+=$15} END{printf "%d %d", u+0, s+0}' /proc/$pid/task/*/stat 2>/dev/null)
            ut=$((ut + ${u:-0})); st=$((st + ${s:-0}))
        fi
    done
    echo "$ut $st"
}

snapshot_si() { awk '/^cpu /{print $8}' /proc/stat 2>/dev/null; }

snapshot_rss() {
    local port=$1 rss=0
    for pid in $(pgrep -x redis-server); do
        if tr '\0' ' ' < /proc/$pid/cmdline 2>/dev/null | grep -Eq ":${port}\$|:${port} "; then
            local r=$(awk '/^VmRSS:/{print $2+0}' /proc/$pid/status 2>/dev/null)
            rss=$((rss + ${r:-0}))
        fi
    done
    echo $rss
}

# ============================================================================
# Prefill: HW07 多端点 VADD
# ============================================================================
ENDPOINTS=${ENDPOINTS:-"${HW01_IP}:${SERVER_PORT},${HW02_IP}:${SERVER_PORT}"}

prefill() {
    log "  Prefill ${NUM_KEYS} keys via multi-endpoint VADD..."
    ssh "$MEMTIER_HOST" "pkill -9 -f memtier_benchmark" 2>/dev/null || true
    local nfill=$NUM_KEYS
    ssh "$MEMTIER_HOST" "ulimit -n 200000; $MEMTIER_TASKSET $MEMTIER \
        --protocol vemb_v16 --vemb-v16-dim $DIM \
        --vemb-v16-endpoints=$ENDPOINTS \
        -t 8 -c 1 --pipeline=32 \
        --ratio=1:0 --key-pattern=S:S \
        --key-prefix=item: --key-minimum=1 --key-maximum=$nfill \
        -n $nfill --hide-histogram" \
        > "$RAWDIR/prefill.log" 2>&1
    local n_sets=$(grep "^Totals" "$RAWDIR/prefill.log" 2>/dev/null | awk '{print $2}')
    local n_sets_int=$(printf "%.0f" "${n_sets:-0}")
    log "  prefill done: ${n_sets_int} sets/sec (target=${NUM_KEYS} keys)"
    # prefill 8 个 channel 的关闭 (teardown 走 owner 通知, ~1s) 会短暂阻塞
    # accept/控制帧应答, 等 teardown 排空再起 bench。
    sleep 3
}

# ============================================================================
# 一档 sweep
# ============================================================================
run_one_config() {
    local idx=$1
    local t=${TS[$idx]} c=${CS[$idx]} p=${PS[$idx]}
    log "--- config $((idx+1))/${NCONFIGS}: t=$t c=$c pipeline=$p ---"

    # === jiffies before ===
    read j0_ut_hw01 j0_st_hw01 < <(snapshot_jiffies $SERVER_PORT)
    read j0_ut_hw02 j0_st_hw02 < <(ssh "$HW02_SSH" "$(declare -f snapshot_jiffies); snapshot_jiffies $SERVER_PORT")
    JB_NS=$(date +%s%N)
    local si0_hw01=$(snapshot_si)
    local si0_hw02=$(ssh "$HW02_SSH" "$(declare -f snapshot_si); snapshot_si")
    local rss0_hw01=$(snapshot_rss $SERVER_PORT)
    local rss0_hw02=$(ssh "$HW02_SSH" "$(declare -f snapshot_rss); snapshot_rss $SERVER_PORT")

    # === SAR 双机同时启动 ===
    local sar01="$RAWDIR/sar_hw01_t${t}_c${c}_p${p}.log"
    local sar02="$RAWDIR/sar_hw02_t${t}_c${c}_p${p}.log"
    rm -f "$sar01" "$sar02"
    local sar_dur=$((TEST_TIME + 5))
    sar -n DEV 1 $sar_dur > "$sar01" 2>&1 &
    local sar01_pid=$!
    ssh "$HW02_SSH" "sar -n DEV 1 $sar_dur" > "$sar02" 2>&1 &
    local sar02_pid=$!
    sleep 1

    # === memtier (多端点 VEMB 读, NUMA pinning) ===
    local raw_remote="/tmp/cluster_vemb_t${t}_c${c}_p${p}.log"
    local raw_local="$RAWDIR/t${t}_c${c}_p${p}.log"
    ssh "$MEMTIER_HOST" "ulimit -n 200000; $MEMTIER_TASKSET $MEMTIER \
        --protocol vemb_v16 --vemb-v16-dim $DIM \
        --vemb-v16-endpoints=$ENDPOINTS \
        -t $t -c $c --pipeline=$p \
        --test-time=$TEST_TIME --hide-histogram \
        --ratio=0:1 --key-pattern=R:R \
        --key-prefix=item: --key-minimum=1 --key-maximum=$NUM_KEYS" \
        > "$raw_local" 2>&1

    # === 收集 SAR ===
    wait $sar01_pid 2>/dev/null || true
    wait $sar02_pid 2>/dev/null || true

    # === jiffies after ===
    read j1_ut_hw01 j1_st_hw01 < <(snapshot_jiffies $SERVER_PORT)
    read j1_ut_hw02 j1_st_hw02 < <(ssh "$HW02_SSH" "$(declare -f snapshot_jiffies); snapshot_jiffies $SERVER_PORT")
    JA_NS=$(date +%s%N)
    ELAPSED_NS=$((JA_NS - JB_NS > 0 ? JA_NS - JB_NS : TEST_TIME * 1000000000))
    # core 分母 = 纳秒实测窗口 (与脚本13对齐)
    local cores01=$(awk -v d=$(((j1_ut_hw01 + j1_st_hw01) - (j0_ut_hw01 + j0_st_hw01))) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
    local cores02=$(awk -v d=$(((j1_ut_hw02 + j1_st_hw02) - (j0_ut_hw02 + j0_st_hw02))) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
    local si1_hw01=$(snapshot_si)
    local si1_hw02=$(ssh "$HW02_SSH" "$(declare -f snapshot_si); snapshot_si")
    local rss1_hw01=$(snapshot_rss $SERVER_PORT)
    local rss1_hw02=$(ssh "$HW02_SSH" "$(declare -f snapshot_rss); snapshot_rss $SERVER_PORT")
    # sum across both server nodes
    local core_ut=$(awk -v d=$(((j1_ut_hw01 - j0_ut_hw01) + (j1_ut_hw02 - j0_ut_hw02))) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
    local core_st=$(awk -v d=$(((j1_st_hw01 - j0_st_hw01) + (j1_st_hw02 - j0_st_hw02))) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
    local si=$(awk -v d=$(((si1_hw01 - si0_hw01) + (si1_hw02 - si0_hw02))) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
    local rss=$((((rss1_hw01 + rss1_hw02) + (rss0_hw01 + rss0_hw02)) / 2))

    # === 解析吞吐 ===
    local totals=$(grep "^Totals" "$raw_local" 2>/dev/null | tail -1)
    local nfields=$(echo "$totals" | awk '{print NF}')
    local ops=0 avg_lat=0 p50=0 p99=0 kb=0
    if [ -n "$totals" ]; then
        if [ "$nfields" -ge 11 ]; then
            # memtier Totals: 1=Totals 2=Ops/sec 3=Hits 4=Misses 5=MOVED 6=ASK
            #                 7=Avg 8=p50 9=p99 10=p99.9 11=KB/sec
            ops=$(echo "$totals" | awk '{print $2}')
            avg_lat=$(echo "$totals" | awk '{print $7}')
            p50=$(echo "$totals" | awk '{print $8}')
            p99=$(echo "$totals" | awk '{print $9}')
            kb=$(echo "$totals" | awk '{print $11}')
        elif [ "$nfields" -ge 7 ]; then
            ops=$(echo "$totals" | awk '{print $2}')
            avg_lat=$(echo "$totals" | awk '{print $3}')
            p50=$(echo "$totals" | awk '{print $4}')
            p99=$(echo "$totals" | awk '{print $5}')
            kb=$(echo "$totals" | awk '{print $7}')
        fi
    fi

    # === NIC util ===
    local nic01="N/A" nic02="N/A"
    if [ -s "$sar01" ]; then
        nic01=$(awk -v iface="$NIC_IFACE" '$2==iface && NF>=9 {sum+=$NF; n++} END {if(n>0) printf "%.1f", sum/n; else print "N/A"}' "$sar01" 2>/dev/null)
    fi
    if [ -s "$sar02" ]; then
        nic02=$(awk -v iface="$HW02_NIC_IFACE" '$2==iface && NF>=9 {sum+=$NF; n++} END {if(n>0) printf "%.1f", sum/n; else print "N/A"}' "$sar02" 2>/dev/null)
    fi

    printf "VEMB\tcluster_2node\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
        "$t" "$c" "$p" "$ops" "$avg_lat" "$p50" "$p99" "$kb" "$cores01" "$cores02" "$nic01" "$nic02" \
        "$core_ut" "$core_st" "$si" "$rss" >> "$TSV"
    log "  => ops/s=$ops  avg=${avg_lat}ms  p50=${p50}ms  p99=${p99}ms  hw01_cores=$cores01  hw02_cores=$cores02  hw01_nic=${nic01}%  hw02_nic=${nic02}%"
}

# ============================================================================
# Main
# ============================================================================
log "=== 双节点集群 VEMB sweep (${NCONFIGS} 档) ==="
log "  HW01=$HW01_IP:$SERVER_PORT  HW02=$HW02_IP:$SERVER_PORT  MEMTIER=$MEMTIER_HOST"
log "  DIM=$DIM  NUM_KEYS=$NUM_KEYS  TEST_TIME=${TEST_TIME}s  NIC_IFACE=$NIC_IFACE"
log "  HW01 MANIFEST=$HW01_MANIFEST"
log "  HW02 MANIFEST=$HW02_MANIFEST"

# 前置
setup_client || exit 1
deploy_hw02_manifest || exit 1

# 预热 warm 布局: 对称 manifest 下两台同时启动, 先起的一方读到的是对端上一轮遗留的
# region 头; 布局 (value_size/bytes) 不符即 'warm ub layout init failed' -> dataplane
# 被禁用 -> prefill 0 (20261021 高维实测)。DIM=300 此前能过只因旧头恰好同布局。
# 先起一轮让两台各自把本地 region 头刷成本轮布局, 杀掉后再正式启动, 对端头即正确。
prime_warm_layout() {
    log "=== 预热 warm region 布局 (DIM=$DIM) ==="
    start_server_hw01 || return 1
    start_server_hw02 || return 1
    sleep 2
    stop_servers
}
prime_warm_layout || { log "ABORT: warm layout prime failed"; exit 1; }

# TSV header
printf "op\tserver_type\tt\tc\tpipeline\tops_sec\tavg_lat_ms\tp50_ms\tp99_ms\tkb_sec\thw01_cores\thw02_cores\thw01_nic_pct\thw02_nic_pct\tcore_ut\tcore_st\tsi\trss_kb\n" > "$TSV"

# 主循环
for ((idx=0; idx<NCONFIGS; idx++)); do
    start_server_hw01 || { log "ABORT: HW01 server failed"; exit 1; }
    start_server_hw02 || { log "ABORT: HW02 server failed"; exit 1; }
    publish_topology || { log "ABORT: topology publish failed"; exit 1; }
    prefill
    run_one_config $idx
    stop_servers
done

log "=== DONE ==="
log "TSV : $TSV"
log "raw : $RAWDIR/*.log"
echo "----- summary -----"
column -t -s $'\t' "$TSV"
