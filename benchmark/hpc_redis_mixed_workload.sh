#!/bin/bash
# hpc_redis 混合负载性能测试：读+写混合、读+删混合
#
# 两组实验，均在单实例集成 redis-server（pio=21 snw=21）上执行：
#   场景1 读+写：--ratio=1:1 --key-pattern=S:R（VADD 写新 key + VEMB 读已有 key）
#   场景2 读+删：--vemb-v16-vrem --ratio=1:1 --key-pattern=S:R（VREM 顺序删 + VEMB 随机读）
#
# 用法: PORT=6390 bash benchmark/hpc_redis_mixed_workload.sh
#   smoke: TEST_TIME=3 bash benchmark/hpc_redis_mixed_workload.sh
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
HPC=${HPC:-$(dirname "$SCRIPT_DIR")}
MEMTIER=${MEMTIER:-$(dirname $0)/../memtier_benchmark/memtier_benchmark}
MANIFEST=${MANIFEST:-$HPC/examples/vemb_v16_warm_regions_111.yaml}
CLEAR_UB=${CLEAR_UB:-/tmp/clear_ub_device}

DIM=300
PORT=${PORT:-6390}
PIO=${PIO:-21}
SNW=${SNW:-21}
TEST_TIME=${TEST_TIME:-30}
PIPELINE=${PIPELINE:-32}
MEMTIER_T=${MEMTIER_T:-64}
MEMTIER_C=${MEMTIER_C:-4}
SERVER_CPUSET=${SERVER_CPUSET:-0-95}
CLIENT_CPUSET=${CLIENT_CPUSET:-96-191}

# 场景1：读+写混合
RW_NUM_KEYS=${RW_NUM_KEYS:-100000}
RW_MAX_VECTORS=${RW_MAX_VECTORS:-131072}

# 场景2：读+删混合（prefill 大量避免太快删完）
RD_NUM_KEYS=${RD_NUM_KEYS:-500000}
RD_MAX_VECTORS=${RD_MAX_VECTORS:-1048576}

KEY_PREFIX="item:"
PIDFILE=/tmp/hpc_mixed_server_${PORT}.pid
OUTDIR=${OUTDIR:-/tmp/hpc_mixed_workload}
RAWDIR=$OUTDIR/raw
mkdir -p "$RAWDIR"
TSV=$OUTDIR/summary.tsv

printf 'scene\tops_sec\thits\thit_rate\tp50_ms\tp99_ms\tcpu_cores\tmem_base_mb\tmem_peak_mb\tcore_ut\tcore_st\tsi\trss_kb\n' > "$TSV"

log() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }

kill_server() {
    [ -f "$PIDFILE" ] && { local p; p=$(cat "$PIDFILE" 2>/dev/null); [ -n "$p" ] && kill "$p" 2>/dev/null; sleep 0.5; kill -9 "$p" 2>/dev/null; rm -f "$PIDFILE"; }
    pkill -9 -f "redis-server.*:$PORT " 2>/dev/null
    sleep 0.5
}

wait_port() {
    for _ in $(seq 1 50); do ss -tln | grep -q ":$PORT " && return 0; sleep 0.2; done
    return 1
}

start_server() {
    local max_vec=$1
    local logfile=$RAWDIR/server.log
    kill_server
    [ -x "$CLEAR_UB" ] && "$CLEAR_UB" >/dev/null 2>&1
    cd "$HPC"
    numactl --membind=0 taskset -c $SERVER_CPUSET ./src/redis-server \
        --port $PORT --bind 0.0.0.0 --protected-mode no \
        --vemb-v16-enabled yes --vemb-v16-dim $DIM \
        --vemb-v16-max-vectors $max_vec \
        --vemb-v16-warm-regions-manifest "$MANIFEST" \
        --vemb-v16-reset-warm-regions yes \
        --vemb-v16-proxy-io-threads $PIO --vemb-v16-supernode-workers $SNW \
        --daemonize yes --pidfile $PIDFILE --logfile "$logfile" --loglevel notice \
        >/dev/null 2>&1
    wait_port || { echo "FAIL: server did not listen (see $logfile)"; return 1; }
    SERVER_PID=$(cat "$PIDFILE" 2>/dev/null)
}

get_cpu_jiffies() {
    # 输出 "ut st"（所有线程 utime/stime jiffies 分别求和）
    local pid=$1 ut=0 st=0 f rest
    for f in /proc/$pid/task/*/stat; do
        [ -r "$f" ] || continue
        rest=$(sed 's/.*)//' "$f")
        set -- $rest
        ut=$(( ut + ${12:-0} ))
        st=$(( st + ${13:-0} ))
    done
    echo "$ut $st"
}

snapshot_si() {  # 全机 softirq jiffies (/proc/stat cpu 行第 8 列)
    awk '/^cpu /{print $8}' /proc/stat 2>/dev/null
}

prefill() {
    local num_keys=$1
    local tag=$2
    numactl --membind=1 taskset -c $CLIENT_CPUSET $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
        -s 127.0.0.1 -p $PORT -t 32 -c 4 --pipeline=32 \
        --ratio=1:0 --key-pattern=S:S --key-prefix=$KEY_PREFIX \
        --key-minimum=1 --key-maximum=$num_keys -n $num_keys \
        >$RAWDIR/prefill_${tag}.log 2>&1
}

run_mixed() {
    local scene=$1 ratio=$2 key_pat=$3 num_keys=$4 extra_flags=$5
    local raw=$RAWDIR/${scene}.txt
    local j0_ut j0_st j1_ut j1_st cores core_ut core_st
    local mem_base_mb mem_peak_mb rss_kb
    local si0 si1 c_si

    read j0_ut j0_st < <(get_cpu_jiffies "$SERVER_PID")
    J0_NS=$(date +%s%N)
    si0=$(snapshot_si)
    mem_base_mb=$(awk '/^VmRSS:/{printf "%.0f", $2/1024}' /proc/$SERVER_PID/status 2>/dev/null)

    numactl --membind=1 taskset -c $CLIENT_CPUSET $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
        -s 127.0.0.1 -p $PORT -t $MEMTIER_T -c $MEMTIER_C --pipeline=$PIPELINE \
        --ratio=$ratio --key-pattern=$key_pat $extra_flags \
        --key-prefix=$KEY_PREFIX --key-minimum=1 --key-maximum=$num_keys \
        --test-time=$TEST_TIME >"$raw" 2>&1

    read j1_ut j1_st < <(get_cpu_jiffies "$SERVER_PID")
    J1_NS=$(date +%s%N)
    ELAPSED_NS=$((J1_NS - J0_NS > 0 ? J1_NS - J0_NS : TEST_TIME * 1000000000))
    # core 分母 = 纳秒实测窗口 (与脚本13对齐)
    si1=$(snapshot_si)
    mem_peak_mb=$(awk '/^VmHWM:/{printf "%.0f", $2/1024}' /proc/$SERVER_PID/status 2>/dev/null)
    rss_kb=$(awk '/^VmRSS:/{print $2}' /proc/$SERVER_PID/status 2>/dev/null)
    rss_kb=${rss_kb:-0}
    cores=$(awk -v d=$(( (j1_ut - j0_ut) + (j1_st - j0_st) )) -v tt=$ELAPSED_NS 'BEGIN{ if(d<0) print "NA"; else printf "%.2f", d/100.0/(tt/1000000000) }')
    core_ut=$(awk -v d=$(( j1_ut - j0_ut )) -v tt=$ELAPSED_NS 'BEGIN{ if(d<0) print "NA"; else printf "%.2f", d/100.0/(tt/1000000000) }')
    core_st=$(awk -v d=$(( j1_st - j0_st )) -v tt=$ELAPSED_NS 'BEGIN{ if(d<0) print "NA"; else printf "%.2f", d/100.0/(tt/1000000000) }')
    c_si=$(awk -v d=$(( ${si1:-0} - ${si0:-0} )) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')

    local tot; tot=$(grep '^Totals' "$raw" | tail -1)
    local ops hits p50 p99
    ops=$(echo "$tot" | awk '{print $2}')
    hits=$(echo "$tot" | awk '{print $3}')
    p50=$(echo "$tot" | awk '{print $6}')
    p99=$(echo "$tot" | awk '{print $7}')
    [ -z "$ops" ] && ops=0
    [ -z "$hits" ] && hits=0

    local hit_rate
    hit_rate=$(awk -v o="$ops" -v h="$hits" 'BEGIN{ if(o>0) printf "%.1f%%", h/o*100; else print "N/A" }')

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$scene" "$ops" "$hits" "$hit_rate" "$p50" "$p99" "$cores" "$mem_base_mb" "$mem_peak_mb" "$core_ut" "$core_st" "$c_si" "$rss_kb" >> "$TSV"

    log "$scene: ops=$ops hits=$hits ($hit_rate) p50=$p50 p99=$p99 cores=$cores mem=$mem_base_mb/$mem_peak_mb MB core_ut=$core_ut core_st=$core_st si=$c_si rss=${rss_kb}KB"
}

# ============================================================================
# 场景 1：读+写混合（VEMB 读 + VADD 写）
# ============================================================================
log "场景 1：读+写混合（pio=$PIO snw=$SNW, ratio=1:1, prefill $RW_NUM_KEYS）"
start_server "$RW_MAX_VECTORS" || { echo "ABORT: server start failed"; exit 1; }
sleep 1
log "Prefilling $RW_NUM_KEYS vectors..."
prefill "$RW_NUM_KEYS" "rw"
log "Running read+write mixed for ${TEST_TIME}s..."
run_mixed "read_write" "1:1" "S:R" "$RW_NUM_KEYS" ""
kill_server

# ============================================================================
# 场景 2：读+删混合（VEMB 读 + VREM 删）
# ============================================================================
log "场景 2：读+删混合（pio=$PIO snw=$SNW, ratio=1:1, prefill $RD_NUM_KEYS）"
start_server "$RD_MAX_VECTORS" || { echo "ABORT: server start failed"; exit 1; }
sleep 1
log "Prefilling $RD_NUM_KEYS vectors..."
prefill "$RD_NUM_KEYS" "rd"
log "Running read+delete mixed for ${TEST_TIME}s..."
run_mixed "read_delete" "1:1" "S:R" "$RD_NUM_KEYS" "--vemb-v16-vrem"
kill_server

# ============================================================================
# 汇总
# ============================================================================
log "DONE — $TSV"
cat "$TSV"
