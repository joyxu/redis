#!/bin/bash
# 注意：不能用 set -e —— vanilla redis VSIM 读路径在高并发下偶发 connection reset，
# 一档失败会让整个脚本退出触发 cleanup 杀 server，后续档全挂。用 || true 兜底。

# ============================================================================
# Cross-Node VSIM Baseline Benchmark (native Redis 8.6.3 + memtier_benchmark)
# Based on run_cross_node_redis_baseline.sh. Two differences:
#   1. --command changed from VEMB to "VSIM myvectors ELE __key__"
#   2. NUM_KEYS 100000 → 10000 (match hpc-redis side + local VSIM sweep)
# Server config / io-threads / 绑核 identical.
# ============================================================================

# Topology — override via env vars
JUMP="${JUMP:-HW01}"
SERVER="${SERVER:-HW01}"
CLIENT="${CLIENT:-HW03}"
SERVER_HOST="${SERVER_HOST:-192.168.1.111}"
SERVER_PORT="${SERVER_PORT:-7001}"
CODE_DIR="${CODE_DIR:-/root/gqs/codespace/redis-8.6.3}"
MEMTIER_DIR="${MEMTIER_DIR:-/root/gqs/codespace/UnifiedBus/memtier_benchmark_origin}"
SERVER_LOG="${SERVER_LOG:-/tmp/redis_vsim_baseline_server.log}"

TEST_TIME=${TEST_TIME:-60}
# t×c ≤ 4000：c=20 稳定（c=100 时 400 连接 19s 后崩，见 VEMB baseline 经验）
THREADS_LIST=( ${THREADS_LIST:-20 40 80 120 200} )
CLIENTS_PER_THREAD=${CLIENTS_PER_THREAD:-20}
KEY_MIN=1
KEY_MAX=9999
PIPELINE=16
DIM=${DIM:-300}
NUM_KEYS=10000

LOCAL_RESULT_DIR="benchmark/results/vsim_cross_baseline"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_PREFIX="vsim_cross_baseline_${TIMESTAMP}"

mkdir -p "$LOCAL_RESULT_DIR"

log() {
    echo "[$(date '+%H:%M:%S')] $*"
}

get_ts() {
    if command -v python3 >/dev/null 2>&1; then
        python3 -c 'import time; print("%.9f" % time.time())'
    else
        date +%s
    fi
}

cleanup_server() {
    log "Cleaning up server on $SERVER..."
    ssh "$JUMP" "ssh $SERVER 'pkill redis-server 2>/dev/null || true'" || true
}

cleanup_client() {
    log "Cleaning up bench on $CLIENT..."
    ssh "$JUMP" "ssh $CLIENT \"pkill memtier_bench 2>/dev/null || true; pkill memtier_benchmark 2>/dev/null || true; rm -f /tmp/vsim_cross_baseline_bench.sh /tmp/vsim_cross_baseline_*.log 2>/dev/null || true\"" || true
}

cleanup_mem_sampler() {
    # Kill any lingering mem sampler ssh + remote while-loop on $SERVER.
    if [ -n "$SAMPLER_SSH_PID" ]; then
        kill "$SAMPLER_SSH_PID" 2>/dev/null || true
        wait "$SAMPLER_SSH_PID" 2>/dev/null || true
    fi
    ssh "$JUMP" "ssh $SERVER 'rm -f /tmp/mem_samples_${RESULT_PREFIX}_*.log'" 2>/dev/null || true
}

cleanup_all() {
    cleanup_mem_sampler
    cleanup_client
    cleanup_server
}

trap cleanup_all EXIT

# ============================================================================
# Step 1: Start redis-server baseline on $SERVER
# ============================================================================
log "=== Step 1: Start redis-server baseline on $SERVER ==="

ssh "$JUMP" "ssh $SERVER bash -s" << REMOTE_EOF
    set -e
    cd $CODE_DIR

    pkill redis-server 2>/dev/null || true
    # Wait for old server to release port (RDB save on SIGTERM).
    for i in \$(seq 1 80); do
        pgrep -x redis-server >/dev/null 2>&1 || break
        sleep 0.1
    done
    pgrep -x redis-server >/dev/null 2>&1 && pkill -9 redis-server 2>/dev/null || true
    rm -rf dump.rdb appendonly.aof appendonlydir

    numactl --membind=0 taskset -c 0-95 ./src/redis-server \
        --port $SERVER_PORT \
        --protected-mode no \
        --dir ./ \
        --tcp-keepalive 1800 \
        --timeout 0 \
        --io-threads 16 --io-threads-do-reads yes \
        --daemonize yes \
        --logfile $SERVER_LOG
    sleep 2

    ./src/redis-cli -p $SERVER_PORT FLUSHDB
REMOTE_EOF

log "Waiting for server to listen on $SERVER_PORT..."
for i in $(seq 1 30); do
    if ssh "$JUMP" "ssh $SERVER ss -tlnp" 2>/dev/null | grep -q ":$SERVER_PORT "; then
        log "Server is listening."
        break
    fi
    sleep 1
    if [ "$i" -eq 30 ]; then
        log "ERROR: redis-server failed to start on $SERVER"
        ssh "$JUMP" "ssh $SERVER 'tail -40 $SERVER_LOG 2>/dev/null || true'" || true
        exit 1
    fi
done

log "Checking $CLIENT -> $SERVER:$SERVER_PORT connectivity..."
if ! ssh "$JUMP" "ssh $CLIENT \"timeout 3 bash -c ': <>/dev/tcp/$SERVER_HOST/$SERVER_PORT'\"" 2>/dev/null; then
    log "ERROR: $CLIENT cannot reach $SERVER redis-server"
    exit 1
fi
log "$CLIENT connectivity OK"

# ============================================================================
# Step 2: Prefill vectors on $SERVER
# ============================================================================
log "=== Step 2: Prefill $NUM_KEYS ${DIM}-dim vectors on $SERVER ==="

ssh "$JUMP" "ssh $SERVER bash -s" << REMOTE_EOF
    set -e
    cd $CODE_DIR

    VEC300=\$(seq -s " " 1 $DIM | sed "s/[0-9]*/0.1/g")

    for w in \$(seq 0 3); do
        start=\$((w * 2500))
        end=\$((start + 2499))
        (for i in \$(seq \$start \$end); do
            echo "VADD myvectors VALUES $DIM \$VEC300 item:\$i"
        done | ./src/redis-cli -p $SERVER_PORT --pipe) &
    done
    wait
    echo "Prefill done, VCARD:"
    ./src/redis-cli -p $SERVER_PORT VCARD myvectors
REMOTE_EOF

# ============================================================================
# Step 3: Run memtier VSIM benchmark on $CLIENT
# ============================================================================
log "=== Step 3: Run memtier VSIM benchmark on $CLIENT ==="

if ! ssh "$JUMP" "ssh $CLIENT \"test -x $MEMTIER_DIR/memtier_benchmark\"" 2>/dev/null; then
    log "ERROR: memtier_benchmark not found at $MEMTIER_DIR/memtier_benchmark on $CLIENT"
    exit 1
fi

BENCH_SCRIPT_NAME="vsim_cross_baseline_bench.sh"
BENCH_SCRIPT_JUMP="/tmp/$BENCH_SCRIPT_NAME"

ssh "$JUMP" "cat > $BENCH_SCRIPT_JUMP" <<'BENCH_SCRIPT'
#!/bin/bash
# 不用 set -e：vanilla redis VSIM 高并发下偶发 connection reset，memtier 非零退出
# 是正常的，不能让一档失败中断整轮。
threads=$1
clients=$2
test_time=$3
host=$4
port=$5
memtier_dir=$6
key_min=$7
key_max=$8
pipeline=$9
out_file=${10}

cd "$memtier_dir"
./memtier_benchmark \
    -h "$host" -p "$port" \
    --hide-histogram --test-time="$test_time" --select-db=0 \
    -c "$clients" -t "$threads" --pipeline="$pipeline" \
    --command="VSIM myvectors ELE __key__" \
    --command-key-pattern=R \
    --key-prefix=item: \
    --key-minimum="$key_min" --key-maximum="$key_max" \
    > "$out_file" 2>&1 || true
BENCH_SCRIPT

OPS_SEC=()
AVG_LAT=()
P99_LAT=()
P999_LAT=()
KB_SEC=()
RUN_SEC=()
THREAD_VAL=()
# Per-thread 内存采集（VmRSS，单位 MB）。baseline redis 没有 warm region mmap，
# RSS 就是全部内存（HNSW 图 + 向量数据 + 连接都在普通堆里）。
MEM_BASE_MB=()
MEM_PEAK_MB=()
MEM_AVG_MB=()
SAMPLER_SSH_PID=""

for threads in "${THREADS_LIST[@]}"; do
    log "--- VSIM threads=$threads clients=$CLIENTS_PER_THREAD pipeline=$PIPELINE time=${TEST_TIME}s ---"

    remote_out="/tmp/${RESULT_PREFIX}_t${threads}.log"
    local_out="$LOCAL_RESULT_DIR/${RESULT_PREFIX}_t${threads}.log"

    ssh "$JUMP" "scp $BENCH_SCRIPT_JUMP $CLIENT:/tmp/$BENCH_SCRIPT_NAME"

    # === 内存采集 - 启动 sampler ===（baseline server 在 $SERVER 上，经 $JUMP 跳板）
    SERVER_PID=$(ssh "$JUMP" "ssh $SERVER 'ss -tlnp 2>/dev/null | grep \":$SERVER_PORT \" | grep -oP \"pid=\\K[0-9]+\" | head -1'")
    BASE_MB=""
    PEAK_MB=""
    AVG_MB=""
    SAMPLES_FILE="/tmp/mem_samples_${RESULT_PREFIX}_t${threads}.log"
    if [ -n "$SERVER_PID" ]; then
        local_base=$(ssh "$JUMP" "ssh $SERVER 'awk \"/^VmRSS:/{print \\\$2}\" /proc/$SERVER_PID/status'" 2>/dev/null)
        BASE_MB=$(awk "BEGIN{printf \"%.0f\", ${local_base:-0}/1024}")
        ssh "$JUMP" "ssh $SERVER 'echo 1 > /proc/$SERVER_PID/clear_refs 2>/dev/null'" || true
        ssh "$JUMP" "ssh $SERVER 'rm -f $SAMPLES_FILE'" 2>/dev/null || true
        # sampler：通过 JUMP→SERVER 双层 ssh，本地后台。kill ssh 时整链断开。
        ssh "$JUMP" "ssh $SERVER 'while kill -0 $SERVER_PID 2>/dev/null; do awk \"/^VmRSS:/{print \\\$2}\" /proc/$SERVER_PID/status; sleep 0.5; done > $SAMPLES_FILE'" \
            >/dev/null 2>&1 &
        SAMPLER_SSH_PID=$!
        log "  mem sampling: pid=$SERVER_PID base=${BASE_MB}MB"
    else
        log "  WARN: server PID not found on :$SERVER_PORT, mem sampling skipped"
    fi

    RUN_START=$(get_ts)
    ssh "$JUMP" "ssh $CLIENT \"bash /tmp/$BENCH_SCRIPT_NAME $threads $CLIENTS_PER_THREAD $TEST_TIME $SERVER_HOST $SERVER_PORT $MEMTIER_DIR $KEY_MIN $KEY_MAX $PIPELINE $remote_out\""
    RUN_END=$(get_ts)

    # === 内存采集 - 停止 sampler + 读结果 ===
    if [ -n "$SERVER_PID" ] && [ -n "$SAMPLER_SSH_PID" ]; then
        sleep 0.7
        kill "$SAMPLER_SSH_PID" 2>/dev/null || true
        wait "$SAMPLER_SSH_PID" 2>/dev/null || true
        SAMPLER_SSH_PID=""
        local_peak=$(ssh "$JUMP" "ssh $SERVER 'awk \"/^VmHWM:/{print \\\$2}\" /proc/$SERVER_PID/status'" 2>/dev/null)
        PEAK_MB=$(awk "BEGIN{printf \"%.0f\", ${local_peak:-0}/1024}")
        local_avg=$(ssh "$JUMP" "ssh $SERVER 'awk \"{s+=\\\$1; n++} END{if(n>0) printf \\\"%.0f\\\", s/n}\" $SAMPLES_FILE'" 2>/dev/null)
        AVG_MB=$(awk "BEGIN{printf \"%.0f\", ${local_avg:-0}/1024}")
        ssh "$JUMP" "ssh $SERVER 'rm -f $SAMPLES_FILE'" 2>/dev/null || true
        log "  mem sampling: peak=${PEAK_MB}MB avg=${AVG_MB}MB"
    fi

    ssh "$JUMP" "ssh $CLIENT \"cat $remote_out\"" > "$local_out" 2>/dev/null || true

    run_sec=$(awk "BEGIN {printf \"%.3f\", $RUN_END - $RUN_START}")

    # Totals 行优先（有完整 latency 百分位）；缺失时从 progress 行提取。
    totals=$(grep "^Totals" "$local_out" 2>/dev/null | tail -1)
    last_progress=$(tr '\r' '\n' < "$local_out" 2>/dev/null | \
        grep -E "^\[RUN #[0-9]+ +[0-9]+%," | tail -1)

    if [ -n "$totals" ] && [ "$(echo "$totals" | awk '{print $2}')" != "0.00" ]; then
        ops=$(echo "$totals" | awk '{print $2}')
        avg=$(echo "$totals" | awk '{print $3}')
        p99=$(echo "$totals" | awk '{print $5}')
        p999=$(echo "$totals" | awk '{print $6}')
        kb=$(echo "$totals" | awk '{print $7}')
    elif [ -n "$last_progress" ]; then
        ops=$(echo "$last_progress" | sed -n 's/.*(avg: *\([0-9][0-9]*\)) ops\/sec.*/\1/p')
        avg=$(echo "$last_progress" | sed -n 's/.*(avg: *\([0-9.][0-9.]*\)) msec latency.*/\1/p')
        kb=$(echo "$last_progress" | sed -n 's/.*(avg: *\([0-9.][0-9.]*\)\([MK]\)B\/sec).*/\1\2B\/sec/p')
        p99="N/A"
        p999="N/A"
        log "  WARN: t$threads no valid Totals (connection reset?), extracted from progress line"
    else
        log "ERROR: t$threads produced no usable data"
        tail -20 "$local_out" 2>/dev/null || true
        ops="N/A"; avg="N/A"; p99="N/A"; p999="N/A"; kb="N/A"
    fi

    THREAD_VAL+=("$threads")
    RUN_SEC+=("$run_sec")
    OPS_SEC+=("$ops")
    AVG_LAT+=("$avg")
    P99_LAT+=("$p99")
    P999_LAT+=("$p999")
    KB_SEC+=("$kb")
    MEM_BASE_MB+=("${BASE_MB:-N/A}")
    MEM_PEAK_MB+=("${PEAK_MB:-N/A}")
    MEM_AVG_MB+=("${AVG_MB:-N/A}")

    log "  t$threads: ops/sec=$ops avg_lat=$avg p99=$p99 p99.9=$p999 KB/sec=$kb base=${BASE_MB:-N/A}MB peak=${PEAK_MB:-N/A}MB"
done

# ============================================================================
# Step 4: Collect server log
# ============================================================================
log "=== Step 4: Collecting results ==="

ssh "$JUMP" "ssh $SERVER 'cat $SERVER_LOG'" > "$LOCAL_RESULT_DIR/${RESULT_PREFIX}_server_${SERVER}.log" 2>/dev/null || true

# ============================================================================
# Step 5: Print summary
# ============================================================================
SUMMARY_FILE="$LOCAL_RESULT_DIR/${RESULT_PREFIX}_summary.txt"

log "=== Results Summary ==="
{
    printf "%-8s %12s %12s %12s %12s %12s %12s %10s %10s %10s\n" "threads" "ops/sec" "avg_lat" "p99_lat" "p99.9_lat" "KB/sec" "run_time(s)" "base_MB" "peak_MB" "avg_MB"
    for i in "${!OPS_SEC[@]}"; do
        printf "%-8s %12s %12s %12s %12s %12s %12s %10s %10s %10s\n" \
            "${THREAD_VAL[$i]}" \
            "${OPS_SEC[$i]:-N/A}" \
            "${AVG_LAT[$i]:-N/A}" \
            "${P99_LAT[$i]:-N/A}" \
            "${P999_LAT[$i]:-N/A}" \
            "${KB_SEC[$i]:-N/A}" \
            "${RUN_SEC[$i]:-N/A}" \
            "${MEM_BASE_MB[$i]:-N/A}" \
            "${MEM_PEAK_MB[$i]:-N/A}" \
            "${MEM_AVG_MB[$i]:-N/A}"
    done
} | tee "$SUMMARY_FILE"
echo ""
echo "# Memory 口径（baseline redis）: VmRSS / VmHWM 来自 /proc/$PID/status，"
echo "# base_MB = 档位开始前 RSS（clear_refs 后），peak_MB = VmHWM 本档位峰值，"
echo "# avg_MB = 本档位 0.5s 采样 RSS 均值。baseline 全部在普通堆，RSS 即全部内存。"
echo ""

echo "Result files:"
ls -la "$LOCAL_RESULT_DIR/${RESULT_PREFIX}"*

echo ""
log "=== All done ==="
