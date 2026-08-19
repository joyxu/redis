#!/bin/bash
set -e

# ============================================================================
# Cross-Node VSIM Benchmark (hpc-redis VEMB V16 --vemb-v16-vsim)
# HW01: redis-server (VEMB V16 TCP, VSIM_INLINE path)
# HW03: memtier_benchmark --protocol vemb_v16 --vemb-v16-vsim (TCP client)
#
# Based on run_cross_node_memtier.sh. Only difference: bench runs VSIM mode
# (hijacks GET path to send VSIM_INLINE opcode 0x30 with fixed query vector).
# Server config / prefill / Step 0 tar-sync are identical.
# ============================================================================

HW01="${HW01:-HW01}"
HW02="${HW02:-HW03}"
SERVER_HOST="${SERVER_HOST:-192.168.1.111}"
SERVER_PORT="${SERVER_PORT:-6379}"
CODE_DIR="/root/gqs/codespace/UnifiedBus"
MEMTIER_DIR="$CODE_DIR/memtier_benchmark"
SERVER_LOG="/tmp/redis_cross_node_vsim.log"
TMUX_SERVER="redis_cross_vsim"

BENCH_DIM=${BENCH_DIM:-300}
VECTOR_STRIDE=$((BENCH_DIM * 4))
# manifest 按 dim 选：300 维用原 manifest，其他维度用 _dim{N} manifest
if [ "$BENCH_DIM" -eq 300 ]; then
    MANIFEST_FILE="examples/vemb_v16_warm_regions_111.yaml"
else
    MANIFEST_FILE="examples/vemb_v16_warm_regions_111_dim${BENCH_DIM}.yaml"
fi
BENCH_TIME=${BENCH_TIME:-60}
NUM_KEYS=10000
KEY_PREFIX="item:"

# t×c ≤ 64：c=20+ 时 proxy 会掉连接（见 vemb_connection_scaling_limit 记忆），
# 用深 pipeline=32 而非堆连接数来榨吞吐。
THREADS_LIST=( ${THREADS_LIST:-4 8 16} )
CLIENTS_PER_THREAD=${CLIENTS_PER_THREAD:-4}
PIPELINE=32

LOCAL_RESULT_DIR="benchmark/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_PREFIX="cross_node_vsim_${TIMESTAMP}"

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

cleanup_hw01() {
    log "Cleaning up HW01 server..."
    # SIGTERM first so server runs shutdown handlers (writes stats log),
    # wait up to 8s for graceful exit, then SIGKILL as last resort.
    ssh "$HW01" "pkill -TERM redis-server 2>/dev/null || true; \
        for i in \$(seq 1 80); do \
            pgrep -x redis-server >/dev/null 2>&1 || break; \
            sleep 0.1; \
        done; \
        pgrep -x redis-server >/dev/null 2>&1 && pkill -9 redis-server 2>/dev/null || true; \
        tmux kill-session -t $TMUX_SERVER 2>/dev/null || true" || true
}

cleanup_hw02() {
    log "Cleaning up HW02 bench..."
    ssh "$HW01" "ssh $HW02 \"pkill -9 memtier_bench 2>/dev/null || true; pkill -9 memtier_benchmark 2>/dev/null || true; rm -f /tmp/cross_node_vsim_*.log /tmp/cross_node_vsim_bench.sh 2>/dev/null || true\"" || true
}

cleanup_mem_sampler() {
    # Kill any lingering mem sampler ssh + remote while-loop. Idempotent.
    if [ -n "$SAMPLER_SSH_PID" ]; then
        kill "$SAMPLER_SSH_PID" 2>/dev/null || true
        wait "$SAMPLER_SSH_PID" 2>/dev/null || true
    fi
    ssh "$HW01" "rm -f /tmp/mem_samples_${RESULT_PREFIX}_*.log" 2>/dev/null || true
}

cleanup_all() {
    cleanup_mem_sampler
    cleanup_hw02
    cleanup_hw01
}

trap cleanup_all EXIT

log "=== Step 0: Build redis-server(HW01) + sync source & build memtier(HW03) ==="
# HW01=aarch64 (server), HW03=x86_64 (memtier client) —— 跨架构不能 scp binary，
# 必须在 HW03 本地编译。但 HW03 源码也 stale（mutagen 只同步到 HW01），所以先
# tar 同步当前源码 HW01→HW03，再 HW03 本地 make。
ssh "$HW01" bash -s << REMOTE_EOF
    set -e
    cd $CODE_DIR/hpc-redis
    echo "Building redis-server & redis-cli on HW01..."
    rm -f src/redis-server
    make -C src redis-server -j\$(nproc)

    echo "Syncing memtier + hpc-redis source HW01 -> HW03 (tar over ssh)..."
    cd $CODE_DIR
    tar -cf - --exclude='.git' --exclude='*.o' --exclude='*.a' \
        --exclude='redis-server' --exclude='redis-cli' --exclude='redis-benchmark' \
        memtier_benchmark \
        | ssh $HW02 "mkdir -p $CODE_DIR && tar -xf - -C $CODE_DIR"
    tar -cf - --exclude='.git' --exclude='*.o' --exclude='*.a' \
        --exclude='redis-server' --exclude='redis-cli' --exclude='redis-benchmark' \
        -C hpc-redis src clients deps/xxhash \
        | ssh $HW02 "mkdir -p $CODE_DIR/hpc-redis && tar -xf - -C $CODE_DIR/hpc-redis"

    echo "Building libvemb_v16_client.a natively on HW03 (x86_64)..."
    ssh $HW02 "cd $CODE_DIR/hpc-redis/clients/c && make clean 2>/dev/null; make -j\\\$(nproc) static install-headers && \
        ls -la build/libvemb_v16_client.a"
    echo "Building memtier_benchmark natively on HW03 (x86_64)..."
    ssh $HW02 "cd $MEMTIER_DIR && rm -f memtier_benchmark && make -j\\\$(nproc) && \
        echo 'HW03 memtier build OK' && uname -m && ls -la memtier_benchmark"
REMOTE_EOF

# Ensure manifest exists on HW01 (mutagen sync may lag for newly created files,
# e.g. dim8 manifest — observed 5min sync delay causing server init failure).
log "Ensuring manifest $MANIFEST_FILE exists on HW01..."
cat "$MANIFEST_FILE" | ssh "$HW01" "mkdir -p $CODE_DIR/hpc-redis/examples && cat > $CODE_DIR/hpc-redis/$MANIFEST_FILE" 2>/dev/null || true

# ============================================================================
# Step 1: Start redis-server on HW01
# ============================================================================
log "=== Step 1: Start redis-server on HW01 ==="

ssh "$HW01" bash -s << REMOTE_EOF
    set -e
    cd $CODE_DIR/hpc-redis

    pkill -9 redis-server 2>/dev/null || true
    tmux kill-session -t $TMUX_SERVER 2>/dev/null || true
    rm -rf /tmp/redis-cross-node-vsim
    mkdir -p /tmp/redis-cross-node-vsim
    sleep 1

    tmux new-session -d -s $TMUX_SERVER
    tmux send-keys -t $TMUX_SERVER "cd $CODE_DIR/hpc-redis && numactl --membind=0 taskset -c 0-95 ./src/redis-server \
        --port $SERVER_PORT --bind 0.0.0.0 --protected-mode no \
        --dir /tmp/redis-cross-node-vsim --save '' --appendonly no \
        --vemb-v16-enabled yes --vemb-v16-dim $BENCH_DIM \
        --vemb-v16-max-vectors 131072 \
        --vemb-v16-warm-regions-manifest $CODE_DIR/hpc-redis/$MANIFEST_FILE \
        --vemb-v16-reset-warm-regions yes \
        --vemb-v16-proxy-io-threads 32 --vemb-v16-supernode-workers 64 \
        --loglevel notice \
        > $SERVER_LOG 2>&1" Enter

    echo "Server started in tmux session: $TMUX_SERVER"
REMOTE_EOF

log "Waiting for server to listen on $SERVER_PORT..."
for i in $(seq 1 60); do
    if ssh "$HW01" "ss -tlnp | grep -q ':$SERVER_PORT'" 2>/dev/null; then
        log "Server is listening."
        break
    fi
    sleep 1
    if [ "$i" -eq 60 ]; then
        log "ERROR: redis-server failed to start"
        ssh "$HW01" "tail -40 $SERVER_LOG 2>/dev/null || true" || true
        exit 1
    fi
done

log "Checking HW02 -> HW01:$SERVER_PORT connectivity..."
if ! ssh "$HW01" "ssh $HW02 \"timeout 3 bash -c ': <>/dev/tcp/$SERVER_HOST/$SERVER_PORT'\"" 2>/dev/null; then
    log "ERROR: HW02 cannot reach HW01 redis-server"
    exit 1
fi
log "HW02 connectivity OK"

# ============================================================================
# Step 2: Prefill on HW01
# ============================================================================
log "=== Step 2: Prefill $NUM_KEYS vectors on HW01 ==="

ssh "$HW01" bash -s << REMOTE_EOF
    set -e
    cd $CODE_DIR/hpc-redis

    PREFILL_LOG=/tmp/vemb_prefill_vsim.log
    rm -f \$PREFILL_LOG

    $CODE_DIR/memtier_benchmark/memtier_benchmark --protocol vemb_v16 --vemb-v16-dim $BENCH_DIM \
        -s 127.0.0.1 -p $SERVER_PORT \
        -n $NUM_KEYS -c 1 -t 1 --ratio=1:0 --pipeline=16 \
        --key-pattern=S:S --key-prefix="$KEY_PREFIX" --key-minimum=1 --key-maximum="$NUM_KEYS" \
        > \$PREFILL_LOG 2>&1

    if ! grep -q "^Totals" \$PREFILL_LOG; then
        echo "ERROR: prefill produced no Totals line"
        tail -40 \$PREFILL_LOG
        exit 1
    fi
    echo "Prefill done"
    tail -3 \$PREFILL_LOG
REMOTE_EOF

# Sanity check: read back one key via VEMB path (confirms prefill landed)
ssh "$HW01" bash -s << SANITY_EOF
    set -e
    cd /root/gqs/codespace/UnifiedBus/memtier_benchmark
    ./memtier_benchmark --protocol vemb_v16 --vemb-v16-dim $BENCH_DIM \
        -s 127.0.0.1 -p 6379 -n 1 -c 1 -t 1 --ratio=0:1 --pipeline=1 \
        --key-prefix="item:" --key-minimum=1 --key-maximum=1 \
        > /tmp/vsim_sanity_check.log 2>&1
    grep -q "^Totals" /tmp/vsim_sanity_check.log
SANITY_EOF
if [ $? -ne 0 ]; then
    log "ERROR: VEMB sanity read failed after prefill"
    ssh "$HW01" "cat /tmp/vsim_sanity_check.log" || true
    exit 1
fi
log "Prefill sanity read OK"

# ============================================================================
# Step 3: Run memtier VSIM benchmark on HW02
# ============================================================================
log "=== Step 3: Run memtier VSIM benchmark on HW02 ==="

if ! ssh "$HW01" "ssh $HW02 \"test -x $MEMTIER_DIR/memtier_benchmark\"" 2>/dev/null; then
    log "ERROR: memtier_benchmark not found at $MEMTIER_DIR/memtier_benchmark on HW02"
    exit 1
fi

# Deploy bench helper script to HW01 (copied to HW02 each iteration)
BENCH_SCRIPT_NAME="cross_node_vsim_bench.sh"
BENCH_SCRIPT_HW01="/tmp/$BENCH_SCRIPT_NAME"

ssh "$HW01" "cat > $BENCH_SCRIPT_HW01" <<'BENCH_SCRIPT'
#!/bin/bash
# 注意：不能用 set -e —— memtier VEMB V16 binary 协议有 test-time 到点不退出的
# 已知 bug，下面用 timeout 兜底强杀，被杀时退出码非零（124/137），set -e 会误判。
mode=$1
threads=$2
clients=$3
test_time=$4
host=$5
port=$6
memtier_dir=$7
dim=$8
key_prefix=$9
key_min=${10}
key_max=${11}
pipeline=${12}
out_file=${13}

cd "$memtier_dir"
if [ "$mode" = "vsim" ]; then
    vsim_flag="--vemb-v16-vsim"
else
    vsim_flag=""
fi

# timeout 兜底：memtier VEMB V16 binary 协议下，test-time 到点后工作循环虽正常
# 完成（progress 停在 100%），但收尾会把每个 op 的延迟逐行 dump（千万级 ops →
# 数百 MB 日志 + 数分钟耗时），期间忽略 SIGTERM。-k 5：先 SIGTERM 给 5s 优雅退出
# （足够写 Totals 行），仍不退就 SIGKILL 强杀。
timeout -k 5 $((test_time + 60)) ./memtier_benchmark --protocol vemb_v16 --vemb-v16-dim "$dim" $vsim_flag \
    -s "$host" -p "$port" --test-time "$test_time" \
    -c "$clients" -t "$threads" --ratio=0:1 --pipeline="$pipeline" \
    --key-prefix="$key_prefix" --key-minimum="$key_min" --key-maximum="$key_max" \
    > "$out_file" 2>&1 || true
BENCH_SCRIPT

VSIM_OPS=()
VSIM_LAT=()
VSIM_P99=()
VSIM_P999=()
VSIM_KB=()
RUN_SEC=()
THREAD_VAL=()
# Per-thread 内存采集（VmRSS，单位 MB）。hpc-redis 的 RSS 不含 warm region
# (pfn-mapped, 见 warm_region_not_in_host_rss 记忆)，warm region 单独作静态列。
MEM_BASE_MB=()
MEM_PEAK_MB=()
MEM_AVG_MB=()
SAMPLER_SSH_PID=""

for threads in "${THREADS_LIST[@]}"; do
    log "--- VSIM threads=$threads clients=$CLIENTS_PER_THREAD pipeline=$PIPELINE time=${BENCH_TIME}s ---"

    vsim_out="/tmp/${RESULT_PREFIX}_vsim_t${threads}.log"
    local_vsim="$LOCAL_RESULT_DIR/${RESULT_PREFIX}_vsim_t${threads}.log"

    ssh "$HW01" "scp $BENCH_SCRIPT_HW01 $HW02:/tmp/$BENCH_SCRIPT_NAME"

    # === 内存采集 - 启动 sampler ===
    SERVER_PID=$(ssh "$HW01" "ss -tlnp 2>/dev/null | grep ':$SERVER_PORT ' | grep -oP 'pid=\K[0-9]+' | head -1")
    BASE_MB=""
    PEAK_MB=""
    AVG_MB=""
    SAMPLES_FILE="/tmp/mem_samples_${RESULT_PREFIX}_t${threads}.log"
    if [ -n "$SERVER_PID" ]; then
        local_base=$(ssh "$HW01" "awk '/^VmRSS:/{print \$2}' /proc/$SERVER_PID/status" 2>/dev/null)
        BASE_MB=$(awk "BEGIN{printf \"%.0f\", ${local_base:-0}/1024}")
        ssh "$HW01" "echo 1 > /proc/$SERVER_PID/clear_refs 2>/dev/null" || true
        ssh "$HW01" "rm -f $SAMPLES_FILE" 2>/dev/null || true
        ssh "$HW01" "while kill -0 $SERVER_PID 2>/dev/null; do awk '/^VmRSS:/{print \$2}' /proc/$SERVER_PID/status; sleep 0.5; done > $SAMPLES_FILE" \
            >/dev/null 2>&1 &
        SAMPLER_SSH_PID=$!
        log "  mem sampling: pid=$SERVER_PID base=${BASE_MB}MB"
    else
        log "  WARN: server PID not found on :$SERVER_PORT, mem sampling skipped"
    fi

    RUN_START=$(get_ts)

    ssh "$HW01" "ssh $HW02 \"bash /tmp/$BENCH_SCRIPT_NAME vsim $threads $CLIENTS_PER_THREAD $BENCH_TIME $SERVER_HOST $SERVER_PORT $MEMTIER_DIR $BENCH_DIM $KEY_PREFIX 1 $NUM_KEYS $PIPELINE $vsim_out\""
    ssh "$HW01" "ssh $HW02 \"cat $vsim_out\"" > "$local_vsim" 2>/dev/null || true

    RUN_END=$(get_ts)

    # === 内存采集 - 停止 sampler + 读结果 ===
    if [ -n "$SERVER_PID" ] && [ -n "$SAMPLER_SSH_PID" ]; then
        sleep 0.7
        kill "$SAMPLER_SSH_PID" 2>/dev/null || true
        wait "$SAMPLER_SSH_PID" 2>/dev/null || true
        SAMPLER_SSH_PID=""
        local_peak=$(ssh "$HW01" "awk '/^VmHWM:/{print \$2}' /proc/$SERVER_PID/status" 2>/dev/null)
        PEAK_MB=$(awk "BEGIN{printf \"%.0f\", ${local_peak:-0}/1024}")
        local_avg=$(ssh "$HW01" "awk '{s+=\$1; n++} END{if(n>0) printf \"%.0f\", s/n}' $SAMPLES_FILE 2>/dev/null")
        AVG_MB=$(awk "BEGIN{printf \"%.0f\", ${local_avg:-0}/1024}")
        ssh "$HW01" "rm -f $SAMPLES_FILE" 2>/dev/null || true
        log "  mem sampling: peak=${PEAK_MB}MB avg=${AVG_MB}MB"
    fi

    run_sec=$(awk "BEGIN {printf \"%.3f\", $RUN_END - $RUN_START}")

    local_out="$local_vsim"

    # memtier 被 timeout 兜底强杀时，最终 Totals 行常缺失；只要 test-time
    # 窗口内吐过 progress 行就能提取 ops/sec / KB/sec。progress 行用 \r 分隔。
    if ! tr '\r' '\n' < "$local_out" 2>/dev/null | grep -qE "^\[RUN #[0-9]+ +[0-9]+%,"; then
        log "ERROR: vsim t$threads produced no progress data"
        tail -40 "$local_out" || true
        exit 1
    fi

    totals=$(tr '\r' '\n' < "$local_out" | grep "^Totals" | tail -1)
    last_progress=$(tr '\r' '\n' < "$local_out" | \
        grep -E "^\[RUN #[0-9]+ +100%," | tail -1)
    if [ -z "$last_progress" ]; then
        last_progress=$(tr '\r' '\n' < "$local_out" | \
            awk '/^\[RUN #[0-9]+ +[0-9]+%,/ { if ($(3)+0 >= 100 && !f) { print; f=1 } }')
    fi
    if [ -z "$last_progress" ]; then
        last_progress=$(tr '\r' '\n' < "$local_out" | \
            grep -E "^\[RUN #[0-9]+ +[0-9]+%," | tail -1)
    fi

    ops=$(echo "$last_progress" | sed -n 's/.*(avg: *\([0-9][0-9]*\)) ops\/sec.*/\1/p')
    if [ -z "$ops" ]; then
        ops=$(echo "$totals" | awk '{print $2}')
    fi
    kb=$(echo "$last_progress" | sed -n 's/.*(avg: *\([0-9.][0-9.]*\)\([MK]\)B\/sec).*/\1\2B\/sec/p')
    if [ -z "$kb" ]; then
        kb=$(echo "$totals" | awk '{print $9}')
    fi
    avg=$(echo "$totals" | awk '{print $5}')
    p99=$(echo "$totals" | awk '{print $7}')
    p999=$(echo "$totals" | awk '{print $8}')

    VSIM_OPS+=("$ops")
    VSIM_LAT+=("$avg")
    VSIM_P99+=("$p99")
    VSIM_P999+=("$p999")
    VSIM_KB+=("$kb")

    log "  vsim t$threads: ops/sec=$ops avg_lat=$avg p99=$p99 p99.9=$p999 KB/sec=$kb"

    THREAD_VAL+=("$threads")
    RUN_SEC+=("$run_sec")
    MEM_BASE_MB+=("${BASE_MB:-N/A}")
    MEM_PEAK_MB+=("${PEAK_MB:-N/A}")
    MEM_AVG_MB+=("${AVG_MB:-N/A}")
done

# ============================================================================
# Step 4: Collect server log
# ============================================================================
log "=== Step 4: Collecting results ==="

ssh "$HW01" "cat $SERVER_LOG" > "$LOCAL_RESULT_DIR/${RESULT_PREFIX}_server_hw01.log" 2>/dev/null || true

# ============================================================================
# Step 5: Print summary
# ============================================================================
SUMMARY_FILE="$LOCAL_RESULT_DIR/${RESULT_PREFIX}_summary.txt"

# 静态值：warm region 配置大小（MB），从 manifest 累加 bytes 字段。
# hpc-redis 的 RSS 不含 warm region，所以这列作为补充列展示预留量。
STATIC_WARM_REGION_MB="N/A"
if [ -f "$MANIFEST_FILE" ]; then
    STATIC_WARM_REGION_MB=$(awk '
        /^warm_regions:/ {in_wr=1; next}
        in_wr && /^[^ ]/ {in_wr=0}
        in_wr && /bytes:/ {gsub(/[^0-9]/, "", $2); sum += $2}
        END {printf "%.0f", sum/1024/1024}
    ' "$MANIFEST_FILE")
fi

log "=== Results Summary ==="
{
    printf "%-8s %12s %12s %12s %12s %12s %12s %10s %10s %10s\n" \
        "t" "ops/sec" "avg_lat" "p99_lat" "p99.9_lat" "KB/sec" "run_time(s)" "base_MB" "peak_MB" "avg_MB"
    for i in "${!THREAD_VAL[@]}"; do
        printf "%-8s %12s %12s %12s %12s %12s %12s %10s %10s %10s\n" \
            "${THREAD_VAL[$i]}" \
            "${VSIM_OPS[$i]:-N/A}" "${VSIM_LAT[$i]:-N/A}" "${VSIM_P99[$i]:-N/A}" "${VSIM_P999[$i]:-N/A}" "${VSIM_KB[$i]:-N/A}" "${RUN_SEC[$i]:-N/A}" \
            "${MEM_BASE_MB[$i]:-N/A}" "${MEM_PEAK_MB[$i]:-N/A}" "${MEM_AVG_MB[$i]:-N/A}"
    done
} | tee "$SUMMARY_FILE"
echo ""
echo "Memory columns (base/peak/avg MB) reflect host-side VmRSS only."
echo "  hpc-redis: RSS does NOT include warm region (pfn-mapped, see warm_region_not_in_host_rss memory)."
echo "            Warm region reserved (from manifest): ${STATIC_WARM_REGION_MB} MB"

echo "Result files:"
ls -la "$LOCAL_RESULT_DIR/${RESULT_PREFIX}"*

echo ""
log "=== All done ==="
