#!/bin/bash
set -e

# ============================================================================
# Cross-Node Memtier VEMB/VSIM Benchmark Script
# HW01: redis-server (VEMB V16 TCP)
# HW02: memtier_benchmark --protocol vemb_v16 (TCP client)
# ============================================================================

HW01="${HW01:-HW01}"
HW02="${HW02:-HW03}"
SERVER_HOST="${SERVER_HOST:-192.168.1.111}"
SERVER_PORT="${SERVER_PORT:-6379}"
CODE_DIR="/root/gqs/codespace/UnifiedBus"
MEMTIER_DIR="$CODE_DIR/memtier_benchmark"
SERVER_LOG="/tmp/redis_cross_node.log"
TMUX_SERVER="redis_cross"

BENCH_DIM=${BENCH_DIM:-300}
VECTOR_STRIDE=$((BENCH_DIM * 4))
# manifest 按 dim 选：300 维用原 manifest，其他维度用 _dim{N} manifest
if [ "$BENCH_DIM" -eq 300 ]; then
    MANIFEST_FILE="examples/vemb_v16_warm_regions_111.yaml"
else
    MANIFEST_FILE="examples/vemb_v16_warm_regions_111_dim${BENCH_DIM}.yaml"
fi
BENCH_TIME=${BENCH_TIME:-60}
NUM_KEYS=${NUM_KEYS:-10000}
KEY_PREFIX="item:"

# t×c ≤ 64：c=20+ 时 proxy 会掉连接（见 vemb_connection_scaling_limit 记忆），
# 用深 pipeline=32 而非堆连接数来榨吞吐。t16c4=64 是 sweep 数据里的甜点。
THREADS_LIST=( ${THREADS_LIST:-4 8 16} )
CLIENTS_PER_THREAD=${CLIENTS_PER_THREAD:-4}
PIPELINE=32
# 100G NIC 统计：sar -n DEV 1 监听 server 端口所在网卡（默认 eth4=192.168.1.111 mlx5_0）
# 本地回环不需要 NIC 统计；只 100G 跨节点场景启用。
NIC_IFACE=${NIC_IFACE:-eth4}

LOCAL_RESULT_DIR="benchmark/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_PREFIX="cross_node_memtier_${TIMESTAMP}"

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
    ssh "$HW01" "ssh $HW02 \"pkill -9 memtier_bench 2>/dev/null || true; pkill memtier_benchmark 2>/dev/null || true; rm -f /tmp/cross_node_memtier_*.log /tmp/cross_node_memtier_bench.sh 2>/dev/null || true\"" || true
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
# rsync 当前源码 HW01→HW03，再 HW03 本地 make。
# stale memtier 的危害：(1) 帧格式跟当前 server 不兼容 → 全部 read 返 error；
# (2) per-op 延迟直方图 dump bug（11M 行=600MB+，数分钟，表现为脚本"卡住"）。
ssh "$HW01" bash -s << REMOTE_EOF
    set -e
    cd $CODE_DIR/hpc-redis
    echo "Building redis-server & redis-cli on HW01..."
    rm -f src/redis-server
    make -C src redis-server -j\$(nproc)

    echo "Syncing memtier + hpc-redis source HW01 -> HW03 (tar over ssh)..."
    cd $CODE_DIR
    # 排除 memtier_benchmark 二进制本身：HW01 是 ARM，HW02/HW06 可能是 x86_64，
    # 跨架构 binary 复制会导致 "cannot execute binary file"。
    tar -cf - --exclude='.git' --exclude='*.o' --exclude='*.a' \
        --exclude='redis-server' --exclude='redis-cli' --exclude='redis-benchmark' \
        --exclude='memtier_benchmark' --exclude='memtier_benchmark_*' \
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
    # make clean：跨架构复用目录时 .o 是上一次编的 ARM 版，会触发
    # "cannot execute binary file"。强制 clean 后重编。
    ssh $HW02 "cd $MEMTIER_DIR && make clean 2>/dev/null; rm -f memtier_benchmark && make -j\\\$(nproc) && \
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
    rm -rf /tmp/redis-cross-node
    mkdir -p /tmp/redis-cross-node
    sleep 1

    tmux new-session -d -s $TMUX_SERVER
    tmux send-keys -t $TMUX_SERVER "cd $CODE_DIR/hpc-redis && numactl --membind=0 taskset -c 0-95 ./src/redis-server \
        --port $SERVER_PORT --bind 0.0.0.0 --protected-mode no \
        --dir /tmp/redis-cross-node --save '' --appendonly no \
        --vemb-v16-enabled yes --vemb-v16-dim $BENCH_DIM \
        --vemb-v16-max-vectors 131072 \
        --vemb-v16-warm-regions-manifest $CODE_DIR/hpc-redis/$MANIFEST_FILE \
        --vemb-v16-reset-warm-regions yes \
        --vemb-v16-proxy-io-threads ${PIO:-32} --vemb-v16-supernode-workers ${SNW:-64} \
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

    PREFILL_LOG=/tmp/vemb_prefill_memtier.log
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

# Sanity check: read back one key via the same VEMB path used by the benchmark
ssh "$HW01" bash -s << SANITY_EOF
    set -e
    cd /root/gqs/codespace/UnifiedBus/memtier_benchmark
    ./memtier_benchmark --protocol vemb_v16 --vemb-v16-dim $BENCH_DIM \
        -s 127.0.0.1 -p $SERVER_PORT -n 1 -c 1 -t 1 --ratio=0:1 --pipeline=1 \
        --key-prefix="$KEY_PREFIX" --key-minimum=1 --key-maximum=1 \
        > /tmp/vemb_sanity_check.log 2>&1
    grep -q "^Totals" /tmp/vemb_sanity_check.log
SANITY_EOF
if [ $? -ne 0 ]; then
    log "ERROR: VEMB sanity read failed after prefill"
    ssh "$HW01" "cat /tmp/vemb_sanity_check.log" || true
    exit 1
fi
log "Prefill sanity read OK"

# ============================================================================
# Step 3: Run memtier benchmark on HW02
# ============================================================================
log "=== Step 3: Run memtier benchmark on HW02 ==="

if ! ssh "$HW01" "ssh $HW02 \"test -x $MEMTIER_DIR/memtier_benchmark\"" 2>/dev/null; then
    log "ERROR: memtier_benchmark not found at $MEMTIER_DIR/memtier_benchmark on HW02"
    exit 1
fi

# Deploy bench helper script to HW01 (copied to HW02 each iteration)
BENCH_SCRIPT_NAME="cross_node_memtier_bench.sh"
BENCH_SCRIPT_HW01="/tmp/$BENCH_SCRIPT_NAME"

ssh "$HW01" "cat > $BENCH_SCRIPT_HW01" <<'BENCH_SCRIPT'
#!/bin/bash
# 注意：不能用 set -e —— memtier VEMB V16 binary 协议有 test-time 到点不退出的
# 已知 bug（见 hpc_redis_* / redis_baseline_* sweep 脚本），下面用 timeout 兜底
# 强杀，被杀时退出码非零（124/137），set -e 会误判整轮失败。
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
# （足够写 Totals 行），仍不退就 SIGKILL 强杀，保证最多 test_time+25s 结束。
# 数据从 progress 的 100% 行提取（ops/sec/avg_lat/KB），不依赖被截断的 dump。
timeout -k 5 $((test_time + 20)) ./memtier_benchmark --protocol vemb_v16 --vemb-v16-dim "$dim" $vsim_flag \
    -s "$host" -p "$port" --test-time "$test_time" \
    -c "$clients" -t "$threads" --ratio=0:1 --pipeline="$pipeline" \
    --key-prefix="$key_prefix" --key-minimum="$key_min" --key-maximum="$key_max" \
    > "$out_file" 2>&1 || true
BENCH_SCRIPT

VEMB_OPS=()
VSIM_OPS=()
VEMB_LAT=()
VSIM_LAT=()
VEMB_P50=()
VSIM_P50=()
VEMB_P99=()
VSIM_P99=()
VEMB_P999=()
VSIM_P999=()
VEMB_KB=()
VSIM_KB=()
RUN_SEC=()
THREAD_VAL=()
CORES=()
# Per-thread 内存采集（VmRSS，单位 kB）。hpc-redis 的 RSS 不含 warm region
# (pfn-mapped, 见 warm_region_not_in_host_rss 记忆)，warm region 单独作静态列。
MEM_BASE_MB=()
MEM_PEAK_MB=()
MEM_AVG_MB=()
# 网卡利用率（sar -n DEV %ifutil 平均，跨节点场景）
NIC_UTIL=()
SAMPLER_SSH_PID=""

# Upload jiffies helper to HW01（sum utime+stime across all TIDs）
ssh "$HW01" "cat > /tmp/get_jiffies.sh" <<'JIFFIES_EOF'
#!/bin/bash
pid=$1
s=0
for f in /proc/$pid/task/*/stat; do
    [ -r "$f" ] || continue
    r=$(sed 's/.*)//' "$f")
    set -- $r
    s=$((s + ${12:-0} + ${13:-0}))
done
echo $s
JIFFIES_EOF

for threads in "${THREADS_LIST[@]}"; do
    log "--- threads=$threads clients=$CLIENTS_PER_THREAD pipeline=$PIPELINE time=${BENCH_TIME}s ---"

    vemb_out="/tmp/${RESULT_PREFIX}_vemb_t${threads}.log"
    vsim_out="/tmp/${RESULT_PREFIX}_vsim_t${threads}.log"
    local_vemb="$LOCAL_RESULT_DIR/${RESULT_PREFIX}_vemb_t${threads}.log"
    local_vsim="$LOCAL_RESULT_DIR/${RESULT_PREFIX}_vsim_t${threads}.log"

    ssh "$HW01" "scp $BENCH_SCRIPT_HW01 $HW02:/tmp/$BENCH_SCRIPT_NAME"

    # === 内存采集 - 启动 sampler ===
    # server PID 按端口提取（pgrep 不可靠：redis 重写 cmdline 不含参数）
    SERVER_PID=$(ssh "$HW01" "ss -tlnp 2>/dev/null | grep ':$SERVER_PORT ' | grep -oP 'pid=\K[0-9]+' | head -1")
    BASE_MB=""
    PEAK_MB=""
    AVG_MB=""
    SAMPLES_FILE="/tmp/mem_samples_${RESULT_PREFIX}_t${threads}.log"
    if [ -n "$SERVER_PID" ]; then
        BASE_MB=$(ssh "$HW01" "awk '/^VmRSS:/{print \$2}' /proc/$SERVER_PID/status" 2>/dev/null)
        # kB → MB
        BASE_MB=$(awk "BEGIN{printf \"%.0f\", ${BASE_MB:-0}/1024}")
        # 重置 VmHWM = 当前 RSS，bench 后读到的是 bench 期间峰值（不含 prefill）
        ssh "$HW01" "echo 1 > /proc/$SERVER_PID/clear_refs 2>/dev/null" || true
        ssh "$HW01" "rm -f $SAMPLES_FILE" 2>/dev/null || true
        # 后台 sampler：ssh 在本地后台跑，bench 结束时 kill 它。
        # 远端 while 循环 ssh 断开自动退出（SIGHUP）。
        ssh "$HW01" "while kill -0 $SERVER_PID 2>/dev/null; do awk '/^VmRSS:/{print \$2}' /proc/$SERVER_PID/status; sleep 0.5; done > $SAMPLES_FILE" \
            >/dev/null 2>&1 &
        SAMPLER_SSH_PID=$!
        log "  mem sampling: pid=$SERVER_PID base=${BASE_MB}MB"
    else
        log "  WARN: server PID not found on :$SERVER_PORT, mem sampling skipped"
    fi

    RUN_START=$(get_ts)
    J0=$(ssh "$HW01" "bash /tmp/get_jiffies.sh $SERVER_PID" 2>/dev/null)

    # === NIC 利用率采集（sar -n DEV 1 后台跑，bench 期间累积）===
    # sar 在 server (HW01) 后台启动，跑 (BENCH_TIME+5) 秒后自动退出；
    # 输出到 /tmp/sar_nic_${RESULT_PREFIX}_t${threads}.log，结束后 awk 提取 eth4 %ifutil 平均。
    sar_log="/tmp/sar_nic_${RESULT_PREFIX}_t${threads}.log"
    ssh "$HW01" "rm -f $sar_log; nohup sar -n DEV 1 $((BENCH_TIME + 5)) > $sar_log 2>&1 &" 2>/dev/null
    log "  sar -n DEV 1 started on HW01 (iface=$NIC_IFACE, sampling ${BENCH_TIME}s)"

    # VEMB
    ssh "$HW01" "ssh $HW02 \"bash /tmp/$BENCH_SCRIPT_NAME vemb $threads $CLIENTS_PER_THREAD $BENCH_TIME $SERVER_HOST $SERVER_PORT $MEMTIER_DIR $BENCH_DIM $KEY_PREFIX 1 $NUM_KEYS $PIPELINE $vemb_out\""
    ssh "$HW01" "ssh $HW02 \"cat $vemb_out\"" > "$local_vemb" 2>/dev/null || true

    # VSIM (disabled — pure VEMB mode)
    # ssh "$HW01" "ssh $HW02 \"bash /tmp/$BENCH_SCRIPT_NAME vsim $threads $CLIENTS_PER_THREAD $BENCH_TIME $SERVER_HOST $SERVER_PORT $MEMTIER_DIR $BENCH_DIM $KEY_PREFIX 1 $NUM_KEYS $PIPELINE $vsim_out\""
    # ssh "$HW01" "ssh $HW02 \"cat $vsim_out\"" > "$local_vsim" 2>/dev/null || true

    RUN_END=$(get_ts)
    J1=$(ssh "$HW01" "bash /tmp/get_jiffies.sh $SERVER_PID" 2>/dev/null)

    # === 内存采集 - 停止 sampler + 读结果 ===
    if [ -n "$SERVER_PID" ] && [ -n "$SAMPLER_SSH_PID" ]; then
        sleep 0.7  # 等最后一份样本落地
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

    for mode in vemb; do
        if [ "$mode" = "vemb" ]; then
            local_out="$local_vemb"
        else
            local_out="$local_vsim"
        fi

        # memtier 被 timeout 兜底强杀时，最终 Totals 行常缺失（test-time 到点
        # 不退出 bug 的表现）；只要 test-time 窗口内吐过 progress 行就能提取
        # ops/sec / KB/sec。progress 行用 \r 分隔，先转 \n。
        if ! tr '\r' '\n' < "$local_out" 2>/dev/null | grep -qE "^\[RUN #[0-9]+ +[0-9]+%,"; then
            log "ERROR: $mode t$threads produced no progress data"
            tail -40 "$local_out" || true
            exit 1
        fi

        # Memtier prints a Totals line whose instantaneous fields (ops/sec,
        # KB/sec) are 0 when not every thread is still active at the end of
        # the run — the cumulative averages live in the last progress line
        # like "[RUN #1 100%, 60 secs]  2 threads: 1719276 ops, 0 (avg: 28647) ops/sec, ...".
        # Pull ops/sec + KB/sec from that last progress line (most accurate),
        # and latency percentiles from Totals (those are cumulative).
        # Progress updates are separated by \r (carriage return); convert to
        # \n so grep/sed/tail see them as separate lines.
        totals=$(tr '\r' '\n' < "$local_out" | grep "^Totals" | tail -1)
        # Pick the highest-percent complete line, ignoring the post-run
        # 101% "0 threads" tail that shows partial-shutdown numbers.
        last_progress=$(tr '\r' '\n' < "$local_out" | \
            grep -E "^\[RUN #[0-9]+ +100%," | tail -1)
        if [ -z "$last_progress" ]; then
            # test-time bug 下百分比常跳过正好 100%（直接 102%→280%）；
            # 取第一个 ≥100% 的行——它最接近 test-time 完成点，avg 最准。
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
        # KB/sec avg from progress line "(avg: 44.04MB/sec)".
        # Match "(avg: N.MB/sec)" anywhere — bandwidth avg is the only
        # (avg:...) group whose unit ends in B/sec.
        kb=$(echo "$last_progress" | sed -n 's/.*(avg: *\([0-9.][0-9.]*\)\([MK]\)B\/sec).*/\1\2B\/sec/p')
        if [ -z "$kb" ]; then
            kb=$(echo "$totals" | awk '{print $9}')
        fi
        avg=$(echo "$totals" | awk '{print $5}')
        p50=$(echo "$totals" | awk '{print $6}')
        p99=$(echo "$totals" | awk '{print $7}')
        p999=$(echo "$totals" | awk '{print $8}')

        if [ "$mode" = "vemb" ]; then
            VEMB_OPS+=("$ops")
            VEMB_LAT+=("$avg")
            VEMB_P50+=("$p50")
            VEMB_P99+=("$p99")
            VEMB_P999+=("$p999")
            VEMB_KB+=("$kb")
        else
            VSIM_OPS+=("$ops")
            VSIM_LAT+=("$avg")
            VSIM_P50+=("$p50")
            VSIM_P99+=("$p99")
            VSIM_P999+=("$p999")
            VSIM_KB+=("$kb")
        fi

        log "  $mode t$threads: ops/sec=$ops avg_lat=$avg p50=$p50 p99=$p99 p99.9=$p999 KB/sec=$kb"
    done

    cores=$(awk "BEGIN{ if(\"${J0:-}\"==\"\" || \"${J1:-}\"==\"\") print \"NA\"; else printf \"%.2f\", (${J1:-0}-${J0:-0})/100.0/${BENCH_TIME} }")

    # === NIC 利用率解析 ===
    # sar 输出每行: HH:MM:SS IFACE rxpck/s txpck/s rxkB/s txkB/s ... %ifutil
    # 取 IFACE==NIC_IFACE 的行，对最后一列 %ifutil 求平均
    sleep 2  # 等 sar flush 最后一个样本
    nic_util=$(ssh "$HW01" "awk '\$2==\"$NIC_IFACE\" && NF>=9 {sum+=\$NF; n++} END {if(n>0) printf \"%.1f\", sum/n; else print \"0.0\"}' $sar_log 2>/dev/null" 2>/dev/null)
    [ -z "$nic_util" ] && nic_util="N/A"
    log "  NIC $NIC_IFACE util: ${nic_util}%"
    NIC_UTIL+=("$nic_util")

    THREAD_VAL+=("$threads")
    RUN_SEC+=("$run_sec")
    CORES+=("$cores")
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
    printf "%-8s %-6s %14s %12s %12s %12s %12s %14s %10s %8s %10s %10s %10s %10s\n" \
        "mode" "t" "ops/sec" "avg_lat" "p50_lat" "p99_lat" "p99.9_lat" "KB/sec" "run_time(s)" "cores" "NIC_util%" "base_MB" "peak_MB" "avg_MB"
    for i in "${!THREAD_VAL[@]}"; do
        printf "%-8s %-6s %14s %12s %12s %12s %12s %14s %10s %8s %10s %10s %10s %10s\n" \
            "VEMB" "${THREAD_VAL[$i]}" \
            "${VEMB_OPS[$i]:-N/A}" "${VEMB_LAT[$i]:-N/A}" "${VEMB_P50[$i]:-N/A}" "${VEMB_P99[$i]:-N/A}" "${VEMB_P999[$i]:-N/A}" "${VEMB_KB[$i]:-N/A}" "${RUN_SEC[$i]:-N/A}" \
            "${CORES[$i]:-N/A}" "${NIC_UTIL[$i]:-N/A}" "${MEM_BASE_MB[$i]:-N/A}" "${MEM_PEAK_MB[$i]:-N/A}" "${MEM_AVG_MB[$i]:-N/A}"
    done
} | tee "$SUMMARY_FILE"
echo ""
echo "Memory columns (base/peak/avg MB) reflect host-side VmRSS only."
echo "  hpc-redis: RSS does NOT include warm region (pfn-mapped, see warm_region_not_in_host_rss memory)."
echo "            Warm region reserved (from manifest): ${STATIC_WARM_REGION_MB} MB"
echo "  baseline:  RSS is total memory (no mmap, all in normal heap)."

echo "Result files:"
ls -la "$LOCAL_RESULT_DIR/${RESULT_PREFIX}"*

echo ""
log "=== All done ==="
