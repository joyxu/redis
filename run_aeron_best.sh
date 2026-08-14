#!/bin/bash
# ============================================================================
# run_aeron_best.sh
# Aeron transport (TCP control + UB rings) local loopback VEMB sweep
#
# 使用方法（所有参数都有默认值，按需覆盖）：
#   bash run_aeron_best.sh
#   TEST_TIME=60 T=64 C=4 PIPELINE=32 PIO=21 SNW=21 bash run_aeron_best.sh
#   ssh HW01 'TEST_TIME=60 bash /root/gqs/codespace/UnifiedBus/test_hpc/run_aeron_best.sh'
#
# 可调参数（环境变量）：
#   TEST_TIME     bench 持续秒数         (默认 60)
#   T C           client -t / -c         (默认 64 / 4)
#   PIPELINE      每 channel in-flight   (默认 32)
#   NUM_KEYS      prefill key 数         (默认 10000)
#   MAX_VECTORS   server vector 容量上限 (默认 131072=128K；NUM_KEYS 不能超过这个)
#   DIM           vector 维度            (默认 300)
#   SERVER_MASK   server taskset         (默认 "0-47")
#   PIO           vemb-v16 proxy IO 线程数 (默认 21)
#   SNW           vemb-v16 supernode worker 数 (默认 21)
#   CLIENT_MASK   client taskset         (默认 "96-191")
#   SERVER_HOST   client 连接的 server IP (默认 127.0.0.1)
#   PORT          server 端口            (默认 6395)
#   ROLE          both|server|client     (默认 both)

set -uo pipefail

HPC=${HPC:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)}
REDIS=$HPC/src/redis-server
MEMTIER=$HPC/memtier_benchmark/memtier_benchmark
MANIFEST=${MANIFEST:-$HPC/examples/vemb_v16_warm_regions_111.yaml}

PORT=${PORT:-6395}
SERVER_HOST=${SERVER_HOST:-127.0.0.1}
ROLE=${ROLE:-both}
VERIFY_VEMB_BUILD=${VERIFY_VEMB_BUILD:-yes}
DIM=${DIM:-300}
NUM_KEYS=${NUM_KEYS:-100000}
MAX_VECTORS=${MAX_VECTORS:-131072}
TEST_TIME=${TEST_TIME:-30}
KEY_PREFIX=${KEY_PREFIX:-"item:"}
PIO=${PIO:-8}
SNW=${SNW:-8}
AERON_CONTROL=${AERON_CONTROL:-tcp}
AERON_UB_PATH=${AERON_UB_PATH:-/dev/obmm_shmdev1}
AERON_RESPONSE_UB_PATH=${AERON_RESPONSE_UB_PATH:-/dev/obmm_shmdev2}
AERON_TRANSPORT=${AERON_TRANSPORT:-aeron}
SERVER_TRANSPORT=${SERVER_TRANSPORT:-$AERON_TRANSPORT}
if [ "$SERVER_TRANSPORT" = "tcp" ]; then
    SERVER_TRANSPORT=sniff
fi

VEMB_MODE_ARGS=()
if [ "$AERON_TRANSPORT" = "tcp" ]; then
    VEMB_MODE_ARGS=(--vemb-v16-handle)
fi
VEMB_ENDPOINT_ARGS=()
if [ "$AERON_TRANSPORT" = "aeron-cross-node" ]; then
    VEMB_ENDPOINT_ARGS=(--vemb-v16-endpoints="$SERVER_HOST:$PORT")
fi

SERVER_MASK=${SERVER_MASK:-"0-47"}
CLIENT_MASK=${CLIENT_MASK:-"96-191"}

# === 17 档配置矩阵 ===
TS_DEFAULT=(1 1 1 1  1  2  4  8  16 32 64 64 64 64 64 64 64)
CS_DEFAULT=(1 1 1 1  1  1  1  1  1  1  1  2  4  8  16 32 64)
PS_DEFAULT=(1 4 8 16 32 32 32 32 32 32 32 32 32 32 32 32 32)
TS=( ${TS:-${TS_DEFAULT[*]}} )
CS=( ${CS:-${CS_DEFAULT[*]}} )
PS=( ${PS:-${PS_DEFAULT[*]}} )
NCONFIGS=${#TS[@]}

# === 输出 ===
OUTDIR=${OUTDIR:-benchmark/results/aeron_sweep}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RAWDIR="$OUTDIR/$TIMESTAMP/raw"
TSV="$OUTDIR/$TIMESTAMP/summary.tsv"

PIDFILE=/tmp/vemb_aeron_sweep.pid
SERVER_LOG=/tmp/vemb_aeron_sweep.log
SOCKET=/tmp/vemb_v16.sock

ulimit -n 200000
mkdir -p "$RAWDIR"

log() { echo "[$(date +%H:%M:%S)] $*"; }

verify_vemb_build() {
    [ "$VERIFY_VEMB_BUILD" = "yes" ] || return 0
    case "$ROLE" in
        server) "$HPC/scripts/vemb_v16_build_stamp.sh" verify server ;;
        client) "$HPC/scripts/vemb_v16_build_stamp.sh" verify client ;;
        both)
            "$HPC/scripts/vemb_v16_build_stamp.sh" verify server
            "$HPC/scripts/vemb_v16_build_stamp.sh" verify client
            ;;
        *) return 0 ;;
    esac
}

collect_ub_paths() {
    UB_PATHS=("$AERON_UB_PATH" "$AERON_RESPONSE_UB_PATH")
    [ -r "$MANIFEST" ] || return 0

    while IFS= read -r path; do
        [ -n "$path" ] || continue
        case " ${UB_PATHS[*]} " in
            *" $path "*) ;;
            *) UB_PATHS+=("$path");;
        esac
    done < <(awk '
        /^  - / { provider="" }
        $1 == "provider:" { provider=$2 }
        $1 == "path:" && provider == "ub" { print $2 }
    ' "$MANIFEST")
}

check_ub_paths_in_use() {
    local path holders pid cmd conflict=0

    command -v lsof >/dev/null 2>&1 || {
        echo "FAIL: lsof is required to verify UB path ownership before server start"
        return 1
    }

    for path in "${UB_PATHS[@]}"; do
        [ -n "$path" ] || continue
        holders=$(lsof -t "$path" 2>/dev/null || true)
        [ -n "$holders" ] || continue
        while IFS= read -r pid; do
            [ -n "$pid" ] || continue
            cmd=$(ps -p "$pid" -o args= 2>/dev/null || true)
            echo "FAIL: UB path already in use: $path (pid=$pid${cmd:+ cmd=$cmd})"
            conflict=1
        done < <(printf '%s\n' "$holders" | sort -nu)
    done

    if [ "$conflict" -ne 0 ]; then
        echo "Refusing to start another server with the same UB path."
        return 1
    fi
    return 0
}

get_cpu_jiffies() {
    local pid=$1 sum=0 rest
    for f in /proc/$pid/task/*/stat; do
        [ -r "$f" ] || continue
        rest=$(sed 's/.*)//' "$f")
        set -- $rest
        sum=$(( sum + ${12:-0} + ${13:-0} ))
    done
    echo "$sum"
}

cleanup() {
    [ "$ROLE" = "client" ] && return
    if [ -f "$PIDFILE" ]; then
        local p=$(cat "$PIDFILE" 2>/dev/null)
        [ -n "$p" ] && { kill "$p" 2>/dev/null; sleep 0.3; kill -9 "$p" 2>/dev/null; }
    fi
    pkill -9 -f "redis-server.*:$PORT " 2>/dev/null || true
    rm -f "$SOCKET" "$PIDFILE"
}

# ============================================================================
# Check ownership before cleanup: cleanup deliberately kills the configured
# port, so it must not run before detecting a live process on the same UB path.
if [ "$ROLE" = "both" ] || [ "$ROLE" = "server" ]; then
    collect_ub_paths
    if ! check_ub_paths_in_use; then
        exit 3
    fi
fi

trap cleanup EXIT
cleanup
sleep 0.5

if ! verify_vemb_build; then
    echo "FAIL: VEMB binary does not match current sources"
    exit 4
fi

if [ "$NUM_KEYS" -gt "$MAX_VECTORS" ]; then
    echo "ERROR: NUM_KEYS=$NUM_KEYS > MAX_VECTORS=$MAX_VECTORS"
    exit 2
fi

# ── 启动 server ──
if [ "$ROLE" = "both" ] || [ "$ROLE" = "server" ]; then
    echo "=== start server: mask=$SERVER_MASK pio=$PIO snw=$SNW ==="
    taskset -c "$SERVER_MASK" $REDIS \
        --port $PORT --bind 0.0.0.0 --protected-mode no \
        --vemb-v16-enabled yes --vemb-v16-dim $DIM \
        --vemb-v16-max-vectors $MAX_VECTORS \
        --vemb-v16-warm-regions-manifest "$MANIFEST" \
        --vemb-v16-reset-warm-regions yes \
        --vemb-v16-transport "$SERVER_TRANSPORT" \
        --vemb-v16-aeron-control "$AERON_CONTROL" \
        --vemb-v16-aeron-ub-path "$AERON_UB_PATH" \
        --vemb-v16-aeron-response-ub-path "$AERON_RESPONSE_UB_PATH" \
        --vemb-v16-proxy-io-threads $PIO \
        --vemb-v16-supernode-workers $SNW \
        --daemonize yes --pidfile $PIDFILE --logfile "$SERVER_LOG" --loglevel notice \
        >/dev/null 2>&1

    # Control-plane readiness follows the configured transport. Redis still
    # owns the TCP listener used by Aeron TCP attach and normal cleanup.
    if [ "$AERON_CONTROL" = "uds" ]; then
        for _ in $(seq 1 50); do
            [ -S "$SOCKET" ] && break
            sleep 0.2
        done
        if [ ! -S "$SOCKET" ]; then
            echo "FAIL: UDS socket $SOCKET not ready"
            echo "--- server log tail ---"
            tail -30 "$SERVER_LOG" 2>/dev/null
            exit 1
        fi
    fi

    # Confirm the Redis TCP listener used by Aeron TCP control.
    for _ in $(seq 1 50); do
        ss -tln | grep ":$PORT " >/dev/null && break
        sleep 0.2
    done
    if ! ss -tln | grep ":$PORT " >/dev/null; then
        echo "FAIL: TCP port $PORT not ready"
        echo "--- server log tail ---"
        tail -30 "$SERVER_LOG" 2>/dev/null
        exit 1
    fi
    echo "server up: pid=$(cat $PIDFILE) control=$AERON_CONTROL tcp=$SERVER_HOST:$PORT"
    sleep 1
fi

if [ "$ROLE" = "server" ]; then
    echo "server-only mode: leaving server running at $SERVER_HOST:$PORT"
    trap - EXIT
    exit 0
fi

if [ "$ROLE" != "both" ] && [ "$ROLE" != "client" ]; then
    echo "ERROR: ROLE must be both, server, or client (got $ROLE)"
    exit 2
fi

SRV_PID=$(cat "$PIDFILE" 2>/dev/null)

# ── prefill ──
echo ""
echo "=== prefill: $NUM_KEYS keys, dim=$DIM server=$SERVER_HOST:$PORT ==="
if ! taskset -c "$CLIENT_MASK" $MEMTIER --protocol vemb_v16 --vemb-v16-transport="$AERON_TRANSPORT" \
    "${VEMB_MODE_ARGS[@]}" \
    "${VEMB_ENDPOINT_ARGS[@]}" \
    --vemb-v16-dim $DIM -s $SERVER_HOST -p $PORT \
    -t 1 -c 1 -n $NUM_KEYS --pipeline=32 \
    --ratio=1:0 --key-pattern=S:S \
    --key-prefix=$KEY_PREFIX --key-minimum=1 --key-maximum=$NUM_KEYS \
    > "$RAWDIR/prefill.log" 2>&1; then
    echo "FAIL: Aeron prefill failed"
    tail -80 "$RAWDIR/prefill.log" 2>/dev/null
    exit 1
fi
PREFILL_TOTALS=$(grep "^Totals" "$RAWDIR/prefill.log" | tail -1)
PREFILL_OPS=$(echo "$PREFILL_TOTALS" | awk '{print $2}')
log "prefill done: ${PREFILL_OPS:-N/A} sets/sec"

# === TSV header ===
printf "op\tserver_type\tt\tc\tpipeline\tops_sec\tavg_lat_ms\tp50_ms\tp99_ms\tp99_9_ms\tkb_sec\tcores\tops_per_core\n" > "$TSV"

# === Sweep ===
for ((idx=0; idx<NCONFIGS; idx++)); do
    t=${TS[$idx]}; c=${CS[$idx]}; p=${PS[$idx]}
    log "--- config $((idx+1))/${NCONFIGS}: t=$t c=$c pipeline=$p ---"

    SRV_PID=$(cat $PIDFILE 2>/dev/null)
    J0=0
    [ -n "$SRV_PID" ] && J0=$(get_cpu_jiffies "$SRV_PID")

    if ! taskset -c "$CLIENT_MASK" $MEMTIER --protocol vemb_v16 --vemb-v16-transport="$AERON_TRANSPORT" \
        "${VEMB_MODE_ARGS[@]}" \
        "${VEMB_ENDPOINT_ARGS[@]}" \
        --vemb-v16-dim $DIM -s $SERVER_HOST -p $PORT \
        -t $t -c $c --pipeline=$p \
        --ratio=0:1 --key-pattern=R:R \
        --key-prefix=$KEY_PREFIX --key-minimum=1 --key-maximum=$NUM_KEYS \
        --test-time=$TEST_TIME \
        > "$RAWDIR/t${t}_c${c}_p${p}.log" 2>&1; then
        echo "FAIL: Aeron workload failed: t=$t c=$c pipeline=$p"
        tail -80 "$RAWDIR/t${t}_c${c}_p${p}.log" 2>/dev/null
        exit 1
    fi

    J1=0
    [ -n "$SRV_PID" ] && J1=$(get_cpu_jiffies "$SRV_PID")
    CPU_CORES=$(awk -v d=$((J1 - J0)) -v t=$TEST_TIME -v pid="$SRV_PID" 'BEGIN{ if(pid==""||d<0||t<=0) print "NA"; else printf "%.2f", d/100.0/t }')

    totals=$(grep "^Totals" "$RAWDIR/t${t}_c${c}_p${p}.log" | tail -1)
    read ops avg p50 p99 p999 kb < <(
        echo "$totals" | awk '{if(NF>=9) printf "%s %s %s %s %s %s", $2,$5,$6,$7,$8,$9; else printf "0 NA NA NA NA NA"}'
    )
    OPS_PER_CORE=$(awk -v o="$ops" -v c="$CPU_CORES" 'BEGIN{ if(c=="NA"||c==0||o==0) print "NA"; else printf "%.0f", o/c }')

    printf "VEMB\t%s_local\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
        "$AERON_TRANSPORT" \
        "$t" "$c" "$p" "$ops" "$avg" "$p50" "$p99" "$p999" "$kb" "$CPU_CORES" "$OPS_PER_CORE" >> "$TSV"
    log "  => ops/s=$ops  avg=${avg}ms  p50=${p50}ms  p99=${p99}ms  p99.9=${p999}ms  cores=$CPU_CORES  ops/core=$OPS_PER_CORE"
done

log "=== DONE ==="
log "TSV : $TSV"
log "raw : $RAWDIR/*.log"
echo "----- summary -----"
column -t -s $'\t' "$TSV"
