#!/bin/bash
# ============================================================================
# run_aeron_best.sh
# Aeron transport (TCP control + UB rings) local loopback VEMB sweep
# Default UB layout: request ring on obmm_shmdev1, warm region on
# obmm_shmdev2 (from the default manifest), response ring on obmm_shmdev3.
#
# 使用方法（所有参数都有默认值，按需覆盖）：
#   bash run_aeron_best.sh
#   TEST_TIME=60 T=64 C=4 PIPELINE=32 PIO=21 SNW=21 bash run_aeron_best.sh
#   ssh HW01 'TEST_TIME=60 bash /root/gqs/codespace/UnifiedBus/test_hpc/run_aeron_best.sh'
#
# 可调参数（环境变量）：
#   TEST_TIME     bench 持续秒数         (默认 30)
#   TS CS PS      memtier -t/-c/--pipeline (默认 17 档数组，见 TS_DEFAULT/CS_DEFAULT/PS_DEFAULT)
#   NUM_KEYS      prefill key 数         (默认 100000)
#   MAX_VECTORS   server vector 容量上限 (默认 131072=128K；NUM_KEYS 不能超过这个)
#   DIM           vector 维度            (默认 300)
#   SERVER_MASK   server taskset         (默认 "0-47")
#   PIO           vemb-v16 proxy IO 线程数 (默认 21)
#   SNW           vemb-v16 supernode worker 数 (默认 21)
#   CLIENT_MASK   client taskset         (默认 "96-191")
#   SERVER_HOST   client 连接的 server IP (默认 127.0.0.1)
#   PORT          server 端口            (默认 6395)
#   ROLE          both|server|client     (默认 both)
#   AERON_BATCH_DISABLE=yes  强制使用 v1 channel
#   PROFILE=0|1   启用/禁用 server、client perf 和 SVG 火焰图 (默认 0)
#   SERVER_FLAME_DURATION perf 采样时间 (默认 25)
#   FREQ/EVENT      perf 频率/事件 (默认 99/cycles)
#   OUTDIR          结果目录 (默认 perf/aeron_sweep)
#   REMOTE_OUTDIR   远端临时结果目录 (默认 /tmp/aeron_sweep)

set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
HPC=${HPC:-$(cd "$SCRIPT_DIR/.." && pwd -P)}
REMOTE_NODE=${REMOTE_NODE:-43.154.145.18}
REMOTE_SSH_PORT=${REMOTE_SSH_PORT:-8112}
REMOTE_ROOT=${REMOTE_ROOT:-/root/szz/codespace/hpc-redis}
REMOTE_OUTDIR=${REMOTE_OUTDIR:-/tmp/aeron_sweep}
RUN_LOCAL=${RUN_LOCAL:-0}
REMOTE_CHILD=${AERON_BEST_REMOTE_CHILD:-0}
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
PROFILE=${PROFILE:-0}
SERVER_FLAME_DURATION=${SERVER_FLAME_DURATION:-25}
FREQ=${FREQ:-99}
EVENT=${EVENT:-cycles}
FLAMEGRAPH_DIR=${FLAMEGRAPH_DIR:-$HPC/perf/FlameGraph}
KEY_PREFIX=${KEY_PREFIX:-"item:"}
PIO=${PIO:-8}
SNW=${SNW:-8}
AERON_UB_PATH=${AERON_UB_PATH:-/dev/obmm_shmdev1}
AERON_RESPONSE_UB_PATH=${AERON_RESPONSE_UB_PATH:-/dev/obmm_shmdev3}
AERON_TRANSPORT=${AERON_TRANSPORT:-aeron}
AERON_BATCH_DISABLE=${AERON_BATCH_DISABLE:-no}
AERON_UB_CACHEABLE=${AERON_UB_CACHEABLE:-0}
AERON_PEER_VIEW_MANIFEST=${AERON_PEER_VIEW_MANIFEST:-$HPC/examples/vemb_v16_ub_peer_view_local_111.yaml}
AERON_PEER_VIEW_CLIENT_HOST=${AERON_PEER_VIEW_CLIENT_HOST:-local}
AERON_PEER_VIEW_OWNER_ID=${AERON_PEER_VIEW_OWNER_ID:-0}
SERVER_TRANSPORT=${SERVER_TRANSPORT:-$AERON_TRANSPORT}
case "$AERON_UB_CACHEABLE" in
    0) AERON_UB_CACHEABLE_CONFIG=no ;;
    1) AERON_UB_CACHEABLE_CONFIG=yes ;;
    *) echo "ERROR: AERON_UB_CACHEABLE must be 0 or 1"; exit 2 ;;
esac
export VEMB_V16_AERON_UB_CACHEABLE="$AERON_UB_CACHEABLE"
if [ "$SERVER_TRANSPORT" = "tcp" ]; then
    SERVER_TRANSPORT=sniff
fi

VEMB_MODE_ARGS=()
if [ "$AERON_TRANSPORT" = "tcp" ]; then
    VEMB_MODE_ARGS=(--vemb-v16-handle)
fi
VEMB_BATCH_ARGS=()
if [ "$AERON_BATCH_DISABLE" = "yes" ]; then
    VEMB_BATCH_ARGS=(--vemb-v16-batch-disable)
fi
VEMB_ENDPOINT_ARGS=()
VEMB_PEER_VIEW_ARGS=()
if [ "$AERON_TRANSPORT" = "aeron" ] && [ "$ROLE" != "server" ]; then
    if [ -z "$AERON_PEER_VIEW_MANIFEST" ] ||
       [ -z "$AERON_PEER_VIEW_CLIENT_HOST" ] ||
       [ -z "$AERON_PEER_VIEW_OWNER_ID" ]; then
        echo "ERROR: Aeron requires AERON_PEER_VIEW_MANIFEST, AERON_PEER_VIEW_CLIENT_HOST, and AERON_PEER_VIEW_OWNER_ID"
        exit 2
    fi
    VEMB_PEER_VIEW_ARGS=(
        --vemb-v16-ub-peer-view-manifest="$AERON_PEER_VIEW_MANIFEST"
        --vemb-v16-ub-peer-view-client-host="$AERON_PEER_VIEW_CLIENT_HOST"
        --vemb-v16-ub-peer-view-owner-id="$AERON_PEER_VIEW_OWNER_ID"
    )
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
OUTDIR=${OUTDIR:-perf/aeron_sweep}
RUN_TIMESTAMP=${RUN_TIMESTAMP:-}
TIMESTAMP=${RUN_TIMESTAMP:-$(date +%Y%m%d_%H%M%S)}
RAWDIR="$OUTDIR/$TIMESTAMP/raw"
TSV="$OUTDIR/$TIMESTAMP/summary.tsv"

PIDFILE=/tmp/vemb_aeron_sweep.pid
SERVER_LOG=/tmp/vemb_aeron_sweep.log
SERVER_PERF_PID=
CLIENT_PERF_DATA="$RAWDIR/client.perf.data"

run_remote_same_host() {
    local peer="root@$REMOTE_NODE"
    local local_outdir remote_outdir local_run_dir remote_run_dir
    case "$OUTDIR" in
        /*) local_outdir="$OUTDIR" ;;
        *) local_outdir="$HPC/$OUTDIR" ;;
    esac
    case "$REMOTE_OUTDIR" in
        /*) remote_outdir="$REMOTE_OUTDIR" ;;
        *) remote_outdir="$REMOTE_ROOT/$REMOTE_OUTDIR" ;;
    esac
    local_run_dir="$local_outdir/$TIMESTAMP"
    remote_run_dir="$remote_outdir/$TIMESTAMP"
    local remote_env_names=(
        NUM_KEYS KEY_PREFIX MAX_VECTORS DIM TEST_TIME PROFILE
        SERVER_FLAME_DURATION FREQ EVENT FLAMEGRAPH_DIR PIO SNW
        SERVER_MASK CLIENT_MASK PORT SERVER_HOST ROLE VERIFY_VEMB_BUILD
        AERON_UB_PATH AERON_RESPONSE_UB_PATH AERON_TRANSPORT
        AERON_BATCH_DISABLE AERON_UB_CACHEABLE AERON_PEER_VIEW_MANIFEST
        AERON_PEER_VIEW_CLIENT_HOST AERON_PEER_VIEW_OWNER_ID SERVER_TRANSPORT
        TS CS PS
    )
    local remote_args=("$REMOTE_ROOT" "$TIMESTAMP")
    local name value
    for name in "${remote_env_names[@]}"; do
        if [ "${!name+x}" = x ]; then
            case "$name" in
                TS) value="${TS[*]}" ;;
                CS) value="${CS[*]}" ;;
                PS) value="${PS[*]}" ;;
                *) value=${!name} ;;
            esac
            case "$name" in
                AERON_PEER_VIEW_MANIFEST|FLAMEGRAPH_DIR)
                    case "$value" in
                        "$HPC"/*) value="$REMOTE_ROOT/${value#"$HPC/"}" ;;
                    esac
                    ;;
            esac
            remote_args+=("$name" "$value")
        fi
    done
    remote_args+=("OUTDIR" "$remote_outdir")

    printf '[%s] same-host remote run: %s:%s:%s\n' \
        "$(date '+%F %T')" "$peer" "$REMOTE_SSH_PORT" "$REMOTE_ROOT"
    # SSH concatenates remote command arguments and reparses them remotely;
    # quote each value so array settings such as TS/CS/PS stay one argument.
    local remote_command
    printf -v remote_command '%q ' "${remote_args[@]}"
    if ! ssh -p "$REMOTE_SSH_PORT" "$peer" "bash -s -- $remote_command" <<'REMOTE_SAME_HOST' \
        2>&1 | sed '/^Authorized users only\. All activities may be monitored and reported\.$/d'
set -euo pipefail
root=$1
timestamp=$2
shift 2
while [ "$#" -gt 0 ]; do
    name=$1
    value=$2
    shift 2
    export "$name=$value"
done
export AERON_BEST_REMOTE_CHILD=1
export RUN_TIMESTAMP="$timestamp"
cd "$root"
exec bash scripts/run_aeron_best.sh
REMOTE_SAME_HOST
    then
        echo "ERROR: remote same-host workload failed" >&2
        return 1
    fi

    mkdir -p "$local_outdir"
    if ! scp -P "$REMOTE_SSH_PORT" -r \
        "$peer:$remote_run_dir" "$local_outdir/" 2>&1 | \
        sed '/^Authorized users only\. All activities may be monitored and reported\.$/d'; then
        echo "ERROR: failed to pull remote results: $remote_run_dir" >&2
        return 1
    fi
    printf '[%s] pulled: %s\n' "$(date '+%F %T')" "$local_run_dir"
    if [ "$PROFILE" = 1 ]; then
        printf 'server SVG: %s/raw/server.svg\n' "$local_run_dir"
        printf 'client SVG: %s/raw/client.svg\n' "$local_run_dir"
        printf 'server raw: %s/raw/server.perf.data\n' "$local_run_dir"
        printf 'client raw: %s/raw/client.perf.data\n' "$local_run_dir"
    else
        printf 'perf/SVG: disabled (PROFILE=0)\n'
    fi
    printf 'summary TSV: %s/summary.tsv\n' "$local_run_dir"
}

ulimit -n 200000
mkdir -p "$RAWDIR"

log() { echo "[$(date +%H:%M:%S)] $*"; }

verify_vemb_build() {
    [ "$VERIFY_VEMB_BUILD" = "yes" ] || return 0
    case "$ROLE" in
        server) "$HPC/scripts/vemb_v16_build_stamp.sh" verify server ;;
        client) "$HPC/scripts/vemb_v16_build_stamp.sh" verify client ;;
        both)
            "$HPC/scripts/vemb_v16_build_stamp.sh" verify server || return 1
            "$HPC/scripts/vemb_v16_build_stamp.sh" verify client
            ;;
        *) return 0 ;;
    esac
}

tcp_listener_ready() {
    ss -H -ltn | awk -v port="$PORT" '$4 ~ (":" port "$") { found = 1 } END { exit !found }'
}

validate_profile() {
    case "$PROFILE" in
        0|1) ;;
        *) echo "ERROR: PROFILE must be 0 or 1" >&2; exit 2 ;;
    esac
    case "$SERVER_FLAME_DURATION" in
        ''|*[!0-9]*) echo "ERROR: SERVER_FLAME_DURATION must be a positive integer" >&2; exit 2 ;;
    esac
    [ "$SERVER_FLAME_DURATION" -gt 0 ] || {
        echo "ERROR: SERVER_FLAME_DURATION must be positive" >&2
        exit 2
    }
    [ "$SERVER_FLAME_DURATION" -lt "$TEST_TIME" ] || {
        echo "ERROR: SERVER_FLAME_DURATION must be less than TEST_TIME" >&2
        exit 2
    }
    if [ "$PROFILE" = 1 ]; then
        [ "$ROLE" = both ] || {
            echo "ERROR: PROFILE=1 requires ROLE=both" >&2
            exit 2
        }
        command -v perf >/dev/null 2>&1 || {
            echo "ERROR: PROFILE=1 requires perf" >&2
            exit 2
        }
        if [ ! -x "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" ] ||
           [ ! -x "$FLAMEGRAPH_DIR/flamegraph.pl" ]; then
            if [ -x /root/FlameGraph/stackcollapse-perf.pl ] &&
               [ -x /root/FlameGraph/flamegraph.pl ]; then
                echo "NOTICE: $FLAMEGRAPH_DIR is unavailable; using /root/FlameGraph" >&2
                FLAMEGRAPH_DIR=/root/FlameGraph
            fi
        fi
        [ -x "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" ] || {
            echo "ERROR: missing $FLAMEGRAPH_DIR/stackcollapse-perf.pl" >&2
            exit 2
        }
        [ -x "$FLAMEGRAPH_DIR/flamegraph.pl" ] || {
            echo "ERROR: missing $FLAMEGRAPH_DIR/flamegraph.pl" >&2
            exit 2
        }
    fi
}

start_server_perf() {
    [ "$PROFILE" = 1 ] || return 0
    [ "$ROLE" != client ] || return 0
    local tids
    ps -T -p "$SRV_PID" -o tid=,comm= >"$RAWDIR/server.threads.txt" || {
        echo "FAIL: unable to enumerate redis-server TIDs" >&2
        exit 1
    }
    tids=$(awk '{ printf "%s%s", sep, $1; sep="," }' \
        "$RAWDIR/server.threads.txt")
    [ -n "$tids" ] || {
        echo "FAIL: redis-server has no sampleable TIDs" >&2
        exit 1
    }
    nohup perf record -F "$FREQ" -g -e "$EVENT" --tid "$tids" \
        -o "$RAWDIR/server.perf.data" -- sleep "$SERVER_FLAME_DURATION" \
        >"$RAWDIR/server.perf.log" 2>&1 &
    SERVER_PERF_PID=$!
    printf '%s\n' "$SERVER_PERF_PID" >"$RAWDIR/server.perf.pid"
    log "server perf: tids=$(echo "$tids" | tr ',' ' ' | wc -w | tr -d ' ') duration=${SERVER_FLAME_DURATION}s pid=$SERVER_PERF_PID"
}

wait_server_perf() {
    [ "$PROFILE" = 1 ] || return 0
    [ -n "$SERVER_PERF_PID" ] || return 0
    if ! wait "$SERVER_PERF_PID"; then
        echo "FAIL: server perf record failed" >&2
        tail -80 "$RAWDIR/server.perf.log" 2>/dev/null
        exit 1
    fi
}

render_flamegraphs() {
    [ "$PROFILE" = 1 ] || return 0

    [ -s "$RAWDIR/server.perf.data" ] || {
        echo "FAIL: server perf data is empty" >&2
        exit 1
    }
    perf script -i "$RAWDIR/server.perf.data" >"$RAWDIR/server.perf.script"
    grep -q 'redis-server' "$RAWDIR/server.perf.script" || {
        echo "FAIL: server perf script has no redis-server samples" >&2
        exit 1
    }
    "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" "$RAWDIR/server.perf.script" \
        >"$RAWDIR/server.collapsed.txt"
    awk '{ sub(/^[^;]+;/, "all;redis-server;"); print }' \
        "$RAWDIR/server.collapsed.txt" >"$RAWDIR/server.process.collapsed.txt"
    "$FLAMEGRAPH_DIR/flamegraph.pl" \
        --title "vemb-v16 local server $TIMESTAMP" \
        --subtitle "all redis-server TIDs user+kernel; event=${EVENT}@${FREQ}Hz" \
        "$RAWDIR/server.process.collapsed.txt" >"$RAWDIR/server.svg"
    grep -q 'vemb-v16 local server' "$RAWDIR/server.svg" || {
        echo "FAIL: server flamegraph validation failed" >&2
        exit 1
    }

    [ -s "$CLIENT_PERF_DATA" ] || {
        echo "FAIL: client perf data is empty" >&2
        exit 1
    }
    perf script -i "$CLIENT_PERF_DATA" >"$RAWDIR/client.perf.script"
    grep -q 'memtier_benchmark' "$RAWDIR/client.perf.script" || {
        echo "FAIL: client perf script has no memtier samples" >&2
        exit 1
    }
    "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" "$RAWDIR/client.perf.script" \
        >"$RAWDIR/client.collapsed.txt"
    "$FLAMEGRAPH_DIR/flamegraph.pl" \
        --title "vemb-v16 local client $TIMESTAMP" \
        --subtitle "memtier user+kernel; event=${EVENT}@${FREQ}Hz" \
        "$RAWDIR/client.collapsed.txt" >"$RAWDIR/client.svg"
    grep -q 'vemb-v16 local client' "$RAWDIR/client.svg" || {
        echo "FAIL: client flamegraph validation failed" >&2
        exit 1
    }
}

if [ "$REMOTE_CHILD" != 1 ] && [ "$RUN_LOCAL" != 1 ]; then
    run_remote_same_host
    exit $?
fi

validate_profile
if [ "$PROFILE" = 1 ] && [ "$NCONFIGS" -ne 1 ]; then
    echo "ERROR: PROFILE=1 requires exactly one TS/CS/PS configuration" >&2
    exit 2
fi

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

# 输出 "ut st"（所有线程 utime/stime jiffies 分别求和）
get_cpu_jiffies() {
    local pid=$1 ut=0 st=0 rest
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

snapshot_rss() {  # pid 的 VmRSS KB
    local pid=$1 rss
    rss=$(awk '/^VmRSS:/{print $2}' /proc/$pid/status 2>/dev/null)
    echo "${rss:-0}"
}

cleanup() {
    [ "$ROLE" = "client" ] && return
    if [ -f "$PIDFILE" ]; then
        local p=$(cat "$PIDFILE" 2>/dev/null)
        [ -n "$p" ] && { kill "$p" 2>/dev/null; sleep 0.3; kill -9 "$p" 2>/dev/null; }
    fi
    pkill -9 -f "redis-server.*:$PORT " 2>/dev/null || true
    rm -f "$PIDFILE"
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
        --vemb-v16-aeron-ub-path "$AERON_UB_PATH" \
        --vemb-v16-aeron-response-ub-path "$AERON_RESPONSE_UB_PATH" \
        --aeron-ub-cacheable "$AERON_UB_CACHEABLE_CONFIG" \
        --vemb-v16-proxy-io-threads $PIO \
        --vemb-v16-supernode-workers $SNW \
        --daemonize yes --pidfile $PIDFILE --logfile "$SERVER_LOG" --loglevel warning \
        >/dev/null 2>&1

    # Confirm the Redis TCP listener used by Aeron TCP control.
    for _ in $(seq 1 50); do
        tcp_listener_ready && break
        sleep 0.2
    done
    if ! tcp_listener_ready; then
        echo "FAIL: TCP port $PORT not ready"
        echo "--- server log tail ---"
        tail -30 "$SERVER_LOG" 2>/dev/null
        exit 1
    fi
    echo "server up: pid=$(cat $PIDFILE) control=tcp tcp=$SERVER_HOST:$PORT"
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
    "${VEMB_BATCH_ARGS[@]}" \
    "${VEMB_ENDPOINT_ARGS[@]}" \
    "${VEMB_PEER_VIEW_ARGS[@]}" \
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

if [ "$PROFILE" = 1 ]; then
    start_server_perf
    log "running CLI workload under perf"
else
    log "running CLI workload without perf"
fi

# === TSV header ===
printf "op\tserver_type\tt\tc\tpipeline\tops_sec\tavg_lat_ms\tp50_ms\tp99_ms\tp99_9_ms\tkb_sec\tcores\tops_per_core\tcore_ut\tcore_st\tsi\trss_kb\n" > "$TSV"

# === Sweep ===
for ((idx=0; idx<NCONFIGS; idx++)); do
    t=${TS[$idx]}; c=${CS[$idx]}; p=${PS[$idx]}
    log "--- config $((idx+1))/${NCONFIGS}: t=$t c=$c pipeline=$p ---"

    SRV_PID=$(cat $PIDFILE 2>/dev/null)
    J0_UT=0 J0_ST=0
    if [ -n "$SRV_PID" ]; then
        read J0_UT J0_ST < <(get_cpu_jiffies "$SRV_PID")
    fi
    JB_SI=$(snapshot_si)
    JB_SI=${JB_SI:-0}

    workload_cmd=(
        taskset -c "$CLIENT_MASK" "$MEMTIER"
        --protocol vemb_v16 --vemb-v16-transport="$AERON_TRANSPORT"
        "${VEMB_MODE_ARGS[@]}"
        "${VEMB_BATCH_ARGS[@]}"
        "${VEMB_ENDPOINT_ARGS[@]}"
        "${VEMB_PEER_VIEW_ARGS[@]}"
        --vemb-v16-dim "$DIM" -s "$SERVER_HOST" -p "$PORT"
        -t "$t" -c "$c" --pipeline="$p"
        --ratio=0:1 --key-pattern=R:R
        --key-prefix="$KEY_PREFIX" --key-minimum=1 --key-maximum="$NUM_KEYS"
        --test-time="$TEST_TIME"
    )
    workload_log="$RAWDIR/t${t}_c${c}_p${p}.log"

    # workload 后台启动 + server CPU 0.2s 采样: 稳态 cores 用 TEST_TIME 滑窗 p95 速率,
    # 排除 memtier attach/detach channel 的空闲窗口 (通道多时可达数十秒, 全程平均口径
    # 会把 cores 稀释腰斩; ops_sec 是 memtier 自身稳态窗口值, 两者需同口径)
    SAMP_FILE="$RAWDIR/t${t}_c${c}_p${p}.samples"
    if [ "$PROFILE" = 1 ]; then
        perf record -F "$FREQ" -g -e "$EVENT" -o "$CLIENT_PERF_DATA" -- \
            "${workload_cmd[@]}" >"$workload_log" 2>&1 &
    else
        "${workload_cmd[@]}" >"$workload_log" 2>&1 &
    fi
    MT_PID=$!
    ( while kill -0 $MT_PID 2>/dev/null; do
          if [ -n "$SRV_PID" ] && [ -d /proc/$SRV_PID ]; then
              read SU SS < <(get_cpu_jiffies "$SRV_PID")
              printf "%s %s %s %s\n" "$(date +%s%N)" "$SU" "$SS" "$(snapshot_si)"
          fi
          sleep 0.2
      done ) > "$SAMP_FILE" 2>/dev/null &
    SAMP_PID=$!
    wait $MT_PID
    workload_status=$?
    kill $SAMP_PID 2>/dev/null
    wait $SAMP_PID 2>/dev/null
    if [ "$workload_status" -ne 0 ]; then
        echo "FAIL: Aeron workload failed: t=$t c=$c pipeline=$p"
        tail -80 "$workload_log" 2>/dev/null
        exit 1
    fi

    J1_UT=0 J1_ST=0
    if [ -n "$SRV_PID" ]; then
        read J1_UT J1_ST < <(get_cpu_jiffies "$SRV_PID")
    fi
    JA_SI=$(snapshot_si)
    JA_SI=${JA_SI:-0}
    ELAPSED_NS=$(( TEST_TIME * 1000000000 ))
    CPU_CORES=$(awk -v d=$(( (J1_UT - J0_UT) + (J1_ST - J0_ST) )) -v t=$ELAPSED_NS -v pid="$SRV_PID" 'BEGIN{ if(pid==""||d<0||t<=0) print "NA"; else printf "%.2f", d/100.0/(t/1000000000) }')
    CORE_UT=$(awk -v d=$((J1_UT - J0_UT)) -v t=$ELAPSED_NS -v pid="$SRV_PID" 'BEGIN{ if(pid==""||d<0||t<=0) print "NA"; else printf "%.2f", d/100.0/(t/1000000000) }')
    CORE_ST=$(awk -v d=$((J1_ST - J0_ST)) -v t=$ELAPSED_NS -v pid="$SRV_PID" 'BEGIN{ if(pid==""||d<0||t<=0) print "NA"; else printf "%.2f", d/100.0/(t/1000000000) }')
    C_SI=$(awk -v d=$((JA_SI - JB_SI)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
    RSS_KB=$(snapshot_rss "$SRV_PID")

    # 稳态口径: TEST_TIME 滑窗速率的 p95 (samples 不足时回退全程平均)
    if [ -s "$SAMP_FILE" ]; then
        awk -v tt=$TEST_TIME '
            { ns[NR]=$1; u[NR]=$2; s[NR]=$3; si[NR]=$4; n=NR }
            END {
                ttns = tt * 1e9; j = 1
                for (i = 1; i <= n; i++) {
                    while (j <= n && ns[j] - ns[i] < ttns) j++
                    if (j > n) break
                    dur = (ns[j] - ns[i]) / 1e9
                    if (dur <= 0) continue
                    du = (u[j] - u[i]) / 100 / dur
                    ds = (s[j] - s[i]) / 100 / dur
                    dsi = (si[j] - si[i]) / 100 / dur
                    if (du >= 0 && ds >= 0) printf "%.3f %.3f %.3f %.3f\n", du + ds, du, ds, dsi
                }
            }' "$SAMP_FILE" | sort -rn > "$SAMP_FILE.rates"
        NRATE=$(wc -l < "$SAMP_FILE.rates")
        if [ "$NRATE" -ge 10 ]; then
            K=$(( NRATE / 20 )); [ $K -lt 1 ] && K=1
            read ST_CORES ST_UT ST_ST ST_SI < <(sed -n "${K}p" "$SAMP_FILE.rates")
            CPU_CORES=$(printf "%.2f" "$ST_CORES")
            CORE_UT=$(printf "%.2f" "$ST_UT")
            CORE_ST=$(printf "%.2f" "$ST_ST")
            C_SI=$(printf "%.2f" "$ST_SI")
        fi
    fi

    totals=$(grep "^Totals" "$workload_log" | tail -1)
    # memtier Totals 两种格式:
    #   旧 9 列:  Totals ops hits misses avg p50 p99 p99.9 kb
    #   新 11 列: Totals ops hits misses ?   ?   avg p50 p99 p99.9 kb
    read ops avg p50 p99 p999 kb < <(
        echo "$totals" | awk '{
            if (NF>=11)      printf "%s %s %s %s %s %s", $2,$7,$8,$9,$10,$11;
            else if (NF>=9)  printf "%s %s %s %s %s %s", $2,$5,$6,$7,$8,$9;
            else             printf "0 NA NA NA NA NA";
        }'
    )
    OPS_PER_CORE=$(awk -v o="$ops" -v c="$CPU_CORES" 'BEGIN{ if(c=="NA"||c==0||o==0) print "NA"; else printf "%.0f", o/c }')

    printf "VEMB\t%s_local\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
        "$AERON_TRANSPORT" \
        "$t" "$c" "$p" "$ops" "$avg" "$p50" "$p99" "$p999" "$kb" "$CPU_CORES" "$OPS_PER_CORE" \
        "$CORE_UT" "$CORE_ST" "$C_SI" "$RSS_KB" >> "$TSV"
    log "  => ops/s=$ops  avg=${avg}ms  p50=${p50}ms  p99=${p99}ms  p99.9=${p999}ms  cores=$CPU_CORES  ops/core=$OPS_PER_CORE  core_ut=$CORE_UT  core_st=$CORE_ST  si=$C_SI  rss=${RSS_KB}KB"
done

if [ "$PROFILE" = 1 ]; then
    wait_server_perf
    log "rendering server and client flamegraphs"
    render_flamegraphs
fi

log "=== DONE ==="
log "TSV : $TSV"
log "raw : $RAWDIR/*.log"
if [ "$PROFILE" = 1 ]; then
    printf 'server SVG: %s/server.svg\n' "$RAWDIR"
    printf 'client SVG: %s/client.svg\n' "$RAWDIR"
    printf 'server raw: %s/server.perf.data\n' "$RAWDIR"
    printf 'client raw: %s/client.perf.data\n' "$RAWDIR"
else
    printf 'perf/SVG: disabled (PROFILE=0)\n'
fi
echo "----- summary -----"
column -t -s $'\t' "$TSV"
