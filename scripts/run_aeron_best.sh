#!/bin/bash
# ============================================================================
# run_aeron_best.sh
# Aeron transport (TCP control + UB rings) local loopback VEMB sweep
# Default UB layout: request ring on obmm_shmdev1, warm region on
# obmm_shmdev2 (from the default manifest), response ring on obmm_shmdev3.
#
# 使用方法（所有参数都有默认值，按需覆盖）：
#   bash run_aeron_best.sh
#   NODE=43.154.145.18 SSH_PORT=8112 REMOTE_DIR=/root/szz/codespace/hpc-redis \
#   BUILD=0 WORKERS=21:21 TS=64 CS=4 PIPELINE=32 bash run_aeron_best.sh
#   ssh HW01 'TEST_TIME=60 bash /root/gqs/codespace/UnifiedBus/test_hpc/run_aeron_best.sh'
#
# 可调参数（环境变量）：
#   TEST_TIME     bench 持续秒数         (默认 60)
#   TS CS         client -t / -c         (默认 64 / 4；可传空格分隔矩阵)
#   PIPELINE      每 channel in-flight   (默认 32；可传空格分隔矩阵)
#   NUM_KEYS      prefill key 数         (默认 100000)
#   MAX_VECTORS   server vector 容量上限 (默认 131072=128K；NUM_KEYS 不能超过这个)
#   DIM           vector 维度            (默认 300)
#   SERVER_MASK   server taskset         (默认 "0-47")
#   WORKERS       proxy-io:supernode     (默认 "8:8")
#   CLIENT_MASK   client taskset         (默认 "96-191")
#   SERVER_HOST   client 连接的 server IP (默认 127.0.0.1)
#   PORT          server 端口            (默认 6395)
#   ROLE          both|server|client     (默认 both)
#   AERON_BATCH_DISABLE=yes  强制使用 v1 channel
#   PROFILE=0|1   启用/禁用 server、client perf 和 SVG 火焰图 (默认 0)
#   FLAME_DURATION perf 采样时间        (默认 25)
#   FREQ/EVENT      perf 频率/事件 (默认 99/cycles)
#   LOCAL_ROOT      本地结果目录       (默认 perf/aeron_sweep/<run-id>)
#   RUN_ID          运行标识           (默认 YYYYMMDD_HHMMSS)

set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
HPC=${HPC:-$(cd "$SCRIPT_DIR/.." && pwd -P)}
NODE=${NODE:-43.154.145.18}
SSH_PORT=${SSH_PORT:-8112}
REMOTE_DIR=${REMOTE_DIR:-/root/szz/codespace/hpc-redis}
REMOTE_RESULT_ROOT=/tmp/aeron_sweep
RUN_LOCAL=${RUN_LOCAL:-0}
REMOTE_CHILD=${AERON_BEST_REMOTE_CHILD:-0}
REDIS=$HPC/src/redis-server
MEMTIER=$HPC/memtier_benchmark/memtier_benchmark
MANIFEST=${MANIFEST:-$HPC/examples/vemb_v16_warm_regions_111.yaml}

PORT=${PORT:-6395}
SERVER_HOST=${SERVER_HOST:-127.0.0.1}
ROLE=${ROLE:-both}
BUILD=${BUILD:-0}
case "$BUILD" in
    0) ;;
    1) echo "ERROR: run_aeron_best.sh reuses the prebuilt remote binaries; use BUILD=0" >&2; exit 2 ;;
    *) echo "ERROR: BUILD must be 0" >&2; exit 2 ;;
esac
DIM=${DIM:-300}
NUM_KEYS=${NUM_KEYS:-100000}
MAX_VECTORS=${MAX_VECTORS:-131072}
TEST_TIME=${TEST_TIME:-30}
PROFILE=${PROFILE:-0}
FLAME_DURATION=${FLAME_DURATION:-25}
FREQ=${FREQ:-99}
EVENT=${EVENT:-cycles}
FLAMEGRAPH_DIR=${FLAMEGRAPH_DIR:-$HPC/perf/FlameGraph}
KEY_PREFIX=${KEY_PREFIX:-"item:"}
WORKERS=${WORKERS:-8:8}
case "$WORKERS" in
    *:*) ;;
    *) echo "ERROR: WORKERS must use pio:snw format, for example 8:8" >&2; exit 2 ;;
esac
PIO="${WORKERS%%:*}"
SNW="${WORKERS##*:}"
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

# === 配置矩阵 ===
TS_DEFAULT=(1 1 1 1  1  2  4  8  16 32 64 64 64 64 64 64 64)
CS_DEFAULT=(1 1 1 1  1  1  1  1  1  1  1  2  4  8  16 32 64)
PS_DEFAULT=(1 4 8 16 32 32 32 32 32 32 32 32 32 32 32 32 32)
TS=( ${TS:-${TS_DEFAULT[*]}} )
CS=( ${CS:-${CS_DEFAULT[*]}} )
PS=( ${PIPELINE:-${PS_DEFAULT[*]}} )
NCONFIGS=${#TS[@]}
[ "${#CS[@]}" -eq "$NCONFIGS" ] && [ "${#PS[@]}" -eq "$NCONFIGS" ] || {
    echo "ERROR: TS, CS, and PIPELINE must contain the same number of configurations" >&2
    exit 2
}

# === 输出 ===
RUN_ID=${RUN_ID:-$(date +%Y%m%d_%H%M%S)}
if [ -n "${LOCAL_ROOT:-}" ]; then
    case "$LOCAL_ROOT" in
        /*) ;;
        *) LOCAL_ROOT="$HPC/$LOCAL_ROOT" ;;
    esac
else
    LOCAL_ROOT="$HPC/perf/aeron_sweep/$RUN_ID"
fi
RAWDIR="$LOCAL_ROOT/raw"
TSV="$LOCAL_ROOT/summary.tsv"

PIDFILE=/tmp/vemb_aeron_sweep.pid
SERVER_LOG=/tmp/vemb_aeron_sweep.log
SERVER_PERF_PID=
CLIENT_PERF_DATA="$RAWDIR/client.perf.data"

run_remote_same_host() {
    local peer="root@$NODE"
    local remote_outdir local_run_dir remote_run_dir remote_archive local_archive
    local remote_tar_command remote_cleanup_command
    remote_outdir="$REMOTE_RESULT_ROOT"
    local_run_dir="${LOCAL_ROOT%/}"
    remote_run_dir="$remote_outdir/$RUN_ID"
    remote_archive="$remote_run_dir.tar.gz"
    local_archive="$local_run_dir.tar.gz"
    local remote_env_names=(
        NUM_KEYS KEY_PREFIX MAX_VECTORS DIM TEST_TIME PROFILE
        FLAME_DURATION FREQ EVENT FLAMEGRAPH_DIR WORKERS
        SERVER_MASK CLIENT_MASK PORT SERVER_HOST ROLE BUILD
        AERON_UB_PATH AERON_RESPONSE_UB_PATH AERON_TRANSPORT
        AERON_BATCH_DISABLE AERON_UB_CACHEABLE AERON_PEER_VIEW_MANIFEST
        AERON_PEER_VIEW_CLIENT_HOST AERON_PEER_VIEW_OWNER_ID SERVER_TRANSPORT
        TS CS PIPELINE
    )
    local remote_args=("$REMOTE_DIR" "$RUN_ID")
    local name value
    for name in "${remote_env_names[@]}"; do
        if [ "${!name+x}" = x ]; then
            case "$name" in
                TS) value="${TS[*]}" ;;
                CS) value="${CS[*]}" ;;
                PIPELINE) value="${PIPELINE:-}" ;;
                *) value=${!name} ;;
            esac
            case "$name" in
                AERON_PEER_VIEW_MANIFEST|FLAMEGRAPH_DIR)
                    case "$value" in
                        "$HPC"/*) value="$REMOTE_DIR/${value#"$HPC/"}" ;;
                    esac
                    ;;
            esac
            remote_args+=("$name" "$value")
        fi
    done
    remote_args+=("LOCAL_ROOT" "$remote_run_dir")

    printf '[%s] same-host remote run: %s:%s:%s\n' \
        "$(date '+%F %T')" "$peer" "$SSH_PORT" "$REMOTE_DIR"
    # SSH concatenates remote command arguments and reparses them remotely;
    # quote each value so array settings such as TS/CS/PIPELINE stay one argument.
    local remote_command
    printf -v remote_command '%q ' "${remote_args[@]}"
    if ! ssh -p "$SSH_PORT" "$peer" "bash -s -- $remote_command" <<'REMOTE_SAME_HOST' \
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
export RUN_ID="$timestamp"
cd "$root"
exec bash scripts/run_aeron_best.sh
REMOTE_SAME_HOST
    then
        echo "ERROR: remote same-host workload failed" >&2
        return 1
    fi

    printf '[%s] compressing remote results: %s\n' \
        "$(date '+%F %T')" "$remote_run_dir"
    printf -v remote_tar_command 'tar -C %q -czf %q .' \
        "$remote_run_dir" "$remote_archive"
    if ! ssh -p "$SSH_PORT" "$peer" "$remote_tar_command" 2>&1 | \
        sed '/^Authorized users only\. All activities may be monitored and reported\.$/d'; then
        echo "ERROR: failed to compress remote results: $remote_run_dir" >&2
        return 1
    fi

    mkdir -p "$local_run_dir"
    printf '[%s] pulling: %s:%s -> %s\n' \
        "$(date '+%F %T')" "$peer" "$remote_archive" "$local_archive"
    if ! scp -P "$SSH_PORT" \
        "$peer:$remote_archive" "$local_archive" 2>&1 | \
        sed '/^Authorized users only\. All activities may be monitored and reported\.$/d'; then
        echo "ERROR: failed to pull remote archive: $remote_archive" >&2
        return 1
    fi
    if ! tar -xzf "$local_archive" -C "$local_run_dir"; then
        echo "ERROR: failed to extract local archive: $local_archive" >&2
        return 1
    fi
    rm -f "$local_archive"
    printf -v remote_cleanup_command 'rm -f %q' "$remote_archive"
    ssh -p "$SSH_PORT" "$peer" "$remote_cleanup_command" >/dev/null 2>&1 ||
        echo "WARNING: failed to remove remote temporary archive: $remote_archive" >&2
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
    case "$FLAME_DURATION" in
        ''|*[!0-9]*) echo "ERROR: FLAME_DURATION must be a positive integer" >&2; exit 2 ;;
    esac
    [ "$FLAME_DURATION" -gt 0 ] || {
        echo "ERROR: FLAME_DURATION must be positive" >&2
        exit 2
    }
    [ "$FLAME_DURATION" -lt "$TEST_TIME" ] || {
        echo "ERROR: FLAME_DURATION must be less than TEST_TIME" >&2
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
        -o "$RAWDIR/server.perf.data" -- sleep "$FLAME_DURATION" \
        >"$RAWDIR/server.perf.log" 2>&1 &
    SERVER_PERF_PID=$!
    printf '%s\n' "$SERVER_PERF_PID" >"$RAWDIR/server.perf.pid"
    log "server perf: tids=$(echo "$tids" | tr ',' ' ' | wc -w | tr -d ' ') duration=${FLAME_DURATION}s pid=$SERVER_PERF_PID"
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
        --title "vemb-v16 local server $RUN_ID" \
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
        --title "vemb-v16 local client $RUN_ID" \
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
    echo "ERROR: PROFILE=1 requires exactly one TS/CS/PIPELINE configuration" >&2
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
        --daemonize yes --pidfile $PIDFILE --logfile "$SERVER_LOG" --loglevel notice \
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
printf "op\tserver_type\tt\tc\tpipeline\tops_sec\tavg_lat_ms\tp50_ms\tp99_ms\tp99_9_ms\tkb_sec\tcores\tops_per_core\n" > "$TSV"

# === Sweep ===
for ((idx=0; idx<NCONFIGS; idx++)); do
    t=${TS[$idx]}; c=${CS[$idx]}; p=${PS[$idx]}
    log "--- config $((idx+1))/${NCONFIGS}: t=$t c=$c pipeline=$p ---"

    SRV_PID=$(cat $PIDFILE 2>/dev/null)
    J0=0
    [ -n "$SRV_PID" ] && J0=$(get_cpu_jiffies "$SRV_PID")

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
    if [ "$PROFILE" = 1 ]; then
        perf record -F "$FREQ" -g -e "$EVENT" -o "$CLIENT_PERF_DATA" -- \
            "${workload_cmd[@]}" >"$workload_log" 2>&1
        workload_status=$?
    else
        "${workload_cmd[@]}" >"$workload_log" 2>&1
        workload_status=$?
    fi
    if [ "$workload_status" -ne 0 ]; then
        echo "FAIL: Aeron workload failed: t=$t c=$c pipeline=$p"
        tail -80 "$workload_log" 2>/dev/null
        exit 1
    fi

    J1=0
    [ -n "$SRV_PID" ] && J1=$(get_cpu_jiffies "$SRV_PID")
    CPU_CORES=$(awk -v d=$((J1 - J0)) -v t=$TEST_TIME -v pid="$SRV_PID" 'BEGIN{ if(pid==""||d<0||t<=0) print "NA"; else printf "%.2f", d/100.0/t }')

    totals=$(grep "^Totals" "$workload_log" | tail -1)
    read ops avg p50 p99 p999 kb < <(
        echo "$totals" | awk '
            /^Totals[[:space:]]/ {
                if (NF >= 11)
                    printf "%s %s %s %s %s %s", $2, $7, $8, $9, $10, $11
                else if (NF >= 9)
                    printf "%s %s %s %s %s %s", $2, $5, $6, $7, $8, $9
                else
                    printf "0 NA NA NA NA NA"
            }
        '
    )
    OPS_PER_CORE=$(awk -v o="$ops" -v c="$CPU_CORES" 'BEGIN{ if(c=="NA"||c==0||o==0) print "NA"; else printf "%.0f", o/c }')

    printf "VEMB\t%s_local\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
        "$AERON_TRANSPORT" \
        "$t" "$c" "$p" "$ops" "$avg" "$p50" "$p99" "$p999" "$kb" "$CPU_CORES" "$OPS_PER_CORE" >> "$TSV"
    log "  => ops/s=$ops  avg=${avg}ms  p50=${p50}ms  p99=${p99}ms  p99.9=${p999}ms  cores=$CPU_CORES  ops/core=$OPS_PER_CORE"
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
