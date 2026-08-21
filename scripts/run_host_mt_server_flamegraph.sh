#!/usr/bin/env bash
# Run one host-mt workload remotely, sample only redis-server, and pull back
# the complete server-only user+kernel flamegraph and benchmark artifacts.
#
# Example:
#   bash scripts/run_host_mt_server_flamegraph.sh
#   SCENARIO=cache NUM_KEYS=10000 bash scripts/run_host_mt_server_flamegraph.sh
#   BATCH=16 PIPELINE=32 WORKERS=21:21 bash scripts/run_host_mt_server_flamegraph.sh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"

NODE="${NODE:-43.154.145.18}"
SSH_PORT="${SSH_PORT:-8112}"
REMOTE_DIR="${REMOTE_DIR:-/root/szz/codespace/hpc-redis}"
REMOTE_FLAMEGRAPH_DIR="${REMOTE_FLAMEGRAPH_DIR:-/root/FlameGraph}"

SCENARIO="${SCENARIO:-normal}"
PORT="${PORT:-6390}"
BATCH="${BATCH:-32}"
PIPELINE="${PIPELINE:-32}"
NUM_KEYS="${NUM_KEYS:-100000}"
WORKERS="${WORKERS:-21:21}"
TS="${TS:-64}"
CS="${CS:-4}"
TEST_TIME="${TEST_TIME:-30}"
FLAME_DURATION="${FLAME_DURATION:-20}"
FREQ="${FREQ:-99}"
EVENT="${EVENT:-cycles}"
AFFINITY_MODE="${AFFINITY_MODE:-0}"
BUILD="${BUILD:-1}"
PROFILE="${PROFILE:-1}"
RUN_ID="${RUN_ID:-$(date +%Y%m%d_%H%M%S)}"

LOCAL_ROOT="${LOCAL_ROOT:-$ROOT_DIR/perf/$RUN_ID}"

usage() {
    cat <<'USAGE'
Run one host-mt benchmark group and create a redis-server-only flamegraph.

Usage:
  bash scripts/run_host_mt_server_flamegraph.sh

Important environment variables:
  SCENARIO=normal|cache       output label; default normal
  BATCH=32                    server request/response/queue batch; default 32
  PIPELINE=32                 client pipeline; default 32
  NUM_KEYS=100000             normal key count; cache uses 10000
  WORKERS=21:21               proxy-io:supernode workers
  TS=64 CS=4                  memtier threads and connections
  TEST_TIME=30                workload duration in seconds
  FLAME_DURATION=20           sampling duration; must be less than TEST_TIME
  AFFINITY_MODE=0             0 interleaved, 1 grouped
  BUILD=1                     1 rebuild redis-server remotely, 0 reuse it
  PROFILE=1                   1 collect perf/SVG, 0 run workload only
  NODE=43.154.145.18 SSH_PORT=8112  remote SSH endpoint; user is fixed to root
  REMOTE_DIR=/root/szz/codespace/hpc-redis
  LOCAL_ROOT=<repo>/perf/<run-id>

The command runs one group only. Repeat it with different parameters for
independent, resumable groups.
USAGE
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
    usage
    exit 0
fi

case "$SCENARIO" in
    normal|cache) ;;
    *) die "SCENARIO must be normal or cache" ;;
esac

case "$WORKERS" in
    *:*) ;;
    *) die "WORKERS must use pio:snw format, for example 21:21" ;;
esac

case "$BUILD" in
    0|1) ;;
    *) die "BUILD must be 0 or 1" ;;
esac

case "$PROFILE" in
    0|1) ;;
    *) die "PROFILE must be 0 or 1" ;;
esac

for value in "$BATCH" "$PIPELINE" "$NUM_KEYS" "$TS" "$CS" "$TEST_TIME" "$FLAME_DURATION"; do
    case "$value" in
        ''|*[!0-9]*) die "numeric parameters must be positive integers" ;;
    esac
done
if [ "$PROFILE" = "1" ]; then
    [ "$TEST_TIME" -gt "$FLAME_DURATION" ] ||
        die "FLAME_DURATION=$FLAME_DURATION must be less than TEST_TIME=$TEST_TIME"
fi

PIO="${WORKERS%%:*}"
SNW="${WORKERS##*:}"
WORKERS_LABEL="${WORKERS//:/_}"
LABEL="${SCENARIO}_batch${BATCH}_pipe${PIPELINE}_numkeys${NUM_KEYS}_workers${WORKERS_LABEL}_affinity${AFFINITY_MODE}_t${TS}_c${CS}_${TEST_TIME}s"
REMOTE_ROOT="/tmp/${RUN_ID}"
REMOTE_GROUP_DIR="$REMOTE_ROOT"
LOCAL_GROUP_DIR="$LOCAL_ROOT"
if [[ "$LOCAL_GROUP_DIR" == "$ROOT_DIR/"* ]]; then
    LOCAL_LOG_DIR="${LOCAL_GROUP_DIR#"$ROOT_DIR"/}"
else
    LOCAL_LOG_DIR="$LOCAL_GROUP_DIR"
fi

mkdir -p "$LOCAL_GROUP_DIR"

echo "Running one group: $LABEL"
echo "Remote: root@$NODE:$REMOTE_DIR"
echo "Local : $LOCAL_LOG_DIR"
echo "Config: batch=$BATCH pipeline=$PIPELINE keys=$NUM_KEYS workers=$WORKERS ts=$TS cs=$CS test=${TEST_TIME}s flame=${FLAME_DURATION}s event=$EVENT build=$BUILD profile=$PROFILE"

ssh -p "$SSH_PORT" "root@$NODE" bash -s -- \
    "$REMOTE_DIR" "$REMOTE_GROUP_DIR" "$REMOTE_FLAMEGRAPH_DIR" \
    "$PORT" "$BATCH" "$PIPELINE" "$NUM_KEYS" "$WORKERS" "$PIO" "$SNW" \
    "$TS" "$CS" "$TEST_TIME" "$FLAME_DURATION" "$FREQ" "$EVENT" \
    "$AFFINITY_MODE" "$LABEL" "$BUILD" "$PROFILE" <<'REMOTE_SCRIPT'
set -euo pipefail

status() {
    printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

REMOTE_DIR=$1
REMOTE_GROUP_DIR=$2
FLAMEGRAPH_DIR=$3
PORT=$4
BATCH=$5
PIPELINE=$6
NUM_KEYS=$7
WORKERS=$8
PIO=$9
SNW=${10}
TS=${11}
CS=${12}
TEST_TIME=${13}
FLAME_DURATION=${14}
FREQ=${15}
EVENT=${16}
AFFINITY_MODE=${17}
LABEL=${18}
BUILD=${19}
PROFILE=${20}

mkdir -p "$REMOTE_GROUP_DIR"
cd "$REMOTE_DIR"

if [ "$BUILD" = "1" ]; then
    status "building redis-server"
    make -B -C src redis-server USE_UB=yes \
        PROXY_REQUEST_BATCH="$BATCH" \
        PROXY_RESPONSE_BATCH="$BATCH" \
        PROXY_QUEUE_BATCH="$BATCH" \
        VEMB_V16_PROXY_AFFINITY_MODE="$AFFINITY_MODE" \
        2>&1 | tee "$REMOTE_GROUP_DIR/build.log"
    status "building clients/c"
    make -C clients/c -j 2>&1 | tee -a "$REMOTE_GROUP_DIR/build.log"
    status "building memtier_benchmark"
    make -C memtier_benchmark -j 2>&1 | tee -a "$REMOTE_GROUP_DIR/build.log"
else
    status "skipping remote build; using $REMOTE_DIR/src/redis-server"
    [ -x "$REMOTE_DIR/src/redis-server" ] || {
        echo "missing executable: $REMOTE_DIR/src/redis-server" >&2
        exit 1
    }
    printf 'remote build skipped; using %s\n' "$REMOTE_DIR/src/redis-server" \
        >"$REMOTE_GROUP_DIR/build.log"
fi

status "starting benchmark driver; streaming $REMOTE_GROUP_DIR/driver.log"
env PORT="$PORT" NUM_KEYS="$NUM_KEYS" WORKERS="$WORKERS" TS="$TS" CS="$CS" \
    PIPELINE="$PIPELINE" TEST_TIME="$TEST_TIME" OUTDIR="$REMOTE_GROUP_DIR" \
    bash hpc_redis_max_tput.sh >"$REMOTE_GROUP_DIR/driver.log" 2>&1 &
DRIVER=$!
tail -n +1 -f "$REMOTE_GROUP_DIR/driver.log" &
DRIVER_LOG_TAIL=$!
cleanup_driver_log_tail() {
    kill "$DRIVER_LOG_TAIL" 2>/dev/null || true
    wait "$DRIVER_LOG_TAIL" 2>/dev/null || true
}
trap cleanup_driver_log_tail EXIT
RAW="$REMOTE_GROUP_DIR/raw/pio${PIO}_snw${SNW}_t${TS}_c${CS}.txt"

status "waiting for benchmark workload phase"
for _ in $(seq 1 600); do
    [ -f "$RAW" ] && break
    kill -0 "$DRIVER" 2>/dev/null || break
    sleep 0.1
done
[ -f "$RAW" ] || {
    echo "benchmark did not reach workload phase; see $REMOTE_GROUP_DIR/driver.log" >&2
    exit 1
}
status "workload phase reached"
if [ "$PROFILE" = "1" ]; then
    status "locating redis-server for sampling"
    PIDFILE="/tmp/hpc_max_tput_server_${PORT}.pid"
    [ -r "$PIDFILE" ] || { echo "missing $PIDFILE" >&2; exit 1; }
    PID=$(cat "$PIDFILE")
    [ -r "/proc/$PID/status" ] || { echo "server PID $PID is not readable" >&2; exit 1; }
    [ "$(ps -p "$PID" -o comm= | tr -d ' ')" = "redis-server" ] || {
        echo "PID=$PID is not redis-server" >&2
        exit 1
    }

    TS_NOW=$(date +%Y%m%d_%H%M%S)
    BASE_NAME="$LABEL"
    PERF_DATA="$REMOTE_GROUP_DIR/${BASE_NAME}.perf.data"
    PERF_SCRIPT="$REMOTE_GROUP_DIR/${BASE_NAME}.perf.script"
    COLLAPSED="$REMOTE_GROUP_DIR/${BASE_NAME}.collapsed.txt"
    SVG="$REMOTE_GROUP_DIR/${BASE_NAME}.svg"
    META="$REMOTE_GROUP_DIR/${BASE_NAME}.meta.txt"

    {
        echo "timestamp=$TS_NOW"
        echo "pid=$PID"
        echo "comm=redis-server"
        echo "duration=$FLAME_DURATION"
        echo "freq=$FREQ"
        echo "event=$EVENT"
        echo "mode=single-process user+kernel"
        echo "remote_dir=$REMOTE_DIR"
        echo "batch=$BATCH"
        echo "pipeline=$PIPELINE"
        echo "num_keys=$NUM_KEYS"
        echo "workers=$WORKERS"
        echo "affinity_mode=$AFFINITY_MODE"
        echo "supernode_thread_names=merged"
        echo
        ps -p "$PID" -o 'pid,ppid,comm,args' || true
    } >"$META"

    # Attach to redis-server only. Without :u, the server's kernel I/O stack
    # is retained; without -a, memtier and redis-cli are excluded.
    status "sampling redis-server pid=$PID for ${FLAME_DURATION}s (event=$EVENT)"
    perf record -F "$FREQ" -g -e "$EVENT" -p "$PID" \
        -o "$PERF_DATA" -- sleep "$FLAME_DURATION"
    status "rendering flamegraph"
    perf script -i "$PERF_DATA" >"$PERF_SCRIPT"
    "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" "$PERF_SCRIPT" |
        sed -E 's/^vemb-sn-[0-9]+;/redis-server;/' >"$COLLAPSED"
    "$FLAMEGRAPH_DIR/flamegraph.pl" \
        --title "hpc-redis server-only $LABEL" \
        "$COLLAPSED" >"$SVG"
fi

wait "$DRIVER"
status "benchmark driver completed"
if [ "$PROFILE" = "1" ]; then
    rm -f "$PERF_SCRIPT"
fi
cat "$REMOTE_GROUP_DIR/summary.tsv"
if [ "$PROFILE" = "1" ]; then
    printf 'REMOTE_GROUP_DIR=%s\nSVG=%s\nCOLLAPSED=%s\n' \
        "$REMOTE_GROUP_DIR" "$SVG" "$COLLAPSED"
else
    printf 'REMOTE_GROUP_DIR=%s\nPROFILE=disabled\n' "$REMOTE_GROUP_DIR"
fi
REMOTE_SCRIPT

# Pull the flamegraph, perf metadata, and benchmark summary needed for the
# chapter. Raw per-request memtier logs stay on the remote host because they
# are large and are not needed to inspect or reproduce the flamegraph result.
if [ "$PROFILE" = "1" ]; then
    scp -P "$SSH_PORT" \
        "root@$NODE:$REMOTE_GROUP_DIR/${LABEL}.perf.data" \
        "root@$NODE:$REMOTE_GROUP_DIR/${LABEL}.collapsed.txt" \
        "root@$NODE:$REMOTE_GROUP_DIR/${LABEL}.meta.txt" \
        "root@$NODE:$REMOTE_GROUP_DIR/${LABEL}.svg" \
        "root@$NODE:$REMOTE_GROUP_DIR/summary.tsv" \
        "root@$NODE:$REMOTE_GROUP_DIR/build.log" \
        "root@$NODE:$REMOTE_GROUP_DIR/driver.log" \
        "$LOCAL_GROUP_DIR/"
else
    scp -P "$SSH_PORT" \
        "root@$NODE:$REMOTE_GROUP_DIR/summary.tsv" \
        "root@$NODE:$REMOTE_GROUP_DIR/build.log" \
        "root@$NODE:$REMOTE_GROUP_DIR/driver.log" \
        "$LOCAL_GROUP_DIR/"
fi

echo
if [ "$PROFILE" = "1" ]; then
    echo "Generated locally:"
    echo "  SVG      : $LOCAL_LOG_DIR/${LABEL}.svg"
    echo "  collapsed: $LOCAL_LOG_DIR/${LABEL}.collapsed.txt"
    echo "  meta     : $LOCAL_LOG_DIR/${LABEL}.meta.txt"
else
    echo "Generated locally: $LOCAL_LOG_DIR/{summary,build,driver}.log"
fi
