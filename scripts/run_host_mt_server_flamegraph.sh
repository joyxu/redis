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

SSH_TARGET="${SSH_TARGET:-root@192.168.90.112}"
SSH_PORT="${SSH_PORT:-22}"
REMOTE_DIR="${REMOTE_DIR:-/root/szz/codespace/hpc-redis_bench}"
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
RUN_ID="${RUN_ID:-$(date +%Y%m%d_%H%M%S)}"

LOCAL_ROOT="${LOCAL_ROOT:-$ROOT_DIR/perf/host_mt_server_flamegraphs_$RUN_ID}"

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
  SSH_TARGET=root@192.168.90.112
  REMOTE_DIR=/root/szz/codespace/hpc-redis_bench
  LOCAL_ROOT=<repo>/perf/host_mt_server_flamegraphs_<run-id>

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

for value in "$BATCH" "$PIPELINE" "$NUM_KEYS" "$TS" "$CS" "$TEST_TIME" "$FLAME_DURATION"; do
    case "$value" in
        ''|*[!0-9]*) die "numeric parameters must be positive integers" ;;
    esac
done
[ "$TEST_TIME" -gt "$FLAME_DURATION" ] ||
    die "FLAME_DURATION=$FLAME_DURATION must be less than TEST_TIME=$TEST_TIME"

PIO="${WORKERS%%:*}"
SNW="${WORKERS##*:}"
WORKERS_LABEL="${WORKERS//:/_}"
LABEL="host_mt_server_only_${SCENARIO}_batch${BATCH}_pipe${PIPELINE}_numkeys${NUM_KEYS}_workers${WORKERS_LABEL}_affinity${AFFINITY_MODE}_t${TS}_c${CS}_${TEST_TIME}s"
REMOTE_ROOT="/tmp/host_mt_server_flamegraphs_${RUN_ID}"
REMOTE_GROUP_DIR="$REMOTE_ROOT/$LABEL"
LOCAL_GROUP_DIR="$LOCAL_ROOT/$LABEL"

mkdir -p "$LOCAL_GROUP_DIR"

echo "Running one group: $LABEL"
echo "Remote: $SSH_TARGET:$REMOTE_DIR"
echo "Local : $LOCAL_GROUP_DIR"

ssh -p "$SSH_PORT" "$SSH_TARGET" bash -s -- \
    "$REMOTE_DIR" "$REMOTE_GROUP_DIR" "$REMOTE_FLAMEGRAPH_DIR" \
    "$PORT" "$BATCH" "$PIPELINE" "$NUM_KEYS" "$WORKERS" "$PIO" "$SNW" \
    "$TS" "$CS" "$TEST_TIME" "$FLAME_DURATION" "$FREQ" "$EVENT" \
    "$AFFINITY_MODE" "$LABEL" <<'REMOTE_SCRIPT'
set -euo pipefail

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

mkdir -p "$REMOTE_GROUP_DIR/flame"
cd "$REMOTE_DIR"

make -B -C src redis-server USE_UB=yes \
    PROXY_REQUEST_BATCH="$BATCH" \
    PROXY_RESPONSE_BATCH="$BATCH" \
    PROXY_QUEUE_BATCH="$BATCH" \
    VEMB_V16_PROXY_AFFINITY_MODE="$AFFINITY_MODE" \
    >"$REMOTE_GROUP_DIR/build.log" 2>&1

env PORT="$PORT" NUM_KEYS="$NUM_KEYS" WORKERS="$WORKERS" TS="$TS" CS="$CS" \
    PIPELINE="$PIPELINE" TEST_TIME="$TEST_TIME" OUTDIR="$REMOTE_GROUP_DIR" \
    bash hpc_redis_max_tput.sh >"$REMOTE_GROUP_DIR/driver.log" 2>&1 &
DRIVER=$!
RAW="$REMOTE_GROUP_DIR/raw/pio${PIO}_snw${SNW}_t${TS}_c${CS}.txt"

for _ in $(seq 1 600); do
    [ -f "$RAW" ] && break
    kill -0 "$DRIVER" 2>/dev/null || break
    sleep 0.1
done
[ -f "$RAW" ] || {
    echo "benchmark did not reach workload phase; see $REMOTE_GROUP_DIR/driver.log" >&2
    exit 1
}

PIDFILE="/tmp/hpc_max_tput_server_${PORT}.pid"
[ -r "$PIDFILE" ] || { echo "missing $PIDFILE" >&2; exit 1; }
PID=$(cat "$PIDFILE")
[ -r "/proc/$PID/status" ] || { echo "server PID $PID is not readable" >&2; exit 1; }
[ "$(ps -p "$PID" -o comm= | tr -d ' ')" = "redis-server" ] || {
    echo "PID=$PID is not redis-server" >&2
    exit 1
}

TS_NOW=$(date +%Y%m%d_%H%M%S)
BASE_NAME="flamegraph_hpc_redis_server_only_${LABEL}"
PERF_DATA="$REMOTE_GROUP_DIR/flame/${BASE_NAME}.perf.data"
PERF_SCRIPT="$REMOTE_GROUP_DIR/flame/${BASE_NAME}.perf.script"
COLLAPSED="$REMOTE_GROUP_DIR/flame/${BASE_NAME}.collapsed.txt"
SVG="$REMOTE_GROUP_DIR/flame/${BASE_NAME}.svg"
META="$REMOTE_GROUP_DIR/flame/${BASE_NAME}.meta.txt"

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
    echo
    ps -p "$PID" -o pid,ppid,comm,args
} >"$META"

# Attach to redis-server only. Without :u, the server's kernel I/O stack is
# retained; without -a, memtier and redis-cli are excluded.
perf record -F "$FREQ" -g -e "$EVENT" -p "$PID" \
    -o "$PERF_DATA" -- sleep "$FLAME_DURATION"
perf script -i "$PERF_DATA" >"$PERF_SCRIPT"
"$FLAMEGRAPH_DIR/stackcollapse-perf.pl" "$PERF_SCRIPT" >"$COLLAPSED"
"$FLAMEGRAPH_DIR/flamegraph.pl" \
    --title "hpc-redis server-only host-mt $LABEL" \
    "$COLLAPSED" >"$SVG"

wait "$DRIVER"
rm -f "$PERF_SCRIPT"
cat "$REMOTE_GROUP_DIR/summary.tsv"
printf 'REMOTE_GROUP_DIR=%s\nSVG=%s\nCOLLAPSED=%s\n' \
    "$REMOTE_GROUP_DIR" "$SVG" "$COLLAPSED"
REMOTE_SCRIPT

# Pull the flamegraph, perf metadata, and benchmark summary needed for the
# chapter. Raw per-request memtier logs stay on the remote host because they
# are large and are not needed to inspect or reproduce the flamegraph result.
scp -r -P "$SSH_PORT" "$SSH_TARGET:$REMOTE_GROUP_DIR/flame" "$LOCAL_GROUP_DIR/"
scp -P "$SSH_PORT" \
    "$SSH_TARGET:$REMOTE_GROUP_DIR/summary.tsv" \
    "$SSH_TARGET:$REMOTE_GROUP_DIR/build.log" \
    "$SSH_TARGET:$REMOTE_GROUP_DIR/driver.log" \
    "$LOCAL_GROUP_DIR/"

echo
echo "Generated locally:"
echo "  SVG      : $LOCAL_GROUP_DIR/flame/flamegraph_hpc_redis_server_only_${LABEL}.svg"
echo "  collapsed: $LOCAL_GROUP_DIR/flame/flamegraph_hpc_redis_server_only_${LABEL}.collapsed.txt"
echo "  meta     : $LOCAL_GROUP_DIR/flame/flamegraph_hpc_redis_server_only_${LABEL}.meta.txt"
