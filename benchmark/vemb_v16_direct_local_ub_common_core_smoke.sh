#!/usr/bin/env bash
# Validate the common-core direct-local UB path on host 111. The benchmark
# always bootstraps topology over TCP; the published owner endpoint selects
# the UB data transport after that refresh.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
SERVER="${SERVER:-$ROOT_DIR/src/vemb_v16_server}"
BENCH="${BENCH:-$ROOT_DIR/benchmark/vemb_v16_bench}"
TOPOLOGY_CTL="${TOPOLOGY_CTL:-$ROOT_DIR/benchmark/vemb_v16_topology_ctl}"

CONTROL_HOST="${CONTROL_HOST:-127.0.0.1}"
PORT="${PORT:-6396}"
DIM="${DIM:-300}"
MAX_VECTORS="${MAX_VECTORS:-1024}"
PREFILL="${PREFILL:-100}"
OPS="${OPS:-100}"
TIMEOUT_MS="${TIMEOUT_MS:-15000}"
TOPOLOGY_EPOCH="${TOPOLOGY_EPOCH:-1}"
BUILD="${BUILD:-1}"
RESET_WARM="${RESET_WARM:-1}"
KEEP_SERVER="${KEEP_SERVER:-0}"

# These are the verified host-111 direct-local resources. Keep warm storage
# separate from the two channel pools: all three mappings start at offset 0.
AERON_UB_PATH="${AERON_UB_PATH:-/dev/obmm_shmdev3}"
AERON_RESPONSE_UB_PATH="${AERON_RESPONSE_UB_PATH:-/dev/obmm_shmdev6}"
WARM_UB_PATH="${WARM_UB_PATH:-/dev/obmm_shmdev4}"
WARM_MMAP_OFFSET="${WARM_MMAP_OFFSET:-0}"
WARM_REGION_ID="${WARM_REGION_ID:-1}"
LOG_DIR="${LOG_DIR:-/tmp/vemb_v16_direct_local_ub_$(date +%Y%m%d_%H%M%S)_$$}"
SERVER_LOG="$LOG_DIR/server.log"
TOPOLOGY_LOG="$LOG_DIR/topology.log"
BENCH_LOG="$LOG_DIR/bench.log"
PID_FILE="$LOG_DIR/server.pid"
SERVER_PID=""

usage() {
    cat <<'USAGE'
Validate direct-local UB-Aeron through the vemb_v16_bench common core.

Run this on 111. The seed and advertised owner are both 127.0.0.1, so ATTACH
paths are mapped locally. The topology-control connection remains TCP; the
published owner endpoint is AERON and therefore exercises UB request/response
rings and the warm-handle read path.

Useful overrides:
  PORT=6396 DIM=300 PREFILL=100 OPS=100 TIMEOUT_MS=15000
  AERON_UB_PATH=/dev/obmm_shmdev3
  AERON_RESPONSE_UB_PATH=/dev/obmm_shmdev6
  WARM_UB_PATH=/dev/obmm_shmdev4 WARM_MMAP_OFFSET=0
  BUILD=0                 reuse already-built binaries
  RESET_WARM=0            do not reset the isolated warm layout
  KEEP_SERVER=1           leave the smoke server running after success
  LOG_DIR=/tmp/...        preserve artifacts in a selected directory

The script refuses to use a busy port or UB device. RESET_WARM=1 is safe only
after that exclusivity check has passed.
USAGE
}

die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

is_uint() {
    case "$1" in
        ''|*[!0-9]*) return 1 ;;
        *) return 0 ;;
    esac
}

require_binary() {
    [ -x "$1" ] || die "missing executable: $1"
}

port_in_use() {
    if command -v ss >/dev/null 2>&1; then
        ss -H -ltn | awk -v port="$PORT" '$4 ~ (":" port "$") { found = 1 } END { exit !found }'
        return
    fi
    if command -v lsof >/dev/null 2>&1; then
        lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1
        return
    fi
    die "need ss or lsof to check TCP port ownership"
}

check_ub_paths_idle() {
    command -v lsof >/dev/null 2>&1 ||
        die "lsof is required to verify exclusive UB device ownership"

    local path holders
    local -a paths=("$AERON_UB_PATH" "$AERON_RESPONSE_UB_PATH" "$WARM_UB_PATH")
    local -a seen=()
    for path in "${paths[@]}"; do
        local duplicate=0
        local prior
        for prior in "${seen[@]}"; do
            [ "$path" = "$prior" ] && duplicate=1
        done
        [ "$duplicate" -eq 0 ] || continue
        seen+=("$path")
        [ -r "$path" ] && [ -w "$path" ] ||
            die "UB device is not readable/writable: $path"
        holders="$(lsof -t "$path" 2>/dev/null || true)"
        [ -z "$holders" ] ||
            die "UB device is already in use: $path (pids: ${holders//$'\n'/,})"
    done
}

stop_server() {
    [ -n "$SERVER_PID" ] || return 0
    [ "$KEEP_SERVER" = "1" ] && return 0
    if kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}

cleanup() {
    local status=$?
    stop_server
    if [ "$status" -ne 0 ]; then
        printf 'artifacts: %s\n' "$LOG_DIR" >&2
        [ -f "$SERVER_LOG" ] && tail -100 "$SERVER_LOG" >&2 || true
    fi
}
trap cleanup EXIT

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
    usage
    exit 0
fi

[ "$CONTROL_HOST" = "127.0.0.1" ] ||
    die "CONTROL_HOST must remain 127.0.0.1 for direct-local validation"
for value in "$PORT" "$DIM" "$MAX_VECTORS" "$PREFILL" "$OPS" "$TIMEOUT_MS" \
             "$TOPOLOGY_EPOCH" "$WARM_MMAP_OFFSET" "$WARM_REGION_ID"; do
    is_uint "$value" || die "numeric configuration is invalid: $value"
done
[ "$PORT" -gt 0 ] && [ "$PORT" -le 65535 ] || die "PORT must be in [1, 65535]"
[ "$DIM" -gt 0 ] || die "DIM must be positive"
[ "$MAX_VECTORS" -ge "$PREFILL" ] || die "MAX_VECTORS must cover PREFILL"
case "$BUILD" in 0|1) ;; *) die "BUILD must be 0 or 1" ;; esac
case "$RESET_WARM" in 0|1) ;; *) die "RESET_WARM must be 0 or 1" ;; esac
case "$KEEP_SERVER" in 0|1) ;; *) die "KEEP_SERVER must be 0 or 1" ;; esac

mkdir -p "$LOG_DIR"
if port_in_use; then
    die "TCP port is already listening: $CONTROL_HOST:$PORT"
fi
check_ub_paths_idle

if [ "$BUILD" = "1" ]; then
    make -C "$ROOT_DIR/src" vemb_v16_server
    make -C "$ROOT_DIR/benchmark" vemb_v16_bench vemb_v16_topology_ctl
fi
require_binary "$SERVER"
require_binary "$BENCH"
require_binary "$TOPOLOGY_CTL"

if "$BENCH" --help 2>&1 | grep -q -- '--client-topology'; then
    die "vemb_v16_bench still exposes the removed --client-topology option"
fi

server_args=(
    --transport aeron
    --tcp-host "$CONTROL_HOST"
    --tcp-port "$PORT"
    --aeron-ub-path "$AERON_UB_PATH"
    --aeron-response-ub-path "$AERON_RESPONSE_UB_PATH"
    --proxy-io-threads 1
    --supernode-workers 1
    --vector-region "$WARM_UB_PATH"
    --warm-backend ub
    --warm-mmap-offset "$WARM_MMAP_OFFSET"
    --region-id "$WARM_REGION_ID"
    --dim "$DIM"
    --max-vectors "$MAX_VECTORS"
    --loglevel verbose
)
if [ "$RESET_WARM" = "1" ]; then
    server_args+=(--reset-warm-regions)
fi

printf '[start] server=%s control=tcp://%s:%s request=%s response=%s warm=%s@%s\n' \
    "$SERVER" "$CONTROL_HOST" "$PORT" "$AERON_UB_PATH" \
    "$AERON_RESPONSE_UB_PATH" "$WARM_UB_PATH" "$WARM_MMAP_OFFSET"
"$SERVER" "${server_args[@]}" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
printf '%s\n' "$SERVER_PID" >"$PID_FILE"

for _ in $(seq 1 100); do
    if "$TOPOLOGY_CTL" --get --transport tcp --host "$CONTROL_HOST" \
        --port "$PORT" --timeout-ms 1000 >"$TOPOLOGY_LOG" 2>&1; then
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        die "server exited before TCP control became ready"
    fi
    sleep 0.1
done
grep -q '^status=0$' "$TOPOLOGY_LOG" || die "TCP control was not ready"

"$TOPOLOGY_CTL" --set --transport aeron --host "$CONTROL_HOST" \
    --port "$PORT" --epoch "$TOPOLOGY_EPOCH" \
    --min-write-epoch "$TOPOLOGY_EPOCH" --active 0 --standby 0 \
    --owner-endpoints "0=${CONTROL_HOST}:${PORT}" --timeout-ms "$TIMEOUT_MS" \
    >"$TOPOLOGY_LOG" 2>&1
grep -q '^status=0$' "$TOPOLOGY_LOG" || die "failed to publish AERON owner topology"

"$TOPOLOGY_CTL" --get --transport tcp --host "$CONTROL_HOST" \
    --port "$PORT" --timeout-ms "$TIMEOUT_MS" >>"$TOPOLOGY_LOG" 2>&1
grep -q "endpoint\[0\]=owner:0 transport:aeron host:${CONTROL_HOST} port:${PORT}" \
    "$TOPOLOGY_LOG" || die "topology does not advertise direct-local AERON owner"

printf '[run] common-core VADD prefill + VEMB_HANDLE read\n'
"$BENCH" --endpoints "${CONTROL_HOST}:${PORT}" --dim "$DIM" \
    --prefill "$PREFILL" --keyspace "$PREFILL" --ops "$OPS" --threads 1 \
    --pipeline 1 --mode vemb-handle --timeout-ms "$TIMEOUT_MS" --no-pin \
    >"$BENCH_LOG" 2>&1
cat "$BENCH_LOG"

grep -q '^\[setup\] bootstrap=tcp topology-data=owner-fixed mode=vemb-handle ' \
    "$BENCH_LOG" || die "benchmark did not use the common-core bootstrap path"
grep -q "^\[done\] mode=vemb-handle threads=1 ok=${OPS} fail=0 " "$BENCH_LOG" ||
    die "common-core direct-local UB read did not finish cleanly"
grep -q "read_bytes=$((OPS * DIM * 4))" "$BENCH_LOG" ||
    die "VEMB_HANDLE workload did not materialize every warm vector"

for _ in $(seq 1 50); do
    closing_count="$(grep -c 'vemb_v16 channel closing:.*transport=1' "$SERVER_LOG" || true)"
    [ "$closing_count" -ge 2 ] && break
    sleep 0.1
done
attached_count="$(grep -c 'aeron ATTACH ok:' "$SERVER_LOG" || true)"
closing_count="$(grep -c 'vemb_v16 channel closing:.*transport=1' "$SERVER_LOG" || true)"
[ "$attached_count" -ge 2 ] || die "server did not complete UB AERON ATTACH"
[ "$closing_count" -ge 2 ] || die "server did not receive UB channel close controls"

printf '[ok] direct-local UB common-core smoke passed: attached=%s closed=%s artifacts=%s\n' \
    "$attached_count" "$closing_count" "$LOG_DIR"
if [ "$KEEP_SERVER" = "1" ]; then
    printf '[keep] server pid=%s control=tcp://%s:%s\n' \
        "$SERVER_PID" "$CONTROL_HOST" "$PORT"
fi
