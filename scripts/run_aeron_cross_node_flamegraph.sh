#!/usr/bin/env bash
# Run a fresh VEMB v16 cross-node read benchmark, capture process-wide server
# and CLI user+kernel flamegraphs on their respective hosts, then archive both
# sides locally. Server sampling explicitly binds every redis-server TID so
# SuperNode workers cannot be omitted by a process-leader-only perf attachment.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"

SERVER_NODE="${SERVER_NODE:-192.168.90.111}"
CLIENT_NODE="${CLIENT_NODE:-192.168.90.112}"
SSH_USER="${SSH_USER:-root}"
SSH_PORT="${SSH_PORT:-22}"
SERVER_ROOT="${SERVER_ROOT:-/root/szz/codespace/hpc-redis}"
CLIENT_ROOT="${CLIENT_ROOT:-/root/szz/codespace/hpc-redis}"
FLAMEGRAPH_DIR="${FLAMEGRAPH_DIR:-/root/FlameGraph}"

SERVER_IP="${SERVER_IP:-192.168.90.111}"
PORT="${PORT:-6395}"
SERVER_MANIFEST="${SERVER_MANIFEST:-/tmp/vemb_perf_warm_111.yaml}"
SERVER_REQUEST_UB_PATH="${SERVER_REQUEST_UB_PATH:-/dev/obmm_shmdev3}"
SERVER_RESPONSE_UB_PATH="${SERVER_RESPONSE_UB_PATH:-/dev/obmm_shmdev6}"
SERVER_WARM_UB_PATH="${SERVER_WARM_UB_PATH:-/dev/obmm_shmdev4}"
CLIENT_REQUEST_UB_PATH="${CLIENT_REQUEST_UB_PATH:-/dev/obmm_shmdev7}"
CLIENT_RESPONSE_UB_PATH="${CLIENT_RESPONSE_UB_PATH:-/dev/obmm_shmdev2}"
CLIENT_WARM_UB_PATH="${CLIENT_WARM_UB_PATH:-/dev/obmm_shmdev8}"

DIM="${DIM:-300}"
MAX_VECTORS="${MAX_VECTORS:-131072}"
NUM_KEYS="${NUM_KEYS:-100000}"
KEY_PATTERN="${KEY_PATTERN:-R:R}"
ZIPF_S="${ZIPF_S:-}"
RUN_ID="${RUN_ID:-aeron_cross_$(date +%Y%m%d_%H%M%S)}"
KEY_PREFIX="${KEY_PREFIX:-vemb-cross-flame-${RUN_ID}:}"

THREADS="${THREADS:-64}"
CLIENTS="${CLIENTS:-4}"
PIPELINE="${PIPELINE:-32}"
BATCH_REQUEST_SIZE="${BATCH_REQUEST_SIZE:-32}"
BATCH_MAX_DELAY_US="${BATCH_MAX_DELAY_US:-0}"
L1_ENTRIES="${L1_ENTRIES:-0}"
PROXY_REQUEST_BATCH="${PROXY_REQUEST_BATCH:-$BATCH_REQUEST_SIZE}"
PROXY_RESPONSE_BATCH="${PROXY_RESPONSE_BATCH:-$BATCH_REQUEST_SIZE}"
PROXY_QUEUE_BATCH="${PROXY_QUEUE_BATCH:-$BATCH_REQUEST_SIZE}"
PIO="${PIO:-21}"
SNW="${SNW:-21}"
SERVER_CPU_MASK="${SERVER_CPU_MASK:-0-47}"
CLIENT_CPU_MASK="${CLIENT_CPU_MASK:-96-191}"

TEST_TIME="${TEST_TIME:-30}"
SERVER_FLAME_DURATION="${SERVER_FLAME_DURATION:-25}"
FREQ="${FREQ:-99}"
EVENT="${EVENT:-cycles}"
BUILD="${BUILD:-verify}"
KEEP_SERVER="${KEEP_SERVER:-0}"
REQUIRE_VEMB_THREAD_SAMPLES="${REQUIRE_VEMB_THREAD_SAMPLES:-1}"
MAX_FOREIGN_CPU_PCT="${MAX_FOREIGN_CPU_PCT:-10}"
MAX_FOREIGN_TOTAL_CPU_PCT="${MAX_FOREIGN_TOTAL_CPU_PCT:-20}"
MAX_FOREIGN_CPUSET_BUSY_PCT="${MAX_FOREIGN_CPUSET_BUSY_PCT:-10}"
MAX_FOREIGN_RSS_MB="${MAX_FOREIGN_RSS_MB:-256}"
KILL_OPENCODE="${KILL_OPENCODE:-1}"
KILL_MUTAGEN="${KILL_MUTAGEN:-1}"
DRY_RUN="${DRY_RUN:-0}"

REMOTE_TMP_ROOT="${REMOTE_TMP_ROOT:-/tmp}"
REMOTE_RUN_DIR="${REMOTE_RUN_DIR:-$REMOTE_TMP_ROOT/$RUN_ID}"
LOCAL_ROOT="${LOCAL_ROOT:-$ROOT_DIR/perf/$RUN_ID}"

SERVER_PEER="$SSH_USER@$SERVER_NODE"
CLIENT_PEER="$SSH_USER@$CLIENT_NODE"
SSH_OPTIONS=(-p "$SSH_PORT")
SCP_OPTIONS=(-P "$SSH_PORT")
server_started=0

usage() {
    cat <<'USAGE'
Run a fresh VEMB v16 cross-node read benchmark and generate full process
user+kernel flamegraphs on the server and client hosts.

Usage:
  bash scripts/run_aeron_cross_node_flamegraph.sh
  KEY_PATTERN=Z:Z ZIPF_S=1.5 bash scripts/run_aeron_cross_node_flamegraph.sh

The default is the validated 111 -> 112 setup:
  server=192.168.90.111:6395, client=192.168.90.112
  100k R:R reads, dim=300, t=64, c=4, pipeline=32, batch=32
  server request/response=/dev/obmm_shmdev3,/dev/obmm_shmdev6
  client request/response/warm=/dev/obmm_shmdev7,/dev/obmm_shmdev2,/dev/obmm_shmdev8

Important environment variables:
  SERVER_NODE CLIENT_NODE SSH_USER SSH_PORT SERVER_ROOT CLIENT_ROOT SERVER_IP PORT
  SERVER_MANIFEST SERVER_REQUEST_UB_PATH SERVER_RESPONSE_UB_PATH SERVER_WARM_UB_PATH
  CLIENT_REQUEST_UB_PATH CLIENT_RESPONSE_UB_PATH CLIENT_WARM_UB_PATH
  NUM_KEYS KEY_PATTERN=R:R|Z:Z ZIPF_S KEY_PREFIX DIM MAX_VECTORS
  THREADS CLIENTS PIPELINE BATCH_REQUEST_SIZE BATCH_MAX_DELAY_US L1_ENTRIES
  PROXY_REQUEST_BATCH PROXY_RESPONSE_BATCH PROXY_QUEUE_BATCH
  PIO SNW SERVER_CPU_MASK CLIENT_CPU_MASK
  TEST_TIME SERVER_FLAME_DURATION FREQ EVENT
  MAX_FOREIGN_CPU_PCT MAX_FOREIGN_TOTAL_CPU_PCT MAX_FOREIGN_CPUSET_BUSY_PCT MAX_FOREIGN_RSS_MB
  KILL_OPENCODE=0|1 KILL_MUTAGEN=0|1
  BUILD=verify|build KEEP_SERVER=0|1 REQUIRE_VEMB_THREAD_SAMPLES=0|1
  RUN_ID REMOTE_RUN_DIR LOCAL_ROOT DRY_RUN=0|1

BUILD=verify requires current O3/LTO/SVE build stamps. BUILD=build force-builds
the server on SERVER_NODE and SDK/memtier on CLIENT_NODE before the run. The
three server-internal batch macros default to BATCH_REQUEST_SIZE and are
recorded in the server build stamp.

Use KEY_PATTERN=R:R for uniform random reads. Use KEY_PATTERN=Z:Z together
with a positive ZIPF_S such as 1.0, 1.2, or 1.5 for the documented Zipf hot-key
scenarios. Prefill is always sequential S:S so every generated read key exists.

Before either role starts, both hosts must pass the load gate. It samples all
processes with pidstat for one second and the target CPU set through two
/proc/stat snapshots. It fails when a process exceeds MAX_FOREIGN_CPU_PCT
(10%), all processes exceed MAX_FOREIGN_TOTAL_CPU_PCT (20%), target CPU-set
busy time exceeds MAX_FOREIGN_CPUSET_BUSY_PCT (10%), or a process RSS exceeds
MAX_FOREIGN_RSS_MB (256 MiB).
By default, the opencode tmux session and residual exact-name opencode
processes are SIGKILLed. Each mutagen-agent's direct parent and the agent are
also SIGKILLed on both hosts before this load gate runs. Set KILL_OPENCODE=0
or KILL_MUTAGEN=0 to retain either process type.

The server capture enumerates all redis-server TIDs and uses perf --tid. It
fails by default unless both vemb-sn-* and supernode_pool_thread_main appear
in the resulting raw perf script. Generated remote artifacts are retained;
complete tar archives are pulled into LOCAL_ROOT/server and LOCAL_ROOT/client.

The server archive also includes CPU usage for SERVER_CPU_MASK. The raw
server.cpu.cpuset.mpstat.txt reports each pinned CPU's user, system, hard IRQ,
soft IRQ (si), iowait, and idle percentages. server.cpu.cpuset.summary.tsv
aggregates those percentages into equivalent total CPU cores, while
server.cpu.process.tsv reports redis-server user/system/total core equivalents.
USAGE
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

status() {
    printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

is_uint() {
    case "$1" in
        ''|*[!0-9]*) return 1 ;;
        *) return 0 ;;
    esac
}

server_ssh() {
    ssh "${SSH_OPTIONS[@]}" "$SERVER_PEER" "$@"
}

client_ssh() {
    ssh "${SSH_OPTIONS[@]}" "$CLIENT_PEER" "$@"
}

cleanup_server() {
    [ "$server_started" -eq 1 ] || return 0
    [ "$KEEP_SERVER" -eq 0 ] || return 0
    status "stopping test server on $SERVER_NODE:$PORT"
    server_ssh bash -s -- "$REMOTE_RUN_DIR/server.pid" "$PORT" <<'REMOTE_STOP' || true
set -euo pipefail
pidfile=$1
port=$2
pid="$(cat "$pidfile" 2>/dev/null || true)"
if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid"
fi
for _ in $(seq 1 100); do
    alive=0
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null && alive=1
    if [ "$alive" -eq 0 ] && ! ss -ltn | grep -q ":$port"; then
        exit 0
    fi
    sleep 0.1
done
echo "ERROR: test server did not release port $port" >&2
exit 1
REMOTE_STOP
}

trap cleanup_server EXIT

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
    usage
    exit 0
fi

[ "$BUILD" = verify ] || [ "$BUILD" = build ] ||
    die "BUILD must be verify or build"
[ "$KEEP_SERVER" = 0 ] || [ "$KEEP_SERVER" = 1 ] ||
    die "KEEP_SERVER must be 0 or 1"
[ "$REQUIRE_VEMB_THREAD_SAMPLES" = 0 ] || [ "$REQUIRE_VEMB_THREAD_SAMPLES" = 1 ] ||
    die "REQUIRE_VEMB_THREAD_SAMPLES must be 0 or 1"
[ "$KILL_OPENCODE" = 0 ] || [ "$KILL_OPENCODE" = 1 ] ||
    die "KILL_OPENCODE must be 0 or 1"
[ "$KILL_MUTAGEN" = 0 ] || [ "$KILL_MUTAGEN" = 1 ] ||
    die "KILL_MUTAGEN must be 0 or 1"
[ "$DRY_RUN" = 0 ] || [ "$DRY_RUN" = 1 ] || die "DRY_RUN must be 0 or 1"

for value in "$PORT" "$DIM" "$MAX_VECTORS" "$NUM_KEYS" "$THREADS" "$CLIENTS" \
    "$PIPELINE" "$BATCH_REQUEST_SIZE" "$BATCH_MAX_DELAY_US" "$L1_ENTRIES" \
    "$PROXY_REQUEST_BATCH" "$PROXY_RESPONSE_BATCH" "$PROXY_QUEUE_BATCH" \
    "$PIO" "$SNW" \
    "$TEST_TIME" "$SERVER_FLAME_DURATION" "$FREQ" \
    "$MAX_FOREIGN_CPU_PCT" "$MAX_FOREIGN_TOTAL_CPU_PCT" \
    "$MAX_FOREIGN_CPUSET_BUSY_PCT" "$MAX_FOREIGN_RSS_MB"; do
    is_uint "$value" || die "numeric parameters must be non-negative integers"
done
[ "$PORT" -gt 0 ] && [ "$DIM" -gt 0 ] && [ "$MAX_VECTORS" -gt 0 ] &&
    [ "$NUM_KEYS" -gt 0 ] && [ "$THREADS" -gt 0 ] && [ "$CLIENTS" -gt 0 ] &&
    [ "$PIPELINE" -gt 0 ] && [ "$BATCH_REQUEST_SIZE" -gt 0 ] &&
    [ "$PROXY_REQUEST_BATCH" -gt 0 ] && [ "$PROXY_RESPONSE_BATCH" -gt 0 ] &&
    [ "$PROXY_QUEUE_BATCH" -gt 0 ] &&
    [ "$PIO" -gt 0 ] && [ "$SNW" -gt 0 ] &&
    [ "$TEST_TIME" -gt 0 ] && [ "$SERVER_FLAME_DURATION" -gt 0 ] && [ "$FREQ" -gt 0 ] ||
    die "positive parameters must be greater than zero"
[ "$SERVER_FLAME_DURATION" -lt "$TEST_TIME" ] ||
    die "SERVER_FLAME_DURATION must be less than TEST_TIME"
if [ "$L1_ENTRIES" -ne 0 ]; then
    [ "$L1_ENTRIES" -ge 4 ] && [ $((L1_ENTRIES % 4)) -eq 0 ] ||
        die "L1_ENTRIES must be 0 or 4 times a power of two"
    l1_sets=$((L1_ENTRIES / 4))
    [ $((l1_sets & (l1_sets - 1))) -eq 0 ] ||
        die "L1_ENTRIES must be 0 or 4 times a power of two"
fi

case "$KEY_PATTERN" in
    R:R|S:S|Z:Z) ;;
    *) die "KEY_PATTERN must be R:R, S:S, or Z:Z" ;;
esac
if [ "$KEY_PATTERN" = Z:Z ]; then
    [ -n "$ZIPF_S" ] || die "ZIPF_S is required when KEY_PATTERN=Z:Z"
    awk -v s="$ZIPF_S" 'BEGIN { exit !(s ~ /^[0-9]+([.][0-9]+)?$/ && s > 0) }' ||
        die "ZIPF_S must be a positive decimal when KEY_PATTERN=Z:Z"
fi

pattern_label="${KEY_PATTERN//:/}"
if [ "$KEY_PATTERN" = Z:Z ]; then
    pattern_label="Z${ZIPF_S}"
fi
run_label="keys${NUM_KEYS}_${pattern_label}_d${DIM}_p${PIO}_s${SNW}_t${THREADS}_c${CLIENTS}_pipe${PIPELINE}_b${BATCH_REQUEST_SIZE}_l1${L1_ENTRIES}_prb${PROXY_REQUEST_BATCH}_pob${PROXY_RESPONSE_BATCH}_pqb${PROXY_QUEUE_BATCH}_delay${BATCH_MAX_DELAY_US}us_test${TEST_TIME}s_flame${SERVER_FLAME_DURATION}s_run${RUN_ID}"

mkdir -p "$LOCAL_ROOT/server" "$LOCAL_ROOT/client"

status "cross-node flamegraph run: $RUN_ID"
status "server=$SERVER_PEER:$SERVER_ROOT client=$CLIENT_PEER:$CLIENT_ROOT"
status "keys=$NUM_KEYS pattern=$KEY_PATTERN dim=$DIM t=$THREADS c=$CLIENTS pipeline=$PIPELINE batch=$BATCH_REQUEST_SIZE l1_entries=$L1_ENTRIES server_internal_batches=$PROXY_REQUEST_BATCH:$PROXY_RESPONSE_BATCH:$PROXY_QUEUE_BATCH"
status "server flame=${SERVER_FLAME_DURATION}s@$FREQ event=$EVENT build=$BUILD local=$LOCAL_ROOT"

if [ "$DRY_RUN" -eq 1 ]; then
    status "DRY_RUN=1; no SSH, build, benchmark, or perf command was executed"
    exit 0
fi

status "checking build stamps"
if [ "$BUILD" = build ]; then
    server_ssh "cd '$SERVER_ROOT' && PROXY_REQUEST_BATCH='$PROXY_REQUEST_BATCH' PROXY_RESPONSE_BATCH='$PROXY_RESPONSE_BATCH' PROXY_QUEUE_BATCH='$PROXY_QUEUE_BATCH' bash scripts/vemb_v16_build_stamp.sh build server"
    client_ssh "cd '$CLIENT_ROOT' && bash scripts/vemb_v16_build_stamp.sh build client"
else
    server_ssh "cd '$SERVER_ROOT' && PROXY_REQUEST_BATCH='$PROXY_REQUEST_BATCH' PROXY_RESPONSE_BATCH='$PROXY_RESPONSE_BATCH' PROXY_QUEUE_BATCH='$PROXY_QUEUE_BATCH' bash scripts/vemb_v16_build_stamp.sh verify server"
    client_ssh "cd '$CLIENT_ROOT' && bash scripts/vemb_v16_build_stamp.sh verify client"
fi

host_load_gate() {
    local role=$1
    local peer_fn=$2
    local cpu_mask=$3

    "$peer_fn" bash -s -- \
        "$REMOTE_RUN_DIR" "$role" "$MAX_FOREIGN_CPU_PCT" \
        "$MAX_FOREIGN_TOTAL_CPU_PCT" "$MAX_FOREIGN_RSS_MB" "$KILL_OPENCODE" \
        "$KILL_MUTAGEN" "$cpu_mask" "$MAX_FOREIGN_CPUSET_BUSY_PCT" <<'REMOTE_HOST_LOAD_GATE'
set -euo pipefail

run=$1
role=$2
per_process_limit=$3
total_limit=$4
rss_limit_mb=$5
kill_opencode=$6
kill_mutagen=$7
cpu_mask=$8
cpuset_busy_limit=$9
mkdir -p "$run"
command -v pidstat >/dev/null || { echo "missing pidstat for $role load gate" >&2; exit 1; }

if [ "$kill_opencode" = 1 ]; then
    tmux kill-session -t opencode 2>/dev/null || true
    opencode_pids="$(pgrep -x opencode || true)"
    if [ -n "$opencode_pids" ]; then
        printf 'removing opencode on %s: %s\n' "$role" "$opencode_pids"
        kill -9 $opencode_pids
        for _ in $(seq 1 50); do
            pgrep -x opencode >/dev/null || break
            sleep 0.1
        done
        ! pgrep -x opencode >/dev/null || {
            echo "failed to remove opencode on $role" >&2
            exit 1
        }
    fi
fi

if [ "$kill_mutagen" = 1 ]; then
    mutagen_pids="$(pgrep -x mutagen-agent || true)"
    for pid in $mutagen_pids; do
        parent="$(ps -o ppid= -p "$pid" | tr -d ' ')"
        if [ -n "$parent" ] && [ "$parent" -gt 1 ]; then
            printf 'removing mutagen parent on %s: agent=%s parent=%s\n' \
                "$role" "$pid" "$parent"
            kill -9 "$parent" 2>/dev/null || true
        fi
        kill -9 "$pid" 2>/dev/null || true
    done
    ! pgrep -x mutagen-agent >/dev/null || {
        echo "failed to remove mutagen-agent on $role" >&2
        exit 1
    }
fi

cpu_raw="$run/$role.preflight.pidstat.txt"
cpu_blockers="$run/$role.preflight.cpu-blockers.tsv"
rss_blockers="$run/$role.preflight.rss-blockers.tsv"
cpuset_busy="$run/$role.preflight.cpuset.tsv"
LC_ALL=C pidstat -u -p ALL 1 1 >"$cpu_raw"
awk -v limit="$per_process_limit" '
$1 == "Average:" && $3 ~ /^[0-9]+$/ && $8 ~ /^[0-9]+([.][0-9]+)?$/ && $8 > limit {
    printf "%s\t%.2f\t%s\n", $3, $8, $10
}' "$cpu_raw" >"$cpu_blockers"
total_cpu="$(awk '$1 == "Average:" && $3 ~ /^[0-9]+$/ && $8 ~ /^[0-9]+([.][0-9]+)?$/ { total += $8 } END { printf "%.2f", total }' "$cpu_raw")"
ps -eo pid=,rss=,comm= | awk -v limit_mb="$rss_limit_mb" '
$2 > limit_mb * 1024 { printf "%s\t%.2f\t%s\n", $1, $2 / 1024, $3 }' >"$rss_blockers"

sample_cpuset_stat() {
    awk -v mask="$cpu_mask" '
    function selected(cpu,    parts, n, i, bounds) {
        n = split(mask, parts, ",")
        for (i = 1; i <= n; i++) {
            if (parts[i] ~ /^[0-9]+-[0-9]+$/) {
                split(parts[i], bounds, "-")
                if (cpu >= bounds[1] && cpu <= bounds[2])
                    return 1
            } else if (parts[i] ~ /^[0-9]+$/ && cpu == parts[i]) {
                return 1
            }
        }
        return 0
    }
    $1 ~ /^cpu[0-9]+$/ {
        cpu = substr($1, 4) + 0
        if (!selected(cpu))
            next
        count++
        for (i = 2; i <= NF && i <= 11; i++)
            total += $i
        idle += $5
        if (NF >= 6)
            idle += $6
    }
    END {
        if (count == 0)
            exit 1
        printf "%.0f %.0f %u\n", total, idle, count
    }' /proc/stat
}

read -r cpuset_total_start cpuset_idle_start cpuset_cpu_count < <(sample_cpuset_stat)
sleep 1
read -r cpuset_total_end cpuset_idle_end cpuset_cpu_count_end < <(sample_cpuset_stat)
[ "$cpuset_cpu_count" = "$cpuset_cpu_count_end" ] || {
    echo "ERROR: $role CPU-set changed during load gate" >&2
    exit 1
}
VEMB_CPUSET_MASK="$cpu_mask" awk -v total_start="$cpuset_total_start" \
    -v idle_start="$cpuset_idle_start" -v total_end="$cpuset_total_end" \
    -v idle_end="$cpuset_idle_end" -v cpu_count="$cpuset_cpu_count" '
BEGIN {
    delta_total = total_end - total_start
    delta_idle = idle_end - idle_start
    if (delta_total <= 0 || delta_idle > delta_total)
        exit 1
    busy_pct = 100 * (delta_total - delta_idle) / delta_total
    printf "cpu_mask\tcpu_count\twindow_total_jiffies\tbusy_pct\tbusy_core_equiv\n"
    printf "%s\t%s\t%.0f\t%.3f\t%.6f\n", ENVIRON["VEMB_CPUSET_MASK"], cpu_count, \
           delta_total, busy_pct, busy_pct * cpu_count / 100
}' >"$cpuset_busy"
cpuset_busy_pct="$(awk -F '\t' 'NR == 2 { print $4 }' "$cpuset_busy")"

if [ -s "$cpu_blockers" ] || [ -s "$rss_blockers" ] || \
    awk -v total="$total_cpu" -v limit="$total_limit" 'BEGIN { exit !(total > limit) }' || \
    awk -v busy="$cpuset_busy_pct" -v limit="$cpuset_busy_limit" 'BEGIN { exit !(busy > limit) }'; then
    echo "ERROR: $role host load gate failed: per-process CPU <= ${per_process_limit}%, total CPU <= ${total_limit}%, target CPU-set busy <= ${cpuset_busy_limit}%, RSS <= ${rss_limit_mb} MiB" >&2
    printf 'measured_total_cpu_pct=%s\n' "$total_cpu" >&2
    printf 'measured_cpuset_busy_pct=%s cpuset=%s\n' "$cpuset_busy_pct" "$cpu_mask" >&2
    if [ -s "$cpu_blockers" ]; then
        printf 'cpu_blockers(pid cpu_pct command):\n' >&2
        cat "$cpu_blockers" >&2
    fi
    if [ -s "$rss_blockers" ]; then
        printf 'rss_blockers(pid rss_mib command):\n' >&2
        cat "$rss_blockers" >&2
    fi
    exit 1
fi
printf '%s load gate passed: total_cpu_pct=%s cpuset_busy_pct=%s cpuset=%s\n' \
    "$role" "$total_cpu" "$cpuset_busy_pct" "$cpu_mask"
REMOTE_HOST_LOAD_GATE
}

status "checking host CPU and memory load gates"
host_load_gate server server_ssh "$SERVER_CPU_MASK"
host_load_gate client client_ssh "$CLIENT_CPU_MASK"

status "preflighting server UB paths and starting fresh server"
server_ssh bash -s -- \
    "$SERVER_ROOT" "$REMOTE_RUN_DIR" "$PORT" "$SERVER_IP" "$SERVER_MANIFEST" \
    "$SERVER_REQUEST_UB_PATH" "$SERVER_RESPONSE_UB_PATH" "$SERVER_WARM_UB_PATH" \
    "$DIM" "$MAX_VECTORS" "$BATCH_REQUEST_SIZE" "$PIO" "$SNW" \
    "$SERVER_CPU_MASK" <<'REMOTE_SERVER_START'
set -euo pipefail

root=$1
run=$2
port=$3
server_ip=$4
manifest=$5
request_path=$6
response_path=$7
warm_path=$8
dim=$9
max_vectors=${10}
batch_size=${11}
proxy_io_threads=${12}
supernode_workers=${13}
cpu_mask=${14}

mkdir -p "$run"
pidfile="$run/server.pid"
log="$run/server.log"

[ -x "$root/src/redis-server" ] || { echo "missing redis-server" >&2; exit 1; }
[ -r "$manifest" ] || { echo "missing server manifest: $manifest" >&2; exit 1; }
for path in "$request_path" "$response_path" "$warm_path"; do
    [ -e "$path" ] || { echo "missing UB path: $path" >&2; exit 1; }
    holders="$(lsof -t "$path" 2>/dev/null || true)"
    [ -z "$holders" ] || {
        echo "UB path already in use: $path holders=$holders" >&2
        exit 1
    }
done
! ss -ltn | grep -q ":$port" || {
    echo "port already in use: $port" >&2
    exit 1
}

cd "$root"
setsid -f taskset -c "$cpu_mask" ./src/redis-server \
    --port "$port" --bind "$server_ip" --protected-mode no \
    --vemb-v16-enabled yes --vemb-v16-dim "$dim" \
    --vemb-v16-max-vectors "$max_vectors" \
    --vemb-v16-warm-regions-manifest "$manifest" \
    --vemb-v16-reset-warm-regions yes \
    --vemb-v16-transport aeron --vemb-v16-aeron-control tcp \
    --vemb-v16-aeron-ub-path "$request_path" \
    --vemb-v16-aeron-response-ub-path "$response_path" \
    --vemb-v16-proxy-io-threads "$proxy_io_threads" \
    --vemb-v16-supernode-workers "$supernode_workers" \
    --vemb-v16-batch-request-size "$batch_size" \
    --daemonize yes --pidfile "$pidfile" --logfile "$log" --loglevel notice

for _ in $(seq 1 100); do
    [ -r "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null &&
        ss -ltn | grep -q ":$port" && break
    sleep 0.1
done

pid="$(cat "$pidfile" 2>/dev/null || true)"
[ -n "$pid" ] && kill -0 "$pid" 2>/dev/null || {
    tail -80 "$log" >&2 || true
    exit 1
}
printf 'server_pid=%s\n' "$pid"
REMOTE_SERVER_START
server_started=1

status "preflighting client UB paths and prefilling $NUM_KEYS vectors"
client_ssh bash -s -- \
    "$CLIENT_ROOT" "$REMOTE_RUN_DIR" "$SERVER_IP" "$PORT" "$NUM_KEYS" \
    "$KEY_PREFIX" "$DIM" "$BATCH_REQUEST_SIZE" "$CLIENT_CPU_MASK" \
    "$CLIENT_REQUEST_UB_PATH" "$CLIENT_RESPONSE_UB_PATH" "$CLIENT_WARM_UB_PATH" <<'REMOTE_PREFILL'
set -euo pipefail

root=$1
run=$2
server_ip=$3
port=$4
keys=$5
prefix=$6
dim=$7
batch_size=$8
cpu_mask=$9
shift 9

for path in "$@"; do
    [ -e "$path" ] || { echo "missing UB path: $path" >&2; exit 1; }
    holders="$(lsof -t "$path" 2>/dev/null || true)"
    [ -z "$holders" ] || {
        echo "UB path already in use: $path holders=$holders" >&2
        exit 1
    }
done

mkdir -p "$run"
cd "$root/memtier_benchmark"
taskset -c "$cpu_mask" ./memtier_benchmark \
    --protocol=vemb_v16 --vemb-v16-endpoints="$server_ip:$port" \
    --threads=1 --clients=1 --pipeline="$batch_size" --requests="$keys" \
    --ratio=1:0 --key-prefix="$prefix" --key-minimum=1 --key-maximum="$keys" \
    --key-pattern=S:S --vemb-v16-dim="$dim" \
    --vemb-v16-transport=aeron-cross-node --hide-histogram \
    >"$run/prefill.log" 2>&1

! grep -q 'Connection error' "$run/prefill.log"
awk '/^Totals/ { found = 1; if ($2 > 0) ok = 1 } END { exit !(found && ok) }' \
    "$run/prefill.log"
REMOTE_PREFILL

status "starting all-TID server perf sampling"
server_ssh bash -s -- \
    "$REMOTE_RUN_DIR" "$PIO" "$SNW" \
    "$SERVER_FLAME_DURATION" "$FREQ" "$EVENT" <<'REMOTE_SERVER_PERF_START'
set -euo pipefail

run=$1
expected_io=$2
expected_sn=$3
duration=$4
freq=$5
event=$6
pid="$(cat "$run/server.pid")"

ps -T -p "$pid" -o tid=,comm= >"$run/server.threads.txt"
io="$(grep -c 'vemb-io-' "$run/server.threads.txt" || true)"
sn="$(grep -c 'vemb-sn-' "$run/server.threads.txt" || true)"
[ "$io" -eq "$expected_io" ] || {
    echo "expected $expected_io proxy workers, found $io" >&2
    exit 1
}
[ "$sn" -eq "$expected_sn" ] || {
    echo "expected $expected_sn supernode workers, found $sn" >&2
    exit 1
}
tids="$(awk '{ printf "%s%s", sep, $1; sep="," }' "$run/server.threads.txt")"
nohup perf record -F "$freq" -g -e "$event" --tid "$tids" \
    -o "$run/server.perf.data" -- sleep "$duration" \
    >"$run/server.perf.log" 2>&1 &
echo $! >"$run/server.perf.pid"
printf 'server_pid=%s io_threads=%s sn_threads=%s perf_pid=%s\n' \
    "$pid" "$io" "$sn" "$(cat "$run/server.perf.pid")"
REMOTE_SERVER_PERF_START

status "starting server CPU-set sampling on $SERVER_CPU_MASK"
server_ssh bash -s -- \
    "$REMOTE_RUN_DIR" "$SERVER_CPU_MASK" "$TEST_TIME" <<'REMOTE_SERVER_CPU_START'
set -euo pipefail

run=$1
cpu_mask=$2
duration=$3
pid="$(cat "$run/server.pid")"
command -v mpstat >/dev/null

sample_server_process_cpu() {
    local run=$1 pid=$2 duration=$3
    local start_ns start_utime start_stime end_ns end_utime end_stime hz

    read -r start_utime start_stime < <(awk '{ print $14, $15 }' "/proc/$pid/stat")
    start_ns="$(date +%s%N)"
    hz="$(getconf CLK_TCK)"
    printf '%s\t%s\t%s\t%s\n' "$start_ns" "$start_utime" "$start_stime" "$hz" \
        >"$run/server.cpu.process.start"
    sleep "$duration"
    read -r end_utime end_stime < <(awk '{ print $14, $15 }' "/proc/$pid/stat")
    end_ns="$(date +%s%N)"
    awk -v start_ns="$start_ns" -v end_ns="$end_ns" \
        -v start_utime="$start_utime" -v end_utime="$end_utime" \
        -v start_stime="$start_stime" -v end_stime="$end_stime" -v hz="$hz" '
    BEGIN {
        window_s = (end_ns - start_ns) / 1000000000
        user_s = (end_utime - start_utime) / hz
        system_s = (end_stime - start_stime) / hz
        total_s = user_s + system_s
        printf "metric\tseconds\tcore_equiv\n"
        printf "window\t%.6f\tNA\n", window_s
        printf "user\t%.6f\t%.6f\n", user_s, user_s / window_s
        printf "system\t%.6f\t%.6f\n", system_s, system_s / window_s
        printf "total\t%.6f\t%.6f\n", total_s, total_s / window_s
    }' >"$run/server.cpu.process.tsv"
}
process_sampler_cmd="$(declare -f sample_server_process_cpu)"
process_sampler_cmd+=$'\nsample_server_process_cpu "$@"'
nohup bash -c "$process_sampler_cmd" bash "$run" "$pid" "$duration" \
    >"$run/server.cpu.process.sampler.log" 2>&1 &
echo $! >"$run/server.cpu.process.sampler.pid"
nohup env LC_ALL=C mpstat -P "$cpu_mask" 1 "$duration" \
    >"$run/server.cpu.cpuset.mpstat.txt" 2>&1 &
echo $! >"$run/server.cpu.mpstat.pid"
REMOTE_SERVER_CPU_START

zipf_s_arg="${ZIPF_S:--}"

status "running CLI workload under perf on $CLIENT_NODE"
client_ssh bash -s -- \
    "$CLIENT_ROOT" "$REMOTE_RUN_DIR" "$SERVER_IP" "$PORT" "$NUM_KEYS" \
    "$KEY_PATTERN" "$zipf_s_arg" "$KEY_PREFIX" "$DIM" "$THREADS" "$CLIENTS" \
    "$PIPELINE" "$BATCH_REQUEST_SIZE" "$BATCH_MAX_DELAY_US" "$L1_ENTRIES" "$TEST_TIME" \
    "$CLIENT_CPU_MASK" "$FREQ" "$EVENT" "$FLAMEGRAPH_DIR" "$run_label" <<'REMOTE_CLIENT_PERF'
set -euo pipefail

root=$1
run=$2
server_ip=$3
port=$4
keys=$5
pattern=$6
zipf_s=$7
if [ "$zipf_s" = - ]; then
    zipf_s=
fi
prefix=$8
dim=$9
threads=${10}
clients=${11}
pipeline=${12}
batch_size=${13}
max_delay_us=${14}
l1_entries=${15}
test_time=${16}
cpu_mask=${17}
freq=${18}
event=${19}
flamegraph_dir=${20}
run_label=${21}

mkdir -p "$run"
[ -x "$root/memtier_benchmark/memtier_benchmark" ] || exit 1
[ -x "$flamegraph_dir/stackcollapse-perf.pl" ] || exit 1
[ -x "$flamegraph_dir/flamegraph.pl" ] || exit 1

zipf_args=()
if [ "$pattern" = Z:Z ]; then
    zipf_args=("--key-zipfian-s=$zipf_s")
fi
l1_args=()
if [ "$l1_entries" -ne 0 ]; then
    l1_args=("--vemb-v16-l1-entries=$l1_entries")
fi

cd "$root/memtier_benchmark"
timed_workload_cmd='run_timed_workload() {
    local cpu_mask=$1
    local workload_pid max_rss_kib=0 rss_kib status

    shift
    taskset -c "$cpu_mask" "$@" &
    workload_pid=$!
    while kill -0 "$workload_pid" 2>/dev/null; do
        rss_kib="$(awk '\''/VmHWM:/ { print $2; exit }'\'' "/proc/$workload_pid/status" 2>/dev/null || true)"
        if [ -n "$rss_kib" ] && [ "$rss_kib" -gt "$max_rss_kib" ]; then
            max_rss_kib=$rss_kib
        fi
        sleep 0.05
    done
    wait "$workload_pid"
    status=$?
    printf "l1_client_max_rss_kib=%s\\n" "$max_rss_kib"
    return "$status"
}
run_timed_workload "$@"'
perf record -F "$freq" -g -e "$event" -o "$run/client.perf.data" -- \
    /bin/bash -c "$timed_workload_cmd" bash "$cpu_mask" ./memtier_benchmark \
        --protocol=vemb_v16 --vemb-v16-endpoints="$server_ip:$port" \
        --threads="$threads" --clients="$clients" --pipeline="$pipeline" \
        --ratio=0:1 --key-minimum=1 --key-maximum="$keys" \
        --key-pattern="$pattern" --key-prefix="$prefix" --vemb-v16-dim="$dim" \
        --vemb-v16-handle --vemb-v16-transport=aeron-cross-node \
        --vemb-v16-batch-request-size="$batch_size" \
        --vemb-v16-batch-max-delay-us="$max_delay_us" \
        --test-time="$test_time" --hide-histogram "${zipf_args[@]}" "${l1_args[@]}" \
        >"$run/client.workload.log" 2>&1

if grep -q 'Connection error' "$run/client.workload.log"; then
    echo 'ERROR: client workload reported a connection error' >&2
    exit 1
fi
grep -q 'all workers joined' "$run/client.workload.log"
if grep -Eq 'fallback_v1=[1-9]|flush_backpressure=[1-9]|shared_vector_read_failures=[1-9]|status\[.*(nf|err|other)=[1-9]|handle_deref\[.*fail=[1-9]|l1_queue_full_fallbacks=[1-9]|l1_all_pinned=[1-9]|l1_(key|vector)_storage_exhausted=[1-9]|l1_stale_ref=[1-9]' \
    "$run/client.workload.log"; then
    echo 'ERROR: client workload reported a VEMB data-plane failure' >&2
    exit 1
fi
awk '/^Totals/ { found = 1; if ($2 > 0) ok = 1 } END { exit !(found && ok) }' \
    "$run/client.workload.log"
grep -q '^\[aeron\] l1-summary:' "$run/client.workload.log"

rss_kib="$(awk -F= '/^l1_client_max_rss_kib=/ { value=$2 } END { if (value == "") exit 1; print value }' "$run/client.workload.log")"
awk -v entries="$l1_entries" -v rss_kib="$rss_kib" '
function metric(name,    i, pair) {
    for (i = 1; i <= NF; i++) {
        if ($i ~ ("^" name "=")) {
            split($i, pair, "=")
            return pair[2]
        }
    }
    return "NA"
}
BEGIN {
    OFS = "\t"
    print "l1_entries\tops_sec\tp99_ms\tl1_hit\tl1_miss\tl1_insert\tl1_evict\tl1_hit_ratio\tl1_vector_bytes\tl1_ub_read_bytes_saved\tlocal_completion_count\tserver_items\tub_read_bytes\tclient_max_rss_kib"
}
/^Totals/ {
    ops_sec = $2
    p99_ms = $7
    have_totals = 1
}
/^\[aeron\] l1-summary:/ {
    l1_hit = metric("l1_hit")
    l1_miss = metric("l1_miss")
    l1_insert = metric("l1_insert")
    l1_evict = metric("l1_evict")
    l1_hit_ratio = metric("l1_hit_ratio")
    l1_vector_bytes = metric("l1_vector_bytes")
    l1_ub_read_bytes_saved = metric("l1_ub_read_bytes_saved")
    local_completion_count = metric("local_completion_count")
    server_items = metric("server_items")
    ub_read_bytes = metric("ub_read_bytes")
    have_l1_summary = 1
}
END {
    if (have_totals && have_l1_summary) {
        print entries, ops_sec, p99_ms, l1_hit, l1_miss, l1_insert, l1_evict, \
            l1_hit_ratio, l1_vector_bytes, l1_ub_read_bytes_saved, \
            local_completion_count, server_items, ub_read_bytes, rss_kib
    }
}
' "$run/client.workload.log" >"$run/client.l1_summary.tsv"
awk 'NR == 2 && $1 != "" { found = 1 } END { exit !found }' \
    "$run/client.l1_summary.tsv"

perf script -i "$run/client.perf.data" >"$run/client.perf.script"
grep -q '\[kernel.kallsyms\]' "$run/client.perf.script"
grep -q 'memtier_benchmark' "$run/client.perf.script"
"$flamegraph_dir/stackcollapse-perf.pl" "$run/client.perf.script" \
    >"$run/client.collapsed.txt"
"$flamegraph_dir/flamegraph.pl" \
    --title "vemb-v16 cross-node cli $run_label" \
    --subtitle "process user+kernel; client_cpu=${cpu_mask}; event=${event}@${freq}Hz" \
    "$run/client.collapsed.txt" >"$run/client.svg"
grep -q 'vemb-v16 cross-node cli' "$run/client.svg"
{
    printf 'host=%s\n' "$(hostname)"
    printf 'role=client\nmode=process user+kernel\nevent=%s\nfrequency_hz=%s\n' "$event" "$freq"
    printf 'workload=keys=%s pattern=%s threads=%s clients=%s pipeline=%s batch=%s max_delay_us=%s l1_entries=%s\n' \
        "$keys" "$pattern" "$threads" "$clients" "$pipeline" "$batch_size" "$max_delay_us" "$l1_entries"
    printf 'run_label=%s\n' "$run_label"
    cat "$root/.vemb_v16_build_stamp.client"
} >"$run/client.meta.txt"
tar -C "$run" -czf "$run/client_artifacts.tar.gz" \
    client.preflight.pidstat.txt client.preflight.cpu-blockers.tsv client.preflight.rss-blockers.tsv client.preflight.cpuset.tsv \
    prefill.log client.workload.log client.l1_summary.tsv client.perf.data client.perf.script \
    client.collapsed.txt client.svg client.meta.txt
REMOTE_CLIENT_PERF

status "rendering and validating all-TID server flamegraph"
server_ssh bash -s -- \
    "$SERVER_ROOT" "$REMOTE_RUN_DIR" "$FLAMEGRAPH_DIR" "$SERVER_FLAME_DURATION" \
    "$FREQ" "$EVENT" "$NUM_KEYS" "$KEY_PATTERN" "$THREADS" "$CLIENTS" \
    "$PIPELINE" "$BATCH_REQUEST_SIZE" "$BATCH_MAX_DELAY_US" \
    "$PROXY_REQUEST_BATCH" "$PROXY_RESPONSE_BATCH" "$PROXY_QUEUE_BATCH" \
    "$REQUIRE_VEMB_THREAD_SAMPLES" "$SERVER_CPU_MASK" "$run_label" <<'REMOTE_SERVER_RENDER'
set -euo pipefail

root=$1
run=$2
flamegraph_dir=$3
duration=$4
freq=$5
event=$6
keys=$7
pattern=$8
threads=$9
clients=${10}
pipeline=${11}
batch_size=${12}
max_delay_us=${13}
proxy_request_batch=${14}
proxy_response_batch=${15}
proxy_queue_batch=${16}
require_vemb=${17}
cpu_mask=${18}
run_label=${19}

mpstat_pid="$(cat "$run/server.cpu.mpstat.pid")"
for _ in $(seq 1 100); do
    kill -0 "$mpstat_pid" 2>/dev/null || break
    sleep 0.1
done
! kill -0 "$mpstat_pid" 2>/dev/null || {
    echo "server CPU sampler is still running" >&2
    exit 1
}
process_sampler_pid="$(cat "$run/server.cpu.process.sampler.pid")"
for _ in $(seq 1 100); do
    kill -0 "$process_sampler_pid" 2>/dev/null || break
    sleep 0.1
done
! kill -0 "$process_sampler_pid" 2>/dev/null || {
    echo "server process CPU sampler is still running" >&2
    exit 1
}
test -s "$run/server.cpu.process.tsv"
awk '
BEGIN {
    print "metric\tavg_pct\tcore_equiv\tcpu_count"
}
$1 == "Average:" && $2 ~ /^[0-9]+$/ {
    count++
    usr += $3
    nice += $4
    sys += $5
    iowait += $6
    irq += $7
    soft += $8
    steal += $9
    guest += $10
    gnice += $11
    idle += $12
}
END {
    if (count == 0) exit 1
    printf "usr\t%.6f\t%.6f\t%d\n", usr / count, usr / 100, count
    printf "nice\t%.6f\t%.6f\t%d\n", nice / count, nice / 100, count
    printf "sys\t%.6f\t%.6f\t%d\n", sys / count, sys / 100, count
    printf "iowait\t%.6f\t%.6f\t%d\n", iowait / count, iowait / 100, count
    printf "irq\t%.6f\t%.6f\t%d\n", irq / count, irq / 100, count
    printf "soft\t%.6f\t%.6f\t%d\n", soft / count, soft / 100, count
    printf "steal\t%.6f\t%.6f\t%d\n", steal / count, steal / 100, count
    printf "guest\t%.6f\t%.6f\t%d\n", guest / count, guest / 100, count
    printf "gnice\t%.6f\t%.6f\t%d\n", gnice / count, gnice / 100, count
    printf "total\t%.6f\t%.6f\t%d\n", 100 - idle / count, count - idle / 100, count
}' "$run/server.cpu.cpuset.mpstat.txt" >"$run/server.cpu.cpuset.summary.tsv"
cpu_count="$(awk -F '\t' 'NR == 2 { print $4 }' "$run/server.cpu.cpuset.summary.tsv")"
process_window_s="$(awk -F '\t' '$1 == "window" { print $2 }' "$run/server.cpu.process.tsv")"
{
    printf 'Server CPU Summary\n'
    printf 'CPU set: %s (%s CPUs)\n\n' "$cpu_mask" "$cpu_count"
    printf 'Redis process CPU (%ss window)\n' "$process_window_s"
    printf '%-8s %-36s %12s %12s\n' 'Metric' 'Description' 'Seconds' 'Core equiv'
    printf '%-8s %-36s %12s %12s\n' '--------' '------------------------------------' '------------' '------------'
    awk -F '\t' '
    function description(metric) {
        if (metric == "user") return "Redis user-mode CPU"
        if (metric == "system") return "Redis kernel-mode CPU"
        return "Redis user + system"
    }
    NR > 1 && $1 != "window" {
        printf "%-8s %-36s %12.3f %12.3f\n", $1, description($1), $2, $3
    }' "$run/server.cpu.process.tsv"
    printf '\nCPU-set usage\n'
    printf '%-10s %-34s %12s %12s\n' 'Metric' 'Description' 'Avg %' 'Core equiv'
    printf '%-10s %-34s %12s %12s\n' '----------' '----------------------------------' '------------' '------------'
    awk -F '\t' '
    function description(metric) {
        if (metric == "usr") return "User-mode execution"
        if (metric == "nice") return "Niced user-mode execution"
        if (metric == "sys") return "Kernel-mode execution"
        if (metric == "iowait") return "Waiting for I/O"
        if (metric == "irq") return "Hardware interrupt"
        if (metric == "soft") return "Software interrupt (si)"
        if (metric == "steal") return "Hypervisor steal time"
        if (metric == "guest") return "Guest execution"
        if (metric == "gnice") return "Niced guest execution"
        return "All non-idle time"
    }
    NR > 1 {
        printf "%-10s %-34s %12.3f %12.3f\n", $1, description($1), $2, $3
    }' "$run/server.cpu.cpuset.summary.tsv"
} >"$run/server.cpu.summary.txt"

perf_pid="$(cat "$run/server.perf.pid")"
for _ in $(seq 1 100); do
    kill -0 "$perf_pid" 2>/dev/null || break
    sleep 0.1
done
! kill -0 "$perf_pid" 2>/dev/null || {
    echo "server perf is still running" >&2
    exit 1
}

[ -x "$flamegraph_dir/stackcollapse-perf.pl" ] || exit 1
[ -x "$flamegraph_dir/flamegraph.pl" ] || exit 1
perf script -i "$run/server.perf.data" >"$run/server.perf.script"
grep -q '\[kernel.kallsyms\]' "$run/server.perf.script"
grep -q 'redis-server' "$run/server.perf.script"
if [ "$require_vemb" = 1 ]; then
    grep -q '^vemb-sn-' "$run/server.perf.script"
    grep -q 'supernode_pool_thread_main' "$run/server.perf.script"
    grep -q 'proxy_io_pool_thread_main' "$run/server.perf.script"
fi
"$flamegraph_dir/stackcollapse-perf.pl" "$run/server.perf.script" \
    >"$run/server.collapsed.txt"
awk '{ sub(/^[^;]+;/, "all;redis-server;"); print }' \
    "$run/server.collapsed.txt" >"$run/server.process.collapsed.txt"
"$flamegraph_dir/flamegraph.pl" \
    --title "vemb-v16 cross-node server $run_label" \
    --subtitle "all server TIDs user+kernel; server_cpu=${cpu_mask}; event=${event}@${freq}Hz" \
    "$run/server.process.collapsed.txt" >"$run/server.svg"
grep -q 'vemb-v16 cross-node server' "$run/server.svg"
if [ "$require_vemb" = 1 ]; then
    grep -q 'supernode_pool_thread_main' "$run/server.svg"
    grep -q 'proxy_io_pool_thread_main' "$run/server.svg"
fi
! grep -q 'vemb_v16 handle miss' "$run/server.log"
{
    printf 'host=%s\n' "$(hostname)"
    printf 'role=server\nmode=all redis-server TIDs user+kernel\nevent=%s\nfrequency_hz=%s\nduration_s=%s\n' \
        "$event" "$freq" "$duration"
    printf 'workload=keys=%s pattern=%s threads=%s clients=%s pipeline=%s batch=%s max_delay_us=%s\n' \
        "$keys" "$pattern" "$threads" "$clients" "$pipeline" "$batch_size" "$max_delay_us"
    printf 'server_internal_batches=request=%s response=%s queue=%s supernode=%s\n' \
        "$proxy_request_batch" "$proxy_response_batch" "$proxy_queue_batch" "$proxy_queue_batch"
    printf 'run_label=%s\n' "$run_label"
    printf 'server_cpu_mask=%s\n' "$cpu_mask"
    printf 'server_process_cpu_tsv=server.cpu.process.tsv\n'
    printf 'server_cpuset_cpu_tsv=server.cpu.cpuset.summary.tsv\n'
    printf 'server_cpu_summary=server.cpu.summary.txt\n'
    printf 'vemb_sn_comm_samples=%s\n' "$(grep -c '^vemb-sn-' "$run/server.perf.script" || true)"
    printf 'supernode_entry_samples=%s\n' "$(grep -c 'supernode_pool_thread_main' "$run/server.perf.script" || true)"
    printf 'proxy_entry_samples=%s\n' "$(grep -c 'proxy_io_pool_thread_main' "$run/server.perf.script" || true)"
    cat "$root/.vemb_v16_build_stamp.server"
} >"$run/server.meta.txt"
tar -C "$run" -czf "$run/server_artifacts.tar.gz" \
    server.preflight.pidstat.txt server.preflight.cpu-blockers.tsv server.preflight.rss-blockers.tsv server.preflight.cpuset.tsv \
    server.pid server.log server.threads.txt server.perf.log server.perf.data \
    server.perf.script server.collapsed.txt server.process.collapsed.txt server.svg server.meta.txt \
    server.cpu.process.start server.cpu.process.tsv server.cpu.cpuset.mpstat.txt \
    server.cpu.process.sampler.log server.cpu.cpuset.summary.tsv server.cpu.summary.txt
REMOTE_SERVER_RENDER

status "pulling complete remote artifacts"
scp "${SCP_OPTIONS[@]}" \
    "$SERVER_PEER:$REMOTE_RUN_DIR/server_artifacts.tar.gz" \
    "$LOCAL_ROOT/server/"
scp "${SCP_OPTIONS[@]}" \
    "$CLIENT_PEER:$REMOTE_RUN_DIR/client_artifacts.tar.gz" \
    "$LOCAL_ROOT/client/"
tar -xzf "$LOCAL_ROOT/server/server_artifacts.tar.gz" -C "$LOCAL_ROOT/server"
tar -xzf "$LOCAL_ROOT/client/client_artifacts.tar.gz" -C "$LOCAL_ROOT/client"

test -s "$LOCAL_ROOT/server/server.svg"
test -s "$LOCAL_ROOT/client/client.svg"
test -s "$LOCAL_ROOT/server/server.perf.data"
test -s "$LOCAL_ROOT/client/client.perf.data"
test -s "$LOCAL_ROOT/server/server.cpu.process.tsv"
test -s "$LOCAL_ROOT/server/server.cpu.cpuset.summary.tsv"
test -s "$LOCAL_ROOT/server/server.cpu.summary.txt"

if [[ "$LOCAL_ROOT" == "$ROOT_DIR/"* ]]; then
    local_display="${LOCAL_ROOT#"$ROOT_DIR"/}"
else
    local_display="$LOCAL_ROOT"
fi
status "complete: $local_display"
printf 'server SVG: %s/server/server.svg\n' "$local_display"
printf 'client SVG: %s/client/client.svg\n' "$local_display"
printf 'server raw: %s/server/server.perf.data\n' "$local_display"
printf 'client raw: %s/client/client.perf.data\n' "$local_display"
printf 'server CPU: %s/server/server.cpu.process.tsv\n' "$local_display"
printf 'server CPU set: %s/server/server.cpu.cpuset.summary.tsv\n' "$local_display"
printf '\n'
cat "$LOCAL_ROOT/server/server.cpu.summary.txt"
