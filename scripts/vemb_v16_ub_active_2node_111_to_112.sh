#!/usr/bin/env bash
# Measure a two-owner UB/Aeron cluster that starts with active={0,1}.
# This is intentionally separate from the scaleout runner: no migration or
# topology transition is performed during the workload.
set -euo pipefail

NODE0_HOST="${NODE0_HOST:-192.168.90.111}"
NODE1_HOST="${NODE1_HOST:-192.168.90.112}"
SSH_HOST="${SSH_HOST:-43.154.145.18}"
NODE0_SSH_PORT="${NODE0_SSH_PORT:-8111}"
NODE1_SSH_PORT="${NODE1_SSH_PORT:-8112}"
SSH_USER="${SSH_USER:-root}"
REMOTE_DIR="${REMOTE_DIR:-/root/szz/codespace/hpc-redis}"
MEMTIER="${MEMTIER:-$REMOTE_DIR/memtier_benchmark/memtier_benchmark}"
SERVER_PORT="${SERVER_PORT:-6397}"
DIM="${DIM:-300}"
MAX_VECTORS="${MAX_VECTORS:-65536}"
PREFILL_KEYS="${PREFILL_KEYS:-10000}"
STEADY_KEYS="${STEADY_KEYS:-10000}"
PREFILL_KEY_MIN="${PREFILL_KEY_MIN:-1}"
STEADY_KEY_MIN="${STEADY_KEY_MIN:-}"
TEST_TIME="${TEST_TIME:-30}"
SETTLE_SEC="${SETTLE_SEC:-5}"
CLOSE_WAIT_SEC="${CLOSE_WAIT_SEC:-30}"
SCENARIO="${SCENARIO:-nc_cc}"
CLIENT_MANIFEST_SOURCE="${CLIENT_MANIFEST_SOURCE:-}"
NODE1_AERON_RESPONSE_PATH="${NODE1_AERON_RESPONSE_PATH:-}"
NODE1_AERON_REQUEST_PATH="${NODE1_AERON_REQUEST_PATH:-}"
STEADY_REREAD="${STEADY_REREAD:-}"
ALTERNATE_READS="${ALTERNATE_READS:-0}"
PIPELINE="${PIPELINE:-32}"
BATCH_MAX_DELAY_US="${BATCH_MAX_DELAY_US:-10}"
CLI_STATS_INTERVAL_MS="${CLI_STATS_INTERVAL_MS:-1000}"
SLOT_SCHED="${SLOT_SCHED:-${VEMB_V16_SLOT_SCHED:-fixed}}"
MEMTIER_T="${MEMTIER_T:-64}"
MEMTIER_C="${MEMTIER_C:-4}"
PIO="${PIO:-7}"
SNW="${SNW:-7}"
VNODE_COUNT="${VNODE_COUNT:-100}"
EPOCH="${EPOCH:-$(date +%s)}"
RUN_ID="${RUN_ID:-ub_active_2node_$(date +%Y%m%d_%H%M%S)}"

die() {
    printf 'error: %s\n' "$*" >&2
    exit 2
}

usage() {
    cat <<'EOF'
usage: vemb_v16_ub_active_2node_111_to_112.sh [options]

Options override the corresponding environment variables for this run:
  --scenario nc_cc|cc_nc       request/response direction (default: nc_cc)
  --peer-view-manifest FILE    remote client manifest to copy to node0
  --node1-request-path PATH    node1 Aeron request UB path
  --node1-response-path PATH   node1 Aeron response UB path
  --prefill-key-min N          first numeric key for the prefill range
  --steady-key-min N           first numeric key for the steady range
  --alternate-reads 0|1        write both ranges before A/B then B/A reads
  --steady-reread 0|1          enable the final reread phase
  -h, --help                   show this message
EOF
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --scenario)
            [ "$#" -ge 2 ] || die "--scenario requires a value"
            SCENARIO="$2"
            shift 2
            ;;
        --scenario=*)
            SCENARIO="${1#*=}"
            shift
            ;;
        --peer-view-manifest)
            [ "$#" -ge 2 ] || die "--peer-view-manifest requires a value"
            CLIENT_MANIFEST_SOURCE="$2"
            shift 2
            ;;
        --peer-view-manifest=*)
            CLIENT_MANIFEST_SOURCE="${1#*=}"
            shift
            ;;
        --node1-request-path)
            [ "$#" -ge 2 ] || die "--node1-request-path requires a value"
            NODE1_AERON_REQUEST_PATH="$2"
            shift 2
            ;;
        --node1-request-path=*)
            NODE1_AERON_REQUEST_PATH="${1#*=}"
            shift
            ;;
        --node1-response-path)
            [ "$#" -ge 2 ] || die "--node1-response-path requires a value"
            NODE1_AERON_RESPONSE_PATH="$2"
            shift 2
            ;;
        --node1-response-path=*)
            NODE1_AERON_RESPONSE_PATH="${1#*=}"
            shift
            ;;
        --prefill-key-min)
            [ "$#" -ge 2 ] || die "--prefill-key-min requires a value"
            PREFILL_KEY_MIN="$2"
            shift 2
            ;;
        --prefill-key-min=*)
            PREFILL_KEY_MIN="${1#*=}"
            shift
            ;;
        --steady-key-min)
            [ "$#" -ge 2 ] || die "--steady-key-min requires a value"
            STEADY_KEY_MIN="$2"
            shift 2
            ;;
        --steady-key-min=*)
            STEADY_KEY_MIN="${1#*=}"
            shift
            ;;
        --alternate-reads)
            [ "$#" -ge 2 ] || die "--alternate-reads requires 0 or 1"
            ALTERNATE_READS="$2"
            shift 2
            ;;
        --alternate-reads=*)
            ALTERNATE_READS="${1#*=}"
            shift
            ;;
        --steady-reread)
            [ "$#" -ge 2 ] || die "--steady-reread requires 0 or 1"
            STEADY_REREAD="$2"
            shift 2
            ;;
        --steady-reread=*)
            STEADY_REREAD="${1#*=}"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            die "unknown option: $1"
            ;;
    esac
done

STEADY_KEY_MIN="${STEADY_KEY_MIN:-$((PREFILL_KEY_MIN + PREFILL_KEYS))}"

case "$SCENARIO" in
    nc_cc|scenario1)
        SCENARIO_LABEL="scenario1_nc_cc"
        DEFAULT_CLIENT_MANIFEST_SOURCE="$REMOTE_DIR/examples/vemb_v16_ub_peer_view_111_to_112_active_nc_cc.yaml"
        DEFAULT_NODE1_AERON_REQUEST_PATH="/dev/obmm_shmdev10"
        DEFAULT_NODE1_AERON_RESPONSE_PATH="/dev/obmm_shmdev15"
        DEFAULT_STEADY_REREAD=1
        ;;
    cc_nc|scenario2)
        SCENARIO_LABEL="scenario2_cc_nc_diagnostic"
        DEFAULT_CLIENT_MANIFEST_SOURCE="$REMOTE_DIR/examples/vemb_v16_ub_peer_view_111_to_112_active.yaml"
        DEFAULT_NODE1_AERON_REQUEST_PATH="/dev/obmm_shmdev10"
        DEFAULT_NODE1_AERON_RESPONSE_PATH="/dev/obmm_shmdev11"
        DEFAULT_STEADY_REREAD=0
        ;;
    *)
        die "unsupported SCENARIO='$SCENARIO'; use nc_cc or cc_nc"
        ;;
esac
CLIENT_MANIFEST_SOURCE="${CLIENT_MANIFEST_SOURCE:-$DEFAULT_CLIENT_MANIFEST_SOURCE}"
NODE1_AERON_REQUEST_PATH="${NODE1_AERON_REQUEST_PATH:-$DEFAULT_NODE1_AERON_REQUEST_PATH}"
NODE1_AERON_RESPONSE_PATH="${NODE1_AERON_RESPONSE_PATH:-$DEFAULT_NODE1_AERON_RESPONSE_PATH}"
STEADY_REREAD="${STEADY_REREAD:-$DEFAULT_STEADY_REREAD}"
case "$STEADY_REREAD" in
    0|1) ;;
    *) die "STEADY_REREAD must be 0 or 1" ;;
esac
case "$ALTERNATE_READS" in
    0|1) ;;
    *) die "ALTERNATE_READS must be 0 or 1" ;;
esac
[ -n "$CLIENT_MANIFEST_SOURCE" ] || die "client peer-view manifest path is empty"
[ -n "$NODE1_AERON_REQUEST_PATH" ] || die "node1 request UB path is empty"
[ -n "$NODE1_AERON_RESPONSE_PATH" ] || die "node1 response UB path is empty"
case "$PREFILL_KEY_MIN:$STEADY_KEY_MIN" in
    *[!0-9:]*|:*) die "key range starts must be non-negative integers" ;;
esac
[ "$PREFILL_KEY_MIN" -ge 0 ] || die "PREFILL_KEY_MIN must be non-negative"
[ "$STEADY_KEY_MIN" -ge 0 ] || die "STEADY_KEY_MIN must be non-negative"
case "$SLOT_SCHED" in
    fixed|round_robin) ;;
    *) die "SLOT_SCHED must be fixed or round_robin" ;;
esac

LOCAL_RESULT_DIR="${LOCAL_RESULT_DIR:-$PWD/benchmark/results/scaleout/$RUN_ID}"
NODE0_RESULT_DIR="$REMOTE_DIR/benchmark/results/scaleout/$RUN_ID"
NODE1_RESULT_DIR="$REMOTE_DIR/benchmark/results/scaleout/$RUN_ID"
NODE0_PID_FILE="$NODE0_RESULT_DIR/server_node0.pid"
NODE1_PID_FILE="$NODE1_RESULT_DIR/server_node1.pid"
NODE0_LOG="$NODE0_RESULT_DIR/server_node0.log"
NODE1_LOG="$NODE1_RESULT_DIR/server_node1.log"
CLIENT_MANIFEST="$NODE0_RESULT_DIR/ub_peer_view_111_to_112.yaml"

# Optional phase-scoped client profiling.  Comma-separated labels may include
# old_keys_read, old_keys_after_steady_read, steady_keys_read, and
# steady_keys_reread.  Each selected phase starts a fresh memtier process under
# perf.  The profile covers that process from startup through its normal client
# shutdown; inter-phase settle/snapshot/close-wait work is outside the profile.
PROFILE_PHASES="${PROFILE_PHASES:-}"
PROFILE_FREQ="${PROFILE_FREQ:-99}"
PROFILE_EVENT="${PROFILE_EVENT:-cycles}"
PROFILE_FLAMEGRAPH_DIR="${PROFILE_FLAMEGRAPH_DIR:-/root/FlameGraph}"
PERF_STAT_PHASES="${PERF_STAT_PHASES:-}"
PERF_STAT_EVENTS="${PERF_STAT_EVENTS:-cycles,cache-misses,LLC-load-misses}"
DISABLE_STATS_SAMPLER="${DISABLE_STATS_SAMPLER:-}"
if [ -z "$DISABLE_STATS_SAMPLER" ]; then
    if [ -n "$PROFILE_PHASES" ] || [ -n "$PERF_STAT_PHASES" ]; then
        DISABLE_STATS_SAMPLER=1
    else
        DISABLE_STATS_SAMPLER=0
    fi
fi
PROFILE_LOCAL_DIR="$LOCAL_RESULT_DIR/profiles"

strip_ssh_banner() {
    sed '/^Authorized users only\. All activities may be monitored and reported\.$/d'
}

ssh0() {
    ssh -q -o LogLevel=ERROR -p "$NODE0_SSH_PORT" \
        "$SSH_USER@$SSH_HOST" "$@" 2>&1 | strip_ssh_banner
}

ssh1() {
    ssh -q -o LogLevel=ERROR -p "$NODE1_SSH_PORT" \
        "$SSH_USER@$SSH_HOST" "$@" 2>&1 | strip_ssh_banner
}

scp0() {
    scp -q -P "$NODE0_SSH_PORT" "$SSH_USER@$SSH_HOST:$1" "$2" 2>&1 |
        strip_ssh_banner
}

profile_phase_enabled() {
    local label="$1"
    [ -n "$PROFILE_PHASES" ] || return 1
    case ",$PROFILE_PHASES," in
        *,all,*|*,"$label",*) return 0 ;;
        *) return 1 ;;
    esac
}

perf_stat_phase_enabled() {
    local label="$1"
    [ -n "$PERF_STAT_PHASES" ] || return 1
    case ",$PERF_STAT_PHASES," in
        *,all,*|*,"$label",*) return 0 ;;
        *) return 1 ;;
    esac
}

cleanup() {
    local status=$?
    ssh0 "if test -s '$NODE0_PID_FILE'; then pid=\$(cat '$NODE0_PID_FILE'); kill \$pid 2>/dev/null || true; fi" || true
    ssh1 "if test -s '$NODE1_PID_FILE'; then pid=\$(cat '$NODE1_PID_FILE'); kill \$pid 2>/dev/null || true; fi" || true
    printf 'run_id=%s status=%s artifacts=%s\n' "$RUN_ID" "$status" "$LOCAL_RESULT_DIR" >&2
}
trap cleanup EXIT

mkdir -p "$LOCAL_RESULT_DIR"
cat >"$LOCAL_RESULT_DIR/run.conf" <<EOF
run_id=$RUN_ID
topology=active={0,1}-from-start
node0=$NODE0_HOST
node1=$NODE1_HOST
scenario=$SCENARIO_LABEL
client_manifest_source=$CLIENT_MANIFEST_SOURCE
node1_aeron_request_path=$NODE1_AERON_REQUEST_PATH
node1_aeron_response_path=$NODE1_AERON_RESPONSE_PATH
dim=$DIM
prefill_keys=$PREFILL_KEYS
steady_keys=$STEADY_KEYS
prefill_key_min=$PREFILL_KEY_MIN
prefill_key_max=$((PREFILL_KEY_MIN + PREFILL_KEYS - 1))
steady_key_min=$STEADY_KEY_MIN
steady_key_max=$((STEADY_KEY_MIN + STEADY_KEYS - 1))
threads=$MEMTIER_T
clients=$MEMTIER_C
pipeline=$PIPELINE
batch_max_delay_us=$BATCH_MAX_DELAY_US
cli_stats_interval_ms=$CLI_STATS_INTERVAL_MS
pio=$PIO
snw=$SNW
settle_sec=$SETTLE_SEC
steady_reread=$STEADY_REREAD
alternate_reads=$ALTERNATE_READS
close_wait_sec=$CLOSE_WAIT_SEC
profile_phases=$PROFILE_PHASES
profile_freq=$PROFILE_FREQ
profile_event=$PROFILE_EVENT
perf_stat_phases=$PERF_STAT_PHASES
perf_stat_events=$PERF_STAT_EVENTS
disable_stats_sampler=$DISABLE_STATS_SAMPLER
EOF
printf 'phase\tops_sec\tp50_ms\tp99_ms\tcorrectness\n' >"$LOCAL_RESULT_DIR/summary.tsv"

if [ -n "$PROFILE_PHASES" ]; then
    mkdir -p "$PROFILE_LOCAL_DIR"
fi

ssh0 "mkdir -p '$NODE0_RESULT_DIR' && cp '$REMOTE_DIR/examples/vemb_v16_ub_cluster_111_to_112_node0.yaml' '$NODE0_RESULT_DIR/node0_server.yaml' && cp '$REMOTE_DIR/examples/vemb_v16_ub_cluster_111_to_112_node0_peer_map.yaml' '$NODE0_RESULT_DIR/node0_peer_map.yaml'"
ssh1 "mkdir -p '$NODE1_RESULT_DIR' && cp '$REMOTE_DIR/examples/vemb_v16_ub_cluster_111_to_112_node1.yaml' '$NODE1_RESULT_DIR/node1_server.yaml'"
ssh0 "cp '$CLIENT_MANIFEST_SOURCE' '$CLIENT_MANIFEST'"

NODE0_SERVER_COMMAND="cd '$REMOTE_DIR' && rm -f '$NODE0_PID_FILE' '$NODE0_LOG' && numactl --membind=0 taskset -c 0-95 ./src/redis-server --port '$SERVER_PORT' --bind '$NODE0_HOST' --protected-mode no --vemb-v16-enabled yes --vemb-v16-dim '$DIM' --vemb-v16-max-vectors '$MAX_VECTORS' --vemb-v16-warm-regions-manifest '$NODE0_RESULT_DIR/node0_server.yaml' --vemb-v16-reset-warm-regions yes --vemb-v16-transport aeron --vemb-v16-aeron-ub-path /dev/obmm_shmdev1 --vemb-v16-aeron-response-ub-path /dev/obmm_shmdev2 --vemb-v16-proxy-io-threads '$PIO' --vemb-v16-supernode-workers '$SNW' --vemb-v16-batch-request-size '$PIPELINE' --daemonize yes --pidfile '$NODE0_PID_FILE' --logfile '$NODE0_LOG' --loglevel notice"
NODE1_SERVER_COMMAND="cd '$REMOTE_DIR' && rm -f '$NODE1_PID_FILE' '$NODE1_LOG' && numactl --membind=0 taskset -c 0-95 ./src/redis-server --port '$SERVER_PORT' --bind '$NODE1_HOST' --protected-mode no --vemb-v16-enabled yes --vemb-v16-dim '$DIM' --vemb-v16-max-vectors '$MAX_VECTORS' --vemb-v16-warm-regions-manifest '$NODE1_RESULT_DIR/node1_server.yaml' --vemb-v16-reset-warm-regions yes --vemb-v16-transport aeron --vemb-v16-aeron-ub-path '$NODE1_AERON_REQUEST_PATH' --vemb-v16-aeron-response-ub-path '$NODE1_AERON_RESPONSE_PATH' --vemb-v16-proxy-io-threads '$PIO' --vemb-v16-supernode-workers '$SNW' --vemb-v16-batch-request-size '$PIPELINE' --daemonize yes --pidfile '$NODE1_PID_FILE' --logfile '$NODE1_LOG' --loglevel notice"
printf '[server=node0] command: %s\n' "$NODE0_SERVER_COMMAND"
printf '[server=node1] command: %s\n' "$NODE1_SERVER_COMMAND"
ssh0 "$NODE0_SERVER_COMMAND"
ssh1 "$NODE1_SERVER_COMMAND"

wait_ready() {
    local node="$1" port="$2" ssh_port
    if [ "$node" = "$NODE0_HOST" ]; then ssh_port="$NODE0_SSH_PORT"; else ssh_port="$NODE1_SSH_PORT"; fi
    for _ in $(seq 1 100); do
        if ssh -q -o LogLevel=ERROR -p "$ssh_port" \
            "$SSH_USER@$SSH_HOST" "cd '$REMOTE_DIR' && ./benchmark/vemb_v16_topology_ctl --get --ctl-endpoint tcp --host '$node' --port '$port' --timeout-ms 1000 >/dev/null 2>&1" 2>&1 |
            strip_ssh_banner >/dev/null; then return 0; fi
        sleep 1
    done
    return 1
}
wait_ready "$NODE0_HOST" "$SERVER_PORT"
wait_ready "$NODE1_HOST" "$SERVER_PORT"

ssh1 "cd '$REMOTE_DIR' && ./benchmark/vemb_v16_topology_ctl --set --ctl-endpoint tcp --data-endpoint aeron --host '$NODE1_HOST' --port '$SERVER_PORT' --epoch '$EPOCH' --min-write-epoch '$EPOCH' --vnode-count '$VNODE_COUNT' --active 0,1 --standby 0,1 --owner-endpoints '0=$NODE0_HOST:$SERVER_PORT,1=$NODE1_HOST:$SERVER_PORT' --timeout-ms 5000 >'$NODE1_RESULT_DIR/topology_initial.out'"
ssh0 "cd '$REMOTE_DIR' && ./benchmark/vemb_v16_topology_ctl --set-with-peer-view-map '$NODE0_RESULT_DIR/node0_peer_map.yaml' --ctl-endpoint tcp --data-endpoint aeron --host '$NODE0_HOST' --port '$SERVER_PORT' --epoch '$EPOCH' --min-write-epoch '$EPOCH' --vnode-count '$VNODE_COUNT' --active 0,1 --standby 0,1 --owner-endpoints '0=$NODE0_HOST:$SERVER_PORT,1=$NODE1_HOST:$SERVER_PORT' --timeout-ms 5000 >'$NODE0_RESULT_DIR/topology_initial.out'"

MEMTIER_ARGS="--protocol=vemb_v16 --vemb-v16-dim=$DIM --vemb-v16-handle --vemb-v16-transport=aeron --vemb-v16-ub-peer-view-manifest=$CLIENT_MANIFEST --vemb-v16-ub-peer-view-client-host=111 --vemb-v16-ub-peer-view-owner-id=1 --vemb-v16-batch-request-size=$PIPELINE --vemb-v16-batch-max-delay-us=$BATCH_MAX_DELAY_US"
snapshot_stats() {
    local label="$1"
    local attempt snapshot_ok
    snapshot_ok=0
    for attempt in 1 2 3 4 5; do
        if ssh0 "cd '$REMOTE_DIR' && ./benchmark/vemb_v16_topology_ctl --stats --host '$NODE0_HOST' --port '$SERVER_PORT' --timeout-ms 5000 >'$NODE0_RESULT_DIR/stats_${label}.out' && ./benchmark/vemb_v16_topology_ctl --diagnostic-stats --host '$NODE0_HOST' --port '$SERVER_PORT' --timeout-ms 5000 >'$NODE0_RESULT_DIR/diagnostic_${label}.out'"; then
            snapshot_ok=1
            break
        fi
        [ "$attempt" -lt 5 ] && sleep 1
    done
    if [ "$snapshot_ok" -eq 0 ]; then
        printf 'warning: node0 diagnostic snapshot failed label=%s\n' "$label" >&2
    fi
    snapshot_ok=0
    for attempt in 1 2 3 4 5; do
        if ssh1 "cd '$REMOTE_DIR' && ./benchmark/vemb_v16_topology_ctl --stats --host '$NODE1_HOST' --port '$SERVER_PORT' --timeout-ms 5000 >'$NODE1_RESULT_DIR/stats_${label}.out' && ./benchmark/vemb_v16_topology_ctl --diagnostic-stats --host '$NODE1_HOST' --port '$SERVER_PORT' --timeout-ms 5000 >'$NODE1_RESULT_DIR/diagnostic_${label}.out'"; then
            snapshot_ok=1
            break
        fi
        [ "$attempt" -lt 5 ] && sleep 1
    done
    if [ "$snapshot_ok" -eq 0 ]; then
        printf 'warning: node1 diagnostic snapshot failed label=%s\n' "$label" >&2
    fi
}
wait_server_channels_closed() {
    local label="$1" deadline=$((SECONDS + CLOSE_WAIT_SEC))
    [ "$CLOSE_WAIT_SEC" -gt 0 ] || return 0
    local node0_diag="$NODE0_RESULT_DIR/diagnostic_${label}_close_wait.out"
    local node1_diag="$NODE1_RESULT_DIR/diagnostic_${label}_close_wait.out"
    while [ "$SECONDS" -lt "$deadline" ]; do
        ssh0 "cd '$REMOTE_DIR' && timeout 7 ./benchmark/vemb_v16_topology_ctl --diagnostic-stats --host '$NODE0_HOST' --port '$SERVER_PORT' --timeout-ms 5000 >'$node0_diag'" || true
        ssh1 "cd '$REMOTE_DIR' && timeout 7 ./benchmark/vemb_v16_topology_ctl --diagnostic-stats --host '$NODE1_HOST' --port '$SERVER_PORT' --timeout-ms 5000 >'$node1_diag'" || true
        local node0_channels node1_channels node0_closing node1_closing
        node0_channels=$(ssh0 "awk -F= '/^channel_count=/{print \$2; exit}' '$node0_diag'" 2>/dev/null | tr -d '[:space:]' || true)
        node1_channels=$(ssh1 "awk -F= '/^channel_count=/{print \$2; exit}' '$node1_diag'" 2>/dev/null | tr -d '[:space:]' || true)
        node0_closing=$(ssh0 "awk -F= '/^closing_channel_count=/{print \$2; exit}' '$node0_diag'" 2>/dev/null | tr -d '[:space:]' || true)
        node1_closing=$(ssh1 "awk -F= '/^closing_channel_count=/{print \$2; exit}' '$node1_diag'" 2>/dev/null | tr -d '[:space:]' || true)
        if [ "${node0_channels:-}" = 0 ] && [ "${node1_channels:-}" = 0 ]; then
            return 0
        fi
        sleep 1
    done
    printf 'channel close wait timed out label=%s node0 channel_count=%s closing=%s node1 channel_count=%s closing=%s\n' \
        "$label" "${node0_channels:-unknown}" "${node0_closing:-unknown}" \
        "${node1_channels:-unknown}" "${node1_closing:-unknown}" >&2
    return 1
}
start_resource_samples() {
    local outfile="$1"
    local sample_seconds=$((TEST_TIME + 5))
    local client_pid_file="$2"
    local client_pid
    client_pid=$(ssh0 "cat '$client_pid_file'")
    ssh0 "cd '$REMOTE_DIR'; server_pid=\$(cat '$NODE0_PID_FILE'); nohup pidstat -u -p \$server_pid 1 '$sample_seconds' >'${outfile}.node0_pidstat' 2>&1 < /dev/null & echo \$! >'${outfile}.node0_pidstat.pid'; nohup pidstat -u -p '$client_pid' 1 '$sample_seconds' >'${outfile}.node0_client_pidstat' 2>&1 < /dev/null & echo \$! >'${outfile}.node0_client_pidstat.pid'; nohup mpstat 1 '$sample_seconds' >'${outfile}.node0_mpstat' 2>&1 < /dev/null & echo \$! >'${outfile}.node0_mpstat.pid'"
    ssh1 "cd '$REMOTE_DIR'; server_pid=\$(cat '$NODE1_PID_FILE'); nohup pidstat -u -p \$server_pid 1 '$sample_seconds' >'${outfile}.node1_pidstat' 2>&1 < /dev/null & echo \$! >'${outfile}.node1_pidstat.pid'; nohup mpstat 1 '$sample_seconds' >'${outfile}.node1_mpstat' 2>&1 < /dev/null & echo \$! >'${outfile}.node1_mpstat.pid'"
    if [ "$DISABLE_STATS_SAMPLER" != 1 ]; then
        ssh0 "cd '$REMOTE_DIR'; nohup bash -c 'for ((sample=0; sample<$sample_seconds; sample++)); do date +sample_epoch_ns=%s%N; ./benchmark/vemb_v16_topology_ctl --stats --host $NODE0_HOST --port $SERVER_PORT --timeout-ms 1000 || exit; sleep 1; done' >'${outfile}.node0_net_stats' 2>&1 < /dev/null & echo \$! >'${outfile}.node0_net_stats.pid'; nohup bash -c 'for ((sample=0; sample<$sample_seconds; sample++)); do date +sample_epoch_ns=%s%N; ./benchmark/vemb_v16_topology_ctl --diagnostic-stats --host $NODE0_HOST --port $SERVER_PORT --timeout-ms 5000 || exit; sleep 1; done' >'${outfile}.node0_net_diagnostic_stats' 2>&1 < /dev/null & echo \$! >'${outfile}.node0_net_diagnostic_stats.pid'"
        ssh1 "cd '$REMOTE_DIR'; nohup bash -c 'for ((sample=0; sample<$sample_seconds; sample++)); do date +sample_epoch_ns=%s%N; ./benchmark/vemb_v16_topology_ctl --stats --host $NODE1_HOST --port $SERVER_PORT --timeout-ms 1000 || exit; sleep 1; done' >'${outfile}.node1_net_stats' 2>&1 < /dev/null & echo \$! >'${outfile}.node1_net_stats.pid'; nohup bash -c 'for ((sample=0; sample<$sample_seconds; sample++)); do date +sample_epoch_ns=%s%N; ./benchmark/vemb_v16_topology_ctl --diagnostic-stats --host $NODE1_HOST --port $SERVER_PORT --timeout-ms 5000 || exit; sleep 1; done' >'${outfile}.node1_net_diagnostic_stats' 2>&1 < /dev/null & echo \$! >'${outfile}.node1_net_diagnostic_stats.pid'"
    fi
}
stop_resource_samples() {
    local outfile="$1"
    ssh0 "for suffix in node0_pidstat node0_client_pidstat node0_mpstat node0_net_stats node0_net_diagnostic_stats; do pidfile='${outfile}.'\$suffix.pid; if test -s \"\$pidfile\"; then kill \$(cat \"\$pidfile\") 2>/dev/null || true; fi; done" || true
    ssh1 "for suffix in node1_pidstat node1_mpstat node1_net_stats node1_net_diagnostic_stats; do pidfile='${outfile}.'\$suffix.pid; if test -s \"\$pidfile\"; then kill \$(cat \"\$pidfile\") 2>/dev/null || true; fi; done" || true
}
run_client() {
    local label="$1" mode="$2" min_key="$3" max_key="$4" outfile="$5" totals parsed ops p50 p99
    local client_pid_file="${outfile}.client.pid"
    local client_status_file="${outfile}.client.status"
    local profile_this_phase=0
    local perf_stat_this_phase=0
    local client_exec="'$MEMTIER'"
    local phase_command
    if [ "$mode" = read ] && profile_phase_enabled "$label"; then
        profile_this_phase=1
        client_exec="perf record -F '$PROFILE_FREQ' -g -e '$PROFILE_EVENT' -o '$outfile.perf.data' -- '$MEMTIER'"
        ssh0 "rm -f '$outfile.perf.data' '$outfile.perf.script' '$outfile.collapsed' '$outfile.svg'"
        ssh0 "test -x '$PROFILE_FLAMEGRAPH_DIR/stackcollapse-perf.pl' && test -x '$PROFILE_FLAMEGRAPH_DIR/flamegraph.pl'" || \
            die "FlameGraph scripts not found on node0: $PROFILE_FLAMEGRAPH_DIR"
        printf 'profiling phase=%s event=%s freq=%s stats_sampler=%s\n' \
            "$label" "$PROFILE_EVENT" "$PROFILE_FREQ" "$DISABLE_STATS_SAMPLER"
    elif [ "$mode" = read ] && perf_stat_phase_enabled "$label"; then
        perf_stat_this_phase=1
        client_exec="perf stat -x, -e '$PERF_STAT_EVENTS' -o '$outfile.perf.stat' -- '$MEMTIER'"
        ssh0 "rm -f '$outfile.perf.stat'"
        printf 'perf stat phase=%s events=%s stats_sampler=%s\n' \
            "$label" "$PERF_STAT_EVENTS" "$DISABLE_STATS_SAMPLER"
    fi
    if [ "$mode" = write ]; then
        phase_command="cd '$REMOTE_DIR' && VEMB_V16_SLOT_SCHED='$SLOT_SCHED' VEMB_V16_CLI_STATS_INTERVAL_MS='$CLI_STATS_INTERVAL_MS' numactl --membind=1 taskset -c 96-191 '$MEMTIER' $MEMTIER_ARGS --vemb-v16-endpoints='$NODE0_HOST:$SERVER_PORT' --threads=1 --clients=1 --pipeline=32 --requests='$((max_key - min_key + 1))' --ratio=1:0 --key-pattern=S:S --key-prefix=item: --key-minimum='$min_key' --key-maximum='$max_key' >'$outfile' 2>&1"
        printf '[phase=%s] command: %s\n' "$label" "$phase_command"
        ssh0 "$phase_command"
    else
        phase_command="cd '$REMOTE_DIR'; rm -f '$client_pid_file' '$client_status_file'; (VEMB_V16_SLOT_SCHED='$SLOT_SCHED' VEMB_V16_CLI_STATS_INTERVAL_MS='$CLI_STATS_INTERVAL_MS' numactl --membind=1 taskset -c 96-191 $client_exec $MEMTIER_ARGS --vemb-v16-endpoints='$NODE0_HOST:$SERVER_PORT' --threads='$MEMTIER_T' --clients='$MEMTIER_C' --pipeline='$PIPELINE' --ratio=0:1 --key-pattern=R:R --key-prefix=item: --key-minimum='$min_key' --key-maximum='$max_key' --test-time='$TEST_TIME' >'$outfile' 2>&1 & client_pid=\$!; printf '%s\\n' \$client_pid >'$client_pid_file'; wait \$client_pid; rc=\$?; printf '%s\\n' \$rc >'$client_status_file') </dev/null >/dev/null 2>&1 & bg_pid=\$!; : \$bg_pid"
        printf '[phase=%s] command: %s\n' "$label" "$phase_command"
        ssh0 "$phase_command"
        for _ in $(seq 1 30); do
            if ssh0 "test -s '$client_pid_file'"; then break; fi
            sleep 1
        done
        start_resource_samples "$outfile" "$client_pid_file"
        while ! ssh0 "test -s '$client_status_file'"; do sleep 1; done
        stop_resource_samples "$outfile"
    fi
    wait_server_channels_closed "$label"
    if [ "$profile_this_phase" = 1 ]; then
        ssh0 "cd '$REMOTE_DIR' && perf script -i '$outfile.perf.data' >'$outfile.perf.script' && '$PROFILE_FLAMEGRAPH_DIR/stackcollapse-perf.pl' '$outfile.perf.script' >'$outfile.collapsed' && '$PROFILE_FLAMEGRAPH_DIR/flamegraph.pl' --title 'vemb-v16 active 2node cli $label' --subtitle 'phase=$label; event=$PROFILE_EVENT@$PROFILE_FREQ; process user+kernel' '$outfile.collapsed' >'$outfile.svg'"
        scp0 "$outfile.perf.data" "$PROFILE_LOCAL_DIR/$label.perf.data"
        scp0 "$outfile.perf.script" "$PROFILE_LOCAL_DIR/$label.perf.script"
        scp0 "$outfile.collapsed" "$PROFILE_LOCAL_DIR/$label.collapsed"
        scp0 "$outfile.svg" "$PROFILE_LOCAL_DIR/$label.svg"
    fi
    if [ "$perf_stat_this_phase" = 1 ]; then
        scp0 "$outfile.perf.stat" "$LOCAL_RESULT_DIR/$label.perf.stat"
    fi
    ssh0 "grep -q '^Totals' '$outfile' && ! grep -Eq 'status_(nf|err)=[1-9]|materialized_fail=[1-9]|unmatched=[1-9]' '$outfile'"
    totals=$(ssh0 "grep '^Totals' '$outfile' | tail -1")
    parsed=$(awk '{print $2, $(NF-3), $(NF-2)}' <<<"$totals")
    read -r ops p50 p99 <<<"$parsed"
    printf -v ops '%0.f' "$ops"
    printf '%s\t%s\t%s\t%s\tpass\n' "$label" "$ops" "$p50" "$p99" >>"$LOCAL_RESULT_DIR/summary.tsv"
    printf '%s: %s p50=%s p99=%s\n' "$label" "$ops" "$p50" "$p99"
}

snapshot_stats initial
run_client prefill write "$PREFILL_KEY_MIN" "$((PREFILL_KEY_MIN + PREFILL_KEYS - 1))" "$NODE0_RESULT_DIR/prefill.out"
snapshot_stats prefill_end
if [ "$ALTERNATE_READS" = 1 ]; then
    # Both ranges are materialized before reads; compare A/B and then B/A.
    snapshot_stats steady_write_start
    run_client steady_write write "$STEADY_KEY_MIN" "$((STEADY_KEY_MIN + STEADY_KEYS - 1))" "$NODE0_RESULT_DIR/steady_write.out"
    snapshot_stats steady_write_end
    sleep "$SETTLE_SEC"
    snapshot_stats old_keys_read_start
    run_client old_keys_read read "$PREFILL_KEY_MIN" "$((PREFILL_KEY_MIN + PREFILL_KEYS - 1))" "$NODE0_RESULT_DIR/old_keys_read.out"
    snapshot_stats old_keys_read_end
    snapshot_stats steady_keys_read_start
    run_client steady_keys_read read "$STEADY_KEY_MIN" "$((STEADY_KEY_MIN + STEADY_KEYS - 1))" "$NODE0_RESULT_DIR/steady_keys_read.out"
    snapshot_stats steady_keys_read_end
    snapshot_stats old_keys_after_steady_read_start
    run_client old_keys_after_steady_read read "$PREFILL_KEY_MIN" "$((PREFILL_KEY_MIN + PREFILL_KEYS - 1))" "$NODE0_RESULT_DIR/old_keys_after_steady_read.out"
    snapshot_stats old_keys_after_steady_read_end
    snapshot_stats steady_keys_reread_start
    run_client steady_keys_reread read "$STEADY_KEY_MIN" "$((STEADY_KEY_MIN + STEADY_KEYS - 1))" "$NODE0_RESULT_DIR/steady_keys_reread.out"
    snapshot_stats steady_keys_reread_end
else
    sleep "$SETTLE_SEC"
    snapshot_stats old_keys_read_start
    run_client old_keys_read read "$PREFILL_KEY_MIN" "$((PREFILL_KEY_MIN + PREFILL_KEYS - 1))" "$NODE0_RESULT_DIR/old_keys_read.out"
    snapshot_stats old_keys_read_end
    snapshot_stats steady_write_start
    run_client steady_write write "$STEADY_KEY_MIN" "$((STEADY_KEY_MIN + STEADY_KEYS - 1))" "$NODE0_RESULT_DIR/steady_write.out"
    snapshot_stats steady_write_end
    sleep "$SETTLE_SEC"
    snapshot_stats old_keys_after_steady_read_start
    run_client old_keys_after_steady_read read "$PREFILL_KEY_MIN" "$((PREFILL_KEY_MIN + PREFILL_KEYS - 1))" "$NODE0_RESULT_DIR/old_keys_after_steady_read.out"
    snapshot_stats old_keys_after_steady_read_end
    snapshot_stats steady_keys_read_start
    run_client steady_keys_read read "$STEADY_KEY_MIN" "$((STEADY_KEY_MIN + STEADY_KEYS - 1))" "$NODE0_RESULT_DIR/steady_keys_read.out"
    snapshot_stats steady_keys_read_end
    if [ "$STEADY_REREAD" = 1 ]; then
        snapshot_stats steady_keys_reread_start
        run_client steady_keys_reread read "$STEADY_KEY_MIN" "$((STEADY_KEY_MIN + STEADY_KEYS - 1))" "$NODE0_RESULT_DIR/steady_keys_reread.out"
        snapshot_stats steady_keys_reread_end
    fi
fi

ssh0 "cd '$REMOTE_DIR' && ./benchmark/vemb_v16_topology_ctl --get --ctl-endpoint tcp --host '$NODE0_HOST' --port '$SERVER_PORT' --timeout-ms 5000 >'$NODE0_RESULT_DIR/topology_final.out'"
ssh1 "cd '$REMOTE_DIR' && ./benchmark/vemb_v16_topology_ctl --get --ctl-endpoint tcp --host '$NODE1_HOST' --port '$SERVER_PORT' --timeout-ms 5000 >'$NODE1_RESULT_DIR/topology_final.out'"
ssh0 "grep -E 'runtime warm region attached|runtime remote_meta owner view attached|runtime ub rpc peer attached|vemb_v16 handle miss|remote_meta|batch rejected|ATTACH rejected' '$NODE0_LOG' >'$NODE0_RESULT_DIR/node0_data_summary.log' || true"
ssh1 "grep -E 'runtime warm region attached|runtime remote_meta owner view attached|runtime ub rpc peer attached|vemb_v16 handle miss|remote_meta|batch rejected|ATTACH rejected' '$NODE1_LOG' >'$NODE1_RESULT_DIR/node1_data_summary.log' || true"

printf 'two-node active-ring UB test passed. Artifacts remain at %s on both nodes; local summary is %s\n' "$NODE0_RESULT_DIR" "$LOCAL_RESULT_DIR/summary.tsv"
