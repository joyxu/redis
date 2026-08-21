#!/usr/bin/env bash
# Two-owner UB/Aeron scaleout throughput runner.
#
# Execute from the Mac workspace. Management SSH uses the public forwarded
# ports; server control, topology endpoints, and UB peer-view identities use
# the 111/112 private network only.
set -euo pipefail

NODE0_HOST="${NODE0_HOST:-192.168.90.111}"
NODE1_HOST="${NODE1_HOST:-192.168.90.112}"
NODE0_SSH_HOST="${NODE0_SSH_HOST:-43.154.145.18}"
NODE0_SSH_PORT="${NODE0_SSH_PORT:-8111}"
NODE1_SSH_HOST="${NODE1_SSH_HOST:-43.154.145.18}"
NODE1_SSH_PORT="${NODE1_SSH_PORT:-8112}"
SSH_USER="${SSH_USER:-root}"
REMOTE_DIR="${REMOTE_DIR:-/root/szz/codespace/hpc-redis}"
MEMTIER="${MEMTIER:-$REMOTE_DIR/memtier_benchmark/memtier_benchmark}"

SERVER_PORT="${SERVER_PORT:-6397}"
COORD_PORT="${COORD_PORT:-7397}"
DIM="${DIM:-300}"
MAX_VECTORS="${MAX_VECTORS:-65536}"
PREFILL_KEYS="${PREFILL_KEYS:-10000}"
STEADY_KEYS="${STEADY_KEYS:-$PREFILL_KEYS}"
STEADY_KEY_MIN="${STEADY_KEY_MIN:-$((PREFILL_KEYS + 1))}"
STEADY_KEY_MAX="${STEADY_KEY_MAX:-$((PREFILL_KEYS + STEADY_KEYS))}"
TEST_TIME="${TEST_TIME:-30}"
BG_TIME_SCALEOUT="${BG_TIME_SCALEOUT:-60}"
PIPELINE="${PIPELINE:-32}"
BATCH_MAX_DELAY_US="${BATCH_MAX_DELAY_US:-10}"
MEMTIER_T="${MEMTIER_T:-64}"
MEMTIER_C="${MEMTIER_C:-4}"
PIO="${PIO:-7}"
SNW="${SNW:-7}"
CONTROL_TIMEOUT_MS="${CONTROL_TIMEOUT_MS:-5000}"
 # Local migration work is performed synchronously by the combined topology
 # request.  With 100 vnodes and the requested 64x4/pipeline-32 workload it
 # can exceed three minutes before the notify worker reaches local-done.
COMBINED_CONTROL_TIMEOUT_MS="${COMBINED_CONTROL_TIMEOUT_MS:-600000}"
COORD_WAIT_MS="${COORD_WAIT_MS:-600000}"
VNODE_COUNT="${VNODE_COUNT:-100}"
EPOCH_BASE="${EPOCH_BASE:-$(date +%s)}"
INIT_EPOCH="${INIT_EPOCH:-$((EPOCH_BASE + 1))}"
MIGRATION_EPOCH="${MIGRATION_EPOCH:-$((EPOCH_BASE + 101))}"
CUTOVER_EPOCH="${CUTOVER_EPOCH:-$((MIGRATION_EPOCH + 1))}"
KEEP_SERVERS="${KEEP_SERVERS:-0}"
ARCHIVE_LOCAL="${ARCHIVE_LOCAL:-1}"
RUN_ID="${RUN_ID:-ub_scaleout_$(date +%Y%m%d_%H%M%S)}"
 # The checked-in manifests carry the only complete two-node allocation:
 # node0 dev1-4, node1 dev9-12, and their imported peer devices.  Keep them
 # enabled by default; otherwise the fallback values below silently select
 # the retired allocation and scaleout fails during peer-view ATTACH/mmap.
USE_EXTERNAL_UB_CONFIG="${USE_EXTERNAL_UB_CONFIG:-1}"
UB_CONFIG_DIR="${UB_CONFIG_DIR:-$REMOTE_DIR/examples}"

# The server migration plane has independent UB ranges from the client AERON
# request/response/warm mappings. Device aliases may be shared by the fixed
# 111/112 deployment, but their producer/consumer roles and offset ranges may
# not overlap.
SERVER_META_LOCAL_PATH="${SERVER_META_LOCAL_PATH:-/dev/obmm_shmdev3}"
SERVER_META_PEER_PATH="${SERVER_META_PEER_PATH:-/dev/obmm_shmdev7}"
SERVER_REQUEST_LOCAL_PATH="${SERVER_REQUEST_LOCAL_PATH:-/dev/obmm_shmdev4}"
SERVER_REQUEST_PEER_PATH="${SERVER_REQUEST_PEER_PATH:-/dev/obmm_shmdev13}"
SERVER_RESPONSE_LOCAL_PATH="${SERVER_RESPONSE_LOCAL_PATH:-/dev/obmm_shmdev8}"
SERVER_RESPONSE_PEER_PATH="${SERVER_RESPONSE_PEER_PATH:-/dev/obmm_shmdev9}"
SERVER_WARM_LOCAL_PATH="${SERVER_WARM_LOCAL_PATH:-/dev/obmm_shmdev3}"
SERVER_WARM_PEER_PATH="${SERVER_WARM_PEER_PATH:-/dev/obmm_shmdev7}"
SERVER_WARM_REGION_BYTES="${SERVER_WARM_REGION_BYTES:-1073741824}"
REMOTE_META_MMAP_OFFSET="${REMOTE_META_MMAP_OFFSET:-268435456}"
SERVER_RPC_REQUEST_OFFSET="${SERVER_RPC_REQUEST_OFFSET:-134217728}"
SERVER_RPC_RESPONSE_OFFSET="${SERVER_RPC_RESPONSE_OFFSET:-201326592}"
UB_RPC_TIMEOUT_MS="${UB_RPC_TIMEOUT_MS:-2000}"

# Per-owner paths used by the generated fallback manifests.  The external
# YAML files use the same allocation; keeping these explicit prevents the
# fallback path from assigning node1's local warm region to node0.
NODE0_META_LOCAL_PATH="${NODE0_META_LOCAL_PATH:-/dev/obmm_shmdev3}"
NODE1_META_LOCAL_PATH="${NODE1_META_LOCAL_PATH:-/dev/obmm_shmdev12}"
NODE0_WARM_LOCAL_PATH="${NODE0_WARM_LOCAL_PATH:-/dev/obmm_shmdev3}"
NODE1_WARM_LOCAL_PATH="${NODE1_WARM_LOCAL_PATH:-/dev/obmm_shmdev12}"
NODE0_WARM_PEER_PATH="${NODE0_WARM_PEER_PATH:-/dev/obmm_shmdev16}"
NODE1_WARM_PEER_PATH="${NODE1_WARM_PEER_PATH:-/dev/obmm_shmdev7}"
NODE0_RPC_REQUEST_PATH="${NODE0_RPC_REQUEST_PATH:-/dev/obmm_shmdev4}"
NODE0_RPC_RESPONSE_PATH="${NODE0_RPC_RESPONSE_PATH:-/dev/obmm_shmdev13}"
NODE1_RPC_REQUEST_PATH="${NODE1_RPC_REQUEST_PATH:-/dev/obmm_shmdev9}"
NODE1_RPC_RESPONSE_PATH="${NODE1_RPC_RESPONSE_PATH:-/dev/obmm_shmdev8}"

# Owner 0 shares its local UB pair with the workload client on node 111.
# Owner 1 keeps the cross-node provider pair translated by the fixed
# CLI@111 peer-view manifest to imported dev14/dev15/dev16.
NODE0_AERON_REQUEST_PATH="${NODE0_AERON_REQUEST_PATH:-/dev/obmm_shmdev1}"
NODE0_AERON_RESPONSE_PATH="${NODE0_AERON_RESPONSE_PATH:-/dev/obmm_shmdev2}"
NODE1_AERON_REQUEST_PATH="${NODE1_AERON_REQUEST_PATH:-/dev/obmm_shmdev10}"
NODE1_AERON_RESPONSE_PATH="${NODE1_AERON_RESPONSE_PATH:-/dev/obmm_shmdev11}"
CLIENT_WARM_PATH="${CLIENT_WARM_PATH:-/dev/obmm_shmdev3}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
LOCAL_RESULT_DIR="${LOCAL_RESULT_DIR:-$ROOT_DIR/benchmark/results/scaleout/$RUN_ID}"
REMOTE_SUBDIR="benchmark/results/scaleout/$RUN_ID"
REMOTE_RESULT_DIR="$REMOTE_DIR/$REMOTE_SUBDIR"
NODE0_MANIFEST="$REMOTE_RESULT_DIR/node0_server.yaml"
NODE1_MANIFEST="$REMOTE_RESULT_DIR/node1_server.yaml"
NODE0_PEER_MAP="$REMOTE_RESULT_DIR/node0_server_peer_map.yaml"
CLIENT_PEER_MANIFEST="$REMOTE_RESULT_DIR/ub_peer_view_111_to_112.yaml"
NODE0_LOG="$REMOTE_RESULT_DIR/server_node0.log"
NODE1_LOG="$REMOTE_RESULT_DIR/server_node1.log"
COORD_OUT="$REMOTE_RESULT_DIR/coordinator.out"
COORD_ERR="$REMOTE_RESULT_DIR/coordinator.err"
NODE1_CANDIDATE_ERR="$REMOTE_RESULT_DIR/topology_candidate_node1.err"
NODE1_CANDIDATE_STATUS="$REMOTE_RESULT_DIR/topology_candidate_node1.status"
NODE1_CANDIDATE_PID_FILE="$REMOTE_RESULT_DIR/topology_candidate_node1.pid"
NODE1_CANDIDATE_STARTED="$REMOTE_RESULT_DIR/topology_candidate_node1.started"
NODE1_CANDIDATE_FINISHED="$REMOTE_RESULT_DIR/topology_candidate_node1.finished"
NODE1_CANDIDATE_SSH_PID=""
PREFILL_OUT="$REMOTE_RESULT_DIR/prefill.out"
DURING_OUT="$REMOTE_RESULT_DIR/scaleout_during.out"
DURING_PID_FILE="$REMOTE_RESULT_DIR/scaleout_during.pid"
AFTER_OLD_OUT="$REMOTE_RESULT_DIR/scaleout_after_old_keys.out"
AFTER_PREFILL_OUT="$REMOTE_RESULT_DIR/scaleout_after_prefill.out"
AFTER_OUT="$REMOTE_RESULT_DIR/scaleout_after.out"
NODE0_PID_FILE="$REMOTE_RESULT_DIR/server_node0.pid"
NODE1_PID_FILE="$REMOTE_RESULT_DIR/server_node1.pid"
COORD_PID_FILE="$REMOTE_RESULT_DIR/coordinator.pid"

die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

step() {
    printf '\n== %s ==\n' "$*"
}

is_uint() {
    case "$1" in
        ''|*[!0-9]*) return 1 ;;
        *) return 0 ;;
    esac
}

require_uint() {
    is_uint "$2" || die "$1 must be an unsigned integer: $2"
}

ssh_run() {
    local node="$1"
    shift
    local host port command status
    case "$node" in
        "$NODE0_HOST") host="$NODE0_SSH_HOST"; port="$NODE0_SSH_PORT" ;;
        "$NODE1_HOST") host="$NODE1_SSH_HOST"; port="$NODE1_SSH_PORT" ;;
        *) die "unknown cluster node: $node" ;;
    esac
    command="$*"
    runner_log "ssh start node=$node endpoint=${host}:${port} command=$command"
    if ssh -p "$port" "${SSH_USER}@${host}" "$@"; then
        status=0
    else
        status=$?
    fi
    runner_log "ssh finish node=$node endpoint=${host}:${port} rc=$status command=$command"
    return "$status"
}

remote_stop_pid() {
    local node="$1" pid_file="$2"
    ssh_run "$node" "if test -s '$pid_file'; then pid=\$(cat '$pid_file'); if kill -0 \$pid 2>/dev/null; then kill \$pid; fi; fi" || true
}

stop_owned_processes() {
    remote_stop_pid "$NODE0_HOST" "$DURING_PID_FILE"
    remote_stop_pid "$NODE0_HOST" "$COORD_PID_FILE"
    remote_stop_pid "$NODE0_HOST" "$NODE0_PID_FILE"
    remote_stop_pid "$NODE1_HOST" "$NODE1_CANDIDATE_PID_FILE"
    remote_stop_pid "$NODE1_HOST" "$NODE1_PID_FILE"
}

cleanup() {
    local status=$?
    if [ -n "${NODE1_CANDIDATE_SSH_PID:-}" ] &&
       kill -0 "$NODE1_CANDIDATE_SSH_PID" 2>/dev/null; then
        kill "$NODE1_CANDIDATE_SSH_PID" 2>/dev/null || true
    fi
    [ "$KEEP_SERVERS" = "0" ] && stop_owned_processes
    if [ "$status" -ne 0 ]; then
        printf 'P7 failed; remote artifacts remain in %s\n' "$REMOTE_RESULT_DIR" >&2
    fi
}
trap cleanup EXIT

for setting in SERVER_PORT COORD_PORT DIM MAX_VECTORS PREFILL_KEYS STEADY_KEYS \
               TEST_TIME BG_TIME_SCALEOUT PIPELINE BATCH_MAX_DELAY_US MEMTIER_T MEMTIER_C PIO SNW \
               CONTROL_TIMEOUT_MS \
               COMBINED_CONTROL_TIMEOUT_MS COORD_WAIT_MS VNODE_COUNT \
               INIT_EPOCH MIGRATION_EPOCH CUTOVER_EPOCH SERVER_WARM_REGION_BYTES \
               REMOTE_META_MMAP_OFFSET SERVER_RPC_REQUEST_OFFSET \
               SERVER_RPC_RESPONSE_OFFSET UB_RPC_TIMEOUT_MS NODE0_SSH_PORT \
               NODE1_SSH_PORT; do
    require_uint "$setting" "${!setting}"
done
[ "$SERVER_PORT" -gt 0 ] && [ "$SERVER_PORT" -le 65535 ] || die "invalid SERVER_PORT"
[ "$COORD_PORT" -gt 0 ] && [ "$COORD_PORT" -le 65535 ] || die "invalid COORD_PORT"
[ "$DIM" -gt 0 ] || die "DIM must be positive"
[ "$MAX_VECTORS" -ge "$PREFILL_KEYS" ] || die "MAX_VECTORS must cover PREFILL_KEYS"
[ "$CUTOVER_EPOCH" -gt "$MIGRATION_EPOCH" ] || die "CUTOVER_EPOCH must exceed MIGRATION_EPOCH"
case "$KEEP_SERVERS" in 0|1) ;; *) die "KEEP_SERVERS must be 0 or 1" ;; esac
case "$ARCHIVE_LOCAL" in 0|1) ;; *) die "ARCHIVE_LOCAL must be 0 or 1" ;; esac
[ "$NODE0_SSH_HOST" != "$NODE0_HOST" ] || die "NODE0_SSH_HOST must be the Mac-facing endpoint"
[ "$NODE1_SSH_HOST" != "$NODE1_HOST" ] || die "NODE1_SSH_HOST must be the Mac-facing endpoint"

mkdir -p "$LOCAL_RESULT_DIR"
RUNNER_LOG="$LOCAL_RESULT_DIR/runner.log"
: >"$RUNNER_LOG"
runner_log() {
    printf '%s %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')" "$*" >>"$RUNNER_LOG"
}
runner_log "runner start pid=$$ run_id=$RUN_ID node0=$NODE0_HOST node1=$NODE1_HOST"
printf 'run_id=%s\nnode0_ssh=%s:%s\nnode1_ssh=%s:%s\nnode0_data=%s\nnode1_data=%s\ndim=%s\nprefill_keys=%s\nvnode_count=%s\npio=%s\nsnw=%s\nthreads=%s\nclients=%s\npipeline=%s\n' \
    "$RUN_ID" "$NODE0_SSH_HOST" "$NODE0_SSH_PORT" \
    "$NODE1_SSH_HOST" "$NODE1_SSH_PORT" "$NODE0_HOST" "$NODE1_HOST" \
    "$DIM" "$PREFILL_KEYS" "$VNODE_COUNT" "$PIO" "$SNW" \
    "$MEMTIER_T" "$MEMTIER_C" "$PIPELINE" \
    >"$LOCAL_RESULT_DIR/run.conf"

summary_row() {
    printf '%-32s %-15s %-10s %-10s %-8s %s\n' "$@"
}
summary_row phase ops_sec p50_ms p99_ms wall_s note >"$LOCAL_RESULT_DIR/summary.tsv"

step "Prepare result directories and verify UB throughput build stamps"
ssh_run "$NODE0_HOST" "mkdir -p '$REMOTE_RESULT_DIR'"
ssh_run "$NODE1_HOST" "mkdir -p '$REMOTE_RESULT_DIR'"
for node in "$NODE0_HOST" "$NODE1_HOST"; do
    ssh_run "$node" "cd '$REMOTE_DIR' && bash scripts/vemb_v16_build_stamp.sh verify all"
done

step "Refuse busy server or coordinator ports"
for node in "$NODE0_HOST" "$NODE1_HOST"; do
    ssh_run "$node" "! ss -H -ltn | awk '\$4 ~ /:${SERVER_PORT}\$/ { found = 1 } END { exit found ? 0 : 1 }'"
done
ssh_run "$NODE0_HOST" "! ss -H -ltn | awk '\$4 ~ /:${COORD_PORT}\$/ { found = 1 } END { exit found ? 0 : 1 }'"

step "Write server migration manifests and fixed CLI peer-view manifest"
ssh_run "$NODE0_HOST" "cat >'$NODE0_MANIFEST' <<'YAML'
local_ub_node_id: 0
local_region_weight: 4

remote_meta_provider: ub
remote_meta_path: $NODE0_META_LOCAL_PATH
remote_meta_mmap_offset: $REMOTE_META_MMAP_OFFSET
remote_meta_entries: $MAX_VECTORS
remote_meta_buckets: $((MAX_VECTORS * 2))
ub_rpc_timeout_ms: $UB_RPC_TIMEOUT_MS

warm_regions:
  - region_id: 100
    provider: ub
    path: $NODE0_WARM_LOCAL_PATH
    mmap_offset: 0
    bytes: $SERVER_WARM_REGION_BYTES
    value_size: $((DIM * 4))
    home_ub_node_id: 0
    weight: 1
YAML"
ssh_run "$NODE1_HOST" "cat >'$NODE1_MANIFEST' <<'YAML'
local_ub_node_id: 1
local_region_weight: 4

remote_meta_provider: ub
remote_meta_path: $NODE1_META_LOCAL_PATH
remote_meta_mmap_offset: $REMOTE_META_MMAP_OFFSET
remote_meta_entries: $MAX_VECTORS
remote_meta_buckets: $((MAX_VECTORS * 2))
ub_rpc_timeout_ms: $UB_RPC_TIMEOUT_MS

warm_regions:
  - region_id: 101
    provider: ub
    path: $NODE1_WARM_LOCAL_PATH
    mmap_offset: 0
    bytes: $SERVER_WARM_REGION_BYTES
    value_size: $((DIM * 4))
    home_ub_node_id: 1
    weight: 1
  - region_id: 100
    provider: ub
    path: $NODE1_WARM_PEER_PATH
    mmap_offset: 0
    bytes: $SERVER_WARM_REGION_BYTES
    value_size: $((DIM * 4))
    home_ub_node_id: 0
    weight: 1

remote_meta_views:
  - owner_id: 0
    provider: ub
    path: $NODE1_WARM_PEER_PATH
    mmap_offset: $REMOTE_META_MMAP_OFFSET
    entries: $MAX_VECTORS
    buckets: $((MAX_VECTORS * 2))

ub_rpc_peers:
  - owner_id: 0
    provider: ub
    request_path: $NODE1_RPC_REQUEST_PATH
    request_mmap_offset: $SERVER_RPC_REQUEST_OFFSET
    response_path: $NODE1_RPC_RESPONSE_PATH
    response_mmap_offset: $SERVER_RPC_RESPONSE_OFFSET
    inbound_request_path: $NODE1_RPC_RESPONSE_PATH
    inbound_request_mmap_offset: $SERVER_RPC_REQUEST_OFFSET
    outbound_response_path: $NODE1_RPC_REQUEST_PATH
    outbound_response_mmap_offset: $SERVER_RPC_RESPONSE_OFFSET
YAML"
ssh_run "$NODE0_HOST" "cat >'$NODE0_PEER_MAP' <<'YAML'
expected_local_owner_id: 0
attach_now: true
ub_rpc_timeout_ms: $UB_RPC_TIMEOUT_MS

warm_regions:
  - region_id: 101
    provider: ub
    path: $NODE0_WARM_PEER_PATH
    mmap_offset: 0
    bytes: $SERVER_WARM_REGION_BYTES
    value_size: $((DIM * 4))
    home_ub_node_id: 1
    weight: 1

remote_meta_views:
  - owner_id: 1
    provider: ub
    path: $NODE0_WARM_PEER_PATH
    mmap_offset: $REMOTE_META_MMAP_OFFSET
    entries: $MAX_VECTORS
    buckets: $((MAX_VECTORS * 2))

ub_rpc_peers:
  - owner_id: 1
    provider: ub
    request_path: $NODE0_RPC_REQUEST_PATH
    request_mmap_offset: $SERVER_RPC_REQUEST_OFFSET
    response_path: $NODE0_RPC_RESPONSE_PATH
    response_mmap_offset: $SERVER_RPC_RESPONSE_OFFSET
    inbound_request_path: $NODE0_RPC_RESPONSE_PATH
    inbound_request_mmap_offset: $SERVER_RPC_REQUEST_OFFSET
    outbound_response_path: $NODE0_RPC_REQUEST_PATH
    outbound_response_mmap_offset: $SERVER_RPC_RESPONSE_OFFSET
YAML"
ssh_run "$NODE0_HOST" "tmp='$CLIENT_PEER_MANIFEST.tmp'; cat >\"\$tmp\" <<'YAML'
version: 1
peer_views:
  # The workload client runs on node 111 alongside owner 0.  Keep this
  # mapping explicit so the same resolver covers the initial topology.
  - client_host: 111
    owner_id: 0
    resource_role: v1_request_ring
    resource_id: owner111-v1-request
    generation: 1
    provider_path: $NODE0_AERON_REQUEST_PATH
    client_path: $NODE0_AERON_REQUEST_PATH
    map_from_start: true
  - client_host: 111
    owner_id: 0
    resource_role: v1_response_ring
    resource_id: owner111-v1-response
    generation: 1
    provider_path: $NODE0_AERON_RESPONSE_PATH
    client_path: $NODE0_AERON_RESPONSE_PATH
    map_from_start: true
  - client_host: 111
    owner_id: 0
    resource_role: warm_region
    resource_id: owner111-warm
    generation: 1
    provider_path: $NODE0_WARM_LOCAL_PATH
    client_path: $NODE0_WARM_LOCAL_PATH
    map_from_start: true
  - client_host: 111
    owner_id: 0
    resource_role: v2_request_descriptor
    resource_id: owner111-v2-request-descriptor
    generation: 1
    provider_path: $NODE0_AERON_REQUEST_PATH
    client_path: $NODE0_AERON_REQUEST_PATH
    map_from_start: true
  - client_host: 111
    owner_id: 0
    resource_role: v2_request_arena
    resource_id: owner111-v2-request-arena
    generation: 1
    provider_path: $NODE0_AERON_REQUEST_PATH
    client_path: $NODE0_AERON_REQUEST_PATH
    map_from_start: true
  - client_host: 111
    owner_id: 0
    resource_role: v2_response_descriptor
    resource_id: owner111-v2-response-descriptor
    generation: 1
    provider_path: $NODE0_AERON_RESPONSE_PATH
    client_path: $NODE0_AERON_RESPONSE_PATH
    map_from_start: true
  - client_host: 111
    owner_id: 0
    resource_role: v2_response_arena
    resource_id: owner111-v2-response-arena
    generation: 1
    provider_path: $NODE0_AERON_RESPONSE_PATH
    client_path: $NODE0_AERON_RESPONSE_PATH
    map_from_start: true
YAML
sed -n '/^  - client_host: 111$/,\$p' '$REMOTE_DIR/examples/vemb_v16_ub_peer_view_111_to_112.yaml' >>\"\$tmp\"; mv \"\$tmp\" '$CLIENT_PEER_MANIFEST'"

if [ "$USE_EXTERNAL_UB_CONFIG" = "1" ]; then
    step "Install external UB manifests"
    ssh_run "$NODE0_HOST" "cp '$UB_CONFIG_DIR/vemb_v16_ub_cluster_111_to_112_node0.yaml' '$NODE0_MANIFEST' && cp '$UB_CONFIG_DIR/vemb_v16_ub_cluster_111_to_112_node0_peer_map.yaml' '$NODE0_PEER_MAP'"
    ssh_run "$NODE1_HOST" "cp '$UB_CONFIG_DIR/vemb_v16_ub_cluster_111_to_112_node1.yaml' '$NODE1_MANIFEST'"
fi

start_server() {
    local node="$1" manifest="$2" logfile="$3" pid_file="$4"
    local request_path="$5" response_path="$6"
    ssh_run "$node" "cd '$REMOTE_DIR' && rm -f '$pid_file' '$logfile' && numactl --membind=0 taskset -c 0-95 ./src/redis-server --port '$SERVER_PORT' --bind '$node' --protected-mode no --vemb-v16-enabled yes --vemb-v16-dim '$DIM' --vemb-v16-max-vectors '$MAX_VECTORS' --vemb-v16-warm-regions-manifest '$manifest' --vemb-v16-reset-warm-regions yes --vemb-v16-transport aeron --vemb-v16-aeron-ub-path '$request_path' --vemb-v16-aeron-response-ub-path '$response_path' --vemb-v16-proxy-io-threads '$PIO' --vemb-v16-supernode-workers '$SNW' --vemb-v16-batch-request-size '$PIPELINE' --daemonize yes --pidfile '$pid_file' --logfile '$logfile' --loglevel notice"
}

wait_server_ready() {
    local node="$1" pid_file="$2"
    ssh_run "$node" "cd '$REMOTE_DIR' || exit 1; for _ in \$(seq 1 100); do if ./benchmark/vemb_v16_topology_ctl --get --ctl-endpoint tcp --host '$node' --port '$SERVER_PORT' --timeout-ms 1000 >/dev/null 2>&1; then exit 0; fi; if ! kill -0 \$(cat '$pid_file') 2>/dev/null; then exit 1; fi; sleep 1; done; exit 1"
}

memtier_args() {
    printf '%s\n' "--protocol=vemb_v16 --vemb-v16-dim=$DIM --vemb-v16-handle --vemb-v16-transport=aeron --vemb-v16-ub-peer-view-manifest=$CLIENT_PEER_MANIFEST --vemb-v16-ub-peer-view-client-host=111 --vemb-v16-ub-peer-view-owner-id=1 --vemb-v16-batch-request-size=$PIPELINE --vemb-v16-batch-max-delay-us=$BATCH_MAX_DELAY_US"
}

run_memtier() {
    local test_time=$1 outfile=$2 endpoints=$3 key_min=$4 key_max=$5 extra=${6:-}
    # Batch delay is a single workload-wide setting; pass BATCH_MAX_DELAY_US via memtier_args.
    ssh_run "$NODE0_HOST" "cd '$REMOTE_DIR' && numactl --membind=1 taskset -c 96-191 '$MEMTIER' $(memtier_args) --vemb-v16-endpoints='$endpoints' --threads='$MEMTIER_T' --clients='$MEMTIER_C' --pipeline='$PIPELINE' --ratio=0:1 --key-pattern=R:R --key-prefix=item: --key-minimum='$key_min' --key-maximum='$key_max' --test-time='$test_time' $extra >'$outfile' 2>&1"
    ssh_run "$NODE0_HOST" "grep -q '^Totals' '$outfile' && ! grep -Eq 'status_(nf|err)=[1-9]|materialized_fail=[1-9]|unmatched=[1-9]' '$outfile'" || die "memtier workload failed correctness checks: $outfile"
    local totals
    totals=$(ssh_run "$NODE0_HOST" "grep '^Totals' '$outfile' | tail -1")
    [ -n "$totals" ] || die "memtier produced no Totals: $outfile"
    awk '{print $2, $(NF-3), $(NF-2)}' <<<"$totals"
}

run_memtier_bg() {
    local test_time=$1 outfile=$2 endpoints=$3
    ssh_run "$NODE0_HOST" "nohup sh -c 'cd \"$REMOTE_DIR\" && exec numactl --membind=1 taskset -c 96-191 \"$MEMTIER\" $(memtier_args) --vemb-v16-endpoints=\"$endpoints\" --threads=\"$MEMTIER_T\" --clients=\"$MEMTIER_C\" --pipeline=\"$PIPELINE\" --ratio=0:1 --key-pattern=R:R --key-prefix=item: --key-minimum=1 --key-maximum=\"$PREFILL_KEYS\" --vemb-v16-topology-retry-limit=8 --test-time=\"$test_time\"' >'$outfile' 2>&1 < /dev/null & echo \$! >'$DURING_PID_FILE'"
}

parse_bg_out() {
    local totals i
    for i in $(seq 1 15); do
        totals=$(ssh_run "$NODE0_HOST" "grep '^Totals' '$DURING_OUT' | tail -1")
        [ -n "$totals" ] && break
        sleep 1
    done
    [ -n "$totals" ] || die "background memtier produced no Totals"
    # During migration, status_err is expected when a v2 batch reaches the
    # global migration gate, and a very small number of status_nf responses can
    # occur while the client refreshes its stale topology. Keep materialization
    # and completion matching strict; post-scaleout reads below remain strict
    # for status_nf/status_err as well.
    ssh_run "$NODE0_HOST" "! grep -Eq 'materialized_fail=[1-9]|unmatched=[1-9]' '$DURING_OUT'" || die "background memtier failed data correctness checks"
    awk '{print $2, $(NF-3), $(NF-2)}' <<<"$totals"
}

record_phase() {
    local phase=$1 ops=$2 p50=$3 p99=$4 wall=$5 note=$6
    summary_row "$phase" "$ops" "$p50" "$p99" "$wall" "$note" >>"$LOCAL_RESULT_DIR/summary.tsv"
    step "$phase: ops=$ops p99=$p99 wall=${wall}s ($note)"
}

prefill_data() {
    ssh_run "$NODE0_HOST" "cd '$REMOTE_DIR' && numactl --membind=1 taskset -c 96-191 '$MEMTIER' $(memtier_args) --vemb-v16-endpoints='$NODE0_HOST:$SERVER_PORT' --threads=1 --clients=1 --pipeline=32 --requests='$PREFILL_KEYS' --ratio=1:0 --key-pattern=S:S --key-prefix=item: --key-minimum=1 --key-maximum='$PREFILL_KEYS' >'$PREFILL_OUT' 2>&1"
    ssh_run "$NODE0_HOST" "grep -q '^Totals' '$PREFILL_OUT' && ! grep -Eq 'status_(nf|err)=[1-9]|materialized_fail=[1-9]|unmatched=[1-9]' '$PREFILL_OUT'" || die "prefill failed correctness checks"
}

prefill_steady_data() {
    local endpoints=$1 key_min=$2 key_max=$3 count
    count=$((key_max - key_min + 1))
    ssh_run "$NODE0_HOST" "cd '$REMOTE_DIR' && numactl --membind=1 taskset -c 96-191 '$MEMTIER' $(memtier_args) --vemb-v16-endpoints='$endpoints' --threads=1 --clients=1 --pipeline=32 --requests='$count' --ratio=1:0 --key-pattern=S:S --key-prefix=item: --key-minimum='$key_min' --key-maximum='$key_max' >'$AFTER_PREFILL_OUT' 2>&1"
    ssh_run "$NODE0_HOST" "grep -q '^Totals' '$AFTER_PREFILL_OUT' && ! grep -Eq 'status_(nf|err)=[1-9]|materialized_fail=[1-9]|unmatched=[1-9]' '$AFTER_PREFILL_OUT'" || die "steady prefill failed correctness checks"
}

step "Start owner 0"
start_server "$NODE0_HOST" "$NODE0_MANIFEST" "$NODE0_LOG" "$NODE0_PID_FILE" \
    "$NODE0_AERON_REQUEST_PATH" "$NODE0_AERON_RESPONSE_PATH"
wait_server_ready "$NODE0_HOST" "$NODE0_PID_FILE"

step "Initial topology and prefill"
ssh_run "$NODE0_HOST" "cd '$REMOTE_DIR' && ./benchmark/vemb_v16_topology_ctl --set --ctl-endpoint tcp --data-endpoint aeron --host '$NODE0_HOST' --port '$SERVER_PORT' --epoch '$INIT_EPOCH' --min-write-epoch '$INIT_EPOCH' --vnode-count '$VNODE_COUNT' --active 0 --standby 0 --owner-endpoints '0=$NODE0_HOST:$SERVER_PORT' --timeout-ms '$CONTROL_TIMEOUT_MS' >'$REMOTE_RESULT_DIR/topology_baseline.out'"
prefill_data

FINAL_ENDPOINTS="$NODE0_HOST:$SERVER_PORT,$NODE1_HOST:$SERVER_PORT"
BOOTSTRAP_ENDPOINTS="$NODE0_HOST:$SERVER_PORT"
step "Baseline memtier read"
T0=$(date +%s)
result=$(run_memtier "$TEST_TIME" "$REMOTE_RESULT_DIR/scaleout_baseline.out" "$NODE0_HOST:$SERVER_PORT" 1 "$PREFILL_KEYS")
T1=$(date +%s)
read ops p50 p99 <<<"$result"
record_phase scaleout_baseline "$ops" "$p50" "$p99" "$((T1 - T0))" 'active={0}'

step "Start owner 1 as standby"
start_server "$NODE1_HOST" "$NODE1_MANIFEST" "$NODE1_LOG" "$NODE1_PID_FILE" \
    "$NODE1_AERON_REQUEST_PATH" "$NODE1_AERON_RESPONSE_PATH"
wait_server_ready "$NODE1_HOST" "$NODE1_PID_FILE"

step "During scaleout: stale topology refresh and dynamic owner attach"
# Keep the workload's only bootstrap seed on owner 0.  The client must not
# learn owner 1 from runner arguments: after the candidate topology is
# published, owner 0 returns STALE_TOPOLOGY/MOVED, the client refreshes the
# snapshot over TCP, resolves owner 1's Aeron endpoint in that snapshot, then
# performs TCP ATTACH using the pre-installed peer-view mapping.
run_memtier_bg "$BG_TIME_SCALEOUT" "$DURING_OUT" "$NODE0_HOST:$SERVER_PORT"
T0=$(date +%s)
# The source candidate must be submitted only after the coordinator has
# entered its listen state.  Submitting first updates node1's topology but
# loses the one-shot source notification, leaving the coordinator forever at
# scaleout_all_sources_done=0.
ssh_run "$NODE0_HOST" "nohup sh -c 'cd \"$REMOTE_DIR\" && exec ./benchmark/vemb_v16_topology_ctl --coordinator-listen --ctl-endpoint tcp --data-endpoint aeron --host \"$NODE0_HOST\" --port \"$COORD_PORT\" --vnode-count \"$VNODE_COUNT\" --expected-sources 0 --migration-epoch \"$MIGRATION_EPOCH\" --cutover-epoch \"$CUTOVER_EPOCH\" --standby 0,1 --owner-endpoints \"0=$NODE0_HOST:$SERVER_PORT,1=$NODE1_HOST:$SERVER_PORT\" --wait-ms \"$COORD_WAIT_MS\" --timeout-ms \"$CONTROL_TIMEOUT_MS\"' >'$COORD_OUT' 2>'$COORD_ERR' < /dev/null & echo \$! >'$COORD_PID_FILE'"
sleep 1
# UB local migration may keep node1's combined control request open until the
# source owner notification arrives. Keep this SSH request in a local
# background job, so the remote topology_ctl itself remains foreground and its
# exit status is observable; node0 can submit its peer-view candidate in
# parallel without relying on remote shell backgrounding semantics.
ssh_run "$NODE1_HOST" "cd '$REMOTE_DIR' && date '+%Y-%m-%dT%H:%M:%S%z' >'$NODE1_CANDIDATE_STARTED' && ./benchmark/vemb_v16_topology_ctl --set --ctl-endpoint tcp --data-endpoint aeron --host '$NODE1_HOST' --port '$SERVER_PORT' --epoch '$MIGRATION_EPOCH' --min-write-epoch '$MIGRATION_EPOCH' --vnode-count '$VNODE_COUNT' --active 0 --standby 0,1 --dual-write --auto-scaleout --coordinated-scaleout --owner-endpoints '0=$NODE0_HOST:$SERVER_PORT,1=$NODE1_HOST:$SERVER_PORT' --coordinator-endpoint '$NODE0_HOST:$COORD_PORT' --timeout-ms '$COMBINED_CONTROL_TIMEOUT_MS' >'$REMOTE_RESULT_DIR/topology_candidate_node1.out' 2>'$NODE1_CANDIDATE_ERR'; rc=\$?; printf '%s rc=%s\\n' \"\$(date '+%Y-%m-%dT%H:%M:%S%z')\" \"\$rc\" >'$NODE1_CANDIDATE_FINISHED'; exit \"\$rc\"" &
NODE1_CANDIDATE_SSH_PID=$!
runner_log "node1 candidate ssh background pid=$NODE1_CANDIDATE_SSH_PID"
ssh_run "$NODE0_HOST" "cd '$REMOTE_DIR' && ./benchmark/vemb_v16_topology_ctl --set-with-peer-view-map '$NODE0_PEER_MAP' --ctl-endpoint tcp --data-endpoint aeron --host '$NODE0_HOST' --port '$SERVER_PORT' --epoch '$MIGRATION_EPOCH' --min-write-epoch '$MIGRATION_EPOCH' --vnode-count '$VNODE_COUNT' --active 0 --standby 0,1 --dual-write --auto-scaleout --coordinated-scaleout --owner-endpoints '0=$NODE0_HOST:$SERVER_PORT,1=$NODE1_HOST:$SERVER_PORT' --coordinator-endpoint '$NODE0_HOST:$COORD_PORT' --timeout-ms '$COMBINED_CONTROL_TIMEOUT_MS' >'$REMOTE_RESULT_DIR/topology_candidate_node0.out'"
if wait "$NODE1_CANDIDATE_SSH_PID"; then
    runner_log "node1 candidate ssh wait succeeded pid=$NODE1_CANDIDATE_SSH_PID"
else
    candidate_rc=$?
    runner_log "node1 candidate ssh wait failed pid=$NODE1_CANDIDATE_SSH_PID rc=$candidate_rc"
    die "node1 candidate topology request failed"
fi
NODE1_CANDIDATE_SSH_PID=""
ssh_run "$NODE0_HOST" "for _ in \$(seq 1 $((COORD_WAIT_MS / 1000 + 10))); do if grep -q '^scaleout_all_sources_done=1$' '$COORD_OUT' && grep -q '^scaleout_full_active_published=2 errors=0 targets=2$' '$COORD_OUT'; then exit 0; fi; sleep 1; done; exit 1"
# A successful coordinator publish is only the control-plane half of the
# protocol.  The source must accept local-done ACK and complete source GC
# before its global migration gate is allowed to clear.
ssh_run "$NODE0_HOST" "for _ in \$(seq 1 $((COORD_WAIT_MS / 1000 + 10))); do if grep -q 'scaleout auto local done notified' '$NODE0_LOG' && grep -q 'scaleout auto done' '$NODE0_LOG'; then exit 0; fi; sleep 1; done; echo 'source did not reach local-done notified and done' >&2; grep -E 'scaleout auto|local done|migration_active|batch rejected|ATTACH rejected' '$NODE0_LOG' >&2 || true; exit 1"
T1=$(date +%s)
SCALEOUT_WALL=$((T1 - T0))
REMAIN=$((BG_TIME_SCALEOUT - SCALEOUT_WALL))
if [ "$REMAIN" -gt 0 ]; then sleep "$REMAIN"; fi
sleep 2
result=$(parse_bg_out)
read ops p50 p99 <<<"$result"
record_phase during_scaleout "$ops" "$p50" "$p99" "$SCALEOUT_WALL" "active={0}->{0,1}, endpoints=$FINAL_ENDPOINTS"

step "After scaleout old-key read"
T0=$(date +%s)
result=$(run_memtier "$TEST_TIME" "$AFTER_OLD_OUT" "$BOOTSTRAP_ENDPOINTS" 1 "$PREFILL_KEYS")
T1=$(date +%s)
read ops p50 p99 <<<"$result"
record_phase scaleout_after_old_keys "$ops" "$p50" "$p99" "$((T1 - T0))" "active={0,1}, old_keys=1-$PREFILL_KEYS"

step "After scaleout steady-key read"
prefill_steady_data "$BOOTSTRAP_ENDPOINTS" "$STEADY_KEY_MIN" "$STEADY_KEY_MAX"
T0=$(date +%s)
result=$(run_memtier "$TEST_TIME" "$AFTER_OUT" "$BOOTSTRAP_ENDPOINTS" "$STEADY_KEY_MIN" "$STEADY_KEY_MAX")
T1=$(date +%s)
read ops p50 p99 <<<"$result"
record_phase scaleout_after "$ops" "$p50" "$p99" "$((T1 - T0))" "active={0,1}, steady_keys=$STEADY_KEY_MIN-$STEADY_KEY_MAX"

step "Capture topology and migration evidence"
for node in "$NODE0_HOST" "$NODE1_HOST"; do
    ssh_run "$node" "cd '$REMOTE_DIR' && ./benchmark/vemb_v16_topology_ctl --get --ctl-endpoint tcp --host '$node' --port '$SERVER_PORT' --timeout-ms '$CONTROL_TIMEOUT_MS' >'$REMOTE_RESULT_DIR/topology_final_${node##*.}.out'"
done
ssh_run "$NODE0_HOST" "grep -E 'runtime warm region attached|runtime remote_meta owner view attached|runtime ub rpc peer attached|migration auto plan|scaleout auto|local done|migration_active|batch rejected|ATTACH rejected' '$NODE0_LOG' >'$REMOTE_RESULT_DIR/node0_migration_summary.log' || true"
ssh_run "$NODE1_HOST" "grep -E 'migration auto plan|scaleout auto|local done|migration_active|batch rejected|ATTACH rejected' '$NODE1_LOG' >'$REMOTE_RESULT_DIR/node1_migration_summary.log' || true"

if [ "$KEEP_SERVERS" = "0" ]; then
    step "Stop runner-owned processes before archiving"
    stop_owned_processes
fi

archive_remote_results() {
    local node="$1" destination="$2" host port
    case "$node" in
        "$NODE0_HOST") host="$NODE0_SSH_HOST"; port="$NODE0_SSH_PORT" ;;
        "$NODE1_HOST") host="$NODE1_SSH_HOST"; port="$NODE1_SSH_PORT" ;;
        *) die "unknown archive node: $node" ;;
    esac

    # Keep the largest workload output out of the recursive copy. Transfer it
    # separately in compressed form while retaining the raw file remotely.
    ssh -p "$port" "${SSH_USER}@${host}" \
        "cd '$REMOTE_RESULT_DIR' && tar --exclude='./scaleout_after.out' -cf - ." \
        | tar -xf - -C "$destination"
    ssh -p "$port" "${SSH_USER}@${host}" \
        "gzip -c '$REMOTE_RESULT_DIR/scaleout_after.out'" \
        >"$destination/scaleout_after.out.gz"
}

if [ "$ARCHIVE_LOCAL" = "1" ]; then
    step "Archive remote P7 artifacts locally"
    mkdir -p "$LOCAL_RESULT_DIR/node0" "$LOCAL_RESULT_DIR/node1"
    archive_remote_results "$NODE0_HOST" "$LOCAL_RESULT_DIR/node0"
    archive_remote_results "$NODE1_HOST" "$LOCAL_RESULT_DIR/node1"
fi

printf 'UB/Aeron memtier scaleout passed. Artifacts: %s\n' "$LOCAL_RESULT_DIR"
cat "$LOCAL_RESULT_DIR/summary.tsv"
