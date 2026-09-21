#!/usr/bin/env bash
# Aeron/UB HA failover regression for one active owner and one standby.
#
# This runner is deliberately separate from tlc_ha_replica_ub_111_to_112.sh
# and vemb_v16_ub_active_2node_111_to_112.sh:
#   * owner 0 (111) and owner 1 (112) are identities, not simultaneous owners;
#   * topology.active is exactly one owner (0 -> 1 during failover);
#   * HA Replica keeps dev4/dev13 on 111 and dev9/dev8 on 112;
#   * owner0 uses dev3/dev6/dev1 and owner1 uses dev10/dev15/dev12 for
#     request/response/warm, with the corresponding peer views on the CLI.
#
# The default run is 111 (leader/owner 0) -> 112 (follower/owner 1).  Set
# INITIAL_NODE=112 CLIENT_NODE=112 to exercise the reverse direction.
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
CONFIG_FILE="${TLC_HA_AERON_CONFIG:-$ROOT_DIR/examples/tlc_ha_replica_ub_111_to_112.env}"
if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    cat <<'USAGE'
Usage: benchmark/tlc_ha_aeron_failover_111_to_112.sh

Run the one-active-owner Aeron/UB HA regression. The default direction is
111 (owner 0, leader) -> 112 (owner 1, follower). Environment overrides:
  INITIAL_NODE=112 CLIENT_NODE=112   run the reverse direction
  TLC_HA_AERON_SKIP_SYNC=1            reuse already-built remote binaries
  TLC_HA_AERON_SKIP_KEEPALIVED=1      use HA PROMOTE instead of VRRP/VIP
  TLC_HA_AERON_RESET_REPLICA=0        skip Replica ring reset (diagnostic only)
  TLC_HA_AERON_KEEP_SERVERS=1         leave Redis processes for inspection
  TLC_HA_AERON_TCP_PROBE=0            disable the old TCP connection probe
  TLC_HA_AERON_RUN_ID=name             choose the remote artifact directory
USAGE
    exit 0
fi
if [ ! -f "$CONFIG_FILE" ]; then
    printf 'HA Aeron config not found: %s\n' "$CONFIG_FILE" >&2
    exit 2
fi
# shellcheck disable=SC1090
. "$CONFIG_FILE"

REMOTE_DIR="${HA_REMOTE_DIR:-/root/szz/codespace/hpc-redis}"
SSH_HOST="${TLC_HA_AERON_SSH_HOST:-43.154.145.18}"
SSH_USER="${TLC_HA_AERON_SSH_USER:-root}"
NODE111_PORT="${HA_NODE111_PORT:-8111}"
NODE112_PORT="${HA_NODE112_PORT:-8112}"
NODE111_HOST="${TLC_HA_NODE111_HOST:-192.168.90.111}"
NODE112_HOST="${TLC_HA_NODE112_HOST:-192.168.90.112}"
SERVER_PORT="${TLC_HA_AERON_SERVER_PORT:-6379}"
CONTROL_PORT="${TLC_HA_AERON_CONTROL_PORT:-9738}"
DIM="${TLC_HA_AERON_DIM:-16}"
MAX_VECTORS="${TLC_HA_AERON_MAX_VECTORS:-4096}"
PIO="${TLC_HA_AERON_PROXY_IO_THREADS:-1}"
SNW="${TLC_HA_AERON_SUPERNODE_WORKERS:-1}"
PIPELINE="${TLC_HA_AERON_PIPELINE:-1}"
PREFILL="${TLC_HA_AERON_PREFILL:-8}"
OPS="${TLC_HA_AERON_OPS:-8}"
TIMEOUT_MS="${TLC_HA_AERON_TIMEOUT_MS:-15000}"
WAIT_SECONDS="${TLC_HA_AERON_WAIT_SECONDS:-45}"
VIP_CIDR="${TLC_HA_AERON_VIP:-192.168.90.202/24}"
VIP="${VIP_CIDR%%/*}"
RUN_ID="${TLC_HA_AERON_RUN_ID:-$(date +%Y%m%d_%H%M%S)-$$}"
SSH_CONTROL_PATH="/tmp/tlc_ha_aeron_${USER:-user}_$$_%p"
INITIAL_NODE="${INITIAL_NODE:-111}"
CLIENT_NODE="${CLIENT_NODE:-$INITIAL_NODE}"
SKIP_SYNC="${TLC_HA_AERON_SKIP_SYNC:-0}"
SKIP_KEEPALIVED="${TLC_HA_AERON_SKIP_KEEPALIVED:-0}"
KEEP_SERVERS="${TLC_HA_AERON_KEEP_SERVERS:-0}"
RESET_WARM="${TLC_HA_AERON_RESET_WARM:-yes}"
RESET_REPLICA="${TLC_HA_AERON_RESET_REPLICA:-1}"
TCP_PROBE="${TLC_HA_AERON_TCP_PROBE:-1}"

case "$INITIAL_NODE:$CLIENT_NODE" in
    111:111|112:112) ;;
    *) printf 'INITIAL_NODE and CLIENT_NODE must both be 111 or both be 112\n' >&2; exit 2 ;;
esac
case "$RUN_ID" in *[!A-Za-z0-9_-]*|'') printf 'invalid RUN_ID: %s\n' "$RUN_ID" >&2; exit 2 ;; esac
case "$VIP_CIDR" in */*) ;; *) printf 'VIP must include CIDR: %s\n' "$VIP_CIDR" >&2; exit 2 ;; esac

NODE111_RUN_DIR="$REMOTE_DIR/benchmark/results/tlc_ha_aeron_failover/$RUN_ID/node111"
NODE112_RUN_DIR="$REMOTE_DIR/benchmark/results/tlc_ha_aeron_failover/$RUN_ID/node112"
CLIENT_RUN_DIR="$REMOTE_DIR/benchmark/results/tlc_ha_aeron_failover/$RUN_ID/client"
TCP_PROBE_OUT="$CLIENT_RUN_DIR/old_tcp_connection.out"
TCP_PROBE_PID="$CLIENT_RUN_DIR/old_tcp_connection.pid"

node_port() { [ "$1" = 111 ] && printf '%s' "$NODE111_PORT" || printf '%s' "$NODE112_PORT"; }
node_host() { [ "$1" = 111 ] && printf '%s' "$NODE111_HOST" || printf '%s' "$NODE112_HOST"; }
node_ssh() {
    local node=$1
    shift
    ssh -q -p "$(node_port "$node")" -o BatchMode=yes -o ConnectTimeout=10 \
        -o ControlMaster=auto -o ControlPersist=60 -o "ControlPath=$SSH_CONTROL_PATH" \
        "$SSH_USER@$SSH_HOST" "$@"
}
remote() { local node=$1; shift; node_ssh "$node" "cd '$REMOTE_DIR' && $*"; }

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
step() { printf '\n===== %s =====\n' "$*"; }

server_dir() { [ "$1" = 111 ] && printf '%s' "$NODE111_RUN_DIR" || printf '%s' "$NODE112_RUN_DIR"; }
server_manifest() { printf '%s/server.yaml' "$(server_dir "$1")"; }
server_log() { printf '%s/server.log' "$(server_dir "$1")"; }
server_pid_file() { printf '%s/server.pid' "$(server_dir "$1")"; }
server_peer_map() { printf '%s/peer-view-map.yaml' "$(server_dir "$1")"; }
cold_dir() { printf '%s/cold' "$(server_dir "$1")"; }

stop_redis() {
    local node=$1 pid_file
    pid_file="$(server_pid_file "$node")"
    remote "$node" "if test -s '$pid_file'; then p=\$(cat '$pid_file'); kill -TERM \$p 2>/dev/null || true; fi; for p in \$(ss -ltnp 2>/dev/null | awk -F'pid=' '/:${SERVER_PORT} / && /redis-server/ {split(\$2,a,\",\"); print a[1]}' | sort -u); do kill -TERM \$p 2>/dev/null || true; done; sleep 1; if test -s '$pid_file'; then p=\$(cat '$pid_file'); kill -KILL \$p 2>/dev/null || true; fi; for p in \$(ss -ltnp 2>/dev/null | awk -F'pid=' '/:${SERVER_PORT} / && /redis-server/ {split(\$2,a,\",\"); print a[1]}' | sort -u); do kill -KILL \$p 2>/dev/null || true; done"
}

stop_keepalived() {
    local node=$1 interface
    interface=$([ "$node" = 111 ] && printf eth1 || printf eth0)
    remote "$node" "for p in \$(pgrep -x keepalived || true); do kill -TERM \$p 2>/dev/null || true; done; sleep 1; for p in \$(pgrep -x keepalived || true); do kill -KILL \$p 2>/dev/null || true; done; ip addr del '$VIP_CIDR' dev '$interface' 2>/dev/null || true; rm -f /run/hpc-redis-keepalived/keepalived.pid /run/hpc-redis-keepalived/vrrp.pid /run/hpc-redis-keepalived/checkers.pid"
}

vip_count() {
    local count=0 node
    for node in 111 112; do
        if remote "$node" "ip -o -4 addr show | awk '\$4 == \"$VIP_CIDR\" {found=1} END {exit found ? 0 : 1}'" >/dev/null 2>&1; then
            count=$((count + 1))
        fi
    done
    printf '%s' "$count"
}

wait_vip_owner() {
    local expected=$1 deadline=$((SECONDS + WAIT_SECONDS)) count
    while (( SECONDS < deadline )); do
        count="$(vip_count)"
        if [ "$count" = 1 ] && remote "$expected" "ip -o -4 addr show | awk '\$4 == \"$VIP_CIDR\" {found=1} END {exit found ? 0 : 1}'" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    printf 'VIP owner timeout: expected=%s count=%s\n' "$expected" "$(vip_count)" >&2
    return 1
}

ha_state() {
    local node=$1
    remote "$node" "redis-cli --raw -h 127.0.0.1 -p '$SERVER_PORT' HA STATE 2>/dev/null | tr '\\n' ' '" || true
}

wait_ha_state() {
    local node=$1 role=$2 state=$3 deadline=$((SECONDS + WAIT_SECONDS)) value
    while (( SECONDS < deadline )); do
        value="$(ha_state "$node")"
        if [[ "$value" == *"$role"* && "$value" == *"$state"* ]]; then return 0; fi
        sleep 1
    done
    printf 'HA state timeout node=%s expected=%s/%s actual=%s\n' "$node" "$role" "$state" "$(ha_state "$node")" >&2
    return 1
}

ha_progress() {
    local node=$1
    remote "$node" "redis-cli --raw -h 127.0.0.1 -p '$SERVER_PORT' HA PROGRESS 2>/dev/null | paste -sd ' ' -" || true
}

progress_value() {
    local progress=$1 key=$2
    awk -v key="$key" '{for (i = 1; i < NF; i++) if ($i == key) {print $(i + 1); exit}}' <<<"$progress"
}

wait_recovery_progress() {
    local follower=$1 leader=$2 deadline=$((SECONDS + WAIT_SECONDS)) follower_progress leader_progress
    local follower_durable follower_applied leader_appended leader_peer_durable leader_peer_applied
    while (( SECONDS < deadline )); do
        follower_progress="$(ha_progress "$follower")"
        leader_progress="$(ha_progress "$leader")"
        follower_durable="$(progress_value "$follower_progress" durable_seq)"
        follower_applied="$(progress_value "$follower_progress" applied_seq)"
        leader_appended="$(progress_value "$leader_progress" appended_seq)"
        leader_peer_durable="$(progress_value "$leader_progress" peer_durable_seq)"
        leader_peer_applied="$(progress_value "$leader_progress" peer_applied_seq)"
        if [ -n "$follower_durable" ] && [ "$follower_durable" != 0 ] &&
           [ "$follower_durable" = "$follower_applied" ] &&
           [ "$follower_durable" = "$leader_appended" ] &&
           [ "$follower_durable" = "$leader_peer_durable" ] &&
           [ "$follower_applied" = "$leader_peer_applied" ]; then
            printf 'recovery progress caught up: follower=%s seq=%s leader=%s\n' "$follower" "$follower_durable" "$leader"
            return 0
        fi
        sleep 1
    done
    printf 'recovery progress timeout: follower=%s progress=%s leader=%s progress=%s\n' \
        "$follower" "$(ha_progress "$follower")" "$leader" "$(ha_progress "$leader")" >&2
    return 1
}

sync_node() {
    local node=$1
    NODE="$SSH_HOST" SSH_USER="$SSH_USER" SSH_PORT="$(node_port "$node")" REMOTE_ROOT="$REMOTE_DIR" \
        bash "$ROOT_DIR/scripts/sync_changed_code_to_peer.sh" --all-code --build all --verify-build all
}

write_manifest() {
    local node=$1 manifest owner region request_path response_path warm_path
    manifest="$(server_manifest "$node")"
    owner=$([ "$node" = 111 ] && printf 0 || printf 1)
    region=$([ "$node" = 111 ] && printf 100 || printf 101)
    if [ "$node" = 111 ]; then
        request_path=/dev/obmm_shmdev3
        response_path=/dev/obmm_shmdev6
        warm_path=/dev/obmm_shmdev1
    else
        request_path=/dev/obmm_shmdev10
        response_path=/dev/obmm_shmdev15
        warm_path=/dev/obmm_shmdev12
    fi
    remote "$node" "mkdir -p '$(server_dir "$node")' '$(cold_dir "$node")' '$CLIENT_RUN_DIR'; cat > '$manifest' <<'YAML'
version: 1
local_ub_node_id: $owner
local_region_weight: 4
remote_meta_provider: ub
remote_meta_path: $warm_path
remote_meta_mmap_offset: 268435456
remote_meta_entries: 65536
remote_meta_buckets: 131072
ub_rpc_timeout_ms: 2000
warm_regions:
  - region_id: $region
    provider: ub
    path: $warm_path
    mmap_offset: 0
    bytes: 1073741824
    value_size: $((DIM * 4))
    home_ub_node_id: $owner
    weight: 1
YAML"
}

write_server_peer_map() {
    local node=$1 path owner region
    path="$(server_peer_map "$node")"
    owner=$([ "$node" = 111 ] && printf 0 || printf 1)
    region=$([ "$node" = 111 ] && printf 101 || printf 100)
    remote "$node" "cat > '$path' <<'YAML'
expected_local_owner_id: $owner
attach_now: true
ub_rpc_timeout_ms: 2000

warm_regions:
  - region_id: $region
    provider: ub
    path: /dev/obmm_shmdev16
    mmap_offset: 0
    bytes: 1073741824
    value_size: $((DIM * 4))
    home_ub_node_id: $([ "$node" = 111 ] && printf 1 || printf 0)
    weight: 1

remote_meta_views:
  - owner_id: $([ "$node" = 111 ] && printf 1 || printf 0)
    provider: ub
    path: /dev/obmm_shmdev16
    mmap_offset: 268435456
    entries: 65536
    buckets: 131072

ub_rpc_peers:
  - owner_id: $([ "$node" = 111 ] && printf 1 || printf 0)
    provider: ub
    request_path: /dev/obmm_shmdev10
    request_mmap_offset: 134217728
    response_path: /dev/obmm_shmdev15
    response_mmap_offset: 201326592
    inbound_request_path: /dev/obmm_shmdev14
    inbound_request_mmap_offset: 134217728
    outbound_response_path: /dev/obmm_shmdev11
    outbound_response_mmap_offset: 201326592
YAML"
}

write_client_manifest() {
    local client=$1 path
    path="$CLIENT_RUN_DIR/ub_peer_view.yaml"
    remote "$client" "mkdir -p '$CLIENT_RUN_DIR'; cat > '$path' <<'YAML'
version: 1
peer_views:
  - client_host: $client
    owner_id: 0
    resource_role: v1_request_ring
    resource_id: owner0-v1-request
    generation: 1
    provider_path: /dev/obmm_shmdev3
    client_path: $([ "$client" = 111 ] && printf /dev/obmm_shmdev3 || printf /dev/obmm_shmdev7)
    map_from_start: true
  - client_host: $client
    owner_id: 0
    resource_role: v1_response_ring
    resource_id: owner0-v1-response
    generation: 1
    provider_path: /dev/obmm_shmdev6
    client_path: $([ "$client" = 111 ] && printf /dev/obmm_shmdev6 || printf /dev/obmm_shmdev2)
    map_from_start: true
  - client_host: $client
    owner_id: 0
    resource_role: warm_region
    resource_id: owner0-warm
    generation: 1
    provider_path: /dev/obmm_shmdev1
    client_path: $([ "$client" = 111 ] && printf /dev/obmm_shmdev1 || printf /dev/obmm_shmdev5)
    map_from_start: true
  - client_host: $client
    owner_id: 1
    resource_role: v1_request_ring
    resource_id: owner1-v1-request
    generation: 1
    provider_path: /dev/obmm_shmdev10
    client_path: $([ "$client" = 111 ] && printf /dev/obmm_shmdev14 || printf /dev/obmm_shmdev10)
    map_from_start: true
  - client_host: $client
    owner_id: 1
    resource_role: v1_response_ring
    resource_id: owner1-v1-response
    generation: 1
    provider_path: /dev/obmm_shmdev15
    client_path: $([ "$client" = 111 ] && printf /dev/obmm_shmdev11 || printf /dev/obmm_shmdev15)
    map_from_start: true
  - client_host: $client
    owner_id: 1
    resource_role: warm_region
    resource_id: owner1-warm
    generation: 1
    provider_path: /dev/obmm_shmdev12
    client_path: $([ "$client" = 111 ] && printf /dev/obmm_shmdev16 || printf /dev/obmm_shmdev12)
    map_from_start: true
YAML"
    CLIENT_MANIFEST="$path"
}

reset_replica_rings() {
    remote 111 "TLC_HA_UB_TX_PATH='${TLC_HA_NODE111_TX_PATH:-/dev/obmm_shmdev4}' TLC_HA_UB_RX_PATH='${TLC_HA_NODE111_RX_PATH:-/dev/obmm_shmdev13}' TLC_HA_UB_TX_OFFSET='${TLC_HA_UB_TX_OFFSET:-134217728}' TLC_HA_UB_RX_OFFSET='${TLC_HA_UB_RX_OFFSET:-201326592}' TLC_HA_UB_RESET=1 TLC_HA_UB_RESET_ONLY=1 timeout 30s ./benchmark/tlc_ha_replica_ub_node_ut follower >/dev/null"
    remote 112 "TLC_HA_UB_TX_PATH='${TLC_HA_NODE112_TX_PATH:-/dev/obmm_shmdev9}' TLC_HA_UB_RX_PATH='${TLC_HA_NODE112_RX_PATH:-/dev/obmm_shmdev8}' TLC_HA_UB_TX_OFFSET='${TLC_HA_UB_RX_OFFSET:-201326592}' TLC_HA_UB_RX_OFFSET='${TLC_HA_UB_TX_OFFSET:-134217728}' TLC_HA_UB_RESET=1 TLC_HA_UB_RESET_ONLY=1 timeout 30s ./benchmark/tlc_ha_replica_ub_node_ut follower >/dev/null"
}

start_server() {
    local node=$1 role=$2 node_id=$3 peer_id=$4 peer_host=$5 tx rx txoff rxoff aeron_req aeron_resp
    tx=$([ "$node" = 111 ] && printf '%s' "${TLC_HA_NODE111_TX_PATH:-/dev/obmm_shmdev4}" || printf '%s' "${TLC_HA_NODE112_TX_PATH:-/dev/obmm_shmdev9}")
    rx=$([ "$node" = 111 ] && printf '%s' "${TLC_HA_NODE111_RX_PATH:-/dev/obmm_shmdev13}" || printf '%s' "${TLC_HA_NODE112_RX_PATH:-/dev/obmm_shmdev8}")
    txoff=$([ "$node" = 111 ] && printf '%s' "${TLC_HA_UB_TX_OFFSET:-134217728}" || printf '%s' "${TLC_HA_UB_RX_OFFSET:-201326592}")
    rxoff=$([ "$node" = 111 ] && printf '%s' "${TLC_HA_UB_RX_OFFSET:-201326592}" || printf '%s' "${TLC_HA_UB_TX_OFFSET:-134217728}")
    aeron_req=$([ "$node" = 111 ] && printf /dev/obmm_shmdev3 || printf /dev/obmm_shmdev10)
    aeron_resp=$([ "$node" = 111 ] && printf /dev/obmm_shmdev6 || printf /dev/obmm_shmdev15)
    remote "$node" "rm -f '$(server_pid_file "$node")' '$(server_log "$node")'; HPC_REDIS_COLD_DIR='$(cold_dir "$node")' HPC_REDIS_HA_ROLE='$role' HPC_REDIS_HA_NODE_ID='$node_id' HPC_REDIS_HA_PEER_NODE_ID='$peer_id' HPC_REDIS_HA_TERM='$HA_TERM' HPC_REDIS_HA_CONTROL_BIND_HOST='$(node_host "$node")' HPC_REDIS_HA_CONTROL_PORT='$CONTROL_PORT' HPC_REDIS_HA_PEER_HOST='$peer_host' HPC_REDIS_HA_PEER_PORT='$CONTROL_PORT' HPC_REDIS_HA_TX_PATH='$tx' HPC_REDIS_HA_TX_OFFSET='$txoff' HPC_REDIS_HA_RX_PATH='$rx' HPC_REDIS_HA_RX_OFFSET='$rxoff' ./src/redis-server --port '$SERVER_PORT' --bind 0.0.0.0 --protected-mode no --save '' --appendonly no --vemb-v16-enabled yes --vemb-v16-dim '$DIM' --vemb-v16-max-vectors '$MAX_VECTORS' --vemb-v16-warm-regions-manifest '$(server_manifest "$node")' --vemb-v16-reset-warm-regions '$RESET_WARM' --vemb-v16-transport aeron --vemb-v16-aeron-control tcp --vemb-v16-aeron-ub-path '$aeron_req' --vemb-v16-aeron-response-ub-path '$aeron_resp' --vemb-v16-tcp-host '$(node_host "$node")' --vemb-v16-proxy-io-threads '$PIO' --vemb-v16-supernode-workers '$SNW' --daemonize yes --pidfile '$(server_pid_file "$node")' --logfile '$(server_log "$node")' --loglevel notice"
}

set_topology() {
    local node=$1 active=$2 epoch=$3
    remote "$node" "./benchmark/vemb_v16_topology_ctl --set-with-peer-view-map '$(server_peer_map "$node")' --ctl-endpoint tcp --data-endpoint aeron --host '$(node_host "$node")' --port '$SERVER_PORT' --epoch '$epoch' --min-write-epoch '$epoch' --vnode-count 100 --active '$active' --standby 0,1 --owner-endpoints '0=$NODE111_HOST:$SERVER_PORT,1=$NODE112_HOST:$SERVER_PORT' --timeout-ms 15000"
}

run_client_mode() {
    local client=$1 label=$2 expected_owner=$3 mode=$4 out endpoint_host client_timeout_seconds prefill_arg
    out="$CLIENT_RUN_DIR/$label.out"
    if [ "$SKIP_KEEPALIVED" = 1 ]; then
        endpoint_host=$([ "$expected_owner" = 0 ] && printf '%s' "$NODE111_HOST" || printf '%s' "$NODE112_HOST")
    else
        endpoint_host=$VIP
    fi
    prefill_arg="$PREFILL"
    [ "$mode" = vemb-handle ] && prefill_arg=0
    client_timeout_seconds=$((TIMEOUT_MS / 1000 + 60))
    if ! remote "$client" "timeout --signal=TERM --kill-after=5s '${client_timeout_seconds}s' ./benchmark/vemb_v16_bench --transport aeron --endpoints '$NODE111_HOST:$SERVER_PORT,$NODE112_HOST:$SERVER_PORT' --ha-endpoint '$endpoint_host:$SERVER_PORT' --dim '$DIM' --prefill '$prefill_arg' --keyspace '$PREFILL' --ops '$OPS' --threads 1 --pipeline '$PIPELINE' --mode '$mode' --timeout-ms '$TIMEOUT_MS' --no-pin --ub-peer-view-manifest '$CLIENT_MANIFEST' --ub-peer-view-client-host '$client' >'$out' 2>&1"; then
        printf 'WARN: client SSH command returned non-zero; validating remote output: %s\n' "$out" >&2
    fi
    remote "$client" "grep -Eq '\\[done\\].*fail=0' '$out'"
    remote "$client" "grep -Eq 'owner channel ready owner=$expected_owner .*transport=aeron' '$out'"
    printf 'client=%s label=%s owner=%s mode=%s output=%s\n' "$client" "$label" "$expected_owner" "$mode" "$out"
}

run_client() {
    run_client_mode "$1" "$2" "$3" vadd
}

start_tcp_probe() {
    [ "$TCP_PROBE" = 1 ] || return 0
    local client=$1 endpoint_host=$2
    remote "$client" "command -v redis-cli >/dev/null; rm -f '$TCP_PROBE_OUT' '$TCP_PROBE_PID'; ( { while :; do printf 'PING\\n'; sleep 1; done; } | timeout --signal=TERM --kill-after=2s 30s redis-cli --raw -h '$endpoint_host' -p '$SERVER_PORT' >'$TCP_PROBE_OUT' 2>&1; printf 'probe_exit=%s\\n' \"\$?\" >>'$TCP_PROBE_OUT' ) >/dev/null 2>&1 & echo \$! >'$TCP_PROBE_PID'"
    for _ in $(seq 1 15); do
        if remote "$client" "grep -q '^PONG' '$TCP_PROBE_OUT'" >/dev/null 2>&1; then
            printf 'old TCP probe connected endpoint=%s:%s output=%s\n' "$endpoint_host" "$SERVER_PORT" "$TCP_PROBE_OUT"
            return 0
        fi
        sleep 1
    done
    printf 'WARN: old TCP probe did not observe PONG before failover\n' >&2
    return 1
}

finish_tcp_probe() {
    [ "$TCP_PROBE" = 1 ] || return 0
    local client=$1
    remote "$client" "if test -s '$TCP_PROBE_PID'; then p=\$(cat '$TCP_PROBE_PID'); kill -TERM \$p 2>/dev/null || true; fi; sleep 1; cat '$TCP_PROBE_OUT'; grep -q '^PONG' '$TCP_PROBE_OUT' && grep -Eq 'Could not connect|Connection refused|Connection reset|I/O error' '$TCP_PROBE_OUT'"
}

cleanup() {
    local status=$?
    if [ "$TCP_PROBE" = 1 ]; then
        remote "$CLIENT_NODE" "if test -s '$TCP_PROBE_PID'; then p=\$(cat '$TCP_PROBE_PID'); kill -TERM \$p 2>/dev/null || true; fi" || true
    fi
    if [ "$KEEP_SERVERS" != 1 ]; then
        stop_redis 111 || true
        stop_redis 112 || true
    fi
    if [ "$SKIP_KEEPALIVED" != 1 ]; then
        stop_keepalived 111 || true
        stop_keepalived 112 || true
    fi
    if [ "$status" -ne 0 ]; then
        printf 'FAILED; remote artifacts remain under %s\n' "$REMOTE_DIR/benchmark/results/tlc_ha_aeron_failover/$RUN_ID" >&2
    fi
}
trap cleanup EXIT

HA_TERM=1
step "P1 sync and build"
if [ "$SKIP_SYNC" = 1 ]; then
    printf 'using existing remote Aeron binaries (TLC_HA_AERON_SKIP_SYNC=1)\n'
else
    sync_node 111
    sync_node 112
fi
for node in 111 112; do
    tx_path=$([ "$node" = 111 ] && printf '%s' "${TLC_HA_NODE111_TX_PATH:-/dev/obmm_shmdev4}" || printf '%s' "${TLC_HA_NODE112_TX_PATH:-/dev/obmm_shmdev9}")
    rx_path=$([ "$node" = 111 ] && printf '%s' "${TLC_HA_NODE111_RX_PATH:-/dev/obmm_shmdev13}" || printf '%s' "${TLC_HA_NODE112_RX_PATH:-/dev/obmm_shmdev8}")
    remote "$node" "test -x ./src/redis-server && test -x ./benchmark/vemb_v16_bench && test -x ./benchmark/vemb_v16_topology_ctl && test -x ./benchmark/tlc_ha_replica_ub_node_ut && test -r '$tx_path' && test -w '$tx_path' && test -r '$rx_path' && test -w '$rx_path'" || die "node $node preflight failed"
done

step "P2 stop stale services, clean VIP, and reset only Replica rings"
stop_redis 111 || true
stop_redis 112 || true
if [ "$SKIP_KEEPALIVED" != 1 ]; then stop_keepalived 111 || true; stop_keepalived 112 || true; fi
[ "$(vip_count)" = 0 ] || die "VIP remains after cleanup"
if [ "$RESET_REPLICA" = 1 ]; then
    reset_replica_rings
else
    printf 'skipping Replica ring reset (TLC_HA_AERON_RESET_REPLICA=0)\n'
fi
write_manifest 111
write_manifest 112
write_server_peer_map 111
write_server_peer_map 112
write_client_manifest "$CLIENT_NODE"

step "P3 start one leader and one follower in Aeron mode"
if [ "$INITIAL_NODE" = 111 ]; then
    HA_TERM=1; start_server 111 leader 111 112 "$NODE112_HOST"; start_server 112 follower 112 111 "$NODE111_HOST"
else
    HA_TERM=1; start_server 112 leader 112 111 "$NODE111_HOST"; start_server 111 follower 111 112 "$NODE112_HOST"
fi
for node in 111 112; do
    remote "$node" "for i in \$(seq 1 60); do ./benchmark/vemb_v16_topology_ctl --get --ctl-endpoint tcp --host '$(node_host "$node")' --port '$SERVER_PORT' --timeout-ms 1000 >/dev/null 2>&1 && exit 0; sleep 1; done; exit 1"
done

step "P4 publish topology with exactly one active owner"
EPOCH=$(( $(date +%s) + 100 ))
set_topology 111 "$([ "$INITIAL_NODE" = 111 ] && printf 0 || printf 1)" "$EPOCH"
set_topology 112 "$([ "$INITIAL_NODE" = 111 ] && printf 0 || printf 1)" "$EPOCH"
wait_ha_state "$INITIAL_NODE" LEADER MASTER
wait_ha_state "$([ "$INITIAL_NODE" = 111 ] && printf 112 || printf 111)" FOLLOWER BACKUP
if [ "$SKIP_KEEPALIVED" != 1 ]; then
    bash "$ROOT_DIR/scripts/deploy_keepalived_ha.sh" --all --vip "$VIP_CIDR" check
    bash "$ROOT_DIR/scripts/deploy_keepalived_ha.sh" --all --vip "$VIP_CIDR" start
    wait_vip_owner "$INITIAL_NODE"
fi

step "P5 initial CLI Aeron ATTACH and VADD"
run_client "$CLIENT_NODE" initial "$([ "$INITIAL_NODE" = 111 ] && printf 0 || printf 1)"
run_client_mode "$CLIENT_NODE" initial_handle "$([ "$INITIAL_NODE" = 111 ] && printf 0 || printf 1)" vemb-handle
start_tcp_probe "$CLIENT_NODE" "$(node_host "$INITIAL_NODE")"

step "P6 kill active Redis and wait for keepalived failover"
ACTIVE_PID_FILE="$(server_pid_file "$INITIAL_NODE")"
remote "$INITIAL_NODE" "p=\$(cat '$ACTIVE_PID_FILE'); kill -KILL \$p"
if [ "$SKIP_KEEPALIVED" != 1 ]; then
    FAILOVER_NODE=$([ "$INITIAL_NODE" = 111 ] && printf 112 || printf 111)
    wait_vip_owner "$FAILOVER_NODE"
    wait_ha_state "$FAILOVER_NODE" LEADER MASTER
else
    printf 'keepalived skipped; promoting standby through HA PROMOTE\n'
    FAILOVER_NODE=$([ "$INITIAL_NODE" = 111 ] && printf 112 || printf 111)
    remote "$FAILOVER_NODE" "redis-cli -h 127.0.0.1 -p '$SERVER_PORT' HA PROMOTE"
fi

step "P7 publish failover topology active=standby and re-ATTACH through VIP"
FAILOVER_OWNER=$([ "$FAILOVER_NODE" = 111 ] && printf 0 || printf 1)
FAILOVER_EPOCH=$((EPOCH + 1))
set_topology "$FAILOVER_NODE" "$FAILOVER_OWNER" "$FAILOVER_EPOCH"
run_client "$CLIENT_NODE" after_failover "$FAILOVER_OWNER"
run_client_mode "$CLIENT_NODE" after_failover_handle "$FAILOVER_OWNER" vemb-handle
finish_tcp_probe "$CLIENT_NODE"
remote "$FAILOVER_NODE" "grep -Eq 'aeron ATTACH ok|aeron ATTACH warm candidate' '$(server_log "$FAILOVER_NODE")'"

step "P8 restart old node as follower and verify recovery"
HA_TERM=2
OLD_NODE="$INITIAL_NODE"
OLD_OWNER=$([ "$OLD_NODE" = 111 ] && printf 0 || printf 1)
OLD_PEER=$([ "$OLD_NODE" = 111 ] && printf 112 || printf 111)
start_server "$OLD_NODE" follower "$([ "$OLD_NODE" = 111 ] && printf 111 || printf 112)" "$([ "$OLD_NODE" = 111 ] && printf 112 || printf 111)" "$(node_host "$OLD_PEER")"
remote "$OLD_NODE" "for i in \$(seq 1 60); do ./benchmark/vemb_v16_topology_ctl --get --ctl-endpoint tcp --host '$(node_host "$OLD_NODE")' --port '$SERVER_PORT' --timeout-ms 1000 >/dev/null 2>&1 && exit 0; sleep 1; done; exit 1"
set_topology "$OLD_NODE" "$FAILOVER_OWNER" "$FAILOVER_EPOCH"
wait_ha_state "$OLD_NODE" FOLLOWER BACKUP
wait_recovery_progress "$OLD_NODE" "$FAILOVER_NODE"
printf 'tlc_ha_aeron_failover_111_to_112: PASS active_owner=%s standby_owner=%s run=%s\n' "$FAILOVER_OWNER" "$OLD_OWNER" "$RUN_ID"
