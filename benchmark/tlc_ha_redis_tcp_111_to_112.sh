#!/usr/bin/env bash
# Real Redis TCP/SDK HA regression: Leader writes, Follower serves reads.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
CONFIG_FILE=${TLC_HA_UB_CONFIG:-$REPO_DIR/examples/tlc_ha_replica_ub_111_to_112.env}
if [ ! -f "$CONFIG_FILE" ]; then
    printf 'HA UB config not found: %s\n' "$CONFIG_FILE" >&2
    exit 1
fi
# shellcheck disable=SC1090
. "$CONFIG_FILE"

: "${HA_REMOTE_DIR:?HA_REMOTE_DIR is required by the HA UB config}"
: "${HA_NODE111_SSH:?HA_NODE111_SSH is required by the HA UB config}"
: "${HA_NODE112_SSH:?HA_NODE112_SSH is required by the HA UB config}"
: "${HA_NODE111_PORT:?HA_NODE111_PORT is required by the HA UB config}"
: "${HA_NODE112_PORT:?HA_NODE112_PORT is required by the HA UB config}"
: "${TLC_HA_NODE111_TX_PATH:?TLC_HA_NODE111_TX_PATH is required by the HA UB config}"
: "${TLC_HA_NODE111_RX_PATH:?TLC_HA_NODE111_RX_PATH is required by the HA UB config}"
: "${TLC_HA_NODE112_TX_PATH:?TLC_HA_NODE112_TX_PATH is required by the HA UB config}"
: "${TLC_HA_NODE112_RX_PATH:?TLC_HA_NODE112_RX_PATH is required by the HA UB config}"
: "${TLC_HA_UB_TX_OFFSET:?TLC_HA_UB_TX_OFFSET is required by the HA UB config}"
: "${TLC_HA_UB_RX_OFFSET:?TLC_HA_UB_RX_OFFSET is required by the HA UB config}"

command -v ssh >/dev/null || { printf 'ssh is required\n' >&2; exit 1; }
command -v scp >/dev/null || { printf 'scp is required\n' >&2; exit 1; }

REMOTE_DIR=$HA_REMOTE_DIR
NODE111_SSH=$HA_NODE111_SSH
NODE112_SSH=$HA_NODE112_SSH
NODE111_PORT=$HA_NODE111_PORT
NODE112_PORT=$HA_NODE112_PORT
NODE111_HOST=${TLC_HA_NODE111_HOST:-192.168.90.111}
NODE112_HOST=${TLC_HA_NODE112_HOST:-192.168.90.112}
NODE111_TX_PATH=$TLC_HA_NODE111_TX_PATH
NODE111_RX_PATH=$TLC_HA_NODE111_RX_PATH
NODE112_TX_PATH=$TLC_HA_NODE112_TX_PATH
NODE112_RX_PATH=$TLC_HA_NODE112_RX_PATH
TX_OFFSET=$TLC_HA_UB_TX_OFFSET
RX_OFFSET=$TLC_HA_UB_RX_OFFSET
SERVER_PORT=${TLC_HA_REDIS_TCP_PORT:-6399}
DIM=${TLC_HA_REDIS_TCP_DIM:-16}
EVENT_COUNT=${TLC_HA_REDIS_TCP_EVENT_COUNT:-10000}
# The single local warm region reserves allocator metadata alongside payload;
# use 32K configured vectors so the effective writable capacity exceeds 10K.
MAX_VECTORS=${TLC_HA_REDIS_TCP_MAX_VECTORS:-32768}
RUN_ID=${TLC_HA_REDIS_TCP_RUN_ID:-$(date +%Y%m%d_%H%M%S)-$$}

if [[ ! $RUN_ID =~ ^[A-Za-z0-9_-]+$ ]]; then
    printf 'run id may contain only letters, digits, underscore, and hyphen: %s\n' \
           "$RUN_ID" >&2
    exit 1
fi
if (( SERVER_PORT == 0 || SERVER_PORT > 65535 || DIM == 0 ||
      EVENT_COUNT < 2 || MAX_VECTORS < EVENT_COUNT )); then
    printf 'invalid Redis port, dim, event count, or max vectors\n' >&2
    exit 1
fi
if (( TX_OFFSET == 0 || RX_OFFSET == 0 || TX_OFFSET % 64 != 0 ||
      RX_OFFSET % 64 != 0 || TX_OFFSET == RX_OFFSET )); then
    printf 'invalid Replica ring offsets: tx=%s rx=%s\n' "$TX_OFFSET" \
           "$RX_OFFSET" >&2
    exit 1
fi

REMOTE_RUN_DIR="$REMOTE_DIR/benchmark/results/tlc_ha_redis_tcp/$RUN_ID"
NODE111_MANIFEST="$REMOTE_RUN_DIR/node111.yaml"
NODE112_MANIFEST="$REMOTE_RUN_DIR/node112.yaml"
NODE111_COLD="$REMOTE_RUN_DIR/cold-node111"
NODE112_COLD="$REMOTE_RUN_DIR/cold-node112"
NODE111_PID="$REMOTE_RUN_DIR/leader.pid"
NODE112_PID="$REMOTE_RUN_DIR/follower.pid"
NODE111_LOG="$REMOTE_RUN_DIR/leader.log"
NODE112_LOG="$REMOTE_RUN_DIR/follower.log"
CLIENT_LOG="$REMOTE_RUN_DIR/sdk-write.log"
RECOVERY_LOG="$REMOTE_RUN_DIR/sdk-follower-recovery.log"
REGION_BYTES=$((DIM * 4 * MAX_VECTORS * 2))
EVENTS_TOTAL=$((EVENT_COUNT + 4))

SYNC_FILES=(
    src/Makefile
    src/tlc_cold.c src/tlc_cold.h src/tlc_core.c src/tlc_core.h
    src/tlc_ha_replica.c src/tlc_ha_replica.h
    src/vemb_v16_mapped_region.c src/vemb_v16_mapped_region.h
    src/vemb_v16_server_integration.c src/vemb_v16_storage.c
    src/vemb_v16_tlc.c src/vemb_v16_tlc.h
    benchmark/Makefile benchmark/tlc_ha_replica_ub_node_ut.c
    clients/c/Makefile clients/c/sdk_ha_replica_tcp.c
)

remote() {
    local port=$1 host=$2 command=$3
    ssh -p "$port" "$host" "cd '$REMOTE_DIR' && $command"
}

sync_node() {
    local port=$1 host=$2
    for file in "${SYNC_FILES[@]}"; do
        scp -P "$port" "$REPO_DIR/$file" "$host:$REMOTE_DIR/$file"
    done
    remote "$port" "$host" \
        "make -C src redis-server && make -C benchmark tlc_ha_replica_ub_node_ut && make -C clients/c ha-replica-tcp"
}

check_node() {
    local port=$1 host=$2 tx_path=$3 rx_path=$4
    remote "$port" "$host" \
        "test -x './src/redis-server' && test -x './benchmark/tlc_ha_replica_ub_node_ut' && test -x './clients/c/build/sdk_ha_replica_tcp' && test -r '$tx_path' && test -w '$tx_path' && test -r '$rx_path' && test -w '$rx_path'"
}

stop_server() {
    local port=$1 host=$2 pid_file=$3
    remote "$port" "$host" \
        "if test -s '$pid_file'; then server_pid=\$(cat '$pid_file'); kill -TERM \$server_pid 2>/dev/null || true; for _ in 1 2 3 4 5; do kill -0 \$server_pid 2>/dev/null || break; sleep 1; done; kill -KILL \$server_pid 2>/dev/null || true; fi" || true
}

wait_ready() {
    local port=$1 host=$2
    for _ in $(seq 1 60); do
        if remote "$port" "$host" \
            "bash -c '</dev/tcp/127.0.0.1/$SERVER_PORT'" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    return 1
}

write_manifest() {
    local port=$1 host=$2 manifest=$3 region_name=$4 node_id=$5
    remote "$port" "$host" \
        "printf '%s\\n' 'local_region_weight: 4' 'warm_regions:' '  - region_id: 0' '    provider: shm' '    path: $region_name' '    is_local: true' '    cache_policy: cacheable' '    mmap_offset: 0' '    bytes: $REGION_BYTES' '    value_size: $((DIM * 4))' '    home_ub_node_id: $node_id' > '$manifest'"
}

start_server() {
    local port=$1 host=$2 role=$3 node_id=$4 peer_id=$5 tx_path=$6 tx_offset=$7
    local rx_path=$8 rx_offset=$9 cold=${10} manifest=${11} pid_file=${12}
    local log_file=${13} advertise_host=${14}
    remote "$port" "$host" \
        "HPC_REDIS_COLD_DIR='$cold' HPC_REDIS_HA_ROLE='$role' HPC_REDIS_HA_NODE_ID='$node_id' HPC_REDIS_HA_PEER_NODE_ID='$peer_id' HPC_REDIS_HA_TERM=1 HPC_REDIS_HA_TX_PATH='$tx_path' HPC_REDIS_HA_TX_OFFSET='$tx_offset' HPC_REDIS_HA_RX_PATH='$rx_path' HPC_REDIS_HA_RX_OFFSET='$rx_offset' ./src/redis-server --port '$SERVER_PORT' --bind 0.0.0.0 --protected-mode no --save '' --appendonly no --vemb-v16-enabled yes --vemb-v16-dim '$DIM' --vemb-v16-max-vectors '$MAX_VECTORS' --vemb-v16-warm-regions-manifest '$manifest' --vemb-v16-reset-warm-regions yes --vemb-v16-transport sniff --vemb-v16-tcp-host '$advertise_host' --vemb-v16-proxy-io-threads 1 --vemb-v16-supernode-workers 1 --daemonize yes --pidfile '$pid_file' --logfile '$log_file' --loglevel notice"
}

reset_rings() {
    remote "$NODE111_PORT" "$NODE111_SSH" \
        "TLC_HA_UB_TX_PATH='$NODE111_TX_PATH' TLC_HA_UB_RX_PATH='$NODE111_RX_PATH' TLC_HA_UB_TX_OFFSET='$TX_OFFSET' TLC_HA_UB_RX_OFFSET='$RX_OFFSET' TLC_HA_UB_RESET_ONLY=1 ./benchmark/tlc_ha_replica_ub_node_ut follower"
    remote "$NODE112_PORT" "$NODE112_SSH" \
        "TLC_HA_UB_TX_PATH='$NODE112_TX_PATH' TLC_HA_UB_RX_PATH='$NODE112_RX_PATH' TLC_HA_UB_TX_OFFSET='$RX_OFFSET' TLC_HA_UB_RX_OFFSET='$TX_OFFSET' TLC_HA_UB_RESET_ONLY=1 ./benchmark/tlc_ha_replica_ub_node_ut follower"
}

cleanup() {
    stop_server "$NODE111_PORT" "$NODE111_SSH" "$NODE111_PID"
    stop_server "$NODE112_PORT" "$NODE112_SSH" "$NODE112_PID"
}
trap cleanup EXIT

printf '== sync and build ==\n'
if [ "${TLC_HA_REDIS_TCP_SKIP_SYNC:-0}" != 1 ]; then
    sync_node "$NODE111_PORT" "$NODE111_SSH"
    sync_node "$NODE112_PORT" "$NODE112_SSH"
else
    printf 'using existing remote binaries (TLC_HA_REDIS_TCP_SKIP_SYNC=1)\n'
fi
check_node "$NODE111_PORT" "$NODE111_SSH" "$NODE111_TX_PATH" "$NODE111_RX_PATH"
check_node "$NODE112_PORT" "$NODE112_SSH" "$NODE112_TX_PATH" "$NODE112_RX_PATH"

for endpoint in "$NODE111_PORT:$NODE111_SSH" "$NODE112_PORT:$NODE112_SSH"; do
    endpoint_port=${endpoint%%:*}
    endpoint_host=${endpoint#*:}
    remote "$endpoint_port" "$endpoint_host" \
        "test ! -e '$REMOTE_RUN_DIR' && mkdir -p '$REMOTE_RUN_DIR' '$NODE111_COLD' '$NODE112_COLD'"
done
write_manifest "$NODE111_PORT" "$NODE111_SSH" "$NODE111_MANIFEST" \
               "/tlc_ha_redis_tcp_111_$RUN_ID" 111
write_manifest "$NODE112_PORT" "$NODE112_SSH" "$NODE112_MANIFEST" \
               "/tlc_ha_redis_tcp_112_$RUN_ID" 112

printf '== reset Replica rings and start Follower ==\n'
reset_rings
start_server "$NODE112_PORT" "$NODE112_SSH" follower 112 111 \
             "$NODE112_TX_PATH" "$RX_OFFSET" "$NODE112_RX_PATH" "$TX_OFFSET" \
             "$NODE112_COLD" "$NODE112_MANIFEST" "$NODE112_PID" "$NODE112_LOG" \
             "$NODE112_HOST"
wait_ready "$NODE112_PORT" "$NODE112_SSH" || {
    remote "$NODE112_PORT" "$NODE112_SSH" "cat '$NODE112_LOG'" >&2
    exit 1
}

printf '== start Leader and write through TCP SDK ==\n'
start_server "$NODE111_PORT" "$NODE111_SSH" leader 111 112 \
             "$NODE111_TX_PATH" "$TX_OFFSET" "$NODE111_RX_PATH" "$RX_OFFSET" \
             "$NODE111_COLD" "$NODE111_MANIFEST" "$NODE111_PID" "$NODE111_LOG" \
             "$NODE111_HOST"
wait_ready "$NODE111_PORT" "$NODE111_SSH" || {
    remote "$NODE111_PORT" "$NODE111_SSH" "cat '$NODE111_LOG'" >&2
    exit 1
}
remote "$NODE111_PORT" "$NODE111_SSH" \
    "./clients/c/build/sdk_ha_replica_tcp write '$NODE111_HOST' '$SERVER_PORT' '$NODE112_HOST' '$SERVER_PORT' '$DIM' '$EVENT_COUNT' >'$CLIENT_LOG' 2>&1"
remote "$NODE111_PORT" "$NODE111_SSH" \
    "grep -q 'sdk_ha_replica_tcp: PASS events=$EVENTS_TOTAL' '$CLIENT_LOG'"
sleep 2

remote "$NODE111_PORT" "$NODE111_SSH" \
    "grep -q 'HA Replica started: role=leader' '$NODE111_LOG' && grep -Eq 'HA Replica progress: role=.*appended=$EVENTS_TOTAL.*peer_accepted=$EVENTS_TOTAL.*peer_durable=$EVENTS_TOTAL.*peer_applied=$EVENTS_TOTAL.*peer_health=1' '$NODE111_LOG'"
remote "$NODE112_PORT" "$NODE112_SSH" \
    "grep -q 'HA Replica started: role=follower' '$NODE112_LOG' && grep -Eq 'HA Replica progress: role=.*appended=$EVENTS_TOTAL.*durable=$EVENTS_TOTAL.*applied=$EVENTS_TOTAL.*peer_health=1' '$NODE112_LOG'"
remote "$NODE111_PORT" "$NODE111_SSH" "cat '$CLIENT_LOG'; tail -20 '$NODE111_LOG'"
remote "$NODE112_PORT" "$NODE112_SSH" "tail -20 '$NODE112_LOG'"
printf 'PASS: Redis TCP SDK -> Leader -> Replica UB -> Follower TCP SDK (%s events)\n' \
       "$EVENTS_TOTAL"

printf '== restart Follower from the same COLD directory ==\n'
stop_server "$NODE111_PORT" "$NODE111_SSH" "$NODE111_PID"
stop_server "$NODE112_PORT" "$NODE112_SSH" "$NODE112_PID"
reset_rings
start_server "$NODE112_PORT" "$NODE112_SSH" follower 112 111 \
             "$NODE112_TX_PATH" "$RX_OFFSET" "$NODE112_RX_PATH" "$TX_OFFSET" \
             "$NODE112_COLD" "$NODE112_MANIFEST" "$NODE112_PID" "$NODE112_LOG" \
             "$NODE112_HOST"
wait_ready "$NODE112_PORT" "$NODE112_SSH" || {
    remote "$NODE112_PORT" "$NODE112_SSH" "cat '$NODE112_LOG'" >&2
    exit 1
}
remote "$NODE111_PORT" "$NODE111_SSH" \
    "./clients/c/build/sdk_ha_replica_tcp verify '$NODE112_HOST' '$SERVER_PORT' '$DIM' '$EVENT_COUNT' >'$RECOVERY_LOG' 2>&1"
remote "$NODE111_PORT" "$NODE111_SSH" \
    "grep -q 'sdk_ha_replica_tcp: VERIFY PASS events=$EVENTS_TOTAL' '$RECOVERY_LOG'"
remote "$NODE111_PORT" "$NODE111_SSH" "cat '$RECOVERY_LOG'"
printf 'PASS: Follower COLD recovery after reset WARM region\n'
printf 'artifacts: %s (on 111 and 112)\n' "$REMOTE_RUN_DIR"
