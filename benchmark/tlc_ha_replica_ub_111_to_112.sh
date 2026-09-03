#!/usr/bin/env bash
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
NODE111_TX_PATH=$TLC_HA_NODE111_TX_PATH
NODE111_RX_PATH=$TLC_HA_NODE111_RX_PATH
NODE112_TX_PATH=$TLC_HA_NODE112_TX_PATH
NODE112_RX_PATH=$TLC_HA_NODE112_RX_PATH
TX_OFFSET=$TLC_HA_UB_TX_OFFSET
RX_OFFSET=$TLC_HA_UB_RX_OFFSET
NODE111_COLD_DIR=${TLC_HA_NODE111_COLD_DIR:-/tmp/tlc-ha-ub-node111-$$}
NODE112_COLD_DIR=${TLC_HA_NODE112_COLD_DIR:-/tmp/tlc-ha-ub-node112-$$}
FOLLOWER_LOG=${TLC_HA_FOLLOWER_LOG:-/tmp/tlc_ha_ub_follower.log}
LEADER_LOG=${TLC_HA_LEADER_LOG:-/tmp/tlc_ha_ub_leader.log}
TEST_TIMEOUT_SECONDS=${TLC_HA_TEST_TIMEOUT_SECONDS:-60}
START_DELAY_SECONDS=${TLC_HA_START_DELAY_SECONDS:-1}
VISIBILITY_OFFSET=${TLC_HA_VISIBILITY_OFFSET:-7516192768}
VISIBILITY_FRAME_BYTES=${TLC_HA_VISIBILITY_FRAME_BYTES:-4096}
VISIBILITY_ITERATIONS=${TLC_HA_VISIBILITY_ITERATIONS:-10000}
VISIBILITY_TIMEOUT_SECONDS=${TLC_HA_VISIBILITY_TIMEOUT_SECONDS:-30}
VISIBILITY_START_DELAY_SECONDS=${TLC_HA_VISIBILITY_START_DELAY_SECONDS:-1}
EXPECTED_EVENTS=${TLC_HA_EXPECTED_EVENTS:-10000}
REPLAY_START=${TLC_HA_REPLAY_START:-1}
REPLAY_END=${TLC_HA_REPLAY_END:-$EXPECTED_EVENTS}

if (( VISIBILITY_OFFSET % 64 != 0 )); then
    printf 'visibility offset must be 64-byte aligned: %s\n' "$VISIBILITY_OFFSET" >&2
    exit 1
fi
if (( TX_OFFSET == 0 || RX_OFFSET == 0 || TX_OFFSET % 64 != 0 || RX_OFFSET % 64 != 0 )); then
    printf 'Replica ring offsets must be non-zero and 64-byte aligned: tx=%s rx=%s\n' \
           "$TX_OFFSET" "$RX_OFFSET" >&2
    exit 1
fi
if (( TX_OFFSET == RX_OFFSET )); then
    printf 'Replica TX/RX offsets must be distinct: %s\n' "$TX_OFFSET" >&2
    exit 1
fi
if (( VISIBILITY_FRAME_BYTES < 192 || VISIBILITY_FRAME_BYTES % 64 != 0 )); then
    printf 'visibility frame bytes must be >=192 and 64-byte aligned: %s\n' \
           "$VISIBILITY_FRAME_BYTES" >&2
    exit 1
fi
if (( VISIBILITY_ITERATIONS == 0 || EXPECTED_EVENTS == 0 )); then
    printf 'visibility iterations and expected events must be non-zero\n' >&2
    exit 1
fi
if (( REPLAY_START == 0 || REPLAY_END < REPLAY_START || REPLAY_END > EXPECTED_EVENTS )); then
    printf 'invalid replay range: start=%s end=%s expected_events=%s\n' \
           "$REPLAY_START" "$REPLAY_END" "$EXPECTED_EVENTS" >&2
    exit 1
fi
if (( VISIBILITY_OFFSET == TX_OFFSET || VISIBILITY_OFFSET == RX_OFFSET )); then
    printf 'visibility offset overlaps a Replica ring offset: %s\n' "$VISIBILITY_OFFSET" >&2
    exit 1
fi

COMMON_FILES=(
    src/tlc_cold.c src/tlc_cold.h src/tlc_core.c src/tlc_core.h
    src/tlc_ha_replica.c src/tlc_ha_replica.h
    src/vemb_v16_mapped_region.c src/vemb_v16_mapped_region.h
    benchmark/Makefile benchmark/tlc_ha_replica_ub_node_ut.c
    benchmark/ub_cc_nc_visibility_ut.c
)

remote() {
    local port=$1 host=$2 command=$3
    ssh -p "$port" "$host" "cd '$REMOTE_DIR' && $command"
}

sync_node() {
    local port=$1 host=$2
    for file in "${COMMON_FILES[@]}"; do
        scp -P "$port" "$REPO_DIR/$file" "$host:$REMOTE_DIR/$file"
    done
    remote "$port" "$host" \
        "make -C benchmark tlc_ha_replica_ub_node_ut ub_cc_nc_visibility_ut"
}

check_environment() {
    local port=$1 host=$2 tx_path=$3 rx_path=$4
    remote "$port" "$host" \
        "command -v timeout >/dev/null && test -r '$tx_path' && test -w '$tx_path' && test -r '$rx_path' && test -w '$rx_path'"
}

check_node() {
    local port=$1 host=$2 cold_dir=$3
    remote "$port" "$host" \
        "test -x '$REMOTE_DIR/benchmark/tlc_ha_replica_ub_node_ut' && test -x '$REMOTE_DIR/benchmark/ub_cc_nc_visibility_ut' && mkdir -p '$cold_dir'"
}

run_visibility_direction() {
    local writer_port=$1 writer_host=$2 writer_data=$3 writer_ack=$4
    local reader_port=$5 reader_host=$6 reader_data=$7 reader_ack=$8
    local seed=$9 label=${10}
    local writer_log="/tmp/tlc_ha_visibility_${label}_writer.log"
    local reader_log="/tmp/tlc_ha_visibility_${label}_reader.log"
    local writer_command
    local reader_command

    writer_command="timeout '${VISIBILITY_TIMEOUT_SECONDS}s' ./benchmark/ub_cc_nc_visibility_ut frame-writer-nc --path '$writer_data' --ack-path '$writer_ack' --offset '$VISIBILITY_OFFSET' --seed '$seed' --frame-bytes '$VISIBILITY_FRAME_BYTES' --iterations '$VISIBILITY_ITERATIONS' --timeout-seconds '$VISIBILITY_TIMEOUT_SECONDS'"
    reader_command="timeout '${VISIBILITY_TIMEOUT_SECONDS}s' ./benchmark/ub_cc_nc_visibility_ut frame-reader-cc --path '$reader_data' --ack-path '$reader_ack' --offset '$VISIBILITY_OFFSET' --seed '$seed' --frame-bytes '$VISIBILITY_FRAME_BYTES' --iterations '$VISIBILITY_ITERATIONS' --timeout-seconds '$VISIBILITY_TIMEOUT_SECONDS'"

    printf 'visibility %s: writer=%s reader=%s offset=%s\n' \
           "$label" "$writer_data" "$reader_data" "$VISIBILITY_OFFSET"
    ssh -p "$writer_port" "$writer_host" \
        "cd '$REMOTE_DIR' && $writer_command" >"$writer_log" 2>&1 &
    local writer_pid=$!
    sleep "$VISIBILITY_START_DELAY_SECONDS"
    remote "$reader_port" "$reader_host" "$reader_command" >"$reader_log" 2>&1
    wait "$writer_pid"
    grep -q 'FRAME_WRITER_COMPLETE' "$writer_log"
    grep -q 'NOT_REPRODUCED' "$reader_log"
    cat "$writer_log"
    cat "$reader_log"
    printf 'visibility %s: PASS\n' "$label"
}

printf 'HA preflight: 111 Leader, 112 Follower\n'
check_environment "$NODE111_PORT" "$NODE111_SSH" "$NODE111_TX_PATH" "$NODE111_RX_PATH"
check_environment "$NODE112_PORT" "$NODE112_SSH" "$NODE112_TX_PATH" "$NODE112_RX_PATH"
sync_node "$NODE111_PORT" "$NODE111_SSH"
sync_node "$NODE112_PORT" "$NODE112_SSH"
check_node "$NODE111_PORT" "$NODE111_SSH" "$NODE111_COLD_DIR"
check_node "$NODE112_PORT" "$NODE112_SSH" "$NODE112_COLD_DIR"

run_visibility_direction \
    "$NODE111_PORT" "$NODE111_SSH" "$NODE111_TX_PATH" "$NODE111_RX_PATH" \
    "$NODE112_PORT" "$NODE112_SSH" "$NODE112_RX_PATH" "$NODE112_TX_PATH" \
    0x1111000000000000 111_to_112
run_visibility_direction \
    "$NODE112_PORT" "$NODE112_SSH" "$NODE112_TX_PATH" "$NODE112_RX_PATH" \
    "$NODE111_PORT" "$NODE111_SSH" "$NODE111_RX_PATH" "$NODE111_TX_PATH" \
    0x1122000000000000 112_to_111

reset_command="TLC_HA_UB_TX_PATH='$NODE111_TX_PATH' TLC_HA_UB_RX_PATH='$NODE111_RX_PATH' TLC_HA_UB_TX_OFFSET='$TX_OFFSET' TLC_HA_UB_RX_OFFSET='$RX_OFFSET' TLC_HA_UB_RESET=1 TLC_HA_UB_RESET_ONLY=1 ./benchmark/tlc_ha_replica_ub_node_ut follower"
remote "$NODE111_PORT" "$NODE111_SSH" "$reset_command"
reset_peer_command="TLC_HA_UB_TX_PATH='$NODE112_TX_PATH' TLC_HA_UB_RX_PATH='$NODE112_RX_PATH' TLC_HA_UB_TX_OFFSET='$RX_OFFSET' TLC_HA_UB_RX_OFFSET='$TX_OFFSET' TLC_HA_UB_RESET=1 TLC_HA_UB_RESET_ONLY=1 ./benchmark/tlc_ha_replica_ub_node_ut follower"
remote "$NODE112_PORT" "$NODE112_SSH" "$reset_peer_command"

follower_command="TLC_HA_UB_TX_PATH='$NODE112_TX_PATH' TLC_HA_UB_RX_PATH='$NODE112_RX_PATH' TLC_HA_UB_TX_OFFSET='$RX_OFFSET' TLC_HA_UB_RX_OFFSET='$TX_OFFSET' TLC_HA_UB_COLD_DIR='$NODE112_COLD_DIR' TLC_HA_UB_EXPECTED_EVENTS='$EXPECTED_EVENTS' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut follower"
leader_command="TLC_HA_UB_TX_PATH='$NODE111_TX_PATH' TLC_HA_UB_RX_PATH='$NODE111_RX_PATH' TLC_HA_UB_TX_OFFSET='$TX_OFFSET' TLC_HA_UB_RX_OFFSET='$RX_OFFSET' TLC_HA_UB_COLD_DIR='$NODE111_COLD_DIR' TLC_HA_UB_EXPECTED_EVENTS='$EXPECTED_EVENTS' TLC_HA_UB_REPLAY_START='$REPLAY_START' TLC_HA_UB_REPLAY_END='$REPLAY_END' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut leader"

ssh -p "$NODE112_PORT" "$NODE112_SSH" "cd '$REMOTE_DIR' && $follower_command" >"$FOLLOWER_LOG" 2>&1 &
follower_ssh_pid=$!
cleanup() {
    kill "$follower_ssh_pid" 2>/dev/null || true
    kill "${restart_follower_ssh_pid:-}" 2>/dev/null || true
}
trap cleanup EXIT
sleep "$START_DELAY_SECONDS"
remote "$NODE111_PORT" "$NODE111_SSH" "$leader_command" >"$LEADER_LOG" 2>&1
wait "$follower_ssh_pid"
grep -q "leader PASS events=${EXPECTED_EVENTS} accepted=" "$LEADER_LOG"
grep -q 'follower PASS applied' "$FOLLOWER_LOG"
cat "$LEADER_LOG"
cat "$FOLLOWER_LOG"
printf 'tlc_ha_replica_ub_111_to_112: PASS (visibility, replication, append ACK, heartbeat, async apply)\n'

recovery_command="TLC_HA_UB_COLD_DIR='$NODE112_COLD_DIR' TLC_HA_UB_EXPECTED_EVENTS='$EXPECTED_EVENTS' ./benchmark/tlc_ha_replica_ub_node_ut follower-recover"
remote "$NODE112_PORT" "$NODE112_SSH" "$recovery_command" >"$FOLLOWER_LOG.recovery" 2>&1
grep -q 'follower recovery PASS' "$FOLLOWER_LOG.recovery"
cat "$FOLLOWER_LOG.recovery"
printf 'tlc_ha_replica_ub_111_to_112: PASS (persistent follower COLD recovery)\n'

# Both runtimes are stopped at this point; rebuild the shared rings before
# attaching the restarted Follower and issuing a manual replay range.
remote "$NODE111_PORT" "$NODE111_SSH" "$reset_command"
remote "$NODE112_PORT" "$NODE112_SSH" "$reset_peer_command"
restart_follower_command="TLC_HA_UB_TX_PATH='$NODE112_TX_PATH' TLC_HA_UB_RX_PATH='$NODE112_RX_PATH' TLC_HA_UB_TX_OFFSET='$RX_OFFSET' TLC_HA_UB_RX_OFFSET='$TX_OFFSET' TLC_HA_UB_COLD_DIR='$NODE112_COLD_DIR' TLC_HA_UB_EXPECTED_EVENTS='$EXPECTED_EVENTS' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut follower"
replay_leader_command="TLC_HA_UB_TX_PATH='$NODE111_TX_PATH' TLC_HA_UB_RX_PATH='$NODE111_RX_PATH' TLC_HA_UB_TX_OFFSET='$TX_OFFSET' TLC_HA_UB_RX_OFFSET='$RX_OFFSET' TLC_HA_UB_COLD_DIR='$NODE111_COLD_DIR' TLC_HA_UB_EXPECTED_EVENTS='$EXPECTED_EVENTS' TLC_HA_UB_REPLAY_START='$REPLAY_START' TLC_HA_UB_REPLAY_END='$REPLAY_END' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut leader-replay"
ssh -p "$NODE112_PORT" "$NODE112_SSH" "cd '$REMOTE_DIR' && $restart_follower_command" >"$FOLLOWER_LOG.restart" 2>&1 &
restart_follower_ssh_pid=$!
sleep 0.1
remote "$NODE111_PORT" "$NODE111_SSH" "$replay_leader_command" >"$LEADER_LOG.replay" 2>&1
wait "$restart_follower_ssh_pid"
grep -q 'leader replay PASS' "$LEADER_LOG.replay"
grep -q 'follower PASS applied' "$FOLLOWER_LOG.restart"
cat "$LEADER_LOG.replay"
cat "$FOLLOWER_LOG.restart"
printf 'tlc_ha_replica_ub_111_to_112: PASS (manual AOF replay after follower restart)\n'
printf 'NOT_IMPLEMENTED: cross-node checkpoint resync, automatic reconnect, failover, redis-server TCP -> Replica -> Follower end-to-end\n'
