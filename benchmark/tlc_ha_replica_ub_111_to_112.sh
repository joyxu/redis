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
NODE111_HOST=${TLC_HA_NODE111_HOST:-192.168.90.111}
NODE112_HOST=${TLC_HA_NODE112_HOST:-192.168.90.112}
CONTROL_PORT=${TLC_HA_UB_CONTROL_PORT:-9736}
NODE111_COLD_DIR=${TLC_HA_NODE111_COLD_DIR:-/tmp/tlc-ha-ub-node111-$$}
NODE112_COLD_DIR=${TLC_HA_NODE112_COLD_DIR:-/tmp/tlc-ha-ub-node112-$$}
FOLLOWER_LOG=${TLC_HA_FOLLOWER_LOG:-/tmp/tlc_ha_ub_follower.log}
LEADER_LOG=${TLC_HA_LEADER_LOG:-/tmp/tlc_ha_ub_leader.log}
TEST_TIMEOUT_SECONDS=${TLC_HA_TEST_TIMEOUT_SECONDS:-60}
RESTART_REPLAY_TIMEOUT_SECONDS=${TLC_HA_RESTART_REPLAY_TIMEOUT_SECONDS:-120}
START_DELAY_SECONDS=${TLC_HA_START_DELAY_SECONDS:-1}
VISIBILITY_OFFSET=${TLC_HA_VISIBILITY_OFFSET:-7516192768}
VISIBILITY_FRAME_BYTES=${TLC_HA_VISIBILITY_FRAME_BYTES:-4096}
VISIBILITY_ITERATIONS=${TLC_HA_VISIBILITY_ITERATIONS:-10000}
VISIBILITY_TIMEOUT_SECONDS=${TLC_HA_VISIBILITY_TIMEOUT_SECONDS:-30}
VISIBILITY_START_DELAY_SECONDS=${TLC_HA_VISIBILITY_START_DELAY_SECONDS:-1}
EXPECTED_EVENTS=${TLC_HA_EXPECTED_EVENTS:-10000}
REPLAY_START=${TLC_HA_REPLAY_START:-1}
REPLAY_END=${TLC_HA_REPLAY_END:-$EXPECTED_EVENTS}
M7_ONLY=${TLC_HA_UB_M7_ONLY:-0}

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
if (( TEST_TIMEOUT_SECONDS == 0 || RESTART_REPLAY_TIMEOUT_SECONDS == 0 )); then
    printf 'test timeouts must be non-zero: test=%s restart_replay=%s\n' \
           "$TEST_TIMEOUT_SECONDS" "$RESTART_REPLAY_TIMEOUT_SECONDS" >&2
    exit 1
fi
if (( REPLAY_START == 0 || REPLAY_END < REPLAY_START || REPLAY_END > EXPECTED_EVENTS )); then
    printf 'invalid replay range: start=%s end=%s expected_events=%s\n' \
           "$REPLAY_START" "$REPLAY_END" "$EXPECTED_EVENTS" >&2
    exit 1
fi
RESTART_REPLAY_TIMEOUT_MS=$((RESTART_REPLAY_TIMEOUT_SECONDS * 1000))
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
        "make -B -C benchmark tlc_ha_replica_ub_node_ut ub_cc_nc_visibility_ut"
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

reset_command="TLC_HA_UB_TX_PATH='$NODE111_TX_PATH' TLC_HA_UB_RX_PATH='$NODE111_RX_PATH' TLC_HA_UB_TX_OFFSET='$TX_OFFSET' TLC_HA_UB_RX_OFFSET='$RX_OFFSET' TLC_HA_UB_RESET=1 TLC_HA_UB_RESET_ONLY=1 ./benchmark/tlc_ha_replica_ub_node_ut follower"
reset_peer_command="TLC_HA_UB_TX_PATH='$NODE112_TX_PATH' TLC_HA_UB_RX_PATH='$NODE112_RX_PATH' TLC_HA_UB_TX_OFFSET='$RX_OFFSET' TLC_HA_UB_RX_OFFSET='$TX_OFFSET' TLC_HA_UB_RESET=1 TLC_HA_UB_RESET_ONLY=1 ./benchmark/tlc_ha_replica_ub_node_ut follower"

# Control plane env: Follower listens, Leader connects to Follower's host.
CTRL_LEADER="TLC_HA_UB_CONTROL_PORT='$CONTROL_PORT' TLC_HA_UB_BIND_HOST='${TLC_HA_UB_NODE111_BIND_HOST:-$NODE111_HOST}' TLC_HA_UB_PEER_HOST='$NODE112_HOST'"
CTRL_FOLLOWER="TLC_HA_UB_CONTROL_PORT='$CONTROL_PORT' TLC_HA_UB_BIND_HOST='${TLC_HA_UB_NODE112_BIND_HOST:-$NODE112_HOST}' TLC_HA_UB_PEER_HOST='$NODE111_HOST'"

cleanup() {
    kill "${follower_ssh_pid:-}" 2>/dev/null || true
    kill "${restart_follower_ssh_pid:-}" 2>/dev/null || true
    kill "${m5_follower_ssh_pid:-}" 2>/dev/null || true
    kill "${m5_abort_follower_ssh_pid:-}" 2>/dev/null || true
    kill "${m6_follower_ssh_pid:-}" 2>/dev/null || true
    kill "${m6_retention_follower_ssh_pid:-}" 2>/dev/null || true
    kill "${m7_retention_follower_ssh_pid:-}" 2>/dev/null || true
    kill "${m7_pressure_follower_ssh_pid:-}" 2>/dev/null || true
}
trap cleanup EXIT

run_m7_tests() {
    remote "$NODE111_PORT" "$NODE111_SSH" "$reset_command"
    remote "$NODE112_PORT" "$NODE112_SSH" "$reset_peer_command"
    m7_retention_follower_command="$CTRL_FOLLOWER TLC_HA_UB_RETENTION_EVENTS=8 TLC_HA_UB_TX_PATH='$NODE112_TX_PATH' TLC_HA_UB_RX_PATH='$NODE112_RX_PATH' TLC_HA_UB_TX_OFFSET='$RX_OFFSET' TLC_HA_UB_RX_OFFSET='$TX_OFFSET' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut follower-m7-retention"
    m7_retention_leader_command="$CTRL_LEADER TLC_HA_UB_RETENTION_EVENTS=8 TLC_HA_UB_TX_PATH='$NODE111_TX_PATH' TLC_HA_UB_RX_PATH='$NODE111_RX_PATH' TLC_HA_UB_TX_OFFSET='$TX_OFFSET' TLC_HA_UB_RX_OFFSET='$RX_OFFSET' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut leader-m7-retention"
    ssh -p "$NODE112_PORT" "$NODE112_SSH" "cd '$REMOTE_DIR' && $m7_retention_follower_command" >"$FOLLOWER_LOG.m7-retention" 2>&1 &
    m7_retention_follower_ssh_pid=$!
    sleep "$START_DELAY_SECONDS"
    remote "$NODE111_PORT" "$NODE111_SSH" "$m7_retention_leader_command" >"$LEADER_LOG.m7-retention" 2>&1
    wait "$m7_retention_follower_ssh_pid"
    grep -q 'HA Replica retention compacted:' "$LEADER_LOG.m7-retention"
    grep -q 'leader M7 retention PASS R=8 C=16' "$LEADER_LOG.m7-retention"
    grep -q 'follower M7 retention PASS durable=8' "$FOLLOWER_LOG.m7-retention"
    cat "$LEADER_LOG.m7-retention"
    cat "$FOLLOWER_LOG.m7-retention"
    printf 'tlc_ha_replica_ub_111_to_112: PASS (M7 retention soft compact)\n'

    remote "$NODE111_PORT" "$NODE111_SSH" "$reset_command"
    remote "$NODE112_PORT" "$NODE112_SSH" "$reset_peer_command"
    m7_pressure_follower_command="$CTRL_FOLLOWER TLC_HA_UB_RETENTION_EVENTS=8 TLC_HA_UB_TX_PATH='$NODE112_TX_PATH' TLC_HA_UB_RX_PATH='$NODE112_RX_PATH' TLC_HA_UB_TX_OFFSET='$RX_OFFSET' TLC_HA_UB_RX_OFFSET='$TX_OFFSET' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut follower-m7-pressure-stall"
    m7_pressure_leader_command="$CTRL_LEADER TLC_HA_UB_RETENTION_EVENTS=8 TLC_HA_UB_TX_PATH='$NODE111_TX_PATH' TLC_HA_UB_RX_PATH='$NODE111_RX_PATH' TLC_HA_UB_TX_OFFSET='$TX_OFFSET' TLC_HA_UB_RX_OFFSET='$RX_OFFSET' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut leader-m7-pressure"
    ssh -p "$NODE112_PORT" "$NODE112_SSH" "cd '$REMOTE_DIR' && $m7_pressure_follower_command" >"$FOLLOWER_LOG.m7-pressure" 2>&1 &
    m7_pressure_follower_ssh_pid=$!
    sleep "$START_DELAY_SECONDS"
    remote "$NODE111_PORT" "$NODE111_SSH" "$m7_pressure_leader_command" >"$LEADER_LOG.m7-pressure" 2>&1
    wait "$m7_pressure_follower_ssh_pid"
    grep -q 'HA Replica retention pressure aborts pinned resync:' "$LEADER_LOG.m7-pressure"
    grep -q 'leader M7 pressure PASS R=8 C=16' "$LEADER_LOG.m7-pressure"
    grep -q 'follower M7 pressure stall PASS' "$FOLLOWER_LOG.m7-pressure"
    cat "$LEADER_LOG.m7-pressure"
    cat "$FOLLOWER_LOG.m7-pressure"
    printf 'tlc_ha_replica_ub_111_to_112: PASS (M7 retention hard pressure abort)\n'
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

if (( M7_ONLY != 0 )); then
    run_m7_tests
    exit 0
fi

run_visibility_direction \
    "$NODE111_PORT" "$NODE111_SSH" "$NODE111_TX_PATH" "$NODE111_RX_PATH" \
    "$NODE112_PORT" "$NODE112_SSH" "$NODE112_RX_PATH" "$NODE112_TX_PATH" \
    0x1111000000000000 111_to_112
run_visibility_direction \
    "$NODE112_PORT" "$NODE112_SSH" "$NODE112_TX_PATH" "$NODE112_RX_PATH" \
    "$NODE111_PORT" "$NODE111_SSH" "$NODE111_RX_PATH" "$NODE111_TX_PATH" \
    0x1122000000000000 112_to_111

remote "$NODE111_PORT" "$NODE111_SSH" "$reset_command"
remote "$NODE112_PORT" "$NODE112_SSH" "$reset_peer_command"

follower_command="$CTRL_FOLLOWER TLC_HA_UB_TX_PATH='$NODE112_TX_PATH' TLC_HA_UB_RX_PATH='$NODE112_RX_PATH' TLC_HA_UB_TX_OFFSET='$RX_OFFSET' TLC_HA_UB_RX_OFFSET='$TX_OFFSET' TLC_HA_UB_COLD_DIR='$NODE112_COLD_DIR' TLC_HA_UB_EXPECTED_EVENTS='$EXPECTED_EVENTS' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut follower"
leader_command="$CTRL_LEADER TLC_HA_UB_TX_PATH='$NODE111_TX_PATH' TLC_HA_UB_RX_PATH='$NODE111_RX_PATH' TLC_HA_UB_TX_OFFSET='$TX_OFFSET' TLC_HA_UB_RX_OFFSET='$RX_OFFSET' TLC_HA_UB_COLD_DIR='$NODE111_COLD_DIR' TLC_HA_UB_EXPECTED_EVENTS='$EXPECTED_EVENTS' TLC_HA_UB_REPLAY_START='$REPLAY_START' TLC_HA_UB_REPLAY_END='$REPLAY_END' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut leader"

ssh -p "$NODE112_PORT" "$NODE112_SSH" "cd '$REMOTE_DIR' && $follower_command" >"$FOLLOWER_LOG" 2>&1 &
follower_ssh_pid=$!
sleep "$START_DELAY_SECONDS"
remote "$NODE111_PORT" "$NODE111_SSH" "$leader_command" >"$LEADER_LOG" 2>&1
wait "$follower_ssh_pid"
grep -q "leader PASS events=${EXPECTED_EVENTS} accepted=" "$LEADER_LOG"
grep -q 'follower PASS applied' "$FOLLOWER_LOG"
cat "$LEADER_LOG"
cat "$FOLLOWER_LOG"
printf 'tlc_ha_replica_ub_111_to_112: PASS (visibility, replication, append ACK, heartbeat, async apply)\n'

# Snapshot control frames use a fresh pair of rings so normal EVENTS cannot
# interleave with this artifact-only transport check.
remote "$NODE111_PORT" "$NODE111_SSH" "$reset_command"
remote "$NODE112_PORT" "$NODE112_SSH" "$reset_peer_command"
snapshot_follower_command="$CTRL_FOLLOWER TLC_HA_UB_TX_PATH='$NODE112_TX_PATH' TLC_HA_UB_RX_PATH='$NODE112_RX_PATH' TLC_HA_UB_TX_OFFSET='$RX_OFFSET' TLC_HA_UB_RX_OFFSET='$TX_OFFSET' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut follower-snapshot"
snapshot_leader_command="$CTRL_LEADER TLC_HA_UB_TX_PATH='$NODE111_TX_PATH' TLC_HA_UB_RX_PATH='$NODE111_RX_PATH' TLC_HA_UB_TX_OFFSET='$TX_OFFSET' TLC_HA_UB_RX_OFFSET='$RX_OFFSET' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut leader-snapshot"
ssh -p "$NODE112_PORT" "$NODE112_SSH" "cd '$REMOTE_DIR' && $snapshot_follower_command" >"$FOLLOWER_LOG.snapshot" 2>&1 &
snapshot_follower_ssh_pid=$!
sleep "$START_DELAY_SECONDS"
remote "$NODE111_PORT" "$NODE111_SSH" "$snapshot_leader_command" >"$LEADER_LOG.snapshot" 2>&1
wait "$snapshot_follower_ssh_pid"
grep -q 'leader snapshot PASS bytes=200000' "$LEADER_LOG.snapshot"
grep -q 'follower snapshot PASS bytes=200000' "$FOLLOWER_LOG.snapshot"
cat "$LEADER_LOG.snapshot"
cat "$FOLLOWER_LOG.snapshot"
printf 'tlc_ha_replica_ub_111_to_112: PASS (111 -> 112 snapshot chunks)\n'

# A real checkpoint artifact follows the same transport but Follower also
# enters the lock-free FENCED/quiesce path and installs it in place.
remote "$NODE111_PORT" "$NODE111_SSH" "$reset_command"
remote "$NODE112_PORT" "$NODE112_SSH" "$reset_peer_command"
install_follower_command="$CTRL_FOLLOWER TLC_HA_UB_TX_PATH='$NODE112_TX_PATH' TLC_HA_UB_RX_PATH='$NODE112_RX_PATH' TLC_HA_UB_TX_OFFSET='$RX_OFFSET' TLC_HA_UB_RX_OFFSET='$TX_OFFSET' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut follower-install"
install_leader_command="$CTRL_LEADER TLC_HA_UB_TX_PATH='$NODE111_TX_PATH' TLC_HA_UB_RX_PATH='$NODE111_RX_PATH' TLC_HA_UB_TX_OFFSET='$TX_OFFSET' TLC_HA_UB_RX_OFFSET='$RX_OFFSET' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut leader-install"
ssh -p "$NODE112_PORT" "$NODE112_SSH" "cd '$REMOTE_DIR' && $install_follower_command" >"$FOLLOWER_LOG.install" 2>&1 &
install_follower_ssh_pid=$!
sleep "$START_DELAY_SECONDS"
remote "$NODE111_PORT" "$NODE111_SSH" "$install_leader_command" >"$LEADER_LOG.install" 2>&1
wait "$install_follower_ssh_pid"
grep -q 'leader install PASS generation=1' "$LEADER_LOG.install"
grep -q 'follower install PASS generation=1' "$FOLLOWER_LOG.install"
cat "$LEADER_LOG.install"
cat "$FOLLOWER_LOG.install"
printf 'tlc_ha_replica_ub_111_to_112: PASS (111 -> 112 checkpoint install)\n'

# M5 exercises the controlled session over the real cross-node UB rings:
# checkpoint tail, boundary extension, final handoff ACK, then H + 1 replay.
remote "$NODE111_PORT" "$NODE111_SSH" "$reset_command"
remote "$NODE112_PORT" "$NODE112_SSH" "$reset_peer_command"
m5_follower_command="$CTRL_FOLLOWER TLC_HA_UB_TX_PATH='$NODE112_TX_PATH' TLC_HA_UB_RX_PATH='$NODE112_RX_PATH' TLC_HA_UB_TX_OFFSET='$RX_OFFSET' TLC_HA_UB_RX_OFFSET='$TX_OFFSET' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut follower-m5"
m5_leader_command="$CTRL_LEADER TLC_HA_UB_TX_PATH='$NODE111_TX_PATH' TLC_HA_UB_RX_PATH='$NODE111_RX_PATH' TLC_HA_UB_TX_OFFSET='$TX_OFFSET' TLC_HA_UB_RX_OFFSET='$RX_OFFSET' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut leader-m5"
ssh -p "$NODE112_PORT" "$NODE112_SSH" "cd '$REMOTE_DIR' && $m5_follower_command" >"$FOLLOWER_LOG.m5" 2>&1 &
m5_follower_ssh_pid=$!
sleep "$START_DELAY_SECONDS"
remote "$NODE111_PORT" "$NODE111_SSH" "$m5_leader_command" >"$LEADER_LOG.m5" 2>&1
wait "$m5_follower_ssh_pid"
grep -q 'leader M5 PASS H=3 accepted=' "$LEADER_LOG.m5"
grep -q 'follower M5 PASS durable=' "$FOLLOWER_LOG.m5"
cat "$LEADER_LOG.m5"
cat "$FOLLOWER_LOG.m5"
printf 'tlc_ha_replica_ub_111_to_112: PASS (M5 resync tail, handoff, H + 1)\n'

# M5 timeout and explicit abort must release the Leader pin and delete the
# Follower artifact while keeping normal emission fenced.
remote "$NODE111_PORT" "$NODE111_SSH" "$reset_command"
remote "$NODE112_PORT" "$NODE112_SSH" "$reset_peer_command"
m5_abort_follower_command="$CTRL_FOLLOWER TLC_HA_UB_TX_PATH='$NODE112_TX_PATH' TLC_HA_UB_RX_PATH='$NODE112_RX_PATH' TLC_HA_UB_TX_OFFSET='$RX_OFFSET' TLC_HA_UB_RX_OFFSET='$TX_OFFSET' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut follower-m5-abort"
m5_abort_leader_command="$CTRL_LEADER TLC_HA_UB_TX_PATH='$NODE111_TX_PATH' TLC_HA_UB_RX_PATH='$NODE111_RX_PATH' TLC_HA_UB_TX_OFFSET='$TX_OFFSET' TLC_HA_UB_RX_OFFSET='$RX_OFFSET' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut leader-m5-abort"
ssh -p "$NODE112_PORT" "$NODE112_SSH" "cd '$REMOTE_DIR' && $m5_abort_follower_command" >"$FOLLOWER_LOG.m5-abort" 2>&1 &
m5_abort_follower_ssh_pid=$!
sleep "$START_DELAY_SECONDS"
remote "$NODE111_PORT" "$NODE111_SSH" "$m5_abort_leader_command" >"$LEADER_LOG.m5-abort" 2>&1
wait "$m5_abort_follower_ssh_pid"
grep -q 'leader M5 abort PASS' "$LEADER_LOG.m5-abort"
grep -q 'follower M5 abort PASS' "$FOLLOWER_LOG.m5-abort"
cat "$LEADER_LOG.m5-abort"
cat "$FOLLOWER_LOG.m5-abort"
printf 'tlc_ha_replica_ub_111_to_112: PASS (M5 timeout and abort cleanup)\n'

# M6 verifies retained-AOF GAP repair. The Leader persists seq 1 before its
# sender starts, then emits only seq 2. No checkpoint is published: the empty
# Follower must request and accept AOF [1,2], then accept normal seq 3.
remote "$NODE111_PORT" "$NODE111_SSH" "$reset_command"
remote "$NODE112_PORT" "$NODE112_SSH" "$reset_peer_command"
m6_follower_command="$CTRL_FOLLOWER TLC_HA_UB_TX_PATH='$NODE112_TX_PATH' TLC_HA_UB_RX_PATH='$NODE112_RX_PATH' TLC_HA_UB_TX_OFFSET='$RX_OFFSET' TLC_HA_UB_RX_OFFSET='$TX_OFFSET' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut follower-m6-gap"
m6_leader_command="$CTRL_LEADER TLC_HA_UB_TX_PATH='$NODE111_TX_PATH' TLC_HA_UB_RX_PATH='$NODE111_RX_PATH' TLC_HA_UB_TX_OFFSET='$TX_OFFSET' TLC_HA_UB_RX_OFFSET='$RX_OFFSET' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut leader-m6-gap"
ssh -p "$NODE112_PORT" "$NODE112_SSH" "cd '$REMOTE_DIR' && $m6_follower_command" >"$FOLLOWER_LOG.m6" 2>&1 &
m6_follower_ssh_pid=$!
sleep "$START_DELAY_SECONDS"
remote "$NODE111_PORT" "$NODE111_SSH" "$m6_leader_command" >"$LEADER_LOG.m6" 2>&1
wait "$m6_follower_ssh_pid"
grep -q 'leader M6 AOF GAP PASS accepted=3' "$LEADER_LOG.m6"
grep -q 'follower M6 AOF GAP PASS durable=3' "$FOLLOWER_LOG.m6"
cat "$LEADER_LOG.m6"
cat "$FOLLOWER_LOG.m6"
printf 'tlc_ha_replica_ub_111_to_112: PASS (M6 automatic GAP -> AOF repair)\n'

# M6 retention verifies that an automatic GAP does not hide a missing AOF
# prefix with an ordinary replay: Leader compacts through checkpoint 96, then
# Follower receives seq 98 and must complete automatic snapshot + tail [97,98].
remote "$NODE111_PORT" "$NODE111_SSH" "$reset_command"
remote "$NODE112_PORT" "$NODE112_SSH" "$reset_peer_command"
m6_retention_follower_command="$CTRL_FOLLOWER TLC_HA_UB_TX_PATH='$NODE112_TX_PATH' TLC_HA_UB_RX_PATH='$NODE112_RX_PATH' TLC_HA_UB_TX_OFFSET='$RX_OFFSET' TLC_HA_UB_RX_OFFSET='$TX_OFFSET' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut follower-m6-retention"
m6_retention_leader_command="$CTRL_LEADER TLC_HA_UB_TX_PATH='$NODE111_TX_PATH' TLC_HA_UB_RX_PATH='$NODE111_RX_PATH' TLC_HA_UB_TX_OFFSET='$TX_OFFSET' TLC_HA_UB_RX_OFFSET='$RX_OFFSET' timeout '${TEST_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut leader-m6-retention"
ssh -p "$NODE112_PORT" "$NODE112_SSH" "cd '$REMOTE_DIR' && $m6_retention_follower_command" >"$FOLLOWER_LOG.m6-retention" 2>&1 &
m6_retention_follower_ssh_pid=$!
sleep "$START_DELAY_SECONDS"
remote "$NODE111_PORT" "$NODE111_SSH" "$m6_retention_leader_command" >"$LEADER_LOG.m6-retention" 2>&1
wait "$m6_retention_follower_ssh_pid"
grep -q 'HA Replica AOF retention missing' "$LEADER_LOG.m6-retention"
grep -q 'HA resync handoff complete:.*H=98' "$LEADER_LOG.m6-retention"
grep -q 'leader M6 retention PASS accepted=98' "$LEADER_LOG.m6-retention"
grep -q 'follower M6 retention PASS durable=98' "$FOLLOWER_LOG.m6-retention"
cat "$LEADER_LOG.m6-retention"
cat "$FOLLOWER_LOG.m6-retention"
printf 'tlc_ha_replica_ub_111_to_112: PASS (M6 automatic retention -> snapshot)\n'

run_m7_tests

recovery_command="TLC_HA_UB_COLD_DIR='$NODE112_COLD_DIR' TLC_HA_UB_EXPECTED_EVENTS='$EXPECTED_EVENTS' ./benchmark/tlc_ha_replica_ub_node_ut follower-recover"
remote "$NODE112_PORT" "$NODE112_SSH" "$recovery_command" >"$FOLLOWER_LOG.recovery" 2>&1
grep -q 'follower recovery PASS' "$FOLLOWER_LOG.recovery"
cat "$FOLLOWER_LOG.recovery"
printf 'tlc_ha_replica_ub_111_to_112: PASS (persistent follower COLD recovery)\n'

# Both runtimes are stopped at this point; rebuild the shared rings before
# attaching the restarted Follower and issuing a manual replay range.
remote "$NODE111_PORT" "$NODE111_SSH" "$reset_command"
remote "$NODE112_PORT" "$NODE112_SSH" "$reset_peer_command"
restart_follower_command="$CTRL_FOLLOWER TLC_HA_UB_TX_PATH='$NODE112_TX_PATH' TLC_HA_UB_RX_PATH='$NODE112_RX_PATH' TLC_HA_UB_TX_OFFSET='$RX_OFFSET' TLC_HA_UB_RX_OFFSET='$TX_OFFSET' TLC_HA_UB_COLD_DIR='$NODE112_COLD_DIR' TLC_HA_UB_EXPECTED_EVENTS='$EXPECTED_EVENTS' TLC_HA_UB_WAIT_FOR_REPLAY=1 TLC_HA_UB_WAIT_FOR_REPLAY_MS='$RESTART_REPLAY_TIMEOUT_MS' timeout '${RESTART_REPLAY_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut follower"
replay_leader_command="$CTRL_LEADER TLC_HA_UB_TX_PATH='$NODE111_TX_PATH' TLC_HA_UB_RX_PATH='$NODE111_RX_PATH' TLC_HA_UB_TX_OFFSET='$TX_OFFSET' TLC_HA_UB_RX_OFFSET='$RX_OFFSET' TLC_HA_UB_COLD_DIR='$NODE111_COLD_DIR' TLC_HA_UB_EXPECTED_EVENTS='$EXPECTED_EVENTS' TLC_HA_UB_REPLAY_START='$REPLAY_START' TLC_HA_UB_REPLAY_END='$REPLAY_END' timeout '${RESTART_REPLAY_TIMEOUT_SECONDS}s' ./benchmark/tlc_ha_replica_ub_node_ut leader-replay"
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
printf 'TODO: automatic reconnect, failover, HA control plane / lineage transition; Redis TCP/SDK e2e is covered by separate scripts\n'
