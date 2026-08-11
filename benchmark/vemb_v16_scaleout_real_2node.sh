#!/usr/bin/env bash

set -euo pipefail

NODE0_HOST="${NODE0_HOST:-192.168.1.111}"
NODE1_HOST="${NODE1_HOST:-192.168.1.112}"
SSH_USER="${SSH_USER:-root}"
REMOTE_DIR="${REMOTE_DIR:-/root/szz/codespace/hpc-redis}"

SERVER_PORT="${SERVER_PORT:-6391}"
COORD_PORT="${COORD_PORT:-7391}"

PAYLOAD_LOCAL_PATH="${PAYLOAD_LOCAL_PATH:-/dev/obmm_shmdev1}"
PAYLOAD_PEER_PATH="${PAYLOAD_PEER_PATH:-/dev/obmm_shmdev5}"
REQUEST_LOCAL_PATH="${REQUEST_LOCAL_PATH:-/dev/obmm_shmdev2}"
REQUEST_PEER_PATH="${REQUEST_PEER_PATH:-/dev/obmm_shmdev6}"
RESPONSE_LOCAL_PATH="${RESPONSE_LOCAL_PATH:-/dev/obmm_shmdev4}"
RESPONSE_PEER_PATH="${RESPONSE_PEER_PATH:-/dev/obmm_shmdev8}"

DIM="${DIM:-16}"
MAX_VECTORS="${MAX_VECTORS:-8192}"
PREFILL_KEYS="${PREFILL_KEYS:-1024}"
POST_KEYSPACE="${POST_KEYSPACE:-2000}"
POST_OPS="${POST_OPS:-2000}"
POST_THREADS="${POST_THREADS:-2}"
LIVE_WRITE_OPS="${LIVE_WRITE_OPS:-50000}"
LIVE_MODE="${LIVE_MODE:-vadd}"
LIVE_THREADS="${LIVE_THREADS:-2}"
LIVE_TIMEOUT_MS="${LIVE_TIMEOUT_MS:-180000}"
MIGRATION_EPOCH="${MIGRATION_EPOCH:-23}"
CUTOVER_EPOCH="${CUTOVER_EPOCH:-24}"
CONTROL_TIMEOUT_MS="${CONTROL_TIMEOUT_MS:-5000}"
COMBINED_CONTROL_TIMEOUT_MS="${COMBINED_CONTROL_TIMEOUT_MS:-180000}"
UB_RPC_TIMEOUT_MS="${UB_RPC_TIMEOUT_MS:-2000}"

RUN_LIVE_WRITE="${RUN_LIVE_WRITE:-1}"
RUN_POST_READ="${RUN_POST_READ:-1}"
VERIFY_MIGRATED_DATA="${VERIFY_MIGRATED_DATA:-1}"
VERIFY_SAMPLE_COUNT="${VERIFY_SAMPLE_COUNT:-8}"
RESET_NODE0="${RESET_NODE0:-1}"
RESET_NODE1="${RESET_NODE1:-1}"

NODE0_MANIFEST="${NODE0_MANIFEST:-/tmp/v16_node0_scaleout.yaml}"
NODE1_MANIFEST="${NODE1_MANIFEST:-/tmp/v16_node1_scaleout.yaml}"
NODE0_PEER_MAP="${NODE0_PEER_MAP:-/tmp/v16_node0_peer_map.yaml}"
NODE0_LOG="${NODE0_LOG:-/tmp/v16_node0_scaleout.log}"
NODE1_LOG="${NODE1_LOG:-/tmp/v16_node1_scaleout.log}"
PREFILL_OUT="${PREFILL_OUT:-/tmp/v16_prefill_scaleout.out}"
LIVE_WRITE_OUT="${LIVE_WRITE_OUT:-/tmp/v16_live_write_scaleout.out}"
LIVE_WRITE_PID="${LIVE_WRITE_PID:-/tmp/v16_live_write_scaleout.pid}"
LIVE_WRITE_RC="${LIVE_WRITE_RC:-/tmp/v16_live_write_scaleout.rc}"
COORD_OUT="${COORD_OUT:-/tmp/v16_coordinator_scaleout.out}"
COORD_ERR="${COORD_ERR:-/tmp/v16_coordinator_scaleout.err}"
POST_WRITE_OUT="${POST_WRITE_OUT:-/tmp/v16_post_write_scaleout.out}"
POST_READ_OUT="${POST_READ_OUT:-/tmp/v16_post_read_scaleout.out}"

ssh_run() {
    local host="$1"
    shift
    ssh -p 22 "${SSH_USER}@${host}" "$@"
}

step() {
    printf '\n== %s ==\n' "$1"
}

remote_kill_vemb_processes() {
    local host="$1"
    ssh_run "${host}" "pkill -f '(^|/)(vemb_v16_server|vemb_v16_topology_ctl|vemb_v16_bench)( |$)' || true"
}

verify_remote_ub_paths_idle() {
    local host="$1"
    local expected_regex="$2"
    ssh_run "${host}" "paths='${REQUEST_LOCAL_PATH} ${RESPONSE_LOCAL_PATH} ${REQUEST_PEER_PATH} ${RESPONSE_PEER_PATH}'; out=\$(lsof \$paths 2>/dev/null || true); if [ -z \"\$out\" ]; then exit 0; fi; filtered=\$(printf '%s\n' \"\$out\" | awk 'NR==1 || \$1 ~ /${expected_regex}/'); if [ -z \"\$filtered\" ]; then printf 'unexpected UB path users on ${host}:\n%s\n' \"\$out\"; exit 1; fi; if [ \"\$(printf '%s\n' \"\$out\" | wc -l)\" -ne \"\$(printf '%s\n' \"\$filtered\" | wc -l)\" ]; then printf 'unexpected UB path users on ${host}:\n%s\n' \"\$out\"; exit 1; fi"
}

step "Build binaries on both nodes"
ssh_run "${NODE0_HOST}" "cd ${REMOTE_DIR} && make -C src vemb_v16_server && make -C benchmark vemb_v16_bench && make -C benchmark vemb_v16_topology_ctl && make -C benchmark vemb_v16_read_verify"
ssh_run "${NODE1_HOST}" "cd ${REMOTE_DIR} && make -C src vemb_v16_server && make -C benchmark vemb_v16_bench && make -C benchmark vemb_v16_topology_ctl && make -C benchmark vemb_v16_read_verify"

step "Write node0 startup manifest"
ssh_run "${NODE0_HOST}" "cat >${NODE0_MANIFEST} <<'YAML'
local_ub_node_id: 0
local_region_weight: 4

remote_meta_provider: ub
remote_meta_path: ${PAYLOAD_LOCAL_PATH}
remote_meta_mmap_offset: 268435456
remote_meta_entries: 8192
remote_meta_buckets: 16384
ub_rpc_timeout_ms: ${UB_RPC_TIMEOUT_MS}

warm_regions:
  - region_id: 100
    provider: ub
    path: ${PAYLOAD_LOCAL_PATH}
    mmap_offset: 0
    bytes: 67108864
    value_size: 64
    home_ub_node_id: 0
    weight: 1
YAML"

step "Write node1 startup manifest"
ssh_run "${NODE1_HOST}" "cat >${NODE1_MANIFEST} <<'YAML'
local_ub_node_id: 1
local_region_weight: 4

remote_meta_provider: ub
remote_meta_path: ${PAYLOAD_LOCAL_PATH}
remote_meta_mmap_offset: 268435456
remote_meta_entries: 8192
remote_meta_buckets: 16384
ub_rpc_timeout_ms: ${UB_RPC_TIMEOUT_MS}

warm_regions:
  - region_id: 101
    provider: ub
    path: ${PAYLOAD_LOCAL_PATH}
    mmap_offset: 0
    bytes: 67108864
    value_size: 64
    home_ub_node_id: 1
    weight: 1
  - region_id: 100
    provider: ub
    path: ${PAYLOAD_PEER_PATH}
    mmap_offset: 0
    bytes: 67108864
    value_size: 64
    home_ub_node_id: 0
    weight: 1

remote_meta_views:
  - owner_id: 0
    provider: ub
    path: ${PAYLOAD_PEER_PATH}
    mmap_offset: 268435456
    entries: 8192
    buckets: 16384

ub_rpc_peers:
  - owner_id: 0
    provider: ub
    request_path: ${REQUEST_LOCAL_PATH}
    request_mmap_offset: 8388608
    response_path: ${RESPONSE_PEER_PATH}
    response_mmap_offset: 16777216
    inbound_request_path: ${REQUEST_PEER_PATH}
    inbound_request_mmap_offset: 8388608
    outbound_response_path: ${RESPONSE_LOCAL_PATH}
    outbound_response_mmap_offset: 16777216
YAML"

step "Stop old processes"
remote_kill_vemb_processes "${NODE0_HOST}"
remote_kill_vemb_processes "${NODE1_HOST}"

step "Verify UB RPC paths are idle before test"
verify_remote_ub_paths_idle "${NODE0_HOST}" "bash|sh|ssh|sshd|lsof|awk"
verify_remote_ub_paths_idle "${NODE1_HOST}" "bash|sh|ssh|sshd|lsof|awk"

step "Start node0 from manifest"
reset_node0_arg=""
if [[ "${RESET_NODE0}" == "1" ]]; then
    reset_node0_arg="--reset-warm-regions"
fi
ssh_run "${NODE0_HOST}" "cd ${REMOTE_DIR} && rm -f ${NODE0_LOG} ${PREFILL_OUT} ${LIVE_WRITE_OUT} ${LIVE_WRITE_PID} ${LIVE_WRITE_RC} ${COORD_OUT} ${COORD_ERR} ${POST_WRITE_OUT} ${POST_READ_OUT} && setsid -f ./src/vemb_v16_server --transport tcp --tcp-host ${NODE0_HOST} --tcp-port ${SERVER_PORT} --proxy-io-threads 1 --supernode-workers 1 --warm-regions-manifest ${NODE0_MANIFEST} ${reset_node0_arg} --dim ${DIM} --max-vectors ${MAX_VECTORS} --loglevel notice >${NODE0_LOG} 2>&1 </dev/null"

step "Start node1 from manifest"
reset_node1_arg=""
if [[ "${RESET_NODE1}" == "1" ]]; then
    reset_node1_arg="--reset-warm-regions"
fi
ssh_run "${NODE1_HOST}" "cd ${REMOTE_DIR} && rm -f ${NODE1_LOG} && setsid -f ./src/vemb_v16_server --transport tcp --tcp-host ${NODE1_HOST} --tcp-port ${SERVER_PORT} --proxy-io-threads 1 --supernode-workers 1 --warm-regions-manifest ${NODE1_MANIFEST} ${reset_node1_arg} --dim ${DIM} --max-vectors ${MAX_VECTORS} --loglevel notice >${NODE1_LOG} 2>&1 </dev/null"

sleep 2

step "Write node0 scaleout peer-view map"
ssh_run "${NODE0_HOST}" "cat >${NODE0_PEER_MAP} <<'YAML'
expected_local_owner_id: 0
attach_now: true
ub_rpc_timeout_ms: ${UB_RPC_TIMEOUT_MS}

warm_regions:
  - region_id: 101
    provider: ub
    path: ${PAYLOAD_PEER_PATH}
    mmap_offset: 0
    bytes: 67108864
    value_size: 64
    home_ub_node_id: 1
    weight: 1

remote_meta_views:
  - owner_id: 1
    provider: ub
    path: ${PAYLOAD_PEER_PATH}
    mmap_offset: 268435456
    entries: 8192
    buckets: 16384

ub_rpc_peers:
  - owner_id: 1
    provider: ub
    request_path: ${REQUEST_LOCAL_PATH}
    request_mmap_offset: 8388608
    response_path: ${RESPONSE_PEER_PATH}
    response_mmap_offset: 16777216
    inbound_request_path: ${REQUEST_PEER_PATH}
    inbound_request_mmap_offset: 8388608
    outbound_response_path: ${RESPONSE_LOCAL_PATH}
    outbound_response_mmap_offset: 16777216
YAML"

step "Verify startup logs"
ssh_run "${NODE0_HOST}" "grep -E 'remote meta ready|ub rpc ready|server ready' ${NODE0_LOG}"
ssh_run "${NODE1_HOST}" "grep -E 'remote meta ready|registered vemb_v16 remote meta owner view|ub rpc ready|server ready' ${NODE1_LOG}"

step "Publish initial topology active={0}"
ssh_run "${NODE0_HOST}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_topology_ctl --set --transport tcp --host ${NODE0_HOST} --port ${SERVER_PORT} --epoch 1 --min-write-epoch 1 --active 0 --standby 0 --owner-endpoints 0=${NODE0_HOST}:${SERVER_PORT} --timeout-ms ${CONTROL_TIMEOUT_MS}"

step "Prefill initial dataset"
ssh_run "${NODE0_HOST}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_bench --transport tcp --endpoints ${NODE0_HOST}:${SERVER_PORT} --dim ${DIM} --prefill ${PREFILL_KEYS} --ops 0 --threads 1 --pipeline 1 --mode vadd --client-topology --timeout-ms 10000 >${PREFILL_OUT} 2>&1 && cat ${PREFILL_OUT}"

if [[ "${RUN_LIVE_WRITE}" == "1" ]]; then
    step "Start live write pressure during scaleout"
    ssh_run "${NODE0_HOST}" "cd ${REMOTE_DIR} && setsid -f sh -lc 'echo \$\$ >${LIVE_WRITE_PID}; ./benchmark/vemb_v16_bench --transport tcp --endpoints ${NODE0_HOST}:${SERVER_PORT} --dim ${DIM} --prefill 0 --keyspace ${PREFILL_KEYS} --ops ${LIVE_WRITE_OPS} --threads ${LIVE_THREADS} --pipeline 1 --mode ${LIVE_MODE} --client-topology --timeout-ms ${LIVE_TIMEOUT_MS} >${LIVE_WRITE_OUT} 2>&1 </dev/null; rc=\$?; echo \$rc >${LIVE_WRITE_RC}; exit \$rc'"
fi

step "Start coordinator listener"
ssh_run "${NODE0_HOST}" "cd ${REMOTE_DIR} && setsid -f ./benchmark/vemb_v16_topology_ctl --coordinator-listen --transport tcp --host ${NODE0_HOST} --port ${COORD_PORT} --expected-sources 0 --migration-epoch ${MIGRATION_EPOCH} --cutover-epoch ${CUTOVER_EPOCH} --standby 0,1 --owner-endpoints 0=${NODE0_HOST}:${SERVER_PORT},1=${NODE1_HOST}:${SERVER_PORT} --wait-ms 60000 --timeout-ms ${CONTROL_TIMEOUT_MS} >${COORD_OUT} 2>${COORD_ERR} </dev/null"
sleep 1

step "Publish candidate topology to node1 then node0"
ssh_run "${NODE1_HOST}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_topology_ctl --set --transport tcp --host ${NODE1_HOST} --port ${SERVER_PORT} --epoch ${MIGRATION_EPOCH} --min-write-epoch ${MIGRATION_EPOCH} --active 0 --standby 0,1 --dual-write --auto-scaleout --coordinated-scaleout --owner-endpoints 0=${NODE0_HOST}:${SERVER_PORT},1=${NODE1_HOST}:${SERVER_PORT} --coordinator-endpoint ${NODE0_HOST}:${COORD_PORT} --timeout-ms ${CONTROL_TIMEOUT_MS}"
ssh_run "${NODE0_HOST}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_topology_ctl --set-with-peer-view-map ${NODE0_PEER_MAP} --transport tcp --host ${NODE0_HOST} --port ${SERVER_PORT} --epoch ${MIGRATION_EPOCH} --min-write-epoch ${MIGRATION_EPOCH} --active 0 --standby 0,1 --dual-write --auto-scaleout --coordinated-scaleout --owner-endpoints 0=${NODE0_HOST}:${SERVER_PORT},1=${NODE1_HOST}:${SERVER_PORT} --coordinator-endpoint ${NODE0_HOST}:${COORD_PORT} --timeout-ms ${COMBINED_CONTROL_TIMEOUT_MS}"

step "Verify node0 combined peer-view attach logs"
ssh_run "${NODE0_HOST}" "for _ in \$(seq 1 20); do if grep -q 'runtime warm region attached: local_owner=0 peer_owner=1' ${NODE0_LOG} && grep -q 'runtime remote_meta owner view attached: local_owner=0 peer_owner=1' ${NODE0_LOG} && grep -q 'runtime ub rpc peer attached: local_owner=0 peer_owner=1' ${NODE0_LOG}; then exit 0; fi; sleep 1; done; exit 1"
ssh_run "${NODE0_HOST}" "grep -E 'runtime warm region attached|runtime remote_meta owner view attached|runtime ub rpc peer attached' ${NODE0_LOG}"

step "Show coordinator result"
ssh_run "${NODE0_HOST}" "for _ in \$(seq 1 60); do if grep -q '^scaleout_all_sources_done=1$' ${COORD_OUT} && grep -q '^scaleout_full_active_published=' ${COORD_OUT}; then exit 0; fi; sleep 1; done; exit 1"
ssh_run "${NODE0_HOST}" "cat ${COORD_OUT}; echo '---'; cat ${COORD_ERR} || true"

step "Show source migration summary"
ssh_run "${NODE0_HOST}" "grep -E 'migration auto plan|scaleout auto|local done' ${NODE0_LOG} | tail -n 20"

step "Verify final topology on both nodes"
ssh_run "${NODE0_HOST}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_topology_ctl --get --transport tcp --host ${NODE0_HOST} --port ${SERVER_PORT} --timeout-ms ${CONTROL_TIMEOUT_MS}"
ssh_run "${NODE1_HOST}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_topology_ctl --get --transport tcp --host ${NODE1_HOST} --port ${SERVER_PORT} --timeout-ms ${CONTROL_TIMEOUT_MS}"

if [[ "${VERIFY_MIGRATED_DATA}" == "1" ]]; then
    step "Verify migrated prefill data on node1"
    ssh_run "${NODE0_HOST}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_read_verify --host ${NODE1_HOST} --port ${SERVER_PORT} --dim ${DIM} --prefill ${PREFILL_KEYS} --sample-count ${VERIFY_SAMPLE_COUNT} --target-owner 1 --timeout-ms 10000"
fi

step "Post-cutover write validation"
ssh_run "${NODE0_HOST}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_bench --transport tcp --endpoints ${NODE0_HOST}:${SERVER_PORT},${NODE1_HOST}:${SERVER_PORT} --dim ${DIM} --prefill 0 --keyspace ${POST_KEYSPACE} --ops ${POST_OPS} --threads ${POST_THREADS} --pipeline 1 --mode vadd --client-topology --timeout-ms 10000 >${POST_WRITE_OUT} 2>&1 && cat ${POST_WRITE_OUT}"
ssh_run "${NODE0_HOST}" "awk '/^\\[stats node=1\\]/{in_node=1;next} in_node && /^\\[stats\\] total=/{for(i=1;i<=NF;i++) if(\$i ~ /^vadd=/){sub(/^vadd=/,\"\",\$i); if(\$i+0>0) exit 0; else exit 1}} END{if(!in_node) exit 1}' ${POST_WRITE_OUT}"

if [[ "${RUN_POST_READ}" == "1" ]]; then
    step "Post-cutover read validation"
    ssh_run "${NODE0_HOST}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_bench --transport tcp --endpoints ${NODE0_HOST}:${SERVER_PORT},${NODE1_HOST}:${SERVER_PORT} --dim ${DIM} --prefill 0 --keyspace ${POST_KEYSPACE} --ops ${POST_OPS} --threads ${POST_THREADS} --pipeline 1 --mode vemb-inline --client-topology --timeout-ms 10000 >${POST_READ_OUT} 2>&1 && cat ${POST_READ_OUT}"
fi

if [[ "${RUN_LIVE_WRITE}" == "1" ]]; then
    step "Wait live write pressure result"
    ssh_run "${NODE0_HOST}" "pid=\$(cat ${LIVE_WRITE_PID}); for _ in \$(seq 1 240); do if ! kill -0 \$pid 2>/dev/null; then break; fi; sleep 1; done; if kill -0 \$pid 2>/dev/null; then echo 'live write still running'; exit 1; fi; grep -q '^\\[done\\]' ${LIVE_WRITE_OUT}; test \"\$(cat ${LIVE_WRITE_RC})\" = \"0\""
    ssh_run "${NODE0_HOST}" "cat ${LIVE_WRITE_OUT}"
fi

step "Done"
echo "Scaleout flow finished. Useful logs:"
echo "  node0: ssh ${SSH_USER}@${NODE0_HOST} 'tail -200 ${NODE0_LOG}'"
echo "  node1: ssh ${SSH_USER}@${NODE1_HOST} 'tail -200 ${NODE1_LOG}'"
echo "  coordinator: ssh ${SSH_USER}@${NODE0_HOST} 'cat ${COORD_OUT}'"
