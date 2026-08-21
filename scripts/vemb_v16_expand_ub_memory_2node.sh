#!/usr/bin/env bash

set -euo pipefail

NODE0_HOST="${NODE0_HOST:-192.168.1.111}"
NODE1_HOST="${NODE1_HOST:-192.168.1.112}"
SSH_USER="${SSH_USER:-root}"
REMOTE_DIR="${REMOTE_DIR:-/root/szz/codespace/hpc-redis}"

SERVER_PORT="${SERVER_PORT:-6391}"

DIM="${DIM:-16}"
MAX_VECTORS="${MAX_VECTORS:-8192}"
PREFILL_KEYS="${PREFILL_KEYS:-1024}"
POST_KEYSPACE="${POST_KEYSPACE:-2000}"
POST_OPS="${POST_OPS:-2000}"
POST_THREADS="${POST_THREADS:-2}"

OWNER0_REGION_BYTES="${OWNER0_REGION_BYTES:-67108864}"
OWNER1_REGION_BYTES="${OWNER1_REGION_BYTES:-67108864}"
REGION_VALUE_SIZE="${REGION_VALUE_SIZE:-64}"

RESET_NODE0="${RESET_NODE0:-1}"
RESET_NODE1="${RESET_NODE1:-1}"

NODE0_MANIFEST="${NODE0_MANIFEST:-/tmp/v16_node0_expand_ub.yaml}"
NODE1_MANIFEST="${NODE1_MANIFEST:-/tmp/v16_node1_expand_ub.yaml}"
NODE0_PEER_MAP="${NODE0_PEER_MAP:-/tmp/v16_node0_expand_ub_peer_map.yaml}"
NODE1_LOCAL_MAP="${NODE1_LOCAL_MAP:-/tmp/v16_node1_expand_ub_local_map.yaml}"
NODE0_LOG="${NODE0_LOG:-/tmp/v16_node0_expand_ub.log}"
NODE1_LOG="${NODE1_LOG:-/tmp/v16_node1_expand_ub.log}"
PREFILL_OUT="${PREFILL_OUT:-/tmp/v16_expand_ub_prefill.out}"
POST_WRITE_OUT="${POST_WRITE_OUT:-/tmp/v16_expand_ub_post_write.out}"
POST_READ_OUT="${POST_READ_OUT:-/tmp/v16_expand_ub_post_read.out}"
NODE0_TOPO_OUT="${NODE0_TOPO_OUT:-/tmp/v16_expand_ub_node0_topology.out}"
NODE1_TOPO_OUT="${NODE1_TOPO_OUT:-/tmp/v16_expand_ub_node1_topology.out}"

EXTRA_REGION_ID="${EXTRA_REGION_ID:-102}"
EXTRA_REGION_BYTES="${EXTRA_REGION_BYTES:-67108864}"
EXTRA_REGION_VALUE_SIZE="${EXTRA_REGION_VALUE_SIZE:-${REGION_VALUE_SIZE}}"
NODE1_EXTRA_LOCAL_PATH="${NODE1_EXTRA_LOCAL_PATH:-/dev/obmm_shmdev3}"
NODE0_EXTRA_PEER_PATH="${NODE0_EXTRA_PEER_PATH:-/dev/obmm_shmdev7}"

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

step "Build binaries on both nodes"
ssh_run "${NODE0_HOST}" "cd ${REMOTE_DIR} && make -C src vemb_v16_server && make -C benchmark vemb_v16_bench && make -C benchmark vemb_v16_topology_ctl"
ssh_run "${NODE1_HOST}" "cd ${REMOTE_DIR} && make -C src vemb_v16_server && make -C benchmark vemb_v16_bench && make -C benchmark vemb_v16_topology_ctl"

step "Write node0 startup manifest"
ssh_run "${NODE0_HOST}" "cat >${NODE0_MANIFEST} <<YAML
local_ub_node_id: 0
local_region_weight: 4

remote_meta_provider: ub
remote_meta_path: /dev/obmm_shmdev1
remote_meta_cache_policy: cacheable
remote_meta_mmap_offset: 268435456
remote_meta_entries: 8192
remote_meta_buckets: 16384
ub_rpc_timeout_ms: 200

warm_regions:
  - region_id: 100
    provider: ub
    path: /dev/obmm_shmdev1
    cache_policy: cacheable
    mmap_offset: 0
    bytes: ${OWNER0_REGION_BYTES}
    value_size: ${REGION_VALUE_SIZE}
    home_ub_node_id: 0
    weight: 1
  - region_id: 101
    provider: ub
    path: /dev/obmm_shmdev5
    cache_policy: noncacheable
    mmap_offset: 0
    bytes: ${OWNER1_REGION_BYTES}
    value_size: ${REGION_VALUE_SIZE}
    home_ub_node_id: 1
    weight: 1

remote_meta_views:
  - owner_id: 1
    provider: ub
    path: /dev/obmm_shmdev5
    cache_policy: noncacheable
    mmap_offset: 268435456
    entries: 8192
    buckets: 16384

ub_rpc_peers:
  - owner_id: 1
    provider: ub
    request_path: /dev/obmm_shmdev6
    request_cache_policy: noncacheable
    request_mmap_offset: 8388608
    response_path: /dev/obmm_shmdev4
    response_cache_policy: cacheable
    response_mmap_offset: 16777216
    inbound_request_path: /dev/obmm_shmdev2
    inbound_request_cache_policy: cacheable
    inbound_request_mmap_offset: 8388608
    outbound_response_path: /dev/obmm_shmdev8
    outbound_response_cache_policy: noncacheable
    outbound_response_mmap_offset: 16777216
YAML"

step "Write node1 startup manifest"
ssh_run "${NODE1_HOST}" "cat >${NODE1_MANIFEST} <<YAML
local_ub_node_id: 1
local_region_weight: 4

remote_meta_provider: ub
remote_meta_path: /dev/obmm_shmdev1
remote_meta_cache_policy: cacheable
remote_meta_mmap_offset: 268435456
remote_meta_entries: 8192
remote_meta_buckets: 16384
ub_rpc_timeout_ms: 200

warm_regions:
  - region_id: 101
    provider: ub
    path: /dev/obmm_shmdev1
    cache_policy: cacheable
    mmap_offset: 0
    bytes: ${OWNER1_REGION_BYTES}
    value_size: ${REGION_VALUE_SIZE}
    home_ub_node_id: 1
    weight: 1
  - region_id: 100
    provider: ub
    path: /dev/obmm_shmdev5
    cache_policy: noncacheable
    mmap_offset: 0
    bytes: ${OWNER0_REGION_BYTES}
    value_size: ${REGION_VALUE_SIZE}
    home_ub_node_id: 0
    weight: 1

remote_meta_views:
  - owner_id: 0
    provider: ub
    path: /dev/obmm_shmdev5
    cache_policy: noncacheable
    mmap_offset: 268435456
    entries: 8192
    buckets: 16384

ub_rpc_peers:
  - owner_id: 0
    provider: ub
    request_path: /dev/obmm_shmdev6
    request_cache_policy: noncacheable
    request_mmap_offset: 8388608
    response_path: /dev/obmm_shmdev4
    response_cache_policy: cacheable
    response_mmap_offset: 16777216
    inbound_request_path: /dev/obmm_shmdev2
    inbound_request_cache_policy: cacheable
    inbound_request_mmap_offset: 8388608
    outbound_response_path: /dev/obmm_shmdev8
    outbound_response_cache_policy: noncacheable
    outbound_response_mmap_offset: 16777216
YAML"

step "Stop old processes"
remote_kill_vemb_processes "${NODE0_HOST}"
remote_kill_vemb_processes "${NODE1_HOST}"

step "Start node0 from manifest"
reset_node0_arg=""
if [[ "${RESET_NODE0}" == "1" ]]; then
    reset_node0_arg="--reset-warm-regions"
fi
ssh_run "${NODE0_HOST}" "cd ${REMOTE_DIR} && rm -f ${NODE0_LOG} ${PREFILL_OUT} ${POST_WRITE_OUT} ${POST_READ_OUT} ${NODE0_TOPO_OUT} ${NODE1_TOPO_OUT} && setsid -f ./src/vemb_v16_server --transport tcp --tcp-host ${NODE0_HOST} --tcp-port ${SERVER_PORT} --proxy-io-threads 1 --supernode-workers 1 --warm-regions-manifest ${NODE0_MANIFEST} ${reset_node0_arg} --dim ${DIM} --max-vectors ${MAX_VECTORS} --loglevel notice >${NODE0_LOG} 2>&1 </dev/null"

step "Start node1 from manifest"
reset_node1_arg=""
if [[ "${RESET_NODE1}" == "1" ]]; then
    reset_node1_arg="--reset-warm-regions"
fi
ssh_run "${NODE1_HOST}" "cd ${REMOTE_DIR} && rm -f ${NODE1_LOG} && setsid -f ./src/vemb_v16_server --transport tcp --tcp-host ${NODE1_HOST} --tcp-port ${SERVER_PORT} --proxy-io-threads 1 --supernode-workers 1 --warm-regions-manifest ${NODE1_MANIFEST} ${reset_node1_arg} --dim ${DIM} --max-vectors ${MAX_VECTORS} --loglevel notice >${NODE1_LOG} 2>&1 </dev/null"

sleep 2

step "Verify startup logs"
ssh_run "${NODE0_HOST}" "grep -E 'remote meta ready|registered vemb_v16 remote meta owner view|ub rpc ready|server ready' ${NODE0_LOG}"
ssh_run "${NODE1_HOST}" "grep -E 'remote meta ready|registered vemb_v16 remote meta owner view|ub rpc ready|server ready' ${NODE1_LOG}"

step "Publish steady topology active={0,1}"
ssh_run "${NODE0_HOST}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_topology_ctl --set --transport tcp --host ${NODE0_HOST} --port ${SERVER_PORT} --epoch 1 --min-write-epoch 1 --active 0,1 --standby 0,1 --owner-endpoints 0=${NODE0_HOST}:${SERVER_PORT},1=${NODE1_HOST}:${SERVER_PORT} --timeout-ms 5000"
ssh_run "${NODE1_HOST}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_topology_ctl --set --transport tcp --host ${NODE1_HOST} --port ${SERVER_PORT} --epoch 1 --min-write-epoch 1 --active 0,1 --standby 0,1 --owner-endpoints 0=${NODE0_HOST}:${SERVER_PORT},1=${NODE1_HOST}:${SERVER_PORT} --timeout-ms 5000"

step "Prefill baseline dataset"
ssh_run "${NODE0_HOST}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_bench --transport tcp --endpoints ${NODE0_HOST}:${SERVER_PORT},${NODE1_HOST}:${SERVER_PORT} --dim ${DIM} --prefill ${PREFILL_KEYS} --ops 0 --threads 1 --pipeline 1 --mode vadd --timeout-ms 10000 >${PREFILL_OUT} 2>&1 && cat ${PREFILL_OUT}"

step "Write node1 local UB expansion map"
ssh_run "${NODE1_HOST}" "cat >${NODE1_LOCAL_MAP} <<YAML
expected_local_owner_id: 1
attach_now: true

warm_regions:
  - region_id: ${EXTRA_REGION_ID}
    provider: ub
    path: ${NODE1_EXTRA_LOCAL_PATH}
    cache_policy: cacheable
    mmap_offset: 0
    bytes: ${EXTRA_REGION_BYTES}
    value_size: ${EXTRA_REGION_VALUE_SIZE}
    home_ub_node_id: 1
    weight: 1
YAML"

step "Write node0 peer UB expansion map"
ssh_run "${NODE0_HOST}" "cat >${NODE0_PEER_MAP} <<YAML
expected_local_owner_id: 0
attach_now: true

warm_regions:
  - region_id: ${EXTRA_REGION_ID}
    provider: ub
    path: ${NODE0_EXTRA_PEER_PATH}
    cache_policy: noncacheable
    mmap_offset: 0
    bytes: ${EXTRA_REGION_BYTES}
    value_size: ${EXTRA_REGION_VALUE_SIZE}
    home_ub_node_id: 1
    weight: 1
YAML"

step "Apply local and peer UB expansion maps"
ssh_run "${NODE1_HOST}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_topology_ctl --apply-peer-view-map ${NODE1_LOCAL_MAP} --transport tcp --host ${NODE1_HOST} --port ${SERVER_PORT} --timeout-ms 5000"
ssh_run "${NODE0_HOST}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_topology_ctl --apply-peer-view-map ${NODE0_PEER_MAP} --transport tcp --host ${NODE0_HOST} --port ${SERVER_PORT} --timeout-ms 5000"

step "Verify runtime attach logs"
ssh_run "${NODE1_HOST}" "grep -q 'runtime warm region attached: local_owner=1 peer_owner=1 region_id=${EXTRA_REGION_ID}' ${NODE1_LOG}"
ssh_run "${NODE0_HOST}" "grep -q 'runtime warm region attached: local_owner=0 peer_owner=1 region_id=${EXTRA_REGION_ID}' ${NODE0_LOG}"
ssh_run "${NODE1_HOST}" "grep 'runtime warm region attached: local_owner=1 peer_owner=1 region_id=${EXTRA_REGION_ID}' ${NODE1_LOG}"
ssh_run "${NODE0_HOST}" "grep 'runtime warm region attached: local_owner=0 peer_owner=1 region_id=${EXTRA_REGION_ID}' ${NODE0_LOG}"

step "Verify topology unchanged on both nodes"
ssh_run "${NODE0_HOST}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_topology_ctl --get --transport tcp --host ${NODE0_HOST} --port ${SERVER_PORT} --timeout-ms 5000 >${NODE0_TOPO_OUT} && grep -q '^current_topology_epoch=1$' ${NODE0_TOPO_OUT} && grep -q '^active_owners=0,1$' ${NODE0_TOPO_OUT} && grep -q '^standby_owners=0,1$' ${NODE0_TOPO_OUT}"
ssh_run "${NODE1_HOST}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_topology_ctl --get --transport tcp --host ${NODE1_HOST} --port ${SERVER_PORT} --timeout-ms 5000 >${NODE1_TOPO_OUT} && grep -q '^current_topology_epoch=1$' ${NODE1_TOPO_OUT} && grep -q '^active_owners=0,1$' ${NODE1_TOPO_OUT} && grep -q '^standby_owners=0,1$' ${NODE1_TOPO_OUT}"

step "Run post-expand write validation"
ssh_run "${NODE0_HOST}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_bench --transport tcp --endpoints ${NODE0_HOST}:${SERVER_PORT},${NODE1_HOST}:${SERVER_PORT} --dim ${DIM} --prefill 0 --keyspace ${POST_KEYSPACE} --ops ${POST_OPS} --threads ${POST_THREADS} --pipeline 1 --mode vadd --timeout-ms 10000 >${POST_WRITE_OUT} 2>&1 && cat ${POST_WRITE_OUT}"
ssh_run "${NODE0_HOST}" "grep -q 'warm regions=3' ${POST_WRITE_OUT}"

step "Run post-expand read validation"
ssh_run "${NODE0_HOST}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_bench --transport tcp --endpoints ${NODE0_HOST}:${SERVER_PORT},${NODE1_HOST}:${SERVER_PORT} --dim ${DIM} --prefill 0 --keyspace ${POST_KEYSPACE} --ops ${POST_OPS} --threads ${POST_THREADS} --pipeline 1 --mode vemb-inline --timeout-ms 10000 >${POST_READ_OUT} 2>&1 && cat ${POST_READ_OUT}"
ssh_run "${NODE0_HOST}" "grep -q 'warm regions=3' ${POST_READ_OUT}"

step "Done"
echo "UB-memory-only expansion finished. Useful logs:"
echo "  node0: ssh ${SSH_USER}@${NODE0_HOST} 'tail -200 ${NODE0_LOG}'"
echo "  node1: ssh ${SSH_USER}@${NODE1_HOST} 'tail -200 ${NODE1_LOG}'"
