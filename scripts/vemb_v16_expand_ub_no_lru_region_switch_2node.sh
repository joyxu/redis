#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_SCRIPT="${BASE_SCRIPT:-${SCRIPT_DIR}/vemb_v16_expand_ub_memory_2node.sh}"

NODE0_HOST="${NODE0_HOST:-192.168.1.111}"
NODE1_HOST="${NODE1_HOST:-192.168.1.112}"
SSH_USER="${SSH_USER:-root}"

export NODE0_HOST
export NODE1_HOST
export SSH_USER

export OWNER1_REGION_BYTES="${OWNER1_REGION_BYTES:-256}"
export PREFILL_KEYS="${PREFILL_KEYS:-0}"
export POST_KEYSPACE="${POST_KEYSPACE:-2000}"
export POST_OPS="${POST_OPS:-2000}"
export POST_THREADS="${POST_THREADS:-2}"

export NODE0_MANIFEST="${NODE0_MANIFEST:-/tmp/v16_node0_expand_ub_no_lru.yaml}"
export NODE1_MANIFEST="${NODE1_MANIFEST:-/tmp/v16_node1_expand_ub_no_lru.yaml}"
export NODE0_PEER_MAP="${NODE0_PEER_MAP:-/tmp/v16_node0_expand_ub_no_lru_peer_map.yaml}"
export NODE1_LOCAL_MAP="${NODE1_LOCAL_MAP:-/tmp/v16_node1_expand_ub_no_lru_local_map.yaml}"
export NODE0_LOG="${NODE0_LOG:-/tmp/v16_node0_expand_ub_no_lru.log}"
export NODE1_LOG="${NODE1_LOG:-/tmp/v16_node1_expand_ub_no_lru.log}"
export PREFILL_OUT="${PREFILL_OUT:-/tmp/v16_expand_ub_no_lru_prefill.out}"
export POST_WRITE_OUT="${POST_WRITE_OUT:-/tmp/v16_expand_ub_no_lru_post_write.out}"
export POST_READ_OUT="${POST_READ_OUT:-/tmp/v16_expand_ub_no_lru_post_read.out}"
export NODE0_TOPO_OUT="${NODE0_TOPO_OUT:-/tmp/v16_expand_ub_no_lru_node0_topology.out}"
export NODE1_TOPO_OUT="${NODE1_TOPO_OUT:-/tmp/v16_expand_ub_no_lru_node1_topology.out}"

ssh_run() {
    local host="$1"
    shift
    ssh -p 22 "${SSH_USER}@${host}" "$@"
}

step() {
    printf '\n== %s ==\n' "$1"
}

bash "${BASE_SCRIPT}"

step "Validate no-LRU region switch evidence"
ssh_run "${NODE0_HOST}" "grep -q '\\[done\\] mode=vadd threads=${POST_THREADS} ok=$((POST_OPS * POST_THREADS)) fail=0' ${POST_WRITE_OUT}"
ssh_run "${NODE0_HOST}" "grep -q '\\[done\\] mode=vemb-inline threads=${POST_THREADS} ok=$((POST_OPS * POST_THREADS)) fail=0' ${POST_READ_OUT}"
ssh_run "${NODE0_HOST}" "grep -q 'warm regions=3' ${POST_WRITE_OUT}"
ssh_run "${NODE0_HOST}" "grep -q 'warm regions=3' ${POST_READ_OUT}"
ssh_run "${NODE0_HOST}" "grep -q 'evict_ok=0' ${POST_WRITE_OUT}"
ssh_run "${NODE0_HOST}" "grep -q 'stale_handle=0' ${POST_READ_OUT}"
ssh_run "${NODE1_HOST}" "grep -q 'tlc warm region switch: reason=set_no_place .*region_id=101' ${NODE1_LOG}"

fallback_output="$(
    ssh_run "${NODE0_HOST}" \
        "grep -Eo 'fallback=[0-9]+' ${POST_WRITE_OUT} | cut -d= -f2 | sort -nr | head -1"
)"
fallback_count="$(printf '%s\n' "${fallback_output}" | awk '/^[0-9]+$/ { print; exit }')"
if [[ -z "${fallback_count}" || "${fallback_count}" -le 0 ]]; then
    echo "expected fallback>0 in ${POST_WRITE_OUT}, got '${fallback_count}'" >&2
    exit 1
fi

echo "No-LRU UB expansion region-switch validation passed: fallback=${fallback_count}, evict_ok=0"
