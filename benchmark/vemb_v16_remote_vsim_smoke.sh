#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE_PORT="${VEMB_V16_SMOKE_BASE_PORT:-6420}"
DIM="${VEMB_V16_SMOKE_DIM:-8}"
PREFILL="${VEMB_V16_SMOKE_PREFILL:-64}"
OPS="${VEMB_V16_SMOKE_OPS:-32}"
TMPDIR="${TMPDIR:-/tmp}/vemb_v16_remote_vsim_smoke_$$"

mkdir -p "$TMPDIR"

PID0=""
PID1=""

cleanup_servers() {
    if [[ -n "$PID0" ]] && kill -0 "$PID0" 2>/dev/null; then
        kill "$PID0" 2>/dev/null || true
        wait "$PID0" 2>/dev/null || true
    fi
    if [[ -n "$PID1" ]] && kill -0 "$PID1" 2>/dev/null; then
        kill "$PID1" 2>/dev/null || true
        wait "$PID1" 2>/dev/null || true
    fi
    PID0=""
    PID1=""
}
trap cleanup_servers EXIT

write_manifest() {
    local manifest="$1"
    local owner="$2"
    local case_name="$3"
    local include_remote_meta_view="$4"
    local remote_meta_sets="$5"
    local remote_meta_ways="$6"
    local peer
    local local_payload
    local remote_payload
    local local_rm
    local remote_rm
    local req_local_remote
    local req_remote_local
    local resp_local_remote
    local resp_remote_local

    if [[ "$owner" == "0" ]]; then
        peer=1
        local_payload="/v16_${case_name}_payload0"
        remote_payload="/v16_${case_name}_payload1"
        local_rm="/v16_${case_name}_rm0"
        remote_rm="/v16_${case_name}_rm1"
        req_local_remote="/v16_${case_name}_rpc_req_0_1"
        req_remote_local="/v16_${case_name}_rpc_req_1_0"
        resp_local_remote="/v16_${case_name}_rpc_resp_0_1"
        resp_remote_local="/v16_${case_name}_rpc_resp_1_0"
    else
        peer=0
        local_payload="/v16_${case_name}_payload1"
        remote_payload="/v16_${case_name}_payload0"
        local_rm="/v16_${case_name}_rm1"
        remote_rm="/v16_${case_name}_rm0"
        req_local_remote="/v16_${case_name}_rpc_req_1_0"
        req_remote_local="/v16_${case_name}_rpc_req_0_1"
        resp_local_remote="/v16_${case_name}_rpc_resp_1_0"
        resp_remote_local="/v16_${case_name}_rpc_resp_0_1"
    fi

    cat > "$manifest" <<EOF_MANIFEST
local_ub_node_id: ${owner}
local_region_weight: 4
remote_meta_provider: shm
remote_meta_path: ${local_rm}
remote_meta_sets: ${remote_meta_sets}
remote_meta_ways: ${remote_meta_ways}
ub_rpc_timeout_ms: 100
warm_regions:
  - region_id: 100
    provider: shm
    path: /v16_${case_name}_payload0
    mmap_offset: 0
    bytes: 8192
    value_size: 32
    home_ub_node_id: 0
    weight: 1
  - region_id: 200
    provider: shm
    path: /v16_${case_name}_payload1
    mmap_offset: 0
    bytes: 8192
    value_size: 32
    home_ub_node_id: 1
    weight: 1
EOF_MANIFEST

    if [[ "$include_remote_meta_view" == "yes" ]]; then
        cat >> "$manifest" <<EOF_MANIFEST
remote_meta_views:
  - owner_id: ${peer}
    provider: shm
    path: ${remote_rm}
    sets: ${remote_meta_sets}
    ways: ${remote_meta_ways}
EOF_MANIFEST
    fi

    cat >> "$manifest" <<EOF_MANIFEST
ub_rpc_peers:
  - owner_id: ${peer}
    provider: shm
    request_path: ${req_local_remote}
    response_path: ${resp_remote_local}
    inbound_request_path: ${req_remote_local}
    outbound_response_path: ${resp_local_remote}
EOF_MANIFEST
}

run_case() {
    local case_name="$1"
    local include_remote_meta_view="$2"
    local remote_meta_sets="$3"
    local remote_meta_ways="$4"
    local expected_source="$5"
    local port0=$((BASE_PORT + RANDOM % 1000))
    local port1=$((port0 + 1))
    local manifest0="$TMPDIR/${case_name}_sn0.yaml"
    local manifest1="$TMPDIR/${case_name}_sn1.yaml"
    local log0="$TMPDIR/${case_name}_sn0.log"
    local log1="$TMPDIR/${case_name}_sn1.log"
    local bench_log="$TMPDIR/${case_name}_bench.log"

    write_manifest "$manifest0" 0 "$case_name" \
        "$include_remote_meta_view" "$remote_meta_sets" "$remote_meta_ways"
    write_manifest "$manifest1" 1 "$case_name" \
        "$include_remote_meta_view" "$remote_meta_sets" "$remote_meta_ways"

    "$ROOT/src/vemb_v16_server" \
        --transport tcp \
        --tcp-host 127.0.0.1 \
        --tcp-port "$port0" \
        --proxy-io-threads 1 \
        --supernode-workers 2 \
        --warm-regions-manifest "$manifest0" \
        --reset-warm-regions \
        --dim "$DIM" \
        --loglevel debug > "$log0" 2>&1 &
    PID0="$!"

    sleep 0.3

    "$ROOT/src/vemb_v16_server" \
        --transport tcp \
        --tcp-host 127.0.0.1 \
        --tcp-port "$port1" \
        --proxy-io-threads 1 \
        --supernode-workers 2 \
        --warm-regions-manifest "$manifest1" \
        --dim "$DIM" \
        --loglevel debug > "$log1" 2>&1 &
    PID1="$!"

    sleep 0.7

    "$ROOT/benchmark/vemb_v16_bench" \
        --transport tcp \
        --endpoints "127.0.0.1:${port0},127.0.0.1:${port1}" \
        --mode vsim-key-key \
        --vsim-key2-owner remote \
        --dim "$DIM" \
        --prefill "$PREFILL" \
        --ops "$OPS" \
        --threads 1 \
        --pipeline 1 \
        --timeout-ms 5000 > "$bench_log" 2>&1

    if ! grep -q "key2_source=${expected_source}" "$log0" "$log1"; then
        echo "remote VSIM smoke failed: case=${case_name} expected key2_source=${expected_source}" >&2
        echo "logs kept at: $TMPDIR" >&2
        exit 1
    fi

    cleanup_servers
    echo "remote VSIM smoke case passed: ${case_name}"
}

make -C "$ROOT/src" vemb_v16_server
make -C "$ROOT/benchmark" vemb_v16_bench

run_case "hit" "yes" 64 4 2
run_case "miss_rpc" "no" 64 4 3
run_case "conflict_rpc" "yes" 1 1 3

echo "remote VSIM smoke passed"
echo "logs: $TMPDIR"
