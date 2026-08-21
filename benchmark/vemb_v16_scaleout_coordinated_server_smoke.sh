#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER="$ROOT/src/vemb_v16_server"
BENCH="$ROOT/benchmark/vemb_v16_bench"
CTL="$ROOT/benchmark/vemb_v16_topology_ctl"
BASE_PORT="${VEMB_V16_SCALEOUT_SMOKE_BASE_PORT:-$((29600 + $$ % 2000))}"
COORD_PORT=$((BASE_PORT + 10))
DIM="${VEMB_V16_SCALEOUT_SMOKE_DIM:-4}"
PREFILL="${VEMB_V16_SCALEOUT_SMOKE_PREFILL:-256}"
OPS="${VEMB_V16_SCALEOUT_SMOKE_OPS:-96}"
MIGRATION_EPOCH="${VEMB_V16_SCALEOUT_SMOKE_MIGRATION_EPOCH:-23}"
CUTOVER_EPOCH=$((MIGRATION_EPOCH + 1))
LIVE_WRITE="${VEMB_V16_SCALEOUT_LIVE_WRITE:-0}"
LIVE_MODE="${VEMB_V16_SCALEOUT_LIVE_MODE:-vadd}"
LIVE_OPS="${VEMB_V16_SCALEOUT_LIVE_OPS:-50000}"
LIVE_THREADS="${VEMB_V16_SCALEOUT_LIVE_THREADS:-2}"
LIVE_TIMEOUT_MS="${VEMB_V16_SCALEOUT_LIVE_TIMEOUT_MS:-60000}"
TMPDIR="${TMPDIR:-/tmp}/vemb_v16_scaleout_server_smoke_$$"
SUCCESS=0

mkdir -p "$TMPDIR"

PIDS=()
PORTS=()
COORD_PID=""
LIVE_PID=""

cleanup() {
    if [[ -n "${COORD_PID:-}" ]] && kill -0 "$COORD_PID" 2>/dev/null; then
        kill "$COORD_PID" 2>/dev/null || true
        wait "$COORD_PID" 2>/dev/null || true
    fi
    if [[ -n "${LIVE_PID:-}" ]] && kill -0 "$LIVE_PID" 2>/dev/null; then
        kill "$LIVE_PID" 2>/dev/null || true
        wait "$LIVE_PID" 2>/dev/null || true
    fi
    for pid in "${PIDS[@]-}"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    if [[ "$SUCCESS" == "1" ]]; then
        rm -rf "$TMPDIR"
    else
        printf '[info] coordinated scaleout server smoke logs kept at: %s\n' "$TMPDIR" >&2
    fi
}
trap cleanup EXIT

payload_path() {
    local case_name="$1"
    local owner="$2"
    printf '/v16%sp%s' "$case_name" "$owner"
}

remote_meta_path() {
    local case_name="$1"
    local owner="$2"
    printf '/v16%sm%s' "$case_name" "$owner"
}

rpc_req_path() {
    local case_name="$1"
    local src="$2"
    local dst="$3"
    printf '/v16%sq%s%s' "$case_name" "$src" "$dst"
}

rpc_resp_path() {
    local case_name="$1"
    local src="$2"
    local dst="$3"
    printf '/v16%sr%s%s' "$case_name" "$src" "$dst"
}

write_manifest() {
    local manifest="$1"
    local owner="$2"
    local case_name="$3"
    local value_size=$((DIM * 4))
    local bytes=$((value_size * 1024))
    local rm_path
    rm_path="$(remote_meta_path "$case_name" "$owner")"

    {
        printf 'local_ub_node_id: %s\n' "$owner"
        printf 'local_region_weight: 4\n'
        printf 'remote_meta_provider: shm\n'
        printf 'remote_meta_path: %s\n' "$rm_path"
        printf 'remote_meta_sets: 128\n'
        printf 'remote_meta_ways: 4\n'
        printf 'ub_rpc_timeout_ms: 200\n'
        printf 'warm_regions:\n'
        for peer in 0 1 2; do
            printf '  - region_id: %s\n' $((100 + peer))
            printf '    provider: shm\n'
            printf '    path: %s\n' "$(payload_path "$case_name" "$peer")"
            printf '    mmap_offset: 0\n'
            printf '    bytes: %s\n' "$bytes"
            printf '    value_size: %s\n' "$value_size"
            printf '    home_ub_node_id: %s\n' "$peer"
            printf '    weight: 1\n'
        done
        printf 'remote_meta_views:\n'
        for peer in 0 1 2; do
            if [[ "$peer" == "$owner" ]]; then
                continue
            fi
            printf '  - owner_id: %s\n' "$peer"
            printf '    provider: shm\n'
            printf '    path: %s\n' "$(remote_meta_path "$case_name" "$peer")"
            printf '    sets: 128\n'
            printf '    ways: 4\n'
        done
        printf 'ub_rpc_peers:\n'
        for peer in 0 1 2; do
            if [[ "$peer" == "$owner" ]]; then
                continue
            fi
            printf '  - owner_id: %s\n' "$peer"
            printf '    provider: shm\n'
            printf '    request_path: %s\n' "$(rpc_req_path "$case_name" "$owner" "$peer")"
            printf '    response_path: %s\n' "$(rpc_resp_path "$case_name" "$peer" "$owner")"
            printf '    inbound_request_path: %s\n' "$(rpc_req_path "$case_name" "$peer" "$owner")"
            printf '    outbound_response_path: %s\n' "$(rpc_resp_path "$case_name" "$owner" "$peer")"
        done
    } > "$manifest"
}

wait_for_node() {
    local node="$1"
    local port="$2"
    for _ in $(seq 1 80); do
        if "$CTL" --get --transport tcp --host 127.0.0.1 --port "$port" \
            --timeout-ms 1000 > "$TMPDIR/topology_get_ready_${node}.out" 2>&1; then
            printf '[ok] node %s ready on port %s\n' "$node" "$port"
            return 0
        fi
        sleep 0.2
    done
    printf '[fail] node %s did not become ready on port %s\n' "$node" "$port" >&2
    cat "$TMPDIR/node_${node}.log" >&2 || true
    return 1
}

wait_for_coordinator() {
    python3 - "$COORD_PORT" <<'PY'
import socket
import sys
import time

port = int(sys.argv[1])
deadline = time.time() + 10
while time.time() < deadline:
    try:
        sock = socket.create_connection(("127.0.0.1", port), timeout=0.2)
        sock.close()
        raise SystemExit(0)
    except OSError:
        time.sleep(0.05)
raise SystemExit(1)
PY
}

owner_endpoints() {
    printf '0=127.0.0.1:%s,1=127.0.0.1:%s,2=127.0.0.1:%s' \
        "${PORTS[0]}" "${PORTS[1]}" "${PORTS[2]}"
}

publish_initial_topology() {
    local node="$1"
    local port="$2"
    "$CTL" \
        --set \
        --transport tcp \
        --host 127.0.0.1 \
        --port "$port" \
        --epoch 1 \
        --min-write-epoch 1 \
        --active 0,1 \
        --standby 0,1 \
        --owner-endpoints "0=127.0.0.1:${PORTS[0]},1=127.0.0.1:${PORTS[1]}" \
        --timeout-ms 5000 > "$TMPDIR/topology_initial_${node}.out"
    if ! grep -q '^status=0$' "$TMPDIR/topology_initial_${node}.out"; then
        printf '[fail] initial topology publish failed for node %s\n' "$node" >&2
        cat "$TMPDIR/topology_initial_${node}.out" >&2
        return 1
    fi
}

publish_candidate_topology() {
    local node="$1"
    local port="$2"
    "$CTL" \
        --set \
        --transport tcp \
        --host 127.0.0.1 \
        --port "$port" \
        --epoch "$MIGRATION_EPOCH" \
        --min-write-epoch "$MIGRATION_EPOCH" \
        --active 0,1 \
        --standby 0,1,2 \
        --dual-write \
        --auto-scaleout \
        --coordinated-scaleout \
        --owner-endpoints "$(owner_endpoints)" \
        --coordinator-endpoint "127.0.0.1:${COORD_PORT}" \
        --timeout-ms 5000 > "$TMPDIR/topology_candidate_${node}.out"
    if ! grep -q '^status=0$' "$TMPDIR/topology_candidate_${node}.out"; then
        printf '[fail] candidate topology publish failed for node %s\n' "$node" >&2
        cat "$TMPDIR/topology_candidate_${node}.out" >&2
        return 1
    fi
}

assert_full_active_topology() {
    local node="$1"
    local port="$2"
    local out="$TMPDIR/topology_final_${node}.out"
    "$CTL" --get --transport tcp --host 127.0.0.1 --port "$port" \
        --timeout-ms 5000 > "$out"
    if ! grep -q '^status=0$' "$out" ||
        ! grep -q "^current_topology_epoch=${CUTOVER_EPOCH}$" "$out" ||
        ! grep -q "^min_write_epoch=${CUTOVER_EPOCH}$" "$out" ||
        ! grep -q '^active_owners=0,1,2$' "$out" ||
        ! grep -q '^standby_owners=0,1,2$' "$out" ||
        ! grep -q '^endpoint_count=3$' "$out"; then
        printf '[fail] node %s did not converge to full-active topology\n' "$node" >&2
        cat "$out" >&2
        return 1
    fi
}

assert_source_migrated() {
    local node="$1"
    local log="$TMPDIR/node_${node}.log"
    if ! grep -Eq "vemb_v16 migration auto plan: local_owner=${node} .*marked=[1-9]" "$log"; then
        printf '[fail] expected node %s to mark at least one migrating key\n' "$node" >&2
        cat "$log" >&2
        return 1
    fi
    if ! grep -q "vemb_v16 scaleout auto done: local_owner=${node} migration_epoch=${MIGRATION_EPOCH} cutover_epoch=${CUTOVER_EPOCH}" "$log"; then
        printf '[fail] expected node %s to finish local scaleout state machine\n' "$node" >&2
        cat "$log" >&2
        return 1
    fi
}

start_live_workload() {
    if [[ "$LIVE_WRITE" != "1" ]]; then
        return 0
    fi
    local out="$TMPDIR/live_write.out"
    "$BENCH" \
        --transport tcp \
        --endpoints "127.0.0.1:${PORTS[0]}" \
        --dim "$DIM" \
        --prefill 0 \
        --keyspace "$PREFILL" \
        --ops "$LIVE_OPS" \
        --threads "$LIVE_THREADS" \
        --pipeline 1 \
        --mode "$LIVE_MODE" \
        --timeout-ms "$LIVE_TIMEOUT_MS" > "$out" 2>&1 &
    LIVE_PID="$!"
    sleep 0.3
    if ! kill -0 "$LIVE_PID" 2>/dev/null; then
        wait "$LIVE_PID" 2>/dev/null || true
        printf '[fail] live workload exited before candidate publish\n' >&2
        cat "$out" >&2
        return 1
    fi
    printf '[ok] live workload started mode=%s ops=%s threads=%s\n' \
        "$LIVE_MODE" "$LIVE_OPS" "$LIVE_THREADS"
}

wait_live_workload() {
    if [[ "$LIVE_WRITE" != "1" ]]; then
        return 0
    fi
    local out="$TMPDIR/live_write.out"
    if ! wait "$LIVE_PID"; then
        LIVE_PID=""
        printf '[fail] live workload exited with error\n' >&2
        cat "$out" >&2
        return 1
    fi
    LIVE_PID=""
    local fail_count
    fail_count="$(sed -n 's/.* fail=\([0-9][0-9]*\) .*/\1/p' "$out" | tail -1)"
    if [[ "${fail_count:-}" != "0" ]]; then
        printf '[fail] live workload reported failures\n' >&2
        cat "$out" >&2
        return 1
    fi
    if ! grep -q '^\[client-topology\]' "$out"; then
        printf '[fail] live workload did not run with client topology\n' >&2
        cat "$out" >&2
        return 1
    fi
    printf '[ok] live workload completed fail=0\n'
}

printf '[build] src/vemb_v16_server\n'
make -C "$ROOT/src" vemb_v16_server >/dev/null
printf '[build] benchmark/vemb_v16_bench\n'
make -C "$ROOT/benchmark" vemb_v16_bench >/dev/null
printf '[build] benchmark/vemb_v16_topology_ctl\n'
make -C "$ROOT/benchmark" vemb_v16_topology_ctl >/dev/null

CASE_NAME="s$$"
for node in 0 1 2; do
    write_manifest "$TMPDIR/node_${node}.yaml" "$node" "$CASE_NAME"
done

for node in 0 1 2; do
    port=$((BASE_PORT + node))
    PORTS+=("$port")
    server_args=(
        "$SERVER"
        --transport tcp
        --tcp-host 127.0.0.1
        --tcp-port "$port"
        --proxy-io-threads 1
        --supernode-workers 1
        --warm-regions-manifest "$TMPDIR/node_${node}.yaml"
        --dim "$DIM"
        --max-vectors 1024
        --loglevel notice
    )
    if [[ "$node" == "0" ]]; then
        server_args+=(--reset-warm-regions)
    fi
    "${server_args[@]}" > "$TMPDIR/node_${node}.log" 2>&1 &
    PIDS+=("$!")
    sleep 0.2
done

for node in 0 1 2; do
    wait_for_node "$node" "${PORTS[$node]}"
done

for node in 0 1; do
    publish_initial_topology "$node" "${PORTS[$node]}"
done
printf '[ok] initial active topology published to old sources\n'

PREFILL_OUT="$TMPDIR/prefill.out"
"$BENCH" \
    --transport tcp \
    --endpoints "127.0.0.1:${PORTS[0]}" \
    --dim "$DIM" \
    --prefill "$PREFILL" \
    --ops 0 \
    --threads 1 \
    --pipeline 1 \
    --mode vadd \
    --timeout-ms 10000 > "$PREFILL_OUT" 2>&1
if ! grep -q "\\[prefill\\] inserted=${PREFILL}" "$PREFILL_OUT"; then
    printf '[fail] prefill did not insert expected keys\n' >&2
    cat "$PREFILL_OUT" >&2
    exit 1
fi
printf '[ok] prefilled %s keys through client topology\n' "$PREFILL"

"$CTL" \
    --coordinator-listen \
    --transport tcp \
    --host 127.0.0.1 \
    --port "$COORD_PORT" \
    --expected-sources 0,1 \
    --migration-epoch "$MIGRATION_EPOCH" \
    --cutover-epoch "$CUTOVER_EPOCH" \
    --standby 0,1,2 \
    --owner-endpoints "$(owner_endpoints)" \
    --wait-ms 30000 \
    --timeout-ms 5000 > "$TMPDIR/coordinator.out" 2> "$TMPDIR/coordinator.err" &
COORD_PID="$!"
if ! wait_for_coordinator; then
    printf '[fail] coordinator did not become ready\n' >&2
    cat "$TMPDIR/coordinator.err" >&2 || true
    exit 1
fi
printf '[ok] coordinator listening on port %s\n' "$COORD_PORT"

start_live_workload

publish_candidate_topology 2 "${PORTS[2]}"
publish_candidate_topology 0 "${PORTS[0]}"
publish_candidate_topology 1 "${PORTS[1]}"
printf '[ok] coordinated scaleout candidate published\n'

if ! wait "$COORD_PID"; then
    printf '[fail] coordinator exited with error\n' >&2
    cat "$TMPDIR/coordinator.out" >&2 || true
    cat "$TMPDIR/coordinator.err" >&2 || true
    exit 1
fi
COORD_PID=""
if ! grep -q '^scaleout_all_sources_done=2$' "$TMPDIR/coordinator.out" ||
    ! grep -q '^scaleout_full_active_published=3 errors=0 targets=3$' "$TMPDIR/coordinator.out"; then
    printf '[fail] coordinator did not publish expected full-active topology\n' >&2
    cat "$TMPDIR/coordinator.out" >&2
    exit 1
fi
printf '[ok] coordinator collected both sources and published full-active topology\n'

wait_live_workload

for node in 0 1 2; do
    assert_full_active_topology "$node" "${PORTS[$node]}"
done
assert_source_migrated 0
assert_source_migrated 1
printf '[ok] all nodes converged to active owners 0,1,2\n'

RUN_OUT="$TMPDIR/post_cutover.out"
"$BENCH" \
    --transport tcp \
    --endpoints "127.0.0.1:${PORTS[0]}" \
    --dim "$DIM" \
    --prefill 0 \
    --ops "$OPS" \
    --threads 1 \
    --pipeline 1 \
    --mode vadd \
    --timeout-ms 10000 > "$RUN_OUT" 2>&1

fail_count="$(sed -n 's/.* fail=\([0-9][0-9]*\) .*/\1/p' "$RUN_OUT" | tail -1)"
node2_vadd="$(awk '
    /^\[stats node=2\]/ { in_node = 1; next }
    in_node && /^\[stats\] total=/ {
        for (i = 1; i <= NF; i++) {
            if ($i ~ /^vadd=/) {
                sub(/^vadd=/, "", $i);
                print $i;
                exit;
            }
        }
    }
' "$RUN_OUT")"

if [[ "${fail_count:-}" != "0" ]]; then
    printf '[fail] post-cutover client topology run reported failures\n' >&2
    cat "$RUN_OUT" >&2
    exit 1
fi
if [[ -z "${node2_vadd:-}" || "$node2_vadd" == "0" ]]; then
    printf '[fail] expected node2 to receive direct VADD writes after cutover, got vadd=%s\n' \
        "${node2_vadd:-missing}" >&2
    cat "$RUN_OUT" >&2
    exit 1
fi

printf '[ok] coordinated scaleout server smoke passed node2_vadd=%s\n' "$node2_vadd"
SUCCESS=1
