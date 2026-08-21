#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER="$ROOT/src/vemb_v16_server"
BENCH="$ROOT/benchmark/vemb_v16_bench"
CTL="$ROOT/benchmark/vemb_v16_topology_ctl"
BASE_PORT="${VEMB_V16_TOPOLOGY_SMOKE_BASE_PORT:-$((27600 + $$ % 3000))}"
DIM="${VEMB_V16_TOPOLOGY_SMOKE_DIM:-4}"
OPS="${VEMB_V16_TOPOLOGY_SMOKE_OPS:-128}"
TMPDIR="${TMPDIR:-/tmp}/vemb_v16_client_topology_smoke_$$"
SUCCESS=0

mkdir -p "$TMPDIR"

PIDS=()
PORTS=()

cleanup() {
    for pid in "${PIDS[@]-}"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    if [[ "$SUCCESS" == "1" ]]; then
        rm -rf "$TMPDIR"
    else
        printf '[info] client topology smoke logs kept at: %s\n' "$TMPDIR" >&2
    fi
}
trap cleanup EXIT

wait_for_node() {
    local node="$1"
    local port="$2"
    for _ in $(seq 1 50); do
        if "$CTL" --get --transport tcp --host 127.0.0.1 --port "$port" \
            --timeout-ms 1000 > "$TMPDIR/topology_get_${node}.out" 2>&1; then
            printf '[ok] node %s ready on port %s\n' "$node" "$port"
            return 0
        fi
        sleep 0.2
    done
    printf '[fail] node %s did not become ready on port %s\n' "$node" "$port" >&2
    cat "$TMPDIR/node_${node}.log" >&2 || true
    return 1
}

publish_topology() {
    local node="$1"
    local port="$2"
    local owner_endpoints="0=127.0.0.1:${PORTS[0]},1=127.0.0.1:${PORTS[1]},2=127.0.0.1:${PORTS[2]}"
    "$CTL" \
        --set \
        --transport tcp \
        --host 127.0.0.1 \
        --port "$port" \
        --epoch 9 \
        --min-write-epoch 9 \
        --active 2 \
        --standby 0,1,2 \
        --owner-endpoints "$owner_endpoints" \
        --timeout-ms 5000 > "$TMPDIR/topology_set_${node}.out"
    if ! grep -q '^active_owners=2$' "$TMPDIR/topology_set_${node}.out" ||
        ! grep -q '^endpoint_count=3$' "$TMPDIR/topology_set_${node}.out"; then
        printf '[fail] node %s did not publish expected direct-owner topology\n' "$node" >&2
        cat "$TMPDIR/topology_set_${node}.out" >&2
        return 1
    fi
    printf '[ok] node %s topology published\n' "$node"
}

printf '[build] src/vemb_v16_server\n'
make -C "$ROOT/src" vemb_v16_server >/dev/null
printf '[build] benchmark/vemb_v16_bench\n'
make -C "$ROOT/benchmark" vemb_v16_bench >/dev/null
printf '[build] benchmark/vemb_v16_topology_ctl\n'
make -C "$ROOT/benchmark" vemb_v16_topology_ctl >/dev/null

for node in 0 1 2; do
    port=$((BASE_PORT + node))
    PORTS+=("$port")
    "$SERVER" \
        --transport tcp \
        --tcp-host 127.0.0.1 \
        --tcp-port "$port" \
        --proxy-io-threads 1 \
        --supernode-workers 1 \
        --vector-region "/v16ct$$_${node}" \
        --warm-backend shm \
        --dim "$DIM" \
        --max-vectors 512 \
        --loglevel warning > "$TMPDIR/node_${node}.log" 2>&1 &
    PIDS+=("$!")
done

for node in 0 1 2; do
    wait_for_node "$node" "${PORTS[$node]}"
done

for node in 0 1 2; do
    publish_topology "$node" "${PORTS[$node]}"
done

BOOTSTRAP_ENDPOINT="127.0.0.1:${PORTS[0]}"
BENCH_OUT="$TMPDIR/bench.out"

"$BENCH" \
    --transport tcp \
    --endpoints "$BOOTSTRAP_ENDPOINT" \
    --dim "$DIM" \
    --prefill 0 \
    --ops "$OPS" \
    --threads 1 \
    --pipeline 1 \
    --mode vadd \
    --timeout-ms 5000 > "$BENCH_OUT" 2>&1

fail_count="$(sed -n 's/.* fail=\([0-9][0-9]*\) .*/\1/p' "$BENCH_OUT" | tail -1)"
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
' "$BENCH_OUT")"

if [[ "${fail_count:-}" != "0" ]]; then
    printf '[fail] bench reported failures\n' >&2
    cat "$BENCH_OUT" >&2
    exit 1
fi

if [[ -z "${node2_vadd:-}" || "$node2_vadd" == "0" ]]; then
    printf '[fail] expected discovered node 2 to receive direct VADD writes, got vadd=%s\n' \
        "${node2_vadd:-missing}" >&2
    cat "$BENCH_OUT" >&2
    exit 1
fi

printf '[ok] client topology smoke passed bootstrap=%s discovered_node2_vadd=%s\n' \
    "$BOOTSTRAP_ENDPOINT" "$node2_vadd"
SUCCESS=1
