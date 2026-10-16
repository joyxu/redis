#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER="$ROOT_DIR/src/vemb_v16_server"
BENCH="$ROOT_DIR/benchmark/vemb_v16_bench"
PORT="${VEMB_V16_SMOKE_PORT:-$((26391 + $$ % 10000))}"
TCP_SOCKET="/tmp/vemb_v16_transport_smoke_tcp_$$.sock"
SHM_SOCKET="/tmp/vemb_v16_transport_smoke_aeron_$$.sock"
TCP_REGION="/v16sm_tcp_$$"
SHM_REGION="/v16sm_aeron_$$"
TCP_LOG="$(mktemp "${TMPDIR:-/tmp}/vemb_v16_tcp.XXXXXX.log")"
SHM_LOG="$(mktemp "${TMPDIR:-/tmp}/vemb_v16_shm.XXXXXX.log")"
BOTH_OUT="/tmp/vemb_v16_transport_both_$$.out"
TCP_PID=""
SHM_PID=""

cleanup() {
    if [[ -n "$TCP_PID" ]] && kill -0 "$TCP_PID" 2>/dev/null; then
        kill "$TCP_PID" 2>/dev/null || true
        wait "$TCP_PID" 2>/dev/null || true
    fi
    if [[ -n "$SHM_PID" ]] && kill -0 "$SHM_PID" 2>/dev/null; then
        kill "$SHM_PID" 2>/dev/null || true
        wait "$SHM_PID" 2>/dev/null || true
    fi
    rm -f "$TCP_SOCKET" "$SHM_SOCKET" "$TCP_LOG" "$SHM_LOG" "$BOTH_OUT"
}
trap cleanup EXIT

run_with_retry() {
    local label="$1"
    shift

    for _ in $(seq 1 30); do
        if "$@" >/dev/null 2>&1; then
            printf '[ok] %s\n' "$label"
            return 0
        fi
        sleep 0.2
    done

    printf '[fail] %s\n' "$label" >&2
    if [[ "$label" == tcp* ]]; then
        printf '%s\n' '--- tcp server log ---' >&2
        cat "$TCP_LOG" >&2 || true
    elif [[ "$label" == shm* ]]; then
        printf '%s\n' '--- shm server log ---' >&2
        cat "$SHM_LOG" >&2 || true
    fi
    "$@"
}

printf '[build] src/vemb_v16_server\n'
make -C "$ROOT_DIR/src" vemb_v16_server >/dev/null
printf '[build] benchmark/vemb_v16_bench\n'
make -C "$ROOT_DIR/benchmark" vemb_v16_bench >/dev/null

if "$SERVER" --transport both >"$BOTH_OUT" 2>&1; then
    printf '[fail] --transport both unexpectedly succeeded\n' >&2
    exit 1
fi
if ! grep -q 'invalid transport' "$BOTH_OUT"; then
    printf '[fail] --transport both did not report invalid transport\n' >&2
    cat "$BOTH_OUT" >&2
    exit 1
fi
rm -f "$BOTH_OUT"
printf '[ok] --transport both rejected\n'

"$SERVER" \
    --transport tcp \
    --socket "$TCP_SOCKET" \
    --tcp-host 127.0.0.1 \
    --tcp-port "$PORT" \
    --proxy-io-threads 1 \
    --supernode-workers 1 \
    --vector-region "$TCP_REGION" \
    --warm-backend shm \
    --dim 16 \
    --warm-bytes 65536 \
    --loglevel warning >"$TCP_LOG" 2>&1 &
TCP_PID=$!

run_with_retry "tcp ping" \
    "$BENCH" \
    --transport tcp \
    --host 127.0.0.1 \
    --port "$PORT" \
    --dim 16 \
    --prefill 0 \
    --ops 8 \
    --threads 1 \
    --pipeline 1 \
    --mode ping \
    --timeout-ms 5000

if [[ -S "$TCP_SOCKET" || -e "$TCP_SOCKET" ]]; then
    printf '[fail] tcp mode created uds socket: %s\n' "$TCP_SOCKET" >&2
    exit 1
fi
printf '[ok] tcp mode did not create uds socket\n'

kill "$TCP_PID" 2>/dev/null || true
wait "$TCP_PID" 2>/dev/null || true
TCP_PID=""

"$SERVER" \
    --transport aeron \
    --socket "$SHM_SOCKET" \
    --tcp-host 127.0.0.1 \
    --tcp-port "$PORT" \
    --proxy-io-threads 1 \
    --supernode-workers 1 \
    --vector-region "$SHM_REGION" \
    --warm-backend shm \
    --dim 16 \
    --warm-bytes 65536 \
    --loglevel warning >"$SHM_LOG" 2>&1 &
SHM_PID=$!

run_with_retry "aeron ping" \
    "$BENCH" \
    --transport aeron \
    --socket "$SHM_SOCKET" \
    --dim 16 \
    --prefill 0 \
    --ops 8 \
    --threads 1 \
    --pipeline 1 \
    --mode ping \
    --timeout-ms 5000

run_with_retry "aeron vemb-handle" \
    "$BENCH" \
    --transport aeron \
    --socket "$SHM_SOCKET" \
    --dim 16 \
    --prefill 32 \
    --keyspace 32 \
    --ops 32 \
    --threads 1 \
    --pipeline 1 \
    --mode vemb-handle \
    --timeout-ms 15000 \
    --no-pin

printf '[ok] vemb_v16 transport smoke passed\n'
