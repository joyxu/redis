#!/bin/bash
set -e

SNIFF_PORT=6379
DIM=${VEMB_DIM:-300}
HOST=${VEMB_HOST:-127.0.0.1}
PORT=${VEMB_PORT:-$SNIFF_PORT}
MEMTIER_DIR=/root/gqs/codespace/UnifiedBus/memtier_benchmark

# 确保server在运行
if ! ss -tlnp | grep -q ":$SNIFF_PORT"; then
    echo "redis-server not running, starting..."
    pkill -f redis-server 2>/dev/null || true
    sleep 1
    cd /root/gqs/codespace/UnifiedBus/hpc-redis
    ./src/redis-server \
      --port 6379 --vemb-v16-enabled yes --vemb-v16-dim 300 \
      --vemb-v16-warm-regions-manifest /root/gqs/codespace/UnifiedBus/hpc-redis/examples/vemb_v16_warm_regions_111.yaml \
      --vemb-v16-proxy-io-threads 32 \
      --vemb-v16-supernode-workers 64 \
      --daemonize yes --loglevel notice
    sleep 3
fi

cd "$MEMTIER_DIR"

echo "========================================"
echo "  VEMB V16 VSIM memtier multi-client"
echo "  Port: $PORT (sniff-only, no 6391)"
echo "========================================"

run_test() {
    local name="$1"
    shift
    echo ""
    echo "--- $name ---"
    ./memtier_benchmark "$@" 2>&1 | tee "/tmp/memtier_vsim_${name}.log" | awk '
        /^Gets/ || /^Totals/ {
            printf "  %-22s | OPS: %12s | Avg: %8s ms | p50: %8s ms | p99: %8s ms | p99.9: %8s ms | BW: %10s KB/s\n",
                type, $2, $5, $6, $7, $8, $9
        }
    ' type="$name"
}

# Prefill: write vectors first so queries can hit
run_test "prefill_64_keys" \
    --protocol vemb_v16 --vemb-v16-dim "$DIM" \
    -s "$HOST" -p "$PORT" --test-time 5 -c 1 -t 1 --ratio=1:0 \
    --key-prefix="item:" --key-minimum=1 --key-maximum=64

run_test "1_conn_baseline" \
    --protocol vemb_v16 --vemb-v16-dim "$DIM" --vemb-v16-vsim \
    -s "$HOST" -p "$PORT" --test-time 5 -c 1 -t 1 --ratio=0:1 \
    --key-prefix="item:" --key-minimum=1 --key-maximum=64

run_test "8_conn_pipeline16" \
    --protocol vemb_v16 --vemb-v16-dim "$DIM" --vemb-v16-vsim \
    -s "$HOST" -p "$PORT" --test-time 5 -c 1 -t 8 --ratio=0:1 --pipeline=16 \
    --key-prefix="item:" --key-minimum=1 --key-maximum=64

run_test "16_conn_pipeline32" \
    --protocol vemb_v16 --vemb-v16-dim "$DIM" --vemb-v16-vsim \
    -s "$HOST" -p "$PORT" --test-time 5 -c 1 -t 16 --ratio=0:1 --pipeline=32 \
    --key-prefix="item:" --key-minimum=1 --key-maximum=64

run_test "32_conn_pipeline32" \
    --protocol vemb_v16 --vemb-v16-dim "$DIM" --vemb-v16-vsim \
    -s "$HOST" -p "$PORT" --test-time 5 -c 1 -t 32 --ratio=0:1 --pipeline=32 \
    --key-prefix="item:" --key-minimum=1 --key-maximum=64

echo ""
echo "Done"
