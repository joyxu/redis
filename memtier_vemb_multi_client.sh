#!/bin/bash
set -e

SNIFF_PORT=6379
DIM=${VEMB_DIM:-300}
HOST=${VEMB_HOST:-127.0.0.1}
PORT=${VEMB_PORT:-$SNIFF_PORT}
MEMTIER_DIR=/root/gqs/codespace/UnifiedBus/memtier_benchmark
CODE_DIR=/root/gqs/codespace/UnifiedBus/hpc-redis
NUM_KEYS=10000
KEY_PREFIX="item:"
MANIFEST="$CODE_DIR/examples/vemb_v16_warm_regions_111.yaml"

# 确保server在运行
if ! ss -tlnp | grep -q ":$SNIFF_PORT"; then
    echo "redis-server not running, starting..."
    pkill -f redis-server 2>/dev/null || true
    sleep 1
    cd "$CODE_DIR"
    ./src/redis-server \
      --port 6379 --bind 0.0.0.0 --protected-mode no \
      --vemb-v16-enabled yes --vemb-v16-dim 300 \
      --vemb-v16-warm-regions-manifest "$MANIFEST" \
      --vemb-v16-reset-warm-regions yes \
      --vemb-v16-proxy-io-threads 32 \
      --vemb-v16-supernode-workers 64 \
      --daemonize yes --loglevel notice
    sleep 3
fi

REDIS_CLI="$CODE_DIR/src/redis-cli"
[ -x "$REDIS_CLI" ] || REDIS_CLI="$CODE_DIR/output/src/redis-cli"

# 清空旧数据，保证每次测试基线一致
"$REDIS_CLI" -p "$SNIFF_PORT" FLUSHDB

cd "$MEMTIER_DIR"

echo "========================================"
echo "  VEMB V16 memtier multi-client"
echo "  Port: $PORT (sniff-only, no 6391)"
echo "========================================"

run_test() {
    local name="$1"
    shift
    echo ""
    echo "--- $name ---"
    ./memtier_benchmark "$@" 2>&1 | tee "/tmp/memtier_vemb_${name}.log" | awk '
        /^Gets/ || /^Totals/ {
            printf "  %-22s | OPS: %12s | Avg: %8s ms | p50: %8s ms | p99: %8s ms | p99.9: %8s ms | BW: %10s KB/s\n",
                type, $2, $5, $6, $7, $8, $9
        }
    ' type="$name"
}

# Prefill: 按数量写入，确保 1..NUM_KEYS 都存在
run_test "prefill_${NUM_KEYS}_keys" \
    --protocol vemb_v16 --vemb-v16-dim "$DIM" \
    -s "$HOST" -p "$PORT" -n "$NUM_KEYS" -c 1 -t 1 --ratio=1:0 \
    --key-pattern=S:S --key-prefix="$KEY_PREFIX" --key-minimum=1 --key-maximum="$NUM_KEYS"

run_test "1_conn_baseline" \
    --protocol vemb_v16 --vemb-v16-dim "$DIM" \
    -s "$HOST" -p "$PORT" --test-time 5 -c 1 -t 1 --ratio=0:1 \
    --key-prefix="$KEY_PREFIX" --key-minimum=1 --key-maximum="$NUM_KEYS"

run_test "8_conn_pipeline16" \
    --protocol vemb_v16 --vemb-v16-dim "$DIM" \
    -s "$HOST" -p "$PORT" --test-time 5 -c 1 -t 8 --ratio=0:1 --pipeline=16 \
    --key-prefix="$KEY_PREFIX" --key-minimum=1 --key-maximum="$NUM_KEYS"

run_test "16_conn_pipeline32" \
    --protocol vemb_v16 --vemb-v16-dim "$DIM" \
    -s "$HOST" -p "$PORT" --test-time 5 -c 1 -t 16 --ratio=0:1 --pipeline=32 \
    --key-prefix="$KEY_PREFIX" --key-minimum=1 --key-maximum="$NUM_KEYS"

run_test "32_conn_pipeline32" \
    --protocol vemb_v16 --vemb-v16-dim "$DIM" \
    -s "$HOST" -p "$PORT" --test-time 5 -c 1 -t 32 --ratio=0:1 --pipeline=32 \
    --key-prefix="$KEY_PREFIX" --key-minimum=1 --key-maximum="$NUM_KEYS"

echo ""
echo "Done"
