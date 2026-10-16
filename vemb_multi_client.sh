#!/bin/bash
set -e

CODE_DIR=/root/gqs/codespace/UnifiedBus/hpc-redis
SNIFF_PORT=6379
VEMB_PORT=${VEMB_PORT:-$SNIFF_PORT}
DIM=${VEMB_DIM:-300}
MANIFEST="$CODE_DIR/examples/vemb_v16_warm_regions_111.yaml"

cd "$CODE_DIR"

# 确保server在运行（总是重启，避免UB warm region stale state导致VEMB dataplane fallback）
if ss -tlnp | grep -q ":$SNIFF_PORT"; then
    echo "Stopping existing redis-server..."
    pkill -f redis-server 2>/dev/null || true
    sleep 2
fi
if true; then
    echo "Starting fresh redis-server..."
    pkill -f redis-server 2>/dev/null || true
    sleep 1
    rm -rf /tmp/redis-vemb-bench
    mkdir -p /tmp/redis-vemb-bench
    rm -f /tmp/redis_vemb_bench_server.log
    ./src/redis-server \
      --port 6379 --bind 0.0.0.0 --protected-mode no \
      --dir /tmp/redis-vemb-bench --save '' --appendonly no \
      --vemb-v16-enabled yes --vemb-v16-dim "$DIM" \
      --vemb-v16-warm-regions-manifest "$MANIFEST" \
      --vemb-v16-reset-warm-regions yes \
      --vemb-v16-proxy-io-threads 32 \
      --vemb-v16-supernode-workers 64 \
      --daemonize yes --loglevel notice \
      --logfile /tmp/redis_vemb_bench_server.log \
    sleep 4
fi

# 预填充（通过 fast path 逐个写入，避免 RESP pipe 在 sniff 模式下走不通 internal stc 的问题）
echo "Prefilling if needed..."
VEC_CSV=$(seq -s ',' 1 "$DIM" | sed 's/[0-9]*/0.1/g')
rm -rf /tmp/vemb_prefill_files
mkdir -p /tmp/vemb_prefill_files
for k in $(seq 1 32); do
    (
        for i in $(seq 1 3125); do
            echo "VADD item:$k elem$i $VEC_CSV"
        done > /tmp/vemb_prefill_files/set_$k.txt
        ./output/src/redis-cli --vemb-v16-dim "$DIM" < /tmp/vemb_prefill_files/set_$k.txt >/dev/null 2>&1
    ) &
done
wait
sleep 1

echo ""
echo "=========================================="
echo "  VEMB V16 TCP Fast Path Multi-Client"
echo "  Port: $VEMB_PORT (sniff-only, no 6391)"
echo "=========================================="
echo ""

for N in 1 4 8 16 32 64; do
    echo "--- $N clients ---"
    rm -f /tmp/vemb_client_*.time

    for i in $(seq 1 $N); do
        set_id=$(( (i - 1) % 32 + 1 ))
        elem_id=$((i % 1000 + 1))
        (
            { time ./output/src/redis-cli --vemb-v16-dim "$DIM" \
                -r 200000 VEMB item:${set_id} elem${elem_id} >/dev/null 2>&1; } \
            2>/tmp/vemb_client_${i}.time
        ) &
    done
    wait

    # 解析所有客户端的real time，取最慢的
    max_real=0
    for i in $(seq 1 $N); do
        real_sec=$(cat /tmp/vemb_client_${i}.time 2>/dev/null | grep real | awk '{print $2}' | sed 's/m/*60+/;s/s//' | bc -l 2>/dev/null || echo 0)
        if [ "$(echo "$real_sec > $max_real" | bc -l 2>/dev/null || echo 0)" = "1" ]; then
            max_real=$real_sec
        fi
    done

    total_ops=$((N * 200000))
    # 避免除0
    if [ "$(echo "$max_real > 0" | bc -l 2>/dev/null || echo 0)" = "1" ]; then
        qps=$(echo "scale=2; $total_ops / $max_real" | bc -l)
        avg_ns=$(echo "scale=2; $max_real * 1000000000 / $total_ops" | bc -l)
        printf "  Clients: %2d | Total ops: %8d | Max real: %.3fs | OPS: %12s | Avg latency: %10s ns/op\n" \
            $N $total_ops $max_real "$qps" "$avg_ns"
    else
        echo "  Clients: $N | ERROR: could not parse time"
    fi
    echo ""
done

echo "Done"
