#!/bin/bash
# warm region 容量边界回归【HW01 执行】
#
# 验证写入分配的容量感知行为:
#   场景1 (溢出承接): region1 1MB(819 slot) + region2 4MB(3276 slot), 写 2000 key。
#          单 region1 装不下, 总量 4095 装得下 -> 读应基本全命中(丢 key=分配器未溢出)。
#   场景2 (容量不足): 单 region 1MB(819 slot), 写 2000 key -> 读应大量 miss。
#
# 断言用比例而非精确计数, 容忍哈希分布与重试噪声:
#   场景1 hits_ratio >= 0.99    场景2 miss_ratio >= 0.30 (理论 ~0.59)
#
# 用法: bash benchmark/warm_region_capacity_test.sh
set -u

HPC=${HPC:-/root/gqs/codespace/UnifiedBus/hpc-redis}
REDIS=$HPC/src/redis-server
MEMTIER=$HPC/memtier_benchmark/memtier_benchmark
PORT=${PORT:-16390}
DIM=300
KEYS=2000
BASE=/tmp/warm_capacity_test
PIO=${PIO:-2}
SNW=${SNW:-2}

mkdir -p "$BASE"
PASS_COUNT=0
FAIL_COUNT=0

MANIFEST_OVERFLOW=$HPC/examples/vemb_v16_warm_regions_111_cap_overflow.yaml
MANIFEST_INSUFFICIENT=$HPC/examples/vemb_v16_warm_regions_111_cap_insufficient.yaml

start_server() {
    local manifest=$1
    taskset -c 0-47 "$REDIS" \
        --port $PORT --bind 0.0.0.0 --protected-mode no \
        --vemb-v16-enabled yes --vemb-v16-dim $DIM \
        --vemb-v16-warm-regions-manifest "$manifest" \
        --vemb-v16-reset-warm-regions yes \
        --vemb-v16-proxy-io-threads $PIO \
        --vemb-v16-supernode-workers $SNW \
        --appendonly no --save '' \
        --dir $BASE --logfile $BASE/server.log --loglevel warning \
        --daemonize yes --pidfile $BASE/server.pid >/dev/null 2>&1
    for _ in $(seq 1 40); do ss -tln | grep -q ":$PORT " && return 0; sleep 0.25; done
    echo "FAIL: server (manifest=$manifest) not listening"; return 1
}

stop_server() {
    if [ -s $BASE/server.pid ]; then
        local p=$(cat $BASE/server.pid 2>/dev/null)
        [ -n "$p" ] && { kill "$p" 2>/dev/null; sleep 0.5; kill -9 "$p" 2>/dev/null; }
        rm -f $BASE/server.pid
    fi
}

# 写 + 读, 输出读轮 "hits misses"
run_workload() {
    "$MEMTIER" --protocol vemb_v16 --vemb-v16-dim $DIM \
        -s 127.0.0.1 -p $PORT -t 1 -c 1 -n $KEYS --pipeline=32 \
        --ratio=1:0 --key-pattern=S:S \
        --key-prefix=item: --key-minimum=1 --key-maximum=$KEYS \
        --hide-histogram >$BASE/write.out 2>&1
    "$MEMTIER" --protocol vemb_v16 --vemb-v16-dim $DIM \
        -s 127.0.0.1 -p $PORT -t 1 -c 1 -n $KEYS --pipeline=32 \
        --ratio=0:1 --key-pattern=R:R \
        --key-prefix=item: --key-minimum=1 --key-maximum=$KEYS \
        --hide-histogram >$BASE/read.out 2>&1
    awk '/^Gets/{print $3, $4}' $BASE/read.out | tail -1
}

check_scenario() {
    local name=$1 manifest=$2 hits_ratio_min=$3
    local miss_ratio_min=$4
    stop_server
    start_server "$manifest" || { echo "[$name] FAIL: server start"; FAIL_COUNT=$((FAIL_COUNT+1)); return; }
    local out=$(run_workload)
    local hits=${out%% *} misses=${out##* }
    if [ -z "$hits" ] || [ -z "$misses" ]; then
        echo "[$name] FAIL: no read stats (out='$out', see $BASE/read.out)"
        FAIL_COUNT=$((FAIL_COUNT+1)); stop_server; return
    fi
    local hits_ratio=$(awk -v h="$hits" -v m="$misses" 'BEGIN{t=h+m; if(t<=0) print -1; else printf "%.4f", h/t}')
    local miss_ratio=$(awk -v r="$hits_ratio" 'BEGIN{printf "%.4f", (r<0)?-1:1-r}')
    local verdict
    verdict=$(awk -v hr="$hits_ratio" -v hmin="$hits_ratio_min" -v mmin="$miss_ratio_min" \
        'BEGIN{if (hr>=0 && hr>=hmin && 1-hr>=mmin) print "PASS"; else print "FAIL"}')
    echo "[$name] $verdict: hits=$hits misses=$misses hits_ratio=$hits_ratio (need >= $hits_ratio_min)"
    if [ "$verdict" = PASS ]; then PASS_COUNT=$((PASS_COUNT+1)); else FAIL_COUNT=$((FAIL_COUNT+1)); fi
    stop_server
}

trap stop_server EXIT

echo "== warm region capacity test (KEYS=$KEYS, PORT=$PORT) =="
# 场景1: 溢出承接 — 1MB(819 slot) + 4MB(3276 slot), 总量足够, 读基本全命中
check_scenario overflow "$MANIFEST_OVERFLOW" 0.99 0
# 场景2: 容量不足 — 单 1MB(819 slot), 写 2000 key, 读大量 miss
check_scenario insufficient "$MANIFEST_INSUFFICIENT" 0 0.30

echo "== summary: PASS=$PASS_COUNT FAIL=$FAIL_COUNT =="
[ "$FAIL_COUNT" -eq 0 ]
