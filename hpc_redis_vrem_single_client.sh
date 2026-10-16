#!/bin/bash
# hpc_redis VREM 单 client pipeline 扫描（方法论 A）
#
#   -t 1 -c 1 -n FIXED_N 模式，扫 pipeline 深度。每个 pipeline 点用不重叠 key 区间
#   （分片），保证 100% 命中——前一个 pipeline 点删的 key 不影响后一个。
#   测的是"单连接顺序删除最大吞吐"，反映服务端单请求删除处理能力 × pipeline。
#
#   对比 (B) 多(t,c)混合扫描：(A) 是真删除吞吐（100%命中），(B) 是协议帧混合吞吐。
#
# 用法: PORT=6390 ./hpc_redis_vrem_single_client.sh
#   smoke: WORKERS="1:2" PIPELINES="32" FIXED_N=10000 ./hpc_redis_vrem_single_client.sh
set -uo pipefail

HPC=/root/gqs/codespace/UnifiedBus/hpc-redis
MEMTIER=/root/gqs/codespace/UnifiedBus/memtier_benchmark/memtier_benchmark
MANIFEST=$HPC/examples/vemb_v16_warm_regions_111.yaml
CLEAR_UB=/tmp/clear_ub_device

# CPU 绑核：server 独占 node0(0-95)，memtier 独占 node1(96-191)
SERVER_BIND="numactl --membind=0 taskset -c 0-95"
CLIENT_BIND="numactl --membind=1 taskset -c 96-191"

DIM=300
PREFILL_KEYS=800000
FIXED_N=${FIXED_N:-100000}   # 每 pipeline 点删除量；6 点分片共 6*FIXED_N <= PREFILL_KEYS
KEY_PREFIX="item:"
PORT=${PORT:-6390}
# pipeline 深度扫描
PIPELINES=( ${PIPELINES:-1 8 16 32 64 128} )
# pio snw pairs
WORKERS=( ${WORKERS:-1:2 2:4 4:8 8:16 16:32 32:64} )

OUTDIR=/tmp/hpc_vrem_single_client
RAWDIR=$OUTDIR/raw
mkdir -p "$RAWDIR"
TSV=$OUTDIR/summary.tsv
PIDFILE=/tmp/hpc_vrem_single.pid

printf 'pio\tsnw\tpipeline\tn\tkey_min\tkey_max\tops_sec\thits\tp50_ms\tp99_ms\tcpu_cores\telapsed\n' > "$TSV"
declare -A OPS
BEST_OPS=0; BEST_KEY=""

log() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }

kill_server() {
    [ -f "$PIDFILE" ] && { local p; p=$(cat "$PIDFILE" 2>/dev/null); [ -n "$p" ] && kill "$p" 2>/dev/null; sleep 0.5; kill -9 "$p" 2>/dev/null; rm -f "$PIDFILE"; }
    pkill -9 -f "redis-server.*:$PORT " 2>/dev/null
    sleep 0.5
}
wait_port() {
    for _ in $(seq 1 50); do ss -tln | grep -q ":$PORT " && return 0; sleep 0.2; done
    return 1
}
start_server() {
    local pio=$1 snw=$2
    local logfile=$RAWDIR/pio${pio}_snw${snw}.server.log
    kill_server
    [ -x "$CLEAR_UB" ] && "$CLEAR_UB" >/dev/null 2>&1
    cd "$HPC"
    $SERVER_BIND ./src/redis-server \
        --port $PORT --bind 0.0.0.0 --protected-mode no \
        --vemb-v16-enabled yes --vemb-v16-dim $DIM \
        --vemb-v16-warm-regions-manifest "$MANIFEST" \
        --vemb-v16-reset-warm-regions yes \
        --vemb-v16-proxy-io-threads $pio --vemb-v16-supernode-workers $snw \
        --daemonize yes --pidfile $PIDFILE --logfile "$logfile" --loglevel notice \
        >/dev/null 2>&1
    wait_port || { echo "FAIL: pio=$pio snw=$snw server did not listen"; return 1; }
    SERVER_PID=$(cat "$PIDFILE" 2>/dev/null)
}
prefill() {
    $CLIENT_BIND $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
        -s 127.0.0.1 -p $PORT -t 8 -c 1 --pipeline=32 -n $PREFILL_KEYS \
        --ratio=1:0 --key-pattern=S:S --key-prefix=$KEY_PREFIX \
        --key-minimum=1 --key-maximum=$PREFILL_KEYS >$RAWDIR/prefill.log 2>&1
}
get_cpu_jiffies() {
    local pid=$1 sum=0 f rest
    for f in /proc/$pid/task/*/stat; do
        [ -r "$f" ] || continue
        rest=$(sed 's/.*)//' "$f")
        set -- $rest
        sum=$(( sum + ${12:-0} + ${13:-0} ))
    done
    echo "$sum"
}

# 总分片量检查
total_shard=$(( ${#PIPELINES[@]} * FIXED_N ))
[ "$total_shard" -gt "$PREFILL_KEYS" ] && { echo "FAIL: ${#PIPELINES[@]} pipelines × FIXED_N=$FIXED_N = $total_shard > PREFILL_KEYS=$PREFILL_KEYS"; exit 1; }

log "hpc_redis VREM single-client sweep — ${#WORKERS[@]} worker cfgs × ${#PIPELINES[@]} pipelines = $(( ${#WORKERS[@]}*${#PIPELINES[@]} )) runs (FIXED_N=$FIXED_N, 分片不重叠)"
for w in "${WORKERS[@]}"; do
    IFS=':' read pio snw <<< "$w"
    log "pio=$pio snw=$snw — starting server + prefill $PREFILL_KEYS"
    start_server "$pio" "$snw" || continue
    sleep 1
    prefill
    base=0
    for pl in "${PIPELINES[@]}"; do
        kmin=$((base + 1))
        kmax=$((base + FIXED_N))
        base=$((base + FIXED_N))
        tag=pio${pio}_snw${snw}_pl${pl}
        raw=$RAWDIR/${tag}.txt
        j0=$(get_cpu_jiffies "$SERVER_PID")
        t0=$(date +%s.%N)
        # -n 模式跑完即止；单 client 顺序删 [kmin,kmax]，100% 命中
        timeout 120s $CLIENT_BIND $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM --vemb-v16-vrem \
            -s 127.0.0.1 -p $PORT -t 1 -c 1 --pipeline=$pl \
            -n $FIXED_N --ratio=1:0 --key-pattern=S:S --key-prefix=$KEY_PREFIX \
            --key-minimum=$kmin --key-maximum=$kmax >"$raw" 2>&1
        t1=$(date +%s.%N)
        j1=$(get_cpu_jiffies "$SERVER_PID")
        elapsed=$(awk "BEGIN{printf \"%.3f\", $t1-$t0}")
        # cores 用 true_time=FIXED_N/ops 剔除 memtier 启动开销（elapsed 含 ~0.7s 固定开销）
        true_time=$(awk -v n=$FIXED_N -v o=$ops 'BEGIN{ if(o>0) printf "%.3f", n/o; else print "0"}')
        cores=$(awk -v d=$(( j1 - j0 )) -v t=$true_time 'BEGIN{ if(d<0||t<=0) print "NA"; else printf "%.2f", d/100.0/t }')
        tot=$(grep '^Totals' "$raw" | tail -1)
        ops=$(echo "$tot" | awk '{print $2}')
        hits=$(echo "$tot" | awk '{print $3}')
        p50=$(echo "$tot" | awk '{print $6}')
        p99=$(echo "$tot" | awk '{print $7}')
        [ -z "$ops" ] && ops=0
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$pio" "$snw" "$pl" "$FIXED_N" "$kmin" "$kmax" "$ops" "$hits" "$p50" "$p99" "$cores" "$elapsed" >> "$TSV"
        k="${pio}|${snw}|pl${pl}"
        OPS[$k]=$ops
        awk "BEGIN{exit !($ops > $BEST_OPS)}" && { BEST_OPS=$ops; BEST_KEY="$k"; }
        printf '   pio=%-2s snw=%-2s pl%-3s  ops=%-12s hits=%-10s p50=%-8s cores=%-5s elapsed=%ss\n' \
            "$pio" "$snw" "$pl" "$ops" "$hits" "$p50" "$cores" "$elapsed"
    done
    kill_server
done
kill_server

log "DONE — $TSV"
printf '\n\033[1m── 各 worker 配置峰值 ──\033[0m\n'
for w in "${WORKERS[@]}"; do
    IFS=':' read pio snw <<< "$w"
    lb=0; lk=""
    for k in "${!OPS[@]}"; do
        case "$k" in "${pio}|${snw}|"*) ;; *) continue;; esac
        v=${OPS[$k]}
        awk "BEGIN{exit !($v > $lb)}" && { lb=$v; lk="$k"; }
    done
    [ -n "$lk" ] && printf '  pio=%-2s snw=%-2s best: %s = %s ops/sec\n' "$pio" "$snw" "$lk" "$lb"
done
printf '\n全局最优: %s = %s ops/sec\n' "$BEST_KEY" "$BEST_OPS"
