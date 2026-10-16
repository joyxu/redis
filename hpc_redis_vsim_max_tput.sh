#!/bin/bash
# hpc_redis VSIM 最大吞吐探索：pio:snw=1:2 递增 + memtier -t×-c≤64
#
#   方法论与 hpc_redis_max_tput.sh 完全一致，唯一差异：run_client 加 --vemb-v16-vsim
#   pio:snw 走 (1,2)(2,4)(4,8)(8,16)(16,32)(32,64)，每个 server 配置重启
#   memtier -t×-c 限制 ≤64（避开 VEMB 连接 hang），扫 product=64 前沿
#   floating（不 taskset），prefill 写向量后跑 VSIM 相似度查询
#
# 用法: PORT=6390 ./hpc_redis_vsim_max_tput.sh
#   smoke: WORKERS="32 64" TS="32" CS="1" TEST_TIME=3 ./hpc_redis_vsim_max_tput.sh
set -uo pipefail

HPC=/root/gqs/codespace/UnifiedBus/hpc-redis
MEMTIER=/root/gqs/codespace/UnifiedBus/memtier_benchmark/memtier_benchmark
MANIFEST=$HPC/examples/vemb_v16_warm_regions_111.yaml
CLEAR_UB=/tmp/clear_ub_device

# CPU 绑核：server 独占 node0(0-95)，memtier 独占 node1(96-191)
SERVER_BIND="numactl --membind=0 taskset -c 0-95"
CLIENT_BIND="numactl --membind=1 taskset -c 96-191"

DIM=300
NUM_KEYS=10000
KEY_PREFIX="item:"
PORT=${PORT:-6390}
TEST_TIME=${TEST_TIME:-5}
PIPELINE=32
# pio snw pairs (1:2 ratio up to 32:64); format: "pio:snw ..."
WORKERS=( ${WORKERS:-1:2 2:4 4:8 8:16 16:32 32:64} )
# memtier -t values
TS=( ${TS:-1 1 2 4 8 16 32 32 64} )
# memtier -c values (paired with TS; t[i]*c[i] must be <=64)
CS=( ${CS:-1 64 32 16 8 4 2 1 1} )

OUTDIR=${OUTDIR:-/tmp/hpc_vsim_max_tput}
RAWDIR=$OUTDIR/raw
mkdir -p "$RAWDIR"
TSV=$OUTDIR/summary.tsv
PIDFILE=/tmp/hpc_vsim_max_tput_server.pid

printf 'pio\tsnw\tt\tc\ttxc\tops_sec\thits\tp50_ms\tp99_ms\tcpu_cores\trun_ops\tmem_base_mb\tmem_peak_mb\tmem_avg_mb\n' > "$TSV"
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
    wait_port || { echo "FAIL: pio=$pio snw=$snw server did not listen (see $logfile)"; return 1; }
    SERVER_PID=$(cat "$PIDFILE" 2>/dev/null)
}

prefill() {
    # 写向量（不加 --vemb-v16-vsim），VSIM 查询需要先有向量集合
    $CLIENT_BIND $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
        -s 127.0.0.1 -p $PORT -t 1 -c 1 -n $NUM_KEYS \
        --ratio=1:0 --key-pattern=S:S --key-prefix=$KEY_PREFIX \
        --key-minimum=1 --key-maximum=$NUM_KEYS >$RAWDIR/prefill.log 2>&1
}

# ───────── 汇总某 PID 所有 TID 的 (utime+stime) jiffies ─────────
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

run_client() {
    local pio=$1 snw=$2 t=$3 c=$4
    local tag=pio${pio}_snw${snw}_t${t}_c${c}
    local raw=$RAWDIR/${tag}.txt
    local j0 j1 cores
    local mem_base_mb mem_peak_mb mem_avg_mb mem_samples_file mem_sampler_pid
    j0=$(get_cpu_jiffies "$SERVER_PID")
    # ── 内存采集 - 启动 sampler ──
    mem_base_mb=$(awk '/^VmRSS:/{printf "%.0f", $2/1024}' /proc/$SERVER_PID/status 2>/dev/null)
    echo 1 > /proc/$SERVER_PID/clear_refs 2>/dev/null || true
    mem_samples_file=/tmp/mem_samples_vsim_${tag}.log
    rm -f "$mem_samples_file"
    ( while kill -0 "$SERVER_PID" 2>/dev/null; do
        awk '/^VmRSS:/{print $2}' /proc/$SERVER_PID/status 2>/dev/null
        sleep 0.5
      done > "$mem_samples_file" ) &
    mem_sampler_pid=$!
    # VSIM: 加 --vemb-v16-vsim 走相似度查询，其余与 VEMB 读完全一致
    #   VSIM 协议下 memtier 有 bug：跑完 --test-time 不退出（卡在 RUN #1 100%+ 循环），
    #   必须 timeout 兜底。timeout = TEST_TIME + 30s grace；raw 文件里 RUN #1 100% 行
    #   由后续 grep 兜底解析。
    timeout $((TEST_TIME + 30))s $CLIENT_BIND $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM --vemb-v16-vsim \
        -s 127.0.0.1 -p $PORT -t $t -c $c --pipeline=$PIPELINE \
        --ratio=0:1 --key-pattern=R:R --key-prefix=$KEY_PREFIX \
        --key-minimum=1 --key-maximum=$NUM_KEYS --test-time=$TEST_TIME >"$raw" 2>&1
    j1=$(get_cpu_jiffies "$SERVER_PID")
    # ── 内存采集 - 停止 sampler + 读结果 ──
    kill "$mem_sampler_pid" 2>/dev/null || true
    wait "$mem_sampler_pid" 2>/dev/null || true
    sleep 0.2
    mem_peak_mb=$(awk '/^VmHWM:/{printf "%.0f", $2/1024}' /proc/$SERVER_PID/status 2>/dev/null)
    mem_avg_mb=$(awk '{s+=$1;n++} END{if(n>0) printf "%.0f", s/n/1024}' "$mem_samples_file" 2>/dev/null)
    rm -f "$mem_samples_file"
    # 高并发时 server 线程 churn (连接线程退出) 会丢 jiffies → delta 负; clamp 成 NA
    cores=$(awk -v d=$(( j1 - j0 )) -v tt=$TEST_TIME 'BEGIN{ if(d<0) print "NA"; else printf "%.2f", d/100.0/tt }')
    local tot; tot=$(grep '^Totals' "$raw" | tail -1)
    local ops hits p50 p99
    ops=$(echo "$tot" | awk '{print $2}')
    hits=$(echo "$tot" | awk '{print $3}')
    p50=$(echo "$tot" | awk '{print $6}')
    p99=$(echo "$tot" | awk '{print $7}')
    local runops; runops=$(grep 'RUN #1 100%' "$raw" | grep -oE 'avg: *[0-9.,]+' | head -1 | grep -oE '[0-9.,]+')
    [ -z "$ops" ] && ops=0; [ -z "$runops" ] && runops=0
    [ -z "$mem_base_mb" ] && mem_base_mb=0; [ -z "$mem_peak_mb" ] && mem_peak_mb=0; [ -z "$mem_avg_mb" ] && mem_avg_mb=0
    local eff=$ops
    awk "BEGIN{exit !($ops < 1)}" && [ "$runops" != 0 ] && eff=$runops
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$pio" "$snw" "$t" "$c" "$((t*c))" "$ops" "$hits" "$p50" "$p99" "$cores" "$runops" \
        "$mem_base_mb" "$mem_peak_mb" "$mem_avg_mb" >> "$TSV"
    local k="${pio}|${snw}|t${t}|c${c}"
    OPS[$k]=$eff
    awk "BEGIN{exit !($eff > $BEST_OPS)}" && { BEST_OPS=$eff; BEST_KEY="$k"; }
    printf '   pio=%-2s snw=%-2s t%-2s c%-2s  ops=%-12s hits=%-12s p50=%-8s cores=%-5s  mem=%s/%sMB\n' \
        "$pio" "$snw" "$t" "$c" "$eff" "$hits" "$p50" "$cores" "$mem_base_mb" "$mem_peak_mb"
}

log "hpc_redis VSIM max-tput sweep — ${#WORKERS[@]} worker cfgs × ${#TS[@]} memtier pts = $(( ${#WORKERS[@]}*${#TS[@]} )) runs"
for w in "${WORKERS[@]}"; do
    IFS=':' read pio snw <<< "$w"
    log "pio=$pio snw=$snw — starting server"
    start_server "$pio" "$snw" || continue
    sleep 1
    prefill
    n=${#TS[@]}
    for ((i=0; i<n; i++)); do
        run_client "$pio" "$snw" "${TS[$i]}" "${CS[$i]}"
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
