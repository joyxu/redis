#!/bin/bash
# Cross-NUMA memtier -t/-c 吞吐探索 (找最大 ops/sec 的 N×t×c 组合)
#
#   服务端 taskset -c 1-N    (numactl --membind=0)   核数随 N 递增
#   客户端 taskset -c 97-(96+t) (numactl --membind=1) 核数随 -t 递增
#   2 impl (redis baseline / hpc_redis) × 6 N × 6 t × 4 c = 288 组
#   prefill 10000 key + 100% 读 (RESP VEMB 或 VEMB V16 VEMB_INLINE)，TEST_TIME/组
#
# 用法: ./cross_numa_memtier_sweep.sh
#   环境变量覆盖 (smoke):
#     NS="2" IMPLS="hpc_redis" TS="1 8" CS="64" TEST_TIME=3
#
# 不 commit；结果写到 /tmp/memtier_sweep_results/。
set -uo pipefail

# ───────── 路径 (HW01) ─────────
REDIS_BASELINE=/root/gqs/codespace/redis-8.6.3
HPC_REDIS=/root/gqs/codespace/UnifiedBus/hpc-redis
MEMTIER_VEMB=/root/gqs/codespace/UnifiedBus/memtier_benchmark/memtier_benchmark
MEMTIER_ORIGIN=/root/gqs/codespace/UnifiedBus/memtier_benchmark_origin/memtier_benchmark
CLEAR_UB=/tmp/clear_ub_device            # 可选；缺失则靠 server reset flag
MANIFEST_HPC=${MANIFEST_HPC:-$HPC_REDIS/examples/vemb_v16_warm_regions_111.yaml}

# ───────── 实验参数 ─────────
DIM=300
NUM_KEYS=10000
KEY_PREFIX="item:"
PORT=${PORT:-6390}
TEST_TIME=${TEST_TIME:-5}
PIPELINE=32

# ───────── 固定 300-dim vector (redis baseline prefill/读都用固定向量) ─────────
# memtier --command 模式无法每请求生成随机向量；用 awk 固定种子生成一次复用。
# 固定向量会让 HNSW 图拓扑退化，但 VEMB 是按 element 取向量，不受图拓扑影响。
FIXED_VECTOR=$(awk 'BEGIN{
    srand(42);
    out="";
    for(j=0;j<300;j++){ out=out sprintf("%.5f", rand()*j); if(j<299) out=out " "; }
    print out;
}')
NS=( ${NS:-2 4 8 16 32 64} )
TS=( ${TS:-1 2 4 8 16 32} )
CS=( ${CS:-16 32 64} )
IMPLS=( ${IMPLS:-redis hpc_redis} )
SERVER_MEMBIND=0
CLIENT_MEMBIND=1

# ───────── 输出 ─────────
OUTDIR=${OUTDIR:-/tmp/memtier_sweep_results}
RAWDIR=$OUTDIR/raw
mkdir -p "$RAWDIR"
TSV=$OUTDIR/summary.tsv
PIDFILE=/tmp/memtier_sweep_server.pid

printf 'impl\tN\tt\tc\tpipeline\tops_sec\tp50_ms\tp99_ms\tcpu_cores\trun_ops_sec\tmem_base_mb\tmem_peak_mb\tmem_avg_mb\n' > "$TSV"
declare -A OPS CPU
BEST_OPS=0; BEST_KEY=""

log() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }

# ───────── worker 配置: snw=min(8,max(1,N/4)), pio=N-snw ─────────
calc_workers() {
    local n=$1 snw=$(( $1 / 4 ))
    [ $snw -lt 1 ] && snw=1
    [ $snw -gt 8 ] && snw=8
    echo "$(( n - snw )) $snw"
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

# ───────── server 生命周期 ─────────
kill_server() {
    if [ -f "$PIDFILE" ]; then
        local p; p=$(cat "$PIDFILE" 2>/dev/null)
        [ -n "$p" ] && kill "$p" 2>/dev/null
        for _ in {1..30}; do kill -0 "$p" 2>/dev/null || break; sleep 0.1; done
        kill -9 "$p" 2>/dev/null
        rm -f "$PIDFILE"
    fi
    pkill -f "port $PORT " 2>/dev/null
    sleep 0.5
}

wait_port() {
    for _ in {1..50}; do ss -tln 2>/dev/null | grep -q ":$PORT " && return 0; sleep 0.2; done
    return 1
}

# ───────── 启动某实现 (taskset -c 1-N) ─────────
start_server() {
    local impl=$1 N=$2 PIO=0 SNW=0
    local logfile=$RAWDIR/${impl}_N${N}.server.log
    kill_server
    [ -x "$CLEAR_UB" ] && "$CLEAR_UB" >/dev/null 2>&1

    local cpus="1-$N"
    local bind="numactl --membind=$SERVER_MEMBIND taskset -c $cpus"

    case "$impl" in
    redis)
        $bind $REDIS_BASELINE/src/redis-server \
            --port $PORT --bind 0.0.0.0 --protected-mode no \
            --io-threads $N --io-threads-do-reads yes \
            --daemonize yes --pidfile $PIDFILE --logfile "$logfile" --loglevel notice \
            >/dev/null 2>&1
        ;;
    hpc_redis)
        if [ -n "${W_PIO:-}" ] && [ -n "${W_SNW:-}" ]; then
            PIO=$W_PIO; SNW=$W_SNW
        else
            read PIO SNW < <(calc_workers $N)
        fi
        $bind $HPC_REDIS/src/redis-server \
            --port $PORT --bind 0.0.0.0 --protected-mode no \
            --vemb-v16-enabled yes --vemb-v16-dim $DIM \
            --vemb-v16-warm-regions-manifest "$MANIFEST_HPC" \
            --vemb-v16-reset-warm-regions yes \
            --vemb-v16-proxy-io-threads $PIO --vemb-v16-supernode-workers $SNW \
            --daemonize yes --pidfile $PIDFILE --logfile "$logfile" --loglevel notice \
            >/dev/null 2>&1
        ;;
    *)
        echo "unknown impl: $impl"; return 1
        ;;
    esac

    local p=""
    for _ in {1..50}; do [ -f "$PIDFILE" ] && { p=$(cat "$PIDFILE" 2>/dev/null); [ -n "$p" ] && break; }; sleep 0.2; done
    wait_port || { echo "FAIL: $impl N=$N server did not listen on $PORT (see $logfile)"; return 1; }
    SERVER_PID=$p
}

# ───────── prefill (单线程，绑 97-191) ─────────
prefill() {
    local impl=$1
    local raw=$RAWDIR/${impl}_prefill.log
    local bind="numactl --membind=$CLIENT_MEMBIND taskset -c 97-191"
    if [ "$impl" = redis ]; then
        # RESP VADD：固定 300-dim vector + __key__ 顺序写入 vset "myset"
        $bind $MEMTIER_ORIGIN \
            --command="VADD myset VALUES 300 $FIXED_VECTOR __key__" --command-key-pattern=S \
            --key-prefix=$KEY_PREFIX --key-minimum=1 --key-maximum=$NUM_KEYS \
            -s 127.0.0.1 -p $PORT -t 1 -c 1 --pipeline=$PIPELINE \
            -n $NUM_KEYS \
            >"$raw" 2>&1
    else
        $bind $MEMTIER_VEMB \
            --protocol vemb_v16 --vemb-v16-dim $DIM \
            -s 127.0.0.1 -p $PORT -t 1 -c 1 -n $NUM_KEYS \
            --ratio=1:0 --key-pattern=S:S --key-prefix=$KEY_PREFIX \
            --key-minimum=1 --key-maximum=$NUM_KEYS \
            >"$raw" 2>&1
    fi
    printf '   %-10s prefill done (%s keys)\n' "$impl" "$NUM_KEYS"
}

# ───────── 跑一组 client (taskset -c 97-(96+t)) ─────────
#   $1=impl $2=N $3=t $4=c
run_client() {
    local impl=$1 N=$2 t=$3 c=$4
    local tag=${impl}_N${N}_t${t}_c${c}
    local raw=$RAWDIR/${tag}.txt

    local j0 j1 cores
    local mem_base_mb mem_peak_mb mem_avg_mb mem_samples_file mem_sampler_pid
    j0=$(get_cpu_jiffies "$SERVER_PID")

    # ── 内存采集 - 启动 sampler ──
    # hpc_redis: VmRSS 不含 warm region mmap（pfn-map），warm region reserved = 1024 MB 固定开销不在列中
    # baseline: 全在普通堆，VmRSS 即全部内存
    mem_base_mb=$(awk '/^VmRSS:/{printf "%.0f", $2/1024}' /proc/$SERVER_PID/status 2>/dev/null)
    echo 1 > /proc/$SERVER_PID/clear_refs 2>/dev/null || true
    mem_samples_file=/tmp/mem_samples_${tag}.log
    rm -f "$mem_samples_file"
    ( while kill -0 "$SERVER_PID" 2>/dev/null; do
        awk '/^VmRSS:/{print $2}' /proc/$SERVER_PID/status 2>/dev/null
        sleep 0.5
      done > "$mem_samples_file" ) &
    mem_sampler_pid=$!

    local cpubind="97-$(( 96 + t ))"
    local bind="numactl --membind=$CLIENT_MEMBIND taskset -c $cpubind"
    if [ "$impl" = redis ]; then
        # RESP VEMB：按 element 取向量（"Return the vector associated with an element"）
        $bind $MEMTIER_ORIGIN \
            --command="VEMB myset __key__" --command-key-pattern=R \
            --key-prefix=$KEY_PREFIX --key-minimum=1 --key-maximum=$NUM_KEYS \
            -s 127.0.0.1 -p $PORT -t $t -c $c --pipeline=$PIPELINE \
            --test-time=$TEST_TIME >"$raw" 2>&1
    else
        $bind $MEMTIER_VEMB \
            --protocol vemb_v16 --vemb-v16-dim $DIM \
            -s 127.0.0.1 -p $PORT -t $t -c $c --pipeline=$PIPELINE \
            --ratio=0:1 --key-pattern=R:R --key-prefix=$KEY_PREFIX \
            --key-minimum=1 --key-maximum=$NUM_KEYS \
            --test-time=$TEST_TIME >"$raw" 2>&1
    fi

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
    local ops p50 p99
    ops=$(echo "$tot" | awk '{print $2}')
    p50=$(echo "$tot" | awk '{print $6}')
    p99=$(echo "$tot" | awk '{print $7}')
    # VEMB 高并发聚合 bug 兜底: Totals=0 时取 RUN 100% 第一个 avg
    local runops; runops=$(grep 'RUN #1 100%' "$raw" | grep -oE 'avg: *[0-9.,]+' | head -1 | grep -oE '[0-9.,]+')
    [ -z "$ops" ] && ops=0
    [ -z "$runops" ] && runops=0
    [ -z "$mem_base_mb" ] && mem_base_mb=0; [ -z "$mem_peak_mb" ] && mem_peak_mb=0; [ -z "$mem_avg_mb" ] && mem_avg_mb=0
    local eff=$ops
    if awk "BEGIN{exit !($ops < 1)}" && [ "$runops" != 0 ]; then eff=$runops; fi

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$impl" "$N" "$t" "$c" "$PIPELINE" "$ops" "$p50" "$p99" "$cores" "$runops" \
        "$mem_base_mb" "$mem_peak_mb" "$mem_avg_mb" >> "$TSV"

    local k="${impl}|${N}|${t}|${c}"
    OPS[$k]=$eff; CPU[$k]=$cores
    if awk "BEGIN{exit !($eff > $BEST_OPS)}"; then
        BEST_OPS=$eff; BEST_KEY="$k"
    fi
    printf '   %-10s N=%-2s t%-2s c%-3s  ops=%-12s p50=%-8s cores=%-5s  mem=%s/%sMB\n' \
        "$impl" "$N" "$t" "$c" "$eff" "${p50:-NA}" "$cores" "$mem_base_mb" "$mem_peak_mb"
}

# ───────── 主循环 ─────────
log "memtier sweep — ${#IMPLS[@]} impl × ${#NS[@]} N × ${#TS[@]} t × ${#CS[@]} c = $(( ${#IMPLS[@]}*${#NS[@]}*${#TS[@]}*${#CS[@]} )) runs"
for impl in "${IMPLS[@]}"; do
    for N in "${NS[@]}"; do
        log "$impl N=$N — starting server (taskset -c 1-$N)"
        start_server "$impl" "$N" || { kill_server; continue; }
        sleep 1
        prefill "$impl"
        for t in "${TS[@]}"; do
            for c in "${CS[@]}"; do
                run_client "$impl" "$N" "$t" "$c"
            done
        done
        kill_server
    done
done
kill_server

# ───────── 汇总: 每 impl 最大吞吐组合 + 全局最优 ─────────
log "DONE — 结果: $TSV"
printf '\n\033[1m── 每 impl 最大吞吐组合 (N,t,c) ──\033[0m\n'
for impl in "${IMPLS[@]}"; do
    lb=0; lk=""
    for k in "${!OPS[@]}"; do
        case "$k" in
            "${impl}|"*) ;;
            *) continue;;
        esac
        v=${OPS[$k]}
        if awk "BEGIN{exit !($v > $lb)}"; then
            lb=$v; lk="$k"
        fi
    done
    if [ -n "$lk" ]; then
        IFS='|' read _ bn bt bc <<< "$lk"
        bcores=${CPU[$lk]}
        printf '  %-10s best: N=%-2s t=%-2s c=%-3s  ops/sec=%-12s (server cores=%s)\n' \
            "$impl" "$bn" "$bt" "$bc" "$lb" "$bcores"
    fi
done
printf '\n全局最优: %s = %s ops/sec\n' "$BEST_KEY" "$BEST_OPS"
printf '\n原始 memtier: %s/*.txt  服务端日志: %s/*.server.log\n' "$RAWDIR" "$RAWDIR"
