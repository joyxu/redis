#!/bin/bash
# 注意：不能用 set -e —— vanilla redis VEMB 高并发偶发 connection reset，
# 一档失败会触发 cleanup 杀全部 server。用 || true 兜底。
#
# ============================================================================
# Multi-Instance Redis 8.6.3 Baseline (核数拉平 hpc-redis) - 17 档 sweep 版
#
# 在 SERVER (HW01) 本地直接执行；跨节点时 ssh CLIENT 启 memtier。
#
# N 个 Redis 实例，各绑 IO_THREADS 核，总核数 ≈ hpc-redis 的 48 核
# 数据分片：每实例持有不交叠的 key 范围
# N 个 memtier 并行打，各连各的端口，汇总吞吐
#
# 跟 run_vemb_{local_loopback,cross_node}_sweep.sh 对齐:
#   - 17 档 (t,c,pipeline) 配置矩阵（TS/CS/PS 数组）
#   - server 启动参数对齐（--bind 0.0.0.0 / --tcp-backlog 16384 / --appendonly no / --save ''）
#   - 输出 TSV 列对齐 sweep 脚本（op/server_type/t/c/pipeline/ops_sec/avg/p50/p99/kb_sec/cores/nic_util）
#
# 用法 (在 HW01 上执行):
#   bash run_multi_instance_redis_baseline.sh                                 # 默认 17 档, 跨节点
#   TS="64" CS="4" PS="32" bash ...                                          # 单档调试
#   LOCAL_BENCH=1 RAW=1 DIM=300 bash ...                                     # 本地回环 + RAW
#   DIM=8 NIC_IFACE=eth4 bash ...                                            # 跨节点 DIM=8
# ============================================================================
set -uo pipefail

# === 拓扑 ===
CLIENT=${CLIENT:-HW02}
SERVER_HOST=${SERVER_HOST:-192.168.1.111}
BASE_PORT=${BASE_PORT:-7001}
CODE_DIR=${CODE_DIR:-/root/gqs/codespace/redis-8.6.3}
MEMTIER_DIR=${MEMTIER_DIR:-/root/gqs/codespace/UnifiedBus/memtier_benchmark_origin}

# === 实验参数 ===
LOCAL_BENCH=${LOCAL_BENCH:-0}
NUM_INSTANCES=${NUM_INSTANCES:-12}
IO_THREADS=${IO_THREADS:-4}
DIM=${DIM:-300}
NUM_KEYS=${NUM_KEYS:-100000}
TEST_TIME=${TEST_TIME:-30}
RAW=${RAW:-0}
RAW_SUFFIX=""
[ "$RAW" = "1" ] && RAW_SUFFIX=" raw"
NIC_IFACE=${NIC_IFACE:-eth4}

# === 17 档配置矩阵（跟 run_vemb_*_sweep.sh 完全一致）===
TS_DEFAULT=(1 1 1 1  1  2  4  8  16 32 64 64 64 64 64 64 64)
CS_DEFAULT=(1 1 1 1  1  1  1  1  1  1  1  2  4  8  16 32 64)
PS_DEFAULT=(1 4 8 16 32 32 32 32 32 32 32 32 32 32 32 32 32)
TS=( ${TS:-${TS_DEFAULT[*]}} )
CS=( ${CS:-${CS_DEFAULT[*]}} )
PS=( ${PS:-${PS_DEFAULT[*]}} )
if [ ${#TS[@]} -ne ${#CS[@]} ] || [ ${#TS[@]} -ne ${#PS[@]} ]; then
    echo "ERROR: TS/CS/PS length mismatch (TS=${#TS[@]} CS=${#CS[@]} PS=${#PS[@]})"
    exit 2
fi
NCONFIGS=${#TS[@]}

# === 客户端核 ===
CLIENT_CPU_START=${CLIENT_CPU_START:-97}
CLIENT_CPU_END=${CLIENT_CPU_END:-191}
CLIENT_CPUSET=${CLIENT_CPUSET:-}
[ -n "$CLIENT_CPUSET" ] && CLIENT_CPU_SPEC="$CLIENT_CPUSET" || CLIENT_CPU_SPEC="$CLIENT_CPU_START-$CLIENT_CPU_END"

CORES_PER_INSTANCE=$IO_THREADS

# === 输出 ===
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR=${OUTDIR:-benchmark/results/vemb_multi_instance_baseline/${TIMESTAMP}}
RAWDIR="$OUTDIR/raw"
TSV="$OUTDIR/summary.tsv"

ulimit -n 200000
mkdir -p "$RAWDIR"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# ============================================================================
# jiffies (本机直接读 /proc)
# ============================================================================
get_jiffies() {
    # 输出 "ut st"（所有线程 utime/stime jiffies 分别求和）
    local pid=$1 ut=0 st=0
    for f in /proc/$pid/task/*/stat; do
        [ -r "$f" ] || continue
        local r=$(sed 's/.*)//' "$f")
        set -- $r
        ut=$((ut + ${12:-0}))
        st=$((st + ${13:-0}))
    done
    echo "$ut $st"
}

snapshot_si() {  # 全机 softirq jiffies (/proc/stat cpu 行第 8 列)
    awk '/^cpu /{print $8}' /proc/stat 2>/dev/null
}

snapshot_rss() {  # 对给定 pid 求 VmRSS KB 之和
    local pid rss total=0
    for pid in "$@"; do
        [ -n "$pid" ] || continue
        rss=$(awk '/^VmRSS:/{print $2}' /proc/$pid/status 2>/dev/null)
        total=$(( total + ${rss:-0} ))
    done
    echo "$total"
}

# ----------------------------------------------------------------------------
# 单实例操作（直接本地执行）
# ----------------------------------------------------------------------------
INST_PORT=0 INST_CORE_START=0 INST_CORE_END=0 INST_KEY_MIN=0 INST_KEY_MAX=0
INST_DATA_DIR="" INST_LOG=""

inst_var() {
    local iid=$1
    local keys_per=$(( (NUM_KEYS + NUM_INSTANCES - 1) / NUM_INSTANCES ))
    INST_PORT=$((BASE_PORT + iid))
    INST_CORE_START=$((iid * CORES_PER_INSTANCE))
    INST_CORE_END=$((INST_CORE_START + CORES_PER_INSTANCE - 1))
    INST_KEY_MIN=$((iid * keys_per + 1))
    INST_KEY_MAX=$(( (iid + 1) * keys_per ))
    [ $INST_KEY_MAX -gt $NUM_KEYS ] && INST_KEY_MAX=$NUM_KEYS
    INST_DATA_DIR="/tmp/redis_multi_inst_${iid}"
    INST_LOG="${INST_DATA_DIR}/redis.log"
}

start_instance() {
    local iid=$1; inst_var $iid
    rm -rf "$INST_DATA_DIR" && mkdir -p "$INST_DATA_DIR"
    numactl --membind=0 taskset -c $INST_CORE_START-$INST_CORE_END \
        $CODE_DIR/src/redis-server \
            --port $INST_PORT --bind 0.0.0.0 --protected-mode no \
            --tcp-backlog 16384 --tcp-keepalive 1800 --timeout 0 \
            --io-threads $IO_THREADS --io-threads-do-reads yes \
            --appendonly no --save '' \
            --dir "$INST_DATA_DIR" --logfile "$INST_LOG" \
            --daemonize yes --pidfile "${INST_DATA_DIR}/redis.pid"
}

stop_instance() {
    local iid=$1; inst_var $iid
    $CODE_DIR/src/redis-cli -p $INST_PORT --timeout 2 SHUTDOWN NOSAVE 2>/dev/null || true
    pkill -9 -f "redis-server.*:$INST_PORT" 2>/dev/null || true
    for pid in $(ss -tlnp 2>/dev/null | grep ":$INST_PORT " | grep -oP 'pid=\K[0-9]+' | sort -u); do
        kill -9 "$pid" 2>/dev/null || true
    done
}

prefill_instance() {
    local iid=$1; inst_var $iid
    local vec300=$(seq -s " " 1 $DIM | sed "s/[0-9]*/0.1/g")
    local expected=$((INST_KEY_MAX - INST_KEY_MIN + 1))
    for attempt in 1 2 3 4 5; do
        $CODE_DIR/src/redis-cli -p $INST_PORT DEL myvectors >/dev/null 2>&1
        sleep 0.2
        {
            for i in $(seq $INST_KEY_MIN $INST_KEY_MAX); do
                echo "VADD myvectors VALUES $DIM $vec300 item:$i"
            done
        } | $CODE_DIR/src/redis-cli -p $INST_PORT --pipe >/dev/null 2>&1
        local card=$($CODE_DIR/src/redis-cli -p $INST_PORT VCARD myvectors 2>/dev/null)
        [ -z "$card" ] && card=0
        local probe=$($CODE_DIR/src/redis-cli -p $INST_PORT VEMB myvectors item:$INST_KEY_MIN 2>/dev/null | wc -c)
        if [ "$card" -ge "$expected" ] && [ "$probe" -gt 100 ]; then
            log "    inst $iid: prefill OK card=$card (attempt $attempt)"
            return 0
        fi
        sleep 2
    done
    log "    inst $iid: prefill FAIL"
    return 1
}

get_pid() {
    local iid=$1; inst_var $iid
    ss -tlnp 2>/dev/null | grep ":$INST_PORT " | grep -oP 'pid=\K[0-9]+' | head -1
}

# ----------------------------------------------------------------------------
# 全部实例启停 / prefill (并行)
# ----------------------------------------------------------------------------
start_all_instances() {
    log "  Starting $NUM_INSTANCES instances (io-threads=$IO_THREADS, ${CORES_PER_INSTANCE} cores each)..."
    for i in $(seq 0 $((NUM_INSTANCES - 1))); do
        start_instance $i
    done
    local up
    for attempt in $(seq 1 60); do
        up=0
        for i in $(seq 0 $((NUM_INSTANCES - 1))); do
            inst_var $i
            ss -tln | grep -q ":$INST_PORT " && up=$((up + 1))
        done
        [ "$up" = "$NUM_INSTANCES" ] && return 0
        sleep 1
    done
    log "ABORT: only ${up:-0}/$NUM_INSTANCES listening"
    return 1
}

prefill_all_instances() {
    log "  Prefilling $NUM_KEYS vectors across $NUM_INSTANCES instances (parallel)..."
    for i in $(seq 0 $((NUM_INSTANCES - 1))); do
        prefill_instance $i &
    done
    wait
}

stop_all_instances() {
    for i in $(seq 0 $((NUM_INSTANCES - 1))); do
        stop_instance $i &
    done
    wait
}

# ----------------------------------------------------------------------------
# 一档 sweep
# ----------------------------------------------------------------------------
run_one_config() {
    local idx=$1
    local t=${TS[$idx]} c=${CS[$idx]} p=${PS[$idx]}
    log "--- config $((idx+1))/${NCONFIGS}: t=$t c=$c pipeline=$p ---"

    start_all_instances || { log "FAIL: start_all_instances"; return 1; }
    prefill_all_instances

    # PIDs + J0
    declare -a INSTANCE_PIDS J0_UT_VALUES J0_ST_VALUES
    for i in $(seq 0 $((NUM_INSTANCES - 1))); do
        INSTANCE_PIDS[$i]=$(get_pid $i)
        if [ -n "${INSTANCE_PIDS[$i]}" ]; then
            read J0_UT_VALUES[$i] J0_ST_VALUES[$i] < <(get_jiffies ${INSTANCE_PIDS[$i]})
        else
            J0_UT_VALUES[$i]=0
            J0_ST_VALUES[$i]=0
            log "  WARN: no PID for inst $i"
        fi
    done

    # sar NIC 监控（仅跨节点；本地回环走 lo 跳过）
    local sar_log="$RAWDIR/sar_t${t}_c${c}_p${p}.log"
    rm -f "$sar_log"
    [ "$LOCAL_BENCH" != "1" ] && nohup sar -n DEV 1 $((TEST_TIME + 5)) > "$sar_log" 2>&1 &

    # bench host
    local bench_host
    [ "$LOCAL_BENCH" = "1" ] && bench_host="127.0.0.1" || bench_host="$SERVER_HOST"

    # Parallel memtier (每实例用同样的 t/c/p)
    local jb_si ja_si c_si rss_kb
    jb_si=$(snapshot_si)
    log "  launching $NUM_INSTANCES parallel memtier (per-inst: -t $t -c $c --pipeline=$p)"
    declare -a REMOTE_OUTS
    for i in $(seq 0 $((NUM_INSTANCES - 1))); do
        inst_var $i
        local remote_out="/tmp/multi_inst_${TIMESTAMP}_t${t}_c${c}_p${p}_inst${i}.log"
        REMOTE_OUTS[$i]=$remote_out
        if [ "$LOCAL_BENCH" = "1" ]; then
            taskset -c $CLIENT_CPU_SPEC \
                $MEMTIER_DIR/memtier_benchmark \
                    -h "$bench_host" -p "$INST_PORT" \
                    --hide-histogram --test-time="$TEST_TIME" --select-db=0 \
                    -c "$c" -t "$t" --pipeline="$p" \
                    --command="VEMB myvectors __key__${RAW_SUFFIX}" \
                    --command-key-pattern=R \
                    --key-prefix=item: \
                    --key-minimum="$INST_KEY_MIN" --key-maximum="$INST_KEY_MAX" \
                    > "$remote_out" 2>&1 &
        else
            ssh "$CLIENT" "taskset -c $CLIENT_CPU_SPEC \
                $MEMTIER_DIR/memtier_benchmark \
                    -h '$bench_host' -p '$INST_PORT' \
                    --hide-histogram --test-time='$TEST_TIME' --select-db=0 \
                    -c '$c' -t '$t' --pipeline='$p' \
                    --command='VEMB myvectors __key__${RAW_SUFFIX}' \
                    --command-key-pattern=R \
                    --key-prefix='item:' \
                    --key-minimum='$INST_KEY_MIN' --key-maximum='$INST_KEY_MAX' \
                    > '$remote_out' 2>&1" &
        fi
    done
    wait

    # softirq delta + server rss (VmRSS KB, 所有实例求和)
    ja_si=$(snapshot_si)
    ja_si=${ja_si:-0}
    c_si=$(awk -v d=$((ja_si - jb_si)) -v s=$TEST_TIME 'BEGIN{printf "%.2f", d/100.0/s}')
    rss_kb=$(snapshot_rss ${INSTANCE_PIDS[@]})

    # Collect per-instance + aggregate
    local TOTAL_OPS=0 TOTAL_KB_SEC=0 AVG_LAT_SUM=0 P50_SUM=0 P99_SUM=0 VALID=0 TOTAL_CORES=0 TOTAL_UT=0 TOTAL_ST=0
    for i in $(seq 0 $((NUM_INSTANCES - 1))); do
        local local_out="$RAWDIR/inst${i}_t${t}_c${c}_p${p}.log"
        local remote_out="${REMOTE_OUTS[$i]}"
        if [ "$LOCAL_BENCH" = "1" ]; then
            cp "$remote_out" "$local_out" 2>/dev/null || true
        else
            ssh "$CLIENT" "cat $remote_out" > "$local_out" 2>/dev/null || true
        fi

        # Totals 行字段位置 (跟 sweep 脚本同一套解析)
        #   NF>=9: 带 Hits/Misses, ops=$2 avg=$5 p50=$6 p99=$7 kb=$9
        #   NF>=7: 普通,        ops=$2 avg=$3 p50=$4 p99=$5 kb=$7
        local totals=$(grep "^Totals" "$local_out" 2>/dev/null | tail -1)
        local nfields=$(echo "$totals" | awk '{print NF}')
        local ops=0 avg=0 p50=0 p99=0 kb=0
        if [ -n "$totals" ] && [ "$(echo "$totals" | awk '{print $2}')" != "0.00" ]; then
            if [ "$nfields" -ge 9 ]; then
                ops=$(echo "$totals" | awk '{print $2}')
                avg=$(echo "$totals" | awk '{print $5}')
                p50=$(echo "$totals" | awk '{print $6}')
                p99=$(echo "$totals" | awk '{print $7}')
                kb=$(echo "$totals" | awk '{print $9}')
            elif [ "$nfields" -ge 7 ]; then
                ops=$(echo "$totals" | awk '{print $2}')
                avg=$(echo "$totals" | awk '{print $3}')
                p50=$(echo "$totals" | awk '{print $4}')
                p99=$(echo "$totals" | awk '{print $5}')
                kb=$(echo "$totals" | awk '{print $7}')
            fi
        else
            log "  WARN: inst $i no Totals"
        fi

        # cores via jiffies (ut/st 拆分)
        local pid="${INSTANCE_PIDS[$i]}"
        local inst_cores=0 inst_ut=0 inst_st=0
        if [ -n "$pid" ]; then
            local j1_ut j1_st
            read j1_ut j1_st < <(get_jiffies $pid)
            inst_ut=$(awk -v d=$((j1_ut - ${J0_UT_VALUES[$i]:-0})) -v s=$TEST_TIME 'BEGIN{printf "%.2f", d/100.0/s}')
            inst_st=$(awk -v d=$((j1_st - ${J0_ST_VALUES[$i]:-0})) -v s=$TEST_TIME 'BEGIN{printf "%.2f", d/100.0/s}')
            inst_cores=$(awk -v u=$inst_ut -v v=$inst_st 'BEGIN{printf "%.2f", u + v}')
        fi
        TOTAL_CORES=$(awk "BEGIN{ printf \"%.2f\", $TOTAL_CORES + $inst_cores }")
        TOTAL_UT=$(awk "BEGIN{ printf \"%.2f\", $TOTAL_UT + $inst_ut }")
        TOTAL_ST=$(awk "BEGIN{ printf \"%.2f\", $TOTAL_ST + $inst_st }")

        TOTAL_OPS=$((TOTAL_OPS + $(echo "$ops" | awk '{printf "%.0f", $1}')))
        TOTAL_KB_SEC=$((TOTAL_KB_SEC + $(echo "$kb" | awk '{printf "%.0f", $1}')))
        AVG_LAT_SUM=$(awk "BEGIN{printf \"%.4f\", $AVG_LAT_SUM + ${avg:-0}}")
        P50_SUM=$(awk "BEGIN{printf \"%.4f\", $P50_SUM + ${p50:-0}}")
        P99_SUM=$(awk "BEGIN{printf \"%.4f\", $P99_SUM + ${p99:-0}}")
        VALID=$((VALID + 1))
    done

    local AVG_LAT=$(awk "BEGIN{ if($VALID>0) printf \"%.3f\", $AVG_LAT_SUM/$VALID; else print \"NA\" }")
    local AVG_P50=$(awk "BEGIN{ if($VALID>0) printf \"%.3f\", $P50_SUM/$VALID; else print \"NA\" }")
    local AVG_P99=$(awk "BEGIN{ if($VALID>0) printf \"%.3f\", $P99_SUM/$VALID; else print \"NA\" }")

    local NIC_UTIL="N/A"
    if [ "$LOCAL_BENCH" != "1" ]; then
        sleep 2  # 等 sar flush 最后样本
        NIC_UTIL=$(awk -v iface="$NIC_IFACE" '$2==iface && NF>=9 {sum+=$NF; n++} END {if(n>0) printf "%.1f", sum/n; else print "N/A"}' "$sar_log" 2>/dev/null)
        [ -z "$NIC_UTIL" ] && NIC_UTIL="N/A"
    fi

    printf "VEMB\tbaseline_multi_inst\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
        "$t" "$c" "$p" "$TOTAL_OPS" "$AVG_LAT" "$AVG_P50" "$AVG_P99" "$TOTAL_KB_SEC" "$TOTAL_CORES" "$NIC_UTIL" "$TOTAL_UT" "$TOTAL_ST" "$c_si" "$rss_kb" >> "$TSV"
    log "  => ops/s=$TOTAL_OPS  avg=${AVG_LAT}ms  p50=${AVG_P50}ms  p99=${AVG_P99}ms  cores=$TOTAL_CORES  ${NIC_IFACE}_util=${NIC_UTIL}%  core_ut=$TOTAL_UT  core_st=$TOTAL_ST  si=$c_si  rss=${rss_kb}KB"

    stop_all_instances
}

# ============================================================================
# Cleanup lingering instances
# ============================================================================
log "Cleaning up lingering instances..."
for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    stop_instance $i &
done
wait

# ============================================================================
# Connectivity check (LOCAL_BENCH=1 跳过)
# ============================================================================
if [ "$LOCAL_BENCH" != "1" ]; then
    log "Checking ssh $CLIENT reachable..."
    ssh "$CLIENT" "true" 2>/dev/null || { log "ERROR: cannot ssh to CLIENT=$CLIENT"; exit 1; }
    ssh "$CLIENT" "test -x $MEMTIER_DIR/memtier_benchmark" 2>/dev/null \
        || { log "ERROR: CLIENT memtier missing at $MEMTIER_DIR/memtier_benchmark"; exit 1; }
    log "  ssh ok, memtier ok"
fi

# ============================================================================
# TSV header + main sweep loop
# ============================================================================
printf "op\tserver_type\tt\tc\tpipeline\tops_sec\tavg_lat_ms\tp50_ms\tp99_ms\tkb_sec\tcores\tnic_util_pct\tcore_ut\tcore_st\tsi\trss_kb\n" > "$TSV"

log "=== ${NCONFIGS}-档 sweep × ${NUM_INSTANCES} 实例 × io-threads=${IO_THREADS} (每档 restart) ==="
log "  LOCAL_BENCH=$LOCAL_BENCH  RAW=$RAW  DIM=$DIM  NUM_KEYS=$NUM_KEYS  TEST_TIME=${TEST_TIME}s"
log "  client_cpuset=$CLIENT_CPU_SPEC  NIC_IFACE=$NIC_IFACE"

for ((idx=0; idx<NCONFIGS; idx++)); do
    run_one_config $idx
done

log "=== DONE ==="
log "TSV : $TSV"
log "raw : $RAWDIR/*.log"
echo "----- summary -----"
column -t -s $'\t' "$TSV"
