#!/usr/bin/env bash
# ============================================================================
# run_vemb_cross_node_sweep.sh
# 跨节点 100G 网卡 VEMB/VSIM/VADD/VREM 吞吐/延迟 sweep
#   —— 同一 OP 下, baseline redis (RESP3) 与 hpc-redis (VEMB V16) 各跑一遍对比
#   —— 17 档 (t,c,pipeline) 配置矩阵 + sar -n DEV 1 网卡利用率统计
#   —— 每档重启 server 保证干净起点 (hpc --vemb-v16-reset-warm-regions yes)
#
# 运行模型 (跟 run_vemb_local_loopback_sweep.sh 一致, 在 SERVER 上直接跑):
#   SERVER 本地起 redis-server + sar + jiffies/mem sampler
#   ssh CLIENT 启 memtier_benchmark
#
# 用法 (在 SERVER 上执行):
#   OP_TYPE=VEMB bash benchmark/run_vemb_cross_node_sweep.sh
#   OP_TYPE=VSIM bash benchmark/run_vemb_cross_node_sweep.sh
#   OP_TYPE=VSIM_2KEY bash benchmark/run_vemb_cross_node_sweep.sh
#   OP_TYPE=VADD bash benchmark/run_vemb_cross_node_sweep.sh
#   OP_TYPE=VREM bash benchmark/run_vemb_cross_node_sweep.sh
#   TEST_TIME=10 OP_TYPE=VSIM bash ...                       # smoke
#   SERVERS_ONLY="baseline" OP_TYPE=VEMB bash ...            # 只跑 baseline
#   TS="64" CS="4" PS="32" OP_TYPE=VEMB bash ...             # 单档
#
# 命令对照 (跟 run_vemb_local_loopback_sweep.sh 完全一致):
#   OP_TYPE   | hpc memtier (VEMB V16)                    | baseline memtier (RESP3)
#   ----------+-------------------------------------------+-----------------------------------------------
#   VEMB      | --vemb-v16 --ratio=0:1 R:R item:          | --command="VEMB myset __key__ raw" item: R
#   VSIM      | --vemb-v16-vsim R:R item:                 | --command="VSIM myset VALUES \$dim \$FIXED_VECTOR COUNT 1" R item:
#   VSIM_2KEY | --vemb-v16-vsim-key-key --ratio=0:1 R:R   | --command="VEMB myset __key__ raw" R:R item:
#   VADD      | --ratio=1:0 S:S item:                     | --command="VADD myset VALUES \$dim \$fixed_vec __key__" S item:
#   VREM      | --vemb-v16-vrem 1:0 S:S                   | --command="VREM myset __key__" S item:
# ============================================================================
set -uo pipefail

# === 拓扑 (SERVER=本地, CLIENT=远端 memtier 节点) ===
CLIENT=${CLIENT:-HW02}
SERVER_HOST=${SERVER_HOST:-192.168.1.111}
SERVER_PORT=${SERVER_PORT:-6390}
NIC_IFACE=${NIC_IFACE:-eth4}        # SERVER 上 100G mlx5 网卡 (sar -n DEV 监听)

# === 路径 (SERVER 本地) ===
REDIS_DIR=${REDIS_DIR:-/root/gqs/codespace/redis-8.6.3}
HPC_DIR=${HPC_DIR:-/root/gqs/codespace/UnifiedBus/hpc-redis}
BASELINE_MEMTIER=${BASELINE_MEMTIER:-/root/gqs/codespace/UnifiedBus/memtier_benchmark_origin/memtier_benchmark}
HPC_MEMTIER=${HPC_MEMTIER:-$HPC_DIR/memtier_benchmark/memtier_benchmark}
# CLIENT 上 memtier 路径 (假设跟 SERVER 同构, 都在 $CODE_DIR 下)
CLIENT_BASELINE_MEMTIER=${CLIENT_BASELINE_MEMTIER:-/root/gqs/codespace/UnifiedBus/memtier_benchmark_origin/memtier_benchmark}
CLIENT_HPC_MEMTIER=${CLIENT_HPC_MEMTIER:-/root/gqs/codespace/UnifiedBus/hpc-redis/memtier_benchmark/memtier_benchmark}
MANIFEST=${MANIFEST:-$HPC_DIR/examples/vemb_v16_warm_regions_111.yaml}
if [ "${DIM:-300}" = "8" ] && [ -z "${MANIFEST_OVERRIDE+x}" ]; then
    MANIFEST=$HPC_DIR/examples/vemb_v16_warm_regions_111_dim8.yaml
fi
DATA_DIR=${DATA_DIR:-/tmp/redis-cross-sweep}

# === OP_TYPE ===
OP_TYPE=${OP_TYPE:-VEMB}
case "$OP_TYPE" in
    VEMB|VSIM|VSIM_2KEY|VADD|VREM) ;;
    *) echo "ERROR: OP_TYPE must be one of VEMB/VSIM/VSIM_2KEY/VADD/VREM (got: $OP_TYPE)"; exit 2 ;;
esac

# === 测试参数 ===
TEST_TIME=${TEST_TIME:-30}
DIM=${DIM:-300}
NUM_VSETS=${NUM_VSETS:-16}
VECTORS_PER_VSET=${VECTORS_PER_VSET:-6250}   # 16*6250 = 100K (仅 VEMB)
KEY_OFFSET=${KEY_OFFSET:-1}
NUM_KEYS=${NUM_KEYS:-100000}

# === server 固定口径 (跟本地脚本一致) ===
BASELINE_IO_THREADS=${BASELINE_IO_THREADS:-4}
HPC_PIO=${HPC_PIO:-2}
HPC_SNW=${HPC_SNW:-2}

# === CPU 绑核 (SERVER 端) ===
BASELINE_SERVER_CPUSET=${BASELINE_SERVER_CPUSET:-31-38}
HPC_SERVER_CPUSET=${HPC_SERVER_CPUSET:-41-48}

# === 17 档配置矩阵 (跟本地脚本一致) ===
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

# === 跑哪些 server ===
SERVERS_ONLY=${SERVERS_ONLY:-"baseline hpc"}

# === 输出 ===
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR=${OUTDIR:-benchmark/results/${OP_TYPE,,}_cross_node_sweep/${TIMESTAMP}}
RAWDIR="$OUTDIR/raw"
TSV="$OUTDIR/summary.tsv"

ulimit -n 200000
mkdir -p "$RAWDIR" "$DATA_DIR"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# ----------------------------------------------------------------------------
# baseline VADD 用固定向量 (跟 redis_baseline_vadd_max_tput.sh:49-54 对齐)
FIXED_VECTOR=$(awk 'BEGIN{
    srand(42);
    out="";
    for(j=0;j<300;j++){ out=out sprintf("%.5f", rand()*j); if(j<299) out=out " "; }
    print out;
}')

# ----------------------------------------------------------------------------
op_key_range() {
    case "$OP_TYPE" in
        *)      echo "item: 1 $NUM_KEYS" ;;
    esac
}

# ----------------------------------------------------------------------------
# SERVER 本地 jiffies helper (utime/stime split across all TIDs)
snapshot_jiffies() {
    local port=$1 ut=0 st=0
    for pid in $(pgrep -x redis-server); do
        if tr '\0' ' ' < /proc/$pid/cmdline 2>/dev/null | grep -Eq ":${port}\$|:${port} "; then
            read u s < <(awk '{u+=$14; s+=$15} END{printf "%d %d", u+0, s+0}' /proc/$pid/task/*/stat 2>/dev/null)
            ut=$((ut + ${u:-0})); st=$((st + ${s:-0}))
        fi
    done
    echo "$ut $st"
}

# server RSS (KB) by port match
snapshot_rss() {
    local port=$1 rss=0
    for pid in $(pgrep -x redis-server); do
        if tr '\0' ' ' < /proc/$pid/cmdline 2>/dev/null | grep -Eq ":${port}\$|:${port} "; then
            local r=$(awk '/^VmRSS:/{print $2}' /proc/$pid/status 2>/dev/null)
            rss=$((rss + ${r:-0}))
        fi
    done
    echo "$rss"
}

# softirq from /proc/stat
snapshot_si() { awk '/^cpu /{print $8}' /proc/stat 2>/dev/null; }

wait_port() {
    local port=$1 count=0
    while ! $REDIS_DIR/src/redis-cli -h 127.0.0.1 -p $port PING 2>/dev/null | grep -q PONG; do
        sleep 0.2
        count=$((count + 1))
        [ $count -gt 100 ] && { log "TIMEOUT waiting 127.0.0.1:$port"; return 1; }
    done
    return 0
}

# ----------------------------------------------------------------------------
# SERVER 本地起 redis-server (不 ssh)
start_baseline() {
    log "  启动 baseline redis-server (io-threads=$BASELINE_IO_THREADS)..."
    taskset -c $BASELINE_SERVER_CPUSET \
        $REDIS_DIR/src/redis-server \
            --port $SERVER_PORT --bind 0.0.0.0 --protected-mode no \
            --tcp-backlog 16384 --tcp-keepalive 1800 --timeout 0 \
            --io-threads $BASELINE_IO_THREADS --io-threads-do-reads yes \
            --appendonly no --save '' \
            --dir $DATA_DIR --logfile $DATA_DIR/baseline.log \
            --daemonize yes
    wait_port $SERVER_PORT || { log "FAIL: baseline server"; exit 1; }
}

start_hpc() {
    log "  启动 hpc-redis (pio=$HPC_PIO snw=$HPC_SNW)..."
    local max_vec=$((NUM_KEYS + 1000))
    taskset -c $HPC_SERVER_CPUSET \
        $HPC_DIR/src/redis-server \
            --port $SERVER_PORT --bind 0.0.0.0 --protected-mode no \
            --tcp-backlog 16384 --tcp-keepalive 1800 --timeout 0 \
            --io-threads $BASELINE_IO_THREADS --io-threads-do-reads yes \
            --vemb-v16-enabled yes \
            --vemb-v16-tcp-host $SERVER_HOST \
            --vemb-v16-dim $DIM \
            --vemb-v16-max-vectors $max_vec \
            --vemb-v16-proxy-io-threads $HPC_PIO \
            --vemb-v16-supernode-workers $HPC_SNW \
            --vemb-v16-warm-regions-manifest $MANIFEST \
            --vemb-v16-reset-warm-regions yes \
            --appendonly no --save '' \
            --dir $DATA_DIR --logfile $DATA_DIR/hpc.log \
            --daemonize yes
    wait_port $SERVER_PORT || { log "FAIL: hpc server"; exit 1; }
}

stop_server() {
    log "  关闭 redis-server..."
    pkill -9 -x redis-server 2>/dev/null
    sleep 1
}

# ----------------------------------------------------------------------------
# prefill 在 SERVER 本地跑 (数据落在 SERVER, memtier 实测时走网卡读)
prefill() {
    local server_type=$1
    if [ "$OP_TYPE" = "VADD" ]; then
        log "  [$server_type] OP_TYPE=VADD 跳过 prefill"
        $REDIS_DIR/src/redis-cli -h 127.0.0.1 -p $SERVER_PORT FLUSHDB >/dev/null 2>&1
        return 0
    fi

    local prefix kmin kmax
    read prefix kmin kmax < <(op_key_range)
    log "  [$server_type] prefill OP=$OP_TYPE key=$prefix[$kmin..$kmax] (DIM=$DIM)..."
    $REDIS_DIR/src/redis-cli -h 127.0.0.1 -p $SERVER_PORT FLUSHDB >/dev/null 2>&1

    if [ "$server_type" = "hpc" ]; then
        local nfill=$((kmax - kmin + 1))
        $HPC_MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
            -s 127.0.0.1 -p $SERVER_PORT -t 64 -c 4 -n $nfill \
            --ratio=1:0 --key-pattern=S:S --key-prefix=$prefix \
            --key-minimum=$kmin --key-maximum=$kmax \
            > "$RAWDIR/${server_type}_${OP_TYPE}_prefill.log" 2>&1
    else
        awk -v n=$NUM_KEYS -v dim=$DIM 'BEGIN{
            srand(42);
            for (i=1; i<=n; i++) {
                printf "VADD myset VALUES %d", dim;
                for (j=0; j<dim; j++) printf " %f", rand()*j*0.001;
                printf " item:%d\n", i;
            }
        }' | $REDIS_DIR/src/redis-cli -h 127.0.0.1 -p $SERVER_PORT --pipe >"$RAWDIR/${server_type}_${OP_TYPE}_prefill_pipe.log" 2>&1
        local pipe_rc=$?
        if [ $pipe_rc -ne 0 ]; then
            # 不重试; 记录死亡原因 (pipefail 语义: awk=1 / cli=2 / signal=128+n)
            log "  [$server_type] prefill pipe FAILED rc=$pipe_rc (see ${RAWDIR}/${server_type}_${OP_TYPE}_prefill_pipe.log)"
            echo "[$(date '+%H:%M:%S')] $OP_TYPE $server_type prefill pipe rc=$pipe_rc" >> "$RAWDIR/prefill_failures.log"
        fi
    fi

    if [ "$server_type" = "hpc" ]; then
        local prefill_log="$RAWDIR/${server_type}_${OP_TYPE}_prefill.log"
        local n_sets=$(grep "^Totals" "$prefill_log" 2>/dev/null | awk '{print $2}')
        local n_sets_int=$(printf "%.0f" "${n_sets:-0}")
        log "  [$server_type] prefill 完成: ${n_sets_int} sets/sec (target=${NUM_KEYS} keys)"
    else
        local vc=$( $REDIS_DIR/src/redis-cli -h 127.0.0.1 -p $SERVER_PORT VCARD myset 2>/dev/null)
        log "  [$server_type] prefill 完成: myset VCARD=$vc"
    fi
}

# ----------------------------------------------------------------------------
# 部署 bench helper 到 CLIENT (脚本启动时一次性部署, 后续每档调用)
BENCH_HELPER_NAME="vemb_cross_bench_helper.sh"

deploy_bench_helper() {
    ssh "$CLIENT" "cat > /tmp/$BENCH_HELPER_NAME" <<'BENCH_EOF'
#!/bin/bash
# 不用 set -e: hpc VSIM/VADD/VREM memtier 有 test-time 不退出 bug, 用 timeout 兜底,
# 被强杀时退出码非零 (124/137) 不能误判失败。
op_type=$1; t=$2; c=$3; p=$4; test_time=$5
host=$6; port=$7; dim=$8; prefix=$9; kmin=${10}; kmax=${11}
fixed_vec="${12}"; memtier=${13}; out_file=${14}; is_hpc="${15:-0}"

# 用数组传参, 避免 eval/嵌套引号问题
cmd=("$memtier" -s "$host" -p "$port" -t "$t" -c "$c" --pipeline="$p" \
    --test-time="$test_time" --hide-histogram)

case "$op_type" in
    VEMB)
        if [ "$is_hpc" = "1" ]; then
            cmd+=(--protocol vemb_v16 --vemb-v16-dim "$dim" --ratio=0:1 --key-pattern=R:R)
        else
            cmd+=(--protocol=resp3 --command="VEMB myset __key__ raw" --command-key-pattern=R)
        fi ;;
    VSIM)
        if [ "$is_hpc" = "1" ]; then
            cmd+=(--protocol vemb_v16 --vemb-v16-dim "$dim" --vemb-v16-vsim --ratio=0:1 --key-pattern=R:R)
        else
            cmd+=(--protocol=resp3 --command="VSIM myset VALUES $dim $FIXED_VECTOR COUNT 1" --command-key-pattern=R)
        fi ;;
    VSIM_2KEY)
        if [ "$is_hpc" = "1" ]; then
            cmd+=(--protocol vemb_v16 --vemb-v16-dim "$dim" --vemb-v16-vsim-key-key --ratio=0:1 --key-pattern=R:R)
        else
            cmd+=(--protocol=resp3 --command="VEMB myset __key__ raw" --command-key-pattern=R)
        fi ;;
    VADD)
        if [ "$is_hpc" = "1" ]; then
            cmd+=(--protocol vemb_v16 --vemb-v16-dim "$dim" --ratio=1:0 --key-pattern=S:S)
        else
            cmd+=(--protocol=resp3 --command="VADD myset VALUES $dim $fixed_vec __key__" --command-key-pattern=S)
        fi ;;
    VREM)
        if [ "$is_hpc" = "1" ]; then
            cmd+=(--protocol vemb_v16 --vemb-v16-dim "$dim" --vemb-v16-vrem --ratio=1:0 --key-pattern=S:S)
        else
            cmd+=(--protocol=resp3 --command="VREM myset __key__" --command-key-pattern=S)
        fi ;;
esac
cmd+=(--key-prefix="$prefix" --key-minimum="$kmin" --key-maximum="$kmax")

# memtier needs time to drain in-flight requests after test_time (e.g. baseline
# VADD at high in-flight counts drains slowly), so keep a generous headroom.
timeout -k 5 $((test_time + 60)) "${cmd[@]}" > "$out_file" 2>&1 || true
BENCH_EOF
    ssh "$CLIENT" "chmod +x /tmp/$BENCH_HELPER_NAME" 2>/dev/null
}

# ----------------------------------------------------------------------------
# 一档: 已 start_server + prefill, 跑 memtier + 采集 sar/jiffies
run_one_config() {
    local server_type=$1 idx=$2
    local t=${TS[$idx]} c=${CS[$idx]} p=${PS[$idx]}
    local prefix kmin kmax
    read prefix kmin kmax < <(op_key_range)
    local raw_local="$RAWDIR/${server_type}_${OP_TYPE}_t${t}_c${c}_p${p}.log"
    local raw_remote="/tmp/${OP_TYPE}_${server_type}_t${t}_c${c}_p${p}.log"
    log "  [$server_type] OP=$OP_TYPE t=$t c=$c pipeline=$p time=${TEST_TIME}s"

    local client_memtier
    if [ "$server_type" = "hpc" ]; then
        client_memtier=$CLIENT_HPC_MEMTIER
    else
        client_memtier=$CLIENT_BASELINE_MEMTIER
    fi

    # === sar -n DEV 1 后台跑 (TEST_TIME + 5 秒, 自动退出) ===
    local sar_log="$RAWDIR/sar_${server_type}_${OP_TYPE}_t${t}_c${c}_p${p}.log"
    rm -f "$sar_log"
    nohup sar -n DEV 1 $((TEST_TIME + 5)) > "$sar_log" 2>&1 &
    local sar_pid=$!

    # === jiffies before (ut/st split) ===
    local jb_ut jb_st
    read jb_ut jb_st < <(snapshot_jiffies $SERVER_PORT)
    local jb_iowait=$(awk '/^cpu /{print $6}' /proc/stat 2>/dev/null)
    local jb_si=$(snapshot_si)
    local jb_hi=$(awk '/^cpu /{print $7}' /proc/stat 2>/dev/null)
    local jb_ns=$(date +%s%N)

    # === run memtier on CLIENT ===
    # is_hpc 通过位置参数 ${15} 传给 helper (ssh 不传环境变量)
    local is_hpc_arg=0
    [ "$server_type" = "hpc" ] && is_hpc_arg=1
    ssh "$CLIENT" "bash /tmp/$BENCH_HELPER_NAME \
            $OP_TYPE $t $c $p $TEST_TIME \
            $SERVER_HOST $SERVER_PORT $DIM $prefix $kmin $kmax \
            '$FIXED_VECTOR' $client_memtier $raw_remote $is_hpc_arg" 2>&1
    ssh "$CLIENT" "cat $raw_remote" > "$raw_local" 2>/dev/null || true

    # === jiffies after (ut/st split) ===
    local ja_ut ja_st
    read ja_ut ja_st < <(snapshot_jiffies $SERVER_PORT)
    local ja_ns=$(date +%s%N)
    local elapsed_ns=$((ja_ns - jb_ns > 0 ? ja_ns - jb_ns : TEST_TIME * 1000000000))
    # core 分母 = 纳秒实测窗口 (与脚本13对齐: 分子分母同区间)
    local cores=$(awk -v du=$((ja_ut - jb_ut)) -v ds=$((ja_st - jb_st)) -v s=$elapsed_ns 'BEGIN{printf "%.2f", (du+ds)/100.0/(s/1000000000)}')
    local core_ut=$(awk -v d=$((ja_ut - jb_ut)) -v s=$elapsed_ns 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
    local core_st=$(awk -v d=$((ja_st - jb_st)) -v s=$elapsed_ns 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
    local ja_iowait=$(awk '/^cpu /{print $6}' /proc/stat 2>/dev/null)
    local ja_si=$(snapshot_si)
    local ja_hi=$(awk '/^cpu /{print $7}' /proc/stat 2>/dev/null)
    local c_iowait=$(awk -v d=$((ja_iowait - jb_iowait)) -v s=$elapsed_ns 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
    local c_si=$(awk -v d=$((ja_si - jb_si)) -v s=$elapsed_ns 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
    local c_hi=$(awk -v d=$((ja_hi - jb_hi)) -v s=$elapsed_ns 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
    local rss=$(snapshot_rss $SERVER_PORT)

    # === sar %ifutil 解析 ===
    wait $sar_pid 2>/dev/null || true
    sleep 1  # 等 sar flush 最后一个样本
    local nic_util
    nic_util=$(awk -v iface="$NIC_IFACE" '$2==iface && NF>=9 {sum+=$NF; n++} END {if(n>0) printf "%.1f", sum/n; else print "0.0"}' "$sar_log" 2>/dev/null)
    [ -z "$nic_util" ] && nic_util="N/A"

    # === memtier Totals 解析 (按 NF 自动判: NF>=9 带 Hits/Misses / NF>=7 普通) ===
    local totals ops avg p50 p99 kb
    totals=$(grep "^Totals" "$raw_local" | tail -1)
    read ops avg p50 p99 kb < <(
        echo "$totals" | awk '{
            if (NF>=9)      printf "%s %s %s %s %s", $2,$5,$6,$7,$9
            else if (NF>=7) printf "%s %s %s %s %s", $2,$3,$4,$5,$7
            else            printf "0 NA NA NA NA"
        }'
    )

    # VSIM_2KEY baseline: 1 memtier op = 1× VEMB raw, need 2× for 2-key compare
    local ops_note=""
    if [ "$OP_TYPE" = "VSIM_2KEY" ] && [ "$server_type" = "baseline" ]; then
        ops_note=" (÷2, 2-key equiv)"
        ops=$(awk "BEGIN {printf \"%.2f\", $ops/2}")
    fi
    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
        "$OP_TYPE" "$server_type" "$t" "$c" "$p" "$ops" "$avg" "$p50" "$p99" "$kb" "$cores" \
        "$nic_util" "$c_iowait" "$core_ut" "$core_st" "$c_si" "$c_hi" "$rss" >> "$TSV"
    log "    => ops/s=$ops$ops_note  avg=${avg}ms  p50=${p50}ms  p99=${p99}ms  cores=$cores(ut=$core_ut st=$core_st)  ${NIC_IFACE}_util=${nic_util}%  iowait=$c_iowait si=$c_si hi=$c_hi rss=${rss}KB"
    rm -f "$raw_local"
    ssh "$CLIENT" "rm -f $raw_remote" 2>/dev/null
}

# ============================================================================
trap 'stop_server' EXIT INT TERM

log "=== ${OP_TYPE} 跨节点 100G sweep (${NCONFIGS} 档 × ${SERVERS_ONLY} server) ==="
log "SERVER=本地 ($SERVER_HOST:$SERVER_PORT)  CLIENT=$CLIENT  NIC_IFACE=$NIC_IFACE"
log "TEST_TIME=${TEST_TIME}s/档, DIM=$DIM"
log "${OP_TYPE}: NUM_KEYS=$NUM_KEYS single myset"

# 检查 CLIENT ssh 可达 + memtier 路径
log "checking ssh $CLIENT reachable..."
ssh "$CLIENT" "true" 2>/dev/null || { log "ERROR: cannot ssh to CLIENT=$CLIENT"; exit 1; }
ssh "$CLIENT" "test -x $CLIENT_BASELINE_MEMTIER && test -x $CLIENT_HPC_MEMTIER" 2>/dev/null \
    || { log "ERROR: CLIENT memtier missing. expected:
    $CLIENT_BASELINE_MEMTIER
    $CLIENT_HPC_MEMTIER"; exit 1; }
log "  ssh ok, memtier ok"

# 部署 bench helper (一次性, 不是每档都传)
log "deploying bench helper to $CLIENT..."
deploy_bench_helper

printf "op\tserver_type\tt\tc\tpipeline\tops_sec\tavg_lat_ms\tp50_ms\tp99_ms\tkb_sec\tcores\tnic_util_pct\tiowait\tcore_ut\tcore_st\tsi\thi\trss_kb\n" > "$TSV"

for server_type in $SERVERS_ONLY; do
    if [ "$OP_TYPE" = "VEMB" ]; then
        # VEMB: 不重启 server, start → prefill → run all configs → stop
        log "--- [$server_type] VEMB (server 不重启, ${NCONFIGS} 档连跑) ---"
        case $server_type in
            baseline) start_baseline ;;
            hpc)      start_hpc ;;
            *) echo "ERROR: unknown server_type=$server_type"; exit 2 ;;
        esac
        prefill $server_type
        # 验证 server 在 prefill 后仍然存活
        if ! $REDIS_DIR/src/redis-cli -h 127.0.0.1 -p $SERVER_PORT PING 2>/dev/null | grep -q PONG; then
            log "FATAL: [$server_type] server unreachable after prefill"
            exit 1
        fi
        for ((i=0; i<NCONFIGS; i++)); do
            log "--- [$server_type] config $((i+1))/${NCONFIGS}: t=${TS[$i]} c=${CS[$i]} p=${PS[$i]} ---"
            run_one_config $server_type $i
        done
        stop_server
    else
        # VSIM/VADD/VREM: 每档重启 server (修改数据, 需要干净起点)
        for ((i=0; i<NCONFIGS; i++)); do
            log "--- [$server_type] config $((i+1))/${NCONFIGS}: t=${TS[$i]} c=${CS[$i]} p=${PS[$i]} ---"
            case $server_type in
                baseline) start_baseline ;;
                hpc)      start_hpc ;;
                *) echo "ERROR: unknown server_type=$server_type"; exit 2 ;;
            esac
            prefill $server_type
            run_one_config $server_type $i
            stop_server
        done
    fi
done

log "=== DONE ==="
log "TSV : $TSV"
log "raw : $RAWDIR/*.log"
echo "----- summary -----"
column -t -s $'\t' "$TSV"
