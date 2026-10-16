#!/usr/bin/env bash
# baseline 存储默认 NOQUANT(fp32, VEMB RAW 返回 f32 与 hpc 响应对齐); BASELINE_NOQUANT=0 恢复 int8 量化
# ============================================================================
# run_vemb_cross_node_sweep.sh
# 跨节点 100G 网卡 VEMB/VSIM/VADD/VREM 吞吐/延迟 sweep
#   —— 同一 OP 下, baseline redis (RESP3) 与 hpc-redis (VEMB V16) 各跑一遍对比
#   —— 9 档 (t,c,pipeline) 配置矩阵 + sar -n DEV 1 网卡利用率统计
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
#   PROFILE=1 SERVERS_ONLY=hpc TS="64" CS="4" PS="32" \
#       HPC_PIO=3 HPC_SNW=3 SERVER_HOST=192.168.90.111 \
#       OP_TYPE=VEMB bash ...                                # 单档 + perf/火焰图
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
# baseline 免 prefill: 兼容 RDB 存在则加载 (auto), 不存在走 prefill; 0=强制 prefill
BASELINE_RDB=${BASELINE_RDB:-auto}
RDB_DIR=${RDB_DIR:-$HPC_DIR/benchmark/baseline_rdb}
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

# === 读分布 (仅 baseline VEMB; hpc 分支固定 R:R) ===
KEY_PATTERN=${KEY_PATTERN:-R}
ZIPF_S=${ZIPF_S:-}
case "$KEY_PATTERN" in
    R) ;;
    Z)
        [ "$OP_TYPE" = "VEMB" ] || { echo "ERROR: KEY_PATTERN=Z only supports OP_TYPE=VEMB"; exit 2; }
        [ -n "$ZIPF_S" ] || { echo "ERROR: ZIPF_S is required when KEY_PATTERN=Z"; exit 2; }
        awk -v s="$ZIPF_S" 'BEGIN { exit !(s ~ /^[0-9]+([.][0-9]+)?$/ && s > 0) }' || \
            { echo "ERROR: ZIPF_S must be a positive decimal (got: $ZIPF_S)"; exit 2; }
        ;;
    *) echo "ERROR: KEY_PATTERN must be R or Z (got: $KEY_PATTERN)"; exit 2 ;;
esac
[ "$KEY_PATTERN" = "Z" ] || [ -z "$ZIPF_S" ] || { echo "ERROR: ZIPF_S is only allowed when KEY_PATTERN=Z"; exit 2; }

# === server 固定口径 (跟本地脚本一致) ===
BASELINE_IO_THREADS=${BASELINE_IO_THREADS:-4}
HPC_PIO=${HPC_PIO:-2}
HPC_SNW=${HPC_SNW:-2}

# === profiling (口径对齐 scripts/run_aeron_cross_node_flamegraph.sh) ===
# PROFILE=1 时 hpc 档位采 perf + 出火焰图 (server 全 TID + client memtier);
# baseline 不采。PROFILE_TIERS 空格分隔 t64_c4_p32 样式, 空 = 全部档。
PROFILE=${PROFILE:-0}
PROFILE_TIERS=${PROFILE_TIERS:-}
SERVER_FLAME_DURATION=${SERVER_FLAME_DURATION:-25}
FREQ=${FREQ:-99}
EVENT=${EVENT:-cycles}
FLAMEGRAPH_DIR=${FLAMEGRAPH_DIR:-/root/FlameGraph}
CLIENT_FLAMEGRAPH_DIR=${CLIENT_FLAMEGRAPH_DIR:-/root/FlameGraph}
REQUIRE_VEMB_THREAD_SAMPLES=${REQUIRE_VEMB_THREAD_SAMPLES:-1}

# === CPU 绑核 (SERVER 端) ===
BASELINE_SERVER_CPUSET=${BASELINE_SERVER_CPUSET:-31-38}
HPC_SERVER_CPUSET=${HPC_SERVER_CPUSET:-41-48}

# === 9 档配置矩阵 (跟本地脚本一致) ===
# 7 档精简矩阵 (原 9 档, 20260826 削减: 保留低并发斜率 + 高并发饱和 + 两条 c 扫描)
TS_DEFAULT=( 1  1  4 16 64 64 64)
CS_DEFAULT=( 1  1  1  1  1  4 16)
PS_DEFAULT=( 1 32 32 32 32 32 32)
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
# 返回兼容 RDB 路径 (不存在返回空)。VADD 是纯写测试, 永不加载。
baseline_rdb_path() {
    [ "$BASELINE_RDB" = "0" ] && return 1
    [ "$OP_TYPE" = "VADD" ] && return 1
    local tag=$([ "${BASELINE_NOQUANT:-1}" = "0" ] && echo INT8 || echo NOQUANT)
    local f="$RDB_DIR/${NUM_KEYS}K_${DIM}D_${tag}_myset.rdb"
    [ -f "$f" ] || return 1
    echo "$f"
}

start_baseline() {
    log "  启动 baseline redis-server (io-threads=$BASELINE_IO_THREADS)..."
    local rdb
    rdb=$(baseline_rdb_path || true)
    local dir_args=(--dir $DATA_DIR)
    if [ -n "$rdb" ]; then
        dir_args=(--dir "$(dirname "$rdb")" --dbfilename "$(basename "$rdb")")
        log "  加载预填充 RDB: $rdb (跳过 prefill)"
    fi
    taskset -c $BASELINE_SERVER_CPUSET \
        $REDIS_DIR/src/redis-server \
            --port $SERVER_PORT --bind 0.0.0.0 --protected-mode no \
            --tcp-backlog 16384 --tcp-keepalive 1800 --timeout 0 \
            --io-threads $BASELINE_IO_THREADS --io-threads-do-reads yes \
            --appendonly no --save '' \
            "${dir_args[@]}" --logfile $DATA_DIR/baseline.log \
            --daemonize yes
    wait_port $SERVER_PORT || { log "FAIL: baseline server"; exit 1; }
    if [ -n "$rdb" ]; then
        # 就绪判据: VCARD 为数字 (PING 在 LOADING 期间也有回复)
        local v=""
        for _ in $(seq 1 300); do
            v=$($REDIS_DIR/src/redis-cli -h 127.0.0.1 -p $SERVER_PORT VCARD myset 2>/dev/null | grep -E '^[0-9]+$') && break
            sleep 0.1
        done
        [ "$v" = "$NUM_KEYS" ] || { log "FAIL: RDB 加载校验 VCARD=$v != $NUM_KEYS ($rdb)"; exit 1; }
        log "  RDB 加载完成: VCARD=$v"
    fi
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
            --vemb-v16-proxy-io-threads $HPC_PIO \
            --vemb-v16-supernode-workers $HPC_SNW \
            --vemb-v16-warm-regions-manifest $MANIFEST \
            --vemb-v16-reset-warm-regions yes \
            --appendonly no --save '' \
            --dir $DATA_DIR --logfile $DATA_DIR/hpc.log --loglevel warning \
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
    # baseline 已由 RDB 预填充 → 跳过 (FLUSHDB 也跳过)
    if [ "$server_type" = "baseline" ] && [ -n "$(baseline_rdb_path || true)" ]; then
        log "  [$server_type] OP_TYPE=$OP_TYPE 使用 RDB 预填充, 跳过 prefill"
        return 0
    fi
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
                printf " item:%d%s\n", i, (ENVIRON["BASELINE_NOQUANT"]=="0" ? "" : " NOQUANT");
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
kp="${16:-R}"; zipf_s="${17:-}"
perf_out="${18:-}"; perf_freq="${19:-99}"; perf_event="${20:-cycles}"
[ "$zipf_s" = "-" ] && zipf_s=""
[ "$perf_out" = "-" ] && perf_out=""
[ "$zipf_s" = "-" ] && zipf_s=""

# 用数组传参, 避免 eval/嵌套引号问题
cmd=("$memtier" -s "$host" -p "$port" -t "$t" -c "$c" --pipeline="$p" \
    --test-time="$test_time" --hide-histogram)

case "$op_type" in
    VEMB)
        if [ "$is_hpc" = "1" ]; then
            cmd+=(--protocol vemb_v16 --vemb-v16-dim "$dim" --ratio=0:1 --key-pattern=R:R)
        elif [ "$kp" = "Z" ]; then
            # arbitrary command 模式禁止主 --key-pattern, 指数锚点走
            # --command-key-pattern=Z; --distinct-client-seed 对齐 aeron 每
            # worker 独立种子, 避免 TCP 默认 seed 0 下所有线程同一 zipf
            # 序列同步热点。
            cmd+=(--protocol=resp3 --command="VEMB myset __key__ raw" --command-key-pattern=Z \
                --key-zipfian-s="$zipf_s" --distinct-client-seed)
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
            cmd+=(--protocol=resp3 --command="VADD myset VALUES $dim $fixed_vec __key__$( [ "${BASELINE_NOQUANT:-1}" != 0 ] && echo " NOQUANT" )" --command-key-pattern=S)
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
# perf 包在 timeout 外层: timeout 兜底杀 memtier 时 perf 正常收尾落盘.
if [ -n "$perf_out" ]; then
    perf record -F "$perf_freq" -g -e "$perf_event" -o "$perf_out" -- \
        timeout -k 5 $((test_time + 60)) "${cmd[@]}" > "$out_file" 2>&1 || true
else
    timeout -k 5 $((test_time + 60)) "${cmd[@]}" > "$out_file" 2>&1 || true
fi
BENCH_EOF
    ssh "$CLIENT" "chmod +x /tmp/$BENCH_HELPER_NAME" 2>/dev/null
}

# ----------------------------------------------------------------------------
# profiling helpers (server 本地 / client 经 ssh; 口径对齐 aeron flamegraph 脚本)
find_hpc_pid() {  # 按端口扫 /proc cmdline, 与 snapshot_jiffies 同一匹配规则
    local pid
    for pid in $(pgrep -x redis-server); do
        if tr '\0' ' ' < /proc/$pid/cmdline 2>/dev/null | grep -Eq ":${SERVER_PORT}\$|:${SERVER_PORT} "; then
            echo "$pid"; return 0
        fi
    done
    return 1
}

profile_enabled_for_tier() {  # server_type t c p
    [ "$PROFILE" = "1" ] || return 1
    [ "$1" = "hpc" ] || return 1
    if [ -n "$PROFILE_TIERS" ]; then
        local tag="t$2_c$3_p$4"
        [[ " $PROFILE_TIERS " == *" $tag "* ]] || return 1
    fi
    return 0
}

profile_server_start() {  # run_dir — 绑 server 全部 TID (不漏 supernode worker)
    local run=$1 pid io sn tids
    pid=$(find_hpc_pid) || { log "FAIL: [prof] hpc server pid not found (port $SERVER_PORT)"; exit 1; }
    ps -T -p "$pid" -o tid=,comm= >"$run/server.threads.txt"
    # TCP 模式不创建 vemb-io-/vemb-sn- 线程 (aeron proxy 专属); 计数仅记录.
    # perf 绑定全部 TID, TCP 下 vemb 工作落在 redis I/O 线程, 同样被采到.
    io=$(grep -c 'vemb-io-' "$run/server.threads.txt" || true)
    sn=$(grep -c 'vemb-sn-' "$run/server.threads.txt" || true)
    log "    [prof] server threads total=$(wc -l <"$run/server.threads.txt") vemb_io=$io vemb_sn=$sn (tcp 预期为 0)"
    tids=$(awk '{ printf "%s%s", sep, $1; sep="," }' "$run/server.threads.txt")
    nohup perf record -F "$FREQ" -g -e "$EVENT" --tid "$tids" \
        -o "$run/server.perf.data" -- sleep "$SERVER_FLAME_DURATION" \
        >"$run/server.perf.log" 2>&1 &
    echo $! >"$run/server.perf.pid"
    log "    [prof] server perf started dur=${SERVER_FLAME_DURATION}s io=$io sn=$sn pid=$(cat "$run/server.perf.pid")"
}

profile_server_finish() {  # run_dir — 等 perf 结束 → script → collapse → svg + 样本校验
    local run=$1 perf_pid
    perf_pid=$(cat "$run/server.perf.pid")
    for _ in $(seq 1 100); do kill -0 "$perf_pid" 2>/dev/null || break; sleep 0.1; done
    ! kill -0 "$perf_pid" 2>/dev/null || { log "FAIL: [prof] server perf still running"; exit 1; }
    perf script -i "$run/server.perf.data" >"$run/server.perf.script"
    grep -q '\[kernel.kallsyms\]' "$run/server.perf.script" || { log "FAIL: [prof] no kernel symbols in server samples"; exit 1; }
    grep -q 'redis-server' "$run/server.perf.script" || { log "FAIL: [prof] no redis-server samples"; exit 1; }
    if [ "$REQUIRE_VEMB_THREAD_SAMPLES" = "1" ]; then
        # TCP 口径的 vemb 服务路径锚点 (proxy_submit_vemb 是 static, O3/LTO 下内联不可见)
        grep -q 'vemb_v16_supernode_handle_vemb_job' "$run/server.perf.script" || { log "FAIL: [prof] no supernode vemb samples"; exit 1; }
        grep -q 'proxy_io_epoll_thread_main' "$run/server.perf.script" || { log "FAIL: [prof] no proxy-io samples"; exit 1; }
    fi
    "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" "$run/server.perf.script" >"$run/server.collapsed.txt"
    awk '{ sub(/^[^;]+;/, "all;redis-server;"); print }' \
        "$run/server.collapsed.txt" >"$run/server.process.collapsed.txt"
    "$FLAMEGRAPH_DIR/flamegraph.pl" \
        --title "vemb-v16 cross-node tcp server ${run##*/}" \
        --subtitle "all server TIDs user+kernel; event=${EVENT}@${FREQ}Hz; pio=${HPC_PIO} snw=${HPC_SNW}" \
        "$run/server.process.collapsed.txt" >"$run/server.svg"
    log "    [prof] server flamegraph -> $run/server.svg"
}

profile_client_finish() {  # run_dir remote_perf — client 侧出图后取回
    local run=$1 rperf=$2
    ssh "$CLIENT" bash -s -- "$rperf" "$CLIENT_FLAMEGRAPH_DIR" "$FREQ" "$EVENT" <<'CLIENT_FLAME_EOF'
set -euo pipefail
rperf=$1; fg=$2; freq=$3; event=$4
[ -x "$fg/stackcollapse-perf.pl" ] && [ -x "$fg/flamegraph.pl" ]
perf script -i "$rperf" >"$rperf.script"
"$fg/stackcollapse-perf.pl" "$rperf.script" >"$rperf.collapsed"
"$fg/flamegraph.pl" --title "vemb-v16 cross-node tcp client ${rperf##*/}" \
    --subtitle "memtier client; event=${event}@${freq}Hz" \
    "$rperf.collapsed" >"$rperf.svg"
CLIENT_FLAME_EOF
    scp -q "$CLIENT:$rperf" "$run/client.perf.data" || log "WARN: [prof] client.perf.data fetch failed"
    scp -q "$CLIENT:$rperf.script" "$run/client.perf.script"
    scp -q "$CLIENT:$rperf.collapsed" "$run/client.collapsed.txt"
    scp -q "$CLIENT:$rperf.svg" "$run/client.svg"
    ssh "$CLIENT" "rm -f $rperf $rperf.script $rperf.collapsed $rperf.svg" 2>/dev/null
    log "    [prof] client flamegraph -> $run/client.svg"
}

# ----------------------------------------------------------------------------
# 一档: 已 start_server + prefill, 跑 memtier + 采集 sar/jiffies
run_one_config() {
    local server_type=$1 idx=$2
    local t=${TS[$idx]} c=${CS[$idx]} p=${PS[$idx]}
    local prefix kmin kmax
    read prefix kmin kmax < <(op_key_range)
    local kp_tag=""
    [ "$KEY_PATTERN" = "Z" ] && kp_tag="_zipf${ZIPF_S}"
    local raw_local="$RAWDIR/${server_type}_${OP_TYPE}_t${t}_c${c}_p${p}${kp_tag}.log"
    local raw_remote="/tmp/${OP_TYPE}_${server_type}_t${t}_c${c}_p${p}${kp_tag}.log"
    local prof=0
    profile_enabled_for_tier "$server_type" $t $c $p && prof=1
    local prof_dir="$OUTDIR/profile/${server_type}_t${t}_c${c}_p${p}${kp_tag}"
    local perf_arg="-"
    [ "$prof" = "1" ] && { mkdir -p "$prof_dir"; perf_arg="/tmp/${OP_TYPE}_${server_type}_t${t}_c${c}_p${p}.perf.data"; }
    log "  [$server_type] OP=$OP_TYPE t=$t c=$c pipeline=$p time=${TEST_TIME}s$([ "$prof" = "1" ] && echo " [profiled]")"

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

    # === server perf 起 (memtier 前启动, 绑全 TID) ===
    [ "$prof" = "1" ] && profile_server_start "$prof_dir"

    # === jiffies before (ut/st split) ===
    local jb_ut jb_st
    read jb_ut jb_st < <(snapshot_jiffies $SERVER_PORT)
    local jb_iowait=$(awk '/^cpu /{print $6}' /proc/stat 2>/dev/null)
    local jb_si=$(snapshot_si)
    local jb_hi=$(awk '/^cpu /{print $7}' /proc/stat 2>/dev/null)
    local jb_ns=$(date +%s%N)

    # === run memtier on CLIENT ===
    # is_hpc 通过位置参数 ${15} 传给 helper (ssh 不传环境变量); ${16}/${17} 为
    # KEY_PATTERN / ZIPF_S (空 ZIPF_S 用 - 占位, 保证位置参数不漂移)
    local is_hpc_arg=0
    [ "$server_type" = "hpc" ] && is_hpc_arg=1
    local zipf_arg=${ZIPF_S:--}
    ssh "$CLIENT" "bash /tmp/$BENCH_HELPER_NAME \
            $OP_TYPE $t $c $p $TEST_TIME \
            $SERVER_HOST $SERVER_PORT $DIM $prefix $kmin $kmax \
            '$FIXED_VECTOR' $client_memtier $raw_remote $is_hpc_arg $KEY_PATTERN $zipf_arg \
            $perf_arg $FREQ $EVENT" 2>&1
    ssh "$CLIENT" "cat $raw_remote" > "$raw_local" 2>/dev/null || true

    # === server/client 火焰图 (workload 结束后生成) ===
    if [ "$prof" = "1" ]; then
        profile_server_finish "$prof_dir"
        profile_client_finish "$prof_dir" "$perf_arg"
    fi

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

    # === memtier Totals 解析 (按 NF 自动判: NF>=11 新vemb格式 / NF>=9 带Hits/Misses / NF>=7 普通) ===
    local totals ops avg p50 p99 kb
    totals=$(grep "^Totals" "$raw_local" | tail -1)
    read ops avg p50 p99 kb < <(
        echo "$totals" | awk '{
            if (NF>=11)      printf "%s %s %s %s %s", $2,$7,$8,$9,$11
            else if (NF>=9)  printf "%s %s %s %s %s", $2,$5,$6,$7,$9
            else if (NF>=7)  printf "%s %s %s %s %s", $2,$3,$4,$5,$7
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

# profiling preflight: 两端工具 + 火焰窗口必须落在测试窗口内
if [ "$PROFILE" = "1" ]; then
    command -v perf >/dev/null || { log "ERROR: PROFILE=1 needs perf on SERVER"; exit 1; }
    [ -x "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" ] && [ -x "$FLAMEGRAPH_DIR/flamegraph.pl" ] \
        || { log "ERROR: PROFILE=1 needs FlameGraph at $FLAMEGRAPH_DIR"; exit 1; }
    ssh "$CLIENT" "command -v perf >/dev/null && test -x $CLIENT_FLAMEGRAPH_DIR/stackcollapse-perf.pl && test -x $CLIENT_FLAMEGRAPH_DIR/flamegraph.pl" 2>/dev/null \
        || { log "ERROR: PROFILE=1 needs perf + FlameGraph on CLIENT ($CLIENT_FLAMEGRAPH_DIR)"; exit 1; }
    [ "$SERVER_FLAME_DURATION" -lt "$TEST_TIME" ] \
        || { log "ERROR: SERVER_FLAME_DURATION=$SERVER_FLAME_DURATION must be < TEST_TIME=$TEST_TIME"; exit 1; }
    log "  profiling enabled: server_flame=${SERVER_FLAME_DURATION}s@${FREQ} event=$EVENT tiers=${PROFILE_TIERS:-all}"
fi

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
