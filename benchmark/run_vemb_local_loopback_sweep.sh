#!/usr/bin/env bash
# ============================================================================
# run_vemb_local_loopback_sweep.sh
# 单实例本地回环 VEMB/VSIM/VADD/VREM 吞吐/延迟 sweep
#   —— 同一 OP 下, baseline redis 与 hpc-redis 各跑一遍对比
#   —— 17 档 (t,c,pipeline) 配置矩阵, 一次出 34 行 TSV 直接填评测报告表格
#
# 用法:
#   OP_TYPE=VEMB bash benchmark/run_vemb_local_loopback_sweep.sh   # 默认 VEMB
#   OP_TYPE=VSIM bash benchmark/run_vemb_local_loopback_sweep.sh
#   OP_TYPE=VADD bash benchmark/run_vemb_local_loopback_sweep.sh
#   OP_TYPE=VREM bash benchmark/run_vemb_local_loopback_sweep.sh
#   TEST_TIME=3 OP_TYPE=VSIM bash ...                              # smoke 快验
#   SERVERS_ONLY="baseline" OP_TYPE=VEMB bash ...                  # 只跑 baseline
#
# 17 档配置矩阵 (默认):
#   - pipeline=1/4/8/16/32 (t=1 c=1)        5 档
#   - t 翻倍到 64 (c=1 p=32)                6 档
#   - c 递增到 64 (t=64 p=32)               6 档
#
# 固定口径:
#   baseline redis-server: io-threads=4 io-threads-do-reads=yes, RESP3
#   hpc-redis: pio=2 snw=2 (--vemb-v16-proxy-io-threads/-supernode-workers)
#   DIM=300 FP32, 所有 OP 统一用单 myset + NUM_KEYS elements
#   server NUMA0 taskset, memtier NUMA1 taskset
#   transport=TCP 127.0.0.1, VEMB RAW=1 (INT8 量化直传)
#
# 命令对照 (参考 hpc_redis_v{sim,add,rem}_max_tput.sh / redis_baseline_v{sim,add,rem}_max_tput.sh):
#   OP_TYPE   | hpc memtier                                       | baseline memtier (RESP3)
#   ----------+---------------------------------------------------+-----------------------------------------------
#   VEMB      | --protocol vemb_v16 --ratio=0:1 R:R item:        | --command="VEMB myset __key__ raw" item: R
#   VSIM      | --protocol vemb_v16 --vemb-v16-vsim R:R item:     | --command="VSIM myset VALUES $DIM $FIXED_VECTOR COUNT 1" R item:
#   VSIM_2KEY | --protocol vemb_v16 --vemb-v16-vsim-key-key R:R   | --command="VEMB myset __key__ raw" R:R item: (2 fetches, no client cosine)
#   VADD      | --protocol vemb_v16 --ratio=1:0 S:S item:         | --command="VADD myset VALUES $DIM $FIXED_VECTOR __key__" S item:
#   VREM      | --protocol vemb_v16 --vemb-v16-vrem 1:0 S:S item: | --command="VREM myset __key__" S item:
#
# 编译口径 (服务器一致):
#   cd /root/gqs/codespace/redis-8.6.3 && \
#   make -C deps jemalloc && \
#   make CFLAGS="-O2 -pipe -fno-lto" LDFLAGS="-O2 -pipe -fno-lto" CC="gcc -fuse-ld=bfd"
# ============================================================================
set -uo pipefail

# === 路径 ===
REDIS_DIR=${REDIS_DIR:-/root/gqs/codespace/redis-8.6.3}
HPC_DIR=${HPC_DIR:-/root/gqs/codespace/UnifiedBus/hpc-redis}
BASELINE_MEMTIER=${BASELINE_MEMTIER:-/root/gqs/codespace/UnifiedBus/memtier_benchmark_origin/memtier_benchmark}
HPC_MEMTIER=${HPC_MEMTIER:-$HPC_DIR/memtier_benchmark/memtier_benchmark}

DATA_DIR=${DATA_DIR:-/tmp/redis-loopback-sweep}

# === OP_TYPE ===
OP_TYPE=${OP_TYPE:-VEMB}
case "$OP_TYPE" in
    VEMB|VSIM|VSIM_2KEY|VADD|VREM) ;;
    *) echo "ERROR: OP_TYPE must be one of VEMB/VSIM/VSIM_2KEY/VADD/VREM (got: $OP_TYPE)"; exit 2 ;;
esac

# === 测试参数 ===
PORT=${PORT:-6390}
TEST_TIME=${TEST_TIME:-20}
DIM=${DIM:-300}
NUM_VSETS=${NUM_VSETS:-16}
VECTORS_PER_VSET=${VECTORS_PER_VSET:-6250}   # 16*6250 = 100K (仅 VEMB)
KEY_OFFSET=${KEY_OFFSET:-1}
# VSIM/VADD/VREM 用单 myset + N elements
NUM_KEYS=${NUM_KEYS:-100000}

# === server 固定口径 ===
BASELINE_IO_THREADS=${BASELINE_IO_THREADS:-4}
HPC_PIO=${HPC_PIO:-2}
HPC_SNW=${HPC_SNW:-2}
MANIFEST=${MANIFEST:-$HPC_DIR/examples/vemb_v16_warm_regions_111.yaml}

# === CPU 绑核 ===
BASELINE_SERVER_CPUSET=${BASELINE_SERVER_CPUSET:-31-38}
HPC_SERVER_CPUSET=${HPC_SERVER_CPUSET:-41-48}
CLIENT_CPUSET=${CLIENT_CPUSET:-96-191}
NUMA_NODE_SERVER=${NUMA_NODE_SERVER:-0}
NUMA_NODE_CLIENT=${NUMA_NODE_CLIENT:-1}

# === 17 档配置矩阵 (并行数组 TS/CS/PS) ===
# 默认完整跑; 可用 TS/CS/PS 环境变量覆盖 (空格分隔, 三数组等长)
#   smoke: TS="1" CS="1" PS="1" bash ... (1 档)
#   自定义: TS="1 64" CS="1 4" PS="1 32" bash ... (2 档)
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
OUTDIR=${OUTDIR:-benchmark/results/${OP_TYPE,,}_loopback_sweep/${TIMESTAMP}}
RAWDIR="$OUTDIR/raw"
TSV="$OUTDIR/summary.tsv"

ulimit -n 200000
mkdir -p "$RAWDIR" "$DATA_DIR"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# ----------------------------------------------------------------------------
# jiffies 采集 (按端口匹配, 避免 pkill 噪音)
snapshot_jiffies() {
    local port=$1 total=0 j
    for pid in $(pgrep -x redis-server); do
        if tr '\0' ' ' < /proc/$pid/cmdline 2>/dev/null | grep -Eq ":${port}\$|:${port} "; then
            j=$(awk '{s+=$14+$15} END{print s+0}' /proc/$pid/task/*/stat 2>/dev/null)
            total=$((total + ${j:-0}))
        fi
    done
    echo $total
}

# ----------------------------------------------------------------------------
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
start_baseline() {
    log "启动 baseline redis-server (io-threads=$BASELINE_IO_THREADS)..."
    taskset -c $BASELINE_SERVER_CPUSET \
        $REDIS_DIR/src/redis-server \
            --port $PORT --bind 127.0.0.1 --protected-mode no \
            --tcp-backlog 16384 \
            --io-threads $BASELINE_IO_THREADS --io-threads-do-reads yes \
            --appendonly no --save '' \
            --dir $DATA_DIR --logfile $DATA_DIR/baseline.log \
            --daemonize yes
    wait_port $PORT || { log "FAIL: baseline server"; exit 1; }
}

start_hpc() {
    log "启动 hpc-redis (pio=$HPC_PIO snw=$HPC_SNW)..."
    local max_vec=$((NUM_KEYS + 1000))
    taskset -c $HPC_SERVER_CPUSET \
        $HPC_DIR/src/redis-server \
            --port $PORT --bind 127.0.0.1 --protected-mode no \
            --tcp-backlog 16384 \
            --io-threads $BASELINE_IO_THREADS --io-threads-do-reads yes \
            --vemb-v16-enabled yes \
            --vemb-v16-dim $DIM \
            --vemb-v16-max-vectors $max_vec \
            --vemb-v16-proxy-io-threads $HPC_PIO \
            --vemb-v16-supernode-workers $HPC_SNW \
            --vemb-v16-warm-regions-manifest $MANIFEST \
            --vemb-v16-reset-warm-regions yes \
            --appendonly no --save '' \
            --dir $DATA_DIR --logfile $DATA_DIR/hpc.log \
            --daemonize yes
    wait_port $PORT || { log "FAIL: hpc server"; exit 1; }
}

stop_server() {
    log "关闭 redis-server..."
    pkill -9 -x redis-server 2>/dev/null
    sleep 1
}

# ----------------------------------------------------------------------------
# baseline VADD 用固定向量 (memtier --command 无法动态生成, 跟 redis_baseline_vadd_max_tput.sh:49-54 对齐)
FIXED_VECTOR=$(awk 'BEGIN{
    srand(42);
    out="";
    for(j=0;j<300;j++){ out=out sprintf("%.5f", rand()*j); if(j<299) out=out " "; }
    print out;
}')

# ----------------------------------------------------------------------------
# OP_TYPE → (prefix, kmin, kmax) 三个 key-range 段
# 所有 OP_TYPE 统一走单 myset + NUM_KEYS item:N elements
op_key_range() {
    case "$OP_TYPE" in
        *)      echo "item: 1 $NUM_KEYS" ;;
    esac
}

# ----------------------------------------------------------------------------
prefill() {
    local server_type=$1
    # VADD 本身就是写入, 不需要 prefill (但仍 FLUSHDB 保证干净起点)
    if [ "$OP_TYPE" = "VADD" ]; then
        log "[$server_type] OP_TYPE=VADD 跳过 prefill (VADD 本身即写入)"
        $REDIS_DIR/src/redis-cli -h 127.0.0.1 -p $PORT FLUSHDB >/dev/null 2>&1
        return 0
    fi

    local prefix kmin kmax
    read prefix kmin kmax < <(op_key_range)
    log "[$server_type] OP_TYPE=$OP_TYPE prefill key=$prefix[$kmin..$kmax] (DIM=$DIM)..."
    $REDIS_DIR/src/redis-cli -h 127.0.0.1 -p $PORT FLUSHDB >/dev/null 2>&1

    if [ "$server_type" = "hpc" ]; then
        # hpc server: VEMB V16 协议单 conn 写入
        #   所有 OP 统一 item: prefix, -n=NUM_KEYS
        #   VSIM/VREM: item: prefix, -n=NUM_KEYS
        local nfill=$((kmax - kmin + 1))
        # 写死 -t 8 -c 1 并发 (参考 hpc_redis_vrem_max_tput.sh:85)
        taskset -c $CLIENT_CPUSET \
            $HPC_MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
                -s 127.0.0.1 -p $PORT -t 8 -c 1 -n $nfill \
                --ratio=1:0 --key-pattern=S:S --key-prefix=$prefix \
                --key-minimum=$kmin --key-maximum=$kmax \
                > "$RAWDIR/${server_type}_${OP_TYPE}_prefill.log" 2>&1
    else
        # baseline server: RESP3 --pipe 批量灌 VADD
        awk -v n=$NUM_KEYS -v dim=$DIM 'BEGIN{
            srand(42);
            for (i=1; i<=n; i++) {
                printf "VADD myset VALUES %d", dim;
                for (j=0; j<dim; j++) printf " %f", rand()*j*0.001;
                printf " item:%d\n", i;
            }
        }' | $REDIS_DIR/src/redis-cli -h 127.0.0.1 -p $PORT --pipe >/dev/null 2>&1
    fi

    if [ "$server_type" = "hpc" ]; then
        local prefill_log="$RAWDIR/${server_type}_${OP_TYPE}_prefill.log"
        local n_sets=$(grep "^Totals" "$prefill_log" 2>/dev/null | awk '{print $2}')
        local n_sets_int=$(printf "%.0f" "${n_sets:-0}")
        log "[$server_type] prefill 完成: ${n_sets_int} sets/sec (target=${NUM_KEYS} keys)"
    else
        local vc=$( $REDIS_DIR/src/redis-cli -h 127.0.0.1 -p $PORT VCARD myset 2>/dev/null)
        log "[$server_type] prefill 完成: myset VCARD=$vc"
    fi
}

# ----------------------------------------------------------------------------
# 单 server 跑一档 (调用方保证已 prefill)
run_one_config() {
    local server_type=$1 idx=$2
    local t=${TS[$idx]} c=${CS[$idx]} p=${PS[$idx]}
    local prefix kmin kmax
    read prefix kmin kmax < <(op_key_range)
    local raw="$RAWDIR/${server_type}_${OP_TYPE}_t${t}_c${c}_p${p}.log"
    log "  [$server_type] OP=$OP_TYPE t=$t c=$c pipeline=$p time=${TEST_TIME}s"

    local jb=$(snapshot_jiffies $PORT)
    if [ "$server_type" = "hpc" ]; then
        # hpc server: VEMB V16 二进制协议
        #   所有 OP 统一 item: key 范围
        #   VSIM:  ratio=0:1 R:R --vemb-v16-vsim  (相似度查询)
        #   VADD:  ratio=1:0 S:S    (写入)
        #   VREM:  ratio=1:0 S:S --vemb-v16-vrem  (删除)
        local op_flag="" ratio="--ratio=0:1" kp="R:R"
        case "$OP_TYPE" in
            VEMB) ;;
            VSIM) op_flag="--vemb-v16-vsim" ;;
            VSIM_2KEY) op_flag="--vemb-v16-vsim-key-key" ;;
            VADD) ratio="--ratio=1:0"; kp="S:S" ;;
            VREM) op_flag="--vemb-v16-vrem"; ratio="--ratio=1:0"; kp="S:S" ;;
        esac
        # VSIM/VADD/VREM 高并发可能 hang, timeout 兜底 (TEST_TIME + 30s); VEMB 也统一走 timeout
        local hpc_timeout=$((TEST_TIME + 30))
        timeout ${hpc_timeout}s numactl --membind=$NUMA_NODE_CLIENT taskset -c $CLIENT_CPUSET \
            $HPC_MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM $op_flag \
                -s 127.0.0.1 -p $PORT -t $t -c $c --pipeline=$p \
                $ratio --key-pattern=$kp \
                --key-prefix=$prefix --key-minimum=$kmin --key-maximum=$kmax \
                --test-time=$TEST_TIME --hide-histogram \
                > "$raw" 2>&1 || true
    else
        # baseline server: RESP3 module 命令 (memtier --protocol=resp3 --command=...)
        #   VEMB: VEMB myset ELE __key__ raw  R       (单 myset 取向量)
        #   VSIM: VSIM myset ELE __key__      R       (单 myset KNN 查询)
        #   VADD: VADD myset VALUES $DIM $FIXED_VECTOR __key__  S  (单 myset 插入)
        #   VREM: VREM myset __key__          S       (单 myset 删除)
        local cmd kp
        case "$OP_TYPE" in
            VEMB) cmd="VEMB myset __key__ raw";                              kp="R" ;;
            VSIM) cmd="VSIM myset VALUES $DIM $FIXED_VECTOR COUNT 1";        kp="R" ;;
            VSIM_2KEY) cmd="VEMB myset __key__ raw";                         kp="R" ;;  # 1 VEMB/op, ×2 for 2-key compare
            VADD) cmd="VADD myset VALUES $DIM $FIXED_VECTOR __key__";        kp="S" ;;
            VREM) cmd="VREM myset __key__";                                  kp="S" ;;
        esac
        numactl --membind=$NUMA_NODE_CLIENT taskset -c $CLIENT_CPUSET \
            $BASELINE_MEMTIER --protocol=resp3 \
                -s 127.0.0.1 -p $PORT -t $t -c $c --pipeline=$p \
                --command="$cmd" --command-key-pattern=$kp \
                --key-prefix=$prefix --key-minimum=$kmin --key-maximum=$kmax \
                --data-size=128 \
                --test-time=$TEST_TIME --hide-histogram --select-db=0 \
                > "$raw" 2>&1 || true
    fi
    local ja=$(snapshot_jiffies $PORT)
    local cores=$(awk -v d=$((ja - jb)) -v s=$TEST_TIME 'BEGIN{printf "%.2f", d/100.0/s}')

    # memtier Totals 行字段位置 (按 NF 自动判: NF>=9 带 Hits/Misses / NF>=7 普通)
    local totals ops avg p50 p99 kb
    totals=$(grep "^Totals" "$raw" | tail -1)
    read ops avg p50 p99 kb < <(
        echo "$totals" | awk '{
            if (NF>=9)      printf "%s %s %s %s %s", $2,$5,$6,$7,$9
            else if (NF>=7) printf "%s %s %s %s %s", $2,$3,$4,$5,$7
            else            printf "0 NA NA NA NA"
        }'
    )
    # VSIM_2KEY baseline: 1 memtier op = 1× VEMB raw, need 2× for 2-key compare
    # => TSV writes ÷2 so it's directly comparable to hpc VSIM_KEY_KEY (per-2-key ops/sec)
    local ops_note=""
    if [ "$OP_TYPE" = "VSIM_2KEY" ] && [ "$server_type" = "baseline" ]; then
        ops_note=" (÷2, 2-key equiv)"
        ops=$(awk "BEGIN {printf \"%.2f\", $ops/2}")
    fi
    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
        "$OP_TYPE" "$server_type" "$t" "$c" "$p" "$ops" "$avg" "$p50" "$p99" "$kb" "$cores" >> "$TSV"
    log "    => ops/s=$ops$ops_note  avg=${avg}ms  p50=${p50}ms  p99=${p99}ms  cores=$cores"
    rm -f "$raw"
}

# ============================================================================
trap 'stop_server' EXIT INT TERM

log "=== ${OP_TYPE} 本地回环 sweep (${NCONFIGS} 档 × ${SERVERS_ONLY} server) ==="
log "TEST_TIME=${TEST_TIME}s/档, DIM=$DIM, TCP 127.0.0.1:$PORT"
log "DIM=$DIM NUM_KEYS=$NUM_KEYS"

printf "op\tserver_type\tt\tc\tpipeline\tops_sec\tavg_lat_ms\tp50_ms\tp99_ms\tkb_sec\tcores\n" > "$TSV"

for server_type in $SERVERS_ONLY; do
    if [ "$OP_TYPE" = "VEMB" ]; then
        # VEMB: 不重启 server, start → prefill → run all configs → stop
        case $server_type in
            baseline) start_baseline ;;
            hpc) start_hpc ;;
            *) echo "ERROR: unknown server_type=$server_type"; exit 2 ;;
        esac
        prefill $server_type
        # 验证 server 在 prefill 后仍然存活
        if ! $REDIS_DIR/src/redis-cli -h 127.0.0.1 -p $PORT PING 2>/dev/null | grep -q PONG; then
            log "FATAL: [$server_type] server unreachable after prefill"
            exit 1
        fi
        for ((i=0; i<NCONFIGS; i++)); do
            run_one_config $server_type $i
        done
        stop_server
    else
        # VSIM/VADD/VREM: 每档重启 server (修改数据, 需要干净起点)
        for ((i=0; i<NCONFIGS; i++)); do
            case $server_type in
                baseline) start_baseline ;;
                hpc) start_hpc ;;
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
