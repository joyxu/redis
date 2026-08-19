#!/usr/bin/env bash
# ============================================================================
# run_redis_cluster_scaleout_vemb.sh
# 4 节点 redis cluster 扩容过程 VEMB 吞吐测试 (baseline redis-8.6.3)
#
# 拓扑: HW01/HW02/HW05/HW04 每节点 1 master + 1 replica (8 实例), REPLICAS=1 固定
# 网络: 192.168.1.x (100G mlx5 直连); memtier 在 MEMTIER_HOST (默认 HW06) 上跑
#
# 流程:
#   段1 baseline  : N_INIT master + N_INIT replica (默认 3+3), prefill, memtier 跑 TEST_TIME
#   段2 during    : 后台 memtier 持续打 → add-node 第 N_INIT+1 master + replica → reshard
#                   → 等迁移完成 → 停 memtier → 取 during 期间 ops/sec
#   段3 after     : N_FINAL master + N_FINAL replica (默认 4+4) 稳态, memtier 跑 TEST_TIME
#
# 用法:
#   bash benchmark/run_redis_cluster_scaleout_vemb.sh            # 完整 (默认 TEST_TIME=30)
#   TEST_TIME=3 bash benchmark/run_redis_cluster_scaleout_vemb.sh  # smoke 快验
#   MEMTIER_HOST=HW06 RAW=1 TEST_TIME=60 CLIENTS=200 THREADS=16 IO_THREADS=4 \
#     bash benchmark/run_redis_cluster_scaleout_vemb.sh          # 正式参数
#
# 可调参数:
#   N_INIT=3      起始 master 数 (HW01..HW0{N_INIT}, 顺序取前 N_INIT 个节点)
#   N_FINAL=4     扩容后 master 数 (加入第 N_INIT+1..N_FINAL 节点)
#   REPLICAS=1    每 master 配 replica 数 (固定 1, 改动需同步 start_node_pair)
#   DURING_TIME   段2 memtier 持续秒 (默认 TEST_TIME + 60s, 需大于 reshard 预期)
#   RAW=1         走 VEMB raw 路径 (server 端 INT8 直传)
#
# 编译口径 (四节点一致):
#   make -C deps jemalloc && \
#   make CFLAGS="-O2 -pipe -fno-lto" LDFLAGS="-O2 -pipe -fno-lto" CC="gcc -fuse-ld=bfd"
# ============================================================================

set -uo pipefail

# === 节点 (顺序很重要: 前 N_INIT 个是初始 master, 后面是扩容目标) ===
declare -a NODES=("HW01" "HW02" "HW05" "HW04")
declare -a IPS=("192.168.1.111" "192.168.1.112" "192.168.1.20" "192.168.1.21")
NNODES=${#NODES[@]}

# === 路径 ===
REDIS_DIR=${REDIS_DIR:-/root/gqs/codespace/redis-8.6.3}
MEMTIER=${MEMTIER:-/root/gqs/codespace/UnifiedBus/hpc-redis/memtier_benchmark/memtier_benchmark}
DATA_DIR=${DATA_DIR:-/tmp/redis-cluster-data}
MEMTIER_HOST=${MEMTIER_HOST:-HW04}

# === cluster 参数 ===
PORT=${PORT:-7000}           # master 端口
REPL_PORT_OFF=${REPL_PORT_OFF:-1}  # replica 端口 = PORT + REPL_PORT_OFF (= 7001)
CLUSTER_TIMEOUT=${CLUSTER_TIMEOUT:-10000}

# === 扩容拓扑 ===
N_INIT=${N_INIT:-3}
N_FINAL=${N_FINAL:-4}
REPLICAS=${REPLICAS:-1}
if [ "$REPLICAS" -ne 1 ]; then
    echo "ERROR: 本脚本只支持 REPLICAS=1 (每节点 1 master + 1 replica)."; exit 2
fi
if [ "$N_FINAL" -gt "$NNODES" ]; then
    echo "ERROR: N_FINAL=$N_FINAL > NNODES=$NNODES."; exit 2
fi
if [ "$N_INIT" -ge "$N_FINAL" ]; then
    echo "ERROR: N_INIT=$N_INIT must be < N_FINAL=$N_FINAL."; exit 2
fi

# === 测试参数 ===
TEST_TIME=${TEST_TIME:-30}
CLIENTS=${CLIENTS:-200}
THREADS=${THREADS:-16}
PIPELINE=${PIPELINE:-32}
IO_THREADS=${IO_THREADS:-4}
# 段2 memtier 持续秒 (需 > reshard 预期; 默认 TEST_TIME + 60s margin)
DURING_TIME=${DURING_TIME:-$((TEST_TIME + 60))}

# RESP 协议 / RAW 路径
PROTO=${PROTO:-resp2}
case "$PROTO" in resp2|resp3) ;; *) echo "ERROR: PROTO must be resp2 or resp3 (got: $PROTO)"; exit 2;; esac
RAW=${RAW:-0}
RAW_SUFFIX=""
[ "$RAW" = "1" ] && RAW_SUFFIX=" raw"

# === 数据规模 ===
NUM_VSETS=${NUM_VSETS:-16}
VECTORS_PER_VSET=${VECTORS_PER_VSET:-6250}
DIM=${DIM:-300}

# === CPU 绑核 ===
CORES_PER_NODE=${CORES_PER_NODE:-96}
CORES_PER_MASTER=${CORES_PER_MASTER:-48}  # master 用前 48 核, replica 用后 48 核

# === 输出 ===
OUTDIR=${OUTDIR:-/tmp/redis_cluster_scaleout_${PROTO}_raw${RAW}_init${N_INIT}_to${N_FINAL}}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RAWDIR="$OUTDIR/raw"
TSV="$OUTDIR/summary_${TIMESTAMP}.tsv"
mkdir -p "$RAWDIR"

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"
ulimit -n 65536
REDIS_CLI="$REDIS_DIR/src/redis-cli"

log()  { echo "[$(date +%H:%M:%S)] $*"; }
ssh_node() { local i=$1; shift; ssh $SSH_OPTS "${NODES[$i]}" "$@" 2>&1 | grep -v "Authorized users"; }

# ----------------------------------------------------------------------------
cleanup_node() {
    local i=$1
    # SKIP_RM=1 时保留 $DATA_DIR 用于 debug（看启动失败时的 redis.log）
    local rm_part="rm -rf $DATA_DIR/inst* 2>/dev/null"
    [ "${SKIP_RM:-0}" = "1" ] && rm_part="true"
    ssh $SSH_OPTS "${NODES[$i]}" "pkill -9 -x redis-server 2>/dev/null; \
                  pkill -9 -x memtier_benchm 2>/dev/null; \
                  $rm_part; true" >/dev/null 2>&1
}

cleanup_all() {
    log "cleanup all nodes..."
    for ((i=0; i<NNODES; i++)); do cleanup_node $i; done
    if [ "$MEMTIER_HOST" != "HW01" ]; then
        ssh $SSH_OPTS "$MEMTIER_HOST" "pkill -9 -x memtier_benchm 2>/dev/null; true" >/dev/null 2>&1
    fi
}

# ----------------------------------------------------------------------------
# 每节点起 2 实例: master (PORT, cores 0-47) + replica (PORT+1, cores 48-95)
start_node_pair() {
    local i=$1 ip=${IPS[$i]}
    local mport=$PORT rport=$((PORT + REPL_PORT_OFF))
    local mddir="$DATA_DIR/inst0"
    local rddir="$DATA_DIR/inst1"
    local c0=$((CORES_PER_MASTER))
    log "  ${NODES[$i]}: master=$mport(cores 0-$((c0-1))) replica=$rport(cores $c0-$((CORES_PER_NODE-1)))"
    ssh_node $i "mkdir -p $mddir $rddir && cd $REDIS_DIR && \
        numactl --membind=0 taskset -c 0-$((c0-1)) ./src/redis-server \
            --port $mport --bind 0.0.0.0 --protected-mode no \
            --cluster-enabled yes --cluster-config-file nodes.conf \
            --cluster-node-timeout $CLUSTER_TIMEOUT --cluster-announce-ip $ip \
            --io-threads $IO_THREADS --io-threads-do-reads yes \
            --appendonly no --save '' --dir $mddir --logfile $mddir/redis.log --daemonize yes && \
        numactl --membind=0 taskset -c $c0-$((CORES_PER_NODE-1)) ./src/redis-server \
            --port $rport --bind 0.0.0.0 --protected-mode no \
            --cluster-enabled yes --cluster-config-file nodes.conf \
            --cluster-node-timeout $CLUSTER_TIMEOUT --cluster-announce-ip $ip \
            --io-threads $IO_THREADS --io-threads-do-reads yes \
            --appendonly no --save '' --dir $rddir --logfile $rddir/redis.log --daemonize yes" >/dev/null
}

wait_port() {
    local ip=$1 port=$2 count=0
    while ! ($REDIS_CLI -h $ip -p $port PING 2>/dev/null | grep -q PONG); do
        sleep 0.5; ((count++))
        [ $count -gt 60 ] && { log "TIMEOUT waiting $ip:$port"; return 1; }
    done
}

# ----------------------------------------------------------------------------
# 用前 N_INIT 节点的 master+replica 建 cluster (master=PORT, replica=PORT+1)
create_initial_cluster() {
    local endpoints=""
    for ((i=0; i<N_INIT; i++)); do
        endpoints="$endpoints ${IPS[$i]}:$PORT ${IPS[$i]}:$((PORT + REPL_PORT_OFF))"
    done
    log "create initial cluster: $N_INIT masters + $N_INIT replicas (--cluster-replicas 1)..."
    echo yes | $REDIS_CLI --cluster create $endpoints --cluster-replicas 1 2>&1 \
        | grep -E "Slots|Master|Replica|slots:|OK|All|coverage|agree|Can't|err" | head -40
}

check_cluster() {
    local label=$1
    log "cluster info [$label]:"
    $REDIS_CLI -h ${IPS[0]} -p $PORT CLUSTER INFO 2>/dev/null \
        | grep -E "cluster_state|cluster_slots_ok|cluster_known_nodes|cluster_size"
    log "nodes [$label] (role + slot count):"
    $REDIS_CLI -h ${IPS[0]} -p $PORT CLUSTER NODES 2>/dev/null \
        | awk '{
            role=$3; sub(/,.*/, "", role);
            slots=$NF; n=split($0,a," "); slot_str=a[n];
            printf "  %-40s %-10s %s\n", $2, role, slot_str
        }' | head -20
}

# ----------------------------------------------------------------------------
# 节点 ID (CLUSTER MYID), 直接 ssh 到那台机查
get_node_id() {
    local ip=$1 port=$2
    ssh $SSH_OPTS "${NODES[$(ip_to_idx $ip)]}" "$REDIS_CLI -h $ip -p $port CLUSTER MYID" 2>/dev/null | tail -1
}

# IPS[i] -> i
ip_to_idx() {
    local ip=$1 i
    for ((i=0; i<NNODES; i++)); do
        [ "${IPS[$i]}" = "$ip" ] && echo $i && return
    done
    echo "-1"
}

# 扩容: 加入第 new_idx 节点的 master + replica
add_node_pair() {
    local new_idx=$1
    local new_ip=${IPS[$new_idx]}
    local new_mport=$PORT new_rport=$((PORT + REPL_PORT_OFF))
    log "add-node: ${NODES[$new_idx]} master=$new_ip:$new_mport replica=$new_ip:$new_rport"

    # 1) 加新 master (任一现有节点作 entry)
    $REDIS_CLI --cluster add-node ${new_ip}:${new_mport} ${IPS[0]}:$PORT 2>&1 \
        | grep -E "Adding|>>>|OK|error|err" | head -10

    # 2) 取新 master 的 node-id
    local new_mid
    new_mid=$(get_node_id $new_ip $new_mport)
    log "  new master id: $new_mid"

    # 3) 等 gossip 把新 master 传播到所有现有节点
    #    否则 reshard 会报 ERR I don't know about node <new_mid>
    log "  waiting for gossip to propagate new master to all nodes..."
    local known=0 max_wait=30
    for ((w=0; w<max_wait; w++)); do
        known=$($REDIS_CLI -h ${IPS[0]} -p $PORT CLUSTER NODES 2>/dev/null | grep -c "$new_mid")
        # 至少有 N_INIT 个节点认识新 master (CLUSTER NODES 输出每个节点一行, 含新 master 自己)
        [ "$known" -ge $((N_INIT + 1)) ] && { log "  gossip propagated after ${w}s (known in $known node rows)"; break; }
        sleep 1
    done

    # 4) 加新 replica, 指定 master-id
    $REDIS_CLI --cluster add-node ${new_ip}:${new_rport} ${IPS[0]}:$PORT \
        --cluster-slave --cluster-master-id $new_mid 2>&1 \
        | grep -E "Adding|>>>|OK|replica|error|err" | head -10
}

# reshard: 从所有现有 master 平均迁移 slots_new 个 slot 到新 master
# slots_new = 16384 / N_FINAL
reshard_to_new() {
    local new_idx=$1
    local new_ip=${IPS[$new_idx]} new_mport=$PORT
    local slots_new=$((16384 / N_FINAL))
    local new_mid
    new_mid=$(get_node_id $new_ip $new_mport)
    log "reshard: move $slots_new slots from all masters -> $new_mid (${NODES[$new_idx]})"
    local reshard_log="$RAWDIR/02b_reshard_${new_idx}.log"
    # --cluster-from all: 所有现有 master 平均分摊
    # --cluster-yes: 不交互
    # 完整输出落文件便于诊断 (脚本 stdout 只打关键行)
    $REDIS_CLI --cluster reshard ${IPS[0]}:$PORT \
        --cluster-from all \
        --cluster-to $new_mid \
        --cluster-slots $slots_new \
        --cluster-yes > "$reshard_log" 2>&1 || true
    grep -E "Moving slot|Ready to move|error|err|fail" "$reshard_log" | tail -20

    # 完整性检查: 用 CLUSTER INFO 的 cluster_size (= 有 slot 的 master 数) + 新 master DBSIZE
    local csize new_dbsize
    csize=$($REDIS_CLI -h ${IPS[0]} -p $PORT CLUSTER INFO 2>/dev/null \
            | awk -F: '/cluster_size/{gsub(/[[:space:]]/,"",$2);print $2}')
    new_dbsize=$($REDIS_CLI -h $new_ip -p $new_mport DBSIZE 2>/dev/null)
    log "  after reshard: cluster_size=$csize (expect $N_FINAL), new master ${NODES[$new_idx]} DBSIZE=$new_dbsize"
    if [ "$csize" -lt "$N_FINAL" ] || [ "$new_dbsize" -eq 0 ]; then
        log "  WARN: reshard incomplete (cluster_size=$csize < $N_FINAL or dbsize=0), sleep 3s + retry..."
        sleep 3
        $REDIS_CLI --cluster reshard ${IPS[0]}:$PORT \
            --cluster-from all \
            --cluster-to $new_mid \
            --cluster-slots $slots_new \
            --cluster-yes >> "$reshard_log" 2>&1 || true
        csize=$($REDIS_CLI -h ${IPS[0]} -p $PORT CLUSTER INFO 2>/dev/null \
                | awk -F: '/cluster_size/{gsub(/[[:space:]]/,"",$2);print $2}')
        new_dbsize=$($REDIS_CLI -h $new_ip -p $new_mport DBSIZE 2>/dev/null)
        log "  retry done: cluster_size=$csize, DBSIZE=$new_dbsize"
    fi
}

# 等 cluster_state=ok 且 slot 全 OK
wait_cluster_stable() {
    local max_wait=${1:-120} count=0
    while [ $count -lt $max_wait ]; do
        local cstate slots_ok
        cstate=$($REDIS_CLI -h ${IPS[0]} -p $PORT CLUSTER INFO 2>/dev/null | awk -F: '/cluster_state/{gsub(/[[:space:]]/,"",$2);print $2}')
        slots_ok=$($REDIS_CLI -h ${IPS[0]} -p $PORT CLUSTER INFO 2>/dev/null | awk -F: '/cluster_slots_ok/{gsub(/[[:space:]]/,"",$2);print $2}')
        [ "$cstate" = "ok" ] && [ "$slots_ok" = "16384" ] && { log "cluster stable after ${count}s"; return 0; }
        sleep 1; ((count++))
    done
    log "WARN: cluster not stable after ${max_wait}s (state=$cstate slots_ok=$slots_ok)"
    return 1
}

# ----------------------------------------------------------------------------
prefill() {
    local v0=$KEY_OFFSET v1=$((KEY_OFFSET + NUM_VSETS - 1))
    log "prefill: vset$v0..vset$v1 ($NUM_VSETS vsets) x $VECTORS_PER_VSET vectors (dim=$DIM)..."
    $REDIS_CLI -c -h ${IPS[0]} -p $PORT FLUSHALL >/dev/null 2>&1
    awk -v off=$KEY_OFFSET -v m=$NUM_VSETS -v k=$VECTORS_PER_VSET -v dim=$DIM 'BEGIN{
        srand(42);
        for (i=0; i<m; i++) {
            v = off + i;
            for (e=0; e<k; e++) {
                printf "VADD vset%d VALUES %d", v, dim;
                for (j=0; j<dim; j++) printf " %f", rand()*0.001;
                printf " elem%d\n", e;
            }
        }
    }' | $REDIS_CLI -c -h ${IPS[0]} -p $PORT >/dev/null 2>&1
    log "prefill done. VCARD sample:"
    for v in $v0 $v1; do
        printf "  vset%d VCARD=%s\n" "$v" \
            "$($REDIS_CLI -c -h ${IPS[0]} -p $PORT VCARD vset$v 2>/dev/null)"
    done
}

# ----------------------------------------------------------------------------
snapshot_jiffies_all() {
    local ut=0 st=0
    for ((i=0; i<NNODES; i++)); do
        local u s
        read u s < <(ssh $SSH_OPTS "${NODES[$i]}" \
            "ut=0; st=0; for pid in \$(pgrep -x redis-server); do \
                 read uu ss < <(awk '{u+=\$14; s+=\$15} END{printf \"%d %d\", u+0, s+0}' /proc/\$pid/task/*/stat 2>/dev/null); \
                 ut=\$((ut + \${uu:-0})); st=\$((st + \${ss:-0})); \
             done; echo \"\$ut \$st\"" 2>/dev/null | tail -1)
        ut=$((ut + ${u:-0})); st=$((st + ${s:-0}))
    done
    echo "$ut $st"
}

snapshot_si_all() {
    local total=0
    for ((i=0; i<NNODES; i++)); do
        local v
        v=$(ssh $SSH_OPTS "${NODES[$i]}" "awk '/^cpu /{print \$8}' /proc/stat 2>/dev/null" 2>/dev/null | tail -1)
        total=$((total + ${v:-0}))
    done
    echo $total
}

snapshot_rss_all() {
    local total=0
    for ((i=0; i<NNODES; i++)); do
        local v
        v=$(ssh $SSH_OPTS "${NODES[$i]}" \
            "rss=0; for pid in \$(pgrep -x redis-server); do \
                 r=\$(awk '/^VmRSS:/{print \$2+0}' /proc/\$pid/status 2>/dev/null); \
                 rss=\$((rss + \${r:-0})); \
             done; echo \$rss" 2>/dev/null | tail -1)
        total=$((total + ${v:-0}))
    done
    echo $total
}

# ----------------------------------------------------------------------------
# 跑 memtier TEST_TIME 秒, 写入 $1=log 文件, 返回 (cores, ops, avg, p50, p99, p999, kb)
# 参数: $1=log_path  $2=duration_sec  $3=label
# 调用者读 $TSV_ROW 拿到结果 (除 cores)
run_memtier() {
    local log_path=$1 duration=$2 label=$3
    local proto_flag=""
    [ "$PROTO" = "resp3" ] && proto_flag="--protocol=$PROTO"
    local remote_cmd="$MEMTIER -s ${IPS[0]} -p $PORT --cluster-mode \
        $proto_flag \
        -t $THREADS -c $CLIENTS --pipeline=$PIPELINE \
        --command='VEMB __key__ elem0${RAW_SUFFIX}' --command-key-pattern=R \
        --key-prefix=vset --key-minimum=$KEY_OFFSET --key-maximum=$((KEY_OFFSET + NUM_VSETS - 1)) \
        --data-size=128 \
        --test-time=$duration --hide-histogram --select-db=0"
    log "  memtier [$label] t=$THREADS c=$CLIENTS p=$PIPELINE time=${duration}s -> $log_path"
    if [ "$MEMTIER_HOST" = "HW01" ]; then
        eval "$remote_cmd" > "$log_path" 2>&1 || true
    else
        ssh $SSH_OPTS "$MEMTIER_HOST" "$remote_cmd" > "$log_path" 2>&1 || true
    fi
}

# 从 memtier 输出解析 Totals (ops avg p50 p99 p999 kb), 兼容 cluster-mode 9列 / 非 cluster 8列
parse_memtier_totals() {
    local log=$1
    local totals ops avg p50 p99 p999 kb
    # memtier 在 RAW 模式输出可能含二进制字符 (INT8 字节), grep -a 强制文本模式
    # 同时 strip \r (memtier 进度行带 ^M), 避免字段解析失败
    totals=$(grep -a "^Totals" "$log" | tr -d '\r' | tail -1)
    read ops avg p50 p99 p999 kb < <(
        echo "$totals" | awk '{
            if (NF>=8) {
                # cluster 模式: Totals ops MOVED ASK avg p50 p99 p999 kb
                # 非 cluster : Totals ops avg p50 p99 p999 kb
                if (NF==9) printf "%s %s %s %s %s %s", $2, $5, $6, $7, $8, $9
                else       printf "%s %s %s %s %s %s", $2, $3, $4, $5, $6, $7
            } else {
                printf "0 NA NA NA NA NA"
            }
        }'
    )
    echo "$ops $avg $p50 $p99 $p999 $kb"
}

# ============================================================================
trap 'cleanup_all' EXIT INT TERM

log "=== config: N_INIT=$N_INIT -> N_FINAL=$N_FINAL, REPLICAS=$REPLICAS, PROTO=$PROTO RAW=$RAW ==="
log "=== memtier on $MEMTIER_HOST, TEST_TIME=$TEST_TIME, DURING_TIME=$DURING_TIME ==="

# vset key offset 选择 (跟 cluster_vemb 一致, 让 vset 在 4 节点分布均匀)
KEY_OFFSET=${KEY_OFFSET:-2}
log "KEY_OFFSET=$KEY_OFFSET (NUM_VSETS=$NUM_VSETS)"

log "=== STEP 1: cleanup residuals ==="
cleanup_all
sleep 1

log "=== STEP 2: start $NNODES x 2 redis instances (master+replica per node) ==="
for ((i=0; i<NNODES; i++)); do start_node_pair $i; done
sleep 2
for ((i=0; i<NNODES; i++)); do
    wait_port ${IPS[$i]} $PORT || {
        log "FAIL: ${IPS[$i]}:$PORT not up"
        log "  --- diagnostic on ${NODES[$i]} ---"
        ssh $SSH_OPTS "${NODES[$i]}" "ps -eo pid,cmd | grep -E 'redis-server' | grep -v grep; ss -tln | grep -E ':($PORT|$((PORT+REPL_PORT_OFF))) '; tail -5 $DATA_DIR/inst0/redis.log 2>&1; tail -5 $DATA_DIR/inst1/redis.log 2>&1" 2>&1 | grep -v "Authorized users"
        exit 1
    }
    wait_port ${IPS[$i]} $((PORT + REPL_PORT_OFF)) || { log "FAIL: ${IPS[$i]}:$((PORT+REPL_PORT_OFF)) not up"; exit 1; }
done
log "all $((NNODES * 2)) instances up."

log "=== STEP 3: create initial cluster ($N_INIT masters + $N_INIT replicas) ==="
create_initial_cluster
wait_cluster_stable 30
check_cluster "initial"

log "=== STEP 4: prefill VEMB data ==="
prefill

# TSV 表头
printf "phase\tmaster_count\tduration_s\tops_sec\tavg_lat_ms\tp50_ms\tp99_ms\tp999_ms\tkb_sec\tcores_used\tcore_ut\tcore_st\tsi\trss_kb\n" > "$TSV"

# ----------------------------------------------------------------------------
log "=== STEP 5: 段1 baseline (N_INIT=$N_INIT masters) ==="
read jb_ut jb_st < <(snapshot_jiffies_all)
JB_NS=$(date +%s%N)
si_b=$(snapshot_si_all); rss_b=$(snapshot_rss_all)
run_memtier "$RAWDIR/01_baseline.log" $TEST_TIME "baseline"
read ja_ut ja_st < <(snapshot_jiffies_all)
JA_NS=$(date +%s%N)
ELAPSED_NS=$((JA_NS - JB_NS > 0 ? JA_NS - JB_NS : TEST_TIME * 1000000000))
# core 分母 = 纳秒实测窗口 (与脚本13对齐)
si_a=$(snapshot_si_all); rss_a=$(snapshot_rss_all)
cores=$(awk -v d=$((ja_ut + ja_st - jb_ut - jb_st)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
core_ut=$(awk -v d=$((ja_ut - jb_ut)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
core_st=$(awk -v d=$((ja_st - jb_st)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
si=$(awk -v d=$((si_a - si_b)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
rss=$(((rss_a + rss_b) / 2))
read ops avg p50 p99 p999 kb < <(parse_memtier_totals "$RAWDIR/01_baseline.log")
printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "baseline" "$N_INIT" "$TEST_TIME" "$ops" "$avg" "$p50" "$p99" "$p999" "$kb" "$cores" \
    "$core_ut" "$core_st" "$si" "$rss" >> "$TSV"
log "  => baseline: ops/s=$ops  avg=${avg}ms  p50=${p50}ms  p99=${p99}ms  cores=$cores"

# ----------------------------------------------------------------------------
log "=== STEP 6: 段2 during scaleout (扩容 N_INIT -> N_FINAL, memtier 后台持续打) ==="
# 后台启动 memtier, 跑 DURING_TIME 秒 (期间完成 add-node + reshard)
read jb_ut jb_st < <(snapshot_jiffies_all)
JB_NS=$(date +%s%N)
si_b=$(snapshot_si_all); rss_b=$(snapshot_rss_all)
run_memtier "$RAWDIR/02_during.log" $DURING_TIME "during-scaleout" &
MEMTIER_PID=$!
log "  memtier started in background (pid=$MEMTIER_PID), duration=$DURING_TIME s"

# 等 memtier 进入稳态 (3s)
sleep 3

# 顺序加入第 N_INIT+1..N_FINAL 节点 (默认只加第 4 个, 但保留循环支持多步扩容)
SCALE_START=$((N_INIT))
for ((idx=SCALE_START; idx<N_FINAL; idx++)); do
    log "  -- add node pair ${NODES[$idx]} (master_idx=$idx) --"
    add_node_pair $idx
    reshard_to_new $idx
done

# 等 cluster 稳定 (slot 全 OK)
wait_cluster_stable 120

# 等 memtier 跑完 (它还会跑一会儿, 让 DURING_TIME 涵盖整个扩容)
log "  waiting for memtier to finish (pid=$MEMTIER_PID)..."
wait $MEMTIER_PID
read ja_ut ja_st < <(snapshot_jiffies_all)
JA_NS=$(date +%s%N)
ELAPSED_NS=$((JA_NS - JB_NS > 0 ? JA_NS - JB_NS : TEST_TIME * 1000000000))
# core 分母 = 纳秒实测窗口 (与脚本13对齐)
si_a=$(snapshot_si_all); rss_a=$(snapshot_rss_all)
cores=$(awk -v d=$((ja_ut + ja_st - jb_ut - jb_st)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
core_ut=$(awk -v d=$((ja_ut - jb_ut)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
core_st=$(awk -v d=$((ja_st - jb_st)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
si=$(awk -v d=$((si_a - si_b)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
rss=$(((rss_a + rss_b) / 2))
read ops avg p50 p99 p999 kb < <(parse_memtier_totals "$RAWDIR/02_during.log")
printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "during_scaleout" "${N_INIT}->${N_FINAL}" "$DURING_TIME" "$ops" "$avg" "$p50" "$p99" "$p999" "$kb" "$cores" \
    "$core_ut" "$core_st" "$si" "$rss" >> "$TSV"
log "  => during_scaleout: ops/s=$ops  avg=${avg}ms  p50=${p50}ms  p99=${p99}ms  cores=$cores (over ${DURING_TIME}s)"

check_cluster "after-scaleout"

# ----------------------------------------------------------------------------
log "=== STEP 7: 段3 after (N_FINAL=$N_FINAL masters, 稳态) ==="
read jb_ut jb_st < <(snapshot_jiffies_all)
JB_NS=$(date +%s%N)
si_b=$(snapshot_si_all); rss_b=$(snapshot_rss_all)
run_memtier "$RAWDIR/03_after.log" $TEST_TIME "after"
read ja_ut ja_st < <(snapshot_jiffies_all)
JA_NS=$(date +%s%N)
ELAPSED_NS=$((JA_NS - JB_NS > 0 ? JA_NS - JB_NS : TEST_TIME * 1000000000))
# core 分母 = 纳秒实测窗口 (与脚本13对齐)
si_a=$(snapshot_si_all); rss_a=$(snapshot_rss_all)
cores=$(awk -v d=$((ja_ut + ja_st - jb_ut - jb_st)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
core_ut=$(awk -v d=$((ja_ut - jb_ut)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
core_st=$(awk -v d=$((ja_st - jb_st)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
si=$(awk -v d=$((si_a - si_b)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
rss=$(((rss_a + rss_b) / 2))
read ops avg p50 p99 p999 kb < <(parse_memtier_totals "$RAWDIR/03_after.log")
printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "after" "$N_FINAL" "$TEST_TIME" "$ops" "$avg" "$p50" "$p99" "$p999" "$kb" "$cores" \
    "$core_ut" "$core_st" "$si" "$rss" >> "$TSV"
log "  => after: ops/s=$ops  avg=${avg}ms  p50=${p50}ms  p99=${p99}ms  cores=$cores"

log "=== DONE ==="
log "TSV  : $TSV"
log "raw  : $RAWDIR/{01_baseline,02_during,03_after}.log"
echo "----- summary [scaleout ${N_INIT}->${N_FINAL} masters, PROTO=$PROTO RAW=$RAW] -----"
awk -F'\t' 'NR==1{printf "%-18s %-14s %10s %14s %12s %10s %10s %10s %13s %11s\n","phase","masters","dur_s","ops_sec","avg_ms","p50_ms","p99_ms","p999_ms","kb_sec","cores"}
            NR>1{printf "%-18s %-14s %10s %14s %12s %10s %10s %10s %13s %11s\n",$1,$2,$3,$4,$5,$6,$7,$8,$9,$10}' "$TSV"
