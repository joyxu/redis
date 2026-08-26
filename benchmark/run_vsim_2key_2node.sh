#!/usr/bin/env bash
# baseline 存储默认 NOQUANT(fp32, VEMB RAW 返回 f32 与 hpc 响应对齐); BASELINE_NOQUANT=0 恢复 int8 量化
# ============================================================================
# run_vsim_2key_2node.sh
# 2-node VSIM_2KEY: Redis Cluster (baseline) vs hpc-redis 吞吐/延迟 对比
#
# 拓扑:
#   - Node0: 192.168.1.111 (HW01) — server + memtier client (脚本在本机执行)
#   - Node1: 192.168.1.112 (HW02) — server (SSH 远程)
# 网络: 192.168.1.x (100G mlx5 直连)
#
# Baseline:  3-master Redis Cluster (node0 多跑 1 个 0-slot 实例满足 quorum),
#            key prefix "{item:" 作为隐式 hash-tag — Redis 对每个 element 独立
#            CRC16("{item:X") → 不同 slot → ~50/50 分布到双节点
# hpc-redis: 2-supernode, SDK consistent-hash 按 element key 路由 ~50/50,
#            examples/cluster_vsim_111.yaml / cluster_vsim_112.yaml
#
# VSIM_2KEY:
#   baseline — memtier --cluster-mode, 每 op = 1× VEMB raw, ops÷2 得到 2-key 等效值
#   hpc      — memtier --vemb-v16-vsim-key-key, 每 op = 2-key fetch + server cosine
#
# 用法:
#   bash benchmark/run_vsim_2key_2node.sh                           # 完整 17 档
#   TEST_TIME=5 NUM_KEYS=1000 TS="64" CS="4" PS="32" \
#     bash benchmark/run_vsim_2key_2node.sh                         # smoke
#   SERVERS_ONLY=baseline bash benchmark/run_vsim_2key_2node.sh     # 只跑 baseline
# ============================================================================
set -uo pipefail

# === 节点 ===
NODE0_HOST=${NODE0_HOST:-192.168.1.111}
NODE1_HOST=${NODE1_HOST:-192.168.1.112}
NODE1_SSH=${NODE1_SSH:-HW02}

# === 路径 (所有节点一致) ===
REDIS_DIR=${REDIS_DIR:-/root/gqs/codespace/redis-8.6.3}
HPC_DIR=${HPC_DIR:-/root/gqs/codespace/UnifiedBus/hpc-redis}
BASELINE_MEMTIER=${BASELINE_MEMTIER:-/root/gqs/codespace/UnifiedBus/memtier_benchmark_origin/memtier_benchmark}
# baseline 免 prefill: 每 node 一份 RDB (slot 布局固定 0-8191/8192-16383)
# 首轮 prefill 后自动捕获 (SAVE 落盘), 之后直接加载; 0=强制 prefill 不捕获
BASELINE_RDB=${BASELINE_RDB:-auto}
RDB_DIR=${RDB_DIR:-$HPC_DIR/benchmark/baseline_rdb}
HPC_MEMTIER=${HPC_MEMTIER:-$HPC_DIR/memtier_benchmark/memtier_benchmark}

# === 端口 ===
BASELINE_PORT=${BASELINE_PORT:-7000}      # 主实例 (有 slot)
BASELINE_QUORUM_PORT=${BASELINE_QUORUM_PORT:-7001}  # quorum-only (0 slot)
HPC_PORT=${HPC_PORT:-6390}

# === 测试参数 ===
OP_TYPE=VSIM_2KEY
TEST_TIME=${TEST_TIME:-30}
DIM=${DIM:-300}
NUM_KEYS=${NUM_KEYS:-100000}

# === server 固定口径 ===
BASELINE_IO_THREADS=${BASELINE_IO_THREADS:-4}
HPC_PIO=${HPC_PIO:-2}
HPC_SNW=${HPC_SNW:-2}

# === hpc manifest ===
HPC_MANIFEST_0=${HPC_MANIFEST_0:-$HPC_DIR/examples/cluster_vsim_111.yaml}
HPC_MANIFEST_1=${HPC_MANIFEST_1:-$HPC_DIR/examples/cluster_vsim_112.yaml}

# === 17 档配置矩阵 (跟 run_vemb_local_loopback_sweep.sh 一致) ===
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
OUTDIR=${OUTDIR:-benchmark/results/${OP_TYPE,,}_2node/${TIMESTAMP}}
TSV="$OUTDIR/summary.tsv"

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"

ulimit -n 200000
mkdir -p "$OUTDIR"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# ----------------------------------------------------------------------------
# jiffies: node0=local, node1=ssh
snapshot_jiffies_0() {
    local ut=0 st=0
    for pid in $(pgrep -x redis-server 2>/dev/null); do
        read u s < <(awk '{u+=$14; s+=$15} END{printf "%d %d", u+0, s+0}' /proc/$pid/task/*/stat 2>/dev/null)
        ut=$((ut + ${u:-0})); st=$((st + ${s:-0}))
    done
    echo "$ut $st"
}
snapshot_jiffies_1() {
    ssh $SSH_OPTS "$NODE1_SSH" \
        "ut=0; st=0; for pid in \$(pgrep -x redis-server); do read u s < <(awk '{u+=\$14; s+=\$15} END{printf \"%d %d\", u+0, s+0}' /proc/\$pid/task/*/stat 2>/dev/null); ut=\$((ut + \${u:-0})); st=\$((st + \${s:-0})); done; echo \"\$ut \$st\"" 2>/dev/null
}
snapshot_si_0() { awk '/^cpu /{print $8}' /proc/stat 2>/dev/null; }
snapshot_si_1() { ssh $SSH_OPTS "$NODE1_SSH" "awk '/^cpu /{print \$8}' /proc/stat 2>/dev/null" 2>/dev/null; }
snapshot_rss_0() {
    local rss=0
    for pid in $(pgrep -x redis-server 2>/dev/null); do
        local r=$(awk '/^VmRSS:/{print $2+0}' /proc/$pid/status 2>/dev/null)
        rss=$((rss + ${r:-0}))
    done
    echo $rss
}
snapshot_rss_1() {
    ssh $SSH_OPTS "$NODE1_SSH" \
        "rss=0; for pid in \$(pgrep -x redis-server); do r=\$(awk '/^VmRSS:/{print \$2+0}' /proc/\$pid/status 2>/dev/null); rss=\$((rss + \${r:-0})); done; echo \$rss" 2>/dev/null
}

# ----------------------------------------------------------------------------
wait_port_0() {
    local count=0
    while ! $REDIS_DIR/src/redis-cli -h 127.0.0.1 -p "$1" PING 2>/dev/null | grep -q PONG; do
        sleep 0.2; count=$((count + 1))
        [ $count -gt 100 ] && { log "TIMEOUT waiting 127.0.0.1:$1"; return 1; }
    done
    return 0
}
wait_port_1() {
    local count=0
    while ! ssh $SSH_OPTS "$NODE1_SSH" \
        "$REDIS_DIR/src/redis-cli -h 127.0.0.1 -p $1 PING 2>/dev/null | grep -q PONG" 2>/dev/null; do
        sleep 0.2; count=$((count + 1))
        [ $count -gt 100 ] && { log "TIMEOUT waiting $NODE1_HOST:$1"; return 1; }
    done
    return 0
}

# ============================================================================
#                              Baseline Redis Cluster
#   node0: 2 instances (7000=main, 7001=quorum-only 0-slot)
#   node1: 1 instance  (7000=main)
#   3 masters total → quorum satisfied
# ============================================================================
# 兼容 RDB 路径 (node 侧); 不存在返回空
vsim_rdb_path() {
    [ "$BASELINE_RDB" = "0" ] && return 1
    local tag=$([ "${BASELINE_NOQUANT:-1}" = "0" ] && echo INT8 || echo NOQUANT)
    local f="$RDB_DIR/${NUM_KEYS}K_${DIM}D_${tag}_vsim2key_n$1.rdb"
    [ -f "$f" ] || return 1
    echo "$f"
}

# prefill 后捕获: 每 data-node SAVE → 统一存 RDB_DIR + DBSIZE meta
capture_vsim_rdb() {
    [ "$BASELINE_RDB" = "0" ] && return 0
    local tag=$([ "${BASELINE_NOQUANT:-1}" = "0" ] && echo INT8 || echo NOQUANT)
    local base="${NUM_KEYS}K_${DIM}D_${tag}_vsim2key"
    # node0 主实例
    local d0
    d0=$($REDIS_DIR/src/redis-cli -h "$NODE0_HOST" -p "$BASELINE_PORT" DBSIZE 2>/dev/null)
    $REDIS_DIR/src/redis-cli -h "$NODE0_HOST" -p "$BASELINE_PORT" SAVE >/dev/null 2>&1
    mv /tmp/redis-cluster-baseline/dump.rdb "$RDB_DIR/${base}_n0.rdb" 2>/dev/null && chmod 444 "$RDB_DIR/${base}_n0.rdb" && echo "$d0" > "$RDB_DIR/${base}_n0.size"
    # node1
    local d1
    d1=$(ssh $SSH_OPTS "$NODE1_SSH" "$REDIS_DIR/src/redis-cli -h 127.0.0.1 -p $BASELINE_PORT DBSIZE 2>/dev/null; $REDIS_DIR/src/redis-cli -h 127.0.0.1 -p $BASELINE_PORT SAVE >/dev/null 2>&1; echo done" 2>/dev/null | head -1)
    ssh $SSH_OPTS "$NODE1_SSH" "cat /tmp/redis-cluster-baseline/dump.rdb" > "$RDB_DIR/${base}_n1.rdb" 2>/dev/null && chmod 444 "$RDB_DIR/${base}_n1.rdb" && echo "$d1" > "$RDB_DIR/${base}_n1.size"
    log "  [rdb] 捕获完成: ${base}_n0.rdb($d0 keys) ${base}_n1.rdb($d1 keys)"
}

# RDB 就绪校验 (cluster ok 后): DBSIZE 与捕获时一致
check_vsim_rdb_loaded() {
    local tag=$([ "${BASELINE_NOQUANT:-1}" = "0" ] && echo INT8 || echo NOQUANT)
    local base="${NUM_KEYS}K_${DIM}D_${tag}_vsim2key"
    local want0=$(cat "$RDB_DIR/${base}_n0.size" 2>/dev/null)
    local want1=$(cat "$RDB_DIR/${base}_n1.size" 2>/dev/null)
    [ -z "$want0" ] && return 0
    local d0 d1
    d0=$($REDIS_DIR/src/redis-cli -h "$NODE0_HOST" -p "$BASELINE_PORT" DBSIZE 2>/dev/null)
    d1=$(ssh $SSH_OPTS "$NODE1_SSH" "$REDIS_DIR/src/redis-cli -h 127.0.0.1 -p $BASELINE_PORT DBSIZE 2>/dev/null" 2>/dev/null)
    if [ "$d0" = "$want0" ] && [ "$d1" = "$want1" ]; then
        log "  [rdb] 校验 OK: n0=$d0 n1=$d1"; return 0
    fi
    log "  [rdb] 校验不符: n0=$d0/$want0 n1=$d1/$want1"; return 1
}

# 给 (host:BASELINE_PORT) 补齐 [from,to] 的 slot。RDB 预加载的 key 会触发 redis
# auto-claim (busy), busy 的正是 key 所在 slot 且已在正确节点, 只需补缺失的空 slot。
fill_cluster_slots() {
    local host=$1 from=$2 to=$3 try mine_id missing rc
    for try in 1 2 3 4 5; do
        # 注意: redis-cli 收到 ERR 回复时 exit code 仍是 0, 必须判输出内容
        local out
        out=$($REDIS_DIR/src/redis-cli -h "$host" -p "$BASELINE_PORT" CLUSTER ADDSLOTS $(seq $from $to) 2>&1)
        [ "$out" = "OK" ] && return 0
        log "    fill[$host] try=$try addslots: $(echo "$out" | head -c 60)"
        mine_id=$($REDIS_DIR/src/redis-cli -h "$host" -p "$BASELINE_PORT" CLUSTER MYID 2>/dev/null)
        [ -z "$mine_id" ] && { sleep 1; continue; }
        missing=$($REDIS_DIR/src/redis-cli -h "$host" -p "$BASELINE_PORT" CLUSTER NODES 2>/dev/null | awk -v me="$mine_id" -v f="$from" -v t="$to" '
            $1==me { for(i=9;i<=NF;i++){ if($i ~ /-/) { split($i,a,"-"); for(s=a[1];s<=a[2];s++) have[s]=1 } else have[$i+0]=1 } }
            END { for(s=f;s<=t;s++) if(!(s in have)) print s }')
        log "    fill[$host] missing_count=$(echo $missing | wc -w)"
        log "    fill_cluster_slots $host try=$try missing=$(echo $missing | wc -w) slots"
        [ -z "$missing" ] && return 0
        local out2
        out2=$($REDIS_DIR/src/redis-cli -h "$host" -p "$BASELINE_PORT" CLUSTER ADDSLOTS $missing 2>&1)
        [ "$out2" = "OK" ] && return 0
        log "    fill[$host] try=$try addslots-missing: $(echo "$out2" | head -c 60)"
        sleep 1
    done
    log "WARN: fill_cluster_slots $host [$from,$to] 未完全成功"
    return 1
}

start_baseline_0() {
    log "  [node0] start baseline redis-server main port=$BASELINE_PORT"
    mkdir -p /tmp/redis-cluster-baseline
    rm -f /tmp/redis-cluster-baseline/nodes-*.conf /tmp/redis-cluster-baseline/dump.rdb  # 防跨 run 残留
    local rdb0
    rdb0=$(vsim_rdb_path 0 || true)
    local dir_args=(--dir /tmp/redis-cluster-baseline)
    if [ -n "$rdb0" ]; then
        dir_args=(--dir "$(dirname "$rdb0")" --dbfilename "$(basename "$rdb0")")
        log "  [node0] 加载预填充 RDB: $rdb0"
    fi
    taskset -c 0-47 \
        $REDIS_DIR/src/redis-server \
            --port $BASELINE_PORT --bind 0.0.0.0 --protected-mode no \
            --cluster-enabled yes \
            --cluster-config-file /tmp/redis-cluster-baseline/nodes-$BASELINE_PORT.conf \
            --cluster-node-timeout 10000 \
            --io-threads $BASELINE_IO_THREADS --io-threads-do-reads yes \
            --appendonly no --save '' \
            "${dir_args[@]}" \
            --logfile /tmp/redis-cluster-baseline/redis-$BASELINE_PORT.log \
            --daemonize yes

    log "  [node0] start baseline redis-server quorum-only port=$BASELINE_QUORUM_PORT"
    taskset -c 48 \
        $REDIS_DIR/src/redis-server \
            --port $BASELINE_QUORUM_PORT --bind 0.0.0.0 --protected-mode no \
            --cluster-enabled yes \
            --cluster-config-file /tmp/redis-cluster-baseline/nodes-$BASELINE_QUORUM_PORT.conf \
            --cluster-node-timeout 10000 \
            --io-threads 1 --io-threads-do-reads yes \
            --appendonly no --save '' \
            --dir /tmp/redis-cluster-baseline \
            --logfile /tmp/redis-cluster-baseline/redis-$BASELINE_QUORUM_PORT.log \
            --daemonize yes
}

start_baseline_1() {
    log "  [node1] start baseline redis-server port=$BASELINE_PORT"
    local rdb1
    rdb1=$(vsim_rdb_path 1 || true)
    local remote_dir="/tmp/redis-cluster-baseline"
    local remote_dbargs=""
    if [ -n "$rdb1" ]; then
        # 把 rdb 复制到 node1 固定路径, quorum 不在 node1 不受影响
        scp -q "$rdb1" "$NODE1_SSH:/tmp/vsim2key_n1.rdb" 2>/dev/null
        remote_dir="/tmp"
        remote_dbargs="--dbfilename vsim2key_n1.rdb"
        log "  [node1] 加载预填充 RDB: $rdb1"
    fi
    ssh $SSH_OPTS "$NODE1_SSH" "
        mkdir -p /tmp/redis-cluster-baseline && rm -f /tmp/redis-cluster-baseline/nodes-*.conf /tmp/redis-cluster-baseline/dump.rdb && \
        taskset -c 0-95 \
        $REDIS_DIR/src/redis-server \
            --port $BASELINE_PORT --bind 0.0.0.0 --protected-mode no \
            --cluster-enabled yes \
            --cluster-config-file /tmp/redis-cluster-baseline/nodes-$BASELINE_PORT.conf \
            --cluster-node-timeout 10000 \
            --io-threads $BASELINE_IO_THREADS --io-threads-do-reads yes \
            --appendonly no --save '' \
            --dir $remote_dir $remote_dbargs \
            --logfile /tmp/redis-cluster-baseline/redis-$BASELINE_PORT.log \
            --daemonize yes
    " 2>/dev/null
}

stop_baseline() {
    log "baseline: stopping servers..."
    pkill -9 -x redis-server 2>/dev/null; sleep 0.5; true
    ssh $SSH_OPTS "$NODE1_SSH" "pkill -9 -x redis-server 2>/dev/null; sleep 0.5; true" 2>/dev/null
}

run_baseline() {
    log "============================================"
    log "  Baseline: Redis Cluster 2-node VSIM_2KEY"
    log "============================================"

    # --- 1. Cleanup ---
    stop_baseline
    rm -f /tmp/redis-cluster-baseline/nodes-*.conf
    ssh $SSH_OPTS "$NODE1_SSH" "rm -f /tmp/redis-cluster-baseline/nodes-*.conf" 2>/dev/null

    # --- 2. Start (3 instances total: 2 on node0 + 1 on node1) ---
    log "starting baseline servers (3 instances for quorum)..."
    start_baseline_0 || { log "FAIL: start node0 instances"; exit 1; }
    start_baseline_1 || { log "FAIL: start node1"; exit 1; }
    sleep 2

    # --- 3. Wait ports ---
    wait_port_0 "$BASELINE_PORT" || { log "FAIL: node0:$BASELINE_PORT not up"; exit 1; }
    wait_port_0 "$BASELINE_QUORUM_PORT" || { log "FAIL: node0:$BASELINE_QUORUM_PORT not up"; exit 1; }
    wait_port_1 "$BASELINE_PORT" || { log "FAIL: node1:$BASELINE_PORT not up"; exit 1; }
    log "all 3 baseline instances up"

    # --- 4. Create cluster: MEET all 3, then fill slots ---
    # RDB 预加载的 key 会触发 redis auto-claim 抢占 key 所在 slot, 整段 ADDSLOTS
    # 会因 "already busy" 原子失败。fill_cluster_slots 先整段尝试, 失败则解析
    # CLUSTER NODES 求差集只补缺失的空 slot (busy 的已在正确节点, 无需处理)。
    log "creating 3-master cluster (2 with slots + 1 quorum-only)..."
    $REDIS_DIR/src/redis-cli -h "$NODE0_HOST" -p "$BASELINE_PORT" \
        CLUSTER MEET "$NODE1_HOST" "$BASELINE_PORT" 2>/dev/null
    $REDIS_DIR/src/redis-cli -h "$NODE0_HOST" -p "$BASELINE_PORT" \
        CLUSTER MEET "$NODE0_HOST" "$BASELINE_QUORUM_PORT" 2>/dev/null
    sleep 3
    fill_cluster_slots "$NODE0_HOST" 0 8191
    fill_cluster_slots "$NODE1_HOST" 8192 16383
    sleep 2

    # --- 5. Wait cluster_state=ok (not just slots_ok) ---
    local sa0 ds0
    sa0=$($REDIS_DIR/src/redis-cli -h "$NODE0_HOST" -p "$BASELINE_PORT" CLUSTER INFO 2>/dev/null | awk -F: '/cluster_slots_assigned/{gsub(/[[:space:]]/,"",$2);print $2}')
    ds0=$($REDIS_DIR/src/redis-cli -h "$NODE0_HOST" -p "$BASELINE_PORT" DBSIZE 2>/dev/null)
    log "waiting for cluster_state=ok... [slots_assigned=${sa0:-NA} dbsize0=${ds0:-NA}]"
    local cstate="fail" max_wait=60
    for ((w=0; w<max_wait; w++)); do
        cstate=$($REDIS_DIR/src/redis-cli -h "$NODE0_HOST" -p "$BASELINE_PORT" CLUSTER INFO 2>/dev/null \
            | awk -F: '/cluster_state/{gsub(/[[:space:]]/,"",$2);print $2}')
        [ "$cstate" = "ok" ] && break
        sleep 1
    done
    if [ "$cstate" != "ok" ]; then
        log "FAIL: cluster_state=$cstate (expect ok after ${max_wait}s)"
        exit 1
    fi
    log "cluster_state=ok after ${w}s"

    # --- 6. Prefill ({item:X as implicit hash-tag key, single elem0 per key) ---
    # RDB 已加载 → 跳过 prefill 数据灌入 (FLUSHALL 也跳过), 但不能 return:
    # 本函数后续还有 DBSIZE 校验与 bench, return 会整个跳过 baseline 压测
    local rdb_prefilled=0
    if [ -n "$(vsim_rdb_path 0 || true)" ] && [ -n "$(vsim_rdb_path 1 || true)" ]; then
        if check_vsim_rdb_loaded; then
            log "prefill skipped: using pre-loaded RDBs"
            rdb_prefilled=1
        else
            log "WARN: RDB 校验失败, 回退 prefill"
        fi
    fi
    if [ "$rdb_prefilled" = "0" ]; then
    log "prefilling $NUM_KEYS elements with hash-tag distribution..."
    $REDIS_DIR/src/redis-cli -c -h "$NODE0_HOST" -p "$BASELINE_PORT" FLUSHALL >/dev/null 2>&1

    local prefill_start=$(date +%s)
    awk -v n=$NUM_KEYS -v dim=$DIM 'BEGIN{
        srand(42);
        for (i=1; i<=n; i++) {
            printf "VADD {item:%d VALUES %d", i, dim;
            for (j=0; j<dim; j++) printf " %.5f", rand()*j*0.001;
            printf " elem0%s\n", (ENVIRON["BASELINE_NOQUANT"]=="0" ? "" : " NOQUANT");
        }
    }' | $REDIS_DIR/src/redis-cli -c -h "$NODE0_HOST" -p "$BASELINE_PORT" \
        > /dev/null 2>&1
    local prefill_end=$(date +%s)
    log "  prefill took $((prefill_end - prefill_start))s"

    # --- 7. Verify DBSIZE ---
    local dbsize0 dbsize1
    dbsize0=$($REDIS_DIR/src/redis-cli -h "$NODE0_HOST" -p "$BASELINE_PORT" DBSIZE 2>/dev/null)
    dbsize1=$($REDIS_DIR/src/redis-cli -h "$NODE1_HOST" -p "$BASELINE_PORT" DBSIZE 2>/dev/null)
    log "  DBSIZE: node0=$dbsize0  node1=$dbsize1"
    # prefill 成功 → 捕获 RDB 供下次使用
    if [ "${dbsize0:-0}" -gt 0 ] || [ "${dbsize1:-0}" -gt 0 ]; then
        capture_vsim_rdb
    fi
    if [ "${dbsize0:-0}" -eq 0 ] && [ "${dbsize1:-0}" -eq 0 ]; then
        log "FAIL: both nodes have 0 keys after prefill"
        exit 1
    fi
    fi  # end rdb_prefilled=0

    # --- 8. Benchmark (17 configs, no restart — VSIM_2KEY is read-only) ---
    log "benchmark: $NCONFIGS configs, TEST_TIME=${TEST_TIME}s"
    for ((idx=0; idx<NCONFIGS; idx++)); do
        local t=${TS[$idx]} c=${CS[$idx]} p=${PS[$idx]}
        local raw="/tmp/vsim2key_baseline_$$.log"
        log "  [baseline] t=$t c=$c p=$p"

        local jb_ut0 jb_st0 jb_ut1 jb_st1 ja_ut0 ja_st0 ja_ut1 ja_st1
        read jb_ut0 jb_st0 < <(snapshot_jiffies_0)
        read jb_ut1 jb_st1 < <(snapshot_jiffies_1)
        JB_NS=$(date +%s%N)
        local si0=$(( $(snapshot_si_0) + $(snapshot_si_1) ))
        local rss0=$(( $(snapshot_rss_0) + $(snapshot_rss_1) ))

        taskset -c 96-191 \
            $BASELINE_MEMTIER \
                -s "$NODE0_HOST" -p "$BASELINE_PORT" --cluster-mode \
                -t "$t" -c "$c" --pipeline="$p" \
                --command='VEMB __key__ elem0 raw' --command-key-pattern=R \
                --key-prefix='{item:' --key-minimum=1 --key-maximum="$NUM_KEYS" \
                --data-size=128 \
                --test-time="$TEST_TIME" --hide-histogram \
                > "$raw" 2>&1 || true

        read ja_ut0 ja_st0 < <(snapshot_jiffies_0)
        read ja_ut1 ja_st1 < <(snapshot_jiffies_1)
        JA_NS=$(date +%s%N)
        ELAPSED_NS=$((JA_NS - JB_NS > 0 ? JA_NS - JB_NS : TEST_TIME * 1000000000))
        # core 分母 = 纳秒实测窗口 (与脚本13对齐)
        local si1=$(( $(snapshot_si_0) + $(snapshot_si_1) ))
        local rss1=$(( $(snapshot_rss_0) + $(snapshot_rss_1) ))
        local cores=$(awk -v d=$(((ja_ut0 + ja_st0 + ja_ut1 + ja_st1) - (jb_ut0 + jb_st0 + jb_ut1 + jb_st1))) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
        local core_ut=$(awk -v d=$(((ja_ut0 - jb_ut0) + (ja_ut1 - jb_ut1))) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
        local core_st=$(awk -v d=$(((ja_st0 - jb_st0) + (ja_st1 - jb_st1))) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
        local si=$(awk -v d=$((si1 - si0)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
        local rss=$(((rss1 + rss0) / 2))

        # Parse Totals (cluster mode: NF>=9 with MOVED/ASK)
        local totals ops avg p50 p99 kb
        totals=$(grep "^Totals" "$raw" | tr -d '\r' | tail -1)
        read ops avg p50 p99 kb < <(
            echo "$totals" | awk '{
                if (NF>=11)      printf "%s %s %s %s %s", $2,$7,$8,$9,$11
                else if (NF>=9)  printf "%s %s %s %s %s", $2,$5,$6,$7,$9
                else if (NF>=7)  printf "%s %s %s %s %s", $2,$3,$4,$5,$7
                else            printf "0 NA NA NA NA"
            }'
        )
        rm -f "$raw"
        # VSIM_2KEY: 1 op = 1 VEMB fetch, need 2 per comparison → ÷2
        local ops_div2=$(awk "BEGIN {printf \"%.2f\", ${ops:-0}/2}")

        printf "VSIM_2KEY\tbaseline\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
            "$t" "$c" "$p" "$ops_div2" "$avg" "$p50" "$p99" "$kb" "$cores" \
            "$core_ut" "$core_st" "$si" "$rss" >> "$TSV"
        log "    => ops/s=$ops_div2 (÷2, raw=$ops)  avg=${avg}ms  p50=${p50}ms  p99=${p99}ms  cores=$cores"
    done

    # --- 9. Cleanup ---
    stop_baseline
}

# ============================================================================
#                              hpc-redis
# ============================================================================
start_hpc_0() {
    local max_vec=$((NUM_KEYS + 1000))
    log "  [node0] start hpc-redis port=$HPC_PORT manifest=$(basename $HPC_MANIFEST_0)"
    ulimit -n 200000
    taskset -c 0-95 \
        $HPC_DIR/src/redis-server \
            --port $HPC_PORT --bind 0.0.0.0 --protected-mode no \
            --vemb-v16-tcp-host $NODE0_HOST \
            --io-threads $BASELINE_IO_THREADS --io-threads-do-reads yes \
            --vemb-v16-enabled yes \
            --vemb-v16-dim $DIM \
            --vemb-v16-max-vectors $max_vec \
            --vemb-v16-proxy-io-threads $HPC_PIO \
            --vemb-v16-supernode-workers $HPC_SNW \
            --vemb-v16-warm-regions-manifest $HPC_MANIFEST_0 \
            --vemb-v16-reset-warm-regions yes \
            --appendonly no --save '' \
            --dir /tmp --logfile /tmp/hpc-redis-$HPC_PORT.log \
            --daemonize yes
}
start_hpc_1() {
    local max_vec=$((NUM_KEYS + 1000))
    log "  [node1] start hpc-redis port=$HPC_PORT manifest=$(basename $HPC_MANIFEST_1)"
    ssh $SSH_OPTS "$NODE1_SSH" "
        ulimit -n 200000 && \
        taskset -c 0-95 \
        $HPC_DIR/src/redis-server \
            --port $HPC_PORT --bind 0.0.0.0 --protected-mode no \
            --vemb-v16-tcp-host $NODE1_HOST \
            --io-threads $BASELINE_IO_THREADS --io-threads-do-reads yes \
            --vemb-v16-enabled yes \
            --vemb-v16-dim $DIM \
            --vemb-v16-max-vectors $max_vec \
            --vemb-v16-proxy-io-threads $HPC_PIO \
            --vemb-v16-supernode-workers $HPC_SNW \
            --vemb-v16-warm-regions-manifest $HPC_MANIFEST_1 \
            --vemb-v16-reset-warm-regions yes \
            --appendonly no --save '' \
            --dir /tmp --logfile /tmp/hpc-redis-$HPC_PORT.log \
            --daemonize yes
    " 2>/dev/null
}

stop_hpc() {
    log "hpc: stopping servers..."
    pkill -9 -x redis-server 2>/dev/null; sleep 0.5; true
    ssh $SSH_OPTS "$NODE1_SSH" "pkill -9 -x redis-server 2>/dev/null; sleep 0.5; true" 2>/dev/null
}

run_hpc() {
    log "============================================"
    log "  hpc-redis: 2-node VSIM_2KEY"
    log "============================================"

    local ENDPOINTS="$NODE0_HOST:$HPC_PORT,$NODE1_HOST:$HPC_PORT"

    # --- 1. Cleanup ---
    stop_hpc

    # --- 2. Start ---
    log "starting hpc-redis servers..."
    start_hpc_0 || { log "FAIL: start hpc node0"; exit 1; }
    start_hpc_1 || { log "FAIL: start hpc node1"; exit 1; }
    sleep 2

    # --- 3. Wait ports ---
    wait_port_0 "$HPC_PORT" || { log "FAIL: hpc node0 port $HPC_PORT not up"; exit 1; }
    wait_port_1 "$HPC_PORT" || { log "FAIL: hpc node1 port $HPC_PORT not up"; exit 1; }
    log "both hpc-redis servers up"

    # --- 4. Prefill (multi-endpoint, SDK consistent hashing distributes data) ---
    log "prefilling $NUM_KEYS elements via multi-endpoint..."
    local prefill_start=$(date +%s)
    local prefill_log="/tmp/vsim2key_hpc_prefill_$$.log"
    taskset -c 96-191 \
        $HPC_MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
            --vemb-v16-endpoints="$ENDPOINTS" \
            -t 8 -c 1 --pipeline=32 \
            --ratio=1:0 --key-pattern=S:S \
            --key-prefix='item:' --key-minimum=1 --key-maximum="$NUM_KEYS" \
            -n "$NUM_KEYS" \
            > "$prefill_log" 2>&1
    local prefill_end=$(date +%s)

    local prefill_sets=$(grep "^Totals" "$prefill_log" 2>/dev/null | awk '{print $2}')
    rm -f "$prefill_log"
    log "  prefill took $((prefill_end - prefill_start))s, sets/sec=$prefill_sets"

    if [ -z "${prefill_sets:-}" ] || [ "${prefill_sets:-0}" = "0" ]; then
        log "FAIL: hpc prefill returned 0 sets/sec"
        exit 1
    fi

    # --- 5. Benchmark (17 configs, no restart — VSIM_2KEY is read-only) ---
    log "benchmark: $NCONFIGS configs, TEST_TIME=${TEST_TIME}s"
    for ((idx=0; idx<NCONFIGS; idx++)); do
        local t=${TS[$idx]} c=${CS[$idx]} p=${PS[$idx]}
        local raw="/tmp/vsim2key_hpc_$$.log"
        log "  [hpc] t=$t c=$c p=$p"

        local jb_ut0 jb_st0 jb_ut1 jb_st1 ja_ut0 ja_st0 ja_ut1 ja_st1
        read jb_ut0 jb_st0 < <(snapshot_jiffies_0)
        read jb_ut1 jb_st1 < <(snapshot_jiffies_1)
        JB_NS=$(date +%s%N)
        local si0=$(( $(snapshot_si_0) + $(snapshot_si_1) ))
        local rss0=$(( $(snapshot_rss_0) + $(snapshot_rss_1) ))

        taskset -c 96-191 \
            $HPC_MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
                --vemb-v16-endpoints="$ENDPOINTS" \
                --vemb-v16-vsim-key-key \
                -t "$t" -c "$c" --pipeline="$p" \
                --ratio=0:1 --key-pattern=R:R \
                --key-prefix='item:' --key-minimum=1 --key-maximum="$NUM_KEYS" \
                --test-time="$TEST_TIME" --hide-histogram \
                > "$raw" 2>&1

        read ja_ut0 ja_st0 < <(snapshot_jiffies_0)
        read ja_ut1 ja_st1 < <(snapshot_jiffies_1)
        JA_NS=$(date +%s%N)
        ELAPSED_NS=$((JA_NS - JB_NS > 0 ? JA_NS - JB_NS : TEST_TIME * 1000000000))
        # core 分母 = 纳秒实测窗口 (与脚本13对齐)
        local si1=$(( $(snapshot_si_0) + $(snapshot_si_1) ))
        local rss1=$(( $(snapshot_rss_0) + $(snapshot_rss_1) ))
        local cores=$(awk -v d=$(((ja_ut0 + ja_st0 + ja_ut1 + ja_st1) - (jb_ut0 + jb_st0 + jb_ut1 + jb_st1))) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
        local core_ut=$(awk -v d=$(((ja_ut0 - jb_ut0) + (ja_ut1 - jb_ut1))) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
        local core_st=$(awk -v d=$(((ja_st0 - jb_st0) + (ja_st1 - jb_st1))) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
        local si=$(awk -v d=$((si1 - si0)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
        local rss=$(((rss1 + rss0) / 2))

        # Parse Totals
        local totals ops avg p50 p99 kb
        totals=$(grep "^Totals" "$raw" | tr -d '\r' | tail -1)
        read ops avg p50 p99 kb < <(
            echo "$totals" | awk '{
                if (NF>=11)      printf "%s %s %s %s %s", $2,$7,$8,$9,$11
                else if (NF>=9)  printf "%s %s %s %s %s", $2,$5,$6,$7,$9
                else if (NF>=7)  printf "%s %s %s %s %s", $2,$3,$4,$5,$7
                else            printf "0 NA NA NA NA"
            }'
        )

        rm -f "$raw"
        printf "VSIM_2KEY\thpc\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
            "$t" "$c" "$p" "${ops:-0}" "$avg" "$p50" "$p99" "$kb" "$cores" \
            "$core_ut" "$core_st" "$si" "$rss" >> "$TSV"
        log "    => ops/s=${ops:-0}  avg=${avg}ms  p50=${p50}ms  p99=${p99}ms  cores=$cores"
    done

    # --- 6. Cleanup ---
    stop_hpc
}

# ============================================================================
# Main
# ============================================================================
trap 'stop_baseline 2>/dev/null; stop_hpc 2>/dev/null' EXIT INT TERM

log "=== VSIM_2KEY 2-node: baseline Redis Cluster vs hpc-redis ==="
log "NODE0=$NODE0_HOST (local)  NODE1=$NODE1_HOST (via $NODE1_SSH)"
log "NUM_KEYS=$NUM_KEYS  DIM=$DIM  TEST_TIME=${TEST_TIME}s"
log "configs: $NCONFIGS  (TS=${TS[*]})"
log "servers: $SERVERS_ONLY"
log "output: $OUTDIR"

printf "op\tserver_type\tt\tc\tpipeline\tops_sec\tavg_lat_ms\tp50_ms\tp99_ms\tkb_sec\tcores\tcore_ut\tcore_st\tsi\trss_kb\n" > "$TSV"

for st in $SERVERS_ONLY; do
    case $st in
        baseline) run_baseline ;;
        hpc)      run_hpc ;;
        *) echo "ERROR: unknown server_type=$st"; exit 2 ;;
    esac
done

log "=== DONE ==="
log "TSV: $TSV"
echo "----- summary -----"
column -t -s $'\t' "$TSV"
