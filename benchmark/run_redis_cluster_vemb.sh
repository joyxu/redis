#!/usr/bin/env bash
# baseline 存储默认 NOQUANT(fp32, VEMB RAW 返回 f32 与 hpc 响应对齐); BASELINE_NOQUANT=0 恢复 int8 量化
# ============================================================================
# run_redis_cluster_vemb.sh
# 4 节点原生 redis cluster (baseline redis-8.6.3) VEMB 吞吐/延迟 9 档 sweep
#
# 拓扑: HW01/HW02/HW05/HW04 每节点 1 个 redis 实例, cluster mode
# 网络: 192.168.1.x (100G mlx5 直连), data port 7000, cluster bus 17000
# 测试: prefill 多 vset -> memtier --cluster-mode VEMB
#
# 用法:
#   bash benchmark/run_redis_cluster_vemb.sh                      # 默认 9 档
#   TEST_TIME=10 bash benchmark/run_redis_cluster_vemb.sh         # smoke
#   TS="64" CS="8" PS="32" bash benchmark/run_redis_cluster_vemb.sh  # 单档
#   MEMTIER_HOST=HW07 bash benchmark/run_redis_cluster_vemb.sh    # 换客户端
# ============================================================================

set -uo pipefail

# === 节点 ===
declare -a NODES=("HW01" "HW02" "HW05" "HW04")
declare -a IPS=("192.168.1.111" "192.168.1.112" "192.168.1.20" "192.168.1.21")
NNODES=${#NODES[@]}

# === 路径 ===
REDIS_DIR=${REDIS_DIR:-/root/gqs/codespace/redis-8.6.3}
MEMTIER=${MEMTIER:-/root/gqs/codespace/UnifiedBus/memtier_benchmark_origin/memtier_benchmark}
DATA_DIR=${DATA_DIR:-/tmp/redis-cluster-data}
MEMTIER_HOST=${MEMTIER_HOST:-HW01}

# === cluster 参数 ===
PORT=${PORT:-7000}
CLUSTER_TIMEOUT=${CLUSTER_TIMEOUT:-10000}

# === 测试参数 ===
TEST_TIME=${TEST_TIME:-30}
IO_THREADS=${IO_THREADS:-4}
INSTANCES_PER_NODE=${INSTANCES_PER_NODE:-1}
REPLICAS=${REPLICAS:-0}

# === 数据规模 ===
NUM_VSETS=${NUM_VSETS:-16}
# baseline 免 prefill: 每 master 实例一份 RDB (slot 布局由 --cluster create 决定,
# 生成时捕获, 布局变化需 FORCE 重新生成); 0=强制 prefill
BASELINE_RDB=${BASELINE_RDB:-auto}
RDB_DIR=${RDB_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/benchmark/baseline_rdb}
VECTORS_PER_VSET=${VECTORS_PER_VSET:-6250}
DIM=${DIM:-300}

# === CPU 绑核 ===
CORES_PER_NODE=${CORES_PER_NODE:-96}

# === 9 档配置矩阵 ===
# 9 档精简矩阵 (原 9 档, 20260826 削减: 保留低并发斜率 + 高并发饱和 + 两条 c 扫描)
TS_DEFAULT=( 1  1  4 16 64 64 64 32 64)
CS_DEFAULT=( 1  1  1  1  1  4 16 32 64)
PS_DEFAULT=( 1 32 32 32 32 32 32 32 32)
TS=( ${TS:-${TS_DEFAULT[*]}} )
CS=( ${CS:-${CS_DEFAULT[*]}} )
PS=( ${PS:-${PS_DEFAULT[*]}} )
NCONFIGS=${#TS[@]}

# === 输出 ===
OUTDIR=${OUTDIR:-benchmark/results/redis_cluster_vemb}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RAWDIR="$OUTDIR/$TIMESTAMP/raw"
TSV="$OUTDIR/$TIMESTAMP/summary.tsv"

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"
ulimit -n 65536
mkdir -p "$RAWDIR"

log()  { echo "[$(date +%H:%M:%S)] $*"; }

ssh_node() { local i=$1; shift; ssh $SSH_OPTS "${NODES[$i]}" "$@" 2>&1 | grep -v "Authorized users"; }

# ----------------------------------------------------------------------------
pick_key_offset() {
    NUM_VSETS=$NUM_VSETS python3 -c "
import os
def crc16(data):
    crc = 0
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if (crc & 0x8000) else (crc << 1)
            crc &= 0xFFFF
    return crc
N = int(os.environ.get('NUM_VSETS', '16'))
best_off, best_spread = 1, 1 << 30
for off in range(1, 5000):
    cnt = [0, 0, 0, 0]
    for i in range(N):
        cnt[(crc16(('vset%d' % (off + i)).encode()) % 16384) // 4096] += 1
    spread = max(cnt) - min(cnt)
    if spread < best_spread:
        best_spread, best_off = spread, off
        if spread == 0:
            break
print(best_off)
" 2>/dev/null
}

# ----------------------------------------------------------------------------
cleanup_node() {
    local i=$1
    ssh $SSH_OPTS "${NODES[$i]}" "pkill -9 -x redis-server 2>/dev/null; \
                  pkill -9 -x memtier_benchm 2>/dev/null; \
                  rm -rf $DATA_DIR/inst* 2>/dev/null; true" \
        >/dev/null 2>&1
}

cleanup_all() {
    log "cleanup all nodes..."
    for ((i=0; i<NNODES; i++)); do cleanup_node $i; done
    if [ "$MEMTIER_HOST" != "HW01" ] && [ "$MEMTIER_HOST" != "HW02" ]; then
        ssh $SSH_OPTS "$MEMTIER_HOST" "pkill -9 -x memtier_benchm 2>/dev/null; true" >/dev/null 2>&1
    fi
}

# ----------------------------------------------------------------------------
start_node() {
    local i=$1 ip=${IPS[$i]}
    # RDB 预放置: 若本节点承载 master (按 .layout 顺序), 把对应 rdb 拷到各 inst 目录
    local base; base=$(cluster_rdb_base 2>/dev/null)
    if [ "$BASELINE_RDB" != "0" ] && [ -f "${base}.layout" ]; then
        local idx=0 m mip
        while IFS= read -r m; do
            mip=${m%%:*}
            if [ "$mip" = "$ip" ] && [ -f "${base}_m${idx}.rdb" ]; then
                ssh -n $SSH_OPTS "${NODES[$i]}" "mkdir -p $DATA_DIR/inst0" >/dev/null 2>&1
                ssh $SSH_OPTS "${NODES[$i]}" "cat > $DATA_DIR/inst0/prefill.rdb" < "${base}_m${idx}.rdb" 2>/dev/null
                log "  ${NODES[$i]}: 预放置 RDB m$idx ($(stat -c%s ${base}_m${idx}.rdb)B)"
                break  # 每节点 1 实例默认; 多实例时 slot 布局也变, 不支持
            fi
            idx=$((idx+1))
        done <<< "$(cat ${base}.layout)"
    fi
    local cores_per_inst=$((CORES_PER_NODE / INSTANCES_PER_NODE))
    for ((j=0; j<INSTANCES_PER_NODE; j++)); do
        local port=$((PORT + j))
        # 该实例目录有预放置 rdb 则加载
        local RDB_FLAG=" --dbfilename prefill.rdb"
        ssh -n $SSH_OPTS "${NODES[$i]}" "test -f $DATA_DIR/inst$j/prefill.rdb" >/dev/null 2>&1 || RDB_FLAG=""
        [ -n "$RDB_FLAG" ] && log "    ${NODES[$i]} inst$j: 将加载 prefill.rdb"
        local c0=$((j * cores_per_inst))
        local c1=$(((j + 1) * cores_per_inst - 1))
        local ddir="$DATA_DIR/inst${j}"
        log "  ${NODES[$i]} inst$j: port=$port cores=$c0-$c1 dir=$ddir"
        local start_cmd="mkdir -p $ddir && cd $REDIS_DIR && \
            numactl --membind=0 taskset -c $c0-$c1 \
            ./src/redis-server \
                --port $port --bind 0.0.0.0 --protected-mode no \
                --cluster-enabled yes \
                --cluster-config-file nodes.conf \
                --cluster-node-timeout $CLUSTER_TIMEOUT \
                --cluster-announce-ip $ip \
                --io-threads $IO_THREADS --io-threads-do-reads yes \
                --appendonly no --save '' \
                --dir $ddir --logfile $ddir/redis.log \
                --daemonize yes ${RDB_FLAG}"
        local src_out
        src_out=$(ssh_node $i "$start_cmd")
        local src_rc=$?
        log "    start rc=$src_rc out=$(echo "$src_out" | head -c 100)"
    done
}

wait_port() {
    local ip=$1 port=$2 count=0
    while ! ($REDIS_DIR/src/redis-cli -h $ip -p $port PING 2>/dev/null | grep -q PONG); do
        sleep 0.5; ((count++))
        [ $count -gt 60 ] && { log "TIMEOUT waiting $ip:$port"; return 1; }
    done
}

# ----------------------------------------------------------------------------
# 给节点补齐 [from,to] 的 slot。RDB 预加载的 key 会触发 auto-claim (busy),
# busy 的正是 key 所在 slot 且已在正确节点, 只补缺失的空 slot。
# 注意: redis-cli 收到 ERR 回复时 exit code 仍为 0, 必须判输出内容。
fill_node_slots() {
    local ip=$1 from=$2 to=$3 try mine_id missing out
    for try in 1 2 3 4 5; do
        out=$($REDIS_DIR/src/redis-cli -h $ip -p $PORT CLUSTER ADDSLOTS $(seq $from $to) 2>&1)
        [ "$out" = "OK" ] && return 0
        mine_id=$($REDIS_DIR/src/redis-cli -h $ip -p $PORT CLUSTER MYID 2>/dev/null)
        [ -z "$mine_id" ] && { sleep 1; continue; }
        missing=$($REDIS_DIR/src/redis-cli -h $ip -p $PORT CLUSTER NODES 2>/dev/null | awk -v me="$mine_id" -v f="$from" -v t="$to" '
            $1==me { for(i=9;i<=NF;i++){ if($i ~ /-/) { split($i,a,"-"); for(s=a[1];s<=a[2];s++) have[s]=1 } else have[$i+0]=1 } }
            END { for(s=f;s<=t;s++) if(!(s in have)) print s }')
        [ -z "$missing" ] && return 0
        out=$($REDIS_DIR/src/redis-cli -h $ip -p $PORT CLUSTER ADDSLOTS $missing 2>&1)
        [ "$out" = "OK" ] && return 0
        sleep 1
    done
    log "WARN: fill_node_slots $ip [$from,$to] 未完全成功"
    return 1
}

create_cluster() {
    local ntotal=$((NNODES * INSTANCES_PER_NODE))
    local nmasters=$((ntotal / (REPLICAS + 1)))
    log "create cluster (--cluster-replicas $REPLICAS, $ntotal nodes = $nmasters masters)..."
    local endpoints=""
    for ((i=0; i<NNODES; i++)); do
        for ((j=0; j<INSTANCES_PER_NODE; j++)); do
            endpoints="$endpoints ${IPS[$i]}:$((PORT + j))"
        done
    done
    # 空 cluster (无 RDB 预加载) 走原生 --cluster create
    if [ ! -f "$DATA_DIR/inst0/prefill.rdb" ]; then
        echo yes | $REDIS_DIR/src/redis-cli --cluster create $endpoints --cluster-replicas $REPLICAS 2>&1 \
            | grep -E "Slots|Master|Replica|slots:|OK|All|coverage|agree|Can't|err" | head -60
        return $?
    fi
    # RDB 预加载模式: --cluster create 拒绝非空节点 (ERR Node is not empty),
    # 改用 MEET + 均分 ADDSLOTS (fill 补差, 见 fill_node_slots)
    local n_per=$((16384 / nmasters)) m=0 first_ip=${IPS[0]} first_port=$PORT
    $REDIS_DIR/src/redis-cli -h $first_ip -p $first_port CLUSTER MEET ${IPS[1]} $PORT >/dev/null 2>&1
    [ $NNODES -gt 2 ] && $REDIS_DIR/src/redis-cli -h $first_ip -p $first_port CLUSTER MEET ${IPS[2]} $PORT >/dev/null 2>&1
    [ $NNODES -gt 3 ] && $REDIS_DIR/src/redis-cli -h $first_ip -p $first_port CLUSTER MEET ${IPS[3]} $PORT >/dev/null 2>&1
    sleep 3
    local k=0
    for ((i=0; i<NNODES; i++)); do
        for ((j=0; j<INSTANCES_PER_NODE; j++)); do
            [ $k -ge $nmasters ] && break
            local f=$((k * n_per)) t=$(( (k+1) * n_per - 1 ))
            [ $k -eq $((nmasters-1)) ] && t=16383
            fill_node_slots ${IPS[$i]} $f $t
            k=$((k+1))
        done
    done
    sleep 2
}

check_cluster() {
    log "cluster info:"
    $REDIS_DIR/src/redis-cli -h ${IPS[0]} -p $PORT CLUSTER INFO 2>/dev/null \
        | grep -E "cluster_state|cluster_slots_ok|cluster_known_nodes|cluster_size"
    log "vset slot distribution (vset$KEY_OFFSET..vset$((KEY_OFFSET+NUM_VSETS-1))):"
    for v in $(seq $KEY_OFFSET $((KEY_OFFSET + NUM_VSETS - 1))); do
        local slot=$($REDIS_DIR/src/redis-cli -h ${IPS[0]} -p $PORT CLUSTER KEYSLOT vset$v 2>/dev/null)
        printf "  vset%-3d -> slot %s\n" "$v" "$slot"
    done
}

# ----------------------------------------------------------------------------
rdb_tag() { [ "${BASELINE_NOQUANT:-1}" = "0" ] && echo INT8 || echo NOQUANT; }

# master 实例列表 (ip:port), 与 --cluster create 的分配一致时才能复用 RDB
# 生成时把当时的 master 顺序写入 .layout, 加载时校验一致
cluster_rdb_base() { echo "$RDB_DIR/${NUM_VSETS}V${VECTORS_PER_VSET}_${DIM}D_$(rdb_tag)_rc"; }

# master ip:port 列表 (排序稳定), 从 CLUSTER NODES 解析 (无 replicas 时全 master;
# 有 replicas 时取 role==master 的行)
cluster_masters_list() {
    $REDIS_DIR/src/redis-cli -h ${IPS[0]} -p $PORT CLUSTER NODES 2>/dev/null \
        | awk '$3 ~ /master/ {sub(/@.*/, "", $2); print $2}' | sort
}

# prefill 成功后捕获: 逐 master SAVE → <base>_mN.rdb + .layout(各master的vset归属)
capture_cluster_rdb() {
    [ "$BASELINE_RDB" = "0" ] && return 0
    local base; base=$(cluster_rdb_base)
    mkdir -p "$RDB_DIR"
    local masters
    masters=$(cluster_masters_list)
    [ -z "$masters" ] && return 0
    printf '%s\n' "$masters" > "${base}.layout"
    local idx=0 m ip port
    while IFS= read -r m; do
        ip=${m%%:*}; port=${m##*:}
        local node=HW01
        for ((i=0; i<NNODES; i++)); do [[ "${IPS[$i]}" == "$ip" ]] && node=${NODES[$i]}; done
        $REDIS_DIR/src/redis-cli -h $ip -p $port SAVE >/dev/null 2>&1
        # dump.rdb 落在该实例 --dir; port 对应 inst$((port-PORT)) 目录
        local inst=$((port - PORT))
        local d="$DATA_DIR/inst${inst}/dump.rdb"
        # 注意: ssh 必须 -n (否则吃掉 while 的 stdin, 循环只跑一轮)
        ssh -n $SSH_OPTS "$node" "[ -f $d ]" >/dev/null 2>&1 || continue
        ssh -n $SSH_OPTS "$node" "cat $d" > "${base}_m${idx}.rdb" && chmod 444 "${base}_m${idx}.rdb"
        ssh -n $SSH_OPTS "$node" "rm -f $d" 2>/dev/null
        log "    [rdb] m$idx ← $node:$port ($d)"
        idx=$((idx+1))
    done <<< "$masters"
    log "  [rdb] 捕获 $idx masters → ${base}_m*.rdb"
}

# 兼容 RDB 是否存在且布局匹配
cluster_rdb_ready() {
    [ "$BASELINE_RDB" = "0" ] && return 1
    local base; base=$(cluster_rdb_base)
    [ -f "${base}.layout" ] || return 1
    local masters
    masters=$(cluster_masters_list)
    [ "$masters" != "$(cat ${base}.layout)" ] && return 1
    local idx=0
    while IFS= read -r _; do
        [ -f "${base}_m${idx}.rdb" ] || return 1
        idx=$((idx+1))
    done <<< "$(cat ${base}.layout)"
    return 0
}

prefill() {
    local v0=$KEY_OFFSET v1=$((KEY_OFFSET + NUM_VSETS - 1))
    # RDB 已在各 master 就位 → 校验一个样本 vset 后跳过
    if cluster_rdb_ready; then
        local card=$($REDIS_DIR/src/redis-cli -c -h ${IPS[0]} -p $PORT VCARD vset$v0 2>/dev/null)
        if [ "$card" = "$VECTORS_PER_VSET" ]; then
            log "prefill skipped: using pre-loaded RDBs (vset$v0 VCARD=$card)"
            return 0
        fi
        log "WARN: RDB VCARD=$card != $VECTORS_PER_VSET, 回退 prefill"
    fi
    log "prefill: vset$v0..vset$v1 ($NUM_VSETS vsets) x $VECTORS_PER_VSET vectors (dim=$DIM)..."
    $REDIS_DIR/src/redis-cli -c -h ${IPS[0]} -p $PORT FLUSHALL >/dev/null 2>&1
    awk -v off=$KEY_OFFSET -v m=$NUM_VSETS -v k=$VECTORS_PER_VSET -v dim=$DIM 'BEGIN{
        srand(42);
        for (i=0; i<m; i++) {
            v = off + i;
            for (e=0; e<k; e++) {
                printf "VADD vset%d VALUES %d", v, dim;
                for (j=0; j<dim; j++) printf " %f", rand()*0.001;
                printf " elem%d%s\n", e, (ENVIRON["BASELINE_NOQUANT"]=="0" ? "" : " NOQUANT");
            }
        }
    }' | $REDIS_DIR/src/redis-cli -c -h ${IPS[0]} -p $PORT >/dev/null 2>&1
    log "prefill done. VCARD sample:"
    for v in $v0 $v1; do
        printf "  vset%d VCARD=%s\n" "$v" \
            "$($REDIS_DIR/src/redis-cli -c -h ${IPS[0]} -p $PORT VCARD vset$v 2>/dev/null)"
    done
    capture_cluster_rdb
}

# ----------------------------------------------------------------------------
snapshot_jiffies_node() {
    local i=$1 port=$PORT
    ssh $SSH_OPTS "${NODES[$i]}" \
        "ut=0; st=0; for pid in \$(pgrep -x redis-server); do \
             if tr '\0' ' ' < /proc/\$pid/cmdline 2>/dev/null | grep -Eq \":${port}\$|:${port} \"; then \
                 read u s < <(awk '{u+=\$14; s+=\$15} END{printf \"%d %d\", u+0, s+0}' /proc/\$pid/task/*/stat 2>/dev/null); \
                 ut=\$((ut + \${u:-0})); st=\$((st + \${s:-0})); \
             fi; \
         done; echo \"\$ut \$st\"" \
        2>/dev/null | tail -1
}

snapshot_si_node() {
    local i=$1
    ssh $SSH_OPTS "${NODES[$i]}" "awk '/^cpu /{print \$8}' /proc/stat 2>/dev/null" 2>/dev/null | tail -1
}

snapshot_rss_node() {
    local i=$1 port=$PORT
    ssh $SSH_OPTS "${NODES[$i]}" \
        "rss=0; for pid in \$(pgrep -x redis-server); do \
             if tr '\0' ' ' < /proc/\$pid/cmdline 2>/dev/null | grep -Eq \":${port}\$|:${port} \"; then \
                 r=\$(awk '/^VmRSS:/{print \$2+0}' /proc/\$pid/status 2>/dev/null); \
                 rss=\$((rss + \${r:-0})); \
             fi; \
         done; echo \$rss" \
        2>/dev/null | tail -1
}

# ----------------------------------------------------------------------------
run_one_config() {
    local idx=$1
    local t=${TS[$idx]} c=${CS[$idx]} p=${PS[$idx]}
    local raw="$RAWDIR/vemb_t${t}_c${c}_p${p}.log"
    log "--- config $((idx+1))/${NCONFIGS}: t=$t c=$c pipeline=$p ---"

    # jiffies before
    local jb_ut=0 jb_st=0
    JB_NS=$(date +%s%N)

    for ((i=0; i<NNODES; i++)); do
        read u s < <(snapshot_jiffies_node $i)
        jb_ut=$((jb_ut + ${u:-0})); jb_st=$((jb_st + ${s:-0}))
    done
    local si_b=0 rss_b=0
    for ((i=0; i<NNODES; i++)); do
        si_b=$((si_b + $(snapshot_si_node $i)))
        rss_b=$((rss_b + $(snapshot_rss_node $i)))
    done

    # memtier cluster mode: VEMB __key__ elem0 raw
    local remote_cmd="$MEMTIER -s ${IPS[0]} -p $PORT --cluster-mode \
        -t $t -c $c --pipeline=$p \
        --command='VEMB __key__ elem0 raw' --command-key-pattern=R \
        --key-prefix=vset --key-minimum=$KEY_OFFSET --key-maximum=$((KEY_OFFSET + NUM_VSETS - 1)) \
        --data-size=128 \
        --test-time=$TEST_TIME --hide-histogram"
    if [ "$MEMTIER_HOST" = "HW01" ]; then
        eval "$remote_cmd" > "$raw" 2>&1 || true
    else
        ssh $SSH_OPTS "$MEMTIER_HOST" "$remote_cmd" > "$raw" 2>&1 || true
    fi

    # jiffies after
    JA_NS=$(date +%s%N)
    ELAPSED_NS=$((JA_NS - JB_NS > 0 ? JA_NS - JB_NS : TEST_TIME * 1000000000))
    # core 分母 = 纳秒实测窗口 (与脚本13对齐: 分子分母同区间)
    local ja_ut=0 ja_st=0
    for ((i=0; i<NNODES; i++)); do
        read u s < <(snapshot_jiffies_node $i)
        ja_ut=$((ja_ut + ${u:-0})); ja_st=$((ja_st + ${s:-0}))
    done
    local si_a=0 rss_a=0
    for ((i=0; i<NNODES; i++)); do
        si_a=$((si_a + $(snapshot_si_node $i)))
        rss_a=$((rss_a + $(snapshot_rss_node $i)))
    done
    local cores=$(awk -v d=$((ja_ut + ja_st - jb_ut - jb_st)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
    local core_ut=$(awk -v d=$((ja_ut - jb_ut)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
    local core_st=$(awk -v d=$((ja_st - jb_st)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
    local si=$(awk -v d=$((si_a - si_b)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
    local rss=$(((rss_a + rss_b) / 2))

    # parse Totals (cluster mode has MOVED/ASK columns)
    local totals ops avg p50 p99 p999 kb
    totals=$(grep "^Totals" "$raw" | tail -1)
    read ops avg p50 p99 p999 kb < <(
        echo "$totals" | awk '{
            if (NF>=9) printf "%s %s %s %s %s %s", $2,$5,$6,$7,$8,$9
            else       printf "0 NA NA NA NA NA"
        }'
    )

    printf "VEMB\tredis_cluster_4node\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
        "$t" "$c" "$p" "$ops" "$avg" "$p50" "$p99" "$p999" "$kb" "$cores" "$core_ut" "$core_st" "$si" "$rss" >> "$TSV"
    log "  => ops/s=$ops  avg=${avg}ms  p50=${p50}ms  p99=${p99}ms  cores=$cores"
}

# ============================================================================
trap 'cleanup_all' EXIT INT TERM

log "=== STEP 1: cleanup + KEY_OFFSET ==="
if [ -z "${KEY_OFFSET+x}" ]; then
    KEY_OFFSET=$(pick_key_offset)
    KEY_OFFSET=${KEY_OFFSET:-1}
    log "auto KEY_OFFSET=$KEY_OFFSET (NUM_VSETS=$NUM_VSETS)"
else
    log "user KEY_OFFSET=$KEY_OFFSET (NUM_VSETS=$NUM_VSETS)"
fi
cleanup_all
sleep 1

log "=== STEP 2: start $NNODES x $INSTANCES_PER_NODE redis instances ==="
for ((i=0; i<NNODES; i++)); do start_node $i; done
sleep 2
for ((i=0; i<NNODES; i++)); do
    for ((j=0; j<INSTANCES_PER_NODE; j++)); do
        wait_port ${IPS[$i]} $((PORT + j)) || { log "FAIL: ${IPS[$i]}:$((PORT+j)) not up"; exit 1; }
    done
done
log "all $((NNODES * INSTANCES_PER_NODE)) instances up."

log "=== STEP 3: create cluster ==="
create_cluster
cstate=""
for ((w=0; w<30; w++)); do
    cstate=$($REDIS_DIR/src/redis-cli -h ${IPS[0]} -p $PORT CLUSTER INFO 2>/dev/null \
             | awk -F: '/cluster_state/{gsub(/[[:space:]]/,"",$2);print $2}')
    [ "$cstate" = "ok" ] && break
    sleep 1
done
if [ "$cstate" != "ok" ]; then
    log "FAIL: cluster_state=$cstate (expect ok)"
    exit 1
fi
log "cluster_state=ok after ${w}s"
check_cluster

log "=== STEP 4: prefill ==="
prefill

log "=== STEP 5: VEMB 9-config sweep ==="
log "  MEMTIER_HOST=$MEMTIER_HOST  NUM_VSETS=$NUM_VSETS  DIM=$DIM"
printf "op\tserver_type\tt\tc\tpipeline\tops_sec\tavg_lat_ms\tp50_ms\tp99_ms\tp999_ms\tkb_sec\tcores\tcore_ut\tcore_st\tsi\trss_kb\n" > "$TSV"

for ((idx=0; idx<NCONFIGS; idx++)); do
    run_one_config $idx
done

log "=== DONE ==="
log "TSV  : $TSV"
log "raw  : $RAWDIR/vemb_t*_c*_p*.log"
echo "----- summary -----"
column -t -s $'\t' "$TSV"
