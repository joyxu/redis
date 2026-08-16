#!/bin/bash
# hpc_redis scaleout 扩容过程吞吐测试（双节点 0→1 扩容，三段采集）
#
# 三段采集法：
#   段1 baseline    — 稳态 VEMB 读 TEST_TIME 秒
#   段2 during      — 后台 VEMB 读 + 触发扩容，等完成后停止
#   段3 after       — 新稳态 VEMB 读 TEST_TIME 秒
#
# 用法: bash benchmark/hpc_redis_scaleout_throughput.sh
#   smoke: TEST_TIME=3 PREFILL_KEYS=1000 bash benchmark/hpc_redis_scaleout_throughput.sh
#
# 所有产物（yaml、log、tsv、raw）写 benchmark/results/scaleout/<run_id>/ 下，不放 /tmp。
set -euo pipefail

# ============================================================================
# 网络拓扑
# ============================================================================
NODE0_HOST="${NODE0_HOST:-192.168.1.111}"
NODE1_HOST="${NODE1_HOST:-192.168.1.112}"
SSH_USER="${SSH_USER:-root}"
REMOTE_DIR="${REMOTE_DIR:-/root/gqs/codespace/UnifiedBus/hpc-redis}"
MEMTIER="${MEMTIER:-$REMOTE_DIR/memtier_benchmark/memtier_benchmark}"

PAYLOAD_LOCAL_PATH="${PAYLOAD_LOCAL_PATH:-/dev/obmm_shmdev1}"
PAYLOAD_PEER_PATH="${PAYLOAD_PEER_PATH:-/dev/obmm_shmdev5}"
REQUEST_LOCAL_PATH="${REQUEST_LOCAL_PATH:-/dev/obmm_shmdev2}"
REQUEST_PEER_PATH="${REQUEST_PEER_PATH:-/dev/obmm_shmdev6}"
RESPONSE_LOCAL_PATH="${RESPONSE_LOCAL_PATH:-/dev/obmm_shmdev4}"
RESPONSE_PEER_PATH="${RESPONSE_PEER_PATH:-/dev/obmm_shmdev8}"

# ============================================================================
# server 参数
# ============================================================================
PORT="${PORT:-6391}"
COORD_PORT="${COORD_PORT:-7391}"
DIM="${DIM:-300}"
VECTOR_BYTES="${VECTOR_BYTES:-$((DIM * 4))}"
MAX_VECTORS="${MAX_VECTORS:-65536}"
PIO="${PIO:-21}"
SNW="${SNW:-21}"
UB_RPC_TIMEOUT_MS="${UB_RPC_TIMEOUT_MS:-2000}"
WARM_REGION_BYTES="${WARM_REGION_BYTES:-4294967296}"
REMOTE_META_MMAP_OFFSET="${REMOTE_META_MMAP_OFFSET:-$((WARM_REGION_BYTES + 1073741824))}"

# ============================================================================
# 测试参数
# ============================================================================
PREFILL_KEYS="${PREFILL_KEYS:-10000}"
STEADY_KEYS="${STEADY_KEYS:-$PREFILL_KEYS}"
STEADY_KEY_MIN="${STEADY_KEY_MIN:-$((PREFILL_KEYS + 1))}"
STEADY_KEY_MAX="${STEADY_KEY_MAX:-$((PREFILL_KEYS + STEADY_KEYS))}"
TEST_TIME="${TEST_TIME:-30}"
# 后台 memtier 时长：必须 > 扩容耗时，让进程能自然结束输出 Totals
BG_TIME_SCALEOUT="${BG_TIME_SCALEOUT:-60}"
PIPELINE="${PIPELINE:-32}"
MEMTIER_T="${MEMTIER_T:-64}"
MEMTIER_C="${MEMTIER_C:-4}"
VNODE_COUNT="${VNODE_COUNT:-10}"

# ============================================================================
# Epoch（用时间戳避免与历史测试冲突）
# ============================================================================
EPOCH_BASE=${EPOCH_BASE:-$(date +%s)}
INIT_EPOCH=$((EPOCH_BASE + 1))
MIGRATION_EPOCH=$((EPOCH_BASE + 101))
CUTOVER_EPOCH=$((EPOCH_BASE + 102))
CONTROL_TIMEOUT=5000
COMBINED_TIMEOUT=180000

# ============================================================================
# 产物路径（本地 + 远端都落在 benchmark/results/scaleout/<run_id>/ 下，不放 /tmp）
# ============================================================================
RESULTS_DIR="${RESULTS_DIR:-$(cd "$(dirname "$0")" && pwd)/results/scaleout}"
RUN_ID="${RUN_ID:-$(date +%Y%m%d_%H%M%S)}"

# 本地产物（TSV 汇总）
LOCAL_RUN_DIR="$RESULTS_DIR/$RUN_ID"
mkdir -p "$LOCAL_RUN_DIR"
TSV="$LOCAL_RUN_DIR/summary.tsv"

# 远端产物子目录（相对 REMOTE_DIR；node0/node1 同路径）
REMOTE_SUBDIR="benchmark/results/scaleout/$RUN_ID"
NODE0_MANIFEST="$REMOTE_DIR/$REMOTE_SUBDIR/node0.yaml"
NODE1_MANIFEST="$REMOTE_DIR/$REMOTE_SUBDIR/node1.yaml"
NODE0_PEER_MAP="$REMOTE_DIR/$REMOTE_SUBDIR/node0_peermap.yaml"
NODE0_LOG="$REMOTE_DIR/$REMOTE_SUBDIR/server_node0.log"
NODE1_LOG="$REMOTE_DIR/$REMOTE_SUBDIR/server_node1.log"
COORD_OUT="$REMOTE_DIR/$REMOTE_SUBDIR/coord.out"
COORD_ERR="$REMOTE_DIR/$REMOTE_SUBDIR/coord.err"
PREFILL_LOG="$REMOTE_DIR/$REMOTE_SUBDIR/prefill.log"
RAW_DIR="$REMOTE_DIR/$REMOTE_SUBDIR/raw"

# 工具函数
ssh_run() { ssh -p 22 "${SSH_USER}@$1" "${@:2}"; }
log() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }

# 在两个节点上提前创建子目录
ssh_run "$NODE0_HOST" "mkdir -p $REMOTE_DIR/$REMOTE_SUBDIR/raw"
ssh_run "$NODE1_HOST" "mkdir -p $REMOTE_DIR/$REMOTE_SUBDIR/raw"

# TSV 表头
printf 'phase\tops_sec\thits\tp50_ms\tp99_ms\twall_s\tnote\tcore_ut\tcore_st\tsi\trss_kb\n' > "$TSV"

# ============================================================================
# Manifest：按 scripts/test_scale_host.md 记录的 UB 映射生成到远端产物目录
# ============================================================================
write_manifests() {
    log "Phase 1: Write manifests to $REMOTE_SUBDIR/"
    ssh_run "$NODE0_HOST" "cat >$NODE0_MANIFEST <<YAML
local_ub_node_id: 0
local_region_weight: 4

remote_meta_provider: ub
remote_meta_path: $PAYLOAD_LOCAL_PATH
remote_meta_mmap_offset: $REMOTE_META_MMAP_OFFSET
remote_meta_entries: $MAX_VECTORS
remote_meta_buckets: $((MAX_VECTORS * 2))
ub_rpc_timeout_ms: $UB_RPC_TIMEOUT_MS

warm_regions:
  - region_id: 100
    provider: ub
    path: $PAYLOAD_LOCAL_PATH
    mmap_offset: 0
    bytes: $WARM_REGION_BYTES
    value_size: $VECTOR_BYTES
    home_ub_node_id: 0
    weight: 1
YAML"

    ssh_run "$NODE1_HOST" "cat >$NODE1_MANIFEST <<YAML
local_ub_node_id: 1
local_region_weight: 4

remote_meta_provider: ub
remote_meta_path: $PAYLOAD_LOCAL_PATH
remote_meta_mmap_offset: $REMOTE_META_MMAP_OFFSET
remote_meta_entries: $MAX_VECTORS
remote_meta_buckets: $((MAX_VECTORS * 2))
ub_rpc_timeout_ms: $UB_RPC_TIMEOUT_MS

warm_regions:
  - region_id: 101
    provider: ub
    path: $PAYLOAD_LOCAL_PATH
    mmap_offset: 0
    bytes: $WARM_REGION_BYTES
    value_size: $VECTOR_BYTES
    home_ub_node_id: 1
    weight: 1
  - region_id: 100
    provider: ub
    path: $PAYLOAD_PEER_PATH
    mmap_offset: 0
    bytes: $WARM_REGION_BYTES
    value_size: $VECTOR_BYTES
    home_ub_node_id: 0
    weight: 1

remote_meta_views:
  - owner_id: 0
    provider: ub
    path: $PAYLOAD_PEER_PATH
    mmap_offset: $REMOTE_META_MMAP_OFFSET
    entries: $MAX_VECTORS
    buckets: $((MAX_VECTORS * 2))

ub_rpc_peers:
  - owner_id: 0
    provider: ub
    request_path: $REQUEST_LOCAL_PATH
    request_mmap_offset: 8388608
    response_path: $RESPONSE_PEER_PATH
    response_mmap_offset: 16777216
    inbound_request_path: $REQUEST_PEER_PATH
    inbound_request_mmap_offset: 8388608
    outbound_response_path: $RESPONSE_LOCAL_PATH
    outbound_response_mmap_offset: 16777216
YAML"

    ssh_run "$NODE0_HOST" "cat >$NODE0_PEER_MAP <<YAML
expected_local_owner_id: 0
attach_now: true
ub_rpc_timeout_ms: $UB_RPC_TIMEOUT_MS

warm_regions:
  - region_id: 101
    provider: ub
    path: $PAYLOAD_PEER_PATH
    mmap_offset: 0
    bytes: $WARM_REGION_BYTES
    value_size: $VECTOR_BYTES
    home_ub_node_id: 1
    weight: 1

remote_meta_views:
  - owner_id: 1
    provider: ub
    path: $PAYLOAD_PEER_PATH
    mmap_offset: $REMOTE_META_MMAP_OFFSET
    entries: $MAX_VECTORS
    buckets: $((MAX_VECTORS * 2))

ub_rpc_peers:
  - owner_id: 1
    provider: ub
    request_path: $REQUEST_LOCAL_PATH
    request_mmap_offset: 8388608
    response_path: $RESPONSE_PEER_PATH
    response_mmap_offset: 16777216
    inbound_request_path: $REQUEST_PEER_PATH
    inbound_request_mmap_offset: 8388608
    outbound_response_path: $RESPONSE_LOCAL_PATH
    outbound_response_mmap_offset: 16777216
YAML"
}

# ============================================================================
# 启停 server / 端口探测
# ============================================================================
# 在指定节点启动集成 redis-server（numactl + taskset 绑 NUMA0 0-95 核）
start_node() {
    local host=$1 manifest=$2 logfile=$3 reset=$4
    local reset_flag=""
    [ "$reset" = "1" ] && reset_flag="--vemb-v16-reset-warm-regions yes"
    ssh_run "$host" "cd $REMOTE_DIR && rm -f $logfile && \
        numactl --membind=0 taskset -c 0-95 ./src/redis-server \
        --port $PORT --bind 0.0.0.0 --protected-mode no \
        --vemb-v16-enabled yes --vemb-v16-dim $DIM \
        --vemb-v16-max-vectors $MAX_VECTORS \
        --vemb-v16-warm-regions-manifest $manifest \
        $reset_flag \
        --vemb-v16-proxy-io-threads $PIO --vemb-v16-supernode-workers $SNW \
        --daemonize yes --logfile $logfile --loglevel warning \
        >/dev/null 2>&1"
}

# 停指定节点上的集成 redis-server（按端口匹配）
stop_node() {
    local host=$1
    ssh_run "$host" "pkill -9 -f 'redis-server.*:$PORT' 2>/dev/null || true; pkill -9 -f 'vemb_v16_topology_ctl.*$COORD_PORT' 2>/dev/null || true; sleep 0.5" || true
}

verify_remote_prereqs() {
    local host=$1
    ssh_run "$host" "test -d $REMOTE_DIR && test -x $REMOTE_DIR/src/redis-server && test -x $REMOTE_DIR/benchmark/vemb_v16_topology_ctl && test -x $REMOTE_DIR/benchmark/vemb_v16_bench && test -x $MEMTIER && ls $PAYLOAD_LOCAL_PATH $PAYLOAD_PEER_PATH $REQUEST_LOCAL_PATH $REQUEST_PEER_PATH $RESPONSE_LOCAL_PATH $RESPONSE_PEER_PATH >/dev/null"
}

verify_remote_ub_paths_idle() {
    local host=$1
    ssh_run "$host" "paths='$REQUEST_LOCAL_PATH $RESPONSE_LOCAL_PATH $REQUEST_PEER_PATH $RESPONSE_PEER_PATH'; out=\$(lsof \$paths 2>/dev/null || true); if [ -z \"\$out\" ]; then exit 0; fi; filtered=\$(printf '%s\n' \"\$out\" | awk 'NR==1 || \$1 ~ /bash|sh|ssh|sshd|lsof|awk/'); if [ \"\$(printf '%s\n' \"\$out\" | wc -l)\" -ne \"\$(printf '%s\n' \"\$filtered\" | wc -l)\" ]; then printf 'unexpected UB path users on ${host}:\n%s\n' \"\$out\"; exit 1; fi"
}

# 等 redis-server 在节点上 listen（最多 25s）
wait_port() {
    local host=$1
    for _ in $(seq 1 50); do
        ssh_run "$host" "ss -tln | grep -q ':$PORT '" && return 0
        sleep 0.5
    done
    return 1
}

# ============================================================================
# memtier 负载工具
# ============================================================================
# 前台跑 memtier 拿 Totals（baseline/after 段用）
run_memtier() {
    local host=$1 tt=$2 outfile=$3 extra=$4
    local endpoints=${5:-}
    local key_min=${6:-1}
    local key_max=${7:-$PREFILL_KEYS}
    local route_args="-s $host -p $PORT"
    if [ -n "$endpoints" ]; then
        route_args="--vemb-v16-endpoints=$endpoints"
    fi
    ssh_run "$host" "numactl --membind=1 taskset -c 96-191 \
        $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
        $route_args -t $MEMTIER_T -c $MEMTIER_C --pipeline=$PIPELINE \
        --ratio=0:1 --key-pattern=R:R --key-prefix=item: \
        --key-minimum=$key_min --key-maximum=$key_max \
        --test-time=$tt $extra >$outfile 2>&1" || true
    local tot; tot=$(ssh_run "$host" "grep '^Totals' $outfile 2>/dev/null | tail -1")
    local ops hits p50 p99
    ops=$(echo "$tot" | awk '{print $2}')
    hits=$(echo "$tot" | awk '{print $3}')
    p50=$(echo "$tot" | awk '{print $(NF-3)}')
    p99=$(echo "$tot" | awk '{print $(NF-2)}')
    [ -z "$ops" ] && ops=0
    echo "$ops $hits $p50 $p99"
}

# 前台跑 memtier：exec_host 上执行，直连 target_host:PORT
run_memtier_target() {
    local exec_host=$1 target_host=$2 tt=$3 outfile=$4 extra=$5
    local key_min=${6:-1}
    local key_max=${7:-$PREFILL_KEYS}
    ssh_run "$exec_host" "numactl --membind=1 taskset -c 96-191 \
        $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
        -s $target_host -p $PORT -t $MEMTIER_T -c $MEMTIER_C --pipeline=$PIPELINE \
        --ratio=0:1 --key-pattern=R:R --key-prefix=item: \
        --key-minimum=$key_min --key-maximum=$key_max \
        --test-time=$tt $extra >$outfile 2>&1" || true
    local tot; tot=$(ssh_run "$exec_host" "grep '^Totals' $outfile 2>/dev/null | tail -1")
    local ops hits p50 p99
    ops=$(echo "$tot" | awk '{print $2}')
    hits=$(echo "$tot" | awk '{print $3}')
    p50=$(echo "$tot" | awk '{print $(NF-3)}')
    p99=$(echo "$tot" | awk '{print $(NF-2)}')
    [ -z "$ops" ] && ops=0
    echo "$ops $hits $p50 $p99"
}

# 后台启 memtier（during 段用，test-time 后自然退出）
run_memtier_bg() {
    local host=$1 outfile=$2 bg_time=$3 extra=${4:-} endpoints=${5:-}
    local route_args="-s $host -p $PORT"
    if [ -n "$endpoints" ]; then
        route_args="--vemb-v16-endpoints=$endpoints"
    fi
    ssh_run "$host" "numactl --membind=1 taskset -c 96-191 \
        $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
        $route_args -t $MEMTIER_T -c $MEMTIER_C --pipeline=$PIPELINE \
        --ratio=0:1 --key-pattern=R:R --key-prefix=item: \
        --key-minimum=1 --key-maximum=$PREFILL_KEYS \
        --test-time=$bg_time $extra >$outfile 2>&1 &" || true
}

# 强制 kill 后台 memtier（清理用）
stop_memtier_bg() {
    ssh_run "$NODE0_HOST" "pkill -9 -f memtier_benchmark 2>/dev/null" || true
}

# 从后台 memtier 日志提取 Totals（与 baseline/after 口径一致；不回退瞬时速率）
parse_bg_out() {
    local host=$1 outfile=$2
    local tot ops hits p50 p99
    # memtier 偶尔在 sleep 后才 flush Totals，给 15s 重试窗口
    local i
    for i in $(seq 1 15); do
        tot=$(ssh_run "$host" "grep '^Totals' $outfile 2>/dev/null | tail -1")
        [ -n "$tot" ] && break
        sleep 1
    done
    if [ -z "$tot" ]; then
        echo "ERROR: no Totals in $outfile (memtier killed?)" >&2
        echo "0 0 NA NA"
        return 1
    fi
    ops=$(echo "$tot" | awk '{print $2}')
    hits=$(echo "$tot" | awk '{print $3}')
    p50=$(echo "$tot" | awk '{print $(NF-3)}')
    p99=$(echo "$tot" | awk '{print $(NF-2)}')
    [ -z "$ops" ] && ops=0
    echo "$ops $hits $p50 $p99"
}

# 把一段结果写 TSV 并打 log
record_phase() {
    local phase=$1 ops=$2 hits=$3 p50=$4 p99=$5 wall=$6 note=$7 cut=$8 sut=$9 si=${10} rss=${11}
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$phase" "$ops" "$hits" "$p50" "$p99" "$wall" "$note" "$cut" "$sut" "$si" "$rss" >> "$TSV"
    log "$phase: ops=$ops p99=$p99 wall=${wall}s ($note) core_ut=$cut core_st=$sut si=$si rss=$rss"
}

# ── 双节点 server 资源采集 ──
# 在指定 host 上找 server pid（按端口匹配），输出 "ut st si rss"
# ut/st: 该 pid 所有 task 的 utime/stime jiffies 之和
# si   : /proc/stat 的 softirq jiffies（全局）
# rss  : 该 pid 的 VmRSS KB
snap_host() {
    local host=$1
    ssh_run "$host" "
        pid=\$(pgrep -f 'redis-server.*:$PORT' 2>/dev/null | head -1)
        if [ -z \"\$pid\" ]; then echo '0 0 0 0'; exit 0; fi
        ut=\$(awk '{u+=\$14} END{print u+0}' /proc/\$pid/task/*/stat 2>/dev/null)
        st=\$(awk '{s+=\$15} END{print s+0}' /proc/\$pid/task/*/stat 2>/dev/null)
        si=\$(awk '/^cpu /{print \$8}' /proc/stat 2>/dev/null)
        rss=\$(awk '/^VmRSS:/{print \$2}' /proc/\$pid/status 2>/dev/null)
        printf '%s %s %s %s' \"\${ut:-0}\" \"\${st:-0}\" \"\${si:-0}\" \"\${rss:-0}\"
    "
}

# 双节点 snapshot：返回 "ut_sum st_sum si_sum rss_sum"（NODE0 + NODE1 求和）
snap_both() {
    local n0 n1
    read -r n0_ut n0_st n0_si n0_rss < <(snap_host "$NODE0_HOST")
    read -r n1_ut n1_st n1_si n1_rss < <(snap_host "$NODE1_HOST")
    printf '%d %d %d %d' \
        $((n0_ut + n1_ut)) $((n0_st + n1_st)) $((n0_si + n1_si)) $((n0_rss + n1_rss))
}

# 由 before/after 两份双节点 snapshot + wall 算出 4 个指标
# 用法: compute_metrics <b_ut> <b_st> <b_si> <a_ut> <a_st> <a_si> <rss> <wall>
# 输出: "core_ut core_st si rss"
compute_metrics() {
    local b_ut=$1 b_st=$2 b_si=$3 a_ut=$4 a_st=$5 a_si=$6 rss=$7 wall=$8
    local core_ut core_st si
    core_ut=$(awk -v d=$((a_ut - b_ut)) -v s=$wall 'BEGIN{ if(d<0||s<=0) print "NA"; else printf "%.2f", d/100.0/s }')
    core_st=$(awk -v d=$((a_st - b_st)) -v s=$wall 'BEGIN{ if(d<0||s<=0) print "NA"; else printf "%.2f", d/100.0/s }')
    si=$(awk -v d=$((a_si - b_si)) -v s=$wall 'BEGIN{ if(d<0||s<=0) print "NA"; else printf "%.2f", d/100.0/s }')
    [ -z "$rss" ] && rss="NA"
    printf '%s %s %s %s' "$core_ut" "$core_st" "$si" "$rss"
}

# 用 SET prefill PREFILL_KEYS 条向量到 node0
prefill_data() {
    log "Prefilling $PREFILL_KEYS vectors to node0"
    ssh_run "$NODE0_HOST" "numactl --membind=1 taskset -c 96-191 \
        $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
        -s $NODE0_HOST -p $PORT -t 32 -c 4 --pipeline=32 \
        --ratio=1:0 --key-pattern=S:S --key-prefix=item: \
        --key-minimum=1 --key-maximum=$PREFILL_KEYS -n $PREFILL_KEYS \
        >$PREFILL_LOG 2>&1"
}

prefill_steady_data() {
    local endpoints=$1 key_min=$2 key_max=$3 outfile=$4
    local count=$((key_max - key_min + 1))
    log "Prefilling post-cutover steady keyspace ($count vectors, endpoints=$endpoints)"
    ssh_run "$NODE0_HOST" "numactl --membind=1 taskset -c 96-191 \
        $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
        --vemb-v16-client-topology --vemb-v16-endpoints=$endpoints \
        -t 32 -c 4 --pipeline=32 \
        --ratio=1:0 --key-pattern=S:S --key-prefix=item: \
        --key-minimum=$key_min --key-maximum=$key_max -n $count \
        >$outfile 2>&1"
}

# ============================================================================
# 主流程
# ============================================================================

# --- Phase 1: Cleanup + Manifests ---
log "Phase 1: Cleanup + Manifests"
stop_node "$NODE0_HOST"
stop_node "$NODE1_HOST"
verify_remote_prereqs "$NODE0_HOST"
verify_remote_prereqs "$NODE1_HOST"
verify_remote_ub_paths_idle "$NODE0_HOST"
verify_remote_ub_paths_idle "$NODE1_HOST"
write_manifests

# --- Phase 2: Start servers ---
log "Phase 2: Start servers (node0, node1)"
start_node "$NODE0_HOST" "$NODE0_MANIFEST" "$NODE0_LOG" 1
wait_port "$NODE0_HOST" || { echo "FAIL: node0 not listening"; exit 1; }

start_node "$NODE1_HOST" "$NODE1_MANIFEST" "$NODE1_LOG" 1
wait_port "$NODE1_HOST" || { echo "FAIL: node1 not listening"; exit 1; }
sleep 2

log "Verify startup"
ssh_run "$NODE0_HOST" "grep -E 'remote meta ready|ub rpc ready|server ready' $NODE0_LOG" || true
ssh_run "$NODE1_HOST" "grep -E 'remote meta ready|registered.*remote meta|ub rpc ready|server ready' $NODE1_LOG" || true

# --- Phase 3: Initial topology + prefill ---
log "Phase 3: Initial topology (active={0}, epoch=$INIT_EPOCH) + prefill"
ssh_run "$NODE0_HOST" "cd $REMOTE_DIR && ./benchmark/vemb_v16_topology_ctl --set --transport tcp \
    --host $NODE0_HOST --port $PORT --epoch $INIT_EPOCH --min-write-epoch $INIT_EPOCH \
    --vnode-count $VNODE_COUNT \
    --active 0 --standby 0 \
    --owner-endpoints 0=$NODE0_HOST:$PORT \
    --timeout-ms $CONTROL_TIMEOUT" || { echo "FAIL: initial topology"; exit 1; }

prefill_data

# ============================================================================
# Phase 4-5: 扩容三段采集
# ============================================================================
log "Phase 4-5: Scaleout sweep (baseline / during / after)"
FINAL_ENDPOINTS="$NODE0_HOST:$PORT,$NODE1_HOST:$PORT"

# 段1: baseline (active={0})
log "段1: baseline VEMB read (${TEST_TIME}s)"
read -r b_ut b_st b_si b_rss < <(snap_both)
T0=$(date +%s)
result=$(run_memtier "$NODE0_HOST" "$TEST_TIME" "$RAW_DIR/scaleout_baseline.txt" "")
T1=$(date +%s)
read ops hits p50 p99 <<< "$result"
read -r a_ut a_st a_si a_rss < <(snap_both)
read -r cut sut si rss < <(compute_metrics "$b_ut" "$b_st" "$b_si" "$a_ut" "$a_st" "$a_si" "$a_rss" "$((T1-T0))")
record_phase "scaleout_baseline" "$ops" "$hits" "$p50" "$p99" "$((T1-T0))" "active={0}" "$cut" "$sut" "$si" "$rss"

# 段2: during scaleout (后台 memtier + 触发扩容)
log "段2: during scaleout (background VEMB ${BG_TIME_SCALEOUT}s + topology change)"
read -r d2_b_ut d2_b_st d2_b_si d2_b_rss < <(snap_both)
run_memtier_bg "$NODE0_HOST" "$RAW_DIR/scaleout_during.txt" "$BG_TIME_SCALEOUT" \
    "--vemb-v16-client-topology --vemb-v16-topology-retry-limit=8" \
    "$FINAL_ENDPOINTS"
T0=$(date +%s)

# 启动 coordinator（在 node0 上 setsid -f 后台跑）
ssh_run "$NODE0_HOST" "cd $REMOTE_DIR && setsid -f ./benchmark/vemb_v16_topology_ctl \
    --coordinator-listen --transport tcp --host $NODE0_HOST --port $COORD_PORT \
    --vnode-count $VNODE_COUNT \
    --expected-sources 0 --migration-epoch $MIGRATION_EPOCH --cutover-epoch $CUTOVER_EPOCH \
    --standby 0,1 --owner-endpoints 0=$NODE0_HOST:$PORT,1=$NODE1_HOST:$PORT \
    --wait-ms 60000 --timeout-ms $CONTROL_TIMEOUT >$COORD_OUT 2>$COORD_ERR </dev/null"
sleep 1

# 发布候选拓扑到 node1
ssh_run "$NODE1_HOST" "cd $REMOTE_DIR && ./benchmark/vemb_v16_topology_ctl --set --transport tcp \
    --host $NODE1_HOST --port $PORT --epoch $MIGRATION_EPOCH --min-write-epoch $MIGRATION_EPOCH \
    --vnode-count $VNODE_COUNT \
    --active 0 --standby 0,1 --dual-write --auto-scaleout --coordinated-scaleout \
    --owner-endpoints 0=$NODE0_HOST:$PORT,1=$NODE1_HOST:$PORT \
    --coordinator-endpoint $NODE0_HOST:$COORD_PORT --timeout-ms $CONTROL_TIMEOUT"

# 发布候选拓扑到 node0 (带 peer-view-map)
ssh_run "$NODE0_HOST" "cd $REMOTE_DIR && ./benchmark/vemb_v16_topology_ctl \
    --set-with-peer-view-map $NODE0_PEER_MAP --transport tcp \
    --host $NODE0_HOST --port $PORT --epoch $MIGRATION_EPOCH --min-write-epoch $MIGRATION_EPOCH \
    --vnode-count $VNODE_COUNT \
    --active 0 --standby 0,1 --dual-write --auto-scaleout --coordinated-scaleout \
    --owner-endpoints 0=$NODE0_HOST:$PORT,1=$NODE1_HOST:$PORT \
    --coordinator-endpoint $NODE0_HOST:$COORD_PORT --timeout-ms $COMBINED_TIMEOUT"

# 等扩容完成（最多 BG_TIME_SCALEOUT+60s，给 cutover 充足时间）
SCALEOUT_WAIT=$((BG_TIME_SCALEOUT + 60))
ssh_run "$NODE0_HOST" "for _ in \$(seq 1 $SCALEOUT_WAIT); do \
    if grep -q '^scaleout_all_sources_done=1$' $COORD_OUT 2>/dev/null && \
       grep -q '^scaleout_full_active_published=' $COORD_OUT 2>/dev/null; then exit 0; fi; \
    sleep 1; done; exit 1"
T1=$(date +%s)
SCALEOUT_WALL=$((T1-T0))

# 等后台 memtier 跑完（让它自然结束输出 Totals）
log "等待后台 memtier 自然结束（剩 $((BG_TIME_SCALEOUT - SCALEOUT_WALL))s）"
REMAIN=$((BG_TIME_SCALEOUT - SCALEOUT_WALL))
if [ "$REMAIN" -gt 0 ]; then
    sleep "$REMAIN"
fi
sleep 2  # 给 memtier 输出 Totals 的时间

result=$(parse_bg_out "$NODE0_HOST" "$RAW_DIR/scaleout_during.txt")
read ops hits p50 p99 <<< "$result"
read -r d2_a_ut d2_a_st d2_a_si d2_a_rss < <(snap_both)
read -r cut sut si rss < <(compute_metrics "$d2_b_ut" "$d2_b_st" "$d2_b_si" "$d2_a_ut" "$d2_a_st" "$d2_a_si" "$d2_a_rss" "$SCALEOUT_WALL")
record_phase "during_scaleout" "$ops" "$hits" "${p50:-NA}" "${p99:-NA}" "$SCALEOUT_WALL" "active={0}->{0,1}, memtier-client-topology-retry, endpoints=$FINAL_ENDPOINTS" "$cut" "$sut" "$si" "$rss"

ssh_run "$NODE0_HOST" "cat $COORD_OUT; echo '---'; cat $COORD_ERR 2>/dev/null" || true

# 段3a: after scaleout, direct node0 read original keyspace
log "段3a: after scaleout direct node0 old-key VEMB read (${TEST_TIME}s)"
read -r d3a_b_ut d3a_b_st d3a_b_si d3a_b_rss < <(snap_both)
T0=$(date +%s)
result=$(run_memtier_target "$NODE0_HOST" "$NODE0_HOST" "$TEST_TIME" "$RAW_DIR/scaleout_after_direct_node0_old_keys.txt" \
    "" 1 "$PREFILL_KEYS")
T1=$(date +%s)
read ops hits p50 p99 <<< "$result"
read -r d3a_a_ut d3a_a_st d3a_a_si d3a_a_rss < <(snap_both)
read -r cut sut si rss < <(compute_metrics "$d3a_b_ut" "$d3a_b_st" "$d3a_b_si" "$d3a_a_ut" "$d3a_a_st" "$d3a_a_si" "$d3a_a_rss" "$((T1-T0))")
record_phase "scaleout_after_direct_node0_old_keys" "$ops" "$hits" "$p50" "$p99" "$((T1-T0))" "active={0,1}, direct=$NODE0_HOST:$PORT, old_keys=1-$PREFILL_KEYS" "$cut" "$sut" "$si" "$rss"

# 段3b: after scaleout, direct node1 read original keyspace from node0 client
log "段3b: after scaleout direct node1 old-key VEMB read (${TEST_TIME}s)"
read -r d3b_b_ut d3b_b_st d3b_b_si d3b_b_rss < <(snap_both)
T0=$(date +%s)
result=$(run_memtier_target "$NODE0_HOST" "$NODE1_HOST" "$TEST_TIME" "$RAW_DIR/scaleout_after_direct_node1_old_keys.txt" \
    "" 1 "$PREFILL_KEYS")
T1=$(date +%s)
read ops hits p50 p99 <<< "$result"
read -r d3b_a_ut d3b_a_st d3b_a_si d3b_a_rss < <(snap_both)
read -r cut sut si rss < <(compute_metrics "$d3b_b_ut" "$d3b_b_st" "$d3b_b_si" "$d3b_a_ut" "$d3b_a_st" "$d3b_a_si" "$d3b_a_rss" "$((T1-T0))")
record_phase "scaleout_after_direct_node1_old_keys" "$ops" "$hits" "$p50" "$p99" "$((T1-T0))" "active={0,1}, direct=$NODE1_HOST:$PORT from node0, old_keys=1-$PREFILL_KEYS" "$cut" "$sut" "$si" "$rss"

# 段3c: after scaleout, client-topology read original migrated keyspace
log "段3c: after scaleout client-topology old-key VEMB read (${TEST_TIME}s)"
read -r d3c_b_ut d3c_b_st d3c_b_si d3c_b_rss < <(snap_both)
T0=$(date +%s)
result=$(run_memtier "$NODE0_HOST" "$TEST_TIME" "$RAW_DIR/scaleout_after_old_keys.txt" \
    "--vemb-v16-client-topology" "$FINAL_ENDPOINTS" 1 "$PREFILL_KEYS")
T1=$(date +%s)
read ops hits p50 p99 <<< "$result"
read -r d3c_a_ut d3c_a_st d3c_a_si d3c_a_rss < <(snap_both)
read -r cut sut si rss < <(compute_metrics "$d3c_b_ut" "$d3c_b_st" "$d3c_b_si" "$d3c_a_ut" "$d3c_a_st" "$d3c_a_si" "$d3c_a_rss" "$((T1-T0))")
record_phase "scaleout_after_old_keys" "$ops" "$hits" "$p50" "$p99" "$((T1-T0))" "active={0,1}, memtier-client-topology, old_keys=1-$PREFILL_KEYS, endpoints=$FINAL_ENDPOINTS" "$cut" "$sut" "$si" "$rss"

# 段3d: after scaleout, write/read new steady keyspace (active={0,1})
log "段3d: after scaleout new-key VEMB read (${TEST_TIME}s)"
read -r d3d_b_ut d3d_b_st d3d_b_si d3d_b_rss < <(snap_both)
T0=$(date +%s)
prefill_steady_data "$FINAL_ENDPOINTS" "$STEADY_KEY_MIN" "$STEADY_KEY_MAX" "$RAW_DIR/scaleout_after_prefill.txt"
result=$(run_memtier "$NODE0_HOST" "$TEST_TIME" "$RAW_DIR/scaleout_after.txt" \
    "--vemb-v16-client-topology" "$FINAL_ENDPOINTS" "$STEADY_KEY_MIN" "$STEADY_KEY_MAX")
T1=$(date +%s)
read ops hits p50 p99 <<< "$result"
read -r d3d_a_ut d3d_a_st d3d_a_si d3d_a_rss < <(snap_both)
read -r cut sut si rss < <(compute_metrics "$d3d_b_ut" "$d3d_b_st" "$d3d_b_si" "$d3d_a_ut" "$d3d_a_st" "$d3d_a_si" "$d3d_a_rss" "$((T1-T0))")
record_phase "scaleout_after" "$ops" "$hits" "$p50" "$p99" "$((T1-T0))" "active={0,1}, memtier-client-topology, steady_keys=$STEADY_KEY_MIN-$STEADY_KEY_MAX, endpoints=$FINAL_ENDPOINTS" "$cut" "$sut" "$si" "$rss"

# --- Phase 6: Cleanup ---
log "Phase 6: Cleanup"
stop_memtier_bg || true
stop_node "$NODE0_HOST"
stop_node "$NODE1_HOST"

# Truncate server logs to last ${LOG_TAIL_LINES:-0} lines — vemb_v16
# per-iteration logs during scaleout/baseline_push can otherwise grow to
# GBs and exhaust disk.  Default 0 = keep full log for post-run analysis.
# Set LOG_TAIL_LINES=5000 to restore old truncate behavior.
LOG_TAIL_LINES="${LOG_TAIL_LINES:-500}"
if [ "$LOG_TAIL_LINES" -gt 0 ]; then
    truncate_log() {
        local host=$1 path=$2
        ssh_run "$host" "[ -f $path ] && { tail -${LOG_TAIL_LINES} $path > ${path}.tmp && mv ${path}.tmp $path; }" || true
    }
    truncate_log "$NODE0_HOST" "$NODE0_LOG"
    truncate_log "$NODE1_HOST" "$NODE1_LOG"
fi

log "DONE — $TSV"
cat "$TSV"
