#!/usr/bin/env bash
# Static two-owner Aeron/UB cluster throughput runner (no scaleout).
#
# 与 vemb_v16_scaleout_ub_cluster_111_to_112.sh 同一套 server/manifest/peer-view
# 部署，但直接把拓扑设为 active={0,1} 稳态后压测吞吐——不含 coordinator、
# 迁移、cutover。用于回答"纯 aeron cluster 模式吞吐多少"。
#
# 在本地 Mac 执行（管理面走外网转发口，数据面走 111/112 内网）:
#   REMOTE_DIR=/root/gqs/codespace/UnifiedBus/hpc-redis \
#     bash benchmark/vemb_v16_aeron_cluster_tput.sh
# 默认 9 档矩阵 (1,1,1)...(64,64,32); 单档: SWEEP=0 TS=64 CS=4 PS=32 TEST_TIME=30
# OP_TYPE=VSIM_2KEY 跑双 key 余弦对比 (报告 3.2.7 HPC Aeron)
set -euo pipefail

NODE0_HOST="${NODE0_HOST:-192.168.90.111}"
NODE1_HOST="${NODE1_HOST:-192.168.90.112}"
NODE0_SSH_HOST="${NODE0_SSH_HOST:-43.154.145.18}"
NODE0_SSH_PORT="${NODE0_SSH_PORT:-8111}"
NODE1_SSH_HOST="${NODE1_SSH_HOST:-43.154.145.18}"
NODE1_SSH_PORT="${NODE1_SSH_PORT:-8112}"
SSH_USER="${SSH_USER:-root}"
REMOTE_DIR="${REMOTE_DIR:-/root/gqs/codespace/UnifiedBus/hpc-redis}"
MEMTIER="$REMOTE_DIR/memtier_benchmark/memtier_benchmark"

SERVER_PORT="${SERVER_PORT:-6397}"
DIM="${DIM:-300}"
MAX_VECTORS="${MAX_VECTORS:-65536}"
NUM_KEYS="${NUM_KEYS:-100000}"
TEST_TIME="${TEST_TIME:-30}"
PIPELINE="${PIPELINE:-32}"
BATCH_MAX_DELAY_US="${BATCH_MAX_DELAY_US:-10}"
MEMTIER_T="${MEMTIER_T:-64}"
MEMTIER_C="${MEMTIER_C:-4}"
PIO="${PIO:-7}"
SNW="${SNW:-7}"
VNODE_COUNT="${VNODE_COUNT:-100}"
CONTROL_TIMEOUT_MS="${CONTROL_TIMEOUT_MS:-5000}"
KEEP_SERVERS="${KEEP_SERVERS:-0}"
ARCHIVE_LOCAL="${ARCHIVE_LOCAL:-1}"
RUN_ID="${RUN_ID:-aeron_cluster_tput_$(date +%Y%m%d_%H%M%S)}"

# UB 布局与 scaleout 脚本一致 (node0 dev1-4 / node1 dev9-12, 详见 examples yaml)
USE_EXTERNAL_UB_CONFIG="${USE_EXTERNAL_UB_CONFIG:-1}"
UB_CONFIG_DIR="${UB_CONFIG_DIR:-$REMOTE_DIR/examples}"
NODE0_AERON_REQUEST_PATH="${NODE0_AERON_REQUEST_PATH:-/dev/obmm_shmdev1}"
NODE0_AERON_RESPONSE_PATH="${NODE0_AERON_RESPONSE_PATH:-/dev/obmm_shmdev2}"
NODE1_AERON_REQUEST_PATH="${NODE1_AERON_REQUEST_PATH:-/dev/obmm_shmdev10}"
NODE1_AERON_RESPONSE_PATH="${NODE1_AERON_RESPONSE_PATH:-/dev/obmm_shmdev11}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
LOCAL_RESULT_DIR="${LOCAL_RESULT_DIR:-$ROOT_DIR/benchmark/results/aeron_cluster/$RUN_ID}"
REMOTE_SUBDIR="benchmark/results/aeron_cluster/$RUN_ID"
REMOTE_RESULT_DIR="$REMOTE_DIR/$REMOTE_SUBDIR"

NODE0_MANIFEST="$REMOTE_RESULT_DIR/node0_server.yaml"
NODE1_MANIFEST="$REMOTE_RESULT_DIR/node1_server.yaml"
NODE0_PEER_MAP="$REMOTE_RESULT_DIR/node0_server_peer_map.yaml"
CLIENT_PEER_MANIFEST="$REMOTE_RESULT_DIR/ub_peer_view_111_to_112.yaml"
NODE0_LOG="$REMOTE_RESULT_DIR/server_node0.log"
NODE1_LOG="$REMOTE_RESULT_DIR/server_node1.log"
NODE0_PID_FILE="$REMOTE_RESULT_DIR/server_node0.pid"
NODE1_PID_FILE="$REMOTE_RESULT_DIR/server_node1.pid"

step() { printf '\n===== %s =====\n' "$*"; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

ssh_run() {
    local node="$1" host port
    shift
    case "$node" in
        "$NODE0_HOST") host="$NODE0_SSH_HOST"; port="$NODE0_SSH_PORT" ;;
        "$NODE1_HOST") host="$NODE1_SSH_HOST"; port="$NODE1_SSH_PORT" ;;
        *) die "unknown cluster node: $node" ;;
    esac
    ssh -p "$port" "${SSH_USER}@${host}" "$@"
}

remote_stop_pid() {
    ssh_run "$1" "if test -s '$2'; then pid=\$(cat '$2'); if kill -0 \$pid 2>/dev/null; then kill \$pid; fi; fi" || true
}
cleanup() {
    local status=$?
    if [ "$KEEP_SERVERS" = "0" ]; then
        remote_stop_pid "$NODE0_HOST" "$NODE0_PID_FILE"
        remote_stop_pid "$NODE1_HOST" "$NODE1_PID_FILE"
    fi
    if [ "$status" -ne 0 ]; then
        printf 'FAILED; remote artifacts remain in %s\n' "$REMOTE_RESULT_DIR" >&2
    fi
}
trap cleanup EXIT

# ── P1: preflight ──────────────────────────────────────────────────────────
step "P1: preflight (stamps + ports)"
ssh_run "$NODE0_HOST" "mkdir -p '$REMOTE_RESULT_DIR'"
ssh_run "$NODE1_HOST" "mkdir -p '$REMOTE_RESULT_DIR'"
for node in "$NODE0_HOST" "$NODE1_HOST"; do
    ssh_run "$node" "cd '$REMOTE_DIR' && bash scripts/vemb_v16_build_stamp.sh verify all"
done
for node in "$NODE0_HOST" "$NODE1_HOST"; do
    ssh_run "$node" "! ss -H -ltn | awk '\$4 ~ /:${SERVER_PORT}\$/ { found = 1 } END { exit found ? 0 : 1 }'"
done

# ── P2: manifests ──────────────────────────────────────────────────────────
step "P2: install manifests + client peer-view"
if [ "$USE_EXTERNAL_UB_CONFIG" = "1" ]; then
    # 高维用对应 dim 变体 manifest (value_size=dim*4, 4GB region); 300 维用原版
    DIM_SUFFIX=""
    case "$DIM" in
        300) ;;
        *) DIM_SUFFIX="_dim${DIM}"
           for n in node0 node1; do
               [ -f "$ROOT_DIR/examples/vemb_v16_ub_cluster_111_to_112_${n}${DIM_SUFFIX}.yaml" ] ||                    die "missing dim manifest: vemb_v16_ub_cluster_111_to_112_${n}${DIM_SUFFIX}.yaml"
           done ;;
    esac
    ssh_run "$NODE0_HOST" "cp '$UB_CONFIG_DIR/vemb_v16_ub_cluster_111_to_112_node0${DIM_SUFFIX}.yaml' '$NODE0_MANIFEST' && cp '$UB_CONFIG_DIR/vemb_v16_ub_cluster_111_to_112_node0_peer_map.yaml' '$NODE0_PEER_MAP'"
    ssh_run "$NODE1_HOST" "cp '$UB_CONFIG_DIR/vemb_v16_ub_cluster_111_to_112_node1${DIM_SUFFIX}.yaml' '$NODE1_MANIFEST'"
else
    die "fallback manifest generation not supported; keep USE_EXTERNAL_UB_CONFIG=1"
fi
# client (111) 视图: owner0 本地视图 + examples 里 client_host=111 的 owner1 转换视图
ssh_run "$NODE0_HOST" "tmp='$CLIENT_PEER_MANIFEST.tmp'; cat >\"\$tmp\" <<'YAML'
version: 1
peer_views:
  - client_host: 111
    owner_id: 0
    resource_role: v1_request_ring
    resource_id: owner111-v1-request
    generation: 1
    provider_path: $NODE0_AERON_REQUEST_PATH
    client_path: $NODE0_AERON_REQUEST_PATH
    map_from_start: true
  - client_host: 111
    owner_id: 0
    resource_role: v1_response_ring
    resource_id: owner111-v1-response
    generation: 1
    provider_path: $NODE0_AERON_RESPONSE_PATH
    client_path: $NODE0_AERON_RESPONSE_PATH
    map_from_start: true
  - client_host: 111
    owner_id: 0
    resource_role: warm_region
    resource_id: owner111-warm
    generation: 1
    provider_path: /dev/obmm_shmdev3
    client_path: /dev/obmm_shmdev3
    map_from_start: true
  - client_host: 111
    owner_id: 0
    resource_role: v2_request_descriptor
    resource_id: owner111-v2-request-descriptor
    generation: 1
    provider_path: $NODE0_AERON_REQUEST_PATH
    client_path: $NODE0_AERON_REQUEST_PATH
    map_from_start: true
  - client_host: 111
    owner_id: 0
    resource_role: v2_request_arena
    resource_id: owner111-v2-request-arena
    generation: 1
    provider_path: $NODE0_AERON_REQUEST_PATH
    client_path: $NODE0_AERON_REQUEST_PATH
    map_from_start: true
  - client_host: 111
    owner_id: 0
    resource_role: v2_response_descriptor
    resource_id: owner111-v2-response-descriptor
    generation: 1
    provider_path: $NODE0_AERON_RESPONSE_PATH
    client_path: $NODE0_AERON_RESPONSE_PATH
    map_from_start: true
  - client_host: 111
    owner_id: 0
    resource_role: v2_response_arena
    resource_id: owner111-v2-response-arena
    generation: 1
    provider_path: $NODE0_AERON_RESPONSE_PATH
    client_path: $NODE0_AERON_RESPONSE_PATH
    map_from_start: true
YAML
sed -n '/^  - client_host: 111$/,\$p' '$REMOTE_DIR/examples/vemb_v16_ub_peer_view_111_to_112.yaml' >>\"\$tmp\"; mv \"\$tmp\" '$CLIENT_PEER_MANIFEST'"

# ── P3: start servers ──────────────────────────────────────────────────────
start_server() {
    local node="$1" manifest="$2" logfile="$3" pid_file="$4" req="$5" resp="$6"
    ssh_run "$node" "cd '$REMOTE_DIR' && rm -f '$pid_file' '$logfile' && numactl --membind=0 taskset -c 0-95 ./src/redis-server --port '$SERVER_PORT' --bind '$node' --protected-mode no --vemb-v16-enabled yes --vemb-v16-dim '$DIM' --vemb-v16-max-vectors '$MAX_VECTORS' --vemb-v16-warm-regions-manifest '$manifest' --vemb-v16-reset-warm-regions yes --vemb-v16-transport aeron --vemb-v16-aeron-ub-path '$req' --vemb-v16-aeron-response-ub-path '$resp' --vemb-v16-proxy-io-threads '$PIO' --vemb-v16-supernode-workers '$SNW' --vemb-v16-batch-request-size '$PIPELINE' --daemonize yes --pidfile '$pid_file' --logfile '$logfile' --loglevel warning"
}
wait_server_ready() {
    ssh_run "$1" "cd '$REMOTE_DIR' || exit 1; for _ in \$(seq 1 100); do if ./benchmark/vemb_v16_topology_ctl --get --ctl-endpoint tcp --host '$1' --port '$SERVER_PORT' --timeout-ms 1000 >/dev/null 2>&1; then exit 0; fi; if ! kill -0 \$(cat '$2') 2>/dev/null; then exit 1; fi; sleep 1; done; exit 1"
}

step "P3: start owner0 + owner1 (aeron)"
start_server "$NODE0_HOST" "$NODE0_MANIFEST" "$NODE0_LOG" "$NODE0_PID_FILE" \
    "$NODE0_AERON_REQUEST_PATH" "$NODE0_AERON_RESPONSE_PATH"
start_server "$NODE1_HOST" "$NODE1_MANIFEST" "$NODE1_LOG" "$NODE1_PID_FILE" \
    "$NODE1_AERON_REQUEST_PATH" "$NODE1_AERON_RESPONSE_PATH"
wait_server_ready "$NODE0_HOST" "$NODE0_PID_FILE" || die "owner0 not ready"
wait_server_ready "$NODE1_HOST" "$NODE1_PID_FILE" || die "owner1 not ready"

# ── P4: static topology active={0,1} ──────────────────────────────────────
step "P4: set static topology active={0,1}"
EPOCH=$(( $(date +%s) + 100 ))
OWNER_ENDPOINTS="0=$NODE0_HOST:$SERVER_PORT,1=$NODE1_HOST:$SERVER_PORT"
# node1 先设, node0 带 peer-view-map 后设 (对齐 scaleout 顺序, 避免写路由缺口)
ssh_run "$NODE1_HOST" "cd '$REMOTE_DIR' && ./benchmark/vemb_v16_topology_ctl --set --ctl-endpoint tcp --data-endpoint aeron --host '$NODE1_HOST' --port '$SERVER_PORT' --epoch '$EPOCH' --min-write-epoch '$EPOCH' --vnode-count '$VNODE_COUNT' --active 0,1 --standby 0,1 --owner-endpoints '$OWNER_ENDPOINTS' --timeout-ms '$CONTROL_TIMEOUT_MS' >'$REMOTE_RESULT_DIR/topology_node1.out'"
ssh_run "$NODE0_HOST" "cd '$REMOTE_DIR' && ./benchmark/vemb_v16_topology_ctl --set-with-peer-view-map '$NODE0_PEER_MAP' --ctl-endpoint tcp --data-endpoint aeron --host '$NODE0_HOST' --port '$SERVER_PORT' --epoch '$EPOCH' --min-write-epoch '$EPOCH' --vnode-count '$VNODE_COUNT' --active 0,1 --standby 0,1 --owner-endpoints '$OWNER_ENDPOINTS' --timeout-ms 60000 >'$REMOTE_RESULT_DIR/topology_node0.out'"

# ── P5: prefill (双 owner 分片写入) ────────────────────────────────────────
step "P5: prefill $NUM_KEYS keys via both owners"
PREFILL_OUT="$REMOTE_RESULT_DIR/prefill.out"
ssh_run "$NODE0_HOST" "cd '$REMOTE_DIR' && numactl --membind=1 taskset -c 96-191 '$MEMTIER' --protocol=vemb_v16 --vemb-v16-dim='$DIM' --vemb-v16-handle --vemb-v16-transport=aeron --vemb-v16-ub-peer-view-manifest='$CLIENT_PEER_MANIFEST' --vemb-v16-ub-peer-view-client-host=111 --vemb-v16-ub-peer-view-owner-id=1 --vemb-v16-endpoints='$NODE0_HOST:$SERVER_PORT,$NODE1_HOST:$SERVER_PORT' --threads=1 --clients=1 --pipeline=32 --requests='$NUM_KEYS' --ratio=1:0 --key-pattern=S:S --key-prefix=item: --key-minimum=1 --key-maximum='$NUM_KEYS' >'$PREFILL_OUT' 2>&1"
ssh_run "$NODE0_HOST" "grep -q '^Totals' '$PREFILL_OUT' && ! grep -Eq 'status_(nf|err)=[1-9]|materialized_fail=[1-9]|unmatched=[1-9]' '$PREFILL_OUT'" || die "prefill failed correctness checks: $PREFILL_OUT"

# ── P6: bench (9 档矩阵 × OP_TYPE) ─────────────────────────────────────────
# 9 档精简矩阵 (与 TCP sweep 脚本一致); SWEEP=0 时用单档 (TS/CS/PS, 默认取
# MEMTIER_T/MEMTIER_C/PIPELINE 兼容旧用法); SWEEP=1 (默认) 忽略单值跑 9 档,
# 除非显式传了多值 TS/CS/PS (空格分隔, 三数组等长)。
TS=( ${TS:-} )
CS=( ${CS:-} )
PS=( ${PS:-} )
SWEEP=${SWEEP:-1}
if [ "$SWEEP" = "1" ] && [ "${#TS[@]}" -le 1 ] && [ "${#CS[@]}" -le 1 ] && [ "${#PS[@]}" -le 1 ]; then
    TS=( 1  1  4 16 64 64 64 32 64)
    CS=( 1  1  1  1  1  4 16 32 64)
    PS=( 1 32 32 32 32 32 32 32 32)
fi
[ "${#TS[@]}" -ge 1 ] || TS=( ${MEMTIER_T:-64} )
[ "${#CS[@]}" -ge 1 ] || CS=( ${MEMTIER_C:-4} )
[ "${#PS[@]}" -ge 1 ] || PS=( ${PIPELINE:-32} )
NCONFIGS=${#TS[@]}
[ "${#CS[@]}" -eq "$NCONFIGS" ] && [ "${#PS[@]}" -eq "$NCONFIGS" ] \
    || die "TS/CS/PS length mismatch (TS=${#TS[@]} CS=${#CS[@]} PS=${#PS[@]})"

# OP_TYPE: VEMB(默认读) / VSIM_2KEY (双 key 余弦, 报告 3.2.7)
OP_TYPE=${OP_TYPE:-VEMB}
OP_FLAG=""
case "$OP_TYPE" in
    VEMB) ;;
    VSIM_2KEY) OP_FLAG="--vemb-v16-vsim-key-key" ;;
    *) die "OP_TYPE must be VEMB or VSIM_2KEY (got: $OP_TYPE)" ;;
esac

# 双节点 server CPU/RSS 快照 (ut/st jiffies 求和 + 全机 si + VmRSS KB)
# 注意: 用 ssh -n 直连 — ssh_run 无 -n, 在 read < <(...) 的进程替换里 ssh 会吃
# stdin/管道, 导致 read EOF 触发 set -e 退出
snapshot_ssh() { # node cmd
    local node="$1" host port
    case "$node" in
        "$NODE0_HOST") host="$NODE0_SSH_HOST"; port="$NODE0_SSH_PORT" ;;
        "$NODE1_HOST") host="$NODE1_SSH_HOST"; port="$NODE1_SSH_PORT" ;;
        *) die "unknown cluster node: $node" ;;
    esac
    ssh -n -p "$port" "${SSH_USER}@$host" "$2" 2>/dev/null
}
cluster_cpu_snapshot() { # -> "ut st si" (快照异常输出 NA)
    local n0 n1
    n0=$(snapshot_ssh "$NODE0_HOST" "awk '{u+=\$14; s+=\$15} END{print u+0, s+0}' /proc/\$(cat '$NODE0_PID_FILE')/task/*/stat 2>/dev/null; awk '/^cpu /{print \$8}' /proc/stat" | tr '\n' ' ')
    n1=$(snapshot_ssh "$NODE1_HOST" "awk '{u+=\$14; s+=\$15} END{print u+0, s+0}' /proc/\$(cat '$NODE1_PID_FILE')/task/*/stat 2>/dev/null; awk '/^cpu /{print \$8}' /proc/stat" | tr '\n' ' ')
    awk -v a="$n0" -v b="$n1" 'BEGIN{
        if (a !~ /^[0-9]+ [0-9]+ [0-9]+ $/ || b !~ /^[0-9]+ [0-9]+ [0-9]+ $/) { print "NA NA NA"; exit }
        split(a,x," "); split(b,y," "); printf "%d %d %d\n", x[1]+y[1], x[2]+y[2], x[3]+y[3]
    }'
}
cluster_rss_snapshot() { # KB, 两节点之和
    local r0 r1
    r0=$(snapshot_ssh "$NODE0_HOST" "awk '/^VmRSS:/{print \$2}' /proc/\$(cat '$NODE0_PID_FILE')/status 2>/dev/null")
    r1=$(snapshot_ssh "$NODE1_HOST" "awk '/^VmRSS:/{print \$2}' /proc/\$(cat '$NODE1_PID_FILE')/status 2>/dev/null")
    echo $(( ${r0:-0} + ${r1:-0} ))
}

step "P6: steady-state cluster op=$OP_TYPE ${NCONFIGS} 档 × ${TEST_TIME}s"
mkdir -p "$LOCAL_RESULT_DIR"
printf 'op\tt\tc\tpipeline\tops_sec\tp50_ms\tp99_ms\twall_s\tcores\tcore_ut\tcore_st\tsi\trss_kb\tnote\n' >"$LOCAL_RESULT_DIR/summary.tsv"

for ((idx=0; idx<NCONFIGS; idx++)); do
    t=${TS[$idx]}; c=${CS[$idx]}; p=${PS[$idx]}
    BENCH_OUT="$REMOTE_RESULT_DIR/cluster_tput_t${t}_c${c}_p${p}.out"
    read B_UT B_ST B_SI < <(cluster_cpu_snapshot)
    T0=$(date +%s%N)
    ssh_run "$NODE0_HOST" "cd '$REMOTE_DIR' && numactl --membind=1 taskset -c 96-191 '$MEMTIER' --protocol=vemb_v16 --vemb-v16-dim='$DIM' --vemb-v16-handle --vemb-v16-transport=aeron --vemb-v16-ub-peer-view-manifest='$CLIENT_PEER_MANIFEST' --vemb-v16-ub-peer-view-client-host=111 --vemb-v16-ub-peer-view-owner-id=1 --vemb-v16-batch-request-size='$p' --vemb-v16-batch-max-delay-us='$BATCH_MAX_DELAY_US' $OP_FLAG --vemb-v16-endpoints='$NODE0_HOST:$SERVER_PORT' --threads='$t' --clients='$c' --pipeline='$p' --ratio=0:1 --key-pattern=R:R --key-prefix=item: --key-minimum=1 --key-maximum='$NUM_KEYS' --test-time='$TEST_TIME' >'$BENCH_OUT' 2>&1"
    T1=$(date +%s%N)
    read A_UT A_ST A_SI < <(cluster_cpu_snapshot)
    RSS=$(cluster_rss_snapshot)
    ELAPSED_NS=$(( T1 - T0 ))
    [ "$ELAPSED_NS" -le 0 ] && ELAPSED_NS=$(( TEST_TIME * 1000000000 ))
    # 快照异常 (pid 消失/负增量) 时 CPU 列记 NA, 不让脏数据进 TSV
    if [ "$B_UT" = "NA" ] || [ "$A_UT" = "NA" ] || [ "$A_UT" -lt "$B_UT" ] || [ "$A_ST" -lt "$B_ST" ] || [ "$A_SI" -lt "$B_SI" ]; then
        CORES="NA"; CORE_UT="NA"; CORE_ST="NA"; C_SI="NA"
    else
    CORES=$(awk -v du=$((A_UT-B_UT)) -v ds=$((A_ST-B_ST)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", (du+ds)/100.0/(s/1000000000)}')
    CORE_UT=$(awk -v d=$((A_UT-B_UT)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
    CORE_ST=$(awk -v d=$((A_ST-B_ST)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
    C_SI=$(awk -v d=$((A_SI-B_SI)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
    fi

    ssh_run "$NODE0_HOST" "grep -q '^Totals' '$BENCH_OUT' && ! grep -Eq 'status_(nf|err)=[1-9]|materialized_fail=[1-9]|unmatched=[1-9]' '$BENCH_OUT'" \
        || die "bench failed correctness checks: $BENCH_OUT"
    TOTALS=$(ssh_run "$NODE0_HOST" "grep '^Totals' '$BENCH_OUT' | tail -1")
    OPS=$(awk '{print $2}' <<<"$TOTALS")
    P50=$(awk '{print $(NF-3)}' <<<"$TOTALS")
    P99=$(awk '{print $(NF-2)}' <<<"$TOTALS")
    WALL=$(( ELAPSED_NS / 1000000000 ))

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\tactive={0,1}, endpoints=%s:%s\n' \
        "$OP_TYPE" "$t" "$c" "$p" "$OPS" "$P50" "$P99" "$WALL" \
        "$CORES" "$CORE_UT" "$CORE_ST" "$C_SI" "$RSS" "$NODE0_HOST" "$SERVER_PORT" \
        >>"$LOCAL_RESULT_DIR/summary.tsv"
    step "  [$((idx+1))/${NCONFIGS}] t=$t c=$c p=$p: ops=$OPS p50=$P50 p99=$P99 cores=$CORES(ut=$CORE_UT st=$CORE_ST) si=$C_SI rss=${RSS}KB"
done
step "RESULT: 见 $LOCAL_RESULT_DIR/summary.tsv"

# ── P7: archive (压缩传输, 避免外网长连接裸流假死) ─────────────────────────
if [ "$ARCHIVE_LOCAL" = "1" ]; then
    step "P7: archive artifacts (compressed)"
    for entry in "node0:$NODE0_SSH_HOST:$NODE0_SSH_PORT" "node1:$NODE1_SSH_HOST:$NODE1_SSH_PORT"; do
        name=${entry%%:*}
        rest=${entry#*:}
        host=${rest%%:*}
        port=${rest##*:}
        mkdir -p "$LOCAL_RESULT_DIR/$name"
        ssh -p "$port" "${SSH_USER}@$host" "cd '$REMOTE_RESULT_DIR' && tar czf - ." | tar xzf - -C "$LOCAL_RESULT_DIR/$name"
    done
fi

step "PASS — $LOCAL_RESULT_DIR"
cat "$LOCAL_RESULT_DIR/summary.tsv"
