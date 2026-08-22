#!/bin/bash
# Warm region 拓扑扫描：4 种分布 × 2 transport = 8 组
# 分布：纯本地 / 7:1 / 1:1 / 全远端
# transport：TCP / Aeron
# 注：prefill 全部走 TCP（Aeron runner 多 worker 共享 SHM channel 违反 SPSC，VADD 丢 7/8）
set -uo pipefail

HPC=/root/gqs/codespace/UnifiedBus/hpc-redis
MEMTIER=/root/gqs/codespace/UnifiedBus/hpc-redis/memtier_benchmark/memtier_benchmark
MFEST_DIR=${HPC}/examples

PORT=6390
DIM=300
NUM_KEYS=10000
TEST_TIME=30
PIPELINE=32
T=64
C=4
PIO=32
SNW=64

OUTDIR=/tmp/warm_topo_sweep
RAWDIR=${OUTDIR}/raw
mkdir -p ${RAWDIR}

SRV_BIND='numactl --membind=0 taskset -c 0-95'
CLI_BIND='numactl --membind=0 taskset -c 96-191'

log() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }

# 读 server pid 的 utime/stime 拆分（jiffies），输出 "ut st"
get_cpu_jiffies() {
    local pid=$1
    awk '{u+=$14; s+=$15} END{printf "%d %d", u+0, s+0}' /proc/$pid/task/*/stat 2>/dev/null
}
# softirq jiffies 全局快照
snapshot_si() { awk '/^cpu /{print $8}' /proc/stat 2>/dev/null; }
# server pid VmRSS KB
snapshot_rss() { local pid=$1; awk '/^VmRSS:/{print $2}' /proc/$pid/status 2>/dev/null; }

kill_server() {
    local pids=$(pgrep -f "redis-server.*:${PORT}" 2>/dev/null)
    [ -n "$pids" ] && kill -9 $pids 2>/dev/null
    pkill -9 -f "redis-server.*${PORT}" 2>/dev/null
    [ -x /tmp/clear_ub_device ] && /tmp/clear_ub_device >/dev/null 2>&1
    for _ in $(seq 1 30); do
        ss -tln | grep -q ":${PORT} " || break
        sleep 0.3
    done
    rm -f /tmp/vemb_v16.sock
    sleep 0.5
}

start_server() {
    local manifest=$1
    local logfile=$2
    local transport=${3:-tcp}
    kill_server
    cd ${HPC}
    # Aeron transport needs explicit control config so the listener accepts
    # ATTACH frames; plain sniff mode ignores them.
    local aeron_args=""
    [ "${transport}" = "aeron" ] && aeron_args="--vemb-v16-transport aeron \
        --vemb-v16-aeron-control tcp \
        --vemb-v16-aeron-ub-path /dev/obmm_shmdev1 \
        --vemb-v16-aeron-response-ub-path /dev/obmm_shmdev2"
    ${SRV_BIND} ./src/redis-server \
        --port ${PORT} --bind 0.0.0.0 --protected-mode no \
        --vemb-v16-enabled yes --vemb-v16-dim ${DIM} \
        --vemb-v16-max-vectors 1048576 \
        --vemb-v16-warm-regions-manifest ${manifest} \
        --vemb-v16-reset-warm-regions yes \
        ${aeron_args} \
        --vemb-v16-proxy-io-threads ${PIO} --vemb-v16-supernode-workers ${SNW} \
        --save '' --dbfilename '' --logfile ${logfile} --loglevel notice \
        --daemonize yes
    for _ in $(seq 1 50); do ss -tln | grep -q ":${PORT} " && return 0; sleep 0.2; done
    echo "FAIL: server ${PORT}"; exit 1
}

prefill_tcp() {
    local transport=${2:-tcp}
    local arg=""
    [ "${transport}" = "aeron" ] && arg="--vemb-v16-transport=aeron"
    ${CLI_BIND} ${MEMTIER} --protocol vemb_v16 --vemb-v16-dim ${DIM} ${arg} \
        -s 127.0.0.1 -p ${PORT} -t 8 -c 4 --pipeline=${PIPELINE} -n ${NUM_KEYS} \
        --ratio=1:0 --key-pattern=S:S --key-prefix=item: \
        --key-minimum=1 --key-maximum=${NUM_KEYS} >$1 2>&1
}

run_read() {
    local transport=$1
    local out=$2
    local arg=""
    [ "${transport}" = "aeron" ] && arg="--vemb-v16-transport=aeron"
    ${CLI_BIND} ${MEMTIER} --protocol vemb_v16 --vemb-v16-dim ${DIM} ${arg} \
        -s 127.0.0.1 -p ${PORT} -t ${T} -c ${C} --pipeline=${PIPELINE} \
        --ratio=0:1 --key-pattern=R:R --key-prefix=item: \
        --key-minimum=1 --key-maximum=${NUM_KEYS} --test-time=${TEST_TIME} >${out} 2>&1
}

# topology matrix: name | manifest | expected_local_pct
declare -a TOPOS=(
    "local|${MFEST_DIR}/vemb_v16_warm_regions_111.yaml|100"
    "7to1|${MFEST_DIR}/vemb_v16_warm_regions_111_mix7to1.yaml|87.5"
    "1to1|${MFEST_DIR}/vemb_v16_warm_regions_111_mix1to1.yaml|50"
    "allremote|${MFEST_DIR}/vemb_v16_warm_regions_111_allremote.yaml|0"
)

for topo_entry in "${TOPOS[@]}"; do
    IFS='|' read -r name manifest pct <<< "$topo_entry"
    for transport in tcp aeron; do
        tag="${name}_${transport}"
        log "Test: ${tag} (expected_local=${pct}%)"
        start_server ${manifest} ${RAWDIR}/server_${tag}.log ${transport}
        sleep 2
        prefill_tcp ${RAWDIR}/prefill_${tag}.log ${transport}
        # ── bench 前 server pid + 快照 ──
        SRV_PID=$(pgrep -f "redis-server.*:${PORT}" 2>/dev/null | head -1)
        if [ -n "$SRV_PID" ]; then
            read J0_UT J0_ST < <(get_cpu_jiffies "$SRV_PID")
J0_NS=$(date +%s%N)
        else
            log "WARN: no redis-server on :${PORT}, jiffies/rss will be 0"
            J0_UT=0; J0_ST=0
        fi
        J0_SI=$(snapshot_si)
        run_read ${transport} ${RAWDIR}/${tag}.txt
        # ── bench 后快照 ──
        if [ -n "$SRV_PID" ]; then
            read J1_UT J1_ST < <(get_cpu_jiffies "$SRV_PID")
J1_NS=$(date +%s%N)
ELAPSED_NS=$((J1_NS - J0_NS > 0 ? J1_NS - J0_NS : TEST_TIME * 1000000000))
# core 分母 = 纳秒实测窗口 (与脚本13对齐)
        else
            J1_UT=0; J1_ST=0
        fi
        J1_SI=$(snapshot_si)
        RSS=$(snapshot_rss "$SRV_PID")
        kill_server
        # ── 计算并落地每个 tag 的 4 指标 ──
        CORE_UT=$(awk -v d=$((J1_UT - J0_UT)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
        CORE_ST=$(awk -v d=$((J1_ST - J0_ST)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
        C_SI=$(awk -v d=$((J1_SI - J0_SI)) -v s=$ELAPSED_NS 'BEGIN{printf "%.2f", d/100.0/(s/1000000000)}')
        [ -z "$RSS" ] && RSS="NA"
        printf '%s %s %s %s\n' "$CORE_UT" "$CORE_ST" "$C_SI" "$RSS" > ${RAWDIR}/metrics_${tag}.txt
    done
done

log 'DONE — results:'
echo
printf '%-20s | %-8s | %s\n' 'topology' 'transp' 'ops/sec       Pavg     P50      P99      hits    misses  core_ut  core_st  si       rss'
printf '%-20s-+-%s-+-%s\n' '--------------------' '--------' '------------------------------------------------------------------------------------------'
for name in local 7to1 1to1 allremote; do
    for transport in tcp aeron; do
        tag="${name}_${transport}"
        t=$(grep '^Totals' ${RAWDIR}/${tag}.txt | tail -1)
        ops=$(echo "$t" | awk '{print $2}')
        hits=$(echo "$t" | awk '{print $3}')
        miss=$(echo "$t" | awk '{print $4}')
        pavg=$(echo "$t" | awk '{print $5}')
        p50=$(echo "$t" | awk '{print $6}')
        p99=$(echo "$t" | awk '{print $7}')
        read CORE_UT CORE_ST C_SI RSS < ${RAWDIR}/metrics_${tag}.txt
        printf '%-20s | %-8s | %s  %s  %s  %s  %s  %s  %s  %s  %s  %s\n' \
            "$name" "$transport" "$ops" "$pavg" "$p50" "$p99" "$hits" "$miss" \
            "$CORE_UT" "$CORE_ST" "$C_SI" "$RSS"
    done
done
