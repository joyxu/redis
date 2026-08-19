#!/bin/bash
# bench_transport_matrix.sh — 4 拓扑 × 2 transport = 8 组 memtier 基准
#
# 用法: bash bench_transport_matrix.sh <manifest_name> <transport> [num_keys] [test_time]
#   manifest_name: pure_local_10k | mix_7to1_10k | mix_1to1_10k | allremote_10k
#   transport:     tcp | aeron
#   num_keys:      默认 10000
#   test_time:     默认 30
#
# 配置: server pio=21 snw=21 mask=0-47 / client t=64 c=4 pipeline=32 mask=96-191
set -uo pipefail

HPC=/root/gqs/codespace/UnifiedBus/hpc-redis
REDIS=$HPC/src/redis-server
MEMTIER=$HPC/memtier_benchmark/memtier_benchmark

MANIFEST_NAME=${1:-mix_7to1_10k}
TRANSPORT=${2:-aeron}
NUM_KEYS=${3:-10000}
TEST_TIME=${4:-30}
MANIFEST=$HPC/examples/vemb_v16_warm_regions_${MANIFEST_NAME}.yaml

PORT=6395
DIM=300
MAX_VECTORS=131072
PIPELINE=32
KEY_PREFIX="item:"

SERVER_MASK="0-47"
CLIENT_MASK="96-191"
T=64; C=4

SOCKET=/tmp/vemb_v16.sock
PIDFILE=/tmp/vemb_bench_${MANIFEST_NAME}_${TRANSPORT}.pid
SERVER_LOG=/tmp/vemb_bench_${MANIFEST_NAME}_${TRANSPORT}.server.log
BENCH_OUT=/tmp/vemb_bench_${MANIFEST_NAME}_${TRANSPORT}.stdout
BENCH_ERR=/tmp/vemb_bench_${MANIFEST_NAME}_${TRANSPORT}.stderr

# 读取 server pid 的 utime/stime 拆分（jiffies），输出 "ut st"
get_cpu_jiffies() {
    local pid=$1
    awk '{u+=$14; s+=$15} END{printf "%d %d", u+0, s+0}' /proc/$pid/task/*/stat 2>/dev/null
}

# softirq jiffies 全局快照
snapshot_si() { awk '/^cpu /{print $8}' /proc/stat 2>/dev/null; }

cleanup() {
    if [ -f "$PIDFILE" ]; then
        local p=$(cat "$PIDFILE" 2>/dev/null)
        [ -n "$p" ] && { kill "$p" 2>/dev/null; sleep 0.5; kill -9 "$p" 2>/dev/null; }
        rm -f "$PIDFILE"
    fi
    pkill -9 -f "redis-server.*:$PORT " 2>/dev/null || true
    rm -f "$SOCKET"
}
trap cleanup EXIT
cleanup
sleep 1

echo "============================================================"
echo "  Manifest : $MANIFEST_NAME"
echo "  Transport: $TRANSPORT"
echo "  Keys: $NUM_KEYS  Dim: $DIM  Test time: ${TEST_TIME}s"
echo "  Server: pio=21 snw=21 mask=$SERVER_MASK"
echo "  Client: t=$T c=$C pipeline=$PIPELINE mask=$CLIENT_MASK"
echo "============================================================"

# ── 启动 server ──
echo ">>> starting server..."
# Aeron transport requires the explicit transport/aeron-control config so the
# Redis listener accepts ATTACH frames (plain sniff mode ignores them).
AERON_ARGS=""
if [ "$TRANSPORT" = "aeron" ]; then
    AERON_ARGS="--vemb-v16-transport aeron --vemb-v16-aeron-control tcp \
        --vemb-v16-aeron-ub-path /dev/obmm_shmdev1 \
        --vemb-v16-aeron-response-ub-path /dev/obmm_shmdev2"
fi
taskset -c "$SERVER_MASK" $REDIS \
    --port $PORT --bind 0.0.0.0 --protected-mode no \
    --vemb-v16-enabled yes --vemb-v16-dim $DIM \
    --vemb-v16-max-vectors $MAX_VECTORS \
    --vemb-v16-warm-regions-manifest "$MANIFEST" \
    --vemb-v16-reset-warm-regions yes \
    $AERON_ARGS \
    --vemb-v16-proxy-io-threads 21 \
    --vemb-v16-supernode-workers 21 \
    --daemonize yes --pidfile $PIDFILE --logfile "$SERVER_LOG" --loglevel notice \
    >/dev/null 2>&1

for _ in $(seq 1 50); do [ -S "$SOCKET" ] && break; sleep 0.2; done
for _ in $(seq 1 50); do ss -tln | grep -q ":$PORT " && break; sleep 0.2; done
if ! ss -tln | grep -q ":$PORT "; then
    echo "FAIL: server port not ready"; tail -20 "$SERVER_LOG"; exit 1
fi
echo "    server up: pid=$(cat $PIDFILE)"
sleep 1

# ── prefill (single-thread sequential, same transport as the benchmark;
#    aeron-control servers only accept ATTACH frames on the TCP port, so a
#    plain vemb_v16 TCP prefill would stall when TRANSPORT=aeron) ──
echo ">>> prefilling $NUM_KEYS keys via ${TRANSPORT}..."
PREFILL_OUT=$(taskset -c "$CLIENT_MASK" $MEMTIER \
    --protocol vemb_v16 --vemb-v16-transport=${TRANSPORT} \
    --vemb-v16-dim $DIM -s 127.0.0.1 -p $PORT \
    -t 1 -c 1 -n $NUM_KEYS --pipeline=32 \
    --ratio=1:0 --key-pattern=S:S \
    --key-prefix=$KEY_PREFIX --key-minimum=1 --key-maximum=$NUM_KEYS \
    2>&1)
PREFILL_OPS=$(echo "$PREFILL_OUT" | grep "^Totals" | tail -1 | awk '{print $2}')
echo "    prefill done: ops=$PREFILL_OPS"
sleep 1

# ── 验证 warm region 分布：扫描每个 region 的 slot state 统计 READY 数量 ──
echo ">>> verifying warm region distribution..."
python3 - "$MANIFEST" <<'PYEOF'
import sys, os, mmap, struct, re

manifest = sys.argv[1]
try:
    with open(manifest) as f:
        text = f.read()
except OSError as e:
    print(f"  WARN: cannot read manifest {manifest}: {e}")
    sys.exit(0)

# 简易 yaml 解析（不依赖 PyYAML）
regions = []
cur = {}
for line in text.splitlines():
    if line.strip().startswith('- region_id:'):
        if cur: regions.append(cur)
        cur = {'region_id': int(line.split(':',1)[1].strip())}
    else:
        m = re.match(r'\s+(\w+):\s*(.+?)\s*$', line)
        if m and cur is not None:
            k, v = m.group(1), m.group(2)
            if k in ('path','mmap_offset','bytes','value_size','home_ub_node_id','weight'):
                cur[k] = int(v) if k != 'path' else v
if cur: regions.append(cur)

if not regions:
    print("  WARN: no warm_regions found in manifest")
    sys.exit(0)

HEADER_FMT = struct.Struct('<IIIII4xQ32s')   # magic, version, region_id, capacity_slots, value_size, region_bytes, reserved
SLOT_FMT   = struct.Struct('<IIIIQQQQQII')   # state, region_id, local_slot, bytes, owner_gen, write_seq, key_hash, key_fp, last_access_ns, clock_bit, cold_state
HEADER_SZ  = 64
SLOT_SZ    = 64
SLOT_READY = 2

total_ready = 0
region_stats = []
for r in regions:
    path   = r.get('path')
    offset = r.get('mmap_offset', 0)
    rbytes = r.get('bytes', 0)
    rid    = r.get('region_id')
    if not path or not rbytes:
        print(f"  region_id={rid}: skip (missing path/bytes)"); continue

    # 本地物理 shmdev (1-4) 要 O_RDWR；远端 UB view (5-8) 要 O_RDWR|O_SYNC。
    # 先试 O_RDWR，mmap EPERM 则 fallback 到 O_SYNC。
    m = None
    for flags_open in (os.O_RDWR, os.O_RDWR | os.O_SYNC):
        try:
            fd = os.open(path, flags_open)
            try:
                m = mmap.mmap(fd, rbytes, mmap.MAP_SHARED, mmap.PROT_READ, offset=offset)
            finally:
                os.close(fd)
            break
        except PermissionError:
            m = None
            continue
        except OSError as e:
            m = None
            last_err = e
            continue
    if m is None:
        print(f"  region_id={rid} path={path}: mmap failed (tried O_RDWR and O_RDWR|O_SYNC), skipping")
        continue

    # 读 header
    magic, version, h_rid, cap, vsize, rg_bytes, _ = HEADER_FMT.unpack_from(m, 0)
    if magic not in (0x5631414c, 0x56314149):   # V1AL / V1AI
        print(f"  region_id={rid} path={path}: bad magic=0x{magic:x}, skipping")
        m.close(); continue

    # 统计 slot states
    counts = {0:0, 1:0, 2:0, 3:0}
    for i in range(cap):
        off = HEADER_SZ + i * SLOT_SZ
        if off + SLOT_SZ > rbytes: break
        state = SLOT_FMT.unpack_from(m, off)[0]
        counts[state] = counts.get(state, 0) + 1
    m.close()

    ready = counts.get(SLOT_READY, 0)
    total_ready += ready
    region_stats.append((rid, path, ready, cap, rbytes))

    state_str = '/'.join(f"{s}:{counts.get(s,0)}" for s in sorted(counts))
    print(f"  region_id={rid} path={path:24s} READY={ready:6d}/{cap:<6d} bytes={rbytes:>12d} states[{state_str}]")

if total_ready > 0:
    print("  ── distribution ──")
    for rid, path, ready, cap, rbytes in region_stats:
        pct = 100.0 * ready / total_ready
        bar = '#' * int(round(pct/2))
        print(f"    region_id={rid}: {ready:6d} ({pct:5.1f}%) {bar}")
else:
    print("  WARN: no READY slots found in any region")
PYEOF

# ── bench ──
echo ">>> bench: ${TRANSPORT} READ t=$T c=$C pipeline=$PIPELINE ${TEST_TIME}s..."
SRV_PID=$(cat $PIDFILE 2>/dev/null)
read J0_UT J0_ST < <(get_cpu_jiffies "$SRV_PID")
J0_NS=$(date +%s%N)
J0_SI=$(snapshot_si)

taskset -c "$CLIENT_MASK" $MEMTIER \
    --protocol vemb_v16 --vemb-v16-transport=${TRANSPORT} \
    --vemb-v16-dim $DIM -s 127.0.0.1 -p $PORT \
    -t $T -c $C --pipeline=$PIPELINE \
    --ratio=0:1 --key-pattern=R:R \
    --key-prefix=$KEY_PREFIX --key-minimum=1 --key-maximum=$NUM_KEYS \
    --test-time=$TEST_TIME \
    >$BENCH_OUT 2>$BENCH_ERR

read J1_UT J1_ST < <(get_cpu_jiffies "$SRV_PID")
J1_NS=$(date +%s%N)
ELAPSED_NS=$((J1_NS - J0_NS > 0 ? J1_NS - J0_NS : TEST_TIME * 1000000000))
# core 分母 = 纳秒实测窗口 (与脚本13对齐)
J1_SI=$(snapshot_si)

# ── 汇总 ──
echo ""
echo "============================================================"
echo "  RESULT: $MANIFEST_NAME / $TRANSPORT"
echo "============================================================"
BENCH_TOTALS=$(grep "^Totals" "$BENCH_OUT" | tail -1)
OPS_SEC=$(echo "$BENCH_TOTALS" | awk '{print $2}')
HITS_SEC=$(echo "$BENCH_TOTALS" | awk '{print $3}')
MISS_SEC=$(echo "$BENCH_TOTALS" | awk '{print $4}')
P50=$(echo "$BENCH_TOTALS" | awk '{print $6}')
P99=$(echo "$BENCH_TOTALS" | awk '{print $8}')
KBSEC=$(echo "$BENCH_TOTALS" | awk '{print $9}')

CORE_UT=$(awk -v d=$((J1_UT - J0_UT)) -v t=$ELAPSED_NS 'BEGIN{ if(d<0||t<=0) print "NA"; else printf "%.2f", d/100.0/(t/1000000000) }')
CORE_ST=$(awk -v d=$((J1_ST - J0_ST)) -v t=$ELAPSED_NS 'BEGIN{ if(d<0||t<=0) print "NA"; else printf "%.2f", d/100.0/(t/1000000000) }')
CPU_CORES=$(awk -v u="$CORE_UT" -v s="$CORE_ST" 'BEGIN{ if(u=="NA"||s=="NA") print "NA"; else printf "%.2f", u+s }')
OPS_PER_CORE=$(awk -v o="$OPS_SEC" -v c="$CPU_CORES" 'BEGIN{ if(c=="NA"||c==0) print "NA"; else printf "%.0f", o/c }')
GBSEC=$(awk -v k="$KBSEC" 'BEGIN{ printf "%.2f", k/1024/1024 }')
C_SI=$(awk -v d=$((J1_SI - J0_SI)) -v t=$ELAPSED_NS 'BEGIN{ if(d<0||t<=0) print "NA"; else printf "%.2f", d/100.0/(t/1000000000) }')
RSS=$(awk '/^VmRSS:/{print $2}' /proc/$SRV_PID/status 2>/dev/null)
[ -z "$RSS" ] && RSS="NA"

DERF_OK=$(grep -oE "handle_deref\[ok=[0-9]+ fail=[0-9]+\]" "$BENCH_ERR" | \
          awk -F"ok=" '{split($2,a," "); sum+=a[1]} END {print sum+0}')
DERF_FAIL=$(grep -oE "handle_deref\[ok=[0-9]+ fail=[0-9]+\]" "$BENCH_ERR" | \
            awk -F"fail=" '{split($2,a,"]"); sum+=a[1]} END {print sum+0}')
STATUS_NF=$(grep -oE "status\[ok=[0-9]+ nf=[0-9]+" "$BENCH_ERR" | \
            awk -F"nf=" '{sum+=$2} END {print sum+0}')

printf '  ops/sec         : %s\n' "$OPS_SEC"
printf '  hits/sec        : %s\n' "$HITS_SEC"
printf '  misses/sec      : %s  (status nf: %s)\n' "$MISS_SEC" "$STATUS_NF"
printf '  p50 / p99       : %s / %s ms\n' "$P50" "$P99"
printf '  wire throughput : %s GB/sec\n' "$GBSEC"
printf '  server CPU cores: %s  (over %ss)\n' "$CPU_CORES" "$TEST_TIME"
printf '  ops/core/sec    : %s\n' "$OPS_PER_CORE"
printf '  handle_deref    : ok=%s fail=%s\n' "$DERF_OK" "$DERF_FAIL"
printf '  core_ut/st       : %s / %s\n' "$CORE_UT" "$CORE_ST"
printf '  softirq          : %s\n' "$C_SI"
printf '  rss              : %s KB\n' "$RSS"

echo ""
echo "  stdout: $BENCH_OUT"
echo "  stderr: $BENCH_ERR"
echo "  server: $SERVER_LOG"
echo "============================================================"
