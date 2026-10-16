#!/bin/bash
# sve_vs_memcpy_e2e.sh
# 端到端对比 SVE flat load (-DUSE_ARM_SVE) vs memcpy 两种搬运路径的
# VEMB_INLINE 读吞吐。整库 -DUSE_ARM_SVE 开关，唯一变量是
# sve_streaming_load_f32 的 load 指令 (svld1_f32 vs memcpy)。
#
# Usage:
#   bash benchmark/sve_vs_memcpy_e2e.sh [local|remote]
#       local  = vemb_v16_warm_regions_111.yaml        (本地 pfn-map warm region)
#       remote = vemb_v16_warm_regions_111_remote.yaml (远端 UB memory)  [默认]
#
# 可选环境变量:
#   MEMTIER          = memtier_benchmark 可执行路径
#   SERVER_CPUSET    = server 绑核 (默认 1-96)
#   CLIENT_CPUSET    = client 绑核 (默认 97-191)
#   SERVER_NUMA      = server numa node (默认 0)
#   CLIENT_NUMA      = client numa node (默认 1)
#   PIO / SNW / T / C / DUR / NUM_KEYS  = 负载参数
#   WORKDIR          = 中间产物目录 (默认 /tmp/sve_e2e)
#
# 必须在 hpc-redis 项目根或其 benchmark/ 子目录下运行；脚本会自动定位项目根。

set -uo pipefail

# === 项目根自动定位 ===========================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HPC="$(cd "$SCRIPT_DIR/.." && pwd)"
[ -f "$HPC/src/server.c" ] || {
    echo "ERROR: 找不到 $HPC/src/server.c，请在 hpc-redis 项目根或 benchmark/ 下运行" >&2
    exit 2
}

# === 参数解析 =================================================================
SCENE="${1:-remote}"
case "$SCENE" in
    local)  MANIFEST="$HPC/examples/vemb_v16_warm_regions_111.yaml" ;;
    remote) MANIFEST="$HPC/examples/vemb_v16_warm_regions_111_remote.yaml" ;;
    -h|--help|"")
        sed -n '2,20p' "${BASH_SOURCE[0]}"
        exit 0 ;;
    *) echo "ERROR: 未知参数 '$SCENE'，可选: local|remote" >&2; exit 2 ;;
esac
[ -f "$MANIFEST" ] || { echo "ERROR: manifest 不存在: $MANIFEST" >&2; exit 2; }

# === 外部依赖与可调参数 =======================================================
MEMTIER="${MEMTIER:-$(dirname "$HPC")/memtier_benchmark/memtier_benchmark}"
[ -x "$MEMTIER" ] || {
    echo "ERROR: 找不到 memtier_benchmark，请 export MEMTIER=/path/to/memtier_benchmark" >&2
    exit 2
}

DIM=300
KEY_PREFIX="item:"
NUM_KEYS=${NUM_KEYS:-100000}
DUR=${DUR:-30}
PIO=${PIO:-32}; SNW=${SNW:-64}; T=${T:-64}; C=${C:-4}

SERVER_CPUSET=${SERVER_CPUSET:-1-96}
CLIENT_CPUSET=${CLIENT_CPUSET:-97-191}
SERVER_NUMA=${SERVER_NUMA:-0}
CLIENT_NUMA=${CLIENT_NUMA:-1}

PORT_SVE=6390
PORT_NOSVE=6391

WORKDIR=${WORKDIR:-/tmp/sve_e2e}
NOSVE_TREE="$WORKDIR/nosve-tree"
SVE_BIN="$HPC/src/redis-server"
NOSVE_BIN="$NOSVE_TREE/src/redis-server"
LOGDIR="$WORKDIR/logs"

mkdir -p "$LOGDIR"

# === cleanup =================================================================
cleanup() {
    pkill -9 -f "redis-server.*:$PORT_SVE"   2>/dev/null
    pkill -9 -f "redis-server.*:$PORT_NOSVE" 2>/dev/null
    git -C "$HPC" worktree remove "$NOSVE_TREE" --force 2>/dev/null
    git -C "$HPC" worktree prune 2>/dev/null
}
trap cleanup EXIT

# === 工具函数 =================================================================
count_ld1w() { objdump -d "$1" 2>/dev/null | grep -c '\bld1w\b'; }

wait_port() {
    local port=$1 i
    for ((i=0; i<60; i++)); do
        ss -tln | grep -q ":$port " && return 0
        sleep 0.2
    done
    return 1
}

banner() {
    echo ""
    echo "===================================================="
    echo " $1"
    echo "===================================================="
}

# === 阶段 1: SVE 版编译（主目录默认带 -DUSE_ARM_SVE）=========================
build_sve() {
    banner "[1/3] build SVE 版  (主目录默认 -DUSE_ARM_SVE)"
    cd "$HPC"
    if make -C src -j32 redis-server 2>&1 | tee "$LOGDIR/build_sve.log" | grep -qiE 'error:'; then
        echo "FAIL: SVE 版编译失败（见 $LOGDIR/build_sve.log）" >&2
        exit 1
    fi
    local n=$(count_ld1w "$SVE_BIN")
    echo "SVE binary: $SVE_BIN   ld1w=$n"
}

# === 阶段 2: memcpy 版编译（worktree 关 SVE）=================================
build_nosve() {
    banner "[2/3] build memcpy 版  (worktree 去 -DUSE_ARM_SVE + disable sve_config.h auto-detect)"
    rm -rf "$NOSVE_TREE"
    git -C "$HPC" worktree add --detach "$NOSVE_TREE" HEAD 2>&1 | tail -1

    # worktree 缺已构建 deps，从主目录拷
    rm -rf "$NOSVE_TREE/deps"
    cp -a "$HPC/deps" "$NOSVE_TREE/deps"

    # 改 1: Makefile 去掉 -DUSE_ARM_SVE（保留 -march=armv8.2-a+sve，保证编过）
    sed -i 's/-DUSE_ARM_SVE //g' "$NOSVE_TREE/src/Makefile"

    # 改 2: 关键 —— sve_config.h 里 #if defined(__ARM_FEATURE_SVE) || defined(USE_SVE)
    #         会自动 define USE_ARM_SVE；只要 -march=...+sve 在，编译器就 define
    #         __ARM_FEATURE_SVE。必须 disable 这条 auto-detection 才能真走 memcpy 分支。
    sed -i 's@#if defined(__ARM_FEATURE_SVE) || defined(USE_SVE)@#if 0 /* disabled for nosve build */@' \
        "$NOSVE_TREE/src/sve_config.h"

    echo "改动确认:"
    grep -n 'FINAL_CFLAGS += -march' "$NOSVE_TREE/src/Makefile"
    grep -A1 'disabled for nosve'    "$NOSVE_TREE/src/sve_config.h"

    cd "$NOSVE_TREE"
    if make -C src -j32 redis-server 2>&1 | tee "$LOGDIR/build_nosve.log" | grep -qiE 'error:'; then
        echo "FAIL: memcpy 版编译失败（部分 SVE 代码可能无 #else fallback）" >&2
        grep -iE 'error:' "$LOGDIR/build_nosve.log" | head -10 >&2
        exit 1
    fi
    local n=$(count_ld1w "$NOSVE_BIN")
    echo "nosve binary: $NOSVE_BIN   ld1w=$n"
}

# === 阶段 3: benchmark ========================================================
run_bench() {
    local bin=$1 port=$2 tag=$3
    banner "[3/3] bench $tag  (port $port  pio=$PIO snw=$SNW t=$T c=$C ${DUR}s)"
    pkill -9 -f "redis-server.*:$port" 2>/dev/null; sleep 1

    # 启 server
    numactl --membind=$SERVER_NUMA taskset -c $SERVER_CPUSET "$bin" \
        --port $port --bind 0.0.0.0 --protected-mode no \
        --vemb-v16-enabled yes --vemb-v16-dim $DIM \
        --vemb-v16-warm-regions-manifest "$MANIFEST" \
        --vemb-v16-reset-warm-regions yes \
        --vemb-v16-proxy-io-threads $PIO --vemb-v16-supernode-workers $SNW \
        --daemonize yes --loglevel notice \
        >"$LOGDIR/${tag}_server.log" 2>&1
    wait_port $port || {
        echo "FAIL: $tag server 起不来（见 $LOGDIR/${tag}_server.log）" >&2
        tail -5 "$LOGDIR/${tag}_server.log" >&2
        return 1
    }

    # prefill
    numactl --membind=$CLIENT_NUMA taskset -c $CLIENT_CPUSET "$MEMTIER" \
        --protocol vemb_v16 --vemb-v16-dim $DIM \
        -s 127.0.0.1 -p $port -t 1 -c 1 -n $NUM_KEYS \
        --ratio=1:0 --key-pattern=S:S --key-prefix=$KEY_PREFIX \
        --key-minimum=1 --key-maximum=$NUM_KEYS \
        >"$LOGDIR/${tag}_prefill.log" 2>&1
    grep -E '^Sets' "$LOGDIR/${tag}_prefill.log" | head -1 | awk -v t=$tag \
        '{printf "prefill %-8s sets/sec=%s\n", t, $2}'

    # read (VEMB_INLINE R:R)
    numactl --membind=$CLIENT_NUMA taskset -c $CLIENT_CPUSET "$MEMTIER" \
        --protocol vemb_v16 --vemb-v16-dim $DIM \
        -s 127.0.0.1 -p $port -t $T -c $C --pipeline=32 --test-time=$DUR \
        --ratio=0:1 --key-pattern=R:R --key-prefix=$KEY_PREFIX \
        --key-minimum=1 --key-maximum=$NUM_KEYS \
        >"$LOGDIR/${tag}_read.log" 2>&1

    local line=$(grep -E '^Gets' "$LOGDIR/${tag}_read.log" | head -1)
    local ops=$(echo "$line"  | awk '{print $2}')
    local p50=$(echo "$line"  | awk '{print $5}')
    local p99=$(echo "$line"  | awk '{print $7}')
    printf "%-8s ops=%-14s p50=%-10s p99=%s\n" "$tag" "$ops" "$p50" "$p99"
    echo "${tag}_OPS=$ops" >"$LOGDIR/${tag}_result.env"

    pkill -9 -f "redis-server.*:$port" 2>/dev/null; sleep 1
}

# === main ====================================================================
banner "SVE flat load vs memcpy 端到端对比"
cat <<EOF
scene        = $SCENE
manifest     = $(basename "$MANIFEST")
HPC          = $HPC
MEMTIER      = $MEMTIER
pio/snw/t/c  = $PIO / $SNW / $T / $C
dur          = ${DUR}s   num_keys=$NUM_KEYS
server       = numa$SERVER_NUMA / cores $SERVER_CPUSET
client       = numa$CLIENT_NUMA / cores $CLIENT_CPUSET
workdir      = $WORKDIR
logs         = $LOGDIR/
EOF

build_sve
build_nosve

# 校验：两版 ld1w 必须有差，否则 sve_config.h 没改成功
SVE_N=$(count_ld1w "$SVE_BIN")
NS_N=$(count_ld1w "$NOSVE_BIN")
banner "ld1w 指令数校验"
echo "sve=$SVE_N   nosve=$NS_N   差=$((SVE_N-NS_N))"
if [ "$SVE_N" -eq "$NS_N" ]; then
    echo "ERROR: 两版 ld1w 数相同 —— sve_config.h 改动未生效，memcpy 版仍走 SVE 路径" >&2
    exit 1
fi

run_bench "$SVE_BIN"   $PORT_SVE   sve
run_bench "$NOSVE_BIN" $PORT_NOSVE nosve

# === 汇总 ====================================================================
banner "汇总"
SVE_OPS=$(grep OPS= "$LOGDIR/sve_result.env"   | cut -d= -f2)
NS_OPS=$(grep OPS= "$LOGDIR/nosve_result.env"  | cut -d= -f2)
if [ -n "$SVE_OPS" ] && [ -n "$NS_OPS" ] && [ "$NS_OPS" != "0.00" ] 2>/dev/null; then
    awk -v s=$SVE_OPS -v n=$NS_OPS \
        'BEGIN{printf "SVE/memcpy 吞吐比 = %.2fx  (SVE 相对 %+d%%)\n", s/n, (s-n)*100/n}'
else
    echo "(某版结果缺失，见 $LOGDIR/)"
fi
echo ""
echo "详细日志: $LOGDIR/"
echo "done"
