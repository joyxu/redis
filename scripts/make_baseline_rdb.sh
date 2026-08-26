#!/bin/bash
# make_baseline_rdb.sh — 生成 baseline 预填充 RDB (供各 baseline 测试脚本免 prefill 加载)
#
# 用法:
#   DIM=300 NUM_KEYS=100000 NOQUANT=1 SET=myset bash scripts/make_baseline_rdb.sh
#
# 产物: benchmark/baseline_rdb/{NUM_KEYS}K_{DIM}D_{NOQUANT|INT8}_{SET}.rdb (只读 444)
#
# 内容与 run_vemb_local_loopback_sweep.sh 的 baseline prefill 逐字节一致
# (同 awk 公式 + srand(42)), 保证 bench 读到的数据不变。
#
# 注意:
#   - RDB 只能被生成它的 redis 版本加载 (当前 8.6.3); baseline 换版本需重新生成
#   - 生成后 chmod 444, 测试期间 --save '' + kill/NOSAVE 退出, 文件不会被回写
#   - 大维度文件较大 (d300/100K ≈ 133MB, d3072/100K ≈ 1.3GB), 按需生成常用规格
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
REDIS_DIR=${REDIS_DIR:-/root/gqs/codespace/redis-8.6.3}
OUT_DIR=${OUT_DIR:-$ROOT/benchmark/baseline_rdb}
PORT=${PORT:-6398}

DIM=${DIM:-300}
NUM_KEYS=${NUM_KEYS:-100000}
NOQUANT=${NOQUANT:-1}
SET=${SET:-myset}
# SCENE=rand    : 与 loopback/cross sweep 相同 (srand 42, rand()*j*0.001) [默认]
# SCENE=multi   : 与 run_multi_instance_redis_baseline.sh 相同 (固定 0.1 序列)
# SHARD/NSHARDS : 生成第 SHARD (0基) 个分片 (仅 multi 场景, key 区间按脚本同款切分)
SCENE=${SCENE:-rand}
SHARD=${SHARD:-0}
NSHARDS=${NSHARDS:-1}

mkdir -p "$OUT_DIR"
TAG=$([ "$NOQUANT" = 1 ] && echo NOQUANT || echo INT8)
SUF_SHARD=""; [ "$SCENE" = multi ] && [ "$NSHARDS" != 1 ] && SUF_SHARD="_sh$(printf '%02d' "$SHARD")"
RDB_NAME="${NUM_KEYS}K_${DIM}D_${TAG}_${SET}_${SCENE}${SUF_SHARD}.rdb"
RDB_PATH="$OUT_DIR/$RDB_NAME"

# 已存在则跳过 (FORCE=1 覆盖)
if [ -f "$RDB_PATH" ] && [ "${FORCE:-0}" != 1 ]; then
    echo "exists: $RDB_PATH (FORCE=1 重新生成)"
    exit 0
fi

SRV=$REDIS_DIR/src/redis-server
CLI=$REDIS_DIR/src/redis-cli
[ -x "$SRV" ] || { echo "redis-server 不存在: $SRV (REDIS_DIR 可覆盖)"; exit 1; }

WORK=$(mktemp -d /tmp/mkrdb.XXXXXX)
cleanup() {
    rm -rf "$WORK"
    # 端口卫生: 杀掉本脚本占用端口的残留实例, 防止数据累积
    for pid in $(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | sort -u); do
        kill -9 "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT

# 起前先清残留 (上次失败可能留下旧实例导致 VCARD 累积)
for pid in $(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | sort -u); do
    kill -9 "$pid" 2>/dev/null || true
done
sleep 0.5
if ss -tln 2>/dev/null | grep -q ":$PORT "; then
    echo "端口 $PORT 仍被占用, 拒绝生成"; exit 1
fi

echo "== 生成 $RDB_NAME (DIM=$DIM NUM_KEYS=$NUM_KEYS NOQUANT=$NOQUANT SET=$SET) ..."
"$SRV" --port "$PORT" --dir "$WORK" --dbfilename gen.rdb \
    --daemonize yes --save '' --appendonly no --logfile "$WORK/gen.log"
for _ in $(seq 1 50); do "$CLI" -p "$PORT" PING >/dev/null 2>&1 && break; sleep 0.2; done

T0=$(date +%s%N)
SUF=""; [ "$NOQUANT" = 1 ] && SUF=" NOQUANT"
# 先在主 shell 算好期望值 (管道子 shell 内的赋值传不出来)
if [ "$SCENE" = multi ]; then
    keys_per=$(( (NUM_KEYS + NSHARDS - 1) / NSHARDS ))
    MMIN=$(( SHARD * keys_per + 1 ))
    MMAX=$(( (SHARD + 1) * keys_per )); [ "$MMAX" -gt "$NUM_KEYS" ] && MMAX=$NUM_KEYS
    EXPECT=$((MMAX - MMIN + 1))
else
    EXPECT=$NUM_KEYS
fi
gen_pipe() {
    if [ "$SCENE" = multi ]; then
        # 与 run_multi_instance_redis_baseline.sh 完全一致: 固定 0.1 序列 + 按 SHARD 切 key 区间
        local vec i
        vec=$(seq -s " " 1 "$DIM" | sed "s/[0-9]*/0.1/g")
        for i in $(seq "$MMIN" "$MMAX"); do
            echo "VADD $SET VALUES $DIM $vec item:$i$SUF"
        done
    else
        awk -v n="$NUM_KEYS" -v dim="$DIM" -v set="$SET" -v suf="$SUF" 'BEGIN{
            srand(42);
            for (i=1; i<=n; i++) {
                printf "VADD %s VALUES %d", set, dim;
                for (j=0; j<dim; j++) printf " %f", rand()*j*0.001;
                printf " item:%d%s
", i, suf;
            }
        }'
    fi
}
gen_pipe | "$CLI" -p "$PORT" --pipe > "$WORK/pipe_ack.txt" 2>&1
grep -q "errors: 0" "$WORK/pipe_ack.txt" || { echo "prefill 失败: $(tail -1 "$WORK/pipe_ack.txt")"; exit 1; }
T1=$(date +%s%N)

VC=$("$CLI" -p "$PORT" VCARD "$SET")
[ "$VC" = "$EXPECT" ] || { echo "VCARD 校验失败: $VC != $EXPECT"; exit 1; }

"$CLI" -p "$PORT" SAVE >/dev/null 2>&1
"$CLI" -p "$PORT" SHUTDOWN NOSAVE >/dev/null 2>&1 || kill -9 $("$CLI" -p "$PORT" INFO server 2>/dev/null | grep process_id) 2>/dev/null || true
sleep 0.5

[ -f "$WORK/gen.rdb" ] || { echo "SAVE 未产出 rdb"; exit 1; }
mv "$WORK/gen.rdb" "$RDB_PATH"
chmod 444 "$RDB_PATH"
echo "== done: $RDB_PATH ($(du -h "$RDB_PATH" | cut -f1), prefill $(( (T1-T0)/1000000 ))ms, md5=$(md5sum "$RDB_PATH" | cut -d" " -f1))"
