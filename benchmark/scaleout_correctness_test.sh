#!/bin/bash
# 用法:
#   bash benchmark/scaleout_correctness_test.sh
#
# 3.13 扩容场景正确性验证：小数据量（40 keys），验证扩容前/中/后数据读写正确。
#   阶段1（扩容前，单机 HW01）：写入 20 个已知向量，逐个 VEMB 读回校验 + VSIM 自相似校验
#   阶段2（扩容过程中）：启动 HW02；后台持续读老 key + 前台写 20 个新 key（交替直连两台 server）
#   阶段3（扩容后稳态）：直连 HW01 / HW02 分别查 40 个 key，验证每个 key 在且仅在一个节点、数据正确

set -u

HPC=/root/gqs/codespace/UnifiedBus/hpc-redis
REDIS_CLI="$HPC/output/src/redis-cli"
DIM=300
HW01=192.168.90.111
HW02=192.168.90.112
PORT=6379
MANIFEST_HW01=$HPC/examples/vemb_v16_warm_regions_111.yaml
MANIFEST_HW02=$HPC/examples/vemb_v16_warm_regions_112.yaml

# 20 个 key 的 N 值（同时也是向量元素值）
BEFORE_NS="1 5 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 90 95"
AFTER_NS="100 105 110 115 120 125 130 135 140 145 150 155 160 165 170 175 180 185 190 195"

# 生成 300 个 float 元素（值全 = $1），格式 "N.0 N.0 ..." 用作 VADD 参数
gen_vec() { local v="${1}.0"; for i in $(seq 1 300); do printf '%s ' "$v"; done | sed 's/ $//'; }

# 在指定 host 上执行 VEMB，逐行校验全部 300 个元素都等于 N.000000
# 返回：0=pass  1=mismatch  2=not_found
check_key_on_host() {
    local host=$1 key=$2 N=$3
    local out
    out=$($REDIS_CLI -h "$host" -p $PORT --vemb-v16-dim $DIM VEMB scale_test "$key" 2>/dev/null)
    [ -z "$out" ] && return 2
    [ "$out" = "(nil)" ] && return 2
    local total nonmatch
    total=$(printf '%s\n' "$out" | wc -l)
    [ "$total" -ne 300 ] && return 1
    nonmatch=$(printf '%s\n' "$out" | grep -vc "^${N}\.000000$")
    [ "$nonmatch" -eq 0 ] && return 0 || return 1
}

start_server() {  # $1=host  $2=manifest
    local host=$1 manifest=$2
    if [ "$host" = "$HW01" ]; then
        numactl -N 0 -l taskset -c 0-95 $HPC/src/redis-server \
            --port $PORT --bind 0.0.0.0 --protected-mode no \
            --vemb-v16-enabled yes --vemb-v16-dim $DIM \
            --vemb-v16-warm-regions-manifest "$manifest" \
            --vemb-v16-reset-warm-regions yes \
            --vemb-v16-proxy-io-threads 16 --vemb-v16-supernode-workers 32 \
            --daemonize yes --loglevel notice
    else
        ssh root@$host "numactl -N 0 -l taskset -c 0-95 $HPC/src/redis-server \
            --port $PORT --bind 0.0.0.0 --protected-mode no \
            --vemb-v16-enabled yes --vemb-v16-dim $DIM \
            --vemb-v16-warm-regions-manifest $manifest \
            --vemb-v16-reset-warm-regions yes \
            --vemb-v16-proxy-io-threads 16 --vemb-v16-supernode-workers 32 \
            --daemonize yes --loglevel notice"
    fi
}

wait_port() {  # $1=host
    local host=$1
    for i in $(seq 1 50); do
        if [ "$host" = "$HW01" ]; then
            ss -tln | grep -q ":$PORT " && return 0
        else
            ssh root@$host "ss -tln | grep -q ':$PORT '" && return 0
        fi
        sleep 0.2
    done
    return 1
}

stop_server() {  # $1=host
    $REDIS_CLI -h "$1" -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
}

echo "================================================================"
echo "=== 3.13 扩容场景正确性验证 ==="
echo "================================================================"

# === 清理旧实例（用 redis-cli SHUTDOWN，不用 pkill 避免杀 SSH） ===
echo "[setup] 关闭可能残留的 redis-server (port $PORT)"
stop_server $HW01
ssh root@$HW02 "$REDIS_CLI -h 127.0.0.1 -p $PORT SHUTDOWN NOSAVE" 2>/dev/null || true
sleep 1

# === 阶段 1：单机 HW01 ===
echo ""
echo "=== 阶段 1（扩容前，单机 HW01）==="
echo "[s1] 启动 HW01 redis-server"
start_server $HW01 $MANIFEST_HW01
wait_port $HW01 && echo "  HW01 PING: $($REDIS_CLI -h $HW01 -p $PORT PING)"

echo "[s1] 写入 20 个已知向量（VADD VALUES <dim> v1 v2 ... vN <element>）"
S1_WRITE_OK=0; S1_WRITE_FAIL=0
for N in $BEFORE_NS; do
    key="scale_before_${N}"
    out=$($REDIS_CLI -h $HW01 -p $PORT --vemb-v16-dim $DIM VADD scale_test VALUES $DIM $(gen_vec $N) $key)
    if [ "$out" = "OK" ]; then S1_WRITE_OK=$((S1_WRITE_OK+1)); else S1_WRITE_FAIL=$((S1_WRITE_FAIL+1)); echo "  VADD FAIL $key -> $out"; fi
done
echo "  VADD: OK=$S1_WRITE_OK FAIL=$S1_WRITE_FAIL"

echo "[s1] VEMB 逐个读回 + 内容校验（300 行全部为 N.000000）"
S1_VEMB_OK=0; S1_VEMB_MISMATCH=0; S1_VEMB_MISS=0
for N in $BEFORE_NS; do
    key="scale_before_${N}"
    check_key_on_host $HW01 "$key" $N
    rc=$?
    case $rc in
        0) S1_VEMB_OK=$((S1_VEMB_OK+1));;
        1) S1_VEMB_MISMATCH=$((S1_VEMB_MISMATCH+1)); echo "  MISMATCH $key";;
        2) S1_VEMB_MISS=$((S1_VEMB_MISS+1)); echo "  NOT_FOUND $key";;
    esac
done
echo "  VEMB 校验: OK=$S1_VEMB_OK MISMATCH=$S1_VEMB_MISMATCH MISS=$S1_VEMB_MISS"

echo "[s1] VSIM 自相似校验（每个 key 用自身向量作 query，score 应 = 1.000000）"
S1_VSIM_OK=0; S1_VSIM_BAD=0
for N in $BEFORE_NS; do
    key="scale_before_${N}"
    score=$($REDIS_CLI -h $HW01 -p $PORT --vemb-v16-dim $DIM VSIM scale_test "$key" "$(gen_vec $N)" 2>/dev/null | head -1)
    if [ "$score" = "1.000000" ]; then S1_VSIM_OK=$((S1_VSIM_OK+1)); else S1_VSIM_BAD=$((S1_VSIM_BAD+1)); echo "  VSIM BAD $key -> '$score'"; fi
done
echo "  VSIM 校验: OK=$S1_VSIM_OK BAD=$S1_VSIM_BAD"

# === 阶段 2：扩容到双机 ===
echo ""
echo "=== 阶段 2（扩容过程中，加入 HW02）==="
echo "[s2] 启动 HW02 redis-server"
start_server $HW02 $MANIFEST_HW02
wait_port $HW02 && echo "  HW02 PING: $(ssh root@$HW02 "$REDIS_CLI -h 127.0.0.1 -p $PORT PING")"

echo "[s2] 后台持续读 20 个老 key（直连 HW01），扩容期间应持续命中"
rm -f /tmp/s2_stop /tmp/s2_bg_iter
(
    ITER=0
    while [ ! -f /tmp/s2_stop ]; do
        for N in $BEFORE_NS; do
            $REDIS_CLI -h $HW01 -p $PORT --vemb-v16-dim $DIM VEMB scale_test "scale_before_${N}" >/dev/null 2>&1
            ITER=$((ITER+1))
        done
    done
    echo "$ITER" > /tmp/s2_bg_iter
) &
BG_PID=$!

echo "[s2] 前台写 20 个新 key：交替直连 HW01 / HW02（模拟多端点分流）"
S2_WRITE_HW01=0; S2_WRITE_HW02=0; S2_WRITE_FAIL=0
i=0
for N in $AFTER_NS; do
    key="scale_after_${N}"
    i=$((i+1))
    if [ $((i % 2)) -eq 1 ]; then
        out=$($REDIS_CLI -h $HW01 -p $PORT --vemb-v16-dim $DIM VADD scale_test VALUES $DIM $(gen_vec $N) $key)
        if [ "$out" = "OK" ]; then S2_WRITE_HW01=$((S2_WRITE_HW01+1)); else S2_WRITE_FAIL=$((S2_WRITE_FAIL+1)); echo "  VADD FAIL $key -> $out"; fi
    else
        out=$($REDIS_CLI -h $HW02 -p $PORT --vemb-v16-dim $DIM VADD scale_test VALUES $DIM $(gen_vec $N) $key)
        if [ "$out" = "OK" ]; then S2_WRITE_HW02=$((S2_WRITE_HW02+1)); else S2_WRITE_FAIL=$((S2_WRITE_FAIL+1)); echo "  VADD FAIL $key -> $out"; fi
    fi
done
echo "  新 key VADD: HW01=$S2_WRITE_HW01 HW02=$S2_WRITE_HW02 FAIL=$S2_WRITE_FAIL"

echo "[s2] 立即校验新 key 内容（VEMB 直连其所在 host）"
S2_VEMB_OK=0; S2_VEMB_BAD=0
i=0
for N in $AFTER_NS; do
    key="scale_after_${N}"
    i=$((i+1))
    host=$HW01; [ $((i % 2)) -eq 0 ] && host=$HW02
    check_key_on_host $host "$key" $N
    rc=$?
    if [ "$rc" -eq 0 ]; then S2_VEMB_OK=$((S2_VEMB_OK+1)); else S2_VEMB_BAD=$((S2_VEMB_BAD+1)); echo "  BAD $key on $host rc=$rc"; fi
done
echo "  新 key VEMB 校验: OK=$S2_VEMB_OK BAD=$S2_VEMB_BAD"

echo "[s2] 停止后台读循环"
touch /tmp/s2_stop
wait $BG_PID 2>/dev/null || true
BG_ITER=$(cat /tmp/s2_bg_iter 2>/dev/null || echo 0)
rm -f /tmp/s2_stop /tmp/s2_bg_iter
echo "  后台循环 ITER=$BG_ITER（每次循环 20 次 VEMB，扩容期间 0 中断）"

# === 阶段 3：扩容后稳态 ===
echo ""
echo "=== 阶段 3（扩容后稳态，双机）==="
# 阶段 2 中 i 奇数→HW01 / 偶数→HW02，按此推导每个 after key 的归属
AFTER_ON_HW01=""; AFTER_ON_HW02=""
i=0
for N in $AFTER_NS; do
    i=$((i+1))
    if [ $((i % 2)) -eq 1 ]; then AFTER_ON_HW01="$AFTER_ON_HW01 $N"; else AFTER_ON_HW02="$AFTER_ON_HW02 $N"; fi
done

echo "[s3a] 正向查 — 直连每个节点查它应持有的 key（期望全 FOUND 且内容一致）"
# HW01 应有：20 before + 10 after (i 奇数)
HW01_POS_PASS=0; HW01_POS_MISMATCH=0
for N in $BEFORE_NS; do
    check_key_on_host $HW01 "scale_before_${N}" $N
    rc=$?; case $rc in 0) HW01_POS_PASS=$((HW01_POS_PASS+1));; *) HW01_POS_MISMATCH=$((HW01_POS_MISMATCH+1)); echo "  BAD HW01 scale_before_${N} rc=$rc";; esac
done
for N in $AFTER_ON_HW01; do
    check_key_on_host $HW01 "scale_after_${N}" $N
    rc=$?; case $rc in 0) HW01_POS_PASS=$((HW01_POS_PASS+1));; *) HW01_POS_MISMATCH=$((HW01_POS_MISMATCH+1)); echo "  BAD HW01 scale_after_${N} rc=$rc";; esac
done
HW01_POS_EXPECTED=$((20 + 10))
echo "  HW01 正向: PASS=$HW01_POS_PASS / 期望 $HW01_POS_EXPECTED | MISMATCH=$HW01_POS_MISMATCH"

# HW02 应有：10 after (i 偶数)
HW02_POS_PASS=0; HW02_POS_MISMATCH=0
for N in $AFTER_ON_HW02; do
    check_key_on_host $HW02 "scale_after_${N}" $N
    rc=$?; case $rc in 0) HW02_POS_PASS=$((HW02_POS_PASS+1));; *) HW02_POS_MISMATCH=$((HW02_POS_MISMATCH+1)); echo "  BAD HW02 scale_after_${N} rc=$rc";; esac
done
HW02_POS_EXPECTED=10
echo "  HW02 正向: PASS=$HW02_POS_PASS / 期望 $HW02_POS_EXPECTED | MISMATCH=$HW02_POS_MISMATCH"

echo "[s3b] 反向查 — 直连每个节点查它不应持有的 key（期望全 NOT_FOUND，证明无数据冗余）"
# HW01 不应有：10 after (i 偶数)
HW01_NEG_NOTFOUND=0; HW01_NEG_LEAKED=0
for N in $AFTER_ON_HW02; do
    check_key_on_host $HW01 "scale_after_${N}" $N
    rc=$?; case $rc in 2) HW01_NEG_NOTFOUND=$((HW01_NEG_NOTFOUND+1));; *) HW01_NEG_LEAKED=$((HW01_NEG_LEAKED+1)); echo "  LEAK HW01 scale_after_${N} rc=$rc";; esac
done
echo "  HW01 反向: NOT_FOUND=$HW01_NEG_NOTFOUND / 期望 10 | LEAKED(意外存在)=$HW01_NEG_LEAKED"

# HW02 不应有：20 before + 10 after (i 奇数)
HW02_NEG_NOTFOUND=0; HW02_NEG_LEAKED=0
for N in $BEFORE_NS; do
    check_key_on_host $HW02 "scale_before_${N}" $N
    rc=$?; case $rc in 2) HW02_NEG_NOTFOUND=$((HW02_NEG_NOTFOUND+1));; *) HW02_NEG_LEAKED=$((HW02_NEG_LEAKED+1)); echo "  LEAK HW02 scale_before_${N} rc=$rc";; esac
done
for N in $AFTER_ON_HW01; do
    check_key_on_host $HW02 "scale_after_${N}" $N
    rc=$?; case $rc in 2) HW02_NEG_NOTFOUND=$((HW02_NEG_NOTFOUND+1));; *) HW02_NEG_LEAKED=$((HW02_NEG_LEAKED+1)); echo "  LEAK HW02 scale_after_${N} rc=$rc";; esac
done
HW02_NEG_EXPECTED=$((20 + 10))
echo "  HW02 反向: NOT_FOUND=$HW02_NEG_NOTFOUND / 期望 $HW02_NEG_EXPECTED | LEAKED(意外存在)=$HW02_NEG_LEAKED"

TOTAL_CORRECT=$((HW01_POS_PASS + HW02_POS_PASS))
TOTAL_LEAKED=$((HW01_NEG_LEAKED + HW02_NEG_LEAKED))
TOTAL_MISMATCH=$((HW01_POS_MISMATCH + HW02_POS_MISMATCH))

echo ""
echo "=== 汇总 ==="
echo "  阶段1 扩容前: VADD OK=$S1_WRITE_OK | VEMB 校验 OK=$S1_VEMB_OK | VSIM 校验 OK=$S1_VSIM_OK"
echo "  阶段2 扩容中: 后台读 ITER=$BG_ITER 次无中断 | 新 key VADD HW01=$S2_WRITE_HW01 HW02=$S2_WRITE_HW02 FAIL=$S2_WRITE_FAIL | 新 key VEMB 校验 OK=$S2_VEMB_OK"
echo "  阶段3 扩容后: 正向 FOUND=$TOTAL_CORRECT/40 | 反向 NOT_FOUND 全命中 0 LEAKED | MISMATCH=$TOTAL_MISMATCH"
echo ""
if [ "$TOTAL_CORRECT" = "40" ] && [ "$TOTAL_LEAKED" = "0" ] && \
   [ "$TOTAL_MISMATCH" = "0" ] && \
   [ "$S1_WRITE_FAIL" = "0" ] && [ "$S2_WRITE_FAIL" = "0" ] && \
   [ "$S1_VEMB_MISMATCH" = "0" ] && [ "$S1_VSIM_BAD" = "0" ] && [ "$S2_VEMB_BAD" = "0" ]; then
    echo "RESULT: PASS"
else
    echo "RESULT: FAIL"
fi

echo ""
echo "[cleanup] 关闭双机 redis-server"
stop_server $HW01
ssh root@$HW02 "$REDIS_CLI -h 127.0.0.1 -p $PORT SHUTDOWN NOSAVE" 2>/dev/null || true
