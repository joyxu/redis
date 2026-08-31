#!/bin/bash
# kvc_correctness_gate.sh — 正确性门禁（T1-4）
# 用法: bash kvc_correctness_gate.sh   （在 HW01 执行; HW02 可达则自动跑跨节点段）
#
# 覆盖:
#   1. 同机 verify_write 循环 ×5（flip+直读+位置缓存全开）
#   2. 跨节点 verify_write 循环 ×5（HW02 读 HW01, 直读防线全开）
#   3. Remove→invalidate 联动（短租约 master, INVALID 增量 == 删除数）
#   4. daemon 重启 slot 持久性（shm 状态跨进程生存）
# 判定: 全部通过 → GATE GREEN; 任一失败 → GATE RED 并给出失败项

set -u
PASS=0; FAIL=0; FAILED_ITEMS=""
ok()   { PASS=$((PASS+1)); echo "  [PASS] $1"; }
bad()  { FAIL=$((FAIL+1)); FAILED_ITEMS="$FAILED_ITEMS $1"; echo "  [FAIL] $1"; }

RB=$(python3 -c "print((4*1024**3 + 4*1024**3//8 + 16384 + 4095)//4096*4096)")
SDK=/root/gqs/codespace/UnifiedBus/hpc-redis/kvc-sdk
export MC_TCP_ENABLE_CONNECTION_POOL=1 MC_TCP_LANES_PER_PEER=16

restart_cluster() { # $1=ttl_ms
  pkill -x mooncake_client; pkill -x mooncake_master; pkill -f http_metadata_server
  sleep 2
  sysctl -w net.ipv4.tcp_tw_reuse=1 >/dev/null 2>&1
  PYTHONPATH=/opt/mc_py nohup python3 -m mooncake.http_metadata_server --port 8080 > /tmp/meta.log 2>&1 &
  sleep 1
  KVC_MASTER_BLOCK_SIZE=4096 nohup /tmp/mc_build/mooncake-store/src/mooncake_master \
    -default_kv_lease_ttl=${1:-600000} -http_metadata_server_port=8081 -metrics_port=9004 \
    -logtostderr > /tmp/master.log 2>&1 &
  sleep 2
  KVC_SEGMENT_DEVICE=/dev/obmm_shmdev1 KVC_REGION_ID=1 KVC_BLOCK_SIZE=4096 \
  KVC_SDK_LIB=$SDK/build/libkvc_server.so \
  nohup /tmp/mc_build/mooncake-store/src/mooncake_client --host=127.0.0.1 --global_segment_size=4GB \
    --master_server_address=localhost:50051 --metadata_server=http://127.0.0.1:8080/metadata \
    --protocol=tcp --port=50052 --logtostderr > /tmp/client.log 2>&1 &
  for _ in $(seq 1 40); do ss -tln 2>/dev/null | grep -q ":50052 " && break; sleep 0.5; done
  sleep 1
}

vf_run() { # $1=prefix  $2=host(master)  $3=meta  $4=local-hostname  $5=flip_device  $6=pattern
  KVC_FLIP_DEVICE=$5 KVC_FLIP_REGION=1 KVC_FLIP_BLOCK=4096 KVC_FLIP_BYTES=$RB \
  KVC_DIRECT_READ=1 KVC_POS_CACHE=1 KVC_SDK_LIB=$SDK/build/libkvc_server.so \
  PYTHONPATH=/opt/mc_py LD_LIBRARY_PATH=/opt/mc_py/mooncake:/tmp/mc_build/mooncake-common \
  python3 /root/gqs/codespace/UnifiedBus/hpc-redis/Mooncake/mooncake-store/benchmarks/store_kv_bench.py \
    --scenario verify_write --io-api plain \
    --local-hostname $4 --metadata-server $3 --master-server $2 --protocol tcp \
    --global-segment-size 0 --local-buffer-size $((32*1024*1024)) \
    --nr-objects 16 --batch-size 4 --key-prefix $1 --key-size 20 --value-size 4096 \
    --memory-replica-num 1 --nof-replica-num 0 --verify --pattern $6 2>&1 | \
    grep -A2 "phase verify_read" | grep -oE "verify_failures=[0-9]+" | tail -1
}

echo "===== T1-4 correctness gate $(date +%H:%M:%S) ====="

restart_cluster 600000
echo "--- 1. 同机 verify ×5 ---"
for i in 1 2 3 4 5; do
  r=$(vf_run cg$i 127.0.0.1:50051 http://127.0.0.1:8080/metadata 127.0.0.1:50071 /dev/obmm_shmdev1 0x61)
  [ "$r" = "verify_failures=0" ] && ok "same-node verify #$i" || bad "same-node verify #$i ($r)"
done

echo "--- 2. 跨节点 verify ×5（HW02 可达时）---"
if ssh -o ConnectTimeout=5 HW02 true 2>/dev/null; then
  for i in 1 2 3 4 5; do
    r=$(ssh HW02 "KVC_FLIP_DEVICE=/dev/obmm_shmdev5 KVC_FLIP_REGION=1 KVC_FLIP_BLOCK=4096 KVC_FLIP_BYTES=$RB \
    KVC_DIRECT_READ=1 KVC_POS_CACHE=1 KVC_SDK_LIB=$SDK/build/libkvc_server.so \
    PYTHONPATH=/opt/mc_py LD_LIBRARY_PATH=/opt/mc_py/mooncake:/tmp/mc_build/mooncake-common \
    python3 /root/gqs/codespace/UnifiedBus/hpc-redis/Mooncake/mooncake-store/benchmarks/store_kv_bench.py \
      --scenario verify_write --io-api plain \
      --local-hostname 192.168.1.112:50071 --metadata-server http://192.168.1.111:8080/metadata \
      --master-server 192.168.1.111:50051 --protocol tcp \
      --global-segment-size 0 --local-buffer-size $((32*1024*1024)) \
      --nr-objects 16 --batch-size 4 --key-prefix cxg$i --key-size 20 --value-size 4096 \
      --memory-replica-num 1 --nof-replica-num 0 --verify --pattern 0x62 2>&1 | \
      grep -A2 'phase verify_read' | grep -oE 'verify_failures=[0-9]+' | tail -1")
    [ "$r" = "verify_failures=0" ] && ok "cross-node verify #$i" || bad "cross-node verify #$i ($r)"
  done
else
  echo "  [SKIP] HW02 不可达, 跳过跨节点段"
fi

echo "--- 3. Remove→invalidate 联动（短租约 master）---"
restart_cluster 2000
inv_before=$(taskset -c 0-15 $SDK/build/kvc_probe /dev/obmm_shmdev1 $RB 4096 1 2>/dev/null | grep -oE "INVALID=[0-9]+" | grep -oE "[0-9]+")
KVC_FLIP_DEVICE=/dev/obmm_shmdev1 KVC_FLIP_REGION=1 KVC_FLIP_BLOCK=4096 KVC_FLIP_BYTES=$RB \
KVC_DIRECT_READ=1 KVC_POS_CACHE=1 KVC_SDK_LIB=$SDK/build/libkvc_server.so \
PYTHONPATH=/opt/mc_py LD_LIBRARY_PATH=/opt/mc_py/mooncake:/tmp/mc_build/mooncake-common \
python3 - <<EOF 2>/dev/null
from mooncake.store import MooncakeDistributedStore
import time
s = MooncakeDistributedStore()
s.setup("127.0.0.1:50094", "http://127.0.0.1:8080/metadata", 0, 32*1024*1024, "tcp", "", "127.0.0.1:50051")
for i in range(4):
    assert s.put("cgr%03d" % i, b"\x63" * 4096) == 0
for i in range(4):
    assert len(s.get("cgr%03d" % i)) == 4096
time.sleep(3)   # 等读租约过 2s TTL
for i in range(4):
    assert s.remove("cgr%03d" % i) == 0
print("REMOVE_OK")
EOF
inv_after=$(taskset -c 0-15 $SDK/build/kvc_probe /dev/obmm_shmdev1 $RB 4096 1 2>/dev/null | grep -oE "INVALID=[0-9]+" | grep -oE "[0-9]+")
delta=$((inv_after - inv_before))
[ "$delta" = "4" ] && ok "remove invalidation (INVALID +$delta)" || bad "remove invalidation (INVALID +$delta, expect 4)"

echo "--- 4. daemon 重启 slot 持久性 ---"
restart_cluster 600000
KVC_FLIP_DEVICE=/dev/obmm_shmdev1 KVC_FLIP_REGION=1 KVC_FLIP_BLOCK=4096 KVC_FLIP_BYTES=$RB \
KVC_DIRECT_READ=1 KVC_SDK_LIB=$SDK/build/libkvc_server.so \
PYTHONPATH=/opt/mc_py LD_LIBRARY_PATH=/opt/mc_py/mooncake:/tmp/mc_build/mooncake-common \
python3 - <<EOF 2>/dev/null
from mooncake.store import MooncakeDistributedStore
s = MooncakeDistributedStore()
s.setup("127.0.0.1:50093", "http://127.0.0.1:8080/metadata", 0, 32*1024*1024, "tcp", "", "127.0.0.1:50051")
for i in range(8):
    assert s.put("cgp%03d" % i, b"\x64" * 4096) == 0
print("PUT_OK")
EOF
r1=$(taskset -c 0-15 $SDK/build/kvc_probe /dev/obmm_shmdev1 $RB 4096 1 2>/dev/null | grep -oE "READY=[0-9]+" | grep -oE "[0-9]+")
# 只重启 daemon（master 保留, 段重挂）
pkill -x mooncake_client; sleep 2
KVC_SEGMENT_DEVICE=/dev/obmm_shmdev1 KVC_REGION_ID=1 KVC_BLOCK_SIZE=4096 \
KVC_SDK_LIB=$SDK/build/libkvc_server.so \
nohup /tmp/mc_build/mooncake-store/src/mooncake_client --host=127.0.0.1 --global_segment_size=4GB \
  --master_server_address=localhost:50051 --metadata_server=http://127.0.0.1:8080/metadata \
  --protocol=tcp --port=50052 --logtostderr > /tmp/client2.log 2>&1 &
for _ in $(seq 1 40); do ss -tln 2>/dev/null | grep -q ":50052 " && break; sleep 0.5; done
sleep 1
r2=$(taskset -c 0-15 $SDK/build/kvc_probe /dev/obmm_shmdev1 $RB 4096 1 2>/dev/null | grep -oE "READY=[0-9]+" | grep -oE "[0-9]+")
[ "$r1" = "$r2" ] && ok "slot persistence across daemon restart (READY=$r2)" || bad "slot persistence (READY $r1 -> $r2)"

echo ""
echo "===== GATE: PASS=$PASS FAIL=$FAIL ====="
[ "$FAIL" = "0" ] && echo "STATUS: GREEN" || echo "STATUS: RED — failed:$FAILED_ITEMS"
exit $FAIL
