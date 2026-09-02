#!/bin/bash
# kvc_regression.sh — KVC 集成性能回归（P2 每阶段改完必跑）
#
# 用法: bash kvc_regression.sh [标签]
#   三配置矩阵: anon / kvc-flipoff / kvc-flipon × (verify + read×2)
#   每轮全量重置三件套（清 TE metadata 残留 + master key + 段注册）
#   产出: /tmp/kvc_regress_<标签>.log  + 终端汇总表
#   判定: 与基线档（tag=baseline 存于 /tmp/kvc_regress_baseline_summary.txt）
#         对比, 读吞吐偏差 >5% 或出现失败即回归
#
# 前置: HW01 上 /tmp/mc_build 构建已含最新 patch; /opt/mc_py/mooncake/store.so
#       已同步为最新构建产物（脚本开头自动 cp）

set -u
TAG=${1:-run}

# 环境前提（本轮调试定位的两个坑，防复发）:
#  1) TE TCP one-shot 连接 → 默认端口池耗尽（~530MB 写入量）→ tcp_tw_reuse
#  2) master 默认 KV 租约 10s → runtime>10s 的 read_perf 出 miss → TTL 调大
sysctl -w net.ipv4.tcp_tw_reuse=1 >/dev/null 2>&1 || true
sysctl -w "net.ipv4.ip_local_port_range=10240 65535" >/dev/null 2>&1 || true

# TE 基线推到最强: 连接池 + 16 lane（公平对比; 实测 TE 108.5 -> 273.5 MiB/s）
export MC_TCP_ENABLE_CONNECTION_POOL=1
export MC_TCP_LANES_PER_PEER=16
LOG=/tmp/kvc_regress_${TAG}.log
MCB=/tmp/mc_build
PYROOT=/opt/mc_py

# 自动同步 python 绑定（历史坑: /opt/mc_py 是独立副本）
cp -f $MCB/mooncake-integration/store.cpython-311-aarch64-linux-gnu.so \
      $PYROOT/mooncake/store.so || exit 1

bench() { # $1=scenario $2=prefix $3..=extra
  cd /root/gqs/codespace/UnifiedBus/hpc-redis/Mooncake || return 1
  if [ "$FLIPON" = 1 ]; then
    export KVC_FLIP_DEVICE=/dev/obmm_shmdev1 KVC_FLIP_REGION=1 KVC_FLIP_BLOCK=4096
    export KVC_FLIP_BYTES=$(python3 -c "print((4*1024**3 + 4*1024**3//8 + 16384 + 4095)//4096*4096)")
    export KVC_SDK_LIB=/root/gqs/codespace/UnifiedBus/hpc-redis/kvc-sdk/build/libkvc_server.so
  else
    unset KVC_FLIP_DEVICE
  fi
  PYTHONPATH=$PYROOT LD_LIBRARY_PATH=$PYROOT/mooncake:$MCB/mooncake-common \
  python3 mooncake-store/benchmarks/store_kv_bench.py --scenario "$1" --io-api plain \
    --local-hostname 127.0.0.1:50071 \
    --metadata-server http://127.0.0.1:8080/metadata \
    --master-server 127.0.0.1:50051 --protocol tcp \
    --global-segment-size 0 --local-buffer-size $((32*1024*1024)) \
    --key-prefix "$2" --key-size 20 "${@:3}" >> $LOG 2>&1
}

start_cluster() { # $1=kvc|anon  $2=tag
  pkill -x mooncake_client; pkill -x mooncake_master; pkill -f http_metadata_server
  sleep 2
  PYTHONPATH=$PYROOT nohup python3 -m mooncake.http_metadata_server --port 8080 > /tmp/meta.log 2>&1 &
  sleep 1
  if [ "$1" = kvc ]; then
    KVC_MASTER_BLOCK_SIZE=4096 nohup $MCB/mooncake-store/src/mooncake_master -default_kv_lease_ttl=600000 -http_metadata_server_port=8081 -metrics_port=9004 -logtostderr > /tmp/master.log 2>&1 &
  else
    nohup $MCB/mooncake-store/src/mooncake_master -default_kv_lease_ttl=600000 -http_metadata_server_port=8081 -metrics_port=9004 -logtostderr > /tmp/master.log 2>&1 &
  fi
  sleep 2
  if [ "$1" = kvc ]; then
    KVC_SEGMENT_DEVICE=/dev/obmm_shmdev1 KVC_REGION_ID=1 KVC_BLOCK_SIZE=4096 \
    KVC_SDK_LIB=/root/gqs/codespace/UnifiedBus/hpc-redis/kvc-sdk/build/libkvc_server.so \
    nohup $MCB/mooncake-store/src/mooncake_client --host=127.0.0.1 --global_segment_size=4GB \
      --master_server_address=localhost:50051 --metadata_server=http://127.0.0.1:8080/metadata \
      --protocol=tcp --port=50052 --logtostderr > /tmp/client_$2.log 2>&1 &
  else
    nohup $MCB/mooncake-store/src/mooncake_client --host=127.0.0.1 --global_segment_size=4GB \
      --master_server_address=localhost:50051 --metadata_server=http://127.0.0.1:8080/metadata \
      --protocol=tcp --port=50052 --logtostderr > /tmp/client_$2.log 2>&1 &
  fi
  # 就绪探测: 等 daemon 50052 监听 + master 50051 监听（防启动竞态）
  for _ in $(seq 1 40); do
    if ss -tln 2>/dev/null | grep -q ":50052 " && ss -tln 2>/dev/null | grep -q ":50051 "; then
      sleep 1; return 0
    fi
    sleep 0.5
  done
  echo "WARN: cluster $1/$2 not ready in 20s" >&2
}

VERIFY_ARGS="--nr-objects 16 --batch-size 4 --value-size 4096 --memory-replica-num 1 --nof-replica-num 0 --verify --pattern 0xab"
READ_ARGS="--nr-objects 2048 --batch-size 32 --value-size 4096 --memory-replica-num 1 --nof-replica-num 0 --runtime 10"

: > $LOG
for cfg in anon kvc-off kvc-on kvc-direct kvc-full; do
  case $cfg in
    anon)        M=anon; FLIPON=0; unset KVC_POS_CACHE ;;
    kvc-off)     M=kvc;  FLIPON=0; unset KVC_POS_CACHE ;;
    kvc-on)      M=kvc;  FLIPON=1; unset KVC_DIRECT_READ KVC_POS_CACHE ;;
    kvc-direct)  M=kvc;  FLIPON=1; export KVC_DIRECT_READ=1; unset KVC_POS_CACHE ;;
    # 满配: 直读 + 位置缓存（终态目标口径）
    kvc-full)    M=kvc;  FLIPON=1; export KVC_DIRECT_READ=1 KVC_POS_CACHE=1 ;;
  esac
  echo "########## CONFIG: $cfg ##########" >> $LOG
  start_cluster $M ${cfg}_v; echo "== $cfg verify ==" >> $LOG; bench verify_write ${cfg}v $VERIFY_ARGS
  start_cluster $M ${cfg}_r; echo "== $cfg read run1 ==" >> $LOG; bench read_perf ${cfg}r1 $READ_ARGS
  start_cluster $M ${cfg}_r; echo "== $cfg read run2 ==" >> $LOG; bench read_perf ${cfg}r2 $READ_ARGS
done

# ---- 汇总表 ----
SUM=/tmp/kvc_regress_${TAG}_summary.txt
{
echo "===== regression summary [$TAG] $(date +%H:%M:%S) ====="
printf "%-10s %-12s %-10s %-10s %-10s %-10s %-8s\n" config phase MiB_s kv_s p50_ms p99_ms failures
awk -v tag="$TAG" '
  /^########## CONFIG/ { cfg=$3 }
  /^== / { phase=$2; sub(/==/,"",phase); phase=$2 }
  /kv\/s=/ && !/prepare/ {
    for(i=1;i<=NF;i++){
      if($i ~ /^kv\/s=/){kv=$i; sub("kv/s=","",kv)}
      if($i ~ /^MiB\/s=/){mb=$i; sub("MiB/s=","",mb)}
    }
    # 对应上一行的 phase read_perf 才计
  }
  /phase read_perf/ {inread=1; next}
  inread && /kv\/s=/ {
    mb=""; kv=""
    for(i=1;i<=NF;i++){
      if($i ~ /^MiB\/s=/){mb=$i; sub("MiB/s=","",mb)}
      if($i ~ /^kv\/s=/){kv=$i; sub("kv/s=","",kv)}
    }
    printf "%-10s %-12s %-10s %-10s\n", cfg, "read_perf", mb, kv
    inread=0
  }
' $LOG
echo "failures: $(grep -c "RuntimeError\|failed_requests=[1-9]" $LOG)"
} | tee $SUM
echo ""
echo "full log: $LOG"
[ "$(grep -c 'RuntimeError\|failed_requests=[1-9]' $LOG)" = "0" ] && echo "STATUS: ALL GREEN" || echo "STATUS: HAS FAILURES"
