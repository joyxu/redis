#!/bin/bash
# kvc_ubme_clean.sh — KVC 动态段脏态清理（journal + 孤儿 obmm 段 + decoder 窗口）
#
# 用法: bash kvc_ubme_clean.sh [devid]   （HW01 devid=0, HW02 devid=30; 缺省自动探测）
#
# 与 VEMB 共存约定:
#   - obmm_shmdev 1-16 与 decoder 0x980x 窗口是平台基线, 本脚本绝不清理;
#   - 两套 clean 脚本不要并发跑。
set -u
# 缺省自动探测: /proc/mami/udm/udevice 的 udevid[<n>]（partner 运行时同源）
DEVID=${1:-$(awk -F'[][]' '/udevid/{print $2; exit}' /proc/mami/udm/udevice 2>/dev/null)}
DEVID=${DEVID:-0}
AISDK=/home/ubengine_test/test/aisdk

echo "=== kvc ubme clean (devid=$DEVID) ==="

# 1. decoder 动态窗口（非 0x980x 基线）
if [ -d "$AISDK" ]; then
  cd "$AISDK" || true
  for h in $(./drv_mami_dt ubmem handle_query devid "$DEVID" chipId 0 dieId 0 portSetId 0 \
             decoderIdx 1 queryNum 64 type 4 2>/dev/null | \
             grep -oE 'handle=0x[0-9a-f]+, hpa=0x[0-9a-f]+' | grep -v 'hpa=0x980' | \
             grep -oE 'handle=0x[0-9a-f]+' | sed 's/handle=//'); do
    ./drv_mami_dt ubmem decoder_del devid "$DEVID" chipId 0 dieId 0 portSetId 0 \
      decoderIdx 1 handle "$h" >/dev/null 2>&1 && echo "decoder_del $h"
  done
fi

# 2. 孤儿 obmm 段（基线 1-16 保留; 幽灵盲扫 17-200）
for id in $(seq 17 200); do
  obmmctl unimport --mem_id "$id" >/dev/null 2>&1
  obmmctl unexport --mem_id "$id" >/dev/null 2>&1
done

# 3. journal（清掉后 /dev 节点成为孤儿 → 上面的盲扫已先行处理）
rm -f /var/run/ubme/*.journal

echo "=== done: $(ls /dev | grep -c obmm_shmdev) shmdev nodes remain (baseline 16)"
