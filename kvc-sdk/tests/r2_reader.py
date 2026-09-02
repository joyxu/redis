import os, sys, time
from mooncake.store import MooncakeDistributedStore

# R2 用例驱动: 长驻读者跨 daemon 重启（单进程两阶段）。
# put+get（lazy_init, 持旧 owner_base）→ 等 /tmp/r2_go（门禁此时
# kill -9 daemon 并重启, owner_base 必变）→ 同进程继续 get 校验。
# epoch 复查应作废 poscache/刷新 owner_base: 不崩、不错读。
GO = "/tmp/r2_go"
for f in (GO,):
    if os.path.exists(f):
        os.unlink(f)

s = MooncakeDistributedStore()
assert s.setup("127.0.0.1:50140", "http://127.0.0.1:8080/metadata", 0,
               32*1024*1024, "tcp", "", "127.0.0.1:50051") == 0
data = bytes([0xB7]) * 4096

for i in range(8):
    assert s.put("r2k%03d" % i, data) == 0
for i in range(8):
    assert s.get("r2k%03d" % i) == data
print("PHASE1_OK", flush=True)

end = time.time() + 120
while not os.path.exists(GO) and time.time() < end:
    time.sleep(0.5)
if not os.path.exists(GO):
    print("NO_SIGNAL"); sys.exit(2)
time.sleep(3)   # 等 daemon 段就绪

bad = miss = 0
for i in range(8):
    try:
        r = s.get("r2k%03d" % i)
    except Exception:
        miss += 1; continue
    if r is None or len(r) == 0:
        miss += 1
    elif r != data:
        bad += 1
# miss 属上游语义: master 判 daemon 崩溃(client_expired)会清段上对象;
# 本用例守护的是"不崩、不错读"（stale owner_base 的危害）
print("PHASE2 bad=%d miss=%d" % (bad, miss))
sys.exit(1 if bad else 0)
