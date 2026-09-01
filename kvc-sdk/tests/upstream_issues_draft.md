# 上游反馈草稿（kvcache-ai/Mooncake）— 三份 issue 文本

状态: 草稿, 未发布。发布方式: gh issue create -R kvcache-ai/Mooncake
（需确认是否以个人账号提交、是否匿名化环境细节）

---

## Issue 1: TCP transport one-shot connections exhaust ephemeral ports under
           sustained write volume

**Environment**: Mooncake store, protocol=tcp, Linux (default sysctls),
single-writer store_kv_bench fill workload.

**Symptom**: sustained TE writes (~530MB cumulative, threshold varies with
system uptime/history) begin failing wholesale:
- client log floods `TcpTransport::getConnection failed to create connection
  ... Error: connect: Cannot assign requested address` (EADDRNOTAVAIL)
  125k+ occurrences in one run
- bench observes TRANSFER_FAIL(-800) on remaining puts; retries then surface
  OBJECT_ALREADY_EXISTS(-705) paths
- failure threshold DETERIORATES across successive runs (residual TIME_WAIT
  keeps consuming the pool)

**Root cause chain**:
1. tcp_transport.cpp startTransfer() takes the one-shot path when
   MC_TCP_ENABLE_CONNECTION_POOL is unset (default false) — one new TCP
   connection per transfer, closed immediately
2. client-side TIME_WAIT (60s) accumulates faster than expiry at ~3k+ conn/s
3. default ip_local_port_range (~28k ports) exhausts → connect() fails

**Reproducer**: fresh master+client (anon segment), `store_kv_bench.py
--scenario fill --nr-objects 262144 --value-size 4096` on otherwise idle-ish
host; fails at ~136k objects deterministically in a quiet window.

**Workarounds we validated** (262144/262144 objects, 0 failures):
- `sysctl net.ipv4.tcp_tw_reuse=1` + widened port range, OR
- `MC_TCP_ENABLE_CONNECTION_POOL=1` (also 2.4x write throughput:
  108.5 -> 273.5 MiB/s in our measurements)

**Suggestions**:
- consider defaulting enable_connection_pool to true, or documenting the
  sysctl prerequisite prominently for TCP deployments
- getConnection() failure could surface a more specific error code distinct
  from generic TRANSFER_FAIL

---

## Issue 2: default_kv_lease_ttl=10s silently expires objects in
           read_perf-style long runs (misses without any error)

**Symptom**: store_kv_bench read_perf with --runtime 30 reports ~40% misses
(misses=2362048/5762048) although every object was prepared seconds earlier;
misses start ~10s into the run.

**Root cause**: master default_kv_lease_ttl="10000" (10s). Objects not
re-leased expire mid-run; subsequent GetReplicaList returns not-found. Reads
report as misses, no error anywhere — surprising for benchmark users who
assume prepared data persists for the run.

**Reproducer**: `read_perf --nr-objects 50000 --runtime 30` vs `--runtime 5`
(5s run: misses=0; 30s run: misses ≈ 41%).

**Suggestion**: at minimum document in store_kv_bench README that runtimes
beyond the lease TTL require `-default_kv_lease_ttl` adjustment; ideally the
bench could auto-warn when runtime > ttl.

---

## Issue 3: store_kv_bench.py remove_perf crashes — isExist vs is_exist

**Symptom**: `--scenario remove_perf` throws in the worker thread:
`AttributeError: 'mooncake.store.MooncakeDistributedStore' object has no
attribute 'isExist'. Did you mean: 'is_exist'?` (metadata_operation,
bench line ~534); the remove_perf phase then reports zeros and exit=0 —
failure is easy to miss.

**Fix**: one-liner rename isExist → is_exist (python binding exposes
is_exist).

**Extra observation** while testing remove: removing an object whose lease
was just acquired (e.g. get() then remove() in the same client) returns
-706 OBJECT_HAS_LEASE for the full lease TTL. Understandable semantically,
but worth documenting for bench/CI authors since it makes
get-then-immediately-remove sequences fail.
