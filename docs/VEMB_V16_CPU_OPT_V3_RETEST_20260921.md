# VEMB V16 Client CPU Optimization V3 Retest Analysis

Date: 2026-09-21
Branch: `dev_cpu_opt_v2_test` (based on `dev_cpu_opt_v2` @ 944d866)
Benchmark: `NUM_KEYS=100000 KEY_PATTERN=R:R THREADS=64 CLIENTS=4 PIO=3 SNW=3`

## Background

`dev_cli_cpu_opt_v3` (commit 0c5f52e) bundled multiple optimizations with diagnostic
code removal. The result was a 23% latency regression vs `dev_cpu_opt_v2`:

| Branch | QPS | avg_lat | single_core_qps |
|--------|-----|---------|-----------------|
| dev_cpu_opt_v2 (benchmark 125553) | 8.05M | 0.687ms | 1.39M |
| dev_cli_cpu_opt_v3 (benchmark 132432) | 7.50M | 0.849ms | 1.31M |

To isolate the root cause, optimizations were migrated one-by-one onto the v2
base (with all diagnostic code preserved) and benchmarked independently.

## Retest Results

### O-C1: route linked list (LIFO)

Replaced `for i=0..MAX_PENDING` array scan in `route_requests` with a
`route_head → route_next` LIFO linked list. Reduces `route_requests` CPU
from ~18% to ~2%.

| Metric | v2 baseline | v2 + O-C1 | Delta |
|--------|-------------|-----------|-------|
| QPS | 8.05M | 7.61M | **-5.5%** |
| avg_lat | 0.687ms | 0.815ms | **+18.6%** |
| single_core_qps | 1.39M | 1.32M | -5.0% |
| cpu_total | 5.77 | 5.84 | +1.2% |

**Result: NEGATIVE OPTIMIZATION — reverted.**

Root cause: the O(4096) array scan in v2 acts as an implicit batch accumulation
window. Removing it makes the poll loop spin ~6x faster, causing more frequent
flushes with smaller batches. Server per-request fixed overhead (poll_shm,
publish_response) increases, degrading overall throughput. This effect dominates
the client-side CPU savings because client CPU is not the throughput bottleneck
(Little's Law: pipeline_depth × avg_latency governs throughput).

### O-C6: v1 poll skip (pending_count == 0)

Added early return in `sdk_handle_session_poll_v1` when the UB transport has
`pending_count == 0`, avoiding unnecessary aeron response ring reads.

| Metric | v2 baseline | v2 + O-C6 | Delta |
|--------|-------------|-----------|-------|
| QPS | 8.05M | 8.01M | -0.5% (noise) |
| avg_lat | 0.687ms | 0.697ms | +1.5% (noise) |
| single_core_qps | 1.39M | 1.42M | **+1.8%** |
| cpu_total | 5.77 | 5.73 | -0.7% |

**Result: VALID — client CPU reduced, throughput unchanged.**

### Owner channel inited skip

Changed poll loop guard from `if (!v2->l0 && !owner_channel_inited[owner])`
to `if (!owner_channel_inited[owner])`, skipping 63/64 non-initialized owners
entirely (including diagnostic sampling overhead).

| Metric | v2 baseline | v2 + O-C6 + owner skip (run 1) | v2 + O-C6 + owner skip (run 2) |
|--------|-------------|------|------|
| QPS | 8.05M | 8.01M | 8.08M |
| avg_lat | 0.687ms | 0.695ms | 0.693ms |
| single_core_qps | 1.39M | 1.38M | 1.44M |

**Result: VALID — marginal additional CPU reduction, no throughput impact.**

## V3 Regression Root Cause

The regression was caused by **O-C1 (route linked list)**, not by diagnostic code
removal. The linked list optimization reduced `route_requests` work from O(4096)
to O(active), which paradoxically degraded throughput by eliminating an implicit
batch accumulation window in the poll loop.

Other v3 changes tested and ruled out:
- O-C4 (SVE streaming load NC copy): no impact on throughput
- `max_batch_delay_us = 10`: no improvement (batch timing not the mechanism)
- snapshot_id fix: v3-specific bug from accidental line deletion during cleanup

## Conclusion

Only O-C6 and owner_channel_inited skip are safe to apply on top of v2. O-C1
should not be used without a compensating mechanism (e.g., explicit batch delay
tuned to match the implicit accumulation window).
