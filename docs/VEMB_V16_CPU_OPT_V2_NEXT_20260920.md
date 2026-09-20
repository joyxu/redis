# VEMB V16 CPU Opt V2 — 后续优化方向

基于 `98d064d`（S1–S4 + diagnostic counter 移除）的 5:5 flamegraph 分析。

## 1. 当前 CPU 分布（5:5, 8.225M QPS, 8.26 cores）

Server 采样分为两大线程组：PIO (60.7%) 和 SuperNode (39.3%)。

### PIO 线程内部

| 函数 | 占 PIO % | 占总 % | 说明 |
|---|---:|---:|---|
| `proxy_io_pool_thread_main` self | 55.07 | 33.42 | 空轮询 busy-poll，flamegraph 左侧空白来源 |
| `vemb_v16_aeron_publish_batch_response` | 21.69 | 13.16 | 响应发布 |
| `vemb_v16_aeron_poll_shm_requests` | 14.71 | 8.93 | UB ring 请求读取 |
| `drain_completions` | 4.84 | 2.94 | SuperNode 完成队列排空 |
| `drain_job_return_queues` | 3.51 | 2.13 | job 回收 |

### SuperNode 线程内部

| 函数 | 占 SN % | 占总 % | 说明 |
|---|---:|---:|---|
| `tlc_core_get_warm_location_raw` | 56.91 | 22.37 | warm 区域查找（有效功） |
| `supernode_pool_thread_main` self | 10.73 | 4.22 | SN 空轮询 |
| `vemb_v16_supernode_handle_vemb_job` | 7.27 | 2.86 | job 分发 |

## 2. 优化方向（按潜在收益排序）

### O1. `aeron_snapshot_readers` seq_cst → acq_rel（PIO 热循环）

每轮 PIO poll 循环执行 3 次 `memory_order_seq_cst` 原子操作：

```
fetch_add(&readers, 1, seq_cst)   // 进入
load(&snapshot, seq_cst)          // 读指针
fetch_sub(&readers, 1, seq_cst)   // 退出
```

ARM 上 `seq_cst` RMW = `LDAXR/STLXR` + `DMB ISH`。降级方案：

| 位置 | 当前 | 降级 | 理由 |
|---|---|---|---|
| reader `fetch_add` | seq_cst | acq_rel | acquire 保证后续 load 有序；release 保证 writer 可见 |
| reader `load(&snapshot)` | seq_cst | acquire | 看到 writer exchange(release) 的新 buffer |
| reader `fetch_sub` | seq_cst | release | 之前的 channel 读取在 decrement 前完成 |
| writer `load(&readers)` | seq_cst | acquire | 看到 reader fetch_sub(release) |
| writer `load(&snapshot)` | seq_cst | acquire | 单线程写入，relaxed 也行 |
| writer `exchange(&snapshot)` | seq_cst | release | 发布 buffer 内容 |

每轮省 2 个 `DMB ISH`。5 个 PIO 线程 × 每秒数百万轮 = 每秒数千万次全屏障。

**预估**：per-core QPS +1–3%。

### O2. PIO 空轮询自适应退避

`proxy_io_pool_thread_main` self 占总 CPU 33%，是最大单项开销。当前用 `aeron_io_tiny_pause(idle_rounds)` 做微退避，但 idle 检测粒度可能不够。方向：
- 连续 N 轮无 work 后加长 pause
- 基于 snapshot 中 channel 数量动态调整
- 减少 PIO 线程数（从 5 降到 3-4，如果 QPS 不降）

**预估**：core_equiv 降 1–2 cores，但 QPS 可能受影响，需谨慎。

### O3. `vemb_v16_aeron_publish_batch_response` 优化

占总 CPU 12.9%。可能方向：
- 合并多个小响应为一次批量 arena commit
- 减少 response ring 的 release fence 次数
- S3/S4 已优化过 arena layout，进一步需看细分热点

**预估**：per-core QPS +2–5%，取决于具体瓶颈。

### O4. `tlc_core_get_warm_location_raw` 优化

占总 CPU 19.9%，是**有效功**。方向：
- 提高 `location_cache` 命中率（增大 cache 或优化 eviction 策略）
- hash table layout 优化（cache line 对齐、prefetch 下一跳）
- 热 key fast path

**预估**：per-core QPS +3–8%，但改动复杂度高。

### O5. SuperNode 空轮询

`supernode_pool_thread_main` self 占 4.2%，与 O2 同类问题但占比小。

## 3. 实施记录

| 编号 | 状态 | 提交 | QPS (M) | core_equiv | QPS/core (M) | QPS/core 变化 |
|---|---|---|---:|---:|---:|---:|
| baseline | 完成 | `98d064d` | 8.225 | 8.261 | 1.017 | — |
| O1 | 完成 | 见下 | 8.227 | 8.175 | 1.023 | +0.61% |
| O2 | 待定 | — | — | — | — | — |
| O3 | 待定 | — | — | — | — | — |
| O4 | 待定 | — | — | — | — | — |
| O5 | 待定 | — | — | — | — | — |

### O1 产物

| 运行 | Server flamegraph | Client flamegraph |
|---|---|---|
| O1 | [SVG](../perf/p5_s5_svr0-15_uniform_20260920_140733/server/server.svg) | [SVG](../perf/p5_s5_svr0-15_uniform_20260920_140733/client/client.svg) |
