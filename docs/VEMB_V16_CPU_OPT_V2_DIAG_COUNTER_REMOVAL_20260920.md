# VEMB V16 Hot-path Diagnostic Counter 移除回归

## 1. 背景

commit `f53ce26` 在 lookup 热路径引入了 per-request `atomic_fetch_add` 诊断计数器，用于采集 cache hit/miss、warm region 命中分布和 handle lookup miss 分类。这些计数器在每次 VEMB 查询中触发，虽然单次开销小，但在高 QPS 场景下累积影响 per-core 吞吐。

本次移除仅针对**热路径**的计数器调用，保留字段声明、`atomic_init` 和冷路径的 stats 读取代码（返回 0），不影响结构体布局和 wire format。

## 2. 移除内容

| 文件 | 移除项 | 热路径调用次数/req |
|---|---|---|
| `src/tlc_core.c` | `note_lookup_location()` 函数（整个删除） | 1 (hit) 或 1 (miss) |
| `src/tlc_core.c` | `tlc_core_get_warm_location_raw()` 内 6 个 `atomic_fetch_add` + 3 个 `note_lookup_location()` 调用 | 2–4 |
| `src/vemb_v16_supernode.c` | `vemb_v16_tlc_note_handle_lookup_miss()` 调用 | 1 (miss only) |
| `src/vemb_v16_tlc.c` | `vemb_v16_tlc_note_handle_lookup_miss()` 函数体 | — |
| `src/vemb_v16_tlc.h` | `vemb_v16_tlc_note_handle_lookup_miss()` 声明 | — |

保留项（冷路径，不影响性能）：
- `tlc_counter_add` 仍为真实 `atomic_fetch_add`（`remote_meta_*`、`ub_lookup_rpc_*` 等非热路径计数器继续工作）
- 所有计数器字段声明（`tlc_core_t`、`tlc_core_stats_t` 等）
- `atomic_init` 初始化（启动时一次）
- `tlc_core_get_stats` / `tlc_core_get_region_stats` 的 stats 读取（冷路径，返回 0）
- `vemb_v16_tlc_get_lookup_diagnostic_stats()` 函数（冷路径）
- `vemb_v16_diagnostic_stats_t`、`NET_DIAGNOSTIC_STATS` TCP handler、shutdown 日志

## 3. 测试条件

- 基准提交：`094e698`（含 S1–S4 优化 + 脚本 bugfix）
- 测试提交：`094e698` + 本次 diagnostic counter 移除
- 脚本：`run_aeron_cross_node_flamegraph.sh`，111 Server / 112 CLI
- 参数：`NUM_KEYS=100000 KEY_PATTERN=R:R THREADS=64 CLIENTS=4 PIO=5 SNW=5`
- Server CPU 0-15，CLI CPU 96-191，Warm 4 GiB，dim=300，pipeline/batch=32
- Server req/resp/warm = dev3/dev6/dev4，CLI = dev7/dev2/dev8

## 4. 数据

| 配置 | QPS (M) | P99 (ms) | Server CPU core | QPS/core (M) |
|---|---:|---:|---:|---:|
| 移除前 (5:5) | 8.197664 | 1.207 | 8.576 | 0.972308 |
| 移除后 (5:5) | 8.224877 | 1.199 | 8.261 | 1.017166 |

| 指标 | 变化 |
|---|---:|
| QPS | +0.33% |
| Server CPU core | −3.67% |
| **QPS/core** | **+4.61%** |
| P99 延迟 | −0.66% |

相同 QPS 水平下减少 0.32 cores，per-core 吞吐从 972K 提升到 1.017M。

## 5. 产物

| 运行 | Server flamegraph | Client flamegraph |
|---|---|---|
| 移除前 | [SVG](../perf/p5_s5_svr0-15_uniform_20260920_134150/server/server.svg) | [SVG](../perf/p5_s5_svr0-15_uniform_20260920_134150/client/client.svg) |
| 移除后 | [SVG](../perf/p5_s5_svr0-15_uniform_20260920_134939/server/server.svg) | [SVG](../perf/p5_s5_svr0-15_uniform_20260920_134939/client/client.svg) |

完整 perf data 和 workload log 在 `perf/p5_s5_svr0-15_uniform_20260920_134150/`（移除前）和 `perf/p5_s5_svr0-15_uniform_20260920_134939/`（移除后）。
