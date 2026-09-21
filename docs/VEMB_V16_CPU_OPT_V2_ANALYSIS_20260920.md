# VEMB V16 CPU Opt V2 — 优化总结与后续分析

基于 `944d866`（S1–S4 + diagnostic counter 移除 + O1 seq_cst 降级）的 flamegraph 分析。

## 1. 已完成优化总结

### 1.1 S1–S4 hot-path 优化（`8c1f403`）

基线：`cfd2cf4`，测试条件 5:5 / 6:6，Server CPU 0-15，THREADS=64，CLIENTS=4，pipeline/batch=32。

| 阶段 | 优化内容 | request % | response % |
|---|---|---:|---:|
| S0 | 原始基线 | 21.14 | 23.64 |
| S1 | v2 HANDLE 直接构造 READ job，省去通用 request 清零及中间 key copy | 4.70 | 25.10 |
| S2 | batch context 按需初始化 | 4.05 | 25.20 |
| S3 | producer tail/布局/arena 结束位置本地化，按需刷新 head | 4.17 | 18.58 |
| S4 | 一次 reserve/commit，消除 descriptor 发布重复检查 | 4.10 | 12.44 |

上表为 5:5 Server collapsed stacks 的 request handler 和 response publish inclusive 采样权重。

S0 → S4 累计效果（5:5 两轮均值）：

| 指标 | S0 | S4 均值 | 变化 |
|---|---:|---:|---:|
| QPS (M) | 7.905 | 8.214 | +3.91% |
| Server CPU core | 8.864 | 8.585 | −3.15% |
| QPS/core (M) | 0.892 | 0.957 | **+7.29%** |

S1 为主要贡献项（单核吞吐 +4.83%），S3/S4 优化 response publish 路径，S2 边际效果不显著。

### 1.2 热路径诊断计数器移除（`98d064d`）

移除 `tlc_core_get_warm_location_raw` 内 6 个 `atomic_fetch_add` + 3 个 `note_lookup_location()` 调用，以及 SuperNode 中的 `vemb_v16_tlc_note_handle_lookup_miss()`。保留字段声明、`atomic_init` 和冷路径 stats 读取代码。

| 指标 | 移除前 | 移除后 | 变化 |
|---|---:|---:|---:|
| QPS (M) | 8.198 | 8.225 | +0.33% |
| Server CPU core | 8.576 | 8.261 | −3.67% |
| QPS/core (M) | 0.972 | 1.017 | **+4.61%** |

### 1.3 O1: aeron_snapshot seq_cst → acq_rel（`944d866`）

PIO poll 循环中 `aeron_snapshot_readers` 的 3 次 `seq_cst` 原子操作降级：

| 位置 | 原 | 降级后 | 理由 |
|---|---|---|---|
| reader `fetch_add` | seq_cst | acq_rel | acquire 保证后续 load 有序，release 保证 writer 可见 |
| reader `load(&snapshot)` | seq_cst | acquire | 看到 writer exchange(release) 的新 buffer |
| reader `fetch_sub` | seq_cst | release | channel 读取在 decrement 前完成 |
| writer `load(&readers)` | seq_cst | acquire | 看到 reader fetch_sub(release) |
| writer `load(&snapshot)` | seq_cst | acquire | 单线程写入 |
| writer `exchange(&snapshot)` | seq_cst | release | 发布 buffer 内容 |

每轮省 2 个 ARM `DMB ISH` 全屏障。

| 指标 | O1 前 | O1 后 | 变化 |
|---|---:|---:|---:|
| QPS/core (M) | 1.017 | 1.023 | **+0.61%** |

### 1.4 累计效果（`cfd2cf4` → `944d866`，5:5）

| 指标 | 原始基线 | 当前 | 累计变化 |
|---|---:|---:|---:|
| QPS (M) | 7.905 | 8.227 | +4.07% |
| Server CPU core | 8.864 | 8.175 | −7.77% |
| QPS/core (M) | 0.892 | 1.023 | **+14.69%** |

### 1.5 累计效果（含 O2-C 配置调优）

| 指标 | 原始基线 (5:5) | 4:6 | 3:4 |
|---|---:|---:|---:|
| QPS (M) | 7.905 | 8.167 | 8.031 |
| Server CPU core | 8.864 | 7.292 | 6.056 |
| QPS/core (M) | 0.892 | 1.120 | 1.326 |
| **QPS/core 累计变化** | — | **+25.6%** | **+48.7%** |
| P99 (ms) | — | 1.215 | 1.463 |

注：O2-C 是配置调优（改变 PIO:SNW 比例），非代码改动。4:6 延迟友好（P99 +1.3%），3:4 CPU 效率最高（P99 +22%）。

## 2. 当前 CPU 分布

来源：`perf/p5_s5_svr0-15_uniform_20260920_141408/server/server.svg`，8.227M QPS，8.175 cores。

Server 采样分为两大线程组：PIO (60.23%) 和 SuperNode (39.06%)。

### 2.1 PIO 线程（60.23% inclusive）

| 函数 | inclusive % | self % | 说明 |
|---|---:|---:|---|
| `proxy_io_pool_thread_main` | 60.23 | **~32.5** | self 为结构性原子操作开销（LTO 内联 channel_acquire CAS + channel_release atomic_sub），非空转 |
| `drain_completions` | 15.50 | ~3.1 | SuperNode 完成队列排空 + response 发布 |
| `vemb_v16_aeron_publish_batch_response` | 12.42 | — | drain_completions 子项，response 发布 |
| `vemb_v16_aeron_poll_shm_requests` | 9.34 | — | UB ring 请求读取 + 分发 |
| `vemb_v16_proxy_handle_batch_request` | 4.59 | — | poll_shm_requests 子项，batch 请求处理 |
| `drain_job_return_queues` | 2.43 | — | job 对象回收 |
| `__nanosleep` | 0.14 | 0.14 | 退避后的实际 sleep |

self ~32.5% 的计算：`proxy_io_pool_thread_main` inclusive 60.23% 减去其所有子函数之和（~27.7%）。这个 self time 对应 LTO 内联后的 `proxy_io_channel_acquire`（CAS 循环）、`proxy_io_channel_release`（`atomic_sub`）、`aeron_snapshot_readers` 的 `fetch_add/fetch_sub`，以及 `cpu_relax()` yield 指令——**主要是结构性原子操作开销，非简单空转**。

### 2.2 SuperNode 线程（39.06% inclusive）

| 函数 | inclusive % | self % | 说明 |
|---|---:|---:|---|
| `supernode_pool_thread_main` | 39.06 | **~3.9** | self 为空轮询 |
| `drain_shard_queues` | 33.55 | — | 分片任务消费 |
| `vemb_v16_supernode_handle_vemb_job` | 25.41 | — | job 处理（含 TLC 查找） |
| `vemb_v16_tlc_get_handle_stable_read` | 23.79 | ~1.0 | handle 稳定读取 |
| `tlc_core_get_warm_location_raw` | 22.75 | — | warm 区域查找（有效功） |
| `warm_lookup_region` | 1.48 | — | 区域匹配 |
| `vemb_v16_supernode_flush_completion_batch` | 1.34 | — | completion 批量写回 |
| `publish_shard_job_batch` | 1.14 | — | job 分发到 shard 队列 |
| `syscall` (futex_wait) | ~1.6 | — | idle wait（已有退避生效） |

### 2.3 分布概览

```
                    Total CPU 100%
┌──────────────────────────────────────────────────────────┐
│           PIO 60.23%            │     SuperNode 39.06%   │
│                                 │                        │
│  ┌─────────┐  ┌──────────────┐  │  ┌──────────────────┐  │
│  │ IDLE    │  │ drain_compl  │  │  │ drain_shard_     │  │
│  │ 32.5%   │  │ 15.50%       │  │  │ queues 33.55%    │  │
│  │         │  │ ┌──────────┐ │  │  │ ┌──────────────┐ │  │
│  │ 结构性  │  │ │publish   │ │  │  │ │handle_vemb_  │ │  │
│  │ 原子操  │  │ │_batch_   │ │  │  │ │job 25.41%    │ │  │
│  │ 作开销  │  │ │response  │ │  │  │ │ ┌──────────┐ │ │  │
│  │ (LTO    │  │ │12.42%    │ │  │  │ │ │warm_loc  │ │ │  │
│  │ inlined)│  │ └──────────┘ │  │  │ │ │22.75%    │ │ │  │
│  └─────────┘  └──────────────┘  │  │ │ │(有效功)  │ │ │  │
│  ┌────────┐  ┌────────┐        │  │ │ └──────────┘ │ │  │
│  │poll_shm│  │drain_  │        │  │ └──────────────┘ │  │
│  │9.34%   │  │job_ret │        │  │ ┌────┐ ┌──────┐  │  │
│  │        │  │2.43%   │        │  │ │idle│ │futex │  │  │
│  └────────┘  └────────┘        │  │ │3.9%│ │1.6%  │  │  │
│                                 │  │ └────┘ └──────┘  │  │
└─────────────────────────────────┴──┴──────────────────┘  │
                                                           │
```

## 3. 优化方向

### O2. PIO 空轮询退避策略优化

**目标 CPU 减少**：初始预估 ~15–20%，实测修正为 **~1%**。

#### O2 认知修正

初始分析认为 PIO 的 32.5% self time 是 `cpu_relax()` 空转浪费。**回归测试证实这个判断有误**：

PIO self time 的本质是**结构性原子操作开销**——LTO 将 `proxy_io_channel_acquire`（CAS）和 `proxy_io_channel_release`（`atomic_sub`）内联，使其 cycles 计入 `proxy_io_aeron_poll_thread_main` 的 self time。每轮 poll 迭代的最小原子操作成本：

| 操作 | 次数/迭代/线程 | 说明 |
|---|---:|---|
| `aeron_snapshot_readers` fetch_add | 1 | acquire reader refcount |
| `aeron_snapshot_readers` fetch_sub | 1 | release reader refcount |
| `channel_acquire` CAS | ≥1 per channel | 尝试获取 channel ownership |
| `channel->active` load | 1 per channel | 检查 channel 状态 |
| `channel_release` atomic_sub | 1 per channel | 释放 channel ownership |
| misc loads/stores | ~2 | snapshot pointer load, control flow |

以 4 channels / 5 PIO 线程为例，4 个有 channel 的线程每轮 ~6 次原子操作，1 个空线程也至少 2 次（readers inc/dec）。这些是**有效功的固定税**——只要 channel 有活跃 client，PIO 线程就必须轮询，且无法通过退避绕过。

#### O2 回归数据

基线：`944d866`（O1 之后），5:5 PIO:SNW。

| 指标 | 基线 (141408) | R1 退避优化 (160912) | R2 scan skip (161918) | R2 vs 基线 |
|---|---:|---:|---:|---:|
| QPS (M) | 8.227 | 8.219 | 8.251 | +0.29% |
| Server CPU core | 8.084 | 8.061 | 8.107 | +0.28% |
| QPS/core (M) | 1.018 | 1.020 | 1.018 | +0.02% |
| P99 (ms) | 1.199 | 1.199 | 1.199 | 无变化 |

- **R1**（三级退避，SPIN=32 NAP=96，衰减重置）：QPS/core +0.21%，在噪声范围内
- **R2**（R1 + 空闲线程跳过整个 channel scan）：QPS/core +0.02%，无显著变化

#### 为什么退避几乎无效

4 clients 通过 `sequence % pio_count` round-robin 分配到 PIO 线程：4 个线程各分到 1 个 channel，始终有工作；第 5 个线程 0 channels，是唯一受益于退避的线程。退避只节省了 1/5 的空轮询开销，而这 1 个线程的全部 CPU 占比仅约 `32.5% / 5 ≈ 6.5%`。

#### O2 实际杠杆：减少 PIO 线程数

真正能大幅降低 PIO CPU 的方案是**减少 PIO 线程数**从 5 到 3–4：

| 配置 | 预期效果 | 风险 |
|---|---|---|
| PIO=4, SNW=6 | 消除 1 个完全空闲的 PIO 线程，减少 ~6% CPU；多出 1 个 SNW 可分摊 SN 工作 | 4 channels / 4 PIO = 均匀分配，应无吞吐损失 |
| PIO=3, SNW=7 | 消除 2 个 PIO 线程（含 1 个有 channel 的），减少 ~12% CPU | 3 PIO 处理 4 channels，1 个 PIO 负载翻倍，可能成为瓶颈 |
| PIO=2, SNW=8 | 激进配置，各 PIO 处理 2 channels | 可能 poll 延迟增大 |

#### O2-C 回归数据

| 指标 | 基线 5:5 (141408) | 4:6 (170047) | 3:4 (165605) |
|---|---:|---:|---:|
| QPS (M) | 8.227 | 8.167 | 8.031 |
| Server CPU core | 8.084 | 7.292 | 6.056 |
| Server QPS/core (M) | 1.018 | 1.120 | 1.326 |
| **QPS/core 变化** | — | **+10.0%** | **+30.3%** |
| P99 (ms) | 1.199 | 1.215 | 1.463 |
| avg_lat (ms) | 0.669 | 0.685 | 0.699 |

CPU 分布对比：

| 组件 | 5:5 | 4:6 | 3:4 |
|---|---:|---:|---:|
| PIO inclusive | 60.23% | 52.95% | 47.82% |
| **PIO self** | **32.50%** | **20.03%** | **11.11%** |
| SN inclusive | 39.06% | 46.49% | 51.69% |
| SN self | 3.90% | 5.60% | 3.57% |
| drain_completions | 15.50% | 16.52% | 18.06% |
| poll_shm_requests | 9.34% | 10.02% | 10.89% |
| drain_job_return | 2.43% | 6.38% | 7.76% |
| warm_location_raw | 22.75% | 22.02% | 27.01% |

**4:6 分析**：4 PIO 对 4 channels 实现 1:1 映射，消除空闲线程。QPS/core +10%，P99 几乎不变（1.215 vs 1.199），是延迟敏感场景的安全选择。PIO self 从 32.5% 降到 20%，SN self 略涨到 5.6%（6 个 SN 线程有 2 个分不到足够工作）。

**3:4 分析**：QPS/core **+30.3%**，V2 全系列最大单项优化。1 个 PIO 线程需处理 2 channels（`4 channels % 3 PIO = 1 线程双倍负载`），CPU 效率大幅提升但 P99 涨 22%。`drain_job_return_queues` 从 2.43% 涨到 7.76%，3 个 PIO 回收原来 5 个线程分摊的 job 量。

#### O2 设计指导：线程数与退避互补

回归数据揭示了线程配置与退避策略的互补关系：

- **线程数 > channel 数**（如 5:5 对 4 clients）：多出的空闲线程 busy-poll 浪费 CPU，退避代码在此场景下有效——但 5:5 回归仅 +1% 因为只有 1 个线程空闲。**线程数越多超出 channel 数，退避越重要。**
- **线程数 = channel 数**（如 4:6）：无空闲线程，退避几乎不触发，CPU 自然紧凑。
- **线程数 < channel 数**（如 3:4）：所有线程始终有工作，退避完全不触发，CPU 效率最高但延迟上升。

结论：**退避代码应保留**——它对线程数 > channel 数的运行时场景是必要的安全网（动态 client 连接/断开、client 数不可预知时）。同时应在服务启动时根据 channel 数自动调整 PIO 线程数（`PIO = min(configured_pio, channel_count)`）。

#### O2 代码变更（已测试，已撤销）

以下代码改动经过两轮回归测试，效果不显著（5:5 +~1%，3:4 +0%），已撤销：

- 三级退避（SPIN=32, NAP=96, deep=50us）+ channel scan skip + 衰减重置
- 5:5 配置下仅 1/5 空闲线程受益，4 个有 channel 的线程始终 busy，退避无法触发
- 3:4 配置下所有线程均有工作，退避完全不触发

**后续不应重复此方向**——PIO self time 的本质是结构性原子操作开销，非空转浪费。

### O3. `drain_completions` / `publish_batch_response` 进一步批量化

**目标 CPU 减少**：~3–5% total（3:4 profile 下 self 14.83%）

**现状（3:4 profile）**：`drain_completions` inclusive 18.06%，其中 `vemb_v16_aeron_publish_batch_response` self 14.83%（是 3:4 下仅次于 warm_location_raw 的第二大 self 热点）。S3/S4 已优化 arena layout 和一次 reserve/commit，从 S0 的 23.6% 降到当前水平。

**perf annotate 热点定位（3:3 profile，`publish_batch_response` self 16.73%）**：

| 热点 | 指令 | 占 publish 函数 % | 源码 |
|---|---|---:|---|
| `atomic_thread_fence(release)` | `DMB ISH` | 49.25% | `batch_ring.h:153` |
| `atomic_store(tail, release)` | `STLR` | 42.44% | `batch_ring.h:177` |

两个 release barrier 合计 91.69%，等待 UB 跨节点 NC store drain。

**方案**：

| 方案 | 改动 | 预期效果 |
|---|---|---|
| A. 攒 completion 批量 publish | `batch_context_complete` 不立即触发 publish，攒到 N 个或 batch 全部完成后一次性 publish | 减少 response ring 的 fence 和 tail 更新次数 |
| B. 减少 per-response release fence | 当前可能每个 response frame 一次 release store；改为 batch 结束时一次 release | 每 batch 省 (N-1) 次 release fence |
| C. response ring commit 合并 | 多个 response descriptor 一次 commit tail advance | 减少 consumer 端的 head acquire 次数 |

**实施要点**：
- 需确认 `batch_context_complete` 的调用模式：如果每个 completion 独立到达（来自不同 SN 线程），则无法 batch；如果同一 PIO 轮次内多个 completion 同时就绪，可以攒起来。
- S4 已实现一次 reserve/commit，进一步优化的边际收益取决于 per-frame 还是 per-batch 的 fence 粒度。
- wire format 和 consumer 端不能假设 batch 原子发布。

#### O3-A: 移除 commit marker + 消除冗余 fence

**背景**：commit marker 是在 UB 配置不确定时期引入的防御性校验机制。当前 UB 配置已明确：NC write + CC read，`atomic_store(tail, release)` → `atomic_load(tail, acquire)` 建立完整的 happens-before 链。在此前提下，commit marker 和保护它的 fence 是冗余的。

**happens-before 证明**：

```
Producer                                   Consumer
--------                                   --------
store ①: sve_streaming_load → arena body
store ②: commit[0..7] = batch_id          
store ③: sve_streaming_load → descriptor   
store ④: atomic_store(tail, release)  ──→  load ⑤: atomic_load(tail, acquire)
                                           load ⑥: memcpy(descriptor)
                                           load ⑦: decode(arena body)
                                           load ⑧: verify commit == batch_id
```

④ release → ⑤ acquire 保证 ①②③ 在 ⑤ 之后的 ⑥⑦⑧ 看来全部完成。因此：
- load ⑦ 一定看到 store ① 的完整 body — **不需要 commit marker 验证**
- load ⑧ 一定等于 desc.batch_id — **校验永远 pass**

**commit marker 存在的历史原因**（`client_sdk.c:6229-6231` 注释）：

> A local CC mapping can observe the published descriptor before a reused remote-NC arena line becomes readable.

这是 UB 配置不正确时的 race：descriptor ring 和 arena 在不同 coherence domain，CC mapping 的 descriptor 可见性快于 NC mapping 的 arena 内容。正确配置下此 race 不存在。

**改动**：
1. 移除 `batch_ring.h:153` 的 `atomic_thread_fence(release)` — 它唯一的作用是保证 body store 在 commit store 之前可见，去掉 commit 后无需此 fence
2. 移除 `batch_ring.h:154-163` 的 volatile commit 写入
3. `batch_arena_publish` 参数去掉 `batch_id`（commit 不再需要，batch_id 已在 descriptor 中）
4. `batch_response_encode`/`batch_request_encode` 去掉尾部 `put_u64(batch_id)`
5. `batch_response_decode`/`batch_request_decode` 去掉尾部 `get_u64()` 校验
6. `batch_request_encoded_len` 去掉 `+ VEMB_V16_BATCH_COMMIT_BYTES`
7. `VEMB_V16_BATCH_COMMIT_BYTES` 宏删除
8. consumer 校验简化：`view.batch_id != desc.batch_id` 保留（desc 内部一致性检查），commit 相关长度计算移除
9. UT 对应更新

**预期效果**：从 2 个 barrier（49%+42%）降到 1 个（~42%），`publish_batch_response` CPU 减少约 50%。3:3 profile 下 publish self 从 16.73% 降到 ~8-9%。

**验证方案**：使用 `ub_cc_nc_visibility_ut` 的 `frame-writer-nc` / `frame-reader-cc` 模式在真实双节点运行 100 万次迭代，确认无 `FRAME_VISIBILITY_FAILURE`。通过后进行 benchmark 回归。

#### O3-A 回归结果

**测试配置**：3:3 PIO:SNW，Server CPU 0-15，4 clients，30s TEST_TIME。

| 指标 | O3-A 前 | O3-A 后 | 变化 |
|---|---:|---:|---:|
| QPS (M) | 7.867 | 7.867 | +0.00% |
| CPU cores | 5.821 | 5.821 | +0.00% |
| QPS/core (M) | 1.362 | 1.362 | +0.00% |

**无性能变化**。`perf annotate` 确认原因：移除 `atomic_thread_fence(release)` 后，91.33% 的采样全部转移到剩余的 `atomic_store(tail, release)` 对应的 `STLR` 指令。两个 barrier 的 stall 本质相同——等待 NC store drain 到达远端 UB——移除一个只是让另一个承担全部等待时间。

**结论**：commit marker 移除和 fence 消除作为代码清理是正确的（简化 wire format、减少冗余），但不产生性能收益。NC store drain 延迟是硬件瓶颈，barrier 数量不影响总等待时间。要消除此瓶颈需改变 NC/CC 映射方向。

#### O3-B: UB 设备方向反转（CC write → NC read）

**动机**：O3-A 证实 NC store drain 是不可压缩的硬件瓶颈。当前模式下 producer 写 NC（import device, O_RDWR fallback O_SYNC），consumer 读 CC（export device, O_RDWR 成功）。ARM `STLR` 在 NC 映射上必须等待跨节点 ack，这是 91% 热点的根因。

反转方向让 producer 写 CC（export device）、consumer 读 NC（import device），`STLR` 在 CC 映射上只需 flush 本地 store buffer，延迟从跨节点 round-trip 降到本地 cache 操作。

**设备映射**（对称关系：111/112 export dev1..4 → peer import dev5..8）：

| 方向 | 原 provider@111 | 原 client@112 | 说明 |
|---|---|---|---|
| Request | dev3 (export) | dev7 (import) | CLI NC write → Server CC read |
| Response | dev6 (import) | dev2 (export) | Server NC write → CLI CC read |
| Warm | dev4 (export) | dev8 (import) | 不变 |

反转 = 交换 provider_path 和 client_path 的设备编号，复用同一对物理设备。

##### 实验 1: 全反转（CC write → NC read 双向）

Request 和 Response 都反转：producer 用 export (CC)，consumer 用 import (NC)。

YAML: `examples/vemb_v16_ub_peer_view_112_to_111_cc_write_nc_read.yaml`

```
SERVER_REQUEST_UB_PATH=/dev/obmm_shmdev7   # 111 import (NC read)
SERVER_RESPONSE_UB_PATH=/dev/obmm_shmdev2  # 111 export (CC write)
CLIENT_REQUEST_UB_PATH=/dev/obmm_shmdev3   # 112 export (CC write)
CLIENT_RESPONSE_UB_PATH=/dev/obmm_shmdev6  # 112 import (NC read)
```

| 指标 | 原 (NC→CC) | 全反转 (CC→NC) | 变化 |
|---|---:|---:|---:|
| QPS (M) | 7.867 | 0.219 | **-97.2%** |
| CPU cores | 5.821 | 3.224 | -44.6% |
| QPS/core (M) | 1.362 | 0.069 | **-95.0%** |
| avg_lat (ms) | — | 35.50 | — |
| P99 (ms) | — | 76.29 | — |

**灾难性下降**。perf 确认瓶颈在 `poll_shm`：Server PIO 线程的 request ring busy-poll 变成 NC read，每次 `atomic_load(tail, acquire)` 都是跨节点 round-trip（~数百ns），即使无新数据也要穿透到远端。

**根因分析**：NC write 的开销是 **per-message**（有数据才写），NC read poll 的开销是 **per-iteration**（空转也付代价）。Busy-poll consumer 的迭代频率远高于实际消息到达频率，NC read poll 的总开销远大于 NC write 的 store drain。

##### 实验 2: 混合模式（request NC→CC 不变，response CC→NC 反转）

只反转 response 方向：Server response 写变 CC（消除 91% STLR 热点），request 保持原样（Server CC read poll 不受影响）。CLI response read 变 NC，但 CLI 是"发请求→等响应"模式，不像 Server PIO 那样长时间空转 poll。

YAML: `examples/vemb_v16_ub_peer_view_112_to_111_hybrid.yaml`

```
SERVER_RESPONSE_UB_PATH=/dev/obmm_shmdev2  # 111 export (CC write)
CLIENT_RESPONSE_UB_PATH=/dev/obmm_shmdev6  # 112 import (NC read)
# request/warm 保持原配置不变
```

**3:3 PIO:SNW 结果：**

| 指标 | 原 (NC→CC) 3:3 | 混合 3:3 | 变化 |
|---|---:|---:|---:|
| QPS (M) | 7.867 | 6.976 | -11.3% |
| CPU cores | 5.821 | 5.289 | -9.1% |
| QPS/core (M) | 1.362 | 1.341 | **-1.5%** |
| avg_lat (ms) | — | 0.82 | — |
| P99 (ms) | — | 1.40 | — |

QPS 绝对值降 11%，但 CPU 也降 9%，QPS/core 仅降 1.5%。Server 端 `vemb_v16_aeron_publish_batch_response` 从主要热点（函数内 91% 在 STLR）降到仅占全局 ~1.5%。

Server perf report（混合 3:3）：

| 函数 | 占比 | 说明 |
|---|---:|---|
| `tlc_core_get_warm_location_raw` | ~29% (3x SNW) | 有效业务逻辑 |
| `proxy_io_pool_thread_main` | ~20% (3x PIO) | IO 主循环 |
| `vemb_v16_aeron_poll_shm_requests` | ~18% (3x PIO) | request NC read poll |
| `vemb_v16_aeron_publish_batch_response` | ~1.5% (3x PIO) | response CC write |

**2:3 PIO:SNW 结果：**

减少一个 PIO 线程，进一步削减 request NC read poll 开销。

| 指标 | 原 (NC→CC) 3:3 | 混合 2:3 | 变化 |
|---|---:|---:|---:|
| QPS (M) | 7.867 | 6.325 | -19.6% |
| CPU cores | 5.821 | 4.626 | -20.5% |
| QPS/core (M) | 1.362 | 1.407 | **+3.3%** |
| avg_lat (ms) | — | 0.90 | — |
| P99 (ms) | — | 1.94 | — |

**QPS/core 提升 3.3%**，是混合模式下的最佳结果。减少 PIO 线程省掉一份 poll_shm 开销，response CC write 的收益得到放大。

##### O3-B 结果汇总

| 配置 | QPS (M) | CPU cores | QPS/core (M) | vs 基线 |
|---|---:|---:|---:|---:|
| 原 NC→CC 3:3 (基线) | 7.867 | 5.821 | 1.362 | — |
| 全反转 CC→NC 3:3 | 0.219 | 3.224 | 0.069 | **-95.0%** |
| 混合 3:3 | 6.976 | 5.289 | 1.341 | -1.5% |
| **混合 2:3** | **6.325** | **4.626** | **1.407** | **+3.3%** |

##### O3-B 结论

1. **全反转不可行**：busy-poll consumer 的 NC read 开销是 per-iteration 的灾难性开销，不适合高频空转场景。
2. **混合模式可行**：只反转 response 方向（Server CC write, CLI NC read），消除 Server 端 91% STLR 热点。QPS/core 在 3:3 下持平（-1.5%），在 2:3 下提升 +3.3%。
3. **关键不对称性**：Server PIO 线程是长时间 busy-poll（request ring 必须用 CC read），CLI response poll 是短暂的"发请求→等响应"模式（NC read 可接受）。这种不对称性使得混合模式在 response 方向可行但 request 方向不可行。
4. **混合模式改变了 Server 端的 CPU 瓶颈格局**：response publish 从 P1 热点 (~15% self) 降到噪声水平 (~1.5%)，热点转移到实际业务逻辑 (`warm_location_raw` ~29%) 和 request poll (~18%)。后续优化应聚焦这两个方向。

### O8. Client 侧瓶颈分析（混合 2:3 模式）

混合 2:3 模式下 Server 端瓶颈大幅缓解（response publish 从 ~15% 降到 ~1.5%），系统瓶颈转移到 Client 侧。

**Client perf profile** (`perf/p2_s3_svr0-15_uniform_20260921_003650/client/client.perf.data`)：

| 排名 | 函数 | self % | 归属 | 调用链 |
|---:|---|---:|---|---|
| 1 | `sve_streaming_load_f32` | 35.44% | warm vector NC 读回 | `sdk_ub_read_warm_vector` → `common_core_handle_completion` |
| 2 | `vemb_v16_aeron_batch_poll_response` | 19.38% | response ring NC read poll + decode | `sdk_handle_session_poll_v2` |
| 3 | `sdk_handle_session_route_requests` | 19.21% | 请求路由 slot 扫描 | `common_core_drive_sessions` |
| 4 | `__kernel_gettimeofday` (vDSO) | 7.47% | 统计打点 | `common_core_account_read` |
| 5 | `__udivti3` | 4.20% | 128-bit 除法 | `getMonotonicNs_aarch64` |
| 6 | `sdk_ub_poll` | 1.52% | UB poll wrapper | — |
| 7 | `sdk_handle_session_flush_owner` | 1.21% | flush | — |

##### O-C1: `sdk_handle_session_route_requests` 空 slot 扫描（19.21%）——已实现

**热点细节**：`perf annotate` 显示 87.72% 采样落在 `add x19, x19, #0x1`（循环计数器递增），9.66% 在 `ldrb w0, [x27, #193]`（读取 `request->state`）。这是一个 O(4096) 线性扫描 `requests[]` 数组查找 `state == SDK_HANDLE_SESSION_REQUEST_ROUTE` 的 slot，绝大多数 slot 为空。

```
   87.72 :   43b448: add     x19, x19, #0x1      ← 循环计数器 ++
    9.66 :   43b330: ldrb    w0, [x27, #193]      ← load request->state
    0.73 :   43b338: b.ne    43b448               ← state != ROUTE, skip
    0.31 :   43b454: b.ne    43b330               ← 继续循环
```

**根因**：`SDK_HANDLE_SESSION_MAX_PENDING = 4096`，但 active slot 通常只有 batch_size（几十个），97%+ 的迭代在做无用的 state load + compare。

**实现方案**：intrusive singly-linked list（`route_next` 字段 + `route_head`），将 O(N=4096) 扫描变为 O(active≈batch_size) 遍历。详见上方"O-C1 实现"一节。

**结果**：`route_requests` self% 从 4.42% 降到 1.74%（-60.6%）。

##### O-C2: `getMonotonicNs_aarch64` 128-bit 除法（4.20%）——已实现

**热点细节**：`__udivti3` 全部来自 `getMonotonicNs_aarch64`（`monotonic.c:144-145`）：
```c
__uint128_t ns = (__uint128_t)__cntvct() * 1000000000ULL;
return (monotime)(ns / mono_cntfrq_hz);   // ← 128-bit 软件除法
```

每次 `vemb_v16_monotonic_ns()` 调用产生一次 `__udivti3`（libgcc 软件除法，~40 条指令）。在 `sdk_handle_session_route_requests` 内，每个 active request 调用 **6–8 次** `monotonic_ns()`（`route_start`, `route_done`, `channel_start`, `channel_end`, `owner_path_start`, `l0_submit_start`, `l0_submit_end` 等诊断时间戳）。

**实现方案**：移除所有诊断 timing 调用，保留 18 个功能性 result/correctness 计数器。详见上方"O-C2 实现"一节。

**结果**：`__udivti3` self% 从 2.95% 降到 0.66%（-77.6%），剩余来自功能性 deadline/poll 调用。

##### O-C3: `common_core_account_read` gettimeofday（7.47%）

全部来自 memtier_benchmark 的 `common_core_account_read` 统计回调，每个 completion 调用一次 `gettimeofday`。

**性质**：benchmark 客户端自身开销，不影响生产环境。但会限制 benchmark 测出的 QPS 上限。

**改进方案**：使用 `--hide-histogram` 或降低统计采样率。预期释放 ~7.47% client CPU 给请求驱动。

##### O-C4: `vemb_v16_aeron_batch_poll_response` NC read + decode（19.38%）

**热点细节**：
- LDAR（NC acquire poll）只占 0.13%，说明空转 poll 不是主要开销
- 36.01% 和 35.88% 落在 `vemb_v16_resp_decode` 的 `cmp` 指令，4.95% + 3.88% 在 response field store
- 1.28% 在函数 prologue（函数调用开销）

**根因**：NC read poll 的实际开销不在空转（CLI 的"发请求→等响应"模式下空转少），而在成功 poll 到 response 后的 **decode 过程**——decode 中的每次 load 都穿越 UB NC 链路。

**改进方案**：poll 成功后，先 `sve_streaming_load` 整块将 response frame 拷贝到本地栈（一次 bulk NC read），再 decode 本地副本（避免 decode 中多次散装 NC load）。预期将 decode 路径的 NC load 延迟从 O(field_count × NC_latency) 降到 O(1 × NC_bulk_latency + local_decode)。**已实现，poll_v2 self 26.8%→21.8%（-18.7%），P99 -8.9%**。

##### O-C5: `sve_streaming_load_f32` warm vector 读回（35.44%）

全部来自 `sdk_ub_read_warm_vector`——从 warm region（dev8, NC import device）读向量数据。调用链：`sdk_ub_read_warm_vector` → `common_core_handle_completion` → `sdk_handle_session_finish`。

**性质**：业务有效功——benchmark 必须读回向量数据以验证正确性。是 NC import 上的 bulk SVE copy，延迟受限于 UB 链路带宽。

**可探讨方向**：
- 如果 client 不需要完整向量数据（只需 handle），可跳过向量读回
- 预测性 prefetch 降低单次读取启动延迟
- 此项优化空间有限，属于有效功的硬下限

##### O8 综合评估

混合 2:3 模式下 Client 侧可优化空间合计约 **31%**：

| 编号 | 方向 | 当前 self % | 优化后 | 复杂度 |
|---|---|---:|---:|---|
| O-C1 | route_requests slot 扫描 → linked list | 4.42% | **1.74%** | 中，**已实现** |
| O-C2 | `__udivti3` → 移除诊断 timing | 2.95% | **0.66%** | 低，**已实现** |
| O-C3 | gettimeofday 统计 → 降采样 | 7.47% | ~0% | benchmark 配置 |
| O-C4 | response decode NC 散装读 → bulk copy + local decode | 19.38% | **~14%** | 中，**已实现** |
| O-C5 | warm vector NC 读回 | 35.44% | ~35% | 有效功，不可消除 |

释放 ~31% client CPU 后，client 线程可驱动更多请求给 server，有望在混合 2:3 基础上进一步提升 QPS/core。其中 O-C1（slot 扫描）和 O-C2（128-bit 除法）是最佳性价比方向，实现简单且收益确定。

#### O-C1 实现：intrusive linked list 替代 O(4096) 线性扫描

**改动**：在 `sdk_handle_session_request_t` 中新增 `uint32_t route_next` 字段，维护 `state == ROUTE` 的 slot 组成的 intrusive singly-linked list。`route_head` 存储在 `vemb_v16_client_handle_session` 结构体中，哨兵值 `SDK_HANDLE_SESSION_NO_ENTRY = UINT32_MAX`。

**状态机**：`FREE(0) → ROUTE(1) → L0(2)/V1(3) → FREE(0)`
- 进入 ROUTE 时（`common_core_submit` / `apply_response` re-route）：slot 追加到 `route_head` 链表
- `route_requests()` 遍历链表而非扫描 4096 slots，处理完成后将 slot 从链表中移除

**代码改动**（`vemb_v16_client_sdk.c`）：
- `sdk_handle_session_request_t` 新增 `route_next` 字段
- `sdk_handle_session_route_requests()` 从 `for (i=0; i<MAX_PENDING; i++)` 改为 `while (head != NO_ENTRY)` 链表遍历
- 所有将 request state 设为 `ROUTE` 的地方追加链表 append 操作
- `finish()` 中 state 恢复 FREE 时不需要额外操作（链表只在 route_requests 遍历时消费）

**Client flamegraph 验证**（3:3 PIO:SNW，混合 2:3 UB 模式）：

| 函数 | O-C1 前 self % | O-C1 后 self % | 变化 |
|---|---:|---:|---:|
| `sdk_handle_session_route_requests` | 4.42% | 1.74% | **-60.6%** |

注：混合 2:3 模式下 client route_requests 的采样权重远低于原始 O8 分析中的 19.21%（该数据来自 NC→CC 模式的 2:3 profile），但相对改善一致。

#### O-C2 实现：移除 SDK 诊断 timing，保留功能性计数器

**改动**：移除 `vemb_v16_client_sdk.c` 中所有用于诊断的 `vemb_v16_monotonic_ns()` 调用（per-sample latency histogram、`poll_at` 时间戳、`route_start_ns`/`channel_end_ns` 等诊断计时），消除热路径中的 `getMonotonicNs_aarch64` → `__udivti3` 128-bit 软件除法。

**保留项**：
- 18 个功能性 result/correctness 计数器（`submitted/completed/ok/not_found/errors/retries/v1_requests/v2_items/v2_frames/fallback_v1/channel_reopen/handle_region_count/materialize_ok/materialize_fail/materialize_bytes/pending_peak/pending_current/active_groups_current`）
- `vemb_v16_client_get_owner_stats()` 公开 API 和 `vemb_v16_client_owner_stats_snapshot_t` 类型
- `owner_channel_snapshot_id[]`（功能性拓扑追踪）
- 功能性 `vemb_v16_monotonic_ns()` 调用：deadline 计算、UB poll spin loop timeout

**移除项**：
- `poll_at` 时间戳参数（`poll()` 从 4 参数变 3 参数）
- `flush_owner` 诊断计时（从 7 参数变 5 参数）
- per-sample latency histogram 相关类型：`vemb_v16_client_owner_region_stats_t`、`COPY_LATENCY_BUCKETS`、`OWNER_REGION_STATS_MAX`
- `owner_region_stats[]`、`last_handle_owner_id` 结构体字段
- 所有 diagnostic `fprintf` 函数

**代码改动**：
- `vemb_v16_client_sdk.h`：stats 类型从多层嵌套（含 region_stats、latency 桶）精简为 18 字段扁平 `vemb_v16_client_owner_stats_t`
- `vemb_v16_client_sdk.c`：移除约 200 行诊断代码，内部 `sdk_owner_stats_t` 与公开类型字段对齐（`_Static_assert` 校验 layout），`get_owner_stats()` 使用 `memcpy` + 就地计算 `pending_current`/`active_groups_current`
- `memtier_benchmark/vemb_v16_aeron_runner.cpp`：移除 `common_core_merge_owner_stats`、`common_core_merge_region_stats` 和诊断 fprintf 块

**Client flamegraph 验证**（3:3 PIO:SNW，混合 2:3 UB 模式）：

| 函数 | O-C2 前 self % | O-C2 后 self % | 变化 |
|---|---:|---:|---:|
| `__udivti3` | 2.95% | 0.66% | **-77.6%** |
| `getMonotonicNs_aarch64` | 3.08% | 0.69% | **-77.6%** |

剩余 0.66% 来自功能性 deadline 计算和 UB poll spin loop 中的 `monotonic_ns()` 调用，不可移除。

#### O-C1 + O-C2 综合结果

**测试配置**：3:3 PIO:SNW，Server CPU 0-15，4 clients，混合 2:3 UB 模式，30s TEST_TIME。

| 指标 | O-C1/C2 前 (084405) | O-C1/C2 后 (094320) | 变化 |
|---|---:|---:|---:|
| QPS (M) | 6.279 | 6.186 | -1.5% |
| Client CPU cores | 4.655 | 4.653 | -0.05% |
| Client QPS/core (M) | 1.387 | 1.365 | -1.6% |
| avg_lat (ms) | 0.948 | 0.971 | +2.4% |
| P99 (ms) | 1.703 | 1.623 | **-4.7%** |

**分析**：QPS 略降 1.6% 在 run-to-run 噪声范围内（~2%），但 P99 延迟改善 4.7%。Client CPU 基本不变——O-C1/O-C2 释放的 CPU 被更高效的请求路由和更少的 cache pollution 所替代。关键改善在 CPU profile 分布：

| 函数 | 前 self % | 后 self % | 说明 |
|---|---:|---:|---|
| `route_requests` | 4.42% | 1.74% | O-C1: 线性扫描 → 链表 |
| `__udivti3` | 2.95% | 0.66% | O-C2: 诊断 timing 移除 |
| 合计释放 | — | — | **~4.97 个百分点** |

O-C1 + O-C2 合计释放约 5 个百分点的 client CPU 采样权重给实际业务逻辑。P99 改善表明减少的 cache pollution 和指令数降低了尾延迟。

#### O-C4 实现：response decode 前 bulk NC copy

**改动**：修改 `vemb_v16_aeron_batch_poll_response()`（`vemb_v16_client_sdk.c`），在 `batch_response_decode()` 之前将整个 NC response frame 通过 `sve_streaming_load()` 批量拷贝到栈上本地缓冲区，decode 改为读本地副本而非逐字段穿越 NC 链路。

**原理**：原始代码中 `batch_response_decode()` 在 NC 映射内存上执行多次散装 load（`get_u32`/`get_u64`/`get_u16` + 每个 item 的 `vemb_v16_resp_decode`），每次 load 均穿越 UB NC 链路。Server 端（v1 路径）已采用类似的 bulk copy 策略。改为一次 SVE streaming load 批量拷贝后在 L1 cache 中 decode，将 NC load 延迟从 O(field_count × NC_latency) 降到 O(1 × NC_bulk_latency + local_decode)。

**实现细节**：
```c
uint8_t local_frame[VEMB_V16_BATCH_RESPONSE_HEADER_BYTES +
                    VEMB_V16_BATCH_REQUEST_SIZE_MAX *
                        (sizeof(uint16_t) + VEMB_V16_AERON_RESP_WIRE_MAX_LEN)];
const uint8_t *frame;
if (desc.bytes <= sizeof(local_frame)) {
    sve_streaming_load(nc_frame, local_frame, desc.bytes);
    frame = local_frame;
} else {
    frame = nc_frame;  // fallback: oversized frame 直接 NC decode
}
int decode_rc = batch_response_decode(&response, entries, frame, desc.bytes);
```
- 栈缓冲区大小覆盖 batch_size=128 的最大 frame（~8472 bytes）
- 超大 frame 回退到直接 NC decode（安全兜底）
- Server 端 request ring 读取已是 CC 映射（本地 cache hit），无需此优化

**Client flamegraph 验证**（3:3 PIO:SNW，混合 2:3 UB 模式）：

| 函数 | O-C4 前 self % (094320) | O-C4 后 self % (100919) | 变化 |
|---|---:|---:|---:|
| `sdk_handle_session_poll_v2`（含 LTO inline） | ~26.8% | ~21.8% | **-18.7%** (-5pp) |
| `sdk_ub_poll` | 7.33% | 12.03% | +64% (相对) |

`poll_v2` 下降 5 个百分点：decode 路径的 NC load 被 bulk copy 替代，散装 NC 延迟消除。`sdk_ub_poll` 相对上升因 decode 变快后 poll 循环迭代更多次（同一时间窗口内完成更多 poll 轮次），是正常的比例重分配。

#### O-C1 + O-C2 + O-C4 综合结果

**测试配置**：3:3 PIO:SNW，Server CPU 0-15，4 clients，混合 2:3 UB 模式，30s TEST_TIME。

| 指标 | 基线 (084405) | O-C1+C2 后 (094320) | O-C1+C2+C4 后 (100919) | 累计变化 |
|---|---:|---:|---:|---:|
| QPS (M) | 6.279 | 6.186 | 6.433 | +2.5% |
| Client CPU cores | 4.655 | 4.653 | 4.591 | -1.4% |
| Client QPS/core (M) | 1.387 | 1.365 | 1.401 | **+1.0%** |
| avg_lat (ms) | 0.948 | 0.971 | 0.930 | -1.9% |
| P99 (ms) | 1.703 | 1.623 | 1.479 | **-13.2%** |

**O-C4 单项效果**（094320 → 100919）：
- QPS/core：1.365M → 1.401M（**+2.7%**）
- P99：1.623ms → 1.479ms（**-8.9%**）
- Client CPU：4.653 → 4.591 cores（**-1.3%**）

**分析**：O-C4 是三项 client 优化中对 QPS 提升最显著的一项。bulk NC copy 不仅减少了 decode 路径的 NC load 延迟，还改善了 L1 cache 利用率（一次连续写入 vs 多次散装 load 的 cache line 竞争）。P99 累计改善 13.2% 表明 NC 散装读是尾延迟的主要来源之一。

Server 端无需类似优化：Server 从 request ring 读取使用 CC 映射（export device），由本地 CPU cache 服务，无 NC 穿越延迟。

#### O-C5 dead code 清理

O-C5 在 `vemb_v16_cli_l0.h` / `vemb_v16_cli_l0.c` 中新增了 `batch_begin` 和 `resolve_response_ctx`，意图将 `resolve_response` + `batch_item_count` + `get_batch_identity` 合并为一次 L0 查找。但这些函数 **从未接入 `sdk_handle_session_poll_v2`**（poll_v2 仍使用原有的逐个调用路径），属于 dead code。之前观测到的 1.401M → 1.355M "回退" 经三次重跑（1.355M / 1.407M / 1.390M）确认为 ±2-3% 的 run-to-run variance，与 O-C5 代码无关。

已清理：移除 `vemb_v16_cli_l0_batch_begin` 和 `vemb_v16_cli_l0_resolve_response_ctx` 的声明和实现（l0.h 12 行，l0.c 49 行）。

#### O-C6: v1 poll skip（`pending_count == 0` 早返回）

**现状**：`vemb_v16_client_handle_session_poll` 每次迭代对每个 owner 无条件调用 `poll_v1`，即使无 v1 in-flight 请求。`poll_v1` 走 `sdk_ub_poll` → `vemb_v16_aeron_poll_response` → `vemb_v16_client_poll`，做一次 NC `atomic_load(&ring->tail, acquire)` 空转。

**改动**：在 `sdk_handle_session_poll_v1` 入口检查 `sdk_ub_state(backend)->pending_count == 0`，为零时直接返回，跳过 NC ring poll。添加 `sdk_ub_state` 前向声明解决编译顺序问题。

**测试结果**（混合 2:3，benchmark 112430）：

| 指标 | O-C4 后基线 | O-C6 后 | 变化 |
|---|---:|---:|---:|
| QPS/core (M) | 1.355 (低) / 1.401 (高) | 1.407 / 1.390 | ±2-3% noise |

v1 poll skip 在 benchmark 场景（纯 v2 流量）下消除了 `sdk_ub_poll` 的火焰图占比，但因 run-to-run variance，QPS 改善未超出噪声范围。对混合 v1+v2 生产场景仍有价值。

#### O-C7: poll_v2 空转 NC acquire load 分析（`perf annotate`）

**测试配置**：混合 2:3，benchmark 113808

**观测**：`vemb_v16_cli_l0_finish` 始终占 `sdk_handle_session_poll_v2` 的 ~45%（inclusive），与所有 client 优化无关。即「有用功」（response 向量读回 + completion 处理）的比例恒定，client 侧 polling 开销优化不改变吞吐瓶颈。

**poll_v2 时间分布**（113808 collapsed stacks 汇总）：

| 类别 | cycles (B) | 占 poll_v2 | 说明 |
|---|---:|---:|---|
| `sve_streaming_load_f32`（NC 向量读） | 1,638 | 45.3% | 有用功硬底——读回 response 向量 |
| poll_v2 self（LTO 内联） | 1,475 | 40.8% | `batch_desc_peek` + `resolve_response` + decode |
| `common_core_account_read` | 419 | 11.6% | gettimeofday + hdr_record（benchmark 统计） |
| `std::unordered_map` + 其他 | 89 | 2.3% | pending map 查找/删除 |

**`perf annotate` 指令级分析**（`sdk_handle_session_poll_v2.constprop.0`）：

```
 0.47%   43a770: ldar    x2, [x2]        ← batch_desc_peek: NC acquire load (ring->tail)
 0.04%   43a774: cmp     x5, x2          ← head == tail?
 0.00%   43a778: b.eq    43a948          ← 空转时跳转到返回
   ...
64.97%   43a948: ldp     x29, x30, [sp]  ← 函数 epilogue（返回指令）
```

**关键发现**：

1. **`ldar` 指令本身仅 0.47%**——ARM 上 acquire load 的屏障开销不是瓶颈
2. **64.97% 集中在返回路径 `ldp x29, x30, [sp]`**——perf 采样在 `ldar` stall 期间无法退休指令，stall 结束后第一条退休的指令（函数 epilogue）吸收了全部等待时间
3. **这说明绝大多数 poll_v2 调用是空转**：`ldar` 读到 `head == tail`，等待 NC DRAM round-trip 后跳转返回
4. **relaxed pre-check 方案无效**：NC 映射下 relaxed 和 acquire 都穿透 DRAM，barrier 差异可忽略。`ldar` → `ldr` 不减少 NC round-trip，只省 ARM 排序指令（在 NC stall ~200-400ns 面前可忽略）

**结论**：poll_v2 的 40.8% overhead 中，空转 NC DRAM round-trip 是主要成本。优化方向应减少空转 poll 次数，而非降低单次 poll 成本。

**O-C7 skip 实测（已验证无效，代码已撤销）**：

实现 per-owner `v2_poll_skip` 计数器，batch publish 后设为 2，poll_v2 入口递减跳过。`perf annotate` 确认 skip 分支被执行（2.78% 在 skip 出口），但 single_core_qps 无变化（1.364M vs 1.390M baseline，在噪声范围内）。

**无效根因**——client poll 空转优化是系统级死胡同：

1. **Client CPU 不是瓶颈**：64 线程仅消耗 4.62 cores（每线程 ~7.2%），远未饱和
2. **Server CPU 不是瓶颈**：2 PIO + 3 SN = 5 线程 busy-spin，16 核中 11 核空闲
3. **真正的瓶颈是 pipeline depth × latency**：单 slot 上限 = pipeline(32) / avg_latency(1.033ms) = 30.9K ops/sec，256 slots 理论 7.93M，实际 6.12M（77.2%）
4. skip 减少了 NC poll 次数，但节省的 CPU 时间被用于更频繁地调用空的 `drive_sessions` 循环——client 线程在 pipeline 满载等待 response 期间无有效工作可做
5. sleep 会增加 response 发现延迟（增大 avg_latency），反而降低 throughput

**后续不应继续 client poll 频率优化**。提升 single_core_qps 的有效方向：
- **Server 侧优化**（O4 poll_shm/handle_batch，O5 warm_location_raw）：减少 server 处理延迟 → 降低 avg_latency → 提升 pipeline 利用率
- **增大 pipeline depth**：直接提高单 slot 并发上限
- **减少 batch 凑包间隙**：优化 L0 submit → flush → publish 路径的 gap

### O4. `aeron_poll_shm_requests` ring 消费优化

**目标 CPU 减少**：~2–3% total（3:4 profile 下 self 4.00%）

**现状（3:4 profile）**：inclusive 10.89%，self 4.00%，含 `vemb_v16_proxy_handle_batch_request` self 4.31%。

**方案**：

| 方案 | 改动 | 预期效果 |
|---|---|---|
| A. prefetch request descriptor | 在处理当前 request 时 prefetch 下一个 descriptor | 减少 cache miss stall |
| B. 减少 consumer head 更新 | 批量消费 N 个 request 后再 advance head 一次 | 减少 atomic store 次数 |
| C. 拉大 batch 消费大小 | 增大单次 poll 返回的 request 数量上限 | 减少 while 循环和 ring 状态检查轮次 |

**实施要点**：
- ARM 上 `__builtin_prefetch` 对 UB 映射的 device memory 可能无效（取决于映射属性），需确认 request ring arena 的 memory type。
- head 更新延迟会增大 producer 端看到的 ring 使用率，如果 ring 较浅可能导致 producer FULL 退避。

### O5. `tlc_core_get_warm_location_raw` 缓存/数据结构优化

**目标 CPU 减少**：~3–5% total（3:4 profile 下 self **23.85%**，最大单一 self 热点）

**现状（3:4 profile）**：23.85% self + 1.80% warm_lookup_region + 0.53% location_cache_store_entry = ~26%。是 CPU 中占比最大的「有效功」。

**方案**：

| 方案 | 改动 | 预期效果 | 复杂度 |
|---|---|---|---|
| A. 增大 location_cache | 提高 cache 容量或换 eviction 策略 | 减少 full lookup 次数 | 低 |
| B. hash table cache-line 对齐 | bucket 按 64B 对齐，减少 false sharing 和 split load | 减少 cache miss | 中 |
| C. prefetch 下一跳 | hash 冲突链遍历时 prefetch next node | 隐藏 pointer-chase 延迟 | 中 |
| D. hot-key fast path | 高频 key 用独立的小 hash map 或 direct-mapped cache | 减少通用路径开销 | 中 |
| E. 向量化 hash 计算 | 用 NEON/SVE 加速 key hash | 减少 hash 计算本身的开销 | 高 |

**实施要点**：
- 需要先用 `perf stat` 采集 `location_cache` 的 hit/miss 率（诊断计数器已移除，但可以临时加 perf 硬件计数器）。如果 cache hit 率已经很高（>95%），增大 cache 的边际收益有限。
- hash table 结构改动影响面广，需要覆盖所有 lookup/insert/delete/resize 路径。
- 这部分是真正在做有效功，优化的 ROI 低于减少空轮询。

### O6. SuperNode 空轮询（~3.9%）

**目标 CPU 减少**：~1–2% total

**现状**：SN 线程已有三级退避（spin → clock check → futex wait），self 占比仅 3.9%，远低于 PIO。`futex_wait` 占 0.76% 说明退避机制已生效。

**方案**：
- 降低 `VEMB_V16_SUPERNODE_IDLE_SPIN_NS` 或 `VEMB_V16_SUPERNODE_IDLE_CLOCK_CHECK_ROUNDS` 让 SN 更快进入 futex wait
- 但 SN 唤醒延迟直接影响 request 处理延迟，需要更谨慎

**优先级低**，3.9% 的 self 占比在当前瓶颈格局下收益有限。

### O7. 跨线程 job 回收优化（`drain_job_return_queues`）——已验证收益有限

**3:4 profile self**：7.20%（因线程数减少而从 5:5 的 2.43% 上涨）

**已测试方案及结果（代码已撤销）**：

| 方案 | 改动 | self 变化 | QPS/core 变化 |
|---|---|---:|---:|
| A. 限制 drain 轮次 (max=2) | `while` → 有上限循环 | 7.20→6.95% | +0% |
| C. 内联 release + relaxed 语义 | acquire→relaxed, release→relaxed | 6.95→6.83% | +0.3% |

**无效原因分析**：
- 方案 A：`while` 循环在实际负载下很少超过 2 轮，限制轮次无实质改善
- 方案 C：每 slot 省 2 条 ARM 屏障指令（`ldar→ldr` + `stlr→str`），但 slot 数量有限，绝对收益 ~0.3%
- SN 端的 `publish_job_return_batch` 已经是批量发布，方案 D 已被现有代码实现
- 7% self 是 per-slot `atomic_load(state)` + `atomic_store(state, FREE)` + `poll_batch` memcpy 的**硬下限**

**后续不应重复此方向**——`drain_job_return_queues` 的 self time 在 3:4 配置下是不可压缩的固定税。减少此开销的唯一途径是减少 job return 数量本身（如 SN 侧直接回收 slot），但这涉及跨线程 ownership 模型的根本变更，投入产出比极低。

## 4. 实施优先级（基于 3:4 profile 修正）

3:4 profile 下 self-time 排名（即真正消耗 CPU 的叶子函数）：

| 排名 | 函数 | self % | 归属 | 可优化性 |
|---:|---|---:|---|---|
| 1 | `tlc_core_get_warm_location_raw` | 23.85% | SN 有效功 | 中（O5） |
| 2 | `publish_batch_response` | 14.83% | PIO response | 中（O3） |
| 3 | `drain_shard_queues` | 14.63% | SN job 分发 | 低（结构性） |
| 4 | `proxy_io_pool_thread_main` | 11.13% | PIO 轮询 | **已验证不可优化（O2）** |
| 5 | `drain_job_return_queues` | 6.83% | PIO job 回收 | **已验证为硬下限（O7）** |
| 6 | `handle_batch_request` | 4.31% | PIO request | 中（O4） |
| 7 | `poll_shm_requests` | 4.00% | PIO ring 消费 | 中（O4） |
| 8 | `supernode_pool_thread_main` | 2.50% | SN 轮询 | 低（O6） |
| 9 | `drain_completions` | 1.94% | PIO completion | 低 |
| 10 | `warm_lookup_region` | 1.80% | SN hash | 附属 O5 |

实施路径：

| 优先级 | 编号 | 方向 | self % (3:4) | 预估收益 | 复杂度 | 状态 |
|---|---|---|---:|---|---|---|
| **P0** | O2-C | PIO 线程数调优 | — | **+30.3% QPS/core** | 零 | **已验证（3:4 / 4:6）** |
| **P0** | O3-B | UB 方向混合模式 + PIO 减配 | — | **+3.3% QPS/core** | 配置 | **已验证（混合 2:3）** |
| **P1** | O-C1 | Client route_requests slot 扫描 → linked list | 19.21% (cli) | route 4.42→1.74% | 中 | **已实现已验证** |
| **P1** | O-C2 | Client 诊断 timing 移除 | 4.20% (cli) | `__udivti3` 2.95→0.66% | 低 | **已实现已验证** |
| **P1** | O-C4 | Client response decode bulk copy | 19.38% (cli) | poll_v2 26.8→21.8% | 中 | **已实现已验证** |
| **P1** | O-C6 | Client v1 poll skip (`pending_count == 0`) | ~13% (cli) | 消除空 v1 NC poll | 低 | **已实现，noise** |
| **P2** | O4 | Server poll_shm + handle_batch 优化 | 8.31% (svr) | +2–3% | 低–中 | 待实现 |
| **P2** | O5 | Server warm_location_raw 缓存/prefetch | 23.85% (svr) | +3–8% | 高 | 待实现 |
| **P3** | O6 | Server SuperNode 空轮询 | 2.50% (svr) | +1% | 低 | 待实现 |
| ~~P0~~ | ~~O2-A~~ | ~~PIO 退避策略~~ | 11.13% | ~~+10–20%~~ | — | **已验证无效（+~1%）** |
| ~~P1~~ | ~~O3-A~~ | ~~commit marker + fence 移除~~ | 14.83% | ~~+50%~~ | — | **已验证无效（+0%，NC drain 硬件瓶颈）** |
| ~~P1~~ | ~~O7~~ | ~~drain_job_return 优化~~ | 6.83% | ~~+3–5%~~ | — | **已验证无效（+0.3%）** |
| ~~P1~~ | ~~O-C7~~ | ~~Client poll_v2 空转 NC 降频~~ | 40.8% (cli) | ~~+4-6%~~ | — | **已验证无效（pipeline×latency 瓶颈，非 client CPU）** |

## 6. 已验证无效方向（避免重复优化）

以下方向经过实测和分析，确认收益不足以证明改动的合理性。**后续优化应跳过这些方向**。

### 6.1 PIO 退避策略代码优化（O2-A）

**测试配置**：5:5 PIO:SNW，Server CPU 0-15，4 clients

| 方案 | 具体改动 | 回归结果 |
|---|---|---|
| 三级退避 | SPIN 256→32，新增 NAP=96，deep=50us | QPS/core +0.21% |
| channel scan skip | 空闲线程 `continue` 跳过 snapshot/channel 原子操作 | QPS/core +0.02% |
| 衰减重置 | `idle_rounds /= 2` 替代 `= 0` | 含在上述 |

**无效根因**：
- PIO self time 32.5% 是 LTO 内联的 `channel_acquire` CAS + `channel_release` atomic_sub 等结构性原子操作，不是 `cpu_relax` 空转
- 4 clients / 5 PIO = 4 个线程始终有 channel，只有 1 个空闲线程受益于退避
- 退避对有 channel 的线程无效——它们每轮必须做原子操作检查 channel 状态

### 6.2 `drain_job_return_queues` 代码优化（O7）

**测试配置**：3:4 PIO:SNW，Server CPU 0-15，4 clients

| 方案 | 具体改动 | self 变化 | QPS/core 变化 |
|---|---|---:|---:|
| 限制 drain 轮次 | `while` → `max 2 rounds` | 7.20→6.95% | +0.0% |
| relaxed 内存序 | state load acquire→relaxed, store release→relaxed | 6.95→6.83% | +0.3% |

**无效根因**：
- `while` 循环实际很少超过 2 轮——ring 容量 256，PROXY_QUEUE_BATCH=32，正常负载下一轮就排空
- relaxed 语义省的是 ARM `ldar/stlr` 屏障指令，per-slot 开销极小
- 7% self 是 per-slot `atomic_load(state)` + `atomic_store(state, FREE)` + `poll_batch` memcpy 的硬下限
- SN 端的 `publish_job_return_batch` 已经是批量发布，无法从 SN 侧减少写入频次

### 6.3 O3-A: commit marker 移除 + fence 消除

**测试配置**：3:3 PIO:SNW，Server CPU 0-15，4 clients

| 方案 | 具体改动 | 回归结果 |
|---|---|---|
| fence 移除 | 删除 `atomic_thread_fence(release)` (49.25% 热点) | QPS/core +0.0% |
| commit marker 移除 | 删除 8 字节 batch_id 尾部校验 | QPS/core +0.0% |

**无效根因**：
- 两个 barrier（fence 49% + STLR 42%）等待的是同一件事——NC store drain 到远端 UB
- 移除 fence 后，STLR 指令独自承担全部等待时间（91% → STLR 独占 91%）
- NC store drain 延迟是硬件层面的跨节点 round-trip，barrier 数量不影响总等待时间
- 代码改动作为清理保留（简化 wire format），但不应期望性能收益

### 6.4 O3-B 全反转：CC write → NC read 双向

**测试配置**：3:3 PIO:SNW，Server CPU 0-15，4 clients

| 指标 | 原 NC→CC | 全反转 CC→NC | 变化 |
|---|---:|---:|---:|
| QPS (M) | 7.867 | 0.219 | -97.2% |
| QPS/core (M) | 1.362 | 0.069 | -95.0% |

**灾难性下降根因**：
- Server PIO busy-poll request ring 变成 NC read，每次 `atomic_load(tail, acquire)` 都是跨节点 round-trip
- NC write 的开销是 per-message（有数据才写），NC read poll 的开销是 per-iteration（空转也付代价）
- busy-poll 的迭代频率远高于消息到达频率，总开销放大数个数量级
- **结论：CC write → NC read 不适用于高频 busy-poll consumer（request ring）**；仅适用于"发请求→等响应"的短暂 poll 场景（response ring 的混合模式）

### 6.5 CPU mask 收紧（0-15 → 0-7）

**测试结果**：3:4 在 8 核（0-7）上 QPS/core 1.319M vs 16 核（0-15）上 1.330M，差异 -0.8%。7 线程跑 8 核足够，但无额外收益——profile 分布不变，只是 CPU set 利用率从 39% 升到 77%。

### 6.6 结论

3:4 profile 下，PIO self（11%）和 drain_job_return（7%）合计 ~18% 的 self time 已不可通过代码层面优化压缩。O3-A 证实 NC store drain 是硬件瓶颈、barrier 数量无关；O3-B 全反转证实 busy-poll consumer 不能用 NC read。

混合模式（O3-B）改变了瓶颈格局：response publish 从 P1 热点降到噪声水平。Client 侧分析（O8）揭示系统瓶颈已从 Server 转移到 Client：
- **O-C1** route_requests O(4096) 空 slot 扫描（4.42%→1.74%）：**已实现**，intrusive linked list
- **O-C2** `getMonotonicNs` 128-bit 除法（2.95%→0.66%）：**已实现**，移除诊断 timing
- **O-C4** response decode NC 散装读（poll_v2 26.8%→21.8%）：**已实现**，bulk NC copy + local decode
- **O4** Server poll_shm + handle_batch（混合 3:3 下 ~18% self）：request 消费路径
- **O5** Server warm_location_raw（混合 3:3 下 ~29% self）：有效功，需 hash/cache 结构改动

**混合 2:3 配置的 QPS/core 1.401M 是 O-C1+C2+C4 后最优**。三项 Client 优化合计：QPS/core +1.0%，P99 -13.2%，Client CPU -1.4%。O-C5 为 dead code 已清理，O-C6（v1 poll skip）效果在噪声范围。O-C7（poll_v2 空转降频）经 `perf annotate` + skip 实测确认：空转 NC poll 占 40.8% client CPU 但 client CPU 并非系统瓶颈——真正瓶颈是 pipeline depth × avg_latency，client 侧 poll 优化是死胡同。后续应转向 Server 侧优化（O4/O5）或增大 pipeline depth。

## 7. 产物

| 项目 | 路径 |
|---|---|
| O-C1+O-C2 前 client flamegraph | `perf/p2_s3_svr0-15_uniform_20260921_084405/client/client.svg` |
| O-C1+O-C2 后 client flamegraph | `perf/p2_s3_svr0-15_uniform_20260921_094320/client/client.svg` |
| O-C4 后 client flamegraph | `perf/p2_s3_svr0-15_uniform_20260921_100919/client/client.svg` |
| O-C6+C7 分析 client flamegraph | `perf/p2_s3_svr0-15_uniform_20260921_112430/client/client.svg` |
| O-C7 perf annotate 数据 | `/tmp/p2_s3_svr0-15_uniform_20260921_113808/client.perf.data`（远端） |
| O3-B 全反转 flamegraph | `perf/p3_s3_svr0-15_uniform_20260921_001129/server/server.svg` |
| O3-B 混合 3:3 flamegraph | `perf/p3_s3_svr0-15_uniform_20260921_002313/server/server.svg` |
| O3-B 混合 2:3 server flamegraph | `perf/p2_s3_svr0-15_uniform_20260921_003650/server/server.svg` |
| O3-B 混合 2:3 client flamegraph | `perf/p2_s3_svr0-15_uniform_20260921_003650/client/client.svg` |
| O3-B 全反转 YAML | `examples/vemb_v16_ub_peer_view_112_to_111_cc_write_nc_read.yaml` |
| O3-B 混合 YAML | `examples/vemb_v16_ub_peer_view_112_to_111_hybrid.yaml` |
| O7 relaxed 語義 flamegraph | `perf/p3_s4_svr0-15_uniform_20260920_172144/server/server.svg` |
| O7 drain 限轮 flamegraph | `perf/p3_s4_svr0-15_uniform_20260920_171433/server/server.svg` |
| 0-7 CPU mask flamegraph | `perf/p3_s4_svr0-7_uniform_20260920_172505/server/server.svg` |
| O2-C PIO=3:SNW=4 flamegraph | `perf/p3_s4_svr0-15_uniform_20260920_165605/server/server.svg` |
| O2-C PIO=4:SNW=6 flamegraph | `perf/p4_s6_svr0-15_uniform_20260920_170047/server/server.svg` |
| 当前 flamegraph（O1 后基线） | `perf/p5_s5_svr0-15_uniform_20260920_141408/server/server.svg` |
| O2-R1 退避优化 flamegraph | `perf/p5_s5_svr0-15_uniform_20260920_160912/server/server.svg` |
| O2-R2 scan skip flamegraph | `perf/p5_s5_svr0-15_uniform_20260920_161918/server/server.svg` |
| O1 前 flamegraph | `perf/p5_s5_svr0-15_uniform_20260920_134939/server/server.svg` |
| diag 移除前 flamegraph | `perf/p5_s5_svr0-15_uniform_20260920_134150/server/server.svg` |
| S0–S4R2 flamegraph | `perf/cpu_opt_v2_20260919_232600_<阶段>_p5_s5/server/server.svg` |
| 回归文档 | `docs/VEMB_V16_CPU_OPT_V2_REGRESSION_20260919.md` |
| 诊断移除文档 | `docs/VEMB_V16_CPU_OPT_V2_DIAG_COUNTER_REMOVAL_20260920.md` |
| O1–O5 方向文档 | `docs/VEMB_V16_CPU_OPT_V2_NEXT_20260920.md` |
| 本文档 | `docs/VEMB_V16_CPU_OPT_V2_ANALYSIS_20260920.md` |
