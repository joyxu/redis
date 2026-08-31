# VEMB v16 UB CLI 性能定位与优化任务

本文档记录 `CC -> NC` active-ring UB 场景下 old-key 与 steady-key 读性能差异的
当前证据，并作为后续 CLI 优化的基线。本文档只讨论客户端数据面，不把
`benchmark/vemb_v16_topology_ctl` 控制面查询当作业务请求或 perf 样本。

## 1. 测试口径

- 脚本：`scripts/vemb_v16_ub_active_2node_111_to_112.sh`
- 场景：`cc_nc`，request/response 使用当前脚本配置的 CC -> NC 方向
- 客户端：64 threads x 4 clients，pipeline=32，dim=300
- key 范围：old `item:10000..19999`，steady `item:20000..29999`
- `batch-max-delay-us=10`
- 正式 clean CLI profile 使用 `DISABLE_STATS_SAMPLER=1`，每个读阶段单独启动
  `perf record -- memtier_benchmark`
- 所有有效回归均通过 correctness 检查；steady clean profile 的 QPS 为
  `11,991,813`，p50/p99 为 `0.455/0.743 ms`

`topology_ctl` 只负责拓扑设置、stats/diagnostic snapshot、就绪检查和 close-wait。
它没有被 `perf record` 包裹，也不产生 memtier 业务 QPS。关闭 stats sampler 后，
测试阶段没有周期性 topology 控制请求。

## 2. 请求链路

```text
client submit
  -> drive_sessions_start
  -> slot_poll_start
  -> route_start -> route_done
  -> l0_start -> l0_enqueue
  -> deadline_target -> poll_check
  -> flush_enter -> L0 prepare -> Aeron publish
  -> response poll -> finish -> completion callback
  -> pending lookup -> read_vector/warm copy -> accounting
```

## 3. 已确认的数据

### 3.1 请求级边界统计

在无 flamegraph/bpftrace 干扰的 `ub_active_2node_20260826_182428`、delay=10us、
64 threads x 4 clients 回归中，old/steady/old-after/steady-reread 均通过正确性检查。
关键平均值如下，单位为 us：

| 区间 | old | steady | 增量 |
| --- | ---: | ---: | ---: |
| `drive_sessions_start -> slot_poll_start` | 674.0 | 1,353.7 | +679.7 |
| `slot_poll_start -> route_start` | 6.3 | 6.1 | -0.2 |
| `submit -> route_start` | 686.1 | 1,366.0 | +679.9 |
| `L0 enqueue -> deadline target` | 68.2 | 197.2 | +129.0 |
| `L0 -> observed deadline due` | 135.4 | 342.4 | +207.0 |

独立 `submit_to_publish` 计时显示：

| 指标 | old | steady | 结论 |
| --- | ---: | ---: | --- |
| `submit -> publish` | 0.585 ms | 0.842 ms | 增加 0.257 ms |
| L0 `prepare` | 0.254 us | 0.263 us | 基本不变 |
| `prepare -> publish` | 2.339 us | 2.312 us | 基本不变 |
| Aeron publish | 4.227 us | 4.142 us | 基本不变 |

因此，steady 的主要新增 wall time 在 batch publish 之前，而不是 L0 batch 构造、
Aeron publish 或 deadline 到期后的 flush 执行。`drive_sessions_start -> slot_poll_start`
是当前已定位的最大区间；`l0_enqueue -> deadline_target` 是另一段稳定增量。

### 3.2 response/server 边界统计

无错误交替复测 `ub_active_2node_service_clean_150239` 的结果：

| 阶段 | QPS | `publish -> response_poll` | server service | response wait |
| --- | ---: | ---: | ---: | ---: |
| old | 11,771,738 | 237.179 us | 16.183 us | 220.997 us |
| steady | 10,117,055 | 246.340 us | 14.570 us | 231.769 us |

steady 的 `submit -> response_poll` 增加约 `1.4--1.8 ms`，但 publish 后只增加约
`9--14 us`，server service 没有增加。因此不能把约 2M QPS 差距归因于 server 处理
或 response publish 后的等待。

### 3.3 callback 边界统计

`ub_active_2node_callback_boundary_131834` 中：

| 指标 | old | steady |
| --- | ---: | ---: |
| owner0 callback | 18.64 us | 44.59 us |
| owner1 callback | 19.50 us | 49.96 us |
| callback 内 pending lookup | 0.114 us | 0.114 us |
| callback 内 `read_vector` | 4.87 us | 7.22 us |
| callback 内 accounting | 0.72 us | 0.98 us |

callback 增量是真实的 client completion 路径增量，但 `read_vector` 只解释其中一部分。
独立 warm-copy UT 在 256 并发下复现了 steady p99 尾延迟升高，但没有复现 CLI 压测中
全部 sampled copy 平均值的 3.5 倍增长；因此不能把“单独的远端内存 copy”写成唯一根因。

### 3.4 pipeline/batch 容量对照

为验证集群场景下一个 session 的 pipeline 被两个 owner 分摊后容易形成小 batch，
在 `threads=64`、`clients=1`、`delay=10us`、`cc_nc` 下逐步提高 read pipeline。
`server --vemb-v16-batch-request-size` 与 CLI batch size 随脚本的 `PIPELINE` 一起调整；
steady write 阶段仍使用脚本固定的 pipeline=32。原三轮 delay=10us 测试均 correctness pass；
新增的 delay=0 控制实验也 correctness pass：

| read pipeline | old-key QPS | old-key after steady QPS | steady-key QPS | steady p50/p99 |
| ---: | ---: | ---: | ---: | ---: |
| 32 | 14,066,514 | 14,456,820 | 12,868,981 | 0.103/0.231 ms |
| 64 | 15,947,158 | 15,832,934 | 14,239,397 | 0.167/0.399 ms |
| 80 | 16,234,975 | 16,276,764 | 14,539,727 | 0.199/0.487 ms |
| 80 (delay=0) | 16,318,820 | 16,381,232 | 14,530,011 | 0.199/0.479 ms |

新增的 `pipeline=80、BATCH_MAX_DELAY_US=0` 控制实验来自
`ub_active_2node_20260826_223534`；其余主要参数保持为 `threads=64`、`clients=1`、
`cc_nc`、`SLOT_SCHED=fixed`，server/client batch size 均为 80。该行的 prefill 和
steady write 仍使用脚本固定的 pipeline=32，表中读阶段使用 pipeline=80。

与 delay=10us 的 pipeline=80 对照相比，old-key、old-key after steady 和 steady-key
QPS 变化分别约为 `+0.52%`、`+0.64%` 和 `-0.07%`，steady p99 从 `0.487ms` 变为
`0.479ms`。单轮结果不能证明存在收益，但至少表明关闭 10us deadline 没有改变当前
吞吐等级；该控制实验应与原三轮 delay=10us 数据分开解读。

pipeline=64 相比 pipeline=32 将 old/steady 的绝对 QPS 分别提高约 13.4% 和 10.7%；
pipeline=80 继续提高到约 16.23M/14.54M。owner batch 统计也显示 batch 从 pipeline=32
时约 11--13 项提高到 pipeline=64/80 时约 20--24 项，证明原先存在 batch underfill。

但是 pipeline=64/80 的 owner 统计仍为：

```text
flush_full     = 0
flush_deadline = flush_calls
flush_eager    = 0
```

即 batch 仍主要由 `delay=10us` 到期触发，而不是达到 batch 上限。pipeline=80 下，
代表性 owner1 统计为 `avg_batch=24`、`v2_poll_avg=85.7us`、`poll_avg=92.5us`，
steady 仍未转变为 full-batch flush。

提高 pipeline 后 steady/old 的相对差距仍然存在：pipeline=32 时约 8.5%，
pipeline=80 时约 10.5%。因此结论是：

1. pipeline 增大能够减少 batch underfill，并显著提高绝对 QPS；
2. batch 构造和 flush 本身不是 steady 额外耗时的全部来源；
3. 当 old 和 steady 的平均 batch 已接近时，steady 仍有更高的 owner1 v2 response poll
   成本，剩余瓶颈位于 response poll/response processing 及 callback 路径；
4. 若继续测试 batch 饱和，应单独改变 `batch-max-delay-us` 或继续提高 pipeline，
   但这会改变 bounded coalescing 语义，必须作为控制实验记录，不能直接作为正式基线。

## 4. Clean CLI 火焰图证据

产物：

- `benchmark/results/scaleout/ub_active_2node_flame_clean_20260826_1855/profiles/old_keys_read.svg`
- `benchmark/results/scaleout/ub_active_2node_flame_clean_20260826_1915/profiles/steady_keys_read.svg`

火焰图是 user+kernel CPU 栈的 inclusive sampled share；不同函数区间会重叠，不能把
百分比相加，也不能用它直接测量 off-CPU 等待。

| inclusive 栈 | old | steady | 变化 |
| --- | ---: | ---: | ---: |
| `sdk_ub_read_warm_vector` | 20.00% | 27.65% | +7.65 pp |
| `vemb_v16_aeron_read_vector` | 19.86% | 27.52% | +7.66 pp |
| `sdk_handle_session_poll_v2` | 60.69% | 63.73% | +3.04 pp |
| `common_core_handle_completion` | 33.30% | 39.55% | +6.25 pp |
| `sdk_handle_session_route_requests` | 21.35% | 19.28% | -2.07 pp |
| `sdk_handle_session_flush_owner` | 8.77% | 8.19% | -0.58 pp |

火焰图与 callback 统计相互印证：steady 的 completion 路径中，warm-handle
`read_vector`/copy 占用的 CPU 样本增加。另一方面，火焰图没有显示 route、L0 prepare、
Aeron publish 的 steady 独有热点。由于 `submit -> publish` 的主要部分是线程未执行时的
排队/等待，火焰图本身不会显示这段 wall time；这部分仍以请求级边界统计为准。

## 5. 当前结论

### 已确认

1. QPS 差距的主要来源是 **client batch publish 之前的调度等待**，最大已定位区间是
   `drive_sessions_start -> slot_poll_start`；其次是 owner/channel 状态处理和
   `l0_enqueue -> deadline_target`。
2. steady 的 completion callback 更重，火焰图和 callback 计时都显示
   `read_vector`/warm-handle 路径及其尾延迟增加。
3. 可以排除 server service、response publish 后等待、L0 prepare、Aeron publish 和
   finish bookkeeping 是主要原因。
4. client=1 且提高 pipeline 后，slot queue 基本消失但 steady 差距仍在；pipeline 增大
   只消除了部分 batch underfill，未消除 owner1 的 v2 response poll 增量。

### 尚未确认

- `slot_poll_start` 前的增量究竟由 slot 顺序/扫描、owner poll 唤醒，还是 OS 线程调度
  造成，当前统计只定位到边界，尚未完成函数级拆分。
- warm-handle 增量中，远端内存访问、cache/NUMA 位置、completion 并发竞争各自的比例
  尚未确定。独立 UT 不支持把 warm copy 单独作为全部根因。

因此当前不应表述为“steady 慢是远端内存 copy 单一原因”，准确表述是：

> steady 的额外耗时由 publish 前的 client slot/poll/deadline 调度等待主导，
> 并叠加 completion callback 中 warm-handle read/copy 的 CPU 与尾延迟增加。

## 6. 优化任务列表

### A. 优化 publish 前的 client 调度等待

- [ ] **A1：补齐等待原因统计。** 在保持 sampled timing 的前提下记录 slot 的 ready、
  poll、deadline、owner/channel 状态和实际 flush 触发原因，区分“前序 slot 未完成”、
  “等待下一次 poll”与“等待 deadline”。
- [ ] **A2：检查 slot 调度顺序。** 以 `drive_sessions_start -> slot_poll_start` 为重点，
  评估 ready-slot 队列、轮询顺序和空 slot 扫描；先证明是否存在 steady slot 被前序
  slot 阻塞或被延后处理，再决定是否调整调度策略。
- [ ] **A3：前移 owner/channel 初始化。** `ensure_owner_channel` 应负责 owner 对应的
  v1 基础数据通道建立、资源代际确认和必要的 reattach；`sdk_owner_v2_enable` 中的
  v2 Aeron batch ATTACH、资源获取和 L0 创建属于 channel/session 初始化工作，不应在
  每条请求的 route/submit 热路径中懒加载。目标是请求路径只读取已验证的 channel/session
  READY 状态，正常 steady 请求不执行同步 attach、mmap、分配或 L0 创建。

  不能直接把这些工作放进 `vemb_v16_client_create`：该阶段尚未取得 topology owner
  endpoint，UB peer-view manifest 也可能尚未通过配置 API 就绪。建议在
  `configure_ub_peer_view + topology_refresh` 之后增加显式 owner prepare 阶段，对当前
  active ring 的 owner 依次预热 v1 channel、v2 batch channel 和 L0；拓扑 refresh 完成后
  对新 owner 重复该 prepare。migration/quiescing/draining 期间不得启用 v2，待稳定
  full-active topology 生效后再预热。

  运行时仍必须保留异常路径：topology/resource generation 变化时允许 fence、close、
  reattach；v2 ATTACH 失败时标记 unavailable 并回退 v1，不在业务请求中同步重试。为
  验证前移是否命中根因，先补充每个 owner 的 `ensure_owner_channel` fast-return、
  resource-check、reopen/open，以及 v2 `READY` 返回、实际 ATTACH、失败、L0 create
  和状态转换计数；缓存只允许建立在 generation、owner identity 和 channel state 不变
  的严格契约上。

  在两 active owners、`client=1`、`pipeline=80` 的诊断轮中，每个 client session 对每个
  owner 都观察到 `v2_enable_attach=1`、`v2_enable_reopen=1`，而
  `v2_enable_ready_fast` 达到数百万次；没有动态刷新或重复 ATTACH/L0 创建。因此 A3
  不是 steady QPS 差距的根因，只能消除每个 session 首批请求承担的一次性初始化抖动。
  已将 active-ring owner 的 channel/v2 预热移到 handle session 创建前，并将
  `ensure_owner_channel` 拆成稳定 snapshot fast path 与新 owner/新 snapshot slow path。
  稳定路径只读取本地 channel-ready、snapshot-id 和 backend-ready 状态；endpoint 查找、
  resource 检查、fence、close/reopen 保留在 slow path。steady 请求仍保留 generation、
  migration、resource failure 和 v1 fallback 异常路径。
- [ ] **A4：评估 deadline 触发。** 保持 delay=10us 作为正式基线，使用 delay=0 作为
  控制实验，比较 `l0_enqueue -> deadline_target`、`deadline_target -> poll_check`
  和 QPS；不得通过绕过 deadline 破坏 bounded coalescing 语义。
- [ ] **A5：验收。** old/steady/old-after/steady-reread 交替测试全部 correctness pass；
  `submit -> publish` 增量和 `drive_sessions_start -> slot_poll_start` 增量下降，且
  `prepare`、publish error、ring full、duplicate/lost completion 不恶化。

### B. 优化 completion callback / warm-handle 路径

- [ ] **B1：拆分 callback CPU 路径。** 保留采样计时，分别记录 response decode、pending
  lookup、handle resolve、region mapping、`read_vector`、warm copy 和 accounting，
  并按 owner/local-imported region 分组。
- [ ] **B2：确认 copy 增量来源。** 在 256 sessions、pipeline=32 的异步 UT 和真实 CLI
  压测中同时采集 copy p50/p99、callback p50/p99、UB/NUMA 访问计数；区分固定 copy
  成本与高并发尾延迟，避免只依据 sampled average 改代码。
- [ ] **B3：减少 callback 重复工作。** 在确认 generation 和资源所有权契约后，评估
  pending/region 映射缓存、重复 topology 查找消除和批量 completion 处理；不得删除
  stale response、generation、ring integrity 或 publish failure 检查。
- [ ] **B4：评估 copy 调度。** 对 imported warm region 检查预取、批量化和 NUMA placement
  的收益；所有改变必须保持 dim=300、handle identity、region_id/region_index 区分和
  migration fence 语义。
- [ ] **B5：验收。** callback 与 `read_vector` 的 p99 下降，steady/old 差距缩小；同步
  和异步 UT、真实 active-ring 回归均 correctness pass，且不得引入 materialization
  failure、stale vector、status error 或资源泄漏。

## 7. 优化顺序

1. 先完成 A1/A2，确认 publish 前等待的具体调度原因；这是 QPS 影响最大的部分。
2. 再完成 A3/A4，区分 owner/channel 状态等待与 deadline 等待，避免错误地把 delay
   语义当成 L0 执行成本。
3. 并行完成 B1/B2，建立 callback/warm-copy 的稳定分位数基线。
4. 只有在 B2 确认具体热点后实施 B3/B4；每次改动都用同一组 old/steady 交替 workload
   回归，避免把调度等待和 copy CPU 变化混在一次改动中。

## 8. A1/A2 首轮实现：slot 调度缺陷与可回退验证

代码检查确认当前 runner 存在一个 benchmark 调度层面的设计缺陷：

1. `threads=64, clients=4` 表示每个 worker 拥有 4 个独立 handle session，
   不是 4 个并行 poll 线程；4 个 session 由同一 worker 线程串行驱动。
2. `common_core_run_async_reads()` 先给所有 slot 各填充 `pipeline=32` 个请求，
   再调用 `common_core_drive_sessions()` 按 `worker->slots.begin()` 的固定顺序
   逐个 poll。于是一个 slot 的 callback、route、deadline 和 response drain 会延后
   后续 slot 的 poll。
3. deadline 只在该 session 的 `vemb_v16_client_handle_session_poll_at()` 中检查，
   没有独立 timer worker；后续 slot 的 deadline 观察天然受前序 slot 执行时间影响。
4. `drive_sessions_start -> slot_poll_start` 因此表示“本轮 worker 驱动开始到该
   slot 获得执行权”的排队时间，不是 slot poll 函数内部耗时。steady callback 更重时，
   该串行队列会被进一步放大。

这解释了为什么当前最大增量落在该边界，但它属于 runner 使用/调度方式问题，不是
UB ring 或 wire protocol 缺陷。直接把 `clients` 改为 1 会把每 worker 的 outstanding
从 `4 x 32` 降为 `1 x 32`，不是等价实验；直接增加 threads 又可能超过 benchmark
绑定的 CPU 数，也不能作为最终优化方案。

已加入以下可回退验证：

- `VEMB_V16_SLOT_SCHED` 未设置或为 `fixed` 时保持原有固定顺序。
- `VEMB_V16_SLOT_SCHED=round_robin` 时每轮从下一个 slot 开始驱动，保留每个 slot
  的 pipeline、owner session、deadline 和 batch 语义，只消除固定 slot 优先级。
- 开启 `VEMB_V16_CLI_STATS_INTERVAL_MS` 后，每秒输出 `[cli-slot-stat]`，包括每个
  slot 的 `queue_avg/min/max_ns`、`poll_avg/min/max_ns`、callback 数和空 poll 数。
  这些统计只在 `VEMB_V16_CLI_STATS_INTERVAL_MS` 非零时取时间戳；clean perf profile
  应显式设置该变量为 `0`，避免计时和 stats 输出进入样本。

首轮验证命令应保持同一 workload，仅切换调度模式：

```bash
VEMB_V16_SLOT_SCHED=fixed \
VEMB_V16_CLI_STATS_INTERVAL_MS=1000 \
bash scripts/vemb_v16_ub_active_2node_111_to_112.sh --scenario cc_nc

VEMB_V16_SLOT_SCHED=round_robin \
VEMB_V16_CLI_STATS_INTERVAL_MS=1000 \
bash scripts/vemb_v16_ub_active_2node_111_to_112.sh --scenario cc_nc
```

验收重点：

- fixed 模式下 slot 0/1/2/3 的 queue wait 是否随固定顺序递增；
- round-robin 是否降低后续 slot 的 queue wait 和 old/steady 的差距；
- 两种模式的 pipeline、batch size、correctness、ring-full、publish-error 和
  lost/duplicate completion 必须保持一致。

当前只完成 A1 统计和 A2 的低风险 round-robin 开关；尚未启用更激进的“submit 一个
slot 后立即 poll 该 slot”的交错模式。该模式可能改变 deadline 观察节奏和阶段 batch
形成，必须在 round-robin 数据证明固定优先级是主要问题后再单独评估。
