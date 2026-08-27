# VEMB V16 UB Request/Response NC→CC A/B 汇报（2026-08-25）

本报告比较 owner1 request/response ring 的访问方向，目的是确认 NC→CC 配置
是否适合当前两节点 active-ring。结果表明 response 使用 CC→NC 时 QPS 最好，
但 request 也切换为 CC→NC 会导致性能显著下降。因此 runner 的 `cc_nc` 场景
最终固定为非对称配置：request NC→CC、response CC→NC；`nc_cc` 保持原有的
双向 NC→CC 配置不变。

## UB 参数与数据面角色

下表中的箭头表示同一 ring 上 producer 到 consumer 的 cache/access mode，
不是 CLI 与 server 的网络方向：

| 参数 | server 角色 | client 角色 |
| --- | --- | --- |
| `--vemb-v16-aeron-ub-path` | request ring consumer | request ring producer |
| `--vemb-v16-aeron-response-ub-path` | response ring producer | response ring consumer |

因此 request 数据流是 `CLI producer -> server consumer`，response 数据流是
`server producer -> CLI consumer`。`nc_cc` 的两条 ring 都是 `NC -> CC`；
最终的 `cc_nc` 场景为 request `NC -> CC`、response `CC -> NC`。双向
`CC -> NC` 仅作为诊断对照，不作为 runner 的默认配置。

两轮使用同一 runner、两节点 `active={0,1}`、`DIM=300`、`t64/c4/pipeline=32`、
`PIO/SNW=7:7`、`BATCH_MAX_DELAY_US=10us` 和 30 秒读窗口。正确配置测试启用
`steady_keys_reread`；为缩短诊断回退测试，回退轮关闭了该阶段，以下共同阶段
仍可直接比较。表格中的 QPS、CPU 和 ops/core 均四舍五入为整数；延迟统一换算为
整数微秒（us），不保留小数。


## 本轮远端同步与回归结果（2026-08-23）

以下表格来自主设计文档同名小节，记录本轮四组公共 workload。QPS、CPU 和
ops/core 均为整数四舍五入；延迟统一使用整数微秒。

| 场景（脚本） | 线程参数 / 状态 | QPS (ops/s) | p50 (us，按 avg) | p99 (us) | server CPU (cores) | client CPU (cores) | ops/core | 正确性 | 产物 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| [TCP-local / host-mt server flamegraph](../scripts/run_host_mt_server_flamegraph.sh) | `WORKERS=16:16` | 11,428,873 | 639 | 1,383 | 23 | — | 506,824 | — | [perf/20260823_130514](../perf/20260823_130514/) |
| [Aeron-local / Aeron best](../scripts/run_aeron_best.sh) | `WORKERS=21:21` | 31,744,269 | 192 | 327 | 39 | — | 808,359 | — | [perf/aeron_sweep/20260823_104930](../perf/aeron_sweep/20260823_104930/) |
| [Aeron-local / Aeron best](../scripts/run_aeron_best.sh) | `WORKERS=7:7` | 16,633,994 | 423 | 559 | 14 | — | 1,173,888 | — | [perf/aeron_sweep/20260823_105921](../perf/aeron_sweep/20260823_105921/) |
| [Aeron-cross-node](../scripts/run_aeron_cross_node_flamegraph.sh) | `PIO/SNW=7:7` | 11,082,054 | 505 | 895 | 12 | _ | 935,962 | - | [perf/p7_s7_svr0-15_uniform_20260823_113040](../perf/p7_s7_svr0-15_uniform_20260823_113040/) |



## 场景一：正确 NC→CC

配置语义为：node1 response producer 使用 `/dev/obmm_shmdev15`（NC），CLI
owner1 response consumer 使用 `/dev/obmm_shmdev11`（CC）。

产物：`benchmark/results/scaleout/ub_active_2node_newcfg_20260825_094032/`

| 阶段 | QPS (ops/s) | p50 (us) | p99 (us) | correctness |
| --- | ---: | ---: | ---: | --- |
| prefill | 4,757 | 2,191 | 2,223 | pass |
| old keys read | 6,839,750 | 239 | 4,191 | pass |
| steady write | 4,983 | 2,191 | 2,223 | pass |
| old keys after steady | 6,396,004 | 215 | 4,351 | pass |
| steady keys read | 6,168,998 | 239 | 3,823 | pass |
| steady keys reread | 5,976,524 | 239 | 3,647 | pass |

阶段末 diagnostic 均为 `lookup_cache_miss=0`、`lookup_final_miss=0`、
`handle_lookup_miss=0`，两节点 `channel_count=0`。node1 Server 日志确认
`dev15` 以 `mode=NC` 启动。

## 场景二：response 回退 CC→NC

本节数据的配置语义为：node1 response producer 使用 `/dev/obmm_shmdev11`
（CC），CLI owner1 response consumer 使用 `/dev/obmm_shmdev15`；当时 request
仍使用 NC→CC。这是历史高 QPS 映射，违反当前硬件要求的 producer NC、consumer
CC 方向，仅用于因果 A/B，不是双向 CC→NC 场景的性能结果。

产物：`benchmark/results/scaleout/ub_active_2node_oldpath_diag_20260825_095643/`

| 阶段 | QPS (ops/s) | p50 (us) | p99 (us) | correctness |
| --- | ---: | ---: | ---: | --- |
| prefill | 4,765 | 2,239 | 2,271 | pass |
| old keys read | 13,128,648 | 423 | 671 | pass |
| steady write | 5,543 | 1,191 | 2,271 | pass |
| old keys after steady | 13,007,964 | 423 | 671 | pass |
| steady keys read | 11,015,546 | 471 | 751 | pass |

node1 Server 日志确认 response pool 为 `dev11 mode=CC`；阶段末 diagnostic
同样没有 `lookup_cache_miss`、`lookup_final_miss` 或 `handle_lookup_miss`，且
channel close 清理完成。

## 双向 CC→NC 诊断结果

为验证 request 方向是否也应反转，曾将 request 同时切换为 node1 `dev14`
（NC consumer）、CLI `dev10`（CC producer），response 仍为 node1 `dev11`
（CC producer）、CLI `dev15`（NC consumer）。在无其他 benchmark 进程干扰的
重测中，双向 CC→NC 的 QPS 如下：

| 阶段 | QPS (ops/s) | p50 (us) | p99 (us) | correctness |
| --- | ---: | ---: | ---: | --- |
| prefill | 4,779 | 2,175 | 2,191 | pass |
| old keys read | 221,554 | 255 | 13,567 | pass |
| steady write | 4,974 | 2,175 | 2,191 | pass |
| old keys after steady | 216,588 | 271 | 13,759 | pass |
| steady keys read | 220,140 | 287 | 13,183 | pass |

与 response-only CC→NC 的约 11M–13M QPS 相比，request 也反转后仅约 0.22M
QPS，下降约 50–60 倍。由此确认 request 不适合沿用 response 的 CC→NC 方向。

## 结论

因此，吞吐从约 13M 降到约 6M 与 response ring 的 CC/NC 配置切换高度相关，
而不是由 steady 写入后的 old-key 额外退化造成。三个共同阶段中，CC→NC 回退
相对正确 NC→CC 分别约为 `1.92x`、`2.03x` 和 `1.79x`。双向 CC→NC 的重测
进一步表明，response 的最佳 CC→NC 方向不能直接应用于 request；request
反转会使 QPS 降至约 0.22M。因此当前 runner 的推荐配置是 request NC→CC、
response CC→NC，双向 NC→CC 和双向 CC→NC 均不是当前 workload 的最佳选择。

## 配置文件与启动切换

两套 active-ring 客户端 manifest 已固定为：

- 场景一（正确 NC→CC）：`examples/vemb_v16_ub_peer_view_111_to_112_active_nc_cc.yaml`
- 场景二（诊断 response CC→NC、request NC→CC）：`examples/vemb_v16_ub_peer_view_111_to_112_active.yaml`

场景二不是生产配置，只用于复现本报告的因果 A/B。runner 会切换 node1 的
response UB 启动参数，并保持 request 为 NC→CC：

```bash
# 场景一，默认；node1 response producer=dev15，CLI consumer=dev11
bash scripts/vemb_v16_ub_active_2node_111_to_112.sh --scenario nc_cc

# 场景二；request 为 node1=dev10/CLI=dev14，response 为 node1=dev11/CLI=dev15
bash scripts/vemb_v16_ub_active_2node_111_to_112.sh --scenario cc_nc
```

也可以使用环境变量覆盖场景默认值，例如：

```bash
SCENARIO=cc_nc TEST_TIME=30 PREFILL_KEYS=10000 STEADY_REREAD=0 \
  bash scripts/vemb_v16_ub_active_2node_111_to_112.sh
```

`--peer-view-manifest` 和 `CLIENT_MANIFEST_SOURCE` 指向 node0 远端可读取的
manifest；`--node1-request-path`/`NODE1_AERON_REQUEST_PATH` 和
`--node1-response-path`/`NODE1_AERON_RESPONSE_PATH` 分别指定 node1 启动时的
request/response UB path。它们必须与 manifest 中同一场景的 provider/client
方向保持一致，否则 ATTACH 会按错误的设备视图映射。
