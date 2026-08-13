# VEMB V16 Aeron lane snapshot 热路径优化数据汇报

## 1. 汇报结论

在相同的 `100K`、`PIO=16`、`SNW=16`、`t=64`、`c=4`、`pipeline=32`、`batch=32`、30 秒配置下，三组跨节点读取结果如下：

- Uniform `R:R`：`13.8276M QPS`，P99 `0.655ms`。
- Zipf `s=1.2`：`18.474646M QPS`，P99 `0.487ms`。
- Zipf `s=1.5`：`23.252459M QPS`，P99 `0.383ms`。

随着热点程度增加，Zipf `s=1.5` 相比 Uniform 吞吐提升约 `68.2%`，P99 降低约 `41.5%`，说明热点分布
能够更充分发挥 batch 聚合收益。

## 2. 测试条件

| 项目 | 配置 |
|---|---|
| 读取规模 | `100K` keys |
| workload | Uniform `R:R`、Zipf `s=1.2`、Zipf `s=1.5` |
| worker / client | `t=64`、`c=4` |
| pipeline / batch | `pipeline=32`、`batch=32` |
| lane 配置 | `PIO=16`、`SNW=16` |
| 测试时长 | `30s` |
| 结果选择 | Uniform 两轮中按 QPS 选择较好的一组；Zipf 保留两个场景 |
| 测试代码提交 | `b98ffae`（实验基线） |

## 3. 核心数据

| 分布 | run | QPS | P99 | server process cores | 测试代码提交 | 产物 |
|---|---|---:|---:|---:|---|---|
| Uniform `R:R` | `aeron_cross_20260807_171001` | 13.8276M | 0.655ms | 14.633 | `b98ffae` | [perf](../perf/aeron_cross_20260807_171001/) |
| Zipf `s=1.2` | `aeron_lane_snapshot_p16_s16_zipf12_frame_retry_r2_20260810_095505` | 18.474646M | 0.487ms | 13.176 | `b98ffae` | [perf](../perf/aeron_lane_snapshot_p16_s16_zipf12_frame_retry_r2_20260810_095505/) |
| Zipf `s=1.5` | `aeron_lane_snapshot_p16_s16_zipf15_frame_retry_20260810_095815` | 23.252459M | 0.383ms | 12.314 | `b98ffae` | [perf](../perf/aeron_lane_snapshot_p16_s16_zipf15_frame_retry_20260810_095815/) |


## 4. 聚合与向量读取统计

| 分布 | QPS | P99 | leaders | followers | frames | 平均 fanout | 测试代码提交 |
|---|---:|---:|---:|---:|---:|---:|---|
| Uniform `R:R` | 13.8276M | 0.655ms | 415,287,770 | 64,518 | 12,979,759 | 1.000155 | `b98ffae` |
| Zipf `s=1.2` | 18.474646M | 0.487ms | 417,064,260 | 137,447,836 | 17,328,503 | 1.329560 | `b98ffae` |
| Zipf `s=1.5` | 23.252459M | 0.383ms | 349,513,065 | 348,682,775 | 21,818,620 | 1.997624 | `b98ffae` |

平均 fanout 按 `(leaders + followers) / leaders` 计算。三个场景均汇总了 64 个 worker 的 `batch-session`
统计。

## 5. 稳定性与说明

三组测试均完成 64 个 worker join。Zipf 两组在 v2 response arena 临时可见性重试修复后完成复测，
最终 `fallback_v1`、publish failure、backpressure、stale/non-OK/unmatched response、vector read failure
和 handle dereference failure 均为 `0`。

Zipf 样本中的 response retry warning 表示 client 读到尚未完整可见的 response arena 后重试 descriptor，
最终不会消费该帧的部分内容；该 warning 不计为数据面失败。

原始记录见 [Aeron 跨节点进度文档](VEMB_V16_AERON_CROSS_NODE_PROGRESS_20260728.md) 的 21.18 节。

## 6. UB 配置修复后复测

UB 可见性配置修复后，使用相同的 `100K`、`t=64`、`c=4`、`pipeline=32`、`batch=32`、30 秒配置
复测 Uniform `R:R`，比较 `PIO:SNW=16:16`、`12:12` 与 `10:10`。

| 分布 | PIO:SNW | run | QPS | P99 | server process cores | 单 core QPS | 测试代码提交 | 产物 |
|---|---|---|---:|---:|---:|---:|---|---|
| Uniform `R:R` | `16:16` | `ubfix_p16_s16_rr_20260811_171000` | 8.071674M | 1.031ms | 12.310 | 0.6557M | `80b817de0d7c` | [perf](../perf/ubfix_p16_s16_rr_20260811_171000/) |
| Uniform `R:R` | `12:12` | `ubfix_p12_s12_rr_20260811_172200` | 8.091466M | 1.087ms | 10.190 | 0.7941M | `80b817de0d7c` | [perf](../perf/ubfix_p12_s12_rr_20260811_172200/) |
| Uniform `R:R` | `10:10` | `ubfix_p10_s10_rr_20260811_172600` | 7.959598M | 1.311ms | 8.882 | 0.8961M | `80b817de0d7c` | [perf](../perf/ubfix_p10_s10_rr_20260811_172600/) |

| 分布 | PIO:SNW | QPS | P99 | leaders | followers | frames | 平均 fanout | 测试代码提交 |
|---|---|---:|---:|---:|---:|---:|---:|---|
| Uniform `R:R` | `16:16` | 8.071674M | 1.031ms | 242,262,177 | 37,727 | 7,571,872 | 1.000156 | `80b817de0d7c` |
| Uniform `R:R` | `12:12` | 8.091466M | 1.087ms | 242,873,488 | 37,840 | 7,590,979 | 1.000156 | `80b817de0d7c` |
| Uniform `R:R` | `10:10` | 7.959598M | 1.311ms | 238,924,816 | 37,104 | 7,467,560 | 1.000155 | `80b817de0d7c` |

| PIO:SNW | worker join | 完成请求 | `publish_fail` / `unmatched` | non-OK / handle dereference failure | `fallback_v1` / backpressure | vector read failure / stale response |
|---|---:|---:|---:|---:|---:|---:|
| `16:16` | 64 / 64 | 242,299,904 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| `12:12` | 64 / 64 | 242,911,328 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| `10:10` | 64 / 64 | 238,961,920 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |

`12:12` 相比 `16:16` QPS 提升 `0.245%`，P99 增加 `5.432%`，server process cores 降低 `17.222%`。
`10:10` 相比 `12:12` QPS 降低 `1.630%`，P99 增加 `20.607%`。三组 flamegraph 均显示
`proxy_io_pool_thread_main` 存在交替轻载 lane；降低 lane 数未消除该现象，`12:12` 是三组中 QPS 最高的配置。

### 6.1 无统计插桩 lane 数复测

为排除计数器开销和换向部署的影响，恢复 `192.168.90.111` 为 server、`192.168.90.112` 为 CLI，
使用 server `0-47`、CLI `96-191` CPU 集合；测试窗口内清理 `mutagen-agent`。保持 `100K`、Uniform
`R:R`、`t=64`、`c=4`、`pipeline=32`、`batch=32` 和 30 秒配置，且构建不含 lane 统计插桩。

| 分布 | PIO:SNW | run | QPS | P99 | server process cores | 单 core QPS | 测试代码提交 | 产物 |
|---|---|---|---:|---:|---:|---:|---|---|
| Uniform `R:R` | `12:12` | `ubfix_p12_s12_original_cli96_nostats_20260811_185333` | 7.902345M | 1.111ms | 9.674 | 0.8169M | `80b817de0d7c` | [perf](../perf/ubfix_p12_s12_original_cli96_nostats_20260811_185333/) |
| Uniform `R:R` | `10:10` | `ubfix_p10_s10_original_cli96_nostats_20260811_190630` | 7.671689M | 1.327ms | 9.147 | 0.8387M | `80b817de0d7c` | [perf](../perf/ubfix_p10_s10_original_cli96_nostats_20260811_190630/) |
| Uniform `R:R` | `7:7` | `ubfix_p7_s7_original_cli96_nostats_20260811_190131` | 7.880579M | 1.087ms | 10.909 | 0.7224M | `80b817de0d7c` | [perf](../perf/ubfix_p7_s7_original_cli96_nostats_20260811_190131/) |
| Uniform `R:R` | `5:5` | `ubfix_p5_s5_original_cli96_nostats_20260811_190931` | 7.391747M | 1.335ms | 8.643 | 0.8552M | `80b817de0d7c` | [perf](../perf/ubfix_p5_s5_original_cli96_nostats_20260811_190931/) |

Zipf 复测保持 CLI `96-191`、`t=64`、`c=4`、`pipeline=32`、`batch=32` 和 30 秒配置，server
CPU 集合收敛为 `0-15`；测试窗口内无 Mutagen 干扰，构建同样不含 lane 统计插桩。

| 分布 | PIO:SNW | run | QPS | P99 | server process cores | 单 core QPS | 测试代码提交 | 产物 |
|---|---|---|---:|---:|---:|---:|---|---|
| Zipf `s=1.2` | `10:10` | `ubfix_p10_s10_svr0-15_zipf12_nostats_20260811_213700` | 8.998354M | 0.991ms | 7.530 | 1.1949M | `80b817de0d7c` | [perf](../perf/ubfix_p10_s10_svr0-15_zipf12_nostats_20260811_213700/) |
| Zipf `s=1.2` | `12:12` | `ubfix_p12_s12_svr0-15_zipf12_nostats_20260811_213300` | 10.071355M | 0.903ms | 9.013 | 1.1174M | `80b817de0d7c` | [perf](../perf/ubfix_p12_s12_svr0-15_zipf12_nostats_20260811_213300/) |
| Zipf `s=1.2` | `14:14` | `ubfix_p14_s14_svr0-15_zipf12_nostats_20260811_214300` | 11.350924M | 0.815ms | 10.657 | 1.0652M | `80b817de0d7c` | [perf](../perf/ubfix_p14_s14_svr0-15_zipf12_nostats_20260811_214300/) |
| Zipf `s=1.2` | `16:16` | `ubfix_p16_s16_svr0-15_zipf12_nostats_20260811_215100` | 11.645365M | 0.903ms | 12.149 | 0.9586M | `80b817de0d7c` | [perf](../perf/ubfix_p16_s16_svr0-15_zipf12_nostats_20260811_215100/) |

| 分布 | PIO:SNW | run | QPS | P99 | server process cores | 单 core QPS | 测试代码提交 | 产物 |
|---|---|---|---:|---:|---:|---:|---|---|
| Zipf `s=1.5` | `10:10` | `ubfix_p10_s10_svr0-15_zipf15_nostats_20260811_214000` | 10.775114M | 0.823ms | 7.085 | 1.5207M | `80b817de0d7c` | [perf](../perf/ubfix_p10_s10_svr0-15_zipf15_nostats_20260811_214000/) |
| Zipf `s=1.5` | `12:12` | `ubfix_p12_s12_svr0-15_zipf15_nostats_20260811_213500` | 11.937860M | 0.743ms | 8.443 | 1.4140M | `80b817de0d7c` | [perf](../perf/ubfix_p12_s12_svr0-15_zipf15_nostats_20260811_213500/) |
| Zipf `s=1.5` | `14:14` | `ubfix_p14_s14_svr0-15_zipf15_nostats_20260811_214600` | 14.102431M | 0.647ms | 10.083 | 1.3986M | `80b817de0d7c` | [perf](../perf/ubfix_p14_s14_svr0-15_zipf15_nostats_20260811_214600/) |
| Zipf `s=1.5` | `16:16` | `ubfix_p16_s16_svr0-15_zipf15_nostats_20260811_220000` | 15.348211M | 0.591ms | 11.235 | 1.3661M | `80b817de0d7c` | [perf](../perf/ubfix_p16_s16_svr0-15_zipf15_nostats_20260811_220000/) |

`14:14` 两组的 `fallback_v1`、`publish_fail`、`unmatched`、non-OK、`handle_deref fail`、
`shared_vector_read_failures`、backpressure 与 stale response 均为 `0`。相较 `12:12`，QPS 在
Zipf `s=1.2` 和 `s=1.5` 分别提高 `12.705%` 和 `18.132%`；后者达到 `14.102431M QPS`。
`16:16` 两组的相同数据面与 handle dereference 计数也均为 `0`；Zipf `s=1.5` 达到
`15.348211M QPS`、P99 `0.591ms`。

在 `0-15` server CPU 集合下，`12:12` 相比 `10:10` 在 Zipf `s=1.2` 和 `s=1.5` 分别高
`11.924%` 和 `10.791%`；`10:10` 的单 core QPS 更高，但总 QPS 与 P99 均不及 `12:12`。

为比较 CLI 在途深度和批聚合粒度，保持 `12:12`、server `0-15`、CLI `96-191`、`100K`、Uniform
`R:R`、`t=64`、`c=4` 和 30 秒配置。`64` 组将 CLI `pipeline`、CLI `batch_size`、server
`batch-request-size`，以及 server 编译期 `PROXY_REQUEST_BATCH`、`PROXY_RESPONSE_BATCH`、
`PROXY_QUEUE_BATCH`（同时为 `VEMB_V16_SUPERNODE_BATCH`）全部设为 `64`。

| 分布 | PIO:SNW | run | QPS | P99 | server process cores | 单 core QPS | 测试代码提交 | 产物 |
|---|---|---|---:|---:|---:|---:|---|---|
| Uniform `R:R`（全部 batch=`64`） | `12:12` | `ubfix_p12_s12_svr0-15_uniform_allbatch64_nostats_20260812_092800` | 7.826773M | 2.527ms | 10.067 | 0.7775M | `80b817de0d7c` | [perf](../perf/ubfix_p12_s12_svr0-15_uniform_allbatch64_nostats_20260812_092800/) |
| Uniform `R:R`（`pipeline=batch=128`，仅 runtime） | `12:12` | `ubfix_p12_s12_svr0-15_uniform_pipe128_batch128_nostats_20260812_001000` | 7.771201M | 4.799ms | 10.486 | 0.7411M | `80b817de0d7c` | [perf](../perf/ubfix_p12_s12_svr0-15_uniform_pipe128_batch128_nostats_20260812_001000/) |

完整 `64` 组的 `fallback_v1`、`publish_fail`、`unmatched`、non-OK、`handle_deref fail`、
`shared_vector_read_failures`、backpressure 与 stale response 均为 `0`。其 QPS 为 `7.826773M`、
P99 为 `2.527ms`，相较此前仅改运行期 batch 的错误 `64` 记录（`7.792914M`）没有实质差异。
`128` 行尚未将三项 server 内部 batch 重编译为 `128`，因此不是完整对照，待复测后替换；该 CPU
集合下未测全部 batch=`32` 对照，故不将此表与前述 server `0-47` 的 Uniform 数据直接作定量比较。

在上述 Uniform 复测中，`12:12` 保持最高 QPS；`7:7` 仅低 `0.275%`，且 P99 最低（`1.087ms`）。奇数 lane 数消除了
channel index 奇偶性导致的半数 lane 固定空闲，但每条 lane 的负载相应提高，未超过 `12:12` 的总 QPS。
`10:10` 和 `5:5` 相比 `12:12` 分别低 `2.919%` 和 `6.461%`，P99 分别高 `21.242%` 和 `20.162%`。

## 7. `12:12` lane 计数器诊断（换向部署）

为排除 server 侧环境干扰，使用 `192.168.90.112` 作为 server、`192.168.90.111` 作为 CLI，保持
`100K`、`R:R`、`t=64`、`c=4`、`pipeline=32`、`batch=32` 和 30 秒设定。UB 资源随角色完整换向：
server 为 request `/dev/obmm_shmdev3`、response `/dev/obmm_shmdev6`、warm `/dev/obmm_shmdev4`；
client 对应 `/dev/obmm_shmdev7`、`/dev/obmm_shmdev2`、`/dev/obmm_shmdev8`。两轮 server 负载门禁均通过。
首轮 CLI 仅绑 `0-47`，第二轮恢复换向前的 `96-191`，并在整个 workload 窗口持续杀死
`mutagen-agent`；第二轮 CLI 标准负载门禁为 `3.84%`。

| 分布 | PIO:SNW | run | QPS | P99 | server process cores | 单 core QPS | 测试代码提交 | 产物 |
|---|---|---|---:|---:|---:|---:|---|---|
| Uniform `R:R` | `12:12` | `ubfix_p12_s12_reversed_clientbusy_20260811_182456` | 7.309778M | 4.575ms | 10.052 | 0.7272M | `80b817de0d7c` | [perf](../perf/ubfix_p12_s12_reversed_clientbusy_20260811_182456/) |
| Uniform `R:R` | `12:12` | `ubfix_p12_s12_reversed_cli96_nomutagen_20260811_183752` | 7.890790M | 1.119ms | 10.418 | 0.7574M | `80b817de0d7c` | [perf](../perf/ubfix_p12_s12_reversed_cli96_nomutagen_20260811_183752/) |

| lane 类型 | 有效 lane | 空闲 lane | 有效 lane 请求数 | 有效 lane 均值 | 最小 / 最大 | max-min / 均值 | 测试代码提交 |
|---|---|---|---:|---:|---:|---:|---|
| PIO | `0,2,4,6,8,10`（6 / 12） | `1,3,5,7,9,11` | 219,488,280 | 36,581,380 | 34,400,148 / 38,291,070 | 10.636% | `80b817de0d7c` |
| SNW | `0,2,4,6,8,10`（6 / 12） | `1,3,5,7,9,11` | 219,488,280 | 36,581,380 | 34,400,148 / 38,291,070 | 10.636% | `80b817de0d7c` |

第二轮的相同 6 条有效 PIO/SNW 分别处理 236,814,631 个请求，最小 / 最大为
39,220,260 / 39,694,233，离散度仅 `1.201%`；PIO request、completion、returned-job 与 SNW dequeue、
execute 均相等。恢复 CLI 到 96 核并清理 Mutagen 后，QPS 较首轮提升 `7.949%`，P99 从 4.575ms 降至
1.119ms，故首轮的主要额外限制是 CLI 的 64 worker 在 48 核上的过度订阅，而非 server 或 UB 换向。

每条有效 PIO 的 request、completion 和 returned-job 完全相等；对应 SNW 的 dequeue 和 execute 也完全相等，
因此没有丢包、重复或 PIO/SNW 间转移不平衡。问题在 channel-to-lane 分配：每个 batch session 固定创建一个
legacy v1 fallback channel 和一个 v2 batch channel，二者占用连续 channel index；本次预填充先占用一个
legacy channel，v2 channel 随后固定落在偶数 index。当前 server 以 `channel_index % lane_count` 选 lane，
所以 `PIO=SNW=12` 将所有正常 v2 流量固定映射到一半 lane。此现象与 UB request/response/warm 的换向无关；
奇数 lane 数（例如此前 `11:11`）会因取模改变奇偶性而覆盖全部 lane。

## 8. CLI Cache `12:12` 测试
在前述 lane 基线上接入 worker-local CLI L1 completed-vector cache 后，使用 `L1_ENTRIES=4096`、
server `0-15`、CLI `96-191`，保持 `100K`、`t=64`、`c=4`、`pipeline=32`、`batch=32`、
`max_delay=0` 和 30 秒配置。四场启动前均通过 build stamp、全主机 workload gate 与目标 CPU-set
gate；server 均确认有 12 个 `vemb-io-*` 和 12 个 `vemb-sn-*` 线程。

| 分布 | PIO:SNW | run | QPS | P99 | 当前 L1 hit | server process cores | 单 core QPS | 测试代码提交 | 产物 |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| Uniform `R:R` | `12:12` | `l1p4_uniform_enabled_p12_s12_svr0_15_wavefix_20260812_1732` | 7.944863M | 1.391ms | 4.0925% | 10.291 | 0.7720M | `5ab79d39` + L1 working tree | [perf](../perf/l1p4_uniform_enabled_p12_s12_svr0_15_wavefix_20260812_1732/) |
| Zipf `s=1.0` | `12:12` | `l1p4_zipf10_enabled_p12_s12_svr0_15_wavefix_20260812_1753` | 13.190124M | 0.703ms | 59.3575% | 8.973 | 1.4699M | `5ab79d39` + L1 working tree | [perf](../perf/l1p4_zipf10_enabled_p12_s12_svr0_15_wavefix_20260812_1753/) |
| Zipf `s=1.2` | `12:12` | `l1p4_zipf12_enabled_p12_s12_svr0_15_wavefix_20260812_1735` | 17.628362M | 0.511ms | 83.9782% | 8.397 | 2.0994M | `5ab79d39` + L1 working tree | [perf](../perf/l1p4_zipf12_enabled_p12_s12_svr0_15_wavefix_20260812_1735/) |
| Zipf `s=1.5` | `12:12` | `l1p4_zipf15_enabled_p12_s12_svr0_15_wavefix_20260812_1739` | 39.550067M | 0.383ms | 97.6277% | 7.841 | 5.0442M | `5ab79d39` + L1 working tree | [perf](../perf/l1p4_zipf15_enabled_p12_s12_svr0_15_wavefix_20260812_1739/) |

`server process cores` 为 redis-server 的 user+system CPU time / 30 秒 wall time；`单 core QPS`
按该 process core-equivalent 计算。所有样本的 `fallback_v1`、backpressure、non-OK、unmatched
response、handle/shared-vector read failure、L1 queue-full、all-pinned、key/vector storage exhaustion
和 stale ref 均为 `0`。uniform 远端 L0 聚合为 30.686 items/frame，已恢复接近 32-item batch。

本表是 L1 enabled-only 功能与资源验证：用户要求不跑 4-key，且当前分支没有开发 L1 的版本才是
disabled 版本。因此不与本报告历史无 L1 lane 数据计算 A/B 性能收益，也不以该表改变原有 lane 数结论。
