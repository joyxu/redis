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
| 工作区状态 | `基线 b98ffae + 未提交 diff`；结果目录应归档对应 diff |

## 3. 核心数据

| 分布 | run | QPS | P99 | server process cores | 测试代码提交 | 工作区状态 | 产物 |
|---|---|---:|---:|---:|---|---|---|
| Uniform `R:R` | `aeron_cross_20260807_171001` | 13.8276M | 0.655ms | 14.633 | `b98ffae` | `基线 b98ffae + 未提交 diff` | [perf](../perf/aeron_cross_20260807_171001/) |
| Zipf `s=1.2` | `aeron_lane_snapshot_p16_s16_zipf12_frame_retry_r2_20260810_095505` | 18.474646M | 0.487ms | 13.176 | `b98ffae` | `基线 b98ffae + 未提交 diff` | [perf](../perf/aeron_lane_snapshot_p16_s16_zipf12_frame_retry_r2_20260810_095505/) |
| Zipf `s=1.5` | `aeron_lane_snapshot_p16_s16_zipf15_frame_retry_20260810_095815` | 23.252459M | 0.383ms | 12.314 | `b98ffae` | `基线 b98ffae + 未提交 diff` | [perf](../perf/aeron_lane_snapshot_p16_s16_zipf15_frame_retry_20260810_095815/) |


## 4. 聚合与向量读取统计

| 分布 | QPS | P99 | leaders | followers | frames | 平均 fanout | 测试代码提交 | 工作区状态 |
|---|---:|---:|---:|---:|---:|---:|---|---|
| Uniform `R:R` | 13.8276M | 0.655ms | 415,287,770 | 64,518 | 12,979,759 | 1.000155 | `b98ffae` | `基线 b98ffae + 未提交 diff` |
| Zipf `s=1.2` | 18.474646M | 0.487ms | 417,064,260 | 137,447,836 | 17,328,503 | 1.329560 | `b98ffae` | `基线 b98ffae + 未提交 diff` |
| Zipf `s=1.5` | 23.252459M | 0.383ms | 349,513,065 | 348,682,775 | 21,818,620 | 1.997624 | `b98ffae` | `基线 b98ffae + 未提交 diff` |

平均 fanout 按 `(leaders + followers) / leaders` 计算。三个场景均汇总了 64 个 worker 的 `batch-session`
统计。

## 5. 稳定性与说明

三组测试均完成 64 个 worker join。Zipf 两组在 v2 response arena 临时可见性重试修复后完成复测，
最终 `fallback_v1`、publish failure、backpressure、stale/non-OK/unmatched response、vector read failure
和 handle dereference failure 均为 `0`。

Zipf 样本中的 response retry warning 表示 client 读到尚未完整可见的 response arena 后重试 descriptor，
最终不会消费该帧的部分内容；该 warning 不计为数据面失败。

原始记录见 [Aeron 跨节点进度文档](VEMB_V16_AERON_CROSS_NODE_PROGRESS_20260728.md) 的 21.18 节。
