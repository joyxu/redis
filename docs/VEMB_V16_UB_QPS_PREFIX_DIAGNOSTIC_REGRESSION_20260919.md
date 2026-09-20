# VEMB V16 UB QPS、Key 前缀、诊断开销与 COLD 持久化回归记录

测试日期：2026-09-19。本文汇总当日相关回归，不修改业务代码，不代表新的性能优化提交。

## 1. 结论摘要

- 在相同 `4383547 + 撤销 f53ce263` 测试代码下，仅将 key 前缀从 49 字节改为 `item:` 的 5 字节，5:5 QPS 从 7.414M 升至 7.816M，提升 5.42%；6:6 从 7.533M 升至 7.995M，提升 6.13%。此前约 5%--6% 的差距主要来自前缀不一致，不能全部归为 merge 或诊断代码回退。
- 对齐 `item:` 后，撤销诊断版 5:5 为 7.816M，与 `dev_aeron_cluster` 的 `23018eb` 基线 7.877M 仅差 -0.78%。这些单轮样本不支持“大幅代码性能回退”的结论。
- 在 `item:` 下恢复完整 `4383547` 后，5:5 为 7.840M，6:6 为 8.019M；相对撤销诊断版分别为 +0.32%、+0.29%，报告 QPS 基本一致。
- 总 QPS 相近不等于 CPU 成本相同。撤销诊断版的 Server 单核 QPS 分别高 4.35% 和 4.12%；恢复诊断后 Server CPU 核数分别增加 4.68% 和 4.42%。这支持存在 CPU 成本，但不是纯诊断开销的严格定量结论。
- 本轮不能解释历史文档中的 9.571M。回到文档对应的 `c20eeb7` 后，三轮均值为 8.090M；跨版本 CLI 路径和计时口径也有差异。
- `dev_cold_ha` 的 `cfd2cf4` 在单 Server、HA 关闭、`item:` 下完成 COLD 开关两轮对照。5:5 的开启相对关闭 QPS 变化为 -1.22%、+0.04%；6:6 为 -0.86%、-1.13%。稳态纯读性能整体接近，但 6:6 两轮均低约 1%，不能断言完全零影响。
- COLD 开启样本均核验了 100,000 条 AOF 记录及校验和；计时前完成同步，读测期间 AOF 未变化、Server 磁盘读写字节无增长，`cold_promote=0`，无读错误。结果支持 COLD 不参与本次读路径，不涵盖持续写入或刷盘与读取并发的场景。
- `c20eeb7` 基线重复三轮，COLD 开关对照各配置各有两次有效样本，其余配置为单轮。COLD 补充轮按同线程配置关闭后开启相邻测试；没有随机化多轮交错、置信区间或严格同窗 CPU/ops 采样，不将小幅差异认定为稳定收益或已隔离的固定成本。

## 2. 代码定位与工作区

| 简写 | 完整 commit | 用途 |
|---|---|---|
| `23018eb` | `23018eb9041a85f38762c0b23b47df007387033c` | 当日 `dev_aeron_cluster` 基线 |
| `c20eeb7` | `c20eeb74ec56785c1abd749f2ca74b3a5101a68b` | 历史 P3 SNW idle clock sampling 检查点 |
| `4383547` | `4383547b771a7f03c428b161af2485a3449bd4b8` | `merge branch dev_aeron_cluster into dev` |
| `f53ce263` | `f53ce263c5513ebfce40d8b6fe1b7e854762ac29` | `feat(vemb-v16): add UB diagnostic instrumentation and latency coverage` |
| `cfd2cf4` | `cfd2cf4776d4584e96120a20e97919d662a10dc3` | `dev_cold_ha` 指定检查点，单 Server COLD 持久化开关对照 |

`f53ce263` 在 `4383547` 的祖先链中。同名的 `839e775dcb8c854727ea17eccd72b101b487473a` 不在该祖先链中，本次没有撤销它。

文中“撤销诊断版”不是一个新 commit，而是：

```text
HEAD = 4383547b771a7f03c428b161af2485a3449bd4b8
工作区 = 基线 4383547 + 未提交 diff
来源 = git revert --no-commit f53ce263
测试分支 = bench/4383547-without-f53ce263-20260919
```

撤销涉及 19 个文件。SDK batch publish 路径发生冲突，解决时保留后续加入的 `v2_batches_outstanding` 协议状态，包括 publish 前递增、publish 失败回退和 response 消费后递减，只撤去对应诊断部分。两台补丁一致：

```text
SHA256 = cbebb0d4edae4f1d2801787ac73369670c671f09dff0ed548c09990dc6a89b1a
```

完整撤销不等于只禁用计数器：该提交还包含 close/ACK 行为、日志开关、benchmark 报告及测试工具变化。撤销后恢复了旧版部分调试日志，不能把结果全部归因于统计计数。

恢复诊断时，先备份撤销版产物，再反向应用上述补丁，确认工作区干净，重新编译。诊断对照结束时两台均为干净的 `4383547`，未创建撤销 commit；测试分支名称本身不能复原之前的未提交改动，必须使用归档补丁。

后续按要求将两台切到 `cfd2cf4`，完整重编译 Server/CLI 并校验 build stamp，再进行第 5.6 节的对照。COLD 测试结束时两台均为干净的 `cfd2cf4`，未改动业务代码；启停 COLD 和校验 AOF 仅使用仓库外的临时 runner。此前完整 `4383547` 的构建产物已另外备份。

## 3. 固定配置

| 项目 | 配置 |
|---|---|
| Server | 111，`192.168.90.111:6395` |
| CLI | 112，`192.168.90.112` |
| 两端仓库 | `/root/szz/codespace/hpc-redis` |
| 调度位置 | 在 111 执行脚本，SSH 到本机和 112，结果归档在 111 |
| workload | 100,000 keys，Uniform `R:R`，纯 HANDLE 读；预填充 `S:S` |
| 向量 | dim=300，1200 bytes |
| 客户端并发 | `THREADS=64 CLIENTS=4` |
| batch/pipeline | 均为 32；内部 request/response/queue batch 均为 32 |
| 其他参数 | `BATCH_MAX_DELAY_US=0 L1_ENTRIES=0 MAX_VECTORS=131072` |
| CPU mask | Server `0-15`，CLI `96-191` |
| 时长 | 配置压测 30s；Server perf 25s，cycles@99Hz |
| 构建 | 两端重新编译并校验 build stamp；`USE_SVE=yes`，O3/LTO |
| Server OPT | `-O3 -flto -fno-omit-frame-pointer` |
| warm manifest | region 1，`/dev/obmm_shmdev4`，offset=0，bytes=4294967296 |
| 安全开关 | `KILL_OPENCODE=0 KILL_MUTAGEN=0`；保留脚本负载及 UB 占用检查 |

目标 `4383547` 自带示例 manifest 的 warm bytes 为 1 GiB，因此本轮显式指定之前备份的 4 GiB manifest，避免默认值改变对照：

```text
/root/szz/codespace/hpc-redis_23018eb_artifacts_20260919_c20/examples/vemb_perf_warm_111.yaml
```

UB 设备沿用已验证映射，不为此次性能对照另改缓存策略：

| 数据路径 | 111 Server | 112 CLI | 写入方到读取方 |
|---|---|---|---|
| request | `/dev/obmm_shmdev3`，CC | `/dev/obmm_shmdev7`，NC | CLI NC -> Server CC |
| response | `/dev/obmm_shmdev6`，NC | `/dev/obmm_shmdev2`，CC | Server NC -> CLI CC |
| warm | `/dev/obmm_shmdev4`，CC | `/dev/obmm_shmdev8`，NC | Server 写，CLI 读 |

request/response 的 channel offset 由 ATTACH 动态分配，不是全部固定为 0。warm region 从 offset 0 映射，本轮旧布局 payload offset 为 229064960。

两种 key 前缀定义如下，冒号也是 key 的一部分：

```text
短前缀：item:
长度：5 bytes

长前缀：vemb-cross-flame-aeron_c20eeb7_p6_s6_20260919_r1:
长度：49 bytes
```

前缀改变同时影响 key 长度、哈希值、缓存分布、比较和编码成本。本文没有单独分离“长度”和“哈希分布”的贡献。

## 4. 指标定义

- QPS：`client.workload.log` 的 `Totals` logical ops/sec；表中 M=1,000,000。
- P99：同一 `Totals` 的 P99 latency，单位 ms。旧 Aeron runner 与 common-core 的列布局不同，解析时不能复用相同列号。
- CPU core：`server.cpu.process.tsv` 的 `total core_equiv`，即 Server 进程 user+system CPU 秒数除以约 30 秒采样窗口；不是 PIO/SNW 线程数，也不是整机 CPU-set 的 busy core。
- 单核 QPS：报告 QPS / Server CPU core，是派生效率指标，不是单线程 benchmark 的吞吐。
- QPS 的有效时长与 Server CPU 的采样窗口不完全一致。尤其 common-core 的报告窗口包含初始化、统计和 session 收尾。因此单核 QPS 不能直接等同于严格同窗的 steady-state CPU/op。
- 下表使用原始数据计算后再显示，避免用三位小数反算百分比。

## 5. 测试数据

### 5.1 `dev_aeron_cluster` 当日基线

均使用 `item:`。A1 是后续 5:5 的主要参考，A2--A5 保留作线程数和 PROFILE 背景对照。

| ID | 测试代码提交 | 工作区状态 | PIO:SNW | PROFILE | QPS (M) | P99 (ms) | CPU core | 单核 QPS (M/core) |
|---|---|---|---|---:|---:|---:|---:|---:|
| A1 | `23018eb` | 干净 | 5:5 | 1 | 7.877410 | 1.455 | 8.449813 | 0.932258 |
| A2 | `23018eb` | 干净 | 7:7 | 1 | 8.045840 | 1.223 | 10.632799 | 0.756700 |
| A3 | `23018eb` | 干净 | 10:10 | 1 | 7.948564 | 1.215 | 13.843364 | 0.574179 |
| A4 | `23018eb` | 干净 | 7:7 | 0 | 7.914906 | 1.215 | 10.606944 | 0.746200 |
| A5 | `23018eb` | 干净 | 6:6 | 0 | 7.844436 | 1.215 | 9.522420 | 0.823786 |

### 5.2 历史检查点 `c20eeb7` 重测

均为 6:6、长前缀，使用该提交自带旧脚本和 `aeron-cross-node` runner，开启 perf。旧脚本没有 `PROFILE` 开关，不能靠传入 `PROFILE=0` 禁用采样。

| ID | 测试代码提交 | 工作区状态 | QPS (M) | P99 (ms) | CPU core | 单核 QPS (M/core) |
|---|---|---|---:|---:|---:|---:|
| B1 | `c20eeb7` | 干净 | 8.080164 | 1.111 | 9.964451 | 0.810899 |
| B2 | `c20eeb7` | 干净 | 8.082752 | 1.111 | 9.958664 | 0.811630 |
| B3 | `c20eeb7` | 干净 | 8.105718 | 1.111 | 9.982651 | 0.811980 |

三轮 QPS 均值 8.089544M，比历史文档的 9.571227M 低 15.48%。历史值见 [跨机阶段总结第 22.10 节](VEMB_V16_AERON_CROSS_NODE_PROGRESS_20260728.md)。历史原始日志显示测试二进制为 `a5e8ec91, modified=1`，文档将最终优化检查点归为 `c20eeb7`；不要将历史样本描述为已核验的干净 `c20eeb7` 构建。

### 5.3 `4383547` 长前缀对照

均开启 perf。C1 的“诊断存在”表示完整提交原样运行；C2/C3 是撤销整个 `f53ce263` 的实验，不是运行时关掉一个统计开关。

| ID | 测试代码提交 | 工作区状态 | PIO:SNW | QPS (M) | P99 (ms) | CPU core | 单核 QPS (M/core) |
|---|---|---|---|---:|---:|---:|---:|
| C1 | `4383547` | 干净，诊断存在 | 6:6 | 7.589412 | 1.303 | 10.335037 | 0.734338 |
| C2 | `4383547` | 基线 `4383547` + 未提交撤销 diff | 6:6 | 7.533392 | 1.311 | 9.971835 | 0.755467 |
| C3 | `4383547` | 基线 `4383547` + 未提交撤销 diff | 5:5 | 7.413743 | 1.583 | 8.914360 | 0.831663 |

### 5.4 `item:` 对齐后的诊断对照

均开启 perf。D1/D2 与 C3/C2 分别只改变 key 前缀；D3/D4 恢复完整 `4383547` 并重新编译，保持短前缀及对应线程数。

| ID | 测试代码提交 | 工作区状态 | PIO:SNW | QPS (M) | P99 (ms) | CPU core | 单核 QPS (M/core) |
|---|---|---|---|---:|---:|---:|---:|
| D1 | `4383547` | 基线 `4383547` + 未提交撤销 diff | 5:5 | 7.815781 | 1.503 | 8.440771 | 0.925956 |
| D2 | `4383547` | 基线 `4383547` + 未提交撤销 diff | 6:6 | 7.995494 | 1.239 | 9.595607 | 0.833245 |
| D3 | `4383547` | 干净，诊断恢复 | 5:5 | 7.840456 | 1.367 | 8.835977 | 0.887333 |
| D4 | `4383547` | 干净，诊断恢复 | 6:6 | 8.018542 | 1.239 | 10.019291 | 0.800310 |

### 5.5 完成量与 QPS 时间窗口

本表用于避免把分母变化误认为稳态吞吐变化。不能直接用完成量除以配置的 30s 替代原始 QPS，因为每 worker 的初始化、排空和统计边界并不相同。

| ID | 测试代码提交 | 工作区状态 | logical ops | 有效时长 (s) |
|---|---|---|---:|---:|
| A1 | `23018eb` | 干净 | 251428510 | 31.917664 |
| C1 | `4383547` | 干净 | 235859010 | 31.077376 |
| C2 | `4383547` | 基线 `4383547` + 未提交撤销 diff | 241282882 | 32.028450 |
| C3 | `4383547` | 基线 `4383547` + 未提交撤销 diff | 237222773 | 31.997708 |
| D1 | `4383547` | 基线 `4383547` + 未提交撤销 diff | 249699333 | 31.948097 |
| D2 | `4383547` | 基线 `4383547` + 未提交撤销 diff | 254998755 | 31.892809 |
| D3 | `4383547` | 干净 | 242165538 | 30.886667 |
| D4 | `4383547` | 干净 | 242432217 | 30.233951 |
| E1 | `cfd2cf4` | 干净，COLD 关闭 | 248902128 | 31.270196 |
| E2 | `cfd2cf4` | 干净，COLD 关闭 | 249960288 | 30.986185 |
| E3 | `cfd2cf4` | 干净，COLD 开启 | 248758919 | 31.639254 |
| E4 | `cfd2cf4` | 干净，COLD 开启 | 248608926 | 31.085704 |
| E5 | `cfd2cf4` | 干净，COLD 关闭 | 243393504 | 30.710877 |
| E6 | `cfd2cf4` | 干净，COLD 开启 | 245473307 | 30.960275 |
| E7 | `cfd2cf4` | 干净，COLD 关闭 | 253553220 | 31.466845 |
| E8 | `cfd2cf4` | 干净，COLD 开启 | 243082481 | 30.511499 |

### 5.6 `cfd2cf4` 单 Server COLD 持久化对照

两端均为完整 `cfd2cf4`，诊断代码保留。沿用第 3 节的 `item:`、4 GiB warm、UB 设备映射和 CPU mask，全部 `PROFILE=1`。没有启用 HA Replica：Server 的 `HPC_REDIS_HA_ROLE` 未设置，持久化记录的 `ha_term=0`。

COLD 通过 **Server 启动环境**中的 `HPC_REDIS_COLD_DIR` 启用，每次使用独立的新目录。不能只在调度 shell 中设置变量后假设 SSH 会转发。关闭组不设置该变量或传入空值；开启组使用下列提交内默认配置：

| COLD 参数 | 值 |
|---|---:|
| segment_bytes | 67108864 (64 MiB) |
| queue_capacity | 131072 |
| group_max_entries | 64 |
| group_max_delay_us | 1000 |
| retention_events | 1048576 |

预填充 PUT 使用 `TLC_COLD_ACK_ACCEPTED`，表示已追加 AOF，不等于每条同步落盘。开启组在预填充后等待 2 秒、执行 `sync -f <cold_dir>`，校验全部 AOF 记录后才开始 perf 和读测。补充轮关闭组也等待 2 秒。未读取运行时 `durable_seq`；本次以同步完成及文件校验作为计时前持久化证据，不是对正常 group commit 耐久 ACK 语义的独立验证。

首轮执行顺序为 E1、E2、E3、E4；补充轮为 E5、E6、E7、E8。同线程配置的补充轮关闭/开启相邻执行，减少时间段变化影响。

| ID | 轮次 | PIO:SNW | COLD | QPS (M) | P99 (ms) | CPU core | 单核 QPS (M/core) |
|---|---|---|---|---:|---:|---:|---:|
| E1 | 首轮 | 5:5 | 关闭 | 7.959724 | 1.415 | 8.934364 | 0.890911 |
| E2 | 首轮 | 6:6 | 关闭 | 8.066830 | 1.239 | 9.986370 | 0.807784 |
| E3 | 首轮 | 5:5 | 开启 | 7.862351 | 1.431 | 8.916345 | 0.881791 |
| E4 | 首轮 | 6:6 | 开启 | 7.997532 | 1.239 | 10.048148 | 0.795921 |
| E5 | 补充轮 | 5:5 | 关闭 | 7.925319 | 1.447 | 8.935652 | 0.886932 |
| E6 | 补充轮 | 5:5 | 开启 | 7.928654 | 1.423 | 8.893338 | 0.891527 |
| E7 | 补充轮 | 6:6 | 关闭 | 8.057790 | 1.239 | 10.064205 | 0.800638 |
| E8 | 补充轮 | 6:6 | 开启 | 7.966914 | 1.239 | 9.997068 | 0.796925 |

E1/E2 相对完整 `4383547` 的 D3/D4，QPS 分别为 +1.52%、+0.60%。这组 COLD/HA 均关闭的样本没有显示明显 warm 读取回退，但不能用它替代 COLD 开启后的对照，更不代表已验证 HA Replica 开启的成本。

## 6. 结论与限制

### 6.1 Key 前缀是已验证的重要变量

C3 -> D1 和 C2 -> D2 的代码、构建、线程数及其余配置不变，前缀缩短后 QPS 分别提高 5.42% 和 6.13%。两个线程配置方向一致，支持此前大部分差距来自 workload 输入变化。

不能反过来声称“merge 没有任何性能影响”：D1 相对 A1 尚有 -0.78%，且没有交错重复。也不能将该结果推广为任意长 key 都必然下降同样比例。

### 6.2 撤销诊断版的单核效率更高

用 D1 对 D3、D2 对 D4 比较：

- 5:5：0.925956M/core 对 0.887333M/core，撤销版高 4.35%。
- 6:6：0.833245M/core 对 0.800310M/core，撤销版高 4.12%。
- 恢复诊断后的总 QPS 分别仅高 0.32% 和 0.29%，但 CPU 分别高 4.68% 和 4.42%。

这说明不能只看总 QPS 判断诊断开销。但完整提交撤销包含非统计改动，各组只有单轮，QPS 与 CPU 也不是同窗采样。因此“约 4%”是当前派生指标的观测优势，不是已隔离的纯诊断 CPU 成本。

### 6.3 代码与火焰图线索

- `f53ce263` 给 `tlc_core_get_warm_location_raw()` 增加 cache hit/miss 计数，并经 `note_lookup_location()` 更新 local/imported hit 和 region hit。正常 warm 命中路径新增三次共享原子加法；relaxed memory order 不消除 cacheline 争用。
- `tlc_counter_add()` 从空操作改为原子加法。其各调用路径是否命中，需要按 workload 分开判断，不能把所有计数都算到本轮本地读上。
- SDK 增加 route/channel/v2-enable 等阶段时间戳；部分时间戳全量采集，最终汇总虽有 1/1024 抽样，也不等于只付出抽样计时成本。
- common-core 在统计输出、diagnostic 查询、session close 之后才结束计时。该结构在 `f53ce263` 之前已存在；此提交增加窗口内工作，而不是首次移动结束计时点。
- 撤销后仍存在 VSIM_KEY_KEY 等后续变更。由二进制调试类型信息核对，`sdk_handle_session_request_t` 在 `23018eb` 为 208 bytes，在撤销诊断版为 336 bytes。普通 HANDLE 也使用该结构，存在缓存足迹和初始化成本候选，但尚未做单独 A/B。
- A1 与 C3 的 Server warm lookup 叶子采样占比分别为 22.19%、27.57%，CLI route 分别为 12.84%、14.46%。这是相对采样占比，不是绝对每请求耗时，且两轮 key 前缀不同，不能单凭百分比证明代码退化。
- 撤销诊断版相对 `23018eb` 的 `tlc_core.c`、`vemb_v16_tlc.c`、`vemb_v16_supernode.c` 已一致；已有热路径的开销仍可能因输入、数据布局及调度变化而变化。

### 6.4 未得出的结论

没有证明 common-sdk 对性能完全无影响，没有证明诊断零开销，也没有复现历史 9.571M。A2/A4 的 PROFILE 对照未显示关闭 perf 带来提升，但不能据此推断所有版本、环境都与 perf 无关。

如需严格归因，下一步应保持 `item:`，用同窗 ops/CPU 计量，交错重复；只关闭 Server 诊断计数、只关闭 CLI 诊断采集、两端同时关闭分别测试，并保持协议状态、日志策略和计时边界不变。这些实验尚未执行。

### 6.5 COLD 不参与本次读路径，性能尚不能称为完全零影响

同轮同线程配置，开启相对关闭的 QPS 变化：

| 轮次 | PIO:SNW | 关闭 -> 开启 | QPS 变化 |
|---|---|---|---:|
| 首轮 | 5:5 | E1 -> E3 | -1.22% |
| 首轮 | 6:6 | E2 -> E4 | -0.86% |
| 补充轮 | 5:5 | E5 -> E6 | +0.04% |
| 补充轮 | 6:6 | E7 -> E8 | -1.13% |

- 5:5 补充轮基本持平，首轮约 1.22% 的下降未稳定重现。6:6 两轮均低约 1%，不能直接认定为纯随机波动，也不能仅凭两次样本认定为稳定的 COLD 固定成本。
- 6:6 两轮开启/关闭的 P99 均为 1.239 ms；5:5 的 P99 变化方向不一致。未观察到明显的尾延迟恶化。
- 当前 `tlc_core_get_warm_location_raw()` 从 location cache、hot/warm 查找，失败直接返回 miss，不读取持久化 COLD。四个开启样本的 `cold_promote=0`、AOF 不变、Server 磁盘读写字节无增长，且 perf 未采到 COLD 执行栈，与该读路径一致。采样中未出现函数不单独构成“从未执行”的证明。
- 本次结论限定为单 Server、无 HA Replica、全部数据驻留 warm、预填充落盘完成后的稳态纯 HANDLE 读。没有验证持续写入、group commit 刷盘、checkpoint、恢复或 HA 复制与读取并发时的 CPU、锁和 I/O 竞争。
- 可以表述为“COLD 不参与本次读路径，开启后的稳态纯读性能整体接近”。不能写为“任何情况下开启 COLD 对读取完全零影响”。要进一步判定 6:6 约 1% 的差异，需要更多随机化交错重复和同窗 ops/CPU 计量。

## 7. 验证及异常样本

- A、C、D 组有效样本的 `status_notfound`、`status_err`、`unmatched`、`materialized_fail` 均为 0，脚本成功退出。
- B 组旧脚本的数据面错误检查通过，包括 fallback、backpressure、stale response 和 handle read failure 等检查。
- 所有有效回归结束后均确认测试 Server 退出、UB 设备释放；开启 perf 的样本生成了两端 SVG。
- 撤销版两端完整重编译及 build stamp 验证通过；111 上 `vemb_v16_cli_l0_ut` 和 `vemb_v16_cli_deadline_ut` 通过。
- `aeron_c20eeb7_p6_s6_profile0_20260919_r1`：基于干净 `c20eeb7` 二进制，用临时无 perf 脚本得到 4.200786M，但 CPU 统计出现负值。该异常样本不纳入有效性能结论。临时脚本已按要求删除，日志保留；后来出现的其他 Server 启动晚于本轮，不能据此证明是它导致异常。
- `aeron_4383547_p6_s6_compare_c20_20260919_r1`：负载检查失败，未产生有效压测 QPS。111 上 `/home/.syslog/` 的用户态 `kthreadd` 几乎占满 192 核；经用户授权停止相关进程和启动脚本后，重新检查负载通过，再执行有效的 r2。没有删除该目录文件，也未停止系统 `[kthreadd]`。
- 回退 `c20eeb7` 后从本地运行新版脚本，曾因缺少默认 manifest 失败；新版 peer-view 参数也与旧 CLI 不匹配。该失败不属于性能结果。B 组实际使用远端对应提交的旧版脚本。
- E 组八个有效样本均成功退出，64 个 worker 的 `status_nf`、`status_err`、`materialized_fail`、`unmatched` 均为 0，诊断 `final_miss=0`、`cold_promote=0`。
- E3/E4/E6/E8 每次均生成 2 个 AOF segment，共 127388895 bytes；100,000 个唯一 `item:1` 至 `item:100000`、连续 seq 1 至 100000、PUT/version/向量长度及全部 XXH3 校验和通过。读测前后各 segment 的 SHA256 一致，Server `/proc/<pid>/io` 的 `read_bytes` 和 `write_bytes` 增量均为 0。这里不将包含网络 I/O 的 `rchar/wchar` 误作磁盘读写字节。
- 新 COLD 目录首次启动会打印 `COLD checkpoint validation failed: generation=0 errno=22`：目录尚无 checkpoint manifest，当前恢复代码记录告警后回退 AOF replay。该启动告警不表示本轮 AOF 写入或 fsync 失败；有效样本没有发现 COLD 追加/落盘失败。
- COLD 临时 runner 的首次环境核验曾因 Redis 改写进程标题后 `/proc/<pid>/environ` 不可用而中止；改为记录启动环境、核验 Server 打开的 AOF 文件。随后补充轮关闭组曾因 SSH 丢失空参数在启动前中止，改用非空占位符传递。这些无有效读测的运行不纳入 E 组，未因此修改业务代码。

## 8. 产物索引与备份

完整产物目前位于 111：

```text
/root/szz/codespace/hpc-redis/perf/<RUN_ID>/
```

两端原始数据在各自 `/tmp/<RUN_ID>/`，调度日志在 111 的 `/tmp/<RUN_ID>.runner.log`。主要文件为 `client/client.workload.log`、common-core 的 `client/client.workload.summary.tsv`、`server/server.cpu.process.tsv`、两端 `*.meta.txt`、perf 数据及 SVG。

| ID | RUN_ID |
|---|---|
| A1 | `aeron_dev_aeron_cluster_p5_s5_20260919_r1` |
| A2 | `aeron_dev_aeron_cluster_p7_s7_20260919_r1` |
| A3 | `aeron_dev_aeron_cluster_p10_s10_20260919_r1` |
| A4 | `aeron_dev_aeron_cluster_p7_s7_profile0_20260919_r1` |
| A5 | `aeron_dev_aeron_cluster_p6_s6_profile0_20260919_r1` |
| B1 | `aeron_c20eeb7_p6_s6_20260919_r1` |
| B2 | `aeron_c20eeb7_p6_s6_20260919_r2` |
| B3 | `aeron_c20eeb7_p6_s6_20260919_r3` |
| C1 | `aeron_4383547_p6_s6_compare_c20_20260919_r2` |
| C2 | `aeron_4383547_revert_f53ce263_p6_s6_20260919_r1` |
| C3 | `aeron_4383547_revert_f53ce263_p5_s5_20260919_r1` |
| D1 | `aeron_4383547_revert_f53ce263_p5_s5_item_20260919_r1` |
| D2 | `aeron_4383547_revert_f53ce263_p6_s6_item_20260919_r1` |
| D3 | `aeron_4383547_diag_p5_s5_item_20260919_r1` |
| D4 | `aeron_4383547_diag_p6_s6_item_20260919_r1` |
| E1 | `aeron_cfd2cf4_p5_s5_item_20260919_r1` |
| E2 | `aeron_cfd2cf4_p6_s6_item_20260919_r1` |
| E3 | `aeron_cfd2cf4_cold_p5_s5_item_20260919_r2` |
| E4 | `aeron_cfd2cf4_cold_p6_s6_item_20260919_r2` |
| E5 | `aeron_cfd2cf4_cold_off_p5_s5_item_20260919_r4` |
| E6 | `aeron_cfd2cf4_cold_on_p5_s5_item_20260919_r4` |
| E7 | `aeron_cfd2cf4_cold_off_p6_s6_item_20260919_r4` |
| E8 | `aeron_cfd2cf4_cold_on_p6_s6_item_20260919_r4` |

按用户要求，C2/C3 的两端 SVG 已回传本地并校验 SHA256：

- C2：[Server](../perf/aeron_4383547_revert_f53ce263_p6_s6_20260919_r1/server/server.svg)、[CLI](../perf/aeron_4383547_revert_f53ce263_p6_s6_20260919_r1/client/client.svg)。
- C3：[Server](../perf/aeron_4383547_revert_f53ce263_p5_s5_20260919_r1/server/server.svg)、[CLI](../perf/aeron_4383547_revert_f53ce263_p5_s5_20260919_r1/client/client.svg)。

`perf/` 为本地忽略目录，这些链接不保证在新 clone 中存在。D 组产物目前留在远端，没有因撰写本文而下载或改写原始结果。

E 组 8 组共 16 张 SVG 已全部回传本地，逐文件 SHA256 与远端一致：

| ID | 本地 Server 火焰图 | 本地 CLI 火焰图 |
|---|---|---|
| E1 | [Server](../perf/aeron_cfd2cf4_p5_s5_item_20260919_r1/server/server.svg) | [CLI](../perf/aeron_cfd2cf4_p5_s5_item_20260919_r1/client/client.svg) |
| E2 | [Server](../perf/aeron_cfd2cf4_p6_s6_item_20260919_r1/server/server.svg) | [CLI](../perf/aeron_cfd2cf4_p6_s6_item_20260919_r1/client/client.svg) |
| E3 | [Server](../perf/aeron_cfd2cf4_cold_p5_s5_item_20260919_r2/server/server.svg) | [CLI](../perf/aeron_cfd2cf4_cold_p5_s5_item_20260919_r2/client/client.svg) |
| E4 | [Server](../perf/aeron_cfd2cf4_cold_p6_s6_item_20260919_r2/server/server.svg) | [CLI](../perf/aeron_cfd2cf4_cold_p6_s6_item_20260919_r2/client/client.svg) |
| E5 | [Server](../perf/aeron_cfd2cf4_cold_off_p5_s5_item_20260919_r4/server/server.svg) | [CLI](../perf/aeron_cfd2cf4_cold_off_p5_s5_item_20260919_r4/client/client.svg) |
| E6 | [Server](../perf/aeron_cfd2cf4_cold_on_p5_s5_item_20260919_r4/server/server.svg) | [CLI](../perf/aeron_cfd2cf4_cold_on_p5_s5_item_20260919_r4/client/client.svg) |
| E7 | [Server](../perf/aeron_cfd2cf4_cold_off_p6_s6_item_20260919_r4/server/server.svg) | [CLI](../perf/aeron_cfd2cf4_cold_off_p6_s6_item_20260919_r4/client/client.svg) |
| E8 | [Server](../perf/aeron_cfd2cf4_cold_on_p6_s6_item_20260919_r4/server/server.svg) | [CLI](../perf/aeron_cfd2cf4_cold_on_p6_s6_item_20260919_r4/client/client.svg) |

除 SVG 外，E 组日志及原始 perf 数据仍留在远端。E3--E8 的 `perf/<RUN_ID>/server/cold.*` 保存本组适用的启动环境、AOF 文件描述符、读测前后文件校验结果和 I/O 快照，调度日志另存为 `perf/<RUN_ID>/runner.log`。关闭组没有 AOF 校验快照。

111 的持久化目录均保留：

| ID | COLD 目录 |
|---|---|
| E3 | `/root/szz/codespace/vemb-cold-p5-20260919-7jgDDK` |
| E4 | `/root/szz/codespace/vemb-cold-p6-20260919-JmsXM6` |
| E6 | `/root/szz/codespace/vemb-cold-p5-20260919-ekCkU4` |
| E8 | `/root/szz/codespace/vemb-cold-p6-20260919-QGWEPU` |

两端已保留的关键备份路径：

```text
/root/szz/codespace/hpc-redis_23018eb_artifacts_20260919_c20/
/root/szz/codespace/hpc-redis_c20eeb7_artifacts_20260919_pre438/
/root/szz/codespace/hpc-redis_4383547_artifacts_20260919_prediagrevert/
/root/szz/codespace/hpc-redis_4383547_nodiag_artifacts_20260919/
/root/szz/codespace/hpc-redis_4383547_artifacts_20260919_precfd2cf4/
```

`revert_f53ce263.patch` 在 `prediagrevert` 和 `nodiag` 两处均有归档；未提交补丁实际保存在这些备份目录，不应假设每个 perf 目录都有副本。`nodiag` 还保存撤销版 SDK、Server、memtier 和 build stamps；`prediagrevert` 保留最初完整 `4383547` 的产物，`precfd2cf4` 保留切换到 `cfd2cf4` 前恢复完整诊断后的构建产物和旧 peer-view 配置。

## 9. 复测命令

以下适用于已恢复的完整 `4383547`。在 111 的 `/root/szz/codespace/hpc-redis` 执行；确认设备和负载空闲、build stamp 有效，并使用新的 `RUN_ID`，不要覆盖已有结果。

```bash
SERVER_NODE=127.0.0.1 CLIENT_NODE=192.168.90.112 \
SERVER_SSH_PORT=22 CLIENT_SSH_PORT=22 \
NUM_KEYS=100000 KEY_PATTERN=R:R KEY_PREFIX=item: \
THREADS=64 CLIENTS=4 PIO=5 SNW=5 \
DIM=300 MAX_VECTORS=131072 \
PIPELINE=32 BATCH_REQUEST_SIZE=32 BATCH_MAX_DELAY_US=0 L1_ENTRIES=0 \
SERVER_CPU_MASK=0-15 CLIENT_CPU_MASK=96-191 \
TEST_TIME=30 SERVER_FLAME_DURATION=25 PROFILE=1 BUILD=verify \
SERVER_MANIFEST=/root/szz/codespace/hpc-redis_23018eb_artifacts_20260919_c20/examples/vemb_perf_warm_111.yaml \
KILL_OPENCODE=0 KILL_MUTAGEN=0 \
RUN_ID="aeron_4383547_diag_p5_s5_item_$(date +%Y%m%d_%H%M%S)" \
bash scripts/run_aeron_cross_node_flamegraph.sh
```

6:6 仅改 `PIO=6 SNW=6` 和 `RUN_ID` 中的线程标记。切换诊断实验版本必须先保存工作区和构建产物，再应用归档补丁、重新编译；不能仅修改分支名称或伪造 build stamp。

### 9.1 COLD 对照复测注意事项

E 组使用 `cfd2cf4` 重新编译后的两端二进制。111 上的临时脚本为 `/tmp/vemb-cold-read-20260919/run_cold_read.sh`，AOF 校验器为同目录 `verify_cold_aof.py`，均未提交进仓库。临时 runner 沿用原脚本的 workload/perf 流程，仅增加 COLD 启停传参、预填充后的等待/同步和前后校验。复测前应核对临时文件仍存在，不能假定新环境已包含它们。

在第 9 节相同 workload 参数之外，必须显式保持以下配置，避免 `cfd2cf4` 的 HA 默认设备分配改变对照：

```text
SERVER_WARM_UB_PATH=/dev/obmm_shmdev4
CLIENT_WARM_UB_PATH=/dev/obmm_shmdev8
CLIENT_PEER_VIEW_MANIFEST=/root/szz/codespace/hpc-redis_4383547_artifacts_20260919_precfd2cf4/examples/vemb_v16_ub_peer_view_112_to_111.yaml
SERVER_RESERVED_UB_PATHS=/dev/obmm_shmdev13
CLIENT_RESERVED_UB_PATHS=/dev/obmm_shmdev9
```

保留 4 GiB `SERVER_MANIFEST` 和真实 lsof 占用检查。仅在确认没有 HA Replica 或设备持有者后使用上述 reserved-path 配置。

开启组将 `COLD_TEST_DIR` 设置为 `mktemp -d /root/szz/codespace/vemb-cold-p5-20260919-XXXXXX` 新建的空目录；关闭组传空值。临时 runner 会将目录显式转发到 Server 启动 shell，设置 `HPC_REDIS_COLD_DIR` 并取消 `HPC_REDIS_HA_ROLE`。不要使用已有持久化目录来做本轮预填充对照，也不要删除已有数据。

使用新的 `RUN_ID` 和 `LOCAL_ROOT=/root/szz/codespace/hpc-redis/perf/<RUN_ID>`，执行 `bash /tmp/vemb-cold-read-20260919/run_cold_read.sh`。`LOCAL_ROOT` 需显式指定，因为脚本不在仓库的 `scripts/` 目录。所有数值仍按第 4 节口径读取，不用配置的 30 秒替换 CLI 的有效时长。
