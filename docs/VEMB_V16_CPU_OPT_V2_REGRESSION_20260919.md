# VEMB V16 CPU Opt V2 分轮优化回归

## 1. 范围和固定条件

- 分支：本地及 111/112 为 `dev_cpu_opt_v2`。
- 基准提交：`cfd2cf4776d4584e96120a20e97919d662a10dc3`。每轮为该提交上的累积改动，使用源码快照和补丁标识，不把相同 HEAD 当成相同测试代码。
- 系列：`cpu_opt_v2_20260919_232600`。
- 单 Server@111、CLI@112，COLD 开启，HA Replica 关闭；每次使用独立空 COLD 目录。
- `KEY_PREFIX=item:`，100000 keys，`R:R`，dim=300，THREADS=64，CLIENTS=4，pipeline/batch=32，L1=0，TEST_TIME=30，PROFILE=1，cycles@99Hz，Server perf 25s。
- Server CPU 0-15，CLI CPU 96-191；request NC -> CC，response NC -> CC。Server req/resp/warm 为 dev3/dev6/dev4，CLI 对应 dev7/dev2/dev8。Warm 4 GiB，offset=0。
- 保持 wire format、可见性屏障、诊断配置和各阶段生命周期约束，不混入线程数或缓存策略优化。

前序分析及 COLD 基线见 [前缀、诊断与 COLD 回归](VEMB_V16_UB_QPS_PREFIX_DIAGNOSTIC_REGRESSION_20260919.md)。

## 2. 阶段

| 阶段 | 累积修改 | 当前状态 |
|---|---|---|
| S0 | 原始 cfd2cf4，新环境基线 | 完成 |
| S1 | v2 HANDLE 直接构造 READ job，省去通用 request 清零及中间 key copy | 完成首轮 |
| S2 | batch context 按需初始化 | 完成首轮 |
| S3 | producer tail、布局及 arena 结束位置本地化，按需刷新 head | 完成首轮 |
| S4 | 一次 reserve/commit，消除 descriptor 发布重复检查 | 首轮及 S4R2 复测完成 |

S1 复用原有 pool 重试、generation、发布和回收。测试对照通用构造与直接构造，覆盖二进制 key、最长 key、slot 复用、池满和源 key 生命周期。

## 3. 数据

QPS 取 `client.workload.log` 的 Totals；CPU 为 `server.cpu.process.tsv` 的 total core_equiv；单核 QPS 为二者相除。QPS 与 CPU 的窗口不同，不将该派生指标视为严格同窗 CPU/op。每阶段先各跑一轮 5:5/6:6，最终复测用于确认变化，不由单轮微小差异宣称稳定收益。

| 阶段 | PIO:SNW | QPS (M) | P99 (ms) | Server CPU core | QPS/core (M) |
|---|---|---:|---:|---:|---:|
| S0 | 5:5 | 7.905089 | 1.455 | 8.864378 | 0.891782 |
| S0 | 6:6 | 8.019970 | 1.239 | 10.034331 | 0.799253 |
| S1 | 5:5 | 8.038570 | 1.231 | 8.598755 | 0.934853 |
| S1 | 6:6 | 8.040708 | 1.223 | 9.801577 | 0.820348 |
| S2 | 5:5 | 8.002375 | 1.223 | 8.643175 | 0.925861 |
| S2 | 6:6 | 8.034162 | 1.223 | 9.774878 | 0.821919 |
| S3 | 5:5 | 8.136452 | 1.215 | 8.648320 | 0.940813 |
| S3 | 6:6 | 8.135933 | 1.215 | 9.783239 | 0.831620 |
| S4 | 5:5 | 8.209464 | 1.207 | 8.564829 | 0.958509 |
| S4 | 6:6 | 8.194560 | 1.207 | 9.775074 | 0.838312 |
| S4R2 | 5:5 | 8.218478 | 1.207 | 8.604585 | 0.955128 |
| S4R2 | 6:6 | 8.198760 | 1.207 | 9.816671 | 0.835187 |

最终版本两轮均值相对 S0：

| PIO:SNW | S0 QPS (M) | S4 平均 QPS (M) | S0 core | S4 平均 core | S0 M/core | S4 M/core | QPS 提升 | 单核吞吐提升 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 5:5 | 7.905089 | 8.213971 | 8.864378 | 8.584707 | 0.891782 | 0.956814 | 3.91% | 7.29% |
| 6:6 | 8.019970 | 8.196660 | 10.034331 | 9.795873 | 0.799253 | 0.836746 | 2.20% | 4.69% |

上表最终 M/core 用平均 QPS 除以平均 core。5:5/6:6 的 Server core 分别下降 3.15%/2.38%；最终两次 QPS 差异为 0.11%/0.05%。S0 仅一轮、未进行交错 ABBA 基线复测，因此这是当前环境观测，不是严格统计显著性结论。该配置下 5:5 的吞吐与 6:6 相近、单核效率更高；没有恢复到 9M–11M，也未通过改变 prefix、线程、诊断或 UB 映射制造对照差异。

S1 首轮相较 S0，5:5 单核吞吐约增加 4.83%，6:6 约增加 2.64%。S2 相较 S1 的差异较小，不能据此宣称稳定收益。

5:5 Server collapsed stacks 的 request handler inclusive 权重从 S0 的 21.141% 降到 S1 的 4.704%；response publish 为 23.635% 和 25.101%。这是各轮采样权重的占比，不是耗时或绝对 CPU 节省比例；部分 libc 栈没有函数符号，不能仅凭 `memset` 名称匹配统计清零开销。

| 阶段 | 5:5 request % | 5:5 response % | 6:6 request % | 6:6 response % |
|---|---:|---:|---:|---:|
| S0 | 21.141 | 23.635 | 18.388 | 21.364 |
| S1 | 4.704 | 25.101 | 4.257 | 21.609 |
| S2 | 4.052 | 25.197 | 4.153 | 22.070 |
| S3 | 4.167 | 18.584 | 3.881 | 16.450 |
| S4 | 4.099 | 12.436 | 3.567 | 10.614 |
| S4R2 | 4.068 | 12.009 | 3.598 | 10.561 |

S4 首轮的端到端 QPS 只提高约 2%–4%，不能由两个热点占比的下降推导出几十个百分点的总 CPU 节省。5:5 CLI 的 `sve_streaming_load_f32` self 权重仍为 57.683%；Server 的 `proxy_io_pool_thread_main` self 为 31.941%，优化后仍有 polling/调度开销。后续应区分 CLI 向量物化与 Server 空轮询，不在本轮同时改动这些路径。

## 4. 产物和复现

111 的源码检查点在 `/root/szz/codespace/hpc-redis/perf/cpu_opt_v2_20260919_232600/<阶段>/`：`source.tgz`、`source.patch`、`base_commit.txt`、`binaries.sha256` 和本轮 Server 二进制。

运行目录名为 `cpu_opt_v2_20260919_232600_<阶段>_p5_s5` 或 `_p6_s6`，完整产物位于 111 的仓库 `perf/`。两端火焰图按同目录回传本地并校验哈希。

额外的远端源码归档回传未获安全审查许可，因此源码检查点保留在 111，不声称已下载；本地有当前完整实现。所有性能结果和 SVG 单独回传，不受影响。

| 阶段 | 5:5 Server | 5:5 CLI | 6:6 Server | 6:6 CLI |
|---|---|---|---|---|
| S0 | [SVG](../perf/cpu_opt_v2_20260919_232600_S0_p5_s5/server/server.svg) | [SVG](../perf/cpu_opt_v2_20260919_232600_S0_p5_s5/client/client.svg) | [SVG](../perf/cpu_opt_v2_20260919_232600_S0_p6_s6/server/server.svg) | [SVG](../perf/cpu_opt_v2_20260919_232600_S0_p6_s6/client/client.svg) |
| S1 | [SVG](../perf/cpu_opt_v2_20260919_232600_S1_p5_s5/server/server.svg) | [SVG](../perf/cpu_opt_v2_20260919_232600_S1_p5_s5/client/client.svg) | [SVG](../perf/cpu_opt_v2_20260919_232600_S1_p6_s6/server/server.svg) | [SVG](../perf/cpu_opt_v2_20260919_232600_S1_p6_s6/client/client.svg) |
| S2 | [SVG](../perf/cpu_opt_v2_20260919_232600_S2_p5_s5/server/server.svg) | [SVG](../perf/cpu_opt_v2_20260919_232600_S2_p5_s5/client/client.svg) | [SVG](../perf/cpu_opt_v2_20260919_232600_S2_p6_s6/server/server.svg) | [SVG](../perf/cpu_opt_v2_20260919_232600_S2_p6_s6/client/client.svg) |
| S3 | [SVG](../perf/cpu_opt_v2_20260919_232600_S3_p5_s5/server/server.svg) | [SVG](../perf/cpu_opt_v2_20260919_232600_S3_p5_s5/client/client.svg) | [SVG](../perf/cpu_opt_v2_20260919_232600_S3_p6_s6/server/server.svg) | [SVG](../perf/cpu_opt_v2_20260919_232600_S3_p6_s6/client/client.svg) |
| S4 | [SVG](../perf/cpu_opt_v2_20260919_232600_S4_p5_s5/server/server.svg) | [SVG](../perf/cpu_opt_v2_20260919_232600_S4_p5_s5/client/client.svg) | [SVG](../perf/cpu_opt_v2_20260919_232600_S4_p6_s6/server/server.svg) | [SVG](../perf/cpu_opt_v2_20260919_232600_S4_p6_s6/client/client.svg) |
| S4R2 | [SVG](../perf/cpu_opt_v2_20260919_232600_S4R2_p5_s5/server/server.svg) | [SVG](../perf/cpu_opt_v2_20260919_232600_S4R2_p5_s5/client/client.svg) | [SVG](../perf/cpu_opt_v2_20260919_232600_S4R2_p6_s6/server/server.svg) | [SVG](../perf/cpu_opt_v2_20260919_232600_S4R2_p6_s6/client/client.svg) |

调度脚本保留在本地 `perf/cpu_opt_v2_20260919_232600/run_round.sh` 及 111 的 `/tmp/vemb-cold-read-20260919/run_cpu_opt_round.sh`。它复用已验证的 COLD runner，预填充后等待并同步、校验 100000 条 AOF，再开始计时；结束后比较 AOF 和 Server I/O。临时 runner 和校验器不保证在新机器存在。

## 5. 测试记录

- S1：111 新增 direct READ job 单测通过；两端正式 build stamp 编译通过，SVE 配置保持不变。
- S2：111 context/ring 单测通过；context 覆盖 poisoned storage、generation 回绕、128/1/65/32 个条目、跨 bitmap word、反序完成、重复完成、成功/错误响应编码解码；两端重新编译通过。
- S3：本地 ring/context、111 ring/context/READ job 单测通过；112 SDK peer-view 集成测试通过。ring 增加 4 轮 descriptor 满载复用、full 时不写 arena、释放后污染 descriptor 证明无需回读、20 万变长 frame 的双线程 SPSC 测试。
- S3 同时修复旧实现的空 arena 回绕边界：当 tail 非零而新 frame 等于 arena 容量时，仅在 acquire 确认无未消费 frame 后跳过 padding，避免永久 FULL。SDK attach 复用已有 header 校验，并验证新通道为空，producer 生命周期与 channel 一致。
- S4：本地 ring/context 通过，ring 的 ASan/UBSan 通过；111 强制重编的 READ job/context/ring、close drain、proxy topology 均通过；112 强制重编的 peer-view、L0、deadline 均通过。两个既有 Server 集成 UT 的 Makefile link 行漏列 `vemb_v16_mapped_region.c`，使用 `CC="gcc ../src/vemb_v16_mapped_region.c"` 仅为这两个 target 补齐链接，未改生产构建和源文件。
- S0–S4/S4R2 共 24 张 Server/CLI SVG 已回传并逐一 SHA-256 校验一致。
- 共 12 次回归：`status_err/status_notfound/unmatched/materialized_fail/misses/moved/ask` 均为 0；每轮 100000 条 COLD AOF 记录及 checksum 正确，压测前后 AOF 哈希和 `/proc/PID/io` 的 `read_bytes/write_bytes` 不变。审计脚本为本地 `perf/cpu_opt_v2_20260919_232600/audit_rounds.py`。
- 最终两端同步 dry-run 均为 `candidates=9 different=0`，Server/CLI build stamp verify 均通过。分支仍是 `dev_cpu_opt_v2`，HEAD 仍是 `cfd2cf4`，优化改动尚未提交。远端工作区没有意外的 `release.h` 修改。
- 真实 UB 环境补测：先确认设备无人占用，重新编译 `ub_cc_nc_visibility_ut`，在 scratch offset=7516192768、frame=4096、iterations=1000000 下测试 response `111 dev6 NC -> 112 dev2 CC`（ack dev7 -> dev3）及 request `112 dev7 NC -> 111 dev3 CC`（ack dev6 -> dev2）。两个 writer 都输出 `FRAME_WRITER_COMPLETE`，两个 reader 都输出 `NOT_REPRODUCED iterations=1000000`。没有设置 `--expect-mixed/--expect-visibility-failure`，此时 NOT_REPRODUCED 表示正常回归通过，而非预期失败模式。日志在本地 `perf/cpu_opt_v2_20260919_232600/validation/`。该 UT 验证 UB 环境，不直接包含优化后的 batch ring；后者由上述 12 次实际跨节点回归覆盖。

## 6. 实现边界

- S1 只替换 v2 HANDLE 构造；保留一份 job-local key 拷贝，因为 SuperNode 执行时 request arena 可能已经回收。仅删除中间 request 和第二次 key copy，不删除必要的生命周期拷贝。
- S2 唯一生产 acquire 调用为 proxy batch handler；成功 completion、stale topology、pool full 和 publish failure 路径均完整赋值响应项。只清理控制字段与 completed bitmap，不依赖旧 entries 内容。
- S3/S4 唯一生产 arena publisher 为 Server batch response 和 SDK batch HANDLE request，均为 channel 独占 SPSC producer；禁止混用通用 publish。初始化时固定 layout，重建 channel 时重置本地状态，约增加 2 KiB/channel 的本地 ledger。
- 共享 head 只在 descriptor/arena 容量不足时 acquire 刷新；缓存过期只会保守拒绝，不允许覆盖未消费 frame。frame end 用本地 ledger 回收。
- 一次检查同时保留 descriptor 和 arena 容量；之后 consumer 只能释放空间，因此直接 commit 不会失败。body → release fence → commit marker → descriptor → release tail 的顺序不变，wire format 和通用 ring API 不变。
