# WARM allocator v2：顺序分配与回收栈

## 基础与目标

基于 `dev_alloc` 快照 `9547218`，在 `dev_alloc_v2` 实现。保留 manifest 推导容量、动态 key metadata、location cache、slot generation/seqlock 和迁移 fence；替换 hash 起点、bitmap word 搜索和 region vnode 选择。在线查询不恢复 imported index 或物理 region 扫描，SVE 和 payload stride 不在本次修改范围。

## 分配状态与所有权

每个 region 的 owner 在本地 DRAM 维护 `next_slot`、预分配的 `uint32_t free_slots[capacity_slots]`、`free_count` 和 allocator mutex。回收栈每个物理槽占 4B DRAM，4GiB/1200B region 约需 13.65MiB。首次 attach 可以扫描 slot metadata 一次，确定历史 high-water mark 并收集其中的空闲洞；这只重建分配状态，不建立 key 反查索引。

第一阶段明确一个 region 只有一个所属 HPC-Redis 可分配和写入；其他 HPC-Redis 只通过 handle/remote metadata/RPC 读取。同一 owner 进程内多线程共享 region mutex；不实现跨实例 allocator registry、跨进程共享 next_slot 或回收栈。`is_local` 表示所属 region；非所属 region 不参与新分配、覆盖、回收或恢复 reset。slot 状态 CAS 继续保护状态转换，但不作为支持多个 allocator writer 的承诺。

分配在 region 锁下先 pop 回收栈，否则领取 `next_slot++`，没有候选立即返回 FULL；领取后仍通过 FREE→FILLING CAS 再写 payload、发布 READY。正常单 owner 每个 slot 领取固定成本，不扫描 bitmap。候选非 FREE 表示 allocator 生命周期或所有权契约被破坏，不执行候选搜索兜底。不启用 LRU 占用槽淘汰。

region 按 manifest 顺序用满后再切换，仅使用所属 region，取消向非所属 region 的写入 fallback。每个 core 保留活跃 region 提示；活跃 region 无空间后遍历有界 region 列表，检查各 region 的 allocator 状态并切换。因此槽选择为 O(1)，跨 region 切换为 O(region_count)，不随 slot 数增长。旧 region 后续回收的槽仍会在后续切换中重新使用，不永久跳过。当前上限为 256 个 configured/runtime region。

## 删除与回收

在 key shard 锁下，通过已知 metadata location 精确定位旧 slot；校验 generation/identity、取得写 seqlock 后使旧 slot 失效，推进 write_seq，发布 FREE，再入回收栈。只有成功回收的一方可 push，避免重复入栈。复用时推进 owner_generation，不重置 generation/write_seq，防止旧 handle 的 ABA。

DEL、COLD DEL replay、迁移 tombstone 安装需要采用一致的 slot 释放逻辑，同时保留 key version、tombstone 和迁移 fence。服务端 core 的 payload copy 使用状态、generation 与前后 seqlock 校验；这些保证不能外推到 SDK。当前 SDK 直接复制 UB payload，没有 seqlock 校验，并发回收/复用时仍可能读到混合 payload。按用户确认，本阶段不修改 SDK，暂不验收端到端并发回收读场景；SDK 完整 payload 读取保证留到下一阶段。

## COLD 与 HA 恢复

启动恢复和 HA fenced 恢复须停止请求/复制应用，确保本节点拥有的 region 独占可重建。由启动/HA ownership 边界保证 region 的独占重建；不引入 allocator 引用计数。仅初始化本地 region；远端映射保持原样。重建清除 location cache/key metadata，释放本地旧 slot 并重置 next_slot/free_count，保留并推进 slot generation/write_seq。无需清零整个 payload，旧字节不再可由有效 slot 读取。

之后加载 checkpoint，并按各 metadata shard 的 captured_seq 回放增量日志；无 checkpoint 走原有全日志回放。不会删除 COLD 日志或调用清空 COLD 的 resync reset。恢复失败不开放服务；重试重新初始化恢复状态。该流程解决旧 slot 占满但内存索引为空的已知回归。现有 COLD 写入 ACK/失败事件语义不在本次改变范围；历史回放峰值超过 WARM 容量的最终状态归并另行设计，不承诺支持超过容量的历史物化峰值。

## 落地步骤与验收

1. 保存旧分支快照并创建新分支，先写本文。
2. 替换 region bitmap/vnode 分配器，补齐 attach/destroy 生命周期和顺序 region 选择。
3. 实现精确 slot 回收，覆盖删除后重新写同 key/其他 key，以及旧 handle 拒绝。
4. 适配启动 COLD 和 fenced resync 重建，验证旧 region 已满、纯日志、checkpoint+tail、重复恢复与共享/远端保护。
5. 更新与旧 hash 分布、对齐优先、imported 发现绑定的 UT；保留 owner 内多线程满容量、并发唯一性、BUSY、动态索引和迁移语义断言。把跨 HPC-Redis 同 region 多 writer 用例改为 owner 写/reader 只读及非 owner 无法分配的验收。
6. 在 UB 机器独立目录编译运行 TLC、manifest、COLD、sync、resync、迁移和可运行的 HA UT；不修改正在运行的正式测试目录或停止无关进程。真实 HA 网络验收需要对应 UB/keepalived 环境。
7. 构建服务端，运行相同 100K Uniform 3:3 性能对照并记录 CLIENTS=3/4、总 QPS、单核 QPS和错误计数。没有实际结果前不宣称恢复性能或所有 UT 通过。

## 实施记录

- 已建立快照 `9547218`，在 `dev_alloc_v2` 先提交设计 `239e2c2`，随后实施代码。
- `src/tlc_core.c/.h` 已替换为单 owner 的 next_slot + 预分配回收栈；删除 bitmap/vnode 搜索，非所属 region 不参与分配、覆盖、释放或恢复 reset。DEL、COLD DEL、迁移 tombstone 使用统一回收路径；启动 COLD 恢复重建本地 WARM/index/cache，保留 COLD 文件及递增的 generation/write_seq。
- TLC UT 新增跨 region 回收复用、4 线程反复分配删除后满容量重填、纯日志重复恢复与 peer metadata 不变验证；调整依赖旧 hash 分布和多 writer 的 fixture。
- 本地 TLC、manifest、sync、resync UT 通过。111 独立目录 `/root/szz/codespace/hpc-redis-dev-alloc-v2-20261016` 构建并运行 9 个相关 UT 全部通过：`vemb_v16_tlc_ut`、`vemb_v16_manifest_ut`、`vemb_v16_migration_control_ut`、`tlc_cold_ut`、`tlc_replica_cold_ut`、`tlc_sync_ut`、`tlc_resync_ut`、`tlc_ha_replica_ut`、`tlc_ha_replica_process_ut`。日志 `/tmp/alloc-v2-remote-<name>.log`。本轮未执行真实双节点 UB/keepalived HA 测试。
- 111 独立目录服务端以 libc、O3/LTO/SVE、内部 batch32 构建；`tlc_core.c` SHA256 为 `e0028403092ead089453ccbe7e9bc164fa247381f1f5abd8a2114698826b5aea`，服务端 SHA256 为 `acd0eaed0bae3b6e2af42fff4d4b22c0eb41ec68caca46c6d1543ef00fa362ca`。原远端目录及 112 客户端二进制保持原样。
- 用户明确将 SDK 修复延至下一阶段，本阶段不验收并发回收读完整性；服务端 core handle UT 不代表 SDK 已具备该保证。

- 2026-10-16 范围收窄：第一阶段按单 region 单 HPC-Redis writer 实施，不建立跨实例 allocator registry；非所属 region 只读。

### 100K 只读性能：首轮回归与相邻对照

2026-10-16，参数 `NUM_KEYS=100000 DIM=300 KEY_PATTERN=R:R THREADS=64 CLIENTS=3 PIO=3 SNW=3 BUILD=skip PROFILE=1 KILL_OPENCODE=0 KILL_MUTAGEN=0`，客户端保持原目录二进制。两轮设备和负载门禁均通过；v2 运行时 `/proc/600407/exe` 的哈希与上述新构建相同。

| 实现 | 总 QPS | 单核 QPS | 平均/p99 ms | artifacts |
|---|---:|---:|---|---|
| v2 顺序分配 | 3,862,873.61 | 874,028.05 | 1.06531 / 1.719 | `perf/p3_s3_c3_uniform_alloc_v2_20261016` |
| 原目录旧分配器，相邻对照 | 8,040,984.27 | 1,368,324.62 | 0.59867 / 0.863 | `perf/p3_s3_c3_uniform_alloc_v2_control_20261016` |

两轮 status_err/notfound/materialized_fail/unmatched 均为 0。客户端 cycles 火焰图叶节点 `sve_streaming_load_f32` 占比 v2 为 81.48%，对照为 31.59%；服务端 v2 proxy 主循环占比增加，不能直接当作 proxy 自身变慢的证据。v2 没有改 SDK/SVE 代码，但改变了 payload 的物理位置：1200B 连续 slot 起点依次为 0/48/32/16 mod64，仅 1/4 对齐；旧策略优先选择64B对齐起点。正在用 dim304（1216B，64B整倍数）做无代码改动的诊断，尚不能据首轮认定因果或宣布性能验收通过。

诊断准备中的 `p3_s3_c3_uniform_alloc_v2_dim304_20261016` 因 manifest value_size 仍为1200，与 dim304 不一致，SDK bootstrap 失败，未产生有效压测结果。修正后使用独立临时 manifest `/tmp/alloc-v2-warm-dim304.yaml`（value_size=1216），不修改原 manifest。

修正 manifest 后，同一 v2 二进制 dim304 诊断结果：总 QPS `8,439,763.44`，单核 QPS `1,459,665.40`，平均/p99 `0.62891/0.759ms`，四项失败计数均为0。artifacts：`perf/p3_s3_c3_uniform_alloc_v2_dim304_fixed_20261016`。客户端 SVE 复制叶节点 cycles 占比降至12.06%。该结果强烈支持未对齐 UB 读取是顺序分配回归的主要因素；同时改变了 payload 长度与容量，不能当成 dim300 已修复，也不能单凭此证明完全因果。

### 后续对齐适配（尚未实施）

不能为了恢复低占用测试而只优先分配1/4对齐槽，否则满载时仍会触发大量非对齐读。建议将物理 `slot_stride_size` 与逻辑 `value_size` 分开：在 manifest/config 边界根据 payload_bytes 自动算 `ceil(payload_bytes/64)*64`；capacity、slot metadata 布局/reset/attach 使用物理 stride，所有 offset 与 payload 写地址使用 slot*stride，协议 bytes、逻辑维度及 SDK 复制长度仍为 payload_bytes。1200B对应1216B，padding约1.33%，顺序 next_slot/回收栈算法无需改变。必须统一 owner、remote attach 和恢复的容量推导，并明确旧共享 layout 拒绝/重建规则，补容量边界、最后一槽、handle offset、COLD恢复UT，最后用原 dim300/C3/C4 复测。该适配尚未实施；当前 dim300 的性能验收未通过。
