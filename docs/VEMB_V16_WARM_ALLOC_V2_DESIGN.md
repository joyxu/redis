# WARM allocator v2：顺序分配与回收栈

## Server cache-hit 本地 generation 镜像与 CLI 适配计划（2026-10-17）

当前紧凑缓存和按物理槽容量推导缓存大小的代码已独立提交为 `9fc8cf8`，不包含文档。本节是下一步实现契约，取代文末尚未细化的独立epoch建议。

采用每个所属 region 一张本地 `_Atomic uint64_t slot_location_versions[capacity_slots]`，每槽8B，1M槽约8MB。它是共享 `owner_generation` 的可失效镜像，不是hash扩容epoch，也不新增独立奇偶计数。0表示未验证或正在修改，非零表示该代slot已完成发布。cache entry复用现有generation比较，不增加entry大小。共享64B slot_meta、handle wire布局和payload copy协议本阶段不变。

写侧持有原有slot写权限后、改变身份或payload前，先将本地版本清为0并执行屏障；发布稳定write_seq和READY后，再以release发布当前generation。覆盖、分配、淘汰、DEL/COLD DEL replay、迁移tombstone/GC、恢复reset必须覆盖此生命周期。失败抢锁不得恢复或覆盖另一写者的本地版本。释放后保持0；恢复是fenced操作，清空cache与镜像，generation/write_seq保留递增语义。初始attach镜像均为0，已有槽继续完整验证，直到由当前owner正式写入发布；不得把迟到查询结果无条件写回镜像。

读侧以cache entry seqlock取得稳定的key身份和location/generation，再读取所属region的本地版本。非零且等于缓存generation即可返回location，不读取UB slot_meta；版本不符则cache miss，回退现有metadata查找和完整slot校验。缓存填充只保留原先已验证/已提交location的generation，不能拼接旧location与新镜像版本。迟到的旧cache填充只能造成miss，不能命中新代slot。同key覆盖暂时关闭快路径，完成后可重新启用原handle，因为位置和generation未变；payload版本由write_seq管理。

仅单owner、所有写操作经过当前core的region允许镜像快路径，远端只读region仍完整校验共享slot_meta。启用LRU访问记录的构建继续原验证路径。保留cache查找外层的tombstone及SOURCE CUTOVER/SOURCE_GC前后fence。版本匹配是lookup时刻的位置有效性，不是pin或lease；返回后slot仍可能失效，最终复制者必须再次校验共享状态。

### CLI 校验 TODO（下一阶段，本阶段不实现）

- SDK提供完整handle读取API，携带region_id/local_slot/offset/bytes/owner_generation，并从请求上下文保留key hash/fingerprint；独立可保存handle应包含所需key身份。wire当前不编码key_hash，优先从原请求派生，独立传递确有需要时再版本化扩展协议。
- ATTACH显式提供metadata映射描述、layout版本、capacity、stride与region生命周期身份；CLI只读映射header/slot_meta，不能从payload指针猜测metadata位置。处理重启/HA/region重建导致generation重新初始化的ABA，旧mapping/handle必须被资源代际或region incarnation拒绝。
- 外部边界一次校验映射、slot范围、bytes、offset=slot*stride及溢出。读取稳定READY/generation/key身份快照，复制到私有buffer，再校验write_seq；仅同一偶数序列及匹配身份可交付。generation不符重新按key取handle；忙/序列变化有限重试，失败丢弃buffer。允许完整旧值，不保证最新值；裸UB指针不提供同等保证。
- 写侧修改metadata/payload前发布写中状态，完成UB payload可见性后才发布稳定序列。对实际UB CC/NC映射验证屏障和可见性，C acquire/release不能替代设备一致性协议，不直接宣称普通内存seqlock已覆盖跨节点UB。
- 覆盖同key原位写、DEL后不同key复用、延迟handle、COLD恢复、迁移/HA、重建ABA和故意撕裂payload的端到端测试；补齐SDK、batch client、CLI所有materialize入口。offset-only读取不能作为并发回收安全API，materialized_ok也不代表完整性校验。

### Server 验收

检查全部slot mutation和cache填充调用点；UT覆盖正常命中、覆盖、删除复用、只读region、恢复和迁移fence，保留payload copy generation/seqlock测试。运行TLC/manifest/迁移相关UT及diff检查。UB性能以同一client、100K/1M、4:4参数做A/B，记录总QPS、单核QPS、错误计数及slot验证占比；本地UT不等于UB性能或SDK并发回收验收。

### 本地落地记录（2026-10-17）

Server已实现本地generation镜像的分配/销毁、写前失效、写后发布、删除与恢复清零，以及location_cache_get/peek两入口校验。全量slot验证用于metadata慢路径和非所属region；原payload copy generation/seqlock及迁移/tombstone fence保留。每个所属slot增加8B本地内存，无新增cache entry字段或共享layout变更。本次Server代码和文档尚未提交，前置代码快照为9fc8cf8。

新增UT用mprotect(PROT_NONE)禁止访问共享slot_meta页面，在此期间两个cache入口仍能返回正确handle，直接验证快路径没有共享metadata读取；恢复页面访问后验证同key覆盖、同hash不同key槽位复用、旧handle拒绝和新payload复制。并发回收UT加入读线程，在覆盖/删除/复用期间仅接受符合各key数据模式的成功复制，允许过期handle复制失败。这些测试使用本地内存，不外推为SDK/跨节点UB完整性验收。

本地重新编译并通过vemb_v16_tlc_ut、vemb_v16_manifest_ut、tlc_sync_ut、tlc_resync_ut及git diff --check。vemb_v16_migration_control_ut可编译，但运行SIGBUS；使用git show HEAD:src/tlc_core.c提取9fc8cf8基线独立编译，在同一非单调topology epoch测试位置同样SIGBUS（signal 10）。因此不能宣称迁移控制UT通过，也尚未定位该既有崩溃原因。日志位于/tmp/hpc-cachehit-{ut-final,manifest-final,sync-final,resync-final,migration}.log，基线日志位于/var/folders/8c/_3dvtkk141d4p_jmlpgmhl000000gn/T/hpc-cachehit-baseline-rstqckal/。

111 SSH连接超时，用户确认暂时无法连接机器。新快路径尚未同步/构建到UB远端，未运行100K/1M性能A/B；不能宣称恢复或提升QPS。机器恢复后先验证远端UT，再保持client和压测参数一致执行A/B，避免将SDK校验未实现的混合写负载作为端到端正确性验收。

本文保留阶段一的设计和实验历史；自动物理 stride 已在文末“阶段二实现与验证记录”落地，当前状态以该节为准。

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

### 后续对齐适配（阶段一计划，阶段二已实施）

旧分配器 dim304 相邻对照为总 QPS `7,898,796.22`、单核 `1,343,641.87`，平均/p99 `0.68242/0.799ms`，四项失败计数均为0；artifacts：`perf/p3_s3_c3_uniform_alloc_v2_control_dim304_20261016`。两种实现的 dim304 均没有重现 v2 dim300 的大幅回归；单轮 v2 高于旧版的幅度不作为稳定提升结论。分配器实现已保存为 `0de05a8`，dim300 对齐适配仍未实施。

不能为了恢复低占用测试而只优先分配1/4对齐槽，否则满载时仍会触发大量非对齐读。建议将物理 `slot_stride_size` 与逻辑 `value_size` 分开：在 manifest/config 边界根据 payload_bytes 自动算 `ceil(payload_bytes/64)*64`；capacity、slot metadata 布局/reset/attach 使用物理 stride，所有 offset 与 payload 写地址使用 slot*stride，协议 bytes、逻辑维度及 SDK 复制长度仍为 payload_bytes。1200B对应1216B，padding约1.33%，顺序 next_slot/回收栈算法无需改变。必须统一 owner、remote attach 和恢复的容量推导，并明确旧共享 layout 拒绝/重建规则，补容量边界、最后一槽、handle offset、COLD恢复UT，最后用原 dim300/C3/C4 复测。该适配尚未实施；当前 dim300 的性能验收未通过。

### 阶段二实施计划：自动物理 stride

用户在阶段一提交后要求继续，本阶段实施64B对齐。manifest/provider 边界自动推导 slot_stride_size，所有生产 manifest region（SHM、UB、runtime peer attach）统一使用 floor(region_bytes/stride) 容量；mmap_offset 需64B对齐。低层直接传入内存的 TLC/core 配置支持显式 stride；零值保留其既有紧凑 buffer 语义（stride=value_size），生产 provider 总是传入自动计算的非零 stride，不以测试开关改变生产策略。

共享 header 升级版本并记录 payload 大小、物理 stride 和区域大小；attach 校验全部布局身份，拒绝旧版或不一致布局，owner 明确 reset 后才能使用新布局。SDK 继续按照服务端返回的 offset/bytes 复制，不修改 SDK 协议或 SVE。验证新增 dim300 非整除容量、最后一槽、padding 不覆盖、回收、恢复及旧布局拒绝测试，再完成原参数跨机对照。

### 阶段二实现与验证记录（2026-10-16）

上述“尚未实施”为阶段一的历史状态，本阶段已实施物理 stride。生产 manifest/provider 自动计算 `slot_stride_size=align_up(value_size,64)`；启动、runtime region、peer view、remote-meta 默认容量和 channel legacy max_vectors 字段均按物理 stride 推导。TLC/core 使用该 stride 生成和校验 offset、写入 payload；bytes 和 vector_dim 继续表示逻辑长度。4GiB payload window、1200B payload 对应1216B stride和3,532,045槽，另需226,050,944B header/slot metadata；每槽padding16B约占物理槽空间1.32%。manifest 的 bytes 仍表示 payload window，不包含前置 metadata。

共享 header 保持64B大小，版本升为2，记录value_size、slot_stride_size、region_bytes；SHM和UB attach均验证版本/region/capacity/逻辑大小/物理stride/区域大小。旧布局拒绝attach，不能无条件解释为新布局；owner显式reset并从COLD恢复后再使用新布局。非owner不会初始化空白header。初始化期间保持INITIALIZING标记，完成后再发布MAGIC。

新增 `test_manifest_aligned_stride_recovery_and_layout_identity` 使用1200B payload、1216B stride、SHM与mock UB两个owner region，验证非整除容量、最后一槽、跨region、padding和尾部不覆盖、满容量拒绝、删除复用、旧handle拒绝、原region上的COLD恢复，以及旧版本/错误stride/value_size/region_bytes/非对齐mmap_offset拒绝。旧manifest UT的独立场景使用64B payload以保留其原本容量/路由语义；dim300新用例直接覆盖自动padding，不通过更改压测维度绕过问题。

本地 TLC、manifest UT通过。迁移控制UT本地发生SIGBUS，未将其算作本地通过；111原生构建执行通过。111独立目录的9个相关UT全部通过：vemb_v16_tlc_ut、vemb_v16_manifest_ut、vemb_v16_migration_control_ut、tlc_cold_ut、tlc_replica_cold_ut、tlc_sync_ut、tlc_resync_ut、tlc_ha_replica_ut、tlc_ha_replica_process_ut。日志 `/tmp/alloc-v2-stride-<name>.log`。没有执行真实双节点UB/keepalived HA，也没有验收SDK并发回收读取。

111服务端仍在 `/root/szz/codespace/hpc-redis-dev-alloc-v2-20261016` 独立构建，libc、O3/LTO/SVE、内部batch32；原目录与112客户端保持原样。`tlc_core.c` SHA256 `b8c8b80ad68087e0ae88f8dc3ad1e5ab11aed8d324a925c5668fadb24ac6abb7`；服务端SHA256 `d944219cd3b5367cb2a5955b7980a4dd01ea9d3c4b467a648edd2f66cba542bb`。

原参数 `NUM_KEYS=100000 DIM=300 KEY_PATTERN=R:R THREADS=64 CLIENTS=3 PIO=3 SNW=3 BUILD=skip PROFILE=1 KILL_OPENCODE=0 KILL_MUTAGEN=0`：总QPS `8,448,774.78`、单核 `1,458,593.30`、平均/p99 `0.62863/0.759ms`；status_notfound/status_err/materialized_fail/unmatched均为0。artifacts `perf/p3_s3_c3_uniform_alloc_v2_stride64_20261016`，客户端SVE复制叶节点cycles占比11.91%。同一SDK、原始dim300条件下消除了阶段一3.863M的明显回归；单轮结果不作为稳定提升幅度结论。此前旧分配器C3相邻对照为8.041M/1.368M。

C4首轮 `p3_s3_c4_uniform_alloc_v2_stride64_20261016` 被111编译进程cc1plus的CPU/RSS负载门禁拦截，未启动测试服务器、没有有效性能结果；保留无关进程，门禁不变，待其退出后复跑。

C4重试 `p3_s3_c4_uniform_alloc_v2_stride64_retry_20261016` 同样没有通过负载门禁：111上mooncake_master（PID615944）约88.35%单进程CPU，总CPU测量91.26%；没有启动测试服务器，没有有效压测结果，未终止该进程。

### 1M key 读取回归的缓存容量诊断（2026-10-16）

用户在同一 v2 对齐构建、同一客户端和 `PROFILE=0` 条件下观察到：100K key/C4 为 `10,061,899.27` QPS、单核 `1,415,097.69`，而 1M key/C4 为 `5,370,188.52`、`684,706.74`；两轮均无 notfound、error、materialized_fail 或 unmatched。当前生产代码的 `TLC_CORE_DEFAULT_HOT_CAPACITY=65536`，`location_cache_init` 将其乘二后取整，因此实际位置缓存容量为131,072个entry，并且每次只探测4个位置。1M key最多只有约13.1%能同时驻留位置缓存，其余读取会进入 `key_meta_shard` 锁、动态 metadata hash 查找、位置校验和缓存替换路径。

为分离元数据查找因素，使用 `benchmark/vemb_v16_warm_lookup_bench` 在不执行RPC和UB payload复制的条件下，以100万条已加载key做同一随机读取。111 ARM、当前 v2源码强制重编译后的结果（单核、3M steady hit）如下：

| location cache requested capacity | steady lookup/s | 每次 lookup |
|---:|---:|---:|
| 65,536（生产默认，实际131,072） | 2.02M | 495.3ns |
| 1,048,576（实际2,097,152） | 4.50M | 222.7ns |

本地 x86 同一基准的结果为4.28M/s与9.10M/s，方向一致。该实验确认：1M key 时固定的小位置缓存会把大量请求推入慢路径，是必须优先验证的候选瓶颈；它没有证明全部5.37M到10.06M的差值都由缓存造成，也没有测量UB复制、Aeron排队和CPU频率变化。微基准不是性能验收结果。

当时计划的默认/大位置缓存跨机A/B已在下文完成：1M Uniform的大缓存比默认缓存高约45.6%，但仍有工作集相关的每核效率差距。公开 `lookup_cache_hit/miss` 统计仍未递增，不能据其判断命中率；不要重复只扩大缓存的实验。1M大缓存约2,097,152个entry，Redis RSS约1.07GiB，缓存继续扩大会增加内存成本。

2026-10-16 20:24（北京时间）按用户要求开始跨机验证。首轮 `p4_s4_1m_default_cache_20261016_202407` 在服务启动/预填充前被111负载门禁拒绝：`mooncake_master` PID631323 RSS285.12MiB，高于256MiB阈值；总CPU4.81%、目标CPU集合0-15忙碌0%，CPU门禁通过。未终止该进程，未修改门禁。该轮没有workload、QPS、单核QPS或正确性计数；不能作为性能样本。因同一阻塞尚在，扩大缓存Uniform和Zipf两组未启动。

准备的默认服务端目录为 `/root/szz/codespace/hpc-redis-dev-alloc-v2-20261016`，SHA256 `d944219cd3b5367cb2a5955b7980a4dd01ea9d3c4b467a648edd2f66cba542bb`；大缓存独立目录 `/root/szz/codespace/hpc-redis-alloc-v2-cache-probe-20261016`，SHA256 `62070e407dbdce9452bd049fe600a9036ee4b30bbdd9737cfe31451daad7b773`，实验patch仅将TLC配置hot_capacity改为1048576，实际entry容量2097152。两者为libc/O3/LTO/SVE、内部batch32发布构建，未开启lookup诊断。原始v2服务端和SDK未改动。

待负载门禁通过后，顺序执行默认缓存Uniform、大缓存Uniform、大缓存Zipf（Z:Z、ZIPF_S=1.5）。统一1M key、dim300、THREADS64、CLIENTS4、PIO4、SNW4、pipeline32、batch32、PROFILE0、BUILDskip、KILL_OPENCODE0、KILL_MUTAGEN0。每组使用唯一RUN_ID，比较总QPS、单核QPS、CPU、延迟及四项失败计数。Zipf只用于观察访问局部性，不能等同于“预填1M只读取其中100K”。大缓存容量足够也不保证全部命中，4次有限探测仍可能发生冲突替换，需诊断命中率才能确认。逻辑payload吞吐QPS*1200B不等于实测UB总线带宽；1216B物理stride也不代表每次一定传输1216B。当前尚无证据认定UB随机读取是剩余瓶颈。

### 1M key 跨机缓存A/B结果（2026-10-16 20:26—20:31）

用户明确授权停止上述Mooncake进程后，对核实身份的PID631323发送SIGTERM；随后进程退出，两机负载和UB设备检查通过，顺序完成三组测试。参数与前述计划一致，SDK位置缓存关闭。没有修改正式v2代码或SDK；大缓存仍为独立实验构建。

| 服务端位置缓存 / 访问分布 | 总QPS | 单核QPS | 平均 / p99 ms | 进程RSS KiB | fanout |
|---|---:|---:|---|---:|---:|
| 默认131,072 entry / Uniform | 5,409,704.86 | 691,013.05 | 1.38148 / 1.543 | 688,504 | 1.000015 |
| 大缓存2,097,152 entry / Uniform | 7,877,852.63 | 1,005,515.07 | 0.90515 / 1.079 | 1,074,048 | 1.000015 |
| 同一大缓存 / Zipf s=1.5 | 9,451,222.77 | 1,604,254.44 | 0.59960 / 1.047 | 1,074,064 | 1.992000 |

三组status_notfound/status_err/materialized_fail/unmatched均为0。单核QPS沿用脚本口径；PROFILE=0，没有生成perf采样或SVG。三组SDK SHA256均为 `886ce2f76d697ce2b6666cb5d6c566a52ddbebe4bacccad347b36b8a5a61dc99`，memtier均为 `3356fa2c9492f2689a046e88717da336506ac3bd5f9d925e8bee4f85c3aa59bf`。默认/大缓存服务端SHA256分别为本节前述 `d944219c...` / `62070e40...`，完整哈希随server.meta.txt归档。

本地artifacts分别为：

- `perf/p4_s4_1m_default_cache_20261016_202649`
- `perf/p4_s4_1m_large_cache_retry_20261016_202919`
- `perf/p4_s4_1m_zipf_large_cache_20261016_203019`

每个目录保存client.workload.summary.tsv、client/server.meta.txt及server.cpu.process.tsv和cpuset统计。大缓存首次尝试 `p4_s4_1m_large_cache_20261016_202749` 完成workload后因实验目录缺少 `.vemb_v16_build_stamp.server` 在汇总阶段中止；补齐stamp后重新执行，表中仅采用retry的完整报告，不将首次不完整报告混入样本。

结论：Uniform条件下只扩大服务端位置缓存，总QPS提高45.6%、单核提高45.5%，进程RSS增加约376.5MiB；说明固定的小位置缓存是1M key回归的重要原因。仍未达到历史100K的10.06M，但本轮没有测量实际cache hit/miss，不能宣称已全部命中，也不能把剩余差距直接归因于UB硬件。Zipf的逻辑QPS比大缓存Uniform高20.0%，同时fanout接近2；脚本fanout=(leaders+followers)/leaders，热点访问会增加SDK请求合并，改变实际物理请求量，因此该组不能独立证明“UB工作集缓存造成全部差异”。后续应优先采集真实cache命中率和分阶段耗时，再用预填1M但限制读取100K范围的Uniform对照，尽量保持fanout接近1；避免直接更改正式缓存容量或宣称10M已恢复。当前每组一轮，尚无稳定性统计。

### 大缓存版本的 cycles 与 LLC miss 火焰图（2026-10-16 20:56—21:05）

为区分“大缓存仍有hash慢路径”与“命中后随机校验成本”，使用同一大缓存服务端构建 `62070e407dbdce9452bd049fe600a9036ee4b30bbdd9737cfe31451daad7b773`、同一客户端SDK/memtier二进制，对1M和100K Uniform分别采集`cycles`和`LLC-load-misses`。4:4、PIO/SNW4、64线程、pipeline/batch32、PROFILE采样25秒、压测30秒，均为单轮，四项失败计数均为0。SDK与memtier SHA256分别为 `886ce2f7...` 和 `3356fa2c...`，各轮一致。112源码戳与旧构建戳不一致，因此使用BUILD=skip，未重建客户端；实际客户端二进制哈希与之前各轮一致。

`cycles`轮：100K总QPS `10,095,301.27`、单核 `1,411,054.01`，Redis进程约使用7.142 core；1M为 `7,950,834.83`、`1,020,295.04`，约使用7.779 core。1M吞吐低约21.2%，Redis CPU增加约8.9%，每个Redis core的有效吞吐下降约27.7%。所以“多用了一些core”是观察结果，不是根因；更准确地说，1M每个请求需要更多CPU周期，调度到更多worker只能部分补偿。

按cycles火焰图的self sample占比，1M/100K分别为：`tlc_core_get_warm_location_raw` 23.08%/16.84%，`location_cache_validate_location` 15.38%/7.49%，`key_meta_find_locked` 0.86%/<0.5%。这表明扩大缓存后key-meta shard hash查找不再是主要成本；显著增长的是location cache命中后的slot验证路径。每个location-cache命中仍调用`location_cache_validate_location`，继续通过`warm_slot_probe_status`读取slot_meta快照，检查READY、write_seq、generation、bytes、key_hash和fingerprint。slot_meta每槽占64B；1M随机key对应约64MB的slot_meta工作集，而100K约6.4MB。该工作集差异与函数热点上升相符。

`LLC-load-misses`事件采样进一步支持此判断：在被采样的LLC miss栈中，`location_cache_validate_location`占比1M为31.91%、100K为17.83%；`tlc_core_get_warm_location_raw`为26.21%/17.59%。这是采样栈归因比例，不是每请求LLC miss次数或绝对UB总线流量，但方向与cycles结果一致。server侧`sve_streaming_load_f32`仅约0.20%/0.34% cycles样本，server火焰图没有显示payload复制是server瓶颈；payload复制主要发生在client读取侧，需分开看。

client cycles火焰图中，1M的`sdk_handle_session_route_requests` self占比60.26%，100K为10.45%；`sve_streaming_load_f32`则为12.46%/32.88%。`sdk_handle_session_route_requests`并非新发现的未优化热点：其O(4096)空slot扫描的来源，以及用`route_head`/`route_next` intrusive list改为遍历active请求的O-C1方案和验证结果，已记录在[`docs/client_cpu_opt_v2.md`](client_cpu_opt_v2.md)；该文档也记录了O-C2移除诊断时间戳以消除热路径`__udivti3`的实现。本轮使用`BUILD=skip`，且112上的源码构建戳与二进制不一致，因此本轮profile不能证明实际运行的SDK是否包含这些优化，也不能据此把route函数列为新的代码优化方向。应先核对被测SDK二进制与O-C1/O-C2版本的提交和产物哈希，再决定是否需要重跑client profile。脚本没有采集client CPU core-equivalent，当前也不能判断该函数是否限制总吞吐。

火焰图及原始采样：

- 1M cycles：`perf/p4_s4_1m_large_cache_profile_20261016_205649/server/server.svg` 与 `.../client/client.svg`
- 100K cycles：`perf/p4_s4_100k_large_cache_profile_20261016_205834/server/server.svg` 与 `.../client/client.svg`
- 1M LLC miss：`perf/p4_s4_1m_large_cache_llcmiss_20261016_210231/server/server.svg`
- 100K LLC miss：`perf/p4_s4_100k_large_cache_llcmiss_20261016_210418/server/server.svg`

历史实验已明确证明直接跳过slot验证违反shared slot_meta的generation/owner约束，并已撤回；不重复该实验，也不删除校验。下一步的目标不是泛泛地“再采一遍L1/LLC/DTLB”，而是解释1M工作集相对100K的slot校验增量来自哪里：L1 miss但由较近层级命中，LLC miss导致更远端取数，还是随机metadata访问带来的dTLB miss/page walk。现有LLC miss火焰图只给出了采样栈归因比例，尚未给出每请求的绝对miss数；因此若目标机器PMU支持，应在相同SN worker和相同工作负载上用`perf stat`测量`L1-dcache-load-misses`、LLC load miss及dTLB miss/walk事件，并除以成功逻辑读数。事件不支持或发生明显multiplex时，分组单独测量并报告运行比例，避免把采样栈比例当成每请求开销。结合目前LLC样本已明显上升这一证据，L1/DTLB是进一步区分机制的诊断项，不是开始修改slot校验的前提。

客户端先做独立的版本核验：将本轮实际SDK二进制与`docs/client_cpu_opt_v2.md`记录的O-C1/O-C2优化产物对齐，确认路由链表实现和诊断计时移除都已进入被测二进制；之后才有必要复测client的函数占比和CPU core-equivalent。所有优化必须保留slot身份/generation、删除/复用和迁移约束，再做相同工作集A/B。

### 1M key 的 L1/LLC/DTLB per-worker PMU 对照（2026-10-16 21:49—21:51）

在同一大缓存服务端构建 `62070e407dbdce9452bd049fe600a9036ee4b30bbdd9737cfe31451daad7b773` 上，按上一节的4:4 Uniform参数分别跑100K和1M key。使用`PROFILE=0 SERVER_PMU=1`，按SNW/PIO线程采集事件组`{cycles,instructions,L1-dcache-load-misses,LLC-load-misses,dTLB-load-misses,dtlb_walk}`，避免火焰图采样干扰。分析每轮PMU第5至第25秒的21个一秒区间，将4个SNW线程计数求和后除以同期逻辑读数估算每逻辑读事件数；稳态窗口内事件运行比例为100%，首个启动样本和结束样本的`not counted`记录不纳入统计。该值包含SNW的轮询及其他工作，不能解释成单次slot_meta访问成本。

| SNW事件 | 100K / op | 1M / op | 1M相对变化 |
|---|---:|---:|---:|
| cycles | 745.61 | 1,158.21 | +55.3% |
| instructions | 852.03 | 686.27 | -19.5% |
| IPC（instructions / cycles） | 1.14 | 0.59 | -48.1% |
| L1-dcache-load-misses | 14.98 | 17.26 | +15.3% |
| LLC-load-misses | 2.95 | 6.58 | +123.4% |
| dTLB-load-misses | 3.83 | 6.02 | +57.3% |
| dtlb_walk | 2.49 | 4.88 | +96.0% |

100K为 `9,957,661.24` QPS、单核 `1,390,200.09`；1M为 `7,943,484.47` QPS、单核 `1,013,982.15`。两轮notfound、status_err、materialized_fail、unmatched均为0，client SDK与memtier哈希相同。SNW的LLC miss/op超过2倍、dTLB walk接近2倍，而退休指令/op下降、cycles/op显著上升，表明SNW侧每逻辑读的访问等待明显增加，而非执行了更多指令。PIO的LLC miss/op为0.610降至0.544，但PIO instructions/op为966.21升至1,653.18、cycles/op为924.05升至1,158.26、L1 miss/op为40.63升至56.62、dTLB miss/op为8.55升至12.09。因此LLC miss增量集中在SNW，但PIO侧也出现每请求工作量/等待成本增长；不能把端到端QPS差值全部归因于slot_meta或SNW。

这与前一节`location_cache_validate_location` cycles self占比从7.49%升到15.38%、LLC miss栈占比从17.83%升到31.91%的结果一致，支持大工作集下SNW随机metadata访问的cache locality与地址转换成本上升。server log中的`MAP_SHARED|MAP_HUGETLB`失败只表示应用显式请求的CPU hugetlb映射未被OBMM设备接受，随后退回普通`MAP_SHARED`设备映射；它既不说明UB backing不是硬件大页，也不能说明普通设备mmap最终使用的CPU页表叶大小。当前设备sysfs只报告UB export地址和区域大小，没有给出CPU映射粒度。dTLB PMU计数是独立观察到的翻译事件，但per-worker计数聚合了worker的所有访问，尚不能把miss精确归到slot_meta某条load，也不能把全部QPS差值归因于DTLB。111提供`arm_spe_0`，且暴露load filter和min-latency配置；下一步先在运行中的Redis `/proc/<pid>/smaps` 核实该设备映射的`KernelPageSize`/`MMUPageSize`，若设备映射的smaps字段不足以判定，再查OBMM驱动mmap实现；随后用SPE对SNW和PIO分别采样load latency及虚拟地址，验证高延迟样本是否落在slot_meta映射区。还应分别分析PIO增加的指令/工作成本；不要据此移除slot身份校验。

Artifacts：`perf/p4_s4_100k_l1_llc_dtlb_20261016/`和`perf/p4_s4_1m_l1_llc_dtlb_20261016/`，包含每worker原始PMU CSV、事件和TID列表、workload summary及server/client metadata。

### SPE 地址样本复核（2026-10-17）

对 `perf/allocv2_1m_spe_addr_20261016/server/server-spe-address.data` 在 SNW 时间窗 `210895,210922` 做了定向 `perf report --mem-mode` 复核，样本无丢失。报告中可以直接看到以下访问形态：

- `l1d-miss`：`tlc_core_get_warm_location_raw`、`location_cache_validate_location` 和少量 `key_meta_find_locked` 均出现 `RAM hit + Walker miss`；四个 SNW 的相对归因形态一致。
- `llc-miss`：上述两个 warm-location/slot 校验函数仍是主要用户态归因栈，且多数样本为 `RAM hit + Walker miss`。
- `l1d-access`：同一批函数同时存在 `L1 hit + Walker hit` 和 `RAM hit + Walker miss`，说明报告同时覆盖了缓存命中与高延迟地址转换路径。

这一步确认了“SNW 的 warm location/slot 校验访问会伴随 DTLB walker miss，并在部分样本上走到 RAM”这一机制证据；它不能给出每请求的绝对延迟，也不能单独证明访问的是 `slot_meta` 还是 payload。更关键的是，地址样本中的数据对象（例如 `0xfffe34dae2e0`）属于 SPE 采样时的进程地址空间，而归档的运行时映射边界来自另一份保存记录（当前记录的 `/dev/obmm_shmdev4` 起点为 `0xfffe9e90e000`）；两者不一致。因此本轮不做 metadata/payload 地址比例统计，也不把旧地址与当前映射强行比较。

要完成地址归类，下一轮必须在 Redis 仍存活时同一脚本内保存 `/proc/<pid>/maps`/`smaps`，并以该 PID、同一 perf.data 和同一时间窗计算 `warm_base <= data_object < warm_base + layout_metadata_bytes`。在此之前，现有 PMU per-op 结果和 SPE 的函数/Walker 证据足以支持“1M 工作集增大了 SNW 的 LLC/DTLB 压力”，但不足以支持修改 slot 校验或宣称 payload 访存是唯一根因。

### 同 PID 映射绑定复测（2026-10-17）

本轮使用 `RUN_ID=allocv2_1m_spe_maps_20261017d`，启动后记录服务端 PID `853754`，在该进程存活期间保存了 `/proc/853754/maps` 和 `/proc/853754/smaps`，并对同一 PID 启动 SPE：

```text
perf record -e arm_spe_0/load_filter=1,pa_enable=1,min_latency=512/ \
  -p 853754 -o server-spe-address.data -- sleep 40
```

本轮 workload 为1M key、4:4、Uniform、dim300、pipeline/batch32、PROFILE=0；QPS `4,840,774.39`，单核 `632,537.54`，四项失败计数均为0。该轮吞吐明显低于前一轮，属于诊断样本，不能作为性能对照。

同一 PID 的 WARM 映射为 `0xfffe89b8b000-0xffff97600000`，大小 `4,524,036,096` bytes；layout metadata `226,050,944` bytes，因此 metadata 范围为 `[0xfffe89b8b000, 0xfffe9731f380)`，payload 从 `0xfffe9731f380` 开始。`smaps` 对该设备映射报告 `KernelPageSize=4 kB`、`MMUPageSize=4 kB`；这再次确认 CPU 侧当前是4KiB页表映射，不能据此否定 UB backing 的2MiB分配粒度。

同一 SPE 文件的 `perf report --mem-mode` 仍显示 `location_cache_validate_location` 在 `l1d-miss`、`llc-miss` 和 `tlb-miss` 中以 `RAM hit + Walker miss` 为主要归因形态，且四个 SNW 一致。当前 ARM perf 输出的高层表格未稳定展开 `data_object` 行，故本轮尚未得到可审计的 metadata/payload 样本计数；但此前“地址来自另一轮映射、不可比较”的问题已经排除。下一步只需补一个离线解析器，从该同一 SPE 文件提取 data object 并按上述区间计数，避免再启动大文件采样。

Artifacts：`perf/allocv2_1m_spe_maps_20261017d/server/server.maps`、`server.smaps`、`server/server-spe-address.data` 及 workload summary。

负载恢复后的有效C4轮为 `perf/p3_s3_c4_uniform_alloc_v2_stride64_idle_20261016`：总QPS `7,756,944.18`、单核 `1,371,439.27`、平均/p99 `0.92507/1.103ms`，四项失败计数全为0。实际 `/proc/616913/exe` SHA256与本阶段二进制 `d944219c...` 一致，客户端SVE复制叶节点cycles占比10.78%。C3/C4仍有并发规模相关差异，不能宣称所有参数均已达到8M。

随后使用原目录旧分配器做相邻C4对照，参数完全相同。artifact `perf/p3_s3_c4_uniform_alloc_v2_stride64_control_20261016`，服务端记录哈希 `280a63b983571e46601a52083775655b8cf6176c6f38adf36473de8ac83509f8`；旧分配器总QPS `7,466,836.97`、单核 `1,278,176.20`、平均/p99 `0.87967/1.191ms`，四项失败计数全为0。两轮SDK静态库哈希 `886ce2f76d697ce2b6666cb5d6c566a52ddbebe4bacccad347b36b8a5a61dc99`，memtier哈希 `3356fa2c9492f2689a046e88717da336506ac3bd5f9d925e8bee4f85c3aa59bf`，均一致。v2对齐版本轮总QPS高约3.9%，单核高约7.3%；平均延迟略高而p99较低，不能笼统宣称所有延迟指标提升。当前证据支持相对旧分配器未出现吞吐劣化；不代表已完成多轮稳定性、高占用或SDK并发回收读验收。

### 同构 100K/1M PMU 复测（2026-10-17）

为确认上一轮 1M 结论不是运行时段或机器状态造成，使用同一个大缓存服务端二进制 `62070e407dbdce9452bd049fe600a9036ee4b30bbdd9737cfe31451daad7b773`、同一个 SDK `886ce2f...` 和 memtier `3356fa2...`，在同一时间窗口连续执行 100K 与 1M Uniform，参数均为 `THREADS=64 CLIENTS=4 PIO=4 SNW=4 pipeline=32 batch=32 PROFILE=0`。服务端同时采集 `{cycles,instructions,L1-dcache-load-misses,LLC-load-misses,dTLB-load-misses,dtlb_walk}`；PMU 只统计第 5 至第 25 秒的稳态样本。

| 数据集 | 总 QPS | 单核 QPS | 平均 / P99 ms | Redis core | RSS KiB |
|---:|---:|---:|---:|---:|---:|
| 100K | 9,941,126.03 | 1,375,961.44 | 0.56248 / 0.983 | 7.211 | 866,684 |
| 1M | 7,843,463.42 | 1,004,268.23 | 0.90599 / 1.063 | 7.795 | 1,077,104 |

相同构建下，1M 相对 100K 的总 QPS 下降 21.1%，单核 QPS 下降 27.0%。SNW 每逻辑读的计数为：cycles `751.30 -> 1,171.86`（+56.0%）、instructions `849.74 -> 692.59`（-18.5%）、L1 miss `14.94 -> 16.34`（+9.4%）、LLC miss `2.95 -> 6.62`（+124.7%）、dTLB miss `3.92 -> 5.54`（+41.4%）、DTLB walk `2.90 -> 4.53`（+56.3%）。PIO 侧 cycles `924.59 -> 1,171.85`、instructions `947.40 -> 1,587.01`、dTLB miss `8.20 -> 11.12`，说明端到端增量不只来自 SNW，但 SNW 的 LLC miss 增量最明显。

两轮正确性计数均为零。服务端 WARM 设备映射分别为 `4,521,021,440` bytes，`smaps` 均报告 `KernelPageSize=4 kB`、`MMUPageSize=4 kB`、`THPeligible=0`、`ShmemPmdMapped=0`；启动日志均记录 `MAP_HUGETLB` 返回 `EINVAL` 后退回普通 mmap。该轮确认了“1M 工作集导致 SNW 访问等待和地址转换压力增加”，并确认 CPU 侧当前没有使用 huge-TLB 映射；仍不能仅凭 PMU 将全部 QPS 差值归因于 slot_meta 或断言 UB backing 本身不是 2MiB 粒度。

Artifacts：`perf/allocv2_next_100k_pmu_20261017/`、`perf/allocv2_next_1m_pmu_20261017/`。两轮的 `server.maps`/`server.smaps` 与 PMU CSV 已保存，可用于后续同一 PID 的 SPE 地址归类。下一步应做受控的 metadata locality 或页表粒度 A/B，并保持 slot generation/owner 校验不变；不再重复只扩大 location cache 的实验。

### OBMM `MAP_HUGETLB` 能力探针（2026-10-17）

为区分“映射长度未按2MiB对齐”和“设备不支持 huge-TLB mmap”，在111停止测试服务后直接对空闲 `/dev/obmm_shmdev4` 做了 root 只读探针。2MiB hugepage 池本身可用：`nr_hugepages=131072`、`free_hugepages=95487`。对 metadata 长度 `226,050,944`、向上取整长度 `226,492,416`、完整 WARM 长度 `4,521,018,240` 及其向上取整长度 `4,521,459,712`，`MAP_SHARED|MAP_HUGETLB` 全部返回 `EINVAL`；增加显式 `MAP_HUGE_2MB` 标志仍返回 `EINVAL`。因此当前 `MAP_HUGETLB` 失败是 OBMM 设备 mmap 接口能力/约束问题，不是 hugepage 池耗尽，也不能通过应用侧简单 round-up 修复。普通 mmap 仍报告4KiB CPU 页表；若要降低 DTLB 压力，需要驱动/设备侧提供 PMD 映射能力，或先优化 metadata 的访问局部性。

### 1M resident、100K active-read 对照（2026-10-17）

为区分“WARM 中驻留的 key 总数”和“本轮实际访问的 key 工作集”，临时只在 client workload wrapper 中把读阶段的 `--key-maximum` 从1M改为100K；prefill阶段仍完整写入1M key，服务端 allocator、manifest、SDK和memtier二进制均未修改。相同大缓存服务端、4:4、PIO/SNW4、64线程、pipeline/batch32下，1M resident/100K active-read 得到总 QPS `9,975,189.68`、单核 `1,378,157.72`、平均/P99 `0.55966/0.983ms`，所有正确性计数为零。

该结果与同构的100K resident/100K active-read `9,941,126.03` / `1,375,961.44` 基本一致；SNW 计数也接近：cycles `752.83` vs `751.30`、LLC miss `2.95` vs `2.95`、DTLB walk `2.50` vs `2.90`。相比1M active-read的 `7.843M` QPS和 `6.62` LLC miss/op，差异随实际访问工作集出现，而不是随WARM总容量出现。该对照确认应优化 active-read 路径上的 location/slot metadata locality；扩大 region 或仅增加总 payload 容量不会解决1M Uniform读回归。

Artifact：`perf/allocv2_resident1m_read100k_20261017/`，其中保存了服务端 PMU、maps/smaps、客户端 workload summary。临时 wrapper 已在测试结束后清理。

### 位置缓存命中后的 slot metadata 预取 A/B（2026-10-17）

为验证随机 `slot_meta` 访问是否能通过软件预取隐藏 UB 读取等待，做了一个编译期诊断实验：在 `location_cache_validate_location()` 解析出有效 region/slot 后，对完整 64B `slot_meta` 发出一次 `__builtin_prefetch()`，随后仍执行原有的 `state`、`write_seq`、`owner_generation`、`bytes`、key hash/fingerprint 和二次 seqlock 校验；共享布局、读协议和回收语义均未改变。客户端、manifest 和压测参数保持不变。

100K active-read 结果为 `9.943M/1.378M` QPS/单核 QPS，与同构无预取轮 `9.941M/1.376M` 基本一致。1M active-read 预取轮为 `5.652M/0.722M`，关闭预取的相邻轮为 `5.582M/0.711M`；两轮均明显低于此前稳定窗口的约 `7.84M/1.00M`，差异只有约1.3%，不能视为预取收益。两轮正确性计数均为零。

结论：软件预取没有显示可重复的端到端收益，且可能增加设备/内存请求压力；诊断宏和代码不保留。当前应继续做结构性 metadata locality A/B（减少随机工作集或由驱动提供更大页表映射），不能以该实验支持修改读协议或删除 slot 身份校验。Artifacts：`perf/allocv2_prefetch_100k_20261017/`、`perf/allocv2_prefetch_1m_20261017/`、`perf/allocv2_noprefetch_1m_20261017/`。

### 当前 v2 二进制的 100K 回归与 1M slot 校验诊断（2026-10-17）

最新边界校验改动同步到111的 `/root/szz/codespace/hpc-redis-dev-alloc-v2-20261016` 后，远端 `vemb_v16_tlc_ut` 通过，服务端以 `USE_UB=yes USE_SVE=yes` 重建。相同构建下的100K、4:4、Uniform回归为总QPS `10,014,372.69`、单核 `1,417,375.45`，平均/P99 `0.55835/0.983ms`，四项失败计数均为0；artifact为 `perf/p4_s4_svr0-15_uniform_20261017_085717`。这确认动态容量边界校验和compact location-cache entry没有损失100K的10M档位。

同一服务端二进制的1M、4:4、Uniform、`PROFILE=1`得到总QPS `9,332,084.94`、单核 `1,197,384.12`，平均/P99 `0.72285/0.935ms`，Redis RSS `1,121,612 KiB`；artifact为 `perf/p4_s4_1m_compact_cache_latest_20261017`。cycles火焰图中，`location_cache_validate_location` self占 `17.92%`，`vemb_v16_tlc_get_handle_stable_read`占 `40.56%`；client侧 `sdk_handle_session_route_requests`占 `48.26%`、`sve_streaming_load_f32`占 `14.53%`。该轮四项失败计数均为0。

为避免把函数热点误判为hash miss，临时编译了已有的`VEMB_V16_LOOKUP_DIAGNOSTICS`宏，并在1M workload上读取每个SN worker的采样日志。诊断构建自身会显著降低吞吐（该轮仅`3.746M` QPS），不作为性能样本；只使用它的计数：4个SN worker合计约`118.0M`次TLC lookup，cache hit约`117.98M`，miss约`19,949`，miss比例约`0.017%`。因此`location_cache_validate_location`的主要成本不是动态hash查找，而是几乎每一个命中都重复读取并校验slot metadata。

对同一1M、4:4工作集，仅将PMU附着到4个`vemb-sn-*`线程，避免此前把计数落到PIO线程或超出硬件计数器容量：

| 计数 | SNW合计 | 比例 |
|---|---:|---:|
| L1-dcache-loads | 16,247,279,293 | - |
| L1-dcache-load-misses | 837,025,035 | 5.15% |
| LLC-loads | 483,931,837 | - |
| LLC-load-misses | 329,929,592 | 68.18% |
| dTLB-loads | 40,615,808,322 | - |
| dTLB-load-misses | 645,076,404 | 1.59% |
| dtlb_walk | 489,599,392 | - |

对应有效吞吐约`9.357M` QPS；原始计数保存在`perf/p4_s4_1m_pmu_sn_target_20261017/server/server.pmu.sn.csv`和`perf/p4_s4_1m_pmu_sn_dtlb_20261017/server/server.pmu.sn.dtlb.csv`。这组结果支持“slot metadata随机访问主要受LLC局部性影响，DTLB是次要但可观的附加成本”；线程级计数仍包含SNW的轮询和其他访问，不能把每个miss精确归因到某一条slot_meta load。

当前结论与约束：

- 不能继续只扩大location cache；命中率已经接近100%，剩余差距来自命中后的slot验证和工作集局部性。
- 不能直接删除slot身份、generation、owner或seqlock校验。普通读取最终仍要保证payload复制不会跨越写入边界。
- 下一步应设计“可失效的cache-hit快路径”：slot被释放、复用、同key覆盖或迁移时，必须使对应cache entry失效；命中时只读取紧凑的本地失效版本/epoch，未失效再返回location，必要时才读取完整slot_meta。payload复制阶段继续保留现有seqlock前后检查。
- 该快路径必须先做受控A/B和并发回收UT，再进入正式性能构建；本轮没有保留诊断宏，远端生产`redis-server`已恢复为 `48ed593dc34c282731f31f9038114329614927bc174c6ca884252521a6aceca0`。
