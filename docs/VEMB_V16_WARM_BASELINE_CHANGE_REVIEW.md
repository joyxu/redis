# VEMB v16 当前实现相对 HEAD 基线的变更审查

> 历史审查：本文的 import index、在线发现扫描和 bitmap 搜索保留建议已被后续决策替代。`dev_alloc_v2` 以 [allocator v2 设计](VEMB_V16_WARM_ALLOC_V2_DESIGN.md) 为准：单 region 单 owner，顺序分配与回收栈，其他节点只读；不恢复 imported 查询。性能与 UT 最新记录见新设计文档。

本文以基线 `e1166dc9e7ff69d069ad6252e31f62ef167e7e89` 为参照，审查当前工作树中与 WARM 容量、分配、索引和读取路径有关的改动。目标是判断哪些改动是“满载能力”的必要组成，哪些改动应该继续隔离验证，避免把性能实验误当成设计要求。

## 结论

当前应保留“按 region 统一空闲槽池分配 + 动态 key 元数据索引 + manifest 推导物理容量”这条主线。它解决了原来按桶/固定 8-way 分配导致局部桶耗尽、整体还有空闲空间的问题，也是实现 WARM 满载的必要条件。

当前没有证据表明读取协议、Aeron 批处理或 SVE 本身被改成了更快或更慢的算法。已经回退的 `sve_streaming_load_f32` 前缀对齐实验不应重新引入；它不是正确性修复，实测属于负优化风险。

仍有两个需要单独收敛的性能风险：

1. 本地 key 元数据缺失时，读取会退化为整个 region 扫描；这是兼容发现路径，不能直接删除，但必须限制触发范围并持续观测。
2. 分配器目前以 64B 对齐槽作为搜索起点，而 1200B payload 的槽步长并不能让所有槽都对齐。该策略只改变搜索起点，不牺牲容量；稀疏场景收益已有证据，接近满载时的收益尚未证明。

## 应保留的功能性改动

| 改动 | 判断 | 理由 |
| --- | --- | --- |
| region 级共享 free bitmap，槽状态用 FREE/FILLING/READY 做跨进程 CAS | 必须保留 | 这是取消 8-way 限制后仍能安全并发分配的核心。仅有进程本地 bitmap 不足以解决多个 TLC/进程竞争同一槽的问题。 |
| `warm_place_in_region()` 从 hash 起点开始遍历全部 free word | 必须保留 | hash 只决定起点，所有后续槽仍可达，避免 hash 偏斜把空闲容量困在不可用桶中。 |
| key metadata 从固定容量改为分片动态扩容，记录 key 到 location 的完整映射 | 必须保留 | 满载需要允许 key 数超过初始容量；分片锁下扩容、稳定 ordinal 和 75% 负载阈值保持了并发安全和分页稳定性。 |
| region 容量由 `region_bytes / value_size` 推导，legacy `max_vectors` 仅作为首 region 的兼容描述字段 | 必须保留 | 消除了 `vemb-v16-max-vectors` 对 WARM 容量的人为上限，同时保留旧 wire 字段的兼容性。 |
| attach 时建立 READY/FILLING 槽的 immutable import index | 保留 | 允许新进程发现已有数据，避免每次读取都扫描 region；generation、fingerprint 和 slot 校验属于并发/协议完整性检查。 |
| 删除未使用的 HOT 表、warm_idx、旧 warm record/hash 数组和 tlc bitmap | 保留 | 这些结构没有参与当前读写闭环，删除后降低重复状态和初始化成本。删除前已有调用点迁移到 key metadata/import index。 |
| reset/destroy 时释放 free bitmap、import index、动态 shard 数组 | 必须保留 | 属于资源所有权和状态切换清理，不能为了简化代码去掉。 |

## 保留但需要后续收敛的改动

### 全 region 扫描兼容路径

`warm_lookup_region()` 先查 attach 时的 import index；只有没有命中时才逐槽调用 `warm_slot_probe_status()`。该路径解决了“远端发布的数据没有本地 key_meta”这一兼容问题，因此不能因为它慢就直接删除。当前实现的问题是：当 key 不存在且任意槽处于 BUSY 时，最多重复扫描整个 region 1024 轮；即使没有 BUSY，单次 miss 也可能是 `O(capacity_slots)`。

建议保留代码，但把它定义成兼容/恢复路径，并完成以下约束：

- 正常本地读写必须通过 key metadata 或 import index 命中；为该路径增加计数、扫描槽数和 BUSY 重试计数。
- 对稳定运行的本地 READY 数据，不允许每次请求重新进入全扫描。
- 后续可增加 region 级发布索引或 RPC 查询来消除扫描，但要先证明所有生产调用方都能提供该索引，再删除 fallback。

### 64B 对齐优先的槽搜索

当前算法只改变搜索起点，不预留、不填充、不限制可用槽，因此对容量正确性是安全的。它在之前的稀疏实验中明显优于任意非对齐起点，值得保留为候选优化。但对 1200B value，64B 对齐槽的比例受步长约束，满载后搜索会自然落到普通槽；因此不能把稀疏结果直接作为满载性能保证。

验收必须覆盖 25%、75%、90% 和 98% 占用率，并同时记录总 QPS、单核 QPS、平均探测 word 数和分配失败率。若高占用时线性扫描成为热点，再增加层级 free-word summary 或近似计数器；不要恢复 8-way 限制。

### remote_meta 默认容量

未显式提供 `remote_meta_entry_count`/set-way 配置时，当前实现按所有本地 region 的物理槽数计算 entry 数，再按约 2 倍计算 bucket 数（见 `storage_remote_meta_open_view()`）。这与“容量从 manifest 推导”一致，但可能使远端索引随 UB 物理容量一起变大，带来固定内存开销。

该逻辑暂不应回退到固定 max-vectors；应补充启动日志和容量预算，并在 manifest 很大时验证 `remote_meta` 是否允许显式比例/上限。如果部署不需要为每个物理槽预留远端索引，可改为按预计 key 数动态扩容，而不是恢复旧的固定桶表。

## 独立改动，应保留但不要与 WARM 性能结论混合

- HA resync 在收到更新 session 时清除旧的 `snapshot_installed_pending`，避免把旧快照误认为新 session 已完成。该改动修复状态机一致性，和 GET 热路径无关，应单独保留并由 HA UT 覆盖。
- runtime SHM attach、失败清理、manifest/peer descriptor 校验和 response 初始化为错误状态，属于资源所有权、协议完整性或外部输入校验，应保留。
- `VEMB_V16_LOOKUP_DIAGNOSTICS`、pipeline diagnostics 和可选 PMU 采样默认关闭；诊断代码不进入生产热路径，可以保留用于复现性能差异。
- benchmark/脚本中删除参数、修正构建和链接目标的改动可保留，但脚本里仍残留 `MAX_VECTORS` 变量和校验。它们应改名为容量/测试数据集参数或明确标注为兼容脚本，不能继续宣称已经完全取消 max-vectors。

## 不应保留或不应作为设计依据的内容

- 已回退的 `sve_streaming_load_f32` 16 行前缀 peel：没有正确性必要性，当前证据显示会造成性能下降。
- 为了恢复吞吐而跳过完整 location 校验、放宽 generation/slot 状态检查、或在 cache 命中后无条件相信旧 location：这些检查负责防止读到半写 payload，不能作为优化手段删除。
- 默认开启 LRU/在线占用槽回收：当前没有安全的 epoch/reclamation 协议，删除仍保留 payload；在协议补齐前不能把它当作满载方案。
- 仅凭一次 3:3、4:4 或 10K/100K 结果判断读取算法优劣。当前已知 3:3 可达到约 7.8M，4 client 会受并发/客户端侧压力影响；10K 与 100K 的工作集、缓存和连接数不同，必须用同一二进制、同一 allocator、同一 PROFILE 设置对照。

## 建议的落地顺序

1. 保留共享槽池、动态 key metadata、manifest 容量推导和 import index，先为它们补齐容量/占用率指标。
2. 在相同构建参数下跑 25/75/90/98% 占用率的 3:3、4:4 对照，确认总 QPS 和单核 QPS；把全扫描计数和平均 free-word 探测数写入结果。
3. 若性能下降只在 cache miss 或 import miss 出现，优先优化索引发布/查询，避免改动 payload 读取和 SVE。
4. 清理脚本中的 `MAX_VECTORS` 命名和硬校验，保留仅用于旧 wire/测试兼容的字段说明。
5. 只有在上述 UT、满载压测和 HA/迁移测试都通过后，才考虑层级空闲摘要、批量索引发布或 COW 等二次优化。

