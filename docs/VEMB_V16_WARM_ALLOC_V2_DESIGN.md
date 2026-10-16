# WARM allocator v2：顺序分配与回收栈

## 基础与目标

基于 `dev_alloc` 快照 `9547218`，在 `dev_alloc_v2` 实现。保留 manifest 推导容量、动态 key metadata、location cache、slot generation/seqlock 和迁移 fence；替换 hash 起点、bitmap word 搜索和 region vnode 选择。在线查询不恢复 imported index 或物理 region 扫描，SVE 和 payload stride 不在本次修改范围。

## 分配状态与所有权

每个 region 的 owner 在本地 DRAM 维护 `next_slot`、预分配的 `uint32_t free_slots[capacity_slots]`、`free_count` 和 allocator mutex。回收栈每个物理槽占 4B DRAM，4GiB/1200B region 约需 13.65MiB。首次 attach 可以扫描 slot metadata 一次，确定历史 high-water mark 并收集其中的空闲洞；这只重建分配状态，不建立 key 反查索引。

同一进程中多个 TLC 映射同一 `slot_meta` 时，通过带引用计数的 allocator registry 共享该状态；registry 锁仅用于 attach/destroy，分配仅使用 region 锁。生产约束仍是每个 region 一个 owner 进程、同 key 路由到同一 owner。保留共享 slot 状态 CAS 防止外部占用导致覆盖；跨进程独立 writer 的 O(1) 回收可见性不由本地栈提供，不将其宣称为已支持。未来若需要跨进程共享 allocator，必须提供共享控制状态和相应协议。

分配在 region 锁下先 pop 回收栈，否则领取 `next_slot++`，没有候选立即返回 FULL；领取后仍通过 FREE→FILLING CAS 再写 payload、发布 READY。正常单 owner 每个 slot 领取固定成本，不扫描 bitmap。存在外部 writer 争抢的兼容场景可能跳过被占用候选，不承诺其最坏 O(1)。不启用 LRU 占用槽淘汰。

region 按 manifest 顺序用满后再切换，保持本地优先、远端 fallback 的现有能力。每个 core 保留活跃 region 提示；活跃 region 无空间后遍历有界 region 列表，检查各 region 的 allocator 状态并切换。因此槽选择为 O(1)，跨 region 切换为 O(region_count)，不随 slot 数增长。旧 region 后续回收的槽仍会在后续切换中重新使用，不永久跳过。当前上限为 256 个 configured/runtime region。

## 删除与回收

在 key shard 锁下，通过已知 metadata location 精确定位旧 slot；校验 generation/identity、取得写 seqlock 后使旧 slot 失效，推进 write_seq，发布 FREE，再入回收栈。只有成功回收的一方可 push，避免重复入栈。复用时推进 owner_generation，不重置 generation/write_seq，防止旧 handle 的 ABA。

DEL、COLD DEL replay、迁移 tombstone 安装需要采用一致的 slot 释放逻辑，同时保留 key version、tombstone 和迁移 fence。读取仍依赖现有状态、generation 与前后 seqlock 校验，防止复用时读到混合 payload。回收不改变 SDK 的读取协议。

## COLD 与 HA 恢复

启动恢复和 HA fenced 恢复须停止请求/复制应用，确保本节点拥有的 region 独占可重建。预先检查所有待重建 region 的 allocator 引用，若仍有其他 TLC 使用则失败，禁止部分 reset 后才发现冲突。仅初始化本地 region；远端映射保持原样。重建清除 location cache/key metadata，释放本地旧 slot 并重置 next_slot/free_count，保留并推进 slot generation/write_seq。无需清零整个 payload，旧字节不再可由有效 slot 读取。

之后加载 checkpoint，并按各 metadata shard 的 captured_seq 回放增量日志；无 checkpoint 走原有全日志回放。不会删除 COLD 日志或调用清空 COLD 的 resync reset。恢复失败不开放服务；重试重新初始化恢复状态。该流程解决旧 slot 占满但内存索引为空的已知回归。现有 COLD 写入 ACK/失败事件语义不在本次改变范围；历史回放峰值超过 WARM 容量的最终状态归并另行设计，不承诺支持超过容量的历史物化峰值。

## 落地步骤与验收

1. 保存旧分支快照并创建新分支，先写本文。
2. 替换 region bitmap/vnode 分配器，补齐 attach/destroy 生命周期和顺序 region 选择。
3. 实现精确 slot 回收，覆盖删除后重新写同 key/其他 key，以及旧 handle 拒绝。
4. 适配启动 COLD 和 fenced resync 重建，验证旧 region 已满、纯日志、checkpoint+tail、重复恢复与共享/远端保护。
5. 更新与旧 hash 分布、对齐优先、imported 发现绑定的 UT；保留满容量、并发唯一性、BUSY、动态索引和迁移语义断言。
6. 在 UB 机器独立目录编译运行 TLC、manifest、COLD、sync、resync、迁移和可运行的 HA UT；不修改正在运行的正式测试目录或停止无关进程。真实 HA 网络验收需要对应 UB/keepalived 环境。
7. 构建服务端，运行相同 100K Uniform 3:3 性能对照并记录 CLIENTS=3/4、总 QPS、单核 QPS和错误计数。没有实际结果前不宣称恢复性能或所有 UT 通过。

## 实施记录

- 已建立快照与新分支；代码实施及验证进行中。
