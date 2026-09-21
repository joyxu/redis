# VEMB V16 WARM Allocator v2 设计与落地计划

日期：2026-09-15

状态：P0-P6 主体已实现并完成本地、目标 aarch64 和真实双节点 UB 映射验证。当前代码已经落地 owner-local 派生容量、process-local 两层 bitmap allocator、local-only PUT、slot release/reuse、32B layout v2、启动重建和容量/allocator benchmark。`max_vectors` 仍作为若干旧 ABI/协议字段的忽略参数保留，后续可在协议版本切换时删除，但不再决定实际 WARM 容量。

## 1. 目标

当前 WARM 的主要问题是容量由 `max_vectors` 人工限制，region 之间存在不必要的 remote 写入和 fallback，slot 分配使用简单的递增指针后无法高效复用释放的 slot。本文将 WARM 改为：

1. 每个 HPC-Redis 只写自己拥有的 local region。
2. 所有 local region 的 slot 数之和就是本节点 WARM 容量，不再依赖 `max_vectors`。
3. 把 local region 逻辑上拼成一个 global slot array。
4. 先复用 free slot，再使用有边界的 `allocated_slot_index` bump pointer。
5. 使用两层 summary bitmap、thread-local word hint 和 CAS 抢占，支持多线程 owner 进程无锁分配。
6. WARM 满时返回明确的 `WARM_OOM`，不能写 remote region，也不能通过未声明的路径偷偷 spill 到其他节点。
7. slot metadata 压缩到 32B，同时保留 `key_fingerprint`，一个 64B cacheline 放两个 metadata。

## 2. 明确的 ownership 语义

CLI 已经通过一致性 hash 把 key 近似均匀地路由到 owner HPC-Redis。服务端仍要在 proxy/topology 边界校验 owner 和 topology epoch，不能完全信任 CLI。

### 2.1 写权限

- `is_local == 1` 的 region 才能参与本节点 WARM allocation。
- 只有该 region 的 owner HPC-Redis 可以写 payload、slot metadata、free bitmap 和 bump pointer。
- 普通 PUT、overwrite、DELETE、eviction 都不能写 remote region。
- migration target 只能写自己的 local region；source 在 `SOURCE_GC` 后才释放 slot。
- remote HPC-Redis 和 CLI 可以读 local region，但不能执行 allocator mutation。
- 建议 remote mapping 使用只读权限；即使底层映射暂时可写，也必须由服务端 owner 检查保证语义。

### 2.2 容量耗尽语义

本设计的 allocator 只负责 WARM 容量。所有 local region 没有可用 slot 时返回 `WARM_OOM`，语义等同 Redis 内存耗尽：

- 不尝试 remote region。
- 不因为 remote region 还有空间而成功写入。
- 不把 WARM 满伪装成普通 COLD 成功。
- 如果上层仍需要 COLD durability，必须先成功 reserve WARM slot，再执行 COLD append；WARM reserve 失败时不能先提交 COLD 后再返回成功。

## 3. 容量模型：移除 `max_vectors`

### 3.1 唯一容量公式

对每个 local region：

```text
region_capacity_slots = floor(payload_region_bytes / value_size)
```

本节点 WARM 总容量：

```text
local_capacity_slots =
    sum(region_capacity_slots for all regions where is_local == 1)
```

remote region 不计入本节点可写容量。各 region 尾部不足一个 `value_size` 的 bytes 单独统计为不可用尾部，不向上取整。

`uint64_t` 用于中间容量计算；如果 ABI 中的 `local_slot` 仍是 `uint32_t`，初始化时必须拒绝超过 `UINT32_MAX` 的单 region slot 数，不能截断。

### 3.2 manifest 要求

删除 `max_vectors` 后，manifest 必须提供完整的 local region 列表和每个 region 的 payload bytes。没有 manifest 时不能再用 `max_vectors` 推导默认 region 大小；建议直接启动失败，并给出缺少 local region manifest 的错误。

建议字段：

```yaml
local_ub_node_id: 0
warm_regions:
  - region_id: 10
    bytes: 1073741824       # payload bytes
    value_size: 1200
    home_ub_node_id: 0
    is_local: true
  - region_id: 11
    bytes: 1073741824
    value_size: 1200
    home_ub_node_id: 1
    is_local: false
```

所有 region 的 `value_size` 必须一致。`region_id` 是跨进程稳定标识，`region_index` 只是当前进程数组下标，不能混用。

当前 slot metadata layout 通过独立 allocator metadata mapping 管理，payload bytes 不应扣除这部分进程外/元数据映射的大小。如果未来把 metadata 和 payload 放到同一物理 region，容量计算必须先扣除 header、slot metadata 和对齐空间，再执行上述公式。

### 3.3 max_vectors 移除范围

代码实现时需要一次性审计并删除或改名以下使用点：

- server/config CLI 参数和 Redis 配置项。
- `tlc_core`、TLC storage、proxy、channel descriptor 中的容量字段。
- protocol descriptor、manifest 校验和日志。
- key metadata/warm table 的预分配容量。它们可以使用 `local_capacity_slots` 的派生值，或单独使用明确的 metadata load factor。
- benchmark、smoke test 和用户文档中的 `--max-vectors`。

内部可以保留 `warm_capacity_slots` 作为派生值，但不能再让调用者传入一个与 region 总容量可能不一致的独立上限。

## 4. 32B slot metadata layout

保留 `key_fingerprint`。推荐 layout v2：

```c
typedef struct vemb_v16_warm_slot_meta_v2 {
    _Atomic uint64_t state_version;
    _Atomic uint64_t owner_generation;
    uint64_t key_hash;
    uint64_t key_fingerprint;
} vemb_v16_warm_slot_meta_v2_t;
```

### 4.1 `state_version` 编码

```text
低 3 bit：state
  00 FREE
  01 FILLING
  10 READY
  11 EVICTING

其余 bit 保存 seqlock version；version 奇数表示写入中，偶数表示稳定。`region_id`、`local_slot`、`bytes` 都由运行时 region 和 slot index 推导，不再占用共享 metadata。`last_access_ns`、clock bit 和 COLD 状态属于 owner 进程的 runtime 数组，不写入共享 layout。

高 62 bit：version
```

稳定 READY snapshot 必须满足：state 为 READY，version 为偶数。writer 抢占时把 version 加一并进入 FILLING/EVICTING，完成 payload 和 metadata 发布后再把 version 加一并发布 READY。读者在 payload 前后读取两次 `state_version`，只有完全相等且仍是稳定 READY 才接受结果。

### 4.2 被移出的字段

- `region_id`：由 region runtime/header 推导。
- `local_slot`：由 metadata 数组下标推导。
- `bytes`：固定 `value_size`，不需要逐 slot 保存。
- `last_access_ns`、`clock_bit`：当前 allocator 不启用共享 LRU；若未来启用 eviction，应放到 owner-private eviction metadata 或独立 side metadata。
- `cold_state`：由 key metadata/COLD 事务状态管理，不塞入每个 slot 的稳定热路径 metadata。

`owner_generation` 负责 stale handle 检查；`key_hash + key_fingerprint` 负责 key identity 检查。两者不能互相替代。

## 5. Global slot allocator

### 5.1 逻辑地址空间

把所有 local region 按 region index 拼成一个逻辑 global slot array：

```text
global_slot [0, prefix[0])                    -> local region 0
global_slot [prefix[0], prefix[1])            -> local region 1
...
global_slot [prefix[n-1], local_capacity)     -> local region n-1
```

分配器内部使用 global slot；对外 location 仍返回：

```text
{ region_id, region_index, local_slot, offset, bytes, owner_generation }
```

global slot 转换为 region 的查找可以使用前缀数组和二分；region 数量很小且固定时也可以使用线性扫描。该转换不应参与 bitmap CAS 热循环。

### 5.2 分配顺序

```text
1. 从 process-local free bitmap 复用释放的 global slot。
2. bitmap 没有 free slot 时，CAS loop 推进 allocated_slot_index。
3. 两者都失败时返回 WARM_OOM。
```

`allocated_slot_index` 不能使用无边界 `fetch_add`。必须使用带上限的 CAS loop，保证并发线程不会把 index 推过 `local_capacity_slots` 后再回滚。

### 5.3 两层 summary bitmap

```text
level 0: 每个 bit 表示一个 global slot 是否 free
level 1: 每个 bit 表示对应 level-0 word 是否存在 free bit
```

推荐 bit 语义：`1 = free`，`0 = allocated/reserved`。

分配流程：

1. 线程从 thread-local word hint 开始读取 level 1。
2. 用 `ctz` 找到一个可能有 free bit 的 level-0 word。
3. 对 level-0 word 做 CAS，清除一个 free bit。
4. CAS 成功即取得 slot；如果 word 变为零，清除 level-1 对应 bit。
5. CAS 失败时重新加载该 word，并执行 bounded retry/换下一个 summary word。

释放流程：

1. 完成 owner generation/state 发布后，对 level-0 word 使用 `fetch_or` 设置 free bit。
2. 如果原 word 为零，再对 level 1 使用 `fetch_or` 设置 summary bit。
3. `free_count` 使用 relaxed atomic 统计，可用于观测和 OOM 快速判断，但不能作为正确性依据。

bitmap、summary、hint、bump pointer 和 free count 都位于 owner 进程普通内存中，不放入 payload region，也不作为跨进程持久化状态。重启或 HA 接管时扫描共享 slot metadata 重建 bitmap。

### 5.4 无锁边界

目标平台上应检查 `atomic_is_lock_free`：

- 64-bit bitmap word、summary word、bump pointer 在目标架构上必须是 lock-free atomic。
- 满足条件时 allocator 热路径不使用 mutex。
- 不满足条件时允许 owner-local mutex 作为可移植 fallback，但必须通过统计暴露，不能把 fallback 当成正常性能结果。
- CAS 失败重试中使用架构对应的 `pause/yield`，并设置 bounded retry，避免异常竞争下无限忙等。

## 6. 写、读和释放路径

### 6.1 新 key PUT

```text
1. proxy 校验 request shape、owner 和 topology epoch。
2. owner key metadata 做完整 key compare。
3. 如果是已有 local key，尝试原 slot overwrite。
4. 否则从 global allocator reserve local slot。
5. reserve 失败，立即返回 WARM_OOM。
6. 如果启用 COLD durability，再 append COLD；失败时释放 reservation。
7. 写 payload。
8. 写 key_hash/key_fingerprint/owner_generation。
9. 发布 READY 和 key -> location。
```

不能出现“COLD 已接受、WARM 已满、最终 PUT 仍返回成功”的状态。

### 6.2 overwrite

overwrite 必须使用完整 key metadata 找到旧 location，再通过：

```text
state_version stable READY
key_hash + key_fingerprint
owner_generation
```

三者共同校验。overwrite 不重新选择 region，不迁移到 remote region。

### 6.3 read

remote read 可以读取 owner 的 local region，但必须验证：

- region_id 和 local_slot 在 owner 发布的 descriptor 范围内；
- state_version 是稳定 READY；
- key_hash/key_fingerprint 与请求 identity 一致；
- owner_generation 与 handle 一致；
- payload 读取前后 seqlock/version 未变化。

### 6.4 DELETE 和 slot release

DELETE 先在 owner key metadata 写 tombstone/失效 location，再将 slot 从 READY 变为 FREE，并把 slot 放回 bitmap。旧 handle 因 owner_generation/state 校验失败而失效。remote 节点不能直接释放 slot。

## 7. 代码实现分阶段规划

### P0：接口和契约冻结

- 冻结 manifest 中 local/remote region 语义。
- 定义 `WARM_OOM`、owner 校验失败、stale handle 的错误码。
- 定义 layout v2 版本和 attach/reset 行为；由于代码尚未上线，不做旧 layout 兼容。
- 删除所有“remote region 可以作为写 fallback”的设计描述。

### P1：独立 allocator 模块

新增建议模块：

```text
src/tlc_warm_allocator.h
src/tlc_warm_allocator.c
```

职责：

- local region prefix table。
- two-level free bitmap。
- thread-local word hint。
- bounded bump CAS。
- reserve/release/rebuild/stats。
- lock-free capability 检查。

该模块不解析 key、不写 payload、不处理 COLD，保持 allocator 与 TLC 状态机边界清晰。

### P2：layout v2 和启动重建

修改：

- `src/vemb_v16_warm_region_layout.h/.c`：32B slot metadata、state/version 编码、layout version。
- `src/vemb_v16_storage.c`：按 local payload region 计算容量，owner 初始化 metadata，remote attach 只读。
- 启动时扫描 slot metadata：READY/FILLING/EVICTING 按恢复策略处理，最终重建 free bitmap。

扫描是启动路径，不进入 PUT/GET 热路径。

### P3：tlc_core 集成

修改 `src/tlc_core.c` 及对应 header：

- 删除 per-region `next_slot` 作为唯一 allocator 的假设。
- 新 key 使用 global allocator；overwrite 保持原 location。
- owner-only write gate。
- WARM reserve 在任何 COLD append 前执行。
- 删除 `max_vectors` 作为容量来源，使用派生 `local_capacity_slots`。
- 更新 handle 校验、generation 和 32B metadata snapshot。

### P4：remote metadata 和 migration

修改 `src/vemb_v16_remote_meta.{h,c}` 及 migration path：

- remote metadata 继续保存 `key_hash + key_fingerprint`。
- remote publish/update/read 都只允许 owner 写。
- migration target 只 reserve 自己 local region。
- source 在 SOURCE_GC 完成后 release；旧 location 必须因 generation/tombstone 失效。

### P5：配置、协议和工具

- 删除 server、Redis config、CLI、descriptor、bench 中的 `max_vectors`。
- manifest 成为容量的唯一外部输入。
- stats 增加 `local_capacity_slots`、`allocated_slots`、`free_slots`、`warm_oom`、每 region 的 `capacity/used/is_local`。
- 更新所有启动脚本和用户文档。

### P6：验证、调优和上线门槛

- 先运行 allocator 单测和内存模型测试。
- 再跑 TLC 单机容量/并发测试。
- 最后跑多 HPC-Redis owner/read、migration 和真实 payload 压测。
- 未达到满载率、正确性和性能门槛前，不切换默认实现。

## 8. 满载率测试设计

### 8.1 指标定义

逻辑 slot 利用率：

```text
logical_fill_ratio = (READY + FILLING + EVICTING) / local_capacity_slots
```

在停止写入并等待 in-flight 操作结束后，`FILLING/EVICTING` 应为零，此时：

```text
quiescent_fill_ratio = READY / local_capacity_slots
```

payload 物理可用率：

```text
payload_capacity_bytes =
    sum(floor(region_bytes / value_size) * value_size)

payload_fill_ratio_at_full =
    payload_capacity_bytes / sum(region_bytes for local regions)
```

每个 region 的尾部浪费最多 `value_size - 1` bytes，因此：

```text
1 - payload_fill_ratio_at_full
    < local_region_count * value_size / total_local_region_bytes
```

当每个 region bytes 是 `value_size` 的整数倍时，payload fill ratio 可以达到 100%。metadata mapping 不计入 payload bytes；如果 metadata 与 payload 共用物理 region，则必须把 metadata bytes 纳入分母并单独报告。

### 8.2 必测场景

1. **精确整除**：region bytes 是 `value_size` 的整数倍，写入正好 `local_capacity_slots` 个 key，第 `capacity+1` 个必须返回 `WARM_OOM`。
2. **非整除尾部**：多个 region 使用不同余数，验证实际容量等于各 `floor` 之和，不能多写一个 slot。
3. **local/remote 混合**：remote region 容量很大但 local 已满，仍必须返回 `WARM_OOM`，remote `used_slots` 保持不变。
4. **释放复用**：填满后删除 25%/50%/75% slot，再写入同等数量 key，验证 bitmap 复用，不增加 bump index。
5. **交错释放**：随机释放奇偶 slot和连续 slot，验证 summary bitmap 不丢 bit，最终可重新填满。
6. **并发填充**：多线程同时写入，停止后 `READY` 数量必须等于成功 PUT 数量，不能重复分配或丢失 slot。
7. **重启重建**：构造 READY/FREE 混合 metadata，重启/attach 后扫描重建 bitmap，重新填满到理论容量。
8. **forced hash collision**：测试 key_hash 相同但 fingerprint 不同的两个 key，不能 overwrite、remote metadata 不能合并成同一个 entry。

### 8.3 满载率验收

在无 eviction、无失败 append、无 in-flight 请求的静态填充场景：

- `quiescent_fill_ratio` 必须达到 `1.0`，或由于测试注入失败而明确小于 1.0 并报告失败原因。
- 第一个 `WARM_OOM` 出现时，`allocated_slots + free_slots == local_capacity_slots`。
- 不允许因为 region 选择、hash set、线程竞争而提前 OOM。
- 报告 payload tail waste、metadata process-memory bytes 和 bitmap bytes，不能只报告 key 数量。

## 9. 性能测试设计

### 9.1 Allocator microbenchmark

新增独立 benchmark，绕过网络、COLD 和 payload copy，只测 allocator：

| 场景 | 线程 | 操作 | 目的 |
|---|---:|---|---|
| bump-only | 1/2/4/8/16/32 | reserve | 测无竞争和边界 CAS 成本 |
| bitmap-only | 1/2/4/8/16/32 | reserve/release | 测复用热路径 |
| mixed | 1/2/4/8/16/32 | 80% reserve, 20% release | 模拟稳定 workload |
| hotspot | 1/2/4/8/16/32 | 同一组 bitmap word | 测 CAS 竞争 |
| full | 1/2/4/8/16/32 | reserve 至 OOM | 测满载边界 |
| rebuild | 1 | scan + rebuild | 测启动成本 |

记录：ops/s、p50/p95/p99 ns、CAS retry、summary scan、bump success、bitmap success、OOM 次数、false-sharing 事件（如可测）。

### 9.2 端到端 TLC benchmark

使用现有 TLC/Redis benchmark，在相同 `dim/value_size`、相同 worker 数和相同 local region bytes 下比较：

1. 当前 per-region bump/set allocator。
2. v2 allocator，只有 bump 发生时（无释放）。
3. v2 allocator，50% slot 释放后持续复用。
4. v2 allocator，多线程热点 region。
5. owner-only local write 与旧 remote fallback 的对照组。

读写比例至少包含 `100% PUT`、`80R/20W`、`50R/50W` 和 `100% GET`。读路径要分别报告 local read、remote read，避免把网络差异误归因于 allocator。

记录：吞吐、端到端 p50/p95/p99/p999、WARM reserve 时间、bitmap CAS retry、payload copy 时间、remote read 延迟、WARM_OOM 率、CPU cycles/op、RSS 和 metadata RSS。

### 9.3 性能门槛

具体绝对数字依赖 CPU、UB provider 和 `value_size`，因此先使用相对门槛：

- 无竞争 reserve p99 相对当前 bump-only 基线增加不超过 10%。
- 已释放 slot 的 bitmap reuse p99 不超过 bump-only 基线的 1.5 倍。
- 16 线程 mixed workload 的吞吐不低于单线程线性扩展目标的 70%；超过 16 线程后单独报告内存争用，不把平台瓶颈算作 allocator 回归。
- owner-only local write 相对旧跨节点 write fallback 的本地 CPU 路径不增加额外 RPC；远端写计数必须为零。
- 在 bitmap CAS retry 持续升高时，p99 必须可解释地随竞争上升，不能出现无界自旋。
- 启动 bitmap rebuild 的时间和 slot 数线性相关，并报告每百万 slot 的扫描耗时。

这些是验收门槛，不是尚未测量的性能承诺。实现后必须在目标 Linux/UB 机器上运行至少三轮，报告中位数和离散度。

## 10. 可观测性

建议新增或统一以下 counters：

```text
warm_local_capacity_slots
warm_allocated_slots
warm_free_slots
warm_alloc_bump_success
warm_alloc_bitmap_success
warm_alloc_bitmap_cas_retry
warm_alloc_summary_scan
warm_alloc_oom
warm_release_count
warm_rebuild_slots
warm_remote_write_reject
warm_stale_handle_reject
```

每个 region 至少输出：

```text
region_id, region_index, is_local, capacity_slots,
ready_slots, free_slots, allocated_slot_index, full
```

统计采样必须使用 monotonic clock，不能在每次 allocator 操作中无条件取时间戳。

## 11. 风险和未决事项

1. 32B metadata 会让相邻 slot 共用一个 cacheline；需要用 CAS contention benchmark 判断是否需要对高竞争 region 使用可选 padding。
2. free bitmap 是 process-local volatile state，HA 接管必须可靠扫描 slot metadata；扫描期间不能允许写入。
3. 如果未来重新启用共享 LRU/eviction，`last_access`、`clock_bit` 和 COLD 状态不能直接塞回 32B layout，应另设 owner-private metadata。
4. remote metadata 仍需保留 `key_hash + key_fingerprint`，不能因为 owner-only write 就删掉 fingerprint。
5. region bytes 非整除 `value_size` 的尾部浪费应通过 manifest sizing 规避，而不是在 allocator 内部向上越界。
6. 老代码和旧文档中仍存在大量 `max_vectors` 示例；代码切换前必须完成全仓库审计，避免 CLI、protocol 和 benchmark 语义不一致。

## 12. 推荐落地顺序

```text
先冻结 manifest/ownership/error contract
  -> 独立实现 allocator + 单测
  -> layout v2 + 启动 rebuild
  -> tlc_core local-only write 集成
  -> 删除 max_vectors 并更新协议/脚本
  -> remote metadata/migration 接入
  -> 满载率测试
  -> microbenchmark
  -> 端到端性能和真实 UB 验证
```

在 P6 验证完成前，不建议把 v2 allocator 作为默认生产路径。

## 13. 当前实现与实测结果

本轮已经加入：

- `src/tlc_warm_allocator.{h,c}`：两层 bitmap、summary CAS、thread-local hint、bounded bump CAS、release/rebuild 和 lock-free capability 检查。
- `tlc_core`：local region 容量派生、global local slot 映射、local-only PUT、DELETE slot release、WARM OOM 计数和容量统计。
- `benchmark/tlc_warm_allocator_ut.c`：满载、释放复用和 8 线程唯一 slot 测试。
- `benchmark/tlc_warm_capacity_ut.c`：两个 local region 加一个大 remote region 的容量/remote-write 隔离测试。
- `benchmark/tlc_warm_allocator_bench.c`：reserve/release microbenchmark。

### 13.1 正确性与满载率

本地和目标 aarch64 均通过：

- `tlc_warm_allocator_ut`：首次 bump、全量 bitmap reuse、8 线程唯一分配、跨 3 个 summary word 的 9000-slot rebuild、未分配/repeated release 拒绝。
- `tlc_warm_capacity_ut`：两个 local region 的容量为 `2 + 3 = 5`，另一个 8-slot remote region 不计入写容量；前 5 个 key 成功，第 6 个返回 `WARM_OOM`，删除后可复用，重启 rebuild 后边界不变。
- `tlc_sync_ut`、`tlc_resync_ut`、`vemb_v16_batch_close_drain_ut`、`vemb_v16_proxy_topology_transport_ut` 全部通过。
- 目标 aarch64 的 `vemb_v16_migration_control_ut` 全部通过。

静态整除场景的结果：

```text
local_capacity_slots = 5
READY slots before OOM = 5
quiescent_fill_ratio = 5 / 5 = 1.0
payload_fill_ratio_at_full = 100%
remote writes = 0
```

真实 UB 使用 `VEMB_V16_VSIM_UB_CC_NC_VISIBILITY_REPRO.md` 第 2 节记录的映射：

```text
local WARM: 111 /dev/obmm_shmdev12 (CC)
remote read: 112 /dev/obmm_shmdev16 (NC)
reverse untouched sentinel: 112 /dev/obmm_shmdev12 (CC)
                         -> 111 /dev/obmm_shmdev16 (NC)
```

111 上配置一个 64 KiB local region 和一个 64 KiB remote region，`value_size=64`，派生 local 容量为 1024。实测结果：

- 连续写入 1024 个 key 成功，`full=1`；第 1025 个在 item 1024 返回 `WARM_OOM`，满载率 `1024/1024 = 100%`。
- 112 通过 imported NC `dev16` 直接 mmap，读到 `magic=0x5631414c`、layout version 2、region 1700、capacity 1024、metadata size 32、稳定 READY、正确 payload 和 `key_fingerprint`。
- 删除 slot 0 后 `full=0`；写入新 key 后 bitmap 复用成功，`full=1`、`alloc_local=1`、`alloc_remote=0`，slot generation 从 1 推进到 2。
- 不 reset 重启 owner 后，新写仍返回 OOM，跨节点 mmap 仍读到相同 READY slot，证明启动扫描重建了满载 allocator。
- 反向 remote region 的 64B 哨兵在填充、OOM、删除复用和重启后保持不变，证明 owner PUT 未写 remote region。

### 13.2 Allocator microbenchmark

benchmark 是同一 allocator 上立即 reserve/release 的热点 bitmap 场景，每个线程 500,000 次循环，结果采用 3 轮中位数；`ops/s` 沿用 benchmark 输出口径，每次计数包含一对 reserve/release。

| 线程数 | macOS 开发机中位数 | aarch64 目标机中位数 | lock-free |
|---:|---:|---:|---:|
| 1 | 115.8M ops/s | 11.55M ops/s | yes |
| 2 | 6.87M ops/s | 2.80M ops/s | yes |
| 4 | 7.46M ops/s | 1.76M ops/s | yes |
| 8 | 3.06M ops/s | 1.40M ops/s | yes |
| 16 | - | 1.51M ops/s | yes |
| 32 | - | 1.71M ops/s | yes |

aarch64 三轮原始 `ops/s`：

| 线程数 | round 1 | round 2 | round 3 |
|---:|---:|---:|---:|
| 1 | 11,545,330 | 11,476,530 | 11,579,384 |
| 2 | 2,680,654 | 3,009,267 | 2,801,317 |
| 4 | 2,070,457 | 1,746,579 | 1,761,095 |
| 8 | 1,391,100 | 1,490,736 | 1,396,906 |
| 16 | 1,289,208 | 1,692,729 | 1,514,548 |
| 32 | 1,736,621 | 1,233,237 | 1,710,787 |

该 workload 会让所有线程反复争抢同一批 bitmap word，2 线程起吞吐明显下降，确认当前主要瓶颈是热点 CAS/cacheline 竞争，而不是 mutex fallback。该结论只适用于立即 reserve/release 的极端回收路径，不能外推为端到端 VADD 吞吐。

### 13.3 生产语义 workload 对照

为区分 bump cursor 和 free bitmap 的成本，`tlc_warm_allocator_bench` 增加两种固定总工作量模式：

- `fill`：全部线程合计分配 `capacity` 个 slot，不释放；结束后检查 `free=0`、`full=1`，并额外确认下一次分配返回 FULL。
- `mixed-80w20d`：每周期严格执行 4 次分配和 1 次释放，删除本周期第一个已分配 slot。测试执行 `capacity/4` 个周期，最终 live slot 为 `3 * capacity/4`。

目标 aarch64 上使用 `capacity=1,048,576`，每组 3 轮，以下为中位数。这里的 `ops/s` 是 allocator API call 数；一次释放也计一个 op：

| 线程数 | 纯新增写满 | 80% 分配 + 20% 释放 |
|---:|---:|---:|
| 1 | 78.55M ops/s | 40.39M ops/s |
| 2 | 23.45M ops/s | 7.98M ops/s |
| 4 | 18.74M ops/s | 4.33M ops/s |
| 8 | 20.43M ops/s | 3.38M ops/s |
| 16 | 16.65M ops/s | 2.89M ops/s |
| 32 | 15.67M ops/s | 2.81M ops/s |

所有轮次均为 `lock_free=1`、`valid=1`。这组结果补充证明：不仅 bitmap word 会形成热点，纯新增路径的单个 `allocated_slot_index` CAS cacheline 同样无法随线程数扩展。bitmap 路径下降更严重，仍应优先增加按 worker 分散 free word 的对照；bump 路径则应评估按 worker 批量领取连续 slot range，但批量缓存必须可回收，不能降低最终满载率。

端到端验证使用逻辑 112 的本地 CC `/dev/obmm_shmdev12` 和 remote NC `/dev/obmm_shmdev16`，限制在此前验证过的 64 KiB 范围。配置 `value_size=64`、派生 local capacity 1024、8 个 proxy I/O worker 和 8 个 supernode worker；client 每线程持有独立 channel。`vemb_v16_bench` 新增 `mixed-80w20d`，每线程独占 key 区间，每 5 个请求发送 4 个唯一 key VADD，再 VREM 该周期第一个已写入 key。

| client 线程数 | 纯新增写满 QPS 中位数 | 80% VADD + 20% VREM QPS 中位数 |
|---:|---:|---:|
| 1 | 46,192 | 47,334 |
| 2 | 80,541 | 78,631 |
| 4 | 119,356 | 116,254 |
| 8 | 133,091 | 149,716 |

为获得旧 placement 设计的同机对照，使用 `cfd2cf4` 的 clean source 构建旧
server，并继续使用同一个当前版 `vemb_v16_bench` client workload。每轮开始前，
分别在逻辑 112 的 local CC `/dev/obmm_shmdev12` 和逻辑 111 的 local CC
`/dev/obmm_shmdev12` 重置 V1 metadata；后者在逻辑 112 上对应 remote NC
`/dev/obmm_shmdev16`。其他参数保持为 `value_size=64`、每 region 1024 slots、
PIO/SNW=8/8，每档 3 轮取中位数。

| client 线程数 | 旧版纯新增尝试 QPS | 当前纯新增 QPS | 旧版 80/20 QPS | 当前 80/20 QPS |
|---:|---:|---:|---:|---:|
| 1 | 40,440 | 46,192 | 45,099 | 47,334 |
| 2 | 69,124 | 80,541 | 72,595 | 78,631 |
| 4 | 119,290 | 119,356 | 116,599 | 116,254 |
| 8 | 131,618 | 133,091 | 147,290 | 149,716 |

旧版纯新增列是 1024 次请求的尝试 QPS，不能解释为成功写满吞吐。每一轮都只有
1022 次成功、2 次失败，其中 `alloc_local=872`、`alloc_remote=150`、
`fallback=150`。因此 local region 只使用 `872/1024 = 85.16%` 就开始依赖
remote region，两个相同 4-way set 都满时仍会在总空闲 slot 很多的情况下失败。
按成功请求折算，旧版纯新增成功 QPS 分别为 40,361、68,989、119,057 和
131,361；当前版本同时达到 1024/1024 成功、local 100% 满载和
`alloc_remote=0`。

旧版 80/20 每轮 1000 个请求均成功，计数为 800 VADD、200 VREM，最终 V1
metadata 扫描结果是 local `READY=740/FREE=284`、remote
`READY=60/FREE=964`。即 600 个逻辑 live key 仍占用 800 个物理 slot：旧
`VREM` 只发布 tombstone 和失效 location，不释放 slot。当前版本同 workload
结束时为 local `READY=600/FREE=424`、remote allocation 为 0。

吞吐上，当前版本相对旧版在 1/2 client 线程分别提升：纯新增 `14.22%/16.52%`，
80/20 `4.96%/8.32%`。4/8 线程两者接近，其中 4 线程 80/20 的 `-0.30%`
属于三轮短样本波动范围；这里更确定的收益是满载率、owner-local 写语义和删除
回收正确性，而不是高并发 QPS 的数量级变化。

旧设计没有独立的 `tlc_warm_allocator_alloc/release` API，因此不能把同一个
allocator microbenchmark 链接到旧代码。人为移植旧 placement 逻辑会变成模型
测试，不等同于旧生产路径；上述对照选择实际 supernode/TLC/UB 端到端路径。

当前版本端到端每档 3 轮均通过：

- 纯新增每轮 1024 VADD 全部成功，metadata 为 `READY=1024, FREE=0`；额外唯一 key 返回失败，server 记录一次 WARM allocation failure；`alloc_local=1024, alloc_remote=0`。
- 80/20 每轮严格为 800 VADD、200 VREM，metadata 为 `READY=600, FREE=424, FILLING=0, EVICTING=0`；`alloc_remote=0`。
- 1 到 8 client channel 的端到端吞吐保持上升，没有复现 allocator microbenchmark 从 2 线程开始的整体吞吐下降。

端到端样本只有 1024 个 local slot，且 common-core client 是同步请求模式，因此该 QPS 只用于验证真实 supernode/TLC/UB 路径和扩展方向，不作为最大吞吐结论。正式容量下的性能结论仍需使用 memtier 长时间 workload，并保持相同的 4:1 VADD/VREM key 生命周期。

### 13.4 任意 slot 分配后的 lookup 索引契约

100k key 的 Aeron 集成回归暴露了一个 allocator 与旧 lookup 的契约错位：v2
allocator 按 global bump/bitmap 分配任意 local slot，但旧
`warm_lookup_region()` 仍只探测 `key_hash` 对应的 8-way set。写入时的
process-local location cache 能覆盖大部分 key；发生 cache 冲突淘汰后，fallback
无法找到不在旧 set 内的合法 slot。实测表现为 100k key prefill 全部成功，但读
workload 固定有 9,217 个 key 反复返回 NOTFOUND。

修正后的索引层次是：

```text
location cache (无锁热路径)
    -> owner key_meta shard (完整 key -> authoritative location)
    -> generation/hash/fingerprint/state 校验
    -> legacy warm/remote scan fallback
```

`key_meta` 已由 PUT/DELETE/migration 在 shard lock 下维护，不需要再增加第二套
process-local hash index。cache miss 时只在目标 shard 内复制 location，释放锁后
复用已有 slot 校验；cache hit 和 remote payload read 路径不增加锁。新增容量回归
将 location cache 压到 1,024 entries，顺序写入 4,096 个由 bump allocator 任意
放置的 key，再逐一 lookup；修正前稳定失败，修正后 4,096/4,096 全部成功。

### 13.5 CC/NC 可见性与最终 Aeron 回归

使用 `benchmark/ub_cc_nc_visibility_ut.c` 对第 2 节设备映射做独立可见性验证，
避免只依赖 Aeron workload 间接判断：

- request 方向：112 `dev1` CC 写入，111 `dev5` NC 读取，第一次采样即为
  `VISIBLE`。
- response 方向：111 `dev6` NC 写入，112 `dev2` CC 读取，第一次采样即为
  `VISIBLE`。
- 4 KiB frame 连续验证 100 generations；reader 输出
  `NOT_REPRODUCED iterations=100`，writer 输出
  `FRAME_WRITER_COMPLETE iterations=100`，未观察到 mixed/stale frame。

在此基础上执行 `scripts/run_aeron_cross_node_flamegraph.sh` 最终回归，run ID 为
`warm_allocator_v2_aeron_regression_20260917_214500`：

- prefill 100,000/100,000 成功，QPS 214,603.33。
- 30 秒读取完成 15,416,992 logical ops，QPS 504,242.31；
  `status_notfound=0`、`status_err=0`、`materialized_fail=0`、`unmatched=0`。
- 平均延迟 15.1338 ms，p50 11.967 ms，p99 32.127 ms。
- Redis process CPU 15.415 cores，CPU-set total 15.493 cores；server RSS/peak
  868,172 KiB，client RSS 594,164 KiB。
- server/client perf.data、collapsed stacks 和 flamegraph SVG 均已生成。

这组结果同时覆盖 UB CC/NC 原始可见性、100k key 任意 slot lookup，以及正式
Aeron 请求/响应路径。可见性专项测试没有复现 stale/mixed frame，最终 workload
也没有出现修正前的固定 NOTFOUND。

### 13.6 已知集成阻塞

- `vemb_v16_tlc_ut` 在覆盖完 checkpoint/AOF、local-only OOM、remote read/meta 等前置场景后，停在测试未创建配对 UB RPC peer rings；断点是 `vemb_v16_ub_rpc_create(...)`，不是 allocator 断言。
- `vemb_v16_manifest_ut` 停在历史用例 `test_peer_view_map_can_attach_local_region`：生产路径只允许动态 attach UB，而旧用例要求动态 attach `LOCAL_SHM`。该限制在本次改动前已存在。
- 目标机完整 `redis-server` rebuild 曾被 `output/src/vemb_v16_server_integration.d` 中残留的历史 `../ubme/ubme_seam.h` 依赖阻塞。当前源码不包含该头文件；这是旧 `.d/.o` 构建缓存，不是生产源码的外部依赖。
- macOS 上 `vemb_v16_migration_control_ut` 的 topology handler 线程受既有小栈/`___chkstk_darwin` 问题影响；同一用例在目标 aarch64 全部通过。
