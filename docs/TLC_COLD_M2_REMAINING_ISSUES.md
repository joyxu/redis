# M2 COLD 层后续问题清单

本文记录 M2 单机 COLD 接入已经完成后仍需解决的问题，不重新定义架构。
设计基准见：

- [TLC 2xN HA Architecture](./TLC_2X3_HA_ARCHITECTURE.md)
- [TLC COLD Implementation Plan](./TLC_COLD_IMPLEMENTATION_PLAN.md)

## 1. 当前结论

M2 已具备以下能力：

- 单机 PUT/DEL 进入持久 COLD AOF。
- COLD runtime 支持本地 group commit/fsync 和 `durable_seq` 推进。
- COLD 启用时可从 AOF replay WARM metadata 和数据。
- WARM publish 失败时保留 AOF，并打印 `WARNING`。
- 普通 GET/VEMB/VSIM 读取路径不访问磁盘 COLD。

但 M2 还不能视为严格完成。主要缺口集中在 WARM 可见性、失败重试、崩溃边界、
完整状态恢复和故障注入验收。

## 2. 待解决问题

### M2-01：WARM staging 尚未真正实现

当前写路径大致为：

```text
COLD append/accepted
    -> warm_put / warm_overwrite
    -> metadata commit
后台 group commit/fsync -> durable_seq
```

`warm_put()` 会直接把 slot 发布为 `READY`。普通读取在没有 tombstone/source-fence
时会直接走 location cache、hot cache 或 WARM slot lookup，并不要求 key-meta 已经
存在；因此 metadata commit 完成前确实可能看到该 slot。这不是 COLD 磁盘访问，
但违反了“WARM staging 暂不可见”的设计要求。若上层调用链先查 key-meta，则不会
暴露；风险来自当前低层 raw/stable-read 接口允许绕过该检查直接查 WARM。

后续需要：

- 增加明确的 `STAGING/FILLING` 可见性状态。
- 只有 metadata commit 成功后才发布 `READY`。
- publish 失败时正确处理 staging slot，不留下可见孤儿数据。
- 不在普通读路径增加 COLD 锁、磁盘 I/O 或 durable 等待。

相关代码：[tlc_core.c](../src/tlc_core.c)。

### M2-02：缺少进程内自动重试编排

当前 WARM publish 失败后，AOF event 会保留，并提供显式的
`tlc_core_recover_cold()` / `vemb_v16_tlc_recover_cold()` replay 接口，但没有：

- pending event queue；
- 后台重试 worker；
- 重试次数和退避策略；
- 重试成功/失败指标；
- 节点关闭时的 pending event 状态管理。

因此同一进程内只能依赖外部显式触发恢复，不能自动完成 durable event 到 WARM 的
最终收敛。重试机制不能无限循环，也不能放到读路径执行。

### M2-03：AOF replay 只恢复普通 PUT/DEL

当前 replay 主要恢复 key、value、version、term 和 tombstone。以下状态尚未纳入
单机 COLD 恢复契约：

- owner epoch；
- migration state；
- source/target owner；
- route version；
- 完整 HA state。

这对单机普通写入足够，但不能恢复完整迁移状态。Follower/HA 实现前必须冻结
event 格式和状态恢复边界，避免 M6 再次修改 AOF 兼容格式。

### M2-04：恢复按 checkpoint 增量 replay（已完成）

当前已提供 checkpoint 起点过滤，按各 metadata shard 的 `captured_seq` replay AOF
增量；没有有效 checkpoint 时才回退到全量 replay。

目标流程应为：

```text
选择有效 checkpoint generation
    -> 读取每个 key-shard 的 captured_seq
    -> 只 replay captured_seq 之后的 AOF
```

该项由 M3/M4 主路径完成，不再作为遗留问题。

### M2-05：durable_seq 与重启恢复边界（已确定，不再是待实现项）

运行期间，`durable_seq` 是严格的本地 durable 保证边界：只有 group commit 的
fsync 成功后才推进。`durable_seq` 之前的 event 必须在重启后恢复；
`durable_seq` 之后但已 append 的 event 属于 accepted 状态，重启后可以尽力恢复，
但不提供必然保留保证。

进程崩溃前可能出现：

```text
write 已完成
fsync 尚未完成
进程崩溃
```

当前不引入 durable marker。启动扫描对 framing/checksum 完整的 entry 执行机会性
恢复；扫描得到的完整 entry 不等同于崩溃前已获得 `LOCAL_DURABLE_ACK`。因此，
运行期间的 `durable_seq` 是严格保证边界，重启后的完整 AOF 前缀是机会性恢复边界。

### M2-06：写路径不应在 key-meta shard lock 内等待 fsync（设计已确定，待落地）

当前实现仍在持有 key-meta shard lock 时等待 COLD durable ACK。读路径不受影响，
但同一 metadata shard 的写请求会被磁盘延迟串行化，可能造成：

- 写尾延迟升高；
- group commit 延迟传播到更多写请求；
- fsync 故障时锁持有时间过长。

按本次决策，写路径不再在该锁内等待 durable ACK；先完成 AOF append/接受，
由后台 flush coordinator 按 pending event 数量（或字节数）达到阈值，或等待超时，
统一执行 fsync 并推进 `durable_seq`。如仍保留 `LOCAL_DURABLE_ACK` 接口，必须明确
它是显式等待接口，而不是普通写路径的持锁步骤；版本检查和并发写顺序仍需保持正确。

### M2-07：hash 策略（已确认不作为遗留问题）

恢复时根据 key 重新计算 hash。当前 hash 算法、seed 和实现属于固定策略，预期不会
变化，也不单独引入 hash algorithm/version 字段。

因此不再把 `key_hash` 持久化列为 M2 遗留项；变更 hash 策略时另行升级存储格式并
处理兼容性。

### M2-08：DEL 后旧 WARM slot/旧 handle 访问（协议已确定，测试待补）

DEL 会发布 tombstone，但旧 WARM payload 和 slot 可能暂时保留。标准 key 查询会
通过 tombstone filter 拦截；已经发出的旧 handle 是否仍可访问由调用方自行校验
location generation/写序列，server 不为旧 handle 提供额外防护，也不保证旧 payload
自动失效。

后续需要：

- 在调用方接口文档中明确 stale handle 必须自行校验；
- 增加 DEL 与并发 handle read 的调用方测试。

### M2-09：生产配置仍是环境变量临时注入

当前生产启动通过：

```text
HPC_REDIS_COLD_DIR
HPC_REDIS_KEY_SHARD_COUNT
```

该方式可以启动单机 COLD，但配置没有进入正式 manifest/config 契约。metadata
shard 数量属于当前编译期固定策略，变更时仍需显式阻止不兼容的已有 AOF replay。
后续需要将以下内容纳入正式配置和版本校验：

- COLD 目录及权限；
- key-shard 数量；
- segment 和 group commit 参数；
- AOF/checkpoint 兼容版本。

### M2-10：故障注入和性能验收尾项

当前测试覆盖正常 append、group commit、尾部截断、非末尾损坏和基础重启恢复，
但还缺少：

- WARM publish 失败、metadata allocation 失败、replay 中途失败和 replay 后重试成功；
- 真实进程级崩溃发生在 write/fsync/metadata commit 各阶段；
- 真实 ENOSPC/硬件 I/O 环境；
- key-meta lock 对写尾延迟的影响；
- COLD 启用前后的 GET/VEMB/VSIM 热点基准对比。

### M2-11：WARM 内存布局和 COLD queue 性能优化（TODO）

后续性能优化登记：

- 将 WARM 层内存分配改为连续数组，并使用 freelist 管理 tombstone 释放的 slot，
  提高 WARM 层内存满载率；
- 将当前 `pthread mutex + cond` 有界 queue 评估替换为文档中提及的 lock-free
  queue（例如 `blockingconcurrentqueue.h` 加固定容量 token/semaphore 管理），
  保持严格容量上限、背压、event 所有权和 shutdown 语义。

两项均未开始，须先以基准证明现有实现是瓶颈，再进行实现和故障注入回归；不得
改变普通读路径的同步和 I/O 约束。

### M2-12：COLD 持久化健康状态机和写入保护（设计已确定，待实现）

后续增加最小状态机：

```text
HEALTHY -> WRITE_BLOCKED -> RECOVERING -> HEALTHY
                         \-> STOPPING（关闭路径）
```

- append/fsync 错误或 queue/pending 内存预算超限时进入 `WRITE_BLOCKED`；
- `WRITE_BLOCKED` 拒绝新的持久化写入，但普通读继续服务；
- 后台探测 append/fsync 恢复后进入 `RECOVERING`，连续成功且队列低于低水位后恢复
  `HEALTHY`；
- `DEGRADED` 先作为 fsync 延迟和 durable lag 指标，不直接阻断写入；
- `TLC_COLD_ACK_ACCEPTED` 已返回的 event 不撤回，`durable_seq` 仍只在 fsync 成功后
  更新；
- 关闭时必须唤醒所有 waiter、排空 queue/pending，并尝试最终 fsync。

该状态机需要原子转换、失败路径 waiter 唤醒、I/O 恢复后的 `durable_seq` 校正，
以及写入保护状态的监控指标。代码未实现，当前 `io_error` 仅提供错误熔断基础。

## 3. 建议解决顺序

```text
M2-01 WARM staging 可见性
    -> M2-02 进程内重试和收敛
    -> M2-03 event/HA 状态契约
    -> M2-06 写锁外后台 fsync 落地
    -> M2-08 stale handle 调用方测试
    -> M2-09 正式配置
    -> M2-10 故障注入与性能验收
```

M3/M4 的 checkpoint 和 recovery 设计应复用本清单中的 event 版本、durable 边界
和重试语义，不能另行定义互相冲突的恢复规则。

## 4. M3/M4/M5 项目登记与验收状态

## 4.0 当前状态总览（不含 Follower/HA）

以下汇总基础验收结果和仍未完成的尾项：

| 项目 | 当前状态 | 说明 |
|---|---|---|
| Checkpoint 发布故障注入 | 已完成基础验收 | 测试构建专用 failpoint 已覆盖文件/manifest 的 write、fsync、rename 和目录 fsync；生产构建不包含注入状态 |
| 并发 fuzzy checkpoint | 已完成基础验收 | 多 writer 与 checkpoint 并发、重启后逐条比较 metadata/WARM value 已通过 |
| 完整恢复一致性 | 基础主路径已完成 | 已覆盖 checkpoint 后新增、更新、删除、WARM 丢失，并逐条比较重启前后的最终数据；损坏/gap/尾部异常仍归入严格异常边界 |
| 严格恢复异常边界 | 单机基础验收完成 | COLD 层损坏/gap、旧 term、state 应用失败及单节点启动阻断已通过；HA 场景需由另一副本接管 |
| generation 自动回退 | 待完成 | active generation 损坏时选择旧 generation，并清理 `.tmp`/orphan generation |
| compact 崩溃故障注入 | 已完成基础验收 | 测试构建 failpoint 覆盖删除 AOF segment、删除旧 generation、目录 fsync 前中断；每个场景均完成重启恢复校验；真实进程级 `kill -9` 仍可后续补充 |
| 磁盘/I/O 异常集成验收 | 已完成基础验收 | 测试构建 failpoint 覆盖 AOF append 和 group-commit fsync 失败；durable ACK/`durable_seq` 边界、后续写入熔断、告警和重启扫描均通过；真实 ENOSPC/硬件 I/O 环境仍可后续补充 |

上述项目不包含 Follower COLD、复制 cursor、`ha_safe_point_seq` 推进和 Node group
切换；这些属于最后的 HA 阶段。

### M5-01：checkpoint generation 保留和回退（部分实现）

- 维护 active generation 与 retained generation 集合。
- 最新 generation 或 manifest 校验失败时，按 generation 从新到旧选择可用版本。
- 至少保留一个可恢复旧版本；manifest 发布失败不得删除旧版本。
- 明确 `.tmp` 和 orphan generation 的启动清理策略。

该项不作为当前 M4 的完成条件。已实现按 retention count 清理旧 checkpoint 文件，
但最新 generation 损坏时自动选择旧 generation、active/retained catalog 和
启动清理策略仍待完成。AOF compact 后必须同时保留最旧 retained generation 之后
所需的 AOF 增量。

### M3-02：checkpoint 发布故障注入

- 覆盖 checkpoint write/fsync/rename、目录 fsync、manifest write/fsync/rename。
- 每个失败点都必须保留旧 manifest 和旧的有效 generation。

已完成基础故障注入验收。`tlc_cold_set_checkpoint_failpoint()` 及其枚举只在
`TLC_COLD_ENABLE_FAILPOINT` 测试构建中导出；生产构建不分配 failpoint 状态，注入
宏展开为常量 0，不增加正常 checkpoint 路径的运行时状态或分支。测试覆盖 checkpoint
文件/manifest 的 write、fsync、rename 和目录 fsync 失败。每个失败点都验证旧 generation
仍可校验；文件 rename 或 manifest rename 已成功但后续目录 fsync 失败时，系统仍只会
看到完整的新文件/manifest，不会看到半写入内容。

### M3-03：并发 fuzzy checkpoint 验收

- checkpoint 与多 worker 写入并发运行。
- 每个 metadata shard 独立捕获 `captured_seq`。
- 校验 checkpoint state 与后续 AOF 增量 replay 不丢失、不重复。

已完成生产写路径验收：`benchmark/vemb_v16_tlc_ut` 新增多个 writer 并发执行
`vemb_v16_tlc_put_with_epoch()`，在 writer 进行中调用
`vemb_v16_tlc_publish_checkpoint()`，随后销毁并重新创建 TLC 实例。测试逐条读取
恢复后的 metadata 和 WARM value，与原始 worker/key/value 集合比较，验证 fuzzy
checkpoint state 和 `captured_seq` 之后 AOF replay 的结果一致。该测试覆盖的是
实际 WARM checkpoint 序列化/恢复路径，不是单纯的 COLD 文件回调测试。

### M4-01：checkpoint reader 与完整校验（已实现）

- 读取 manifest 和 generation 文件。
- 校验 header、metadata shard 数量/顺序、record 边界、state checksum 和 generation checksum。
- 只接受完整且 checksum 一致的 generation。

已提供 `tlc_cold_validate_checkpoint()` 和 `tlc_cold_load_checkpoint()`，并在
TLC 恢复入口中使用。

### M4-02：checkpoint + AOF 增量恢复（已实现基础闭环）

- 先加载有效 generation 的 metadata/WARM state。
- 对每个 metadata shard 只 replay `captured_seq` 之后的 AOF event。
- 无有效 checkpoint 时回退到完整 AOF replay。

已实现 metadata shard state 加载和 `tlc_cold_replay_after()`；正常路径验证了
checkpoint 后新增 PUT/DEL 均可通过 AOF 增量恢复。manifest 指向的 generation
无法加载时目前回退到完整 AOF replay，旧 generation 自动选择仍待完成。

### M4-03：严格恢复异常边界（单机 COLD 验收完成）

- AOF 尾部半条 entry 可以截断。
- 中间损坏、seq gap、metadata shard 不匹配、checksum 冲突必须停止恢复。
- 恢复失败必须阻断 storage/Proxy/Node 启动，不能进入可服务状态。

当前代码和 `benchmark/tlc_cold_ut` 已覆盖 checkpoint generation 损坏、AOF checksum
损坏/中间损坏、有效 checksum 下的 seq gap、metadata shard 越界，以及尾部半条 entry
的停止/截断边界。测试结果确认：只有最后一个 segment 的文件尾部允许截断，其余异常
均返回失败并输出 `WARNING`，不会继续 replay 错误数据。

旧 term/version 冲突已在恢复 replay 中明确处理：低于当前 metadata topology epoch 的
event 输出 `WARNING` 并停止恢复，旧版本 event 仍按幂等规则忽略。checkpoint checksum
正确但 state 应用失败（例如恢复 WARM 容量不足）时，测试确认恢复失败、COLD 句柄清理，
重复恢复不会进入服务路径。

恢复失败必须沿 `vemb_v16_storage_ctx_create_from_manifest()` 向上返回，阻断
Proxy/Node 启动，不得进入可服务状态。单节点因此不可用；双副本 HA 由接入层
切换到另一健康 Node，故障 Node 只在隔离状态下重试恢复/同步。

`benchmark/vemb_v16_tlc_ut` 已增加单节点启动失败测试：损坏 AOF、旧 term 或 state
应用失败时 `vemb_v16_tlc_enable_cold()` 返回失败，测试实例随即销毁，不进入服务路径。

该项目不阻塞当前 M4 正常恢复主路径。

### M4-04：checkpoint + AOF 集成验收（基础闭环已完成）

已完成基础完整一致性测试：

```text
checkpoint
    -> 后续 PUT/DEL AOF
    -> 重启
    -> 加载 checkpoint
    -> replay captured_seq 之后的 AOF
    -> 验证恢复数据与崩溃前一致
```

测试覆盖 checkpoint 后新增数据、更新、删除和 WARM 丢失，并在重启后逐条比较最终
metadata 与 WARM value。AOF 中间损坏、seq gap、尾部半条 entry 属于严格异常边界
验收，仍按 M4-03 单独补充。

### M5-02：compact 与保留边界（基础实现）

- 仅在有效 checkpoint 和 HA safe point 允许的边界内回收 AOF/generation。
- compact 中途崩溃后仍可使用旧 manifest 和旧 generation 恢复。

已实现 `tlc_cold_compact()` 的单机基础路径：按
`min(checkpoint_floor_seq, retained_generation_floor_seq, ha_safe_point_seq)` 回收
sealed AOF segment，并处理
segment ID 前缀空洞；compact 前会完整校验 active generation，边界落在 segment
中间时保留该 segment，并预检 retention 集合，以最旧保留 generation 的
`checkpoint_seq` 限制 AOF 回收边界。compact 崩溃故障注入、generation 自动回退和
HA safe point 接入仍待完成。测试构建专用 compact failpoint 已覆盖三类中断点，
并验证中断后关闭、重启仍可加载 active checkpoint 且只 replay checkpoint 之后的
AOF event；generation 自动回退和真实进程级 `kill -9` 仍待完成。

`benchmark/tlc_cold_ut` 已覆盖：checkpoint floor 超过 manifest 时拒绝 compact、
safe point 收紧回收边界、active segment 保留，以及 compact 后重启加载 checkpoint
并 replay `captured_seq` 之后的 AOF。

测试环境边界：`benchmark/vemb_v16_tlc_ut` 的 fuzzy checkpoint 测试不依赖 UB；但同一
测试程序中的既有 UB RPC ring 用例必须在支持 Ascend UB 和远程 peer ring 的机器上运行。
本机出现 `failed to open vemb_v16 ub rpc peer rings` 只表示测试环境不满足该用例前提。
