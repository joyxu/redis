# TLC COLD 层落地计划与进度跟踪

本文用于跟踪 HPC-Redis COLD 层的工程落地，不重新定义架构协议。
架构和行为基准见：

- [TLC 2xN HA Architecture](./TLC_2X3_HA_ARCHITECTURE.md)

## 1. 目标和范围

先完成**单个 HPC-Redis Node 的 COLD 完整闭环**，再实现 Follower 异步复制。
COLD 不参与普通读和在线计算，只负责本地持久化、故障恢复、AOF 复制来源和
备份容错。Follower 不作为单机 COLD 的前置依赖。

读热点保护是硬约束：COLD 的 queue、writer、flush、checkpoint 和 recovery
线程不得出现在 `GET/VEMB/VSIM` 的调用链中；读路径不得获取 COLD mutex、执行
磁盘 I/O 或等待 `durable_seq`。COLD 进度只通过后台指标和恢复接口观察，不能
让普通读取增加同步点。

目标顺序：

```text
单机 AOF
    -> 后台 durable_seq/显式 durable ACK
    -> 单机 fuzzy checkpoint
    -> 单机恢复
    -> 单机 compact
    -> Follower 异步复制（最后）
```

不在本计划中引入：

```text
MVCC 事务
全局事务快照
固定三个 HPC-Redis Node 的数量假设
同步 quorum=2 提交
通过重放全部 AOF 生成 checkpoint
```

## 2. 已冻结的设计约束

### 2.1 COLD 数据流

```text
写 worker
    -> 有界 append queue
    -> 单一 AOF writer
    -> AOF segment
    -> local flush coordinator
    -> durable_seq
```

多个 worker 不直接并发写同一个 AOF。AOF writer 是每个副本的唯一 append
消费者；Follower 上线后也必须使用同样的本地管线。

### 2.2 序号语义

每个 HPC-Redis Node 内使用标量进度，不使用 `seq[n]` 数组：

```text
appended_seq       AOF append 成功的最高连续 seq
durable_seq        本地 fsync/group commit 完成的最高连续 seq
replicated_seq     Follower durable ACK 的最高连续 seq
ha_safe_point_seq  两副本都已本地持久化的最高连续 seq
```

`durable_seq` 只在运行期间的 group commit/fsync 成功后推进，不单独持久化 durable
marker。重启时，`durable_seq` 之前的 event 必须恢复；之后已经 append 且 framing/
checksum 完整的 event 按 accepted 语义机会性恢复，不保证必然保留。

Checkpoint generation 内按 metadata shard 保存：

```text
captured_seq_of(g, meta_shard_id)
```

它是 fuzzy checkpoint 的分区元数据，不是 HPC-Redis Node 进度数组。

当前实现中，COLD 分区直接等于 `tlc_core` 的 metadata shard。metadata shard
数量由 `TLC_CORE_KEY_META_SHARDS` 编译期参数决定；它不是固定三个，也不是
HPC-Redis Node 数量。路由统一使用 `key_meta_shard_for_hash()`，不再使用独立
的 key-shard resolver。

### 2.3 写入成功边界

```text
owner/term/route 校验
    -> WARM staging（不可见）
    -> COLD append queue
    -> AOF append/accepted
    -> 发布 WARM metadata/HA state
    -> 返回写入结果

后台 flush coordinator 按 pending event 数量/字节阈值或超时执行
group commit/fsync，并推进 `durable_seq`。`LOCAL_DURABLE_ACK` 如保留，
只作为显式等待接口，不得在持有 key-meta shard lock 时调用。
```

Follower 不在 `LOCAL_DURABLE_ACK` 的等待链路中。

### 2.4 Checkpoint 语义

Checkpoint 直接序列化各 metadata shard 的稳定 HA state，不重放全部 AOF，也不
使用 MVCC。每个 metadata shard 只在短暂写保护期间序列化，并记录自己的
`captured_seq`。不同 metadata shard 可以来自不同捕获时刻，这就是 fuzzy checkpoint。

Checkpoint 只捕获已经达到本地 `durable_seq` 的状态；AOF 负责保存 checkpoint
期间的增量和崩溃恢复日志。

### 2.5 持久化健康状态和写入保护

COLD 持久化链路采用最小状态机，避免磁盘故障时无限积压内存：

```text
HEALTHY
    | append/fsync 失败，或超过内存预算
    v
WRITE_BLOCKED
    | 后台探测 append/fsync 恢复
    v
RECOVERING
    | 连续探测成功，队列降到低水位
    v
HEALTHY
```

`DEGRADED` 初期只作为观测指标，不立即阻断写入；当 fsync 延迟、
`appended_seq - durable_seq` 或内存占用持续超过阈值时，才转为
`WRITE_BLOCKED`。server 停止时从任意运行态转为 `STOPPING`，负责唤醒 waiter、
排空队列并尝试最终 fsync。

状态转换约束：

- append 失败或 fsync 失败通过原子状态转换进入 `WRITE_BLOCKED`；
- `WRITE_BLOCKED` 下拒绝新的持久化写入，不再扩大 append queue 或 pending batch；
- `RECOVERING` 只执行后台恢复探测，不立即放开新写入；
- 只有 append、fsync 均成功且队列低于低水位后才回到 `HEALTHY`；
- `TLC_COLD_ACK_ACCEPTED` 已返回的 event 不撤回，后续请求必须看到写入保护状态；
- 普通读路径在持久化故障期间仍可继续服务内存/WARM 数据。

动态 queue 仍必须受 `queued_items`、`queued_bytes` 和 pending flush batch 预算约束。
append queue 与 pending batch 使用独立预算；达到高水位先背压，超过预算或超时后拒绝。
lock-free queue（例如 `blockingconcurrentqueue.h`）只能在基准证明 mutex 为瓶颈后引入，
并通过 C wrapper 保持 event 所有权、shutdown、顺序和容量语义。

WARM 发布继续遵循：

```text
WARM staging -> COLD append accepted -> WARM metadata commit
```

append 失败不得发布 WARM 正式状态。建议暴露 `appended_seq`、`durable_seq`、
`durable_lag`、queue/pending 深度和字节数、最后一次 I/O 错误以及写入保护状态，
用于告警和恢复判断。

## 3. 当前代码基线和差距

持久 COLD 已接入 TLC 写路径；旧的 `tlc_core` 内存 COLD segment/index 已删除。
`three_layer_cache*` 中的历史内存 cold API 仍仅供兼容测试，不再作为 TLC 持久化实现：

| 位置 | 当前情况 | 落地处理 |
|---|---|---|
| `src/three_layer_cache_ub.h` | `cold_layer_t` 是内存 segment 结构，含固定容量和固定 segment 数 | 不作为最终磁盘 COLD API；保留兼容测试，新增独立 COLD 接口 |
| `src/three_layer_cache.c` / `src/three_layer_cache_ub.c` | 历史 `cold_append()` 内存 API | 不接入 TLC 持久化写路径 |
| `src/tlc_core.c` | 使用独立 `tlc_cold_t` AOF runtime | AOF accepted 后发布 WARM；fsync 由后台 flush coordinator 负责 |
| `tlc_core_put_location_epoch()` | AOF accepted 后更新 WARM；durable_seq 由后台推进 | AOF replay 支持启动恢复和显式幂等重放 |
| `src/vemb_v16_tlc.c` | `vemb_v16_tlc_put_with_epoch()` 是正式写路径入口 | 由写入协调逻辑接入后台 flush；显式 durable ACK 在锁外等待 |
| `src/supernode_worker.c` | 当前主要承担批量计算 worker | 只提交 immutable event payload，不直接写 AOF |
| `TLC_NUM_SHARDS` 等宏 | 存在固定测试拓扑配置 | 生产 key-shard 数量和 HPC-Redis Node 数由配置决定 |

## 4. 里程碑和进度

状态取值：`未开始`、`进行中`、`已完成`、`阻塞`。每完成一个里程碑，应同时
更新本表、对应代码测试和本文的验收记录。

| 里程碑 | 内容 | 主要交付物 | 状态 |
|---|---|---|---|
| M0 | 冻结 COLD 数据契约 | event 格式、序号语义、配置和错误码 | 已完成 |
| M1 | 单机 AOF 管线 | append queue、唯一 writer、segment、group commit | 已完成 |
| M2 | 单机写路径接入 | WARM staging、后台 durable_seq、WARM publish | 基础接入完成，staging/异步 fsync 待落地 |
| M3 | 单机 fuzzy checkpoint | metadata shard 捕获、generation、manifest | 已完成基础实现和测试构建故障注入 |
| M4 | 单机恢复 | checkpoint 校验、按 captured_seq replay AOF | 已完成主路径和单机严格异常验收 |
| M5 | 单机 compact 和故障验收 | retention、AOF 回收、故障注入测试 | 进行中 |
| M6 | Follower 异步复制 | replication cursor、Follower COLD、safe point、切主 | 规划完成，未开始 |

## 5. 分阶段实施内容

### M0：冻结 COLD 数据契约

建议新增独立接口文件：

```text
src/tlc_cold.h
```

至少定义：

- `cold_backup_event` 的编码、所有权和生命周期。
- `appended_seq`、`durable_seq` 的推进规则。
- AOF entry、segment、manifest、checkpoint generation 的版本字段。
- append、flush、checkpoint、recovery 的错误码。
- metadata shard 分区 ID 与 COLD event/checkpoint 的对应关系。

验收条件：

- event 可以独立编码、解码和校验 checksum。
- event payload 不引用可被 WARM 淘汰或覆盖的内存。
- 明确区分 `constructed`、`accepted`、`durable`。

### M1：单机 AOF queue、writer 和 group commit

建议拆分实现：

```text
src/tlc_cold_format.c
src/tlc_cold_queue.c
src/tlc_cold_aof.c
src/tlc_cold_flush.c
```

实现：

- 有界 MPSC append queue，满时背压且不丢 event。
- 单一 AOF writer 分配连续 seq 并独占当前 segment。
- entry framing、checksum、segment rotate 和 seal。
- 时间戳复用 `vemb_v16_monotonic_ns()`；checksum 使用 xxHash3 流式多段计算，
  不逐字节执行自定义 FNV，也不为拼接 payload 增加额外复制。
- local flush coordinator 按条数、字节数或时间窗口执行 group commit。
- fsync 成功后推进 `durable_seq`。
- 崩溃后截断尾部不完整 entry；中间损坏或 seq gap 必须报告错误。

验收条件：

- 多 worker 并发提交不会破坏 AOF 顺序和 framing。
- queue 满、磁盘写失败、fsync 失败均能返回明确状态。
- 单机重启可扫描 AOF 并恢复到最后一个完整 durable entry。

当前实现验收记录（`src/tlc_cold.c`）：

- append queue 和待刷盘 pending batch 均有界；容量耗尽时通过条件变量背压，event
  所有权不丢失。
- writer 是唯一 AOF append 消费者；AOF 写入持有 `io_mu`，加入 pending batch 在
  释放 `io_mu` 后进行，避免与 flush coordinator 形成锁反转。
- group commit 在条数/时间窗口达到后执行 `fsync`，失败通过 durable ACK 和后续
  submit 传播；`appended_seq` 不冒充 `durable_seq`。
- 启动扫描校验 header、op、长度、checksum 和连续 seq；仅允许最后一个 segment
  截断尾部不完整 entry，中间 segment 尾损坏直接拒绝启动。
- `benchmark/tlc_cold_ut` 覆盖多 worker、容量为 1 的背压、segment rotate、重启
  恢复、末尾尾部截断、非末尾损坏拒绝，以及未等待 durable ACK 时的 shutdown drain。

### M2：接入单机 WARM 写路径

以 `vemb_v16_tlc_put_with_epoch()` 和 `tlc_core_put_location_epoch()` 为主要
接入点，形成：

```text
prepare event + WARM staging
    -> COLD submit/append accepted
    -> publish WARM metadata/HA state
    -> 后台 group commit/fsync 推进 durable_seq
```

禁止：

- AOF 失败后发布 WARM 正式状态。
- 普通写路径不得在持有 key-meta shard lock 时等待 fsync；如需
  `LOCAL_DURABLE_ACK`，只能通过显式锁外等待接口完成。
- 请求线程等待 Follower。
- 在低层 `tlc_core` 中绕过 COLD writer 直接写 segment。
- 在 `tlc_core_get_*`、VEMB/VSIM 读取函数中增加 COLD 锁、磁盘读取或 durable 等待。

当前实现进度：

- `tlc_core_enable_cold()` / `vemb_v16_tlc_enable_cold()` 可在 Node 接收写请求前
  启用本地持久 COLD runtime；COLD 分区直接使用 `key_meta_shard_for_hash()`，
  不额外注入 resolver。
- `tlc_core_put_location_epoch()` 在 WARM 正式提交前提交 `PUT` event；当前代码仍
  在 key-meta shard lock 内等待 `TLC_COLD_ACK_DURABLE`，按新决策待改为 append
  accepted 后由后台 flush coordinator 负责 fsync。
- `tlc_core_delete_with_epoch()` 采用同样顺序提交 `DEL` event，再发布 tombstone；
  fsync 由后台 flush coordinator 按阈值或超时触发。
- 当前 event 的 `term` 使用写路径的 `topology_epoch`；独立 HA term 注入留到
  Follower/HA 阶段统一接入。
- 生产启动边界支持通过 `HPC_REDIS_COLD_DIR` 启用 COLD；metadata shard 数量
  使用当前编译期参数，后续再扩展动态调整。
- COLD append/queue 提交失败时不发布 WARM 正式状态；WARM 写入或 metadata publish
  失败时保留已落盘 AOF entry，并打印包含 key hash 的 `WARNING` 供后续显式恢复。
  后台 fsync 失败时推进失败告警和写入保护，不得把未 durable 的 event 当作已持久化。
- COLD runtime 由 `tlc_core_destroy()` 关闭；普通读函数未增加 COLD 调用或等待。
- `tlc_cold_replay()`、`tlc_core_recover_cold()` 和
  `vemb_v16_tlc_recover_cold()` 已提供恢复/幂等重放接口；启用 COLD 时自动
  replay，失败会打印 `WARNING` 并拒绝启用。进程内自动重试编排仍待补充。启动注入通过
  `HPC_REDIS_COLD_DIR` 配置。

验收条件：

- 已返回 `LOCAL_DURABLE_ACK` 的写入，进程重启后可以恢复。
- `ACK_ACCEPTED` 写入在 fsync 前崩溃时允许丢失，也允许因完整 AOF entry 保留而恢复；
  该恢复不等同于返回过 `LOCAL_DURABLE_ACK`。
- WARM 发布失败时 AOF entry 保留且可幂等重试。
- `appended_seq` 和 `durable_seq` 的含义与实际推进点一致。
- 读热点路径的基准吞吐和尾延迟不能因 COLD 接入增加同步/I/O 操作。

### M3：单机 metadata shard fuzzy checkpoint

当前已实现：`tlc_core_publish_checkpoint()` / `vemb_v16_tlc_publish_checkpoint()`
按 metadata shard 遍历当前 metadata/WARM 状态，直接生成对应分区 state；
`tlc_cold_publish_checkpoint()` 负责 generation 文件 checksum、临时文件 fsync、
原子 rename、目录 fsync 和 manifest 原子发布。发布由独立 checkpoint mutex
串行化；generation 已发布后不可覆盖，且新 generation 必须严格大于 manifest
中的当前 generation，重复或回退 generation 会拒绝并打印 `WARNING`。

`vemb_v16_tlc_publish_checkpoint()` 是参数形状的外部校验边界（包括非零
generation 和结果指针）；`tlc_core_publish_checkpoint()` 只校验 COLD 等运行时
状态，并直接使用 `core->key_meta_shard_count`。下层文件发布器
按头文件中的严格前置条件执行，不重复检查同一组指针和数量条件；它仍校验
`captured_seq <= durable_seq`、metadata shard 顺序、状态指针完整性以及 manifest/
文件格式完整性。

当前 checkpoint builder 使用对应 metadata shard 的短暂写保护来捕获稳定 entry，
因此锁粒度、AOF 分区粒度和 checkpoint 分区粒度一致。已增加 generation 文件
校验 API，并在发布新 generation 前验证旧 manifest/generation；保留 generation
回退和完整故障注入属于后续 M3/M4 验收工作。

Checkpoint builder 直接从 metadata shard HA state 生成数据：

```text
选择 metadata shard
    -> 获取该 metadata shard 短暂写保护
    -> 序列化稳定状态
    -> 记录 captured_seq
    -> 释放写保护
    -> 继续下一个 metadata shard
```

generation 文件至少包含：

```text
generation, term
checkpoint_seq = min(all metadata shard captured_seq)
meta_shard_count, generation_checksum

per metadata shard:
    meta_shard_id
    captured_seq
    state_checksum
    serialized state
```

发布使用：

```text
checkpoint.<generation>.tmp
    -> write/checksum
    -> fsync
    -> atomic rename
    -> fsync directory
    -> fsync + atomic publish manifest
```

验收条件：

- 不暂停整个 HPC-Redis Node。
- 不通过重放全部 AOF 生成 checkpoint。
- 每个 captured state 不超过本地 `durable_seq`。
- 并发写入期间不同 metadata shard 可以产生不同 `captured_seq`。

### M4：单机恢复

```text
读取 manifest
    -> 校验 active/retained generation
    -> 加载各 metadata shard state
    -> 按 metadata shard captured_seq 过滤 AOF 增量
    -> 校验 term、seq、version、checksum、tombstone
    -> 重建 HA state
    -> 重建 WARM/HOT
```

当前实现已提供 `tlc_cold_validate_checkpoint()`、`tlc_cold_load_checkpoint()`
和 `tlc_cold_replay_after()`。TLC 启动恢复优先校验并加载 manifest 指向的有效
metadata-shard checkpoint，再按各 shard 的 `captured_seq` replay AOF，从而恢复
checkpoint 时刻及其后的完整数据。只有不存在 checkpoint 或 checkpoint 校验失败
时才回退到完整 AOF replay；checkpoint 已校验通过但 state 加载失败会直接报告恢复
失败，storage 创建失败并阻断 Node 启动。正常服务路径不存在部分可见 WARM；直接
调用底层恢复接口的测试/维护工具仍应在失败后丢弃该 core。manifest 指向的 generation 损坏时，自动从旧
generation 回退仍需后续补齐。

恢复失败的可用性语义：单节点部署没有可替代副本时，HPC-Redis Node 必须保持不可用；
双副本 HA 部署由 HA/LVS 接入层将请求切换到另一个健康的 HPC-Redis Node，故障节点
在隔离状态下重新同步和恢复，不能让启动失败的节点接收业务请求。

验收条件：

- 有效 checkpoint 加载后，AOF 增量可以恢复到崩溃前的完整数据状态。
- AOF 尾部半条 entry 可以安全截断。
- AOF 中间损坏、gap、旧 term 或 checksum 冲突会停止恢复。
- WARM 丢失后可以由 checkpoint + AOF 重建。

旧 generation 自动回退不属于当前 M4 主路径。由于 checkpoint 只是恢复加速基线，
在 AOF 尚未 compact 删除历史段之前，最新 checkpoint 校验失败可以回退到全量
AOF replay。generation 保留集合、旧 generation 自动选择以及与 AOF compact 边界
的协调统一延后到 M5。

### M5：单机 compact 和故障验收

单机阶段先实现 checkpoint 约束；Follower 接入后再叠加 HA safe point：

```text
checkpoint_floor_seq = 调用方请求的 checkpoint 回收边界
retained_generation_floor_seq = retention 集合中最小 checkpoint_seq

单机：
    aof_compact_through_seq = min(checkpoint_floor_seq,
                                   retained_generation_floor_seq)

双副本：
    aof_compact_through_seq =
        min(checkpoint_floor_seq, retained_generation_floor_seq,
            ha_safe_point_seq)
```

实现：

- 只处理 sealed AOF segment。
- 只有 segment 最后一条完整 event 的 seq 不超过 compact 边界时才直接回收；
  边界落在 segment 中间时暂不回收该 segment，避免为了 compact 引入二次重写。
- compact 前必须读取并完整校验 active checkpoint generation；manifest 不存在或
  generation 校验失败时拒绝回收。
- compact 会预检 retention 集合中的每个 generation，并以最旧保留 generation 的
  `checkpoint_seq` 作为额外下界；调用方提供的 `checkpoint_floor_seq` 超过该下界时拒绝
  操作，防止回收旧 generation 仍需要的 AOF。
- checkpoint generation 按 retention count 保留最近 generation（当前 manifest generation
  包含在该数量内），始终保留 manifest 当前 generation；
  generation 损坏时自动回退旧 generation 仍登记为后续项目。

当前已实现单机显式 `tlc_cold_compact()`：compact 边界取
`min(checkpoint_floor_seq, retained_generation_floor_seq, ha_safe_point_seq)`，单机没有
Follower 时调用方传入 `UINT64_MAX` 作为不额外收紧的 safe point。compact 只回收 active segment 之前、
且最后一条 event 的 seq 不超过边界的 sealed AOF segment，并按 retention count
清理旧 checkpoint 文件；恢复扫描允许回收前缀后保留的 segment ID 空洞。已增加
active generation 完整校验和 safe point 边界测试；compact 中途崩溃回退、generation
自动回退、generation 与 AOF 保留边界的完整故障注入，以及 Follower safe point 接入
仍待完成。测试构建已增加 compact 删除 AOF segment、删除旧 generation、目录 fsync
前中断的故障注入；三类中断均已完成关闭并重启后的 checkpoint + AOF 恢复验收。
AOF append 和 group-commit fsync 失败的测试注入也已验证：失败请求不返回 durable
ACK，`durable_seq` 不越界，后续写入被熔断并输出错误告警。

验收条件：

- compact 崩溃不会破坏 manifest 可恢复区间。
- 不删除任一保留 generation 所需的 AOF。
- 已返回 `LOCAL_DURABLE_ACK` 的 event 不会因 compact 丢失。
- 磁盘空间耗尽时触发告警和写入保护，不静默丢数据。

### M6：最后实现 Follower 异步复制

Follower 只在 M0-M5 完成并通过验收后落地：

```text
primary replication cursor
    -> replication queue
    -> Follower COLD ingress
    -> Follower append queue
    -> Follower AOF writer（沿用原 seq）
    -> Follower flush coordinator
    -> durable ACK
    -> replicated_seq
    -> ha_safe_point_seq
```

Follower 必须复用单机阶段已经验证的：

```text
AOF event format
AOF writer
group commit
manifest/checkpoint
recovery
compact
```

本阶段新增：

- HA Agent sender/receiver。
- replication cursor 和有界 replication queue。
- Follower term/seq/checksum 校验。
- Follower 幂等追加和 durable ACK。
- `replicated_seq`、`ha_safe_point_seq` 推进。
- AOF 补段、snapshot resync 和 HPC-Redis Node 粒度切主。

验收条件：

- Follower 网络接收线程不直接写 AOF。
- Follower durable ACK 只在本地 fsync/group commit 后发送。
- Follower lag 不影响 primary 的本地 durable ACK。
- safe point 未推进时 compact 不越界删除。
- 一个 HPC-Redis Node 故障只切换对应 Node group。

## 6. 测试矩阵

### 单元测试

- event 编解码、版本和 checksum。
- seq 连续性和幂等。
- append queue 满和所有权转移。
- segment rotate、seal 和尾部截断。
- group commit 批处理和 fsync 错误。
- checkpoint generation/manifest 原子发布、发布串行化和 generation 单调性。
- checkpoint generation 损坏、AOF 中间损坏、seq gap、metadata shard 越界和尾部截断边界。

### 单机集成测试

- 多 worker 并发写入。
- durable ACK 后进程重启。
- WARM 发布失败后的 AOF 重试。
- 并发写入期间 metadata shard fuzzy checkpoint。
- checkpoint 损坏回退。
- compact 中途崩溃恢复。
- 磁盘空间不足和恢复告警。

### 最后阶段的 HA 测试

- Follower 延迟、断连和队列满。
- 重复 event、seq gap、term 冲突和 checksum 冲突。
- Follower durable ACK 推进 safe point。
- primary 故障后按 HPC-Redis Node 粒度切换。
- 未同步 AOF 尾部的数据边界报告。
- snapshot resync 与 compact 保留边界协同。

## 7. 当前进度记录

当前已完成 M0、M1，M2 已完成基础接入：新增独立 `src/tlc_cold.h` / `src/tlc_cold.c`，
实现有界 append queue、单一 AOF writer、segment rotate、group commit、
`appended_seq` / `durable_seq` 和基础单元测试 `benchmark/tlc_cold_ut.c`。
M2 已接入 `tlc_core` 和 v16 写主链路；COLD replay 只在启用/恢复或显式重放时执行，
不会进入 `GET/VEMB/VSIM` 读取热点。

```text
M0  已完成
M1  已完成：本地 AOF runtime、恢复扫描、背压和 shutdown drain 已验收；测试构建
    已覆盖 append/fsync I/O 失败的 ACK、seq 和熔断边界，真实 ENOSPC/硬件 I/O
    环境仍留待集成测试环境覆盖
M2  基础接入完成：单机 PUT/DEL AOF accepted 提交、metadata shard 路由、生产启动
    配置注入，以及 AOF replay 的 WARM 恢复/幂等重放；持锁等待 durable ACK 和
    严格 WARM staging 仍待按新决策落地
M3  基础实现和测试验收完成：已实现按 metadata shard 生成 checkpoint state、不可变 generation
    文件、checksum、fsync/rename/manifest 原子发布、发布串行化、generation 单调性
    和 generation 文件校验；已完成测试构建专用发布故障注入及并发 fuzzy checkpoint
    验收
M4  主路径和单机严格异常验收完成：已实现 manifest/generation 完整校验、checkpoint state 加载，以及
    按 metadata shard `captured_seq` 之后 replay AOF；严格异常边界和完整集成验收
    已完成单机基础验收；AOF 损坏/gap/尾部异常、旧 term、state 应用失败和单节点
    启动阻断均已通过。旧 generation 自动回退延后到 M5
M5  进行中：已实现单机 sealed AOF segment 回收、checkpoint retention、active
    generation 完整校验、safe point 边界和 compact 后 segment ID 空洞恢复；
    `benchmark/tlc_cold_ut` 已验收 floor 越界拒绝、safe point 收紧、retention、
    compact 后重启恢复和增量 replay，并通过 AOF segment 删除、旧 generation 删除、
    目录 fsync 前中断三类 compact 中断恢复测试。generation 自动回退、真实进程级
    `kill -9` 注入和 HA safe point 接入仍待完成

测试环境说明：`benchmark/vemb_v16_tlc_ut` 中的 COLD/fuzzy checkpoint 测试可在本机
执行；其中既有 UB RPC ring 测试依赖 Ascend UB 运行时和远程 peer ring，出现
`failed to open vemb_v16 ub rpc peer rings` 时应在支持 UB 的远程机器上复验，不能将
该环境错误归因于 COLD 或 checkpoint 实现。
M6  未开始
```

后续每次实现或验证后，更新：

```text
里程碑状态
完成日期
涉及文件
测试命令和结果
未解决问题
```

## 8. 参考文档

- [TLC_2X3_HA_ARCHITECTURE.md](./TLC_2X3_HA_ARCHITECTURE.md)：COLD、HA 双副本、
  key-shard fuzzy checkpoint、恢复和 compact 的完整架构设计。
- [MySQL_InnoDB_log_Doublewrite.md](../../hpc-redis-data/docs/MySQL_InnoDB_log_Doublewrite.md)：
  InnoDB redo、group commit、checkpoint 和 Doublewrite 的参考分析。

## 9. TODO：基于 lock-free queue 优化 COLD queue

当前 M1 使用 `pthread mutex + cond` 实现有界 MPSC queue，优先保证容量、
背压和 event 所有权语义。后续性能优化阶段基于本文前述 lock-free queue
设计评估 DuckDB 引入的 `blockingconcurrentqueue.h`，候选集成方式为：

```text
BlockingConcurrentQueue
    + free_slots semaphore
    + fixed event pool
    + per-worker ProducerToken
    + AOF writer ConsumerToken
```

评估前提：

- `BlockingConcurrentQueue` 默认不提供严格最大容量，不能直接替换当前 queue。
- `free_slots` 必须保证 queue 中 event 数量有硬上限，并在 enqueue 失败时归还。
- event 应来自固定或有界内存池，不能依赖队列内部无限动态扩容。
- `wait_dequeue` 只用于 writer 等待 event，不能代替 durable ACK。
- shutdown 必须唤醒阻塞的 writer，确保线程可退出。
- C++ queue 需要通过独立 wrapper 暴露 C ABI，不能把 C++ 类型泄漏到 COLD C 接口。
- 必须通过基准证明 queue mutex 是实际瓶颈后再替换，且读热点路径不得因此增加同步。

状态：未开始。完成标准：功能/故障注入测试不回归，容量和背压可证明，且
多 worker 写入吞吐和尾延迟优于当前 M1 实现。

## 10. TODO：提高 WARM 层内存满载率

当前 WARM 层按 slot/region 管理容量。后续将评估改为连续数组分配，并使用
freelist 管理 tombstone 对应的可复用 slot，以减少分片和删除留下的容量空洞，
提高 WARM 层的有效内存利用率。

约束：

- 保持 `region_id`、`local_slot`、`owner_generation` 句柄语义不变；
- tombstone 和旧 handle 的可见性规则不变，调用方仍需自行校验 stale handle；
- 不能让普通读路径增加 COLD 锁、磁盘 I/O 或 durable 等待；
- 必须覆盖并发 PUT/DEL、重启恢复、WARM 容量耗尽和故障注入测试。

状态：未开始。
