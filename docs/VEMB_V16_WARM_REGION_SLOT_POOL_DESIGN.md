# WARM region 空闲槽池改造设计与实施步骤

日期：2026-10-15

2026-10-17更新：Server cache-hit本地generation镜像、失效/发布契约及CLI安全handle读取TODO见[WARM allocator v2设计](VEMB_V16_WARM_ALLOC_V2_DESIGN.md)开头章节。本地快路径已实现并通过TLC/manifest/sync/resync UT；迁移控制UT的SIGBUS可在9fc8cf8基线复现。远端机器暂时无法连接，UB性能A/B待恢复后执行，尚无新性能结论。

## 1. 目标与范围

取消 WARM payload 的 8-way 绑定。新 key 直接从选中 region 的统一空闲槽池领取任意槽，不再先访问 key 对应的集合。哈希继续用于 key 索引、并发 shard 和 region 选择，不决定物理槽位置。

本次先落地物理分配与配套定位：region 空闲池、位置索引查找、覆盖写、初始化/运行时 attach/HA reset/destroy，以及容量和并发回归测试。动态 key_meta 扩容、删除后的安全回收、remote_meta 删除属于后续独立阶段，不能把完成物理分配改造描述为这三项已经完成。

验收目标：在索引可容纳 key、没有其他分配者抢占、默认关闭 LRU 的条件下，单 region 可把全部物理槽分配完，key 的哈希分布不再造成提前失败。多 region 保留本地优先和原有 vnode/weight 选择顺序。

## 2. 当前问题

- `region_set_bounds()` 将每个 key 限制在一个最多 8 槽的集合中；集合满后只能换 region。
- `warm_lookup_region()` 同样只查这个集合，单独修改写路径会使位置缓存 miss 后读不到数据。
- 普通 PUT 通过 `key_meta.location` 原位覆盖，但恢复、副本应用、checkpoint 加载和迁移也直接调用 `warm_put()`，必须统一覆盖已有 location 的处理。
- 多个 TLC 可映射同一 region；进程内空闲信息不能替代共享 `slot_meta.state` 的原子所有权转换。
- 删除保留 tombstone，但没有归还物理槽；key_meta shard 仍为固定容量。这两个限制不会随取消 8-way 自动消失。

## 3. 结构与职责

```text
key -> 内存 key_meta 索引 -> region_id / local_slot / generation
                                      |
                           slot_meta + 固定大小 payload

新 key -> region 选择 -> region 空闲 bitmap -> CAS FREE -> FILLING
```

### 3.1 region 空闲池

每个运行时 region 持有进程内原子空闲 bitmap 和分配游标；bitmap 的一个 bit 对应一个候选物理槽。启动和运行时 attach 时读取共享槽状态构建空闲池，尾部不足一个 word 的无效 bit 必须清零。

分配时直接从 bitmap 取候选位，原子移除该位，再通过共享 `slot_meta.state` 的 `FREE -> FILLING` CAS 获取槽所有权。另一个 TLC 已领取候选槽时，CAS 失败并继续取候选位，不能写入对方槽。bitmap 仅为当前分配者的候选池，最终所有权以共享状态为准。

本阶段物理槽不会在在线删除后重新变为 FREE，因此已消耗的候选位不必在线恢复。HA 全量 reset 必须在现有隔离条件下同时重建槽状态和空闲池；将来引入在线回收时，必须补齐各分配者发现新空槽的机制，不能只更新回收者自己的 bitmap。

bitmap 属于进程内运行时资源，本阶段不修改共享 slot_meta/header 布局和版本。create 失败、runtime attach 失败、正常 destroy 都需正确释放资源。

已有 payload 的 region 在 attach 时额外构建只读的槽身份索引，以 `key_hash + fingerprint` 定位 attach 时已存在的槽，并在使用前验证 generation/state。该索引用于本地 key_meta 尚未恢复时识别已有数据，避免恢复到满 region 时重复分配；新建空 region 不分配这张索引。attach 后其他 TLC 新发布的槽仍由共享发现路径读取。HA reset 清空该索引。

### 3.2 key 索引与查找

`key_meta` 是本 owner 的完整 key 元数据；`slot_meta` 只描述物理槽身份与状态；`location_cache` 是可丢弃的加速缓存。

位置缓存 miss 后，优先在 shard 锁下从 key_meta 取 location，并验证槽身份、generation 和写序列。释放 shard 锁前复制 location，不向调用者返回元数据项指针。保留原有 tombstone 和 source CUTOVER/SOURCE_GC 的前后 fence。

对本 core 尚无元数据的共享/导入数据，兼容查找可以扫描整个 region，不再按 8-way 猜位置；命中后进入位置缓存。该扫描是共享数据发现路径，不能让本 owner 正常已索引读退化为全 region 扫描。后续 remote_meta/RPC 改造将让 owner RPC 成为远端权威定位路径。

迁移 snapshot 已持有 key shard 锁，必须使用已持锁的位置查找逻辑，避免新索引读路径再次获取同一把锁。

### 3.3 写入与覆盖

所有生产 `warm_put()` 调用点已由 key shard 锁保护：普通 PUT、COLD/副本事件应用、checkpoint state 加载、迁移应用。内部接口按此严格前置条件实现。

已存在物理位置的 key 通过 key_meta 中的 location 原位覆盖；新 key 从 region 空闲池分配。恢复和迁移同样优先使用本地已记录的位置，不能每次重放都领取新槽。被淘汰或已失效的位置仍须通过 generation/state 校验，不得盲写。

删除后再次写回同一个 key，应保留当前利用旧槽重新写入的能力；由于 tombstone 不保存有效 location，此兼容路径需要按槽身份发现旧位置。不同 owner 的业务写入仍由 topology/迁移协议决定归属；空闲池只仲裁物理槽，不能承担分布式同 key 写入串行化。

可选 LRU 构建保持原有开关和持久化状态约束，候选范围从单集合改为整个 region；默认构建只在空闲池无法领取槽时换 region/失败。

## 4. 本次实施步骤

1. 写入本文，明确本次范围和后续依赖。
2. 增加 region 空闲 bitmap 的构建、候选领取、reset 和销毁；通过 slot state CAS 保留跨 TLC 所有权。
3. 删除 `TLC_CORE_WARM_WAYS`、`region_ways()`、`region_set_bounds()`，新分配直接使用空闲池，更新不再准确的 `set_no_place` 日志。
4. 统一 `warm_put()` 已索引覆盖逻辑，核对普通写入、重放、checkpoint 和迁移四类生产调用点。
5. 缓存 miss 后直接使用 key_meta.location；共享数据兼容查找覆盖全 region；snapshot 使用持锁版本，保留源端 fence/tombstone。
6. 增加容量、非整 word 尾部、缓存碰撞、共享池竞争、预占槽 attach、重复应用和跨 region fallback 测试。
7. 编译并运行 TLC 单测，以及受此次改动影响的 HA replica、同步和 resync 单测；运行 `git diff --check`，记录验证结果。

## 5. 验收场景

- 构造旧 8-way 同桶 key，实际 key 数超过 8，写满 region 全部槽，确认 location 唯一且 payload 正确。
- 槽数不是 64 的倍数时，不越界、不少分配、不分配尾部无效 bit；再插入一个新 key 在默认构建下失败。
- 构造 location_cache 相同探测窗口的 key，挤出缓存后仍能按完整 key 从内存索引定位任意槽。
- region 填满后已存在 key 仍可原位覆盖，generation 规则不变。
- 同 key 多次恢复/副本应用/迁移更新不持续占用新槽。
- 多线程和多个 TLC 共享 region 填满时无重复槽、无 payload 覆盖。
- 初始化已有占用槽和 runtime attach 的 region，不能把既有槽当空闲槽；共享读取能发现不在原 bucket 的数据。
- HA reset 后空闲池恢复可分配；SOURCE_GC/CUTOVER/tombstone 回归通过。

## 6. 后续完整满载能力

### 6.1 动态 key_meta shard

每 shard 持锁扩容并 rehash，分配失败时保留旧表。所有 entry 指针必须在锁内使用。代码中 checkpoint、迁移枚举和 `collect_source_active_keys()` 的 cursor 有固定 shard 容量假设，必须一起改；不能只替换插入函数。首版不需要为已受锁保护的表额外引入 RCU。

删除 key 的业务 tombstone 是版本/复制/迁移状态，不等同于开放寻址实现的删除标记。扩容 rehash 必须保留未达到安全水位的业务 tombstone，不能以“清理 hash tombstone”为由丢弃。

### 6.2 安全回收

先补齐服务端/客户端直接读的槽身份和写序列校验，或建立读者 pin/lease；再实现删除和 SOURCE_GC 的物理槽回收。回收保留必要的 tombstone/version，更新空闲池并保证其他映射该 region 的分配者能发现新空槽。最后设计 tombstone 的复制/重放/迁移安全清理水位。

### 6.3 远端定位 RPC

owner 身份和 topology 解析从 remote_meta view 中解耦；远端查询直接 RPC 获取 owner 索引中的 handle，再校验并读取 WARM。删除 remote_meta 查询、发布、repair、映射与配置，同时保留必要的 RPC peer、region attach、HA/迁移状态机。压测全部远端查询走 RPC 的吞吐和 P99 后完成收敛。

## 7. 实施记录

- 已落地 region 级原子空闲 bitmap、共享槽 CAS 仲裁、key_meta location 定位、attach 身份索引和全 region 共享发现；已删除 8-way 分配/查找边界。配置 region 与 runtime attach region 均处理初始化、失败释放、reset 和 destroy。
- 本地 `vemb_v16_tlc_ut`、`tlc_sync_ut`、`tlc_resync_ut` 与 `git diff --check` 通过；此前的 ASan/UBSan、LRU 开关定向验证通过。
- 初阶段本地 HA 网络场景的失败曾在 HEAD 基线复现。第二阶段在两台远端运行完整 UT 后也复现，确认是既有 resync pending-session 状态问题并修复；最终 HA UT 和跨机 UB HA 均通过，见第 9 节。
- 代码同步至 `root@192.168.90.111:22` 和 `root@192.168.90.112:22` 的 `/root/szz/codespace/hpc-redis`，关键源码与本地逐文件校验 SHA-256。两台均为 Linux aarch64，在机器上原生编译并通过上述三组 TLC 单测。

### 7.1 真实 UB WARM 槽池验证

容量回归测试支持显式指定 `TLC_UT_WARM_UB_PATH` 与 `TLC_UT_WARM_UB_OFFSET`。未设置时使用普通内存；设置后将 slot_meta 和 payload 一起映射到指定 UB 设备范围并重新初始化。运行前需确认本机 Export 和对端对应 Import 无使用者，避免覆盖其他测试数据。

两台机器分别使用各自本地 Export `/dev/obmm_shmdev4`，偏移 `335544320`（320 MiB）。最大映射范围为 9352 字节；对应对端 Import 为 `/dev/obmm_shmdev8`。运行前两端设备均通过空闲检查。每台机器执行：

```bash
make -C benchmark vemb_v16_tlc_ut
TLC_UT_WARM_UB_PATH=/dev/obmm_shmdev4 \
TLC_UT_WARM_UB_OFFSET=335544320 \
./benchmark/vemb_v16_tlc_ut
```

111 和 112 的结果均为 `all tests passed`。同旧桶、同 location_cache 探测窗口的 key 分别占用 1/1、7/7、64/64、65/65、129/129 个物理槽，默认无淘汰构建的额外新 key 写入按预期失败。满载覆盖写、删除后原位写回、全满 region 重新 attach 后逐 key 读取和覆盖均通过。日志位于两台机器的 `/tmp/warm_slot_ub_build.log` 与 `/tmp/warm_slot_ub_test.log`。

这验证了指定 UB 映射范围内的物理槽可以全部利用；没有执行整块设备满容量压测，也没有据此宣称固定 key_meta shard、在线回收和跨机缓存一致性问题已全部解决。

### 7.2 跨机 UB Replica 与恢复验证

使用 `benchmark/tlc_ha_redis_tcp_111_to_112.sh`，111 为 Leader，112 为 Follower。Replica 双向设备分别为 111 `dev4@128MiB` → 112 `dev8@128MiB`，112 `dev9@192MiB` → 111 `dev13@192MiB`；本场景 WARM 使用独立 SHM，验证与上述 UB WARM 槽池测试互补。

```bash
HA_NODE111_SSH=root@192.168.90.111 \
HA_NODE112_SSH=root@192.168.90.112 \
HA_NODE111_PORT=22 HA_NODE112_PORT=22 \
TLC_HA_REDIS_TCP_SKIP_SYNC=1 \
TLC_HA_REDIS_TCP_EVENT_COUNT=20 \
TLC_HA_REDIS_TCP_MAX_VECTORS=128 \
TLC_HA_REDIS_TCP_RUN_ID=codex_warm_slot_20261015 \
bash benchmark/tlc_ha_redis_tcp_111_to_112.sh
```

实际运行的 24 个事件（20 个基础写入加脚本附加操作）经 UB 同步后，Follower TCP SDK 读取通过，双方 durable/applied 进度符合脚本断言；重启 Follower 并清空测试 WARM 后，从同一 COLD 目录恢复并再次校验通过。测试服务器已由脚本退出清理。两端日志和 COLD 数据保留在 `/root/szz/codespace/hpc-redis/benchmark/results/tlc_ha_redis_tcp/codex_warm_slot_20261015`。

复跑需使用新的 run id；以上 `SKIP_SYNC=1` 命令要求先在两端构建 `src/redis-server`、`benchmark/tlc_ha_replica_ub_node_ut` 和 `clients/c/build/sdk_ha_replica_tcp`。

## 8. 容量自动推导与动态 key 索引（第二阶段）

取消 Redis `vemb-v16-max-vectors` 和独立 server `--max-vectors`。manifest 的各 region `bytes / value_size` 是物理容量的唯一来源；独立 server 不使用 manifest 时以 `--warm-bytes` 指定 payload 预算。容量累计使用 64 位，单 region 的 local_slot 仍遵循现有 32 位协议范围。runtime attach 发布后，容量统计覆盖新增 region；映射区域不是 owner 的独占配额。

key_meta 保留固定 shard 数与现有锁，改为稳定追加顺序的记录数组和单独的 Hash 定位表。达到 75% 负载前，在 shard 锁内翻倍扩容，复制记录时不改变序号，再重建 Hash 表。分配失败保留旧数组/Hash；新 key 在持久化或 payload 写入前完成索引空间预留。所有 entry 指针仅在 shard 锁内使用。tombstone、版本、迁移状态全部保留，物理槽数不作为索引记录数的上限。

迁移分页游标改为 `(shard_index, entry_index)`，记录序号在扩容时不变，避免 rehash 漏项。该遍历不是全局一致性快照，已完成 shard 的并发新写仍由现有拓扑写路由/迁移协议处理。checkpoint 的逐 shard 锁内枚举和恢复插入同步适配，HA reset 清空记录及 Hash 定位表。

清理已无生产写入者的旧 HOT→WARM entry/hash/锁结构与 TLC bitmap，避免容量增大后无效预分配。位置缓存保持独立容量。初始 key_meta 依据实际物理容量估算、限制单 shard 预分配规模，运行时独立增长。

保留 wire `max_vectors` 字段的二进制布局，用首个 region 的实际槽数填充，供旧单 region 映射兼容；多 region 客户端使用每个 region 的明确 bytes，不能使用总槽数推导单个映射长度。remote_meta 默认配置由物理容量推导；本阶段不改变其共享布局或实现其动态扩容，不把它视为完整 key 索引。

落地顺序：记录设计 → 动态 key_meta 与稳定分页 → 自动容量/API/配置删除 → 更新活跃脚本与示例 → 容量偏斜、tombstone、恢复、迁移、运行时 attach 回归 → 两台 UB 机器构建/UT/真实 UB 槽池及 HA 链路验收。HA 验收以远端所需环境为准；本地环境缺失不判定为产品缺陷。


## 9. 第二阶段实施与最终验收

### 9.1 已落地内容与配置迁移

- 已移除 Redis `vemb-v16-max-vectors` 配置、独立 server `--max-vectors` 参数，以及 storage/proxy/TLC/core 初始化中的对应容量参数。
- manifest 继续通过每个 region 的 `bytes` 指定 **payload 字节预算**，不包含 slot_meta/header。容量为 `sum(floor(bytes / value_size))`，尾部不足一个向量的字节不可使用。UB 实际映射长度仍包含 allocator layout 与 payload。
- 无 manifest 的独立 server 使用 `--warm-bytes BYTES`。例如 `--dim 300 --warm-bytes 78643200` 对应 65536 个槽。Redis 仍使用 manifest。现存配置文件必须删除旧容量项，不能继续依赖旧默认值。
- `key_meta` 保留 256 个 shard，单 shard 的初始记录容量为 64～256，达到 75% Hash 负载时在锁内翻倍；记录数组按追加序号稳定增长，Hash 索引单独重建。位置缓存、共享 slot_meta 与物理 payload 不随索引 rehash 搬迁。
- checkpoint、增量重放、tombstone、迁移枚举、HA reset/destroy 已适配。新增偏斜用例将 320 个 key 全部送入同一 shard，覆盖 192 个实际物理槽、跨扩容分页、128 个额外 tombstone 和 checkpoint + tail 恢复。
- 在线 region attach 同时支持 UB 和 SHM；SHM 与初始 attach 保持相同的独立 payload/allocator 映射布局。容量查询包含发布完成的 runtime region。descriptor 的兼容 `max_vectors` 字段仅返回首个 region 的实际槽数。
- 更新了活跃启动/测试脚本与当前运行示例；历史实验报告中的旧命令保留为历史记录。

### 9.2 UT 中发现并修复的问题

- 两个 proxy UT 构建目标缺少 `vemb_v16_mapped_region.c` 的链接输入，已补齐。
- runner plan UT 仍传已删除的 batch-enable 参数，已按当前生产函数签名和条件更新。
- peer-view attach 原先仅接受 UB，和现有 SHM 在线扩容 UT 不一致；已补齐 SHM 映射和失败清理。peer-view 响应计数由 callee 初始化，消除未初始化输出。
- 远端 `tlc_ha_replica_ut` 证实：原始快照已安装但未接收对应控制请求时，旧 `resync_snapshot_installed_pending` 会拒绝后续新 session。现在更新的 session 丢弃旧待握手完成状态，并要求安装新 session 自己的快照；同 session 身份不匹配和旧 session 仍拒绝。现有 5002→5003 网络场景即为回归覆盖。

### 9.3 最终验证结果（2026-10-15）

代码同步至 111、112 的 `/root/szz/codespace/hpc-redis`，两端均原生构建 Redis、独立 server、C SDK 和 35 个 TLC/VEMB UT 目标成功。

两端分别执行 32 个无需额外参数的 TLC/VEMB 功能 UT，**均为 32/32 通过**：包括 COLD、sync、replica COLD、HA replica、HA 多进程、resync，以及 Aeron ring、batch、CLI、proxy topology、remote_meta、topology、migration、TCP/UB/peer-view transport、TLC、manifest 和 migration control。日志位于两端 `/tmp/hpc-auto-cap-validation/`；本次范围不包含旧版 Redis/proxy 全仓测试。另两个带 `_ut` 后缀的 warm-copy-latency 和 UB-batch-load 工具是需要额外部署参数的性能测试，本次只验证构建，不计入 32 组功能 UT。

真实 UB WARM 测试在确认两端设备无使用者后，分别使用各自 Export `/dev/obmm_shmdev4@335544320`，对端对应 Import `/dev/obmm_shmdev8`。1、7、64、65、129 槽均能全部写满，满载后更新、重新 attach、读取校验和额外新 key 失败断言通过。日志：`/tmp/hpc-auto-cap-validation/warm-ub.log`。这验证指定小映射的全部物理槽，不等价于整块设备容量压力测试。

跨机 HA 使用第 7.2 节相同设备方向及参数，run id 改为 `codex_auto_capacity_20261015`。24 个事件经真实 UB Replica 同步通过，Follower 重启并 reset WARM 后的 COLD 恢复与 TCP SDK 校验通过；脚本退出已清理测试服务器。两端 artifacts：`benchmark/results/tlc_ha_redis_tcp/codex_auto_capacity_20261015`。该脚本调用 `tlc_ha_replica_ub_node_ut` 的 reset-only 模式初始化 Replica ring；另单独执行该 UT 的 111 leader / 112 follower 成对模式验证 24 个事件（日志 `/tmp/hpc-auto-cap-validation/ub-node.log`）。

所有修改 shell 脚本通过 `bash -n`，`git diff --check` 通过。最终本地源码与两端同步文件逐文件校验 SHA-256。

### 9.4 保留的边界

本阶段取消人为 max-vectors 和固定 shard 配额；索引仍受主机可分配内存及现有整数寻址范围约束，内存分配失败保留旧表。物理槽全满会正常拒绝新 key。DEL/SOURCE_GC 的安全物理槽回收、tombstone 安全清理和 remote_meta 共享表的动态扩容/移除仍按第 6 节另行实施；本次不声称长期 churn 可以自动回收全部物理空间。

### 9.5 memtier 完整构建补充回归

用户的 `NUM_KEYS=100000 KEY_PATTERN=R:R THREADS=64 CLIENTS=4 PIO=3 SNW=3 BUILD=build KILL_OPENCODE=1 bash scripts/run_aeron_cross_node_flamegraph.sh` 暴露了功能 UT 未覆盖的 memtier 编译遗漏：`protocol.h` 的构造函数默认参数仍引用已删除的 `VEMB_V16_DEFAULT_MAX_VECTORS`。检查所有生产调用后，删除未使用的 `m_max_vectors` 成员和构造参数，clone 同步改为只传 dim；不恢复人为容量常量。全仓 C/C++ 源文件不再引用该常量。

修复已同步两端，111、112 均完成 `scripts/vemb_v16_build_stamp.sh build client` 的 SDK + memtier O3/LTO/SVE 完整构建。原命令已通过 server/client 构建与两端负载检查；执行到设备检查时，111 `/dev/obmm_shmdev4` 被另一项 `mooncake_client` 工作负载占用，脚本按预期拒绝重置。用户明确要求保留该进程、只验证构建，因此未继续执行本次 100k flamegraph 性能场景，不将其记录为端到端通过。


## 10. Uniform 性能回退排查与分配布局修正（2026-10-15）

### 10.1 已确认的现象与对照

用户报告 `NUM_KEYS=100000 KEY_PATTERN=R:R THREADS=64 CLIENTS=4 PIO=3 SNW=3 BUILD=skip KILL_OPENCODE=1 bash scripts/run_aeron_cross_node_flamegraph.sh` 只达到 3,807,649.51 QPS，低于架构文档 2026-10-12 的 8,014,805 QPS。错误、notfound、materialized_fail 均为零。首先保留相同客户端二进制、batch、CPU 集及工作负载，仅改变服务端分配布局进行对照。

| 分配布局 | 本地 perf run id | QPS | p50/ms | p99/ms |
| --- | --- | ---: | ---: | ---: |
| region bitmap 从首 word、最低空闲 bit 开始 | `warm_pool_compact_control_20261015` | 3,782,579.08 | 1.447 | 2.287 |
| hash 选择起始 word，word 内仍取最低空闲 bit | `warm_pool_spread_ab_20261015` | 4,593,236.95 | 1.207 | 1.959 |

原布局复现了用户的回退。分散 word 的单轮对照提升 21.43%，但仍明显低于历史基线，不能据此宣称问题全部解决。两轮所有请求和 payload materialization 均成功。

按 collapsed 栈的采样权重统计，历史基线客户端 `sve_streaming_load_f32` inclusive 占比 53.49%，用户回退轮 81.31%，紧凑布局复跑 81.33%，分散 word 后 76.76%。服务端 PIO self 占比同时升高，TLC lookup 占比降低。该证据指向客户端 UB payload 读取等待增加，而非证明 region 分配扫描消耗了 GET 时间。采样占比是相对量，不等价于每请求耗时，也不能单凭这些数据认定具体 UB bank/channel 原因。

旧 8-way 按 hash 将稀疏 key 分布在整个 region；最初的统一池实现总从首空闲 bit 分配，将 100K 个 1200-byte payload 集中在 4GiB region 前约 120MB。分配策略改变了远端读取地址分布，是已得到对照实验支持的性能因素。

### 10.2 当前代码与实施步骤

1. 保留 region 统一空闲 bitmap 和共享 slot_meta 的 FREE→FILLING CAS，不恢复 8-way 容量限制。
2. 以 `key_hash % capacity_slots` 选择首选槽，从对应 word、对应 bit 开始查找；当前 word 无合适空闲 bit 时回绕，随后遍历其他 word。hash 只选择搜索起点，所有物理空闲槽始终可达。
3. 删除固定从零开始的 `free_word_hint`。GET 路径、wire handle、payload 步长和映射布局不变。
4. 新增稀疏布局回归：4096 槽写入 256 个固定 key，校验 payload、region 四个区间以及 word 内四个 bit 区间均有分布。同时保留小容量、满载、偏斜、重 attach、双 TLC 抢槽等容量/所有权测试。
5. 将原 UT 中依赖“首个 key 必在 slot 0”的断言改为实际 handle 的槽位、offset 和 payload 一致性；外部导入/迁移源显式指定的 slot 0 语义不变。
6. 两端构建和功能 UT 后，在设备与 CPU 空闲时按相同命令跑当前槽位分散版本，再与紧凑布局和 word 分散结果比较。若仍有差距，继续用同期旧算法对照及 payload 读取测量隔离历史版本/运行环境因素，不将历史 8M 当作已恢复结果。

### 10.3 本轮性能验收边界

最新的 word+bit 分散版本在 111 完成相同 O3/LTO/SVE 参数的 Redis server 构建，但 `warm_pool_spread_slots_ab_20261015` 在负载检查阶段退出，尚未执行 preload/workload：111 出现 `store_kv_bench_`（约 33 核）、`mooncake_master` 和 `mooncake_client` 工作负载。用户要求保留进程、先完成代码与 UT 验证，因此不绕过负载/设备保护，不执行真实 UB reset 或成对 HA 测试。本节的 4.593M 数据属于中间 word-only 版本，不能作为最新版本性能结果。

最新代码验证：本地 `vemb_v16_tlc_ut`、`tlc_sync_ut`、`tlc_resync_ut` 通过；111、112 均完成 35 个 UT 目标构建并分别执行 32 组功能 UT，均为 32/32 通过。两端构建日志 `/tmp/hpc-spread-ut-build.log`，逐项运行日志 `/tmp/hpc-auto-cap-validation/*.spread.log`。`src/tlc_core.c` 与 `benchmark/vemb_v16_tlc_ut.c` 的本地/两端 SHA-256 一致。`git diff --check` 通过。本轮未运行设备写入测试，以上 UT 结果不代表最新版本已经通过真实 UB 性能验收。

### 10.4 下一轮定位方法

先增加独立的生产 TLC API 定位基准，分开输出预加载成本、handle 命中查询与不存在 key 查询的 wall/process CPU 成本，并验证每次返回结果。通过 capacity、keys、hot-capacity 参数分别控制物理占用和位置缓存压力；同一工作负载可在旧实现的隔离源码目录构建对照。工具不修改生产索引/协议，不添加每请求原子计数或时钟开销。本地 SHM 结果只用于定位服务端 CPU 成本，不能代替 UB 端到端 QPS。现有 lookup_cache_hit/miss 等统计没有递增点，不作为路径命中率证据。硬件恢复可用后，首先完成最新分散槽位版本的端到端对照。

### 10.5 payload 起点对齐对照

硬件恢复空闲后，`warm_pool_spread_slots_retry_20261015` 完成 100K 原参数对照，得到 3,718,944.72 QPS、p50 1.487ms、p99 2.335ms，所有请求及 materialization 成功。任意槽位分散未恢复性能。旧 8-way 首选槽为 8 的倍数，1200-byte 向量的这些 offset 均为 64-byte 对齐；随机任意槽起点只有约 1/4 对齐。因此下一步保持统一池和 packed payload 步长不变，将 hash 的首选位置映射至 offset 满足 64-byte 对齐的槽：槽步长为 `64 / gcd(value_size, 64)`，冲突仍搜索所有空闲 bit，尾槽与非对齐槽都可用，不增加 padding、不损失容量。单独比较这一变化，不提前认定对齐为已确认根因。补充跨 word/尾槽满载回归后运行相同硬件命令。

对齐首选槽的两轮结果为 7,494,549.89 / 7,494,870.49 QPS，单核 1,286,473.35 / 1,284,330.16 QPS，性能稳定但还没有达到历史基线。第一轮客户端 SVE inclusive 降至 28.86%，服务端等待开始主导；接下来构建隔离 HEAD 服务端进行同期对照。

高占用时不能依赖“优先对齐槽”：1200-byte packed payload 的全部槽中只有 1/4 起点对齐。因此另外验证 SVE 读取对非对齐源的处理：先按 predicate 拷贝到下一个 64-byte 源边界的短前缀，再执行原展开循环，尾部仍使用 predicate。禁止 padding 和越界读取，添加两侧 guard-page 与不同源/目的偏移测试。先只更新客户端，以免服务端和客户端变化混在同一轮实验。

SVE 前缀修改的 `sve_streaming_copy_ut` 已在两台 ARM 机器启用真实 SVE 后通过 131,072 组用例：不同源/目的 byte 偏移、0～2048 byte 长度、页首/页尾 guard-page、目标前后哨兵；本地仅验证非 SVE fallback。两端最新 32 组 TLC/VEMB 功能 UT 均通过，日志 `/tmp/hpc-auto-cap-validation/*.aligned.log`。

CPU 定位工具 `benchmark/vemb_v16_warm_lookup_bench` 使用生产 TLC API、8-byte payload 隔离 metadata 成本，参数为 `capacity keys hot_capacity iterations miss_iterations`。例如 `65536 8192 16 1000000 1000`，本地 HEAD 隔离旧 core 得到 steady hit 39.852ns、miss 145.588ns；当前 core 为 36.229ns、miss 58,779.039ns。命中路径没有显示倍数级 CPU 回退，兼容共享发现的全 region miss 扫描则确实放大了约 400 倍。这是本地单轮 SHM 定位结果，不代表 ARM/UB 性能。旧 core 在相同 65,536 物理槽下写入第 15,333 个固定测试 key 即因 8-way 集合满而失败；基准明确拒绝继续输出读取结果。当前统一池的 65,536 槽全满测试成功。

`warm_pool_aligned_sve_head_20261015` 的采样窗口与隔离旧版构建可能重叠，不纳入有效性能结论。后续性能实验与编译/UT 串行。

### 10.6 位置缓存校验开销与安全边界

同期 HEAD 服务端对照 `warm_pool_head_reference_retry_20261015` 为 8,044,158.24 QPS、单核 1,384,079.79 QPS；对齐分配版本为 7,494,870.49 QPS、单核 1,284,330.16 QPS。火焰图显示新版本每次位置缓存命中额外进入 `location_cache_validate_location`，约占服务端采样 16%，而旧 HOT 命中直接返回位置。

曾进行一次跳过 slot 校验的实验，`warm_pool_cachefast_20261015` 达到 7,852,131.22 QPS，但该版本不满足共享 slot_meta 的 generation/owner 校验约束，已撤回，不能作为验收结果。位置缓存命中仍必须校验对应 slot 的身份和 generation；不能用“当前暂不回收槽”替代并发共享状态校验。后续应采用保持校验语义的方式优化，例如让 region/slot 定位内联、减少重复 region 查找和无效分支，再用相同硬件窗口重新测试。

### 10.7 重新核对基线与 128-byte 首选地址实验

源码复核纠正第 10.6 节的归因：HEAD 的 location_cache_get 同样调用 location_cache_validate_location。旧火焰图未单独显示符号，不能证明不存在该校验，可能是 LTO 内联。已撤回轻量 slot 校验和强制内联实验，恢复原始完整校验实现，避免继续依赖错误归因。

旧 8-way 的首选槽是 8 的倍数，对 1200-byte payload 是 128-byte 对齐；64-byte 首选槽实验只保证 4 的倍数。下一轮单独测试 128-byte 首选地址，保留 packed payload 与完整 region 可达性、不 padding、不损失槽容量。65 槽满载回归与本地 TLC UT 已通过。该实验不证明 UB 硬件缓存行大小是 128-byte，只有硬件对照可以判断布局效果。

### 10.8 相邻窗口单项对照结果

以下实验保持同一客户端二进制、100K R:R、64 threads、4 clients、3:3 workers、batch/pipeline32，保留完整 slot 校验。

| 方案 | run id | QPS | 单核 QPS |
| --- | --- | ---: | ---: |
| 128B 首选槽 | warm_pool_align128_fullverify_20261015 | 7,077,145.22 | 1,220,643.83 |
| 旧 hash / 8 槽起点，允许跨全 region 分配 | warm_pool_legacy_origin_20261015 | 6,999,445.93 | 见该轮 key-results |
| 相邻窗口 HEAD | warm_pool_head_window2_20261015 | 8,118,092.78 | 1,408,369.55 |
| 旧起点 + 四倍位置缓存（131072→524288 项） | warm_pool_cache_size_ab_20261015 | 7,289,769.04 | 1,266,735.08 |

各轮返回与 materialization 均无错误。128B 对齐、复现旧首选映射、单独扩大缓存均未恢复 8M；不能认为环境变慢，因为同期 HEAD 达到 8.118M。撤回这些实验改动，恢复 64B 首选分散布局、原始位置缓存容量与原始完整 slot 校验；不保留未经证明的轻量校验或 always_inline。

下一步需在诊断构建中增加每 worker 的 cache hit/miss、索引探测次数和 shard lock 等待统计，测量 lookup CPU/请求，而不是将 inclusive 占比等同于额外成本。同时对比编译与 LTO 产物。上述结果只验证稀疏的 100K 纯读场景，尚未实现满载时总/单核 QPS 无劣化的验收目标。

完整校验链（location validation、slot probe、slot snapshot）强制内联实验 `warm_pool_fullinline_20261015` 得到 7,066,728.08 QPS，同样未恢复 8M，已撤回。所有本轮实验的完整校验保持开启，不采纳跳过 generation/identity 的实现。当前残余回退根因尚未确认，不把缓存未命中、锁竞争或函数调用开销写成已证实结论。可复查每轮 `perf/<run id>/{server,client}/*.svg` 及 `client.workload.key-results.txt`。

撤回后的 64B 分散/完整校验源码已同步 111、112，两端分别构建功能 UT 并运行 32/32 全通过，日志 `/tmp/hpc-auto-cap-validation/*.investigation.log`；111 Redis server 按 O3/LTO/SVE 与 batch32 重建完成。`git diff --check` 通过。没有宣称恢复 8M 或满载性能验收通过。

### 10.9 HEAD / 当前版本的 3:3 与 4:4 同窗口对照

2026-10-15 23:04～23:10 串行运行 HEAD 3:3、当前 4:4、HEAD 4:4、当前 3:3。全部采用 100K key、R:R、64 threads、4 clients、dim300、pipeline/batch32、30 秒 workload、相同 manifest 和 CPU 集。客户端 SDK/memtier 的 SHA-256 四轮一致；保留完整 slot 校验和负载/设备保护，不停止无关进程。当前核心源码 SHA-256 为 `127b769b9aef5c619f0fe5fe05066b051c6064b85d5a00572006b9140b1d11a9`。

| 服务端 | PIO:SNW | 总 QPS | 单核 QPS | p50/ms | p99/ms |
| --- | --- | ---: | ---: | ---: | ---: |
| HEAD | 3:3 | 8,032,686.19 | 1,390,954.58 | 0.671 | 1.335 |
| 当前 | 3:3 | 7,009,247.09 | 1,214,217.38 | 0.983 | 1.255 |
| HEAD | 4:4 | 8,173,414.58 | 1,161,549.11 | 0.671 | 1.215 |
| 当前 | 4:4 | 8,792,659.23 | 1,196,468.67 | 0.615 | 1.159 |

单核口径沿用脚本：QPS /（Redis process CPU cores + CPU-set softirq cores）。各轮 notfound、status_err、materialized_fail、unmatched 均为零。当前版本在 3:3 下总吞吐低于 HEAD 12.74%，但 4:4 下总吞吐高 7.58%、单核高 3.01%。HEAD 的 4:4 同样可以超过 8M。当前 4:4 的单核效率仍低于 HEAD 3:3，不能宣称已经达到原 3:3 的效率目标。

每格仅一轮，不能视为重复实验的中位数或认定锁竞争是根因。结果说明 worker 配比显著影响版本差异，应结合 PIO/SNW 分线程成本和缓存回退比例继续定位，而不是将新增 shard 查询直接解释为所有配置下的吞吐下降。本场景是稀疏纯读，不代表满载或 80/20 混合负载验收。

四组完整 artifacts 位于 `perf/warm_compare_{head,current}_p{3,4}_s{3,4}_20261015_2304/` 的实际对应目录，均有 `server/server.svg`、`client/client.svg` 与 key-results；调度日志在 `perf/warm_compare_20261015_2304/`。脚本结束已清理测试 server，6395 无监听。本次未修改生产代码。

### 10.10 恢复 3:3 的阶段定位

追加当前版本交叉配比，参数与 10.9 相同：3:4 为 6,994,336.04 QPS、单核 1,112,997.21；4:3 为 6,856,123.91 QPS、单核 1,003,853.24。错误、notfound、物化失败和 unmatched 均为零。目录分别为 `perf/warm_compare_current_p3_s4_20261015_cross`、`perf/warm_compare_current_p4_s3_20261015_cross`。仅单独增加一类 worker 没有恢复吞吐。

解释边界：`assign_channel_owners()` 使用同一个 sequence 分别对 PIO/SNW 数量取模。3:3、4:4 是一对一配对，3:4、4:3 会遍历 12 种队列组合，因此混合配置同时改变队列拓扑，不能把它当成完全隔离的单阶段扩展测试，也不能据此证明某一类 worker 没有瓶颈。

已有 collapsed 栈按权重聚合后，以 process CPU/QPS 粗略折算：HEAD 3:3 总成本约 717ns/op，其中 PIO 362ns、SNW 352ns、lookup inclusive 252ns；当前 3:3 约 821ns/op，其中 PIO 417ns、SNW 400ns、lookup inclusive 278ns。lookup 属于 SNW，不能相加。采样窗口和 workload 窗口不同，这些数值只是估算，包含忙轮询成本，不是函数时延；当前 key_meta_find_locked 的 inclusive 约 21ns/op 也不含全部内联锁开销。不能把全部约 104ns/op 差距归因于索引查询。

下一项应保持 3:3 队列拓扑，诊断每 PIO/SNW 的逻辑请求数、实际 batch 大小、队列空轮询/满等待和位置缓存 hit/miss；采用诊断构建的线程局部计数，避免给正式版本增加每请求共享原子操作。若回退成本占主导，再审查锁内复制 location、锁外完整 slot 校验的并发契约并做单项对照。若回退不足以解释差距，应继续检查响应发布、轮询和调度协同，不跳过 slot 校验、不恢复桶容量限制。当前仍未确定能恢复 3:3 的具体代码改动。

### 10.11 线程局部计数与采样计时诊断

增加编译宏 `VEMB_V16_LOOKUP_DIAGNOSTICS`。关闭时无计数/计时/日志代码；开启时用线程局部累计计数，每 1024 次 lookup 采样一次耗时，worker 每 65536 次循环检查是否需要按秒输出。诊断包括位置缓存成功命中/回退、索引成功/失败、共享扫描、采样获取锁/查索引/校验时间，以及 PIO 提交、SNW 出队、completion 批次和提交重试。无每请求共享原子计数。`idle` 是空循环次数，不能换算为空闲时间。

当前诊断源码单独构建于 111 `/tmp/hpc-warm-diag-20261015`，HEAD 对照于 `/tmp/hpc-warm-head-diag-20261015`。两者保留各自读取逻辑和完整 slot 校验；HEAD 仅插入相同口径的 TLS 计数与采样。未替换正式二进制。三轮使用同一个客户端和 10.9 的相同参数，编译和 workload 串行。首次诊断 run 因归档缺少独立构建说明文件失败，不纳入结果；有效 run 如下。

| 诊断版本 | 配比 | 总 QPS | 单核 QPS | 缓存未命中率 | 命中路径采样 ns |
| --- | --- | ---: | ---: | ---: | ---: |
| HEAD | 3:3 | 7,959,704.86 | 1,366,887.58 | 9.217% | 250.9 |
| 当前 | 3:3 | 6,938,198.38 | 1,201,141.97 | 9.220% | 270.9 |
| 当前 | 4:4 | 8,825,735.95 | 1,182,272.13 | 9.221% | 254.6 |

HEAD 3:3 缓存未命中的探测阶段采样平均 122.5ns，之后旧 HOT/8-way 完整回退为 272.4ns。当前 3:3 对应探测阶段 134.8ns，获取 shard 锁阶段 69.0ns，索引查找 299.7ns，slot 校验及解锁阶段 188.9ns；当前 4:4 后三者分别为 72.1/297.5/195.1ns。计时包含时钟调用和边界开销，各版本回退分段数量不同，不能把相加后的差值当成无扰动的净成本，也不能把获取锁阶段均值称为纯竞争等待。诊断构建的吞吐不替代正式构建验收。

三轮均无错误、notfound、物化失败或 unmatched。当前 lookup 回退全部通过 key_meta 定位，indexed_fail=0、scan=0；不存在全 region 扫描拖慢这轮 workload 的证据。两版本提交和出队平均约 31.995 项/批，当前 3:3 completion 约 31.8、HEAD 约 31.89；job 队列提交重试为零。当前 3:3 三个 PIO 的活跃区间提交速率约 2.30～2.33M 项/秒，未发现明显单 worker 偏斜。活跃区间按相邻计数记录的逻辑项速率超过 100K/秒筛选，避免把 preload/退出后的空转混入批次与速率分析。

已确认的优化目标是未命中后的索引回退成本；命中率本身几乎一致，扩大 worker 数也未改善索引回退耗时。没有证据支持把 shard 锁竞争、全 region 扫描或 batch 严重碎片化视为主要根因。缓存成功命中路径在 3:3 也有约 20ns 的采样差异，且当前 4:4 恢复到约 255ns，需要继续隔离地址分布、缓存局部性及流水线影响。尚未恢复 3:3 到基线，不宣称全部差距由索引解释。

有效 artifacts：`perf/warm_diag_head_p3_s3_20261015/`、`perf/warm_diag_current_p3_s3_20261015_retry/`、`perf/warm_diag_current_p4_s4_20261015_retry/`；每组含原始 `server.log`、完整火焰图和 key-results。解析脚本与 JSON 位于 `perf/warm_diagnostics_20261015/`。下一项单变量实验应优先减少回退路径的内存访问/临界区成本，并保持 3:3、统一槽池和 slot 校验，随后用关闭诊断的构建对照验证。

### 10.12 临界区与索引冲突访问的单项实验

审查 raw lookup 的三个生产调用点：常规读取在外层保留迁移/tombstone 前后检查；stable-read 在无 fence/tombstone 时进入 raw；snapshot 传入 key_shard_locked=1 并始终保留外部锁。实验在锁内复制 location 和状态，避免 record 指针越过解锁点，锁外继续完整校验 slot 身份/generation。关闭诊断后，`warm_unlock_p3_s3_20261015` 得到 6,998,019.53 QPS、单核 1,214,869.83，与原 7,009,247.09 / 1,214,217.38 基本一致，未显示恢复收益，已撤回。

增加只在诊断宏开启时存在的 meta_finds/meta_probes/meta_probe_max TLS 计数。`warm_diag_probes_p3_s3_20261015` 的活跃区间每次索引查询平均检查 1.775～1.780 个占用表项；不是大多数请求发生长链扫描。该轮索引查找采样约 302～306ns，但成功缓存命中采样降至 244～247ns。这说明此前 271ns vs HEAD 251ns 的约 20ns 差异未跨诊断构建稳定复现，不能确定为 payload/metadata 地址布局的固有成本；诊断指令和编译布局本身可能扰动结果。该轮 QPS 6,758,259.67，仅用于路径诊断。

进一步实验把本地 key_meta hash 表项由 32-bit ordinal 改为 64-bit 的 high-hash tag + ordinal。先过滤 tag，再读取 record 并完整比较 key；动态增长、稳定 ordinal、完整 slot 校验均保留。关闭诊断的 `warm_hash_tag_p3_s3_20261015` 为 7,024,375.44 QPS、单核 1,223,448.02。单轮总吞吐只高于原结果约 0.22%，没有证明稳定收益，已撤回，不保留表项加倍的内存成本。两个实验均无 notfound、status_err、materialized_fail 或 unmatched；各自本地 TLC UT 通过。正式二进制未替换，源码只保留 opt-in 诊断计数。

300ns 是一次缓存未命中后索引查询的采样成本，只有约 9.2% 请求支付，不能全部算为每请求新增成本。当前完整回退约 558ns，对照 HEAD 约 272ns，乘以回退率后额外约 26ns/请求；这仍含不同分段计时开销，也不等于全部 CPU/QPS 差距。缩短临界区和过滤冲突 record 的两次对照均未恢复 3:3，因此不能宣布已经找出单一主要根因。下一阶段需使用无诊断构建的每线程 PMU/CPU 数据隔离指令、缓存访问及 PIO/SNW 交互成本，避免仅凭采样函数时长推导端到端瓶颈。

### 10.13 无诊断构建的每线程 PMU 对照

脚本增加默认关闭的 `SERVER_PMU=1`，独立于 `PROFILE`。本次使用 `PROFILE=0`，避免火焰图采样和源码计数扰动；预填充后对 PIO/SNW TID 运行 `perf stat --per-thread -I 1000`。默认事件为 cycles/instructions/cache-references/cache-misses，`SERVER_PMU_EVENTS` 可指定同组硬件事件。原始计数、线程列表、事件列表随 server artifacts 保存。分析取第 5～26 秒稳定区间，要求事件可计数、运行比例至少 99.9%；每请求计数以该窗口时长乘整轮 QPS 归一化，是包含忙轮询的估计值。

四事件对照：HEAD 为 7,877,075.26 QPS，当前为 6,844,850.60 QPS。SNW 分别为 856.08/967.92 cycles/op、772.45/790.21 instructions/op、IPC 0.902/0.816；PIO 分别为 875.12/1007.13 cycles/op、883.28/1101.77 instructions/op。目录为 `perf/warm_pmu_{head,current}_p3_s3_20261015`。

随后反序运行当前与 HEAD，使用同组六事件 `{cycles,instructions,stalled-cycles-backend,stalled-cycles-frontend,L1-dcache-load-misses,dTLB-load-misses}`，稳定区间全部事件运行比例 100%。

| 指标 | HEAD 3:3 | 当前 3:3 |
| --- | ---: | ---: |
| 总 QPS | 7,872,837.57 | 6,986,570.38 |
| SNW cycles/op | 856.92 | 940.51 |
| SNW instructions/op | 766.83 | 776.69 |
| SNW IPC | 0.895 | 0.826 |
| SNW backend-stall cycles/op | 436.27 | 507.95 |
| SNW frontend-stall cycles/op | 602.54 | 656.81 |
| SNW L1-dcache-load-misses/op | 16.24 | 16.86 |
| SNW dTLB-load-misses/op | 5.07 | 4.84 |
| PIO instructions/op | 897.63 | 1083.45 |

该轮 SNW 指令仅增加约 1.3%，周期增加约 9.8%，后端停顿占周期比例从 50.91% 增至 54.01%；dTLB miss 没有增加。证据支持继续定位 SNW 的依赖访问/等待成本，但不能凭通用 stall 事件证明具体哪条 load 是根因；frontend/backend 事件可重叠，不能相加为互斥耗时。PIO 更多指令也可能来自等待期间的轮询，不能直接视为业务路径新增指令。目录为 `perf/warm_stalls_{head,current}_p3_s3_20261015_retry`，解析脚本及 JSON 位于 `perf/warm_pmu_analysis_20261015/`。不带 retry 的两份 stalls run 因事件参数远端 shell 展开只采到 cycles，已排除；脚本已修正事件参数引用。

### 10.14 位置缓存增加探测次数的单项实验

在独立目录 `/tmp/hpc-warm-cache8-20261015` 关闭源码诊断构建，仅将位置缓存探测从 4 次改为 8 次；统一 region 空闲池、动态索引、完整 slot 校验保持不变。使用上述相同 3:3、PROFILE=0、默认四事件 PMU 参数，得到 6,906,056.48 QPS、单核 1,202,572.31；SNW 为 942.65 cycles/op、782.53 instructions/op、IPC 0.830。结果仍处于当前版本约 6.84～6.99M 的范围，未恢复 HEAD 约 7.87M 的水平。错误、notfound、物化失败、unmatched 全部为零。目录为 `perf/warm_cache8_p3_s3_20261015`。

实验构建的 TLC UT 在固定假设“超过 4 个同起点 key 必然淘汰首个 cache entry”的断言失败，该断言不适用于 8 次探测；不能称该实验 UT 全通过。因无吞吐收益，已撤回探测次数的编译覆盖入口，默认仍为 4 次，随后重新构建并运行正式配置 `vemb_v16_tlc_ut`，全部通过。脚本 `bash -n` 和 `git diff --check` 通过。未替换远端正式二进制。

下一步应在保持 3:3 拓扑的前提下定位 SNW 具体停顿位置：先检查 UB 主机是否提供可用的 load-latency/SPE 采样；若支持则对 HEAD/当前同口径采样，区分本地索引依赖链、共享 slot metadata 以及 payload 读取。通用 PMU 只能给出阶段方向，尚不能证明 300ns 索引采样解释了全部吞吐差距，不继续保留没有端到端收益的参数试探。

### 10.15 全部工作集命中位置缓存的对照

按用户建议，在独立目录 `/tmp/hpc-warm-allcache-20261016` 将位置缓存从 131,072 项扩大到 1,048,576 项，探测上限从 4 改为 64。仅缓存 key→location，payload 仍在 WARM；保留统一槽池、完整 slot 校验和迁移/tombstone 检查。这是用于消除索引回退的实验配置，不修改正式源码默认值，也不代表满载情况下的内存方案。

先开启 TLS 诊断运行同一 100K Uniform、3:3 workload。三个 SNW 的有效区间累计 210,297,923 次 lookup 全部成功命中位置缓存：miss=0、indexed_ok=0、scan=0，实际读取不再支付 key_meta 回退成本。该诊断轮为 6,867,874.72 QPS、单核 1,209,170.34；目录为 `perf/warm_allcache_diag_p3_s3_20261016`。

随后同一源码关闭诊断构建，使用 PROFILE=0、默认四事件 PMU，得到 7,086,705.33 QPS、单核 1,239,155.03。SNW 为 905.45 cycles/op、768.81 instructions/op、IPC 0.849，仍未达到此前 HEAD 约 7.87M 的水平。目录为 `perf/warm_allcache_release_p3_s3_20261016`，计数解析在 `perf/warm_pmu_analysis_20261015/allcache_{diag,release}.json`。两轮错误、notfound、物化失败和 unmatched 全部为零；无诊断轮本身不采集 hit/miss，其零回退依据来自同配置诊断轮。

该实验已证明全工作集命中在本次 workload 下可实现，但消除回退的配置尚未恢复基线。不能简单把两个配置的吞吐差解释为纯索引成本，因为缓存容量、探测窗口和内存布局同时改变。追加 HEAD 同缓存配置对照，并记录新发现的构建变量：原 HEAD 二进制使用 jemalloc，当前构建使用 libc；跨版本差距不能仅归因于 WARM 分配策略，后续应控制分配器。

HEAD 同大缓存配置的有效重跑 `perf/warm_head_allcache_release_p3_s3_20261016_retry` 为 8,121,487.87 QPS、单核 1,402,578.28，错误及物化失败均为零。首次补建在 jemalloc 生成头文件前编译了对象，启动时出现指针截断及 Lua 初始化崩溃，没有进入 workload，已排除；独立构建生成依赖后完整重编译对象，启动与压测通过。正式二进制未替换。该轮 HEAD 仍使用其原有 jemalloc，尚不能排除当前 libc 构建差异。

### 10.16 当前版本改用 jemalloc 的控制实验

用户指出应统一分配器，因此另建 `/tmp/hpc-warm-current-jemalloc-allcache-20261016`，在当前版本同一大缓存源码上显式 `MALLOC=jemalloc` 构建；通过 `.make-settings` 与二进制 `je_malloc` 符号确认生效。保留各自其余构建选项、统一槽池和完整 slot 校验，关闭诊断，继续使用 100K Uniform、3:3、PROFILE=0、四事件 PMU。分配器依赖在独立目录构建，不替换正式二进制。

| 版本 | 分配器 | 位置缓存 | 总 QPS | 单核 QPS |
| --- | --- | --- | ---: | ---: |
| HEAD | jemalloc | 1,048,576 项 / 64 次探测 | 8,121,487.87 | 1,402,578.28 |
| 当前 | libc | 1,048,576 项 / 64 次探测 | 7,086,705.33 | 1,239,155.03 |
| 当前 | jemalloc | 1,048,576 项 / 64 次探测 | 6,956,211.19 | 1,230,660.92 |
| 当前 | jemalloc | 原始 131,072 项 / 4 次探测 | 6,692,304.10 | 1,167,044.00 |

当前 jemalloc 大缓存轮为 `perf/warm_current_jemalloc_allcache_p3_s3_20261016`，错误、notfound、物化失败及 unmatched 均为零。SNW 为 901.06 cycles/op、777.21 instructions/op；与 libc 的 905.45/768.81 接近，未显示吞吐恢复。全命中计数只在 10.15 的 libc 诊断轮实测；jemalloc 轮保留相同缓存容量/探测/键集合，但未采 hit/miss，不能将零回退计数冒充为该轮的直接观测。以上单轮数据不足以认定 jemalloc 导致下降，但说明仅替换分配器尚未消除差距。仍需检查其余构建选项和实际执行路径，不能认定跨版本对照已只剩分配策略一个变量。

另建 `/tmp/hpc-warm-current-jemalloc-default-20261016`，覆盖为本地原始缓存配置源码，显式 MALLOC=jemalloc，重跑得到表中第四行。目录为 `perf/warm_current_jemalloc_default_p3_s3_20261016`，错误、notfound、物化失败及 unmatched 均为零。此轮也未恢复基线，因此不能把大缓存作为 jemalloc 无收益的唯一解释。两份当前版本二进制均保留 je_malloc 符号，正式服务器未替换。默认缓存配置在 111 显式使用 USE_JEMALLOC、生成头文件和同一静态 jemalloc 库构建 TLC UT，结果 `vemb_v16_tlc_ut: all tests passed`；日志 `/tmp/hpc-current-jemalloc-tlc-ut{,-build}.log`。脚本语法与本地 diff 检查通过。

构建一致性说明：Linux Makefile 默认选择 jemalloc，但会读取既有 `.make-settings`；此前当前版本目录保存了 MALLOC=libc，而 HEAD 目录保存了 MALLOC=jemalloc。不能只看 Linux 默认值推断实际分配器。后续 A/B 应显式传入分配器并记录最终编译配置及二进制符号，避免沿用目录残留配置。

### 10.17 用户复测：CLIENTS=3 的 3:3 表现

用户使用正式当前版本、原始位置缓存运行 `NUM_KEYS=100000 KEY_PATTERN=R:R THREADS=64 CLIENTS=3 PIO=3 SNW=3 BUILD=skip KILL_OPENCODE=1 bash scripts/run_aeron_cross_node_flamegraph.sh`，目录 `perf/p3_s3_svr0-15_uniform_20261016_003540`。结果 7,824,892.74 QPS、单核 1,337,699.41，平均延迟 0.62276ms、P99 0.887ms，错误、notfound、物化失败和 unmatched 均为零。

已读取该轮 server/client meta：正式服务端 artifact 哈希 c0dcf717ce97e23a6fad030cfa627e4837e48fdc2bf3067026b36fd7d2779b4f，SDK 哈希 52aa34a7f876e46a534573b1b78a3adee3503dbce06e3ba51b8d1d0634d991f5，memtier 哈希 3356fa2c9492f2689a046e88717da336506ac3bd5f9d925e8bee4f85c3aa59bf，均与 10.9 当前 3:3 的 CLIENTS=4 对照相同。该轮并非使用 jemalloc 或大缓存实验二进制。PROFILE 均为 1，线程数、CPU 绑定、数据量、pipeline/batch 相同。两轮 KILL_OPENCODE 取值不同，负载门禁均通过，尚未通过相邻重复对照排除时段差异。

CLIENTS 是每个 memtier 线程的连接数，因此总连接从 256 降至 192，名义在途上限从 8192 降至 6144。相对于 10.9 当前 CLIENTS=4 的 7,009,247.09 / 1,214,217.38，总 QPS 提高约 11.64%、单核提高约 10.17%。相比此前 HEAD CLIENTS=4 的 8,032,686.19 / 1,390,954.58，仍低约 2.59% / 3.83%；这不是同 CLIENTS 的版本验收。

这轮说明同一当前二进制能在 3:3 下接近历史基线，连接并发及端到端交互应升为优先诊断项；不能仅将 3:3 的低吞吐归因于索引回退或分配器。`assign_channel_owners` 按 sequence 对 PIO/SNW 取模，3:3 仍为三组配对，减少连接同时减少每 worker 的 channel 扫描量和客户端事件循环连接数。192 可均分三组，但 256 也仅有一个连接的全局计数差，不能仅凭整除就宣称已找到根因，更不能假定并发创建时每个客户端线程恰好连接每组一次。下一项应对 HEAD/当前都采用 CLIENTS=3/4，串行交替重复，以中位数和完整 CPU/profile 数据比较，固定其余参数与分配器。

### 10.18 撤回 SVE 对齐前缀并复测

用户要求撤回 `sve_streaming_load_f32` 的源地址对齐前缀。仅删除 `src/sve_operation.c` 中这 16 行，使整个文件恢复为 HEAD 内容（SHA256 58c429dbe8ed2ed32dddbc67cff0a71c488fbe612e34a6dceaf0b8bc3891087a）；统一槽池、索引、原始位置缓存及 slot 校验均保持不变。该文件已同步 111/112，111 正式 Redis 二进制重建为 d190dca4032c2d1fef99328fae01d1fb83c2929f11cabce1c51ee5d2e7a02816，build stamp 写入并校验通过。旧二进制及 stamp 备份于 111 `/tmp/hpc-sve-prefix-rollback-20261016/`。

本次沿用正式当前版本的 libc、O3/LTO/SVE、内部 batch32，以隔离前缀撤回；没有同时切换分配器。SDK 实际使用 `clients/c/vemb_v16_client_sdk.c` 中独立的同名函数，该实现从未加入上述前缀，因此保持客户端二进制哈希 3356fa2c9492f2689a046e88717da336506ac3bd5f9d925e8bee4f85c3aa59bf 不变。此前 10.5 将该改动描述为客户端前缀优化，不能据此声称真实 SDK payload 路径已应用/验证该优化，现予以纠正。

111 真实 SVE 的复制测试通过 131,072 组，TLC UT 全通过；本地复制测试通过 131,072 组但只覆盖非 SVE fallback。111 日志在 `/tmp/hpc-sve-prefix-rollback-20261016/`。本地 `git diff --check` 通过。复测保持 PROFILE=1、THREADS=64、CLIENTS=4、PIO=3、SNW=3，分别使用 100K 和 10K Uniform；KILL_OPENCODE=0，沿用负载和设备门禁，保留其他进程。

| 数据集 | 前缀回退前（用户相邻记录）总 QPS / 单核 | 前缀回退后总 QPS / 单核 |
| --- | ---: | ---: |
| 100K | 7,011,372.59 / 1,219,844.14 | 7,570,535.58 / 1,297,573.72 |
| 10K | 7,949,364.39 / 1,507,304.83 | 8,156,480.56 / 1,524,964.03 |

100K 结果来自 `perf/warm_sve_prefix_rollback_100k_p3_s3_20261016`，相对用户回退前记录总吞吐高约 7.98%、单核高约 6.37%；10K 来自 `perf/warm_sve_prefix_rollback_10k_p3_s3_20261016`，总吞吐高约 2.61%、单核高约 1.17%。两轮错误、notfound、物化失败、unmatched 均为零，保存完整 server/client 火焰图；服务端 artifact 均为上述 d190dca4，CLI artifact 均保持 3356fa2c。两轮均已清理测试服务器。保留前缀回退，但仅各一轮且时段不同，不将差值全部归为该代码的稳定净收益；100K 尚未恢复到 8M 基线。本次未重跑全部 HA/UB 集成套件，不将相关历史验证作为本次重跑结果。

### 10.19 “全部命中位置缓存”实验：差距不只来自 cache miss

为验证 7.5M 与 HEAD 约 8.0M 的差距是否主要由位置缓存未命中造成，使用同一套 100K Uniform、3:3、64 threads、pipeline/batch 32 的对照 artifact，比较原始缓存和扩大缓存两组。两组均关闭 lookup diagnostics；`PROFILE=1` 只表示 perf 采样。

| 版本 | 位置缓存配置 | 总 QPS | 单核 QPS |
| --- | --- | ---: | ---: |
| HEAD | 131,072 项 / 4 次探测 | 8,032,686.19 | 1,390,954.58 |
| 当前 | 131,072 项 / 4 次探测 | 7,009,247.09 | 1,214,217.38 |
| HEAD | 1,048,576 项 / 64 次探测 | 8,121,487.87 | 1,402,578.28 |
| 当前 | 1,048,576 项 / 64 次探测 | 7,086,705.33 | 1,239,155.03 |

原始缓存对照分别来自 `perf/warm_compare_head_p3_s3_20261015_2304` 和 `perf/warm_compare_current_p3_s3_20261015_2304`；扩大缓存对照来自 `perf/warm_head_allcache_release_p3_s3_20261016` 和 `perf/warm_allcache_release_p3_s3_20261016`。扩大 current 缓存后仅提高约 1.1%，仍比扩大缓存 HEAD 低约 12.7%；因此不能把剩余差距归因于位置缓存容量不足，也不能通过简单增加 probe 次数恢复 8M。

该实验同时确认：当前火焰图中出现 `location_cache_validate_location`，并不等于所有性能差距都来自该函数。缓存扩大后该路径的触发概率下降，但动态 key metadata 查找、location 校验、payload 读取和 SN worker 调度的总成本仍然存在。后续应继续使用诊断构建拆分这些阶段的耗时，优先验证动态索引查找和 slot 校验；不要再重复“只扩大 cache”或“只增加 probe”实验，除非同时改变了 workload、allocator 或编译参数。

### 10.20 无 SVE prefix 版本重新验证大位置缓存

上节的 all-cache 对照使用的是回退前的 current 二进制，不能代表当前已经删除 SVE prefix 的 7.5M 版本。此次保持 `src/sve_operation.c` 与 HEAD 完全一致，只在 111 临时将 `TLC_CORE_DEFAULT_HOT_CAPACITY` 从 `65536` 改为 `1048576`，并强制删除 `tlc_core.o`/`vemb_v16_tlc.o` 后重编，避免 Makefile 未追踪头文件变化。注意该宏是容量目标，`location_cache_init()` 会将其乘 2 后向上取 2 的幂；因此默认宏 65,536 实际分配 131,072 项，本次宏值 1,048,576 实际分配 2,097,152 项。客户端、SVE 代码、allocator、O3/LTO、内部 batch32、100K Uniform、3:3、CLIENTS=4 均保持不变，关闭 lookup diagnostics，PROFILE=1 仅用于 perf 采样。

第一次未强制重编的结果为 `7.798M / 1.336M`，其 server hash 仍是正式 65,536 cache 二进制，因此判定为无效，不纳入结论。强制重编后 server hash 为 `2c73e20c...`，有效结果如下：

| 版本 | cache 容量 | 总 QPS | 单核 QPS |
| --- | ---: | ---: | ---: |
| 无 prefix，原始配置 | 65,536 | 约 7.57M | 约 1.298M |
| 无 prefix，大 cache | 2,097,152 | 7,860,883.55 | 1,347,789.91 |
| HEAD 对照 | 131,072 | 8,032,686.19 | 1,390,954.58 |

有效 artifact 为 `perf/warm_noprefix_allcache_forced_100k_p3_s3_20261016`，错误、notfound、物化失败和 unmatched 均为零。大 cache 相比无 prefix 的原始配置提升约 3.8%（与不同时间窗口的 7.57M 结果相比），但仍比 HEAD 低约 2.1%；单核 QPS 仍低约 3.1%。因此，在当前无 prefix 版本上，扩大位置缓存确实能恢复一部分吞吐，但不能完全解释或消除与 HEAD 的差距。正式 111 服务端已恢复 `TLC_CORE_DEFAULT_HOT_CAPACITY=65536` 并重编，实验配置没有留在正式构建中。

本轮结论：位置缓存容量是一个真实但次要的影响因素；后续应重点拆分动态 key metadata、location 校验和 SN worker 调度成本，而不是继续单独扩大 cache。该实验不应重复，除非同时更换 allocator、连接数或编译参数并重新建立完整 A/B。

### 10.21 位置缓存全命中计数复核与 hash table 回退计划

对无 prefix、大 cache 版本尝试开启 `VEMB_V16_LOOKUP_DIAGNOSTICS` 以直接读取 `cache_hit/cache_miss/meta_finds/meta_probes`。该轮 QPS 为 7,723,388.24 / 单核 1,326,345.98，错误、notfound、物化失败和 unmatched 均为零；但 artifact 中只确认了 `lookup_diag` 字符串，未出现 proxy worker 的 `pipeline_diag`/`lookup_diag` 汇总日志，说明该构建宏没有完整覆盖到实际 worker 编译单元，不能据此证明全部请求命中 location cache。该实验结果仅作构建诊断失败记录，不作为性能结论。正式服务端已恢复无 diagnostics、65,536 宏配置。

下一项应做两个独立实验：

1. 在确认 diagnostics 通过实际 worker 日志或 shutdown stats 输出后，验证大 cache 轮是否仍发生 `key_meta_shard` 查找。
2. 单独实现一个临时 legacy key metadata layout：恢复 HEAD 的每 shard 固定容量、hash probe 直接落在 entries 槽位的布局，保持当前统一 WARM 槽池、slot 校验和协议不变，再与当前动态 `hash_table -> ordinal -> entries` 布局对照。

第二项不能直接把 HEAD 的整个 `tlc_core.c` 覆盖回来，因为当前迁移分页依赖 dense ordinal 稳定性，且 HEAD 没有当前的 region free pool/import index。实验必须隔离 key metadata layout，并在完成后恢复正式源码；如果 legacy layout 能恢复吞吐，再进一步决定是否用“稳定 ordinal + 读侧紧凑索引”替代完整回退。

### 10.22 独立目录 legacy metadata layout 对照

按上述计划在 111 创建了独立测试目录 `/root/szz/codespace/hpc-redis-legacy-meta-20261016`。该目录复用当前源码树的硬链接，单独重建 `output`；只修改实验目录的 `src/tlc_core.c`，正式目录 `/root/szz/codespace/hpc-redis` 未改动。

实验 layout 保留当前统一 WARM 槽池、import index、location 校验和协议，只把 key metadata 改为 HEAD 风格的 direct open addressing：每个 shard 预分配固定容量，`hash probe` 直接访问 `entries[pos]`，去掉 `hash_table[pos] -> ordinal -> entries[ordinal]` 间接层。为保证 100K 读压测预填充成功，实验表按 WARM 容量计算为每 shard 约 1024 个槽；该实验不包含动态扩容和迁移分页验收，不能作为可合并实现。

| 版本 | key metadata layout | 总 QPS | 单核 QPS |
| --- | --- | ---: | ---: |
| 当前无 prefix | 动态 `hash_table -> ordinal -> entries` | 约 7.57M | 约 1.298M |
| legacy 实验目录 | 固定 direct `entries[pos]` | 7,656,800.02 | 1,311,898.08 |
| HEAD | 固定 direct layout | 8,032,686.19 | 1,390,954.58 |

legacy 结果 artifact 为 `perf/warm_legacy_meta_100k_p3_s3_20261016`，错误、notfound、物化失败和 unmatched 均为零。相对当前无 prefix 结果只提升约 1% 左右，仍比 HEAD 低约 4.7%；因此 `hash_table -> ordinal` 间接访问确实可能有小幅成本，但不是当前 0.5M 差距的唯一主因。实验目录保留用于后续 migration/cursor 修复验证，正式服务端继续使用动态 metadata layout。

### 10.23 UB payload 地址对齐是下一优先级

代码核对确认当前 UB payload 的物理地址为：

```text
payload_addr = mapped_addr + local_slot * value_size
```

layout header 和 `slot_meta[]` 区域按 64B 对齐，UB payload 基址也按 64B 对齐；但当前 manifest 的 `value_size=1200`，不是 64B 的整数倍。因此 slot 起始地址的相位按 1200 mod 64 = 48 循环，只有每 4 个 slot 中约 1 个从 64B cacheline 边界开始。`warm_place_in_region()` 仅把 hash 起点优先选到这些 slot，无法在高占用时保证最终分配仍然对齐。

这可能解释当前设计与 HEAD 之间的剩余读取差距：SVE 非对齐 load 在语义上正确，但 UB cacheline 访问可能产生跨 cacheline 请求、额外事务或更差的预取行为。当前还没有把“slot 地址相位”和“单次 UB payload copy 耗时”关联起来，因此不能直接下结论。

下一步实验不改现有正式 region，使用独立 manifest/UB 区域比较：

1. 在 `tlc_core_copy_warm_location_value()` 统计 payload 地址 `addr & 63` 的分布，并按相位统计 copy 时间。
2. A 组保持 `slot_stride=1200`，B 组使用 `slot_stride=round_up(1200,64)=1216`，两组保持相同 key、连接数、batch、allocator 和 SVE 代码。
3. B 组的 `capacity_slots` 必须按 `region_bytes / 1216` 计算，manifest 需要携带布局版本或 `slot_alignment=64`；不能把已有 1200B 紧密布局的 region 直接按 1216B 解释。
4. 在 25%、75%、90%、98% 占用率下比较总 QPS、单核 QPS、UB copy ns/op、cacheline 相位和实际槽容量。

若 B 组提升接近 0.5M 且容量损失约 1.32% 可接受，再考虑把 `slot_stride_size` 作为 manifest 归一化字段落地；在实验完成前不修改现有 1200B region 的协议或生产布局。

补充边界：当前跨机脚本使用的 warm manifest 为 4GiB、`value_size=1200`，紧密布局约有 3,579,139 个 slot，其中理论 64B 对齐 slot 约 894,785 个。100K workload 只占约 2.79%，因此即使 hash 分布理想，优先对齐槽尚未整体耗尽；该场景下对齐问题主要来自 hash 碰撞后落入普通槽的少量 key。要验证它是否能解释 0.5M 差距，必须增加 90%/98% 占用率测试，不能只重跑当前 100K 稀疏读测试。

### 10.24 地址相位诊断接入复核

按 10.23 的第一步尝试将 payload 相位和 copy 耗时接入诊断。原有 `VEMB_V16_LOOKUP_DIAGNOSTICS` 使用线程局部计数，并依赖 proxy worker 的 `pipeline_diag` 周期日志；在正式跨机 worker 路径中没有产生该日志，因此不能用它确认 cache 命中或 payload 地址分布。一次临时全局计数接入在远端头文件保护差异处停止，未进入正式源码；`tlc_core.c` 随即从备份恢复，远端重新以无 diagnostics 宏、`TLC_CORE_DEFAULT_HOT_CAPACITY=65536` 和无 SVE prefix 参数构建，检查通过。

随后用正式二进制做了 1,000-key、1:1、PIO/SNW=1 的短启动探针，结果全部请求成功（`status_err=0`、`materialized_fail=0`、`unmatched=0`）。再通过临时脚本副本将 server 日志级别提高到 `notice`，读取到正式 4GiB manifest 的 payload `mapped_addr=0xfffe8e1ff100`，其低 6 位为 0；`value_size=1200`、`capacity_slots=3579139` 与 manifest 计算一致。因此 UB 映射基址本身满足 64B 对齐，运行时 slot 地址仍按 `0 + local_slot * 1200 (mod 64)` 产生 `0/48/32/16` 四相位。该结果排除了“整个 UB 映射基址未对齐”，但尚未测得各相位实际访问比例及 copy ns/op；0.5M 差距不能仅凭基址对齐归因。

随后尝试把全局 payload 计数编译进远端 Redis，并在每 1,048,576 次 copy 时输出。临时二进制包含 `payload_diag` 符号，但跨机脚本归档的 server artifact 仍标记为旧的正式 hash，运行日志也没有对应计数行，因此该轮不能作为运行时统计结果。临时改动已全部恢复，正式远端重新以无 diagnostics 宏构建；本次没有修改生产读取路径。后续若继续做计时，应先让脚本的 build stamp/hash 与实际启动的 `src/redis-server` 强一致，再进行 A/B。

### 10.25 Proxy CPU 优化候选

用户希望在暂不追逐 0.5M UB 读取差距的前提下降低 proxy CPU。对 7.57M 轮 `perf/warm_sve_prefix_rollback_100k_p3_s3_20261016/server/server.collapsed.txt` 与 HEAD 火焰图对照后，当前 proxy 的主要热点仍是 `proxy_io_pool_thread_main`，其中 request poll、completion drain 和 batch response publish 占主要采样。当前 proxy 源码与 HEAD 基本一致，因此这不是统一 WARM 槽池直接引入的额外 proxy 分支。

候选按风险和收益排序如下：

1. **Response batch 直接编码到 response arena。** 当前路径先调用 `batch_response_encode()` 写入栈上临时 buffer，再由 `batch_arena_publish()` 将完整 frame 复制到 response arena。可以拆出“预留 arena 空间”和“发布 descriptor/commit marker”两个阶段，直接把 response 编码到目标 arena，减少一次 frame copy。必须保持 frame 连续、ring full 回退、commit marker 最后写入以及 descriptor release/acquire 顺序。
2. **一次 channel 访问连续处理有限数量的 request batch。** 当前 IO worker 每轮完成 channel acquire、completion drain、一次 request poll、channel release。可以评估在一次 channel 访问中连续处理最多 2 个 batch，再切换到下一个 channel，以摊薄 channel 生命周期原子操作和 snapshot 遍历成本。必须设置上限，避免热点 channel 长时间占用 worker。
3. **减少稳定 channel 的生命周期检查。** 对 worker-owned 的稳定 channel 使用静态工作列表，只在创建、关闭和迁移时更新动态状态，减少正常轮询中的 active/acquire/release 检查。该方案涉及 channel 生命周期和 HA 关闭路径，风险高于前两项。

不优先修改 request decode：batch v2 已经直接从 request arena 解码，收益预计有限。`location_cache_validate_location` 属于 SuperNode/TLC 路径，也不应归入 proxy 优化。后续实验应先单独验证候选 1，再验证候选 2；每项都使用同一 client、同一 workload、同一 PIO/SNW 配置，并记录 proxy CPU、总 QPS、单核 QPS、ring full 和错误计数。当前仅完成设计记录，没有保留未确认的 proxy 代码改动。

### 10.26 当前实现中会扫描整个 WARM region 的路径

当前代码确实存在整 region 扫描，主要有以下几类，影响范围不同：

1. **读路径的兼容性发现扫描。** `warm_lookup_region()` 先调用 `warm_lookup_imported()`；当 import index 没有命中后，会执行 `for (slot = 0; slot < region->capacity_slots; slot++)`，逐个读取 slot metadata，寻找匹配的 `key_hash`/fingerprint。这个路径会在位置缓存未命中、key metadata 没找到、随后进入 `warm_lookup()`，并且目标 region 的 import index 未建立或未命中时触发。若扫描期间观察到 BUSY，还会按 `TLC_CORE_WARM_BUSY_RETRIES` 重复整段扫描。对当前 4GiB、1200B manifest，这是一轮约 3,579,139 个 slot 的 metadata 扫描，属于不合理的请求级 fallback。

   正常写入且 key metadata 和 location cache 都有效时不会进入该路径；因此它未必出现在稳定的全命中火焰图中，但只要发生 cache/index miss、共享 region 未建立 import index、或旧数据无法导入 index，就可能产生长尾甚至吞吐骤降。

2. **新数据分配的 free bitmap 全 region 搜索。** `warm_place_in_region()` 先从 hash 对应的 preferred word 开始，然后遍历 `free_word_count`，直到找到可用 bit。preferred word 没有空闲位、发生并发争抢、或 region 接近满载时，单次写入可能遍历整个 free bitmap；如果 region 已满，还会完整确认没有可用 slot 后才切换到下一个 region。这里扫描的是 bitmap word，不是 1200B payload，但 4GiB region 仍约有 55,924 个 word，写入高峰时会产生明显 CPU 成本。

3. **初始化和恢复阶段的全量扫描。** `warm_region_reset_free_pool()` 按 slot 扫描整个 region 重建 free bitmap；`warm_region_import_index_init()` 也按 slot 扫描整个 region，重建 imported index。这两类扫描发生在 attach/reset/restart，不直接消耗稳定读请求 CPU，但会造成启动或 HA 恢复延迟。`warm_region_used_slots()` 在统计 region full/used 状态时同样按 slot 扫描，属于管理面成本。

其中第一类是当前设计最不合理的地方：已经引入 key metadata、location cache 和 imported index 后，读路径仍保留“没有索引就扫整个物理 region”的兼容性兜底，索引失效时的代价从 O(1) 退化为 O(capacity)。这也解释了为什么不能仅凭稳定 cache-hit 火焰图排除 region scan：只要少量 miss 或共享 region index 缺失，就会产生极重的异常路径。

后续应把读路径改成明确的索引契约：本地写入由 key metadata 提供唯一定位，已 attach 的共享 region 必须在建立服务前完成 import index；运行时 index miss 直接返回 MISS 或触发受控的异步重建，不允许在请求线程扫描整个 region。写入侧则应增加每 region 的 O(1) 空闲 word 提示/分层 free pool，使“找不到空闲位”能够快速判断，避免接近满载时反复遍历 bitmap。初始化阶段的全量扫描可以保留为显式恢复操作，但不能复用为在线请求 fallback。

### 10.27 对 imported region 适用范围的校正

前一节把“imported region 必须在 attach 时建立索引”写成了常规运行前提，需要按当前部署拓扑收窄：

- 正常节点只 attach 自己负责的本地 WARM region。启动时本地 UB region 通常由 manifest reset/初始化，业务 key metadata 由本地写入或 COLD 恢复建立，不需要通过扫描旧 payload 反推 key 到 slot 的映射。
- 远端 region 的正常访问路径是 remote metadata 与 UB RPC 返回 handle，client 根据 handle 读取对应的远端映射；不会把远端 payload 当成本节点的普通 WARM region 再做物理扫描。
- 代码确实支持通过 peer-view 的 `ATTACH_NOW` 把一个 peer region 动态 attach 到本地 runtime region 列表，但这是显式的通用能力，不等于 HA 自动接管远端 region。当前两节点测试拓扑也不把它作为常规读路径。

因此，在当前实际场景中，`warm_region_import_index_init()` 和 `warm_lookup_region()` 的 imported-region 兼容逻辑大概率不是必要的数据路径。需要进一步确认所有生产调用点后，优先考虑删除或隔离 `warm_lookup_region()` 的整 region fallback：key metadata 缺失就返回 MISS，远端 key 通过 remote metadata/UB RPC 获取，只有显式启用 peer-region attach 的恢复场景才允许建立 imported index。这样可以从设计上消除在线请求扫描整个 region 的可能性。

### 10.28 删除 imported-region 在线发现设计

按 10.27 的结论，当前实现已删除 imported-region 的在线发现设计。`tlc_core.c` 不再维护 `import_entries`，attach 时不再调用 `warm_region_import_index_init()`，也不再保留 `warm_lookup_imported()`、`warm_lookup_region()` 和 `warm_lookup()` 的物理 region 扫描路径。`tlc_core_get_warm_location_raw()` 在 location cache 未命中后只查询 key metadata；metadata 不存在或位置校验失败即返回 MISS。写入侧对没有有效 metadata location 的 tombstone 直接走新 slot 分配，不再扫描旧 payload 试图复用位置。

显式 peer-view `ATTACH_NOW` 仍可把 region 映射到 runtime region 列表，但它不再自动建立 payload 反查索引；该能力必须由上层同步提供 key metadata/handle。这样在线请求不会因为 metadata miss 退化为 O(region_capacity) 扫描。远端构建和测试进展见 10.29；删除在线扫描不等于 COLD 恢复流程已经适配完成。

### 10.29 删除 imported 逻辑后的远端同步与 3:3 复测（2026-10-16）

两台 UB 机器 `/root/szz/codespace/hpc-redis` 已同步当前代码。按本地工作树的 68 个修改/新增代码及脚本文件，加上 `src/sve_operation.c`，逐文件计算 SHA256，与 111、112 对比，69 个文件均一致。`tlc_core.c` SHA256 为 `df765dbcc59f928b441995a0013bec8b967a4e9738185dfe4c83d8d169005ffb`。没有恢复 SVE prefix，也没有实施 10.25 的 proxy 优化。

111 重新构建服务端：`MALLOC=libc USE_SVE=yes OPT="-O3 -flto -fno-omit-frame-pointer"`，三个 proxy batch 均为 32，lookup/payload diagnostics 关闭。构建设置仍包含历史参数 `REDIS_CFLAGS=-DTLC_CORE_ENABLE_WARM_LOCATION_VERIFY=0`，但当前 `tlc_core.c/.h` 已无该宏的条件分支，不能把这个参数解释为本轮额外删除了 location 校验。补齐历史诊断文件差异后，服务端二进制 SHA256 为 `280a63b983571e46601a52083775655b8cf6176c6f38adf36473de8ac83509f8`，已更新 build stamp。112 压测 client 二进制继续使用 `3356fa2c9492f2689a046e88717da336506ac3bd5f9d925e8bee4f85c3aa59bf`。

最初一次启动被已有 Mooncake/store workload 的负载门禁阻止；保留其进程，待其自行结束后重试，没有放宽设备或负载门禁。第一次成功运行 `p3_s3_c3_uniform_remove_import_retry_20261016` 得到总 QPS `8,049,959.60`、单核 QPS `1,368,089.60`。该轮 `tlc_core.c` 已更新，但完整源码复核发现几份历史诊断文件尚有差异，随后补齐并重新构建，因此最终同步版本的验收采用下述复测结果。

最终同步版复测的公共参数为 `NUM_KEYS=100000 KEY_PATTERN=R:R THREADS=64 PIO=3 SNW=3 BUILD=skip PROFILE=1 KILL_OPENCODE=0 KILL_MUTAGEN=0`，dim=300、pipeline=32、batch=32、SDK L1 cache 关闭。`BUILD=skip` 使用上述刚完成构建的二进制；CLIENTS=3 轮运行时读取 `/proc/583789/exe` 并计算 SHA256，确认实际启动程序与 `280a63b9...` 完全一致。

| CLIENTS | 总 QPS | 单核 QPS | 平均延迟 ms | p99 ms | artifact |
| ---: | ---: | ---: | ---: | ---: | --- |
| 3 | 8,049,552.10 | 1,370,935.75 | 0.60290 | 0.863 | `perf/p3_s3_c3_uniform_remove_import_fullsync_20261016` |
| 4 | 7,548,570.77 | 1,292,924.80 | 0.86577 | 1.183 | `perf/p3_s3_c4_uniform_remove_import_fullsync_20261016` |

CLIENTS=3 轮 `status_notfound/status_err/materialized_fail/unmatched` 均为 0，成功物化 245,034,435 次。按现有脚本，单核口径为 `QPS / (Redis process user+system core_equiv + CPU-set softirq core_equiv)`，对应 `5.856275 + 0.015300` 核；不直接使用 CPU-set 的 `cpu_total` 作为分母。CLIENTS=3 与之前约 7.57M 的 CLIENTS=4 并发不同，不能直接据此认定删除 imported 逻辑消除了 0.5M 差距。

CLIENTS=4 轮上述四项失败计数同样全为 0，成功物化 222,759,161 次。该轮总 QPS 比之前相同 CLIENTS=4 的无 prefix 结果 `7,570,535.58` 低约 0.29%，单核 QPS 比 `1,297,573.72` 低约 0.36%；单次结果不足以区分此幅度的差异与运行波动。当前证据支持“删除 imported 兼容逻辑后，100K 稳定读吞吐未出现明显变化”，不能宣称 CLIENTS=4 已恢复 HEAD 的约 8M。相同最终二进制下 CLIENTS 从 3 增加到 4，总 QPS 下降约 6.22%，单核下降约 5.69%；原因仍需单独诊断，不能仅凭这两轮断言由 client 或 UB 引起。两轮 server/client 火焰图均已归档，测试服务端已停止。

相关 UT 的结果：`vemb_v16_manifest_ut` 通过；`vemb_v16_tlc_ut` 在 `test_persistent_cold_write_order` 第 625 行失败。测试重建 TLC 实例时复用仍有旧 slot 的单槽 region，COLD checkpoint 回放需要重新分配 slot，但旧 slot 仍占用，导致恢复失败。这是删除 imported 逻辑后暴露的实际恢复回归，不能归因于缺少 UB/HA 环境。后续必须在明确 region 所有权和恢复生命周期后适配 COLD 重建，并验证 tombstone、满槽复用、共享 region 测试；不得通过恢复在线全 region 扫描来掩盖问题。本节只记录性能复测，不代表全部 UT 通过。

### 10.30 allocator v2 的单 owner 范围

`dev_alloc` 工作树已保存为 `9547218`，新分支 `dev_alloc_v2` 先提交设计文档 `239e2c2`，再实施新分配器。新的 [设计及验证记录](VEMB_V16_WARM_ALLOC_V2_DESIGN.md) 明确一个 region 只有所属 HPC-Redis 可分配、修改和回收，其他 HPC-Redis 只读；不实现多 allocator writer 的共享控制状态。hash bitmap/vnode 选择由 next_slot 和本地回收栈替代，非所属 region 不再参与写入 fallback。动态 key metadata、manifest 容量推导、generation/seqlock 与迁移 fence 保留。

本阶段 SDK 保持原有 UB 直接复制路径；按用户确认，并发回收/复用时的完整 payload 读取保证留到下一阶段，不纳入 allocator v2 验收。详见新设计的删除与回收章节。

### 10.31 allocator v2 自动64B物理stride

在 `dev_alloc_v2` 实施 `slot_stride_size=align_up(manifest.value_size,64)`。容量、metadata布局、reset/attach、offset与payload写地址统一使用物理stride；协议bytes保持逻辑payload长度，SDK/SVE不变。共享layout升为版本2，旧版和尺寸不一致的attach失败；只有owner可初始化空白header。设计、布局升级约束、UT和原参数实验详见 [allocator v2 阶段二记录](VEMB_V16_WARM_ALLOC_V2_DESIGN.md)。

111独立目录9个相关UT全部通过；原100K、dim300、64threads、3:3、CLIENTS=3、PROFILE=1读压测达到8,448,774.78总QPS、1,458,593.30单核QPS，四项失败计数全为0；artifacts `perf/p3_s3_c3_uniform_alloc_v2_stride64_20261016`。相比阶段一未对齐顺序分配的3.863M/0.874M，当前证据支持物理stride对齐消除了大幅读取回归，不能据单轮声称稳定提升幅度或满载性能通过。SDK并发回收读取仍不在验收范围。

CLIENTS=4在两次负载门禁中止后通过空闲窗口复测：7,756,944.18总QPS、1,371,439.27单核QPS，平均/p99为0.92507/1.103ms，四项失败计数全为0。artifacts `perf/p3_s3_c4_uniform_alloc_v2_stride64_idle_20261016`。C4仍低于本阶段C3，不将对齐修复解释为所有并发参数均达到8M。

同参数、同SDK/memtier二进制的相邻旧分配器C4对照为7,466,836.97总QPS、1,278,176.20单核QPS，四项失败计数全为0；artifact `perf/p3_s3_c4_uniform_alloc_v2_stride64_control_20261016`。本轮v2对齐版总QPS高约3.9%、单核高约7.3%，支持没有吞吐劣化；单轮不作为稳定提升幅度结论，且未执行90%/98%占用场景。

2026-10-16的1M key缓存容量诊断及20:24跨机测试门禁结果见 [allocator v2诊断记录](VEMB_V16_WARM_ALLOC_V2_DESIGN.md)。ARM CPU-only lookup默认缓存约495ns，大缓存约223ns；跨机A/B首轮被111 Mooncake进程RSS门禁拦截，尚无新QPS结果，不能据微基准归因全部吞吐差异。

同日20:26—20:31，用户授权停止该Mooncake进程后完成三组跨机测试：1M Uniform默认位置缓存5.410M/单核0.691M，独立大缓存构建7.878M/1.006M（+45.6%/+45.5%），大缓存Zipf1.5为9.451M/1.604M，四项失败计数均0。Zipf fanout为1.992，包含请求合并收益，不能独立归因UB缓存。完整参数、构建哈希、artifacts及证据限制已记入上述v2文档的“1M key 跨机缓存A/B结果”。

同日PROFILE=1的cycles与LLC miss对照显示，大缓存下1M key的`location_cache_validate_location` cycles self占比15.38%，100K为7.49%；LLC miss采样栈归因占比为31.91%/17.83%。`key_meta_find_locked` cycles占比不足1%，故主要剩余热点转为每次cache hit仍访问随机slot_meta验证。详细火焰图、client端路由候选和限制见allocator v2文档“cycles 与 LLC miss 火焰图”。

SPE 地址样本复核显示，SNW 的 `tlc_core_get_warm_location_raw`、`location_cache_validate_location` 在 `l1d-miss`/`llc-miss` 样本中出现 `RAM hit + Walker miss`。这支持 warm location/slot 校验路径承受地址转换与远端取数等待，但归档的 data object 地址与当前保存的 `/dev/obmm_shmdev4` 映射边界不属于同一地址空间，因此没有做 metadata/payload 比例归类。下一轮必须和同一 perf.data 同时保存 Redis 的 maps/smaps 后再分类；在此之前不据 SPE 地址样本修改校验协议。

2026-10-17 已完成同 PID 绑定复测：服务端 PID `853754` 同时保存 `maps/smaps` 并采集 SPE，WARM 映射为 `0xfffe89b8b000-0xffff97600000`，metadata 结束地址为 `0xfffe9731f380`；`smaps` 报告 CPU 侧 `4 kB` 页。该轮1M/4:4诊断QPS为4.841M，四项错误计数为0；SPE仍显示 slot 校验路径 `RAM hit + Walker miss`，但 ARM perf 当前输出未稳定展开 data object，metadata/payload比例待离线解析，不据此修改协议。详见 [allocator v2诊断记录](VEMB_V16_WARM_ALLOC_V2_DESIGN.md) 的“同 PID 映射绑定复测”。

### 10.32 大缓存 100K/1M 的 L1、LLC 与 DTLB 计数

2026-10-16 使用同一大缓存服务端构建、相同4:4 Uniform workload，以`PROFILE=0 SERVER_PMU=1`按worker采集`cycles/instructions/L1-dcache-load-misses/LLC-load-misses/dTLB-load-misses/dtlb_walk`，稳定窗口内事件运行比例均为100%。1M相对100K的SNW计数/op：cycles `745.61→1,158.21`，instructions `852.03→686.27`，L1 miss `14.98→17.26`，LLC miss `2.95→6.58`，dTLB miss `3.83→6.02`，DTLB walk `2.49→4.88`；QPS `9.958M→7.943M`。PIO LLC miss/op为`0.610→0.544`，但PIO instructions/op `966→1,653`、cycles/op `924→1,158`，说明不能把全部吞吐损失只归因于SNW或slot_meta。结果支持工作集扩大后SNW访问等待及地址转换压力增大，但线程级PMU不能把计数精确归因到`slot_meta`的单条load。完整口径、正确性计数、限制与artifacts见[allocator v2诊断记录](VEMB_V16_WARM_ALLOC_V2_DESIGN.md)的“1M key 的 L1/LLC/DTLB per-worker PMU 对照”。

同构复测 `allocv2_next_100k_pmu_20261017` / `allocv2_next_1m_pmu_20261017` 使用同一大缓存构建和同一 4:4 参数，得到 `9.941M/1.376M` 与 `7.843M/1.004M` QPS/单核 QPS。1M 相对 100K 的 SNW cycles/op、LLC miss/op、DTLB walk/op 分别增加 56.0%、124.7%、56.3%，而 instructions/op 下降18.5%；两轮 WARM `smaps` 均为4KiB页，且启动日志确认 huge-TLB mmap 返回 `EINVAL` 后退回普通 mmap。该复测确认前述访存等待方向稳定，但下一步仍需对 metadata locality 或页表粒度做受控 A/B，不能据此直接修改 slot 校验。

随后对空闲 `/dev/obmm_shmdev4` 做 `MAP_HUGETLB` 能力探针：111 的2MiB hugepage池可用（`nr_hugepages=131072`、`free_hugepages=95487`），但 metadata/完整WARM的原始长度、2MiB向上取整长度以及显式 `MAP_HUGE_2MB` 均返回 `EINVAL`。因此应用侧 round-up 不能解决当前 huge-TLB 失败；页表粒度优化需要 OBMM 驱动/设备支持，近期优先级低于 metadata locality A/B。

随后做“1M resident、100K active-read”对照：prefill完整写入1M key，读阶段只访问前100K，得到 `9.975M/1.378M` QPS/单核 QPS，与100K resident/100K active-read的 `9.941M/1.376M` 基本一致；SNW LLC miss均约`2.95/op`。因此回归由实际访问的 metadata 工作集触发，而不是由WARM总容量或已分配payload数量触发。Artifact为`perf/allocv2_resident1m_read100k_20261017/`，下一步应围绕active-read路径的metadata locality做受控A/B。

### 10.33 最新 v2 100K 回归与 cache-hit 校验结论（2026-10-17）

最新边界校验同步并重建后，100K、4:4、Uniform 回归为 `10,014,372.69` QPS、`1,417,375.45` 单核 QPS，四项失败计数均为0，artifact 为 `perf/p4_s4_svr0-15_uniform_20261017_085717`。同一生产二进制的1M、4:4、Uniform profile 为 `9,332,084.94` QPS、`1,197,384.12` 单核 QPS；server 火焰图中 `location_cache_validate_location` 占 `17.92%` cycles。

临时启用已有 lookup 诊断后，1M workload 的4个SN worker合计约`118.0M`次lookup，cache hit约`117.98M`，miss约`19,949`，miss比例约`0.017%`。该诊断构建吞吐仅`3.746M`，不作为性能样本；它只证明热点来自命中后的slot metadata校验，而不是hash miss。

仅对4个SN worker采集PMU，L1 miss为`5.15%`，LLC miss为`68.18%`，dTLB miss为`1.59%`，`dtlb_walk`为`489,599,392`；原始计数见 `perf/p4_s4_1m_pmu_sn_target_20261017/` 和 `perf/p4_s4_1m_pmu_sn_dtlb_20261017/`。下一步应设计带明确失效通知的cache-hit快路径，保留payload复制阶段的generation/seqlock校验，不直接删除slot身份校验。
