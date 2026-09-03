# TLC HA Replica Descriptor Ring 与 Payload Arena 优化设计

## 结论

当前 `tlc_ha_replica` 的固定槽 ring 可以工作，且已通过 111/112 的直接 UB
复制与 Redis TCP/SDK 端到端回归。它不是本轮 HA 正确性问题的来源，因此本优化不
改变已验证的 COLD append、ACK、异步 apply、heartbeat 或恢复语义。

后续应将 UB Replica 数据面演进为：**可配置 SPSC descriptor ring + 循环 payload
arena**。一个 descriptor 指向 arena 中一帧完整的 Replica wire frame；不再为每个
frame 预留一个等大的 `slot_bytes` 槽。这样 ring 深度和单个 batch 最大字节数可以
独立配置，也消除了 `vemb_v16_client_ring` 当前固定 256 个槽对 Replica layout 的
限制。

这不是把 `replica_ring_publish()`/`replica_ring_poll()` 改调
`vemb_v16_client_publish()`/`vemb_v16_client_poll()` 的机械替换。当前 Replica TX
由多个逻辑调用方发送，且当前共享布局、初始化与跨机可见性契约和 client ring 不同；
这些边界必须先显式收敛。

## 当前基线

现有 UB Replica ring 位于 `src/tlc_ha_replica.c`，共享布局为：

```text
shared ring header
    -> slot[0] { sequence, payload[slot_bytes] }
    -> slot[1] { sequence, payload[slot_bytes] }
    -> ...
```

每个 slot 都带 sequence，`replica_ring_publish()` 用 claim/publish sequence，
`replica_ring_poll()` 用 reclaim sequence。其优点是固定布局直接、能串行化多个
producer 的 claim；代价是任一小 ACK、heartbeat 和大 EVENTS frame 都占用一个完整
`slot_bytes` 槽。

真实 Redis HA 服务当前配置为 `slot_count=8`、`slot_bytes=131072`、
`max_batch_events=32`、`max_batch_bytes=65536`。因此实际数据面不是固定 256 槽；
`slot_count` 已由 `tlc_ha_replica_ring_config_t` 配置。固定 256 槽来自
`src/vemb_v16_client_ring.h`：

```c
#define VEMB_V16_CLIENT_MAX_BATCH_PIPELINE 128u
#define VEMB_V16_CLIENT_RING_SIZE \
    (VEMB_V16_CLIENT_MAX_BATCH_PIPELINE * 2u)
```

`vemb_v16_client_ring_bytes()` 和 `vemb_v16_client_ring_init()` 都使用这个常量，
所以不能原样叠加到目前的 `8 x 128KiB` Replica 映射上。

`src/vemb_v16_batch_ring.h` 已实现“descriptor 指向 arena”的基本思路，但它编码的
是 VEMB request/response batch，且假定既有 client ring 的固定 layout。它可复用
其 producer reclaim、环绕和最终 commit 的方法，不能直接作为 Replica 传输协议。

## 优化目标与非目标

目标：

- descriptor 数量、arena 大小和最大 Replica frame 大小均由启动配置确定；
- 大小不一的 EVENTS、ACK、heartbeat 只消耗实际 arena 字节；
- frame 在 arena 内保持连续，Consumer 可以直接校验 wire header、payload 与 checksum；
- 保持当前一条 Node group、一对双向 ring、固定 path/offset 的部署模型；
- 继续支持 cacheable/non-cacheable UB mapping，并把可见性与提交顺序写入 layout
  契约；
- 新旧 ring layout 不互认，避免在已有共享区上误解释内存。

非目标：

- 不修改 Redis TCP/SDK、COLD AOF event、ACK、heartbeat 和 checkpoint/resync
  协议；
- 不在本优化中加入自动重连、切主、fencing 或跨 Node group 多路复用；
- 不把 client request/response 的现有 256 槽 ABI 变更为 Replica 的配置；
- 不把 Follower `applied_seq` 当作 durable ACK，也不改变当前 group commit 语义。

## 目标布局

每个方向仍使用一个独立映射区。布局版本提升为 `TLC_HA_REPLICA_RING_VERSION=2`：

```text
replica arena ring header
    magic / version / cache-policy-independent geometry
    descriptor_count / descriptor_mask / descriptor_stride
    descriptor_offset / arena_offset / arena_bytes
    producer head/tail state
    -> descriptor ring (fixed-size control records)
    -> circular payload arena (variable-size complete frames)
```

建议 descriptor 至少携带：

```c
typedef struct tlc_ha_replica_frame_desc {
    uint64_t arena_start;       /* 单调递增的逻辑 arena byte position */
    uint64_t frame_id;          /* 提交 token，且与 arena trailer 对应 */
    uint32_t wire_bytes;        /* header + payload，不含 arena trailer */
    uint32_t reserved;
    uint64_t sequence;          /* descriptor 的发布序列 */
} tlc_ha_replica_frame_desc_t;
```

`arena_start % arena_bytes` 是该 frame 的物理首地址。frame 不允许跨 arena 尾部；
剩余尾部不足时 producer 跳过该尾部，从 offset 0 开始下一帧。每帧物理内容为：

```text
[ Replica wire header ][ Replica payload ][ 8-byte frame_id commit trailer ]
```

trailer 不是网络 wire frame 的一部分，不能改变 stream transport 的字节流，也不纳入
既有 header 的 `payload_bytes`。它只用于 UB arena 中的可见性完成判定；原有 wire
checksum 仍覆盖 header 与 payload。

配置至少需要以下几何参数：

```text
descriptor_count: 2 的幂，至少 2
descriptor_bytes: 固定为 sizeof(tlc_ha_replica_frame_desc_t) 的对齐值
arena_bytes:      可容纳 max_wire_bytes + commit trailer
max_wire_bytes:   sizeof(frame header) + max_batch_bytes
```

启动时一次性验证 `max_wire_bytes + commit_bytes <= arena_bytes`，映射 header 中的
magic、version、offset、stride、count、arena_bytes 必须与本地配置完全一致。现有
version 1 映射只能显式 reset 后才可升级到 version 2，不能自动覆盖正在使用或残留的
共享区域。

## 生产与回收契约

### 唯一 publisher

`vemb_v16_client_ring` 的 head/tail 算法是 SPSC。现有 Replica 虽然每方向只有一个
consumer，但一个 TX ring 并非只有一个逻辑 producer：

- Leader 的 event sender 发送 EVENTS；
- Follower receiver 发送 ACK；
- heartbeat thread 在两种角色下都发送 HEARTBEAT。

现在这些路径通过 `stream_write_mutex` 串行化 `replica_send_frame()`，而共享 slot
ring 本身使用 sequence claim 支持竞争。要改用 SPSC descriptor ring，必须将同一
方向的全部 frame 先进入本地 outbound queue，由一个专属 transport publisher 线程
拥有 descriptor tail、arena tail 和 arena reclaim 状态。不得让 sender、receiver 和
heartbeat 直接并发发布 descriptor。

这个 publisher 只改变本地发送调度：EVENTS 的 AOF seq 顺序必须保持，ACK 与
heartbeat 仍按当前 frame 校验和进度定义发送。队列满、arena 满和 descriptor 满是
真实运行时背压，按现有停止/重试策略处理；不为内部已验证参数添加重复的 NULL 或
范围回退分支。

### 发布顺序

Producer 对一帧执行如下顺序：

```text
确认 descriptor 与 arena 都有容量
    -> 写 wire header 和 payload 到 arena
    -> release fence
    -> 写 frame_id commit trailer（最后一次 frame 写入）
    -> 写完整 descriptor
    -> release publish descriptor tail
```

Consumer acquire 观察 descriptor tail 后，先验证 descriptor sequence、`wire_bytes`
范围和 `arena_start`，再 acquire 读取 commit trailer。只有 trailer 与 `frame_id`
匹配，才读取并验证现有 Replica header 与 checksum。这样显式保持 UB 的 payload 可见
先于 descriptor 可见，适用于 cacheable/non-cacheable mapping；不能只假定 CPU 本地
release/store 足以替代 UB 的可见性约定。

### 消费与 arena 回收

Consumer 只能顺序消费 descriptor。producer acquire 观察 consumer head 后，依据
最后一个已释放 descriptor 的 `arena_start + wire_bytes + commit_bytes` 推进可复用的
逻辑 `arena_head`；这与 `vemb_v16_batch_ring.h` 的 reclaim 思路一致。

第一阶段保持当前接收所有权模型：Consumer 在释放 descriptor 前将完整 frame 复制到
本地 `rx_frame`，随后才允许 arena 回收，并继续原有的 decode、Follower COLD append、
ACK 与 async apply 路径。这样不会把 arena 覆盖与 COLD append 并发在一起。

后续若要取消这一次 `rx_frame` 拷贝并直接从 arena decode，则 descriptor 不能在
Follower COLD append 已经接受并拥有所需 key/value 数据之前释放；若 COLD API 不深拷贝，
则须推迟到 append 完成。不得在仅解析 header 后就回收 arena。

## 与现有组件的关系

建议把 `vemb_v16_client_ring` 的 SPSC descriptor 能力抽成可配置内部 API，例如：

```c
size_t vemb_v16_descriptor_ring_bytes(uint32_t slot_size,
                                      uint32_t slot_count);
int vemb_v16_descriptor_ring_init(vemb_v16_client_ring_t *ring,
                                  uint32_t slot_size,
                                  uint32_t slot_count);
```

名称不是最终 API 承诺，核心要求是 `slot_count` 显式参与 bytes、init、mask 与 layout
验证。现有 `vemb_v16_client_ring_bytes(slot_size)` 与
`vemb_v16_client_ring_init(ring, slot_size)` 保持原有 256 槽行为，避免 client/peer-view
映射 ABI 在本优化中被静默改变。Replica 使用新的可配置接口和自己的 version 2 header，
而不是重新解释 client ring 的旧共享内存。

`vemb_v16_batch_ring.h` 中可复用的是 descriptor/arena 的算法约束：连续 frame、绝对
arena cursor、通过 consumer head 回收、最终 commit marker。Replica 应定义自己的
descriptor、frame_id 和 wire 校验，不能复用 request/response 的 `batch_id`、协议 magic
或 decode API。

## 实施阶段

1. 新增可配置 descriptor ring helper 及本地 UT。覆盖 2、8、256 等 descriptor count，
   验证 bytes/layout、满/空、wrap 和不改变旧 client ring 的 ABI。
2. 新增独立 Replica arena layout v2 与 UT。覆盖 frame 大小混合、arena 尾部跳过、
   descriptor 满、arena 满、commit 不可见、坏 descriptor、坏 trailer 与 checksum。
3. 将 Replica 发送改为唯一 transport publisher。先保留 `rx_frame` copy，并比较 v1/v2
   的 EVENTS/ACK/heartbeat 排序、progress 和停止行为。
4. 将 server 启动配置从 `slot_count/slot_bytes` 明确迁移为 descriptor/arena 几何；固定
   111/112 的 path/offset 不变，但为 v2 分配足够且不重叠的映射范围，并显式 reset。
5. 在真实 UB 机器执行完整回归，再决定是否采用直接 arena decode 的第二阶段优化。

每一阶段均应保留 v1 回归入口，直到 v2 在目标 UB 环境稳定；不得把尚未完成 v2 的
吞吐预期写成 HA 正确性或故障恢复结论。

## 验收标准

- 本地：descriptor/arena UT、`tlc_ha_replica_ut`、`tlc_replica_cold_ut`、
  `tlc_resync_ut` 通过；stream transport 行为不变。
- 真实 111/112：direct UB Replica 回归完成 10K events，确认 append ACK、heartbeat、
  Follower async apply、progress 单调和 `peer_health=HEALTHY`。
- 真实 111/112：Redis TCP/SDK 路径完成 10,004 events，Follower TCP 读取验证 retained
  vector、updated key 和 tombstone；reset Follower WARM 后以同一 COLD 目录恢复并验证
  durable 前缀。
- 故障输入：v2 magic/version/geometry 不匹配必须拒绝 attach；坏 commit trailer、坏
  descriptor 或坏 checksum 不得推进 Follower ACK、durable 或 applied progress。
- 性能：单独报告小控制帧与大 EVENTS 混合负载下的 arena 占用、descriptor/arena 满次数、
  sender 阻塞时间和端到端吞吐。没有完成对照测试前，不预设性能提升幅度。

## 风险与决策点

最大的实现风险不是 descriptor 本身，而是把当前多逻辑 producer 的发送路径错误地
当作 SPSC。第二个风险是将 CPU 内存序误当作跨 UB mapping 的 payload 可见性证明。
因此唯一 publisher、commit trailer、版本化 geometry 和真实机 UB 回归是不可省略的
前置。

是否实施 v2 应以两个信号决定：固定大 slot 的空间浪费是否在目标负载中可观，以及
现有 `8 x 128KiB` ring 是否出现可量化背压。若二者均未出现，保持当前已验证的固定
slot 方案比引入新的跨机 layout 更合适。
