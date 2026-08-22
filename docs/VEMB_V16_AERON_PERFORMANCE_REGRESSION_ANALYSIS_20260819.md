# VEMB V16 Aeron 本地性能回退定位报告

## 1. 结论

在 `TS=64`、`CS=4`、`PS=32`、`PIO=21`、`SNW=21`、`100K R:R` 的本地 Aeron 测试中，当前结果约为 `32.7M QPS`，相比 `89f2875` 基线的约 `50M QPS` 低约 `34.6%`。

主要原因不是 server 的 `PIO/SNW` 配置，也不是 UB 带宽，而是当前工作区将本地 `aeron` 从旧的 direct ring runner 改成了 common-core SDK runner。该重构尚未提交，不能归因到某个正式 commit。

此前 `c6ea39c` 引入的 compact frame 解码清零问题是另一处独立回退，已经通过仅清零 request 标量区修复；它不是当前 `32.7M QPS` 的主要剩余原因。

## 2. 对照结果

测试条件均为 `NUM_KEYS=100000`、`R:R`、`TS=64`、`PS=32`、`PIO=21`、`SNW=21`，测试时长约 30 秒。

| runner / 版本 | CS | QPS | 说明 |
|---|---:|---:|---|
| `89f2875` 旧 direct Aeron runner | 4 | 约 49.9--50.6M | 基线 |
| `a84d4ff` direct batch runner | 4 | 约 49.16M | 仍接近基线 |
| `c6ea39c` compact frame 原始实现 | 4 | 约 33.13M | request decode 每次清零完整 vector |
| `c6ea39c` + request decode 临时修复 | 4 | 约 48.50M | 证明大对象清零是独立回退 |
| 当前 common-core runner | 1 | 约 40.99M | 当前工作区 |
| 当前 common-core runner | 4 | 约 32.70M | 当前工作区 |
| 当前 common-core + `AERON_BATCH_DISABLE=yes` | 4 | 约 33.17M | 与默认值基本相同 |

`CS=4` 相比当前 `CS=1` 额外下降约 `20.3%`，但旧 direct runner 在相同 `CS=4` 下仍能达到约 `50M QPS`，所以 `CS` 本身不是根因，而是放大了 common-core session 调度开销。

## 3. 旧 runner 与当前 runner 的数据路径差异

### 3.1 旧 direct Aeron runner

旧路径在 `memtier_benchmark/vemb_v16_aeron_runner.cpp` 中直接维护每个 worker 的 Aeron channel：

- 每个 worker 直接持有 `CS` 个 channel。
- 请求使用 `vemb_v16_aeron_publish_request_batch()` 批量写入 ring。
- 响应使用 `vemb_v16_aeron_poll_response_batch_ex()` 批量读取 ring。
- pending 请求使用固定数组、head/tail 和 pipeline 计数，正常路径为 O(1)。
- 请求构造使用栈上的 request batch，不需要为每个 key 创建 C++ `std::string`。

### 3.2 当前 common-core runner

当前未提交工作区中的 [memtier_benchmark/vemb_v16_aeron_runner.cpp](../memtier_benchmark/vemb_v16_aeron_runner.cpp) 将每个 worker 的每个 client 转换成完整 SDK slot：

- `CS=4` 时每个 worker 创建 4 个 `vemb_v16_client_t` 和 4 个 session。
- [common_core_drive_sessions()](../memtier_benchmark/vemb_v16_aeron_runner.cpp#L343) 每轮对每个 slot 执行 session `flush()` 和 `poll()`。
- [common_core_run_async_reads()](../memtier_benchmark/vemb_v16_aeron_runner.cpp#L367) 每个请求创建 `std::string`，调用 `gettimeofday()`，并写入 `unordered_map` pending。
- response callback 通过 `unordered_map` 查找并删除 pending，而不是旧 runner 的固定环形数组。
- 请求还要经过 cluster operation、owner routing、session state 和 SDK transport wrapper，即使本地只有一个 owner。

## 4. common-core 的具体热路径开销

当前 SDK 的 owner 数量上限为 `VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS=64`：

- [vemb_v16_client_handle_session_flush()](../clients/c/vemb_v16_client_sdk.c#L2100) 每次遍历 64 个 owner。
- [vemb_v16_client_handle_session_poll()](../clients/c/vemb_v16_client_sdk.c#L2113) 每次再次遍历 64 个 owner。
- `CS=4` 时每个 worker 有 4 个 slot，因此一次 worker drive 会重复执行 4 组 owner 扫描。
- [vemb_v16_client_handle_session_submit()](../clients/c/vemb_v16_client_sdk.c#L2063) 分配 request id 时线性扫描最多 4096 个 pending slot。
- [sdk_handle_session_poll_v1()](../clients/c/vemb_v16_client_sdk.c#L1978) 收到响应后还要在线性 request 表中查找 operation id。

本地只有一个实际 owner，但上述循环仍按 64 个 owner 执行。这解释了 `CS=4` 比 `CS=1` 更明显的下降，也解释了为什么 `AERON_BATCH_DISABLE=yes` 几乎不能恢复 QPS：该开关只改变 SDK session 类型，不会绕过 common-core 的 owner/session 驱动层。

## 5. compact frame 回退与修复

提交 `c6ea39c` 将 Aeron ring 从完整结构体改为 compact frame。`vemb_v16_req_decode()` 原本每次执行：

```c
memset(req, 0, sizeof(*req));
```

`vemb_v16_req_t` 中包含最大 1200 个 float 的 vector scratch 区，而 `R:R` 读取请求不携带 vector，因此该清零会在每个请求上产生不必要的大内存写入。

当前修复位于 [src/vemb_v16_protocol.h:1803](../src/vemb_v16_protocol.h#L1803)：

```c
memset(req, 0, offsetof(vemb_v16_req_t, vector));
```

旧 direct runner 上该修复可将约 `33M QPS` 恢复到约 `48.5M QPS`，说明 compact frame 编解码本身不是当前剩余约 `17M QPS` 差距的主要原因。

## 6. 其他改动的影响判断

| 改动 | 判断 |
|---|---|
| `a84d4ff` direct batch publish/poll | 影响较小，约 `49.16M QPS`，仍接近基线 |
| `c6ea39c` compact frame | 原始版本有明显回退，request decode 清零已修复 |
| `6249f28` UB batch SDK | 主要服务于 batch/cross-node，当前本地默认不会因 `AERON_BATCH_DISABLE` 自动回到旧 runner |
| `a150ab7` worker-local L1 cache | 当前 `l1_entries=0`，不是本次回退原因 |
| `1118dfa` 显式 dim 校验 | setup 校验，不在 workload 热路径 |
| PIO/SNW 从 21 调整 | 本次对照固定为 `21:21`，不是主要原因 |

## 7. 修复方向

本地与跨节点 Aeron 统一使用 common-core SDK。peer-view 是所有 UB owner 的
client-side 资源解析配置；同机 manifest 的 provider/client path 相同，跨节点
manifest 才把服务端路径转换成客户端可见的直通路径。两者不再通过 transport
名称或 server flag 选择不同执行路径。保留 `vemb_v16_req_decode()` 的局部清零
修复，并以 `89f2875` 的 `CS=4` 约 `50M QPS` 作为回归门槛。

## 8. 当前工作区状态

本报告涉及的 common-core runner、SDK session 和部分 transport 文件当前存在未提交修改。性能结论基于这些工作区修改以及此前 `/tmp/hpc-redis-89f2875-aeron` 回归 worktree 的对照结果；不能把 common-core 回退归因到 `bedafd4` 正式提交本身。

## 9. 同机与跨节点 Aeron 路径收敛目标

当前同机与跨节点 Aeron 的 steady-state 路径在协议和 resolver 语义上统一；
差异只应来自实际 UB 设备位置、映射 cache policy 和硬件访问代价，而不是
部署模式开关：

- 所有 client 都通过 peer-view manifest 解析 ATTACH 返回的 provider path、offset
  和 client path；同机只是 provider/client path 相同的 manifest。
- 同机和跨节点都支持相同的 v1 与 v2 descriptor/arena channel。v2 是否启用只由
  协商能力、batch 参数和 session 状态决定。
- server 不再使用 `--vemb-v16-cross-node-aeron` 改变 response UB cache policy，
  也不根据部署位置改变 proxy 调度策略。
- 两个脚本默认的 CPU 集合也不同：同机 server 是 `0-47`，跨节点 server 是
  `0-15`。这不是协议差异，但会直接影响吞吐对照。

目标是让部署位置不再决定协议和执行分支，A/B 测试只保留 UB cache policy
（以及由硬件带来的真实本地/远端访问代价）：

1. 所有 ATTACH 返回资源描述，资源的实际 mmap、provider-to-client path/offset
   转换和 cache policy 由 client 完成；server 不依赖 client peer-view 配置。
2. 同机和跨节点 client 都支持相同的 v1 与 v2 descriptor/arena channel。v2
   是否启用只由协商能力、batch 参数和 session 状态决定，不由部署模式决定。
3. 删除 `--vemb-v16-cross-node-aeron` 及其 server-side 分支。server 不再根据
   部署位置选择 response cache policy；cache policy 应由 client 的映射请求/资源
   描述决定。
4. proxy/server 的 idle polling 统一为 1 ms 起步，满环发布使用统一 backoff；
   后续可通过可测量的 `10 us` 级短等待替换固定毫秒等待，但必须避免每轮无条件
   忙等导致 CPU 抢占和 QPS 下降。
5. 性能对照必须统一 `PIO/SNW`、CPU mask、pipeline、batch size、v1/v2 模式和
   workload，只改变 UB cache policy 或实际设备位置。

peer-view 不是可选 resolver，也不是“跨节点模式开关”，而是所有 Aeron UB
部署的必需 client-side resource mapping。它不改变 server 的协议、调度或热路径；
同机和跨节点只使用不同的映射内容。
