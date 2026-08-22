# VEMB V16 实现 TODO

日期：2026-05-20

> 弃用说明：`src/vemb_v16_server` 与 `benchmark/vemb_v16_bench` 仅保留给协议兼容和功能 smoke 测试。不得用于性能压测或性能结论；正式压测统一使用 `src/redis-server` 和 `memtier_benchmark`。

## 当前落地范围

实现一条独立 VEMB V16 数据面路径，不改造 `tlc_v16`，也不依赖 Redis command / RedisModule / blocked-client 路径：

```text
vemb_v16_bench / future redis-cli-vemb
-> UDS control alloc channel
-> shared-memory request ring
-> vemb_v16 proxy/channel worker
-> SPSC job ring
-> vemb_v16 SuperNode worker
-> SPSC completion ring
-> shared-memory response ring
-> optional client vector-region read
```

## P0/P1 当前状态

- P0 已完成：standalone path 具备 server 侧 stats，包括 proxy request/completion poll、VEMB/VADD publish、ring full、SuperNode poll/completion、sampled table lookup、bitmap lock/unlock、vector load、completion publish。
- P0 已完成：bench 侧输出 client request ring publish spins 和 response empty polls，用于判断瓶颈是在 client/proxy ring 还是 SuperNode/completion。
- P0 已完成：VEMB 在 SuperNode 内部先做 `vector_key -> row_id` lookup，再调用 `sve_serial_contiguous_read_traced()` 读取 300 dim vector；因此 baseline 会包含 bitmap acquire/release 和 SVE/标量 contiguous load 成本。
- P1 已完成：`vemb_v16_bench` 支持统一线程列表，例如 `--threads 1,2,4,8,16`。
- P1 已完成：`ping`、`vemb-handle`、`vadd`、`vrem` 四条模式可独立 baseline。
- P1 已完成：`--hot-key-id N` 可让所有 worker 命中同一 row，用于制造 bitmap lock 冲突并观察 `bitmap_lock_failure`。
- P1 已完成：bench 每轮结束会关闭 channel 后再拉 stats，`active_channels=0` 代表 channel 生命周期清理完成。

本地短基线（macOS，`--dim 300 --prefill 256 --ops 2000 --threads 1,2`，只用于验证统计闭环，不代表 Linux server 性能）：

| mode | threads=1 | threads=2 | 关键校验 |
| --- | ---: | ---: | --- |
| `ping` | 233k QPS | 429k QPS | 不进入 SuperNode，`total/vemb/vadd=0` |
| `vemb-handle` | 321k QPS | 492k QPS | response 返回 handle，client 读取 vector region |
| `vadd` | 219k QPS | 368k QPS | VADD 全量传 1200B vector，进入 VADD full-vector ring |

## 待实现

- TODO：增加 WARM 内存淘汰机制。
  当前 WARM region / vector table 以固定容量预分配为主，写满后的行为需要补齐。后续应引入 capacity / high-watermark / low-watermark 配置，在 WARM 使用量超过高水位时触发淘汰；淘汰策略可先采用 clock / sampled LRU，优先淘汰 clean、低访问频率、非 hot 的 vector row。若 row 存在 dirty 数据，需要先落 COLD / append log 或确认已有持久副本后再释放。指标侧需要补充 `warm_evict_attempts`、`warm_evict_success`、`warm_evict_dirty_flush`、`warm_evict_fail`、`warm_bytes_used` 和 `warm_bytes_reclaimed`，便于压测观察淘汰是否成为读写路径瓶颈。

## Phase 1

- `src/vemb_v16_protocol.h` 定义无 Redis 依赖的 channel descriptor、request、response、stats 协议。
- `src/vemb_v16_proxy.c` 实现 standalone proxy/channel worker、SPSC job ring、SPSC completion ring、内存 vector table 和 SuperNode worker。
- `src/vemb_v16_server.c` 是独立 main，只负责启动 proxy 和 signal 生命周期。
- `benchmark/vemb_v16_bench.c` 直接连接 standalone server，支持 `ping`、`vemb-handle`、`vemb-inline`、`vadd`、`vrem`。
- 第一版用进程内 vector table 代替 UB vector table，保持 `vector_key -> handle/offset -> vector region` 模型，后续替换为 UB backend。
- 当前版本没有 `server.h`、`RedisModuleCtx`、`RedisModule_BlockClient()`、`RedisModule_UnblockClient()` 依赖。

## 当前运行方式

启动独立 server：

```bash
./src/vemb_v16_server --dim 300 --max-vectors 65536
```

`vadd` 会向进程内 table 写入新 key。重复跑 VADD baseline 时，`--max-vectors` 需要大于本次进程生命周期内累计写入量；如果只是小容量验证，建议重启 server 清空 table。

运行 bench：

```bash
./benchmark/vemb_v16_bench --mode ping --dim 300 --prefill 0 --ops 200000 --threads 1,2,4,8,16
./benchmark/vemb_v16_bench --mode vemb-handle --dim 300 --prefill 65536 --ops 200000 --threads 1,2,4,8,16
./benchmark/vemb_v16_bench --mode vadd --dim 300 --prefill 0 --ops 200000 --threads 1,2,4,8,16
```

## Multi-proxy / SuperNode 运行方式

当前 multi-proxy / multi-SuperNode 模型是：

```text
one vemb_v16_server process = one proxy + one SuperNode group
bench/CLI consistent_hash(vector_key) -> node_index -> target endpoint
```

bench 侧通过一致性 hash 分片到各个 `proxy/SuperNode` endpoint。Proxy 不做 hash，不持有拓扑；每个 server 实例使用独立 `--vector-region` 和 `--region-id`。

### 场景一：同机多实例

在同一台机器上启动多个 TCP server 实例，使用不同端口、不同 WARM region：

```bash
./src/vemb_v16_server \
  --transport tcp \
  --tcp-host 127.0.0.1 \
  --tcp-port 6391 \
  --proxy-io-threads 4 \
  --supernode-workers 8 \
  --vector-region /vemb_v16_vectors_0 \
  --region-id 0 \
  --warm-backend shm \
  --dim 300 \
  --max-vectors 131072
```

```bash
./src/vemb_v16_server \
  --transport tcp \
  --tcp-host 127.0.0.1 \
  --tcp-port 6392 \
  --proxy-io-threads 4 \
  --supernode-workers 8 \
  --vector-region /vemb_v16_vectors_1 \
  --region-id 1 \
  --warm-backend shm \
  --dim 300 \
  --max-vectors 131072
```

bench 使用 `--endpoints` 列出所有本机 endpoint：

```bash
./benchmark/vemb_v16_bench \
  --transport tcp \
  --endpoints 127.0.0.1:6391,127.0.0.1:6392 \
  --mode vemb-inline \
  --dim 300 \
  --prefill 65536 \
  --ops 200000 \
  --threads 1,2,4,8,16 \
  --pipeline 1 \
  --timeout-ms 10000
```

80/20 混合读写，全量 vector 返回：

```bash
./benchmark/vemb_v16_bench \
  --transport tcp \
  --endpoints 127.0.0.1:6391,127.0.0.1:6392 \
  --mode mixed-80r20w \
  --dim 300 \
  --prefill 65536 \
  --ops 200000 \
  --threads 1,2,4,8,16 \
  --pipeline 1 \
  --timeout-ms 10000
```

### 场景二：bench 跨机器访问多节点

当 bench 不在 server 本机时，server 不能绑定 `127.0.0.1`。应绑定 server 机器的网卡 IP，或绑定 `0.0.0.0`；bench 的 `--endpoints` 使用实际 server IP。

Server 机器 A，假设 IP 为 `10.0.0.11`：

```bash
./src/vemb_v16_server \
  --transport tcp \
  --tcp-host 10.0.0.11 \
  --tcp-port 6391 \
  --proxy-io-threads 4 \
  --supernode-workers 8 \
  --vector-region /vemb_v16_vectors_0 \
  --region-id 0 \
  --warm-backend shm \
  --dim 300 \
  --max-vectors 131072
```

Server 机器 B，假设 IP 为 `10.0.0.12`：

```bash
./src/vemb_v16_server \
  --transport tcp \
  --tcp-host 10.0.0.12 \
  --tcp-port 6391 \
  --proxy-io-threads 4 \
  --supernode-workers 8 \
  --vector-region /vemb_v16_vectors_1 \
  --region-id 1 \
  --warm-backend shm \
  --dim 300 \
  --max-vectors 131072
```

Bench 机器连接远端节点：

```bash
./benchmark/vemb_v16_bench \
  --transport tcp \
  --endpoints 10.0.0.11:6391,10.0.0.12:6391 \
  --mode vemb-inline \
  --dim 300 \
  --prefill 65536 \
  --ops 200000 \
  --threads 1,2,4,8,16 \
  --pipeline 1 \
  --timeout-ms 10000
```

80/20 混合读写，全量 vector 返回：

```bash
./benchmark/vemb_v16_bench \
  --transport tcp \
  --endpoints 10.0.0.11:6391,10.0.0.12:6391 \
  --mode mixed-80r20w \
  --dim 300 \
  --prefill 65536 \
  --ops 200000 \
  --threads 1,2,4,8,16 \
  --pipeline 1 \
  --timeout-ms 10000
```

注意事项：

- TCP 下读模式只允许 `vemb-inline` 或 `mixed-80r20w`；其中 `mixed-80r20w` 的读侧发送 `VEMB_V16_OP_VEMB_INLINE` 并返回全量 vector，写侧为 `VEMB_V16_OP_VADD`。
- 跨机器运行时需要放通 server 端 `--tcp-port`，例如 `6391`。
- `--transport aeron` 的 multi-node 使用 `--sockets PATH[,PATH...]`，当前适合同机 UDS + SHM/Aeron ring；跨机器 Aeron 需要后续 `aeron-over-UB` 设计。

## VSIM inline / key-key 设计

当前先落地两类 VSIM：

```text
VSIM key, other_vector_inline_data
VSIM key1, key2
```

`VSIM key, other_vector_inline_data` 对应 bench 模式：

```bash
./benchmark/vemb_v16_bench \
  --transport tcp \
  --endpoints 127.0.0.1:6391,127.0.0.1:6392 \
  --mode vsim-inline \
  --dim 300 \
  --prefill 65536 \
  --ops 200000 \
  --threads 1,2,4,8,16 \
  --pipeline 1 \
  --timeout-ms 10000
```

语义：

```text
bench routes key by consistent hash
SuperNode lookup vector(key)
SuperNode computes cosine(vector(key), other_vector_inline_data)
response returns score
```

`VSIM key1, key2` 对应 bench 模式：

```bash
./benchmark/vemb_v16_bench \
  --transport tcp \
  --endpoints 127.0.0.1:6391,127.0.0.1:6392 \
  --mode vsim-key-key \
  --dim 300 \
  --prefill 65536 \
  --ops 200000 \
  --threads 1,2,4,8,16 \
  --pipeline 1 \
  --timeout-ms 10000
```

P0 约束：

- `VSIM key1, key2` 只支持同分片执行。
- bench 先用 `key1` 做 consistent hash 得到目标 node，再在 `prefill` 范围内选择同 node 的 `key2`。
- 如果没有找到同 node 的 `key2`，bench 会退回使用 `key1` 自身作为 `key2`，保证请求仍在单 SuperNode 内闭环。
- Server 不做跨 SuperNode fanout；Proxy 仍不做 hash。

服务端执行：

```text
SuperNode lookup vector(key1)
SuperNode lookup vector(key2)
SuperNode computes cosine(vector(key1), vector(key2))
response returns score
```

cosine 计算统一使用轻量抽取出的 `sve_cosine_similarity_f32()`。该函数位于 `sve_similarity.c/.h`，避免 standalone `vemb_v16_server` 链接完整 `sve_compute.c` 时引入 Redis runtime / SDS / Lua 依赖。

### VSIM 测试指令

先启动一个 TCP standalone server，用于本机 smoke：

```bash
./src/vemb_v16_server \
  --transport tcp \
  --tcp-host 127.0.0.1 \
  --tcp-port 6400 \
  --proxy-io-threads 1 \
  --supernode-workers 1 \
  --vector-region /v16vsim \
  --region-id 100 \
  --warm-backend shm \
  --dim 8 \
  --max-vectors 128 \
  --loglevel warning
```

场景一：`VSIM key, other_vector_inline_data`。bench 通过 `key` 路由到目标 proxy/SuperNode，请求体携带另一个完整 inline vector，server 返回 cosine score：

```bash
./benchmark/vemb_v16_bench \
  --transport tcp \
  --endpoints 127.0.0.1:6400 \
  --mode vsim-inline \
  --dim 8 \
  --prefill 16 \
  --ops 16 \
  --threads 1 \
  --pipeline 1 \
  --timeout-ms 5000
```

场景二：`VSIM key1, key2`。bench 保证 `key2` 与 `key1` 命中同一个 endpoint，server 在同 SuperNode 内读取两个 vector 并返回 cosine score：

```bash
./benchmark/vemb_v16_bench \
  --transport tcp \
  --endpoints 127.0.0.1:6400 \
  --mode vsim-key-key \
  --dim 8 \
  --prefill 16 \
  --ops 16 \
  --threads 1 \
  --pipeline 1 \
  --timeout-ms 5000
```

多 proxy/SuperNode 压测时，将 `--endpoints` 改成实际 server 列表即可；bench 侧继续使用 consistent hash 分片：

```bash
./benchmark/vemb_v16_bench \
  --transport tcp \
  --endpoints 127.0.0.1:6391,127.0.0.1:6392 \
  --mode vsim-inline \
  --dim 300 \
  --prefill 65536 \
  --ops 200000 \
  --threads 1,2,4,8,16 \
  --pipeline 1 \
  --timeout-ms 10000

./benchmark/vemb_v16_bench \
  --transport tcp \
  --endpoints 127.0.0.1:6391,127.0.0.1:6392 \
  --mode vsim-key-key \
  --dim 300 \
  --prefill 65536 \
  --ops 200000 \
  --threads 1,2,4,8,16 \
  --pipeline 1 \
  --timeout-ms 10000
```

后续跨分片 `VSIM key1, key2` 有三种可选路线：

- client-side join：bench/CLI 分别向两个节点取 vector，本地计算 cosine。
- coordinator proxy：key1 所在 proxy 远程请求 key2 所在 proxy，再在 coordinator 计算。
- UB shared read：依赖后续 `aeron-over-UB` / region map，让 coordinator 可读取远端 WARM region。

关键观察项：

- `request_publish_spins` 高：client -> proxy request ring 消费不及时。
- `response_empty_polls` 高：client 在等待 proxy 回包，需结合 SuperNode/completion 计数判断。
- `proxy_vemb_ring_full` / `proxy_vadd_ring_full` 高：proxy -> SuperNode job ring 是瓶颈。
- `supernode_completion_ring_full` 高：SuperNode -> proxy completion ring 是瓶颈。
- `proxy_response_ring_full` 高：proxy -> client response ring 是瓶颈。
- `bitmap_lock_failure` 高：多个 SuperNode worker 正在争抢同一 row，热点 key 或 row 映射冲突明显。
- `vector_load_avg_ns` 高：SuperNode 内部 contiguous read 是瓶颈；在 ARM SVE build 上这里对应 SVE load/store 路径，在非 SVE build 上是 scalar `memcpy` fallback。
- `published_jobs == completed_jobs == vemb_requests/vadd_requests`：完整路径没有丢请求。

热点冲突验证：

```bash
./benchmark/vemb_v16_bench --mode vemb-handle --dim 300 --prefill 65536 --ops 200000 --threads 2,4,8,16 --hot-key-id 0
```

该模式用于观察同一 `row_id` 的 bitmap acquire 冲突，不代表均匀 key 分布下的生产性能。

## 后续

## TODO：proxy/SuperNode 线程池化

当前 `vemb_v16_bench` / `vemb_v16_server` 的线程模型是 channel 绑定线程：

```text
bench worker N
-> channel N
-> server proxy channel thread N
-> server SuperNode thread N
```

也就是说，bench `--threads 64` 时，server 侧会随 channel 数线性创建约 `64 proxy channel threads + 64 SuperNode threads`。这个模型实现简单、channel 隔离好，但连接数和线程数强绑定，不适合后续跨机 TCP、多 client、长连接生产形态。

目标模型是把 channel 降级为连接/session，把执行资源改为固定线程池：

```text
many bench workers / TCP connections / SHM channels
-> proxy I/O worker pool
-> bounded job queues or shard queues
-> SuperNode worker pool
-> completion / response dispatch
```

预期收益：

- 线程数不再随 bench/client/channel 数线性增长，server 侧可以固定为 `proxy_io_threads=N`、`supernode_workers=M`。
- 减少线程栈、调度、上下文切换和 cache footprint，压测线程数超过物理 core 后更稳定。
- 更容易做跨 channel 负载均衡，避免某个热 channel 独占一个 SuperNode thread 而其他 thread 空闲。
- 更适合 TCP 跨机长连接：大量 TCP connection 由少量 I/O worker 管理，计算由固定 SuperNode worker 执行。
- 内部 ring/queue 数量可以从 per-channel ring 逐步收敛为 per-worker/per-shard queue，降低内存和 cache 压力。
- 可以做全局 backpressure：global inflight、per-channel inflight、per-worker queue depth、超时/拒绝策略。

第一版约束：

- 同一 channel 内先保持请求/响应有序，不立即引入乱序 response。
- `bench` 当前 pipeline 按发送顺序等待 response；如果后续允许同一 channel 并行执行，需要 client 侧按 `req_id` 匹配，或者 server 侧做 per-channel reorder buffer。
- 优先实现折中方案：`accept/control main thread + proxy_io_threads + supernode_workers`，其中 `channel_id` 或 `key_hash` 路由到固定 worker/shard，先拿到线程数受控和跨 channel 均衡收益。
- SHM transport 需要 proxy I/O worker 轮询多个 request ring，并把 completion 写回对应 response ring。
- TCP transport 需要 proxy I/O worker 管理多个 persistent fd，后续可从 `poll` 演进到 `epoll` / `kqueue`。
- stats 需要同时保留 per-channel 可观测性和 per-worker 聚合指标，避免线程池化后定位瓶颈变困难。

落地 TODO：

1. P0：已完成。SuperNode 执行已统一收敛为固定 worker 池。
   - channel 保留 `completion_ring` 作为响应有序与 close 协议边界。
   - 主请求分发不再依赖 per-channel `vemb_job_ring` / `vadd_job_ring`。
   - 固定 worker 通过 shard queue 消费请求，并保持同一 channel 的 completion 回写边界。
2. P1：已完成。proxy 请求处理已统一收敛为固定 I/O worker。
   - accept/control main thread 只负责 accept、HELLO/WELCOME、channel lifecycle。
   - `proxy_io_worker[k]` 管理多个 TCP fd，并轮询自己负责的 SHM channel 子集，处理 request poll、job dispatch、completion drain、response write。
   - Linux 下 worker 优先使用 `epoll`；非 Linux 继续使用 `poll`。
3. P2：已完成第一版 Linux epoll 化。
   - Linux 下每个 proxy I/O worker 拥有一个 `epoll_fd`，非 Linux 继续 fallback 到 `poll`。
   - TCP fd 注册 `EPOLLIN | EPOLLERR | EPOLLHUP`，fd 注册状态由 worker 维护。
   - channel close 会等待 worker 从 epoll 中摘除 fd，避免 fd close/reuse 与 epoll 注册表交叉。
   - 已完成第一版慢 client backpressure：Linux pooled proxy I/O channel 在 response 写不动时转入 per-channel backlog，并通过 `EPOLLOUT` 继续 flush，避免同步写长期占住 worker。
   - 已完成第一版 completion 唤醒：Linux proxy I/O worker 使用 worker-local `eventfd`，SuperNode 在 completion 发布后按 arm/disarm 语义通知对应 worker，减少 idle scan；后续再评估是否扩展到 job queue 唤醒。
4. P3：已完成第一版。队列收敛。
   - TCP / SHM VEMB / VADD 主路径均已收敛到 `proxy_io_worker -> supernode_worker` SPSC shard queue。
   - shard queue 已是当前唯一主路径，不再作为“仅双开 worker 时生效”的可选分支。
   - 当前 VADD 仍走 full-vector payload shard queue，先拿到 pooled 线程模型收益，后续再评估 staged payload 或小 descriptor 化以继续压缩内存/复制成本。
   - Linux pooled SuperNode worker 已完成第一版 job queue 唤醒：publisher 按目标 `supernode_worker_id` 唤醒 worker-local `eventfd`，worker idle 时按 arm/disarm 语义等待，减少 shard queue 空转扫描。
5. P4：跨 worker 并行与 response reorder。
   - 从 `channel_index` 路由升级为 `key_hash` / shard 路由。
   - 单 channel 可并行打到多个 SuperNode worker。
   - client 或 server 引入 `req_id` 匹配 / reorder buffer，保持 pipeline response 语义。

当前第一步命令示例：

```bash
./src/vemb_v16_server \
  --transport tcp \
  --tcp-host 127.0.0.1 \
  --tcp-port 6391 \
  --proxy-io-threads 8 \
  --supernode-workers 16 \
  --vector-region /vemb_v16_vectors \
  --warm-backend shm \
  --dim 300 \
  --max-vectors 131072
```

慢 client / backlog 验证可使用：

```bash
./benchmark/vemb_v16_slow_client_bench \
  --host 127.0.0.1 \
  --port 6391 \
  --proxy-io-threads 1 \
  --prefill 1024 \
  --slow-ops 8192 \
  --probe-ops 1000 \
  --stall-ms 2000
```

建议配合 `--proxy-io-threads 1` 或在空闲 server 上运行，便于让 slow/probe channel 更稳定地落到同一 proxy I/O worker。

## Phase 2：Aeron Ring 化

设计文档：`docs/VEMB_V16_PHASE2_AERON_RING_DESIGN.md`

核心要求：

- 采用 Aeron fixed-slot SPSC ring。
- 不复用 `src/ring_buffer.h/.c`。
- 不引入 `server.h` / Redis runtime。
- 将当前 `vemb_v16_proxy.c` 内部临时 `job_ring` / `completion_ring` 替换为 typed Aeron ring。
- 将 SuperNode worker loop 从 proxy 文件拆出。
- 将进程内 vector table 从 proxy 文件拆出，后续替换为 UB backend。
- VEMB 使用小 descriptor ring；VADD 暂时保留 full-vector job ring，全量传输 1200B vector。

落地顺序：

1. 新增 `src/vemb_v16_aeron_ring.h`。
2. 新增 `src/vemb_v16_dataplane.h`，放置 `vemb_v16_job_t` / `vemb_v16_completion_t`。
3. 用 Aeron typed ring 替换 `vemb_v16_proxy.c` 内部临时 ring。
4. 新增 `src/vemb_v16_supernode.c/.h`，SuperNode 通过 job ring 收请求，通过 completion ring 回结果。
5. 新增 `src/vemb_v16_table.c/.h`，把 table/backend 从 proxy 拆出。
6. 拆分 VEMB 小 job ring 和 VADD full-vector job ring，避免 VEMB 复制 1200B payload。
7. 补充性能计数：proxy poll、job ring、SuperNode 执行、completion、response ring。

## Phase 3+

- 将进程内 vector table 替换为 UB vector table。
- 单 WARM layer 多 UB region 支持：已完成核心 mock/shm 路径。
  - warm provider 支持打开 `regions[]`，每个 region 来自 obmmctl 预先创建的 UB path 或 POSIX SHM。
  - tlc_core metadata 从 `warm_idx -> warm_slot` 扩展为 `warm_idx -> {region_id, local_slot, offset, bytes}`。
  - 新 key 通过 WARM region hash ring 分配；本地 UB region 使用更高 weight，region 满后 fallback 到下一个可用 region。
  - overwrite 保持原 `{region_id, offset}`，不重新 hash 迁移。
- OBMM warm regions manifest：已完成 parser 与 `shm/mock_ub/ub` provider 接入，真实生产 UB 仍待环境验证。
  - manifest 记录 `region_id/path/mmap_offset/bytes/value_size/home_ub_node_id/weight`。
  - server 配置 `local_ub_node_id`，用 `home_ub_node_id == local_ub_node_id` 判断 local region。
  - 不通过 `/dev/obmm_shmdevX` 数字后缀推断 locality。
- bench/client 多 region mmap：已完成 channel descriptor 多 region 暴露，bench 按 `resp.region_id` 查找 mapped region。
- TCP read path：已明确只能 inline vector；server 会拒绝 TCP `VEMB_HANDLE` 读请求，bench TCP 读模式只允许 `vemb-inline` 或 `mixed-80r20w`，读请求使用 `VEMB_V16_OP_VEMB_INLINE`。
- warm region aggregate stats：已完成 server/bench 输出 `warm_region_count`、`warm_region_full_count`、`warm_alloc_local`、`warm_alloc_remote`、`warm_alloc_fallback`、`warm_alloc_cold_spill`、`warm_alloc_fail`、`warm_region_hash_local_pct`。
- TODO：真实生产 OBMM/UB 验证。
  需要 Huawei 方提供生产环境 UB 初始化案例，明确 `obmmctl export/import`、UB path 创建、region 暴露方式，以及哪块 UB region 是 local region。
- TODO：per-region stats 对外输出。
  `tlc_core_get_region_stats()` 已实现，但协议/bench/server 还未输出每个 region 的 `capacity_slots`、`used_slots`、`is_local`、`full`。
- TODO：WARM eviction。
  当前 region 满后 fallback，所有 region 满后 cold spill；还没有 high-watermark/low-watermark、clock/LRU、dirty flush、slot reuse。
- TODO：bench 多节点拓扑补齐 `aeron` 跨机器模式。
  当前 `aeron` transport 仍假设 CLI 与 proxy/SuperNode 在同机，通过 UDS control plane + POSIX SHM request/response ring 通信；当 CLI 与 proxy/SuperNode 跨机器部署时，不能直接使用本机 SHM。后续需要设计 `aeron-over-UB` 路径：控制面可走远端 endpoint，request/response ring 与 WARM data region 通过 UB 可 mmap 区域承载，从而实现 CLI 与远端 proxy/SuperNode 的跨机器数据通信。
- 增加文本命令 CLI，解析兼容 `VADD myvectors ... item:N` / `VEMB myvectors item:N RAW`。
- 在 VEMB 热路径稳定后，再把 VADD full-vector job ring 改为 staged VADD。
- 增加 consistent hash ring 和多 SuperNode 进程消费。
- 增加 VSIM query/result path。
