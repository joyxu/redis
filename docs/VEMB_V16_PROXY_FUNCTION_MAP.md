# VEMB V16 Proxy Function Map

这份文档用于阅读 [src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1) 时快速定位函数职责。它不是实现设计文档，而是面向代码分析的分层导图。

## 总体阅读顺序

建议按下面顺序阅读：

1. 入口和生命周期：`vemb_v16_proxy_create()`、`vemb_v16_proxy_run()`、`vemb_v16_proxy_stop()`、`vemb_v16_proxy_destroy()`
2. 控制面：`vemb_v16_tcp_handle_fd()`、`alloc_channel_common()`、`close_channel()`
3. 调度主路径：`vemb_v16_proxy_handle_request()`、`publish_shard_job()`、`drain_completions()`
4. 执行主路径：`supernode_pool_thread_main()`、`drain_job_shard_queues()`、`apply_unified_shard_job()`、`apply_vemb_job()`、`apply_vadd_job()`
5. 传输细节：TCP 请求/响应、UB/SHM ring 读写
6. 并发同步：`*_acquire()`、`*_release()`、`*_close_begin()`、`*_wait_closed()`

Transport 语义是严格二选一：

- `--transport tcp`：控制面和数据面都走 TCP。
- `--transport aeron`：控制面同样走 TCP，数据面走 UB/Aeron ring。
- 当前没有 `both` 语义；如果以后要支持双开，需要显式重新设计 channel lifecycle 和 stats/close 路径。

主数据流可以先记成两条线：

- TCP：`TCP request -> proxy io worker -> job shard queue -> supernode worker -> completion ring -> TCP response`
- SHM：`SHM/Aeron request ring -> proxy io worker -> job shard queue -> supernode worker -> completion ring -> SHM/Aeron response ring`

## 0. 核心对象

阅读前先认 4 个结构体：

- `vemb_v16_proxy_t`：全局运行时，持有监听 fd、worker 池、channel 表、job shard queue 和统计信息。[src/vemb_v16_proxy_types.h](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy_types.h:67)
- `vemb_v16_channel_t`：单个 client channel 的状态，统一承载 TCP 和 UB/SHM 两种接入方式。[src/vemb_v16_proxy_types.h](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy_types.h:38)
- `vemb_v16_proxy_io_worker_t`：负责接入侧轮询、收请求、发 completion 的调度 worker。[src/vemb_v16_proxy_types.h](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy_types.h:14)
- `vemb_v16_supernode_pool_worker_t`：负责执行 VADD/VEMB job 的 worker。[src/vemb_v16_proxy_types.h](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy_types.h:23)

## 1. 入口与生命周期层

这一层回答的是：proxy 怎么被创建、启动、停止、销毁。

- `vemb_v16_proxy_create()`：初始化 proxy 基本配置和全局状态，但不启动线程和监听 socket。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1655)
- `vemb_v16_proxy_enable_tcp()`：打开 TCP 数据面配置。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:3607)
- `vemb_v16_proxy_enable_aeron_tcp_control()`：打开 TCP 控制面和 UB/Aeron 数据面配置。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:3624)
- `vemb_v16_proxy_set_supernode_workers()`：设置执行侧 worker 数量。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1732)
- `vemb_v16_proxy_set_proxy_io_threads()`：设置调度侧 worker 数量。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1740)
- `vemb_v16_proxy_run()`：启动 TCP listener、初始化 job shard queue、拉起 worker 池，并进入 accept 主循环；Aeron 的 ATTACH/CLOSE/status 也通过此 listener。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:3792)
- `vemb_v16_proxy_stop()`：把 `running` 置 0，并打断监听 fd。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1883)
- `vemb_v16_proxy_destroy()`：停机、join worker、释放 channel 和队列资源。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1748)
- `vemb_v16_proxy_get_stats()`：汇总活跃 channel、关闭 channel、queue depth 与底层存储统计。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1892)

## 2. 控制面

这一层负责“建链”和“拆链”，不做 VADD/VEMB 业务执行。

- `vemb_v16_tcp_handle_fd()`：处理一个新接入的 TCP fd。它处理 TCP 数据帧、Aeron ATTACH、stats、close 与 topology 控制帧。[src/vemb_v16_tcp_transport.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_tcp_transport.c:441)
- `alloc_channel_common()`：统一分配 channel 状态，是 TCP 和 UB/SHM 两条控制路径的汇合点。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:780)
- `vemb_v16_proxy_alloc_shm_channel()`：UB/SHM channel 的薄封装。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:895)
- `vemb_v16_proxy_alloc_tcp_channel()`：TCP channel 的薄封装。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:900)
- `fill_channel_desc()`：把 channel 元信息回传给 client。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:743)
- `close_channel()`：统一关闭 channel，并等待调度侧和执行侧退出该 channel。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:907)
- `vemb_v16_proxy_close_channel_by_id()`、`vemb_v16_proxy_close_all_channels()`：控制面关闭接口。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:943)
- `cleanup_unstarted_channel()`、`reset_closed_channel()`：处理失败回滚和关闭后的清理。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:763)

## 3. 调度层

这一层负责把请求从接入侧搬运到执行侧，再把 completion 搬运回 client。

### 3.1 请求调度

- `vemb_v16_proxy_handle_request()`：解析和校验协议字段，构造统一 request job，然后投递给 job shard queue。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:549)
- `publish_shard_job()`：调度层核心函数，把 job 从某个 proxy io worker 路由到某个 supernode worker 对应的 job shard queue。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:506)
- `init_job_shard_queues()`、`shard_queue_index()`、`shard_queue_topology_ready()`：维护调度拓扑和路由索引。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:296)

同一 channel/connection 的 `PING/VEMB/VADD/VSIM` 请求在源头进入同一个 job shard queue，因此连接内保持 FIFO；不同 channel 之间仍允许交错执行。协议校验失败的 `error_response` 是异常直返路径，当前不进入 job FIFO。

### 3.2 completion 调度

- `drain_completions()`：从 channel completion ring 拉 completion，并按传输类型回写给 client。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:706)
- `publish_response()`：单条 completion 的统一回写入口，分发到 TCP 或 UB/SHM。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:483)
- `vemb_v16_proxy_fill_response_from_completion()`：把执行结果转换成协议 response 结构。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:467)

## 4. 执行层

这一层是 SuperNode worker 真正做事的地方，负责消费 job 并调用底层执行逻辑。

- `supernode_pool_thread_main()`：执行侧主循环，持续 drain 统一 job shard queue。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1572)
- `drain_job_shard_queues()`：执行侧核心函数，对一个 supernode worker 扫描它负责的所有 job shard queue。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1594)
- `drain_shard_queues()`：通用 batch drain helper，按 queue 批量取 job 后调用统一分发函数。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1527)
- `apply_unified_shard_job()`：按 op 分发 `PING/VEMB/VADD/VSIM`，其中 PING 生成 synthetic completion。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1486)
- `apply_vemb_job()`：调用 `vemb_v16_supernode_handle_vemb_job()` 执行读取类工作。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1425)
- `apply_vadd_job()`：调用 `vemb_v16_supernode_handle_vadd_job()` 执行写入类工作。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1438)

这层可以只盯住一件事：它不负责接入，不负责协议，只负责“消费 job 并把结果放进 completion ring”。

## 5. 传输层

这一层要分成两条线看：TCP 和 UB/SHM。

Transport 拆文件的目标边界是：

- `vemb_v16_proxy.c` 保留 lifecycle、channel 表、worker 池、`vemb_v16_proxy_handle_request()`、shard queue 调度和 SuperNode 执行路径。
- `vemb_v16_tcp_transport.c` 承担 TCP listener 创建、accept 后的控制帧处理、`HELLO/WELCOME` 建链、TCP request 读入、TCP response 编码/回写、backpressure backlog。
- `vemb_v16_aeron_transport.c` 承担 UB/Aeron ring 创建/销毁、request ring poll、response ring publish；不拥有 listener 或 UDS 控制面。
- 两个 transport 文件不直接执行 VADD/VEMB，只把 request 交给 proxy 层的 `vemb_v16_proxy_handle_request()`；completion 回写则由 proxy 层按 channel transport 分派到对应 transport helper。
- `vemb_v16_proxy_run()` 统一创建并 accept TCP listener；channel 的数据面由 channel transport 决定。

当前已完成独立编译拆分：TCP control/data helper 位于 [src/vemb_v16_tcp_transport.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_tcp_transport.c:1)，UB/Aeron data helper 位于 [src/vemb_v16_aeron_transport.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_aeron_transport.c:1)。二者通过 [src/vemb_v16_proxy_internal.h](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy_internal.h:1) 声明的 accessor 和 lifecycle 回调访问 proxy/channel 状态，transport 文件内不直接解引用 `ch->` / `proxy->` 字段。transport 对 proxy 暴露的接口分别声明在 [src/vemb_v16_tcp_transport.h](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_tcp_transport.h:1) 和 [src/vemb_v16_aeron_transport.h](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_aeron_transport.h:1)。

Transport 二选一语义可以用 [scripts/vemb_v16_transport_smoke.sh](/Users/szza/codespace/work/hpc-redis/scripts/vemb_v16_transport_smoke.sh:1) 做轻量回归：脚本会构建 server/bench，确认 `--transport both` 被拒绝，再分别用 `tcp` 和 `aeron` 跑一组 `ping`。

### 5.1 TCP 路径

请求读入：

- `vemb_v16_tcp_listen()`：创建 TCP listener，并返回 transport listener ops。[src/vemb_v16_tcp_transport.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_tcp_transport.c:22)
- `channel_read_tcp_request()`：读一帧 TCP request 并交给 `vemb_v16_proxy_handle_request()`。[src/vemb_v16_tcp_transport.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_tcp_transport.c:374)
- `vemb_v16_tcp_read_ready_requests()`：在 fd 已经 ready 的前提下连续读取一个批次。[src/vemb_v16_tcp_transport.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_tcp_transport.c:404)
- `tcp_poll_input()`：辅助判断 socket 是否还有可读数据。[src/vemb_v16_tcp_transport.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_tcp_transport.c:355)

响应写回：

- `vemb_v16_tcp_publish_response()`：单条 response 回写。[src/vemb_v16_tcp_transport.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_tcp_transport.c:223)
- `vemb_v16_tcp_publish_response_batch()`：批量 completion 回写。[src/vemb_v16_tcp_transport.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_tcp_transport.c:263)
- `encode_tcp_response_bytes()`、`encode_tcp_response_batch()`：把 response 编码成 TCP frame。[src/vemb_v16_tcp_transport.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_tcp_transport.c:130)
- `vemb_v16_tcp_flush_response_backlog()`、`append_tcp_response_backlog()`、`ensure_tcp_response_backlog_capacity()`：处理非阻塞写和 backpressure backlog。[src/vemb_v16_tcp_transport.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_tcp_transport.c:104)

TCP inline vector response 的 payload 由 SuperNode 写入 completion snapshot，TCP 编码阶段只消费 snapshot，不再按 handle 回源读取 WARM slot。这样 TCP 路径不会在 SuperNode completion 到 proxy encode 之间重新打开 stale handle 窗口。

### 5.2 UB/SHM 路径

- `vemb_v16_aeron_create_shared_ring()`、`vemb_v16_aeron_destroy_shared_ring()`：创建和销毁 client 可见的共享内存 ring。[src/vemb_v16_aeron_transport.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_aeron_transport.c:52)
- `vemb_v16_aeron_poll_shm_requests()`：从 request ring 批量拉取请求并交给 `vemb_v16_proxy_handle_request()`。[src/vemb_v16_aeron_transport.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_aeron_transport.c:107)
- `vemb_v16_aeron_publish_response()`：把 response 发布到 response ring。[src/vemb_v16_aeron_transport.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_aeron_transport.c:141)

## 6. 并发与状态同步层

这一层用于保证 channel 关闭时，proxy io worker 和 supernode worker 不会并发踩同一份状态。

- `proxy_io_channel_acquire()` / `proxy_io_channel_release()`：调度侧引用计数。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:371)
- `supernode_channel_acquire()` / `supernode_channel_release()`：执行侧引用计数。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:350)
- `proxy_io_channel_close_begin()` / `proxy_io_channel_wait_closed()`：等待所有调度侧持有者退出。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:418)
- `supernode_channel_close_begin()` / `supernode_channel_wait_closed()`：等待所有执行侧持有者退出。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:441)
- `proxy_io_channel_deactivate()`：把 channel 标记为不再接收请求，同时打断 TCP fd。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:985)

如果在分析时想回答“为什么 close_channel 不会和 worker 打架”，就重点看这一组函数。

## 7. Worker 线程层次

可以把线程模型再压成两层：

- 主线程：只做 accept 和控制面分发。核心函数是 `vemb_v16_proxy_run()`、`vemb_v16_aeron_handle_control_fd()`、`vemb_v16_tcp_handle_fd()`。
- proxy io worker：只做接入侧调度和回写 completion。核心函数是 `proxy_io_poll_thread_main()` 或 Linux 下的 `proxy_io_epoll_thread_main()`。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1005)
- supernode worker：只做 PING/VADD/VEMB/VSIM job 执行或分发。核心函数是 `supernode_pool_thread_main()`。[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1572)

启动和停止入口：

- `start_proxy_io_pool()` / `stop_proxy_io_pool()`：[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1378)
- `start_supernode_pool()` / `stop_supernode_pool()`：[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1618)

## 8. 最值得先读的函数

如果只想用最短时间抓住全局结构，优先读这 8 个函数：

1. `vemb_v16_proxy_run()`：[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1774)
2. `vemb_v16_aeron_handle_control_fd()`：[src/vemb_v16_aeron_transport.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_aeron_transport.c:155)
3. `vemb_v16_tcp_handle_fd()`：[src/vemb_v16_tcp_transport.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_tcp_transport.c:441)
4. `vemb_v16_proxy_handle_request()`：[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:549)
5. `publish_shard_job()`：[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:506)
6. `drain_completions()`：[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:706)
7. `drain_job_shard_queues()`：[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1594)
8. `supernode_pool_thread_main()`：[src/vemb_v16_proxy.c](/Users/szza/codespace/work/hpc-redis/src/vemb_v16_proxy.c:1572)

## 9. 分析时建议问的 4 个问题

看到任意一个函数时，可以先问：

1. 它属于哪一层：入口、控制、调度、执行、传输、同步？
2. 它处理的是 TCP 还是 UB/SHM？
3. 它是在搬运请求/响应，还是在真正执行业务？
4. 它操作的核心对象是什么：`channel`、`job shard queue`、`completion ring`、`worker`？

这样读下来，函数很多，但不会混。
