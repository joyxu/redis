$ ssh -p 22 root@192.168.90.112
$ directory:
$ - /root/szz/codespace/hpc-redis_bench
$ - /root/FlameGraph/flamegraph.pl: only for redis-server
$- 编译的时 redis-server，不是 vemb_server
$ run
```
  make -C src redis-server USE_UB=yes

  normal path:
  NUM_KEYS=100000 WORKERS='21:21' TS='64' CS='4' TEST_TIME=30 bash hpc_redis_max_tput.sh

  cache case:
  NUM_KEYS=10000 WORKERS='21:21' TS='64' CS='4' TEST_TIME=30 bash hpc_redis_max_tput.sh
```

## Note:
- 仅同步代码，不其他文档或者二进制，覆盖远程同位置代码
- 有问题及时反馈不要自己猜测
- 结果需要: `ops/sec / p50 / p99 / cpu_cores` + 火焰图
- 远端的火焰图 svg 拉取到本地的 perf 目录下
- 热点key: NUM_KEYS 可以降低

## 本轮火焰图约束
- 不要错误编译 src/vemb_v16_server 并错误使用
- 远端实验代码固定使用 `/root/szz/codespace/hpc-redis_bench`，不得使用旧的 `/root/szz/codespace/hpc-redis` 路径。
- 火焰图只采样 hpc-redis 的 `redis-server` 进程及其线程，禁止把独立的 `memtier_benchmark`、`redis-cli` 或其他进程纳入火焰图。
- 先从 `/tmp/hpc_max_tput_server_<PORT>.pid` 获取 server PID，再使用进程定向采样：
  `perf record -F 99 -g -e cycles -p <REDIS_SERVER_PID> -- sleep 20`
- 必须保留用户态和内核态调用链；不得使用 `cycles:u`，也不得使用系统范围的 `-a` 采样。
- 每组实验单独执行、单独保存产物；某组失败时不得影响已完成组，支持从失败组继续。
- 火焰图及 collapsed 数据按实验参数命名，并将 SVG 拉取到本地 `perf/` 目录。命名至少包含 `batch`、`pipeline`、`workers` 和 `NUM_KEYS`，例如：
  `flamegraph_hpc_redis_server_only_host_mt_20260728_batch32_pipe32_workers21_21_numkeys100000.svg`
- 生成后必须检查 collapsed 数据和 SVG 中包含 `recv`、`writev`、`tcp_recvmsg`、`tcp_sendmsg` 等 server I/O 调用链，且不包含 `memtier` 或 `redis-cli`。
- normal 场景与 cache 场景的 `NUM_KEYS` 不同；cache 场景只对比最佳参数组 `batch/pipeline=32/32`、`WORKERS=21:21`，使用 `NUM_KEYS=10000`，不对每组 normal 参数重复执行 cache 对比。
- 本地使用 `scripts/run_host_mt_server_flamegraph.sh` 执行一组测试并拉回完整产物；脚本不会批量循环。例如：
  `BATCH=32 PIPELINE=32 WORKERS=21:21 NUM_KEYS=100000 bash scripts/run_host_mt_server_flamegraph.sh`
  `SCENARIO=cache BATCH=32 PIPELINE=32 WORKERS=21:21 NUM_KEYS=10000 bash scripts/run_host_mt_server_flamegraph.sh`

## memtier_benchmar
- 如果需要编译 memtier_benchmark, 流程:
```
- 先编译 client sdk : cd clients/c/ && make -j
- 再编译 memtier_benchmark:  autoreconf -ivf && ./configure && make -j
make -C clients/c -j && make -C memtier_benchmark -j
```
