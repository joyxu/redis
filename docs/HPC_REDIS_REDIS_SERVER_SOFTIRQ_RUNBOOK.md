# hpc-redis Redis Server 软中断分析手册

本手册用于分析 `redis-server` 线程运行期间执行的软中断（softirq）工作，
目标是定位服务端 `si` CPU 开销的来源，并以可重复的证据验证优化效果。

## 分析范围

在 `top` 和 `mpstat` 中，`si` 是 Linux 软中断处理程序消耗的 CPU 时间。
它是按 CPU 统计的指标，而不是按进程统计。软中断可在两种上下文中执行：

1. 当前运行的 `redis-server` 线程内联处理软中断，例如 TCP 系统调用重新
   开启 bottom half 时。
2. 内核将工作推迟给 `ksoftirqd/N` 内核线程。

本手册关注第一种情况。现有的
`scripts/run_host_mt_server_flamegraph.sh` 默认以 `EVENT=cycles` 对
`redis-server` PID 执行 `perf` 采样。因此它同时保留 Redis 线程的用户态和
内核态调用栈，其中包括内联软中断处理；它有意排除了 memtier、redis-cli 和
`ksoftirqd`。

这正适用于回答“哪个 Redis 线程和调用路径导致它执行软中断？”的问题。仅当
需要调查 `ksoftirqd` 或其他进程造成的 CPU 干扰时，才需要全机采样。

## 测量约定

同一组对比实验中必须保持以下条件不变：

- workload 操作、数据集、`BATCH`、`PIPELINE`、`WORKERS`、`TS` 和 `CS`；
- `TEST_TIME`、`FLAME_DURATION`、CPU 亲和性模式、网络拓扑和内核/NIC 配置；
- Redis 二进制与机器后台负载。

每轮实验只改变一个候选设置。记录 QPS、平均延迟、p99 延迟、每 CPU 的
`%soft` 和产生的火焰图路径。绝对 `si` 百分比本身不是优化目标；更有意义的
主指标是每单位完成工作消耗的软中断 CPU 时间，同时 QPS 和 p99 必须保持或改善。

## 基线采集

启动 host-mt workload，同时保留用户态和内核态栈：

```bash
TEST_TIME=60 FLAME_DURATION=30 EVENT=cycles \
BATCH=32 PIPELINE=32 WORKERS=21:21 \
bash scripts/run_host_mt_server_flamegraph.sh
```

`FLAME_DURATION` 必须小于 `TEST_TIME`。该脚本会将 `.perf.data`、折叠调用栈
文本、火焰图 SVG、元数据和压测汇总结果拉回 `perf/<run-id>/`。

在 workload 的同一稳定区间内，在 server 主机采集 CPU 和软中断类型计数：

```bash
mpstat -P ALL 1
pidstat -t -u -p "$(pgrep -n -x redis-server)" 1
watch -n 1 'grep -E "NET_RX|NET_TX|TIMER|SCHED|RCU" /proc/softirqs'
```

使用测量区间内的 `/proc/softirqs` 计数增量，而不是绝对值。还应记录 NIC 队列
和中断分布：

```bash
grep -Ei 'eth|ens|enp|mlx|virtio' /proc/interrupts
ethtool -S <nic> | grep -Ei 'rx|drop|miss'
```

## 火焰图归因

打开生成的 SVG，并搜索以下符号：

```text
do_softirq
handle_softirqs
net_rx_action
napi_poll
run_rebalance_domains
run_timer_softirq
rcu_core_si
```

对每个显著的 `do_softirq` 块，都沿调用栈向上和向下检查。调用者说明 Redis
线程及业务操作；被调用者说明软中断类型和内核工作。

现有 host-mt 产物中有一条典型 TCP 路径：

```text
proxy I/O thread
  -> recv or writev
  -> TCP send/receive path
  -> __local_bh_enable_ip
  -> do_softirq -> net_rx_action -> napi_poll
```

这表示 proxy I/O 线程正在内联处理网络包，并不表示 server 的向量或存储路径
本身是软中断 CPU 开销来源。

| `do_softirq` 下方的内核栈 | 含义 | 后续证据 |
| --- | --- | --- |
| `net_rx_action -> napi_poll -> tcp_*` | RX/NAPI 收包处理 | 包速率、RX 队列/IRQ 放置、请求与响应批量化 |
| `run_rebalance_domains` | 调度器均衡（`SCHED`） | 线程迁核和 CPU/IRQ 亲和性 |
| `run_timer_softirq` | 定时器工作 | 轮询周期、超时频率、唤醒速率 |
| `rcu_core_si` | RCU 回调 | 连接/对象抖动和清理活动 |
| `net_tx_action` | 延后的 TX 工作 | TX 队列压力、包速率和 NIC 计数器 |

### 只采样 Redis 线程实际执行的软中断

`/proc/stat` 的 `softirq` 字段是全机、按 CPU 的累计 jiffies，不能单独表示
Redis 的 `si`。若系统安装了 `bpftrace`，使用 `irq:softirq_entry` 和
`irq:softirq_exit` 的时间差，能够统计 Redis 线程真正处于软中断处理程序内的
累计时间。

必须按进程 TGID 过滤，而不是按 `comm` 过滤。proxy I/O 和 SuperNode worker
可能修改线程名，但它们仍属于同一个 `redis-server` 进程：

```bash
SERVER_PID="${SERVER_PID:-$(pgrep -n -x redis-server)}"

sudo bpftrace -e "
tracepoint:irq:softirq_entry /pid == $SERVER_PID/ {
  @start[tid, args->vec] = nsecs;
}
tracepoint:irq:softirq_exit /@start[tid, args->vec]/ {
  \$start = @start[tid, args->vec];
  @time_us[tid, args->vec] = sum((nsecs - \$start) / 1000);
  @count[tid, args->vec] = count();
  delete(@start[tid, args->vec]);
}
interval:s:30 {
  exit();
}
END {
  print(@time_us);
  print(@count);
}
"
```

输出键中的 `tid` 是 Redis 的具体线程，可用下列命令映射到线程名和运行 CPU：

```bash
ps -T -p "$SERVER_PID" -o pid,tid,psr,comm
```

输出键中的 `vec` 是软中断类型。常见的向量编号为 `1`（`TIMER`）、
`3`（`NET_RX`）、`7`（`SCHED`）和 `9`（`RCU`）。必须以被测主机的
`/proc/softirqs` 中的名称为准，不应仅依赖不同内核配置下可能变化的向量编号。
`time_us` 是对应 Redis 线程实际执行该软中断的累计时间，`count` 是进入次数。

该统计有意排除 `ksoftirqd`：若网络工作已转移到 `ksoftirqd/N`，它不属于 Redis
线程实际执行的 `si`，但仍可能抢占相同 CPU 并影响延迟。此类 CPU 干扰应使用全机
`perf record -a` 单独确认；Redis 调用路径的归因仍以 server-only 火焰图为主。

## 优化实验

### `NET_RX` 占主导

目标是减少每个完成请求对应的包数和唤醒次数，而不是将记账隐藏或迁移到别处。

1. 分别扫描 `PIPELINE` 和 `BATCH`。有效的组合应降低每请求 `NET_RX` 时间，
   同时保持或改善 p99 延迟。
2. 确认请求读取和响应发布仍采用批量方式。在 proxy 路径中检查软中断栈附近的
   `recv`/`writev`；大量小响应写入会同时提高包率和唤醒率。
3. 确认 benchmark 流量是否使用 loopback。出现 `loopback_xmit`、`__netif_rx`
   或 `process_backlog` 的栈表明 Redis 可在自己的 CPU 上内联处理对端包；此时
   仅迁移物理 NIC IRQ 并不能消除这部分开销。
4. 对物理 NIC 流量，将 RSS 队列和 NIC IRQ 映射到避开饱和 proxy/SuperNode 核心
   的 CPU，同时保持预期网络 worker 核心具有足够的局部性。使用
   `/proc/interrupts`、每 CPU `%soft`、QPS 和 p99 验证结果。
5. 仅以受控实验方式评估 GRO/GSO/TSO 与 NIC 中断合并。它们可能提升包处理效率，
   但也可能恶化 p99 延迟。

不要为了降低中断次数而直接引入 busy polling。它通常会把包处理移入 Redis
线程，反而增加归因到该线程的 CPU 开销。

### `SCHED` 占主导

为 proxy I/O worker、SuperNode worker 和 NIC IRQ/RSS 队列指定明确的 CPU
集合。除非火焰图证明局部性有利，否则不要把繁忙的 proxy I/O 线程和高负载 RX
队列混在一起。亲和性调整后，检查 `run_rebalance_domains` 是否消失，并确认 QPS
和 p99 确实改善，而非仅将工作迁移到其他核心。

### `TIMER` 或 `RCU` 占主导

对于 `TIMER`，检查高频轮询、超时处理和周期性唤醒循环。仅在确认延迟要求后，才
通过合并周期性工作或增大周期来优化。

对于 `RCU`，将采样窗口与连接抖动、分配/释放活动、请求缓冲区回收和测试收尾相关联。
不要将只在初始化或关闭阶段观察到的 RCU 当作稳态请求成本来优化。

## 验收标准

只有同一 workload 同时满足下列条件，优化才可接受：

1. 目标内联软中断栈，或其每请求耗时下降；
2. QPS 持平或更高；
3. p99 延迟持平或更低；
4. CPU 开销没有仅仅转移给其他 Redis 线程、其他 CPU 或 `ksoftirqd`。

若第 4 条无法确定，对相同时间区间做全机采样：

```bash
sudo perf record -a -F 99 -g -e cycles -- sleep 30
```

该采样仅用于回答干扰来源问题。归因 Redis 调用路径中的内联软中断成本时，仍以
server-only 火焰图为主产物。
