# VEMB v16 PMU 指标采集与性能诊断

日期：2026-10-17

本文说明如何使用 Linux `perf stat` 获取 VEMB v16 服务端线程的硬件性能计数器（PMU），以及如何解释这些指标并决定下一步优化方向。本文重点覆盖 SNW（`vemb-sn-*`）线程的缓存和地址转换诊断。

## 1. 指标来源与边界

PMU 计数来自服务端进程中指定线程的硬件计数器，不是火焰图的采样栈占比，也不是 `mpstat` 的 CPU 百分比。

当前诊断使用的线程是：

```text
vemb-sn-00 ... vemb-sn-03
```

线程级计数覆盖这些线程执行期间的全部内存访问，包括 SNW 轮询、队列处理、TLC 查找、slot metadata 校验以及其他工作。因此，PMU 计数可以证明线程遇到了缓存或地址转换压力，但不能单独证明某一次 miss 一定来自某一条 `slot_meta` load。

当前 1M key 诊断的原始文件为：

- [SNW 缓存 PMU](/Users/szza/codespace/work/hpc-redis/perf/p4_s4_1m_pmu_sn_target_20261017/server/server.pmu.sn.csv)
- [SNW dTLB PMU](/Users/szza/codespace/work/hpc-redis/perf/p4_s4_1m_pmu_sn_dtlb_20261017/server/server.pmu.sn.dtlb.csv)

## 2. 采集方式

### 2.1 获取 SNW TID

服务启动并完成预填充后，从服务端 PID 获取线程 ID：

```bash
ps -T -p <redis_pid> -o tid=,comm= \
  | awk '$2 ~ /^vemb-sn-/'
```

不要使用进程 PID 代替线程 TID。为了只观察 SNW，需要确认输出中没有 `vemb-io-*` 线程。

### 2.2 SNW 缓存计数

使用四个 SNW TID 采集执行周期、退休指令、L1 数据加载和 LLC 加载：

```bash
perf stat \
  -t <snw_tid_0>,<snw_tid_1>,<snw_tid_2>,<snw_tid_3> \
  -e cycles,instructions,\
L1-dcache-loads,L1-dcache-load-misses,\
LLC-loads,LLC-load-misses \
  -- sleep 40 \
  2> server.pmu.sn.txt
```

### 2.3 SNW 地址转换计数

地址转换事件单独采集：

```bash
perf stat \
  -t <snw_tid_0>,<snw_tid_1>,<snw_tid_2>,<snw_tid_3> \
  -e dTLB-loads,dTLB-load-misses,dtlb_walk \
  -- sleep 40 \
  2> server.pmu.sn.dtlb.txt
```

分组采集可以减少硬件计数器不够时的 multiplex。每份输出都应检查：

1. 是否出现 `<not counted>` 或 `<not supported>`。
2. `time elapsed` 是否覆盖完整 workload 稳态窗口。
3. 使用 `perf stat -I 1000` 时，事件的 `enabled/running` 是否接近 100%。

### 2.4 使用跨机脚本的通用 PMU 采集

`scripts/run_aeron_cross_node_flamegraph.sh` 的 `SERVER_PMU=1` 会在服务端启动后枚举 `vemb-io-*` 和 `vemb-sn-*`，然后执行线程级 `perf stat`：

```bash
SERVER_PMU=1 \
SERVER_PMU_EVENTS='{cycles,instructions,L1-dcache-loads,L1-dcache-load-misses,LLC-loads,LLC-load-misses}' \
PROFILE=0 BUILD=skip \
NUM_KEYS=1000000 THREADS=64 CLIENTS=4 PIO=4 SNW=4 \
bash scripts/run_aeron_cross_node_flamegraph.sh
```

脚本使用的核心形式是：

```bash
perf stat --per-thread --tid "$tids" -I 1000 -x ';' \
  -e "$events" -o server.pmu.csv -- sleep "$duration"
```

这种通用模式会同时统计 PIO 和 SNW。若要得到本文中的 SNW 专属百分比，应只传入 SNW TID，或从输出中仅聚合 `vemb-sn-*` 行。`PROFILE=1` 的 `perf record` 用于生成火焰图；它和 `perf stat` PMU 计数是两种不同采集，不应混用解释。

ARM PMU 对事件别名的支持依赖内核和处理器实现。正式实验前应检查：

```bash
perf list | grep -E 'L1-dcache|LLC|dTLB|dtlb_walk'
```

如果事件不存在、被报告为 unsupported，不能用替代事件直接宣称同一指标。

## 3. 指标含义

| 指标 | 表示什么 | 重点解释 |
|---|---|---|
| `cycles` | 线程实际消耗的 CPU 周期 | 反映总执行时间成本。`cycles/op` 上升通常意味着执行或等待成本增加。 |
| `instructions` | 已退休的机器指令数 | 反映软件实际执行的工作量。指令数升高通常指向额外分支、锁、查找或协议处理。 |
| `IPC` | `instructions / cycles` | 反映每个周期退休的指令数。指令数不高但 cycles 很高、IPC 下降，通常更符合访存或后端等待。 |
| `L1-dcache-loads` | L1 数据缓存观察到的加载访问数 | 是 L1 load miss rate 的分母，不是字节数，也不严格等于 cache line 数。 |
| `L1-dcache-load-misses` | 未在 L1 数据缓存命中的加载数 | L1 miss 后仍可能在 L2 或 LLC 命中，因此不等于访问了远端内存。 |
| `LLC-loads` | 被最后级缓存（LLC）事件统计到的加载数 | 事件的精确定义依赖 ARM PMU；它通常不是所有 L1 loads 的简单子集。 |
| `LLC-load-misses` | 在 LLC 中也未命中的加载数 | 通常需要访问更远的内存层级或设备映射，延迟和带宽代价更高。 |
| `dTLB-loads` | 数据加载触发的地址转换查询数 | 统计虚拟地址转换查询，不代表数据大小。 |
| `dTLB-load-misses` | dTLB 未命中的数据地址转换数 | 需要访问更低级 TLB，或进一步触发页表遍历。 |
| `dtlb_walk` | 硬件页表遍历事件数 | 表示地址转换确实进入了 page walk；不一定与 dTLB miss 一一对应。 |
| `time elapsed` | 计数器统计窗口的墙钟时间 | 用于确认采样是否覆盖完整稳态 workload。 |
| `enabled/running` | 事件被启用时间与实际运行时间 | `running` 明显低于 `enabled` 时发生 multiplex，计数需要谨慎解释。 |

### 3.1 为什么 L1 miss 和 LLC miss 可以同时存在

一次加载可能在 L1 未命中，但在 L2 或 LLC 命中。因此：

- L1 miss 说明近端 L1 局部性不足。
- LLC miss 说明在更深缓存层次仍未找到数据。
- 两个比例不能相加，也不能把 L1 miss 直接等同于内存访问。

### 3.2 `dtlb_walk` 与 dTLB miss 的区别

`dTLB-load-misses` 是地址转换查询没有在当前 dTLB 命中的计数；`dtlb_walk` 是实际发生页表遍历的硬件事件。由于硬件可能使用更低级 TLB、合并请求或统计不同阶段，两者不应强行按相同次数解释。

## 4. 计算方式

先按线程类型过滤，再把所有 SNW 线程的同名事件相加。不要直接平均各线程百分比，因为各线程的访问量可能不同。

```text
L1 miss rate =
  sum(L1-dcache-load-misses) / sum(L1-dcache-loads) * 100

LLC miss rate =
  sum(LLC-load-misses) / sum(LLC-loads) * 100

dTLB miss rate =
  sum(dTLB-load-misses) / sum(dTLB-loads) * 100
```

如果 workload 输出了成功逻辑操作数，还可以计算每操作计数：

```text
cycles/op       = sum(cycles) / logical_ops
instructions/op = sum(instructions) / logical_ops
LLC misses/op   = sum(LLC-load-misses) / logical_ops
dTLB walks/op   = sum(dtlb_walk) / logical_ops
```

每操作计数比单纯百分比更适合比较 100K 与 1M 工作集，因为它能区分“请求数变化”和“单请求访问成本变化”。

## 5. 本次 1M SNW 结果

四个 SNW 线程聚合后的原始计数为：

| 事件 | 聚合计数 | 计算结果 |
|---|---:|---:|
| `L1-dcache-loads` | 16,247,279,293 | - |
| `L1-dcache-load-misses` | 837,025,035 | `5.15%` |
| `LLC-loads` | 483,931,837 | - |
| `LLC-load-misses` | 329,929,592 | `68.18%` |
| `dTLB-loads` | 40,615,808,322 | - |
| `dTLB-load-misses` | 645,076,404 | `1.59%` |
| `dtlb_walk` | 489,599,392 | 独立计数 |

因此本轮的直接判断是：SNW 的 LLC 层局部性明显不足，地址转换也有额外压力，但仅凭线程级 PMU 不能把这些事件精确归因到某个 `slot_meta` 字段。

## 6. 对性能优化的指导意义

### 6.1 `cycles/op` 上升，`instructions/op` 基本不变或下降

优先怀疑访存延迟、缓存未命中、地址转换或设备访问等待。此时不应先做协议层或哈希表重构，应进一步比较 L1、LLC、dTLB 和页表遍历。

### 6.2 L1 miss 高，LLC miss 低

数据主要在更深缓存中，问题偏向 L1 局部性或访问顺序。可以评估 metadata 紧凑布局、相邻访问、批内排序和减少重复验证，但不宜直接引入 hugepage 或修改协议。

### 6.3 LLC miss 高

这是大工作集随机访问的重点信号。当前场景下应优先检查：

- location cache 是否真的覆盖 active-read 工作集。
- cache hit 后是否仍然随机读取分散的 `slot_meta`。
- `slot_meta` 与 payload 是否相距过远。
- region/slot 的物理布局是否破坏了局部性。
- 是否存在每请求重复读取相同 metadata 的路径。

可以用 100K active-read 与 1M active-read、metadata 紧凑布局和受控 cache-hit 快路径做 A/B。不能仅凭 LLC miss 直接删除 generation、owner 或 seqlock 校验。

### 6.4 dTLB miss 或 `dtlb_walk` 明显升高

优先检查映射和页表粒度：

- `/proc/<pid>/smaps` 中的 `KernelPageSize` 和 `MMUPageSize`。
- UB 设备 mmap 是否真正支持所需页表粒度。
- metadata 是否跨越过多的 4 KiB 页。
- region 访问是否随机到足以驱逐 dTLB 项。

应用侧简单把长度向上取整，不等于设备映射一定使用 hugepage；需要有驱动或设备支持的证据后再改变映射策略。

### 6.5 `instructions/op` 或 PIO instructions/op 上升，但 LLC miss 不升

优先检查软件路径：代理批处理、队列轮询、锁竞争、哈希查找、路由和响应组装。此时不能把性能下降全部归因于 UB 读取。

### 6.6 所有事件的 `running` 明显不足

先修正采集方式：减少同时采集的事件、分组采集或缩短事件集合。计数器发生 multiplex 时，不能直接比较不同实验的原始计数。

## 7. 解释限制

1. PMU 事件名称是内核和处理器相关的别名，必须以目标机器 `perf list` 和 PMU 文档为准。
2. `perf stat` 是线程级聚合，不能定位到 `slot_meta` 的单条 load；需要 `perf record` 地址采样、Arm SPE 或源码级插桩才能进一步归因。
3. `LLC-loads` 的硬件定义可能与“所有 L1 miss”不同，不能把两个事件当成严格的包含关系。
4. `dtlb_walk` 不是页数、不是字节数，也不是 dTLB miss 的一对一映射。
5. 火焰图中的函数占比是采样栈占比；PMU miss rate 是硬件计数比值；两者不能直接比较百分数。
6. 线程级计数包含轮询和其他服务工作，不能直接解释成一次 `slot_meta` 访问的固定成本。

## 8. 复现实验检查表

- 使用相同服务端构建、manifest、PIO/SNW、客户端和 workload。
- 记录 workload 的 `logical_ops`、QPS、延迟和错误计数。
- 记录被采集的 TID，确认只包含目标线程。
- 缓存事件和 dTLB 事件尽量分组采集。
- 保存原始 `perf stat` 文本，不只保存计算后的百分比。
- 检查 `not counted`、`not supported`、`time elapsed` 和 `enabled/running`。
- 同时报告总计数、miss rate 和每操作计数。
- 任何优化结论至少需要相邻 A/B 轮次和正确性计数为零。
