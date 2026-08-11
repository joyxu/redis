# 最佳实践

## 简介

本章节提供 HPC-Redis 在生产与压测场景下的典型使用方式与调优经验，覆盖：

- `redis-cli` 内置 VEMB V16 快速路径的正确用法。
- `memtier_benchmark` 通过客户端 SDK 压测 HPC-Redis 的方法。
- 单机 VEMB V16 端到端部署示例。
- 跨节点多 SuperNode 部署示例。
- 性能调优要点。
- SVE flat load 与 memcpy 搬运路径的场景对比与切换方法。

**限制说明：**

- `redis-cli` 的 VEMB V16 快速路径**不支持 RESP pipeline 批处理**，仅适用于功能验证与小规模手工测试。大规模压测请使用 SDK / `memtier_benchmark`。
- HPC-Redis 当前依赖鲲鹏 SVE 指令集与 UB.MEM 设备，跨架构（x86_64）平台不在支持范围内。
- VEMB V16 协议通过 6379 端口与 RESP 复用，无需新增独立端口；嗅探机制根据首包自动分流。

---

## redis-cli VEMB V16 快速路径使用

`redis-cli` 在命令行带 `--vemb-v16-dim <N>` 参数时，`VADD` / `VEMB` / `VSIM` / `VREM` 命令会自动走 VEMB V16 二进制协议。

```bash
# 写入（生成 1..300 序列作为向量）
./output/src/redis-cli -p 6379 --vemb-v16-dim 300 \
    VADD myset elem1 $(seq 1 300 | tr '\n' ' ')

# 读取
./output/src/redis-cli -p 6379 --vemb-v16-dim 300 VEMB myset elem1

# 相似度
./output/src/redis-cli -p 6379 --vemb-v16-dim 300 \
    VSIM myset elem1 $(seq 1 300 | tr '\n' ' ')

# 删除
./output/src/redis-cli -p 6379 --vemb-v16-dim 300 VREM myset elem1
```

**注意点：**

- `--vemb-v16-dim` 参数值必须与服务端 `--vemb-v16-dim` 一致，否则帧解析失败。
- 同一 key 重复测试可加 `-r <count>` 让 `redis-cli` 重复发送命令。
- 大规模 / 高并发压测不要使用 `redis-cli`，改用 SDK 或 `memtier_benchmark`。

## memtier_benchmark 调用 SDK 压测

`memtier_benchmark` 的自定义分支（`UnifiedBus/memtier_benchmark`）已通过链接 `libvemb_v16_client.a` 集成 VEMB V16 协议，支持 `--protocol vemb_v16` 模式直接压测 HPC-Redis。

```bash
MEMTIER=/root/gqs/codespace/UnifiedBus/memtier_benchmark/memtier_benchmark

# 预填充（写入 100000 个向量）
taskset -c 96-191 $MEMTIER \
    --protocol vemb_v16 --vemb-v16-dim 300 \
    -s 127.0.0.1 -p 6379 -t 1 -c 1 -n 100000 \
    --ratio=1:0 --key-pattern=S:S --key-prefix=item: \
    --key-minimum=1 --key-maximum=100000

# 读吞吐测试（VEMB_INLINE，随机 key，64 线程 × 4 连接，pipeline 32，30 秒）
taskset -c 96-191 $MEMTIER \
    --protocol vemb_v16 --vemb-v16-dim 300 \
    -s 127.0.0.1 -p 6379 -t 64 -c 4 --pipeline=32 --test-time=30 \
    --ratio=0:1 --key-pattern=R:R --key-prefix=item: \
    --key-minimum=1 --key-maximum=100000
```

`memtier_benchmark` 关键参数说明：

| 参数 | 说明 |
| --- | --- |
| `--protocol vemb_v16` | 启用 VEMB V16 二进制协议（而非 RESP）。 |
| `--vemb-v16-dim 300` | 向量维度，需与服务端一致。 |
| `--ratio=1:0` / `--ratio=0:1` | 写入 / 读取比例。`1:0` 纯写、`0:1` 纯读。 |
| `--key-pattern=S:S` | 顺序写（Set）。`R:R` 随机读（Read）。 |
| `-t <threads> -c <clients>` | 线程数 × 每线程连接数 = 总连接数。 |
| `--pipeline=<N>` | 单连接 pipeline 深度。 |
| `--test-time=<s>` | 测试时长（秒）。与 `-n <count>` 二选一。 |

## 单机 VEMB V16 端到端部署

适用于单台鲲鹏服务器上的功能验证与吞吐压测。典型配置：server 绑 NUMA 0（核心 0-95），client 绑 NUMA 1（核心 96-191）。

```bash
HPC=/root/gqs/codespace/UnifiedBus/hpc-redis
MEMTIER=/root/gqs/codespace/UnifiedBus/memtier_benchmark/memtier_benchmark
MANIFEST=$HPC/examples/vemb_v16_warm_regions_111.yaml
DIM=300; NUM_KEYS=100000; KEY_PREFIX="item:"

# 1. 启动 redis-server（pio=32 snw=64 甜点配比）
taskset -c 0-95 $HPC/src/redis-server \
  --port 6379 --bind 0.0.0.0 --protected-mode no \
  --vemb-v16-enabled yes --vemb-v16-dim $DIM --vemb-v16-max-vectors 131072 \
  --vemb-v16-warm-regions-manifest "$MANIFEST" \
  --vemb-v16-reset-warm-regions yes \
  --vemb-v16-proxy-io-threads 32 --vemb-v16-supernode-workers 64 \
  --daemonize yes --loglevel notice

# 2. 等待端口就绪
for i in $(seq 1 50); do ss -tln | grep -q ':6379 ' && break; sleep 0.2; done

# 3. 预填充
taskset -c 96-191 $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
  -s 127.0.0.1 -p 6379 -t 1 -c 1 -n $NUM_KEYS \
  --ratio=1:0 --key-pattern=S:S --key-prefix=$KEY_PREFIX \
  --key-minimum=1 --key-maximum=$NUM_KEYS

# 4. 读吞吐测试（30 秒）
taskset -c 96-191 $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
  -s 127.0.0.1 -p 6379 -t 64 -c 4 --pipeline=32 --test-time=30 \
  --ratio=0:1 --key-pattern=R:R --key-prefix=$KEY_PREFIX \
  --key-minimum=1 --key-maximum=$NUM_KEYS

# 5. 关闭
$HPC/output/src/redis-cli -p 6379 SHUTDOWN NOSAVE
```

## 跨节点多 SuperNode 部署

适用于双节点场景。每节点各起一个 `redis-server`，客户端通过 `memtier_benchmark --vemb-v16-endpoints` 同时连接两个端点，按 key 一致性哈希自动分流。

### 启动双节点服务

```bash
# server 1
taskset -c 0-95 \
    /root/gqs/codespace/UnifiedBus/hpc-redis/src/redis-server \
    --port 6379 --bind 0.0.0.0 --protected-mode no \
    --vemb-v16-enabled yes --vemb-v16-dim 300 --vemb-v16-max-vectors 131072 \
    --vemb-v16-warm-regions-manifest \
        /root/gqs/codespace/UnifiedBus/hpc-redis/examples/vemb_v16_warm_regions_111.yaml \
    --vemb-v16-reset-warm-regions yes \
    --vemb-v16-proxy-io-threads 32 --vemb-v16-supernode-workers 64 \
    --daemonize yes --loglevel notice

# server 2
taskset -c 0-95 \
    /root/gqs/codespace/UnifiedBus/hpc-redis/src/redis-server \
    --port 6379 --bind 0.0.0.0 --protected-mode no \
    --vemb-v16-enabled yes --vemb-v16-dim 300 --vemb-v16-max-vectors 131072 \
    --vemb-v16-warm-regions-manifest \
        /root/gqs/codespace/UnifiedBus/hpc-redis/examples/vemb_v16_warm_regions_112.yaml \
    --vemb-v16-reset-warm-regions yes \
    --vemb-v16-proxy-io-threads 32 --vemb-v16-supernode-workers 64 \
    --daemonize yes --loglevel notice
```

### 跨节点压测（memtier_benchmark）

每节点先各自 prefill 自己的 region（key 由 memtier 客户端按一致性哈希分流，两节点的 `key-maximum` 需保持一致，保证同一 key 永远路由到同一节点）：

```bash
# HW02 作为 client，同时压两台 server
MEMTIER=/root/gqs/codespace/UnifiedBus/memtier_benchmark/memtier_benchmark

# 1) 预填充：每节点 prefill 自己负责的那一半 key（memtier 按 key 哈希分流写入）
taskset -c 0-95 $MEMTIER \
    --protocol vemb_v16 --vemb-v16-dim 300 \
    --vemb-v16-endpoints=192.168.90.111:6379,192.168.90.112:6379 \
    -t 64 -c 4 --pipeline=32 -n 100000 \
    --ratio=1:0 --key-pattern=S:S --key-prefix=item: \
    --key-minimum=1 --key-maximum=100000

# 2) 读吞吐测试（key 哈希分流到对应节点）
taskset -c 0-95 $MEMTIER \
    --protocol vemb_v16 --vemb-v16-dim 300 \
    --vemb-v16-endpoints=192.168.90.111:6379,192.168.90.112:6379 \
    -t 64 -c 4 --pipeline=32 --test-time=60 \
    --ratio=0:1 --key-pattern=R:R --key-prefix=item: \
    --key-minimum=1 --key-maximum=100000
```


```bash
# 3) 关闭双机
/root/gqs/codespace/UnifiedBus/hpc-redis/output/src/redis-cli -h 192.168.90.111 -p 6379 SHUTDOWN NOSAVE
ssh root@192.168.90.112 "/root/gqs/codespace/UnifiedBus/hpc-redis/output/src/redis-cli -p 6379 SHUTDOWN NOSAVE"
```

## 性能调优

### pio / snw 配比

VEMB V16 的吞吐主要受两个线程池影响：

| 线程池 | 参数 | 作用 |
| --- | --- | --- |
| Proxy I/O 线程 | `--vemb-v16-proxy-io-threads`（pio） | 承接 VEMB V16 帧的读取 / 解析 / 转发。 |
| SuperNode workers | `--vemb-v16-supernode-workers`（snw） | 处理三层缓存查询与向量计算。 |


### NUMA 绑核

所有压测脚本应固定 server 与 client 的 NUMA 节点与核心范围，避免跨 NUMA 内存访问带来的延迟抖动。

| 角色 | 推荐配置 |
| --- | --- |
| server | `taskset -c 0-95`（NUMA 0，核心 0-95） |
| client（memtier / SDK 程序） | `taskset -c 96-191`（NUMA 1，核心 96-191） |

未绑核的压测数据不具备可比性，请在测试报告中显式声明绑核配置。


## SVE flat load 与 memcpy 场景对比

HPC-Redis 默认编译带 `-DUSE_ARM_SVE`，向量搬运函数 `sve_streaming_load_f32` 使用 `svld1_f32`（SVE `ld1w` 指令）。如需对比 SVE 与 `memcpy` 的性能差异，可使用仓库自带的端到端对比脚本 `benchmark/sve_vs_memcpy_e2e.sh`。

### 脚本功能

脚本通过 git worktree 构建两个版本的 `redis-server`：

- **SVE 版**：主目录默认编译，带 `-DUSE_ARM_SVE`。
- **memcpy 版**：在 worktree 中移除 `-DUSE_ARM_SVE` 并禁用 `sve_config.h` 的自动检测，强制走 `memcpy` 分支。

脚本会自动：

1. 编译两个版本的 `redis-server`。
2. 用 `objdump` 统计两版的 `ld1w` 指令数，验证 SVE 开关确实生效（两版差值 > 0）。
3. 启动 server、预填充、跑读吞吐测试，汇总两版的 ops/sec 与 p50 / p99 延迟。
4. 输出 SVE / memcpy 吞吐比。

### 使用方法

```bash
# 远端 UB 内存场景（默认，使用 _111_remote.yaml manifest）
bash benchmark/sve_vs_memcpy_e2e.sh remote

# 本地 pfn-map 场景（使用 _111.yaml manifest）
bash benchmark/sve_vs_memcpy_e2e.sh local
```

可选环境变量覆盖（不设则用默认值）：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `MEMTIER` | `../memtier_benchmark/memtier_benchmark` | memtier 可执行路径。 |
| `SERVER_CPUSET` | `1-96` | server 绑核范围。 |
| `CLIENT_CPUSET` | `97-191` | client 绑核范围。 |
| `SERVER_NUMA` | `0` | server NUMA 节点。 |
| `CLIENT_NUMA` | `1` | client NUMA 节点。 |
| `PIO` / `SNW` / `T` / `C` | `32` / `64` / `64` / `4` | pio / snw / 线程数 / 连接数。 |
| `DUR` | `30` | 测试时长（秒）。 |
| `NUM_KEYS` | `100000` | 预填充 key 数。 |
| `WORKDIR` | `/tmp/sve_e2e` | 中间产物目录（nosve-tree / logs）。 |

### 场景对比结论数据

| 场景 | SVE / memcpy 吞吐比 | 说明 |
| --- | --- | --- |
| 本地 pfn-map（`111.yaml`） | 1.00x | 本地热区命中缓存，SVE 与 memcpy 持平。 |
| 远端 UB 内存（`111_remote.yaml`） | 2.41x（+141%） | 远端访问延迟高，SVE 连续 `ld1w` 显著优于 `memcpy`。 |

> 场景选择建议：生产环境若使用远端 UB 内存作为热区，保持 SVE 开启（默认）；纯本地 DRAM 场景 SVE 与 memcpy 无差异，保持默认即可。

---
