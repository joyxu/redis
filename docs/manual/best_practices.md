# 最佳实践

## 简介

本章节提供 HPC-Redis 在生产与压测场景下的典型使用方式与调优经验，覆盖：

- `redis-cli` 内置 VEMB V16 快速路径的正确用法。
- `memtier_benchmark` 通过客户端 SDK 压测 HPC-Redis 的方法。
- 单机 VEMB V16 端到端部署示例（TCP 与 Aeron 两种数据面）。
- 跨节点多 SuperNode 部署示例。
- 在线扩容（scaleout）与高可用（HA）回归。
- 高维向量（dim 1024/2048/3072）与 VSIM_2KEY 负载。
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

`memtier_benchmark` 的自定义分支（`UnifiedBus/memtier_benchmark`）已通过链接 `libvemb_v16_client.a` 集成 VEMB V16 协议，支持 `--protocol vemb_v16` 模式直接压测 HPC-Redis。编译方法（先 SDK 后 memtier，目录须同级）见 [安装指南](installation_guide.md#编译-memtier_benchmarkvemb-v16-压测工具)。

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

## 单机 Aeron 端到端部署（本地回环）

Aeron 数据面 = TCP 控制面 + UB 环数据面。本地回环默认设备布局：请求环 shmdev1、warm region shmdev2（manifest 默认）、响应环 shmdev3。

```bash
HPC=/root/gqs/codespace/UnifiedBus/hpc-redis
MEMTIER=/root/gqs/codespace/UnifiedBus/memtier_benchmark/memtier_benchmark
DIM=300; NUM_KEYS=100000; KEY_PREFIX="item:"

# 1. 启动 server（transport=aeron + 两个环设备）
taskset -c 0-95 $HPC/src/redis-server \
  --port 6379 --bind 0.0.0.0 --protected-mode no \
  --vemb-v16-enabled yes --vemb-v16-dim $DIM --vemb-v16-max-vectors 131072 \
  --vemb-v16-warm-regions-manifest $HPC/examples/vemb_v16_warm_regions_111.yaml \
  --vemb-v16-reset-warm-regions yes \
  --vemb-v16-transport aeron \
  --vemb-v16-aeron-ub-path /dev/obmm_shmdev1 \
  --vemb-v16-aeron-response-ub-path /dev/obmm_shmdev2 \
  --vemb-v16-proxy-io-threads 32 --vemb-v16-supernode-workers 64 \
  --daemonize yes --loglevel notice

# 2. 预填充 + 读吞吐（memtier 指定 aeron 传输）
taskset -c 96-191 $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
  --vemb-v16-transport=aeron -s 127.0.0.1 -p 6379 \
  -t 1 -c 1 -n $NUM_KEYS --ratio=1:0 --key-pattern=S:S \
  --key-prefix=$KEY_PREFIX --key-minimum=1 --key-maximum=$NUM_KEYS

taskset -c 96-191 $MEMTIER --protocol vemb_v16 --vemb-v16-dim $DIM \
  --vemb-v16-transport=aeron -s 127.0.0.1 -p 6379 \
  -t 64 -c 4 --pipeline=32 --test-time=30 --ratio=0:1 --key-pattern=R:R \
  --key-prefix=$KEY_PREFIX --key-minimum=1 --key-maximum=$NUM_KEYS
```

一键脚本（参数全默认、可 env 覆盖）：`bash scripts/run_aeron_best.sh`。

## 跨节点 Aeron 部署

与单机的差异两处：服务端必须通告对外地址（`--vemb-v16-tcp-host <本机IP>`）；客户端（SDK）需加载 peer-view manifest 把服务端设备路径翻译为本节点 import 设备（`vemb_v16_client_configure_ub_peer_view`，manifest 见 `examples/vemb_v16_ub_peer_view_112_to_111*.yaml`）。完整流程见 `scripts/run_aeron_cross_node_flamegraph.sh`（单命令全流程，含拓扑发布与预热）与 `benchmark/vemb_v16_aeron_cluster_tput.sh`（集群口径，DIM 变体自动选 manifest）。

## 在线扩容（scaleout）回归

```bash
# TCP 数据面：三段采集（baseline/during/after），结果落 benchmark/results/scaleout/
bash scripts/hpc_redis_scaleout_throughput.sh
# 冒烟：TEST_TIME=3 PREFILL_KEYS=1000 bash scripts/hpc_redis_scaleout_throughput.sh

# Aeron 数据面扩容（Mac 侧执行）
bash scripts/vemb_v16_scaleout_ub_cluster_111_to_112.sh
```

判据：迁移完成 `scaleout_all_sources_done=1`；扩容中（during 段）吞吐不归零即业务不中断。

## 高可用（HA）failover 回归

```bash
# 1. keepalived 部署（详见使用手册 HA 章节；先 leader 后 standby 启动）
scripts/deploy_keepalived_ha.sh --node 111 --priority 100 --vip 192.168.90.202/24 deploy
scripts/deploy_keepalived_ha.sh --node 112 --priority 90  --vip 192.168.90.202/24 deploy

# 2. 端到端 failover 回归（副本追平 + 主备切换 + 反向）
benchmark/tlc_ha_replica_ub_111_to_112.sh        # UB 副本恢复
benchmark/tlc_ha_aeron_failover_111_to_112.sh    # Aeron failover；反向：INITIAL_NODE=112 CLIENT_NODE=112
```

## 高维向量与 VSIM_2KEY

```bash
# 高维（以 dim=1024 为例；2048/3072 同理，manifest 按后缀选择）
DIM=1024 MANIFEST=examples/cluster_111_dim1024.yaml ...（启动命令同上，仅换 dim 与 manifest）

# VSIM_2KEY 负载（双 key 取回 + 服务端余弦）
$MEMTIER --protocol vemb_v16 --vemb-v16-dim 300 \
  --vemb-v16-vsim-key-key -s 127.0.0.1 -p 6379 -t 64 -c 4 --pipeline=32 \
  --test-time=30 --ratio=0:1 --key-pattern=R:R --key-prefix=item: \
  --key-minimum=1 --key-maximum=100000
# 双节点对比回归：benchmark/run_vsim_2key_2node.sh
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
