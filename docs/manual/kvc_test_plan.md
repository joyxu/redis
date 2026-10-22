# KVC 测试方案：四条读路径对比

## 简介

本方案给出 Mooncake Store 全栈（master + 段 owner + 原生基准进程 `store_kv_bench_native`）下，四条 TE 读路径在统一测试条件下的对比测试方法：

| 读路径 | `--protocol` | 数据通道 | 特点 |
| --- | --- | --- | --- |
| TE over TCP | `tcp` | 以太网（内核协议栈） | 通用基线，无专用硬件依赖 |
| TE over URMA | `ub` | 鲲鹏 URMA | 绕过内核协议栈，需 URMA 设备 |
| TE over ubmem | `ubmem` | UB.MEM 设备窗口直接拷贝 / 镜像窗访问 | 同机为内存拷贝语义；跨节点经 UB fabric |
| TE over cxl | `cxl` | 设备窗口偏移拷贝（CXL 语义映射到 UB 设备） | 上游标准路径，与 ubmem 同构参照 |

每条路径在**本地**（driver 与 owner 同机）与**跨节点**（owner 与 driver 分属两节点）两种拓扑下测试，采集读吞吐（MiB/s）与 p50 延迟随读线程数（8-128，跨节点可扩展至 256）的变化。

## 测试环境与统一条件

### 环境要求

- 两台鲲鹏节点（示例：owner = `192.168.90.161`（HW01），bench = `192.168.90.162`（HW02），以实际环境为准），root 互信 SSH。
- UB.MEM 设备 `/dev/obmm_shmdev*`：每节点分为本地池窗（本节点 export）与镜像窗（对端 import），编号以环境 `obmmctl` 布局为准。
- URMA 设备（如 `udma1`）由 urma 库在进程初始化时动态枚举，无需预先创建设备节点。
- Python 元数据服务（Mooncake python 包）；不可用时可用 master 内嵌元数据服务替代（`-enable_http_metadata_server=true -http_metadata_server_port=8080`，见"1MB 大块参考数据"节）。
- 编译开关：`-DUSE_TCP=ON -DUSE_UB=ON -DUSE_UBMEM=ON -DUSE_CXL=ON -DBUILD_BENCHMARK=ON`，四组测试共用一份编译产物（编译步骤与依赖见 [KVC 用户指南：TE over ubmem](kvc_ubmem_user_guide.md)"编译"节）。

### 系统参数（每组测试前确认）

```bash
sysctl -w net.ipv4.tcp_tw_reuse=1                      # TE TCP one-shot 连接模式需要
sysctl -w "net.ipv4.ip_local_port_range=10240 65535"
```

master 的 `-default_kv_lease_ttl` 统一取 600000 ms（默认 10 s 会导致长时间读测试出现 MISS）。

### 统一测试参数

| 参数 | 取值 | 说明 |
| --- | --- | --- |
| 数据集 | 100000 key × 4096B | 数据集大小是影响吞吐的敏感变量，四组必须一致 |
| 批大小 | `--batch_size=64` | |
| driver 本地缓冲 | `--local_buffer_size=256MB` | |
| owner 段 | `--global_segment_size=4GB` | cxl 组除外（见组 4） |
| 读线程梯度 | 8 / 16 / 32 / 64 / 128 | 跨节点可增加 192 / 256 |
| 每档时长 | `--runtime=20` | |
| fill 阶段 | `--read_threads=1`，写完退出 | 单线程串行写入 |
| key 前缀 | 每组独立（如 `te1` / `ur` / `um` / `lcx`） | 避免与既有数据冲突 |

## 公共准备（owner 节点，每组测试前）

```bash
export MOON=<仓库根>/Mooncake
export LD_LIBRARY_PATH=$MOON/build/mooncake-common

# HTTP 元数据服务（启动一次，全程复用；8080 已监听则跳过）
ss -ltn | grep -q ":8080 " || { \
  PYTHONPATH=<python 模块根> nohup python3 -m mooncake.http_metadata_server \
  --port 8080 > /tmp/meta.log 2>&1 & sleep 2; }

# master（cxl 组使用组 4 的专用 master，其余组通用）
KVC_MASTER_BLOCK_SIZE=4096 nohup $MOON/build/mooncake-store/src/mooncake_master \
  -default_kv_lease_ttl=600000 -http_metadata_server_port=8081 -metrics_port=9004 \
  -logtostderr > /tmp/master.log 2>&1 &
sleep 4
```

driver 公共参数（下文以 `$COMMON` 引用；跨节点时 master / metadata 地址更换为 owner 对外 IP）：

```bash
export COMMON="--master_server=127.0.0.1:50051 \
  --metadata_server=http://127.0.0.1:8080/metadata \
  --num_objects=100000 --value_size=4096 --batch_size=64 \
  --local_buffer_size=$((256*1024*1024))"
```

**执行规则（每组测试前检查）：**

1. **每次 bench 调用使用唯一的 `--local_hostname` 端口**（rpc_meta 注册键，重复注册报 400）。
2. **执行前确认无残留进程**：`pgrep -f store_kv_bench_native` 应为空；更换测试组前
   `pkill -x mooncake_client`。
3. **fill 必须单线程串行执行**：fill 命令携带 `--read_threads=1`，写入完成、进程退出后再执行读梯度。

driver 主要参数速查：

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `--protocol` | `tcp` | 读路径：`tcp` / `ub` / `ubmem` / `cxl` |
| `--key_prefix` | `n1` | 每组使用新的 key 前缀 |
| `--skip_fill` | false | 跳过 fill，复用已有数据 |
| `--read_threads` | 8 | 读线程数 |
| `--runtime` | 30 | 读阶段秒数 |
| `--verify` | true | 读时逐字节校验（`verify_fail` 计数） |
| `--local_hostname` | `127.0.0.1:19500` | 进程注册名，端口必须唯一 |

## 测试组配置

每组流程一致：按组配置启动 owner → 确认就绪 → fill → 读梯度 → 收尾
（`pkill -x mooncake_client; sleep 6; pkill -x mooncake_master; sleep 2`）。
四组差异仅在 owner 侧环境变量与参数。

### 组 1：TE over TCP

| 项 | 取值 |
| --- | --- |
| owner 环境变量 | `MC_TCP_ENABLE_CONNECTION_POOL=1`、`MC_TCP_LANES_PER_PEER=16`（充分发挥 TCP 路径性能） |
| owner 协议 | `--protocol=tcp --port=50052` |
| driver 协议 / 前缀 | `--protocol=tcp` / `te1` |

```bash
export MC_TCP_ENABLE_CONNECTION_POOL=1 MC_TCP_LANES_PER_PEER=16
unset MC_CUSTOM_TOPO_JSON
nohup $MOON/build/mooncake-store/src/mooncake_client --host=127.0.0.1 \
  --global_segment_size=4GB --master_server_address=localhost:50051 \
  --metadata_server=http://127.0.0.1:8080/metadata \
  --protocol=tcp --port=50052 --threads=32 --logtostderr > /tmp/client.log 2>&1 &
sleep 8 && ss -tln | grep -q ':50052 ' && echo CLIENT_UP

BIN=$MOON/build/mooncake-store/benchmarks/store_kv_bench_native
# fill（串行）
$BIN $COMMON --key_prefix=te1 --protocol=tcp \
  --local_hostname=127.0.0.1:21001 --read_threads=1 --runtime=1
# 读梯度
port=21011
for T in 8 16 32 64 128; do
  $BIN $COMMON --key_prefix=te1 --protocol=tcp --skip_fill \
    --read_threads=$T --runtime=20 --local_hostname=127.0.0.1:$port
  port=$((port+1))
done
```

说明：TCP 路径在 ≥64 线程时会发生传输超时（吞吐降至 0.2 MiB/s、大量请求失败、p50 约 60000 ms）。这是单线程 asio 数据面在高并发下的已知行为，并非环境故障，如实记录即可。

### 组 2：TE over URMA（`ub`）

| 项 | 取值 |
| --- | --- |
| owner 环境变量 | `MC_CUSTOM_TOPO_JSON=/tmp/topo.json`（注入 URMA 设备；owner 与 driver 均需设置） |
| owner 协议 | `--protocol=ub --port=50052` |
| driver 协议 / 前缀 | `--protocol=ub` / `ur` |

```bash
unset MC_TCP_ENABLE_CONNECTION_POOL MC_TCP_LANES_PER_PEER
echo '{"cpu:0": [["udma1"], []], "cpu:1": [["udma1"], []]}' > /tmp/topo.json
export MC_CUSTOM_TOPO_JSON=/tmp/topo.json
nohup $MOON/build/mooncake-store/src/mooncake_client --host=127.0.0.1 \
  --global_segment_size=4GB --master_server_address=localhost:50051 \
  --metadata_server=http://127.0.0.1:8080/metadata \
  --protocol=ub --port=50052 --threads=32 --logtostderr > /tmp/client_ub.log 2>&1 &
sleep 8 && ss -tln | grep -q ':50052 ' && echo CLIENT_UP
grep -c "udma1 add to list" /tmp/client_ub.log   # 应为 1
```

driver 的 fill 与读梯度同组 1 形态，更换为 `--protocol=ub --key_prefix=ur`，端口自 21101 / 21111 起递增。

注意：

- URMA 设备名（如 `udma1`）不在 RDMA 设备注册表中，必须经 `MC_CUSTOM_TOPO_JSON` 注入；未注入时 UbTransport 会回退到 mock 设备（日志出现 `mock_urma_device`）并随后崩溃。
- **不要设置 `MC_MS_AUTO_DISC=1`**：auto-discover 会依次安装所有传输层，本机无 RNIC 时 RDMA 安装失败将导致整个初始化中止。

### 组 3：TE over ubmem

| 项 | 取值 |
| --- | --- |
| owner 环境变量 | `MC_UBMEM_DEV_PATH=<本地池窗>`、`MC_UBMEM_DEV_SIZE=8GB` |
| owner 协议 | `--protocol=ubmem --port=50052` |
| driver 协议 / 前缀 | `--protocol=ubmem` / `um` |

```bash
export MC_UBMEM_DEV_PATH=/dev/obmm_shmdev2        # 本地池窗
export MC_UBMEM_DEV_SIZE=$((8*1024*1024*1024))
fuser /dev/obmm_shmdev2 && echo BUSY              # 必须无占用
nohup $MOON/build/mooncake-store/src/mooncake_client --host=127.0.0.1 \
  --global_segment_size=4GB --master_server_address=localhost:50051 \
  --metadata_server=http://127.0.0.1:8080/metadata \
  --protocol=ubmem --port=50052 --threads=32 --logtostderr > /tmp/client_ubm.log 2>&1 &
sleep 8 && ss -tln | grep -q ':50052 ' && echo CLIENT_UP
grep "Mounting ubmem segment" /tmp/client_ubm.log   # 段挂载成功标志
```

driver 的 fill 与读梯度同组 1 形态，更换为 `--protocol=ubmem --key_prefix=um`，端口自 23001 / 23011 起递增。窗口分区规则与详细使用方法见 [KVC 用户指南：TE over ubmem](kvc_ubmem_user_guide.md)。

### 组 4：TE over cxl

与其他组的差异：master 必须以 cachelib + cxl 模式启动（kv Handle 为窗口内偏移，缺少 `--enable_cxl` 时 fill 会发生段错误）；**无独立 owner 进程，fill 进程即段 owner**（段描述符随进程生存周期失效），须在整个读梯度期间保持运行。

| 项 | 取值 |
| --- | --- |
| master | `-memory_allocator=cachelib -enable_cxl=true -cxl_path=<本地池窗> -cxl_size=4GB -rpc_port=50061` |
| 环境变量 | `MC_CXL_DEV_PATH=<本地池窗>`、`MC_CXL_DEV_SIZE=4GB` |
| driver 协议 / 前缀 / master | `--protocol=cxl` / `lcx` / `--master_server=127.0.0.1:50061` |

```bash
export MC_CXL_DEV_PATH=/dev/obmm_shmdev4
export MC_CXL_DEV_SIZE=$((4*1024*1024*1024))
fuser /dev/obmm_shmdev4 && echo BUSY
nohup $MOON/build/mooncake-store/src/mooncake_master \
  -memory_allocator=cachelib -enable_cxl=true \
  -cxl_path=/dev/obmm_shmdev4 -cxl_size=$((4*1024*1024*1024)) \
  -rpc_port=50061 -default_kv_lease_ttl=600000 \
  -http_metadata_server_port=8081 -metrics_port=9004 \
  -logtostderr > /tmp/master_cxl.log 2>&1 &
sleep 5 && grep "Init CXL global allocator" /tmp/master_cxl.log   # 启动标志

CXL_COMMON="--master_server=127.0.0.1:50061 \
  --metadata_server=http://127.0.0.1:8080/metadata \
  --num_objects=100000 --value_size=4096 --batch_size=64 \
  --local_buffer_size=$((256*1024*1024)) --protocol=cxl --key_prefix=lcx"
# fill 后台保活（--runtime 覆盖整个读梯度；日志出现 "Local buffer size is 0" 属正常）
nohup $BIN $CXL_COMMON --local_hostname=127.0.0.1:21501 \
  --read_threads=1 --runtime=400 > /tmp/cxl_fill.log 2>&1 &
until grep -q "fill done: 100000 objects" /tmp/cxl_fill.log; do sleep 2; done
port=21511
for T in 8 16 32 64 128; do
  $BIN $CXL_COMMON --skip_fill --read_threads=$T --runtime=20 \
    --local_hostname=127.0.0.1:$port
  port=$((port+1))
done
pkill -f 'runtime=400'; pkill -f 'enable_cxl=true'; sleep 2
```

## 跨节点测试要点

四组的跨节点部署与本地一致（owner 运行在 owner 节点，bench 在另一节点），差异集中在：

- **公共**：owner 的 `--host` 使用本机**对外 IP**（不能为 `127.0.0.1`）；bench 侧 `$COMMON` 的 master / metadata 地址更换为 owner 对外 IP；同步三个文件到 bench 节点（bench 二进制 + `libkvc_server.so` + `libasio.so`，并设置 `LD_LIBRARY_PATH`）。
- **TCP / URMA 组**：命令仅地址变化；URMA 的 topo.json 在 bench 侧同样生成并 export。
  **多网卡节点 TCP 组必须在 owner 与 bench 两侧都设 `MC_TCP_BIND_ADDRESS=<本机高速网口 IP>`**：TE 的 TCP 数据面地址不取 `--host`（`--host` 只决定段名），未设时取第一块网卡 IP（本环境为 10G 管理口 `192.168.90.x`），数据会被低速口线速钉死。跑前用 `/sys/class/net/<if>/statistics/rx_bytes` 增量核对流量确实走 100G 口 `192.168.1.x`。
- **ubmem 组**：owner 段位于 owner 本地池窗；bench 侧 `MC_UBMEM_DEV_PATH` 填**本节点自己的本地池窗**（本端缓冲使用），并设置
  `MC_UBMEM_PEER_DEV_MAP="<owner IP>:<owner 设备>=<本端镜像设备>"`（读操作经镜像窗完成）。
  先执行单线程 smoke 验证（`--read_threads=1 --runtime=5`），`verify_fail=0`（跨窗口字节级校验）通过后再执行梯度；每档执行后检查 `dmesg | grep -iE "ucmi|mami" | grep -icE "error|fail"` 无增量。
- **cxl 组**：owner 写本地池窗，bench 读本端镜像窗（同一块物理内存，偏移地址恒等）。
  上游 cxl 代码以纯 `O_RDWR` 打开设备，而镜像窗拒绝纯 `O_RDWR`，需经 `LD_PRELOAD` 适配层补充 `O_SYNC` 标志（实现见 `docs/KVC_NATIVE_BENCH_REPRO.md` 跨节点 cxl 节）；fill 保活进程运行在 owner 节点。
- **更换测试组重跑前**：清理残留的 rpc_meta 键（前一进程被强制终止且未执行析构时会残留，重跑报 400），
  `curl -X DELETE "<metadata>/metadata?key=mooncake/rpc_meta/<host:port>"` 或直接更换端口。
- **数据集规模保持 10 万 key**：跨节点吞吐对数据集规模敏感，不同规模的测试数据不可直接对比。

## 结果采集与判读

- 结果仅取 stderr 的 `RESULT` 行；fill 命令读阶段的数字无效，仅以 `fill done` 判断写入完整性。
- `failed` / `verify_fail` 应为 0；非零时先查"常见问题"。
- 吞吐 = `MiB/s` 字段（= kv/s × value_size / 2²⁰）；延迟取 `p50_ms`。
- 不同日期的绝对值存在 ±5% 左右的波动，各路径间的倍数关系（如 URMA 约为 TCP 的 6-7 倍）保持稳定。

结果记录表模板（每种拓扑一张，格式 = MiB/s（p50 ms））：

| 线程 | TE over TCP | TE over URMA | TE over ubmem | TE over cxl |
| ---: | --- | --- | --- | --- |
| 8 | | | | |
| 16 | | | | |
| 32 | | | | |
| 64 | | | | |
| 128 | | | | |

## 参考数据

测试条件：批量 64、10 万 key × 4KB、每档 20 秒、串行 fill（2026-10 实测；环境与代码版本详见 `docs/KVC_NATIVE_BENCH_REPRO.md`）。

本地（driver 与 owner 同机）：

| 线程 | TE over TCP | TE over URMA | TE over ubmem | TE over cxl |
| ---: | --- | --- | --- | --- |
| 8 | 444.2（4.4） | 2243.8（0.9） | 3860.2（0.50） | 3741.0（0.51） |
| 16 | 444.4（8.9） | 2701.1（1.5） | 5844.1（0.65） | 5720.1（0.67） |
| 32 | 434.8（17.9） | 1927.7（4.1） | 7688.2（1.01） | 7429.9（1.05） |
| 64 | 超时¹（60051） | 1572.8（10.2） | 8218.4（1.95） | 7698.0（2.14） |
| 128 | 超时¹（60023） | 1319.3（24.3） | **8319.1（3.83）** | 7597.7（4.23） |

跨节点（owner 与 driver 分属两节点）：

| 线程 | TE over TCP | TE over URMA | TE over ubmem | TE over cxl |
| ---: | --- | --- | --- | --- |
| 8 | 259.9（7.9） | 2213.4（0.9） | 2952.5（0.66） | 2865.1（0.68） |
| 16 | 250.5（16.2） | 3027.7（1.3） | 4994.2（0.78） | 4765.8（0.81） |
| 32 | 254.0（32.3） | 1967.2（4.0） | 7543.3（1.03） | 6830.2（1.15） |
| 64 | 超时¹（60056） | 1594.5（10.0） | 8185.3（1.97） | 7040.6（2.32） |
| 128 | 超时¹（60116） | 1322.9（24.2） | **8266.6（3.75）** | 7264.9（4.61） |

¹ TCP ≥64 线程发生传输超时（见组 1 说明）。

## 1MB 大块参考数据（2026-10-20 实测）

同一测试方法在 **value = 1 MiB** 口径下的 TCP 与 TE over ubmem 对比（owner = HW01 / 192.168.90.161，bench 同机或 HW02 / 192.168.90.162，每档 20 s，verify 开启，failed / verify_fail 全零）。

### 与 4KB 口径的参数差异

| 参数 | 4KB 口径 | 1MB 口径 | 原因 |
| --- | --- | --- | --- |
| `--value_size` | 4096 | 1048576 | |
| `--batch_size` | 64 | 16 | 每读线程缓冲 = batch × value；batch=64 时 128 线程需 8 GB，超出 ubmem 窗口分区上限（段 4 GB + 缓冲 ≤ 8 GB） |
| `--num_objects` | 100000 | 3072 | 1 MB slot 下 4 GB 段共 4096 slot，取 75%（低于 0.90 驱逐高水位） |
| `--local_buffer_size` | 256MB | 2.5GB | 128 线程 × 16 MB = 2 GB + 填充缓冲余量 |
| `KVC_MASTER_BLOCK_SIZE` | 4096 | 1048576 | 定长 slot 分配器 block 须等于 value 大小 |

### 结果（格式 = MiB/s（p50 ms））

本地（driver 与 owner 同在 HW01）：

| 线程 | TE over TCP | ubmem 默认（拷贝池） | ubmem（`MC_UBMEM_COPY_THREADS=0`） |
| ---: | --- | --- | --- |
| 1 | 2243²（5.1） | 4813²（0.8） | 4742²（1.9） |
| 8 | 4612（25.2） | 4184（17.2） | 39737（1.8） |
| 16 | 4554（53.6） | 3583（46.4） | 56692（2.2） |
| 32 | 4606（108.8） | 2959（126.5） | 57367（2.8） |
| 64 | 4715（220.1） | 2873（254.1） | 58630（5.4） |
| 128 | 4799（428.1） | 2974（471.1） | **57029（9.8）** |

跨节点（owner 在 HW01，bench 在 HW02；TCP 数据面经 100G 口 `192.168.1.x`，另附 10G 口 `192.168.90.x` 对照）：

| 线程 | TE over TCP（100G） | TE over TCP（10G）³ | ubmem 默认（拷贝池） | ubmem（`MC_UBMEM_COPY_THREADS=0`） |
| ---: | --- | --- | --- | --- |
| 1 | 1738²（7.3） | 893²（16.1） | 2253²（5.6） | 1267²（11.3） |
| 8 | **3450（35.3）** | 1127（112.3） | 2885（31.6） | 7721（15.4） |
| 16 | 2543（108.4） | 1134（226.1） | 2775（60.6） | 11209（21.6） |
| 32 | 2566（220.8） | 1146（453.7） | 2817（122.6） | 11267（44.3） |
| 64 | 2763（355.0） | 1172（910.0） | 2626（263.6） | 10518（82.6） |
| 128 | 2981（614.7） | 1224（1821.8） | 2804（495.1） | **11313（197.0）** |

² 单线程行取自 5-10 s smoke（其余档 20 s）。
³ 10G 列为未设 `MC_TCP_BIND_ADDRESS` 时的结果——TE 数据面默认落到第一块网卡（10G 管理口），1.1-1.2 GB/s 正是 10 Gbps 线速；见下文"环境实测记录"。ubmem 数据面走 UB fabric，不受以太网口影响。

### 判读

- **TCP 在 1MB 口径下没有 4KB 的 ≥64 线程超时风暴**，转为带宽受限：本地 ~4.6-4.8 GB/s（单线程 asio 数据面 + 内核协议栈三次拷贝）；跨节点经 100G 口 2.5-3.5 GB/s，8 线程即达峰值 3450 后随并发回落——远低于 100G 线速（~11.6 GB/s），瓶颈在 TE TCP 数据面（单 asio 线程 + 内核协议栈，MTU 1500）而非网口；经 10G 口则被线速钉死在 1.1-1.2 GB/s。4KB 口径的 444 MiB/s 天花板是每请求开销主导，1MB 把单请求开销摊薄后 TCP 提升约一个量级。
- **ubmem 默认配置在大块下存在反直觉的并发塌陷**：≥ 1 MiB 的拷贝走并行分片拷贝池，而拷贝池的 dispatch 持有**全局互斥锁**——所有大块拷贝被串行化。16 线程起吞吐反而低于或接近 TCP（本地 3.6 GB/s vs 4.6，跨节点 2.8 vs 2.5-3.0 GB/s 且 p99 达 3.4 s）。多线程大块场景 driver 侧必须设 `MC_UBMEM_COPY_THREADS=0`（关闭拷贝池、各线程内联 memcpy）。
- **关闭拷贝池后 ubmem 全面胜出**：本地 57-59 GB/s（贴内存带宽，约为 TCP 的 12 倍）；跨节点 11.2-11.3 GB/s（约为 100G TCP 峰值的 3.3 倍、128 线程时的 3.8 倍），且 16 线程即达平台。
- **单线程场景拷贝池有利**（本地 4813 vs 4742，跨节点 2253 vs 1267）：9 路并行分片能把单次拷贝摊到多核。单线程部署保留默认、多线程部署设 0。

### 本对节点（161/162）环境实测记录

- **多网卡节点 TCP 必须显式设 `MC_TCP_BIND_ADDRESS`**：TE 的 TCP 数据面地址不取 `--host`（`--host` 只决定段名），而是取 `MC_TCP_BIND_ADDRESS`，未设置时用 `findLocalIpAddresses()[0]`——本对节点上即第一块网卡 10G 管理口 `192.168.90.x`。owner 与 bench 两侧都要设为各自的 100G 地址（`192.168.1.161` / `192.168.1.162`）；监听端为 `::` any，不影响接受连接。用 `/sys/class/net/<if>/statistics/rx_bytes` 的增量核对流量走向：首次按段名 192.168.1.161 测时 100G 口 0 流量、10G 口 1174 MiB/s，设置后 100G 口 3107 MiB/s、10G 口 0。
- master 内嵌元数据服务可用：`-enable_http_metadata_server=true -http_metadata_server_port=8080`（python aiohttp 元数据服务器在本对节点不可用且无外网安装）。元数据 URL 仍为 `http://<ip>:8080/metadata`。
- `libkvc_server.so` 必须在节点上原生构建：从工作站整树同步过来的 `kvc-sdk/build/` 产物是 Mach-O，加载报 `invalid ELF header`。在节点上执行 `cd kvc-sdk && make clean && make libs` 后恢复正常。
- **跨节点镜像映射与老节点对不同**：本对实测 HW01 `/dev/obmm_shmdev2` ↔ HW02 `/dev/obmm_shmdev10`（111/112 老对为 shmdev6）。映射写错时读长度正常但 `verify_fail` 全爆（读到的是别的内存），**必须先跑单线程 smoke 确认 verify_fail=0 再上梯度**；也可用 magic 探针（owner 侧窗口写特征串、driver 侧逐窗口扫描）直接定位镜像设备。

## 常见问题

| 症状 | 原因 | 处置 |
| --- | --- | --- |
| `No matched device found: mock_urma_device` 后崩溃 | URMA 组未设置 `MC_CUSTOM_TOPO_JSON` | 设置该环境变量，owner 与 driver 均需携带 |
| owner 启动后 RPC 连续失败并退出 | 设置了 `MC_MS_AUTO_DISC=1` | 去掉该环境变量 |
| fill 日志无 `fill done: 100000 objects` | 段容量不足，或 key 前缀与既有数据冲突 | 段固定 4GB，每组使用新的 key 前缀 |
| setup 报 `INVALID_PARAMS(-600)` | 同名 `--local_hostname` 在短时间内重复注册 | 更换唯一端口重试 |
| `Duplicate rpc_meta key not allowed`（HTTP 400） | 前一进程被强制终止且未执行析构，元数据残留同名键 | `curl -X DELETE` 清除残留键或更换端口 |
| `open /dev/obmm_shmdev*` 报 EPERM | 镜像窗拒绝纯 `O_RDWR` 打开 | ubmem 传输层自动以 O_SYNC 兜底；cxl 跨节点需适配层；本地池窗应以纯 `O_RDWR` 打开 |
| fill 阶段发生段错误（cxl 组） | master 缺少 `-enable_cxl=true` | 按组 4 配置重启 master |
| `second instance is unsupported` | 单进程安装了第二个 ubmem transport 实例 | 预期行为（设计防线）；检查是否创建了两个引擎 |
| bench 读结果大量 MISS | 数据被淘汰 / 数据段被替换 / TTL 过期 | 调大 master TTL，重建 owner 并重新 fill |
| TCP 64/128 线程吞吐 0.2 MiB/s、p50=60s | 单线程 asio 数据面高并发下的传输超时（已知行为） | 如实记录 |
| TE TCP 吞吐异常偏低且伴随传输错误 | 端口资源耗尽（系统参数未设置） | 设置 `tcp_tw_reuse=1` 并扩大端口范围 |
| `No module named aiohttp`（python 元数据服务起不来） | 节点无 aiohttp 且无外网 | 改用 master 内嵌元数据服务：`-enable_http_metadata_server=true -http_metadata_server_port=8080` |
| `invalid ELF header: libkvc_server.so` | 从工作站同步来的 kvc-sdk 构建产物为 Mach-O | 节点上 `cd kvc-sdk && make clean && make libs` 原生重建 |
| 1MB 大块多线程下 ubmem 吞吐骤降、p99 秒级（甚至低于 TCP） | ≥1 MiB 拷贝走并行分片拷贝池，其 dispatch 为全局互斥，大块拷贝被串行化 | driver 侧设 `MC_UBMEM_COPY_THREADS=0`（单线程场景保留默认反而更快） |
| 跨节点读长度正常但 `verify_fail` 大量失败 | `MC_UBMEM_PEER_DEV_MAP` 镜像设备不对（不同节点对映射不同，如 161/162 对为 shmdev10 而非 shmdev6） | 用 magic 探针定位镜像设备；fill 在 owner 本地做，smoke verify_fail=0 通过后再上梯度 |
| 跨节点 TCP 吞吐卡在 ~1.2 GB/s（≈10 Gbps），换高速口地址也不变 | TE TCP 数据面地址取 `MC_TCP_BIND_ADDRESS` 或第一块网卡 IP，与 `--host` 无关，流量落到 10G 管理口 | owner / bench 两侧设 `MC_TCP_BIND_ADDRESS=<100G IP>`；用网口 `rx_bytes` 增量核对 |

---
