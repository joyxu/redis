# KVC 用户指南：TE over ubmem

## 简介

本指南说明 TE over ubmem 读路径的编译与使用方法：将 Mooncake Store 的数据段承载在 UB.MEM 共享内存设备窗口上，读请求通过设备窗口以内存拷贝方式完成。同机部署等价于共享内存拷贝；跨节点部署经镜像窗口通过 UB fabric 访问远端内存；两者均不经过网络协议栈。实现方案见 [KVC 架构文档：TE over ubmem](kvc_ubmem_architecture.md)。

使用 TE over ubmem 需要以下进程：

| 角色 | 进程 | 职责 |
| --- | --- | --- |
| 元数据服务 | `mooncake.http_metadata_server` | 段描述符 / rpc_meta 存取（HTTP） |
| master | `mooncake_master` | slot 分配与租约管理 |
| 段 owner | `mooncake_client --protocol=ubmem` | 承载数据段（位于设备窗口底部） |
| driver | `store_kv_bench_native --protocol=ubmem` | 写入（fill）与读取（本地缓冲位于窗口顶部） |

## 前提条件

### 环境要求

- 鲲鹏 aarch64 平台，openEuler 22.03 / 24.03（同[安装指南](installation_guide.md)）。
- UB.MEM 设备 `/dev/obmm_shmdev*` 已通过 `obmmctl` 完成本节点 export 与（跨节点部署时）对端 import。
- Python 3.11+ 及 Mooncake 的 python 包（提供 `http_metadata_server`）。

### 目录布局

```
<仓库根>/
├── kvc-sdk/     # Mooncake 编译期依赖其头文件与 libkvc_server.so
└── Mooncake/    # Mooncake（含 ubmem 传输层）
```

> 两者必须同处仓库根下（Mooncake 的 CMake 按兄弟目录相对路径解析 kvc-sdk），且
> **必须先编译 kvc-sdk 再配置 Mooncake**。

### 设备窗口准备

- TE over ubmem 以**本地池窗**（本节点 export 的设备）承载本进程数据；跨节点读取对端数据时，由传输层经 `MC_UBMEM_PEER_DEV_MAP` 指定的**镜像窗**（对端 import 的设备）访问。具体设备编号以环境 `obmmctl` 布局为准。

### 环境变量

| 变量 | 必设 | 说明 |
| --- | --- | --- |
| `MC_UBMEM_DEV_PATH` | 是 | 本进程池设备（本地池窗），如 `/dev/obmm_shmdev2` |
| `MC_UBMEM_DEV_SIZE` | 是 | 完整窗口字节数（store 分区模式强制要求显式设置） |
| `MC_UBMEM_PEER_DEV_MAP` | 跨节点 | `"对端主机:对端设备=本端镜像设备,..."` |
| `MC_UBMEM_INLINE_MEMCPY` | 否 | 拷贝引擎，默认 `1`（memcpy）；`0` 为 NC 窗软件流水 |
| `MC_UBMEM_COPY_THREADS` | 否 | ≥1MB 大块并行拷贝线程数，默认 8 |
| `MC_UBMEM_FORCE_O_SYNC` | 否 | 诊断开关，常规使用不设置 |

### 窗口分区规则

owner 数据段固定位于窗口底部，driver 本地缓冲固定位于窗口顶部，二者共用一个完整窗口：

```
┌────────────────────── 设备窗口（MC_UBMEM_DEV_SIZE，如 8GB）──────────────────────┐
│ [base, base + 段大小)                 │ [base + 窗口 − 缓冲大小, base + 窗口)   │
│ owner 数据段（--global_segment_size） │ driver 本地缓冲（--local_buffer_size）  │
└───────────────────────────────────────┴─────────────────────────────────────────┘

约束：段大小 + 缓冲大小 ≤ 窗口大小
```

| 进程 | 段大小 | 缓冲大小 |
| --- | --- | --- |
| owner（`mooncake_client`） | `--global_segment_size=4GB` | 0（可不设置） |
| driver（bench） | 0（`--global_segment_size` 缺省） | `--local_buffer_size=256MB` |

## 编译

### 1. 编译 kvc-sdk

```bash
cd <仓库根>/kvc-sdk
make            # = check + libs + tools
```

### 2. 编译 Mooncake

```bash
cd <仓库根>/Mooncake
cmake -B build -DCMAKE_BUILD_TYPE=Release \
      -DUSE_TCP=ON -DUSE_UBMEM=ON -DBUILD_BENCHMARK=ON
cmake --build build --target mooncake_master mooncake_client store_kv_bench_native -j
```

| 编译开关 | 作用 |
| --- | --- |
| `USE_UBMEM` | ubmem 传输层（`--protocol=ubmem` 必需） |
| `USE_TCP` | master / 元数据通信走 TCP（必需） |
| `BUILD_BENCHMARK` | `store_kv_bench_native` 基准 driver |

依赖说明：

- 系统依赖经包管理器安装（`dnf install rdma-core-devel glog-devel gflags-devel
  jsoncpp-devel libunwind-devel numactl-devel python3-devel boost-devel
  protobuf-devel yaml-cpp-devel libcurl-devel hiredis-devel liburing-devel
  jemalloc-devel libzstd-devel elfutils-libelf-devel patchelf xxhash-devel
  libbsd-devel`）。
- openEuler 24.03 的 boost 包名为 `boost-devel`（1.83）。
- `msgpack-devel` 不在软件源中且为必需依赖，需源码安装（msgpack-c `cpp_master` 分支，纯头文件，`cmake --build --target install` 安装到 /usr/local）。
- `Mooncake/extern/{yalantinglibs,pybind11}` 为 git 子模块，需
  `git submodule update --init` 补充内容，yalantinglibs 同样安装到 /usr/local。

产物：

| 文件 | 角色 |
| --- | --- |
| `build/mooncake-store/src/mooncake_master` | master |
| `build/mooncake-store/src/mooncake_client` | 段 owner daemon |
| `build/mooncake-store/benchmarks/store_kv_bench_native` | driver |
| `build/mooncake-common/libasio.so` | 运行库（跨机部署需同步） |
| `../kvc-sdk/build/libkvc_server.so` | 介质库（跨机部署需同步） |

ubmem 传输层静态链入 client 与 bench，无需分发额外传输库。

## 使用方法（单机）

owner 与 driver 位于同一节点，数据经本地池窗拷贝完成（cacheable 语义，memcpy 引擎）。

### 1. 启动元数据服务与 master

```bash
export MOON=<仓库根>/Mooncake
export LD_LIBRARY_PATH=$MOON/build/mooncake-common

# HTTP 元数据服务（启动一次，全程复用）
ss -ltn | grep -q ":8080 " || { \
  PYTHONPATH=<python 模块根> nohup python3 -m mooncake.http_metadata_server \
  --port 8080 > /tmp/meta.log 2>&1 & sleep 2; }

# master（block_size 与 value 尺寸对齐；长测试调大 TTL）
KVC_MASTER_BLOCK_SIZE=4096 nohup $MOON/build/mooncake-store/src/mooncake_master \
  -default_kv_lease_ttl=600000 -http_metadata_server_port=8081 -metrics_port=9004 \
  -logtostderr > /tmp/master.log 2>&1 &
sleep 4
```

### 2. 启动段 owner

```bash
export MC_UBMEM_DEV_PATH=/dev/obmm_shmdev2        # 本地池窗
export MC_UBMEM_DEV_SIZE=$((8*1024*1024*1024))    # 完整窗口 8GB
fuser /dev/obmm_shmdev2 && echo BUSY              # 必须无占用

nohup $MOON/build/mooncake-store/src/mooncake_client --host=127.0.0.1 \
  --global_segment_size=4GB --master_server_address=localhost:50051 \
  --metadata_server=http://127.0.0.1:8080/metadata \
  --protocol=ubmem --port=50052 --threads=32 --logtostderr > /tmp/client_ubm.log 2>&1 &
sleep 8 && ss -tln | grep -q ':50052 ' && echo CLIENT_UP
grep "Mounting ubmem segment" /tmp/client_ubm.log  # 段挂载成功标志
```

预期输出：`CLIENT_UP`，且日志中出现 `Mounting ubmem segment: 4294967296 bytes`。

### 3. 写入数据（fill）

```bash
BIN=$MOON/build/mooncake-store/benchmarks/store_kv_bench_native
$BIN --master_server=127.0.0.1:50051 \
     --metadata_server=http://127.0.0.1:8080/metadata \
     --num_objects=100000 --value_size=4096 --batch_size=64 \
     --local_buffer_size=$((256*1024*1024)) \
     --protocol=ubmem --key_prefix=um \
     --local_hostname=127.0.0.1:23001 --read_threads=1 --runtime=1
```

预期结果：日志出现 `fill done: 100000 objects`（fill 必须单线程串行执行，写入完成、进程退出后再执行读取）。

### 4. 读取测试

```bash
port=23011
for T in 8 16 32 64 128; do
  $BIN --master_server=127.0.0.1:50051 \
       --metadata_server=http://127.0.0.1:8080/metadata \
       --num_objects=100000 --value_size=4096 --batch_size=64 \
       --local_buffer_size=$((256*1024*1024)) \
       --protocol=ubmem --key_prefix=um --skip_fill \
       --read_threads=$T --runtime=20 --local_hostname=127.0.0.1:$port
  port=$((port+1))
done
```

每档在 stderr 输出一行结果：

```
RESULT threads=128 kv/s=... MiB/s=... failed=0 verify_fail=0 p50_ms=...
```

### 5. 停止服务

```bash
pkill -x mooncake_client; sleep 6; pkill -x mooncake_master; sleep 2
```

> owner 须以 SIGTERM 优雅退出（如上）；直接 `kill -9` 会在元数据服务中残留注册键，下次同端口启动报 400（处置见"常见问题"）。

## 使用方法（跨节点）

owner 与 driver 分属两节点：owner 段位于 owner 节点的本地池窗，driver 经本节点镜像窗读取同一块物理内存。示例：owner = `192.168.90.161`（HW01），driver = `192.168.90.162`（HW02），以实际环境为准。

### 1. 同步二进制到 driver 节点

```bash
ssh root@192.168.90.162 'mkdir -p /root/gqs/nvsync'
scp $MOON/build/mooncake-store/benchmarks/store_kv_bench_native root@192.168.90.162:/root/gqs/nvsync/
scp <仓库根>/kvc-sdk/build/libkvc_server.so root@192.168.90.162:/root/gqs/nvsync/
scp $MOON/build/mooncake-common/libasio.so root@192.168.90.162:/root/gqs/nvsync/
```

driver 节点无需搭建完整构建树。

### 2. owner 侧（HW01）

元数据服务与 master 同"单机"第 1 步。owner 启动与单机的差异仅一处：`--host` 使用本机**对外 IP**：

```bash
export MC_UBMEM_DEV_PATH=/dev/obmm_shmdev2
export MC_UBMEM_DEV_SIZE=$((8*1024*1024*1024))
nohup $MOON/build/mooncake-store/src/mooncake_client --host=192.168.90.161 \
  --global_segment_size=4GB --master_server_address=127.0.0.1:50051 \
  --metadata_server=http://127.0.0.1:8080/metadata \
  --protocol=ubmem --port=50052 --threads=32 --logtostderr > /tmp/client_ubx.log 2>&1 &
```

就绪判据同单机（`CLIENT_UP` + `Mounting ubmem segment`）。

### 3. driver 侧（HW02）

```bash
export LD_LIBRARY_PATH=/root/gqs/nvsync
export MC_UBMEM_DEV_PATH=/dev/obmm_shmdev3        # 本地池窗（本端缓冲使用）
export MC_UBMEM_DEV_SIZE=$((8*1024*1024*1024))
export MC_UBMEM_PEER_DEV_MAP="192.168.90.161:/dev/obmm_shmdev2=/dev/obmm_shmdev6"
#                                                  对端主机:对端设备=本端镜像设备

BIN=/root/gqs/nvsync/store_kv_bench_native
UBX_COMMON="--master_server=192.168.90.161:50051 \
  --metadata_server=http://192.168.90.161:8080/metadata \
  --num_objects=100000 --value_size=4096 --batch_size=64 \
  --local_buffer_size=$((256*1024*1024)) --protocol=ubmem --key_prefix=umx"

# fill（经镜像窗串行写入）
$BIN $UBX_COMMON --local_hostname=192.168.90.162:24101 --read_threads=1 --runtime=1

# smoke 验证：单线程 5 秒，verify_fail=0（跨窗口字节级校验）通过后再执行梯度
$BIN $UBX_COMMON --skip_fill --read_threads=1 --runtime=5 \
  --local_hostname=192.168.90.162:24110

# 读梯度
port=24111
for T in 8 16 32 64 128; do
  $BIN $UBX_COMMON --skip_fill --read_threads=$T --runtime=20 \
    --local_hostname=192.168.90.162:$port
  dmesg | grep -iE "ucmi|mami" | grep -icE "error|fail"   # 应无增量
  port=$((port+1))
done
```

注意事项：

- 镜像窗由传输层自动以 `O_RDWR|O_SYNC` 打开（日志可见 `mapped ... nc=1`），无需手工干预。
- 每档执行完成后检查 `dmesg` 中 ucmi / mami 错误无增量；出现增量说明镜像窗配置或对端 cna 配置有误，应先排查再继续。
- 更换端口重跑前，先清理可能残留的元数据键（处置见"常见问题"）。

## 结果验证与判读

- 结果仅取 stderr 的 `RESULT` 行；fill 命令读阶段的数字无效，仅以 `fill done` 判断写入完整性。
- `failed` / `verify_fail` 应为 0；非零说明数据校验或传输失败，先查"常见问题"。
- 吞吐 = `MiB/s` 字段（= kv/s × value_size / 2²⁰）；延迟取 `p50_ms`。
- 不同日期的绝对值存在 ±5% 左右的波动，属正常现象。

四条读路径（TCP / URMA / ubmem / cxl）在统一测试条件下的对比方法与参考数据见
[KVC 测试方案：四条读路径对比](kvc_test_plan.md)。

## 常见问题

| 症状 | 原因 | 处置 |
| --- | --- | --- |
| owner 启动报 `ubmem window partition invalid` | 段大小 + 缓冲大小超过窗口，或未设置 `MC_UBMEM_DEV_SIZE` | 核对窗口分区约束（见"窗口分区规则"） |
| `open /dev/obmm_shmdev*` 报 EPERM | 该窗口为镜像窗，拒绝纯 `O_RDWR` 打开 | 镜像窗由跨节点镜像机制（`MC_UBMEM_PEER_DEV_MAP`）使用；`MC_UBMEM_DEV_PATH` 应配置本地池窗设备 |
| 日志无 `Mounting ubmem segment` | 未设置 `MC_UBMEM_DEV_PATH`，或设备被占用，或段大小为 0 | 检查环境变量与 `fuser` 输出；owner 须携带 `--global_segment_size` |
| `no mirror configured for peer ...` | 跨节点未设置 `MC_UBMEM_PEER_DEV_MAP`，或键（对端主机:对端设备）不匹配 | 按"对端主机:对端设备=本端镜像设备"格式设置；对端设备须与 owner 的 `MC_UBMEM_DEV_PATH` 一致 |
| `second instance is unsupported` | 单进程安装了第二个 ubmem transport 实例 | 预期行为（设计防线）；检查进程内是否创建了两个引擎 |
| 元数据服务报 `Duplicate rpc_meta key not allowed`（HTTP 400） | 前一进程被强制终止且未执行析构，元数据残留同名键 | `curl -X DELETE` 清除残留键或更换端口 |
| fill 日志无 `fill done: 100000 objects` | 段容量不足，或 key 前缀与既有数据冲突 | 段固定 4GB，每组使用新的 key 前缀 |
| bench 读结果大量 MISS | 数据被淘汰 / TTL 过期 / 数据段被替换 | 调大 master TTL，重建 owner 并重新 fill |

---
