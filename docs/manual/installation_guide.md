# 安装指南

## 环境部署

### 环境要求

部署 HPC-Redis 前请确认软硬件环境满足下表要求。详细的已验证环境清单见 [README](README.md) 的 "已验证的软硬件环境" 一节。

**硬件要求**

| 项目 | 最低要求 | 推荐配置 |
| --- | --- | --- |
| 处理器 | 鲲鹏 920（aarch64，SVE） | 鲲鹏 930（aarch64，SVE2 256-bit） |
| 核数 | 32 核 | 192 核（2 socket × 96 核） |
| 内存 | 64 GB | 256 GB 及以上 |
| UB.MEM 设备 | `/dev/obmm_shmdev1` 可访问 | `/dev/obmm_shmdev{1..8}` 全部初始化 |
| 网络（跨节点） | 10 GbE | 100 GbE RoCE（mlx5） |

**软件要求**

| 项目 | 版本要求 | 检查命令 |
| --- | --- | --- |
| 操作系统 | openEuler 22.03 / 24.03 | `cat /etc/os-release` |
| 内核 | ≥ 5.10 | `uname -r` |
| GCC | ≥ 10.3（支持 `-march=armv8.2-a+sve`） | `gcc --version` |
| GNU Make | ≥ 4.0 | `make --version` |

### 获取源码

HPC-Redis 源码托管在内部 Git 仓库，同时通过 mutagen 在本地 macOS 与服务器（HW01）之间实时同步。以下命令在目标服务器（HW01 / HW02）上执行。

**从内部 Git 仓库克隆**

```bash
git clone https://github.com/moreaicc/hpc-redis.git
cd hpc-redis
```

### 编译 redis-server
0. 编译依赖（仅首次编译需要）
   ```bash
   gcc -O3 -fPIC -c deps/xxhash/xxhash.c -o deps/xxhash/xxhash.o
   ar rcs deps/xxhash/libxxhash.a deps/xxhash/xxhash.o
   cd deps/jemalloc
   ./autogen.sh --with-version=5.3.0-0-g0
   make
   make install
   cd -
   ```

1. 编译 `redis-server`

   ```bash
   make -C src -j$(nproc) redis-server
   ```

   首次编译会自动构建 `deps/` 下的 jemalloc、hiredis、lua 等依赖；增量编译只重链变更的目标。

2. 确认产物

   ```bash
   ls -lh src/redis-server
   ```

   预期输出（具体大小以实际编译为准）：

   ```
   -rwxr-xr-x 1 root root 25M Jul 15 10:00 src/redis-server
   ```

### 编译客户端 SDK

客户端 SDK 位于 `clients/c/`，构建后产出静态库 `libvemb_v16_client.a`、动态库 `libvemb_v16_client.so` 与扁平化头文件 `build/include/vemb_v16_client_sdk.h`。

1. 进入 SDK 目录。

   ```bash
   cd clients/c
   ```

2. 构建 SDK（同时产出 `.a` / `.so` 与扁平化头文件）。

   ```bash
   make
   ```

3. 确认产物。

   ```bash
   ls -lh build/libvemb_v16_client.a build/libvemb_v16_client.so build/include/vemb_v16_client_sdk.h
   ```

   预期输出：

   ```
   -rw-r--r-- 1 root root 320K Jul 15 10:00 build/libvemb_v16_client.a
   -rwxr-xr-x 1 root root 180K Jul 15 10:00 build/libvemb_v16_client.so
   -rw-r--r-- 1 root root  18K Jul 15 10:00 build/include/vemb_v16_client_sdk.h
   ```

4. （可选）编译示例代码验证 SDK 可用性。

   ```bash
   cd /root/gqs/codespace/UnifiedBus/hpc-redis
   # 注意：必须加 -I src 才能解析 SDK 头文件中的 xxhash 相对路径
   gcc -I clients/c/build/include -I src \
       clients/c/example.c \
       clients/c/build/libvemb_v16_client.a \
       -o clients/c/example -lpthread
   ```

   如编译无报错且产出 `clients/c/example` 可执行文件，则 SDK 构建完成。

## 安装后检查

完成 `redis-server` 与 SDK 编译后，请按以下步骤验证安装结果。

### 检查 redis-server 可执行

```bash
./src/redis-server --version
```

预期输出包含 Redis 版本号与编译信息，例如：

```
Redis server v=x.x.x sha=xxxxxxxx:0 malloc=jemalloc-5.3.0 bits=64 build=xxxxxxxx
```

### 启动 redis-server 并验证 VEMB V16 启用

1. 启动一个最小配置的 `redis-server`。

```bash
./src/redis-server --port 6379 \
   --vemb-v16-enabled yes \
   --vemb-v16-dim 8 \
   --vemb-v16-max-vectors 1024 \
   --vemb-v16-warm-regions-manifest examples/vemb_v16_warm_regions_111_dim8.yaml \
   --vemb-v16-reset-warm-regions yes \
   --vemb-v16-proxy-io-threads 1 \
   --vemb-v16-supernode-workers 2 \
   --daemonize yes --loglevel notice \
   --logfile /tmp/redis-6379.log
```

2. 检查启动日志，确认 VEMB V16 已就绪。
```bash
tail -50 /tmp/redis-6379.log | grep "VEMB V16"
```

预期输出：
```text
... * VEMB V16 integration ready
```

### 检查 UB.MEM 设备可访问

```bash
ls -l /dev/obmm_shmdev*
```

预期输出（设备节点数量以实际硬件为准）：

```
crw------- 1 root root 240, 1 Jul 15 10:00 /dev/obmm_shmdev1
crw------- 1 root root 240, 2 Jul 15 10:00 /dev/obmm_shmdev2
...
```


### 检查客户端 SDK 头文件可被业务代码引用

```bash
echo '#include "vemb_v16_client_sdk.h"
int main(void){return 0;}' > /tmp/sdk_hdr_check.c

gcc -I clients/c/build/include -c /tmp/sdk_hdr_check.c -o /tmp/sdk_hdr_check.o && \
    echo "SDK header OK" || echo "SDK header MISSING"

rm -f /tmp/sdk_hdr_check.c /tmp/sdk_hdr_check.o
```

预期输出：

```
SDK header OK
```

## 卸载 HPC-Redis

HPC-Redis 采用就地编译运行模式，不向系统目录写入文件。卸载只需删除源码目录与运行时产物即可。

1. 停止所有运行中的 `redis-server` 进程。

   ```bash
   pkill -f 'redis-server.*--vemb-v16-enabled yes'
   ```


2. 删除运行时数据与日志（路径以实际部署为准）。

   ```bash
   rm -rf /var/lib/hpc-redis /var/log/hpc-redis
   ```

> UB.MEM 设备节点的清理（`obmmctl --reset` 等）属于平台运维操作，不在本文档集范围内。如需彻底释放 UB.MEM 资源请联系系统管理员。

---
