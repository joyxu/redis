# 使用手册

本文档描述 HPC-Redis 集成版 `redis-server` 的启动配置、客户端 SDK 的完整使用方法、多端点集成示例与日常维护操作。

阅读本文档前请确保：

- 已按 [安装指南](installation_guide.md) 完成 `redis-server` 与客户端 SDK 编译。
- 已完成 [快速入门](quick_start.md) 的最小化验证。

---

## 启动 HPC-Redis 服务

HPC-Redis 是在原生 Redis 基础上集成了 VEMB V16 向量快速路径、UB.MEM 远程内存与 SVE2 向量计算的增强版本。服务启动支持**命令行参数**与**配置文件**两种方式，两者可混用（命令行参数覆盖配置文件中的同名项）。

### 通过命令行参数启动 redis-server

适用于快速验证、压测脚本场景。VEMB V16 相关参数均以 `--vemb-v16-*` 前缀透传：

```bash
numactl -N 0 -l taskset -c 0-95 ./src/redis-server \
  --port 6379 \
  --bind 0.0.0.0 --protected-mode no \
  --vemb-v16-enabled yes \
  --vemb-v16-dim 300 \
  --vemb-v16-max-vectors 131072 \
  --vemb-v16-warm-regions-manifest ./examples/vemb_v16_warm_regions_111.yaml \
  --vemb-v16-reset-warm-regions yes \
  --vemb-v16-proxy-io-threads 32 \
  --vemb-v16-supernode-workers 64 \
  --daemonize yes \
  --loglevel notice
```

启动后验证：

```bash
./output/src/redis-cli -p 6379 PING
# → PONG
```

### 通过配置文件启动 redis-server

适用于长期运行、需要可重复部署的场景。将上述参数写入 `redis.conf`（去掉 `--` 前缀），再用 `redis-server` 加载：

```bash
./src/redis-server /path/to/redis.conf
```

`redis.conf` 片段示例：

```ini
bind 0.0.0.0
port 6379
protected-mode no
daemonize yes
loglevel notice
logfile /var/log/hpc-redis/redis-6379.log
dir /var/lib/hpc-redis

# —— VEMB V16 向量快速路径 ——
vemb-v16-enabled yes
vemb-v16-dim 300
vemb-v16-max-vectors 131072
vemb-v16-warm-regions-manifest /etc/hpc-redis/warm_regions.yaml
vemb-v16-reset-warm-regions yes
vemb-v16-proxy-io-threads 32
vemb-v16-supernode-workers 64

# —— 网络 / I/O ——
tcp-backlog 4096
io-threads 8
io-threads-do-reads yes
maxclients 10000

# —— 持久化（向量数据通常关闭）——
save ""
appendonly no
```

### redis-server 配置项说明

**VEMB V16 相关**

| 参数 | 取值 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `vemb-v16-enabled` | yes / no | no | 总开关。开启后 Redis 端口同时承载 RESP 与 VEMB V16 嗅探二进制协议。 |
| `vemb-v16-dim` | 0 – `VEMB_V16_MAX_DIM` | 无默认值 | 启用 VEMB V16 时必须显式设置，并需与客户端 SDK / `memtier_benchmark --vemb-v16-dim` 一致；启动后不可修改。 |
| `vemb-v16-max-vectors` | ≥ 1 | 131072 | 预留向量槽位数。 |
| `vemb-v16-warm-regions-manifest` | 路径 | 空 | 热区 manifest YAML，描述 region_id / path / mmap_offset / bytes / value_size 等。**必传**。 |
| `vemb-v16-reset-warm-regions` | yes / no | no | 启动时是否重置热区（首次部署或切换拓扑时设为 yes）。 |
| `vemb-v16-supernode-workers` | 0 – 256 | 0 | SuperNode 处理线程数，0 表示按 CPU 自动推算。生产建议 `pio × 2`。 |
| `vemb-v16-proxy-io-threads` | 0 – 256 | 0 | Proxy I/O 线程数。生产建议与 `supernode-workers` 按 1:2 配置。 |

### warm-regions manifest 文件配置

`--vemb-v16-warm-regions-manifest` 指向的 YAML 文件描述本实例需要 mmap 的所有热区（warm region）以及跨节点 UB RPC 拓扑。**这是 hpc-redis 启动的必备文件**，未传或解析失败会直接拒绝启动。

#### 文件结构

YAML 文件包含 4 段（缩进敏感，支持 `#` 行注释）：

1. **顶层标量字段**：本节点身份与跨节点基础设施描述
2. **`warm_regions:` 列表**：本地 mmap 的热区列表（每项一个 UB 共享内存段或 SHM 文件）
3. **`remote_meta_views:` 列表**（可选）：远端元数据 hash 表只读镜像，跨节点 scaleout 用
4. **`ub_rpc_peers:` 列表**（可选）：跨节点 UB RPC ring 配置，跨节点 scaleout 必填

#### 顶层字段

| 字段 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `local_ub_node_id` | u32 | 必填 | 本节点 UB ID。集群内每节点唯一，如 HW01=0、HW02=1、HW05=2、HW04=3。 |
| `local_region_weight` | u32 | 4 | 本地 region 的默认权重（read path 在多 region 间加权分流用）。 |
| `remote_meta_backend` 或 `remote_meta_provider` | `ub` / `shm` | - | 远端元数据后端类型。 |
| `remote_meta_path` | str | - | 远端元数据 SHM/UB 设备路径。 |
| `remote_meta_mmap_offset` | u64 | 0 | 远端元数据 mmap 偏移（字节）。 |
| `remote_meta_entries` 或 `remote_meta_entry_count` | u32 | - | 远端元数据条目数。 |
| `remote_meta_buckets` 或 `remote_meta_bucket_count` | u32 | - | 远端元数据 bucket 数。 |
| `remote_meta_sets` 或 `remote_meta_set_count` | u32 | - | 远端元数据 set 数。 |
| `remote_meta_ways` | u32 | - | 远端元数据 hash 表 way 数。 |
| `job_plane_backend` 或 `job_plane_provider` | `ub` / `shm` | - | Job plane 后端类型（scaleout 任务平面）。 |
| `job_plane_path` | str | - | Job plane SHM/UB 设备路径。 |
| `job_plane_mmap_offset` | u64 | 0 | Job plane mmap 偏移（字节）。 |
| `ub_rpc_timeout_ms` | u32 | - | UB RPC 超时（毫秒）。 |

#### warm_regions[] 字段（每个 `- ` 开一项）

| 字段 | 别名 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `region_id` | - | u32 | 必填 | 全局唯一 region ID，跨节点不能重复。 |
| `provider` | `backend` | `ub` / `shm` | `ub` | `ub` = UnifiedBus 共享内存设备；`shm` = 本地 POSIX SHM（`/dev/shm/xxx`）。 |
| `path` | - | str | 必填 | 设备/SHM 文件路径，如 `/dev/obmm_shmdev2` 或 `/dev/shm/vemb_region1`。 |
| `mmap_offset` | - | u64 | 0 | mmap 偏移（字节）。UB 设备多 region 共享同一 fd 时用。 |
| `bytes` | `region_bytes` | u64 | 必填 | region 大小（字节）。必须 ≥ `value_size × max_vectors`。 |
| `value_size` | - | u32 | 同 `--vemb-v16-dim × 4` | 单条向量字节数（FP32 = dim × 4）。dim=300 → 1200，dim=8 → 32。 |
| `home_ub_node_id` | - | u32 | 必填 | 该 region 的数据归属节点。等于 `local_ub_node_id` = 本地 region；等于其他节点 ID = 远端镜像（本节点只读 mmap）。 |
| `weight` | - | u32 | `local_region_weight` | 该 region 在本实例 read path 的权重，覆盖默认 `local_region_weight`。 |
| `is_local` | - | yes / no | 自动推断 | 显式标记是否本地 region。不写则按 `home_ub_node_id == local_ub_node_id` 推断。 |

#### 示例 1：单节点单 region（最简部署）

`examples/vemb_v16_warm_regions_111.yaml`：

```yaml
# HW01, dim=300, 单 region 本地部署
local_ub_node_id: 0
local_region_weight: 8
warm_regions:
  - region_id: 1
    provider: ub
    path: /dev/obmm_shmdev2
    mmap_offset: 0
    bytes: 1073741824            # 1 GiB
    value_size: 1200             # 300-dim FP32 = 300 × 4
    home_ub_node_id: 0           # 数据归属本节点
    weight: 1
```

对应启动命令：

```bash
./src/redis-server --port 6390 \
    --vemb-v16-enabled yes --vemb-v16-dim 300 \
    --vemb-v16-warm-regions-manifest examples/vemb_v16_warm_regions_111.yaml \
    --vemb-v16-reset-warm-regions yes \
    --daemonize yes
```

#### 示例 2：双节点 scaleout（本地 + 远端镜像）

`examples/vemb_v16_warm_regions_112_multi.yaml`（HW02 侧，配本地 region_id=2 + 远端镜像 region_id=1）：

```yaml
local_ub_node_id: 1
local_region_weight: 8
warm_regions:
  - region_id: 2
    provider: ub
    path: /dev/obmm_shmdev2
    mmap_offset: 0
    bytes: 1073741824
    value_size: 1200
    home_ub_node_id: 1           # 本地 region，数据归 HW02
    weight: 1
  - region_id: 1
    provider: ub
    path: /dev/obmm_shmdev4      # 远端 region 在本节点的镜像 mmap 入口
    mmap_offset: 0
    bytes: 1073741824
    value_size: 1200
    home_ub_node_id: 0           # 数据归属 HW01，本节点只读
    weight: 1
```

HW01 侧配置对称（`local_ub_node_id: 0`、本地 region_id=1 + 远端镜像 region_id=2）。双节点同时启动后，redis-server 通过 `home_ub_node_id` 区分本地写路径和远端只读镜像。

#### 示例 3：低维向量（dim=8）

只改 `value_size` 为 `dim × 4`：

```yaml
warm_regions:
  - region_id: 1
    provider: ub
    path: /dev/obmm_shmdev2
    bytes: 1073741824
    value_size: 32               # 8-dim FP32 = 8 × 4
    home_ub_node_id: 0
```

启动参数同步改为 `--vemb-v16-dim 8`。


#### 配置要点

1. **`region_id` 全局唯一**：跨节点不能重复。建议按 owner 节点分段（HW01 用 region_id=1、HW02 用 region_id=2、HW05 用 region_id=3 …）。
2. **`home_ub_node_id` 决定读写路径**：等于 `local_ub_node_id` 时走本地写路径（VADD 直接落到本 region），不等时本节点只读 mmap（写请求会被 proxy 转发到 owner）。
3. **`bytes` 与 `value_size` 关系**：`bytes` 必须 ≥ `value_size × max_vectors`，预留膨胀空间（推荐 2× 以上）。过小会在 VADD 写入到上限后失败。
4. **`weight` 影响读分流**：本地 + 多个远端镜像共存时，按 weight 加权选 region 读。单 region 场景写 1 即可。
5. **`mmap_offset`**：UB 设备多 region 共用同一 fd 时用偏移区分；单 region 单独设备时写 0。
6. **`provider` 选择**：
   - `ub`：生产首选，跨节点共享、低延迟、走硬件 cache snoop。
   - `shm`：仅本地、功能验证用，性能差且无法跨节点。
7. **首次部署 / 切换拓扑**：必须配合 `--vemb-v16-reset-warm-regions yes` 启动一次，否则 region 残留旧数据。
8. **scaleout 必须双机对称 + `ub_rpc_peers`**：跨节点拓扑 RPC 需要额外配置 ring 字段（request/response/inbound_request/outbound_response 各一组 owner_id + path + mmap_offset），详见 `examples/` 下含 `ub_rpc_peers` 段的示例与 scaleout 专题文档。

---

## 使用 VEMB V16 客户端 SDK

`clients/c/vemb_v16_client_sdk.{c,h}` 提供 VEMB V16 协议的客户端 API，业务侧通过链接 SDK 调用 `vemb_v16_client_*` 系列函数访问向量快速路径（`VADD` 写入、`VEMB` 读取、`VSIM` 相似度、`VREM` 删除）。本节提供的仅是使用示例代码，使用过程中请根据实际业务需求进行配置修改。

### 调用前提

业务代码编译时需要链接 SDK 头文件与库文件。SDK 提供静态库 `libvemb_v16_client.a` 与动态库 `libvemb_v16_client.so` 两种形态。

- 头文件：`clients/c/build/include/vemb_v16_client_sdk.h`（扁平化后的公开头文件）
- 静态库：`clients/c/build/libvemb_v16_client.a`
- 动态库：`clients/c/build/libvemb_v16_client.so`

**构建 SDK：**

```bash
cd clients/c
make              # 同时构建 .a / .so 并扁平化头文件到 build/include/
```

**业务侧链接方式：**

下面的 `$HPCREDIS_DIR` 指向 hpc-redis 仓库根目录，请按实际路径替换（服务器上通常是 `/root/gqs/codespace/UnifiedBus/hpc-redis`）。

```bash
export HPCREDIS_DIR=/root/gqs/codespace/UnifiedBus/hpc-redis

# 方式 A：链接静态库（推荐，避免运行时 LD_LIBRARY_PATH）
# 注意：必须加 -I$HPCREDIS_DIR/src 才能解析 SDK 头文件中的 xxhash 相对路径
gcc my_app.c -I$HPCREDIS_DIR/clients/c/build/include -I$HPCREDIS_DIR/src \
    $HPCREDIS_DIR/clients/c/build/libvemb_v16_client.a -o my_app -lpthread

# 方式 B：链接动态库，编译期指定 rpath
gcc my_app.c -I$HPCREDIS_DIR/clients/c/build/include -I$HPCREDIS_DIR/src \
    -L$HPCREDIS_DIR/clients/c/build -lvemb_v16_client \
    -Wl,-rpath,$HPCREDIS_DIR/clients/c/build -o my_app

# 方式 C：链接动态库，运行时设置环境变量
export LD_LIBRARY_PATH=$HPCREDIS_DIR/clients/c/build:$LD_LIBRARY_PATH
gcc my_app.c -I$HPCREDIS_DIR/clients/c/build/include -I$HPCREDIS_DIR/src \
    -L$HPCREDIS_DIR/clients/c/build -lvemb_v16_client -o my_app
```

### 同步 API 调用示例

以下示例展示单端点连接下的 VADD / VEMB / VSIM 全流程。完整可编译版本见 `clients/c/example.c`。

```c
#include "vemb_v16_client_sdk.h"
#include <stdio.h>

#define DIM 300

int main(void)
{
    /* 1. 配置 TCP bootstrap seed，并在启动时固定 TCP 数据面 */
    const char *seeds[] = {"127.0.0.1:6379"};
    vemb_v16_client_t *c = vemb_v16_client_create(
        seeds, 1, DIM, 0, VEMB_V16_TRANSPORT_TCP);
    if (!c) { fprintf(stderr, "connect failed\n"); return 1; }

    /* 多 seed 容错：
     * const char *eps[] = {"192.168.90.111:6379", "192.168.90.112:6379"};
     * vemb_v16_client_t *c = vemb_v16_client_create(
     *     eps, 2, DIM, 0, VEMB_V16_TRANSPORT_TCP);
     */

    /* 2. VADD — 写入向量 */
    float vec[DIM];
    for (uint32_t i = 0; i < DIM; i++) vec[i] = (float)i;
    if (vemb_v16_client_vadd(c, "myset", "elem1", vec, DIM) == 0) {
        printf("VADD OK\n");
    }

    /* 3. VEMB — 读取向量（inline vector 路径） */
    float out[DIM];
    uint32_t out_dim = 0;
    int rc = vemb_v16_client_vemb_vector(c, "myset", "elem1",
                                         out, DIM, &out_dim);
    if (rc == 0)      printf("VEMB OK, dim=%u\n", out_dim);
    else if (rc == 1) printf("VEMB not found\n");

    /* 4. VSIM — 余弦相似度 */
    float score = 0.0f;
    rc = vemb_v16_client_vsim(c, "myset", "elem1", vec, DIM, &score);
    if (rc == 0) printf("VSIM score=%.4f\n", score);

    /* 5. VREM — 删除向量（幂等） */
    vemb_v16_client_vrem(c, "myset", "elem1");

    /* 6. 释放连接 */
    vemb_v16_client_destroy(c);
    return 0;
}
```

编译并运行：

```bash
# 注意：必须加 -I src 才能解析 SDK 头文件中的 xxhash 相对路径
gcc -I clients/c/build/include -I src app.c \
    clients/c/build/libvemb_v16_client.a -o app -lpthread
./app
```

预期输出：

```
VADD OK
VEMB OK, dim=300
VSIM score=1.0000
```

### Pipeline 批量调用示例

`*_pipeline` 系列接口用于高吞吐批量场景，单次调用发送多个请求并等待全部响应。SDK 根据每个 key 的 hash 路由 owner，并按 owner 分组提交。

```c
/* 批量 VEMB 读取 */
const char *sets[3]  = {"myset", "myset", "myset"};
const char *elems[3] = {"e1", "e2", "e3"};
float vectors[3 * DIM];
vemb_v16_pipeline_resp_t resps[3] = {0};

int rc = vemb_v16_client_vemb_pipeline(
    c, sets, elems, 3, vectors, resps, 16);
if (rc == 0) {
    for (int i = 0; i < 3; i++) {
        printf("[%d] status=%d offset=%llu bytes=%u\n",
               i, resps[i].status,
               (unsigned long long)resps[i].offset,
               resps[i].bytes);
    }
}
```

可用的 pipeline 接口：

- `vemb_v16_client_vadd_pipeline` — 批量写入
- `vemb_v16_client_vemb_pipeline` — 批量读取（返回 handle / offset）
- `vemb_v16_client_vsim_pipeline` — 批量余弦相似度

`max_inflight` 参数控制每批最大在途请求数，常用 16 / 32 / 64。

### 异步 / Buffer-based 低级 API

对于已有事件循环（libevent / libuv / epoll）的集成场景，SDK 提供帧序列化 / 反序列化原语，由调用方负责传输：

```c
/* 将 VADD 帧序列化到调用方提供的 buffer */
uint8_t buf[1024];
ssize_t n = vemb_v16_serialize_vadd(buf, sizeof(buf),
                                    channel_id, req_id,
                                    key, key_len,
                                    vector, dim);
/* n 字节写入 evbuffer / sendmsg … */

/* 解析返回帧 */
vemb_v16_resp_t resp;
size_t inline_bytes = 0;
ssize_t used = vemb_v16_parse_response(buf, buf_len, &resp, &inline_bytes);
```

可用的序列化原语：`vemb_v16_serialize_hello` / `_vadd` / `_vemb` / `_vemb_inline` / `_vsim_inline` / `_vrem`。

可用的反序列化原语：`vemb_v16_parse_welcome` / `vemb_v16_parse_response`。

热区 mmap 辅助：`vemb_v16_open_warm_region` / `vemb_v16_close_warm_region`（无需 client 句柄即可独立调用）。

### SDK API 速查表

| API | 用途 | 返回值 |
| --- | --- | --- |
| `vemb_v16_client_create(seeds[], seed_count, dim, timeout_ms, transport_type)` | 创建固定 TCP 或 AERON transport 的 client；seeds 是 TCP topology bootstrap 地址 | client 句柄，失败返回 NULL |
| `vemb_v16_client_configure_ub_peer_view(c, manifest, client_host)` | 首次 UB channel 前配置固定 remote UB peer-view；manifest owner_id 必须是 topology owner | 0=OK, -1=配置无效或 UB 身份已固定 |
| `vemb_v16_client_destroy(c)` | 释放 client | void |
| `vemb_v16_client_vadd(c, set, elem, vec, dim)` | 写入向量 | 0=OK, -1=ERR |
| `vemb_v16_client_vemb_vector(c, set, elem, out, cap, *dim)` | 读取 inline 向量 | 0=OK, 1=NOT_FOUND, -1=ERR |
| `vemb_v16_client_vemb_handle(c, set, elem, *off, *bytes, *dim, *rid)` | 仅取 handle（zero-copy 路径） | 同上 |
| `vemb_v16_client_read_vector(c, off, bytes, out, cap)` | 用 handle 读取向量 | 0=OK, -1=ERR |
| `vemb_v16_client_vsim(c, set, elem, query, dim, *score)` | 余弦相似度 | 0=OK, 1=NOT_FOUND, -1=ERR |
| `vemb_v16_client_vrem(c, set, elem)` | 删除向量（幂等） | 0=OK, -1=ERR |
| `vemb_v16_client_vadd_pipeline(...)` | 批量写入 | 0=OK, -1=ERR |
| `vemb_v16_client_vemb_pipeline(...)` | 批量读取 | 0=OK, -1=ERR |
| `vemb_v16_client_vsim_pipeline(...)` | 批量相似度 | 0=OK, -1=ERR |
| `vemb_v16_client_vsim_repeat(...)` / `_vemb_repeat` / `_vadd_repeat` | 同 key 重复压测 | 0=OK, -1=ERR |
| `vemb_v16_client_ping(c)` | 数据面心跳 | 0=OK, -1=ERR |
| `vemb_v16_client_stats(c, *stats)` | 拉 proxy 运行时统计 | 0=OK, -1=ERR |
| `vemb_v16_client_topology_refresh(c)` | 显式预热拓扑 | 0=OK, -1=ERR |
| `vemb_v16_client_get_redirect_stats(c, *out)` | 累计 ASK/MOVED 重定向次数 | void |
| `vemb_v16_client_set_retry_budget(c, n)` | 设置透明重试上限（默认 256） | void |

### 线程安全与拓扑重试

- **线程安全**：单个 `vemb_v16_client_t` 句柄**非线程安全**。多线程应用必须每线程创建一个 client。
- **拓扑重试**：单 key 与 pipeline 操作内部透明重试 `ASK` / `MOVED` / `STALE_TOPOLOGY`，调用方只看到 `OK` / `NOT_FOUND` / `ERR`。重定向次数可通过 `vemb_v16_client_get_redirect_stats()` 观察。

## SDK 完整集成示例

下面是一个端到端的多线程集成示例，覆盖多端点连接、pipeline 批量读写、统计观测等典型用法。示例代码可独立编译运行，作为业务集成的起点。

```c
/* sdk_demo.c — 多端点 / 多线程 / pipeline 集成示例
 *
 * 编译：
 *   gcc -I$HPCREDIS_DIR/clients/c/build/include -I$HPCREDIS_DIR/src \
 *       sdk_demo.c \
 *       $HPCREDIS_DIR/clients/c/build/libvemb_v16_client.a \
 *       -o sdk_demo -lpthread
 */
#include "vemb_v16_client_sdk.h"
#include <pthread.h>
#include <stdio.h>
#include <string.h>

#define DIM       300
#define NTHREADS  8
#define BATCH     32
#define ROUNDS    1000

static const char *ENDPOINTS[] = {
    "192.168.90.111:6379",
    "192.168.90.112:6379",
};
static const int N_EP = sizeof(ENDPOINTS) / sizeof(ENDPOINTS[0]);

typedef struct {
    int       tid;
    uint64_t  ok, miss, err;
} thread_ctx_t;

static void *worker(void *arg)
{
    thread_ctx_t *ctx = (thread_ctx_t *)arg;

    /* 1. 每线程独立 client（句柄非线程安全） */
    vemb_v16_client_t *c = vemb_v16_client_create(
        ENDPOINTS, N_EP, DIM, /*timeout_ms=*/0,
        VEMB_V16_TRANSPORT_TCP);
    if (!c) {
        fprintf(stderr, "[t%d] connect failed\n", ctx->tid);
        return NULL;
    }

    /* 2. 预热拓扑，避免首 op RTT 尖刺 */
    vemb_v16_client_topology_refresh(c);

    /* 3. 构造一批 key + vector */
    const char *sets[BATCH];
    const char *elems[BATCH];
    char  elem_buf[BATCH][32];
    const float *vecs[BATCH];
    float vec[DIM];
    for (uint32_t i = 0; i < DIM; i++) vec[i] = (float)i;
    for (int i = 0; i < BATCH; i++) {
        sets[i]  = "myset";
        snprintf(elem_buf[i], sizeof(elem_buf[i]), "t%d-k%d", ctx->tid, i);
        elems[i] = elem_buf[i];
        vecs[i]  = vec;
    }

    /* 4. 主循环：pipeline 写 + pipeline 读 */
    for (int r = 0; r < ROUNDS; r++) {
        if (vemb_v16_client_vadd_pipeline(c, sets, elems, vecs,
                                          BATCH, /*max_inflight=*/16) == 0) {
            ctx->ok += BATCH;
        } else {
            ctx->err++;
            continue;
        }

        float out_vectors[BATCH * DIM];
        vemb_v16_pipeline_resp_t resps[BATCH] = {0};
        if (vemb_v16_client_vemb_pipeline(c, sets, elems,
                                          BATCH, out_vectors, resps, 16) == 0) {
            for (int i = 0; i < BATCH; i++) {
                if      (resps[i].status == 0) ctx->ok++;
                else if (resps[i].status == 1) ctx->miss++;
                else                            ctx->err++;
            }
        }
    }

    /* 5. 拉取本线程的 proxy 统计与重定向计数 */
    vemb_v16_stats_t         stats = {0};
    vemb_v16_redirect_stats_t rs   = {0};
    vemb_v16_client_stats(c, &stats);
    vemb_v16_client_get_redirect_stats(c, &rs);
    printf("[t%d] ok=%lu miss=%lu err=%lu | vadd=%lu vemb=%lu | moved=%lu ask=%lu\n",
           ctx->tid,
           (unsigned long)ctx->ok, (unsigned long)ctx->miss, (unsigned long)ctx->err,
           (unsigned long)stats.vadd_requests, (unsigned long)stats.vemb_requests,
           (unsigned long)rs.moved_redirects, (unsigned long)rs.ask_redirects);

    vemb_v16_client_destroy(c);
    return NULL;
}

int main(void)
{
    pthread_t     th[NTHREADS];
    thread_ctx_t  ctx[NTHREADS] = {0};

    for (int i = 0; i < NTHREADS; i++) {
        ctx[i].tid = i;
        pthread_create(&th[i], NULL, worker, &ctx[i]);
    }
    for (int i = 0; i < NTHREADS; i++) pthread_join(th[i], NULL);

    uint64_t ok = 0, miss = 0, err = 0;
    for (int i = 0; i < NTHREADS; i++) { ok += ctx[i].ok; miss += ctx[i].miss; err += ctx[i].err; }
    printf("\n=== summary: ok=%lu miss=%lu err=%lu ===\n",
           (unsigned long)ok, (unsigned long)miss, (unsigned long)err);
    return 0;
}
```

编译运行：

```bash
export HPCREDIS_DIR=/root/gqs/codespace/UnifiedBus/hpc-redis
# 注意：必须加 -I$HPCREDIS_DIR/src 才能解析 SDK 头文件中的 xxhash 相对路径
gcc -I$HPCREDIS_DIR/clients/c/build/include -I$HPCREDIS_DIR/src \
    sdk_demo.c \
    $HPCREDIS_DIR/clients/c/build/libvemb_v16_client.a \
    -o sdk_demo -lpthread
./sdk_demo
```

关键点：

- 每线程一个 `vemb_v16_client_t`，禁止跨线程共享句柄。
- `vemb_v16_client_create` 接受一个或多个 TCP bootstrap seed，并固定 TCP 或 AERON
  数据面；SDK 从服务端 topology 选择 key 的 owner。Server 在 topology-set 边界拒绝
  transport 不匹配的 endpoint；SDK 若意外收到错配 snapshot 只记录 `WARNING`，
  不改变启动时固定的数据面。
- 跨节点 UB 场景在首次 UB 请求前调用 `vemb_v16_client_configure_ub_peer_view`；
  manifest 的 `owner_id` 使用 topology 逻辑编号，而非主机名中的 111/112 编号。
- 业务侧只需调 `vadd_pipeline` / `vemb_pipeline` / `vsim_pipeline`，`ASK` / `MOVED` / `STALE_TOPOLOGY` 由 SDK 透明重试。

> 生产级压测工具 `memtier_benchmark`（自定义分支 `UnifiedBus/memtier_benchmark`）也是基于此 SDK 集成 VEMB V16 协议，详见该仓库 README 与 [最佳实践](best_practices.md)。

---

## 维护 HPC-Redis

### 查询 HPC-Redis 日志

掌握日志查询方法以便在遇到故障时对根因进行定位和分析。

HPC-Redis 涉及的日志信息如下表所示。

| 目录 | 文件名 | 文件内容说明 |
| --- | --- | --- |
| `logfile` 指定目录 / 默认 stdout | `redis-<port>.log` | Redis 主进程日志，受 `loglevel` 控制。**注意**：VEMB V16 线程产生的 `LL_VERBOSE` 级别日志会被 `serverLog` 宏静默丢弃，无论 `--loglevel` 如何设置。如需查看 VEMB 详细日志，请用 `loglevel debug`（注意会带来性能下降）。 |
| `/var/log/` / 自定义 | `dmesg` / `syslog` | UB.MEM 设备驱动与内核态日志。通过 `dmesg -T | grep obmm` 查询设备层错误。 |
| 进程 cwd | `redis-<pid>.resp` / 嗅探日志 | VEMB V16 嗅探路径与 RESP 路径的诊断输出，仅在 `loglevel debug` 下落盘。 |

修改日志级别（无需重启）：

```bash
./output/src/redis-cli -p 6379 CONFIG SET loglevel debug
```

### 查询运行时统计

**Proxy 数据面统计（通过 SDK / redis-cli 均可拉取）：**

```c
vemb_v16_stats_t stats;
vemb_v16_client_stats(c, &stats);
/* stats.total_requests / vadd_requests / vemb_requests /
 * vsim_requests / active_channels … */
```

**拓扑重定向观测（仅 SDK）：**

```c
vemb_v16_redirect_stats_t rs;
vemb_v16_client_get_redirect_stats(c, &rs);
/* rs.ask_redirects / moved_redirects /
 * stale_topology_responses / topology_refresh_calls */
```

**Redis 内置 INFO（TLC / UB.MEM 状态）：**

```bash
./output/src/redis-cli -p 6379 INFO memory
./output/src/redis-cli -p 6379 INFO stats
```

> 注意：UB.MEM 热区走 pfn-map，`/proc/<pid>/status` 中的 `VmRSS` / `VmHWM` **不包含**热区映射内存，对比 baseline 时需显式声明口径。

### 关闭服务

```bash
./output/src/redis-cli -p 6379 SHUTDOWN NOSAVE
```

如 `redis-cli` 无法连接（进程已 hang），可用 `pkill -9 -f 'redis-server.*:6379'` 强制结束。

---
