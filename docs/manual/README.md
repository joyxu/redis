# HPC-Redis

## 简介

HPC-Redis 是基于 **灵衢 UB.MEM 远程内存**、 **鲲鹏处理器内置 SVE2 / SVE 向量化计算**与**VEMB V16 向量协议** 的增强版本，提供高吞吐、低延迟的向量检索服务。

HPC-Redis 的核心特点：

- **集成版 `redis-server`**：VEMB V16 向量协议直接内嵌进 Redis 主进程，通过 Redis 端口同时承载 RESP 协议与 VEMB V16 二进制协议，无需独立部署额外的向量服务进程。
- **四类向量操作**：在原有 RESP 命令基础上新增 `VADD`（写入）、`VEMB`（读取）、`VSIM`（余弦相似度）、`VREM`（删除）四种向量操作，覆盖向量检索的主要路径；另支持 `VSIM_2KEY` 双 key 余弦负载（经 memtier `--vemb-v16-vsim-key-key`，双 key 取回 + 服务端相似度）。
- **Aeron 数据面**：除 TCP 外提供 Aeron 传输（TCP 控制面 + UB 共享内存环数据面，v1 双环 + v2 batch 四区通道），单机 17:17 配置实测读吞吐可达 3000 万 ops/s 量级。`vemb-v16-transport sniff|aeron` 一键切换。
- **UB.MEM 三层缓存 + COLD 持久化**：通过鲲鹏 UB.MEM 设备（`/dev/obmm_shmdev{1..16}`）将远端节点内存映射为本地热区（warm region），由 pfn-map 承载，构建 "本地 DRAM / UB 热区 / COLD 冷存储" 三层缓存层次；设置 `HPC_REDIS_COLD_DIR` 即启用 COLD 持久化与重启恢复。
- **高可用（HA）**：keepalived VIP 故障迁移 + 心跳检测 + HA Replica 副本恢复 + 跨节点重同步，单 owner 故障后 standby 接管数据面。
- **在线扩容（scaleout）**：双节点拓扑在线变更（active 0 → active {0,1}）+ 数据迁移，TCP 与 Aeron 两种数据面均支持，扩容过程业务不中断。
- **SVE2 / SVE 向量化计算**：在支持 SVE 的鲲鹏处理器上，向量搬运与相似度计算使用 `svld1_f32` 等 SVE 指令（`ld1w`），相较 `memcpy` 在远端 UB 内存场景获得显著吞吐提升。
- **客户端 SDK**：提供 C 语言静态库 `libvemb_v16_client.a` 与动态库 `libvemb_v16_client.so`，封装 HELLO/WELCOME 握手、请求/响应帧序列化、多端点一致性哈希路由与拓扑重试，业务侧链接 SDK 即可访问向量快速路径。
- **拓扑运维工具**：`vemb_v16_topology_ctl` 支持拓扑查询/发布（`--get` / `--set` / `--set-with-peer-view-map`）、运行统计与扩容协调器模式。


## 能力支持与规格

### 向量操作支持

| 操作 | 命令 | 说明 | VEMB V16 二进制路径 |
| --- | --- | --- | --- |
| 写入 | `VADD` | 向指定集合写入一个向量 | 支持 |
| 读取 | `VEMB` | 取回指定元素的向量（inline vector） | 支持 |
| 相似度 | `VSIM` | 计算指定元素与 query 向量的余弦相似度 | 支持 |
| 双 key 相似度 | `VSIM_2KEY` | 双 key 取回 + 服务端余弦（memtier `--vemb-v16-vsim-key-key` 负载） | 支持 |
| 删除 | `VREM` | 从集合中删除指定元素（幂等） | 支持 |


### 客户端 SDK 接入

提供 C 语言静态库 `libvemb_v16_client.a` 与动态库 `libvemb_v16_client.so`，封装 HELLO/WELCOME 握手、请求/响应帧序列化、多端点一致性哈希路由与拓扑重试。业务侧链接 SDK 后即可通过 `vemb_v16_client_sdk.h` 中声明的 API 访问向量快速路径。

| 链接方式 | 说明 |
| --- | --- |
| 静态链接（推荐） | 链接 `libvemb_v16_client.a`，SDK 被吸收进业务 binary，部署单一可执行文件。 |
| 动态链接 | 链接 `libvemb_v16_client.so`，运行时需 `LD_LIBRARY_PATH` 或 `rpath` 定位。 |

> 示例代码见 `clients/c/example.c`（单端点 VADD / VEMB / VSIM 端到端）。

### 配置与调优能力

| 能力 | 对应参数 / 机制 | 说明 |
| --- | --- | --- |
| Proxy I/O 线程 | `--vemb-v16-proxy-io-threads` | 承接 VEMB V16 帧的读取 / 解析 / 转发，建议与 SuperNode workers 按 1:2 配置。 |
| SuperNode workers | `--vemb-v16-supernode-workers` | 处理三层缓存查询与向量计算的工作线程数。 |
| 热区 manifest | `--vemb-v16-warm-regions-manifest` | YAML 描述 UB.MEM 热区映射（region_id / path / mmap_offset / bytes / value_size 等）。 |
|

## 已验证的软硬件环境

### 硬件环境

| 项目 | 规格 |
| --- | --- |
| 处理器 | 鲲鹏 920 / 鲲鹏 930（aarch64，支持 SVE / SVE2 256-bit） |
| 核数 | 单机 192 核（2 socket × 96 核，典型配置） |
| 内存 | 单机 ≥ 256 GB DDR4 / DDR5 |
| UB.MEM 设备 | `/dev/obmm_shmdev{1..16}`（8 个本地 export + 8 个对端 import，各 8 GiB），通过 `obmmctl` 初始化；跨节点 SNP（Scalable Node Platform）按需启用 |
| 网络 | 节点间 100 GbE RoCE（mlx5）或 10 GbE TCP；跨节点部署要求网卡带宽 ≥ 预期向量读吞吐 |
| 存储 | 本地 NVMe SSD（持久化关闭时非必需） |

### 软件环境

| 项目 | 版本要求 |
| --- | --- |
| 操作系统 | openEuler 22.03 LTS / openEuler 24.03 LTS |
| 内核 | Linux 5.10+（建议 6.6+，UB.MEM 驱动对内核版本有依赖） |
| GCC | 10.3+（需支持 `-march=armv8.2-a+sve`） |
| GNU Make | 4.x |
| jemalloc | 5.x（Redis 默认内存分配器） |
| Python | 3.8+（部分压测脚本依赖） |
| mutagen（可选） | 0.18+（本地 macOS 与服务器间代码同步，非运行时依赖） |

> 跨架构（x86_64）平台不在已验证范围内。HPC-Redis 依赖鲲鹏 SVE 指令集与 UB.MEM 设备，仅在 aarch64 + 鲲鹏处理器上提供完整能力。

## 文档导航

| 文档 | 内容 |
| --- | --- |
| [安装指南](installation_guide.md) | 环境要求、获取源码、编译 `redis-server` 与客户端 SDK、安装后检查、卸载。 |
| [快速入门](quick_start.md) | 一键启动 HPC-Redis、通过 `redis-cli` 快速验证向量操作、通过 SDK 端到端示例验证完整链路。 |
| [使用手册](user_guide.md) | 启动参数与配置文件、客户端 SDK 同步 / Pipeline / 异步 API、多端点集成示例、日志与运行时统计、关闭服务。 |
| [最佳实践](best_practices.md) | `redis-cli` 快速路径使用、`memtier_benchmark` 压测、单机 / 跨节点部署示例、性能调优、SVE 与 memcpy 场景对比。 |
| [KVC 架构文档：TE over ubmem](kvc_ubmem_architecture.md) | TE over ubmem 传输层的实现方案：设备窗抽象、数据面设计、跨节点镜像机制与 store 层接线。 |
| [KVC 用户指南：TE over ubmem](kvc_ubmem_user_guide.md) | TE over ubmem 的编译开关、环境变量、窗口分区规则与单机 / 跨节点使用方法。 |
| [KVC 测试方案：四条读路径对比](kvc_test_plan.md) | TE over TCP / URMA / ubmem / cxl 的同口径对比测试方法、结果判读与参考数据。 |
| [FAQ](faq.md) | 硬件、UB.MEM 设备、安装编译、运行验证、性能异常等常见问题与排障方法。 |

---
