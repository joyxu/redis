# KVC 架构文档：TE over ubmem 实现方案

## 简介

TE over ubmem 是 mooncake-transfer-engine 的传输层实现之一（协议名 `ubmem`），以 UB.MEM 共享内存设备窗口（`/dev/obmm_shmdev*`）作为传输介质承载标准 TE 流量：

- **同机部署**：读写在同一设备窗口内完成，等价于共享内存拷贝，不经过网络协议栈。
- **跨节点部署**：两端节点通过各自的设备窗口视图访问同一物理内存（两侧偏移地址一致），数据搬运由 UB fabric 硬件完成，等效于一次设备内存访问，无协议封装开销。
- **传输层插件，而非独立存储**：不改变 Mooncake 的 master / 分配 / 租约体系，仅在 TE 数据面增加一种介质访问方式，store 层 get / put 流程原样复用。

## 背景

### Mooncake

Mooncake 是 Moonshot AI 为其大模型服务 Kimi 构建并开源的 LLM 推理基础设施（论文获 FAST'25 最佳论文奖），核心设计是**以 KVCache 为中心的存算分离架构**：将 prefill 与 decode 两类计算分离到不同集群调度，同时把 GPU 集群中闲置的 CPU、DRAM 与 SSD 资源聚合为一个分布式 KVCache 池，使前缀计算结果可以跨推理实例复用，从而降低推理时延、提升集群吞吐。

开源代码库按功能划分为以下组件，本方案只涉及前两个：

| 组件 | 职责 |
| --- | --- |
| mooncake-transfer-engine（TE） | 高性能数据搬运框架。对上提供统一的批量数据搬运接口（注册内存、提交 TransferTask），对下以可插拔的 transport 适配不同的网络与介质（RDMA、TCP、CXL、UB 等），并提供拓扑感知选路、多网卡带宽聚合与链路故障切换 |
| mooncake-store | 构建在 TE 之上的分布式 KVCache 存储引擎（下节详述） |
| mooncake-ep / mooncake-pg | 面向大规模 MoE 推理的专家并行通信与容错进程组，与本方案无关 |

### Mooncake Store

Mooncake Store 是面向 LLM 推理场景的分布式键值缓存存储引擎，用于在推理集群内存放并管理可复用的 KVCache 与模型权重。它对外提供对象级的 Put / Get / Remove 接口，支持多副本、租约与驱逐策略，以及 DRAM / SSD 多级存储；对象一经 Put 成功即以整体对外可见，Get 保证返回完整、正确的数据。与 Redis / Memcached 一类通用缓存不同，它面向的是 KVCache 这类"一次写入、多次读取、不原地更新"的对象，分配与驱逐策略均针对该访问模式设计。

部署形态上由两类角色组成：

- **Master Service（`mooncake_master`）**：集群的控制面。它把所有节点贡献的内存视作一个逻辑资源池，负责节点加入 / 退出、对象空间（slot）分配、元数据维护、副本放置与驱逐。Master 只提供元数据，**不经手任何数据流**。
- **Client**：同一个 Client 类承担两种角色——作为客户端，被上层应用调用发起 Put / Get；作为存储端，把本进程挂载的一段连续内存（segment）贡献给集群供其他 Client 读写。数据搬运直接在 Client 与 Client 之间经 TE 完成，旁路 Master。一个实例可以只扮演其中一种角色：本方案中 driver 进程以纯客户端形态运行（`global_segment_size=0`），owner 进程（`mooncake_client`）承担存储端角色。

此外，TE 自身依赖一个独立的元数据服务（本方案使用 `http_metadata_server`）发布与查询段描述符（SegmentDesc：段名、协议、基址等），供各端点互相发现对端内存段。它与 Master Service 分属两套机制，二者均走 TCP。

### 读写路径与 TE over ubmem 的位置

Mooncake Store 的读写路径自上而下分为三层：

1. **应用 / driver 层**：发起 get / put。本方案中为推理引擎或测试 driver（如 `store_kv_bench_native`）。
2. **mooncake-store 服务层**（`client_service` / `RealClient`）：负责批处理（BatchGet / BatchPut）、本地缓冲管理与元数据交互——每批向 Master 发起一次 RPC 取得各 key 的位置信息（Handle：所在段及段内地址），据此组装 TransferTask 提交给 TE。
3. **mooncake-transfer-engine（TE）**：执行实际的数据搬运。TE 内部的 MultiTransport 按目标段的协议（`protocol`）把每个搬运请求分发给对应的 transport 插件。上游 TE 支持 rdma / nvlink / efa / nvmeof 等多种协议，本方案的构建启用了 `tcp` / `ub`（URMA）/ `ubmem` / `cxl` 四个并列插件。

TE over ubmem 的作用位置即此：**TE 的 transport 插件层**。当数据段以 `protocol="ubmem"` 发布时，该段的全部数据搬运任务由 UbmemTransport 执行——同机在设备窗口内按偏移拷贝，跨节点经镜像窗口访问对端物理内存。

![TE over ubmem 在 Mooncake Store 中的位置](kvc_ubmem_position.svg)

作用范围界定：

- **仅数据面**：作用于 get / put 管线之下的数据搬运段（TransferTask 的执行）。store 服务层的批处理、缓冲管理、分配与租约逻辑不变。
- **控制面不经过**：master 的 slot 分配 / 租约 RPC 与元数据服务的段描述符发布、查询均走 TCP，不占用 UB fabric 事务。
- **对上层透明**：store 层与 driver 仅通过 `--protocol=ubmem` 与环境变量启用该路径，不感知设备窗口细节。

一次典型读请求的完整路径：

1. driver 发起 get；
2. store 服务层每批执行一次 master RPC，取得 key 的位置信息（Handle，TCP 通道）；
3. store 层组装 TransferTask 提交 TE；
4. MultiTransport 依据目标段协议（`ubmem`）分发至 UbmemTransport；
5. UbmemTransport 解析目标窗口，在设备窗口内按偏移将数据拷贝至 driver 本地缓冲，同步完成并返回。

## 总体构架

总体构架沿调用链自上而下分为四级。最上层是 mooncake-store 及其应用进程（`mooncake_client` / `store_kv_bench_native`）：`client_service` 在以 `--protocol=ubmem` 初始化时安装 ubmem transport，`real_client` 按约定对设备窗口分区——数据段挂载于窗口底部，本地缓冲取窗口顶部。第二级是 mooncake-transfer-engine：`multi_transport` 提供 transport 工厂（`protocol=="ubmem"` 的安装分支），并为 `getBaseAddr` 增加 ubmem 分支供 store 层获取窗口基址；`transfer_metadata` 扩展 SegmentDesc，使其携带 `ubmem_name` / `ubmem_base_addr` 随段发布。第三级是 UbmemTransport（`ubmem_transport.cpp`），完成整窗 mmap、属主地址到窗口内偏移的换算与同步内联拷贝。最底层是 UB.MEM 设备窗口：本端数据段位于本地池窗，跨节点访问经镜像窗，物理搬运由 UB fabric 直达对端节点内存。

![TE over ubmem 总体构架](kvc_ubmem_overall_structure.svg)

涉及代码：

| 位置 | 内容 |
| --- | --- |
| `Mooncake/mooncake-transfer-engine/src/transport/ubmem_transport/` | 传输层本体（安装 / 注册 / 解析 / 拷贝） |
| `Mooncake/mooncake-transfer-engine/include/transport/transport.h` | Slice union 增加 `ubmem.dest_addr` |
| `Mooncake/mooncake-transfer-engine/src/multi_transport.cpp` | 工厂分支与 `getBaseAddr` 扩展 |
| `Mooncake/mooncake-transfer-engine/include/transfer_metadata.h` | SegmentDesc 增加 `ubmem_name` / `ubmem_base_addr` 字段与编解码、协议白名单 |
| `Mooncake/mooncake-store/src/client_service.cpp` | `--protocol=ubmem` 时的 transport 安装分支 |
| `Mooncake/mooncake-store/src/real_client.cpp` | 窗口分区挂载（见"store 层集成"） |

## 核心抽象：设备窗口

传输层设计基于设备窗口的三个基本性质。

**窗口即访问介质**。每个 `obmm_shmdevN` 设备节点以整窗 `mmap` 方式使用：本地池窗映射本节点物理内存，镜像窗映射对端节点物理内存。进程对窗口内偏移的读写即对该物理内存的读写。

**偏移地址恒等**。同一块物理内存在两端节点上的两个视图（属主侧本地池窗与访问侧镜像窗）共享同一套偏移地址：`本地窗基址 + off` 与 `镜像窗基址 + off` 指向同一字节。这是跨节点机制的正确性基础：两侧偏移天然一致，无需地址转换表。

## 数据面实现

`install` / `registerLocalMemory` / `submitTransferTask` 分别对应介质访问能力的三个环节：`install` 把设备窗口接入 TE 并将承载它的段发布给对端（接入介质），`registerLocalMemory` 把窗口内内存登记为可传输区域（登记内存），`submitTransferTask` 在源与目标窗口之间执行实际搬运（执行搬运）。三者齐备，TE 即获得以 UB.MEM 设备窗口为介质的搬运能力：`protocol="ubmem"` 段上的标准 get / put 数据面全部以设备窗口内存访问完成，上层管线无需任何改动。

TE 侧的接入点是 Transport 插件接口。`UbmemTransport` 继承 Transport 基类，实现 `install` / `registerLocalMemory` / `submitTransferTask` 三个核心接口；MultiTransport 不是基类，而是工厂与分发器——初始化时按 `protocol=="ubmem"` 实例化插件并持有，运行期 `submitTransfer` 先按目标段协议选出插件（`selectTransport`），再把任务列表分发给所选插件的 `submitTransferTask`。本节即按这三个接口组织：

| Transport 接口 | UbmemTransport 实现 | 小节 |
| --- | --- | --- |
| `install()` | 整窗 mmap 设备窗口、解析跨节点镜像映射表、启动大块拷贝线程池、发布段描述符 | 安装与段发布 |
| `registerLocalMemory()` | 窗口内地址换算为 `BufferDesc` 偏移并记入元数据 | 内存注册 |
| `submitTransferTask()` | 目标解析（段 → 本端窗口映射）＋ 拷贝执行（偏移换算与同步拷贝） | 搬运执行 |

### 安装与段发布（install）

`install()` 补齐的能力即"以 UB.MEM 设备窗口为介质搬运数据"：把整窗 mmap 后的设备窗口注册为 TE 的本端介质（对应 rdma 插件之网卡、cxl 插件之 DAX 设备），并把承载窗口的内存段发布到元数据服务、使对端可发现。它完成以下动作，任一步失败即终止安装：

1. 进程级单实例检查（CAS）：进程内已存在存活实例则拒绝安装。
2. 打开 `MC_UBMEM_DEV_PATH` 指定的池设备，窗口大小取 `MC_UBMEM_DEV_SIZE`（缺省以 `lseek` 探测），整窗 `mmap(MAP_SHARED)`。
3. 解析 `MC_UBMEM_PEER_DEV_MAP` 跨节点镜像映射表。
4. 启动大块并行拷贝线程池（`MC_UBMEM_COPY_THREADS`，默认 8）。
5. 构造并发布段描述符：`protocol="ubmem"`、`ubmem_name`（设备路径）、`ubmem_base_addr`（本端映射基址），经元数据服务对外可见。

段描述符随进程生存周期撤销发布：进程退出（包括被强制终止）后即失效。

### 内存注册（registerLocalMemory）

`registerLocalMemory(addr, length)` 是 TE 将本端内存登记为可传输区域的接口。UbmemTransport 的实现校验地址位于窗口内 `[base, base + window_size)`，换算为 `BufferDesc{offset = addr − base, length}` 记入元数据；窗口外地址与越界长度直接返回错误。

### 搬运执行（submitTransferTask）

`submitTransferTask(task_list)` 是 TE 执行数据搬运的接口：任务列表由 MultiTransport 按目标段协议选中本插件后传入（同一插件的请求合并为一个列表）。UbmemTransport 的实现为同步内联——每个请求先做目标解析、再做拷贝执行，函数返回即搬运完成。

#### 目标解析（每请求）

快路径查询**无锁段解析缓存**：缓存以段 id 为索引，以原子指针存放 `{目标窗口映射, 属主窗口基址, NC 标志}`，命中后直接返回，无需加锁，也不涉及字符串解析。每个段仅在首个请求执行一次慢路径。

慢路径流程：

```
读取段描述符（校验 protocol=="ubmem" 且携带 ubmem_name）
  ├─ 同机目标（按段名 host 部分比较）→ 直接映射目标设备（cacheable 语义）
  └─ 跨机目标 → 查询 MC_UBMEM_PEER_DEV_MAP："对端主机:对端设备" → 本端镜像设备
                （镜像窗映射按需惰性建立，恒为 NC 语义）
解析结果写入缓存（并发首次解析以 CAS 写入，失败方丢弃，二者结果等价）
```

同机判定按 host 部分比较的原因：段名与端点各自带有端口（如 `127.0.0.1:50052` 与 `127.0.0.1:23011`），按完整字符串比较会将同机目标误判为跨机目标，错误地查找镜像设备。

#### 拷贝执行

地址换算（每请求同步完成，由调用线程直接执行）：

```
窗口内偏移 = target_offset（属主侧 VA）− 段描述符.ubmem_base_addr
实际访问地址 = 本端窗口基址 + 窗口内偏移
```

边界校验（偏移低于属主基址、偏移 + 长度越出窗口）失败时该请求标记 FAILED，任务不中断。拷贝引擎按窗口语义与长度选择：

| 条件 | 引擎 |
| --- | --- |
| 长度 ≥ 1MB | 常驻并行分片线程池：提交线程与 N 个 worker 各拷贝一个分片（64B 对齐切分），全部完成后返回 |
| NC 窗且 `MC_UBMEM_INLINE_MEMCPY=0` | 8 深独立 load 软件流水（仅单线程 NC 场景占优） |
| 其余（默认） | glibc memcpy（NEON 向量化，高并发下 cacheable / NC 均优） |

完成语义为同步内联：`submitTransferTask` 返回时拷贝已经完成，随后执行设备内存屏障（`__sync_synchronize`）保证设备侧写入对后续访问可见。不存在异步任务队列。

## 跨节点设计

拓扑：owner 段位于 owner 节点的本地池窗（如 `obmm_shmdev2`），driver 通过本节点的镜像窗（如 `obmm_shmdev6`）访问。物理路径为 driver CPU → UB fabric → owner 内存。

与其他跨节点路径的机制对比：

| 路径 | 数据通道 | 校验 / 元数据通道 |
| --- | --- | --- |
| ubmem | 镜像窗读（fabric 内存访问） | 每批一次 master RPC（TCP，不占用 fabric 事务） |
| tcp / urma | 以太网 / DMA 搬运 | 同左 |
| KVC 直读 | 镜像窗读 | 每 key 一次 slot 查询（同样占用 fabric 事务） |

ubmem 每个 key 仅产生一次 fabric 内存访问，且同一批内各次拷贝相互独立、可完全并行流水执行，因此跨节点吞吐接近本地水平（128 线程峰值 8266.6 对 8319.1 MiB/s，约 99%）；KVC 直读的数据与校验两次访问存在依赖关系、共享 fabric 事务处理能力，跨节点吞吐明显低于 ubmem。

## store 层集成

real_client 按窗口分区约定使用设备窗口：

```
┌──────────────────── 设备窗口（MC_UBMEM_DEV_SIZE，如 8GB）────────────────────┐
│ [base, base + global_segment_size)             │ [base + window − buf, base + window) │
│ owner 数据段（MountSegment 挂载，窗口底部）      │ 本地缓冲（ClientBufferAllocator，窗口顶部） │
└────────────────────────────────────────────────┴──────────────────────────────┘
```

- `MC_UBMEM_DEV_SIZE` 必须为完整窗口大小，否则分区不合法；约束条件为
  `global_segment_size + local_buffer_size ≤ window`。
- owner 进程（`--global_segment_size=4GB`）：段占用窗口底部，`local_buffer_size` 可为 0。
- driver 进程（`--global_segment_size=0`）：不挂载数据段，仅使用窗口顶部缓冲（如 256MB）。
- 窗口顶部缓冲适用于单 driver 场景；多 driver 进程并存时需为各进程划分互不重叠的窗口区域。
- driver 读取对端数据时，本端缓冲使用本节点自己的本地池窗，读操作经镜像窗完成——两者是不同窗口，经 `MC_UBMEM_PEER_DEV_MAP` 关联。

## 环境变量

| 变量 | 必设 | 说明 |
| --- | --- | --- |
| `MC_UBMEM_DEV_PATH` | 是 | 本进程池设备（如 `/dev/obmm_shmdev2`），本地池窗 |
| `MC_UBMEM_DEV_SIZE` | 是¹ | 窗口字节数（完整窗口；store 分区模式强制要求显式设置） |
| `MC_UBMEM_PEER_DEV_MAP` | 跨节点 | 镜像解析表：`"对端主机:对端设备=本端镜像设备,..."` |
| `MC_UBMEM_INLINE_MEMCPY` | 否 | 内联拷贝引擎，默认 `1`=glibc memcpy；`0` 恢复 NC 窗软件流水 |
| `MC_UBMEM_COPY_THREADS` | 否 | ≥1MB 大块并行分片线程池线程数，默认 8（0-64）。拷贝池 dispatch 为全局互斥，多线程大块场景会被串行化，应设 0（各线程内联拷贝）；仅单线程大块保留默认有利 |
| `MC_UBMEM_FORCE_O_SYNC` | 否 | 诊断开关：两侧强制 O_SYNC 映射 |

## 语义边界与实现约束

- **单协议段**：`protocol="ubmem"` 的段不参与多协议组合路由；传输层不含分配器（缓冲为窗口顶部手工分区，非 slot 语义）。
- **一致性由上层保证**：传输层只负责字节搬运，读写可见性由 store 层的租约与分配语义保证。
- **owner 须长期驻留**：段描述符随进程生存周期撤销发布，owner 退出后读方即无法访问。
- **进程内至多一个存活实例**：install() 以进程级 CAS 检查拒绝第二实例并存（类级解析缓存按裸段 id 索引，多实例的段 id 空间相互冲突）；顺序"销毁→重建"合法（析构清理缓存并释放标志）。
- **实例布局冻结**：不得为 UbmemTransport 增加任何实例成员——对象大小一旦改变，会在启动阶段触发一处与本传输层无关的潜在堆越界（`malloc(): invalid size`）。新增状态一律使用类级 static 成员（不参与 sizeof），头文件中有注记，修改前必须阅读。

## 实验

在双机鲲鹏环境部署 Mooncake Store 全栈（元数据服务 + mooncake_master + 段 owner mooncake_client + 原生基准 store_kv_bench_native），以 `--protocol=ubmem` 验证 TE over ubmem 读路径的吞吐与延迟。测试机器为 owner=192.168.90.111（HW01）、driver=192.168.90.112（HW02）；数据集 100000 key × 4KB，批大小 64，owner 段 4GB（窗口底部）+ driver 本地缓冲 256MB（窗口顶部），`MC_UBMEM_DEV_SIZE=8GB`，跨节点经 `MC_UBMEM_PEER_DEV_MAP` 镜像窗访问；读线程梯度 8 / 16 / 32 / 64 / 128，每档 20s，fill 单线程串行，verify 开启（failed / verify_fail 全零）。

TE over ubmem 读吞吐（4KB 口径，格式 = MiB/s（p50 ms））：

| 线程 | 本地（driver 与 owner 同机） | 跨节点（经镜像窗 / UB fabric） |
| ---: | --- | --- |
| 8 | 3860.2（0.50） | 2952.5（0.66） |
| 16 | 5844.1（0.65） | 4994.2（0.78） |
| 32 | 7688.2（1.01） | 7543.3（1.03） |
| 64 | 8218.4（1.95） | 8185.3（1.97） |
| 128 | **8319.1（3.83）** | **8266.6（3.75）** |

与其他读路径对照（跨节点，8-32 线程档，四条路径均有有效结果，格式 = MiB/s（p50 ms））：

| 线程 | TE over TCP | TE over URMA | TE over ubmem | TE over cxl |
| ---: | ---: | ---: | ---: | ---: |
| 8 | 259.9（7.9） | 2213.4（0.9） | **2952.5（0.66）** | 2865.1（0.68） |
| 16 | 250.5（16.2） | 3027.7（1.3） | **4994.2（0.78）** | 4765.8（0.81） |
| 32 | 254.0（32.3） | 1967.2（4.0） | **7543.3（1.03）** | 6830.2（1.15） |

判读：

- 本地峰值 8319.1 MiB/s 达同条件 UB 直读（绕过 TE 与 store 管线的裸 mmap 批读，峰值 8299.4）水平——TE 管线开销已不构成瓶颈，吞吐由内存带宽决定。
- 跨节点峰值 8266.6 MiB/s 为本地的 99%，各线程档均居各读路径之首：每 key 仅一次 fabric 访存，批内各拷贝相互独立、可全速流水。
- 低并发对照档（32 线程，跨节点）：ubmem 为 URMA 的 3.8 倍，较同构的 cxl 高约 10%；TCP 在 4KB 小对象口径下受每请求协议栈开销限制（254 MiB/s）。
- 达同窗随机 4KB 纯拷贝上限（12,961 MiB/s）的 64%，其余为 store 管线（master RPC、批处理、校验）开销。

1 MiB 大块口径（2026-10-20 实测，owner=192.168.90.161 / driver=192.168.90.162，批 16、3072 key、driver 缓冲 2.5GB、`MC_UBMEM_COPY_THREADS=0`，格式 = MiB/s（p50 ms））：

| 线程 | 本地 | 跨节点 |
| ---: | --- | --- |
| 8 | 39737（1.8） | 7721（15.4） |
| 16 | 56692（2.2） | 11209（21.6） |
| 64 | 58630（5.4） | 10518（82.6） |
| 128 | **57029（9.8）** | **11313（197.0）** |

- 大块场景吞吐上限大幅抬升：本地 57-59 GB/s 贴内存带宽（TCP 同口径约 4.6-4.8 GB/s，约 12 倍）；跨节点 11.2-11.3 GB/s，约为 100G 口 TCP 峰值的 3.3 倍，16 线程即达平台。
- 多线程大块部署 driver 侧须设 `MC_UBMEM_COPY_THREADS=0`：默认拷贝池的 dispatch 为全局互斥，会把 ≥1MB 拷贝串行化，吞吐反而低于 TCP（单线程大块场景保留默认更有利）。

复现方法与四路径完整数据见 [KVC 测试方案](kvc_test_plan.md)；逐条命令的复现手册见 `docs/KVC_NATIVE_BENCH_REPRO.md`。

## 更多信息

| 文档 | 内容 |
| --- | --- |
| [KVC 用户指南：TE over ubmem](kvc_ubmem_user_guide.md) | 编译开关、环境变量与部署使用方法 |
| [KVC 测试方案：四条读路径对比](kvc_test_plan.md) | 四条读路径的统一条件对比测试方法与参考数据 |
| `docs/UBMEM_TRANSPORT.md` | 实现侧工程文档（含优化历史与实测明细） |

---
