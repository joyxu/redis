# KVC 架构文档：TE over ubmem 实现方案

## 简介

TE over ubmem 是 mooncake-transfer-engine 的传输层实现之一（协议名 `ubmem`），以 UB.MEM 共享内存设备窗口（`/dev/obmm_shmdev*`）作为传输介质承载标准 TE 流量：

- **同机部署**：读写在同一设备窗口内完成，等价于共享内存拷贝，不经过网络协议栈。
- **跨节点部署**：两端节点通过各自的设备窗口视图访问同一物理内存（两侧偏移地址一致），数据搬运由 UB fabric 硬件完成，等效于一次设备内存访问，无协议封装开销。
- **传输层插件，而非独立存储**：不改变 Mooncake 的 master / 分配 / 租约体系，仅在 TE 数据面增加一种介质访问方式，store 层 get / put 流程原样复用。

## 背景：Mooncake Store

Mooncake Store 的读写路径自上而下分为三层：应用 / driver 层发起 get / put；mooncake-store 服务层（client_service / RealClient）负责批处理、缓冲管理与元数据交互；数据搬运由 mooncake-transfer-engine（TE）执行。TE 内部的 MultiTransport 按目标段的协议（`protocol`）将数据搬运请求分发给对应的 transport 插件，`tcp` / `ub` / `ubmem` / `cxl` 即四个并列的插件。

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

## 总体结构

```
mooncake-store（mooncake_client / store_kv_bench_native）
    │  client_service：--protocol=ubmem 安装分支
    │  real_client：窗口分区（段位于窗口底部，本地缓冲位于顶部）
    ▼
mooncake-transfer-engine
    │  multi_transport：transport 工厂（protocol=="ubmem"）+ getBaseAddr 的 ubmem 分支
    │  transfer_metadata：SegmentDesc 携带 ubmem_name / ubmem_base_addr
    ▼
UbmemTransport（ubmem_transport.cpp）
    │  整窗 mmap + 偏移换算 + 同步内联拷贝
    ▼
UB.MEM 设备窗口（本地池窗 / 镜像窗）── UB fabric ── 对端节点内存
```

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

### 安装与段发布

`install()` 完成以下动作，任一步失败即终止安装：

1. 进程级单实例检查（CAS）：进程内已存在存活实例则拒绝安装。
2. 打开 `MC_UBMEM_DEV_PATH` 指定的池设备，窗口大小取 `MC_UBMEM_DEV_SIZE`（缺省以 `lseek` 探测），整窗 `mmap(MAP_SHARED)`。
3. 解析 `MC_UBMEM_PEER_DEV_MAP` 跨节点镜像映射表。
4. 启动大块并行拷贝线程池（`MC_UBMEM_COPY_THREADS`，默认 8）。
5. 构造并发布段描述符：`protocol="ubmem"`、`ubmem_name`（设备路径）、`ubmem_base_addr`（本端映射基址），经元数据服务对外可见。

段描述符随进程生存周期撤销发布：进程退出（包括被强制终止）后即失效。

### 内存注册

`registerLocalMemory(addr, length)` 校验地址位于窗口内 `[base, base + window_size)`，换算为 `BufferDesc{offset = addr − base, length}` 记入元数据。窗口外地址与越界长度直接返回错误。

### 目标解析（每请求）

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

### 拷贝执行

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
| `MC_UBMEM_COPY_THREADS` | 否 | ≥1MB 大块并行分片线程池线程数，默认 8（0-64） |
| `MC_UBMEM_FORCE_O_SYNC` | 否 | 诊断开关：两侧强制 O_SYNC 映射 |

## 语义边界与实现约束

- **单协议段**：`protocol="ubmem"` 的段不参与多协议组合路由；传输层不含分配器（缓冲为窗口顶部手工分区，非 slot 语义）。
- **一致性由上层保证**：传输层只负责字节搬运，读写可见性由 store 层的租约与分配语义保证。
- **owner 须长期驻留**：段描述符随进程生存周期撤销发布，owner 退出后读方即无法访问。
- **进程内至多一个存活实例**：install() 以进程级 CAS 检查拒绝第二实例并存（类级解析缓存按裸段 id 索引，多实例的段 id 空间相互冲突）；顺序"销毁→重建"合法（析构清理缓存并释放标志）。
- **实例布局冻结**：不得为 UbmemTransport 增加任何实例成员——对象大小一旦改变，会在启动阶段触发一处与本传输层无关的潜在堆越界（`malloc(): invalid size`）。新增状态一律使用类级 static 成员（不参与 sizeof），头文件中有注记，修改前必须阅读。

## 更多信息

| 文档 | 内容 |
| --- | --- |
| [KVC 用户指南：TE over ubmem](kvc_ubmem_user_guide.md) | 编译开关、环境变量与部署使用方法 |
| [KVC 测试方案：四条读路径对比](kvc_test_plan.md) | 四条读路径的统一条件对比测试方法与参考数据 |
| `docs/UBMEM_TRANSPORT.md` | 实现侧工程文档（含优化历史与实测明细） |

---
