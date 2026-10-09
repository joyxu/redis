# KVC SDK — hpc-redis 的 KV Cache 介质 SDK

把 hpc-redis 已验证的 UB 共享内存能力封装为独立的 KV Cache（KVC）介质 SDK，
供 Mooncake Store 及其他推理框架接入。纯 C ABI，零 Redis 依赖。

## 定位

```
                     libkvc_server.so            libkvc_client.so
                    （介质管理者）                 （读方）
                   ┌──────────────────┐        ┌──────────────────┐
  mooncake_master ─┤ slot 分配器(位图) │   推理 ┤ region_set       │
  (替代 Offset-    │                  │   框架 ├──────────────────┤
   BufferAllocator)│ region 生命周期   │        │ read_handle(拷贝)│
  mooncake_client ─┤ (shmdev mmap)    │        │ deref_handle(0拷)│
  (持段方)         │ slot 状态机      │        └──────────────────┘
                   │ slot_addr(deref) │
                   └──────────────────┘
                              │
                    core: ub / layout / ring / net
                    （从 src/ 抽取的耦合度-1 积木）
```

与 Mooncake 的接缝（已全部落地）：

| Mooncake 触点 | SDK 提供 |
|---|---|
| MountSegment（挂段） | `kvc_region_open_local`（validate 复用/CREATE 初始化）+ `kvc_region_meta` + `kvc_region_publish_base` |
| PutStart 分配 offset（master） | `kvc_slot_allocator_alloc`（offset = slot_idx × block_size；master 侧经 KvcSlotAllocator 包装为 BufferAllocatorBase） |
| PutEnd（写完可见） | `kvc_slot_mark_ready`（release 语义 + write_seq；批量集中发布） |
| GetReplicaList → 读 | `kvc_direct_read`：slot READY 检查 → 流水拷贝 → 读后哨兵/write_seq 校验，不符回退 TE |
| Remove / 驱逐 | `kvc_slot_invalidate`（位置缓存驱动） |
| master 重启恢复 | `kvc_slot_allocator_rebuild`（从介质 slot 状态重建位图） |

## 能力分级

- **已落地**：region 生命周期（含重启持久复用）/ slot 分配器（master 集成）/ slot
  状态机 / 直读 + 读后校验防线 / 直写（`kvc_write_slot_direct`）/ 崩溃恢复
  （`scan_state`+`recover_stale`）/ 位图重建 / 位置缓存硬校验（Mooncake 侧 poscache）/ 统计。
- **未做（触发条件见 optional/）**：多 size class region_set 完整化、EVICTING 态、
  内置路由（无 master 自组网）、变长 blob、控制面 ring 化。

## 构建

```bash
make check   # 公共头自包含性检查
make all     # 产出 build/libkvc_{server,client}.{so,a} + build/headers_check
make tools   # + build/kvc_smoke  build/kvc_probe  build/kvc_nc_bw
```

编译口径与 clients/c/Makefile 对齐（aarch64 加 `-DUSE_ARM_SVE -march=armv8.2-a+sve`），
仅依赖 `-lpthread -lm`。

## 目录

```
include/kvc/     公共 API（kvc_common / kvc_server / kvc_client）
core/            从 src/ 抽取的积木（各 README 记录来源与耦合度）
  ub/            shmdev mmap + O_SYNC fallback + kvc_copy_remote（平台硬约束见 README）
  layout/        region header + slot 表布局（shared_allocator 64B header 模式）
  ring/          SPSC 环（预留）
  net/           TCP 帧 + attach 握手报文
optional/topo/   一致性哈希路由（沉底，启用条件见 README）
client/ server/  双库实现
examples/        参考进程 / demo 骨架
tests/
  kvc_smoke.c          端到端语义冒烟（P0-P1，33 断言）
  kvc_probe.c          独立进程 region 探针（header/slot 状态/数据 pattern）
  kvc_nc_bw.c          nc 远端读带宽（线程扩展性）
  kvc_trace_replay.py  FAST25 真实 trace 回放（批量/loops 预热-稳态）
  kvc_regression.sh    性能回归门禁（4 配置矩阵，内置 sysctl/TTL/TE-pool 前提）
  kvc_correctness_gate.sh  正确性门禁（同机/跨节点 verify ×5 + Remove 联动 + 重启持久，12 项）
  upstream_issues_draft.md  上游三份 issue 草稿
  headers_check.c      头文件自包含检查
```

## 关键约束与前置

- 一个 region 一个 block_size（= 目标模型 KV page，如 glm5 MLA 7MB）；
  混合层组模型用多 region 分 size class。
- 远端访问必须 nc/O_SYNC 映射 + 流水拷贝（`kvc_copy_remote`，glibc memcpy
  在 nc 内存上不流水，单线程差 30 倍）；decoder_flag 必须 0x63（见 core/ub/README）。
- **测试环境前提**（否则数据失真）：
  - `sysctl tcp_tw_reuse=1` + 宽端口范围 —— TE TCP one-shot 连接在默认
    sysctl 下有端口压力，会同时压低 TE 基线并诱发传输异常；
  - master `-default_kv_lease_ttl` 按测试时长调大（默认 10s）。
- 内存序契约：写端 release 翻 READY、读端 acquire 查状态（SNP 可见性顺序），
  详见 kvc_common.h。

## 实测基线（store_kv_bench, 4KB block, daemon 段 + 纯 client 拓扑）

前提：sysctl 校准 + TE 连接池 16lane（基线推到最强）+ master TTL 调大。

### 同机（HW01）

| 配置 | MiB/s | kv/s | p50 | vs TE |
|---|---:|---:|---:|---|
| TE（连接池+16lane，最强基线） | 273 | 70,508 | 0.45 ms | 1× |
| KVC 直读 | ~850 | ~218k | 0.14 ms | 3.1× |
| KVC 直读+位置缓存（含防线） | **1408** | 360,645 | **0.083 ms** | **5.2×** |

### 跨节点（HW02 → HW01，nc 映射）

| 配置 | MiB/s | kv/s | p50 | vs TE |
|---|---:|---:|---:|---|
| TE（连接池+16lane） | 157 | 39,564 | 0.80 ms | 1× |
| KVC 直读+位置缓存（含防线） | **405** | 103,644 | **0.301 ms** | **2.6×** |

### FAST25 真实 trace 回放（2000 请求批量，同机）

| 场景 | 阶段 | 命中率 | KVC MiB/s | RAM(TE) MiB/s | KVC 倍数 |
|---|---|---:|---:|---:|---:|
| conversation | 稳态(100%命中) | 100% | **787** | 267 | 2.9× |
| synthetic | 稳态(100%命中) | 100% | **1381** | 266 | 5.2× |
| toolagent | 稳态(100%命中) | 100% | **1380** | 258 | 5.3× |
| 三场景 | 冷回放(写:读≈2:1) | 29-44% | 208-246 | 180-200 | 1.1-1.4× |

注意：早前 91×/143× 的倍数基于端口压力下失真的 TE 基线（9.9 MiB/s），已作废；
上表全部为基线最强口径。跨节点直读历史上出现过 2 次未定位间歇性内容不符
（均在端口压力窗口内，防线部署后 13+ 次同模式不复现），跨节点直读默认 env
关闭，详见 core/ub/README。

## 测量口径说明（重要，防误读）

- store_kv_bench 的 MiB/s = 应用侧有效字节（kv/s × value_size），非线速。
- read_perf 常用工作集 8MB（L3 常驻）；>L3 验证：200MB×30s 长跑
  （1341 MiB/s，misses=0，需 lease TTL 调大）+ kvc_probe 全量字节 pattern 校验。
- 对比基线是"TE-TCP + python 单车道"形态；对标 Mooncake 生产形态
  （RDMA + C++）比值会显著缩小。正确表述：移除了传输栈开销。
- 写路径微基准（8MB/45ms）在本机有 ±20% 时段性波动（共租干扰），
  小样本不可用，正式数字需独占机器或 ≥100k 对象长跑（见下节）。

## 耦合开发工作流（hpc-redis ⇄ Mooncake submodule）

Mooncake 以 submodule 钉在个人 fork（qs-ftw/Mooncake）。两侧改动常耦合
（SDK 接口变 → 适配层必须同步变），规程：

```bash
# 一次性准备
git clone --recurse-submodules <hpc-redis>     # 新克隆自动拉配套 Mooncake
git submodule update --init                    # 已有克隆补拉

# 日常耦合修改（顺序不能反）
1. cd Mooncake && 改 && git commit && git push origin feature/kvc-adapter
2. cd .. && 改 hpc-redis && git add Mooncake <改动文件>   # 记录新指针
3. git commit   # 该 commit 自此永远指向配套的 Mooncake 版本
```

检出任意历史 hpc-redis commit 后 `git submodule update` 即得到当时
兼容的 Mooncake（不多不少）。注：指针是 commit SHA 不是分支名，
回溯到 submodule 注册（beea9a3）之前的 commit 无 Mooncake 钉扎。

## 写路径开销的测量学备注

prepare/fill 微基准（8MB/45ms）在本机存在 ±20% 的时段性波动（共租干扰；
-800 故障阈值的历史漂移同源）。"写路径 -18~24%" 仅在部分时间窗稳定出现：
- flip 机制固有开销上界 = 数个百分点（best case flip-on≈flip-off≈187 MiB/s）
- 矩阵窗口内的 gap 为环境+访存放大的混合，本机无法进一步归因
- 写路径出正式数字需：独占机器 或 nr-objects ≥100k 的长跑；小样本不可用

## 跨节点 TE 基线调优结论

TE（连接池+16lane）跨节点天花板 ≈ 152-157 MiB/s（p50 ~0.8ms）。
- TCP 缓冲 16MB: +3.4%（4KB 小消息不吃 BDP）
- 巨帧 MTU 9000: 无效（该负载形态延迟受限, 非带宽受限）
- 实验后已恢复 MTU 1500 / 原缓冲值
最终校准: 跨节点 KVC 直读+位置缓存 405 MiB/s vs TE 最强 157 ≈ 2.6×。
