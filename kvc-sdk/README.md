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

与 Mooncake 的接缝（P0 主线）：

| Mooncake 触点 | SDK 提供 |
|---|---|
| MountSegment（挂段） | `kvc_region_open_local` + `kvc_region_meta` |
| PutStart 分配 offset（master） | `kvc_slot_allocator_alloc`（offset = slot_idx × block_size） |
| PutEnd（写完可见） | `kvc_slot_mark_ready`（release 语义 + write_seq） |
| GetReplicaList → 读 | `kvc_deref_handle`（同 UB 域 load 指令零拷贝） |

## 接口优先级

- **P0（当前）**：region 生命周期 / slot 分配器 / slot 状态机 / read+defer。
  写路径数据搬运走 Mooncake TE（protocol="ub"）。`include/kvc/` 三个头文件即全部 P0 接口。
- **P1**：batch_get_handle、write_slot_direct（绕 TE 直写）、slot_scan_state
  （崩溃恢复：半写 slot 识别回收）、server worker 池（core/ring）、stats。
- **P2**：多 size class region_set 完善、EVICTING 态（驱逐/读并发）、examples 完整化。
- **P3（沉底，默认不做）**：optional/topo 内置路由（无 master 自组网）、
  变长 blob、控制面 ring 化。

## 构建

```bash
make check    # P0: 公共头自包含性检查（当前唯一目标）
make clean
# 实现落地后: 产出 build/libkvc_{client,server}.{so,a}（Makefile 中已留注释模板）
```

编译口径与 clients/c/Makefile 对齐（aarch64 加 `-DUSE_ARM_SVE -march=armv8.2-a+sve`），
仅依赖 `-lpthread -lm`。

## 目录

```
include/kvc/   公共 API（kvc_common / kvc_server / kvc_client）
core/          从 src/ 抽取的积木（各 README 记录来源与耦合度）
  ub/          shmdev mmap + O_SYNC fallback（实测依据见 README）
  layout/      region header + slot 表布局（shared_allocator 64B header 模式）
  ring/        SPSC 环（P1 worker 池用）
  net/         TCP 帧 + attach 握手报文
optional/topo/ 一致性哈希路由（P3，启用条件见 README）
client/ server/  实现占位
examples/      参考进程 / demo 骨架
tests/         headers_check（P0 验收）
```

## 关键约束与前置

- 一个 region 一个 block_size（= 目标模型 KV page，如 glm5 MLA 7MB）；
  混合层组模型用多 region 分 size class。
- **远端读的 Gate**：cachable/SNP import —— 当前 noncacheable import 实测
  读仅 67MB/s（逐 cache line 往返），必须重建 cachable import 后远端读路径才成立。
  （实测脚本：`Mooncake/benchmarks/storage_benchmark_v1/ub_shmdev_bench.py`）
- 内存序契约：写端 release 翻 READY、读端 acquire 查状态（SNP 可见性顺序），
  详见 kvc_common.h。
