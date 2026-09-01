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
- 远端访问必须 nc/O_SYNC 映射 + 流水拷贝（`kvc_copy_remote`，glibc memcpy
  在 nc 内存上不流水，单线程差 30 倍）；decoder_flag 必须 0x63（见 core/ub/README）。
- **测试环境前提**（否则数据失真）：
  - `sysctl tcp_tw_reuse=1` + 宽端口范围 —— TE TCP one-shot 连接在默认
    sysctl 下有端口压力，会同时压低 TE 基线并诱发传输异常；
  - master `-default_kv_lease_ttl` 按测试时长调大（默认 10s）。
- 内存序契约：写端 release 翻 READY、读端 acquire 查状态（SNP 可见性顺序），
  详见 kvc_common.h。

## 实测基线（store_kv_bench, 4KB block, 同机, sysctl 校准后）

| 配置 | MiB/s | p50 | vs TE |
|---|---:|---:|---|
| TE（原生） | 109 | 0.85 ms | 1× |
| + 直读 | 850 | 0.14 ms | 7.8× |
| + 直读+位置缓存 | 1391 | 0.084 ms | 12.8× |

跨节点（HW02→HW01, nc 直读）：447 MiB/s / p50 0.28ms。
注：早前 91×/143× 的基线是端口压力下的失真 TE（9.9 MiB/s），上表为修复后口径。
跨节点直读存在 2 次未定位间歇性内容不符（均发生在端口压力窗口内，修复后
13+ 次同模式不复现），默认 env 关闭，详见 core/ub/README。

## 测量口径说明（重要，防误读）

- store_kv_bench 的 MiB/s = 应用侧有效字节（kv/s × value_size），非线速。
- 同机直读 1416 MiB/s 的工作集仅 8MB（L3 常驻）——反映的是"移除传输栈"
  加 L3 命中，不代表 DRAM 带宽。
- SDK 层单线程物理上限（tools/kvc_rw_bw 实测，512MB 工作集 >L3，
  全量逐字节校验 bad=0）：直读 1731 MB/s / 直写 8452 MB/s。
- 143× 的对比基线是"TE-TCP 回环 + python 单车道"部署形态；对标 Mooncake
  生产形态（RDMA + C++）比值会显著缩小。正确表述：移除了传输栈开销。

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
