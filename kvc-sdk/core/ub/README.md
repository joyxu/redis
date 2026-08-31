# core/ub — UB 内存访问层

region 物理层：shmdev 的 open/mmap/close + O_SYNC fallback。

## 积木来源（实现阶段从 src/ 抽取）

| 文件 | 耦合度 | 用途 |
|---|---|---|
| `src/urma.h` / `src/urma.c` | 1（仅 libc + pthread） | 单边读写抽象（ctx/segment/jetty），SDK 内部可选用 |
| shmdev 直开路径 | 新写（~100 行） | `open(device, O_RDWR)` → EPERM 则 `O_RDWR|O_SYNC` → `mmap(MAP_SHARED)`；close 时 flush 对字符设备忽略 EINVAL |

## 实测依据

- 本地 shmdev（cached）：读 15GB/s / 写 25GB/s，与文件 mmap 无差异（零带宽税）
- 远端 shmdev（O_SYNC→noncacheable）：读 67MB/s（逐 line 往返）、写 5.8GB/s（posted）
  —— 见 `Mooncake/benchmarks/storage_benchmark_v1/ub_shmdev_bench.py` 的 HW01/HW02 实测
- **Gate**：远端读要可用必须 cachable/SNP import（当前 noncacheable import 被内核拒绝
  cachable 映射）。此为 SDK 读路径性能成立的前置条件。

## 注意

- `src/vemb_v16_ub_bridge.{h,c}`（libubme 动态 region 管理）**已不在仓库**。
  动态 export/import 管理属 P1+，届时直接对接 `/root/jxw/ubme` 的 libubme 或 obmmctl。
- P0 的 region 参数（device_path/mmap_offset）由接入方静态给出（manifest 方式，
  与 hpc-redis `examples/vemb_v16_warm_regions_*.yaml` 同构）。

## ⚠️ 平台硬约束（HW01/HW02 实测 + 维护者确认）

- **设备归属**：HW01 视角 shmdev **1-4、9-12 = 本地内存**；**5-8 = HW02 内存的 import**。
  HW02 对称（1-4/9-12 本地，5-8 = HW01 内存）。
- **decoder_flag 必须为 0x63**（ub_node_*.cfg 现值）。**严禁改用 0x60** —— 会导致 UB
  远端读出现一致性问题（数据错误，非性能问题）。memattr=nc + O_SYNC 是远端访问
  的正确且唯一安全配置。
- 远端 nc 映射的读性能：单线程 memcpy ≈ 67MB/s（逐 cache line 往返）；
  跨节点读优化方向是多线程并行（每线程独立 load 流水），不是改 flag。
- 本地 shmdev（cached）：读 15GB/s / 写 25GB/s，与文件 mmap 无差异。

## ⚠️ 已知间歇性问题（P2b 期间发现，未定位）

现象：跨节点直读（nc 映射）在特定时序下 verify 16/16 内容不符（两次复现，
均在"read_perf 大流量 nc 读之后紧跟 verify_write"的场景），随后同组合连续
8 次通过。已排除：写路径（probe 本地数据正确）、位置缓存（verify_write
中每 key 仅读一次，缓存全程 miss）。

怀疑方向：nc 读与段 owner（daemon）cache 脏行的 snoop 竞态窗口 —— 与
"decoder_flag 必须 0x63"同族的底层一致性问题，需平台侧确认。

处置：跨节点直读保持 env 门控默认关闭（KVC_DIRECT_READ 不设即走 TE，
TE 路径始终正确）；同机直读不受影响（本地 cache 一致性由 CPU 保证，
多次 verify 全过）。后续调试抓手：给直读加内容 checksum 双检选项、
复现时 dump 不匹配字节与 slot 元数据。
