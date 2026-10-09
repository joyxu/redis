# core/ring — SPSC 无锁环形队列

## 积木来源

| 文件 | 耦合度 | 说明 |
|---|---|---|
| `src/vemb_v16_aeron_ring.h` | 1（header-only，零依赖） | head/tail 分 cacheline、slot_size/slot_count 参数化（count 为 2 的幂）、publish/poll 单个与批量、自适应退避阻塞版 |

## 在 SDK 中的角色

P0 不用（无 worker 池）。P1 起：

1. server worker 池的交接队列（对标 Mooncake MemcpyWorkerPool 的 mutex+condvar
   替换，分片 SPSC 消锁竞争）
2. slot 状态服务化后的控制信令通道（跨节点时承载于 shmdev，本地走 UDS/共享内存）

## 注意

- slot 大小是参数不是硬编码，但需核查 KB→MB 级 slot 下的批量发布计数与
  batch 接口假设（hpc-redis 用途是 KB 级向量请求）
- 与 layout 模块在同一 shmdev 共存时用 mmap_offset 错开
