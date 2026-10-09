# core/layout — region 物理布局与 slot 表

region header + 定长 slot 状态表的内存布局，是 `kvc_region_*` / `kvc_slot_*`
接口的实现底座。

## 积木来源

| 文件 | 耦合度 | 复用点 |
|---|---|---|
| `src/vemb_v16_shared_allocator.h` | 1 | **单 cacheline（64B）header 模式**：魔数/版本/region_id/capacity/value_size/used 一行装下，`_Static_assert(sizeof==64)`；跨进程共享的分配头 |

## 布局设计（P0 落地时实现）

```
region 基址 (mmap_offset 起)
┌────────────────────────┐  offset 0
│ region header (64B)     │  魔数 | 版本 | region_id | capacity_slots
│                         │  block_size | used | 保留
├────────────────────────┤  offset 64
│ slot[0] 元数据 (32B)    │  state | owner_gen | write_seq | key_hash
│ slot[1] 元数据 ...      │  （元数据与数据分离，扫状态不触碰数据行）
│ ...                     │
├────────────────────────┤  data_off（对齐到 block_size）
│ slot[0] 数据 block_size │
│ slot[1] 数据 ...        │
└────────────────────────┘
```

- slot_idx → 数据地址 = data_off + slot_idx × block_size
- handle.offset（master 分配的段内 offset）= data_off + slot_idx × block_size，
  即 master 视角的 offset 与 slot 一一对应（TlcSlotAllocator 语义）
- 状态翻转用 release/acquire 原子（SNP 可见性顺序契约见 kvc_common.h）
