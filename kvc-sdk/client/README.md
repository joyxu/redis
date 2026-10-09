# client/ — libkvc_client 实现占位

实现 `include/kvc/kvc_client.h`：

- `kvc_region_set_*`：region_id → region 注册表（P0 简单数组/哈希）
- `kvc_read_handle` / `kvc_deref_handle`：查表 → `kvc_slot_query` 就绪检查 →
  memcpy / 返回指针

依赖 core/{ub, layout}。P1 增加 `kvc_batch_get_handle`、
`kvc_write_slot_direct`（写端直写）、attach 内置。
