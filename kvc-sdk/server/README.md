# server/ — libkvc_server 实现占位

实现 `include/kvc/kvc_server.h`：

- region 生命周期（open_local/open_remote/close/meta）→ core/ub + core/layout
- slot 分配器（纯位图，master 可用，不依赖 mmap）→ core/layout
- slot 状态机（mark/query，release/acquire 原子）→ core/layout
- `kvc_region_slot_addr` → 地址换算

**链接方**：
- mooncake_client 进程（持段方）
- mooncake_master 进程（仅链 slot 分配器符号，作 TlcSlotAllocator 底座）

P1 增加：worker 池（core/ring）、`kvc_slot_scan_state` 崩溃恢复扫描。
