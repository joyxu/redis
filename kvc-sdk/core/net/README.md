# core/net — TCP 帧收发与 attach 握手

## 积木来源（耦合度全为 1，直接复制）

| 文件 | 用途 |
|---|---|
| `src/vemb_v16_net.h` / `.c` | connect/listen/read_full/write_full/write_frame（任意 payload 长度前缀帧） |
| `src/vemb_v16_aeron_attach.h` 的 wire 结构 | attach 握手报文定义（client 向 server 要 shmdev_path + ring offsets） |

## 在 SDK 中的角色（P0/P1）

- P0: 仅作为 `kvc_region_open_remote` 的参数获取途径（接入方借 attach 拿
  device_path/mmap_offset 后传给 cfg）—— SDK 不强制依赖
- P1: 完整 attach 流程内置（`kvc_attach(host, port, region_id, cfg_out)`）
- P1: slot 状态服务化（server worker 池的信令走 core/ring，跨节点时 ring
  参数的获取走本模块握手）

## 抽取方式

直接复制两个 .h 一个 .c；`vemb_v16_net.c` 只依赖 libc sockets，
include 链上的 `vemb_v16_protocol.h` 用 `clients/c/internal/macro.h`
shadow 模式隔离（照抄现有 client SDK Makefile 的 -I 顺序）。
