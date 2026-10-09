# optional/topo — 一致性哈希路由（P3，最后才考虑）

**默认不实现、不编译。** 仅当需要"无 master 独立组集群"形态（SDK 自举路由：
client 本地 ring_owner(key_hash) → owner endpoint 直连）时启用。

若接入 Mooncake（主线），placement 由 master 唯一决定，本模块**必须不参与**，
否则两套路由权威冲突。

## 积木来源（届时抽取）

- `src/vemb_v16_topology.h` / `.c`（耦合度 1）：ring_build / ring_owner /
  build_expand_plan + epoch 版本
- `src/vemb_v16_hash.h` + `deps/xxhash/`

启用条件（满足其一再排期）：
1. 出现明确的无 master 部署需求（demo/benchmark 多节点自组网）
2. 主线（Mooncake 适配）P0-P2 全部完成
