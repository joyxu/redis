# VEMB V16 Dim 配置与变更

本文说明 VEMB V16 在“一个 server 只使用一个固定 dim”的前提下，如何选择、部署和变更向量维度。

## 设计边界

- `VEMB_V16_MAX_DIM` 是编译期上限，不是当前 server 的实际维度。
- `vector_dim` 是启动时选择的实际维度，贯穿 proxy、storage、TLC、channel 和 manifest。
- 同一个 server/proxy 不支持同时使用多个 dim。
- server 启动后不支持在线修改 dim；修改必须重启并重新准备向量 backing。
- 客户端、server 和所有 Aeron/TCP endpoint 必须使用相同的实际 dim。

协议中的请求 `dim` 和 channel descriptor 中的 `vector_dim` 必须保留，用于握手阶段检测客户端/server 配置不一致。

正常请求的 dim 与 server 配置一致；不一致属于异常输入。握手、channel 分配、SDK descriptor、SuperNode shape 和 storage manifest 的拒绝分支使用 `unlikely` 或 `RETURN_IF`，不会把异常 dim 判断作为正常数据路径的预测分支。

## 编译期上限

默认上限由 `VEMB_V16_MAX_DIM` 提供。需要提高上限时，在构建所有相关组件时统一传入，例如：

```sh
make -C src CFLAGS="-DVEMB_V16_MAX_DIM=8192"
make -C clients/c CFLAGS="-DVEMB_V16_MAX_DIM=8192"
make -C benchmark CFLAGS="-DVEMB_V16_MAX_DIM=8192"
make -C memtier_benchmark CPPFLAGS="-DVEMB_V16_MAX_DIM=8192"
```

实际构建入口以各目录的 Makefile 为准。server、redis-cli、SDK、memtier 和 benchmark 不应混用不同 `VEMB_V16_MAX_DIM` 构建产物。

`VEMB_V16_MAX_DIM` 必须覆盖目标 dim，并且不能超过协议 `dim` 字段和实现允许的上限。只修改实际 dim、且目标值没有超过当前编译上限时，不需要重新编译。

## 部署一个新的 dim

以下示例使用 `N=768`。

### 1. 准备 manifest 和 backing

所有向量 region 的单条 payload 大小必须为：

```text
value_size = N * sizeof(float) = 3072
```

同时应满足：

```text
region_bytes >= value_size
region_bytes % value_size == 0
```

如果 manifest 显式指定了 `value_size`，每个 region 都必须与 `N * 4` 一致。不要直接复用按旧 dim 创建的 backing；旧 backing 中的向量不能按新 dim 解释。

### 2. 停止旧 server

停止所有使用旧 dim 的 server、proxy、Aeron channel 和 client。不要在旧 dataplane 运行期间修改 manifest 或 backing。

### 3. 清空或重新创建 warm region

根据部署方式重新分配 backing，或使用 reset 选项清理 allocator/slot metadata。改变 dim 后建议使用全新的 region 文件或 UB 区域，并重新写入数据；仅重置 allocator 不会把旧的 300 维 payload 转换成 768 维 payload。

### 4. 使用新 dim 启动 server

standalone server：

```sh
./src/vemb_v16_server \
  --dim 768 \
  --warm-regions-manifest /path/to/regions-768.yaml \
  --reset-warm-regions
```

Redis server：

```text
vemb-v16-enabled yes
vemb-v16-dim 768
vemb-v16-warm-regions-manifest /path/to/regions-768.yaml
vemb-v16-reset-warm-regions yes
```

`vemb-v16-dim` 是启动配置。server 初始化完成后不应通过 `CONFIG SET` 修改；如需变更，重复完整的停止、重建 backing、重启流程。

### 5. 使用相同 dim 启动 client

redis-cli：

```sh
./src/redis-cli --vemb-v16-dim 768 VEMB myset elem1
```

memtier/SDK/benchmark 也必须显式使用 `768`。多 endpoint client 的所有 endpoint 必须配置为相同 dim。

## 不一致时的预期行为

客户端 dim 与 server dim 不一致时，应在以下阶段直接失败：

- TCP HELLO/WELCOME
- framed Aeron channel allocation
- UDS Aeron allocation
- Aeron raw v1/v2 ATTACH
- SDK channel descriptor 校验

不应先创建 channel，再让请求在 SuperNode 中以 `vector_bytes` 或 job shape 错误失败。

## 验证清单

每次新增或变更 dim，至少验证：

1. server 启动日志中的 `dim` 与目标值一致。
2. manifest 所有 region 的 `value_size == dim * 4`。
3. TCP、UDS Aeron、raw Aeron v1/v2 均能完成握手。
4. `VADD`、`VEMB`、`VSIM`、`VREM` 使用目标维度的数据完成读写。
5. 使用错误 dim 的 client 在握手阶段失败。
6. 重启后数据仍按新 dim 正确读取。

建议测试维度至少覆盖 `1`、`8`、`300`、`768` 和 `VEMB_V16_MAX_DIM`。

## 回滚

回滚到旧 dim 也必须停止 server，恢复旧 manifest/backing，并使用旧 dim 重启。新旧 dim 的向量 payload 不兼容；如果旧 backing 已被覆盖，应从备份恢复数据。
