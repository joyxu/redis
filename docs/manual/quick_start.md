# 快速入门

## 简介

本章节通过一个最小化的端到端流程，帮助用户在 快速完成 HPC-Redis 的启动与向量操作验证。涵盖两个验证路径：

- **路径 A（RESP 路径）**：使用 `redis-cli` 内置的 VEMB V16 兼容路径发送 `VADD` / `VEMB` / `VSIM` 命令。
- **路径 B（SDK 路径）**：使用客户端 SDK 编写一个 C 程序，通过 VEMB V16 二进制协议完成向量写入与读取。

两条路径均连接到同一个集成版 `redis-server`。

## 前提条件

- 已按 [安装指南](installation_guide.md) 完成 `redis-server` 与客户端 SDK 的编译。
- 当前用户对 `src/redis-server`、`output/src/redis-cli`、`clients/c/build/libvemb_v16_client.a` 有执行 / 读取权限。
- 已准备好热区 manifest 文件 `examples/vemb_v16_warm_regions_111.yaml`（仓库自带）。如需切换为远端 UB 内存场景，使用 `examples/vemb_v16_warm_regions_111_remote.yaml`。
- UB.MEM 设备 `/dev/obmm_shmdev*` 已通过 `obmmctl` 初始化（详见 [安装后检查](installation_guide.md#安装后检查)）。
- 本节示例在单机本地回环（`127.0.0.1:6379`）上演示；跨节点部署请参考 [最佳实践](best_practices.md)。

## 启动 HPC-Redis 服务

1. 启动集成版 `redis-server`（启用 VEMB V16，绑定 NUMA 0，server 占用核心 0-95）。

   ```bash
   numactl -N 0 -l taskset -c 0-95 ./src/redis-server \
     --port 6379 \
     --bind 0.0.0.0 --protected-mode no \
     --vemb-v16-enabled yes \
     --vemb-v16-dim 300 \
     --vemb-v16-max-vectors 131072 \
     --vemb-v16-warm-regions-manifest ./examples/vemb_v16_warm_regions_111.yaml \
     --vemb-v16-reset-warm-regions yes \
     --vemb-v16-proxy-io-threads 32 \
     --vemb-v16-supernode-workers 64 \
     --daemonize yes \
     --loglevel notice
   ```

2. 等待端口监听就绪。

   ```bash
   for i in $(seq 1 50); do
       ss -tln | grep -q ':6379 ' && break
       sleep 0.2
   done
   ss -tln | grep ':6379 '
   ```

   预期输出：

   ```
   LISTEN 0 4096 0.0.0.0:6379 0.0.0.0:*
   ```

3. 用 `redis-cli` 验证 RESP 路径可用。

   ```bash
   ./output/src/redis-cli -p 6379 PING
   ```

   预期输出：

   ```
   PONG
   ```

## 使用 redis-cli 快速验证向量操作

`redis-cli` 内置 VEMB V16 快速路径：当命令行带 `--vemb-v16-dim` 参数时，`VADD` / `VEMB` / `VSIM` / `VREM` 命令会自动走 VEMB V16 二进制协议（而非 RESP 文本协议）。

1. 写入向量（VADD）。

   ```bash
   # --vemb-v16-dim 300 触发 VEMB V16 快速路径
   # 标准格式：VADD <set> VALUES <dim> <v1> <v2> ... <vN> <element>
   ./output/src/redis-cli -p 6379 --vemb-v16-dim 300 \
       VADD myset VALUES 300 $(seq 1 300) elem1
   ```

   预期输出：

   ```
   OK
   ```

   > **格式说明**：VADD 在 VEMB V16 快速路径下支持三种写法：
   > - **标准格式**（推荐）：`VADD <set> VALUES <dim> <v1> ... <vN> <element>`
   > - **简化格式**：`VADD <set> <element> "<v1>,<v2>,...,<vN>"`（向量作为单一 argv，可用空格或逗号分隔）
   > - **FP32 格式**：未在快速路径实现，会自动 fallback 到 RESP

2. 读取向量（VEMB）。

   ```bash
   ./output/src/redis-cli -p 6379 --vemb-v16-dim 300 \
       VEMB myset elem1
   ```

   预期输出（向量按每行一个浮点输出）：

   ```
   1.000000
   2.000000
   3.000000
   ...
   300.000000
   ```

3. 计算相似度（VSIM）。

   ```bash
   # 向量以单 argv 形式传入（用引号包裹让 shell 不拆分）
   ./output/src/redis-cli -p 6379 --vemb-v16-dim 300 \
       VSIM myset elem1 "$(seq 1 300 | tr '\n' ' ')"
   ```

   预期输出（与自身相似度为 1）：

   ```
   1.000000
   ```

4. 删除向量（VREM）。

   ```bash
   ./output/src/redis-cli -p 6379 --vemb-v16-dim 300 \
       VREM myset elem1
   ```

   预期输出（返回整数 `0` 表示成功删除一个元素）：

   ```
   0
   ```


## 使用客户端 SDK 验证完整链路

下面通过 SDK 编写一个最简的 C 程序，依次完成 VADD → VEMB → VSIM 全流程，验证 VEMB V16 二进制协议端到端可用。

1. 创建示例源文件 `quick_start_demo.c`。

   ```c
   #include "vemb_v16_client_sdk.h"
   #include <stdio.h>

   #define DIM 300

   int main(void)
   {
       /* 1. 配置 TCP bootstrap seed；数据 owner 由 topology 决定 */
       const char *seeds[] = {"127.0.0.1:6379"};
       vemb_v16_client_t *c = vemb_v16_client_create(seeds, 1, DIM, 0);
       if (!c) { fprintf(stderr, "connect failed\n"); return 1; }

       /* 2. 构造一个待写入的向量 */
       float vec[DIM];
       for (uint32_t i = 0; i < DIM; i++) vec[i] = (float)(i + 1);

       /* 3. VADD 写入 */
       if (vemb_v16_client_vadd(c, "myset", "elem1", vec, DIM) == 0) {
           printf("VADD OK\n");
       } else {
           fprintf(stderr, "VADD FAIL\n");
       }

       /* 4. VEMB 读回 */
       float out[DIM];
       uint32_t out_dim = 0;
       int rc = vemb_v16_client_vemb_vector(c, "myset", "elem1",
                                            out, DIM, &out_dim);
       if (rc == 0)      printf("VEMB OK, dim=%u, first=%f\n", out_dim, out[0]);
       else if (rc == 1) printf("VEMB not found\n");
       else              printf("VEMB error\n");

       /* 5. VSIM 相似度 */
       float score = 0.0f;
       rc = vemb_v16_client_vsim(c, "myset", "elem1", vec, DIM, &score);
       if (rc == 0) printf("VSIM score=%.4f\n", score);

       /* 6. 释放连接 */
       vemb_v16_client_destroy(c);
       return 0;
   }
   ```

2. 编译并运行。

   ```bash
   # 注意：必须加 -I src 才能解析 SDK 头文件中的 xxhash 相对路径
   gcc -I clients/c/build/include -I src \
       quick_start_demo.c \
       clients/c/build/libvemb_v16_client.a \
       -o quick_start_demo -lpthread

   ./quick_start_demo
   ```

   预期输出：

   ```
   VADD OK
   VEMB OK, dim=300, first=1.000000
   VSIM score=1.0000
   ```

3. 清理测试数据并关闭服务（可选）。

   ```bash
   ./output/src/redis-cli -p 6379 VREM myset elem1
   ./output/src/redis-cli -p 6379 SHUTDOWN NOSAVE
   ```

## 后续步骤

- 完整配置项与运维方法见 [使用手册](user_guide.md)。
- 性能压测、跨节点部署、SVE 与 memcpy 对比等进阶场景见 [最佳实践](best_practices.md)。
- 编译失败、连接异常、吞吐不达预期等问题见 [FAQ](faq.md)。

---
