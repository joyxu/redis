# FAQ

## 硬件相关

### __问题__

HPC-Redis 支持哪些处理器架构？能否在 x86_64 平台运行？

### __回答__

HPC-Redis 仅支持 aarch64 架构，且依赖鲲鹏处理器的 SVE / SVE2 指令集与 UB.MEM 设备。已验证处理器型号为鲲鹏 920 / 鲲鹏 930。

x86_64 平台不在支持范围内：源码中向量搬运与相似度计算使用 `svld1_f32` 等 SVE 指令，在 x86_64 上无法编译；同时 UB.MEM 设备（`/dev/obmm_shmdev*`）是鲲鹏平台专属硬件。

### __问题__

BIOS 中需要开启哪些选项才能使用 SVE 与 UB.MEM？

### __回答__

- **SVE / SVE2**：鲲鹏 920 / 930 默认支持 SVE 256-bit，无需 BIOS 特殊配置。可用 `/proc/cpuinfo` 中 `Features` 字段确认是否包含 `sve`。
- **UB.MEM**：需要在 BIOS 中开启相应的大内存映射 / SNP（Scalable Node Platform）选项；UB.MEM 设备节点的创建与初始化由 `obmmctl` 工具完成，详细操作请联系系统管理员。

### __问题__

如何确认当前环境 UB.MEM 设备已就绪？

### __回答__

```bash
ls -l /dev/obmm_shmdev*
```

预期输出包含例如 `/dev/obmm_shmdev1` 等字符设备节点。若设备不存在或权限不足，请联系系统管理员通过 `obmmctl` 初始化。

## License / UB.MEM 设备相关

### __问题__

HPC-Redis 是否需要 License？

### __回答__

HPC-Redis 本身是开源软件（基于 Redis 协议），无 License 限制。但 UB.MEM 设备的能力依赖鲲鹏平台硬件许可与 BIOS 配置，这部分由平台运维团队负责，不在 HPC-Redis 文档范围内。

### __问题__

热区（warm region）映射的内存是否计入 `redis-server` 的 RSS？

### __回答__

不计入。UB.MEM 热区通过 pfn-map 映射，`/proc/<pid>/status` 中的 `VmRSS` / `VmHWM` / `VmPSS` 均不包含热区内存。

对比 baseline 时需显式声明口径（"含热区" / "不含热区"）。若需观察热区实际占用，请通过 `obmmctl` 工具或 manifest 中的 `bytes` 字段计算。

### __问题__

能否修改 UB.MEM 设备节点的权限、reset 设备或重启关联 daemon？

### __回答__

**不建议自行操作**。UB.MEM 设备的 reset / 权限修改 / daemon 重启属于平台运维操作，误操作可能导致热区数据丢失或跨节点 SNP 状态不一致。探查热区状态必须用只读手段（`/proc/<pid>/smaps`、server 日志、`lsmod`）。

## 安装相关

### __问题__

编译 `redis-server` 报错 `error: unknown type name 'svbool_t'` 或 `'svfloat32_t'`。

### __问题现象描述__

执行 `make -C src -j$(nproc) redis-server` 时报 SVE 类型未定义错误。

### __关键过程、根本原因分析__

SVE 类型（`svbool_t` / `svfloat32_t` 等）由编译器的 SVE 头文件提供，需要 `-march=armv8.2-a+sve` 及以上编译选项。默认情况下 `src/Makefile` 已带此选项；若用户自定义 `CFLAGS` 覆盖了 `FINAL_CFLAGS`，或使用低于 10.3 版本的 GCC，可能导致 SVE 支持被剥离。

### __结论、解决方案及效果__

1. 确认 GCC 版本 ≥ 10.3。

   ```bash
   gcc --version
   ```

2. 检查 `src/Makefile` 的 `FINAL_CFLAGS` 是否包含 `-march=armv8.2-a+sve -DUSE_ARM_SVE`。

   ```bash
   grep 'FINAL_CFLAGS += -march' src/Makefile
   ```

   预期输出：

   ```
   FINAL_CFLAGS += -march=armv8.2-a+sve
   FINAL_CFLAGS += -DUSE_ARM_SVE
   ```

3. 若确认 Makefile 正确但仍报错，清理后重新全量编译。

   ```bash
   make -C src distclean
   make -C src -j$(nproc) redis-server
   ```

### __问题__

编译客户端 SDK 时找不到 `vemb_v16_client_sdk.h` 头文件。

### __问题现象描述__

业务代码 `#include "vemb_v16_client_sdk.h"` 时报 `fatal error: vemb_v16_client_sdk.h: No such file or directory`。

### __关键过程、根本原因分析__

SDK 头文件在构建后被扁平化到 `clients/c/build/include/` 目录。若未先执行 `make` 构建 SDK，或业务代码 `-I` 路径指向了错误的目录（如 SDK 源码目录 `clients/c/` 而非 `clients/c/build/include/`），会出现此错误。

### __结论、解决方案及效果__

1. 确认 SDK 已构建。

   ```bash
   ls clients/c/build/include/vemb_v16_client_sdk.h
   ```

2. 若文件不存在，执行 SDK 构建。

   ```bash
   cd clients/c && make && cd ..
   ```

3. 业务代码编译时使用扁平化头文件路径。

   ```bash
   # 注意：必须加 -I src 才能解析 SDK 头文件中的 xxhash 相对路径
   gcc -I clients/c/build/include -I src my_app.c clients/c/build/libvemb_v16_client.a -o my_app -lpthread
   ```


## 验证相关

### __问题__

压测客户端报 `Connection reset by peer` 或 `connect: Cannot assign requested address`。

### __问题现象描述__

memtier 或 SDK 程序在高并发连接时大量报连接错误。

### __关键过程、根本原因分析__

可能原因：

- 服务端 `maxclients` 配置过低，超出上限的连接被拒绝。
- 客户端 ephemeral port 耗尽（`TIME_WAIT` 堆积）。

### __结论、解决方案及效果__

1. 检查 server 日志是否报 `maxclients reached`（路径以启动时 `--logfile` 指定为准）。

   ```bash
   grep -i maxclients /tmp/redis-6379.log
   ```

2. 若是 `maxclients` 限制，调高配置：

   ```bash
   ./output/src/redis-cli -p 6379 CONFIG SET maxclients 10000
   ```

3. 若是客户端 ephemeral port 耗尽，调整内核参数：

   ```bash
   sysctl -w net.ipv4.ip_local_port_range="1024 65535"
   sysctl -w net.ipv4.tcp_tw_reuse=1
   ```


### __问题__

VEMB 读取向量返回空或 `not found`，但 VADD 明明已成功。

### __问题现象描述__

VADD 返回 OK，但紧接的 VEMB 读回 `not found` 或空向量。

### __关键过程、根本原因分析__

可能原因：

- 客户端 SDK 的 `dim` 参数与服务端 `--vemb-v16-dim` 不一致，导致帧解析偏移错误。
- 多端点路由下，VADD 与 VEMB 命中了不同的后端节点（SDK 按一致性哈希分流，相同 set+elem 不会路由到不同节点；若 manifest 配置不一致则可能发生）。
- 热区 manifest 未正确加载（`--vemb-v16-reset-warm-regions yes` 未设，残留旧数据）。

### __结论、解决方案及效果__

1. 确认客户端 SDK `dim` 与服务端 `--vemb-v16-dim` 完全一致。

2. 确认所有节点使用一致的 manifest（HW01 用 `111.yaml`、HW02 用 `112.yaml`，但 region_id 映射需对应）。

3. 首次部署或切换拓扑时，加 `--vemb-v16-reset-warm-regions yes` 重置热区。

4. 若问题持续，开启 server 端 `loglevel debug` 查看嗅探路径诊断信息。

### __问题__

压测过程中 `redis-server` 被 OOM Killer 杀死或进程消失。

### __问题现象描述__

长时间压测后 `redis-server` 进程消失，`dmesg` 中有 `Out of memory: Killed process` 记录。

### __关键过程、根本原因分析__

可能原因：

- `maxmemory` 配置过低，Redis 主进程的常规 key space 撑爆内存上限。
- UB.MEM 热区映射失败导致 fallback 到本地 DRAM，叠加常规 key space 超出物理内存。
- 业务侧写入了大量 RESP key（非 VEMB 向量），占用了热区之外的内存。

### __结论、解决方案及效果__

1. 检查 server 日志与 `dmesg` 确认 OOM（日志路径以启动时 `--logfile` 指定为准）。

   ```bash
   dmesg -T | grep -i 'killed process'
   tail -100 /tmp/redis-6379.log
   ```

2. 若 `maxmemory` 配置过低，按物理内存调高（或关闭上限 `maxmemory 0`，仅在监控充分的场景使用）。

   ```bash
   ./output/src/redis-cli -p 6379 CONFIG SET maxmemory 0
   ```

3. 确认 UB.MEM 设备状态正常（`ls /dev/obmm_shmdev*`），避免热区 fallback。

4. 若业务侧确实需要写大量 RESP key，请评估物理内存是否足够，并考虑引入冷存储分层。

---

