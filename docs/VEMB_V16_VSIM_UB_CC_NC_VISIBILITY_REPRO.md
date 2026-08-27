# VEMB v16 VSIM UB CC/NC 可见性问题最小复现

## 1. 背景

VEMB v16 双节点 VSIM 场景中，VADD 在本地 UB region 写入 vector 和
remote meta，另一个节点通过远端 UB mapping 查询 remote meta 并读取
vector。

实际测试中，VADD 成功，但跨节点 VSIM 的 key2 remote-meta lookup 返回
`NOT_FOUND`。为了排除 VSIM、hash、owner 路由、atomic RMW 和数据结构等因素，
增加了一个只包含 `open`、`mmap`、64B 写入和 64B 读取的最小复现 UT。

## 2. UB 设备映射关系

当前 SSH 直接按逻辑节点登录：

| 逻辑节点 | SSH 登录 |
|---|---|
| `111` | `ssh -p 22 root@192.168.90.111` |
| `112` | `ssh -p 22 root@192.168.90.112` |

下文中的 `111`/`112` 指对应逻辑角色和 SSH 主机；相同地址用于 VEMB
数据面时仍按具体服务端口区分。

| 本地 Export 设备 | 对端 Import 设备 |
|---|---|
| 111 `/dev/obmm_shmdev1` | 112 `/dev/obmm_shmdev5` |
| 111 `/dev/obmm_shmdev2` | 112 `/dev/obmm_shmdev6` |
| 111 `/dev/obmm_shmdev3` | 112 `/dev/obmm_shmdev7` |
| 111 `/dev/obmm_shmdev4` | 112 `/dev/obmm_shmdev8` |
| 112 `/dev/obmm_shmdev1` | 111 `/dev/obmm_shmdev5` |
| 112 `/dev/obmm_shmdev2` | 111 `/dev/obmm_shmdev6` |
| 112 `/dev/obmm_shmdev3` | 111 `/dev/obmm_shmdev7` |
| 112 `/dev/obmm_shmdev4` | 111 `/dev/obmm_shmdev8` |

第二组设备使用相同的编号偏移规则：

| 本地 Export 设备 | 对端 Import 设备 |
|---|---|
| 111 `/dev/obmm_shmdev9` | 112 `/dev/obmm_shmdev13` |
| 111 `/dev/obmm_shmdev10` | 112 `/dev/obmm_shmdev14` |
| 111 `/dev/obmm_shmdev11` | 112 `/dev/obmm_shmdev15` |
| 111 `/dev/obmm_shmdev12` | 112 `/dev/obmm_shmdev16` |
| 112 `/dev/obmm_shmdev9` | 111 `/dev/obmm_shmdev13` |
| 112 `/dev/obmm_shmdev10` | 111 `/dev/obmm_shmdev14` |
| 112 `/dev/obmm_shmdev11` | 111 `/dev/obmm_shmdev15` |
| 112 `/dev/obmm_shmdev12` | 111 `/dev/obmm_shmdev16` |

当前设备配置模式为：

- 本地 Export：CC。
- 远端 Import：NC。
- `USE_CC_MODE=no`。
- 应用不调用 `obmm_set_ownership`，ownership 由 `/dev/obmm_shmdev*` 驱动管理。

`obmm_dev_mapping.jpeg` 与当前双机实测不符，不能作为当前调试依据。

## 3. 最小复现 UT

UT 源码：

```text
benchmark/ub_cc_nc_visibility_ut.c
```

构建目标已经加入：

```text
benchmark/Makefile
```

UT 有四个 line 运行模式：

### Writer

- 操作本地 Export 设备。
- 使用 `open(path, O_RDWR)`，即本地 CC mapping。
- 使用 `MAP_SHARED` mmap。
- 向指定的 64B 对齐偏移一次性写入完整 64B cacheline。
- 写入后保持 mmap 存活，等待指定时间后才 `munmap`。

### Reader

- 操作对端 Import 设备。
- 使用 `open(path, O_RDWR | O_SYNC)`，即远端 NC mapping。
- 使用 `MAP_SHARED` mmap。
- 每秒读取并校验一次指定的完整 64B cacheline。

`writer-nc`/`reader-cc` 是反向访问模式对照：writer 使用
`O_RDWR|O_SYNC(NC)`，reader 使用本地 `O_RDWR(CC)`。其中 imported 设备不保证
允许普通 `O_RDWR` 打开；若驱动返回 `EPERM`，应使用 NC reader，而不能据此判定
Export/Import 映射失败。

UT 特意不使用以下机制：

- VSIM 或 VEMB 业务代码。
- remote-meta 数据结构。
- ownership API。
- atomic RMW。
- `msync`。
- 显式 cache flush。

因此，该 UT 只验证以下访问模型：

```text
本地 CC mmap 普通 CPU store
    -> UB
    -> 远端 NC mmap 普通 CPU load
```

## 4. 构建方式

在 111 和 112 节点分别执行：

```bash
cd /root/szz/codespace/hpc-redis
make -C benchmark ub_cc_nc_visibility_ut
```

## 5. 复现方式

以下命令使用 8G region 内的 `7516192768` 偏移，只覆盖从该偏移开始的 64B。
执行前需要确认该偏移没有存放有效数据。

### 5.1 验证 111 本地 CC 写入，112 远端 NC 读取

先在 111 启动 writer：

```bash
cd /root/szz/codespace/hpc-redis/benchmark

./ub_cc_nc_visibility_ut writer \
    --path /dev/obmm_shmdev1 \
    --offset 7516192768 \
    --seed 0x1111000000000000 \
    --hold-seconds 60
```

writer 显示 `WRITER_READY` 后，在 112 启动 reader：

```bash
cd /root/szz/codespace/hpc-redis/benchmark

./ub_cc_nc_visibility_ut reader \
    --path /dev/obmm_shmdev5 \
    --offset 7516192768 \
    --seed 0x1111000000000000 \
    --watch-seconds 70
```

### 5.2 验证 112 本地 CC 写入，111 远端 NC 读取

在 112 启动 writer：

```bash
cd /root/szz/codespace/hpc-redis/benchmark

./ub_cc_nc_visibility_ut writer \
    --path /dev/obmm_shmdev1 \
    --offset 7516192768 \
    --seed 0x1122000000000000 \
    --hold-seconds 60
```

在 111 启动 reader：

```bash
cd /root/szz/codespace/hpc-redis/benchmark

./ub_cc_nc_visibility_ut reader \
    --path /dev/obmm_shmdev5 \
    --offset 7516192768 \
    --seed 0x1122000000000000 \
    --watch-seconds 70
```

其余 3 组 UB region 也应按同样方式验证：

```text
本地 /dev/obmm_shmdev2 -> 对端 /dev/obmm_shmdev6
本地 /dev/obmm_shmdev3 -> 对端 /dev/obmm_shmdev7
本地 /dev/obmm_shmdev4 -> 对端 /dev/obmm_shmdev8
```

## 6. 2026-07-09 远端实测结果

在以下真实机器上完成双向验证：

- `node0=192.168.90.111`
- `node1=192.168.90.112`

构建：

```bash
cd /root/szz/codespace/hpc-redis
make -C benchmark ub_cc_nc_visibility_ut
```

共验证 8 组方向：

- `111:1 -> 112:5`
- `111:2 -> 112:6`
- `111:3 -> 112:7`
- `111:4 -> 112:8`
- `112:1 -> 111:5`
- `112:2 -> 111:6`
- `112:3 -> 111:7`
- `112:4 -> 111:8`

结果全部通过，reader 在 `attempt=0` 即读到 `VISIBLE`。

示例输出：

```text
WRITER_READY path=/dev/obmm_shmdev1 flags=O_RDWR(CC) offset=7516192768 hold_seconds=4
written: 1111000000000000 ... 1111000000000007

READER_READY path=/dev/obmm_shmdev5 flags=O_RDWR|O_SYNC(NC) offset=7516192768 watch_seconds=8
expected: 1111000000000000 ... 1111000000000007
attempt=0 result=VISIBLE
```

## 7. 当前结论

当前这批真实双机上的有效映射关系应固定为：

- `1 -> 5`
- `2 -> 6`
- `3 -> 7`
- `4 -> 8`

2026-08-24 使用新的 64B 对齐 scratch offset 对第二组设备完成同口径双向复测：

- `111:9 -> 112:13`、`111:10 -> 112:14`、`111:11 -> 112:15`、`111:12 -> 112:16`
- `112:9 -> 111:13`、`112:10 -> 111:14`、`112:11 -> 111:15`、`112:12 -> 111:16`

八组均为本地 Export `writer`（`O_RDWR/CC`）写入、对端 Import `reader`
（`O_RDWR|O_SYNC/NC`）在 `attempt=0` 返回 `VISIBLE`。因此第二组当前实机的
映射规则可以固定为 `9 -> 13`、`10 -> 14`、`11 -> 15`、`12 -> 16`，且两个
方向都成立。

两台机器上的 imported `dev5/6/7/8` 与 `dev13/14/15/16` 分别强制使用普通
`O_RDWR(CC)`，当前驱动全部返回 `EPERM`，不是 `dev15` 特例。使用
`O_RDWR|O_SYNC(NC)` 后相应路径均可见；此前对 imported `dev15` 做的 NC writer
到 local `dev11` reader 也已通过。这是 imported 访问模式的统一限制，不是某个
设备对的物理可见性失败。后续业务路径不能把 imported path 的 `O_RDWR` 打开失败
或强制 CC 失败误报为 UB 映射错误。

并且在这次 UT 中，没有复现“必须等 `munmap` 后远端才可见”的旧现象。

## 8. 已确认的事实

1. `bench_ub_dim.sh` 已验证四组 Export/Import 映射关系，跨节点读取全部
   `verify: OK`。
2. `bench_ub_dim.sh` 的 writer 使用 `--cacheable false`，写完后立即
   `munmap`，其访问模型与 VSIM 不同。
3. 最小 UT 中，writer 使用本地 CC mapping，reader 使用远端 NC mapping。
4. writer 写入的是一条完整、64B 对齐的 cacheline。
5. 当前真实双机上，四组映射都能在 writer 持有 mmap 期间直接被远端 reader 读到。
6. 这次结果不支持“当前机器必须等 `munmap` 后才可见”的旧结论。
7. 该 UT 仍然不依赖 VSIM、remote meta、atomic RMW 或 ownership API。
8. 第二组 `dev9..dev12/dev13..dev16` 的八个双向组合也已通过同一 UT；两台机器
   上 imported `dev5..dev8` 和 `dev13..dev16` 的 CC 打开均被 `EPERM` 拒绝，
   需要由 O_SYNC fallback 处理。

## 9. 对当前扩容调试的影响

这次 UT 的意义主要是两点：

- 可以把当前双机环境的 UB 路径基线明确固定为
  `1/5, 2/6, 3/7, 4/8` 和 `9/13, 10/14, 11/15, 12/16`
- 当前扩容问题不能再归因于“peer-view path 选错成 3/4 或 11/15 这一组”；
  这两组的最小 CC/NC 可见性均已通过

## 10. 若后续仍出现可见性异常

若未来再次观测到 VSIM 或 remote-meta 可见性异常，应优先记录：

1. 具体使用的是哪一对 export/import 设备。
2. 是否仍然满足 `1/5, 2/6, 3/7, 4/8`。
3. UT 是否还能复现。
4. 是否只有业务路径异常，而最小 UT 正常。

## 11. 备注

本文已被更新为当前真实双机结果，不再保留旧的 `3/4` 映射结论。

## 12. v2 batch response 完整 frame 可见性 UT（2026-08-10）

单 cacheline 的 `writer`/`reader` 模式只能确认最终能否读到一条线，不能覆盖
v2 batch response arena 复用时的关键错误窗口：server 已发布 response
descriptor，client 也读到了当前 `batch_id`，但同一个多 cacheline frame 的某些
body line 仍是上一代数据。

为此，`ub_cc_nc_visibility_ut` 新增以下两个模式：

```text
frame-writer-nc
frame-reader-cc
```

布局及发布顺序不依赖 VEMB wire struct：

```text
data path + offset:
  [ header cacheline ][ body cachelines ][ commit cacheline ][ descriptor cacheline ]

frame[0]                 = generation
frame[1..7]              = reserved header bytes
frame[8..N-9]            = pattern(seed, generation, word-index)
frame[N-8..N-2]          = reserved commit bytes
frame[N-1]               = generation
descriptor[0]  = generation, written last
```

每一代使用反向 UB ack 防止 writer 在 client 检查前重用同一 frame：

```text
111 server                                  112 client
----------                                  ----------
NC write /dev/obmm_shmdev6   -> CC read /dev/obmm_shmdev2
CC read  /dev/obmm_shmdev3   <- NC write /dev/obmm_shmdev7
```

writer 先写完整 body（含 header/trailer），最后发布 descriptor；reader 看到下一代
descriptor 后检查 header、trailer 和全部 body word，再写 ack。故出现
`FRAME_VISIBILITY_FAILURE` 时，不可能是 writer 提前覆盖尚未消费的 frame。

UT 只使用普通 store/load 和 acquire/release fence 来约束本端程序顺序；它不调用
`dc cvac`、`dc ivac`、`dsb sy`、`msync` 或 ownership API。它检测 UB 的范围级
可见性，不把 checksum 当作修复方案。

### 12.1 真实双节点复现

选择未被运行中服务使用的 4KiB scratch 区间。以下示例沿用 `7516192768`，总共
使用 `4096 + 64` byte；启动 reader 前先确认 writer 已打印
`FRAME_WRITER_READY`。

111 上启动 server 侧 NC writer：

```bash
cd /root/szz/codespace/hpc-redis/benchmark

./ub_cc_nc_visibility_ut frame-writer-nc \
  --path /dev/obmm_shmdev6 --ack-path /dev/obmm_shmdev3 \
  --offset 7516192768 --seed 0x6a1d000000000000 \
  --frame-bytes 4096 --iterations 1000000 --timeout-seconds 300
```

112 上启动 client 侧 CC reader：

```bash
cd /root/szz/codespace/hpc-redis/benchmark

./ub_cc_nc_visibility_ut frame-reader-cc \
  --path /dev/obmm_shmdev2 --ack-path /dev/obmm_shmdev7 \
  --offset 7516192768 --seed 0x6a1d000000000000 \
  --frame-bytes 4096 --iterations 1000000 --timeout-seconds 300 \
  --expect-visibility-failure
```

`--expect-visibility-failure` 用于已知 UB 故障的复现：任意
`FRAME_VISIBILITY_FAILURE type=marker|mixed` 都返回 `0`。若跑完仍未观察到，reader
输出 `NOT_REPRODUCED` 并返回 `3`，不能把未触发当成通过。正常回归验证不带该开关：
任一 frame 可见性失败即返回非零。`--expect-mixed` 保留给更严格的 body-only 情形，
它只接受 `type=mixed`。

### 12.2 检测器自验证

`--inject-mixed-at N` 仅用于验证 UT 的检测逻辑，不用于 UB 真实复现。它在 writer
的第 N 代故意保留 body word 3 的上一代值，但仍发布当前 header、trailer 和
descriptor。reader 配合 `--expect-mixed` 必须输出：

```text
FRAME_VISIBILITY_FAILURE type=mixed generation=N descriptor=N index=3 ... header=N trailer=N
```

这证明 UT 可以检测与实际故障同形态的“当前 frame 标记 + 旧 body”状态；是否由
真实 UB 路径触发，仍必须以 12.1 的双节点、无注入运行结果为准。

### 12.3 当前验证状态

本地普通共享文件已完成两项自验证：

1. 1,000 次 4KiB frame 复用，无注入，writer/reader 都成功，输出
   `NOT_REPRODUCED`。
2. 第 2 代启用 `--inject-mixed-at 2`，reader 使用 `--expect-mixed` 成功输出
   `FRAME_VISIBILITY_FAILURE type=mixed generation=2 descriptor=2 index=3`，同时
   header/trailer 都为 `2`。
3. 新 SSH 登录入口下的真实双机、无注入运行已在第 2 代复现：111 writer 为
   `dev6` NC，112 reader 为 `dev2` CC，输出
   `FRAME_VISIBILITY_FAILURE type=marker generation=2 descriptor=2 header=2 trailer=1`。
   reader 已看到当前 descriptor 与 header，但同一 frame 的 trailer 仍是上一代；反向 ack
   尚未允许 writer 重用该 frame，因此该结果排除 producer 提前覆盖。使用
   `--expect-visibility-failure` 的 reader 返回 `0`。

这验证了 generation、反向 ack、检查逻辑和真实 UB 范围级部分可见性。后续复测必须按
12.1 的无注入路径执行，不能以注入自验证替代。

### 12.3.1 第二组 dev9-dev16 frame 对照

按同样的 data/ack 角色把第二组设备平移 8 个编号：

```text
111 writer: data dev14 (imported NC), ack dev11 (local CC)
112 reader: data dev10 (local CC), ack dev15 (imported NC)
```

使用 `frame_bytes=4096`、无注入、100 代复用时，第 1 代可以完成握手；第 2 代
两端分别出现：

```text
reader: TIMEOUT waiting for descriptor=2 actual=1
writer: TIMEOUT waiting for ack=2 actual=1
```

反向角色 `112 dev13/dev12 -> 111 dev9/dev16` 也在第 2 代停止推进。该结果不否定
前述八个 64B line 映射，因为 line 的 CC writer/NC reader 和 imported-to-local
NC writer/CC reader 都已通过；它说明多 cacheline frame 的 descriptor/ack 连续发布
还没有得到稳定的范围级可见性。因而当前只能结论为“dev9-dev16 的 line 映射正常”，
不能把它扩展为“v2 response frame 在该 pair 上已验证正常”。

### 12.4 response frame cacheline 隔离复测（已撤回）

response codec 已改为如下 ABI：首个 64B cacheline 包含 24B header fields 与保留字节，entries
body 从下一个 cacheline 开始；body 后填充，commit `batch_id` 位于最后一个 64B cacheline 的末尾
8B。frame 总长度向上对齐至 64B，因此相邻 frame 也不会与该 commit line 共用 cacheline。
`batch_arena_publish()` 仍只在 body、padding 与 commit line 前 56B 复制完成后，最后写入 commit
的 8B。

对于 32 个 `VEMB_HANDLE + OK` entry，frame 从旧布局的 `1184B` 变为 `1280B`，增加 `96B`。
当前进一步将每个 entry 固定为 64B slot 后，frame 变为 `2176B`：`64B header +
32 * 64B entry + 64B commit line`。该变化已由本地和 111/112 的
`vemb_v16_batch_ring_ut` 验证；本地 UT 明确断言 32-entry frame 长度为 `2176B`、总长度为
64B 的整数倍，并完成一次 encode/decode round-trip。

随后使用相同 UB 方向和精确 `2176B`（32 个固定 64B entry）的无注入 UT 复测，仍在第 2 代得到：

```text
FRAME_VISIBILITY_FAILURE type=marker generation=2 descriptor=2 header=2 trailer=1
READER_EXIT=0
```

因此 header、body、commit trailer 的 cacheline 隔离没有消除可见性故障。它排除了控制字段与 body
共用 cacheline 以及 entry 跨 cacheline 的局部布局因素；UB 仍能让不同连续 cacheline 在 client CC
映射中以不同代次可见。固定 entry 只把“entry 内部跨 cacheline”转化为“entry 整行新旧”，没有提供
整 frame 的原子可见性。
该 response ABI 已撤回：当前代码恢复为 24B header、可变长 entry 和末尾 8B commit 的紧凑布局。
本次是 UB frame UT 验证，未作为新 ABI 完整重建 Redis/client 并运行端到端 workload。
