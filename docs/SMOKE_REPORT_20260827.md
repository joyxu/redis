# 命令表 64x64x32 Smoke 执行报告 (20260827)

## 一、执行结果 (34 条命令, 从尾部往上)

| 章节 | 命令 | 结果 | 数据有效 |
|---|---|---|---|
| §7 VSIM_2KEY TCP | baseline + hpc | ✅ PASS | ✅ |
| §7 VSIM_2KEY aeron | 64x64x32 | ❌→**✅ 已修复** | 修复后有效 |
| §4 扩容 TCP | HW01 代跑 | ✅ PASS | ✅ 8.93M→9.28M |
| §4 扩容 aeron | baseline+during | ⚠️ partial | baseline 17.6M / during 14.1M; after 缺 |
| §6 7:1 拓扑 | 4 条 | ✅ 全 PASS | ✅ |
| §3.1 集群 TCP | 双机 cluster | ✅ PASS | ✅ |
| §3.1 集群 aeron | 64x64x32 | ❌→**✅ 已修复** | 修复后有效 |
| §2 同核数 | 11 条 | ✅ 全 PASS | ✅ |
| §1 单实例 | 19 条 | ✅ 全 PASS | ✅ (VADD 3.8K≈历史 3.5K) |

## 二、修复记录

### 问题① (已修复): aeron cluster VSIM_2KEY 高并发大面积 err
- **现象**: c≥8 且 t≥4 时 status_err 13-75%
- **根因**: `VEMB_V16_VSIM_JOB_POOL_SLOTS=256` (proxy.c:45), 热点 proxy_io_worker 的 vsim job pool 在高并发突发下瞬间打满 → `job_pool_alloc_slot` 失败 → `prepare_request_job` 返 -1 → `publish_status_response(ERR)` 直接回客户端
- **修复**: 池扩大 256→2048; `prepare_request_job` 池满时加 8 轮 cpu_relax 有界重试
- **验证**: t4c8 ✅939K / t8c16 ✅1.10M / t32c32 ✅996K / t64c64 err 从 75%→~0.05%

### 问题② (已修复): 命令表执行位置标注错误
- `hpc_redis_scaleout_throughput.sh` 需内网直连 192.168.1.x:22, Mac 只走 frp 转发口不可达
- **修复**: 命令表标注从"本地 Mac 执行"改为"HW01 执行"

### 问题③ (已修复): scaleout after 阶段看似 migration 不完成 — 实为日志级别配置错误
- **现象**: 两次复现, 脚本 600s 等待 node0 日志出现 "scaleout auto local done notified" / "scaleout auto done", 始终等不到 → die
- **根因**: migration 实际**完全成功** (coordinator 收到 done=1/1, active publish ok). 但这两个日志消息是 **LL_NOTICE** 级别, 脚本启动 server 用了  → 日志被过滤 → 脚本 grep 永远匹配不到 → 等到超时
- **修复**:  server 启动参数  → 
- **验证**: 完整跑通 rc=0, 三 phase 数据齐全: baseline 17.45M / during (迁移中) / after_old 12.76M / after_steady 11.17M
