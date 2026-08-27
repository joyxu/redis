```sh
NODE=192.168.90.112 BUILD=1 WORKERS=20:20 \
  REMOTE_DIR=/root/szz/codespace/hpc-redis_bench \
  bash scripts/run_host_mt_server_flamegraph.sh

NODE=192.168.90.112 \
  bash scripts/sync_changed_code_to_peer.sh --dry-run
```

跨节点 VEMB 测试必须在启动任一角色前完成代码同步和构建：

```sh
NODE=192.168.90.111 bash scripts/sync_changed_code_to_peer.sh \
  --all-code --build server --verify-build server
NODE=192.168.90.112 bash scripts/sync_changed_code_to_peer.sh \
  --all-code --build client --verify-build client
```

`vemb_v16_build_stamp.sh` 会记录源码、编译设置、编译策略和产物 hash。
手工完成等价的远端构建后，执行
`bash scripts/vemb_v16_build_stamp.sh write server|client` 写入 stamp；
`run_aeron_best.sh` 默认会校验匹配的 stamp。

## 跨节点 VEMB 火焰图

`run_aeron_cross_node_flamegraph.sh` 在 111 节点启动全新的 VEMB v16
server，在 112 节点执行顺序 VADD 预填充，再通过 `perf` 运行真实的 V2
`VEMB_HANDLE` 读 workload，最后把完整的 server/client 工件拉取到
`perf/`。server 采样会绑定每一个 Redis TID，因此合并后的 server 火焰图
同时包含 proxy IO 和 SuperNode worker 的用户态、内核态栈。

每次脚本调用只运行一组 workload。预填充和读请求使用同一个
`KEY_PREFIX`，默认值固定为 `item:`；只有需要隔离命名空间时才显式设置
其他前缀。

### 前置条件

- 使用 `SSH_USER`/`SSH_PORT` 可以免密 SSH 到两个节点。
- 两台远端机器上的源码树已经同步。脚本本身不会同步源码。
- 配置的 `/dev/obmm_shmdev*` 设备存在且没有被其他进程占用。
- `FLAMEGRAPH_DIR` 下存在可执行的 `stackcollapse-perf.pl` 和
  `flamegraph.pl`。
- server/client 节点具备 `perf`、`pidstat`、`mpstat`、`lsof` 和 `ss`。

如果远端源码尚未同步，先执行：

```sh
NODE=192.168.90.111 bash scripts/sync_changed_code_to_peer.sh \
  --all-code --build server --verify-build server
NODE=192.168.90.112 bash scripts/sync_changed_code_to_peer.sh \
  --all-code --build client --verify-build client
```

### 基本命令

运行默认的 100K uniform workload：

```sh
bash scripts/run_aeron_cross_node_flamegraph.sh
```

运行 100K、Zipf `s=1.5` 热点 workload：

```sh
KEY_PATTERN=Z:Z ZIPF_S=1.5 \
  bash scripts/run_aeron_cross_node_flamegraph.sh
```

运行 10K、Zipf `s=1.5` workload：

```sh
NUM_KEYS=10000 KEY_PATTERN=Z:Z ZIPF_S=1.5 \
  bash scripts/run_aeron_cross_node_flamegraph.sh
```

只校验配置，不执行 SSH、构建、压测和采样：

```sh
DRY_RUN=1 KEY_PATTERN=Z:Z ZIPF_S=1.5 \
  bash scripts/run_aeron_cross_node_flamegraph.sh
```

标准 `PIO:SNW=12:12`、server CPU `0-15` 样本。该命令强制 server/client
全量重建，并固定使用 `item:` 前缀：

```sh
RUN_ID=aeron_p12_s12_r1 BUILD=build KEY_PREFIX=item: \
PIO=12 SNW=12 SERVER_CPU_MASK=0-15 CLIENT_CPU_MASK=96-191 \
THREADS=64 CLIENTS=4 PIPELINE=32 BATCH_REQUEST_SIZE=32 \
TEST_TIME=30 SERVER_FLAME_DURATION=25 \
bash scripts/run_aeron_cross_node_flamegraph.sh
```

短时 smoke test。必须保持 `SERVER_FLAME_DURATION < TEST_TIME`：

```sh
TEST_TIME=5 SERVER_FLAME_DURATION=3 BUILD=build \
  bash scripts/run_aeron_cross_node_flamegraph.sh
```

### 测试场景

比较不同结果时使用以下场景定义：

| 场景 | 环境变量 | 目的 |
|---|---|---|
| Uniform baseline | `KEY_PATTERN=R:R NUM_KEYS=100000` | 在完整 keyspace 上执行均匀随机读。 |
| Zipf 热点 | `KEY_PATTERN=Z:Z ZIPF_S=1.0`、`1.2` 或 `1.5` | 测量热点局部性逐步增强时的表现；比较指数时固定 key 数、CPU 和 batch 配置。 |
| Sequential read | `KEY_PATTERN=S:S` | 按确定的 key 顺序读取，仅在确实需要顺序访问时使用。 |
| L1 enabled | `L1_ENTRIES=4096` 或其他 `4 * 2^k` | 测量 worker-local L1；必须和相同 CPU mask 下的 `L1_ENTRIES=0` 对比。 |
| PIO/SNW sweep | `PIO=N SNW=N` | 比较 server worker 配置；固定线程、client、CPU mask、key 前缀和 batch。 |
| CPU mask comparison | `SERVER_CPU_MASK=0-15` 或 `0-47` | 比较 process cores 和 CPU-set cores；不同 mask 不能放在同一个 A/B 结论中。 |

每组 A/B 必须固定 `KEY_PREFIX`、`NUM_KEYS`、`DIM`、client mask、pipeline、
batch 大小、测试时长、build policy 和源码 commit，只改变正在测量的场景变量。

### 重要参数

| 变量 | 默认值 | 说明 |
|---|---:|---|
| `NUM_KEYS` | `100000` | 预填充和读取的 keyspace 大小。 |
| `KEY_PREFIX` | `item:` | 预填充和读 workload 共用的 key 前缀，可显式覆盖。 |
| `DIM`, `MAX_VECTORS` | `300`, `131072` | vector 维度和 server vector 容量上限。 |
| `KEY_PATTERN` | `R:R` | 读模式：`R:R` 均匀随机、`S:S` 顺序、`Z:Z` Zipf。 |
| `ZIPF_S` | 无 | Zipf 指数；`KEY_PATTERN=Z:Z` 时必须为正数。 |
| `TEST_TIME` | `30` | client 读 workload 时长，单位秒。 |
| `SERVER_FLAME_DURATION` | `25` | server `perf` 采样时长，必须小于 `TEST_TIME`。 |
| `THREADS`, `CLIENTS` | `64`, `4` | memtier worker 线程数及每线程 client 数。 |
| `PIPELINE` | `32` | 每个 channel 的 pipeline 深度。 |
| `BATCH_REQUEST_SIZE` | `32` | client/server 的 VEMB v2 request batch 大小。 |
| `BATCH_MAX_DELAY_US` | `0` | client batch deadline；`0` 表示关闭按延迟 flush。 |
| `L1_ENTRIES` | `0` | worker-local CLI L1 entry 数，必须为 `0` 或 `4 * 2^k`。 |
| `PROXY_REQUEST_BATCH`, `PROXY_RESPONSE_BATCH`, `PROXY_QUEUE_BATCH` | `BATCH_REQUEST_SIZE` | server 内部 request、response、queue batch 大小。 |
| `PIO`, `SNW` | `21`, `21` | server proxy-IO 和 SuperNode worker 数。 |
| `SERVER_CPU_MASK`, `CLIENT_CPU_MASK` | `0-15`, `96-191` | server/client 使用的 CPU 集合。 |
| `SERVER_NODE`, `CLIENT_NODE` | `192.168.90.111`, `192.168.90.112` | 远端 server/client 节点。 |
| `SSH_USER`, `SSH_PORT` | `root`, 空 | SSH 用户和端口；`SSH_PORT` 为空时不传 `-p`，直接使用 `~/.ssh/config` 中主机别名的端口配置。 |
| `SERVER_ROOT`, `CLIENT_ROOT` | `/root/szz/codespace/hpc-redis` | 远端源码根目录。 |
| `FLAMEGRAPH_DIR` | `/root/FlameGraph` | 远端 FlameGraph 工具目录。 |
| `SERVER_MANIFEST` | `$SERVER_ROOT/examples/vemb_perf_warm_111.yaml` | server warm-region manifest；默认使用仓库内的 4 GiB `/dev/obmm_shmdev4` 配置。 |
| `PORT`, `SERVER_IP` | `6395`, `192.168.90.111` | Redis control endpoint。 |
| `SERVER_REQUEST_UB_PATH`, `SERVER_RESPONSE_UB_PATH`, `SERVER_WARM_UB_PATH` | `/dev/obmm_shmdev3`, `/dev/obmm_shmdev6`, `/dev/obmm_shmdev4` | server request/response/warm UB 设备。 |
| `CLIENT_REQUEST_UB_PATH`, `CLIENT_RESPONSE_UB_PATH`, `CLIENT_WARM_UB_PATH` | `/dev/obmm_shmdev7`, `/dev/obmm_shmdev2`, `/dev/obmm_shmdev8` | client request/response/warm UB 设备。 |
| `FREQ`, `EVENT` | `99`, `cycles` | `perf` 采样频率和 event。 |
| `BUILD` | `verify` | `verify` 要求 O3/LTO/SVE build stamp 匹配；`build` 会先重建对应远端角色。 |
| `KEEP_SERVER` | `0` | 设为 `1` 时，测试结束后保留临时 server。 |
| `REQUIRE_VEMB_THREAD_SAMPLES` | `1` | 要求 server perf 中出现 proxy IO 和 SuperNode 符号。 |
| `MAX_FOREIGN_CPU_PCT` | `10` | 1 秒 `pidstat` 中，任一外部进程超过该 CPU 百分比即失败。 |
| `MAX_FOREIGN_TOTAL_CPU_PCT` | `20` | 所有外部进程 CPU 总和超过该百分比即失败。 |
| `MAX_FOREIGN_CPUSET_BUSY_PCT` | `10` | 目标 CPU 集合的外部 busy 超过该百分比即失败。 |
| `MAX_FOREIGN_RSS_MB` | `256` | 外部进程 RSS 超过该 MiB 即失败。 |
| `KILL_OPENCODE` | `1` | 设为 `0` 保留 `opencode` tmux session 和同名进程。 |
| `KILL_MUTAGEN` | `1` | 设为 `0` 保留 `mutagen-agent` 及其直接 parent。 |
| `RUN_ID`, `LOCAL_ROOT` | `p<PIO>_s<SNW>_svr<SERVER_CPU_MASK>_<distribution>_<timestamp>` | 工件名称和本地输出目录；显式设置 `RUN_ID` 可覆盖。 |
| `REMOTE_TMP_ROOT`, `REMOTE_RUN_DIR` | `/tmp`, `$REMOTE_TMP_ROOT/$RUN_ID` | 远端临时运行目录。 |
| `DRY_RUN` | `0` | 设为 `1` 只校验参数，不执行 SSH、构建和测试。 |

`SERVER_REQUEST_UB_PATH`、`SERVER_RESPONSE_UB_PATH`、`SERVER_WARM_UB_PATH`、
`CLIENT_REQUEST_UB_PATH`、`CLIENT_RESPONSE_UB_PATH` 和 `CLIENT_WARM_UB_PATH`
可覆盖默认 UB 设备。该脚本使用 Aeron TCP control 加 UB ring 的跨节点路径，
不要与 local loopback runner 的参数混用。

### 门禁与进程清理

server 和 client 在启动 server、prefill 或 benchmark 前都必须通过 CPU/RSS
门禁。门禁会报告阻塞 PID、CPU 百分比、RSS 和命令，原始 `pidstat` 及 blocker
文件会保留在成功工件中。应优先清理无关 workload，不要直接绕过门禁。

默认情况下脚本会停止 `opencode` tmux session 和残留同名进程，并先结束
`mutagen-agent` 的直接 parent，再结束 agent，防止立即 respawn。只有明确需要
保留对应 workload 时才设置 `KILL_OPENCODE=0` 或 `KILL_MUTAGEN=0`。

### 执行流程与输出

脚本按以下顺序执行：参数校验、两节点 load gate、UB ownership 检查、启动全新
server、顺序预填充 `NUM_KEYS`、启动 all-TID server `perf` 和 CPU 采样、执行
client workload、生成两端火焰图；除非设置 `KEEP_SERVER=1`，最后会停止临时 server。

成功工件默认保存在 `perf/$RUN_ID/`：

| 路径 | 内容 |
|---|---|
| `server/server.svg` | 所有 Redis TID 的用户态/内核态火焰图。 |
| `client/client.svg` | client 进程用户态/内核态火焰图。 |
| `server/server.perf.data`, `client/client.perf.data` | 原始 perf 数据。 |
| `server/server.cpu.process.tsv` | Redis process user/system/total core-equivalent CPU，含窗口末 VmRSS（`rss_kb`）与进程峰值 VmHWM（`rss_peak_kb`）。 |
| `server/server.cpu.cpuset.summary.tsv` | 固定 CPU 集合的分类平均值和 core-equivalent。 |
| `server/server.cpu.summary.txt` | 对齐后的 server CPU 人类可读表格。 |
| `client/client.workload.summary.tsv` | 纵向、指标左对齐、value 右对齐的 workload 摘要。 |
| `client/client.l1_summary.tsv` | L1/resource 摘要，包括 QPS/P99、server items、UB bytes 和 RSS。 |
| `client/client.workload.log` | 完整 workload 日志，包括每个 worker 的 leader/follower 计数。 |

`client.workload.summary.tsv` 不再输出 `run_label`，run label 保存在
`client.meta.txt`。摘要包含 `qps_ops_sec`、`avg_lat_ms`、`p50_ms`、
`p99_ms`、`p99_9_ms`、有效统计窗口、logical ops、`leaders`、`followers`
以及 `leaders_plus_followers_per_sec`。最后一个速率使用与 QPS 相同的有效
窗口，应该和 `qps_ops_sec` 相等；不一致表示 completion 丢失或未被 accounting。

查看对齐后的摘要：

```sh
column -t -s $'\t' perf/$RUN_ID/client/client.workload.summary.tsv
```

每个 SVG 的 title 包含紧凑 run label：key 数和分布、维度、`PIO`/`SNW`、
threads/clients、pipeline、batch 大小、batch delay、测试/火焰图时长和
`RUN_ID`。subtitle 包含角色、CPU mask、event 和采样频率。

server archive 还包含 `server.cpu.process.tsv`、
`server.cpu.cpuset.mpstat.txt` 和 `server.cpu.summary.txt`。其中 process TSV
记录 Redis user/system/total core-equivalent；CPU-set 文件记录 `usr`、`sys`、
`irq`、`soft` (`si`)、`iowait` 和 `total=100-idle`。该脚本不采集 client CPU
统计；client CPU 使用 `client.perf.data` 和火焰图分析。

如果 load gate 超限、UB 设备已被占用、server 线程数量不匹配 `PIO`/`SNW`、
必要 flamegraph 符号缺失、workload 出现 connection/data-plane error，或摘要/
工件为空，脚本会失败且不报告有效结果。保留失败目录，优先检查
`*.preflight.*`、`server.log` 和 `client.workload.log`。
