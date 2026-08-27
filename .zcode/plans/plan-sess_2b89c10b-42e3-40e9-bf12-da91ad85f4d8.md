# 目标
1. 按 docs/REPORT_SKELETON.md 新版报告结构，产出全量数据采集命令表（新 md 文件）。
2. 补齐各 sweep 脚本的 Aeron 模式能力，使报告 3.2.1–3.2.4/3.2.7 的 "hpc Aeron" 各表都能采到数据。

# 现状结论（已探明）
- Aeron 模式 = server 侧 `--vemb-v16-transport aeron --vemb-v16-aeron-ub-path <req> --vemb-v16-aeron-response-ub-path <resp>`；client 侧 `--vemb-v16-transport=aeron --vemb-v16-ub-peer-view-manifest/-client-host/-owner-id`（memtier 强制要求 peer-view 三件套）。`-s/-p` 单端点与 `--vemb-v16-endpoints` 多端点在 aeron 下都支持；VADD/VREM/VSIM 各 op flag 在 aeron runner 下均可用。
- 已有 aeron 能力：scripts/run_aeron_best.sh（本地回环）、scripts/run_aeron_cross_node_flamegraph.sh（跨节点抓 perf 用，非 TSV sweep）、benchmark/vemb_v16_aeron_cluster_tput.sh（双节点 aeron cluster，含 client peer-view manifest 合成逻辑 L112-172 可复用）、bench_aeron_3scenarios.sh（3.2.6 已覆盖）。
- 缺口（本次实现）：run_vemb_local_loopback_sweep.sh、run_vemb_cross_node_sweep.sh、run_vsim_2key_2node.sh、run_cluster_2node_sweep.sh、scripts/hpc_redis_scaleout_throughput.sh 五个脚本均只有 TCP/sniff 模式。

# 改动内容

## 1. 新文件 docs/REPORT_COMMAND_TABLE.md
按报告章节逐表列出命令（脚本 + 环境变量），覆盖：
- 3.2.1 本地回环/跨节点 × VEMB/VSIM_2KEY/VADD/VREM × baseline/hpc TCP/hpc Aeron × 9 档（OP_TYPE=..., HPC_TRANSPORT=aeron, SERVERS_ONLY 等）
- 3.2.1 & 3.2.2 高维 DIM=1024/2048/3072 64x1x32
- 3.2.2 同核数 17:17 vs baseline 12 实例（含跨节点 NIC 采集）
- 3.2.3 集群三方案（原生 / hpc TCP / hpc Aeron，含高维）
- 3.2.4 扩容 hpc TCP / hpc Aeron / 原生
- 3.2.5 混合读写、3.2.6 7:1 拓扑、3.2.7 VSIM_2KEY 双节点
- 附：每条命令的产物 TSV 路径约定（对应报告 "文件:" 字段）与执行纪律（串行、空闲检查）

## 2. benchmark/run_vemb_local_loopback_sweep.sh — 加 HPC_TRANSPORT（默认 tcp）
- tcp 时行为完全不变（零 diff 风险）。
- aeron 分支：server 追加 `--vemb-v16-transport aeron --vemb-v16-aeron-ub-path /dev/obmm_shmdev1 --vemb-v16-aeron-response-ub-path /dev/obmm_shmdev3`（与 run_aeron_best 相同分配；warm manifest 在 shmdev2，无冲突）；memtier prefill/bench 追加 `--vemb-v16-transport=aeron --vemb-v16-handle` + peer-view 三件套（manifest=examples/vemb_v16_ub_peer_view_local_111.yaml, client-host=local, owner-id=0，均可环境变量覆盖）。
- 启动前对两个 UB path 做 `lsof -t` 占用检查（抄 run_aeron_best 的 check_ub_paths_in_use）。

## 3. benchmark/run_vemb_cross_node_sweep.sh — 同款 HPC_TRANSPORT
- server(HW01) 追加 aeron 参数（UB 分配沿用 flamegraph 脚本：req=shmdev3 resp=shmdev6，需与 warm manifest 设备不相交，实现时核对 MANIFEST 的 path 后定）。
- ssh 部署的 bench helper 增加位置参数（transport/manifest/client-host/owner-id），hpc+aeron 分支注入 peer-view 三件套；manifest 用两机同步仓库里的 examples/vemb_v16_ub_peer_view_112_to_111.yaml，client-host=112, owner-id=0。
- 同样加 UB 占用检查。

## 4. benchmark/run_vsim_2key_2node.sh — HPC_TRANSPORT=aeron
- 两节点 server 各加 aeron 参数，UB 设备两节点错开（参考 vemb_v16_aeron_cluster_tput：node0 shmdev1/2、node1 shmdev10/11，warm manifest 设备核对不相交后定稿）。
- client(node0, 多端点) 需要"覆盖两个 owner"的 peer-view manifest：复用 aeron_cluster_tput.sh L112-172 的合成逻辑（owner0 本地 view + 从 111_to_112.yaml 翻译的 owner1 view），抽成脚本内函数生成临时 manifest。

## 5. run_cluster_2node_sweep.sh — TRANSPORT=aeron
- aeron 分支：两节点 server 加 aeron 参数（同上设备错开）；集群拓扑从"多端点直连"改为 topology_ctl 建链（node1 `--set --ctl-endpoint tcp --data-endpoint aeron --owner-endpoints ...`，node0 `--set-with-peer-view-map examples/vemb_v16_ub_cluster_111_to_112_node0_peer_map.yaml ...`，抄 aeron_cluster_tput L196-197）；client 侧用合成 peer manifest + `--vemb-v16-endpoints` + transport=aeron。
- 9 档 sweep / CPU 采样 / TSV 输出结构不动。

## 6. scripts/hpc_redis_scaleout_throughput.sh — TRANSPORT=aeron
- start_node 按节点加 aeron UB 路径；三处 topology_ctl `--transport tcp` 改为按 TRANSPORT 切 `--ctl-endpoint tcp --data-endpoint aeron`（node0 侧带 peer-view-map）；run_memtier 加 peer-view 三件套（合成 manifest 复用第 4 条的函数思路或静态生成）。

## 7. 文档同步
- docs/PERF_REGRESSION_GUIDE.md 对应测试项补 Aeron 模式命令行。

# 验证
1. 全部改动脚本 `bash -n` 通过。
2. 本地回环 aeron smoke（HW01，TS=1 CS=1 PS=1 TEST_TIME=10，OP_TYPE=VEMB）：确认 TSV 出行、ops 非 0、cores/ut/st 有值、UB 设备启动前无占用、跑后无残留。
3. 跨节点 aeron smoke（HW01+HW02，单档 VEMB + VSIM_2KEY）。
4. TCP 模式回归 smoke：改后脚本默认参数跑单档，确认与改前行为一致（零回归）。
（跑前按纪律做空闲检查；UB 用例严格串行）

# 说明
- HW05 仍失联，3.2.3 原生 4 节点 cluster 与 3.2.4 原生扩容涉及 HW04/HW05 的项在命令表中标注"待 HW05 恢复"。
- 3.2.4 hpc Aeron 扩容依赖第 6 条；3.2.3 Aeron 双机 cluster 依赖第 5 条。