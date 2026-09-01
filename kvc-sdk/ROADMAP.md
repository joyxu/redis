# KVC SDK ROADMAP

> 单一任务账本。原则：**正确性 > 功能完整性 > 性能调优 > 其他**；
> 方向上始终**融入 Mooncake 现有框架**（master/TE/租约体系为唯一权威），
> 不做旁支自组网。每完成一项就地打勾并注明结果；新任务只进这份文档。

状态图例：☐ 待做  🔄 进行中  ✅ 完成  ❌ 否决/搁置（写明原因）

---

## 梯队一：正确性（已完成 ✅）

- ✅ T1-1 跨节点间歇性内容不符定位
  （TE 误报假说证伪：14 轮端口压力重演全响亮报错；真凶指向平台 snoop
   窗口，15+ 次不复现，待平台侧确认 → 见 E1）
- ✅ T1-2 直读读后校验防线（哨兵重读 + write_seq 复查，脏读回退 TE；
   同机零退化，跨节点 -22%）
- ✅ T1-3 Remove→invalidate 联动（位置缓存驱动；上游租约语义 -706 查明）
- ✅ T1-4 正确性门禁 `kvc_correctness_gate.sh`（同机/跨节点 verify×5 +
   Remove 联动 + 重启持久，12/12 GREEN；揪出并修复重启清零 bug）
- ✅ 附带定位（非 KVC 问题，纯净对照证实）：
   TE-TCP one-shot 端口耗尽（tcp_tw_reuse 修复）；master 默认 10s 租约
   （TTL 调大修复）→ 上游反馈见 U1-U3

## 梯队二：功能完整性 + 真实负载（已完成 ✅）

- ✅ T2-1 读侧直读（同机 3.1×，最强基线口径）
- ✅ T2-2 跨节点 nc 直读 + 流水拷贝（2.6×，默认 env 关）
- ✅ T2-3 master KvcSlotAllocator（分配权统一，消除跨 slot 对象）
- ✅ T2-4 位置缓存（同机 +64%，跨节点 +26%，slot 硬校验）
- ✅ T2-5 批量 flip（无收益，保留代码；写差距非原子开销）
- ✅ T2-6 FAST25 trace 回放接入（三场景，冷/预热/稳态全数字，0 错误）

---

## 梯队三：当前待办

### 🔴 优先级 0：跨节点拓扑感知与 fallback（本轮最高优先）

- 🔴 **T0-1 拓扑感知缺失**：跨节点直读的"HW02 shmdev5 ↔ HW01 shmdev1"
  映射关系**完全靠人肉配置约定**（env 写死设备号 + 手工算几何参数），
  KVC 不感知、不验证 obmm 层的 provision 映射。唯一自动传递的是
  owner_base（shm 发布）。缺：owner 拓扑身份（host/owner_id/几何）的
  shm 自描述 + 读者侧启动校验（防接错设备/几何不一致）。
- 🔴 **T0-2 无 UB 映射机器的 fallback**：env 设了但设备不存在/mmap 失败时，
  lazy_init **每请求重试失败**（无退避、无永久禁用、无告警）。
  缺：探测失败 → 永久禁用直读 + 一次性 LOG(ERROR) → 之后全走 TE。
- 🔴 **T0-3 最小功能验证**：T0-1/T0-2 补齐后，用 **store_kv_bench 功能
  用例**验证三项能力：① 无映射机器（不配/配错设备）→ 自动降级纯 TE，
  verify 仍全过；② 配对设备 → 直读生效（direct 计数增长）+ verify 全过；
  ③ 接错设备（几何/身份不符）→ 启动即告警并降级，不静默错读。

> 详细补法后续专门规划，此处只记能力缺口与验收口径。

### A. 收尾与卫生（小件，先清）

- ☐ A1 未提交改动打包提交
  （README 刷新 / issue 草稿 / trace_replay / nc_bw / 回归+门禁脚本 / rw_bw 删除）
- ☐ A2 daemon 启动恢复流程接入：启动时调 `recover_stale`+`allocator_rebuild`
  （SDK API 已有；shmdev1 现存 64 个 WRITING 残留为现成测试素材）
  验收：kill -9 daemon → 重启 → 残留回收 + master 重启后位图重建正确
- ☐ A3 适配层几何配置去重：block_size/region_bytes 推算公式在
  real_client / client_service(kvcflip) / poscache 三处重复 → 抽成单一
  配置点（env 或共享配置文件），三处引用
- ☐ A4 门禁纳入 kvc_smoke + kvc_probe（当前门禁只跑 store_kv_bench 族）
- ☐ A5 HW02 侧 daemon 恢复流程验证（A2 的对端）
- ☐ A6 `kvc_api_version()`：dlopen 适配层启动时 ABI 版本协商

### B. 性能（测得动再做；本机共租噪声 ±20%，微基准小样本不可信）

- ☐ B1 跨节点直读防线代价优化（-22% → 目标 <10%：哨兵单点化/抽样）
- ☐ B2 跨节点 TE 巨帧无效已定论；多 lane（>16）或 SR-IOV 直通留观察
- ☐ B3 正式性能报告：独占机器窗口重测全矩阵（store_kv_bench + trace 回放
  + 门禁），出可对外引用的数字

### C. 正确性深化

- ☐ C1 E1 跨节点平台确认：与鲲鹏平台侧确认 nc 读 vs 脏 cache snoop 窗口
  （0x63/0x60 同族问题）；确认后决定跨节点直读是否转默认开
- ☐ C2 驱逐推送：master 主动驱逐（非 Remove）→ daemon invalidate
  （借 TaskPoll 通知路径；当前靠 key_hash 防线兜底 + slot 复用回收）
- ☐ C3 HA 场景：master 主备切换/快照恢复时 KVC 侧行为验证（未碰过）

---

## 🔴 分布式推理场景审查（T0-4）：未修，只记录

> 以"复杂分布式推理框架（vLLM TP 多 rank / 异步 batch / 长跑）真实接入"
> 为镜头对当前实现的全量审查。**只记录不修**；修复排期另行决策。
> 按爆雷时点排序，⭐ = 判定正确性级。

### ⭐ 立刻爆（正常配置即触发数据损坏/崩溃）

- ⭐ R1 **多段单例互踩**：real_client 的 `while (global_segment_size > 0)`
  拆段循环（max_mr_size 触发多段）下，适配器 `g_region` 是进程级单例——
  第二个 Mooncake 段走"复用同一 region"分支，两段映射到同一数据区，
  **数据互踩**。4GB 段 + 拆段是 Mooncake 正常行为，非边缘配置。
  - 风险场景：daemon `--global_segment_size=8GB`（> max_mr_size 默认
    拆段阈值）→ Mooncake 挂出两个段。
  - 看护 setup：① daemon 用 8GB 起段（确认 client.log 出现两次
    "Mounting segment"）；② bench 写对象集 A（读回校验过）后继续写
    对象集 B 至跨入第二段；③ 读回 A 全量比对。
  - 判定：A 内容被 B 覆盖/混乱 = bug 实锤；独立完好 = 该路径暂安全
    （需确认 master 是否真的把对象分到了两段——查 master Mem Storage
    与段分布，避免"没触发拆段"的假阴性）。
- ⭐ R2 **owner_base 漂移**：owner_base 是 daemon 进程 VA，daemon 重启必变；
  shm 里旧值失效。已 lazy_init 的读者进程（不重启的推理进程）拿旧
  owner_base 继续换算 offset → 错误地址读（段错误/脏数据）。
  现有"几何校验"无法校验一个 64 位 VA。缺：owner_base 代数/纪元机制 +
  读者侧检测重初始化。
  - 风险场景：长驻推理进程（poscache/直读已初始化）+ daemon 重启
    （运维动作/崩溃拉起）→ 读者进程不重启继续 get。
  - 看护 setup：① 起全集群，bench 进程 A 先跑一轮 get（触发 lazy_init，
    持有旧 owner_base）；② kill -9 daemon → 重启（region 复用，
    owner_base 变新值）；③ A 继续跑 read_perf（不重启）。
  - 判定：段错误/内容错 = bug 实锤（当前预期会炸，因为无纪元检测）；
    看 ASAN/核心地址是否落在 region 映射区间外可进一步确证漂移路径。

### ⭐ 高危（接真实框架第一批触发）

- ⭐ R3 **定长-only 分配悬崖**：KvcSlotAllocator 对 size≠block 返回 null。
  vLLM 的 checksum 元数据对象、变长尾部对象一进来 put 全挂。
  - 风险场景：master 开 KVC_MASTER_BLOCK_SIZE 后，任何非 block 尺寸的
    put（如 value_size=4100、或上游元数据旁对象）。
  - 看护 setup：① store_kv_bench `--value-size 4100`（非 512 倍数会被
    入口校验拦截——先确认拦截层 vs 分配器层谁先接住）；②
    `--value-size 8192`（合法但 ≠block，应触发分配器 null→put 失败）。
  - 判定：8192 的 put 报错而非静默成功 = 悬崖仍在（如实记录）；
    若静默成功则说明有旁路，需要追。根修=多 size class（D3）或
    分配器内 size-class 桶。
- ⭐ R4 **PutRevoke 路径无 flip**：Put 失败/撤销时 slot 停留 WRITING
  （脏状态窗口直到被复用）；PutStart 竞争失败（TP 广播同请求场景）
  的对象同理。需要 Revoke→INVALID/FREE 联动。
  - 风险场景：① 同 key 并发双 put（一个成功一个 Revoke）；
    ② put 后传输失败（端口压力等）触发 PutRevoke。
  - 看护 setup：① 制造传输失败（关 tcp_tw_reuse + 压端口，已验证
    可稳定诱发 -800）；② 失败后立刻 kvc_probe 扫 region。
  - 判定：probe 出现 WRITING 计数 > 0 且长期不回收 = 窗口实锤
    （预期与 -800 数量相关）；结合 recover_stale 计数可量化泄漏速率。
- ⭐ R5 **直读绕过 checksum 校验**：BatchGet 的 VerifyObjectChecksum
  在 TE 路径执行，直读路径无内容级校验（哨兵只证"没被改"不证"是这份"）。
  上游启用 checksum 时直读证据链弱于 TE 一档。
  - 风险场景：上游 enable object checksum（bench 传 checksum 或
    Mooncake 配置开启）+ 直读命中。
  - 看护 setup：① 确认 Mooncake checksum 的开启开关与生成方式
    （store_kv_bench 的 object_checksum 传递链）；② 同一负载分别
    直读 on/off，人为在 region 数据区改一个字节（debug 写或 dd），
    比较两条路径的检出能力。
  - 判定：TE 路径报 CHECKSUM_MISMATCH 而直读路径返回"成功" = 差距
    实锤（当前预期如此）；根修=直读后算 checksum 比对（代价 O(n)，
    需决策）。

### 🟠 中危（长跑/生产化前必爆）

- 🟠 R6 **直读跳过租约申请**：poscache 命中跳过 GetReplicaList = 跳过
  读租约 → master 可在直读进行中驱逐 → slot 复用 → 防线拦截 → 回退 TE
  → 重查 master 对象已没 → miss。语义安全但直读优势归零 + 付回退成本。
  缺：直读命中的租约续期/驱逐抑制。
  - 风险场景：长跑（runtime > 租约 TTL）+ 池内数据被驱逐复用。
  - 看护 setup：① master TTL 设短（如 2s）；② poscache TTL 设长
    （如 60s，制造"缓存活着、租约死了"的错位）；③ read_perf 长跑，
    统计 miss 分布与 g_guard_rejects/g_direct_fallback 计数。
  - 判定：miss 集中在 TTL 边界后 + fallback 计数激增 = 赛跑实锤；
    记录频率作为租约续期方案的决策输入。
- 🟠 R7 **环境变量矩阵人肉部署**：KVC_* 六个 env × 每进程 × 每机器，
  不一致即静默降级/错读（T0-1 的延伸，需单一配置点 + owner 广播 + 校验）。
  - 风险场景：多机器部署时某台 block_size/bytes 配错（如 daemon 4G
    读者算 2G）。
  - 看护 setup：① 读者故意配错 KVC_FLIP_BYTES（-/+ 一半）；② 错设备号；
    ③ 漏 KVC_SDK_LIB。各起 bench。
  - 判定：每项都应"启动即告警 + 降级 TE + verify 仍全过"；静默错读
    = T0-1/T0-2 缺口实锤（这正是 T0-3 验收的三个用例）。
- 🟠 R8 **可观测性为零**：direct/flip/guard 计数器无出口（无周期日志、
  无 metrics 端点），生产上"为什么慢/为什么回退"黑盒。
  - 看护 setup：跑一轮 mixed 负载后检查：direct/flip/guard 计数
    在任何日志/接口中是否可见（当前预期：不可见，需手动 gdb）。
  - 判定：不可见即实锤；最小修复=周期性 LOG(INFO) 计数器（半小时级）。
- 🟠 R9 **异步/旁路路径未挂钩**：直读与 flip 只挂主路径；
  prefer_alloc_in_same_node 分支、hot_cache 分支等静默走 TE——
  行为不一致且难排查。
  - 风险场景：bench/connector 走 `prefer_alloc_in_same_node=true` 或
    开启 hot_cache 的配置。
  - 看护 setup：① `MC_STORE_HOT_CACHE=true` + 同前缀反复读（触发
    hot_cache 路径）；② `prefer_alloc_in_same_node` 的 get_batch 调用
    （需小型 python 驱动）；③ 对比主路径的 flip/direct 计数变化。
  - 判定：旁路路径下 flip/direct 计数不增长 = 未覆盖实锤（当前预期）；
    记录哪些路径未挂，作为接线清单。

### 🟢 低危（记录在案）

- 🟢 R10 大 block（MB 级 KV page）从未实测：flip 换算/几何公式理论通用，
  但 put_batch 32×7MB 的 TE 切片、max_mr_size 交互未验证。
  - 看护 setup：trace 回放工具加 `--page-size` 大页跑法（7MB×batch 32，
    需相应调大 region 与 max_mr_size）；观察 TE 切片、flip、probe。
  - 判定：0 错误 + probe READY 精确 = 通过；任何错位/截断 = 实锤。
- 🟢 R11 poscache 单进程多线程 batch 调用的锁竞争未测。
  - 看护 setup：numjobs>1 的等价多线程驱动（store_kv_bench 当前
    单车道，需小改或独立驱动）打 poscache 命中路径。
  - 判定：吞吐不随线程涨/latency 抖 = 争抢实锤（预期 mutex 是瓶颈）。
- 🟢 R12 防线依赖链：直读安全 = READY 检查 × key_hash × write_seq 三道
  同时在场。当前 Mooncake 路径闭环 ✓；未来任何"绕过 SDK 直连 shm"的
  使用方式都会裸奔。
  - 看护方式：无需测试——在 SDK README 与 kvc_server.h 显著标注
    "直读安全性以 SDK 计数器/校验为唯一授权路径"硬边界即可。

---

## 梯队四：生产化（需要外部条件或决策）

- ☐ D1 Mooncake patch 正式化：dlopen → CMake 正式链接 + 上游 PR
  （前置：U1-U3 issue 发出并跟进）
- ☐ D2 真实推理端到端：vLLM(+connector) 在 GPU 节点实测 TTFT/吞吐收益
  （当前 CPU 环境只能做 connector 形状驱动，已否决替代方案）
- ☐ D3 多 size class / 多 region manifest：混合层组模型或多模型共池
  （触发条件未到）

---

## ❌ 否决 / 搁置记录（防重复提议）

- ❌ 无 master 自组网 / 内置路由（optional/topo）：方向定为融入现有框架
- ❌ PIO/SNW 执行模型进数据面：与单边直读哲学冲突
- ❌ 变长 blob：KV block 定长语义下无需求，触发才议
- ❌ kvc_rw_bw.c：与 probe/nc_bw 重叠，已删除
- ❌ "SDK 形状驱动"替代 vLLM 实测：被否（按计划来）
- 搁置：vLLM connector 实测（等 GPU/决策）

## 外部依赖

- U1-U3 上游 issue 发布（草稿：tests/upstream_issues_draft.md）
- E1 平台侧 SNP/snoop 语义确认
- GPU 节点（D2）
