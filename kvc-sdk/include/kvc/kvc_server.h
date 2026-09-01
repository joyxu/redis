/* kvc_server.h — KVC SDK 服务端接口（P0）: 介质的管理者
 *
 * 使用方:
 *   - mooncake_client 进程: open_local 建立并挂出 UB region（MountSegment 的被挂方）
 *   - mooncake_master 进程: kvc_slot_allocator_* 纯元数据位图分配
 *     （即未来 TlcSlotAllocator : BufferAllocatorBase 的函数体，替代
 *      OffsetBufferAllocator——master 全程不碰内存）
 *   - 写端 client: mark_writing/mark_ready（PutStart/PutEnd 语义落到 shm 状态机）
 *
 * P0 边界: 写路径的数据搬运本身走 Mooncake TE（segment protocol="ub"），
 * SDK 只负责"分配 offset、标状态、给地址"。
 */
#ifndef KVC_SERVER_H
#define KVC_SERVER_H

#include "kvc_common.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kvc_region kvc_region_t;          /* opaque: 已映射的 region */

/* region 几何元数据（master 构建 allocator 视图 / 对外上报用） */
typedef struct {
    uint32_t region_id;
    uint32_t block_size;
    uint32_t capacity_slots;   /* 数据区可容纳的 slot 数 */
    uint64_t data_off;         /* 数据区在 region 内的字节偏移（slot 元数据表之后） */
    uint64_t data_bytes;       /* 数据区字节数 = capacity_slots * block_size */
} kvc_region_meta_t;

/* ---------------------------------------------------------------------------
 * 1. region 生命周期
 * ------------------------------------------------------------------------- */

/* 打开本地 region: mmap 本地 shmdev（O_RDWR，EPERM 时按 flags 回退 O_SYNC）
 * 或匿名内存；KVC_REGION_CREATE 时初始化 region header（魔数/版本/容量/
 * block_size）与 slot 状态表（全部 FREE）。幂等重开（无 CREATE）则校验
 * 已有 header 与 cfg 一致，不一致返回 KVC_ESTALE。
 * 前置条件: cfg 满足 kvc_common.h 所列约束。 */
kvc_region_t *kvc_region_open_local(const kvc_region_config_t *cfg);

/* attach 远端 region: 通过 attach 握手获取 peer 的映射参数后 mmap peer
 * shmdev（O_SYNC fallback 已实测验证）。P0 要求接入方先完成 attach 拿到
 * device_path/mmap_offset（握手复用 core/net）；P1 提供完整 attach 流程。
 * 前置条件: cfg 描述的是 peer 侧同一 region 的几何参数。 */
kvc_region_t *kvc_region_open_remote(const kvc_region_config_t *cfg);

/* 关闭 region（仅 munmap 本进程映射，不影响其他持有者；slot 表内容留存于
 * 共享内存中，重开后状态延续）。 */
void kvc_region_close(kvc_region_t *region);

/* 查询 region 几何（供 master 构建 allocator 元数据视图 / 上报） */
int kvc_region_meta(const kvc_region_t *region, kvc_region_meta_t *out);

/* 数据区直接访问（Mooncake 适配用：段基址 = 数据区基址，
 * 段大小 = data_bytes，slot 元数据表对 Mooncake 不可见） */
void    *kvc_region_data_base(const kvc_region_t *region);
uint64_t kvc_region_data_bytes(const kvc_region_t *region);
uint32_t kvc_region_id(const kvc_region_t *region);

/* ---------------------------------------------------------------------------
 * 2. slot 分配器 — master 专用，纯元数据位图，不需要 mmap 任何内存
 *
 * 分配结果 offset = slot_idx * block_size，与 Mooncake master 的
 * 段内 offset 语义一一对应。master 侧单线程持锁调用（master 本就如此，
 * 无跨进程分配竞争）。
 * ------------------------------------------------------------------------- */
typedef struct kvc_slot_allocator kvc_slot_allocator_t;

/* 创建/销毁。分配位图驻留调用方进程内存；master 重启后由 PutEnd 元数据
 * 重建（P1 的 kvc_slot_scan_state 提供从 shm slot 表直接重建的路径）。 */
kvc_slot_allocator_t *kvc_slot_allocator_create(const kvc_region_meta_t *meta);
void  kvc_slot_allocator_destroy(kvc_slot_allocator_t *alloc);

/* 分配一个空闲 slot。region 满返回 KVC_EFULL。 */
int   kvc_slot_allocator_alloc(kvc_slot_allocator_t *alloc, uint32_t *slot_idx);

/* 归还 slot（重复 free 或越界返回 KVC_EINVAL）。 */
int   kvc_slot_allocator_free(kvc_slot_allocator_t *alloc, uint32_t slot_idx);

uint32_t kvc_slot_allocator_capacity(const kvc_slot_allocator_t *alloc);
uint32_t kvc_slot_allocator_used(const kvc_slot_allocator_t *alloc);

/* ---------------------------------------------------------------------------
 * 3. slot 状态机 — 跨节点可见性协议（内存序契约见 kvc_common.h）
 *
 * 典型 Mooncake 写路径映射:
 *   PutStart  → alloc_slot + mark_writing(key_hash)
 *   数据搬运  → （Mooncake TE, protocol="ub"）
 *   PutEnd    → mark_ready
 * ------------------------------------------------------------------------- */

/* 占据 slot 并标记写入中（owner_gen 递增, key_hash 记录指纹）。
 * 前置条件: slot 处于 FREE 或 INVALID。 */
int kvc_slot_mark_writing(kvc_region_t *region, uint32_t slot_idx,
                          uint64_t key_hash);

/* 写入完成，发布可见（write_seq 递增 + release 语义状态翻转）。
 * 前置条件: slot 处于 WRITING 且为本持有者。 */
int kvc_slot_mark_ready(kvc_region_t *region, uint32_t slot_idx);

/* 失效（驱逐/删除路径；master 驱逐回调触发）。INVALID 后 slot 数据不可再读。 */
int kvc_slot_invalidate(kvc_region_t *region, uint32_t slot_idx);

/* 状态查询（读端就绪判定；acquire 语义读）。 */
int kvc_slot_query(const kvc_region_t *region, uint32_t slot_idx,
                   kvc_slot_info_t *out);

/* ---------------------------------------------------------------------------
 * 4. 直接寻址 — 零拷贝核心
 * ------------------------------------------------------------------------- */

/* 返回 slot 虚拟地址（本地或已映射的远端 region 均可），调用方直接 deref。
 * 同 UB cache-coherent 域内读 = 一条 load 指令。
 * 前置条件: slot 已 READY（或调用方明确知道自己在写）。
 * 生存期: 到下一次该 slot 的 invalidate/覆写为止。 */
void *kvc_region_slot_addr(const kvc_region_t *region, uint32_t slot_idx);

/* ===========================================================================
 * P1 接口 — 数据面完善
 * =========================================================================== */

/* 写端直写（UB_DIRECT 种子）: mark_writing → memcpy → mark_ready 一步完成，
 * 绕过任何传输引擎。要求 region 为可写映射。
 * 前置条件: slot 处于 FREE/INVALID。返回 KVC_OK 表示对端已可见。 */
int kvc_write_slot_direct(kvc_region_t *region, uint32_t slot_idx,
                          uint64_t key_hash, const void *block);

/* 遍历 slot 状态表（观测/恢复扫描）。回调返回非 0 立即中止并透传返回值。
 * 回调内不得调用本 region 的写接口。 */
typedef int (*kvc_slot_scan_cb)(void *ctx, uint32_t slot_idx,
                                const kvc_slot_info_t *info);
int kvc_slot_scan_state(const kvc_region_t *region, kvc_slot_scan_cb cb,
                        void *ctx);

/* 崩溃恢复: WRITING 残留 slot 回收为 FREE（P1 单写者假设: 所有 WRITING 视为
 * 写入者已死）。返回回收数量。READY/INVALID 不动。 */
uint32_t kvc_slot_recover_stale(kvc_region_t *region);

/* 分配器位图重建（master 重启恢复路径）:
 * READY / INVALID / WRITING → 已分配；FREE → 空闲。
 * 前置条件: alloc 与 region 的 capacity 一致。 */
int kvc_slot_allocator_rebuild(kvc_slot_allocator_t *alloc,
                               kvc_region_t *region);

/* region 统计（快照，内部即一次 scan） */
typedef struct {
    uint32_t capacity;
    uint64_t free_slots, writing_slots, ready_slots, invalid_slots;
    uint64_t max_write_seq;   /* 观测到的最大写序号（活性参考） */
} kvc_region_stats_t;

int kvc_region_stats(const kvc_region_t *region, kvc_region_stats_t *out);

/* P1: owner 数据区基址发布/查询。
 * Mooncake 的 replica.buffer_address_ 是段 owner 进程的绝对 VA；owner 调
 * publish 把 data_base VA 写进 region header（release），其他进程 query 后
 * 用 buffer_address_ - owner_base 得到段内 offset（= slot_idx * block_size）。
 * 返回 0 表示 owner 尚未发布。 */
int kvc_region_publish_base(kvc_region_t *region);
uint64_t kvc_region_owner_base(const kvc_region_t *region);

/* P0.5: region 状态探测（不做完整 open，只读 header 判定）。
 * 返回 KVC_PROBE_INITIALIZED(0)=已初始化且几何一致
 *       KVC_PROBE_UNINITIALIZED(1)=无 magic, CREATE 安全
 *       KVC_PROBE_CONFLICT(-1)=已初始化但几何/身份冲突, CREATE 会摧毁数据 */
#define KVC_PROBE_INITIALIZED   0
#define KVC_PROBE_UNINITIALIZED 1
#define KVC_PROBE_CONFLICT     -1
int kvc_region_probe(const kvc_region_config_t *cfg);

#ifdef __cplusplus
}
#endif

#endif /* KVC_SERVER_H */
