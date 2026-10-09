/* kvc_common.h — KVC SDK 公共类型与错误码（P0）
 *
 * KVC SDK 把 hpc-redis 的 UB 共享内存能力封装成 KV Cache 介质 SDK，
 * 供 Mooncake Store 及其他推理框架接入。本头文件定义全级别共用的
 * 基础类型，无任何逻辑。
 *
 * 依赖: 仅 libc（stdint/stddef），零 Redis 依赖，纯 C ABI。
 */
#ifndef KVC_COMMON_H
#define KVC_COMMON_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define KVC_SDK_VERSION_MAJOR 0
#define KVC_SDK_VERSION_MINOR 1

/* ---------------------------------------------------------------------------
 * 错误码 — 所有接口统一返回 int, KVC_OK(0) 成功, 负值失败
 * ------------------------------------------------------------------------- */
typedef enum {
    KVC_OK          = 0,   /* 成功 */
    KVC_EINVAL      = -1,  /* 参数非法（违反前置条件） */
    KVC_ENOMEM      = -2,  /* 内存/mmap 分配失败 */
    KVC_ENOENT      = -3,  /* region / slot 不存在 */
    KVC_EFULL       = -4,  /* region 无空闲 slot */
    KVC_EAGAIN      = -5,  /* 暂时不可用，可重试（如 attach 未完成） */
    KVC_ETIMEOUT    = -6,  /* 操作超时 */
    KVC_ESTALE      = -7,  /* 句柄/状态已过期（slot 被覆写或驱逐） */
    KVC_ENODEV      = -8,  /* 设备不可用（shmdev 打开/mmap 失败） */
    KVC_ENOTREADY   = -9,  /* slot 尚未就绪（WRITING 中，读端看到） */
} kvc_rc_t;

/* ---------------------------------------------------------------------------
 * 零拷贝句柄 — 与 Mooncake replica descriptor 对齐
 *
 * 由 Mooncake master 的 GetReplicaList（或接入方自己的元数据层）给出，
 * client 凭句柄定位数据: region_id → 已注册的映射, offset → slot 内偏移。
 * 约定: offset 恒为 block_size 的整数倍（offset = slot_idx * block_size），
 * 因此 slot_idx = offset / block_size。
 * ------------------------------------------------------------------------- */
typedef struct {
    uint64_t offset;       /* 段内偏移（相对数据区基址）= slot_idx * block_size，
                              与 Mooncake replica offset 语义一致 */
    uint32_t region_id;    /* 对应 Mooncake segment_id */
    uint32_t block_size;   /* 定长 block 字节数（= KV page size） */
} kvc_handle_t;            /* 布局契约: 16 字节（大字段在前避免 padding） */

/* ---------------------------------------------------------------------------
 * region 提供者类型
 * ------------------------------------------------------------------------- */
typedef enum {
    KVC_PROVIDER_DEVICE = 0,  /* /dev/obmm_shmdevN 字符设备（UB 本地/远端内存） */
    KVC_PROVIDER_ANON   = 1,  /* 匿名 mmap（单机回环 / 测试用） */
} kvc_provider_t;

/* region 打开标志 */
#define KVC_REGION_O_SYNC_FALLBACK 0x1u  /* O_RDWR 被拒(EPERM)时回退 O_RDWR|O_SYNC
                                            （远端 noncacheable import 需要，
                                             已在 HW01/HW02 实测验证） */
#define KVC_REGION_CREATE          0x2u  /* 本地 region 首次建立时初始化 header/slot 表 */

/* ---------------------------------------------------------------------------
 * region 配置 — 一个 region 一个 block_size（一种 KV page size class）
 *
 * 前置条件（违反返回 KVC_EINVAL）:
 *   - block_size 为 512 的倍数（与 Mooncake 写路径 value_size 对齐要求一致）
 *   - bytes 为 block_size 的整数倍
 *   - bytes >= region header + 至少 1 个 slot
 * ------------------------------------------------------------------------- */
typedef struct {
    uint32_t      region_id;      /* 全局唯一，对应 Mooncake segment_id */
    kvc_provider_t provider;      /* DEVICE / ANON */
    char          device_path[96];/* provider=DEVICE 时有效, 如 /dev/obmm_shmdev1 */
    uint64_t      mmap_offset;    /* 设备内偏移（多 region 共用一个 shmdev 时错开） */
    uint64_t      bytes;          /* region 总字节数 */
    uint32_t      block_size;     /* 定长 block = KV page size */
    uint32_t      flags;          /* KVC_REGION_* 组合 */
} kvc_region_config_t;

/* ---------------------------------------------------------------------------
 * slot 状态机 — 跨节点数据可见性协议
 *
 * 状态翻转的字节序/内存序契约（实现必须遵守，调用方可依赖）:
 *   - 写端: 先写 slot 数据，再以 release 语义翻转 state=READY 并递增 write_seq
 *     （对应 SNP 语义下的全屏障：保证 peer 读到 READY 时数据必已可见）
 *   - 读端: 以 acquire 语义读 state/write_seq，READY 且 seq 匹配方可 deref
 *   - owner_gen 用于写者身份代数，崩溃恢复时识别半写残留（WRITING 且 gen/seq
 *     不完整的 slot 直接回收——P1 的 slot_scan_state 提供）
 * ------------------------------------------------------------------------- */
typedef enum {
    KVC_SLOT_FREE = 0,   /* 空闲，可分配 */
    KVC_SLOT_WRITING = 1,/* 已分配，写入中（读端不可见） */
    KVC_SLOT_READY = 2,  /* 写入完成，读端可见 */
    KVC_SLOT_INVALID = 3,/* 已失效（驱逐/覆写中；P2 细化 EVICTING 子态） */
} kvc_slot_state_t;

typedef struct {
    kvc_slot_state_t state;
    uint32_t owner_gen;     /* 写者代数（崩溃恢复判据） */
    uint64_t write_seq;     /* 单调递增写序号（就绪/陈旧判据） */
    uint64_t key_hash;      /* 64bit key 指纹（debug/恢复扫描用） */
} kvc_slot_info_t;

#ifdef __cplusplus
}
#endif

#endif /* KVC_COMMON_H */
