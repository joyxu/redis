/* kvc_layout.h — region 物理布局（内部头，不对外安装）
 *
 * 布局（详见 core/layout/README.md）:
 *   [0, 64)                  region header（单 cacheline）
 *   [64, 64+cap*32)          slot 元数据表（state/owner_gen/write_seq/key_hash）
 *   [data_off, data_off+cap*block)  数据区（定长 slot，元数据与数据分离）
 *
 * 确定性: data_off 与 capacity 由 (region_bytes, block_size) 唯一决定，
 * init 与 validate 两侧用同一算法，无需落盘持久化。
 */
#ifndef KVC_LAYOUT_H
#define KVC_LAYOUT_H

#include <stddef.h>
#include <stdint.h>

#define KVC_LAYOUT_MAGIC   0x5243564Bu   /* "KVCR" */
#define KVC_LAYOUT_VERSION 1u

/* region header — 64 字节，单 cacheline */
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t region_id;
    uint32_t block_size;
    uint32_t capacity_slots;
    uint32_t used_hint;      /* 松散计数，仅供观测 */
    uint64_t owner_base;     /* owner 进程的数据区 VA（跨进程 offset 换算用） */
    uint32_t reserved1;
    uint64_t data_off;
    uint64_t region_bytes;
} kvc_layout_header_t;

/* slot 元数据 — 32 字节，独立于数据行（扫状态不触碰数据） */
typedef struct {
    uint32_t state;          /* kvc_slot_state_t */
    uint32_t owner_gen;
    uint64_t write_seq;
    uint64_t key_hash;
} kvc_layout_slot_t;

#define KVC_LAYOUT_HEADER_SIZE ((uint64_t)sizeof(kvc_layout_header_t))
#define KVC_LAYOUT_SLOT_META_SIZE ((uint64_t)sizeof(kvc_layout_slot_t))

/* 解算几何: 给定 region_bytes/block_size，输出 capacity 与 data_off。
 * 返回 0 成功（bytes 至少容纳 header + 1 个 slot），-1 参数非法。 */
int kvc_layout_solve(uint64_t region_bytes, uint32_t block_size,
                     uint32_t *capacity_out, uint64_t *data_off_out);

/* 初始化（CREATE 路径）: 写 header、清零 slot 表（全部 FREE）。 */
int kvc_layout_init(void *base, uint64_t region_bytes, uint32_t region_id,
                    uint32_t block_size);

/* 校验（重开/远端 attach 路径）: header 与期望几何一致。 */
int kvc_layout_validate(const void *base, uint64_t region_bytes,
                        uint32_t region_id, uint32_t block_size);

/* 内联访问器（base 为 region 映射基址） */
static inline kvc_layout_header_t *kvc_layout_header(void *base)
{
    return (kvc_layout_header_t *)base;
}

static inline const kvc_layout_header_t *kvc_layout_header_ro(const void *base)
{
    return (const kvc_layout_header_t *)base;
}

static inline kvc_layout_slot_t *kvc_layout_slot(void *base, uint32_t idx)
{
    return (kvc_layout_slot_t *)((char *)base + KVC_LAYOUT_HEADER_SIZE +
                                 (uint64_t)idx * KVC_LAYOUT_SLOT_META_SIZE);
}

static inline void *kvc_layout_slot_data(const void *base, uint64_t data_off,
                                         uint32_t idx, uint32_t block_size)
{
    return (char *)base + data_off + (uint64_t)idx * block_size;
}

#endif /* KVC_LAYOUT_H */
