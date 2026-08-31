/* kvc_layout.c — region 布局解算/初始化/校验 */
#include "kvc_layout.h"

#include <string.h>

/* 几何解算: 从一个保守的 capacity 估计出发迭代收敛。
 * meta_end = 64 + cap*32; data_off = align_up(meta_end, min(block,4096));
 * cap = (region_bytes - data_off) / block。两轮迭代足够收敛。 */
int kvc_layout_solve(uint64_t region_bytes, uint32_t block_size,
                     uint32_t *capacity_out, uint64_t *data_off_out)
{
    if (!capacity_out || !data_off_out || block_size == 0 ||
        (block_size & 511u) != 0)
        return -1;

    uint32_t align = block_size < 4096u ? block_size : 4096u;
    uint64_t cap = region_bytes /
                   (KVC_LAYOUT_SLOT_META_SIZE + (uint64_t)block_size);
    uint64_t data_off = 0;

    for (int iter = 0; iter < 2; iter++) {
        uint64_t meta_end =
            KVC_LAYOUT_HEADER_SIZE + cap * KVC_LAYOUT_SLOT_META_SIZE;
        data_off = (meta_end + align - 1) / align * align;
        if (data_off >= region_bytes)
            return -1;
        uint64_t cap2 = (region_bytes - data_off) / block_size;
        if (cap2 == cap)
            break;
        cap = cap2;
    }
    if (cap == 0 || cap > 0xffffffffu)
        return -1;

    *capacity_out = (uint32_t)cap;
    *data_off_out = data_off;
    return 0;
}

int kvc_layout_init(void *base, uint64_t region_bytes, uint32_t region_id,
                    uint32_t block_size)
{
    uint32_t capacity;
    uint64_t data_off;
    if (!base || kvc_layout_solve(region_bytes, block_size, &capacity,
                                  &data_off) != 0)
        return -1;

    kvc_layout_header_t *h = kvc_layout_header(base);
    memset(h, 0, KVC_LAYOUT_HEADER_SIZE);
    h->magic = KVC_LAYOUT_MAGIC;
    h->version = KVC_LAYOUT_VERSION;
    h->region_id = region_id;
    h->block_size = block_size;
    h->capacity_slots = capacity;
    h->data_off = data_off;
    h->region_bytes = region_bytes;

    /* slot 表清零: state 0 == KVC_SLOT_FREE */
    memset((char *)base + KVC_LAYOUT_HEADER_SIZE, 0,
           (uint64_t)capacity * KVC_LAYOUT_SLOT_META_SIZE);
    return 0;
}

int kvc_layout_validate(const void *base, uint64_t region_bytes,
                        uint32_t region_id, uint32_t block_size)
{
    uint32_t capacity;
    uint64_t data_off;
    if (!base || kvc_layout_solve(region_bytes, block_size, &capacity,
                                  &data_off) != 0)
        return -1;

    const kvc_layout_header_t *h = kvc_layout_header_ro(base);
    if (h->magic != KVC_LAYOUT_MAGIC || h->version != KVC_LAYOUT_VERSION)
        return -1;
    if (h->region_id != region_id || h->block_size != block_size)
        return -1;
    if (h->capacity_slots != capacity || h->data_off != data_off ||
        h->region_bytes != region_bytes)
        return -1;
    return 0;
}
