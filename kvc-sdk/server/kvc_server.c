/* kvc_server.c — libkvc_server 实现（P0）
 *
 * region 生命周期 + slot 分配器（纯位图）+ slot 状态机 + 直接寻址。
 * 内存序契约见 kvc_common.h：写端 release 发布，读端 acquire 消费。
 */
#include "kvc_region_internal.h"
#include "kvc_ub.h"

#include <stdlib.h>
#include <string.h>

/* ---------------------------------------------------------------------------
 * 前置条件检查（最高外部边界，只在此处做）
 * ------------------------------------------------------------------------- */
static int cfg_check(const kvc_region_config_t *cfg)
{
    if (!cfg)
        return KVC_EINVAL;
    if (cfg->block_size == 0 || (cfg->block_size & 511u) != 0)
        return KVC_EINVAL;
    if (cfg->bytes == 0 || cfg->bytes % cfg->block_size != 0)
        return KVC_EINVAL;
    if (cfg->provider == KVC_PROVIDER_DEVICE &&
        (cfg->device_path[0] == '\0' || cfg->mmap_offset != 0))
        return KVC_EINVAL;   /* P0: 设备内偏移仅支持 0（整设备一个 region） */
    if (cfg->provider != KVC_PROVIDER_DEVICE && cfg->provider != KVC_PROVIDER_ANON)
        return KVC_EINVAL;
    return KVC_OK;
}

/* ---------------------------------------------------------------------------
 * region 生命周期
 * ------------------------------------------------------------------------- */
static kvc_region_t *region_open(const kvc_region_config_t *cfg, int create)
{
    if (cfg_check(cfg) != KVC_OK)
        return NULL;

    void *base;
    if (cfg->provider == KVC_PROVIDER_DEVICE)
        base = kvc_map_memory(cfg->device_path, cfg->mmap_offset, cfg->bytes,
                              cfg->flags);
    else
        base = kvc_map_memory(NULL, 0, cfg->bytes, 0);
    if (!base)
        return NULL;

    uint32_t capacity;
    uint64_t data_off;
    if (kvc_layout_solve(cfg->bytes, cfg->block_size, &capacity, &data_off) != 0)
        goto fail;

    if (create ? kvc_layout_init(base, cfg->bytes, cfg->region_id,
                                 cfg->block_size) != 0
               : kvc_layout_validate(base, cfg->bytes, cfg->region_id,
                                     cfg->block_size) != 0)
        goto fail;

    kvc_region_t *r = calloc(1, sizeof(*r));
    if (!r)
        goto fail;
    r->cfg = *cfg;
    r->base = base;
    r->capacity_slots = capacity;
    r->data_off = data_off;
    return r;

fail:
    kvc_unmap_memory(base, cfg->bytes);
    return NULL;
}

kvc_region_t *kvc_region_open_local(const kvc_region_config_t *cfg)
{
    return region_open(cfg, (cfg->flags & KVC_REGION_CREATE) ? 1 : 0);
}

kvc_region_t *kvc_region_open_remote(const kvc_region_config_t *cfg)
{
    /* remote 永不 CREATE，必须校验既有 header */
    return region_open(cfg, 0);
}

void kvc_region_close(kvc_region_t *region)
{
    if (!region)
        return;
    kvc_unmap_memory(region->base, region->cfg.bytes);
    free(region);
}

int kvc_region_meta(const kvc_region_t *region, kvc_region_meta_t *out)
{
    if (!region || !out)
        return KVC_EINVAL;
    out->region_id = region->cfg.region_id;
    out->block_size = region->cfg.block_size;
    out->capacity_slots = region->capacity_slots;
    out->data_off = region->data_off;
    out->data_bytes = (uint64_t)region->capacity_slots * region->cfg.block_size;
    return KVC_OK;
}

void *kvc_region_data_base(const kvc_region_t *region)
{
    return region ? (char *)region->base + region->data_off : NULL;
}

uint64_t kvc_region_data_bytes(const kvc_region_t *region)
{
    return region ? (uint64_t)region->capacity_slots * region->cfg.block_size
                  : 0;
}

uint32_t kvc_region_id(const kvc_region_t *region)
{
    return region ? region->cfg.region_id : 0;
}

/* ---------------------------------------------------------------------------
 * slot 分配器 — 纯元数据位图，master 可用，不需要任何内存映射
 * ------------------------------------------------------------------------- */
struct kvc_slot_allocator {
    kvc_region_meta_t meta;
    uint64_t  nwords;
    uint64_t *bits;        /* 1 = 已分配 */
    uint32_t  used;
};

kvc_slot_allocator_t *kvc_slot_allocator_create(const kvc_region_meta_t *meta)
{
    if (!meta || meta->capacity_slots == 0)
        return NULL;
    kvc_slot_allocator_t *a = calloc(1, sizeof(*a));
    if (!a)
        return NULL;
    a->meta = *meta;
    a->nwords = ((uint64_t)meta->capacity_slots + 63) / 64;
    a->bits = calloc(a->nwords, sizeof(uint64_t));
    if (!a->bits) {
        free(a);
        return NULL;
    }
    return a;
}

void kvc_slot_allocator_destroy(kvc_slot_allocator_t *alloc)
{
    if (!alloc)
        return;
    free(alloc->bits);
    free(alloc);
}

int kvc_slot_allocator_alloc(kvc_slot_allocator_t *alloc, uint32_t *slot_idx)
{
    if (!alloc || !slot_idx)
        return KVC_EINVAL;
    for (uint64_t w = 0; w < alloc->nwords; w++) {
        uint64_t word = alloc->bits[w];
        if (word == UINT64_MAX)
            continue;
        uint32_t b = 0;
        while (b < 64 && ((word >> b) & 1ULL))
            b++;   /* 首个 0 位即空闲位（word != ~0 保证存在） */
        uint32_t idx = (uint32_t)(w * 64 + b);
        if (idx >= alloc->meta.capacity_slots)
            return KVC_EFULL;
        alloc->bits[w] = word | (1ULL << b);
        alloc->used++;
        *slot_idx = idx;
        return KVC_OK;
    }
    return KVC_EFULL;
}

int kvc_slot_allocator_free(kvc_slot_allocator_t *alloc, uint32_t slot_idx)
{
    if (!alloc || slot_idx >= alloc->meta.capacity_slots)
        return KVC_EINVAL;
    uint64_t w = slot_idx / 64, b = slot_idx % 64;
    if ((alloc->bits[w] & (1ULL << b)) == 0)
        return KVC_EINVAL;
    alloc->bits[w] &= ~(1ULL << b);
    alloc->used--;
    return KVC_OK;
}

uint32_t kvc_slot_allocator_capacity(const kvc_slot_allocator_t *alloc)
{
    return alloc ? alloc->meta.capacity_slots : 0;
}

uint32_t kvc_slot_allocator_used(const kvc_slot_allocator_t *alloc)
{
    return alloc ? alloc->used : 0;
}

/* ---------------------------------------------------------------------------
 * slot 状态机（release/acquire 原子，SNP 可见性顺序）
 * ------------------------------------------------------------------------- */
int kvc_slot_mark_writing(kvc_region_t *region, uint32_t slot_idx,
                          uint64_t key_hash)
{
    if (!region || slot_idx >= region->capacity_slots)
        return KVC_EINVAL;
    kvc_layout_slot_t *s = kvc_layout_slot(region->base, slot_idx);

    uint32_t expected = KVC_SLOT_FREE;
    if (!__atomic_compare_exchange_n(&s->state, &expected, KVC_SLOT_WRITING,
                                     0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
        return KVC_ESTALE;

    s->owner_gen++;   /* 仅 WRITING 持有者触碰其余字段，普通写即可 */
    s->key_hash = key_hash;
    return KVC_OK;
}

int kvc_slot_mark_ready(kvc_region_t *region, uint32_t slot_idx)
{
    if (!region || slot_idx >= region->capacity_slots)
        return KVC_EINVAL;
    kvc_layout_slot_t *s = kvc_layout_slot(region->base, slot_idx);

    uint32_t st = __atomic_load_n(&s->state, __ATOMIC_ACQUIRE);
    if (st != KVC_SLOT_WRITING)
        return KVC_EINVAL;

    s->write_seq++;
    /* release: slot 数据与 write_seq 先于 state=READY 对 peer 可见 */
    __atomic_store_n(&s->state, KVC_SLOT_READY, __ATOMIC_RELEASE);
    return KVC_OK;
}

int kvc_slot_invalidate(kvc_region_t *region, uint32_t slot_idx)
{
    if (!region || slot_idx >= region->capacity_slots)
        return KVC_EINVAL;
    kvc_layout_slot_t *s = kvc_layout_slot(region->base, slot_idx);
    uint32_t st = __atomic_load_n(&s->state, __ATOMIC_ACQUIRE);
    if (st == KVC_SLOT_FREE)
        return KVC_EINVAL;
    __atomic_store_n(&s->state, KVC_SLOT_INVALID, __ATOMIC_RELEASE);
    return KVC_OK;
}

int kvc_slot_query(const kvc_region_t *region, uint32_t slot_idx,
                   kvc_slot_info_t *out)
{
    if (!region || !out || slot_idx >= region->capacity_slots)
        return KVC_EINVAL;
    const kvc_layout_slot_t *s = kvc_layout_slot(region->base, slot_idx);

    /* acquire: state=READY 之后的数据读取不会重排到此前 */
    uint32_t st = __atomic_load_n(&s->state, __ATOMIC_ACQUIRE);
    out->state = (kvc_slot_state_t)st;
    out->owner_gen = s->owner_gen;
    out->write_seq = s->write_seq;
    out->key_hash = s->key_hash;
    return KVC_OK;
}

/* ---------------------------------------------------------------------------
 * 直接寻址
 * ------------------------------------------------------------------------- */
void *kvc_region_slot_addr(const kvc_region_t *region, uint32_t slot_idx)
{
    if (!region || slot_idx >= region->capacity_slots)
        return NULL;
    return kvc_layout_slot_data(region->base, region->data_off, slot_idx,
                                region->cfg.block_size);
}

/* ===========================================================================
 * P1 实现
 * =========================================================================== */
int kvc_write_slot_direct(kvc_region_t *region, uint32_t slot_idx,
                          uint64_t key_hash, const void *block)
{
    if (!region || !block)
        return KVC_EINVAL;
    int rc = kvc_slot_mark_writing(region, slot_idx, key_hash);
    if (rc != KVC_OK)
        return rc;
    memcpy(kvc_region_slot_addr(region, slot_idx), block,
           region->cfg.block_size);
    return kvc_slot_mark_ready(region, slot_idx);
}

int kvc_slot_scan_state(const kvc_region_t *region, kvc_slot_scan_cb cb,
                        void *ctx)
{
    if (!region || !cb)
        return KVC_EINVAL;
    for (uint32_t i = 0; i < region->capacity_slots; i++) {
        kvc_slot_info_t info;
        if (kvc_slot_query(region, i, &info) != KVC_OK)
            return KVC_EINVAL;
        int rc = cb(ctx, i, &info);
        if (rc != 0)
            return rc;
    }
    return KVC_OK;
}

uint32_t kvc_slot_recover_stale(kvc_region_t *region)
{
    if (!region)
        return 0;
    uint32_t recovered = 0;
    for (uint32_t i = 0; i < region->capacity_slots; i++) {
        kvc_layout_slot_t *s = kvc_layout_slot(region->base, i);
        uint32_t st = __atomic_load_n(&s->state, __ATOMIC_ACQUIRE);
        if (st == KVC_SLOT_WRITING) {
            /* 单写者假设（P1）: WRITING 残留即写者已死。
             * 先置 FREE 让分配器可复用；owner_gen 留痕供审计。 */
            __atomic_store_n(&s->state, KVC_SLOT_FREE, __ATOMIC_RELEASE);
            recovered++;
        }
    }
    return recovered;
}

int kvc_slot_allocator_rebuild(kvc_slot_allocator_t *alloc,
                               kvc_region_t *region)
{
    if (!alloc || !region)
        return KVC_EINVAL;
    if (alloc->meta.capacity_slots != region->capacity_slots)
        return KVC_EINVAL;

    memset(alloc->bits, 0, alloc->nwords * sizeof(uint64_t));
    alloc->used = 0;
    for (uint32_t i = 0; i < region->capacity_slots; i++) {
        kvc_slot_info_t info;
        if (kvc_slot_query(region, i, &info) != KVC_OK)
            return KVC_EINVAL;
        if (info.state != KVC_SLOT_FREE) {
            alloc->bits[i / 64] |= 1ULL << (i % 64);
            alloc->used++;
        }
    }
    return KVC_OK;
}

int kvc_region_stats(const kvc_region_t *region, kvc_region_stats_t *out)
{
    if (!region || !out)
        return KVC_EINVAL;
    memset(out, 0, sizeof(*out));
    out->capacity = region->capacity_slots;
    for (uint32_t i = 0; i < region->capacity_slots; i++) {
        const kvc_layout_slot_t *s = kvc_layout_slot(region->base, i);
        uint32_t st = __atomic_load_n(&s->state, __ATOMIC_ACQUIRE);
        switch (st) {
        case KVC_SLOT_FREE:     out->free_slots++; break;
        case KVC_SLOT_WRITING:  out->writing_slots++; break;
        case KVC_SLOT_READY:    out->ready_slots++; break;
        default:                out->invalid_slots++; break;
        }
        if (s->write_seq > out->max_write_seq)
            out->max_write_seq = s->write_seq;
    }
    return KVC_OK;
}

int kvc_region_publish_base(kvc_region_t *region)
{
    if (!region)
        return KVC_EINVAL;
    kvc_layout_header_t *h = kvc_layout_header(region->base);
    __atomic_store_n(&h->owner_base,
                     (uint64_t)(uintptr_t)((char *)region->base +
                                           region->data_off),
                     __ATOMIC_RELEASE);
    return KVC_OK;
}

uint64_t kvc_region_owner_base(const kvc_region_t *region)
{
    if (!region)
        return 0;
    const kvc_layout_header_t *h = kvc_layout_header_ro(region->base);
    return __atomic_load_n(&h->owner_base, __ATOMIC_ACQUIRE);
}
