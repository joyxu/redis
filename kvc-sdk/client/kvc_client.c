/* kvc_client.c — libkvc_client 实现（P0）: region 注册表 + 凭句柄读 */
#include "kvc/kvc_client.h"
#include "../server/kvc_region_internal.h"
#include "../core/ub/kvc_ub.h"

#include <stdlib.h>
#include <string.h>

struct kvc_region_set {
    kvc_region_t **regions;
    uint32_t       count;
    uint32_t       cap;
};

kvc_region_set_t *kvc_region_set_create(void)
{
    kvc_region_set_t *set = calloc(1, sizeof(*set));
    if (set) {
        set->cap = 8;
        set->regions = calloc(set->cap, sizeof(*set->regions));
        if (!set->regions) {
            free(set);
            return NULL;
        }
    }
    return set;
}

void kvc_region_set_destroy(kvc_region_set_t *set)
{
    if (!set)
        return;
    for (uint32_t i = 0; i < set->count; i++)
        kvc_region_close(set->regions[i]);
    free(set->regions);
    free(set);
}

int kvc_region_set_add(kvc_region_set_t *set, kvc_region_t *region)
{
    if (!set || !region)
        return KVC_EINVAL;
    for (uint32_t i = 0; i < set->count; i++)
        if (kvc_region_id(set->regions[i]) == kvc_region_id(region))
            return KVC_EINVAL;
    if (set->count == set->cap) {
        uint32_t ncap = set->cap * 2;
        kvc_region_t **nr = realloc(set->regions, ncap * sizeof(*nr));
        if (!nr)
            return KVC_ENOMEM;
        set->regions = nr;
        set->cap = ncap;
    }
    set->regions[set->count++] = region;
    return KVC_OK;
}

static kvc_region_t *set_find(kvc_region_set_t *set, uint32_t region_id)
{
    for (uint32_t i = 0; i < set->count; i++)
        if (kvc_region_id(set->regions[i]) == region_id)
            return set->regions[i];
    return NULL;
}

/* handle.offset 相对数据区基址，slot_idx = offset / block_size */
static int handle_check(const kvc_handle_t *h, kvc_region_t *r,
                        uint32_t *slot_idx)
{
    if (h->block_size != r->cfg.block_size)
        return KVC_EINVAL;
    if (h->offset % h->block_size != 0)
        return KVC_EINVAL;
    uint64_t idx = h->offset / h->block_size;
    if (idx >= r->capacity_slots)
        return KVC_ENOENT;
    *slot_idx = (uint32_t)idx;
    return KVC_OK;
}

int kvc_read_handle(const kvc_handle_t *handle, kvc_region_set_t *set,
                    void *out_block)
{
    if (!handle || !set || !out_block)
        return KVC_EINVAL;
    kvc_region_t *r = set_find(set, handle->region_id);
    if (!r)
        return KVC_ENOENT;

    uint32_t slot_idx;
    int rc = handle_check(handle, r, &slot_idx);
    if (rc != KVC_OK)
        return rc;

    kvc_slot_info_t info;
    rc = kvc_slot_query(r, slot_idx, &info);
    if (rc != KVC_OK)
        return rc;
    if (info.state != KVC_SLOT_READY)
        return info.state == KVC_SLOT_WRITING ? KVC_ENOTREADY : KVC_ESTALE;

    /* nc 远端内存用流水拷贝（glibc memcpy 不流水, 单线程差 30 倍） */
    kvc_copy_remote(out_block, kvc_region_slot_addr(r, slot_idx),
                    handle->block_size);
    return KVC_OK;
}

int kvc_deref_handle(const kvc_handle_t *handle, kvc_region_set_t *set,
                     const void **out_ptr)
{
    if (!handle || !set || !out_ptr)
        return KVC_EINVAL;
    kvc_region_t *r = set_find(set, handle->region_id);
    if (!r)
        return KVC_ENOENT;

    uint32_t slot_idx;
    int rc = handle_check(handle, r, &slot_idx);
    if (rc != KVC_OK)
        return rc;

    kvc_slot_info_t info;
    rc = kvc_slot_query(r, slot_idx, &info);
    if (rc != KVC_OK)
        return rc;
    if (info.state != KVC_SLOT_READY)
        return info.state == KVC_SLOT_WRITING ? KVC_ENOTREADY : KVC_ESTALE;

    *out_ptr = kvc_region_slot_addr(r, slot_idx);
    return KVC_OK;
}

uint32_t kvc_batch_read_handle(const kvc_handle_t *hs, uint32_t n,
                               kvc_region_set_t *set, void *out_blocks)
{
    if (!hs || !set || !out_blocks)
        return 0;
    uint32_t ok = 0;
    char *cursor = out_blocks;
    for (uint32_t i = 0; i < n; i++) {
        if (kvc_read_handle(&hs[i], set, cursor) == KVC_OK)
            ok++;
        cursor += hs[i].block_size;
    }
    return ok;
}
