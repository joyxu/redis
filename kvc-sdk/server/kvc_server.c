/* kvc_server.c — libkvc_server 实现（P0）
 *
 * region 生命周期 + slot 分配器（纯位图）+ slot 状态机 + 直接寻址。
 * 内存序契约见 kvc_common.h：写端 release 发布，读端 acquire 消费。
 */
#include "kvc_region_internal.h"
#include "kvc_ub.h"
#include "ubme_seam.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

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
    if (cfg->provider == KVC_PROVIDER_UBME) {
        if (cfg->seg_name[0] == '\0' || cfg->mmap_offset != 0)
            return KVC_EINVAL;
        if (!(cfg->flags & KVC_REGION_CREATE) &&
            cfg->local_path[0] == '\0')
            return KVC_EINVAL;   /* reader 必须填 attach/import 所得本地路径 */
    }
    if (cfg->provider != KVC_PROVIDER_DEVICE && cfg->provider != KVC_PROVIDER_ANON &&
        cfg->provider != KVC_PROVIDER_UBME)
        return KVC_EINVAL;
    return KVC_OK;
}

/* ---------------------------------------------------------------------------
 * region 生命周期
 * ------------------------------------------------------------------------- */
static kvc_region_t *g_owner_region;   /* 进程内 owner region（驱逐执行端复用） */

kvc_region_t *kvc_owner_region(void)
{
    return g_owner_region;
}

void kvc_seg_name(char out[64], const char *owner_host)
{
    uint64_t h = kvc_identity_hash(owner_host ? owner_host : "");
    snprintf(out, 64, "kvc_seg_h%016llx", (unsigned long long)h);
}

int kvc_ubme_init(uint32_t node_id, uint16_t tcp_port, const char *peers_spec)
{
    return ubme_seam_init(node_id, tcp_port, peers_spec) == 0 ? KVC_OK
                                                             : KVC_ENODEV;
}

int kvc_region_attach_ubme(const char *seg_name, uint32_t timeout_ms,
                           char *out_path, size_t out_cap)
{
    if (!seg_name || !seg_name[0])
        return KVC_EINVAL;
    /* mesh 建联等待: ctx 刚 init 时 mesh 握手异步进行中，冷启动的按名发现
     * 会空手而归（journal 复用未命中时）。有界重试: attach 失败且配置了
     * peers → 500ms 后再试，直到成功或耗尽 timeout_ms; 同机复用形态（无
     * peers）一次即判，不空转。 */
    for (uint32_t waited = 0;; waited += 500) {
        if (ubme_seam_attach_segment(seg_name, timeout_ms, out_path,
                                     out_cap) == 0)
            return KVC_OK;
        if (ubme_seam_peer_ids(NULL, 0) == 0 || waited + 500 > timeout_ms)
            break;
        struct timespec ts = {.tv_sec = 0, .tv_nsec = 500 * 1000 * 1000L};
        nanosleep(&ts, NULL);
    }
    fprintf(stderr, "[kvc-attach] %s: attach failed (journal reuse + "
                    "mesh find + PULL import)\n",
            seg_name);
    return KVC_ENOENT;
}

int kvc_region_detach_ubme(const char *seg_name)
{
    if (!seg_name || !seg_name[0])
        return KVC_EINVAL;
    return ubme_seam_detach_segment(seg_name) == 0 ? KVC_OK : KVC_ENOENT;
}

int kvc_ubme_imports_stale(void)
{
    return ubme_seam_imports_stale();
}

void kvc_owner_shutdown(void)
{
    if (g_owner_region)
        kvc_region_close(g_owner_region);   /* munmap 全 region + 清 owner 登记 */
    ubme_seam_shutdown();
}

static kvc_region_t *region_open(const kvc_region_config_t *cfg, int create)
{
    if (cfg_check(cfg) != KVC_OK)
        return NULL;

    void *base;
    uint64_t region_bytes = cfg->bytes;
    if (cfg->provider == KVC_PROVIDER_DEVICE) {
        base = kvc_map_memory(cfg->device_path, cfg->mmap_offset, cfg->bytes,
                              cfg->flags);
    } else if (cfg->provider == KVC_PROVIDER_UBME) {
        if (create) {
            /* owner: ctx（幂等）+ export 动态段（2MiB 对齐，实际尺寸为
             * 布局权威——段不跨进程重启，头/表从零初始化）。 */
            char path[UBME_SEAM_PATH_MAX];
            uint64_t actual = 0;
            if (ubme_seam_init(cfg->ubme_node_id, cfg->ubme_tcp_port,
                               cfg->ubme_peers) != 0)
                return NULL;
            if (ubme_seam_create_segment(cfg->seg_name, cfg->bytes, path,
                                         sizeof(path), &actual) != 0)
                return NULL;
            region_bytes = actual;
            base = kvc_map_memory(path, 0, region_bytes, cfg->flags);
            if (!base) {
                /* mmap 失败回滚 export, 不留半截段 */
                ubme_seam_detach_segment(cfg->seg_name);
                return NULL;
            }
        } else {
            /* reader: 映射 attach/import 所得本地设备路径。同机 journal
             * 复用节点是 owner 的 CC export（O_RDWR 直过，cached 正确）；
             * 跨节点 NC import 节点 O_RDWR 被拒(EPERM)后回退 O_SYNC
             * （实测路径）。两者统一走 FALLBACK 语义。 */
            base = kvc_map_memory(cfg->local_path, 0, region_bytes,
                                  cfg->flags | KVC_REGION_O_SYNC_FALLBACK);
        }
    } else {
        base = kvc_map_memory(NULL, 0, cfg->bytes, 0);
    }
    if (!base)
        return NULL;

    uint32_t capacity;
    uint64_t data_off;
    if (kvc_layout_solve(region_bytes, cfg->block_size, &capacity, &data_off) != 0)
        goto fail;

    if (create ? kvc_layout_init(base, region_bytes, cfg->region_id,
                                 cfg->block_size) != 0
               : kvc_layout_validate(base, region_bytes, cfg->region_id,
                                     cfg->block_size) != 0)
        goto fail;

    kvc_region_t *r = calloc(1, sizeof(*r));
    if (!r)
        goto fail;
    r->cfg = *cfg;
    r->cfg.bytes = region_bytes;   /* UBME create: 写回 2MiB 对齐实际值 */
    r->base = base;
    r->capacity_slots = capacity;
    r->data_off = data_off;
    if (create && cfg->provider == KVC_PROVIDER_UBME)
        g_owner_region = r;
    return r;

fail:
    kvc_unmap_memory(base, region_bytes);
    if (cfg->provider == KVC_PROVIDER_UBME)
        /* 布局失败: 回滚本段的 export/import 持有, 不留孤儿段 */
        ubme_seam_detach_segment(cfg->seg_name);
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
    if (region == g_owner_region)
        g_owner_region = NULL;
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
    out->region_bytes = region->cfg.bytes;
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

    /* master 重启位图清零后重新分配持久 region 的 slot 时, 可能残留 INVALID
     * (旧对象已删)——视为可复用。仅 READY 不可直接覆写(读者可能正在读)。 */
    uint32_t expected = KVC_SLOT_FREE;
    if (__atomic_compare_exchange_n(&s->state, &expected, KVC_SLOT_WRITING,
                                    0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
        goto acquired;
    expected = KVC_SLOT_INVALID;
    if (!__atomic_compare_exchange_n(&s->state, &expected, KVC_SLOT_WRITING,
                                     0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
        return KVC_ESTALE;
acquired:;

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
    if (getenv("KVC_DEBUG_INVALIDATE"))
        fprintf(stderr, "[kvc] invalidate slot=%u\n", slot_idx);
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

int kvc_region_probe(const kvc_region_config_t *cfg)
{
    if (cfg_check(cfg) != KVC_OK)
        return KVC_PROBE_CONFLICT;
    void *base = kvc_map_memory(cfg->provider == KVC_PROVIDER_DEVICE
                                    ? cfg->device_path
                                    : NULL,
                                cfg->mmap_offset, cfg->bytes, cfg->flags);
    if (!base)
        return KVC_PROBE_CONFLICT;
    int rc = kvc_layout_probe(base, cfg->bytes, cfg->region_id,
                              cfg->block_size);
    kvc_unmap_memory(base, cfg->bytes);
    return rc == 0   ? KVC_PROBE_INITIALIZED
           : rc == 1 ? KVC_PROBE_UNINITIALIZED
                     : KVC_PROBE_CONFLICT;
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

/* ---------------------------------------------------------------------------
 * owner 拓扑身份（T0-1）
 * ------------------------------------------------------------------------- */
uint64_t kvc_identity_hash(const char *identity)
{
    if (!identity)
        return 0;
    uint64_t h = 1469598103934665603ULL;   /* FNV-1a 64 offset basis */
    for (const unsigned char *p = (const unsigned char *)identity; *p; p++) {
        h ^= *p;
        h *= 1099511628211ULL;             /* FNV-1a 64 prime */
    }
    return h ? h : 1;                      /* 0 保留为"未发布" */
}

int kvc_region_publish_identity(kvc_region_t *region, const char *owner_identity)
{
    if (!region || !owner_identity)
        return KVC_EINVAL;
    kvc_layout_header_t *h = kvc_layout_header(region->base);
    uint64_t id = kvc_identity_hash(owner_identity);
    uint32_t next = __atomic_load_n(&h->owner_epoch, __ATOMIC_ACQUIRE) + 1;
    h->owner_host_id = id;   /* 平写；epoch release 之后才对读者可见 */
    __atomic_store_n(&h->owner_epoch, next, __ATOMIC_RELEASE);
    return KVC_OK;
}

int kvc_region_owner_info(const kvc_region_t *region, uint64_t *host_id,
                          uint32_t *epoch)
{
    if (!region || !host_id || !epoch)
        return KVC_EINVAL;
    const kvc_layout_header_t *h = kvc_layout_header_ro(region->base);
    *epoch = __atomic_load_n(&h->owner_epoch, __ATOMIC_ACQUIRE);
    *host_id = h->owner_host_id;   /* epoch acquire 之后的平读已有序 */
    return KVC_OK;
}

uint32_t kvc_region_owner_epoch(const kvc_region_t *region)
{
    if (!region)
        return 0;
    return __atomic_load_n(&kvc_layout_header_ro(region->base)->owner_epoch,
                           __ATOMIC_ACQUIRE);
}

int kvc_region_probe_identity(const char *device_path, uint64_t mmap_offset,
                              uint32_t flags, kvc_region_meta_t *geo_out,
                              uint64_t *host_id, uint32_t *epoch_out)
{
    if (!device_path || !geo_out || !host_id || !epoch_out)
        return KVC_EINVAL;

    void *base = kvc_map_memory(device_path, mmap_offset, 4096, flags);
    if (!base)
        return KVC_ENODEV;
    const kvc_layout_header_t *h = kvc_layout_header_ro(base);
    int rc = KVC_OK;
    if (h->magic != KVC_LAYOUT_MAGIC || h->version != KVC_LAYOUT_VERSION) {
        rc = KVC_ENOENT;
        goto out;
    }
    uint32_t capacity;
    uint64_t data_off;
    if (kvc_layout_solve(h->region_bytes, h->block_size, &capacity,
                         &data_off) != 0 ||
        capacity != h->capacity_slots || data_off != h->data_off) {
        rc = KVC_ESTALE;   /* header 自身几何不自洽（写坏/接错） */
        goto out;
    }
    geo_out->region_id = h->region_id;
    geo_out->block_size = h->block_size;
    geo_out->capacity_slots = h->capacity_slots;
    geo_out->data_off = h->data_off;
    geo_out->data_bytes = (uint64_t)h->capacity_slots * h->block_size;
    geo_out->region_bytes = h->region_bytes;
    *epoch_out = __atomic_load_n(&h->owner_epoch, __ATOMIC_ACQUIRE);
    *host_id = h->owner_host_id;
out:
    kvc_unmap_memory(base, 4096);
    return rc;
}

uint32_t kvc_api_version(void) { return KVC_ABI_VERSION; }
