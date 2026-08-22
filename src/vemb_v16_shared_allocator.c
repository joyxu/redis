#define _GNU_SOURCE

#include "vemb_v16_shared_allocator.h"
#include "cpu_relax.h"
#include "macro.h"
#include "vemb_v16_log.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>

static uint64_t fnv1a64(const char *s) {
    uint64_t h = 1469598103934665603ULL;
    for (; s && *s; s++) {
        h ^= (unsigned char)*s;
        h *= 1099511628211ULL;
    }
    return h;
}

int vemb_v16_shared_allocator_name_from_region_path(const char *region_path,
                                                    uint32_t region_id,
                                                    char *out,
                                                    size_t out_len) {
    RETURN_IF(!region_path || !region_path[0] || !out || out_len == 0, -1);
    if (region_path[0] == '/' &&
        strchr(region_path + 1, '/') == NULL &&
        strlen(region_path) + strlen(".alloc") < out_len &&
        strlen(region_path) + strlen(".alloc") < 31u) {
        snprintf(out, out_len, "%s.alloc", region_path);
        return 0;
    }
    uint64_t h = fnv1a64(region_path);
    int n = snprintf(out, out_len,
                     "/v16a_%08x_%08x",
                     region_id,
                     (unsigned)(h ^ (h >> 32)));
    RETURN_IF(n <= 0 || (size_t)n >= out_len, -1);
    return 0;
}

static void init_allocator(vemb_v16_shared_region_allocator_t *allocator,
                           uint32_t region_id,
                           uint32_t capacity_slots) {
    memset(allocator, 0, sizeof(*allocator));
    allocator->region_id = region_id;
    allocator->capacity_slots = capacity_slots;
    allocator->version = VEMB_V16_SHARED_ALLOCATOR_VERSION;
    atomic_init(&allocator->next_slot, 0);
    atomic_init(&allocator->full, 0);
    atomic_init(&allocator->used_slots, 0);
    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&allocator->magic,
                          VEMB_V16_SHARED_ALLOCATOR_MAGIC,
                          memory_order_release);
}

static int wait_allocator_ready(vemb_v16_shared_region_allocator_t *allocator,
                                uint32_t region_id,
                                uint32_t capacity_slots) {
    for (uint32_t i = 0; i < 1000000u; i++) {
        uint32_t magic = atomic_load_explicit(&allocator->magic,
                                              memory_order_acquire);
        if (magic == VEMB_V16_SHARED_ALLOCATOR_MAGIC) {
            RETURN_IF(allocator->version != VEMB_V16_SHARED_ALLOCATOR_VERSION,
                      -1);
            RETURN_IF(allocator->region_id != region_id ||
                      allocator->capacity_slots != capacity_slots,
                      -1);
            return 0;
        }
        if (magic == 0) {
            uint32_t expected = 0;
            if (atomic_compare_exchange_strong_explicit(
                    &allocator->magic,
                    &expected,
                    VEMB_V16_SHARED_ALLOCATOR_INITIALIZING,
                    memory_order_acq_rel,
                    memory_order_acquire)) {
                init_allocator(allocator, region_id, capacity_slots);
                return 0;
            }
        }
        cpu_relax();
    }
    return -1;
}

int vemb_v16_shared_allocator_attach(vemb_v16_shared_allocator_mapping_t *mapping,
                                     const vemb_v16_mapped_region_t *region,
                                     uint64_t view_offset,
                                     uint32_t backend_type,
                                     const char *path,
                                     uint64_t mmap_offset,
                                     uint32_t region_id,
                                     uint32_t capacity_slots) {
    RETURN_IF(!mapping || !region || !path || !path[0], -1);
    RETURN_IF(region->mapped_addr == NULL || region->mapping_addr == NULL, -1);
    RETURN_IF(strlen(path) >= sizeof(mapping->name), -1);
    RETURN_IF(view_offset > region->requested_size ||
              sizeof(vemb_v16_shared_region_allocator_t) >
                  region->requested_size - (size_t)view_offset,
              -1);
    if (backend_type == VEMB_V16_REGION_UB &&
        (mmap_offset % VEMB_V16_SHARED_ALLOCATOR_ALIGNMENT) != 0) {
        serverLog(LL_WARNING,
                  "vemb_v16 shared allocator ub offset misaligned: path=%s offset=%llu alignment=%u header_size=%zu",
                  path,
                  (unsigned long long)mmap_offset,
                  VEMB_V16_SHARED_ALLOCATOR_ALIGNMENT,
                  sizeof(vemb_v16_shared_region_allocator_t));
        return -1;
    }

    memset(mapping, 0, sizeof(*mapping));
    mapping->fd = -1;
    mapping->mapping = region;
    mapping->backend_type = backend_type ? backend_type : VEMB_V16_REGION_LOCAL_SHM;
    mapping->mapping_size = sizeof(vemb_v16_shared_region_allocator_t);
    mapping->mmap_offset = mmap_offset;
    mapping->mmap_aligned_offset = region->mmap_aligned_offset;
    mapping->mapping_addr = region->mapping_addr;
    memcpy(mapping->name, path, strlen(path) + 1);
    mapping->allocator = (vemb_v16_shared_region_allocator_t *) (region->mapped_addr + view_offset);
    serverLog(LL_NOTICE,
              "vemb_v16 shared allocator attach ok: backend=%u path=%s offset=%llu view_offset=%llu header_size=%zu mapping_bytes=%zu mapping_addr=%p allocator=%p region_id=%u capacity=%u",
              mapping->backend_type,
              path,
              (unsigned long long)mmap_offset,
              (unsigned long long)view_offset,
              mapping->mapping_size,
              region->mapping_bytes,
              region->mapping_addr,
              mapping->allocator,
              region_id,
              capacity_slots);
    if (wait_allocator_ready(mapping->allocator, region_id, capacity_slots) != 0) {
        serverLog(LL_WARNING,
                  "vemb_v16 shared allocator ready check failed: backend=%u path=%s region_id=%u capacity=%u magic=%08x version=%u existing_region_id=%u existing_capacity=%u",
                  mapping->backend_type,
                  path,
                  region_id,
                  capacity_slots,
                  atomic_load_explicit(&mapping->allocator->magic,
                                       memory_order_acquire),
                  mapping->allocator->version,
                  mapping->allocator->region_id,
                  mapping->allocator->capacity_slots);
        return -1;
    }
    return 0;
}

int vemb_v16_shared_allocator_open(vemb_v16_shared_allocator_mapping_t *mapping,
                                   uint32_t backend_type,
                                   const char *path,
                                   uint64_t mmap_offset,
                                   uint32_t region_id,
                                   uint32_t capacity_slots,
                                   uint32_t is_local) {
    RETURN_IF(!mapping || !path || !path[0] || capacity_slots == 0, -1);
    if (backend_type == VEMB_V16_REGION_UB &&
        (mmap_offset % VEMB_V16_SHARED_ALLOCATOR_ALIGNMENT) != 0) {
        serverLog(LL_WARNING,
                  "vemb_v16 shared allocator ub offset misaligned: path=%s offset=%llu alignment=%u header_size=%zu",
                  path,
                  (unsigned long long)mmap_offset,
                  VEMB_V16_SHARED_ALLOCATOR_ALIGNMENT,
                  sizeof(vemb_v16_shared_region_allocator_t));
        return -1;
    }

    vemb_v16_mapped_region_t region;
    if (vemb_v16_mapped_region_open(&region,
                                    backend_type,
                                    is_local ? VEMB_V16_UB_CACHE_POLICY_CACHEABLE :
                                               VEMB_V16_UB_CACHE_POLICY_NONCACHEABLE,
                                    path,
                                    mmap_offset,
                                    sizeof(vemb_v16_shared_region_allocator_t)) != 0) {
        serverLog(LL_WARNING,
                  "vemb_v16 shared allocator open failed: backend=%u path=%s offset=%llu region_id=%u capacity=%u",
                  backend_type,
                  path,
                  (unsigned long long)mmap_offset,
                  region_id,
                  capacity_slots);
        return -1;
    }

    if (vemb_v16_shared_allocator_attach(mapping,
                                         &region,
                                         0,
                                         backend_type,
                                         path,
                                         mmap_offset,
                                         region_id,
                                         capacity_slots) != 0) {
        vemb_v16_mapped_region_close(&region);
        return -1;
    }
    mapping->owned_mapping = region;
    mapping->mapping = &mapping->owned_mapping;
    mapping->owns_mapping = 1;
    mapping->mapping_addr = mapping->owned_mapping.mapping_addr;
    mapping->mmap_aligned_offset = mapping->owned_mapping.mmap_aligned_offset;
    return 0;
}

int vemb_v16_shared_allocator_reset(uint32_t backend_type,
                                    const char *path,
                                    uint64_t mmap_offset,
                                    uint32_t region_id,
                                    uint32_t capacity_slots,
                                    uint32_t is_local) {
    RETURN_IF(!path || !path[0] || capacity_slots == 0, -1);
    backend_type = backend_type ? backend_type : VEMB_V16_REGION_LOCAL_SHM;
    if (backend_type == VEMB_V16_REGION_LOCAL_SHM)
        return vemb_v16_shared_allocator_unlink(path);

    vemb_v16_mapped_region_t region;
    if (vemb_v16_mapped_region_open(&region,
                                    backend_type,
                                    is_local ? VEMB_V16_UB_CACHE_POLICY_CACHEABLE :
                                               VEMB_V16_UB_CACHE_POLICY_NONCACHEABLE,
                                    path,
                                    mmap_offset,
                                    sizeof(vemb_v16_shared_region_allocator_t)) != 0) {
        serverLog(LL_WARNING,
                  "vemb_v16 shared allocator reset open failed: backend=%u path=%s offset=%llu region_id=%u capacity=%u",
                  backend_type,
                  path,
                  (unsigned long long)mmap_offset,
                  region_id,
                  capacity_slots);
        return -1;
    }
    vemb_v16_shared_region_allocator_t *allocator =
        (vemb_v16_shared_region_allocator_t *)region.mapped_addr;
    serverLog(LL_NOTICE,
              "vemb_v16 shared allocator reset ok: backend=%u path=%s offset=%llu header_size=%zu mapping_bytes=%zu mapping_addr=%p allocator=%p region_id=%u capacity=%u",
              backend_type,
              path,
              (unsigned long long)mmap_offset,
              sizeof(vemb_v16_shared_region_allocator_t),
              region.mapping_bytes,
              region.mapping_addr,
              allocator,
              region_id,
              capacity_slots);
    init_allocator(allocator, region_id, capacity_slots);
    vemb_v16_mapped_region_close(&region);
    return 0;
}

void vemb_v16_shared_allocator_close(vemb_v16_shared_allocator_mapping_t *mapping) {
    RETURN_IF(!mapping);
    if (mapping->owns_mapping)
        vemb_v16_mapped_region_close(&mapping->owned_mapping);
    memset(mapping, 0, sizeof(*mapping));
    mapping->fd = -1;
}

int vemb_v16_shared_allocator_unlink(const char *name) {
    RETURN_IF(!name || name[0] != '/', -1);
    return shm_unlink(name);
}

int vemb_v16_shared_allocator_alloc(vemb_v16_shared_region_allocator_t *allocator,
                                    uint32_t *local_slot) {
    RETURN_IF(!allocator || !local_slot, -1);
    if (atomic_load_explicit(&allocator->full, memory_order_acquire))
        return VEMB_V16_SHARED_ALLOCATOR_FULL;

    uint32_t slot =
        atomic_fetch_add_explicit(&allocator->next_slot, 1,
                                  memory_order_relaxed);
    if (slot < allocator->capacity_slots) {
        *local_slot = slot;
        atomic_fetch_add_explicit(&allocator->used_slots, 1,
                                  memory_order_relaxed);
        return VEMB_V16_SHARED_ALLOCATOR_OK;
    }
    atomic_store_explicit(&allocator->full, 1, memory_order_release);
    return VEMB_V16_SHARED_ALLOCATOR_FULL;
}

uint32_t vemb_v16_shared_allocator_full(const vemb_v16_shared_region_allocator_t *allocator) {
    if (!allocator)
        return 1;
    return atomic_load_explicit((const _Atomic uint32_t *)&allocator->full,
                                memory_order_acquire);
}

uint32_t vemb_v16_shared_allocator_used_slots(const vemb_v16_shared_region_allocator_t *allocator) {
    if (!allocator)
        return 0;
    uint32_t used =
        atomic_load_explicit((const _Atomic uint32_t *)&allocator->used_slots,
                             memory_order_relaxed);
    if (used > allocator->capacity_slots)
        used = allocator->capacity_slots;
    return used;
}
