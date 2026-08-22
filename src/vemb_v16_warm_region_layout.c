#define _GNU_SOURCE

#include "vemb_v16_hash.h"
#include "vemb_v16_warm_region_layout.h"
#include "macro.h"
#include "vemb_v16_log.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>

static size_t slot_meta_bytes(uint32_t capacity_slots) {
    return (size_t)capacity_slots * sizeof(vemb_v16_warm_slot_meta_t);
}

size_t vemb_v16_warm_region_layout_bytes(uint32_t capacity_slots) {
    return sizeof(vemb_v16_warm_region_header_t) + slot_meta_bytes(capacity_slots);
}

vemb_v16_warm_slot_meta_t *vemb_v16_warm_region_slot_meta(void *base) {
    return (vemb_v16_warm_slot_meta_t *)(
        (uint8_t *)base + sizeof(vemb_v16_warm_region_header_t));
}

static void init_slot_meta(vemb_v16_warm_region_header_t *header,
                           uint32_t region_id,
                           uint32_t capacity_slots) {
    vemb_v16_warm_slot_meta_t *slots =
        vemb_v16_warm_region_slot_meta(header);
    memset(slots, 0, slot_meta_bytes(capacity_slots));
    for (uint32_t i = 0; i < capacity_slots; i++) {
        slots[i].region_id = region_id;
        slots[i].local_slot = i;
        atomic_init(&slots[i].state, VEMB_V16_WARM_SLOT_FREE);
        atomic_init(&slots[i].owner_generation, 0);
        atomic_init(&slots[i].write_seq, 0);
        atomic_init(&slots[i].last_access_ns, 0);
        atomic_init(&slots[i].clock_bit, 0);
        atomic_init(&slots[i].cold_state, VEMB_V16_WARM_SLOT_COLD_NONE);
    }
}

static int warm_region_layout_unlink(const char *name) {
    RETURN_IF(name[0] != '/', -1);
    return shm_unlink(name);
}

int vemb_v16_warm_region_layout_name_from_region_path(const char *region_path,
                                                    uint32_t region_id,
                                                    char *out,
                                                    size_t out_len) {
    RETURN_IF(!region_path[0] || out_len == 0, -1);
    if (region_path[0] == '/' &&
        strchr(region_path + 1, '/') == NULL &&
        strlen(region_path) + strlen(".alloc") < out_len &&
        strlen(region_path) + strlen(".alloc") < 31u) {
        snprintf(out, out_len, "%s.alloc", region_path);
        return 0;
    }
    uint64_t h = vemb_v16_fnv1a64(region_path);
    int n = snprintf(out, out_len,
                     "/v16a_%08x_%08x",
                     region_id,
                     (unsigned)(h ^ (h >> 32)));
    RETURN_IF(n <= 0 || (size_t)n >= out_len, -1);
    return 0;
}

static void init_header(vemb_v16_warm_region_header_t *header,
                        uint32_t region_id,
                        uint32_t capacity_slots) {
    memset(header, 0, sizeof(*header));
    header->region_id = region_id;
    header->capacity_slots = capacity_slots;
    header->version = VEMB_V16_WARM_REGION_LAYOUT_VERSION;
    init_slot_meta(header, region_id, capacity_slots);
    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&header->magic,
                          VEMB_V16_WARM_REGION_LAYOUT_MAGIC,
                          memory_order_release);
}

int vemb_v16_warm_region_layout_reset(uint32_t backend_type,
                                      uint32_t cache_policy,
                                      const char *path,
                                      uint64_t mmap_offset,
                                      uint32_t region_id,
                                      uint32_t capacity_slots) {
    RETURN_IF(!path[0] || capacity_slots == 0, -1);
    backend_type = backend_type ? backend_type : VEMB_V16_REGION_LOCAL_SHM;
    if (backend_type == VEMB_V16_REGION_LOCAL_SHM)
        return warm_region_layout_unlink(path);

    vemb_v16_mapped_region_t region;
    if (vemb_v16_mapped_region_open(&region,
                                    backend_type,
                                    cache_policy,
                                    path,
                                    mmap_offset,
                                    vemb_v16_warm_region_layout_bytes(
                                        capacity_slots)) != 0) {
        serverLog(LL_WARNING,
                  "vemb_v16 warm region layout reset open failed: backend=%u path=%s offset=%llu region_id=%u capacity=%u",
                  backend_type,
                  path,
                  (unsigned long long)mmap_offset,
                  region_id,
                  capacity_slots);
        return -1;
    }
    vemb_v16_warm_region_header_t *header = (vemb_v16_warm_region_header_t *)region.mapped_addr;
    serverLog(LL_NOTICE,
              "vemb_v16 warm region layout reset ok: backend=%u path=%s offset=%llu header_size=%zu mapping_bytes=%zu mapping_addr=%p header=%p region_id=%u capacity=%u",
              backend_type,
              path,
              (unsigned long long)mmap_offset,
              vemb_v16_warm_region_layout_bytes(capacity_slots),
              region.mapping_bytes,
              region.mapping_addr,
              header,
              region_id,
              capacity_slots);
    init_header(header, region_id, capacity_slots);
    vemb_v16_mapped_region_close(&region);
    return 0;
}
