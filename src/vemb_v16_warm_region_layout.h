#ifndef __VEMB_V16_WARM_REGION_LAYOUT_H
#define __VEMB_V16_WARM_REGION_LAYOUT_H

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>

#include "vemb_v16_mapped_region.h"

#define VEMB_V16_WARM_REGION_LAYOUT_MAGIC 0x5631414cu /* V1AL */
#define VEMB_V16_WARM_REGION_LAYOUT_INITIALIZING 0x56314149u /* V1AI */
#define VEMB_V16_WARM_REGION_LAYOUT_VERSION 2u
#define VEMB_V16_WARM_REGION_LAYOUT_NAME_MAX 256u
#define VEMB_V16_WARM_REGION_LAYOUT_ALIGNMENT 64u

#define VEMB_V16_WARM_REGION_SLOT_ACQUIRE_OK 0
#define VEMB_V16_WARM_REGION_SLOT_ACQUIRE_FULL 1

typedef enum vemb_v16_warm_slot_state {
    VEMB_V16_WARM_SLOT_FREE = 0,
    VEMB_V16_WARM_SLOT_FILLING = 1,
    VEMB_V16_WARM_SLOT_READY = 2,
    VEMB_V16_WARM_SLOT_EVICTING = 3,
} vemb_v16_warm_slot_state_t;

typedef enum vemb_v16_warm_slot_cold_state {
    VEMB_V16_WARM_SLOT_COLD_NONE = 0,
    VEMB_V16_WARM_SLOT_COLD_PENDING = 1,
    VEMB_V16_WARM_SLOT_COLD_COMMITTED = 2,
} vemb_v16_warm_slot_cold_state_t;

typedef struct vemb_v16_warm_slot_meta {
    /* low 3 bits: slot state; bits 3..: seqlock version (odd = writing) */
    _Atomic uint64_t state_version;
    _Atomic uint64_t owner_generation;
    uint64_t key_hash;
    uint64_t key_fingerprint;
} vemb_v16_warm_slot_meta_t;

_Static_assert(sizeof(vemb_v16_warm_slot_meta_t) ==
                   32u,
               "vemb_v16_warm_slot_meta_t must be 32 bytes");

#define VEMB_V16_WARM_SLOT_STATE_MASK UINT64_C(0x7)
#define VEMB_V16_WARM_SLOT_SEQ_SHIFT 3u

static inline uint32_t vemb_v16_warm_slot_state(uint64_t state_version) {
    return (uint32_t)(state_version & VEMB_V16_WARM_SLOT_STATE_MASK);
}

static inline uint64_t vemb_v16_warm_slot_seq(uint64_t state_version) {
    return state_version >> VEMB_V16_WARM_SLOT_SEQ_SHIFT;
}

static inline uint64_t vemb_v16_warm_slot_pack(uint64_t seq,
                                                uint32_t state) {
    return (seq << VEMB_V16_WARM_SLOT_SEQ_SHIFT) |
           ((uint64_t)state & VEMB_V16_WARM_SLOT_STATE_MASK);
}

/*
 * Shared warm-region layout:
 *   [warm_region_header][slot_meta[capacity_slots]][payload bytes...]
 */
typedef struct vemb_v16_warm_region_header {
    _Atomic uint32_t magic;
    uint32_t version;
    uint32_t region_id;
    uint32_t capacity_slots;
    uint32_t value_size;
    uint64_t region_bytes;
    uint8_t reserved[32];
} vemb_v16_warm_region_header_t;

_Static_assert(sizeof(vemb_v16_warm_region_header_t) ==
                   VEMB_V16_WARM_REGION_LAYOUT_ALIGNMENT,
               "vemb_v16_warm_region_header_t must be one aligned UB cacheline");

int vemb_v16_warm_region_layout_name_from_region_path(const char *region_path,
                                                    uint32_t region_id,
                                                    char *out,
                                                    size_t out_len);
int vemb_v16_warm_region_layout_reset(uint32_t backend_type,
                                      uint32_t cache_policy,
                                      const char *path,
                                      uint64_t mmap_offset,
                                      uint32_t region_id,
                                    uint32_t capacity_slots);
size_t vemb_v16_warm_region_layout_bytes(uint32_t capacity_slots);
vemb_v16_warm_slot_meta_t *vemb_v16_warm_region_slot_meta(void *base);
#endif
