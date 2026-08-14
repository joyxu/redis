#ifndef __VEMB_V16_DATAPLANE_H
#define __VEMB_V16_DATAPLANE_H

#include "vemb_v16_protocol.h"

#include <stdatomic.h>
#include <stdint.h>

typedef enum vemb_v16_job_kind {
    VEMB_V16_JOB_KIND_BASE = 1,
    VEMB_V16_JOB_KIND_READ = 2,
    VEMB_V16_JOB_KIND_VSIM_KEY_KEY = 3,
    VEMB_V16_JOB_KIND_INLINE_VECTOR = 4,
} vemb_v16_job_kind_t;

typedef struct vemb_v16_job_base {
    uint8_t kind;
    uint8_t op;
    uint16_t flags;
    uint32_t req_id;
    uint32_t channel_index;
    uint32_t reserved0;
    uint64_t channel_id;
    uint64_t key_hash;
    uint64_t topology_epoch;
    uint64_t batch_token;
} vemb_v16_job_base_t;

typedef enum vemb_v16_job_pool_type {
    VEMB_V16_JOB_POOL_READ = 0,
    VEMB_V16_JOB_POOL_VSIM_KEY_KEY = 1,
    VEMB_V16_JOB_POOL_INLINE_VECTOR = 2,
    VEMB_V16_JOB_POOL_COUNT = 3,
} vemb_v16_job_pool_type_t;

typedef enum vemb_v16_job_slot_state {
    VEMB_V16_JOB_SLOT_FREE = 0,
    VEMB_V16_JOB_SLOT_RESERVED = 1,
    VEMB_V16_JOB_SLOT_PUBLISHED = 2,
    VEMB_V16_JOB_SLOT_RUNNING = 3,
} vemb_v16_job_slot_state_t;

typedef struct vemb_v16_job_ref {
    uint16_t proxy_worker_id;
    uint16_t pool_type;
    uint32_t slot_id;
    uint32_t generation;
    uint32_t req_id;
    uint8_t op;
    uint16_t reserved0;
} vemb_v16_job_ref_t;

typedef struct vemb_v16_job_return {
    uint16_t pool_type;
    uint16_t reserved0;
    uint32_t slot_id;
    uint32_t generation;
} vemb_v16_job_return_t;

typedef struct vemb_v16_vemb_job {
    vemb_v16_job_base_t base;
    uint32_t key_len;
    uint32_t dim;
    uint32_t vector_bytes;
    uint32_t reserved1;
    char key[VEMB_V16_MAX_KEY_LEN];
} vemb_v16_vemb_job_t;

typedef struct vemb_v16_vsim_key_key_job {
    vemb_v16_job_base_t base;
    uint32_t key_len;
    uint32_t key2_len;
    uint32_t dim;
    uint32_t vector_bytes;
    uint64_t key2_hash;
    char key[VEMB_V16_MAX_KEY_LEN];
    char key2[VEMB_V16_MAX_KEY_LEN];
} vemb_v16_vsim_key_key_job_t;

typedef struct vemb_v16_vadd_job {
    vemb_v16_job_base_t base;
    uint32_t key_len;
    uint32_t dim;
    uint32_t vector_bytes;
    uint32_t reserved1;
    char key[VEMB_V16_MAX_KEY_LEN];
    float vector[VEMB_V16_MAX_DIM];
} vemb_v16_vadd_job_t;

typedef struct vemb_v16_job_slot_hdr {
    uint8_t op;
    uint8_t reserved0;
    atomic_uint state;
    uint32_t generation;
} vemb_v16_job_slot_hdr_t;

typedef struct vemb_v16_job_slot {
    vemb_v16_job_slot_hdr_t hdr;
    union {
        vemb_v16_job_base_t base_job;
        vemb_v16_vemb_job_t read_job;
        vemb_v16_vsim_key_key_job_t vsim_job;
        vemb_v16_vadd_job_t inline_job;
    } u;
} vemb_v16_job_slot_t;

typedef struct vemb_v16_job_pool {
    uint16_t pool_type;
    uint16_t reserved1;
    uint32_t slot_count;
    uint32_t slot_stride;
    uint32_t free_count;
    uint32_t reserved2;
    void *slots;
    uint32_t *free_stack;
} vemb_v16_job_pool_t;

typedef struct vemb_v16_completion {
    uint8_t status;
    uint8_t op;
    uint16_t flags;
    uint32_t req_id;
    uint32_t channel_index;
    uint32_t dim;
    uint64_t channel_id;
    uint64_t batch_token;
    uint64_t key_hash;
    uint64_t vector_offset;
    uint32_t vector_bytes;
    uint32_t region_id;
    uint32_t local_slot;
    uint32_t reserved1;
    uint64_t owner_generation;
    uint32_t redirect_owner;
    uint32_t reserved_redirect;
    uint8_t *inline_vector;
    uint32_t inline_vector_bytes;
    uint32_t reserved2;
    float score;
} vemb_v16_completion_t;

#endif
