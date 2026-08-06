#ifndef VEMB_V16_BATCH_CONTEXT_H
#define VEMB_V16_BATCH_CONTEXT_H

#include "vemb_v16_batch_ring.h"

#include <stdint.h>
#include <string.h>

#define VEMB_V16_BATCH_CONTEXTS_PER_PROXY_WORKER 4u

typedef struct vemb_v16_batch_context {
    uint64_t channel_id;
    uint64_t batch_id;
    uint64_t topology_epoch;
    uint32_t generation;
    uint32_t item_count;
    uint32_t pending_count;
    uint32_t active;
    uint64_t completed[(VEMB_V16_BATCH_REQUEST_SIZE_MAX + 63u) / 64u];
    batch_response_t response;
} vemb_v16_batch_context_t;

static inline uint64_t vemb_v16_batch_token_make(uint32_t worker_id,
                                                   uint32_t context_slot,
                                                   uint32_t generation,
                                                   uint32_t item_index) {
    return ((uint64_t)generation << 32) | ((uint64_t)worker_id << 16) |
        ((uint64_t)context_slot << 8) | item_index;
}

static inline int vemb_v16_batch_token_decode(uint64_t token,
                                               uint32_t *worker_id,
                                               uint32_t *context_slot,
                                               uint32_t *generation,
                                               uint32_t *item_index) {
    if (token == 0)
        return -1;
    *worker_id = (uint32_t)((token >> 16) & UINT16_MAX);
    *context_slot = (uint32_t)((token >> 8) & UINT8_MAX);
    *generation = (uint32_t)(token >> 32);
    *item_index = (uint32_t)(token & UINT8_MAX);
    return 0;
}

static inline vemb_v16_batch_context_t *vemb_v16_batch_context_acquire(
    vemb_v16_batch_context_t *contexts, uint32_t context_count,
    uint64_t channel_id, uint64_t batch_id, uint64_t topology_epoch,
    uint32_t item_count, uint32_t *out_slot) {
    for (uint32_t i = 0; i < context_count; i++) {
        vemb_v16_batch_context_t *context = &contexts[i];
        if (context->active)
            continue;
        uint32_t generation = context->generation + 1u;
        if (generation == 0)
            generation = 1;
        memset(context, 0, sizeof(*context));
        context->channel_id = channel_id;
        context->batch_id = batch_id;
        context->topology_epoch = topology_epoch;
        context->generation = generation;
        context->item_count = item_count;
        context->active = 1;
        context->response = (batch_response_t){
            .batch_id = batch_id,
            .topology_epoch = topology_epoch,
            .item_count = item_count,
        };
        *out_slot = i;
        return context;
    }
    return NULL;
}

static inline void vemb_v16_batch_context_release(
    vemb_v16_batch_context_t *context) {
    context->active = 0;
}

static inline void vemb_v16_batch_context_abort_channel(
    vemb_v16_batch_context_t *contexts, uint32_t context_count,
    uint64_t channel_id) {
    for (uint32_t i = 0; i < context_count; i++) {
        if (contexts[i].active && contexts[i].channel_id == channel_id)
            vemb_v16_batch_context_release(&contexts[i]);
    }
}

static inline vemb_v16_batch_context_t *vemb_v16_batch_context_lookup(
    vemb_v16_batch_context_t *contexts, uint32_t context_count,
    uint32_t expected_worker_id, uint64_t channel_id, uint64_t token,
    uint32_t *out_item_index) {
    uint32_t worker_id, context_slot, generation, item_index;
    if (vemb_v16_batch_token_decode(token, &worker_id, &context_slot,
                                    &generation, &item_index) != 0 ||
        worker_id != expected_worker_id || context_slot >= context_count)
        return NULL;
    vemb_v16_batch_context_t *context = &contexts[context_slot];
    if (!context->active || context->generation != generation ||
        context->channel_id != channel_id || item_index >= context->item_count)
        return NULL;
    *out_item_index = item_index;
    return context;
}

/* -1: invalid item, 0: duplicate, 1: accepted but incomplete, 2: final. */
static inline int vemb_v16_batch_context_mark_complete(
    vemb_v16_batch_context_t *context, uint32_t item_index) {
    if (!context->active || item_index >= context->item_count ||
        context->pending_count == 0)
        return -1;
    uint64_t mask = UINT64_C(1) << (item_index & 63u);
    uint64_t *word = &context->completed[item_index >> 6];
    if (*word & mask)
        return 0;
    *word |= mask;
    context->pending_count--;
    return context->pending_count == 0 ? 2 : 1;
}

#endif
