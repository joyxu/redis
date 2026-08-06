#include "vemb_v16_batch_context.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(void) {
    vemb_v16_batch_context_t contexts[
        VEMB_V16_BATCH_CONTEXTS_PER_PROXY_WORKER];
    memset(contexts, 0, sizeof(contexts));

    for (uint32_t i = 0; i < VEMB_V16_BATCH_CONTEXTS_PER_PROXY_WORKER; i++) {
        uint32_t slot = UINT32_MAX;
        vemb_v16_batch_context_t *context = vemb_v16_batch_context_acquire(
            contexts, VEMB_V16_BATCH_CONTEXTS_PER_PROXY_WORKER,
            i == 3 ? 200 : 100, i + 1,
            9, 3, &slot);
        assert(context == &contexts[i]);
        assert(slot == i && context->generation == 1 && context->active);
    }
    uint32_t slot = UINT32_MAX;
    assert(vemb_v16_batch_context_acquire(
               contexts, VEMB_V16_BATCH_CONTEXTS_PER_PROXY_WORKER, 100, 5,
               9, 3, &slot) == NULL);

    vemb_v16_batch_context_t *context = &contexts[0];
    context->pending_count = 3;
    uint64_t token0 = vemb_v16_batch_token_make(7, 0, context->generation, 0);
    uint64_t token1 = vemb_v16_batch_token_make(7, 0, context->generation, 1);
    uint64_t token2 = vemb_v16_batch_token_make(7, 0, context->generation, 2);
    uint32_t item_index = UINT32_MAX;
    assert(vemb_v16_batch_context_lookup(
               contexts, VEMB_V16_BATCH_CONTEXTS_PER_PROXY_WORKER, 7, 100,
               token2, &item_index) == context && item_index == 2);
    assert(vemb_v16_batch_context_mark_complete(context, item_index) == 1);
    assert(vemb_v16_batch_context_lookup(
               contexts, VEMB_V16_BATCH_CONTEXTS_PER_PROXY_WORKER, 7, 100,
               token0, &item_index) == context && item_index == 0);
    assert(vemb_v16_batch_context_mark_complete(context, item_index) == 1);
    assert(vemb_v16_batch_context_mark_complete(context, item_index) == 0);
    assert(vemb_v16_batch_context_lookup(
               contexts, VEMB_V16_BATCH_CONTEXTS_PER_PROXY_WORKER, 7, 100,
               token1, &item_index) == context && item_index == 1);
    assert(vemb_v16_batch_context_mark_complete(context, item_index) == 2);

    uint32_t old_generation = context->generation;
    vemb_v16_batch_context_release(context);
    assert(vemb_v16_batch_context_acquire(
               contexts, VEMB_V16_BATCH_CONTEXTS_PER_PROXY_WORKER, 100, 6,
               10, 1, &slot) == context);
    assert(slot == 0 && context->generation == old_generation + 1);
    assert(vemb_v16_batch_context_lookup(
               contexts, VEMB_V16_BATCH_CONTEXTS_PER_PROXY_WORKER, 7, 100,
               token0, &item_index) == NULL);
    assert(vemb_v16_batch_context_lookup(
               contexts, VEMB_V16_BATCH_CONTEXTS_PER_PROXY_WORKER, 8, 100,
               vemb_v16_batch_token_make(8, 0, context->generation, 0),
               &item_index) == context);

    vemb_v16_batch_context_abort_channel(
        contexts, VEMB_V16_BATCH_CONTEXTS_PER_PROXY_WORKER, 100);
    assert(!contexts[0].active && !contexts[1].active && !contexts[2].active &&
           contexts[3].active && contexts[3].channel_id == 200);

    puts("vemb_v16_batch_context_ut: PASS");
    return 0;
}
