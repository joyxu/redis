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

    const uint32_t sizes[] = {VEMB_V16_BATCH_REQUEST_SIZE_MAX, 1, 65, 32};
    for (size_t n = 0; n < sizeof(sizes) / sizeof(sizes[0]); n++) {
        context = &contexts[0];
        memset(context, 0xa5, sizeof(*context));
        context->active = 0;
        context->generation = UINT32_MAX;
        context = vemb_v16_batch_context_acquire(contexts, 1, 77, 88, 99,
                                                sizes[n], &slot);
        assert(context && slot == 0 && context->generation == 1);
        assert(context->channel_id == 77 && context->batch_id == 88);
        assert(context->topology_epoch == 99 && context->pending_count == 0);
        assert(context->response.batch_id == 88);
        assert(context->response.topology_epoch == 99);
        assert(context->response.item_count == sizes[n]);
        for (size_t i = 0; i < sizeof(context->completed) / sizeof(uint64_t); i++)
            assert(context->completed[i] == 0);
        /* Acquisition must not touch entries; writers initialize even errors. */
        const unsigned char *bytes = (const unsigned char *)context->response.entries;
        for (size_t i = 0; i < sizeof(context->response.entries); i++)
            assert(bytes[i] == 0xa5);
        context->pending_count = sizes[n];
        for (uint32_t i = sizes[n]; i-- > 0;) {
            context->response.entries[i] = (vemb_v16_resp_t){
                .req_id = i, .op = VEMB_V16_OP_VEMB_HANDLE,
                .status = i % 2 ? VEMB_V16_STATUS_ERR : VEMB_V16_STATUS_OK,
            };
            assert(vemb_v16_batch_context_mark_complete(context, i) == (i ? 1 : 2));
            if (i)
                assert(vemb_v16_batch_context_mark_complete(context, i) == 0);
        }
        uint8_t frame[16384];
        vemb_v16_resp_t entries[VEMB_V16_BATCH_REQUEST_SIZE_MAX];
        batch_response_view_t view;
        size_t length = batch_response_encode(frame, &context->response);
        assert(batch_response_decode(&view, entries, frame, length) == 0);
        assert(view.item_count == sizes[n]);
        for (uint32_t i = 0; i < sizes[n]; i++) {
            assert(entries[i].req_id == i);
            assert(entries[i].status == context->response.entries[i].status);
            assert(entries[i].key_hash == 0 && entries[i].vector_bytes == 0);
        }
        vemb_v16_batch_context_release(context);
    }

    puts("vemb_v16_batch_context_ut: PASS");
    return 0;
}
