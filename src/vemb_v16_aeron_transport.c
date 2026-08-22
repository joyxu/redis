#define _GNU_SOURCE

#include "cpu_relax.h"
#include "vemb_v16_aeron_transport.h"
#include "vemb_v16_client_ring.h"
#include "vemb_v16_proxy_types.h"

#include <stdint.h>

/// TCP/UB Aeron transport implementation.

/// UB/SHM transport: poll client request ring and hand jobs to the scheduler.
int vemb_v16_aeron_poll_shm_requests(vemb_v16_channel_t *ch,
                                     uint32_t proxy_io_worker_id) {
    if (ch->batch_v2) {
        batch_desc_t desc;
        int peek = batch_desc_peek(ch->request_ring, &desc);
        if (peek <= 0)
            return peek;
        if (!batch_desc_is_current(ch->request_ring, &desc))
            return 0;
        if (desc.bytes == 0 || desc.bytes > ch->batch_max_bytes ||
            desc.start % ch->batch_max_bytes + desc.bytes > ch->batch_max_bytes)
            return 0;
        batch_request_view_t view;
        const uint8_t *frame = ch->batch_allocation.request_arena_mapping +
            (desc.start % ch->batch_max_bytes);
        if (batch_request_decode(&view, frame, desc.bytes) != 0 ||
            view.batch_id != desc.batch_id ||
            view.item_count != desc.item_count)
            return 0;
        int rc = vemb_v16_proxy_handle_batch_request(ch, &view,
                                                      proxy_io_worker_id);
        if (rc > 0)
            vemb_v16_client_consume_batch(ch->request_ring, 1);
        return rc;
    }
    vemb_v16_client_ring_t *request_ring = vemb_v16_channel_request_ring(ch);
    uint8_t wire[PROXY_REQUEST_BATCH][VEMB_V16_AERON_REQ_WIRE_MAX_LEN];
    uint32_t wire_lens[PROXY_REQUEST_BATCH];
    vemb_v16_req_t reqs[PROXY_REQUEST_BATCH];
    const vemb_v16_req_t *req_ptrs[PROXY_REQUEST_BATCH];
    uint32_t wire_count = vemb_v16_client_poll_batch(
        request_ring, wire, wire_lens, sizeof(wire[0]), PROXY_REQUEST_BATCH);
    if (wire_count == 0)
        return 0;

    uint32_t req_count = 0;
    for (uint32_t i = 0; i < wire_count; i++) {
        if (vemb_v16_req_decode(&reqs[req_count], wire[i], wire_lens[i]) != 0)
            continue;
        req_ptrs[req_count] = &reqs[req_count];
        req_count++;
    }
    if (req_count == 0)
        return (int)wire_count;
    vemb_v16_proxy_handle_request_ptr_batch(ch,
                                            req_ptrs,
                                            (int)sizeof(reqs[0]),
                                            req_count,
                                            proxy_io_worker_id);
    return (int)wire_count;
}

static int publish_wire_response(vemb_v16_channel_t *ch,
                                 const vemb_v16_resp_t *resp) {
    uint8_t wire[VEMB_V16_AERON_RESP_WIRE_MAX_LEN];
    size_t wire_len = 0;
    if (vemb_v16_resp_encode(wire, sizeof(wire), resp, &wire_len) != 0)
        return -1;
    ring_rc_t rc;
    while ((rc = vemb_v16_client_publish(
                vemb_v16_channel_response_ring(ch), wire,
                (uint32_t)wire_len)) == RING_ERR_FULL &&
           vemb_v16_channel_proxy_running(ch) && vemb_v16_channel_active(ch)) {
        vemb_v16_channel_add_proxy_response_ring_full(ch, 1);
        cpu_relax();
    }
    return rc == RING_OK && vemb_v16_channel_active(ch) ?
        RING_OK : RING_ERR_INVALID;
}

/// UB/SHM transport: publish one response to the client response ring.
int vemb_v16_aeron_publish_response(vemb_v16_channel_t *ch,
                                    const vemb_v16_resp_t *resp) {
    return publish_wire_response(ch, resp);
}

int vemb_v16_aeron_publish_response_batch(vemb_v16_channel_t *ch,
                                          const vemb_v16_resp_t *resps,
                                          uint32_t count) {
    uint8_t wire[PROXY_RESPONSE_BATCH][VEMB_V16_AERON_RESP_WIRE_MAX_LEN];
    const void *wire_ptrs[PROXY_RESPONSE_BATCH];
    uint32_t wire_lens[PROXY_RESPONSE_BATCH];
    if (count > PROXY_RESPONSE_BATCH)
        return -1;
    for (uint32_t i = 0; i < count; i++) {
        size_t wire_len = 0;
        if (vemb_v16_resp_encode(wire[i], sizeof(wire[i]), &resps[i],
                                 &wire_len) != 0)
            return -1;
        wire_ptrs[i] = wire[i];
        wire_lens[i] = (uint32_t)wire_len;
    }
    while (vemb_v16_client_publish_ptr_batch(
               vemb_v16_channel_response_ring(ch), wire_ptrs, wire_lens,
               count) != 0 &&
           vemb_v16_channel_proxy_running(ch) &&
           vemb_v16_channel_active(ch)) {
        vemb_v16_channel_add_proxy_response_ring_full(ch, 1);
        cpu_relax();
    }
    return vemb_v16_channel_active(ch) ? 0 : -1;
}

int vemb_v16_aeron_publish_batch_response(
    vemb_v16_channel_t *ch, const batch_response_t *response) {
    if (!ch || !ch->batch_v2 || !response)
        return RING_ERR_INVALID;
    uint8_t wire[VEMB_V16_BATCH_MAX_BYTES_MAX];
    size_t wire_len = batch_response_encode(wire, response);
    if (wire_len > ch->batch_max_bytes)
        return RING_ERR_INVALID;
    uint32_t spins = 0;
    int rc;
    while ((rc = batch_arena_publish(
                ch->response_ring, ch->batch_allocation.response_arena_mapping,
                ch->batch_max_bytes, &ch->batch_response_producer, wire,
                (uint32_t)wire_len, response->item_count,
                response->batch_id)) == RING_ERR_FULL &&
           vemb_v16_channel_proxy_running(ch) && vemb_v16_channel_active(ch)) {
        vemb_v16_channel_add_proxy_response_ring_full(ch, 1);
        cpu_relax();
        spins++;
    }
    (void)spins;
    return rc == RING_OK && vemb_v16_channel_active(ch) ?
        RING_OK : RING_ERR_INVALID;
}
