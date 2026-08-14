#include "vemb_v16_client_ring.h"
#include "vemb_v16_protocol.h"

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

int main(void) {
    const uint32_t slot_size = vemb_v16_aeron_req_slot_size(300);
    assert(slot_size % CACHELINE_SIZE == 0);

    size_t ring_bytes = vemb_v16_client_ring_bytes(slot_size);
    void *storage = NULL;
    assert(posix_memalign(&storage, CACHELINE_SIZE, ring_bytes) == 0);
    memset(storage, 0, ring_bytes);
    vemb_v16_client_ring_t *ring = storage;
    vemb_v16_client_ring_init(ring, slot_size);
    assert(ring->slots_off % CACHELINE_SIZE == 0);
    assert((RING_SLOT_META_BYTES + ring->slot_size) % CACHELINE_SIZE == 0);

    const uint8_t first[] = {1, 2, 3};
    const uint8_t second[] = {4, 5, 6, 7, 8};
    const void *frames[] = {first, second};
    const uint32_t lengths[] = {sizeof(first), sizeof(second)};
    assert(vemb_v16_client_publish_ptr_batch(ring, frames, lengths, 2) == 0);

    uint8_t output[2][64] = {{0}};
    uint32_t output_lengths[2] = {0};
    assert(vemb_v16_client_poll_batch(ring, output, output_lengths,
                                              sizeof(output[0]), 2) == 2);
    assert(output_lengths[0] == sizeof(first));
    assert(output_lengths[1] == sizeof(second));
    assert(memcmp(output[0], first, sizeof(first)) == 0);
    assert(memcmp(output[1], second, sizeof(second)) == 0);
    free(storage);

    vemb_v16_req_t req = {0};
    req.op = VEMB_V16_OP_VEMB_HANDLE;
    req.req_id = 17;
    req.channel_id = 99;
    req.dim = 300;
    req.vector_bytes = 300 * sizeof(float);
    req.key_len = 5;
    memcpy(req.key, "hello", req.key_len);
    uint8_t req_wire[VEMB_V16_AERON_REQ_WIRE_MAX_LEN];
    size_t req_wire_len = 0;
    assert(vemb_v16_req_encode(req_wire, sizeof(req_wire), &req,
                               &req_wire_len) == 0);
    vemb_v16_req_t decoded_req;
    assert(vemb_v16_req_decode(&decoded_req, req_wire, req_wire_len) == 0);
    assert(decoded_req.req_id == req.req_id);
    assert(decoded_req.channel_id == req.channel_id);
    assert(decoded_req.key_len == req.key_len);
    assert(memcmp(decoded_req.key, req.key, req.key_len) == 0);

    vemb_v16_resp_t resp = {
        .status = VEMB_V16_STATUS_OK,
        .op = VEMB_V16_OP_VEMB_HANDLE,
        .req_id = req.req_id,
        .vector_offset = 4096,
        .vector_bytes = req.vector_bytes,
        .region_id = 3,
        .local_slot = 7,
        .owner_generation = 11,
    };
    uint8_t resp_wire[VEMB_V16_AERON_RESP_WIRE_MAX_LEN];
    size_t resp_wire_len = 0;
    assert(vemb_v16_resp_encode(resp_wire, sizeof(resp_wire), &resp,
                                &resp_wire_len) == 0);
    assert(resp_wire_len <= VEMB_V16_AERON_RESP_WIRE_MAX_LEN);
    vemb_v16_resp_t decoded_resp;
    assert(vemb_v16_resp_decode(&decoded_resp, resp_wire, resp_wire_len) == 0);
    assert(decoded_resp.req_id == resp.req_id);
    assert(decoded_resp.vector_offset == resp.vector_offset);
    assert(decoded_resp.vector_bytes == resp.vector_bytes);
    assert(decoded_resp.region_id == resp.region_id);
    return 0;
}
