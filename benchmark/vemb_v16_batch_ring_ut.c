#include "vemb_v16_batch_ring.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(void) {
    const char *keys[] = {"a", "key-b"};
    const uint16_t lens[] = {1, 5};
    uint8_t frame[256];
    uint32_t key_bytes = lens[0] + lens[1];
    size_t frame_len = batch_request_encoded_len(key_bytes, 2);
    batch_request_encode(frame, 7, 9, keys, lens, 2, key_bytes);
    batch_request_view_t view;
    assert(batch_request_decode(&view, frame, frame_len) == 0);
    assert(view.batch_id == 7 && view.topology_epoch == 9 && view.item_count == 2);
    assert(batch_request_key_len_at(&view, 0) == 1 &&
           batch_request_key_len_at(&view, 1) == 5);
    assert(memcmp(view.keys, "akey-b", 6) == 0);
    frame[frame_len - 1] ^= 1;
    assert(batch_request_decode(&view, frame, frame_len) != 0);
    frame[frame_len - 1] ^= 1;

    uint8_t descriptor_storage[vemb_v16_client_ring_bytes(64)];
    uint8_t descriptor_arena[128];
    vemb_v16_client_ring_t *descriptor_ring =
        (vemb_v16_client_ring_t *)descriptor_storage;
    vemb_v16_client_ring_init(descriptor_ring, 64);
    batch_arena_producer_t producer;
    batch_arena_producer_init(&producer);
    frame[frame_len - 1] ^= 1;
    assert(batch_arena_publish(descriptor_ring, descriptor_arena,
                                         sizeof(descriptor_arena), &producer,
                                         frame, (uint32_t)frame_len, 2, 7) == RING_OK);
    assert(batch_request_decode(&view, descriptor_arena,
                                         frame_len) == 0);
    frame[frame_len - 1] ^= 1;

    batch_response_t response = {
        .batch_id = 7,
        .topology_epoch = 9,
        .item_count = 2,
    };
    response.entries[0] = (vemb_v16_resp_t){
        .status = VEMB_V16_STATUS_NOT_FOUND,
        .op = VEMB_V16_OP_VEMB_HANDLE,
        .req_id = 99,
    };
    response.entries[1] = (vemb_v16_resp_t){
        .status = VEMB_V16_STATUS_OK,
        .op = VEMB_V16_OP_VEMB_HANDLE,
        .req_id = 100,
        .vector_bytes = 16,
        .vector_offset = 4096,
        .region_id = 1,
        .local_slot = 2,
        .owner_generation = 3,
    };
    frame_len = batch_response_encode(frame, &response);
    batch_response_view_t response_view;
    vemb_v16_resp_t decoded[VEMB_V16_BATCH_REQUEST_SIZE_MAX];
    assert(batch_response_decode(&response_view, decoded, frame, frame_len) == 0);
    assert(response_view.batch_id == 7 && response_view.topology_epoch == 9 &&
           response_view.item_count == 2 && decoded[0].req_id == 0 &&
           decoded[1].req_id == 1 && decoded[1].vector_offset == 4096);
    frame[frame_len - 1] ^= 1;
    assert(batch_response_decode(&response_view, decoded, frame, frame_len) != 0);

    uint8_t descriptor_storage2[vemb_v16_client_ring_bytes(64)];
    uint8_t descriptor_arena2[64];
    vemb_v16_client_ring_t *descriptor_ring2 =
        (vemb_v16_client_ring_t *)descriptor_storage2;
    vemb_v16_client_ring_init(descriptor_ring2, 64);
    batch_arena_producer_t producer2;
    batch_arena_producer_init(&producer2);
    uint8_t first[40] = {3};
    uint8_t second[40] = {4};
    assert(batch_arena_publish(descriptor_ring2, descriptor_arena2,
                                         sizeof(descriptor_arena2), &producer2,
                                         first, sizeof(first), 1, 7) == RING_OK);
    assert(batch_arena_publish(descriptor_ring2, descriptor_arena2,
                                         sizeof(descriptor_arena2), &producer2,
                                         second, sizeof(second), 1, 8) == RING_ERR_FULL);
    batch_desc_t shared_desc;
    batch_desc_t *shared_slot = (batch_desc_t *)(
        ring_slot_base(descriptor_ring2, 0) +
        RING_SLOT_META_BYTES);
    assert(shared_slot->sequence == 1);
    shared_slot->sequence = 0;
    assert(batch_desc_peek(descriptor_ring2, &shared_desc) == 1);
    assert(!batch_desc_is_current(descriptor_ring2, &shared_desc));
    shared_slot->sequence = 1;
    assert(batch_desc_peek(descriptor_ring2, &shared_desc) == 1);
    assert(batch_desc_is_current(descriptor_ring2, &shared_desc));
    assert(shared_desc.bytes == sizeof(first) &&
           shared_desc.batch_id == 7 &&
           descriptor_arena2[shared_desc.start % sizeof(descriptor_arena2)] == 3);
    vemb_v16_client_consume_batch(descriptor_ring2, 1);
    assert(batch_arena_publish(descriptor_ring2, descriptor_arena2,
                                         sizeof(descriptor_arena2), &producer2,
                                         second, sizeof(second), 1, 8) == RING_OK);
    assert(batch_desc_peek(descriptor_ring2, &shared_desc) == 1);
    assert(shared_desc.start == sizeof(descriptor_arena2));
    assert(shared_desc.batch_id == 8);
    assert(descriptor_arena2[0] == 4);
    puts("vemb_v16_batch_ring_ut: PASS");
    return 0;
}
