#include "vemb_v16_batch_ring.h"
#include "cpu_relax.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

static void test_reclaim(void) {
    size_t ring_bytes = vemb_v16_client_ring_bytes(64);
    vemb_v16_client_ring_t *ring = aligned_alloc(64, ring_bytes);
    assert(ring);
    uint8_t arena[32768], before[sizeof(arena)], frame[64] = {0};
    batch_arena_producer_t producer;
    vemb_v16_client_ring_init(ring, 64);
    batch_arena_producer_init(&producer, ring);
    for (uint32_t round = 0; round < 4; round++) {
        for (uint32_t i = 0; i < VEMB_V16_CLIENT_RING_SIZE; i++)
            assert(batch_arena_publish(ring, arena, sizeof(arena), &producer,
                frame, sizeof(frame), 1, producer.descriptor_tail + 1) == RING_OK);
        uint64_t tail = producer.descriptor_tail;
        memcpy(before, arena, sizeof(arena));
        assert(batch_arena_publish(ring, arena, sizeof(arena), &producer,
            frame, sizeof(frame), 1, tail + 1) == RING_ERR_FULL);
        assert(producer.descriptor_tail == tail && atomic_load(&ring->tail) == tail);
        assert(memcmp(before, arena, sizeof(arena)) == 0);
        vemb_v16_client_consume_batch(ring, VEMB_V16_CLIENT_RING_SIZE);
        /* Reclaim must not depend on rereading any released descriptor. */
        memset((uint8_t *)ring + ring->slots_off, 0xa5, ring_bytes - ring->slots_off);
    }
    vemb_v16_client_ring_init(ring, 64);
    batch_arena_producer_init(&producer, ring);
    assert(batch_arena_publish(ring, arena, 64, &producer, frame, 16, 1, 1) == RING_OK);
    vemb_v16_client_consume_batch(ring, 1);
    assert(batch_arena_publish(ring, arena, 64, &producer, frame, 64, 1, 2) == RING_OK);
    assert(producer.arena_tail == 128 && producer.arena_head == 64);
    assert(batch_arena_publish(ring, arena, 64, &producer, frame, 65, 1, 3) == RING_ERR_INVALID);
    free(ring);
}

typedef struct stress_state {
    vemb_v16_client_ring_t *ring;
    uint8_t arena[8192];
} stress_state_t;

#define STRESS_FRAMES 200000u

static void *stress_consumer(void *arg) {
    stress_state_t *state = arg;
    for (uint64_t id = 1; id <= STRESS_FRAMES; id++) {
        batch_desc_t desc;
        while (!batch_desc_peek(state->ring, &desc))
            cpu_relax();
        assert(batch_desc_is_current(state->ring, &desc));
        assert(desc.batch_id == id && desc.sequence == id && desc.item_count == 1);
        assert(desc.bytes == 16 + (id * 97) % 4096);
        uint32_t offset = desc.start % sizeof(state->arena);
        assert(offset + desc.bytes <= sizeof(state->arena));
        const uint8_t *body = state->arena + offset;
        for (uint32_t i = 0; i < desc.bytes - VEMB_V16_BATCH_COMMIT_BYTES; i++)
            assert(body[i] == (uint8_t)id);
        const uint8_t *commit = body + desc.bytes - VEMB_V16_BATCH_COMMIT_BYTES;
        assert(vemb_v16_proto_get_u64(&commit) == id);
        vemb_v16_client_consume_batch(state->ring, 1);
    }
    return NULL;
}

static void test_spsc(void) {
    stress_state_t state;
    state.ring = aligned_alloc(64, vemb_v16_client_ring_bytes(64));
    assert(state.ring);
    vemb_v16_client_ring_init(state.ring, 64);
    batch_arena_producer_t producer;
    batch_arena_producer_init(&producer, state.ring);
    pthread_t consumer;
    assert(pthread_create(&consumer, NULL, stress_consumer, &state) == 0);
    uint8_t frame[4112];
    for (uint64_t id = 1; id <= STRESS_FRAMES; id++) {
        uint32_t bytes = 16 + (id * 97) % 4096;
        memset(frame, (uint8_t)id, bytes);
        ring_rc_t rc;
        while ((rc = batch_arena_publish(state.ring, state.arena, sizeof(state.arena),
                    &producer, frame, bytes, 1, id)) == RING_ERR_FULL)
            cpu_relax();
        assert(rc == RING_OK);
    }
    assert(pthread_join(consumer, NULL) == 0);
    assert(atomic_load(&state.ring->head) == STRESS_FRAMES);
    free(state.ring);
}

int main(void) {
    const char *keys[] = {"a", "key-b"};
    const uint16_t lens[] = {1, 5};
    uint8_t frame[256];
    uint32_t key_bytes = lens[0] + lens[1];
    size_t frame_len = batch_request_encoded_len(key_bytes, 2);
    /* A batch carries its core-selected submit epoch, independently from the
     * v2 channel ATTACH epoch. */
    batch_request_encode(frame, 7, 43, keys, lens, 2, key_bytes);
    batch_request_view_t view;
    assert(batch_request_decode(&view, frame, frame_len) == 0);
    assert(view.batch_id == 7 && view.topology_epoch == 43 && view.item_count == 2);
    assert(batch_request_key_len_at(&view, 0) == 1 &&
           batch_request_key_len_at(&view, 1) == 5);
    assert(memcmp(view.keys, "akey-b", 6) == 0);
    frame[frame_len - 1] ^= 1;
    assert(batch_request_decode(&view, frame, frame_len) != 0);
    frame[frame_len - 1] ^= 1;

    _Alignas(64) uint8_t descriptor_storage[vemb_v16_client_ring_bytes(64)];
    uint8_t descriptor_arena[128];
    vemb_v16_client_ring_t *descriptor_ring =
        (vemb_v16_client_ring_t *)descriptor_storage;
    vemb_v16_client_ring_init(descriptor_ring, 64);
    batch_arena_producer_t producer;
    batch_arena_producer_init(&producer, descriptor_ring);
    frame[frame_len - 1] ^= 1;
    assert(batch_arena_publish(descriptor_ring, descriptor_arena,
                                         sizeof(descriptor_arena), &producer,
                                         frame, (uint32_t)frame_len, 2, 7) == RING_OK);
    assert(batch_request_decode(&view, descriptor_arena,
                                         frame_len) == 0);
    frame[frame_len - 1] ^= 1;

    batch_response_t response = {
        .batch_id = 7,
        .topology_epoch = 43,
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
    assert(response_view.batch_id == 7 && response_view.topology_epoch == 43 &&
           response_view.item_count == 2 && decoded[0].req_id == 0 &&
           decoded[1].req_id == 1 && decoded[1].vector_offset == 4096);
    frame[frame_len - 1] ^= 1;
    assert(batch_response_decode(&response_view, decoded, frame, frame_len) != 0);

    _Alignas(64) uint8_t descriptor_storage2[vemb_v16_client_ring_bytes(64)];
    uint8_t descriptor_arena2[64];
    vemb_v16_client_ring_t *descriptor_ring2 =
        (vemb_v16_client_ring_t *)descriptor_storage2;
    vemb_v16_client_ring_init(descriptor_ring2, 64);
    batch_arena_producer_t producer2;
    batch_arena_producer_init(&producer2, descriptor_ring2);
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
    test_reclaim();
    test_spsc();
    puts("vemb_v16_batch_ring_ut: PASS");
    return 0;
}
