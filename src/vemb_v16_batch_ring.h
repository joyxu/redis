#ifndef VEMB_V16_BATCH_RING_H
#define VEMB_V16_BATCH_RING_H

#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

#include "macro.h"
#include "vemb_v16_aeron_attach.h"
#include "vemb_v16_client_ring.h"
#include "vemb_v16_protocol.h"
#include "sve_operation.h"

/*
 * Batch arena contract: one producer and one consumer own each arena/ring.
 *
 * Producer                                      Consumer
 * --------                                      --------
 * encode frame body + batch_id commit
 * copy body to arena (without commit)
 * release fence
 * write commit marker
 * publish descriptor (tail, release)  ------>  acquire tail
 *                                              read descriptor
 *                                              validate sequence and bounds
 *                                              decode frame and verify commit
 *                                              dispatch batch
 * consume descriptor (head, release)  <-----  finish processing
 * acquire head
 * look up locally recorded frame end
 * advance local arena_head
 * reuse arena space
 *
 * In the descriptor ring, producer-owned tail is the next write position and
 * consumer-owned head is the next consume position. In the local producer
 * state, arena_tail is the next frame write position and arena_head is the
 * first reusable byte after consumed frames.
 *
 * The producer owns arena_tail. The consumer advances ring->head only after
 * processing succeeds. The producer observes that head with acquire ordering,
 * then derives reclaimable arena_head from its local frame-end ledger. The
 * commit marker must remain the final frame write; descriptor publication
 * makes the completed frame visible to the consumer.
 */

#define VEMB_V16_BATCH_REQUEST_MAGIC 0x56314252u
#define VEMB_V16_BATCH_REQUEST_HEADER_BYTES 28u
#define VEMB_V16_BATCH_RESPONSE_MAGIC 0x56314253u
#define VEMB_V16_BATCH_RESPONSE_HEADER_BYTES 24u
#define VEMB_V16_BATCH_COMMIT_BYTES 8u

typedef struct batch_request_view {
    uint64_t batch_id;
    uint64_t topology_epoch;
    uint32_t item_count;
    uint32_t key_bytes;
    const uint8_t *key_lens;
    const uint8_t *keys;
} batch_request_view_t;

static inline uint16_t batch_request_key_len_at(
        const batch_request_view_t *view, uint32_t index) {
    const uint8_t *p = view->key_lens + (size_t)index * sizeof(uint16_t);
    return vemb_v16_proto_get_u16(&p);
}

typedef struct batch_desc {
    uint64_t start;      /* Absolute arena byte where the frame begins. */
    uint64_t batch_id;   /* Must match the frame's final commit marker. */
    uint32_t bytes;      /* Encoded frame bytes, including the commit marker. */
    uint32_t item_count; /* Number of request or response entries. */
    uint64_t sequence;   /* Descriptor sequence, expected to equal head + 1. */
} batch_desc_t;

/* One producer owns this state for the entire lifetime of a fresh channel.
 * Ring layout is fixed at attach and validated by the mapping boundary. */
typedef struct batch_arena_producer {
    uint64_t arena_tail;    /* Next absolute arena byte to write. */
    uint64_t observed_head; /* Consumer head position already used for reclaim. */
    uint64_t arena_head;    /* First absolute arena byte safe to reuse. */
    uint64_t descriptor_tail;
    uint8_t *slots_base;
    uint32_t slot_stride;
    uint32_t slot_count;
    uint32_t slot_mask;
    uint64_t frame_end[VEMB_V16_CLIENT_RING_SIZE];
} batch_arena_producer_t;

/* The ring must be newly initialized, empty, and have the fixed v2 layout.
 * Reconnect creates a new ring and reinitializes this producer with it. */
static inline void batch_arena_producer_init(
    batch_arena_producer_t *producer, vemb_v16_client_ring_t *ring) {
    producer->arena_tail = 0;
    producer->observed_head = 0;
    producer->arena_head = 0;
    producer->descriptor_tail = 0;
    producer->slots_base = (uint8_t *)ring + ring->slots_off;
    producer->slot_stride = RING_SLOT_META_BYTES + ring->slot_size;
    producer->slot_count = ring->slot_count;
    producer->slot_mask = ring->slot_mask;
    /* Ledger entries become valid only after their descriptor is published. */
}

static inline int batch_desc_is_current(
    vemb_v16_client_ring_t *ring, const batch_desc_t *desc) {
    return desc->sequence == atomic_load_explicit(&ring->head, memory_order_relaxed) + 1;
}

static inline ring_rc_t batch_arena_publish(
    vemb_v16_client_ring_t *ring, uint8_t *arena, uint32_t arena_bytes,
    batch_arena_producer_t *producer, const void *frame,
    uint32_t frame_bytes, uint32_t item_count, uint64_t batch_id) {
    /* The same producer exclusively owns this ring; generic publication must
     * not be interleaved. Layout/lifetime are guaranteed by attach. Only FULL
     * is retryable; an oversized frame cannot fit even in an empty arena. */
    /* ERR_INVALID: a frame that cannot fit the arena cannot be published. */
    RETURN_IF(frame_bytes < VEMB_V16_BATCH_COMMIT_BYTES ||
              frame_bytes > arena_bytes, RING_ERR_INVALID);

    uint64_t tail = producer->descriptor_tail;
    uint64_t start = producer->arena_tail;
    uint32_t offset = (uint32_t)(start % arena_bytes);
    /* Keep each frame contiguous for direct consumer decode; skip tail
     * fragments instead of splitting a frame across the arena wrap. */
    if (frame_bytes > arena_bytes - offset) {
        start += arena_bytes - offset;
        offset = 0;
    }
    /* A stale cached head is conservative. Refresh only under pressure and
     * reclaim from local memory, never by reading back NC descriptors. */
    if (tail - producer->observed_head >= producer->slot_count ||
        start + frame_bytes - producer->arena_head > arena_bytes) {
        uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
        if (head != producer->observed_head) {
            producer->arena_head = producer->frame_end[(head - 1) & producer->slot_mask];
            producer->observed_head = head;
        }
        /* An empty arena may discard wrap padding, including when the next
         * frame occupies the entire arena. No unconsumed frame is skipped. */
        if (head == tail)
            producer->arena_head = start;
    }
    RETURN_IF(tail - producer->observed_head >= producer->slot_count,
              RING_ERR_FULL);
    RETURN_IF(start + frame_bytes - producer->arena_head > arena_bytes,
              RING_ERR_FULL);

    /* Publish the commit marker only after the complete frame body is visible.
     * A marker included in the bulk copy can otherwise become visible before
     * an older arena body on the remote CC mapping. */
    sve_streaming_load_f32(frame, arena + offset,
                           frame_bytes - VEMB_V16_BATCH_COMMIT_BYTES);
    atomic_thread_fence(memory_order_release);
    volatile uint8_t *commit = arena + offset + frame_bytes -
        VEMB_V16_BATCH_COMMIT_BYTES;
    commit[0] = (uint8_t)(batch_id >> 56);
    commit[1] = (uint8_t)(batch_id >> 48);
    commit[2] = (uint8_t)(batch_id >> 40);
    commit[3] = (uint8_t)(batch_id >> 32);
    commit[4] = (uint8_t)(batch_id >> 24);
    commit[5] = (uint8_t)(batch_id >> 16);
    commit[6] = (uint8_t)(batch_id >> 8);
    commit[7] = (uint8_t)batch_id;
    batch_desc_t desc = {
        .start = start,
        .batch_id = batch_id,
        .bytes = frame_bytes,
        .item_count = item_count,
        .sequence = tail + 1,
    };
    /* Descriptor and arena capacity were reserved together above. The sole
     * consumer can only free space, so commit cannot fail or need rechecking. */
    uint8_t *slot = producer->slots_base +
        (tail & producer->slot_mask) * producer->slot_stride;
    sve_streaming_load_f32(&desc, slot + RING_SLOT_META_BYTES, sizeof(desc));
    *(uint32_t *)slot = sizeof(desc);
    atomic_store_explicit(&ring->tail, tail + 1, memory_order_release);
    producer->arena_tail = start + frame_bytes;
    producer->frame_end[tail & producer->slot_mask] = producer->arena_tail;
    producer->descriptor_tail = tail + 1;
    return RING_OK;
}

// batch_arena_consume = batch_desc_peek + vemb_v16_client_consume_batch
static inline int batch_desc_peek(
    vemb_v16_client_ring_t *ring, batch_desc_t *desc) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
    if (head == tail)
        return 0;
    uint8_t *slot = ring_slot_base(ring, head);
    memcpy(desc, slot + RING_SLOT_META_BYTES, sizeof(*desc));
    return 1;
}

static inline size_t batch_request_encoded_len(
        uint32_t key_bytes, uint32_t item_count) {
    return VEMB_V16_BATCH_REQUEST_HEADER_BYTES +
        (size_t)item_count * sizeof(uint16_t) + key_bytes +
        VEMB_V16_BATCH_COMMIT_BYTES;
}

static inline void batch_request_encode(
        uint8_t *dst, uint64_t batch_id, uint64_t topology_epoch,
        const char *const *keys, const uint16_t *key_lens,
        uint32_t item_count, uint32_t key_bytes) {
    uint8_t *p = dst;
    vemb_v16_proto_put_u32(&p, VEMB_V16_BATCH_REQUEST_MAGIC);
    vemb_v16_proto_put_u64(&p, batch_id);
    vemb_v16_proto_put_u64(&p, topology_epoch);
    vemb_v16_proto_put_u32(&p, item_count);
    vemb_v16_proto_put_u32(&p, key_bytes);
    for (uint32_t i = 0; i < item_count; i++)
        vemb_v16_proto_put_u16(&p, key_lens[i]);
    for (uint32_t i = 0; i < item_count; i++)
        vemb_v16_proto_put_bytes(&p, keys[i], key_lens[i]);
    vemb_v16_proto_put_u64(&p, batch_id);
}

static inline int batch_request_decode(
        batch_request_view_t *view, const uint8_t *src, size_t len) {
    RETURN_IF(len < VEMB_V16_BATCH_REQUEST_HEADER_BYTES +
                    VEMB_V16_BATCH_COMMIT_BYTES, -1);
    const uint8_t *p = src;
    RETURN_IF(vemb_v16_proto_get_u32(&p) != VEMB_V16_BATCH_REQUEST_MAGIC, -1);
    view->batch_id = vemb_v16_proto_get_u64(&p);
    view->topology_epoch = vemb_v16_proto_get_u64(&p);
    view->item_count = vemb_v16_proto_get_u32(&p);
    view->key_bytes = vemb_v16_proto_get_u32(&p);
    RETURN_IF(view->item_count == 0 ||
              view->item_count > VEMB_V16_BATCH_REQUEST_SIZE_MAX ||
              len != VEMB_V16_BATCH_REQUEST_HEADER_BYTES +
                         (size_t)view->item_count * sizeof(uint16_t) +
                         view->key_bytes + VEMB_V16_BATCH_COMMIT_BYTES, -1);
    view->key_lens = p;
    p += (size_t)view->item_count * sizeof(uint16_t);
    view->keys = p;
    uint32_t total = 0;
    for (uint32_t i = 0; i < view->item_count; i++) {
        uint16_t key_len = batch_request_key_len_at(view, i);
        RETURN_IF(key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN, -1);
        total += key_len;
    }
    p += view->key_bytes;
    RETURN_IF(total != view->key_bytes ||
              vemb_v16_proto_get_u64(&p) != view->batch_id, -1);
    return 0;
}

typedef struct batch_response {
    uint64_t batch_id;
    uint64_t topology_epoch;
    uint32_t item_count;
    vemb_v16_resp_t entries[VEMB_V16_BATCH_REQUEST_SIZE_MAX];
} batch_response_t;

typedef struct batch_response_view {
    uint64_t batch_id;
    uint64_t topology_epoch;
    uint32_t item_count;
} batch_response_view_t;

static inline size_t batch_response_encode(
    uint8_t *dst, const batch_response_t *response) {
    uint8_t *p = dst;
    vemb_v16_proto_put_u32(&p, VEMB_V16_BATCH_RESPONSE_MAGIC);
    vemb_v16_proto_put_u64(&p, response->batch_id);
    vemb_v16_proto_put_u64(&p, response->topology_epoch);
    vemb_v16_proto_put_u32(&p, response->item_count);
    for (uint32_t i = 0; i < response->item_count; i++) {
        size_t wire_len = vemb_v16_resp_encoded_len(&response->entries[i]);
        vemb_v16_proto_put_u16(&p, (uint16_t)wire_len);
        vemb_v16_resp_encode(p, wire_len, &response->entries[i], NULL);
        p += wire_len;
    }
    vemb_v16_proto_put_u64(&p, response->batch_id);
    return (size_t)(p - dst);
}

static inline int batch_response_decode(
    batch_response_view_t *view, vemb_v16_resp_t *entries,
    const uint8_t *src, size_t len) {
    RETURN_IF(len < VEMB_V16_BATCH_RESPONSE_HEADER_BYTES +
                    VEMB_V16_BATCH_COMMIT_BYTES, -1);
    const uint8_t *p = src;
    RETURN_IF(vemb_v16_proto_get_u32(&p) != VEMB_V16_BATCH_RESPONSE_MAGIC,
              -1);
    view->batch_id = vemb_v16_proto_get_u64(&p);
    view->topology_epoch = vemb_v16_proto_get_u64(&p);
    view->item_count = vemb_v16_proto_get_u32(&p);
    RETURN_IF(view->item_count == 0 ||
              view->item_count > VEMB_V16_BATCH_REQUEST_SIZE_MAX, -1);
    for (uint32_t i = 0; i < view->item_count; i++) {
        RETURN_IF((size_t)(src + len - p) <
                  sizeof(uint16_t) + VEMB_V16_BATCH_COMMIT_BYTES, -1);
        uint16_t wire_len = vemb_v16_proto_get_u16(&p);
        RETURN_IF((size_t)(src + len - p) <
                  (size_t)wire_len + VEMB_V16_BATCH_COMMIT_BYTES, -1);
        RETURN_IF(vemb_v16_resp_decode(&entries[i], p, wire_len) != 0,
                  -1);
        p += wire_len;
        entries[i].req_id = i;
    }
    RETURN_IF(vemb_v16_proto_get_u64(&p) != view->batch_id, -1);
    return p == src + len ? 0 : -1;
}

#endif
