#ifndef __VEMB_V16_CLIENT_RING_H
#define __VEMB_V16_CLIENT_RING_H

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>

#include "macro.h"
#include "vemb_v16_cacheline.h"
#include "vemb_v16_ring_rc.h"
#include "vemb_v16_util.h"

/* Each target supplies its own implementation: src/sve_operation.c for the
 * server and clients/c/vemb_v16_client_sdk.c for the SDK. */
void sve_streaming_load_f32(const void *src, void *dst, size_t size);

#define VEMB_V16_CLIENT_MAX_BATCH_PIPELINE 128u
#define VEMB_V16_CLIENT_RING_SIZE \
    (VEMB_V16_CLIENT_MAX_BATCH_PIPELINE * 2u)
#define VEMB_V16_CLIENT_RING_MASK (VEMB_V16_CLIENT_RING_SIZE - 1u)
#define RING_SLOT_META_BYTES CACHELINE_SIZE


typedef struct vemb_v16_client_ring {
    _Alignas(64) atomic_uint_fast64_t head; /* Consumer-owned next slot to consume. */
    _Alignas(64) atomic_uint_fast64_t tail; /* Producer-owned next slot to write. */
    uint32_t slot_size;
    uint32_t slot_count;
    uint32_t slot_mask;
    uint32_t reserved;
    uint64_t slots_off;   /* byte offset from ring start to slots area */
} vemb_v16_client_ring_t;

static inline size_t vemb_v16_client_ring_bytes(uint32_t slot_size) {
    size_t slots_off = align_up_size(
        sizeof(vemb_v16_client_ring_t), CACHELINE_SIZE);
    size_t slot_stride = align_up_size(
        RING_SLOT_META_BYTES + slot_size,
        CACHELINE_SIZE);
    return slots_off + slot_stride * VEMB_V16_CLIENT_RING_SIZE;
}

/* Map a logical head/tail index to its physical slot base address.
 * Slot layout:
 * slot base --> [ metadata: CACHELINE_SIZE ][ payload: slot_size ]
 */
static inline uint8_t *ring_slot_base(
    vemb_v16_client_ring_t *ring, uint64_t index) {
    uint8_t *slots_base = (uint8_t *)ring + ring->slots_off;
    return slots_base + (index & ring->slot_mask) *
        (RING_SLOT_META_BYTES + ring->slot_size);
}

static inline void vemb_v16_client_ring_init(vemb_v16_client_ring_t *ring,
                                             uint32_t slot_size) {
    atomic_init(&ring->head, 0);
    atomic_init(&ring->tail, 0);
    ring->slot_size = align_up_size(slot_size, CACHELINE_SIZE);
    ring->slot_count = VEMB_V16_CLIENT_RING_SIZE;
    ring->slot_mask = VEMB_V16_CLIENT_RING_MASK;
    ring->reserved = 0;
    ring->slots_off = align_up_size(sizeof(*ring), CACHELINE_SIZE);
}

static inline ring_rc_t vemb_v16_client_publish(
    vemb_v16_client_ring_t *ring, const void *data, uint32_t len) {
    RETURN_IF(len > ring->slot_size, RING_ERR_INVALID);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
    if (tail - head >= ring->slot_count) return RING_ERR_FULL;
    uint8_t *slot = ring_slot_base(ring, tail);
    sve_streaming_load_f32(data,
                           slot + RING_SLOT_META_BYTES, len);
    *(uint32_t *)slot = len;
    atomic_store_explicit(&ring->tail, tail + 1, memory_order_release);
    return RING_OK;
}

static inline ring_rc_t vemb_v16_client_publish_batch(
    vemb_v16_client_ring_t *ring, const void *slots, uint32_t len,
    uint32_t count) {
    if (count == 0) return RING_OK;
    RETURN_IF(len > ring->slot_size, RING_ERR_INVALID);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
    if (tail - head + count > ring->slot_count) return RING_ERR_FULL;
    const uint8_t *src = slots;
    for (uint32_t i = 0; i < count; i++) {
        uint8_t *slot = ring_slot_base(ring, tail + i);
        sve_streaming_load_f32(
            src + (size_t)i * len,
            slot + RING_SLOT_META_BYTES,
            len);
        *(uint32_t *)slot = len;
    }
    atomic_store_explicit(&ring->tail, tail + count, memory_order_release);
    return RING_OK;
}

/* Publish variable-length frames as one producer transaction. All capacity
 * is checked before the first slot is copied, so a failed call publishes no
 * partial batch. */
static inline ring_rc_t vemb_v16_client_publish_ptr_batch(
    vemb_v16_client_ring_t *ring,
    const void *const *data,
    const uint32_t *lens,
    uint32_t count) {
    if (count == 0) return RING_OK;
    RETURN_IF(!data || !lens, RING_ERR_INVALID);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
    if (tail - head + count > ring->slot_count) return RING_ERR_FULL;
    for (uint32_t i = 0; i < count; i++) {
        RETURN_IF(!data[i] || lens[i] > ring->slot_size, RING_ERR_INVALID);
        uint8_t *slot = ring_slot_base(ring, tail + i);
        sve_streaming_load_f32(
            data[i],
            slot + RING_SLOT_META_BYTES,
            lens[i]);
        *(uint32_t *)slot = lens[i];
    }
    atomic_store_explicit(&ring->tail, tail + count, memory_order_release);
    return RING_OK;
}

static inline int vemb_v16_client_poll(vemb_v16_client_ring_t *ring,
                                       void *data,
                                       uint32_t max_len) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
    if (head >= tail) return 0;
    uint32_t len = *(uint32_t *)ring_slot_base(ring, head);
    if (len > ring->slot_size) len = ring->slot_size;
    if (len > max_len) len = max_len;
    sve_streaming_load_f32(
        ring_slot_base(ring, head) +
            RING_SLOT_META_BYTES,
        data,
        len);
    atomic_store_explicit(&ring->head, head + 1, memory_order_release);
    return (int)len;
}

static inline uint32_t vemb_v16_client_poll_batch(
    vemb_v16_client_ring_t *ring, void *slots, uint32_t *lengths,
    uint32_t max_len, uint32_t max_count) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
    uint64_t available = tail - head;
    if (available == 0 || max_count == 0) return 0;
    if (available > max_count) available = max_count;
    uint8_t *dst = slots;
    for (uint32_t i = 0; i < (uint32_t)available; i++) {
        uint8_t *slot = ring_slot_base(ring, head + i);
        uint32_t len = *(uint32_t *)slot;
        if (len > ring->slot_size) len = ring->slot_size;
        uint32_t copy_len = len > max_len ? max_len : len;
        if (lengths) lengths[i] = copy_len;
        sve_streaming_load_f32(
            slot + RING_SLOT_META_BYTES,
            dst + (size_t)i * max_len,
            copy_len);
    }
    atomic_store_explicit(&ring->head, head + available, memory_order_release);
    return (uint32_t)available;
}

static inline uint32_t vemb_v16_client_peek_batch(vemb_v16_client_ring_t *ring,
                                                  const void **slots,
                                                  uint32_t max_count) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
    uint64_t available = tail - head;
    if (available == 0 || max_count == 0) return 0;
    if (available > max_count) available = max_count;
    for (uint32_t i = 0; i < (uint32_t)available; i++) {
        slots[i] = ring_slot_base(ring, head + i) +
            RING_SLOT_META_BYTES;
    }
    return (uint32_t)available;
}

static inline void vemb_v16_client_consume_batch(vemb_v16_client_ring_t *ring,
                                                 uint32_t count) {
    RETURN_IF(count == 0);
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    atomic_store_explicit(&ring->head, head + count, memory_order_release);
}

static inline uint64_t vemb_v16_client_available(vemb_v16_client_ring_t *ring) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
    return tail - head;
}

#endif
