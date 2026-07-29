#ifndef __VEMB_V16_CLIENT_RING_H
#define __VEMB_V16_CLIENT_RING_H

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include "vemb_v16_cacheline.h"

#ifdef VEMB_V16_CLIENT_RING_USE_SVE
/* Each target supplies its own implementation: src/sve_operation.c for the
 * server and clients/c/vemb_v16_client_sdk.c for the SDK. */
void sve_streaming_load_f32(const void *src, void *dst, size_t size);
static inline void vemb_v16_client_ring_copy(const void *src,
                                             void *dst,
                                             size_t size) {
    sve_streaming_load_f32(src, dst, size);
}
#else
static inline void vemb_v16_client_ring_copy(const void *src,
                                             void *dst,
                                             size_t size) {
    memcpy(dst, src, size);
}
#endif

/* Adaptive backoff — 三阶段（参考 aeron_ipc.h::aeron_poll_adaptive）：
 *   spins <  64: 纯 spin，compiler barrier only
 *   spins < 256: spin + ARM yield / x86 pause
 *   spins ≥ 256: nanosleep(1μs) */
static inline void vemb_v16_client_backoff(uint32_t spins) {
    if (spins < 64u) {
        __asm__ volatile("" ::: "memory");
    } else if (spins < 256u) {
#if defined(__aarch64__)
        __asm__ volatile("yield" ::: "memory");
#elif defined(__x86_64__)
        __asm__ volatile("pause" ::: "memory");
#else
        __asm__ volatile("" ::: "memory");
#endif
    } else {
        struct timespec ts = {0, 1000};  /* 1μs */
        nanosleep(&ts, NULL);
    }
}

#define VEMB_V16_CLIENT_MAX_BATCH_PIPELINE 128u
#define VEMB_V16_CLIENT_RING_SIZE \
    (VEMB_V16_CLIENT_MAX_BATCH_PIPELINE * 2u)
#define VEMB_V16_CLIENT_RING_MASK (VEMB_V16_CLIENT_RING_SIZE - 1u)
#define VEMB_V16_CLIENT_RING_SLOT_META_BYTES VEMB_V16_CACHELINE_SIZE

typedef struct vemb_v16_client_ring {
    _Alignas(64) atomic_uint_fast64_t head;
    _Alignas(64) atomic_uint_fast64_t tail;
    uint32_t slot_size;
    uint32_t slot_count;
    uint32_t slot_mask;
    uint32_t reserved;
    uint64_t slots_off;   /* byte offset from ring start to slots area */
} vemb_v16_client_ring_t;

static inline size_t vemb_v16_client_ring_bytes(uint32_t slot_size) {
    size_t slots_off = vemb_v16_align_up_size(
        sizeof(vemb_v16_client_ring_t), VEMB_V16_CACHELINE_SIZE);
    size_t slot_stride = vemb_v16_align_up_size(
        VEMB_V16_CLIENT_RING_SLOT_META_BYTES + slot_size,
        VEMB_V16_CACHELINE_SIZE);
    return slots_off + slot_stride * VEMB_V16_CLIENT_RING_SIZE;
}

static inline uint32_t vemb_v16_client_ring_aligned_slot_size(
    uint32_t payload_size) {
    return vemb_v16_align_up_u32(payload_size, VEMB_V16_CACHELINE_SIZE);
}

static inline size_t vemb_v16_client_ring_slot_stride(uint32_t slot_size) {
    return vemb_v16_align_up_size(
        VEMB_V16_CLIENT_RING_SLOT_META_BYTES + slot_size,
        VEMB_V16_CACHELINE_SIZE);
}

static inline uint8_t *vemb_v16_client_ring_slot_base(
    vemb_v16_client_ring_t *ring, uint64_t index) {
    uint8_t *slots_base = (uint8_t *)ring + ring->slots_off;
    return slots_base + (index & ring->slot_mask) *
        vemb_v16_client_ring_slot_stride(ring->slot_size);
}

static inline void vemb_v16_client_ring_init(vemb_v16_client_ring_t *ring,
                                             uint32_t slot_size) {
    atomic_init(&ring->head, 0);
    atomic_init(&ring->tail, 0);
    ring->slot_size = vemb_v16_client_ring_aligned_slot_size(slot_size);
    ring->slot_count = VEMB_V16_CLIENT_RING_SIZE;
    ring->slot_mask = VEMB_V16_CLIENT_RING_MASK;
    ring->reserved = 0;
    ring->slots_off = vemb_v16_align_up_size(
        sizeof(*ring), VEMB_V16_CACHELINE_SIZE);
}

static inline int vemb_v16_client_publish(vemb_v16_client_ring_t *ring,
                                          const void *data,
                                          uint32_t len) {
    if (len > ring->slot_size) return -2;
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
    if (tail - head >= ring->slot_count) return -1;
    uint8_t *slot = vemb_v16_client_ring_slot_base(ring, tail);
    vemb_v16_client_ring_copy(data, slot + VEMB_V16_CLIENT_RING_SLOT_META_BYTES,
                              len);
    *(uint32_t *)slot = len;
    atomic_store_explicit(&ring->tail, tail + 1, memory_order_release);
    return 0;
}

static inline int vemb_v16_client_publish_batch(vemb_v16_client_ring_t *ring,
                                                const void *slots,
                                                uint32_t len,
                                                uint32_t count) {
    if (count == 0) return 0;
    if (len > ring->slot_size) return -2;
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
    if (tail - head + count > ring->slot_count) return -1;
    const uint8_t *src = slots;
    for (uint32_t i = 0; i < count; i++) {
        uint8_t *slot = vemb_v16_client_ring_slot_base(ring, tail + i);
        vemb_v16_client_ring_copy(
            src + (size_t)i * len,
            slot + VEMB_V16_CLIENT_RING_SLOT_META_BYTES,
            len);
        *(uint32_t *)slot = len;
    }
    atomic_store_explicit(&ring->tail, tail + count, memory_order_release);
    return 0;
}

/* Publish variable-length frames as one producer transaction. All capacity
 * is checked before the first slot is copied, so a failed call publishes no
 * partial batch. */
static inline int vemb_v16_client_publish_ptr_batch(
    vemb_v16_client_ring_t *ring,
    const void *const *data,
    const uint32_t *lens,
    uint32_t count) {
    if (count == 0) return 0;
    if (!data || !lens) return -2;
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
    if (tail - head + count > ring->slot_count) return -1;
    for (uint32_t i = 0; i < count; i++) {
        if (!data[i] || lens[i] > ring->slot_size) return -2;
        uint8_t *slot = vemb_v16_client_ring_slot_base(ring, tail + i);
        vemb_v16_client_ring_copy(
            data[i],
            slot + VEMB_V16_CLIENT_RING_SLOT_META_BYTES,
            lens[i]);
        *(uint32_t *)slot = lens[i];
    }
    atomic_store_explicit(&ring->tail, tail + count, memory_order_release);
    return 0;
}

static inline int vemb_v16_client_poll(vemb_v16_client_ring_t *ring,
                                       void *data,
                                       uint32_t max_len) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
    if (head >= tail) return 0;
    uint32_t len = *(uint32_t *)vemb_v16_client_ring_slot_base(ring, head);
    if (len > ring->slot_size) len = ring->slot_size;
    if (len > max_len) len = max_len;
    vemb_v16_client_ring_copy(
        vemb_v16_client_ring_slot_base(ring, head) +
            VEMB_V16_CLIENT_RING_SLOT_META_BYTES,
        data,
        len);
    atomic_store_explicit(&ring->head, head + 1, memory_order_release);
    return (int)len;
}

static inline uint32_t vemb_v16_client_poll_batch_lengths(
    vemb_v16_client_ring_t *ring, void *slots, uint32_t *lengths,
    uint32_t max_len, uint32_t max_count) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
    uint64_t available = tail - head;
    if (available == 0 || max_count == 0) return 0;
    if (available > max_count) available = max_count;
    uint8_t *dst = slots;
    for (uint32_t i = 0; i < (uint32_t)available; i++) {
        uint8_t *slot = vemb_v16_client_ring_slot_base(ring, head + i);
        uint32_t len = *(uint32_t *)slot;
        if (len > ring->slot_size) len = ring->slot_size;
        uint32_t copy_len = len > max_len ? max_len : len;
        if (lengths) lengths[i] = copy_len;
        vemb_v16_client_ring_copy(
            slot + VEMB_V16_CLIENT_RING_SLOT_META_BYTES,
            dst + (size_t)i * max_len,
            copy_len);
    }
    atomic_store_explicit(&ring->head, head + available, memory_order_release);
    return (uint32_t)available;
}

static inline uint32_t vemb_v16_client_poll_batch(vemb_v16_client_ring_t *ring,
                                                  void *slots,
                                                  uint32_t max_len,
                                                  uint32_t max_count) {
    return vemb_v16_client_poll_batch_lengths(ring, slots, NULL,
                                              max_len, max_count);
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
        slots[i] = vemb_v16_client_ring_slot_base(ring, head + i) +
            VEMB_V16_CLIENT_RING_SLOT_META_BYTES;
    }
    return (uint32_t)available;
}

static inline void vemb_v16_client_consume_batch(vemb_v16_client_ring_t *ring,
                                                 uint32_t count) {
    if (count == 0) return;
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    atomic_store_explicit(&ring->head, head + count, memory_order_release);
}

static inline uint64_t vemb_v16_client_available(vemb_v16_client_ring_t *ring) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
    return tail - head;
}

/* Adaptive publish — 阻塞直到 publish 成功（三阶段退避）。 */
static inline void vemb_v16_client_publish_adaptive(vemb_v16_client_ring_t *ring,
                                                     const void *data,
                                                     uint32_t len) {
    uint32_t spins = 0;
    while (vemb_v16_client_publish(ring, data, len) != 0) {
        vemb_v16_client_backoff(spins++);
    }
}

/* Adaptive poll — 阻塞直到拿到数据（三阶段退避）。返回值总是 > 0。 */
static inline int vemb_v16_client_poll_adaptive(vemb_v16_client_ring_t *ring,
                                                 void *data,
                                                 uint32_t max_len) {
    uint32_t spins = 0;
    int got;
    while ((got = vemb_v16_client_poll(ring, data, max_len)) == 0) {
        vemb_v16_client_backoff(spins++);
    }
    return got;
}

#endif
