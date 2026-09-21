#include "tlc_warm_allocator.h"

#include "cpu_relax.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

#define TLC_WARM_ALLOC_WORD_BITS 64u

static _Thread_local uint32_t tlc_warm_allocator_word_hint;

static uint32_t word_count_for(uint32_t slots) {
    return (slots + TLC_WARM_ALLOC_WORD_BITS - 1u) /
           TLC_WARM_ALLOC_WORD_BITS;
}

static uint64_t valid_mask_for(uint32_t word, uint32_t capacity) {
    uint32_t first = word * TLC_WARM_ALLOC_WORD_BITS;
    uint32_t remaining = capacity > first ? capacity - first : 0;
    if (remaining >= TLC_WARM_ALLOC_WORD_BITS)
        return UINT64_MAX;
    return remaining == 0 ? 0 : (UINT64_C(1) << remaining) - 1u;
}

static void clear_bitmap(tlc_warm_allocator_t *allocator) {
    for (uint32_t i = 0; i < allocator->free_word_count; i++)
        atomic_store_explicit(&allocator->free_words[i], 0,
                              memory_order_relaxed);
    for (uint32_t i = 0; i < allocator->summary_word_count; i++)
        atomic_store_explicit(&allocator->summary_words[i], 0,
                              memory_order_relaxed);
    atomic_store_explicit(&allocator->free_count, 0, memory_order_relaxed);
    atomic_store_explicit(&allocator->allocated_slot_index, 0,
                          memory_order_relaxed);
    tlc_warm_allocator_word_hint = 0;
}

static int set_free_bit(tlc_warm_allocator_t *allocator,
                        uint32_t global_slot) {
    uint32_t word = global_slot / TLC_WARM_ALLOC_WORD_BITS;
    uint32_t bit = global_slot % TLC_WARM_ALLOC_WORD_BITS;
    uint64_t mask = UINT64_C(1) << bit;
    uint64_t old = atomic_fetch_or_explicit(&allocator->free_words[word],
                                            mask,
                                            memory_order_acq_rel);
    if ((old & mask) == 0) {
        uint32_t summary_word = word / TLC_WARM_ALLOC_WORD_BITS;
        uint32_t summary_bit = word % TLC_WARM_ALLOC_WORD_BITS;
        atomic_fetch_or_explicit(&allocator->summary_words[summary_word],
                                 UINT64_C(1) << summary_bit,
                                 memory_order_release);
        atomic_fetch_add_explicit(&allocator->free_count, 1,
                                  memory_order_release);
        return 1;
    }
    return 0;
}

static int alloc_free_bitmap(tlc_warm_allocator_t *allocator,
                             uint32_t *global_slot) {
    if (atomic_load_explicit(&allocator->free_count, memory_order_acquire) == 0)
        return TLC_WARM_ALLOC_FULL;

    uint32_t start_word =
        tlc_warm_allocator_word_hint % allocator->free_word_count;
    uint32_t start_summary_word = start_word / TLC_WARM_ALLOC_WORD_BITS;
    for (uint32_t scanned = 0;
         scanned < allocator->summary_word_count;
         scanned++) {
        uint32_t summary_word =
            (start_summary_word + scanned) % allocator->summary_word_count;
        uint64_t summary = atomic_load_explicit(
            &allocator->summary_words[summary_word], memory_order_acquire);
        uint32_t start_bit = scanned == 0 ?
            start_word % TLC_WARM_ALLOC_WORD_BITS : 0;
        uint64_t phases[2] = {
            summary & (UINT64_MAX << start_bit),
            start_bit == 0 ? 0 :
                summary & ((UINT64_C(1) << start_bit) - 1u),
        };
        for (uint32_t phase = 0; phase < 2; phase++) {
            uint64_t candidates = phases[phase];
            while (candidates != 0) {
                uint32_t summary_bit =
                    (uint32_t)__builtin_ctzll(candidates);
                uint32_t word = summary_word * TLC_WARM_ALLOC_WORD_BITS +
                                summary_bit;
                if (word >= allocator->free_word_count) {
                    candidates &= candidates - 1u;
                    continue;
                }
                uint64_t old = atomic_load_explicit(
                    &allocator->free_words[word], memory_order_relaxed);
                for (;;) {
                    uint64_t valid = valid_mask_for(
                        word, allocator->capacity_slots);
                    uint64_t available = old & valid;
                    if (available == 0)
                        break;
                    uint32_t bit = (uint32_t)__builtin_ctzll(available);
                    uint64_t mask = UINT64_C(1) << bit;
                    uint64_t next = old & ~mask;
                    if (atomic_compare_exchange_weak_explicit(
                            &allocator->free_words[word], &old, next,
                            memory_order_acq_rel, memory_order_relaxed)) {
                        uint32_t slot =
                            word * TLC_WARM_ALLOC_WORD_BITS + bit;
                        atomic_fetch_sub_explicit(&allocator->free_count, 1,
                                                  memory_order_relaxed);
                        if (next == 0) {
                            uint32_t sw = word / TLC_WARM_ALLOC_WORD_BITS;
                            uint32_t sb = word % TLC_WARM_ALLOC_WORD_BITS;
                            atomic_fetch_and_explicit(
                                &allocator->summary_words[sw],
                                ~(UINT64_C(1) << sb),
                                memory_order_acq_rel);
                            /* A concurrent release may have raced the clear. */
                            if (atomic_load_explicit(
                                    &allocator->free_words[word],
                                    memory_order_acquire) != 0) {
                                atomic_fetch_or_explicit(
                                    &allocator->summary_words[sw],
                                    UINT64_C(1) << sb,
                                    memory_order_release);
                            }
                        }
                        tlc_warm_allocator_word_hint = word;
                        *global_slot = slot;
                        return TLC_WARM_ALLOC_OK;
                    }
                    cpu_relax();
                }
                candidates &= candidates - 1u;
            }
        }
    }
    return TLC_WARM_ALLOC_FULL;
}

static int alloc_bump(tlc_warm_allocator_t *allocator,
                      uint32_t *global_slot) {
    uint32_t next = atomic_load_explicit(&allocator->allocated_slot_index,
                                         memory_order_relaxed);
    for (;;) {
        if (next >= allocator->capacity_slots)
            return TLC_WARM_ALLOC_FULL;
        uint32_t desired = next + 1u;
        if (atomic_compare_exchange_weak_explicit(
                &allocator->allocated_slot_index, &next, desired,
                memory_order_relaxed, memory_order_relaxed)) {
            *global_slot = next;
            return TLC_WARM_ALLOC_OK;
        }
        cpu_relax();
    }
}

int tlc_warm_allocator_init(tlc_warm_allocator_t *allocator,
                            uint32_t capacity_slots) {
    if (!allocator || capacity_slots == 0)
        return -1;
    memset(allocator, 0, sizeof(*allocator));
    allocator->capacity_slots = capacity_slots;
    allocator->free_word_count = word_count_for(capacity_slots);
    allocator->summary_word_count = word_count_for(allocator->free_word_count);
    allocator->free_words = calloc(allocator->free_word_count,
                                   sizeof(*allocator->free_words));
    allocator->summary_words = calloc(allocator->summary_word_count,
                                      sizeof(*allocator->summary_words));
    if (!allocator->free_words || !allocator->summary_words) {
        tlc_warm_allocator_destroy(allocator);
        return -1;
    }
    atomic_init(&allocator->allocated_slot_index, 0);
    atomic_init(&allocator->free_count, 0);
    allocator->lock_free = atomic_is_lock_free(&allocator->free_words[0]) &&
                           atomic_is_lock_free(&allocator->summary_words[0]) &&
                           atomic_is_lock_free(&allocator->allocated_slot_index);
    if (!allocator->lock_free) {
        if (pthread_mutex_init(&allocator->fallback_mu, NULL) != 0) {
            tlc_warm_allocator_destroy(allocator);
            return -1;
        }
        allocator->fallback_mu_init = 1;
    }
    return 0;
}

void tlc_warm_allocator_destroy(tlc_warm_allocator_t *allocator) {
    if (!allocator)
        return;
    if (allocator->fallback_mu_init)
        pthread_mutex_destroy(&allocator->fallback_mu);
    free(allocator->free_words);
    free(allocator->summary_words);
    memset(allocator, 0, sizeof(*allocator));
}

int tlc_warm_allocator_rebuild(tlc_warm_allocator_t *allocator,
                               tlc_warm_allocator_used_fn used_fn,
                               void *arg) {
    if (!allocator || !used_fn)
        return -1;
    if (allocator->fallback_mu_init)
        pthread_mutex_lock(&allocator->fallback_mu);
    clear_bitmap(allocator);
    for (uint32_t slot = 0; slot < allocator->capacity_slots; slot++) {
        if (!used_fn(slot, arg))
            (void)set_free_bit(allocator, slot);
    }
    atomic_store_explicit(&allocator->allocated_slot_index,
                          allocator->capacity_slots,
                          memory_order_release);
    if (allocator->fallback_mu_init)
        pthread_mutex_unlock(&allocator->fallback_mu);
    return 0;
}

int tlc_warm_allocator_alloc(tlc_warm_allocator_t *allocator,
                             uint32_t *global_slot) {
    if (!allocator || !global_slot)
        return -1;
    if (allocator->fallback_mu_init)
        pthread_mutex_lock(&allocator->fallback_mu);
    int rc = alloc_free_bitmap(allocator, global_slot);
    if (rc == TLC_WARM_ALLOC_FULL)
        rc = alloc_bump(allocator, global_slot);
    if (allocator->fallback_mu_init)
        pthread_mutex_unlock(&allocator->fallback_mu);
    return rc;
}

int tlc_warm_allocator_release(tlc_warm_allocator_t *allocator,
                               uint32_t global_slot) {
    if (!allocator || global_slot >= allocator->capacity_slots)
        return -1;
    if (allocator->fallback_mu_init)
        pthread_mutex_lock(&allocator->fallback_mu);
    uint32_t bump = atomic_load_explicit(&allocator->allocated_slot_index,
                                         memory_order_acquire);
    if (global_slot >= bump || !set_free_bit(allocator, global_slot)) {
        if (allocator->fallback_mu_init)
            pthread_mutex_unlock(&allocator->fallback_mu);
        return -1;
    }
    if (allocator->fallback_mu_init)
        pthread_mutex_unlock(&allocator->fallback_mu);
    return 0;
}

uint32_t tlc_warm_allocator_capacity(const tlc_warm_allocator_t *allocator) {
    return allocator ? allocator->capacity_slots : 0;
}

uint64_t tlc_warm_allocator_free_count(const tlc_warm_allocator_t *allocator) {
    if (!allocator)
        return 0;
    uint64_t recycled = atomic_load_explicit(
        (const atomic_uint_fast64_t *)&allocator->free_count,
        memory_order_acquire);
    uint32_t bump = atomic_load_explicit(
        (const _Atomic uint32_t *)&allocator->allocated_slot_index,
        memory_order_acquire);
    uint64_t untouched = bump < allocator->capacity_slots ?
        allocator->capacity_slots - bump : 0;
    uint64_t total = untouched + recycled;
    return total <= allocator->capacity_slots ?
        total : allocator->capacity_slots;
}

uint32_t tlc_warm_allocator_lock_free(const tlc_warm_allocator_t *allocator) {
    return allocator ? allocator->lock_free : 0;
}
