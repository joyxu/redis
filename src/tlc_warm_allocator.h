#ifndef __TLC_WARM_ALLOCATOR_H
#define __TLC_WARM_ALLOCATOR_H

#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>

/* Return codes for the owner-process WARM slot allocator. */
#define TLC_WARM_ALLOC_OK 0
#define TLC_WARM_ALLOC_FULL 1

typedef int (*tlc_warm_allocator_used_fn)(uint32_t global_slot, void *arg);

/*
 * Process-local allocator state.  Payload and slot metadata remain owned by
 * the WARM region; this structure only tracks reusable local slots.
 */
typedef struct tlc_warm_allocator {
    uint32_t capacity_slots;
    uint32_t free_word_count;
    uint32_t summary_word_count;
    _Atomic uint32_t allocated_slot_index;
    atomic_uint_fast64_t free_count;
    atomic_uint_fast64_t *free_words;
    atomic_uint_fast64_t *summary_words;
    pthread_mutex_t fallback_mu;
    uint32_t fallback_mu_init;
    uint32_t lock_free;
} tlc_warm_allocator_t;

int tlc_warm_allocator_init(tlc_warm_allocator_t *allocator,
                            uint32_t capacity_slots);
void tlc_warm_allocator_destroy(tlc_warm_allocator_t *allocator);

/* Rebuild from an owner-consistent snapshot of slot state. */
int tlc_warm_allocator_rebuild(tlc_warm_allocator_t *allocator,
                               tlc_warm_allocator_used_fn used_fn,
                               void *arg);

/* Reserve one global slot, preferring released bitmap slots. */
int tlc_warm_allocator_alloc(tlc_warm_allocator_t *allocator,
                             uint32_t *global_slot);

/* Return a previously reserved slot to the process-local free bitmap. */
int tlc_warm_allocator_release(tlc_warm_allocator_t *allocator,
                               uint32_t global_slot);

uint32_t tlc_warm_allocator_capacity(const tlc_warm_allocator_t *allocator);
/* Total currently free slots: untouched bump range plus released slots. */
uint64_t tlc_warm_allocator_free_count(const tlc_warm_allocator_t *allocator);
uint32_t tlc_warm_allocator_lock_free(const tlc_warm_allocator_t *allocator);

#endif
