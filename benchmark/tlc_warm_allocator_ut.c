#include "tlc_warm_allocator.h"

#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct allocator_thread_arg {
    tlc_warm_allocator_t *allocator;
    uint32_t iterations;
    uint32_t *slots;
} allocator_thread_arg_t;

typedef struct rebuild_fixture {
    uint8_t *used;
} rebuild_fixture_t;

static int rebuild_slot_used(uint32_t global_slot, void *arg) {
    rebuild_fixture_t *fixture = arg;
    return fixture->used[global_slot] != 0;
}

static void *allocator_worker(void *arg) {
    allocator_thread_arg_t *worker = arg;
    for (uint32_t i = 0; i < worker->iterations; i++) {
        uint32_t slot = UINT32_MAX;
        int rc = tlc_warm_allocator_alloc(worker->allocator, &slot);
        if (rc != TLC_WARM_ALLOC_OK)
            break;
        worker->slots[i] = slot;
    }
    return NULL;
}

static void test_full_and_reuse(void) {
    tlc_warm_allocator_t allocator;
    assert(tlc_warm_allocator_init(&allocator, 130) == 0);
    assert(tlc_warm_allocator_free_count(&allocator) == 130);
    assert(tlc_warm_allocator_release(&allocator, 129) == -1);
    uint32_t *slots = calloc(130, sizeof(*slots));
    assert(slots);
    for (uint32_t i = 0; i < 130; i++) {
        assert(tlc_warm_allocator_alloc(&allocator, &slots[i]) ==
               TLC_WARM_ALLOC_OK);
        assert(tlc_warm_allocator_free_count(&allocator) == 129u - i);
    }
    uint32_t extra = UINT32_MAX;
    assert(tlc_warm_allocator_alloc(&allocator, &extra) == TLC_WARM_ALLOC_FULL);
    assert(tlc_warm_allocator_release(&allocator, slots[3]) == 0);
    assert(tlc_warm_allocator_release(&allocator, slots[129]) == 0);
    assert(tlc_warm_allocator_release(&allocator, slots[3]) == -1);
    assert(tlc_warm_allocator_free_count(&allocator) == 2);
    assert(tlc_warm_allocator_alloc(&allocator, &extra) == TLC_WARM_ALLOC_OK);
    assert(tlc_warm_allocator_alloc(&allocator, &extra) == TLC_WARM_ALLOC_OK);
    assert(tlc_warm_allocator_free_count(&allocator) == 0);
    free(slots);
    tlc_warm_allocator_destroy(&allocator);
}

static void test_concurrent_fill(void) {
    enum { capacity = 4096, thread_count = 8 };
    tlc_warm_allocator_t allocator;
    assert(tlc_warm_allocator_init(&allocator, capacity) == 0);
    pthread_t threads[thread_count];
    allocator_thread_arg_t args[thread_count];
    uint32_t *slots = calloc(capacity, sizeof(*slots));
    assert(slots);
    uint32_t per_thread = capacity / thread_count;
    for (uint32_t i = 0; i < thread_count; i++) {
        args[i] = (allocator_thread_arg_t){
            .allocator = &allocator,
            .iterations = per_thread,
            .slots = slots + i * per_thread,
        };
        assert(pthread_create(&threads[i], NULL, allocator_worker, &args[i]) == 0);
    }
    for (uint32_t i = 0; i < thread_count; i++)
        assert(pthread_join(threads[i], NULL) == 0);
    uint8_t *seen = calloc(capacity, sizeof(*seen));
    assert(seen);
    for (uint32_t i = 0; i < capacity; i++) {
        assert(slots[i] < capacity);
        assert(seen[slots[i]] == 0);
        seen[slots[i]] = 1;
    }
    uint32_t extra = UINT32_MAX;
    assert(tlc_warm_allocator_alloc(&allocator, &extra) == TLC_WARM_ALLOC_FULL);

    for (uint32_t i = 0; i < capacity; i++)
        assert(tlc_warm_allocator_release(&allocator, slots[i]) == 0);
    assert(tlc_warm_allocator_free_count(&allocator) == capacity);
    for (uint32_t i = 0; i < capacity; i++) {
        slots[i] = UINT32_MAX;
        seen[i] = 0;
    }
    for (uint32_t i = 0; i < thread_count; i++)
        assert(pthread_create(&threads[i], NULL, allocator_worker, &args[i]) == 0);
    for (uint32_t i = 0; i < thread_count; i++)
        assert(pthread_join(threads[i], NULL) == 0);
    for (uint32_t i = 0; i < capacity; i++) {
        assert(slots[i] < capacity);
        assert(seen[slots[i]] == 0);
        seen[slots[i]] = 1;
    }
    assert(tlc_warm_allocator_free_count(&allocator) == 0);
    assert(tlc_warm_allocator_alloc(&allocator, &extra) == TLC_WARM_ALLOC_FULL);
    free(seen);
    free(slots);
    tlc_warm_allocator_destroy(&allocator);
}

static void test_rebuild_across_summary_words(void) {
    enum { capacity = 9000 };
    tlc_warm_allocator_t allocator;
    uint8_t *used = malloc(capacity);
    assert(used);
    memset(used, 1, capacity);
    used[2] = 0;
    used[4097] = 0;
    used[8193] = 0;
    rebuild_fixture_t fixture = {.used = used};
    assert(tlc_warm_allocator_init(&allocator, capacity) == 0);
    assert(tlc_warm_allocator_rebuild(&allocator,
                                      rebuild_slot_used,
                                      &fixture) == 0);
    assert(tlc_warm_allocator_free_count(&allocator) == 3);
    uint8_t found[3] = {0};
    for (uint32_t i = 0; i < 3; i++) {
        uint32_t slot = UINT32_MAX;
        assert(tlc_warm_allocator_alloc(&allocator, &slot) ==
               TLC_WARM_ALLOC_OK);
        if (slot == 2)
            found[0] = 1;
        else if (slot == 4097)
            found[1] = 1;
        else if (slot == 8193)
            found[2] = 1;
        else
            assert(0);
    }
    assert(found[0] && found[1] && found[2]);
    uint32_t extra = UINT32_MAX;
    assert(tlc_warm_allocator_alloc(&allocator, &extra) ==
           TLC_WARM_ALLOC_FULL);
    tlc_warm_allocator_destroy(&allocator);
    free(used);
}

int main(void) {
    test_full_and_reuse();
    test_concurrent_fill();
    test_rebuild_across_summary_words();
    puts("tlc warm allocator: PASS");
    return 0;
}
