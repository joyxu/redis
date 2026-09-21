#define _POSIX_C_SOURCE 200809L

#include "tlc_warm_allocator.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef enum bench_mode {
    BENCH_CHURN,
    BENCH_FILL,
    BENCH_MIXED_80W20D,
} bench_mode_t;

typedef struct bench_arg {
    tlc_warm_allocator_t *allocator;
    bench_mode_t mode;
    uint64_t iterations;
    uint64_t successful_ops;
    uint64_t allocations;
    uint64_t releases;
} bench_arg_t;

static uint64_t monotonic_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * UINT64_C(1000000000) +
           (uint64_t)ts.tv_nsec;
}

static const char *mode_name(bench_mode_t mode) {
    switch (mode) {
    case BENCH_CHURN: return "churn";
    case BENCH_FILL: return "fill";
    case BENCH_MIXED_80W20D: return "mixed-80w20d";
    }
    return "unknown";
}

static int parse_mode(const char *value, bench_mode_t *mode) {
    if (!strcmp(value, "churn")) {
        *mode = BENCH_CHURN;
        return 0;
    }
    if (!strcmp(value, "fill")) {
        *mode = BENCH_FILL;
        return 0;
    }
    if (!strcmp(value, "mixed-80w20d")) {
        *mode = BENCH_MIXED_80W20D;
        return 0;
    }
    return -1;
}

static void *bench_worker(void *arg) {
    bench_arg_t *worker = arg;
    if (worker->mode == BENCH_MIXED_80W20D) {
        for (uint64_t cycle = 0; cycle < worker->iterations; cycle++) {
            uint32_t slots[4];
            for (uint32_t i = 0; i < 4; i++) {
                if (tlc_warm_allocator_alloc(worker->allocator, &slots[i]) !=
                    TLC_WARM_ALLOC_OK)
                    return NULL;
                worker->allocations++;
                worker->successful_ops++;
            }
            if (tlc_warm_allocator_release(worker->allocator, slots[0]) != 0)
                return NULL;
            worker->releases++;
            worker->successful_ops++;
        }
        return NULL;
    }

    for (uint64_t i = 0; i < worker->iterations; i++) {
        uint32_t slot = UINT32_MAX;
        if (tlc_warm_allocator_alloc(worker->allocator, &slot) !=
            TLC_WARM_ALLOC_OK)
            break;
        worker->allocations++;
        worker->successful_ops++;
        if (worker->mode == BENCH_CHURN) {
            if (tlc_warm_allocator_release(worker->allocator, slot) != 0)
                break;
            worker->releases++;
            worker->successful_ops++;
        }
    }
    return NULL;
}

static uint64_t worker_iterations(bench_mode_t mode,
                                  uint32_t capacity,
                                  uint64_t churn_iterations,
                                  uint32_t threads,
                                  uint32_t worker_id) {
    uint64_t total = mode == BENCH_CHURN ? churn_iterations * threads :
        (mode == BENCH_FILL ? capacity : capacity / 4u);
    return total / threads + (worker_id < total % threads ? 1u : 0u);
}

static uint64_t requested_ops(bench_mode_t mode,
                              uint32_t capacity,
                              uint64_t churn_iterations,
                              uint32_t threads) {
    if (mode == BENCH_CHURN)
        return churn_iterations * threads * 2u;
    if (mode == BENCH_FILL)
        return capacity;
    return (uint64_t)(capacity / 4u) * 5u;
}

int main(int argc, char **argv) {
    bench_mode_t mode;
    if (argc < 2 || parse_mode(argv[1], &mode) != 0) {
        fprintf(stderr,
                "usage: %s churn|fill|mixed-80w20d THREADS [CAPACITY_OR_ITERATIONS]\n",
                argv[0]);
        return 2;
    }
    uint32_t threads = argc > 2 ? (uint32_t)strtoul(argv[2], NULL, 10) : 1;
    uint64_t value = argc > 3 ? strtoull(argv[3], NULL, 10) :
        (mode == BENCH_CHURN ? UINT64_C(500000) : UINT64_C(1048576));
    if (threads == 0 || threads > 128 || value == 0 ||
        (mode != BENCH_CHURN && value > UINT32_MAX))
        return 2;

    uint32_t capacity = mode == BENCH_CHURN ? 1u << 20 : (uint32_t)value;
    uint64_t churn_iterations = mode == BENCH_CHURN ? value : 0;
    tlc_warm_allocator_t allocator;
    if (tlc_warm_allocator_init(&allocator, capacity) != 0)
        return 1;
    pthread_t *ids = calloc(threads, sizeof(*ids));
    bench_arg_t *args = calloc(threads, sizeof(*args));
    if (!ids || !args)
        return 1;
    uint64_t start = monotonic_ns();
    for (uint32_t i = 0; i < threads; i++) {
        args[i] = (bench_arg_t){
            .allocator = &allocator,
            .mode = mode,
            .iterations = worker_iterations(mode, capacity,
                                            churn_iterations, threads, i),
        };
        if (pthread_create(&ids[i], NULL, bench_worker, &args[i]) != 0)
            return 1;
    }
    uint64_t successful_ops = 0;
    uint64_t allocations = 0;
    uint64_t releases = 0;
    for (uint32_t i = 0; i < threads; i++) {
        pthread_join(ids[i], NULL);
        successful_ops += args[i].successful_ops;
        allocations += args[i].allocations;
        releases += args[i].releases;
    }
    uint64_t elapsed = monotonic_ns() - start;
    uint64_t expected_ops = requested_ops(mode, capacity,
                                          churn_iterations, threads);
    uint64_t expected_live = mode == BENCH_FILL ? capacity :
        (mode == BENCH_MIXED_80W20D ?
            UINT64_C(3) * (capacity / 4u) : 0);
    uint64_t free_slots = tlc_warm_allocator_free_count(&allocator);
    uint32_t full = free_slots == 0;
    int valid = successful_ops == expected_ops &&
                allocations - releases == expected_live &&
                free_slots == (uint64_t)capacity - expected_live;
    if (mode == BENCH_FILL) {
        uint32_t extra;
        valid = valid &&
            tlc_warm_allocator_alloc(&allocator, &extra) == TLC_WARM_ALLOC_FULL;
    }
    double seconds = (double)elapsed / 1e9;
    printf("mode=%s threads=%u capacity=%u requested_ops=%llu successful_ops=%llu allocations=%llu releases=%llu live=%llu free=%llu full=%u elapsed_ns=%llu ops_per_sec=%.0f lock_free=%u valid=%u\n",
           mode_name(mode), threads, capacity,
           (unsigned long long)expected_ops,
           (unsigned long long)successful_ops,
           (unsigned long long)allocations,
           (unsigned long long)releases,
           (unsigned long long)(allocations - releases),
           (unsigned long long)free_slots, full,
           (unsigned long long)elapsed,
           seconds > 0 ? (double)successful_ops / seconds : 0.0,
           tlc_warm_allocator_lock_free(&allocator),
           valid ? 1u : 0u);
    free(ids);
    free(args);
    tlc_warm_allocator_destroy(&allocator);
    return valid ? 0 : 1;
}
