/* CPU-only production TLC handle lookup benchmark. No RPC or UB payload copy.
 * Usage: capacity keys hot_capacity iterations miss_iterations
 * Compile with TLC_LOOKUP_LEGACY_CAPACITY only for the old core/header ABI. */
#include "tlc_core.h"
#include "vemb_v16_hash.h"
#include "monotonic.h"
#include "zmalloc.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct lookup_key {
    char text[32];
    uint32_t len;
    uint32_t slot;
    uint64_t hash;
} lookup_key_t;

static uint32_t parse_count(const char *text) {
    char *end;
    errno = 0;
    unsigned long long n = strtoull(text, &end, 10);
    if (errno || end == text || *end || !n || n > UINT32_MAX) {
        fprintf(stderr, "invalid positive uint32: %s\n", text);
        exit(2);
    }
    return (uint32_t)n;
}

static uint64_t cpu_ns(void) {
    struct timespec t;
    if (clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t) != 0) {
        perror("clock_gettime");
        exit(1);
    }
    return (uint64_t)t.tv_sec * UINT64_C(1000000000) + t.tv_nsec;
}

static void report(const char *phase, uint32_t ops, uint64_t wall, uint64_t cpu) {
    printf("phase=%s ops=%u wall_ns=%" PRIu64 " cpu_ns=%" PRIu64
           " ns_per_op=%.3f cpu_ns_per_op=%.3f qps=%.3f\n",
           phase, ops, wall, cpu, (double)wall / ops, (double)cpu / ops,
           (double)ops * 1e9 / wall);
}

int main(int argc, char **argv) {
    if (argc != 6) {
        fprintf(stderr, "usage: %s capacity keys hot_capacity iterations miss_iterations\n", argv[0]);
        return 2;
    }
    uint32_t capacity = parse_count(argv[1]), count = parse_count(argv[2]);
    uint32_t hot = parse_count(argv[3]), iterations = parse_count(argv[4]);
    uint32_t misses = parse_count(argv[5]);
    if (count > capacity) {
        fprintf(stderr, "keys must be <= capacity\n");
        return 2;
    }
    monotonicInit();
    /* Small payloads deliberately isolate metadata and index CPU cost. */
    float *payload = zcalloc_num(capacity, 2 * sizeof(float));
    vemb_v16_warm_slot_meta_t *slots = zcalloc_num(capacity, sizeof(*slots));
    lookup_key_t *keys = zcalloc_num(count, sizeof(*keys));
    if (!payload || !slots || !keys) {
        fprintf(stderr, "allocation failed\n");
        return 1;
    }
    for (uint32_t i = 0; i < capacity; i++) {
        atomic_init(&slots[i].state, VEMB_V16_WARM_SLOT_FREE);
        atomic_init(&slots[i].owner_generation, 0);
        atomic_init(&slots[i].write_seq, 0);
        atomic_init(&slots[i].last_access_ns, 0);
        atomic_init(&slots[i].clock_bit, 0);
        atomic_init(&slots[i].cold_state, VEMB_V16_WARM_SLOT_COLD_NONE);
        slots[i].region_id = 1;
        slots[i].local_slot = i;
    }
    tlc_core_warm_region_config_t region = {
        .region_id = 1, .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1, .weight = 1, .value_size = 2 * sizeof(float),
        .region_bytes = (uint64_t)capacity * 2 * sizeof(float),
        .mapped_addr = (uint8_t *)payload, .slot_meta = slots,
    };
    tlc_core_config_t config = {
        .value_size = region.value_size, .hot_capacity = hot,
        .warm_regions = &region, .warm_region_count = 1,
#ifdef TLC_LOOKUP_LEGACY_CAPACITY
        .warm_capacity = capacity,
#endif
    };
    tlc_core_t *core;
    if (tlc_core_create(&core, &config) != 0)
        return 1;
    printf("capacity=%u keys=%u occupancy=%.6f hot_capacity=%u payload_bytes=%u\n",
           capacity, count, (double)count / capacity, hot, region.value_size);
    uint64_t wall = getMonotonicNs(), cpu = cpu_ns();
    for (uint32_t i = 0; i < count; i++) {
        lookup_key_t *key = &keys[i];
        key->len = snprintf(key->text, sizeof(key->text), "item:%u", i);
        key->hash = vemb_v16_xxh3_64(key->text, key->len);
        float value[2] = {(float)i, (float)(i + 1)};
        if (tlc_core_put(core, key->text, key->len, key->hash,
                         value, sizeof(value), &key->slot) != 0) {
            fprintf(stderr, "preload failed at key=%u; no read result is valid\n", i);
            return 1;
        }
    }
    uint64_t used_cpu = cpu_ns() - cpu;
    report("preload", count, elapsedNs(wall), used_cpu);
    for (unsigned pass = 0; pass < 2; pass++) {
        uint64_t rng = 1;
        wall = getMonotonicNs(); cpu = cpu_ns();
        for (uint32_t i = 0; i < iterations; i++) {
            rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
            const lookup_key_t *key = &keys[rng % count];
            tlc_warm_location_t location;
            if (tlc_core_get_warm_location_stable_read(core, key->text, key->len,
                    key->hash, &location) != 0 || location.local_slot != key->slot ||
                    location.offset != (uint64_t)key->slot * region.value_size) {
                fprintf(stderr, "hit lookup failed\n");
                return 1;
            }
        }
        used_cpu = cpu_ns() - cpu;
        report(pass ? "hit_steady" : "hit_warmup", iterations, elapsedNs(wall), used_cpu);
    }
    const char absent[] = "absent-key";
    uint64_t hash = vemb_v16_xxh3_64(absent, sizeof(absent) - 1);
    wall = getMonotonicNs(); cpu = cpu_ns();
    for (uint32_t i = 0; i < misses; i++) {
        tlc_warm_location_t location;
        if (tlc_core_get_warm_location_stable_read(core, absent, sizeof(absent) - 1,
                                                   hash, &location) == 0) {
            fprintf(stderr, "missing key unexpectedly found\n");
            return 1;
        }
    }
    used_cpu = cpu_ns() - cpu;
    report("miss", misses, elapsedNs(wall), used_cpu);
    tlc_core_destroy(core);
    zfree(keys); zfree(slots); zfree(payload);
    return 0;
}
