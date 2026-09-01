#include "../src/vemb_v16_tlc.h"
#include "../src/tlc_core.h"
#include "../src/monotonic.h"
#include "../src/vemb_v16_remote_meta.h"
#include "../src/vemb_v16_ub_rpc.h"
#include "../src/zmalloc.h"

#include <assert.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

static void fill_vector(float *vector, uint32_t dim, uint32_t seed) {
    for (uint32_t i = 0; i < dim; i++)
        vector[i] = (float)(seed + i);
}

#if TLC_CORE_ENABLE_COLD_LAYER
static void make_key(char *buf, size_t len, uint32_t id) {
    snprintf(buf, len, "item:%u", id);
}
#endif

typedef struct fuzzy_checkpoint_writer_arg {
    vemb_v16_tlc_t *tlc;
    uint32_t worker_id;
    uint32_t count;
    atomic_int *failures;
} fuzzy_checkpoint_writer_arg_t;

static void *fuzzy_checkpoint_writer(void *opaque) {
    fuzzy_checkpoint_writer_arg_t *arg = opaque;
    for (uint32_t i = 0; i < arg->count; i++) {
        char key[64];
        snprintf(key, sizeof(key), "fuzzy:%u:%u", arg->worker_id, i);
        float value[2] = {(float)arg->worker_id, (float)i};
        uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
        vemb_v16_vector_handle_t handle;
        uint32_t warm_slot = TLC_CORE_INVALID_SLOT;
        if (vemb_v16_tlc_put_with_epoch(arg->tlc, key, strlen(key), key_hash,
                                        value, sizeof(value), 1, &handle,
                                        &warm_slot) != 0)
            atomic_fetch_add_explicit(arg->failures, 1, memory_order_relaxed);
        if (i == arg->count / 2)
            usleep(10000);
        if ((i & 7u) == 0)
            sched_yield();
    }
    return NULL;
}

static void init_fuzzy_slot_meta(vemb_v16_warm_slot_meta_t *slot_meta,
                                 uint32_t count,
                                 uint32_t region_id) {
    for (uint32_t i = 0; i < count; i++) {
        atomic_init(&slot_meta[i].state, VEMB_V16_WARM_SLOT_FREE);
        atomic_init(&slot_meta[i].owner_generation, 0);
        atomic_init(&slot_meta[i].write_seq, 0);
        atomic_init(&slot_meta[i].last_access_ns, 0);
        atomic_init(&slot_meta[i].clock_bit, 0);
        atomic_init(&slot_meta[i].cold_state, VEMB_V16_WARM_SLOT_COLD_NONE);
        slot_meta[i].region_id = region_id;
        slot_meta[i].local_slot = i;
    }
}

static void test_fuzzy_checkpoint_concurrent_writes(void) {
    enum { SLOT_COUNT = 4096, WORKER_COUNT = 4, WRITES_PER_WORKER = 128 };
    float *region = zmalloc(SLOT_COUNT * 2 * sizeof(float));
    vemb_v16_warm_slot_meta_t *slot_meta = zcalloc_num(
        SLOT_COUNT, sizeof(*slot_meta));
    assert(region != NULL && slot_meta != NULL);
    init_fuzzy_slot_meta(slot_meta, SLOT_COUNT, 910);
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 910,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = SLOT_COUNT * 2 * sizeof(float),
        .value_size = 2 * sizeof(float),
        .slot_meta = slot_meta,
    };
    char directory[] = "/tmp/tlc-fuzzy-checkpoint-XXXXXX";
    assert(mkdtemp(directory) != NULL);
    tlc_cold_config_t cold_config = {
        .directory = directory,
        .segment_bytes = 4096,
        .queue_capacity = 32,
        .group_max_entries = 16,
        .group_max_delay_us = 1000,
    };
    vemb_v16_tlc_t *tlc = NULL;
    assert(vemb_v16_tlc_create(&tlc, 2, SLOT_COUNT, &warm, 1, 1) == 0);
    assert(vemb_v16_tlc_enable_cold(tlc, &cold_config) == 0);
    pthread_t threads[WORKER_COUNT];
    fuzzy_checkpoint_writer_arg_t args[WORKER_COUNT];
    atomic_int failures;
    atomic_init(&failures, 0);
    for (uint32_t i = 0; i < WORKER_COUNT; i++) {
        args[i] = (fuzzy_checkpoint_writer_arg_t){
            .tlc = tlc, .worker_id = i, .count = WRITES_PER_WORKER,
            .failures = &failures};
        assert(pthread_create(&threads[i], NULL, fuzzy_checkpoint_writer,
                              &args[i]) == 0);
    }
    usleep(1000);
    tlc_cold_checkpoint_result_t checkpoint_result;
    assert(vemb_v16_tlc_publish_checkpoint(tlc, 1, 1,
                                           &checkpoint_result) == 0);
    for (uint32_t i = 0; i < WORKER_COUNT; i++)
        assert(pthread_join(threads[i], NULL) == 0);
    assert(atomic_load_explicit(&failures, memory_order_acquire) == 0);
    vemb_v16_tlc_destroy(tlc);

    vemb_v16_warm_slot_meta_t *recovered_slot_meta = zcalloc_num(
        SLOT_COUNT, sizeof(*recovered_slot_meta));
    float *recovered_region = zmalloc(SLOT_COUNT * 2 * sizeof(float));
    assert(recovered_slot_meta != NULL && recovered_region != NULL);
    init_fuzzy_slot_meta(recovered_slot_meta, SLOT_COUNT, 911);
    vemb_v16_tlc_warm_region_t recovered_warm = warm;
    recovered_warm.region_id = 911;
    recovered_warm.mapped_addr = recovered_region;
    recovered_warm.slot_meta = recovered_slot_meta;
    vemb_v16_tlc_t *recovered = NULL;
    assert(vemb_v16_tlc_create(&recovered, 2, SLOT_COUNT,
                               &recovered_warm, 1, 1) == 0);
    assert(vemb_v16_tlc_enable_cold(recovered, &cold_config) == 0);
    for (uint32_t worker = 0; worker < WORKER_COUNT; worker++) {
        for (uint32_t i = 0; i < WRITES_PER_WORKER; i++) {
            char key[64];
            snprintf(key, sizeof(key), "fuzzy:%u:%u", worker, i);
            uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
            tlc_core_key_migration_info_t info;
            assert(tlc_core_get_migration_info(recovered->core, key,
                                               strlen(key), key_hash,
                                               &info) == 0);
            assert(info.key_version == 1);
            float expected[2] = {(float)worker, (float)i};
            float stored[2] = {0};
            assert(tlc_core_copy_warm_location_value(
                       recovered->core, key_hash, &info.location, stored,
                       sizeof(stored), 1024) == 0);
            assert(memcmp(stored, expected, sizeof(expected)) == 0);
        }
    }
    vemb_v16_tlc_destroy(recovered);
    zfree(recovered_region);
    zfree(recovered_slot_meta);
    zfree(region);
    zfree(slot_meta);
    printf("fuzzy checkpoint concurrent writes: PASS\n");
}

static void assert_recovered_value(vemb_v16_tlc_t *tlc,
                                   const char *key,
                                   const float expected[2]) {
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
    tlc_core_key_migration_info_t info;
    assert(tlc_core_get_migration_info(tlc->core, key, strlen(key), key_hash,
                                       &info) == 0);
    float actual[2] = {0};
    assert(tlc_core_copy_warm_location_value(tlc->core, key_hash,
                                             &info.location, actual,
                                             sizeof(actual), 1024) == 0);
    assert(memcmp(actual, expected, sizeof(actual)) == 0);
}

static void test_checkpoint_aof_full_recovery_consistency(void) {
    enum { SLOT_COUNT = 32 };
    float *region = zmalloc(SLOT_COUNT * 2 * sizeof(float));
    vemb_v16_warm_slot_meta_t *slot_meta = zcalloc_num(
        SLOT_COUNT, sizeof(*slot_meta));
    assert(region != NULL && slot_meta != NULL);
    init_fuzzy_slot_meta(slot_meta, SLOT_COUNT, 920);
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 920,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = SLOT_COUNT * 2 * sizeof(float),
        .value_size = 2 * sizeof(float),
        .slot_meta = slot_meta,
    };
    char directory[] = "/tmp/tlc-full-recovery-XXXXXX";
    assert(mkdtemp(directory) != NULL);
    tlc_cold_config_t cold_config = {
        .directory = directory,
        .segment_bytes = 512,
        .queue_capacity = 8,
        .group_max_entries = 4,
        .group_max_delay_us = 1000,
    };
    vemb_v16_tlc_t *tlc = NULL;
    assert(vemb_v16_tlc_create(&tlc, 2, SLOT_COUNT, &warm, 1, 1) == 0);
    assert(vemb_v16_tlc_enable_cold(tlc, &cold_config) == 0);
    const char *keys[] = {"recover:keep", "recover:update", "recover:delete"};
    const float initial[][2] = {{1, 2}, {3, 4}, {5, 6}};
    for (size_t i = 0; i < 3; i++) {
        uint64_t key_hash = vemb_v16_xxh3_64_str(keys[i], strlen(keys[i]));
        vemb_v16_vector_handle_t handle;
        uint32_t warm_slot;
        assert(vemb_v16_tlc_put_with_epoch(tlc, keys[i], strlen(keys[i]),
                                           key_hash, initial[i],
                                           sizeof(initial[i]), 1, &handle,
                                           &warm_slot) == 0);
    }
    tlc_cold_checkpoint_result_t checkpoint_result;
    assert(vemb_v16_tlc_publish_checkpoint(tlc, 1, 1,
                                           &checkpoint_result) == 0);

    const float updated[] = {30, 40};
    uint64_t update_hash = vemb_v16_xxh3_64_str(keys[1], strlen(keys[1]));
    vemb_v16_vector_handle_t handle;
    uint32_t warm_slot;
    assert(vemb_v16_tlc_put_with_epoch(tlc, keys[1], strlen(keys[1]),
                                       update_hash, updated, sizeof(updated),
                                       2, &handle, &warm_slot) == 0);
    uint64_t delete_hash = vemb_v16_xxh3_64_str(keys[2], strlen(keys[2]));
    assert(tlc_core_delete_with_epoch(tlc->core, keys[2], strlen(keys[2]),
                                      delete_hash, 2, NULL) == 0);
    const char new_key[] = "recover:new";
    const float new_value[] = {70, 80};
    uint64_t new_hash = vemb_v16_xxh3_64_str(new_key, sizeof(new_key) - 1);
    assert(vemb_v16_tlc_put_with_epoch(tlc, new_key, sizeof(new_key) - 1,
                                       new_hash, new_value, sizeof(new_value),
                                       1, &handle, &warm_slot) == 0);
    vemb_v16_tlc_destroy(tlc);

    float *recovered_region = zmalloc(SLOT_COUNT * 2 * sizeof(float));
    vemb_v16_warm_slot_meta_t *recovered_slot_meta = zcalloc_num(
        SLOT_COUNT, sizeof(*recovered_slot_meta));
    assert(recovered_region != NULL && recovered_slot_meta != NULL);
    init_fuzzy_slot_meta(recovered_slot_meta, SLOT_COUNT, 921);
    warm.region_id = 921;
    warm.mapped_addr = recovered_region;
    warm.slot_meta = recovered_slot_meta;
    vemb_v16_tlc_t *recovered = NULL;
    assert(vemb_v16_tlc_create(&recovered, 2, SLOT_COUNT, &warm, 1, 1) == 0);
    assert(vemb_v16_tlc_enable_cold(recovered, &cold_config) == 0);
    assert_recovered_value(recovered, keys[0], initial[0]);
    assert_recovered_value(recovered, keys[1], updated);
    assert_recovered_value(recovered, new_key, new_value);
    tlc_core_key_migration_info_t deleted_info;
    assert(tlc_core_get_migration_info(recovered->core, keys[2], strlen(keys[2]),
                                       delete_hash, &deleted_info) == 0);
    assert(deleted_info.tombstone != 0);
    assert(IS_INVALID_LOCATION(deleted_info.location));
    vemb_v16_tlc_destroy(recovered);
    zfree(recovered_region);
    zfree(recovered_slot_meta);
    zfree(region);
    zfree(slot_meta);
    printf("checkpoint + AOF full recovery consistency: PASS\n");
}

static void test_single_node_recovery_failure_blocks_startup(void) {
    enum { SLOT_COUNT = 4 };
    char directory[] = "/tmp/tlc-single-node-recovery-fail-XXXXXX";
    assert(mkdtemp(directory) != NULL);
    tlc_cold_config_t cold_config = {
        .directory = directory, .segment_bytes = 4096, .queue_capacity = 2,
        .group_max_entries = 1, .group_max_delay_us = 1000};
    tlc_cold_t *cold = NULL;
    assert(tlc_cold_open(&cold, &cold_config) == 0);
    const char key[] = "single-node-failure-key";
    const char value[] = "single-node-failure-value";
    tlc_cold_event_input_t event = {
        .term = 1, .op = TLC_COLD_OP_PUT, .meta_shard_id = 0, .version = 1,
        .key = key, .key_len = sizeof(key) - 1, .value = value,
        .value_len = sizeof(value) - 1};
    assert(tlc_cold_submit(cold, &event, TLC_COLD_ACK_DURABLE, NULL) == 0);
    tlc_cold_close(cold);
    char path[4096];
    assert(snprintf(path, sizeof(path), "%s/aof-%020d.log", directory, 0) <
           (int)sizeof(path));
    int fd = open(path, O_RDWR);
    assert(fd >= 0);
    uint8_t corrupt = 0;
    assert(read(fd, &corrupt, sizeof(corrupt)) == (ssize_t)sizeof(corrupt));
    corrupt ^= 0xff;
    assert(lseek(fd, 0, SEEK_SET) == 0);
    assert(write(fd, &corrupt, sizeof(corrupt)) == (ssize_t)sizeof(corrupt));
    assert(close(fd) == 0);

    float *region = zmalloc(SLOT_COUNT * 2 * sizeof(float));
    vemb_v16_warm_slot_meta_t *slot_meta = zcalloc_num(
        SLOT_COUNT, sizeof(*slot_meta));
    assert(region != NULL && slot_meta != NULL);
    init_fuzzy_slot_meta(slot_meta, SLOT_COUNT, 930);
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 930, .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1, .weight = 1, .mapped_addr = region,
        .region_bytes = SLOT_COUNT * 2 * sizeof(float),
        .value_size = 2 * sizeof(float), .slot_meta = slot_meta};
    vemb_v16_tlc_t *tlc = NULL;
    assert(vemb_v16_tlc_create(&tlc, 2, SLOT_COUNT, &warm, 1, 1) == 0);
    assert(vemb_v16_tlc_enable_cold(tlc, &cold_config) != 0);
    vemb_v16_tlc_destroy(tlc);
    zfree(region);
    zfree(slot_meta);
    printf("single-node recovery failure blocks startup: PASS\n");
}

static void test_recovery_strict_term_and_state_boundaries(void) {
    const uint32_t value_size = 2 * sizeof(float);
    const char *key = "strict-term-key";
    const float first_value[] = {1, 2};
    const float stale_value[] = {3, 4};
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
    char directory[] = "/tmp/tlc-strict-term-XXXXXX";
    assert(mkdtemp(directory) != NULL);
    tlc_cold_config_t cold_config = {
        .directory = directory, .segment_bytes = 512, .queue_capacity = 4,
        .group_max_entries = 2, .group_max_delay_us = 1000};

    float *region = zmalloc(4 * value_size);
    vemb_v16_warm_slot_meta_t *slot_meta = zcalloc_num(
        4, sizeof(*slot_meta));
    assert(region != NULL && slot_meta != NULL);
    init_fuzzy_slot_meta(slot_meta, 4, 940);
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 940, .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1, .weight = 1, .mapped_addr = region,
        .region_bytes = 4 * value_size, .value_size = value_size,
        .slot_meta = slot_meta};
    vemb_v16_tlc_t *tlc = NULL;
    assert(vemb_v16_tlc_create(&tlc, 2, 4, &warm, 1, 1) == 0);
    assert(vemb_v16_tlc_enable_cold(tlc, &cold_config) == 0);
    vemb_v16_vector_handle_t handle;
    uint32_t warm_slot;
    assert(vemb_v16_tlc_put_with_epoch(
               tlc, key, strlen(key), key_hash, first_value, value_size, 2,
               &handle, &warm_slot) == 0);
    tlc_cold_checkpoint_result_t checkpoint_result;
    assert(vemb_v16_tlc_publish_checkpoint(tlc, 1, 2,
                                           &checkpoint_result) == 0);
    vemb_v16_tlc_destroy(tlc);
    zfree(region);
    zfree(slot_meta);

    tlc_cold_t *cold = NULL;
    assert(tlc_cold_open(&cold, &cold_config) == 0);
    tlc_cold_event_input_t stale_event = {
        .term = 1,
        .op = TLC_COLD_OP_PUT,
        .meta_shard_id = (uint32_t)(vemb_v16_mix32_u64(key_hash) & 255u),
        .version = 2,
        .key = key,
        .key_len = (uint32_t)strlen(key),
        .value = stale_value,
        .value_len = value_size};
    assert(tlc_cold_submit(cold, &stale_event, TLC_COLD_ACK_DURABLE, NULL) ==
           0);
    tlc_cold_close(cold);

    region = zmalloc(4 * value_size);
    slot_meta = zcalloc_num(4, sizeof(*slot_meta));
    assert(region != NULL && slot_meta != NULL);
    init_fuzzy_slot_meta(slot_meta, 4, 941);
    warm.region_id = 941;
    warm.mapped_addr = region;
    warm.slot_meta = slot_meta;
    tlc = NULL;
    assert(vemb_v16_tlc_create(&tlc, 2, 4, &warm, 1, 1) == 0);
    assert(vemb_v16_tlc_enable_cold(tlc, &cold_config) != 0);
    vemb_v16_tlc_destroy(tlc);
    zfree(region);
    zfree(slot_meta);
    printf("strict recovery term boundary: PASS\n");

    char state_directory[] = "/tmp/tlc-strict-state-XXXXXX";
    assert(mkdtemp(state_directory) != NULL);
    cold_config.directory = state_directory;
    region = zmalloc(4 * value_size);
    slot_meta = zcalloc_num(4, sizeof(*slot_meta));
    assert(region != NULL && slot_meta != NULL);
    init_fuzzy_slot_meta(slot_meta, 4, 942);
    warm.region_id = 942;
    warm.mapped_addr = region;
    warm.slot_meta = slot_meta;
    tlc = NULL;
    assert(vemb_v16_tlc_create(&tlc, 2, 4, &warm, 1, 1) == 0);
    assert(vemb_v16_tlc_enable_cold(tlc, &cold_config) == 0);
    const char *key2 = "strict-state-key-2";
    const float second_value[] = {5, 6};
    uint64_t key2_hash = vemb_v16_xxh3_64_str(key2, strlen(key2));
    assert(vemb_v16_tlc_put_with_epoch(
               tlc, key, strlen(key), key_hash, first_value, value_size, 1,
               &handle, &warm_slot) == 0);
    assert(vemb_v16_tlc_put_with_epoch(
               tlc, key2, strlen(key2), key2_hash, second_value, value_size, 1,
               &handle, &warm_slot) == 0);
    assert(vemb_v16_tlc_publish_checkpoint(tlc, 1, 1,
                                           &checkpoint_result) == 0);
    vemb_v16_tlc_destroy(tlc);
    zfree(region);
    zfree(slot_meta);

    region = zmalloc(value_size);
    slot_meta = zcalloc_num(1, sizeof(*slot_meta));
    assert(region != NULL && slot_meta != NULL);
    init_fuzzy_slot_meta(slot_meta, 1, 943);
    warm.region_id = 943;
    warm.mapped_addr = region;
    warm.region_bytes = value_size;
    warm.slot_meta = slot_meta;
    tlc = NULL;
    assert(vemb_v16_tlc_create(&tlc, 2, 1, &warm, 1, 1) == 0);
    assert(vemb_v16_tlc_enable_cold(tlc, &cold_config) != 0);
    assert(vemb_v16_tlc_recover_cold(tlc) != 0);
    vemb_v16_tlc_destroy(tlc);
    zfree(region);
    zfree(slot_meta);
    printf("checkpoint state load failure boundary: PASS\n");
}

static void init_test_allocator(vemb_v16_warm_region_header_t *allocator,
                                uint32_t region_id,
                                uint32_t capacity_slots) {
    memset(allocator, 0, sizeof(*allocator));
    atomic_init(&allocator->magic, VEMB_V16_WARM_REGION_LAYOUT_MAGIC);
    allocator->version = VEMB_V16_WARM_REGION_LAYOUT_VERSION;
    allocator->region_id = region_id;
    allocator->capacity_slots = capacity_slots;
}

static vemb_v16_warm_region_header_t *init_test_allocator_with_slot_meta(
        void *backing,
        uint32_t region_id,
        uint32_t capacity_slots) {
    vemb_v16_warm_region_header_t *allocator = backing;
    memset(backing, 0, vemb_v16_warm_region_layout_bytes(capacity_slots));
    atomic_init(&allocator->magic, VEMB_V16_WARM_REGION_LAYOUT_MAGIC);
    allocator->version = VEMB_V16_WARM_REGION_LAYOUT_VERSION;
    allocator->region_id = region_id;
    allocator->capacity_slots = capacity_slots;
    vemb_v16_warm_slot_meta_t *slots =
        vemb_v16_warm_region_slot_meta(allocator);
    for (uint32_t i = 0; i < capacity_slots; i++) {
        slots[i].region_id = region_id;
        slots[i].local_slot = i;
        atomic_init(&slots[i].state, VEMB_V16_WARM_SLOT_FREE);
        atomic_init(&slots[i].owner_generation, 0);
        atomic_init(&slots[i].write_seq, 0);
        atomic_init(&slots[i].last_access_ns, 0);
        atomic_init(&slots[i].clock_bit, 0);
        atomic_init(&slots[i].cold_state, VEMB_V16_WARM_SLOT_COLD_NONE);
    }
    return allocator;
}

typedef struct tlc_ut_slot_meta_entry {
    void *mapped_addr;
    uint32_t region_id;
    uint32_t capacity_slots;
    vemb_v16_warm_slot_meta_t *slot_meta;
} tlc_ut_slot_meta_entry_t;

#define TLC_UT_SLOT_META_REGISTRY_MAX 128u

static tlc_ut_slot_meta_entry_t
    tlc_ut_slot_meta_registry[TLC_UT_SLOT_META_REGISTRY_MAX];

static vemb_v16_warm_slot_meta_t *tlc_ut_slot_meta_acquire(
        void *mapped_addr,
        uint32_t region_id,
        uint32_t capacity_slots) {
    for (uint32_t i = 0; i < TLC_UT_SLOT_META_REGISTRY_MAX; i++) {
        tlc_ut_slot_meta_entry_t *entry = &tlc_ut_slot_meta_registry[i];
        if (entry->mapped_addr == mapped_addr &&
            entry->region_id == region_id &&
            entry->capacity_slots == capacity_slots) {
            return entry->slot_meta;
        }
    }

    for (uint32_t i = 0; i < TLC_UT_SLOT_META_REGISTRY_MAX; i++) {
        tlc_ut_slot_meta_entry_t *entry = &tlc_ut_slot_meta_registry[i];
        if (entry->mapped_addr)
            continue;

        vemb_v16_warm_slot_meta_t *slots =
            calloc(capacity_slots, sizeof(*slots));
        assert(slots);
        for (uint32_t slot = 0; slot < capacity_slots; slot++) {
            slots[slot].region_id = region_id;
            slots[slot].local_slot = slot;
            atomic_init(&slots[slot].state, VEMB_V16_WARM_SLOT_FREE);
            atomic_init(&slots[slot].owner_generation, 0);
            atomic_init(&slots[slot].write_seq, 0);
            atomic_init(&slots[slot].last_access_ns, 0);
            atomic_init(&slots[slot].clock_bit, 0);
            atomic_init(&slots[slot].cold_state, VEMB_V16_WARM_SLOT_COLD_NONE);
        }
        *entry = (tlc_ut_slot_meta_entry_t){
            .mapped_addr = mapped_addr,
            .region_id = region_id,
            .capacity_slots = capacity_slots,
            .slot_meta = slots,
        };
        return slots;
    }

    assert(!"tlc_ut slot_meta registry exhausted");
    return NULL;
}

static void tlc_ut_ensure_slot_meta(vemb_v16_tlc_warm_region_t *warm_region) {
    if (warm_region->slot_meta)
        return;
    assert(warm_region->mapped_addr);
    assert(warm_region->value_size > 0);
    assert(warm_region->region_bytes >= warm_region->value_size);
    uint32_t capacity_slots =
        (uint32_t)(warm_region->region_bytes / warm_region->value_size);
    warm_region->slot_meta = tlc_ut_slot_meta_acquire(warm_region->mapped_addr,
                                                      warm_region->region_id,
                                                      capacity_slots);
}

static void tlc_ut_ensure_regions_slot_meta(
        vemb_v16_tlc_warm_region_t *warm_regions,
        uint32_t warm_region_count) {
    for (uint32_t i = 0; i < warm_region_count; i++)
        tlc_ut_ensure_slot_meta(&warm_regions[i]);
}

static int tlc_ut_create(vemb_v16_tlc_t **out,
                         uint32_t vector_dim,
                         uint32_t max_vectors,
                         vemb_v16_tlc_warm_region_t *warm_regions,
                         uint32_t warm_region_count,
                         uint32_t local_region_weight) {
    tlc_ut_ensure_regions_slot_meta(warm_regions, warm_region_count);
    return vemb_v16_tlc_create(out,
                               vector_dim,
                               max_vectors,
                               warm_regions,
                               warm_region_count,
                               local_region_weight);
}

static void test_persistent_cold_write_order(void) {
    float region[2] = {0};
    vemb_v16_warm_slot_meta_t slot_meta = {0};
    atomic_init(&slot_meta.state, VEMB_V16_WARM_SLOT_FREE);
    atomic_init(&slot_meta.owner_generation, 0);
    atomic_init(&slot_meta.write_seq, 0);
    atomic_init(&slot_meta.last_access_ns, 0);
    atomic_init(&slot_meta.clock_bit, 0);
    atomic_init(&slot_meta.cold_state, VEMB_V16_WARM_SLOT_COLD_NONE);
    slot_meta.region_id = 700;
    slot_meta.local_slot = 0;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 700,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = sizeof(region),
        .slot_meta = &slot_meta,
    };
    char directory[] = "/tmp/tlc-m2-cold-XXXXXX";
    assert(mkdtemp(directory) != NULL);
    tlc_cold_config_t cold_config = {
        .directory = directory,
        .segment_bytes = 1024,
        .queue_capacity = 2,
        .group_max_entries = 2,
        .group_max_delay_us = 1000,
    };
    vemb_v16_tlc_t *tlc = NULL;
    assert(vemb_v16_tlc_create(&tlc, 2, 1, &warm, 1, 1) == 0);
    assert(vemb_v16_tlc_enable_cold(tlc, &cold_config) == 0);
    const char key[] = "m2-key";
    float value[2] = {1.0f, 2.0f};
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, sizeof(key) - 1);
    vemb_v16_vector_handle_t handle;
    uint32_t warm_slot = TLC_CORE_INVALID_SLOT;
    assert(vemb_v16_tlc_put_with_epoch(tlc,
                                       key,
                                       sizeof(key) - 1,
                                       key_hash,
                                       value,
                                       sizeof(value),
                                       1,
                                       &handle,
                                       &warm_slot) == 0);
    tlc_cold_checkpoint_result_t checkpoint_result;
    assert(vemb_v16_tlc_publish_checkpoint(tlc,
                                           1,
                                           1,
                                           &checkpoint_result) == 0);
    assert(checkpoint_result.generation == 1);
    /* ACK_ACCEPTED does not wait for the background fsync; a checkpoint
     * captured immediately after the write may therefore start at seq 0. */
    assert(checkpoint_result.checkpoint_seq <= 1);
    assert(tlc_core_delete_with_epoch(tlc->core,
                                      key,
                                      sizeof(key) - 1,
                                      key_hash,
                                      1,
                                      NULL) == 0);
    vemb_v16_tlc_destroy(tlc);

    tlc_cold_t *cold = NULL;
    assert(tlc_cold_open(&cold, &cold_config) == 0);
    tlc_cold_progress_t progress;
    assert(tlc_cold_get_progress(cold, &progress) == 0);
    assert(progress.appended_seq == 2);
    assert(progress.durable_seq == 2);
    tlc_cold_close(cold);

    vemb_v16_tlc_t *recovered = NULL;
    assert(vemb_v16_tlc_create(&recovered, 2, 1, &warm, 1, 1) == 0);
    assert(vemb_v16_tlc_enable_cold(recovered, &cold_config) == 0);
    tlc_core_key_migration_info_t info;
    assert(tlc_core_get_migration_info(recovered->core,
                                       key,
                                       sizeof(key) - 1,
                                       key_hash,
                                       &info) == 0);
    assert(info.key_version == 2);
    assert(info.tombstone == 1);
    vemb_v16_tlc_destroy(recovered);
}

static int tlc_ut_attach_warm_region(vemb_v16_tlc_t *tlc,
                                     vemb_v16_tlc_warm_region_t *warm_region) {
    tlc_ut_ensure_slot_meta(warm_region);
    return vemb_v16_tlc_attach_warm_region(tlc, warm_region);
}

#define vemb_v16_tlc_create tlc_ut_create
#define vemb_v16_tlc_attach_warm_region tlc_ut_attach_warm_region

static void set_rpc_ring(vemb_v16_ub_rpc_ring_config_t *ring,
                         const char *path) {
    memset(ring, 0, sizeof(*ring));
    ring->backend_type = VEMB_V16_REGION_LOCAL_SHM;
    snprintf(ring->path, sizeof(ring->path), "%s", path);
}

static void cleanup_rpc_rings(const char *req_a_b,
                              const char *req_b_a,
                              const char *resp_a_b,
                              const char *resp_b_a) {
    shm_unlink(req_a_b);
    shm_unlink(req_b_a);
    shm_unlink(resp_a_b);
    shm_unlink(resp_b_a);
}

static void make_rpc_peers(vemb_v16_ub_rpc_peer_t *peer_a_to_b,
                           uint32_t owner_b,
                           vemb_v16_ub_rpc_peer_t *peer_b_to_a,
                           uint32_t owner_a,
                           const char *req_a_b,
                           const char *req_b_a,
                           const char *resp_a_b,
                           const char *resp_b_a) {
    memset(peer_a_to_b, 0, sizeof(*peer_a_to_b));
    memset(peer_b_to_a, 0, sizeof(*peer_b_to_a));
    peer_a_to_b->owner_id = owner_b;
    set_rpc_ring(&peer_a_to_b->request, req_a_b);
    set_rpc_ring(&peer_a_to_b->response, resp_b_a);
    set_rpc_ring(&peer_a_to_b->inbound_request, req_b_a);
    set_rpc_ring(&peer_a_to_b->outbound_response, resp_a_b);

    peer_b_to_a->owner_id = owner_a;
    set_rpc_ring(&peer_b_to_a->request, req_b_a);
    set_rpc_ring(&peer_b_to_a->response, resp_a_b);
    set_rpc_ring(&peer_b_to_a->inbound_request, req_a_b);
    set_rpc_ring(&peer_b_to_a->outbound_response, resp_b_a);
}

static void assert_region_stats(vemb_v16_tlc_t *tlc,
                                uint32_t region_id,
                                uint32_t expected_used_slots,
                                uint32_t expected_full) {
    tlc_core_region_stats_t stats[TLC_CORE_MAX_WARM_REGIONS];
    uint32_t count = tlc_core_get_region_stats(tlc->core,
                                               stats,
                                               TLC_CORE_MAX_WARM_REGIONS);
    for (uint32_t i = 0; i < count; i++) {
        if (stats[i].region_id != region_id)
            continue;
        assert(stats[i].used_slots == expected_used_slots);
        assert(stats[i].full == expected_full);
        return;
    }
    assert(!"region stats not found");
}

static void test_put_get_handle(void) {
    enum { dim = 4, max_vectors = 8 };
    float region[dim * max_vectors];
    float vector[dim];
    vemb_v16_warm_region_header_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 7,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = 0;
    const char *key = "item:1";
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 7, max_vectors);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm, 1, 4) == 0);
    fill_vector(vector, dim, 10);
    assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                            vector, sizeof(vector), &handle, &warm_slot) == 0);
    assert(handle.region_id == 7);
    assert(handle.bytes == sizeof(vector));
    assert(handle.local_slot == 0);
    assert(handle.owner_generation == 1);
    assert(handle.offset == 0);
    assert(warm_slot == 0);
    assert(memcmp(region, vector, sizeof(vector)) == 0);

    memset(&handle, 0, sizeof(handle));
    warm_slot = 99;
    assert(vemb_v16_tlc_get_handle(tlc, key, (uint32_t)strlen(key), key_hash,
                                   &handle, &warm_slot) == 0);
    assert(handle.region_id == 7);
    assert(handle.offset == 0);
    assert(handle.local_slot == 0);
    assert(handle.owner_generation == 1);
    assert(warm_slot == 0);
    vemb_v16_tlc_destroy(tlc);
}

static void test_overwrite_and_capacity(void) {
    enum { dim = 2, max_vectors = 1 };
    float region[dim * max_vectors];
    float first[dim], second[dim];
    vemb_v16_warm_region_header_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 0,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = 0;
    const char *key = "only";
    const char *evicting = "extra";
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
    uint64_t evicting_hash = vemb_v16_xxh3_64_str(evicting, strlen(evicting));

    init_test_allocator(&allocator, 0, max_vectors);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm, 1, 4) == 0);
    fill_vector(first, dim, 1);
    fill_vector(second, dim, 100);
    assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                            first, sizeof(first), &handle, &warm_slot) == 0);
    assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                            second, sizeof(second), &handle, &warm_slot) == 0);
    assert(warm_slot == 0);
    assert(memcmp(region, second, sizeof(second)) == 0);
#if TLC_CORE_ALLOW_LRU_EVICTION
    memset(&handle, 0xff, sizeof(handle));
    warm_slot = 0;
    assert(vemb_v16_tlc_put(tlc, evicting, (uint32_t)strlen(evicting), evicting_hash,
                            first, sizeof(first), &handle, &warm_slot) == 0);
    assert(warm_slot == 0);
    assert(handle.bytes == sizeof(first));
    assert(handle.owner_generation == 2);
    assert(vemb_v16_tlc_get_handle(tlc, evicting, (uint32_t)strlen(evicting),
                                   evicting_hash, &handle, &warm_slot) == 0);
    tlc_core_stats_t stats;
    tlc_core_get_stats(tlc->core, &stats);
    assert(stats.warm_same_key_overwrite >= 1);
    assert(stats.warm_eviction_success >= 1);
#else
    (void)evicting;
    (void)evicting_hash;
    tlc_core_stats_t stats;
    tlc_core_get_stats(tlc->core, &stats);
    assert(stats.warm_same_key_overwrite >= 1);
    assert(stats.warm_eviction_success == 0);
#endif
    vemb_v16_tlc_destroy(tlc);
}

static void test_eviction_rejects_stale_handle(void) {
    enum { dim = 2, max_vectors = 1 };
    float region[dim * max_vectors];
    float first[dim], second[dim];
    vemb_v16_warm_region_header_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 6,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_vector_handle_t stale = {0};
    vemb_v16_vector_handle_t fresh = {0};
    uint32_t warm_slot = UINT32_MAX;
    const uint8_t *bytes = NULL;
    uint32_t len = 0;
    const char *key1 = "stale:one";
    const char *key2 = "stale:two";
    uint64_t key1_hash = vemb_v16_xxh3_64_str(key1, strlen(key1));
    uint64_t key2_hash = vemb_v16_xxh3_64_str(key2, strlen(key2));

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 6, max_vectors);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm, 1, 4) == 0);
    fill_vector(first, dim, 11);
    fill_vector(second, dim, 22);
    assert(vemb_v16_tlc_put(tlc, key1, (uint32_t)strlen(key1), key1_hash,
                            first, sizeof(first), &stale, &warm_slot) == 0);
    assert(vemb_v16_tlc_vector_slice(tlc, &stale, &bytes, &len) == 0);
    assert(len == sizeof(first));
    assert(memcmp(bytes, first, sizeof(first)) == 0);

#if TLC_CORE_ALLOW_LRU_EVICTION
    assert(vemb_v16_tlc_put(tlc, key2, (uint32_t)strlen(key2), key2_hash,
                            second, sizeof(second), &fresh, &warm_slot) == 0);
    assert(fresh.local_slot == stale.local_slot);
    assert(fresh.owner_generation == stale.owner_generation + 1);
    assert(vemb_v16_tlc_vector_slice(tlc, &stale, &bytes, &len) != 0);
    assert(vemb_v16_tlc_vector_slice(tlc, &fresh, &bytes, &len) == 0);
    assert(len == sizeof(second));
    assert(memcmp(bytes, second, sizeof(second)) == 0);
    tlc_core_stats_t stats;
    tlc_core_get_stats(tlc->core, &stats);
    assert(stats.warm_stale_handle_reject >= 1);
#else
    assert(vemb_v16_tlc_put(tlc, key2, (uint32_t)strlen(key2), key2_hash,
                            second, sizeof(second), &fresh, &warm_slot) != 0);
    assert(vemb_v16_tlc_vector_slice(tlc, &stale, &bytes, &len) == 0);
    assert(len == sizeof(first));
    assert(memcmp(bytes, first, sizeof(first)) == 0);
#endif
    vemb_v16_tlc_destroy(tlc);
}

#if TLC_CORE_ENABLE_COLD_LAYER
static void test_cold_read_through_promotes_warm_handle(void) {
    enum { dim = 3, max_vectors = 4 };
    float region[dim * max_vectors];
    float vector[dim];
    vemb_v16_warm_region_header_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 42,
        .backend_type = VEMB_V16_REGION_UB,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = 99;
    const char *key = "cold-key";
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 42, max_vectors);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm, 1, 4) == 0);
    fill_vector(vector, dim, 200);
    assert(tlc_core_cold_append(tlc->core, key, (uint32_t)strlen(key), key_hash,
                                    vector, sizeof(vector)) == 0);
    assert(vemb_v16_tlc_get_handle(tlc, key, (uint32_t)strlen(key), key_hash,
                                   &handle, &warm_slot) == 0);
    assert(handle.region_id == 42);
    assert(handle.bytes == sizeof(vector));
    assert(handle.offset == 0);
    assert(warm_slot == 0);
    assert(memcmp(region, vector, sizeof(vector)) == 0);
    vemb_v16_tlc_destroy(tlc);
}
#endif

#if !TLC_CORE_ENABLE_COLD_LAYER
static void test_disabled_cold_append_is_noop(void) {
    enum { dim = 2, max_vectors = 4 };
    float region[dim * max_vectors];
    float vector[dim];
    vemb_v16_warm_region_header_t allocator;
    tlc_core_t *core = NULL;
    tlc_core_warm_region_config_t warm = {
        .region_id = 42,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .value_size = sizeof(vector),
        .region_bytes = sizeof(region),
        .mapped_addr = (uint8_t *)region,
        .slot_meta = NULL,
    };
    tlc_core_config_t config = {
        .value_size = sizeof(vector),
        .warm_capacity = max_vectors,
        .hot_capacity = 4,
        .warm_regions = &warm,
        .warm_region_count = 1,
        .local_region_weight = 1,
    };
    const char *key = "cold-disabled";
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
    tlc_warm_location_t location = {0};

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 42, max_vectors);
    warm.slot_meta = tlc_ut_slot_meta_acquire(warm.mapped_addr,
                                              warm.region_id,
                                              max_vectors);
    assert(tlc_core_create(&core, &config) == 0);
    fill_vector(vector, dim, 42);
    assert(tlc_core_cold_append(core,
                                key,
                                (uint32_t)strlen(key),
                                key_hash,
                                vector,
                                sizeof(vector)) != 0);
    assert(tlc_core_get_warm_location(core,
                                      key,
                                      (uint32_t)strlen(key),
                                      key_hash,
                                      &location) != 0);
    tlc_core_destroy(core);
}
#endif

static void test_cold_same_key_updates_do_not_exhaust_log(void) {
    enum { dim = 2, max_vectors = 1 };
    float region[dim * max_vectors];
    float vector[dim];
    float expected[dim];
    vemb_v16_warm_region_header_t allocator;
    tlc_core_t *core = NULL;
    tlc_core_warm_region_config_t warm = {
        .region_id = 77,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .value_size = sizeof(vector),
        .region_bytes = sizeof(region),
        .mapped_addr = (uint8_t *)region,
        .slot_meta = NULL,
    };
    tlc_core_config_t config = {
        .value_size = sizeof(vector),
        .warm_capacity = max_vectors,
        .hot_capacity = 4,
        .warm_regions = &warm,
        .warm_region_count = 1,
        .local_region_weight = 1,
    };
    const char *key = "hot-update";
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
    tlc_warm_location_t location = {0};

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 77, max_vectors);
    warm.slot_meta = tlc_ut_slot_meta_acquire(warm.mapped_addr,
                                              warm.region_id,
                                              max_vectors);
    assert(tlc_core_create(&core, &config) == 0);
    for (uint32_t i = 0; i < 16; i++) {
        fill_vector(vector, dim, 1000 + i);
        assert(tlc_core_put_location_epoch(core,
                                           key,
                                           (uint32_t)strlen(key),
                                           key_hash,
                                           vector,
                                           sizeof(vector),
                                           0,
                                           0,
                                           &location) == 0);
    }
    fill_vector(expected, dim, 1015);
    assert(tlc_core_get_warm_location(core,
                                      key,
                                      (uint32_t)strlen(key),
                                      key_hash,
                                      &location) == 0);
    assert(location.bytes == sizeof(expected));
    assert(memcmp((uint8_t *)region + location.offset,
                  expected,
                  sizeof(expected)) == 0);
    tlc_core_destroy(core);
}

static void test_hot_is_cache_only(void) {
    /*
     * TLC warm placement is set-associative and hash-routed, so a region can
     * evict within one set before the whole payload view is numerically full.
     * Keep this test within a stable no-eviction budget and verify the hot
     * layer is only a cache over real warm handles.
     */
    enum { dim = 2, max_vectors = 8 };
    float region[dim * max_vectors];
    float vector[dim];
    vemb_v16_warm_region_header_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 5,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = 0;
    char key[32];

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 5, max_vectors);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm, 1, 4) == 0);
    for (uint32_t i = 0; i < max_vectors; i++) {
        snprintf(key, sizeof(key), "hot-cache:%u", i);
        fill_vector(vector, dim, i + 1);
        uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
        assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                                vector, sizeof(vector),
                                &handle, &warm_slot) == 0);
    }
    for (uint32_t i = 0; i < max_vectors; i++) {
        snprintf(key, sizeof(key), "hot-cache:%u", i);
        uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
        memset(&handle, 0, sizeof(handle));
        warm_slot = UINT32_MAX;
        assert(vemb_v16_tlc_get_handle(tlc, key, (uint32_t)strlen(key),
                                       key_hash, &handle, &warm_slot) == 0);
        assert(handle.region_id == 5);
        assert(handle.offset == (uint64_t)warm_slot * sizeof(vector));
        assert(warm_slot < max_vectors);
    }
    vemb_v16_tlc_destroy(tlc);
}

#if TLC_CORE_ENABLE_COLD_LAYER
static void test_prefill_distribution_stays_warm(void) {
    enum { dim = 1, max_vectors = 131072, prefill = 65536 };
    float *region = calloc((size_t)dim * max_vectors, sizeof(*region));
    float vector[dim];
    vemb_v16_warm_region_header_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 19,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .mapped_addr = region,
        .region_bytes = (uint64_t)sizeof(*region) * dim * max_vectors,
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    uint32_t cold_only_writes = 0;
    char key[32];

    assert(region);
    init_test_allocator(&allocator, 19, max_vectors);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm, 1, 4) == 0);
    for (uint32_t i = 0; i < prefill; i++) {
        make_key(key, sizeof(key), i);
        fill_vector(vector, dim, i);
        uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
        memset(&handle, 0, sizeof(handle));
        warm_slot = UINT32_MAX;
        assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                                vector, sizeof(vector),
                                &handle, &warm_slot) == 0);
        if (warm_slot == UINT32_MAX)
            cold_only_writes++;
    }
    assert(cold_only_writes == 0);

    for (uint32_t i = 0; i < prefill; i++) {
        make_key(key, sizeof(key), i);
        uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
        memset(&handle, 0, sizeof(handle));
        warm_slot = UINT32_MAX;
        assert(vemb_v16_tlc_get_handle(tlc, key, (uint32_t)strlen(key),
                                       key_hash, &handle, &warm_slot) == 0);
        assert(handle.region_id == 19);
        assert(handle.bytes == sizeof(vector));
        assert(warm_slot < max_vectors);
    }
    vemb_v16_tlc_destroy(tlc);
    free(region);
}
#endif

typedef struct concurrent_arg {
    vemb_v16_tlc_t *tlc;
    int tid;
    int iterations;
    uint32_t dim;
    uint32_t max_vectors;
} concurrent_arg_t;

static void *concurrent_worker(void *arg) {
    concurrent_arg_t *a = arg;
    float vector[4];
    char key[32];
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = 0;

    assert(a->dim == 4);
    for (int i = 0; i < a->iterations; i++) {
        uint32_t id = (uint32_t)(a->tid * a->iterations + i);
        snprintf(key, sizeof(key), "k:%u", id);
        fill_vector(vector, a->dim, id);
        uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
        assert(vemb_v16_tlc_put(a->tlc, key, (uint32_t)strlen(key), key_hash,
                                vector, sizeof(vector),
                                &handle, &warm_slot) == 0);
        assert(handle.region_id == 11);
        assert(handle.bytes == sizeof(vector));
        assert(handle.offset + handle.bytes <=
               (uint64_t)a->dim * a->max_vectors * sizeof(float));
        memset(&handle, 0xff, sizeof(handle));
        warm_slot = UINT32_MAX;
        assert(vemb_v16_tlc_get_handle(a->tlc, key, (uint32_t)strlen(key),
                                       key_hash, &handle, &warm_slot) == 0);
        assert(handle.region_id == 11);
        assert(warm_slot < a->max_vectors);
    }
    return NULL;
}

static void test_concurrent_distinct_keys(void) {
    enum { dim = 4, max_vectors = 1024, threads = 4, iterations = 24 };
    float region[dim * max_vectors];
    vemb_v16_warm_region_header_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    pthread_t tids[threads];
    concurrent_arg_t args[threads];
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 11,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 11, max_vectors);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm, 1, 4) == 0);
    for (int i = 0; i < threads; i++) {
        args[i] = (concurrent_arg_t){
            .tlc = tlc,
            .tid = i,
            .iterations = iterations,
            .dim = dim,
            .max_vectors = max_vectors,
        };
        assert(pthread_create(&tids[i], NULL, concurrent_worker, &args[i]) == 0);
    }
    for (int i = 0; i < threads; i++)
        assert(pthread_join(tids[i], NULL) == 0);
    vemb_v16_tlc_destroy(tlc);
}

static uint32_t collect_keys_for_region(uint32_t wanted_region_id,
                                        char keys[][32],
                                        uint32_t needed) {
    enum { dim = 2, max_vectors = 64 };
    float region1[dim * max_vectors];
    float region2[dim * max_vectors];
    float vector[dim];
    vemb_v16_warm_region_header_t alloc1, alloc2;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t regions[] = {
        {
            .region_id = 1,
            .backend_type = VEMB_V16_REGION_UB,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = region1,
            .region_bytes = sizeof(region1),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
        {
            .region_id = 2,
            .backend_type = VEMB_V16_REGION_UB,
            .is_local = 0,
            .weight = 1,
            .mapped_addr = region2,
            .region_bytes = sizeof(region2),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
    };
    uint32_t found = 0;

    memset(region1, 0, sizeof(region1));
    memset(region2, 0, sizeof(region2));
    init_test_allocator(&alloc1, 1, max_vectors);
    init_test_allocator(&alloc2, 2, max_vectors);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors,
                                    regions, 2, 16) == 0);
    fill_vector(vector, dim, 500);
    for (uint32_t i = 0; i < 10000 && found < needed; i++) {
        char key[32];
        vemb_v16_vector_handle_t handle = {0};
        uint32_t warm_slot = UINT32_MAX;
        snprintf(key, sizeof(key), "region-probe:%u", i);
        uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
        assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                                vector, sizeof(vector),
                                &handle, &warm_slot) == 0);
        if (handle.region_id == wanted_region_id) {
            snprintf(keys[found], 32, "%s", key);
            found++;
        }
    }
    vemb_v16_tlc_destroy(tlc);
    return found;
}

static void test_multi_region_local_full_fallback_and_overwrite(void) {
    enum { dim = 2 };
    float local_region[dim * 1];
    float remote_region[dim * 4];
    float first[dim], second[dim], overwrite[dim];
    char local_keys[2][32];
    vemb_v16_warm_region_header_t local_alloc, remote_alloc;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t regions[] = {
        {
            .region_id = 1,
            .backend_type = VEMB_V16_REGION_UB,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = local_region,
            .region_bytes = sizeof(local_region),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
        {
            .region_id = 2,
            .backend_type = VEMB_V16_REGION_UB,
            .is_local = 0,
            .weight = 1,
            .mapped_addr = remote_region,
            .region_bytes = sizeof(remote_region),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
    };
    vemb_v16_vector_handle_t h1 = {0}, h2 = {0}, h3 = {0};
    uint32_t warm_slot = UINT32_MAX;

    assert(collect_keys_for_region(1, local_keys, 2) == 2);
    memset(local_region, 0, sizeof(local_region));
    memset(remote_region, 0, sizeof(remote_region));
    init_test_allocator(&local_alloc, 1, 1);
    init_test_allocator(&remote_alloc, 2, 4);
    assert(vemb_v16_tlc_create(&tlc, dim, 5, regions, 2, 16) == 0);

    fill_vector(first, dim, 10);
    fill_vector(second, dim, 20);
    fill_vector(overwrite, dim, 30);
    uint64_t h1_hash = vemb_v16_xxh3_64_str(local_keys[0], strlen(local_keys[0]));
    uint64_t h2_hash = vemb_v16_xxh3_64_str(local_keys[1], strlen(local_keys[1]));

    assert(vemb_v16_tlc_put(tlc, local_keys[0], (uint32_t)strlen(local_keys[0]),
                            h1_hash, first, sizeof(first),
                            &h1, &warm_slot) == 0);
    assert(h1.region_id == 1);
    assert(h1.offset == 0);
    assert(memcmp(local_region, first, sizeof(first)) == 0);

    assert(vemb_v16_tlc_put(tlc, local_keys[1], (uint32_t)strlen(local_keys[1]),
                            h2_hash, second, sizeof(second),
                            &h2, &warm_slot) == 0);
#if TLC_CORE_ALLOW_LRU_EVICTION
    assert(h2.region_id == 1);
    assert(h2.offset == 0);
    assert(memcmp(local_region, second, sizeof(second)) == 0);
#else
    assert(h2.region_id == 2);
    assert(h2.offset < sizeof(remote_region));
    assert(memcmp((uint8_t *)remote_region + h2.offset,
                  second,
                  sizeof(second)) == 0);
#endif

    assert(vemb_v16_tlc_put(tlc, local_keys[0], (uint32_t)strlen(local_keys[0]),
                            h1_hash, overwrite, sizeof(overwrite),
                            &h3, &warm_slot) == 0);
    assert(h3.region_id == h1.region_id);
    assert(h3.offset == h1.offset);
    assert(memcmp(local_region, overwrite, sizeof(overwrite)) == 0);
    tlc_core_stats_t stats;
    tlc_core_get_stats(tlc->core, &stats);
    assert(stats.warm_region_count == 2);
    assert(stats.warm_alloc_local >= 1);
    assert(stats.warm_region_full_count >= 1);
    vemb_v16_tlc_destroy(tlc);
}

static void test_multi_region_all_full_evicts_committed_warm(void) {
    enum { dim = 2, max_vectors = 2 };
    float region1[dim];
    float region2[dim];
    float vector[dim];
    vemb_v16_warm_region_header_t alloc1, alloc2;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t regions[] = {
        {
            .region_id = 10,
            .backend_type = VEMB_V16_REGION_UB,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = region1,
            .region_bytes = sizeof(region1),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
        {
            .region_id = 20,
            .backend_type = VEMB_V16_REGION_UB,
            .is_local = 0,
            .weight = 1,
            .mapped_addr = region2,
            .region_bytes = sizeof(region2),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
    };
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = 0;
    char key[32];

    init_test_allocator(&alloc1, 10, 1);
    init_test_allocator(&alloc2, 20, 1);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors,
                                    regions, 2, 8) == 0);
    fill_vector(vector, dim, 700);
    for (uint32_t i = 0; i < max_vectors; i++) {
        snprintf(key, sizeof(key), "full:%u", i);
        uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
        assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                                vector, sizeof(vector),
                                &handle, &warm_slot) == 0);
        assert(handle.bytes == sizeof(vector));
    }

    memset(&handle, 0xff, sizeof(handle));
    snprintf(key, sizeof(key), "full:%u", max_vectors);
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
#if TLC_CORE_ALLOW_LRU_EVICTION
    assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                            vector, sizeof(vector),
                            &handle, &warm_slot) == 0);
    assert(warm_slot != UINT32_MAX);
    assert(handle.bytes == sizeof(vector));
    assert(handle.owner_generation >= 2);
    tlc_core_stats_t stats;
    tlc_core_get_stats(tlc->core, &stats);
    assert(stats.warm_region_count == 2);
    assert(stats.warm_region_full_count >= 1);
    assert(stats.warm_alloc_cold_spill == 0);
#else
    assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                            vector, sizeof(vector),
                            &handle, &warm_slot) != 0);
    tlc_core_stats_t stats;
    tlc_core_get_stats(tlc->core, &stats);
    assert(stats.warm_region_count == 2);
    assert(stats.warm_region_full_count >= 1);
    assert(stats.warm_eviction_success == 0);
#endif
    vemb_v16_tlc_destroy(tlc);
}

static void test_shared_slot_meta_two_tlcs_unique_slots(void) {
    enum { dim = 2, max_vectors = 4 };
    float region[dim * max_vectors];
    float v1[dim], v2[dim], overwrite[dim];
    vemb_v16_warm_region_header_t allocator;
    vemb_v16_tlc_t *tlc1 = NULL, *tlc2 = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 77,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_vector_handle_t h1 = {0}, h2 = {0}, h3 = {0};
    uint32_t warm_slot = UINT32_MAX;
    const char *key1 = "shared:one";
    const char *key2 = "shared:two";
    uint64_t key1_hash = vemb_v16_xxh3_64_str(key1, strlen(key1));
    uint64_t key2_hash = vemb_v16_xxh3_64_str(key2, strlen(key2));

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 77, max_vectors);
    assert(vemb_v16_tlc_create(&tlc1, dim, max_vectors, &warm, 1, 4) == 0);
    assert(vemb_v16_tlc_create(&tlc2, dim, max_vectors, &warm, 1, 4) == 0);

    fill_vector(v1, dim, 1000);
    fill_vector(v2, dim, 2000);
    fill_vector(overwrite, dim, 3000);
    assert(vemb_v16_tlc_put(tlc1, key1, (uint32_t)strlen(key1), key1_hash,
                            v1, sizeof(v1), &h1, &warm_slot) == 0);
    assert(h1.region_id == 77);
    assert(h1.offset == 0);
    assert(warm_slot == 0);
    assert(vemb_v16_tlc_put(tlc2, key2, (uint32_t)strlen(key2), key2_hash,
                            v2, sizeof(v2), &h2, &warm_slot) == 0);
    assert(h2.region_id == 77);
    assert(h2.offset == sizeof(v2));
    assert(warm_slot == 1);
    assert_region_stats(tlc1, 77, 2, 0);

    assert(vemb_v16_tlc_put(tlc1, key1, (uint32_t)strlen(key1), key1_hash,
                            overwrite, sizeof(overwrite), &h3,
                            &warm_slot) == 0);
    assert(h3.region_id == h1.region_id);
    assert(h3.offset == h1.offset);
    assert(warm_slot == 0);
    assert_region_stats(tlc1, 77, 2, 0);
    assert(memcmp(region, overwrite, sizeof(overwrite)) == 0);
    assert(memcmp(region + dim, v2, sizeof(v2)) == 0);

    vemb_v16_tlc_destroy(tlc2);
    vemb_v16_tlc_destroy(tlc1);
}

static void test_vsim_key2_lookup_local_source(void) {
    enum { dim = 2, max_vectors = 4 };
    float region[dim * max_vectors];
    float v1[dim], v2[dim];
    vemb_v16_warm_region_header_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 88,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_vector_handle_t handle = {0};
    vemb_v16_vector_handle_t key2_handle = {0};
    vemb_v16_tlc_lookup_source_t source = VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
    uint32_t warm_slot = UINT32_MAX;
    const char *key1 = "vsim:one";
    const char *key2 = "vsim:two";
    const char *missing = "vsim:missing";
    uint64_t key1_hash = vemb_v16_xxh3_64_str(key1, strlen(key1));
    uint64_t key2_hash = vemb_v16_xxh3_64_str(key2, strlen(key2));
    uint64_t missing_hash = vemb_v16_xxh3_64_str(missing, strlen(missing));

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 88, max_vectors);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm, 1, 4) == 0);

    fill_vector(v1, dim, 100);
    fill_vector(v2, dim, 200);
    assert(vemb_v16_tlc_put(tlc, key1, (uint32_t)strlen(key1), key1_hash,
                            v1, sizeof(v1), &handle, &warm_slot) == 0);
    assert(vemb_v16_tlc_put(tlc, key2, (uint32_t)strlen(key2), key2_hash,
                            v2, sizeof(v2), &handle, &warm_slot) == 0);

    assert(vemb_v16_tlc_lookup_vsim_key2(tlc,
                                         key2,
                                         (uint32_t)strlen(key2),
                                         key2_hash,
                                         &key2_handle,
                                         &source) == 0);
    assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_LOCAL);


    assert(key2_handle.region_id == 88);
    assert(key2_handle.offset == sizeof(v2));
    assert(key2_handle.bytes == sizeof(v2));

    source = VEMB_V16_TLC_LOOKUP_SOURCE_LOCAL;
    memset(&key2_handle, 0xff, sizeof(key2_handle));
    assert(vemb_v16_tlc_lookup_vsim_key2(tlc,
                                         missing,
                                         (uint32_t)strlen(missing),
                                         missing_hash,
                                         &key2_handle,
                                         &source) != 0);
    assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_NONE);



    vemb_v16_tlc_destroy(tlc);
}

static uint32_t fixed_owner_resolver(uint64_t key_hash,
                                     const char *key,
                                     uint32_t key_len,
                                     void *arg);

static void test_vsim_key2_lookup_remote_source(void) {
    enum { dim = 2, max_vectors = 4, remote_entries = 4, remote_buckets = 8 };
    float region[dim * max_vectors];
    float vector[dim];
    vemb_v16_warm_region_header_t allocator;
    vemb_v16_tlc_t *owner = NULL;
    vemb_v16_tlc_t *reader = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 99,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_remote_meta_view_t owner_meta;
    vemb_v16_remote_meta_view_t reader_meta;
    size_t remote_meta_bytes =
        vemb_v16_remote_meta_layout_bytes(remote_entries, remote_buckets);
    void *owner_meta_base = NULL;
    void *reader_meta_base = NULL;
    uint32_t wanted_owner = 1;
    const char *key2 = "vsim:remote-key2";
    uint64_t key2_hash = vemb_v16_xxh3_64_str(key2, strlen(key2));
    vemb_v16_vector_handle_t handle = {0};
    vemb_v16_vector_handle_t remote_handle = {0};
    vemb_v16_tlc_lookup_source_t source = VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
    uint32_t warm_slot = UINT32_MAX;
    const uint8_t *bytes = NULL;
    uint32_t len = 0;

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 99, max_vectors);
    assert(posix_memalign(&owner_meta_base, 64, remote_meta_bytes) == 0);
    assert(posix_memalign(&reader_meta_base, 64, remote_meta_bytes) == 0);
    assert(vemb_v16_remote_meta_init(&owner_meta,
                                     owner_meta_base,
                                     remote_meta_bytes,
                                     1,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_remote_meta_init(&reader_meta,
                                     reader_meta_base,
                                     remote_meta_bytes,
                                     2,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);

    assert(vemb_v16_tlc_create(&owner, dim, max_vectors, &warm, 1, 4) == 0);
    assert(vemb_v16_tlc_create(&reader, dim, max_vectors, &warm, 1, 4) == 0);
    vemb_v16_tlc_set_remote_meta_view(owner, &owner_meta, 8);
    vemb_v16_tlc_set_remote_meta_view(reader, &reader_meta, 8);
    assert(vemb_v16_tlc_set_remote_meta_owner_view(reader, 1, &owner_meta) == 0);
    vemb_v16_tlc_set_owner_resolver(reader,
                                    fixed_owner_resolver,
                                    &wanted_owner);

    fill_vector(vector, dim, 300);
    assert(vemb_v16_tlc_put(owner, key2, (uint32_t)strlen(key2), key2_hash,
                            vector, sizeof(vector), &handle, &warm_slot) == 0);
    assert(publish_remote_meta_to_view(owner,
                                       owner->remote_meta_view,
                                       key2,
                                       (uint32_t)strlen(key2),
                                       key2_hash,
                                       &handle,
                                       0) == 0);

    assert(vemb_v16_tlc_lookup_vsim_key2(reader,
                                         key2,
                                         (uint32_t)strlen(key2),
                                         key2_hash,
                                         &remote_handle,
                                         &source) == 0);
    assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_REMOTE);


    assert(remote_handle.region_id == handle.region_id);
    assert(remote_handle.offset == handle.offset);
    assert(remote_handle.bytes == handle.bytes);
    assert(remote_handle.local_slot == handle.local_slot);
    assert(remote_handle.owner_generation == handle.owner_generation);
    assert(vemb_v16_tlc_vector_slice(reader, &remote_handle, &bytes, &len) == 0);
    assert(len == sizeof(vector));
    assert(memcmp(bytes, vector, sizeof(vector)) == 0);

    vemb_v16_tlc_destroy(reader);
    vemb_v16_tlc_destroy(owner);
    free(reader_meta_base);
    free(owner_meta_base);
}

static void test_remote_meta_async_publish_flush(void) {
    enum { dim = 2, max_vectors = 4, remote_entries = 4, remote_buckets = 8 };
    float region[dim * max_vectors];
    float vector[dim];
    vemb_v16_warm_region_header_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 299,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_remote_meta_view_t meta;
    size_t meta_bytes =
        vemb_v16_remote_meta_layout_bytes(remote_entries, remote_buckets);
    void *meta_base = NULL;
    const char *key = "remote-meta:async";
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
    vemb_v16_vector_handle_t handle = {0};
    vemb_v16_remote_meta_handle_t remote_handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    vemb_v16_stats_t stats;

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 299, max_vectors);
    assert(posix_memalign(&meta_base, 64, meta_bytes) == 0);
    assert(vemb_v16_remote_meta_init(&meta,
                                     meta_base,
                                     meta_bytes,
                                     9,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &warm, 1, 4) == 0);
    vemb_v16_tlc_set_remote_meta_view(tlc, &meta, 8);

    fill_vector(vector, dim, 700);
    assert(vemb_v16_tlc_put(tlc,
                            key,
                            (uint32_t)strlen(key),
                            key_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);
    assert(enqueue_remote_meta_publish(tlc,
                                       tlc->remote_meta_view,
                                       key,
                                       (uint32_t)strlen(key),
                                       key_hash,
                                       &handle,
                                       0) == 0);
    (void)vemb_v16_tlc_flush_remote_meta_publishes(tlc, 0);
    assert(vemb_v16_remote_meta_lookup(&meta,
                                       key,
                                       (uint32_t)strlen(key),
                                       key_hash,
                                       8,
                                       &remote_handle) ==
           VEMB_V16_REMOTE_META_OK);
    assert(remote_handle.local_slot == handle.local_slot);
    memset(&stats, 0, sizeof(stats));
    vemb_v16_tlc_get_runtime_stats(tlc, &stats);
    assert(stats.remote_meta_publish_async_enqueue >= 1);
    assert(stats.remote_meta_publish_ok >= 1);

    vemb_v16_tlc_destroy(tlc);
    free(meta_base);
}

static void test_vsim_key2_lookup_rpc_fallback_and_repair(void) {
    enum { dim = 2, max_vectors = 4, remote_entries = 4, remote_buckets = 8 };
    float region[dim * max_vectors];
    float vector[dim];
    vemb_v16_warm_region_header_t allocator;
    vemb_v16_tlc_t *owner = NULL;
    vemb_v16_tlc_t *reader = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 399,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_remote_meta_view_t owner_meta;
    vemb_v16_remote_meta_view_t reader_meta;
    size_t remote_meta_bytes =
        vemb_v16_remote_meta_layout_bytes(remote_entries, remote_buckets);
    void *owner_meta_base = NULL;
    void *reader_meta_base = NULL;
    uint32_t wanted_owner = 1;
    const char *key2 = "vsim:rpc-key2";
    uint64_t key2_hash = vemb_v16_xxh3_64_str(key2, strlen(key2));
    vemb_v16_vector_handle_t handle = {0};
    vemb_v16_vector_handle_t remote_handle = {0};
    vemb_v16_tlc_lookup_source_t source = VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
    uint32_t warm_slot = UINT32_MAX;
    vemb_v16_remote_meta_handle_t repaired = {0};
    vemb_v16_stats_t stats;

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 399, max_vectors);
    assert(posix_memalign(&owner_meta_base, 64, remote_meta_bytes) == 0);
    assert(posix_memalign(&reader_meta_base, 64, remote_meta_bytes) == 0);
    assert(vemb_v16_remote_meta_init(&owner_meta,
                                     owner_meta_base,
                                     remote_meta_bytes,
                                     1,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_remote_meta_init(&reader_meta,
                                     reader_meta_base,
                                     remote_meta_bytes,
                                     2,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);

    assert(vemb_v16_tlc_create(&owner, dim, max_vectors, &warm, 1, 4) == 0);
    assert(vemb_v16_tlc_create(&reader, dim, max_vectors, &warm, 1, 4) == 0);
    vemb_v16_tlc_set_remote_meta_view(owner, &owner_meta, 8);
    vemb_v16_tlc_set_remote_meta_view(reader, &reader_meta, 8);
    assert(vemb_v16_tlc_set_remote_meta_owner_view(reader, 1, &owner_meta) == 0);
    vemb_v16_tlc_set_owner_resolver(reader,
                                    fixed_owner_resolver,
                                    &wanted_owner);
    vemb_v16_tlc_set_lookup_rpc(reader,
                                vemb_v16_tlc_lookup_rpc_local_handler,
                                owner);

    fill_vector(vector, dim, 800);
    assert(vemb_v16_tlc_put(owner,
                            key2,
                            (uint32_t)strlen(key2),
                            key2_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);

    assert(vemb_v16_tlc_lookup_vsim_key2(reader,
                                         key2,
                                         (uint32_t)strlen(key2),
                                         key2_hash,
                                         &remote_handle,
                                         &source) == 0);
    assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_UB_RPC);

    assert(remote_handle.local_slot == handle.local_slot);
    assert(remote_handle.owner_generation == handle.owner_generation);

    (void)vemb_v16_tlc_flush_remote_meta_publishes(reader, 0);
    assert(vemb_v16_remote_meta_lookup(&owner_meta,
                                       key2,
                                       (uint32_t)strlen(key2),
                                       key2_hash,
                                       8,
                                       &repaired) ==
           VEMB_V16_REMOTE_META_OK);
    assert(repaired.local_slot == handle.local_slot);
    memset(&stats, 0, sizeof(stats));
    vemb_v16_tlc_get_runtime_stats(reader, &stats);
    assert(stats.remote_meta_lookup_miss >= 1);
    assert(stats.ub_lookup_rpc_ok >= 1);
    assert(stats.ub_lookup_rpc_handle >= 1);
    assert(stats.remote_meta_repair_enqueue >= 1);

    vemb_v16_tlc_destroy(reader);
    vemb_v16_tlc_destroy(owner);
    free(reader_meta_base);
    free(owner_meta_base);
}

static void test_vsim_key2_lookup_ub_ring_rpc_fallback_and_repair(void) {
    enum { dim = 2, max_vectors = 8, remote_entries = 8, remote_buckets = 16 };
    float region[dim * max_vectors];
    float vector[dim];
    vemb_v16_warm_region_header_t allocator;
    vemb_v16_tlc_t *owner = NULL;
    vemb_v16_tlc_t *reader = NULL;
    vemb_v16_ub_rpc_t *owner_rpc = NULL;
    vemb_v16_ub_rpc_t *reader_rpc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 499,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_remote_meta_view_t owner_meta;
    vemb_v16_remote_meta_view_t reader_meta;
    size_t remote_meta_bytes =
        vemb_v16_remote_meta_layout_bytes(remote_entries, remote_buckets);
    void *owner_meta_base = NULL;
    void *reader_meta_base = NULL;
    uint32_t wanted_owner = 1;
    const char *key2 = "vsim:ub-ring-rpc-key2";
    uint64_t key2_hash = vemb_v16_xxh3_64_str(key2, strlen(key2));
    vemb_v16_vector_handle_t handle = {0};
    vemb_v16_vector_handle_t remote_handle = {0};
    vemb_v16_tlc_lookup_source_t source = VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
    uint32_t warm_slot = UINT32_MAX;
    vemb_v16_remote_meta_handle_t repaired = {0};
    vemb_v16_stats_t stats;
    char req_reader_owner[64];
    char req_owner_reader[64];
    char resp_reader_owner[64];
    char resp_owner_reader[64];
    vemb_v16_ub_rpc_peer_t reader_peer;
    vemb_v16_ub_rpc_peer_t owner_peer;

    snprintf(req_reader_owner, sizeof(req_reader_owner),
             "/v16rpc_%ld_req_2_1", (long)getpid());
    snprintf(req_owner_reader, sizeof(req_owner_reader),
             "/v16rpc_%ld_req_1_2", (long)getpid());
    snprintf(resp_reader_owner, sizeof(resp_reader_owner),
             "/v16rpc_%ld_resp_2_1", (long)getpid());
    snprintf(resp_owner_reader, sizeof(resp_owner_reader),
             "/v16rpc_%ld_resp_1_2", (long)getpid());
    cleanup_rpc_rings(req_reader_owner,
                      req_owner_reader,
                      resp_reader_owner,
                      resp_owner_reader);
    make_rpc_peers(&reader_peer,
                   1,
                   &owner_peer,
                   2,
                   req_reader_owner,
                   req_owner_reader,
                   resp_reader_owner,
                   resp_owner_reader);

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 499, max_vectors);
    assert(posix_memalign(&owner_meta_base, 64, remote_meta_bytes) == 0);
    assert(posix_memalign(&reader_meta_base, 64, remote_meta_bytes) == 0);
    assert(vemb_v16_remote_meta_init(&owner_meta,
                                     owner_meta_base,
                                     remote_meta_bytes,
                                     1,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_remote_meta_init(&reader_meta,
                                     reader_meta_base,
                                     remote_meta_bytes,
                                     2,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);

    assert(vemb_v16_tlc_create(&owner, dim, max_vectors, &warm, 1, 4) == 0);
    assert(vemb_v16_tlc_create(&reader, dim, max_vectors, &warm, 1, 4) == 0);
    vemb_v16_tlc_set_remote_meta_view(owner, &owner_meta, 8);
    vemb_v16_tlc_set_remote_meta_view(reader, &reader_meta, 8);
    assert(vemb_v16_tlc_set_remote_meta_owner_view(reader, 1, &owner_meta) == 0);
    vemb_v16_tlc_set_owner_resolver(reader,
                                    fixed_owner_resolver,
                                    &wanted_owner);
    assert(vemb_v16_ub_rpc_create(&owner_rpc,
                                  owner,
                                  1,
                                  100,
                                  &owner_peer,
                                  1) == 0);
    assert(vemb_v16_ub_rpc_create(&reader_rpc,
                                  reader,
                                  2,
                                  100,
                                  &reader_peer,
                                  1) == 0);

    fill_vector(vector, dim, 1800);
    assert(vemb_v16_tlc_put(owner,
                            key2,
                            (uint32_t)strlen(key2),
                            key2_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);

    assert(vemb_v16_tlc_lookup_vsim_key2(reader,
                                         key2,
                                         (uint32_t)strlen(key2),
                                         key2_hash,
                                         &remote_handle,
                                         &source) == 0);
    assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_UB_RPC);

    assert(remote_handle.local_slot == handle.local_slot);
    assert(remote_handle.owner_generation == handle.owner_generation);

    (void)vemb_v16_tlc_flush_remote_meta_publishes(reader, 0);
    assert(vemb_v16_remote_meta_lookup(&owner_meta,
                                       key2,
                                       (uint32_t)strlen(key2),
                                       key2_hash,
                                       8,
                                       &repaired) ==
           VEMB_V16_REMOTE_META_OK);
    assert(repaired.local_slot == handle.local_slot);
    memset(&stats, 0, sizeof(stats));
    vemb_v16_tlc_get_runtime_stats(reader, &stats);
    assert(stats.ub_lookup_rpc_ok >= 1);
    assert(stats.ub_lookup_rpc_handle >= 1);
    assert(stats.remote_meta_repair_enqueue >= 1);

    vemb_v16_ub_rpc_destroy(reader_rpc);
    vemb_v16_ub_rpc_destroy(owner_rpc);
    vemb_v16_tlc_destroy(reader);
    vemb_v16_tlc_destroy(owner);
    free(reader_meta_base);
    free(owner_meta_base);
    cleanup_rpc_rings(req_reader_owner,
                      req_owner_reader,
                      resp_reader_owner,
                      resp_owner_reader);
}

typedef struct ub_ring_rpc_concurrent_arg {
    vemb_v16_tlc_t *reader;
    char (*keys)[VEMB_V16_MAX_KEY_LEN];
    uint64_t *hashes;
    uint32_t key_count;
    uint32_t loops;
    uint32_t tid;
    atomic_uint_fast32_t *ok_count;
} ub_ring_rpc_concurrent_arg_t;

static void *ub_ring_rpc_concurrent_worker(void *arg) {
    ub_ring_rpc_concurrent_arg_t *ctx = arg;
    for (uint32_t i = 0; i < ctx->loops; i++) {
        uint32_t idx = (i + ctx->tid) % ctx->key_count;
        vemb_v16_vector_handle_t handle = {0};
        vemb_v16_tlc_lookup_source_t source =
            VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
        assert(vemb_v16_tlc_lookup_vsim_key2(
                   ctx->reader,
                   ctx->keys[idx],
                   (uint32_t)strlen(ctx->keys[idx]),
                   ctx->hashes[idx],
                   &handle,
                   &source) == 0);
        assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_UB_RPC);
        assert(handle.key_hash == ctx->hashes[idx]);
        assert(handle.bytes != 0);
        atomic_fetch_add_explicit(ctx->ok_count, 1,
                                  memory_order_relaxed);
    }
    return NULL;
}

static void test_vsim_key2_lookup_ub_ring_rpc_concurrent(void) {
    enum {
        dim = 2,
        max_vectors = 32,
        remote_entries = 8,
        remote_buckets = 16,
        key_count = 8,
        thread_count = 4,
        loops = 32,
    };
    float region[dim * max_vectors];
    float vector[dim];
    vemb_v16_warm_region_header_t allocator;
    vemb_v16_tlc_t *owner = NULL;
    vemb_v16_tlc_t *reader = NULL;
    vemb_v16_ub_rpc_t *owner_rpc = NULL;
    vemb_v16_ub_rpc_t *reader_rpc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 599,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_remote_meta_view_t owner_meta;
    vemb_v16_remote_meta_view_t reader_meta;
    size_t remote_meta_bytes =
        vemb_v16_remote_meta_layout_bytes(remote_entries, remote_buckets);
    void *owner_meta_base = NULL;
    void *reader_meta_base = NULL;
    uint32_t wanted_owner = 1;
    char keys[key_count][VEMB_V16_MAX_KEY_LEN];
    uint64_t hashes[key_count];
    pthread_t threads[thread_count];
    ub_ring_rpc_concurrent_arg_t args[thread_count];
    atomic_uint_fast32_t ok_count;
    char req_reader_owner[64];
    char req_owner_reader[64];
    char resp_reader_owner[64];
    char resp_owner_reader[64];
    vemb_v16_ub_rpc_peer_t reader_peer;
    vemb_v16_ub_rpc_peer_t owner_peer;

    snprintf(req_reader_owner, sizeof(req_reader_owner),
             "/v16rpc_%ld_c_req_2_1", (long)getpid());
    snprintf(req_owner_reader, sizeof(req_owner_reader),
             "/v16rpc_%ld_c_req_1_2", (long)getpid());
    snprintf(resp_reader_owner, sizeof(resp_reader_owner),
             "/v16rpc_%ld_c_resp_2_1", (long)getpid());
    snprintf(resp_owner_reader, sizeof(resp_owner_reader),
             "/v16rpc_%ld_c_resp_1_2", (long)getpid());
    cleanup_rpc_rings(req_reader_owner,
                      req_owner_reader,
                      resp_reader_owner,
                      resp_owner_reader);
    make_rpc_peers(&reader_peer,
                   1,
                   &owner_peer,
                   2,
                   req_reader_owner,
                   req_owner_reader,
                   resp_reader_owner,
                   resp_owner_reader);

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 599, max_vectors);
    atomic_init(&ok_count, 0);
    assert(posix_memalign(&owner_meta_base, 64, remote_meta_bytes) == 0);
    assert(posix_memalign(&reader_meta_base, 64, remote_meta_bytes) == 0);
    assert(vemb_v16_remote_meta_init(&owner_meta,
                                     owner_meta_base,
                                     remote_meta_bytes,
                                     1,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_remote_meta_init(&reader_meta,
                                     reader_meta_base,
                                     remote_meta_bytes,
                                     2,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);

    assert(vemb_v16_tlc_create(&owner, dim, max_vectors, &warm, 1, 4) == 0);
    assert(vemb_v16_tlc_create(&reader, dim, max_vectors, &warm, 1, 4) == 0);
    vemb_v16_tlc_set_remote_meta_view(owner, &owner_meta, 8);
    vemb_v16_tlc_set_remote_meta_view(reader, &reader_meta, 8);
    vemb_v16_tlc_set_owner_resolver(reader,
                                    fixed_owner_resolver,
                                    &wanted_owner);
    assert(vemb_v16_ub_rpc_create(&owner_rpc,
                                  owner,
                                  1,
                                  100,
                                  &owner_peer,
                                  1) == 0);
    assert(vemb_v16_ub_rpc_create(&reader_rpc,
                                  reader,
                                  2,
                                  100,
                                  &reader_peer,
                                  1) == 0);

    for (uint32_t i = 0; i < key_count; i++) {
        vemb_v16_vector_handle_t handle = {0};
        uint32_t warm_slot = UINT32_MAX;
        snprintf(keys[i], sizeof(keys[i]), "vsim:ub-ring-rpc-conc:%u", i);
        hashes[i] = vemb_v16_xxh3_64_str(keys[i], strlen(keys[i]));
        fill_vector(vector, dim, 2000 + i);
        assert(vemb_v16_tlc_put(owner,
                                keys[i],
                                (uint32_t)strlen(keys[i]),
                                hashes[i],
                                vector,
                                sizeof(vector),
                                &handle,
                                &warm_slot) == 0);
    }

    for (uint32_t t = 0; t < thread_count; t++) {
        args[t] = (ub_ring_rpc_concurrent_arg_t){
            .reader = reader,
            .keys = keys,
            .hashes = hashes,
            .key_count = key_count,
            .loops = loops,
            .tid = t,
            .ok_count = &ok_count,
        };
        assert(pthread_create(&threads[t],
                              NULL,
                              ub_ring_rpc_concurrent_worker,
                              &args[t]) == 0);
    }
    for (uint32_t t = 0; t < thread_count; t++)
        assert(pthread_join(threads[t], NULL) == 0);
    assert(atomic_load_explicit(&ok_count, memory_order_relaxed) ==
           thread_count * loops);

    vemb_v16_stats_t stats;
    memset(&stats, 0, sizeof(stats));
    vemb_v16_tlc_get_runtime_stats(reader, &stats);
    assert(stats.ub_lookup_rpc_ok >= thread_count * loops);
    assert(stats.ub_lookup_rpc_handle >= thread_count * loops);

    vemb_v16_ub_rpc_destroy(reader_rpc);
    vemb_v16_ub_rpc_destroy(owner_rpc);
    vemb_v16_tlc_destroy(reader);
    vemb_v16_tlc_destroy(owner);
    free(reader_meta_base);
    free(owner_meta_base);
    cleanup_rpc_rings(req_reader_owner,
                      req_owner_reader,
                      resp_reader_owner,
                      resp_owner_reader);
}

static void test_vsim_key2_lookup_ub_ring_rpc_stale_and_conflict(void) {
    enum { dim = 2, max_vectors = 8 };
    float region[dim * max_vectors];
    float first[dim], second[dim], other[dim];
    vemb_v16_warm_region_header_t allocator;
    vemb_v16_tlc_t *owner = NULL;
    vemb_v16_tlc_t *reader = NULL;
    vemb_v16_ub_rpc_t *owner_rpc = NULL;
    vemb_v16_ub_rpc_t *reader_rpc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 699,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_remote_meta_view_t owner_meta;
    vemb_v16_remote_meta_view_t reader_meta;
    size_t remote_meta_bytes =
        vemb_v16_remote_meta_layout_bytes_for_sets(1, 1);
    void *owner_meta_base = NULL;
    void *reader_meta_base = NULL;
    uint32_t wanted_owner = 1;
    const char *stale_key = "vsim:ub-ring-rpc-stale";
    const char *evict_a = "vsim:ub-ring-rpc-evict-a";
    const char *evict_b = "vsim:ub-ring-rpc-evict-b";
    uint64_t stale_hash = vemb_v16_xxh3_64_str(stale_key, strlen(stale_key));
    uint64_t evict_a_hash = vemb_v16_xxh3_64_str(evict_a, strlen(evict_a));
    uint64_t evict_b_hash = vemb_v16_xxh3_64_str(evict_b, strlen(evict_b));
    vemb_v16_vector_handle_t stale_old = {0};
    vemb_v16_vector_handle_t stale_new = {0};
    vemb_v16_vector_handle_t evict_a_handle = {0};
    vemb_v16_vector_handle_t evict_b_handle = {0};
    vemb_v16_vector_handle_t remote_handle = {0};
    vemb_v16_tlc_lookup_source_t source = VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
    uint32_t warm_slot = UINT32_MAX;
    vemb_v16_stats_t stats;
    char req_reader_owner[64];
    char req_owner_reader[64];
    char resp_reader_owner[64];
    char resp_owner_reader[64];
    vemb_v16_ub_rpc_peer_t reader_peer;
    vemb_v16_ub_rpc_peer_t owner_peer;

    snprintf(req_reader_owner, sizeof(req_reader_owner),
             "/v16rpc_%ld_sc_req_2_1", (long)getpid());
    snprintf(req_owner_reader, sizeof(req_owner_reader),
             "/v16rpc_%ld_sc_req_1_2", (long)getpid());
    snprintf(resp_reader_owner, sizeof(resp_reader_owner),
             "/v16rpc_%ld_sc_resp_2_1", (long)getpid());
    snprintf(resp_owner_reader, sizeof(resp_owner_reader),
             "/v16rpc_%ld_sc_resp_1_2", (long)getpid());
    cleanup_rpc_rings(req_reader_owner,
                      req_owner_reader,
                      resp_reader_owner,
                      resp_owner_reader);
    make_rpc_peers(&reader_peer,
                   1,
                   &owner_peer,
                   2,
                   req_reader_owner,
                   req_owner_reader,
                   resp_reader_owner,
                   resp_owner_reader);

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 699, max_vectors);
    assert(posix_memalign(&owner_meta_base, 64, remote_meta_bytes) == 0);
    assert(posix_memalign(&reader_meta_base, 64, remote_meta_bytes) == 0);
    assert(vemb_v16_remote_meta_init_sets(&owner_meta,
                                          owner_meta_base,
                                          remote_meta_bytes,
                                          1,
                                          dim * sizeof(float),
                                          1,
                                          1) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_remote_meta_init_sets(&reader_meta,
                                          reader_meta_base,
                                          remote_meta_bytes,
                                          2,
                                          dim * sizeof(float),
                                          1,
                                          1) ==
           VEMB_V16_REMOTE_META_OK);

    assert(vemb_v16_tlc_create(&owner, dim, max_vectors, &warm, 1, 4) == 0);
    assert(vemb_v16_tlc_create(&reader, dim, max_vectors, &warm, 1, 4) == 0);
    vemb_v16_tlc_set_remote_meta_view(owner, &owner_meta, 8);
    vemb_v16_tlc_set_remote_meta_view(reader, &reader_meta, 8);
    assert(vemb_v16_tlc_set_remote_meta_owner_view(reader, 1, &owner_meta) == 0);
    vemb_v16_tlc_set_owner_resolver(reader,
                                    fixed_owner_resolver,
                                    &wanted_owner);
    assert(vemb_v16_ub_rpc_create(&owner_rpc,
                                  owner,
                                  1,
                                  100,
                                  &owner_peer,
                                  1) == 0);
    assert(vemb_v16_ub_rpc_create(&reader_rpc,
                                  reader,
                                  2,
                                  100,
                                  &reader_peer,
                                  1) == 0);

    fill_vector(first, dim, 3000);
    assert(vemb_v16_tlc_put(owner,
                            stale_key,
                            (uint32_t)strlen(stale_key),
                            stale_hash,
                            first,
                            sizeof(first),
                            &stale_new,
                            &warm_slot) == 0);
    fill_vector(second, dim, 3100);
    assert(vemb_v16_tlc_put(owner,
                            stale_key,
                            (uint32_t)strlen(stale_key),
                            stale_hash,
                            second,
                            sizeof(second),
                            &stale_new,
                            &warm_slot) == 0);
    stale_old = stale_new;
    stale_old.owner_generation++;
    assert(publish_remote_meta_to_view(owner,
                                       owner->remote_meta_view,
                                       stale_key,
                                       (uint32_t)strlen(stale_key),
                                       stale_hash,
                                       &stale_old,
                                       0) == 0);

    assert(vemb_v16_tlc_lookup_vsim_key2(reader,
                                         stale_key,
                                         (uint32_t)strlen(stale_key),
                                         stale_hash,
                                         &remote_handle,
                                         &source) == 0);
    assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_UB_RPC);
    assert(remote_handle.owner_generation == stale_new.owner_generation);
    assert(remote_handle.local_slot == stale_new.local_slot);
    (void)vemb_v16_tlc_flush_remote_meta_publishes(reader, 0);

    fill_vector(first, dim, 3200);
    fill_vector(other, dim, 3300);
    assert(vemb_v16_tlc_put(owner,
                            evict_a,
                            (uint32_t)strlen(evict_a),
                            evict_a_hash,
                            first,
                            sizeof(first),
                            &evict_a_handle,
                            &warm_slot) == 0);
    assert(publish_remote_meta_to_view(owner,
                                       owner->remote_meta_view,
                                       evict_a,
                                       (uint32_t)strlen(evict_a),
                                       evict_a_hash,
                                       &evict_a_handle,
                                       0) == 0);
    assert(vemb_v16_tlc_put(owner,
                            evict_b,
                            (uint32_t)strlen(evict_b),
                            evict_b_hash,
                            other,
                            sizeof(other),
                            &evict_b_handle,
                            &warm_slot) == 0);
    assert(publish_remote_meta_to_view(owner,
                                       owner->remote_meta_view,
                                       evict_b,
                                       (uint32_t)strlen(evict_b),
                                       evict_b_hash,
                                       &evict_b_handle,
                                       0) == 0);

    memset(&remote_handle, 0, sizeof(remote_handle));
    source = VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
    assert(vemb_v16_tlc_lookup_vsim_key2(reader,
                                         evict_a,
                                         (uint32_t)strlen(evict_a),
                                         evict_a_hash,
                                         &remote_handle,
                                         &source) == 0);
    assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_UB_RPC);
    assert(remote_handle.local_slot == evict_a_handle.local_slot);
    assert(remote_handle.owner_generation == evict_a_handle.owner_generation);

    memset(&stats, 0, sizeof(stats));
    vemb_v16_tlc_get_runtime_stats(reader, &stats);
    assert(stats.ub_lookup_rpc_ok >= 2);
    assert(stats.ub_lookup_rpc_handle >= 2);
    assert(stats.remote_meta_lookup_set_conflict >= 1);

    vemb_v16_ub_rpc_destroy(reader_rpc);
    vemb_v16_ub_rpc_destroy(owner_rpc);
    vemb_v16_tlc_destroy(reader);
    vemb_v16_tlc_destroy(owner);
    free(reader_meta_base);
    free(owner_meta_base);
    cleanup_rpc_rings(req_reader_owner,
                      req_owner_reader,
                      resp_reader_owner,
                      resp_owner_reader);
}

static void test_vsim_key2_lookup_remote_meta_stale(void) {
    enum { dim = 2, max_vectors = 1, remote_entries = 4, remote_buckets = 8 };
    float region[dim * max_vectors];
    float first[dim], second[dim];
    vemb_v16_warm_region_header_t allocator;
    vemb_v16_tlc_t *owner = NULL;
    vemb_v16_tlc_t *reader = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 199,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_remote_meta_view_t owner_meta;
    vemb_v16_remote_meta_view_t reader_meta;
    size_t remote_meta_bytes =
        vemb_v16_remote_meta_layout_bytes(remote_entries, remote_buckets);
    void *owner_meta_base = NULL;
    void *reader_meta_base = NULL;
    uint32_t wanted_owner = 1;
    const char *key1 = "vsim:remote-stale-old";
    const char *key2 = "vsim:remote-stale-new";
    uint64_t key1_hash = vemb_v16_xxh3_64_str(key1, strlen(key1));
    uint64_t key2_hash = vemb_v16_xxh3_64_str(key2, strlen(key2));
    vemb_v16_vector_handle_t handle = {0};
    vemb_v16_vector_handle_t remote_handle = {0};
    vemb_v16_tlc_lookup_source_t source = VEMB_V16_TLC_LOOKUP_SOURCE_LOCAL;
    uint32_t warm_slot = UINT32_MAX;
    tlc_core_stats_t stats;

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 199, max_vectors);
    assert(posix_memalign(&owner_meta_base, 64, remote_meta_bytes) == 0);
    assert(posix_memalign(&reader_meta_base, 64, remote_meta_bytes) == 0);
    assert(vemb_v16_remote_meta_init(&owner_meta,
                                     owner_meta_base,
                                     remote_meta_bytes,
                                     1,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_remote_meta_init(&reader_meta,
                                     reader_meta_base,
                                     remote_meta_bytes,
                                     2,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);

    assert(vemb_v16_tlc_create(&owner, dim, max_vectors, &warm, 1, 4) == 0);
    assert(vemb_v16_tlc_create(&reader, dim, max_vectors, &warm, 1, 4) == 0);
    vemb_v16_tlc_set_remote_meta_view(owner, &owner_meta, 8);
    vemb_v16_tlc_set_remote_meta_view(reader, &reader_meta, 8);
    assert(vemb_v16_tlc_set_remote_meta_owner_view(reader, 1, &owner_meta) == 0);
    vemb_v16_tlc_set_owner_resolver(reader,
                                    fixed_owner_resolver,
                                    &wanted_owner);

    fill_vector(first, dim, 500);
    fill_vector(second, dim, 600);
    assert(vemb_v16_tlc_put(owner, key1, (uint32_t)strlen(key1), key1_hash,
                            first, sizeof(first), &handle, &warm_slot) == 0);
    assert(publish_remote_meta_to_view(owner,
                                       owner->remote_meta_view,
                                       key1,
                                       (uint32_t)strlen(key1),
                                       key1_hash,
                                       &handle,
                                       0) == 0);
#if TLC_CORE_ALLOW_LRU_EVICTION
    assert(vemb_v16_tlc_put(owner, key2, (uint32_t)strlen(key2), key2_hash,
                            second, sizeof(second), &handle, &warm_slot) == 0);
#else
    (void)key2_hash;
    handle.owner_generation++;
    assert(publish_remote_meta_to_view(owner,
                                       owner->remote_meta_view,
                                       key1,
                                       (uint32_t)strlen(key1),
                                       key1_hash,
                                       &handle,
                                       0) == 0);
#endif

    assert(vemb_v16_tlc_lookup_vsim_key2(reader,
                                         key1,
                                         (uint32_t)strlen(key1),
                                         key1_hash,
                                         &remote_handle,
                                         &source) != 0);
    assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_NONE);

    tlc_core_get_stats(reader->core, &stats);
    assert(stats.remote_meta_stale >= 1);
    assert(stats.warm_stale_handle_reject >= 1);

    vemb_v16_tlc_destroy(reader);
    vemb_v16_tlc_destroy(owner);
    free(reader_meta_base);
    free(owner_meta_base);
}

static uint32_t fixed_owner_resolver(uint64_t key_hash,
                                     const char *key,
                                     uint32_t key_len,
                                     void *arg) {
    (void)key_hash;
    (void)key;
    (void)key_len;
    return *(uint32_t *)arg;
}

static void test_vsim_key2_lookup_remote_owner_routing(void) {
    enum { dim = 2, max_vectors = 4, remote_entries = 4, remote_buckets = 8 };
    float region1[dim * max_vectors];
    float region2[dim * max_vectors];
    float vector[dim];
    vemb_v16_warm_region_header_t alloc1, alloc2;
    vemb_v16_tlc_t *owner2 = NULL;
    vemb_v16_tlc_t *reader = NULL;
    vemb_v16_tlc_warm_region_t owner2_region = {
        .region_id = 992,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region2,
        .region_bytes = sizeof(region2),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_tlc_warm_region_t reader_regions[] = {
        {
            .region_id = 991,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = region1,
            .region_bytes = sizeof(region1),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
        {
            .region_id = 992,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 0,
            .weight = 1,
            .mapped_addr = region2,
            .region_bytes = sizeof(region2),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
    };
    vemb_v16_remote_meta_view_t owner1_meta, owner2_meta;
    size_t meta_bytes =
        vemb_v16_remote_meta_layout_bytes(remote_entries, remote_buckets);
    void *owner1_base = NULL;
    void *owner2_base = NULL;
    uint32_t wanted_owner = 2;
    const char *key2 = "vsim:owner2-key2";
    uint64_t key2_hash = vemb_v16_xxh3_64_str(key2, strlen(key2));
    vemb_v16_vector_handle_t handle = {0};
    vemb_v16_vector_handle_t remote_handle = {0};
    vemb_v16_tlc_lookup_source_t source = VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
    uint32_t warm_slot = UINT32_MAX;
    const uint8_t *bytes = NULL;
    uint32_t len = 0;

    memset(region1, 0, sizeof(region1));
    memset(region2, 0, sizeof(region2));
    init_test_allocator(&alloc1, 991, max_vectors);
    init_test_allocator(&alloc2, 992, max_vectors);
    assert(posix_memalign(&owner1_base, 64, meta_bytes) == 0);
    assert(posix_memalign(&owner2_base, 64, meta_bytes) == 0);
    assert(vemb_v16_remote_meta_init(&owner1_meta,
                                     owner1_base,
                                     meta_bytes,
                                     1,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);
    assert(vemb_v16_remote_meta_init(&owner2_meta,
                                     owner2_base,
                                     meta_bytes,
                                     2,
                                     dim * sizeof(float),
                                     remote_entries,
                                     remote_buckets) ==
           VEMB_V16_REMOTE_META_OK);

    assert(vemb_v16_tlc_create(&owner2, dim, max_vectors,
                               &owner2_region, 1, 4) == 0);
    assert(vemb_v16_tlc_create(&reader, dim, max_vectors,
                               reader_regions, 2, 4) == 0);
    vemb_v16_tlc_set_remote_meta_view(owner2, &owner2_meta, 8);
    vemb_v16_tlc_set_remote_meta_view(reader, &owner1_meta, 8);
    assert(vemb_v16_tlc_set_remote_meta_owner_view(reader, 2, &owner2_meta) == 0);
    vemb_v16_tlc_set_owner_resolver(reader,
                                    fixed_owner_resolver,
                                    &wanted_owner);

    fill_vector(vector, dim, 400);
    assert(vemb_v16_tlc_put(owner2,
                            key2,
                            (uint32_t)strlen(key2),
                            key2_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);
    assert(publish_remote_meta_to_view(owner2,
                                       owner2->remote_meta_view,
                                       key2,
                                       (uint32_t)strlen(key2),
                                       key2_hash,
                                       &handle,
                                       0) == 0);

    assert(vemb_v16_tlc_lookup_vsim_key2(reader,
                                         key2,
                                         (uint32_t)strlen(key2),
                                         key2_hash,
                                         &remote_handle,
                                         &source) == 0);
    assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_REMOTE);


    assert(remote_handle.region_id == handle.region_id);
    assert(remote_handle.offset == handle.offset);
    assert(remote_handle.bytes == handle.bytes);
    assert(remote_handle.local_slot == handle.local_slot);
    assert(remote_handle.owner_generation == handle.owner_generation);
    assert(vemb_v16_tlc_vector_slice(reader, &remote_handle, &bytes, &len) == 0);
    assert(len == sizeof(vector));
    assert(memcmp(bytes, vector, sizeof(vector)) == 0);

    vemb_v16_tlc_destroy(reader);
    vemb_v16_tlc_destroy(owner2);
    free(owner2_base);
    free(owner1_base);
}

static void test_shared_slot_meta_local_set_before_remote(void) {
    enum { dim = 2, max_vectors = 6 };
    float local0[dim], local1[dim], remote[dim * 4];
    vemb_v16_warm_region_header_t alloc0, alloc1, alloc_remote;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t regions[] = {
        {
            .region_id = 100,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = local0,
            .region_bytes = sizeof(local0),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
        {
            .region_id = 101,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = local1,
            .region_bytes = sizeof(local1),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
        {
            .region_id = 200,
            .backend_type = VEMB_V16_REGION_UB,
            .is_local = 0,
            .weight = 1,
            .mapped_addr = remote,
            .region_bytes = sizeof(remote),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
    };
    float vector[dim];
    uint32_t local_writes = 0;
    uint32_t remote_writes = 0;

    memset(local0, 0, sizeof(local0));
    memset(local1, 0, sizeof(local1));
    memset(remote, 0, sizeof(remote));
    init_test_allocator(&alloc0, 100, 1);
    init_test_allocator(&alloc1, 101, 1);
    init_test_allocator(&alloc_remote, 200, 4);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, regions, 3, 4) == 0);
    fill_vector(vector, dim, 4000);

    for (uint32_t i = 0; i < 3; i++) {
        char key[32];
        vemb_v16_vector_handle_t handle = {0};
        uint32_t warm_slot = UINT32_MAX;
        snprintf(key, sizeof(key), "local-first:%u", i);
        uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
        assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                                vector, sizeof(vector), &handle,
                                &warm_slot) == 0);
        if (handle.region_id == 100 || handle.region_id == 101)
            local_writes++;
        if (handle.region_id == 200)
            remote_writes++;
    }
#if TLC_CORE_ALLOW_LRU_EVICTION
    assert(local_writes == 3);
    assert(remote_writes == 0);
#else
    assert(local_writes == 2);
    assert(remote_writes == 1);
#endif
    assert_region_stats(tlc, 100, 1, 1);
    assert_region_stats(tlc, 101, 1, 1);
#if TLC_CORE_ALLOW_LRU_EVICTION
    assert_region_stats(tlc, 200, 0, 0);
#else
    assert_region_stats(tlc, 200, 1, 0);
#endif

    vemb_v16_tlc_destroy(tlc);
}

static void test_runtime_attach_remote_region_after_create(void) {
    enum { dim = 2, max_vectors = 8 };
    float local_region[dim];
    float remote_region[dim * 4];
    float vector[dim];
    uint8_t local_alloc_backing[sizeof(vemb_v16_warm_region_header_t) +
                                sizeof(vemb_v16_warm_slot_meta_t)];
    vemb_v16_warm_region_header_t *local_alloc = NULL;
    vemb_v16_warm_region_header_t remote_alloc;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t local = {
        .region_id = 300,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = local_region,
        .region_bytes = sizeof(local_region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_tlc_warm_region_t remote = {
        .region_id = 301,
        .backend_type = VEMB_V16_REGION_UB,
        .is_local = 0,
        .weight = 1,
        .mapped_addr = remote_region,
        .region_bytes = sizeof(remote_region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };

    memset(local_region, 0, sizeof(local_region));
    memset(remote_region, 0, sizeof(remote_region));
    local_alloc = init_test_allocator_with_slot_meta(local_alloc_backing, 300, 1);
    local.slot_meta = vemb_v16_warm_region_slot_meta(local_alloc);
    init_test_allocator(&remote_alloc, 301, 4);
    assert(vemb_v16_tlc_create(&tlc, dim, max_vectors, &local, 1, 4) == 0);
    fill_vector(vector, dim, 9000);

    {
        const char *key = "runtime-local";
        vemb_v16_vector_handle_t handle = {0};
        uint32_t warm_slot = UINT32_MAX;
        uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
        assert(vemb_v16_tlc_put(tlc, key, (uint32_t)strlen(key), key_hash,
                                vector, sizeof(vector), &handle,
                                &warm_slot) == 0);
        assert(handle.region_id == 300);
        assert_region_stats(tlc, 300, 1, 1);
        atomic_store_explicit(
            &vemb_v16_warm_region_slot_meta(local_alloc)[0].cold_state,
            VEMB_V16_WARM_SLOT_COLD_NONE,
            memory_order_release);
    }

    assert(vemb_v16_tlc_attach_warm_region(tlc, &remote) == 0);

    {
        const char *key = "runtime-remote";
        tlc_warm_location_t location = {0};
        uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
        assert(tlc_core_put_location_epoch(tlc->core,
                                           key,
                                           (uint32_t)strlen(key),
                                           key_hash,
                                           vector,
                                           sizeof(vector),
                                           0,
                                           0,
                                           &location) == 0);
        assert(location.region_id == 301);
        assert_region_stats(tlc, 301, 1, 0);
    }

    vemb_v16_tlc_destroy(tlc);
}

static void test_migration_snapshot_apply_rejects_stale(void) {
    enum { dim = 3, max_vectors = 8 };
    float source_region[dim * max_vectors];
    float dest_region[dim * max_vectors];
    float v1[dim], v2[dim];
    float snapshot_value1[dim], snapshot_value2[dim];
    vemb_v16_warm_region_header_t source_allocator;
    vemb_v16_warm_region_header_t dest_allocator;
    vemb_v16_tlc_t *source = NULL;
    vemb_v16_tlc_t *dest = NULL;
    vemb_v16_tlc_warm_region_t source_warm = {
        .region_id = 701,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = source_region,
        .region_bytes = sizeof(source_region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_tlc_warm_region_t dest_warm = {
        .region_id = 703,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = dest_region,
        .region_bytes = sizeof(dest_region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    const char *key = "migration:key";
    uint32_t key_len = (uint32_t)strlen(key);
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, key_len);
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    tlc_core_key_migration_info_t info = {0};
    tlc_core_migration_snapshot_t snap1 = {0};
    tlc_core_migration_snapshot_t snap2 = {0};
    tlc_core_migration_apply_status_t status =
        TLC_CORE_MIGRATION_ERROR;
    const uint8_t *stored = NULL;
    uint32_t stored_len = 0;

    memset(source_region, 0, sizeof(source_region));
    memset(dest_region, 0, sizeof(dest_region));
    init_test_allocator(&source_allocator, 701, max_vectors);
    init_test_allocator(&dest_allocator, 703, max_vectors);
    assert(vemb_v16_tlc_create(&source,
                               dim,
                               max_vectors,
                               &source_warm,
                               1,
                               4) == 0);
    assert(vemb_v16_tlc_create(&dest,
                               dim,
                               max_vectors,
                               &dest_warm,
                               1,
                               4) == 0);

    fill_vector(v1, dim, 7000);
    assert(vemb_v16_tlc_put(source,
                            key,
                            key_len,
                            key_hash,
                            v1,
                            sizeof(v1),
                            &handle,
                            &warm_slot) == 0);
    assert(tlc_core_get_migration_info(source->core,
                                           key,
                                           key_len,
                                           key_hash,
                                           &info) == 0);
    assert(info.key_version == 1);
    assert(info.migration_state == TLC_CORE_KEY_SOURCE_ACTIVE);

    assert(tlc_core_mark_migrating_in_shard(source->core,
                                            key,
                                            key_len,
                                            key_hash,
                                            2,
                                            3,
                                            0,
                                            &info) == 0);
    assert(info.key_version == 1);
    assert(info.topology_epoch == 2);
    assert(info.migration_state == TLC_CORE_KEY_MIGRATING);
    assert(info.target_owner == 3);

    assert(tlc_core_snapshot(source->core,
                                 key,
                                 key_len,
                                 key_hash,
                                 1,
                                 3,
                                 &snap1,
                                 snapshot_value1,
                                 sizeof(snapshot_value1)) == 0);
    assert(snap1.key_version == 1);
    assert(snap1.topology_epoch == 2);
    assert(snap1.source_owner == 1);
    assert(snap1.target_owner == 3);
    assert(memcmp(snapshot_value1, v1, sizeof(v1)) == 0);

    assert(vemb_v16_tlc_apply_migration(dest,
                                        &snap1,
                                        snapshot_value1,
                                        sizeof(snapshot_value1),
                                        &status,
                                        &handle) == 0);
    assert(status == TLC_CORE_MIGRATION_APPLIED);
    assert(handle.region_id == 703);
    assert(vemb_v16_tlc_vector_slice(dest,
                                     &handle,
                                     &stored,
                                     &stored_len) == 0);
    assert(stored_len == sizeof(v1));
    assert(memcmp(stored, v1, sizeof(v1)) == 0);

    fill_vector(v2, dim, 8000);
    assert(vemb_v16_tlc_put(source,
                            key,
                            key_len,
                            key_hash,
                            v2,
                            sizeof(v2),
                            &handle,
                            &warm_slot) == 0);
    assert(tlc_core_snapshot(source->core,
                                 key,
                                 key_len,
                                 key_hash,
                                 1,
                                 3,
                                 &snap2,
                                 snapshot_value2,
                                 sizeof(snapshot_value2)) == 0);
    assert(snap2.key_version == 2);
    assert(memcmp(snapshot_value2, v2, sizeof(v2)) == 0);

    assert(vemb_v16_tlc_apply_migration(dest,
                                        &snap2,
                                        snapshot_value2,
                                        sizeof(snapshot_value2),
                                        &status,
                                        &handle) == 0);
    assert(status == TLC_CORE_MIGRATION_APPLIED);
    assert(vemb_v16_tlc_apply_migration(dest,
                                        &snap2,
                                        snapshot_value2,
                                        sizeof(snapshot_value2),
                                        &status,
                                        &handle) == 0);
    assert(status == TLC_CORE_MIGRATION_DUPLICATE);

    assert(vemb_v16_tlc_apply_migration(dest,
                                        &snap1,
                                        snapshot_value1,
                                        sizeof(snapshot_value1),
                                        &status,
                                        &handle) == 0);
    assert(status == TLC_CORE_MIGRATION_STALE_REJECTED);
    assert(vemb_v16_tlc_get_handle(dest,
                                   key,
                                   key_len,
                                   key_hash,
                                   &handle,
                                   &warm_slot) == 0);
    assert(vemb_v16_tlc_vector_slice(dest,
                                     &handle,
                                     &stored,
                                     &stored_len) == 0);
    assert(stored_len == sizeof(v2));
    assert(memcmp(stored, v2, sizeof(v2)) == 0);

    vemb_v16_tlc_destroy(dest);
    vemb_v16_tlc_destroy(source);
}

static void test_put_with_epoch_rejects_stale_epoch(void) {
    enum { dim = 2, max_vectors = 4 };
    float region[dim * max_vectors];
    float v1[dim], v2[dim], v3[dim];
    vemb_v16_warm_region_header_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 709,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    const char *key = "migration:epoch-write";
    uint32_t key_len = (uint32_t)strlen(key);
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, key_len);
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    tlc_core_key_migration_info_t info = {0};
    const uint8_t *stored = NULL;
    uint32_t stored_len = 0;

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 709, max_vectors);
    assert(vemb_v16_tlc_create(&tlc,
                               dim,
                               max_vectors,
                               &warm,
                               1,
                               4) == 0);

    fill_vector(v1, dim, 7300);
    assert(vemb_v16_tlc_put_with_epoch(tlc,
                                       key,
                                       key_len,
                                       key_hash,
                                       v1,
                                       sizeof(v1),
                                       7,
                                       &handle,
                                       &warm_slot) == 0);
    assert(tlc_core_get_migration_info(tlc->core,
                                           key,
                                           key_len,
                                           key_hash,
                                           &info) == 0);
    assert(info.key_version == 1);
    assert(info.topology_epoch == 7);

    fill_vector(v2, dim, 7400);
    assert(vemb_v16_tlc_put_with_epoch(tlc,
                                       key,
                                       key_len,
                                       key_hash,
                                       v2,
                                       sizeof(v2),
                                       6,
                                       &handle,
                                       &warm_slot) != 0);
    assert(tlc_core_get_migration_info(tlc->core,
                                           key,
                                           key_len,
                                           key_hash,
                                           &info) == 0);
    assert(info.key_version == 1);
    assert(info.topology_epoch == 7);
    assert(vemb_v16_tlc_get_handle(tlc,
                                   key,
                                   key_len,
                                   key_hash,
                                   &handle,
                                   &warm_slot) == 0);
    assert(vemb_v16_tlc_vector_slice(tlc,
                                     &handle,
                                     &stored,
                                     &stored_len) == 0);
    assert(stored_len == sizeof(v1));
    assert(memcmp(stored, v1, sizeof(v1)) == 0);

    fill_vector(v3, dim, 7500);
    assert(vemb_v16_tlc_put_with_epoch(tlc,
                                       key,
                                       key_len,
                                       key_hash,
                                       v3,
                                       sizeof(v3),
                                       8,
                                       &handle,
                                       &warm_slot) == 0);
    assert(tlc_core_get_migration_info(tlc->core,
                                           key,
                                           key_len,
                                           key_hash,
                                           &info) == 0);
    assert(info.key_version == 2);
    assert(info.topology_epoch == 8);

    vemb_v16_tlc_destroy(tlc);
}

static void test_migration_source_cutover_rejects_old_owner_access(void) {
    enum { dim = 2, max_vectors = 4 };
    float source_region[dim * max_vectors];
    float v1[dim], v2[dim], v3[dim], snapshot_value[dim];
    vemb_v16_warm_region_header_t source_allocator;
    vemb_v16_tlc_t *source = NULL;
    vemb_v16_tlc_warm_region_t source_warm = {
        .region_id = 711,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = source_region,
        .region_bytes = sizeof(source_region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    const char *key = "migration:cutover:key";
    uint32_t key_len = (uint32_t)strlen(key);
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, key_len);
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    tlc_core_key_migration_info_t info = {0};
    tlc_core_migration_snapshot_t snapshot = {0};

    memset(source_region, 0, sizeof(source_region));
    init_test_allocator(&source_allocator, 711, max_vectors);
    assert(vemb_v16_tlc_create(&source,
                               dim,
                               max_vectors,
                               &source_warm,
                               1,
                               4) == 0);

    fill_vector(v1, dim, 7100);
    assert(vemb_v16_tlc_put(source,
                            key,
                            key_len,
                            key_hash,
                            v1,
                            sizeof(v1),
                            &handle,
                            &warm_slot) == 0);
    assert(tlc_core_mark_migrating_in_shard(source->core,
                                            key,
                                            key_len,
                                            key_hash,
                                            20,
                                            3,
                                            0,
                                            &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_MIGRATING);
    assert(info.owner_epoch == 0);

    fill_vector(v2, dim, 7200);
    assert(vemb_v16_tlc_put(source,
                            key,
                            key_len,
                            key_hash,
                            v2,
                            sizeof(v2),
                            &handle,
                            &warm_slot) == 0);
    assert(tlc_core_mark_cutover(source->core,
                                     key,
                                     key_len,
                                     key_hash,
                                     19,
                                     3,
                                     &info) != 0);
    assert(tlc_core_mark_cutover(source->core,
                                     key,
                                     key_len,
                                     key_hash,
                                     21,
                                     3,
                                     &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_CUTOVER);
    assert(info.topology_epoch == 21);
    assert(info.owner_epoch == 21);
    assert(info.target_owner == 3);
    assert(tlc_core_key_is_source_cutover(source->core,
                                              key,
                                              key_len,
                                              key_hash,
                                              &info) == 1);
    assert(vemb_v16_tlc_get_handle(source,
                                   key,
                                   key_len,
                                   key_hash,
                                   &handle,
                                   &warm_slot) != 0);

    fill_vector(v3, dim, 7300);
    assert(vemb_v16_tlc_put(source,
                            key,
                            key_len,
                            key_hash,
                            v3,
                            sizeof(v3),
                            &handle,
                            &warm_slot) != 0);
    assert(tlc_core_snapshot(source->core,
                                 key,
                                 key_len,
                                 key_hash,
                                 1,
                                 3,
                                 &snapshot,
                                 snapshot_value,
                                 sizeof(snapshot_value)) != 0);
    assert(tlc_core_mark_migrating_in_shard(source->core,
                                            key,
                                            key_len,
                                            key_hash,
                                            22,
                                            3,
                                            0,
                                            &info) != 0);

    vemb_v16_tlc_destroy(source);
}

static void test_migration_delta_rpc_apply_idempotent_and_tombstone(void) {
    enum { dim = 2, max_vectors = 8 };
    float source_region[dim * max_vectors];
    float dest_region[dim * max_vectors];
    float v1[dim], v2[dim], stale[dim];
    vemb_v16_warm_region_header_t source_allocator;
    vemb_v16_warm_region_header_t dest_allocator;
    vemb_v16_tlc_t *dest = NULL;
    vemb_v16_tlc_warm_region_t regions[] = {
        {
            .region_id = 901,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 0,
            .weight = 1,
            .mapped_addr = source_region,
            .region_bytes = sizeof(source_region),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
        {
            .region_id = 903,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = dest_region,
            .region_bytes = sizeof(dest_region),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
    };
    const char *key = "migration:delta:key";
    uint32_t key_len = (uint32_t)strlen(key);
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, key_len);
    vemb_v16_ub_migration_rpc_req_t req = {0};
    vemb_v16_ub_migration_rpc_req_t lease_req = {0};
    vemb_v16_ub_migration_rpc_resp_t resp = {0};
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    const uint8_t *stored = NULL;
    uint32_t stored_len = 0;
    tlc_core_key_migration_info_t info = {0};

    memset(source_region, 0, sizeof(source_region));
    memset(dest_region, 0, sizeof(dest_region));
    init_test_allocator(&source_allocator, 901, max_vectors);
    init_test_allocator(&dest_allocator, 903, max_vectors);
    assert(vemb_v16_tlc_create(&dest,
                               dim,
                               max_vectors,
                               regions,
                               2,
                               4) == 0);

    lease_req.request_id = 9001;
    lease_req.src_owner_id = 1;
    lease_req.dst_owner_id = 3;
    lease_req.op = VEMB_V16_UB_MIGRATION_RPC_LEASE_COMMIT_REQ;
    lease_req.key_hash = key_hash;
    lease_req.topology_epoch = 45;
    lease_req.key_len = key_len;
    lease_req.target_owner_id = 3;
    memcpy(lease_req.key, key, key_len);
    lease_req.lease = (vemb_v16_ub_migration_lease_desc_t){
        .topology_epoch = 45,
        .owner_epoch = 45,
        .source_owner = 1,
        .target_owner = 3,
        .shard_id = 5,
    };
    assert(vemb_v16_tlc_migration_rpc_local_handler(dest,
                                                    &lease_req,
                                                    &resp) == 0);
    assert(resp.status == VEMB_V16_UB_MIGRATION_RPC_RETRY);

    fill_vector(v1, dim, 9100);
    memcpy(source_region, v1, sizeof(v1));
    req.request_id = 9101;
    req.src_owner_id = 1;
    req.dst_owner_id = 3;
    req.op = VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT;
    req.key_hash = key_hash;
    req.topology_epoch = 44;
    req.key_len = key_len;
    req.target_owner_id = 3;
    memcpy(req.key, key, key_len);
    req.delta = (vemb_v16_ub_migration_delta_desc_t){
        .key_hash = key_hash,
        .key_version = 11,
        .topology_epoch = 44,
        .delta_seq = 7,
        .op = VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT,
        .shard_id = 5,
        .key_len = key_len,
        .source_owner = 1,
        .target_owner = 3,
        .value_size = sizeof(v1),
        .region_id = 901,
        .local_slot = 0,
        .bytes = sizeof(v1),
        .offset = 0,
        .owner_generation = 1,
    };
    memcpy(req.delta.key, key, key_len);

    assert(vemb_v16_tlc_migration_rpc_local_handler(dest, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_UB_MIGRATION_RPC_OK);
    assert(resp.delta_ack.status == VEMB_V16_UB_MIGRATION_RPC_OK);
    assert(resp.delta_ack.applied_seq == 7);
    assert(resp.delta_ack.source_owner == 1);
    assert(resp.delta_ack.target_owner == 3);
    assert(resp.delta_ack.shard_id == 5);
    assert(vemb_v16_tlc_get_handle(dest,
                                   key,
                                   key_len,
                                   key_hash,
                                   &handle,
                                   &warm_slot) == 0);
    assert(handle.region_id == 903);
    assert(warm_slot == 0);
    assert(vemb_v16_tlc_vector_slice(dest,
                                     &handle,
                                     &stored,
                                     &stored_len) == 0);
    assert(stored_len == sizeof(v1));
    assert(memcmp(stored, v1, sizeof(v1)) == 0);

    req.request_id++;
    memset(&resp, 0, sizeof(resp));
    assert(vemb_v16_tlc_migration_rpc_local_handler(dest, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_UB_MIGRATION_RPC_DUPLICATE);
    assert(resp.delta_ack.status == VEMB_V16_UB_MIGRATION_RPC_DUPLICATE);
    assert(resp.delta_ack.applied_seq == 7);

    fill_vector(stale, dim, 9050);
    memcpy(source_region + dim, stale, sizeof(stale));
    req.request_id++;
    req.delta.key_version = 10;
    req.delta.delta_seq = 8;
    req.delta.local_slot = 1;
    req.delta.offset = sizeof(v1);
    memset(&resp, 0, sizeof(resp));
    assert(vemb_v16_tlc_migration_rpc_local_handler(dest, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_UB_MIGRATION_RPC_STALE_REJECTED);
    assert(resp.delta_ack.status ==
           VEMB_V16_UB_MIGRATION_RPC_STALE_REJECTED);
    assert(vemb_v16_tlc_get_handle(dest,
                                   key,
                                   key_len,
                                   key_hash,
                                   &handle,
                                   &warm_slot) == 0);
    assert(vemb_v16_tlc_vector_slice(dest,
                                     &handle,
                                     &stored,
                                     &stored_len) == 0);
    assert(stored_len == sizeof(v1));
    assert(memcmp(stored, v1, sizeof(v1)) == 0);

    fill_vector(v2, dim, 9200);
    memcpy(source_region + dim * 2, v2, sizeof(v2));
    req.request_id++;
    req.delta.key_version = 12;
    req.delta.delta_seq = 9;
    req.delta.local_slot = 2;
    req.delta.offset = sizeof(v1) * 2;
    memset(&resp, 0, sizeof(resp));
    assert(vemb_v16_tlc_migration_rpc_local_handler(dest, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_UB_MIGRATION_RPC_OK);
    assert(resp.delta_ack.applied_seq == 9);
    assert(vemb_v16_tlc_get_handle(dest,
                                   key,
                                   key_len,
                                   key_hash,
                                   &handle,
                                   &warm_slot) == 0);
    assert(handle.region_id == 903);
    assert(vemb_v16_tlc_vector_slice(dest,
                                     &handle,
                                     &stored,
                                     &stored_len) == 0);
    assert(stored_len == sizeof(v2));
    assert(memcmp(stored, v2, sizeof(v2)) == 0);

    req.request_id++;
    req.op = VEMB_V16_UB_MIGRATION_RPC_DELTA_DELETE;
    req.delta.op = VEMB_V16_UB_MIGRATION_RPC_DELTA_DELETE;
    req.delta.key_version = 13;
    req.delta.delta_seq = 10;
    req.delta.tombstone = 1;
    req.delta.value_size = 0;
    req.delta.region_id = TLC_CORE_INVALID_REGION_ID;
    req.delta.local_slot = TLC_CORE_INVALID_SLOT;
    req.delta.bytes = 0;
    req.delta.offset = 0;
    req.delta.owner_generation = 0;
    memset(&resp, 0, sizeof(resp));
    assert(vemb_v16_tlc_migration_rpc_local_handler(dest, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_UB_MIGRATION_RPC_OK);
    assert(resp.delta_ack.applied_seq == 10);
    assert(vemb_v16_tlc_get_handle(dest,
                                   key,
                                   key_len,
                                   key_hash,
                                   &handle,
                                   &warm_slot) != 0);
    assert(tlc_core_get_migration_info(dest->core,
                                           key,
                                           key_len,
                                           key_hash,
                                           &info) == 0);
    assert(info.key_version == 13);
    assert(info.tombstone == 1);
    assert(info.migration_state == TLC_CORE_KEY_DEST_COMMITTED);

    req.request_id++;
    req.op = VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT;
    req.delta.op = VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT;
    req.delta.key_version = 12;
    req.delta.delta_seq = 11;
    req.delta.tombstone = 0;
    req.delta.value_size = sizeof(v2);
    req.delta.region_id = 901;
    req.delta.local_slot = 2;
    req.delta.bytes = sizeof(v2);
    req.delta.offset = sizeof(v1) * 2;
    req.delta.owner_generation = 1;
    memset(&resp, 0, sizeof(resp));
    assert(vemb_v16_tlc_migration_rpc_local_handler(dest, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_UB_MIGRATION_RPC_STALE_REJECTED);
    assert(resp.delta_ack.status ==
           VEMB_V16_UB_MIGRATION_RPC_STALE_REJECTED);
    assert(resp.delta_ack.applied_seq == 11);
    assert(vemb_v16_tlc_get_handle(dest,
                                   key,
                                   key_len,
                                   key_hash,
                                   &handle,
                                   &warm_slot) != 0);

    req.request_id++;
    req.op = VEMB_V16_UB_MIGRATION_RPC_BARRIER_REQ;
    req.barrier = (vemb_v16_ub_migration_barrier_desc_t){
        .topology_epoch = 44,
        .barrier_seq = 11,
        .source_owner = 1,
        .target_owner = 3,
        .shard_id = 5,
    };
    const char *missing_key = "migration:delta:missing";
    uint32_t missing_key_len = (uint32_t)strlen(missing_key);
    uint64_t missing_key_hash = vemb_v16_xxh3_64_str(missing_key,
                                                 missing_key_len);
    req.key_hash = missing_key_hash;
    req.key_len = missing_key_len;
    memset(req.key, 0, sizeof(req.key));
    memcpy(req.key, missing_key, missing_key_len);
    memset(&resp, 0, sizeof(resp));
    assert(vemb_v16_tlc_migration_rpc_local_handler(dest, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_UB_MIGRATION_RPC_RETRY);
    assert(resp.delta_ack.applied_seq == 11);
    assert(resp.delta_ack.barrier_seq == 11);

    req.request_id++;
    req.key_hash = key_hash;
    req.key_len = key_len;
    memset(req.key, 0, sizeof(req.key));
    memcpy(req.key, key, key_len);
    memset(&resp, 0, sizeof(resp));
    assert(vemb_v16_tlc_migration_rpc_local_handler(dest, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_UB_MIGRATION_RPC_OK);
    assert(resp.delta_ack.applied_seq == 11);
    assert(resp.delta_ack.barrier_seq == 11);
    assert(resp.barrier.barrier_seq == 11);

    req.request_id++;
    req.barrier.barrier_seq = 12;
    memset(&resp, 0, sizeof(resp));
    assert(vemb_v16_tlc_migration_rpc_local_handler(dest, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_UB_MIGRATION_RPC_RETRY);
    assert(resp.delta_ack.applied_seq == 11);
    assert(resp.delta_ack.barrier_seq == 12);

    req.request_id++;
    req.op = VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT;
    req.delta.op = VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT;
    req.delta.key_version = 14;
    req.delta.delta_seq = 12;
    req.delta.tombstone = 0;
    req.delta.value_size = sizeof(v2);
    req.delta.region_id = 901;
    req.delta.local_slot = 2;
    req.delta.bytes = sizeof(v2);
    req.delta.offset = sizeof(v1) * 2;
    req.delta.owner_generation = 1;
    memset(&resp, 0, sizeof(resp));
    assert(vemb_v16_tlc_migration_rpc_local_handler(dest, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_UB_MIGRATION_RPC_OK);
    assert(resp.delta_ack.applied_seq == 12);
    assert(resp.delta_ack.barrier_seq == 12);
    assert(vemb_v16_tlc_get_handle(dest,
                                   key,
                                   key_len,
                                   key_hash,
                                   &handle,
                                   &warm_slot) == 0);
    assert(vemb_v16_tlc_vector_slice(dest,
                                     &handle,
                                     &stored,
                                     &stored_len) == 0);
    assert(stored_len == sizeof(v2));
    assert(memcmp(stored, v2, sizeof(v2)) == 0);

    req.request_id++;
    req.op = VEMB_V16_UB_MIGRATION_RPC_BARRIER_REQ;
    req.barrier.barrier_seq = 12;
    memset(&resp, 0, sizeof(resp));
    assert(vemb_v16_tlc_migration_rpc_local_handler(dest, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_UB_MIGRATION_RPC_OK);
    assert(resp.delta_ack.applied_seq == 12);
    assert(resp.delta_ack.barrier_seq == 12);

    req.request_id++;
    req.op = VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT;
    req.delta.delta_seq = 14;
    req.delta.key_version = 15;
    memset(&resp, 0, sizeof(resp));
    assert(vemb_v16_tlc_migration_rpc_local_handler(dest, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_UB_MIGRATION_RPC_RETRY);
    assert(resp.delta_ack.applied_seq == 12);

    lease_req.request_id++;
    memset(&resp, 0, sizeof(resp));
    assert(vemb_v16_tlc_migration_rpc_local_handler(dest,
                                                    &lease_req,
                                                    &resp) == 0);
    assert(resp.status == VEMB_V16_UB_MIGRATION_RPC_OK);
    assert(resp.lease.owner_epoch == 45);
    assert(resp.lease.target_owner == 3);
    assert(tlc_core_get_migration_info(dest->core,
                                           key,
                                           key_len,
                                           key_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_DEST_COMMITTED);
    assert(info.owner_epoch == 45);

    lease_req.request_id++;
    lease_req.lease.owner_epoch = 44;
    memset(&resp, 0, sizeof(resp));
    assert(vemb_v16_tlc_migration_rpc_local_handler(dest,
                                                    &lease_req,
                                                    &resp) == 0);
    assert(resp.status == VEMB_V16_UB_MIGRATION_RPC_RETRY);
    assert(tlc_core_get_migration_info(dest->core,
                                           key,
                                           key_len,
                                           key_hash,
                                           &info) == 0);
    assert(info.owner_epoch == 45);

    vemb_v16_tlc_destroy(dest);
}

static void test_migration_delta_ub_ring_rpc_put(void) {
    enum { dim = 2, max_vectors = 8 };
    float source_region[dim * max_vectors];
    float dest_region[dim * max_vectors];
    float vector[dim];
    vemb_v16_warm_region_header_t source_allocator;
    vemb_v16_warm_region_header_t dest_allocator;
    vemb_v16_tlc_t *source = NULL;
    vemb_v16_tlc_t *dest = NULL;
    vemb_v16_ub_rpc_t *source_rpc = NULL;
    vemb_v16_ub_rpc_t *dest_rpc = NULL;
    vemb_v16_tlc_warm_region_t source_warm = {
        .region_id = 921,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = source_region,
        .region_bytes = sizeof(source_region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_tlc_warm_region_t dest_regions[] = {
        {
            .region_id = 921,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 0,
            .weight = 1,
            .mapped_addr = source_region,
            .region_bytes = sizeof(source_region),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
        {
            .region_id = 923,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = dest_region,
            .region_bytes = sizeof(dest_region),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
    };
    vemb_v16_ub_rpc_peer_t source_peer;
    vemb_v16_ub_rpc_peer_t dest_peer;
    char req_source_dest[64];
    char req_dest_source[64];
    char resp_source_dest[64];
    char resp_dest_source[64];
    const char *key = "migration:delta:ring:key";
    uint32_t key_len = (uint32_t)strlen(key);
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, key_len);
    vemb_v16_vector_handle_t source_handle = {0};
    vemb_v16_vector_handle_t dest_handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    vemb_v16_ub_migration_rpc_req_t req = {0};
    vemb_v16_ub_migration_rpc_resp_t resp = {0};
    const uint8_t *stored = NULL;
    uint32_t stored_len = 0;

    snprintf(req_source_dest, sizeof(req_source_dest),
             "/v16mig_delta_%ld_req_1_3", (long)getpid());
    snprintf(req_dest_source, sizeof(req_dest_source),
             "/v16mig_delta_%ld_req_3_1", (long)getpid());
    snprintf(resp_source_dest, sizeof(resp_source_dest),
             "/v16mig_delta_%ld_resp_1_3", (long)getpid());
    snprintf(resp_dest_source, sizeof(resp_dest_source),
             "/v16mig_delta_%ld_resp_3_1", (long)getpid());
    cleanup_rpc_rings(req_source_dest,
                      req_dest_source,
                      resp_source_dest,
                      resp_dest_source);

    memset(source_region, 0, sizeof(source_region));
    memset(dest_region, 0, sizeof(dest_region));
    init_test_allocator(&source_allocator, 921, max_vectors);
    init_test_allocator(&dest_allocator, 923, max_vectors);
    assert(vemb_v16_tlc_create(&source,
                               dim,
                               max_vectors,
                               &source_warm,
                               1,
                               4) == 0);
    assert(vemb_v16_tlc_create(&dest,
                               dim,
                               max_vectors,
                               dest_regions,
                               2,
                               4) == 0);
    make_rpc_peers(&source_peer,
                   3,
                   &dest_peer,
                   1,
                   req_source_dest,
                   req_dest_source,
                   resp_source_dest,
                   resp_dest_source);
    assert(vemb_v16_ub_rpc_create(&source_rpc,
                                  source,
                                  1,
                                  100,
                                  &source_peer,
                                  1) == 0);
    assert(vemb_v16_ub_rpc_create(&dest_rpc,
                                  dest,
                                  3,
                                  100,
                                  &dest_peer,
                                  1) == 0);

    fill_vector(vector, dim, 9300);
    assert(vemb_v16_tlc_put(source,
                            key,
                            key_len,
                            key_hash,
                            vector,
                            sizeof(vector),
                            &source_handle,
                            &warm_slot) == 0);
    assert(source_handle.region_id == 921);
    assert(source_handle.bytes == sizeof(vector));

    req.request_id = 9301;
    req.src_owner_id = 1;
    req.dst_owner_id = 3;
    req.op = VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT;
    req.key_hash = key_hash;
    req.topology_epoch = 55;
    req.key_len = key_len;
    req.target_owner_id = 3;
    memcpy(req.key, key, key_len);
    req.delta = (vemb_v16_ub_migration_delta_desc_t){
        .key_hash = key_hash,
        .key_version = 1,
        .topology_epoch = 55,
        .delta_seq = 1,
        .op = VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT,
        .shard_id = 9,
        .key_len = key_len,
        .source_owner = 1,
        .target_owner = 3,
        .value_size = sizeof(vector),
        .region_id = source_handle.region_id,
        .local_slot = source_handle.local_slot,
        .bytes = source_handle.bytes,
        .offset = source_handle.offset,
        .owner_generation = source_handle.owner_generation,
    };
    memcpy(req.delta.key, key, key_len);

    assert(vemb_v16_ub_rpc_migrate_request(source_rpc, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_UB_MIGRATION_RPC_OK);
    assert(resp.request_id == req.request_id);
    assert(resp.op == VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT);
    assert(resp.delta_ack.status == VEMB_V16_UB_MIGRATION_RPC_OK);
    assert(resp.delta_ack.applied_seq == 1);
    assert(vemb_v16_tlc_get_handle(dest,
                                   key,
                                   key_len,
                                   key_hash,
                                   &dest_handle,
                                   &warm_slot) == 0);
    assert(dest_handle.region_id == 923);
    assert(vemb_v16_tlc_vector_slice(dest,
                                     &dest_handle,
                                     &stored,
                                     &stored_len) == 0);
    assert(stored_len == sizeof(vector));
    assert(memcmp(stored, vector, sizeof(vector)) == 0);

    vemb_v16_ub_rpc_destroy(dest_rpc);
    vemb_v16_ub_rpc_destroy(source_rpc);
    vemb_v16_tlc_destroy(dest);
    vemb_v16_tlc_destroy(source);
    cleanup_rpc_rings(req_source_dest,
                      req_dest_source,
                      resp_source_dest,
                      resp_dest_source);
}

static void snapshot_desc_to_core_snapshot(
        const vemb_v16_ub_migration_snapshot_desc_t *desc,
        tlc_core_migration_snapshot_t *snapshot) {
    memset(snapshot, 0, sizeof(*snapshot));
    snapshot->key_hash = desc->key_hash;
    snapshot->key_version = desc->key_version;
    snapshot->topology_epoch = desc->topology_epoch;
    snapshot->key_len = desc->key_len;
    snapshot->migration_state = desc->migration_state;
    snapshot->source_owner = desc->source_owner;
    snapshot->target_owner = desc->target_owner;
    snapshot->tombstone = desc->tombstone;
    snapshot->value_size = desc->value_size;
    snapshot->shard_id = desc->shard_id;
    snapshot->location = (tlc_warm_location_t){
        .region_id = desc->region_id,
        .region_index = UINT32_MAX,
        .local_slot = desc->local_slot,
        .bytes = desc->bytes,
        .offset = desc->offset,
        .owner_generation = desc->owner_generation,
    };
    memcpy(snapshot->key, desc->key, desc->key_len);
}

static void test_migration_snapshot_ub_ring_rpc_descriptor(void) {
    enum { dim = 2, max_vectors = 8 };
    float source_region[dim * max_vectors];
    float dest_region[dim * max_vectors];
    float vector[dim];
    float snapshot_value[dim];
    vemb_v16_warm_region_header_t source_allocator;
    vemb_v16_warm_region_header_t dest_allocator;
    vemb_v16_tlc_t *source = NULL;
    vemb_v16_tlc_t *dest = NULL;
    vemb_v16_ub_rpc_t *source_rpc = NULL;
    vemb_v16_ub_rpc_t *dest_rpc = NULL;
    vemb_v16_tlc_warm_region_t source_warm = {
        .region_id = 801,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = source_region,
        .region_bytes = sizeof(source_region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_tlc_warm_region_t dest_warm = {
        .region_id = 803,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = dest_region,
        .region_bytes = sizeof(dest_region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_ub_rpc_peer_t source_peer;
    vemb_v16_ub_rpc_peer_t dest_peer;
    char req_source_dest[64];
    char req_dest_source[64];
    char resp_source_dest[64];
    char resp_dest_source[64];
    const char *key = "migration:ring:key";
    uint32_t key_len = (uint32_t)strlen(key);
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, key_len);
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    tlc_core_key_migration_info_t info = {0};
    vemb_v16_ub_migration_rpc_req_t req = {0};
    vemb_v16_ub_migration_rpc_resp_t resp = {0};
    tlc_core_migration_snapshot_t snapshot = {0};
    tlc_core_migration_apply_status_t status =
        TLC_CORE_MIGRATION_ERROR;
    const uint8_t *stored = NULL;
    uint32_t stored_len = 0;

    snprintf(req_source_dest, sizeof(req_source_dest),
             "/v16mig_%ld_req_1_3", (long)getpid());
    snprintf(req_dest_source, sizeof(req_dest_source),
             "/v16mig_%ld_req_3_1", (long)getpid());
    snprintf(resp_source_dest, sizeof(resp_source_dest),
             "/v16mig_%ld_resp_1_3", (long)getpid());
    snprintf(resp_dest_source, sizeof(resp_dest_source),
             "/v16mig_%ld_resp_3_1", (long)getpid());
    cleanup_rpc_rings(req_source_dest,
                      req_dest_source,
                      resp_source_dest,
                      resp_dest_source);

    memset(source_region, 0, sizeof(source_region));
    memset(dest_region, 0, sizeof(dest_region));
    init_test_allocator(&source_allocator, 801, max_vectors);
    init_test_allocator(&dest_allocator, 803, max_vectors);
    assert(vemb_v16_tlc_create(&source,
                               dim,
                               max_vectors,
                               &source_warm,
                               1,
                               4) == 0);
    assert(vemb_v16_tlc_create(&dest,
                               dim,
                               max_vectors,
                               &dest_warm,
                               1,
                               4) == 0);
    make_rpc_peers(&source_peer,
                   3,
                   &dest_peer,
                   1,
                   req_source_dest,
                   req_dest_source,
                   resp_source_dest,
                   resp_dest_source);
    assert(vemb_v16_ub_rpc_create(&source_rpc,
                                  source,
                                  1,
                                  100,
                                  &source_peer,
                                  1) == 0);
    assert(vemb_v16_ub_rpc_create(&dest_rpc,
                                  dest,
                                  3,
                                  100,
                                  &dest_peer,
                                  1) == 0);

    fill_vector(vector, dim, 9000);
    assert(vemb_v16_tlc_put(source,
                            key,
                            key_len,
                            key_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);
    assert(tlc_core_mark_migrating_in_shard(source->core,
                                            key,
                                            key_len,
                                            key_hash,
                                            33,
                                            3,
                                            0,
                                            &info) == 0);

    req.request_id = 9001;
    req.src_owner_id = 3;
    req.dst_owner_id = 1;
    req.op = VEMB_V16_UB_MIGRATION_RPC_SNAPSHOT_REQ;
    req.key_hash = key_hash;
    req.topology_epoch = 33;
    req.key_len = key_len;
    req.target_owner_id = 3;
    memcpy(req.key, key, key_len);
    assert(vemb_v16_ub_rpc_migrate_snapshot(dest_rpc,
                                            &req,
                                            &resp) == 0);
    assert(resp.status == VEMB_V16_UB_MIGRATION_RPC_OK);
    assert(resp.request_id == req.request_id);
    assert(resp.snapshot.key_hash == key_hash);
    assert(resp.snapshot.key_version == 1);
    assert(resp.snapshot.topology_epoch == 33);
    assert(resp.snapshot.source_owner == 1);
    assert(resp.snapshot.target_owner == 3);
    assert(resp.snapshot.region_id == 801);
    assert(resp.snapshot.bytes == sizeof(vector));
    assert(resp.snapshot.offset + resp.snapshot.bytes <=
           sizeof(source_region));

    memcpy(snapshot_value,
           (const uint8_t *)source_region + resp.snapshot.offset,
           resp.snapshot.bytes);
    assert(memcmp(snapshot_value, vector, sizeof(vector)) == 0);
    snapshot_desc_to_core_snapshot(&resp.snapshot, &snapshot);
    assert(vemb_v16_tlc_apply_migration(dest,
                                        &snapshot,
                                        snapshot_value,
                                        sizeof(snapshot_value),
                                        &status,
                                        &handle) == 0);
    assert(status == TLC_CORE_MIGRATION_APPLIED);
    assert(vemb_v16_tlc_vector_slice(dest,
                                     &handle,
                                     &stored,
                                     &stored_len) == 0);
    assert(stored_len == sizeof(vector));
    assert(memcmp(stored, vector, sizeof(vector)) == 0);

    vemb_v16_ub_rpc_destroy(dest_rpc);
    vemb_v16_ub_rpc_destroy(source_rpc);
    vemb_v16_tlc_destroy(dest);
    vemb_v16_tlc_destroy(source);
    cleanup_rpc_rings(req_source_dest,
                      req_dest_source,
                      resp_source_dest,
                      resp_dest_source);
}

int main(void) {
    monotonicInit();

    test_persistent_cold_write_order();
    test_fuzzy_checkpoint_concurrent_writes();
    test_checkpoint_aof_full_recovery_consistency();
    test_single_node_recovery_failure_blocks_startup();
    test_recovery_strict_term_and_state_boundaries();
    test_put_get_handle();
    test_overwrite_and_capacity();
    test_eviction_rejects_stale_handle();
#if TLC_CORE_ENABLE_COLD_LAYER
    test_cold_read_through_promotes_warm_handle();
#else
    test_disabled_cold_append_is_noop();
#endif
    test_cold_same_key_updates_do_not_exhaust_log();
    test_hot_is_cache_only();
#if TLC_CORE_ENABLE_COLD_LAYER
    test_prefill_distribution_stays_warm();
#endif
    test_concurrent_distinct_keys();
    test_multi_region_local_full_fallback_and_overwrite();
    test_multi_region_all_full_evicts_committed_warm();
    test_shared_slot_meta_two_tlcs_unique_slots();
    test_vsim_key2_lookup_local_source();
    test_vsim_key2_lookup_remote_source();
    test_remote_meta_async_publish_flush();
    test_vsim_key2_lookup_rpc_fallback_and_repair();
    test_vsim_key2_lookup_ub_ring_rpc_fallback_and_repair();
    test_vsim_key2_lookup_ub_ring_rpc_concurrent();
    test_vsim_key2_lookup_ub_ring_rpc_stale_and_conflict();
    test_vsim_key2_lookup_remote_meta_stale();
    test_vsim_key2_lookup_remote_owner_routing();
    test_shared_slot_meta_local_set_before_remote();
    test_runtime_attach_remote_region_after_create();
    test_migration_snapshot_apply_rejects_stale();
    test_put_with_epoch_rejects_stale_epoch();
    test_migration_source_cutover_rejects_old_owner_access();
    test_migration_delta_rpc_apply_idempotent_and_tombstone();
    test_migration_delta_ub_ring_rpc_put();
    test_migration_snapshot_ub_ring_rpc_descriptor();
    printf("vemb_v16_tlc_ut: all tests passed\n");
    return 0;
}
