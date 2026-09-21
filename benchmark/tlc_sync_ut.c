#include "tlc_core.h"
#include "vemb_v16_hash.h"
#include "zmalloc.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>

enum {
    VALUE_SIZE = 8,
    SLOT_COUNT = 32,
};

typedef struct test_core {
    tlc_core_t *core;
    uint8_t *region;
    vemb_v16_warm_slot_meta_t *slot_meta;
} test_core_t;

static void init_slot_meta(vemb_v16_warm_slot_meta_t *slot_meta,
                           uint32_t region_id) {
    for (uint32_t i = 0; i < SLOT_COUNT; i++) {
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

static void test_core_init(test_core_t *test, uint32_t region_id) {
    test->region = zcalloc(VALUE_SIZE * SLOT_COUNT);
    test->slot_meta = zcalloc_num(SLOT_COUNT, sizeof(*test->slot_meta));
    assert(test->region != NULL && test->slot_meta != NULL);
    init_slot_meta(test->slot_meta, region_id);

    tlc_core_warm_region_config_t warm = {
        .region_id = region_id,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .value_size = VALUE_SIZE,
        .region_bytes = VALUE_SIZE * SLOT_COUNT,
        .mapped_addr = test->region,
        .slot_meta = test->slot_meta,
    };
    tlc_core_config_t config = {
        .value_size = VALUE_SIZE,
        .warm_capacity = SLOT_COUNT,
        .hot_capacity = 16,
        .warm_regions = &warm,
        .warm_region_count = 1,
        .local_region_weight = 1,
    };
    assert(tlc_core_create(&test->core, &config) == 0);
}

static void test_core_free(test_core_t *test) {
    tlc_core_destroy(test->core);
    zfree(test->slot_meta);
    zfree(test->region);
}

static tlc_cold_event_input_t make_event(const char *key,
                                         const uint8_t *value,
                                         uint32_t op,
                                         uint64_t version,
                                         uint64_t topology_epoch) {
    uint32_t key_len = (uint32_t)strlen(key);
    uint64_t key_hash = vemb_v16_xxh3_64(key, key_len);
    return (tlc_cold_event_input_t){
        .ha_term = 1,
        .topology_epoch = topology_epoch,
        .op = op,
        .meta_shard_id = vemb_v16_mix32_u64(key_hash) & 255u,
        .version = version,
        .key = key,
        .key_len = key_len,
        .value = value,
        .value_len = op == TLC_COLD_OP_PUT ? VALUE_SIZE : 0,
    };
}

static void assert_value(tlc_core_t *core,
                         const char *key,
                         const uint8_t *expected) {
    uint32_t key_len = (uint32_t)strlen(key);
    uint64_t key_hash = vemb_v16_xxh3_64(key, key_len);
    tlc_warm_location_t location;
    uint8_t actual[VALUE_SIZE];
    assert(tlc_core_get_warm_location(core, key, key_len, key_hash,
                                      &location) == 0);
    assert(tlc_core_copy_warm_location_value(core, key_hash, &location,
                                             actual, sizeof(actual), 8) == 0);
    assert(memcmp(actual, expected, sizeof(actual)) == 0);
}

static void assert_meta(tlc_core_t *core,
                        const char *key,
                        uint64_t key_version,
                        uint32_t tombstone) {
    uint32_t key_len = (uint32_t)strlen(key);
    uint64_t key_hash = vemb_v16_xxh3_64(key, key_len);
    tlc_core_key_migration_info_t info;
    assert(tlc_core_get_migration_info(core, key, key_len, key_hash,
                                       &info) == 0);
    assert(info.key_version == key_version);
    assert(info.tombstone == tombstone);
}

static void test_replica_apply(void) {
    test_core_t leader = {0};
    test_core_t follower = {0};
    test_core_init(&leader, 1);
    test_core_init(&follower, 2);

    const char *key = "sync-key";
    const uint8_t value1[VALUE_SIZE] = {1, 2, 3, 4, 5, 6, 7, 8};
    const uint8_t value2[VALUE_SIZE] = {8, 7, 6, 5, 4, 3, 2, 1};
    uint64_t key_hash = vemb_v16_xxh3_64(key, strlen(key));
    uint32_t warm_slot;
    assert(tlc_core_put(leader.core, key, strlen(key), key_hash,
                        value1, sizeof(value1), &warm_slot) == 0);

    tlc_core_replica_apply_status_t status;
    tlc_cold_event_input_t event = make_event(key, value1,
                                              TLC_COLD_OP_PUT, 1, 1);
    assert(tlc_core_apply_replica_event(follower.core, &event, 1, &status) == 0);
    assert(status == TLC_CORE_REPLICA_APPLY_APPLIED);
    assert_value(follower.core, key, value1);
    assert_meta(follower.core, key, 1, 0);

    event = make_event(key, value2, TLC_COLD_OP_PUT, 2, 1);
    assert(tlc_core_apply_replica_event(follower.core, &event, 2, &status) == 0);
    assert(status == TLC_CORE_REPLICA_APPLY_APPLIED);
    assert_value(follower.core, key, value2);
    assert_meta(follower.core, key, 2, 0);

    event = make_event(key, value1, TLC_COLD_OP_PUT, 1, 1);
    assert(tlc_core_apply_replica_event(follower.core, &event, 2, &status) == 0);
    assert(status == TLC_CORE_REPLICA_APPLY_DUPLICATE);
    assert_value(follower.core, key, value2);

    event = make_event("gap-key", value1, TLC_COLD_OP_PUT, 1, 1);
    assert(tlc_core_apply_replica_event(follower.core, &event, 4, &status) == 0);
    assert(status == TLC_CORE_REPLICA_APPLY_GAP);

    event = make_event(key, NULL, TLC_COLD_OP_DEL, 3, 1);
    assert(tlc_core_apply_replica_event(follower.core, &event, 3, &status) == 0);
    assert(status == TLC_CORE_REPLICA_APPLY_APPLIED);
    assert_meta(follower.core, key, 3, 1);
    tlc_warm_location_t location;
    assert(tlc_core_get_warm_location(follower.core, key, strlen(key),
                                      key_hash, &location) != 0);

    event = make_event(key, value1, TLC_COLD_OP_PUT, 4, 2);
    assert(tlc_core_apply_replica_event(follower.core, &event, 4, &status) == 0);
    assert(status == TLC_CORE_REPLICA_APPLY_APPLIED);
    assert_meta(follower.core, key, 4, 0);
    event = make_event(key, value2, TLC_COLD_OP_PUT, 5, 1);
    assert(tlc_core_apply_replica_event(follower.core, &event, 5, &status) == 0);
    assert(status == TLC_CORE_REPLICA_APPLY_STALE);
    assert_value(follower.core, key, value1);

    test_core_free(&leader);
    test_core_free(&follower);
}

static void test_replica_state_isolated(void) {
    test_core_t first = {0};
    test_core_t second = {0};
    test_core_init(&first, 3);
    test_core_init(&second, 4);
    const uint8_t value[VALUE_SIZE] = {0};
    tlc_core_replica_apply_status_t status;
    tlc_cold_event_input_t event = make_event("group-key", value,
                                              TLC_COLD_OP_PUT, 1, 1);
    assert(tlc_core_apply_replica_event(first.core, &event, 2, &status) == 0);
    assert(status == TLC_CORE_REPLICA_APPLY_GAP);
    assert(tlc_core_apply_replica_event(second.core, &event, 1, &status) == 0);
    assert(status == TLC_CORE_REPLICA_APPLY_APPLIED);
    assert_value(second.core, "group-key", value);
    test_core_free(&first);
    test_core_free(&second);
}

int main(void) {
    test_replica_apply();
    test_replica_state_isolated();
    return 0;
}
