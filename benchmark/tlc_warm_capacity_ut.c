#include "tlc_core.h"
#include "vemb_v16_hash.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void init_slots(vemb_v16_warm_slot_meta_t *slots,
                       uint32_t count,
                       uint32_t region_id) {
    memset(slots, 0, sizeof(*slots) * count);
    for (uint32_t i = 0; i < count; i++) {
        (void)region_id;
        (void)i;
        atomic_init(&slots[i].state_version,
                    vemb_v16_warm_slot_pack(0, VEMB_V16_WARM_SLOT_FREE));
        atomic_init(&slots[i].owner_generation, 0);
    }
}

static void test_arbitrary_slot_lookup(void) {
    enum { SLOT_COUNT = 4096 };
    uint64_t *payload = calloc(SLOT_COUNT, sizeof(*payload));
    vemb_v16_warm_slot_meta_t *meta = calloc(SLOT_COUNT, sizeof(*meta));
    assert(payload && meta);
    init_slots(meta, SLOT_COUNT, 30);

    tlc_core_warm_region_config_t region = {
        .region_id = 30,
        .is_local = 1,
        .weight = 1,
        .value_size = sizeof(*payload),
        .region_bytes = SLOT_COUNT * sizeof(*payload),
        .mapped_addr = (uint8_t *)payload,
        .slot_meta = meta,
    };
    tlc_core_config_t config = {
        .value_size = sizeof(*payload),
        .hot_capacity = 1,
        .warm_regions = &region,
        .warm_region_count = 1,
        .local_region_weight = 1,
    };
    tlc_core_t *core = NULL;
    assert(tlc_core_create(&core, &config) == 0);

    for (uint32_t i = 0; i < SLOT_COUNT; i++) {
        char key[32];
        uint64_t value = i + 1u;
        int key_len = snprintf(key, sizeof(key), "index:%u", i);
        assert(key_len > 0 && (size_t)key_len < sizeof(key));
        assert(tlc_core_put(core, key, (uint32_t)key_len,
                            vemb_v16_xxh3_64_str(key, (size_t)key_len),
                            &value, sizeof(value), NULL) == 0);
    }
    for (uint32_t i = 0; i < SLOT_COUNT; i++) {
        char key[32];
        tlc_warm_location_t location;
        int key_len = snprintf(key, sizeof(key), "index:%u", i);
        assert(key_len > 0 && (size_t)key_len < sizeof(key));
        assert(tlc_core_get_warm_location(
                   core, key, (uint32_t)key_len,
                   vemb_v16_xxh3_64_str(key, (size_t)key_len),
                   &location) == 0);
    }

    tlc_core_destroy(core);
    free(meta);
    free(payload);
}

int main(void) {
    uint64_t local_a[2] = {0};
    uint64_t local_b[3] = {0};
    uint64_t remote[8] = {0};
    vemb_v16_warm_slot_meta_t meta_a[2];
    vemb_v16_warm_slot_meta_t meta_b[3];
    vemb_v16_warm_slot_meta_t meta_remote[8];
    init_slots(meta_a, 2, 10);
    init_slots(meta_b, 3, 11);
    init_slots(meta_remote, 8, 20);
    tlc_core_warm_region_config_t regions[] = {
        {.region_id = 10, .is_local = 1, .weight = 1,
         .value_size = sizeof(uint64_t), .region_bytes = sizeof(local_a),
         .mapped_addr = (uint8_t *)local_a, .slot_meta = meta_a},
        {.region_id = 11, .is_local = 1, .weight = 1,
         .value_size = sizeof(uint64_t), .region_bytes = sizeof(local_b),
         .mapped_addr = (uint8_t *)local_b, .slot_meta = meta_b},
        {.region_id = 20, .is_local = 0, .weight = 1,
         .value_size = sizeof(uint64_t), .region_bytes = sizeof(remote),
         .mapped_addr = (uint8_t *)remote, .slot_meta = meta_remote},
    };
    tlc_core_config_t config = {
        .value_size = sizeof(uint64_t),
        .warm_capacity = 1, /* Deliberately ignored; capacity is derived. */
        .hot_capacity = 64,
        .warm_regions = regions,
        .warm_region_count = 3,
        .local_region_weight = 1,
    };
    tlc_core_t *core = NULL;
    assert(tlc_core_create(&core, &config) == 0);
    tlc_core_stats_t stats;
    tlc_core_get_stats(core, &stats);
    assert(stats.warm_local_capacity_slots == 5);
    assert(stats.warm_allocated_slots == 0);
    assert(stats.warm_free_slots == 5);
    for (uint32_t i = 0; i < 5; i++) {
        char key[32];
        uint64_t value = i + 1;
        snprintf(key, sizeof(key), "capacity:%u", i);
        assert(tlc_core_put(core, key, (uint32_t)strlen(key),
                            vemb_v16_xxh3_64_str(key, strlen(key)),
                            &value, sizeof(value), NULL) == 0);
        tlc_core_get_stats(core, &stats);
        assert(stats.warm_allocated_slots == i + 1u);
        assert(stats.warm_free_slots == 4u - i);
    }
    char full_key[] = "capacity:full";
    uint64_t full_value = 99;
    assert(tlc_core_put(core, full_key, sizeof(full_key) - 1,
                        vemb_v16_xxh3_64_str(full_key, sizeof(full_key) - 1),
                        &full_value, sizeof(full_value), NULL) != 0);
    for (uint32_t i = 0; i < 8; i++)
        assert(vemb_v16_warm_slot_state(atomic_load_explicit(
                   &meta_remote[i].state_version, memory_order_acquire)) ==
               VEMB_V16_WARM_SLOT_FREE);

    char deleted_key[] = "capacity:1";
    assert(tlc_core_delete_with_epoch(
               core, deleted_key, sizeof(deleted_key) - 1,
               vemb_v16_xxh3_64_str(deleted_key, sizeof(deleted_key) - 1),
               0, NULL) == 0);
    assert(tlc_core_put(core, full_key, sizeof(full_key) - 1,
                        vemb_v16_xxh3_64_str(full_key, sizeof(full_key) - 1),
                        &full_value, sizeof(full_value), NULL) == 0);
    tlc_core_get_stats(core, &stats);
    assert(stats.warm_local_capacity_slots == 5);
    assert(stats.warm_free_slots == 0);
    assert(stats.warm_alloc_oom >= 1);
    tlc_core_destroy(core);

    /* Attach rebuild must treat every READY local slot as occupied. */
    assert(tlc_core_create(&core, &config) == 0);
    tlc_core_get_stats(core, &stats);
    assert(stats.warm_allocated_slots == 5);
    assert(stats.warm_free_slots == 0);
    char restart_key[] = "capacity:restart";
    assert(tlc_core_put(core, restart_key, sizeof(restart_key) - 1,
                        vemb_v16_xxh3_64_str(restart_key,
                                            sizeof(restart_key) - 1),
                        &full_value, sizeof(full_value), NULL) != 0);
    tlc_core_destroy(core);

    /* A FREE slot discovered during rebuild must be reusable exactly once. */
    uint64_t state_version = atomic_load_explicit(
        &meta_a[0].state_version, memory_order_acquire);
    atomic_store_explicit(
        &meta_a[0].state_version,
        vemb_v16_warm_slot_pack(vemb_v16_warm_slot_seq(state_version) + 2u,
                                VEMB_V16_WARM_SLOT_FREE),
        memory_order_release);
    assert(tlc_core_create(&core, &config) == 0);
    tlc_core_get_stats(core, &stats);
    assert(stats.warm_allocated_slots == 4);
    assert(stats.warm_free_slots == 1);
    assert(tlc_core_put(core, restart_key, sizeof(restart_key) - 1,
                        vemb_v16_xxh3_64_str(restart_key,
                                            sizeof(restart_key) - 1),
                        &full_value, sizeof(full_value), NULL) == 0);
    tlc_core_get_stats(core, &stats);
    assert(stats.warm_allocated_slots == 5);
    assert(stats.warm_free_slots == 0);
    tlc_core_destroy(core);
    test_arbitrary_slot_lookup();
    puts("tlc warm local capacity: PASS");
    return 0;
}
