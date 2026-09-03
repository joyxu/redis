#include "tlc_core.h"
#include "monotonic.h"
#include "vemb_v16_hash.h"
#include "zmalloc.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

enum { VALUE_SIZE = 8, SLOT_COUNT = 32, META_SHARD_COUNT = 256 };

typedef struct test_node {
    tlc_core_t *core;
    uint8_t *region;
    vemb_v16_warm_slot_meta_t *slot_meta;
} test_node_t;

static void node_init(test_node_t *node, uint32_t region_id,
                      const char *directory) {
    node->region = zcalloc(VALUE_SIZE * SLOT_COUNT);
    node->slot_meta = zcalloc_num(SLOT_COUNT, sizeof(*node->slot_meta));
    assert(node->region && node->slot_meta);
    for (uint32_t i = 0; i < SLOT_COUNT; i++) {
        atomic_init(&node->slot_meta[i].state, VEMB_V16_WARM_SLOT_FREE);
        atomic_init(&node->slot_meta[i].owner_generation, 0);
        atomic_init(&node->slot_meta[i].write_seq, 0);
        atomic_init(&node->slot_meta[i].last_access_ns, 0);
        atomic_init(&node->slot_meta[i].clock_bit, 0);
        atomic_init(&node->slot_meta[i].cold_state,
                    VEMB_V16_WARM_SLOT_COLD_NONE);
        node->slot_meta[i].region_id = region_id;
        node->slot_meta[i].local_slot = i;
    }
    tlc_core_warm_region_config_t warm = {
        .region_id = region_id,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .value_size = VALUE_SIZE,
        .region_bytes = VALUE_SIZE * SLOT_COUNT,
        .mapped_addr = node->region,
        .slot_meta = node->slot_meta,
    };
    tlc_core_config_t core_config = {
        .value_size = VALUE_SIZE,
        .warm_capacity = SLOT_COUNT,
        .hot_capacity = 16,
        .warm_regions = &warm,
        .warm_region_count = 1,
        .local_region_weight = 1,
    };
    assert(tlc_core_create(&node->core, &core_config) == 0);
    tlc_cold_config_t cold_config = {
        .directory = directory,
        .segment_bytes = 4096,
        .queue_capacity = 16,
        .group_max_entries = 8,
        .group_max_delay_us = 1000,
    };
    assert(tlc_core_enable_cold(node->core, &cold_config) == 0);
}

static void node_free(test_node_t *node) {
    tlc_core_destroy(node->core);
    zfree(node->slot_meta);
    zfree(node->region);
}

static void put_value(tlc_core_t *core, const char *key, uint8_t seed) {
    uint8_t value[VALUE_SIZE];
    for (uint32_t i = 0; i < VALUE_SIZE; i++)
        value[i] = (uint8_t)(seed + i);
    uint64_t hash = vemb_v16_xxh3_64(key, strlen(key));
    uint32_t slot;
    assert(tlc_core_put(core, key, strlen(key), hash, value, sizeof(value),
                        &slot) == 0);
}

static void assert_value(tlc_core_t *core, const char *key, uint8_t seed) {
    uint8_t expected[VALUE_SIZE];
    for (uint32_t i = 0; i < VALUE_SIZE; i++)
        expected[i] = (uint8_t)(seed + i);
    uint64_t hash = vemb_v16_xxh3_64(key, strlen(key));
    tlc_warm_location_t location;
    assert(tlc_core_get_warm_location(core, key, strlen(key), hash,
                                      &location) == 0);
    uint8_t actual[VALUE_SIZE];
    assert(tlc_core_copy_warm_location_value(core, hash, &location, actual,
                                             sizeof(actual), 8) == 0);
    assert(memcmp(actual, expected, sizeof(actual)) == 0);
}

static void wait_durable(tlc_core_t *core, uint64_t expected) {
    tlc_cold_t *cold = tlc_core_get_cold(core);
    tlc_cold_progress_t progress;
    for (uint32_t i = 0; i < 3000; i++) {
        assert(tlc_cold_get_progress(cold, &progress) == 0);
        if (progress.durable_seq >= expected)
            return;
        usleep(1000);
    }
    assert(0);
}

int main(void) {
    assert(monotonicInit() != NULL);
    char leader_dir[] = "/tmp/tlc-resync-leader-XXXXXX";
    char follower_dir[] = "/tmp/tlc-resync-follower-XXXXXX";
    assert(mkdtemp(leader_dir) && mkdtemp(follower_dir));
    test_node_t leader = {0};
    test_node_t follower = {0};
    node_init(&leader, 11, leader_dir);
    node_init(&follower, 12, follower_dir);
    put_value(leader.core, "resync-key-1", 1);
    put_value(leader.core, "resync-key-2", 11);
    put_value(leader.core, "resync-key-3", 21);
    wait_durable(leader.core, 3);
    tlc_cold_checkpoint_result_t checkpoint;
    assert(tlc_core_publish_checkpoint(leader.core, 1, 7, &checkpoint) == 0);
    assert(checkpoint.checkpoint_seq == 3);

    void *checkpoint_blob = NULL;
    size_t checkpoint_blob_bytes = 0;
    tlc_cold_checkpoint_result_t exported;
    assert(tlc_cold_export_checkpoint(tlc_core_get_cold(leader.core), META_SHARD_COUNT,
                                      &checkpoint_blob, &checkpoint_blob_bytes,
                                      &exported) == 0);
    assert(checkpoint_blob != NULL && checkpoint_blob_bytes > 0);
    assert(exported.generation == checkpoint.generation);
    assert(exported.checkpoint_seq == checkpoint.checkpoint_seq);

    char import_dir[] = "/tmp/tlc-resync-import-XXXXXX";
    assert(mkdtemp(import_dir));
    test_node_t imported = {0};
    node_init(&imported, 13, import_dir);
    tlc_cold_checkpoint_result_t imported_result;
    assert(tlc_cold_import_checkpoint(tlc_core_get_cold(imported.core), META_SHARD_COUNT,
                                      checkpoint_blob, checkpoint_blob_bytes,
                                      &imported_result) == 0);
    assert(imported_result.generation == exported.generation);
    assert(imported_result.checkpoint_seq == exported.checkpoint_seq);
    assert(tlc_core_recover_cold(imported.core) == 0);
    assert_value(imported.core, "resync-key-1", 1);
    assert_value(imported.core, "resync-key-2", 11);
    assert_value(imported.core, "resync-key-3", 21);

    void *corrupt_blob = zmalloc(checkpoint_blob_bytes);
    assert(corrupt_blob != NULL);
    memcpy(corrupt_blob, checkpoint_blob, checkpoint_blob_bytes);
    ((uint8_t *)corrupt_blob)[checkpoint_blob_bytes - 1] ^= 0x1;
    assert(tlc_cold_import_checkpoint(tlc_core_get_cold(imported.core), META_SHARD_COUNT,
                                      corrupt_blob, checkpoint_blob_bytes,
                                      &imported_result) != 0);
    assert(tlc_cold_import_checkpoint(tlc_core_get_cold(imported.core), META_SHARD_COUNT,
                                      checkpoint_blob, checkpoint_blob_bytes,
                                      &imported_result) != 0);
    assert(tlc_cold_validate_checkpoint(tlc_core_get_cold(imported.core), 0, META_SHARD_COUNT,
                                        &imported_result) == 0);
    zfree(corrupt_blob);
    tlc_cold_free_checkpoint_blob(checkpoint_blob);

    assert(tlc_cold_compact(tlc_core_get_cold(leader.core),
                            checkpoint.checkpoint_seq, UINT64_MAX, 1) == 0);
    assert(tlc_cold_export_checkpoint(tlc_core_get_cold(leader.core), META_SHARD_COUNT,
                                      &checkpoint_blob, &checkpoint_blob_bytes,
                                      &exported) == 0);
    tlc_cold_free_checkpoint_blob(checkpoint_blob);

    put_value(leader.core, "resync-key-4", 31);
    wait_durable(leader.core, 4);
    tlc_cold_checkpoint_result_t result;
    assert(tlc_core_resync_from(leader.core, follower.core, 4, &result) == 0);
    assert(result.generation == checkpoint.generation);
    assert(result.checkpoint_seq == checkpoint.checkpoint_seq);
    assert_value(follower.core, "resync-key-1", 1);
    assert_value(follower.core, "resync-key-2", 11);
    assert_value(follower.core, "resync-key-3", 21);
    assert_value(follower.core, "resync-key-4", 31);
    tlc_cold_progress_t progress;
    assert(tlc_cold_get_progress(tlc_core_get_cold(follower.core),
                                 &progress) == 0);
    assert(progress.durable_seq == 4);
    assert(tlc_core_resync_from(leader.core, follower.core, 5,
                                &result) != 0);
    node_free(&imported);
    node_free(&leader);
    node_free(&follower);
    printf("tlc_resync_ut: PASS\n");
    return 0;
}
