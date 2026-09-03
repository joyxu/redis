#include "../src/tlc_ha_replica.h"
#include "monotonic.h"
#include "vemb_v16_hash.h"
#include "zmalloc.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

enum { VALUE_SIZE = 8, SLOT_COUNT = 32 };

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
        .region_id = region_id, .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1, .weight = 1, .value_size = VALUE_SIZE,
        .region_bytes = VALUE_SIZE * SLOT_COUNT, .mapped_addr = node->region,
        .slot_meta = node->slot_meta,
    };
    tlc_core_config_t core_config = {
        .value_size = VALUE_SIZE, .warm_capacity = SLOT_COUNT,
        .hot_capacity = 16, .warm_regions = &warm, .warm_region_count = 1,
        .local_region_weight = 1,
    };
    assert(tlc_core_create(&node->core, &core_config) == 0);
    tlc_cold_config_t cold_config = {
        .directory = directory, .segment_bytes = 4096, .queue_capacity = 16,
        .group_max_entries = 4, .group_max_delay_us = 1000,
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

static int value_matches(tlc_core_t *core, const char *key, uint8_t seed) {
    uint8_t expected[VALUE_SIZE];
    for (uint32_t i = 0; i < VALUE_SIZE; i++)
        expected[i] = (uint8_t)(seed + i);
    uint64_t hash = vemb_v16_xxh3_64(key, strlen(key));
    tlc_warm_location_t location;
    if (tlc_core_get_warm_location(core, key, strlen(key), hash,
                                   &location) != 0)
        return 0;
    uint8_t actual[VALUE_SIZE];
    return tlc_core_copy_warm_location_value(core, hash, &location, actual,
                                             sizeof(actual), 8) == 0 &&
           memcmp(actual, expected, sizeof(actual)) == 0;
}

static int value_absent(tlc_core_t *core, const char *key) {
    uint64_t hash = vemb_v16_xxh3_64(key, strlen(key));
    tlc_warm_location_t location;
    return tlc_core_get_warm_location(core, key, strlen(key), hash,
                                      &location) != 0;
}

static uint8_t final_seed(uint32_t key_index, uint32_t expected_events) {
    assert(key_index < 4 && expected_events >= 6);
    uint8_t seed = (uint8_t)(7 + key_index);
    uint32_t update_events = expected_events - 6;
    for (uint32_t i = 0; i < update_events; i++)
        if (i % 4 == key_index)
            seed = (uint8_t)(100 + key_index);
    return seed;
}

static tlc_ha_replica_ring_config_t ring_config(const char *path,
                                                uint64_t offset) {
    tlc_ha_replica_ring_config_t config = {
        .backend_type = VEMB_V16_REGION_UB,
        /* Replica frames span multiple cachelines; publish from an NC view
         * so the remote import observes the payload before its sequence. */
        .cache_policy = VEMB_V16_UB_CACHE_POLICY_NONCACHEABLE,
        .mmap_offset = offset,
        .slot_count = 8,
        .slot_bytes = 131072,
    };
    snprintf(config.path, sizeof(config.path), "%s", path);
    return config;
}

int main(int argc, char **argv) {
    assert(monotonicInit() != NULL);
    assert(argc == 2 && (strcmp(argv[1], "leader") == 0 ||
                         strcmp(argv[1], "leader-replay") == 0 ||
                         strcmp(argv[1], "follower") == 0 ||
                         strcmp(argv[1], "follower-recover") == 0));
    int leader = strcmp(argv[1], "leader") == 0;
    int leader_replay = strcmp(argv[1], "leader-replay") == 0;
    int recovery_only = strcmp(argv[1], "follower-recover") == 0;
    const char *path = getenv("TLC_HA_UB_PATH");
    const char *tx_path = getenv("TLC_HA_UB_TX_PATH");
    const char *rx_path = getenv("TLC_HA_UB_RX_PATH");
    const char *tx_text = getenv("TLC_HA_UB_TX_OFFSET");
    const char *rx_text = getenv("TLC_HA_UB_RX_OFFSET");
    const char *cold_directory = getenv("TLC_HA_UB_COLD_DIR");
    assert((recovery_only || ((tx_path || path) && (rx_path || path) &&
                             tx_text && rx_text)));
    if (recovery_only)
        assert(cold_directory && *cold_directory);
    uint64_t tx_offset = tx_text ? strtoull(tx_text, NULL, 0) : 0;
    uint64_t rx_offset = rx_text ? strtoull(rx_text, NULL, 0) : 0;
    const char *effective_tx_path = tx_path ? tx_path : path;
    const char *effective_rx_path = rx_path ? rx_path : path;
    if (getenv("TLC_HA_UB_RESET_ONLY")) {
        tlc_ha_replica_ring_config_t reset_tx = ring_config(effective_tx_path,
                                                            tx_offset);
        tlc_ha_replica_ring_config_t reset_rx = ring_config(effective_rx_path,
                                                            rx_offset);
        assert(tlc_ha_replica_reset_ring(&reset_tx) == 0);
        assert(tlc_ha_replica_reset_ring(&reset_rx) == 0);
        return 0;
    }
    tlc_ha_replica_ring_config_t tx = {0};
    tlc_ha_replica_ring_config_t rx = {0};
    if (!recovery_only) {
        tx = ring_config(effective_tx_path, tx_offset);
        rx = ring_config(effective_rx_path, rx_offset);
    }
    if (getenv("TLC_HA_UB_RESET")) {
        assert(tlc_ha_replica_reset_ring(&tx) == 0);
        assert(tlc_ha_replica_reset_ring(&rx) == 0);
    }

    char directory_template[] = "/tmp/tlc-ha-ub-node-XXXXXX";
    char *directory = (char *)cold_directory;
    if (!directory) {
        directory = mkdtemp(directory_template);
        assert(directory != NULL);
    }
    test_node_t node = {0};
    node_init(&node, (leader || leader_replay) ? 111 : 112, directory);
    if (recovery_only) {
        uint32_t expected_events =
            getenv("TLC_HA_UB_EXPECTED_EVENTS") ?
            (uint32_t)strtoul(getenv("TLC_HA_UB_EXPECTED_EVENTS"), NULL, 0) :
            10000;
        tlc_cold_progress_t progress = {0};
        assert(tlc_cold_get_progress(tlc_core_get_cold(node.core),
                                      &progress) == 0);
        assert(expected_events >= 6);
        int recovered = value_matches(node.core, "ub-machine-key-1",
                                      final_seed(0, expected_events)) &&
                        value_matches(node.core, "ub-machine-key-2",
                                      final_seed(1, expected_events)) &&
                        value_absent(node.core, "ub-machine-key-3") &&
                        value_absent(node.core, "ub-machine-key-4") &&
                        progress.durable_seq >= expected_events;
        printf("tlc_ha_replica_ub_node_ut: follower recovery %s dir=%s durable=%llu\n",
               recovered ? "PASS" : "FAIL", directory,
               (unsigned long long)progress.durable_seq);
        node_free(&node);
        return recovered ? 0 : 1;
    }
    tlc_ha_replica_config_t config = {
        .core = node.core,
        .cold = tlc_core_get_cold(node.core),
        .fd = -1,
        .role = (leader || leader_replay) ? TLC_HA_REPLICA_LEADER :
                                           TLC_HA_REPLICA_FOLLOWER,
        .transport = TLC_HA_REPLICA_TRANSPORT_UB,
        .tx_ring = tx,
        .rx_ring = rx,
        .queue_capacity = 32,
        .max_batch_events = 8,
        .max_batch_bytes = 65536,
        .hpc_node_id = (leader || leader_replay) ? 111 : 112,
        .peer_node_id = (leader || leader_replay) ? 112 : 111,
        .ha_term = 9,
        .heartbeat_interval_ms = 20,
        .heartbeat_timeout_ms = 200,
    };
    tlc_ha_replica_t *replica = NULL;
    assert(tlc_ha_replica_start(&replica, &config) == 0);
    const uint32_t expected_events =
        getenv("TLC_HA_UB_EXPECTED_EVENTS") ?
        (uint32_t)strtoul(getenv("TLC_HA_UB_EXPECTED_EVENTS"), NULL, 0) :
        10000;
    if (leader_replay) {
        const char *replay_start_text = getenv("TLC_HA_UB_REPLAY_START");
        const char *replay_end_text = getenv("TLC_HA_UB_REPLAY_END");
        assert(replay_start_text && replay_end_text);
        uint64_t replay_start = strtoull(replay_start_text, NULL, 0);
        uint64_t replay_end = strtoull(replay_end_text, NULL, 0);
        assert(replay_start != 0 && replay_end >= replay_start);
        assert(tlc_ha_replica_replay_from(replica, replay_start,
                                          replay_end) == 0);
        for (uint32_t i = 0; i < 10000 &&
             tlc_ha_replica_peer_accepted_seq(replica) < replay_end; i++)
            usleep(1000);
        assert(tlc_ha_replica_peer_accepted_seq(replica) >= replay_end);
        printf("tlc_ha_replica_ub_node_ut: leader replay PASS start=%llu end=%llu\n",
               (unsigned long long)replay_start,
               (unsigned long long)replay_end);
        usleep(100000);
    } else if (leader) {
        static const char *keys[] = {
            "ub-machine-key-1", "ub-machine-key-2",
            "ub-machine-key-3", "ub-machine-key-4",
        };
        for (uint32_t i = 0; i < 4; i++)
            put_value(node.core, keys[i], (uint8_t)(7 + i));
        assert(expected_events >= 6);
        const uint32_t update_events = expected_events - 6;
        for (uint32_t i = 0; i < update_events; i++)
            put_value(node.core, keys[i % 4], (uint8_t)(100 + (i % 4)));
        tlc_core_key_migration_info_t delete_info = {0};
        for (uint32_t i = 2; i < 4; i++) {
            uint64_t hash = vemb_v16_xxh3_64(keys[i], strlen(keys[i]));
            assert(tlc_core_delete_with_epoch(node.core,
                                               keys[i],
                                               strlen(keys[i]),
                                               hash,
                                               0,
                                               &delete_info) == 0);
        }
        for (uint32_t i = 0; i < 60000 &&
             tlc_ha_replica_peer_accepted_seq(replica) < expected_events; i++)
            usleep(1000);
        assert(tlc_ha_replica_peer_accepted_seq(replica) >= expected_events);
        for (uint32_t i = 0; i < 60000 &&
             tlc_ha_replica_peer_health(replica) != TLC_HA_REPLICA_HEALTHY; i++)
            usleep(1000);
        assert(tlc_ha_replica_peer_health(replica) == TLC_HA_REPLICA_HEALTHY);
        printf("tlc_ha_replica_ub_node_ut: leader PASS events=%u accepted=%llu\n",
               expected_events,
               (unsigned long long)tlc_ha_replica_peer_accepted_seq(replica));
        /* Keep the peer heartbeatable while the follower completes checks. */
        usleep(5000000);
    } else {
        int ready = 0;
        assert(expected_events >= 6);
        for (uint32_t i = 0; i < 60000; i++) {
            if (value_matches(node.core, "ub-machine-key-1",
                              final_seed(0, expected_events)) &&
                value_matches(node.core, "ub-machine-key-2",
                              final_seed(1, expected_events)) &&
                value_absent(node.core, "ub-machine-key-3") &&
                value_absent(node.core, "ub-machine-key-4")) {
                ready = 1;
                break;
            }
            usleep(1000);
        }
        assert(ready);
        for (uint32_t i = 0; i < 60000 &&
             tlc_ha_replica_peer_health(replica) != TLC_HA_REPLICA_HEALTHY; i++)
            usleep(1000);
        assert(tlc_ha_replica_peer_health(replica) == TLC_HA_REPLICA_HEALTHY);
        printf("tlc_ha_replica_ub_node_ut: follower PASS applied\n");
        /* Leave the receiver alive briefly so a restarted Leader can issue
         * an explicit replay range after recovery. */
        usleep(500000);
    }
    tlc_ha_replica_stop(replica);
    node_free(&node);
    return 0;
}
