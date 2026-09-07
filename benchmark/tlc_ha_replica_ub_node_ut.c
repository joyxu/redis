#include "../src/tlc_ha_replica.h"
#include "monotonic.h"
#include "vemb_v16_hash.h"
#include "zmalloc.h"

#include <assert.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <stdint.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

enum { VALUE_SIZE = 8, SLOT_COUNT = 32 };

typedef struct test_node {
    tlc_core_t *core;
    uint8_t *region;
    vemb_v16_warm_slot_meta_t *slot_meta;
} test_node_t;

static void node_init(test_node_t *node, uint32_t region_id,
                      const char *directory, uint64_t retention_events) {
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
        .retention_events = retention_events,
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

static int wait_for_value(tlc_core_t *core, const char *key, uint8_t seed) {
    for (uint32_t i = 0; i < 60000; i++) {
        if (value_matches(core, key, seed))
            return 1;
        usleep(1000);
    }
    return 0;
}

static int wait_for_durable(tlc_cold_t *cold, uint64_t expected) {
    for (uint32_t i = 0; i < 60000; i++) {
        tlc_cold_progress_t progress;
        if (tlc_cold_get_progress(cold, &progress) == 0 &&
            progress.durable_seq >= expected)
            return 1;
        usleep(1000);
    }
    return 0;
}

static int wait_for_retention_compact(tlc_cold_t *cold,
                                      uint64_t target_events) {
    for (uint32_t i = 0; i < 60000; i++) {
        tlc_cold_retention_window_t window;
        if (tlc_cold_get_retention_window(cold, &window) == 0 &&
            window.appended_seq >= target_events &&
            (window.retained_floor_seq == 0 ||
             window.appended_seq - window.retained_floor_seq + 1u <
                 target_events))
            return 1;
        usleep(1000);
    }
    return 0;
}

static int wait_for_retention_floor(tlc_cold_t *cold, uint64_t floor,
                                    uint64_t checkpoint_seq) {
    for (uint32_t i = 0; i < 60000; i++) {
        tlc_cold_retention_window_t window;
        tlc_cold_checkpoint_result_t checkpoint;
        if (tlc_cold_get_retention_window(cold, &window) == 0 &&
            window.retained_floor_seq == floor &&
            tlc_cold_validate_checkpoint(cold, 0, 256, &checkpoint) == 0 &&
            checkpoint.checkpoint_seq >= checkpoint_seq)
            return 1;
        usleep(1000);
    }
    return 0;
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
                         strcmp(argv[1], "leader-snapshot") == 0 ||
                         strcmp(argv[1], "leader-install") == 0 ||
                         strcmp(argv[1], "leader-m5") == 0 ||
                         strcmp(argv[1], "leader-m5-abort") == 0 ||
                         strcmp(argv[1], "leader-m6-gap") == 0 ||
                         strcmp(argv[1], "leader-m6-retention") == 0 ||
                         strcmp(argv[1], "leader-m7-retention") == 0 ||
                         strcmp(argv[1], "leader-m7-pressure") == 0 ||
                         strcmp(argv[1], "follower") == 0 ||
                         strcmp(argv[1], "follower-snapshot") == 0 ||
                         strcmp(argv[1], "follower-install") == 0 ||
                         strcmp(argv[1], "follower-m5") == 0 ||
                         strcmp(argv[1], "follower-m5-abort") == 0 ||
                         strcmp(argv[1], "follower-m6-gap") == 0 ||
                         strcmp(argv[1], "follower-m6-retention") == 0 ||
                         strcmp(argv[1], "follower-m7-retention") == 0 ||
                         strcmp(argv[1], "follower-m7-pressure-stall") == 0 ||
                         strcmp(argv[1], "follower-recover") == 0));
    int leader = strcmp(argv[1], "leader") == 0;
    int leader_replay = strcmp(argv[1], "leader-replay") == 0;
    int leader_snapshot = strcmp(argv[1], "leader-snapshot") == 0;
    int leader_install = strcmp(argv[1], "leader-install") == 0;
    int leader_m5 = strcmp(argv[1], "leader-m5") == 0;
    int leader_m5_abort = strcmp(argv[1], "leader-m5-abort") == 0;
    int leader_m6_gap = strcmp(argv[1], "leader-m6-gap") == 0;
    int leader_m6_retention = strcmp(argv[1], "leader-m6-retention") == 0;
    int leader_m7_retention = strcmp(argv[1], "leader-m7-retention") == 0;
    int leader_m7_pressure = strcmp(argv[1], "leader-m7-pressure") == 0;
    int follower_snapshot = strcmp(argv[1], "follower-snapshot") == 0;
    int follower_install = strcmp(argv[1], "follower-install") == 0;
    int follower_m5 = strcmp(argv[1], "follower-m5") == 0;
    int follower_m5_abort = strcmp(argv[1], "follower-m5-abort") == 0;
    int follower_m6_gap = strcmp(argv[1], "follower-m6-gap") == 0;
    int follower_m6_retention = strcmp(argv[1], "follower-m6-retention") == 0;
    int follower_m7_retention = strcmp(argv[1], "follower-m7-retention") == 0;
    int follower_m7_pressure_stall =
        strcmp(argv[1], "follower-m7-pressure-stall") == 0;
    int is_leader = leader || leader_replay || leader_snapshot ||
                    leader_install || leader_m5 || leader_m5_abort ||
                    leader_m6_gap || leader_m6_retention ||
                    leader_m7_retention || leader_m7_pressure;
    int recovery_only = strcmp(argv[1], "follower-recover") == 0;
    const char *path = getenv("TLC_HA_UB_PATH");
    const char *tx_path = getenv("TLC_HA_UB_TX_PATH");
    const char *rx_path = getenv("TLC_HA_UB_RX_PATH");
    const char *tx_text = getenv("TLC_HA_UB_TX_OFFSET");
    const char *rx_text = getenv("TLC_HA_UB_RX_OFFSET");
    const char *cold_directory = getenv("TLC_HA_UB_COLD_DIR");
    uint64_t retention_events = getenv("TLC_HA_UB_RETENTION_EVENTS") ?
        strtoull(getenv("TLC_HA_UB_RETENTION_EVENTS"), NULL, 0) : 0;
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
    node_init(&node, is_leader ? 111 : 112,
              directory, retention_events);
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
    const uint64_t ha_term = 9;
    if (leader_m6_gap || leader_m6_retention)
        assert(tlc_core_set_ha_term(node.core, ha_term) == 0);
    if (leader_m6_gap) {
        /* Persist seq 1 before the sender exists, then only emit seq 2.
         * Do not publish a checkpoint: M6 GAP recovery must use retained AOF. */
        put_value(node.core, "ub-m6-checkpoint", 0xa1);
        assert(wait_for_durable(tlc_core_get_cold(node.core), 1));
    }
    if (leader_m6_retention) {
        tlc_cold_t *cold = tlc_core_get_cold(node.core);
        for (uint32_t seq = 1; seq <= 96; seq++)
            put_value(node.core, "ub-m6-retention-key", (uint8_t)seq);
        assert(wait_for_durable(cold, 96));
        tlc_cold_checkpoint_result_t checkpoint;
        assert(tlc_core_publish_checkpoint(node.core, 96, ha_term, &checkpoint) == 0);
        put_value(node.core, "ub-m6-retention-key", 0x61);
        assert(wait_for_durable(cold, 97));
        assert(tlc_cold_compact(cold, checkpoint.checkpoint_seq,
                                UINT64_MAX, 1) == 0);
    }
    const char *bind_host = NULL;
    const char *peer_host = NULL;
    uint16_t control_port = 0;
    const char *control_port_text = getenv("TLC_HA_UB_CONTROL_PORT");
    if (control_port_text) {
        control_port = (uint16_t)strtoul(control_port_text, NULL, 0);
        assert(control_port != 0);
        bind_host = getenv("TLC_HA_UB_BIND_HOST");
        peer_host = getenv("TLC_HA_UB_PEER_HOST");
        assert(bind_host && *bind_host && peer_host && *peer_host);
    }
    tlc_ha_replica_config_t config = {
        .core = node.core,
        .cold = tlc_core_get_cold(node.core),
        .control_bind_host = bind_host,
        .control_bind_port = control_port,
        .peer_advertised_host = peer_host,
        .peer_control_port = control_port,
        .role = is_leader ?
            TLC_HA_REPLICA_LEADER : TLC_HA_REPLICA_FOLLOWER,
        .tx_ring = tx,
        .rx_ring = rx,
        .queue_capacity = 32,
        .max_batch_events = 8,
        .max_batch_bytes = 65536,
        .hpc_node_id = is_leader ? 111 : 112,
        .peer_node_id = is_leader ? 112 : 111,
        .ha_term = ha_term,
        .heartbeat_interval_ms = 20,
        .heartbeat_timeout_ms = 200,
    };
    if (leader_m5_abort || follower_m5_abort)
        config.resync_timeout_ms = 200;
    if (follower_m7_pressure_stall)
        config.max_resync_snapshot_bytes = 1;
    tlc_ha_replica_t *replica = NULL;
    assert(tlc_ha_replica_start(&replica, &config) == 0);
    const uint32_t expected_events =
        getenv("TLC_HA_UB_EXPECTED_EVENTS") ?
        (uint32_t)strtoul(getenv("TLC_HA_UB_EXPECTED_EVENTS"), NULL, 0) :
        10000;
    if (leader_install) {
        const char *key = "ub-resync-install-key";
        put_value(node.core, key, 0x51);
        tlc_cold_progress_t progress;
        for (uint32_t i = 0; i < 60000; i++) {
            assert(tlc_cold_get_progress(tlc_core_get_cold(node.core),
                                         &progress) == 0);
            if (progress.durable_seq >= 1)
                break;
            usleep(1000);
        }
        assert(progress.durable_seq >= 1);
        tlc_cold_checkpoint_result_t checkpoint;
        assert(tlc_core_publish_checkpoint(node.core, 1, 9, &checkpoint) == 0);
        tlc_cold_resync_snapshot_t snapshot;
        assert(tlc_cold_begin_resync_snapshot(tlc_core_get_cold(node.core), 256,
                                              &snapshot) == 0);
        assert(tlc_ha_replica_send_resync_snapshot(replica, 6001, 5,
                                                    &snapshot, 32768) == 0);
        tlc_cold_end_resync_snapshot(tlc_core_get_cold(node.core), &snapshot);
        printf("tlc_ha_replica_ub_node_ut: leader install PASS generation=%llu\n",
               (unsigned long long)checkpoint.generation);
        usleep(500000);
    } else if (follower_install) {
        tlc_cold_checkpoint_result_t installed;
        int rc = -1;
        for (uint32_t i = 0; i < 60000 && rc != 0; i++) {
            rc = tlc_ha_replica_install_resync_snapshot(replica, &installed);
            if (rc != 0)
                usleep(1000);
        }
        assert(rc == 0 && installed.generation == 1);
        assert(value_matches(node.core, "ub-resync-install-key", 0x51));
        printf("tlc_ha_replica_ub_node_ut: follower install PASS generation=%llu\n",
               (unsigned long long)installed.generation);
    } else if (leader_m6_gap) {
        tlc_cold_t *cold = tlc_core_get_cold(node.core);
        for (uint32_t i = 0; i < 60000 &&
             tlc_ha_replica_peer_health(replica) !=
                 TLC_HA_REPLICA_HEALTHY; i++)
            usleep(1000);
        assert(tlc_ha_replica_peer_health(replica) ==
               TLC_HA_REPLICA_HEALTHY);
        put_value(node.core, "ub-m6-gap", 0xa2);
        assert(wait_for_durable(cold, 2));
        tlc_ha_replica_progress_t progress;
        for (uint32_t i = 0; i < 60000; i++) {
            assert(tlc_ha_replica_get_progress(replica, &progress) == 0);
            if (progress.peer_accepted_seq >= 2)
                break;
            usleep(1000);
        }
        assert(progress.peer_accepted_seq >= 2);
        put_value(node.core, "ub-m6-post-handoff", 0xa3);
        assert(wait_for_durable(cold, 3));
        for (uint32_t i = 0; i < 60000; i++) {
            assert(tlc_ha_replica_get_progress(replica, &progress) == 0);
            if (progress.peer_accepted_seq >= 3)
                break;
            usleep(1000);
        }
        assert(progress.peer_accepted_seq >= 3);
        printf("tlc_ha_replica_ub_node_ut: leader M6 AOF GAP PASS accepted=%llu\n",
               (unsigned long long)progress.peer_accepted_seq);
    } else if (follower_m6_gap) {
        assert(wait_for_value(node.core, "ub-m6-checkpoint", 0xa1));
        assert(wait_for_value(node.core, "ub-m6-gap", 0xa2));
        assert(wait_for_value(node.core, "ub-m6-post-handoff", 0xa3));
        assert(wait_for_durable(tlc_core_get_cold(node.core), 3));
        tlc_cold_progress_t progress;
        assert(tlc_cold_get_progress(tlc_core_get_cold(node.core),
                                     &progress) == 0);
        printf("tlc_ha_replica_ub_node_ut: follower M6 AOF GAP PASS durable=%llu\n",
               (unsigned long long)progress.durable_seq);
    } else if (leader_m6_retention) {
        tlc_cold_t *cold = tlc_core_get_cold(node.core);
        for (uint32_t i = 0; i < 60000 &&
             tlc_ha_replica_peer_health(replica) != TLC_HA_REPLICA_HEALTHY; i++)
            usleep(1000);
        assert(tlc_ha_replica_peer_health(replica) == TLC_HA_REPLICA_HEALTHY);
        put_value(node.core, "ub-m6-retention-key", 0x62);
        assert(wait_for_durable(cold, 98));
        tlc_ha_replica_progress_t progress;
        for (uint32_t i = 0; i < 60000; i++) {
            assert(tlc_ha_replica_get_progress(replica, &progress) == 0);
            if (progress.peer_accepted_seq >= 98)
                break;
            usleep(1000);
        }
        assert(progress.peer_accepted_seq >= 98);
        printf("tlc_ha_replica_ub_node_ut: leader M6 retention PASS accepted=%llu\n",
               (unsigned long long)progress.peer_accepted_seq);
    } else if (follower_m6_retention) {
        assert(wait_for_value(node.core, "ub-m6-retention-key", 0x62));
        assert(wait_for_durable(tlc_core_get_cold(node.core), 98));
        tlc_cold_progress_t progress;
        assert(tlc_cold_get_progress(tlc_core_get_cold(node.core),
                                     &progress) == 0);
        printf("tlc_ha_replica_ub_node_ut: follower M6 retention PASS durable=%llu\n",
               (unsigned long long)progress.durable_seq);
    } else if (leader_m7_retention) {
        enum { M7_RETENTION_EVENTS = 8 };
        assert(retention_events == M7_RETENTION_EVENTS);
        tlc_cold_t *cold = tlc_core_get_cold(node.core);
        for (uint32_t i = 1; i <= M7_RETENTION_EVENTS; i++)
            put_value(node.core, "ub-m7-retention-key", (uint8_t)i);
        assert(wait_for_durable(cold, M7_RETENTION_EVENTS));
        assert(wait_for_retention_compact(cold, M7_RETENTION_EVENTS));
        tlc_cold_retention_window_t window;
        tlc_cold_checkpoint_result_t checkpoint;
        assert(tlc_cold_get_retention_window(cold, &window) == 0);
        assert(tlc_cold_validate_checkpoint(cold, 0, 256, &checkpoint) == 0);
        assert(window.target_events == M7_RETENTION_EVENTS);
        assert(window.ring_capacity == 16);
        assert(checkpoint.checkpoint_seq >= M7_RETENTION_EVENTS);
        tlc_ha_replica_progress_t progress;
        for (uint32_t i = 0; i < 60000; i++) {
            assert(tlc_ha_replica_get_progress(replica, &progress) == 0);
            if (progress.peer_accepted_seq >= M7_RETENTION_EVENTS)
                break;
            usleep(1000);
        }
        assert(progress.peer_accepted_seq >= M7_RETENTION_EVENTS);
        printf("tlc_ha_replica_ub_node_ut: leader M7 retention PASS R=%llu C=%llu floor=%llu checkpoint=%llu accepted=%llu\n",
               (unsigned long long)window.target_events,
               (unsigned long long)window.ring_capacity,
               (unsigned long long)window.retained_floor_seq,
               (unsigned long long)checkpoint.checkpoint_seq,
               (unsigned long long)progress.peer_accepted_seq);
    } else if (follower_m7_retention) {
        assert(retention_events == 8);
        assert(wait_for_value(node.core, "ub-m7-retention-key", 8));
        assert(wait_for_durable(tlc_core_get_cold(node.core), 8));
        tlc_cold_progress_t progress;
        assert(tlc_cold_get_progress(tlc_core_get_cold(node.core),
                                     &progress) == 0);
        printf("tlc_ha_replica_ub_node_ut: follower M7 retention PASS durable=%llu\n",
               (unsigned long long)progress.durable_seq);
    } else if (leader_m7_pressure) {
        enum { M7_RETENTION_EVENTS = 8, M7_HARD_SEQ = 14 };
        assert(retention_events == M7_RETENTION_EVENTS);
        tlc_cold_t *cold = tlc_core_get_cold(node.core);
        put_value(node.core, "ub-m7-pressure-key", 1);
        assert(wait_for_durable(cold, 1));
        tlc_cold_checkpoint_result_t checkpoint;
        assert(tlc_core_publish_checkpoint(node.core, 1, 9, &checkpoint) == 0);
        assert(tlc_ha_replica_begin_resync(replica, 7701, 9, 32768) == 0);
        for (uint32_t seq = 2; seq <= 9; seq++)
            put_value(node.core, "ub-m7-pressure-key", (uint8_t)seq);
        assert(wait_for_durable(cold, 9));
        assert(wait_for_retention_floor(cold, 1, 9));
        for (uint32_t seq = 10; seq <= M7_HARD_SEQ; seq++)
            put_value(node.core, "ub-m7-pressure-key", (uint8_t)seq);
        assert(wait_for_durable(cold, M7_HARD_SEQ));
        assert(wait_for_retention_compact(cold, M7_RETENTION_EVENTS));
        tlc_cold_retention_window_t window;
        assert(tlc_cold_get_retention_window(cold, &window) == 0);
        assert(window.target_events == M7_RETENTION_EVENTS);
        assert(window.ring_capacity == 16);
        assert(window.retained_floor_seq == 0 || window.retained_floor_seq > 1);
        printf("tlc_ha_replica_ub_node_ut: leader M7 pressure PASS R=%llu C=%llu floor=%llu hard_seq=%u\n",
               (unsigned long long)window.target_events,
               (unsigned long long)window.ring_capacity,
               (unsigned long long)window.retained_floor_seq,
               M7_HARD_SEQ);
    } else if (follower_m7_pressure_stall) {
        usleep(30000000);
        char path[PATH_MAX];
        size_t bytes = 0;
        tlc_ha_resync_snapshot_begin_t begin;
        assert(tlc_ha_replica_take_resync_snapshot(replica, path, sizeof(path),
                                                    &bytes, &begin) != 0);
        printf("tlc_ha_replica_ub_node_ut: follower M7 pressure stall PASS\n");
    } else if (leader_m5) {
        tlc_cold_t *cold = tlc_core_get_cold(node.core);
        put_value(node.core, "ub-m5-checkpoint", 0x51);
        assert(wait_for_durable(cold, 1));
        tlc_cold_checkpoint_result_t checkpoint;
        assert(tlc_core_publish_checkpoint(node.core, 1, 9, &checkpoint) == 0);
        put_value(node.core, "ub-m5-tail", 0x61);
        assert(wait_for_durable(cold, 2));
        assert(tlc_ha_replica_begin_resync(replica, 7001, 6, 32768) == 0);
        put_value(node.core, "ub-m5-late", 0x71);
        assert(wait_for_durable(cold, 3));
        tlc_ha_replica_progress_t progress;
        for (uint32_t i = 0; i < 60000; i++) {
            assert(tlc_ha_replica_get_progress(replica, &progress) == 0);
            if (progress.peer_durable_seq >= 3)
                break;
            usleep(1000);
        }
        assert(progress.peer_durable_seq >= 3);
        usleep(100000);
        put_value(node.core, "ub-m5-post-handoff", 0x81);
        assert(wait_for_durable(cold, 4));
        for (uint32_t i = 0; i < 60000; i++) {
            assert(tlc_ha_replica_get_progress(replica, &progress) == 0);
            if (progress.peer_accepted_seq >= 4 &&
                progress.peer_applied_seq >= 4)
                break;
            usleep(1000);
        }
        assert(progress.peer_accepted_seq >= 4 &&
               progress.peer_applied_seq >= 4);
        printf("tlc_ha_replica_ub_node_ut: leader M5 PASS H=3 accepted=%llu applied=%llu\n",
               (unsigned long long)progress.peer_accepted_seq,
               (unsigned long long)progress.peer_applied_seq);
        usleep(500000);
    } else if (leader_m5_abort) {
        tlc_cold_t *cold = tlc_core_get_cold(node.core);
        put_value(node.core, "ub-m5-abort-checkpoint", 0x91);
        assert(wait_for_durable(cold, 1));
        tlc_cold_checkpoint_result_t checkpoint;
        assert(tlc_core_publish_checkpoint(node.core, 1, 9, &checkpoint) == 0);
        assert(tlc_ha_replica_begin_resync(replica, 7101, 8, 32768) == 0);
        usleep(500000);
        assert(tlc_ha_replica_begin_resync(replica, 7102, 9, 32768) == 0);
        assert(tlc_ha_replica_abort_resync(replica) == 0);
        printf("tlc_ha_replica_ub_node_ut: leader M5 abort PASS\n");
    } else if (follower_m5) {
        tlc_cold_checkpoint_result_t installed;
        int rc = -1;
        for (uint32_t i = 0; i < 60000 && rc != 0; i++) {
            rc = tlc_ha_replica_install_resync_snapshot(replica, &installed);
            if (rc != 0)
                usleep(1000);
        }
        assert(rc == 0 && installed.generation == 1);
        assert(wait_for_value(node.core, "ub-m5-checkpoint", 0x51));
        assert(wait_for_value(node.core, "ub-m5-tail", 0x61));
        assert(wait_for_value(node.core, "ub-m5-late", 0x71));
        assert(wait_for_value(node.core, "ub-m5-post-handoff", 0x81));
        tlc_cold_t *cold = tlc_core_get_cold(node.core);
        assert(wait_for_durable(cold, 4));
        tlc_cold_progress_t progress;
        assert(tlc_cold_get_progress(cold, &progress) == 0);
        printf("tlc_ha_replica_ub_node_ut: follower M5 PASS durable=%llu\n",
               (unsigned long long)progress.durable_seq);
        usleep(500000);
    } else if (follower_m5_abort) {
        usleep(2000000);
        char path[PATH_MAX];
        size_t bytes = 0;
        tlc_ha_resync_snapshot_begin_t begin;
        assert(tlc_ha_replica_take_resync_snapshot(replica, path, sizeof(path),
                                                    &bytes, &begin) != 0);
        printf("tlc_ha_replica_ub_node_ut: follower M5 abort PASS\n");
    } else if (leader_snapshot) {
        enum { SNAPSHOT_BYTES = 200000 };
        uint8_t *blob = zmalloc(SNAPSHOT_BYTES);
        assert(blob != NULL);
        for (uint32_t i = 0; i < SNAPSHOT_BYTES; i++)
            blob[i] = (uint8_t)(i * 29u);
        tlc_cold_resync_snapshot_t snapshot = {
            .checkpoint_blob = blob,
            .checkpoint_blob_bytes = SNAPSHOT_BYTES,
            .meta_shard_count = 256,
            .checkpoint = {.generation = 9, .ha_term = 9,
                           .checkpoint_seq = 41},
            .checkpoint_blob_checksum = vemb_v16_xxh3_64(blob,
                                                          SNAPSHOT_BYTES),
            .captured_seq_checksum = 0x1234,
            .tail_start_seq = 42,
            .durable_boundary_seq = 99,
        };
        assert(tlc_ha_replica_send_resync_snapshot(replica, 5001, 3,
                                                    &snapshot, 32768) == 0);
        zfree(blob);
        printf("tlc_ha_replica_ub_node_ut: leader snapshot PASS bytes=%u\n",
               SNAPSHOT_BYTES);
        usleep(500000);
    } else if (follower_snapshot) {
        char path[PATH_MAX] = {0};
        size_t bytes = 0;
        tlc_ha_resync_snapshot_begin_t begin;
        for (uint32_t i = 0; i < 60000 && !path[0]; i++) {
            tlc_ha_replica_take_resync_snapshot(replica, path, sizeof(path),
                                                &bytes, &begin);
            if (!path[0])
                usleep(1000);
        }
        assert(path[0] != '\0' && bytes == 200000);
        int artifact_fd = open(path, O_RDONLY);
        assert(artifact_fd >= 0);
        uint8_t *blob = zmalloc(bytes);
        assert(blob != NULL);
        size_t offset = 0;
        while (offset < bytes) {
            ssize_t received = read(artifact_fd, blob + offset, bytes - offset);
            assert(received > 0);
            offset += (size_t)received;
        }
        assert(close(artifact_fd) == 0);
        for (uint32_t i = 0; i < bytes; i++)
            assert(blob[i] == (uint8_t)(i * 29u));
        assert(begin.session_id == 5001 && begin.topology_epoch == 3);
        zfree(blob);
        assert(unlink(path) == 0);
        printf("tlc_ha_replica_ub_node_ut: follower snapshot PASS bytes=%zu\n",
               bytes);
    } else if (leader_replay) {
        const char *replay_start_text = getenv("TLC_HA_UB_REPLAY_START");
        const char *replay_end_text = getenv("TLC_HA_UB_REPLAY_END");
        assert(replay_start_text && replay_end_text);
        uint64_t replay_start = strtoull(replay_start_text, NULL, 0);
        uint64_t replay_end = strtoull(replay_end_text, NULL, 0);
        assert(replay_start != 0 && replay_end >= replay_start);
        for (uint32_t i = 0; i < 60000 &&
             tlc_ha_replica_peer_health(replica) !=
                 TLC_HA_REPLICA_HEALTHY; i++)
            usleep(1000);
        assert(tlc_ha_replica_peer_health(replica) ==
               TLC_HA_REPLICA_HEALTHY);
        assert(tlc_ha_replica_replay_from(replica, replay_start,
                                          replay_end) == 0);
        for (uint32_t i = 0; i < 60000 &&
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
        if (getenv("TLC_HA_UB_WAIT_FOR_REPLAY")) {
            uint32_t wait_for_replay_ms =
                getenv("TLC_HA_UB_WAIT_FOR_REPLAY_MS") ?
                    (uint32_t)strtoul(getenv("TLC_HA_UB_WAIT_FOR_REPLAY_MS"),
                                      NULL, 0) :
                    60000;
            tlc_ha_replica_progress_t progress = {0};
            for (uint32_t i = 0; i < wait_for_replay_ms; i++) {
                assert(tlc_ha_replica_get_progress(replica, &progress) == 0);
                if (progress.applied_seq >= expected_events)
                    break;
                usleep(1000);
            }
            assert(progress.applied_seq >= expected_events);
        }
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
