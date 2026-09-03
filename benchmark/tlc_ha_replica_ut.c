#include "tlc_ha_replica.h"
#include "monotonic.h"
#include "vemb_v16_hash.h"
#include "zmalloc.h"

#include <assert.h>
#include <arpa/inet.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

enum { VALUE_SIZE = 8, SLOT_COUNT = 64 };

typedef struct test_frame_wire {
    uint32_t magic;
    uint16_t version;
    uint16_t kind;
    uint32_t header_bytes;
    uint64_t first_seq;
    uint32_t event_count;
    uint32_t payload_bytes;
    uint64_t checksum;
} test_frame_wire_t;

typedef struct test_heartbeat_wire {
    uint64_t hpc_node_id;
    uint64_t peer_node_id;
    uint64_t ha_term;
    uint32_t role;
    uint32_t health;
    uint64_t sent_at_ns;
    uint64_t durable_seq;
    uint64_t progress_seq;
} test_heartbeat_wire_t;

static uint64_t test_hton64(uint64_t value) {
    uint32_t hi = htonl((uint32_t)(value >> 32));
    uint32_t lo = htonl((uint32_t)value);
    return ((uint64_t)lo << 32) | hi;
}

static void test_write_full(int fd, const void *data, size_t length) {
    const uint8_t *cursor = data;
    while (length != 0) {
        ssize_t written = write(fd, cursor, length);
        assert(written > 0);
        cursor += (size_t)written;
        length -= (size_t)written;
    }
}

static void test_send_heartbeat(int fd, uint64_t node_id, uint64_t peer_id,
                                uint64_t term, uint64_t durable,
                                uint64_t progress, int corrupt_checksum) {
    test_heartbeat_wire_t payload = {
        .hpc_node_id = test_hton64(node_id),
        .peer_node_id = test_hton64(peer_id),
        .ha_term = test_hton64(term),
        .role = htonl(TLC_HA_REPLICA_FOLLOWER),
        .health = htonl(TLC_HA_REPLICA_HEALTHY),
        .sent_at_ns = test_hton64(1),
        .durable_seq = test_hton64(durable),
        .progress_seq = test_hton64(progress),
    };
    test_frame_wire_t frame = {
        .magic = htonl(UINT32_C(0x54485250)),
        .version = htons(1),
        .kind = htons(3),
        .header_bytes = htonl(sizeof(frame)),
        .first_seq = 0,
        .event_count = 0,
        .payload_bytes = htonl(sizeof(payload)),
        .checksum = 0,
    };
    uint8_t bytes[sizeof(frame) + sizeof(payload)];
    memcpy(bytes, &frame, sizeof(frame));
    memcpy(bytes + sizeof(frame), &payload, sizeof(payload));
    uint64_t checksum = vemb_v16_xxh3_64(bytes, sizeof(bytes));
    frame.checksum = test_hton64(corrupt_checksum ? checksum ^ 1 : checksum);
    memcpy(bytes, &frame, sizeof(frame));
    test_write_full(fd, bytes, sizeof(bytes));
}

typedef struct test_node {
    tlc_core_t *core;
    tlc_cold_t *cold;
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
        .hot_capacity = 32,
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
    node->cold = tlc_core_get_cold(node->core);
    assert(node->cold != NULL);
}

static void node_free(test_node_t *node) {
    tlc_core_destroy(node->core);
    zfree(node->slot_meta);
    zfree(node->region);
}

static int wait_for_value(tlc_core_t *core,
                          const char *key,
                          const uint8_t *expected) {
    uint64_t hash = vemb_v16_xxh3_64(key, strlen(key));
    for (uint32_t attempt = 0; attempt < 3000; attempt++) {
        tlc_warm_location_t location;
        if (tlc_core_get_warm_location(core, key, strlen(key), hash,
                                        &location) == 0) {
            uint8_t value[VALUE_SIZE];
            if (tlc_core_copy_warm_location_value(core, hash, &location,
                                                   value, sizeof(value), 8) == 0)
                return memcmp(value, expected, VALUE_SIZE) == 0;
        }
        usleep(1000);
    }
    return 0;
}

static void run_network_test(tlc_ha_replica_transport_t transport) {
    char leader_dir[] = "/tmp/tlc-ha-leader-XXXXXX";
    char follower_dir[] = "/tmp/tlc-ha-follower-XXXXXX";
    assert(mkdtemp(leader_dir) && mkdtemp(follower_dir));
    test_node_t leader = {0};
    test_node_t follower = {0};
    node_init(&leader, 101, leader_dir);
    node_init(&follower, 102, follower_dir);

    int sockets[2] = {-1, -1};
    char ub_path[128] = {0};
    int ub_device = 0;
    const char *tx_offset_text = getenv("TLC_HA_UB_TX_OFFSET");
    const char *rx_offset_text = getenv("TLC_HA_UB_RX_OFFSET");
    const uint64_t tx_offset = tx_offset_text ? strtoull(tx_offset_text, NULL, 0) : 0;
    const uint64_t rx_offset = rx_offset_text ? strtoull(rx_offset_text, NULL, 0) :
                               (UINT64_C(2) << 20);
    if (transport == TLC_HA_REPLICA_TRANSPORT_STREAM) {
        assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    } else {
        const char *configured_path = getenv("TLC_HA_UB_PATH");
        ub_device = configured_path && configured_path[0] != '\0';
        snprintf(ub_path, sizeof(ub_path), "%s", ub_device ?
                 configured_path : "/tlc-ha-ub-local");
        tlc_ha_replica_ring_config_t ring = {
            .backend_type = ub_device ? VEMB_V16_REGION_UB :
                                        VEMB_V16_REGION_LOCAL_SHM,
            .cache_policy = VEMB_V16_UB_CACHE_POLICY_CACHEABLE,
            .slot_count = 8,
            .slot_bytes = 131072,
        };
        snprintf(ring.path, sizeof(ring.path), "%s", ub_path);
        ring.mmap_offset = tx_offset;
        if (tlc_ha_replica_reset_ring(&ring) != 0) {
            if (!ub_device)
                shm_unlink(ub_path);
            node_free(&leader);
            node_free(&follower);
            return;
        }
        ring.mmap_offset = rx_offset;
        if (tlc_ha_replica_reset_ring(&ring) != 0) {
            if (!ub_device)
                shm_unlink(ub_path);
            node_free(&leader);
            node_free(&follower);
            return;
        }
    }
    tlc_ha_replica_t *leader_replica = NULL;
    tlc_ha_replica_t *follower_replica = NULL;
    tlc_ha_replica_config_t leader_config = {
        .core = leader.core,
        .cold = leader.cold,
        .fd = sockets[0],
        .role = TLC_HA_REPLICA_LEADER,
        .transport = transport,
        .queue_capacity = 32,
        .max_batch_events = 8,
        .max_batch_bytes = 65536,
        .hpc_node_id = 111,
        .peer_node_id = 112,
        .ha_term = 7,
        .heartbeat_interval_ms = 10,
        .heartbeat_timeout_ms = 100,
    };
    if (transport == TLC_HA_REPLICA_TRANSPORT_UB) {
        leader_config.tx_ring.backend_type = ub_device ? VEMB_V16_REGION_UB :
                                               VEMB_V16_REGION_LOCAL_SHM;
        leader_config.tx_ring.cache_policy = VEMB_V16_UB_CACHE_POLICY_CACHEABLE;
        leader_config.tx_ring.slot_count = 8;
        leader_config.tx_ring.slot_bytes = 131072;
        leader_config.tx_ring.mmap_offset = tx_offset;
        snprintf(leader_config.tx_ring.path, sizeof(leader_config.tx_ring.path),
                 "%s", ub_path);
        leader_config.rx_ring = leader_config.tx_ring;
        leader_config.rx_ring.mmap_offset = rx_offset;
        snprintf(leader_config.rx_ring.path, sizeof(leader_config.rx_ring.path),
                 "%s", ub_path);
    }
    tlc_ha_replica_config_t follower_config = leader_config;
    follower_config.core = follower.core;
    follower_config.fd = sockets[1];
    follower_config.role = TLC_HA_REPLICA_FOLLOWER;
    follower_config.cold = follower.cold;
    follower_config.hpc_node_id = 112;
    follower_config.peer_node_id = 111;
    if (transport == TLC_HA_REPLICA_TRANSPORT_UB) {
        follower_config.tx_ring = leader_config.rx_ring;
        follower_config.rx_ring = leader_config.tx_ring;
    }
    assert(tlc_ha_replica_start(&leader_replica, &leader_config) == 0);
    assert(tlc_ha_replica_start(&follower_replica, &follower_config) == 0);
    const char *key = "network-key";
    const uint8_t value[VALUE_SIZE] = {1, 3, 5, 7, 9, 11, 13, 15};
    uint64_t key_hash = vemb_v16_xxh3_64(key, strlen(key));
    uint32_t warm_slot;
    assert(tlc_core_put(leader.core, key, strlen(key), key_hash,
                        value, sizeof(value), &warm_slot) == 0);
    assert(wait_for_value(follower.core, key, value));
    for (uint32_t attempt = 0; attempt < 3000 &&
             tlc_ha_replica_peer_accepted_seq(leader_replica) < 1; attempt++)
        usleep(1000);
    assert(tlc_ha_replica_peer_accepted_seq(leader_replica) >= 1);
    for (uint32_t attempt = 0; attempt < 3000 &&
         (tlc_ha_replica_peer_health(leader_replica) !=
              TLC_HA_REPLICA_HEALTHY ||
          tlc_ha_replica_peer_health(follower_replica) !=
              TLC_HA_REPLICA_HEALTHY); attempt++)
        usleep(1000);
    assert(tlc_ha_replica_peer_health(leader_replica) ==
           TLC_HA_REPLICA_HEALTHY);
    assert(tlc_ha_replica_peer_health(follower_replica) ==
           TLC_HA_REPLICA_HEALTHY);
    tlc_ha_replica_stop(leader_replica);
    tlc_ha_replica_stop(follower_replica);
    node_free(&leader);
    node_free(&follower);
    if (transport == TLC_HA_REPLICA_TRANSPORT_UB) {
        if (!ub_device)
            shm_unlink(ub_path);
    }
}

static tlc_ha_replica_t *start_heartbeat_probe(test_node_t *node, int fd) {
    tlc_ha_replica_config_t config = {
        .core = node->core,
        .cold = node->cold,
        .fd = fd,
        .role = TLC_HA_REPLICA_LEADER,
        .transport = TLC_HA_REPLICA_TRANSPORT_STREAM,
        .queue_capacity = 8,
        .max_batch_events = 4,
        .max_batch_bytes = 4096,
        .hpc_node_id = 111,
        .peer_node_id = 112,
        .ha_term = 7,
        .heartbeat_interval_ms = 10,
        .heartbeat_timeout_ms = 50,
    };
    tlc_ha_replica_t *replica = NULL;
    assert(tlc_ha_replica_start(&replica, &config) == 0);
    return replica;
}

static void run_heartbeat_error_tests(void) {
    {
        char directory[] = "/tmp/tlc-ha-heartbeat-order-XXXXXX";
        assert(mkdtemp(directory));
        test_node_t node = {0};
        node_init(&node, 103, directory);
        int sockets[2];
        assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
        tlc_ha_replica_t *replica = start_heartbeat_probe(&node, sockets[0]);
        test_send_heartbeat(sockets[1], 112, 111, 7, 9, 8, 0);
        test_send_heartbeat(sockets[1], 112, 111, 7, 3, 2, 0);
        tlc_ha_replica_progress_t progress;
        for (uint32_t i = 0; i < 100; i++) {
            assert(tlc_ha_replica_get_progress(replica, &progress) == 0);
            if (progress.peer_durable_seq >= 9)
                break;
            usleep(1000);
        }
        assert(progress.peer_durable_seq == 9);
        tlc_ha_replica_stop(replica);
        close(sockets[1]);
        node_free(&node);
    }

    for (uint32_t case_id = 0; case_id < 3; case_id++) {
        char directory[] = "/tmp/tlc-ha-heartbeat-invalid-XXXXXX";
        assert(mkdtemp(directory));
        test_node_t node = {0};
        node_init(&node, 104 + case_id, directory);
        int sockets[2];
        assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
        tlc_ha_replica_t *replica = start_heartbeat_probe(&node, sockets[0]);
        if (case_id == 0)
            test_send_heartbeat(sockets[1], 112, 111, 6, 4, 4, 0);
        else if (case_id == 1)
            test_send_heartbeat(sockets[1], 999, 111, 7, 4, 4, 0);
        else
            test_send_heartbeat(sockets[1], 112, 111, 7, 4, 4, 1);
        usleep(20000);
        assert(tlc_ha_replica_peer_health(replica) !=
               TLC_HA_REPLICA_HEALTHY);
        tlc_ha_replica_stop(replica);
        close(sockets[1]);
        node_free(&node);
    }

    {
        char directory[] = "/tmp/tlc-ha-heartbeat-timeout-XXXXXX";
        assert(mkdtemp(directory));
        test_node_t node = {0};
        node_init(&node, 108, directory);
        int sockets[2];
        assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
        tlc_ha_replica_t *replica = start_heartbeat_probe(&node, sockets[0]);
        usleep(100000);
        assert(tlc_ha_replica_peer_health(replica) ==
               TLC_HA_REPLICA_UNAVAILABLE);
        test_send_heartbeat(sockets[1], 112, 111, 7, 1, 1, 0);
        for (uint32_t i = 0; i < 100 &&
             tlc_ha_replica_peer_health(replica) !=
                 TLC_HA_REPLICA_HEALTHY; i++)
            usleep(1000);
        assert(tlc_ha_replica_peer_health(replica) ==
               TLC_HA_REPLICA_HEALTHY);
        tlc_ha_replica_stop(replica);
        close(sockets[1]);
        node_free(&node);
    }
}

int main(void) {
    assert(monotonicInit() != NULL);
    run_network_test(TLC_HA_REPLICA_TRANSPORT_STREAM);
    run_network_test(TLC_HA_REPLICA_TRANSPORT_UB);
    run_heartbeat_error_tests();
    printf("tlc_ha_replica_ut: PASS\n");
    return 0;
}
