#include "tlc_ha_replica.h"
#include "monotonic.h"
#include "vemb_v16_hash.h"
#include "zmalloc.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/wait.h>
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
        atomic_init(&node->slot_meta[i].state_version,
                    vemb_v16_warm_slot_pack(0, VEMB_V16_WARM_SLOT_FREE));
        atomic_init(&node->slot_meta[i].owner_generation, 0);
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

static atomic_uint test_ring_counter;

typedef struct test_ub_rings {
    char tx_path[64];
    char rx_path[64];
    tlc_ha_replica_ring_config_t tx;
    tlc_ha_replica_ring_config_t rx;
} test_ub_rings_t;

static int test_ub_rings_create(test_ub_rings_t *out) {
    uint32_t id = atomic_fetch_add(&test_ring_counter, 1);
    snprintf(out->tx_path, sizeof(out->tx_path), "/tlc-ha-proc-tx-%u-%u",
             (unsigned)getpid(), id);
    snprintf(out->rx_path, sizeof(out->rx_path), "/tlc-ha-proc-rx-%u-%u",
             (unsigned)getpid(), id);
    tlc_ha_replica_ring_config_t ring = {
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .cache_policy = VEMB_V16_UB_CACHE_POLICY_CACHEABLE,
        .slot_count = 8,
        .slot_bytes = 131072,
        .mmap_offset = 0,
    };
    snprintf(ring.path, sizeof(ring.path), "%s", out->tx_path);
    if (tlc_ha_replica_reset_ring(&ring) != 0)
        return -1;
    out->tx = ring;
    snprintf(ring.path, sizeof(ring.path), "%s", out->rx_path);
    if (tlc_ha_replica_reset_ring(&ring) != 0) {
        shm_unlink(out->tx_path);
        return -1;
    }
    out->rx = ring;
    return 0;
}

static void test_ub_rings_destroy(test_ub_rings_t *rings) {
    shm_unlink(rings->tx_path);
    shm_unlink(rings->rx_path);
}

static void write_byte(int fd) {
    uint8_t value = 1;
    assert(write(fd, &value, sizeof(value)) == (ssize_t)sizeof(value));
}

static void read_byte(int fd) {
    uint8_t value;
    assert(read(fd, &value, sizeof(value)) == (ssize_t)sizeof(value));
}

int main(void) {
    assert(monotonicInit() != NULL);
    char leader_dir[] = "/tmp/tlc-ha-process-leader-XXXXXX";
    char follower_dir[] = "/tmp/tlc-ha-process-follower-XXXXXX";
    assert(mkdtemp(leader_dir) && mkdtemp(follower_dir));
    int sockets[2], ready_pipe[2], release_pipe[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    assert(pipe(ready_pipe) == 0 && pipe(release_pipe) == 0);
    test_ub_rings_t rings;
    assert(test_ub_rings_create(&rings) == 0);
    pid_t child = fork();
    assert(child >= 0);
    if (child == 0) {
        close(sockets[0]); close(ready_pipe[0]); close(release_pipe[1]);
        test_node_t follower = {0};
        node_init(&follower, 202, follower_dir);
        tlc_ha_replica_config_t config = {
            .core = follower.core, .cold = tlc_core_get_cold(follower.core),
            .control_fd = sockets[1], .role = TLC_HA_REPLICA_FOLLOWER,
            .tx_ring = rings.rx, .rx_ring = rings.tx,
            .queue_capacity = 16,
            .max_batch_events = 8, .max_batch_bytes = 65536,
            .hpc_node_id = 202, .peer_node_id = 201, .ha_term = 9,
            .heartbeat_interval_ms = 20, .heartbeat_timeout_ms = 200,
        };
        tlc_ha_replica_t *replica = NULL;
        assert(tlc_ha_replica_start(&replica, &config) == 0);
        write_byte(ready_pipe[1]);
        int ready = 0;
        for (uint32_t i = 0; i < 5000; i++) {
            if (value_matches(follower.core, "process-key-1", 3) &&
                value_matches(follower.core, "process-key-2", 33)) {
                ready = 1;
                break;
            }
            usleep(1000);
        }
        assert(ready);
        write_byte(ready_pipe[1]);
        read_byte(release_pipe[0]);
        tlc_ha_replica_stop(replica);
        node_free(&follower);
        _exit(0);
    }
    close(sockets[1]); close(ready_pipe[1]); close(release_pipe[0]);
    read_byte(ready_pipe[0]);
    test_node_t leader = {0};
    node_init(&leader, 201, leader_dir);
    tlc_ha_replica_config_t config = {
        .core = leader.core, .cold = tlc_core_get_cold(leader.core),
        .control_fd = sockets[0], .role = TLC_HA_REPLICA_LEADER,
        .tx_ring = rings.tx, .rx_ring = rings.rx,
        .queue_capacity = 16,
        .max_batch_events = 8, .max_batch_bytes = 65536,
        .hpc_node_id = 201, .peer_node_id = 202, .ha_term = 9,
        .heartbeat_interval_ms = 20, .heartbeat_timeout_ms = 200,
    };
    tlc_ha_replica_t *replica = NULL;
    assert(tlc_ha_replica_start(&replica, &config) == 0);
    put_value(leader.core, "process-key-1", 3);
    put_value(leader.core, "process-key-2", 33);
    read_byte(ready_pipe[0]);
    for (uint32_t i = 0; i < 5000 &&
         tlc_ha_replica_peer_accepted_seq(replica) < 2; i++)
        usleep(1000);
    assert(tlc_ha_replica_peer_accepted_seq(replica) >= 2);
    for (uint32_t i = 0; i < 1000 &&
         tlc_ha_replica_peer_health(replica) != TLC_HA_REPLICA_HEALTHY; i++)
        usleep(1000);
    assert(tlc_ha_replica_peer_health(replica) == TLC_HA_REPLICA_HEALTHY);
    write_byte(release_pipe[1]);
    tlc_ha_replica_stop(replica);
    node_free(&leader);
    int status;
    assert(waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    close(ready_pipe[0]); close(release_pipe[1]);
    test_ub_rings_destroy(&rings);
    printf("tlc_ha_replica_process_ut: PASS\n");
    return 0;
}
