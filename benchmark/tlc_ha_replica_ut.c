#include "tlc_ha_replica.h"
#include "monotonic.h"
#include "vemb_v16_hash.h"
#include "zmalloc.h"

#include <assert.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <limits.h>
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

static uint8_t *test_read_file(const char *path, size_t bytes) {
    int fd = open(path, O_RDONLY);
    assert(fd >= 0);
    uint8_t *data = zmalloc(bytes);
    assert(data != NULL);
    size_t offset = 0;
    while (offset < bytes) {
        ssize_t received = read(fd, data + offset, bytes - offset);
        assert(received > 0);
        offset += (size_t)received;
    }
    assert(close(fd) == 0);
    return data;
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

static void node_init_with_retention(test_node_t *node, uint32_t region_id,
                                     const char *directory,
                                     uint64_t retention_events) {
    node->region = zcalloc(VALUE_SIZE * SLOT_COUNT);
    node->slot_meta = zcalloc_num(SLOT_COUNT, sizeof(*node->slot_meta));
    assert(node->region && node->slot_meta);
    for (uint32_t i = 0; i < SLOT_COUNT; i++) {
        atomic_init(&node->slot_meta[i].state_version,
                    vemb_v16_warm_slot_pack(0, VEMB_V16_WARM_SLOT_FREE));
        atomic_init(&node->slot_meta[i].owner_generation, 0);
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
        .retention_events = retention_events,
    };
    assert(tlc_core_enable_cold(node->core, &cold_config) == 0);
    node->cold = tlc_core_get_cold(node->core);
    assert(node->cold != NULL);
}

static void node_init(test_node_t *node, uint32_t region_id,
                      const char *directory) {
    node_init_with_retention(node, region_id, directory, 0);
}

static void node_free(test_node_t *node) {
    tlc_core_destroy(node->core);
    zfree(node->slot_meta);
    zfree(node->region);
}

static _Atomic uint32_t test_ring_counter = 0;

typedef struct test_ub_rings {
    char tx_path[64];
    char rx_path[64];
    tlc_ha_replica_ring_config_t tx;
    tlc_ha_replica_ring_config_t rx;
} test_ub_rings_t;

static int test_ub_rings_create(test_ub_rings_t *out) {
    uint32_t id = atomic_fetch_add(&test_ring_counter, 1);
    snprintf(out->tx_path, sizeof(out->tx_path), "/tlc-ha-ut-tx-%u-%u",
             (unsigned)getpid(), id);
    snprintf(out->rx_path, sizeof(out->rx_path), "/tlc-ha-ut-rx-%u-%u",
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

static tlc_ha_resync_snapshot_begin_t test_snapshot_begin(
        uint64_t session_id, const void *blob, size_t blob_bytes) {
    return (tlc_ha_resync_snapshot_begin_t){
        .session_id = session_id,
        .leader_node_id = 111,
        .follower_node_id = 112,
        .ha_term = 7,
        .topology_epoch = 3,
        .generation = 9,
        .checkpoint_seq = 41,
        .durable_boundary_seq = 99,
        .checkpoint_blob_bytes = blob_bytes,
        .checkpoint_blob_checksum = vemb_v16_xxh3_64(blob, blob_bytes),
        .captured_seq_checksum = 0x1234,
        .meta_shard_count = 256,
    };
}

static tlc_ha_resync_snapshot_chunk_t test_snapshot_chunk(
        uint64_t session_id, uint64_t offset, const void *data,
        uint32_t bytes) {
    return (tlc_ha_resync_snapshot_chunk_t){
        .session_id = session_id,
        .offset = offset,
        .bytes = bytes,
        .checksum = vemb_v16_xxh3_64(data, bytes),
    };
}

static void test_resync_snapshot_assembler(void) {
    uint8_t blob[257];
    for (uint32_t i = 0; i < sizeof(blob); i++)
        blob[i] = (uint8_t)(i * 13u);
    char artifact_directory[] = "/tmp/tlc-ha-resync-artifact-XXXXXX";
    assert(mkdtemp(artifact_directory) != NULL);
    tlc_ha_resync_assembler_t *assembler = NULL;
    assert(tlc_ha_resync_assembler_create(&assembler, 112, 111, 7,
                                           4096, artifact_directory) == 0);

    tlc_ha_resync_snapshot_begin_t begin =
        test_snapshot_begin(1001, blob, sizeof(blob));
    assert(tlc_ha_resync_assembler_begin(assembler, &begin) == 0);
    tlc_ha_resync_snapshot_chunk_t first =
        test_snapshot_chunk(begin.session_id, 0, blob, 97);
    tlc_ha_resync_snapshot_chunk_t second =
        test_snapshot_chunk(begin.session_id, 97, blob + 97,
                            sizeof(blob) - 97);
    assert(tlc_ha_resync_assembler_append(assembler, &first, blob) == 0);
    assert(tlc_ha_resync_assembler_append(assembler, &second, blob + 97) == 0);
    tlc_ha_resync_snapshot_end_t end = {
        .session_id = begin.session_id,
        .checkpoint_blob_bytes = sizeof(blob),
        .checkpoint_blob_checksum = begin.checkpoint_blob_checksum,
    };
    char assembled_path[PATH_MAX];
    size_t assembled_bytes = 0;
    tlc_ha_resync_snapshot_begin_t assembled_begin;
    assert(tlc_ha_resync_assembler_finish(assembler, &end, assembled_path,
                                           sizeof(assembled_path), &assembled_bytes,
                                           &assembled_begin) == 0);
    assert(assembled_bytes == sizeof(blob));
    uint8_t *assembled_blob = test_read_file(assembled_path, assembled_bytes);
    assert(memcmp(assembled_blob, blob, sizeof(blob)) == 0);
    assert(assembled_begin.session_id == begin.session_id);
    zfree(assembled_blob);
    assert(unlink(assembled_path) == 0);
    assert(tlc_ha_resync_assembler_stage(assembler) ==
           TLC_HA_RESYNC_SNAPSHOT_COMPLETE);
    tlc_ha_resync_assembler_destroy(assembler);

    assert(tlc_ha_resync_assembler_create(&assembler, 112, 111, 7,
                                           4096, artifact_directory) == 0);
    begin = test_snapshot_begin(1002, blob, sizeof(blob));
    begin.follower_node_id = 113;
    assert(tlc_ha_resync_assembler_begin(assembler, &begin) != 0);
    assert(tlc_ha_resync_assembler_reason(assembler) ==
           TLC_HA_RESYNC_REASON_IDENTITY);
    tlc_ha_resync_assembler_destroy(assembler);

    assert(tlc_ha_resync_assembler_create(&assembler, 112, 111, 7,
                                           4096, artifact_directory) == 0);
    begin = test_snapshot_begin(1003, blob, sizeof(blob));
    assert(tlc_ha_resync_assembler_begin(assembler, &begin) == 0);
    first = test_snapshot_chunk(begin.session_id, 1, blob, 97);
    assert(tlc_ha_resync_assembler_append(assembler, &first, blob) != 0);
    assert(tlc_ha_resync_assembler_reason(assembler) ==
           TLC_HA_RESYNC_REASON_OFFSET);
    begin = test_snapshot_begin(1004, blob, sizeof(blob));
    assert(tlc_ha_resync_assembler_begin(assembler, &begin) == 0);
    first = test_snapshot_chunk(begin.session_id, 0, blob, 97);
    first.checksum ^= 1;
    assert(tlc_ha_resync_assembler_append(assembler, &first, blob) != 0);
    assert(tlc_ha_resync_assembler_reason(assembler) ==
           TLC_HA_RESYNC_REASON_CHECKSUM);
    tlc_ha_resync_assembler_destroy(assembler);

    assert(tlc_ha_resync_assembler_create(&assembler, 112, 111, 7,
                                           4096, artifact_directory) == 0);
    begin = test_snapshot_begin(1005, blob, sizeof(blob));
    begin.checkpoint_blob_checksum ^= 1;
    assert(tlc_ha_resync_assembler_begin(assembler, &begin) == 0);
    first = test_snapshot_chunk(begin.session_id, 0, blob, 97);
    second = test_snapshot_chunk(begin.session_id, 97, blob + 97,
                                 sizeof(blob) - 97);
    assert(tlc_ha_resync_assembler_append(assembler, &first, blob) == 0);
    assert(tlc_ha_resync_assembler_append(assembler, &second, blob + 97) == 0);
    end = (tlc_ha_resync_snapshot_end_t){
        .session_id = begin.session_id,
        .checkpoint_blob_bytes = sizeof(blob),
        .checkpoint_blob_checksum = begin.checkpoint_blob_checksum,
    };
    assert(tlc_ha_resync_assembler_finish(assembler, &end, assembled_path,
                                           sizeof(assembled_path), &assembled_bytes,
                                           &assembled_begin) != 0);
    assert(tlc_ha_resync_assembler_reason(assembler) ==
           TLC_HA_RESYNC_REASON_CHECKSUM);
    char failed_part_path[PATH_MAX];
    int failed_part_path_bytes = snprintf(
        failed_part_path, sizeof(failed_part_path),
        "%s/%016llx/checkpoint.blob.part", artifact_directory,
        (unsigned long long)begin.session_id);
    assert(failed_part_path_bytes > 0 &&
           (size_t)failed_part_path_bytes < sizeof(failed_part_path));
    assert(access(failed_part_path, F_OK) != 0);
    tlc_ha_resync_assembler_destroy(assembler);
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
                                                   value, sizeof(value), 8) == 0 &&
                memcmp(value, expected, VALUE_SIZE) == 0)
                return 1;
        }
        usleep(1000);
    }
    return 0;
}

static int wait_for_cold_durable(tlc_cold_t *cold, uint64_t expected) {
    for (uint32_t attempt = 0; attempt < 3000; attempt++) {
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
    for (uint32_t attempt = 0; attempt < 3000; attempt++) {
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
    for (uint32_t attempt = 0; attempt < 3000; attempt++) {
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

static void run_automatic_retention_maintenance_test(void) {
    char leader_dir[] = "/tmp/tlc-ha-maintenance-leader-XXXXXX";
    assert(mkdtemp(leader_dir));
    test_node_t leader = {0};
    enum { RETENTION_EVENTS = 8 };
    node_init_with_retention(&leader, 121, leader_dir, RETENTION_EVENTS);

    int sockets[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    test_ub_rings_t rings;
    assert(test_ub_rings_create(&rings) == 0);
    tlc_ha_replica_config_t config = {
        .core = leader.core,
        .cold = leader.cold,
        .control_fd = sockets[0],
        .role = TLC_HA_REPLICA_LEADER,
        .tx_ring = rings.tx,
        .rx_ring = rings.rx,
        .queue_capacity = 32,
        .max_batch_events = 8,
        .max_batch_bytes = 65536,
        .hpc_node_id = 121,
        .peer_node_id = 122,
        .ha_term = 7,
        .topology_epoch = 1,
        .heartbeat_interval_ms = 10,
        .heartbeat_timeout_ms = 100,
    };
    tlc_ha_replica_t *replica = NULL;
    assert(tlc_ha_replica_start(&replica, &config) == 0);

    const char *key = "retention-maintenance";
    uint64_t key_hash = vemb_v16_xxh3_64(key, strlen(key));
    uint32_t slot = 0;
    uint8_t value[VALUE_SIZE] = {0};
    for (uint32_t seq = 1; seq <= RETENTION_EVENTS; seq++) {
        value[0] = (uint8_t)seq;
        assert(tlc_core_put(leader.core, key, strlen(key), key_hash, value,
                            sizeof(value), &slot) == 0);
    }
    assert(wait_for_cold_durable(leader.cold, RETENTION_EVENTS));
    assert(wait_for_retention_compact(leader.cold, RETENTION_EVENTS));
    tlc_cold_checkpoint_result_t checkpoint;
    assert(tlc_cold_validate_checkpoint(leader.cold, 0, 256, &checkpoint) == 0);
    assert(checkpoint.checkpoint_seq >= RETENTION_EVENTS);

    tlc_ha_replica_stop(replica);
    close(sockets[1]);
    test_ub_rings_destroy(&rings);
    node_free(&leader);
}

static void run_retention_pressure_abort_test(void) {
    char leader_dir[] = "/tmp/tlc-ha-retention-pressure-XXXXXX";
    assert(mkdtemp(leader_dir));
    test_node_t leader = {0};
    enum { RETENTION_EVENTS = 8, HARD_SEQ = 14 };
    node_init_with_retention(&leader, 123, leader_dir, RETENTION_EVENTS);

    const char *key = "retention-pressure";
    uint64_t key_hash = vemb_v16_xxh3_64(key, strlen(key));
    uint32_t slot = 0;
    uint8_t value[VALUE_SIZE] = {1};
    assert(tlc_core_put(leader.core, key, strlen(key), key_hash, value,
                        sizeof(value), &slot) == 0);
    assert(wait_for_cold_durable(leader.cold, 1));
    tlc_cold_checkpoint_result_t initial_checkpoint;
    assert(tlc_core_publish_checkpoint(leader.core, 1, 7,
                                       &initial_checkpoint) == 0);

    int sockets[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    test_ub_rings_t rings;
    assert(test_ub_rings_create(&rings) == 0);
    tlc_ha_replica_config_t config = {
        .core = leader.core,
        .cold = leader.cold,
        .control_fd = sockets[0],
        .role = TLC_HA_REPLICA_LEADER,
        .tx_ring = rings.tx,
        .rx_ring = rings.rx,
        .queue_capacity = 32,
        .max_batch_events = 8,
        .max_batch_bytes = 65536,
        .hpc_node_id = 123,
        .peer_node_id = 124,
        .ha_term = 7,
        .topology_epoch = 1,
        .heartbeat_interval_ms = 10,
        .heartbeat_timeout_ms = 100,
    };
    tlc_ha_replica_t *replica = NULL;
    assert(tlc_ha_replica_start(&replica, &config) == 0);
    assert(tlc_ha_replica_begin_resync(replica, 12345, 1, 4096) == 0);

    for (uint32_t seq = 2; seq <= 9; seq++) {
        value[0] = (uint8_t)seq;
        assert(tlc_core_put(leader.core, key, strlen(key), key_hash, value,
                            sizeof(value), &slot) == 0);
    }
    assert(wait_for_cold_durable(leader.cold, 9));
    /* The active snapshot pin keeps the old segment containing tail [2,9]. */
    assert(wait_for_retention_floor(leader.cold, 1, 9));

    for (uint32_t seq = 10; seq <= HARD_SEQ; seq++) {
        value[0] = (uint8_t)seq;
        assert(tlc_core_put(leader.core, key, strlen(key), key_hash, value,
                            sizeof(value), &slot) == 0);
    }
    assert(wait_for_cold_durable(leader.cold, HARD_SEQ));
    /* 85% of C=16 aborts the pin holder, then compact advances the floor. */
    assert(wait_for_retention_compact(leader.cold, RETENTION_EVENTS));
    tlc_cold_retention_window_t window;
    assert(tlc_cold_get_retention_window(leader.cold, &window) == 0);
    assert(window.retained_floor_seq == 0 || window.retained_floor_seq > 1);

    tlc_ha_replica_stop(replica);
    close(sockets[1]);
    test_ub_rings_destroy(&rings);
    node_free(&leader);
}

static void run_network_test(void) {
    char leader_dir[] = "/tmp/tlc-ha-leader-XXXXXX";
    char follower_dir[] = "/tmp/tlc-ha-follower-XXXXXX";
    assert(mkdtemp(leader_dir) && mkdtemp(follower_dir));
    test_node_t leader = {0};
    test_node_t follower = {0};
    node_init(&leader, 101, leader_dir);
    node_init(&follower, 102, follower_dir);

    int sockets[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    test_ub_rings_t rings;
    assert(test_ub_rings_create(&rings) == 0);
    tlc_ha_replica_t *leader_replica = NULL;
    tlc_ha_replica_t *follower_replica = NULL;
    tlc_ha_replica_config_t leader_config = {
        .core = leader.core,
        .cold = leader.cold,
        .control_fd = sockets[0],
        .role = TLC_HA_REPLICA_LEADER,
        .tx_ring = rings.tx,
        .rx_ring = rings.rx,
        .queue_capacity = 32,
        .max_batch_events = 8,
        .max_batch_bytes = 65536,
        .hpc_node_id = 111,
        .peer_node_id = 112,
        .ha_term = 7,
        .heartbeat_interval_ms = 10,
        .heartbeat_timeout_ms = 100,
        .resync_timeout_ms = 200,
    };
    tlc_ha_replica_config_t follower_config = leader_config;
    follower_config.core = follower.core;
    follower_config.control_fd = sockets[1];
    follower_config.role = TLC_HA_REPLICA_FOLLOWER;
    follower_config.cold = follower.cold;
    follower_config.hpc_node_id = 112;
    follower_config.peer_node_id = 111;
    follower_config.tx_ring = rings.rx;
    follower_config.rx_ring = rings.tx;
    assert(tlc_ha_replica_start(&follower_replica, &follower_config) == 0);
    /* A restarted Follower can wait for a replay before a Leader attaches.
     * Its apply worker must not interpret the empty initial queue as EOF. */
    usleep(2000);
    assert(tlc_ha_replica_start(&leader_replica, &leader_config) == 0);
    /* Re-announce the current owner to exercise the M10 control path without
     * changing the term used by the resync fixtures below. */
    assert(tlc_ha_replica_transition_role(leader_replica,
                                          TLC_HA_REPLICA_LEADER,
                                          TLC_HA_REPLICA_STATE_MASTER, 7) == 0);
    for (uint32_t i = 0; i < 100 &&
         !tlc_ha_replica_leader_announce_acked(leader_replica); i++)
        usleep(1000);
    assert(tlc_ha_replica_leader_announce_acked(leader_replica));
    assert(tlc_ha_replica_role(follower_replica) ==
           TLC_HA_REPLICA_FOLLOWER);
    assert(tlc_ha_replica_ha_state(follower_replica) ==
           TLC_HA_REPLICA_STATE_BACKUP);

    enum { RESYNC_BLOB_BYTES = 200000 };
    uint8_t *resync_blob = zmalloc(RESYNC_BLOB_BYTES);
    assert(resync_blob != NULL);
    for (uint32_t i = 0; i < RESYNC_BLOB_BYTES; i++)
        resync_blob[i] = (uint8_t)(i * 29u);
    tlc_cold_resync_snapshot_t snapshot = {
        .checkpoint_blob = resync_blob,
        .checkpoint_blob_bytes = RESYNC_BLOB_BYTES,
        .meta_shard_count = 256,
        .checkpoint = {
            .generation = 9,
            .ha_term = 7,
            .checkpoint_seq = 41,
        },
        .checkpoint_blob_checksum = vemb_v16_xxh3_64(resync_blob,
                                                      RESYNC_BLOB_BYTES),
        .captured_seq_checksum = 0x1234,
        .tail_start_seq = 42,
        .durable_boundary_seq = 99,
    };
    assert(tlc_ha_replica_send_resync_snapshot(leader_replica, 5001, 3,
                                                &snapshot, 32768) == 0);
    char received_path[PATH_MAX] = {0};
    size_t received_blob_bytes = 0;
    tlc_ha_resync_snapshot_begin_t received_begin;
    for (uint32_t attempt = 0; attempt < 3000 && !received_path[0]; attempt++) {
        tlc_ha_replica_take_resync_snapshot(follower_replica, received_path,
                                            sizeof(received_path), &received_blob_bytes,
                                            &received_begin);
        if (!received_path[0])
            usleep(1000);
    }
    assert(received_path[0] != '\0');
    assert(received_blob_bytes == RESYNC_BLOB_BYTES);
    assert(received_begin.session_id == 5001);
    assert(received_begin.topology_epoch == 3);
    uint8_t *received_blob = test_read_file(received_path, received_blob_bytes);
    assert(memcmp(received_blob, resync_blob, RESYNC_BLOB_BYTES) == 0);
    zfree(received_blob);
    assert(unlink(received_path) == 0);
    zfree(resync_blob);
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
    tlc_cold_progress_t leader_progress;
    for (uint32_t attempt = 0; attempt < 3000; attempt++) {
        assert(tlc_cold_get_progress(leader.cold, &leader_progress) == 0);
        if (leader_progress.durable_seq >= 1)
            break;
        usleep(1000);
    }
    assert(leader_progress.durable_seq >= 1);
    tlc_cold_checkpoint_result_t checkpoint;
    assert(tlc_core_publish_checkpoint(leader.core, 1, 7, &checkpoint) == 0);
    tlc_cold_resync_snapshot_t install_snapshot;
    assert(tlc_cold_begin_resync_snapshot(leader.cold, 256,
                                          &install_snapshot) == 0);
    assert(tlc_ha_replica_send_resync_snapshot(leader_replica, 5002, 4,
                                                &install_snapshot, 32768) == 0);
    char install_path[PATH_MAX];
    int install_path_bytes = snprintf(install_path, sizeof(install_path),
                                      "%s.resync/%016llx/checkpoint.blob",
                                      follower_dir, (unsigned long long)5002);
    assert(install_path_bytes > 0 &&
           (size_t)install_path_bytes < sizeof(install_path));
    tlc_cold_checkpoint_result_t installed;
    int install_rc = -1;
    for (uint32_t attempt = 0; attempt < 3000 && install_rc != 0; attempt++) {
        install_rc = tlc_ha_replica_install_resync_snapshot(follower_replica,
                                                             &installed);
        if (install_rc != 0)
            usleep(1000);
    }
    assert(install_rc == 0);
    assert(installed.generation == checkpoint.generation);
    assert(installed.checkpoint_seq == checkpoint.checkpoint_seq);
    assert(access(install_path, F_OK) != 0);
    assert(wait_for_value(follower.core, key, value));
    tlc_cold_end_resync_snapshot(leader.cold, &install_snapshot);

    const char *tail_key = "m5-tail-key";
    const uint8_t tail_value[VALUE_SIZE] = {2, 4, 6, 8, 10, 12, 14, 16};
    uint64_t tail_hash = vemb_v16_xxh3_64(tail_key, strlen(tail_key));
    assert(tlc_core_put(leader.core, tail_key, strlen(tail_key), tail_hash,
                        tail_value, sizeof(tail_value), &warm_slot) == 0);
    assert(wait_for_cold_durable(leader.cold, 2));
    assert(tlc_ha_replica_begin_resync(leader_replica, 5003, 5, 32768) == 0);

    const char *late_key = "m5-late-key";
    const uint8_t late_value[VALUE_SIZE] = {16, 14, 12, 10, 8, 6, 4, 2};
    uint64_t late_hash = vemb_v16_xxh3_64(late_key, strlen(late_key));
    assert(tlc_core_put(leader.core, late_key, strlen(late_key), late_hash,
                        late_value, sizeof(late_value), &warm_slot) == 0);
    assert(wait_for_cold_durable(leader.cold, 3));

    /* M6 control worker consumes the verified artifact and installs it. */
    assert(wait_for_value(follower.core, tail_key, tail_value));
    assert(wait_for_value(follower.core, late_key, late_value));

    const char *post_handoff_key = "m5-post-handoff-key";
    const uint8_t post_handoff_value[VALUE_SIZE] =
        {17, 15, 13, 11, 9, 7, 5, 3};
    uint64_t post_handoff_hash = vemb_v16_xxh3_64(post_handoff_key,
                                                   strlen(post_handoff_key));
    assert(tlc_core_put(leader.core, post_handoff_key,
                        strlen(post_handoff_key), post_handoff_hash,
                        post_handoff_value, sizeof(post_handoff_value),
                        &warm_slot) == 0);
    assert(wait_for_value(follower.core, post_handoff_key,
                          post_handoff_value));
    assert(wait_for_cold_durable(follower.cold, 4));
    assert(tlc_ha_replica_peer_accepted_seq(leader_replica) >= 4);

    assert(tlc_ha_replica_begin_resync(leader_replica, 5004, 6, 32768) == 0);
    usleep(500000);
    char aborted_path[PATH_MAX];
    size_t aborted_bytes = 0;
    tlc_ha_resync_snapshot_begin_t aborted_begin;
    assert(tlc_ha_replica_take_resync_snapshot(follower_replica, aborted_path,
                                                sizeof(aborted_path),
                                                &aborted_bytes,
                                                &aborted_begin) != 0);
    assert(tlc_ha_replica_begin_resync(leader_replica, 5005, 7, 32768) == 0);
    assert(tlc_ha_replica_abort_resync(leader_replica) == 0);
    usleep(20000);
    tlc_ha_replica_stop(leader_replica);
    tlc_ha_replica_stop(follower_replica);
    test_ub_rings_destroy(&rings);
    node_free(&leader);
    node_free(&follower);
}

static void run_automatic_gap_resync_test(void) {
    char leader_dir[] = "/tmp/tlc-ha-auto-leader-XXXXXX";
    char follower_dir[] = "/tmp/tlc-ha-auto-follower-XXXXXX";
    assert(mkdtemp(leader_dir) && mkdtemp(follower_dir));
    test_node_t leader = {0};
    test_node_t follower = {0};
    node_init(&leader, 109, leader_dir);
    node_init(&follower, 110, follower_dir);
    assert(tlc_core_set_ha_term(leader.core, 7) == 0);

    const char *first_key = "auto-gap-first";
    const char *second_key = "auto-gap-second";
    const char *third_key = "auto-gap-third";
    const uint8_t first_value[VALUE_SIZE] = {1, 1, 1, 1, 1, 1, 1, 1};
    const uint8_t second_value[VALUE_SIZE] = {2, 2, 2, 2, 2, 2, 2, 2};
    const uint8_t third_value[VALUE_SIZE] = {3, 3, 3, 3, 3, 3, 3, 3};
    uint32_t slot = 0;
    assert(tlc_core_put(leader.core, first_key, strlen(first_key),
                        vemb_v16_xxh3_64(first_key, strlen(first_key)),
                        first_value, sizeof(first_value), &slot) == 0);
    assert(tlc_core_put(leader.core, second_key, strlen(second_key),
                        vemb_v16_xxh3_64(second_key, strlen(second_key)),
                        second_value, sizeof(second_value), &slot) == 0);
    assert(wait_for_cold_durable(leader.cold, 2));

    int sockets[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    test_ub_rings_t rings;
    assert(test_ub_rings_create(&rings) == 0);
    tlc_ha_replica_config_t leader_config = {
        .core = leader.core,
        .cold = leader.cold,
        .control_fd = sockets[0],
        .role = TLC_HA_REPLICA_LEADER,
        .tx_ring = rings.tx,
        .rx_ring = rings.rx,
        .queue_capacity = 32,
        .max_batch_events = 8,
        .max_batch_bytes = 65536,
        .hpc_node_id = 111,
        .peer_node_id = 112,
        .ha_term = 7,
        .topology_epoch = 1,
        .heartbeat_interval_ms = 10,
        .heartbeat_timeout_ms = 100,
        .resync_timeout_ms = 500,
    };
    tlc_ha_replica_config_t follower_config = leader_config;
    follower_config.core = follower.core;
    follower_config.cold = follower.cold;
    follower_config.control_fd = sockets[1];
    follower_config.role = TLC_HA_REPLICA_FOLLOWER;
    follower_config.hpc_node_id = 112;
    follower_config.peer_node_id = 111;
    follower_config.tx_ring = rings.rx;
    follower_config.rx_ring = rings.tx;
    tlc_ha_replica_t *leader_replica = NULL;
    tlc_ha_replica_t *follower_replica = NULL;
    assert(tlc_ha_replica_start(&follower_replica, &follower_config) == 0);
    assert(tlc_ha_replica_start(&leader_replica, &leader_config) == 0);

    /* Follower starts at seq 0; replaying only seq 2 forces GAP repair.
     * No checkpoint is published: repair must come from the retained AOF. */
    assert(tlc_ha_replica_replay_from(leader_replica, 2, 2) == 0);
    assert(wait_for_value(follower.core, first_key, first_value));
    assert(wait_for_value(follower.core, second_key, second_value));
    for (uint32_t attempt = 0; attempt < 3000 &&
         tlc_ha_replica_peer_accepted_seq(leader_replica) < 2; attempt++)
        usleep(1000);
    assert(tlc_ha_replica_peer_accepted_seq(leader_replica) >= 2);

    assert(tlc_core_put(leader.core, third_key, strlen(third_key),
                        vemb_v16_xxh3_64(third_key, strlen(third_key)),
                        third_value, sizeof(third_value), &slot) == 0);
    assert(wait_for_value(follower.core, third_key, third_value));
    tlc_ha_replica_stop(leader_replica);
    tlc_ha_replica_stop(follower_replica);
    test_ub_rings_destroy(&rings);
    node_free(&leader);
    node_free(&follower);
}

static void run_automatic_retention_resync_test(void) {
    char leader_dir[] = "/tmp/tlc-ha-retention-leader-XXXXXX";
    char follower_dir[] = "/tmp/tlc-ha-retention-follower-XXXXXX";
    assert(mkdtemp(leader_dir) && mkdtemp(follower_dir));
    test_node_t leader = {0};
    test_node_t follower = {0};
    node_init(&leader, 111, leader_dir);
    node_init(&follower, 112, follower_dir);

    const char *key = "auto-retention-key";
    uint64_t key_hash = vemb_v16_xxh3_64(key, strlen(key));
    uint32_t slot = 0;
    uint8_t value[VALUE_SIZE] = {0};
    for (uint32_t seq = 1; seq <= 96; seq++) {
        value[0] = (uint8_t)seq;
        assert(tlc_core_put(leader.core, key, strlen(key), key_hash, value,
                            sizeof(value), &slot) == 0);
    }
    assert(wait_for_cold_durable(leader.cold, 96));
    tlc_cold_checkpoint_result_t checkpoint;
    assert(tlc_core_publish_checkpoint(leader.core, 96, 7, &checkpoint) == 0);
    value[0] = 0x61;
    assert(tlc_core_put(leader.core, key, strlen(key), key_hash, value,
                        sizeof(value), &slot) == 0);
    assert(wait_for_cold_durable(leader.cold, 97));
    assert(tlc_cold_compact(leader.cold, checkpoint.checkpoint_seq,
                            UINT64_MAX, 1) == 0);

    int sockets[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    test_ub_rings_t rings;
    assert(test_ub_rings_create(&rings) == 0);
    tlc_ha_replica_config_t leader_config = {
        .core = leader.core,
        .cold = leader.cold,
        .control_fd = sockets[0],
        .role = TLC_HA_REPLICA_LEADER,
        .tx_ring = rings.tx,
        .rx_ring = rings.rx,
        .queue_capacity = 32,
        .max_batch_events = 8,
        .max_batch_bytes = 65536,
        .hpc_node_id = 111,
        .peer_node_id = 112,
        .ha_term = 7,
        .topology_epoch = 1,
        .heartbeat_interval_ms = 10,
        .heartbeat_timeout_ms = 100,
        .resync_timeout_ms = 1000,
    };
    tlc_ha_replica_config_t follower_config = leader_config;
    follower_config.core = follower.core;
    follower_config.cold = follower.cold;
    follower_config.control_fd = sockets[1];
    follower_config.role = TLC_HA_REPLICA_FOLLOWER;
    follower_config.hpc_node_id = 112;
    follower_config.peer_node_id = 111;
    follower_config.tx_ring = rings.rx;
    follower_config.rx_ring = rings.tx;
    tlc_ha_replica_t *leader_replica = NULL;
    tlc_ha_replica_t *follower_replica = NULL;
    assert(tlc_ha_replica_start(&follower_replica, &follower_config) == 0);
    assert(tlc_ha_replica_start(&leader_replica, &leader_config) == 0);

    /* seq 98 makes the empty Follower request [1,98]. Compaction removed
     * that prefix, so automatic recovery must install checkpoint 96. */
    value[0] = 0x62;
    assert(tlc_core_put(leader.core, key, strlen(key), key_hash, value,
                        sizeof(value), &slot) == 0);
    assert(wait_for_value(follower.core, key, value));
    assert(wait_for_cold_durable(follower.cold, 98));
    for (uint32_t attempt = 0; attempt < 10000 &&
         tlc_ha_replica_peer_accepted_seq(leader_replica) < 98; attempt++)
        usleep(1000);
    assert(tlc_ha_replica_peer_accepted_seq(leader_replica) >= 98);
    tlc_ha_replica_stop(leader_replica);
    tlc_ha_replica_stop(follower_replica);
    test_ub_rings_destroy(&rings);
    node_free(&leader);
    node_free(&follower);
}

static tlc_ha_replica_t *start_heartbeat_probe(test_node_t *node, int fd,
                                                test_ub_rings_t *rings) {
    tlc_ha_replica_config_t config = {
        .core = node->core,
        .cold = node->cold,
        .control_fd = fd,
        .role = TLC_HA_REPLICA_LEADER,
        .tx_ring = rings->tx,
        .rx_ring = rings->rx,
        .queue_capacity = 8,
        .max_batch_events = 4,
        .max_batch_bytes = 4096,
        .hpc_node_id = 111,
        .peer_node_id = 112,
        .ha_term = 7,
        .heartbeat_interval_ms = 10,
        .heartbeat_timeout_ms = 50,
        .heartbeat_backoff_max_ms = 20,
    };
    tlc_ha_replica_t *replica = NULL;
    assert(tlc_ha_replica_start(&replica, &config) == 0);
    return replica;
}

static tlc_ha_replica_t *start_follower_heartbeat_probe(test_node_t *node,
                                                         int fd,
                                                         test_ub_rings_t *rings) {
    tlc_ha_replica_config_t config = {
        .core = node->core,
        .cold = node->cold,
        .control_fd = fd,
        .role = TLC_HA_REPLICA_FOLLOWER,
        .tx_ring = rings->tx,
        .rx_ring = rings->rx,
        .queue_capacity = 4,
        .max_batch_events = 4,
        .max_batch_bytes = 4096,
        .hpc_node_id = 112,
        .peer_node_id = 111,
        .ha_term = 7,
        .heartbeat_interval_ms = 10,
        .heartbeat_timeout_ms = 50,
        .heartbeat_backoff_max_ms = 20,
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
        test_ub_rings_t rings;
        assert(test_ub_rings_create(&rings) == 0);
        tlc_ha_replica_t *replica = start_heartbeat_probe(&node, sockets[0],
                                                          &rings);
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
        test_ub_rings_destroy(&rings);
        node_free(&node);
    }

    for (uint32_t case_id = 0; case_id < 3; case_id++) {
        char directory[] = "/tmp/tlc-ha-heartbeat-invalid-XXXXXX";
        assert(mkdtemp(directory));
        test_node_t node = {0};
        node_init(&node, 104 + case_id, directory);
        int sockets[2];
        assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
        test_ub_rings_t rings;
        assert(test_ub_rings_create(&rings) == 0);
        tlc_ha_replica_t *replica = start_heartbeat_probe(&node, sockets[0],
                                                          &rings);
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
        test_ub_rings_destroy(&rings);
        node_free(&node);
    }

    {
        char directory[] = "/tmp/tlc-ha-heartbeat-timeout-XXXXXX";
        assert(mkdtemp(directory));
        test_node_t node = {0};
        node_init(&node, 108, directory);
        int sockets[2];
        assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
        test_ub_rings_t rings;
        assert(test_ub_rings_create(&rings) == 0);
        tlc_ha_replica_t *replica = start_heartbeat_probe(&node, sockets[0],
                                                          &rings);
        for (uint32_t i = 0; i < 100 &&
             tlc_ha_replica_heartbeat_missed_count(replica) == 0; i++)
            usleep(1000);
        assert(tlc_ha_replica_heartbeat_missed_count(replica) == 1);
        assert(tlc_ha_replica_ha_state(replica) ==
               TLC_HA_REPLICA_STATE_MASTER);
        assert(!tlc_ha_replica_heartbeat_failure_pending(replica));
        usleep(100000);
        assert(tlc_ha_replica_peer_health(replica) ==
               TLC_HA_REPLICA_UNAVAILABLE);
        assert(!tlc_ha_replica_peer_liveness(replica));
        assert(tlc_ha_replica_ha_state(replica) ==
               TLC_HA_REPLICA_STATE_MASTER);
        assert(tlc_ha_replica_heartbeat_missed_count(replica) >= 3);
        assert(!tlc_ha_replica_heartbeat_failure_pending(replica));
        test_send_heartbeat(sockets[1], 112, 111, 7, 1, 1, 0);
        for (uint32_t i = 0; i < 100 &&
             tlc_ha_replica_peer_health(replica) !=
                 TLC_HA_REPLICA_HEALTHY; i++)
            usleep(1000);
        assert(tlc_ha_replica_peer_health(replica) ==
               TLC_HA_REPLICA_HEALTHY);
        for (uint32_t i = 0; i < 100 &&
             !tlc_ha_replica_peer_liveness(replica); i++)
            usleep(1000);
        assert(tlc_ha_replica_peer_liveness(replica));
        assert(tlc_ha_replica_heartbeat_missed_count(replica) == 0);
        assert(tlc_ha_replica_ha_state(replica) ==
               TLC_HA_REPLICA_STATE_MASTER);
        assert(!tlc_ha_replica_heartbeat_failure_pending(replica));
        tlc_ha_replica_stop(replica);
        close(sockets[1]);
        test_ub_rings_destroy(&rings);
        node_free(&node);
    }
}

static void run_stream_reconnect_test(void) {
    char directory[] = "/tmp/tlc-ha-reconnect-XXXXXX";
    assert(mkdtemp(directory));
    test_node_t node = {0};
    node_init(&node, 113, directory);
    int old_sockets[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, old_sockets) == 0);
    test_ub_rings_t rings;
    assert(test_ub_rings_create(&rings) == 0);
    tlc_ha_replica_t *replica = start_heartbeat_probe(&node, old_sockets[0],
                                                      &rings);
    close(old_sockets[1]);
    usleep(30000);

    int new_sockets[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, new_sockets) == 0);
    assert(tlc_ha_replica_reconnect(replica, new_sockets[0], 2) == 0);
    assert(tlc_ha_replica_connection_epoch(replica) == 2);
    test_send_heartbeat(new_sockets[1], 112, 111, 7, 1, 1, 0);
    for (uint32_t i = 0; i < 100 &&
         !tlc_ha_replica_peer_liveness(replica); i++)
        usleep(1000);
    assert(tlc_ha_replica_peer_liveness(replica));

    tlc_ha_replica_stop(replica);
    close(new_sockets[1]);
    test_ub_rings_destroy(&rings);
    node_free(&node);
}

static void run_external_failover_guard_test(void) {
    char directory[] = "/tmp/tlc-ha-auto-promotion-XXXXXX";
    assert(mkdtemp(directory));
    test_node_t node = {0};
    node_init(&node, 114, directory);
    int sockets[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    test_ub_rings_t rings;
    assert(test_ub_rings_create(&rings) == 0);
    tlc_ha_replica_t *replica = start_follower_heartbeat_probe(&node,
                                                                sockets[0],
                                                                &rings);
    close(sockets[1]);
    for (uint32_t i = 0; i < 500 &&
         tlc_ha_replica_heartbeat_missed_count(replica) < 3; i++)
        usleep(1000);
    assert(tlc_ha_replica_role(replica) == TLC_HA_REPLICA_FOLLOWER);
    assert(tlc_ha_replica_ha_state(replica) == TLC_HA_REPLICA_STATE_BACKUP);
    assert(tlc_ha_replica_ha_term(replica) == 7);
    assert(tlc_core_ha_term(node.core) == 7);
    assert(!tlc_ha_replica_heartbeat_failure_pending(replica));
    assert(!tlc_ha_replica_leader_announce_acked(replica));
    tlc_ha_replica_stop(replica);
    test_ub_rings_destroy(&rings);
    node_free(&node);
}

static void run_external_promote_apply_lag_timeout_test(void) {
    char directory[] = "/tmp/tlc-ha-promote-lag-XXXXXX";
    assert(mkdtemp(directory));
    test_node_t node = {0};
    node_init(&node, 117, directory);
    int sockets[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    test_ub_rings_t rings;
    assert(test_ub_rings_create(&rings) == 0);
    tlc_ha_replica_t *replica = start_follower_heartbeat_probe(&node,
                                                                sockets[0],
                                                                &rings);

    /* Append a durable replica event without routing it through the apply
     * queue. This models a Follower whose COLD append has completed while
     * its Core apply cursor is still behind. */
    const uint8_t value[VALUE_SIZE] = {1, 2, 3, 4, 5, 6, 7, 8};
    const char key[] = "promote-lag";
    tlc_cold_event_input_t event = {
        .ha_term = 7,
        .topology_epoch = 1,
        .op = TLC_COLD_OP_PUT,
        .meta_shard_id = 0,
        .version = 1,
        .key = key,
        .key_len = (uint32_t)(sizeof(key) - 1u),
        .value = value,
        .value_len = sizeof(value),
    };
    tlc_cold_replica_batch_status_t batch_status;
    uint64_t durable_seq = 0;
    assert(tlc_cold_submit_replica_batch(node.cold, 1, &event, 1,
                                         TLC_COLD_ACK_DURABLE,
                                         &batch_status, &durable_seq) == 0);
    assert(batch_status == TLC_COLD_REPLICA_BATCH_APPLIED);
    assert(durable_seq == 1);

    int rc = tlc_ha_replica_external_promote(replica);
    assert(rc == TLC_HA_REPLICA_ERR_APPLY_LAG);
    assert(tlc_ha_replica_role(replica) == TLC_HA_REPLICA_FOLLOWER);
    assert(tlc_ha_replica_ha_state(replica) == TLC_HA_REPLICA_STATE_BACKUP);
    assert(tlc_ha_replica_write_fenced(replica));

    tlc_ha_replica_stop(replica);
    close(sockets[1]);
    test_ub_rings_destroy(&rings);
    node_free(&node);
}

static void run_reconnect_leader_announce_test(void) {
    char leader_dir[] = "/tmp/tlc-ha-reconnect-leader-XXXXXX";
    char follower_dir[] = "/tmp/tlc-ha-reconnect-follower-XXXXXX";
    assert(mkdtemp(leader_dir) && mkdtemp(follower_dir));
    test_node_t leader = {0};
    test_node_t follower = {0};
    node_init(&leader, 115, leader_dir);
    node_init(&follower, 116, follower_dir);
    assert(tlc_core_set_ha_term(leader.core, 8) == 0);
    const uint8_t recovered_first[VALUE_SIZE] = {4, 4, 4, 4, 4, 4, 4, 4};
    const uint8_t recovered_second[VALUE_SIZE] = {5, 5, 5, 5, 5, 5, 5, 5};
    uint32_t recovered_slot = 0;
    assert(tlc_core_put(leader.core, "reconnect-first", 15,
                        vemb_v16_xxh3_64("reconnect-first", 15),
                        recovered_first, sizeof(recovered_first),
                        &recovered_slot) == 0);
    assert(tlc_core_put(leader.core, "reconnect-second", 16,
                        vemb_v16_xxh3_64("reconnect-second", 16),
                        recovered_second, sizeof(recovered_second),
                        &recovered_slot) == 0);
    assert(wait_for_cold_durable(leader.cold, 2));
    int sockets[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    test_ub_rings_t rings;
    assert(test_ub_rings_create(&rings) == 0);
    tlc_ha_replica_config_t leader_config = {
        .core = leader.core,
        .cold = leader.cold,
        .control_fd = sockets[0],
        .role = TLC_HA_REPLICA_LEADER,
        .tx_ring = rings.tx,
        .rx_ring = rings.rx,
        .queue_capacity = 8,
        .max_batch_events = 4,
        .max_batch_bytes = 4096,
        .hpc_node_id = 111,
        .peer_node_id = 112,
        .ha_term = 8,
        .heartbeat_interval_ms = 10,
        .heartbeat_timeout_ms = 50,
        .heartbeat_backoff_max_ms = 20,
    };
    tlc_ha_replica_config_t follower_config = leader_config;
    follower_config.core = follower.core;
    follower_config.cold = follower.cold;
    follower_config.control_fd = sockets[1];
    follower_config.role = TLC_HA_REPLICA_FOLLOWER;
    follower_config.hpc_node_id = 112;
    follower_config.peer_node_id = 111;
    follower_config.ha_term = 7;
    follower_config.heartbeat_timeout_ms = 1000;
    follower_config.tx_ring = rings.rx;
    follower_config.rx_ring = rings.tx;
    tlc_ha_replica_t *leader_replica = NULL;
    tlc_ha_replica_t *follower_replica = NULL;
    assert(tlc_ha_replica_start(&leader_replica, &leader_config) == 0);
    assert(tlc_ha_replica_start(&follower_replica, &follower_config) == 0);

    shutdown(sockets[0], SHUT_RDWR);
    shutdown(sockets[1], SHUT_RDWR);
    usleep(100000);
    int reconnect_sockets[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, reconnect_sockets) == 0);
    assert(tlc_ha_replica_reconnect(follower_replica,
                                    reconnect_sockets[1], 2) == 0);
    assert(tlc_ha_replica_reconnect(leader_replica,
                                    reconnect_sockets[0], 2) == 0);
    for (uint32_t i = 0; i < 100 &&
         !tlc_ha_replica_leader_announce_acked(leader_replica); i++)
        usleep(1000);
    assert(tlc_ha_replica_role(leader_replica) == TLC_HA_REPLICA_LEADER);
    assert(tlc_ha_replica_ha_state(leader_replica) ==
           TLC_HA_REPLICA_STATE_MASTER);
    assert(tlc_ha_replica_leader_announce_acked(leader_replica));
    assert(wait_for_value(follower.core, "reconnect-first", recovered_first));
    assert(wait_for_value(follower.core, "reconnect-second", recovered_second));
    assert(tlc_core_ha_term(follower.core) == 8);

    tlc_ha_replica_stop(leader_replica);
    tlc_ha_replica_stop(follower_replica);
    test_ub_rings_destroy(&rings);
    node_free(&leader);
    node_free(&follower);
}

int main(void) {
    assert(monotonicInit() != NULL);
    test_resync_snapshot_assembler();
    run_network_test();
    run_automatic_retention_maintenance_test();
    run_retention_pressure_abort_test();
    run_automatic_gap_resync_test();
    run_automatic_retention_resync_test();
    run_heartbeat_error_tests();
    run_stream_reconnect_test();
    run_external_failover_guard_test();
    run_external_promote_apply_lag_timeout_test();
    run_reconnect_leader_announce_test();
    printf("tlc_ha_replica_ut: PASS\n");
    return 0;
}
