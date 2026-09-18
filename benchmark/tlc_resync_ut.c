#include "tlc_core.h"
#include "monotonic.h"
#include "vemb_v16_hash.h"
#include "zmalloc.h"

#include <assert.h>
#include <dirent.h>
#include <fcntl.h>
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

static void assert_absent(tlc_core_t *core, const char *key) {
    uint64_t hash = vemb_v16_xxh3_64(key, strlen(key));
    tlc_warm_location_t location;
    assert(tlc_core_get_warm_location(core, key, strlen(key), hash,
                                      &location) != 0);
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

static void append_cold_event(tlc_cold_t *cold, uint64_t seq) {
    char key[32];
    uint8_t value[VALUE_SIZE];
    int key_len = snprintf(key, sizeof(key), "snapshot-tail-%llu",
                           (unsigned long long)seq);
    assert(key_len > 0 && (size_t)key_len < sizeof(key));
    memset(value, (int)seq, sizeof(value));
    tlc_cold_event_input_t event = {
        .ha_term = 7,
        .topology_epoch = 1,
        .op = TLC_COLD_OP_PUT,
        .meta_shard_id = seq % META_SHARD_COUNT,
        .version = seq,
        .key = key,
        .key_len = (uint32_t)key_len,
        .value = value,
        .value_len = sizeof(value),
    };
    uint64_t appended = 0;
    assert(tlc_cold_submit(cold, &event, TLC_COLD_ACK_DURABLE,
                           &appended) == 0);
    assert(appended == seq);
}

static uint32_t count_aof_segments(const char *directory) {
    DIR *dir = opendir(directory);
    assert(dir != NULL);
    uint32_t count = 0;
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strncmp(entry->d_name, "aof-", 4) == 0)
            count++;
    }
    assert(closedir(dir) == 0);
    return count;
}

static void write_file(const char *path, const void *data, size_t bytes) {
    int fd = open(path, O_CREAT | O_EXCL | O_WRONLY, 0600);
    assert(fd >= 0);
    const uint8_t *cursor = data;
    while (bytes != 0) {
        ssize_t written = write(fd, cursor, bytes);
        assert(written > 0);
        cursor += (size_t)written;
        bytes -= (size_t)written;
    }
    assert(close(fd) == 0);
}

typedef struct snapshot_read_check {
    uint64_t expected_seq;
} snapshot_read_check_t;

static int check_snapshot_event(const tlc_cold_event_input_t *input,
                                uint64_t seq, void *arg) {
    snapshot_read_check_t *check = arg;
    assert(seq == check->expected_seq++);
    assert(input->version == seq);
    return 0;
}

static void test_resync_snapshot_session(void) {
    char directory[] = "/tmp/tlc-resync-session-XXXXXX";
    assert(mkdtemp(directory));
    test_node_t node = {0};
    node_init(&node, 15, directory);
    put_value(node.core, "snapshot-base-1", 1);
    put_value(node.core, "snapshot-base-2", 11);
    put_value(node.core, "snapshot-base-3", 21);
    wait_durable(node.core, 3);

    tlc_cold_t *cold = tlc_core_get_cold(node.core);
    tlc_cold_checkpoint_result_t checkpoint_one;
    assert(tlc_core_publish_checkpoint(node.core, 1, 7,
                                       &checkpoint_one) == 0);
    assert(checkpoint_one.checkpoint_seq == 3);
    for (uint64_t seq = 4; seq <= 123; seq++)
        append_cold_event(cold, seq);

    tlc_cold_resync_snapshot_t snapshot;
    assert(tlc_cold_begin_resync_snapshot(cold, META_SHARD_COUNT,
                                          &snapshot) == 0);
    assert(snapshot.checkpoint.generation == checkpoint_one.generation);
    assert(snapshot.tail_start_seq == 4);
    assert(snapshot.durable_boundary_seq == 123);
    assert(snapshot.checkpoint_blob != NULL);
    assert(snapshot.captured_seq != NULL);
    append_cold_event(cold, 124);

    uint64_t next_seq = snapshot.tail_start_seq - 1u;
    uint32_t event_count = 0;
    assert(tlc_cold_read_resync_snapshot(cold, &snapshot, &next_seq, 11,
                                         check_snapshot_event, NULL,
                                         &event_count) != 0);
    next_seq = snapshot.tail_start_seq;
    snapshot_read_check_t check = {.expected_seq = next_seq};
    do {
        assert(tlc_cold_read_resync_snapshot(cold, &snapshot, &next_seq, 11,
                                             check_snapshot_event, &check,
                                             &event_count) == 0);
        assert(event_count > 0);
    } while (next_seq <= snapshot.durable_boundary_seq);
    assert(next_seq == snapshot.durable_boundary_seq + 1u);
    assert(check.expected_seq == 124);
    assert(tlc_cold_read_resync_snapshot(cold, &snapshot, &next_seq, 11,
                                         check_snapshot_event, &check,
                                         &event_count) == 0);
    assert(event_count == 0);

    tlc_cold_checkpoint_result_t checkpoint_two;
    assert(tlc_core_publish_checkpoint(node.core, 2, 7,
                                       &checkpoint_two) == 0);
    assert(checkpoint_two.checkpoint_seq == 124);
    uint32_t before_compact = count_aof_segments(directory);
    assert(tlc_cold_compact(cold, checkpoint_two.checkpoint_seq,
                            UINT64_MAX, 1) == 0);
    assert(count_aof_segments(directory) == before_compact);
    tlc_cold_end_resync_snapshot(cold, &snapshot);
    assert(tlc_cold_compact(cold, checkpoint_two.checkpoint_seq,
                            UINT64_MAX, 1) == 0);
    assert(count_aof_segments(directory) < before_compact);
    node_free(&node);
}

static void test_checkpointed_event_skips_warm(void) {
    char directory[] = "/tmp/tlc-resync-checkpointed-XXXXXX";
    assert(mkdtemp(directory));
    test_node_t node = {0};
    node_init(&node, 14, directory);

    const char *key = "checkpointed-event";
    uint8_t value[VALUE_SIZE];
    memset(value, 0x5a, sizeof(value));
    uint64_t key_hash = vemb_v16_xxh3_64(key, strlen(key));
    uint64_t captured_seq[META_SHARD_COUNT];
    for (uint32_t i = 0; i < META_SHARD_COUNT; i++)
        captured_seq[i] = UINT64_MAX;
    tlc_cold_event_input_t event = {
        .ha_term = 7,
        .topology_epoch = 1,
        .op = TLC_COLD_OP_PUT,
        .meta_shard_id = vemb_v16_mix32_u64(key_hash) & (META_SHARD_COUNT - 1u),
        .version = 1,
        .key = key,
        .key_len = strlen(key),
        .value = value,
        .value_len = sizeof(value),
    };
    tlc_core_replica_apply_status_t status = TLC_CORE_REPLICA_APPLY_ERROR;
    assert(tlc_core_apply_resync_event(node.core, &event, 1, captured_seq,
                                       META_SHARD_COUNT, &status) == 0);
    assert(status == TLC_CORE_REPLICA_APPLY_CHECKPOINTED);
    tlc_warm_location_t location;
    assert(tlc_core_get_warm_location(node.core, key, strlen(key), key_hash,
                                      &location) != 0);
    node_free(&node);
}

int main(void) {
    assert(monotonicInit() != NULL);
    test_checkpointed_event_skips_warm();
    test_resync_snapshot_session();
    char leader_dir[] = "/tmp/tlc-resync-leader-XXXXXX";
    char follower_dir[] = "/tmp/tlc-resync-follower-XXXXXX";
    assert(mkdtemp(leader_dir) && mkdtemp(follower_dir));
    test_node_t leader = {0};
    test_node_t follower = {0};
    node_init(&leader, 11, leader_dir);
    node_init(&follower, 12, follower_dir);
    put_value(follower.core, "follower-stale-key", 91);
    wait_durable(follower.core, 1);
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

    void *bad_install_blob = zmalloc(checkpoint_blob_bytes);
    assert(bad_install_blob != NULL);
    memcpy(bad_install_blob, checkpoint_blob, checkpoint_blob_bytes);
    ((uint8_t *)bad_install_blob)[checkpoint_blob_bytes - 1] ^= 0x1;
    tlc_cold_checkpoint_result_t failed_install_result;
    assert(tlc_core_install_resync_checkpoint(follower.core, bad_install_blob,
                                              checkpoint_blob_bytes,
                                              &failed_install_result) != 0);
    assert_absent(follower.core, "follower-stale-key");
    zfree(bad_install_blob);

    tlc_cold_checkpoint_result_t installed_result;
    assert(tlc_core_install_resync_checkpoint(follower.core, checkpoint_blob,
                                              checkpoint_blob_bytes,
                                              &installed_result) == 0);
    assert(installed_result.generation == checkpoint.generation);
    assert(installed_result.checkpoint_seq == checkpoint.checkpoint_seq);
    assert_value(follower.core, "resync-key-1", 1);
    assert_value(follower.core, "resync-key-2", 11);
    assert_value(follower.core, "resync-key-3", 21);
    assert_absent(follower.core, "follower-stale-key");
    tlc_cold_progress_t installed_progress;
    assert(tlc_cold_get_progress(tlc_core_get_cold(follower.core),
                                 &installed_progress) == 0);
    assert(installed_progress.durable_seq == checkpoint.checkpoint_seq);

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

    char file_import_dir[] = "/tmp/tlc-resync-file-import-XXXXXX";
    char file_artifact_dir[] = "/tmp/tlc-resync-file-artifact-XXXXXX";
    assert(mkdtemp(file_import_dir) && mkdtemp(file_artifact_dir));
    test_node_t file_imported = {0};
    node_init(&file_imported, 16, file_import_dir);
    char corrupt_artifact[256];
    int corrupt_path_bytes = snprintf(corrupt_artifact,
                                      sizeof(corrupt_artifact),
                                      "%s/checkpoint.blob", file_artifact_dir);
    assert(corrupt_path_bytes > 0 &&
           (size_t)corrupt_path_bytes < sizeof(corrupt_artifact));
    void *file_corrupt_blob = zmalloc(checkpoint_blob_bytes);
    assert(file_corrupt_blob != NULL);
    memcpy(file_corrupt_blob, checkpoint_blob, checkpoint_blob_bytes);
    ((uint8_t *)file_corrupt_blob)[checkpoint_blob_bytes - 1] ^= 0x1;
    write_file(corrupt_artifact, file_corrupt_blob, checkpoint_blob_bytes);
    int corrupt_fd = open(corrupt_artifact, O_RDONLY);
    assert(corrupt_fd >= 0);
    assert(tlc_cold_import_checkpoint_file(
               tlc_core_get_cold(file_imported.core), META_SHARD_COUNT,
               corrupt_fd, corrupt_artifact, checkpoint_blob_bytes,
               &imported_result) != 0);
    assert(close(corrupt_fd) == 0);
    assert(access(corrupt_artifact, F_OK) == 0);
    zfree(file_corrupt_blob);

    char file_artifact[256];
    int file_path_bytes = snprintf(file_artifact, sizeof(file_artifact),
                                   "%s/checkpoint-good.blob", file_artifact_dir);
    assert(file_path_bytes > 0 && (size_t)file_path_bytes < sizeof(file_artifact));
    write_file(file_artifact, checkpoint_blob, checkpoint_blob_bytes);
    int file_fd = open(file_artifact, O_RDONLY);
    assert(file_fd >= 0);
    assert(tlc_cold_import_checkpoint_file(
               tlc_core_get_cold(file_imported.core), META_SHARD_COUNT,
               file_fd, file_artifact, checkpoint_blob_bytes,
               &imported_result) == 0);
    assert(close(file_fd) == 0);
    assert(access(file_artifact, F_OK) != 0);
    assert(tlc_core_recover_cold(file_imported.core) == 0);
    assert_value(file_imported.core, "resync-key-1", 1);
    assert_value(file_imported.core, "resync-key-2", 11);
    assert_value(file_imported.core, "resync-key-3", 21);
    node_free(&file_imported);

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
    assert_absent(follower.core, "follower-stale-key");
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
