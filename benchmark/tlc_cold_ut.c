#include "tlc_cold.h"
#include "vemb_v16_hash.h"

#include <assert.h>
#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>
#include <stddef.h>

#include "monotonic.h"

static int count_aof_segments(const char *directory) {
    DIR *dir = opendir(directory);
    assert(dir != NULL);
    int count = 0;
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strncmp(entry->d_name, "aof-", 4) == 0)
            count++;
    }
    closedir(dir);
    return count;
}

static int checkpoint_exists(const char *directory, uint64_t generation) {
    char path[4096];
    assert(snprintf(path, sizeof(path), "%s/checkpoint-%020llu", directory,
                    (unsigned long long)generation) < (int)sizeof(path));
    return access(path, R_OK) == 0;
}

static int count_replay_event(const tlc_cold_event_input_t *input,
                              uint64_t seq,
                              void *arg) {
    int *count = arg;
    assert(input != NULL);
    assert(seq >= 5 && seq <= 8);
    (*count)++;
    return 0;
}

static int count_checkpoint_record(uint32_t meta_shard_id,
                                   uint64_t captured_seq,
                                   const void *state,
                                   uint32_t state_len,
                                   void *arg) {
    int *count = arg;
    assert(meta_shard_id < 2);
    assert(captured_seq == 4);
    assert(state == NULL);
    assert(state_len == 0);
    (*count)++;
    return 0;
}

typedef struct test_cold_frame_header {
    uint32_t magic;
    uint16_t format_version;
    uint16_t header_bytes;
    uint64_t term;
    uint64_t seq;
    uint32_t op;
    uint32_t meta_shard_id;
    uint32_t key_len;
    uint32_t value_len;
    uint64_t version;
    uint64_t checksum;
} test_cold_frame_header_t;

static void rewrite_second_event_seq_gap(const char *directory) {
    char path[4096];
    assert(snprintf(path, sizeof(path), "%s/aof-%020d.log", directory, 0) <
           (int)sizeof(path));
    int fd = open(path, O_RDWR);
    assert(fd >= 0);
    test_cold_frame_header_t first;
    test_cold_frame_header_t second;
    assert(pread(fd, &first, sizeof(first), 0) == (ssize_t)sizeof(first));
    off_t second_offset = (off_t)sizeof(first) + first.key_len + first.value_len;
    assert(pread(fd, &second, sizeof(second), second_offset) ==
           (ssize_t)sizeof(second));
    uint8_t key[64];
    uint8_t value[64];
    assert(second.key_len <= sizeof(key) && second.value_len <= sizeof(value));
    assert(pread(fd, key, second.key_len,
                 second_offset + (off_t)sizeof(second)) ==
           (ssize_t)second.key_len);
    assert(pread(fd, value, second.value_len,
                 second_offset + (off_t)sizeof(second) + second.key_len) ==
           (ssize_t)second.value_len);
    second.seq = first.seq + 2;
    XXH3_state_t *state = XXH3_createState();
    assert(state != NULL);
    XXH3_64bits_reset(state);
    XXH3_64bits_update(state, &second, offsetof(test_cold_frame_header_t,
                                                checksum));
    XXH3_64bits_update(state, key, second.key_len);
    XXH3_64bits_update(state, value, second.value_len);
    second.checksum = XXH3_64bits_digest(state);
    XXH3_freeState(state);
    assert(pwrite(fd, &second, sizeof(second), second_offset) ==
           (ssize_t)sizeof(second));
    assert(fsync(fd) == 0);
    assert(close(fd) == 0);
}

static void test_strict_aof_seq_gap_rejected(void) {
    char directory[] = "/tmp/tlc-cold-seq-gap-XXXXXX";
    assert(mkdtemp(directory) != NULL);
    tlc_cold_config_t config = {
        .directory = directory, .segment_bytes = 4096, .queue_capacity = 4,
        .group_max_entries = 2, .group_max_delay_us = 1000};
    tlc_cold_t *cold = NULL;
    assert(tlc_cold_open(&cold, &config) == 0);
    for (uint32_t i = 0; i < 2; i++) {
        char key[32];
        char value[32];
        int key_len = snprintf(key, sizeof(key), "gap-key-%u", i);
        int value_len = snprintf(value, sizeof(value), "gap-value-%u", i);
        tlc_cold_event_input_t event = {
            .term = 1, .op = TLC_COLD_OP_PUT, .meta_shard_id = 0,
            .version = i + 1, .key = key, .key_len = (uint32_t)key_len,
            .value = value, .value_len = (uint32_t)value_len};
        assert(tlc_cold_submit(cold, &event, TLC_COLD_ACK_DURABLE, NULL) == 0);
    }
    tlc_cold_close(cold);
    rewrite_second_event_seq_gap(directory);
    assert(tlc_cold_open(&cold, &config) != 0);
}

static void test_strict_checkpoint_corruption_rejected(void) {
    char directory[] = "/tmp/tlc-cold-checkpoint-corrupt-XXXXXX";
    assert(mkdtemp(directory) != NULL);
    tlc_cold_config_t config = {
        .directory = directory, .segment_bytes = 4096, .queue_capacity = 2,
        .group_max_entries = 1, .group_max_delay_us = 1000};
    tlc_cold_t *cold = NULL;
    assert(tlc_cold_open(&cold, &config) == 0);
    const char key[] = "checkpoint-corrupt-key";
    const char value[] = "checkpoint-corrupt-value";
    tlc_cold_event_input_t event = {
        .term = 1, .op = TLC_COLD_OP_PUT, .meta_shard_id = 0, .version = 1,
        .key = key, .key_len = sizeof(key) - 1, .value = value,
        .value_len = sizeof(value) - 1};
    assert(tlc_cold_submit(cold, &event, TLC_COLD_ACK_DURABLE, NULL) == 0);
    const char state[] = "checkpoint-corrupt-state";
    tlc_cold_checkpoint_record_t record = {
        .meta_shard_id = 0, .captured_seq = 1, .state = state,
        .state_len = sizeof(state) - 1};
    tlc_cold_checkpoint_result_t result;
    assert(tlc_cold_publish_checkpoint(cold, 1, 1, 1, &record, 1, &result) == 0);
    tlc_cold_close(cold);
    char path[4096];
    assert(snprintf(path, sizeof(path), "%s/checkpoint-%020d", directory, 1) <
           (int)sizeof(path));
    int fd = open(path, O_RDWR);
    assert(fd >= 0);
    assert(lseek(fd, 0, SEEK_SET) == 0);
    uint8_t corrupt = 0xff;
    assert(write(fd, &corrupt, sizeof(corrupt)) == (ssize_t)sizeof(corrupt));
    assert(fsync(fd) == 0);
    assert(close(fd) == 0);
    assert(tlc_cold_open(&cold, &config) == 0);
    assert(tlc_cold_validate_checkpoint(cold, 0, 1, NULL) != 0);
    tlc_cold_close(cold);
}

static int strict_noop_replay(const tlc_cold_event_input_t *input,
                              uint64_t seq,
                              void *arg) {
    (void)input;
    (void)seq;
    (void)arg;
    return 0;
}

static int count_post_checkpoint_event(const tlc_cold_event_input_t *input,
                                        uint64_t seq,
                                        void *arg) {
    int *count = arg;
    assert(input != NULL);
    assert(seq > 6 && seq <= 10);
    (*count)++;
    return 0;
}

static void test_compact_interruption_recovery(
        tlc_cold_compact_failpoint_t failpoint) {
    char directory[] = "/tmp/tlc-cold-compact-interrupt-XXXXXX";
    assert(mkdtemp(directory) != NULL);
    tlc_cold_config_t config = {
        .directory = directory, .segment_bytes = 128, .queue_capacity = 4,
        .group_max_entries = 2, .group_max_delay_us = 1000};
    tlc_cold_t *cold = NULL;
    assert(tlc_cold_open(&cold, &config) == 0);
    for (uint32_t i = 0; i < 6; i++) {
        char key[32];
        char value[32];
        int key_len = snprintf(key, sizeof(key), "interrupt-key-%u", i);
        int value_len = snprintf(value, sizeof(value), "interrupt-value-%u", i);
        tlc_cold_event_input_t event = {
            .term = 1, .op = TLC_COLD_OP_PUT, .meta_shard_id = 0,
            .version = i + 1, .key = key, .key_len = (uint32_t)key_len,
            .value = value, .value_len = (uint32_t)value_len};
        assert(tlc_cold_submit(cold, &event, TLC_COLD_ACK_DURABLE, NULL) == 0);
    }
    tlc_cold_checkpoint_record_t record = {
        .meta_shard_id = 0, .captured_seq = 6, .state = NULL, .state_len = 0};
    tlc_cold_checkpoint_result_t result;
    assert(tlc_cold_publish_checkpoint(cold, 1, 1, 1, &record, 1, &result) == 0);
    for (uint32_t i = 6; i < 10; i++) {
        char key[32];
        char value[32];
        int key_len = snprintf(key, sizeof(key), "interrupt-key-%u", i);
        int value_len = snprintf(value, sizeof(value), "interrupt-value-%u", i);
        tlc_cold_event_input_t event = {
            .term = 1, .op = TLC_COLD_OP_PUT, .meta_shard_id = 0,
            .version = i + 1, .key = key, .key_len = (uint32_t)key_len,
            .value = value, .value_len = (uint32_t)value_len};
        assert(tlc_cold_submit(cold, &event, TLC_COLD_ACK_DURABLE, NULL) == 0);
    }
    assert(tlc_cold_publish_checkpoint(cold, 2, 1, 1, &record, 1, &result) == 0);
    assert(tlc_cold_set_compact_failpoint(cold, failpoint) == 0);
    assert(tlc_cold_compact(cold, 6, UINT64_MAX, 1) != 0);
    tlc_cold_close(cold);

    assert(tlc_cold_open(&cold, &config) == 0);
    assert(tlc_cold_validate_checkpoint(cold, 0, 1, &result) == 0);
    uint64_t captured_seq[] = {6};
    int replay_count = 0;
    assert(tlc_cold_replay_after(cold, captured_seq, 1,
                                 count_post_checkpoint_event,
                                 &replay_count) == 0);
    assert(replay_count == 4);
    tlc_cold_close(cold);
}

static void test_strict_metadata_shard_mismatch_rejected(void) {
    char directory[] = "/tmp/tlc-cold-shard-mismatch-XXXXXX";
    assert(mkdtemp(directory) != NULL);
    tlc_cold_config_t config = {
        .directory = directory, .segment_bytes = 4096, .queue_capacity = 2,
        .group_max_entries = 1, .group_max_delay_us = 1000};
    tlc_cold_t *cold = NULL;
    assert(tlc_cold_open(&cold, &config) == 0);
    const char key[] = "shard-mismatch-key";
    const char value[] = "shard-mismatch-value";
    tlc_cold_event_input_t event = {
        .term = 1, .op = TLC_COLD_OP_PUT, .meta_shard_id = 1, .version = 1,
        .key = key, .key_len = sizeof(key) - 1, .value = value,
        .value_len = sizeof(value) - 1};
    assert(tlc_cold_submit(cold, &event, TLC_COLD_ACK_DURABLE, NULL) == 0);
    uint64_t captured_seq[] = {0};
    assert(tlc_cold_replay_after(cold, captured_seq, 1,
                                 strict_noop_replay, NULL) != 0);
    tlc_cold_close(cold);
}

static void test_checkpoint_publish_failure(
        tlc_cold_checkpoint_failpoint_t failpoint) {
    char directory[] = "/tmp/tlc-cold-checkpoint-fail-XXXXXX";
    assert(mkdtemp(directory) != NULL);
    tlc_cold_config_t config = {
        .directory = directory,
        .segment_bytes = 256,
        .queue_capacity = 2,
        .group_max_entries = 2,
        .group_max_delay_us = 5000,
    };
    tlc_cold_t *cold = NULL;
    assert(tlc_cold_open(&cold, &config) == 0);
    const char key[] = "checkpoint-failure-key";
    const char value[] = "checkpoint-failure-value";
    tlc_cold_event_input_t event = {
        .term = 1,
        .op = TLC_COLD_OP_PUT,
        .meta_shard_id = 0,
        .version = 1,
        .key = key,
        .key_len = sizeof(key) - 1,
        .value = value,
        .value_len = sizeof(value) - 1,
    };
    assert(tlc_cold_submit(cold, &event, TLC_COLD_ACK_DURABLE, NULL) == 0);
    const char state[] = "checkpoint-state";
    tlc_cold_checkpoint_record_t record = {
        .meta_shard_id = 0,
        .captured_seq = 1,
        .state = state,
        .state_len = sizeof(state) - 1,
    };
    tlc_cold_checkpoint_result_t result;
    assert(tlc_cold_publish_checkpoint(cold, 1, 1, 1, &record, 1,
                                       &result) == 0);
    assert(tlc_cold_set_checkpoint_failpoint(cold, failpoint) == 0);
    assert(tlc_cold_publish_checkpoint(cold, 2, 1, 1, &record, 1,
                                       &result) != 0);
    assert(tlc_cold_validate_checkpoint(cold, 1, 1, &result) == 0);
    assert(checkpoint_exists(directory, 1));
    assert(tlc_cold_validate_checkpoint(cold, 0, 1, &result) == 0);
    tlc_cold_close(cold);
}

static void test_aof_io_failure(tlc_cold_io_failpoint_t failpoint) {
    char directory[] = "/tmp/tlc-cold-io-fail-XXXXXX";
    assert(mkdtemp(directory) != NULL);
    tlc_cold_config_t config = {
        .directory = directory, .segment_bytes = 4096, .queue_capacity = 2,
        .group_max_entries = 2, .group_max_delay_us = 1000};
    tlc_cold_t *cold = NULL;
    assert(tlc_cold_open(&cold, &config) == 0);
    const char key[] = "io-failure-key";
    const char value[] = "io-failure-value";
    tlc_cold_event_input_t event = {
        .term = 1, .op = TLC_COLD_OP_PUT, .meta_shard_id = 0, .version = 1,
        .key = key, .key_len = sizeof(key) - 1, .value = value,
        .value_len = sizeof(value) - 1};
    assert(tlc_cold_set_io_failpoint(cold, failpoint) == 0);
    uint64_t seq = UINT64_MAX;
    assert(tlc_cold_submit(cold, &event,
                           failpoint == TLC_COLD_IO_FAIL_APPEND_WRITE ?
                               TLC_COLD_ACK_ACCEPTED : TLC_COLD_ACK_DURABLE,
                           &seq) != 0);
    tlc_cold_progress_t progress;
    assert(tlc_cold_get_progress(cold, &progress) == 0);
    if (failpoint == TLC_COLD_IO_FAIL_APPEND_WRITE) {
        assert(seq == 0);
        assert(progress.appended_seq == 0);
        assert(progress.durable_seq == 0);
    } else {
        assert(seq == 1);
        assert(progress.appended_seq == 1);
        assert(progress.durable_seq == 0);
    }
    assert(tlc_cold_submit(cold, &event, TLC_COLD_ACK_ACCEPTED, NULL) != 0);
    tlc_cold_close(cold);

    assert(tlc_cold_open(&cold, &config) == 0);
    assert(tlc_cold_get_progress(cold, &progress) == 0);
    if (failpoint == TLC_COLD_IO_FAIL_APPEND_WRITE)
        assert(progress.appended_seq == 0 && progress.durable_seq == 0);
    else
        assert(progress.appended_seq == 1);
    tlc_cold_close(cold);
}

typedef struct cold_writer_test_arg {
    tlc_cold_t *cold;
    uint32_t worker_id;
    uint64_t first_seq;
} cold_writer_test_arg_t;

static void *submit_worker(void *opaque) {
    cold_writer_test_arg_t *arg = opaque;
    char key[32];
    char value[64];
    for (uint32_t i = 0; i < 12; i++) {
        int key_len = snprintf(key, sizeof(key), "worker-%u-key-%u", arg->worker_id, i);
        int value_len = snprintf(value, sizeof(value), "worker-%u-value-%u", arg->worker_id, i);
        tlc_cold_event_input_t event = {
            .term = 3,
            .op = TLC_COLD_OP_PUT,
            .meta_shard_id = arg->worker_id,
            .version = i + 1,
            .key = key,
            .key_len = (uint32_t)key_len,
            .value = value,
            .value_len = (uint32_t)value_len,
        };
        uint64_t seq = 0;
        assert(tlc_cold_submit(arg->cold, &event, TLC_COLD_ACK_DURABLE, &seq) == 0);
        if (i == 0)
            arg->first_seq = seq;
    }
    return NULL;
}

int main(void) {
    assert(monotonicInit() != NULL);
    test_strict_aof_seq_gap_rejected();
    test_strict_checkpoint_corruption_rejected();
    test_strict_metadata_shard_mismatch_rejected();
    for (int failpoint = TLC_COLD_COMPACT_FAIL_AFTER_SEGMENT_DELETE;
         failpoint <= TLC_COLD_COMPACT_FAIL_BEFORE_DIRECTORY_FSYNC;
         failpoint++) {
        test_compact_interruption_recovery(
            (tlc_cold_compact_failpoint_t)failpoint);
    }
    for (int failpoint = TLC_COLD_CHECKPOINT_FAIL_FILE_WRITE;
         failpoint <= TLC_COLD_CHECKPOINT_FAIL_MANIFEST_DIRECTORY_FSYNC;
         failpoint++) {
        test_checkpoint_publish_failure(
            (tlc_cold_checkpoint_failpoint_t)failpoint);
    }
    test_aof_io_failure(TLC_COLD_IO_FAIL_APPEND_WRITE);
    test_aof_io_failure(TLC_COLD_IO_FAIL_GROUP_FSYNC);
    char directory[] = "/tmp/tlc-cold-ut-XXXXXX";
    assert(mkdtemp(directory) != NULL);
    tlc_cold_config_t config = {
        .directory = directory,
        .segment_bytes = 192,
        .queue_capacity = 1,
        .group_max_entries = 4,
        .group_max_delay_us = 5000,
    };
    tlc_cold_t *cold = NULL;
    assert(tlc_cold_open(&cold, &config) == 0);

    const char key0[] = "key-0";
    const char value0[] = "value-0";
    uint64_t seq0 = 0;
    tlc_cold_event_input_t put = {
        .term = 3,
        .op = TLC_COLD_OP_PUT,
        .meta_shard_id = 11,
        .version = 1,
        .key = key0,
        .key_len = sizeof(key0) - 1,
        .value = value0,
        .value_len = sizeof(value0) - 1,
    };
    assert(tlc_cold_submit(cold, &put, TLC_COLD_ACK_DURABLE, &seq0) == 0);
    assert(seq0 == 1);

    const char key1[] = "key-1";
    const char value1[] = "value-1";
    put.key = key1;
    put.key_len = sizeof(key1) - 1;
    put.value = value1;
    put.value_len = sizeof(value1) - 1;
    put.version = 2;
    uint64_t seq1 = 0;
    assert(tlc_cold_submit(cold, &put, TLC_COLD_ACK_ACCEPTED, &seq1) == 0);
    assert(seq1 == 2);

    const char key2[] = "key-2";
    put.op = TLC_COLD_OP_DEL;
    put.key = key2;
    put.key_len = sizeof(key2) - 1;
    put.value = NULL;
    put.value_len = 0;
    put.version = 3;
    uint64_t seq2 = 0;
    assert(tlc_cold_submit(cold, &put, TLC_COLD_ACK_DURABLE, &seq2) == 0);
    assert(seq2 == 3);

    tlc_cold_progress_t progress;
    tlc_cold_get_progress(cold, &progress);
    assert(progress.appended_seq == 3);
    assert(progress.durable_seq == 3);

    const char shard0_state[] = "shard-0-state";
    const char shard1_state[] = "shard-1-state";
    tlc_cold_checkpoint_record_t checkpoint_records[] = {
        {.meta_shard_id = 0, .captured_seq = 2,
         .state = shard0_state, .state_len = sizeof(shard0_state) - 1},
        {.meta_shard_id = 1, .captured_seq = 3,
         .state = shard1_state, .state_len = sizeof(shard1_state) - 1},
        {.meta_shard_id = 2, .captured_seq = 3,
         .state = NULL, .state_len = 0},
    };
    tlc_cold_checkpoint_result_t checkpoint_result;
    assert(tlc_cold_publish_checkpoint(cold, 7, 3, 3,
                                       checkpoint_records, 3,
                                       &checkpoint_result) == 0);
    assert(checkpoint_result.generation == 7);
    assert(checkpoint_result.checkpoint_seq == 2);
    tlc_cold_checkpoint_result_t validated_checkpoint;
    assert(tlc_cold_validate_checkpoint(cold, 7, 3,
                                        &validated_checkpoint) == 0);
    assert(validated_checkpoint.generation == 7);
    assert(validated_checkpoint.checkpoint_seq == 2);
    char checkpoint_path[4096];
    char manifest_path[4096];
    assert(snprintf(checkpoint_path, sizeof(checkpoint_path),
                    "%s/checkpoint-%020d", directory, 7) <
           (int)sizeof(checkpoint_path));
    assert(snprintf(manifest_path, sizeof(manifest_path),
                    "%s/checkpoint.manifest", directory) <
           (int)sizeof(manifest_path));
    assert(access(checkpoint_path, R_OK) == 0);
    assert(access(manifest_path, R_OK) == 0);
    assert(tlc_cold_publish_checkpoint(cold, 7, 3, 3,
                                       checkpoint_records, 3,
                                       &checkpoint_result) != 0);
    assert(tlc_cold_publish_checkpoint(cold, 6, 3, 3,
                                       checkpoint_records, 3,
                                       &checkpoint_result) != 0);

    pthread_t workers[4];
    cold_writer_test_arg_t args[4];
    for (uint32_t i = 0; i < 4; i++) {
        args[i] = (cold_writer_test_arg_t){.cold = cold, .worker_id = i};
        assert(pthread_create(&workers[i], NULL, submit_worker, &args[i]) == 0);
    }
    for (size_t i = 0; i < 4; i++)
        assert(pthread_join(workers[i], NULL) == 0);
    tlc_cold_get_progress(cold, &progress);
    assert(progress.appended_seq == 51);
    assert(progress.durable_seq == 51);
    assert(tlc_cold_publish_checkpoint(cold, 8, 3, 3,
                                       checkpoint_records, 3,
                                       &checkpoint_result) == 0);
    /* The checkpoint floor is the minimum captured sequence (2). */
    assert(tlc_cold_compact(cold, 3, UINT64_MAX, 1) != 0);
    assert(tlc_cold_compact(cold, 2, UINT64_MAX, 1) == 0);
    uint64_t latest_segment = progress.segment_id;
    tlc_cold_close(cold);
    assert(count_aof_segments(directory) >= 2);

    /* Compact must retain post-checkpoint AOF and preserve checkpoint recovery. */
    char path[4096];
    char compact_directory[] = "/tmp/tlc-cold-compact-XXXXXX";
    assert(mkdtemp(compact_directory) != NULL);
    config.directory = compact_directory;
    config.segment_bytes = 128;
    assert(tlc_cold_open(&cold, &config) == 0);
    for (uint32_t i = 0; i < 4; i++) {
        char key[32];
        char value[64];
        int key_len = snprintf(key, sizeof(key), "compact-key-%u", i);
        int value_len = snprintf(value, sizeof(value), "compact-value-%u", i);
        put.op = TLC_COLD_OP_PUT;
        put.meta_shard_id = i % 2;
        put.key = key;
        put.key_len = (uint32_t)key_len;
        put.value = value;
        put.value_len = (uint32_t)value_len;
        put.version = i + 1;
        assert(tlc_cold_submit(cold, &put, TLC_COLD_ACK_DURABLE, NULL) == 0);
    }
    tlc_cold_checkpoint_record_t compact_records[] = {
        {.meta_shard_id = 0, .captured_seq = 4, .state = NULL, .state_len = 0},
        {.meta_shard_id = 1, .captured_seq = 4, .state = NULL, .state_len = 0},
    };
    assert(tlc_cold_publish_checkpoint(cold, 1, 3, 2,
                                       compact_records, 2,
                                       &checkpoint_result) == 0);
    for (uint32_t i = 4; i < 8; i++) {
        char key[32];
        char value[64];
        int key_len = snprintf(key, sizeof(key), "compact-key-%u", i);
        int value_len = snprintf(value, sizeof(value), "compact-value-%u", i);
        put.meta_shard_id = i % 2;
        put.key = key;
        put.key_len = (uint32_t)key_len;
        put.value = value;
        put.value_len = (uint32_t)value_len;
        put.version = i + 1;
        assert(tlc_cold_submit(cold, &put, TLC_COLD_ACK_DURABLE, NULL) == 0);
    }
    compact_records[0].captured_seq = 2;
    compact_records[1].captured_seq = 2;
    assert(tlc_cold_publish_checkpoint(cold, 2, 3, 2,
                                       compact_records, 2,
                                       &checkpoint_result) == 0);
    compact_records[0].captured_seq = 4;
    compact_records[1].captured_seq = 4;
    assert(tlc_cold_publish_checkpoint(cold, 3, 3, 2,
                                       compact_records, 2,
                                       &checkpoint_result) == 0);
    int segments_before = count_aof_segments(compact_directory);
    tlc_cold_get_progress(cold, &progress);
    uint64_t compact_active_segment = progress.segment_id;
    assert(tlc_cold_compact(cold, 4, 3, 2) != 0);
    assert(tlc_cold_compact(cold, 2, 3, 2) == 0);
    assert(!checkpoint_exists(compact_directory, 1));
    assert(checkpoint_exists(compact_directory, 2));
    assert(checkpoint_exists(compact_directory, 3));
    int segments_after = count_aof_segments(compact_directory);
    assert(segments_after < segments_before);
    assert(snprintf(path, sizeof(path), "%s/aof-%020llu.log", compact_directory,
                    (unsigned long long)compact_active_segment) < (int)sizeof(path));
    assert(access(path, R_OK) == 0);
    tlc_cold_close(cold);
    config.directory = compact_directory;
    assert(tlc_cold_open(&cold, &config) == 0);
    int checkpoint_count = 0;
    assert(tlc_cold_load_checkpoint(cold, 2, count_checkpoint_record,
                                    &checkpoint_count, &checkpoint_result) == 0);
    assert(checkpoint_count == 2);
    uint64_t captured_seq[] = {4, 4};
    int replay_count = 0;
    assert(tlc_cold_replay_after(cold, captured_seq, 2,
                                 count_replay_event, &replay_count) == 0);
    assert(replay_count == 4);
    tlc_cold_close(cold);

    config.directory = directory;
    config.segment_bytes = 192;
    assert(snprintf(path, sizeof(path), "%s/aof-%020llu.log", directory,
                    (unsigned long long)latest_segment) < (int)sizeof(path));
    int fd = open(path, O_WRONLY | O_APPEND);
    assert(fd >= 0);
    assert(write(fd, "tail", 4) == 4);
    close(fd);

    assert(tlc_cold_open(&cold, &config) == 0);
    tlc_cold_get_progress(cold, &progress);
    assert(progress.appended_seq == 51);
    assert(progress.durable_seq == 51);
    put.op = TLC_COLD_OP_PUT;
    put.key = key0;
    put.key_len = sizeof(key0) - 1;
    put.value = value0;
    put.value_len = sizeof(value0) - 1;
    put.version = 4;
    uint64_t seq3 = 0;
    assert(tlc_cold_submit(cold, &put, TLC_COLD_ACK_DURABLE, &seq3) == 0);
    assert(seq3 == 52);
    tlc_cold_close(cold);

    char corrupt_directory[] = "/tmp/tlc-cold-corrupt-XXXXXX";
    assert(mkdtemp(corrupt_directory) != NULL);
    config.directory = corrupt_directory;
    assert(tlc_cold_open(&cold, &config) == 0);
    assert(tlc_cold_submit(cold, &put, TLC_COLD_ACK_DURABLE, NULL) == 0);
    tlc_cold_close(cold);
    assert(snprintf(path, sizeof(path), "%s/aof-%020d.log", corrupt_directory, 0) <
           (int)sizeof(path));
    fd = open(path, O_WRONLY);
    assert(fd >= 0);
    assert(write(fd, "X", 1) == 1);
    close(fd);
    config.directory = corrupt_directory;
    assert(tlc_cold_open(&cold, &config) != 0);

    char shutdown_directory[] = "/tmp/tlc-cold-shutdown-XXXXXX";
    assert(mkdtemp(shutdown_directory) != NULL);
    config.directory = shutdown_directory;
    config.group_max_delay_us = 5000000;
    assert(tlc_cold_open(&cold, &config) == 0);
    uint64_t shutdown_seq = 0;
    assert(tlc_cold_submit(cold, &put, TLC_COLD_ACK_ACCEPTED,
                           &shutdown_seq) == 0);
    assert(shutdown_seq == 1);
    tlc_cold_close(cold);
    assert(tlc_cold_open(&cold, &config) == 0);
    tlc_cold_get_progress(cold, &progress);
    assert(progress.appended_seq == 1);
    assert(progress.durable_seq == 1);
    tlc_cold_close(cold);

    char nonlast_directory[] = "/tmp/tlc-cold-nonlast-XXXXXX";
    assert(mkdtemp(nonlast_directory) != NULL);
    config.directory = nonlast_directory;
    config.queue_capacity = 1;
    config.group_max_delay_us = 5000;
    assert(tlc_cold_open(&cold, &config) == 0);
    for (uint32_t i = 0; i < 20; i++) {
        char key[32];
        char value[64];
        int key_len = snprintf(key, sizeof(key), "nonlast-key-%u", i);
        int value_len = snprintf(value, sizeof(value), "nonlast-value-%u", i);
        put.op = TLC_COLD_OP_PUT;
        put.key = key;
        put.key_len = (uint32_t)key_len;
        put.value = value;
        put.value_len = (uint32_t)value_len;
        put.version = i + 1;
        assert(tlc_cold_submit(cold, &put, TLC_COLD_ACK_DURABLE, NULL) == 0);
    }
    tlc_cold_get_progress(cold, &progress);
    assert(progress.segment_id >= 1);
    tlc_cold_close(cold);
    assert(snprintf(path, sizeof(path), "%s/aof-%020d.log", nonlast_directory,
                    0) < (int)sizeof(path));
    fd = open(path, O_WRONLY | O_APPEND);
    assert(fd >= 0);
    assert(write(fd, "tail", 4) == 4);
    close(fd);
    config.directory = nonlast_directory;
    assert(tlc_cold_open(&cold, &config) != 0);

    printf("tlc_cold_ut: PASS\n");
    return 0;
}
