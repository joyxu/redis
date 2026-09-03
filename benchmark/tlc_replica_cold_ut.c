#include "tlc_cold.h"
#include "monotonic.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct replay_state {
    uint64_t last_seq;
    uint32_t count;
} replay_state_t;

static int count_replay_event(const tlc_cold_event_input_t *event,
                              uint64_t seq,
                              void *arg) {
    replay_state_t *state = arg;
    assert(event->key_len != 0);
    state->last_seq = seq;
    state->count++;
    return 0;
}

static tlc_cold_event_input_t make_put(const char *key,
                                       const uint8_t *value,
                                       uint64_t version) {
    return (tlc_cold_event_input_t){
        .ha_term = 1,
        .topology_epoch = 1,
        .op = TLC_COLD_OP_PUT,
        .meta_shard_id = 0,
        .version = version,
        .key = key,
        .key_len = (uint32_t)strlen(key),
        .value = value,
        .value_len = 4,
    };
}

int main(void) {
    assert(monotonicInit() != NULL);
    char directory[] = "/tmp/tlc-replica-cold-XXXXXX";
    assert(mkdtemp(directory) != NULL);
    tlc_cold_config_t config = {
        .directory = directory,
        .segment_bytes = 4096,
        .queue_capacity = 4,
        .group_max_entries = 8,
        .group_max_delay_us = 1000,
    };

    const uint8_t value1[4] = {1, 2, 3, 4};
    const uint8_t value2[4] = {4, 3, 2, 1};
    tlc_cold_event_input_t events[2] = {
        make_put("replica-key-1", value1, 1),
        make_put("replica-key-2", value2, 1),
    };
    tlc_cold_t *cold = NULL;
    assert(tlc_cold_open(&cold, &config) == 0);
    tlc_cold_replica_batch_status_t status;
    uint64_t durable_seq = 0;
    assert(tlc_cold_submit_replica_batch(cold, 1, events, 2,
                                         TLC_COLD_ACK_ACCEPTED, &status,
                                         &durable_seq) == 0);
    assert(status == TLC_COLD_REPLICA_BATCH_APPLIED);
    assert(durable_seq == 2);
    tlc_cold_progress_t progress;
    assert(tlc_cold_get_progress(cold, &progress) == 0);
    assert(progress.appended_seq == 2);

    assert(tlc_cold_submit_replica_batch(cold, 1, events, 2,
                                         TLC_COLD_ACK_ACCEPTED, &status,
                                         &durable_seq) == 0);
    assert(status == TLC_COLD_REPLICA_BATCH_DUPLICATE);
    assert(durable_seq == 2);

    tlc_cold_event_input_t conflict = make_put("replica-key-1", value2, 1);
    assert(tlc_cold_submit_replica_batch(cold, 1, &conflict, 1,
                                         TLC_COLD_ACK_DURABLE, &status,
                                         NULL) == 0);
    assert(status == TLC_COLD_REPLICA_BATCH_CONFLICT);

    assert(tlc_cold_submit_replica_batch(cold, 4, &events[0], 1,
                                         TLC_COLD_ACK_DURABLE, &status,
                                         NULL) == 0);
    assert(status == TLC_COLD_REPLICA_BATCH_GAP);

    tlc_cold_event_input_t event3 = make_put("replica-key-3", value1, 1);
    assert(tlc_cold_submit_replica_batch(cold, 3, &event3, 1,
                                         TLC_COLD_ACK_DURABLE, &status,
                                         &durable_seq) == 0);
    assert(status == TLC_COLD_REPLICA_BATCH_APPLIED);
    assert(durable_seq == 3);
    assert(tlc_cold_get_progress(cold, &progress) == 0);
    assert(progress.appended_seq == 3 && progress.durable_seq == 3);
    tlc_cold_close(cold);

    assert(tlc_cold_open(&cold, &config) == 0);
    assert(tlc_cold_get_progress(cold, &progress) == 0);
    assert(progress.appended_seq == 3 && progress.durable_seq == 3);
    tlc_cold_event_input_t tail[2] = {events[1], event3};
    assert(tlc_cold_submit_replica_batch(cold, 2, tail, 2,
                                         TLC_COLD_ACK_DURABLE, &status,
                                         &durable_seq) == 0);
    assert(status == TLC_COLD_REPLICA_BATCH_DUPLICATE);
    assert(durable_seq == 3);

    replay_state_t replay = {0};
    assert(tlc_cold_replay(cold, count_replay_event, &replay) == 0);
    assert(replay.count == 3 && replay.last_seq == 3);
    tlc_cold_seq_cursor_t cursor;
    assert(tlc_cold_get_seq_cursor(cold, 3, &cursor) == 0);
    assert(cursor.seq == 1);
    replay = (replay_state_t){0};
    assert(tlc_cold_replay_range(cold, 2, 3, count_replay_event,
                                 &replay) == 0);
    assert(replay.count == 2 && replay.last_seq == 3);
    tlc_cold_close(cold);
    printf("tlc_replica_cold_ut: PASS\n");
    return 0;
}
