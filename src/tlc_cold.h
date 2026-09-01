#ifndef TLC_COLD_H
#define TLC_COLD_H

#include <stddef.h>
#include <stdint.h>

typedef struct tlc_cold tlc_cold_t;

/* Return values for all public COLD operations. */
enum {
    TLC_COLD_OK = 0,
    TLC_COLD_ERR = -1,
};

typedef enum tlc_cold_ack_mode {
    TLC_COLD_ACK_ACCEPTED = 0,
    TLC_COLD_ACK_DURABLE = 1,
} tlc_cold_ack_mode_t;

typedef enum tlc_cold_op {
    TLC_COLD_OP_PUT = 1,
    TLC_COLD_OP_DEL = 2,
} tlc_cold_op_t;

typedef struct tlc_cold_event_input {
    uint64_t term;
    uint32_t op;
    uint32_t meta_shard_id;
    uint64_t version;
    const void *key;
    uint32_t key_len;
    const void *value;
    uint32_t value_len;
} tlc_cold_event_input_t;

typedef struct tlc_cold_config {
    /* The directory must exist or be creatable by the process. */
    const char *directory;
    uint64_t segment_bytes;
    uint32_t queue_capacity;
    uint32_t group_max_entries;
    uint64_t group_max_delay_us;
} tlc_cold_config_t;

typedef struct tlc_cold_progress {
    uint64_t appended_seq;
    uint64_t durable_seq;
    uint64_t segment_id;
    uint64_t segment_offset;
} tlc_cold_progress_t;

typedef struct tlc_cold_checkpoint_record {
    uint32_t meta_shard_id;
    uint64_t captured_seq;
    const void *state;
    uint32_t state_len;
} tlc_cold_checkpoint_record_t;

typedef struct tlc_cold_checkpoint_result {
    uint64_t generation;
    uint64_t checkpoint_seq;
    uint64_t generation_checksum;
} tlc_cold_checkpoint_result_t;

/* One-shot checkpoint publish fault points, available only in test builds. */
typedef enum tlc_cold_checkpoint_failpoint {
    TLC_COLD_CHECKPOINT_FAIL_NONE = 0,
    TLC_COLD_CHECKPOINT_FAIL_FILE_WRITE,
    TLC_COLD_CHECKPOINT_FAIL_FILE_FSYNC,
    TLC_COLD_CHECKPOINT_FAIL_FILE_RENAME,
    TLC_COLD_CHECKPOINT_FAIL_FILE_DIRECTORY_FSYNC,
    TLC_COLD_CHECKPOINT_FAIL_MANIFEST_WRITE,
    TLC_COLD_CHECKPOINT_FAIL_MANIFEST_FSYNC,
    TLC_COLD_CHECKPOINT_FAIL_MANIFEST_RENAME,
    TLC_COLD_CHECKPOINT_FAIL_MANIFEST_DIRECTORY_FSYNC,
} tlc_cold_checkpoint_failpoint_t;

/* One-shot AOF I/O failure points, available only in test builds. */
typedef enum tlc_cold_io_failpoint {
    TLC_COLD_IO_FAIL_NONE = 0,
    TLC_COLD_IO_FAIL_APPEND_WRITE,
    TLC_COLD_IO_FAIL_GROUP_FSYNC,
} tlc_cold_io_failpoint_t;

typedef int (*tlc_cold_replay_fn)(const tlc_cold_event_input_t *input,
                                  uint64_t seq,
                                  void *arg);
typedef int (*tlc_cold_checkpoint_load_fn)(uint32_t meta_shard_id,
                                           uint64_t captured_seq,
                                           const void *state,
                                           uint32_t state_len,
                                           void *arg);

/* monotonicInit() must complete before opening a COLD runtime. */
int tlc_cold_open(tlc_cold_t **out, const tlc_cold_config_t *config);
void tlc_cold_close(tlc_cold_t *cold);

/*
 * Submit an event. The input buffers are copied before this function returns.
 * ACK_ACCEPTED waits for AOF append; ACK_DURABLE additionally waits for the
 * group commit containing the event to finish. seq is optional.
 */
int tlc_cold_submit(tlc_cold_t *cold,
                    const tlc_cold_event_input_t *input,
                    tlc_cold_ack_mode_t ack_mode,
                    uint64_t *seq);

/* Returns 0 on success; cold and progress are required. */
int tlc_cold_get_progress(const tlc_cold_t *cold,
                          tlc_cold_progress_t *progress);

/*
 * Replay every complete AOF event in sequence order. The callback owns no
 * event buffers; key/value are valid only until it returns. It must not call
 * back into this COLD runtime. This is used for WARM recovery and retrying a
 * previously durable event whose metadata publish failed.
 */
int tlc_cold_replay(tlc_cold_t *cold, tlc_cold_replay_fn callback, void *arg);
int tlc_cold_replay_after(tlc_cold_t *cold,
                          const uint64_t *captured_seq,
                          uint32_t meta_shard_count,
                          tlc_cold_replay_fn callback,
                          void *arg);

/*
 * Publish one immutable checkpoint generation and atomically update manifest.
 * Preconditions: cold, records and result are non-NULL; generation and
 * meta_shard_count are non-zero; record_count equals meta_shard_count; records
 * are ordered by meta_shard_id [0, meta_shard_count); empty state has state=NULL.
 * The TLC core boundary validates these shape conditions before calling here.
 */
int tlc_cold_publish_checkpoint(
    tlc_cold_t *cold,
    uint64_t generation,
    uint64_t term,
    uint32_t meta_shard_count,
    const tlc_cold_checkpoint_record_t *records,
    uint32_t record_count,
    tlc_cold_checkpoint_result_t *result);

#ifdef TLC_COLD_ENABLE_FAILPOINT
/* Set a one-shot publish failure; the point is cleared when it fires. */
int tlc_cold_set_checkpoint_failpoint(
    tlc_cold_t *cold,
    tlc_cold_checkpoint_failpoint_t failpoint);

int tlc_cold_set_io_failpoint(tlc_cold_t *cold,
                              tlc_cold_io_failpoint_t failpoint);

typedef enum tlc_cold_compact_failpoint {
    TLC_COLD_COMPACT_FAIL_NONE = 0,
    TLC_COLD_COMPACT_FAIL_AFTER_SEGMENT_DELETE,
    TLC_COLD_COMPACT_FAIL_AFTER_GENERATION_DELETE,
    TLC_COLD_COMPACT_FAIL_BEFORE_DIRECTORY_FSYNC,
} tlc_cold_compact_failpoint_t;

int tlc_cold_set_compact_failpoint(
    tlc_cold_t *cold,
    tlc_cold_compact_failpoint_t failpoint);
#endif

/* Validate one published generation; generation=0 validates manifest active. */
int tlc_cold_validate_checkpoint(tlc_cold_t *cold,
                                 uint64_t generation,
                                 uint32_t expected_meta_shard_count,
                                 tlc_cold_checkpoint_result_t *result);

/* Load the manifest-selected, fully validated checkpoint into the caller. */
int tlc_cold_load_checkpoint(tlc_cold_t *cold,
                             uint32_t expected_meta_shard_count,
                             tlc_cold_checkpoint_load_fn callback,
                             void *arg,
                             tlc_cold_checkpoint_result_t *result);

/* Compact sealed AOF segments and trim old published generations. */
int tlc_cold_compact(tlc_cold_t *cold,
                     uint64_t checkpoint_floor_seq,
                     uint64_t ha_safe_point_seq,
                     uint32_t checkpoint_retention_count);

#endif
