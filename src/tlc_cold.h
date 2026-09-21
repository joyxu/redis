#ifndef TLC_COLD_H
#define TLC_COLD_H

#include <stddef.h>
#include <stdint.h>

typedef struct tlc_cold tlc_cold_t;

/* Return values for all public COLD operations. */
enum {
    TLC_COLD_OK = 0,
    /* A replay callback intentionally stopped the current scan. */
    TLC_COLD_REPLAY_STOP = 1,
    TLC_COLD_ERR = -1,
    TLC_COLD_REPLICA_GAP = -2,
    TLC_COLD_REPLICA_CONFLICT = -3,
    /* The requested snapshot tail is no longer retained. */
    TLC_COLD_RESYNC_REQUIRED = -4,
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
    /* Owner fencing term; zero is used by Standalone mode. */
    uint64_t ha_term;
    /* Topology/migration epoch associated with this logical event. */
    uint64_t topology_epoch;
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
    /* Target retained AOF events. The cursor ring is ceil_pow2(1.5x this).
     * Zero selects the production default. */
    uint64_t retention_events;
} tlc_cold_config_t;

typedef struct tlc_cold_progress {
    uint64_t appended_seq;
    uint64_t durable_seq;
    uint64_t segment_id;
    uint64_t segment_offset;
} tlc_cold_progress_t;

typedef struct tlc_cold_seq_cursor {
    uint64_t seq;
    uint64_t segment_id;
    uint64_t segment_offset;
} tlc_cold_seq_cursor_t;

typedef struct tlc_cold_retention_window {
    uint64_t retained_floor_seq;
    uint64_t appended_seq;
    uint64_t target_events;
    uint64_t ring_capacity;
} tlc_cold_retention_window_t;

typedef enum tlc_cold_replica_batch_status {
    TLC_COLD_REPLICA_BATCH_APPLIED = 0,
    TLC_COLD_REPLICA_BATCH_DUPLICATE = 1,
    TLC_COLD_REPLICA_BATCH_GAP = 2,
    TLC_COLD_REPLICA_BATCH_CONFLICT = 3,
    TLC_COLD_REPLICA_BATCH_ERROR = 4,
} tlc_cold_replica_batch_status_t;

typedef struct tlc_cold_checkpoint_record {
    uint32_t meta_shard_id;
    uint64_t captured_seq;
    const void *state;
    uint32_t state_len;
} tlc_cold_checkpoint_record_t;

typedef struct tlc_cold_checkpoint_result {
    uint64_t generation;
    /* Owner fencing term captured by the checkpoint generation. */
    uint64_t ha_term;
    uint64_t checkpoint_seq;
    uint64_t generation_checksum;
} tlc_cold_checkpoint_result_t;

/*
 * Leader-owned immutable resync artifact. The returned blob and captured_seq
 * belong to this object and are released by tlc_cold_end_resync_snapshot().
 * A caller must serialize read/end operations for one snapshot.
 */
typedef struct tlc_cold_resync_snapshot {
    void *checkpoint_blob;
    size_t checkpoint_blob_bytes;
    uint64_t *captured_seq;
    uint32_t meta_shard_count;
    tlc_cold_checkpoint_result_t checkpoint;
    uint64_t checkpoint_blob_checksum;
    uint64_t captured_seq_checksum;
    uint64_t tail_start_seq;
    uint64_t durable_boundary_seq;
    uint64_t retention_pin_token;
} tlc_cold_resync_snapshot_t;

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
typedef int (*tlc_cold_append_sink_fn)(const tlc_cold_event_input_t *input,
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
/* Borrowed configured COLD directory; valid while cold remains alive. */
const char *tlc_cold_directory(const tlc_cold_t *cold);
/* Snapshot the retained AOF window for low-frequency retention control. */
int tlc_cold_get_retention_window(tlc_cold_t *cold,
                                  tlc_cold_retention_window_t *window);
/* Seal the current nonempty AOF segment so a later compact can reclaim it. */
int tlc_cold_seal_segment(tlc_cold_t *cold);

/* Find the exact retained AOF offset for seq. */
int tlc_cold_get_seq_cursor(tlc_cold_t *cold,
                            uint64_t seq,
                            tlc_cold_seq_cursor_t *cursor);

/* Configure the append sink before concurrent submissions begin. */
int tlc_cold_set_append_sink(tlc_cold_t *cold,
                             tlc_cold_append_sink_fn sink,
                             void *arg);

/*
 * Append a contiguous Leader sequence range to the local Replica AOF.
 * The input event buffers are copied before this function returns. A new
 * range is appended with the supplied original seq values and acknowledges
 * according to ack_mode. An identical already acknowledged range is reported
 * as DUPLICATE; a different payload for the same seq is CONFLICT. A range
 * that does not start at the next local seq is reported as GAP. The caller
 * must serialize this operation with Replica apply/recovery lifecycle changes.
 */
int tlc_cold_submit_replica_batch(
    tlc_cold_t *cold,
    uint64_t first_seq,
    const tlc_cold_event_input_t *events,
    uint32_t event_count,
    tlc_cold_ack_mode_t ack_mode,
    tlc_cold_replica_batch_status_t *status,
    uint64_t *acknowledged_seq);

/*
 * Replay every complete AOF event in sequence order. The callback owns no
 * event buffers; key/value are valid only until it returns. It must not call
 * back into this COLD runtime. Returning TLC_COLD_REPLAY_STOP ends the scan
 * without an AOF error; other nonzero returns are propagated. This is used
 * for WARM recovery and retrying a previously durable event whose metadata
 * publish failed.
 */
int tlc_cold_replay(tlc_cold_t *cold, tlc_cold_replay_fn callback, void *arg);
/*
 * Replay one inclusive contiguous seq range from the AOF. A missing start
 * record or any discontinuity returns TLC_COLD_RESYNC_REQUIRED.
 */
int tlc_cold_replay_range(tlc_cold_t *cold,
                          uint64_t start_seq,
                          uint64_t end_seq,
                          tlc_cold_replay_fn callback,
                          void *arg);
/*
 * Replay at most max_events events from an inclusive contiguous seq range.
 * *next_seq receives the first unread seq on return and *event_count receives
 * the number of replayed events. A missing start record or any discontinuity
 * returns TLC_COLD_RESYNC_REQUIRED.
 */
int tlc_cold_replay_range_limited(tlc_cold_t *cold,
                                  uint64_t start_seq,
                                  uint64_t end_seq,
                                  uint32_t max_events,
                                  tlc_cold_replay_fn callback,
                                  void *arg,
                                  uint64_t *next_seq,
                                  uint32_t *event_count);
int tlc_cold_replay_after(tlc_cold_t *cold,
                          const uint64_t *captured_seq,
                          uint32_t meta_shard_count,
                          tlc_cold_replay_fn callback,
                          void *arg);

/*
 * Create a Leader snapshot session. It exports an already validated active
 * checkpoint, fixes durable_boundary_seq, and pins [tail_start_seq, B] from
 * AOF compaction until tlc_cold_end_resync_snapshot().
 */
int tlc_cold_begin_resync_snapshot(
    tlc_cold_t *cold,
    uint32_t expected_meta_shard_count,
    tlc_cold_resync_snapshot_t *snapshot);

/*
 * Read at most max_events from the pinned inclusive [tail_start_seq, B]
 * range. *next_seq is the next requested seq and advances on success; it
 * must initially equal snapshot->tail_start_seq and reaches B + 1 at EOF.
 * A missing/non-contiguous AOF prefix returns TLC_COLD_RESYNC_REQUIRED.
 */
int tlc_cold_read_resync_snapshot(
    tlc_cold_t *cold,
    const tlc_cold_resync_snapshot_t *snapshot,
    uint64_t *next_seq,
    uint32_t max_events,
    tlc_cold_replay_fn callback,
    void *arg,
    uint32_t *event_count);

/* Release the snapshot blob, captured-seq copy, and its AOF retention pin. */
void tlc_cold_end_resync_snapshot(
    tlc_cold_t *cold,
    tlc_cold_resync_snapshot_t *snapshot);
/* Extend an active Leader session's retained tail boundary before its next round. */
int tlc_cold_extend_resync_snapshot(tlc_cold_t *cold,
                                    tlc_cold_resync_snapshot_t *snapshot,
                                    uint64_t durable_boundary_seq);

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
    uint64_t ha_term,
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

/* Clone the active validated checkpoint into an empty fenced Replica COLD. */
int tlc_cold_clone_checkpoint(tlc_cold_t *source,
                              tlc_cold_t *target,
                              uint32_t expected_meta_shard_count,
                              tlc_cold_checkpoint_result_t *result);
/*
 * Export the active validated checkpoint as an owned immutable blob.
 * Preconditions: cold, blob, blob_bytes and result are non-NULL; the expected
 * shard count is non-zero. The caller owns the returned blob and must release
 * it with tlc_cold_free_checkpoint_blob().
 */
int tlc_cold_export_checkpoint(tlc_cold_t *cold,
                               uint32_t expected_meta_shard_count,
                               void **blob,
                               size_t *blob_bytes,
                               tlc_cold_checkpoint_result_t *result);
/*
 * Import a validated checkpoint blob into an empty fenced Replica COLD.
 * Preconditions: cold, blob and result are non-NULL; expected shard count is
 * non-zero; no AOF submissions or lifecycle changes run concurrently. The
 * target has no checkpoint manifest and its seq prefix is zero. The blob is
 * immutable for the duration of the call. Invalid input never replaces an
 * existing valid generation.
 */
int tlc_cold_import_checkpoint(tlc_cold_t *cold,
                               uint32_t expected_meta_shard_count,
                               const void *blob,
                               size_t blob_bytes,
                               tlc_cold_checkpoint_result_t *result);
/*
 * Import an immutable complete checkpoint artifact without mapping or copying
 * the full blob. Preconditions: fd is a readable descriptor for artifact_path,
 * blob_bytes is its validated exact size, and artifact_path remains exclusively
 * owned and unchanged until this call returns. The empty fenced target contract
 * is the same as tlc_cold_import_checkpoint(). On success artifact_path is
 * atomically moved into this COLD's checkpoint generation.
 */
int tlc_cold_import_checkpoint_file(tlc_cold_t *cold,
                                    uint32_t expected_meta_shard_count,
                                    int fd,
                                    const char *artifact_path,
                                    size_t blob_bytes,
                                    tlc_cold_checkpoint_result_t *result);
void tlc_cold_free_checkpoint_blob(void *blob);
/*
 * Reset this COLD object in place to an empty fenced runtime. Preconditions:
 * the caller has stopped all submissions, replay callbacks and lifecycle
 * users; no request or active resync snapshot can be waiting on this COLD.
 * Only COLD-owned AOF and
 * checkpoint files in its configured directory are removed. On failure the
 * object remains stopped and the caller must keep the Follower fenced.
 */
int tlc_cold_reset_fenced(tlc_cold_t *cold);
/* Seed an empty Replica COLD with the checkpoint's durable prefix. */
int tlc_cold_set_replica_base_seq(tlc_cold_t *cold, uint64_t base_seq);

/* Compact sealed AOF segments and trim old published generations. */
int tlc_cold_compact(tlc_cold_t *cold,
                     uint64_t checkpoint_floor_seq,
                     uint64_t ha_safe_point_seq,
                     uint32_t checkpoint_retention_count);

#endif
