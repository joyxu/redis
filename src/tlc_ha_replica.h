#ifndef TLC_HA_REPLICA_H
#define TLC_HA_REPLICA_H

#include "tlc_core.h"

#include <stddef.h>
#include <stdint.h>

typedef struct tlc_ha_replica tlc_ha_replica_t;

typedef struct tlc_ha_replica_progress {
    uint64_t appended_seq;
    uint64_t durable_seq;
    uint64_t applied_seq;
    uint64_t peer_accepted_seq;
    uint64_t peer_durable_seq;
    uint64_t peer_applied_seq;
    /* Earliest Leader AOF seq dropped from the lossy sender queue. */
    uint64_t replay_from_seq;
} tlc_ha_replica_progress_t;

typedef enum tlc_ha_replica_role {
    TLC_HA_REPLICA_LEADER = 1,
    TLC_HA_REPLICA_FOLLOWER = 2,
} tlc_ha_replica_role_t;

/* Runtime HA ownership state. Role describes replication direction; state
 * describes liveness and ownership transitions. */
typedef enum tlc_ha_replica_ha_state {
    TLC_HA_REPLICA_STATE_INIT = 0,
    TLC_HA_REPLICA_STATE_BACKUP = 1,
    TLC_HA_REPLICA_STATE_SUSPECT = 2,
    TLC_HA_REPLICA_STATE_CANDIDATE = 3,
    TLC_HA_REPLICA_STATE_RECOVERING = 4,
    TLC_HA_REPLICA_STATE_MASTER = 5,
    TLC_HA_REPLICA_STATE_FAULT = 6,
    TLC_HA_REPLICA_STATE_FENCED = 7,
} tlc_ha_replica_ha_state_t;

typedef enum tlc_ha_replica_health {
    TLC_HA_REPLICA_HEALTHY = 1,
    TLC_HA_REPLICA_DEGRADED = 2,
    TLC_HA_REPLICA_RECOVERING = 3,
    TLC_HA_REPLICA_FAILED = 4,
    TLC_HA_REPLICA_UNAVAILABLE = 5,
} tlc_ha_replica_health_t;

typedef enum tlc_ha_resync_stage {
    TLC_HA_RESYNC_IDLE = 0,
    TLC_HA_RESYNC_RECEIVING_SNAPSHOT = 1,
    TLC_HA_RESYNC_SNAPSHOT_COMPLETE = 2,
    TLC_HA_RESYNC_FAILED = 3,
} tlc_ha_resync_stage_t;

typedef enum tlc_ha_resync_reason {
    TLC_HA_RESYNC_REASON_NONE = 0,
    TLC_HA_RESYNC_REASON_IDENTITY = 1,
    TLC_HA_RESYNC_REASON_TERM = 2,
    TLC_HA_RESYNC_REASON_SESSION = 3,
    TLC_HA_RESYNC_REASON_GEOMETRY = 4,
    TLC_HA_RESYNC_REASON_OFFSET = 5,
    TLC_HA_RESYNC_REASON_CHECKSUM = 6,
    TLC_HA_RESYNC_REASON_ALLOCATION = 7,
    TLC_HA_RESYNC_REASON_IO = 8,
} tlc_ha_resync_reason_t;

/* Reasons that can request a new snapshot from the fixed current Leader. */
typedef enum tlc_ha_resync_required_reason {
    TLC_HA_RESYNC_REQUIRED_GAP = 1,
    TLC_HA_RESYNC_REQUIRED_CONFLICT = 2,
    TLC_HA_RESYNC_REQUIRED_RETENTION = 3,
} tlc_ha_resync_required_reason_t;

typedef struct tlc_ha_resync_snapshot_begin {
    uint64_t session_id;
    uint64_t leader_node_id;
    uint64_t follower_node_id;
    uint64_t ha_term;
    uint64_t topology_epoch;
    uint64_t generation;
    uint64_t checkpoint_seq;
    uint64_t durable_boundary_seq;
    uint64_t checkpoint_blob_bytes;
    uint64_t checkpoint_blob_checksum;
    uint64_t captured_seq_checksum;
    uint32_t meta_shard_count;
} tlc_ha_resync_snapshot_begin_t;

typedef struct tlc_ha_resync_snapshot_chunk {
    uint64_t session_id;
    uint64_t offset;
    uint32_t bytes;
    uint64_t checksum;
} tlc_ha_resync_snapshot_chunk_t;

typedef struct tlc_ha_resync_snapshot_end {
    uint64_t session_id;
    uint64_t checkpoint_blob_bytes;
    uint64_t checkpoint_blob_checksum;
} tlc_ha_resync_snapshot_end_t;

typedef struct tlc_ha_resync_assembler tlc_ha_resync_assembler_t;

/*
 * Create a Follower-only snapshot assembler. The caller supplies the fixed
 * local/peer identity and current term expected at wire ingress. max_blob_bytes
 * bounds one checkpoint artifact. No COLD/Core state is changed by this API.
 */
int tlc_ha_resync_assembler_create(tlc_ha_resync_assembler_t **out,
                                   uint64_t local_node_id,
                                   uint64_t peer_node_id,
                                   uint64_t ha_term,
                                   uint64_t max_blob_bytes,
                                   const char *artifact_directory);
void tlc_ha_resync_assembler_destroy(tlc_ha_resync_assembler_t *assembler);

/* Begin a new session after a validated SNAPSHOT_BEGIN control payload. */
int tlc_ha_resync_assembler_begin(
    tlc_ha_resync_assembler_t *assembler,
    const tlc_ha_resync_snapshot_begin_t *begin);

/* Append one validated SNAPSHOT_CHUNK payload in strictly increasing offset. */
int tlc_ha_resync_assembler_append(
    tlc_ha_resync_assembler_t *assembler,
    const tlc_ha_resync_snapshot_chunk_t *chunk,
    const void *data);

/*
 * Validate SNAPSHOT_END, fsync and atomically finalize the received artifact.
 * path is the complete .blob path. No complete checkpoint buffer is allocated
 * by the assembler.
 */
int tlc_ha_resync_assembler_finish(
    tlc_ha_resync_assembler_t *assembler,
    const tlc_ha_resync_snapshot_end_t *end,
    char *path,
    size_t path_size,
    size_t *blob_bytes,
    tlc_ha_resync_snapshot_begin_t *begin);
tlc_ha_resync_stage_t tlc_ha_resync_assembler_stage(
    const tlc_ha_resync_assembler_t *assembler);
tlc_ha_resync_reason_t tlc_ha_resync_assembler_reason(
    const tlc_ha_resync_assembler_t *assembler);

typedef struct tlc_ha_replica_ring_config {
    uint32_t backend_type;
    uint32_t cache_policy;
    uint64_t mmap_offset;
    uint32_t slot_count;
    uint32_t slot_bytes;
    char path[256];
} tlc_ha_replica_ring_config_t;

typedef struct tlc_ha_replica_config {
    tlc_core_t *core;
    tlc_cold_t *cold;
    /* TCP control endpoint owned by the Replica runtime. */
    const char *control_bind_host;
    uint16_t control_bind_port;
    const char *peer_advertised_host;
    uint16_t peer_control_port;
    /* Deprecated compatibility path for legacy in-process tests. */
    int control_fd;
    tlc_ha_replica_role_t role;
    tlc_ha_replica_ring_config_t tx_ring;
    tlc_ha_replica_ring_config_t rx_ring;
    uint32_t queue_capacity;
    uint32_t max_batch_events;
    uint32_t max_batch_bytes;
    /* Maximum complete checkpoint artifact accepted by the Follower. */
    uint64_t max_resync_snapshot_bytes;
    /* Fixed sender identity used to reject cross-Node-group heartbeats. */
    uint64_t hpc_node_id;
    /* Fixed peer identity expected on the connected Replica channel. */
    uint64_t peer_node_id;
    /* Owner fencing term carried by events and heartbeats. */
    uint64_t ha_term;
    /* Initial connection epoch; zero selects epoch 1. */
    uint64_t connection_epoch;
    /* Local heartbeat cadence; zero selects the default. */
    uint32_t heartbeat_interval_ms;
    /* Receiver-local liveness timeout; zero selects the default. */
    uint32_t heartbeat_timeout_ms;
    /* Consecutive timeout samples required before publishing failure. */
    uint32_t heartbeat_failure_threshold;
    /* Maximum timeout probe backoff; zero selects the default. */
    uint32_t heartbeat_backoff_max_ms;
    /* Minimum time spent in SUSPECT before publishing failure. */
    uint32_t heartbeat_suspect_hold_down_ms;
    /* Controlled resync inactivity timeout; zero selects the default. */
    uint32_t resync_timeout_ms;
    /* Fixed topology lineage for automatic resync; zero selects epoch 1. */
    uint64_t topology_epoch;
    /* Automatic snapshot chunk size; zero selects 32 KiB. */
    uint32_t resync_chunk_bytes;
} tlc_ha_replica_config_t;

/*
 * Start a bidirectional Replica runtime. The runtime owns the TCP control
 * listener and connection for the configured endpoint; UB rings carry the
 * data plane. Leader events are sourced from the core event sink; Follower
 * frames are durably appended before async apply. Control frames (heartbeat,
 * announce, resync coordination) flow over TCP; data frames (EVENTS, ACK,
 * snapshot chunks) flow over UB rings.
 */
int tlc_ha_replica_start(tlc_ha_replica_t **out,
                         const tlc_ha_replica_config_t *config);
/* Reset one shared Replica ring while no producer or consumer is attached. */
int tlc_ha_replica_reset_ring(const tlc_ha_replica_ring_config_t *config);
void tlc_ha_replica_stop(tlc_ha_replica_t *replica);

/* Queue a contiguous Leader AOF range for retransmission after reconnect.
 * Queue saturation is lossy: the earliest dropped seq is recorded and the
 * sender re-reads that range from Leader COLD. The caller must not concurrently
 * append new Leader events while the range is being enumerated. */
int tlc_ha_replica_replay_from(tlc_ha_replica_t *replica,
                               uint64_t start_seq,
                               uint64_t end_seq);

/*
 * Send one already pinned Leader snapshot as BEGIN/CHUNK/END frames. The
 * caller keeps snapshot alive until this synchronous call returns. This only
 * transfers the artifact; it does not perform follower installation.
 */
int tlc_ha_replica_send_resync_snapshot(
    tlc_ha_replica_t *replica,
    uint64_t session_id,
    uint64_t topology_epoch,
    const tlc_cold_resync_snapshot_t *snapshot,
    uint32_t chunk_bytes);

/*
 * Start one controlled M5 session. It pins the Leader checkpoint/tail, sends
 * RESYNC_REQUEST plus the snapshot, then the listener drives tail rounds and
 * final handoff from Follower control frames. Only one session is active per
 * Leader Replica runtime.
 */
int tlc_ha_replica_begin_resync(tlc_ha_replica_t *replica,
                                 uint64_t session_id,
                                 uint64_t topology_epoch,
                                 uint32_t chunk_bytes);

/*
 * Abort one active controlled session. The Leader keeps normal emission gated
 * until a later resync reaches HANDOFF_ACK; the Follower remains fenced.
 * This orchestration API must not race Follower snapshot installation.
 */
int tlc_ha_replica_abort_resync(tlc_ha_replica_t *replica);

/* Transfer a completed, verified Follower snapshot artifact path to M4. */
int tlc_ha_replica_take_resync_snapshot(
    tlc_ha_replica_t *replica,
    char *path,
    size_t path_size,
    size_t *blob_bytes,
    tlc_ha_resync_snapshot_begin_t *begin);

/*
 * Fence normal Follower replication, wait for the lock-free ingress/apply
 * counters to quiesce, discard queued old events, then install one completed
 * snapshot artifact into the only active Core/COLD runtime. On failure the
 * Follower remains fenced for a new resync attempt.
 */
int tlc_ha_replica_install_resync_snapshot(
    tlc_ha_replica_t *replica,
    tlc_cold_checkpoint_result_t *result);

/* Returns the highest peer AOF append acknowledged by the ACK frame. */
uint64_t tlc_ha_replica_peer_accepted_seq(const tlc_ha_replica_t *replica);
/* Snapshot local COLD progress and peer ACK/heartbeat progress. */
int tlc_ha_replica_get_progress(const tlc_ha_replica_t *replica,
                                tlc_ha_replica_progress_t *progress);
/* Returns the last valid health reported by the peer. */
tlc_ha_replica_health_t tlc_ha_replica_peer_health(
        const tlc_ha_replica_t *replica);
/* Returns receiver-local monotonic time of the last valid peer heartbeat. */
uint64_t tlc_ha_replica_last_heartbeat_ns(
        const tlc_ha_replica_t *replica);
/* Returns the current runtime replication direction. */
tlc_ha_replica_role_t tlc_ha_replica_role(
        const tlc_ha_replica_t *replica);
/* Returns the current owner fencing term. */
uint64_t tlc_ha_replica_ha_term(const tlc_ha_replica_t *replica);
/*
 * Atomically transition ownership after fencing ingress and draining all
 * in-flight work. A term increase is persisted before the new role is
 * published. Leader transitions announce the new owner before normal
 * emission is unfenced.
 */
int tlc_ha_replica_transition_role(tlc_ha_replica_t *replica,
                                   tlc_ha_replica_role_t role,
                                   tlc_ha_replica_ha_state_t state,
                                   uint64_t new_term);
/* Current connection lineage used by M10 control frames. */
uint64_t tlc_ha_replica_connection_epoch(const tlc_ha_replica_t *replica);
/* Deprecated: production reconnects are performed by the control thread. */
int tlc_ha_replica_reconnect(tlc_ha_replica_t *replica, int control_fd,
                             uint64_t connection_epoch);
/* Returns whether the current-term Leader announcement was acknowledged. */
int tlc_ha_replica_leader_announce_acked(
        const tlc_ha_replica_t *replica);
/* Returns whether a valid peer heartbeat was received within the timeout. */
int tlc_ha_replica_peer_liveness(const tlc_ha_replica_t *replica);
/* Returns the current runtime HA ownership state. */
tlc_ha_replica_ha_state_t tlc_ha_replica_ha_state(
        const tlc_ha_replica_t *replica);
/* Returns consecutive timeout samples observed by the heartbeat detector. */
uint32_t tlc_ha_replica_heartbeat_missed_count(
        const tlc_ha_replica_t *replica);
/* Returns whether the detector has published a failure event to the controller. */
int tlc_ha_replica_heartbeat_failure_pending(
        const tlc_ha_replica_t *replica);

#endif
