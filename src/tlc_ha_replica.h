#ifndef TLC_HA_REPLICA_H
#define TLC_HA_REPLICA_H

#include "tlc_core.h"

#include <stdint.h>

typedef struct tlc_ha_replica tlc_ha_replica_t;

typedef struct tlc_ha_replica_progress {
    uint64_t appended_seq;
    uint64_t durable_seq;
    uint64_t applied_seq;
    uint64_t peer_accepted_seq;
    uint64_t peer_durable_seq;
    uint64_t peer_applied_seq;
} tlc_ha_replica_progress_t;

typedef enum tlc_ha_replica_role {
    TLC_HA_REPLICA_LEADER = 1,
    TLC_HA_REPLICA_FOLLOWER = 2,
} tlc_ha_replica_role_t;

typedef enum tlc_ha_replica_transport {
    TLC_HA_REPLICA_TRANSPORT_STREAM = 0,
    TLC_HA_REPLICA_TRANSPORT_UB = 1,
} tlc_ha_replica_transport_t;

typedef enum tlc_ha_replica_health {
    TLC_HA_REPLICA_HEALTHY = 1,
    TLC_HA_REPLICA_DEGRADED = 2,
    TLC_HA_REPLICA_RECOVERING = 3,
    TLC_HA_REPLICA_FAILED = 4,
    TLC_HA_REPLICA_UNAVAILABLE = 5,
} tlc_ha_replica_health_t;

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
    int fd;
    tlc_ha_replica_role_t role;
    tlc_ha_replica_transport_t transport;
    tlc_ha_replica_ring_config_t tx_ring;
    tlc_ha_replica_ring_config_t rx_ring;
    uint32_t queue_capacity;
    uint32_t max_batch_events;
    uint32_t max_batch_bytes;
    /* Fixed sender identity used to reject cross-Node-group heartbeats. */
    uint64_t hpc_node_id;
    /* Fixed peer identity expected on the connected Replica channel. */
    uint64_t peer_node_id;
    /* Owner fencing term carried by events and heartbeats. */
    uint64_t ha_term;
    /* Local heartbeat cadence; zero selects the default. */
    uint32_t heartbeat_interval_ms;
    /* Receiver-local liveness timeout; zero selects the default. */
    uint32_t heartbeat_timeout_ms;
} tlc_ha_replica_config_t;

/*
 * Start a bidirectional Replica stream on an already connected blocking fd.
 * The fd is owned by the returned runtime. Leader events are sourced from the
 * core event sink; Follower frames are durably appended before async apply.
 * STREAM uses the connected fd. UB uses tx_ring/rx_ring as an independent
 * bidirectional shared-memory channel and does not use UB RPC pending slots.
 */
int tlc_ha_replica_start(tlc_ha_replica_t **out,
                         const tlc_ha_replica_config_t *config);
/* Reset one shared Replica ring while no producer or consumer is attached. */
int tlc_ha_replica_reset_ring(const tlc_ha_replica_ring_config_t *config);
void tlc_ha_replica_stop(tlc_ha_replica_t *replica);

/* Queue a contiguous Leader AOF range for retransmission after reconnect.
 * The caller must not concurrently append new Leader events while the range
 * is being enumerated; the sender remains alive and drains the queued range. */
int tlc_ha_replica_replay_from(tlc_ha_replica_t *replica,
                               uint64_t start_seq,
                               uint64_t end_seq);

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

#endif
