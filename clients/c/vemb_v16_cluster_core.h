#ifndef VEMB_V16_CLUSTER_CORE_H
#define VEMB_V16_CLUSTER_CORE_H

#include "../../src/vemb_v16_client_topology.h"

#include <stdint.h>

/*
 * Transport-agnostic client cluster state. All callers are single-threaded
 * SDK workers. The decoded topology passed to publish_topology() is already
 * validated at the control-plane boundary.
 */
typedef enum vemb_v16_cluster_topology_state {
    VEMB_V16_CLUSTER_TOPOLOGY_UNINITIALIZED = 0,
    VEMB_V16_CLUSTER_TOPOLOGY_READY,
    VEMB_V16_CLUSTER_TOPOLOGY_STALE,
} vemb_v16_cluster_topology_state_t;

typedef struct vemb_v16_cluster_owner_channel {
    uint64_t generation;
    uint8_t ready;
} vemb_v16_cluster_owner_channel_t;

typedef struct vemb_v16_cluster_stats {
    uint64_t ask_redirects;
    uint64_t moved_redirects;
    uint64_t moved_target_unavailable;
    uint64_t stale_topology_responses;
    uint64_t topology_refresh_calls;
    uint64_t retry_exhaustions;
    uint64_t logical_successes;
    uint64_t logical_not_found;
    uint64_t logical_errors;
} vemb_v16_cluster_stats_t;

typedef struct vemb_v16_cluster_core {
    vemb_v16_client_topology_t topology;
    vemb_v16_cluster_owner_channel_t
        owner_channels[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];
    vemb_v16_cluster_stats_t stats;
    uint32_t retry_budget;
    uint64_t next_operation_id;
    uint8_t topology_enabled;
    vemb_v16_cluster_topology_state_t topology_state;
} vemb_v16_cluster_core_t;

typedef struct vemb_v16_cluster_operation {
    uint64_t operation_id;
    uint64_t key_hash;
    uint64_t topology_epoch;
    uint32_t target_owner;
    uint32_t moved_owner;
    uint32_t attempts;
    uint32_t moved_refresh_attempts;
    uint8_t ask_retry_pending;
    uint8_t ask_retry_used;
    uint8_t moved_retry_pending;
    uint8_t exhausted;
    uint8_t completed;
} vemb_v16_cluster_operation_t;

typedef struct vemb_v16_cluster_route {
    uint64_t topology_epoch;
    uint64_t owner_channel_generation;
    uint32_t owner_id;
    uint32_t request_flags;
} vemb_v16_cluster_route_t;

typedef enum vemb_v16_cluster_prepare_result {
    VEMB_V16_CLUSTER_PREPARE_READY = 0,
    VEMB_V16_CLUSTER_PREPARE_NEEDS_TOPOLOGY,
    VEMB_V16_CLUSTER_PREPARE_RETRY_EXHAUSTED,
} vemb_v16_cluster_prepare_result_t;

typedef enum vemb_v16_cluster_response_action {
    VEMB_V16_CLUSTER_RESPONSE_FINAL = 0,
    VEMB_V16_CLUSTER_RESPONSE_ASK_RETRY,
    VEMB_V16_CLUSTER_RESPONSE_REFRESH,
} vemb_v16_cluster_response_action_t;

void vemb_v16_cluster_core_init(vemb_v16_cluster_core_t *core,
                                int topology_enabled,
                                uint32_t retry_budget);
void vemb_v16_cluster_core_set_retry_budget(vemb_v16_cluster_core_t *core,
                                            uint32_t retry_budget);
/* Switch an existing seed client to topology routing. The next prepare()
 * requests a topology snapshot before selecting an owner. */
void vemb_v16_cluster_core_enable_topology(vemb_v16_cluster_core_t *core);
void vemb_v16_cluster_core_publish_topology(
    vemb_v16_cluster_core_t *core,
    const vemb_v16_client_topology_t *topology);
void vemb_v16_cluster_core_mark_topology_stale(vemb_v16_cluster_core_t *core);
int vemb_v16_cluster_core_topology_ready(
    const vemb_v16_cluster_core_t *core);
const vemb_v16_client_topology_t *vemb_v16_cluster_core_topology(
    const vemb_v16_cluster_core_t *core);

/* Assigns a stable logical identity. The caller keeps operation alive until
 * complete(); transport backends never retain its address. */
void vemb_v16_cluster_operation_init(vemb_v16_cluster_core_t *core,
                                     vemb_v16_cluster_operation_t *operation,
                                     uint64_t key_hash);
vemb_v16_cluster_prepare_result_t vemb_v16_cluster_core_prepare(
    vemb_v16_cluster_core_t *core,
    vemb_v16_cluster_operation_t *operation,
    vemb_v16_cluster_route_t *route);
vemb_v16_cluster_response_action_t vemb_v16_cluster_core_on_response(
    vemb_v16_cluster_core_t *core,
    vemb_v16_cluster_operation_t *operation,
    const vemb_v16_resp_t *response);
/* Complete each operation exactly once, after a terminal response or a
 * local transport failure. */
void vemb_v16_cluster_core_complete(vemb_v16_cluster_core_t *core,
                                    vemb_v16_cluster_operation_t *operation,
                                    uint8_t final_status);

/* The caller owns the transport channel and invokes these at its lifecycle
 * boundary. owner_id is a validated topology owner. Channel readiness is
 * independent of the topology epoch; the caller compares endpoint binding. */
int vemb_v16_cluster_core_owner_channel_ready(
    const vemb_v16_cluster_core_t *core,
    uint32_t owner_id);
void vemb_v16_cluster_core_owner_channel_opened(
    vemb_v16_cluster_core_t *core,
    uint32_t owner_id);
void vemb_v16_cluster_core_owner_channel_closed(
    vemb_v16_cluster_core_t *core,
    uint32_t owner_id);
const vemb_v16_cluster_stats_t *vemb_v16_cluster_core_stats(
    const vemb_v16_cluster_core_t *core);

#endif
