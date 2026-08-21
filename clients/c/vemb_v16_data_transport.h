#ifndef VEMB_V16_DATA_TRANSPORT_H
#define VEMB_V16_DATA_TRANSPORT_H

#include "../../src/vemb_v16_protocol.h"

#include <stddef.h>
#include <stdint.h>

/*
 * Internal client data-plane boundary. The cluster core owns logical
 * operation identity, routing, retries and final accounting. A transport
 * owns its channel state and converts its native completion into the stable
 * operation_id supplied at submit time.
 */

#define VEMB_V16_TRANSPORT_WAIT_FOREVER UINT32_MAX

typedef struct vemb_v16_data_transport_ops vemb_v16_data_transport_ops_t;

typedef struct vemb_v16_warm_view {
    uint32_t region_id;
    uint64_t region_bytes;
    size_t mapping_bytes;
    uint8_t *mapping_addr;
    uint8_t *mapped_addr;
} vemb_v16_warm_view_t;

typedef struct vemb_v16_data_channel {
    const vemb_v16_data_transport_ops_t *ops;
    void *state;
    uint64_t channel_id;
    uint64_t generation;
    /* Server-owned allocation identity.  It is distinct from the SDK-local
     * generation, which changes every time this channel is re-opened. */
    uint64_t resource_generation;
    /* The topology snapshot epoch at which resource_generation was last
     * authoritatively checked.  It is a check cadence, not a resource id. */
    uint64_t resource_checked_topology_epoch;
    char host[64];
    uint16_t port;
    vemb_v16_warm_view_t warm;
} vemb_v16_data_channel_t;

typedef struct vemb_v16_transport_open_spec {
    const char *host;
    uint16_t port;
    uint32_t vector_dim;
    uint32_t timeout_ms;
    uint32_t owner_id;
    /* Borrowed by open_owner_channel() only. It carries backend-private
     * resolver state without exposing it to cluster-core routing. */
    const void *backend_context;
} vemb_v16_transport_open_spec_t;

typedef struct vemb_v16_transport_submission {
    uint64_t operation_id;
    uint32_t wire_req_id;
    uint32_t owner_id;
    uint64_t submit_epoch;
    const vemb_v16_req_t *request;
    uint8_t *inline_vector;
    uint32_t inline_vector_cap;
} vemb_v16_transport_submission_t;

typedef struct vemb_v16_transport_completion {
    uint64_t operation_id;
    uint64_t channel_generation;
    uint32_t owner_id;
    uint64_t submit_epoch;
    vemb_v16_resp_t response;
    uint32_t inline_vector_bytes;
} vemb_v16_transport_completion_t;

typedef enum vemb_v16_transport_poll_result {
    VEMB_V16_TRANSPORT_POLL_FAILED = -1,
    VEMB_V16_TRANSPORT_POLL_EMPTY = 0,
    VEMB_V16_TRANSPORT_POLL_COMPLETION = 1,
} vemb_v16_transport_poll_result_t;

typedef enum vemb_v16_transport_resource_state {
    VEMB_V16_TRANSPORT_RESOURCE_FAILED = -1,
    VEMB_V16_TRANSPORT_RESOURCE_CURRENT = 0,
    VEMB_V16_TRANSPORT_RESOURCE_REATTACH = 1,
} vemb_v16_transport_resource_state_t;

/*
 * open_owner_channel() succeeds only with a ready data channel. poll() is
 * transport-neutral: timeout_ms == 0 is nonblocking and WAIT_FOREVER waits
 * for one completion. TCP waits for fd readability; UB checks a response
 * ring. A returned completion has already passed the transport's wire and
 * request-identity checks.
 *
 * submit() borrows request and inline_vector only for the duration of the
 * call. The transport must retain the operation identity and output buffer
 * association needed by a later poll(). UB/AERON is the only backend that
 * implements open_warm_region()/read_warm_vector(); TCP leaves both optional
 * hooks NULL because it never accepts VEMB_HANDLE. close_channel() releases
 * any UB warm view opened for the channel. read_warm_vector() resolves the
 * response's region_id inside that channel; callers must not dereference a
 * transport mapping directly.
 *
 * check_resource() is called only at an owner lifecycle boundary after a
 * newer topology snapshot. It must not allocate a replacement channel.
 * fence() has the strict precondition that the core already delivered every
 * completion for this owner; it returns 0 only when the transport has no
 * submitted request left. The core owns the actual completion drain so no
 * confirmed result is silently discarded during a reattach.
 */
struct vemb_v16_data_transport_ops {
    int (*open_owner_channel)(vemb_v16_data_channel_t *channel,
                              const vemb_v16_transport_open_spec_t *spec);
    int (*submit)(vemb_v16_data_channel_t *channel,
                  const vemb_v16_transport_submission_t *submission);
    vemb_v16_transport_poll_result_t (*poll)(
        vemb_v16_data_channel_t *channel,
        uint32_t timeout_ms,
        vemb_v16_transport_completion_t *out);
    /* UB/AERON capability hooks. TCP leaves them NULL. Callers establish the
     * UB-only VEMB_HANDLE precondition at the public SDK boundary. */
    int (*open_warm_region)(vemb_v16_data_channel_t *channel,
                            vemb_v16_warm_view_t **out);
    int (*read_warm_vector)(vemb_v16_data_channel_t *channel,
                            uint32_t region_id,
                            uint64_t offset,
                            uint32_t bytes,
                            void *out,
                            uint32_t out_cap);
    vemb_v16_transport_resource_state_t (*check_resource)(
        vemb_v16_data_channel_t *channel);
    int (*fence)(vemb_v16_data_channel_t *channel);
    void (*close_channel)(vemb_v16_data_channel_t *channel);
};

#endif
