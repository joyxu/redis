#ifndef VEMB_V16_OWNER_SESSION_H
#define VEMB_V16_OWNER_SESSION_H

#include <stdint.h>

/*
 * Internal owner-session state shared by the v1 and v2 submit paths. The
 * cluster core has already selected owner_id, topology_epoch and the current
 * owner channel generation before an identity reaches this boundary.
 */
typedef struct vemb_v16_owner_session_identity {
    uint64_t topology_epoch;
    uint64_t owner_generation;
    uint32_t owner_id;
} vemb_v16_owner_session_identity_t;

typedef enum vemb_v16_owner_session_v2_state {
    VEMB_V16_OWNER_SESSION_V1_ONLY = 0,
    VEMB_V16_OWNER_SESSION_V2_REOPENING,
    VEMB_V16_OWNER_SESSION_V2_READY,
    VEMB_V16_OWNER_SESSION_V2_QUIESCING,
    VEMB_V16_OWNER_SESSION_V2_DRAINING,
} vemb_v16_owner_session_v2_state_t;

typedef enum vemb_v16_owner_session_submit_path {
    VEMB_V16_OWNER_SESSION_SUBMIT_V1 = 0,
    VEMB_V16_OWNER_SESSION_SUBMIT_V2,
} vemb_v16_owner_session_submit_path_t;

typedef struct vemb_v16_owner_session {
    uint64_t owner_generation;
    uint64_t observed_topology_epoch;
    uint32_t owner_id;
    uint32_t published_v2_batches;
    vemb_v16_owner_session_v2_state_t v2_state;
    uint8_t topology_observed;
    uint8_t migration_active;
} vemb_v16_owner_session_t;

/* Strict internal API contract: the caller has a validated topology route and
 * invokes transitions from one worker only. owner_id is fixed for the lifetime
 * of the object. A v2 batch is counted from successful ring publication until
 * every response item has been handled or requeued by the common core. */

static inline int vemb_v16_owner_session_identity_equal(
    const vemb_v16_owner_session_identity_t *lhs,
    const vemb_v16_owner_session_identity_t *rhs)
{
    return lhs->owner_id == rhs->owner_id &&
        lhs->topology_epoch == rhs->topology_epoch &&
        lhs->owner_generation == rhs->owner_generation;
}

void vemb_v16_owner_session_init(vemb_v16_owner_session_t *session,
                                 uint32_t owner_id,
                                 uint64_t owner_generation);

/* Observe every topology snapshot before L0 grouping. A new epoch or an
 * active migration immediately blocks new v2 publication and returns 1 when
 * the caller must move unpublished groups to its v1/core requeue path. */
int vemb_v16_owner_session_observe_topology(
    vemb_v16_owner_session_t *session, uint64_t topology_epoch,
    int migration_active);

/* Rebinding is legal only after every v2 batch is drained and the session is
 * v1-only. This value is the core's owner-channel generation, never an ATTACH
 * topology epoch. */
void vemb_v16_owner_session_rebind_owner_generation(
    vemb_v16_owner_session_t *session, uint64_t owner_generation);

/* Resource attach is driven by the owner/session lifecycle after topology is
 * stable. A failed attach returns to v1-only; it does not change routing. */
void vemb_v16_owner_session_begin_reopen(vemb_v16_owner_session_t *session);
void vemb_v16_owner_session_v2_channel_ready(
    vemb_v16_owner_session_t *session);
void vemb_v16_owner_session_v2_channel_failed(
    vemb_v16_owner_session_t *session);

/* Transition READY to QUIESCING. The caller first requeues every unpublished
 * L0 group through cluster core, then calls finish_quiescing(). */
int vemb_v16_owner_session_begin_quiesce(
    vemb_v16_owner_session_t *session);
void vemb_v16_owner_session_finish_quiescing(
    vemb_v16_owner_session_t *session);

/* A response with epoch/redirect semantics requiring a core retry quiesces
 * v2 before its affected items are requeued. publish/finish bracket only
 * successfully published v2 frames. */
void vemb_v16_owner_session_v2_batch_published(
    vemb_v16_owner_session_t *session);
void vemb_v16_owner_session_v2_batch_finished(
    vemb_v16_owner_session_t *session);

/* Terminal owner teardown after every L0 group has been completed locally.
 * It is not a retry transition: the caller has already delivered ERR or
 * requeued every affected logical operation through cluster core. */
void vemb_v16_owner_session_abort_v2_batches(
    vemb_v16_owner_session_t *session);

vemb_v16_owner_session_submit_path_t
vemb_v16_owner_session_select_submit_path(
    const vemb_v16_owner_session_t *session,
    const vemb_v16_owner_session_identity_t *identity);

#endif
