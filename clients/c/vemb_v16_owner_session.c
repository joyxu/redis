#include "vemb_v16_owner_session.h"

#include <assert.h>

void vemb_v16_owner_session_init(vemb_v16_owner_session_t *session,
                                 uint32_t owner_id,
                                 uint64_t owner_generation)
{
    *session = (vemb_v16_owner_session_t){
        .owner_id = owner_id,
        .owner_generation = owner_generation,
        .v2_state = VEMB_V16_OWNER_SESSION_V1_ONLY,
    };
}

int vemb_v16_owner_session_begin_quiesce(
    vemb_v16_owner_session_t *session)
{
    if (session->v2_state != VEMB_V16_OWNER_SESSION_V2_READY)
        return 0;
    session->v2_state = VEMB_V16_OWNER_SESSION_V2_QUIESCING;
    return 1;
}

int vemb_v16_owner_session_observe_topology(
    vemb_v16_owner_session_t *session, uint64_t topology_epoch,
    int migration_active)
{
    int changed = !session->topology_observed ||
        session->observed_topology_epoch != topology_epoch;
    session->observed_topology_epoch = topology_epoch;
    session->topology_observed = 1;
    session->migration_active = migration_active != 0;
    if (session->migration_active || changed)
        return vemb_v16_owner_session_begin_quiesce(session);
    return 0;
}

void vemb_v16_owner_session_rebind_owner_generation(
    vemb_v16_owner_session_t *session, uint64_t owner_generation)
{
    assert(session->v2_state == VEMB_V16_OWNER_SESSION_V1_ONLY);
    assert(session->published_v2_batches == 0);
    session->owner_generation = owner_generation;
}

void vemb_v16_owner_session_begin_reopen(vemb_v16_owner_session_t *session)
{
    assert(session->v2_state == VEMB_V16_OWNER_SESSION_V1_ONLY);
    assert(!session->migration_active);
    assert(session->published_v2_batches == 0);
    session->v2_state = VEMB_V16_OWNER_SESSION_V2_REOPENING;
}

void vemb_v16_owner_session_v2_channel_ready(
    vemb_v16_owner_session_t *session)
{
    assert(session->v2_state == VEMB_V16_OWNER_SESSION_V2_REOPENING);
    session->v2_state = VEMB_V16_OWNER_SESSION_V2_READY;
}

void vemb_v16_owner_session_v2_channel_failed(
    vemb_v16_owner_session_t *session)
{
    assert(session->v2_state == VEMB_V16_OWNER_SESSION_V2_REOPENING);
    session->v2_state = VEMB_V16_OWNER_SESSION_V1_ONLY;
}

void vemb_v16_owner_session_finish_quiescing(
    vemb_v16_owner_session_t *session)
{
    assert(session->v2_state == VEMB_V16_OWNER_SESSION_V2_QUIESCING);
    session->v2_state = session->published_v2_batches == 0 ?
        VEMB_V16_OWNER_SESSION_V1_ONLY :
        VEMB_V16_OWNER_SESSION_V2_DRAINING;
}

void vemb_v16_owner_session_v2_batch_published(
    vemb_v16_owner_session_t *session)
{
    assert(session->v2_state == VEMB_V16_OWNER_SESSION_V2_READY);
    session->published_v2_batches++;
}

void vemb_v16_owner_session_v2_batch_finished(
    vemb_v16_owner_session_t *session)
{
    assert(session->published_v2_batches != 0);
    session->published_v2_batches--;
    if (session->published_v2_batches == 0 &&
        session->v2_state == VEMB_V16_OWNER_SESSION_V2_DRAINING)
        session->v2_state = VEMB_V16_OWNER_SESSION_V1_ONLY;
}

void vemb_v16_owner_session_abort_v2_batches(
    vemb_v16_owner_session_t *session)
{
    assert(session->v2_state != VEMB_V16_OWNER_SESSION_V2_REOPENING);
    session->published_v2_batches = 0;
    session->v2_state = VEMB_V16_OWNER_SESSION_V1_ONLY;
}

vemb_v16_owner_session_submit_path_t
vemb_v16_owner_session_select_submit_path(
    const vemb_v16_owner_session_t *session,
    const vemb_v16_owner_session_identity_t *identity)
{
    assert(identity->owner_id == session->owner_id);
    if (session->v2_state == VEMB_V16_OWNER_SESSION_V2_READY &&
        !session->migration_active &&
        identity->owner_generation == session->owner_generation &&
        session->topology_observed &&
        identity->topology_epoch == session->observed_topology_epoch)
        return VEMB_V16_OWNER_SESSION_SUBMIT_V2;
    return VEMB_V16_OWNER_SESSION_SUBMIT_V1;
}
