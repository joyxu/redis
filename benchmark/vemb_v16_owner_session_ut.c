#include <assert.h>
#include <stdint.h>
#include <stdio.h>

#include "../clients/c/vemb_v16_owner_session.h"

static vemb_v16_owner_session_identity_t make_identity(uint32_t owner_id,
                                                        uint64_t epoch,
                                                        uint64_t generation)
{
    return (vemb_v16_owner_session_identity_t){
        .owner_id = owner_id,
        .topology_epoch = epoch,
        .owner_generation = generation,
    };
}

static void ready_v2(vemb_v16_owner_session_t *session, uint64_t epoch)
{
    assert(vemb_v16_owner_session_observe_topology(session, epoch, 0) == 0);
    vemb_v16_owner_session_begin_reopen(session);
    vemb_v16_owner_session_v2_channel_ready(session);
}

static void test_epoch_change_quiesces_then_reopens(void)
{
    vemb_v16_owner_session_t session;
    vemb_v16_owner_session_init(&session, 2, 7);
    ready_v2(&session, 41);

    vemb_v16_owner_session_identity_t old = make_identity(2, 41, 7);
    assert(vemb_v16_owner_session_select_submit_path(&session, &old) ==
           VEMB_V16_OWNER_SESSION_SUBMIT_V2);
    vemb_v16_owner_session_v2_batch_published(&session);

    assert(vemb_v16_owner_session_observe_topology(&session, 42, 0) == 1);
    assert(session.v2_state == VEMB_V16_OWNER_SESSION_V2_QUIESCING);
    vemb_v16_owner_session_identity_t next = make_identity(2, 42, 7);
    assert(vemb_v16_owner_session_select_submit_path(&session, &next) ==
           VEMB_V16_OWNER_SESSION_SUBMIT_V1);

    vemb_v16_owner_session_finish_quiescing(&session);
    assert(session.v2_state == VEMB_V16_OWNER_SESSION_V2_DRAINING);
    vemb_v16_owner_session_v2_batch_finished(&session);
    assert(session.v2_state == VEMB_V16_OWNER_SESSION_V1_ONLY);

    vemb_v16_owner_session_begin_reopen(&session);
    vemb_v16_owner_session_v2_channel_ready(&session);
    assert(vemb_v16_owner_session_select_submit_path(&session, &next) ==
           VEMB_V16_OWNER_SESSION_SUBMIT_V2);
}

static void test_migration_forces_v1_until_stable_reopen(void)
{
    vemb_v16_owner_session_t session;
    vemb_v16_owner_session_init(&session, 1, 3);
    ready_v2(&session, 9);
    vemb_v16_owner_session_v2_batch_published(&session);

    assert(vemb_v16_owner_session_observe_topology(&session, 9, 1) == 1);
    assert(session.v2_state == VEMB_V16_OWNER_SESSION_V2_QUIESCING);
    vemb_v16_owner_session_finish_quiescing(&session);
    vemb_v16_owner_session_v2_batch_finished(&session);
    assert(session.v2_state == VEMB_V16_OWNER_SESSION_V1_ONLY);

    vemb_v16_owner_session_identity_t identity = make_identity(1, 9, 3);
    assert(vemb_v16_owner_session_select_submit_path(&session, &identity) ==
           VEMB_V16_OWNER_SESSION_SUBMIT_V1);
    assert(vemb_v16_owner_session_observe_topology(&session, 10, 0) == 0);
    vemb_v16_owner_session_begin_reopen(&session);
    vemb_v16_owner_session_v2_channel_ready(&session);
    identity.topology_epoch = 10;
    assert(vemb_v16_owner_session_select_submit_path(&session, &identity) ==
           VEMB_V16_OWNER_SESSION_SUBMIT_V2);
}

static void test_generation_fences_v2_identity(void)
{
    vemb_v16_owner_session_t session;
    vemb_v16_owner_session_init(&session, 4, 12);
    ready_v2(&session, 31);
    vemb_v16_owner_session_identity_t stale = make_identity(4, 31, 11);
    assert(vemb_v16_owner_session_select_submit_path(&session, &stale) ==
           VEMB_V16_OWNER_SESSION_SUBMIT_V1);

    assert(vemb_v16_owner_session_begin_quiesce(&session) == 1);
    vemb_v16_owner_session_finish_quiescing(&session);
    vemb_v16_owner_session_rebind_owner_generation(&session, 13);
    vemb_v16_owner_session_begin_reopen(&session);
    vemb_v16_owner_session_v2_channel_failed(&session);
    assert(session.v2_state == VEMB_V16_OWNER_SESSION_V1_ONLY);
}

int main(void)
{
    test_epoch_change_quiesces_then_reopens();
    test_migration_forces_v1_until_stable_reopen();
    test_generation_fences_v2_identity();
    printf("vemb_v16_owner_session_ut: all tests passed\n");
    return 0;
}
