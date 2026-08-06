#include <assert.h>
#include <stdint.h>
#include <stdio.h>

#include "../clients/c/internal/vemb_v16_cli_deadline.h"
#include "../clients/c/internal/vemb_v16_cli_l0.h"

static void test_deadline_policy(void) {
    vemb_v16_cli_deadline_t deadline;
    vemb_v16_cli_deadline_init(&deadline, 100000);
    vemb_v16_cli_deadline_on_new_leader(&deadline, 1000);
    assert(deadline.deadline_ns == 101000);
    vemb_v16_cli_deadline_on_new_leader(&deadline, 90000);
    assert(deadline.deadline_ns == 101000);
    assert(!vemb_v16_cli_deadline_flush_due(&deadline, 1, 100999));
    assert(vemb_v16_cli_deadline_flush_due(&deadline, 1, 101000));
    vemb_v16_cli_deadline_after_progress(&deadline, 2, 101000);
    assert(deadline.deadline_ns == 201000);
    assert(!vemb_v16_cli_deadline_flush_due(&deadline, 2, 200999));
    assert(vemb_v16_cli_deadline_flush_due(&deadline, 2, 201000));
    vemb_v16_cli_deadline_after_progress(&deadline, 0, 101000);
    assert(deadline.deadline_ns == 0);

    vemb_v16_cli_deadline_init(&deadline, 0);
    assert(vemb_v16_cli_deadline_flush_due(&deadline, 1, 0));
    assert(!vemb_v16_cli_deadline_flush_due(&deadline, 0, 0));
}

static void test_pending_frame_accounting(void) {
    vemb_v16_cli_l0_t *l0 = vemb_v16_cli_l0_create(1);
    assert(l0);
    uint32_t entry, channel;
    assert(vemb_v16_cli_l0_submit(l0, "a", 1, 1, 1, &entry, &channel) ==
           VEMB_V16_CLI_L0_NEW_LEADER);
    assert(vemb_v16_cli_l0_pending_item_count(l0, channel) == 1);
    assert(vemb_v16_cli_l0_pending_frame_bytes(l0, channel) == 31);
    assert(vemb_v16_cli_l0_submit(l0, "bb", 2, 2, 2, &entry, &channel) ==
           VEMB_V16_CLI_L0_NEW_LEADER);
    assert(vemb_v16_cli_l0_pending_item_count(l0, channel) == 2);
    assert(vemb_v16_cli_l0_pending_frame_bytes(l0, channel) == 35);
    assert(vemb_v16_cli_l0_submit(l0, "a", 1, 1, 3, &entry, &channel) ==
           VEMB_V16_CLI_L0_COALESCED_FOLLOWER);
    assert(vemb_v16_cli_l0_pending_item_count(l0, channel) == 2);

    vemb_v16_cli_l0_batch_draft_t draft;
    assert(vemb_v16_cli_l0_prepare_batch(l0, channel, 1, 128, &draft) == 1);
    assert(vemb_v16_cli_l0_publish_batch(l0, &draft, 1) == 0);
    assert(vemb_v16_cli_l0_pending_item_count(l0, channel) == 1);
    assert(vemb_v16_cli_l0_pending_frame_bytes(l0, channel) == 32);
    vemb_v16_cli_l0_abort_all(l0, NULL, NULL);
    vemb_v16_cli_l0_destroy(l0);
}

int main(void) {
    test_deadline_policy();
    test_pending_frame_accounting();
    printf("vemb_v16_cli_deadline_ut: all tests passed\n");
    return 0;
}
