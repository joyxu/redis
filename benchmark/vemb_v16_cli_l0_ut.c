#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "../clients/c/internal/vemb_v16_cli_l0.h"

typedef struct callback_log {
    uint64_t cookies[8192];
    uint32_t count;
} callback_log_t;

static void record_cookie(void *priv, uint64_t caller_cookie) {
    callback_log_t *log = priv;
    assert(log->count < sizeof(log->cookies) / sizeof(log->cookies[0]));
    log->cookies[log->count++] = caller_cookie;
}

static void test_coalesce_and_out_of_order_response(void) {
    vemb_v16_cli_l0_t *l0 = vemb_v16_cli_l0_create(2);
    assert(l0);
    uint32_t entry_a, entry_b, channel_a, channel_b;
    assert(vemb_v16_cli_l0_submit(l0, "key-a", 5, UINT64_C(0x110), 11,
                                   &entry_a, &channel_a) ==
           VEMB_V16_CLI_L0_NEW_LEADER);
    assert(vemb_v16_cli_l0_submit(l0, "key-a", 5, UINT64_C(0x110), 12,
                                   &entry_a, &channel_a) ==
           VEMB_V16_CLI_L0_COALESCED_FOLLOWER);
    /* Same fingerprint and full hash, but exact bytes must not coalesce. */
    assert(vemb_v16_cli_l0_submit(l0, "key-b", 5, UINT64_C(0x110), 13,
                                   &entry_b, &channel_b) ==
           VEMB_V16_CLI_L0_NEW_LEADER);
    assert(channel_a == channel_b);

    vemb_v16_cli_l0_batch_draft_t draft;
    assert(vemb_v16_cli_l0_prepare_batch(l0, channel_a, 8, 256, &draft) == 2);
    assert(vemb_v16_cli_l0_publish_batch(l0, &draft, 101) == 0);

    vemb_v16_cli_l0_completion_t completion_b;
    assert(vemb_v16_cli_l0_resolve_response(l0, channel_a, 101, 1,
                                             &completion_b) == 1);
    assert(vemb_v16_cli_l0_resolve_response(l0, channel_a, 101, 1,
                                             &completion_b) == 0);
    callback_log_t log = {0};
    assert(vemb_v16_cli_l0_finish(l0, &completion_b, record_cookie, &log) == 0);
    assert(log.count == 1 && log.cookies[0] == 13);

    vemb_v16_cli_l0_completion_t completion_a;
    assert(vemb_v16_cli_l0_resolve_response(l0, channel_a, 101, 0,
                                             &completion_a) == 1);
    assert(vemb_v16_cli_l0_finish(l0, &completion_a, record_cookie, &log) == 0);
    assert(log.count == 3 && log.cookies[1] == 11 && log.cookies[2] == 12);

    vemb_v16_cli_l0_stats_t stats;
    vemb_v16_cli_l0_get_stats(l0, &stats);
    assert(stats.new_leader_groups == 2);
    assert(stats.coalesced_followers == 1);
    assert(stats.exact_key_mismatch == 1);
    assert(stats.active_groups == 0);
    vemb_v16_cli_l0_destroy(l0);
}

static void test_fallback_and_generation_rejection(void) {
    vemb_v16_cli_l0_t *l0 = vemb_v16_cli_l0_create(1);
    assert(l0);
    uint32_t entry, channel;
    assert(vemb_v16_cli_l0_submit(l0, "fallback", 8, 7, 21, &entry, &channel) == 0);
    assert(vemb_v16_cli_l0_mark_fallback_v1(l0, entry) == 0);
    assert(vemb_v16_cli_l0_submit(l0, "fallback", 8, 7, 22, &entry, &channel) == 1);
    vemb_v16_cli_l0_completion_t old = {.entry_id = entry, .generation = 1};
    callback_log_t log = {0};
    assert(vemb_v16_cli_l0_finish(l0, &old, record_cookie, &log) == 0);
    assert(log.count == 2 && log.cookies[0] == 21 && log.cookies[1] == 22);

    uint32_t reused;
    assert(vemb_v16_cli_l0_submit(l0, "new-key", 7, 8, 23, &reused, &channel) == 0);
    assert(reused == old.entry_id);
    assert(vemb_v16_cli_l0_finish(l0, &old, record_cookie, &log) == -1);
    vemb_v16_cli_l0_abort_all(l0, record_cookie, &log);
    assert(log.count == 3 && log.cookies[2] == 23);
    vemb_v16_cli_l0_destroy(l0);
}

static void test_capacity_fallbacks(void) {
    vemb_v16_cli_l0_t *bucket_l0 = vemb_v16_cli_l0_create(1);
    assert(bucket_l0);
    char key[32];
    uint32_t entry, channel;
    for (uint32_t i = 0; i < VEMB_V16_CLI_L0_BUCKET_SLOTS; i++) {
        int n = snprintf(key, sizeof(key), "bucket-%u", i);
        assert(n > 0);
        assert(vemb_v16_cli_l0_submit(bucket_l0, key, (uint16_t)n,
                                       UINT64_C(0x42) | ((uint64_t)i << 8), i,
                                       &entry, &channel) == 0);
    }
    assert(vemb_v16_cli_l0_submit(bucket_l0, "bucket-overflow", 15,
                                   UINT64_C(0x42) | (UINT64_C(99) << 8), 99,
                                   &entry, &channel) ==
           VEMB_V16_CLI_L0_BUCKET_FULL);
    vemb_v16_cli_l0_abort_all(bucket_l0, NULL, NULL);
    vemb_v16_cli_l0_destroy(bucket_l0);

    vemb_v16_cli_l0_t *follower_l0 = vemb_v16_cli_l0_create(1);
    assert(follower_l0);
    assert(vemb_v16_cli_l0_submit(follower_l0, "hot", 3, 1, 0,
                                   &entry, &channel) == 0);
    for (uint32_t i = 0; i < VEMB_V16_CLI_L0_MAX_FOLLOWERS; i++) {
        assert(vemb_v16_cli_l0_submit(follower_l0, "hot", 3, 1, i + 1,
                                       &entry, &channel) == 1);
    }
    assert(vemb_v16_cli_l0_submit(follower_l0, "hot", 3, 1, 9999,
                                   &entry, &channel) ==
           VEMB_V16_CLI_L0_FOLLOWER_EXHAUSTED);
    vemb_v16_cli_l0_abort_all(follower_l0, NULL, NULL);
    vemb_v16_cli_l0_destroy(follower_l0);
}

static void make_binary_key(char *key, uint16_t key_len, uint32_t value) {
    memset(key, 'x', key_len);
    memcpy(key, &value, sizeof(value));
}

static void test_key_slab_and_entry_pool_exhaustion(void) {
    char key[VEMB_V16_MAX_KEY_LEN];
    uint32_t entry, channel;
    vemb_v16_cli_l0_t *slab_l0 = vemb_v16_cli_l0_create(1);
    assert(slab_l0);
    for (uint32_t i = 0; i < 512; i++) {
        make_binary_key(key, 16, i);
        assert(vemb_v16_cli_l0_submit(slab_l0, key, 16, i, i,
                                       &entry, &channel) == 0);
    }
    make_binary_key(key, 16, 513);
    assert(vemb_v16_cli_l0_submit(slab_l0, key, 16, 513, 513,
                                   &entry, &channel) ==
           VEMB_V16_CLI_L0_KEY_SLAB_EXHAUSTED);
    vemb_v16_cli_l0_abort_all(slab_l0, NULL, NULL);
    vemb_v16_cli_l0_destroy(slab_l0);

    vemb_v16_cli_l0_t *entry_l0 = vemb_v16_cli_l0_create(1);
    assert(entry_l0);
    const uint16_t lengths[] = {16, 32, 64, 80};
    const uint32_t counts[] = {512, 256, 128, 128};
    uint32_t value = 0;
    for (uint32_t cls = 0; cls < 4; cls++) {
        for (uint32_t i = 0; i < counts[cls]; i++, value++) {
            make_binary_key(key, lengths[cls], value);
            assert(vemb_v16_cli_l0_submit(entry_l0, key, lengths[cls],
                                           value, value, &entry, &channel) == 0);
        }
    }
    make_binary_key(key, 16, value);
    assert(vemb_v16_cli_l0_submit(entry_l0, key, 16, value, value,
                                   &entry, &channel) ==
           VEMB_V16_CLI_L0_ENTRY_EXHAUSTED);
    vemb_v16_cli_l0_abort_all(entry_l0, NULL, NULL);
    vemb_v16_cli_l0_destroy(entry_l0);
}

static void test_prepared_draft_keeps_group_indexed(void) {
    vemb_v16_cli_l0_t *l0 = vemb_v16_cli_l0_create(1);
    assert(l0);
    uint32_t entry, channel;
    assert(vemb_v16_cli_l0_submit(l0, "queued", 6, 5, 31,
                                   &entry, &channel) == 0);
    vemb_v16_cli_l0_batch_draft_t draft;
    assert(vemb_v16_cli_l0_prepare_batch(l0, channel, 8, 128, &draft) == 1);
    assert(vemb_v16_cli_l0_submit(l0, "queued", 6, 5, 32,
                                   &entry, &channel) == 1);
    callback_log_t log = {0};
    vemb_v16_cli_l0_abort_all(l0, record_cookie, &log);
    assert(log.count == 2 && log.cookies[0] == 31 && log.cookies[1] == 32);
    vemb_v16_cli_l0_destroy(l0);
}

static void test_identity_fences_groups_and_batches(void) {
    vemb_v16_cli_l0_t *l0 = vemb_v16_cli_l0_create(1);
    assert(l0);
    const vemb_v16_owner_session_identity_t epoch_7 = {
        .owner_id = 2, .topology_epoch = 7, .owner_generation = 4,
    };
    const vemb_v16_owner_session_identity_t epoch_8 = {
        .owner_id = 2, .topology_epoch = 8, .owner_generation = 4,
    };
    const vemb_v16_owner_session_identity_t other_owner = {
        .owner_id = 3, .topology_epoch = 8, .owner_generation = 4,
    };
    uint32_t entry_a, entry_b, entry_c, channel;
    assert(vemb_v16_cli_l0_submit_with_identity(
               l0, "same", 4, 19, 71, &epoch_7, &entry_a, &channel) ==
           VEMB_V16_CLI_L0_NEW_LEADER);
    assert(vemb_v16_cli_l0_submit_with_identity(
               l0, "same", 4, 19, 72, &epoch_7, &entry_a, &channel) ==
           VEMB_V16_CLI_L0_COALESCED_FOLLOWER);
    assert(vemb_v16_cli_l0_submit_with_identity(
               l0, "same", 4, 19, 73, &epoch_8, &entry_b, &channel) ==
           VEMB_V16_CLI_L0_NEW_LEADER);
    assert(vemb_v16_cli_l0_submit_with_identity(
               l0, "same", 4, 19, 74, &other_owner, &entry_c, &channel) ==
           VEMB_V16_CLI_L0_NEW_LEADER);
    assert(entry_a != entry_b && entry_b != entry_c);

    vemb_v16_cli_l0_batch_draft_t draft;
    assert(vemb_v16_cli_l0_prepare_batch(l0, channel, 8, 256, &draft) == 1);
    assert(vemb_v16_owner_session_identity_equal(&draft.identity, &epoch_7));
    assert(vemb_v16_cli_l0_publish_batch(l0, &draft, 401) == 0);
    vemb_v16_owner_session_identity_t published;
    assert(vemb_v16_cli_l0_get_batch_identity(l0, channel, 401, &published) == 0);
    assert(vemb_v16_owner_session_identity_equal(&published, &epoch_7));

    assert(vemb_v16_cli_l0_prepare_batch(l0, channel, 8, 256, &draft) == 1);
    assert(vemb_v16_owner_session_identity_equal(&draft.identity, &epoch_8));
    vemb_v16_cli_l0_abort_all(l0, NULL, NULL);
    vemb_v16_cli_l0_destroy(l0);
}

int main(void) {
    test_coalesce_and_out_of_order_response();
    test_fallback_and_generation_rejection();
    test_capacity_fallbacks();
    test_key_slab_and_entry_pool_exhaustion();
    test_prepared_draft_keeps_group_indexed();
    test_identity_fences_groups_and_batches();
    printf("vemb_v16_cli_l0_ut: all tests passed\n");
    return 0;
}
