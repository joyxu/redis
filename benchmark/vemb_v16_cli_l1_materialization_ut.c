#include "../src/redisassert.h"
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "../clients/c/internal/vemb_v16_cli_l1.h"

/* Exercise the internal group finish order without requiring a live channel. */
#include "../clients/c/vemb_v16_aeron_batch_client.c"

typedef struct materialization_test_ctx {
    vemb_v16_cli_l1_t *l1;
    vemb_v16_aeron_batch_client_t *client;
    uint32_t hook_calls;
    uint32_t completion_calls;
    int hook_before_completion;
    uint16_t expected_status;
    uint8_t expected_vector_valid;
    uint8_t expected_hook_calls;
} materialization_test_ctx_t;

static void on_materialized_group(
    void *priv, const char *key, uint16_t key_len,
    const vemb_v16_resp_t *response,
    const vemb_v16_aeron_batch_vector_view_t *vector_view) {
    materialization_test_ctx_t *ctx = priv;
    assert(response->status == VEMB_V16_STATUS_OK);
    assert(vector_view->valid);
    assert(ctx->completion_calls == 0);
    ctx->hook_before_completion = 1;
    ctx->hook_calls++;
    assert(vemb_v16_cli_l1_put(ctx->l1, key, key_len,
                                vemb_v16_xxh3_64_str(key, key_len),
                                vector_view->data,
                                (uint16_t)vector_view->bytes) ==
           VEMB_V16_CLI_L1_PUT_INSERTED);
}

static void on_completion(void *priv, uint64_t caller_cookie,
                          const vemb_v16_resp_t *response,
                          const vemb_v16_aeron_batch_vector_view_t *vector_view) {
    materialization_test_ctx_t *ctx = priv;
    assert(response->status == ctx->expected_status);
    assert(vector_view->valid == ctx->expected_vector_valid);
    assert(ctx->hook_calls == ctx->expected_hook_calls);
    assert(caller_cookie != 0);
    ctx->completion_calls++;
}

static vemb_v16_cli_l0_completion_t publish_and_resolve_group(
    vemb_v16_cli_l0_t *l0, uint32_t channel_index) {
    vemb_v16_cli_l0_batch_draft_t draft;
    assert(vemb_v16_cli_l0_prepare_batch(l0, channel_index, 1, 1024,
                                          &draft) == 1);
    assert(vemb_v16_cli_l0_publish_batch(l0, &draft, 1) == 0);
    vemb_v16_cli_l0_completion_t completion;
    assert(vemb_v16_cli_l0_resolve_response(l0, channel_index, 1, 0,
                                             &completion) == 1);
    return completion;
}

static void test_materialization_fills_once_before_fanout(void) {
    vemb_v16_aeron_batch_client_t client = {0};
    client.l0 = vemb_v16_cli_l0_create(1);
    assert(client.l0);
    client.dim = 2;
    const char key[] = "key";
    uint32_t entry_id;
    uint32_t channel_index;
    uint64_t hash = vemb_v16_xxh3_64_str(key, 3);
    assert(vemb_v16_cli_l0_submit(client.l0, key, 3, hash, 11, &entry_id,
                                   &channel_index) ==
           VEMB_V16_CLI_L0_NEW_LEADER);
    assert(vemb_v16_cli_l0_submit(client.l0, key, 3, hash, 22, &entry_id,
                                   &channel_index) ==
           VEMB_V16_CLI_L0_COALESCED_FOLLOWER);

    const float vector[2] = {1.0f, 2.0f};
    vemb_v16_aeron_batch_vector_view_t vector_view = {
        .data = vector,
        .bytes = sizeof(vector),
        .attempted = 1,
        .valid = 1,
    };
    vemb_v16_resp_t response = {
        .status = VEMB_V16_STATUS_OK,
        .op = VEMB_V16_OP_VEMB_HANDLE,
        .vector_bytes = sizeof(vector),
    };
    vemb_v16_cli_l1_config_t l1_config = {
        .dim = 2,
        .entry_count = 4,
    };
    materialization_test_ctx_t ctx = {
        .l1 = vemb_v16_cli_l1_create(&l1_config),
        .client = &client,
        .expected_status = VEMB_V16_STATUS_OK,
        .expected_vector_valid = 1,
        .expected_hook_calls = 1,
    };
    assert(ctx.l1);
    vemb_v16_cli_l0_completion_t completion =
        publish_and_resolve_group(client.l0, channel_index);
    assert(completion.entry_id == entry_id);
    assert(batch_client_finish_materialized_group(
               &client, &completion, &response, &vector_view,
               on_materialized_group, &ctx, on_completion, &ctx) == 2);
    assert(ctx.hook_calls == 1);
    assert(ctx.completion_calls == 2);
    assert(ctx.hook_before_completion);

    vemb_v16_cli_l1_value_t value;
    assert(vemb_v16_cli_l1_lookup(ctx.l1, key, 3, hash, &value) == 1);
    assert(value.vector_bytes == sizeof(vector));
    assert(((const float *)value.vector)[0] == vector[0]);
    assert(vemb_v16_cli_l1_release(ctx.l1, &value.ref) == 0);
    vemb_v16_cli_l1_stats_t stats;
    vemb_v16_cli_l1_get_stats(ctx.l1, &stats);
    assert(stats.inserts == 1);

    vemb_v16_cli_l1_destroy(ctx.l1);
    vemb_v16_cli_l0_destroy(client.l0);
}

static void test_invalid_vector_does_not_fill(void) {
    vemb_v16_aeron_batch_client_t client = {0};
    client.l0 = vemb_v16_cli_l0_create(1);
    assert(client.l0);
    const char key[] = "bad";
    uint32_t entry_id;
    uint32_t channel_index;
    uint64_t hash = vemb_v16_xxh3_64_str(key, 3);
    assert(vemb_v16_cli_l0_submit(client.l0, key, 3, hash, 31, &entry_id,
                                   &channel_index) ==
           VEMB_V16_CLI_L0_NEW_LEADER);
    const float vector[2] = {3.0f, 4.0f};
    vemb_v16_aeron_batch_vector_view_t vector_view = {
        .data = vector,
        .bytes = sizeof(vector),
        .attempted = 1,
        .valid = 0,
    };
    vemb_v16_resp_t response = {
        .status = VEMB_V16_STATUS_OK,
        .op = VEMB_V16_OP_VEMB_HANDLE,
    };
    vemb_v16_cli_l1_config_t l1_config = {
        .dim = 2,
        .entry_count = 4,
    };
    materialization_test_ctx_t ctx = {
        .l1 = vemb_v16_cli_l1_create(&l1_config),
        .client = &client,
        .expected_status = VEMB_V16_STATUS_OK,
        .expected_vector_valid = 0,
    };
    assert(ctx.l1);
    vemb_v16_cli_l0_completion_t completion =
        publish_and_resolve_group(client.l0, channel_index);
    assert(completion.entry_id == entry_id);
    assert(batch_client_finish_materialized_group(
               &client, &completion, &response, &vector_view,
               on_materialized_group, &ctx, on_completion, &ctx) == 1);
    assert(ctx.hook_calls == 0);
    assert(ctx.completion_calls == 1);
    vemb_v16_cli_l1_value_t value;
    assert(vemb_v16_cli_l1_lookup(ctx.l1, key, 3, hash, &value) == 0);
    vemb_v16_cli_l1_destroy(ctx.l1);
    vemb_v16_cli_l0_destroy(client.l0);
}

static void test_non_ok_and_v1_fallback_do_not_fill(void) {
    const float vector[2] = {5.0f, 6.0f};
    vemb_v16_aeron_batch_vector_view_t vector_view = {
        .data = vector,
        .bytes = sizeof(vector),
        .attempted = 1,
        .valid = 1,
    };
    vemb_v16_cli_l1_config_t l1_config = {
        .dim = 2,
        .entry_count = 4,
    };

    vemb_v16_aeron_batch_client_t client = {0};
    client.l0 = vemb_v16_cli_l0_create(1);
    assert(client.l0);
    const char error_key[] = "err";
    uint32_t entry_id;
    uint32_t channel_index;
    uint64_t hash = vemb_v16_xxh3_64_str(error_key, 3);
    assert(vemb_v16_cli_l0_submit(client.l0, error_key, 3, hash, 41,
                                   &entry_id, &channel_index) ==
           VEMB_V16_CLI_L0_NEW_LEADER);
    materialization_test_ctx_t error_ctx = {
        .l1 = vemb_v16_cli_l1_create(&l1_config),
        .client = &client,
        .expected_status = VEMB_V16_STATUS_ERR,
        .expected_vector_valid = 1,
    };
    assert(error_ctx.l1);
    vemb_v16_resp_t error_response = {
        .status = VEMB_V16_STATUS_ERR,
        .op = VEMB_V16_OP_VEMB_HANDLE,
        .vector_bytes = sizeof(vector),
    };
    vemb_v16_cli_l0_completion_t completion =
        publish_and_resolve_group(client.l0, channel_index);
    assert(completion.entry_id == entry_id);
    assert(batch_client_finish_materialized_group(
               &client, &completion, &error_response, &vector_view,
               on_materialized_group, &error_ctx, on_completion,
               &error_ctx) == 1);
    assert(error_ctx.hook_calls == 0);
    assert(error_ctx.completion_calls == 1);
    vemb_v16_cli_l1_value_t value;
    assert(vemb_v16_cli_l1_lookup(error_ctx.l1, error_key, 3, hash,
                                   &value) == 0);
    vemb_v16_cli_l1_destroy(error_ctx.l1);
    vemb_v16_cli_l0_destroy(client.l0);

    client = (vemb_v16_aeron_batch_client_t){0};
    client.l0 = vemb_v16_cli_l0_create(1);
    assert(client.l0);
    const char fallback_key[] = "fallback";
    hash = vemb_v16_xxh3_64_str(fallback_key, 8);
    assert(vemb_v16_cli_l0_submit(client.l0, fallback_key, 8, hash, 51,
                                   &entry_id, &channel_index) ==
           VEMB_V16_CLI_L0_NEW_LEADER);
    assert(vemb_v16_cli_l0_mark_fallback_v1(client.l0, entry_id) == 0);
    materialization_test_ctx_t fallback_ctx = {
        .l1 = vemb_v16_cli_l1_create(&l1_config),
        .client = &client,
        .expected_status = VEMB_V16_STATUS_OK,
        .expected_vector_valid = 1,
    };
    assert(fallback_ctx.l1);
    vemb_v16_resp_t ok_response = {
        .status = VEMB_V16_STATUS_OK,
        .op = VEMB_V16_OP_VEMB_HANDLE,
        .vector_bytes = sizeof(vector),
    };
    const char *borrowed_key;
    uint16_t borrowed_key_len;
    uint32_t generation;
    assert(vemb_v16_cli_l0_get_group(client.l0, entry_id, &borrowed_key,
                                      &borrowed_key_len, &generation, NULL) ==
           0);
    assert(borrowed_key_len == sizeof(fallback_key) - 1u);
    assert(memcmp(borrowed_key, fallback_key, borrowed_key_len) == 0);
    completion = (vemb_v16_cli_l0_completion_t){
        .entry_id = entry_id,
        .generation = generation,
    };
    assert(batch_client_finish_materialized_group(
               &client, &completion, &ok_response, &vector_view,
               on_materialized_group, &fallback_ctx, on_completion,
               &fallback_ctx) == 1);
    assert(fallback_ctx.hook_calls == 0);
    assert(fallback_ctx.completion_calls == 1);
    assert(vemb_v16_cli_l1_lookup(fallback_ctx.l1, fallback_key, 8, hash,
                                   &value) == 0);
    vemb_v16_cli_l1_destroy(fallback_ctx.l1);
    vemb_v16_cli_l0_destroy(client.l0);
}

int main(void) {
    test_materialization_fills_once_before_fanout();
    test_invalid_vector_does_not_fill();
    test_non_ok_and_v1_fallback_do_not_fill();
    puts("vemb_v16_cli_l1_materialization_ut: all tests passed");
    return 0;
}
