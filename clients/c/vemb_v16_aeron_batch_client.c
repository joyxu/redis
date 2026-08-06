#include "../../src/fmacros.h"

#include "macro.h"
#include "vemb_v16_client_sdk.h"
#include "internal/vemb_v16_cli_deadline.h"
#include "internal/vemb_v16_cli_l0.h"
#include "../../src/vemb_v16_util.h"

#include <stdlib.h>
#include <stdatomic.h>
#include <string.h>

#define VEMB_V16_BATCH_CLIENT_DIRECT_PENDING 1024u

typedef struct vemb_v16_batch_client_group_pending {
    uint32_t req_id;
    uint32_t generation;
    uint8_t active;
} vemb_v16_batch_client_group_pending_t;

typedef struct vemb_v16_batch_client_direct_pending {
    uint32_t req_id;
    uint64_t caller_cookie;
    uint8_t active;
} vemb_v16_batch_client_direct_pending_t;

struct vemb_v16_aeron_batch_client {
    vemb_v16_aeron_channel_t *legacy_channel;
    vemb_v16_aeron_batch_channel_t *batch_channel;
    vemb_v16_cli_l0_t *l0;
    uint32_t dim;
    uint32_t effective_batch_size;
    uint32_t max_batch_bytes;
    uint64_t max_batch_delay_ns;
    vemb_v16_cli_deadline_t flush_deadline;
    uint32_t next_req_id;
    uint64_t next_batch_id;
    int batch_enabled;
    int closing;
    float *shared_vector;
    float *direct_vector;
    vemb_v16_batch_client_group_pending_t
        group_pending[VEMB_V16_CLI_L0_MAX_ENTRIES];
    vemb_v16_batch_client_direct_pending_t
        direct_pending[VEMB_V16_BATCH_CLIENT_DIRECT_PENDING];
    vemb_v16_aeron_batch_client_stats_t stats;
};

typedef struct vemb_v16_batch_client_fanout {
    vemb_v16_aeron_batch_completion_cb cb;
    void *priv;
    const vemb_v16_resp_t *response;
    uint32_t completed;
} vemb_v16_batch_client_fanout_t;

typedef struct vemb_v16_batch_client_vector_fanout {
    vemb_v16_aeron_batch_vector_completion_cb cb;
    void *priv;
    const vemb_v16_resp_t *response;
    const vemb_v16_aeron_batch_vector_view_t *vector_view;
    uint32_t completed;
} vemb_v16_batch_client_vector_fanout_t;

static atomic_uint_fast8_t batch_client_monotonic_state = 0;

static int batch_client_init_monotonic_clock(void) {
    uint_fast8_t state = atomic_load_explicit(&batch_client_monotonic_state,
                                              memory_order_acquire);
    if (state == 2)
        return 0;
    uint_fast8_t expected = 0;
    if (atomic_compare_exchange_strong_explicit(&batch_client_monotonic_state,
                                                &expected, 1,
                                                memory_order_acq_rel,
                                                memory_order_acquire)) {
        monotonicInit();
        atomic_store_explicit(&batch_client_monotonic_state, 2,
                              memory_order_release);
        return 0;
    }
    while (atomic_load_explicit(&batch_client_monotonic_state,
                                memory_order_acquire) != 2) {
    }
    return 0;
}

static uint32_t next_nonzero_u32(uint32_t *value) {
    uint32_t next = (*value)++;
    if (next == 0)
        next = (*value)++;
    return next;
}

static uint64_t next_nonzero_u64(uint64_t *value) {
    uint64_t next = (*value)++;
    if (next == 0)
        next = (*value)++;
    return next;
}

enum batch_client_flush_reason {
    BATCH_CLIENT_FLUSH_EXPLICIT = 0,
    BATCH_CLIENT_FLUSH_EAGER,
    BATCH_CLIENT_FLUSH_FULL,
    BATCH_CLIENT_FLUSH_DEADLINE,
};

static uint64_t batch_client_now_ns(
    const vemb_v16_aeron_batch_client_t *client) {
    return client->max_batch_delay_ns ? vemb_v16_monotonic_ns() : 0;
}

static void batch_client_deadline_after_progress(
    vemb_v16_aeron_batch_client_t *client) {
    uint32_t pending = vemb_v16_cli_l0_pending_item_count(client->l0, 0);
    if (pending == 0) {
        vemb_v16_cli_deadline_clear(&client->flush_deadline);
        return;
    }
    vemb_v16_cli_deadline_after_progress(&client->flush_deadline, pending,
                                         batch_client_now_ns(client));
}

static void batch_client_fanout(void *priv, uint64_t caller_cookie) {
    vemb_v16_batch_client_fanout_t *fanout = priv;
    if (fanout->cb)
        fanout->cb(fanout->priv, caller_cookie, fanout->response);
    fanout->completed++;
}

static void batch_client_vector_fanout(void *priv, uint64_t caller_cookie) {
    vemb_v16_batch_client_vector_fanout_t *fanout = priv;
    if (fanout->cb)
        fanout->cb(fanout->priv, caller_cookie, fanout->response,
                   fanout->vector_view);
    fanout->completed++;
}

static void batch_client_make_handle_request(
    vemb_v16_aeron_batch_client_t *client, const char *key, uint16_t key_len,
    uint32_t req_id, vemb_v16_req_t *request) {
    *request = (vemb_v16_req_t){
        .op = VEMB_V16_OP_VEMB_HANDLE,
        .req_id = req_id,
        .channel_id = vemb_v16_aeron_channel_id(client->legacy_channel),
        .key_hash = vemb_v16_xxh3_64_str(key, key_len),
        .key_len = key_len,
        .dim = client->dim,
        .vector_bytes = client->dim * sizeof(float),
    };
    memcpy(request->key, key, key_len);
}

static int batch_client_publish_request(vemb_v16_aeron_batch_client_t *client,
                                        const char *key, uint16_t key_len,
                                        uint32_t req_id) {
    vemb_v16_req_t request;
    uint8_t wire[VEMB_V16_AERON_REQ_WIRE_MAX_LEN];
    size_t wire_len = 0;
    batch_client_make_handle_request(client, key, key_len, req_id, &request);
    if (vemb_v16_req_encode(wire, sizeof(wire), &request, &wire_len) != 0)
        return -1;
    return vemb_v16_aeron_publish_request(client->legacy_channel, wire,
                                           (uint32_t)wire_len);
}

static int batch_client_submit_direct(vemb_v16_aeron_batch_client_t *client,
                                      const char *key, uint16_t key_len,
                                      uint64_t caller_cookie) {
    uint32_t slot = VEMB_V16_BATCH_CLIENT_DIRECT_PENDING;
    for (uint32_t i = 0; i < VEMB_V16_BATCH_CLIENT_DIRECT_PENDING; i++) {
        if (!client->direct_pending[i].active) {
            slot = i;
            break;
        }
    }
    if (slot == VEMB_V16_BATCH_CLIENT_DIRECT_PENDING)
        return -2;
    uint32_t req_id = next_nonzero_u32(&client->next_req_id);
    int rc = batch_client_publish_request(client, key, key_len, req_id);
    RETURN_IF(rc != 0, rc);
    client->direct_pending[slot] = (vemb_v16_batch_client_direct_pending_t){
        .req_id = req_id,
        .caller_cookie = caller_cookie,
        .active = 1,
    };
    client->stats.v1_direct_requests++;
    return 0;
}

static int batch_client_submit_group_v1(vemb_v16_aeron_batch_client_t *client,
                                        uint32_t entry_id) {
    if (entry_id >= VEMB_V16_CLI_L0_MAX_ENTRIES ||
        client->group_pending[entry_id].active)
        return -1;
    const char *key;
    uint16_t key_len;
    uint32_t generation;
    if (vemb_v16_cli_l0_get_group(client->l0, entry_id, &key, &key_len,
                                   &generation, NULL) != 0)
        return -1;
    uint32_t req_id = next_nonzero_u32(&client->next_req_id);
    int rc = batch_client_publish_request(client, key, key_len, req_id);
    RETURN_IF(rc != 0, rc);
    if (vemb_v16_cli_l0_mark_fallback_v1(client->l0, entry_id) != 0)
        return -1;
    client->group_pending[entry_id] = (vemb_v16_batch_client_group_pending_t){
        .req_id = req_id,
        .generation = generation,
        .active = 1,
    };
    client->stats.l0_fallback_v1++;
    return 0;
}

static void batch_client_disable_v2(vemb_v16_aeron_batch_client_t *client) {
    if (!client->batch_enabled)
        return;
    client->batch_enabled = 0;
}

static uint32_t batch_client_finish_group(
    vemb_v16_aeron_batch_client_t *client,
    const vemb_v16_cli_l0_completion_t *completion,
    const vemb_v16_resp_t *response,
    vemb_v16_aeron_batch_completion_cb cb, void *priv) {
    vemb_v16_batch_client_fanout_t fanout = {
        .cb = cb,
        .priv = priv,
        .response = response,
    };
    client->group_pending[completion->entry_id].active = 0;
    if (vemb_v16_cli_l0_finish(client->l0, completion, batch_client_fanout,
                                &fanout) != 0)
        return 0;
    return fanout.completed;
}

static void batch_client_materialize_vector(
    vemb_v16_aeron_batch_client_t *client, const vemb_v16_resp_t *response,
    float *out, vemb_v16_aeron_batch_vector_view_t *view) {
    *view = (vemb_v16_aeron_batch_vector_view_t){0};
    if (response->status != VEMB_V16_STATUS_OK)
        return;
    view->attempted = 1;
    int bytes = vemb_v16_aeron_read_vector(client->legacy_channel,
                                            response->region_id,
                                            response->vector_offset,
                                            response->vector_bytes, out,
                                            client->dim * sizeof(float));
    if (bytes <= 0)
        return;
    view->data = out;
    view->bytes = (uint32_t)bytes;
    view->valid = 1;
}

static uint32_t batch_client_finish_group_shared(
    vemb_v16_aeron_batch_client_t *client,
    const vemb_v16_cli_l0_completion_t *completion,
    const vemb_v16_resp_t *response,
    vemb_v16_aeron_batch_vector_completion_cb cb, void *priv) {
    vemb_v16_aeron_batch_vector_view_t vector_view;
    float *vector = client->shared_vector;
    batch_client_materialize_vector(client, response, vector, &vector_view);
    if (vector_view.attempted) {
        if (vector_view.valid) {
            client->stats.shared_vector_group_reads++;
            client->stats.shared_vector_group_bytes += vector_view.bytes;
        } else {
            client->stats.shared_vector_group_read_failures++;
        }
    }
    vemb_v16_batch_client_vector_fanout_t fanout = {
        .cb = cb,
        .priv = priv,
        .response = response,
        .vector_view = &vector_view,
    };
    client->group_pending[completion->entry_id].active = 0;
    if (vemb_v16_cli_l0_finish(client->l0, completion,
                                batch_client_vector_fanout, &fanout) != 0)
        return 0;
    if (vector_view.valid)
        client->stats.shared_vector_fanout += fanout.completed;
    return fanout.completed;
}

vemb_v16_aeron_batch_client_t *vemb_v16_aeron_batch_client_open_remote(
    const char *host, uint16_t port, uint32_t dim,
    const vemb_v16_aeron_batch_client_options_t *options) {
    if (!host || !host[0] || port == 0 || dim == 0 || dim > VEMB_V16_MAX_DIM)
        return NULL;
    vemb_v16_aeron_batch_client_t *client = calloc(1, sizeof(*client));
    if (!client)
        return NULL;
    client->dim = dim;
    client->next_req_id = 1;
    client->next_batch_id = 1;
    client->shared_vector = calloc(dim, sizeof(float));
    client->direct_vector = calloc(dim, sizeof(float));
    if (!client->shared_vector || !client->direct_vector)
        goto fail;
    client->legacy_channel = vemb_v16_aeron_open_remote(host, port, dim);
    if (!client->legacy_channel ||
        vemb_v16_aeron_open_warm_region(client->legacy_channel) != 0)
        goto fail;

    uint32_t requested_size = options && options->requested_batch_size ?
        options->requested_batch_size : 32u;
    uint32_t requested_bytes = options ? options->requested_max_batch_bytes : 0;
    uint32_t requested_delay_us = options ? options->max_batch_delay_us : 0;
    if (requested_delay_us != 0 && batch_client_init_monotonic_clock() != 0)
        goto fail;
    client->max_batch_delay_ns = (uint64_t)requested_delay_us * 1000u;
    vemb_v16_cli_deadline_init(&client->flush_deadline,
                               client->max_batch_delay_ns);
    client->batch_channel = vemb_v16_aeron_open_remote_batch(
        host, port, dim, requested_size, requested_bytes);
    if (!client->batch_channel)
        goto fail;
    vemb_v16_aeron_batch_resources_t resources;
    if (vemb_v16_aeron_batch_get_resources(client->batch_channel, &resources) != 0 ||
        !(client->l0 = vemb_v16_cli_l0_create(1)))
        goto disable_batch;
    client->effective_batch_size = resources.effective_batch_size;
    client->max_batch_bytes = resources.max_batch_bytes;
    client->batch_enabled = 1;
    return client;

disable_batch:
    vemb_v16_aeron_batch_close(client->batch_channel);
    client->batch_channel = NULL;
    goto fail;
fail:
    if (client->legacy_channel)
        vemb_v16_aeron_close(client->legacy_channel);
    free(client->direct_vector);
    free(client->shared_vector);
    free(client);
    return NULL;
}

static int batch_client_flush(vemb_v16_aeron_batch_client_t *client,
                              enum batch_client_flush_reason reason) {
    if (!client || client->closing)
        return -1;
    if (!client->batch_enabled)
        return 0;
    vemb_v16_cli_l0_batch_draft_t draft;
    int prepared = vemb_v16_cli_l0_prepare_batch(
        client->l0, 0, client->effective_batch_size, client->max_batch_bytes,
        &draft);
    if (prepared == -2) {
        client->stats.batch_flush_backpressure++;
        return -2;
    }
    if (prepared < 0)
        return -1;
    if (prepared == 0) {
        if (draft.oversized_entry_id == UINT32_MAX)
            return 0;
        int rc = batch_client_submit_group_v1(client, draft.oversized_entry_id);
        if (rc == 0)
            batch_client_deadline_after_progress(client);
        return rc;
    }
    uint64_t batch_id = next_nonzero_u64(&client->next_batch_id);
    int rc = vemb_v16_aeron_batch_publish_handle(
        client->batch_channel, batch_id, draft.keys, draft.key_lens,
        draft.item_count);
    if (rc != RING_OK) {
        if (rc == RING_ERR_FULL)
            client->stats.batch_flush_backpressure++;
        return rc;
    }
    if (vemb_v16_cli_l0_publish_batch(client->l0, &draft, batch_id) != 0)
        return -1;
    client->stats.batch_frames++;
    client->stats.batch_items += draft.item_count;
    client->stats.batch_frame_bytes += 28u +
        (uint64_t)draft.item_count * sizeof(uint16_t);
    for (uint32_t i = 0; i < draft.item_count; i++)
        client->stats.batch_frame_bytes += draft.key_lens[i];
    if (reason == BATCH_CLIENT_FLUSH_EAGER)
        client->stats.batch_flush_eager++;
    else if (reason == BATCH_CLIENT_FLUSH_FULL)
        client->stats.batch_flush_full++;
    else if (reason == BATCH_CLIENT_FLUSH_DEADLINE)
        client->stats.batch_flush_deadline++;
    batch_client_deadline_after_progress(client);
    return 0;
}

int vemb_v16_aeron_batch_client_flush(vemb_v16_aeron_batch_client_t *client) {
    return batch_client_flush(client, BATCH_CLIENT_FLUSH_EXPLICIT);
}

static int batch_client_flush_if_full(vemb_v16_aeron_batch_client_t *client) {
    uint32_t pending = vemb_v16_cli_l0_pending_item_count(client->l0, 0);
    uint32_t bytes = vemb_v16_cli_l0_pending_frame_bytes(client->l0, 0);
    if (pending < client->effective_batch_size && bytes < client->max_batch_bytes)
        return 0;
    return batch_client_flush(client, BATCH_CLIENT_FLUSH_FULL);
}

static int batch_client_flush_if_due(vemb_v16_aeron_batch_client_t *client) {
    uint32_t pending = vemb_v16_cli_l0_pending_item_count(client->l0, 0);
    if (pending == 0)
        return 0;
    uint64_t now_ns = batch_client_now_ns(client);
    if (!vemb_v16_cli_deadline_flush_due(&client->flush_deadline, pending,
                                         now_ns))
        return 0;
    return batch_client_flush(client, client->max_batch_delay_ns == 0 ?
                              BATCH_CLIENT_FLUSH_EAGER :
                              BATCH_CLIENT_FLUSH_DEADLINE);
}

int vemb_v16_aeron_batch_client_submit_handle(
    vemb_v16_aeron_batch_client_t *client, const char *final_key,
    uint16_t key_len, uint64_t caller_cookie) {
    if (!client || client->closing || !final_key || key_len == 0 ||
        key_len > VEMB_V16_MAX_KEY_LEN)
        return -1;
    if (!client->batch_enabled)
        return batch_client_submit_direct(client, final_key, key_len, caller_cookie);
    uint32_t entry_id, channel_index;
    int rc = vemb_v16_cli_l0_submit(client->l0, final_key, key_len,
                                     vemb_v16_xxh3_64_str(final_key, key_len),
                                     caller_cookie, &entry_id, &channel_index);
    if (rc == VEMB_V16_CLI_L0_NEW_LEADER) {
        vemb_v16_cli_deadline_on_new_leader(&client->flush_deadline,
                                            batch_client_now_ns(client));
        (void)batch_client_flush_if_full(client);
        return 0;
    }
    if (rc == VEMB_V16_CLI_L0_COALESCED_FOLLOWER)
        return 0;
    return batch_client_submit_direct(client, final_key, key_len, caller_cookie);
}

static int batch_client_poll_v1(vemb_v16_aeron_batch_client_t *client,
                                vemb_v16_aeron_batch_completion_cb cb,
                                void *priv) {
    vemb_v16_resp_t response;
    int rc = vemb_v16_aeron_poll_response(client->legacy_channel, &response,
                                           sizeof(response));
    if (rc <= 0)
        return rc;
    for (uint32_t i = 0; i < VEMB_V16_CLI_L0_MAX_ENTRIES; i++) {
        vemb_v16_batch_client_group_pending_t *pending =
            &client->group_pending[i];
        if (!pending->active || pending->req_id != response.req_id)
            continue;
        vemb_v16_cli_l0_completion_t completion = {
            .entry_id = i,
            .generation = pending->generation,
        };
        return (int)batch_client_finish_group(client, &completion, &response,
                                              cb, priv);
    }
    for (uint32_t i = 0; i < VEMB_V16_BATCH_CLIENT_DIRECT_PENDING; i++) {
        vemb_v16_batch_client_direct_pending_t *pending =
            &client->direct_pending[i];
        if (!pending->active || pending->req_id != response.req_id)
            continue;
        pending->active = 0;
        if (cb)
            cb(priv, pending->caller_cookie, &response);
        return 1;
    }
    return 0;
}

static int batch_client_poll_v1_shared(
    vemb_v16_aeron_batch_client_t *client,
    vemb_v16_aeron_batch_vector_completion_cb cb, void *priv) {
    vemb_v16_resp_t response;
    int rc = vemb_v16_aeron_poll_response(client->legacy_channel, &response,
                                           sizeof(response));
    if (rc <= 0)
        return rc;
    for (uint32_t i = 0; i < VEMB_V16_CLI_L0_MAX_ENTRIES; i++) {
        vemb_v16_batch_client_group_pending_t *pending =
            &client->group_pending[i];
        if (!pending->active || pending->req_id != response.req_id)
            continue;
        vemb_v16_cli_l0_completion_t completion = {
            .entry_id = i,
            .generation = pending->generation,
        };
        return (int)batch_client_finish_group_shared(client, &completion,
                                                     &response, cb, priv);
    }
    for (uint32_t i = 0; i < VEMB_V16_BATCH_CLIENT_DIRECT_PENDING; i++) {
        vemb_v16_batch_client_direct_pending_t *pending =
            &client->direct_pending[i];
        if (!pending->active || pending->req_id != response.req_id)
            continue;
        pending->active = 0;
        vemb_v16_aeron_batch_vector_view_t vector_view;
        batch_client_materialize_vector(client, &response, client->direct_vector,
                                        &vector_view);
        if (cb)
            cb(priv, pending->caller_cookie, &response, &vector_view);
        return 1;
    }
    return 0;
}

static int batch_client_poll_v2(vemb_v16_aeron_batch_client_t *client,
                                vemb_v16_aeron_batch_completion_cb cb,
                                void *priv) {
    if (!client->batch_channel)
        return 0;
    uint64_t batch_id, epoch;
    vemb_v16_resp_t responses[VEMB_V16_BATCH_REQUEST_SIZE_MAX];
    int count = vemb_v16_aeron_batch_poll_response(
        client->batch_channel, &batch_id, &epoch, responses);
    if (count == 0)
        return count;
    int callbacks = 0;
    int stale_epoch = epoch != vemb_v16_aeron_batch_topology_epoch(client->batch_channel);
    if (stale_epoch)
        client->stats.v2_stale_epochs++;
    for (int i = 0; i < count; i++) {
        vemb_v16_cli_l0_completion_t completion;
        if (vemb_v16_cli_l0_resolve_response(client->l0, 0, batch_id,
                                             (uint32_t)i, &completion) != 1)
            continue;
        if (stale_epoch || responses[i].status == VEMB_V16_STATUS_STALE_TOPOLOGY) {
            if (responses[i].status == VEMB_V16_STATUS_STALE_TOPOLOGY)
                client->stats.v2_stale_responses++;
            batch_client_disable_v2(client);
            if (batch_client_submit_group_v1(client, completion.entry_id) == 0)
                continue;
            vemb_v16_resp_t error = responses[i];
            error.status = VEMB_V16_STATUS_ERR;
            callbacks += (int)batch_client_finish_group(client, &completion,
                                                         &error, cb, priv);
            continue;
        }
        callbacks += (int)batch_client_finish_group(client, &completion,
                                                     &responses[i], cb, priv);
    }
    return callbacks;
}

static int batch_client_poll_v2_shared(
    vemb_v16_aeron_batch_client_t *client,
    vemb_v16_aeron_batch_vector_completion_cb cb, void *priv) {
    if (!client->batch_channel)
        return 0;
    uint64_t batch_id, epoch;
    vemb_v16_resp_t responses[VEMB_V16_BATCH_REQUEST_SIZE_MAX];
    int count = vemb_v16_aeron_batch_poll_response(
        client->batch_channel, &batch_id, &epoch, responses);
    if (count == 0)
        return count;
    int callbacks = 0;
    int stale_epoch = epoch !=
        vemb_v16_aeron_batch_topology_epoch(client->batch_channel);
    if (stale_epoch)
        client->stats.v2_stale_epochs++;
    for (int i = 0; i < count; i++) {
        vemb_v16_cli_l0_completion_t completion;
        if (vemb_v16_cli_l0_resolve_response(client->l0, 0, batch_id,
                                             (uint32_t)i, &completion) != 1)
            continue;
        if (stale_epoch || responses[i].status == VEMB_V16_STATUS_STALE_TOPOLOGY) {
            if (responses[i].status == VEMB_V16_STATUS_STALE_TOPOLOGY)
                client->stats.v2_stale_responses++;
            batch_client_disable_v2(client);
            if (batch_client_submit_group_v1(client, completion.entry_id) == 0)
                continue;
            vemb_v16_resp_t error = responses[i];
            error.status = VEMB_V16_STATUS_ERR;
            callbacks += (int)batch_client_finish_group_shared(client,
                                                                 &completion,
                                                                 &error, cb,
                                                                 priv);
            continue;
        }
        callbacks += (int)batch_client_finish_group_shared(client, &completion,
                                                             &responses[i], cb,
                                                             priv);
    }
    return callbacks;
}

int vemb_v16_aeron_batch_client_poll(
    vemb_v16_aeron_batch_client_t *client,
    vemb_v16_aeron_batch_completion_cb cb, void *priv) {
    if (!client || client->closing)
        return -1;
    (void)batch_client_flush_if_due(client);
    int callbacks = 0;
    int rc = batch_client_poll_v2(client, cb, priv);
    if (rc > 0)
        callbacks += rc;
    rc = batch_client_poll_v1(client, cb, priv);
    if (rc > 0)
        callbacks += rc;
    return callbacks;
}

int vemb_v16_aeron_batch_client_poll_shared_vector(
    vemb_v16_aeron_batch_client_t *client,
    vemb_v16_aeron_batch_vector_completion_cb cb, void *priv) {
    if (!client || client->closing)
        return -1;
    (void)batch_client_flush_if_due(client);
    int callbacks = 0;
    int rc = batch_client_poll_v2_shared(client, cb, priv);
    if (rc > 0)
        callbacks += rc;
    rc = batch_client_poll_v1_shared(client, cb, priv);
    if (rc > 0)
        callbacks += rc;
    return callbacks;
}

uint64_t vemb_v16_aeron_batch_client_next_flush_deadline_ns(
    const vemb_v16_aeron_batch_client_t *client) {
    if (!client || client->closing || !client->batch_enabled)
        return 0;
    return client->flush_deadline.deadline_ns;
}

int vemb_v16_aeron_batch_client_read_vector(
    vemb_v16_aeron_batch_client_t *client, uint32_t region_id,
    uint64_t offset, uint32_t bytes, float *out, uint32_t out_cap) {
    if (!client)
        return -1;
    return vemb_v16_aeron_read_vector(client->legacy_channel, region_id,
                                      offset, bytes, out, out_cap);
}

int vemb_v16_aeron_batch_client_batch_enabled(
    const vemb_v16_aeron_batch_client_t *client) {
    return client ? client->batch_enabled : 0;
}

void vemb_v16_aeron_batch_client_get_stats(
    const vemb_v16_aeron_batch_client_t *client,
    vemb_v16_aeron_batch_client_stats_t *out) {
    if (!client || !out)
        return;
    *out = client->stats;
    out->batch_enabled = client->batch_enabled;
    out->effective_batch_size = client->effective_batch_size;
    out->max_batch_bytes = client->max_batch_bytes;
    if (client->l0) {
        vemb_v16_cli_l0_stats_t l0_stats;
        vemb_v16_cli_l0_get_stats(client->l0, &l0_stats);
        out->l0_new_leader_groups = l0_stats.new_leader_groups;
        out->l0_coalesced_followers = l0_stats.coalesced_followers;
        out->l0_exact_key_mismatch = l0_stats.exact_key_mismatch;
        out->l0_bucket_full = l0_stats.bucket_full;
        out->l0_entry_exhausted = l0_stats.entry_exhausted;
        out->l0_follower_exhausted = l0_stats.follower_exhausted;
        out->l0_key_slab_exhausted = l0_stats.key_slab_exhausted;
        out->l0_stale_response = l0_stats.stale_response;
        out->l0_active_groups = l0_stats.active_groups;
    }
}

void vemb_v16_aeron_batch_client_close(
    vemb_v16_aeron_batch_client_t *client,
    vemb_v16_aeron_batch_completion_cb cb, void *priv) {
    if (!client)
        return;
    client->closing = 1;
    vemb_v16_resp_t error = {
        .status = VEMB_V16_STATUS_ERR,
        .op = VEMB_V16_OP_VEMB_HANDLE,
    };
    if (client->l0) {
        vemb_v16_batch_client_fanout_t fanout = {
            .cb = cb,
            .priv = priv,
            .response = &error,
        };
        vemb_v16_cli_l0_abort_all(client->l0, batch_client_fanout, &fanout);
        vemb_v16_cli_l0_destroy(client->l0);
    }
    free(client->direct_vector);
    free(client->shared_vector);
    for (uint32_t i = 0; i < VEMB_V16_BATCH_CLIENT_DIRECT_PENDING; i++) {
        if (!client->direct_pending[i].active)
            continue;
        if (cb)
            cb(priv, client->direct_pending[i].caller_cookie, &error);
    }
    if (client->batch_channel)
        vemb_v16_aeron_batch_close(client->batch_channel);
    if (client->legacy_channel)
        vemb_v16_aeron_close(client->legacy_channel);
    free(client);
}
