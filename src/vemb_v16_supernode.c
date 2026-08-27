#define _GNU_SOURCE

#include "cpu_relax.h"
#include "vemb_v16_supernode.h"
#include "vemb_v16_dataplane.h"
#include "vemb_v16_log.h"
#include "vemb_v16_protocol.h"
#include "vemb_v16_stats.h"
#include "redisassert.h"
#include "sve_similarity.h"
#include "zmalloc.h"
#include "macro.h"

#include <stdlib.h>
#include <stdatomic.h>
#include <string.h>
#ifdef __linux__
#include <unistd.h>
#endif

#ifndef VEMB_V16_SUPERNODE_BATCH
#ifdef PROXY_QUEUE_BATCH
#define VEMB_V16_SUPERNODE_BATCH PROXY_QUEUE_BATCH
#else
#define VEMB_V16_SUPERNODE_BATCH 32u
#endif
#endif

typedef struct vemb_v16_inline_snapshot {
    uint32_t payload_bytes;
    uint32_t reserved;
    uint8_t payload[];
} vemb_v16_inline_snapshot_t;

static int job_shape_matches_tlc(uint32_t dim,
                                 uint32_t vector_bytes,
                                 const vemb_v16_tlc_t *tlc) {
    return dim == tlc->vector_dim &&
        vector_bytes == tlc->value_size;
}

static int job_key_is_source_cutover(vemb_v16_tlc_t *tlc,
                                     const char *key,
                                     uint32_t key_len,
                                     uint64_t key_hash,
                                     tlc_core_key_migration_info_t *info) {
    int rc = tlc_core_key_is_source_cutover(tlc->core,
                                            key,
                                            key_len,
                                            key_hash,
                                            info);
    return rc > 0;
}

static void completion_set_moved(vemb_v16_completion_t *completion,
                                 const tlc_core_key_migration_info_t *info) {
    completion->status = VEMB_V16_STATUS_MOVED;
    completion->redirect_owner = info ? info->target_owner : UINT32_MAX;
}

static void completion_set_ask(vemb_v16_completion_t *completion,
                               const tlc_core_key_migration_info_t *info) {
    completion->status = VEMB_V16_STATUS_ASK;
    completion->redirect_owner = info ? info->target_owner : UINT32_MAX;
}

typedef enum vemb_v16_lookup_miss_kind {
    VEMB_V16_LOOKUP_MISS_NOT_FOUND = 0,
    VEMB_V16_LOOKUP_MISS_STALE_TOPOLOGY = 1,
    VEMB_V16_LOOKUP_MISS_MOVED = 2,
} vemb_v16_lookup_miss_kind_t;

/* Lookup misses are rare; classify them from local per-key migration metadata
 * so a released migration-active counter cannot hide a source cutover fence.
 * This is not the VSIM remote-meta lookup path. */
static vemb_v16_lookup_miss_kind_t classify_lookup_miss(
        vemb_v16_tlc_t *tlc,
        const char *key,
        uint32_t key_len,
        uint64_t key_hash,
        tlc_core_key_migration_info_t *info) {
    assert(info != NULL);
    *info = (tlc_core_key_migration_info_t){
        .source_owner = UINT32_MAX,
        .target_owner = UINT32_MAX,
    };
    if (tlc_core_get_migration_info(tlc->core,
                                    key,
                                    key_len,
                                    key_hash,
                                    info) != 0) {
        return VEMB_V16_LOOKUP_MISS_NOT_FOUND;
    }

    switch (info->migration_state) {
    case TLC_CORE_KEY_CUTOVER:
    case TLC_CORE_KEY_SOURCE_GC:
        return VEMB_V16_LOOKUP_MISS_MOVED;
    case TLC_CORE_KEY_MIGRATING:
    case TLC_CORE_KEY_DEST_PREPARED:
    case TLC_CORE_KEY_DEST_COMMITTED:
        return VEMB_V16_LOOKUP_MISS_STALE_TOPOLOGY;
    case TLC_CORE_KEY_SOURCE_ACTIVE:
    default:
        return VEMB_V16_LOOKUP_MISS_NOT_FOUND;
    }
}

static void completion_set_lookup_miss(
        vemb_v16_completion_t *completion,
        vemb_v16_tlc_t *tlc,
        const char *key,
        uint32_t key_len,
        uint64_t key_hash) {
    tlc_core_key_migration_info_t info;
    vemb_v16_lookup_miss_kind_t kind = classify_lookup_miss(tlc,
                                                             key,
                                                             key_len,
                                                             key_hash,
                                                             &info);
    if (kind == VEMB_V16_LOOKUP_MISS_MOVED) {
        completion_set_moved(completion, &info);
    } else if (kind == VEMB_V16_LOOKUP_MISS_STALE_TOPOLOGY) {
        completion->status = VEMB_V16_STATUS_STALE_TOPOLOGY;
    } else {
        completion->status = VEMB_V16_STATUS_NOT_FOUND;
    }
    if (completion->op == VEMB_V16_OP_VEMB_HANDLE)
        vemb_v16_tlc_note_handle_lookup_miss(tlc, completion->status);
}

static void completion_set_vector_handle(
    vemb_v16_completion_t *completion,
    const vemb_v16_vector_handle_t *handle) {
    completion->vector_offset = handle->offset;
    completion->vector_bytes = handle->bytes;
    completion->region_id = handle->region_id;
    completion->local_slot = handle->local_slot;
    completion->owner_generation = handle->owner_generation;
}

static vemb_v16_inline_snapshot_t *inline_snapshot_from_payload(
    const uint8_t *payload) {
    assert(payload != NULL);
    return (vemb_v16_inline_snapshot_t *)(payload -
                                          offsetof(vemb_v16_inline_snapshot_t,
                                                   payload));
}

static uint8_t *completion_alloc_inline_snapshot(uint32_t vector_bytes) {
    size_t bytes = sizeof(vemb_v16_inline_snapshot_t) + vector_bytes;
    vemb_v16_inline_snapshot_t *snapshot = zmalloc(bytes);
    RETURN_IF(!snapshot, NULL);
    snapshot->payload_bytes = vector_bytes;
    snapshot->reserved = 0;
    return snapshot->payload;
}

void vemb_v16_completion_release_inline_snapshot(
    vemb_v16_completion_t *completion) {
    RETURN_IF(!completion || !completion->inline_vector);
    vemb_v16_inline_snapshot_t *snapshot =
        inline_snapshot_from_payload(completion->inline_vector);
    assert(snapshot->payload_bytes == completion->inline_vector_bytes);
    zfree(snapshot);
    completion->inline_vector = NULL;
    completion->inline_vector_bytes = 0;
}

static int snapshot_vemb_payload(vemb_v16_supernode_ctx_t *ctx,
                                 vemb_v16_tlc_t *tlc,
                                 uint8_t op,
                                 uint32_t vector_bytes,
                                 const vemb_v16_vector_handle_t *handle,
                                 vemb_v16_completion_t *completion) {
    (void)ctx;
    RETURN_IF(op != VEMB_V16_OP_VEMB_INLINE, -1);
    completion->inline_vector = completion_alloc_inline_snapshot(vector_bytes);
    RETURN_IF(!completion->inline_vector, -1);
    /*
     * Release may run on the copy-failed path below, so publish the snapshot
     * length as soon as the snapshot exists instead of waiting for load
     * success.
     */
    completion->inline_vector_bytes = vector_bytes;

    uint32_t vector_len = 0;
    int copy_rc = vemb_v16_tlc_load_vector(tlc,
                                           handle,
                                           completion->inline_vector,
                                           vector_bytes,
                                           &vector_len);
    if (copy_rc != 0 || unlikely(vector_len != vector_bytes)) {
        vemb_v16_completion_release_inline_snapshot(completion);
        return -1;
    }
    return 0;
}

static void vemb_v16_notify_completion_consumer(vemb_v16_supernode_ctx_t *ctx) {
#ifdef __linux__
    RETURN_IF(!ctx->completion_notify_armed);
    RETURN_IF(!ctx->completion_notify_fd);
    int expected = 1;
    if (!atomic_compare_exchange_strong_explicit(ctx->completion_notify_armed,
                                                 &expected,
                                                 0,
                                                 memory_order_acq_rel,
                                                 memory_order_relaxed)) {
        return;
    }

    int notify_fd = *ctx->completion_notify_fd;
    if (likely(notify_fd >= 0)) {
        uint64_t one = 1;
        (void)write(notify_fd, &one, sizeof(one));
    }
#else
    (void)ctx;
#endif
}

void vemb_v16_supernode_flush_completion_batch(vemb_v16_supernode_ctx_t *ctx,
                                               int notify) {
    uint32_t count = *ctx->completion_batch_count;
    RETURN_IF(count == 0);

    while (vemb_v16_aeron_publish_batch(ctx->completion_ring,
                                        ctx->completion_batch,
                                        count) != 0 &&
           atomic_load_explicit(ctx->running, memory_order_relaxed) &&
           atomic_load_explicit(ctx->channel_active, memory_order_acquire)) {
        cpu_relax();
    }
    *ctx->completion_batch_count = 0;
    if (notify)
        vemb_v16_notify_completion_consumer(ctx);
}

static void vemb_v16_publish_completion(vemb_v16_supernode_ctx_t *ctx,
                                        const vemb_v16_completion_t *completion) {
    if (!ctx->completion_batch || !ctx->completion_batch_count ||
        ctx->completion_batch_capacity == 0) {
        uint32_t spins = 0;
        int use_backoff = 1;
        while (vemb_v16_aeron_publish(ctx->completion_ring, completion) != 0 &&
               atomic_load_explicit(ctx->running, memory_order_relaxed) &&
               atomic_load_explicit(ctx->channel_active, memory_order_acquire)) {
            if (use_backoff) vemb_v16_aeron_backoff(spins++);
            else             cpu_relax();
        }
        vemb_v16_notify_completion_consumer(ctx);
        return;
    }

    uint32_t count = *ctx->completion_batch_count;
    if (count == ctx->completion_batch_capacity) {
        vemb_v16_supernode_flush_completion_batch(ctx, 0);
        count = 0;
    }
    ctx->completion_batch[count] = *completion;
    *ctx->completion_batch_count = count + 1;
}

void vemb_v16_supernode_handle_base_job(vemb_v16_supernode_ctx_t *ctx,
                                        const vemb_v16_job_base_t *job) {
    vemb_v16_completion_t completion = {
        .status = job->op == VEMB_V16_OP_PING ?
            VEMB_V16_STATUS_OK :
            VEMB_V16_STATUS_ERR,
        .op = job->op,
        .flags = job->flags,
        .req_id = job->req_id,
        .channel_index = job->channel_index,
        .channel_id = job->channel_id,
        .batch_token = job->batch_token,
        .key_hash = job->key_hash,
    };
    vemb_v16_publish_completion(ctx, &completion);
}

void vemb_v16_supernode_handle_vemb_job(vemb_v16_supernode_ctx_t *ctx,
                                        const vemb_v16_vemb_job_t *vemb_job) {
    vemb_v16_storage_ctx_t *storage = ctx->storage;
    vemb_v16_tlc_t *tlc = storage->tlc;
    const vemb_v16_job_base_t *job = &vemb_job->base;
    vemb_v16_completion_t completion = {
        .status = VEMB_V16_STATUS_OK,
        .op = job->op,
        .flags = job->flags,
        .req_id = job->req_id,
        .channel_index = job->channel_index,
        .channel_id = job->channel_id,
        .batch_token = job->batch_token,
        .key_hash = job->key_hash,
        .dim = vemb_job->dim,
        .vector_bytes = vemb_job->vector_bytes,
    };
    uint32_t warm_slot = 0;
    vemb_v16_vector_handle_t handle = {0};
    const char *err_reason = NULL;
    int migration_active = vemb_v16_storage_migration_active(storage);
    int needs_payload_snapshot = job->op == VEMB_V16_OP_VEMB_INLINE;
    if (unlikely(!job_shape_matches_tlc(vemb_job->dim,
                                        vemb_job->vector_bytes, tlc))) {
        completion.status = VEMB_V16_STATUS_ERR;
        err_reason = "shape_mismatch";
        goto finish_vemb_job;
    }
    if (migration_active &&
        vemb_v16_storage_write_epoch_is_stale(storage, job->topology_epoch)) {
        completion.status = VEMB_V16_STATUS_STALE_TOPOLOGY;
        goto finish_vemb_job;
    }

    if (needs_payload_snapshot &&
        vemb_v16_tlc_get_cached_handle(tlc,
                                       vemb_job->key,
                                       vemb_job->key_len,
                                       job->key_hash,
                                       &handle,
                                       &warm_slot) == 0) {
        completion_set_vector_handle(&completion, &handle);
        if (snapshot_vemb_payload(ctx,
                                  tlc,
                                  job->op,
                                  vemb_job->vector_bytes,
                                  &handle,
                                  &completion) == 0) {
            goto finish_vemb_job;
        }
        completion.status = VEMB_V16_STATUS_OK;
        completion.vector_bytes = vemb_job->vector_bytes;
    }

    int handle_rc = (!needs_payload_snapshot && !migration_active) ?
        vemb_v16_tlc_get_handle_stable_read(tlc,
                                            vemb_job->key,
                                            vemb_job->key_len,
                                            job->key_hash,
                                            &handle,
                                            &warm_slot) :
        vemb_v16_tlc_get_handle(tlc,
                                vemb_job->key,
                                vemb_job->key_len,
                                job->key_hash,
                                &handle,
                                &warm_slot);
    if (handle_rc != 0) {
        completion_set_lookup_miss(&completion,
                                   tlc,
                                   vemb_job->key,
                                   vemb_job->key_len,
                                   job->key_hash);
        serverLog(LL_WARNING,
                  "vemb_v16 handle miss: req_id=%u batch_token=%llu hash=%llu key=%.*s rc=%d status=%u migration_active=%d",
                  job->req_id, (unsigned long long)job->batch_token,
                  (unsigned long long)job->key_hash, (int)vemb_job->key_len,
                  vemb_job->key, handle_rc, completion.status,
                  migration_active);
    } else {
        completion_set_vector_handle(&completion, &handle);
        if (needs_payload_snapshot) {
            if (snapshot_vemb_payload(ctx,
                                      tlc,
                                      job->op,
                                      vemb_job->vector_bytes,
                                      &handle,
                                      &completion) != 0) {
                completion.status = VEMB_V16_STATUS_ERR;
                completion.vector_bytes = 0;
                err_reason = "snapshot_payload_failed";
            }
        }
    }

finish_vemb_job:
    if (completion.status == VEMB_V16_STATUS_ERR) {
        uint64_t current_epoch = 0;
        uint64_t min_write_epoch = 0;
        tlc_core_key_migration_info_t info = {
            .source_owner = UINT32_MAX,
            .target_owner = UINT32_MAX,
        };
        int info_rc = tlc_core_get_migration_info(tlc->core,
                                                  vemb_job->key,
                                                  vemb_job->key_len,
                                                  job->key_hash,
                                                  &info);
        vemb_v16_storage_epoch_get(storage, &current_epoch, &min_write_epoch);
        serverLog(LL_WARNING,
                  "vemb_v16 vemb request failed: req_id=%u op=%u key_hash=%llu key_len=%u reason=%s dim=%u/%u vector_bytes=%u/%u request_epoch=%llu current_epoch=%llu min_write_epoch=%llu migration_active=%d needs_payload_snapshot=%d info_rc=%d state=%u info_epoch=%llu owner_epoch=%llu source=%u target=%u shard=%u",
                  job->req_id,
                  job->op,
                  (unsigned long long)job->key_hash,
                  vemb_job->key_len,
                  err_reason ? err_reason : "unknown",
                  vemb_job->dim,
                  tlc->vector_dim,
                  vemb_job->vector_bytes,
                  tlc->value_size,
                  (unsigned long long)job->topology_epoch,
                  (unsigned long long)current_epoch,
                  (unsigned long long)min_write_epoch,
                  migration_active,
                  needs_payload_snapshot,
                  info_rc,
                  info.migration_state,
                  (unsigned long long)info.topology_epoch,
                  (unsigned long long)info.owner_epoch,
                  info.source_owner,
                  info.target_owner,
                  info.shard_id);
    }
    vemb_v16_publish_completion(ctx, &completion);
}

void vemb_v16_supernode_handle_vsim_key_key_job(
    vemb_v16_supernode_ctx_t *ctx,
    const vemb_v16_vsim_key_key_job_t *vsim_job) {
    vemb_v16_storage_ctx_t *storage = ctx->storage;
    vemb_v16_tlc_t *tlc = storage->tlc;
    const vemb_v16_job_base_t *job = &vsim_job->base;
    vemb_v16_completion_t completion = {
        .status = VEMB_V16_STATUS_OK,
        .op = job->op,
        .flags = job->flags,
        .req_id = job->req_id,
        .channel_index = job->channel_index,
        .channel_id = job->channel_id,
        .batch_token = job->batch_token,
        .key_hash = job->key_hash,
        .dim = vsim_job->dim,
        .vector_bytes = vsim_job->vector_bytes,
    };
    uint32_t warm_slot = 0;
    vemb_v16_vector_handle_t handle = {0};
    int migration_active = vemb_v16_storage_migration_active(storage);

    if (unlikely(!job_shape_matches_tlc(vsim_job->dim,
                                        vsim_job->vector_bytes, tlc))) {
        completion.status = VEMB_V16_STATUS_ERR;
        goto finish_vsim_job;
    }
    if (migration_active &&
        vemb_v16_storage_write_epoch_is_stale(storage, job->topology_epoch)) {
        completion.status = VEMB_V16_STATUS_STALE_TOPOLOGY;
        goto finish_vsim_job;
    }

    if (vemb_v16_tlc_get_handle(tlc, vsim_job->key, vsim_job->key_len,
                                job->key_hash, &handle, &warm_slot) != 0) {
        completion_set_lookup_miss(&completion,
                                   tlc,
                                   vsim_job->key,
                                   vsim_job->key_len,
                                   job->key_hash);
        goto finish_vsim_job;
    }

    completion_set_vector_handle(&completion, &handle);

    vemb_v16_vector_handle_t handle2 = {0};
    vemb_v16_tlc_lookup_source_t key2_source = VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
    if (vemb_v16_tlc_lookup_vsim_key2(tlc,
                                      vsim_job->key2,
                                      vsim_job->key2_len,
                                      vsim_job->key2_hash,
                                      &handle2,
                                      &key2_source) != 0) {
        completion_set_lookup_miss(&completion,
                                   tlc,
                                   vsim_job->key2,
                                   vsim_job->key2_len,
                                   vsim_job->key2_hash);
        serverLog(LL_NOTICE,
                  "vemb_v16 vsim key-key key2 lookup miss: req_id=%u key_hash=%llu key2_hash=%llu key2_len=%u status=%u",
                  job->req_id,
                  (unsigned long long)job->key_hash,
                  (unsigned long long)vsim_job->key2_hash,
                  vsim_job->key2_len,
                  completion.status);
        goto finish_vsim_job;
    }

    const uint8_t *v1_bytes = NULL;
    const uint8_t *v2_bytes = NULL;
    uint32_t v1_len = 0;
    uint32_t v2_len = 0;
    int v1_rc = vemb_v16_tlc_vector_slice(tlc,
                                          &handle,
                                          &v1_bytes,
                                          &v1_len);
    int v2_rc = vemb_v16_tlc_vector_slice(tlc,
                                          &handle2,
                                          &v2_bytes,
                                          &v2_len);
    if (unlikely(v1_rc != 0 || v2_rc != 0 ||
                 v1_len != vsim_job->vector_bytes ||
                 v2_len != vsim_job->vector_bytes)) {
        serverLog(LL_WARNING,
                  "vemb_v16 vsim key-key vector slice failed: req_id=%u key_hash=%llu key2_hash=%llu region1=%u offset1=%llu bytes1=%u region2=%u offset2=%llu bytes2=%u expected_bytes=%u",
                  job->req_id,
                  (unsigned long long)job->key_hash,
                  (unsigned long long)vsim_job->key2_hash,
                  handle.region_id,
                  (unsigned long long)handle.offset,
                  handle.bytes,
                  handle2.region_id,
                  (unsigned long long)handle2.offset,
                  handle2.bytes,
                  vsim_job->vector_bytes);
        completion.status = VEMB_V16_STATUS_ERR;
    } else {
        const float *v1 = (const float *)(const void *)v1_bytes;
        const float *v2 = (const float *)(const void *)v2_bytes;
        completion.score = sve_cosine_similarity_f32(v1, v2, vsim_job->dim);
        serverLog(LL_DEBUG,
                  "vemb_v16 vsim key-key ok: req_id=%u key_hash=%llu key2_hash=%llu key2_source=%u region1=%u offset1=%llu region2=%u offset2=%llu score=%f",
                  job->req_id,
                  (unsigned long long)job->key_hash,
                  (unsigned long long)vsim_job->key2_hash,
                  key2_source,
                  handle.region_id,
                  (unsigned long long)handle.offset,
                  handle2.region_id,
                  (unsigned long long)handle2.offset,
                  completion.score);
    }

finish_vsim_job:
    vemb_v16_publish_completion(ctx, &completion);
}

void vemb_v16_supernode_handle_vrem_job(vemb_v16_supernode_ctx_t *ctx,
                                        const vemb_v16_vemb_job_t *vrem_job) {
    vemb_v16_storage_ctx_t *storage = ctx->storage;
    vemb_v16_tlc_t *tlc = storage->tlc;
    const vemb_v16_job_base_t *job = &vrem_job->base;
    vemb_v16_completion_t completion = {
        .status = VEMB_V16_STATUS_OK,
        .op = job->op,
        .flags = job->flags,
        .req_id = job->req_id,
        .channel_index = job->channel_index,
        .channel_id = job->channel_id,
        .batch_token = job->batch_token,
        .key_hash = job->key_hash,
        .dim = vrem_job->dim,
        .vector_bytes = vrem_job->vector_bytes,
    };
    int stale_topology = 0;
    tlc_core_key_migration_info_t redirect_info = {0};
    uint64_t write_topology_epoch = job->topology_epoch;
    int ask_redirect = (job->flags & VEMB_V16_REQ_F_ASK_REDIRECT) != 0;
    vemb_v16_vector_handle_t existing = {0};
    uint32_t warm_slot = 0;
    int migration_active = vemb_v16_storage_migration_active(storage);

    if (ask_redirect) {
        if (vemb_v16_storage_ask_redirect_write_ready(
                storage,
                vrem_job->key,
                vrem_job->key_len,
                job->key_hash,
                &redirect_info) != 0) {
            completion_set_ask(&completion, &redirect_info);
        } else {
            write_topology_epoch = redirect_info.owner_epoch ?
                redirect_info.owner_epoch : redirect_info.topology_epoch;
        }
    } else if (migration_active) {
        if (vemb_v16_storage_write_epoch_is_stale(storage,
                                                  job->topology_epoch)) {
            stale_topology = 1;
        } else if (job_key_is_source_cutover(tlc,
                                             vrem_job->key,
                                             vrem_job->key_len,
                                             job->key_hash,
                                             &redirect_info)) {
            completion_set_moved(&completion, &redirect_info);
        } else if (vemb_v16_storage_migration_write_blocked_info(
                       storage,
                       vrem_job->key,
                       vrem_job->key_len,
                       job->key_hash,
                       &redirect_info,
                       NULL)) {
            completion_set_ask(&completion, &redirect_info);
        }
    }

    if (completion.status == VEMB_V16_STATUS_OK && !stale_topology) {
        if (vemb_v16_tlc_get_handle(tlc,
                                    vrem_job->key,
                                    vrem_job->key_len,
                                    job->key_hash,
                                    &existing,
                                    &warm_slot) != 0) {
            if (!ask_redirect) {
                completion_set_lookup_miss(&completion,
                                           tlc,
                                           vrem_job->key,
                                           vrem_job->key_len,
                                           job->key_hash);
            } else {
                completion.status = VEMB_V16_STATUS_NOT_FOUND;
                completion.vector_bytes = 0;
            }
        } else {
            tlc_core_key_migration_info_t delete_info = {0};
            if (vemb_v16_storage_delete_with_epoch(storage,
                                                   vrem_job->key,
                                                   vrem_job->key_len,
                                                   job->key_hash,
                                                   write_topology_epoch,
                                                   &delete_info,
                                                   NULL) == 0) {
                completion.vector_bytes = 0;
                completion.dim = 0;
                completion.region_id = UINT32_MAX;
                completion.local_slot = UINT32_MAX;
            } else {
                if (ask_redirect) {
                    completion_set_ask(&completion, &redirect_info);
                } else {
                    memset(&redirect_info, 0, sizeof(redirect_info));
                    if (migration_active &&
                        job_key_is_source_cutover(tlc,
                                                  vrem_job->key,
                                                  vrem_job->key_len,
                                                  job->key_hash,
                                                  &redirect_info)) {
                        completion_set_moved(&completion, &redirect_info);
                    } else {
                        completion.status = VEMB_V16_STATUS_ERR;
                    }
                }
            }
        }
    }
    if (stale_topology)
        completion.status = VEMB_V16_STATUS_STALE_TOPOLOGY;

    vemb_v16_publish_completion(ctx, &completion);
}

void vemb_v16_supernode_handle_vadd_job(vemb_v16_supernode_ctx_t *ctx,
                                        const vemb_v16_vadd_job_t *vadd_job) {
    vemb_v16_storage_ctx_t *storage = ctx->storage;
    vemb_v16_tlc_t *tlc = storage->tlc;
    const vemb_v16_job_base_t *job = &vadd_job->base;
    vemb_v16_completion_t completion = {
        .status = VEMB_V16_STATUS_OK,
        .op = job->op,
        .flags = job->flags,
        .req_id = job->req_id,
        .channel_index = job->channel_index,
        .channel_id = job->channel_id,
        .batch_token = job->batch_token,
        .key_hash = job->key_hash,
        .dim = vadd_job->dim,
        .vector_bytes = vadd_job->vector_bytes,
    };
    const char *err_reason = NULL;

    if ((job->op == VEMB_V16_OP_VADD ||
         job->op == VEMB_V16_OP_VSIM_INLINE) &&
        unlikely(!job_shape_matches_tlc(vadd_job->dim,
                                        vadd_job->vector_bytes, tlc))) {
        completion.status = VEMB_V16_STATUS_ERR;
        err_reason = "shape_mismatch";
        goto finish_vadd_job;
    }

    if (job->op == VEMB_V16_OP_VADD) {
        uint32_t warm_slot = 0;
        vemb_v16_vector_handle_t handle = {0};
        int put_rc = -1;
        int stale_topology = 0;
        uint64_t write_topology_epoch = job->topology_epoch;
        int ask_redirect = (job->flags & VEMB_V16_REQ_F_ASK_REDIRECT) != 0;
        int migration_active = vemb_v16_storage_migration_active(storage);
        tlc_core_key_migration_info_t redirect_info = {0};
        if (ask_redirect) {
            if (vemb_v16_storage_ask_redirect_write_ready(
                    storage,
                    vadd_job->key,
                    vadd_job->key_len,
                    job->key_hash,
                    &redirect_info) != 0) {
                completion_set_ask(&completion, &redirect_info);
            } else {
                write_topology_epoch = redirect_info.owner_epoch ?
                    redirect_info.owner_epoch : redirect_info.topology_epoch;
            }
        } else if (migration_active) {
            if (vemb_v16_storage_write_epoch_is_stale(storage,
                                                      job->topology_epoch)) {
                stale_topology = 1;
            } else if (job_key_is_source_cutover(tlc,
                                                 vadd_job->key,
                                                 vadd_job->key_len,
                                                 job->key_hash,
                                                 &redirect_info)) {
                completion_set_moved(&completion, &redirect_info);
            } else if (vemb_v16_storage_migration_write_blocked_info(
                           storage,
                           vadd_job->key,
                           vadd_job->key_len,
                           job->key_hash,
                           &redirect_info,
                           NULL)) {
                completion_set_ask(&completion, &redirect_info);
            }
        }

        // ASK is resolved before this point; entering here means local put can proceed.
        // normal vadd put if not redirected or moved, and topology is not stale
        if (completion.status == VEMB_V16_STATUS_OK && !stale_topology) {
            put_rc = vemb_v16_tlc_put_with_epoch(tlc,
                                                 vadd_job->key,
                                                 vadd_job->key_len,
                                                 job->key_hash,
                                                 vadd_job->vector,
                                                 vadd_job->vector_bytes,
                                                 write_topology_epoch,
                                                 &handle,
                                                 &warm_slot);
            if (put_rc != 0 && !ask_redirect) {
                tlc_core_key_migration_info_t redirect_info = {0};
                if (migration_active &&
                    job_key_is_source_cutover(tlc,
                                              vadd_job->key,
                                              vadd_job->key_len,
                                              job->key_hash,
                                              &redirect_info)) {
                    completion_set_moved(&completion, &redirect_info);
                }
            }
            // MOVED after the local put attempt must not continue local success completion.
            if (completion.status != VEMB_V16_STATUS_MOVED) {
                if (put_rc != 0) {
                    completion.status = VEMB_V16_STATUS_ERR;
                    err_reason = "tlc_put";
                } else {
                    completion_set_vector_handle(&completion, &handle);
                    if (!ask_redirect &&
                        migration_active &&
                        vemb_v16_storage_migration_delta_put_after_local_write(
                            storage,
                            vadd_job->key,
                            vadd_job->key_len,
                            job->key_hash,
                            &handle,
                            NULL) != 0) {
                        completion.status = VEMB_V16_STATUS_ERR;
                        err_reason = "delta_publish";
                    }
                    if (handle.bytes > 0 &&
                        completion.status == VEMB_V16_STATUS_OK &&
                        vemb_v16_tlc_remote_meta_owner_view_count(tlc) > 1) {
                        (void)enqueue_remote_meta_publish(tlc,
                                                          tlc->remote_meta_view,
                                                          vadd_job->key,
                                                          vadd_job->key_len,
                                                          job->key_hash,
                                                          &handle,
                                                          0);
                    }
                }
            }
        }
        if (stale_topology)
            completion.status = VEMB_V16_STATUS_STALE_TOPOLOGY;
    } else if (job->op == VEMB_V16_OP_VSIM_INLINE) {
        uint32_t warm_slot = 0;
        vemb_v16_vector_handle_t handle = {0};
        if (vemb_v16_tlc_get_handle(tlc, vadd_job->key, vadd_job->key_len,
                                    job->key_hash, &handle, &warm_slot) != 0) {
            completion_set_lookup_miss(&completion,
                                       tlc,
                                       vadd_job->key,
                                       vadd_job->key_len,
                                       job->key_hash);
        } else {
            const uint8_t *stored_bytes = NULL;
            uint32_t stored_len = 0;
            if (unlikely(vemb_v16_tlc_vector_slice(tlc, &handle,
                                                   &stored_bytes,
                                                   &stored_len) != 0 ||
                         stored_len != vadd_job->vector_bytes)) {
                completion.status = VEMB_V16_STATUS_ERR;
            } else {
                const float *stored = (const float *)(const void *)stored_bytes;
                completion_set_vector_handle(&completion, &handle);
                completion.score = sve_cosine_similarity_f32(stored, vadd_job->vector, vadd_job->dim);
            }
        }
    } else {
        completion.status = VEMB_V16_STATUS_ERR;
    }

finish_vadd_job:
    if (completion.status == VEMB_V16_STATUS_ERR &&
        job->op == VEMB_V16_OP_VADD) {
        uint64_t current_epoch = 0;
        uint64_t min_write_epoch = 0;
        tlc_core_key_migration_info_t info = {
            .source_owner = UINT32_MAX,
            .target_owner = UINT32_MAX,
        };
        int info_rc = tlc_core_get_migration_info(tlc->core,
                                                  vadd_job->key,
                                                  vadd_job->key_len,
                                                  job->key_hash,
                                                  &info);
        vemb_v16_storage_epoch_get(storage, &current_epoch, &min_write_epoch);
        serverLog(LL_WARNING,
                  "vemb_v16 vadd request failed: req_id=%u key_hash=%llu key_len=%u reason=%s dim=%u/%u vector_bytes=%u/%u request_epoch=%llu current_epoch=%llu min_write_epoch=%llu info_rc=%d state=%u info_epoch=%llu owner_epoch=%llu source=%u target=%u shard=%u",
                  job->req_id,
                  (unsigned long long)job->key_hash,
                  vadd_job->key_len,
                  err_reason ? err_reason : "unknown",
                  vadd_job->dim,
                  tlc->vector_dim,
                  vadd_job->vector_bytes,
                  tlc->value_size,
                  (unsigned long long)job->topology_epoch,
                  (unsigned long long)current_epoch,
                  (unsigned long long)min_write_epoch,
                  info_rc,
                  info.migration_state,
                  (unsigned long long)info.topology_epoch,
                  (unsigned long long)info.owner_epoch,
                  info.source_owner,
                  info.target_owner,
                  info.shard_id);
    }
    vemb_v16_publish_completion(ctx, &completion);
}

int vemb_v16_supernode_scratch_init(vemb_v16_supernode_scratch_t *scratch) {
    assert(scratch != NULL);
    memset(scratch, 0, sizeof(*scratch));
    scratch->job_refs = zmalloc(sizeof(*scratch->job_refs) *
                                VEMB_V16_SUPERNODE_BATCH);
    if (!scratch->job_refs) {
        vemb_v16_supernode_scratch_cleanup(scratch);
        return -1;
    }
    scratch->completion_batch = zmalloc(sizeof(*scratch->completion_batch) *
                                        VEMB_V16_SUPERNODE_BATCH);
    if (!scratch->completion_batch) {
        vemb_v16_supernode_scratch_cleanup(scratch);
        return -1;
    }
    return 0;
}

void vemb_v16_supernode_scratch_cleanup(vemb_v16_supernode_scratch_t *scratch) {
    assert(scratch != NULL);
    zfree(scratch->job_refs);
    zfree(scratch->completion_batch);
    memset(scratch, 0, sizeof(*scratch));
}
