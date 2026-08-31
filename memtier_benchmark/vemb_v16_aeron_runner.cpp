/*
 * Copyright (C) 2026 Redis Labs Ltd.
 *
 * This file is part of memtier_benchmark.
 *
 * memtier_benchmark is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "vemb_v16_aeron_runner.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

/* Transport mode: "aeron" (TCP control + local UB ring) or
 * "aeron-cross-node" (TCP attach + UB ring).
 * Set by main() from --vemb-v16-transport via vemb_v16_aeron_set_transport().
 * Default = "aeron" preserves existing loopback behavior. */
static std::string g_aeron_transport_mode = "aeron";
static std::string g_aeron_remote_endpoint;  /* "host:port" for cross-node */
static bool g_aeron_control_uds = false;  /* control plane: 0=tcp://, 1=UDS path */

void vemb_v16_aeron_set_transport(const std::string &mode,
                                  const std::string &endpoint,
                                  bool control_uds) {
    g_aeron_control_uds = control_uds;
    g_aeron_transport_mode = mode;
    g_aeron_remote_endpoint = endpoint;
}

extern "C" {
#include "vemb_v16_protocol.h"
#include "vemb_v16_client_sdk.h"
#include "vemb_v16_vector_gen.h"
#include "monotonic.h"
}

enum {
    COMMON_CORE_SET_CMD_IDX = 0,
    COMMON_CORE_GET_CMD_IDX = 2,
    /* sync-op 每次批量: 摊薄每批提交/轮询开销; 每组 inflight 仍受 pipeline 限制 */
    COMMON_CORE_SYNC_BATCH_MAX = 128,
};

struct common_core_worker;

struct common_core_pending {
    struct timeval sent_time;
    uint32_t bytes_tx;
};

struct common_core_slot {
    common_core_worker *worker;
    vemb_v16_client_t *client;
    vemb_v16_client_handle_session_t *handle_session;
    vemb_v16_client_vector_session_t *vector_session;
    std::unordered_map<uint64_t, common_core_pending> pending;
    uint64_t drive_count;
    uint64_t drive_queue_ns_sum;
    uint64_t drive_queue_ns_min;
    uint64_t drive_queue_ns_max;
    uint64_t poll_ns_sum;
    uint64_t poll_ns_min;
    uint64_t poll_ns_max;
    uint64_t callback_count;
    uint64_t empty_drive_count;
};

struct common_core_worker {
    benchmark_config *cfg;
    object_generator *obj_gen;
    std::vector<const char *> *seeds;
    uint32_t worker_id;
    unsigned long long budget;
    unsigned long long issued;
    unsigned long long completed;
    unsigned long long status_ok;
    unsigned long long status_not_found;
    unsigned long long status_error;
    unsigned long long materialized_ok;
    unsigned long long materialized_fail;
    unsigned long long unmatched;
    unsigned long set_ratio_count;
    unsigned long get_ratio_count;
    uint64_t key2_rng;
    uint64_t completion_sample_index;
    uint64_t completion_sample_count;
    uint64_t completion_latency_ns_sum;
    uint64_t completion_latency_ns_min;
    uint64_t completion_latency_ns_max;
    uint64_t completion_lookup_ns_sum;
    uint64_t completion_lookup_ns_min;
    uint64_t completion_lookup_ns_max;
    uint64_t materialize_sample_count;
    uint64_t materialize_ns_sum;
    uint64_t materialize_ns_min;
    uint64_t materialize_ns_max;
    uint64_t completion_account_ns_sum;
    uint64_t completion_account_ns_min;
    uint64_t completion_account_ns_max;
    bool use_handle_session;
    bool setup_failed;
    bool round_robin_slots;
    uint64_t cli_stats_interval_ns;
    run_stats *stats;
    std::atomic<bool> *stop;
    std::vector<common_core_slot> slots;
    size_t next_slot;
};

static uint64_t common_core_completion_sample_start(
    common_core_worker *worker, bool *sampled)
{
    uint64_t index = worker->completion_sample_index++;
    *sampled = (index & 1023u) == 0;
    return *sampled ? getMonotonicNs() : 0;
}

static void common_core_completion_sample_end(
    common_core_worker *worker, bool sampled, uint64_t start_ns)
{
    if (!sampled)
        return;
    uint64_t latency_ns = getMonotonicNs() - start_ns;
    worker->completion_sample_count++;
    worker->completion_latency_ns_sum += latency_ns;
    if (worker->completion_latency_ns_min == 0 ||
        latency_ns < worker->completion_latency_ns_min)
        worker->completion_latency_ns_min = latency_ns;
    if (latency_ns > worker->completion_latency_ns_max)
        worker->completion_latency_ns_max = latency_ns;
}

static void common_core_relax(void)
{
#if defined(__x86_64__) || defined(__i386__)
    __asm__ volatile("pause" ::: "memory");
#elif defined(__aarch64__) || defined(__arm__)
    __asm__ volatile("yield" ::: "memory");
#else
    __asm__ volatile("" ::: "memory");
#endif
}

static int common_core_obj_iter_type(const benchmark_config *cfg,
                                     unsigned char index)
{
    if (cfg->key_pattern[index] == 'R')
        return OBJECT_GENERATOR_KEY_RANDOM;
    if (cfg->key_pattern[index] == 'G')
        return OBJECT_GENERATOR_KEY_GAUSSIAN;
    if (cfg->key_pattern[index] == 'Z')
        return OBJECT_GENERATOR_KEY_ZIPFIAN;
    if (index == COMMON_CORE_SET_CMD_IDX)
        return OBJECT_GENERATOR_KEY_SET_ITER;
    return OBJECT_GENERATOR_KEY_GET_ITER;
}

/* key2: 同 key1 前缀 + [key_min,key_max] 随机编号 (LCG 与 protocol.cpp TCP 路径一致) */
static void common_core_next_key2(common_core_worker *w,
                                  const std::string &k1,
                                  char *out, size_t out_cap)
{
    size_t prefix_len = k1.size();
    while (prefix_len > 0 && k1[prefix_len - 1] >= '0' &&
           k1[prefix_len - 1] <= '9')
        prefix_len--;
    w->key2_rng = w->key2_rng * 6364136223846793005ULL +
                  1442695040888963407ULL;
    unsigned long long range =
        w->cfg->key_maximum - w->cfg->key_minimum + 1;
    snprintf(out, out_cap, "%.*s%llu", (int)prefix_len, k1.c_str(),
             (unsigned long long)(w->cfg->key_minimum +
                                  (w->key2_rng % range)));
}

static std::string common_core_next_key(common_core_worker *worker,
                                        bool is_set)
{
    int iterator = common_core_obj_iter_type(
        worker->cfg, is_set ? COMMON_CORE_SET_CMD_IDX : COMMON_CORE_GET_CMD_IDX);
    unsigned long long key_index = worker->obj_gen->get_key_index(iterator);
    worker->obj_gen->generate_key(key_index);
    return std::string(worker->obj_gen->get_key(), worker->obj_gen->get_key_len());
}

static uint8_t common_core_next_op(common_core_worker *worker)
{
    if (worker->cfg->vemb_v16_vsim_key_key)
        return VEMB_V16_OP_VSIM_KEY_KEY;
    if (worker->cfg->vemb_v16_vsim)
        return VEMB_V16_OP_VSIM_INLINE;
    if (worker->cfg->vemb_v16_vrem)
        return VEMB_V16_OP_VREM;
    if (worker->cfg->ratio.a > 0 && worker->cfg->ratio.b == 0)
        return VEMB_V16_OP_VADD;
    if (worker->cfg->ratio.b > 0 && worker->cfg->ratio.a == 0)
        return VEMB_V16_OP_VEMB_HANDLE;

    if (worker->set_ratio_count < worker->cfg->ratio.a) {
        worker->set_ratio_count++;
        if (worker->set_ratio_count >= worker->cfg->ratio.a &&
            worker->get_ratio_count >= worker->cfg->ratio.b) {
            worker->set_ratio_count = 0;
            worker->get_ratio_count = 0;
        }
        return VEMB_V16_OP_VADD;
    }
    worker->get_ratio_count++;
    if (worker->set_ratio_count >= worker->cfg->ratio.a &&
        worker->get_ratio_count >= worker->cfg->ratio.b) {
        worker->set_ratio_count = 0;
        worker->get_ratio_count = 0;
    }
    return VEMB_V16_OP_VEMB_HANDLE;
}

/* key-key 走 async handle session (v1) 的条件:
 * 不依赖 L1 cache / 未禁用 batch (与 pure read 的 session 条件对齐) */
static bool common_core_key_key_session(const benchmark_config *cfg)
{
    return cfg->vemb_v16_vsim_key_key &&
        cfg->vemb_v16_l1_entries == 0 && !cfg->vemb_v16_batch_disable;
}

static bool common_core_pure_read(const benchmark_config *cfg)
{
    return !cfg->vemb_v16_vsim && !cfg->vemb_v16_vsim_key_key &&
        !cfg->vemb_v16_vrem &&
        cfg->ratio.a == 0 && cfg->ratio.b > 0;
}

static bool common_core_l1_entries_valid(uint32_t entry_count)
{
    if (entry_count < 4 || entry_count % 4 != 0)
        return false;
    uint32_t set_count = entry_count / 4;
    return (set_count & (set_count - 1u)) == 0;
}

static void common_core_account_read(common_core_worker *worker,
                                     const common_core_pending &pending,
                                     int status, uint32_t bytes_rx,
                                     bool materialized)
{
    struct timeval now;
    gettimeofday(&now, NULL);
    unsigned int hits = 0;
    unsigned int misses = 0;

    if (status == 0) {
        worker->status_ok++;
        if (materialized) {
            worker->materialized_ok++;
            hits = 1;
        } else {
            worker->materialized_fail++;
            worker->status_error++;
        }
    } else if (status == 1) {
        worker->status_not_found++;
        misses = 1;
    } else {
        worker->status_error++;
    }
    worker->stats->update_get_op(
        &now, bytes_rx, pending.bytes_tx,
        (unsigned int)ts_diff(pending.sent_time, now), hits, misses);
    worker->completed++;
}

static void common_core_account_set(common_core_worker *worker,
                                    const common_core_pending &pending,
                                    bool succeeded)
{
    struct timeval now;
    gettimeofday(&now, NULL);
    if (succeeded)
        worker->status_ok++;
    else
        worker->status_error++;
    worker->stats->update_set_op(
        &now, 0, pending.bytes_tx,
        (unsigned int)ts_diff(pending.sent_time, now));
    worker->completed++;
}

static void common_core_handle_completion(
    void *priv, uint64_t caller_cookie,
    const vemb_v16_pipeline_resp_t *response)
{
    common_core_slot *slot = static_cast<common_core_slot *>(priv);
    bool sampled = false;
    uint64_t sample_start = common_core_completion_sample_start(
        slot->worker, &sampled);
    std::unordered_map<uint64_t, common_core_pending>::iterator it =
        slot->pending.find(caller_cookie);
    if (it == slot->pending.end()) {
        slot->worker->unmatched++;
        common_core_completion_sample_end(slot->worker, sampled, sample_start);
        return;
    }

    common_core_pending pending = it->second;
    slot->pending.erase(it);
    uint64_t lookup_ns = sampled ? getMonotonicNs() - sample_start : 0;
    bool materialized = false;
    uint64_t materialize_start_ns = sampled ? getMonotonicNs() : 0;
    if (slot->worker->cfg->vemb_v16_vsim_key_key) {
        /* key-key 返回 score, 无向量物化 */
        materialized = response->status == 0;
    } else if (response->status == 0) {
        float vector[VEMB_V16_MAX_DIM];
        materialized = vemb_v16_client_read_vector(
            slot->client, response->offset, response->bytes, vector,
            slot->worker->cfg->vemb_v16_dim) == 0;
    }
    uint64_t materialize_ns = sampled ?
        getMonotonicNs() - materialize_start_ns : 0;
    common_core_account_read(slot->worker, pending, response->status,
                             response->bytes, materialized);
    uint64_t account_ns = sampled ?
        getMonotonicNs() - (materialize_start_ns + materialize_ns) : 0;
    if (sampled) {
        common_core_worker *worker = slot->worker;
        worker->completion_lookup_ns_sum += lookup_ns;
        if (worker->completion_lookup_ns_min == 0 ||
            lookup_ns < worker->completion_lookup_ns_min)
            worker->completion_lookup_ns_min = lookup_ns;
        if (lookup_ns > worker->completion_lookup_ns_max)
            worker->completion_lookup_ns_max = lookup_ns;
        worker->materialize_sample_count++;
        worker->materialize_ns_sum += materialize_ns;
        if (worker->materialize_ns_min == 0 ||
            materialize_ns < worker->materialize_ns_min)
            worker->materialize_ns_min = materialize_ns;
        if (materialize_ns > worker->materialize_ns_max)
            worker->materialize_ns_max = materialize_ns;
        worker->completion_account_ns_sum += account_ns;
        if (worker->completion_account_ns_min == 0 ||
            account_ns < worker->completion_account_ns_min)
            worker->completion_account_ns_min = account_ns;
        if (account_ns > worker->completion_account_ns_max)
            worker->completion_account_ns_max = account_ns;
    }
    common_core_completion_sample_end(slot->worker, sampled, sample_start);
}

static void common_core_vector_completion(
    void *priv, uint64_t caller_cookie,
    const vemb_v16_pipeline_resp_t *response, const float *vector)
{
    common_core_slot *slot = static_cast<common_core_slot *>(priv);
    bool sampled = false;
    uint64_t sample_start = common_core_completion_sample_start(
        slot->worker, &sampled);
    std::unordered_map<uint64_t, common_core_pending>::iterator it =
        slot->pending.find(caller_cookie);
    if (it == slot->pending.end()) {
        slot->worker->unmatched++;
        common_core_completion_sample_end(slot->worker, sampled, sample_start);
        return;
    }

    common_core_pending pending = it->second;
    slot->pending.erase(it);
    common_core_account_read(slot->worker, pending, response->status,
                             response->bytes,
                             response->status != 0 || vector != NULL);
    common_core_completion_sample_end(slot->worker, sampled, sample_start);
}

static void common_core_close_slot(common_core_slot *slot)
{
    if (slot->handle_session) {
        vemb_v16_client_handle_session_close(
            slot->handle_session, common_core_handle_completion, slot);
        slot->handle_session = NULL;
    }
    if (slot->vector_session) {
        vemb_v16_client_vector_session_close(
            slot->vector_session, common_core_vector_completion, slot);
        slot->vector_session = NULL;
    }
    if (slot->client) {
        vemb_v16_client_destroy(slot->client);
        slot->client = NULL;
    }
}

static int common_core_prepare_slot(common_core_slot *slot)
{
    common_core_worker *worker = slot->worker;
    slot->client = vemb_v16_client_create(
        worker->seeds->data(), (int)worker->seeds->size(),
        worker->cfg->vemb_v16_dim, 10000, VEMB_V16_TRANSPORT_AERON);
    if (!slot->client)
        return -1;

    /* Every Aeron deployment uses the client-side peer-view resolver. A
     * same-host manifest has identical provider/client paths; it is not a
     * reason to bypass mapping or select a different transport path. */
    if (!worker->cfg->vemb_v16_ub_peer_view_manifest ||
        !worker->cfg->vemb_v16_ub_peer_view_manifest[0] ||
        !worker->cfg->vemb_v16_ub_peer_view_client_host ||
        !worker->cfg->vemb_v16_ub_peer_view_client_host[0] ||
        vemb_v16_client_configure_ub_peer_view(
            slot->client, worker->cfg->vemb_v16_ub_peer_view_manifest,
            worker->cfg->vemb_v16_ub_peer_view_client_host) != 0) {
        fprintf(stderr,
                "vemb_v16 aeron runner: peer-view manifest and client host are required\n");
        return -1;
    }
    if (vemb_v16_client_set_ub_batch_request_size(
            slot->client, worker->cfg->vemb_v16_batch_request_size) != 0 ||
        vemb_v16_client_topology_refresh(slot->client) != 0) {
        return -1;
    }
    vemb_v16_client_set_retry_budget(
        slot->client, worker->cfg->vemb_v16_topology_retry_limit);

    if (!common_core_pure_read(worker->cfg) &&
        !common_core_key_key_session(worker->cfg))
        return 0;
    if (worker->use_handle_session) {
        if (vemb_v16_client_prepare_active_owner_channels(slot->client) != 0)
            return -1;
        vemb_v16_client_handle_session_options_t options = {
            .max_batch_delay_us = worker->cfg->vemb_v16_batch_max_delay_us,
        };
        slot->handle_session = vemb_v16_client_handle_session_create(
            slot->client, &options);
        return slot->handle_session ? 0 : -1;
    }

    vemb_v16_client_vector_session_options_t options = {
        .cache_mode = worker->cfg->vemb_v16_l1_entries ?
            VEMB_V16_CLIENT_VECTOR_CACHE_IMMUTABLE_SNAPSHOT :
            VEMB_V16_CLIENT_VECTOR_CACHE_DISABLED,
        .cache_entries = worker->cfg->vemb_v16_l1_entries,
    };
    slot->vector_session = vemb_v16_client_vector_session_create_with_options(
        slot->client, &options);
    return slot->vector_session ? 0 : -1;
}

static bool common_core_should_stop(const common_core_worker *worker,
                                    uint64_t deadline_ns)
{
    return worker->stop->load(std::memory_order_acquire) ||
        (worker->budget != 0 && worker->issued >= worker->budget) ||
        (worker->budget == 0 && getMonotonicNs() >= deadline_ns);
}

static size_t common_core_pending_count(const common_core_worker *worker)
{
    size_t count = 0;
    for (std::vector<common_core_slot>::const_iterator it =
             worker->slots.begin(); it != worker->slots.end(); ++it) {
        count += it->pending.size();
    }
    return count;
}

static void common_core_merge_owner_stats(
    vemb_v16_client_owner_stats_t *dst,
    const vemb_v16_client_owner_stats_t *src) {
    dst->submitted += src->submitted;
    dst->completed += src->completed;
    dst->ok += src->ok;
    dst->not_found += src->not_found;
    dst->errors += src->errors;
    dst->retries += src->retries;
    dst->v1_requests += src->v1_requests;
    dst->v2_items += src->v2_items;
    dst->v2_frames += src->v2_frames;
    dst->fallback_v1 += src->fallback_v1;
    dst->channel_reopen += src->channel_reopen;
    dst->handle_region_count += src->handle_region_count;
    dst->materialize_ok += src->materialize_ok;
    dst->materialize_fail += src->materialize_fail;
    dst->materialize_bytes += src->materialize_bytes;
    dst->pending_current += src->pending_current;
    dst->active_groups_current += src->active_groups_current;
    if (dst->pending_peak < src->pending_peak)
        dst->pending_peak = src->pending_peak;
    dst->poll_calls += src->poll_calls;
    dst->poll_empty += src->poll_empty;
    dst->poll_callbacks += src->poll_callbacks;
    dst->poll_latency_sample_count += src->poll_latency_sample_count;
    dst->poll_latency_ns_sum += src->poll_latency_ns_sum;
    if (dst->poll_latency_ns_min == 0 ||
        (src->poll_latency_ns_min != 0 &&
         src->poll_latency_ns_min < dst->poll_latency_ns_min))
        dst->poll_latency_ns_min = src->poll_latency_ns_min;
    if (src->poll_latency_ns_max > dst->poll_latency_ns_max)
        dst->poll_latency_ns_max = src->poll_latency_ns_max;
    dst->poll_flush_sample_count += src->poll_flush_sample_count;
    dst->poll_flush_ns_sum += src->poll_flush_ns_sum;
    dst->poll_v2_sample_count += src->poll_v2_sample_count;
    dst->poll_v2_ns_sum += src->poll_v2_ns_sum;
    dst->poll_v1_sample_count += src->poll_v1_sample_count;
    dst->poll_v1_ns_sum += src->poll_v1_ns_sum;
    dst->callback_sample_index += src->callback_sample_index;
    dst->callback_sample_count += src->callback_sample_count;
    dst->callback_ns_sum += src->callback_ns_sum;
    if (dst->callback_ns_min == 0 ||
        (src->callback_ns_min != 0 &&
         src->callback_ns_min < dst->callback_ns_min))
        dst->callback_ns_min = src->callback_ns_min;
    if (src->callback_ns_max > dst->callback_ns_max)
        dst->callback_ns_max = src->callback_ns_max;
    dst->submit_to_callback_ns_sum += src->submit_to_callback_ns_sum;
    if (dst->submit_to_callback_ns_min == 0 ||
        (src->submit_to_callback_ns_min != 0 &&
         src->submit_to_callback_ns_min < dst->submit_to_callback_ns_min))
        dst->submit_to_callback_ns_min = src->submit_to_callback_ns_min;
    if (src->submit_to_callback_ns_max > dst->submit_to_callback_ns_max)
        dst->submit_to_callback_ns_max = src->submit_to_callback_ns_max;
    dst->response_to_finish_sample_count +=
        src->response_to_finish_sample_count;
    dst->response_to_finish_ns_sum += src->response_to_finish_ns_sum;
    if (dst->response_to_finish_ns_min == 0 ||
        (src->response_to_finish_ns_min != 0 &&
         src->response_to_finish_ns_min < dst->response_to_finish_ns_min))
        dst->response_to_finish_ns_min = src->response_to_finish_ns_min;
    if (src->response_to_finish_ns_max > dst->response_to_finish_ns_max)
        dst->response_to_finish_ns_max = src->response_to_finish_ns_max;
    dst->finish_sample_count += src->finish_sample_count;
    dst->finish_ns_sum += src->finish_ns_sum;
    if (dst->finish_ns_min == 0 ||
        (src->finish_ns_min != 0 && src->finish_ns_min < dst->finish_ns_min))
        dst->finish_ns_min = src->finish_ns_min;
    if (src->finish_ns_max > dst->finish_ns_max)
        dst->finish_ns_max = src->finish_ns_max;
    dst->response_to_callback_sample_count +=
        src->response_to_callback_sample_count;
    dst->response_to_callback_ns_sum += src->response_to_callback_ns_sum;
    if (dst->response_to_callback_ns_min == 0 ||
        (src->response_to_callback_ns_min != 0 &&
         src->response_to_callback_ns_min <
             dst->response_to_callback_ns_min))
        dst->response_to_callback_ns_min =
            src->response_to_callback_ns_min;
    if (src->response_to_callback_ns_max >
        dst->response_to_callback_ns_max)
        dst->response_to_callback_ns_max =
            src->response_to_callback_ns_max;
    dst->submit_to_response_poll_sample_count +=
        src->submit_to_response_poll_sample_count;
    dst->submit_to_response_poll_ns_sum +=
        src->submit_to_response_poll_ns_sum;
    if (dst->submit_to_response_poll_ns_min == 0 ||
        (src->submit_to_response_poll_ns_min != 0 &&
         src->submit_to_response_poll_ns_min <
             dst->submit_to_response_poll_ns_min))
        dst->submit_to_response_poll_ns_min =
            src->submit_to_response_poll_ns_min;
    if (src->submit_to_response_poll_ns_max >
        dst->submit_to_response_poll_ns_max)
        dst->submit_to_response_poll_ns_max =
            src->submit_to_response_poll_ns_max;
    dst->submit_to_publish_sample_index +=
        src->submit_to_publish_sample_index;
    dst->submit_to_publish_sample_count +=
        src->submit_to_publish_sample_count;
    dst->submit_to_publish_ns_sum += src->submit_to_publish_ns_sum;
    if (dst->submit_to_publish_ns_min == 0 ||
        (src->submit_to_publish_ns_min != 0 &&
         src->submit_to_publish_ns_min < dst->submit_to_publish_ns_min))
        dst->submit_to_publish_ns_min = src->submit_to_publish_ns_min;
    if (src->submit_to_publish_ns_max > dst->submit_to_publish_ns_max)
        dst->submit_to_publish_ns_max = src->submit_to_publish_ns_max;
    dst->v2_batch_timing_sample_count += src->v2_batch_timing_sample_count;
    dst->v2_publish_to_response_poll_ns_sum +=
        src->v2_publish_to_response_poll_ns_sum;
    dst->v2_response_wait_ns_sum += src->v2_response_wait_ns_sum;
    if (src->callback_count_per_poll_max > dst->callback_count_per_poll_max)
        dst->callback_count_per_poll_max = src->callback_count_per_poll_max;
    dst->v2_flush_calls += src->v2_flush_calls;
    dst->v2_flush_full += src->v2_flush_full;
    dst->v2_flush_deadline += src->v2_flush_deadline;
    dst->v2_flush_eager += src->v2_flush_eager;
    dst->v2_flush_l0_backpressure += src->v2_flush_l0_backpressure;
    dst->v2_prepare_sample_count += src->v2_prepare_sample_count;
    dst->v2_prepare_ns_sum += src->v2_prepare_ns_sum;
    if (dst->v2_prepare_ns_min == 0 ||
        (src->v2_prepare_ns_min != 0 &&
         src->v2_prepare_ns_min < dst->v2_prepare_ns_min))
        dst->v2_prepare_ns_min = src->v2_prepare_ns_min;
    if (src->v2_prepare_ns_max > dst->v2_prepare_ns_max)
        dst->v2_prepare_ns_max = src->v2_prepare_ns_max;
    dst->v2_publish_sample_count += src->v2_publish_sample_count;
    dst->v2_publish_ns_sum += src->v2_publish_ns_sum;
    if (dst->v2_publish_ns_min == 0 ||
        (src->v2_publish_ns_min != 0 &&
         src->v2_publish_ns_min < dst->v2_publish_ns_min))
        dst->v2_publish_ns_min = src->v2_publish_ns_min;
    if (src->v2_publish_ns_max > dst->v2_publish_ns_max)
        dst->v2_publish_ns_max = src->v2_publish_ns_max;
    dst->v2_prepare_to_publish_ns_sum +=
        src->v2_prepare_to_publish_ns_sum;
    dst->submit_to_l0_sample_count += src->submit_to_l0_sample_count;
    dst->submit_to_l0_ns_sum += src->submit_to_l0_ns_sum;
    dst->submit_to_route_start_sample_count +=
        src->submit_to_route_start_sample_count;
    dst->submit_to_route_start_ns_sum += src->submit_to_route_start_ns_sum;
    dst->submit_to_drive_start_sample_count +=
        src->submit_to_drive_start_sample_count;
    dst->submit_to_drive_start_ns_sum += src->submit_to_drive_start_ns_sum;
    dst->drive_start_to_slot_poll_sample_count +=
        src->drive_start_to_slot_poll_sample_count;
    dst->drive_start_to_slot_poll_ns_sum +=
        src->drive_start_to_slot_poll_ns_sum;
    dst->slot_poll_to_route_start_sample_count +=
        src->slot_poll_to_route_start_sample_count;
    dst->slot_poll_to_route_start_ns_sum +=
        src->slot_poll_to_route_start_ns_sum;
    dst->route_duration_sample_count += src->route_duration_sample_count;
    dst->route_duration_ns_sum += src->route_duration_ns_sum;
    dst->route_to_l0_start_sample_count +=
        src->route_to_l0_start_sample_count;
    dst->route_to_l0_start_ns_sum += src->route_to_l0_start_ns_sum;
    dst->l0_submit_duration_sample_count +=
        src->l0_submit_duration_sample_count;
    dst->l0_submit_duration_ns_sum += src->l0_submit_duration_ns_sum;
    dst->route_to_channel_start_sample_count +=
        src->route_to_channel_start_sample_count;
    dst->route_to_channel_start_ns_sum += src->route_to_channel_start_ns_sum;
    dst->channel_duration_sample_count += src->channel_duration_sample_count;
    dst->channel_duration_ns_sum += src->channel_duration_ns_sum;
    dst->channel_to_path_start_sample_count +=
        src->channel_to_path_start_sample_count;
    dst->channel_to_path_start_ns_sum += src->channel_to_path_start_ns_sum;
    dst->owner_path_duration_sample_count +=
        src->owner_path_duration_sample_count;
    dst->owner_path_duration_ns_sum += src->owner_path_duration_ns_sum;
    dst->owner_path_to_l0_start_sample_count +=
        src->owner_path_to_l0_start_sample_count;
    dst->owner_path_to_l0_start_ns_sum +=
        src->owner_path_to_l0_start_ns_sum;
    dst->v2_enable_duration_sample_count +=
        src->v2_enable_duration_sample_count;
    dst->v2_enable_duration_ns_sum += src->v2_enable_duration_ns_sum;
    dst->v2_enable_ready_fast_count += src->v2_enable_ready_fast_count;
    dst->v2_enable_attach_count += src->v2_enable_attach_count;
    dst->v2_enable_reopen_count += src->v2_enable_reopen_count;
    dst->l0_to_deadline_due_sample_count +=
        src->l0_to_deadline_due_sample_count;
    dst->l0_to_deadline_due_ns_sum += src->l0_to_deadline_due_ns_sum;
    dst->l0_to_deadline_set_sample_count +=
        src->l0_to_deadline_set_sample_count;
    dst->l0_to_deadline_set_ns_sum += src->l0_to_deadline_set_ns_sum;
    dst->deadline_set_to_l0_sample_count +=
        src->deadline_set_to_l0_sample_count;
    dst->deadline_set_to_l0_ns_sum += src->deadline_set_to_l0_ns_sum;
    dst->deadline_set_to_due_sample_count +=
        src->deadline_set_to_due_sample_count;
    dst->deadline_set_to_due_ns_sum += src->deadline_set_to_due_ns_sum;
    dst->deadline_target_to_due_sample_count +=
        src->deadline_target_to_due_sample_count;
    dst->deadline_target_to_due_ns_sum += src->deadline_target_to_due_ns_sum;
    dst->deadline_poll_to_check_sample_count +=
        src->deadline_poll_to_check_sample_count;
    dst->deadline_poll_to_check_ns_sum += src->deadline_poll_to_check_ns_sum;
    dst->deadline_target_to_check_sample_count +=
        src->deadline_target_to_check_sample_count;
    dst->deadline_target_to_check_ns_sum +=
        src->deadline_target_to_check_ns_sum;
    dst->l0_enqueue_to_deadline_target_sample_count +=
        src->l0_enqueue_to_deadline_target_sample_count;
    dst->l0_enqueue_to_deadline_target_ns_sum +=
        src->l0_enqueue_to_deadline_target_ns_sum;
    dst->deadline_target_to_poll_check_sample_count +=
        src->deadline_target_to_poll_check_sample_count;
    dst->deadline_target_to_poll_check_ns_sum +=
        src->deadline_target_to_poll_check_ns_sum;
    dst->deadline_check_duration_sample_count +=
        src->deadline_check_duration_sample_count;
    dst->deadline_check_duration_ns_sum +=
        src->deadline_check_duration_ns_sum;
    dst->deadline_due_to_flush_sample_count +=
        src->deadline_due_to_flush_sample_count;
    dst->deadline_due_to_flush_ns_sum +=
        src->deadline_due_to_flush_ns_sum;
    dst->flush_to_publish_sample_count += src->flush_to_publish_sample_count;
    dst->flush_to_publish_ns_sum += src->flush_to_publish_ns_sum;
    dst->v2_deadline_set_count += src->v2_deadline_set_count;
    dst->v2_deadline_due_count += src->v2_deadline_due_count;
    dst->v2_deadline_age_ns_sum += src->v2_deadline_age_ns_sum;
    dst->v2_deadline_due_to_flush_ns_sum +=
        src->v2_deadline_due_to_flush_ns_sum;
    if (dst->v2_deadline_due_to_flush_ns_min == 0 ||
        (src->v2_deadline_due_to_flush_ns_min != 0 &&
         src->v2_deadline_due_to_flush_ns_min <
             dst->v2_deadline_due_to_flush_ns_min))
        dst->v2_deadline_due_to_flush_ns_min =
            src->v2_deadline_due_to_flush_ns_min;
    if (src->v2_deadline_due_to_flush_ns_max >
        dst->v2_deadline_due_to_flush_ns_max)
        dst->v2_deadline_due_to_flush_ns_max =
            src->v2_deadline_due_to_flush_ns_max;
    dst->v2_publish_ring_full += src->v2_publish_ring_full;
    dst->v2_publish_errors += src->v2_publish_errors;
}

static void common_core_merge_region_stats(
    vemb_v16_client_owner_region_stats_t *dst,
    const vemb_v16_client_owner_region_stats_t *src) {
    dst->handle_count += src->handle_count;
    dst->copy_count += src->copy_count;
    dst->copy_bytes += src->copy_bytes;
    dst->copy_sample_count += src->copy_sample_count;
    dst->copy_latency_ns_sum += src->copy_latency_ns_sum;
    if (dst->copy_latency_ns_min == 0 ||
        (src->copy_latency_ns_min != 0 &&
         src->copy_latency_ns_min < dst->copy_latency_ns_min))
        dst->copy_latency_ns_min = src->copy_latency_ns_min;
    if (src->copy_latency_ns_max > dst->copy_latency_ns_max)
        dst->copy_latency_ns_max = src->copy_latency_ns_max;
    for (uint32_t bucket = 0;
         bucket < VEMB_V16_CLIENT_COPY_LATENCY_BUCKETS; bucket++)
        dst->copy_latency_ns_buckets[bucket] +=
            src->copy_latency_ns_buckets[bucket];
}

static void common_core_emit_cli_stats(common_core_worker *worker,
                                       uint64_t phase_sec) {
    std::vector<vemb_v16_client_owner_stats_t> owners(
        VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS);
    std::vector<vemb_v16_client_owner_region_stats_t> regions(
        VEMB_V16_CLIENT_OWNER_REGION_STATS_MAX);
    uint32_t region_count = 0;

    for (std::vector<common_core_slot>::iterator it = worker->slots.begin();
         it != worker->slots.end(); ++it) {
        if (!it->client)
            continue;
        std::unique_ptr<vemb_v16_client_owner_stats_snapshot_t> snapshot(
            new vemb_v16_client_owner_stats_snapshot_t());
        vemb_v16_client_get_owner_stats(it->client, snapshot.get());
        for (uint32_t owner = 0;
             owner < snapshot->owner_count &&
             owner < VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS; owner++)
            common_core_merge_owner_stats(&owners[owner],
                                          &snapshot->owners[owner]);
        uint32_t snapshot_region_count = std::min<uint32_t>(
            snapshot->owner_region_count,
            VEMB_V16_CLIENT_OWNER_REGION_STATS_MAX);
        for (uint32_t i = 0; i < snapshot_region_count; i++) {
            const vemb_v16_client_owner_region_stats_t *src =
                &snapshot->owner_regions[i];
            uint32_t slot = 0;
            while (slot < region_count &&
                   (regions[slot].owner_id != src->owner_id ||
                    regions[slot].region_id != src->region_id))
                slot++;
            if (slot == region_count) {
                if (region_count == VEMB_V16_CLIENT_OWNER_REGION_STATS_MAX)
                    continue;
                regions[slot] = *src;
                region_count++;
            } else {
                common_core_merge_region_stats(&regions[slot], src);
            }
        }
    }

    for (uint32_t owner = 0;
         owner < VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS; owner++) {
        const vemb_v16_client_owner_stats_t *stats = &owners[owner];
        if (stats->submitted == 0 && stats->completed == 0 &&
            stats->v2_items == 0 && stats->poll_calls == 0)
            continue;
        unsigned long long avg_batch = stats->v2_frames == 0 ? 0ull :
            (unsigned long long)(stats->v2_items / stats->v2_frames);
        unsigned long long poll_avg_ns =
            stats->poll_latency_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->poll_latency_ns_sum /
                                 stats->poll_latency_sample_count);
        unsigned long long flush_avg_ns =
            stats->poll_flush_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->poll_flush_ns_sum /
                                 stats->poll_flush_sample_count);
        unsigned long long v2_poll_avg_ns =
            stats->poll_v2_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->poll_v2_ns_sum /
                                 stats->poll_v2_sample_count);
        unsigned long long v1_poll_avg_ns =
            stats->poll_v1_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->poll_v1_ns_sum /
                                 stats->poll_v1_sample_count);
        unsigned long long callback_avg_ns =
            stats->callback_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->callback_ns_sum /
                                 stats->callback_sample_count);
        unsigned long long submit_to_callback_avg_ns =
            stats->callback_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->submit_to_callback_ns_sum /
                                 stats->callback_sample_count);
        unsigned long long response_to_finish_avg_ns =
            stats->response_to_finish_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->response_to_finish_ns_sum /
                                 stats->response_to_finish_sample_count);
        unsigned long long finish_avg_ns =
            stats->finish_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->finish_ns_sum /
                                 stats->finish_sample_count);
        unsigned long long response_to_callback_avg_ns =
            stats->response_to_callback_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->response_to_callback_ns_sum /
                                 stats->response_to_callback_sample_count);
        unsigned long long submit_to_response_poll_avg_ns =
            stats->submit_to_response_poll_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->submit_to_response_poll_ns_sum /
                                 stats->submit_to_response_poll_sample_count);
        unsigned long long submit_to_publish_avg_ns =
            stats->submit_to_publish_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->submit_to_publish_ns_sum /
                                 stats->submit_to_publish_sample_count);
        unsigned long long submit_to_l0_avg_ns =
            stats->submit_to_l0_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->submit_to_l0_ns_sum /
                                 stats->submit_to_l0_sample_count);
        unsigned long long submit_to_route_start_avg_ns =
            stats->submit_to_route_start_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->submit_to_route_start_ns_sum /
                                 stats->submit_to_route_start_sample_count);
        unsigned long long submit_to_drive_start_avg_ns =
            stats->submit_to_drive_start_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->submit_to_drive_start_ns_sum /
                                 stats->submit_to_drive_start_sample_count);
        unsigned long long drive_start_to_slot_poll_avg_ns =
            stats->drive_start_to_slot_poll_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->drive_start_to_slot_poll_ns_sum /
                                 stats->drive_start_to_slot_poll_sample_count);
        unsigned long long slot_poll_to_route_start_avg_ns =
            stats->slot_poll_to_route_start_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->slot_poll_to_route_start_ns_sum /
                                 stats->slot_poll_to_route_start_sample_count);
        unsigned long long route_duration_avg_ns =
            stats->route_duration_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->route_duration_ns_sum /
                                 stats->route_duration_sample_count);
        unsigned long long route_to_l0_start_avg_ns =
            stats->route_to_l0_start_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->route_to_l0_start_ns_sum /
                                 stats->route_to_l0_start_sample_count);
        unsigned long long l0_submit_duration_avg_ns =
            stats->l0_submit_duration_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->l0_submit_duration_ns_sum /
                                 stats->l0_submit_duration_sample_count);
        unsigned long long route_to_channel_start_avg_ns =
            stats->route_to_channel_start_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->route_to_channel_start_ns_sum /
                                 stats->route_to_channel_start_sample_count);
        unsigned long long channel_duration_avg_ns =
            stats->channel_duration_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->channel_duration_ns_sum /
                                 stats->channel_duration_sample_count);
        unsigned long long channel_to_path_start_avg_ns =
            stats->channel_to_path_start_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->channel_to_path_start_ns_sum /
                                 stats->channel_to_path_start_sample_count);
        unsigned long long owner_path_duration_avg_ns =
            stats->owner_path_duration_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->owner_path_duration_ns_sum /
                                 stats->owner_path_duration_sample_count);
        unsigned long long owner_path_to_l0_start_avg_ns =
            stats->owner_path_to_l0_start_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->owner_path_to_l0_start_ns_sum /
                                 stats->owner_path_to_l0_start_sample_count);
        unsigned long long v2_enable_duration_avg_ns =
            stats->v2_enable_duration_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->v2_enable_duration_ns_sum /
                                 stats->v2_enable_duration_sample_count);
        unsigned long long l0_to_due_avg_ns =
            stats->l0_to_deadline_due_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->l0_to_deadline_due_ns_sum /
                                 stats->l0_to_deadline_due_sample_count);
        unsigned long long l0_to_deadline_set_avg_ns =
            stats->l0_to_deadline_set_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->l0_to_deadline_set_ns_sum /
                                 stats->l0_to_deadline_set_sample_count);
        unsigned long long deadline_set_to_l0_avg_ns =
            stats->deadline_set_to_l0_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->deadline_set_to_l0_ns_sum /
                                 stats->deadline_set_to_l0_sample_count);
        unsigned long long deadline_set_to_due_avg_ns =
            stats->deadline_set_to_due_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->deadline_set_to_due_ns_sum /
                                 stats->deadline_set_to_due_sample_count);
        unsigned long long deadline_target_to_due_avg_ns =
            stats->deadline_target_to_due_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->deadline_target_to_due_ns_sum /
                                 stats->deadline_target_to_due_sample_count);
        unsigned long long deadline_poll_to_check_avg_ns =
            stats->deadline_poll_to_check_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->deadline_poll_to_check_ns_sum /
                                 stats->deadline_poll_to_check_sample_count);
        unsigned long long deadline_target_to_check_avg_ns =
            stats->deadline_target_to_check_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->deadline_target_to_check_ns_sum /
                                 stats->deadline_target_to_check_sample_count);
        unsigned long long l0_enqueue_to_deadline_target_avg_ns =
            stats->l0_enqueue_to_deadline_target_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->l0_enqueue_to_deadline_target_ns_sum /
                                 stats->l0_enqueue_to_deadline_target_sample_count);
        unsigned long long deadline_target_to_poll_check_avg_ns =
            stats->deadline_target_to_poll_check_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->deadline_target_to_poll_check_ns_sum /
                                 stats->deadline_target_to_poll_check_sample_count);
        unsigned long long deadline_check_duration_avg_ns =
            stats->deadline_check_duration_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->deadline_check_duration_ns_sum /
                                 stats->deadline_check_duration_sample_count);
        unsigned long long due_to_flush_req_avg_ns =
            stats->deadline_due_to_flush_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->deadline_due_to_flush_ns_sum /
                                 stats->deadline_due_to_flush_sample_count);
        unsigned long long flush_to_publish_avg_ns =
            stats->flush_to_publish_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->flush_to_publish_ns_sum /
                                 stats->flush_to_publish_sample_count);
        unsigned long long prepare_avg_ns =
            stats->v2_prepare_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->v2_prepare_ns_sum /
                                 stats->v2_prepare_sample_count);
        unsigned long long publish_avg_ns =
            stats->v2_publish_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->v2_publish_ns_sum /
                                 stats->v2_publish_sample_count);
        unsigned long long prepare_to_publish_avg_ns =
            stats->v2_prepare_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->v2_prepare_to_publish_ns_sum /
                                 stats->v2_prepare_sample_count);
        unsigned long long deadline_age_avg_ns =
            stats->v2_deadline_due_count == 0 ? 0ull :
            (unsigned long long)(stats->v2_deadline_age_ns_sum /
                                 stats->v2_deadline_due_count);
        unsigned long long due_to_flush_avg_ns =
            stats->v2_deadline_due_count == 0 ? 0ull :
            (unsigned long long)(stats->v2_deadline_due_to_flush_ns_sum /
                                 stats->v2_deadline_due_count);
        unsigned long long v2_publish_to_response_poll_avg_ns =
            stats->v2_batch_timing_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->v2_publish_to_response_poll_ns_sum /
                                 stats->v2_batch_timing_sample_count);
        unsigned long long v2_response_wait_avg_ns =
            stats->v2_batch_timing_sample_count == 0 ? 0ull :
            (unsigned long long)(stats->v2_response_wait_ns_sum /
                                 stats->v2_batch_timing_sample_count);
        fprintf(stderr,
                "[cli-stat] worker=%u sec=%llu owner=%u "
                "submitted=%llu completed=%llu v2_items=%llu v2_frames=%llu "
                "avg_batch=%llu pending=%llu pending_peak=%llu "
                "active_groups=%llu "
                "poll_calls=%llu poll_empty=%llu poll_callbacks=%llu "
                "poll_avg_ns=%llu poll_min_ns=%llu poll_max_ns=%llu "
                "flush_avg_ns=%llu v2_poll_avg_ns=%llu v1_poll_avg_ns=%llu "
                "callback_samples=%llu callback_avg_ns=%llu "
                "callback_min_ns=%llu callback_max_ns=%llu "
                "submit_to_callback_avg_ns=%llu "
                "submit_to_callback_min_ns=%llu "
                "submit_to_callback_max_ns=%llu "
                "response_to_finish_avg_ns=%llu "
                "finish_avg_ns=%llu finish_min_ns=%llu finish_max_ns=%llu "
                "response_to_callback_avg_ns=%llu "
                "submit_to_response_poll_avg_ns=%llu "
                "submit_to_publish_avg_ns=%llu "
                "submit_to_l0_avg_ns=%llu l0_to_due_avg_ns=%llu "
                "submit_to_route_start_avg_ns=%llu "
                "submit_to_drive_start_avg_ns=%llu "
                "drive_start_to_slot_poll_avg_ns=%llu "
                "slot_poll_to_route_start_avg_ns=%llu "
                "route_duration_avg_ns=%llu "
                "route_to_l0_start_avg_ns=%llu "
                "l0_submit_duration_avg_ns=%llu "
                "route_to_channel_start_avg_ns=%llu "
                "channel_duration_avg_ns=%llu "
                "channel_to_path_start_avg_ns=%llu "
                "owner_path_duration_avg_ns=%llu "
                "owner_path_to_l0_start_avg_ns=%llu "
                "v2_enable_duration_avg_ns=%llu "
                "l0_to_deadline_set_avg_ns=%llu "
                "deadline_set_to_l0_avg_ns=%llu "
                "deadline_set_to_due_avg_ns=%llu "
                "deadline_target_to_due_avg_ns=%llu "
                "deadline_poll_to_check_avg_ns=%llu "
                "deadline_target_to_check_avg_ns=%llu "
                "l0_enqueue_to_deadline_target_avg_ns=%llu "
                "deadline_target_to_poll_check_avg_ns=%llu "
                "deadline_check_duration_avg_ns=%llu "
                "due_to_flush_req_avg_ns=%llu flush_to_publish_avg_ns=%llu "
                "prepare_avg_ns=%llu prepare_min_ns=%llu prepare_max_ns=%llu "
                "publish_avg_ns=%llu publish_min_ns=%llu publish_max_ns=%llu "
                "prepare_to_publish_avg_ns=%llu "
                "deadline_set=%llu deadline_due=%llu "
                "deadline_age_avg_ns=%llu due_to_flush_avg_ns=%llu "
                "due_to_flush_min_ns=%llu due_to_flush_max_ns=%llu "
                "v2_publish_to_response_poll_avg_ns=%llu "
                "v2_response_wait_avg_ns=%llu "
                "callbacks_per_poll_max=%llu "
                "flush_calls=%llu flush_full=%llu flush_deadline=%llu "
                "flush_eager=%llu "
                "flush_l0_bp=%llu ring_full=%llu publish_err=%llu\n",
                worker->worker_id, (unsigned long long)phase_sec, owner,
                (unsigned long long)stats->submitted,
                (unsigned long long)stats->completed,
                (unsigned long long)stats->v2_items,
                (unsigned long long)stats->v2_frames, avg_batch,
                (unsigned long long)stats->pending_current,
                (unsigned long long)stats->pending_peak,
                (unsigned long long)stats->active_groups_current,
                (unsigned long long)stats->poll_calls,
                (unsigned long long)stats->poll_empty,
                (unsigned long long)stats->poll_callbacks, poll_avg_ns,
                (unsigned long long)stats->poll_latency_ns_min,
                (unsigned long long)stats->poll_latency_ns_max,
                flush_avg_ns, v2_poll_avg_ns, v1_poll_avg_ns,
                (unsigned long long)stats->callback_sample_count,
                callback_avg_ns,
                (unsigned long long)stats->callback_ns_min,
                (unsigned long long)stats->callback_ns_max,
                submit_to_callback_avg_ns,
                (unsigned long long)stats->submit_to_callback_ns_min,
                (unsigned long long)stats->submit_to_callback_ns_max,
                response_to_finish_avg_ns, finish_avg_ns,
                (unsigned long long)stats->finish_ns_min,
                (unsigned long long)stats->finish_ns_max,
                response_to_callback_avg_ns,
                submit_to_response_poll_avg_ns,
                submit_to_publish_avg_ns,
                submit_to_l0_avg_ns,
                l0_to_due_avg_ns,
                submit_to_route_start_avg_ns,
                submit_to_drive_start_avg_ns,
                drive_start_to_slot_poll_avg_ns,
                slot_poll_to_route_start_avg_ns,
                route_duration_avg_ns,
                route_to_l0_start_avg_ns,
                l0_submit_duration_avg_ns,
                route_to_channel_start_avg_ns,
                channel_duration_avg_ns,
                channel_to_path_start_avg_ns,
                owner_path_duration_avg_ns,
                owner_path_to_l0_start_avg_ns,
                v2_enable_duration_avg_ns,
                l0_to_deadline_set_avg_ns,
                deadline_set_to_l0_avg_ns,
                deadline_set_to_due_avg_ns,
                deadline_target_to_due_avg_ns,
                deadline_poll_to_check_avg_ns,
                deadline_target_to_check_avg_ns,
                l0_enqueue_to_deadline_target_avg_ns,
                deadline_target_to_poll_check_avg_ns,
                deadline_check_duration_avg_ns,
                due_to_flush_req_avg_ns,
                flush_to_publish_avg_ns,
                prepare_avg_ns,
                (unsigned long long)stats->v2_prepare_ns_min,
                (unsigned long long)stats->v2_prepare_ns_max,
                publish_avg_ns,
                (unsigned long long)stats->v2_publish_ns_min,
                (unsigned long long)stats->v2_publish_ns_max,
                prepare_to_publish_avg_ns,
                (unsigned long long)stats->v2_deadline_set_count,
                (unsigned long long)stats->v2_deadline_due_count,
                deadline_age_avg_ns,
                due_to_flush_avg_ns,
                (unsigned long long)stats->v2_deadline_due_to_flush_ns_min,
                (unsigned long long)stats->v2_deadline_due_to_flush_ns_max,
                v2_publish_to_response_poll_avg_ns,
                v2_response_wait_avg_ns,
                (unsigned long long)stats->callback_count_per_poll_max,
                (unsigned long long)stats->v2_flush_calls,
                (unsigned long long)stats->v2_flush_full,
                (unsigned long long)stats->v2_flush_deadline,
                (unsigned long long)stats->v2_flush_eager,
                (unsigned long long)stats->v2_flush_l0_backpressure,
                (unsigned long long)stats->v2_publish_ring_full,
                (unsigned long long)stats->v2_publish_errors);
    }
    fprintf(stderr,
            "[cli-stat-completion] worker=%u sec=%llu samples=%llu "
            "callback_avg_ns=%llu callback_min_ns=%llu "
            "callback_max_ns=%llu\n",
            worker->worker_id, (unsigned long long)phase_sec,
            (unsigned long long)worker->completion_sample_count,
            worker->completion_sample_count == 0 ? 0ull :
                (unsigned long long)(worker->completion_latency_ns_sum /
                                     worker->completion_sample_count),
            (unsigned long long)worker->completion_latency_ns_min,
            (unsigned long long)worker->completion_latency_ns_max);
    for (uint32_t i = 0; i < region_count; i++) {
        const vemb_v16_client_owner_region_stats_t *region = &regions[i];
        if (region->copy_count == 0 && region->handle_count == 0)
            continue;
        fprintf(stderr,
                "[cli-stat-region] worker=%u sec=%llu owner=%u region=%u "
                "handles=%llu copies=%llu copy_bytes=%llu samples=%llu "
                "copy_ns_sum=%llu "
                "copy_min_ns=%llu copy_max_ns=%llu\n",
                worker->worker_id, (unsigned long long)phase_sec,
                region->owner_id, region->region_id,
                (unsigned long long)region->handle_count,
                (unsigned long long)region->copy_count,
                (unsigned long long)region->copy_bytes,
                (unsigned long long)region->copy_sample_count,
                (unsigned long long)region->copy_latency_ns_sum,
                (unsigned long long)region->copy_latency_ns_min,
                (unsigned long long)region->copy_latency_ns_max);
    }
    for (size_t slot_index = 0; slot_index < worker->slots.size();
         slot_index++) {
        const common_core_slot *slot = &worker->slots[slot_index];
        if (slot->drive_count == 0)
            continue;
        unsigned long long queue_avg_ns =
            (unsigned long long)(slot->drive_queue_ns_sum /
                                 slot->drive_count);
        unsigned long long poll_avg_ns =
            (unsigned long long)(slot->poll_ns_sum / slot->drive_count);
        fprintf(stderr,
                "[cli-slot-stat] worker=%u sec=%llu slot=%zu "
                "schedule=%s drives=%llu queue_avg_ns=%llu "
                "queue_min_ns=%llu queue_max_ns=%llu "
                "poll_avg_ns=%llu poll_min_ns=%llu poll_max_ns=%llu "
                "callbacks=%llu empty=%llu\n",
                worker->worker_id, (unsigned long long)phase_sec, slot_index,
                worker->round_robin_slots ? "round_robin" : "fixed",
                (unsigned long long)slot->drive_count, queue_avg_ns,
                (unsigned long long)slot->drive_queue_ns_min,
                (unsigned long long)slot->drive_queue_ns_max, poll_avg_ns,
                (unsigned long long)slot->poll_ns_min,
                (unsigned long long)slot->poll_ns_max,
                (unsigned long long)slot->callback_count,
                (unsigned long long)slot->empty_drive_count);
    }
}

static int common_core_drive_sessions(common_core_worker *worker)
{
    int callbacks = 0;
    uint64_t drive_sessions_start_ns = getMonotonicNs();
    const size_t slot_count = worker->slots.size();
    if (slot_count == 0)
        return 0;
    const bool sample_slot_stats = worker->cli_stats_interval_ns != 0;
    const size_t start_slot = worker->round_robin_slots ?
        worker->next_slot % slot_count : 0;
    for (size_t offset = 0; offset < slot_count; offset++) {
        const size_t slot_index = (start_slot + offset) % slot_count;
        common_core_slot *slot = &worker->slots[slot_index];
        const uint64_t slot_poll_start_ns = sample_slot_stats ?
            getMonotonicNs() : 0;
        int rc;
        if (worker->use_handle_session) {
            /* poll() owns handle-session routing and deadline/full flushes.
             * An explicit flush here would defeat max_batch_delay_us by
             * eagerly publishing the small set released by the last poll. */
            rc = vemb_v16_client_handle_session_poll_at(
                slot->handle_session, common_core_handle_completion, slot,
                drive_sessions_start_ns);
        } else {
            if (vemb_v16_client_vector_session_flush(slot->vector_session) != 0)
                return -1;
            rc = vemb_v16_client_vector_session_poll(
                slot->vector_session, common_core_vector_completion, slot);
        }
        if (sample_slot_stats) {
            const uint64_t slot_poll_end_ns = getMonotonicNs();
            const uint64_t queue_ns = slot_poll_start_ns -
                drive_sessions_start_ns;
            const uint64_t poll_ns = slot_poll_end_ns - slot_poll_start_ns;
            slot->drive_count++;
            slot->drive_queue_ns_sum += queue_ns;
            if (slot->drive_queue_ns_min == 0 ||
                queue_ns < slot->drive_queue_ns_min)
                slot->drive_queue_ns_min = queue_ns;
            if (queue_ns > slot->drive_queue_ns_max)
                slot->drive_queue_ns_max = queue_ns;
            slot->poll_ns_sum += poll_ns;
            if (slot->poll_ns_min == 0 || poll_ns < slot->poll_ns_min)
                slot->poll_ns_min = poll_ns;
            if (poll_ns > slot->poll_ns_max)
                slot->poll_ns_max = poll_ns;
            slot->callback_count += rc > 0 ? (uint64_t)rc : 0;
            if (rc == 0)
                slot->empty_drive_count++;
        }
        if (rc < 0)
            return -1;
        callbacks += rc;
    }
    if (worker->round_robin_slots)
        worker->next_slot = (start_slot + 1) % slot_count;
    return callbacks;
}

static void common_core_run_async_reads(common_core_worker *worker,
                                        uint64_t deadline_ns)
{
    const uint32_t pipeline = std::max(1u, worker->cfg->pipeline);
    uint64_t next_cookie = 1;
    uint64_t next_cli_stats_ns = worker->cli_stats_interval_ns == 0 ? 0 :
        getMonotonicNs() + worker->cli_stats_interval_ns;
    uint64_t cli_stats_sec = 1;

    while (!common_core_should_stop(worker, deadline_ns)) {
        int submitted = 0;
        for (std::vector<common_core_slot>::iterator it = worker->slots.begin();
             it != worker->slots.end() &&
             !common_core_should_stop(worker, deadline_ns); ++it) {
            while (it->pending.size() < pipeline &&
                   !common_core_should_stop(worker, deadline_ns)) {
                std::string key = common_core_next_key(worker, false);
                common_core_pending pending;
                gettimeofday(&pending.sent_time, NULL);
                pending.bytes_tx = (uint32_t)key.size() + 24u;
                uint64_t cookie = next_cookie++;
                int rc;
                if (worker->cfg->vemb_v16_vsim_key_key &&
                    it->handle_session) {
                    char key2_buf[VEMB_V16_MAX_KEY_LEN];
                    common_core_next_key2(worker, key, key2_buf,
                                          sizeof(key2_buf));
                    pending.bytes_tx += strlen(key2_buf);
                    rc = vemb_v16_client_handle_session_submit_vsim_key_key(
                        it->handle_session, NULL, key.c_str(), key2_buf,
                        cookie);
                } else if (worker->use_handle_session) {
                    rc = vemb_v16_client_handle_session_submit(
                        it->handle_session, NULL, key.c_str(), cookie);
                } else {
                    rc = vemb_v16_client_vector_session_submit(
                        it->vector_session, NULL, key.c_str(), cookie);
                }
                worker->issued++;
                if (rc != 0) {
                    common_core_account_read(worker, pending, -1, 0, false);
                    continue;
                }
                it->pending.insert(std::make_pair(cookie, pending));
                submitted++;
            }
        }

        int callbacks = common_core_drive_sessions(worker);
        if (callbacks < 0) {
            worker->setup_failed = true;
            worker->stop->store(true, std::memory_order_release);
            return;
        }
        if (next_cli_stats_ns != 0) {
            uint64_t now_ns = getMonotonicNs();
            if (now_ns >= next_cli_stats_ns) {
                common_core_emit_cli_stats(worker, cli_stats_sec++);
                next_cli_stats_ns = now_ns + worker->cli_stats_interval_ns;
            }
        }
        if (submitted == 0 && callbacks == 0)
            common_core_relax();
    }

    uint64_t drain_deadline = getMonotonicNs() + 1000000000ull;
    while (common_core_pending_count(worker) != 0 &&
           getMonotonicNs() < drain_deadline) {
        int callbacks = common_core_drive_sessions(worker);
        if (callbacks < 0)
            break;
        if (callbacks == 0)
            common_core_relax();
    }
}

static void common_core_run_vadd_batch(common_core_worker *worker,
                                       common_core_slot *slot,
                                       uint32_t count)
{
    std::vector<std::string> keys;
    std::vector<const char *> sets(count, NULL);
    std::vector<const char *> elems;
    std::vector<float> vectors((size_t)count * worker->cfg->vemb_v16_dim);
    std::vector<const float *> vector_ptrs;
    std::vector<common_core_pending> pending(count);
    keys.reserve(count);
    elems.reserve(count);
    vector_ptrs.reserve(count);

    for (uint32_t i = 0; i < count; i++) {
        keys.push_back(common_core_next_key(worker, true));
        gettimeofday(&pending[i].sent_time, NULL);
        pending[i].bytes_tx = (uint32_t)keys.back().size() + 24u +
            worker->cfg->vemb_v16_dim * sizeof(float);
        vemb_v16_fill_vector(
            &vectors[(size_t)i * worker->cfg->vemb_v16_dim],
            worker->cfg->vemb_v16_dim, (uint32_t)(worker->issued + i));
    }
    for (uint32_t i = 0; i < count; i++) {
        elems.push_back(keys[i].c_str());
        vector_ptrs.push_back(&vectors[(size_t)i * worker->cfg->vemb_v16_dim]);
    }

    int rc = vemb_v16_client_vadd_pipeline(
        slot->client, sets.data(), elems.data(), vector_ptrs.data(), count,
        std::min<uint32_t>(count, worker->cfg->pipeline));
    for (uint32_t i = 0; i < count; i++)
        common_core_account_set(worker, pending[i], rc == 0);
    worker->issued += count;
}

static void common_core_run_key_key_batch(common_core_worker *worker,
                                          common_core_slot *slot,
                                          uint32_t count)
{
    std::vector<std::string> keys1;
    std::vector<std::string> keys2;
    std::vector<const char *> sets(count, NULL);
    std::vector<const char *> elems1;
    std::vector<const char *> elems2;
    std::vector<float> scores(count, 0.0f);
    std::vector<common_core_pending> pending(count);
    keys1.reserve(count);
    keys2.reserve(count);
    elems1.reserve(count);
    elems2.reserve(count);

    /* key1 走 memtier key-pattern; key2 同前缀 + [key_min,key_max] 随机
     * (LCG 与 protocol.cpp TCP 路径一致) */
    const benchmark_config *cfg = worker->cfg;
    unsigned long long range = cfg->key_maximum - cfg->key_minimum + 1;
    for (uint32_t i = 0; i < count; i++) {
        keys1.push_back(common_core_next_key(worker, false));
        const std::string &k1 = keys1.back();
        size_t prefix_len = k1.size();
        while (prefix_len > 0 && k1[prefix_len - 1] >= '0' &&
               k1[prefix_len - 1] <= '9')
            prefix_len--;
        worker->key2_rng = worker->key2_rng * 6364136223846793005ULL +
                           1442695040888963407ULL;
        char key2_buf[VEMB_V16_MAX_KEY_LEN];
        snprintf(key2_buf, sizeof(key2_buf), "%.*s%llu",
                 (int)prefix_len, k1.c_str(),
                 (unsigned long long)(cfg->key_minimum +
                                      (worker->key2_rng % range)));
        keys2.push_back(key2_buf);
        gettimeofday(&pending[i].sent_time, NULL);
        pending[i].bytes_tx = (uint32_t)(k1.size() + keys2.back().size()) + 24u;
    }
    for (uint32_t i = 0; i < count; i++) {
        elems1.push_back(keys1[i].c_str());
        elems2.push_back(keys2[i].c_str());
    }

    int rc = vemb_v16_client_vsim_key_key_pipeline(
        slot->client, sets.data(), elems1.data(), elems2.data(),
        count, scores.data(),
        std::min<uint32_t>(count, worker->cfg->pipeline));
    for (uint32_t i = 0; i < count; i++)
        common_core_account_read(worker, pending[i], rc, 0, rc == 0);
    worker->issued += count;
}

static void common_core_run_sync_op(common_core_worker *worker,
                                    common_core_slot *slot, uint8_t op)
{
    if (op == VEMB_V16_OP_VADD) {
        uint32_t count = COMMON_CORE_SYNC_BATCH_MAX;
        if (worker->budget != 0)
            count = (uint32_t)std::min<unsigned long long>(
                count, worker->budget - worker->issued);
        common_core_run_vadd_batch(worker, slot, count);
        return;
    }

    if (op == VEMB_V16_OP_VSIM_KEY_KEY) {
        uint32_t count = COMMON_CORE_SYNC_BATCH_MAX;
        if (worker->budget != 0)
            count = (uint32_t)std::min<unsigned long long>(
                count, worker->budget - worker->issued);
        common_core_run_key_key_batch(worker, slot, count);
        return;
    }

    bool is_set = op == VEMB_V16_OP_VREM;
    std::string key = common_core_next_key(worker, is_set);
    common_core_pending pending;
    gettimeofday(&pending.sent_time, NULL);
    pending.bytes_tx = (uint32_t)key.size() + 24u;
    int rc;

    if (op == VEMB_V16_OP_VREM) {
        rc = vemb_v16_client_vrem(slot->client, NULL, key.c_str());
        common_core_account_set(worker, pending, rc == 0);
    } else if (op == VEMB_V16_OP_VSIM_INLINE) {
        std::vector<float> query(worker->cfg->vemb_v16_dim);
        float score = 0.0f;
        vemb_v16_fill_vector(query.data(), worker->cfg->vemb_v16_dim,
                                (uint32_t)worker->issued);
        rc = vemb_v16_client_vsim(slot->client, NULL, key.c_str(),
                                  query.data(), worker->cfg->vemb_v16_dim,
                                  &score);
        common_core_account_read(worker, pending, rc, 0, rc == 0);
    } else {
        std::vector<float> vector(worker->cfg->vemb_v16_dim);
        uint32_t out_dim = 0;
        rc = vemb_v16_client_vemb_vector(
            slot->client, NULL, key.c_str(), vector.data(),
            worker->cfg->vemb_v16_dim, &out_dim);
        common_core_account_read(worker, pending, rc,
                                 rc == 0 ? out_dim * sizeof(float) : 0,
                                 rc == 0);
    }
    worker->issued++;
}

static void common_core_run_sync_ops(common_core_worker *worker,
                                     uint64_t deadline_ns)
{
    size_t next_slot = 0;
    while (!common_core_should_stop(worker, deadline_ns)) {
        common_core_slot *slot = &worker->slots[next_slot];
        next_slot = (next_slot + 1) % worker->slots.size();
        common_core_run_sync_op(worker, slot, common_core_next_op(worker));
    }
}

static void *common_core_worker_main(void *arg)
{
    common_core_worker *worker = static_cast<common_core_worker *>(arg);
    uint64_t leaders = 0;
    uint64_t followers = 0;
    struct timeval start;
    gettimeofday(&start, NULL);
    worker->stats->set_start_time(&start);

    for (std::vector<common_core_slot>::iterator it = worker->slots.begin();
         it != worker->slots.end(); ++it) {
        if (common_core_prepare_slot(&*it) != 0) {
            worker->setup_failed = true;
            worker->stop->store(true, std::memory_order_release);
            break;
        }
    }

    uint64_t deadline_ns = getMonotonicNs() +
        (uint64_t)worker->cfg->test_time * 1000000000ull;
    if (!worker->setup_failed) {
        if (common_core_pure_read(worker->cfg) ||
            common_core_key_key_session(worker->cfg))
            common_core_run_async_reads(worker, deadline_ns);
        else
            common_core_run_sync_ops(worker, deadline_ns);
    }

    for (std::vector<common_core_slot>::iterator it = worker->slots.begin();
         it != worker->slots.end(); ++it) {
        if (it->client) {
            vemb_v16_logical_stats_t logical;
            vemb_v16_redirect_stats_t redirects;
            vemb_v16_fanout_stats_t fanout;
            vemb_v16_client_owner_stats_snapshot_t owner_snapshot;
            vemb_v16_client_get_logical_stats(it->client, &logical);
            vemb_v16_client_get_redirect_stats(it->client, &redirects);
            vemb_v16_client_get_fanout_stats(it->client, &fanout);
            vemb_v16_client_get_owner_stats(it->client, &owner_snapshot);
            leaders += fanout.leaders;
            followers += fanout.followers;
            fprintf(stderr,
                    "[common-core] w%u slot logical[ok=%llu nf=%llu err=%llu] "
                    "redirect[ask=%llu moved=%llu stale=%llu refresh=%llu]\n",
                    worker->worker_id,
                    (unsigned long long)logical.successes,
                    (unsigned long long)logical.not_found,
                    (unsigned long long)logical.errors,
                    (unsigned long long)redirects.ask_redirects,
                    (unsigned long long)redirects.moved_redirects,
                    (unsigned long long)redirects.stale_topology_responses,
                    (unsigned long long)redirects.topology_refresh_calls);
            for (uint32_t owner = 0; owner < owner_snapshot.owner_count;
                 owner++) {
                const vemb_v16_client_owner_stats_t *owner_stats =
                    &owner_snapshot.owners[owner];
                if (owner_stats->submitted == 0 &&
                    owner_stats->completed == 0 &&
                    owner_stats->v1_requests == 0 &&
                    owner_stats->v2_items == 0)
                    continue;
                fprintf(stderr,
                        "[common-core] w%u owner=%u submitted=%llu completed=%llu "
                        "ok=%llu nf=%llu err=%llu retries=%llu v1=%llu "
                        "v2_frames=%llu v2_items=%llu fallback_v1=%llu "
                        "reopen=%llu handles=%llu materialize_ok=%llu "
                        "materialize_fail=%llu materialize_bytes=%llu pending_peak=%llu\n",
                        worker->worker_id, owner,
                        (unsigned long long)owner_stats->submitted,
                        (unsigned long long)owner_stats->completed,
                        (unsigned long long)owner_stats->ok,
                        (unsigned long long)owner_stats->not_found,
                        (unsigned long long)owner_stats->errors,
                        (unsigned long long)owner_stats->retries,
                        (unsigned long long)owner_stats->v1_requests,
                        (unsigned long long)owner_stats->v2_frames,
                        (unsigned long long)owner_stats->v2_items,
                        (unsigned long long)owner_stats->fallback_v1,
                        (unsigned long long)owner_stats->channel_reopen,
                        (unsigned long long)owner_stats->handle_region_count,
                        (unsigned long long)owner_stats->materialize_ok,
                        (unsigned long long)owner_stats->materialize_fail,
                        (unsigned long long)owner_stats->materialize_bytes,
                        (unsigned long long)owner_stats->pending_peak);
                fprintf(stderr,
                        "[common-core] w%u owner_transport owner=%u "
                        "pending=%llu active_groups=%llu "
                        "poll_calls=%llu poll_empty=%llu "
                        "poll_callbacks=%llu poll_samples=%llu "
                        "poll_ns_sum=%llu poll_ns_min=%llu poll_ns_max=%llu "
                        "flush_samples=%llu flush_ns_sum=%llu "
                        "v2_poll_samples=%llu v2_poll_ns_sum=%llu "
                        "v1_poll_samples=%llu v1_poll_ns_sum=%llu "
                        "callback_samples=%llu callback_ns_sum=%llu "
                        "callback_ns_min=%llu callback_ns_max=%llu "
                        "submit_to_callback_ns_sum=%llu "
                        "submit_to_callback_ns_min=%llu "
                        "submit_to_callback_ns_max=%llu "
                        "response_to_finish_samples=%llu "
                        "response_to_finish_ns_sum=%llu "
                        "response_to_finish_ns_min=%llu "
                        "response_to_finish_ns_max=%llu "
                        "finish_samples=%llu finish_ns_sum=%llu "
                        "finish_ns_min=%llu finish_ns_max=%llu "
                        "response_to_callback_samples=%llu "
                        "response_to_callback_ns_sum=%llu "
                        "response_to_callback_ns_min=%llu "
                        "response_to_callback_ns_max=%llu "
                        "submit_to_response_poll_samples=%llu "
                        "submit_to_response_poll_ns_sum=%llu "
                        "submit_to_response_poll_ns_min=%llu "
                        "submit_to_response_poll_ns_max=%llu "
                        "submit_to_publish_samples=%llu "
                        "submit_to_publish_ns_sum=%llu "
                        "submit_to_publish_ns_min=%llu "
                        "submit_to_publish_ns_max=%llu "
                        "submit_to_l0_samples=%llu submit_to_l0_ns_sum=%llu "
                        "submit_to_route_start_samples=%llu "
                        "submit_to_route_start_ns_sum=%llu "
                        "submit_to_drive_start_samples=%llu "
                        "submit_to_drive_start_ns_sum=%llu "
                        "drive_start_to_slot_poll_samples=%llu "
                        "drive_start_to_slot_poll_ns_sum=%llu "
                        "slot_poll_to_route_start_samples=%llu "
                        "slot_poll_to_route_start_ns_sum=%llu "
                        "route_duration_samples=%llu "
                        "route_duration_ns_sum=%llu "
                        "route_to_l0_start_samples=%llu "
                        "route_to_l0_start_ns_sum=%llu "
                        "l0_submit_duration_samples=%llu "
                        "l0_submit_duration_ns_sum=%llu "
                        "route_to_channel_start_samples=%llu "
                        "route_to_channel_start_ns_sum=%llu "
                        "channel_duration_samples=%llu "
                        "channel_duration_ns_sum=%llu "
                        "channel_to_path_start_samples=%llu "
                        "channel_to_path_start_ns_sum=%llu "
                        "owner_path_duration_samples=%llu "
                        "owner_path_duration_ns_sum=%llu "
                        "owner_path_to_l0_start_samples=%llu "
                        "owner_path_to_l0_start_ns_sum=%llu "
                        "v2_enable_duration_samples=%llu "
                        "v2_enable_duration_ns_sum=%llu "
                        "v2_enable_ready_fast=%llu "
                        "v2_enable_attach=%llu "
                        "v2_enable_reopen=%llu "
                        "l0_to_due_samples=%llu l0_to_due_ns_sum=%llu "
                        "l0_to_deadline_set_samples=%llu "
                        "l0_to_deadline_set_ns_sum=%llu "
                        "deadline_set_to_l0_samples=%llu "
                        "deadline_set_to_l0_ns_sum=%llu "
                        "deadline_set_to_due_samples=%llu "
                        "deadline_set_to_due_ns_sum=%llu "
                        "deadline_target_to_due_samples=%llu "
                        "deadline_target_to_due_ns_sum=%llu "
                        "deadline_poll_to_check_samples=%llu "
                        "deadline_poll_to_check_ns_sum=%llu "
                        "deadline_target_to_check_samples=%llu "
                        "deadline_target_to_check_ns_sum=%llu "
                        "l0_enqueue_to_deadline_target_samples=%llu "
                        "l0_enqueue_to_deadline_target_ns_sum=%llu "
                        "deadline_target_to_poll_check_samples=%llu "
                        "deadline_target_to_poll_check_ns_sum=%llu "
                        "deadline_check_duration_samples=%llu "
                        "deadline_check_duration_ns_sum=%llu "
                        "due_to_flush_req_samples=%llu "
                        "due_to_flush_req_ns_sum=%llu "
                        "flush_to_publish_samples=%llu "
                        "flush_to_publish_ns_sum=%llu "
                        "prepare_samples=%llu prepare_ns_sum=%llu "
                        "prepare_ns_min=%llu prepare_ns_max=%llu "
                        "publish_samples=%llu publish_ns_sum=%llu "
                        "publish_ns_min=%llu publish_ns_max=%llu "
                        "prepare_to_publish_ns_sum=%llu "
                        "deadline_set=%llu deadline_due=%llu "
                        "deadline_age_ns_sum=%llu "
                        "due_to_flush_ns_sum=%llu due_to_flush_ns_min=%llu "
                        "due_to_flush_ns_max=%llu "
                        "v2_batch_timing_samples=%llu "
                        "v2_publish_to_response_poll_ns_sum=%llu "
                        "v2_response_wait_ns_sum=%llu "
                        "callbacks_per_poll_max=%llu "
                        "flush_calls=%llu flush_full=%llu flush_deadline=%llu "
                        "flush_eager=%llu "
                        "flush_l0_bp=%llu ring_full=%llu publish_err=%llu\n",
                        worker->worker_id, owner,
                        (unsigned long long)owner_stats->pending_current,
                        (unsigned long long)owner_stats->active_groups_current,
                        (unsigned long long)owner_stats->poll_calls,
                        (unsigned long long)owner_stats->poll_empty,
                        (unsigned long long)owner_stats->poll_callbacks,
                        (unsigned long long)owner_stats->poll_latency_sample_count,
                        (unsigned long long)owner_stats->poll_latency_ns_sum,
                        (unsigned long long)owner_stats->poll_latency_ns_min,
                        (unsigned long long)owner_stats->poll_latency_ns_max,
                        (unsigned long long)owner_stats->poll_flush_sample_count,
                        (unsigned long long)owner_stats->poll_flush_ns_sum,
                        (unsigned long long)owner_stats->poll_v2_sample_count,
                        (unsigned long long)owner_stats->poll_v2_ns_sum,
                        (unsigned long long)owner_stats->poll_v1_sample_count,
                        (unsigned long long)owner_stats->poll_v1_ns_sum,
                        (unsigned long long)owner_stats->callback_sample_count,
                        (unsigned long long)owner_stats->callback_ns_sum,
                        (unsigned long long)owner_stats->callback_ns_min,
                        (unsigned long long)owner_stats->callback_ns_max,
                        (unsigned long long)owner_stats->submit_to_callback_ns_sum,
                        (unsigned long long)owner_stats->submit_to_callback_ns_min,
                        (unsigned long long)owner_stats->submit_to_callback_ns_max,
                        (unsigned long long)
                            owner_stats->response_to_finish_sample_count,
                        (unsigned long long)owner_stats->response_to_finish_ns_sum,
                        (unsigned long long)
                            owner_stats->response_to_finish_ns_min,
                        (unsigned long long)
                            owner_stats->response_to_finish_ns_max,
                        (unsigned long long)owner_stats->finish_sample_count,
                        (unsigned long long)owner_stats->finish_ns_sum,
                        (unsigned long long)owner_stats->finish_ns_min,
                        (unsigned long long)owner_stats->finish_ns_max,
                        (unsigned long long)
                            owner_stats->response_to_callback_sample_count,
                        (unsigned long long)
                            owner_stats->response_to_callback_ns_sum,
                        (unsigned long long)
                            owner_stats->response_to_callback_ns_min,
                        (unsigned long long)
                            owner_stats->response_to_callback_ns_max,
                        (unsigned long long)
                            owner_stats->submit_to_response_poll_sample_count,
                        (unsigned long long)
                            owner_stats->submit_to_response_poll_ns_sum,
                        (unsigned long long)
                            owner_stats->submit_to_response_poll_ns_min,
                        (unsigned long long)
                            owner_stats->submit_to_response_poll_ns_max,
                        (unsigned long long)
                            owner_stats->submit_to_publish_sample_count,
                        (unsigned long long)
                            owner_stats->submit_to_publish_ns_sum,
                        (unsigned long long)
                            owner_stats->submit_to_publish_ns_min,
                        (unsigned long long)
                            owner_stats->submit_to_publish_ns_max,
                        (unsigned long long)
                            owner_stats->submit_to_l0_sample_count,
                        (unsigned long long)owner_stats->submit_to_l0_ns_sum,
                        (unsigned long long)
                            owner_stats->submit_to_route_start_sample_count,
                            (unsigned long long)
                            owner_stats->submit_to_route_start_ns_sum,
                        (unsigned long long)
                            owner_stats->submit_to_drive_start_sample_count,
                        (unsigned long long)
                            owner_stats->submit_to_drive_start_ns_sum,
                        (unsigned long long)
                            owner_stats->drive_start_to_slot_poll_sample_count,
                        (unsigned long long)
                            owner_stats->drive_start_to_slot_poll_ns_sum,
                        (unsigned long long)
                            owner_stats->slot_poll_to_route_start_sample_count,
                        (unsigned long long)
                            owner_stats->slot_poll_to_route_start_ns_sum,
                        (unsigned long long)
                            owner_stats->route_duration_sample_count,
                        (unsigned long long)
                            owner_stats->route_duration_ns_sum,
                        (unsigned long long)
                            owner_stats->route_to_l0_start_sample_count,
                        (unsigned long long)
                            owner_stats->route_to_l0_start_ns_sum,
                        (unsigned long long)
                            owner_stats->l0_submit_duration_sample_count,
                        (unsigned long long)
                            owner_stats->l0_submit_duration_ns_sum,
                        (unsigned long long)
                            owner_stats->route_to_channel_start_sample_count,
                        (unsigned long long)
                            owner_stats->route_to_channel_start_ns_sum,
                        (unsigned long long)
                            owner_stats->channel_duration_sample_count,
                        (unsigned long long)
                            owner_stats->channel_duration_ns_sum,
                        (unsigned long long)
                            owner_stats->channel_to_path_start_sample_count,
                        (unsigned long long)
                            owner_stats->channel_to_path_start_ns_sum,
                        (unsigned long long)
                            owner_stats->owner_path_duration_sample_count,
                        (unsigned long long)
                            owner_stats->owner_path_duration_ns_sum,
                        (unsigned long long)
                            owner_stats->owner_path_to_l0_start_sample_count,
                        (unsigned long long)
                            owner_stats->owner_path_to_l0_start_ns_sum,
                        (unsigned long long)
                            owner_stats->v2_enable_duration_sample_count,
                        (unsigned long long)
                            owner_stats->v2_enable_duration_ns_sum,
                        (unsigned long long)
                            owner_stats->v2_enable_ready_fast_count,
                        (unsigned long long)
                            owner_stats->v2_enable_attach_count,
                        (unsigned long long)
                            owner_stats->v2_enable_reopen_count,
                        (unsigned long long)
                            owner_stats->l0_to_deadline_due_sample_count,
                        (unsigned long long)
                            owner_stats->l0_to_deadline_due_ns_sum,
                        (unsigned long long)
                            owner_stats->l0_to_deadline_set_sample_count,
                        (unsigned long long)
                            owner_stats->l0_to_deadline_set_ns_sum,
                        (unsigned long long)
                            owner_stats->deadline_set_to_l0_sample_count,
                        (unsigned long long)
                            owner_stats->deadline_set_to_l0_ns_sum,
                        (unsigned long long)
                            owner_stats->deadline_set_to_due_sample_count,
                        (unsigned long long)
                            owner_stats->deadline_set_to_due_ns_sum,
                        (unsigned long long)
                            owner_stats->deadline_target_to_due_sample_count,
                        (unsigned long long)
                            owner_stats->deadline_target_to_due_ns_sum,
                        (unsigned long long)
                            owner_stats->deadline_poll_to_check_sample_count,
                        (unsigned long long)
                            owner_stats->deadline_poll_to_check_ns_sum,
                        (unsigned long long)
                            owner_stats->deadline_target_to_check_sample_count,
                        (unsigned long long)
                            owner_stats->deadline_target_to_check_ns_sum,
                        (unsigned long long)
                            owner_stats->l0_enqueue_to_deadline_target_sample_count,
                        (unsigned long long)
                            owner_stats->l0_enqueue_to_deadline_target_ns_sum,
                        (unsigned long long)
                            owner_stats->deadline_target_to_poll_check_sample_count,
                        (unsigned long long)
                            owner_stats->deadline_target_to_poll_check_ns_sum,
                        (unsigned long long)
                            owner_stats->deadline_check_duration_sample_count,
                        (unsigned long long)
                            owner_stats->deadline_check_duration_ns_sum,
                        (unsigned long long)
                            owner_stats->deadline_due_to_flush_sample_count,
                        (unsigned long long)
                            owner_stats->deadline_due_to_flush_ns_sum,
                        (unsigned long long)
                            owner_stats->flush_to_publish_sample_count,
                        (unsigned long long)
                            owner_stats->flush_to_publish_ns_sum,
                        (unsigned long long)
                            owner_stats->v2_prepare_sample_count,
                        (unsigned long long)owner_stats->v2_prepare_ns_sum,
                        (unsigned long long)owner_stats->v2_prepare_ns_min,
                        (unsigned long long)owner_stats->v2_prepare_ns_max,
                        (unsigned long long)
                            owner_stats->v2_publish_sample_count,
                        (unsigned long long)owner_stats->v2_publish_ns_sum,
                        (unsigned long long)owner_stats->v2_publish_ns_min,
                        (unsigned long long)owner_stats->v2_publish_ns_max,
                        (unsigned long long)
                            owner_stats->v2_prepare_to_publish_ns_sum,
                        (unsigned long long)owner_stats->v2_deadline_set_count,
                        (unsigned long long)owner_stats->v2_deadline_due_count,
                        (unsigned long long)owner_stats->v2_deadline_age_ns_sum,
                        (unsigned long long)
                            owner_stats->v2_deadline_due_to_flush_ns_sum,
                        (unsigned long long)
                            owner_stats->v2_deadline_due_to_flush_ns_min,
                        (unsigned long long)
                            owner_stats->v2_deadline_due_to_flush_ns_max,
                        (unsigned long long)
                            owner_stats->v2_batch_timing_sample_count,
                        (unsigned long long)
                            owner_stats->v2_publish_to_response_poll_ns_sum,
                        (unsigned long long)
                            owner_stats->v2_response_wait_ns_sum,
                        (unsigned long long)owner_stats->callback_count_per_poll_max,
                        (unsigned long long)owner_stats->v2_flush_calls,
                        (unsigned long long)owner_stats->v2_flush_full,
                        (unsigned long long)owner_stats->v2_flush_deadline,
                        (unsigned long long)owner_stats->v2_flush_eager,
                        (unsigned long long)owner_stats->v2_flush_l0_backpressure,
                        (unsigned long long)owner_stats->v2_publish_ring_full,
                        (unsigned long long)owner_stats->v2_publish_errors);
            }
            for (uint32_t i = 0; i < owner_snapshot.owner_region_count; i++) {
                const vemb_v16_client_owner_region_stats_t *region =
                    &owner_snapshot.owner_regions[i];
                fprintf(stderr,
                        "[common-core] w%u owner_region owner=%u region=%u "
                        "handles=%llu copies=%llu samples=%llu "
                        "copy_ns_sum=%llu copy_ns_min=%llu copy_ns_max=%llu\n",
                        worker->worker_id,
                        region->owner_id, region->region_id,
                        (unsigned long long)region->handle_count,
                        (unsigned long long)region->copy_count,
                        (unsigned long long)region->copy_sample_count,
                        (unsigned long long)region->copy_latency_ns_sum,
                        (unsigned long long)region->copy_latency_ns_min,
                        (unsigned long long)region->copy_latency_ns_max);
                fprintf(stderr,
                        "[common-core] w%u owner_region_copy_hist "
                        "owner=%u region=%u log2_ns=",
                        worker->worker_id, region->owner_id,
                        region->region_id);
                for (uint32_t bucket = 0;
                     bucket < VEMB_V16_CLIENT_COPY_LATENCY_BUCKETS; bucket++)
                    fprintf(stderr, "%s%llu", bucket == 0 ? "" : ",",
                            (unsigned long long)
                                region->copy_latency_ns_buckets[bucket]);
                fprintf(stderr, "\n");
            }
            if (it == worker->slots.begin()) {
                vemb_v16_diagnostic_stats_t diagnostic;
                if (vemb_v16_client_diagnostic_stats(it->client,
                                                     &diagnostic) == 0) {
                    fprintf(stderr,
                            "[common-core] diagnostic cache_hit=%llu "
                            "cache_miss=%llu warm_local_hit=%llu "
                            "warm_imported_hit=%llu cold_promote=%llu "
                            "final_miss=%llu handle_miss=%llu "
                            "handle_not_found=%llu handle_moved=%llu "
                            "handle_stale=%llu regions=%u\n",
                            (unsigned long long)diagnostic.lookup_cache_hit,
                            (unsigned long long)diagnostic.lookup_cache_miss,
                            (unsigned long long)diagnostic.warm_local_hit,
                            (unsigned long long)diagnostic.warm_imported_hit,
                            (unsigned long long)diagnostic.cold_promote,
                            (unsigned long long)diagnostic.lookup_final_miss,
                            (unsigned long long)diagnostic.handle_lookup_miss,
                            (unsigned long long)diagnostic.handle_lookup_miss_not_found,
                            (unsigned long long)diagnostic.handle_lookup_miss_moved,
                            (unsigned long long)diagnostic.handle_lookup_miss_stale,
                            diagnostic.region_count);
                    for (uint32_t region = 0;
                         region < diagnostic.region_count; region++) {
                        const vemb_v16_diagnostic_region_stats_t *r =
                            &diagnostic.regions[region];
                        fprintf(stderr,
                                "[common-core] diagnostic_region index=%u id=%u "
                                "local=%u lookup_hits=%llu cold_promotes=%llu\n",
                                r->region_index, r->region_id, r->is_local,
                                (unsigned long long)r->lookup_hits,
                                (unsigned long long)r->cold_promotes);
                    }
                }
            }
        }
        common_core_close_slot(&*it);
    }

    struct timeval end;
    gettimeofday(&end, NULL);
    worker->stats->set_end_time(&end);
    fprintf(stderr,
            "[common-core] w%u done: ops_done=%llu status_ok=%llu "
            "status_nf=%llu status_err=%llu materialized_ok=%llu "
            "materialized_fail=%llu unmatched=%llu leaders=%llu "
            "followers=%llu completion_samples=%llu "
            "completion_avg_ns=%llu completion_min_ns=%llu "
            "completion_max_ns=%llu "
            "lookup_avg_ns=%llu lookup_min_ns=%llu lookup_max_ns=%llu "
            "materialize_samples=%llu materialize_avg_ns=%llu "
            "materialize_min_ns=%llu materialize_max_ns=%llu "
            "account_avg_ns=%llu account_min_ns=%llu account_max_ns=%llu\n",
            worker->worker_id, (unsigned long long)worker->completed,
            (unsigned long long)worker->status_ok,
            (unsigned long long)worker->status_not_found,
            (unsigned long long)worker->status_error,
            (unsigned long long)worker->materialized_ok,
            (unsigned long long)worker->materialized_fail,
            (unsigned long long)worker->unmatched,
            (unsigned long long)leaders,
            (unsigned long long)followers,
            (unsigned long long)worker->completion_sample_count,
            worker->completion_sample_count == 0 ? 0ull :
                (unsigned long long)(worker->completion_latency_ns_sum /
                                     worker->completion_sample_count),
            (unsigned long long)worker->completion_latency_ns_min,
            (unsigned long long)worker->completion_latency_ns_max,
            worker->completion_sample_count == 0 ? 0ull :
                (unsigned long long)(worker->completion_lookup_ns_sum /
                                     worker->completion_sample_count),
            (unsigned long long)worker->completion_lookup_ns_min,
            (unsigned long long)worker->completion_lookup_ns_max,
            (unsigned long long)worker->materialize_sample_count,
            worker->materialize_sample_count == 0 ? 0ull :
                (unsigned long long)(worker->materialize_ns_sum /
                                     worker->materialize_sample_count),
            (unsigned long long)worker->materialize_ns_min,
            (unsigned long long)worker->materialize_ns_max,
            worker->completion_sample_count == 0 ? 0ull :
                (unsigned long long)(worker->completion_account_ns_sum /
                                     worker->completion_sample_count),
            (unsigned long long)worker->completion_account_ns_min,
            (unsigned long long)worker->completion_account_ns_max);
    return NULL;
}

static uint64_t common_core_cli_stats_interval_ns_from_env(void)
{
    const char *value = getenv("VEMB_V16_CLI_STATS_INTERVAL_MS");
    if (!value || !value[0])
        return 0;
    char *end = NULL;
    unsigned long long interval_ms = strtoull(value, &end, 10);
    if (end == value || *end != '\0' || interval_ms == 0)
        return 0;
    if (interval_ms > 60000ull)
        interval_ms = 60000ull;
    return (uint64_t)interval_ms * 1000000ull;
}

static bool common_core_round_robin_slots_from_env(void)
{
    const char *value = getenv("VEMB_V16_SLOT_SCHED");
    return value && strcmp(value, "round_robin") == 0;
}

static bool common_core_parse_seeds(const benchmark_config *cfg,
                                    std::vector<std::string> *storage,
                                    std::vector<const char *> *seeds)
{
    std::string list = cfg->vemb_v16_endpoints && cfg->vemb_v16_endpoints[0] ?
        cfg->vemb_v16_endpoints : "";
    if (list.empty()) {
        char endpoint[128];
        snprintf(endpoint, sizeof(endpoint), "%s:%u",
                 cfg->server && cfg->server[0] ? cfg->server : VEMB_V16_TCP_HOST,
                 cfg->port ? cfg->port : VEMB_V16_TCP_PORT);
        list = endpoint;
    }

    size_t begin = 0;
    while (begin < list.size()) {
        size_t end = list.find(',', begin);
        std::string seed = list.substr(begin, end == std::string::npos ?
                                        std::string::npos : end - begin);
        size_t first = seed.find_first_not_of(" \t");
        size_t last = seed.find_last_not_of(" \t");
        if (first == std::string::npos)
            return false;
        storage->push_back(seed.substr(first, last - first + 1));
        if (end == std::string::npos)
            break;
        begin = end + 1;
    }
    if (storage->empty() || storage->size() > 16)
        return false;
    seeds->reserve(storage->size());
    for (std::vector<std::string>::const_iterator it = storage->begin();
         it != storage->end(); ++it) {
        seeds->push_back(it->c_str());
    }
    return true;
}

run_stats vemb_v16_aeron_run(benchmark_config *cfg, object_generator *obj_gen)
{
    std::vector<std::string> seed_storage;
    std::vector<const char *> seeds;
    if (!common_core_parse_seeds(cfg, &seed_storage, &seeds)) {
        benchmark_error_log("[common-core] invalid VEMB TCP bootstrap endpoint list\n");
        exit(1);
    }
    if (cfg->vemb_v16_l1_entries != 0 &&
        !common_core_l1_entries_valid(cfg->vemb_v16_l1_entries)) {
        benchmark_error_log(
            "[common-core] --vemb-v16-l1-entries must be 4 times a power of two\n");
        exit(1);
    }
    monotonicInit();
    uint64_t cli_stats_interval_ns =
        common_core_cli_stats_interval_ns_from_env();
    const bool round_robin_slots = common_core_round_robin_slots_from_env();
    const bool use_handle_session = (common_core_pure_read(cfg) ||
                                     common_core_key_key_session(cfg)) &&
        cfg->vemb_v16_l1_entries == 0 && !cfg->vemb_v16_batch_disable;
    fprintf(stderr,
            "[common-core] runner start: mode=%s seeds=%zu t=%u c=%u pipeline=%u "
            "dim=%u read-session=%s cache_entries=%u\n",
            cfg->vemb_v16_transport, seeds.size(), cfg->threads, cfg->clients,
            cfg->pipeline, cfg->vemb_v16_dim,
            use_handle_session ? "handle" :
            common_core_pure_read(cfg) ? "vector" : "sync",
            cfg->vemb_v16_l1_entries);
    fprintf(stderr, "[common-core] slot-sched=%s\n",
            round_robin_slots ? "round_robin" : "fixed");
    fprintf(stderr, "[common-core] cli-stat interval_ms=%llu\n",
            (unsigned long long)(cli_stats_interval_ns / 1000000ull));

    std::atomic<bool> stop(false);
    std::vector<common_core_worker *> workers;
    std::vector<pthread_t> threads(cfg->threads);
    workers.reserve(cfg->threads);

    unsigned long long base_budget = cfg->requests / cfg->threads;
    unsigned long long extra_budget = cfg->requests % cfg->threads;
    for (unsigned int i = 0; i < cfg->threads; i++) {
        common_core_worker *worker = new common_core_worker();
        worker->cfg = cfg;
        worker->obj_gen = obj_gen->clone();
        worker->obj_gen->set_random_seed((int)i + 1);
        worker->seeds = &seeds;
        worker->worker_id = i;
        worker->budget = cfg->requests ? base_budget + (i < extra_budget) : 0;
        worker->issued = 0;
        worker->completed = 0;
        worker->status_ok = 0;
        worker->status_not_found = 0;
        worker->status_error = 0;
        worker->materialized_ok = 0;
        worker->materialized_fail = 0;
        worker->unmatched = 0;
        worker->set_ratio_count = 0;
        worker->get_ratio_count = 0;
        worker->key2_rng = 42 + i;
        worker->use_handle_session = use_handle_session;
        worker->setup_failed = false;
        worker->round_robin_slots = round_robin_slots;
        worker->cli_stats_interval_ns = cli_stats_interval_ns;
        worker->stats = new run_stats(cfg);
        worker->stop = &stop;
        worker->next_slot = 0;
        worker->slots.resize(cfg->clients);
        for (std::vector<common_core_slot>::iterator it =
                 worker->slots.begin(); it != worker->slots.end(); ++it) {
            it->worker = worker;
            it->client = NULL;
            it->handle_session = NULL;
            it->vector_session = NULL;
            it->drive_count = 0;
            it->drive_queue_ns_sum = 0;
            it->drive_queue_ns_min = 0;
            it->drive_queue_ns_max = 0;
            it->poll_ns_sum = 0;
            it->poll_ns_min = 0;
            it->poll_ns_max = 0;
            it->callback_count = 0;
            it->empty_drive_count = 0;
        }
        workers.push_back(worker);
    }

    for (unsigned int i = 0; i < cfg->threads; i++) {
        int rc = pthread_create(&threads[i], NULL, common_core_worker_main,
                                workers[i]);
        if (rc != 0) {
            benchmark_error_log("[common-core] pthread_create %u failed: %s\n",
                                i, strerror(rc));
            stop.store(true, std::memory_order_release);
            for (unsigned int j = 0; j < i; j++)
                pthread_join(threads[j], NULL);
            exit(1);
        }
    }
    for (unsigned int i = 0; i < cfg->threads; i++)
        pthread_join(threads[i], NULL);
    fprintf(stderr, "[common-core] all workers joined\n");

    run_stats merged(cfg);
    int iteration = 0;
    bool setup_failed = false;
    for (std::vector<common_core_worker *>::iterator it = workers.begin();
         it != workers.end(); ++it) {
        setup_failed = setup_failed || (*it)->setup_failed;
        merged.merge(*(*it)->stats, iteration++);
    }
    fprintf(stderr,
            "[common-core] cache-summary: enabled=%u entries_per_client=%u "
            "scope=sdk-session immutable_snapshot=%u\n",
            cfg->vemb_v16_l1_entries != 0, cfg->vemb_v16_l1_entries,
            cfg->vemb_v16_l1_entries != 0);

    for (std::vector<common_core_worker *>::iterator it = workers.begin();
         it != workers.end(); ++it) {
        delete (*it)->obj_gen;
        delete (*it)->stats;
        delete *it;
    }
    if (setup_failed) {
        benchmark_error_log("[common-core] SDK setup or data-plane session failed\n");
        exit(1);
    }
    return merged;
}
