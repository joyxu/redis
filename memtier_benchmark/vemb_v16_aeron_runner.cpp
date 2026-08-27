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
#include <string>
#include <unordered_map>
#include <vector>

extern "C" {
#include "vemb_v16_client_sdk.h"
#include "monotonic.h"
}

enum {
    COMMON_CORE_SET_CMD_IDX = 0,
    COMMON_CORE_GET_CMD_IDX = 2,
    COMMON_CORE_SYNC_BATCH_MAX = 32,
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
    bool use_handle_session;
    bool setup_failed;
    run_stats *stats;
    std::atomic<bool> *stop;
    std::vector<common_core_slot> slots;
};

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

static void common_core_fill_vector(float *vector, uint32_t dim,
                                    uint32_t global_id)
{
    uint32_t state = global_id * 2654435761u + 12345u;
    for (uint32_t i = 0; i < dim; i++) {
        state = state * 1103515245u + 12345u;
        uint32_t bits = (state >> 9) | 0x40000000u;
        float value;
        memcpy(&value, &bits, sizeof(value));
        vector[i] = value * 0.001f;
    }
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

static bool common_core_pure_read(const benchmark_config *cfg)
{
    return !cfg->vemb_v16_vsim && !cfg->vemb_v16_vrem &&
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
    std::unordered_map<uint64_t, common_core_pending>::iterator it =
        slot->pending.find(caller_cookie);
    if (it == slot->pending.end()) {
        slot->worker->unmatched++;
        return;
    }

    common_core_pending pending = it->second;
    slot->pending.erase(it);
    bool materialized = false;
    if (response->status == 0) {
        float vector[VEMB_V16_MAX_DIM];
        materialized = vemb_v16_client_read_vector(
            slot->client, response->offset, response->bytes, vector,
            slot->worker->cfg->vemb_v16_dim) == 0;
    }
    common_core_account_read(slot->worker, pending, response->status,
                             response->bytes, materialized);
}

static void common_core_vector_completion(
    void *priv, uint64_t caller_cookie,
    const vemb_v16_pipeline_resp_t *response, const float *vector)
{
    common_core_slot *slot = static_cast<common_core_slot *>(priv);
    std::unordered_map<uint64_t, common_core_pending>::iterator it =
        slot->pending.find(caller_cookie);
    if (it == slot->pending.end()) {
        slot->worker->unmatched++;
        return;
    }

    common_core_pending pending = it->second;
    slot->pending.erase(it);
    common_core_account_read(slot->worker, pending, response->status,
                             response->bytes,
                             response->status != 0 || vector != NULL);
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

    if (!common_core_pure_read(worker->cfg))
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

static int common_core_drive_sessions(common_core_worker *worker)
{
    int callbacks = 0;
    for (std::vector<common_core_slot>::iterator it = worker->slots.begin();
         it != worker->slots.end(); ++it) {
        int rc;
        if (worker->use_handle_session) {
            /* poll() owns handle-session routing and deadline/full flushes.
             * An explicit flush here would defeat max_batch_delay_us by
             * eagerly publishing the small set released by the last poll. */
            rc = vemb_v16_client_handle_session_poll(
                it->handle_session, common_core_handle_completion, &*it);
        } else {
            if (vemb_v16_client_vector_session_flush(it->vector_session) != 0)
                return -1;
            rc = vemb_v16_client_vector_session_poll(
                it->vector_session, common_core_vector_completion, &*it);
        }
        if (rc < 0)
            return -1;
        callbacks += rc;
    }
    return callbacks;
}

static void common_core_run_async_reads(common_core_worker *worker,
                                        uint64_t deadline_ns)
{
    const uint32_t pipeline = std::max(1u, worker->cfg->pipeline);
    uint64_t next_cookie = 1;

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
                int rc = worker->use_handle_session ?
                    vemb_v16_client_handle_session_submit(
                        it->handle_session, NULL, key.c_str(), cookie) :
                    vemb_v16_client_vector_session_submit(
                        it->vector_session, NULL, key.c_str(), cookie);
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
        common_core_fill_vector(
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
        common_core_fill_vector(query.data(), worker->cfg->vemb_v16_dim,
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
        if (common_core_pure_read(worker->cfg))
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
            vemb_v16_client_get_logical_stats(it->client, &logical);
            vemb_v16_client_get_redirect_stats(it->client, &redirects);
            vemb_v16_client_get_fanout_stats(it->client, &fanout);
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
            "followers=%llu\n",
            worker->worker_id, (unsigned long long)worker->completed,
            (unsigned long long)worker->status_ok,
            (unsigned long long)worker->status_not_found,
            (unsigned long long)worker->status_error,
            (unsigned long long)worker->materialized_ok,
            (unsigned long long)worker->materialized_fail,
            (unsigned long long)worker->unmatched,
            (unsigned long long)leaders,
            (unsigned long long)followers);
    return NULL;
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
    const bool use_handle_session = common_core_pure_read(cfg) &&
        cfg->vemb_v16_l1_entries == 0 && !cfg->vemb_v16_batch_disable;
    fprintf(stderr,
            "[common-core] runner start: mode=%s seeds=%zu t=%u c=%u pipeline=%u "
            "dim=%u read-session=%s cache_entries=%u\n",
            cfg->vemb_v16_transport, seeds.size(), cfg->threads, cfg->clients,
            cfg->pipeline, cfg->vemb_v16_dim,
            use_handle_session ? "handle" :
            common_core_pure_read(cfg) ? "vector" : "sync",
            cfg->vemb_v16_l1_entries);

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
        worker->use_handle_session = use_handle_session;
        worker->setup_failed = false;
        worker->stats = new run_stats(cfg);
        worker->stop = &stop;
        worker->slots.resize(cfg->clients);
        for (std::vector<common_core_slot>::iterator it =
                 worker->slots.begin(); it != worker->slots.end(); ++it) {
            it->worker = worker;
            it->client = NULL;
            it->handle_session = NULL;
            it->vector_session = NULL;
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
