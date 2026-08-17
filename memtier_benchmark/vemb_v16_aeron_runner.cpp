/*
 * Copyright (C) 2026 Redis Labs Ltd.
 *
 * This file is part of memtier_benchmark.
 *
 * memtier_benchmark is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2.
 *
 * memtier_benchmark is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "vemb_v16_aeron_runner.h"
#include "vemb_v16_aeron_runner_plan.h"
#include "vemb_v16_aeron_local_completion.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/time.h>
#include <atomic>
#include <vector>

#include "vemb_v16_protocol.h"

/* Transport mode: "aeron" (TCP control + local UB ring) or
 * "aeron-cross-node" (TCP attach + UB ring).
 * Set by main() from --vemb-v16-transport via vemb_v16_aeron_set_transport().
 * Default = "aeron" preserves existing loopback behavior. */
static std::string g_aeron_transport_mode = "aeron";
static std::string g_aeron_remote_endpoint;  /* "host:port" for cross-node */

void vemb_v16_aeron_set_transport(const std::string &mode,
                                  const std::string &endpoint) {
    g_aeron_transport_mode = mode;
    g_aeron_remote_endpoint = endpoint;
}

extern "C" {
/* SDK ships the aeron (TCP control + UB SPSC ring) transport as opaque handles.
 * No C11 <stdatomic.h> dependency leaks into this C++ translation unit. */
#include "vemb_v16_client_sdk.h"
}

/* ------------------------------------------------------------------ */
/* Worker state                                                       */
/* ------------------------------------------------------------------ */

struct pending_op {
    uint32_t     req_id;
    int          is_set;     /* 1=set/vadd/vrem, 0=get/vemb/vsim */
    struct timeval sent_time;
    uint32_t     bytes_tx;
};

struct worker_arg {
    benchmark_config    *cfg;
    object_generator    *obj_gen;
    uint32_t             worker_id;
    bool                skip_handle_read;
    /* channels — SDK owns the handles; we hold pointers */
    std::vector<vemb_v16_aeron_channel_t *> channels;
    std::vector<vemb_v16_aeron_batch_client_t *> batch_clients;
    bool                batch_sessions_enabled;
    /* SDK-owned worker-local completed-vector cache. */
    vemb_v16_cli_l1_t *l1;
    vemb_v16_aeron_local_completion_queue local_completions;
    std::vector<vemb_v16_aeron_remote_batch_wave> remote_waves;
    uint64_t local_completion_count;
    uint64_t l1_ub_read_bytes_saved;
    uint64_t l1_queue_full_fallbacks;
    /* ratio bookkeeping for mixed workloads */
    unsigned long set_ratio_count;
    unsigned long get_ratio_count;
    /* pending slots — pipeline depth per channel */
    std::vector<std::vector<pending_op>> pending;  /* [ch][slot] */
    std::vector<uint32_t>                pending_head;
    std::vector<uint32_t>                pending_tail;
    std::vector<uint32_t>                pending_count;
    /* stats — pointer because run_stats has no default ctor */
    run_stats    *stats;
    /* coordination */
    std::atomic<bool> *stop;
    std::atomic<bool>  done;
    std::atomic<unsigned long long> ops_done;
    std::atomic<unsigned long long> ops_fail;
    /* budget (-n requests total / N workers) */
    unsigned long long budget;
};

static uint64_t now_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static bool l1_entry_count_valid(uint32_t entry_count) {
    if (entry_count < VEMB_V16_CLI_L1_WAYS ||
        entry_count % VEMB_V16_CLI_L1_WAYS != 0)
        return false;
    uint32_t set_count = entry_count / VEMB_V16_CLI_L1_WAYS;
    return (set_count & (set_count - 1u)) == 0;
}

struct aeron_l1_stats_total {
    uint64_t hits;
    uint64_t misses;
    uint64_t inserts;
    uint64_t evicts;
    uint64_t exact_key_mismatch;
    uint64_t all_pinned;
    uint64_t key_storage_exhausted;
    uint64_t vector_storage_exhausted;
    uint64_t stale_ref;
    uint64_t live_vector_bytes;
    uint64_t live_entries;
};

static void l1_stats_add(aeron_l1_stats_total *total,
                         const vemb_v16_cli_l1_stats_t *worker) {
    total->hits += worker->hits;
    total->misses += worker->misses;
    total->inserts += worker->inserts;
    total->evicts += worker->evicts;
    total->exact_key_mismatch += worker->exact_key_mismatch;
    total->all_pinned += worker->all_pinned;
    total->key_storage_exhausted += worker->key_storage_exhausted;
    total->vector_storage_exhausted += worker->vector_storage_exhausted;
    total->stale_ref += worker->stale_ref;
    total->live_vector_bytes += worker->live_vector_bytes;
    total->live_entries += worker->live_entries;
}

/* Map cfg->key_pattern[idx] to object_generator iterator type.
 * Mirrors client.h:151 obj_iter_type — sequential patterns use SET/GET
 * iterators (stateful counters), R/G/Z are stateless distributions. */
#define AERON_SET_CMD_IDX 0
#define AERON_GET_CMD_IDX 2
#define AERON_BATCH_SIZE 32u
static int obj_iter_type(benchmark_config *cfg, unsigned char index) {
    if (cfg->key_pattern[index] == 'R') return OBJECT_GENERATOR_KEY_RANDOM;
    if (cfg->key_pattern[index] == 'G') return OBJECT_GENERATOR_KEY_GAUSSIAN;
    if (cfg->key_pattern[index] == 'Z') return OBJECT_GENERATOR_KEY_ZIPFIAN;
    if (index == key_pattern_set) return OBJECT_GENERATOR_KEY_SET_ITER;
    return OBJECT_GENERATOR_KEY_GET_ITER;
}

/* Fill a vector deterministically — same intent as vemb_v16_bench.c fill_vector.
 * We use a simple LCG per index to avoid runtime randomness cost on hot path. */
static void fill_vector_for_index(float *vec, uint32_t dim, uint32_t global_id) {
    uint32_t s = global_id * 2654435761u + 12345u;
    for (uint32_t i = 0; i < dim; i++) {
        s = s * 1103515245u + 12345u;
        uint32_t bits = (s >> 9) | 0x40000000u;  /* finite non-zero */
        float f;
        memcpy(&f, &bits, sizeof(f));
        vec[i] = f * 0.001f;
    }
}

/* Build a vemb_v16_req_t for VADD/VREM/VEMB/VSIM. Mirrors vemb_v16_bench.c:759
 * prepare_req + protocol.cpp:1470 write_command_set/get. */
static size_t build_req(worker_arg *w, unsigned long long global_op_idx,
                        uint8_t op, const char *key, int key_len,
                        vemb_v16_req_t *req, uint32_t *out_actual_key_len) {
    uint32_t actual_key_len = (key_len < (int)VEMB_V16_MAX_KEY_LEN)
        ? (uint32_t)key_len : VEMB_V16_MAX_KEY_LEN - 1;
    memset(req, 0, sizeof(*req));
    req->op = op;
    req->req_id = (uint32_t)(global_op_idx + 1);
    req->key_hash = vemb_v16_xxh3_64_str(key, actual_key_len);
    req->key_len = actual_key_len;
    req->dim = w->cfg->vemb_v16_dim;
    memcpy(req->key, key, actual_key_len);

    if (op == VEMB_V16_OP_VADD) {
        req->vector_bytes = w->cfg->vemb_v16_dim * sizeof(float);
        fill_vector_for_index(req->vector, w->cfg->vemb_v16_dim, (uint32_t)global_op_idx);
        *out_actual_key_len = actual_key_len;
        return vemb_v16_req_inline_len(req->vector_bytes);
    }
    if (op == VEMB_V16_OP_VREM) {
        req->dim = 0;
        req->vector_bytes = 0;
        *out_actual_key_len = actual_key_len;
        return vemb_v16_req_handle_len();
    }
    if (op == VEMB_V16_OP_VSIM_INLINE) {
        req->vector_bytes = w->cfg->vemb_v16_dim * sizeof(float);
        fill_vector_for_index(req->vector, w->cfg->vemb_v16_dim,
                              (uint32_t)global_op_idx + 0x9e3779b9u);
        *out_actual_key_len = actual_key_len;
        return vemb_v16_req_inline_len(req->vector_bytes);
    }
    /* VEMB_INLINE / VEMB_HANDLE: key-only request on the wire (handle_len),
     * but the server still validates vector_bytes == dim*sizeof(float) for
     * shape consistency (else returns shape_mismatch). Match vemb_v16_bench's
     * prepare_req convention by setting vector_bytes even though we don't
     * send the payload. */
    req->dim = w->cfg->vemb_v16_dim;
    req->vector_bytes = w->cfg->vemb_v16_dim * sizeof(float);
    *out_actual_key_len = actual_key_len;
    return vemb_v16_req_handle_len();
}

/* Decide next op for this worker. Mirrors client.cpp:365 create_request. */
static uint8_t pick_next_op(worker_arg *w) {
    /* vsim/vrem override */
    if (w->cfg->vemb_v16_vsim) return VEMB_V16_OP_VSIM_INLINE;
    if (w->cfg->vemb_v16_vrem) return VEMB_V16_OP_VREM;

    /* pure set (--ratio=1:0) */
    if (w->cfg->ratio.a > 0 && w->cfg->ratio.b == 0) return VEMB_V16_OP_VADD;
    /* pure get (--ratio=0:1) — default to VEMB_HANDLE per memtier help text */
    if (w->cfg->ratio.b > 0 && w->cfg->ratio.a == 0) return VEMB_V16_OP_VEMB_HANDLE;

    /* mixed — rotate by ratio */
    if (w->set_ratio_count < w->cfg->ratio.a) {
        w->set_ratio_count++;
        if (w->set_ratio_count >= w->cfg->ratio.a &&
            w->get_ratio_count >= w->cfg->ratio.b) {
            w->set_ratio_count = 0;
            w->get_ratio_count = 0;
        }
        return VEMB_V16_OP_VADD;
    }
    if (w->get_ratio_count < w->cfg->ratio.b) {
        w->get_ratio_count++;
        if (w->set_ratio_count >= w->cfg->ratio.a &&
            w->get_ratio_count >= w->cfg->ratio.b) {
            w->set_ratio_count = 0;
            w->get_ratio_count = 0;
        }
        return VEMB_V16_OP_VEMB_HANDLE;
    }
    /* fallthrough — shouldn't happen */
    w->set_ratio_count = 0;
    w->get_ratio_count = 0;
    return VEMB_V16_OP_VEMB_HANDLE;
}

static int is_set_op(uint8_t op) {
    return op == VEMB_V16_OP_VADD || op == VEMB_V16_OP_VREM;
}

struct aeron_debug_counters {
    uint64_t *status_ok;
    uint64_t *status_notfound;
    uint64_t *status_err;
    uint64_t *status_other;
    uint64_t *handle_deref_ok;
    uint64_t *handle_deref_fail;
    uint64_t *unmatched_responses;
};

static int account_response(worker_arg *w,
                            uint32_t ch,
                            uint32_t pipeline,
                            const vemb_v16_resp_t *resp,
                            uint32_t wire_bytes,
                            float *vec_scratch,
                            aeron_debug_counters *debug,
                            const vemb_v16_aeron_batch_vector_view_t *vector_view) {
    if (w->pending_count[ch] == 0)
        return 0;

    uint32_t slot = w->pending_head[ch];
    pending_op *pe = &w->pending[ch][slot];
    if (pe->req_id != resp->req_id) {
        int found = -1;
        for (uint32_t j = 0; j < w->pending_count[ch]; j++) {
            uint32_t s2 = (w->pending_head[ch] + j) % pipeline;
            if (w->pending[ch][s2].req_id == resp->req_id) {
                found = (int)s2;
                break;
            }
        }
        if (found < 0)
        {
            (*debug->unmatched_responses)++;
            if (*debug->unmatched_responses <= 4) {
                fprintf(stderr,
                        "[aeron] unmatched response: channel=%u req_id=%u "
                        "pending_head=%u pending_count=%u expected_req_id=%u\n",
                        ch, resp->req_id, w->pending_head[ch],
                        w->pending_count[ch], pe->req_id);
            }
            return 0;
        }
        if (found != (int)slot) {
            pending_op tmp = w->pending[ch][slot];
            w->pending[ch][slot] = w->pending[ch][found];
            w->pending[ch][found] = tmp;
            pe = &w->pending[ch][slot];
        }
    }

    w->pending_head[ch] = (slot + 1) % pipeline;
    w->pending_count[ch]--;

    struct timeval now;
    gettimeofday(&now, NULL);
    unsigned int latency_usec = (unsigned int)ts_diff(pe->sent_time, now);
    unsigned int bytes_rx = wire_bytes;
    if (pe->is_set) {
        w->stats->update_set_op(&now, bytes_rx, pe->bytes_tx, latency_usec);
    } else {
        switch (resp->status) {
            case VEMB_V16_STATUS_OK: (*debug->status_ok)++; break;
            case VEMB_V16_STATUS_NOT_FOUND: (*debug->status_notfound)++; break;
            case VEMB_V16_STATUS_ERR: (*debug->status_err)++; break;
            default: (*debug->status_other)++; break;
        }
        unsigned int hits = 0, misses = 0;
        if (resp->status == VEMB_V16_STATUS_OK) {
            if (vector_view && vector_view->attempted) {
                if (vector_view->valid) {
                    hits = 1;
                    (*debug->handle_deref_ok)++;
                } else {
                    (*debug->handle_deref_fail)++;
                }
            } else if (w->skip_handle_read) {
                hits = 1;
            } else {
                int n = vemb_v16_aeron_read_vector(w->channels[ch],
                                                   resp->region_id,
                                                   resp->vector_offset,
                                                   resp->vector_bytes,
                                                   vec_scratch,
                                                   sizeof(float) * VEMB_V16_MAX_DIM);
                if (n > 0) { hits = 1; (*debug->handle_deref_ok)++; }
                else       { (*debug->handle_deref_fail)++; }
            }
        } else if (resp->status == VEMB_V16_STATUS_NOT_FOUND) {
            misses = 1;
        }
        w->stats->update_get_op(&now, bytes_rx, pe->bytes_tx,
                                latency_usec, hits, misses);
    }
    w->ops_done.fetch_add(1, std::memory_order_relaxed);
    return 1;
}

struct batch_completion_ctx {
    worker_arg *worker;
    uint32_t channel_index;
    uint32_t pipeline;
    float *vec_scratch;
    aeron_debug_counters *debug;
    int completed;
};

static void account_batch_completion(void *priv, uint64_t caller_cookie,
                                     const vemb_v16_resp_t *response) {
    batch_completion_ctx *ctx = (batch_completion_ctx *)priv;
    vemb_v16_resp_t logical_response = *response;
    logical_response.req_id = (uint32_t)caller_cookie;
    int accounted = account_response(ctx->worker, ctx->channel_index,
                                     ctx->pipeline, &logical_response, 0,
                                     ctx->vec_scratch, ctx->debug, NULL);
    ctx->completed += accounted;
    if (accounted && ctx->worker->l1)
        ctx->worker->remote_waves[ctx->channel_index].complete_one();
}

static void account_batch_vector_completion(
    void *priv, uint64_t caller_cookie, const vemb_v16_resp_t *response,
    const vemb_v16_aeron_batch_vector_view_t *vector_view) {
    batch_completion_ctx *ctx = (batch_completion_ctx *)priv;
    vemb_v16_resp_t logical_response = *response;
    logical_response.req_id = (uint32_t)caller_cookie;
    int accounted = account_response(ctx->worker, ctx->channel_index,
                                     ctx->pipeline, &logical_response, 0,
                                     ctx->vec_scratch, ctx->debug,
                                     vector_view);
    ctx->completed += accounted;
    if (accounted && ctx->worker->l1)
        ctx->worker->remote_waves[ctx->channel_index].complete_one();
}

static void fill_l1_from_materialized_group(
    void *priv, const char *final_key, uint16_t key_len,
    const vemb_v16_resp_t *response,
    const vemb_v16_aeron_batch_vector_view_t *vector_view) {
    worker_arg *w = (worker_arg *)priv;
    assert(response->status == VEMB_V16_STATUS_OK);
    assert(vector_view->valid);
    (void)vemb_v16_cli_l1_put(w->l1, final_key, key_len,
                               vemb_v16_xxh3_64_str(final_key, key_len),
                               vector_view->data,
                               (uint16_t)vector_view->bytes);
}

static int account_local_completions(worker_arg *w, uint32_t pipeline,
                                     aeron_debug_counters *debug) {
    int completed = 0;
    while (!w->local_completions.empty()) {
        vemb_v16_aeron_local_completion completion = w->local_completions.pop();
        vemb_v16_resp_t response = {};
        response.req_id = (uint32_t)completion.caller_cookie;
        response.status = VEMB_V16_STATUS_OK;
        vemb_v16_aeron_batch_vector_view_t vector_view = {
            .data = completion.vector,
            .bytes = completion.vector_bytes,
            .attempted = 1,
            .valid = 1,
        };
        int accounted = account_response(w, completion.channel_index, pipeline,
                                         &response, 0, NULL, debug, &vector_view);
        assert(accounted == 1);
        completed += accounted;
        w->local_completion_count++;
        w->l1_ub_read_bytes_saved += completion.vector_bytes;
        assert(vemb_v16_cli_l1_release(w->l1, &completion.ref) == 0);
    }
    return completed;
}

static void *worker_main(void *arg) {
    worker_arg *w = (worker_arg *)arg;
    uint32_t n_ch = w->batch_sessions_enabled ?
        (uint32_t)w->batch_clients.size() : (uint32_t)w->channels.size();
    uint32_t pipeline = w->cfg->pipeline > 0 ? w->cfg->pipeline : 1;

    /* struct timeval timezone-aware for run_stats::update_*_op. */
    struct timeval start_tv;
    gettimeofday(&start_tv, NULL);
    w->stats->set_start_time(&start_tv);

    unsigned long long op_idx = 0;
    unsigned long long budget = w->budget;  /* 0 = unlimited (--test-time) */
    uint32_t next_ch = 0;
    vemb_v16_req_t req_batch[AERON_BATCH_SIZE];
    uint8_t req_wire[AERON_BATCH_SIZE][VEMB_V16_AERON_REQ_WIRE_MAX_LEN];
    const void *req_ptrs[AERON_BATCH_SIZE];
    uint32_t req_lens[AERON_BATCH_SIZE];
    vemb_v16_resp_t resp_batch[AERON_BATCH_SIZE];
    uint32_t resp_wire_lens[AERON_BATCH_SIZE];
    /* Scratch buffer for VEMB_HANDLE dereference — large enough for any
     * dim up to VEMB_V16_MAX_DIM. Lives on the worker stack. */
    float vec_scratch[VEMB_V16_MAX_DIM];
    uint64_t debug_publish_ok = 0, debug_publish_fail = 0;
    uint64_t debug_poll_zero = 0, debug_poll_got = 0;
    uint64_t debug_loop_count = 0;
    uint64_t debug_status_ok = 0, debug_status_notfound = 0, debug_status_err = 0, debug_status_other = 0;
    uint64_t debug_handle_deref_ok = 0, debug_handle_deref_fail = 0;
    uint64_t debug_unmatched_responses = 0;
    aeron_debug_counters debug = {
        &debug_status_ok, &debug_status_notfound, &debug_status_err,
        &debug_status_other, &debug_handle_deref_ok, &debug_handle_deref_fail,
        &debug_unmatched_responses
    };

    while (!w->stop->load(std::memory_order_acquire)) {
        if (budget > 0 && op_idx >= budget) {
            /* drain then exit */
            break;
        }
        debug_loop_count++;

        /* ---- publish phase: round-robin channels, fill each to pipeline in
         * one variable-length batch. ---- */
        int published_any = 0;
        for (uint32_t k = 0; k < n_ch; k++) {
            uint32_t ch = (next_ch + k) % n_ch;
            if (w->l1 && w->remote_waves[ch].active())
                continue;
            uint32_t batch_count = pipeline - w->pending_count[ch];
            if (batch_count > AERON_BATCH_SIZE) batch_count = AERON_BATCH_SIZE;
            if (budget > 0 && op_idx < budget && budget - op_idx < batch_count)
                batch_count = (uint32_t)(budget - op_idx);
            if (batch_count == 0) continue;
            if (w->stop->load(std::memory_order_acquire)) break;

            if (w->batch_sessions_enabled) {
                uint32_t accepted = 0;
                uint32_t remote_submitted = 0;
                struct timeval now;
                gettimeofday(&now, NULL);
                for (uint32_t i = 0; i < batch_count; i++) {
                    unsigned long long key_index = w->obj_gen->get_key_index(
                        obj_iter_type(w->cfg, AERON_GET_CMD_IDX));
                    w->obj_gen->generate_key(key_index);
                    uint32_t actual_key_len = 0;
                    (void)build_req(w, op_idx + accepted,
                                    VEMB_V16_OP_VEMB_HANDLE,
                                    w->obj_gen->get_key(),
                                    w->obj_gen->get_key_len(), &req_batch[i],
                                    &actual_key_len);
                    if (w->l1) {
                        vemb_v16_cli_l1_value_t value;
                        int l1_hit = vemb_v16_cli_l1_lookup(
                            w->l1, req_batch[i].key, (uint16_t)actual_key_len,
                            req_batch[i].key_hash, &value);
                        if (l1_hit) {
                            uint32_t slot = w->pending_tail[ch];
                            vemb_v16_aeron_local_completion completion = {
                                .channel_index = ch,
                                .req_id = req_batch[i].req_id,
                                .caller_cookie = req_batch[i].req_id,
                                .sent_time = now,
                                .vector = (const float *)value.vector,
                                .vector_bytes = value.vector_bytes,
                                .ref = value.ref,
                            };
                            if (w->local_completions.push(completion)) {
                                w->pending[ch][slot].req_id = req_batch[i].req_id;
                                w->pending[ch][slot].is_set = 0;
                                w->pending[ch][slot].sent_time = now;
                                w->pending[ch][slot].bytes_tx = 0;
                                w->pending_tail[ch] = (slot + 1) % pipeline;
                                w->pending_count[ch]++;
                                accepted++;
                                debug_publish_ok++;
                                continue;
                            }
                            w->l1_queue_full_fallbacks++;
                            assert(vemb_v16_cli_l1_release(w->l1, &value.ref) == 0);
                        }
                    }
                    int submit_rc = vemb_v16_aeron_batch_client_submit_handle(
                        w->batch_clients[ch], req_batch[i].key,
                        (uint16_t)actual_key_len, req_batch[i].req_id);
                    if (submit_rc != 0)
                        break;
                    uint32_t slot = w->pending_tail[ch];
                    w->pending[ch][slot].req_id = req_batch[i].req_id;
                    w->pending[ch][slot].is_set = 0;
                    w->pending[ch][slot].sent_time = now;
                    w->pending[ch][slot].bytes_tx = 0;
                    w->pending_tail[ch] = (slot + 1) % pipeline;
                    w->pending_count[ch]++;
                    remote_submitted++;
                    accepted++;
                    debug_publish_ok++;
                }
                if (accepted == 0) {
                    debug_publish_fail++;
                    continue;
                }
                if (w->l1 && remote_submitted != 0)
                    w->remote_waves[ch].seal(remote_submitted);
                op_idx += accepted;
                published_any = 1;
                next_ch = (ch + 1) % n_ch;
                continue;
            }

            for (uint32_t i = 0; i < batch_count; i++) {
                uint8_t op = pick_next_op(w);
                int iter = is_set_op(op)
                    ? obj_iter_type(w->cfg, AERON_SET_CMD_IDX)
                    : obj_iter_type(w->cfg, AERON_GET_CMD_IDX);
                unsigned long long key_index = w->obj_gen->get_key_index(iter);
                w->obj_gen->generate_key(key_index);
                uint32_t actual_key_len = 0;
                (void)build_req(w, op_idx + i, op,
                                 w->obj_gen->get_key(),
                                 w->obj_gen->get_key_len(),
                                 &req_batch[i], &actual_key_len);
                req_batch[i].channel_id =
                    vemb_v16_aeron_channel_id(w->channels[ch]);
                size_t req_len = 0;
                if (vemb_v16_req_encode(req_wire[i], sizeof(req_wire[i]),
                                        &req_batch[i], &req_len) != 0) {
                    req_lens[i] = 0;
                    continue;
                }
                req_ptrs[i] = req_wire[i];
                req_lens[i] = (uint32_t)req_len;
            }

            int pub_rc = vemb_v16_aeron_publish_request_batch(
                w->channels[ch], req_ptrs, req_lens, batch_count);
            if (pub_rc != 0) {
                debug_publish_fail++;
                continue;
            }
            struct timeval now;
            gettimeofday(&now, NULL);
            for (uint32_t i = 0; i < batch_count; i++) {
                uint32_t slot = w->pending_tail[ch];
                w->pending[ch][slot].req_id = req_batch[i].req_id;
                w->pending[ch][slot].is_set = is_set_op(req_batch[i].op);
                w->pending[ch][slot].sent_time = now;
                w->pending[ch][slot].bytes_tx = req_lens[i];
                w->pending_tail[ch] = (slot + 1) % pipeline;
                w->pending_count[ch]++;
                debug_publish_ok++;
            }
            op_idx += batch_count;
            published_any = 1;
            next_ch = (ch + 1) % n_ch;
        }

        /* ---- poll phase: copy a response batch from each channel. ---- */
        int polled_any = account_local_completions(w, pipeline, &debug) > 0;
        for (uint32_t ch = 0; ch < n_ch; ch++) {
            if (w->pending_count[ch] == 0) continue;
            if (w->batch_sessions_enabled) {
                batch_completion_ctx ctx = {
                    .worker = w, .channel_index = ch, .pipeline = pipeline,
                    .vec_scratch = vec_scratch, .debug = &debug,
                };
                int got = w->skip_handle_read ?
                    vemb_v16_aeron_batch_client_poll(
                        w->batch_clients[ch], account_batch_completion, &ctx) :
                    (w->l1 ?
                     vemb_v16_aeron_batch_client_poll_shared_vector_with_materialization_hook(
                         w->batch_clients[ch], fill_l1_from_materialized_group,
                         w, account_batch_vector_completion, &ctx) :
                     vemb_v16_aeron_batch_client_poll_shared_vector(
                         w->batch_clients[ch], account_batch_vector_completion,
                         &ctx));
                if (got == 0) {
                    debug_poll_zero++;
                    continue;
                }
                if (got > 0) {
                    debug_poll_got += (uint64_t)got;
                    polled_any |= ctx.completed > 0;
                }
                continue;
            }
            uint32_t got = vemb_v16_aeron_poll_response_batch_ex(
                w->channels[ch], resp_batch, resp_wire_lens,
                sizeof(resp_batch[0]), AERON_BATCH_SIZE);
            if (got == 0) {
                debug_poll_zero++;
                continue;
            }
            debug_poll_got += got;
            for (uint32_t i = 0; i < got; i++)
                polled_any |= account_response(w, ch, pipeline,
                                               &resp_batch[i], resp_wire_lens[i],
                                               vec_scratch,
                                               &debug, NULL);
        }

        /* if nothing published and nothing polled, busy-spin with a CPU yield
         * hint. Do NOT use nanosleep — even 200ns requests round up to ~1-10ms
         * kernel tick latency on Linux, which becomes the bottleneck when
         * the server-side proxy goes to sleep between batches. */
        if (!published_any && !polled_any) {
#if defined(__x86_64__) || defined(__i386__)
            __asm__ volatile("pause" ::: "memory");
#elif defined(__aarch64__) || defined(__arm__)
            __asm__ volatile("yield" ::: "memory");
#else
            __asm__ volatile("" ::: "memory");
#endif
        }
    }

    /* ---- drain phase: up to ~1s to collect outstanding responses ---- */
    {
        uint64_t drain_start = now_ns();
        while (1) {
            int any_pending = 0;
            for (uint32_t ch = 0; ch < n_ch; ch++) {
                if (w->pending_count[ch] > 0) { any_pending = 1; break; }
            }
            if (!any_pending) break;
            if (now_ns() - drain_start > 1000000000ull) break;

            (void)account_local_completions(w, pipeline, &debug);

            for (uint32_t ch = 0; ch < n_ch; ch++) {
                if (w->pending_count[ch] == 0) continue;
                if (w->batch_sessions_enabled) {
                    batch_completion_ctx ctx = {
                        .worker = w, .channel_index = ch, .pipeline = pipeline,
                        .vec_scratch = vec_scratch, .debug = &debug,
                    };
                    int got = w->skip_handle_read ?
                        vemb_v16_aeron_batch_client_poll(
                            w->batch_clients[ch], account_batch_completion,
                            &ctx) :
                        (w->l1 ?
                         vemb_v16_aeron_batch_client_poll_shared_vector_with_materialization_hook(
                             w->batch_clients[ch],
                             fill_l1_from_materialized_group, w,
                             account_batch_vector_completion, &ctx) :
                         vemb_v16_aeron_batch_client_poll_shared_vector(
                             w->batch_clients[ch],
                             account_batch_vector_completion, &ctx));
                    if (got > 0)
                        debug_poll_got += (uint64_t)got;
                    continue;
                }
                uint32_t got = vemb_v16_aeron_poll_response_batch_ex(
                    w->channels[ch], resp_batch, resp_wire_lens,
                    sizeof(resp_batch[0]), AERON_BATCH_SIZE);
                if (got == 0) {
                    debug_poll_zero++;
                    continue;
                }
                debug_poll_got += got;
                for (uint32_t i = 0; i < got; i++)
                    account_response(w, ch, pipeline, &resp_batch[i],
                                     resp_wire_lens[i], vec_scratch, &debug,
                                     NULL);
            }
            struct timespec ts = {0, 500};
            nanosleep(&ts, NULL);
        }
    }

    struct timeval end_tv;
    gettimeofday(&end_tv, NULL);
    w->stats->set_end_time(&end_tv);
    if (w->batch_sessions_enabled) {
        uint64_t leaders = 0, followers = 0, frames = 0, items = 0;
        uint64_t fallback_v1 = 0, frame_bytes = 0;
        uint64_t flush_eager = 0, flush_full = 0, flush_deadline = 0;
        uint64_t flush_backpressure = 0;
        uint64_t v2_stale_epochs = 0;
        uint64_t v2_stale_responses = 0;
        uint64_t vector_reads = 0, vector_read_failures = 0;
        uint64_t vector_bytes = 0, vector_fanout = 0;
        for (size_t i = 0; i < w->batch_clients.size(); i++) {
            vemb_v16_aeron_batch_client_stats_t stats;
            vemb_v16_aeron_batch_client_get_stats(w->batch_clients[i], &stats);
            leaders += stats.l0_new_leader_groups;
            followers += stats.l0_coalesced_followers;
            frames += stats.batch_frames;
            items += stats.batch_items;
            fallback_v1 += stats.l0_fallback_v1;
            frame_bytes += stats.batch_frame_bytes;
            flush_eager += stats.batch_flush_eager;
            flush_full += stats.batch_flush_full;
            flush_deadline += stats.batch_flush_deadline;
            flush_backpressure += stats.batch_flush_backpressure;
            v2_stale_epochs += stats.v2_stale_epochs;
            v2_stale_responses += stats.v2_stale_responses;
            vector_reads += stats.shared_vector_group_reads;
            vector_read_failures += stats.shared_vector_group_read_failures;
            vector_bytes += stats.shared_vector_group_bytes;
            vector_fanout += stats.shared_vector_fanout;
        }
        fprintf(stderr,
                "[aeron] w%u batch-session: leaders=%llu followers=%llu "
                "frames=%llu unique_items=%llu frame_bytes=%llu fallback_v1=%llu "
                "flush_eager=%llu flush_full=%llu flush_deadline=%llu "
                "flush_backpressure=%llu shared_vector_reads=%llu "
                "shared_vector_read_failures=%llu shared_vector_bytes=%llu "
                "shared_vector_fanout=%llu v2_stale_epochs=%llu "
                "v2_stale_responses=%llu\n",
                w->worker_id, (unsigned long long)leaders,
                (unsigned long long)followers, (unsigned long long)frames,
                (unsigned long long)items, (unsigned long long)frame_bytes,
                (unsigned long long)fallback_v1,
                (unsigned long long)flush_eager,
                (unsigned long long)flush_full,
                (unsigned long long)flush_deadline,
                (unsigned long long)flush_backpressure,
                (unsigned long long)vector_reads,
                (unsigned long long)vector_read_failures,
                (unsigned long long)vector_bytes,
                (unsigned long long)vector_fanout,
                (unsigned long long)v2_stale_epochs,
                (unsigned long long)v2_stale_responses);
    }
    fprintf(stderr, "[aeron] w%u done: loops=%llu publish_ok=%llu publish_fail=%llu poll_zero=%llu poll_got=%llu ops_done=%llu unmatched=%llu status[ok=%llu nf=%llu err=%llu other=%llu] handle_deref[ok=%llu fail=%llu]\n",
            w->worker_id,
            (unsigned long long)debug_loop_count,
            (unsigned long long)debug_publish_ok,
            (unsigned long long)debug_publish_fail,
            (unsigned long long)debug_poll_zero,
            (unsigned long long)debug_poll_got,
            (unsigned long long)w->ops_done.load(),
            (unsigned long long)debug_unmatched_responses,
            (unsigned long long)debug_status_ok,
            (unsigned long long)debug_status_notfound,
            (unsigned long long)debug_status_err,
            (unsigned long long)debug_status_other,
            (unsigned long long)debug_handle_deref_ok,
            (unsigned long long)debug_handle_deref_fail);
    w->done.store(true, std::memory_order_release);
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Public entry                                                       */
/* ------------------------------------------------------------------ */

run_stats vemb_v16_aeron_run(benchmark_config* cfg, object_generator* obj_gen) {
    const char *skip_handle_read_env = getenv("VEMB_AERON_SKIP_HANDLE_READ");
    bool skip_handle_read = skip_handle_read_env &&
        (!strcmp(skip_handle_read_env, "1") ||
         !strcmp(skip_handle_read_env, "yes") ||
         !strcmp(skip_handle_read_env, "true"));
    fprintf(stderr, "[aeron] side-channel runner start (t=%u c=%u pipeline=%u dim=%u)\n",
            cfg->threads, cfg->clients, cfg->pipeline, cfg->vemb_v16_dim);
    fprintf(stderr, "[aeron] handle read: %s\n",
            skip_handle_read ? "skipped (metadata only)" : "enabled");

    /* Validate config */
    if (cfg->vemb_v16_dim == 0) {
        benchmark_error_log("[aeron] --vemb-v16-dim required\n");
        exit(1);
    }
    if (cfg->vemb_v16_endpoints && g_aeron_transport_mode != "aeron-cross-node") {
        benchmark_error_log("[aeron] --vemb-v16-endpoints not supported in aeron mode (use --vemb-v16-transport=aeron-cross-node)\n");
        exit(1);
    }
    uint32_t total_channels = cfg->threads * cfg->clients;
    if (total_channels == 0) {
        benchmark_error_log("[aeron] threads*clients must be > 0\n");
        exit(1);
    }
    if (total_channels > VEMB_V16_MAX_CHANNELS) {
        benchmark_error_log("[aeron] t*c=%u exceeds VEMB_V16_MAX_CHANNELS=%u\n",
                            total_channels, VEMB_V16_MAX_CHANNELS);
        exit(1);
    }

    char control_endpoint[320];
    const char *control_path = NULL;
    if (cfg->server && cfg->server[0] && cfg->port != 0) {
        snprintf(control_endpoint,
                 sizeof(control_endpoint),
                 "tcp://%s:%u",
                 cfg->server,
                 (unsigned)cfg->port);
        control_path = control_endpoint;
    } else {
        snprintf(control_endpoint, sizeof(control_endpoint),
                 "tcp://%s:%u", VEMB_V16_TCP_HOST,
                 (unsigned)VEMB_V16_TCP_PORT);
        control_path = control_endpoint;
    }

    /* Best-effort cleanup of any stale channels from a previous crashed run.
     * All Aeron modes use TCP control; ring mappings come from the server's
     * advertised UB path and offset. */
    bool cross_node = (g_aeron_transport_mode == "aeron-cross-node");
    std::string cn_host;
    uint16_t    cn_port = 0;
    if (cross_node) {
        auto colon = g_aeron_remote_endpoint.find(':');
        if (colon == std::string::npos) {
            benchmark_error_log("[aeron] cross-node endpoint must be HOST:PORT, got '%s'\n",
                                g_aeron_remote_endpoint.c_str());
            exit(1);
        }
        cn_host = g_aeron_remote_endpoint.substr(0, colon);
        cn_port = (uint16_t)atoi(g_aeron_remote_endpoint.c_str() + colon + 1);
        if (cn_host.empty() || cn_port == 0) {
            benchmark_error_log("[aeron] cross-node endpoint parse failed\n");
            exit(1);
        }
        fprintf(stderr, "[aeron] cross-node mode: %s:%u\n", cn_host.c_str(), cn_port);
    } else {
        vemb_v16_aeron_close_all(control_path);
        fprintf(stderr, "[aeron] local mode: control=%s\n", control_path);
    }

    vemb_v16_aeron_runner_channel_plan channel_plan =
        vemb_v16_aeron_runner_plan_channels(
            cross_node, cfg->ratio.a, cfg->ratio.b, cfg->vemb_v16_vsim,
            cfg->vemb_v16_vrem, cfg->vemb_v16_batch_disable, total_channels);
    bool batch_sessions_enabled = channel_plan.batch_sessions_enabled;
    if (cfg->vemb_v16_l1_entries != 0 && !batch_sessions_enabled) {
        benchmark_error_log("[aeron] --vemb-v16-l1-entries requires pure cross-node VEMB_HANDLE batch sessions\n");
        exit(1);
    }
    if (cfg->vemb_v16_l1_entries != 0 &&
        !l1_entry_count_valid(cfg->vemb_v16_l1_entries)) {
        benchmark_error_log("[aeron] --vemb-v16-l1-entries must be 4 times a power of two\n");
        exit(1);
    }
    std::vector<vemb_v16_aeron_channel_t *> all_channels;
    std::vector<vemb_v16_aeron_batch_client_t *> all_batch_clients(
        channel_plan.batch_sessions, nullptr);

    /* A batch session owns its permanent v1 fallback and v2 channel. Do not
     * attach an unused runner v1 channel before creating it. */
    if (!batch_sessions_enabled) {
        fprintf(stderr, "[aeron] allocating %u channels%s\n",
                total_channels, cross_node ? " (cross-node)" : "");
        all_channels.assign(channel_plan.legacy_channels, nullptr);
        for (uint32_t i = 0; i < channel_plan.legacy_channels; i++) {
            if (cross_node) {
                all_channels[i] = vemb_v16_aeron_open_remote(
                    cn_host.c_str(), cn_port, cfg->vemb_v16_dim);
            } else {
                all_channels[i] = vemb_v16_aeron_open(
                    control_path, cfg->vemb_v16_dim);
            }
            if (!all_channels[i]) {
                benchmark_error_log(
                    "[aeron] open channel %u failed: %s\n", i,
                    errno ? strerror(errno)
                          : "rejected by server (check server log for ATTACH/pool errors)");
                for (uint32_t j = 0; j < i; j++) {
                    vemb_v16_aeron_close(all_channels[j]);
                    all_channels[j] = nullptr;
                }
                exit(1);
            }
            /* Map the warm region so worker_main can dereference VEMB_HANDLE
             * offsets and read the actual vector bytes. Cross-node ATTACH
             * returns server-side UB paths, mapped by the SDK here. */
            if (vemb_v16_aeron_open_warm_region(all_channels[i]) != 0) {
                benchmark_error_log("[aeron] open warm region %u failed: %s\n",
                                    i, strerror(errno));
                for (uint32_t j = 0; j <= i; j++) {
                    vemb_v16_aeron_close(all_channels[j]);
                    all_channels[j] = nullptr;
                }
                exit(1);
            }
        }
        fprintf(stderr, "[aeron] all %u channels ready%s\n",
                channel_plan.legacy_channels,
                cross_node ? " (cross-node, no warm region)" : " (warm region mapped)");
    }

    if (batch_sessions_enabled) {
        vemb_v16_aeron_batch_client_options_t batch_options = {
            .requested_batch_size = cfg->vemb_v16_batch_request_size,
            .max_batch_delay_us = cfg->vemb_v16_batch_max_delay_us,
        };
        for (uint32_t i = 0; i < channel_plan.batch_sessions; i++) {
            all_batch_clients[i] = vemb_v16_aeron_batch_client_open_remote(
                cn_host.c_str(), cn_port, cfg->vemb_v16_dim, &batch_options);
            if (!all_batch_clients[i]) {
                benchmark_error_log("[aeron] open batch session %u failed: %s\n",
                                    i, strerror(errno));
                for (uint32_t j = 0; j < i; j++)
                    vemb_v16_aeron_batch_client_close(all_batch_clients[j], NULL, NULL);
                exit(1);
            }
        }
        fprintf(stderr,
                "[aeron] batch sessions ready (channels=%u v1+v2 requested-size=%u max-delay-us=%u)\n",
                channel_plan.batch_sessions,
                cfg->vemb_v16_batch_request_size,
                cfg->vemb_v16_batch_max_delay_us);
    }

    /* Setup workers */
    std::vector<worker_arg> workers(cfg->threads);
    std::vector<pthread_t>  tids(cfg->threads);
    std::atomic<bool> stop(false);

    /* Budget: if --requests (-n) is set, divide across workers.
     * If only --test-time, budget=0 (unlimited, stop flag ends loop). */
    unsigned long long per_worker_budget = 0;
    if (cfg->requests > 0) {
        per_worker_budget = cfg->requests / cfg->threads;
        if (per_worker_budget == 0) per_worker_budget = 1;
    }

    uint32_t pipeline = cfg->pipeline > 0 ? cfg->pipeline : 1;
    uint64_t local_completion_capacity = (uint64_t)cfg->clients * pipeline;
    if (cfg->vemb_v16_l1_entries != 0 &&
        local_completion_capacity > UINT32_MAX) {
        benchmark_error_log("[aeron] clients*pipeline exceeds local completion queue capacity\n");
        exit(1);
    }

    for (unsigned int i = 0; i < cfg->threads; i++) {
        workers[i].stats = new run_stats(cfg);
        workers[i].cfg = cfg;
        workers[i].obj_gen = obj_gen->clone();
        /* Give each worker a distinct random seed so that RANDOM/ZIPFIAN
         * key patterns don't collide across workers (mirrors client.cpp:73
         * distinct_client_seed path). */
        workers[i].obj_gen->set_random_seed((int)i + 1);
        workers[i].worker_id = i;
        workers[i].skip_handle_read = skip_handle_read;
        workers[i].batch_sessions_enabled = batch_sessions_enabled;
        workers[i].l1 = NULL;
        workers[i].local_completion_count = 0;
        workers[i].l1_ub_read_bytes_saved = 0;
        workers[i].l1_queue_full_fallbacks = 0;
        workers[i].stop = &stop;
        workers[i].done.store(false, std::memory_order_relaxed);
        workers[i].ops_done.store(0, std::memory_order_relaxed);
        workers[i].ops_fail.store(0, std::memory_order_relaxed);
        workers[i].set_ratio_count = 0;
        workers[i].get_ratio_count = 0;
        workers[i].budget = per_worker_budget;

        /* Batch sessions own their channels; legacy runner channels are only
         * assigned to paths which use the legacy publish/poll loop. */
        if (!batch_sessions_enabled)
            workers[i].channels.reserve(cfg->clients);
        if (batch_sessions_enabled)
            workers[i].batch_clients.reserve(cfg->clients);
        for (unsigned int j = 0; j < cfg->clients; j++) {
            uint32_t idx = i * cfg->clients + j;
            if (batch_sessions_enabled)
                workers[i].batch_clients.push_back(all_batch_clients[idx]);
            else
                workers[i].channels.push_back(all_channels[idx]);
        }
        workers[i].pending.resize(cfg->clients);
        workers[i].pending_head.assign(cfg->clients, 0);
        workers[i].pending_tail.assign(cfg->clients, 0);
        workers[i].pending_count.assign(cfg->clients, 0);
        workers[i].remote_waves.resize(cfg->clients);
        for (unsigned int j = 0; j < cfg->clients; j++) {
            workers[i].pending[j].assign(pipeline, pending_op());
        }
        if (cfg->vemb_v16_l1_entries != 0) {
            vemb_v16_cli_l1_config_t l1_config = {
                cfg->vemb_v16_dim,
                cfg->vemb_v16_l1_entries,
                0,
                0,
            };
            workers[i].l1 = vemb_v16_cli_l1_create(&l1_config);
            if (!workers[i].l1) {
                benchmark_error_log("[aeron] create worker %u L1 failed\n", i);
                exit(1);
            }
            workers[i].local_completions.init(
                (uint32_t)local_completion_capacity);
        }
    }

    /* Launch */
    fprintf(stderr, "[aeron] launching %u worker threads\n", cfg->threads);
    for (unsigned int i = 0; i < cfg->threads; i++) {
        int rc = pthread_create(&tids[i], NULL, worker_main, &workers[i]);
        if (rc != 0) {
            benchmark_error_log("[aeron] pthread_create %u failed: %s\n",
                                i, strerror(rc));
            exit(1);
        }
    }

    /* Test-time stop arm */
    if (cfg->test_time > 0) {
        sleep(cfg->test_time);
        stop.store(true, std::memory_order_release);
    }

    /* Wait for all workers to report done */
    for (unsigned int i = 0; i < cfg->threads; i++) {
        pthread_join(tids[i], NULL);
    }
    fprintf(stderr, "[aeron] all workers joined\n");

    aeron_l1_stats_total l1_total = {};
    uint64_t local_completion_count = 0;
    uint64_t l1_ub_read_bytes_saved = 0;
    uint64_t l1_queue_full_fallbacks = 0;
    uint64_t server_items = 0;
    uint64_t ub_read_bytes = 0;
    for (size_t i = 0; i < all_batch_clients.size(); i++) {
        if (!all_batch_clients[i])
            continue;
        vemb_v16_aeron_batch_client_stats_t batch_stats;
        vemb_v16_aeron_batch_client_get_stats(all_batch_clients[i],
                                               &batch_stats);
        server_items += batch_stats.batch_items + batch_stats.l0_fallback_v1 +
            batch_stats.v1_direct_requests;
        ub_read_bytes += batch_stats.shared_vector_group_bytes;
    }
    for (unsigned int i = 0; i < cfg->threads; i++) {
        if (workers[i].l1) {
            vemb_v16_cli_l1_stats_t l1_worker;
            vemb_v16_cli_l1_get_stats(workers[i].l1, &l1_worker);
            l1_stats_add(&l1_total, &l1_worker);
            vemb_v16_cli_l1_destroy(workers[i].l1);
            workers[i].l1 = NULL;
        }
        local_completion_count += workers[i].local_completion_count;
        l1_ub_read_bytes_saved += workers[i].l1_ub_read_bytes_saved;
        l1_queue_full_fallbacks += workers[i].l1_queue_full_fallbacks;
    }
    uint64_t l1_lookups = l1_total.hits + l1_total.misses;
    double l1_hit_ratio = l1_lookups ?
        (double)l1_total.hits / (double)l1_lookups : 0.0;
    uint64_t l1_vector_capacity_bytes = (uint64_t)cfg->threads *
        cfg->vemb_v16_l1_entries * cfg->vemb_v16_dim * sizeof(float);
    fprintf(stderr,
            "[aeron] l1-summary: enabled=%u entries_per_worker=%u "
            "l1_hit=%llu l1_miss=%llu l1_insert=%llu l1_evict=%llu "
            "l1_hit_ratio=%.6f l1_vector_bytes=%llu "
            "l1_vector_capacity_bytes=%llu l1_ub_read_bytes_saved=%llu "
            "local_completion_count=%llu l1_queue_full_fallbacks=%llu "
            "server_items=%llu ub_read_bytes=%llu "
            "l1_live_entries=%llu l1_all_pinned=%llu "
            "l1_key_storage_exhausted=%llu "
            "l1_vector_storage_exhausted=%llu l1_stale_ref=%llu\n",
            cfg->vemb_v16_l1_entries != 0, cfg->vemb_v16_l1_entries,
            (unsigned long long)l1_total.hits,
            (unsigned long long)l1_total.misses,
            (unsigned long long)l1_total.inserts,
            (unsigned long long)l1_total.evicts, l1_hit_ratio,
            (unsigned long long)l1_total.live_vector_bytes,
            (unsigned long long)l1_vector_capacity_bytes,
            (unsigned long long)l1_ub_read_bytes_saved,
            (unsigned long long)local_completion_count,
            (unsigned long long)l1_queue_full_fallbacks,
            (unsigned long long)server_items,
            (unsigned long long)ub_read_bytes,
            (unsigned long long)l1_total.live_entries,
            (unsigned long long)l1_total.all_pinned,
            (unsigned long long)l1_total.key_storage_exhausted,
            (unsigned long long)l1_total.vector_storage_exhausted,
            (unsigned long long)l1_total.stale_ref);

    /* Merge per-worker stats into one run_stats — reuse run_stats::merge
     * with iteration counter (matches client_group::merge_run_stats). */
    run_stats merged(cfg);
    int iteration = 0;
    for (unsigned int i = 0; i < cfg->threads; i++) {
        merged.merge(*workers[i].stats, iteration++);
    }

    /* Teardown channels — SDK handles UB ring unmap + TCP close notify. */
    for (uint32_t i = 0; i < channel_plan.batch_sessions; i++) {
        if (all_batch_clients[i]) {
            vemb_v16_aeron_batch_client_close(all_batch_clients[i], NULL, NULL);
            all_batch_clients[i] = nullptr;
        }
    }
    for (vemb_v16_aeron_channel_t *channel : all_channels)
        vemb_v16_aeron_close(channel);

    /* Destroy cloned obj_gens + per-worker stats */
    for (unsigned int i = 0; i < cfg->threads; i++) {
        delete workers[i].obj_gen;
        delete workers[i].stats;
    }

    return merged;
}
