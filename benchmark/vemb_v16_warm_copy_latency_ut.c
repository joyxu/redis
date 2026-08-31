/*
 * Isolate VEMB_HANDLE lookup and warm-vector copy latency.
 *
 * This is an integration UT: it expects a running Aeron/UB server and a
 * client peer-view manifest. It writes two equal-sized key ranges first, then
 * reads old -> steady -> old -> steady while timing handle lookup and the
 * subsequent read_vector() separately.
 */

#define _GNU_SOURCE

#include "../clients/c/vemb_v16_client_sdk.h"
#include "../src/monotonic.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <string.h>

typedef struct {
    const char *endpoint;
    const char *manifest;
    const char *client_host;
    uint32_t dim;
    uint32_t count;
    uint32_t old_start;
    uint32_t steady_start;
    uint32_t warmup;
    uint32_t iterations;
    uint32_t threads;
    uint32_t clients;
    uint32_t pipeline;
    uint32_t timeout_ms;
} warm_copy_config_t;

typedef struct {
    uint64_t *lookup_ns;
    uint64_t *copy_ns;
    uint64_t *total_ns;
    uint32_t count;
} warm_copy_samples_t;

typedef struct {
    const warm_copy_config_t *cfg;
    uint32_t start;
    uint32_t sample_offset;
    warm_copy_samples_t *samples;
    atomic_int *failed;
    vemb_v16_client_t *client;
    vemb_v16_client_handle_session_t *session;
    float *vector;
    uint64_t pending_start[256];
    uint32_t next_submit;
    uint32_t callback_count;
    uint32_t completed;
} warm_copy_thread_arg_t;

static uint64_t now_ns(void)
{
    return vemb_v16_monotonic_ns();
}

static int compare_u64(const void *lhs, const void *rhs)
{
    const uint64_t a = *(const uint64_t *)lhs;
    const uint64_t b = *(const uint64_t *)rhs;
    return a > b ? 1 : a < b ? -1 : 0;
}

static uint64_t percentile(const uint64_t *values, uint32_t count,
                           unsigned percentile_value)
{
    uint64_t rank = ((uint64_t)count * percentile_value + 99) / 100;
    if (rank == 0)
        rank = 1;
    return values[rank - 1];
}

static void fill_vector(float *vector, uint32_t dim, uint32_t key_id)
{
    for (uint32_t i = 0; i < dim; i++)
        vector[i] = (float)(key_id + i) * 0.001f;
}

static int make_key(char *key, size_t capacity, uint32_t key_id)
{
    int written = snprintf(key, capacity, "item:%u", key_id);
    return written > 0 && (size_t)written < capacity ? 0 : -1;
}

static int write_range(vemb_v16_client_t *client, const warm_copy_config_t *cfg,
                       uint32_t start, float *vector)
{
    char key[VEMB_V16_MAX_KEY_LEN];
    for (uint32_t i = 0; i < cfg->count; i++) {
        uint32_t key_id = start + i;
        if (make_key(key, sizeof(key), key_id) != 0)
            return -1;
        fill_vector(vector, cfg->dim, key_id);
        if (vemb_v16_client_vadd(client, NULL, key, vector, cfg->dim) != 0) {
            fprintf(stderr, "write failed key=%s\n", key);
            return -1;
        }
    }
    return 0;
}

static void async_completion(void *opaque, uint64_t cookie,
                             const vemb_v16_pipeline_resp_t *response)
{
    warm_copy_thread_arg_t *arg = opaque;
    uint32_t slot = (uint32_t)(cookie % 256u);
    uint64_t copy_start = now_ns();
    uint64_t offset = response->offset;
    int rc = response->status == 0 ?
        vemb_v16_client_read_vector(arg->client, offset, response->bytes,
                                    arg->vector, arg->cfg->dim) : -1;
    uint64_t end = now_ns();
    if (rc != 0 || response->status != 0) {
        atomic_store_explicit(arg->failed, 1, memory_order_release);
        return;
    }
    arg->callback_count++;
    if (cookie < arg->cfg->warmup)
        return;
    uint32_t sample = arg->sample_offset + arg->completed++;
    arg->samples->lookup_ns[sample] = copy_start - arg->pending_start[slot];
    arg->samples->copy_ns[sample] = end - copy_start;
    arg->samples->total_ns[sample] = end - arg->pending_start[slot];
}

static void *read_thread_main(void *opaque)
{
    warm_copy_thread_arg_t *arg = opaque;
    const warm_copy_config_t *cfg = arg->cfg;
    const char *seeds[] = {cfg->endpoint};
    vemb_v16_client_t *client = vemb_v16_client_create(
        seeds, 1, cfg->dim, cfg->timeout_ms, VEMB_V16_TRANSPORT_AERON);
    float *vector = calloc(cfg->dim, sizeof(*vector));
    if (!client || !vector ||
        vemb_v16_client_configure_ub_peer_view(
            client, cfg->manifest, cfg->client_host) != 0 ||
        vemb_v16_client_topology_refresh(client) != 0) {
        atomic_store_explicit(arg->failed, 1, memory_order_release);
        free(vector);
        vemb_v16_client_destroy(client);
        return NULL;
    }

    vemb_v16_client_handle_session_options_t options = {
        .max_batch_delay_us = 10,
    };
    arg->client = client;
    arg->vector = vector;
    arg->session = vemb_v16_client_handle_session_create(client, &options);
    if (!arg->session)
        goto failed;
    uint32_t target = cfg->iterations;
    while (arg->completed < target) {
        while (arg->next_submit < target + cfg->warmup &&
               arg->next_submit - arg->callback_count < cfg->pipeline) {
            uint32_t logical = arg->next_submit++;
            uint32_t key_index = logical < cfg->warmup ?
                (logical % cfg->count) :
                ((logical - cfg->warmup) % cfg->count);
            char key[VEMB_V16_MAX_KEY_LEN];
            if (make_key(key, sizeof(key), arg->start + key_index) != 0)
                goto failed_session;
            uint64_t cookie = logical;
            arg->pending_start[cookie % 256u] = now_ns();
            if (vemb_v16_client_handle_session_submit(
                    arg->session, NULL, key, cookie) != 0)
                goto failed_session;
        }
        int callbacks = vemb_v16_client_handle_session_poll(
            arg->session, async_completion, arg);
        if (callbacks < 0 || atomic_load_explicit(arg->failed,
                                                  memory_order_acquire))
            goto failed_session;
    }
    vemb_v16_client_handle_session_close(arg->session, NULL, NULL);
    arg->session = NULL;
    free(vector);
    vemb_v16_client_destroy(client);
    return NULL;

failed_session:
    if (arg->session)
        vemb_v16_client_handle_session_close(arg->session, NULL, NULL);
failed:
    atomic_store_explicit(arg->failed, 1, memory_order_release);
    free(vector);
    vemb_v16_client_destroy(client);
    return NULL;
}

static int read_phase(const warm_copy_config_t *cfg, const char *label,
                      uint32_t start)
{
    uint64_t worker_count_u64 = (uint64_t)cfg->threads * cfg->clients;
    uint64_t total_count_u64 = (uint64_t)cfg->iterations * worker_count_u64;
    if (worker_count_u64 == 0 || worker_count_u64 > UINT32_MAX)
        return -1;
    uint32_t worker_count = (uint32_t)worker_count_u64;
    if (total_count_u64 == 0 || total_count_u64 > UINT32_MAX)
        return -1;
    uint32_t total_count = (uint32_t)total_count_u64;

    warm_copy_samples_t samples = {
        .lookup_ns = calloc(total_count, sizeof(*samples.lookup_ns)),
        .copy_ns = calloc(total_count, sizeof(*samples.copy_ns)),
        .total_ns = calloc(total_count, sizeof(*samples.total_ns)),
        .count = total_count,
    };
    if (!samples.lookup_ns || !samples.copy_ns || !samples.total_ns) {
        free(samples.lookup_ns);
        free(samples.copy_ns);
        free(samples.total_ns);
        return -1;
    }

    pthread_t *threads = calloc(worker_count, sizeof(*threads));
    warm_copy_thread_arg_t *args = calloc(worker_count, sizeof(*args));
    atomic_int failed = 0;
    uint32_t created = 0;
    if (!threads || !args) {
        free(threads);
        free(args);
        free(samples.lookup_ns);
        free(samples.copy_ns);
        free(samples.total_ns);
        return -1;
    }
    for (uint32_t i = 0; i < worker_count; i++) {
        args[i] = (warm_copy_thread_arg_t){
            .cfg = cfg,
            .start = start,
            .sample_offset = i * cfg->iterations,
            .samples = &samples,
            .failed = &failed,
        };
        if (pthread_create(&threads[i], NULL, read_thread_main, &args[i]) != 0)
            break;
        created++;
    }
    for (uint32_t i = 0; i < created; i++)
        pthread_join(threads[i], NULL);
    if (created != worker_count ||
        atomic_load_explicit(&failed, memory_order_acquire)) {
        free(threads);
        free(args);
        free(samples.lookup_ns);
        free(samples.copy_ns);
        free(samples.total_ns);
        return -1;
    }

    uint64_t lookup_sum = 0;
    uint64_t copy_sum = 0;
    uint64_t total_sum = 0;
    for (uint32_t i = 0; i < samples.count; i++) {
        lookup_sum += samples.lookup_ns[i];
        copy_sum += samples.copy_ns[i];
        total_sum += samples.total_ns[i];
    }
    qsort(samples.lookup_ns, samples.count, sizeof(*samples.lookup_ns),
          compare_u64);
    qsort(samples.copy_ns, samples.count, sizeof(*samples.copy_ns),
          compare_u64);
    qsort(samples.total_ns, samples.count, sizeof(*samples.total_ns),
          compare_u64);
    printf("phase=%s count=%u pre_copy_avg_ns=%" PRIu64
           " pre_copy_p50_ns=%" PRIu64 " pre_copy_p99_ns=%" PRIu64
           " copy_avg_ns=%" PRIu64 " copy_p50_ns=%" PRIu64
           " copy_p99_ns=%" PRIu64 " total_avg_ns=%" PRIu64
           " total_p50_ns=%" PRIu64 " total_p99_ns=%" PRIu64 "\n",
           label, samples.count, lookup_sum / samples.count,
           percentile(samples.lookup_ns, samples.count, 50),
           percentile(samples.lookup_ns, samples.count, 99),
           copy_sum / samples.count,
           percentile(samples.copy_ns, samples.count, 50),
           percentile(samples.copy_ns, samples.count, 99),
           total_sum / samples.count,
           percentile(samples.total_ns, samples.count, 50),
           percentile(samples.total_ns, samples.count, 99));
    free(threads);
    free(args);
    free(samples.lookup_ns);
    free(samples.copy_ns);
    free(samples.total_ns);
    return 0;
}

static int parse_u32(const char *text, uint32_t *out)
{
    char *end = NULL;
    unsigned long value = strtoul(text, &end, 10);
    if (end == text || *end != '\0' || value > UINT32_MAX)
        return -1;
    *out = (uint32_t)value;
    return 0;
}

static void usage(const char *program)
{
    fprintf(stderr,
            "usage: %s --endpoint HOST:PORT --manifest FILE "
            "--client-host HOST --dim N [options]\n"
            "  --count N --old-start N --steady-start N --warmup N "
            "--iterations N --threads N --clients N --timeout-ms N\n", program);
}

int main(int argc, char **argv)
{
    warm_copy_config_t cfg = {
        .dim = 300,
        .count = 10000,
        .old_start = 10000,
        .steady_start = 20000,
        .warmup = 100,
        .iterations = 10000,
        .threads = 1,
        .clients = 1,
        .pipeline = 32,
        .timeout_ms = 10000,
    };
    for (int i = 1; i < argc; i++) {
        if (i + 1 >= argc)
            goto bad_args;
        if (!strcmp(argv[i], "--endpoint")) cfg.endpoint = argv[++i];
        else if (!strcmp(argv[i], "--manifest")) cfg.manifest = argv[++i];
        else if (!strcmp(argv[i], "--client-host")) cfg.client_host = argv[++i];
        else if (!strcmp(argv[i], "--dim")) {
            if (parse_u32(argv[++i], &cfg.dim) != 0) goto bad_args;
        } else if (!strcmp(argv[i], "--count")) {
            if (parse_u32(argv[++i], &cfg.count) != 0) goto bad_args;
        } else if (!strcmp(argv[i], "--old-start")) {
            if (parse_u32(argv[++i], &cfg.old_start) != 0) goto bad_args;
        } else if (!strcmp(argv[i], "--steady-start")) {
            if (parse_u32(argv[++i], &cfg.steady_start) != 0) goto bad_args;
        } else if (!strcmp(argv[i], "--warmup")) {
            if (parse_u32(argv[++i], &cfg.warmup) != 0) goto bad_args;
        } else if (!strcmp(argv[i], "--iterations")) {
            if (parse_u32(argv[++i], &cfg.iterations) != 0) goto bad_args;
        } else if (!strcmp(argv[i], "--threads")) {
            if (parse_u32(argv[++i], &cfg.threads) != 0) goto bad_args;
        } else if (!strcmp(argv[i], "--clients")) {
            if (parse_u32(argv[++i], &cfg.clients) != 0) goto bad_args;
        } else if (!strcmp(argv[i], "--pipeline")) {
            if (parse_u32(argv[++i], &cfg.pipeline) != 0) goto bad_args;
        } else if (!strcmp(argv[i], "--timeout-ms")) {
            if (parse_u32(argv[++i], &cfg.timeout_ms) != 0) goto bad_args;
        } else if (!strcmp(argv[i], "--help")) {
            usage(argv[0]);
            return 0;
        } else {
            goto bad_args;
        }
    }
    if (!cfg.endpoint || !cfg.manifest || !cfg.client_host || cfg.dim == 0 ||
        cfg.dim > VEMB_V16_MAX_DIM || cfg.count == 0 ||
        cfg.iterations == 0 || cfg.threads == 0 || cfg.clients == 0 ||
        cfg.pipeline == 0 || cfg.pipeline > 256 ||
        cfg.old_start > UINT32_MAX - cfg.count ||
        cfg.steady_start > UINT32_MAX - cfg.count)
        goto bad_args;

    monotonicInit();
    const char *seeds[] = {cfg.endpoint};
    vemb_v16_client_t *client = vemb_v16_client_create(
        seeds, 1, cfg.dim, cfg.timeout_ms, VEMB_V16_TRANSPORT_AERON);
    if (!client || vemb_v16_client_configure_ub_peer_view(
                    client, cfg.manifest, cfg.client_host) != 0 ||
        vemb_v16_client_topology_refresh(client) != 0)
        goto failed_client;

    float *vector = calloc(cfg.dim, sizeof(*vector));
    if (!vector || write_range(client, &cfg, cfg.old_start, vector) != 0 ||
        write_range(client, &cfg, cfg.steady_start, vector) != 0)
        goto failed_run;
    printf("injected old=%u..%u steady=%u..%u\n", cfg.old_start,
           cfg.old_start + cfg.count - 1, cfg.steady_start,
           cfg.steady_start + cfg.count - 1);
    free(vector);
    vector = NULL;
    vemb_v16_client_destroy(client);
    client = NULL;
    if (read_phase(&cfg, "old", cfg.old_start) != 0 ||
        read_phase(&cfg, "steady", cfg.steady_start) != 0 ||
        read_phase(&cfg, "old_reread", cfg.old_start) != 0 ||
        read_phase(&cfg, "steady_reread", cfg.steady_start) != 0)
        goto failed_run;
    return 0;

failed_run:
    free(vector);
    vemb_v16_client_destroy(client);
    return 1;
failed_client:
    vemb_v16_client_destroy(client);
    return 1;
bad_args:
    usage(argv[0]);
    return 2;
}
