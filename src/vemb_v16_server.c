#define _GNU_SOURCE

/*
 * Deprecated compatibility harness. Keep it for focused protocol smoke tests;
 * all VEMB performance scripts must start redis-server and use memtier.
 */

#include "vemb_v16_proxy.h"
#include "vemb_v16_storage.h"
#include "vemb_v16_log.h"
#include "monotonic.h"

#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static vemb_v16_proxy_t *g_proxy;

static uint32_t clamp_worker_count(long value, uint32_t max_value) {
    if (value < 1)
        return 1;
    if ((unsigned long)value > max_value)
        return max_value;
    return (uint32_t)value;
}

static uint32_t default_balanced_worker_count(void) {
    long cpus = sysconf(_SC_NPROCESSORS_ONLN);
    long target = cpus > 0 ? cpus / 4 : 1;
    if (target < 1)
        target = 1;
    if (target > 32)
        target = 32;
    return clamp_worker_count(target, VEMB_V16_MAX_CHANNELS);
}

static uint32_t default_proxy_io_threads(void) {
    return default_balanced_worker_count();
}

static uint32_t default_supernode_workers(void) {
    return default_balanced_worker_count();
}

static void on_signal(int sig) {
    (void)sig;
    vemb_v16_proxy_stop(g_proxy);
}

int main(int argc, char **argv) {
    int ret = 1;
    vemb_v16_storage_ctx_t *storage = NULL;
    const char *vector_region_name = VEMB_V16_DEFAULT_VECTOR_REGION;
    const char *warm_regions_manifest = NULL;
    uint32_t dim = 0;
    uint32_t max_vectors = VEMB_V16_DEFAULT_MAX_VECTORS;
    uint32_t warm_region_id = 0;
    uint32_t warm_backend_type = VEMB_V16_REGION_UB;
    uint64_t warm_mmap_offset = 0;
    int loglevel = LL_NOTICE;
    const char *tcp_host = VEMB_V16_TCP_HOST;
    uint16_t tcp_port = VEMB_V16_TCP_PORT;
    const char *transport = "tcp";
    const char *aeron_ub_path = VEMB_V16_DEFAULT_AERON_UB_PATH;
    const char *aeron_response_ub_path =
        VEMB_V16_DEFAULT_AERON_RESPONSE_UB_PATH;
    uint32_t proxy_io_threads = default_proxy_io_threads();
    uint32_t supernode_workers = default_supernode_workers();
    uint32_t batch_request_size = VEMB_V16_BATCH_REQUEST_SIZE_DEFAULT;
    int reset_warm_regions = 0;

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--transport") && i + 1 < argc) {
            transport = argv[++i];
            if (strcmp(transport, "aeron") &&
                strcmp(transport, "tcp")) {
                fprintf(stderr, "invalid transport\n");
                goto cleanup;
            }
        } else if (!strcmp(argv[i], "--tcp-host") && i + 1 < argc) {
            tcp_host = argv[++i];
        } else if (!strcmp(argv[i], "--tcp-port") && i + 1 < argc) {
            tcp_port = (uint16_t)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--aeron-ub-path") && i + 1 < argc) {
            aeron_ub_path = argv[++i];
        } else if (!strcmp(argv[i], "--aeron-response-ub-path") &&
                   i + 1 < argc) {
            aeron_response_ub_path = argv[++i];
        } else if (!strcmp(argv[i], "--proxy-io-threads") && i + 1 < argc) {
            proxy_io_threads = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--supernode-workers") && i + 1 < argc) {
            supernode_workers = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--batch-request-size") && i + 1 < argc) {
            batch_request_size = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--vector-region") && i + 1 < argc) {
            vector_region_name = argv[++i];
        } else if (!strcmp(argv[i], "--warm-regions-manifest") && i + 1 < argc) {
            warm_regions_manifest = argv[++i];
        } else if (!strcmp(argv[i], "--reset-warm-regions")) {
            reset_warm_regions = 1;
        } else if (!strcmp(argv[i], "--dim") && i + 1 < argc) {
            dim = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--max-vectors") && i + 1 < argc) {
            max_vectors = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--region-id") && i + 1 < argc) {
            warm_region_id = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--warm-backend") && i + 1 < argc) {
            const char *backend = argv[++i];
            if (!strcmp(backend, "shm")) {
                warm_backend_type = VEMB_V16_REGION_LOCAL_SHM;
            } else if (!strcmp(backend, "ub")) {
                warm_backend_type = VEMB_V16_REGION_UB;
            } else {
                fprintf(stderr, "invalid warm backend\n");
                goto cleanup;
            }
        } else if (!strcmp(argv[i], "--warm-mmap-offset") && i + 1 < argc) {
            warm_mmap_offset = strtoull(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--loglevel") && i + 1 < argc) {
            if (vemb_v16_parse_log_level(argv[++i], &loglevel) != 0) {
                fprintf(stderr, "invalid loglevel\n");
                goto cleanup;
            }
        } else if (!strcmp(argv[i], "--help")) {
        printf("usage: %s [--transport tcp|aeron] [--aeron-ub-path PATH] [--aeron-response-ub-path PATH] [--tcp-host HOST] [--tcp-port PORT] [--proxy-io-threads N] [--supernode-workers N] [--vector-region SHM_NAME_OR_UB_PATH] [--warm-regions-manifest PATH] [--reset-warm-regions] [--region-id N] [--warm-backend shm|ub] [--warm-mmap-offset N] [--dim N] [--max-vectors N] [--loglevel debug|verbose|notice|warning|nothing]\n", argv[0]);
            ret = 0;
            goto cleanup;
        }
    }

    if (proxy_io_threads == 0) {
        fprintf(stderr, "--proxy-io-threads must be >= 1\n");
        goto cleanup;
    }
    if (supernode_workers == 0) {
        fprintf(stderr, "--supernode-workers must be >= 1\n");
        goto cleanup;
    }
    if (batch_request_size == 0 ||
        batch_request_size > VEMB_V16_BATCH_REQUEST_SIZE_MAX) {
        fprintf(stderr, "invalid batch request size\n");
        goto cleanup;
    }
    if (dim == 0 || dim > VEMB_V16_MAX_DIM) {
        fprintf(stderr, "--dim is required and must be in [1, %u]\n",
                VEMB_V16_MAX_DIM);
        goto cleanup;
    }
    if (max_vectors == 0) {
        fprintf(stderr, "--max-vectors must be >= 1\n");
        goto cleanup;
    }

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN);
    monotonicInit();
    vemb_v16_log_init();
    vemb_v16_set_log_level(loglevel);
    serverLog(LL_NOTICE, "vemb_v16 server starting: transport=%s tcp_control=%s:%u proxy_io_threads=%u supernode_workers=%u dim=%u max_vectors=%u vector_region=%s warm_regions_manifest=%s",
              transport, tcp_host, tcp_port,
              proxy_io_threads,
              supernode_workers, dim, max_vectors, vector_region_name,
              warm_regions_manifest ? warm_regions_manifest : "(none)");

    vemb_v16_warm_regions_manifest_t manifest;
    memset(&manifest, 0, sizeof(manifest));
    if (warm_regions_manifest) {
        if (vemb_v16_parse_warm_regions_manifest(warm_regions_manifest,
                                                 dim * sizeof(float),
                                                 &manifest) != 0) {
            serverLog(LL_WARNING, "failed to parse warm regions manifest: %s",
                      warm_regions_manifest);
            goto cleanup;
        }
    } else {
        if (!vector_region_name || !vector_region_name[0])
            vector_region_name = VEMB_V16_DEFAULT_VECTOR_REGION;
        if (vector_region_name[0] != '/' ||
            strlen(vector_region_name) >= sizeof(manifest.regions[0].path)) {
            serverLog(LL_WARNING, "invalid vemb_v16 vector region name: %s",
                      vector_region_name);
            goto cleanup;
        }
        manifest.local_region_weight = 4;
        manifest.region_count = 1;
        vemb_v16_manifest_region_t *region = &manifest.regions[0];
        region->region_id = warm_region_id;
        region->backend_type = warm_backend_type ?
            warm_backend_type : VEMB_V16_REGION_LOCAL_SHM;
        region->cache_policy = VEMB_V16_UB_CACHE_POLICY_CACHEABLE;
        region->has_cache_policy = 1;
        region->is_local = 1;
        region->has_is_local = 1;
        region->weight = 1;
        region->value_size = dim * sizeof(float);
        region->mmap_offset = warm_mmap_offset;
        region->region_bytes = (uint64_t)region->value_size * max_vectors;
        strncpy(region->path, vector_region_name, sizeof(region->path) - 1);
    }
    if (reset_warm_regions) {
        serverLog(LL_NOTICE, "resetting vemb_v16 warm regions before storage open");
        if (vemb_v16_storage_reset_manifest_regions(&manifest) != 0) {
            serverLog(LL_WARNING, "failed to reset vemb_v16 warm regions");
            goto cleanup;
        }
    }
    int storage_rc = vemb_v16_storage_ctx_create_from_manifest(&storage,
                                                               dim,
                                                               dim * sizeof(float),
                                                               max_vectors,
                                                               &manifest);
    if (storage_rc != 0) {
        serverLog(LL_WARNING, "failed to create vemb_v16 storage");
        goto cleanup;
    }
    if (vemb_v16_proxy_create(&g_proxy,
                              dim,
                              max_vectors,
                              storage,
                              &manifest) != 0) {
        serverLog(LL_WARNING, "failed to create vemb_v16 proxy");
        goto cleanup;
    }
    if (vemb_v16_proxy_set_aeron_ub_path(g_proxy, aeron_ub_path) != 0) {
        serverLog(LL_WARNING, "failed to configure Aeron UB path: %s",
                  aeron_ub_path);
        goto cleanup;
    }
    if (vemb_v16_proxy_set_aeron_response_ub_path(
            g_proxy, aeron_response_ub_path) != 0) {
        serverLog(LL_WARNING,
                  "failed to configure Aeron response UB path: %s",
                  aeron_response_ub_path);
        goto cleanup;
    }
    if (vemb_v16_proxy_set_supernode_workers(g_proxy, supernode_workers) != 0) {
        serverLog(LL_WARNING, "failed to configure vemb_v16 supernode workers");
        goto cleanup;
    }
    if (vemb_v16_proxy_set_proxy_io_threads(g_proxy, proxy_io_threads) != 0) {
        serverLog(LL_WARNING, "failed to configure vemb_v16 proxy io threads");
        goto cleanup;
    }
    if (vemb_v16_proxy_set_batch_request_size(g_proxy,
                                              batch_request_size) != 0) {
        serverLog(LL_WARNING, "failed to configure batch request size");
        goto cleanup;
    }
    if (!strcmp(transport, "aeron")) {
        if (vemb_v16_proxy_enable_aeron_tcp_control(g_proxy,
                                                     tcp_host,
                                                     tcp_port) != 0) {
            serverLog(LL_WARNING, "failed to enable vemb_v16 Aeron TCP control plane");
            goto cleanup;
        }
    } else {
        if (vemb_v16_proxy_enable_tcp(g_proxy, tcp_host, tcp_port) != 0) {
            serverLog(LL_WARNING, "failed to enable vemb_v16 tcp transport");
            goto cleanup;
        }
    }

    ret = vemb_v16_proxy_run(g_proxy);
    vemb_v16_stats_t stats;
    vemb_v16_proxy_get_stats(g_proxy, &stats);
    serverLog(LL_NOTICE, "vemb_v16 stats: total=%llu vadd=%llu vemb=%llu vsim=%llu not_found=%llu published=%llu completed=%llu active_channels=%llu",
           (unsigned long long)stats.total_requests,
           (unsigned long long)stats.vadd_requests,
           (unsigned long long)stats.vemb_requests,
           (unsigned long long)stats.vsim_requests,
           (unsigned long long)stats.not_found,
           (unsigned long long)stats.published_jobs,
           (unsigned long long)stats.completed_jobs,
           (unsigned long long)stats.active_channels);
    serverLog(LL_NOTICE, "vemb_v16 stats: bitmap_lock_success=%llu bitmap_lock_failure=%llu",
           (unsigned long long)stats.bitmap_lock_success,
           (unsigned long long)stats.bitmap_lock_failure);
    serverLog(LL_NOTICE, "vemb_v16 stats: migration moved=%llu stale=%llu ask=%llu forward=%llu duplicate=%llu source_gc=%llu gc_safe_watermark=%llu baseline_sent=%llu baseline_skipped=%llu baseline_error=%llu baseline_retry_queued=%llu baseline_retry_sent=%llu baseline_retry_pending=%llu",
           (unsigned long long)stats.moved_count,
           (unsigned long long)stats.stale_count,
           (unsigned long long)stats.ask_count,
           (unsigned long long)stats.forward_count,
           (unsigned long long)stats.duplicate_request_count,
           (unsigned long long)stats.source_gc_count,
           (unsigned long long)stats.gc_safe_watermark,
           (unsigned long long)stats.migration_baseline_sent,
           (unsigned long long)stats.migration_baseline_skipped,
           (unsigned long long)stats.migration_baseline_error,
           (unsigned long long)stats.migration_baseline_retry_queued,
           (unsigned long long)stats.migration_baseline_retry_sent,
           (unsigned long long)stats.migration_baseline_retry_pending);
    serverLog(LL_NOTICE, "vemb_v16 stats: warm_regions=%llu warm_full=%llu warm_alloc_local=%llu warm_alloc_remote=%llu warm_fallback=%llu warm_cold_spill=%llu warm_fail=%llu warm_evict_ok=%llu warm_evict_fail=%llu warm_overwrite=%llu warm_stale=%llu remote_meta_stale=%llu warm_local_pct=%llu",
           (unsigned long long)stats.warm_region_count,
           (unsigned long long)stats.warm_region_full_count,
           (unsigned long long)stats.warm_alloc_local,
           (unsigned long long)stats.warm_alloc_remote,
           (unsigned long long)stats.warm_alloc_fallback,
           (unsigned long long)stats.warm_alloc_cold_spill,
           (unsigned long long)stats.warm_alloc_fail,
           (unsigned long long)stats.warm_eviction_success,
           (unsigned long long)stats.warm_eviction_fail,
           (unsigned long long)stats.warm_same_key_overwrite,
           (unsigned long long)stats.warm_stale_handle_reject,
           (unsigned long long)stats.remote_meta_stale,
           (unsigned long long)stats.warm_region_hash_local_pct);
    serverLog(LL_NOTICE, "vemb_v16 stats: depth request=%llu response=%llu job_shard=%llu completion=%llu",
           (unsigned long long)stats.request_ring_depth,
           (unsigned long long)stats.response_ring_depth,
           (unsigned long long)stats.job_shard_queue_depth,
           (unsigned long long)stats.completion_ring_depth);
    serverLog(LL_NOTICE,
              "vemb_v16 stats: remote_meta hit=%llu miss=%llu busy=%llu probes=%llu async_enqueue=%llu async_drop=%llu publish_ok=%llu insert=%llu update=%llu evict=%llu repair_enqueue=%llu repair_ok=%llu rpc_count=%llu rpc_ok=%llu rpc_not_found=%llu rpc_busy=%llu rpc_timeout=%llu rpc_error=%llu rpc_handle=%llu rpc_snapshot=%llu",
              (unsigned long long)stats.remote_meta_lookup_hit,
              (unsigned long long)stats.remote_meta_lookup_miss,
              (unsigned long long)stats.remote_meta_lookup_busy,
              (unsigned long long)stats.remote_meta_lookup_way_probe,
              (unsigned long long)stats.remote_meta_publish_async_enqueue,
              (unsigned long long)stats.remote_meta_publish_async_drop,
              (unsigned long long)stats.remote_meta_publish_ok,
              (unsigned long long)stats.remote_meta_publish_insert,
              (unsigned long long)stats.remote_meta_publish_update,
              (unsigned long long)stats.remote_meta_publish_evict,
              (unsigned long long)stats.remote_meta_repair_enqueue,
              (unsigned long long)stats.remote_meta_repair_ok,
              (unsigned long long)stats.ub_lookup_rpc_count,
              (unsigned long long)stats.ub_lookup_rpc_ok,
              (unsigned long long)stats.ub_lookup_rpc_not_found,
              (unsigned long long)stats.ub_lookup_rpc_busy,
              (unsigned long long)stats.ub_lookup_rpc_timeout,
              (unsigned long long)stats.ub_lookup_rpc_error,
              (unsigned long long)stats.ub_lookup_rpc_handle,
              (unsigned long long)stats.ub_lookup_rpc_snapshot);
cleanup:
    if (g_proxy) {
        vemb_v16_proxy_destroy(g_proxy);
        g_proxy = NULL;
    }
    if (storage)
        vemb_v16_storage_ctx_destroy(storage);
    return ret == 0 ? 0 : 1;
}
