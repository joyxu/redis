/*
 * Vector Engine - UB Implementation
 * High-performance UB bus-based vector operations with SVE acceleration
 *
 * Key differences from native Redis vector-set behaviour:
 *
 * 1. Single-table addressing model
 *    Redis uses per-key HNSW graphs where elements are user-defined names
 *    (e.g. "doc:123"). UB uses a flat shared-memory table where vectors are
 *    addressed by row index. The `key` parameter is therefore unused in all
 *    UB engine callbacks (UNUSED(key)).
 *
 * 2. Element identity
 *    In VSIM results, `element` is the stringified row index ("0", "1", …)
 *    rather than a user-supplied name. Callers must maintain their own
 *    index-to-object mapping externally.
 *
 * 3. Attributes not supported
 *    Redis HNSW nodes can carry JSON attributes (set via VADD … SETATTR)
 *    used for hybrid FILTER queries. UB stores raw vectors only, so
 *    `attributes` is always NULL in query results. VSIM … WITHATTRIBS will
 *    return nil for every element; VSIM … FILTER is not available.
 *
 * 4. Similarity computation strategy
 *    Redis VSIM walks an HNSW graph (approximate, O(log N)).
 *    UB VSIM currently performs key-scoped brute-force score computation
 *    (exact, O(N·dim)) and returns one score per active row.
 *    It does not perform top-k selection internally.
 */

#include "macro.h"
#include "vector_engine.h"
#include "ub_client.h"
#include "ub_metadata.h"
#include "proxy_aggregator.h"
#include "sve_config.h"
#include "sve_similarity.h"
#include "supernode_worker.h"
#include "zmalloc.h"
#include "server.h"

#include <inttypes.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#ifndef UNUSED
#define UNUSED(V) ((void)(V))
#endif

/* ============================================================
 * VSIM helper types and functions
 * ============================================================ */

/* Helper function to extract C string from Redis object */
static const char *ub_engine_object_to_cstring(void *arg, sds *tmp)
{
    robj *obj = (robj *)arg;

    if (arg == NULL) {
        return NULL;
    }
    if (sdsEncodedObject(obj)) {
        return obj->ptr;
    }

    obj = getDecodedObject(obj);
    if (obj == NULL || !sdsEncodedObject(obj)) {
        return NULL;
    }

    *tmp = sdsdup(obj->ptr);
    decrRefCount(obj);
    return *tmp;
}

/* ============================================================
 * UB Engine Common Helpers
 * ============================================================ */

/* Cached address space, loaded once during init */
static ub_address_space_t *ub_cached_addr_space = NULL;

/*
 * Get the cached address space. Returns NULL if engine not initialized.
 */
static inline ub_address_space_t *ub_engine_get_addr_space(void) {
    return ub_cached_addr_space;
}

static const char *ub_engine_key_to_cstring(void *key, sds *tmp)
{
    if (!key) return NULL;
    return ub_engine_object_to_cstring(key, tmp);
}

static ub_vector_set_meta_t *ub_engine_lookup_set_meta(void *key, sds *key_tmp)
{
    const char *key_name = ub_engine_key_to_cstring(key, key_tmp);
    if (!key_name) return NULL;
    return ub_metadata_get_set(key_name);
}

static ub_vector_set_meta_t *ub_engine_get_or_create_set_meta(void *key,
                                                              size_t dim,
                                                              sds *key_tmp)
{
    const char *key_name = ub_engine_key_to_cstring(key, key_tmp);
    if (!key_name) return NULL;
    return ub_metadata_get_or_create_set(key_name, dim);
}

/* ============================================================
 * UB Engine Implementation
 * ============================================================ */

static int ub_engine_init(void) {
    serverLog(LL_NOTICE, "Initializing UB Vector Engine");
    if (ub_client_init(&server.ub) != C_OK) return C_ERR;
    if (ub_metadata_init() != C_OK) return C_ERR;

    /* Pre-load the embedding table once */
    if (ub_client_load_embedding_table(server.ub.table_name, &ub_cached_addr_space) != C_OK
        || !ub_cached_addr_space) {
        serverLog(LL_WARNING, "UB Vector Engine: failed to load embedding table");
        return C_ERR;
    }

    if (proxy_aggregator_init(1) != C_OK) {
        serverLog(LL_WARNING, "UB Vector Engine: failed to initialize FC proxy");
        return C_ERR;
    }
    if (supernode_init(0, server.supernode_workers) != C_OK) {
        serverLog(LL_WARNING, "UB Vector Engine: failed to initialize local SuperNode");
        proxy_aggregator_shutdown();
        return C_ERR;
    }

    return C_OK;
}

static void ub_engine_cleanup(void) {
    supernode_shutdown();
    proxy_aggregator_shutdown();
    ub_cached_addr_space = NULL;  /* owned by ub_client, freed in ub_client_cleanup */
    ub_metadata_cleanup();
    ub_client_cleanup();
    serverLog(LL_NOTICE, "Cleaning up UB Vector Engine");
}

static int ub_engine_vadd(void *ctx, void *key, vector_data_t *vector,
                          void *element, void *attributes) {
    ub_mem_config_t *cfg = &server.ub;
    ub_address_space_t *addr_space = ub_engine_get_addr_space();
    ub_vector_set_meta_t *set = NULL;
    uint64_t row_id = 0;
    int row_created = 0;
    sds key_tmp = NULL;
    sds element_tmp = NULL;
    int rc = C_ERR;

    UNUSED(attributes);

    if (!addr_space) return C_ERR;
    if (!vector || cfg->vector_dimension <= 0) return C_ERR;
    if (unlikely(vector->dim != (size_t)cfg->vector_dimension)) return C_ERR;

    set = ub_engine_get_or_create_set_meta(key, vector->dim, &key_tmp);
    if (!set) goto cleanup;
    const char *element_name = ctx ? ub_engine_object_to_cstring(element, &element_tmp)
                                   : (const char *)element;
    if (!element_name) goto cleanup;
    if (ub_metadata_lookup_row(set, element_name, &row_id) != C_OK) {
        if (ub_metadata_alloc_row(set, element_name, &row_id) != C_OK) goto cleanup;
        row_created = 1;
    }
    if (ub_client_store_single(addr_space, row_id, vector->data, vector->dim) != C_OK) goto cleanup;

    rc = C_OK;

cleanup:
    if (rc != C_OK && row_created && set) {
        const char *element_name = element_tmp ? element_tmp : (const char *)element;
        if (element_name) ub_metadata_remove_row(set, element_name, NULL);
    }
    sdsfree(key_tmp);
    sdsfree(element_tmp);
    return rc;
}

static int ub_engine_vrem(void *ctx, void *key, void *element) {
    ub_mem_config_t *cfg = &server.ub;
    ub_address_space_t *addr_space = ub_engine_get_addr_space();
    ub_vector_set_meta_t *set = NULL;
    uint64_t row_id = 0;
    sds key_tmp = NULL;
    sds element_tmp = NULL;
    float *zero_buf = NULL;
    int rc = C_ERR;

    if (!addr_space || cfg->vector_dimension <= 0) return C_ERR;

    set = ub_engine_lookup_set_meta(key, &key_tmp);
    if (!set) goto cleanup;
    const char *element_name = ctx ? ub_engine_object_to_cstring(element, &element_tmp)
                                   : (const char *)element;
    if (!element_name) goto cleanup;
    if (ub_metadata_remove_row(set, element_name, &row_id) != C_OK) goto cleanup;

    /* Allocate zero-filled buffer for soft delete */
    zero_buf = zcalloc((size_t)cfg->vector_dimension * sizeof(float));
    if (!zero_buf) goto cleanup;

    if (ub_client_store_single(addr_space, row_id,
                              zero_buf, (size_t)cfg->vector_dimension) != C_OK) goto cleanup;

    rc = C_OK;

cleanup:
    zfree(zero_buf);
    sdsfree(key_tmp);
    sdsfree(element_tmp);
    return rc;
}

/*
 * WARNING:
 * ub_engine_vsim() is currently implemented as a score-only similarity kernel.
 * It computes one similarity score for every active row under the specified key
 * and returns the full result list. The `count` parameter is intentionally
 * ignored here. Any top-k selection, thresholding, or ranking policy must be
 * implemented by the caller or by a later proxy/supernode stage.
 */
static int ub_engine_vsim(void *ctx, void *key, vector_data_t *query_vector,
                          size_t count, vector_query_result_t **results,
                          size_t *num_results) {
    ub_mem_config_t *cfg = &server.ub;
    ub_address_space_t *addr_space = ub_engine_get_addr_space();
    float *candidates_buf = NULL;
    uint64_t *rows = NULL;
    sds *elements = NULL;
    ub_vector_set_meta_t *set = NULL;
    sds key_tmp = NULL;
    size_t dim, row_count;
    int rc = C_ERR;

    UNUSED(ctx);
    UNUSED(count);

    RETURN_IF(!query_vector || !results || !num_results || !addr_space, C_ERR);

    dim = (size_t)cfg->vector_dimension;
    if (dim == 0 || unlikely(query_vector->dim != dim)) return C_ERR;

    if (addr_space->vector_stride_bytes == 0) return C_ERR;

    set = ub_engine_lookup_set_meta(key, &key_tmp);
    if (!set) {
        *num_results = 0;
        *results = NULL;
        return C_OK;
    }

    row_count = ub_metadata_collect_rows(set, &rows, &elements);
    if (row_count == 0) {
        *num_results = 0;
        *results = NULL;
        goto cleanup;
    }

    /*
     * Load all rows for the current key into a contiguous buffer and compute
     * one similarity score per row. This path intentionally does not do top-k
     * selection; it acts as a pure similarity-compute kernel.
     */
    candidates_buf = zmalloc(row_count * dim * sizeof(float));
    if (!candidates_buf) goto cleanup;

    if (ub_client_perform_gather_load(addr_space, rows, row_count,
                                      candidates_buf, dim) != C_OK) {
        goto cleanup;
    }

    *results = vector_query_result_create(row_count);
    if (!*results) goto cleanup;

    for (size_t i = 0; i < row_count; i++) {
        (*results)[i].element = sdsdup(elements[i]);
        (*results)[i].score = (double)sve_cosine_similarity_f32(query_vector->data,
                                                                candidates_buf + i * dim,
                                                                dim);
        (*results)[i].attributes = NULL;
    }
    *num_results = row_count;
    rc = C_OK;

cleanup:
    zfree(candidates_buf);
    zfree(rows);
    if (elements) {
        for (size_t i = 0; i < row_count; i++) sdsfree(elements[i]);
        zfree(elements);
    }
    sdsfree(key_tmp);
    return rc;
}

/* Exported for use by vset module */
int ub_engine_vemb(void *ctx, void *key, void *element, vector_data_t *result) {
    ub_mem_config_t *cfg = &server.ub;
    ub_address_space_t *addr_space = ub_engine_get_addr_space();
    ub_vector_set_meta_t *set = NULL;
    uint64_t row_id = 0;
    sds key_tmp = NULL;
    sds element_tmp = NULL;
    int rc = C_ERR;

    if (!result || !addr_space || cfg->vector_dimension <= 0) return C_ERR;

    set = ub_engine_lookup_set_meta(key, &key_tmp);
    if (!set) {
        rc = C_OK;
        goto cleanup;
    }
    const char *element_name = ctx ? ub_engine_object_to_cstring(element, &element_tmp)
                                   : (const char *)element;
    if (!element_name) {
        rc = C_OK;
        goto cleanup;
    }
    if (ub_metadata_lookup_row(set, element_name, &row_id) != C_OK) {
        rc = C_OK;
        goto cleanup;
    }

    result->dim = set->dim;
    result->is_fp32 = 1;
    result->data = zmalloc(sizeof(float) * result->dim);
    if (!result->data) goto cleanup;

    if (ub_client_load_single(addr_space, row_id,
                             result->data, result->dim) != C_OK) {
        zfree(result->data);
        result->data = NULL;
        result->dim = 0;
        goto cleanup;
    }

    rc = C_OK;

cleanup:
    sdsfree(key_tmp);
    sdsfree(element_tmp);
    return rc;
}

static int ub_engine_vcard(void *ctx, void *key) {
    ub_vector_set_meta_t *set;
    sds key_tmp = NULL;

    UNUSED(ctx);

    set = ub_engine_lookup_set_meta(key, &key_tmp);
    sdsfree(key_tmp);
    return set ? (int)ub_metadata_cardinality(set) : 0;
}

static int ub_engine_vdim(void *ctx, void *key) {
    ub_vector_set_meta_t *set;
    sds key_tmp = NULL;

    UNUSED(ctx);

    set = ub_engine_lookup_set_meta(key, &key_tmp);
    sdsfree(key_tmp);
    return set ? (int)set->dim : (server.ub.vector_dimension > 0 ? server.ub.vector_dimension : 0);
}

static int ub_engine_set_config(const char *key, const char *value) {
    return ub_client_set_config(key, value);
}

static sds ub_engine_get_config(const char *key) {
    return ub_client_get_config(key);
}

static sds ub_engine_get_stats(void) {
    sds stats = ub_client_get_stats();
    sds proxy_stats = proxy_aggregator_get_stats();
    sds supernode_stats = supernode_get_stats();

    if (proxy_stats) {
        stats = sdscat(stats, "\n");
        stats = sdscatsds(stats, proxy_stats);
        sdsfree(proxy_stats);
    }
    if (supernode_stats) {
        stats = sdscat(stats, "\n");
        stats = sdscatsds(stats, supernode_stats);
        sdsfree(supernode_stats);
    }
    return stats;
}

/* UB Engine Structure */
static vector_engine_t ub_vector_engine = {
    .type = VECTOR_ENGINE_UB,
    .init = ub_engine_init,
    .cleanup = ub_engine_cleanup,
    .vadd = ub_engine_vadd,
    .vrem = ub_engine_vrem,
    .vsim = ub_engine_vsim,
    .vemb = ub_engine_vemb,
    .vcard = ub_engine_vcard,
    .vdim = ub_engine_vdim,
    .set_config = ub_engine_set_config,
    .get_config = ub_engine_get_config,
    .get_stats = ub_engine_get_stats
};

/* Auto-register on load via GCC/Clang constructor */
__attribute__((constructor))
static void ub_engine_register(void) {
    vector_engine_register(VECTOR_ENGINE_UB, &ub_vector_engine);
}
