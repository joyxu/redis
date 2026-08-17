#include "vemb_v16_table.h"
#include "vemb_v16_protocol.h"
#include "zmalloc.h"

#include <stdatomic.h>
#include <string.h>

#define VEMB_V16_HASH_BUCKETS 262144u

typedef struct vemb_v16_table_entry {
    uint64_t key_hash;
    uint32_t key_len;
    uint32_t row_id;
    char key[VEMB_V16_MAX_KEY_LEN];
    int used;
} vemb_v16_table_entry_t;

struct vemb_v16_table {
    uint32_t vector_dim;
    uint32_t vector_stride;
    uint32_t max_vectors;
    uint8_t *vector_region;
    size_t vector_region_size;
    ub_address_space_t ubas;
    state_bitmap_t bitmap;
    sve_operation_stats_t sve_stats;
    sve_gather_ctx_t gather_ctx;
    vemb_v16_table_entry_t *index;
    uint32_t hash_mask;
    atomic_uint_fast32_t next_row_id;
};

int vemb_v16_table_create(vemb_v16_table_t **out,
                          uint32_t vector_dim,
                          uint32_t max_vectors,
                          uint8_t *vector_region,
                          size_t vector_region_size) {
    if (!out || !vector_region || vector_dim == 0 || max_vectors == 0)
        return -1;
    vemb_v16_table_t *table = zcalloc(sizeof(*table));
    if (!table) return -1;
    table->vector_dim = vector_dim;
    table->vector_stride = vector_dim * sizeof(float);
    table->max_vectors = max_vectors;
    table->vector_region = vector_region;
    table->vector_region_size = vector_region_size;
    table->ubas = (ub_address_space_t){
        .size = vector_region_size,
        .mapped_addr = vector_region,
        .mapping_addr = vector_region,
        .mapping_size = vector_region_size,
        .vector_stride_bytes = table->vector_stride,
        .shm_fd = -1,
    };
    table->hash_mask = VEMB_V16_HASH_BUCKETS - 1u;
    atomic_init(&table->next_row_id, 0);
    atomic_init(&table->sve_stats.lock_success, 0);
    atomic_init(&table->sve_stats.lock_failure, 0);
    if (bitmap_init(&table->bitmap, max_vectors) != 0) {
        zfree(table);
        return -1;
    }
    sve_gather_ctx_init(&table->gather_ctx,
                        &table->ubas,
                        &table->bitmap,
                        table->vector_dim,
                        table->vector_stride,
                        table->max_vectors,
                        &table->sve_stats);
    table->index = zcalloc(sizeof(*table->index) * VEMB_V16_HASH_BUCKETS);
    if (!table->index) {
        bitmap_destroy(&table->bitmap);
        zfree(table);
        return -1;
    }
    *out = table;
    return 0;
}

void vemb_v16_table_destroy(vemb_v16_table_t *table) {
    bitmap_destroy(&table->bitmap);
    zfree(table->index);
    zfree(table);
}

uint32_t vemb_v16_table_dim(vemb_v16_table_t *table) {
    return table->vector_dim;
}

uint32_t vemb_v16_table_stride(vemb_v16_table_t *table) {
    return table->vector_stride;
}

uint32_t vemb_v16_table_max_vectors(vemb_v16_table_t *table) {
    return table->max_vectors;
}

sve_gather_ctx_t *vemb_v16_table_gather_ctx(vemb_v16_table_t *table) {
    return &table->gather_ctx;
}

sve_operation_stats_t *vemb_v16_table_sve_stats(vemb_v16_table_t *table) {
    return &table->sve_stats;
}

int vemb_v16_table_lookup(vemb_v16_table_t *table,
                          const char *key,
                          uint32_t key_len,
                          uint64_t key_hash,
                          uint32_t *row_id) {
    if (key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN) {
        return -1;
    }

    uint32_t pos = (uint32_t)key_hash & table->hash_mask;
    for (uint32_t i = 0; i <= table->hash_mask; i++) {
        vemb_v16_table_entry_t *entry = &table->index[(pos + i) & table->hash_mask];
        if (!entry->used) return -1;
        if (entry->key_hash == key_hash &&
            entry->key_len == key_len &&
            memcmp(entry->key, key, key_len) == 0) {
            *row_id = entry->row_id;
            return 0;
        }
    }
    return -1;
}

int vemb_v16_table_upsert(vemb_v16_table_t *table,
                          const char *key,
                          uint32_t key_len,
                          uint64_t key_hash,
                          const float *vector,
                          uint32_t vector_bytes,
                          uint32_t *row_id) {
    if (key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN ||
        unlikely(vector_bytes != table->vector_stride) ||
        table->vector_region_size < (size_t)table->vector_stride * table->max_vectors) {
        return -1;
    }

    uint32_t pos = (uint32_t)key_hash & table->hash_mask;
    for (uint32_t i = 0; i <= table->hash_mask; i++) {
        vemb_v16_table_entry_t *entry = &table->index[(pos + i) & table->hash_mask];
        if (!entry->used) {
            uint32_t id = atomic_fetch_add_explicit(&table->next_row_id, 1,
                                                    memory_order_relaxed);
            if (id >= table->max_vectors) return -1;
            entry->used = 1;
            entry->key_hash = key_hash;
            entry->key_len = key_len;
            entry->row_id = id;
            memcpy(entry->key, key, key_len);
            *row_id = id;
            memcpy(table->vector_region + (size_t)id * table->vector_stride,
                   vector, vector_bytes);
            return 0;
        }
        if (entry->key_hash == key_hash &&
            entry->key_len == key_len &&
            memcmp(entry->key, key, key_len) == 0) {
            *row_id = entry->row_id;
            memcpy(table->vector_region + (size_t)entry->row_id * table->vector_stride,
                   vector, vector_bytes);
            return 0;
        }
    }
    return -1;
}
