#include "ub_metadata.h"

#include "macro.h"
#include "server.h"
#include "zmalloc.h"

#include <errno.h>
#include <inttypes.h>
#include <string.h>

static ub_metadata_registry_t g_ub_metadata = {0};

static uint64_t *ub_metadata_uint64_dup(uint64_t value) {
    uint64_t *copy = zmalloc(sizeof(uint64_t));
    if (!copy) return NULL;
    *copy = value;
    return copy;
}

static void ub_metadata_free_uint64(dict *d, void *ptr) {
    UNUSED(d);
    zfree(ptr);
}

static void ub_metadata_free_row_entry(dict *d, void *ptr) {
    UNUSED(d);
    ub_row_entry_t *entry = ptr;
    if (!entry) return;
    sdsfree(entry->element);
    zfree(entry);
}

static uint64_t ub_metadata_dict_uint64_key(const void *key) {
    return dictGenHashFunction(key, sdslen((const sds)key));
}

static int ub_metadata_dict_uint64_key_compare(dictCmpCache *cache, const void *key1, const void *key2) {
    UNUSED(cache);
    return sdscmp((const sds)key1, (const sds)key2) == 0;
}

static void ub_metadata_dict_key_destructor(dict *d, void *key) {
    UNUSED(d);
    sdsfree(key);
}

static dictType ub_metadata_dict_type = {
    .hashFunction = ub_metadata_dict_uint64_key,
    .keyDup = NULL,
    .valDup = NULL,
    .keyCompare = ub_metadata_dict_uint64_key_compare,
    .keyDestructor = ub_metadata_dict_key_destructor,
    .valDestructor = ub_metadata_free_uint64,
    .resizeAllowed = NULL,
    .rehashingStarted = NULL,
    .rehashingCompleted = NULL,
    .bucketChanged = NULL,
    .dictMetadataBytes = NULL,
    .userdata = NULL,
};

static dictType ub_metadata_row_dict_type = {
    .hashFunction = ub_metadata_dict_uint64_key,
    .keyDup = NULL,
    .valDup = NULL,
    .keyCompare = ub_metadata_dict_uint64_key_compare,
    .keyDestructor = ub_metadata_dict_key_destructor,
    .valDestructor = ub_metadata_free_row_entry,
    .resizeAllowed = NULL,
    .rehashingStarted = NULL,
    .rehashingCompleted = NULL,
    .bucketChanged = NULL,
    .dictMetadataBytes = NULL,
    .userdata = NULL,
};

static ub_vector_set_meta_t *ub_metadata_create_set(const char *key, size_t dim) {
    ub_vector_set_meta_t *set = zcalloc(sizeof(*set));
    if (!set) return NULL;

    set->key = sdsnew(key);
    set->dim = dim;
    set->cardinality = 0;
    set->next_row_id = 0;
    set->element_to_row = dictCreate(&ub_metadata_dict_type);
    set->row_to_element = dictCreate(&ub_metadata_row_dict_type);
    if (!set->key || !set->element_to_row || !set->row_to_element ||
        pthread_rwlock_init(&set->lock, NULL) != 0) {
        if (set->element_to_row) dictRelease(set->element_to_row);
        if (set->row_to_element) dictRelease(set->row_to_element);
        sdsfree(set->key);
        zfree(set);
        return NULL;
    }

    return set;
}

static void ub_metadata_release_set(void *ptr) {
    ub_vector_set_meta_t *set = ptr;
    if (!set) return;

    dictRelease(set->element_to_row);
    dictRelease(set->row_to_element);
    pthread_rwlock_destroy(&set->lock);
    sdsfree(set->key);
    zfree(set);
}

static void ub_metadata_registry_val_destructor(dict *d, void *val) {
    UNUSED(d);
    ub_metadata_release_set(val);
}

static dictType ub_metadata_registry_dict_type = {
    .hashFunction = ub_metadata_dict_uint64_key,
    .keyDup = NULL,
    .valDup = NULL,
    .keyCompare = ub_metadata_dict_uint64_key_compare,
    .keyDestructor = ub_metadata_dict_key_destructor,
    .valDestructor = ub_metadata_registry_val_destructor,
    .resizeAllowed = NULL,
    .rehashingStarted = NULL,
    .rehashingCompleted = NULL,
    .bucketChanged = NULL,
    .dictMetadataBytes = NULL,
    .userdata = NULL,
};

static sds ub_metadata_row_key(uint64_t row_id) {
    return sdscatprintf(sdsempty(), "%" PRIu64, row_id);
}

int ub_metadata_init(void) {
    if (g_ub_metadata.initialized) return C_OK;

    g_ub_metadata.sets = dictCreate(&ub_metadata_registry_dict_type);
    if (!g_ub_metadata.sets) return C_ERR;
    if (pthread_rwlock_init(&g_ub_metadata.lock, NULL) != 0) {
        dictRelease(g_ub_metadata.sets);
        g_ub_metadata.sets = NULL;
        return C_ERR;
    }
    g_ub_metadata.initialized = 1;
    return C_OK;
}

void ub_metadata_cleanup(void) {
    if (!g_ub_metadata.initialized) return;

    dictRelease(g_ub_metadata.sets);
    pthread_rwlock_destroy(&g_ub_metadata.lock);
    memset(&g_ub_metadata, 0, sizeof(g_ub_metadata));
}

ub_vector_set_meta_t *ub_metadata_get_set(const char *key) {
    ub_vector_set_meta_t *set = NULL;

    if (!g_ub_metadata.initialized || !key) return NULL;

    pthread_rwlock_rdlock(&g_ub_metadata.lock);
    dictEntry *de = dictFind(g_ub_metadata.sets, (void *)key);
    if (de) set = dictGetVal(de);
    pthread_rwlock_unlock(&g_ub_metadata.lock);
    return set;
}

ub_vector_set_meta_t *ub_metadata_get_or_create_set(const char *key, size_t dim) {
    ub_vector_set_meta_t *set = NULL;

    if (!key || dim == 0) return NULL;
    if (!g_ub_metadata.initialized && ub_metadata_init() != C_OK) return NULL;

    pthread_rwlock_wrlock(&g_ub_metadata.lock);
    dictEntry *de = dictFind(g_ub_metadata.sets, (void *)key);
    if (de) {
        set = dictGetVal(de);
        pthread_rwlock_unlock(&g_ub_metadata.lock);
        if (set && unlikely(set->dim != dim)) return NULL;
        return set;
    }

    set = ub_metadata_create_set(key, dim);
    if (!set) {
        pthread_rwlock_unlock(&g_ub_metadata.lock);
        return NULL;
    }

    sds key_copy = sdsdup(set->key);
    if (!key_copy || dictAdd(g_ub_metadata.sets, key_copy, set) != DICT_OK) {
        sdsfree(key_copy);
        ub_metadata_release_set(set);
        pthread_rwlock_unlock(&g_ub_metadata.lock);
        return NULL;
    }

    pthread_rwlock_unlock(&g_ub_metadata.lock);
    return set;
}

int ub_metadata_lookup_row(ub_vector_set_meta_t *set, const char *element, uint64_t *row_id) {
    if (!set || !element || !row_id) return C_ERR;

    pthread_rwlock_rdlock(&set->lock);
    dictEntry *de = dictFind(set->element_to_row, (void *)element);
    if (!de) {
        pthread_rwlock_unlock(&set->lock);
        return C_ERR;
    }
    *row_id = *(uint64_t *)dictGetVal(de);
    pthread_rwlock_unlock(&set->lock);
    return C_OK;
}

int ub_metadata_alloc_row(ub_vector_set_meta_t *set, const char *element, uint64_t *row_id) {
    if (!set || !element || !row_id) return C_ERR;

    pthread_rwlock_wrlock(&set->lock);

    dictEntry *existing = dictFind(set->element_to_row, (void *)element);
    if (existing) {
        *row_id = *(uint64_t *)dictGetVal(existing);
        pthread_rwlock_unlock(&set->lock);
        return C_OK;
    }

    uint64_t assigned = set->next_row_id++;
    uint64_t *row_copy = ub_metadata_uint64_dup(assigned);
    ub_row_entry_t *entry = zcalloc(sizeof(*entry));
    sds elem_copy = sdsnew(element);
    sds row_key = ub_metadata_row_key(assigned);
    if (!row_copy || !entry || !elem_copy || !row_key) {
        zfree(row_copy);
        zfree(entry);
        sdsfree(elem_copy);
        sdsfree(row_key);
        pthread_rwlock_unlock(&set->lock);
        return C_ERR;
    }

    entry->row_id = assigned;
    entry->element = elem_copy;
    entry->active = 1;

    if (dictAdd(set->element_to_row, sdsdup(elem_copy), row_copy) != DICT_OK ||
        dictAdd(set->row_to_element, row_key, entry) != DICT_OK) {
        if (dictFind(set->element_to_row, elem_copy) == NULL) {
            ub_metadata_free_uint64(NULL, row_copy);
        }
        if (dictFind(set->row_to_element, row_key) == NULL) {
            ub_metadata_free_row_entry(NULL, entry);
            sdsfree(row_key);
        }
        pthread_rwlock_unlock(&set->lock);
        return C_ERR;
    }

    set->cardinality++;
    *row_id = assigned;
    pthread_rwlock_unlock(&set->lock);
    return C_OK;
}

int ub_metadata_remove_row(ub_vector_set_meta_t *set, const char *element, uint64_t *row_id) {
    if (!set || !element) return C_ERR;

    pthread_rwlock_wrlock(&set->lock);
    dictEntry *de = dictFind(set->element_to_row, (void *)element);
    if (!de) {
        pthread_rwlock_unlock(&set->lock);
        return C_ERR;
    }

    uint64_t removed = *(uint64_t *)dictGetVal(de);
    sds row_key = ub_metadata_row_key(removed);
    dictDelete(set->element_to_row, (void *)element);
    dictDelete(set->row_to_element, row_key);
    sdsfree(row_key);
    if (set->cardinality > 0) set->cardinality--;
    if (row_id) *row_id = removed;
    pthread_rwlock_unlock(&set->lock);
    return C_OK;
}

size_t ub_metadata_cardinality(const ub_vector_set_meta_t *set) {
    size_t cardinality;

    if (!set) return 0;

    pthread_rwlock_rdlock((pthread_rwlock_t *)&set->lock);
    cardinality = set->cardinality;
    pthread_rwlock_unlock((pthread_rwlock_t *)&set->lock);
    return cardinality;
}

size_t ub_metadata_collect_rows(ub_vector_set_meta_t *set,
                                uint64_t **rows_out,
                                sds **elements_out) {
    if (!set || !rows_out) return 0;

    *rows_out = NULL;
    if (elements_out) *elements_out = NULL;

    pthread_rwlock_rdlock(&set->lock);
    size_t count = set->cardinality;
    if (count == 0) {
        pthread_rwlock_unlock(&set->lock);
        return 0;
    }

    uint64_t *rows = zmalloc(sizeof(uint64_t) * count);
    sds *elements = elements_out ? zmalloc(sizeof(sds) * count) : NULL;
    if (!rows || (elements_out && !elements)) {
        zfree(rows);
        zfree(elements);
        pthread_rwlock_unlock(&set->lock);
        return 0;
    }

    dictIterator *iter = dictGetIterator(set->element_to_row);
    dictEntry *de;
    size_t i = 0;
    while ((de = dictNext(iter)) != NULL && i < count) {
        rows[i] = *(uint64_t *)dictGetVal(de);
        if (elements) {
            elements[i] = sdsdup((const sds)dictGetKey(de));
            if (!elements[i]) {
                while (i > 0) sdsfree(elements[--i]);
                zfree(rows);
                zfree(elements);
                dictReleaseIterator(iter);
                pthread_rwlock_unlock(&set->lock);
                return 0;
            }
        }
        i++;
    }
    dictReleaseIterator(iter);
    pthread_rwlock_unlock(&set->lock);

    *rows_out = rows;
    if (elements_out) *elements_out = elements;
    return i;
}
