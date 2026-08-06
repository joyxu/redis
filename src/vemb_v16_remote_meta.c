#include "vemb_v16_remote_meta.h"

#include "cpu_relax.h"
#include "macro.h"
#include "vemb_v16_cacheline.h"
#include "vemb_v16_hash.h"
#include "vemb_v16_util.h"

#include <stdatomic.h>
#include <string.h>

typedef char vemb_v16_remote_meta_header_size_must_be_64[
    sizeof(vemb_v16_remote_meta_header_t) == 64 ? 1 : -1];
typedef char vemb_v16_remote_meta_bucket_size_must_be_64[
    sizeof(vemb_v16_remote_meta_bucket_t) == 64 ? 1 : -1];
typedef char vemb_v16_remote_meta_entry_size_must_be_64[
    sizeof(vemb_v16_remote_meta_entry_t) == 64 ? 1 : -1];

static int is_power_of_two(uint32_t value) {
    return value && ((value & (value - 1u)) == 0);
}

static uint64_t mix_hash64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

static uint64_t fingerprint_key(const char *key, uint32_t key_len) {
    return vemb_v16_fnv1a64_bytes(key, key_len);
}

static int derive_legacy_shape(uint32_t requested_entry_count,
                               uint32_t requested_bucket_count,
                               uint32_t *set_count,
                               uint32_t *ways) {
    RETURN_IF(requested_entry_count == 0 || !set_count || !ways,
              VEMB_V16_REMOTE_META_INVALID);

    if (requested_bucket_count &&
        requested_bucket_count < requested_entry_count &&
        is_power_of_two(requested_bucket_count) &&
        requested_entry_count % requested_bucket_count == 0) {
        uint32_t derived_ways = requested_entry_count / requested_bucket_count;
        if (derived_ways > 0 &&
            derived_ways <= VEMB_V16_REMOTE_META_MAX_WAYS) {
            *set_count = requested_bucket_count;
            *ways = derived_ways;
            return VEMB_V16_REMOTE_META_OK;
        }
    }

    uint32_t derived_ways = VEMB_V16_REMOTE_META_DEFAULT_WAYS;
    if (derived_ways > requested_entry_count)
        derived_ways = requested_entry_count;
    if (derived_ways == 0)
        derived_ways = 1;
    uint32_t sets =
        vemb_v16_pow2_ceil_u32(
            (requested_entry_count + derived_ways - 1u) / derived_ways);
    RETURN_IF(sets == 0, VEMB_V16_REMOTE_META_INVALID);
    *set_count = sets;
    *ways = derived_ways;
    return VEMB_V16_REMOTE_META_OK;
}

static int layout_sets(vemb_v16_remote_meta_view_t *view,
                       void *base,
                       size_t bytes,
                       uint32_t set_count,
                       uint32_t ways) {
    RETURN_IF(!view || !base || !is_power_of_two(set_count) ||
              ways == 0 || ways > VEMB_V16_REMOTE_META_MAX_WAYS,
              VEMB_V16_REMOTE_META_INVALID);
    size_t entries_off =
        align_up_size(sizeof(vemb_v16_remote_meta_header_t),
                      CACHELINE_SIZE);
    size_t entry_count = (size_t)set_count * ways;
    size_t need =
        entries_off + entry_count * sizeof(vemb_v16_remote_meta_entry_t);

    RETURN_IF(bytes < need, VEMB_V16_REMOTE_META_INVALID);
    view->base = base;
    view->bytes = bytes;
    view->header = (void *)((uint8_t *)base);
    view->buckets = NULL;
    view->entries = (void *)((uint8_t *)base + entries_off);
    return VEMB_V16_REMOTE_META_OK;
}

size_t vemb_v16_remote_meta_layout_bytes_for_sets(uint32_t set_count,
                                                  uint32_t ways) {
    if (!is_power_of_two(set_count) ||
        ways == 0 ||
        ways > VEMB_V16_REMOTE_META_MAX_WAYS) {
        return 0;
    }
    size_t entries_off =
        align_up_size(sizeof(vemb_v16_remote_meta_header_t),
                      CACHELINE_SIZE);
    return entries_off +
           (size_t)set_count * ways * sizeof(vemb_v16_remote_meta_entry_t);
}

size_t vemb_v16_remote_meta_layout_bytes(uint32_t entry_count,
                                         uint32_t bucket_count) {
    uint32_t set_count = 0;
    uint32_t ways = 0;
    if (derive_legacy_shape(entry_count, bucket_count, &set_count, &ways) !=
        VEMB_V16_REMOTE_META_OK) {
        return 0;
    }
    return vemb_v16_remote_meta_layout_bytes_for_sets(set_count, ways);
}

int vemb_v16_remote_meta_init_sets(vemb_v16_remote_meta_view_t *view,
                                   void *base,
                                   size_t bytes,
                                   uint32_t owner_supernode_id,
                                   uint32_t value_size,
                                   uint32_t set_count,
                                   uint32_t ways) {
    RETURN_IF(layout_sets(view, base, bytes, set_count, ways) !=
              VEMB_V16_REMOTE_META_OK,
              VEMB_V16_REMOTE_META_INVALID);

    memset(base, 0, bytes);
    view->header->magic = VEMB_V16_REMOTE_META_MAGIC;
    view->header->version = VEMB_V16_REMOTE_META_VERSION;
    view->header->owner_supernode_id = owner_supernode_id;
    view->header->entry_count = set_count * ways;
    view->header->bucket_count = set_count;
    view->header->bucket_mask = set_count - 1u;
    view->header->value_size = value_size;
    view->header->flags = 0;
    view->header->generation = 0;
    atomic_init(&view->header->next_entry, 0);
    view->header->ways = ways;

    for (uint32_t i = 0; i < view->header->entry_count; i++) {
        atomic_init(&view->entries[i].version, 0);
        atomic_init(&view->entries[i].flags,
                    VEMB_V16_REMOTE_META_ENTRY_EMPTY);
    }
    return VEMB_V16_REMOTE_META_OK;
}

int vemb_v16_remote_meta_init(vemb_v16_remote_meta_view_t *view,
                              void *base,
                              size_t bytes,
                              uint32_t owner_supernode_id,
                              uint32_t value_size,
                              uint32_t entry_count,
                              uint32_t bucket_count) {
    uint32_t set_count = 0;
    uint32_t ways = 0;
    RETURN_IF(derive_legacy_shape(entry_count, bucket_count,
                                  &set_count, &ways) !=
              VEMB_V16_REMOTE_META_OK,
              VEMB_V16_REMOTE_META_INVALID);
    return vemb_v16_remote_meta_init_sets(view,
                                          base,
                                          bytes,
                                          owner_supernode_id,
                                          value_size,
                                          set_count,
                                          ways);
}

int vemb_v16_remote_meta_attach(vemb_v16_remote_meta_view_t *view,
                                void *base,
                                size_t bytes) {
    RETURN_IF(bytes < sizeof(vemb_v16_remote_meta_header_t),
              VEMB_V16_REMOTE_META_INVALID);
    vemb_v16_remote_meta_header_t *header = base;
    RETURN_IF(header->magic != VEMB_V16_REMOTE_META_MAGIC ||
              header->version != VEMB_V16_REMOTE_META_VERSION ||
              !is_power_of_two(header->bucket_count) ||
              header->bucket_mask != header->bucket_count - 1u ||
              header->entry_count == 0 ||
              header->ways == 0 ||
              header->ways > VEMB_V16_REMOTE_META_MAX_WAYS ||
              header->entry_count != header->bucket_count * header->ways,
              VEMB_V16_REMOTE_META_INVALID);
    return layout_sets(view, base, bytes, header->bucket_count, header->ways);
}

typedef struct vemb_v16_remote_meta_entry_snapshot {
    uint32_t version;
    uint32_t flags;
    uint64_t key_hash;
    uint64_t key_fingerprint;
} vemb_v16_remote_meta_entry_snapshot_t;

static int read_entry_snapshot(vemb_v16_remote_meta_entry_t *entry,
                               vemb_v16_remote_meta_entry_snapshot_t *snap) {
    uint32_t v1 =
        atomic_load_explicit(&entry->version, memory_order_acquire);
    if (v1 & 1u)
        return VEMB_V16_REMOTE_META_BUSY;

    uint32_t flags =
        atomic_load_explicit(&entry->flags, memory_order_acquire);
    uint64_t key_hash =
        atomic_load_explicit(&entry->key_hash, memory_order_relaxed);
    uint64_t key_fingerprint =
        atomic_load_explicit(&entry->key_fingerprint, memory_order_relaxed);

    atomic_thread_fence(memory_order_acquire);
    uint32_t v2 =
        atomic_load_explicit(&entry->version, memory_order_acquire);
    if (v1 != v2 || (v2 & 1u))
        return VEMB_V16_REMOTE_META_BUSY;

    snap->version = v1;
    snap->flags = flags;
    snap->key_hash = key_hash;
    snap->key_fingerprint = key_fingerprint;
    return VEMB_V16_REMOTE_META_OK;
}

static int begin_entry_write_version(vemb_v16_remote_meta_entry_t *entry,
                                     uint32_t expected_version,
                                     uint32_t *odd_version) {
    if (expected_version & 1u)
        return VEMB_V16_REMOTE_META_BUSY;
    uint32_t odd = expected_version + 1u;
    if ((odd & 1u) == 0)
        odd = 1u;
    if (!atomic_compare_exchange_strong_explicit(&entry->version,
                                                 &expected_version,
                                                 odd,
                                                 memory_order_acq_rel,
                                                 memory_order_acquire)) {
        return VEMB_V16_REMOTE_META_BUSY;
    }
    atomic_store_explicit(&entry->flags,
                          VEMB_V16_REMOTE_META_ENTRY_EVICTING,
                          memory_order_release);
    *odd_version = odd;
    return VEMB_V16_REMOTE_META_OK;
}

static void finish_entry_write(vemb_v16_remote_meta_entry_t *entry,
                               uint32_t odd_version,
                               uint64_t key_hash,
                               uint64_t key_fingerprint,
                               const vemb_v16_remote_meta_handle_t *handle) {
    atomic_store_explicit(&entry->key_hash,
                          key_hash,
                          memory_order_relaxed);
    atomic_store_explicit(&entry->key_fingerprint,
                          key_fingerprint,
                          memory_order_relaxed);
    atomic_store_explicit(&entry->region_id,
                          handle->region_id,
                          memory_order_relaxed);
    atomic_store_explicit(&entry->local_slot,
                          handle->local_slot,
                          memory_order_relaxed);
    atomic_store_explicit(&entry->owner_generation,
                          handle->owner_generation,
                          memory_order_relaxed);
    atomic_store_explicit(&entry->offset,
                          handle->offset,
                          memory_order_relaxed);

    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&entry->flags,
                          VEMB_V16_REMOTE_META_ENTRY_VALID |
                              VEMB_V16_REMOTE_META_ENTRY_CLOCK,
                          memory_order_release);
    atomic_store_explicit(&entry->version,
                          odd_version + 1u,
                          memory_order_release);
}

static uint32_t set_start(const vemb_v16_remote_meta_view_t *view,
                          uint32_t set_id) {
    return set_id * view->header->ways;
}

int vemb_v16_remote_meta_publish_with_result(
    vemb_v16_remote_meta_view_t *view,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    const vemb_v16_remote_meta_handle_t *handle,
    vemb_v16_remote_meta_publish_result_t *result) {
    RETURN_IF(!view || !view->header || !handle || key_len == 0,
              VEMB_V16_REMOTE_META_INVALID);
    vemb_v16_remote_meta_publish_result_t local = {
        .rc = VEMB_V16_REMOTE_META_INVALID,
        .set_id = UINT32_MAX,
        .way = UINT32_MAX,
        .action = VEMB_V16_REMOTE_META_PUBLISH_NONE,
    };

    uint64_t fp = fingerprint_key(key, key_len);
    uint32_t set_id =
        (uint32_t)mix_hash64(key_hash) & view->header->bucket_mask;
    local.set_id = set_id;
    uint32_t start = set_start(view, set_id);
    uint32_t empty_way = UINT32_MAX;
    uint32_t victim_way = UINT32_MAX;
    uint32_t busy = 0;

    for (uint32_t way = 0; way < view->header->ways; way++) {
        vemb_v16_remote_meta_entry_t *entry =
            &view->entries[start + way];
        vemb_v16_remote_meta_entry_snapshot_t snap;
        int snap_rc = read_entry_snapshot(entry, &snap);
        if (snap_rc == VEMB_V16_REMOTE_META_BUSY) {
            busy++;
            continue;
        }
        uint32_t state = snap.flags & VEMB_V16_REMOTE_META_ENTRY_STATE_MASK;
        if (state == VEMB_V16_REMOTE_META_ENTRY_VALID) {
            if (snap.key_hash == key_hash &&
                snap.key_fingerprint == fp) {
                uint32_t odd = 0;
                int rc = begin_entry_write_version(entry,
                                                   snap.version,
                                                   &odd);
                if (rc != VEMB_V16_REMOTE_META_OK) {
                    local.rc = rc;
                    if (result) *result = local;
                    return rc;
                }
                finish_entry_write(entry, odd, key_hash, fp, handle);
                local.rc = VEMB_V16_REMOTE_META_OK;
                local.way = way;
                local.action = VEMB_V16_REMOTE_META_PUBLISH_UPDATE;
                if (result) *result = local;
                return local.rc;
            }
            if (victim_way == UINT32_MAX &&
                !(snap.flags & VEMB_V16_REMOTE_META_ENTRY_CLOCK)) {
                victim_way = way;
            }
        } else if (state == VEMB_V16_REMOTE_META_ENTRY_EMPTY) {
            if (empty_way == UINT32_MAX)
                empty_way = way;
        } else {
            busy++;
        }
    }

    if (empty_way != UINT32_MAX) {
        vemb_v16_remote_meta_entry_t *entry =
            &view->entries[start + empty_way];
        vemb_v16_remote_meta_entry_snapshot_t snap;
        int snap_rc = read_entry_snapshot(entry, &snap);
        if (snap_rc != VEMB_V16_REMOTE_META_OK ||
            (snap.flags & VEMB_V16_REMOTE_META_ENTRY_STATE_MASK) !=
                VEMB_V16_REMOTE_META_ENTRY_EMPTY) {
            local.rc = VEMB_V16_REMOTE_META_BUSY;
            if (result) *result = local;
            return local.rc;
        }
        uint32_t odd = 0;
        int rc = begin_entry_write_version(entry, snap.version, &odd);
        if (rc != VEMB_V16_REMOTE_META_OK) {
            local.rc = rc;
            if (result) *result = local;
            return rc;
        }
        finish_entry_write(entry, odd, key_hash, fp, handle);
        atomic_fetch_add_explicit(&view->header->next_entry,
                                  1,
                                  memory_order_relaxed);
        local.rc = VEMB_V16_REMOTE_META_OK;
        local.way = empty_way;
        local.action = VEMB_V16_REMOTE_META_PUBLISH_INSERT;
        if (result) *result = local;
        return local.rc;
    }

    if (victim_way == UINT32_MAX) {
        for (uint32_t way = 0; way < view->header->ways; way++) {
            vemb_v16_remote_meta_entry_t *entry =
                &view->entries[start + way];
            vemb_v16_remote_meta_entry_snapshot_t snap;
            int snap_rc = read_entry_snapshot(entry, &snap);
            if (snap_rc == VEMB_V16_REMOTE_META_BUSY) {
                busy++;
                continue;
            }
            if ((snap.flags & VEMB_V16_REMOTE_META_ENTRY_STATE_MASK) ==
                VEMB_V16_REMOTE_META_ENTRY_VALID) {
                atomic_fetch_and_explicit(
                    &entry->flags,
                    ~VEMB_V16_REMOTE_META_ENTRY_CLOCK,
                    memory_order_acq_rel);
                if (victim_way == UINT32_MAX)
                    victim_way = way;
            }
        }
    }

    if (victim_way == UINT32_MAX) {
        local.rc = busy ? VEMB_V16_REMOTE_META_BUSY :
            VEMB_V16_REMOTE_META_FULL;
        if (result) *result = local;
        return local.rc;
    }

    vemb_v16_remote_meta_entry_t *entry =
        &view->entries[start + victim_way];
    vemb_v16_remote_meta_entry_snapshot_t snap;
    int snap_rc = read_entry_snapshot(entry, &snap);
    if (snap_rc != VEMB_V16_REMOTE_META_OK ||
        (snap.flags & VEMB_V16_REMOTE_META_ENTRY_STATE_MASK) !=
            VEMB_V16_REMOTE_META_ENTRY_VALID) {
        local.rc = VEMB_V16_REMOTE_META_BUSY;
        if (result) *result = local;
        return local.rc;
    }
    uint32_t odd = 0;
    int rc = begin_entry_write_version(entry, snap.version, &odd);
    if (rc != VEMB_V16_REMOTE_META_OK) {
        local.rc = rc;
        if (result) *result = local;
        return rc;
    }
    finish_entry_write(entry, odd, key_hash, fp, handle);
    local.rc = VEMB_V16_REMOTE_META_OK;
    local.way = victim_way;
    local.action = VEMB_V16_REMOTE_META_PUBLISH_EVICT;
    if (result) *result = local;
    return local.rc;
}

int vemb_v16_remote_meta_publish(vemb_v16_remote_meta_view_t *view,
                                 const char *key,
                                 uint32_t key_len,
                                 uint64_t key_hash,
                                 const vemb_v16_remote_meta_handle_t *handle) {
    return vemb_v16_remote_meta_publish_with_result(view,
                                                    key,
                                                    key_len,
                                                    key_hash,
                                                    handle,
                                                    NULL);
}

static int read_entry(vemb_v16_remote_meta_entry_t *entry,
                      uint64_t key_hash,
                      uint64_t key_fingerprint,
                      uint32_t retry_budget,
                      vemb_v16_remote_meta_handle_t *handle) {
    if (retry_budget == 0)
        retry_budget = VEMB_V16_REMOTE_META_DEFAULT_RETRIES;
    for (uint32_t i = 0; i < retry_budget; i++) {
        uint32_t v1 =
            atomic_load_explicit(&entry->version, memory_order_acquire);
        if (v1 & 1u) {
            cpu_relax();
            continue;
        }

        uint32_t flags =
            atomic_load_explicit(&entry->flags, memory_order_acquire);
        uint32_t state = flags & VEMB_V16_REMOTE_META_ENTRY_STATE_MASK;
        if (state == VEMB_V16_REMOTE_META_ENTRY_EMPTY)
            return VEMB_V16_REMOTE_META_NOT_FOUND;
        if (state != VEMB_V16_REMOTE_META_ENTRY_VALID) {
            cpu_relax();
            continue;
        }

        uint64_t entry_hash =
            atomic_load_explicit(&entry->key_hash, memory_order_relaxed);
        uint64_t entry_fp =
            atomic_load_explicit(&entry->key_fingerprint, memory_order_relaxed);
        uint32_t region_id =
            atomic_load_explicit(&entry->region_id, memory_order_relaxed);
        uint32_t local_slot =
            atomic_load_explicit(&entry->local_slot, memory_order_relaxed);
        uint64_t owner_generation =
            atomic_load_explicit(&entry->owner_generation,
                                 memory_order_relaxed);
        uint64_t offset =
            atomic_load_explicit(&entry->offset, memory_order_relaxed);

        atomic_thread_fence(memory_order_acquire);

        uint32_t v2 =
            atomic_load_explicit(&entry->version, memory_order_acquire);
        if (v1 == v2 && !(v2 & 1u)) {
            if (entry_hash != key_hash || entry_fp != key_fingerprint)
                return VEMB_V16_REMOTE_META_NOT_FOUND;
            *handle = (vemb_v16_remote_meta_handle_t){
                .region_id = region_id,
                .bytes = 0,
                .local_slot = local_slot,
                .offset = offset,
                .key_hash = key_hash,
                .owner_generation = owner_generation,
            };
            atomic_fetch_or_explicit(&entry->flags,
                                     VEMB_V16_REMOTE_META_ENTRY_CLOCK,
                                     memory_order_acq_rel);
            return VEMB_V16_REMOTE_META_OK;
        }
        cpu_relax();
    }
    return VEMB_V16_REMOTE_META_BUSY;
}

int vemb_v16_remote_meta_lookup_with_result(
    vemb_v16_remote_meta_view_t *view,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint32_t retry_budget,
    vemb_v16_remote_meta_handle_t *handle,
    vemb_v16_remote_meta_lookup_result_t *result) {
    RETURN_IF(!view || !view->header || !handle || key_len == 0,
              VEMB_V16_REMOTE_META_INVALID);
    vemb_v16_remote_meta_lookup_result_t local = {
        .rc = VEMB_V16_REMOTE_META_NOT_FOUND,
        .set_id = UINT32_MAX,
        .way = UINT32_MAX,
        .probes = 0,
    };

    uint64_t fp = fingerprint_key(key, key_len);
    uint32_t set_id =
        (uint32_t)mix_hash64(key_hash) & view->header->bucket_mask;
    local.set_id = set_id;
    uint32_t start = set_start(view, set_id);
    int saw_busy = 0;

    for (uint32_t way = 0; way < view->header->ways; way++) {
        local.probes++;
        vemb_v16_remote_meta_entry_t *entry =
            &view->entries[start + way];
        int rc = read_entry(entry,
                            key_hash,
                            fp,
                            retry_budget,
                            handle);
        if (rc == VEMB_V16_REMOTE_META_BUSY)
            saw_busy = 1;
        if (rc == VEMB_V16_REMOTE_META_NOT_FOUND)
            continue;
        local.rc = rc;
        local.way = way;
        if (rc == VEMB_V16_REMOTE_META_OK)
            handle->bytes = view->header->value_size;
        if (result) *result = local;
        return rc;
    }

    local.rc = saw_busy ? VEMB_V16_REMOTE_META_BUSY :
        VEMB_V16_REMOTE_META_NOT_FOUND;
    if (result) *result = local;
    return local.rc;
}

int vemb_v16_remote_meta_lookup(vemb_v16_remote_meta_view_t *view,
                                const char *key,
                                uint32_t key_len,
                                uint64_t key_hash,
                                uint32_t retry_budget,
                                vemb_v16_remote_meta_handle_t *handle) {
    return vemb_v16_remote_meta_lookup_with_result(view,
                                                   key,
                                                   key_len,
                                                   key_hash,
                                                   retry_budget,
                                                   handle,
                                                   NULL);
}
