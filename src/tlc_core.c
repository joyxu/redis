#include "tlc_core.h"
#include "config.h"
#include "cpu_relax.h"
#include "macro.h"
#include "sve_operation.h"
#include "vemb_v16_hash.h"
#include "vemb_v16_log.h"
#include "vemb_v16_protocol.h"
#include "vemb_v16_util.h"
#include "zmalloc.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#define TLC_CORE_HOT_PROBES 4u
/*
 * The warm/cold hash tables stay intentionally sparse, but realistic bench
 * keysets can still create 10+ entry linear-probe runs. A slightly larger
 * probe window avoids spilling valid warm keys into the cold path and later
 * surfacing them as false NOT_FOUND responses.
 */
#define TLC_CORE_COLD_PROBES 16u
#define TLC_CORE_INVALID_OFFSET UINT64_MAX
#define TLC_CORE_WARM_WAYS 8u
#define TLC_CORE_WARM_PLACE_RETRIES 3u
#define TLC_CORE_WARM_BUSY_RETRIES 1024u
#define TLC_CORE_DIAG_STALE_LOG_LIMIT 64u
#define TLC_CORE_DIAG_PUT_LOG_LIMIT 128u
#define TLC_CORE_KEY_META_SHARDS 256u
#define TLC_CORE_KEY_META_MIN_SHARD_CAPACITY 64u
#define TLC_CORE_LOCATION_CACHE_PROBES 4u
#define TLC_CORE_LOCATION_CACHE_MIN_CAPACITY 1024u
#define TLC_CORE_LOCATION_CACHE_KEY_WORDS \
    ((VEMB_V16_MAX_KEY_LEN + sizeof(uint64_t) - 1u) / sizeof(uint64_t))

#define SEQLOCK_STATE_STABLE UINT64_C(0)
#define SEQLOCK_STATE_WRITING UINT64_C(1)
#define SEQLOCK_STATE_MASK UINT64_C(1)
#define SEQLOCK_STATE_STEP UINT64_C(1)
#define SEQLOCK_VERSION_STEP UINT64_C(2)
#define SEQLOCK_EMPTY UINT64_C(0)

typedef enum tlc_core_entry_state {
    TLC_CORE_ENTRY_EMPTY = 0,
    TLC_CORE_ENTRY_VALID = 1,
    TLC_CORE_ENTRY_DIRTY = 2,
    TLC_CORE_ENTRY_EXPIRED = 3,
} tlc_core_entry_state_t;

typedef enum tlc_core_warm_verify_rc {
    TLC_CORE_WARM_VERIFY_OK = 0,   // Slot matches and is safe to use.
    TLC_CORE_WARM_VERIFY_MISS = 1, // Slot is absent, stale, or no longer matches.
    TLC_CORE_WARM_VERIFY_BUSY = 2, // Slot is being updated; caller may retry.
} tlc_core_warm_verify_rc_t;

typedef struct tlc_core_hot_entry {
    atomic_uint_fast64_t key_hash;
    atomic_int warm_idx;
    uint32_t _pad;
} tlc_core_hot_entry_t;

typedef struct tlc_core_hold_layer {
    tlc_core_hot_entry_t *table;
    uint32_t capacity;
    uint32_t mask;
    atomic_uint_fast64_t hits;
    atomic_uint_fast64_t misses;
} tlc_core_hold_layer_t;

typedef struct tlc_core_location_cache_entry {
    atomic_uint_fast64_t seq;
    atomic_uint_fast64_t key_hash;
    atomic_uint_fast32_t key_len;
    atomic_uint_fast32_t region_id;
    atomic_uint_fast32_t region_index;
    atomic_uint_fast32_t local_slot;
    atomic_uint_fast32_t bytes;
    atomic_uint_fast64_t offset;
    atomic_uint_fast64_t owner_generation;
    atomic_uint_fast64_t key_words[TLC_CORE_LOCATION_CACHE_KEY_WORDS];
} tlc_core_location_cache_entry_t;

typedef struct tlc_core_location_cache {
    tlc_core_location_cache_entry_t *entries;
    uint32_t capacity;
    uint32_t mask;
} tlc_core_location_cache_t;

typedef struct tlc_core_warm_entry {
    /* TODO: evaluate using uint64_t key_hash as the canonical key, matching three_layer_cache_ub. */
    uint64_t key_hash;
    /* TODO: wire these into the warm eviction/expiration policy. */
    // uint64_t write_ts_ns;
    // uint64_t ttl_ns;
    uint32_t key_len;
    uint32_t value_size;
    tlc_warm_location_t location;
    atomic_uint_fast32_t access_count;
    char key[VEMB_V16_MAX_KEY_LEN];
    atomic_int state;
} tlc_core_warm_entry_t;

typedef struct tlc_core_key_meta_entry {
    int occupied;
    uint32_t key_len;
    uint64_t key_hash;
    uint64_t key_version;
    uint64_t topology_epoch;
    uint64_t owner_epoch;
    uint32_t migration_state;
    uint32_t source_owner;
    uint32_t target_owner;
    uint32_t tombstone;
    uint32_t shard_id;
    tlc_warm_location_t location;
    char key[VEMB_V16_MAX_KEY_LEN];
} tlc_core_key_meta_entry_t;

typedef struct tlc_core_key_meta_shard {
    tlc_core_key_meta_entry_t *entries;
    uint32_t capacity;
    uint32_t mask;
    uint32_t count;
    uint32_t lock_id;
} tlc_core_key_meta_shard_t;

typedef struct tlc_core_warm_region_runtime {
    uint32_t region_id;
    uint32_t backend_type;
    uint32_t is_local;
    uint32_t weight;
    uint32_t value_size;
    uint32_t capacity_slots;
    vemb_v16_warm_slot_meta_t *slot_meta;
    uint64_t region_bytes;
    uint8_t *mapped_addr;
} tlc_core_warm_region_runtime_t;

typedef struct tlc_core_warm_region_vnode {
    uint32_t hash_val;
    uint32_t region_index;
} tlc_core_warm_region_vnode_t;

typedef struct tlc_core_warm_layer {
    tlc_core_warm_entry_t *entries;
    int32_t *hash_table;
    tlc_core_warm_region_runtime_t *regions;
    atomic_uint_fast32_t runtime_region_count;
    tlc_core_warm_region_runtime_t runtime_regions[TLC_CORE_MAX_WARM_REGIONS];
    tlc_core_warm_region_vnode_t *vnodes;
    atomic_uint_fast32_t count;
    uint32_t capacity;
    uint32_t hash_capacity;
    uint32_t region_count;
    uint32_t vnode_count;
    uint32_t mask;
    state_bitmap_t locks;
    atomic_uint_fast64_t hits;
    atomic_uint_fast64_t misses;
} tlc_core_warm_layer_t;

typedef struct tlc_core_cold_record {
    /* TODO: evaluate using uint64_t key_hash as the canonical key, matching three_layer_cache_ub. */
    uint64_t key_hash;
    uint64_t offset;
    uint32_t key_len;
    uint32_t value_size;
    char key[VEMB_V16_MAX_KEY_LEN];
} tlc_core_cold_record_t;

typedef struct tlc_core_cold_segment {
    tlc_core_cold_record_t *records;
    uint8_t *values;
    uint64_t base_offset;
    size_t count;
    size_t capacity;
} tlc_core_cold_segment_t;

typedef struct tlc_core_cold_layer {
    tlc_core_cold_segment_t *segments;
    atomic_int num_segments;
    atomic_uint_fast64_t next_offset;
    atomic_uint_fast64_t *offset_index;
    uint32_t offset_index_size;
    uint32_t oi_mask;
    uint32_t max_segments;
    size_t segment_capacity;
    state_bitmap_t locks;
    atomic_uint_fast64_t hits;
    atomic_uint_fast64_t misses;
} tlc_core_cold_layer_t;

struct tlc_core {
    uint32_t value_size;
    uint32_t warm_capacity;
    tlc_core_hold_layer_t hold;
    tlc_core_location_cache_t location_cache;
    tlc_core_warm_layer_t warm;
    tlc_core_cold_layer_t cold;
    atomic_uint_fast64_t warm_alloc_local;
    atomic_uint_fast64_t warm_alloc_remote;
    atomic_uint_fast64_t warm_alloc_fallback;
    atomic_uint_fast64_t warm_alloc_cold_spill;
    atomic_uint_fast64_t warm_alloc_fail;
    atomic_uint_fast64_t warm_eviction_success;
    atomic_uint_fast64_t warm_eviction_fail;
    atomic_uint_fast64_t warm_same_key_overwrite;
    atomic_uint_fast64_t warm_stale_handle_reject;
    atomic_uint_fast64_t remote_meta_stale;
    atomic_uint_fast64_t lookup_cache_hit;
    atomic_uint_fast64_t lookup_cache_miss;
    atomic_uint_fast64_t lookup_warm_local_hit;
    atomic_uint_fast64_t lookup_warm_imported_hit;
    atomic_uint_fast64_t lookup_cold_promote;
    atomic_uint_fast64_t lookup_final_miss;
    atomic_uint_fast64_t region_lookup_hits[TLC_CORE_MAX_TOTAL_WARM_REGIONS];
    atomic_uint_fast64_t region_cold_promotes[TLC_CORE_MAX_TOTAL_WARM_REGIONS];
    atomic_uint_fast64_t source_fence_active_count;
    atomic_uint_fast64_t tombstone_active_count;
    tlc_core_key_meta_shard_t *key_meta_shards;
    uint32_t key_meta_shard_count;
    uint32_t key_meta_capacity;
    state_bitmap_t key_meta_locks;
    atomic_uint_fast32_t key_meta_count;
};

static const tlc_warm_location_t tlc_invalid_location = {
    .region_id = TLC_CORE_INVALID_REGION_ID,
    .region_index = UINT32_MAX,
    .local_slot = TLC_CORE_INVALID_SLOT,
    .bytes = 0,
    .offset = UINT64_MAX,
    .owner_generation = 0,
};

static void tlc_core_log_put_failure(
        const char *reason,
        const tlc_core_t *core,
        uint64_t key_hash,
        uint32_t key_len,
        uint32_t value_size,
        uint64_t topology_epoch,
        int enforce_epoch,
        const tlc_core_key_meta_entry_t *meta) {

    uint64_t source_fence_count = atomic_load_explicit(
        &core->source_fence_active_count, memory_order_acquire);
    uint32_t key_meta_count = atomic_load_explicit(
        &core->key_meta_count, memory_order_relaxed);
    uint32_t meta_present = 0;
    uint32_t meta_state = 0;
    uint64_t meta_epoch = 0;
    uint64_t owner_epoch = 0;
    uint64_t key_version = 0;
    uint32_t source_owner = UINT32_MAX;
    uint32_t target_owner = UINT32_MAX;
    uint32_t tombstone = 0;
    uint32_t shard_id = UINT32_MAX;
    if (meta) {
        meta_present = 1;
        meta_state = meta->migration_state;
        meta_epoch = meta->topology_epoch;
        owner_epoch = meta->owner_epoch;
        key_version = meta->key_version;
        source_owner = meta->source_owner;
        target_owner = meta->target_owner;
        tombstone = meta->tombstone;
        shard_id = meta->shard_id;
    }
    serverLog(LL_WARNING,
              "tlc_core put_with_epoch failed: reason=%s key_hash=%llu key_len=%u value_size=%u expected_value_size=%u topology_epoch=%llu enforce_epoch=%d key_meta_count=%u/%u source_fence_count=%llu meta_present=%d meta_state=%u meta_epoch=%llu owner_epoch=%llu key_version=%llu source=%u target=%u tombstone=%u shard=%u",
              reason,
              (unsigned long long)key_hash,
              key_len,
              value_size,
              core->value_size,
              (unsigned long long)topology_epoch,
              enforce_epoch,
              key_meta_count,
              core->key_meta_capacity,
              (unsigned long long)source_fence_count,
              meta_present,
              meta_state,
              (unsigned long long)meta_epoch,
              (unsigned long long)owner_epoch,
              (unsigned long long)key_version,
              source_owner,
              target_owner,
              tombstone,
              shard_id);
}

static uint32_t warm_region_used_slots(const tlc_core_warm_region_runtime_t *region) {
    uint32_t used = 0;
    for (uint32_t i = 0; i < region->capacity_slots; i++) {
        if (atomic_load_explicit(&region->slot_meta[i].state,
              memory_order_acquire) != VEMB_V16_WARM_SLOT_FREE) {
            used++;
        }
    }
    return used;
}

static uint32_t warm_region_full(
        const tlc_core_warm_region_runtime_t *region) {
    return warm_region_used_slots(region) >= region->capacity_slots;
}

static int seqlock_seq_is_writing(uint64_t seq) {
    return (seq & SEQLOCK_STATE_MASK) == SEQLOCK_STATE_WRITING;
}

static int seqlock_seq_is_stable(uint64_t seq) {
    return (seq & SEQLOCK_STATE_MASK) == SEQLOCK_STATE_STABLE;
}

static int vnode_compare(const void *a, const void *b) {
    const tlc_core_warm_region_vnode_t *va = a;
    const tlc_core_warm_region_vnode_t *vb = b;
    if (va->hash_val < vb->hash_val) return -1;
    if (va->hash_val > vb->hash_val) return 1;
    if (va->region_index < vb->region_index) return -1;
    if (va->region_index > vb->region_index) return 1;
    return 0;
}

static uint32_t vnode_lower_bound(const tlc_core_warm_layer_t *warm,
                                  uint32_t hash_val) {
    uint32_t lo = 0;
    uint32_t hi = warm->vnode_count;
    while (lo < hi) {
        uint32_t mid = lo + ((hi - lo) >> 1);
        if (warm->vnodes[mid].hash_val < hash_val)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo == warm->vnode_count ? 0 : lo;
}

static int key_valid(const char * _, uint32_t key_len) {
    return likely(key_len > 0 && key_len <= VEMB_V16_MAX_KEY_LEN);
}

static uint32_t warm_runtime_region_count(const tlc_core_warm_layer_t *warm) {
    return (uint32_t)atomic_load_explicit(
        (const atomic_uint_fast32_t *)&warm->runtime_region_count,
        memory_order_acquire);
}

static tlc_core_warm_region_runtime_t *warm_region_by_index(
        tlc_core_warm_layer_t *warm,
        uint32_t region_index) {
    if (region_index < warm->region_count)
        return &warm->regions[region_index];
    uint32_t runtime_index = region_index - warm->region_count;
    uint32_t runtime_count = warm_runtime_region_count(warm);
    if (runtime_index < runtime_count)
        return &warm->runtime_regions[runtime_index];
    return NULL;
}

static int warm_region_init_one(tlc_core_t *core,
                                tlc_core_warm_region_runtime_t *dst,
                                const tlc_core_warm_region_config_t *src,
                                uint32_t region_index) {
    RETURN_IF(!src->slot_meta ||
              src->value_size != core->value_size ||
              src->region_bytes < src->value_size,
              -1);
    uint64_t capacity_slots = src->region_bytes / src->value_size;
    RETURN_IF(capacity_slots > UINT32_MAX, -1);
    uint32_t weight = src->weight ? src->weight : 1u;

    memset(dst, 0, sizeof(*dst));
    *dst = (tlc_core_warm_region_runtime_t){
        .region_id = src->region_id,
        .backend_type = src->backend_type,
        .is_local = src->is_local,
        .weight = weight,
        .value_size = src->value_size,
        .capacity_slots = (uint32_t)capacity_slots,
        .slot_meta = src->slot_meta,
        .region_bytes = src->region_bytes,
        .mapped_addr = src->mapped_addr,
    };
    serverLog(LL_NOTICE,
              "tlc warm region init: region_index=%u region_id=%u backend_type=%u is_local=%u weight=%u value_size=%u region_bytes=%llu capacity_slots=%u mapped_addr=%p slot_meta=%p",
              region_index,
              dst->region_id,
              dst->backend_type,
              dst->is_local,
              dst->weight,
              dst->value_size,
              (unsigned long long)dst->region_bytes,
              dst->capacity_slots,
              dst->mapped_addr,
              (void *)dst->slot_meta);
    return 0;
}

static int key_matches(uint64_t key_hash,
                       const char *key,
                       uint32_t key_len,
                       uint64_t entry_hash,
                       const char *entry_key,
                       uint32_t entry_key_len) {
    return entry_hash == key_hash &&
           entry_key_len == key_len &&
           memcmp(entry_key, key, key_len) == 0;
}

static int key_meta_init(tlc_core_t *core) {
    uint32_t shard_count = TLC_CORE_KEY_META_SHARDS;
    uint64_t target_capacity = (uint64_t)core->warm_capacity * 2u;
    uint64_t per_shard_target = (target_capacity + shard_count - 1u) / shard_count;
    uint32_t shard_capacity = vemb_v16_pow2_ceil_u32(per_shard_target);
    if (shard_capacity < TLC_CORE_KEY_META_MIN_SHARD_CAPACITY)
        shard_capacity = TLC_CORE_KEY_META_MIN_SHARD_CAPACITY;

    core->key_meta_shards = zcalloc(sizeof(tlc_core_key_meta_shard_t) * shard_count);
    RETURN_IF(!core->key_meta_shards, -1);
    RETURN_IF(bitmap_init(&core->key_meta_locks, shard_count) != 0, -1);
    core->key_meta_capacity = shard_capacity * shard_count;
    atomic_store_explicit(&core->key_meta_count, 0, memory_order_relaxed);

    for (uint32_t i = 0; i < shard_count; i++) {
        tlc_core_key_meta_shard_t *shard = &core->key_meta_shards[i];
        shard->entries = zcalloc(sizeof(*shard->entries) * shard_capacity);
        if (!shard->entries) {
            core->key_meta_shard_count = i;
            return -1;
        }
        shard->lock_id = i;
        shard->capacity = shard_capacity;
        shard->mask = shard_capacity - 1u;
        shard->count = 0;
    }
    core->key_meta_shard_count = shard_count;
    return 0;
}

static tlc_core_key_meta_shard_t *key_meta_shard_for_hash(tlc_core_t *core,
                                                          uint64_t key_hash) {
    uint32_t shard_index =
        vemb_v16_mix32_u64(key_hash) & (core->key_meta_shard_count - 1u);
    return &core->key_meta_shards[shard_index];
}

static void key_meta_init_entry_locked(tlc_core_key_meta_entry_t *entry,
                                       const char *key,
                                       uint32_t key_len,
                                       uint64_t key_hash) {
    entry->occupied = 1;
    entry->key_hash = key_hash;
    entry->key_len = key_len;
    memcpy(entry->key, key, key_len);
    entry->key_version = 0;
    entry->topology_epoch = 0;
    entry->owner_epoch = 0;
    entry->migration_state = TLC_CORE_KEY_SOURCE_ACTIVE;
    entry->source_owner = UINT32_MAX;
    entry->target_owner = UINT32_MAX;
    entry->tombstone = 0;
    entry->shard_id = 0;
    entry->location = tlc_invalid_location;
}

static tlc_core_key_meta_entry_t *key_meta_find_locked(
        tlc_core_t *core,
        const char *key,
        uint32_t key_len,
        uint64_t key_hash,
        int create) {
    tlc_core_key_meta_shard_t *shard = key_meta_shard_for_hash(core, key_hash);
    uint32_t slot = vemb_v16_hash_mask_u64(key_hash, shard->mask);
    for (uint32_t i = 0; i < shard->capacity; i++) {
        tlc_core_key_meta_entry_t *entry = &shard->entries[(slot + i) & shard->mask];
        if (!entry->occupied) {
            if (!create) return NULL;
            key_meta_init_entry_locked(entry, key, key_len, key_hash);
            shard->count++;
            atomic_fetch_add_explicit(&core->key_meta_count, 1, memory_order_relaxed);
            return entry;
        }
        if (key_matches(key_hash, key, key_len,
                        entry->key_hash,
                        entry->key,
                        entry->key_len)) {
            return entry;
        }
    }
    return NULL;
}

static void key_meta_fill_info(const tlc_core_key_meta_entry_t *entry,
                               tlc_core_key_migration_info_t *info) {
    if (!info) return;
    *info = (tlc_core_key_migration_info_t){
        .key_hash = entry->key_hash,
        .key_version = entry->key_version,
        .topology_epoch = entry->topology_epoch,
        .owner_epoch = entry->owner_epoch,
        .migration_state = entry->migration_state,
        .source_owner = entry->source_owner,
        .target_owner = entry->target_owner,
        .tombstone = entry->tombstone,
        .shard_id = entry->shard_id,
        .location = entry->location,
    };
}

static int incoming_snapshot_is_stale(
        const tlc_core_key_meta_entry_t *entry,
        const tlc_core_migration_snapshot_t *snapshot) {
    if (snapshot->owner_epoch < entry->owner_epoch)
        return 1;
    if (snapshot->topology_epoch < entry->topology_epoch)
        return 1;
    if (snapshot->topology_epoch == entry->topology_epoch &&
        snapshot->key_version < entry->key_version) {
        return 1;
    }
    return 0;
}

static int incoming_snapshot_is_duplicate(
        const tlc_core_key_meta_entry_t *entry,
        const tlc_core_migration_snapshot_t *snapshot) {
    return snapshot->owner_epoch == entry->owner_epoch &&
           snapshot->topology_epoch == entry->topology_epoch &&
           snapshot->key_version == entry->key_version;
}

static int key_meta_state_blocks_source_access(uint32_t migration_state) {
    return migration_state == TLC_CORE_KEY_CUTOVER ||
           migration_state == TLC_CORE_KEY_SOURCE_GC;
}

/* TODO: maintain this as an active source-fence refcount and decrement it
 * when cutover/source-gc entries are released, instead of leaving it as a
 * conservative once-set fast-path gate. */
int tlc_core_source_fence_active(const tlc_core_t *core) {
    return atomic_load_explicit(&core->source_fence_active_count,
                                memory_order_acquire) != 0;
}

static int tombstone_filter_active(const tlc_core_t *core) {
    return atomic_load_explicit(&core->tombstone_active_count,
                                memory_order_acquire) != 0;
}

static void key_meta_note_source_fence_transition(tlc_core_t *core,
                                                  uint32_t old_state,
                                                  uint32_t new_state) {
    if (!key_meta_state_blocks_source_access(old_state) &&
        key_meta_state_blocks_source_access(new_state)) {
        atomic_fetch_add_explicit(&core->source_fence_active_count,
                                  1,
                                  memory_order_release);
    }
}

static void location_cache_put(tlc_core_t *core,
                               const char *key,
                               uint32_t key_len,
                               uint64_t key_hash,
                               const tlc_warm_location_t *location);

static void key_meta_set_tombstone_locked(tlc_core_t *core,
                                          tlc_core_key_meta_entry_t *meta,
                                          uint32_t tombstone) {
    uint32_t new_value = tombstone ? 1u : 0u;
    if (meta->tombstone == new_value)
        return;
    if (new_value) {
        atomic_fetch_add_explicit(&core->tombstone_active_count,
                                  1,
                                  memory_order_release);
        meta->tombstone = 1;
    } else {
        meta->tombstone = 0;
        atomic_fetch_sub_explicit(&core->tombstone_active_count,
                                  1,
                                  memory_order_release);
    }
}

static int key_meta_blocks_source_access(tlc_core_t *core,
                                         const char *key,
                                         uint32_t key_len,
                                         uint64_t key_hash) {
    int source_fence_active = tlc_core_source_fence_active(core);
    if (!source_fence_active && !tombstone_filter_active(core))
        return 0;

    int blocked = 0;
    tlc_core_key_meta_shard_t *shard =
        key_meta_shard_for_hash(core, key_hash);
    bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
    tlc_core_key_meta_entry_t *key_meta =
        key_meta_find_locked(core, key, key_len, key_hash, 0);
    blocked = key_meta &&
        (key_meta->tombstone ||
         (source_fence_active &&
          key_meta_state_blocks_source_access(key_meta->migration_state)));
    bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    return blocked;
}

static int entry_readable(int state) {
    return state == TLC_CORE_ENTRY_VALID ||
           state == TLC_CORE_ENTRY_DIRTY;
}

static int hot_init(tlc_core_t *core, uint32_t requested_capacity) {
    tlc_core_hold_layer_t *hold = &core->hold;
    hold->capacity = requested_capacity ?
        vemb_v16_pow2_ceil_u32(requested_capacity) :
        TLC_CORE_DEFAULT_HOT_CAPACITY;
    if (hold->capacity < 2) hold->capacity = 2;
    hold->mask = hold->capacity - 1u;
    hold->table = zcalloc(sizeof(*hold->table) * hold->capacity);
    RETURN_IF(!hold->table, -1);
    for (uint32_t i = 0; i < hold->capacity; i++) {
        atomic_init(&hold->table[i].key_hash, 0);
        atomic_init(&hold->table[i].warm_idx, -1);
    }
    atomic_init(&hold->hits, 0);
    atomic_init(&hold->misses, 0);
    return 0;
}

static int32_t hot_get(tlc_core_t *core, uint64_t key_hash) {
    tlc_core_hold_layer_t *hold = &core->hold;
    uint32_t slot = vemb_v16_hash_mask_u64(key_hash, hold->mask);
    for (uint32_t i = 0; i < TLC_CORE_HOT_PROBES; i++) {
        uint32_t pos = (slot + i) & hold->mask;
        if (i + 1 < TLC_CORE_HOT_PROBES) {
            __builtin_prefetch(&hold->table[(slot + i + 1) & hold->mask], 0, 3);
        }
        int32_t warm_idx =
            atomic_load_explicit(&hold->table[pos].warm_idx, memory_order_acquire);
        if (warm_idx < 0) break;
        uint64_t entry_hash =
            atomic_load_explicit(&hold->table[pos].key_hash, memory_order_relaxed);
        if (entry_hash == key_hash) {
            return warm_idx;
        }
    }
    return -1;
}

static uint64_t location_cache_key_word(const char *key,
                                        uint32_t key_len,
                                        uint32_t word_index) {
    uint64_t word = 0;
    uint32_t off = word_index * (uint32_t)sizeof(uint64_t);
    if (off >= key_len)
        return 0;
    uint32_t bytes = key_len - off;
    if (bytes > sizeof(uint64_t))
        bytes = sizeof(uint64_t);
    memcpy(&word, key + off, bytes);
    return word;
}

static int location_cache_init(tlc_core_t *core, uint32_t requested_capacity) {
    uint64_t target = requested_capacity ?
        (uint64_t)requested_capacity * 2u :
        (uint64_t)TLC_CORE_DEFAULT_HOT_CAPACITY * 2u;
    uint32_t capacity = vemb_v16_pow2_ceil_u32(target);
    if (capacity < TLC_CORE_LOCATION_CACHE_MIN_CAPACITY)
        capacity = TLC_CORE_LOCATION_CACHE_MIN_CAPACITY;

    tlc_core_location_cache_t *cache = &core->location_cache;
    cache->entries = zcalloc(sizeof(*cache->entries) * capacity);
    RETURN_IF(!cache->entries, -1);
    cache->capacity = capacity;
    cache->mask = capacity - 1u;

    for (uint32_t i = 0; i < capacity; i++) {
        tlc_core_location_cache_entry_t *entry = &cache->entries[i];
        atomic_init(&entry->seq, SEQLOCK_EMPTY);
        atomic_init(&entry->key_hash, 0);
        atomic_init(&entry->key_len, 0);
        atomic_init(&entry->region_id, TLC_CORE_INVALID_REGION_ID);
        atomic_init(&entry->region_index, UINT32_MAX);
        atomic_init(&entry->local_slot, TLC_CORE_INVALID_SLOT);
        atomic_init(&entry->bytes, 0);
        atomic_init(&entry->offset, 0);
        atomic_init(&entry->owner_generation, 0);
        for (uint32_t w = 0; w < TLC_CORE_LOCATION_CACHE_KEY_WORDS; w++)
            atomic_init(&entry->key_words[w], 0);
    }
    return 0;
}

static void location_cache_destroy(tlc_core_t *core) {
    if (core->location_cache.entries)
        zfree(core->location_cache.entries);
    core->location_cache.entries = NULL;
    core->location_cache.capacity = 0;
    core->location_cache.mask = 0;
}

static int location_cache_entry_key_matches(
        tlc_core_location_cache_entry_t *entry,
        const char *key,
        uint32_t key_len,
        uint64_t key_hash) {
    if (atomic_load_explicit(&entry->key_hash, memory_order_relaxed) != key_hash ||
        atomic_load_explicit(&entry->key_len, memory_order_relaxed) != key_len) {
        return 0;
    }
    uint32_t words =
        (key_len + (uint32_t)sizeof(uint64_t) - 1u) /
        (uint32_t)sizeof(uint64_t);
    for (uint32_t i = 0; i < words; i++) {
        uint64_t want = location_cache_key_word(key, key_len, i);
        uint64_t got =
            atomic_load_explicit(&entry->key_words[i],
                                 memory_order_relaxed);
        if (got != want)
            return 0;
    }
    return 1;
}

static int location_cache_try_begin_write(
        tlc_core_location_cache_entry_t *entry,
        uint64_t *seq_out) {
    uint64_t seq = atomic_load_explicit(&entry->seq, memory_order_acquire);
    if (seqlock_seq_is_writing(seq))
        return -1;
    uint64_t expected = seq;
    if (!atomic_compare_exchange_strong_explicit(&entry->seq,
                                                 &expected,
                                                 seq + SEQLOCK_STATE_STEP,
                                                 memory_order_acq_rel,
                                                 memory_order_acquire)) {
        return -1;
    }
    *seq_out = seq;
    return 0;
}

static void location_cache_store_entry(tlc_core_location_cache_entry_t *entry,
                                       const char *key,
                                       uint32_t key_len,
                                       uint64_t key_hash,
                                       const tlc_warm_location_t *location) {
    uint64_t seq = SEQLOCK_EMPTY;
    if (location_cache_try_begin_write(entry, &seq) != 0)
        return;

    uint32_t words = (key_len + (uint32_t)sizeof(uint64_t) - 1u) / (uint32_t)sizeof(uint64_t);
    for (uint32_t i = 0; i < TLC_CORE_LOCATION_CACHE_KEY_WORDS; i++) {
        uint64_t word = i < words ?  location_cache_key_word(key, key_len, i) : 0;
        atomic_store_explicit(&entry->key_words[i], word, memory_order_relaxed);
    }
    atomic_store_explicit(&entry->region_id, location->region_id, memory_order_relaxed);
    atomic_store_explicit(&entry->region_index, location->region_index, memory_order_relaxed);
    atomic_store_explicit(&entry->local_slot, location->local_slot, memory_order_relaxed);
    atomic_store_explicit(&entry->bytes, location->bytes, memory_order_relaxed);
    atomic_store_explicit(&entry->offset, location->offset, memory_order_relaxed);
    atomic_store_explicit(&entry->owner_generation, location->owner_generation, memory_order_relaxed);
    atomic_store_explicit(&entry->key_hash, key_hash, memory_order_relaxed);
    atomic_store_explicit(&entry->key_len, key_len, memory_order_relaxed);
    atomic_store_explicit(&entry->seq, seq + SEQLOCK_VERSION_STEP, memory_order_release);
}

static void location_cache_put(tlc_core_t *core,
                               const char *key,
                               uint32_t key_len,
                               uint64_t key_hash,
                               const tlc_warm_location_t *location) {
    tlc_core_location_cache_t *cache = &core->location_cache;
    if (!key_valid(key, key_len) || !location ||
        location->region_id == TLC_CORE_INVALID_REGION_ID ||
        location->local_slot == TLC_CORE_INVALID_SLOT) {
        return;
    }

    uint32_t slot = vemb_v16_hash_mask_u64(key_hash, cache->mask);
    tlc_core_location_cache_entry_t *victim = &cache->entries[slot];
    for (uint32_t i = 0; i < TLC_CORE_LOCATION_CACHE_PROBES; i++) {
        tlc_core_location_cache_entry_t *entry =
            &cache->entries[(slot + i) & cache->mask];
        uint64_t seq = atomic_load_explicit(&entry->seq,
                                            memory_order_acquire);
        if (seqlock_seq_is_writing(seq))
            continue;
        if (seq == SEQLOCK_EMPTY ||
            atomic_load_explicit(&entry->key_len,
                                 memory_order_relaxed) == 0 ||
            location_cache_entry_key_matches(entry, key, key_len, key_hash)) {
            location_cache_store_entry(entry, key, key_len, key_hash, location);
            return;
        }
    }
    location_cache_store_entry(victim, key, key_len, key_hash, location);
}

static int warm_regions_init(tlc_core_t *core, const tlc_core_config_t *config) {
    tlc_core_warm_layer_t *warm = &core->warm;
    uint32_t region_count = config->warm_region_count;
    const tlc_core_warm_region_config_t *regions = config->warm_regions;

    RETURN_IF(region_count > TLC_CORE_MAX_WARM_REGIONS, -1);

    warm->regions = zcalloc(sizeof(*warm->regions) * region_count);
    RETURN_IF(!warm->regions, -1);
    warm->region_count = region_count;
    atomic_init(&warm->runtime_region_count, 0);

    uint32_t total_vnodes = 0;
    uint32_t local_region_weight = config->local_region_weight ?
        config->local_region_weight : 4u;
    for (uint32_t i = 0; i < region_count; i++) {
        const tlc_core_warm_region_config_t *src = &regions[i];
        RETURN_IF(!src->mapped_addr ||
                  src->value_size != core->value_size ||
                  src->region_bytes < src->value_size,
                  -1);
        uint64_t capacity_slots = src->region_bytes / src->value_size;
        RETURN_IF(capacity_slots > UINT32_MAX, -1);
        uint32_t weight = src->weight ? src->weight : 1u;
        RETURN_IF(src->is_local &&
                  weight > UINT32_MAX / local_region_weight,
                  -1);
        uint32_t effective_weight = src->is_local ?
            weight * local_region_weight : weight;
        RETURN_IF(effective_weight > UINT32_MAX / 32u,
                  -1);
        RETURN_IF(total_vnodes > UINT32_MAX - 32u * effective_weight, -1);
        total_vnodes += 32u * effective_weight;

        RETURN_IF(warm_region_init_one(core, &warm->regions[i], src, i) != 0,
                  -1);
        serverLog(LL_NOTICE,
                  "tlc warm region effective weight: region_index=%u region_id=%u effective_weight=%u",
                  i,
                  warm->regions[i].region_id,
                  effective_weight);
    }

    warm->vnodes = zcalloc(sizeof(*warm->vnodes) * total_vnodes);
    RETURN_IF(!warm->vnodes, -1);
    warm->vnode_count = total_vnodes;
    uint32_t vnode_pos = 0;
    for (uint32_t i = 0; i < region_count; i++) {
        const tlc_core_warm_region_runtime_t *region = &warm->regions[i];
        uint32_t effective_weight = region->is_local ?
            region->weight * local_region_weight : region->weight;
        uint32_t vnodes = 32u * effective_weight;
        for (uint32_t v = 0; v < vnodes; v++) {
            uint64_t seed = ((uint64_t)region->region_id << 32) |
                            ((uint64_t)i << 24) |
                            (uint64_t)v;
            warm->vnodes[vnode_pos++] = (tlc_core_warm_region_vnode_t){
                .hash_val = vemb_v16_mix32_u64(seed),
                .region_index = i,
            };
        }
    }
    qsort(warm->vnodes, warm->vnode_count, sizeof(*warm->vnodes),
          vnode_compare);
    return 0;
}

static int warm_init(tlc_core_t *core, const tlc_core_config_t *config) {
    tlc_core_warm_layer_t *warm = &core->warm;
    warm->capacity = core->warm_capacity;
    warm->hash_capacity =
        vemb_v16_pow2_ceil_u32((uint64_t)core->warm_capacity * 2u);
    if (warm->hash_capacity < 2) warm->hash_capacity = 2;
    warm->mask = warm->hash_capacity - 1u;
    warm->entries = zcalloc(sizeof(*warm->entries) * warm->capacity);
    warm->hash_table = zcalloc(sizeof(*warm->hash_table) * warm->hash_capacity);
    RETURN_IF(!warm->entries || !warm->hash_table, -1);
    for (uint32_t i = 0; i < warm->hash_capacity; i++)
        warm->hash_table[i] = -1;
    for (uint32_t i = 0; i < warm->capacity; i++) {
        atomic_init(&warm->entries[i].state, TLC_CORE_ENTRY_EMPTY);
        atomic_init(&warm->entries[i].access_count, 0);
    }
    atomic_init(&warm->count, 0);
    atomic_init(&warm->hits, 0);
    atomic_init(&warm->misses, 0);
    if (warm_regions_init(core, config) != 0) {
        return -1;
    }
    return bitmap_init(&warm->locks, warm->hash_capacity);
}

static int warm_entry_matches(tlc_core_warm_entry_t *entry,
                              const char *key,
                              uint32_t key_len,
                              uint64_t key_hash) {
    int state = atomic_load_explicit(&entry->state, memory_order_acquire);
    return entry_readable(state) &&
           key_matches(key_hash, key, key_len,
                       entry->key_hash, entry->key, entry->key_len);
}

static int warm_validate_idx(tlc_core_t *core,
                             int32_t warm_idx,
                             const char *key,
                             uint32_t key_len,
                             uint64_t key_hash,
                             tlc_warm_location_t *location) {
    RETURN_IF(warm_idx < 0 || (uint32_t)warm_idx >= core->warm.capacity, -1);
    tlc_core_warm_entry_t *entry = &core->warm.entries[warm_idx];
    int matched = warm_entry_matches(entry, key, key_len, key_hash);
    RETURN_IF(!matched, -1);
    atomic_fetch_add_explicit(&entry->access_count, 1, memory_order_relaxed);
    *location = entry->location;
    return 0;
}

static uint64_t key_fingerprint(const char *key, uint32_t key_len) {
    return vemb_v16_fnv1a64_bytes(key, key_len);
}

static uint32_t region_ways(const tlc_core_warm_region_runtime_t *region) {
    return region->capacity_slots < TLC_CORE_WARM_WAYS ?
        region->capacity_slots : TLC_CORE_WARM_WAYS;
}

static void region_set_bounds(const tlc_core_warm_region_runtime_t *region,
                              uint64_t key_hash,
                              uint32_t *start,
                              uint32_t *end) {
    uint32_t ways = region_ways(region);
    uint32_t set_count =
        (region->capacity_slots + ways - 1u) / ways;
    // Map the key to one set so placement/probing stays within its ways.
    uint32_t set = set_count ? vemb_v16_mix32_u64(key_hash) % set_count : 0;
    *start = set * ways;
    *end = *start + ways;
    if (*end > region->capacity_slots)
        *end = region->capacity_slots;
}

// Try to claim the slot's seqlock for an in-place writer.
static int slot_seq_try_begin(vemb_v16_warm_slot_meta_t *slot_meta,
                              uint64_t *old_seq) {
    uint64_t seq = atomic_load_explicit(&slot_meta->write_seq,
                                        memory_order_acquire);
    if (seqlock_seq_is_writing(seq))
        return -1;
    uint64_t expected = seq;
    if (!atomic_compare_exchange_strong_explicit(&slot_meta->write_seq,
                                                 &expected,
                                                 seq + SEQLOCK_STATE_STEP,
                                                 memory_order_acq_rel,
                                                 memory_order_acquire)) {
        return -1;
    }
    *old_seq = seq;
    return 0;
}

static void slot_write_payload(tlc_core_warm_region_runtime_t *region,
                               uint32_t local_slot,
                               const void *value,
                               uint32_t value_size) {
    sve_streaming_load_f32(value,
                           region->mapped_addr +
                               (uint64_t)local_slot * region->value_size,
                           value_size);
}

static void slot_publish_ready(vemb_v16_warm_slot_meta_t *slot_meta,
                               uint64_t old_seq) {
    atomic_store_explicit(&slot_meta->write_seq, old_seq + SEQLOCK_VERSION_STEP, memory_order_release);
    atomic_store_explicit(&slot_meta->state, VEMB_V16_WARM_SLOT_READY, memory_order_release);
}

static void fill_location_from_slot(const tlc_core_warm_region_runtime_t *region,
                                    uint32_t region_index,
                                    const vemb_v16_warm_slot_meta_t *slot_meta,
                                    uint32_t local_slot,
                                    tlc_warm_location_t *location) {
    *location = (tlc_warm_location_t){
        .region_id = region->region_id,
        .region_index = region_index,
        .local_slot = local_slot,
        .bytes = slot_meta->bytes,
        .offset = (uint64_t)local_slot * region->value_size,
        .owner_generation = atomic_load_explicit(&slot_meta->owner_generation, memory_order_acquire),
    };
}

typedef struct warm_slot_snapshot {
    uint64_t stable_seq;
    uint64_t owner_generation;
    uint32_t bytes;
    uint64_t key_hash;
    uint64_t key_fingerprint;
} warm_slot_snapshot_t;

static void warm_slot_record_read_access(vemb_v16_warm_slot_meta_t *slot_meta) {
#if TLC_CORE_ALLOW_LRU_EVICTION
    uint32_t clock_bit = atomic_load_explicit(&slot_meta->clock_bit, memory_order_relaxed);
    if (clock_bit != 0) return;
    atomic_store_explicit(&slot_meta->last_access_ns, vemb_v16_monotonic_ns(), memory_order_relaxed);
    atomic_store_explicit(&slot_meta->clock_bit, 1, memory_order_relaxed);
#else
    (void)slot_meta;
#endif
}

static void warm_slot_record_committed_access(vemb_v16_warm_slot_meta_t *slot_meta) {
    atomic_store_explicit(&slot_meta->cold_state, VEMB_V16_WARM_SLOT_COLD_COMMITTED,
                          memory_order_release);
#if TLC_CORE_ALLOW_LRU_EVICTION
    atomic_store_explicit(&slot_meta->last_access_ns, vemb_v16_monotonic_ns(), memory_order_relaxed);
    atomic_store_explicit(&slot_meta->clock_bit, 1, memory_order_relaxed);
#endif
}

// Read a stable slot snapshot or report that the slot is missing/busy.
static tlc_core_warm_verify_rc_t warm_slot_read_snapshot(
                                  vemb_v16_warm_slot_meta_t *slot_meta,
                                  warm_slot_snapshot_t *snapshot) {
    uint32_t state = atomic_load_explicit(&slot_meta->state, memory_order_acquire);
    if (state != VEMB_V16_WARM_SLOT_READY)
        return TLC_CORE_WARM_VERIFY_MISS;
    uint64_t seq1 = atomic_load_explicit(&slot_meta->write_seq, memory_order_acquire);
    if (seqlock_seq_is_writing(seq1))
        return TLC_CORE_WARM_VERIFY_BUSY;
    snapshot->stable_seq = seq1;
    snapshot->owner_generation =
        atomic_load_explicit(&slot_meta->owner_generation, memory_order_acquire);
    snapshot->bytes = slot_meta->bytes;
    snapshot->key_hash = slot_meta->key_hash;
    snapshot->key_fingerprint = slot_meta->key_fingerprint;
    atomic_thread_fence(memory_order_acquire);
    uint64_t seq2 = atomic_load_explicit(&slot_meta->write_seq, memory_order_acquire);
    if (seq1 != seq2 || seqlock_seq_is_writing(seq2))
        return TLC_CORE_WARM_VERIFY_BUSY;
    return TLC_CORE_WARM_VERIFY_OK;
}

static int warm_slot_snapshot_matches(const warm_slot_snapshot_t *snapshot,
                                      uint64_t key_hash,
                                      uint64_t fp,
                                      uint32_t expected_bytes,
                                      uint64_t expected_generation) {
    return snapshot->key_hash == key_hash &&
           (fp == 0 || snapshot->key_fingerprint == fp) &&
           snapshot->bytes == expected_bytes &&
           (expected_generation == 0 ||
            snapshot->owner_generation == expected_generation);
}

// Caller must hold the slot for writing; this does not publish write_seq/state.
static void warm_slot_write_commit(
        tlc_core_warm_region_runtime_t *region,
        vemb_v16_warm_slot_meta_t *slot_meta,
        uint32_t slot,
        uint64_t key_hash,
        uint64_t fp,
        const void *value,
        uint32_t value_size) {
    atomic_fetch_add_explicit(&slot_meta->owner_generation, 1, memory_order_acq_rel);
    slot_meta->region_id = region->region_id;
    slot_meta->local_slot = slot;
    slot_meta->bytes = value_size;
    slot_meta->key_hash = key_hash;
    slot_meta->key_fingerprint = fp;
    warm_slot_record_committed_access(slot_meta);
    slot_write_payload(region, slot, value, value_size);
}

// Probe a slot and report whether it is ready, missing, or still busy.
static tlc_core_warm_verify_rc_t warm_slot_probe_status(
                                  tlc_core_warm_region_runtime_t *region,
                                  uint32_t region_index,
                                  uint32_t local_slot,
                                  uint64_t key_hash,
                                  uint64_t fp,
                                  uint32_t expected_bytes,
                                  uint64_t expected_generation,
                                  tlc_warm_location_t *location) {
    if (local_slot >= region->capacity_slots)
        return TLC_CORE_WARM_VERIFY_MISS;
    vemb_v16_warm_slot_meta_t *slot_meta = &region->slot_meta[local_slot];
    warm_slot_snapshot_t snapshot;
    tlc_core_warm_verify_rc_t rc = warm_slot_read_snapshot(slot_meta, &snapshot);
    if (rc != TLC_CORE_WARM_VERIFY_OK)
        return rc;
    if (!warm_slot_snapshot_matches(&snapshot,
                                    key_hash,
                                    fp,
                                    expected_bytes,
                                    expected_generation)) {
        return TLC_CORE_WARM_VERIFY_MISS;
    }
    warm_slot_record_read_access(slot_meta);
    if (location) {
        *location = (tlc_warm_location_t){
            .region_id = region->region_id,
            .region_index = region_index,
            .local_slot = local_slot,
            .bytes = snapshot.bytes,
            .offset = (uint64_t)local_slot * region->value_size,
            .owner_generation = snapshot.owner_generation,
        };
    }
    return TLC_CORE_WARM_VERIFY_OK;
}

static int resolve_warm_region_for_location(
        tlc_core_t *core,
        const tlc_warm_location_t *location,
        tlc_core_warm_region_runtime_t **region_out,
        uint32_t *region_index_out) {
    tlc_core_warm_layer_t *warm = &core->warm;
    tlc_core_warm_region_runtime_t *region = NULL;
    uint32_t region_index = location->region_index;
    region = warm_region_by_index(warm, region_index);
    if (region && region->region_id == location->region_id) {
        /* fast path */
    } else {
        region = NULL;
        for (uint32_t i = 0; i < warm->region_count; i++) {
            if (warm->regions[i].region_id == location->region_id) {
                region = &warm->regions[i];
                region_index = i;
                break;
            }
        }
        if (!region) {
            uint32_t runtime_count = warm_runtime_region_count(warm);
            for (uint32_t i = 0; i < runtime_count; i++) {
                if (warm->runtime_regions[i].region_id == location->region_id) {
                    region = &warm->runtime_regions[i];
                    region_index = warm->region_count + i;
                    break;
                }
            }
        }
    }
    RETURN_IF(!region ||
              location->local_slot >= region->capacity_slots ||
              location->offset !=
                  (uint64_t)location->local_slot * region->value_size ||
              location->bytes != region->value_size,
              -1);
    *region_out = region;
    *region_index_out = region_index;
    return 0;
}

static void note_lookup_location(tlc_core_t *core,
                                 const tlc_warm_location_t *location,
                                 int cold_promote) {
    tlc_core_warm_region_runtime_t *region = NULL;
    uint32_t region_index = UINT32_MAX;
    if (resolve_warm_region_for_location(core, location, &region,
                                         &region_index) != 0)
        return;
    if (!cold_promote) {
        if (region->is_local)
            atomic_fetch_add_explicit(&core->lookup_warm_local_hit, 1,
                                      memory_order_relaxed);
        else
            atomic_fetch_add_explicit(&core->lookup_warm_imported_hit, 1,
                                      memory_order_relaxed);
    }
    if (region_index < TLC_CORE_MAX_TOTAL_WARM_REGIONS) {
        if (!cold_promote)
            atomic_fetch_add_explicit(
                &core->region_lookup_hits[region_index], 1,
                memory_order_relaxed);
        else
            atomic_fetch_add_explicit(
                &core->region_cold_promotes[region_index], 1,
                memory_order_relaxed);
    }
}

static int location_cache_validate_location(tlc_core_t *core,
                                            uint64_t key_hash,
                                            const tlc_warm_location_t *cached,
                                            tlc_warm_location_t *location) {
    tlc_core_warm_region_runtime_t *region = NULL;
    uint32_t region_index = UINT32_MAX;
    if (resolve_warm_region_for_location(core,
                                         cached,
                                         &region,
                                         &region_index) != 0) {
        return -1;
    }

    for (uint32_t attempt = 0; attempt < TLC_CORE_WARM_BUSY_RETRIES; attempt++) {
        tlc_core_warm_verify_rc_t rc = warm_slot_probe_status(
                                          region,
                                          region_index,
                                          cached->local_slot,
                                          key_hash,
                                          0,
                                          cached->bytes,
                                          cached->owner_generation,
                                          location);
        if (rc == TLC_CORE_WARM_VERIFY_OK)
            return 0;
        if (rc != TLC_CORE_WARM_VERIFY_BUSY)
            return -1;
        cpu_relax();
    }
    return -1;
}

static int location_cache_get(tlc_core_t *core,
                              const char *key,
                              uint32_t key_len,
                              uint64_t key_hash,
                              tlc_warm_location_t *location) {
    RETURN_IF(!key_valid(key, key_len) || !location, -1);

    tlc_core_location_cache_t *cache = &core->location_cache;
    uint32_t slot = vemb_v16_hash_mask_u64(key_hash, cache->mask);
    for (uint32_t i = 0; i < TLC_CORE_LOCATION_CACHE_PROBES; i++) {
        tlc_core_location_cache_entry_t *entry = &cache->entries[(slot + i) & cache->mask];
        uint64_t seq1 = atomic_load_explicit(&entry->seq, memory_order_acquire);
        if (seq1 == SEQLOCK_EMPTY || seqlock_seq_is_writing(seq1))
            continue;
        if (!location_cache_entry_key_matches(entry, key, key_len, key_hash))
            continue;

        tlc_warm_location_t cached = {
            .region_id = atomic_load_explicit(&entry->region_id, memory_order_relaxed),
            .region_index = atomic_load_explicit(&entry->region_index, memory_order_relaxed),
            .local_slot = atomic_load_explicit(&entry->local_slot, memory_order_relaxed),
            .bytes = atomic_load_explicit(&entry->bytes, memory_order_relaxed),
            .offset = atomic_load_explicit(&entry->offset, memory_order_relaxed),
            .owner_generation = atomic_load_explicit(&entry->owner_generation, memory_order_relaxed),
        };
        uint64_t seq2 = atomic_load_explicit(&entry->seq, memory_order_acquire);
        if (seq1 != seq2 || seqlock_seq_is_writing(seq2))
            continue;
        return (location_cache_validate_location(core,
                                                 key_hash,
                                                 &cached,
                                                 location) == 0 ? 0 : -1);
    }
    return -1;
}

static int location_cache_peek(tlc_core_t *core,
                               const char *key,
                               uint32_t key_len,
                               uint64_t key_hash,
                               tlc_warm_location_t *location) {
    RETURN_IF(!key_valid(key, key_len) || !location, -1);
    tlc_core_location_cache_t *cache = &core->location_cache;
    uint32_t slot = vemb_v16_hash_mask_u64(key_hash, cache->mask);
    for (uint32_t i = 0; i < TLC_CORE_LOCATION_CACHE_PROBES; i++) {
        tlc_core_location_cache_entry_t *entry = &cache->entries[(slot + i) & cache->mask];
        uint64_t seq1 = atomic_load_explicit(&entry->seq, memory_order_acquire);
        if (seq1 == SEQLOCK_EMPTY || seqlock_seq_is_writing(seq1)) continue;
        if (!location_cache_entry_key_matches(entry, key, key_len, key_hash)) continue;

        tlc_warm_location_t cached = {
            .region_id = atomic_load_explicit(&entry->region_id, memory_order_relaxed),
            .region_index = atomic_load_explicit(&entry->region_index, memory_order_relaxed),
            .local_slot = atomic_load_explicit(&entry->local_slot, memory_order_relaxed),
            .bytes = atomic_load_explicit(&entry->bytes, memory_order_relaxed),
            .offset = atomic_load_explicit(&entry->offset, memory_order_relaxed),
            .owner_generation = atomic_load_explicit(&entry->owner_generation, memory_order_relaxed),
        };
        uint64_t seq2 = atomic_load_explicit(&entry->seq, memory_order_acquire);
        if (seq1 != seq2 || seqlock_seq_is_writing(seq2)) continue;
        *location = cached;
        return 0;
    }
    return -1;
}

static int warm_lookup_region(tlc_core_warm_region_runtime_t *region,
                              uint32_t region_index,
                              uint64_t key_hash,
                              uint64_t fp,
                              uint32_t expected_bytes,
                              tlc_warm_location_t *location) {
    uint32_t start = 0, end = 0;
    region_set_bounds(region, key_hash, &start, &end);
    for (uint32_t attempt = 0; attempt < TLC_CORE_WARM_BUSY_RETRIES; attempt++) {
        int saw_busy = 0;
        for (uint32_t slot = start; slot < end; slot++) {
            tlc_core_warm_verify_rc_t rc = warm_slot_probe_status(region,
                                       region_index,
                                       slot,
                                       key_hash,
                                       fp,
                                       expected_bytes,
                                       0,
                                       location);
            if (rc == TLC_CORE_WARM_VERIFY_OK)
                return 0;
            if (rc == TLC_CORE_WARM_VERIFY_BUSY)
                saw_busy = 1;
        }
        if (!saw_busy)
            return -1;
        cpu_relax();
    }
    return -1;
}

static tlc_core_warm_verify_rc_t warm_try_overwrite_same(
                                   tlc_core_warm_region_runtime_t *region,
                                   tlc_core_t *core,
                                   uint32_t region_index,
                                   uint32_t slot,
                                   uint64_t key_hash,
                                   uint64_t fp,
                                   uint64_t expected_generation,
                                   const void *value,
                                   uint32_t value_size,
                                   tlc_warm_location_t *location) {
    vemb_v16_warm_slot_meta_t *slot_meta = &region->slot_meta[slot];
    if (atomic_load_explicit(&slot_meta->state, memory_order_acquire) !=
        VEMB_V16_WARM_SLOT_READY) {
        return TLC_CORE_WARM_VERIFY_MISS;
    }
    uint64_t seq = atomic_load_explicit(&slot_meta->write_seq, memory_order_acquire);
    // Same-key overwrite keeps key identity stable, so key_hash/fingerprint
    // can still distinguish BUSY from MISS while payload is being updated.
    if (seqlock_seq_is_writing(seq)) {
        uint64_t slot_key_hash = slot_meta->key_hash;
        uint64_t slot_fp = slot_meta->key_fingerprint;
        atomic_thread_fence(memory_order_acquire);
        return(slot_key_hash == key_hash && slot_fp == fp ?
            TLC_CORE_WARM_VERIFY_BUSY :
            TLC_CORE_WARM_VERIFY_MISS);
    }

    if (slot_meta->key_hash != key_hash || slot_meta->key_fingerprint != fp)
        return TLC_CORE_WARM_VERIFY_MISS;
    if (expected_generation != 0 &&
        atomic_load_explicit(&slot_meta->owner_generation,
                             memory_order_acquire) != expected_generation) {
        return TLC_CORE_WARM_VERIFY_MISS;
    }
    uint64_t old_seq = SEQLOCK_EMPTY;
    if (slot_seq_try_begin(slot_meta, &old_seq) != 0)
        return TLC_CORE_WARM_VERIFY_BUSY;
    atomic_fetch_add_explicit(&core->warm_same_key_overwrite, 1, memory_order_relaxed);
    slot_write_payload(region, slot, value, value_size);
    slot_meta->bytes = value_size;
    warm_slot_record_committed_access(slot_meta);
    atomic_store_explicit(&slot_meta->write_seq, old_seq + SEQLOCK_VERSION_STEP,
                          memory_order_release);
    fill_location_from_slot(region, region_index, slot_meta, slot, location);
    return TLC_CORE_WARM_VERIFY_OK;
}

static int warm_overwrite_location(tlc_core_t *core,
                                   const char *key,
                                   uint32_t key_len,
                                   uint64_t key_hash,
                                   const tlc_warm_location_t *expected,
                                   const void *value,
                                   uint32_t value_size,
                                   tlc_warm_location_t *location) {
    tlc_core_warm_region_runtime_t *region = NULL;
    uint32_t region_index = UINT32_MAX;
    int rc = resolve_warm_region_for_location(core,
                                              expected,
                                              &region,
                                              &region_index);
    RETURN_IF(rc != 0, -1);
    uint64_t fp = key_fingerprint(key, key_len);
    for (uint32_t attempt = 0; attempt < TLC_CORE_WARM_BUSY_RETRIES; attempt++) {
        tlc_core_warm_verify_rc_t rc =
            warm_try_overwrite_same(region,
                                    core,
                                    region_index,
                                    expected->local_slot,
                                    key_hash,
                                    fp,
                                    expected->owner_generation,
                                    value,
                                    value_size,
                                    location);
        if (rc == TLC_CORE_WARM_VERIFY_OK) {
            return 0;
        }
        // Only BUSY is retryable; MISS and other results should stop here.
        if (rc != TLC_CORE_WARM_VERIFY_BUSY) {
            return -1;
        }
        cpu_relax();
    }
    return -1;
}

static int warm_try_fill_free(tlc_core_warm_region_runtime_t *region,
                              uint32_t region_index,
                              uint32_t slot,
                              uint64_t key_hash,
                              uint64_t fp,
                              const void *value,
                              uint32_t value_size,
                              tlc_warm_location_t *location) {
    vemb_v16_warm_slot_meta_t *slot_meta = &region->slot_meta[slot];
    uint32_t expected = VEMB_V16_WARM_SLOT_FREE;
    if (!atomic_compare_exchange_strong_explicit(&slot_meta->state,
                                                 &expected,
                                                 VEMB_V16_WARM_SLOT_FILLING,
                                                 memory_order_acq_rel,
                                                 memory_order_acquire)) {
        return -1;
    }
    uint64_t old_seq = atomic_load_explicit(&slot_meta->write_seq,
                                            memory_order_relaxed);
    uint64_t writing_seq = old_seq;
    if (seqlock_seq_is_stable(writing_seq)) {
        writing_seq += SEQLOCK_STATE_STEP;
    }
    atomic_store_explicit(&slot_meta->write_seq, writing_seq, memory_order_release);
    warm_slot_write_commit(region,
                           slot_meta,
                           slot,
                           key_hash,
                           fp,
                           value,
                           value_size);
    slot_publish_ready(slot_meta, old_seq);
    fill_location_from_slot(region, region_index, slot_meta, slot, location);
    return 0;
}

#if TLC_CORE_ALLOW_LRU_EVICTION
static int warm_slot_can_lru_evict(vemb_v16_warm_slot_meta_t *slot_meta) {
    if (atomic_load_explicit(&slot_meta->state, memory_order_acquire) !=
        VEMB_V16_WARM_SLOT_READY) {
        return 0;
    }
    if (atomic_load_explicit(&slot_meta->cold_state, memory_order_acquire) !=
        VEMB_V16_WARM_SLOT_COLD_COMMITTED) {
        return 0;
    }
    uint64_t seq = atomic_load_explicit(&slot_meta->write_seq,
                                        memory_order_acquire);
    return seqlock_seq_is_stable(seq);
}

static int choose_victim_slot(tlc_core_warm_region_runtime_t *region,
                              uint32_t start,
                              uint32_t end,
                              uint32_t *victim) {
    uint32_t oldest = UINT32_MAX;
    uint64_t oldest_ns = UINT64_MAX;
    for (uint32_t slot = start; slot < end; slot++) {
        vemb_v16_warm_slot_meta_t *slot_meta = &region->slot_meta[slot];
        if (!warm_slot_can_lru_evict(slot_meta))
            continue;
        uint32_t clock_bit =
            atomic_load_explicit(&slot_meta->clock_bit, memory_order_relaxed);
        if (clock_bit == 0) {
            *victim = slot;
            return 0;
        }
        atomic_store_explicit(&slot_meta->clock_bit, 0, memory_order_relaxed);
        uint64_t last_access =
            atomic_load_explicit(&slot_meta->last_access_ns,
                                 memory_order_relaxed);
        if (last_access < oldest_ns) {
            oldest_ns = last_access;
            oldest = slot;
        }
    }
    if (oldest != UINT32_MAX) {
        *victim = oldest;
        return 0;
    }
    return -1;
}

static int warm_try_evict_and_fill(tlc_core_warm_region_runtime_t *region,
                                   tlc_core_t *core,
                                   uint32_t region_index,
                                   uint32_t slot,
                                   uint64_t key_hash,
                                   uint64_t fp,
                                   const void *value,
                                   uint32_t value_size,
                                   tlc_warm_location_t *location) {
    vemb_v16_warm_slot_meta_t *slot_meta = &region->slot_meta[slot];
    uint32_t expected = VEMB_V16_WARM_SLOT_READY;
    if (!atomic_compare_exchange_strong_explicit(&slot_meta->state,
                                                 &expected,
                                                 VEMB_V16_WARM_SLOT_EVICTING,
                                                 memory_order_acq_rel,
                                                 memory_order_acquire)) {
        atomic_fetch_add_explicit(&core->warm_eviction_fail, 1,
                                  memory_order_relaxed);
        return -1;
    }
    if (atomic_load_explicit(&slot_meta->cold_state, memory_order_acquire) !=
        VEMB_V16_WARM_SLOT_COLD_COMMITTED) {
        atomic_store_explicit(&slot_meta->state, VEMB_V16_WARM_SLOT_READY,
                              memory_order_release);
        atomic_fetch_add_explicit(&core->warm_eviction_fail, 1,
                                  memory_order_relaxed);
        return -1;
    }
    uint64_t old_seq = SEQLOCK_EMPTY;
    if (slot_seq_try_begin(slot_meta, &old_seq) != 0) {
        atomic_store_explicit(&slot_meta->state, VEMB_V16_WARM_SLOT_READY,
                              memory_order_release);
        atomic_fetch_add_explicit(&core->warm_eviction_fail, 1,
                                  memory_order_relaxed);
        return -1;
    }
    atomic_store_explicit(&slot_meta->state, VEMB_V16_WARM_SLOT_FILLING,
                          memory_order_release);
    warm_slot_write_commit(region,
                           slot_meta,
                           slot,
                           key_hash,
                           fp,
                           value,
                           value_size);
    slot_publish_ready(slot_meta, old_seq);
    fill_location_from_slot(region, region_index, slot_meta, slot, location);
    atomic_fetch_add_explicit(&core->warm_eviction_success, 1,
                              memory_order_relaxed);
    return 0;
}
#endif

static int warm_place_in_region(tlc_core_warm_region_runtime_t *region,
                                tlc_core_t *core,
                                uint32_t region_index,
                                uint64_t key_hash,
                                uint64_t fp,
                                const void *value,
                                uint32_t value_size,
                                tlc_warm_location_t *location) {
    uint32_t start = 0, end = 0;
    region_set_bounds(region, key_hash, &start, &end);
    for (uint32_t round = 0; round < TLC_CORE_WARM_PLACE_RETRIES; round++) {
        int overwrite_busy = 0;
        for (uint32_t attempt = 0; attempt < TLC_CORE_WARM_BUSY_RETRIES; attempt++) {
            overwrite_busy = 0;
            // Only scan slots in this key's set, not the whole region.
            for (uint32_t slot = start; slot < end; slot++) {
                tlc_core_warm_verify_rc_t rc = warm_try_overwrite_same(
                                            region,
                                            core,
                                            region_index,
                                            slot,
                                            key_hash,
                                            fp,
                                            0,
                                            value,
                                            value_size,
                                            location);
                // MISS means keep scanning this set; only BUSY needs a retry.
                if (rc == TLC_CORE_WARM_VERIFY_OK)
                    return 0;
                if (rc == TLC_CORE_WARM_VERIFY_BUSY)
                    overwrite_busy = 1;
            }
            // No same-key writer is in flight, so stop the busy-retry loop.
            if (!overwrite_busy) break;
            cpu_relax();
        }
        // Still busy after retries, so give up on placing in this region now.
        if (overwrite_busy) return -1;
        for (uint32_t slot = start; slot < end; slot++) {
            if (warm_try_fill_free(region, region_index, slot,
                                   key_hash, fp, value, value_size,
                                   location) == 0) {
                return 0;
            }
        }
#if !TLC_CORE_ALLOW_LRU_EVICTION
        return -1;
#else
        uint32_t victim = UINT32_MAX;
        if (choose_victim_slot(region, start, end, &victim) == 0 &&
            warm_try_evict_and_fill(region, core, region_index, victim,
                                    key_hash, fp, value, value_size,
                                    location) == 0) {
            return 0;
        }
        if (victim == UINT32_MAX) {
            atomic_fetch_add_explicit(&core->warm_eviction_fail, 1, memory_order_relaxed);
        }
        cpu_relax();
#endif
    }
    return -1;
}

static int warm_lookup(tlc_core_t *core,
                       const char *key,
                       uint32_t key_len,
                       uint64_t key_hash,
                       tlc_warm_location_t *location) {
    (void)key_len;
    tlc_core_warm_layer_t *warm = &core->warm;
    uint64_t fp = key_fingerprint(key, key_len);
    uint8_t tried[TLC_CORE_MAX_WARM_REGIONS] = {0};
    uint32_t start = vnode_lower_bound(warm, vemb_v16_mix32_u64(key_hash));
    for (uint32_t pass = 0; pass < 2; pass++) {
        uint32_t want_local = pass == 0 ? 1u : 0u;
        for (uint32_t i = 0; i < warm->vnode_count; i++) {
            uint32_t vnode_pos = (start + i) % warm->vnode_count;
            uint32_t region_index = warm->vnodes[vnode_pos].region_index;
            if (region_index >= warm->region_count || tried[region_index])
                continue;
            tlc_core_warm_region_runtime_t *region = &warm->regions[region_index];
            if ((region->is_local ? 1u : 0u) != want_local)
                continue;
            tried[region_index] = 1;
            if (warm_lookup_region(region, region_index, key_hash, fp,
                                   core->value_size, location) == 0) {
                return 0;
            }
        }
        uint32_t runtime_count = warm_runtime_region_count(warm);
        for (uint32_t i = 0; i < runtime_count; i++) {
            uint32_t region_index = warm->region_count + i;
            tlc_core_warm_region_runtime_t *region = &warm->runtime_regions[i];
            if ((region->is_local ? 1u : 0u) != want_local)
                continue;
            if (warm_lookup_region(region, region_index, key_hash, fp,
                                   core->value_size, location) == 0) {
                return 0;
            }
        }
    }
    return -1;
}

static int warm_put(tlc_core_t *core,
                    const char *key,
                    uint32_t key_len,
                    uint64_t key_hash,
                    const void *value,
                    uint32_t value_size,
                    tlc_warm_location_t *location) {
    tlc_core_warm_layer_t *warm = &core->warm;
    uint64_t fp = key_fingerprint(key, key_len);
    uint8_t tried[TLC_CORE_MAX_WARM_REGIONS] = {0};
    uint32_t attempted_regions = 0;
    uint32_t start = vnode_lower_bound(warm, vemb_v16_mix32_u64(key_hash));
    for (uint32_t pass = 0; pass < 2; pass++) {
        uint32_t want_local = pass == 0 ? 1u : 0u;
        for (uint32_t i = 0; i < warm->vnode_count; i++) {
            uint32_t vnode_pos = (start + i) % warm->vnode_count;
            uint32_t region_index = warm->vnodes[vnode_pos].region_index;
            if (region_index >= warm->region_count || tried[region_index])
                continue;
            tlc_core_warm_region_runtime_t *region = &warm->regions[region_index];
            if ((region->is_local ? 1u : 0u) != want_local)
                continue;
            tried[region_index] = 1;
            attempted_regions++;
            if (!region->mapped_addr || value_size != region->value_size) continue;
            if (warm_place_in_region(region, core, region_index, key_hash, fp,
                                     value, value_size, location) == 0) {
                atomic_fetch_add_explicit(region->is_local ?
                                          &core->warm_alloc_local :
                                          &core->warm_alloc_remote,
                                          1, memory_order_relaxed);
                if (attempted_regions > 1) {
                    atomic_fetch_add_explicit(&core->warm_alloc_fallback,
                                              1, memory_order_relaxed);
                }
                return 0;
            }
            serverLog(LL_NOTICE,
                      "tlc warm region switch: reason=%s key_hash=%llu want_local=%u attempted_regions=%u vnode_pos=%u region_index=%u region_id=%u alloc_rc=%d capacity_slots=%u",
                      "set_no_place",
                      (unsigned long long)key_hash,
                      want_local,
                      attempted_regions,
                      vnode_pos,
                      region_index,
                      region->region_id,
                      VEMB_V16_WARM_REGION_SLOT_ACQUIRE_FULL,
                      region->capacity_slots);
        }
        uint32_t runtime_count = warm_runtime_region_count(warm);
        for (uint32_t i = 0; i < runtime_count; i++) {
            uint32_t region_index = warm->region_count + i;
            tlc_core_warm_region_runtime_t *region = &warm->runtime_regions[i];
            if ((region->is_local ? 1u : 0u) != want_local)
                continue;
            attempted_regions++;
            if (!region->mapped_addr || value_size != region->value_size)
                continue;
            if (warm_place_in_region(region, core, region_index, key_hash, fp,
                                     value, value_size, location) == 0) {
                atomic_fetch_add_explicit(region->is_local ?
                                          &core->warm_alloc_local :
                                          &core->warm_alloc_remote,
                                          1, memory_order_relaxed);
                if (attempted_regions > 1) {
                    atomic_fetch_add_explicit(&core->warm_alloc_fallback,
                                              1, memory_order_relaxed);
                }
                return 0;
            }
            serverLog(LL_NOTICE,
                      "tlc warm region switch: reason=%s key_hash=%llu want_local=%u attempted_regions=%u vnode_pos=%u region_index=%u region_id=%u alloc_rc=%d capacity_slots=%u",
                      "set_no_place_runtime",
                      (unsigned long long)key_hash,
                      want_local,
                      attempted_regions,
                      i,
                      region_index,
                      region->region_id,
                      VEMB_V16_WARM_REGION_SLOT_ACQUIRE_FULL,
                      region->capacity_slots);
        }
    }
    serverLog(LL_WARNING,
              "tlc warm put failed: key_hash=%llu key_len=%u value_size=%u attempted_regions=%u warm_capacity=%u",
              (unsigned long long)key_hash,
              key_len,
              value_size,
              attempted_regions,
              warm->capacity);
    atomic_fetch_add_explicit(&core->warm_alloc_fail, 1, memory_order_relaxed);
    return -1;
}

#if TLC_CORE_ENABLE_COLD_LAYER
static int cold_alloc_segment(tlc_core_t *core, uint32_t seg_id) {
    tlc_core_cold_layer_t *cold = &core->cold;
    RETURN_IF(seg_id >= cold->max_segments, -1);
    tlc_core_cold_segment_t *seg = &cold->segments[seg_id];
    RETURN_IF(seg->records && seg->values, 0);
    seg->capacity = cold->segment_capacity;
    seg->base_offset = (uint64_t)seg_id * cold->segment_capacity;
    seg->records = zcalloc(sizeof(*seg->records) * seg->capacity);
    seg->values = zcalloc((size_t)core->value_size * seg->capacity);
    RETURN_IF(!seg->records || !seg->values, -1);
    return 0;
}
#endif

static int cold_init(tlc_core_t *core,
                     uint32_t max_segments,
                     uint32_t segment_records) {
    tlc_core_cold_layer_t *cold = &core->cold;
    cold->max_segments = max_segments ?
        max_segments : TLC_CORE_DEFAULT_COLD_MAX_SEGMENTS;
    cold->segment_capacity = segment_records;
    if (cold->segment_capacity == 0 ||
        cold->segment_capacity > TLC_CORE_DEFAULT_COLD_SEGMENT_RECORDS) {
        cold->segment_capacity = TLC_CORE_DEFAULT_COLD_SEGMENT_RECORDS;
    }
    atomic_init(&cold->num_segments, 0);
    atomic_init(&cold->next_offset, 0);
    atomic_init(&cold->hits, 0);
    atomic_init(&cold->misses, 0);
#if !TLC_CORE_ENABLE_COLD_LAYER
    (void)core;
    return 0;
#else
    cold->segments =
        zcalloc(sizeof(*cold->segments) * cold->max_segments);
    RETURN_IF(!cold->segments, -1);
    cold->offset_index_size =
        vemb_v16_pow2_ceil_u32((uint64_t)core->warm_capacity * 2u);
    if (cold->offset_index_size < 1024)
        cold->offset_index_size = 1024;
    cold->oi_mask = cold->offset_index_size - 1u;
    cold->offset_index =
        zcalloc(sizeof(*cold->offset_index) * cold->offset_index_size);
    RETURN_IF(!cold->offset_index, -1);
    for (uint32_t i = 0; i < cold->offset_index_size; i++)
        atomic_init(&cold->offset_index[i], TLC_CORE_INVALID_OFFSET);
    atomic_store_explicit(&cold->num_segments, 1, memory_order_relaxed);
    int locks_rc = bitmap_init(&cold->locks, cold->max_segments);
    RETURN_IF(locks_rc != 0, -1);
    return cold_alloc_segment(core, 0);
#endif
}

#if TLC_CORE_ENABLE_COLD_LAYER
static tlc_core_cold_record_t *cold_record_by_offset(tlc_core_t *core,
                                                     uint64_t offset,
                                                     const uint8_t **value) {
    tlc_core_cold_layer_t *cold = &core->cold;
    uint32_t seg_id = (uint32_t)(offset / cold->segment_capacity);
    size_t local = (size_t)(offset % cold->segment_capacity);
    RETURN_IF(seg_id >= cold->max_segments, NULL);
    tlc_core_cold_segment_t *seg = &cold->segments[seg_id];
    RETURN_IF(!seg->records || !seg->values || local >= seg->count, NULL);
    if (value)
        *value = seg->values + local * core->value_size;
    return &seg->records[local];
}

static int cold_offset_matches(tlc_core_t *core,
                               uint64_t offset,
                               const char *key,
                               uint32_t key_len,
                               uint64_t key_hash) {
    const uint8_t *value = NULL;
    tlc_core_cold_record_t *record =
        cold_record_by_offset(core, offset, &value);
    (void)value;
    return record &&
           key_matches(key_hash, key, key_len,
                       record->key_hash, record->key, record->key_len);
}
#endif

#if TLC_CORE_ENABLE_COLD_LAYER
static void cold_index_store(tlc_core_t *core,
                             const char *key,
                             uint32_t key_len,
                             uint64_t key_hash,
                             uint64_t offset) {
    tlc_core_cold_layer_t *cold = &core->cold;
    uint32_t slot = vemb_v16_hash_mask_u64(key_hash, cold->oi_mask);
    for (uint32_t i = 0; i < TLC_CORE_COLD_PROBES; i++) {
        uint32_t pos = (slot + i) & cold->oi_mask;
        uint64_t cur =
            atomic_load_explicit(&cold->offset_index[pos],
                                 memory_order_acquire);
        if (cur == TLC_CORE_INVALID_OFFSET ||
            cold_offset_matches(core, cur, key, key_len, key_hash)) {
            atomic_store_explicit(&cold->offset_index[pos],
                                  offset,
                                  memory_order_release);
            return;
        }
    }
    atomic_store_explicit(&cold->offset_index[slot],
                          offset,
                          memory_order_release);
}
#endif

#if TLC_CORE_ENABLE_COLD_LAYER
static uint64_t cold_index_find_locked(tlc_core_t *core,
                                       const char *key,
                                       uint32_t key_len,
                                       uint64_t key_hash) {
    tlc_core_cold_layer_t *cold = &core->cold;
    uint32_t slot = vemb_v16_hash_mask_u64(key_hash, cold->oi_mask);
    for (uint32_t i = 0; i < TLC_CORE_COLD_PROBES; i++) {
        uint32_t pos = (slot + i) & cold->oi_mask;
        uint64_t offset =
            atomic_load_explicit(&cold->offset_index[pos],
                                 memory_order_acquire);
        if (offset == TLC_CORE_INVALID_OFFSET)
            break;
        if (cold_offset_matches(core, offset, key, key_len, key_hash))
            return offset;
    }
    return TLC_CORE_INVALID_OFFSET;
}
#endif

static int cold_append(tlc_core_t *core,
                       const char *key,
                       uint32_t key_len,
                       uint64_t key_hash,
                       const void *value,
                       uint32_t value_size) {
#if !TLC_CORE_ENABLE_COLD_LAYER
    (void)core;
    (void)key;
    (void)key_len;
    (void)key_hash;
    (void)value;
    (void)value_size;
    return 0;
#else
    tlc_core_cold_layer_t *cold = &core->cold;
    bitmap_lock_blocking(&cold->locks, 0);
    uint64_t existing_offset =
        cold_index_find_locked(core, key, key_len, key_hash);
    if (existing_offset != TLC_CORE_INVALID_OFFSET) {
        const uint8_t *ignored = NULL;
        tlc_core_cold_record_t *existing =
            cold_record_by_offset(core, existing_offset, &ignored);
        if (existing) {
            existing->key_hash = key_hash;
            existing->offset = existing_offset;
            existing->key_len = key_len;
            existing->value_size = value_size;
            memcpy(existing->key, key, key_len);
            uint32_t seg_id =
                (uint32_t)(existing_offset / cold->segment_capacity);
            size_t local =
                (size_t)(existing_offset % cold->segment_capacity);
            memcpy(cold->segments[seg_id].values +
                       local * core->value_size,
                   value,
                   value_size);
            bitmap_unlock(&cold->locks, 0);
            return 0;
        }
    }

    uint64_t offset =
        atomic_load_explicit(&cold->next_offset, memory_order_relaxed);
    uint32_t seg_id = (uint32_t)(offset / cold->segment_capacity);
    size_t local = (size_t)(offset % cold->segment_capacity);
    if (seg_id >= cold->max_segments ||
        cold_alloc_segment(core, seg_id) != 0) {
        bitmap_unlock(&cold->locks, 0);
        return -1;
    }
    int current_segments =
        atomic_load_explicit(&cold->num_segments, memory_order_relaxed);
    if ((int)seg_id >= current_segments) {
        atomic_store_explicit(&cold->num_segments, (int)seg_id + 1,
                              memory_order_relaxed);
    }
    tlc_core_cold_segment_t *seg = &cold->segments[seg_id];
    tlc_core_cold_record_t *record = &seg->records[local];
    record->key_hash = key_hash;
    record->offset = offset;
    record->key_len = key_len;
    record->value_size = value_size;
    memcpy(record->key, key, key_len);
    memcpy(seg->values + local * core->value_size, value, value_size);
    if (local >= seg->count)
        seg->count = local + 1;
    cold_index_store(core, key, key_len, key_hash, offset);
    atomic_store_explicit(&cold->next_offset, offset + 1,
                          memory_order_release);
    bitmap_unlock(&cold->locks, 0);
    return 0;
#endif
}

static int cold_lookup(tlc_core_t *core,
                       const char *key,
                       uint32_t key_len,
                       uint64_t key_hash,
                       const uint8_t **value,
                       uint32_t *value_size) {
#if !TLC_CORE_ENABLE_COLD_LAYER
    (void)core;
    (void)key;
    (void)key_len;
    (void)key_hash;
    (void)value;
    (void)value_size;
    return -1;
#else
    tlc_core_cold_layer_t *cold = &core->cold;
    bitmap_lock_blocking(&cold->locks, 0);
    uint32_t slot = vemb_v16_hash_mask_u64(key_hash, cold->oi_mask);
    for (uint32_t i = 0; i < TLC_CORE_COLD_PROBES; i++) {
        uint32_t pos = (slot + i) & cold->oi_mask;
        uint64_t offset =
            atomic_load_explicit(&cold->offset_index[pos],
                                 memory_order_acquire);
        if (offset == TLC_CORE_INVALID_OFFSET)
            break;
        const uint8_t *cold_value = NULL;
        tlc_core_cold_record_t *record =
            cold_record_by_offset(core, offset, &cold_value);
        if (record &&
            key_matches(key_hash, key, key_len,
                        record->key_hash, record->key, record->key_len)) {
            *value = cold_value;
            *value_size = record->value_size;
            atomic_fetch_add_explicit(&cold->hits, 1, memory_order_relaxed);
            bitmap_unlock(&cold->locks, 0);
            return 0;
        }
    }
    atomic_fetch_add_explicit(&cold->misses, 1, memory_order_relaxed);
    bitmap_unlock(&cold->locks, 0);
    return -1;
#endif
}

int tlc_core_copy_warm_location_value(tlc_core_t *core,
                                      uint64_t key_hash,
                                      const tlc_warm_location_t *location,
                                      void *value_out,
                                      uint32_t value_out_size,
                                      uint32_t retry_budget) {
    RETURN_IF(IS_INVALID_LOCATION(*location) || value_out_size < core->value_size, -1);
    tlc_core_warm_region_runtime_t *region = NULL;
    uint32_t region_index = UINT32_MAX;
    int rc = resolve_warm_region_for_location(core,
                                           location,
                                           &region,
                                           &region_index);
    RETURN_IF(rc != 0 || !region->slot_meta || !region->mapped_addr, -1);
    uint32_t attempts = retry_budget ? retry_budget : 1u;
    vemb_v16_warm_slot_meta_t *slot_meta = &region->slot_meta[location->local_slot];
    for (uint32_t attempt = 0; attempt < attempts; attempt++) {
        warm_slot_snapshot_t snapshot;
        tlc_core_warm_verify_rc_t rc = warm_slot_read_snapshot(slot_meta, &snapshot);
        if (rc == TLC_CORE_WARM_VERIFY_BUSY) {
            cpu_relax();
            continue;
        }
        if (rc != TLC_CORE_WARM_VERIFY_OK ||
            !warm_slot_snapshot_matches(&snapshot,
                                        key_hash,
                                        0,
                                        location->bytes,
                                        location->owner_generation)) {
            break;
        }

        sve_streaming_load_f32(region->mapped_addr + location->offset, value_out, location->bytes);
        atomic_thread_fence(memory_order_acquire);
        uint64_t seq2 = atomic_load_explicit(&slot_meta->write_seq, memory_order_acquire);
        if (snapshot.stable_seq == seq2 && seqlock_seq_is_stable(seq2)) {
            warm_slot_record_read_access(slot_meta);
            return 0;
        }
        cpu_relax();
    }

    atomic_fetch_add_explicit(&core->warm_stale_handle_reject, 1, memory_order_relaxed);
    return -1;
}

int tlc_core_create(tlc_core_t **out, const tlc_core_config_t *config) {
    RETURN_IF(!out || !config || config->value_size == 0 ||
              config->warm_capacity == 0 ||
              config->warm_capacity > INT32_MAX ||
              !config->warm_regions ||
              config->warm_region_count == 0,
              -1);

    tlc_core_t *core = zcalloc(sizeof(*core));
    RETURN_IF(!core, -1);
    core->value_size = config->value_size;
    core->warm_capacity = config->warm_capacity;
    atomic_init(&core->warm_alloc_local, 0);
    atomic_init(&core->warm_alloc_remote, 0);
    atomic_init(&core->warm_alloc_fallback, 0);
    atomic_init(&core->warm_alloc_cold_spill, 0);
    atomic_init(&core->warm_alloc_fail, 0);
    atomic_init(&core->warm_eviction_success, 0);
    atomic_init(&core->warm_eviction_fail, 0);
    atomic_init(&core->warm_same_key_overwrite, 0);
    atomic_init(&core->warm_stale_handle_reject, 0);
    atomic_init(&core->remote_meta_stale, 0);
    atomic_init(&core->lookup_cache_hit, 0);
    atomic_init(&core->lookup_cache_miss, 0);
    atomic_init(&core->lookup_warm_local_hit, 0);
    atomic_init(&core->lookup_warm_imported_hit, 0);
    atomic_init(&core->lookup_cold_promote, 0);
    atomic_init(&core->lookup_final_miss, 0);
    for (uint32_t i = 0; i < TLC_CORE_MAX_TOTAL_WARM_REGIONS; i++) {
        atomic_init(&core->region_lookup_hits[i], 0);
        atomic_init(&core->region_cold_promotes[i], 0);
    }
    atomic_init(&core->source_fence_active_count, 0);
    atomic_init(&core->tombstone_active_count, 0);
    atomic_init(&core->key_meta_count, 0);

    if (hot_init(core, config->hot_capacity) != 0 ||
        location_cache_init(core, config->hot_capacity) != 0 ||
        warm_init(core, config) != 0 ||
        cold_init(core,
                  config->cold_max_segments,
                  config->cold_segment_records) != 0 ||
        key_meta_init(core) != 0) {
        tlc_core_destroy(core);
        return -1;
    }

    *out = core;
    return 0;
}

int tlc_core_attach_warm_region(tlc_core_t *core,
                                const tlc_core_warm_region_config_t *region,
                                uint32_t *region_index) {
    RETURN_IF(!core || !region || !region_index, -1);
    RETURN_IF(region->backend_type != VEMB_V16_REGION_UB, -1);
    tlc_core_warm_layer_t *warm = &core->warm;
    for (uint32_t i = 0; i < warm->region_count; i++) {
        if (warm->regions[i].region_id == region->region_id) {
            *region_index = i;
            return 0;
        }
    }

    uint32_t runtime_count = warm_runtime_region_count(warm);
    for (uint32_t i = 0; i < runtime_count; i++) {
        if (warm->runtime_regions[i].region_id == region->region_id) {
            *region_index = warm->region_count + i;
            return 0;
        }
    }

    RETURN_IF(runtime_count >= TLC_CORE_MAX_WARM_REGIONS, -1);
    RETURN_IF(warm_region_init_one(core,
                                   &warm->runtime_regions[runtime_count],
                                   region,
                                   warm->region_count + runtime_count) != 0,
              -1);
    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&warm->runtime_region_count,
                          runtime_count + 1u,
                          memory_order_release);
    *region_index = warm->region_count + runtime_count;
    return 0;
}

void tlc_core_destroy(tlc_core_t *core) {
    RETURN_IF(!core);
    bitmap_destroy(&core->warm.locks);
    bitmap_destroy(&core->cold.locks);
    if (core->hold.table) zfree(core->hold.table);
    location_cache_destroy(core);
    if (core->warm.entries) zfree(core->warm.entries);
    if (core->warm.hash_table) zfree(core->warm.hash_table);
    if (core->warm.regions) zfree(core->warm.regions);
    if (core->warm.vnodes) zfree(core->warm.vnodes);
    if (core->key_meta_shards) {
        for (uint32_t i = 0; i < core->key_meta_shard_count; i++) {
            zfree(core->key_meta_shards[i].entries);
        }
        zfree(core->key_meta_shards);
    }
    bitmap_destroy(&core->key_meta_locks);
    if (core->cold.segments) {
        for (uint32_t i = 0; i < core->cold.max_segments; i++) {
            if (core->cold.segments[i].records)
                zfree(core->cold.segments[i].records);
            if (core->cold.segments[i].values)
                zfree(core->cold.segments[i].values);
        }
    }
    if (core->cold.segments) zfree(core->cold.segments);
    if (core->cold.offset_index) zfree(core->cold.offset_index);
    zfree(core);
}

int tlc_core_get_warm_slot(tlc_core_t *core,
                           const char *key,
                           uint32_t key_len,
                           uint64_t key_hash,
                           uint32_t *warm_slot) {
    tlc_warm_location_t location = tlc_invalid_location;
    int rc = tlc_core_get_warm_location(core, key, key_len, key_hash,
                                        &location);
    if (rc == 0 && warm_slot)
        *warm_slot = location.local_slot;
    return rc;
}

static int tlc_core_get_warm_location_raw(tlc_core_t *core,
                                          const char *key,
                                          uint32_t key_len,
                                          uint64_t key_hash,
                                          tlc_warm_location_t *location) {
    if (location_cache_get(core, key, key_len, key_hash, location) == 0) {
        atomic_fetch_add_explicit(&core->lookup_cache_hit, 1,
                                  memory_order_relaxed);
        note_lookup_location(core, location, 0);
        return 0;
    }
    atomic_fetch_add_explicit(&core->lookup_cache_miss, 1,
                              memory_order_relaxed);
    int32_t hot_idx = hot_get(core, key_hash);
    if (warm_validate_idx(core, hot_idx, key, key_len, key_hash, location) == 0) {
        note_lookup_location(core, location, 0);
        goto found;
    }
    if (warm_lookup(core, key, key_len, key_hash, location) == 0) {
        note_lookup_location(core, location, 0);
        goto found;
    }

    const uint8_t *cold_value = NULL;
    uint32_t cold_value_size = 0;
    int cold_rc = cold_lookup(core, key, key_len, key_hash,
                              &cold_value, &cold_value_size);
    if (cold_rc != 0) {
        atomic_fetch_add_explicit(&core->lookup_final_miss, 1,
                                  memory_order_relaxed);
        return -1;
    }
    RETURN_IF(cold_value_size != core->value_size, -1);
    int warm_rc = warm_put(core, key, key_len, key_hash,
                           cold_value, cold_value_size, location);
    RETURN_IF(warm_rc != 0, -1);
    atomic_fetch_add_explicit(&core->lookup_cold_promote, 1,
                              memory_order_relaxed);
    note_lookup_location(core, location, 1);

found:
    location_cache_put(core, key, key_len, key_hash, location);
    return 0;
}

int tlc_core_get_warm_location(tlc_core_t *core,
                               const char *key,
                               uint32_t key_len,
                               uint64_t key_hash,
                               tlc_warm_location_t *location) {
    RETURN_IF(!key_valid(key, key_len), -1);
    RETURN_IF(key_meta_blocks_source_access(core, key, key_len, key_hash), -1);
    int rc = tlc_core_get_warm_location_raw(core,
                                            key,
                                            key_len,
                                            key_hash,
                                            location);
    RETURN_IF(!!rc, -1);
    RETURN_IF(key_meta_blocks_source_access(core, key, key_len, key_hash), -1);
    return 0;
}

int tlc_core_get_warm_location_stable_read(tlc_core_t *core,
                                           const char *key,
                                           uint32_t key_len,
                                           uint64_t key_hash,
                                           tlc_warm_location_t *location) {
    RETURN_IF(!key_valid(key, key_len), -1);
    if (tlc_core_source_fence_active(core) || tombstone_filter_active(core)) {
        return tlc_core_get_warm_location(core, key, key_len, key_hash, location);
    }
    return tlc_core_get_warm_location_raw(core, key, key_len, key_hash, location);
}

int tlc_core_get_cached_warm_location(tlc_core_t *core,
                                      const char *key,
                                      uint32_t key_len,
                                      uint64_t key_hash,
                                      tlc_warm_location_t *location) {
    RETURN_IF(!key_valid(key, key_len), -1);
    RETURN_IF(key_meta_blocks_source_access(core, key, key_len, key_hash), -1);
    int rc = location_cache_peek(core, key, key_len, key_hash, location);
    RETURN_IF(!!rc, -1);
    RETURN_IF(key_meta_blocks_source_access(core, key, key_len, key_hash), -1);
    return 0;
}

int tlc_core_put(tlc_core_t *core,
                 const char *key,
                 uint32_t key_len,
                 uint64_t key_hash,
                 const void *value,
                 uint32_t value_size,
                 uint32_t *warm_slot) {
    tlc_warm_location_t location = tlc_invalid_location;
    int rc = tlc_core_put_location_epoch(core,
                                         key,
                                         key_len,
                                         key_hash,
                                         value,
                                         value_size,
                                         0,
                                         0,
                                         &location);
    if (rc == 0 && warm_slot)
        *warm_slot = location.local_slot;
    return rc;
}

static void key_meta_commit_location_locked(tlc_core_t *core,
                                            tlc_core_key_meta_entry_t *key_meta,
                                            const char *key,
                                            uint32_t key_len,
                                            uint64_t key_hash,
                                            const tlc_warm_location_t *location,
                                            uint64_t topology_epoch,
                                            int enforce_epoch,
                                            int update_location_cache) {
    key_meta->key_version++;
    if (enforce_epoch && topology_epoch > key_meta->topology_epoch)
        key_meta->topology_epoch = topology_epoch;
    key_meta_set_tombstone_locked(core, key_meta, 0);
    key_meta->location = *location;
    if (update_location_cache)
        location_cache_put(core, key, key_len, key_hash, location);
}

int tlc_core_put_location_epoch(tlc_core_t *core,
                                const char *key,
                                uint32_t key_len,
                                uint64_t key_hash,
                                const void *value,
                                uint32_t value_size,
                                uint64_t topology_epoch,
                                int enforce_epoch,
                                tlc_warm_location_t *location) {
    int valid_key = key_valid(key, key_len);
    const char *failure_reason = NULL;
    tlc_core_key_meta_shard_t *shard = NULL;
    tlc_core_key_meta_entry_t *failure_meta = NULL;
    int update_location_cache = 0;
    if (unlikely(!valid_key || value_size != core->value_size)) {
        failure_reason = !valid_key ? "invalid_key" : "value_size_mismatch";
        goto rollback;
    }

    shard = key_meta_shard_for_hash(core, key_hash);
    bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
    tlc_core_key_meta_entry_t *key_meta = key_meta_find_locked(core, key, key_len, key_hash, 0);
    if (tlc_core_source_fence_active(core) &&
        key_meta &&
        key_meta_state_blocks_source_access(key_meta->migration_state)) {
        failure_reason = "source_fence_blocked";
        failure_meta = key_meta;
        goto rollback;
    }
    if (enforce_epoch && key_meta && topology_epoch < key_meta->topology_epoch) {
        failure_reason = "stale_epoch";
        failure_meta = key_meta;
        goto rollback;
    }
    if (!key_meta && shard->count >= shard->capacity) {
        failure_reason = "key_meta_full";
        goto rollback;
    }
    int cold_rc = cold_append(core, key, key_len, key_hash, value, value_size);
    if (cold_rc != 0) {
        failure_reason = "cold_append_failed";
        failure_meta = key_meta;
        goto rollback;
    }

    if (key_meta && !key_meta->tombstone &&
        IS_VALID_LOCATION(key_meta->location) &&
        warm_overwrite_location(core,
                                key,
                                key_len,
                                key_hash,
                                &key_meta->location,
                                value,
                                value_size,
                                location) == 0) {
        update_location_cache = 1;
        goto commit;
    }

    if (warm_put(core, key, key_len, key_hash,
                 value, value_size, location) == 0) {
        key_meta = key_meta_find_locked(core, key, key_len, key_hash, 1);
        update_location_cache = 1;
        goto commit;
    }

#if !TLC_CORE_ENABLE_COLD_LAYER
    failure_reason = "warm_put_failed_cold_disabled";
    failure_meta = key_meta;
    goto rollback;
#else
    atomic_fetch_add_explicit(&core->warm_alloc_cold_spill, 1,
                              memory_order_relaxed);
    *location = tlc_invalid_location;
    key_meta = key_meta_find_locked(core, key, key_len, key_hash, 1);
    update_location_cache = 0;
    goto commit;
#endif

commit:
    if (!key_meta) {
        failure_reason = update_location_cache ?
            "key_meta_create_after_warm_failed update_location_cache=1" :
            "key_meta_create_after_spill_failed update_location_cache=0";
        goto rollback;
    }
    key_meta_commit_location_locked(core,
                                    key_meta,
                                    key,
                                    key_len,
                                    key_hash,
                                    location,
                                    topology_epoch,
                                    enforce_epoch,
                                    update_location_cache);
    bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    return 0;

rollback:
    tlc_core_log_put_failure(failure_reason,
                             core,
                             key_hash,
                             key_len,
                             value_size,
                             topology_epoch,
                             enforce_epoch,
                             failure_meta);
    if (shard)
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    return -1;
}

int tlc_core_delete_with_epoch(tlc_core_t *core,
                               const char *key,
                               uint32_t key_len,
                               uint64_t key_hash,
                               uint64_t topology_epoch,
                               tlc_core_key_migration_info_t *info) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!core || !valid_key, -1);
    if (info)
        memset(info, 0, sizeof(*info));

    tlc_core_key_meta_shard_t *shard =
        key_meta_shard_for_hash(core, key_hash);
    bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
    tlc_core_key_meta_entry_t *key_meta =
        key_meta_find_locked(core, key, key_len, key_hash, 0);
    if (!key_meta ||
        key_meta->tombstone ||
        (tlc_core_source_fence_active(core) &&
         key_meta_state_blocks_source_access(key_meta->migration_state)) ||
        topology_epoch < key_meta->topology_epoch) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        return -1;
    }

    key_meta->key_version++;
    if (topology_epoch > key_meta->topology_epoch)
        key_meta->topology_epoch = topology_epoch;
    key_meta_set_tombstone_locked(core, key_meta, 1);
    key_meta->location = tlc_invalid_location;
    key_meta_fill_info(key_meta, info);
    bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    return 0;
}

int tlc_core_cold_append(tlc_core_t *core,
                         const char *key,
                         uint32_t key_len,
                         uint64_t key_hash,
                         const void *value,
                         uint32_t value_size) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!core || !valid_key || value_size != core->value_size, -1);
    return cold_append(core, key, key_len, key_hash, value, value_size);
}

int tlc_core_get_migration_info(tlc_core_t *core,
                                const char *key,
                                uint32_t key_len,
                                uint64_t key_hash,
                                tlc_core_key_migration_info_t *info) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!core || !info || !valid_key, -1);

    tlc_core_key_meta_shard_t *shard =
        key_meta_shard_for_hash(core, key_hash);
    bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
    tlc_core_key_meta_entry_t *key_meta =
        key_meta_find_locked(core, key, key_len, key_hash, 0);
    if (!key_meta) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        return -1;
    }
    key_meta_fill_info(key_meta, info);
    bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    return 0;
}

int tlc_core_mark_migrating_in_shard(tlc_core_t *core,
                                     const char *key,
                                     uint32_t key_len,
                                     uint64_t key_hash,
                                     uint64_t topology_epoch,
                                     uint32_t target_owner,
                                     uint32_t shard_id,
                                     tlc_core_key_migration_info_t *info) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!core || !valid_key || target_owner == UINT32_MAX, -1);

    tlc_core_key_meta_shard_t *shard =
        key_meta_shard_for_hash(core, key_hash);
    bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
    tlc_core_key_meta_entry_t *key_meta =
        key_meta_find_locked(core, key, key_len, key_hash, 0);
    if (!key_meta || key_meta->tombstone) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        return -1;
    }
    if (key_meta->migration_state != TLC_CORE_KEY_SOURCE_ACTIVE &&
        key_meta->migration_state != TLC_CORE_KEY_MIGRATING) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        return -1;
    }
    if (key_meta->migration_state == TLC_CORE_KEY_MIGRATING &&
        (key_meta->target_owner != target_owner ||
         key_meta->shard_id != shard_id)) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        return -1;
    }
    if (topology_epoch < key_meta->topology_epoch) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        return -1;
    }
    key_meta->migration_state = TLC_CORE_KEY_MIGRATING;
    key_meta->target_owner = target_owner;
    key_meta->shard_id = shard_id;
    if (topology_epoch > key_meta->topology_epoch)
        key_meta->topology_epoch = topology_epoch;
    key_meta_fill_info(key_meta, info);
    bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    return 0;
}

int tlc_core_mark_cutover(tlc_core_t *core,
                          const char *key,
                          uint32_t key_len,
                          uint64_t key_hash,
                          uint64_t topology_epoch,
                          uint32_t target_owner,
                          tlc_core_key_migration_info_t *info) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!core || !valid_key || target_owner == UINT32_MAX, -1);

    tlc_core_key_meta_shard_t *shard =
        key_meta_shard_for_hash(core, key_hash);
    bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
    tlc_core_key_meta_entry_t *key_meta =
        key_meta_find_locked(core, key, key_len, key_hash, 0);
    if (!key_meta ||
        (key_meta->migration_state != TLC_CORE_KEY_MIGRATING &&
         key_meta->migration_state != TLC_CORE_KEY_CUTOVER) ||
        key_meta->target_owner != target_owner ||
        topology_epoch < key_meta->topology_epoch) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        return -1;
    }
    key_meta_note_source_fence_transition(core,
                                          key_meta->migration_state,
                                          TLC_CORE_KEY_CUTOVER);
    key_meta->migration_state = TLC_CORE_KEY_CUTOVER;
    if (topology_epoch > key_meta->topology_epoch)
        key_meta->topology_epoch = topology_epoch;
    if (topology_epoch > key_meta->owner_epoch)
        key_meta->owner_epoch = topology_epoch;
    key_meta_fill_info(key_meta, info);
    bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    return 0;
}

int tlc_core_mark_source_gc(tlc_core_t *core,
                            const char *key,
                            uint32_t key_len,
                            uint64_t key_hash,
                            uint64_t topology_epoch,
                            uint32_t target_owner,
                            tlc_core_key_migration_info_t *info) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!core || !valid_key || target_owner == UINT32_MAX, -1);

    tlc_core_key_meta_shard_t *shard =
        key_meta_shard_for_hash(core, key_hash);
    bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
    tlc_core_key_meta_entry_t *key_meta =
        key_meta_find_locked(core, key, key_len, key_hash, 0);
    if (!key_meta ||
        (key_meta->migration_state != TLC_CORE_KEY_CUTOVER &&
         key_meta->migration_state != TLC_CORE_KEY_SOURCE_GC) ||
        key_meta->target_owner != target_owner ||
        topology_epoch < key_meta->topology_epoch) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        return -1;
    }
    key_meta_note_source_fence_transition(core,
                                          key_meta->migration_state,
                                          TLC_CORE_KEY_SOURCE_GC);
    key_meta->migration_state = TLC_CORE_KEY_SOURCE_GC;
    if (topology_epoch > key_meta->topology_epoch)
        key_meta->topology_epoch = topology_epoch;
    if (topology_epoch > key_meta->owner_epoch)
        key_meta->owner_epoch = topology_epoch;
    key_meta_fill_info(key_meta, info);
    bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    return 0;
}

int tlc_core_accept_owner_lease(tlc_core_t *core,
                                const char *key,
                                uint32_t key_len,
                                uint64_t key_hash,
                                uint64_t topology_epoch,
                                uint64_t owner_epoch,
                                uint32_t target_owner,
                                tlc_core_key_migration_info_t *info) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!core || !valid_key || target_owner == UINT32_MAX, -1);

    tlc_core_key_meta_shard_t *shard =
        key_meta_shard_for_hash(core, key_hash);
    bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
    tlc_core_key_meta_entry_t *key_meta =
        key_meta_find_locked(core, key, key_len, key_hash, 0);
    if (!key_meta ||
        key_meta->migration_state != TLC_CORE_KEY_DEST_COMMITTED ||
        key_meta->target_owner != target_owner ||
        topology_epoch < key_meta->topology_epoch ||
        owner_epoch < key_meta->owner_epoch) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        return -1;
    }
    if (topology_epoch > key_meta->topology_epoch)
        key_meta->topology_epoch = topology_epoch;
    if (owner_epoch > key_meta->owner_epoch)
        key_meta->owner_epoch = owner_epoch;
    key_meta_fill_info(key_meta, info);
    bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    return 0;
}

int tlc_core_key_is_source_cutover(tlc_core_t *core,
                                   const char *key,
                                   uint32_t key_len,
                                   uint64_t key_hash,
                                   tlc_core_key_migration_info_t *info) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!core || !valid_key, -1);
    if (!tlc_core_source_fence_active(core))
        return 0;

    int cutover = 0;
    tlc_core_key_meta_shard_t *shard =
        key_meta_shard_for_hash(core, key_hash);
    bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
    tlc_core_key_meta_entry_t *key_meta =
        key_meta_find_locked(core, key, key_len, key_hash, 0);
    if (key_meta) {
        cutover = key_meta_state_blocks_source_access(key_meta->migration_state);
        if (cutover)
            key_meta_fill_info(key_meta, info);
    }
    bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    return cutover;
}

int tlc_core_has_uncommitted_source_migrations(tlc_core_t *core) {
    RETURN_IF(!core, 1);
    int has_uncommitted = 0;
    for (uint32_t s = 0; s < core->key_meta_shard_count; s++) {
        tlc_core_key_meta_shard_t *shard = &core->key_meta_shards[s];
        bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
        for (uint32_t i = 0; i < shard->capacity; i++) {
            const tlc_core_key_meta_entry_t *meta = &shard->entries[i];
            if (meta->occupied &&
                meta->migration_state == TLC_CORE_KEY_MIGRATING &&
                meta->target_owner != UINT32_MAX) {
                has_uncommitted = 1;
                break;
            }
        }
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        if (has_uncommitted)
            break;
    }
    return has_uncommitted;
}

int tlc_core_collect_migration_keys(tlc_core_t *core,
                                    uint64_t topology_epoch,
                                    uint32_t target_owner,
                                    uint32_t shard_id,
                                    uint32_t migration_state,
                                    tlc_core_migration_key_ref_t *keys,
                                    uint32_t max_keys,
                                    uint32_t *key_count) {
    RETURN_IF(!core || !keys || !key_count ||
              target_owner == UINT32_MAX,
              -1);
    *key_count = 0;
    for (uint32_t s = 0; s < core->key_meta_shard_count; s++) {
        tlc_core_key_meta_shard_t *shard = &core->key_meta_shards[s];
        bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
        for (uint32_t i = 0; i < shard->capacity; i++) {
            const tlc_core_key_meta_entry_t *meta = &shard->entries[i];
            if (!meta->occupied ||
                meta->topology_epoch != topology_epoch ||
                meta->target_owner != target_owner ||
                meta->shard_id != shard_id ||
                (migration_state != UINT32_MAX &&
                 meta->migration_state != migration_state)) {
                continue;
            }
            if (*key_count >= max_keys) {
                bitmap_unlock(&core->key_meta_locks, shard->lock_id);
                return -1;
            }
            tlc_core_migration_key_ref_t *entry = &keys[*key_count];
            memset(entry, 0, sizeof(*entry));
            entry->key_hash = meta->key_hash;
            entry->key_len = meta->key_len;
            memcpy(entry->key, meta->key, meta->key_len);
            key_meta_fill_info(meta, &entry->info);
            (*key_count)++;
        }
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    }
    return 0;
}

int tlc_core_collect_migration_keys_page(tlc_core_t *core,
                                         uint64_t topology_epoch,
                                         uint32_t target_owner,
                                         uint32_t shard_id,
                                         uint32_t migration_state,
                                         tlc_core_migration_key_ref_t *keys,
                                         uint32_t max_keys,
                                         uint32_t *key_count,
                                         uint32_t *remaining_count) {
    RETURN_IF(!core || !keys || !key_count || !remaining_count ||
              target_owner == UINT32_MAX || max_keys == 0,
              -1);
    *key_count = 0;
    *remaining_count = 0;

    for (uint32_t s = 0; s < core->key_meta_shard_count; s++) {
        tlc_core_key_meta_shard_t *shard = &core->key_meta_shards[s];
        bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
        for (uint32_t i = 0; i < shard->capacity; i++) {
            const tlc_core_key_meta_entry_t *meta = &shard->entries[i];
            if (!meta->occupied ||
                meta->topology_epoch != topology_epoch ||
                meta->target_owner != target_owner ||
                meta->shard_id != shard_id ||
                (migration_state != UINT32_MAX &&
                 meta->migration_state != migration_state)) {
                continue;
            }
            if (*key_count >= max_keys) {
                (*remaining_count)++;
                continue;
            }
            tlc_core_migration_key_ref_t *entry = &keys[*key_count];
            memset(entry, 0, sizeof(*entry));
            entry->key_hash = meta->key_hash;
            entry->key_len = meta->key_len;
            memcpy(entry->key, meta->key, meta->key_len);
            key_meta_fill_info(meta, &entry->info);
            (*key_count)++;
        }
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    }
    return 0;
}

int tlc_core_count_migration_keys(tlc_core_t *core,
                                  uint64_t topology_epoch,
                                  uint32_t target_owner,
                                  uint32_t shard_id,
                                  uint32_t migration_state,
                                  uint32_t *key_count) {
    RETURN_IF(!core || !key_count || target_owner == UINT32_MAX, -1);
    *key_count = 0;

    for (uint32_t s = 0; s < core->key_meta_shard_count; s++) {
        tlc_core_key_meta_shard_t *shard = &core->key_meta_shards[s];
        bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
        for (uint32_t i = 0; i < shard->capacity; i++) {
            const tlc_core_key_meta_entry_t *meta = &shard->entries[i];
            if (!meta->occupied ||
                meta->topology_epoch != topology_epoch ||
                meta->target_owner != target_owner ||
                meta->shard_id != shard_id ||
                (migration_state != UINT32_MAX &&
                 meta->migration_state != migration_state)) {
                continue;
            }
            (*key_count)++;
        }
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    }
    return 0;
}

int tlc_core_collect_migration_ranges(tlc_core_t *core,
                                      uint64_t topology_epoch,
                                      uint32_t migration_state,
                                      tlc_core_migration_range_ref_t *ranges,
                                      uint32_t max_ranges,
                                      uint32_t *range_count) {
    RETURN_IF(!core || !ranges || !range_count || max_ranges == 0, -1);
    *range_count = 0;
    for (uint32_t s = 0; s < core->key_meta_shard_count; s++) {
        tlc_core_key_meta_shard_t *shard = &core->key_meta_shards[s];
        bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
        for (uint32_t i = 0; i < shard->capacity; i++) {
            const tlc_core_key_meta_entry_t *meta = &shard->entries[i];
            if (!meta->occupied ||
                meta->topology_epoch != topology_epoch ||
                meta->target_owner == UINT32_MAX ||
                (migration_state != UINT32_MAX &&
                 meta->migration_state != migration_state)) {
                continue;
            }

            tlc_core_migration_range_ref_t *range = NULL;
            for (uint32_t j = 0; j < *range_count; j++) {
                if (ranges[j].topology_epoch == meta->topology_epoch &&
                    ranges[j].target_owner == meta->target_owner &&
                    ranges[j].shard_id == meta->shard_id) {
                    range = &ranges[j];
                    break;
                }
            }
            if (!range) {
                if (*range_count >= max_ranges) {
                    bitmap_unlock(&core->key_meta_locks, shard->lock_id);
                    return -1;
                }
                range = &ranges[*range_count];
                memset(range, 0, sizeof(*range));
                range->topology_epoch = meta->topology_epoch;
                range->target_owner = meta->target_owner;
                range->shard_id = meta->shard_id;
                (*range_count)++;
            }
            range->key_count++;
        }
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    }
    return 0;
}

int tlc_core_collect_source_active_keys(tlc_core_t *core,
                                        uint32_t *cursor,
                                        tlc_core_migration_key_ref_t *keys,
                                        uint32_t max_keys,
                                        uint32_t *key_count,
                                        int *done) {
    RETURN_IF(!core || !cursor || !keys || !key_count || !done ||
              max_keys == 0,
              -1);
    *key_count = 0;
    *done = 0;

    uint32_t flat = *cursor;
    uint32_t next = flat;
    for (uint32_t s = 0; s < core->key_meta_shard_count; s++) {
        tlc_core_key_meta_shard_t *shard = &core->key_meta_shards[s];
        uint32_t shard_base = s * shard->capacity;
        if (flat >= shard_base + shard->capacity)
            continue;
        uint32_t i = flat > shard_base ? flat - shard_base : 0;
        bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
        for (; i < shard->capacity; i++) {
            next = shard_base + i;
            const tlc_core_key_meta_entry_t *meta = &shard->entries[i];
            if (!meta->occupied ||
                meta->tombstone ||
                meta->migration_state != TLC_CORE_KEY_SOURCE_ACTIVE) {
                continue;
            }
            if (*key_count >= max_keys) {
                bitmap_unlock(&core->key_meta_locks, shard->lock_id);
                *cursor = next;
                return 0;
            }
            tlc_core_migration_key_ref_t *entry = &keys[*key_count];
            memset(entry, 0, sizeof(*entry));
            entry->key_hash = meta->key_hash;
            entry->key_len = meta->key_len;
            memcpy(entry->key, meta->key, meta->key_len);
            key_meta_fill_info(meta, &entry->info);
            (*key_count)++;
        }
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        flat = shard_base + shard->capacity;
        next = flat;
    }
    *cursor = next;
    if (next >= core->key_meta_capacity)
        *done = 1;
    return 0;
}

int tlc_core_snapshot(tlc_core_t *core,
                      const char *key,
                      uint32_t key_len,
                      uint64_t key_hash,
                      uint32_t source_owner,
                      uint32_t target_owner,
                      tlc_core_migration_snapshot_t *snapshot,
                      void *value_out,
                      uint32_t value_out_size) {
    int valid_key = key_valid(key, key_len);
    RETURN_IF(!core || !snapshot || !valid_key, -1);

    tlc_core_key_meta_shard_t *shard =
        key_meta_shard_for_hash(core, key_hash);
    bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
    tlc_core_key_meta_entry_t *meta =
        key_meta_find_locked(core, key, key_len, key_hash, 0);
    if (!meta) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        return -1;
    }
    if (key_meta_state_blocks_source_access(meta->migration_state)) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        return -1;
    }

    memset(snapshot, 0, sizeof(*snapshot));
    snapshot->key_hash = key_hash;
    snapshot->key_len = key_len;
    snapshot->key_version = meta->key_version;
    snapshot->topology_epoch = meta->topology_epoch;
    snapshot->owner_epoch = meta->owner_epoch;
    snapshot->migration_state = meta->migration_state;
    snapshot->source_owner = source_owner;
    snapshot->target_owner = target_owner;
    snapshot->tombstone = meta->tombstone;
    snapshot->value_size = meta->tombstone ? 0 : core->value_size;
    snapshot->shard_id = meta->shard_id;
    snapshot->location = meta->location;
    memcpy(snapshot->key, key, key_len);

    if (snapshot->tombstone) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        return 0;
    }

    tlc_warm_location_t location = tlc_invalid_location;
    if (tlc_core_get_warm_location_raw(core, key, key_len,
                                       key_hash, &location) != 0) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        return -1;
    }
    if (tlc_core_copy_warm_location_value(core,
                                          key_hash,
                                          &location,
                                          value_out,
                                          value_out_size,
                                          TLC_CORE_WARM_BUSY_RETRIES) != 0) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        return -1;
    }
    snapshot->location = location;
    meta->location = location;
    bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    return 0;
}

int tlc_core_apply_migration(tlc_core_t *core,
                             const tlc_core_migration_snapshot_t *snapshot,
                             const void *value,
                             uint32_t value_size,
                             tlc_core_migration_apply_status_t *status,
                             tlc_warm_location_t *location) {
    RETURN_IF(!core || !snapshot || !status ||
              snapshot->key_len == 0 ||
              snapshot->key_len > VEMB_V16_MAX_KEY_LEN,
              -1);
    if (!snapshot->tombstone &&
        (!value || value_size != core->value_size ||
         snapshot->value_size != core->value_size)) {
        *status = TLC_CORE_MIGRATION_ERROR;
        return -1;
    }

    tlc_core_key_meta_shard_t *shard =
        key_meta_shard_for_hash(core, snapshot->key_hash);
    bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
    tlc_core_key_meta_entry_t *key_meta =
        key_meta_find_locked(core,
                             snapshot->key,
                             snapshot->key_len,
                             snapshot->key_hash,
                             1);
    if (!key_meta) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        *status = TLC_CORE_MIGRATION_ERROR;
        return -1;
    }
    if (incoming_snapshot_is_stale(key_meta, snapshot)) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        *status = TLC_CORE_MIGRATION_STALE_REJECTED;
        return 0;
    }
    if (incoming_snapshot_is_duplicate(key_meta, snapshot)) {
        if (location)
            *location = key_meta->location;
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        *status = TLC_CORE_MIGRATION_DUPLICATE;
        return 0;
    }

    tlc_warm_location_t new_location = tlc_invalid_location;
    if (!snapshot->tombstone) {
        if (cold_append(core,
                        snapshot->key,
                        snapshot->key_len,
                        snapshot->key_hash,
                        value,
                        value_size) != 0 ||
            warm_put(core,
                     snapshot->key,
                     snapshot->key_len,
                     snapshot->key_hash,
                     value,
                     value_size,
                     &new_location) != 0) {
            bitmap_unlock(&core->key_meta_locks, shard->lock_id);
            *status = TLC_CORE_MIGRATION_RETRY;
            return 0;
        }
    }

    key_meta->key_version = snapshot->key_version;
    key_meta->topology_epoch = snapshot->topology_epoch;
    key_meta->owner_epoch = snapshot->owner_epoch;
    key_meta->migration_state = TLC_CORE_KEY_DEST_COMMITTED;
    key_meta->source_owner = snapshot->source_owner;
    key_meta->target_owner = snapshot->target_owner;
    key_meta_set_tombstone_locked(core, key_meta, snapshot->tombstone ? 1u : 0u);
    key_meta->shard_id = snapshot->shard_id;
    key_meta->location = snapshot->tombstone ? tlc_invalid_location : new_location;
    if (!snapshot->tombstone)
        location_cache_put(core,
                           snapshot->key,
                           snapshot->key_len,
                           snapshot->key_hash,
                           &key_meta->location);
    if (location)
        *location = key_meta->location;
    bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    *status = TLC_CORE_MIGRATION_APPLIED;
    return 0;
}

int tlc_core_validate_warm_location(tlc_core_t *core,
                                    uint64_t key_hash,
                                    const tlc_warm_location_t *location) {
    RETURN_IF(!location || IS_INVALID_LOCATION(*location), -1);
    tlc_core_warm_region_runtime_t *region = NULL;
    uint32_t region_index = UINT32_MAX;
    RETURN_IF(resolve_warm_region_for_location(core,
                                               location,
                                               &region,
                                               &region_index) != 0,
              -1);
    if (warm_slot_probe_status(region,
                               region_index,
                               location->local_slot,
                               key_hash,
                               0,
                               location->bytes,
                               location->owner_generation,
                               NULL) != TLC_CORE_WARM_VERIFY_OK) {
        if (location->local_slot < region->capacity_slots) {
            vemb_v16_warm_slot_meta_t *slot_meta = &region->slot_meta[location->local_slot];
            uint32_t state = atomic_load_explicit(&slot_meta->state, memory_order_acquire);
            uint64_t owner_generation = atomic_load_explicit(
                        &slot_meta->owner_generation, memory_order_acquire);
            uint64_t write_seq = atomic_load_explicit(
                        &slot_meta->write_seq, memory_order_acquire);
            uint32_t cold_state = atomic_load_explicit(
                        &slot_meta->cold_state, memory_order_acquire);
            serverLog(LL_NOTICE,
                      "tlc diag stale warm location: key_hash=%llu location_region_id=%u location_region_index=%u location_slot=%u location_offset=%llu location_bytes=%u location_generation=%llu current_state=%u current_region_id=%u current_slot=%u current_key_hash=%llu current_fp=%llu current_bytes=%u current_generation=%llu current_write_seq=%llu current_cold_state=%u",
                      (unsigned long long)key_hash,
                      location->region_id,
                      region_index,
                      location->local_slot,
                      (unsigned long long)location->offset,
                      location->bytes,
                      (unsigned long long)location->owner_generation,
                      state,
                      slot_meta->region_id,
                      slot_meta->local_slot,
                      (unsigned long long)slot_meta->key_hash,
                      (unsigned long long)slot_meta->key_fingerprint,
                      slot_meta->bytes,
                      (unsigned long long)owner_generation,
                      (unsigned long long)write_seq,
                      cold_state);
        }
        atomic_fetch_add_explicit(&core->warm_stale_handle_reject, 1, memory_order_relaxed);
        return -1;
    }
    return 0;
}

void tlc_core_note_remote_meta_stale(tlc_core_t *core) {
    atomic_fetch_add_explicit(&core->remote_meta_stale, 1,
                              memory_order_relaxed);
}

void tlc_core_get_stats(tlc_core_t *core, tlc_core_stats_t *stats) {
    memset(stats, 0, sizeof(*stats));
    tlc_core_warm_layer_t *warm = &core->warm;
    uint32_t runtime_count = warm_runtime_region_count(warm);
    stats->warm_region_count = warm->region_count + runtime_count;
    for (uint32_t i = 0; i < warm->region_count; i++) {
        if (warm_region_full(&warm->regions[i])) {
            stats->warm_region_full_count++;
        }
    }
    for (uint32_t i = 0; i < runtime_count; i++) {
        if (warm_region_full(&warm->runtime_regions[i])) {
            stats->warm_region_full_count++;
        }
    }
    stats->warm_alloc_local = atomic_load_explicit(&core->warm_alloc_local, memory_order_relaxed);
    stats->warm_alloc_remote = atomic_load_explicit(&core->warm_alloc_remote, memory_order_relaxed);
    stats->warm_alloc_fallback = atomic_load_explicit(&core->warm_alloc_fallback, memory_order_relaxed);
    stats->warm_alloc_cold_spill = atomic_load_explicit(&core->warm_alloc_cold_spill, memory_order_relaxed);
    stats->warm_alloc_fail = atomic_load_explicit(&core->warm_alloc_fail, memory_order_relaxed);
    stats->warm_eviction_success = atomic_load_explicit(&core->warm_eviction_success, memory_order_relaxed);
    stats->warm_eviction_fail = atomic_load_explicit(&core->warm_eviction_fail, memory_order_relaxed);
    stats->warm_same_key_overwrite = atomic_load_explicit(&core->warm_same_key_overwrite, memory_order_relaxed);
    stats->warm_stale_handle_reject = atomic_load_explicit(&core->warm_stale_handle_reject, memory_order_relaxed);
    stats->remote_meta_stale = atomic_load_explicit(&core->remote_meta_stale, memory_order_relaxed);
    stats->lookup_cache_hit = atomic_load_explicit(
        &core->lookup_cache_hit, memory_order_relaxed);
    stats->lookup_cache_miss = atomic_load_explicit(
        &core->lookup_cache_miss, memory_order_relaxed);
    stats->lookup_warm_local_hit = atomic_load_explicit(
        &core->lookup_warm_local_hit, memory_order_relaxed);
    stats->lookup_warm_imported_hit = atomic_load_explicit(
        &core->lookup_warm_imported_hit, memory_order_relaxed);
    stats->lookup_cold_promote = atomic_load_explicit(
        &core->lookup_cold_promote, memory_order_relaxed);
    stats->lookup_final_miss = atomic_load_explicit(
        &core->lookup_final_miss, memory_order_relaxed);
    uint64_t total = stats->warm_alloc_local + stats->warm_alloc_remote;
    if (total)
        stats->warm_region_hash_local_pct =
            (stats->warm_alloc_local * 100u) / total;
}

uint32_t tlc_core_get_region_stats(tlc_core_t *core,
                                   tlc_core_region_stats_t *regions,
                                   uint32_t max_regions) {
    tlc_core_warm_layer_t *warm = &core->warm;
    uint32_t runtime_count = warm_runtime_region_count(warm);
    uint32_t n = warm->region_count + runtime_count;
    RETURN_IF (!regions || max_regions == 0, n);
    if (n > max_regions)
        n = max_regions;
    for (uint32_t i = 0; i < n; i++) {
        tlc_core_warm_region_runtime_t *region =
            i < warm->region_count ? &warm->regions[i] :
                &warm->runtime_regions[i - warm->region_count];
        regions[i] = (tlc_core_region_stats_t){
            .capacity_slots = region->capacity_slots,
            .region_id = region->region_id,
            .is_local = region->is_local,
            .used_slots = warm_region_used_slots(region),
            .lookup_hits = i < TLC_CORE_MAX_TOTAL_WARM_REGIONS ?
                atomic_load_explicit(&core->region_lookup_hits[i],
                                     memory_order_relaxed) : 0,
            .cold_promotes = i < TLC_CORE_MAX_TOTAL_WARM_REGIONS ?
                atomic_load_explicit(&core->region_cold_promotes[i],
                                     memory_order_relaxed) : 0,
            .full = warm_region_full(region),
        };
    }
    return n;
}
