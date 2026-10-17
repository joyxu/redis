#include "tlc_core.h"
#include "config.h"
#include "redisassert.h"
#include "cpu_relax.h"
#include "macro.h"
#include "sve_operation.h"
#include "vemb_v16_hash.h"
#include "vemb_v16_log.h"
#include "vemb_v16_protocol.h"
#include "vemb_v16_util.h"
#include "zmalloc.h"
#ifdef VEMB_V16_LOOKUP_DIAGNOSTICS
#include "monotonic.h"

static _Thread_local struct {
    uint64_t calls, cache_hit, cache_miss, indexed_ok, indexed_fail, scan;
    uint64_t hit_samples, hit_ns, miss_samples, cache_miss_ns;
    uint64_t lock_ns, index_ns, validate_ns;
    uint64_t meta_finds, meta_probes, meta_probe_max;
} lookup_diag;

void tlc_core_log_lookup_diagnostics(uint32_t worker_id) {
    serverLog(LL_WARNING,
        "lookup_diag worker=%u calls=%llu hit=%llu miss=%llu indexed_ok=%llu indexed_fail=%llu scan=%llu hit_samples=%llu hit_ns=%llu miss_samples=%llu cache_miss_ns=%llu lock_ns=%llu index_ns=%llu validate_ns=%llu meta_finds=%llu meta_probes=%llu meta_probe_max=%llu",
        worker_id, (unsigned long long)lookup_diag.calls,
        (unsigned long long)lookup_diag.cache_hit,
        (unsigned long long)lookup_diag.cache_miss,
        (unsigned long long)lookup_diag.indexed_ok,
        (unsigned long long)lookup_diag.indexed_fail,
        (unsigned long long)lookup_diag.scan,
        (unsigned long long)lookup_diag.hit_samples,
        (unsigned long long)lookup_diag.hit_ns,
        (unsigned long long)lookup_diag.miss_samples,
        (unsigned long long)lookup_diag.cache_miss_ns,
        (unsigned long long)lookup_diag.lock_ns,
        (unsigned long long)lookup_diag.index_ns,
        (unsigned long long)lookup_diag.validate_ns,
        (unsigned long long)lookup_diag.meta_finds,
        (unsigned long long)lookup_diag.meta_probes,
        (unsigned long long)lookup_diag.meta_probe_max);
}
#endif

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#define TLC_CORE_WARM_PLACE_RETRIES 3u
#define TLC_CORE_WARM_BUSY_RETRIES 1024u
#define TLC_CORE_DIAG_STALE_LOG_LIMIT 64u
#define TLC_CORE_DIAG_PUT_LOG_LIMIT 128u
#define TLC_CORE_KEY_META_SHARDS 256u
#define TLC_CORE_KEY_META_MIN_SHARD_CAPACITY 64u
#define TLC_CORE_KEY_META_MAX_INITIAL_CAPACITY 256u
#define TLC_CORE_LOCATION_CACHE_PROBES 4u
#define TLC_CORE_LOCATION_CACHE_MIN_CAPACITY 1024u

#define SEQLOCK_STATE_STABLE UINT64_C(0)
#define SEQLOCK_STATE_WRITING UINT64_C(1)
#define SEQLOCK_STATE_MASK UINT64_C(1)
#define SEQLOCK_STATE_STEP UINT64_C(1)
#define SEQLOCK_VERSION_STEP UINT64_C(2)
#define SEQLOCK_EMPTY UINT64_C(0)
#define TLC_CORE_APPLY_EVENT_STALE (-2)

typedef enum tlc_core_warm_verify_rc {
    TLC_CORE_WARM_VERIFY_OK = 0,
    TLC_CORE_WARM_VERIFY_MISS = 1,
    TLC_CORE_WARM_VERIFY_BUSY = 2,
} tlc_core_warm_verify_rc_t;

typedef struct tlc_core_location_cache_entry {
    atomic_uint_fast64_t seq;
    atomic_uint_fast64_t key_hash;
    atomic_uint_fast64_t key_fingerprint;
    atomic_uint_fast32_t key_len;
    atomic_uint_fast32_t region_index;
    atomic_uint_fast32_t local_slot;
    atomic_uint_fast64_t owner_generation;
} tlc_core_location_cache_entry_t;

typedef struct tlc_core_location_cache {
    tlc_core_location_cache_entry_t *entries;
    uint32_t capacity;
    uint32_t mask;
} tlc_core_location_cache_t;

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
    /* Dense append order is stable across growth and migration pagination. */
    tlc_core_key_meta_entry_t *entries;
    uint32_t *hash_table; /* Record index + 1; zero is empty. */
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
    uint64_t slot_stride_size;
    /* One owner core, shared by its worker threads; readers have no pool. */
    pthread_mutex_t alloc_mu;
    uint32_t alloc_mu_init;
    uint32_t next_slot;
    uint32_t *free_slots;
    uint32_t free_count;
} tlc_core_warm_region_runtime_t;

typedef struct tlc_core_warm_layer {
    tlc_core_warm_region_runtime_t *regions;
    atomic_uint_fast32_t runtime_region_count;
    tlc_core_warm_region_runtime_t runtime_regions[TLC_CORE_MAX_WARM_REGIONS];
    uint32_t region_count;
    atomic_uint_fast32_t active_region;
} tlc_core_warm_layer_t;

struct tlc_core {
    uint32_t value_size;
    tlc_core_location_cache_t location_cache;
    tlc_core_warm_layer_t warm;
    tlc_cold_t *persistent_cold;
    atomic_uint_fast64_t ha_term;
    atomic_bool write_fenced;
    tlc_core_replica_event_sink_fn replica_event_sink;
    void *replica_event_sink_arg;
    pthread_mutex_t replica_apply_mu;
    uint32_t replica_apply_mu_init;
    uint64_t replica_applied_seq;
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
    state_bitmap_t key_meta_locks;
    atomic_uint_fast64_t key_meta_count;
};

static void tlc_core_reset_recovery_state(tlc_core_t *core);

static int tlc_core_cold_append_sink(const tlc_cold_event_input_t *event,
                                     uint64_t seq,
                                     void *arg);

static const tlc_warm_location_t tlc_invalid_location = {
    .region_id = TLC_CORE_INVALID_REGION_ID,
    .region_index = UINT32_MAX,
    .local_slot = TLC_CORE_INVALID_SLOT,
    .bytes = 0,
    .offset = UINT64_MAX,
    .owner_generation = 0,
};

typedef struct tlc_core_checkpoint_entry {
    uint64_t key_hash;
    uint64_t key_version;
    uint64_t topology_epoch;
    uint64_t owner_epoch;
    uint32_t migration_state;
    uint32_t source_owner;
    uint32_t target_owner;
    uint32_t tombstone;
    uint32_t shard_id;
    uint32_t key_len;
    uint32_t value_len;
} tlc_core_checkpoint_entry_t;

typedef struct tlc_core_checkpoint_state {
    uint8_t *data;
    size_t length;
    size_t capacity;
} tlc_core_checkpoint_state_t;

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
    uint64_t key_meta_count = atomic_load_explicit(
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
              "tlc_core put_with_epoch failed: reason=%s key_hash=%llu key_len=%u value_size=%u expected_value_size=%u topology_epoch=%llu enforce_epoch=%d key_meta_count=%llu source_fence_count=%llu meta_present=%d meta_state=%u meta_epoch=%llu owner_epoch=%llu key_version=%llu source=%u target=%u tombstone=%u shard=%u",
              reason,
              (unsigned long long)key_hash,
              key_len,
              value_size,
              core->value_size,
              (unsigned long long)topology_epoch,
              enforce_epoch,
              (unsigned long long)key_meta_count,
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

uint64_t tlc_core_warm_capacity(const tlc_core_t *core) {
    const tlc_core_warm_layer_t *warm = &core->warm;
    uint64_t capacity = 0;
    for (uint32_t i = 0; i < warm->region_count; i++)
        capacity += warm->regions[i].capacity_slots;
    uint32_t runtime_count = warm_runtime_region_count(warm);
    for (uint32_t i = 0; i < runtime_count; i++)
        capacity += warm->runtime_regions[i].capacity_slots;
    return capacity;
}

/* Owner initialization only; no key discovery and no request-time scan. */
static void warm_region_init_pool(tlc_core_warm_region_runtime_t *region) {
    region->next_slot = 0;
    region->free_count = 0;
    for (uint32_t slot = 0; slot < region->capacity_slots; slot++) {
        if (atomic_load_explicit(&region->slot_meta[slot].state,
                                 memory_order_acquire) != VEMB_V16_WARM_SLOT_FREE)
            region->next_slot = slot + 1u;
    }
    for (uint32_t slot = region->next_slot; slot > 0; slot--) {
        if (atomic_load_explicit(&region->slot_meta[slot - 1u].state,
                                 memory_order_acquire) == VEMB_V16_WARM_SLOT_FREE)
            region->free_slots[region->free_count++] = slot - 1u;
    }
}

static int warm_region_init_one(tlc_core_t *core,
                                tlc_core_warm_region_runtime_t *dst,
                                const tlc_core_warm_region_config_t *src,
                                uint32_t region_index) {
    uint64_t stride = src->slot_stride_size ? src->slot_stride_size : src->value_size;
    RETURN_IF(!src->mapped_addr || !src->slot_meta ||
              src->value_size != core->value_size ||
              stride < src->value_size || src->region_bytes < stride,
              -1);
    uint64_t capacity_slots = src->region_bytes / stride;
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
        .slot_stride_size = stride,
        .slot_meta = src->slot_meta,
        .region_bytes = src->region_bytes,
        .mapped_addr = src->mapped_addr,
    };
    if (dst->is_local) {
        dst->free_slots = zmalloc(sizeof(*dst->free_slots) * (size_t)capacity_slots);
        RETURN_IF(!dst->free_slots, -1);
        if (pthread_mutex_init(&dst->alloc_mu, NULL) != 0) {
            zfree(dst->free_slots);
            dst->free_slots = NULL;
            return -1;
        }
        dst->alloc_mu_init = 1;
        warm_region_init_pool(dst);
    }
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

/* Caller holds the shard lock (or is initializing before publication).
 * Allocation failure leaves both arrays unchanged. Entries keep their ordinals;
 * no pointer into entries may outlive the lock or a subsequent reserve call. */
static int key_meta_resize_locked(tlc_core_key_meta_shard_t *shard,
                                   uint32_t capacity) {
    RETURN_IF((uint64_t)capacity * sizeof(tlc_core_key_meta_entry_t) > SIZE_MAX, -1);
    tlc_core_key_meta_entry_t *entries = ztrycalloc((size_t)capacity * sizeof(*entries));
    uint32_t *hash_table = ztrycalloc((size_t)capacity * sizeof(*hash_table));
    if (!entries || !hash_table) {
        zfree(entries);
        zfree(hash_table);
        return -1;
    }
    if (shard->count)
        memcpy(entries, shard->entries, (size_t)shard->count * sizeof(*entries));
    uint32_t mask = capacity - 1u;
    for (uint32_t i = 0; i < shard->count; i++) {
        uint32_t pos = vemb_v16_hash_mask_u64(entries[i].key_hash, mask);
        while (hash_table[pos])
            pos = (pos + 1u) & mask;
        hash_table[pos] = i + 1u;
    }
    zfree(shard->entries);
    zfree(shard->hash_table);
    shard->entries = entries;
    shard->hash_table = hash_table;
    shard->capacity = capacity;
    shard->mask = mask;
    return 0;
}

static int key_meta_reserve_locked(tlc_core_key_meta_shard_t *shard) {
    if (shard->count < shard->capacity - shard->capacity / 4u)
        return 0;
    RETURN_IF(shard->capacity >= VEMB_V16_MAX_POWER_U32, -1);
    return key_meta_resize_locked(shard, shard->capacity * 2u);
}

static int key_meta_init(tlc_core_t *core) {
    uint32_t shard_count = TLC_CORE_KEY_META_SHARDS;
    uint64_t target = (tlc_core_warm_capacity(core) * 4u + 2u) / 3u;
    uint64_t per_shard = (target + shard_count - 1u) / shard_count;
    if (per_shard > TLC_CORE_KEY_META_MAX_INITIAL_CAPACITY)
        per_shard = TLC_CORE_KEY_META_MAX_INITIAL_CAPACITY;
    uint32_t capacity = vemb_v16_pow2_ceil_u32(per_shard);
    if (capacity < TLC_CORE_KEY_META_MIN_SHARD_CAPACITY)
        capacity = TLC_CORE_KEY_META_MIN_SHARD_CAPACITY;
    core->key_meta_shards = zcalloc_num(shard_count, sizeof(*core->key_meta_shards));
    RETURN_IF(!core->key_meta_shards, -1);
    RETURN_IF(bitmap_init(&core->key_meta_locks, shard_count) != 0, -1);
    core->key_meta_shard_count = shard_count;
    for (uint32_t i = 0; i < shard_count; i++) {
        tlc_core_key_meta_shard_t *shard = &core->key_meta_shards[i];
        shard->lock_id = i;
        RETURN_IF(key_meta_resize_locked(shard, capacity) != 0, -1);
    }
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
    uint32_t pos = vemb_v16_hash_mask_u64(key_hash, shard->mask);
#ifdef VEMB_V16_LOOKUP_DIAGNOSTICS
    lookup_diag.meta_finds++;
    uint64_t diag_probes = 0;
#endif
    while (shard->hash_table[pos]) {
#ifdef VEMB_V16_LOOKUP_DIAGNOSTICS
        lookup_diag.meta_probes++;
        if (++diag_probes > lookup_diag.meta_probe_max)
            lookup_diag.meta_probe_max = diag_probes;
#endif
        tlc_core_key_meta_entry_t *entry =
            &shard->entries[shard->hash_table[pos] - 1u];
        if (key_matches(key_hash, key, key_len, entry->key_hash,
                        entry->key, entry->key_len))
            return entry;
        pos = (pos + 1u) & shard->mask;
    }
    if (!create)
        return NULL;
    RETURN_IF(key_meta_reserve_locked(shard) != 0, NULL);
    pos = vemb_v16_hash_mask_u64(key_hash, shard->mask);
    while (shard->hash_table[pos])
        pos = (pos + 1u) & shard->mask;
    tlc_core_key_meta_entry_t *entry = &shard->entries[shard->count];
    key_meta_init_entry_locked(entry, key, key_len, key_hash);
    shard->hash_table[pos] = ++shard->count;
    atomic_fetch_add_explicit(&core->key_meta_count, 1, memory_order_relaxed);
    return entry;
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

static uint64_t key_fingerprint(const char *key, uint32_t key_len);

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
        atomic_init(&entry->key_fingerprint, 0);
        atomic_init(&entry->key_len, 0);
        atomic_init(&entry->region_index, UINT32_MAX);
        atomic_init(&entry->local_slot, TLC_CORE_INVALID_SLOT);
        atomic_init(&entry->owner_generation, 0);
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
    uint64_t fp = key_fingerprint(key, key_len);
    if (atomic_load_explicit(&entry->key_hash, memory_order_relaxed) != key_hash ||
        atomic_load_explicit(&entry->key_len, memory_order_relaxed) != key_len ||
        atomic_load_explicit(&entry->key_fingerprint,
                             memory_order_relaxed) != fp)
        return 0;
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

    atomic_store_explicit(&entry->key_fingerprint,
                          key_fingerprint(key, key_len),
                          memory_order_relaxed);
    atomic_store_explicit(&entry->region_index, location->region_index, memory_order_relaxed);
    atomic_store_explicit(&entry->local_slot, location->local_slot, memory_order_relaxed);
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

static int location_cache_fill_hint(tlc_core_t *core,
                                    uint32_t region_index,
                                    uint32_t local_slot,
                                    uint64_t owner_generation,
                                    tlc_warm_location_t *location) {
    tlc_core_warm_region_runtime_t *region =
        warm_region_by_index(&core->warm, region_index);
    RETURN_IF(!region || local_slot >= region->capacity_slots || !location, -1);
    *location = (tlc_warm_location_t){
        .region_id = region->region_id,
        .region_index = region_index,
        .local_slot = local_slot,
        .bytes = region->value_size,
        .offset = (uint64_t)local_slot * region->slot_stride_size,
        .owner_generation = owner_generation,
    };
    return 0;
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

    atomic_init(&warm->active_region, 0);
    for (uint32_t i = 0; i < region_count; i++) {
        const tlc_core_warm_region_config_t *src = &regions[i];
        RETURN_IF(warm_region_init_one(core, &warm->regions[i], src, i) != 0, -1);
    }
    return 0;
}

static uint64_t key_fingerprint(const char *key, uint32_t key_len) {
    return vemb_v16_fnv1a64_bytes(key, key_len);
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
                               (uint64_t)local_slot * region->slot_stride_size,
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
        .offset = (uint64_t)local_slot * region->slot_stride_size,
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
    if (region->is_local)
        warm_slot_record_read_access(slot_meta);
    if (location) {
        *location = (tlc_warm_location_t){
            .region_id = region->region_id,
            .region_index = region_index,
            .local_slot = local_slot,
            .bytes = snapshot.bytes,
            .offset = (uint64_t)local_slot * region->slot_stride_size,
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
                  (uint64_t)location->local_slot * region->slot_stride_size ||
              location->bytes != region->value_size,
              -1);
    *region_out = region;
    *region_index_out = region_index;
    return 0;
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

        tlc_warm_location_t cached;
        if (location_cache_fill_hint(
                core,
                atomic_load_explicit(&entry->region_index, memory_order_relaxed),
                atomic_load_explicit(&entry->local_slot, memory_order_relaxed),
                atomic_load_explicit(&entry->owner_generation, memory_order_relaxed),
                &cached) != 0)
            continue;
        uint64_t seq2 = atomic_load_explicit(&entry->seq, memory_order_acquire);
        if (seq1 != seq2 || seqlock_seq_is_writing(seq2))
            continue;
        return (location_cache_validate_location(core, key_hash, &cached,
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

        tlc_warm_location_t cached;
        if (location_cache_fill_hint(
                core,
                atomic_load_explicit(&entry->region_index, memory_order_relaxed),
                atomic_load_explicit(&entry->local_slot, memory_order_relaxed),
                atomic_load_explicit(&entry->owner_generation, memory_order_relaxed),
                &cached) != 0)
            continue;
        uint64_t seq2 = atomic_load_explicit(&entry->seq, memory_order_acquire);
        if (seq1 != seq2 || seqlock_seq_is_writing(seq2)) continue;
        *location = cached;
        return 0;
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

static tlc_core_warm_verify_rc_t warm_overwrite_location(tlc_core_t *core,
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
    RETURN_IF(rc != 0 || !region->is_local, TLC_CORE_WARM_VERIFY_MISS);
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
            return TLC_CORE_WARM_VERIFY_OK;
        }
        // Only BUSY is retryable; MISS and other results should stop here.
        if (rc != TLC_CORE_WARM_VERIFY_BUSY) {
            return rc;
        }
        cpu_relax();
    }
    return TLC_CORE_WARM_VERIFY_BUSY;
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
    pthread_mutex_lock(&region->alloc_mu);
    uint32_t slot;
    if (region->free_count) {
        slot = region->free_slots[--region->free_count];
    } else if (region->next_slot < region->capacity_slots) {
        slot = region->next_slot++;
    } else {
        pthread_mutex_unlock(&region->alloc_mu);
        goto full;
    }
    pthread_mutex_unlock(&region->alloc_mu);
    int rc = warm_try_fill_free(region, region_index, slot, key_hash, fp,
                                value, value_size, location);
    /* A second allocator writer violates this region's ownership contract. */
    assert(rc == 0);
    return 0;
full:
#if TLC_CORE_ALLOW_LRU_EVICTION
    for (uint32_t round = 0; round < TLC_CORE_WARM_PLACE_RETRIES; round++) {
        uint32_t victim = UINT32_MAX;
        if (choose_victim_slot(region, 0, region->capacity_slots, &victim) == 0 &&
            warm_try_evict_and_fill(region, core, region_index, victim,
                                    key_hash, fp, value, value_size,
                                    location) == 0) {
            return 0;
        }
        if (victim == UINT32_MAX) {
            atomic_fetch_add_explicit(&core->warm_eviction_fail, 1, memory_order_relaxed);
        }
        cpu_relax();
    }
#else
    (void)core;
#endif
    return -1;
}

/* Caller holds this key's metadata shard lock; writes are routed to its owner. */
static int warm_put(tlc_core_t *core,
                    const char *key,
                    uint32_t key_len,
                    uint64_t key_hash,
                    const void *value,
                    uint32_t value_size,
                    tlc_warm_location_t *location) {
    tlc_core_key_meta_entry_t *meta = key_meta_find_locked(
        core, key, key_len, key_hash, 0);
    if (meta && IS_VALID_LOCATION(meta->location)) {
        tlc_core_warm_verify_rc_t rc = warm_overwrite_location(
            core, key, key_len, key_hash, &meta->location,
            value, value_size, location);
        if (rc != TLC_CORE_WARM_VERIFY_MISS)
            return rc == TLC_CORE_WARM_VERIFY_OK ? 0 : -1;
    }
    tlc_core_warm_layer_t *warm = &core->warm;
    uint64_t fp = key_fingerprint(key, key_len);
    uint32_t attempted_regions = 0;
    uint32_t count = warm->region_count + warm_runtime_region_count(warm);
    uint32_t start = atomic_load_explicit(&warm->active_region, memory_order_relaxed);
    for (uint32_t i = 0; i < count; i++) {
        uint32_t index = (start + i) % count;
        tlc_core_warm_region_runtime_t *region = warm_region_by_index(warm, index);
        if (!region->is_local)
            continue;
        attempted_regions++;
        if (warm_place_in_region(region, core, index, key_hash, fp,
                                  value, value_size, location) == 0) {
            atomic_store_explicit(&warm->active_region, index, memory_order_relaxed);
            atomic_fetch_add_explicit(&core->warm_alloc_local, 1, memory_order_relaxed);
            if (attempted_regions > 1)
                atomic_fetch_add_explicit(&core->warm_alloc_fallback, 1, memory_order_relaxed);
            return 0;
        }
    }
    serverLog(LL_WARNING,
              "tlc warm put failed: key_hash=%llu key_len=%u value_size=%u attempted_regions=%u warm_capacity=%llu",
              (unsigned long long)key_hash,
              key_len,
              value_size,
              attempted_regions,
              (unsigned long long)tlc_core_warm_capacity(core));
    atomic_fetch_add_explicit(&core->warm_alloc_fail, 1, memory_order_relaxed);
    return -1;
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
            if (region->is_local)
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
              !config->warm_regions ||
              config->warm_region_count == 0,
              -1);

    tlc_core_t *core = zcalloc(sizeof(*core));
    RETURN_IF(!core, -1);
    core->value_size = config->value_size;
    atomic_init(&core->ha_term, 0);
    atomic_init(&core->write_fenced, false);
    if (pthread_mutex_init(&core->replica_apply_mu, NULL) != 0) {
        zfree(core);
        return -1;
    }
    core->replica_apply_mu_init = 1;
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

    if (location_cache_init(core, config->hot_capacity) != 0 ||
        warm_regions_init(core, config) != 0 ||
        key_meta_init(core) != 0) {
        tlc_core_destroy(core);
        return -1;
    }

    *out = core;
    return 0;
}

int tlc_core_enable_cold(tlc_core_t *core,
                         const tlc_cold_config_t *cold_config) {
    RETURN_IF(!core || !cold_config || core->persistent_cold, -1);
    int rc = tlc_cold_open(&core->persistent_cold, cold_config);
    if (rc != 0) {
        serverLog(LL_WARNING,
                  "tlc_core COLD open failed: directory=%s",
                  cold_config->directory);
        return rc;
    }
    rc = tlc_core_recover_cold(core);
    if (rc != 0) {
        serverLog(LL_WARNING,
                  "failed to recover WARM from COLD AOF: directory=%s",
                  cold_config->directory);
        tlc_cold_close(core->persistent_cold);
        core->persistent_cold = NULL;
    } else if (core->replica_event_sink) {
        rc = tlc_cold_set_append_sink(core->persistent_cold,
                                      tlc_core_cold_append_sink, core);
    }
    return rc;
}

static int tlc_core_cold_append_sink(const tlc_cold_event_input_t *event,
                                     uint64_t seq,
                                     void *arg) {
    tlc_core_t *core = arg;
    return core->replica_event_sink(event, seq, core->replica_event_sink_arg);
}

int tlc_core_set_replica_event_sink(tlc_core_t *core,
                                    tlc_core_replica_event_sink_fn sink,
                                    void *arg) {
    RETURN_IF(!core, -1);
    core->replica_event_sink = sink;
    core->replica_event_sink_arg = arg;
    return core->persistent_cold ?
        tlc_cold_set_append_sink(core->persistent_cold,
                                 sink ? tlc_core_cold_append_sink : NULL,
                                 core) : 0;
}

uint64_t tlc_core_ha_term(const tlc_core_t *core) {
    return atomic_load_explicit(&core->ha_term, memory_order_acquire);
}

int tlc_core_set_ha_term(tlc_core_t *core, uint64_t ha_term) {
    if (ha_term == 0)
        return -1;
    uint64_t current = atomic_load_explicit(&core->ha_term, memory_order_acquire);
    for (;;) {
        if (ha_term < current)
            return -1;
        if (ha_term == current)
            return 0;
        if (atomic_compare_exchange_weak_explicit(
                &core->ha_term, &current, ha_term, memory_order_release,
                memory_order_acquire))
            return 0;
    }
}

uint64_t tlc_core_replica_applied_seq(const tlc_core_t *core) {
    return core ? core->replica_applied_seq : 0;
}

int tlc_core_set_write_fenced(tlc_core_t *core, int fenced) {
    if (!core)
        return -1;
    atomic_store_explicit(&core->write_fenced, fenced != 0,
                          memory_order_release);
    return 0;
}

int tlc_core_write_fenced(const tlc_core_t *core) {
    return core && atomic_load_explicit(&core->write_fenced,
                                        memory_order_acquire);
}

tlc_cold_t *tlc_core_get_cold(tlc_core_t *core) {
    return core ? core->persistent_cold : NULL;
}

uint32_t tlc_core_meta_shard_count(const tlc_core_t *core) {
    return core ? core->key_meta_shard_count : 0;
}

int tlc_core_attach_warm_region(tlc_core_t *core,
                                const tlc_core_warm_region_config_t *region,
                                uint32_t *region_index) {
    RETURN_IF(!core || !region || !region_index, -1);
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
    if (core->persistent_cold)
        tlc_cold_close(core->persistent_cold);
    location_cache_destroy(core);
    for (uint32_t i = 0; i < core->warm.region_count; i++) {
        if (core->warm.regions[i].alloc_mu_init)
            pthread_mutex_destroy(&core->warm.regions[i].alloc_mu);
        zfree(core->warm.regions[i].free_slots);
    }
    for (uint32_t i = 0; i < warm_runtime_region_count(&core->warm); i++) {
        if (core->warm.runtime_regions[i].alloc_mu_init)
            pthread_mutex_destroy(&core->warm.runtime_regions[i].alloc_mu);
        zfree(core->warm.runtime_regions[i].free_slots);
    }
    if (core->warm.regions) zfree(core->warm.regions);
    if (core->key_meta_shards) {
        for (uint32_t i = 0; i < core->key_meta_shard_count; i++) {
            zfree(core->key_meta_shards[i].entries);
            zfree(core->key_meta_shards[i].hash_table);
        }
        zfree(core->key_meta_shards);
    }
    bitmap_destroy(&core->key_meta_locks);
    if (core->replica_apply_mu_init)
        pthread_mutex_destroy(&core->replica_apply_mu);
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
                                          tlc_warm_location_t *location,
                                          int key_shard_locked) {
#ifdef VEMB_V16_LOOKUP_DIAGNOSTICS
    int sampled = (++lookup_diag.calls & 1023u) == 0;
    monotime diag_start = sampled ? getMonotonicNs() : 0;
#endif
    if (location_cache_get(core, key, key_len, key_hash, location) == 0) {
#ifdef VEMB_V16_LOOKUP_DIAGNOSTICS
        lookup_diag.cache_hit++;
        if (sampled) {
            lookup_diag.hit_samples++;
            lookup_diag.hit_ns += elapsedNs(diag_start);
        }
#endif
        return 0;
    }
#ifdef VEMB_V16_LOOKUP_DIAGNOSTICS
    lookup_diag.cache_miss++;
    if (sampled) {
        lookup_diag.miss_samples++;
        lookup_diag.cache_miss_ns += elapsedNs(diag_start);
        diag_start = getMonotonicNs();
    }
#endif
    tlc_core_key_meta_shard_t *shard = key_meta_shard_for_hash(core, key_hash);
    if (!key_shard_locked)
        bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
#ifdef VEMB_V16_LOOKUP_DIAGNOSTICS
    if (sampled) {
        lookup_diag.lock_ns += elapsedNs(diag_start);
        diag_start = getMonotonicNs();
    }
#endif
    tlc_core_key_meta_entry_t *meta = key_meta_find_locked(
        core, key, key_len, key_hash, 0);
#ifdef VEMB_V16_LOOKUP_DIAGNOSTICS
    if (sampled) {
        lookup_diag.index_ns += elapsedNs(diag_start);
        diag_start = getMonotonicNs();
    }
#endif
    int indexed = meta != NULL;
    int rc = -1;
    if (meta && !meta->tombstone &&
        !key_meta_state_blocks_source_access(meta->migration_state) &&
        IS_VALID_LOCATION(meta->location)) {
        rc = location_cache_validate_location(core, key_hash, &meta->location,
                                               location);
    }
    if (!key_shard_locked)
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
#ifdef VEMB_V16_LOOKUP_DIAGNOSTICS
    if (sampled)
        lookup_diag.validate_ns += elapsedNs(diag_start);
    if (rc == 0) lookup_diag.indexed_ok++;
    else if (indexed) lookup_diag.indexed_fail++;
#endif
    if (rc == 0)
        goto found;
    if (indexed)
        return -1;
    return -1;

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
                                            location,
                                            0);
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
    return tlc_core_get_warm_location_raw(core, key, key_len, key_hash, location, 0);
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

/* Caller holds the key shard lock. Only the owning allocator may recycle. */
static int warm_release_location(tlc_core_t *core, uint64_t key_hash,
                                  const tlc_warm_location_t *location) {
    if (IS_INVALID_LOCATION(*location))
        return 0;
    tlc_core_warm_region_runtime_t *region;
    uint32_t index;
    if (resolve_warm_region_for_location(core, location, &region, &index) != 0 ||
        !region->is_local)
        return -1;
    vemb_v16_warm_slot_meta_t *slot = &region->slot_meta[location->local_slot];
    for (uint32_t attempt = 0; attempt < TLC_CORE_WARM_BUSY_RETRIES; attempt++) {
        warm_slot_snapshot_t snapshot;
        tlc_core_warm_verify_rc_t rc = warm_slot_read_snapshot(slot, &snapshot);
        if (rc == TLC_CORE_WARM_VERIFY_BUSY) {
            cpu_relax();
            continue;
        }
        /* A stale location must never release a reused slot. */
        if (rc != TLC_CORE_WARM_VERIFY_OK ||
            !warm_slot_snapshot_matches(&snapshot, key_hash, 0,
                                         location->bytes, location->owner_generation))
            return 0;
        uint64_t seq;
        if (slot_seq_try_begin(slot, &seq) != 0)
            continue;
        atomic_store_explicit(&slot->state, VEMB_V16_WARM_SLOT_EVICTING, memory_order_release);
        atomic_fetch_add_explicit(&slot->owner_generation, 1, memory_order_relaxed);
        atomic_store_explicit(&slot->cold_state, VEMB_V16_WARM_SLOT_COLD_NONE, memory_order_relaxed);
        atomic_store_explicit(&slot->write_seq, seq + SEQLOCK_VERSION_STEP, memory_order_release);
        pthread_mutex_lock(&region->alloc_mu);
        atomic_store_explicit(&slot->state, VEMB_V16_WARM_SLOT_FREE, memory_order_release);
        assert(region->free_count < region->capacity_slots);
        region->free_slots[region->free_count++] = location->local_slot;
        pthread_mutex_unlock(&region->alloc_mu);
        return 0;
    }
    return -1;
}

static int tlc_core_persist_event_locked(tlc_core_t *core,
                                         const tlc_core_key_meta_entry_t *key_meta,
                                         const char *key,
                                         uint32_t key_len,
                                         uint64_t key_hash,
                                         uint32_t op,
                                         const void *value,
                                         uint32_t value_size,
                                         uint64_t topology_epoch) {
    if (!core->persistent_cold)
        return 0;
    uint64_t version = key_meta ? key_meta->key_version : 0;
    if (version == UINT64_MAX)
        return -1;
    tlc_core_key_meta_shard_t *shard = key_meta_shard_for_hash(core, key_hash);
    tlc_cold_event_input_t event = {
        .ha_term = atomic_load_explicit(&core->ha_term, memory_order_acquire),
        .topology_epoch = topology_epoch,
        .op = op,
        .meta_shard_id = shard->lock_id,
        .version = version + 1,
        .key = key,
        .key_len = key_len,
        .value = value,
        .value_len = value_size,
    };
    return tlc_cold_submit(core->persistent_cold, &event,
                           TLC_COLD_ACK_ACCEPTED, NULL);
}

static int tlc_core_apply_event(tlc_core_t *core,
                                const tlc_cold_event_input_t *event,
                                uint64_t seq) {
    uint64_t key_hash = vemb_v16_xxh3_64(event->key, event->key_len);
    tlc_core_key_meta_shard_t *shard = key_meta_shard_for_hash(core, key_hash);
    if (shard->lock_id != event->meta_shard_id) {
        serverLog(LL_WARNING,
                  "COLD event key-shard mismatch: seq=%llu key_hash=%llu event_shard=%u resolved_shard=%u",
                  (unsigned long long)seq,
                  (unsigned long long)key_hash,
                  event->meta_shard_id,
                  shard->lock_id);
        return -1;
    }
    bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
    tlc_core_key_meta_entry_t *meta = key_meta_find_locked(
        core, event->key, event->key_len, key_hash, 0);
    if (meta && event->topology_epoch < meta->topology_epoch) {
        uint64_t current_term = meta->topology_epoch;
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        serverLog(LL_WARNING,
                  "COLD event stale topology epoch rejected: seq=%llu key_hash=%llu event_epoch=%llu current_epoch=%llu",
                  (unsigned long long)seq,
                  (unsigned long long)key_hash,
                  (unsigned long long)event->topology_epoch,
                  (unsigned long long)current_term);
        return TLC_CORE_APPLY_EVENT_STALE;
    }
    if (meta && meta->key_version >= event->version) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        return 0;
    }
    if (event->op == TLC_COLD_OP_DEL) {
        if (!meta)
            meta = key_meta_find_locked(core, event->key, event->key_len,
                                        key_hash, 1);
        if (!meta) {
            bitmap_unlock(&core->key_meta_locks, shard->lock_id);
            serverLog(LL_WARNING,
                      "COLD event metadata allocation failed: seq=%llu key_hash=%llu",
                      (unsigned long long)seq,
                      (unsigned long long)key_hash);
            return -1;
        }
        if (warm_release_location(core, key_hash, &meta->location) != 0) {
            bitmap_unlock(&core->key_meta_locks, shard->lock_id);
            return -1;
        }
        meta->key_version = event->version;
        if (event->topology_epoch > meta->topology_epoch)
            meta->topology_epoch = event->topology_epoch;
        key_meta_set_tombstone_locked(core, meta, 1);
        meta->location = tlc_invalid_location;
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        return 0;
    }
    if (!meta && key_meta_reserve_locked(shard) != 0) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        return -1;
    }
    tlc_warm_location_t location = tlc_invalid_location;
    if (event->value_len != core->value_size ||
        warm_put(core, event->key, event->key_len, key_hash,
                 event->value, event->value_len, &location) != 0) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        serverLog(LL_WARNING,
                  "COLD event WARM publish failed: seq=%llu key_hash=%llu value_size=%u expected=%u",
                  (unsigned long long)seq,
                  (unsigned long long)key_hash,
                  event->value_len,
                  core->value_size);
        return -1;
    }
    if (!meta)
        meta = key_meta_find_locked(core, event->key, event->key_len,
                                    key_hash, 1);
    if (!meta) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        serverLog(LL_WARNING,
                      "COLD event metadata publish failed: seq=%llu key_hash=%llu",
                  (unsigned long long)seq,
                  (unsigned long long)key_hash);
        return -1;
    }
    meta->key_version = event->version;
    if (event->topology_epoch > meta->topology_epoch)
        meta->topology_epoch = event->topology_epoch;
    key_meta_set_tombstone_locked(core, meta, 0);
    meta->location = location;
    location_cache_put(core, event->key, event->key_len, key_hash, &location);
    bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    return 0;
}

static int tlc_core_recover_event(const tlc_cold_event_input_t *event,
                                  uint64_t seq,
                                  void *arg) {
    return tlc_core_apply_event((tlc_core_t *)arg, event, seq);
}

static int tlc_core_validate_replica_event(const tlc_core_t *core,
                                           const tlc_cold_event_input_t *event) {
    int key_shape = event->key && key_valid(event->key, event->key_len);
    uint64_t key_hash = key_shape ?
        vemb_v16_xxh3_64(event->key, event->key_len) : 0;
    int valid_shape = key_shape &&
        event->meta_shard_id < core->key_meta_shard_count &&
        (vemb_v16_mix32_u64(key_hash) &
         (core->key_meta_shard_count - 1u)) == event->meta_shard_id &&
        event->version != 0 &&
        ((event->op == TLC_COLD_OP_PUT && event->value &&
          event->value_len == core->value_size) ||
         (event->op == TLC_COLD_OP_DEL && !event->value &&
          event->value_len == 0));
    return valid_shape ? 0 : -1;
}

static int tlc_core_apply_replica_event_locked(
        tlc_core_t *core,
        const tlc_cold_event_input_t *event,
        uint64_t seq,
        tlc_core_replica_apply_status_t *status,
        int skip_warm_apply) {
    if (seq <= core->replica_applied_seq) {
        *status = TLC_CORE_REPLICA_APPLY_DUPLICATE;
        return 0;
    }
    if (seq != core->replica_applied_seq + 1) {
        *status = TLC_CORE_REPLICA_APPLY_GAP;
        return 0;
    }
    if (skip_warm_apply) {
        core->replica_applied_seq = seq;
        *status = TLC_CORE_REPLICA_APPLY_CHECKPOINTED;
        return 0;
    }

    int rc = tlc_core_apply_event(core, event, seq);
    if (rc == 0) {
        core->replica_applied_seq = seq;
        *status = TLC_CORE_REPLICA_APPLY_APPLIED;
    } else if (rc == TLC_CORE_APPLY_EVENT_STALE) {
        *status = TLC_CORE_REPLICA_APPLY_STALE;
    } else {
        *status = TLC_CORE_REPLICA_APPLY_ERROR;
    }
    return rc == 0 || rc == TLC_CORE_APPLY_EVENT_STALE ? 0 : -1;
}

int tlc_core_apply_replica_event(tlc_core_t *core,
                                 const tlc_cold_event_input_t *event,
                                 uint64_t seq,
                                 tlc_core_replica_apply_status_t *status) {
    RETURN_IF(!core || !event || !status || seq == 0, -1);
    if (tlc_core_validate_replica_event(core, event) != 0) {
        *status = TLC_CORE_REPLICA_APPLY_ERROR;
        return -1;
    }
    pthread_mutex_lock(&core->replica_apply_mu);
    int rc = tlc_core_apply_replica_event_locked(core, event, seq, status, 0);
    pthread_mutex_unlock(&core->replica_apply_mu);
    return rc;
}

int tlc_core_apply_resync_event(tlc_core_t *core,
                                const tlc_cold_event_input_t *event,
                                uint64_t seq,
                                const uint64_t *captured_seq,
                                uint32_t shard_count,
                                tlc_core_replica_apply_status_t *status) {
    RETURN_IF(!core || !event || !captured_seq || !status || seq == 0 ||
              shard_count != core->key_meta_shard_count, -1);
    if (tlc_core_validate_replica_event(core, event) != 0) {
        *status = TLC_CORE_REPLICA_APPLY_ERROR;
        return -1;
    }
    pthread_mutex_lock(&core->replica_apply_mu);
    int skip_warm_apply = seq <= captured_seq[event->meta_shard_id];
    int rc = tlc_core_apply_replica_event_locked(core, event, seq, status,
                                                 skip_warm_apply);
    pthread_mutex_unlock(&core->replica_apply_mu);
    return rc;
}

typedef struct tlc_core_checkpoint_recovery {
    tlc_core_t *core;
    uint64_t *captured_seq;
} tlc_core_checkpoint_recovery_t;

static int tlc_core_load_checkpoint_shard(uint32_t meta_shard_id,
                                          uint64_t captured_seq,
                                          const void *state_data,
                                          uint32_t state_len,
                                          void *arg) {
    tlc_core_checkpoint_recovery_t *recovery = arg;
    tlc_core_t *core = recovery->core;
    recovery->captured_seq[meta_shard_id] = captured_seq;
    tlc_core_key_meta_shard_t *shard = &core->key_meta_shards[meta_shard_id];
    const uint8_t *cursor = state_len ? state_data : (const uint8_t *)"";
    const uint8_t *end = cursor + state_len;
    bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
    while (cursor < end) {
        if ((size_t)(end - cursor) < sizeof(tlc_core_checkpoint_entry_t))
            goto failed;
        tlc_core_checkpoint_entry_t entry;
        memcpy(&entry, cursor, sizeof(entry));
        cursor += sizeof(entry);
        if (entry.key_len == 0 || entry.key_len > VEMB_V16_MAX_KEY_LEN ||
            entry.value_len > core->value_size ||
            (entry.tombstone && entry.value_len != 0) ||
            (size_t)(end - cursor) < entry.key_len + entry.value_len)
            goto failed;
        const char *key = (const char *)cursor;
        const uint8_t *value = cursor + entry.key_len;
        uint64_t key_hash = vemb_v16_xxh3_64(key, entry.key_len);
        if (key_hash != entry.key_hash ||
            key_meta_shard_for_hash(core, key_hash) != shard)
            goto failed;
        tlc_core_key_meta_entry_t *meta = key_meta_find_locked(
            core, key, entry.key_len, key_hash, 1);
        if (!meta)
            goto failed;
        meta->key_version = entry.key_version;
        meta->topology_epoch = entry.topology_epoch;
        meta->owner_epoch = entry.owner_epoch;
        meta->migration_state = entry.migration_state;
        meta->source_owner = entry.source_owner;
        meta->target_owner = entry.target_owner;
        meta->shard_id = entry.shard_id;
        key_meta_set_tombstone_locked(core, meta, entry.tombstone != 0);
        if (entry.tombstone) {
            meta->location = tlc_invalid_location;
        } else {
            tlc_warm_location_t location = tlc_invalid_location;
            if (entry.value_len != core->value_size ||
                warm_put(core, key, entry.key_len, key_hash,
                         value, entry.value_len, &location) != 0)
                goto failed;
            meta->location = location;
            location_cache_put(core, key, entry.key_len, key_hash, &location);
        }
        cursor += entry.key_len + entry.value_len;
    }
    bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    return 0;
failed:
    bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    serverLog(LL_WARNING,
              "COLD checkpoint state decode failed: meta_shard_id=%u captured_seq=%llu",
              meta_shard_id, (unsigned long long)captured_seq);
    return -1;
}

int tlc_core_recover_cold(tlc_core_t *core) {
    RETURN_IF(!core || !core->persistent_cold, -1);
    uint32_t meta_shard_count = core->key_meta_shard_count;
    uint64_t *captured_seq = zcalloc_num(meta_shard_count,
                                          sizeof(*captured_seq));
    if (!captured_seq)
        return -1;
    tlc_core_reset_recovery_state(core);
    tlc_cold_checkpoint_result_t checkpoint;
    int checkpoint_valid = tlc_cold_validate_checkpoint(
        core->persistent_cold, 0, meta_shard_count, NULL) == 0;
    if (!checkpoint_valid) {
        zfree(captured_seq);
        int rc = tlc_cold_replay(core->persistent_cold,
                                 tlc_core_recover_event,
                                 core);
        if (rc != 0)
            return rc;
        tlc_cold_progress_t progress;
        if (tlc_cold_get_progress(core->persistent_cold, &progress) != 0)
            return -1;
        pthread_mutex_lock(&core->replica_apply_mu);
        core->replica_applied_seq = progress.durable_seq;
        pthread_mutex_unlock(&core->replica_apply_mu);
        return 0;
    }
    tlc_core_checkpoint_recovery_t recovery = {
        .core = core,
        .captured_seq = captured_seq,
    };
    int checkpoint_rc = tlc_cold_load_checkpoint(
        core->persistent_cold, meta_shard_count,
        tlc_core_load_checkpoint_shard, &recovery, &checkpoint);
    if (checkpoint_rc != 0) {
        zfree(captured_seq);
        return -1;
    }
    int rc = tlc_cold_replay_after(core->persistent_cold,
                                   captured_seq,
                                   meta_shard_count,
                                   tlc_core_recover_event,
                                   core);
    zfree(captured_seq);
    if (rc != 0)
        return rc;
    tlc_cold_progress_t progress;
    if (tlc_cold_get_progress(core->persistent_cold, &progress) != 0)
        return -1;
    pthread_mutex_lock(&core->replica_apply_mu);
    core->replica_applied_seq = progress.durable_seq;
    pthread_mutex_unlock(&core->replica_apply_mu);
    return 0;
}

static int checkpoint_state_append(tlc_core_checkpoint_state_t *state,
                                   const void *data,
                                   size_t length) {
    if (length > SIZE_MAX - state->length)
        return -1;
    size_t required = state->length + length;
    if (required > state->capacity) {
        size_t capacity = state->capacity ? state->capacity : 4096;
        while (capacity < required) {
            if (capacity > SIZE_MAX / 2)
                capacity = required;
            else
                capacity *= 2;
        }
        uint8_t *next = zmalloc(capacity);
        if (!next)
            return -1;
        if (state->length)
            memcpy(next, state->data, state->length);
        zfree(state->data);
        state->data = next;
        state->capacity = capacity;
    }
    if (length)
        memcpy(state->data + state->length, data, length);
    state->length = required;
    return 0;
}

int tlc_core_publish_checkpoint(tlc_core_t *core,
                                uint64_t generation,
                                uint64_t ha_term,
                                tlc_cold_checkpoint_result_t *result) {
    RETURN_IF(!core || !core->persistent_cold, -1);
    uint32_t meta_shard_count = core->key_meta_shard_count;
    tlc_core_checkpoint_state_t *states = zcalloc_num(
        meta_shard_count, sizeof(*states));
    tlc_cold_checkpoint_record_t *records = zcalloc_num(
        meta_shard_count, sizeof(*records));
    uint64_t *captured_seq = zmalloc(sizeof(*captured_seq) * meta_shard_count);
    if (!states || !records || !captured_seq) {
        serverLog(LL_WARNING,
                  "TLC checkpoint allocation failed: generation=%llu meta_shards=%u",
                  (unsigned long long)generation, meta_shard_count);
        zfree(states);
        zfree(records);
        zfree(captured_seq);
        return -1;
    }
    for (uint32_t i = 0; i < meta_shard_count; i++)
        captured_seq[i] = UINT64_MAX;

    int rc = 0;
    for (uint32_t shard_index = 0;
         shard_index < core->key_meta_shard_count && rc == 0;
         shard_index++) {
        tlc_core_key_meta_shard_t *meta_shard =
            &core->key_meta_shards[shard_index];
        bitmap_lock_blocking(&core->key_meta_locks, meta_shard->lock_id);
        for (uint32_t slot = 0; slot < meta_shard->count; slot++) {
            tlc_core_key_meta_entry_t *meta = &meta_shard->entries[slot];
            if (!meta->occupied)
                continue;
            uint32_t value_len = meta->tombstone ? 0 : core->value_size;
            uint8_t *value = NULL;
            if (value_len) {
                value = zmalloc(value_len);
                if (!value || tlc_core_copy_warm_location_value(
                        core, meta->key_hash, &meta->location,
                        value, value_len, TLC_CORE_WARM_BUSY_RETRIES) != 0) {
                    serverLog(LL_WARNING,
                              "TLC checkpoint WARM value capture failed: key_hash=%llu meta_shard_id=%u",
                              (unsigned long long)meta->key_hash,
                              shard_index);
                    zfree(value);
                    rc = -1;
                    break;
                }
            }
            tlc_core_checkpoint_entry_t entry = {
                .key_hash = meta->key_hash,
                .key_version = meta->key_version,
                .topology_epoch = meta->topology_epoch,
                .owner_epoch = meta->owner_epoch,
                .migration_state = meta->migration_state,
                .source_owner = meta->source_owner,
                .target_owner = meta->target_owner,
                .tombstone = meta->tombstone,
                .shard_id = meta->shard_id,
                .key_len = meta->key_len,
                .value_len = value_len,
            };
            if (checkpoint_state_append(&states[shard_index],
                                        &entry, sizeof(entry)) != 0 ||
                checkpoint_state_append(&states[shard_index],
                                        meta->key, meta->key_len) != 0 ||
                checkpoint_state_append(&states[shard_index],
                                        value, value_len) != 0) {
                serverLog(LL_WARNING,
                          "TLC checkpoint state append failed: key_hash=%llu meta_shard_id=%u",
                          (unsigned long long)meta->key_hash,
                          shard_index);
                zfree(value);
                rc = -1;
                break;
            }
            zfree(value);
        }
        tlc_cold_progress_t progress;
        if (tlc_cold_get_progress(core->persistent_cold, &progress) != 0)
            rc = -1;
        bitmap_unlock(&core->key_meta_locks, meta_shard->lock_id);
        if (rc != 0)
            break;
        captured_seq[shard_index] = progress.durable_seq;
    }
    if (rc == 0) {
        tlc_cold_progress_t progress;
        if (tlc_cold_get_progress(core->persistent_cold, &progress) != 0) {
            rc = -1;
        }
        if (rc == 0) {
            for (uint32_t meta_shard_id = 0;
                 meta_shard_id < meta_shard_count; meta_shard_id++) {
                if (captured_seq[meta_shard_id] == UINT64_MAX)
                    captured_seq[meta_shard_id] = progress.durable_seq;
                if (states[meta_shard_id].length > UINT32_MAX) {
                    serverLog(LL_WARNING,
                              "TLC checkpoint state exceeds format limit: meta_shard_id=%u length=%zu",
                              meta_shard_id, states[meta_shard_id].length);
                    rc = -1;
                }
                records[meta_shard_id].meta_shard_id = meta_shard_id;
                records[meta_shard_id].captured_seq = captured_seq[meta_shard_id];
                records[meta_shard_id].state = states[meta_shard_id].data;
                records[meta_shard_id].state_len =
                    (uint32_t)states[meta_shard_id].length;
            }
        }
        if (rc == 0)
            rc = tlc_cold_publish_checkpoint(core->persistent_cold,
                                             generation, ha_term,
                                             meta_shard_count,
                                             records, meta_shard_count,
                                             result);
    }
    for (uint32_t i = 0; i < meta_shard_count; i++)
        zfree(states[i].data);
    zfree(records);
    zfree(states);
    zfree(captured_seq);
    return rc;
}

int tlc_core_compact(tlc_core_t *core,
                     uint64_t checkpoint_floor_seq,
                     uint64_t ha_safe_point_seq,
                     uint32_t checkpoint_retention_count) {
    RETURN_IF(!core || !core->persistent_cold, -1);
    return tlc_cold_compact(core->persistent_cold,
                            checkpoint_floor_seq,
                            ha_safe_point_seq,
                            checkpoint_retention_count);
}

typedef struct tlc_core_resync_context {
    tlc_core_t *follower;
    tlc_cold_t *follower_cold;
    uint64_t *captured_seq;
    uint32_t shard_count;
} tlc_core_resync_context_t;

static void tlc_core_reset_location_cache(tlc_core_t *core) {
    tlc_core_location_cache_t *cache = &core->location_cache;
    for (uint32_t i = 0; i < cache->capacity; i++) {
        tlc_core_location_cache_entry_t *entry = &cache->entries[i];
        atomic_store_explicit(&entry->seq, SEQLOCK_EMPTY, memory_order_relaxed);
        atomic_store_explicit(&entry->key_hash, 0, memory_order_relaxed);
        atomic_store_explicit(&entry->key_fingerprint, 0, memory_order_relaxed);
        atomic_store_explicit(&entry->key_len, 0, memory_order_relaxed);
        atomic_store_explicit(&entry->region_index, UINT32_MAX,
                              memory_order_relaxed);
        atomic_store_explicit(&entry->local_slot, TLC_CORE_INVALID_SLOT,
                              memory_order_relaxed);
        atomic_store_explicit(&entry->owner_generation, 0,
                              memory_order_relaxed);
    }
}

static void tlc_core_reset_warm_region(
        tlc_core_warm_region_runtime_t *region) {
    /* Recovery is fenced. Retain generations/sequences across rebuilding. */
    for (uint32_t slot = 0; slot < region->capacity_slots; slot++) {
        vemb_v16_warm_slot_meta_t *meta = &region->slot_meta[slot];
        atomic_store_explicit(&meta->state, VEMB_V16_WARM_SLOT_FREE,
                              memory_order_relaxed);
        meta->region_id = region->region_id;
        meta->local_slot = slot;
        meta->bytes = region->value_size;
        atomic_fetch_add_explicit(&meta->owner_generation, 1, memory_order_relaxed);
        uint64_t seq = atomic_load_explicit(&meta->write_seq, memory_order_relaxed);
        atomic_store_explicit(&meta->write_seq,
                              (seq | SEQLOCK_STATE_MASK) + SEQLOCK_STATE_STEP,
                              memory_order_relaxed);
        meta->key_hash = 0;
        meta->key_fingerprint = 0;
        atomic_store_explicit(&meta->last_access_ns, 0, memory_order_relaxed);
        atomic_store_explicit(&meta->clock_bit, 0, memory_order_relaxed);
        atomic_store_explicit(&meta->cold_state, VEMB_V16_WARM_SLOT_COLD_NONE,
                              memory_order_relaxed);
    }
    region->next_slot = 0;
    region->free_count = 0;
}

/* Caller fences all reads/writes and owns every region marked local. */
static void tlc_core_reset_recovery_state(tlc_core_t *core) {
    tlc_core_reset_location_cache(core);
    for (uint32_t i = 0; i < core->warm.region_count; i++)
        if (core->warm.regions[i].is_local)
            tlc_core_reset_warm_region(&core->warm.regions[i]);
    uint32_t runtime_count = warm_runtime_region_count(&core->warm);
    for (uint32_t i = 0; i < runtime_count; i++)
        if (core->warm.runtime_regions[i].is_local)
            tlc_core_reset_warm_region(&core->warm.runtime_regions[i]);
    atomic_store_explicit(&core->warm.active_region, 0, memory_order_relaxed);

    for (uint32_t i = 0; i < core->key_meta_shard_count; i++) {
        tlc_core_key_meta_shard_t *shard = &core->key_meta_shards[i];
        memset(shard->entries, 0, sizeof(*shard->entries) * shard->capacity);
        memset(shard->hash_table, 0, sizeof(*shard->hash_table) * shard->capacity);
        shard->count = 0;
    }
    atomic_store_explicit(&core->key_meta_count, 0, memory_order_relaxed);
    atomic_store_explicit(&core->source_fence_active_count, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&core->tombstone_active_count, 0,
                          memory_order_relaxed);
    core->replica_applied_seq = 0;
}

static int tlc_core_reset_fenced_for_resync(tlc_core_t *core) {
    if (tlc_cold_reset_fenced(core->persistent_cold) != 0)
        return -1;
    tlc_core_reset_recovery_state(core);
    return 0;
}

static int tlc_core_capture_checkpoint_seq(uint32_t meta_shard_id,
                                           uint64_t captured_seq,
                                           const void *state,
                                           uint32_t state_len,
                                           void *arg) {
    tlc_core_resync_context_t *context = arg;
    context->captured_seq[meta_shard_id] = captured_seq;
    (void)state;
    (void)state_len;
    return 0;
}

static int tlc_core_resync_replay_event(const tlc_cold_event_input_t *input,
                                        uint64_t seq,
                                        void *arg) {
    tlc_core_resync_context_t *context = arg;
    tlc_cold_replica_batch_status_t batch_status;
    uint64_t durable_seq = 0;
    int rc = tlc_cold_submit_replica_batch(context->follower_cold, seq, input,
                                            1, TLC_COLD_ACK_DURABLE,
                                            &batch_status, &durable_seq);
    if (rc != 0 || (batch_status != TLC_COLD_REPLICA_BATCH_APPLIED &&
                    batch_status != TLC_COLD_REPLICA_BATCH_DUPLICATE)) {
        serverLog(LL_WARNING,
                  "TLC resync tail COLD append failed: seq=%llu rc=%d status=%d",
                  (unsigned long long)seq, rc, batch_status);
        return -1;
    }
    tlc_core_replica_apply_status_t apply_status =
        TLC_CORE_REPLICA_APPLY_ERROR;
    rc = tlc_core_apply_resync_event(context->follower, input, seq,
                                     context->captured_seq,
                                     context->shard_count,
                                     &apply_status);
    if (rc != 0 || (apply_status != TLC_CORE_REPLICA_APPLY_APPLIED &&
                    apply_status != TLC_CORE_REPLICA_APPLY_DUPLICATE &&
                    apply_status != TLC_CORE_REPLICA_APPLY_STALE &&
                    apply_status != TLC_CORE_REPLICA_APPLY_CHECKPOINTED)) {
        serverLog(LL_WARNING,
                  "TLC resync tail apply failed: seq=%llu rc=%d status=%d",
                  (unsigned long long)seq, rc, apply_status);
        return -1;
    }
    return 0;
}

static int tlc_core_finish_resync_checkpoint(tlc_core_t *core,
                                             tlc_cold_checkpoint_result_t *result) {
    if (tlc_core_recover_cold(core) != 0) {
        serverLog(LL_WARNING, "TLC resync failed: follower checkpoint load");
        return -1;
    }
    pthread_mutex_lock(&core->replica_apply_mu);
    core->replica_applied_seq = result->checkpoint_seq;
    pthread_mutex_unlock(&core->replica_apply_mu);
    return 0;
}

int tlc_core_install_resync_checkpoint(
    tlc_core_t *core,
    const void *checkpoint_blob,
    size_t checkpoint_blob_bytes,
    tlc_cold_checkpoint_result_t *result) {
    RETURN_IF(!core || !core->persistent_cold || !checkpoint_blob ||
              checkpoint_blob_bytes == 0 || !result, -1);
    uint32_t shard_count = core->key_meta_shard_count;
    if (tlc_core_reset_fenced_for_resync(core) != 0) {
        serverLog(LL_WARNING, "TLC resync failed: follower fenced reset");
        return -1;
    }
    if (tlc_cold_import_checkpoint(core->persistent_cold, shard_count,
                                   checkpoint_blob, checkpoint_blob_bytes,
                                   result) != 0) {
        serverLog(LL_WARNING, "TLC resync failed: checkpoint import");
        return -1;
    }
    return tlc_core_finish_resync_checkpoint(core, result);
}

int tlc_core_install_resync_checkpoint_file(
    tlc_core_t *core,
    int checkpoint_fd,
    const char *checkpoint_path,
    size_t checkpoint_blob_bytes,
    tlc_cold_checkpoint_result_t *result) {
    RETURN_IF(!core || !core->persistent_cold || checkpoint_fd < 0 ||
              !checkpoint_path || checkpoint_blob_bytes == 0 || !result, -1);
    if (tlc_core_reset_fenced_for_resync(core) != 0) {
        serverLog(LL_WARNING, "TLC resync failed: follower fenced reset");
        return -1;
    }
    if (tlc_cold_import_checkpoint_file(core->persistent_cold,
                                        core->key_meta_shard_count,
                                        checkpoint_fd, checkpoint_path,
                                        checkpoint_blob_bytes, result) != 0) {
        serverLog(LL_WARNING, "TLC resync failed: checkpoint file import");
        return -1;
    }
    return tlc_core_finish_resync_checkpoint(core, result);
}

int tlc_core_resync_from(tlc_core_t *leader,
                         tlc_core_t *follower,
                         uint64_t boundary_seq,
                         tlc_cold_checkpoint_result_t *result) {
    RETURN_IF(!leader || !follower || leader == follower ||
              !leader->persistent_cold || !follower->persistent_cold ||
              !result, -1);
    tlc_cold_progress_t leader_progress;
    if (tlc_cold_get_progress(leader->persistent_cold, &leader_progress) != 0)
        return -1;
    uint64_t boundary = boundary_seq ? boundary_seq :
                                      leader_progress.durable_seq;
    if (boundary > leader_progress.durable_seq)
        return -1;
    uint32_t shard_count = follower->key_meta_shard_count;
    uint64_t *captured_seq = zmalloc(sizeof(*captured_seq) * shard_count);
    if (!captured_seq)
        return -1;
    tlc_cold_checkpoint_result_t checkpoint;
    void *checkpoint_blob = NULL;
    size_t checkpoint_blob_bytes = 0;
    if (tlc_cold_export_checkpoint(leader->persistent_cold, shard_count,
                                   &checkpoint_blob, &checkpoint_blob_bytes,
                                   &checkpoint) != 0) {
        serverLog(LL_WARNING, "TLC resync rejected: leader checkpoint export");
        zfree(captured_seq);
        return -1;
    }
    if (checkpoint.checkpoint_seq > boundary) {
        serverLog(LL_WARNING,
                  "TLC resync rejected: checkpoint_seq=%llu boundary=%llu",
                  (unsigned long long)checkpoint.checkpoint_seq,
                  (unsigned long long)boundary);
        tlc_cold_free_checkpoint_blob(checkpoint_blob);
        zfree(captured_seq);
        return -1;
    }
    tlc_core_resync_context_t checkpoint_context = {
        .captured_seq = captured_seq,
        .shard_count = shard_count,
    };
    if (tlc_cold_load_checkpoint(leader->persistent_cold, shard_count,
                                 tlc_core_capture_checkpoint_seq,
                                 &checkpoint_context, NULL) != 0) {
        serverLog(LL_WARNING, "TLC resync failed: checkpoint metadata load");
        tlc_cold_free_checkpoint_blob(checkpoint_blob);
        zfree(captured_seq);
        return -1;
    }
    int install_rc = tlc_core_install_resync_checkpoint(
        follower, checkpoint_blob, checkpoint_blob_bytes, result);
    tlc_cold_free_checkpoint_blob(checkpoint_blob);
    if (install_rc != 0) {
        zfree(captured_seq);
        return -1;
    }
    if (boundary > checkpoint.checkpoint_seq) {
        tlc_core_resync_context_t context = {
            .follower = follower,
            .follower_cold = follower->persistent_cold,
            .captured_seq = captured_seq,
            .shard_count = shard_count,
        };
        if (tlc_cold_replay_range(leader->persistent_cold,
                                  checkpoint.checkpoint_seq + 1, boundary,
                                  tlc_core_resync_replay_event,
                                  &context) != 0) {
            serverLog(LL_WARNING, "TLC resync failed: AOF tail replay");
            zfree(captured_seq);
            return -1;
        }
    }
    zfree(captured_seq);
    return 0;
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
    if (tlc_core_write_fenced(core))
        return -1;
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
    if (!key_meta && key_meta_reserve_locked(shard) != 0) {
        failure_reason = "key_meta_alloc_failed";
        goto rollback;
    }
    if (tlc_core_persist_event_locked(core,
                                      key_meta,
                                      key,
                                      key_len,
                                      key_hash,
                                      TLC_COLD_OP_PUT,
                                      value,
                                      value_size,
                                      topology_epoch) != 0) {
        serverLog(LL_WARNING,
                  "tlc_core COLD PUT append submit failed: key_hash=%llu key_len=%u topology_epoch=%llu",
                  (unsigned long long)key_hash,
                  key_len,
                  (unsigned long long)topology_epoch);
        failure_reason = "cold_persist_failed";
        failure_meta = key_meta;
        goto rollback;
    }
    if (warm_put(core, key, key_len, key_hash,
                 value, value_size, location) == 0) {
        key_meta = key_meta_find_locked(core, key, key_len, key_hash, 1);
        update_location_cache = 1;
        goto commit;
    }

    if (core->persistent_cold) {
        serverLog(LL_WARNING,
                  "tlc_core WARM publish failed after COLD accepted PUT; AOF entry retained for retry: key_hash=%llu key_len=%u",
                  (unsigned long long)key_hash,
                  key_len);
    }

    failure_reason = "warm_put_failed";
    failure_meta = key_meta;
    goto rollback;

commit:
    if (!key_meta) {
        if (core->persistent_cold) {
            serverLog(LL_WARNING,
                      "tlc_core WARM metadata publish failed after COLD accepted PUT; AOF entry retained: key_hash=%llu key_len=%u",
                      (unsigned long long)key_hash,
                      key_len);
        }
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
    if (tlc_core_write_fenced(core))
        return -1;
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

    if (tlc_core_persist_event_locked(core,
                                      key_meta,
                                      key,
                                      key_len,
                                      key_hash,
                                      TLC_COLD_OP_DEL,
                                      NULL,
                                      0,
                                      topology_epoch) != 0) {
        serverLog(LL_WARNING,
                  "tlc_core COLD DEL append submit failed: key_hash=%llu key_len=%u topology_epoch=%llu",
                  (unsigned long long)key_hash,
                  key_len,
                  (unsigned long long)topology_epoch);
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        return -1;
    }

    if (warm_release_location(core, key_hash, &key_meta->location) != 0) {
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
    if (tlc_core_write_fenced(core))
        return -1;
    RETURN_IF(!core->persistent_cold, -1);
    tlc_core_key_meta_shard_t *shard = key_meta_shard_for_hash(core, key_hash);
    tlc_cold_event_input_t event = {
        .ha_term = 0,
        .topology_epoch = 0,
        .op = TLC_COLD_OP_PUT,
        .meta_shard_id = shard->lock_id,
        .version = 1,
        .key = key,
        .key_len = key_len,
        .value = value,
        .value_len = value_size,
    };
    return tlc_cold_submit(core->persistent_cold, &event,
                           TLC_COLD_ACK_ACCEPTED, NULL);
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
        for (uint32_t i = 0; i < shard->count; i++) {
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
        for (uint32_t i = 0; i < shard->count; i++) {
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
        for (uint32_t i = 0; i < shard->count; i++) {
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
        for (uint32_t i = 0; i < shard->count; i++) {
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
        for (uint32_t i = 0; i < shard->count; i++) {
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
                                        tlc_core_key_cursor_t *cursor,
                                        tlc_core_migration_key_ref_t *keys,
                                        uint32_t max_keys,
                                        uint32_t *key_count,
                                        int *done) {
    RETURN_IF(!core || !cursor || !keys || !key_count || !done ||
              max_keys == 0, -1);
    *key_count = 0;
    *done = 0;
    for (; cursor->shard_index < core->key_meta_shard_count;
         cursor->shard_index++, cursor->entry_index = 0) {
        tlc_core_key_meta_shard_t *shard =
            &core->key_meta_shards[cursor->shard_index];
        bitmap_lock_blocking(&core->key_meta_locks, shard->lock_id);
        while (cursor->entry_index < shard->count) {
            if (*key_count == max_keys) {
                bitmap_unlock(&core->key_meta_locks, shard->lock_id);
                return 0;
            }
            const tlc_core_key_meta_entry_t *meta =
                &shard->entries[cursor->entry_index++];
            if (meta->tombstone ||
                meta->migration_state != TLC_CORE_KEY_SOURCE_ACTIVE)
                continue;
            tlc_core_migration_key_ref_t *entry = &keys[(*key_count)++];
            entry->key_hash = meta->key_hash;
            entry->key_len = meta->key_len;
            memcpy(entry->key, meta->key, meta->key_len);
            key_meta_fill_info(meta, &entry->info);
        }
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
    }
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
                                       key_hash, &location, 1) != 0) {
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

    if (snapshot->tombstone &&
        warm_release_location(core, snapshot->key_hash, &key_meta->location) != 0) {
        bitmap_unlock(&core->key_meta_locks, shard->lock_id);
        *status = TLC_CORE_MIGRATION_ERROR;
        return -1;
    }
    tlc_warm_location_t new_location = tlc_invalid_location;
    if (!snapshot->tombstone) {
        if (warm_put(core,
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
