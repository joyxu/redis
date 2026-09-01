#ifndef __TLC_CORE_H
#define __TLC_CORE_H

#include "vemb_v16_warm_region_layout.h"
#include "vemb_v16_protocol.h"
#include "tlc_cold.h"

#include <stddef.h>
#include <stdint.h>

#define TLC_CORE_DEFAULT_HOT_CAPACITY 65536u
#ifndef TLC_CORE_ALLOW_LRU_EVICTION
#define TLC_CORE_ALLOW_LRU_EVICTION 0
#endif
#define TLC_CORE_INVALID_SLOT UINT32_MAX
#define TLC_CORE_INVALID_REGION_ID UINT32_MAX
#define IS_VALID_LOCATION(loc) \
    ((loc).region_id != TLC_CORE_INVALID_REGION_ID && \
     (loc).local_slot != TLC_CORE_INVALID_SLOT)
#define IS_INVALID_LOCATION(loc) (!IS_VALID_LOCATION(loc))
#define TLC_CORE_MAX_WARM_REGIONS 128u
#define TLC_CORE_MAX_TOTAL_WARM_REGIONS (TLC_CORE_MAX_WARM_REGIONS * 2u)

typedef struct tlc_warm_location {
    uint32_t region_id;
    uint32_t region_index;
    uint32_t local_slot;
    uint32_t bytes;
    uint64_t offset;
    uint64_t owner_generation;
} tlc_warm_location_t;

typedef enum tlc_core_key_migration_state {
    TLC_CORE_KEY_SOURCE_ACTIVE = 0,
    TLC_CORE_KEY_MIGRATING = 1,
    TLC_CORE_KEY_DEST_PREPARED = 2,
    TLC_CORE_KEY_DEST_COMMITTED = 3,
    TLC_CORE_KEY_CUTOVER = 4,
    TLC_CORE_KEY_SOURCE_GC = 5,
} tlc_core_key_migration_state_t;

typedef enum tlc_core_migration_apply_status {
    TLC_CORE_MIGRATION_APPLIED = 0,
    TLC_CORE_MIGRATION_DUPLICATE = 1,
    TLC_CORE_MIGRATION_STALE_REJECTED = 2,
    TLC_CORE_MIGRATION_RETRY = 3,
    TLC_CORE_MIGRATION_ERROR = 4,
} tlc_core_migration_apply_status_t;

typedef struct tlc_core_key_migration_info {
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
} tlc_core_key_migration_info_t;

typedef struct tlc_core_migration_key_ref {
    uint64_t key_hash;
    uint32_t key_len;
    uint32_t reserved0;
    char key[VEMB_V16_MAX_KEY_LEN];
    tlc_core_key_migration_info_t info;
} tlc_core_migration_key_ref_t;

typedef struct tlc_core_migration_range_ref {
    uint64_t topology_epoch;
    uint32_t target_owner;
    uint32_t shard_id;
    uint32_t key_count;
} tlc_core_migration_range_ref_t;

typedef struct tlc_core_migration_snapshot {
    uint64_t key_hash;
    uint64_t key_version;
    uint64_t topology_epoch;
    uint64_t owner_epoch;
    uint32_t key_len;
    uint32_t migration_state;
    uint32_t source_owner;
    uint32_t target_owner;
    uint32_t tombstone;
    uint32_t value_size;
    uint32_t shard_id;
    tlc_warm_location_t location;
    char key[VEMB_V16_MAX_KEY_LEN];
} tlc_core_migration_snapshot_t;

typedef struct tlc_core_warm_region_config {
    uint32_t region_id;
    uint32_t backend_type;
    uint32_t is_local;
    uint32_t weight;
    uint32_t value_size;
    uint64_t region_bytes;
    uint8_t *mapped_addr;
    vemb_v16_warm_slot_meta_t *slot_meta;
} tlc_core_warm_region_config_t;

typedef struct tlc_core_region_stats {
    uint32_t region_id;
    uint32_t is_local;
    uint32_t full;
    uint32_t capacity_slots;
    uint32_t used_slots;
    uint64_t lookup_hits;
    uint64_t cold_promotes;
} tlc_core_region_stats_t;

typedef struct tlc_core_stats {
    uint64_t warm_region_count;
    uint64_t warm_region_full_count;
    uint64_t warm_alloc_local;
    uint64_t warm_alloc_remote;
    uint64_t warm_alloc_fallback;
    uint64_t warm_alloc_cold_spill;
    uint64_t warm_alloc_fail;
    uint64_t warm_eviction_success;
    uint64_t warm_eviction_fail;
    uint64_t warm_same_key_overwrite;
    uint64_t warm_stale_handle_reject;
    uint64_t remote_meta_stale;
    uint64_t warm_region_hash_local_pct;
    uint64_t lookup_cache_hit;
    uint64_t lookup_cache_miss;
    uint64_t lookup_warm_local_hit;
    uint64_t lookup_warm_imported_hit;
    uint64_t lookup_cold_promote;
    uint64_t lookup_final_miss;
} tlc_core_stats_t;

typedef struct tlc_core_config {
    uint32_t value_size;
    uint32_t warm_capacity;
    uint32_t hot_capacity;
    const tlc_core_warm_region_config_t *warm_regions;
    uint32_t warm_region_count;
    uint32_t local_region_weight;
} tlc_core_config_t;

typedef struct tlc_core tlc_core_t;

int tlc_core_create(tlc_core_t **out, const tlc_core_config_t *config);
void tlc_core_destroy(tlc_core_t *core);
/* Must be called once, before concurrent writes begin; resolver_arg remains live. */
int tlc_core_enable_cold(tlc_core_t *core,
                         const tlc_cold_config_t *cold_config);
int tlc_core_recover_cold(tlc_core_t *core);
/* Preconditions: v16 boundary validated generation and output shape. */
int tlc_core_publish_checkpoint(tlc_core_t *core,
                                uint64_t generation,
                                uint64_t term,
                                tlc_cold_checkpoint_result_t *result);
int tlc_core_compact(tlc_core_t *core,
                     uint64_t checkpoint_floor_seq,
                     uint64_t ha_safe_point_seq,
                     uint32_t checkpoint_retention_count);
int tlc_core_source_fence_active(const tlc_core_t *core);
int tlc_core_attach_warm_region(tlc_core_t *core,
                                const tlc_core_warm_region_config_t *region,
                                uint32_t *region_index);

int tlc_core_get_warm_slot(tlc_core_t *core,
                           const char *key,
                           uint32_t key_len,
                           uint64_t key_hash,
                           uint32_t *warm_slot);
int tlc_core_get_warm_location(tlc_core_t *core,
                               const char *key,
                               uint32_t key_len,
                               uint64_t key_hash,
                               tlc_warm_location_t *location);
int tlc_core_get_warm_location_stable_read(tlc_core_t *core,
                                           const char *key,
                                           uint32_t key_len,
                                           uint64_t key_hash,
                                           tlc_warm_location_t *location);
int tlc_core_get_cached_warm_location(tlc_core_t *core,
                                      const char *key,
                                      uint32_t key_len,
                                      uint64_t key_hash,
                                      tlc_warm_location_t *location);
int tlc_core_put(tlc_core_t *core,
                 const char *key,
                 uint32_t key_len,
                 uint64_t key_hash,
                 const void *value,
                 uint32_t value_size,
                 uint32_t *warm_slot);
int tlc_core_put_location_epoch(tlc_core_t *core,
                                const char *key,
                                uint32_t key_len,
                                uint64_t key_hash,
                                const void *value,
                                uint32_t value_size,
                                uint64_t topology_epoch,
                                int enforce_epoch,
                                tlc_warm_location_t *location);
int tlc_core_delete_with_epoch(tlc_core_t *core,
                               const char *key,
                               uint32_t key_len,
                               uint64_t key_hash,
                               uint64_t topology_epoch,
                               tlc_core_key_migration_info_t *info);
/* Legacy explicit durable PUT; requires persistent COLD to be enabled. */
int tlc_core_cold_append(tlc_core_t *core,
                         const char *key,
                         uint32_t key_len,
                         uint64_t key_hash,
                         const void *value,
                         uint32_t value_size);
int tlc_core_get_migration_info(tlc_core_t *core,
                                const char *key,
                                uint32_t key_len,
                                uint64_t key_hash,
                                tlc_core_key_migration_info_t *info);
int tlc_core_mark_migrating_in_shard(tlc_core_t *core,
                                     const char *key,
                                     uint32_t key_len,
                                     uint64_t key_hash,
                                     uint64_t topology_epoch,
                                     uint32_t target_owner,
                                     uint32_t shard_id,
                                     tlc_core_key_migration_info_t *info);
int tlc_core_mark_cutover(tlc_core_t *core,
                          const char *key,
                          uint32_t key_len,
                          uint64_t key_hash,
                          uint64_t topology_epoch,
                          uint32_t target_owner,
                          tlc_core_key_migration_info_t *info);
int tlc_core_mark_source_gc(tlc_core_t *core,
                            const char *key,
                            uint32_t key_len,
                            uint64_t key_hash,
                            uint64_t topology_epoch,
                            uint32_t target_owner,
                            tlc_core_key_migration_info_t *info);
int tlc_core_accept_owner_lease(tlc_core_t *core,
                                const char *key,
                                uint32_t key_len,
                                uint64_t key_hash,
                                uint64_t topology_epoch,
                                uint64_t owner_epoch,
                                uint32_t target_owner,
                                tlc_core_key_migration_info_t *info);
int tlc_core_key_is_source_cutover(tlc_core_t *core,
                                   const char *key,
                                   uint32_t key_len,
                                   uint64_t key_hash,
                                   tlc_core_key_migration_info_t *info);
int tlc_core_has_uncommitted_source_migrations(tlc_core_t *core);
int tlc_core_collect_migration_keys(tlc_core_t *core,
                                    uint64_t topology_epoch,
                                    uint32_t target_owner,
                                    uint32_t shard_id,
                                    uint32_t migration_state,
                                    tlc_core_migration_key_ref_t *keys,
                                    uint32_t max_keys,
                                    uint32_t *key_count);
int tlc_core_collect_migration_keys_page(tlc_core_t *core,
                                         uint64_t topology_epoch,
                                         uint32_t target_owner,
                                         uint32_t shard_id,
                                         uint32_t migration_state,
                                         tlc_core_migration_key_ref_t *keys,
                                         uint32_t max_keys,
                                         uint32_t *key_count,
                                         uint32_t *remaining_count);
int tlc_core_count_migration_keys(tlc_core_t *core,
                                  uint64_t topology_epoch,
                                  uint32_t target_owner,
                                  uint32_t shard_id,
                                  uint32_t migration_state,
                                  uint32_t *key_count);
int tlc_core_collect_migration_ranges(tlc_core_t *core,
                                      uint64_t topology_epoch,
                                      uint32_t migration_state,
                                      tlc_core_migration_range_ref_t *ranges,
                                      uint32_t max_ranges,
                                      uint32_t *range_count);
int tlc_core_collect_source_active_keys(tlc_core_t *core,
                                        uint32_t *cursor,
                                        tlc_core_migration_key_ref_t *keys,
                                        uint32_t max_keys,
                                        uint32_t *key_count,
                                        int *done);
int tlc_core_snapshot(tlc_core_t *core,
                      const char *key,
                      uint32_t key_len,
                      uint64_t key_hash,
                      uint32_t source_owner,
                      uint32_t target_owner,
                      tlc_core_migration_snapshot_t *snapshot,
                      void *value_out,
                      uint32_t value_out_size);
int tlc_core_apply_migration(tlc_core_t *core,
                             const tlc_core_migration_snapshot_t *snapshot,
                             const void *value,
                             uint32_t value_size,
                             tlc_core_migration_apply_status_t *status,
                             tlc_warm_location_t *location);
int tlc_core_validate_warm_location(tlc_core_t *core,
                                    uint64_t key_hash,
                                    const tlc_warm_location_t *location);
int tlc_core_copy_warm_location_value(tlc_core_t *core,
                                      uint64_t key_hash,
                                      const tlc_warm_location_t *location,
                                      void *value_out,
                                      uint32_t value_out_size,
                                      uint32_t retry_budget);
void tlc_core_note_remote_meta_stale(tlc_core_t *core);
void tlc_core_get_stats(tlc_core_t *core, tlc_core_stats_t *stats);
uint32_t tlc_core_get_region_stats(tlc_core_t *core,
                                   tlc_core_region_stats_t *regions,
                                   uint32_t max_regions);

#endif
