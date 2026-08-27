#define _GNU_SOURCE

#include "cpu_relax.h"
#include "macro.h"
#include "util.h"
#include "vemb_v16_client_ring.h"
#include "vemb_v16_storage.h"
#include "vemb_v16_log.h"
#include "vemb_v16_net.h"
#include "vemb_v16_util.h"
#include "zmalloc.h"

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

static char *trim_ws(char *s) {
    while (*s && isspace((unsigned char)*s)) s++;
    char *end = s + strlen(s);
    while (end > s && isspace((unsigned char)end[-1])) end--;
    *end = '\0';
    return s;
}

static void strip_comment(char *s) {
    for (; *s; s++) {
        if (*s == '#') {
            *s = '\0';
            return;
        }
    }
}

static int parse_u32_value(const char *s, uint32_t *out) {
    char *end = NULL;
    unsigned long v = strtoul(s, &end, 10);
    RETURN_IF(end == s || *trim_ws(end) != '\0' || v > UINT32_MAX, -1);
    *out = (uint32_t)v;
    return 0;
}

static int parse_u64_value(const char *s, uint64_t *out) {
    char *end = NULL;
    unsigned long long v = strtoull(s, &end, 10);
    RETURN_IF(end == s || *trim_ws(end) != '\0', -1);
    *out = (uint64_t)v;
    return 0;
}

#define VEMB_V16_STORAGE_MIGRATION_OUTBOX_CAPACITY 4096u
#define VEMB_V16_STORAGE_MIGRATION_DEFAULT_SHARD_ID 0u
#define VEMB_V16_STORAGE_MIGRATION_RANGE_PAGE_LIMIT \
    VEMB_V16_MIGRATION_CONTROL_MAX_RANGE_KEYS
#define VEMB_V16_STORAGE_SCALEOUT_AUTO_MAX_SHARDS 16u
#define VEMB_V16_STORAGE_MIGRATION_RETRY_INTERVAL_US 10000u
#define VEMB_V16_STORAGE_MIGRATION_RETRY_BATCH_SIZE 32u

static int cmp_u32(const void *a, const void *b) {
    uint32_t va = *(const uint32_t *)a;
    uint32_t vb = *(const uint32_t *)b;
    if (va < vb) return -1;
    if (va > vb) return 1;
    return 0;
}

static uint32_t storage_owner_resolver(uint64_t key_hash,
                                       const char *key,
                                       uint32_t key_len,
                                       void *arg) {
    (void)key;
    (void)key_len;
    const vemb_v16_storage_ctx_t *storage = arg;
    const vemb_v16_storage_owner_resolver_snapshot_t *snapshot =
        &storage->owner_resolver_snapshots[atomic_load_explicit(
            &storage->owner_resolver_active_snapshot,
            memory_order_acquire)];
    if (snapshot->owner_ring.node_count > 0)
        return vemb_v16_topology_ring_owner(&snapshot->owner_ring, key_hash);

    uint32_t hash = (uint32_t)key_hash;
    uint32_t left = 0;
    uint32_t right = snapshot->owner_hash_node_count;
    while (left < right) {
        uint32_t mid = left + (right - left) / 2;
        if (snapshot->owner_hash_nodes[mid].hash_value < hash)
            left = mid + 1;
        else
            right = mid;
    }
    if (left >= snapshot->owner_hash_node_count)
        left = 0;
    return snapshot->owner_hash_nodes[left].owner_id;
}

static int parse_bool_value(const char *s, uint32_t *out) {
    if (!strcmp(s, "true") || !strcmp(s, "yes") || !strcmp(s, "1")) {
        *out = 1;
        return 0;
    }
    if (!strcmp(s, "false") || !strcmp(s, "no") || !strcmp(s, "0")) {
        *out = 0;
        return 0;
    }
    return -1;
}

static int parse_backend_value(const char *s, uint32_t *out) {
    if (!strcmp(s, "shm") || !strcmp(s, "local_shm") ||
        !strcmp(s, "mock_ub") || !strcmp(s, "shm_mock_ub")) {
        *out = VEMB_V16_REGION_LOCAL_SHM;
        return 0;
    }
    if (!strcmp(s, "ub")) {
        *out = VEMB_V16_REGION_UB;
        return 0;
    }
    return -1;
}

static int parse_ub_cache_policy_value(const char *value, uint32_t *out) {
    if (!strcmp(value, "cacheable") || !strcmp(value, "cc")) {
        *out = VEMB_V16_UB_CACHE_POLICY_CACHEABLE;
        return 0;
    }
    if (!strcmp(value, "noncacheable") || !strcmp(value, "nc")) {
        *out = VEMB_V16_UB_CACHE_POLICY_NONCACHEABLE;
        return 0;
    }
    return -1;
}

static vemb_v16_warm_slot_meta_t *warm_slot_meta_from_mapping(
        const vemb_v16_mapped_region_t *mapping,
        uint32_t capacity_slots) {
    RETURN_IF(!mapping || !mapping->mapped_addr, NULL);
    size_t layout_bytes =
        vemb_v16_warm_region_layout_bytes(capacity_slots);
    RETURN_IF(mapping->requested_size < layout_bytes, NULL);
    return vemb_v16_warm_region_slot_meta(mapping->mapped_addr);
}

static void warm_region_layout_init_slot_meta(
        vemb_v16_warm_region_header_t *header,
        uint32_t region_id,
        uint32_t capacity_slots) {
    vemb_v16_warm_slot_meta_t *slots =
        vemb_v16_warm_region_slot_meta(header);
    memset(slots, 0, sizeof(*slots) * capacity_slots);
    for (uint32_t slot = 0; slot < capacity_slots; slot++) {
        slots[slot].region_id = region_id;
        slots[slot].local_slot = slot;
        atomic_init(&slots[slot].state, VEMB_V16_WARM_SLOT_FREE);
        atomic_init(&slots[slot].owner_generation, 0);
        atomic_init(&slots[slot].write_seq, 0);
        atomic_init(&slots[slot].last_access_ns, 0);
        atomic_init(&slots[slot].clock_bit, 0);
        atomic_init(&slots[slot].cold_state, VEMB_V16_WARM_SLOT_COLD_NONE);
    }
}

static int warm_region_layout_ensure_initialized(
        const vemb_v16_mapped_region_t *mapping,
        uint32_t region_id,
        uint32_t capacity_slots,
        uint32_t value_size,
        uint64_t region_bytes) {
    RETURN_IF(!mapping || !mapping->mapped_addr || capacity_slots == 0, -1);
    vemb_v16_warm_region_header_t *header =
        (vemb_v16_warm_region_header_t *)mapping->mapped_addr;
    for (uint32_t spin = 0; spin < 1000000u; spin++) {
        uint32_t magic =
            atomic_load_explicit(&header->magic, memory_order_acquire);
        if (magic == VEMB_V16_WARM_REGION_LAYOUT_MAGIC) {
            RETURN_IF(header->version != VEMB_V16_WARM_REGION_LAYOUT_VERSION ||
                          header->region_id != region_id ||
                          header->capacity_slots != capacity_slots,
                      -1);
            return 0;
        }
        if (magic == 0) {
            uint32_t expected = 0;
            if (atomic_compare_exchange_strong_explicit(
                    &header->magic,
                    &expected,
                    VEMB_V16_WARM_REGION_LAYOUT_INITIALIZING,
                    memory_order_acq_rel,
                    memory_order_acquire)) {
                memset(header, 0, sizeof(*header));
                header->version = VEMB_V16_WARM_REGION_LAYOUT_VERSION;
                header->region_id = region_id;
                header->capacity_slots = capacity_slots;
                header->value_size = value_size;
                header->region_bytes = region_bytes;
                warm_region_layout_init_slot_meta(header,
                                                  region_id,
                                                  capacity_slots);
                atomic_thread_fence(memory_order_release);
                atomic_store_explicit(&header->magic,
                                      VEMB_V16_WARM_REGION_LAYOUT_MAGIC,
                                      memory_order_release);
                return 0;
            }
        }
        cpu_relax();
    }
    return -1;
}

static int storage_owner_snapshot_build(
        vemb_v16_storage_owner_resolver_snapshot_t *snapshot,
        const vemb_v16_topology_ring_t *ring) {
    RETURN_IF(!snapshot || !ring, -1);
    memset(snapshot, 0, sizeof(*snapshot));
    snapshot->owner_ring = *ring;
    RETURN_IF(snapshot->owner_ring.node_count >
              VEMB_V16_STORAGE_MAX_OWNER_HASH_NODES,
              -1);
    snapshot->owner_hash_node_count = snapshot->owner_ring.node_count;
    for (uint32_t i = 0; i < snapshot->owner_hash_node_count; i++) {
        snapshot->owner_hash_nodes[i] = (vemb_v16_storage_owner_hash_node_t){
            .hash_value = snapshot->owner_ring.nodes[i].hash_value,
            .owner_id = snapshot->owner_ring.nodes[i].owner_id,
        };
    }
    return 0;
}

static int storage_owner_snapshot_publish(
        vemb_v16_storage_ctx_t *storage,
        const vemb_v16_topology_ring_t *ring) {
    RETURN_IF(!storage || !ring, -1);
    uint32_t active = atomic_load_explicit(&storage->owner_resolver_active_snapshot,
                                           memory_order_relaxed);
    uint32_t publish_slot = active ^ 1u;
    RETURN_IF(storage_owner_snapshot_build(
                  &storage->owner_resolver_snapshots[publish_slot],
                  ring) != 0,
              -1);
    atomic_store_explicit(&storage->owner_resolver_active_snapshot,
                          publish_slot,
                          memory_order_release);
    return 0;
}

static int storage_owner_snapshot_stage_pending(
        vemb_v16_storage_ctx_t *storage,
        const vemb_v16_topology_ring_t *ring) {
    RETURN_IF(!storage || !ring, -1);
    uint32_t active = atomic_load_explicit(&storage->owner_resolver_active_snapshot,
                                           memory_order_relaxed);
    uint32_t pending_slot = active ^ 1u;
    RETURN_IF(storage_owner_snapshot_build(
                  &storage->owner_resolver_snapshots[pending_slot],
                  ring) != 0,
              -1);
    storage->owner_resolver_pending_snapshot = pending_slot;
    storage->owner_resolver_pending_valid = 1;
    return 0;
}

static int storage_owner_snapshot_publish_pending(
        vemb_v16_storage_ctx_t *storage) {
    RETURN_IF(!storage || !storage->owner_resolver_pending_valid, -1);
    atomic_store_explicit(&storage->owner_resolver_active_snapshot,
                          storage->owner_resolver_pending_snapshot,
                          memory_order_release);
    storage->owner_resolver_pending_valid = 0;
    return 0;
}

static void manifest_region_defaults(vemb_v16_manifest_region_t *region,
                                     uint32_t value_size) {
    memset(region, 0, sizeof(*region));
    region->backend_type = VEMB_V16_REGION_LOCAL_SHM;
    region->cache_policy = VEMB_V16_UB_CACHE_POLICY_CACHEABLE;
    region->weight = 1;
    region->value_size = value_size;
}

static void manifest_remote_meta_view_defaults(
        vemb_v16_manifest_remote_meta_view_t *view) {
    memset(view, 0, sizeof(*view));
    view->backend_type = VEMB_V16_REGION_LOCAL_SHM;
    view->cache_policy = VEMB_V16_UB_CACHE_POLICY_CACHEABLE;
}

static void manifest_ub_rpc_ring_defaults(
        vemb_v16_ub_rpc_ring_config_t *ring) {
    memset(ring, 0, sizeof(*ring));
    ring->backend_type = VEMB_V16_REGION_LOCAL_SHM;
    ring->cache_policy = VEMB_V16_UB_CACHE_POLICY_CACHEABLE;
}

static void manifest_ub_rpc_peer_defaults(
        vemb_v16_manifest_ub_rpc_peer_t *peer) {
    memset(peer, 0, sizeof(*peer));
    manifest_ub_rpc_ring_defaults(&peer->request);
    manifest_ub_rpc_ring_defaults(&peer->response);
    manifest_ub_rpc_ring_defaults(&peer->inbound_request);
    manifest_ub_rpc_ring_defaults(&peer->outbound_response);
}

static int manifest_push_region(vemb_v16_warm_regions_manifest_t *manifest,
                                const vemb_v16_manifest_region_t *region) {
    RETURN_IF(manifest->region_count >= VEMB_V16_MAX_MANIFEST_REGIONS, -1);
    RETURN_IF(!region->path[0] || region->value_size == 0 ||
              region->region_bytes < region->value_size, -1);
    for (uint32_t i = 0; i < manifest->region_count; i++) {
        RETURN_IF(manifest->regions[i].region_id == region->region_id, -1);
    }
    manifest->regions[manifest->region_count++] = *region;
    return 0;
}

static int manifest_push_remote_meta_view(
        vemb_v16_warm_regions_manifest_t *manifest,
        const vemb_v16_manifest_remote_meta_view_t *view) {
    RETURN_IF(manifest->remote_meta_view_count >=
              VEMB_V16_MAX_MANIFEST_REMOTE_META_VIEWS, -1);
    RETURN_IF(!view->has_owner_id || !view->path[0], -1);
    RETURN_IF(view->entry_count == 0 &&
              (view->set_count == 0 || view->ways == 0), -1);
    for (uint32_t i = 0; i < manifest->remote_meta_view_count; i++) {
        RETURN_IF(manifest->remote_meta_views[i].owner_id == view->owner_id,
                  -1);
    }
    manifest->remote_meta_views[manifest->remote_meta_view_count++] = *view;
    return 0;
}

static int manifest_ring_config_valid(
        const vemb_v16_ub_rpc_ring_config_t *ring) {
    return ring->path[0] &&
           (ring->backend_type == VEMB_V16_REGION_LOCAL_SHM ||
            ring->backend_type == VEMB_V16_REGION_UB) &&
           (ring->cache_policy == VEMB_V16_UB_CACHE_POLICY_CACHEABLE ||
            ring->cache_policy == VEMB_V16_UB_CACHE_POLICY_NONCACHEABLE);
}

static int manifest_ub_rpc_peer_config_valid(
        const vemb_v16_manifest_ub_rpc_peer_t *peer) {
    if (!peer || !manifest_ring_config_valid(&peer->request) ||
        !manifest_ring_config_valid(&peer->response) ||
        !manifest_ring_config_valid(&peer->inbound_request) ||
        !manifest_ring_config_valid(&peer->outbound_response)) {
        return 0;
    }
    /* Cache policy is an explicit deployment property. Imported UB views may
     * be NC for both request and response directions; preserve only the
     * shape/backend/path checks above. */
    return 1;
}

static int manifest_push_ub_rpc_peer(
        vemb_v16_warm_regions_manifest_t *manifest,
        const vemb_v16_manifest_ub_rpc_peer_t *peer) {
    RETURN_IF(manifest->ub_rpc_peer_count >=
              VEMB_V16_MAX_MANIFEST_UB_RPC_PEERS, -1);
    RETURN_IF(!peer->has_owner_id, -1);
    RETURN_IF(!manifest_ub_rpc_peer_config_valid(peer), -1);
    for (uint32_t i = 0; i < manifest->ub_rpc_peer_count; i++) {
        RETURN_IF(manifest->ub_rpc_peers[i].owner_id == peer->owner_id,
                  -1);
    }
    manifest->ub_rpc_peers[manifest->ub_rpc_peer_count++] = *peer;
    return 0;
}

static int storage_cache_ub_rpc_peer_config(
        vemb_v16_storage_ctx_t *storage,
        const vemb_v16_manifest_ub_rpc_peer_t *src);

static int storage_remote_meta_open_view(vemb_v16_storage_ctx_t *storage,
                                         uint32_t owner_supernode_id,
                                         uint32_t backend_type,
                                         uint32_t cache_policy,
                                         const char *path,
                                         uint64_t mmap_offset,
                                         uint32_t requested_entry_count,
                                         uint32_t requested_bucket_count,
                                         uint32_t requested_set_count,
                                         uint32_t requested_ways,
                                         int allow_malloc,
                                         vemb_v16_mapped_region_t *mapping,
                                         uint32_t *is_mapped,
                                         void **base,
                                         size_t *bytes,
                                         uint32_t *entry_count,
                                         uint32_t *bucket_count,
                                         uint32_t *set_count,
                                         uint32_t *ways,
                                         vemb_v16_remote_meta_view_t *view) {
    *is_mapped = 0;
    *base = NULL;
    *set_count = requested_set_count;
    *ways = requested_ways;
    if (*set_count || *ways) {
        RETURN_IF(*set_count == 0 || *ways == 0, -1);
        *entry_count = *set_count * *ways;
        *bucket_count = *set_count;
        *bytes = vemb_v16_remote_meta_layout_bytes_for_sets(*set_count,
                                                            *ways);
    } else {
        *entry_count = requested_entry_count ?
            requested_entry_count : storage->max_vectors;
        *bucket_count = requested_bucket_count ?
            requested_bucket_count :
            vemb_v16_pow2_ceil_u32((uint64_t)(*entry_count) * 2u);
        *bytes = vemb_v16_remote_meta_layout_bytes(*entry_count,
                                                   *bucket_count);
    }
    RETURN_IF(*bytes == 0, -1);

    if (path && path[0]) {
        if ((mmap_offset & 63u) != 0) {
            serverLog(LL_WARNING,
                      "vemb_v16 remote meta offset misaligned: owner=%u path=%s offset=%llu alignment=64",
                      owner_supernode_id,
                      path,
                      (unsigned long long)mmap_offset);
            return -1;
        }
        *is_mapped = 1;
        if (vemb_v16_mapped_region_open(mapping,
                                        backend_type,
                                        cache_policy,
                                        path,
                                        mmap_offset,
                                        *bytes) != 0) {
            serverLog(LL_WARNING,
                      "failed to open vemb_v16 remote meta backing: owner=%u backend=%u path=%s offset=%llu entries=%u buckets=%u bytes=%zu",
                      owner_supernode_id,
                      backend_type,
                      path,
                      (unsigned long long)mmap_offset,
                      *entry_count,
                      *bucket_count,
                      *bytes);
            return -1;
        }
        *base = mapping->mapped_addr;
    } else if (allow_malloc) {
        if (posix_memalign(base, 64, *bytes) != 0) {
            *base = NULL;
            serverLog(LL_WARNING,
                      "failed to allocate vemb_v16 remote meta: owner=%u entries=%u buckets=%u bytes=%zu",
                      owner_supernode_id,
                      *entry_count,
                      *bucket_count,
                      *bytes);
            return -1;
        }
    } else {
        serverLog(LL_WARNING,
                  "remote meta owner view requires mapped backing: owner=%u",
                  owner_supernode_id);
        return -1;
    }

    vemb_v16_remote_meta_header_t *header = *base;
    int rc;
    if (*is_mapped &&
        header->magic == VEMB_V16_REMOTE_META_MAGIC &&
        header->version == VEMB_V16_REMOTE_META_VERSION) {
        rc = vemb_v16_remote_meta_attach(view, *base, *bytes);
    } else {
        if (*set_count && *ways) {
            rc = vemb_v16_remote_meta_init_sets(view,
                                                *base,
                                                *bytes,
                                                owner_supernode_id,
                                                storage->vector_stride,
                                                *set_count,
                                                *ways);
        } else {
            rc = vemb_v16_remote_meta_init(view,
                                           *base,
                                           *bytes,
                                           owner_supernode_id,
                                           storage->vector_stride,
                                           *entry_count,
                                           *bucket_count);
        }
    }
    if (rc != VEMB_V16_REMOTE_META_OK) {
        serverLog(LL_WARNING,
                  "failed to initialize vemb_v16 remote meta: owner=%u backend=%u path=%s entries=%u buckets=%u bytes=%zu",
                  owner_supernode_id,
                  backend_type,
                  path && path[0] ? path : "(malloc)",
                  *entry_count,
                  *bucket_count,
                  *bytes);
        if (*is_mapped) {
            vemb_v16_mapped_region_close(mapping);
        } else if (*base) {
            free(*base);
        }
        *is_mapped = 0;
        *base = NULL;
        return -1;
    }
    *set_count = view->header->bucket_count;
    *ways = view->header->ways;
    serverLog(LL_NOTICE,
              "vemb_v16 remote meta ready: owner=%u backend=%u path=%s offset=%llu entries=%u sets=%u ways=%u bytes=%zu base=%p mapped=%u",
              owner_supernode_id,
              backend_type,
              path && path[0] ? path : "(malloc)",
              (unsigned long long)mmap_offset,
              view->header->entry_count,
              view->header->bucket_count,
              view->header->ways,
              *bytes,
              *base,
              *is_mapped);
    return 0;
}

static int storage_remote_meta_init(vemb_v16_storage_ctx_t *storage,
                                    const vemb_v16_warm_regions_manifest_t *manifest) {
    uint32_t owner_supernode_id = manifest->has_local_ub_node_id ?
        manifest->local_ub_node_id : 0;
    storage->remote_meta_backend_type = manifest->has_remote_meta_backend_type ?
        manifest->remote_meta_backend_type : VEMB_V16_REGION_LOCAL_SHM;
    storage->remote_meta_mmap_offset = manifest->remote_meta_mmap_offset;
    if (manifest->remote_meta_path[0]) {
        strncpy(storage->remote_meta_path,
                manifest->remote_meta_path,
                sizeof(storage->remote_meta_path) - 1);
    }

    return storage_remote_meta_open_view(storage,
                                         owner_supernode_id,
                                         storage->remote_meta_backend_type,
                                         manifest->has_remote_meta_cache_policy ?
                                             manifest->remote_meta_cache_policy :
                                             VEMB_V16_UB_CACHE_POLICY_CACHEABLE,
                                         storage->remote_meta_path,
                                         storage->remote_meta_mmap_offset,
                                         manifest->remote_meta_entry_count,
                                         manifest->remote_meta_bucket_count,
                                         manifest->remote_meta_set_count,
                                         manifest->remote_meta_ways,
                                         1,
                                         &storage->remote_meta_mapping,
                                         &storage->remote_meta_is_mapped,
                                         &storage->remote_meta_base,
                                         &storage->remote_meta_bytes,
                                         &storage->remote_meta_entry_count,
                                         &storage->remote_meta_bucket_count,
                                         &storage->remote_meta_set_count,
                                         &storage->remote_meta_ways,
                                         &storage->remote_meta_view);
}

static int storage_remote_meta_owner_views_init(
        vemb_v16_storage_ctx_t *storage,
        const vemb_v16_warm_regions_manifest_t *manifest) {
    for (uint32_t i = 0; i < manifest->remote_meta_view_count; i++) {
        const vemb_v16_manifest_remote_meta_view_t *src =
            &manifest->remote_meta_views[i];
        vemb_v16_storage_remote_meta_view_t *dst =
            &storage->remote_meta_owner_views[storage->remote_meta_owner_view_count];
        uint32_t backend_type = src->has_backend_type ?
            src->backend_type : VEMB_V16_REGION_LOCAL_SHM;

        memset(dst, 0, sizeof(*dst));
        dst->mapping.fd = -1;
        dst->owner_id = src->owner_id;
        dst->backend_type = backend_type;
        dst->mmap_offset = src->mmap_offset;
        strncpy(dst->path, src->path, sizeof(dst->path) - 1);

        if (storage_remote_meta_open_view(storage,
                                          dst->owner_id,
                                          dst->backend_type,
                                          src->cache_policy,
                                          dst->path,
                                          dst->mmap_offset,
                                          src->entry_count,
                                          src->bucket_count,
                                          src->set_count,
                                          src->ways,
                                          0,
                                          &dst->mapping,
                                          &dst->is_mapped,
                                          &dst->base,
                                          &dst->bytes,
                                          &dst->entry_count,
                                          &dst->bucket_count,
                                          &dst->set_count,
                                          &dst->ways,
                                          &dst->view) != 0) {
            return -1;
        }
        storage->remote_meta_owner_view_count++;
        if (vemb_v16_tlc_set_remote_meta_owner_view(storage->tlc,
                                                    dst->owner_id,
                                                    &dst->view) != 0) {
            serverLog(LL_WARNING,
                      "failed to register vemb_v16 remote meta owner view: owner=%u path=%s",
                      dst->owner_id,
                      dst->path);
            return -1;
        }
        serverLog(LL_NOTICE,
                  "registered vemb_v16 remote meta owner view: owner=%u backend=%u path=%s offset=%llu entries=%u sets=%u ways=%u",
                  dst->owner_id,
                  dst->backend_type,
                  dst->path,
                  (unsigned long long)dst->mmap_offset,
                  dst->entry_count,
                  dst->set_count,
                  dst->ways);
    }
    return 0;
}

static int storage_owner_resolver_init(
        vemb_v16_storage_ctx_t *storage,
        const vemb_v16_warm_regions_manifest_t *manifest) {
    uint32_t owners[VEMB_V16_STORAGE_MAX_OWNER_COUNT];
    uint32_t owner_count = 0;
    owners[owner_count++] = manifest->has_local_ub_node_id ?
        manifest->local_ub_node_id : 0;
    for (uint32_t i = 0; i < manifest->remote_meta_view_count; i++) {
        uint32_t owner_id = manifest->remote_meta_views[i].owner_id;
        int exists = 0;
        for (uint32_t j = 0; j < owner_count; j++) {
            if (owners[j] == owner_id) {
                exists = 1;
                break;
            }
        }
        if (!exists) {
            RETURN_IF(owner_count >= VEMB_V16_STORAGE_MAX_OWNER_COUNT, -1);
            owners[owner_count++] = owner_id;
        }
    }
    for (uint32_t i = 0; i < manifest->ub_rpc_peer_count; i++) {
        uint32_t owner_id = manifest->ub_rpc_peers[i].owner_id;
        int exists = 0;
        for (uint32_t j = 0; j < owner_count; j++) {
            if (owners[j] == owner_id) {
                exists = 1;
                break;
            }
        }
        if (!exists) {
            RETURN_IF(owner_count >= VEMB_V16_STORAGE_MAX_OWNER_COUNT, -1);
            owners[owner_count++] = owner_id;
        }
    }
    if (owner_count <= 1)
        return 0;

    qsort(owners, owner_count, sizeof(owners[0]), cmp_u32);
    vemb_v16_topology_ring_t owner_ring;
    RETURN_IF(vemb_v16_topology_ring_build(
                  &owner_ring,
                  0,
                  owners,
                  owner_count,
                  VEMB_V16_STORAGE_OWNER_HASH_VNODES) !=
              VEMB_V16_TOPOLOGY_OK,
              -1);
    RETURN_IF(owner_ring.node_count >
              VEMB_V16_STORAGE_MAX_OWNER_HASH_NODES,
              -1);
    RETURN_IF(storage_owner_snapshot_publish(storage, &owner_ring) != 0, -1);
    vemb_v16_tlc_set_owner_resolver(storage->tlc,
                                    storage_owner_resolver,
                                    storage);
    serverLog(LL_NOTICE,
              "vemb_v16 storage owner resolver ready: owners=%u hash_nodes=%u local_owner=%u",
              owner_count,
              owner_ring.node_count,
              manifest->has_local_ub_node_id ? manifest->local_ub_node_id : 0);
    return 0;
}

static int storage_ub_rpc_init(vemb_v16_storage_ctx_t *storage,
                               const vemb_v16_warm_regions_manifest_t *manifest) {
    storage->ub_rpc_timeout_ms = manifest->ub_rpc_timeout_ms;
    storage->ub_rpc_peer_config_count = 0;
    for (uint32_t i = 0; i < manifest->ub_rpc_peer_count; i++) {
        RETURN_IF(!!storage_cache_ub_rpc_peer_config(storage, &manifest->ub_rpc_peers[i]),
                  -1);
    }
    if (manifest->ub_rpc_peer_count == 0)
        return 0;

    vemb_v16_ub_rpc_peer_t peers[VEMB_V16_MAX_MANIFEST_UB_RPC_PEERS];
    memset(peers, 0, sizeof(peers));
    for (uint32_t i = 0; i < manifest->ub_rpc_peer_count; i++) {
        peers[i] = (vemb_v16_ub_rpc_peer_t){
            .owner_id = manifest->ub_rpc_peers[i].owner_id,
            .request = manifest->ub_rpc_peers[i].request,
            .response = manifest->ub_rpc_peers[i].response,
            .inbound_request = manifest->ub_rpc_peers[i].inbound_request,
            .outbound_response = manifest->ub_rpc_peers[i].outbound_response,
        };
    }

    uint32_t local_owner_id = manifest->has_local_ub_node_id ?
        manifest->local_ub_node_id : 0;
    if (vemb_v16_ub_rpc_create(&storage->ub_rpc,
                               storage->tlc,
                               local_owner_id,
                               manifest->ub_rpc_timeout_ms,
                               peers,
                               manifest->ub_rpc_peer_count) != 0) {
        serverLog(LL_WARNING,
                  "failed to initialize vemb_v16 ub rpc: local_owner=%u peers=%u",
                  local_owner_id,
                  manifest->ub_rpc_peer_count);
        return -1;
    }
    vemb_v16_ub_rpc_install_lookup_runtime(storage->tlc, storage->ub_rpc, NULL);
    serverLog(LL_NOTICE,
              "vemb_v16 ub rpc ready: local_owner=%u peers=%u timeout_ms=%u",
              local_owner_id,
              manifest->ub_rpc_peer_count,
              manifest->ub_rpc_timeout_ms);
    return 0;
}

static int storage_has_region_id(const vemb_v16_storage_ctx_t *storage,
                                 uint32_t region_id) {
    for (uint32_t i = 0; i < storage->warm_region_count; i++) {
        if (storage->warm_providers[i].region.region_id == region_id)
            return 1;
    }
    return 0;
}

// Check whether one owner's warm region is already attached in runtime state.
int vemb_v16_storage_has_region_for_owner(const vemb_v16_storage_ctx_t *storage,
                                          uint32_t owner_id) {
    for (uint32_t i = 0; i < storage->warm_region_count; i++) {
        if (storage->warm_providers[i].region.home_ub_node_id == owner_id)
            return 1;
    }
    return 0;
}

static int storage_has_remote_meta_owner_view(const vemb_v16_storage_ctx_t *storage,
                                              uint32_t owner_id) {
    for (uint32_t i = 0; i < storage->remote_meta_owner_view_count; i++) {
        if (storage->remote_meta_owner_views[i].owner_id == owner_id)
            return 1;
    }
    return 0;
}

static const vemb_v16_manifest_remote_meta_view_t *
storage_find_remote_meta_view_config(const vemb_v16_storage_ctx_t *storage,
                                     uint32_t owner_id) {
    for (uint32_t i = 0; i < storage->peer_remote_meta_view_config_count; i++) {
        if (storage->peer_remote_meta_view_configs[i].owner_id == owner_id)
            return &storage->peer_remote_meta_view_configs[i];
    }
    return NULL;
}

static const vemb_v16_manifest_ub_rpc_peer_t *
storage_find_ub_rpc_peer_config(const vemb_v16_storage_ctx_t *storage,
                                uint32_t owner_id) {
    for (uint32_t i = 0; i < storage->ub_rpc_peer_config_count; i++) {
        if (storage->ub_rpc_peer_configs[i].owner_id == owner_id)
            return &storage->ub_rpc_peer_configs[i];
    }
    return NULL;
}

static int storage_cache_peer_region_config(vemb_v16_storage_ctx_t *storage,
                                            const vemb_v16_manifest_region_t *src) {
    for (uint32_t i = 0; i < storage->peer_region_config_count; i++) {
        vemb_v16_manifest_region_t *dst = &storage->peer_region_configs[i];
        if (dst->region_id != src->region_id)
            continue;
        RETURN_IF(memcmp(dst, src, sizeof(*src)) != 0, -1);
        return 0;
    }
    RETURN_IF(storage->peer_region_config_count >= VEMB_V16_PEER_VIEW_MAP_MAX_REGIONS, -1);
    storage->peer_region_configs[storage->peer_region_config_count++] = *src;
    return 0;
}

static int storage_cache_remote_meta_view_config(
        vemb_v16_storage_ctx_t *storage,
        const vemb_v16_manifest_remote_meta_view_t *src) {
    RETURN_IF(!storage || !src || !src->has_owner_id, -1);
    for (uint32_t i = 0; i < storage->peer_remote_meta_view_config_count; i++) {
        vemb_v16_manifest_remote_meta_view_t *dst =
            &storage->peer_remote_meta_view_configs[i];
        if (dst->owner_id != src->owner_id)
            continue;
        RETURN_IF(memcmp(dst, src, sizeof(*src)) != 0, -1);
        return 0;
    }
    RETURN_IF(storage->peer_remote_meta_view_config_count >=
                  VEMB_V16_PEER_VIEW_MAP_MAX_REMOTE_META_VIEWS,
              -1);
    storage->peer_remote_meta_view_configs
        [storage->peer_remote_meta_view_config_count++] = *src;
    return 0;
}

static int storage_cache_ub_rpc_peer_config(
        vemb_v16_storage_ctx_t *storage,
        const vemb_v16_manifest_ub_rpc_peer_t *src) {
    RETURN_IF(!storage || !src || !src->has_owner_id, -1);
    for (uint32_t i = 0; i < storage->ub_rpc_peer_config_count; i++) {
        vemb_v16_manifest_ub_rpc_peer_t *dst =
            &storage->ub_rpc_peer_configs[i];
        if (dst->owner_id != src->owner_id)
            continue;
        RETURN_IF(memcmp(dst, src, sizeof(*src)) != 0, -1);
        return 0;
    }
    RETURN_IF(storage->ub_rpc_peer_config_count >=
                  VEMB_V16_MAX_MANIFEST_UB_RPC_PEERS,
              -1);
    storage->ub_rpc_peer_configs[storage->ub_rpc_peer_config_count++] = *src;
    return 0;
}

static int storage_attach_remote_meta_owner_view_config(
        vemb_v16_storage_ctx_t *storage,
        const vemb_v16_manifest_remote_meta_view_t *src) {
    RETURN_IF(!storage || !src || !src->has_owner_id || !src->path[0], -1);
    if (storage_has_remote_meta_owner_view(storage, src->owner_id))
        return 0;
    RETURN_IF(storage->remote_meta_owner_view_count >=
                  VEMB_V16_MAX_MANIFEST_REMOTE_META_VIEWS,
              -1);
    vemb_v16_storage_remote_meta_view_t *dst =
        &storage->remote_meta_owner_views[storage->remote_meta_owner_view_count];
    memset(dst, 0, sizeof(*dst));
    dst->mapping.fd = -1;
    dst->owner_id = src->owner_id;
    dst->backend_type = src->has_backend_type ?
        src->backend_type : VEMB_V16_REGION_LOCAL_SHM;
    dst->mmap_offset = src->mmap_offset;
    dst->entry_count = src->entry_count;
    dst->bucket_count = src->bucket_count;
    dst->set_count = src->set_count;
    dst->ways = src->ways;
    strncpy(dst->path, src->path, sizeof(dst->path) - 1);
    RETURN_IF(storage_remote_meta_open_view(storage,
                                            dst->owner_id,
                                            dst->backend_type,
                                            src->cache_policy,
                                            dst->path,
                                            dst->mmap_offset,
                                            dst->entry_count,
                                            dst->bucket_count,
                                            dst->set_count,
                                            dst->ways,
                                            0,
                                            &dst->mapping,
                                            &dst->is_mapped,
                                            &dst->base,
                                            &dst->bytes,
                                            &dst->entry_count,
                                            &dst->bucket_count,
                                            &dst->set_count,
                                            &dst->ways,
                                            &dst->view) != 0,
              -1);
    RETURN_IF(vemb_v16_tlc_set_remote_meta_owner_view(storage->tlc,
                                                      dst->owner_id,
                                                      &dst->view) != 0,
              -1);
    storage->remote_meta_owner_view_count++;
    serverLog(LL_NOTICE,
              "vemb_v16 runtime remote_meta owner view attached: local_owner=%u peer_owner=%u path=%s",
              storage->local_owner_id,
              dst->owner_id,
              dst->path);
    return 0;
}

static int reset_ub_rpc_ring_backing(
        const vemb_v16_ub_rpc_ring_config_t *ring,
        int is_response_ring);

static int storage_attach_ub_rpc_peer_config(
        vemb_v16_storage_ctx_t *storage,
        const vemb_v16_manifest_ub_rpc_peer_t *src) {
    RETURN_IF(!storage || !src || !src->has_owner_id, -1);
    if (vemb_v16_ub_rpc_has_peer(storage->ub_rpc, src->owner_id))
        return 0;
    vemb_v16_ub_rpc_peer_t peer = {
        .owner_id = src->owner_id,
        .request = src->request,
        .response = src->response,
        .inbound_request = src->inbound_request,
        .outbound_response = src->outbound_response,
    };
    /* Runtime peer attach is the producer for request and outbound_response.
     * UB receivers are peer-owned CC mappings. Local SHM retains its shared
     * test/runtime reset behavior because it has no NC/CC device direction. */
    if (reset_ub_rpc_ring_backing(&peer.request, 0) != 0 ||
        reset_ub_rpc_ring_backing(&peer.outbound_response, 1) != 0 ||
        (peer.response.backend_type == VEMB_V16_REGION_LOCAL_SHM &&
         reset_ub_rpc_ring_backing(&peer.response, 1) != 0) ||
        (peer.inbound_request.backend_type == VEMB_V16_REGION_LOCAL_SHM &&
         reset_ub_rpc_ring_backing(&peer.inbound_request, 0) != 0)) {
        serverLog(LL_WARNING,
                  "vemb_v16 runtime ub rpc peer reset rings failed: local_owner=%u peer_owner=%u",
                  storage->local_owner_id,
                  peer.owner_id);
        return -1;
    }
    RETURN_IF(vemb_v16_ub_rpc_attach_peer(&storage->ub_rpc,
                                          storage->tlc,
                                          storage->local_owner_id,
                                          storage->ub_rpc_timeout_ms,
                                          &peer) != 0,
              -1);
    if (!storage->migration_retry_thread_started) {
        RETURN_IF(vemb_v16_storage_migration_retry_start(storage, 0, 0) != 0,
                  -1);
    }
    serverLog(LL_NOTICE,
              "vemb_v16 runtime ub rpc peer attached: local_owner=%u peer_owner=%u req=%s resp=%s",
              storage->local_owner_id,
              peer.owner_id,
              peer.request.path,
              peer.response.path);
    return 0;
}

static int storage_attach_warm_region_config(vemb_v16_storage_ctx_t *storage,
                                             const vemb_v16_manifest_region_t *src) {
    RETURN_IF(src->region_id == 0 || !src->path[0], -1);
    RETURN_IF(src->backend_type != VEMB_V16_REGION_UB, -1);
    RETURN_IF(storage_has_region_id(storage, src->region_id), 0);
    RETURN_IF(storage->warm_region_count >= VEMB_V16_MAX_MANIFEST_REGIONS, -1);

    uint32_t i = storage->warm_region_count;
    uint32_t capacity_slots = (uint32_t)(src->region_bytes / src->value_size);
    size_t allocator_layout_bytes = vemb_v16_warm_region_layout_bytes(capacity_slots);

    storage->warm_providers[i].fd = -1;
    storage->warm_data_mappings[i].fd = -1;
    storage->warm_allocator_mappings[i].fd = -1;

    if (src->backend_type == VEMB_V16_REGION_UB && src->is_local) {
        RETURN_IF(vemb_v16_warm_region_layout_reset(src->backend_type,
                                                     src->cache_policy,
                                                     src->path,
                                                     src->mmap_offset,
                                                     src->region_id,
                                                     capacity_slots) != 0,
                  -1);
    }
    RETURN_IF(vemb_v16_mapped_region_open(&storage->warm_data_mappings[i],
                                          src->backend_type,
                                          src->cache_policy,
                                          src->path,
                                          src->mmap_offset,
                                          allocator_layout_bytes +
                                              (size_t)src->region_bytes) != 0,
              -1);
    RETURN_IF(vemb_v16_warm_provider_attach(&storage->warm_providers[i],
                                            &storage->warm_data_mappings[i],
                                            allocator_layout_bytes,
                                            src->region_id,
                                            src->backend_type,
                                            src->path,
                                            src->mmap_offset +
                                                allocator_layout_bytes,
                                            src->value_size,
                                            src->region_bytes,
                                            src->home_ub_node_id,
                                            src->is_local,
                                            src->weight ? src->weight : 1) != 0,
              -1);
    vemb_v16_tlc_warm_region_t warm_region = storage->warm_providers[i].region;
    warm_region.slot_meta = warm_slot_meta_from_mapping(
        &storage->warm_data_mappings[i],
        capacity_slots);
    RETURN_IF(vemb_v16_tlc_attach_warm_region(storage->tlc, &warm_region) != 0, -1);
    storage->warm_region_count++;
    serverLog(LL_NOTICE,
              "vemb_v16 runtime warm region attached: local_owner=%u peer_owner=%u region_id=%u backend=%u path=%s bytes=%llu",
              storage->local_owner_id,
              src->home_ub_node_id,
              src->region_id,
              src->backend_type,
              src->path,
              (unsigned long long)src->region_bytes);
    return 0;
}

// Attach cached peer-view mapping for one owner into runtime state.
int vemb_v16_storage_attach_peer_owner_from_mapping(
        vemb_v16_storage_ctx_t *storage, uint32_t owner_id) {
    RETURN_IF(owner_id == UINT32_MAX, -1);
    for (uint32_t i = 0; i < storage->peer_region_config_count; i++) {
        const vemb_v16_manifest_region_t *region = &storage->peer_region_configs[i];
        if (region->home_ub_node_id != owner_id) continue;
        RETURN_IF(storage_attach_warm_region_config(storage, region) != 0, -1);
    }
    const vemb_v16_manifest_remote_meta_view_t *view =
        storage_find_remote_meta_view_config(storage, owner_id);
    // Remote-meta view is optional here; RPC is the required peer-runtime path.
    if (view) {
        RETURN_IF(storage_attach_remote_meta_owner_view_config(
            storage, view) != 0, -1);
    }
    const vemb_v16_manifest_ub_rpc_peer_t *rpc =
        storage_find_ub_rpc_peer_config(storage, owner_id);
    // Peer owners must have UB-RPC attached for migration and fallback paths.
    RETURN_IF(!rpc, -1);
    RETURN_IF(storage_attach_ub_rpc_peer_config(storage, rpc) != 0, -1);
    return 0;
}

static int parse_ub_rpc_ring_field(vemb_v16_ub_rpc_ring_config_t *ring,
                                   const char *prefix,
                                   const char *key,
                                   const char *value) {
    char expected[64];
    snprintf(expected, sizeof(expected), "%s_provider", prefix);
    if (!strcmp(key, expected)) {
        return parse_backend_value(value, &ring->backend_type);
    }
    snprintf(expected, sizeof(expected), "%s_backend", prefix);
    if (!strcmp(key, expected)) {
        return parse_backend_value(value, &ring->backend_type);
    }
    snprintf(expected, sizeof(expected), "%s_path", prefix);
    if (!strcmp(key, expected)) {
        RETURN_IF(strlen(value) >= sizeof(ring->path), -1);
        redis_strlcpy(ring->path, value, sizeof(ring->path));
        return 0;
    }
    snprintf(expected, sizeof(expected), "%s_mmap_offset", prefix);
    if (!strcmp(key, expected))
        return parse_u64_value(value, &ring->mmap_offset);
    snprintf(expected, sizeof(expected), "%s_cache_policy", prefix);
    if (!strcmp(key, expected))
        return parse_ub_cache_policy_value(value, &ring->cache_policy);
    return 1;
}

static int parse_manifest_field(vemb_v16_warm_regions_manifest_t *manifest,
                                vemb_v16_manifest_region_t *current,
                                vemb_v16_manifest_remote_meta_view_t *current_meta,
                                vemb_v16_manifest_ub_rpc_peer_t *current_rpc,
                                int in_region,
                                int in_remote_meta_view,
                                int in_ub_rpc_peer,
                                const char *key,
                                const char *value) {
    if (!in_region && !in_remote_meta_view && !in_ub_rpc_peer) {
        if (!strcmp(key, "local_ub_node_id")) {
            if (parse_u32_value(value, &manifest->local_ub_node_id) != 0)
                return -1;
            manifest->has_local_ub_node_id = 1;
            return 0;
        }
        if (!strcmp(key, "local_region_weight")) {
            return parse_u32_value(value, &manifest->local_region_weight);
        }
        if (!strcmp(key, "remote_meta_provider") ||
            !strcmp(key, "remote_meta_backend")) {
            if (parse_backend_value(value, &manifest->remote_meta_backend_type) != 0)
                return -1;
            manifest->has_remote_meta_backend_type = 1;
            return 0;
        }
        if (!strcmp(key, "remote_meta_path")) {
            RETURN_IF(strlen(value) >= sizeof(manifest->remote_meta_path), -1);
            redis_strlcpy(manifest->remote_meta_path, value, sizeof(manifest->remote_meta_path));
            return 0;
        }
        if (!strcmp(key, "remote_meta_mmap_offset"))
            return parse_u64_value(value, &manifest->remote_meta_mmap_offset);
        if (!strcmp(key, "remote_meta_cache_policy")) {
            if (parse_ub_cache_policy_value(value,
                                            &manifest->remote_meta_cache_policy) != 0)
                return -1;
            manifest->has_remote_meta_cache_policy = 1;
            return 0;
        }
        if (!strcmp(key, "job_plane_provider") ||
            !strcmp(key, "job_plane_backend")) {
            if (parse_backend_value(value, &manifest->job_plane_backend_type) != 0)
                return -1;
            manifest->has_job_plane_backend_type = 1;
            return 0;
        }
        if (!strcmp(key, "job_plane_path")) {
            RETURN_IF(strlen(value) >= sizeof(manifest->job_plane_path), -1);
            redis_strlcpy(manifest->job_plane_path, value, sizeof(manifest->job_plane_path));
            return 0;
        }
        if (!strcmp(key, "job_plane_mmap_offset"))
            return parse_u64_value(value, &manifest->job_plane_mmap_offset);
        if (!strcmp(key, "remote_meta_entries") ||
            !strcmp(key, "remote_meta_entry_count"))
            return parse_u32_value(value, &manifest->remote_meta_entry_count);
        if (!strcmp(key, "remote_meta_buckets") ||
            !strcmp(key, "remote_meta_bucket_count"))
            return parse_u32_value(value, &manifest->remote_meta_bucket_count);
        if (!strcmp(key, "remote_meta_sets") ||
            !strcmp(key, "remote_meta_set_count"))
            return parse_u32_value(value, &manifest->remote_meta_set_count);
        if (!strcmp(key, "remote_meta_ways"))
            return parse_u32_value(value, &manifest->remote_meta_ways);
        if (!strcmp(key, "ub_rpc_timeout_ms"))
            return parse_u32_value(value, &manifest->ub_rpc_timeout_ms);
        return 0;
    }

    if (in_region) {
        if (!strcmp(key, "region_id"))
            return parse_u32_value(value, &current->region_id);
        if (!strcmp(key, "provider") || !strcmp(key, "backend"))
            return parse_backend_value(value, &current->backend_type);
        if (!strcmp(key, "path")) {
            RETURN_IF(strlen(value) >= sizeof(current->path), -1);
            redis_strlcpy(current->path, value, sizeof(current->path));
            return 0;
        }
        if (!strcmp(key, "client_path")) {
            RETURN_IF(strlen(value) >= sizeof(current->client_path), -1);
            redis_strlcpy(current->client_path, value, sizeof(current->client_path));
            return 0;
        }
        if (!strcmp(key, "mmap_offset"))
            return parse_u64_value(value, &current->mmap_offset);
        if (!strcmp(key, "bytes") || !strcmp(key, "region_bytes"))
            return parse_u64_value(value, &current->region_bytes);
        if (!strcmp(key, "value_size"))
            return parse_u32_value(value, &current->value_size);
        if (!strcmp(key, "home_ub_node_id"))
            return parse_u32_value(value, &current->home_ub_node_id);
        if (!strcmp(key, "weight"))
            return parse_u32_value(value, &current->weight);
        if (!strcmp(key, "is_local")) {
            if (parse_bool_value(value, &current->is_local) != 0)
                return -1;
            current->has_is_local = 1;
            return 0;
        }
        if (!strcmp(key, "cache_policy")) {
            if (parse_ub_cache_policy_value(value, &current->cache_policy) != 0)
                return -1;
            current->has_cache_policy = 1;
            return 0;
        }
        return 0;
    }

    if (in_remote_meta_view) {
        if (!strcmp(key, "owner_id")) {
            if (parse_u32_value(value, &current_meta->owner_id) != 0)
                return -1;
            current_meta->has_owner_id = 1;
            return 0;
        }
        if (!strcmp(key, "provider") || !strcmp(key, "backend")) {
            if (parse_backend_value(value, &current_meta->backend_type) != 0)
                return -1;
            current_meta->has_backend_type = 1;
            return 0;
        }
        if (!strcmp(key, "path")) {
            RETURN_IF(strlen(value) >= sizeof(current_meta->path), -1);
            redis_strlcpy(current_meta->path, value, sizeof(current_meta->path));
            return 0;
        }
        if (!strcmp(key, "mmap_offset"))
            return parse_u64_value(value, &current_meta->mmap_offset);
        if (!strcmp(key, "entries") || !strcmp(key, "entry_count"))
            return parse_u32_value(value, &current_meta->entry_count);
        if (!strcmp(key, "buckets") || !strcmp(key, "bucket_count"))
            return parse_u32_value(value, &current_meta->bucket_count);
        if (!strcmp(key, "sets") || !strcmp(key, "set_count"))
            return parse_u32_value(value, &current_meta->set_count);
        if (!strcmp(key, "ways"))
            return parse_u32_value(value, &current_meta->ways);
        if (!strcmp(key, "cache_policy")) {
            if (parse_ub_cache_policy_value(value,
                                            &current_meta->cache_policy) != 0)
                return -1;
            current_meta->has_cache_policy = 1;
            return 0;
        }
        return 0;
    }

    if (!strcmp(key, "owner_id")) {
        if (parse_u32_value(value, &current_rpc->owner_id) != 0)
            return -1;
        current_rpc->has_owner_id = 1;
        return 0;
    }
    if (!strcmp(key, "provider") || !strcmp(key, "backend")) {
        uint32_t backend_type = VEMB_V16_REGION_LOCAL_SHM;
        if (parse_backend_value(value, &backend_type) != 0)
            return -1;
        current_rpc->request.backend_type = backend_type;
        current_rpc->response.backend_type = backend_type;
        current_rpc->inbound_request.backend_type = backend_type;
        current_rpc->outbound_response.backend_type = backend_type;
        return 0;
    }
    int parsed = parse_ub_rpc_ring_field(&current_rpc->request,
                                         "request",
                                         key,
                                         value);
    if (parsed <= 0)
        return parsed;
    parsed = parse_ub_rpc_ring_field(&current_rpc->response,
                                     "response",
                                     key,
                                     value);
    if (parsed <= 0)
        return parsed;
    parsed = parse_ub_rpc_ring_field(&current_rpc->inbound_request,
                                     "inbound_request",
                                     key,
                                     value);
    if (parsed <= 0)
        return parsed;
    parsed = parse_ub_rpc_ring_field(&current_rpc->outbound_response,
                                     "outbound_response",
                                     key,
                                     value);
    if (parsed <= 0)
        return parsed;
    return 0;
}

int vemb_v16_parse_warm_regions_manifest(
        const char *path,
        uint32_t value_size,
        vemb_v16_warm_regions_manifest_t *manifest) {
    FILE *fp = fopen(path, "r");
    RETURN_IF(!fp, -1);
    memset(manifest, 0, sizeof(*manifest));
    manifest->local_region_weight = 4;

    char line[512];
    vemb_v16_manifest_region_t current;
    vemb_v16_manifest_remote_meta_view_t current_meta;
    vemb_v16_manifest_ub_rpc_peer_t current_rpc;
    int in_region = 0;
    int in_remote_meta_view = 0;
    int in_ub_rpc_peer = 0;
    enum {
        MANIFEST_SECTION_TOP = 0,
        MANIFEST_SECTION_WARM_REGIONS,
        MANIFEST_SECTION_REMOTE_META_VIEWS,
        MANIFEST_SECTION_UB_RPC_PEERS,
    } section = MANIFEST_SECTION_TOP;
    manifest_region_defaults(&current, value_size);
    manifest_remote_meta_view_defaults(&current_meta);
    manifest_ub_rpc_peer_defaults(&current_rpc);

    while (fgets(line, sizeof(line), fp)) {
        strip_comment(line);
        char *p = trim_ws(line);
        if (!p[0])
            continue;
        if (!strncmp(p, "- ", 2)) {
            if (section == MANIFEST_SECTION_WARM_REGIONS) {
                if (in_region &&
                    manifest_push_region(manifest, &current) != 0) {
                    fclose(fp);
                    return -1;
                }
                manifest_region_defaults(&current, value_size);
                in_region = 1;
            } else if (section == MANIFEST_SECTION_REMOTE_META_VIEWS) {
                if (in_remote_meta_view &&
                    manifest_push_remote_meta_view(manifest,
                                                   &current_meta) != 0) {
                    fclose(fp);
                    return -1;
                }
                manifest_remote_meta_view_defaults(&current_meta);
                in_remote_meta_view = 1;
            } else if (section == MANIFEST_SECTION_UB_RPC_PEERS) {
                if (in_ub_rpc_peer &&
                    manifest_push_ub_rpc_peer(manifest,
                                              &current_rpc) != 0) {
                    fclose(fp);
                    return -1;
                }
                manifest_ub_rpc_peer_defaults(&current_rpc);
                in_ub_rpc_peer = 1;
            } else {
                fclose(fp);
                return -1;
            }
            p = trim_ws(p + 2);
            if (!p[0])
                continue;
        }
        char *colon = strchr(p, ':');
        if (!colon)
            continue;
        *colon = '\0';
        char *key = trim_ws(p);
        char *value = trim_ws(colon + 1);
        if (!value[0]) {
            if (!strcmp(key, "warm_regions")) {
                if (in_remote_meta_view &&
                    manifest_push_remote_meta_view(manifest,
                                                   &current_meta) != 0) {
                    fclose(fp);
                    return -1;
                }
                in_remote_meta_view = 0;
                if (in_ub_rpc_peer &&
                    manifest_push_ub_rpc_peer(manifest,
                                              &current_rpc) != 0) {
                    fclose(fp);
                    return -1;
                }
                in_ub_rpc_peer = 0;
                section = MANIFEST_SECTION_WARM_REGIONS;
            } else if (!strcmp(key, "remote_meta_views")) {
                if (in_region &&
                    manifest_push_region(manifest, &current) != 0) {
                    fclose(fp);
                    return -1;
                }
                in_region = 0;
                if (in_ub_rpc_peer &&
                    manifest_push_ub_rpc_peer(manifest,
                                              &current_rpc) != 0) {
                    fclose(fp);
                    return -1;
                }
                in_ub_rpc_peer = 0;
                section = MANIFEST_SECTION_REMOTE_META_VIEWS;
            } else if (!strcmp(key, "ub_rpc_peers")) {
                if (in_region &&
                    manifest_push_region(manifest, &current) != 0) {
                    fclose(fp);
                    return -1;
                }
                in_region = 0;
                if (in_remote_meta_view &&
                    manifest_push_remote_meta_view(manifest,
                                                   &current_meta) != 0) {
                    fclose(fp);
                    return -1;
                }
                in_remote_meta_view = 0;
                section = MANIFEST_SECTION_UB_RPC_PEERS;
            }
            continue;
        }
        if (parse_manifest_field(manifest, &current, &current_meta,
                                 &current_rpc,
                                 in_region, in_remote_meta_view,
                                 in_ub_rpc_peer,
                                 key, value) != 0) {
            fclose(fp);
            return -1;
        }
    }
    if (in_region &&
        manifest_push_region(manifest, &current) != 0) {
        fclose(fp);
        return -1;
    }
    if (in_remote_meta_view &&
        manifest_push_remote_meta_view(manifest, &current_meta) != 0) {
        fclose(fp);
        return -1;
    }
    if (in_ub_rpc_peer &&
        manifest_push_ub_rpc_peer(manifest, &current_rpc) != 0) {
        fclose(fp);
        return -1;
    }
    fclose(fp);
    RETURN_IF(manifest->region_count == 0, -1);

    for (uint32_t i = 0; i < manifest->region_count; i++) {
        vemb_v16_manifest_region_t *region = &manifest->regions[i];
        if (!region->has_is_local && manifest->has_local_ub_node_id)
            region->is_local = region->home_ub_node_id == manifest->local_ub_node_id;
    }
    return 0;
}

static int unlink_shm_if_exists(const char *name) {
    if (name[0] != '/')
        return -1;
    if (shm_unlink(name) == 0)
        return 0;
    return errno == ENOENT ? 0 : -1;
}

static int reset_remote_meta_backing(uint32_t owner_id,
                                     uint32_t backend_type,
                                     uint32_t cache_policy,
                                     const char *path,
                                     uint64_t mmap_offset,
                                     uint32_t value_size,
                                     uint32_t entry_count,
                                     uint32_t bucket_count,
                                     uint32_t set_count,
                                     uint32_t ways) {
    RETURN_IF(!path[0], 0);
    if (backend_type == VEMB_V16_REGION_LOCAL_SHM) {
        int rc = unlink_shm_if_exists(path);
        int unlink_remote_meta_errno = errno;
        serverLog(LL_NOTICE,
                  "reset remote meta shm: owner=%u path=%s rc=%d status=%s",
                  owner_id,
                  path,
                  rc,
                  strerror(unlink_remote_meta_errno));
        RETURN_IF(rc != 0, -1);
        return 0;
    }
    if (backend_type == VEMB_V16_REGION_UB) {
        if (entry_count == 0 && (set_count == 0 || ways == 0)) {
            serverLog(LL_NOTICE,
                      "skip reset remote meta ub without entry_count: owner=%u path=%s offset=%llu",
                      owner_id,
                      path,
                      (unsigned long long)mmap_offset);
            return 0;
        }
        uint32_t effective_bucket_count = bucket_count ?
            bucket_count :
            vemb_v16_pow2_ceil_u32((uint64_t)entry_count * 2u);
        size_t bytes = (set_count && ways) ?
            vemb_v16_remote_meta_layout_bytes_for_sets(set_count, ways) :
            vemb_v16_remote_meta_layout_bytes(entry_count,
                                              effective_bucket_count);
        RETURN_IF(bytes == 0, -1);
        vemb_v16_mapped_region_t mapping;
        memset(&mapping, 0, sizeof(mapping));
        mapping.fd = -1;
        if (vemb_v16_mapped_region_open(&mapping,
                                        backend_type,
                                        cache_policy,
                                        path,
                                        mmap_offset,
                                        bytes) != 0) {
            serverLog(LL_WARNING,
                      "failed to open remote meta ub during reset: owner=%u path=%s offset=%llu entries=%u buckets=%u bytes=%zu",
                      owner_id,
                      path,
                      (unsigned long long)mmap_offset,
                      entry_count,
                      effective_bucket_count,
                      bytes);
            RETURN_IF(1, -1);
        }
        vemb_v16_remote_meta_view_t view;
        int rc = (set_count && ways) ?
            vemb_v16_remote_meta_init_sets(&view,
                                           mapping.mapped_addr,
                                           bytes,
                                           owner_id,
                                           value_size,
                                           set_count,
                                           ways) :
            vemb_v16_remote_meta_init(&view,
                                      mapping.mapped_addr,
                                      bytes,
                                      owner_id,
                                      value_size,
                                      entry_count,
                                      effective_bucket_count);
        serverLog(LL_NOTICE,
                  "reset remote meta ub: owner=%u path=%s offset=%llu entries=%u buckets=%u bytes=%zu rc=%d",
                  owner_id,
                  path,
                  (unsigned long long)mmap_offset,
                  entry_count,
                  effective_bucket_count,
                  bytes,
                  rc);
        vemb_v16_mapped_region_close(&mapping);
        RETURN_IF(rc != VEMB_V16_REMOTE_META_OK, -1);
        return 0;
    }

    serverLog(LL_WARNING,
              "unsupported remote meta backend type during reset: owner=%u backend_type=%u path=%s",
              owner_id,
              backend_type,
              path);
    return -1;
}

static int reset_ub_rpc_ring_backing(
        const vemb_v16_ub_rpc_ring_config_t *ring,
        int is_response_ring) {
    if (!ring || !ring->path[0])
        return 0;
    if (ring->backend_type == VEMB_V16_REGION_LOCAL_SHM) {
        int rc = unlink_shm_if_exists(ring->path);
        int unlink_errno = errno;
        serverLog(LL_NOTICE,
                  "reset ub rpc shm ring: path=%s rc=%d status=%s",
                  ring->path,
                  rc,
                  strerror(unlink_errno));
        RETURN_IF(rc != 0, -1);
        return 0;
    }
    if (ring->backend_type == VEMB_V16_REGION_UB) {
        int rc = is_response_ring ?
            vemb_v16_ub_rpc_reset_response_ring(ring) :
            vemb_v16_ub_rpc_reset_request_ring(ring);
        int reset_errno = errno;
        serverLog(LL_NOTICE,
                  "reset ub rpc ub ring: path=%s offset=%llu rc=%d status=%s kind=%s",
                  ring->path,
                  (unsigned long long)ring->mmap_offset,
                  rc,
                  strerror(reset_errno),
                  is_response_ring ? "response" : "request");
        RETURN_IF(rc != 0, -1);
        return 0;
    }
    serverLog(LL_WARNING,
              "unsupported ub rpc ring backend during reset: backend=%u path=%s",
              ring->backend_type,
              ring->path);
    return -1;
}

int vemb_v16_storage_reset_manifest_regions(const vemb_v16_warm_regions_manifest_t *manifest) {
    for (uint32_t i = 0; i < manifest->region_count; i++) {
        const vemb_v16_manifest_region_t *region = &manifest->regions[i];
        if (manifest->has_local_ub_node_id && !region->is_local &&
            region->backend_type == VEMB_V16_REGION_UB) {
            continue;
        }
        uint32_t capacity_slots = (uint32_t)(region->region_bytes / region->value_size);
        if (region->backend_type == VEMB_V16_REGION_LOCAL_SHM) {
            char layout_name[VEMB_V16_WARM_REGION_LAYOUT_NAME_MAX];
            int rc = vemb_v16_warm_region_layout_name_from_region_path(
                region->path,
                region->region_id,
                layout_name,
                sizeof(layout_name));
            serverLog(LL_NOTICE,
                      "reset warm allocator shm name: region_id=%u path=%s rc=%d status=%s",
                      region->region_id,
                      region->path,
                      rc,
                      strerror(errno));
            RETURN_IF(rc != 0, -1);

            rc = unlink_shm_if_exists(layout_name);
            int unlink_allocator_errno = errno;
            serverLog(LL_NOTICE,
                      "reset warm allocator shm: region_id=%u allocator=%s rc=%d status=%s",
                      region->region_id,
                      layout_name,
                      rc,
                      strerror(unlink_allocator_errno));
            RETURN_IF(rc != 0, -1);

            rc = unlink_shm_if_exists(region->path);
            int unlink_payload_errno = errno;
            serverLog(LL_NOTICE,
                      "reset warm payload shm: region_id=%u path=%s rc=%d status=%s",
                      region->region_id,
                      region->path,
                      rc,
                      strerror(unlink_payload_errno));
            RETURN_IF(rc != 0, -1);
        } else if (region->backend_type == VEMB_V16_REGION_UB) {
            int rc = vemb_v16_warm_region_layout_reset(region->backend_type,
                                                        region->cache_policy,
                                                        region->path,
                                                        region->mmap_offset,
                                                        region->region_id,
                                                        capacity_slots);
            int reset_allocator_errno = errno;
            serverLog(LL_NOTICE,
                      "reset warm allocator ub: region_id=%u path=%s offset=%llu rc=%d status=%s",
                      region->region_id,
                      region->path,
                      (unsigned long long)region->mmap_offset,
                      rc,
                      strerror(reset_allocator_errno));
            RETURN_IF(rc != 0, -1);
        } else {
            serverLog(LL_WARNING,
                      "unsupported warm region backend type during reset: region_id=%u backend_type=%u",
                      region->region_id,
                      region->backend_type);
            return -1;
        }
    }
    uint32_t owner_id = manifest->has_local_ub_node_id ?  manifest->local_ub_node_id : 0;
    uint32_t backend_type = manifest->has_remote_meta_backend_type ?
        manifest->remote_meta_backend_type : VEMB_V16_REGION_LOCAL_SHM;
    if (reset_remote_meta_backing(owner_id,
                                  backend_type,
                                  manifest->has_remote_meta_cache_policy ?
                                      manifest->remote_meta_cache_policy :
                                      VEMB_V16_UB_CACHE_POLICY_CACHEABLE,
                                  manifest->remote_meta_path,
                                  manifest->remote_meta_mmap_offset,
                                  manifest->regions[0].value_size,
                                  manifest->remote_meta_entry_count,
                                  manifest->remote_meta_bucket_count,
                                  manifest->remote_meta_set_count,
                                  manifest->remote_meta_ways) != 0) {
        RETURN_IF(1, -1);
    }
    for (uint32_t i = 0; i < manifest->remote_meta_view_count; i++) {
        const vemb_v16_manifest_remote_meta_view_t *view =
            &manifest->remote_meta_views[i];
        backend_type = view->has_backend_type ?
            view->backend_type : VEMB_V16_REGION_LOCAL_SHM;
        if (manifest->has_local_ub_node_id &&
            view->owner_id != manifest->local_ub_node_id &&
            backend_type == VEMB_V16_REGION_UB) {
            continue;
        }
        if (reset_remote_meta_backing(view->owner_id,
                                      backend_type,
                                      view->cache_policy,
                                      view->path,
                                      view->mmap_offset,
                                      manifest->regions[0].value_size,
                                      view->entry_count,
                                      view->bucket_count,
                                      view->set_count,
                                      view->ways) != 0) {
            RETURN_IF(1, -1);
        }
    }
    for (uint32_t i = 0; i < manifest->ub_rpc_peer_count; i++) {
        const vemb_v16_manifest_ub_rpc_peer_t *peer =
            &manifest->ub_rpc_peers[i];
        if (reset_ub_rpc_ring_backing(&peer->request, 0) != 0 ||
            reset_ub_rpc_ring_backing(&peer->outbound_response, 1) != 0 ||
            (peer->response.backend_type == VEMB_V16_REGION_LOCAL_SHM &&
             reset_ub_rpc_ring_backing(&peer->response, 1) != 0) ||
            (peer->inbound_request.backend_type == VEMB_V16_REGION_LOCAL_SHM &&
             reset_ub_rpc_ring_backing(&peer->inbound_request, 0) != 0)) {
            RETURN_IF(1, -1);
        }
    }
    return 0;
}

int vemb_v16_storage_ctx_create_from_manifest(vemb_v16_storage_ctx_t **out,
                                              uint32_t vector_dim,
                                              uint32_t vector_stride,
                                              uint32_t max_vectors,
                                              const vemb_v16_warm_regions_manifest_t *manifest) {
    RETURN_IF(!out || !manifest ||
              vector_dim == 0 || vector_stride == 0 ||
              vector_dim > VEMB_V16_MAX_DIM ||
              vector_stride != vector_dim * sizeof(float) ||
              max_vectors == 0 || manifest->region_count == 0 ||
              manifest->region_count > VEMB_V16_MAX_MANIFEST_REGIONS, -1);

    for (uint32_t i = 0; i < manifest->region_count; i++) {
        const vemb_v16_manifest_region_t *region = &manifest->regions[i];
        RETURN_IF(region->value_size != vector_stride ||
                  region->region_bytes < region->value_size, -1);
    }

    vemb_v16_storage_ctx_t *storage = zcalloc(sizeof(*storage));
    RETURN_IF(!storage, -1);
    if (pthread_mutex_init(&storage->topology_lock, NULL) != 0) {
        zfree(storage);
        return -1;
    }
    if (pthread_mutex_init(&storage->migration_outbox_lock, NULL) != 0) {
        pthread_mutex_destroy(&storage->topology_lock);
        zfree(storage);
        return -1;
    }
    storage->vector_dim = vector_dim;
    storage->vector_stride = vector_stride;
    storage->max_vectors = max_vectors;
    storage->local_owner_id = manifest->has_local_ub_node_id ?
        manifest->local_ub_node_id : 0;
    atomic_init(&storage->current_topology_epoch, 0);
    atomic_init(&storage->min_write_epoch, 0);
    atomic_init(&storage->migration_active_count, 0);
    atomic_init(&storage->migration_delta_request_id, 1);
    atomic_init(&storage->migration_source_gc_count, 0);
    atomic_init(&storage->migration_gc_safe_watermark, 0);
    atomic_init(&storage->migration_baseline_sent_count, 0);
    atomic_init(&storage->migration_baseline_skipped_count, 0);
    atomic_init(&storage->migration_baseline_error_count, 0);
    atomic_init(&storage->migration_baseline_retry_queued_count, 0);
    atomic_init(&storage->migration_baseline_retry_sent_count, 0);
    atomic_init(&storage->migration_retry_stop, 0);
    atomic_init(&storage->owner_resolver_active_snapshot, 0);
    storage->migration_retry_interval_us =
        VEMB_V16_STORAGE_MIGRATION_RETRY_INTERVAL_US;
    storage->migration_retry_batch_size =
        VEMB_V16_STORAGE_MIGRATION_RETRY_BATCH_SIZE;
    storage->warm_region_count = manifest->region_count;
    storage->local_region_weight = manifest->local_region_weight ?
        manifest->local_region_weight : 4;
    /* Cache local manifest regions so cross-node ATTACH resp can read
     * client_path without re-parsing the yaml. */
    storage->local_manifest_region_count = manifest->region_count;
    if (storage->local_manifest_region_count > VEMB_V16_MAX_MANIFEST_REGIONS)
        storage->local_manifest_region_count = VEMB_V16_MAX_MANIFEST_REGIONS;
    for (uint32_t i = 0; i < storage->local_manifest_region_count; i++)
        storage->local_manifest_regions[i] = manifest->regions[i];
    storage->warm_providers =
        zcalloc(sizeof(*storage->warm_providers) * VEMB_V16_MAX_MANIFEST_REGIONS);
    storage->warm_data_mappings =
        zcalloc(sizeof(*storage->warm_data_mappings) * VEMB_V16_MAX_MANIFEST_REGIONS);
    storage->warm_allocator_mappings =
        zcalloc(sizeof(*storage->warm_allocator_mappings) * VEMB_V16_MAX_MANIFEST_REGIONS);
    if (!storage->warm_providers || !storage->warm_data_mappings ||
        !storage->warm_allocator_mappings) {
        if (storage->warm_providers)
            zfree(storage->warm_providers);
        if (storage->warm_data_mappings)
            zfree(storage->warm_data_mappings);
        if (storage->warm_allocator_mappings)
            zfree(storage->warm_allocator_mappings);
        pthread_mutex_destroy(&storage->migration_outbox_lock);
        pthread_mutex_destroy(&storage->topology_lock);
        zfree(storage);
        return -1;
    }
    for (uint32_t i = 0; i < VEMB_V16_MAX_MANIFEST_REGIONS; i++) {
        storage->warm_providers[i].fd = -1;
        storage->warm_data_mappings[i].fd = -1;
        storage->warm_allocator_mappings[i].fd = -1;
    }

    vemb_v16_tlc_warm_region_t warm_regions[VEMB_V16_MAX_MANIFEST_REGIONS];
    memset(warm_regions, 0, sizeof(warm_regions));
    for (uint32_t i = 0; i < storage->warm_region_count; i++) {
        const vemb_v16_manifest_region_t *src = &manifest->regions[i];
        uint32_t capacity_slots =
            (uint32_t)(src->region_bytes / src->value_size);
        size_t allocator_layout_bytes =
            vemb_v16_warm_region_layout_bytes(capacity_slots);
        if (src->backend_type == VEMB_V16_REGION_UB) {
            /*
             * UB packs one warm-region layout into a single mapping:
             *   [warm_region_header][slot_meta][payload]
             */
            if (vemb_v16_mapped_region_open(&storage->warm_data_mappings[i],
                                            src->backend_type,
                                            src->cache_policy,
                                            src->path,
                                            src->mmap_offset,
                                            allocator_layout_bytes +
                                                (size_t)src->region_bytes) != 0) {
                serverLog(LL_WARNING,
                          "failed to open warm ub backing region: region_id=%u path=%s",
                          src->region_id, src->path);
                goto err;
            }
            if (vemb_v16_warm_provider_attach(&storage->warm_providers[i],
                                              &storage->warm_data_mappings[i],
                                              allocator_layout_bytes,
                                              src->region_id,
                                              src->backend_type,
                                              src->path,
                                              src->mmap_offset +
                                                  allocator_layout_bytes,
                                              src->value_size,
                                              src->region_bytes,
                                              src->home_ub_node_id,
                                              src->is_local,
                                              src->weight) != 0) {
                serverLog(LL_WARNING,
                          "failed to attach warm ub provider: region_id=%u path=%s",
                          src->region_id, src->path);
                goto err;
            }
        } else if (src->backend_type == VEMB_V16_REGION_LOCAL_SHM) {
            if (vemb_v16_mapped_region_open(&storage->warm_data_mappings[i],
                                            src->backend_type,
                                            VEMB_V16_UB_CACHE_POLICY_CACHEABLE,
                                            src->path,
                                            src->mmap_offset,
                                            (size_t)src->region_bytes) != 0) {
                serverLog(LL_WARNING,
                          "failed to open warm shm payload region: region_id=%u path=%s",
                          src->region_id, src->path);
                goto err;
            }
            if (vemb_v16_warm_provider_attach(&storage->warm_providers[i],
                                              &storage->warm_data_mappings[i],
                                              0,
                                              src->region_id,
                                              src->backend_type,
                                              src->path,
                                              src->mmap_offset,
                                              src->value_size,
                                              src->region_bytes,
                                              src->home_ub_node_id,
                                              src->is_local,
                                              src->weight) != 0) {
                serverLog(LL_WARNING,
                          "failed to attach warm shm provider: region_id=%u path=%s",
                          src->region_id, src->path);
                goto err;
            }

            char layout_name[VEMB_V16_WARM_REGION_LAYOUT_NAME_MAX];
            if (vemb_v16_warm_region_layout_name_from_region_path(
                    src->path,
                    src->region_id,
                    layout_name,
                    sizeof(layout_name)) != 0) {
                serverLog(LL_WARNING,
                          "failed to derive warm region layout name: region_id=%u path=%s",
                          src->region_id, src->path);
                goto err;
            }
            if (vemb_v16_mapped_region_open(&storage->warm_allocator_mappings[i],
                                            src->backend_type,
                                            VEMB_V16_UB_CACHE_POLICY_CACHEABLE,
                                            layout_name,
                                            0,
                                            allocator_layout_bytes) != 0) {
                serverLog(LL_WARNING,
                          "failed to open warm shm allocator backing: region_id=%u path=%s allocator=%s",
                          src->region_id, src->path, layout_name);
                 goto err;   
            }
            if (warm_region_layout_ensure_initialized(
                    &storage->warm_allocator_mappings[i],
                    src->region_id,
                    capacity_slots,
                    src->value_size,
                    src->region_bytes) != 0) {
                serverLog(LL_WARNING,
                          "failed to initialize warm shm layout: region_id=%u path=%s allocator=%s",
                          src->region_id, src->path, layout_name);
                goto err;
            }
        } else {
            // Should not happen due to manifest validation, but just in case.
            serverLog(LL_WARNING,
                      "unsupported warm region backend type: region_id=%u backend_type=%u",
                        src->region_id, src->backend_type);
        }
        warm_regions[i] = storage->warm_providers[i].region;
        warm_regions[i].slot_meta = src->backend_type == VEMB_V16_REGION_UB ?
            warm_slot_meta_from_mapping(&storage->warm_data_mappings[i],
                                        capacity_slots) :
            warm_slot_meta_from_mapping(&storage->warm_allocator_mappings[i],
                                        capacity_slots);
        if (!warm_regions[i].slot_meta) {
            serverLog(LL_WARNING,
                      "failed to resolve warm region slot meta: region_id=%u path=%s backend=%u",
                      src->region_id, src->path, src->backend_type);
            goto err;
        }
        if (!src->is_local &&
            storage_cache_peer_region_config(storage, src) != 0) {
            serverLog(LL_WARNING,
                      "failed to cache peer warm region config: region_id=%u owner=%u",
                      src->region_id,
                      src->home_ub_node_id);
            goto err;
        }
    }

    vemb_v16_warm_provider_t *first = &storage->warm_providers[0];
    storage->warm_region_id = first->region.region_id;
    storage->warm_backend_type = first->region.backend_type;
    storage->warm_mmap_offset = first->region.mmap_offset;
    storage->vector_region = first->region.mapped_addr;
    storage->vector_region_size = first->region.region_bytes;
    strncpy(storage->vector_region_name, first->path, sizeof(storage->vector_region_name) - 1);

    if (storage_remote_meta_init(storage, manifest) != 0) {
        goto err;
    }

    if (vemb_v16_tlc_create(&storage->tlc,
                            storage->vector_dim,
                            storage->max_vectors,
                            warm_regions,
                            storage->warm_region_count,
                            storage->local_region_weight) != 0) {
        serverLog(LL_WARNING,
                  "vemb_v16 tlc create failed: dim=%u max_vectors=%u regions=%u",
                  storage->vector_dim,
                  storage->max_vectors,
                  storage->warm_region_count);
        goto err;
    }
    vemb_v16_tlc_set_remote_meta_view(storage->tlc,
                                      &storage->remote_meta_view,
                                      VEMB_V16_REMOTE_META_DEFAULT_RETRIES);
    if (storage_remote_meta_owner_views_init(storage, manifest) != 0) {
        goto err;
    }
    for (uint32_t i = 0; i < manifest->remote_meta_view_count; i++) {
        if (storage_cache_remote_meta_view_config(storage,
                                                  &manifest->remote_meta_views[i]) != 0) {
            serverLog(LL_WARNING,
                      "failed to cache remote meta owner view config: owner=%u",
                      manifest->remote_meta_views[i].owner_id);
            goto err;
        }
    }
    if (storage_owner_resolver_init(storage, manifest) != 0) {
        goto err;
    }
    if (storage_ub_rpc_init(storage, manifest) != 0) {
        goto err;
    }
    if (storage->ub_rpc &&
        vemb_v16_storage_migration_retry_start(storage, 0, 0) != 0) {
        serverLog(LL_WARNING,
                  "failed to start vemb_v16 migration retry worker: local_owner=%u",
                  storage->local_owner_id);
    }

    *out = storage;
    return 0;

err:
    vemb_v16_storage_ctx_destroy(storage);
    return -1;
}

void vemb_v16_storage_ctx_destroy(vemb_v16_storage_ctx_t *storage) {
    assert(storage != NULL);
    vemb_v16_storage_migration_retry_stop(storage);
    vemb_v16_ub_rpc_destroy(storage->ub_rpc);
    storage->ub_rpc = NULL;
    for (uint32_t i = 0; i < storage->migration_outbox_count; i++) {
        vemb_v16_migration_outbox_destroy(storage->migration_outboxes[i]);
        storage->migration_outboxes[i] = NULL;
    }
    storage->migration_outbox_count = 0;
    vemb_v16_tlc_destroy(storage->tlc);
    if (storage->warm_providers) {
        for (uint32_t i = 0; i < storage->warm_region_count; i++)
            vemb_v16_warm_provider_close(&storage->warm_providers[i]);
        if (storage->warm_providers != &storage->warm_provider)
            zfree(storage->warm_providers);
    } else {
        vemb_v16_warm_provider_close(&storage->warm_provider);
    }
    if (storage->warm_data_mappings) {
        for (uint32_t i = 0; i < storage->warm_region_count; i++)
            vemb_v16_mapped_region_close(&storage->warm_data_mappings[i]);
        zfree(storage->warm_data_mappings);
    }
    if (storage->warm_allocator_mappings) {
        for (uint32_t i = 0; i < storage->warm_region_count; i++)
            vemb_v16_mapped_region_close(&storage->warm_allocator_mappings[i]);
        zfree(storage->warm_allocator_mappings);
    }
    for (uint32_t i = 0; i < storage->remote_meta_owner_view_count; i++) {
        vemb_v16_storage_remote_meta_view_t *view =
            &storage->remote_meta_owner_views[i];
        if (view->is_mapped) {
            vemb_v16_mapped_region_close(&view->mapping);
            view->base = NULL;
        } else if (view->base) {
            free(view->base);
            view->base = NULL;
        }
    }
    if (storage->remote_meta_is_mapped) {
        vemb_v16_mapped_region_close(&storage->remote_meta_mapping);
        storage->remote_meta_base = NULL;
    } else if (storage->remote_meta_base) {
        free(storage->remote_meta_base);
        storage->remote_meta_base = NULL;
    }
    pthread_mutex_destroy(&storage->migration_outbox_lock);
    pthread_mutex_destroy(&storage->topology_lock);
    zfree(storage);
}

const char *vemb_v16_storage_vector_region_name(vemb_v16_storage_ctx_t *storage) {
    assert(storage != NULL);
    return storage->vector_region_name;
}

size_t vemb_v16_storage_vector_region_size(vemb_v16_storage_ctx_t *storage) {
    assert(storage != NULL);
    return storage->vector_region_size;
}

sve_operation_stats_t *vemb_v16_storage_sve_stats(vemb_v16_storage_ctx_t *storage) {
    assert(storage != NULL);
    assert(storage->tlc != NULL);
    return &storage->tlc->sve_stats;
}

void vemb_v16_storage_fill_channel_desc(vemb_v16_storage_ctx_t *storage,
                                        vemb_v16_channel_desc_t *desc) {
    assert(storage != NULL);
    assert(desc != NULL);

    vemb_v16_warm_provider_t *provider = storage->warm_providers ?
        &storage->warm_providers[0] : &storage->warm_provider;
    desc->warm_region_id = provider->region.region_id;
    desc->warm_backend_type = provider->region.backend_type;
    desc->warm_region_bytes = provider->region.region_bytes;
    desc->warm_mmap_offset = provider->region.mmap_offset;
    desc->local_owner_id = storage->local_owner_id;
    desc->remote_meta_backend_type = storage->remote_meta_backend_type;
    desc->remote_meta_mmap_offset = storage->remote_meta_mmap_offset;
    desc->remote_meta_entry_count = storage->remote_meta_entry_count;
    desc->remote_meta_bucket_count = storage->remote_meta_bucket_count;
    desc->remote_meta_set_count = storage->remote_meta_set_count;
    desc->remote_meta_ways = storage->remote_meta_ways;
    desc->ub_rpc_timeout_ms = storage->ub_rpc_timeout_ms;
    strncpy(desc->vector_region_name, storage->vector_region_name,
            sizeof(desc->vector_region_name) - 1);
    strncpy(desc->remote_meta_path, storage->remote_meta_path,
            sizeof(desc->remote_meta_path) - 1);
    uint32_t count = storage->warm_region_count;
    if (count > VEMB_V16_MAX_DESC_WARM_REGIONS)
        count = VEMB_V16_MAX_DESC_WARM_REGIONS;
    desc->warm_region_count = count;
    for (uint32_t i = 0; i < count; i++) {
        vemb_v16_warm_provider_t *p = &storage->warm_providers[i];
        desc->warm_regions[i].region_id = p->region.region_id;
        desc->warm_regions[i].backend_type = p->region.backend_type;
        desc->warm_regions[i].region_bytes = p->region.region_bytes;
        desc->warm_regions[i].mmap_offset = p->region.mmap_offset;
        strncpy(desc->warm_regions[i].path, p->path,
                sizeof(desc->warm_regions[i].path) - 1);
    }
    uint32_t peer_count = storage->ub_rpc_peer_config_count;
    if (peer_count > VEMB_V16_MAX_DESC_UB_RPC_PEERS)
        peer_count = VEMB_V16_MAX_DESC_UB_RPC_PEERS;
    desc->ub_rpc_peer_count = peer_count;
    for (uint32_t i = 0; i < peer_count; i++) {
        const vemb_v16_manifest_ub_rpc_peer_t *src =
            &storage->ub_rpc_peer_configs[i];
        desc->ub_rpc_peers[i].owner_id = src->owner_id;
        desc->ub_rpc_peers[i].request_backend_type = src->request.backend_type;
        desc->ub_rpc_peers[i].request_mmap_offset = src->request.mmap_offset;
        strncpy(desc->ub_rpc_peers[i].request_path, src->request.path,
                sizeof(desc->ub_rpc_peers[i].request_path) - 1);
        desc->ub_rpc_peers[i].response_backend_type = src->response.backend_type;
        desc->ub_rpc_peers[i].response_mmap_offset = src->response.mmap_offset;
        strncpy(desc->ub_rpc_peers[i].response_path, src->response.path,
                sizeof(desc->ub_rpc_peers[i].response_path) - 1);
        desc->ub_rpc_peers[i].inbound_request_backend_type =
            src->inbound_request.backend_type;
        desc->ub_rpc_peers[i].inbound_request_mmap_offset =
            src->inbound_request.mmap_offset;
        strncpy(desc->ub_rpc_peers[i].inbound_request_path,
                src->inbound_request.path,
                sizeof(desc->ub_rpc_peers[i].inbound_request_path) - 1);
        desc->ub_rpc_peers[i].outbound_response_backend_type =
            src->outbound_response.backend_type;
        desc->ub_rpc_peers[i].outbound_response_mmap_offset =
            src->outbound_response.mmap_offset;
        strncpy(desc->ub_rpc_peers[i].outbound_response_path,
                src->outbound_response.path,
                sizeof(desc->ub_rpc_peers[i].outbound_response_path) - 1);
    }
}

int vemb_v16_storage_vector_slice(vemb_v16_storage_ctx_t *storage,
                                  vemb_v16_resp_t *resp,
                                  const uint8_t **vector,
                                  uint32_t *vector_bytes) {
    assert(storage != NULL);
    assert(resp != NULL);
    assert(vector != NULL);
    assert(vector_bytes != NULL);

    *vector = NULL;
    *vector_bytes = 0;
    if (resp->op != VEMB_V16_OP_VEMB_INLINE ||
        resp->status != VEMB_V16_STATUS_OK ||
        resp->vector_bytes == 0) {
        return 0;
    }
    vemb_v16_vector_handle_t handle = {
        .region_id = resp->region_id,
        .bytes = resp->vector_bytes,
        .local_slot = resp->local_slot,
        .offset = resp->vector_offset,
        .key_hash = resp->key_hash,
        .owner_generation = resp->owner_generation,
    };
    if (vemb_v16_tlc_vector_slice(storage->tlc, &handle, vector,
                                  vector_bytes) != 0) {
        resp->status = VEMB_V16_STATUS_ERR;
        resp->vector_bytes = 0;
        resp->vector_offset = 0;
        return 0;
    }
    return 0;
}

void vemb_v16_storage_epoch_get(
    const vemb_v16_storage_ctx_t *storage,
    uint64_t *current_topology_epoch,
    uint64_t *min_write_epoch) {
    if (current_topology_epoch) {
        *current_topology_epoch = storage ?
            atomic_load_explicit(&storage->current_topology_epoch,
                                 memory_order_acquire) : 0;
    }
    if (min_write_epoch) {
        *min_write_epoch = storage ?
            atomic_load_explicit(&storage->min_write_epoch,
                                 memory_order_acquire) : 0;
    }
}

int vemb_v16_storage_epoch_set(
    vemb_v16_storage_ctx_t *storage,
    uint64_t current_topology_epoch,
    uint64_t min_write_epoch) {
    RETURN_IF(!storage || min_write_epoch > current_topology_epoch, -1);
    atomic_store_explicit(&storage->current_topology_epoch,
                          current_topology_epoch,
                          memory_order_release);
    atomic_store_explicit(&storage->min_write_epoch,
                          min_write_epoch,
                          memory_order_release);
    return 0;
}

int vemb_v16_storage_write_epoch_is_stale(
    const vemb_v16_storage_ctx_t *storage,
    uint64_t request_topology_epoch) {
    if (!storage)
        return 1;
    if (!vemb_v16_storage_migration_active(storage))
        return 0;
    uint64_t min_write_epoch =
        atomic_load_explicit(&storage->min_write_epoch,
                             memory_order_acquire);
    return request_topology_epoch < min_write_epoch;
}

int vemb_v16_storage_migration_active(
    const vemb_v16_storage_ctx_t *storage) {
    if (!storage)
        return 0;
    return atomic_load_explicit(&storage->migration_active_count,
                                memory_order_acquire) != 0;
}

int vemb_v16_storage_ask_redirect_write_ready(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    tlc_core_key_migration_info_t *info) {
    RETURN_IF(!storage || !key ||
              key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN,
              -1);
    if (info)
        memset(info, 0, sizeof(*info));

    tlc_core_key_migration_info_t current = {
        .source_owner = UINT32_MAX,
        .target_owner = UINT32_MAX,
    };
    if (tlc_core_get_migration_info(storage->tlc->core,
                                        key,
                                        key_len,
                                        key_hash,
                                        &current) != 0 ||
        current.migration_state != TLC_CORE_KEY_DEST_COMMITTED ||
        current.source_owner == UINT32_MAX ||
        current.target_owner != storage->local_owner_id ||
        current.owner_epoch == 0 ||
        current.owner_epoch < current.topology_epoch) {
        if (info)
            *info = current;
        return -1;
    }

    uint64_t progress_epoch = 0;
    uint64_t applied_seq = 0;
    uint64_t barrier_seq = 0;
    if (!vemb_v16_tlc_migration_progress_ready(storage->tlc,
                                               current.source_owner,
                                               current.target_owner,
                                               current.shard_id,
                                               current.owner_epoch,
                                               &progress_epoch,
                                               &applied_seq,
                                               &barrier_seq) ||
        progress_epoch == 0 ||
        applied_seq < barrier_seq) {
        if (info)
            *info = current;
        return -1;
    }

    if (info)
        *info = current;
    return 0;
}

static int migration_info_requires_storage_slow_path(
        const tlc_core_key_migration_info_t *info) {
    return info &&
           (info->migration_state == TLC_CORE_KEY_MIGRATING ||
            info->migration_state == TLC_CORE_KEY_CUTOVER ||
            info->migration_state == TLC_CORE_KEY_SOURCE_GC);
}

static void storage_migration_active_inc(vemb_v16_storage_ctx_t *storage) {
    atomic_fetch_add_explicit(&storage->migration_active_count,
                              1,
                              memory_order_release);
}

static void storage_migration_active_dec(vemb_v16_storage_ctx_t *storage) {
    atomic_fetch_sub_explicit(&storage->migration_active_count,
                              1,
                              memory_order_release);
}

static void storage_update_gc_safe_watermark(
        vemb_v16_storage_ctx_t *storage,
        uint64_t topology_epoch) {
    uint_fast64_t prev =
        atomic_load_explicit(&storage->migration_gc_safe_watermark,
                             memory_order_relaxed);
    while (prev < topology_epoch &&
           !atomic_compare_exchange_weak_explicit(
               &storage->migration_gc_safe_watermark,
               &prev,
               topology_epoch,
               memory_order_relaxed,
               memory_order_relaxed)) {
    }
}

static int topology_owner_subset(const vemb_v16_topology_ring_t *subset,
                                 const vemb_v16_topology_ring_t *superset) {
    for (uint32_t i = 0; i < subset->owner_count; i++) {
        if (!vemb_v16_topology_owner_exists(superset, subset->owners[i]))
            return 0;
    }
    return 1;
}

int vemb_v16_storage_build_topology_rings(
    const vemb_v16_topology_control_req_t *req,
    vemb_v16_topology_ring_t *active_ring,
    vemb_v16_topology_ring_t *standby_ring) {
    uint32_t vnode_count;

    RETURN_IF(req->active_owner_count == 0 ||
              req->standby_owner_count == 0 ||
              req->active_owner_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS ||
              req->standby_owner_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS ||
              req->min_write_epoch > req->current_topology_epoch,
              -1);

    vnode_count = req->vnode_count ?
        req->vnode_count : VEMB_V16_TOPOLOGY_DEFAULT_VNODES;
    if (vemb_v16_topology_ring_build(active_ring,
                                     req->current_topology_epoch,
                                     req->active_owners,
                                     req->active_owner_count,
                                     vnode_count) != VEMB_V16_TOPOLOGY_OK ||
        vemb_v16_topology_ring_build(standby_ring,
                                     req->current_topology_epoch,
                                     req->standby_owners,
                                     req->standby_owner_count,
                                     vnode_count) != VEMB_V16_TOPOLOGY_OK ||
        !topology_owner_subset(active_ring, standby_ring)) {
        return -1;
    }
    return 0;
}

static int topology_endpoint_string_valid(const char *s, size_t n) {
    return s && memchr(s, '\0', n) != NULL;
}

static int topology_endpoint_valid(
        const vemb_v16_topology_endpoint_t *endpoint,
        const vemb_v16_topology_ring_t *standby_ring) {
    if (!endpoint ||
        endpoint->owner_id == UINT32_MAX ||
        (standby_ring &&
         !vemb_v16_topology_owner_exists(standby_ring,
                                         endpoint->owner_id))) {
        return 0;
    }
    if (endpoint->transport_type != VEMB_V16_TRANSPORT_TCP &&
        endpoint->transport_type != VEMB_V16_TRANSPORT_AERON)
        return 0;
    return endpoint->tcp_port != 0 &&
           topology_endpoint_string_valid(endpoint->host,
                                          sizeof(endpoint->host)) &&
           endpoint->host[0] != '\0';
}

static int topology_coordinator_endpoint_valid(
        const vemb_v16_topology_endpoint_t *endpoint) {
    if (!endpoint)
        return 0;
    if (endpoint->transport_type != VEMB_V16_TRANSPORT_TCP &&
        endpoint->transport_type != VEMB_V16_TRANSPORT_AERON)
        return 0;
    return endpoint->tcp_port != 0 &&
           topology_endpoint_string_valid(endpoint->host,
                                          sizeof(endpoint->host)) &&
           endpoint->host[0] != '\0';
}

static void topology_resp_copy_ring_owners(
        vemb_v16_topology_control_resp_t *resp,
        const vemb_v16_topology_ring_t *ring,
        int active) {
    uint32_t *owners = active ? resp->active_owners : resp->standby_owners;
    uint32_t *count = active ?
        &resp->active_owner_count : &resp->standby_owner_count;
    *count = ring->owner_count;
    memcpy(owners, ring->owners, sizeof(uint32_t) * ring->owner_count);
}

static void topology_resp_copy_default_ring(
        vemb_v16_storage_ctx_t *storage,
        vemb_v16_topology_control_resp_t *resp) {
    const vemb_v16_storage_owner_resolver_snapshot_t *snapshot =
        &storage->owner_resolver_snapshots[atomic_load_explicit(
            &storage->owner_resolver_active_snapshot,
            memory_order_acquire)];
    if (snapshot->owner_ring.owner_count > 0) {
        topology_resp_copy_ring_owners(resp, &snapshot->owner_ring, 1);
        topology_resp_copy_ring_owners(resp, &snapshot->owner_ring, 0);
        resp->vnode_count = snapshot->owner_ring.vnode_count;
        return;
    }

    resp->active_owner_count = 1;
    resp->standby_owner_count = 1;
    resp->active_owners[0] = storage->local_owner_id;
    resp->standby_owners[0] = storage->local_owner_id;
    resp->vnode_count = VEMB_V16_TOPOLOGY_DEFAULT_VNODES;
}

static int topology_owner_sets_equal(const vemb_v16_topology_ring_t *a,
                                     const vemb_v16_topology_ring_t *b) {
    if (!a || !b || a->owner_count != b->owner_count)
        return 0;
    for (uint32_t i = 0; i < a->owner_count; i++) {
        if (a->owners[i] != b->owners[i])
            return 0;
    }
    return 1;
}

static int migration_outboxes_all_cutover_ready(
        vemb_v16_storage_ctx_t *storage) {
    int ready = 1;
    pthread_mutex_lock(&storage->migration_outbox_lock);
    for (uint32_t i = 0; i < storage->migration_outbox_count; i++) {
        if (!vemb_v16_migration_outbox_cutover_ready(
                storage->migration_outboxes[i])) {
            ready = 0;
            break;
        }
    }
    pthread_mutex_unlock(&storage->migration_outbox_lock);
    return ready;
}

static int topology_publish_requires_lease_guard(
        vemb_v16_storage_ctx_t *storage,
        const vemb_v16_topology_ring_t *active_ring,
        const vemb_v16_topology_ring_t *standby_ring,
        uint32_t flags) {
    if (!storage ||
        (flags & VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED) ||
        !topology_owner_sets_equal(active_ring, standby_ring)) {
        return 0;
    }

    pthread_mutex_lock(&storage->topology_lock);
    int was_dual_write =
        storage->published_topology_valid &&
        (storage->published_topology_flags &
         VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED);
    pthread_mutex_unlock(&storage->topology_lock);
    return was_dual_write;
}

static int topology_publish_lease_guard_passed(
        vemb_v16_storage_ctx_t *storage) {
    return !tlc_core_has_uncommitted_source_migrations(storage->tlc->core) &&
           migration_outboxes_all_cutover_ready(storage);
}

static void scaleout_auto_set_phase(vemb_v16_storage_ctx_t *storage,
                                    uint32_t phase,
                                    uint32_t error) {
    pthread_mutex_lock(&storage->topology_lock);
    storage->scaleout_auto_phase = phase;
    storage->scaleout_auto_last_error = error;
    pthread_mutex_unlock(&storage->topology_lock);
}

static int scaleout_auto_range_ready(
        const vemb_v16_migration_range_control_resp_t *resp) {
    return resp &&
           resp->status == VEMB_V16_STATUS_OK &&
           resp->range_ready != 0;
}

static uint32_t scaleout_auto_shard_count(
        const vemb_v16_topology_ring_t *standby_ring) {
    uint32_t target_slots = standby_ring && standby_ring->owner_count ?
        standby_ring->owner_count : 1;
    uint32_t max_for_targets =
        VEMB_V16_STORAGE_MAX_SCALEOUT_RANGES / target_slots;
    if (max_for_targets == 0)
        max_for_targets = 1;
    return max_for_targets < VEMB_V16_STORAGE_SCALEOUT_AUTO_MAX_SHARDS ?
        max_for_targets : VEMB_V16_STORAGE_SCALEOUT_AUTO_MAX_SHARDS;
}

static uint32_t scaleout_auto_shard_id(uint64_t key_hash,
                                       uint32_t shard_count) {
    if (shard_count <= 1)
        return VEMB_V16_STORAGE_MIGRATION_DEFAULT_SHARD_ID;
    return (uint32_t)(key_hash % shard_count);
}

static int scaleout_auto_arm_for_topology(
        vemb_v16_storage_ctx_t *storage,
        const vemb_v16_topology_control_req_t *req,
        const vemb_v16_topology_ring_t *standby_ring) {
    if (!storage || !req || !standby_ring ||
        !(req->flags & VEMB_V16_TOPOLOGY_CONTROL_F_AUTO_SCALEOUT) ||
        !(req->flags & VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED)) {
        return 0;
    }
    if (req->current_topology_epoch == UINT64_MAX)
        return -1;

    pthread_mutex_lock(&storage->topology_lock);
    storage->scaleout_auto_enabled = 1;
    storage->scaleout_auto_phase = VEMB_V16_STORAGE_SCALEOUT_DRAINING;
    storage->scaleout_auto_last_error = 0;
    storage->scaleout_auto_coordinated =
        (req->flags &
         VEMB_V16_TOPOLOGY_CONTROL_F_COORDINATED_SCALEOUT) != 0;
    storage->scaleout_auto_migration_epoch = req->current_topology_epoch;
    storage->scaleout_auto_cutover_epoch = req->current_topology_epoch + 1u;
    storage->scaleout_auto_notify_seq = req->current_topology_epoch;
    storage->scaleout_auto_full_ring = *standby_ring;
    storage->scaleout_auto_endpoint_count = req->endpoint_count;
    if (req->endpoint_count > 0) {
        memcpy(storage->scaleout_auto_endpoints,
               req->endpoints,
               sizeof(req->endpoints[0]) * req->endpoint_count);
    }
    storage->scaleout_auto_coordinator_endpoint_valid =
        req->coordinator_endpoint_valid;
    if (req->coordinator_endpoint_valid) {
        storage->scaleout_auto_coordinator_endpoint =
            req->coordinator_endpoint;
    } else {
        memset(&storage->scaleout_auto_coordinator_endpoint,
               0,
               sizeof(storage->scaleout_auto_coordinator_endpoint));
    }
    pthread_mutex_unlock(&storage->topology_lock);

    serverLog(LL_NOTICE,
              "vemb_v16 scaleout auto armed: local_owner=%u migration_epoch=%llu cutover_epoch=%llu owners=%u coordinated=%u",
              storage->local_owner_id,
              (unsigned long long)req->current_topology_epoch,
              (unsigned long long)(req->current_topology_epoch + 1u),
              standby_ring->owner_count,
              (req->flags &
               VEMB_V16_TOPOLOGY_CONTROL_F_COORDINATED_SCALEOUT) != 0);
    return 0;
}

static int scaleout_auto_publish_full_active(
        vemb_v16_storage_ctx_t *storage,
        uint64_t migration_epoch,
        uint64_t cutover_epoch) {
    if (!topology_publish_lease_guard_passed(storage))
        return 0;

    pthread_mutex_lock(&storage->topology_lock);
    if (!storage->scaleout_auto_enabled ||
        storage->scaleout_auto_migration_epoch != migration_epoch ||
        storage->scaleout_auto_cutover_epoch != cutover_epoch) {
        pthread_mutex_unlock(&storage->topology_lock);
        return 0;
    }

    /* Defer owner-resolver publication until the full-active cutover point so
     * VSIM key2 lookups keep using the pre-cutover owner view during migration. */
    if (storage->owner_resolver_pending_valid &&
        storage_owner_snapshot_publish_pending(storage) != 0) {
        pthread_mutex_unlock(&storage->topology_lock);
        return 0;
    }

    storage->active_topology_ring = storage->scaleout_auto_full_ring;
    storage->standby_topology_ring = storage->scaleout_auto_full_ring;
    storage->published_topology_valid = 1;
    storage->published_topology_flags =
        VEMB_V16_TOPOLOGY_CONTROL_F_PUBLISHED;
    storage->published_endpoint_count =
        storage->scaleout_auto_endpoint_count;
    storage->published_coordinator_endpoint_valid = 0;
    memset(&storage->published_coordinator_endpoint,
           0,
           sizeof(storage->published_coordinator_endpoint));
    if (storage->scaleout_auto_endpoint_count > 0) {
        memcpy(storage->published_endpoints,
               storage->scaleout_auto_endpoints,
               sizeof(storage->scaleout_auto_endpoints[0]) *
                   storage->scaleout_auto_endpoint_count);
    }
    atomic_store_explicit(&storage->current_topology_epoch,
                          cutover_epoch,
                          memory_order_release);
    atomic_store_explicit(&storage->min_write_epoch,
                          cutover_epoch,
                          memory_order_release);
    storage->scaleout_auto_phase =
        VEMB_V16_STORAGE_SCALEOUT_SOURCE_GC;
    pthread_mutex_unlock(&storage->topology_lock);

    serverLog(LL_NOTICE,
              "vemb_v16 scaleout auto published full active topology: local_owner=%u migration_epoch=%llu cutover_epoch=%llu",
              storage->local_owner_id,
              (unsigned long long)migration_epoch,
              (unsigned long long)cutover_epoch);
    return 1;
}

static int scaleout_auto_full_active_published(
        vemb_v16_storage_ctx_t *storage,
        uint64_t migration_epoch,
        uint64_t cutover_epoch) {
    int published = 0;
    pthread_mutex_lock(&storage->topology_lock);
    uint64_t current_epoch =
        atomic_load_explicit(&storage->current_topology_epoch,
                             memory_order_acquire);
    published =
        storage->scaleout_auto_enabled &&
        storage->scaleout_auto_migration_epoch == migration_epoch &&
        storage->scaleout_auto_cutover_epoch == cutover_epoch &&
        storage->published_topology_valid &&
        current_epoch == cutover_epoch &&
        !(storage->published_topology_flags &
          VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED) &&
        topology_owner_sets_equal(&storage->active_topology_ring,
                                  &storage->standby_topology_ring) &&
        topology_owner_sets_equal(&storage->active_topology_ring,
                                  &storage->scaleout_auto_full_ring);
    pthread_mutex_unlock(&storage->topology_lock);
    return published;
}

static void scaleout_auto_enter_notify_pending(
        vemb_v16_storage_ctx_t *storage,
        uint64_t migration_epoch,
        uint64_t cutover_epoch) {
    int changed = 0;
    pthread_mutex_lock(&storage->topology_lock);
    if (storage->scaleout_auto_enabled &&
        storage->scaleout_auto_migration_epoch == migration_epoch &&
        storage->scaleout_auto_cutover_epoch == cutover_epoch &&
        storage->scaleout_auto_phase !=
            VEMB_V16_STORAGE_SCALEOUT_NOTIFY_PENDING &&
        storage->scaleout_auto_phase !=
            VEMB_V16_STORAGE_SCALEOUT_NOTIFIED &&
        storage->scaleout_auto_phase !=
            VEMB_V16_STORAGE_SCALEOUT_GLOBAL_CUTOVER_WAIT &&
        storage->scaleout_auto_phase !=
            VEMB_V16_STORAGE_SCALEOUT_SOURCE_GC &&
        storage->scaleout_auto_phase !=
            VEMB_V16_STORAGE_SCALEOUT_DONE) {
        storage->scaleout_auto_phase =
            VEMB_V16_STORAGE_SCALEOUT_NOTIFY_PENDING;
        storage->scaleout_auto_last_error = 0;
        changed = 1;
    }
    pthread_mutex_unlock(&storage->topology_lock);
    if (changed) {
        serverLog(LL_NOTICE,
                  "vemb_v16 scaleout auto local done: local_owner=%u migration_epoch=%llu cutover_epoch=%llu phase=notify_pending",
                  storage->local_owner_id,
                  (unsigned long long)migration_epoch,
                  (unsigned long long)cutover_epoch);
    }
}

static void storage_migration_snapshot_to_desc(
        const tlc_core_migration_snapshot_t *snapshot,
        vemb_v16_ub_migration_snapshot_desc_t *desc) {
    memset(desc, 0, sizeof(*desc));
    desc->key_hash = snapshot->key_hash;
    desc->key_version = snapshot->key_version;
    desc->topology_epoch = snapshot->topology_epoch;
    desc->owner_epoch = snapshot->owner_epoch;
    desc->key_len = snapshot->key_len;
    desc->migration_state = snapshot->migration_state;
    desc->source_owner = snapshot->source_owner;
    desc->target_owner = snapshot->target_owner;
    desc->tombstone = snapshot->tombstone;
    desc->value_size = snapshot->value_size;
    desc->shard_id = snapshot->shard_id;
    desc->region_id = snapshot->location.region_id;
    desc->local_slot = snapshot->location.local_slot;
    desc->bytes = snapshot->location.bytes;
    desc->offset = snapshot->location.offset;
    desc->owner_generation = snapshot->location.owner_generation;
    memcpy(desc->key, snapshot->key, snapshot->key_len);
}

static int migration_rpc_status_is_baseline_terminal(uint32_t status) {
    return status == VEMB_V16_UB_MIGRATION_RPC_OK ||
           status == VEMB_V16_UB_MIGRATION_RPC_DUPLICATE ||
           status == VEMB_V16_UB_MIGRATION_RPC_STALE_REJECTED;
}

static int storage_push_baseline_snapshot(
        vemb_v16_storage_ctx_t *storage,
        const char *key,
        uint32_t key_len,
        uint64_t key_hash,
        uint32_t target_owner) {
    RETURN_IF(!storage || !storage->ub_rpc || !key ||
              key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN ||
              target_owner == UINT32_MAX,
              -1);

    uint8_t snapshot_value[VEMB_V16_MAX_DIM * sizeof(float)];
    tlc_core_migration_snapshot_t snapshot = {0};
    if (tlc_core_snapshot(storage->tlc->core,
                              key,
                              key_len,
                              key_hash,
                              storage->local_owner_id,
                              target_owner,
                              &snapshot,
                              snapshot_value,
                              sizeof(snapshot_value)) != 0) {
        return -1;
    }

    vemb_v16_ub_migration_rpc_req_t req = {
        .request_id =
            atomic_fetch_add_explicit(&storage->migration_delta_request_id,
                                      1,
                                      memory_order_relaxed),
        .src_owner_id = storage->local_owner_id,
        .dst_owner_id = target_owner,
        .op = VEMB_V16_UB_MIGRATION_RPC_BASELINE_PUT,
        .key_hash = key_hash,
        .topology_epoch = snapshot.topology_epoch,
        .key_len = key_len,
        .target_owner_id = target_owner,
    };
    memcpy(req.key, key, key_len);
    storage_migration_snapshot_to_desc(&snapshot, &req.snapshot);

    vemb_v16_ub_migration_rpc_resp_t resp = {0};
    if (vemb_v16_ub_rpc_migrate_request(storage->ub_rpc, &req, &resp) != 0 ||
        !migration_rpc_status_is_baseline_terminal(resp.status)) {
        return -1;
    }
    return 0;
}

static int baseline_retry_entry_matches(
        const vemb_v16_storage_migration_baseline_retry_t *entry,
        const char *key,
        uint32_t key_len,
        uint64_t key_hash,
        uint64_t topology_epoch,
        uint32_t target_owner,
        uint32_t shard_id) {
    return entry &&
           entry->valid &&
           entry->key_hash == key_hash &&
           entry->topology_epoch == topology_epoch &&
           entry->target_owner == target_owner &&
           entry->shard_id == shard_id &&
           entry->key_len == key_len &&
           memcmp(entry->key, key, key_len) == 0;
}

static int baseline_retry_enqueue(
        vemb_v16_storage_ctx_t *storage,
        const char *key,
        uint32_t key_len,
        uint64_t key_hash,
        uint64_t topology_epoch,
        uint32_t target_owner,
        uint32_t shard_id) {
    RETURN_IF(!storage || !key ||
              key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN ||
              target_owner == UINT32_MAX,
              -1);

    pthread_mutex_lock(&storage->migration_outbox_lock);
    for (uint32_t i = 0;
         i < VEMB_V16_STORAGE_MAX_MIGRATION_BASELINE_RETRIES;
         i++) {
        vemb_v16_storage_migration_baseline_retry_t *entry =
            &storage->migration_baseline_retries[i];
        if (baseline_retry_entry_matches(entry,
                                         key,
                                         key_len,
                                         key_hash,
                                         topology_epoch,
                                         target_owner,
                                         shard_id)) {
            pthread_mutex_unlock(&storage->migration_outbox_lock);
            return 0;
        }
    }
    if (storage->migration_baseline_retry_count >=
        VEMB_V16_STORAGE_MAX_MIGRATION_BASELINE_RETRIES) {
        pthread_mutex_unlock(&storage->migration_outbox_lock);
        return -1;
    }

    for (uint32_t i = 0;
         i < VEMB_V16_STORAGE_MAX_MIGRATION_BASELINE_RETRIES;
         i++) {
        vemb_v16_storage_migration_baseline_retry_t *entry =
            &storage->migration_baseline_retries[i];
        if (entry->valid)
            continue;
        memset(entry, 0, sizeof(*entry));
        entry->valid = 1;
        entry->key_len = key_len;
        entry->target_owner = target_owner;
        entry->shard_id = shard_id;
        entry->key_hash = key_hash;
        entry->topology_epoch = topology_epoch;
        memcpy(entry->key, key, key_len);
        storage->migration_baseline_retry_count++;
        pthread_mutex_unlock(&storage->migration_outbox_lock);
        atomic_fetch_add_explicit(
            &storage->migration_baseline_retry_queued_count,
            1,
            memory_order_relaxed);
        return 0;
    }
    pthread_mutex_unlock(&storage->migration_outbox_lock);
    return -1;
}

static void baseline_retry_remove(
        vemb_v16_storage_ctx_t *storage,
        const vemb_v16_storage_migration_baseline_retry_t *wanted) {
    pthread_mutex_lock(&storage->migration_outbox_lock);
    for (uint32_t i = 0;
         i < VEMB_V16_STORAGE_MAX_MIGRATION_BASELINE_RETRIES;
         i++) {
        vemb_v16_storage_migration_baseline_retry_t *entry =
            &storage->migration_baseline_retries[i];
        if (!baseline_retry_entry_matches(entry,
                                          wanted->key,
                                          wanted->key_len,
                                          wanted->key_hash,
                                          wanted->topology_epoch,
                                          wanted->target_owner,
                                          wanted->shard_id)) {
            continue;
        }
        memset(entry, 0, sizeof(*entry));
        if (storage->migration_baseline_retry_count > 0)
            storage->migration_baseline_retry_count--;
        break;
    }
    pthread_mutex_unlock(&storage->migration_outbox_lock);
}

static int baseline_retry_is_still_needed(
        vemb_v16_storage_ctx_t *storage,
        const vemb_v16_storage_migration_baseline_retry_t *entry) {
    tlc_core_key_migration_info_t info = {0};
    return tlc_core_get_migration_info(storage->tlc->core,
                                           entry->key,
                                           entry->key_len,
                                           entry->key_hash,
                                           &info) == 0 &&
           info.migration_state == TLC_CORE_KEY_MIGRATING &&
           info.target_owner == entry->target_owner &&
           info.topology_epoch == entry->topology_epoch &&
           info.shard_id == entry->shard_id;
}

static int storage_auto_mark_migrating_for_topology(
        vemb_v16_storage_ctx_t *storage,
        const vemb_v16_topology_ring_t *active_ring,
        const vemb_v16_topology_ring_t *standby_ring,
        uint64_t topology_epoch,
        uint32_t flags) {
    if (!storage || !active_ring || !standby_ring ||
        !(flags & VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED) ||
        topology_owner_sets_equal(active_ring, standby_ring) ||
        !vemb_v16_topology_owner_exists(active_ring,
                                        storage->local_owner_id)) {
        return 0;
    }

    uint32_t cursor = 0;
    int done = 0;
    uint32_t marked = 0;
    uint32_t skipped = 0;
    uint32_t baseline_sent = 0;
    uint32_t baseline_skipped = 0;
    uint32_t baseline_errors = 0;
    uint32_t errors = 0;
    uint32_t shard_count =
        (flags & VEMB_V16_TOPOLOGY_CONTROL_F_AUTO_SCALEOUT) ?
            scaleout_auto_shard_count(standby_ring) : 1;
    while (!done) {
        tlc_core_migration_key_ref_t keys[VEMB_V16_MIGRATION_CONTROL_MAX_RANGE_KEYS];
        uint32_t key_count = 0;
        if (tlc_core_collect_source_active_keys(storage->tlc->core,
                &cursor,
                keys,
                VEMB_V16_MIGRATION_CONTROL_MAX_RANGE_KEYS,
                &key_count,
                &done) != 0) {
            serverLog(LL_WARNING,
                      "vemb_v16 migration auto plan collect failed: local_owner=%u epoch=%llu",
                      storage->local_owner_id,
                      (unsigned long long)topology_epoch);
            return -1;
        }
        for (uint32_t i = 0; i < key_count; i++) {
            uint32_t active_owner =
                vemb_v16_topology_ring_owner(active_ring, keys[i].key_hash);
            uint32_t standby_owner =
                vemb_v16_topology_ring_owner(standby_ring, keys[i].key_hash);
            if (active_owner != storage->local_owner_id ||
                standby_owner == UINT32_MAX ||
                standby_owner == storage->local_owner_id) {
                skipped++;
                continue;
            }
            uint32_t shard_id =
                scaleout_auto_shard_id(keys[i].key_hash, shard_count);
            if (vemb_v16_storage_migration_mark_migrating_in_shard(
                    storage,
                    keys[i].key,
                    keys[i].key_len,
                    keys[i].key_hash,
                    topology_epoch,
                    standby_owner,
                    shard_id,
                    NULL) == 0) {
                marked++;
                if (!storage->ub_rpc) {
                    baseline_skipped++;
                    atomic_fetch_add_explicit(
                        &storage->migration_baseline_skipped_count,
                        1,
                        memory_order_relaxed);
                } else if (storage_push_baseline_snapshot(
                               storage,
                               keys[i].key,
                               keys[i].key_len,
                               keys[i].key_hash,
                               standby_owner) == 0) {
                    baseline_sent++;
                    atomic_fetch_add_explicit(
                        &storage->migration_baseline_sent_count,
                        1,
                        memory_order_relaxed);
                } else {
                    baseline_errors++;
                    atomic_fetch_add_explicit(
                        &storage->migration_baseline_error_count,
                        1,
                        memory_order_relaxed);
                    if (baseline_retry_enqueue(
                            storage,
                            keys[i].key,
                            keys[i].key_len,
                            keys[i].key_hash,
                            topology_epoch,
                            standby_owner,
                            shard_id) != 0) {
                        serverLog(LL_WARNING,
                                  "vemb_v16 migration baseline retry enqueue failed: local_owner=%u target_owner=%u epoch=%llu key_hash=%llu",
                                  storage->local_owner_id,
                                  standby_owner,
                                  (unsigned long long)topology_epoch,
                                  (unsigned long long)keys[i].key_hash);
                    }
                }
            } else {
                errors++;
            }
        }
    }

    if (marked || skipped || errors ||
        baseline_sent || baseline_skipped || baseline_errors) {
        serverLog((errors || baseline_errors) ? LL_WARNING : LL_NOTICE,
                  "vemb_v16 migration auto plan: local_owner=%u epoch=%llu marked=%u skipped=%u baseline_sent=%u baseline_skipped=%u baseline_errors=%u errors=%u",
                  storage->local_owner_id,
                  (unsigned long long)topology_epoch,
                  marked,
                  skipped,
                  baseline_sent,
                  baseline_skipped,
                  baseline_errors,
                  errors);
    }
    return errors == 0 ? 0 : -1;
}

static int peer_view_region_desc_to_manifest(
        vemb_v16_manifest_region_t *dst,
        const vemb_v16_peer_view_region_desc_t *src,
        uint32_t local_owner_id) {
    RETURN_IF(!dst || !src || src->region_id == 0 || !src->path[0] ||
              src->value_size == 0 || src->region_bytes < src->value_size,
              -1);
    manifest_region_defaults(dst, src->value_size);
    dst->region_id = src->region_id;
    dst->backend_type = src->backend_type;
    dst->home_ub_node_id = src->home_ub_node_id;
    dst->is_local = src->home_ub_node_id == local_owner_id;
    dst->has_is_local = 1;
    dst->weight = src->weight ? src->weight : 1;
    dst->cache_policy = src->cache_policy ? src->cache_policy :
        VEMB_V16_UB_CACHE_POLICY_CACHEABLE;
    dst->has_cache_policy = 1;
    dst->mmap_offset = src->mmap_offset;
    dst->region_bytes = src->region_bytes;
    strncpy(dst->path, src->path, sizeof(dst->path) - 1);
    return 0;
}

static int peer_view_remote_meta_desc_to_manifest(
        vemb_v16_manifest_remote_meta_view_t *dst,
        const vemb_v16_peer_view_remote_meta_desc_t *src) {
    RETURN_IF(!dst || !src || !src->path[0], -1);
    manifest_remote_meta_view_defaults(dst);
    dst->owner_id = src->owner_id;
    dst->has_owner_id = 1;
    dst->backend_type = src->backend_type;
    dst->has_backend_type = 1;
    dst->entry_count = src->entry_count;
    dst->bucket_count = src->bucket_count;
    dst->set_count = src->set_count;
    dst->ways = src->ways;
    dst->cache_policy = src->cache_policy ? src->cache_policy :
        VEMB_V16_UB_CACHE_POLICY_CACHEABLE;
    dst->has_cache_policy = 1;
    dst->mmap_offset = src->mmap_offset;
    strncpy(dst->path, src->path, sizeof(dst->path) - 1);
    RETURN_IF(dst->entry_count == 0 &&
                  (dst->set_count == 0 || dst->ways == 0),
              -1);
    return 0;
}

static void peer_view_ring_desc_to_config(
        vemb_v16_ub_rpc_ring_config_t *dst,
        const vemb_v16_peer_view_ring_desc_t *src) {
    memset(dst, 0, sizeof(*dst));
    dst->backend_type = src->backend_type;
    dst->cache_policy = src->cache_policy ? src->cache_policy :
        VEMB_V16_UB_CACHE_POLICY_CACHEABLE;
    dst->mmap_offset = src->mmap_offset;
    strncpy(dst->path, src->path, sizeof(dst->path) - 1);
}

static int peer_view_ub_rpc_desc_to_manifest(
        vemb_v16_manifest_ub_rpc_peer_t *dst,
        const vemb_v16_peer_view_ub_rpc_peer_desc_t *src) {
    manifest_ub_rpc_peer_defaults(dst);
    dst->owner_id = src->owner_id;
    dst->has_owner_id = 1;
    peer_view_ring_desc_to_config(&dst->request, &src->request);
    peer_view_ring_desc_to_config(&dst->response, &src->response);
    peer_view_ring_desc_to_config(&dst->inbound_request,
                                  &src->inbound_request);
    peer_view_ring_desc_to_config(&dst->outbound_response,
                                  &src->outbound_response);
    RETURN_IF(!manifest_ub_rpc_peer_config_valid(dst), -1);
    return 0;
}

// Store peer-view mapping and optionally attach it into runtime state now.
int vemb_v16_storage_store_peer_view_map(
        vemb_v16_storage_ctx_t *storage,
        const vemb_v16_peer_view_map_req_t *req,
        vemb_v16_peer_view_map_resp_t *resp) {
    if (req->ub_rpc_timeout_ms)
        storage->ub_rpc_timeout_ms = req->ub_rpc_timeout_ms;

    for (uint32_t i = 0; i < req->region_count; i++) {
        vemb_v16_manifest_region_t region;
        RETURN_IF(!!peer_view_region_desc_to_manifest(&region,
                                                    &req->regions[i],
                                                    storage->local_owner_id),
                  -1);
        RETURN_IF(storage_cache_peer_region_config(storage, &region) != 0, -1);
        resp->applied_region_count++;
        if ((req->flags & VEMB_V16_PEER_VIEW_MAP_F_ATTACH_NOW) != 0) {
            RETURN_IF(!!storage_attach_warm_region_config(storage, &region), -1);
        }
    }

    for (uint32_t i = 0; i < req->remote_meta_view_count; i++) {
        vemb_v16_manifest_remote_meta_view_t view;
        RETURN_IF(peer_view_remote_meta_desc_to_manifest(
                    &view, &req->remote_meta_views[i]) != 0, -1);
        RETURN_IF(storage_cache_remote_meta_view_config(storage, &view) != 0, -1);
        resp->applied_remote_meta_view_count++;
        if ((req->flags & VEMB_V16_PEER_VIEW_MAP_F_ATTACH_NOW) != 0) {
            RETURN_IF(storage_attach_remote_meta_owner_view_config(
                        storage, &view) != 0, -1);
        }
    }

    for (uint32_t i = 0; i < req->ub_rpc_peer_count; i++) {
        vemb_v16_manifest_ub_rpc_peer_t peer;
        RETURN_IF(peer_view_ub_rpc_desc_to_manifest(&peer,
                                                    &req->ub_rpc_peers[i]) != 0,
                  -1);
        RETURN_IF(storage_cache_ub_rpc_peer_config(storage, &peer) != 0, -1);
        resp->applied_ub_rpc_peer_count++;
        if ((req->flags & VEMB_V16_PEER_VIEW_MAP_F_ATTACH_NOW) != 0) {
            RETURN_IF(storage_attach_ub_rpc_peer_config(storage,
                                                        &peer) != 0,
                      -1);
        }
    }

    resp->status = VEMB_V16_STATUS_OK;
    serverLog(LL_NOTICE,
              "vemb_v16 peer view map applied: local_owner=%u regions=%u remote_meta_views=%u ub_rpc_peers=%u attach_now=%u",
              storage->local_owner_id,
              resp->applied_region_count,
              resp->applied_remote_meta_view_count,
              resp->applied_ub_rpc_peer_count,
              (req->flags & VEMB_V16_PEER_VIEW_MAP_F_ATTACH_NOW) != 0);
    return 0;
}

int vemb_v16_storage_topology_set_with_rings(
    vemb_v16_storage_ctx_t *storage,
    const vemb_v16_topology_control_req_t *req,
    const vemb_v16_topology_ring_t *active_ring,
    const vemb_v16_topology_ring_t *standby_ring) {
    RETURN_IF(req->endpoint_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS, -1);
    for (uint32_t i = 0; i < req->endpoint_count; i++) {
        int valid = topology_endpoint_valid(&req->endpoints[i], standby_ring);
        RETURN_IF(!valid, -1);
    }
    if (req->coordinator_endpoint_valid &&
        !topology_coordinator_endpoint_valid(&req->coordinator_endpoint)) {
        return -1;
    }
    if (topology_publish_requires_lease_guard(storage,
                                             active_ring,
                                             standby_ring,
                                             req->flags) &&
        !topology_publish_lease_guard_passed(storage)) {
        return -1;
    }

    int owner_sets_changed = !topology_owner_sets_equal(active_ring,
                                                        standby_ring);

    pthread_mutex_lock(&storage->topology_lock);
    uint64_t current_topology_epoch =
        atomic_load_explicit(&storage->current_topology_epoch,
                             memory_order_acquire);
    uint64_t current_min_write_epoch =
        atomic_load_explicit(&storage->min_write_epoch, memory_order_acquire);
    if (req->current_topology_epoch < current_topology_epoch ||
        req->min_write_epoch < current_min_write_epoch) {
        serverLog(LL_WARNING,
                  "vemb_v16 topology rejected non-monotonic epoch: "
                  "local_owner=%u current_epoch=%llu request_epoch=%llu "
                  "current_min_write_epoch=%llu request_min_write_epoch=%llu "
                  "flags=0x%x",
                  storage->local_owner_id,
                  (unsigned long long)current_topology_epoch,
                  (unsigned long long)req->current_topology_epoch,
                  (unsigned long long)current_min_write_epoch,
                  (unsigned long long)req->min_write_epoch,
                  req->flags);
        pthread_mutex_unlock(&storage->topology_lock);
        return -1;
    }
    if (owner_sets_changed) {
        /* Stage the post-expand resolver snapshot now, but keep the current
         * snapshot live until full-active publish/cutover. */
        if (storage_owner_snapshot_stage_pending(storage, standby_ring) != 0) {
            pthread_mutex_unlock(&storage->topology_lock);
            return -1;
        }
    } else {
        /* Non-expand refreshes can publish immediately because there is no
         * migration window that needs the old owner view to stay visible. */
        if (storage_owner_snapshot_publish(storage, standby_ring) != 0) {
            pthread_mutex_unlock(&storage->topology_lock);
            return -1;
        }
        storage->owner_resolver_pending_valid = 0;
    }
    storage->active_topology_ring = *active_ring;
    storage->standby_topology_ring = *standby_ring;
    storage->published_topology_flags =
        req->flags | VEMB_V16_TOPOLOGY_CONTROL_F_PUBLISHED;
    storage->published_endpoint_count = req->endpoint_count;
    storage->published_coordinator_endpoint_valid =
        req->coordinator_endpoint_valid;
    // TODO: remove the check
    if (req->coordinator_endpoint_valid) {
        storage->published_coordinator_endpoint =
            req->coordinator_endpoint;
    } else {
        memset(&storage->published_coordinator_endpoint,
               0,
               sizeof(storage->published_coordinator_endpoint));
    }
    if (req->endpoint_count > 0) {
        memcpy(storage->published_endpoints,
               req->endpoints,
               sizeof(req->endpoints[0]) * req->endpoint_count);
    }
    storage->published_topology_valid = 1;
    atomic_store_explicit(&storage->current_topology_epoch,
                          req->current_topology_epoch,
                          memory_order_release);
    atomic_store_explicit(&storage->min_write_epoch,
                          req->min_write_epoch,
                          memory_order_release);
    pthread_mutex_unlock(&storage->topology_lock);
    int auto_mark_rc = storage_auto_mark_migrating_for_topology(
        storage,
        active_ring,
        standby_ring,
        req->current_topology_epoch,
        req->flags);
    if (auto_mark_rc == 0 &&
        owner_sets_changed &&
        vemb_v16_topology_owner_exists(active_ring,
                                       storage->local_owner_id) &&
        scaleout_auto_arm_for_topology(storage, req, standby_ring) != 0) {
        return -1;
    }
    return 0;
}

int vemb_v16_storage_topology_set(
    vemb_v16_storage_ctx_t *storage,
    const vemb_v16_topology_control_req_t *req) {
    vemb_v16_topology_ring_t active_ring;
    vemb_v16_topology_ring_t standby_ring;
    if (vemb_v16_storage_build_topology_rings(req,
                                              &active_ring,
                                              &standby_ring) != 0) {
        return -1;
    }
    return vemb_v16_storage_topology_set_with_rings(storage,
                                                    req,
                                                    &active_ring,
                                                    &standby_ring);
}

void vemb_v16_storage_topology_get(
    vemb_v16_storage_ctx_t *storage,
    vemb_v16_topology_control_resp_t *resp) {
    memset(resp, 0, sizeof(*resp));

    pthread_mutex_lock(&storage->topology_lock);
    resp->status = VEMB_V16_STATUS_OK;
    resp->current_topology_epoch =
        atomic_load_explicit(&storage->current_topology_epoch,
                             memory_order_acquire);
    resp->min_write_epoch =
        atomic_load_explicit(&storage->min_write_epoch,
                             memory_order_acquire);
    if (storage->published_topology_valid) {
        topology_resp_copy_ring_owners(resp,
                                       &storage->active_topology_ring,
                                       1);
        topology_resp_copy_ring_owners(resp,
                                       &storage->standby_topology_ring,
                                       0);
        resp->vnode_count = storage->standby_topology_ring.vnode_count;
        resp->flags = storage->published_topology_flags;
        resp->endpoint_count = storage->published_endpoint_count;
        resp->coordinator_endpoint_valid =
            storage->published_coordinator_endpoint_valid;
        if (storage->published_coordinator_endpoint_valid) {
            resp->coordinator_endpoint =
                storage->published_coordinator_endpoint;
        }
        if (storage->published_endpoint_count > 0) {
            memcpy(resp->endpoints,
                   storage->published_endpoints,
                   sizeof(resp->endpoints[0]) *
                       storage->published_endpoint_count);
        }
    } else {
        topology_resp_copy_default_ring(storage, resp);
    }
    pthread_mutex_unlock(&storage->topology_lock);
}

static int migration_outbox_matches(
        vemb_v16_migration_outbox_t *outbox,
        uint64_t topology_epoch,
        uint32_t source_owner,
        uint32_t target_owner,
        uint32_t shard_id) {
    vemb_v16_migration_outbox_stats_t stats;
    vemb_v16_migration_outbox_get_stats(outbox, &stats);
    return stats.topology_epoch == topology_epoch &&
           stats.source_owner == source_owner &&
           stats.target_owner == target_owner &&
           stats.shard_id == shard_id;
}

static vemb_v16_migration_outbox_t *migration_outbox_get_or_create_locked(
        vemb_v16_storage_ctx_t *storage,
        uint64_t topology_epoch,
        uint32_t target_owner,
        uint32_t shard_id) {
    uint32_t source_owner = storage->local_owner_id;
    for (uint32_t i = 0; i < storage->migration_outbox_count; i++) {
        vemb_v16_migration_outbox_t *outbox =
            storage->migration_outboxes[i];
        if (migration_outbox_matches(outbox,
                                     topology_epoch,
                                     source_owner,
                                     target_owner,
                                     shard_id)) {
            return outbox;
        }
    }
    if (storage->migration_outbox_count >=
        VEMB_V16_STORAGE_MAX_MIGRATION_OUTBOXES) {
        return NULL;
    }

    vemb_v16_migration_outbox_config_t config = {
        .source_owner = source_owner,
        .target_owner = target_owner,
        .shard_id = shard_id,
        .capacity = VEMB_V16_STORAGE_MIGRATION_OUTBOX_CAPACITY,
        .topology_epoch = topology_epoch,
    };
    vemb_v16_migration_outbox_t *outbox = NULL;
    if (vemb_v16_migration_outbox_create(&outbox, &config) !=
        VEMB_V16_MIGRATION_OUTBOX_OK) {
        return NULL;
    }
    storage->migration_outboxes[storage->migration_outbox_count++] = outbox;
    return outbox;
}

static vemb_v16_migration_outbox_t *migration_outbox_find_locked(
        vemb_v16_storage_ctx_t *storage,
        uint64_t topology_epoch,
        uint32_t target_owner,
        uint32_t shard_id) {
    uint32_t source_owner = storage->local_owner_id;
    for (uint32_t i = 0; i < storage->migration_outbox_count; i++) {
        vemb_v16_migration_outbox_t *outbox =
            storage->migration_outboxes[i];
        if (migration_outbox_matches(outbox,
                                     topology_epoch,
                                     source_owner,
                                     target_owner,
                                     shard_id)) {
            return outbox;
        }
    }
    return NULL;
}

static int migration_rpc_status_is_terminal_ack(uint32_t status) {
    return status == VEMB_V16_UB_MIGRATION_RPC_OK ||
           status == VEMB_V16_UB_MIGRATION_RPC_DUPLICATE ||
           status == VEMB_V16_UB_MIGRATION_RPC_STALE_REJECTED;
}

static int migration_send_delta(
    vemb_v16_storage_ctx_t *storage,
    const vemb_v16_ub_migration_delta_desc_t *delta,
    vemb_v16_ub_migration_delta_ack_desc_t *ack) {
    RETURN_IF(!storage || !storage->ub_rpc || !delta ||
              delta->key_len == 0 ||
              delta->key_len > VEMB_V16_MAX_KEY_LEN,
              -1);
    if (ack)
        memset(ack, 0, sizeof(*ack));

    vemb_v16_ub_migration_rpc_req_t req = {
        .request_id =
            atomic_fetch_add_explicit(&storage->migration_delta_request_id,
                                      1,
                                      memory_order_relaxed),
        .src_owner_id = storage->local_owner_id,
        .dst_owner_id = delta->target_owner,
        .op = delta->op,
        .key_hash = delta->key_hash,
        .topology_epoch = delta->topology_epoch,
        .key_len = delta->key_len,
        .target_owner_id = delta->target_owner,
        .delta = *delta,
    };
    memcpy(req.key, delta->key, delta->key_len);

    vemb_v16_ub_migration_rpc_resp_t resp = {0};
    if (vemb_v16_ub_rpc_migrate_request(storage->ub_rpc, &req, &resp) != 0 ||
        !migration_rpc_status_is_terminal_ack(resp.status)) {
        return -1;
    }
    if (ack)
        *ack = resp.delta_ack;
    return 0;
}

static int migration_target_barrier_ready(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    uint32_t target_owner,
    uint32_t shard_id,
    uint64_t barrier_seq,
    uint64_t *applied_seq) {
    RETURN_IF(!storage || target_owner == UINT32_MAX ||
              (key_len > 0 && (!key || key_len > VEMB_V16_MAX_KEY_LEN)),
              0);
    if (applied_seq)
        *applied_seq = 0;
    if (barrier_seq == 0 && key_len == 0)
        return 1;
    if (!storage->ub_rpc)
        return 0;

    vemb_v16_ub_migration_rpc_req_t req = {
        .request_id =
            atomic_fetch_add_explicit(&storage->migration_delta_request_id,
                                      1,
                                      memory_order_relaxed),
        .src_owner_id = storage->local_owner_id,
        .dst_owner_id = target_owner,
        .op = VEMB_V16_UB_MIGRATION_RPC_BARRIER_REQ,
        .key_hash = key_hash,
        .topology_epoch = topology_epoch,
        .key_len = key_len,
        .target_owner_id = target_owner,
        .barrier = {
            .topology_epoch = topology_epoch,
            .barrier_seq = barrier_seq,
            .source_owner = storage->local_owner_id,
            .target_owner = target_owner,
            .shard_id = shard_id,
        },
    };
    if (key_len > 0)
        memcpy(req.key, key, key_len);
    vemb_v16_ub_migration_rpc_resp_t resp = {0};
    if (vemb_v16_ub_rpc_migrate_request(storage->ub_rpc, &req, &resp) != 0)
        return 0;
    if (applied_seq)
        *applied_seq = resp.delta_ack.applied_seq;
    return resp.status == VEMB_V16_UB_MIGRATION_RPC_OK &&
           resp.delta_ack.applied_seq >= barrier_seq &&
           resp.delta_ack.barrier_seq >= barrier_seq;
}

static int migration_target_commit_lease(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    uint64_t owner_epoch,
    uint32_t target_owner,
    uint32_t shard_id) {
    RETURN_IF(!storage || !storage->ub_rpc || !key ||
              key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN ||
              target_owner == UINT32_MAX || owner_epoch == 0,
              0);

    vemb_v16_ub_migration_rpc_req_t req = {
        .request_id =
            atomic_fetch_add_explicit(&storage->migration_delta_request_id,
                                      1,
                                      memory_order_relaxed),
        .src_owner_id = storage->local_owner_id,
        .dst_owner_id = target_owner,
        .op = VEMB_V16_UB_MIGRATION_RPC_LEASE_COMMIT_REQ,
        .key_hash = key_hash,
        .topology_epoch = topology_epoch,
        .key_len = key_len,
        .target_owner_id = target_owner,
        .lease = {
            .topology_epoch = topology_epoch,
            .owner_epoch = owner_epoch,
            .source_owner = storage->local_owner_id,
            .target_owner = target_owner,
            .shard_id = shard_id,
        },
    };
    memcpy(req.key, key, key_len);

    vemb_v16_ub_migration_rpc_resp_t resp = {0};
    if (vemb_v16_ub_rpc_migrate_request(storage->ub_rpc, &req, &resp) != 0)
        return 0;
    return resp.status == VEMB_V16_UB_MIGRATION_RPC_OK &&
           resp.lease.owner_epoch >= owner_epoch &&
           resp.target_owner_id == target_owner;
}

static int migration_drain_one_outbox(
    vemb_v16_storage_ctx_t *storage,
    vemb_v16_migration_outbox_t *outbox,
    uint32_t max_delta,
    uint32_t *sent_count,
    uint32_t *acked_count) {
    RETURN_IF(!storage || !outbox, -1);
    uint32_t limit = max_delta == 0 ? 1 : max_delta;
    for (uint32_t i = 0; i < limit; i++) {
        vemb_v16_ub_migration_delta_desc_t delta = {0};
        pthread_mutex_lock(&storage->migration_outbox_lock);
        int peek_rc = vemb_v16_migration_outbox_peek(outbox, &delta);
        pthread_mutex_unlock(&storage->migration_outbox_lock);
        if (peek_rc == VEMB_V16_MIGRATION_OUTBOX_EMPTY)
            return 0;
        if (peek_rc != VEMB_V16_MIGRATION_OUTBOX_OK)
            return -1;

        vemb_v16_ub_migration_delta_ack_desc_t ack = {0};
        if (migration_send_delta(storage, &delta, &ack) != 0)
            return -1;
        if (sent_count)
            (*sent_count)++;

        pthread_mutex_lock(&storage->migration_outbox_lock);
        int ack_rc = vemb_v16_migration_outbox_ack(outbox,
                                                   ack.applied_seq);
        pthread_mutex_unlock(&storage->migration_outbox_lock);
        if (ack_rc != VEMB_V16_MIGRATION_OUTBOX_OK)
            return -1;
        if (acked_count)
            (*acked_count)++;
    }
    return 0;
}

int vemb_v16_storage_migration_delta_put_after_local_write(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    const vemb_v16_vector_handle_t *handle,
    vemb_v16_ub_migration_delta_ack_desc_t *ack) {
    RETURN_IF(!storage || !key || !handle ||
              key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN,
              -1);
    if (ack)
        memset(ack, 0, sizeof(*ack));
    if (!vemb_v16_storage_migration_active(storage))
        return 0;

    tlc_core_key_migration_info_t info = {0};
    if (tlc_core_get_migration_info(storage->tlc->core,
                                        key,
                                        key_len,
                                        key_hash,
                                        &info) != 0) {
        return 0;
    }
    if (info.migration_state != TLC_CORE_KEY_MIGRATING ||
        info.target_owner == UINT32_MAX ||
        info.target_owner == storage->local_owner_id) {
        return 0;
    }
    RETURN_IF(handle->bytes == 0, -1);

    vemb_v16_ub_migration_delta_desc_t delta = {
        .key_hash = key_hash,
        .key_version = info.key_version,
        .topology_epoch = info.topology_epoch,
        .owner_epoch = info.owner_epoch,
        .op = VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT,
        .key_len = key_len,
        .source_owner = storage->local_owner_id,
        .target_owner = info.target_owner,
        .value_size = handle->bytes,
        .region_id = handle->region_id,
        .local_slot = handle->local_slot,
        .bytes = handle->bytes,
        .offset = handle->offset,
        .owner_generation = handle->owner_generation,
    };
    memcpy(delta.key, key, key_len);

    vemb_v16_ub_migration_delta_desc_t assigned = {0};
    uint32_t shard_id = info.shard_id;
    pthread_mutex_lock(&storage->migration_outbox_lock);
    vemb_v16_migration_outbox_t *outbox =
        migration_outbox_get_or_create_locked(storage,
                                              info.topology_epoch,
                                              info.target_owner,
                                              shard_id);
    int append_rc = outbox ?
        vemb_v16_migration_outbox_append(outbox, &delta, &assigned) :
        VEMB_V16_MIGRATION_OUTBOX_NOMEM;
    pthread_mutex_unlock(&storage->migration_outbox_lock);
    RETURN_IF(append_rc != VEMB_V16_MIGRATION_OUTBOX_OK, -1);

    if (!storage->ub_rpc)
        return 0;

    vemb_v16_ub_migration_delta_ack_desc_t delta_ack = {0};
    if (migration_send_delta(storage, &assigned, &delta_ack) != 0)
        return 0;
    if (ack)
        *ack = delta_ack;

    pthread_mutex_lock(&storage->migration_outbox_lock);
    int ack_rc = vemb_v16_migration_outbox_ack(outbox,
                                               delta_ack.applied_seq);
    pthread_mutex_unlock(&storage->migration_outbox_lock);
    return ack_rc == VEMB_V16_MIGRATION_OUTBOX_OK ? 0 : -1;
}

static int migration_delta_delete_after_local_delete(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    const tlc_core_key_migration_info_t *info,
    vemb_v16_ub_migration_delta_ack_desc_t *ack) {
    RETURN_IF(!storage || !key || !info ||
              key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN,
              -1);
    if (ack)
        memset(ack, 0, sizeof(*ack));
    if (!vemb_v16_storage_migration_active(storage))
        return 0;
    if (info->migration_state != TLC_CORE_KEY_MIGRATING ||
        info->target_owner == UINT32_MAX ||
        info->target_owner == storage->local_owner_id) {
        return 0;
    }
    RETURN_IF(!info->tombstone, -1);

    vemb_v16_ub_migration_delta_desc_t delta = {
        .key_hash = key_hash,
        .key_version = info->key_version,
        .topology_epoch = info->topology_epoch,
        .owner_epoch = info->owner_epoch,
        .op = VEMB_V16_UB_MIGRATION_RPC_DELTA_DELETE,
        .key_len = key_len,
        .source_owner = storage->local_owner_id,
        .target_owner = info->target_owner,
        .tombstone = 1,
    };
    memcpy(delta.key, key, key_len);

    vemb_v16_ub_migration_delta_desc_t assigned = {0};
    uint32_t shard_id = info->shard_id;
    pthread_mutex_lock(&storage->migration_outbox_lock);
    vemb_v16_migration_outbox_t *outbox =
        migration_outbox_get_or_create_locked(storage,
                                              info->topology_epoch,
                                              info->target_owner,
                                              shard_id);
    int append_rc = outbox ?
        vemb_v16_migration_outbox_append(outbox, &delta, &assigned) :
        VEMB_V16_MIGRATION_OUTBOX_NOMEM;
    pthread_mutex_unlock(&storage->migration_outbox_lock);
    RETURN_IF(append_rc != VEMB_V16_MIGRATION_OUTBOX_OK, -1);

    if (!storage->ub_rpc)
        return 0;

    vemb_v16_ub_migration_delta_ack_desc_t delta_ack = {0};
    if (migration_send_delta(storage, &assigned, &delta_ack) != 0)
        return 0;
    if (ack)
        *ack = delta_ack;

    pthread_mutex_lock(&storage->migration_outbox_lock);
    int ack_rc = vemb_v16_migration_outbox_ack(outbox,
                                               delta_ack.applied_seq);
    pthread_mutex_unlock(&storage->migration_outbox_lock);
    return ack_rc == VEMB_V16_MIGRATION_OUTBOX_OK ? 0 : -1;
}

int vemb_v16_storage_delete_with_epoch(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    tlc_core_key_migration_info_t *info,
    vemb_v16_ub_migration_delta_ack_desc_t *ack) {
    RETURN_IF(!storage || !key ||
              key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN,
              -1);
    if (info)
        memset(info, 0, sizeof(*info));
    if (ack)
        memset(ack, 0, sizeof(*ack));
    if (vemb_v16_storage_write_epoch_is_stale(storage,
                                              topology_epoch) ||
        vemb_v16_storage_migration_write_blocked(storage,
                                                 key,
                                                 key_len,
                                                 key_hash)) {
        return -1;
    }

    tlc_core_key_migration_info_t deleted = {0};
    if (tlc_core_delete_with_epoch(storage->tlc->core,
                                       key,
                                       key_len,
                                       key_hash,
                                       topology_epoch,
                                       &deleted) != 0) {
        return -1;
    }
    if (info)
        *info = deleted;
    return migration_delta_delete_after_local_delete(storage,
                                                    key,
                                                    key_len,
                                                    key_hash,
                                                    &deleted,
                                                    ack);
}

int vemb_v16_storage_migration_drain_outboxes(
    vemb_v16_storage_ctx_t *storage,
    uint32_t max_delta,
    uint32_t *sent_count,
    uint32_t *acked_count) {
    RETURN_IF(!storage, -1);
    if (sent_count)
        *sent_count = 0;
    if (acked_count)
        *acked_count = 0;

    uint32_t limit = max_delta == 0 ? UINT32_MAX : max_delta;
    for (uint32_t drained = 0; drained < limit;) {
        vemb_v16_migration_outbox_t *outbox = NULL;
        pthread_mutex_lock(&storage->migration_outbox_lock);
        for (uint32_t i = 0; i < storage->migration_outbox_count; i++) {
            vemb_v16_ub_migration_delta_desc_t peeked = {0};
            if (vemb_v16_migration_outbox_peek(storage->migration_outboxes[i],
                                               &peeked) ==
                VEMB_V16_MIGRATION_OUTBOX_OK) {
                outbox = storage->migration_outboxes[i];
                break;
            }
        }
        pthread_mutex_unlock(&storage->migration_outbox_lock);
        if (!outbox)
            return 0;

        uint32_t sent_before = sent_count ? *sent_count : 0;
        uint32_t acked_before = acked_count ? *acked_count : 0;
        if (migration_drain_one_outbox(storage,
                                       outbox,
                                       1,
                                       sent_count,
                                       acked_count) != 0) {
            return -1;
        }
        uint32_t sent_after = sent_count ? *sent_count : sent_before + 1;
        uint32_t acked_after = acked_count ? *acked_count : acked_before + 1;
        if (sent_after == sent_before && acked_after == acked_before)
            return 0;
        drained++;
    }
    return 0;
}

int vemb_v16_storage_migration_drain_baselines(
    vemb_v16_storage_ctx_t *storage,
    uint32_t max_snapshot,
    uint32_t *sent_count,
    uint32_t *acked_count) {
    RETURN_IF(!storage, -1);
    if (sent_count)
        *sent_count = 0;
    if (acked_count)
        *acked_count = 0;

    uint32_t limit = max_snapshot == 0 ? UINT32_MAX : max_snapshot;
    for (uint32_t drained = 0; drained < limit;) {
        vemb_v16_storage_migration_baseline_retry_t entry;
        memset(&entry, 0, sizeof(entry));
        pthread_mutex_lock(&storage->migration_outbox_lock);
        for (uint32_t i = 0;
             i < VEMB_V16_STORAGE_MAX_MIGRATION_BASELINE_RETRIES;
             i++) {
            if (storage->migration_baseline_retries[i].valid) {
                entry = storage->migration_baseline_retries[i];
                break;
            }
        }
        pthread_mutex_unlock(&storage->migration_outbox_lock);
        if (!entry.valid)
            return 0;

        if (!baseline_retry_is_still_needed(storage, &entry)) {
            baseline_retry_remove(storage, &entry);
            continue;
        }

        if (storage_push_baseline_snapshot(storage,
                                           entry.key,
                                           entry.key_len,
                                           entry.key_hash,
                                           entry.target_owner) == 0) {
            baseline_retry_remove(storage, &entry);
            if (sent_count)
                (*sent_count)++;
            if (acked_count)
                (*acked_count)++;
            atomic_fetch_add_explicit(
                &storage->migration_baseline_sent_count,
                1,
                memory_order_relaxed);
            atomic_fetch_add_explicit(
                &storage->migration_baseline_retry_sent_count,
                1,
                memory_order_relaxed);
            drained++;
            continue;
        }

        atomic_fetch_add_explicit(&storage->migration_baseline_error_count,
                                  1,
                                  memory_order_relaxed);
        pthread_mutex_lock(&storage->migration_outbox_lock);
        for (uint32_t i = 0;
             i < VEMB_V16_STORAGE_MAX_MIGRATION_BASELINE_RETRIES;
             i++) {
            vemb_v16_storage_migration_baseline_retry_t *queued =
                &storage->migration_baseline_retries[i];
            if (!baseline_retry_entry_matches(queued,
                                              entry.key,
                                              entry.key_len,
                                              entry.key_hash,
                                              entry.topology_epoch,
                                              entry.target_owner,
                                              entry.shard_id)) {
                continue;
            }
            queued->attempts++;
            queued->last_status = VEMB_V16_UB_MIGRATION_RPC_ERROR;
            break;
        }
        pthread_mutex_unlock(&storage->migration_outbox_lock);
        return 0;
    }
    return 0;
}

static int migration_checkpoint_outbox(
    vemb_v16_storage_ctx_t *storage,
    uint64_t topology_epoch,
    uint32_t target_owner,
    uint32_t shard_id,
    vemb_v16_migration_outbox_stats_t *stats) {
    if (stats)
        memset(stats, 0, sizeof(*stats));

    pthread_mutex_lock(&storage->migration_outbox_lock);
    vemb_v16_migration_outbox_t *outbox =
        migration_outbox_get_or_create_locked(storage,
                                              topology_epoch,
                                              target_owner,
                                              shard_id);
    uint64_t checkpoint_seq = 0;
    int rc = outbox ?
        vemb_v16_migration_outbox_checkpoint(outbox, &checkpoint_seq) :
        VEMB_V16_MIGRATION_OUTBOX_NOMEM;
    pthread_mutex_unlock(&storage->migration_outbox_lock);
    if (rc != VEMB_V16_MIGRATION_OUTBOX_OK)
        return -1;

    (void)migration_drain_one_outbox(storage,
                                     outbox,
                                     VEMB_V16_STORAGE_MIGRATION_OUTBOX_CAPACITY,
                                     NULL,
                                     NULL);
    if (stats) {
        pthread_mutex_lock(&storage->migration_outbox_lock);
        vemb_v16_migration_outbox_get_stats(outbox, stats);
        pthread_mutex_unlock(&storage->migration_outbox_lock);
    }
    return 0;
}

static void migration_abort_final_fence(
    vemb_v16_storage_ctx_t *storage,
    uint64_t topology_epoch,
    uint32_t target_owner,
    uint32_t shard_id,
    vemb_v16_migration_outbox_stats_t *stats) {
    if (stats)
        memset(stats, 0, sizeof(*stats));

    pthread_mutex_lock(&storage->migration_outbox_lock);
    vemb_v16_migration_outbox_t *outbox =
        migration_outbox_find_locked(storage,
                                     topology_epoch,
                                     target_owner,
                                     shard_id);
    if (outbox) {
        (void)vemb_v16_migration_outbox_abort_final_fence(outbox);
        if (stats)
            vemb_v16_migration_outbox_get_stats(outbox, stats);
    }
    pthread_mutex_unlock(&storage->migration_outbox_lock);
}

static int migration_prepare_final_fence(
    vemb_v16_storage_ctx_t *storage,
    uint64_t topology_epoch,
    uint32_t target_owner,
    uint32_t shard_id,
    vemb_v16_migration_outbox_stats_t *stats) {
    if (stats)
        memset(stats, 0, sizeof(*stats));

    pthread_mutex_lock(&storage->migration_outbox_lock);
    vemb_v16_migration_outbox_t *outbox =
        migration_outbox_get_or_create_locked(storage,
                                              topology_epoch,
                                              target_owner,
                                              shard_id);
    uint64_t final_barrier_seq = 0;
    int rc = outbox ?
        vemb_v16_migration_outbox_begin_final_fence(outbox,
                                                    &final_barrier_seq) :
        VEMB_V16_MIGRATION_OUTBOX_NOMEM;
    pthread_mutex_unlock(&storage->migration_outbox_lock);
    if (rc != VEMB_V16_MIGRATION_OUTBOX_OK)
        return -1;

    int drain_rc = migration_drain_one_outbox(
        storage,
        outbox,
        VEMB_V16_STORAGE_MIGRATION_OUTBOX_CAPACITY,
        NULL,
        NULL);

    vemb_v16_migration_outbox_stats_t current;
    memset(&current, 0, sizeof(current));
    pthread_mutex_lock(&storage->migration_outbox_lock);
    int ready = vemb_v16_migration_outbox_cutover_ready(outbox);
    vemb_v16_migration_outbox_get_stats(outbox, &current);
    pthread_mutex_unlock(&storage->migration_outbox_lock);

    if (drain_rc != 0 || !ready) {
        migration_abort_final_fence(storage,
                                    topology_epoch,
                                    target_owner,
                                    shard_id,
                                    stats);
        return -1;
    }
    if (stats)
        *stats = current;
    return 0;
}

int vemb_v16_storage_scaleout_auto_step(
    vemb_v16_storage_ctx_t *storage) {
    RETURN_IF(!storage, -1);

    uint32_t enabled = 0;
    uint32_t phase = VEMB_V16_STORAGE_SCALEOUT_IDLE;
    uint32_t coordinated = 0;
    uint64_t migration_epoch = 0;
    uint64_t cutover_epoch = 0;
    pthread_mutex_lock(&storage->topology_lock);
    enabled = storage->scaleout_auto_enabled;
    phase = storage->scaleout_auto_phase;
    coordinated = storage->scaleout_auto_coordinated;
    migration_epoch = storage->scaleout_auto_migration_epoch;
    cutover_epoch = storage->scaleout_auto_cutover_epoch;
    pthread_mutex_unlock(&storage->topology_lock);

    uint32_t sent_count = 0;
    uint32_t acked_count = 0;
    tlc_core_migration_range_ref_t ranges[VEMB_V16_STORAGE_MAX_SCALEOUT_RANGES];
    uint32_t range_count = 0;

    if (!enabled || phase == VEMB_V16_STORAGE_SCALEOUT_DONE)
        return 0;
    if (phase == VEMB_V16_STORAGE_SCALEOUT_ERROR)
        return -1;
    if (phase == VEMB_V16_STORAGE_SCALEOUT_NOTIFY_PENDING)
        return 0;
    if (phase == VEMB_V16_STORAGE_SCALEOUT_NOTIFIED ||
        phase == VEMB_V16_STORAGE_SCALEOUT_GLOBAL_CUTOVER_WAIT) {
        if (!scaleout_auto_full_active_published(storage,
                                                 migration_epoch,
                                                 cutover_epoch)) {
            scaleout_auto_set_phase(
                storage,
                VEMB_V16_STORAGE_SCALEOUT_GLOBAL_CUTOVER_WAIT,
                0);
            return 0;
        }
        goto source_gc;
    }

    (void)vemb_v16_storage_migration_drain_baselines(storage,
                                                     0,
                                                     &sent_count,
                                                     &acked_count);
    (void)vemb_v16_storage_migration_drain_outboxes(storage,
                                                   0,
                                                   &sent_count,
                                                   &acked_count);

    if (tlc_core_collect_migration_ranges(storage->tlc->core,
            migration_epoch,
            TLC_CORE_KEY_MIGRATING,
            ranges,
            VEMB_V16_STORAGE_MAX_SCALEOUT_RANGES,
            &range_count) != 0) {
        scaleout_auto_set_phase(storage,
                                VEMB_V16_STORAGE_SCALEOUT_ERROR,
                                VEMB_V16_STATUS_ERR);
        return -1;
    }

    if (range_count > 0) {
        scaleout_auto_set_phase(storage,
                                VEMB_V16_STORAGE_SCALEOUT_DRAINING,
                                0);
        serverLog(LL_NOTICE,
                  "vemb_v16 scaleout auto draining: local_owner=%u migration_epoch=%llu ranges=%u sent=%u acked=%u",
                  storage->local_owner_id,
                  (unsigned long long)migration_epoch,
                  range_count,
                  sent_count,
                  acked_count);
        for (uint32_t i = 0; i < range_count; i++) {
            vemb_v16_migration_range_control_resp_t barrier_resp;
            memset(&barrier_resp, 0, sizeof(barrier_resp));
            if (vemb_v16_storage_migration_range_barrier(
                    storage,
                    migration_epoch,
                    ranges[i].target_owner,
                    ranges[i].shard_id,
                    0,
                    &barrier_resp) != 0 ||
                !scaleout_auto_range_ready(&barrier_resp)) {
                serverLog(LL_NOTICE,
                          "vemb_v16 scaleout auto barrier wait: local_owner=%u migration_epoch=%llu target_owner=%u shard_id=%u status=%u range_ready=%u pending_delta=%u remaining_keys=%u",
                          storage->local_owner_id,
                          (unsigned long long)migration_epoch,
                          ranges[i].target_owner,
                          ranges[i].shard_id,
                          barrier_resp.status,
                          barrier_resp.range_ready,
                          barrier_resp.pending_delta,
                          barrier_resp.remaining_keys);
                continue;
            }

            vemb_v16_migration_range_control_resp_t cutover_resp;
            memset(&cutover_resp, 0, sizeof(cutover_resp));
            if (vemb_v16_storage_migration_range_mark_cutover(
                    storage,
                    migration_epoch,
                    cutover_epoch,
                    ranges[i].target_owner,
                    ranges[i].shard_id,
                    0,
                    &cutover_resp) == 0) {
                scaleout_auto_set_phase(
                    storage,
                    VEMB_V16_STORAGE_SCALEOUT_CUTOVER_LOCAL,
                    0);
            }
        }
        return 0;
    }

    if (coordinated) {
        if (!topology_publish_lease_guard_passed(storage)) {
            serverLog(LL_NOTICE,
                      "vemb_v16 scaleout auto waiting lease guard: local_owner=%u migration_epoch=%llu cutover_epoch=%llu",
                      storage->local_owner_id,
                      (unsigned long long)migration_epoch,
                      (unsigned long long)cutover_epoch);
            return 0;
        }
        if (!scaleout_auto_full_active_published(storage,
                                                 migration_epoch,
                                                 cutover_epoch)) {
            scaleout_auto_enter_notify_pending(storage,
                                               migration_epoch,
                                               cutover_epoch);
            return 0;
        }
    } else {
        if (scaleout_auto_publish_full_active(storage,
                                              migration_epoch,
                                              cutover_epoch) == 0) {
            return 0;
        }
    }

source_gc:
    range_count = 0;
    if (tlc_core_collect_migration_ranges(storage->tlc->core,
            cutover_epoch,
            TLC_CORE_KEY_CUTOVER,
            ranges,
            VEMB_V16_STORAGE_MAX_SCALEOUT_RANGES,
            &range_count) != 0) {
        scaleout_auto_set_phase(storage,
                                VEMB_V16_STORAGE_SCALEOUT_ERROR,
                                VEMB_V16_STATUS_ERR);
        return -1;
    }

    if (range_count > 0) {
        scaleout_auto_set_phase(storage,
                                VEMB_V16_STORAGE_SCALEOUT_SOURCE_GC,
                                0);
        for (uint32_t i = 0; i < range_count; i++) {
            vemb_v16_migration_range_control_resp_t gc_resp;
            memset(&gc_resp, 0, sizeof(gc_resp));
            (void)vemb_v16_storage_migration_range_mark_source_gc(
                storage,
                migration_epoch,
                cutover_epoch,
                ranges[i].target_owner,
                ranges[i].shard_id,
                0,
                &gc_resp);
        }
    }

    range_count = 0;
    if (tlc_core_collect_migration_ranges(storage->tlc->core,
            cutover_epoch,
            TLC_CORE_KEY_CUTOVER,
            ranges,
            VEMB_V16_STORAGE_MAX_SCALEOUT_RANGES,
            &range_count) != 0) {
        scaleout_auto_set_phase(storage,
                                VEMB_V16_STORAGE_SCALEOUT_ERROR,
                                VEMB_V16_STATUS_ERR);
        return -1;
    }
    if (range_count == 0) {
        scaleout_auto_set_phase(storage,
                                VEMB_V16_STORAGE_SCALEOUT_DONE,
                                0);
        serverLog(LL_NOTICE,
                  "vemb_v16 scaleout auto done: local_owner=%u migration_epoch=%llu cutover_epoch=%llu",
                  storage->local_owner_id,
                  (unsigned long long)migration_epoch,
                  (unsigned long long)cutover_epoch);
    }
    return 0;
}

int vemb_v16_storage_scaleout_auto_get_status(
    vemb_v16_storage_ctx_t *storage,
    vemb_v16_storage_scaleout_auto_status_t *status) {
    memset(status, 0, sizeof(*status));

    pthread_mutex_lock(&storage->topology_lock);
    status->enabled = storage->scaleout_auto_enabled;
    status->phase = storage->scaleout_auto_phase;
    status->last_error = storage->scaleout_auto_last_error;
    status->coordinated = storage->scaleout_auto_coordinated;
    status->source_owner = storage->local_owner_id;
    status->migration_epoch = storage->scaleout_auto_migration_epoch;
    status->cutover_epoch = storage->scaleout_auto_cutover_epoch;
    status->notify_seq = storage->scaleout_auto_notify_seq;
    status->coordinator_endpoint_valid =
        storage->scaleout_auto_coordinator_endpoint_valid;
    if (storage->scaleout_auto_coordinator_endpoint_valid) {
        status->coordinator_endpoint =
            storage->scaleout_auto_coordinator_endpoint;
    }
    pthread_mutex_unlock(&storage->topology_lock);

    pthread_mutex_lock(&storage->migration_outbox_lock);
    status->baseline_retry_pending = storage->migration_baseline_retry_count;
    for (uint32_t i = 0; i < storage->migration_outbox_count; i++) {
        vemb_v16_migration_outbox_stats_t stats;
        memset(&stats, 0, sizeof(stats));
        vemb_v16_migration_outbox_get_stats(storage->migration_outboxes[i],
                                            &stats);
        status->pending_delta += stats.pending_count;
    }
    pthread_mutex_unlock(&storage->migration_outbox_lock);

    if (storage->tlc && status->migration_epoch != 0) {
        tlc_core_migration_range_ref_t ranges[
            VEMB_V16_STORAGE_MAX_SCALEOUT_RANGES];
        uint32_t range_count = 0;
        if (tlc_core_collect_migration_ranges(storage->tlc->core,
                status->migration_epoch,
                TLC_CORE_KEY_MIGRATING,
                ranges,
                VEMB_V16_STORAGE_MAX_SCALEOUT_RANGES,
                &range_count) == 0) {
            status->range_count = range_count;
            for (uint32_t i = 0; i < range_count; i++)
                status->migrating_key_count += ranges[i].key_count;
        }
    }
    return 0;
}

int vemb_v16_storage_scaleout_auto_mark_notified(
    vemb_v16_storage_ctx_t *storage,
    uint64_t migration_epoch,
    uint32_t source_owner,
    uint64_t notify_seq) {
    RETURN_IF(!storage || source_owner != storage->local_owner_id, -1);
    int changed = 0;
    pthread_mutex_lock(&storage->topology_lock);
    if (!storage->scaleout_auto_enabled ||
        !storage->scaleout_auto_coordinated ||
        storage->scaleout_auto_migration_epoch != migration_epoch ||
        storage->scaleout_auto_notify_seq != notify_seq) {
        pthread_mutex_unlock(&storage->topology_lock);
        return -1;
    }
    if (storage->scaleout_auto_phase ==
            VEMB_V16_STORAGE_SCALEOUT_NOTIFY_PENDING ||
        storage->scaleout_auto_phase ==
            VEMB_V16_STORAGE_SCALEOUT_LOCAL_DONE) {
        storage->scaleout_auto_phase =
            VEMB_V16_STORAGE_SCALEOUT_NOTIFIED;
        storage->scaleout_auto_last_error = 0;
        changed = 1;
    } else if (storage->scaleout_auto_phase !=
                   VEMB_V16_STORAGE_SCALEOUT_NOTIFIED &&
               storage->scaleout_auto_phase !=
                   VEMB_V16_STORAGE_SCALEOUT_GLOBAL_CUTOVER_WAIT &&
               storage->scaleout_auto_phase !=
                   VEMB_V16_STORAGE_SCALEOUT_SOURCE_GC &&
               storage->scaleout_auto_phase !=
                   VEMB_V16_STORAGE_SCALEOUT_DONE) {
        pthread_mutex_unlock(&storage->topology_lock);
        return -1;
    }
    pthread_mutex_unlock(&storage->topology_lock);
    if (changed) {
        serverLog(LL_NOTICE,
                  "vemb_v16 scaleout auto local done notified: local_owner=%u migration_epoch=%llu notify_seq=%llu",
                  source_owner,
                  (unsigned long long)migration_epoch,
                  (unsigned long long)notify_seq);
    }
    return 0;
}

static void migration_retry_sleep(uint32_t interval_us) {
    struct timespec ts = {
        .tv_sec = interval_us / 1000000u,
        .tv_nsec = (long)(interval_us % 1000000u) * 1000L,
    };
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
    }
}

static void *migration_retry_main(void *arg) {
    vemb_v16_storage_ctx_t *storage = arg;
    serverLog(LL_NOTICE,
              "vemb_v16 migration retry worker started: local_owner=%u interval_us=%u batch_size=%u",
              storage->local_owner_id,
              storage->migration_retry_interval_us,
              storage->migration_retry_batch_size);
    while (!atomic_load_explicit(&storage->migration_retry_stop,
                                 memory_order_acquire)) {
        uint32_t sent_count = 0;
        uint32_t acked_count = 0;
        uint32_t baseline_sent_count = 0;
        uint32_t baseline_acked_count = 0;
        int rc = vemb_v16_storage_migration_drain_outboxes(
            storage,
            storage->migration_retry_batch_size,
            &sent_count,
            &acked_count);
        int baseline_rc = vemb_v16_storage_migration_drain_baselines(
            storage,
            storage->migration_retry_batch_size,
            &baseline_sent_count,
            &baseline_acked_count);
        int scaleout_rc = vemb_v16_storage_scaleout_auto_step(storage);
        if (rc != 0 || baseline_rc != 0 || scaleout_rc != 0 ||
            (sent_count == 0 && baseline_sent_count == 0) ||
            (acked_count == 0 && baseline_acked_count == 0)) {
            migration_retry_sleep(storage->migration_retry_interval_us);
        }
    }
    serverLog(LL_NOTICE,
              "vemb_v16 migration retry worker stopped: local_owner=%u",
              storage->local_owner_id);
    return NULL;
}

int vemb_v16_storage_migration_retry_start(
    vemb_v16_storage_ctx_t *storage,
    uint32_t interval_us,
    uint32_t batch_size) {
    RETURN_IF(!storage || !storage->ub_rpc, -1);
    if (storage->migration_retry_thread_started)
        return 0;

    storage->migration_retry_interval_us = interval_us ? interval_us :
        VEMB_V16_STORAGE_MIGRATION_RETRY_INTERVAL_US;
    storage->migration_retry_batch_size = batch_size ? batch_size :
        VEMB_V16_STORAGE_MIGRATION_RETRY_BATCH_SIZE;
    atomic_store_explicit(&storage->migration_retry_stop,
                          0,
                          memory_order_release);
    if (pthread_create(&storage->migration_retry_thread,
                       NULL,
                       migration_retry_main,
                       storage) != 0) {
        return -1;
    }
    storage->migration_retry_thread_started = 1;
    return 0;
}

void vemb_v16_storage_migration_retry_stop(
    vemb_v16_storage_ctx_t *storage) {
    if (!storage || !storage->migration_retry_thread_started)
        return;
    atomic_store_explicit(&storage->migration_retry_stop,
                          1,
                          memory_order_release);
    pthread_join(storage->migration_retry_thread, NULL);
    storage->migration_retry_thread_started = 0;
}

int vemb_v16_storage_migration_write_blocked(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash) {
    return vemb_v16_storage_migration_write_blocked_info(storage,
                                                        key,
                                                        key_len,
                                                        key_hash,
                                                        NULL,
                                                        NULL);
}

int vemb_v16_storage_migration_write_blocked_info(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    tlc_core_key_migration_info_t *info,
    vemb_v16_migration_outbox_stats_t *outbox_stats) {
    RETURN_IF(!storage || !key ||
              key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN,
              1);
    if (info)
        memset(info, 0, sizeof(*info));
    if (outbox_stats)
        memset(outbox_stats, 0, sizeof(*outbox_stats));
    if (!vemb_v16_storage_migration_active(storage))
        return 0;
    tlc_core_key_migration_info_t current = {0};
    if (tlc_core_get_migration_info(storage->tlc->core,
                                        key,
                                        key_len,
                                        key_hash,
                                        &current) != 0 ||
        current.migration_state != TLC_CORE_KEY_MIGRATING ||
        current.target_owner == UINT32_MAX ||
        current.target_owner == storage->local_owner_id) {
        return 0;
    }

    uint32_t shard_id = current.shard_id;
    pthread_mutex_lock(&storage->migration_outbox_lock);
    vemb_v16_migration_outbox_t *outbox =
        migration_outbox_find_locked(storage,
                                     current.topology_epoch,
                                     current.target_owner,
                                     shard_id);
    vemb_v16_migration_outbox_stats_t stats;
    memset(&stats, 0, sizeof(stats));
    if (outbox)
        vemb_v16_migration_outbox_get_stats(outbox, &stats);
    pthread_mutex_unlock(&storage->migration_outbox_lock);
    int blocked = outbox &&
        stats.state != VEMB_V16_MIGRATION_OUTBOX_OPEN;
    if (blocked) {
        if (info)
            *info = current;
        if (outbox_stats)
            *outbox_stats = stats;
    }
    return blocked;
}

int vemb_v16_storage_migration_barrier_in_shard(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    uint32_t target_owner,
    uint32_t shard_id,
    tlc_core_key_migration_info_t *info,
    vemb_v16_migration_outbox_stats_t *outbox_stats) {
    RETURN_IF(!storage || !key ||
              key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN ||
              target_owner == UINT32_MAX,
              -1);
    if (info)
        memset(info, 0, sizeof(*info));
    if (outbox_stats)
        memset(outbox_stats, 0, sizeof(*outbox_stats));

    tlc_core_key_migration_info_t current = {0};
    if (tlc_core_get_migration_info(storage->tlc->core,
                                        key,
                                        key_len,
                                        key_hash,
                                        &current) != 0 ||
        current.migration_state != TLC_CORE_KEY_MIGRATING ||
        current.target_owner != target_owner ||
        current.topology_epoch != topology_epoch ||
        current.shard_id != shard_id) {
        return -1;
    }

    if (migration_checkpoint_outbox(storage,
                                    topology_epoch,
                                    target_owner,
                                    shard_id,
                                    outbox_stats) != 0) {
        return -1;
    }
    if (info)
        *info = current;
    return 0;
}

int vemb_v16_storage_migration_barrier(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    uint32_t target_owner,
    tlc_core_key_migration_info_t *info,
    vemb_v16_migration_outbox_stats_t *outbox_stats) {
    return vemb_v16_storage_migration_barrier_in_shard(
        storage,
        key,
        key_len,
        key_hash,
        topology_epoch,
        target_owner,
        VEMB_V16_STORAGE_MIGRATION_DEFAULT_SHARD_ID,
        info,
        outbox_stats);
}

int vemb_v16_storage_migration_mark_cutover_in_shard(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    uint32_t target_owner,
    uint32_t shard_id,
    tlc_core_key_migration_info_t *info) {
    RETURN_IF(!storage || !key, -1);
    tlc_core_key_migration_info_t current = {0};
    if (tlc_core_get_migration_info(storage->tlc->core,
                                        key,
                                        key_len,
        key_hash,
        &current) != 0 ||
        current.target_owner != target_owner ||
        current.topology_epoch > topology_epoch ||
        current.shard_id != shard_id) {
        return -1;
    }
    vemb_v16_migration_outbox_stats_t stats;
    memset(&stats, 0, sizeof(stats));
    if (migration_prepare_final_fence(storage,
                                      current.topology_epoch,
                                      target_owner,
                                      shard_id,
                                      &stats) != 0) {
        return -1;
    }
    if (!migration_target_barrier_ready(storage,
                                        key,
                                        key_len,
                                        key_hash,
                                        current.topology_epoch,
                                        target_owner,
                                        shard_id,
                                        stats.barrier_seq,
                                        NULL)) {
        migration_abort_final_fence(storage,
                                    current.topology_epoch,
                                    target_owner,
                                    shard_id,
                                    NULL);
        return -1;
    }
    if (!migration_target_commit_lease(storage,
                                       key,
                                       key_len,
                                       key_hash,
                                       topology_epoch,
                                       topology_epoch,
                                       target_owner,
                                       shard_id)) {
        migration_abort_final_fence(storage,
                                    current.topology_epoch,
                                    target_owner,
                                    shard_id,
                                    NULL);
        return -1;
    }

    return tlc_core_mark_cutover(storage->tlc->core,
                                     key,
                                     key_len,
                                     key_hash,
                                     topology_epoch,
                                     target_owner,
                                     info);
}

int vemb_v16_storage_migration_mark_cutover(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    uint32_t target_owner,
    tlc_core_key_migration_info_t *info) {
    return vemb_v16_storage_migration_mark_cutover_in_shard(
        storage,
        key,
        key_len,
        key_hash,
        topology_epoch,
        target_owner,
        VEMB_V16_STORAGE_MIGRATION_DEFAULT_SHARD_ID,
        info);
}

int vemb_v16_storage_migration_mark_source_gc_in_shard(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    uint32_t target_owner,
    uint32_t shard_id,
    tlc_core_key_migration_info_t *info) {
    RETURN_IF(!storage || !key ||
              key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN ||
              target_owner == UINT32_MAX,
              -1);
    if (info)
        memset(info, 0, sizeof(*info));

    tlc_core_key_migration_info_t current = {0};
    if (tlc_core_get_migration_info(storage->tlc->core,
                                        key,
                                        key_len,
                                        key_hash,
                                        &current) != 0 ||
        (current.migration_state != TLC_CORE_KEY_CUTOVER &&
         current.migration_state != TLC_CORE_KEY_SOURCE_GC) ||
        current.target_owner != target_owner ||
        current.topology_epoch > topology_epoch ||
        current.shard_id != shard_id) {
        return -1;
    }
    tlc_core_key_migration_info_t updated = {0};
    int rc = tlc_core_mark_source_gc(storage->tlc->core,
                                         key,
                                         key_len,
                                         key_hash,
                                         topology_epoch,
                                         target_owner,
                                         &updated);
    if (rc == 0) {
        atomic_fetch_add_explicit(&storage->migration_source_gc_count,
                                  current.migration_state ==
                                      TLC_CORE_KEY_SOURCE_GC ? 0 : 1,
                                  memory_order_relaxed);
        storage_update_gc_safe_watermark(storage, updated.topology_epoch);
        if (current.migration_state == TLC_CORE_KEY_CUTOVER)
            storage_migration_active_dec(storage);
        if (info)
            *info = updated;
    }
    return rc;
}

int vemb_v16_storage_migration_mark_source_gc(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    uint32_t target_owner,
    tlc_core_key_migration_info_t *info) {
    return vemb_v16_storage_migration_mark_source_gc_in_shard(
        storage,
        key,
        key_len,
        key_hash,
        topology_epoch,
        target_owner,
        VEMB_V16_STORAGE_MIGRATION_DEFAULT_SHARD_ID,
        info);
}

static void migration_range_fill_resp(
        vemb_v16_migration_range_control_resp_t *resp,
        uint8_t status,
        uint64_t migration_topology_epoch,
        uint64_t cutover_topology_epoch,
        uint32_t target_owner,
        uint32_t shard_id,
        const vemb_v16_migration_outbox_stats_t *stats,
        uint32_t key_count,
        uint32_t success_count,
        uint32_t error_count,
        uint32_t remaining_keys,
        uint32_t range_ready,
        uint32_t page_limit) {
    memset(resp, 0, sizeof(*resp));
    resp->status = status;
    resp->migration_topology_epoch = migration_topology_epoch;
    resp->cutover_topology_epoch = cutover_topology_epoch;
    resp->owner_epoch = cutover_topology_epoch;
    resp->target_owner = target_owner;
    resp->shard_id = shard_id;
    resp->key_count = key_count;
    resp->success_count = success_count;
    resp->error_count = error_count;
    resp->remaining_keys = remaining_keys;
    resp->page_key_count = key_count;
    resp->range_done = status == VEMB_V16_STATUS_OK &&
        remaining_keys == 0 && error_count == 0;
    resp->range_ready = range_ready;
    resp->page_limit = page_limit;
    if (!stats)
        return;
    resp->applied_seq = stats->acked_seq;
    resp->barrier_seq = stats->barrier_seq;
    resp->source_seq = stats->next_delta_seq == 0 ?
        0 : stats->next_delta_seq - 1;
    resp->retry_delta = stats->barrier_seq > stats->acked_seq ?
        stats->barrier_seq - stats->acked_seq : 0;
    resp->pending_delta = stats->pending_count;
    resp->outbox_state = stats->state;
}

static uint32_t migration_range_page_limit(uint32_t requested) {
    if (requested == 0 ||
        requested > VEMB_V16_STORAGE_MIGRATION_RANGE_PAGE_LIMIT) {
        return VEMB_V16_STORAGE_MIGRATION_RANGE_PAGE_LIMIT;
    }
    return requested;
}

int vemb_v16_storage_migration_range_barrier(
    vemb_v16_storage_ctx_t *storage,
    uint64_t migration_topology_epoch,
    uint32_t target_owner,
    uint32_t shard_id,
    uint32_t page_limit,
    vemb_v16_migration_range_control_resp_t *resp) {
    RETURN_IF(!storage || target_owner == UINT32_MAX, -1);
    uint32_t limit = migration_range_page_limit(page_limit);
    uint32_t key_count = 0;
    if (tlc_core_count_migration_keys(storage->tlc->core,
            migration_topology_epoch,
            target_owner,
            shard_id,
            TLC_CORE_KEY_MIGRATING,
            &key_count) != 0 ||
        key_count == 0) {
        migration_range_fill_resp(resp,
                                  VEMB_V16_STATUS_ERR,
                                  migration_topology_epoch,
                                  0,
                                  target_owner,
                                  shard_id,
                                  NULL,
                                  key_count,
                                  0,
                                  key_count == 0 ? 0 : key_count,
                                  key_count,
                                  0,
                                  limit);
        return -1;
    }

    vemb_v16_migration_outbox_stats_t stats;
    memset(&stats, 0, sizeof(stats));
    if (migration_checkpoint_outbox(storage,
                                    migration_topology_epoch,
                                    target_owner,
                                    shard_id,
                                    &stats) != 0) {
        migration_range_fill_resp(resp,
                                  VEMB_V16_STATUS_ERR,
                                  migration_topology_epoch,
                                  0,
                                  target_owner,
                                  shard_id,
                                  NULL,
                                  key_count,
                                  0,
                                  key_count,
                                  key_count,
                                  0,
                                  limit);
        return -1;
    }

    uint32_t range_ready = stats.acked_seq >= stats.barrier_seq;
    migration_range_fill_resp(resp,
                              VEMB_V16_STATUS_OK,
                              migration_topology_epoch,
                              0,
                              target_owner,
                              shard_id,
                              &stats,
                              key_count,
                              key_count,
                              0,
                              0,
                              range_ready,
                              limit);
    return 0;
}

int vemb_v16_storage_migration_range_mark_cutover(
    vemb_v16_storage_ctx_t *storage,
    uint64_t migration_topology_epoch,
    uint64_t cutover_topology_epoch,
    uint32_t target_owner,
    uint32_t shard_id,
    uint32_t page_limit,
    vemb_v16_migration_range_control_resp_t *resp) {
    RETURN_IF(!storage || target_owner == UINT32_MAX ||
              cutover_topology_epoch < migration_topology_epoch,
              -1);
    tlc_core_migration_key_ref_t keys[VEMB_V16_MIGRATION_CONTROL_MAX_RANGE_KEYS];
    uint32_t limit = migration_range_page_limit(page_limit);
    uint32_t key_count = 0;
    uint32_t remaining_before = 0;
    if (tlc_core_collect_migration_keys_page(storage->tlc->core,
            migration_topology_epoch,
            target_owner,
            shard_id,
            TLC_CORE_KEY_MIGRATING,
            keys,
            limit,
            &key_count,
            &remaining_before) != 0) {
        migration_range_fill_resp(resp,
                                  VEMB_V16_STATUS_ERR,
                                  migration_topology_epoch,
                                  cutover_topology_epoch,
                                  target_owner,
                                  shard_id,
                                  NULL,
                                  key_count,
                                  0,
                                  key_count,
                                  key_count + remaining_before,
                                  0,
                                  limit);
        return -1;
    }

    if (key_count == 0) {
        vemb_v16_migration_outbox_stats_t stats;
        memset(&stats, 0, sizeof(stats));
        pthread_mutex_lock(&storage->migration_outbox_lock);
        vemb_v16_migration_outbox_t *outbox =
            migration_outbox_find_locked(storage,
                                         migration_topology_epoch,
                                         target_owner,
                                         shard_id);
        if (outbox)
            vemb_v16_migration_outbox_get_stats(outbox, &stats);
        pthread_mutex_unlock(&storage->migration_outbox_lock);
        migration_range_fill_resp(resp,
                                  VEMB_V16_STATUS_OK,
                                  migration_topology_epoch,
                                  cutover_topology_epoch,
                                  target_owner,
                                  shard_id,
                                  outbox ? &stats : NULL,
                                  0,
                                  0,
                                  0,
                                  0,
                                  1,
                                  limit);
        return 0;
    }

    vemb_v16_migration_outbox_stats_t stats;
    memset(&stats, 0, sizeof(stats));
    if (migration_prepare_final_fence(storage,
                                      migration_topology_epoch,
                                      target_owner,
                                      shard_id,
                                      &stats) != 0) {
        uint32_t remaining_keys = key_count + remaining_before;
        migration_range_fill_resp(resp,
                                  VEMB_V16_STATUS_ERR,
                                  migration_topology_epoch,
                                  cutover_topology_epoch,
                                  target_owner,
                                  shard_id,
                                  &stats,
                                  key_count,
                                  0,
                                  key_count,
                                  remaining_keys,
                                  0,
                                  limit);
        return -1;
    }

    uint32_t success_count = 0;
    uint32_t error_count = 0;
    for (uint32_t i = 0; i < key_count; i++) {
        tlc_core_key_migration_info_t current = {0};
        int info_rc = tlc_core_get_migration_info(storage->tlc->core,
                                                  keys[i].key,
                                                  keys[i].key_len,
                                                  keys[i].key_hash,
                                                  &current);
        int target_ready = info_rc == 0 &&
            migration_target_barrier_ready(storage,
                                           keys[i].key,
                                           keys[i].key_len,
                                           keys[i].key_hash,
                                           migration_topology_epoch,
                                           target_owner,
                                           shard_id,
                                           stats.barrier_seq,
                                           NULL);
        if (info_rc != 0 ||
            current.migration_state != TLC_CORE_KEY_MIGRATING ||
            current.target_owner != target_owner ||
            current.topology_epoch != migration_topology_epoch ||
            current.shard_id != shard_id ||
            !target_ready) {
            if (error_count < 4) {
                serverLog(LL_NOTICE,
                          "vemb_v16 range cutover precheck failed: local_owner=%u migration_epoch=%llu target_owner=%u shard_id=%u key_hash=%llu info_rc=%d state=%u current_target=%u current_epoch=%llu current_shard=%u barrier_seq=%llu target_ready=%d",
                          storage->local_owner_id,
                          (unsigned long long)migration_topology_epoch,
                          target_owner,
                          shard_id,
                          (unsigned long long)keys[i].key_hash,
                          info_rc,
                          current.migration_state,
                          current.target_owner,
                          (unsigned long long)current.topology_epoch,
                          current.shard_id,
                          (unsigned long long)stats.barrier_seq,
                          target_ready);
            }
            error_count++;
        }
    }
    if (error_count != 0) {
        uint32_t remaining_keys = key_count + remaining_before;
        migration_abort_final_fence(storage,
                                    migration_topology_epoch,
                                    target_owner,
                                    shard_id,
                                    &stats);
        migration_range_fill_resp(resp,
                                  VEMB_V16_STATUS_ERR,
                                  migration_topology_epoch,
                                  cutover_topology_epoch,
                                  target_owner,
                                  shard_id,
                                  &stats,
                                  key_count,
                                  0,
                                  error_count,
                                  remaining_keys,
                                  0,
                                  limit);
        return -1;
    }

    for (uint32_t i = 0; i < key_count; i++) {
        tlc_core_key_migration_info_t info = {0};
        int lease_ok = migration_target_commit_lease(storage,
                                                     keys[i].key,
                                                     keys[i].key_len,
                                                     keys[i].key_hash,
                                                     cutover_topology_epoch,
                                                     cutover_topology_epoch,
                                                     target_owner,
                                                     shard_id);
        int mark_ok = lease_ok &&
            tlc_core_mark_cutover(storage->tlc->core,
                                  keys[i].key,
                                  keys[i].key_len,
                                  keys[i].key_hash,
                                  cutover_topology_epoch,
                                  target_owner,
                                  &info) == 0;
        if (!mark_ok) {
            if (error_count < 4) {
                serverLog(LL_NOTICE,
                          "vemb_v16 range cutover apply failed: local_owner=%u migration_epoch=%llu cutover_epoch=%llu target_owner=%u shard_id=%u key_hash=%llu lease_ok=%d",
                          storage->local_owner_id,
                          (unsigned long long)migration_topology_epoch,
                          (unsigned long long)cutover_topology_epoch,
                          target_owner,
                          shard_id,
                          (unsigned long long)keys[i].key_hash,
                          lease_ok);
            }
            error_count++;
            break;
        }
        success_count++;
    }

    uint32_t remaining_after = 0;
    if (tlc_core_count_migration_keys(storage->tlc->core,
            migration_topology_epoch,
            target_owner,
            shard_id,
            TLC_CORE_KEY_MIGRATING,
            &remaining_after) != 0) {
        error_count += remaining_before + 1;
        remaining_after = remaining_before;
    }

    memset(&stats, 0, sizeof(stats));
    pthread_mutex_lock(&storage->migration_outbox_lock);
    vemb_v16_migration_outbox_t *outbox =
        migration_outbox_find_locked(storage,
                                     migration_topology_epoch,
                                     target_owner,
                                     shard_id);
    if (outbox)
        vemb_v16_migration_outbox_get_stats(outbox, &stats);
    pthread_mutex_unlock(&storage->migration_outbox_lock);
    migration_range_fill_resp(resp,
                              error_count == 0 ? VEMB_V16_STATUS_OK :
                                  VEMB_V16_STATUS_ERR,
                              migration_topology_epoch,
                              cutover_topology_epoch,
                              target_owner,
                              shard_id,
                              outbox ? &stats : NULL,
                              key_count,
                              success_count,
                              error_count,
                              remaining_after,
                              1,
                              limit);
    return error_count == 0 ? 0 : -1;
}

int vemb_v16_storage_migration_range_mark_source_gc(
    vemb_v16_storage_ctx_t *storage,
    uint64_t migration_topology_epoch,
    uint64_t cutover_topology_epoch,
    uint32_t target_owner,
    uint32_t shard_id,
    uint32_t page_limit,
    vemb_v16_migration_range_control_resp_t *resp) {
    RETURN_IF(!storage || target_owner == UINT32_MAX ||
              cutover_topology_epoch < migration_topology_epoch,
              -1);
    tlc_core_migration_key_ref_t keys[VEMB_V16_MIGRATION_CONTROL_MAX_RANGE_KEYS];
    uint32_t limit = migration_range_page_limit(page_limit);
    uint32_t key_count = 0;
    uint32_t remaining_before = 0;
    if (tlc_core_collect_migration_keys_page(storage->tlc->core,
            cutover_topology_epoch,
            target_owner,
            shard_id,
            TLC_CORE_KEY_CUTOVER,
            keys,
            limit,
            &key_count,
            &remaining_before) != 0) {
        migration_range_fill_resp(resp,
                                  VEMB_V16_STATUS_ERR,
                                  migration_topology_epoch,
                                  cutover_topology_epoch,
                                  target_owner,
                                  shard_id,
                                  NULL,
                                  key_count,
                                  0,
                                  key_count,
                                  key_count + remaining_before,
                                  0,
                                  limit);
        return -1;
    }

    vemb_v16_migration_outbox_stats_t stats;
    memset(&stats, 0, sizeof(stats));
    pthread_mutex_lock(&storage->migration_outbox_lock);
    vemb_v16_migration_outbox_t *outbox =
        migration_outbox_find_locked(storage,
                                     migration_topology_epoch,
                                     target_owner,
                                     shard_id);
    if (outbox)
        vemb_v16_migration_outbox_get_stats(outbox, &stats);
    pthread_mutex_unlock(&storage->migration_outbox_lock);

    if (key_count == 0) {
        migration_range_fill_resp(resp,
                                  VEMB_V16_STATUS_OK,
                                  migration_topology_epoch,
                                  cutover_topology_epoch,
                                  target_owner,
                                  shard_id,
                                  outbox ? &stats : NULL,
                                  0,
                                  0,
                                  0,
                                  0,
                                  1,
                                  limit);
        return 0;
    }

    uint32_t success_count = 0;
    uint32_t error_count = 0;
    for (uint32_t i = 0; i < key_count; i++) {
        tlc_core_key_migration_info_t info = {0};
        if (vemb_v16_storage_migration_mark_source_gc_in_shard(
                storage,
                keys[i].key,
                keys[i].key_len,
                keys[i].key_hash,
                cutover_topology_epoch,
                target_owner,
                shard_id,
                &info) == 0) {
            success_count++;
        } else {
            error_count++;
        }
    }

    uint32_t remaining_after = 0;
    if (tlc_core_count_migration_keys(storage->tlc->core,
            cutover_topology_epoch,
            target_owner,
            shard_id,
            TLC_CORE_KEY_CUTOVER,
            &remaining_after) != 0) {
        error_count += remaining_before + 1;
        remaining_after = remaining_before;
    }

    migration_range_fill_resp(resp,
                              error_count == 0 ? VEMB_V16_STATUS_OK :
                                  VEMB_V16_STATUS_ERR,
                              migration_topology_epoch,
                              cutover_topology_epoch,
                              target_owner,
                              shard_id,
                              outbox ? &stats : NULL,
                              key_count,
                              success_count,
                              error_count,
                              remaining_after,
                              1,
                              limit);
    return error_count == 0 ? 0 : -1;
}

int vemb_v16_storage_migration_mark_migrating(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    uint32_t target_owner,
    tlc_core_key_migration_info_t *info) {
    return vemb_v16_storage_migration_mark_migrating_in_shard(
        storage,
        key,
        key_len,
        key_hash,
        topology_epoch,
        target_owner,
        VEMB_V16_STORAGE_MIGRATION_DEFAULT_SHARD_ID,
        info);
}

int vemb_v16_storage_migration_mark_migrating_in_shard(
    vemb_v16_storage_ctx_t *storage,
    const char *key,
    uint32_t key_len,
    uint64_t key_hash,
    uint64_t topology_epoch,
    uint32_t target_owner,
    uint32_t shard_id,
    tlc_core_key_migration_info_t *info) {
    RETURN_IF(!storage || !key ||
              key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN,
              -1);
    tlc_core_key_migration_info_t current = {0};
    int already_active =
        tlc_core_get_migration_info(storage->tlc->core,
                                        key,
                                        key_len,
                                        key_hash,
                                        &current) == 0 &&
        migration_info_requires_storage_slow_path(&current);
    if (!already_active)
        storage_migration_active_inc(storage);

    int rc = tlc_core_mark_migrating_in_shard(storage->tlc->core,
                                                  key,
                                                  key_len,
                                                  key_hash,
                                                  topology_epoch,
                                                  target_owner,
                                                  shard_id,
                                                  info);
    if (rc != 0 && !already_active)
        storage_migration_active_dec(storage);
    return rc;
}

/* =====================================================================
 *  Aeron UB ring allocation
 * =====================================================================
 *
 * Simple bump allocator within one UB device selected from the configured
 * path (normally /dev/obmm_shmdev1). A legacy range expression is still
 * accepted for compatibility, but the default configuration uses one fixed
 * device. The selected device
 * is mmapped once and subsequent calls carve out ring pairs from the bump
 * pointer. This is intentionally NOT a general allocator — it serves the
 * Aeron channel use case where the proxy owns the device for its lifetime.
 *
 * Concurrency: single mutex. ATTACH requests are rare (per-client, not
 * per-op), so contention is not a concern.
 *
 * ABI note: vemb_v16_client_ring_t has a fixed slot count
 * (VEMB_V16_CLIENT_RING_SIZE = 256). The ring_slots parameter is
 * therefore advisory only — actual ring layout is determined by
 * vemb_v16_client_ring_bytes(slot_size). Callers should still pass
 * ring_slots = VEMB_V16_CLIENT_RING_SIZE for documentation.
 */

/* Reserve 8 GB for cross-node aeron rings. 256-slot rings at a 1.5 KB
 * request slot size use about 0.2 MB per channel pair; the pool therefore
 * has ample room for the configured channel limit. Use ull suffix: 32-bit
 * overflow silently produces 0 and mmap fails. */
#define VEMB_V16_SHMDEV_CROSS_NODE_BYTES (8ull << 30)

typedef struct {
    int      inited;
    int      fd;
    uint32_t cache_policy;
    void    *base;
    size_t   size;
    size_t   bump;  /* next free byte offset */
    char     path[256];
} vemb_v16_shmdev_pool_t;

static pthread_mutex_t g_shmdev_pool_lock = PTHREAD_MUTEX_INITIALIZER;
static vemb_v16_shmdev_pool_t g_shmdev_pools[2];

static int open_ub_path_spec(const char *path_spec, int open_flags,
                             char selected[256], int *used_sync) {
    if (!path_spec || !path_spec[0]) return -1;

    int fd = vemb_v16_open_ub_with_fallback(path_spec, open_flags,
                                            used_sync);
    if (fd >= 0) {
        strncpy(selected, path_spec, 255);
        selected[255] = '\0';
        return fd;
    }

    /* Expand the deployment shorthand /dev/obmm_shmdev1-4. */
    const char *dash = strrchr(path_spec, '-');
    if (!dash || dash == path_spec || !dash[1]) return -1;
    const char *digits = dash;
    while (digits > path_spec && isdigit((unsigned char)digits[-1]))
        digits--;
    if (digits == dash) return -1;

    char *end = NULL;
    unsigned long start = strtoul(digits, &end, 10);
    if (end != dash || start > 255u) return -1;
    unsigned long finish = strtoul(dash + 1, &end, 10);
    if (end == dash + 1 || *end != '\0' || finish < start || finish > 255u)
        return -1;

    char candidate[256];
    size_t prefix_len = (size_t)(digits - path_spec);
    if (prefix_len >= sizeof(candidate)) return -1;
    for (unsigned long id = start; id <= finish; id++) {
        int written = snprintf(candidate, sizeof(candidate), "%.*s%lu",
                           (int)prefix_len, path_spec, id);
        if (written < 0 || (size_t)written >= sizeof(candidate)) return -1;
        fd = vemb_v16_open_ub_with_fallback(candidate, open_flags,
                                             used_sync);
        if (fd >= 0) {
            strncpy(selected, candidate, 255);
            selected[255] = '\0';
            return fd;
        }
    }
    return -1;
}

static int shmdev_pool_init(vemb_v16_shmdev_pool_t *pool,
                            const char *path_spec,
                            uint32_t cache_policy) {
    if (pool->inited)
        return 0;
    (void)cache_policy;
    char selected_path[256];
    int used_sync = 0;
    int fd = open_ub_path_spec(path_spec, O_RDWR, selected_path,
                               &used_sync);
    if (fd < 0) {
        serverLog(LL_WARNING, "aeron ub pool: no usable path in %s errno=%d (%s)",
                  path_spec, errno, strerror(errno));
        return -1;
    }
    void *p = mmap(NULL, VEMB_V16_SHMDEV_CROSS_NODE_BYTES,
                   PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED && (errno == EPERM || errno == EACCES)) {
        close(fd);
        fd = vemb_v16_open_ub_with_fallback(selected_path,
                                            O_RDWR | O_SYNC,
                                            &used_sync);
        if (fd >= 0) {
            used_sync = 1;
            p = mmap(NULL, VEMB_V16_SHMDEV_CROSS_NODE_BYTES,
                     PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        }
    }
    if (p == MAP_FAILED) {
        serverLog(LL_WARNING,
                  "aeron ub pool: mmap %s size=%llu failed errno=%d (%s)",
                  selected_path,
                  (unsigned long long)VEMB_V16_SHMDEV_CROSS_NODE_BYTES,
                  errno, strerror(errno));
        close(fd);
        return -2;
    }
    pool->fd   = fd;
    pool->base = p;
    pool->size = VEMB_V16_SHMDEV_CROSS_NODE_BYTES;
    pool->bump = 0;
    pool->cache_policy = used_sync ?
        VEMB_V16_UB_CACHE_POLICY_NONCACHEABLE :
        VEMB_V16_UB_CACHE_POLICY_CACHEABLE;
    strncpy(pool->path, selected_path, sizeof(pool->path) - 1);
    pool->path[sizeof(pool->path) - 1] = '\0';
    pool->inited = 1;
    serverLog(LL_NOTICE,
              "aeron ub pool ready: configured=%s selected=%s size=%zu mode=%s",
              path_spec, pool->path, pool->size,
              pool->cache_policy == VEMB_V16_UB_CACHE_POLICY_NONCACHEABLE ?
                  "NC" : "CC");
    return 0;
}

static int shmdev_pool_reserve(vemb_v16_shmdev_pool_t *pool,
                               const char *path_spec,
                               uint32_t cache_policy,
                               size_t bytes,
                               uint64_t *out_off,
                               void **out_mapping,
                               size_t *out_bytes,
                               char out_path[256]) {
    if (bytes == 0 || bytes % CACHELINE_SIZE != 0)
        return -1;
    if (shmdev_pool_init(pool, path_spec, cache_policy) != 0)
        return -2;
    size_t aligned = align_up_size(bytes, 4096u);
    if (pool->bump + aligned > pool->size)
        return -1;
    size_t off = pool->bump;
    pool->bump += aligned;
    *out_off = (uint64_t)off;
    *out_mapping = (uint8_t *)pool->base + off;
    *out_bytes = bytes;
    strncpy(out_path, pool->path, 255);
    out_path[255] = '\0';
    return 0;
}

int vemb_v16_storage_alloc_aeron_channel(const char *request_ub_path,
                                         const char *response_ub_path,
                                         uint32_t response_cache_policy,
                                         uint32_t req_slot_size,
                                         uint32_t resp_slot_size,
                                         uint32_t ring_slots,
                                         char out_request_shmdev_path[256],
                                         char out_response_shmdev_path[256],
                                         uint64_t *out_req_off,
                                         uint64_t *out_resp_off,
                                         void **out_req_mapping,
                                         void **out_resp_mapping,
                                         size_t *out_req_bytes,
                                         size_t *out_resp_bytes) {
    (void)ring_slots;  /* slot count is fixed at VEMB_V16_CLIENT_RING_SIZE */
    if (req_slot_size == 0 || resp_slot_size == 0)
        return -1;

    /* Use the inline helper so the byte count matches what every other
     * call site (including client-side poll/publish) computes from the
     * ring header. */
    size_t req_bytes  = vemb_v16_client_ring_bytes(req_slot_size);
    size_t resp_bytes = vemb_v16_client_ring_bytes(resp_slot_size);

    if (!request_ub_path || !request_ub_path[0] ||
        !response_ub_path || !response_ub_path[0] ||
        strcmp(request_ub_path, response_ub_path) == 0 ||
        !out_request_shmdev_path || !out_response_shmdev_path)
        return -1;

    pthread_mutex_lock(&g_shmdev_pool_lock);
    vemb_v16_shmdev_pool_t *req_pool = &g_shmdev_pools[0];
    vemb_v16_shmdev_pool_t *resp_pool = &g_shmdev_pools[1];
    size_t req_bump = req_pool->bump;
    size_t resp_bump = resp_pool->bump;
    int rc = shmdev_pool_reserve(req_pool, request_ub_path,
                                 VEMB_V16_UB_CACHE_POLICY_CACHEABLE,
                                 req_bytes,
                                 out_req_off, out_req_mapping, out_req_bytes,
                                 out_request_shmdev_path);
    if (rc == 0)
        rc = shmdev_pool_reserve(resp_pool, response_ub_path,
                                 response_cache_policy,
                                 resp_bytes,
                                 out_resp_off, out_resp_mapping, out_resp_bytes,
                                 out_response_shmdev_path);
    if (rc != 0) {
        req_pool->bump = req_bump;
        if (resp_pool != req_pool)
            resp_pool->bump = resp_bump;
    }
    pthread_mutex_unlock(&g_shmdev_pool_lock);
    if (rc != 0)
        return rc;

    void *req_map  = *out_req_mapping;
    void *resp_map = *out_resp_mapping;
    memset(req_map,  0, req_bytes);
    memset(resp_map, 0, resp_bytes);

    /* Initialize each ring header and its 64B-aligned slot layout. */
    vemb_v16_client_ring_init((vemb_v16_client_ring_t *)req_map,  req_slot_size);
    vemb_v16_client_ring_init((vemb_v16_client_ring_t *)resp_map, resp_slot_size);

    return 0;
}

void vemb_v16_storage_free_aeron_channel(void *req_mapping, size_t req_bytes,
                                         void *resp_mapping, size_t resp_bytes) {
    /* No-op: pool is process-lifetime. Memory is released when server
     * process exits. This matches the existing local-aeron SHM lifecycle
     * where rings survive until channel close. */
    (void)req_mapping; (void)req_bytes;
    (void)resp_mapping; (void)resp_bytes;
}

int vemb_v16_storage_alloc_aeron_batch_channel(
    const char *request_ub_path, const char *response_ub_path,
    uint32_t response_cache_policy,
    uint32_t descriptor_slot_size, uint32_t descriptor_slots,
    uint32_t request_arena_bytes, uint32_t response_arena_bytes,
    vemb_v16_aeron_batch_channel_allocation_t *out) {
    if (!request_ub_path || !request_ub_path[0] ||
        !response_ub_path || !response_ub_path[0] ||
        strcmp(request_ub_path, response_ub_path) == 0 || !out ||
        descriptor_slot_size == 0 ||
        descriptor_slot_size % CACHELINE_SIZE != 0 ||
        descriptor_slots != VEMB_V16_CLIENT_RING_SIZE ||
        request_arena_bytes == 0 || response_arena_bytes == 0 ||
        request_arena_bytes % CACHELINE_SIZE != 0 ||
        response_arena_bytes % CACHELINE_SIZE != 0)
        return -1;

    memset(out, 0, sizeof(*out));
    size_t desc_bytes = vemb_v16_client_ring_bytes(descriptor_slot_size);
    pthread_mutex_lock(&g_shmdev_pool_lock);
    vemb_v16_shmdev_pool_t *req_pool = &g_shmdev_pools[0];
    vemb_v16_shmdev_pool_t *resp_pool = &g_shmdev_pools[1];
    size_t req_bump = req_pool->bump;
    size_t resp_bump = resp_pool->bump;
    int rc = shmdev_pool_reserve(req_pool, request_ub_path,
                                 VEMB_V16_UB_CACHE_POLICY_CACHEABLE,
                                 desc_bytes,
                                 &out->request_desc_off, &out->request_desc_mapping,
                                 &out->request_desc_bytes, out->request_path);
    if (rc == 0)
        rc = shmdev_pool_reserve(req_pool, request_ub_path,
                                 VEMB_V16_UB_CACHE_POLICY_CACHEABLE,
                                 request_arena_bytes,
                                 &out->request_arena_off, &out->request_arena_mapping,
                                 &out->request_arena_bytes, out->request_path);
    if (rc == 0)
        rc = shmdev_pool_reserve(resp_pool, response_ub_path,
                                 response_cache_policy,
                                 desc_bytes,
                                 &out->response_desc_off, &out->response_desc_mapping,
                                 &out->response_desc_bytes, out->response_path);
    if (rc == 0)
        rc = shmdev_pool_reserve(resp_pool, response_ub_path,
                                 response_cache_policy,
                                 response_arena_bytes,
                                 &out->response_arena_off, &out->response_arena_mapping,
                                 &out->response_arena_bytes, out->response_path);
    if (rc != 0) {
        req_pool->bump = req_bump;
        if (resp_pool != req_pool)
            resp_pool->bump = resp_bump;
        memset(out, 0, sizeof(*out));
    }
    pthread_mutex_unlock(&g_shmdev_pool_lock);
    if (rc != 0)
        return rc;

    memset(out->request_desc_mapping, 0, out->request_desc_bytes);
    memset(out->request_arena_mapping, 0, out->request_arena_bytes);
    memset(out->response_desc_mapping, 0, out->response_desc_bytes);
    memset(out->response_arena_mapping, 0, out->response_arena_bytes);
    vemb_v16_client_ring_init(out->request_desc_mapping, descriptor_slot_size);
    vemb_v16_client_ring_init(out->response_desc_mapping, descriptor_slot_size);
    return 0;
}

const vemb_v16_manifest_region_t *
vemb_v16_storage_first_local_region(
    const vemb_v16_storage_ctx_t *storage) {
    if (!storage) return NULL;
    if (storage->local_manifest_region_count > 0)
        return &storage->local_manifest_regions[0];
    return NULL;
}
