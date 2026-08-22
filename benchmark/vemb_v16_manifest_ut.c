#include "../src/monotonic.h"
#include "../src/vemb_v16_storage.h"

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

static void fill_vector(float *vector, uint32_t dim, uint32_t seed) {
    for (uint32_t i = 0; i < dim; i++)
        vector[i] = (float)(seed + i);
}

static void cleanup_region_and_layout(const char *path, uint32_t region_id) {
    char layout_name[VEMB_V16_WARM_REGION_LAYOUT_NAME_MAX];
    shm_unlink(path);
    if (vemb_v16_warm_region_layout_name_from_region_path(
            path, region_id, layout_name, sizeof(layout_name)) == 0) {
        shm_unlink(layout_name);
    }
}

typedef struct manifest_ut_layout_view {
    vemb_v16_mapped_region_t mapping;
    vemb_v16_warm_region_header_t *header;
} manifest_ut_layout_view_t;

static int manifest_ut_open_layout_view(manifest_ut_layout_view_t *view,
                                        const char *path,
                                        uint32_t capacity_slots) {
    RETURN_IF(!view || !path || capacity_slots == 0, -1);
    memset(view, 0, sizeof(*view));
    if (vemb_v16_mapped_region_open(&view->mapping,
                                    VEMB_V16_REGION_LOCAL_SHM,
                                    VEMB_V16_UB_CACHE_POLICY_CACHEABLE,
                                    path,
                                    0,
                                    vemb_v16_warm_region_layout_bytes(
                                        capacity_slots)) != 0) {
        return -1;
    }
    view->header = (vemb_v16_warm_region_header_t *)view->mapping.mapped_addr;
    return 0;
}

static void manifest_ut_close_layout_view(manifest_ut_layout_view_t *view) {
    if (!view)
        return;
    vemb_v16_mapped_region_close(&view->mapping);
    memset(view, 0, sizeof(*view));
}

static int manifest_ut_claim_slot(manifest_ut_layout_view_t *view,
                                  uint32_t *slot_out) {
    RETURN_IF(!view || !view->header || !slot_out, -1);
    vemb_v16_warm_slot_meta_t *slots =
        vemb_v16_warm_region_slot_meta(view->header);
    RETURN_IF(!slots, -1);
    for (uint32_t slot = 0; slot < view->header->capacity_slots; slot++) {
        uint32_t expected = VEMB_V16_WARM_SLOT_FREE;
        if (atomic_compare_exchange_strong_explicit(&slots[slot].state,
                                                    &expected,
                                                    VEMB_V16_WARM_SLOT_FILLING,
                                                    memory_order_acq_rel,
                                                    memory_order_acquire)) {
            *slot_out = slot;
            return 0;
        }
    }
    return -1;
}

static void cleanup_shm_path(const char *path) {
    shm_unlink(path);
}

static const vemb_v16_storage_owner_resolver_snapshot_t *
manifest_ut_active_owner_snapshot(vemb_v16_storage_ctx_t *storage) {
    uint32_t active = atomic_load_explicit(
        &storage->owner_resolver_active_snapshot,
        memory_order_acquire);
    return &storage->owner_resolver_snapshots[active];
}

static uint32_t manifest_ut_route_owner(vemb_v16_storage_ctx_t *storage,
                                        uint64_t key_hash) {
    const vemb_v16_storage_owner_resolver_snapshot_t *snapshot =
        manifest_ut_active_owner_snapshot(storage);
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

static void test_shm_provider_attaches_existing_payload(void) {
    enum { dim = 2, slots = 2 };
    char shm_name[64];
    vemb_v16_warm_provider_t first, second;
    float vector[dim];

    snprintf(shm_name, sizeof(shm_name),
             "/vemb_v16_provider_%ld_attach", (long)getpid());
    shm_unlink(shm_name);
    memset(&first, 0, sizeof(first));
    memset(&second, 0, sizeof(second));
    first.fd = -1;
    second.fd = -1;

    assert(vemb_v16_warm_provider_open(&first,
                                       301,
                                       VEMB_V16_REGION_LOCAL_SHM,
                                       VEMB_V16_UB_CACHE_POLICY_CACHEABLE,
                                       shm_name,
                                       0,
                                       dim * sizeof(float),
                                       slots * dim * sizeof(float),
                                       0,
                                       1,
                                       1) == 0);
    fill_vector(vector, dim, 11);
    memcpy(first.region.mapped_addr, vector, sizeof(vector));

    assert(vemb_v16_warm_provider_open(&second,
                                       301,
                                       VEMB_V16_REGION_LOCAL_SHM,
                                       VEMB_V16_UB_CACHE_POLICY_CACHEABLE,
                                       shm_name,
                                       0,
                                       dim * sizeof(float),
                                       slots * dim * sizeof(float),
                                       0,
                                       1,
                                       1) == 0);
    assert(memcmp(second.region.mapped_addr, vector, sizeof(vector)) == 0);

    vemb_v16_warm_provider_close(&first);
    assert(memcmp(second.region.mapped_addr, vector, sizeof(vector)) == 0);
    vemb_v16_warm_provider_close(&second);
    shm_unlink(shm_name);
}

static void test_manifest_shm_mock_ub_create_and_put(void) {
    enum { dim = 2, max_vectors = 4 };
    const uint32_t warm_region_bytes = sizeof(float) * dim * 2 + 1;
    char manifest_path[128];
    char shm1[64];
    char ub2[128];
    vemb_v16_storage_ctx_t *storage = NULL;
    float vector[dim];
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    const char *key = "manifest-key";
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));

    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_manifest_%ld.yaml", (long)getpid());
    snprintf(shm1, sizeof(shm1), "/vemb_v16_manifest_%ld_r1", (long)getpid());
    snprintf(ub2, sizeof(ub2), "/tmp/vemb_v16_manifest_%ld_r2.ub", (long)getpid());
    unlink(ub2);
    int ub_fd = open(ub2, O_CREAT | O_TRUNC | O_RDWR, 0666);
    assert(ub_fd >= 0);
    size_t ub2_allocator_layout =
        vemb_v16_warm_region_layout_bytes(2);
    assert(ftruncate(ub_fd,
                     ub2_allocator_layout +
                     warm_region_bytes) == 0);
    close(ub_fd);

    FILE *fp = fopen(manifest_path, "w");
    assert(fp != NULL);
    fprintf(fp,
            "supernode_id: 0\n"
            "local_ub_node_id: 0\n"
            "local_region_weight: 8\n"
            "warm_regions:\n"
            "  - region_id: 101\n"
            "    provider: shm\n"
            "    path: %s\n"
            "    mmap_offset: 0\n"
            "    bytes: %u\n"
            "    value_size: %u\n"
            "    home_ub_node_id: 0\n"
            "    weight: 1\n"
            "  - region_id: 202\n"
            "    provider: ub\n"
            "    path: %s\n"
            "    mmap_offset: 0\n"
            "    bytes: %u\n"
            "    value_size: %u\n"
            "    home_ub_node_id: 1\n"
            "    weight: 1\n",
            shm1,
            warm_region_bytes,
            (unsigned)(sizeof(float) * dim),
            ub2,
            warm_region_bytes,
            (unsigned)(sizeof(float) * dim));
    fclose(fp);

    vemb_v16_warm_regions_manifest_t manifest;
    assert(vemb_v16_parse_warm_regions_manifest(manifest_path,
                                                dim * sizeof(float),
                                                &manifest) == 0);
    assert(vemb_v16_storage_ctx_create_from_manifest(&storage,
                                                     dim,
                                                     dim * sizeof(float),
                                                     max_vectors,
                                                     &manifest) == 0);
    assert(storage->warm_region_count == 2);
    assert(storage->tlc->warm_region_count == 2);
    assert(storage->remote_meta_base != NULL);
    assert(storage->remote_meta_entry_count == max_vectors);
    assert(storage->remote_meta_bucket_count >= max_vectors * 2);
    assert(storage->tlc->remote_meta_view == &storage->remote_meta_view);
    assert(vemb_v16_tlc_find_region(storage->tlc, 101) != NULL);
    assert(vemb_v16_tlc_find_region(storage->tlc, 202) != NULL);
    vemb_v16_channel_desc_t desc;
    memset(&desc, 0, sizeof(desc));
    vemb_v16_storage_fill_channel_desc(storage, &desc);
    assert(desc.warm_region_count == 2);
    assert(desc.warm_regions[0].region_id == 101);
    assert(desc.warm_regions[1].region_id == 202);
    assert(!strcmp(desc.warm_regions[0].path, shm1));
    assert(!strcmp(desc.warm_regions[1].path, ub2));
    assert(desc.warm_regions[1].backend_type == VEMB_V16_REGION_UB);
    assert(desc.warm_regions[1].mmap_offset == ub2_allocator_layout);

    fill_vector(vector, dim, 42);
    assert(vemb_v16_tlc_put(storage->tlc,
                            key,
                            (uint32_t)strlen(key),
                            key_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);
    assert(handle.bytes == sizeof(vector));
    assert(handle.region_id == 101 || handle.region_id == 202);
    const uint8_t *slice = NULL;
    uint32_t slice_bytes = 0;
    assert(vemb_v16_tlc_vector_slice(storage->tlc,
                                     &handle,
                                     &slice,
                                     &slice_bytes) == 0);
    assert(slice_bytes == sizeof(vector));
    assert(memcmp(slice, vector, sizeof(vector)) == 0);
    assert(publish_remote_meta_to_view(storage->tlc,
                                       storage->tlc->remote_meta_view,
                                       key,
                                       (uint32_t)strlen(key),
                                       key_hash,
                                       &handle,
                                       0) == 0);
    vemb_v16_remote_meta_handle_t remote_handle = {0};
    assert(vemb_v16_remote_meta_lookup(&storage->remote_meta_view,
                                       key,
                                       (uint32_t)strlen(key),
                                       key_hash,
                                       VEMB_V16_REMOTE_META_DEFAULT_RETRIES,
                                       &remote_handle) ==
           VEMB_V16_REMOTE_META_OK);
    assert(remote_handle.region_id == handle.region_id);
    assert(remote_handle.offset == handle.offset);
    assert(remote_handle.bytes == handle.bytes);
    assert(remote_handle.local_slot == handle.local_slot);
    assert(remote_handle.owner_generation == handle.owner_generation);

    vemb_v16_storage_ctx_destroy(storage);
    unlink(manifest_path);
    cleanup_region_and_layout(shm1, 101);
    unlink(ub2);
}

static void test_manifest_remote_meta_shm_attach_existing(void) {
    enum { dim = 2, max_vectors = 4 };
    char manifest_path[128];
    char shm1[64];
    char remote_meta_shm[64];
    vemb_v16_storage_ctx_t *first = NULL;
    vemb_v16_storage_ctx_t *second = NULL;
    float vector[dim];
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    const char *key = "remote-meta-persist-key";
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));

    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_manifest_%ld_remote_meta.yaml", (long)getpid());
    snprintf(shm1, sizeof(shm1), "/v16rmr1_%ld", (long)getpid());
    snprintf(remote_meta_shm, sizeof(remote_meta_shm),
             "/v16rmeta_%ld", (long)getpid());
    cleanup_region_and_layout(shm1, 501);
    shm_unlink(remote_meta_shm);

    FILE *fp = fopen(manifest_path, "w");
    assert(fp != NULL);
    fprintf(fp,
            "local_ub_node_id: 0\n"
            "remote_meta_provider: shm\n"
            "remote_meta_path: %s\n"
            "remote_meta_mmap_offset: 0\n"
            "remote_meta_entries: %u\n"
            "remote_meta_buckets: %u\n"
            "warm_regions:\n"
            "  - region_id: 501\n"
            "    provider: shm\n"
            "    path: %s\n"
            "    mmap_offset: 0\n"
            "    bytes: %u\n"
            "    value_size: %u\n"
            "    home_ub_node_id: 0\n"
            "    is_local: true\n"
            "    weight: 1\n",
            remote_meta_shm,
            max_vectors,
            max_vectors * 2,
            shm1,
            (unsigned)(sizeof(float) * dim * max_vectors),
            (unsigned)(sizeof(float) * dim));
    fclose(fp);

    vemb_v16_warm_regions_manifest_t manifest;
    assert(vemb_v16_parse_warm_regions_manifest(manifest_path,
                                                dim * sizeof(float),
                                                &manifest) == 0);
    assert(manifest.remote_meta_path[0] != '\0');
    assert(manifest.remote_meta_backend_type == VEMB_V16_REGION_LOCAL_SHM);
    assert(manifest.remote_meta_entry_count == max_vectors);
    assert(manifest.remote_meta_bucket_count == max_vectors * 2);

    assert(vemb_v16_storage_ctx_create_from_manifest(&first,
                                                     dim,
                                                     dim * sizeof(float),
                                                     max_vectors,
                                                     &manifest) == 0);
    assert(first->remote_meta_is_mapped == 1);
    assert(first->remote_meta_base == first->remote_meta_mapping.mapped_addr);
    fill_vector(vector, dim, 900);
    assert(vemb_v16_tlc_put(first->tlc,
                            key,
                            (uint32_t)strlen(key),
                            key_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);
    assert(publish_remote_meta_to_view(first->tlc,
                                       first->tlc->remote_meta_view,
                                       key,
                                       (uint32_t)strlen(key),
                                       key_hash,
                                       &handle,
                                       0) == 0);
    vemb_v16_storage_ctx_destroy(first);

    assert(vemb_v16_storage_ctx_create_from_manifest(&second,
                                                     dim,
                                                     dim * sizeof(float),
                                                     max_vectors,
                                                     &manifest) == 0);
    assert(second->remote_meta_is_mapped == 1);
    vemb_v16_remote_meta_handle_t remote_handle = {0};
    assert(vemb_v16_remote_meta_lookup(&second->remote_meta_view,
                                       key,
                                       (uint32_t)strlen(key),
                                       key_hash,
                                       VEMB_V16_REMOTE_META_DEFAULT_RETRIES,
                                       &remote_handle) ==
           VEMB_V16_REMOTE_META_OK);
    assert(remote_handle.region_id == handle.region_id);
    assert(remote_handle.offset == handle.offset);
    assert(remote_handle.bytes == handle.bytes);
    assert(remote_handle.local_slot == handle.local_slot);
    assert(remote_handle.owner_generation == handle.owner_generation);

    vemb_v16_storage_ctx_destroy(second);
    unlink(manifest_path);
    cleanup_region_and_layout(shm1, 501);
    shm_unlink(remote_meta_shm);
}

static void test_manifest_remote_meta_owner_views_route_key2(void) {
    enum { dim = 2, max_vectors = 4 };
    char manifest_path[128];
    char shm1[64];
    char remote_meta_default[64];
    char remote_meta_owner1[64];
    char remote_meta_owner2[64];
    vemb_v16_storage_ctx_t *storage = NULL;
    char key[64];
    uint64_t key_hash = 0;
    float vector[dim];
    vemb_v16_vector_handle_t published = {0};
    uint32_t warm_slot = UINT32_MAX;

    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_manifest_%ld_remote_meta_views.yaml",
             (long)getpid());
    snprintf(shm1, sizeof(shm1), "/v16rmvr_%ld", (long)getpid());
    snprintf(remote_meta_default, sizeof(remote_meta_default),
             "/v16rmd_%ld", (long)getpid());
    snprintf(remote_meta_owner1, sizeof(remote_meta_owner1),
             "/v16rmv1_%ld", (long)getpid());
    snprintf(remote_meta_owner2, sizeof(remote_meta_owner2),
             "/v16rmv2_%ld", (long)getpid());
    cleanup_region_and_layout(shm1, 701);
    shm_unlink(remote_meta_default);
    shm_unlink(remote_meta_owner1);
    shm_unlink(remote_meta_owner2);

    FILE *fp = fopen(manifest_path, "w");
    assert(fp != NULL);
    fprintf(fp,
            "local_ub_node_id: 0\n"
            "remote_meta_provider: shm\n"
            "remote_meta_path: %s\n"
            "remote_meta_entries: %u\n"
            "remote_meta_buckets: %u\n"
            "warm_regions:\n"
            "  - region_id: 701\n"
            "    provider: shm\n"
            "    path: %s\n"
            "    mmap_offset: 0\n"
            "    bytes: %u\n"
            "    value_size: %u\n"
            "    home_ub_node_id: 0\n"
            "    is_local: true\n"
            "    weight: 1\n"
            "remote_meta_views:\n"
            "  - owner_id: 1\n"
            "    provider: shm\n"
            "    path: %s\n"
            "    entries: %u\n"
            "    buckets: %u\n"
            "  - owner_id: 2\n"
            "    provider: shm\n"
            "    path: %s\n"
            "    entries: %u\n"
            "    buckets: %u\n",
            remote_meta_default,
            max_vectors,
            max_vectors * 2,
            shm1,
            (unsigned)(sizeof(float) * dim * max_vectors),
            (unsigned)(sizeof(float) * dim),
            remote_meta_owner1,
            max_vectors,
            max_vectors * 2,
            remote_meta_owner2,
            max_vectors,
            max_vectors * 2);
    fclose(fp);

    vemb_v16_warm_regions_manifest_t manifest;
    assert(vemb_v16_parse_warm_regions_manifest(manifest_path,
                                                dim * sizeof(float),
                                                &manifest) == 0);
    assert(manifest.remote_meta_view_count == 2);
    assert(manifest.remote_meta_views[0].owner_id == 1);
    assert(manifest.remote_meta_views[1].owner_id == 2);

    assert(vemb_v16_storage_ctx_create_from_manifest(&storage,
                                                     dim,
                                                     dim * sizeof(float),
                                                     max_vectors,
                                                     &manifest) == 0);
    assert(storage->remote_meta_owner_view_count == 2);
    assert(storage->remote_meta_owner_views[0].owner_id == 1);
    assert(storage->remote_meta_owner_views[1].owner_id == 2);
    assert(storage->tlc->remote_meta_view_count == 3);
    assert(manifest_ut_active_owner_snapshot(storage)->owner_hash_node_count ==
           30);
    uint32_t routed_owner = UINT32_MAX;
    uint32_t remote_view_index = UINT32_MAX;
    for (uint32_t i = 0; i < 10000; i++) {
        snprintf(key, sizeof(key), "remote-owner-auto-key:%u", i);
        key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
        routed_owner = manifest_ut_route_owner(storage, key_hash);
        for (uint32_t view_index = 0;
             view_index < storage->remote_meta_owner_view_count;
             view_index++) {
            if (storage->remote_meta_owner_views[view_index].owner_id !=
                routed_owner) {
                continue;
            }
            remote_view_index = view_index;
            break;
        }
        if (remote_view_index != UINT32_MAX)
            break;
    }
    assert(remote_view_index != UINT32_MAX);
    assert(storage->remote_meta_owner_views[remote_view_index].owner_id ==
           routed_owner);

    fill_vector(vector, dim, 1100);
    assert(vemb_v16_tlc_put(storage->tlc,
                            key,
                            (uint32_t)strlen(key),
                            key_hash,
                            vector,
                            sizeof(vector),
                            &published,
                            &warm_slot) == 0);
    vemb_v16_remote_meta_handle_t remote_handle = {
        .region_id = published.region_id,
        .bytes = published.bytes,
        .local_slot = published.local_slot,
        .offset = published.offset,
        .key_hash = key_hash,
        .owner_generation = published.owner_generation,
    };
    assert(vemb_v16_remote_meta_publish(
               &storage->remote_meta_owner_views[remote_view_index].view,
                                        key,
                                        (uint32_t)strlen(key),
                                        key_hash,
                                        &remote_handle) ==
           VEMB_V16_REMOTE_META_OK);

    vemb_v16_vector_handle_t handle = {0};
    vemb_v16_tlc_lookup_source_t source = VEMB_V16_TLC_LOOKUP_SOURCE_NONE;
    assert(vemb_v16_tlc_lookup_vsim_key2(storage->tlc,
                                         key,
                                         (uint32_t)strlen(key),
                                         key_hash,
                                         &handle,
                                         &source) == 0);
    assert(source == VEMB_V16_TLC_LOOKUP_SOURCE_REMOTE);
    assert(handle.region_id == remote_handle.region_id);
    assert(handle.offset == remote_handle.offset);
    assert(handle.bytes == remote_handle.bytes);
    assert(handle.local_slot == remote_handle.local_slot);
    assert(handle.owner_generation == remote_handle.owner_generation);
    const uint8_t *slice = NULL;
    uint32_t slice_bytes = 0;
    assert(vemb_v16_tlc_vector_slice(storage->tlc,
                                     &handle,
                                     &slice,
                                     &slice_bytes) == 0);
    assert(slice_bytes == sizeof(vector));
    assert(memcmp(slice, vector, sizeof(vector)) == 0);

    vemb_v16_storage_ctx_destroy(storage);
    assert(vemb_v16_storage_reset_manifest_regions(&manifest) == 0);
    int fd = shm_open(remote_meta_owner2, O_RDWR, 0666);
    assert(fd < 0);
    unlink(manifest_path);
    shm_unlink(remote_meta_default);
    shm_unlink(remote_meta_owner1);
    shm_unlink(remote_meta_owner2);
    cleanup_region_and_layout(shm1, 701);
}

static void test_manifest_ub_rpc_peer_parse_and_storage_init(void) {
    enum { dim = 2, max_vectors = 4 };
    char manifest_path[128];
    char shm1[64];
    char remote_meta_default[64];
    char req0_1[64];
    char req1_0[64];
    char resp0_1[64];
    char resp1_0[64];
    vemb_v16_storage_ctx_t *storage = NULL;
    float vector[dim];
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    const char *cutover_key = "manifest-cutover-key";
    uint32_t cutover_key_len = (uint32_t)strlen(cutover_key);
    uint64_t cutover_key_hash =
        vemb_v16_xxh3_64_str(cutover_key, cutover_key_len);
    tlc_core_key_migration_info_t info = {0};
    vemb_v16_migration_outbox_stats_t outbox_stats = {0};

    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_manifest_%ld_ub_rpc.yaml", (long)getpid());
    snprintf(shm1, sizeof(shm1), "/v16rpcmr_%ld", (long)getpid());
    snprintf(remote_meta_default, sizeof(remote_meta_default),
             "/v16rpcmd_%ld", (long)getpid());
    snprintf(req0_1, sizeof(req0_1), "/v16rpcm_%ld_req_0_1", (long)getpid());
    snprintf(req1_0, sizeof(req1_0), "/v16rpcm_%ld_req_1_0", (long)getpid());
    snprintf(resp0_1, sizeof(resp0_1), "/v16rpcm_%ld_resp_0_1", (long)getpid());
    snprintf(resp1_0, sizeof(resp1_0), "/v16rpcm_%ld_resp_1_0", (long)getpid());
    cleanup_region_and_layout(shm1, 801);
    shm_unlink(remote_meta_default);
    shm_unlink(req0_1);
    shm_unlink(req1_0);
    shm_unlink(resp0_1);
    shm_unlink(resp1_0);

    FILE *fp = fopen(manifest_path, "w");
    assert(fp != NULL);
    fprintf(fp,
            "local_ub_node_id: 0\n"
            "remote_meta_provider: shm\n"
            "remote_meta_path: %s\n"
            "remote_meta_entries: %u\n"
            "remote_meta_buckets: %u\n"
            "ub_rpc_timeout_ms: 123\n"
            "warm_regions:\n"
            "  - region_id: 801\n"
            "    provider: shm\n"
            "    path: %s\n"
            "    mmap_offset: 0\n"
            "    bytes: %u\n"
            "    value_size: %u\n"
            "    home_ub_node_id: 0\n"
            "    is_local: true\n"
            "    weight: 1\n"
            "ub_rpc_peers:\n"
            "  - owner_id: 1\n"
            "    provider: shm\n"
            "    request_path: %s\n"
            "    response_path: %s\n"
            "    inbound_request_path: %s\n"
            "    outbound_response_path: %s\n",
            remote_meta_default,
            max_vectors,
            max_vectors * 2,
            shm1,
            (unsigned)(sizeof(float) * dim * max_vectors),
            (unsigned)(sizeof(float) * dim),
            req0_1,
            resp1_0,
            req1_0,
            resp0_1);
    fclose(fp);

    vemb_v16_warm_regions_manifest_t manifest;
    assert(vemb_v16_parse_warm_regions_manifest(manifest_path,
                                                dim * sizeof(float),
                                                &manifest) == 0);
    assert(manifest.ub_rpc_timeout_ms == 123);
    assert(manifest.ub_rpc_peer_count == 1);
    assert(manifest.ub_rpc_peers[0].owner_id == 1);
    assert(!strcmp(manifest.ub_rpc_peers[0].request.path, req0_1));
    assert(!strcmp(manifest.ub_rpc_peers[0].response.path, resp1_0));
    assert(!strcmp(manifest.ub_rpc_peers[0].inbound_request.path, req1_0));
    assert(!strcmp(manifest.ub_rpc_peers[0].outbound_response.path, resp0_1));

    assert(vemb_v16_storage_ctx_create_from_manifest(&storage,
                                                     dim,
                                                     dim * sizeof(float),
                                                     max_vectors,
                                                     &manifest) == 0);
    assert(storage->ub_rpc != NULL);
    assert(manifest_ut_active_owner_snapshot(storage)->owner_hash_node_count ==
           20);
    {
        vemb_v16_channel_desc_t desc;
        memset(&desc, 0, sizeof(desc));
        vemb_v16_storage_fill_channel_desc(storage, &desc);
        assert(desc.local_owner_id == 0);
        assert(desc.remote_meta_backend_type == VEMB_V16_REGION_LOCAL_SHM);
        assert(!strcmp(desc.remote_meta_path, remote_meta_default));
        assert(desc.remote_meta_entry_count == max_vectors);
        assert(desc.remote_meta_bucket_count == max_vectors * 2);
        assert(desc.ub_rpc_timeout_ms == 123);
        assert(desc.ub_rpc_peer_count == 1);
        assert(desc.ub_rpc_peers[0].owner_id == 1);
        assert(!strcmp(desc.ub_rpc_peers[0].request_path, req0_1));
        assert(!strcmp(desc.ub_rpc_peers[0].response_path, resp1_0));
        assert(!strcmp(desc.ub_rpc_peers[0].inbound_request_path, req1_0));
        assert(!strcmp(desc.ub_rpc_peers[0].outbound_response_path, resp0_1));
    }
    fill_vector(vector, dim, 1234);
    assert(vemb_v16_tlc_put(storage->tlc,
                            cutover_key,
                            cutover_key_len,
                            cutover_key_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);
    assert(tlc_core_mark_migrating_in_shard(storage->tlc->core,
                                            cutover_key,
                                            cutover_key_len,
                                            cutover_key_hash,
                                            40,
                                            1,
                                            0,
                                            &info) == 0);
    assert(vemb_v16_storage_migration_mark_cutover(storage,
                                                  cutover_key,
                                                  cutover_key_len,
                                                  cutover_key_hash,
                                                  41,
                                                  1,
                                                  &info) != 0);
    assert(vemb_v16_storage_migration_barrier(storage,
                                             cutover_key,
                                             cutover_key_len,
                                             cutover_key_hash,
                                             40,
                                             1,
                                             &info,
                                             &outbox_stats) == 0);
    assert(outbox_stats.state == VEMB_V16_MIGRATION_OUTBOX_OPEN);
    assert(outbox_stats.barrier_seq == 0);
    assert(vemb_v16_storage_migration_mark_cutover(storage,
                                                  cutover_key,
                                                  cutover_key_len,
                                                  cutover_key_hash,
                                                  41,
                                                  1,
                                                  &info) != 0);
    assert(vemb_v16_tlc_get_handle(storage->tlc,
                                   cutover_key,
                                   cutover_key_len,
                                   cutover_key_hash,
                                   &handle,
                                   &warm_slot) == 0);
    vemb_v16_storage_ctx_destroy(storage);

    assert(vemb_v16_storage_reset_manifest_regions(&manifest) == 0);
    assert(shm_open(req0_1, O_RDWR, 0666) < 0);
    assert(shm_open(req1_0, O_RDWR, 0666) < 0);
    assert(shm_open(resp0_1, O_RDWR, 0666) < 0);
    assert(shm_open(resp1_0, O_RDWR, 0666) < 0);

    unlink(manifest_path);
    shm_unlink(remote_meta_default);
    shm_unlink(req0_1);
    shm_unlink(req1_0);
    shm_unlink(resp0_1);
    shm_unlink(resp1_0);
    cleanup_region_and_layout(shm1, 801);
}

static void test_storage_reset_clears_payload_and_layout(void) {
    enum { dim = 2, slots = 2 };
    char shm_name[64];
    vemb_v16_warm_regions_manifest_t manifest;
    vemb_v16_warm_provider_t provider;
    manifest_ut_layout_view_t layout;
    char layout_name[VEMB_V16_WARM_REGION_LAYOUT_NAME_MAX];
    float vector[dim];

    snprintf(shm_name, sizeof(shm_name),
             "/vemb_v16_reset_%ld_region", (long)getpid());
    cleanup_region_and_layout(shm_name, 401);

    memset(&manifest, 0, sizeof(manifest));
    manifest.region_count = 1;
    manifest.local_region_weight = 4;
    manifest.regions[0].region_id = 401;
    manifest.regions[0].backend_type = VEMB_V16_REGION_LOCAL_SHM;
    manifest.regions[0].is_local = 1;
    manifest.regions[0].has_is_local = 1;
    manifest.regions[0].weight = 1;
    manifest.regions[0].value_size = dim * sizeof(float);
    manifest.regions[0].region_bytes = slots * dim * sizeof(float);
    snprintf(manifest.regions[0].path,
             sizeof(manifest.regions[0].path),
             "%s",
             shm_name);

    memset(&provider, 0, sizeof(provider));
    provider.fd = -1;
    assert(vemb_v16_warm_provider_open(&provider,
                                       401,
                                       VEMB_V16_REGION_LOCAL_SHM,
                                       VEMB_V16_UB_CACHE_POLICY_CACHEABLE,
                                       shm_name,
                                       0,
                                       dim * sizeof(float),
                                       slots * dim * sizeof(float),
                                       0,
                                       1,
                                       1) == 0);
    fill_vector(vector, dim, 55);
    memcpy(provider.region.mapped_addr, vector, sizeof(vector));
    vemb_v16_warm_provider_close(&provider);

    assert(vemb_v16_warm_region_layout_name_from_region_path(
               shm_name, 401, layout_name, sizeof(layout_name)) == 0);
    assert(manifest_ut_open_layout_view(&layout, layout_name, slots) == 0);
    atomic_init(&layout.header->magic, VEMB_V16_WARM_REGION_LAYOUT_MAGIC);
    layout.header->version = VEMB_V16_WARM_REGION_LAYOUT_VERSION;
    layout.header->region_id = 401;
    layout.header->capacity_slots = slots;
    uint32_t slot = UINT32_MAX;
    assert(manifest_ut_claim_slot(&layout, &slot) == 0);
    assert(slot == 0);
    manifest_ut_close_layout_view(&layout);

    assert(vemb_v16_storage_reset_manifest_regions(&manifest) == 0);

    memset(&provider, 0, sizeof(provider));
    provider.fd = -1;
    assert(vemb_v16_warm_provider_open(&provider,
                                       401,
                                       VEMB_V16_REGION_LOCAL_SHM,
                                       VEMB_V16_UB_CACHE_POLICY_CACHEABLE,
                                       shm_name,
                                       0,
                                       dim * sizeof(float),
                                       slots * dim * sizeof(float),
                                       0,
                                       1,
                                       1) == 0);
    float zero[dim] = {0};
    assert(memcmp(provider.region.mapped_addr, zero, sizeof(zero)) == 0);
    vemb_v16_warm_provider_close(&provider);

    assert(manifest_ut_open_layout_view(&layout, layout_name, slots) == 0);
    assert(atomic_load_explicit(&layout.header->magic, memory_order_acquire) ==
           0);
    assert(layout.header->capacity_slots == 0);
    slot = UINT32_MAX;
    assert(manifest_ut_claim_slot(&layout, &slot) != 0);
    manifest_ut_close_layout_view(&layout);
    cleanup_region_and_layout(shm_name, 401);
}

static void test_peer_view_map_can_attach_local_region(void) {
    enum { dim = 2, max_vectors = 4, slots = 2 };
    char manifest_path[128];
    char local0[64];
    char local1[64];
    vemb_v16_storage_ctx_t *storage = NULL;
    vemb_v16_warm_regions_manifest_t manifest;
    vemb_v16_peer_view_map_req_t req;
    vemb_v16_peer_view_map_resp_t resp;
    float vector[dim];
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    const char *key = "peer-view-local-expand";
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));

    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_manifest_%ld_peer_view_local.yaml",
             (long)getpid());
    snprintf(local0, sizeof(local0), "/v16pvlocal0_%ld", (long)getpid());
    snprintf(local1, sizeof(local1), "/v16pvlocal1_%ld", (long)getpid());
    cleanup_region_and_layout(local0, 901);
    cleanup_region_and_layout(local1, 902);

    FILE *fp = fopen(manifest_path, "w");
    assert(fp != NULL);
    fprintf(fp,
            "local_ub_node_id: 1\n"
            "warm_regions:\n"
            "  - region_id: 901\n"
            "    provider: shm\n"
            "    path: %s\n"
            "    mmap_offset: 0\n"
            "    bytes: %u\n"
            "    value_size: %u\n"
            "    home_ub_node_id: 1\n"
            "    is_local: true\n"
            "    weight: 1\n",
            local0,
            (unsigned)(sizeof(float) * dim * slots),
            (unsigned)(sizeof(float) * dim));
    fclose(fp);

    assert(vemb_v16_parse_warm_regions_manifest(manifest_path,
                                                dim * sizeof(float),
                                                &manifest) == 0);
    assert(vemb_v16_storage_ctx_create_from_manifest(&storage,
                                                     dim,
                                                     dim * sizeof(float),
                                                     max_vectors,
                                                     &manifest) == 0);
    assert(storage->warm_region_count == 1);
    assert(storage->tlc->warm_region_count == 1);
    assert(vemb_v16_tlc_find_region(storage->tlc, 901) != NULL);
    assert(vemb_v16_tlc_find_region(storage->tlc, 902) == NULL);

    memset(&req, 0, sizeof(req));
    req.flags = VEMB_V16_PEER_VIEW_MAP_F_ATTACH_NOW;
    req.expected_local_owner_id = 1;
    req.expected_local_owner_valid = 1;
    req.region_count = 1;
    req.regions[0].region_id = 902;
    req.regions[0].backend_type = VEMB_V16_REGION_LOCAL_SHM;
    req.regions[0].home_ub_node_id = 1;
    req.regions[0].weight = 1;
    req.regions[0].value_size = dim * sizeof(float);
    req.regions[0].region_bytes = sizeof(float) * dim * slots;
    snprintf(req.regions[0].path,
             sizeof(req.regions[0].path),
             "%s",
             local1);

    assert(vemb_v16_storage_store_peer_view_map(storage, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_STATUS_OK);
    assert(resp.applied_region_count == 1);
    assert(storage->warm_region_count == 2);
    assert(vemb_v16_tlc_find_region(storage->tlc, 902) != NULL);

    {
        vemb_v16_channel_desc_t desc;
        memset(&desc, 0, sizeof(desc));
        vemb_v16_storage_fill_channel_desc(storage, &desc);
        assert(desc.warm_region_count == 2);
        assert(desc.warm_regions[0].region_id == 901);
        assert(desc.warm_regions[1].region_id == 902);
    }

    fill_vector(vector, dim, 222);
    assert(vemb_v16_tlc_put(storage->tlc,
                            key,
                            (uint32_t)strlen(key),
                            key_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);
    assert(handle.region_id == 901 || handle.region_id == 902);

    vemb_v16_storage_ctx_destroy(storage);
    unlink(manifest_path);
    cleanup_region_and_layout(local0, 901);
    cleanup_region_and_layout(local1, 902);
}

static void test_peer_view_map_attach_ub_rpc_peer_keeps_lookup_registered(void) {
    enum { dim = 2, max_vectors = 4, slots = 2 };
    char manifest_path[128];
    char local0[64];
    char req0_1[64], resp1_0[64], req1_0[64], resp0_1[64];
    char req0_2[64], resp2_0[64], req2_0[64], resp0_2[64];
    vemb_v16_storage_ctx_t *storage = NULL;
    vemb_v16_warm_regions_manifest_t manifest;
    vemb_v16_peer_view_map_req_t req;
    vemb_v16_peer_view_map_resp_t resp;

    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_manifest_%ld_peer_view_rpc.yaml",
             (long)getpid());
    snprintf(local0, sizeof(local0), "/v16pvrpc_local0_%ld", (long)getpid());
    snprintf(req0_1, sizeof(req0_1), "/v16pvrpc_req0_1_%ld", (long)getpid());
    snprintf(resp1_0, sizeof(resp1_0), "/v16pvrpc_resp1_0_%ld", (long)getpid());
    snprintf(req1_0, sizeof(req1_0), "/v16pvrpc_req1_0_%ld", (long)getpid());
    snprintf(resp0_1, sizeof(resp0_1), "/v16pvrpc_resp0_1_%ld", (long)getpid());
    snprintf(req0_2, sizeof(req0_2), "/v16pvrpc_req0_2_%ld", (long)getpid());
    snprintf(resp2_0, sizeof(resp2_0), "/v16pvrpc_resp2_0_%ld", (long)getpid());
    snprintf(req2_0, sizeof(req2_0), "/v16pvrpc_req2_0_%ld", (long)getpid());
    snprintf(resp0_2, sizeof(resp0_2), "/v16pvrpc_resp0_2_%ld", (long)getpid());

    cleanup_region_and_layout(local0, 911);
    cleanup_shm_path(req0_1);
    cleanup_shm_path(resp1_0);
    cleanup_shm_path(req1_0);
    cleanup_shm_path(resp0_1);
    cleanup_shm_path(req0_2);
    cleanup_shm_path(resp2_0);
    cleanup_shm_path(req2_0);
    cleanup_shm_path(resp0_2);

    FILE *fp = fopen(manifest_path, "w");
    assert(fp != NULL);
    fprintf(fp,
            "local_ub_node_id: 0\n"
            "ub_rpc_timeout_ms: 123\n"
            "warm_regions:\n"
            "  - region_id: 911\n"
            "    provider: shm\n"
            "    path: %s\n"
            "    mmap_offset: 0\n"
            "    bytes: %u\n"
            "    value_size: %u\n"
            "    home_ub_node_id: 0\n"
            "    is_local: true\n"
            "    weight: 1\n"
            "ub_rpc_peers:\n"
            "  - owner_id: 1\n"
            "    provider: shm\n"
            "    request_path: %s\n"
            "    response_path: %s\n"
            "    inbound_request_path: %s\n"
            "    outbound_response_path: %s\n",
            local0,
            (unsigned)(sizeof(float) * dim * slots),
            (unsigned)(sizeof(float) * dim),
            req0_1,
            resp1_0,
            req1_0,
            resp0_1);
    fclose(fp);

    assert(vemb_v16_parse_warm_regions_manifest(manifest_path,
                                                dim * sizeof(float),
                                                &manifest) == 0);
    assert(vemb_v16_storage_ctx_create_from_manifest(&storage,
                                                     dim,
                                                     dim * sizeof(float),
                                                     max_vectors,
                                                     &manifest) == 0);
    assert(storage->ub_rpc != NULL);
    assert(storage->tlc->lookup_rpc == vemb_v16_ub_rpc_lookup);
    assert(storage->tlc->lookup_rpc_arg == storage->tlc);
    assert(storage->tlc->lookup_rpc_runtime == storage->ub_rpc);
    assert(vemb_v16_ub_rpc_has_peer(storage->ub_rpc, 1) == 1);
    assert(vemb_v16_ub_rpc_has_peer(storage->ub_rpc, 2) == 0);

    memset(&req, 0, sizeof(req));
    req.flags = VEMB_V16_PEER_VIEW_MAP_F_ATTACH_NOW;
    req.expected_local_owner_id = 0;
    req.expected_local_owner_valid = 1;
    req.ub_rpc_timeout_ms = 456;
    req.ub_rpc_peer_count = 1;
    req.ub_rpc_peers[0].owner_id = 2;
    req.ub_rpc_peers[0].request.backend_type = VEMB_V16_REGION_LOCAL_SHM;
    req.ub_rpc_peers[0].response.backend_type = VEMB_V16_REGION_LOCAL_SHM;
    req.ub_rpc_peers[0].inbound_request.backend_type = VEMB_V16_REGION_LOCAL_SHM;
    req.ub_rpc_peers[0].outbound_response.backend_type = VEMB_V16_REGION_LOCAL_SHM;
    snprintf(req.ub_rpc_peers[0].request.path,
             sizeof(req.ub_rpc_peers[0].request.path),
             "%s",
             req0_2);
    snprintf(req.ub_rpc_peers[0].response.path,
             sizeof(req.ub_rpc_peers[0].response.path),
             "%s",
             resp2_0);
    snprintf(req.ub_rpc_peers[0].inbound_request.path,
             sizeof(req.ub_rpc_peers[0].inbound_request.path),
             "%s",
             req2_0);
    snprintf(req.ub_rpc_peers[0].outbound_response.path,
             sizeof(req.ub_rpc_peers[0].outbound_response.path),
             "%s",
             resp0_2);

    assert(vemb_v16_storage_store_peer_view_map(storage, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_STATUS_OK);
    assert(resp.applied_ub_rpc_peer_count == 1);
    assert(storage->ub_rpc_timeout_ms == 456);
    assert(storage->tlc->lookup_rpc == vemb_v16_ub_rpc_lookup);
    assert(storage->tlc->lookup_rpc_arg == storage->tlc);
    assert(storage->tlc->lookup_rpc_runtime == storage->ub_rpc);
    assert(vemb_v16_ub_rpc_has_peer(storage->ub_rpc, 1) == 1);
    assert(vemb_v16_ub_rpc_has_peer(storage->ub_rpc, 2) == 1);

    vemb_v16_storage_ctx_destroy(storage);
    assert(vemb_v16_storage_reset_manifest_regions(&manifest) == 0);
    unlink(manifest_path);
    cleanup_region_and_layout(local0, 911);
    cleanup_shm_path(req0_1);
    cleanup_shm_path(resp1_0);
    cleanup_shm_path(req1_0);
    cleanup_shm_path(resp0_1);
    cleanup_shm_path(req0_2);
    cleanup_shm_path(resp2_0);
    cleanup_shm_path(req2_0);
    cleanup_shm_path(resp0_2);
}

int main(void) {
    monotonicInit();

    test_shm_provider_attaches_existing_payload();
    test_manifest_shm_mock_ub_create_and_put();
    test_manifest_remote_meta_shm_attach_existing();
    test_manifest_remote_meta_owner_views_route_key2();
    test_manifest_ub_rpc_peer_parse_and_storage_init();
    test_storage_reset_clears_payload_and_layout();
    test_peer_view_map_can_attach_local_region();
    test_peer_view_map_attach_ub_rpc_peer_keeps_lookup_registered();
    printf("vemb_v16_manifest_ut: all tests passed\n");
    return 0;
}
