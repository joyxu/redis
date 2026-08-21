#include "../clients/c/vemb_v16_ub_peer_view.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static void test_fixed_peer_view_manifest(void) {
    char path[128];
    snprintf(path, sizeof(path), "/tmp/vemb_v16_peer_view_%ld.yaml",
             (long)getpid());
    FILE *fp = fopen(path, "w");
    assert(fp != NULL);
    fprintf(fp,
            "version: 1\n"
            "peer_views:\n"
            "  - client_host: 111\n"
            "    owner_id: 1\n"
            "    resource_role: v1_request_ring\n"
            "    resource_id: owner112-v1-request\n"
            "    generation: 1\n"
            "    provider_path: /dev/obmm_shmdev3\n"
            "    client_path: /dev/obmm_shmdev7\n"
            "    map_from_start: true\n"
            "    cache_policy: noncacheable\n"
            "  - client_host: 111\n"
            "    owner_id: 1\n"
            "    resource_role: v1_response_ring\n"
            "    resource_id: owner112-v1-response\n"
            "    generation: 1\n"
            "    provider_path: /dev/obmm_shmdev6\n"
            "    client_path: /dev/obmm_shmdev2\n"
            "    map_from_start: true\n"
            "    cache_policy: cacheable\n"
            "  - client_host: 111\n"
            "    owner_id: 1\n"
            "    resource_role: warm_region\n"
            "    resource_id: owner112-warm\n"
            "    generation: 1\n"
            "    provider_path: /dev/obmm_shmdev4\n"
            "    client_path: /dev/obmm_shmdev8\n"
            "    map_from_start: true\n"
            "    cache_policy: noncacheable\n");
    assert(fclose(fp) == 0);

    vemb_v16_ub_peer_view_manifest_t manifest;
    assert(vemb_v16_ub_peer_view_manifest_load(path, &manifest) == 0);
    assert(manifest.entry_count == 3);

    vemb_v16_ub_peer_view_mapping_t mapping;
    assert(vemb_v16_ub_peer_view_manifest_resolve(
               &manifest, "111", 1,
               VEMB_V16_UB_PEER_VIEW_V1_REQUEST_RING,
               "/dev/obmm_shmdev3", 0, &mapping) == 0);
    assert(!strcmp(mapping.resource_id, "owner112-v1-request"));
    assert(!strcmp(mapping.client_path, "/dev/obmm_shmdev7"));
    assert(mapping.generation == 1);
    assert(mapping.map_flags & VEMB_V16_UB_PEER_VIEW_MAP_F_FROM_START);
    assert(mapping.cache_policy == VEMB_V16_UB_PEER_VIEW_NONCACHEABLE);

    assert(vemb_v16_ub_peer_view_manifest_resolve(
               &manifest, "111", 1,
               VEMB_V16_UB_PEER_VIEW_V1_RESPONSE_RING,
               "/dev/obmm_shmdev6", 1, &mapping) == 0);
    assert(mapping.cache_policy == VEMB_V16_UB_PEER_VIEW_CACHEABLE);
    assert(vemb_v16_ub_peer_view_manifest_resolve(
               &manifest, "111", 1,
               VEMB_V16_UB_PEER_VIEW_WARM_REGION,
               "/dev/obmm_shmdev4", 0, &mapping) == 0);
    assert(!strcmp(mapping.client_path, "/dev/obmm_shmdev8"));
    assert(mapping.cache_policy == VEMB_V16_UB_PEER_VIEW_NONCACHEABLE);
    assert(vemb_v16_ub_peer_view_manifest_resolve(
               &manifest, "111", 1,
               VEMB_V16_UB_PEER_VIEW_V1_RESPONSE_RING,
               "/dev/obmm_shmdev6", 2, &mapping) != 0);
    assert(vemb_v16_ub_peer_view_manifest_resolve(
               &manifest, "112", 1,
               VEMB_V16_UB_PEER_VIEW_V1_RESPONSE_RING,
               "/dev/obmm_shmdev6", 0, &mapping) != 0);
    assert(vemb_v16_ub_peer_view_manifest_resolve(
               &manifest, "111", 111,
               VEMB_V16_UB_PEER_VIEW_V1_RESPONSE_RING,
               "/dev/obmm_shmdev6", 0, &mapping) != 0);
    assert(vemb_v16_ub_peer_view_manifest_resolve(
               &manifest, "111", 1,
               VEMB_V16_UB_PEER_VIEW_WARM_REGION,
               "/dev/obmm_shmdev6", 0, &mapping) != 0);
    unlink(path);
}

static void test_invalid_manifest_is_rejected(void) {
    char path[128];
    snprintf(path, sizeof(path), "/tmp/vemb_v16_peer_view_bad_%ld.yaml",
             (long)getpid());
    FILE *fp = fopen(path, "w");
    assert(fp != NULL);
    fprintf(fp,
            "version: 1\n"
            "peer_views:\n"
            "  - client_host: 111\n"
            "    owner_id: 1\n"
            "    resource_role: v1_request_ring\n"
            "    resource_id: missing-client-path\n"
            "    generation: 1\n"
            "    provider_path: /dev/obmm_shmdev3\n"
            "    map_from_start: true\n");
    assert(fclose(fp) == 0);

    vemb_v16_ub_peer_view_manifest_t manifest;
    assert(vemb_v16_ub_peer_view_manifest_load(path, &manifest) != 0);
    unlink(path);
}

static void test_role_policies_are_path_driven(void) {
    char path[128];
    snprintf(path, sizeof(path), "/tmp/vemb_v16_peer_view_policy_%ld.yaml",
             (long)getpid());
    FILE *fp = fopen(path, "w");
    assert(fp != NULL);
    fprintf(fp,
            "version: 1\n"
            "peer_views:\n"
            "  - client_host: 111\n"
            "    owner_id: 1\n"
            "    resource_role: v1_request_ring\n"
            "    resource_id: owner112-v1-request\n"
            "    generation: 1\n"
            "    provider_path: /dev/obmm_shmdev3\n"
            "    client_path: /dev/obmm_shmdev7\n"
            "    map_from_start: true\n"
            "    cache_policy: cacheable\n");
    assert(fclose(fp) == 0);

    vemb_v16_ub_peer_view_manifest_t manifest;
    assert(vemb_v16_ub_peer_view_manifest_load(path, &manifest) == 0);
    unlink(path);

    fp = fopen(path, "w");
    assert(fp != NULL);
    fprintf(fp,
            "version: 1\n"
            "peer_views:\n"
            "  - client_host: 111\n"
            "    owner_id: 1\n"
            "    resource_role: warm_region\n"
            "    resource_id: owner112-warm\n"
            "    generation: 1\n"
            "    provider_path: /dev/obmm_shmdev4\n"
            "    client_path: /dev/obmm_shmdev8\n"
            "    map_from_start: true\n"
            "    cache_policy: cacheable\n");
    assert(fclose(fp) == 0);
    assert(vemb_v16_ub_peer_view_manifest_load(path, &manifest) == 0);
    unlink(path);
}

int main(void) {
    test_fixed_peer_view_manifest();
    test_invalid_manifest_is_rejected();
    test_role_policies_are_path_driven();
    printf("vemb_v16_ub_peer_view_ut: all tests passed\n");
    return 0;
}
