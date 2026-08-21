#ifndef __VEMB_V16_UB_PEER_VIEW_H
#define __VEMB_V16_UB_PEER_VIEW_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define VEMB_V16_UB_PEER_VIEW_MAX_ENTRIES 32u
#define VEMB_V16_UB_PEER_VIEW_HOST_MAX 64u
#define VEMB_V16_UB_PEER_VIEW_RESOURCE_ID_MAX 64u
#define VEMB_V16_UB_PEER_VIEW_PATH_MAX 256u

typedef enum vemb_v16_ub_peer_view_resource_role {
    VEMB_V16_UB_PEER_VIEW_V1_REQUEST_RING = 1,
    VEMB_V16_UB_PEER_VIEW_V1_RESPONSE_RING = 2,
    VEMB_V16_UB_PEER_VIEW_WARM_REGION = 3,
    VEMB_V16_UB_PEER_VIEW_V2_REQUEST_DESCRIPTOR = 4,
    VEMB_V16_UB_PEER_VIEW_V2_REQUEST_ARENA = 5,
    VEMB_V16_UB_PEER_VIEW_V2_RESPONSE_DESCRIPTOR = 6,
    VEMB_V16_UB_PEER_VIEW_V2_RESPONSE_ARENA = 7,
} vemb_v16_ub_peer_view_resource_role_t;

typedef enum vemb_v16_ub_peer_view_map_flags {
    /* Map from device offset zero before applying ATTACH's resource offset. */
    VEMB_V16_UB_PEER_VIEW_MAP_F_FROM_START = 1u << 0,
} vemb_v16_ub_peer_view_map_flags_t;

typedef enum vemb_v16_ub_peer_view_cache_policy {
    VEMB_V16_UB_PEER_VIEW_CACHEABLE = 1,
    VEMB_V16_UB_PEER_VIEW_NONCACHEABLE = 2,
} vemb_v16_ub_peer_view_cache_policy_t;

typedef struct vemb_v16_ub_peer_view_entry {
    char client_host[VEMB_V16_UB_PEER_VIEW_HOST_MAX];
    uint32_t owner_id;
    uint32_t resource_role;
    char resource_id[VEMB_V16_UB_PEER_VIEW_RESOURCE_ID_MAX];
    uint64_t generation;
    char provider_path[VEMB_V16_UB_PEER_VIEW_PATH_MAX];
    char client_path[VEMB_V16_UB_PEER_VIEW_PATH_MAX];
    uint32_t map_flags;
    uint32_t cache_policy;
} vemb_v16_ub_peer_view_entry_t;

typedef struct vemb_v16_ub_peer_view_manifest {
    uint32_t entry_count;
    vemb_v16_ub_peer_view_entry_t
        entries[VEMB_V16_UB_PEER_VIEW_MAX_ENTRIES];
} vemb_v16_ub_peer_view_manifest_t;

typedef struct vemb_v16_ub_peer_view_mapping {
    char client_path[VEMB_V16_UB_PEER_VIEW_PATH_MAX];
    char resource_id[VEMB_V16_UB_PEER_VIEW_RESOURCE_ID_MAX];
    uint64_t generation;
    uint32_t map_flags;
    uint32_t cache_policy;
    /* Provider and client use the same local UB device. */
    uint8_t local_path;
} vemb_v16_ub_peer_view_mapping_t;

/* Load the fixed deployment manifest. The parser intentionally accepts only
 * the peer_views schema documented in VEMB_V16_TCP_UB_CLUSTER_SCALEOUT_TRANSPORT_DESIGN.md. */
int vemb_v16_ub_peer_view_manifest_load(
    const char *path, vemb_v16_ub_peer_view_manifest_t *manifest);

/* Resolve one ATTACH-advertised provider resource. resource_generation is
 * zero until ATTACH publishes a resource generation; a nonzero value must
 * exactly match the fixed manifest generation. */
int vemb_v16_ub_peer_view_manifest_resolve(
    const vemb_v16_ub_peer_view_manifest_t *manifest,
    const char *client_host, uint32_t owner_id,
    vemb_v16_ub_peer_view_resource_role_t resource_role,
    const char *provider_path, uint64_t resource_generation,
    vemb_v16_ub_peer_view_mapping_t *mapping);

#ifdef __cplusplus
}
#endif

#endif
