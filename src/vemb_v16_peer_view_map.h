#ifndef __VEMB_V16_PEER_VIEW_MAP_H
#define __VEMB_V16_PEER_VIEW_MAP_H

#include <stdint.h>

#define VEMB_V16_PEER_VIEW_MAP_MAX_REGIONS 16u
#define VEMB_V16_PEER_VIEW_MAP_MAX_REMOTE_META_VIEWS 16u
#define VEMB_V16_PEER_VIEW_MAP_MAX_UB_RPC_PEERS 16u

#define VEMB_V16_PEER_VIEW_MAP_F_ATTACH_NOW 0x01u

typedef struct vemb_v16_peer_view_ring_desc {
    uint32_t backend_type;
    uint32_t cache_policy;
    uint64_t mmap_offset;
    char path[256];
} vemb_v16_peer_view_ring_desc_t;

typedef struct vemb_v16_peer_view_region_desc {
    uint32_t region_id;
    uint32_t backend_type;
    uint32_t home_ub_node_id;
    uint32_t weight;
    uint32_t value_size;
    uint32_t cache_policy;
    uint64_t mmap_offset;
    uint64_t region_bytes;
    char path[256];
} vemb_v16_peer_view_region_desc_t;

typedef struct vemb_v16_peer_view_remote_meta_desc {
    uint32_t owner_id;
    uint32_t backend_type;
    uint32_t entry_count;
    uint32_t bucket_count;
    uint32_t set_count;
    uint32_t ways;
    uint32_t cache_policy;
    uint64_t mmap_offset;
    char path[256];
} vemb_v16_peer_view_remote_meta_desc_t;

typedef struct vemb_v16_peer_view_ub_rpc_peer_desc {
    uint32_t owner_id;
    vemb_v16_peer_view_ring_desc_t request;
    vemb_v16_peer_view_ring_desc_t response;
    vemb_v16_peer_view_ring_desc_t inbound_request;
    vemb_v16_peer_view_ring_desc_t outbound_response;
} vemb_v16_peer_view_ub_rpc_peer_desc_t;

typedef struct vemb_v16_peer_view_map_req {
    uint32_t flags;
    uint32_t expected_local_owner_id;
    uint32_t expected_local_owner_valid;
    uint32_t ub_rpc_timeout_ms;
    uint32_t region_count;
    uint32_t remote_meta_view_count;
    uint32_t ub_rpc_peer_count;
    vemb_v16_peer_view_region_desc_t
        regions[VEMB_V16_PEER_VIEW_MAP_MAX_REGIONS];
    vemb_v16_peer_view_remote_meta_desc_t
        remote_meta_views[VEMB_V16_PEER_VIEW_MAP_MAX_REMOTE_META_VIEWS];
    vemb_v16_peer_view_ub_rpc_peer_desc_t
        ub_rpc_peers[VEMB_V16_PEER_VIEW_MAP_MAX_UB_RPC_PEERS];
} vemb_v16_peer_view_map_req_t;

typedef struct vemb_v16_peer_view_map_resp {
    uint8_t status;
    uint8_t reserved0[3];
    uint32_t applied_region_count;
    uint32_t applied_remote_meta_view_count;
    uint32_t applied_ub_rpc_peer_count;
} vemb_v16_peer_view_map_resp_t;

#endif
