#ifndef __VEMB_V16_PROTOCOL_H
#define __VEMB_V16_PROTOCOL_H

#include "macro.h"
#include "vemb_v16_cacheline.h"
#include "vemb_v16_hash.h"
#include "vemb_v16_peer_view_map.h"
#include "vemb_v16_util.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define VEMB_V16_MAGIC 0x56313645u
#define VEMB_V16_VERSION 1u

#define VEMB_V16_UDS_PATH "/tmp/vemb_v16.sock"
#define VEMB_V16_TCP_HOST "127.0.0.1"
#define VEMB_V16_TCP_PORT 6391
#define VEMB_V16_SHM_PREFIX "vemb_v16"
#define VEMB_V16_DEFAULT_AERON_UB_PATH "/dev/obmm_shmdev1"
#define VEMB_V16_DEFAULT_AERON_RESPONSE_UB_PATH "/dev/obmm_shmdev2"
#define VEMB_V16_DEFAULT_VECTOR_REGION "/vemb_v16_vectors"

#ifndef VEMB_V16_MAX_CHANNELS
#define VEMB_V16_MAX_CHANNELS 8192
#endif

#define VEMB_V16_MAX_KEY_LEN 128
#define VEMB_V16_MAX_DIM 4096
#define VEMB_V16_DEFAULT_DIM 300
#define VEMB_V16_DEFAULT_MAX_VECTORS 131072
#define VEMB_V16_ALLOC_REQ_ENCODED_LEN 8u
#define VEMB_V16_NET_STATUS_ENCODED_LEN 9u
#define VEMB_V16_EPOCH_CONTROL_REQ_ENCODED_LEN 20u
#define VEMB_V16_EPOCH_CONTROL_RESP_ENCODED_LEN 17u
#define VEMB_V16_TOPOLOGY_ENDPOINT_ENCODED_LEN 184u
#define VEMB_V16_SCALEOUT_LOCAL_DONE_REQ_ENCODED_LEN 56u
#define VEMB_V16_SCALEOUT_LOCAL_DONE_RESP_ENCODED_LEN 33u
#define VEMB_V16_MIGRATION_CONTROL_RESP_ENCODED_LEN 73u
#define VEMB_V16_MIGRATION_RANGE_CONTROL_REQ_ENCODED_LEN 32u
#define VEMB_V16_MIGRATION_RANGE_CONTROL_RESP_ENCODED_LEN 105u
#define VEMB_V16_RESP_ENCODED_BASE_LEN 6u
#define VEMB_V16_AERON_REQ_WIRE_MAX_LEN \
    (24u + 4u + VEMB_V16_MAX_KEY_LEN + VEMB_V16_MAX_DIM * sizeof(float))
#define VEMB_V16_AERON_RESP_WIRE_MAX_LEN 64u
#define vemb_v16_alloc_req_encoded_len() VEMB_V16_ALLOC_REQ_ENCODED_LEN
#define vemb_v16_net_status_encoded_len() VEMB_V16_NET_STATUS_ENCODED_LEN
#define vemb_v16_epoch_control_req_encoded_len() \
    VEMB_V16_EPOCH_CONTROL_REQ_ENCODED_LEN
#define vemb_v16_epoch_control_resp_encoded_len() \
    VEMB_V16_EPOCH_CONTROL_RESP_ENCODED_LEN
#define vemb_v16_topology_endpoint_encoded_len() \
    VEMB_V16_TOPOLOGY_ENDPOINT_ENCODED_LEN
#define vemb_v16_scaleout_local_done_req_encoded_len() \
    VEMB_V16_SCALEOUT_LOCAL_DONE_REQ_ENCODED_LEN
#define vemb_v16_scaleout_local_done_resp_encoded_len() \
    VEMB_V16_SCALEOUT_LOCAL_DONE_RESP_ENCODED_LEN
#define vemb_v16_migration_control_resp_encoded_len() \
    VEMB_V16_MIGRATION_CONTROL_RESP_ENCODED_LEN
#define vemb_v16_migration_range_control_req_encoded_len() \
    VEMB_V16_MIGRATION_RANGE_CONTROL_REQ_ENCODED_LEN
#define vemb_v16_migration_range_control_resp_encoded_len() \
    VEMB_V16_MIGRATION_RANGE_CONTROL_RESP_ENCODED_LEN
#define vemb_v16_resp_encoded_base_len() VEMB_V16_RESP_ENCODED_BASE_LEN
#define VEMB_V16_MAX_DESC_WARM_REGIONS 16u
#define VEMB_V16_MAX_DESC_UB_RPC_PEERS 16u
#define VEMB_V16_MIGRATION_CONTROL_MAX_BATCH 16u
#define VEMB_V16_MIGRATION_CONTROL_MAX_RANGE_KEYS 64u
#define VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS 64u
#define VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS 64u
#define VEMB_V16_TOPOLOGY_ENDPOINT_HOST_LEN 64u
#define VEMB_V16_TOPOLOGY_ENDPOINT_PATH_LEN 108u

#define VEMB_V16_STATUS_OK 0u
#define VEMB_V16_STATUS_NOT_FOUND 1u
#define VEMB_V16_STATUS_ERR 2u
#define VEMB_V16_STATUS_STALE_TOPOLOGY 3u
#define VEMB_V16_STATUS_MOVED 4u
#define VEMB_V16_STATUS_ASK 5u

#define VEMB_V16_REGION_LOCAL_SHM 1u
#define VEMB_V16_REGION_UB 2u

#define VEMB_V16_TRANSPORT_AERON 1u
#define VEMB_V16_TRANSPORT_TCP 2u

#define VEMB_V16_REQ_F_ASK_REDIRECT 0x02u
#define VEMB_V16_TCP_REQ_OP_MASK 0x3fu
#define VEMB_V16_TCP_REQ_FLAG_MASK 0xc0u
#define VEMB_V16_TCP_REQ_WIRE_F_ASK_REDIRECT 0x40u
#define VEMB_V16_TCP_RESP_OP_MASK 0x3fu
#define VEMB_V16_TCP_RESP_FLAG_MASK 0xc0u
#define VEMB_V16_TOPOLOGY_CONTROL_F_PUBLISHED 0x01u
#define VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED 0x02u
#define VEMB_V16_TOPOLOGY_CONTROL_F_AUTO_SCALEOUT 0x04u
#define VEMB_V16_TOPOLOGY_CONTROL_F_COORDINATED_SCALEOUT 0x08u

enum vemb_v16_ctrl_op {
    VEMB_V16_CTRL_PING = 0x01,
    VEMB_V16_CTRL_ALLOC_CHANNEL = 0x20,
    VEMB_V16_CTRL_CLOSE_CHANNEL = 0x21,
    VEMB_V16_CTRL_STATS = 0x22,
    VEMB_V16_CTRL_CLOSE_ALL_CHANNELS = 0x23,
    VEMB_V16_CTRL_MIGRATION_MARK_MIGRATING = 0x30,
    VEMB_V16_CTRL_MIGRATION_MARK_CUTOVER = 0x31,
    VEMB_V16_CTRL_MIGRATION_MARK_MIGRATING_BATCH = 0x32,
    VEMB_V16_CTRL_MIGRATION_BARRIER = 0x33,
    VEMB_V16_CTRL_MIGRATION_RANGE_BARRIER = 0x34,
    VEMB_V16_CTRL_MIGRATION_RANGE_MARK_CUTOVER = 0x35,
    VEMB_V16_CTRL_MIGRATION_MARK_SOURCE_GC = 0x36,
    VEMB_V16_CTRL_MIGRATION_RANGE_SOURCE_GC = 0x37,
    VEMB_V16_CTRL_EPOCH_SET = 0x40,
    VEMB_V16_CTRL_EPOCH_GET = 0x41,
    VEMB_V16_CTRL_TOPOLOGY_SET = 0x42,
    VEMB_V16_CTRL_TOPOLOGY_GET = 0x43,
    VEMB_V16_CTRL_SCALEOUT_LOCAL_DONE = 0x44,
    VEMB_V16_CTRL_PEER_VIEW_MAP_APPLY = 0x45,
    VEMB_V16_CTRL_PEER_VIEW_MAP_TOPOLOGY_SET = 0x46,
};

enum vemb_v16_net_frame_type {
    VEMB_V16_NET_HELLO = 0x01,
    VEMB_V16_NET_WELCOME = 0x02,
    VEMB_V16_NET_REQUEST = 0x03,
    VEMB_V16_NET_RESPONSE = 0x04,
    VEMB_V16_NET_CLOSE = 0x05,
    VEMB_V16_NET_STATS = 0x06,
    VEMB_V16_NET_CLOSE_CHANNEL = 0x07,
    VEMB_V16_NET_CLOSE_ALL_CHANNELS = 0x08,
    VEMB_V16_NET_CONTROL_STATUS = 0x09,
    VEMB_V16_NET_MIGRATION_MARK_MIGRATING = 0x0a,
    VEMB_V16_NET_MIGRATION_MARK_CUTOVER = 0x0b,
    VEMB_V16_NET_MIGRATION_CONTROL_RESPONSE = 0x0c,
    VEMB_V16_NET_MIGRATION_MARK_MIGRATING_BATCH = 0x0d,
    VEMB_V16_NET_MIGRATION_CONTROL_BATCH_RESPONSE = 0x0e,
    VEMB_V16_NET_EPOCH_SET = 0x0f,
    VEMB_V16_NET_EPOCH_GET = 0x10,
    VEMB_V16_NET_EPOCH_CONTROL_RESPONSE = 0x11,
    VEMB_V16_NET_TOPOLOGY_SET = 0x12,
    VEMB_V16_NET_TOPOLOGY_GET = 0x13,
    VEMB_V16_NET_TOPOLOGY_RESPONSE = 0x14,
    VEMB_V16_NET_MIGRATION_BARRIER = 0x15,
    VEMB_V16_NET_MIGRATION_RANGE_BARRIER = 0x16,
    VEMB_V16_NET_MIGRATION_RANGE_MARK_CUTOVER = 0x17,
    VEMB_V16_NET_MIGRATION_RANGE_CONTROL_RESPONSE = 0x18,
    VEMB_V16_NET_MIGRATION_MARK_SOURCE_GC = 0x19,
    VEMB_V16_NET_MIGRATION_RANGE_SOURCE_GC = 0x1a,
    VEMB_V16_NET_SCALEOUT_LOCAL_DONE = 0x1b,
    VEMB_V16_NET_SCALEOUT_LOCAL_DONE_RESPONSE = 0x1c,
    VEMB_V16_NET_PEER_VIEW_MAP_APPLY = 0x1d,
    VEMB_V16_NET_PEER_VIEW_MAP_RESPONSE = 0x1e,
    VEMB_V16_NET_PEER_VIEW_MAP_TOPOLOGY_SET = 0x1f,
    VEMB_V16_NET_PEER_VIEW_MAP_TOPOLOGY_RESPONSE = 0x20,
    VEMB_V16_NET_ALLOC_AERON_CHANNEL = 0x21,
};

enum vemb_v16_data_op {
    VEMB_V16_OP_PING = 0x01,
    VEMB_V16_OP_VADD = 0x10,
    VEMB_V16_OP_VREM = 0x11,
    VEMB_V16_OP_VEMB_HANDLE = 0x20,
    VEMB_V16_OP_VEMB_INLINE = 0x21,
    VEMB_V16_OP_VSIM_INLINE = 0x30,
    VEMB_V16_OP_VSIM_KEY_KEY = 0x31,
};

enum vemb_v16_ub_lookup_rpc_op {
    VEMB_V16_UB_LOOKUP_RPC_LOOKUP_HANDLE = 0x01,
};

enum vemb_v16_ub_lookup_rpc_status {
    VEMB_V16_UB_LOOKUP_RPC_OK = 0,
    VEMB_V16_UB_LOOKUP_RPC_NOT_FOUND = 1,
    VEMB_V16_UB_LOOKUP_RPC_BUSY = 2,
    VEMB_V16_UB_LOOKUP_RPC_ERROR = 3,
    VEMB_V16_UB_LOOKUP_RPC_TIMEOUT = 4,
};

enum vemb_v16_ub_lookup_rpc_kind {
    VEMB_V16_UB_LOOKUP_RPC_KIND_NONE = 0,
    VEMB_V16_UB_LOOKUP_RPC_KIND_HANDLE = 1,
    VEMB_V16_UB_LOOKUP_RPC_KIND_SNAPSHOT = 2,
};

enum vemb_v16_ub_rpc_frame_kind {
    VEMB_V16_UB_RPC_FRAME_LOOKUP = 1,
    VEMB_V16_UB_RPC_FRAME_MIGRATION = 2,
};

enum vemb_v16_ub_migration_rpc_op {
    VEMB_V16_UB_MIGRATION_RPC_SNAPSHOT_REQ = 0x01,
    VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT = 0x02,
    VEMB_V16_UB_MIGRATION_RPC_DELTA_DELETE = 0x03,
    VEMB_V16_UB_MIGRATION_RPC_DELTA_ACK = 0x04,
    VEMB_V16_UB_MIGRATION_RPC_BARRIER_REQ = 0x05,
    VEMB_V16_UB_MIGRATION_RPC_BARRIER_RESP = 0x06,
    VEMB_V16_UB_MIGRATION_RPC_LEASE_COMMIT_REQ = 0x07,
    VEMB_V16_UB_MIGRATION_RPC_LEASE_COMMIT_RESP = 0x08,
    VEMB_V16_UB_MIGRATION_RPC_BASELINE_PUT = 0x09,
};

enum vemb_v16_ub_migration_rpc_status {
    VEMB_V16_UB_MIGRATION_RPC_OK = 0,
    VEMB_V16_UB_MIGRATION_RPC_NOT_FOUND = 1,
    VEMB_V16_UB_MIGRATION_RPC_ERROR = 2,
    VEMB_V16_UB_MIGRATION_RPC_TIMEOUT = 3,
    VEMB_V16_UB_MIGRATION_RPC_BUSY = 4,
    VEMB_V16_UB_MIGRATION_RPC_DUPLICATE = 5,
    VEMB_V16_UB_MIGRATION_RPC_STALE_REJECTED = 6,
    VEMB_V16_UB_MIGRATION_RPC_RETRY = 7,
};

typedef struct vemb_v16_alloc_req {
    uint32_t vector_dim;
    uint32_t flags;
} vemb_v16_alloc_req_t;

typedef struct vemb_v16_channel_desc {
    uint32_t magic;
    uint32_t version;
    uint64_t channel_id;
    uint32_t channel_index;
    uint32_t vector_dim;
    uint32_t vector_stride;
    uint32_t max_vectors;
    uint32_t request_ring_slot_size;
    uint32_t response_ring_slot_size;
    uint32_t warm_region_id;
    uint32_t warm_backend_type;
    uint64_t warm_region_bytes;
    uint64_t warm_mmap_offset;
    uint32_t local_owner_id;
    uint32_t remote_meta_backend_type;
    uint64_t remote_meta_mmap_offset;
    uint32_t remote_meta_entry_count;
    uint32_t remote_meta_bucket_count;
    uint32_t remote_meta_set_count;
    uint32_t remote_meta_ways;
    uint32_t ub_rpc_timeout_ms;
    char request_ring_name[64];
    char response_ring_name[64];
    char vector_region_name[256];
    char remote_meta_path[256];
    uint32_t warm_region_count;
    uint32_t ub_rpc_peer_count;
    struct {
        uint32_t region_id;
        uint32_t backend_type;
        uint64_t region_bytes;
        uint64_t mmap_offset;
        char path[256];
    } warm_regions[VEMB_V16_MAX_DESC_WARM_REGIONS];
    struct {
        uint32_t owner_id;
        uint32_t request_backend_type;
        uint64_t request_mmap_offset;
        char request_path[256];
        uint32_t response_backend_type;
        uint64_t response_mmap_offset;
        char response_path[256];
        uint32_t inbound_request_backend_type;
        uint64_t inbound_request_mmap_offset;
        char inbound_request_path[256];
        uint32_t outbound_response_backend_type;
        uint64_t outbound_response_mmap_offset;
        char outbound_response_path[256];
    } ub_rpc_peers[VEMB_V16_MAX_DESC_UB_RPC_PEERS];
} vemb_v16_channel_desc_t;

typedef struct vemb_v16_net_hdr {
    uint32_t magic;
    uint16_t version;
    uint16_t type;
    uint32_t flags;
    uint32_t payload_len;
    uint64_t channel_id;
    uint32_t req_id;
    uint32_t reserved;
} vemb_v16_net_hdr_t;

typedef struct vemb_v16_net_status {
    uint8_t status;
    uint8_t reserved0[7];
    uint64_t value;
} vemb_v16_net_status_t;

typedef struct vemb_v16_epoch_control_req {
    uint64_t current_topology_epoch;
    uint64_t min_write_epoch;
    uint32_t flags;
    uint32_t reserved0;
} vemb_v16_epoch_control_req_t;

typedef struct vemb_v16_epoch_control_resp {
    uint8_t status;
    uint8_t reserved0[7];
    uint64_t current_topology_epoch;
    uint64_t min_write_epoch;
} vemb_v16_epoch_control_resp_t;

typedef struct vemb_v16_topology_endpoint {
    uint32_t owner_id;
    uint32_t transport_type;
    uint16_t tcp_port;
    uint16_t reserved0;
    char host[VEMB_V16_TOPOLOGY_ENDPOINT_HOST_LEN];
    char uds_path[VEMB_V16_TOPOLOGY_ENDPOINT_PATH_LEN];
} vemb_v16_topology_endpoint_t;

typedef struct vemb_v16_topology_control_req {
    uint64_t current_topology_epoch;
    uint64_t min_write_epoch;
    uint32_t active_owner_count;
    uint32_t standby_owner_count;
    uint32_t vnode_count;
    uint32_t flags;
    uint32_t active_owners[VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS];
    uint32_t standby_owners[VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS];
    uint32_t endpoint_count;
    uint32_t coordinator_endpoint_valid;
    vemb_v16_topology_endpoint_t coordinator_endpoint;
    vemb_v16_topology_endpoint_t
        endpoints[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];
} vemb_v16_topology_control_req_t;

typedef struct vemb_v16_topology_control_resp {
    uint8_t status;
    uint8_t reserved0[7];
    uint64_t current_topology_epoch;
    uint64_t min_write_epoch;
    uint32_t active_owner_count;
    uint32_t standby_owner_count;
    uint32_t vnode_count;
    uint32_t flags;
    uint32_t active_owners[VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS];
    uint32_t standby_owners[VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS];
    uint32_t endpoint_count;
    uint32_t coordinator_endpoint_valid;
    vemb_v16_topology_endpoint_t coordinator_endpoint;
    vemb_v16_topology_endpoint_t
        endpoints[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];
} vemb_v16_topology_control_resp_t;

typedef struct vemb_v16_peer_view_topology_control_req {
    vemb_v16_peer_view_map_req_t peer_view_map_req;
    vemb_v16_topology_control_req_t topology_req;
} vemb_v16_peer_view_topology_control_req_t;

// TODO: reduce the status
typedef struct vemb_v16_peer_view_topology_control_resp {
    uint8_t status;
    uint8_t peer_view_map_status;
    uint8_t topology_status;
    uint8_t topology_attempted;
    vemb_v16_peer_view_map_resp_t peer_view_map_resp;
    vemb_v16_topology_control_resp_t topology_resp;
} vemb_v16_peer_view_topology_control_resp_t;

typedef struct vemb_v16_scaleout_local_done_req {
    uint64_t migration_topology_epoch;
    uint64_t cutover_topology_epoch;
    uint64_t notify_seq;
    uint32_t source_owner;
    uint32_t phase;
    uint32_t error_code;
    uint32_t pending_delta;
    uint32_t baseline_retry_pending;
    uint32_t migrating_key_count;
    uint32_t range_count;
    uint32_t flags;
    uint32_t reserved0;
} vemb_v16_scaleout_local_done_req_t;

typedef struct vemb_v16_scaleout_local_done_resp {
    uint8_t status;
    uint8_t reserved0[7];
    uint64_t migration_topology_epoch;
    uint64_t cutover_topology_epoch;
    uint64_t notify_seq;
    uint32_t source_owner;
    uint32_t reserved1;
} vemb_v16_scaleout_local_done_resp_t;

typedef struct vemb_v16_migration_control_req {
    uint64_t key_hash;
    uint64_t topology_epoch;
    uint32_t key_len;
    uint32_t target_owner;
    uint32_t shard_id;
    uint32_t reserved1;
    char key[VEMB_V16_MAX_KEY_LEN];
} vemb_v16_migration_control_req_t;

typedef struct vemb_v16_migration_control_resp {
    uint8_t status;
    uint8_t reserved0[7];
    uint64_t key_hash;
    uint64_t key_version;
    uint64_t topology_epoch;
    uint64_t owner_epoch;
    uint64_t applied_seq;
    uint64_t barrier_seq;
    uint32_t migration_state;
    uint32_t target_owner;
    uint32_t tombstone;
    uint32_t pending_delta;
    uint32_t outbox_state;
    uint32_t shard_id;
} vemb_v16_migration_control_resp_t;

typedef struct vemb_v16_migration_control_batch_req {
    uint32_t entry_count;
    uint32_t reserved0;
    vemb_v16_migration_control_req_t
        entries[VEMB_V16_MIGRATION_CONTROL_MAX_BATCH];
} vemb_v16_migration_control_batch_req_t;

typedef struct vemb_v16_migration_control_batch_resp {
    uint8_t status;
    uint8_t reserved0[7];
    uint32_t entry_count;
    uint32_t success_count;
    uint32_t error_count;
    uint32_t reserved1;
    vemb_v16_migration_control_resp_t
        entries[VEMB_V16_MIGRATION_CONTROL_MAX_BATCH];
} vemb_v16_migration_control_batch_resp_t;

typedef struct vemb_v16_migration_range_control_req {
    uint64_t migration_topology_epoch;
    uint64_t cutover_topology_epoch;
    uint32_t target_owner;
    uint32_t shard_id;
    uint32_t flags;
    uint32_t page_limit;
} vemb_v16_migration_range_control_req_t;

typedef struct vemb_v16_migration_range_control_resp {
    uint8_t status;
    uint8_t reserved0[7];
    uint64_t migration_topology_epoch;
    uint64_t cutover_topology_epoch;
    uint64_t owner_epoch;
    uint64_t applied_seq;
    uint64_t barrier_seq;
    uint64_t source_seq;
    uint64_t retry_delta;
    uint32_t target_owner;
    uint32_t shard_id;
    uint32_t key_count;
    uint32_t success_count;
    uint32_t error_count;
    uint32_t pending_delta;
    uint32_t outbox_state;
    uint32_t remaining_keys;
    uint32_t page_key_count;
    uint32_t range_done;
    uint32_t range_ready;
    uint32_t page_limit;
    uint32_t reserved1;
} vemb_v16_migration_range_control_resp_t;

typedef struct vemb_v16_req {
    uint8_t op;
    uint8_t flags;
    uint16_t reserved0;
    uint32_t req_id;
    uint64_t channel_id;
    uint64_t key_hash;
    uint32_t key_len;
    uint32_t key2_len;
    uint64_t key2_hash;
    uint64_t topology_epoch;
    uint32_t dim;
    uint32_t vector_bytes;
    uint32_t reserved1;
    char key[VEMB_V16_MAX_KEY_LEN];
    char key2[VEMB_V16_MAX_KEY_LEN];
    float vector[VEMB_V16_MAX_DIM];
} vemb_v16_req_t;

typedef struct vemb_v16_resp {
    uint8_t status;
    uint8_t op;
    uint16_t flags;
    uint32_t req_id;
    uint64_t key_hash;
    uint64_t vector_offset;
    uint32_t vector_bytes;
    uint32_t dim;
    uint32_t region_id;
    uint32_t local_slot;
    uint64_t owner_generation;
    uint32_t redirect_owner;
    uint32_t reserved2;
    float score;
} vemb_v16_resp_t;

typedef struct vemb_v16_ub_lookup_rpc_req {
    uint64_t request_id;
    uint32_t src_owner_id;
    uint32_t dst_owner_id;
    uint32_t op;
    uint32_t flags;
    uint64_t key_hash;
    uint32_t key_len;
    uint32_t timeout_ns;
    char key[VEMB_V16_MAX_KEY_LEN];
} vemb_v16_ub_lookup_rpc_req_t;

typedef struct vemb_v16_ub_lookup_rpc_resp {
    uint64_t request_id;
    uint32_t status;
    uint32_t kind;
    uint64_t key_hash;
    uint32_t region_id;
    uint32_t local_slot;
    uint64_t offset;
    uint32_t bytes;
    uint32_t snapshot_bytes;
    uint64_t owner_generation;
} vemb_v16_ub_lookup_rpc_resp_t;

typedef struct vemb_v16_ub_migration_delta_desc {
    uint64_t key_hash;
    uint64_t key_version;
    uint64_t topology_epoch;
    uint64_t owner_epoch;
    uint64_t delta_seq;
    uint32_t op;
    uint32_t flags;
    uint32_t shard_id;
    uint32_t key_len;
    uint32_t source_owner;
    uint32_t target_owner;
    uint32_t tombstone;
    uint32_t value_size;
    uint32_t region_id;
    uint32_t local_slot;
    uint32_t bytes;
    uint32_t reserved0;
    uint64_t offset;
    uint64_t owner_generation;
    char key[VEMB_V16_MAX_KEY_LEN];
} vemb_v16_ub_migration_delta_desc_t;

typedef struct vemb_v16_ub_migration_delta_ack_desc {
    uint64_t topology_epoch;
    uint64_t applied_seq;
    uint64_t barrier_seq;
    uint32_t source_owner;
    uint32_t target_owner;
    uint32_t shard_id;
    uint32_t status;
} vemb_v16_ub_migration_delta_ack_desc_t;

typedef struct vemb_v16_ub_migration_barrier_desc {
    uint64_t topology_epoch;
    uint64_t barrier_seq;
    uint32_t source_owner;
    uint32_t target_owner;
    uint32_t shard_id;
    uint32_t flags;
} vemb_v16_ub_migration_barrier_desc_t;

typedef struct vemb_v16_ub_migration_lease_desc {
    uint64_t topology_epoch;
    uint64_t owner_epoch;
    uint32_t source_owner;
    uint32_t target_owner;
    uint32_t shard_id;
    uint32_t flags;
} vemb_v16_ub_migration_lease_desc_t;

typedef struct vemb_v16_ub_migration_snapshot_desc {
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
    uint32_t region_id;
    uint32_t local_slot;
    uint32_t bytes;
    uint32_t shard_id;
    uint64_t offset;
    uint64_t owner_generation;
    char key[VEMB_V16_MAX_KEY_LEN];
} vemb_v16_ub_migration_snapshot_desc_t;

typedef struct vemb_v16_ub_migration_rpc_req {
    uint64_t request_id;
    uint32_t src_owner_id;
    uint32_t dst_owner_id;
    uint32_t op;
    uint32_t flags;
    uint64_t key_hash;
    uint64_t topology_epoch;
    uint32_t key_len;
    uint32_t timeout_ns;
    uint32_t target_owner_id;
    uint32_t reserved0;
    char key[VEMB_V16_MAX_KEY_LEN];
    vemb_v16_ub_migration_delta_desc_t delta;
    vemb_v16_ub_migration_delta_ack_desc_t delta_ack;
    vemb_v16_ub_migration_barrier_desc_t barrier;
    vemb_v16_ub_migration_lease_desc_t lease;
    vemb_v16_ub_migration_snapshot_desc_t snapshot;
} vemb_v16_ub_migration_rpc_req_t;

typedef struct vemb_v16_ub_migration_rpc_resp {
    uint64_t request_id;
    uint32_t status;
    uint32_t op;
    uint64_t key_hash;
    uint64_t topology_epoch;
    uint64_t key_version;
    uint32_t source_owner_id;
    uint32_t target_owner_id;
    vemb_v16_ub_migration_snapshot_desc_t snapshot;
    vemb_v16_ub_migration_delta_ack_desc_t delta_ack;
    vemb_v16_ub_migration_barrier_desc_t barrier;
    vemb_v16_ub_migration_lease_desc_t lease;
} vemb_v16_ub_migration_rpc_resp_t;

typedef struct vemb_v16_stats {
    uint64_t total_requests;
    uint64_t vadd_requests;
    uint64_t vemb_requests;
    uint64_t vsim_requests;
    uint64_t not_found;
    uint64_t published_jobs;
    uint64_t completed_jobs;
    uint64_t active_channels;
    uint64_t proxy_vemb_ring_full;
    uint64_t proxy_vadd_ring_full;
    uint64_t read_pool_alloc_ok;
    uint64_t read_pool_alloc_fail;
    uint64_t read_pool_inuse_peak;
    uint64_t read_pool_free_min;
    uint64_t proxy_response_ring_full;
    uint64_t supernode_completion_publish;
    uint64_t supernode_completion_ring_full;
    uint64_t bitmap_lock_success;
    uint64_t bitmap_lock_failure;
    uint64_t request_ring_depth;
    uint64_t response_ring_depth;
    uint64_t job_shard_queue_depth;
    uint64_t reserved_shard_queue_depth;
    uint64_t completion_ring_depth;
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
    uint64_t remote_meta_lookup_hit;
    uint64_t remote_meta_lookup_miss;
    uint64_t remote_meta_lookup_busy;
    uint64_t remote_meta_lookup_way_probe;
    uint64_t remote_meta_lookup_set_conflict;
    uint64_t remote_meta_publish_async_enqueue;
    uint64_t remote_meta_publish_async_drop;
    uint64_t remote_meta_publish_async_coalesce;
    uint64_t remote_meta_publish_ok;
    uint64_t remote_meta_publish_busy;
    uint64_t remote_meta_publish_insert;
    uint64_t remote_meta_publish_update;
    uint64_t remote_meta_publish_evict;
    uint64_t remote_meta_publish_ns;
    uint64_t ub_lookup_rpc_count;
    uint64_t ub_lookup_rpc_ok;
    uint64_t ub_lookup_rpc_not_found;
    uint64_t ub_lookup_rpc_busy;
    uint64_t ub_lookup_rpc_timeout;
    uint64_t ub_lookup_rpc_error;
    uint64_t ub_lookup_rpc_handle;
    uint64_t ub_lookup_rpc_snapshot;
    uint64_t ub_lookup_rpc_ns;
    uint64_t remote_meta_repair_enqueue;
    uint64_t remote_meta_repair_ok;
    uint64_t remote_meta_repair_drop;
    uint64_t warm_region_hash_local_pct;
    uint64_t moved_count;
    uint64_t stale_count;
    uint64_t ask_count;
    uint64_t forward_count;
    uint64_t duplicate_request_count;
    uint64_t source_gc_count;
    uint64_t gc_safe_watermark;
    uint64_t migration_baseline_sent;
    uint64_t migration_baseline_skipped;
    uint64_t migration_baseline_error;
    uint64_t migration_baseline_retry_queued;
    uint64_t migration_baseline_retry_sent;
    uint64_t migration_baseline_retry_pending;
} vemb_v16_stats_t;

static inline size_t vemb_v16_req_handle_len(void) {
    return offsetof(vemb_v16_req_t, vector);
}

static inline size_t vemb_v16_req_inline_len(uint32_t vector_bytes) {
    return offsetof(vemb_v16_req_t, vector) + (size_t)vector_bytes;
}

/* Aeron slots carry the compact protocol frame, not the internal request
 * struct. The largest frame is VADD/VSIM_INLINE with a maximum-length key. */
static inline uint32_t vemb_v16_aeron_req_slot_size(uint32_t dim) {
    size_t payload = 24u + 4u + VEMB_V16_MAX_KEY_LEN +
        (size_t)dim * sizeof(float);
    return (uint32_t)align_up_size(payload, CACHELINE_SIZE);
}

static inline void vemb_v16_proto_put_u8(uint8_t **p, uint8_t v) {
    *(*p)++ = v;
}

static inline void vemb_v16_proto_put_u16(uint8_t **p, uint16_t v) {
    uint8_t *dst = *p;
    dst[0] = (uint8_t)(v >> 8);
    dst[1] = (uint8_t)v;
    *p = dst + 2;
}

static inline void vemb_v16_proto_put_u32(uint8_t **p, uint32_t v) {
    uint8_t *dst = *p;
    dst[0] = (uint8_t)(v >> 24);
    dst[1] = (uint8_t)(v >> 16);
    dst[2] = (uint8_t)(v >> 8);
    dst[3] = (uint8_t)v;
    *p = dst + 4;
}

static inline void vemb_v16_proto_put_u64(uint8_t **p, uint64_t v) {
    uint8_t *dst = *p;
    dst[0] = (uint8_t)(v >> 56);
    dst[1] = (uint8_t)(v >> 48);
    dst[2] = (uint8_t)(v >> 40);
    dst[3] = (uint8_t)(v >> 32);
    dst[4] = (uint8_t)(v >> 24);
    dst[5] = (uint8_t)(v >> 16);
    dst[6] = (uint8_t)(v >> 8);
    dst[7] = (uint8_t)v;
    *p = dst + 8;
}

static inline uint16_t vemb_v16_proto_get_u16(const uint8_t **p) {
    const uint8_t *src = *p;
    uint16_t v = (uint16_t)(((uint16_t)src[0] << 8) | (uint16_t)src[1]);
    *p = src + 2;
    return v;
}

static inline uint8_t vemb_v16_proto_get_u8(const uint8_t **p) {
    return *(*p)++;
}

static inline uint32_t vemb_v16_proto_get_u32(const uint8_t **p) {
    const uint8_t *src = *p;
    uint32_t v = ((uint32_t)src[0] << 24) |
                 ((uint32_t)src[1] << 16) |
                 ((uint32_t)src[2] << 8) |
                 (uint32_t)src[3];
    *p = src + 4;
    return v;
}

static inline uint64_t vemb_v16_proto_get_u64(const uint8_t **p) {
    const uint8_t *src = *p;
    uint64_t v = ((uint64_t)src[0] << 56) |
                 ((uint64_t)src[1] << 48) |
                 ((uint64_t)src[2] << 40) |
                 ((uint64_t)src[3] << 32) |
                 ((uint64_t)src[4] << 24) |
                 ((uint64_t)src[5] << 16) |
                 ((uint64_t)src[6] << 8) |
                 (uint64_t)src[7];
    *p = src + 8;
    return v;
}

static inline void vemb_v16_proto_put_bytes(uint8_t **p,
                                            const void *src,
                                            size_t len) {
    if (len != 0)
        memcpy(*p, src, len);
    *p += len;
}

static inline void vemb_v16_proto_get_bytes(const uint8_t **p,
                                            void *dst,
                                            size_t len) {
    if (len != 0)
        memcpy(dst, *p, len);
    *p += len;
}

static inline void vemb_v16_proto_put_f32(uint8_t **p, float v) {
    uint32_t bits = 0;
    memcpy(&bits, &v, sizeof(bits));
    vemb_v16_proto_put_u32(p, bits);
}

static inline float vemb_v16_proto_get_f32(const uint8_t **p) {
    uint32_t bits = vemb_v16_proto_get_u32(p);
    float v = 0;
    memcpy(&v, &bits, sizeof(v));
    return v;
}

static inline int vemb_v16_alloc_req_encode(uint8_t *dst,
                                            size_t cap,
                                            const vemb_v16_alloc_req_t *req,
                                            size_t *out_len) {
    RETURN_IF(cap < VEMB_V16_ALLOC_REQ_ENCODED_LEN, -1);
    uint8_t *p = dst;
    vemb_v16_proto_put_u32(&p, req->vector_dim);
    vemb_v16_proto_put_u32(&p, req->flags);
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_alloc_req_decode(vemb_v16_alloc_req_t *req,
                                            const uint8_t *src,
                                            size_t len) {
    RETURN_IF(len != VEMB_V16_ALLOC_REQ_ENCODED_LEN, -1);
    const uint8_t *p = src;
    memset(req, 0, sizeof(*req));
    req->vector_dim = vemb_v16_proto_get_u32(&p);
    req->flags = vemb_v16_proto_get_u32(&p);
    return 0;
}

static inline size_t vemb_v16_channel_desc_encoded_len(
    const vemb_v16_channel_desc_t *desc) {
    uint32_t warm_region_count = desc->warm_region_count;
    uint32_t ub_rpc_peer_count = desc->ub_rpc_peer_count;
    if (warm_region_count > VEMB_V16_MAX_DESC_WARM_REGIONS)
        warm_region_count = VEMB_V16_MAX_DESC_WARM_REGIONS;
    if (ub_rpc_peer_count > VEMB_V16_MAX_DESC_UB_RPC_PEERS)
        ub_rpc_peer_count = VEMB_V16_MAX_DESC_UB_RPC_PEERS;
    return 748u +
           (size_t)warm_region_count * 280u +
           (size_t)ub_rpc_peer_count * 1076u;
}

static inline int vemb_v16_channel_desc_encode(
    uint8_t *dst,
    size_t cap,
    const vemb_v16_channel_desc_t *desc,
    size_t *out_len) {
    size_t need = vemb_v16_channel_desc_encoded_len(desc);
    RETURN_IF(cap < need, -1);
    uint8_t *p = dst;
    uint32_t warm_region_count = desc->warm_region_count;
    uint32_t ub_rpc_peer_count = desc->ub_rpc_peer_count;
    if (warm_region_count > VEMB_V16_MAX_DESC_WARM_REGIONS)
        warm_region_count = VEMB_V16_MAX_DESC_WARM_REGIONS;
    if (ub_rpc_peer_count > VEMB_V16_MAX_DESC_UB_RPC_PEERS)
        ub_rpc_peer_count = VEMB_V16_MAX_DESC_UB_RPC_PEERS;
    vemb_v16_proto_put_u32(&p, desc->magic);
    vemb_v16_proto_put_u32(&p, desc->version);
    vemb_v16_proto_put_u64(&p, desc->channel_id);
    vemb_v16_proto_put_u32(&p, desc->channel_index);
    vemb_v16_proto_put_u32(&p, desc->vector_dim);
    vemb_v16_proto_put_u32(&p, desc->vector_stride);
    vemb_v16_proto_put_u32(&p, desc->max_vectors);
    vemb_v16_proto_put_u32(&p, desc->request_ring_slot_size);
    vemb_v16_proto_put_u32(&p, desc->response_ring_slot_size);
    vemb_v16_proto_put_u32(&p, desc->warm_region_id);
    vemb_v16_proto_put_u32(&p, desc->warm_backend_type);
    vemb_v16_proto_put_u64(&p, desc->warm_region_bytes);
    vemb_v16_proto_put_u64(&p, desc->warm_mmap_offset);
    vemb_v16_proto_put_u32(&p, desc->local_owner_id);
    vemb_v16_proto_put_u32(&p, desc->remote_meta_backend_type);
    vemb_v16_proto_put_u64(&p, desc->remote_meta_mmap_offset);
    vemb_v16_proto_put_u32(&p, desc->remote_meta_entry_count);
    vemb_v16_proto_put_u32(&p, desc->remote_meta_bucket_count);
    vemb_v16_proto_put_u32(&p, desc->remote_meta_set_count);
    vemb_v16_proto_put_u32(&p, desc->remote_meta_ways);
    vemb_v16_proto_put_u32(&p, desc->ub_rpc_timeout_ms);
    vemb_v16_proto_put_bytes(&p,
                             desc->request_ring_name,
                             sizeof(desc->request_ring_name));
    vemb_v16_proto_put_bytes(&p,
                             desc->response_ring_name,
                             sizeof(desc->response_ring_name));
    vemb_v16_proto_put_bytes(&p,
                             desc->vector_region_name,
                             sizeof(desc->vector_region_name));
    vemb_v16_proto_put_bytes(&p,
                             desc->remote_meta_path,
                             sizeof(desc->remote_meta_path));
    vemb_v16_proto_put_u32(&p, warm_region_count);
    vemb_v16_proto_put_u32(&p, ub_rpc_peer_count);
    for (uint32_t i = 0; i < warm_region_count; i++) {
        vemb_v16_proto_put_u32(&p, desc->warm_regions[i].region_id);
        vemb_v16_proto_put_u32(&p, desc->warm_regions[i].backend_type);
        vemb_v16_proto_put_u64(&p, desc->warm_regions[i].region_bytes);
        vemb_v16_proto_put_u64(&p, desc->warm_regions[i].mmap_offset);
        vemb_v16_proto_put_bytes(&p,
                                 desc->warm_regions[i].path,
                                 sizeof(desc->warm_regions[i].path));
    }
    for (uint32_t i = 0; i < ub_rpc_peer_count; i++) {
        vemb_v16_proto_put_u32(&p, desc->ub_rpc_peers[i].owner_id);
        vemb_v16_proto_put_u32(&p, desc->ub_rpc_peers[i].request_backend_type);
        vemb_v16_proto_put_u64(&p, desc->ub_rpc_peers[i].request_mmap_offset);
        vemb_v16_proto_put_bytes(&p, desc->ub_rpc_peers[i].request_path,
                                 sizeof(desc->ub_rpc_peers[i].request_path));
        vemb_v16_proto_put_u32(&p, desc->ub_rpc_peers[i].response_backend_type);
        vemb_v16_proto_put_u64(&p, desc->ub_rpc_peers[i].response_mmap_offset);
        vemb_v16_proto_put_bytes(&p, desc->ub_rpc_peers[i].response_path,
                                 sizeof(desc->ub_rpc_peers[i].response_path));
        vemb_v16_proto_put_u32(&p, desc->ub_rpc_peers[i].inbound_request_backend_type);
        vemb_v16_proto_put_u64(&p, desc->ub_rpc_peers[i].inbound_request_mmap_offset);
        vemb_v16_proto_put_bytes(&p, desc->ub_rpc_peers[i].inbound_request_path,
                                 sizeof(desc->ub_rpc_peers[i].inbound_request_path));
        vemb_v16_proto_put_u32(&p, desc->ub_rpc_peers[i].outbound_response_backend_type);
        vemb_v16_proto_put_u64(&p, desc->ub_rpc_peers[i].outbound_response_mmap_offset);
        vemb_v16_proto_put_bytes(&p, desc->ub_rpc_peers[i].outbound_response_path,
                                 sizeof(desc->ub_rpc_peers[i].outbound_response_path));
    }
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_channel_desc_decode(vemb_v16_channel_desc_t *desc,
                                               const uint8_t *src,
                                               size_t len) {
    if (len < 748u)
        return -1;
    const uint8_t *p = src;
    memset(desc, 0, sizeof(*desc));
    desc->magic = vemb_v16_proto_get_u32(&p);
    desc->version = vemb_v16_proto_get_u32(&p);
    desc->channel_id = vemb_v16_proto_get_u64(&p);
    desc->channel_index = vemb_v16_proto_get_u32(&p);
    desc->vector_dim = vemb_v16_proto_get_u32(&p);
    desc->vector_stride = vemb_v16_proto_get_u32(&p);
    desc->max_vectors = vemb_v16_proto_get_u32(&p);
    desc->request_ring_slot_size = vemb_v16_proto_get_u32(&p);
    desc->response_ring_slot_size = vemb_v16_proto_get_u32(&p);
    desc->warm_region_id = vemb_v16_proto_get_u32(&p);
    desc->warm_backend_type = vemb_v16_proto_get_u32(&p);
    desc->warm_region_bytes = vemb_v16_proto_get_u64(&p);
    desc->warm_mmap_offset = vemb_v16_proto_get_u64(&p);
    desc->local_owner_id = vemb_v16_proto_get_u32(&p);
    desc->remote_meta_backend_type = vemb_v16_proto_get_u32(&p);
    desc->remote_meta_mmap_offset = vemb_v16_proto_get_u64(&p);
    desc->remote_meta_entry_count = vemb_v16_proto_get_u32(&p);
    desc->remote_meta_bucket_count = vemb_v16_proto_get_u32(&p);
    desc->remote_meta_set_count = vemb_v16_proto_get_u32(&p);
    desc->remote_meta_ways = vemb_v16_proto_get_u32(&p);
    desc->ub_rpc_timeout_ms = vemb_v16_proto_get_u32(&p);
    vemb_v16_proto_get_bytes(&p,
                             desc->request_ring_name,
                             sizeof(desc->request_ring_name));
    vemb_v16_proto_get_bytes(&p,
                             desc->response_ring_name,
                             sizeof(desc->response_ring_name));
    vemb_v16_proto_get_bytes(&p,
                             desc->vector_region_name,
                             sizeof(desc->vector_region_name));
    vemb_v16_proto_get_bytes(&p,
                             desc->remote_meta_path,
                             sizeof(desc->remote_meta_path));
    desc->warm_region_count = vemb_v16_proto_get_u32(&p);
    desc->ub_rpc_peer_count = vemb_v16_proto_get_u32(&p);
    if (desc->warm_region_count > VEMB_V16_MAX_DESC_WARM_REGIONS)
        return -1;
    if (desc->ub_rpc_peer_count > VEMB_V16_MAX_DESC_UB_RPC_PEERS)
        return -1;
    if (len != vemb_v16_channel_desc_encoded_len(desc))
        return -1;
    for (uint32_t i = 0; i < desc->warm_region_count; i++) {
        desc->warm_regions[i].region_id = vemb_v16_proto_get_u32(&p);
        desc->warm_regions[i].backend_type = vemb_v16_proto_get_u32(&p);
        desc->warm_regions[i].region_bytes = vemb_v16_proto_get_u64(&p);
        desc->warm_regions[i].mmap_offset = vemb_v16_proto_get_u64(&p);
        vemb_v16_proto_get_bytes(&p,
                                 desc->warm_regions[i].path,
                                 sizeof(desc->warm_regions[i].path));
    }
    for (uint32_t i = 0; i < desc->ub_rpc_peer_count; i++) {
        desc->ub_rpc_peers[i].owner_id = vemb_v16_proto_get_u32(&p);
        desc->ub_rpc_peers[i].request_backend_type = vemb_v16_proto_get_u32(&p);
        desc->ub_rpc_peers[i].request_mmap_offset = vemb_v16_proto_get_u64(&p);
        vemb_v16_proto_get_bytes(&p, desc->ub_rpc_peers[i].request_path,
                                 sizeof(desc->ub_rpc_peers[i].request_path));
        desc->ub_rpc_peers[i].response_backend_type = vemb_v16_proto_get_u32(&p);
        desc->ub_rpc_peers[i].response_mmap_offset = vemb_v16_proto_get_u64(&p);
        vemb_v16_proto_get_bytes(&p, desc->ub_rpc_peers[i].response_path,
                                 sizeof(desc->ub_rpc_peers[i].response_path));
        desc->ub_rpc_peers[i].inbound_request_backend_type = vemb_v16_proto_get_u32(&p);
        desc->ub_rpc_peers[i].inbound_request_mmap_offset = vemb_v16_proto_get_u64(&p);
        vemb_v16_proto_get_bytes(&p, desc->ub_rpc_peers[i].inbound_request_path,
                                 sizeof(desc->ub_rpc_peers[i].inbound_request_path));
        desc->ub_rpc_peers[i].outbound_response_backend_type = vemb_v16_proto_get_u32(&p);
        desc->ub_rpc_peers[i].outbound_response_mmap_offset = vemb_v16_proto_get_u64(&p);
        vemb_v16_proto_get_bytes(&p, desc->ub_rpc_peers[i].outbound_response_path,
                                 sizeof(desc->ub_rpc_peers[i].outbound_response_path));
    }
    return 0;
}

static inline int vemb_v16_net_status_encode(uint8_t *dst,
                                             size_t cap,
                                             const vemb_v16_net_status_t *st,
                                             size_t *out_len) {
    RETURN_IF(cap < VEMB_V16_NET_STATUS_ENCODED_LEN, -1);
    uint8_t *p = dst;
    vemb_v16_proto_put_u8(&p, st->status);
    vemb_v16_proto_put_u64(&p, st->value);
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_net_status_decode(vemb_v16_net_status_t *st,
                                             const uint8_t *src,
                                             size_t len) {
    RETURN_IF(len != VEMB_V16_NET_STATUS_ENCODED_LEN, -1);
    const uint8_t *p = src;
    memset(st, 0, sizeof(*st));
    st->status = vemb_v16_proto_get_u8(&p);
    st->value = vemb_v16_proto_get_u64(&p);
    return 0;
}

static inline int vemb_v16_epoch_control_req_encode(
    uint8_t *dst,
    size_t cap,
    const vemb_v16_epoch_control_req_t *req,
    size_t *out_len) {
    RETURN_IF(cap < VEMB_V16_EPOCH_CONTROL_REQ_ENCODED_LEN, -1);
    uint8_t *p = dst;
    vemb_v16_proto_put_u64(&p, req->current_topology_epoch);
    vemb_v16_proto_put_u64(&p, req->min_write_epoch);
    vemb_v16_proto_put_u32(&p, req->flags);
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_epoch_control_req_decode(
    vemb_v16_epoch_control_req_t *req,
    const uint8_t *src,
    size_t len) {
    RETURN_IF(len != VEMB_V16_EPOCH_CONTROL_REQ_ENCODED_LEN, -1);
    const uint8_t *p = src;
    memset(req, 0, sizeof(*req));
    req->current_topology_epoch = vemb_v16_proto_get_u64(&p);
    req->min_write_epoch = vemb_v16_proto_get_u64(&p);
    req->flags = vemb_v16_proto_get_u32(&p);
    return 0;
}

static inline int vemb_v16_epoch_control_resp_encode(
    uint8_t *dst,
    size_t cap,
    const vemb_v16_epoch_control_resp_t *resp,
    size_t *out_len) {
    RETURN_IF(cap < VEMB_V16_EPOCH_CONTROL_RESP_ENCODED_LEN, -1);
    uint8_t *p = dst;
    vemb_v16_proto_put_u8(&p, resp->status);
    vemb_v16_proto_put_u64(&p, resp->current_topology_epoch);
    vemb_v16_proto_put_u64(&p, resp->min_write_epoch);
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_epoch_control_resp_decode(
    vemb_v16_epoch_control_resp_t *resp,
    const uint8_t *src,
    size_t len) {
    RETURN_IF(len != VEMB_V16_EPOCH_CONTROL_RESP_ENCODED_LEN, -1);
    const uint8_t *p = src;
    memset(resp, 0, sizeof(*resp));
    resp->status = vemb_v16_proto_get_u8(&p);
    resp->current_topology_epoch = vemb_v16_proto_get_u64(&p);
    resp->min_write_epoch = vemb_v16_proto_get_u64(&p);
    return 0;
}

static inline int vemb_v16_topology_endpoint_encode(
    uint8_t *dst,
    size_t cap,
    const vemb_v16_topology_endpoint_t *endpoint,
    size_t *out_len) {
    RETURN_IF(cap < VEMB_V16_TOPOLOGY_ENDPOINT_ENCODED_LEN, -1);
    uint8_t *p = dst;
    vemb_v16_proto_put_u32(&p, endpoint->owner_id);
    vemb_v16_proto_put_u32(&p, endpoint->transport_type);
    vemb_v16_proto_put_u16(&p, endpoint->tcp_port);
    vemb_v16_proto_put_u16(&p, endpoint->reserved0);
    vemb_v16_proto_put_bytes(&p, endpoint->host, sizeof(endpoint->host));
    vemb_v16_proto_put_bytes(&p, endpoint->uds_path,
                             sizeof(endpoint->uds_path));
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_topology_endpoint_decode(
    vemb_v16_topology_endpoint_t *endpoint,
    const uint8_t *src,
    size_t len) {
    RETURN_IF(len != VEMB_V16_TOPOLOGY_ENDPOINT_ENCODED_LEN, -1);
    const uint8_t *p = src;
    memset(endpoint, 0, sizeof(*endpoint));
    endpoint->owner_id = vemb_v16_proto_get_u32(&p);
    endpoint->transport_type = vemb_v16_proto_get_u32(&p);
    endpoint->tcp_port = vemb_v16_proto_get_u16(&p);
    endpoint->reserved0 = vemb_v16_proto_get_u16(&p);
    vemb_v16_proto_get_bytes(&p, endpoint->host, sizeof(endpoint->host));
    vemb_v16_proto_get_bytes(&p, endpoint->uds_path,
                             sizeof(endpoint->uds_path));
    return 0;
}

static inline size_t vemb_v16_topology_control_req_encoded_len(
    const vemb_v16_topology_control_req_t *req) {
    RETURN_IF(req->active_owner_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS ||
                  req->standby_owner_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS ||
                  req->endpoint_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS,
              0);
    size_t len = 40u + (size_t)req->active_owner_count * 4u +
                 (size_t)req->standby_owner_count * 4u;
    if (req->coordinator_endpoint_valid)
        len += VEMB_V16_TOPOLOGY_ENDPOINT_ENCODED_LEN;
    len += (size_t)req->endpoint_count *
           VEMB_V16_TOPOLOGY_ENDPOINT_ENCODED_LEN;
    return len;
}

static inline int vemb_v16_topology_control_req_encode(
    uint8_t *dst,
    size_t cap,
    const vemb_v16_topology_control_req_t *req,
    size_t *out_len) {
    size_t need = vemb_v16_topology_control_req_encoded_len(req);
    RETURN_IF(need == 0 || cap < need, -1);
    uint8_t *p = dst;
    vemb_v16_proto_put_u64(&p, req->current_topology_epoch);
    vemb_v16_proto_put_u64(&p, req->min_write_epoch);
    vemb_v16_proto_put_u32(&p, req->active_owner_count);
    vemb_v16_proto_put_u32(&p, req->standby_owner_count);
    vemb_v16_proto_put_u32(&p, req->vnode_count);
    vemb_v16_proto_put_u32(&p, req->flags);
    for (uint32_t i = 0; i < req->active_owner_count; i++)
        vemb_v16_proto_put_u32(&p, req->active_owners[i]);
    for (uint32_t i = 0; i < req->standby_owner_count; i++)
        vemb_v16_proto_put_u32(&p, req->standby_owners[i]);
    vemb_v16_proto_put_u32(&p, req->endpoint_count);
    vemb_v16_proto_put_u32(&p, req->coordinator_endpoint_valid);
    if (req->coordinator_endpoint_valid) {
        size_t endpoint_len = 0;
        RETURN_IF(vemb_v16_topology_endpoint_encode(
                      p,
                      cap - (size_t)(p - dst),
                      &req->coordinator_endpoint,
                      &endpoint_len) != 0,
                  -1);
        p += endpoint_len;
    }
    for (uint32_t i = 0; i < req->endpoint_count; i++) {
        size_t endpoint_len = 0;
        RETURN_IF(vemb_v16_topology_endpoint_encode(
                      p,
                      cap - (size_t)(p - dst),
                      &req->endpoints[i],
                      &endpoint_len) != 0,
                  -1);
        p += endpoint_len;
    }
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_topology_control_req_decode(
    vemb_v16_topology_control_req_t *req,
    const uint8_t *src,
    size_t len) {
    RETURN_IF(len < 40u, -1);
    const uint8_t *p = src;
    memset(req, 0, sizeof(*req));
    req->current_topology_epoch = vemb_v16_proto_get_u64(&p);
    req->min_write_epoch = vemb_v16_proto_get_u64(&p);
    req->active_owner_count = vemb_v16_proto_get_u32(&p);
    req->standby_owner_count = vemb_v16_proto_get_u32(&p);
    req->vnode_count = vemb_v16_proto_get_u32(&p);
    req->flags = vemb_v16_proto_get_u32(&p);
    RETURN_IF(req->active_owner_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS ||
                  req->standby_owner_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS,
              -1);
    for (uint32_t i = 0; i < req->active_owner_count; i++)
        req->active_owners[i] = vemb_v16_proto_get_u32(&p);
    for (uint32_t i = 0; i < req->standby_owner_count; i++)
        req->standby_owners[i] = vemb_v16_proto_get_u32(&p);
    req->endpoint_count = vemb_v16_proto_get_u32(&p);
    req->coordinator_endpoint_valid = vemb_v16_proto_get_u32(&p);
    RETURN_IF(req->endpoint_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS,
              -1);
    if (req->coordinator_endpoint_valid) {
        RETURN_IF(vemb_v16_topology_endpoint_decode(
                      &req->coordinator_endpoint,
                      p,
                      VEMB_V16_TOPOLOGY_ENDPOINT_ENCODED_LEN) != 0,
                  -1);
        p += VEMB_V16_TOPOLOGY_ENDPOINT_ENCODED_LEN;
    }
    for (uint32_t i = 0; i < req->endpoint_count; i++) {
        RETURN_IF(vemb_v16_topology_endpoint_decode(
                      &req->endpoints[i],
                      p,
                      VEMB_V16_TOPOLOGY_ENDPOINT_ENCODED_LEN) != 0,
                  -1);
        p += VEMB_V16_TOPOLOGY_ENDPOINT_ENCODED_LEN;
    }
    RETURN_IF((size_t)(p - src) != len, -1);
    return 0;
}

static inline size_t vemb_v16_topology_control_resp_encoded_len(
    const vemb_v16_topology_control_resp_t *resp) {
    RETURN_IF(resp->active_owner_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS ||
                  resp->standby_owner_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS ||
                  resp->endpoint_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS,
              0);
    size_t len = 41u + (size_t)resp->active_owner_count * 4u +
                 (size_t)resp->standby_owner_count * 4u;
    if (resp->coordinator_endpoint_valid)
        len += VEMB_V16_TOPOLOGY_ENDPOINT_ENCODED_LEN;
    len += (size_t)resp->endpoint_count *
           VEMB_V16_TOPOLOGY_ENDPOINT_ENCODED_LEN;
    return len;
}

static inline int vemb_v16_topology_control_resp_encode(
    uint8_t *dst,
    size_t cap,
    const vemb_v16_topology_control_resp_t *resp,
    size_t *out_len) {
    size_t need = vemb_v16_topology_control_resp_encoded_len(resp);
    RETURN_IF(need == 0 || cap < need, -1);
    uint8_t *p = dst;
    vemb_v16_proto_put_u8(&p, resp->status);
    vemb_v16_proto_put_u64(&p, resp->current_topology_epoch);
    vemb_v16_proto_put_u64(&p, resp->min_write_epoch);
    vemb_v16_proto_put_u32(&p, resp->active_owner_count);
    vemb_v16_proto_put_u32(&p, resp->standby_owner_count);
    vemb_v16_proto_put_u32(&p, resp->vnode_count);
    vemb_v16_proto_put_u32(&p, resp->flags);
    for (uint32_t i = 0; i < resp->active_owner_count; i++)
        vemb_v16_proto_put_u32(&p, resp->active_owners[i]);
    for (uint32_t i = 0; i < resp->standby_owner_count; i++)
        vemb_v16_proto_put_u32(&p, resp->standby_owners[i]);
    vemb_v16_proto_put_u32(&p, resp->endpoint_count);
    vemb_v16_proto_put_u32(&p, resp->coordinator_endpoint_valid);
    if (resp->coordinator_endpoint_valid) {
        size_t endpoint_len = 0;
        RETURN_IF(vemb_v16_topology_endpoint_encode(
                      p,
                      cap - (size_t)(p - dst),
                      &resp->coordinator_endpoint,
                      &endpoint_len) != 0,
                  -1);
        p += endpoint_len;
    }
    for (uint32_t i = 0; i < resp->endpoint_count; i++) {
        size_t endpoint_len = 0;
        RETURN_IF(vemb_v16_topology_endpoint_encode(
                      p,
                      cap - (size_t)(p - dst),
                      &resp->endpoints[i],
                      &endpoint_len) != 0,
                  -1);
        p += endpoint_len;
    }
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_topology_control_resp_decode(
    vemb_v16_topology_control_resp_t *resp,
    const uint8_t *src,
    size_t len) {
    RETURN_IF(len < 41u, -1);
    const uint8_t *p = src;
    memset(resp, 0, sizeof(*resp));
    resp->status = vemb_v16_proto_get_u8(&p);
    resp->current_topology_epoch = vemb_v16_proto_get_u64(&p);
    resp->min_write_epoch = vemb_v16_proto_get_u64(&p);
    resp->active_owner_count = vemb_v16_proto_get_u32(&p);
    resp->standby_owner_count = vemb_v16_proto_get_u32(&p);
    resp->vnode_count = vemb_v16_proto_get_u32(&p);
    resp->flags = vemb_v16_proto_get_u32(&p);
    RETURN_IF(resp->active_owner_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS ||
                  resp->standby_owner_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS,
              -1);
    for (uint32_t i = 0; i < resp->active_owner_count; i++)
        resp->active_owners[i] = vemb_v16_proto_get_u32(&p);
    for (uint32_t i = 0; i < resp->standby_owner_count; i++)
        resp->standby_owners[i] = vemb_v16_proto_get_u32(&p);
    resp->endpoint_count = vemb_v16_proto_get_u32(&p);
    resp->coordinator_endpoint_valid = vemb_v16_proto_get_u32(&p);
    RETURN_IF(resp->endpoint_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS,
              -1);
    if (resp->coordinator_endpoint_valid) {
        RETURN_IF(vemb_v16_topology_endpoint_decode(
                      &resp->coordinator_endpoint,
                      p,
                      VEMB_V16_TOPOLOGY_ENDPOINT_ENCODED_LEN) != 0,
                  -1);
        p += VEMB_V16_TOPOLOGY_ENDPOINT_ENCODED_LEN;
    }
    for (uint32_t i = 0; i < resp->endpoint_count; i++) {
        RETURN_IF(vemb_v16_topology_endpoint_decode(
                      &resp->endpoints[i],
                      p,
                      VEMB_V16_TOPOLOGY_ENDPOINT_ENCODED_LEN) != 0,
                  -1);
        p += VEMB_V16_TOPOLOGY_ENDPOINT_ENCODED_LEN;
    }
    RETURN_IF((size_t)(p - src) != len, -1);
    return 0;
}

static inline int vemb_v16_scaleout_local_done_req_encode(
    uint8_t *dst,
    size_t cap,
    const vemb_v16_scaleout_local_done_req_t *req,
    size_t *out_len) {
    RETURN_IF(cap < VEMB_V16_SCALEOUT_LOCAL_DONE_REQ_ENCODED_LEN, -1);
    uint8_t *p = dst;
    vemb_v16_proto_put_u64(&p, req->migration_topology_epoch);
    vemb_v16_proto_put_u64(&p, req->cutover_topology_epoch);
    vemb_v16_proto_put_u64(&p, req->notify_seq);
    vemb_v16_proto_put_u32(&p, req->source_owner);
    vemb_v16_proto_put_u32(&p, req->phase);
    vemb_v16_proto_put_u32(&p, req->error_code);
    vemb_v16_proto_put_u32(&p, req->pending_delta);
    vemb_v16_proto_put_u32(&p, req->baseline_retry_pending);
    vemb_v16_proto_put_u32(&p, req->migrating_key_count);
    vemb_v16_proto_put_u32(&p, req->range_count);
    vemb_v16_proto_put_u32(&p, req->flags);
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_scaleout_local_done_req_decode(
    vemb_v16_scaleout_local_done_req_t *req,
    const uint8_t *src,
    size_t len) {
    RETURN_IF(len != VEMB_V16_SCALEOUT_LOCAL_DONE_REQ_ENCODED_LEN, -1);
    const uint8_t *p = src;
    memset(req, 0, sizeof(*req));
    req->migration_topology_epoch = vemb_v16_proto_get_u64(&p);
    req->cutover_topology_epoch = vemb_v16_proto_get_u64(&p);
    req->notify_seq = vemb_v16_proto_get_u64(&p);
    req->source_owner = vemb_v16_proto_get_u32(&p);
    req->phase = vemb_v16_proto_get_u32(&p);
    req->error_code = vemb_v16_proto_get_u32(&p);
    req->pending_delta = vemb_v16_proto_get_u32(&p);
    req->baseline_retry_pending = vemb_v16_proto_get_u32(&p);
    req->migrating_key_count = vemb_v16_proto_get_u32(&p);
    req->range_count = vemb_v16_proto_get_u32(&p);
    req->flags = vemb_v16_proto_get_u32(&p);
    return 0;
}

static inline int vemb_v16_scaleout_local_done_resp_encode(
    uint8_t *dst,
    size_t cap,
    const vemb_v16_scaleout_local_done_resp_t *resp,
    size_t *out_len) {
    RETURN_IF(cap < VEMB_V16_SCALEOUT_LOCAL_DONE_RESP_ENCODED_LEN, -1);
    uint8_t *p = dst;
    vemb_v16_proto_put_u8(&p, resp->status);
    vemb_v16_proto_put_u64(&p, resp->migration_topology_epoch);
    vemb_v16_proto_put_u64(&p, resp->cutover_topology_epoch);
    vemb_v16_proto_put_u64(&p, resp->notify_seq);
    vemb_v16_proto_put_u32(&p, resp->source_owner);
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_scaleout_local_done_resp_decode(
    vemb_v16_scaleout_local_done_resp_t *resp,
    const uint8_t *src,
    size_t len) {
    RETURN_IF(len != VEMB_V16_SCALEOUT_LOCAL_DONE_RESP_ENCODED_LEN, -1);
    const uint8_t *p = src;
    memset(resp, 0, sizeof(*resp));
    resp->status = vemb_v16_proto_get_u8(&p);
    resp->migration_topology_epoch = vemb_v16_proto_get_u64(&p);
    resp->cutover_topology_epoch = vemb_v16_proto_get_u64(&p);
    resp->notify_seq = vemb_v16_proto_get_u64(&p);
    resp->source_owner = vemb_v16_proto_get_u32(&p);
    return 0;
}

static inline size_t vemb_v16_migration_control_req_encoded_len(
    const vemb_v16_migration_control_req_t *req) {
    RETURN_IF(req->key_len > VEMB_V16_MAX_KEY_LEN, 0);
    return 28u + (size_t)req->key_len;
}

static inline int vemb_v16_migration_control_req_encode(
    uint8_t *dst,
    size_t cap,
    const vemb_v16_migration_control_req_t *req,
    size_t *out_len) {
    size_t need = vemb_v16_migration_control_req_encoded_len(req);
    RETURN_IF(need == 0 || cap < need, -1);
    uint8_t *p = dst;
    vemb_v16_proto_put_u64(&p, req->key_hash);
    vemb_v16_proto_put_u64(&p, req->topology_epoch);
    vemb_v16_proto_put_u32(&p, req->key_len);
    vemb_v16_proto_put_u32(&p, req->target_owner);
    vemb_v16_proto_put_u32(&p, req->shard_id);
    vemb_v16_proto_put_bytes(&p, req->key, req->key_len);
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_migration_control_req_decode(
    vemb_v16_migration_control_req_t *req,
    const uint8_t *src,
    size_t len) {
    RETURN_IF(len < 28u, -1);
    const uint8_t *p = src;
    memset(req, 0, sizeof(*req));
    req->key_hash = vemb_v16_proto_get_u64(&p);
    req->topology_epoch = vemb_v16_proto_get_u64(&p);
    req->key_len = vemb_v16_proto_get_u32(&p);
    req->target_owner = vemb_v16_proto_get_u32(&p);
    req->shard_id = vemb_v16_proto_get_u32(&p);
    RETURN_IF(req->key_len > VEMB_V16_MAX_KEY_LEN ||
                  len != 28u + (size_t)req->key_len,
              -1);
    vemb_v16_proto_get_bytes(&p, req->key, req->key_len);
    return 0;
}

static inline int vemb_v16_migration_control_resp_encode(
    uint8_t *dst,
    size_t cap,
    const vemb_v16_migration_control_resp_t *resp,
    size_t *out_len) {
    RETURN_IF(cap < VEMB_V16_MIGRATION_CONTROL_RESP_ENCODED_LEN, -1);
    uint8_t *p = dst;
    vemb_v16_proto_put_u8(&p, resp->status);
    vemb_v16_proto_put_u64(&p, resp->key_hash);
    vemb_v16_proto_put_u64(&p, resp->key_version);
    vemb_v16_proto_put_u64(&p, resp->topology_epoch);
    vemb_v16_proto_put_u64(&p, resp->owner_epoch);
    vemb_v16_proto_put_u64(&p, resp->applied_seq);
    vemb_v16_proto_put_u64(&p, resp->barrier_seq);
    vemb_v16_proto_put_u32(&p, resp->migration_state);
    vemb_v16_proto_put_u32(&p, resp->target_owner);
    vemb_v16_proto_put_u32(&p, resp->tombstone);
    vemb_v16_proto_put_u32(&p, resp->pending_delta);
    vemb_v16_proto_put_u32(&p, resp->outbox_state);
    vemb_v16_proto_put_u32(&p, resp->shard_id);
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_migration_control_resp_decode(
    vemb_v16_migration_control_resp_t *resp,
    const uint8_t *src,
    size_t len) {
    RETURN_IF(len != VEMB_V16_MIGRATION_CONTROL_RESP_ENCODED_LEN, -1);
    const uint8_t *p = src;
    memset(resp, 0, sizeof(*resp));
    resp->status = vemb_v16_proto_get_u8(&p);
    resp->key_hash = vemb_v16_proto_get_u64(&p);
    resp->key_version = vemb_v16_proto_get_u64(&p);
    resp->topology_epoch = vemb_v16_proto_get_u64(&p);
    resp->owner_epoch = vemb_v16_proto_get_u64(&p);
    resp->applied_seq = vemb_v16_proto_get_u64(&p);
    resp->barrier_seq = vemb_v16_proto_get_u64(&p);
    resp->migration_state = vemb_v16_proto_get_u32(&p);
    resp->target_owner = vemb_v16_proto_get_u32(&p);
    resp->tombstone = vemb_v16_proto_get_u32(&p);
    resp->pending_delta = vemb_v16_proto_get_u32(&p);
    resp->outbox_state = vemb_v16_proto_get_u32(&p);
    resp->shard_id = vemb_v16_proto_get_u32(&p);
    return 0;
}

static inline size_t vemb_v16_migration_control_batch_req_encoded_len(
    const vemb_v16_migration_control_batch_req_t *req) {
    RETURN_IF(req->entry_count > VEMB_V16_MIGRATION_CONTROL_MAX_BATCH, 0);
    size_t len = 4u;
    for (uint32_t i = 0; i < req->entry_count; i++) {
        size_t entry_len =
            vemb_v16_migration_control_req_encoded_len(&req->entries[i]);
        RETURN_IF(entry_len == 0, 0);
        len += entry_len;
    }
    return len;
}

static inline int vemb_v16_migration_control_batch_req_encode(
    uint8_t *dst,
    size_t cap,
    const vemb_v16_migration_control_batch_req_t *req,
    size_t *out_len) {
    size_t need = vemb_v16_migration_control_batch_req_encoded_len(req);
    RETURN_IF(need == 0 || cap < need, -1);
    uint8_t *p = dst;
    vemb_v16_proto_put_u32(&p, req->entry_count);
    for (uint32_t i = 0; i < req->entry_count; i++) {
        size_t entry_len = 0;
        RETURN_IF(vemb_v16_migration_control_req_encode(
                      p,
                      cap - (size_t)(p - dst),
                      &req->entries[i],
                      &entry_len) != 0,
                  -1);
        p += entry_len;
    }
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_migration_control_batch_req_decode(
    vemb_v16_migration_control_batch_req_t *req,
    const uint8_t *src,
    size_t len) {
    RETURN_IF(len < 4u, -1);
    const uint8_t *p = src;
    memset(req, 0, sizeof(*req));
    req->entry_count = vemb_v16_proto_get_u32(&p);
    RETURN_IF(req->entry_count > VEMB_V16_MIGRATION_CONTROL_MAX_BATCH, -1);
    for (uint32_t i = 0; i < req->entry_count; i++) {
        RETURN_IF(len - (size_t)(p - src) < 28u, -1);
        const uint8_t *entry_src = p;
        req->entries[i].key_hash = vemb_v16_proto_get_u64(&p);
        req->entries[i].topology_epoch = vemb_v16_proto_get_u64(&p);
        req->entries[i].key_len = vemb_v16_proto_get_u32(&p);
        req->entries[i].target_owner = vemb_v16_proto_get_u32(&p);
        req->entries[i].shard_id = vemb_v16_proto_get_u32(&p);
        RETURN_IF(req->entries[i].key_len > VEMB_V16_MAX_KEY_LEN, -1);
        size_t entry_len = 28u + (size_t)req->entries[i].key_len;
        RETURN_IF(len - (size_t)(entry_src - src) < entry_len, -1);
        vemb_v16_proto_get_bytes(&p,
                                 req->entries[i].key,
                                 req->entries[i].key_len);
    }
    RETURN_IF((size_t)(p - src) != len, -1);
    return 0;
}

static inline size_t vemb_v16_migration_control_batch_resp_encoded_len(
    const vemb_v16_migration_control_batch_resp_t *resp) {
    RETURN_IF(resp->entry_count > VEMB_V16_MIGRATION_CONTROL_MAX_BATCH, 0);
    return 13u + (size_t)resp->entry_count *
        VEMB_V16_MIGRATION_CONTROL_RESP_ENCODED_LEN;
}

static inline int vemb_v16_migration_control_batch_resp_encode(
    uint8_t *dst,
    size_t cap,
    const vemb_v16_migration_control_batch_resp_t *resp,
    size_t *out_len) {
    size_t need = vemb_v16_migration_control_batch_resp_encoded_len(resp);
    RETURN_IF(need == 0 || cap < need, -1);
    uint8_t *p = dst;
    vemb_v16_proto_put_u8(&p, resp->status);
    vemb_v16_proto_put_u32(&p, resp->entry_count);
    vemb_v16_proto_put_u32(&p, resp->success_count);
    vemb_v16_proto_put_u32(&p, resp->error_count);
    for (uint32_t i = 0; i < resp->entry_count; i++) {
        size_t entry_len = 0;
        RETURN_IF(vemb_v16_migration_control_resp_encode(
                      p,
                      cap - (size_t)(p - dst),
                      &resp->entries[i],
                      &entry_len) != 0,
                  -1);
        p += entry_len;
    }
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_migration_control_batch_resp_decode(
    vemb_v16_migration_control_batch_resp_t *resp,
    const uint8_t *src,
    size_t len) {
    RETURN_IF(len < 13u, -1);
    const uint8_t *p = src;
    memset(resp, 0, sizeof(*resp));
    resp->status = vemb_v16_proto_get_u8(&p);
    resp->entry_count = vemb_v16_proto_get_u32(&p);
    resp->success_count = vemb_v16_proto_get_u32(&p);
    resp->error_count = vemb_v16_proto_get_u32(&p);
    RETURN_IF(resp->entry_count > VEMB_V16_MIGRATION_CONTROL_MAX_BATCH, -1);
    for (uint32_t i = 0; i < resp->entry_count; i++) {
        RETURN_IF(vemb_v16_migration_control_resp_decode(
                      &resp->entries[i],
                      p,
                      VEMB_V16_MIGRATION_CONTROL_RESP_ENCODED_LEN) != 0,
                  -1);
        p += VEMB_V16_MIGRATION_CONTROL_RESP_ENCODED_LEN;
    }
    RETURN_IF((size_t)(p - src) != len, -1);
    return 0;
}

static inline int vemb_v16_migration_range_control_req_encode(
    uint8_t *dst,
    size_t cap,
    const vemb_v16_migration_range_control_req_t *req,
    size_t *out_len) {
    RETURN_IF(cap < VEMB_V16_MIGRATION_RANGE_CONTROL_REQ_ENCODED_LEN, -1);
    uint8_t *p = dst;
    vemb_v16_proto_put_u64(&p, req->migration_topology_epoch);
    vemb_v16_proto_put_u64(&p, req->cutover_topology_epoch);
    vemb_v16_proto_put_u32(&p, req->target_owner);
    vemb_v16_proto_put_u32(&p, req->shard_id);
    vemb_v16_proto_put_u32(&p, req->flags);
    vemb_v16_proto_put_u32(&p, req->page_limit);
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_migration_range_control_req_decode(
    vemb_v16_migration_range_control_req_t *req,
    const uint8_t *src,
    size_t len) {
    RETURN_IF(len != VEMB_V16_MIGRATION_RANGE_CONTROL_REQ_ENCODED_LEN, -1);
    const uint8_t *p = src;
    memset(req, 0, sizeof(*req));
    req->migration_topology_epoch = vemb_v16_proto_get_u64(&p);
    req->cutover_topology_epoch = vemb_v16_proto_get_u64(&p);
    req->target_owner = vemb_v16_proto_get_u32(&p);
    req->shard_id = vemb_v16_proto_get_u32(&p);
    req->flags = vemb_v16_proto_get_u32(&p);
    req->page_limit = vemb_v16_proto_get_u32(&p);
    return 0;
}

static inline int vemb_v16_migration_range_control_resp_encode(
    uint8_t *dst,
    size_t cap,
    const vemb_v16_migration_range_control_resp_t *resp,
    size_t *out_len) {
    RETURN_IF(cap < VEMB_V16_MIGRATION_RANGE_CONTROL_RESP_ENCODED_LEN, -1);
    uint8_t *p = dst;
    vemb_v16_proto_put_u8(&p, resp->status);
    vemb_v16_proto_put_u64(&p, resp->migration_topology_epoch);
    vemb_v16_proto_put_u64(&p, resp->cutover_topology_epoch);
    vemb_v16_proto_put_u64(&p, resp->owner_epoch);
    vemb_v16_proto_put_u64(&p, resp->applied_seq);
    vemb_v16_proto_put_u64(&p, resp->barrier_seq);
    vemb_v16_proto_put_u64(&p, resp->source_seq);
    vemb_v16_proto_put_u64(&p, resp->retry_delta);
    vemb_v16_proto_put_u32(&p, resp->target_owner);
    vemb_v16_proto_put_u32(&p, resp->shard_id);
    vemb_v16_proto_put_u32(&p, resp->key_count);
    vemb_v16_proto_put_u32(&p, resp->success_count);
    vemb_v16_proto_put_u32(&p, resp->error_count);
    vemb_v16_proto_put_u32(&p, resp->pending_delta);
    vemb_v16_proto_put_u32(&p, resp->outbox_state);
    vemb_v16_proto_put_u32(&p, resp->remaining_keys);
    vemb_v16_proto_put_u32(&p, resp->page_key_count);
    vemb_v16_proto_put_u32(&p, resp->range_done);
    vemb_v16_proto_put_u32(&p, resp->range_ready);
    vemb_v16_proto_put_u32(&p, resp->page_limit);
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_migration_range_control_resp_decode(
    vemb_v16_migration_range_control_resp_t *resp,
    const uint8_t *src,
    size_t len) {
    RETURN_IF(len != VEMB_V16_MIGRATION_RANGE_CONTROL_RESP_ENCODED_LEN, -1);
    const uint8_t *p = src;
    memset(resp, 0, sizeof(*resp));
    resp->status = vemb_v16_proto_get_u8(&p);
    resp->migration_topology_epoch = vemb_v16_proto_get_u64(&p);
    resp->cutover_topology_epoch = vemb_v16_proto_get_u64(&p);
    resp->owner_epoch = vemb_v16_proto_get_u64(&p);
    resp->applied_seq = vemb_v16_proto_get_u64(&p);
    resp->barrier_seq = vemb_v16_proto_get_u64(&p);
    resp->source_seq = vemb_v16_proto_get_u64(&p);
    resp->retry_delta = vemb_v16_proto_get_u64(&p);
    resp->target_owner = vemb_v16_proto_get_u32(&p);
    resp->shard_id = vemb_v16_proto_get_u32(&p);
    resp->key_count = vemb_v16_proto_get_u32(&p);
    resp->success_count = vemb_v16_proto_get_u32(&p);
    resp->error_count = vemb_v16_proto_get_u32(&p);
    resp->pending_delta = vemb_v16_proto_get_u32(&p);
    resp->outbox_state = vemb_v16_proto_get_u32(&p);
    resp->remaining_keys = vemb_v16_proto_get_u32(&p);
    resp->page_key_count = vemb_v16_proto_get_u32(&p);
    resp->range_done = vemb_v16_proto_get_u32(&p);
    resp->range_ready = vemb_v16_proto_get_u32(&p);
    resp->page_limit = vemb_v16_proto_get_u32(&p);
    return 0;
}

static inline size_t vemb_v16_req_encoded_len(const vemb_v16_req_t *req) {
    switch (req->op) {
    case VEMB_V16_OP_VADD:
    case VEMB_V16_OP_VSIM_INLINE:
        return 24u + 4u + (size_t)req->key_len + (size_t)req->vector_bytes;
    case VEMB_V16_OP_VSIM_KEY_KEY:
        return 24u + 1u + (size_t)req->key_len + (size_t)req->key2_len;
    case VEMB_V16_OP_VEMB_HANDLE:
    case VEMB_V16_OP_VEMB_INLINE:
    case VEMB_V16_OP_VREM:
    case VEMB_V16_OP_PING:
        return 24u + (size_t)req->key_len;
    default:
        return 0;
    }
}

static inline int vemb_v16_req_wire_flags(uint8_t req_flags,
                                          uint8_t *wire_flags_out) {
    RETURN_IF((req_flags & (uint8_t)~VEMB_V16_REQ_F_ASK_REDIRECT) != 0, -1);
    *wire_flags_out =
        (req_flags & VEMB_V16_REQ_F_ASK_REDIRECT) ?
        VEMB_V16_TCP_REQ_WIRE_F_ASK_REDIRECT : 0;
    return 0;
}

static inline uint8_t vemb_v16_req_flags_from_wire(uint8_t wire_flags) {
    uint8_t req_flags = 0;
    if ((wire_flags & VEMB_V16_TCP_REQ_WIRE_F_ASK_REDIRECT) != 0)
        req_flags |= VEMB_V16_REQ_F_ASK_REDIRECT;
    return req_flags;
}

static inline int vemb_v16_req_encode(uint8_t *dst,
                                      size_t cap,
                                      const vemb_v16_req_t *req,
                                      size_t *out_len) {
    RETURN_IF(req->key_len > VEMB_V16_MAX_KEY_LEN ||
                  req->key2_len > VEMB_V16_MAX_KEY_LEN ||
                  req->vector_bytes > sizeof(req->vector) ||
                  req->dim > VEMB_V16_MAX_DIM ||
                  req->dim > UINT16_MAX ||
                  req->op > VEMB_V16_TCP_REQ_OP_MASK,
              -1);
    size_t need = vemb_v16_req_encoded_len(req);
    uint8_t wire_flags = 0;
    RETURN_IF(need == 0 || cap < need ||
              vemb_v16_req_wire_flags(req->flags, &wire_flags) != 0,
              -1);
    uint8_t *p = dst;
    vemb_v16_proto_put_u8(&p, (uint8_t)(req->op | wire_flags));
    vemb_v16_proto_put_u8(&p, (uint8_t)req->key_len);
    vemb_v16_proto_put_u16(&p, (uint16_t)req->dim);
    vemb_v16_proto_put_u32(&p, req->req_id);
    vemb_v16_proto_put_u64(&p, req->channel_id);
    vemb_v16_proto_put_u64(&p, req->topology_epoch);
    switch (req->op) {
    case VEMB_V16_OP_VADD:
    case VEMB_V16_OP_VSIM_INLINE:
        RETURN_IF(req->key_len == 0 || req->dim == 0 ||
                      req->vector_bytes != req->dim * sizeof(float),
                  -1);
        vemb_v16_proto_put_u32(&p, req->vector_bytes);
        vemb_v16_proto_put_bytes(&p, req->key, req->key_len);
        vemb_v16_proto_put_bytes(&p, req->vector, req->vector_bytes);
        break;
    case VEMB_V16_OP_VSIM_KEY_KEY:
        RETURN_IF(req->key_len == 0 || req->key2_len == 0 || req->dim == 0 ||
                      req->vector_bytes != req->dim * sizeof(float),
                  -1);
        vemb_v16_proto_put_u8(&p, (uint8_t)req->key2_len);
        vemb_v16_proto_put_bytes(&p, req->key, req->key_len);
        vemb_v16_proto_put_bytes(&p, req->key2, req->key2_len);
        break;
    case VEMB_V16_OP_VEMB_HANDLE:
    case VEMB_V16_OP_VEMB_INLINE:
        RETURN_IF(req->key_len == 0 || req->dim == 0 ||
                      req->vector_bytes != req->dim * sizeof(float),
                  -1);
        vemb_v16_proto_put_bytes(&p, req->key, req->key_len);
        break;
    case VEMB_V16_OP_VREM:
        RETURN_IF(req->key_len == 0 || req->dim != 0, -1);
        vemb_v16_proto_put_bytes(&p, req->key, req->key_len);
        break;
    case VEMB_V16_OP_PING:
        RETURN_IF(req->key_len != 0 || req->dim != 0, -1);
        break;
    default:
        return -1;
    }
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_req_decode(vemb_v16_req_t *req,
                                      const uint8_t *src,
                                      size_t len) {
    RETURN_IF(len < 24u, -1);
    const uint8_t *p = src;
    memset(req, 0, sizeof(*req));
    uint8_t op_flags = vemb_v16_proto_get_u8(&p);
    req->op = op_flags & VEMB_V16_TCP_REQ_OP_MASK;
    req->flags = vemb_v16_req_flags_from_wire(op_flags & VEMB_V16_TCP_REQ_FLAG_MASK);
    req->key_len = vemb_v16_proto_get_u8(&p);
    req->dim = vemb_v16_proto_get_u16(&p);
    req->req_id = vemb_v16_proto_get_u32(&p);
    req->channel_id = vemb_v16_proto_get_u64(&p);
    req->topology_epoch = vemb_v16_proto_get_u64(&p);
    RETURN_IF(req->key_len > VEMB_V16_MAX_KEY_LEN || req->dim > VEMB_V16_MAX_DIM, -1);
    switch (req->op) {
    case VEMB_V16_OP_VADD:
    case VEMB_V16_OP_VSIM_INLINE:
        req->vector_bytes = vemb_v16_proto_get_u32(&p);
        RETURN_IF(req->key_len == 0 || req->dim == 0 ||
                      req->vector_bytes > sizeof(req->vector) ||
                      req->vector_bytes != req->dim * sizeof(float),
                  -1);
        RETURN_IF(len != 24u + 4u + (size_t)req->key_len +
                             (size_t)req->vector_bytes,
                  -1);
        vemb_v16_proto_get_bytes(&p, req->key, req->key_len);
        vemb_v16_proto_get_bytes(&p, req->vector, req->vector_bytes);
        break;
    case VEMB_V16_OP_VSIM_KEY_KEY:
        req->key2_len = vemb_v16_proto_get_u8(&p);
        RETURN_IF(req->key_len == 0 || req->key2_len == 0 ||
                      req->key2_len > VEMB_V16_MAX_KEY_LEN || req->dim == 0,
                  -1);
        req->vector_bytes = req->dim * sizeof(float);
        RETURN_IF(len != 24u + 1u + (size_t)req->key_len +
                             (size_t)req->key2_len,
                  -1);
        vemb_v16_proto_get_bytes(&p, req->key, req->key_len);
        vemb_v16_proto_get_bytes(&p, req->key2, req->key2_len);
        break;
    case VEMB_V16_OP_VEMB_HANDLE:
    case VEMB_V16_OP_VEMB_INLINE:
        RETURN_IF(req->key_len == 0 || req->dim == 0, -1);
        req->vector_bytes = req->dim * sizeof(float);
        RETURN_IF(len != 24u + (size_t)req->key_len, -1);
        vemb_v16_proto_get_bytes(&p, req->key, req->key_len);
        break;
    case VEMB_V16_OP_VREM:
        RETURN_IF(req->key_len == 0 || req->dim != 0, -1);
        RETURN_IF(len != 24u + (size_t)req->key_len, -1);
        vemb_v16_proto_get_bytes(&p, req->key, req->key_len);
        break;
    case VEMB_V16_OP_PING:
        RETURN_IF(req->key_len != 0 || req->dim != 0 || len != 24u, -1);
        break;
    default:
        return -1;
    }
    req->key_hash = vemb_v16_xxh3_64_str(req->key, req->key_len);
    if (req->key2_len != 0)
        req->key2_hash = vemb_v16_xxh3_64_str(req->key2, req->key2_len);
    return 0;
}

static inline size_t vemb_v16_resp_encoded_len_for_fields(uint8_t status,
                                                          uint8_t op) {
    size_t len = VEMB_V16_RESP_ENCODED_BASE_LEN;
    if (status == VEMB_V16_STATUS_OK) {
        switch (op) {
        case VEMB_V16_OP_VEMB_HANDLE:
        case VEMB_V16_OP_VEMB_INLINE:
            return len + 28u;
        case VEMB_V16_OP_VSIM_INLINE:
        case VEMB_V16_OP_VSIM_KEY_KEY:
            return len + 4u;
        default:
            return len;
        }
    }
    if (status == VEMB_V16_STATUS_MOVED || status == VEMB_V16_STATUS_ASK)
        return len + 4u;
    return len;
}

static inline size_t vemb_v16_resp_encoded_len(const vemb_v16_resp_t *resp) {
    return vemb_v16_resp_encoded_len_for_fields(resp->status, resp->op);
}

static inline uint32_t vemb_v16_aeron_resp_slot_size(void) {
    return (uint32_t)align_up_size(
        vemb_v16_resp_encoded_len_for_fields(VEMB_V16_STATUS_OK,
                                             VEMB_V16_OP_VEMB_HANDLE),
        CACHELINE_SIZE);
}

static inline int vemb_v16_resp_encode(uint8_t *dst,
                                       size_t cap,
                                       const vemb_v16_resp_t *resp,
                                       size_t *out_len) {
    size_t len = vemb_v16_resp_encoded_len(resp);
    RETURN_IF(cap < len || (resp->flags & ~0x3u) != 0, -1);
    uint8_t *p = dst;
    *p++ = resp->status;
    *p++ = (uint8_t)((resp->op & VEMB_V16_TCP_RESP_OP_MASK) |
                     ((resp->flags & 0x3u) << 6));
    vemb_v16_proto_put_u32(&p, resp->req_id);
    if (resp->status == VEMB_V16_STATUS_OK) {
        switch (resp->op) {
        case VEMB_V16_OP_VEMB_HANDLE:
        case VEMB_V16_OP_VEMB_INLINE:
            vemb_v16_proto_put_u32(&p, resp->vector_bytes);
            vemb_v16_proto_put_u64(&p, resp->vector_offset);
            vemb_v16_proto_put_u32(&p, resp->region_id);
            vemb_v16_proto_put_u32(&p, resp->local_slot);
            vemb_v16_proto_put_u64(&p, resp->owner_generation);
            break;
        case VEMB_V16_OP_VSIM_INLINE:
        case VEMB_V16_OP_VSIM_KEY_KEY:
            vemb_v16_proto_put_f32(&p, resp->score);
            break;
        default:
            break;
        }
    } else if (resp->status == VEMB_V16_STATUS_MOVED ||
               resp->status == VEMB_V16_STATUS_ASK) {
        vemb_v16_proto_put_u32(&p, resp->redirect_owner);
    }
    if (out_len) *out_len = (size_t)(p - dst);
    return 0;
}

static inline int vemb_v16_resp_decode(vemb_v16_resp_t *resp,
                                       const uint8_t *src,
                                       size_t len) {
    RETURN_IF(len < vemb_v16_resp_encoded_base_len(), -1);
    const uint8_t *p = src;
    uint8_t op_flags = 0;
    memset(resp, 0, sizeof(*resp));
    resp->status = *p++;
    op_flags = *p++;
    resp->op = (uint8_t)(op_flags & VEMB_V16_TCP_RESP_OP_MASK);
    resp->flags = (uint16_t)((op_flags & VEMB_V16_TCP_RESP_FLAG_MASK) >> 6);
    resp->req_id = vemb_v16_proto_get_u32(&p);
    /* VEMB_INLINE responses append the inline vector payload after the 6B+28B
     * metadata on the wire. The wire `len` here is the full frame payload
     * (metadata + inline bytes); vemb_v16_resp_encoded_len() returns only the
     * metadata portion, so enforce a lower bound — strict equality would reject
     * every VEMB_INLINE response. */
    RETURN_IF(len < vemb_v16_resp_encoded_len(resp), -1);
    if (resp->status == VEMB_V16_STATUS_OK) {
        switch (resp->op) {
        case VEMB_V16_OP_VEMB_HANDLE:
        case VEMB_V16_OP_VEMB_INLINE:
            resp->vector_bytes = vemb_v16_proto_get_u32(&p);
            resp->vector_offset = vemb_v16_proto_get_u64(&p);
            resp->region_id = vemb_v16_proto_get_u32(&p);
            resp->local_slot = vemb_v16_proto_get_u32(&p);
            resp->owner_generation = vemb_v16_proto_get_u64(&p);
            resp->dim = resp->vector_bytes / sizeof(float);
            break;
        case VEMB_V16_OP_VSIM_INLINE:
        case VEMB_V16_OP_VSIM_KEY_KEY:
            resp->score = vemb_v16_proto_get_f32(&p);
            break;
        case VEMB_V16_OP_PING:
        case VEMB_V16_OP_VADD:
        case VEMB_V16_OP_VREM:
            break;
        default:
            return -1;
        }
    } else if (resp->status == VEMB_V16_STATUS_MOVED ||
               resp->status == VEMB_V16_STATUS_ASK) {
        resp->redirect_owner = vemb_v16_proto_get_u32(&p);
    }
    return 0;
}

#endif
