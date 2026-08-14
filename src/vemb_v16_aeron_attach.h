#ifndef VEMB_V16_AERON_ATTACH_H
#define VEMB_V16_AERON_ATTACH_H

#include <stdint.h>

#include "vemb_v16_cacheline.h"
#include "vemb_v16_util.h"

/* =====================================================================
 *  Cross-node Aeron ATTACH protocol
 * =====================================================================
 *
 * Lets a remote client (HW02) open an aeron channel on a remote server
 * (HW01) via TCP, when UDS is not reachable (AF_LOCAL = single-host).
 *
 * Wire format (all integers little-endian, raw struct — no RESP):
 *
 *   Client -> Server:
 *     char     magic[24];          "VEMB_V16_AERON_ATTACH\0"
 *     uint32_t dim;                vector dimension (sizes ring slots)
 *     uint32_t req_slot_size;      0 = use server default
 *     uint32_t resp_slot_size;     0 = use server default
 *     uint32_t flags;              path view flags
 *
 *   Server -> Client:
 *     char     magic[26];          "VEMB_V16_AERON_ATTACHED\0"
 *     int32_t  status;             0 = OK, -1 = rejected
 *     uint64_t channel_id;         server-assigned channel id
 *     uint32_t ring_size_slots;    256
 *     uint32_t request_shmdev_path_len;
 *     char     request_shmdev_path[256];
 *     uint64_t req_ring_off;       byte offset within request path
 *     uint32_t response_shmdev_path_len;
 *     char     response_shmdev_path[256];
 *     uint64_t resp_ring_off;      byte offset within response path
 *     uint32_t req_backend_type;
 *     uint32_t resp_backend_type;
 *     uint32_t req_slot_size;      actual slot size server allocated
 *     uint32_t resp_slot_size;
 *     uint32_t warm_region_count;  0 = no warm region advertised
 *     uint32_t warm_region_id;     region id (valid when count > 0)
 *     uint32_t warm_backend_type;  backend type (ub / local shm)
 *     uint64_t warm_region_bytes;  size in bytes
 *     uint64_t warm_mmap_offset;   byte offset within shmdev file
 *     uint32_t warm_path_len;      bytes in warm_path (incl. NUL)
 *     char     warm_path[256];     server-side path, e.g. "/dev/obmm_shmdev2"
 *
 * Sniff rule: the first 24 bytes of a new TCP connection decide routing.
 * If they exactly equal VEMB_V16_AERON_ATTACH_MAGIC, the connection is
 * handed to vemb_v16_aeron_attach_handle_fd() instead of the normal
 * VEMB V16 RESP / sniff path.
 */

#define VEMB_V16_AERON_ATTACH_MAGIC      "VEMB_V16_AERON_ATTACH\0\0\0"  /* first 24 bytes */
#define VEMB_V16_AERON_ATTACH_MAGIC_LEN  24u
#define VEMB_V16_AERON_ATTACHED_MAGIC    "VEMB_V16_AERON_ATTACHED\0\0\0" /* first 26 bytes */
#define VEMB_V16_AERON_ATTACHED_MAGIC_LEN 26u
#define VEMB_V16_AERON_SHMDEV_PATH_MAX   256u
#define VEMB_V16_AERON_ATTACH_F_REMOTE_PATH 0x1u

/* v2 batch requests use a separate ATTACH ABI and channel. The v1 fixed-size
 * structs below remain unchanged so old SDKs keep their current wire layout. */
#define VEMB_V16_AERON_ATTACH_V2_MAGIC "VEMB_V16_AERON_ATTACH2\0\0"
#define VEMB_V16_AERON_ATTACH_V2_MAGIC_LEN 24u
#define VEMB_V16_AERON_ATTACHED_V2_MAGIC "VEMB_V16_AERON_ATTACHED2\0\0"
#define VEMB_V16_AERON_ATTACHED_V2_MAGIC_LEN 26u
#define VEMB_V16_BATCH_REQUEST_SIZE_DEFAULT 32u
#define VEMB_V16_BATCH_REQUEST_SIZE_MAX 128u
/* v2 keeps every frame contiguous in its byte arena. The first protocol
 * revision uses one fixed bound so a client cannot grow UB allocations via
 * ATTACH. A zero request value means this server default. */
#define VEMB_V16_BATCH_MAX_BYTES_DEFAULT (64u * 1024u)
#define VEMB_V16_BATCH_MAX_BYTES_MAX     (64u * 1024u)
#define VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE 64u

static inline uint32_t vemb_v16_batch_aligned_bytes(uint32_t bytes) {
    return (uint32_t)align_up_size(bytes, CACHELINE_SIZE);
}

static inline uint32_t vemb_v16_effective_batch_request_size(
        uint32_t requested, uint32_t configured) {
    if (requested == 0 || configured == 0)
        return 0;
    if (requested > VEMB_V16_BATCH_REQUEST_SIZE_MAX)
        requested = VEMB_V16_BATCH_REQUEST_SIZE_MAX;
    if (configured > VEMB_V16_BATCH_REQUEST_SIZE_MAX)
        configured = VEMB_V16_BATCH_REQUEST_SIZE_MAX;
    return requested < configured ? requested : configured;
}

typedef struct vemb_v16_aeron_batch_resource_desc {
    uint32_t backend_type;
    uint32_t path_len;
    uint64_t mmap_offset;
    uint64_t bytes;
    char path[VEMB_V16_AERON_SHMDEV_PATH_MAX];
} vemb_v16_aeron_batch_resource_desc_t;

typedef struct vemb_v16_aeron_attach_v2_req {
    char magic[VEMB_V16_AERON_ATTACH_V2_MAGIC_LEN];
    uint32_t dim;
    uint32_t flags;
    uint32_t requested_batch_size;
    uint32_t max_batch_bytes;
} vemb_v16_aeron_attach_v2_req_t;

typedef struct vemb_v16_aeron_attach_v2_resp {
    char magic[VEMB_V16_AERON_ATTACHED_V2_MAGIC_LEN];
    int32_t status;
    uint64_t channel_id;
    uint64_t topology_epoch;
    uint32_t effective_batch_size;
    uint32_t max_batch_bytes;
    uint32_t descriptor_slot_size;
    uint32_t descriptor_ring_slots;
    vemb_v16_aeron_batch_resource_desc_t request_descriptor;
    vemb_v16_aeron_batch_resource_desc_t request_arena;
    vemb_v16_aeron_batch_resource_desc_t response_descriptor;
    vemb_v16_aeron_batch_resource_desc_t response_arena;
} vemb_v16_aeron_attach_v2_resp_t;

typedef struct {
    char     magic[VEMB_V16_AERON_ATTACH_MAGIC_LEN];
    uint32_t dim;
    uint32_t req_slot_size;
    uint32_t resp_slot_size;
    uint32_t flags;
} vemb_v16_aeron_attach_req_t;

typedef struct {
    char     magic[VEMB_V16_AERON_ATTACHED_MAGIC_LEN];
    int32_t  status;
    uint64_t channel_id;
    uint32_t ring_size_slots;
    uint32_t request_shmdev_path_len;
    char     request_shmdev_path[VEMB_V16_AERON_SHMDEV_PATH_MAX];
    uint64_t req_ring_off;
    uint32_t response_shmdev_path_len;
    char     response_shmdev_path[VEMB_V16_AERON_SHMDEV_PATH_MAX];
    uint64_t resp_ring_off;
    uint32_t req_backend_type;
    uint32_t resp_backend_type;
    uint32_t req_slot_size;
    uint32_t resp_slot_size;
    /* Warm region advertisement (cross-node read path). When
     * warm_region_count > 0, warm_path is server-side and the client maps
     * it to its local UB view before mmap'ing it. */
    uint32_t warm_region_count;
    uint32_t warm_region_id;
    uint32_t warm_backend_type;
    uint64_t warm_region_bytes;
    uint64_t warm_mmap_offset;
    uint32_t warm_path_len;
    char     warm_path[VEMB_V16_AERON_SHMDEV_PATH_MAX];
} vemb_v16_aeron_attach_resp_t;

/* Server-side entry: read ATTACH req, allocate channel + shmdev rings,
 * write ATTACH resp. Returns 0 on success, -1 on any error (caller closes
 * fd). Defined in vemb_v16_aeron_attach.c. */
struct vemb_v16_proxy;
int vemb_v16_aeron_attach_handle_fd(struct vemb_v16_proxy *proxy, int fd);

/* v2 counterpart of vemb_v16_aeron_attach_handle_fd(). The TCP sniff router
 * already consumed the v2 magic and this function reads the remaining
 * request fields, allocates four UB regions, and returns their descriptors. */
int vemb_v16_aeron_attach_v2_handle_fd(struct vemb_v16_proxy *proxy, int fd);

/* Client-side helper: send ATTACH req, read ATTACH resp. Returns 0 on
 * success and fills resp. Defined in vemb_v16_aeron_attach.c. */
int vemb_v16_aeron_attach_client_exchange(int fd,
                                          const vemb_v16_aeron_attach_req_t *req,
                                          vemb_v16_aeron_attach_resp_t *resp);

#endif
