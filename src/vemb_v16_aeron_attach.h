#ifndef VEMB_V16_AERON_ATTACH_H
#define VEMB_V16_AERON_ATTACH_H

#include <stdint.h>

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

/* Client-side helper: send ATTACH req, read ATTACH resp. Returns 0 on
 * success and fills resp. Defined in vemb_v16_aeron_attach.c. */
int vemb_v16_aeron_attach_client_exchange(int fd,
                                          const vemb_v16_aeron_attach_req_t *req,
                                          vemb_v16_aeron_attach_resp_t *resp);

#endif
