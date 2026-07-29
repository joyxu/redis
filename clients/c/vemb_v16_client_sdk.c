#define _GNU_SOURCE

#include "vemb_v16_client_sdk.h"
#include "../../src/vemb_v16_net.h"
#include "../../src/vemb_v16_aeron_attach.h"  /* cross-node ATTACH protocol */
/* Ring header is C11 (<stdatomic.h>). Pulled in here — NOT from the
 * public SDK header — so C++ consumers stay clean. */
#include "../../src/vemb_v16_client_ring.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <errno.h>

#ifdef USE_ARM_SVE
#include <arm_sve.h>
#endif

#define VEMB_V16_SDK_MAX_ENDPOINTS 16
/* Must match benchmark/vemb_v16_bench.c VEMB_V16_BENCH_HASH_VNODES so that
 * multi-endpoint routing stays interoperable across tools (a set filled by
 * redis-cli is visible to vemb_v16_bench / memtier). */
#define VEMB_V16_SDK_HASH_VNODES  10

/* Encoded request buffer — large enough for any op:
 *   24 (base) + 4 (vector_bytes) + 128 (key) + 4096*4 (vector) = 16676
 * Round up to a safe upper bound. */
#define VEMB_V16_SDK_ENC_BUF_LEN  (24u + 4u + VEMB_V16_MAX_KEY_LEN + \
                                   VEMB_V16_MAX_DIM * sizeof(float) + 64u)

void sve_streaming_load_f32(const void *src, void *dst, size_t size);

/* Encode req using the compact wire format and write it as a VEMB_V16_NET_REQUEST
 * frame. Returns 0 on success, -1 on error. */
static int sdk_write_request(int fd,
                             uint64_t channel_id,
                             uint32_t req_id,
                             const vemb_v16_req_t *req) {
    uint8_t enc_buf[VEMB_V16_SDK_ENC_BUF_LEN];
    size_t enc_len = 0;
    if (vemb_v16_req_encode(enc_buf, sizeof(enc_buf), req, &enc_len) != 0)
        return -1;
    return vemb_v16_net_write_frame(fd, VEMB_V16_NET_REQUEST, 0,
                                    channel_id, req_id, enc_buf,
                                    (uint32_t)enc_len);
}

typedef struct {
    int      fd;
    uint64_t channel_id;
    char     host[64];
    uint16_t port;
    /* warm region (mmap'd once per backend at connect) */
    uint64_t warm_region_bytes;
    uint64_t warm_mmap_offset;
    size_t   mapping_bytes;
    uint8_t *mapping_addr;
    uint8_t *mapped_addr;
} sdk_backend_t;

typedef struct {
    uint32_t hash_value;
    uint32_t backend_idx;
} sdk_hash_node_t;

struct vemb_v16_client {
    /* Routed view — kept for back-compat with v*_free helpers. Mirrors
     * owner_channels[cur_owner_idx] (or seed_conns[0] in single-endpoint mode). */
    int      fd;
    uint64_t channel_id;
    char     host[64];
    uint16_t port;
    uint64_t warm_region_bytes;
    uint64_t warm_mmap_offset;
    size_t   mapping_bytes;
    uint8_t *mapping_addr;
    uint8_t *mapped_addr;

    uint32_t dim;
    uint32_t req_id;

    /* Seed connections — bootstrap only. Renamed from backends[]; semantics
     * unchanged for single-endpoint mode. */
    int             seed_conn_count;
    int             cur_idx;
    sdk_backend_t   seed_conns[VEMB_V16_SDK_MAX_ENDPOINTS];

    /* Topology view (lazy-initialized on first keyed op when seed_conn_count > 1). */
    vemb_v16_client_topology_t  topology;
    enum {
        TOPO_UNINITIALIZED = 0,
        TOPO_READY,
        TOPO_STALE,
    } topology_state;

    /* Owner-keyed channels, opened lazily by ensure_owner_channel(). */
    sdk_backend_t   owner_channels[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];
    uint8_t         owner_channel_inited[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];

    /* Retry + observability. */
    uint32_t        retry_budget;     /* Default set in create_multi */
    uint32_t        connect_timeout_ms; /* 0 = default 10000 */
    uint64_t        ask_redirects;
    uint64_t        moved_redirects;
    uint64_t        stale_topology_responses;
    uint64_t        topology_refresh_calls;

    /* Legacy static ring — kept only for vemb_v16_route_key() back-compat. */
    sdk_hash_node_t hash_ring[VEMB_V16_SDK_MAX_ENDPOINTS * VEMB_V16_SDK_HASH_VNODES];
    uint32_t        hash_node_count;
};

/* ------------------------------------------------------------------ */
/* Shared helpers (used by both sync & async APIs)                    */
/* ------------------------------------------------------------------ */

int vemb_v16_build_combined_key(char *out, size_t out_cap,
                                const char *set_name, const char *elem_name,
                                uint32_t *out_len)
{
    size_t elem_len = strlen(elem_name);
    if (set_name == NULL || *set_name == '\0') {
        /* 无 set_name：key = elem_name (无分隔符) */
        if (elem_len >= out_cap) {
            fprintf(stderr, "vemb_v16_client: combined key too long: %zu >= %zu\n",
                    elem_len, out_cap);
            return -1;
        }
        memcpy(out, elem_name, elem_len);
        *out_len = (uint32_t)elem_len;
        return 0;
    }
    size_t set_len = strlen(set_name);
    size_t total = set_len + 1 + elem_len;
    if (total >= out_cap) {
        fprintf(stderr, "vemb_v16_client: combined key too long: %zu >= %zu\n",
                total, out_cap);
        return -1;
    }
    memcpy(out, set_name, set_len);
    out[set_len] = '\0';  /* separator */
    memcpy(out + set_len + 1, elem_name, elem_len);
    *out_len = (uint32_t)total;
    return 0;
}

vemb_v16_resp_class_t vemb_v16_classify_resp_status(uint8_t status) {
    switch (status) {
    case VEMB_V16_STATUS_OK:             return VEMB_V16_RESP_CLASS_OK;
    case VEMB_V16_STATUS_NOT_FOUND:      return VEMB_V16_RESP_CLASS_NOT_FOUND;
    case VEMB_V16_STATUS_ASK:            return VEMB_V16_RESP_CLASS_ASK;
    case VEMB_V16_STATUS_MOVED:
    case VEMB_V16_STATUS_STALE_TOPOLOGY: return VEMB_V16_RESP_CLASS_REFRESH;
    default:                             return VEMB_V16_RESP_CLASS_FATAL;
    }
}

/* Forward decl: defined below (shared by ring + warm-region mmap paths). */
static void *vemb_v16_mmap_shmdev_region(const char *path,
                                          uint32_t backend_type,
                                          int use_sync,
                                          uint64_t offset, size_t bytes,
                                          size_t *out_map_bytes,
                                          size_t *out_offset_delta);

static int vemb_v16_open_warm_region_internal(const vemb_v16_channel_desc_t *desc,
                                               void **out_mapping_addr,
                                               size_t *out_mapping_bytes,
                                               void **out_mapped_addr,
                                               uint64_t *out_region_bytes);

int vemb_v16_open_warm_region(const vemb_v16_channel_desc_t *desc,
                              void **out_mapping_addr,
                              size_t *out_mapping_bytes,
                              void **out_mapped_addr,
                              uint64_t *out_region_bytes)
{
    return vemb_v16_open_warm_region_internal(desc, out_mapping_addr,
                                               out_mapping_bytes,
                                               out_mapped_addr,
                                               out_region_bytes);
}

static int vemb_v16_open_warm_region_internal(const vemb_v16_channel_desc_t *desc,
                                               void **out_mapping_addr,
                                               size_t *out_mapping_bytes,
                                               void **out_mapped_addr,
                                               uint64_t *out_region_bytes)
{
    if (!desc || !desc->vector_region_name[0])
        return -1;

    if (desc->warm_backend_type != VEMB_V16_REGION_LOCAL_SHM &&
        desc->warm_backend_type != VEMB_V16_REGION_UB)
        return -1;

    size_t size = desc->warm_region_bytes
        ? (size_t)desc->warm_region_bytes
        : (size_t)desc->vector_stride * desc->max_vectors;

    size_t map_bytes = 0, offset_delta = 0;
    void *ptr = vemb_v16_mmap_shmdev_region(desc->vector_region_name,
                                            desc->warm_backend_type,
                                            0,
                                            desc->warm_mmap_offset, size,
                                            &map_bytes, &offset_delta);
    if (!ptr) return -1;

    if (out_region_bytes) *out_region_bytes = size;
    if (out_mapping_bytes) *out_mapping_bytes = map_bytes;
    if (out_mapping_addr) *out_mapping_addr = ptr;
    if (out_mapped_addr) *out_mapped_addr = (uint8_t *)ptr + offset_delta;
    return 0;
}

void vemb_v16_close_warm_region(void *mapping_addr, size_t mapping_bytes)
{
    if (mapping_addr) {
        munmap(mapping_addr, mapping_bytes);
    }
}

/* ------------------------------------------------------------------ */
/* Async / Buffer-based serialization API                             */
/* ------------------------------------------------------------------ */

ssize_t vemb_v16_serialize_hello(void *buf, size_t buf_cap,
                                 uint32_t vector_dim, uint32_t flags)
{
    size_t total = sizeof(vemb_v16_net_hdr_t) + sizeof(vemb_v16_alloc_req_t);
    if (buf_cap < total)
        return -1;

    vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)buf;
    memset(hdr, 0, sizeof(*hdr));
    hdr->magic = VEMB_V16_MAGIC;
    hdr->version = VEMB_V16_VERSION;
    hdr->type = VEMB_V16_NET_HELLO;
    hdr->payload_len = (uint32_t)sizeof(vemb_v16_alloc_req_t);

    vemb_v16_alloc_req_t *req = (vemb_v16_alloc_req_t *)((char *)buf + sizeof(*hdr));
    memset(req, 0, sizeof(*req));
    req->vector_dim = vector_dim;
    req->flags = flags;

    return (ssize_t)total;
}

ssize_t vemb_v16_serialize_vadd(void *buf, size_t buf_cap,
                                uint64_t channel_id, uint32_t req_id,
                                const char *key, uint32_t key_len,
                                const float *vector, uint32_t dim)
{
    if (!key || key_len == 0 || key_len >= VEMB_V16_MAX_KEY_LEN ||
        !vector || dim == 0 || dim > VEMB_V16_MAX_DIM) {
        return -1;
    }

    vemb_v16_req_t req = {0};
    req.op = VEMB_V16_OP_VADD;
    req.flags = 0;
    req.req_id = req_id;
    req.channel_id = channel_id;
    req.key_len = key_len;
    memcpy(req.key, key, key_len);
    req.dim = dim;
    req.flags = 0;  /* local TCP control: server path is directly visible */
    req.vector_bytes = dim * sizeof(float);
    memcpy(req.vector, vector, dim * sizeof(float));

    if (buf_cap < sizeof(vemb_v16_net_hdr_t))
        return -1;

    size_t payload_len = 0;
    if (vemb_v16_req_encode((uint8_t *)buf + sizeof(vemb_v16_net_hdr_t),
                            buf_cap - sizeof(vemb_v16_net_hdr_t),
                            &req, &payload_len) != 0) {
        return -1;
    }
    size_t total = sizeof(vemb_v16_net_hdr_t) + payload_len;

    vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)buf;
    memset(hdr, 0, sizeof(*hdr));
    hdr->magic = VEMB_V16_MAGIC;
    hdr->version = VEMB_V16_VERSION;
    hdr->type = VEMB_V16_NET_REQUEST;
    hdr->payload_len = (uint32_t)payload_len;
    hdr->channel_id = channel_id;
    hdr->req_id = req_id;

    return (ssize_t)total;
}

ssize_t vemb_v16_serialize_vemb(void *buf, size_t buf_cap,
                                uint64_t channel_id, uint32_t req_id,
                                const char *key, uint32_t key_len,
                                uint32_t dim)
{
    if (!key || key_len == 0 || key_len >= VEMB_V16_MAX_KEY_LEN ||
        dim == 0 || dim > VEMB_V16_MAX_DIM) {
        return -1;
    }

    vemb_v16_req_t req = {0};
    req.op = VEMB_V16_OP_VEMB_HANDLE;
    req.flags = 0;
    req.req_id = req_id;
    req.channel_id = channel_id;
    req.key_len = key_len;
    memcpy(req.key, key, key_len);
    req.dim = dim;
    req.vector_bytes = dim * sizeof(float);

    if (buf_cap < sizeof(vemb_v16_net_hdr_t))
        return -1;

    size_t payload_len = 0;
    if (vemb_v16_req_encode((uint8_t *)buf + sizeof(vemb_v16_net_hdr_t),
                            buf_cap - sizeof(vemb_v16_net_hdr_t),
                            &req, &payload_len) != 0) {
        return -1;
    }
    size_t total = sizeof(vemb_v16_net_hdr_t) + payload_len;

    vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)buf;
    memset(hdr, 0, sizeof(*hdr));
    hdr->magic = VEMB_V16_MAGIC;
    hdr->version = VEMB_V16_VERSION;
    hdr->type = VEMB_V16_NET_REQUEST;
    hdr->payload_len = (uint32_t)payload_len;
    hdr->channel_id = channel_id;
    hdr->req_id = req_id;

    return (ssize_t)total;
}

ssize_t vemb_v16_serialize_vrem(void *buf, size_t buf_cap,
                                uint64_t channel_id, uint32_t req_id,
                                const char *key, uint32_t key_len)
{
    if (!key || key_len == 0 || key_len >= VEMB_V16_MAX_KEY_LEN) {
        return -1;
    }

    vemb_v16_req_t req = {0};
    req.op = VEMB_V16_OP_VREM;
    req.flags = 0;
    req.req_id = req_id;
    req.channel_id = channel_id;
    req.key_len = key_len;
    memcpy(req.key, key, key_len);
    req.dim = 0;
    req.vector_bytes = 0;

    if (buf_cap < sizeof(vemb_v16_net_hdr_t))
        return -1;

    size_t payload_len = 0;
    if (vemb_v16_req_encode((uint8_t *)buf + sizeof(vemb_v16_net_hdr_t),
                            buf_cap - sizeof(vemb_v16_net_hdr_t),
                            &req, &payload_len) != 0) {
        return -1;
    }
    size_t total = sizeof(vemb_v16_net_hdr_t) + payload_len;

    vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)buf;
    memset(hdr, 0, sizeof(*hdr));
    hdr->magic = VEMB_V16_MAGIC;
    hdr->version = VEMB_V16_VERSION;
    hdr->type = VEMB_V16_NET_REQUEST;
    hdr->payload_len = (uint32_t)payload_len;
    hdr->channel_id = channel_id;
    hdr->req_id = req_id;

    return (ssize_t)total;
}

ssize_t vemb_v16_serialize_vemb_inline(void *buf, size_t buf_cap,
                                       uint64_t channel_id, uint32_t req_id,
                                       const char *key, uint32_t key_len,
                                       uint32_t dim)
{
    if (!key || key_len == 0 || key_len >= VEMB_V16_MAX_KEY_LEN ||
        dim == 0 || dim > VEMB_V16_MAX_DIM) {
        return -1;
    }

    vemb_v16_req_t req = {0};
    req.op = VEMB_V16_OP_VEMB_INLINE;
    req.flags = 0;
    req.req_id = req_id;
    req.channel_id = channel_id;
    req.key_len = key_len;
    memcpy(req.key, key, key_len);
    req.dim = dim;
    req.vector_bytes = dim * sizeof(float);

    if (buf_cap < sizeof(vemb_v16_net_hdr_t))
        return -1;

    size_t payload_len = 0;
    if (vemb_v16_req_encode((uint8_t *)buf + sizeof(vemb_v16_net_hdr_t),
                            buf_cap - sizeof(vemb_v16_net_hdr_t),
                            &req, &payload_len) != 0) {
        return -1;
    }
    size_t total = sizeof(vemb_v16_net_hdr_t) + payload_len;

    vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)buf;
    memset(hdr, 0, sizeof(*hdr));
    hdr->magic = VEMB_V16_MAGIC;
    hdr->version = VEMB_V16_VERSION;
    hdr->type = VEMB_V16_NET_REQUEST;
    hdr->payload_len = (uint32_t)payload_len;
    hdr->channel_id = channel_id;
    hdr->req_id = req_id;

    return (ssize_t)total;
}

ssize_t vemb_v16_parse_welcome(const void *buf, size_t buf_len,
                               vemb_v16_channel_desc_t *out_desc)
{
    if (!buf || !out_desc)
        return -1;

    if (buf_len < sizeof(vemb_v16_net_hdr_t))
        return 0;

    const vemb_v16_net_hdr_t *hdr = (const vemb_v16_net_hdr_t *)buf;
    if (hdr->magic != VEMB_V16_MAGIC ||
        hdr->version != VEMB_V16_VERSION ||
        hdr->type != VEMB_V16_NET_WELCOME)
        return -1;

    size_t need = sizeof(*hdr) + hdr->payload_len;
    if (buf_len < need)
        return 0;

    if (vemb_v16_channel_desc_decode(out_desc,
                                     (const uint8_t *)buf + sizeof(*hdr),
                                     hdr->payload_len) != 0)
        return -1;

    return (ssize_t)need;
}

ssize_t vemb_v16_parse_response(const void *buf, size_t buf_len,
                                vemb_v16_resp_t *out_resp,
                                size_t *out_inline_bytes)
{
    if (!buf || !out_resp)
        return -1;
    if (out_inline_bytes)
        *out_inline_bytes = 0;

    if (buf_len < sizeof(vemb_v16_net_hdr_t))
        return 0;

    const vemb_v16_net_hdr_t *hdr = (const vemb_v16_net_hdr_t *)buf;
    if (hdr->magic != VEMB_V16_MAGIC ||
        hdr->version != VEMB_V16_VERSION ||
        hdr->type != VEMB_V16_NET_RESPONSE)
        return -1;

    size_t need = sizeof(*hdr) + hdr->payload_len;
    if (buf_len < need)
        return 0;

    if (hdr->payload_len < vemb_v16_resp_encoded_base_len())
        return -1;

    if (vemb_v16_resp_decode(out_resp,
                             (const uint8_t *)buf + sizeof(*hdr),
                             hdr->payload_len) != 0) {
        return -1;
    }

    /* VEMB_INLINE OK responses append the inline vector payload after the
     * 6B+28B handle metadata. Report the trailing byte count so callers can
     * extract the vector from the frame. */
    if (out_inline_bytes) {
        size_t metadata_len = vemb_v16_resp_encoded_len(out_resp);
        *out_inline_bytes = (hdr->payload_len >= metadata_len)
                              ? (hdr->payload_len - metadata_len)
                              : 0;
    }

    return (ssize_t)need;
}

ssize_t vemb_v16_serialize_vsim_inline(void *buf, size_t buf_cap,
                                       uint64_t channel_id, uint32_t req_id,
                                       const char *key, uint32_t key_len,
                                       const float *query_vector, uint32_t dim)
{
    if (!key || key_len == 0 || key_len >= VEMB_V16_MAX_KEY_LEN ||
        !query_vector || dim == 0 || dim > VEMB_V16_MAX_DIM) {
        return -1;
    }

    vemb_v16_req_t req = {0};
    req.op = VEMB_V16_OP_VSIM_INLINE;
    req.flags = 0;
    req.req_id = req_id;
    req.channel_id = channel_id;
    req.key_len = key_len;
    memcpy(req.key, key, key_len);
    req.dim = dim;
    req.vector_bytes = dim * sizeof(float);
    memcpy(req.vector, query_vector, dim * sizeof(float));

    if (buf_cap < sizeof(vemb_v16_net_hdr_t))
        return -1;

    size_t payload_len = 0;
    if (vemb_v16_req_encode((uint8_t *)buf + sizeof(vemb_v16_net_hdr_t),
                            buf_cap - sizeof(vemb_v16_net_hdr_t),
                            &req, &payload_len) != 0) {
        return -1;
    }
    size_t total = sizeof(vemb_v16_net_hdr_t) + payload_len;

    vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)buf;
    memset(hdr, 0, sizeof(*hdr));
    hdr->magic = VEMB_V16_MAGIC;
    hdr->version = VEMB_V16_VERSION;
    hdr->type = VEMB_V16_NET_REQUEST;
    hdr->payload_len = (uint32_t)payload_len;
    hdr->channel_id = channel_id;
    hdr->req_id = req_id;

    return (ssize_t)total;
}

/* ------------------------------------------------------------------ */
/* Internal helpers for sync API                                      */
/* ------------------------------------------------------------------ */

static int sdk_open_warm_region(sdk_backend_t *b,
                                const vemb_v16_channel_desc_t *desc)
{
    void *mapping_addr = NULL;
    size_t mapping_bytes = 0;
    void *mapped_addr = NULL;
    uint64_t region_bytes = 0;

    if (vemb_v16_open_warm_region(desc, &mapping_addr, &mapping_bytes,
                                  &mapped_addr, &region_bytes) != 0) {
        return -1;
    }

    b->warm_region_bytes = region_bytes;
    b->mapping_bytes     = mapping_bytes;
    b->mapping_addr      = (uint8_t *)mapping_addr;
    b->mapped_addr       = (uint8_t *)mapped_addr;
    return 0;
}

static void sdk_close_warm_region(sdk_backend_t *b)
{
    if (b->mapping_addr) {
        vemb_v16_close_warm_region(b->mapping_addr, b->mapping_bytes);
        b->mapping_addr = NULL;
        b->mapped_addr  = NULL;
    }
}

/* Connect one backend: TCP connect, HELLO/WELCOME, mmap warm region.
 * Expects b->host and b->port to be already populated by the caller.
 * Idempotent: if b->fd >= 0, returns 0 immediately.
 * On WELCOME, copies channel_id into b->channel_id and attempts mmap.
 * mmap failure is non-fatal (e.g. cross-node owner has no local warm region):
 * mapping_addr and mapped_addr are left NULL. */
static int sdk_backend_connect(sdk_backend_t *b, uint32_t dim, uint32_t timeout_ms)
{
    if (!b || b->fd >= 0)
        return 0;
    if (!b->host[0])
        return -1;

    uint32_t effective_timeout = timeout_ms ? timeout_ms : 10000;
    int fd = -1;
    for (int retry = 0; retry < 50; retry++) {
        fd = vemb_v16_net_connect(b->host, b->port, effective_timeout);
        if (fd >= 0) break;
        usleep(100000);
    }
    if (fd < 0) {
        fprintf(stderr, "vemb_v16_client: connect %s:%u failed\n",
                b->host, b->port);
        return -1;
    }

    char hello_buf[64];
    ssize_t hello_len = vemb_v16_serialize_hello(hello_buf, sizeof(hello_buf),
                                                  dim, 0);
    if (hello_len < 0 ||
        vemb_v16_net_write_full(fd, hello_buf, (size_t)hello_len) != 0) {
        close(fd); return -1;
    }

    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_WELCOME) {
        close(fd); return -1;
    }

    /* WELCOME payload uses the compact channel_desc_encode format
     * (matches server-side vemb_v16_channel_desc_encode in proxy.c).
     * Max encoded size: 452 + 16 * 280 = 4932 bytes. */
    uint8_t payload[5120];
    if (hdr.payload_len > sizeof(payload)) {
        close(fd); return -1;
    }
    if (vemb_v16_net_read_full(fd, payload, hdr.payload_len) != 0) {
        close(fd); return -1;
    }

    vemb_v16_channel_desc_t desc;
    if (vemb_v16_channel_desc_decode(&desc, payload, hdr.payload_len) != 0) {
        close(fd); return -1;
    }

    b->fd         = fd;
    b->channel_id = desc.channel_id;

    /* mmap warm region — non-fatal if it fails (cross-node owner may
     * not have a local warm region). */
    if (sdk_open_warm_region(b, &desc) != 0) {
        b->mapping_addr  = NULL;
        b->mapped_addr   = NULL;
        b->mapping_bytes = 0;
    }
    return 0;
}

/* Fetch topology using a FRESH control connection to seed_conns[idx]'s
 * host/port. The server's sniff path only honors TOPOLOGY_GET on a fresh
 * TCP connection (pre-HELLO); reusing an existing channel fd silently
 * fails. So we open a short-lived control connection here, fetch, close. */
static int fetch_topology_via_seed_conn(vemb_v16_client_t *client, int idx) {
    if (!client || idx < 0 || idx >= client->seed_conn_count)
        return -1;
    sdk_backend_t *seed = &client->seed_conns[idx];
    vemb_v16_topology_control_resp_t raw;
    memset(&raw, 0, sizeof(raw));
    int rc = vemb_v16_client_topology_fetch_tcp(
        seed->host, seed->port, 5000, &client->topology, &raw);
    if (rc != 0) {
        return -1;
    }
    client->topology_state = TOPO_READY;
    client->topology_refresh_calls++;
    return 0;
}

static void sdk_close_backend(sdk_backend_t *b)
{
    sdk_close_warm_region(b);
    if (b->fd >= 0) {
        char close_buf[32];
        memset(close_buf, 0, sizeof(close_buf));
        vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)close_buf;
        hdr->magic = VEMB_V16_MAGIC;
        hdr->version = VEMB_V16_VERSION;
        hdr->type = VEMB_V16_NET_CLOSE_CHANNEL;
        hdr->channel_id = b->channel_id;
        vemb_v16_net_write_full(b->fd, close_buf, sizeof(vemb_v16_net_hdr_t));
        close(b->fd);
        b->fd = -1;
    }
}

/* ---- Consistent-hash ring (private reimplementation, aligned with
 *      benchmark/vemb_v16_bench.c so routing decisions interoperate) ---- */

static int sdk_hash_node_cmp(const void *a, const void *b)
{
    const sdk_hash_node_t *ha = a;
    const sdk_hash_node_t *hb = b;
    if (ha->hash_value < hb->hash_value) return -1;
    if (ha->hash_value > hb->hash_value) return 1;
    if (ha->backend_idx < hb->backend_idx) return -1;
    if (ha->backend_idx > hb->backend_idx) return 1;
    return 0;
}

static void sdk_build_hash_ring(vemb_v16_client_t *c)
{
    if (!c || c->seed_conn_count <= 1) { c->hash_node_count = 0; return; }
    c->hash_node_count = 0;
    for (uint32_t node = 0; node < (uint32_t)c->seed_conn_count; node++) {
        for (uint32_t vnode = 0; vnode < VEMB_V16_SDK_HASH_VNODES; vnode++) {
            char vnode_key[64];
            uint32_t vnode_id = c->hash_node_count;
            snprintf(vnode_key, sizeof(vnode_key),
                     "supernode_%u_vnode_%u", node, vnode_id);
            c->hash_ring[c->hash_node_count++] = (sdk_hash_node_t){
                .hash_value  = vemb_v16_murmur3(vnode_key, strlen(vnode_key)),
                .backend_idx = node,
            };
        }
    }
    qsort(c->hash_ring, c->hash_node_count, sizeof(c->hash_ring[0]),
          sdk_hash_node_cmp);
}

/* Pick backend for key and sync the routed view. Pass NULL/"" for the
 * canonical "first backend" (used by ping/stats/etc). */
static int sdk_route(vemb_v16_client_t *c, const char *key)
{
    if (!c || c->seed_conn_count <= 0) return -1;
    int idx = 0;
    if (c->seed_conn_count > 1 && key && key[0]) {
        uint32_t hash = vemb_v16_murmur3(key, strlen(key));
        uint32_t left = 0, right = c->hash_node_count;
        while (left < right) {
            uint32_t mid = left + (right - left) / 2;
            if (c->hash_ring[mid].hash_value < hash) left = mid + 1;
            else right = mid;
        }
        if (left >= c->hash_node_count) left = 0;
        idx = (int)c->hash_ring[left].backend_idx;
    }
    sdk_backend_t *b = &c->seed_conns[idx];
    c->fd                = b->fd;
    c->channel_id        = b->channel_id;
    memcpy(c->host, b->host, sizeof(c->host));
    c->port              = b->port;
    c->warm_region_bytes = b->warm_region_bytes;
    c->warm_mmap_offset  = b->warm_mmap_offset;
    c->mapping_bytes     = b->mapping_bytes;
    c->mapping_addr      = b->mapping_addr;
    c->mapped_addr       = b->mapped_addr;
    c->cur_idx           = idx;
    return 0;
}

/* Parse "host:port" — last ':' wins (IPv6 friendly enough for our use). */
static int sdk_parse_endpoint(const char *s, char *host, size_t host_cap,
                              uint16_t *port)
{
    const char *colon = strrchr(s, ':');
    if (!colon || colon == s) return -1;
    size_t host_len = (size_t)(colon - s);
    if (host_len >= host_cap) return -1;
    memcpy(host, s, host_len);
    host[host_len] = '\0';
    char *end = NULL;
    long p = strtol(colon + 1, &end, 10);
    if (end == colon + 1 || *end != '\0' || p <= 0 || p > 65535) return -1;
    *port = (uint16_t)p;
    return 0;
}

static int read_response(vemb_v16_client_t *c,
                         vemb_v16_resp_t *resp,
                         uint8_t *inline_vector,
                         uint32_t inline_vector_cap,
                         uint32_t *inline_vector_bytes)
{
    if (inline_vector_bytes) *inline_vector_bytes = 0;

    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_full(c->fd, &hdr, sizeof(hdr)) != 0)
        return -1;
    if (hdr.magic != VEMB_V16_MAGIC || hdr.version != VEMB_V16_VERSION ||
        hdr.type != VEMB_V16_NET_RESPONSE ||
        hdr.channel_id != c->channel_id ||
        hdr.payload_len < vemb_v16_resp_encoded_base_len()) {
        return -1;
    }

    /* Compact wire format: 6B base (status + op|flags + req_id) +
     * op-specific metadata body (max 28B for VEMB_HANDLE/INLINE) +
     * optional inline-vector tail (VEMB_INLINE only). Read them in three
     * slices so we can stream the inline tail directly into the caller's
     * buffer without an intermediate heap allocation. */
    uint8_t base[6];  /* VEMB_V16_RESP_ENCODED_BASE_LEN */
    if (vemb_v16_net_read_full(c->fd, base, sizeof(base)) != 0)
        return -1;
    uint8_t status = base[0];
    uint8_t op = (uint8_t)(base[1] & VEMB_V16_TCP_RESP_OP_MASK);
    size_t metadata_len = vemb_v16_resp_encoded_len_for_fields(status, op);
    if (metadata_len > hdr.payload_len)
        return -1;

    size_t body_len = metadata_len - sizeof(base);
    uint8_t body[28];  /* metadata body max (VEMB_HANDLE/INLINE) */
    if (body_len > 0) {
        if (body_len > sizeof(body))
            return -1;
        if (vemb_v16_net_read_full(c->fd, body, body_len) != 0)
            return -1;
    }

    uint8_t meta[34];  /* base + body max */
    memcpy(meta, base, sizeof(base));
    if (body_len > 0)
        memcpy(meta + sizeof(base), body, body_len);
    if (vemb_v16_resp_decode(resp, meta, metadata_len) != 0)
        return -1;

    /* Stream inline-vector tail (if any) directly into the caller's buffer. */
    size_t inline_bytes = hdr.payload_len - metadata_len;
    if (inline_bytes > 0) {
        if (!inline_vector || inline_bytes > inline_vector_cap)
            return -1;
        if (vemb_v16_net_read_full(c->fd, inline_vector, inline_bytes) != 0)
            return -1;
        if (inline_vector_bytes)
            *inline_vector_bytes = (uint32_t)inline_bytes;
    }
    return 0;
}

/* Read one response frame from a specific backend (by fd + channel_id).
 * Same streaming logic as read_response() but parameterized so the retry
 * engine can read from an owner_channel without syncing the routed view.
 * out_inline (if non-NULL) receives the inline vector bytes; the caller
 * must provide out_inline_cap bytes of storage. */
static int recv_resp_backend(int fd, uint64_t channel_id,
                             vemb_v16_resp_t *out_resp,
                             uint8_t *out_inline, uint32_t out_inline_cap,
                             uint32_t *out_inline_bytes)
{
    if (out_inline_bytes) *out_inline_bytes = 0;

    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_full(fd, &hdr, sizeof(hdr)) != 0)
        return -1;
    if (hdr.magic != VEMB_V16_MAGIC || hdr.version != VEMB_V16_VERSION ||
        hdr.type != VEMB_V16_NET_RESPONSE ||
        hdr.channel_id != channel_id ||
        hdr.payload_len < vemb_v16_resp_encoded_base_len()) {
        return -1;
    }

    uint8_t base[6];
    if (vemb_v16_net_read_full(fd, base, sizeof(base)) != 0)
        return -1;
    uint8_t status = base[0];
    uint8_t op = (uint8_t)(base[1] & VEMB_V16_TCP_RESP_OP_MASK);
    size_t metadata_len = vemb_v16_resp_encoded_len_for_fields(status, op);
    if (metadata_len > hdr.payload_len)
        return -1;

    size_t body_len = metadata_len - sizeof(base);
    uint8_t body[28];
    if (body_len > 0) {
        if (body_len > sizeof(body))
            return -1;
        if (vemb_v16_net_read_full(fd, body, body_len) != 0)
            return -1;
    }

    uint8_t meta[34];
    memcpy(meta, base, sizeof(base));
    if (body_len > 0)
        memcpy(meta + sizeof(base), body, body_len);
    if (vemb_v16_resp_decode(out_resp, meta, metadata_len) != 0)
        return -1;

    size_t inline_bytes = hdr.payload_len - metadata_len;
    if (inline_bytes > 0) {
        if (!out_inline || inline_bytes > out_inline_cap)
            return -1;
        if (vemb_v16_net_read_full(fd, out_inline, inline_bytes) != 0)
            return -1;
        if (out_inline_bytes)
            *out_inline_bytes = (uint32_t)inline_bytes;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* Public Sync API                                                    */
/* ------------------------------------------------------------------ */

vemb_v16_client_t *vemb_v16_client_create_multi(const char *endpoints[],
                                                 int endpoint_count,
                                                 uint32_t dim,
                                                 uint32_t timeout_ms)
{
    if (!endpoints || endpoint_count <= 0 ||
        endpoint_count > VEMB_V16_SDK_MAX_ENDPOINTS ||
        dim == 0 || dim > VEMB_V16_MAX_DIM)
        return NULL;

    vemb_v16_client_t *c = calloc(1, sizeof(*c));
    if (!c) return NULL;
    c->dim               = dim;
    c->req_id            = 1;
    c->seed_conn_count   = endpoint_count;
    c->connect_timeout_ms = timeout_ms;

    for (int i = 0; i < endpoint_count; i++) {
        char host[64]; uint16_t port;
        if (sdk_parse_endpoint(endpoints[i], host, sizeof(host), &port) != 0) {
            fprintf(stderr, "vemb_v16_client: bad endpoint '%s'\n", endpoints[i]);
            for (int j = 0; j < i; j++) sdk_close_backend(&c->seed_conns[j]);
            free(c); return NULL;
        }
        sdk_backend_t *b = &c->seed_conns[i];
        memset(b, 0, sizeof(*b));
        b->fd = -1;
        strncpy(b->host, host, sizeof(b->host) - 1);
        b->host[sizeof(b->host) - 1] = '\0';
        b->port = port;
        if (sdk_backend_connect(b, dim, timeout_ms) != 0) {
            for (int j = 0; j < i; j++) sdk_close_backend(&c->seed_conns[j]);
            free(c); return NULL;
        }
    }

    sdk_build_hash_ring(c);
    sdk_route(c, NULL);  /* sync view to backend[0] */

    c->retry_budget = 256;  /* covers worst-case migration window */
    c->topology_state = TOPO_UNINITIALIZED;
    return c;
}

int vemb_v16_route_key(const char *endpoints[], int endpoint_count,
                       const char *key, int *out_backend_idx)
{
    if (!endpoints || endpoint_count <= 0 ||
        endpoint_count > VEMB_V16_SDK_MAX_ENDPOINTS || !out_backend_idx)
        return -1;

    /* Validate endpoint format (same rules as sdk_parse_endpoint). */
    for (int i = 0; i < endpoint_count; i++) {
        if (!endpoints[i]) return -1;
        const char *colon = strrchr(endpoints[i], ':');
        if (!colon || colon == endpoints[i]) return -1;
        char *end = NULL;
        long p = strtol(colon + 1, &end, 10);
        if (end == colon + 1 || *end != '\0' || p <= 0 || p > 65535)
            return -1;
    }

    if (endpoint_count == 1) {
        *out_backend_idx = 0;
        return 0;
    }

    sdk_hash_node_t ring[VEMB_V16_SDK_MAX_ENDPOINTS * VEMB_V16_SDK_HASH_VNODES];
    uint32_t node_count = 0;
    for (uint32_t node = 0; node < (uint32_t)endpoint_count; node++) {
        for (uint32_t vnode = 0; vnode < VEMB_V16_SDK_HASH_VNODES; vnode++) {
            char vnode_key[64];
            uint32_t vnode_id = node_count;
            snprintf(vnode_key, sizeof(vnode_key),
                     "supernode_%u_vnode_%u", node, vnode_id);
            ring[node_count++] = (sdk_hash_node_t){
                .hash_value  = vemb_v16_murmur3(vnode_key, strlen(vnode_key)),
                .backend_idx = node,
            };
        }
    }
    qsort(ring, node_count, sizeof(ring[0]), sdk_hash_node_cmp);

    uint32_t hash = vemb_v16_murmur3(key, strlen(key));
    uint32_t left = 0, right = node_count;
    while (left < right) {
        uint32_t mid = left + (right - left) / 2;
        if (ring[mid].hash_value < hash) left = mid + 1;
        else right = mid;
    }
    if (left >= node_count) left = 0;
    *out_backend_idx = (int)ring[left].backend_idx;
    return 0;
}

vemb_v16_client_t *vemb_v16_client_create(const char *host,
                                          uint16_t port,
                                          uint32_t dim,
                                          uint32_t timeout_ms)
{
    if (!host || dim == 0 || dim > VEMB_V16_MAX_DIM) return NULL;
    char ep[80];
    snprintf(ep, sizeof(ep), "%s:%u", host, port);
    const char *endpoints[1] = { ep };
    return vemb_v16_client_create_multi(endpoints, 1, dim, timeout_ms);
}

void vemb_v16_client_destroy(vemb_v16_client_t *c)
{
    if (!c) return;
    for (int i = 0; i < c->seed_conn_count; i++)
        sdk_close_backend(&c->seed_conns[i]);
    /* Close any lazily-opened owner channels. */
    for (uint32_t i = 0; i < VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS; i++) {
        if (c->owner_channel_inited[i])
            sdk_close_backend(&c->owner_channels[i]);
    }
    free(c);
}

/* Lazy-open a TCP connection to the given owner_id using endpoint info from
 * the cached topology. Returns 0 on success (channel ready in
 * owner_channels[owner_id]), -1 on failure. Idempotent: if channel already
 * open, returns 0 without re-opening.
 *
 * Not yet called by any code path — Task 7's retry engine will invoke this
 * when it receives a MOVED/ASK redirect that targets a different owner. */
static int ensure_owner_channel(vemb_v16_client_t *client, uint32_t owner_id)
{
    if (!client || owner_id >= VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS)
        return -1;
    if (client->owner_channel_inited[owner_id])
        return 0;

    const vemb_v16_topology_endpoint_t *ep =
        vemb_v16_client_topology_find_endpoint(&client->topology, owner_id);
    if (!ep || !ep->host[0]) {
        fprintf(stderr, "vemb_v16_client: no endpoint for owner_id=%u\n",
                owner_id);
        return -1;
    }

    sdk_backend_t *b = &client->owner_channels[owner_id];
    memset(b, 0, sizeof(*b));
    b->fd = -1;
    strncpy(b->host, ep->host, sizeof(b->host) - 1);
    b->host[sizeof(b->host) - 1] = '\0';
    b->port = ep->tcp_port;

    if (sdk_backend_connect(b, client->dim, client->connect_timeout_ms) != 0) {
        /* leave b->fd = -1; caller will handle send failure */
        return -1;
    }
    client->owner_channel_inited[owner_id] = 1;
    return 0;
}

/* ------------------------------------------------------------------ */
/* Sync retry engine                                                  */
/* ------------------------------------------------------------------ */

/* Execute a single-key op with transparent redirect/retry.
 *
 * Single-endpoint fast path: if seed_conn_count == 1 and topology has not
 * been requested (TOPO_UNINITIALIZED), use seed_conns[0] directly — no
 * topology fetch, no owner channel. This preserves the existing
 * zero-latency-overhead path for the common single-server case.
 *
 * Multi-endpoint path: lazy topology fetch on first call, then route by
 * key_hash to the active owner via vemb_v16_client_topology_plan_write.
 * On ASK: one-shot redirect to redirect_owner (resend with
 * VEMB_V16_REQ_F_ASK_REDIRECT flag). On MOVED/STALE: refresh topology
 * and retry from the top of the loop.
 *
 * Returns 0 on success or fatal-error-via-status (caller checks
 * out_resp->status). Returns -1 on network/protocol failure or budget
 * exhaustion.
 *
 * On return, out_resp is populated with the last server response. For
 * VEMB_INLINE, the inline vector bytes are streamed into out_inline /
 * out_inline_cap (caller-allocated). For other ops, pass NULL/0. */
static int client_execute_with_redirect(
        vemb_v16_client_t *client,
        uint8_t  op_type,
        const char *key, uint32_t key_len,
        const float *payload, uint32_t dim,
        vemb_v16_resp_t *out_resp,
        uint8_t *out_inline, uint32_t out_inline_cap,
        uint32_t *out_inline_bytes)
{
    if (!client || !key || key_len == 0 || !out_resp)
        return -1;
    memset(out_resp, 0, sizeof(*out_resp));
    if (out_inline_bytes) *out_inline_bytes = 0;

    int single_endpoint = (client->seed_conn_count == 1);

    /* Multi-endpoint: ensure topology is loaded before first op. */
    if (!single_endpoint && client->topology_state != TOPO_READY) {
        if (fetch_topology_via_seed_conn(client, 0) != 0)
            return -1;
    }

    for (uint32_t attempt = 0; attempt < client->retry_budget; attempt++) {
        sdk_backend_t *target;
        uint64_t topology_epoch = 0;
        uint32_t active_owner = 0;  /* only meaningful when !single_endpoint */

        if (single_endpoint) {
            /* Fast path: seed_conns[0] is the only backend. */
            target = &client->seed_conns[0];
        } else {
            uint32_t key_hash_u32 = vemb_v16_murmur3(key, key_len);
            vemb_v16_client_write_plan_t plan;
            memset(&plan, 0, sizeof(plan));
            if (vemb_v16_client_topology_plan_write(
                    &client->topology,
                    (uint64_t)key_hash_u32,
                    &plan) != 0)
                return -1;
            topology_epoch = plan.topology_epoch;
            active_owner = plan.active_owner;
            if (ensure_owner_channel(client, active_owner) != 0)
                return -1;
            target = &client->owner_channels[active_owner];
        }

        if (target->fd < 0)
            return -1;

        /* Build request */
        vemb_v16_req_t req;
        memset(&req, 0, sizeof(req));
        req.op = op_type;
        req.req_id = client->req_id++;
        req.channel_id = target->channel_id;
        req.key_len = key_len;
        memcpy(req.key, key, key_len);
        req.key_hash = vemb_v16_murmur3(req.key, key_len);
        req.topology_epoch = topology_epoch;
        req.dim = dim;

        switch (op_type) {
        case VEMB_V16_OP_VADD:
        case VEMB_V16_OP_VSIM_INLINE:
            if (!payload || dim == 0)
                return -1;
            req.vector_bytes = dim * sizeof(float);
            memcpy(req.vector, payload, req.vector_bytes);
            break;
        case VEMB_V16_OP_VEMB_HANDLE:
        case VEMB_V16_OP_VEMB_INLINE:
            if (dim == 0)
                return -1;
            req.vector_bytes = dim * sizeof(float);
            break;
        case VEMB_V16_OP_VREM:
            req.dim = 0;
            req.vector_bytes = 0;
            break;
        default:
            return -1;
        }

        if (sdk_write_request(target->fd, target->channel_id,
                              req.req_id, &req) != 0) {
            close(target->fd);
            target->fd = -1;
            if (!single_endpoint)
                client->owner_channel_inited[active_owner] = 0;
            return -1;
        }

        /* Read response from target backend */
        if (recv_resp_backend(target->fd, target->channel_id,
                              out_resp,
                              out_inline, out_inline_cap,
                              out_inline_bytes) != 0) {
            close(target->fd);
            target->fd = -1;
            if (!single_endpoint)
                client->owner_channel_inited[active_owner] = 0;
            return -1;
        }

        /* Classify + act */
        vemb_v16_resp_class_t cls = vemb_v16_classify_resp_status(out_resp->status);
        if (cls == VEMB_V16_RESP_CLASS_OK ||
            cls == VEMB_V16_RESP_CLASS_NOT_FOUND ||
            cls == VEMB_V16_RESP_CLASS_FATAL) {
            return 0;  /* FATAL surfaces ERR via out_resp->status */
        }

        if (cls == VEMB_V16_RESP_CLASS_ASK) {
            /* One-shot redirect: resend to redirect_owner with
             * VEMB_V16_REQ_F_ASK_REDIRECT flag, read response, done. */
            client->ask_redirects++;
            uint32_t ask_owner = out_resp->redirect_owner;
            if (ensure_owner_channel(client, ask_owner) != 0)
                return -1;
            sdk_backend_t *ask_target = &client->owner_channels[ask_owner];
            if (ask_target->fd < 0)
                return -1;

            vemb_v16_req_t ask_req;
            memset(&ask_req, 0, sizeof(ask_req));
            ask_req.op = op_type;
            ask_req.flags = VEMB_V16_REQ_F_ASK_REDIRECT;
            ask_req.req_id = client->req_id++;
            ask_req.channel_id = ask_target->channel_id;
            ask_req.key_len = key_len;
            memcpy(ask_req.key, key, key_len);
            ask_req.key_hash = vemb_v16_murmur3(ask_req.key, key_len);
            ask_req.topology_epoch = topology_epoch;
            ask_req.dim = dim;

            switch (op_type) {
            case VEMB_V16_OP_VADD:
            case VEMB_V16_OP_VSIM_INLINE:
                ask_req.vector_bytes = dim * sizeof(float);
                memcpy(ask_req.vector, payload, ask_req.vector_bytes);
                break;
            case VEMB_V16_OP_VEMB_HANDLE:
            case VEMB_V16_OP_VEMB_INLINE:
                ask_req.vector_bytes = dim * sizeof(float);
                break;
            case VEMB_V16_OP_VREM:
                ask_req.dim = 0;
                ask_req.vector_bytes = 0;
                break;
            default:
                return -1;
            }

            if (sdk_write_request(ask_target->fd, ask_target->channel_id,
                                  ask_req.req_id, &ask_req) != 0) {
                close(ask_target->fd);
                ask_target->fd = -1;
                client->owner_channel_inited[ask_owner] = 0;
                return -1;
            }

            if (recv_resp_backend(ask_target->fd, ask_target->channel_id,
                                  out_resp,
                                  out_inline, out_inline_cap,
                                  out_inline_bytes) != 0) {
                close(ask_target->fd);
                ask_target->fd = -1;
                client->owner_channel_inited[ask_owner] = 0;
                return -1;
            }

            vemb_v16_resp_class_t ask_cls =
                vemb_v16_classify_resp_status(out_resp->status);
            if (ask_cls == VEMB_V16_RESP_CLASS_OK ||
                ask_cls == VEMB_V16_RESP_CLASS_NOT_FOUND ||
                ask_cls == VEMB_V16_RESP_CLASS_FATAL) {
                return 0;  /* ASK resend resolved the op */
            }
            /* Still redirected: fall through to topology refresh */
        }

        /* REFRESH path: MOVED or STALE_TOPOLOGY (or ASK-that-still-redirected).
         * Mark topology stale, re-fetch, and retry from the top. */
        if (out_resp->status == VEMB_V16_STATUS_MOVED)
            client->moved_redirects++;
        else if (out_resp->status == VEMB_V16_STATUS_STALE_TOPOLOGY)
            client->stale_topology_responses++;
        client->topology_state = TOPO_STALE;
        if (fetch_topology_via_seed_conn(client, 0) != 0)
            return -1;
    }

    /* Budget exhausted */
    out_resp->status = VEMB_V16_STATUS_ERR;
    return 0;
}

/* ------------------------------------------------------------------ */
/* Pipeline retry engine                                               */
/* ------------------------------------------------------------------ */

/* Per-entry status for the pipeline engine. */
enum {
    PIPE_ENTRY_PENDING = 0,
    PIPE_ENTRY_OK,
    PIPE_ENTRY_NOT_FOUND,
    PIPE_ENTRY_ERR,
};

/* Per-entry auxiliary result data (offset/bytes/dim/region_id/score). */
typedef struct {
    uint64_t offset;
    uint32_t bytes;
    uint32_t dim;
    uint32_t region_id;
    float    score;
} vemb_v16_pipe_entry_aux_t;

/* Build a vemb_v16_req_t from the engine's flat input arrays. */
static void pipeline_build_req(vemb_v16_req_t *req,
                               uint8_t op_type,
                               uint32_t req_id,
                               uint64_t channel_id,
                               uint64_t topology_epoch,
                               uint32_t dim,
                               const char *key, uint32_t key_len,
                               const float *payload,
                               uint8_t ask_redirect_flag)
{
    memset(req, 0, sizeof(*req));
    req->op = op_type;
    req->flags = ask_redirect_flag;
    req->req_id = req_id;
    req->channel_id = channel_id;
    req->key_len = key_len;
    memcpy(req->key, key, key_len);
    req->key_hash = vemb_v16_murmur3(req->key, key_len);
    req->topology_epoch = topology_epoch;
    req->dim = dim;

    switch (op_type) {
    case VEMB_V16_OP_VADD:
    case VEMB_V16_OP_VSIM_INLINE:
        req->vector_bytes = dim * sizeof(float);
        if (payload)
            memcpy(req->vector, payload, req->vector_bytes);
        break;
    case VEMB_V16_OP_VEMB_HANDLE:
    case VEMB_V16_OP_VEMB_INLINE:
        req->vector_bytes = dim * sizeof(float);
        break;
    case VEMB_V16_OP_VREM:
        req->dim = 0;
        req->vector_bytes = 0;
        break;
    default:
        break;
    }
}

/* Fill out_entry_aux from a response. */
static void pipeline_fill_aux(vemb_v16_pipe_entry_aux_t *aux,
                              const vemb_v16_resp_t *resp,
                              uint32_t client_dim)
{
    if (!aux) return;
    aux->offset    = resp->vector_offset;
    aux->bytes     = resp->vector_bytes;
    aux->dim       = resp->dim > 0 ? resp->dim : client_dim;
    aux->region_id = resp->region_id;
    aux->score     = resp->score;
}

/* Execute a batch of pipeline entries with transparent per-entry redirect/retry.
 *
 * Algorithm:
 * 1. Single-endpoint fast path: if seed_conn_count == 1, send all entries
 *    through seed_conns[0], no topology fetch.
 * 2. Multi-endpoint: lazy topology fetch on entry. Loop bounded by
 *    client->retry_budget. Group PENDING entries by active_owner, send each
 *    group as a pipelined burst, recv in send-order, classify each.
 * 3. Per-entry: OK→terminal; NOT_FOUND→terminal; ASK→one-shot resend to
 *    redirect_owner (final); MOVED/STALE→leave PENDING, refresh topology,
 *    retry; ERR→terminal.
 *
 * Returns 0 if the engine completed (caller inspects out_entry_status per
 * entry). Returns -1 only on local I/O catastrophe. On budget exhaustion,
 * remaining PENDING entries become ERR and return 0.
 */
static int client_pipeline_execute_with_redirect(
        vemb_v16_client_t *client,
        uint8_t  op_type,
        const char **keys, const uint32_t *key_lens,
        const float **payloads,        /* vectors for VADD/VSIM, NULL otherwise */
        uint32_t count,
        uint8_t  *out_entry_status,    /* [count], PIPE_ENTRY_* */
        vemb_v16_pipe_entry_aux_t *out_entry_aux,  /* [count], optional */
        uint32_t max_inflight,
        uint8_t  **out_inline_bufs)    /* [count], per-entry inline capture, NULL OK */
{
    if (!client || !keys || !key_lens || count == 0 || !out_entry_status)
        return -1;
    if (max_inflight == 0) max_inflight = count;

    for (uint32_t i = 0; i < count; i++)
        out_entry_status[i] = PIPE_ENTRY_PENDING;

    int single_endpoint = (client->seed_conn_count == 1);

    /* Multi-endpoint: ensure topology is loaded before first op. */
    if (!single_endpoint && client->topology_state != TOPO_READY) {
        if (fetch_topology_via_seed_conn(client, 0) != 0)
            return -1;
    }

    /* Outer retry loop — bounded by retry_budget. */
    for (uint32_t attempt = 0; attempt < client->retry_budget; attempt++) {
        /* Check if all entries are terminal. */
        uint32_t pending_count = 0;
        for (uint32_t i = 0; i < count; i++) {
            if (out_entry_status[i] == PIPE_ENTRY_PENDING)
                pending_count++;
        }
        if (pending_count == 0)
            return 0;  /* all resolved */

        /* Compute owner for each pending entry (single-endpoint: owner=0). */
        /* For multi-endpoint, we need per-entry owner. Use a VLA sized to
         * count (few owners, many entries). */
        uint32_t owners[count];
        uint64_t topology_epoch = 0;

        for (uint32_t i = 0; i < count; i++) {
            owners[i] = 0;  /* default for single-endpoint */
        }

        if (!single_endpoint) {
            for (uint32_t i = 0; i < count; i++) {
                if (out_entry_status[i] != PIPE_ENTRY_PENDING)
                    continue;
                uint32_t key_hash_u32 = vemb_v16_murmur3(keys[i], key_lens[i]);
                vemb_v16_client_write_plan_t plan;
                memset(&plan, 0, sizeof(plan));
                if (vemb_v16_client_topology_plan_write(
                        &client->topology,
                        (uint64_t)key_hash_u32,
                        &plan) != 0) {
                    /* Can't plan — mark as ERR. */
                    out_entry_status[i] = PIPE_ENTRY_ERR;
                    continue;
                }
                owners[i] = plan.active_owner;
                topology_epoch = plan.topology_epoch;
            }
        }

        /* Collect distinct owners (linear dedup, small N). */
        uint32_t distinct_owners[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];
        uint32_t distinct_count = 0;
        for (uint32_t i = 0; i < count; i++) {
            if (out_entry_status[i] != PIPE_ENTRY_PENDING)
                continue;
            uint32_t ow = owners[i];
            int found = 0;
            for (uint32_t j = 0; j < distinct_count; j++) {
                if (distinct_owners[j] == ow) { found = 1; break; }
            }
            if (!found && distinct_count < VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS)
                distinct_owners[distinct_count++] = ow;
        }

        int any_refresh = 0;

        /* Process each distinct owner's group. */
        for (uint32_t oi = 0; oi < distinct_count; oi++) {
            uint32_t owner = distinct_owners[oi];

            /* Resolve the backend for this owner. */
            sdk_backend_t *target;
            if (single_endpoint) {
                target = &client->seed_conns[0];
            } else {
                if (ensure_owner_channel(client, owner) != 0) {
                    /* Can't open channel — mark this owner's entries ERR. */
                    for (uint32_t i = 0; i < count; i++) {
                        if (out_entry_status[i] == PIPE_ENTRY_PENDING &&
                            owners[i] == owner)
                            out_entry_status[i] = PIPE_ENTRY_ERR;
                    }
                    continue;
                }
                target = &client->owner_channels[owner];
            }
            if (target->fd < 0) {
                for (uint32_t i = 0; i < count; i++) {
                    if (out_entry_status[i] == PIPE_ENTRY_PENDING &&
                        owners[i] == owner)
                        out_entry_status[i] = PIPE_ENTRY_ERR;
                }
                continue;
            }

            /* Collect the indices of pending entries for this owner. */
            uint32_t group_indices[count];
            uint32_t group_count = 0;
            for (uint32_t i = 0; i < count; i++) {
                if (out_entry_status[i] == PIPE_ENTRY_PENDING &&
                    owners[i] == owner)
                    group_indices[group_count++] = i;
            }
            if (group_count == 0)
                continue;

            /* Send-recv in bursts bounded by max_inflight.
             * Track which source index each sent request maps to.
             * Burst window: positions [burst_tail, burst_head) have been
             * sent but not yet recv'd. */
            uint32_t burst_src[count];  /* source entry index per burst slot */
            uint32_t burst_head = 0;    /* next slot to send */
            uint32_t burst_tail = 0;    /* next slot to recv */
            /* We'll use a simple cursor model: group_cursor advances through
             * group_indices as we send; we recv in send-order. */

            uint32_t group_cursor = 0;  /* next entry in group_indices to send */

            while (group_cursor < group_count || burst_tail < burst_head) {
                /* Send burst: fill up to max_inflight outstanding. */
                while (group_cursor < group_count &&
                       (burst_head - burst_tail) < max_inflight) {
                    uint32_t src_idx = group_indices[group_cursor];
                    vemb_v16_req_t req;
                    pipeline_build_req(&req, op_type,
                                       client->req_id++,
                                       target->channel_id,
                                       topology_epoch,
                                       client->dim,
                                       keys[src_idx], key_lens[src_idx],
                                       payloads ? payloads[src_idx] : NULL,
                                       0);  /* no ASK flag on initial send */
                    int dbg_wrc = sdk_write_request(target->fd, target->channel_id,
                                          req.req_id, &req);
                    if (dbg_wrc != 0) {
                        /* I/O error — mark remaining entries ERR. */
                        for (uint32_t k = group_cursor; k < group_count; k++) {
                            out_entry_status[group_indices[k]] = PIPE_ENTRY_ERR;
                        }
                        /* Also mark the in-flight ones ERR. */
                        for (uint32_t k = burst_tail; k < burst_head; k++) {
                            out_entry_status[burst_src[k]] = PIPE_ENTRY_ERR;
                        }
                        /* Close poisoned fd so next op reconnects. */
                        close(target->fd);
                        target->fd = -1;
                        if (!single_endpoint)
                            client->owner_channel_inited[owner] = 0;
                        burst_head = burst_tail;  /* nothing to recv */
                        group_cursor = group_count;
                        break;
                    }
                    burst_src[burst_head] = src_idx;
                    burst_head++;
                    group_cursor++;
                }

                /* Recv one response (in send-order). */
                if (burst_tail < burst_head) {
                    uint32_t src_idx = burst_src[burst_tail];
                    burst_tail++;

                    vemb_v16_resp_t resp;
                    memset(&resp, 0, sizeof(resp));
                    uint8_t *inl_buf = out_inline_bufs ? out_inline_bufs[src_idx] : NULL;
                    uint32_t inl_cap = inl_buf ? client->dim * sizeof(float) : 0;
                    uint32_t inl_bytes = 0;
                    int dbg_rc = recv_resp_backend(target->fd, target->channel_id,
                                          &resp, inl_buf, inl_cap, &inl_bytes);
                    if (dbg_rc != 0) {
                        /* I/O catastrophe on recv — mark this + remaining ERR. */
                        out_entry_status[src_idx] = PIPE_ENTRY_ERR;
                        for (uint32_t k = burst_tail; k < burst_head; k++) {
                            out_entry_status[burst_src[k]] = PIPE_ENTRY_ERR;
                        }
                        /* Mark remaining un-sent entries ERR too. */
                        for (uint32_t k = group_cursor; k < group_count; k++) {
                            out_entry_status[group_indices[k]] = PIPE_ENTRY_ERR;
                        }
                        /* Close poisoned fd so next op reconnects. */
                        close(target->fd);
                        target->fd = -1;
                        if (!single_endpoint)
                            client->owner_channel_inited[owner] = 0;
                        burst_head = burst_tail;
                        group_cursor = group_count;
                        continue;
                    }

                    /* Classify response. */
                    vemb_v16_resp_class_t cls =
                        vemb_v16_classify_resp_status(resp.status);

                    switch (cls) {
                    case VEMB_V16_RESP_CLASS_OK:
                        out_entry_status[src_idx] = PIPE_ENTRY_OK;
                        pipeline_fill_aux(
                            out_entry_aux ? &out_entry_aux[src_idx] : NULL,
                            &resp, client->dim);
                        break;
                    case VEMB_V16_RESP_CLASS_NOT_FOUND:
                        out_entry_status[src_idx] = PIPE_ENTRY_NOT_FOUND;
                        break;
                    case VEMB_V16_RESP_CLASS_ASK: {
                        /* One-shot resend to redirect_owner. */
                        uint32_t ask_owner = resp.redirect_owner;
                        if (ask_owner == owner) {
                            /* Self-ASK: can't interleave a resend on the same
                             * fd that has in-flight responses queued — that
                             * would desync the pipeline.  Treat as a hard
                             * error for this entry; the burst continues
                             * receiving remaining responses normally. */
                            out_entry_status[src_idx] = PIPE_ENTRY_ERR;
                            break;
                        }
                        client->ask_redirects++;
                        sdk_backend_t *ask_target;
                        if (single_endpoint) {
                            ask_target = &client->seed_conns[0];
                        } else {
                            if (ensure_owner_channel(client, ask_owner) != 0) {
                                out_entry_status[src_idx] = PIPE_ENTRY_ERR;
                                break;
                            }
                            ask_target = &client->owner_channels[ask_owner];
                        }
                        if (ask_target->fd < 0) {
                            out_entry_status[src_idx] = PIPE_ENTRY_ERR;
                            break;
                        }
                        vemb_v16_req_t ask_req;
                        pipeline_build_req(&ask_req, op_type,
                                           client->req_id++,
                                           ask_target->channel_id,
                                           topology_epoch,
                                           client->dim,
                                           keys[src_idx], key_lens[src_idx],
                                           payloads ? payloads[src_idx] : NULL,
                                           VEMB_V16_REQ_F_ASK_REDIRECT);
                        if (sdk_write_request(ask_target->fd,
                                              ask_target->channel_id,
                                              ask_req.req_id, &ask_req) != 0 ||
                            recv_resp_backend(ask_target->fd,
                                              ask_target->channel_id,
                                              &resp,
                                              out_inline_bufs ? out_inline_bufs[src_idx] : NULL,
                                              out_inline_bufs ? client->dim * sizeof(float) : 0,
                                              NULL) != 0) {
                            out_entry_status[src_idx] = PIPE_ENTRY_ERR;
                            break;
                        }
                        /* ASK result is final. */
                        vemb_v16_resp_class_t ask_cls =
                            vemb_v16_classify_resp_status(resp.status);
                        if (ask_cls == VEMB_V16_RESP_CLASS_OK) {
                            out_entry_status[src_idx] = PIPE_ENTRY_OK;
                            pipeline_fill_aux(
                                out_entry_aux ? &out_entry_aux[src_idx] : NULL,
                                &resp, client->dim);
                        } else if (ask_cls == VEMB_V16_RESP_CLASS_NOT_FOUND) {
                            out_entry_status[src_idx] = PIPE_ENTRY_NOT_FOUND;
                        } else {
                            out_entry_status[src_idx] = PIPE_ENTRY_ERR;
                        }
                        break;
                    }
                    case VEMB_V16_RESP_CLASS_REFRESH:
                        /* MOVED or STALE — leave PENDING, set refresh. */
                        if (resp.status == VEMB_V16_STATUS_MOVED)
                            client->moved_redirects++;
                        else
                            client->stale_topology_responses++;
                        any_refresh = 1;
                        break;
                    case VEMB_V16_RESP_CLASS_FATAL:
                    default:
                        out_entry_status[src_idx] = PIPE_ENTRY_ERR;
                        break;
                    }
                }
            }
        }

        /* If topology refresh needed, re-fetch and retry. */
        if (any_refresh) {
            if (single_endpoint) {
                /* Single-endpoint shouldn't get MOVED/STALE, but if we do,
                 * just give up on pending entries. */
                for (uint32_t i = 0; i < count; i++) {
                    if (out_entry_status[i] == PIPE_ENTRY_PENDING)
                        out_entry_status[i] = PIPE_ENTRY_ERR;
                }
                return 0;
            }
            client->topology_state = TOPO_STALE;
            if (fetch_topology_via_seed_conn(client, 0) != 0) {
                /* Can't refresh — remaining PENDING become ERR. */
                for (uint32_t i = 0; i < count; i++) {
                    if (out_entry_status[i] == PIPE_ENTRY_PENDING)
                        out_entry_status[i] = PIPE_ENTRY_ERR;
                }
                return 0;
            }
            /* Continue outer loop to re-route pending entries. */
            continue;
        }

        /* No refresh needed — check if anything is still pending. */
        pending_count = 0;
        for (uint32_t i = 0; i < count; i++) {
            if (out_entry_status[i] == PIPE_ENTRY_PENDING)
                pending_count++;
        }
        if (pending_count == 0)
            return 0;

        /* If nothing was refreshed but entries are still pending, they were
         * not sent (shouldn't happen in normal flow). Mark them ERR. */
        if (!any_refresh) {
            for (uint32_t i = 0; i < count; i++) {
                if (out_entry_status[i] == PIPE_ENTRY_PENDING)
                    out_entry_status[i] = PIPE_ENTRY_ERR;
            }
            return 0;
        }
    }

    /* Budget exhausted: remaining PENDING → ERR. */
    for (uint32_t i = 0; i < count; i++) {
        if (out_entry_status[i] == PIPE_ENTRY_PENDING)
            out_entry_status[i] = PIPE_ENTRY_ERR;
    }
    return 0;
}

int vemb_v16_client_vadd(vemb_v16_client_t *c,
                         const char *set_name,
                         const char *elem_name,
                         const float *vector,
                         uint32_t dim)
{
    if (!c || !elem_name || !vector || dim != c->dim)
        return -1;

    char combined[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                    set_name, elem_name, &key_len) != 0)
        return -1;

    vemb_v16_resp_t resp;
    if (client_execute_with_redirect(c, VEMB_V16_OP_VADD,
                                     combined, key_len,
                                     vector, dim,
                                     &resp, NULL, 0, NULL) != 0)
        return -1;

    return resp.status == VEMB_V16_STATUS_OK ? 0 : -1;
}

int vemb_v16_client_vemb_handle(vemb_v16_client_t *c,
                                const char *set_name,
                                const char *elem_name,
                                uint64_t *out_offset,
                                uint32_t *out_bytes,
                                uint32_t *out_dim,
                                uint32_t *out_region_id)
{
    if (!c || !elem_name)
        return -1;

    char combined[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                    set_name, elem_name, &key_len) != 0)
        return -1;

    vemb_v16_resp_t resp;
    if (client_execute_with_redirect(c, VEMB_V16_OP_VEMB_HANDLE,
                                     combined, key_len,
                                     NULL, c->dim,
                                     &resp, NULL, 0, NULL) != 0)
        return -1;

    if (resp.status == VEMB_V16_STATUS_NOT_FOUND)
        return 1;
    if (resp.status != VEMB_V16_STATUS_OK)
        return -1;

    if (out_offset)    *out_offset    = resp.vector_offset;
    if (out_bytes)     *out_bytes     = resp.vector_bytes;
    if (out_dim)       *out_dim       = resp.dim > 0 ? resp.dim : c->dim;
    if (out_region_id) *out_region_id = resp.region_id;
    return 0;
}

int vemb_v16_client_vrem(vemb_v16_client_t *c,
                         const char *set_name,
                         const char *elem_name)
{
    if (!c || !elem_name)
        return -1;

    char combined[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                    set_name, elem_name, &key_len) != 0)
        return -1;

    vemb_v16_resp_t resp;
    if (client_execute_with_redirect(c, VEMB_V16_OP_VREM,
                                     combined, key_len,
                                     NULL, 0,
                                     &resp, NULL, 0, NULL) != 0)
        return -1;

    /* VREM is idempotent: NOT_FOUND (key already gone) is fine */
    if (resp.status == VEMB_V16_STATUS_NOT_FOUND)
        return 0;
    if (resp.status != VEMB_V16_STATUS_OK)
        return -1;
    return 0;
}

int vemb_v16_client_vemb_vector(vemb_v16_client_t *c,
                                const char *set_name,
                                const char *elem_name,
                                float *out_vector,
                                uint32_t out_cap,
                                uint32_t *out_dim)
{
    if (!c || !elem_name || !out_vector || out_cap == 0)
        return -1;

    /* Fast path: get a handle from the server, then read the vector directly
     * from the locally mmap'd warm region.  This avoids sending 1200 B
     * payloads over TCP and is what gives the 3-4M ops/sec single-node
     * throughput.  If the region is not mmap'd or the handle read fails,
     * fall back to the inline-vector request via the retry engine. */
    if (c->mapped_addr) {
        uint64_t offset = 0;
        uint32_t bytes = 0;
        uint32_t dim = 0;
        int rc = vemb_v16_client_vemb_handle(c, set_name, elem_name,
                                             &offset, &bytes, &dim, NULL);
        if (rc == 0 &&
            vemb_v16_client_read_vector(c, offset, bytes,
                                        out_vector, out_cap) == 0) {
            if (out_dim)
                *out_dim = dim > 0 ? dim : c->dim;
            return 0;
        }
    }

    /* Fallback: VEMB_INLINE through the retry engine. */
    char combined[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                    set_name, elem_name, &key_len) != 0)
        return -1;

    vemb_v16_resp_t resp;
    uint32_t inline_bytes = 0;
    uint32_t inline_cap_bytes = out_cap * sizeof(float);
    if (client_execute_with_redirect(c, VEMB_V16_OP_VEMB_INLINE,
                                     combined, key_len,
                                     NULL, c->dim,
                                     &resp,
                                     (uint8_t *)out_vector, inline_cap_bytes,
                                     &inline_bytes) != 0) {
        return -1;
    }

    if (resp.status == VEMB_V16_STATUS_NOT_FOUND)
        return 1;
    if (resp.status != VEMB_V16_STATUS_OK)
        return -1;

    uint32_t expected_bytes = resp.vector_bytes;
    if (inline_bytes != expected_bytes ||
        (expected_bytes / sizeof(float)) > out_cap) {
        return -1;
    }
    if (out_dim)
        *out_dim = resp.dim > 0 ? resp.dim : c->dim;
    return 0;
}

int vemb_v16_client_vsim(vemb_v16_client_t *c,
                         const char *set_name,
                         const char *elem_name,
                         const float *query_vector,
                         uint32_t dim,
                         float *out_score)
{
    if (!c || !elem_name ||
        !query_vector || dim != c->dim || !out_score)
        return -1;

    char combined[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                    set_name, elem_name, &key_len) != 0)
        return -1;

    vemb_v16_resp_t resp;
    if (client_execute_with_redirect(c, VEMB_V16_OP_VSIM_INLINE,
                                     combined, key_len,
                                     query_vector, dim,
                                     &resp, NULL, 0, NULL) != 0)
        return -1;

    if (resp.status == VEMB_V16_STATUS_NOT_FOUND)
        return 1;
    if (resp.status != VEMB_V16_STATUS_OK)
        return -1;

    *out_score = resp.score;
    return 0;
}

/* ------------------------------------------------------------------ */
/* Pipeline API                                                       */
/* ------------------------------------------------------------------ */

int vemb_v16_client_vadd_pipeline(vemb_v16_client_t *c,
                                  const char **set_names,
                                  const char **elem_names,
                                  const float **vectors,
                                  uint32_t count,
                                  uint32_t max_inflight)
{
    if (!c || count == 0 || !set_names || !elem_names || !vectors)
        return -1;

    /* Build combined keys + flat arrays for the engine. */
    char combined_keys[count][VEMB_V16_MAX_KEY_LEN];
    uint32_t key_lens[count];
    const char *key_ptrs[count];
    const float *payload_ptrs[count];

    for (uint32_t i = 0; i < count; i++) {
        if (vemb_v16_build_combined_key(combined_keys[i], VEMB_V16_MAX_KEY_LEN,
                                        set_names[i], elem_names[i],
                                        &key_lens[i]) != 0)
            return -1;
        key_ptrs[i] = combined_keys[i];
        payload_ptrs[i] = vectors[i];
    }

    uint8_t status[count];
    if (client_pipeline_execute_with_redirect(
            c, VEMB_V16_OP_VADD,
            key_ptrs, key_lens,
            payload_ptrs, count,
            status, NULL, max_inflight, NULL) != 0)
        return -1;

    /* Check for errors. */
    for (uint32_t i = 0; i < count; i++) {
        if (status[i] == PIPE_ENTRY_ERR)
            return -1;
    }
    return 0;
}

int vemb_v16_client_vemb_pipeline(vemb_v16_client_t *c,
                                  const char **set_names,
                                  const char **elem_names,
                                  uint32_t count,
                                  float *out_vectors,
                                  vemb_v16_pipeline_resp_t *out_resps,
                                  uint32_t max_inflight)
{
    if (!c || count == 0 || !set_names || !elem_names || !out_resps)
        return -1;

    /* Build combined keys + flat arrays for the engine. */
    char combined_keys[count][VEMB_V16_MAX_KEY_LEN];
    uint32_t key_lens[count];
    const char *key_ptrs[count];

    for (uint32_t i = 0; i < count; i++) {
        if (vemb_v16_build_combined_key(combined_keys[i], VEMB_V16_MAX_KEY_LEN,
                                        set_names[i], elem_names[i],
                                        &key_lens[i]) != 0)
            return -1;
        key_ptrs[i] = combined_keys[i];
    }

    /* VEMB_INLINE: server returns the vector inline in the response.
     * VEMB_HANDLE is rejected on TCP/sniff transport (see
     * tcp_vemb_read_requires_inline_op in proxy.c). */
    uint32_t vec_bytes = c->dim * sizeof(float);
    /* Heap-allocate inline capture buffers — VLAs would blow the stack
     * for large count × dim. */
    uint8_t *inline_blob = malloc((size_t)count * vec_bytes);
    uint8_t **inline_ptrs = malloc((size_t)count * sizeof(uint8_t *));
    if (!inline_blob || !inline_ptrs) {
        free(inline_blob); free(inline_ptrs);
        return -1;
    }
    for (uint32_t i = 0; i < count; i++)
        inline_ptrs[i] = inline_blob + (size_t)i * vec_bytes;

    uint8_t status[count];
    vemb_v16_pipe_entry_aux_t aux[count];
    int engine_rc = client_pipeline_execute_with_redirect(
            c, VEMB_V16_OP_VEMB_INLINE,
            key_ptrs, key_lens,
            NULL, count,
            status, aux, max_inflight, inline_ptrs);

    if (engine_rc != 0) {
        free(inline_blob); free(inline_ptrs);
        return -1;
    }

    /* Translate engine status → out_resps + copy inline vectors. */
    for (uint32_t i = 0; i < count; i++) {
        vemb_v16_pipeline_resp_t *out = &out_resps[i];
        memset(out, 0, sizeof(*out));
        switch (status[i]) {
        case PIPE_ENTRY_OK:
            out->status    = 0;
            out->dim       = aux[i].dim > 0 ? aux[i].dim : c->dim;
            out->bytes     = vec_bytes;
            if (out_vectors)
                memcpy(out_vectors + i * c->dim,
                       inline_ptrs[i], vec_bytes);
            break;
        case PIPE_ENTRY_NOT_FOUND:
            out->status = 1;
            break;
        default:
            out->status = -1;
            break;
        }
    }
    free(inline_blob); free(inline_ptrs);
    return 0;
}

/* ------------------------------------------------------------------ */
/* Diagnostics / Control                                              */
/* ------------------------------------------------------------------ */

int vemb_v16_client_ping(vemb_v16_client_t *c)
{
    if (!c) return -1;
    if (sdk_route(c, NULL) != 0 || c->fd < 0) return -1;

    vemb_v16_req_t req = {0};
    req.op = VEMB_V16_OP_PING;
    req.flags = 0;
    req.req_id = c->req_id++;
    req.channel_id = c->channel_id;

    size_t payload_len = 0;
    char buf[2048];
    if (vemb_v16_req_encode((uint8_t *)buf + sizeof(vemb_v16_net_hdr_t),
                            sizeof(buf) - sizeof(vemb_v16_net_hdr_t),
                            &req, &payload_len) != 0) {
        return -1;
    }
    vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)buf;
    memset(hdr, 0, sizeof(*hdr));
    hdr->magic = VEMB_V16_MAGIC;
    hdr->version = VEMB_V16_VERSION;
    hdr->type = VEMB_V16_NET_REQUEST;
    hdr->payload_len = (uint32_t)payload_len;
    hdr->channel_id = c->channel_id;
    hdr->req_id = req.req_id;

    size_t total = sizeof(*hdr) + payload_len;
    if (vemb_v16_net_write_full(c->fd, buf, total) != 0)
        return -1;

    vemb_v16_resp_t resp;
    if (read_response(c, &resp, NULL, 0, NULL) != 0)
        return -1;

    return (resp.status == VEMB_V16_STATUS_OK) ? 0 : -1;
}

int vemb_v16_client_stats(vemb_v16_client_t *c, vemb_v16_stats_t *out_stats)
{
    if (!c || !out_stats) return -1;
    /* STATS opens a fresh control connection. In multi-endpoint mode it
     * queries the first backend (NULL key routes to backend[0]). */
    if (sdk_route(c, NULL) != 0) return -1;

    int fd = vemb_v16_net_connect(c->host, c->port,
                                  c->connect_timeout_ms ? c->connect_timeout_ms : 10000);
    if (fd < 0) return -1;

    vemb_v16_net_hdr_t hdr = {0};
    hdr.magic = VEMB_V16_MAGIC;
    hdr.version = VEMB_V16_VERSION;
    hdr.type = VEMB_V16_NET_STATS;
    hdr.payload_len = 0;

    int rc = -1;
    if (vemb_v16_net_write_full(fd, &hdr, sizeof(hdr)) != 0)
        goto out;

    vemb_v16_net_hdr_t resp_hdr;
    if (vemb_v16_net_read_header(fd, &resp_hdr) != 0 ||
        resp_hdr.type != VEMB_V16_NET_STATS ||
        resp_hdr.payload_len != sizeof(vemb_v16_stats_t)) {
        goto out;
    }

    if (vemb_v16_net_read_full(fd, out_stats, sizeof(*out_stats)) != 0)
        goto out;

    rc = 0;
out:
    close(fd);
    return rc;
}

/* ------------------------------------------------------------------ */
/* Advanced topology / observability APIs                              */
/* ------------------------------------------------------------------ */

int vemb_v16_client_topology_refresh(vemb_v16_client_t *client)
{
    if (!client) return -1;
    /* Single-endpoint mode doesn't use topology — no-op success. */
    if (client->seed_conn_count == 1) return 0;
    return fetch_topology_via_seed_conn(client, 0);
}

void vemb_v16_client_set_retry_budget(vemb_v16_client_t *client,
                                       uint32_t max_attempts)
{
    if (client) client->retry_budget = max_attempts;
}

void vemb_v16_client_get_redirect_stats(const vemb_v16_client_t *client,
                                         vemb_v16_redirect_stats_t *out)
{
    if (!client || !out) return;
    out->ask_redirects            = client->ask_redirects;
    out->moved_redirects          = client->moved_redirects;
    out->stale_topology_responses = client->stale_topology_responses;
    out->topology_refresh_calls   = client->topology_refresh_calls;
}

/* ------------------------------------------------------------------ */
/* Internal accessors                                                 */
/* ------------------------------------------------------------------ */

int vemb_v16_client_fd(const vemb_v16_client_t *c)
{
    return c ? c->fd : -1;
}

uint64_t vemb_v16_client_channel_id(const vemb_v16_client_t *c)
{
    return c ? c->channel_id : 0;
}

int vemb_v16_client_read_vector(vemb_v16_client_t *c,
                                uint64_t offset,
                                uint32_t bytes,
                                float *out_vector,
                                uint32_t out_cap)
{
    if (!c || !c->mapped_addr || !out_vector || bytes == 0)
        return -1;

    uint32_t nfloats = bytes / sizeof(float);
    if (nfloats > out_cap)
        return -1;
    if (offset + bytes > c->warm_region_bytes)
        return -1;

    memcpy(out_vector, c->mapped_addr + offset, bytes);
    return 0;
}

/* ------------------------------------------------------------------ */
/* VSIM_INLINE Pipeline                                               */
/* ------------------------------------------------------------------ */

int vemb_v16_client_vsim_pipeline(vemb_v16_client_t *c,
                                  const char **set_names,
                                  const char **elem_names,
                                  const float *query_vector,
                                  uint32_t count,
                                  float *out_scores,
                                  uint32_t max_inflight)
{
    if (!c || count == 0 || !set_names || !elem_names ||
        !query_vector || !out_scores)
        return -1;

    /* Build combined keys + flat arrays for the engine. VSIM uses the same
     * query_vector for all entries, so payloads[i] = query_vector for all i. */
    char combined_keys[count][VEMB_V16_MAX_KEY_LEN];
    uint32_t key_lens[count];
    const char *key_ptrs[count];
    const float *payload_ptrs[count];

    for (uint32_t i = 0; i < count; i++) {
        if (vemb_v16_build_combined_key(combined_keys[i], VEMB_V16_MAX_KEY_LEN,
                                        set_names[i], elem_names[i],
                                        &key_lens[i]) != 0)
            return -1;
        key_ptrs[i] = combined_keys[i];
        payload_ptrs[i] = query_vector;
    }

    uint8_t status[count];
    vemb_v16_pipe_entry_aux_t aux[count];
    if (client_pipeline_execute_with_redirect(
            c, VEMB_V16_OP_VSIM_INLINE,
            key_ptrs, key_lens,
            payload_ptrs, count,
            status, aux, max_inflight, NULL) != 0)
        return -1;

    /* Translate engine status → out_scores. */
    for (uint32_t i = 0; i < count; i++) {
        switch (status[i]) {
        case PIPE_ENTRY_OK:
            out_scores[i] = aux[i].score;
            break;
        case PIPE_ENTRY_NOT_FOUND:
            out_scores[i] = 0.0f;  /* sentinel for not-found */
            break;
        default:
            return -1;
        }
    }
    return 0;
}

/* ===================================================================== */
/* Convenience helpers                                                   */
/* ===================================================================== */

float *vemb_v16_parse_vector_csv(const char *str, uint32_t expected_dim)
{
    float *vec = malloc(expected_dim * sizeof(float));
    if (!vec) return NULL;

    char *copy = strdup(str);
    if (!copy) { free(vec); return NULL; }

    char *p = copy;
    uint32_t parsed = 0;
    while (parsed < expected_dim) {
        char *end = NULL;
        float v = strtof(p, &end);
        if (end == p) break;
        vec[parsed++] = v;
        if (*end == '\0') break;
        p = end + 1;
    }
    free(copy);

    if (parsed != expected_dim) {
        free(vec);
        return NULL;
    }
    return vec;
}

float *vemb_v16_parse_vector_argv(char **argv, int argc, int start_idx,
                                   uint32_t expected_dim, int *out_consumed)
{
    if (start_idx >= argc) return NULL;

    /* Case 1: single token with comma-separated values */
    if (strchr(argv[start_idx], ',')) {
        float *vec = vemb_v16_parse_vector_csv(argv[start_idx], expected_dim);
        if (vec && out_consumed) *out_consumed = 1;
        return vec;
    }

    /* Case 2: individual float tokens */
    if (start_idx + (int)expected_dim > argc) return NULL;
    float *vec = malloc(expected_dim * sizeof(float));
    if (!vec) return NULL;
    for (uint32_t i = 0; i < expected_dim; i++) {
        char *end = NULL;
        float v = strtof(argv[start_idx + i], &end);
        if (end == argv[start_idx + i] || *end != '\0') {
            free(vec);
            return NULL;
        }
        vec[i] = v;
    }
    if (out_consumed) *out_consumed = (int)expected_dim;
    return vec;
}

int vemb_v16_client_vsim_repeat(vemb_v16_client_t *c,
                                 const char *set_name, const char *elem_name,
                                 const float *query_vector, uint32_t repeat,
                                 float *out_score, int *out_found,
                                 uint32_t max_inflight)
{
    if (!c || !elem_name || !query_vector || repeat == 0)
        return -1;

#define REPEAT_BATCH 4096
    const char *batch_sets[REPEAT_BATCH];
    const char *batch_elems[REPEAT_BATCH];
    float batch_scores[REPEAT_BATCH];
    for (int i = 0; i < REPEAT_BATCH; i++) {
        batch_sets[i] = set_name;
        batch_elems[i] = elem_name;
    }

    float last_score = 0.0f;
    int last_found = 0;

    for (uint32_t offset = 0; offset < repeat; offset += REPEAT_BATCH) {
        uint32_t n = (repeat - offset < REPEAT_BATCH)
                     ? (repeat - offset) : REPEAT_BATCH;
        if (vemb_v16_client_vsim_pipeline(c, batch_sets, batch_elems,
                                          query_vector, n, batch_scores,
                                          max_inflight) != 0) {
            return -1;
        }
        last_score = batch_scores[n - 1];
        last_found = (last_score != 0.0f);
    }

    if (out_score) *out_score = last_score;
    if (out_found) *out_found = last_found;
    return 0;
#undef REPEAT_BATCH
}

int vemb_v16_client_vemb_repeat(vemb_v16_client_t *c,
                                 const char *set_name, const char *elem_name,
                                 uint32_t repeat, uint32_t max_inflight)
{
    if (!c || !elem_name || repeat == 0)
        return -1;

#define REPEAT_BATCH 4096
    const char *batch_sets[REPEAT_BATCH];
    const char *batch_elems[REPEAT_BATCH];
    vemb_v16_pipeline_resp_t batch_resps[REPEAT_BATCH];
    for (int i = 0; i < REPEAT_BATCH; i++) {
        batch_sets[i] = set_name;
        batch_elems[i] = elem_name;
    }

    for (uint32_t offset = 0; offset < repeat; offset += REPEAT_BATCH) {
        uint32_t n = (repeat - offset < REPEAT_BATCH)
                     ? (repeat - offset) : REPEAT_BATCH;
        memset(batch_resps, 0, sizeof(batch_resps[0]) * n);
        if (vemb_v16_client_vemb_pipeline(c, batch_sets, batch_elems,
                                          n, NULL, batch_resps, max_inflight) != 0) {
            return -1;
        }
    }
    return 0;
#undef REPEAT_BATCH
}

int vemb_v16_client_vadd_repeat(vemb_v16_client_t *c,
                                 const char *set_name, const char *elem_name,
                                 const float *vector, uint32_t repeat,
                                 uint32_t max_inflight)
{
    if (!c || !elem_name || !vector || repeat == 0)
        return -1;

#define REPEAT_BATCH 4096
    const char *batch_sets[REPEAT_BATCH];
    const char *batch_elems[REPEAT_BATCH];
    const float *batch_vectors[REPEAT_BATCH];
    for (int i = 0; i < REPEAT_BATCH; i++) {
        batch_sets[i] = set_name;
        batch_elems[i] = elem_name;
        batch_vectors[i] = vector;
    }

    for (uint32_t offset = 0; offset < repeat; offset += REPEAT_BATCH) {
        uint32_t n = (repeat - offset < REPEAT_BATCH)
                     ? (repeat - offset) : REPEAT_BATCH;
        if (vemb_v16_client_vadd_pipeline(c, batch_sets, batch_elems,
                                          batch_vectors, n, max_inflight) != 0) {
            return -1;
        }
    }
    return 0;
#undef REPEAT_BATCH
}

/* ===================================================================== *
 *  Aeron transport (TCP control + UB-backed SPSC ring)
 * ===================================================================== */

#define VEMB_V16_AERON_UDS_TIMEOUT_MS 5000u

struct vemb_v16_aeron_channel {
    vemb_v16_channel_desc_t    desc;
    vemb_v16_client_ring_t    *req_ring;
    vemb_v16_client_ring_t    *resp_ring;
    char                       control_endpoint[256];
    /* warm regions (lazy; opened by vemb_v16_aeron_open_warm_region) */
    struct {
        void          *mapping_addr;
        size_t         mapping_bytes;
        const uint8_t *mapped_addr;   /* data-area pointer */
        uint64_t       region_bytes;
        uint32_t       region_id;
        int            valid;
    } warm[VEMB_V16_MAX_DESC_WARM_REGIONS];
    uint32_t                   warm_count;
    int                        remote;
};

/* Connect to a UDS endpoint with a fixed receive/send timeout. Returns
 * fd on success, -1 on error. Mirrors benchmark/vemb_v16_bench.c:190
 * connect_uds, but reuses vemb_v16_net_set_timeouts for the timeouts. */
static int vemb_v16_aeron_uds_connect(const char *uds_path) {
    if (!uds_path || !uds_path[0]) return -1;
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    vemb_v16_net_set_timeouts(fd, VEMB_V16_AERON_UDS_TIMEOUT_MS);

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, uds_path, sizeof(addr.sun_path) - 1);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int vemb_v16_aeron_parse_tcp_endpoint(const char *endpoint,
                                             char *host,
                                             size_t host_cap,
                                             uint16_t *port) {
    if (!endpoint || !host || host_cap == 0 || !port)
        return -1;
    const char *p = endpoint;
    if (!strncmp(p, "tcp://", 6))
        p += 6;
    else if (p[0] == '/')
        return -1;
    const char *colon = strrchr(p, ':');
    if (!colon || colon == p || !colon[1])
        return -1;
    size_t host_len = (size_t)(colon - p);
    if (host_len >= host_cap)
        return -1;
    char *endptr = NULL;
    unsigned long parsed = strtoul(colon + 1, &endptr, 10);
    if (!endptr || *endptr != '\0' || parsed == 0 || parsed > 65535)
        return -1;
    memcpy(host, p, host_len);
    host[host_len] = '\0';
    *port = (uint16_t)parsed;
    return 0;
}

static int vemb_v16_aeron_control_connect(const char *endpoint, int *is_tcp) {
    char host[128];
    uint16_t port = 0;
    if (vemb_v16_aeron_parse_tcp_endpoint(endpoint, host, sizeof(host), &port) == 0) {
        if (is_tcp) *is_tcp = 1;
        return vemb_v16_net_connect(host, port, VEMB_V16_AERON_UDS_TIMEOUT_MS);
    }
    if (is_tcp) *is_tcp = 0;
    return vemb_v16_aeron_uds_connect(endpoint);
}

/* TCP connect with timeout — mirrors UDS connect semantics. */
static int vemb_v16_aeron_tcp_connect(const char *host, uint16_t port) {
    if (!host || !host[0]) return -1;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    vemb_v16_net_set_timeouts(fd, VEMB_V16_AERON_UDS_TIMEOUT_MS);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port  = htons(port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        close(fd); return -1;
    }
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd); return -1;
    }
    return fd;
}

/* UDS control op: alloc a channel for the requested dim. Fills desc. */
static int vemb_v16_aeron_uds_alloc(int fd, uint32_t dim,
                                    vemb_v16_channel_desc_t *desc) {
    uint8_t op = VEMB_V16_CTRL_ALLOC_CHANNEL;
    vemb_v16_alloc_req_t req;
    memset(&req, 0, sizeof(req));
    req.vector_dim = dim;
    uint8_t status = VEMB_V16_STATUS_ERR;
    memset(desc, 0, sizeof(*desc));
    if (vemb_v16_net_write_full(fd, &op, sizeof(op)) != 0)            return -1;
    if (vemb_v16_net_write_full(fd, &req,  sizeof(req))  != 0)        return -1;
    if (vemb_v16_net_read_full (fd, &status, sizeof(status)) != 0)    return -1;
    if (status != VEMB_V16_STATUS_OK)                                 return -1;
    if (vemb_v16_net_read_full (fd, desc,   sizeof(*desc))   != 0)    return -1;
    return 0;
}

static int vemb_v16_aeron_tcp_alloc(int fd, uint32_t dim,
                                    vemb_v16_channel_desc_t *desc) {
    vemb_v16_aeron_attach_req_t req;
    memset(&req, 0, sizeof(req));
    memcpy(req.magic,
           VEMB_V16_AERON_ATTACH_MAGIC,
           VEMB_V16_AERON_ATTACH_MAGIC_LEN);
    req.dim = dim;
    vemb_v16_aeron_attach_resp_t resp;
    memset(&resp, 0, sizeof(resp));
    if (vemb_v16_net_write_full(fd, &req, sizeof(req)) != 0 ||
        vemb_v16_net_read_full(fd, &resp, sizeof(resp)) != 0 ||
        memcmp(resp.magic,
               VEMB_V16_AERON_ATTACHED_MAGIC,
               VEMB_V16_AERON_ATTACHED_MAGIC_LEN) != 0 ||
        resp.status != 0 ||
        resp.request_shmdev_path_len == 0 ||
        resp.request_shmdev_path_len > VEMB_V16_AERON_SHMDEV_PATH_MAX ||
        resp.response_shmdev_path_len == 0 ||
        resp.response_shmdev_path_len > VEMB_V16_AERON_SHMDEV_PATH_MAX ||
        resp.req_slot_size == 0 ||
        resp.resp_slot_size == 0 ||
        resp.ring_size_slots != VEMB_V16_CLIENT_RING_SIZE ||
        resp.request_shmdev_path[0] == '\0' ||
        resp.response_shmdev_path[0] == '\0') {
        return -1;
    }

    fprintf(stderr,
            "[sdk] aeron attach resp: req_path=%s resp_path=%s req_off=%llu resp_off=%llu "
            "warm_count=%u warm_id=%u warm_backend=%u warm_bytes=%llu "
            "warm_offset=%llu warm_path=%s\n",
            resp.request_shmdev_path,
            resp.response_shmdev_path,
            (unsigned long long)resp.req_ring_off,
            (unsigned long long)resp.resp_ring_off,
            resp.warm_region_count,
            resp.warm_region_id,
            resp.warm_backend_type,
            (unsigned long long)resp.warm_region_bytes,
            (unsigned long long)resp.warm_mmap_offset,
            resp.warm_path);

    memset(desc, 0, sizeof(*desc));
    desc->magic = VEMB_V16_MAGIC;
    desc->version = VEMB_V16_VERSION;
    desc->channel_id = resp.channel_id;
    desc->vector_dim = dim;
    desc->vector_stride = dim * sizeof(float);
    desc->request_ring_slot_size = resp.req_slot_size;
    desc->response_ring_slot_size = resp.resp_slot_size;
    snprintf(desc->request_ring_name,
             sizeof(desc->request_ring_name),
             "%s@off%llu",
             resp.request_shmdev_path,
             (unsigned long long)resp.req_ring_off);
    snprintf(desc->response_ring_name,
             sizeof(desc->response_ring_name),
             "%s@off%llu",
             resp.response_shmdev_path,
             (unsigned long long)resp.resp_ring_off);
    if (resp.warm_region_count > 1 ||
        (resp.warm_region_count != 0 &&
         (resp.warm_path_len == 0 ||
          resp.warm_path_len > VEMB_V16_AERON_SHMDEV_PATH_MAX ||
          resp.warm_path[0] == '\0'))) {
        return -1;
    }
    if (resp.warm_region_count == 1) {
        desc->warm_region_count = 1;
        desc->warm_regions[0].region_id = resp.warm_region_id;
        desc->warm_regions[0].backend_type = resp.warm_backend_type;
        desc->warm_regions[0].region_bytes = resp.warm_region_bytes;
        desc->warm_regions[0].mmap_offset = resp.warm_mmap_offset;
        strncpy(desc->warm_regions[0].path,
                resp.warm_path,
                sizeof(desc->warm_regions[0].path) - 1);
    }
    return 0;
}

/* UDS control op: close a specific channel by id. */
static int vemb_v16_aeron_uds_close(int fd, uint64_t channel_id) {
    uint8_t op = VEMB_V16_CTRL_CLOSE_CHANNEL;
    uint8_t status = VEMB_V16_STATUS_ERR;
    if (vemb_v16_net_write_full(fd, &op,         sizeof(op))         != 0) return -1;
    if (vemb_v16_net_write_full(fd, &channel_id, sizeof(channel_id)) != 0) return -1;
    if (vemb_v16_net_read_full (fd, &status,     sizeof(status))     != 0) return -1;
    return status == VEMB_V16_STATUS_OK ? 0 : -1;
}

/* UDS control op: close all channels. *closed receives server-reported
 * count. */
static int vemb_v16_aeron_uds_close_all(int fd, uint64_t *closed) {
    uint8_t op = VEMB_V16_CTRL_CLOSE_ALL_CHANNELS;
    uint8_t status = VEMB_V16_STATUS_ERR;
    uint64_t n = 0;
    if (vemb_v16_net_write_full(fd, &op, sizeof(op))         != 0) return -1;
    if (vemb_v16_net_read_full (fd, &status, sizeof(status)) != 0) return -1;
    if (status != VEMB_V16_STATUS_OK)                         return -1;
    if (vemb_v16_net_read_full (fd, &n, sizeof(n))           != 0) return -1;
    if (closed) *closed = n;
    return 0;
}

static int vemb_v16_aeron_tcp_status_control(int fd,
                                             uint16_t type,
                                             uint64_t channel_id,
                                             uint64_t *value) {
    if (vemb_v16_net_write_frame(fd,
                                 type,
                                 0,
                                 channel_id,
                                 0,
                                 NULL,
                                 0) != 0)
        return -1;
    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_CONTROL_STATUS ||
        hdr.payload_len != vemb_v16_net_status_encoded_len())
        return -1;
    uint8_t payload[VEMB_V16_NET_STATUS_ENCODED_LEN];
    vemb_v16_net_status_t st;
    if (vemb_v16_net_read_full(fd, payload, sizeof(payload)) != 0 ||
        vemb_v16_net_status_decode(&st, payload, sizeof(payload)) != 0 ||
        st.status != VEMB_V16_STATUS_OK)
        return -1;
    if (value) *value = st.value;
    return 0;
}

/* Open a UB-backed ring encoded as <device>@off<bytes> + slot_size. */
static int vemb_v16_aeron_ring_open(const char *name,
                                    uint32_t slot_size,
                                    vemb_v16_client_ring_t **out) {
    if (!name || !name[0] || slot_size == 0 || !out) return -1;
    const char *offset_marker = strstr(name, "@off");
    if (!offset_marker) return -1;
    uint64_t mmap_offset = 0;
    char path[256];
    size_t path_len = (size_t)(offset_marker - name);
    if (path_len == 0 || path_len >= sizeof(path)) return -1;
    memcpy(path, name, path_len);
    path[path_len] = '\0';
    char *end = NULL;
    mmap_offset = strtoull(offset_marker + 4, &end, 10);
    if (end == offset_marker + 4 || *end != '\0') return -1;
    /* Same-host Aeron uses the UB device directly with O_RDWR. */
    int fd = open(path, O_RDWR);
    if (fd < 0) return -1;
    size_t bytes = vemb_v16_client_ring_bytes(slot_size);
    long page_size = sysconf(_SC_PAGESIZE);
    uint64_t page_mask = (uint64_t)(page_size > 0 ? page_size : 4096) - 1u;
    uint64_t aligned_offset = mmap_offset & ~page_mask;
    size_t offset_delta = (size_t)(mmap_offset - aligned_offset);
    void *base = mmap(NULL,
                      bytes + offset_delta,
                      PROT_READ | PROT_WRITE,
                      MAP_SHARED,
                      fd,
                      (off_t)aligned_offset);
    close(fd);
    if (base == MAP_FAILED) return -1;
    *out = (vemb_v16_client_ring_t *)((uint8_t *)base + offset_delta);
    return 0;
}

static void vemb_v16_aeron_ring_close(vemb_v16_client_ring_t *r,
                                      uint32_t slot_size) {
    if (!r || slot_size == 0) return;
    munmap(r, vemb_v16_client_ring_bytes(slot_size));
}

/* Map a shared region with read/write access. POSIX SHM names are opened
 * through shm_open; UB devices are regular device paths. Both mappings use
 * O_RDWR so the server and client have identical cacheability attributes. */
static void *vemb_v16_mmap_shmdev_region(const char *path,
                                          uint32_t backend_type,
                                          int use_sync,
                                          uint64_t offset, size_t bytes,
                                          size_t *out_map_bytes,
                                          size_t *out_offset_delta) {
    if (!path || !path[0] || bytes == 0) return NULL;
    if (backend_type != VEMB_V16_REGION_LOCAL_SHM &&
        backend_type != VEMB_V16_REGION_UB) return NULL;
    long page_size = sysconf(_SC_PAGESIZE);
    uint64_t page_mask = (uint64_t)(page_size > 0 ? page_size : 4096) - 1u;
    uint64_t aligned_offset = offset & ~page_mask;
    size_t offset_delta = (size_t)(offset - aligned_offset);
    size_t map_size = bytes + offset_delta;

    int flags = O_RDWR;
    if (backend_type == VEMB_V16_REGION_UB && use_sync)
        flags |= O_SYNC;
    int fd = backend_type == VEMB_V16_REGION_LOCAL_SHM ?
        shm_open(path, flags, 0666) : open(path, flags);
    if (fd < 0) return NULL;
    void *ptr = mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                     fd, (off_t)aligned_offset);
    close(fd);
    if (ptr == MAP_FAILED) ptr = NULL;
    if (!ptr) return NULL;
    if (out_map_bytes)    *out_map_bytes    = map_size;
    if (out_offset_delta) *out_offset_delta = offset_delta;
    return ptr;
}

/* Open a UB-backed ring at a specific byte offset. */
/* Map the server's physical UB view to the corresponding client view.
 * The current two-node layout exposes server devices 1-4 as client devices
 * 5-8. Paths outside that layout are preserved so same-host and non-OBMM
 * test paths continue to work. */
static int vemb_v16_aeron_map_remote_ub_path(const char *server_path,
                                             char *client_path,
                                             size_t client_path_cap) {
    if (!server_path || !server_path[0] ||
        !client_path || client_path_cap == 0)
        return -1;

    size_t source_len = strnlen(server_path, client_path_cap);
    if (source_len >= client_path_cap)
        return -1;

    const char *marker = strstr(server_path, "obmm_shmdev");
    if (!marker) {
        memcpy(client_path, server_path, source_len + 1);
        return 0;
    }

    const char *digits = marker + strlen("obmm_shmdev");
    char *end = NULL;
    unsigned long device_id = strtoul(digits, &end, 10);
    if (end == digits || *end != '\0' || device_id < 1u || device_id > 4u) {
        memcpy(client_path, server_path, source_len + 1);
        return 0;
    }

    size_t prefix_len = (size_t)(digits - server_path);
    int written = snprintf(client_path, client_path_cap, "%.*s%lu",
                           (int)prefix_len, server_path, device_id + 4u);
    return written >= 0 && (size_t)written < client_path_cap ? 0 : -1;
}

static int vemb_v16_aeron_ring_open_shmdev(const char *path,
                                           uint64_t ring_off,
                                           uint32_t slot_size,
                                           int use_sync,
                                           vemb_v16_client_ring_t **out) {
    if (!path || !path[0] || slot_size == 0 || !out) return -1;
    size_t bytes = vemb_v16_client_ring_bytes(slot_size);
    void *ptr = vemb_v16_mmap_shmdev_region(path, VEMB_V16_REGION_UB,
                                             use_sync,
                                             ring_off, bytes, NULL, NULL);
    if (!ptr) return -1;
    *out = (vemb_v16_client_ring_t *)ptr;
    return 0;
}

/* Best-effort server-side close notification. Errors are swallowed
 * because the rings are already unmapped locally by the caller. */
static void vemb_v16_aeron_notify_close(const char *endpoint,
                                        uint64_t channel_id) {
    if (!endpoint || !endpoint[0]) return;
    int is_tcp = 0;
    int fd = vemb_v16_aeron_control_connect(endpoint, &is_tcp);
    if (fd < 0) return;
    if (is_tcp)
        vemb_v16_aeron_tcp_status_control(fd,
                                          VEMB_V16_NET_CLOSE_CHANNEL,
                                          channel_id,
                                          NULL);
    else
        vemb_v16_aeron_uds_close(fd, channel_id);
    close(fd);
}

vemb_v16_aeron_channel_t *vemb_v16_aeron_open(const char *uds_path,
                                              uint32_t dim) {
    if (!uds_path || !uds_path[0] || dim == 0) return NULL;

    vemb_v16_aeron_channel_t *ch = calloc(1, sizeof(*ch));
    if (!ch) return NULL;
    strncpy(ch->control_endpoint, uds_path, sizeof(ch->control_endpoint) - 1);

    int is_tcp = 0;
    int fd = vemb_v16_aeron_control_connect(uds_path, &is_tcp);
    if (fd < 0) { free(ch); return NULL; }
    int rc = is_tcp ?
        vemb_v16_aeron_tcp_alloc(fd, dim, &ch->desc) :
        vemb_v16_aeron_uds_alloc(fd, dim, &ch->desc);
    close(fd);
    if (rc != 0) {
        free(ch);
        return NULL;
    }
    if (vemb_v16_aeron_ring_open(ch->desc.request_ring_name,
                                 ch->desc.request_ring_slot_size,
                                 &ch->req_ring) != 0) {
        vemb_v16_aeron_notify_close(uds_path, ch->desc.channel_id);
        free(ch);
        return NULL;
    }
    if (vemb_v16_aeron_ring_open(ch->desc.response_ring_name,
                                 ch->desc.response_ring_slot_size,
                                 &ch->resp_ring) != 0) {
        vemb_v16_aeron_ring_close(ch->req_ring, ch->desc.request_ring_slot_size);
        vemb_v16_aeron_notify_close(uds_path, ch->desc.channel_id);
        free(ch);
        return NULL;
    }
    return ch;
}

vemb_v16_aeron_channel_t *vemb_v16_aeron_open_remote(const char *host,
                                                     uint16_t port,
                                                     uint32_t dim) {
    if (!host || !host[0] || dim == 0 || port == 0) return NULL;

    int fd = vemb_v16_aeron_tcp_connect(host, port);
    if (fd < 0) return NULL;

    vemb_v16_aeron_attach_req_t req;
    memset(&req, 0, sizeof(req));
    memcpy(req.magic, VEMB_V16_AERON_ATTACH_MAGIC,
           VEMB_V16_AERON_ATTACH_MAGIC_LEN);
    req.dim = dim;
    req.req_slot_size  = 0;  /* 0 = server picks */
    req.resp_slot_size = 0;
    req.flags = VEMB_V16_AERON_ATTACH_F_REMOTE_PATH;

    vemb_v16_aeron_attach_resp_t resp;
    memset(&resp, 0, sizeof(resp));
    /* Inline ATTACH exchange (write_full/read_full from vemb_v16_net.h) —
     * avoids linking the server-only vemb_v16_aeron_attach.c into the SDK. */
    if (vemb_v16_net_write_full(fd, &req,  sizeof(req))  != 0 ||
        vemb_v16_net_read_full (fd, &resp, sizeof(resp)) != 0 ||
        memcmp(resp.magic, VEMB_V16_AERON_ATTACHED_MAGIC,
               VEMB_V16_AERON_ATTACHED_MAGIC_LEN) != 0 ||
        resp.status != 0) {
        close(fd);
        return NULL;
    }
    close(fd);  /* TCP is handshake-only; data goes over shmdev */

    if (resp.request_shmdev_path_len == 0 ||
        resp.request_shmdev_path_len > VEMB_V16_AERON_SHMDEV_PATH_MAX ||
        resp.response_shmdev_path_len == 0 ||
        resp.response_shmdev_path_len > VEMB_V16_AERON_SHMDEV_PATH_MAX ||
        resp.request_shmdev_path[0] == '\0' ||
        resp.response_shmdev_path[0] == '\0' ||
        resp.req_backend_type != VEMB_V16_REGION_UB ||
        resp.resp_backend_type != VEMB_V16_REGION_UB) {
        return NULL;
    }
    char client_request_shmdev_path[VEMB_V16_AERON_SHMDEV_PATH_MAX];
    char client_response_shmdev_path[VEMB_V16_AERON_SHMDEV_PATH_MAX];
    if (vemb_v16_aeron_map_remote_ub_path(
            resp.request_shmdev_path, client_request_shmdev_path,
            sizeof(client_request_shmdev_path)) != 0 ||
        vemb_v16_aeron_map_remote_ub_path(
            resp.response_shmdev_path, client_response_shmdev_path,
            sizeof(client_response_shmdev_path)) != 0) {
        return NULL;
    }

    vemb_v16_aeron_channel_t *ch = calloc(1, sizeof(*ch));
    if (!ch) return NULL;
    snprintf(ch->control_endpoint,
             sizeof(ch->control_endpoint),
             "tcp://%s:%u",
             host,
             (unsigned)port);
    ch->remote = 1;
    ch->desc.channel_id          = resp.channel_id;
    ch->desc.request_ring_slot_size  = resp.req_slot_size;
    ch->desc.response_ring_slot_size = resp.resp_slot_size;

    if (vemb_v16_aeron_ring_open_shmdev(client_request_shmdev_path,
                                        resp.req_ring_off,
                                        resp.req_slot_size,
                                        1,
                                        &ch->req_ring) != 0) {
        free(ch); return NULL;
    }
    if (vemb_v16_aeron_ring_open_shmdev(client_response_shmdev_path,
                                        resp.resp_ring_off,
                                        resp.resp_slot_size,
                                        1,
                                        &ch->resp_ring) != 0) {
        vemb_v16_aeron_ring_close(ch->req_ring, resp.req_slot_size);
        free(ch); return NULL;
    }

    /* Parse advertised warm region (if any) into channel desc so the
     * runner can mmap it for VEMB_HANDLE dereference. The path in the
     * response is server-side, so map it to the local UB view first. */
    char client_warm_path[VEMB_V16_AERON_SHMDEV_PATH_MAX];
    if (resp.warm_region_count > 0 && resp.warm_path[0] &&
        resp.warm_backend_type == VEMB_V16_REGION_UB &&
        vemb_v16_aeron_map_remote_ub_path(resp.warm_path,
                                          client_warm_path,
                                          sizeof(client_warm_path)) == 0) {
        ch->desc.warm_region_count = 1;
        ch->desc.warm_regions[0].region_id   = resp.warm_region_id;
        ch->desc.warm_regions[0].backend_type = resp.warm_backend_type;
        ch->desc.warm_regions[0].region_bytes = resp.warm_region_bytes;
        ch->desc.warm_regions[0].mmap_offset  = resp.warm_mmap_offset;
        strncpy(ch->desc.warm_regions[0].path, client_warm_path,
                sizeof(ch->desc.warm_regions[0].path) - 1);
        ch->desc.warm_regions[0].path[sizeof(ch->desc.warm_regions[0].path) - 1] = 0;
    }

    fprintf(stderr, "[sdk] cross-node ch ok: cid=%llu req_path=%s resp_path=%s req_off=%llu resp_off=%llu req_slot=%u resp_slot=%u\n",
            (unsigned long long)ch->desc.channel_id,
            resp.request_shmdev_path, resp.response_shmdev_path,
            (unsigned long long)resp.req_ring_off,
            (unsigned long long)resp.resp_ring_off,
            resp.req_slot_size, resp.resp_slot_size);
    fprintf(stderr, "[sdk] req_ring hdr: slot_size=%u slot_count=%u slot_mask=%u slots_off=%llu head=%llu tail=%llu\n",
            ch->req_ring->slot_size, ch->req_ring->slot_count,
            ch->req_ring->slot_mask, (unsigned long long)ch->req_ring->slots_off,
            (unsigned long long)ch->req_ring->head,
            (unsigned long long)ch->req_ring->tail);
    fprintf(stderr, "[sdk] resp_ring hdr: slot_size=%u slot_count=%u slot_mask=%u slots_off=%llu head=%llu tail=%llu\n",
            ch->resp_ring->slot_size, ch->resp_ring->slot_count,
            ch->resp_ring->slot_mask, (unsigned long long)ch->resp_ring->slots_off,
            (unsigned long long)ch->resp_ring->head,
            (unsigned long long)ch->resp_ring->tail);
    if (resp.warm_region_count > 0) {
        fprintf(stderr, "[sdk] warm region advertised: region_id=%u backend=%u bytes=%llu mmap_off=%llu server_path=%s local_path=%s\n",
                resp.warm_region_id, resp.warm_backend_type,
                (unsigned long long)resp.warm_region_bytes,
                (unsigned long long)resp.warm_mmap_offset, resp.warm_path,
                ch->desc.warm_region_count ? ch->desc.warm_regions[0].path : "(unmapped)");
    }
    return ch;
}

void vemb_v16_aeron_close(vemb_v16_aeron_channel_t *ch) {
    if (!ch) return;
    for (uint32_t i = 0; i < ch->warm_count; i++) {
        if (ch->warm[i].valid && ch->warm[i].mapping_addr) {
            munmap(ch->warm[i].mapping_addr, ch->warm[i].mapping_bytes);
            ch->warm[i].mapping_addr = NULL;
            ch->warm[i].valid = 0;
        }
    }
    ch->warm_count = 0;
    vemb_v16_aeron_ring_close(ch->req_ring,  ch->desc.request_ring_slot_size);
    vemb_v16_aeron_ring_close(ch->resp_ring, ch->desc.response_ring_slot_size);
    vemb_v16_aeron_notify_close(ch->control_endpoint, ch->desc.channel_id);
    free(ch);
}

int vemb_v16_aeron_close_all(const char *uds_path) {
    if (!uds_path || !uds_path[0]) return -1;
    int is_tcp = 0;
    int fd = vemb_v16_aeron_control_connect(uds_path, &is_tcp);
    if (fd < 0) return -1;
    uint64_t closed = 0;
    int rc = is_tcp ?
        vemb_v16_aeron_tcp_status_control(fd,
                                          VEMB_V16_NET_CLOSE_ALL_CHANNELS,
                                          0,
                                          &closed) :
        vemb_v16_aeron_uds_close_all(fd, &closed);
    close(fd);
    if (rc != 0) return -1;
    return (int)(closed > INT_MAX ? INT_MAX : closed);
}

uint64_t vemb_v16_aeron_channel_id(const vemb_v16_aeron_channel_t *ch) {
    if (!ch) return 0;
    return ch->desc.channel_id;
}

int vemb_v16_aeron_publish_request(vemb_v16_aeron_channel_t *ch,
                                   const void *buf, uint32_t len) {
    static uint32_t s_diag_printed = 0;
    if (!ch) return -3;
    int rc = vemb_v16_client_publish(ch->req_ring, buf, len);
    if (rc != 0 && !__sync_lock_test_and_set(&s_diag_printed, 1)) {
        fprintf(stderr, "[sdk] first publish FAIL rc=%d len=%u slot_size=%u slot_count=%u head=%llu tail=%llu\n",
                rc, len, ch->req_ring->slot_size, ch->req_ring->slot_count,
                (unsigned long long)ch->req_ring->head,
                (unsigned long long)ch->req_ring->tail);
    }
    return rc;
}

int vemb_v16_aeron_publish_request_batch(vemb_v16_aeron_channel_t *ch,
                                         const void *const *bufs,
                                         const uint32_t *lens,
                                         uint32_t count) {
    if (!ch) return -3;
    return vemb_v16_client_publish_ptr_batch(ch->req_ring, bufs, lens, count);
}

int vemb_v16_aeron_poll_response(vemb_v16_aeron_channel_t *ch,
                                 void *buf, uint32_t max_len) {
    if (!ch) return -3;
    if (!buf || max_len < sizeof(vemb_v16_resp_t)) return -2;
    uint8_t wire[VEMB_V16_AERON_RESP_WIRE_MAX_LEN];
    int got = vemb_v16_client_poll(ch->resp_ring, wire, sizeof(wire));
    if (got <= 0) return got;
    vemb_v16_resp_t decoded;
    if (vemb_v16_resp_decode(&decoded, wire, (size_t)got) != 0)
        return -1;
    memcpy(buf, &decoded, sizeof(decoded));
    return (int)sizeof(decoded);
}

uint32_t vemb_v16_aeron_poll_response_batch_ex(
    vemb_v16_aeron_channel_t *ch, void *slots, uint32_t *wire_lens,
    uint32_t max_len, uint32_t max_count) {
    if (!ch || !slots || max_len < sizeof(vemb_v16_resp_t) ||
        max_count == 0) return 0;
    uint32_t local_lens[VEMB_V16_CLIENT_RING_SIZE];
    uint32_t *lengths = wire_lens ? wire_lens : local_lens;
    if (max_count > VEMB_V16_CLIENT_RING_SIZE)
        max_count = VEMB_V16_CLIENT_RING_SIZE;
    uint32_t got = vemb_v16_client_poll_batch_lengths(
        ch->resp_ring, slots, lengths, max_len, max_count);
    for (uint32_t i = 0; i < got; i++) {
        uint8_t *dst = (uint8_t *)slots + (size_t)i * max_len;
        vemb_v16_resp_t decoded;
        if (vemb_v16_resp_decode(&decoded, dst, lengths[i]) == 0) {
            memcpy(dst, &decoded, sizeof(decoded));
        } else {
            memset(&decoded, 0, sizeof(decoded));
            decoded.status = VEMB_V16_STATUS_ERR;
            memcpy(dst, &decoded, sizeof(decoded));
        }
    }
    return got;
}

uint32_t vemb_v16_aeron_poll_response_batch(vemb_v16_aeron_channel_t *ch,
                                            void *slots,
                                            uint32_t max_len,
                                            uint32_t max_count) {
    return vemb_v16_aeron_poll_response_batch_ex(ch, slots, NULL,
                                                 max_len, max_count);
}

/* Helper: open and mmap a single warm region by path. */
static int aeron_mmap_one_region(const char *path,
                                  uint32_t backend_type,
                                  int use_sync,
                                  uint64_t region_bytes, uint64_t mmap_offset,
                                  void **out_mapping_addr, size_t *out_mapping_bytes,
                                  const uint8_t **out_mapped_addr) {
    if (!path || !path[0] || region_bytes == 0) return -1;
    size_t map_bytes = 0, offset_delta = 0;
    void *ptr = vemb_v16_mmap_shmdev_region(path, backend_type, use_sync,
                                            mmap_offset, (size_t)region_bytes,
                                            &map_bytes, &offset_delta);
    if (!ptr) return -1;
    *out_mapping_addr   = ptr;
    *out_mapping_bytes  = map_bytes;
    *out_mapped_addr    = (const uint8_t *)ptr + offset_delta;
    return 0;
}

int vemb_v16_aeron_open_warm_region(vemb_v16_aeron_channel_t *ch) {
    if (!ch) return -1;
    /* Idempotent: unmap previous mappings. */
    for (uint32_t i = 0; i < ch->warm_count; i++) {
        if (ch->warm[i].valid && ch->warm[i].mapping_addr)
            munmap(ch->warm[i].mapping_addr, ch->warm[i].mapping_bytes);
        memset(&ch->warm[i], 0, sizeof(ch->warm[i]));
    }
    ch->warm_count = 0;

    uint32_t count = ch->desc.warm_region_count;
    if (count > VEMB_V16_MAX_DESC_WARM_REGIONS)
        count = VEMB_V16_MAX_DESC_WARM_REGIONS;

    if (count == 0) {
        /* Backward-compatible fallback: single region via legacy fields. */
        void *ma = NULL; size_t mb = 0; void *md = NULL; uint64_t rb = 0;
        if (vemb_v16_open_warm_region_internal(&ch->desc, &ma, &mb, &md, &rb) != 0) {
            fprintf(stderr,
                    "[sdk] warm mmap failed: legacy path=%s backend=%u bytes=%llu offset=%llu errno=%d (%s)\n",
                    ch->desc.vector_region_name,
                    ch->desc.warm_backend_type,
                    (unsigned long long)ch->desc.warm_region_bytes,
                    (unsigned long long)ch->desc.warm_mmap_offset,
                    errno, strerror(errno));
            return -1;
        }
        ch->warm[0].mapping_addr   = ma;
        ch->warm[0].mapping_bytes  = mb;
        ch->warm[0].mapped_addr    = (const uint8_t *)md;
        ch->warm[0].region_bytes   = rb;
        ch->warm[0].region_id      = ch->desc.warm_region_id;
        ch->warm[0].valid          = 1;
        ch->warm_count = 1;
        return 0;
    }

    uint32_t ok = 0;
    for (uint32_t i = 0; i < count; i++) {
        if (aeron_mmap_one_region(ch->desc.warm_regions[i].path,
                                   ch->desc.warm_regions[i].backend_type,
                                   ch->remote,
                                   ch->desc.warm_regions[i].region_bytes,
                                   ch->desc.warm_regions[i].mmap_offset,
                                   &ch->warm[i].mapping_addr,
                                   &ch->warm[i].mapping_bytes,
                                   &ch->warm[i].mapped_addr) != 0) {
            fprintf(stderr,
                    "[sdk] warm mmap failed: index=%u path=%s backend=%u bytes=%llu offset=%llu errno=%d (%s)\n",
                    i, ch->desc.warm_regions[i].path,
                    ch->desc.warm_regions[i].backend_type,
                    (unsigned long long)ch->desc.warm_regions[i].region_bytes,
                    (unsigned long long)ch->desc.warm_regions[i].mmap_offset,
                    errno, strerror(errno));
            ch->warm[i].valid = 0;
            continue;  /* skip inaccessible region; reads to it return -1 */
        }
        ch->warm[i].region_id = ch->desc.warm_regions[i].region_id;
        ch->warm[i].region_bytes = ch->desc.warm_regions[i].region_bytes;
        ch->warm[i].valid = 1;
        ok++;
    }
    ch->warm_count = count;
    return ok > 0 ? 0 : -1;
}

int vemb_v16_aeron_read_vector(const vemb_v16_aeron_channel_t *ch,
                               uint32_t region_id,
                               uint64_t offset, uint32_t bytes,
                               void *out, uint32_t cap) {
    if (!ch || !out)                                   return -1;
    if (bytes == 0 || bytes > cap)                     return -1;
    /* Find the mapping matching region_id (linear scan, N <= 16). */
    const uint8_t *base = NULL;
    uint64_t region_bytes = 0;
    for (uint32_t i = 0; i < ch->warm_count; i++) {
        if (ch->warm[i].valid && ch->warm[i].region_id == region_id) {
            base = ch->warm[i].mapped_addr;
            region_bytes = ch->warm[i].region_bytes;
            break;
        }
    }
    if (!base) return -1;
    /* Bounds check against the server-reported region size. */
    if (offset > region_bytes ||
        (uint64_t)bytes > region_bytes - offset) return -1;
    sve_streaming_load_f32(base + offset, out, bytes);
    return (int)bytes;
}

/* =====================================================================
 *  SVE-aware streaming load — backs vemb_v16_aeron_read_vector and the
 *  protocol.cpp VEMB_INLINE copy path. Ported from src/sve_operation.c
 *  with all server-side deps stripped. Plain memcpy fallback when the
 *  SDK is built without -DUSE_ARM_SVE.
 * ===================================================================== */

void sve_streaming_load(const void *src, void *dst, size_t size) {
#ifdef USE_ARM_SVE
    const uint8_t *s = (const uint8_t *)src;
    uint8_t *d = (uint8_t *)dst;
    size_t off = 0;
    while (off < size) {
        svbool_t pg = svwhilelt_b8_u64((uint64_t)off, (uint64_t)size);
        svuint8_t v = svld1_u8(pg, &s[off]);
        svst1_u8(pg, &d[off], v);
        off += svcntb();
    }
#else
    if (size) memcpy(dst, src, size);
#endif
}

void sve_streaming_load_f32(const void *src, void *dst, size_t size) {
#ifdef USE_ARM_SVE
    if ((((uintptr_t)src | (uintptr_t)dst | size) & (sizeof(float) - 1u)) != 0) {
        sve_streaming_load(src, dst, size);
        return;
    }
    const size_t vl = svcntw();
    size_t rem = size / sizeof(float);
    const float *srcp = (const float *)src;
    float *dstp = (float *)dst;

    svbool_t pg = svptrue_b32();
    while (rem >= vl * 4u) {
        __builtin_prefetch(srcp + vl * 8u, 0, 3);
        svfloat32_t v0 = svld1_f32(pg, srcp);
        svfloat32_t v1 = svld1_f32(pg, srcp + vl);
        svfloat32_t v2 = svld1_f32(pg, srcp + vl * 2u);
        svfloat32_t v3 = svld1_f32(pg, srcp + vl * 3u);
        svst1_f32(pg, dstp, v0);
        svst1_f32(pg, dstp + vl, v1);
        svst1_f32(pg, dstp + vl * 2u, v2);
        svst1_f32(pg, dstp + vl * 3u, v3);
        srcp += vl * 4u;
        dstp += vl * 4u;
        rem  -= vl * 4u;
    }
    while (rem >= vl) {
        svfloat32_t v = svld1_f32(pg, srcp);
        svst1_f32(pg, dstp, v);
        srcp += vl;
        dstp += vl;
        rem  -= vl;
    }
    if (rem > 0) {
        svbool_t tail = svwhilelt_b32_u64(0UL, (uint64_t)rem);
        svfloat32_t v = svld1_f32(tail, srcp);
        svst1_f32(tail, dstp, v);
    }
#else
    if (size) memcpy(dst, src, size);
#endif
}
