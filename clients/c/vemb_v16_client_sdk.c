#define _GNU_SOURCE

#include "vemb_v16_client_sdk.h"
#include "vemb_v16_cluster_core.h"
#include "vemb_v16_data_transport.h"
#include "vemb_v16_owner_session.h"
#include "internal/vemb_v16_cli_deadline.h"
#include "internal/vemb_v16_cli_l0.h"
#include "internal/vemb_v16_cli_l1.h"
#include "../../src/vemb_v16_net.h"
#include "../../src/vemb_v16_aeron_attach.h"  /* TCP ATTACH protocol */
#include "../../src/vemb_v16_batch_ring.h"
#include "../../src/redisassert.h"
#include "../../src/vemb_v16_util.h"
#include "../../src/cpu_relax.h"
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
#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <errno.h>
#include <poll.h>
#include <sched.h>

#ifdef USE_ARM_SVE
#include <arm_sve.h>
#endif

#define VEMB_V16_SDK_MAX_ENDPOINTS 16
/* Encoded request buffer — large enough for any op:
 *   24 (base) + 4 (vector_bytes) + 128 (key) +
 *   VEMB_V16_MAX_DIM*sizeof(float) (vector).
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

typedef vemb_v16_data_channel_t sdk_backend_t;

typedef struct sdk_tcp_pending {
    uint64_t operation_id;
    uint64_t submit_epoch;
    uint32_t owner_id;
    uint32_t wire_req_id;
    uint8_t expected_op;
    uint8_t *inline_vector;
    uint32_t inline_vector_cap;
} sdk_tcp_pending_t;

typedef struct sdk_tcp_channel {
    int      fd;
    vemb_v16_channel_desc_t desc;
    sdk_tcp_pending_t *pending;
    uint32_t pending_count;
    uint32_t pending_cap;
} sdk_tcp_channel_t;

/* UB channel state stays entirely behind the internal data-transport
 * boundary. The core only observes the channel generation and the logical
 * operation identity returned by poll(). */
typedef struct sdk_ub_pending {
    uint64_t operation_id;
    uint64_t submit_epoch;
    uint32_t owner_id;
    uint32_t wire_req_id;
    uint8_t expected_op;
    uint8_t *inline_vector;
    uint32_t inline_vector_cap;
} sdk_ub_pending_t;

typedef struct sdk_ub_channel {
    vemb_v16_aeron_channel_t *aeron;
    sdk_ub_pending_t *pending;
    uint32_t pending_count;
    uint32_t pending_cap;
} sdk_ub_channel_t;

/* sdk_ub_poll 快路径自旋窗口 (ns), VEMB_V16_UB_POLL_SPIN_US 可调 (默认 300us);
 * 覆盖典型 server 批处理 RTT (cluster key-key ~250us), 避免每批落进 1ms poll
 * 睡眠。实测 300us 已到吞吐平台 (~540K ops/s), 更长窗口无收益 */
static uint64_t ub_poll_spin_ns = 300000;
__attribute__((constructor)) static void ub_poll_spin_init(void) {
    const char *s = getenv("VEMB_V16_UB_POLL_SPIN_US");
    if (s && *s)
        ub_poll_spin_ns = (uint64_t)strtoull(s, NULL, 10) * 1000ull;
}

static int sdk_tcp_open_owner_channel(
    vemb_v16_data_channel_t *channel,
    const vemb_v16_transport_open_spec_t *spec);
static void sdk_tcp_close_channel(vemb_v16_data_channel_t *channel);
static int sdk_tcp_submit(vemb_v16_data_channel_t *channel,
                          const vemb_v16_transport_submission_t *submission);
static vemb_v16_transport_poll_result_t sdk_tcp_poll(
    vemb_v16_data_channel_t *channel,
    uint32_t timeout_ms,
    vemb_v16_transport_completion_t *out);
static vemb_v16_transport_resource_state_t sdk_tcp_check_resource(
    vemb_v16_data_channel_t *channel);
static int sdk_tcp_fence(vemb_v16_data_channel_t *channel);
static int sdk_ub_open_owner_channel(
    vemb_v16_data_channel_t *channel,
    const vemb_v16_transport_open_spec_t *spec);
static void sdk_ub_close_channel(vemb_v16_data_channel_t *channel);
static int sdk_ub_submit(vemb_v16_data_channel_t *channel,
                         const vemb_v16_transport_submission_t *submission);
static vemb_v16_transport_poll_result_t sdk_ub_poll(
    vemb_v16_data_channel_t *channel,
    uint32_t timeout_ms,
    vemb_v16_transport_completion_t *out);
static int sdk_ub_open_warm_region(vemb_v16_data_channel_t *channel,
                                   vemb_v16_warm_view_t **out);
static int sdk_ub_read_warm_vector(vemb_v16_data_channel_t *channel,
                                   uint32_t region_id,
                                   uint64_t offset,
                                   uint32_t bytes,
                                   void *out,
                                   uint32_t out_cap);
static vemb_v16_transport_resource_state_t sdk_ub_check_resource(
    vemb_v16_data_channel_t *channel);
static int sdk_ub_fence(vemb_v16_data_channel_t *channel);

static const vemb_v16_data_transport_ops_t sdk_tcp_data_transport_ops = {
    .open_owner_channel = sdk_tcp_open_owner_channel,
    .submit = sdk_tcp_submit,
    .poll = sdk_tcp_poll,
    .check_resource = sdk_tcp_check_resource,
    .fence = sdk_tcp_fence,
    .close_channel = sdk_tcp_close_channel,
};

static const vemb_v16_data_transport_ops_t sdk_ub_data_transport_ops = {
    .open_owner_channel = sdk_ub_open_owner_channel,
    .submit = sdk_ub_submit,
    .poll = sdk_ub_poll,
    .open_warm_region = sdk_ub_open_warm_region,
    .read_warm_vector = sdk_ub_read_warm_vector,
    .check_resource = sdk_ub_check_resource,
    .fence = sdk_ub_fence,
    .close_channel = sdk_ub_close_channel,
};

typedef struct sdk_ub_owner_open_context {
    const vemb_v16_ub_peer_view_manifest_t *peer_view_manifest;
    const char *client_host;
} sdk_ub_owner_open_context_t;

struct vemb_v16_client_handle_session;
struct vemb_v16_client_vector_session;

typedef struct sdk_owner_v2 {
    vemb_v16_aeron_batch_channel_t *channel;
    /* One L0 per routed owner. channel_count=1 is deliberate: the common
     * core has already selected the owner before a key can enter this map. */
    vemb_v16_cli_l0_t *l0;
    uint64_t next_batch_id;
    uint32_t effective_batch_size;
    uint32_t max_batch_bytes;
    /* A rejected ATTACH stays on v1 until topology changes or a channel
     * failure makes a fresh v2 lifecycle necessary. */
    uint8_t unavailable;
} sdk_owner_v2_t;

static void sdk_backend_init(sdk_backend_t *backend,
                             const vemb_v16_data_transport_ops_t *transport)
{
    *backend = (sdk_backend_t){
        .ops = transport,
    };
}

static int sdk_backend_open(sdk_backend_t *backend, uint32_t dim,
                            uint32_t timeout_ms, uint32_t owner_id,
                            const void *backend_context)
{
    vemb_v16_transport_open_spec_t spec = {
        .host = backend->host,
        .port = backend->port,
        .vector_dim = dim,
        .timeout_ms = timeout_ms,
        .owner_id = owner_id,
        .backend_context = backend_context,
    };
    if (backend->ops->open_owner_channel(backend, &spec) != 0)
        return -1;
    return 0;
}

static void sdk_backend_close(sdk_backend_t *backend)
{
    assert(backend->ops != NULL);
    backend->resource_generation = 0;
    backend->resource_checked_topology_epoch = 0;
    /* A failed poll closes broken transport state immediately. The owner
     * lifecycle still reaches here to clear its logical channel state. */
    RETURN_IF(backend->state == NULL);
    backend->ops->close_channel(backend);
}

static int sdk_backend_ready(const sdk_backend_t *backend)
{
    return backend->state != NULL;
}

static int sdk_backend_matches_endpoint(
    const sdk_backend_t *backend,
    const vemb_v16_topology_endpoint_t *endpoint,
    const vemb_v16_data_transport_ops_t *transport)
{
    return backend->ops == transport &&
        sdk_backend_ready(backend) &&
        backend->port == endpoint->tcp_port &&
        strcmp(backend->host, endpoint->host) == 0;
}

static int sdk_backend_submit(sdk_backend_t *backend,
                              const vemb_v16_transport_submission_t *submission)
{
    return backend->ops->submit(backend, submission);
}

static vemb_v16_transport_poll_result_t sdk_backend_poll(
    sdk_backend_t *backend,
    uint32_t timeout_ms,
    vemb_v16_transport_completion_t *completion)
{
    return backend->ops->poll(backend, timeout_ms, completion);
}

static vemb_v16_transport_resource_state_t sdk_backend_check_resource(
    sdk_backend_t *backend)
{
    return backend->ops->check_resource(backend);
}

static int sdk_backend_fence(sdk_backend_t *backend)
{
    return backend->ops->fence(backend);
}

static int sdk_backend_read_warm_vector(sdk_backend_t *backend,
                                        uint32_t region_id,
                                        uint64_t offset,
                                        uint32_t bytes,
                                        void *out,
                                        uint32_t out_cap)
{
    return backend->ops->read_warm_vector(backend, region_id, offset, bytes,
                                          out, out_cap);
}

typedef struct sdk_bootstrap_seed {
    char host[64];
    uint16_t port;
} sdk_bootstrap_seed_t;

struct vemb_v16_client {
    uint32_t dim;
    uint32_t req_id;
    uint32_t connect_timeout_ms; /* 0 = default 10000 */
    uint32_t ub_batch_request_size;
    uint32_t transport_type;
    uint8_t vector_read_op;

    /* Bootstrap addresses carry TCP control only. They are never data
     * channels and are retried round-robin for topology/control requests. */
    uint32_t bootstrap_seed_count;
    uint32_t next_topology_seed;
    uint64_t topology_fetch_attempts;
    sdk_bootstrap_seed_t bootstrap_seeds[VEMB_V16_SDK_MAX_ENDPOINTS];

    /* Shared routing, retry and logical-completion state. It deliberately
     * does not own any transport channel or mapping. */
    vemb_v16_cluster_core_t cluster;

    /* Owner-keyed channels, opened lazily by ensure_owner_channel(). */
    sdk_backend_t   owner_channels[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];
    uint8_t         owner_channel_inited[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];

    /* MOVED override: once any key returns MOVED with target_owner=X, the
     * cached active ring is permanently stale for this client's lifetime
     * (we don't auto-refresh). All subsequent multi-endpoint ops route to
     * override_owner instead of consulting the ring, which avoids the
     * thundering-herd of N workers each opening a fresh TCP control
     * connection to fetch topology. UINT32_MAX = disabled.
     *
     * Valid because cutover starts only after every key's baseline has
     * been pushed to the target (DEST_COMMITTED on node1), so reads of
     * any key against the override owner will succeed there. */
    uint32_t        override_owner;

    /* v2 state is owner-scoped and remains subordinate to the permanent v1
     * channel lifecycle. cluster_core alone supplies route, epoch and retry. */
    vemb_v16_owner_session_t
        owner_sessions[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];
    uint8_t owner_session_inited[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];
    sdk_owner_v2_t owner_v2[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];
    struct vemb_v16_client_handle_session *handle_session;
    struct vemb_v16_client_vector_session *vector_session;

    /* The manifest is client-local deployment state. It is intentionally
     * absent from topology and only selected for matching remote owners. */
    vemb_v16_ub_peer_view_manifest_t ub_peer_view_manifest;
    char            ub_peer_view_client_host[VEMB_V16_UB_PEER_VIEW_HOST_MAX];
    uint8_t         ub_peer_view_ready;

    /* A VEMB_HANDLE belongs to the channel that produced it. The binding is
     * generation-scoped so a closed/re-attached channel cannot serve an old
     * descriptor through a reused owner slot. */
    sdk_backend_t   *last_handle_channel;
    uint64_t        last_handle_generation;
    uint32_t        last_handle_region_id;

};

static void sdk_format_owner_ring(char *out, size_t out_cap,
                                  const vemb_v16_topology_ring_t *ring)
{
    size_t used = 0;
    out[0] = '\0';
    used += (size_t)snprintf(out + used, out_cap - used, "{");
    for (uint32_t i = 0; i < ring->owner_count && used < out_cap; i++) {
        used += (size_t)snprintf(out + used, out_cap - used,
                                 "%s%u", i ? "," : "", ring->owners[i]);
    }
    if (used < out_cap)
        (void)snprintf(out + used, out_cap - used, "}");
    else
        out[out_cap - 1] = '\0';
}

static int sdk_topology_rings_equal(const vemb_v16_topology_ring_t *a,
                                    const vemb_v16_topology_ring_t *b)
{
    if (a->owner_count != b->owner_count ||
        a->node_count != b->node_count)
        return 0;
    for (uint32_t i = 0; i < a->owner_count; i++) {
        if (a->owners[i] != b->owners[i])
            return 0;
    }
    return 1;
}

static void sdk_warn_topology_transport_mismatch(
    const vemb_v16_client_t *client,
    const vemb_v16_client_topology_t *topology,
    const char *seed_host,
    uint16_t seed_port)
{
    for (uint32_t i = 0; i < topology->endpoint_count; i++) {
        const vemb_v16_topology_endpoint_t *endpoint =
            &topology->endpoints[i];
        if (endpoint->transport_type == client->transport_type)
            continue;
        fprintf(stderr,
                "[sdk][WARNING] topology endpoint transport mismatch: "
                "client=%s endpoint=%s owner=%u seed=%s:%u; "
                "client startup transport remains fixed\n",
                vemb_v16_transport_name(client->transport_type),
                vemb_v16_transport_name(endpoint->transport_type),
                endpoint->owner_id, seed_host, seed_port);
        return;
    }
}

static void sdk_owner_v2_stop(vemb_v16_client_t *client, uint32_t owner_id);
static void sdk_owner_channel_close(vemb_v16_client_t *client,
                                    uint32_t owner_id);
static void sdk_handle_session_owner_quiesce(
    vemb_v16_client_t *client, uint32_t owner_id);
static void sdk_vector_session_owner_quiesce(
    vemb_v16_client_t *client, uint32_t owner_id);

/* ------------------------------------------------------------------ */
/* Shared helpers (used by both sync & async APIs)                    */
/* ------------------------------------------------------------------ */

int vemb_v16_build_combined_key(char *out, size_t out_cap,
                                const char *set_name, const char *elem_name,
                                uint32_t *out_len)
{
    assert(out != NULL && out_cap > 0 && elem_name != NULL && out_len != NULL);
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
                                          int writable,
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
    size_t payload_len = vemb_v16_alloc_req_encoded_len();
    size_t total = sizeof(vemb_v16_net_hdr_t) + payload_len;
    if (buf_cap < total)
        return -1;

    vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)buf;
    memset(hdr, 0, sizeof(*hdr));
    hdr->magic = VEMB_V16_MAGIC;
    hdr->version = VEMB_V16_VERSION;
    hdr->type = VEMB_V16_NET_HELLO;
    hdr->payload_len = (uint32_t)payload_len;

    vemb_v16_alloc_req_t req = {
        .vector_dim = vector_dim,
        .flags = flags,
    };
    if (vemb_v16_alloc_req_encode((uint8_t *)buf + sizeof(*hdr),
                                  payload_len,
                                  &req,
                                  NULL) != 0) {
        return -1;
    }

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
    if (out_resp->req_id != hdr->req_id)
        return -1;

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

static sdk_tcp_channel_t *sdk_tcp_state(const sdk_backend_t *channel)
{
    return channel->state;
}

static int sdk_tcp_pending_reserve(sdk_tcp_channel_t *state)
{
    if (state->pending_count < state->pending_cap)
        return 0;
    uint32_t next_cap = state->pending_cap ? state->pending_cap * 2u : 32u;
    size_t pending_bytes = (size_t)next_cap * sizeof(*state->pending);
    if (next_cap < state->pending_cap ||
        pending_bytes / sizeof(*state->pending) != next_cap) {
        return -1;
    }
    sdk_tcp_pending_t *pending = realloc(
        state->pending, pending_bytes);
    assert(pending != NULL);
    state->pending = pending;
    state->pending_cap = next_cap;
    return 0;
}

static sdk_tcp_pending_t *sdk_tcp_pending_find(sdk_tcp_channel_t *state,
                                               uint32_t wire_req_id,
                                               uint32_t *out_index)
{
    for (uint32_t i = 0; i < state->pending_count; i++) {
        if (state->pending[i].wire_req_id == wire_req_id) {
            *out_index = i;
            return &state->pending[i];
        }
    }
    return NULL;
}

static void sdk_tcp_pending_remove(sdk_tcp_channel_t *state, uint32_t index)
{
    state->pending_count--;
    if (index != state->pending_count)
        state->pending[index] = state->pending[state->pending_count];
}

/* TCP data-plane channel creation. The caller has already validated the
 * endpoint configuration; this boundary validates the decoded WELCOME frame. */
static int sdk_tcp_open_owner_channel(
    vemb_v16_data_channel_t *channel,
    const vemb_v16_transport_open_spec_t *spec)
{
    assert(channel->state == NULL);

    uint32_t effective_timeout = spec->timeout_ms ? spec->timeout_ms : 10000;
    int fd = -1;
    for (int retry = 0; retry < 50; retry++) {
        fd = vemb_v16_net_connect(spec->host, spec->port, effective_timeout);
        if (fd >= 0) break;
        usleep(100000);
    }
    if (fd < 0) {
        fprintf(stderr, "vemb_v16_client: connect %s:%u failed\n",
                spec->host, spec->port);
        return -1;
    }

    char hello_buf[64];
    ssize_t hello_len = vemb_v16_serialize_hello(hello_buf, sizeof(hello_buf),
                                                  spec->vector_dim, 0);
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
    if (unlikely(desc.vector_dim != spec->vector_dim ||
                 desc.vector_stride != spec->vector_dim * sizeof(float) ||
                 desc.request_ring_slot_size == 0 ||
                 desc.response_ring_slot_size == 0)) {
        fprintf(stderr,
                "vemb_v16_client: WELCOME dimension mismatch: client_dim=%u "
                "server_dim=%u stride=%u\n",
                spec->vector_dim, desc.vector_dim, desc.vector_stride);
        close(fd);
        return -1;
    }

    sdk_tcp_channel_t *state = calloc(1, sizeof(*state));
    assert(state != NULL);
    state->fd = fd;
    state->desc = desc;
    channel->state = state;
    channel->channel_id = desc.channel_id;
    channel->generation++;
    if (channel->generation == 0)
        channel->generation = 1;
    return 0;
}

/* Fetch topology over a fresh TCP control connection. A bootstrap address
 * can be down or stale, so every snapshot attempt walks the complete seed
 * set from a rotating start point. The snapshot determines owner endpoints;
 * the client startup contract determines the data transport. */
static int fetch_topology_via_bootstrap_seeds(vemb_v16_client_t *client)
{
    if (client->bootstrap_seed_count == 0)
        return -1;

    uint32_t start = client->next_topology_seed %
        client->bootstrap_seed_count;
    uint32_t timeout_ms = client->connect_timeout_ms ?
        client->connect_timeout_ms : 5000;
    uint64_t fetch_no = ++client->topology_fetch_attempts;
    for (uint32_t attempt = 0; attempt < client->bootstrap_seed_count;
         attempt++) {
        uint32_t index = (start + attempt) % client->bootstrap_seed_count;
        const sdk_bootstrap_seed_t *seed = &client->bootstrap_seeds[index];
        vemb_v16_client_topology_t topology;
        if (vemb_v16_client_topology_fetch_tcp(
                seed->host, seed->port, timeout_ms, &topology, NULL) != 0) {
            continue;
        }
        sdk_warn_topology_transport_mismatch(client, &topology,
                                             seed->host, seed->port);
        const vemb_v16_client_topology_t *old =
            vemb_v16_cluster_core_topology(&client->cluster);
        int topology_changed = old->current_topology_epoch !=
                topology.current_topology_epoch ||
            !sdk_topology_rings_equal(&old->active_ring,
                                      &topology.active_ring) ||
            !sdk_topology_rings_equal(&old->standby_ring,
                                      &topology.standby_ring) ||
            old->endpoint_count != topology.endpoint_count;
        if (topology_changed || fetch_no <= 3 || fetch_no % 1000 == 0) {
            char active[128];
            char standby[128];
            sdk_format_owner_ring(active, sizeof(active),
                                  &topology.active_ring);
            sdk_format_owner_ring(standby, sizeof(standby),
                                  &topology.standby_ring);
            fprintf(stderr,
                    "[sdk] topology refresh #%llu seed=%s:%u "
                    "epoch=%llu->%llu min_write=%llu active=%s standby=%s "
                    "endpoints=%u flags=0x%x changed=%d\n",
                    (unsigned long long)fetch_no, seed->host, seed->port,
                    (unsigned long long)old->current_topology_epoch,
                    (unsigned long long)topology.current_topology_epoch,
                    (unsigned long long)topology.min_write_epoch,
                    active, standby, topology.endpoint_count, topology.flags,
                    topology_changed);
        }
        vemb_v16_cluster_core_publish_topology(&client->cluster, &topology);
        int migration_prepare = (topology.flags &
            VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED) != 0;
        for (uint32_t owner = 0;
             owner < VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS; owner++) {
            if (!client->owner_session_inited[owner])
                continue;
            int topology_changed =
                !client->owner_sessions[owner].topology_observed ||
                client->owner_sessions[owner].observed_topology_epoch !=
                    topology.current_topology_epoch;
            int v2_quiesce = vemb_v16_owner_session_observe_topology(
                &client->owner_sessions[owner],
                topology.current_topology_epoch, migration_prepare);
            /* L0/cache identity includes topology epoch even for TCP, where
             * the owner session deliberately remains V1_ONLY. */
            if (topology_changed || v2_quiesce) {
                if (client->handle_session)
                    sdk_handle_session_owner_quiesce(client, owner);
                if (client->vector_session)
                    sdk_vector_session_owner_quiesce(client, owner);
            }
            if (topology_changed)
                client->owner_v2[owner].unavailable = 0;
        }
        client->next_topology_seed =
            (index + 1) % client->bootstrap_seed_count;
        return 0;
    }
    if (fetch_no <= 3 || fetch_no % 1000 == 0)
        fprintf(stderr, "[sdk] topology refresh #%llu failed: all bootstrap seeds unavailable\n",
                (unsigned long long)fetch_no);
    return -1;
}

static void sdk_tcp_close_channel(vemb_v16_data_channel_t *channel)
{
    sdk_tcp_channel_t *state = sdk_tcp_state(channel);
    if (state->fd >= 0) {
        char close_buf[32];
        memset(close_buf, 0, sizeof(close_buf));
        vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)close_buf;
        hdr->magic = VEMB_V16_MAGIC;
        hdr->version = VEMB_V16_VERSION;
        hdr->type = VEMB_V16_NET_CLOSE_CHANNEL;
        hdr->channel_id = channel->channel_id;
        vemb_v16_net_write_full(state->fd, close_buf,
                                 sizeof(vemb_v16_net_hdr_t));
        close(state->fd);
    }
    free(state->pending);
    free(state);
    channel->state = NULL;
    channel->channel_id = 0;
}

static vemb_v16_transport_resource_state_t sdk_tcp_check_resource(
    vemb_v16_data_channel_t *channel)
{
    (void)channel;
    return VEMB_V16_TRANSPORT_RESOURCE_CURRENT;
}

static int sdk_tcp_fence(vemb_v16_data_channel_t *channel)
{
    return sdk_tcp_state(channel)->pending_count == 0 ? 0 : -1;
}

static int sdk_tcp_submit(vemb_v16_data_channel_t *channel,
                          const vemb_v16_transport_submission_t *submission)
{
    sdk_tcp_channel_t *state = sdk_tcp_state(channel);
    uint32_t unused_index = 0;
    if (sdk_tcp_pending_find(state, submission->wire_req_id, &unused_index))
        return -1;
    if (sdk_tcp_pending_reserve(state) != 0)
        return -1;

    sdk_tcp_pending_t *pending = &state->pending[state->pending_count++];
    *pending = (sdk_tcp_pending_t){
        .operation_id = submission->operation_id,
        .submit_epoch = submission->submit_epoch,
        .owner_id = submission->owner_id,
        .wire_req_id = submission->wire_req_id,
        .expected_op = submission->request->op,
        .inline_vector = submission->inline_vector,
        .inline_vector_cap = submission->inline_vector_cap,
    };
    if (sdk_write_request(state->fd, channel->channel_id,
                          submission->wire_req_id, submission->request) == 0) {
        return 0;
    }
    state->pending_count--;
    return -1;
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

static int sdk_tcp_read_inline(int fd, uint8_t *out, uint32_t out_cap,
                               size_t inline_bytes)
{
    if (out && inline_bytes <= out_cap)
        return vemb_v16_net_read_full(fd, out, inline_bytes);

    uint8_t discard[256];
    size_t remaining = inline_bytes;
    while (remaining > 0) {
        size_t chunk = remaining < sizeof(discard) ? remaining : sizeof(discard);
        if (vemb_v16_net_read_full(fd, discard, chunk) != 0)
            return -1;
        remaining -= chunk;
    }
    return -1;
}

static vemb_v16_transport_poll_result_t sdk_tcp_poll(
    vemb_v16_data_channel_t *channel,
    uint32_t timeout_ms,
    vemb_v16_transport_completion_t *out)
{
    sdk_tcp_channel_t *state = sdk_tcp_state(channel);
    struct pollfd pfd = {
        .fd = state->fd,
        .events = POLLIN,
    };
    int wait_ms = timeout_ms == VEMB_V16_TRANSPORT_WAIT_FOREVER ? -1 :
        (timeout_ms > INT_MAX ? INT_MAX : (int)timeout_ms);
    int poll_rc;
    do {
        poll_rc = poll(&pfd, 1, wait_ms);
    } while (poll_rc < 0 && errno == EINTR);
    if (poll_rc == 0)
        return VEMB_V16_TRANSPORT_POLL_EMPTY;
    /* POLLIN and POLLHUP may arrive together when the peer sent its final
     * frame before closing. Consume that frame; the next poll observes EOF. */
    if (poll_rc < 0 || (pfd.revents & POLLNVAL) ||
        !(pfd.revents & POLLIN))
        goto failed;

    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_full(state->fd, &hdr, sizeof(hdr)) != 0)
        goto failed;
    if (hdr.magic != VEMB_V16_MAGIC || hdr.version != VEMB_V16_VERSION ||
        hdr.type != VEMB_V16_NET_RESPONSE ||
        hdr.channel_id != channel->channel_id ||
        hdr.payload_len < vemb_v16_resp_encoded_base_len()) {
        goto failed;
    }

    uint8_t base[6];
    if (vemb_v16_net_read_full(state->fd, base, sizeof(base)) != 0)
        goto failed;
    uint8_t status = base[0];
    uint8_t op = (uint8_t)(base[1] & VEMB_V16_TCP_RESP_OP_MASK);
    size_t metadata_len = vemb_v16_resp_encoded_len_for_fields(status, op);
    if (metadata_len > hdr.payload_len)
        goto failed;

    size_t body_len = metadata_len - sizeof(base);
    uint8_t body[28];
    if (body_len > 0) {
        if (body_len > sizeof(body))
            goto failed;
        if (vemb_v16_net_read_full(state->fd, body, body_len) != 0)
            goto failed;
    }

    uint8_t meta[34];
    memcpy(meta, base, sizeof(base));
    if (body_len > 0)
        memcpy(meta + sizeof(base), body, body_len);
    vemb_v16_resp_t response;
    if (vemb_v16_resp_decode(&response, meta, metadata_len) != 0)
        goto failed;

    size_t inline_bytes = hdr.payload_len - metadata_len;
    uint32_t pending_index = 0;
    sdk_tcp_pending_t *pending = sdk_tcp_pending_find(state, hdr.req_id,
                                                       &pending_index);
    int identity_valid = pending && response.req_id == hdr.req_id &&
        response.op == pending->expected_op;
    if (inline_bytes > 0) {
        int inline_valid = response.status == VEMB_V16_STATUS_OK &&
            response.op == VEMB_V16_OP_VEMB_INLINE &&
            response.vector_bytes == inline_bytes;
        if (sdk_tcp_read_inline(state->fd,
                                identity_valid ? pending->inline_vector : NULL,
                                identity_valid ? pending->inline_vector_cap : 0,
                                inline_bytes) != 0 || !inline_valid) {
            goto failed;
        }
    }
    if (!identity_valid)
        goto failed;

    *out = (vemb_v16_transport_completion_t){
        .operation_id = pending->operation_id,
        .channel_generation = channel->generation,
        .owner_id = pending->owner_id,
        .submit_epoch = pending->submit_epoch,
        .response = response,
        .inline_vector_bytes = (uint32_t)inline_bytes,
    };
    sdk_tcp_pending_remove(state, pending_index);
    return VEMB_V16_TRANSPORT_POLL_COMPLETION;

failed:
    sdk_tcp_close_channel(channel);
    return VEMB_V16_TRANSPORT_POLL_FAILED;
}

/* ------------------------------------------------------------------ */
/* Public Sync API                                                    */
/* ------------------------------------------------------------------ */

vemb_v16_client_t *vemb_v16_client_create(const char *seeds[],
                                           int seed_count,
                                           uint32_t dim,
                                           uint32_t timeout_ms,
                                           uint32_t transport_type)
{
    if (!seeds || seed_count <= 0 ||
        seed_count > VEMB_V16_SDK_MAX_ENDPOINTS ||
        dim == 0 || dim > VEMB_V16_MAX_DIM ||
        (transport_type != VEMB_V16_TRANSPORT_TCP &&
         transport_type != VEMB_V16_TRANSPORT_AERON))
        return NULL;

    vemb_v16_client_t *c = calloc(1, sizeof(*c));
    assert(c != NULL);
    c->dim               = dim;
    c->req_id            = 1;
    c->ub_batch_request_size = VEMB_V16_BATCH_REQUEST_SIZE_DEFAULT;
    c->transport_type = transport_type;
    c->vector_read_op = transport_type == VEMB_V16_TRANSPORT_AERON ?
        VEMB_V16_OP_VEMB_HANDLE : VEMB_V16_OP_VEMB_INLINE;
    c->bootstrap_seed_count = (uint32_t)seed_count;
    c->connect_timeout_ms = timeout_ms;
    vemb_v16_cluster_core_init(&c->cluster, 1, 256);

    for (int i = 0; i < seed_count; i++) {
        char host[64]; uint16_t port;
        if (!seeds[i] ||
            sdk_parse_endpoint(seeds[i], host, sizeof(host), &port) != 0) {
            fprintf(stderr, "vemb_v16_client: bad bootstrap seed '%s'\n",
                    seeds[i] ? seeds[i] : "(null)");
            free(c); return NULL;
        }
        sdk_bootstrap_seed_t *seed = &c->bootstrap_seeds[i];
        memcpy(seed->host, host, strlen(host) + 1);
        seed->port = port;
    }

    return c;
}

int vemb_v16_client_configure_ub_peer_view(
    vemb_v16_client_t *client, const char *manifest_path,
    const char *client_host)
{
    if (!manifest_path || !manifest_path[0] || !client_host ||
        client->transport_type != VEMB_V16_TRANSPORT_AERON ||
        !client_host[0] || strlen(client_host) >=
            sizeof(client->ub_peer_view_client_host)) {
        return -1;
    }
    for (uint32_t i = 0; i < VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS; i++) {
        if (client->owner_channels[i].ops == &sdk_ub_data_transport_ops)
            return -1;
    }

    vemb_v16_ub_peer_view_manifest_t manifest;
    if (vemb_v16_ub_peer_view_manifest_load(manifest_path, &manifest) != 0)
        return -1;
    uint32_t matching_entries = 0;
    for (uint32_t i = 0; i < manifest.entry_count; i++) {
        if (!strcmp(manifest.entries[i].client_host, client_host))
            matching_entries++;
    }
    if (matching_entries == 0)
        return -1;

    client->ub_peer_view_manifest = manifest;
    memcpy(client->ub_peer_view_client_host, client_host,
           strlen(client_host) + 1);
    client->ub_peer_view_ready = 1;
    return 0;
}

int vemb_v16_client_set_ub_batch_request_size(
    vemb_v16_client_t *client, uint32_t requested_batch_size)
{
    if (requested_batch_size == 0 ||
        client->transport_type != VEMB_V16_TRANSPORT_AERON ||
        requested_batch_size > VEMB_V16_BATCH_REQUEST_SIZE_MAX) {
        return -1;
    }
    for (uint32_t i = 0; i < VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS; i++) {
        if (client->owner_v2[i].channel)
            return -1;
    }
    client->ub_batch_request_size = requested_batch_size;
    return 0;
}

void vemb_v16_client_destroy(vemb_v16_client_t *c)
{
    if (c->handle_session)
        vemb_v16_client_handle_session_close(c->handle_session, NULL, NULL);
    if (c->vector_session)
        vemb_v16_client_vector_session_close(c->vector_session, NULL, NULL);
    /* Close data channels selected by topology. */
    for (uint32_t i = 0; i < VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS; i++) {
        if (c->owner_channel_inited[i])
            sdk_owner_channel_close(c, i);
    }
    free(c);
}

/* v2 shares the owner resource-generation fence with v1. The synchronous
 * pipeline drains a frame inline; the event-loop session drains all published
 * frames before this close path is allowed to release either mapping. */
static void sdk_owner_v2_stop(vemb_v16_client_t *client, uint32_t owner_id)
{
    sdk_owner_v2_t *v2 = &client->owner_v2[owner_id];
    assert(client->owner_session_inited[owner_id]);

    vemb_v16_owner_session_t *session =
        &client->owner_sessions[owner_id];
    if (session->v2_state == VEMB_V16_OWNER_SESSION_V2_READY)
        (void)vemb_v16_owner_session_begin_quiesce(session);
    if (session->v2_state == VEMB_V16_OWNER_SESSION_V2_QUIESCING)
        vemb_v16_owner_session_finish_quiescing(session);
    assert(session->v2_state != VEMB_V16_OWNER_SESSION_V2_DRAINING);

    if (v2->channel)
        vemb_v16_aeron_batch_close(v2->channel);
    if (v2->l0)
        vemb_v16_cli_l0_destroy(v2->l0);
    *v2 = (sdk_owner_v2_t){0};
}

static void sdk_owner_channel_close(vemb_v16_client_t *client,
                                    uint32_t owner_id)
{
    assert(client->owner_channel_inited[owner_id]);
    if (client->vector_session)
        sdk_vector_session_owner_quiesce(client, owner_id);
    sdk_owner_v2_stop(client, owner_id);
    sdk_backend_close(&client->owner_channels[owner_id]);
    client->owner_channel_inited[owner_id] = 0;
    vemb_v16_cluster_core_owner_channel_closed(&client->cluster, owner_id);
}

/* v2 is optional and is never the source of owner routing. It can only be
 * reopened after the permanent v1 channel established the owner generation.
 * Peer-view is mandatory for every owner of an AERON client: a rejected ATTACH
 * or migration state leaves the owner on v1 without changing the logical
 * operation. */
static int sdk_owner_v2_enable(vemb_v16_client_t *client, uint32_t owner_id)
{
    sdk_backend_t *backend = &client->owner_channels[owner_id];
    assert(client->owner_session_inited[owner_id]);

    vemb_v16_owner_session_t *session =
        &client->owner_sessions[owner_id];
    sdk_owner_v2_t *v2 = &client->owner_v2[owner_id];
    if (v2->unavailable)
        return 0;
    if (session->v2_state == VEMB_V16_OWNER_SESSION_V2_QUIESCING)
        vemb_v16_owner_session_finish_quiescing(session);
    if (session->migration_active ||
        session->v2_state == VEMB_V16_OWNER_SESSION_V2_DRAINING)
        return 0;
    if (session->v2_state == VEMB_V16_OWNER_SESSION_V2_READY) {
        assert(v2->channel != NULL && v2->l0 != NULL);
        return 1;
    }
    if (session->v2_state != VEMB_V16_OWNER_SESSION_V1_ONLY)
        return 0;

    vemb_v16_owner_session_begin_reopen(session);
    if (!v2->channel) {
        v2->channel = vemb_v16_aeron_open_remote_batch_with_peer_view(
            backend->host, backend->port, client->dim,
            client->ub_batch_request_size, 0,
            &client->ub_peer_view_manifest,
            client->ub_peer_view_client_host, owner_id);
        if (!v2->channel) {
            v2->unavailable = 1;
            vemb_v16_owner_session_v2_channel_failed(session);
            return 0;
        }
        vemb_v16_aeron_batch_resources_t resources;
        vemb_v16_aeron_batch_get_resources(v2->channel, &resources);
        v2->effective_batch_size = resources.effective_batch_size;
        v2->max_batch_bytes = resources.max_batch_bytes;
        v2->next_batch_id = 1;
        v2->l0 = vemb_v16_cli_l0_create(1);
    }
    vemb_v16_owner_session_v2_channel_ready(session);
    return 1;
}

/* Lazy-open a data channel to the given owner_id using endpoint info from
 * the cached topology. Returns 0 on success (channel ready in
 * owner_channels[owner_id]), -1 on failure. Idempotent: if channel already
 * open, returns 0 without re-opening.
 *
 * Its channel identity lives in cluster core. The client startup contract has
 * already fixed one data transport for every owner; a topology refresh may
 * move an endpoint but cannot change its transport. */
static int ensure_owner_channel(vemb_v16_client_t *client, uint32_t owner_id)
{
    assert(owner_id < VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS);

    const vemb_v16_topology_endpoint_t *ep =
        vemb_v16_client_topology_find_endpoint(
            vemb_v16_cluster_core_topology(&client->cluster), owner_id);
    if (!ep || !ep->host[0]) {
        fprintf(stderr, "vemb_v16_client: no endpoint for owner_id=%u\n",
                owner_id);
        return -1;
    }
    const vemb_v16_data_transport_ops_t *transport =
        client->transport_type == VEMB_V16_TRANSPORT_AERON ?
            &sdk_ub_data_transport_ops : &sdk_tcp_data_transport_ops;

    sdk_backend_t *b = &client->owner_channels[owner_id];
    const vemb_v16_client_topology_t *topology =
        vemb_v16_cluster_core_topology(&client->cluster);
    /* A topology epoch alone does not close an endpoint-stable channel. On
     * the first use after a new snapshot, UB asks the server whether the
     * already-attached allocation still has the same resource generation. */
    if (client->owner_channel_inited[owner_id] &&
        vemb_v16_cluster_core_owner_channel_ready(&client->cluster,
                                                   owner_id) &&
        sdk_backend_matches_endpoint(b, ep, transport)) {
        if (b->resource_checked_topology_epoch ==
            topology->current_topology_epoch)
            return 0;
        vemb_v16_transport_resource_state_t resource_state =
            sdk_backend_check_resource(b);
        if (resource_state == VEMB_V16_TRANSPORT_RESOURCE_FAILED)
            return -1;
        if (resource_state == VEMB_V16_TRANSPORT_RESOURCE_CURRENT) {
            b->resource_checked_topology_epoch =
                topology->current_topology_epoch;
            return 0;
        }
        /* The caller's owner-group completion loop has already drained all
         * logical operations. Do not discard a pending confirmation merely
         * to make room for a new resource generation. */
        if (sdk_backend_fence(b) != 0)
            return -1;
        sdk_owner_channel_close(client, owner_id);
    }
    if (client->owner_channel_inited[owner_id]) {
        if (sdk_backend_fence(b) != 0)
            return -1;
        sdk_owner_channel_close(client, owner_id);
    }
    /* The owner slot survives reattach so its local generation
     * remains monotonic and invalidates handles produced by the old mapping. */
    if (!b->ops)
        sdk_backend_init(b, transport);
    strncpy(b->host, ep->host, sizeof(b->host) - 1);
    b->host[sizeof(b->host) - 1] = '\0';
    b->port = ep->tcp_port;

    const void *backend_context = NULL;
    sdk_ub_owner_open_context_t ub_context;
    if (transport == &sdk_ub_data_transport_ops) {
        if (!client->ub_peer_view_ready)
            return -1;
        for (uint32_t i = 0;
             i < client->ub_peer_view_manifest.entry_count; i++) {
            const vemb_v16_ub_peer_view_entry_t *entry =
                &client->ub_peer_view_manifest.entries[i];
            if (entry->owner_id == owner_id &&
                !strcmp(entry->client_host,
                        client->ub_peer_view_client_host)) {
                ub_context = (sdk_ub_owner_open_context_t){
                    .peer_view_manifest = &client->ub_peer_view_manifest,
                    .client_host = client->ub_peer_view_client_host,
                };
                backend_context = &ub_context;
                break;
            }
        }
        if (!backend_context) {
            fprintf(stderr,
                    "vemb_v16_client: peer-view mapping missing for owner_id=%u client_host=%s\n",
                    owner_id, client->ub_peer_view_client_host);
            return -1;
        }
    }
    fprintf(stderr,
            "[sdk] owner channel open begin owner=%u endpoint=%s:%u "
            "transport=%s epoch=%llu\n",
            owner_id, ep->host, ep->tcp_port,
            vemb_v16_transport_name(client->transport_type),
            (unsigned long long)topology->current_topology_epoch);
    if (sdk_backend_open(b, client->dim, client->connect_timeout_ms,
                         owner_id, backend_context) != 0) {
        /* leave channel unopened; caller will handle submit failure */
        fprintf(stderr,
                "[sdk] owner channel open failed owner=%u endpoint=%s:%u "
                "transport=%s\n",
                owner_id, ep->host, ep->tcp_port,
                transport == &sdk_ub_data_transport_ops ? "aeron" : "tcp");
        return -1;
    }
    client->owner_channel_inited[owner_id] = 1;
    b->resource_checked_topology_epoch = topology->current_topology_epoch;
    vemb_v16_cluster_core_owner_channel_opened(&client->cluster, owner_id);
    fprintf(stderr,
            "[sdk] owner channel ready owner=%u channel=%llu endpoint=%s:%u "
            "transport=%s generation=%llu\n",
            owner_id, (unsigned long long)b->channel_id, b->host, b->port,
            transport == &sdk_ub_data_transport_ops ? "aeron" : "tcp",
            (unsigned long long)client->cluster.owner_channels[owner_id].generation);
    uint64_t owner_generation =
        client->cluster.owner_channels[owner_id].generation;
    if (!client->owner_session_inited[owner_id]) {
        vemb_v16_owner_session_init(&client->owner_sessions[owner_id],
                                    owner_id, owner_generation);
        client->owner_session_inited[owner_id] = 1;
    } else {
        vemb_v16_owner_session_rebind_owner_generation(
            &client->owner_sessions[owner_id], owner_generation);
    }
    (void)vemb_v16_owner_session_observe_topology(
        &client->owner_sessions[owner_id], topology->current_topology_epoch,
        (topology->flags & VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED) !=
            0);
    return 0;
}

/* ------------------------------------------------------------------ */
/* Common-core asynchronous VEMB_HANDLE session (AERON clients only) */
/* ------------------------------------------------------------------ */

#define SDK_HANDLE_SESSION_MAX_PENDING VEMB_V16_CLI_L0_MAX_FOLLOWERS
#define SDK_HANDLE_SESSION_NO_ENTRY UINT32_MAX

enum sdk_handle_session_request_state {
    SDK_HANDLE_SESSION_REQUEST_FREE = 0,
    SDK_HANDLE_SESSION_REQUEST_ROUTE,
    SDK_HANDLE_SESSION_REQUEST_L0,
    SDK_HANDLE_SESSION_REQUEST_V1,
};

typedef struct sdk_handle_session_request {
    vemb_v16_cluster_operation_t operation;
    uint64_t caller_cookie;
    uint32_t owner_id;
    uint32_t l0_entry_id;
    uint32_t l0_generation;
    uint16_t key_len;
    uint16_t key2_len;          /* VSIM_KEY_KEY 第二 key (仅 v1 路径) */
    uint8_t op;                 /* v1 提交的 op (默认 VEMB_HANDLE) */
    uint8_t state;
    uint8_t l0_leader;
    uint8_t force_v1;
    char key[VEMB_V16_MAX_KEY_LEN];
    char key2[VEMB_V16_MAX_KEY_LEN];
} sdk_handle_session_request_t;

struct vemb_v16_client_handle_session {
    vemb_v16_client_t *client;
    vemb_v16_cli_deadline_t
        deadlines[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];
    sdk_handle_session_request_t requests[SDK_HANDLE_SESSION_MAX_PENDING];
    uint32_t active_requests;
    uint8_t topology_refresh_pending;
    uint8_t closing;
};

typedef struct sdk_handle_session_fanout {
    vemb_v16_client_handle_session_t *session;
    const vemb_v16_resp_t *response;
    vemb_v16_client_handle_completion_cb cb;
    void *priv;
    uint32_t completed;
} sdk_handle_session_fanout_t;

static vemb_v16_resp_t sdk_handle_session_error_response(void)
{
    return (vemb_v16_resp_t){
        .status = VEMB_V16_STATUS_ERR,
        .op = VEMB_V16_OP_VEMB_HANDLE,
    };
}

static int sdk_handle_session_finish(
    vemb_v16_client_handle_session_t *session, uint32_t request_id,
    const vemb_v16_resp_t *response,
    vemb_v16_client_handle_completion_cb cb, void *priv)
{
    sdk_handle_session_request_t *request = &session->requests[request_id];
    if (request->state == SDK_HANDLE_SESSION_REQUEST_FREE)
        return 0;

    vemb_v16_pipeline_resp_t completion = {
        .status = response->status == VEMB_V16_STATUS_OK ? 0 :
            response->status == VEMB_V16_STATUS_NOT_FOUND ? 1 : -1,
        .offset = response->vector_offset,
        .bytes = response->vector_bytes,
        .dim = response->dim ? response->dim : session->client->dim,
        .region_id = response->region_id,
        .score = response->score,
    };
    uint64_t caller_cookie = request->caller_cookie;
    if (response->status == VEMB_V16_STATUS_OK &&
        response->op == VEMB_V16_OP_VEMB_HANDLE) {
        sdk_backend_t *backend =
            &session->client->owner_channels[request->owner_id];
        session->client->last_handle_channel = backend;
        session->client->last_handle_generation = backend->generation;
        session->client->last_handle_region_id = response->region_id;
    }
    vemb_v16_cluster_core_complete(&session->client->cluster,
                                   &request->operation, response->status);
    *request = (sdk_handle_session_request_t){
        .state = SDK_HANDLE_SESSION_REQUEST_FREE,
        .l0_entry_id = SDK_HANDLE_SESSION_NO_ENTRY,
    };
    session->active_requests--;
    if (cb)
        cb(priv, caller_cookie, &completion);
    return 1;
}

static int sdk_handle_session_apply_response(
    vemb_v16_client_handle_session_t *session, uint32_t request_id,
    const vemb_v16_resp_t *response,
    vemb_v16_client_handle_completion_cb cb, void *priv)
{
    sdk_handle_session_request_t *request = &session->requests[request_id];
    vemb_v16_cluster_response_action_t action =
        vemb_v16_cluster_core_on_response(&session->client->cluster,
                                          &request->operation, response);
    if (action == VEMB_V16_CLUSTER_RESPONSE_FINAL)
        return sdk_handle_session_finish(session, request_id, response, cb,
                                         priv);

    request->state = SDK_HANDLE_SESSION_REQUEST_ROUTE;
    request->l0_entry_id = SDK_HANDLE_SESSION_NO_ENTRY;
    request->l0_generation = 0;
    request->l0_leader = 0;
    request->force_v1 = 1;
    if (action == VEMB_V16_CLUSTER_RESPONSE_REFRESH)
        session->topology_refresh_pending = 1;
    return 0;
}

static void sdk_handle_session_finish_fanout(void *priv,
                                              uint64_t request_id)
{
    sdk_handle_session_fanout_t *fanout = priv;
    assert(request_id < SDK_HANDLE_SESSION_MAX_PENDING);
    fanout->completed += sdk_handle_session_apply_response(
        fanout->session, (uint32_t)request_id, fanout->response, fanout->cb,
        fanout->priv);
}

static void sdk_handle_session_requeue_fanout(void *priv,
                                               uint64_t request_id)
{
    vemb_v16_client_handle_session_t *session = priv;
    assert(request_id < SDK_HANDLE_SESSION_MAX_PENDING);
    sdk_handle_session_request_t *request = &session->requests[request_id];
    assert(request->state != SDK_HANDLE_SESSION_REQUEST_FREE);
    request->state = SDK_HANDLE_SESSION_REQUEST_ROUTE;
    request->l0_entry_id = SDK_HANDLE_SESSION_NO_ENTRY;
    request->l0_generation = 0;
    request->l0_leader = 0;
    /* This group was stopped by topology/migration before v2 publication.
     * Its next core-selected route must use the permanent UB v1 channel. */
    request->force_v1 = 1;
}

static uint32_t sdk_handle_session_find_v1_request(
    const vemb_v16_client_handle_session_t *session, uint64_t operation_id)
{
    for (uint32_t i = 0; i < SDK_HANDLE_SESSION_MAX_PENDING; i++) {
        const sdk_handle_session_request_t *request = &session->requests[i];
        if (request->state == SDK_HANDLE_SESSION_REQUEST_V1 &&
            request->operation.operation_id == operation_id)
            return i;
    }
    return SDK_HANDLE_SESSION_NO_ENTRY;
}

static uint32_t sdk_handle_session_find_l0_leader(
    const vemb_v16_client_handle_session_t *session, uint32_t owner_id,
    uint32_t entry_id)
{
    for (uint32_t i = 0; i < SDK_HANDLE_SESSION_MAX_PENDING; i++) {
        const sdk_handle_session_request_t *request = &session->requests[i];
        if (request->state != SDK_HANDLE_SESSION_REQUEST_FREE &&
            request->owner_id == owner_id && request->l0_leader &&
            request->l0_entry_id == entry_id)
            return i;
    }
    return SDK_HANDLE_SESSION_NO_ENTRY;
}

static uint64_t sdk_handle_session_next_batch_id(sdk_owner_v2_t *v2)
{
    uint64_t batch_id = v2->next_batch_id++;
    if (batch_id == 0)
        batch_id = v2->next_batch_id++;
    return batch_id;
}

static void sdk_handle_session_deadline_after_progress(
    vemb_v16_client_handle_session_t *session, uint32_t owner_id)
{
    sdk_owner_v2_t *v2 = &session->client->owner_v2[owner_id];
    uint32_t pending = v2->l0 ?
        vemb_v16_cli_l0_pending_item_count(v2->l0, 0) : 0;
    vemb_v16_cli_deadline_after_progress(
        &session->deadlines[owner_id], pending,
        session->deadlines[owner_id].delay_ns ? vemb_v16_monotonic_ns() : 0);
}

static int sdk_handle_session_submit_v1(
    vemb_v16_client_handle_session_t *session, uint32_t request_id,
    const vemb_v16_cluster_route_t *route)
{
    sdk_handle_session_request_t *request = &session->requests[request_id];
    sdk_backend_t *backend = &session->client->owner_channels[route->owner_id];
    vemb_v16_req_t wire_request = {
        .op = request->op ? request->op : VEMB_V16_OP_VEMB_HANDLE,
        .flags = route->request_flags,
        .req_id = session->client->req_id++,
        .channel_id = backend->channel_id,
        .key_len = request->key_len,
        .key_hash = request->operation.key_hash,
        .topology_epoch = route->topology_epoch,
        .dim = session->client->dim,
        .vector_bytes = session->client->dim * sizeof(float),
    };
    memcpy(wire_request.key, request->key, request->key_len);
    if (request->key2_len) {
        wire_request.key2_len = request->key2_len;
        memcpy(wire_request.key2, request->key2, request->key2_len);
        wire_request.key2_hash =
            vemb_v16_xxh3_64_str(request->key2, request->key2_len);
    }
    vemb_v16_transport_submission_t submission = {
        .operation_id = request->operation.operation_id,
        .wire_req_id = wire_request.req_id,
        .owner_id = route->owner_id,
        .submit_epoch = route->topology_epoch,
        .request = &wire_request,
    };
    if (sdk_backend_submit(backend, &submission) != 0)
        return -1;
    request->owner_id = route->owner_id;
    request->state = SDK_HANDLE_SESSION_REQUEST_V1;
    return 0;
}

static int sdk_handle_session_submit_l0_group_v1(
    vemb_v16_client_handle_session_t *session, uint32_t owner_id,
    uint32_t entry_id, vemb_v16_client_handle_completion_cb cb, void *priv)
{
    sdk_owner_v2_t *v2 = &session->client->owner_v2[owner_id];
    const char *key;
    uint16_t key_len;
    uint32_t generation;
    uint8_t state;
    assert(v2->l0 != NULL);
    assert(vemb_v16_cli_l0_get_group(v2->l0, entry_id, &key, &key_len,
                                      &generation, &state) == 0);
    assert(state == VEMB_V16_CLI_L0_GROUP_PENDING_SEND);
    uint32_t leader = sdk_handle_session_find_l0_leader(session, owner_id,
                                                         entry_id);
    assert(leader != SDK_HANDLE_SESSION_NO_ENTRY);
    vemb_v16_owner_session_identity_t identity;
    vemb_v16_cli_l0_get_group_identity(v2->l0, entry_id, &identity);
    assert(identity.owner_id == owner_id);
    assert(vemb_v16_cli_l0_mark_fallback_v1(v2->l0, entry_id) == 0);

    vemb_v16_cluster_route_t route = {
        .owner_id = owner_id,
        .topology_epoch = identity.topology_epoch,
        .owner_channel_generation = identity.owner_generation,
    };
    if (sdk_handle_session_submit_v1(session, leader, &route) == 0)
        return 0;

    (void)key;
    (void)key_len;
    vemb_v16_cli_l0_completion_t completion = {
        .entry_id = entry_id,
        .generation = generation,
    };
    vemb_v16_resp_t error = sdk_handle_session_error_response();
    sdk_handle_session_fanout_t fanout = {
        .session = session,
        .response = &error,
        .cb = cb,
        .priv = priv,
    };
    (void)vemb_v16_cli_l0_finish(v2->l0, &completion,
                                  sdk_handle_session_finish_fanout, &fanout);
    return -1;
}

static int sdk_handle_session_flush_owner(
    vemb_v16_client_handle_session_t *session, uint32_t owner_id,
    vemb_v16_client_handle_completion_cb cb, void *priv, int allow_v1_fallback)
{
    vemb_v16_owner_session_t *owner_session =
        &session->client->owner_sessions[owner_id];
    sdk_owner_v2_t *v2 = &session->client->owner_v2[owner_id];
    if (!v2->l0 || !v2->channel ||
        owner_session->v2_state != VEMB_V16_OWNER_SESSION_V2_READY)
        return 0;

    for (;;) {
        vemb_v16_cli_l0_batch_draft_t draft;
        int prepared = vemb_v16_cli_l0_prepare_batch(
            v2->l0, 0, v2->effective_batch_size, v2->max_batch_bytes,
            &draft);
        if (prepared == -2)
            return 0; /* published-frame backpressure; wait for poll() */
        if (prepared < 0)
            return -1;
        if (prepared == 0) {
            if (draft.oversized_entry_id == SDK_HANDLE_SESSION_NO_ENTRY)
                return 0;
            if (!allow_v1_fallback)
                return -1;
            if (sdk_handle_session_submit_l0_group_v1(
                    session, owner_id, draft.oversized_entry_id, cb, priv) != 0)
                return -1;
            sdk_handle_session_deadline_after_progress(session, owner_id);
            continue;
        }

        uint64_t batch_id = sdk_handle_session_next_batch_id(v2);
        int publish_rc = vemb_v16_aeron_batch_publish_handle_at_epoch(
            v2->channel, batch_id, draft.identity.topology_epoch,
            draft.keys, draft.key_lens, draft.item_count);
        if (publish_rc == RING_ERR_FULL)
            return 0;
        if (publish_rc != RING_OK) {
            if (!allow_v1_fallback)
                return -1;
            for (uint32_t i = 0; i < draft.item_count; i++)
                (void)sdk_handle_session_submit_l0_group_v1(
                    session, owner_id, draft.entry_ids[i], cb, priv);
            sdk_handle_session_deadline_after_progress(session, owner_id);
            continue;
        }
        if (batch_id <= 8 || batch_id % 1024 == 0) {
            fprintf(stderr,
                    "[sdk] handle v2 batch submitted owner=%u batch=%llu "
                    "items=%u epoch=%llu channel=%llu\n",
                    owner_id, (unsigned long long)batch_id,
                    draft.item_count,
                    (unsigned long long)draft.identity.topology_epoch,
                    (unsigned long long)vemb_v16_aeron_batch_channel_id(
                        v2->channel));
        }
        assert(vemb_v16_cli_l0_publish_batch(v2->l0, &draft, batch_id) == 0);
        vemb_v16_owner_session_v2_batch_published(owner_session);
        sdk_handle_session_deadline_after_progress(session, owner_id);
    }
}

static uint32_t sdk_handle_session_fail_routes(
    vemb_v16_client_handle_session_t *session,
    vemb_v16_client_handle_completion_cb cb, void *priv)
{
    vemb_v16_resp_t error = sdk_handle_session_error_response();
    uint32_t callbacks = 0;
    for (uint32_t i = 0; i < SDK_HANDLE_SESSION_MAX_PENDING; i++) {
        if (session->requests[i].state == SDK_HANDLE_SESSION_REQUEST_ROUTE)
            callbacks += sdk_handle_session_finish(session, i, &error, cb,
                                                   priv);
    }
    return callbacks;
}

static int sdk_handle_session_refresh_topology(
    vemb_v16_client_handle_session_t *session,
    vemb_v16_client_handle_completion_cb cb, void *priv,
    uint32_t *out_callbacks)
{
    if (!session->topology_refresh_pending &&
        vemb_v16_cluster_core_topology_ready(&session->client->cluster))
        return 0;
    if (fetch_topology_via_bootstrap_seeds(session->client) != 0) {
        *out_callbacks += sdk_handle_session_fail_routes(session, cb, priv);
        return -1;
    }
    session->topology_refresh_pending = 0;
    return 0;
}

static uint32_t sdk_handle_session_route_requests(
    vemb_v16_client_handle_session_t *session,
    vemb_v16_client_handle_completion_cb cb, void *priv)
{
    uint32_t callbacks = 0;
    if (sdk_handle_session_refresh_topology(session, cb, priv,
                                            &callbacks) != 0)
        return callbacks;

    for (uint32_t i = 0; i < SDK_HANDLE_SESSION_MAX_PENDING; i++) {
        sdk_handle_session_request_t *request = &session->requests[i];
        if (request->state != SDK_HANDLE_SESSION_REQUEST_ROUTE)
            continue;

        vemb_v16_cluster_route_t route;
        vemb_v16_cluster_prepare_result_t prepared =
            vemb_v16_cluster_core_prepare(&session->client->cluster,
                                          &request->operation, &route);
        if (prepared == VEMB_V16_CLUSTER_PREPARE_NEEDS_TOPOLOGY) {
            session->topology_refresh_pending = 1;
            return callbacks;
        }
        if (prepared == VEMB_V16_CLUSTER_PREPARE_RETRY_EXHAUSTED) {
            vemb_v16_resp_t error = sdk_handle_session_error_response();
            callbacks += sdk_handle_session_finish(session, i, &error, cb,
                                                   priv);
            continue;
        }
        int channel_rc = ensure_owner_channel(session->client, route.owner_id);
        if (channel_rc != 0) {
            vemb_v16_resp_t error = sdk_handle_session_error_response();
            callbacks += sdk_handle_session_finish(session, i, &error, cb,
                                                   priv);
            continue;
        }
        route.owner_channel_generation =
            session->client->cluster.owner_channels[route.owner_id].generation;

        request->owner_id = route.owner_id;
        if (request->force_v1 || route.request_flags != 0 ||
            !sdk_owner_v2_enable(session->client, route.owner_id)) {
            if (sdk_handle_session_submit_v1(session, i, &route) != 0) {
                vemb_v16_resp_t error = sdk_handle_session_error_response();
                callbacks += sdk_handle_session_finish(session, i, &error, cb,
                                                       priv);
            }
            continue;
        }

        vemb_v16_owner_session_identity_t identity = {
            .owner_id = route.owner_id,
            .topology_epoch = route.topology_epoch,
            .owner_generation = route.owner_channel_generation,
        };
        vemb_v16_owner_session_t *owner_session =
            &session->client->owner_sessions[route.owner_id];
        if (vemb_v16_owner_session_select_submit_path(owner_session,
                                                       &identity) !=
            VEMB_V16_OWNER_SESSION_SUBMIT_V2) {
            if (sdk_handle_session_submit_v1(session, i, &route) != 0) {
                vemb_v16_resp_t error = sdk_handle_session_error_response();
                callbacks += sdk_handle_session_finish(session, i, &error, cb,
                                                       priv);
            }
            continue;
        }

        sdk_owner_v2_t *v2 = &session->client->owner_v2[route.owner_id];
        uint32_t entry_id;
        uint32_t channel_index;
        int submit = vemb_v16_cli_l0_submit_with_identity(
            v2->l0, request->key, request->key_len, request->operation.key_hash,
            i, &identity, &entry_id, &channel_index);
        if (submit == VEMB_V16_CLI_L0_NEW_LEADER ||
            submit == VEMB_V16_CLI_L0_COALESCED_FOLLOWER) {
            const char *ignored_key;
            uint16_t ignored_key_len;
            uint32_t generation;
            uint8_t ignored_state;
            assert(channel_index == 0);
            assert(vemb_v16_cli_l0_get_group(v2->l0, entry_id, &ignored_key,
                                              &ignored_key_len, &generation,
                                              &ignored_state) == 0);
            request->state = SDK_HANDLE_SESSION_REQUEST_L0;
            request->l0_entry_id = entry_id;
            request->l0_generation = generation;
            request->l0_leader = submit == VEMB_V16_CLI_L0_NEW_LEADER;
            if (request->l0_leader)
                vemb_v16_cli_deadline_on_new_leader(
                    &session->deadlines[route.owner_id],
                    session->deadlines[route.owner_id].delay_ns ?
                        vemb_v16_monotonic_ns() : 0);
            continue;
        }

        /* L0 pool pressure preserves operation semantics through UB v1. */
        if (sdk_handle_session_submit_v1(session, i, &route) != 0) {
            vemb_v16_resp_t error = sdk_handle_session_error_response();
            callbacks += sdk_handle_session_finish(session, i, &error, cb,
                                                   priv);
        }
    }
    return callbacks;
}

static void sdk_handle_session_v2_failed(
    vemb_v16_client_handle_session_t *session, uint32_t owner_id,
    vemb_v16_client_handle_completion_cb cb, void *priv)
{
    sdk_owner_v2_t *v2 = &session->client->owner_v2[owner_id];
    fprintf(stderr,
            "[sdk] owner v2 handle path failed owner=%u channel=%llu; "
            "abort batch and request topology refresh\n",
            owner_id,
            v2->channel ?
                (unsigned long long)vemb_v16_aeron_batch_channel_id(v2->channel) :
                0ull);
    vemb_v16_resp_t stale = {
        .status = VEMB_V16_STATUS_STALE_TOPOLOGY,
        .op = VEMB_V16_OP_VEMB_HANDLE,
    };
    sdk_handle_session_fanout_t fanout = {
        .session = session,
        .response = &stale,
        .cb = cb,
        .priv = priv,
    };
    if (v2->l0)
        vemb_v16_cli_l0_abort_all(v2->l0,
                                   sdk_handle_session_finish_fanout, &fanout);
    vemb_v16_owner_session_abort_v2_batches(
        &session->client->owner_sessions[owner_id]);
    if (v2->channel)
        vemb_v16_aeron_batch_close(v2->channel);
    vemb_v16_cli_l0_destroy(v2->l0);
    *v2 = (sdk_owner_v2_t){0};
    session->topology_refresh_pending = 1;
}

static uint32_t sdk_handle_session_poll_v2(
    vemb_v16_client_handle_session_t *session, uint32_t owner_id,
    vemb_v16_client_handle_completion_cb cb, void *priv)
{
    sdk_owner_v2_t *v2 = &session->client->owner_v2[owner_id];
    if (!v2->channel || !v2->l0)
        return 0;

    uint32_t callbacks = 0;
    for (;;) {
        uint64_t batch_id;
        uint64_t response_epoch;
        vemb_v16_resp_t responses[VEMB_V16_BATCH_REQUEST_SIZE_MAX];
        int count = vemb_v16_aeron_batch_poll_response(
            v2->channel, &batch_id, &response_epoch, responses);
        if (count == 0)
            return callbacks;
        if (count < 0) {
            sdk_handle_session_v2_failed(session, owner_id, cb, priv);
            return callbacks;
        }

        vemb_v16_owner_session_identity_t identity;
        uint32_t expected_count = vemb_v16_cli_l0_batch_item_count(
            v2->l0, 0, batch_id);
        if (expected_count == 0 || count != (int)expected_count ||
            vemb_v16_cli_l0_get_batch_identity(v2->l0, 0, batch_id,
                                                &identity) != 0) {
            fprintf(stderr,
                    "[sdk] handle v2 batch identity failure owner=%u "
                    "batch=%llu response_count=%d expected_count=%u "
                    "response_epoch=%llu\n",
                    owner_id, (unsigned long long)batch_id, count,
                    expected_count, (unsigned long long)response_epoch);
            sdk_handle_session_v2_failed(session, owner_id, cb, priv);
            return callbacks;
        }

        int stale_epoch = response_epoch != identity.topology_epoch;
        if (stale_epoch || batch_id <= 8 || batch_id % 1024 == 0) {
            fprintf(stderr,
                    "[sdk] handle v2 batch response owner=%u batch=%llu "
                    "items=%d expected=%u submit_epoch=%llu "
                    "response_epoch=%llu epoch_match=%d\n",
                    owner_id, (unsigned long long)batch_id, count,
                    expected_count,
                    (unsigned long long)identity.topology_epoch,
                    (unsigned long long)response_epoch, !stale_epoch);
        }
        vemb_v16_owner_session_t *owner_session =
            &session->client->owner_sessions[owner_id];
        if (stale_epoch)
            (void)vemb_v16_owner_session_begin_quiesce(owner_session);
        for (uint32_t item = 0; item < expected_count; item++) {
            vemb_v16_cli_l0_completion_t completion;
            if (vemb_v16_cli_l0_resolve_response(v2->l0, 0, batch_id, item,
                                                  &completion) != 1) {
                sdk_handle_session_v2_failed(session, owner_id, cb, priv);
                return callbacks;
            }
            vemb_v16_resp_t response = responses[item];
            if (stale_epoch)
                response.status = VEMB_V16_STATUS_STALE_TOPOLOGY;
            if (response.status == VEMB_V16_STATUS_STALE_TOPOLOGY ||
                response.status == VEMB_V16_STATUS_MOVED)
                (void)vemb_v16_owner_session_begin_quiesce(owner_session);
            sdk_handle_session_fanout_t fanout = {
                .session = session,
                .response = &response,
                .cb = cb,
                .priv = priv,
            };
            if (vemb_v16_cli_l0_finish(v2->l0, &completion,
                                        sdk_handle_session_finish_fanout,
                                        &fanout) == 0)
                callbacks += fanout.completed;
        }
        if (owner_session->v2_state == VEMB_V16_OWNER_SESSION_V2_QUIESCING)
            vemb_v16_owner_session_finish_quiescing(owner_session);
        vemb_v16_owner_session_v2_batch_finished(owner_session);
    }
}

static uint32_t sdk_handle_session_poll_v1(
    vemb_v16_client_handle_session_t *session, uint32_t owner_id,
    vemb_v16_client_handle_completion_cb cb, void *priv)
{
    sdk_backend_t *backend = &session->client->owner_channels[owner_id];
    if (backend->ops != &sdk_ub_data_transport_ops || !sdk_backend_ready(backend))
        return 0;

    uint32_t callbacks = 0;
    for (;;) {
        vemb_v16_transport_completion_t completion;
        vemb_v16_transport_poll_result_t poll_result =
            sdk_backend_poll(backend, 0, &completion);
        if (poll_result != VEMB_V16_TRANSPORT_POLL_COMPLETION)
            return callbacks;
        uint32_t request_id = sdk_handle_session_find_v1_request(
            session, completion.operation_id);
        if (request_id == SDK_HANDLE_SESSION_NO_ENTRY)
            continue;
        sdk_handle_session_request_t *request = &session->requests[request_id];
        if (request->l0_entry_id == SDK_HANDLE_SESSION_NO_ENTRY) {
            callbacks += sdk_handle_session_apply_response(
                session, request_id, &completion.response, cb, priv);
            continue;
        }

        sdk_owner_v2_t *v2 = &session->client->owner_v2[owner_id];
        vemb_v16_cli_l0_completion_t l0_completion = {
            .entry_id = request->l0_entry_id,
            .generation = request->l0_generation,
        };
        sdk_handle_session_fanout_t fanout = {
            .session = session,
            .response = &completion.response,
            .cb = cb,
            .priv = priv,
        };
        if (v2->l0 && vemb_v16_cli_l0_finish(
                v2->l0, &l0_completion, sdk_handle_session_finish_fanout,
                &fanout) == 0)
            callbacks += fanout.completed;
    }
}

static void sdk_handle_session_owner_quiesce(
    vemb_v16_client_t *client, uint32_t owner_id)
{
    vemb_v16_client_handle_session_t *session = client->handle_session;
    sdk_owner_v2_t *v2 = &client->owner_v2[owner_id];
    if (v2->l0)
        vemb_v16_cli_l0_drain_pending(v2->l0,
                                      sdk_handle_session_requeue_fanout,
                                      session);
    vemb_v16_cli_deadline_clear(&session->deadlines[owner_id]);
    vemb_v16_owner_session_t *owner_session =
        &client->owner_sessions[owner_id];
    if (owner_session->v2_state == VEMB_V16_OWNER_SESSION_V2_QUIESCING)
        vemb_v16_owner_session_finish_quiescing(owner_session);
}

vemb_v16_client_handle_session_t *vemb_v16_client_handle_session_create(
    vemb_v16_client_t *client,
    const vemb_v16_client_handle_session_options_t *options)
{
    RETURN_IF(client->vector_read_op != VEMB_V16_OP_VEMB_HANDLE ||
              client->handle_session || client->vector_session, NULL);
    vemb_v16_client_handle_session_t *session = calloc(1, sizeof(*session));
    assert(session != NULL);
    uint64_t delay_ns = options ?
        (uint64_t)options->max_batch_delay_us * 1000u : 0;
    if (delay_ns)
        monotonicInit();
    session->client = client;
    for (uint32_t i = 0; i < SDK_HANDLE_SESSION_MAX_PENDING; i++)
        session->requests[i].l0_entry_id = SDK_HANDLE_SESSION_NO_ENTRY;
    for (uint32_t owner = 0;
         owner < VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS; owner++)
        vemb_v16_cli_deadline_init(&session->deadlines[owner], delay_ns);
    client->handle_session = session;
    return session;
}

int vemb_v16_client_handle_session_submit(
    vemb_v16_client_handle_session_t *session, const char *set_name,
    const char *elem_name, uint64_t caller_cookie)
{
    RETURN_IF(session->closing ||
              session->active_requests == SDK_HANDLE_SESSION_MAX_PENDING,
              -1);
    uint32_t request_id = SDK_HANDLE_SESSION_NO_ENTRY;
    for (uint32_t i = 0; i < SDK_HANDLE_SESSION_MAX_PENDING; i++) {
        if (session->requests[i].state == SDK_HANDLE_SESSION_REQUEST_FREE) {
            request_id = i;
            break;
        }
    }
    assert(request_id != SDK_HANDLE_SESSION_NO_ENTRY);

    char key[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(key, sizeof(key), set_name, elem_name,
                                    &key_len) != 0)
        return -1;
    sdk_handle_session_request_t *request = &session->requests[request_id];
    *request = (sdk_handle_session_request_t){
        .caller_cookie = caller_cookie,
        .key_len = (uint16_t)key_len,
        .op = VEMB_V16_OP_VEMB_HANDLE,
        .state = SDK_HANDLE_SESSION_REQUEST_ROUTE,
        .l0_entry_id = SDK_HANDLE_SESSION_NO_ENTRY,
    };
    memcpy(request->key, key, key_len);
    vemb_v16_cluster_operation_init(
        &session->client->cluster, &request->operation,
        vemb_v16_xxh3_64_str(request->key, request->key_len));
    session->active_requests++;
    return 0;
}

int vemb_v16_client_handle_session_submit_vsim_key_key(
    vemb_v16_client_handle_session_t *session, const char *set_name,
    const char *elem1, const char *elem2, uint64_t caller_cookie)
{
    RETURN_IF(session->closing ||
              session->active_requests == SDK_HANDLE_SESSION_MAX_PENDING,
              -1);
    uint32_t request_id = SDK_HANDLE_SESSION_NO_ENTRY;
    for (uint32_t i = 0; i < SDK_HANDLE_SESSION_MAX_PENDING; i++) {
        if (session->requests[i].state == SDK_HANDLE_SESSION_REQUEST_FREE) {
            request_id = i;
            break;
        }
    }
    assert(request_id != SDK_HANDLE_SESSION_NO_ENTRY);

    char key1[VEMB_V16_MAX_KEY_LEN];
    char key2[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len, key2_len;
    if (vemb_v16_build_combined_key(key1, sizeof(key1), set_name, elem1,
                                    &key_len) != 0 ||
        vemb_v16_build_combined_key(key2, sizeof(key2), set_name, elem2,
                                    &key2_len) != 0)
        return -1;
    sdk_handle_session_request_t *request = &session->requests[request_id];
    *request = (sdk_handle_session_request_t){
        .caller_cookie = caller_cookie,
        .key_len = (uint16_t)key_len,
        .key2_len = (uint16_t)key2_len,
        .op = VEMB_V16_OP_VSIM_KEY_KEY,
        /* key-key 不走 L0/v2 batch (该协议仅承载 handle 读), 强制 v1 */
        .force_v1 = 1,
        .state = SDK_HANDLE_SESSION_REQUEST_ROUTE,
        .l0_entry_id = SDK_HANDLE_SESSION_NO_ENTRY,
    };
    memcpy(request->key, key1, key_len);
    memcpy(request->key2, key2, key2_len);
    vemb_v16_cluster_operation_init(
        &session->client->cluster, &request->operation,
        vemb_v16_xxh3_64_str(request->key, request->key_len));
    session->active_requests++;
    return 0;
}

int vemb_v16_client_handle_session_flush(
    vemb_v16_client_handle_session_t *session)
{
    RETURN_IF(session->closing, -1);
    for (uint32_t owner = 0;
         owner < VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS; owner++) {
        if (sdk_handle_session_flush_owner(session, owner, NULL, NULL, 0) != 0)
            return -1;
    }
    return 0;
}

int vemb_v16_client_handle_session_poll(
    vemb_v16_client_handle_session_t *session,
    vemb_v16_client_handle_completion_cb cb, void *priv)
{
    RETURN_IF(session->closing, -1);
    uint32_t callbacks = 0;
    callbacks += sdk_handle_session_route_requests(session, cb, priv);
    for (uint32_t owner = 0;
         owner < VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS; owner++) {
        sdk_owner_v2_t *v2 = &session->client->owner_v2[owner];
        if (v2->l0 && vemb_v16_cli_deadline_flush_due(
                &session->deadlines[owner],
                vemb_v16_cli_l0_pending_item_count(v2->l0, 0),
                session->deadlines[owner].delay_ns ?
                    vemb_v16_monotonic_ns() : 0))
            (void)sdk_handle_session_flush_owner(session, owner, cb, priv, 1);
        callbacks += sdk_handle_session_poll_v2(session, owner, cb, priv);
        callbacks += sdk_handle_session_poll_v1(session, owner, cb, priv);
    }
    return (int)callbacks;
}

uint64_t vemb_v16_client_handle_session_next_flush_deadline_ns(
    const vemb_v16_client_handle_session_t *session)
{
    RETURN_IF(session->closing, 0);
    uint64_t earliest = 0;
    for (uint32_t owner = 0;
         owner < VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS; owner++) {
        uint64_t deadline = session->deadlines[owner].deadline_ns;
        if (deadline && (!earliest || deadline < earliest))
            earliest = deadline;
    }
    return earliest;
}

void vemb_v16_client_handle_session_close(
    vemb_v16_client_handle_session_t *session,
    vemb_v16_client_handle_completion_cb cb, void *priv)
{
    RETURN_IF(session->closing);
    session->closing = 1;
    vemb_v16_resp_t error = sdk_handle_session_error_response();
    for (uint32_t owner = 0;
         owner < VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS; owner++) {
        sdk_owner_v2_t *v2 = &session->client->owner_v2[owner];
        if (!v2->l0)
            continue;
        sdk_handle_session_fanout_t fanout = {
            .session = session,
            .response = &error,
            .cb = cb,
            .priv = priv,
        };
        vemb_v16_cli_l0_abort_all(v2->l0,
                                   sdk_handle_session_finish_fanout, &fanout);
        vemb_v16_owner_session_abort_v2_batches(
            &session->client->owner_sessions[owner]);
    }
    for (uint32_t i = 0; i < SDK_HANDLE_SESSION_MAX_PENDING; i++) {
        if (session->requests[i].state != SDK_HANDLE_SESSION_REQUEST_FREE)
            (void)sdk_handle_session_finish(session, i, &error, cb, priv);
    }
    session->client->handle_session = NULL;
    free(session);
}

/* ------------------------------------------------------------------ */
/* Common-core logical vector-read session                            */
/* ------------------------------------------------------------------ */

#define SDK_VECTOR_SESSION_MAX_PENDING VEMB_V16_CLI_L0_MAX_ENTRIES
#define SDK_VECTOR_SESSION_NO_ENTRY UINT32_MAX

enum sdk_vector_session_request_state {
    SDK_VECTOR_SESSION_REQUEST_FREE = 0,
    SDK_VECTOR_SESSION_REQUEST_ROUTE,
    SDK_VECTOR_SESSION_REQUEST_L0,
    SDK_VECTOR_SESSION_REQUEST_V1,
    SDK_VECTOR_SESSION_REQUEST_CACHE,
};

typedef struct sdk_vector_session_request {
    vemb_v16_cluster_operation_t operation;
    uint64_t caller_cookie;
    float *vector;
    uint32_t owner_id;
    uint32_t l0_entry_id;
    uint32_t l0_generation;
    uint16_t key_len;
    uint8_t state;
    uint8_t l0_leader;
    char key[VEMB_V16_MAX_KEY_LEN];
} sdk_vector_session_request_t;

struct vemb_v16_client_vector_session {
    vemb_v16_client_t *client;
    vemb_v16_cli_l0_t *l0[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];
    vemb_v16_cli_l1_t *cache;
    sdk_vector_session_request_t requests[SDK_VECTOR_SESSION_MAX_PENDING];
    uint32_t active_requests;
    uint8_t topology_refresh_pending;
    uint8_t closing;
};

typedef struct sdk_vector_session_fanout {
    vemb_v16_client_vector_session_t *session;
    const vemb_v16_resp_t *response;
    const float *vector;
    vemb_v16_client_vector_completion_cb cb;
    void *priv;
    uint32_t completed;
} sdk_vector_session_fanout_t;

static vemb_v16_resp_t sdk_vector_session_error_response(void)
{
    return (vemb_v16_resp_t){
        .status = VEMB_V16_STATUS_ERR,
        .op = VEMB_V16_OP_VEMB_INLINE,
    };
}

static int sdk_vector_session_finish(
    vemb_v16_client_vector_session_t *session, uint32_t request_id,
    const vemb_v16_resp_t *response, const float *vector,
    vemb_v16_client_vector_completion_cb cb, void *priv)
{
    sdk_vector_session_request_t *request = &session->requests[request_id];
    if (request->state == SDK_VECTOR_SESSION_REQUEST_FREE)
        return 0;

    vemb_v16_pipeline_resp_t completion = {
        .status = response->status == VEMB_V16_STATUS_OK ? 0 :
            response->status == VEMB_V16_STATUS_NOT_FOUND ? 1 : -1,
        .offset = response->vector_offset,
        .bytes = response->vector_bytes,
        .dim = response->dim ? response->dim : session->client->dim,
        .region_id = response->region_id,
    };
    uint64_t caller_cookie = request->caller_cookie;
    vemb_v16_cluster_core_complete(&session->client->cluster,
                                   &request->operation, response->status);
    *request = (sdk_vector_session_request_t){
        .state = SDK_VECTOR_SESSION_REQUEST_FREE,
        .l0_entry_id = SDK_VECTOR_SESSION_NO_ENTRY,
    };
    session->active_requests--;
    if (cb)
        cb(priv, caller_cookie, &completion,
           completion.status == 0 ? vector : NULL);
    return 1;
}

static int sdk_vector_session_apply_response(
    vemb_v16_client_vector_session_t *session, uint32_t request_id,
    const vemb_v16_resp_t *response, const float *vector,
    vemb_v16_client_vector_completion_cb cb, void *priv)
{
    sdk_vector_session_request_t *request = &session->requests[request_id];
    vemb_v16_cluster_response_action_t action =
        vemb_v16_cluster_core_on_response(&session->client->cluster,
                                          &request->operation, response);
    if (action == VEMB_V16_CLUSTER_RESPONSE_FINAL)
        return sdk_vector_session_finish(session, request_id, response, vector,
                                         cb, priv);

    request->state = SDK_VECTOR_SESSION_REQUEST_ROUTE;
    request->l0_entry_id = SDK_VECTOR_SESSION_NO_ENTRY;
    request->l0_generation = 0;
    request->l0_leader = 0;
    if (action == VEMB_V16_CLUSTER_RESPONSE_REFRESH)
        session->topology_refresh_pending = 1;
    return 0;
}

static void sdk_vector_session_finish_fanout(void *priv,
                                              uint64_t request_id)
{
    sdk_vector_session_fanout_t *fanout = priv;
    assert(request_id < SDK_VECTOR_SESSION_MAX_PENDING);
    fanout->completed += sdk_vector_session_apply_response(
        fanout->session, (uint32_t)request_id, fanout->response,
        fanout->vector, fanout->cb, fanout->priv);
}

static void sdk_vector_session_requeue_fanout(void *priv,
                                               uint64_t request_id)
{
    vemb_v16_client_vector_session_t *session = priv;
    assert(request_id < SDK_VECTOR_SESSION_MAX_PENDING);
    sdk_vector_session_request_t *request = &session->requests[request_id];
    assert(request->state != SDK_VECTOR_SESSION_REQUEST_FREE);
    request->state = SDK_VECTOR_SESSION_REQUEST_ROUTE;
    request->l0_entry_id = SDK_VECTOR_SESSION_NO_ENTRY;
    request->l0_generation = 0;
    request->l0_leader = 0;
}

static uint32_t sdk_vector_session_find_v1_request(
    const vemb_v16_client_vector_session_t *session, uint64_t operation_id)
{
    for (uint32_t i = 0; i < SDK_VECTOR_SESSION_MAX_PENDING; i++) {
        const sdk_vector_session_request_t *request = &session->requests[i];
        if (request->state == SDK_VECTOR_SESSION_REQUEST_V1 &&
            request->operation.operation_id == operation_id)
            return i;
    }
    return SDK_VECTOR_SESSION_NO_ENTRY;
}

static uint32_t sdk_vector_session_find_l0_leader(
    const vemb_v16_client_vector_session_t *session, uint32_t owner_id,
    uint32_t entry_id)
{
    for (uint32_t i = 0; i < SDK_VECTOR_SESSION_MAX_PENDING; i++) {
        const sdk_vector_session_request_t *request = &session->requests[i];
        if (request->state != SDK_VECTOR_SESSION_REQUEST_FREE &&
            request->owner_id == owner_id && request->l0_leader &&
            request->l0_entry_id == entry_id)
            return i;
    }
    return SDK_VECTOR_SESSION_NO_ENTRY;
}

static int sdk_vector_session_submit_v1(
    vemb_v16_client_vector_session_t *session, uint32_t request_id,
    const vemb_v16_cluster_route_t *route)
{
    sdk_vector_session_request_t *request = &session->requests[request_id];
    sdk_backend_t *backend = &session->client->owner_channels[route->owner_id];
    uint32_t vector_bytes = session->client->dim * sizeof(float);
    float *vector = malloc(vector_bytes);
    assert(vector != NULL);

    vemb_v16_req_t wire_request = {
        .op = session->client->vector_read_op,
        .flags = route->request_flags,
        .req_id = session->client->req_id++,
        .channel_id = backend->channel_id,
        .key_len = request->key_len,
        .key_hash = request->operation.key_hash,
        .topology_epoch = route->topology_epoch,
        .dim = session->client->dim,
        .vector_bytes = vector_bytes,
    };
    memcpy(wire_request.key, request->key, request->key_len);
    vemb_v16_transport_submission_t submission = {
        .operation_id = request->operation.operation_id,
        .wire_req_id = wire_request.req_id,
        .owner_id = route->owner_id,
        .submit_epoch = route->topology_epoch,
        .request = &wire_request,
        .inline_vector = (uint8_t *)vector,
        .inline_vector_cap = vector_bytes,
    };
    if (sdk_backend_submit(backend, &submission) != 0) {
        free(vector);
        return -1;
    }
    request->vector = vector;
    request->owner_id = route->owner_id;
    request->state = SDK_VECTOR_SESSION_REQUEST_V1;
    return 0;
}

static int sdk_vector_session_submit_l0_group_v1(
    vemb_v16_client_vector_session_t *session, uint32_t owner_id,
    uint32_t entry_id, vemb_v16_client_vector_completion_cb cb, void *priv)
{
    vemb_v16_cli_l0_t *l0 = session->l0[owner_id];
    const char *key;
    uint16_t key_len;
    uint32_t generation;
    uint8_t state;
    assert(l0 != NULL);
    assert(vemb_v16_cli_l0_get_group(l0, entry_id, &key, &key_len,
                                      &generation, &state) == 0);
    assert(state == VEMB_V16_CLI_L0_GROUP_PENDING_SEND);
    uint32_t leader = sdk_vector_session_find_l0_leader(session, owner_id,
                                                         entry_id);
    assert(leader != SDK_VECTOR_SESSION_NO_ENTRY);
    vemb_v16_owner_session_identity_t identity;
    vemb_v16_cli_l0_get_group_identity(l0, entry_id, &identity);
    assert(identity.owner_id == owner_id);
    assert(vemb_v16_cli_l0_mark_fallback_v1(l0, entry_id) == 0);

    vemb_v16_cluster_route_t route = {
        .owner_id = owner_id,
        .topology_epoch = identity.topology_epoch,
        .owner_channel_generation = identity.owner_generation,
    };
    if (sdk_vector_session_submit_v1(session, leader, &route) == 0)
        return 0;

    (void)key;
    (void)key_len;
    vemb_v16_cli_l0_completion_t completion = {
        .entry_id = entry_id,
        .generation = generation,
    };
    vemb_v16_resp_t error = sdk_vector_session_error_response();
    sdk_vector_session_fanout_t fanout = {
        .session = session,
        .response = &error,
        .cb = cb,
        .priv = priv,
    };
    (void)vemb_v16_cli_l0_finish(l0, &completion,
                                  sdk_vector_session_finish_fanout, &fanout);
    return -1;
}

static int sdk_vector_session_flush_owner(
    vemb_v16_client_vector_session_t *session, uint32_t owner_id,
    vemb_v16_client_vector_completion_cb cb, void *priv)
{
    vemb_v16_cli_l0_t *l0 = session->l0[owner_id];
    if (!l0)
        return 0;

    for (;;) {
        vemb_v16_cli_l0_batch_draft_t draft;
        int prepared = vemb_v16_cli_l0_prepare_batch(
            l0, 0, 1, UINT32_MAX, &draft);
        if (prepared == 0)
            return 0;
        if (prepared < 0)
            return -1;
        assert(draft.item_count == 1);
        if (sdk_vector_session_submit_l0_group_v1(
                session, owner_id, draft.entry_ids[0], cb, priv) != 0)
            return -1;
    }
}

static uint32_t sdk_vector_session_fail_routes(
    vemb_v16_client_vector_session_t *session,
    vemb_v16_client_vector_completion_cb cb, void *priv)
{
    vemb_v16_resp_t error = sdk_vector_session_error_response();
    uint32_t callbacks = 0;
    for (uint32_t i = 0; i < SDK_VECTOR_SESSION_MAX_PENDING; i++) {
        if (session->requests[i].state == SDK_VECTOR_SESSION_REQUEST_ROUTE)
            callbacks += sdk_vector_session_finish(session, i, &error, NULL,
                                                   cb, priv);
    }
    return callbacks;
}

static int sdk_vector_session_refresh_topology(
    vemb_v16_client_vector_session_t *session,
    vemb_v16_client_vector_completion_cb cb, void *priv,
    uint32_t *out_callbacks)
{
    if (!session->topology_refresh_pending &&
        vemb_v16_cluster_core_topology_ready(&session->client->cluster))
        return 0;
    if (fetch_topology_via_bootstrap_seeds(session->client) != 0) {
        *out_callbacks += sdk_vector_session_fail_routes(session, cb, priv);
        return -1;
    }
    session->topology_refresh_pending = 0;
    return 0;
}

static uint32_t sdk_vector_session_route_requests(
    vemb_v16_client_vector_session_t *session,
    vemb_v16_client_vector_completion_cb cb, void *priv)
{
    uint32_t callbacks = 0;
    if (sdk_vector_session_refresh_topology(session, cb, priv, &callbacks) != 0)
        return callbacks;

    for (uint32_t i = 0; i < SDK_VECTOR_SESSION_MAX_PENDING; i++) {
        sdk_vector_session_request_t *request = &session->requests[i];
        if (request->state != SDK_VECTOR_SESSION_REQUEST_ROUTE)
            continue;

        vemb_v16_cluster_route_t route;
        vemb_v16_cluster_prepare_result_t prepared =
            vemb_v16_cluster_core_prepare(&session->client->cluster,
                                          &request->operation, &route);
        if (prepared == VEMB_V16_CLUSTER_PREPARE_NEEDS_TOPOLOGY) {
            session->topology_refresh_pending = 1;
            return callbacks;
        }
        if (prepared == VEMB_V16_CLUSTER_PREPARE_RETRY_EXHAUSTED) {
            vemb_v16_resp_t error = sdk_vector_session_error_response();
            callbacks += sdk_vector_session_finish(session, i, &error, NULL,
                                                   cb, priv);
            continue;
        }
        if (ensure_owner_channel(session->client, route.owner_id) != 0) {
            vemb_v16_resp_t error = sdk_vector_session_error_response();
            callbacks += sdk_vector_session_finish(session, i, &error, NULL,
                                                   cb, priv);
            continue;
        }

        route.owner_channel_generation =
            session->client->cluster.owner_channels[route.owner_id].generation;
        request->owner_id = route.owner_id;
        /* ASK is a one-shot route supplied by cluster_core. Its redirect flag
         * is wire-visible, so it must not be reconstructed through L0. */
        if (route.request_flags != 0) {
            if (sdk_vector_session_submit_v1(session, i, &route) != 0) {
                vemb_v16_resp_t error = sdk_vector_session_error_response();
                callbacks += sdk_vector_session_finish(session, i, &error,
                                                       NULL, cb, priv);
            }
            continue;
        }
        vemb_v16_cli_l0_t *l0 = session->l0[route.owner_id];
        if (!l0) {
            l0 = vemb_v16_cli_l0_create(1);
            session->l0[route.owner_id] = l0;
        }

        vemb_v16_owner_session_identity_t identity = {
            .owner_id = route.owner_id,
            .topology_epoch = route.topology_epoch,
            .owner_generation = route.owner_channel_generation,
        };
        uint32_t entry_id;
        uint32_t channel_index;
        int submit = vemb_v16_cli_l0_submit_with_identity(
            l0, request->key, request->key_len, request->operation.key_hash,
            i, &identity, &entry_id, &channel_index);
        if (submit == VEMB_V16_CLI_L0_NEW_LEADER ||
            submit == VEMB_V16_CLI_L0_COALESCED_FOLLOWER) {
            const char *ignored_key;
            uint16_t ignored_key_len;
            uint32_t generation;
            uint8_t ignored_state;
            assert(channel_index == 0);
            assert(vemb_v16_cli_l0_get_group(l0, entry_id, &ignored_key,
                                              &ignored_key_len, &generation,
                                              &ignored_state) == 0);
            request->state = SDK_VECTOR_SESSION_REQUEST_L0;
            request->l0_entry_id = entry_id;
            request->l0_generation = generation;
            request->l0_leader = submit == VEMB_V16_CLI_L0_NEW_LEADER;
            continue;
        }

        if (sdk_vector_session_submit_v1(session, i, &route) != 0) {
            vemb_v16_resp_t error = sdk_vector_session_error_response();
            callbacks += sdk_vector_session_finish(session, i, &error, NULL,
                                                   cb, priv);
        }
    }
    return callbacks;
}

static void sdk_vector_session_materialize(
    vemb_v16_client_vector_session_t *session, uint32_t owner_id,
    sdk_vector_session_request_t *request, vemb_v16_resp_t *response,
    uint32_t inline_bytes)
{
    if (response->status != VEMB_V16_STATUS_OK)
        return;

    uint32_t expected_bytes = session->client->dim * sizeof(float);
    if (response->vector_bytes != expected_bytes) {
        *response = sdk_vector_session_error_response();
        return;
    }
    if (response->op != session->client->vector_read_op) {
        *response = sdk_vector_session_error_response();
        return;
    }
    if (session->client->vector_read_op == VEMB_V16_OP_VEMB_INLINE) {
        if (inline_bytes != expected_bytes)
            *response = sdk_vector_session_error_response();
        return;
    }
    if (sdk_backend_read_warm_vector(
            &session->client->owner_channels[owner_id], response->region_id,
            response->vector_offset, response->vector_bytes, request->vector,
            expected_bytes) != 0) {
        *response = sdk_vector_session_error_response();
    }
}

static void sdk_vector_session_cache_put(
    vemb_v16_client_vector_session_t *session,
    const sdk_vector_session_request_t *request,
    const vemb_v16_resp_t *response, const float *vector)
{
    if (session->cache && response->status == VEMB_V16_STATUS_OK)
        (void)vemb_v16_cli_l1_put(
            session->cache, request->key, request->key_len,
            request->operation.key_hash, vector, response->vector_bytes);
}

static uint32_t sdk_vector_session_poll_cache(
    vemb_v16_client_vector_session_t *session,
    vemb_v16_client_vector_completion_cb cb, void *priv)
{
    uint32_t callbacks = 0;
    vemb_v16_resp_t response = {
        .status = VEMB_V16_STATUS_OK,
        .op = VEMB_V16_OP_VEMB_INLINE,
        .dim = session->client->dim,
        .vector_bytes = session->client->dim * sizeof(float),
    };
    for (uint32_t i = 0; i < SDK_VECTOR_SESSION_MAX_PENDING; i++) {
        sdk_vector_session_request_t *request = &session->requests[i];
        if (request->state != SDK_VECTOR_SESSION_REQUEST_CACHE)
            continue;
        float *vector = request->vector;
        request->vector = NULL;
        callbacks += sdk_vector_session_finish(session, i, &response, vector,
                                               cb, priv);
        free(vector);
    }
    return callbacks;
}

static uint32_t sdk_vector_session_poll_v1(
    vemb_v16_client_vector_session_t *session, uint32_t owner_id,
    vemb_v16_client_vector_completion_cb cb, void *priv)
{
    sdk_backend_t *backend = &session->client->owner_channels[owner_id];
    if (!sdk_backend_ready(backend))
        return 0;

    uint32_t callbacks = 0;
    for (;;) {
        vemb_v16_transport_completion_t completion;
        vemb_v16_transport_poll_result_t poll_result =
            sdk_backend_poll(backend, 0, &completion);
        if (poll_result != VEMB_V16_TRANSPORT_POLL_COMPLETION)
            return callbacks;
        uint32_t request_id = sdk_vector_session_find_v1_request(
            session, completion.operation_id);
        if (request_id == SDK_VECTOR_SESSION_NO_ENTRY)
            continue;

        sdk_vector_session_request_t *request = &session->requests[request_id];
        float *vector = request->vector;
        vemb_v16_resp_t response = completion.response;
        sdk_vector_session_materialize(session, owner_id, request, &response,
                                       completion.inline_vector_bytes);
        request->vector = NULL;
        sdk_vector_session_cache_put(session, request, &response, vector);
        if (request->l0_entry_id == SDK_VECTOR_SESSION_NO_ENTRY) {
            callbacks += sdk_vector_session_apply_response(
                session, request_id, &response, vector, cb, priv);
            free(vector);
            continue;
        }

        vemb_v16_cli_l0_t *l0 = session->l0[owner_id];
        vemb_v16_cli_l0_completion_t l0_completion = {
            .entry_id = request->l0_entry_id,
            .generation = request->l0_generation,
        };
        sdk_vector_session_fanout_t fanout = {
            .session = session,
            .response = &response,
            .vector = vector,
            .cb = cb,
            .priv = priv,
        };
        if (l0 && vemb_v16_cli_l0_finish(
                l0, &l0_completion, sdk_vector_session_finish_fanout,
                &fanout) == 0)
            callbacks += fanout.completed;
        free(vector);
    }
}

static void sdk_vector_session_owner_quiesce(
    vemb_v16_client_t *client, uint32_t owner_id)
{
    vemb_v16_client_vector_session_t *session = client->vector_session;
    if (session->l0[owner_id])
        vemb_v16_cli_l0_drain_pending(
            session->l0[owner_id], sdk_vector_session_requeue_fanout, session);
    if (session->cache)
        vemb_v16_cli_l1_clear(session->cache);
}

vemb_v16_client_vector_session_t *vemb_v16_client_vector_session_create(
    vemb_v16_client_t *client)
{
    return vemb_v16_client_vector_session_create_with_options(client, NULL);
}

vemb_v16_client_vector_session_t *
vemb_v16_client_vector_session_create_with_options(
    vemb_v16_client_t *client,
    const vemb_v16_client_vector_session_options_t *options)
{
    RETURN_IF(client->handle_session || client->vector_session, NULL);
    vemb_v16_client_vector_session_t *session = calloc(1, sizeof(*session));
    assert(session != NULL);
    session->client = client;
    for (uint32_t i = 0; i < SDK_VECTOR_SESSION_MAX_PENDING; i++)
        session->requests[i].l0_entry_id = SDK_VECTOR_SESSION_NO_ENTRY;
    if (options && options->cache_mode !=
            VEMB_V16_CLIENT_VECTOR_CACHE_DISABLED) {
        if (options->cache_mode !=
                VEMB_V16_CLIENT_VECTOR_CACHE_IMMUTABLE_SNAPSHOT) {
            free(session);
            return NULL;
        }
        session->cache = vemb_v16_cli_l1_create(
            &(vemb_v16_cli_l1_config_t){
                .dim = client->dim,
                .entry_count = options->cache_entries,
                .key_slot_count = options->cache_entries,
                .vector_slot_count = options->cache_entries,
            });
        if (!session->cache) {
            free(session);
            return NULL;
        }
    }
    client->vector_session = session;
    return session;
}

int vemb_v16_client_vector_session_submit(
    vemb_v16_client_vector_session_t *session, const char *set_name,
    const char *elem_name, uint64_t caller_cookie)
{
    RETURN_IF(session->closing ||
              session->active_requests == SDK_VECTOR_SESSION_MAX_PENDING,
              -1);
    uint32_t request_id = SDK_VECTOR_SESSION_NO_ENTRY;
    for (uint32_t i = 0; i < SDK_VECTOR_SESSION_MAX_PENDING; i++) {
        if (session->requests[i].state == SDK_VECTOR_SESSION_REQUEST_FREE) {
            request_id = i;
            break;
        }
    }
    assert(request_id != SDK_VECTOR_SESSION_NO_ENTRY);

    char key[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(key, sizeof(key), set_name, elem_name,
                                    &key_len) != 0)
        return -1;
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, key_len);
    sdk_vector_session_request_t *request = &session->requests[request_id];
    *request = (sdk_vector_session_request_t){
        .caller_cookie = caller_cookie,
        .key_len = (uint16_t)key_len,
        .state = SDK_VECTOR_SESSION_REQUEST_ROUTE,
        .l0_entry_id = SDK_VECTOR_SESSION_NO_ENTRY,
    };
    memcpy(request->key, key, key_len);
    vemb_v16_cluster_operation_init(
        &session->client->cluster, &request->operation, key_hash);
    if (session->cache) {
        vemb_v16_cli_l1_value_t value;
        if (vemb_v16_cli_l1_lookup(session->cache, request->key,
                                    request->key_len, key_hash, &value) == 1) {
            request->vector = malloc(value.vector_bytes);
            assert(request->vector != NULL);
            memcpy(request->vector, value.vector, value.vector_bytes);
            assert(vemb_v16_cli_l1_release(session->cache, &value.ref) == 0);
            request->state = SDK_VECTOR_SESSION_REQUEST_CACHE;
        }
    }
    session->active_requests++;
    return 0;
}

int vemb_v16_client_vector_session_flush(
    vemb_v16_client_vector_session_t *session)
{
    RETURN_IF(session->closing, -1);
    for (uint32_t owner = 0;
         owner < VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS; owner++) {
        if (sdk_vector_session_flush_owner(session, owner, NULL, NULL) != 0)
            return -1;
    }
    return 0;
}

int vemb_v16_client_vector_session_poll(
    vemb_v16_client_vector_session_t *session,
    vemb_v16_client_vector_completion_cb cb, void *priv)
{
    RETURN_IF(session->closing, -1);
    uint32_t callbacks = 0;
    callbacks += sdk_vector_session_poll_cache(session, cb, priv);
    callbacks += sdk_vector_session_route_requests(session, cb, priv);
    for (uint32_t owner = 0;
         owner < VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS; owner++) {
        (void)sdk_vector_session_flush_owner(session, owner, cb, priv);
        callbacks += sdk_vector_session_poll_v1(session, owner, cb, priv);
    }
    return (int)callbacks;
}

void vemb_v16_client_vector_session_close(
    vemb_v16_client_vector_session_t *session,
    vemb_v16_client_vector_completion_cb cb, void *priv)
{
    RETURN_IF(session->closing);
    session->closing = 1;
    vemb_v16_resp_t error = sdk_vector_session_error_response();
    for (uint32_t i = 0; i < SDK_VECTOR_SESSION_MAX_PENDING; i++) {
        free(session->requests[i].vector);
        session->requests[i].vector = NULL;
    }
    for (uint32_t owner = 0;
         owner < VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS; owner++) {
        if (!session->l0[owner])
            continue;
        sdk_vector_session_fanout_t fanout = {
            .session = session,
            .response = &error,
            .cb = cb,
            .priv = priv,
        };
        vemb_v16_cli_l0_abort_all(session->l0[owner],
                                   sdk_vector_session_finish_fanout, &fanout);
        vemb_v16_cli_l0_destroy(session->l0[owner]);
    }
    for (uint32_t i = 0; i < SDK_VECTOR_SESSION_MAX_PENDING; i++) {
        if (session->requests[i].state != SDK_VECTOR_SESSION_REQUEST_FREE)
            (void)sdk_vector_session_finish(session, i, &error, NULL, cb, priv);
    }
    if (session->cache)
        vemb_v16_cli_l1_destroy(session->cache);
    session->client->vector_session = NULL;
    free(session);
}

/* ------------------------------------------------------------------ */
/* Sync retry engine                                                  */
/* ------------------------------------------------------------------ */

/* Execute a single-key op with transparent redirect/retry. A keyed request
 * always obtains an owner from server topology before it opens a data channel.
 * The owner route uses key_hash via vemb_v16_client_topology_plan_write.
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
        const char *key2, uint32_t key2_len,
        const float *payload, uint32_t dim,
        vemb_v16_resp_t *out_resp,
        uint8_t *out_inline, uint32_t out_inline_cap,
        uint32_t *out_inline_bytes)
{
    RETURN_IF(client->handle_session || client->vector_session, -1);
    memset(out_resp, 0, sizeof(*out_resp));
    if (out_inline_bytes) *out_inline_bytes = 0;

    vemb_v16_cluster_operation_t operation;
    vemb_v16_cluster_operation_init(
        &client->cluster, &operation,
        vemb_v16_xxh3_64_str(key, key_len));

    for (;;) {
        vemb_v16_cluster_route_t route;
        vemb_v16_cluster_prepare_result_t prepare =
            vemb_v16_cluster_core_prepare(&client->cluster, &operation,
                                          &route);
        if (prepare == VEMB_V16_CLUSTER_PREPARE_NEEDS_TOPOLOGY) {
            if (fetch_topology_via_bootstrap_seeds(client) != 0) {
                vemb_v16_cluster_core_complete(&client->cluster, &operation,
                                               VEMB_V16_STATUS_ERR);
                return -1;
            }
            continue;
        }
        if (prepare == VEMB_V16_CLUSTER_PREPARE_RETRY_EXHAUSTED) {
            out_resp->status = VEMB_V16_STATUS_ERR;
            vemb_v16_cluster_core_complete(&client->cluster, &operation,
                                           out_resp->status);
            return 0;
        }

        if (ensure_owner_channel(client, route.owner_id) != 0) {
            vemb_v16_cluster_core_complete(&client->cluster, &operation,
                                           VEMB_V16_STATUS_ERR);
            return -1;
        }
        sdk_backend_t *target = &client->owner_channels[route.owner_id];
        assert(sdk_backend_ready(target));

        /* Build request */
        vemb_v16_req_t req;
        memset(&req, 0, sizeof(req));
        req.op = op_type;
        req.flags = route.request_flags;
        req.req_id = client->req_id++;
        req.channel_id = target->channel_id;
        req.key_len = key_len;
        memcpy(req.key, key, key_len);
        req.key_hash = operation.key_hash;
        req.topology_epoch = route.topology_epoch;
        req.dim = dim;
        if (key2_len > 0) {
            req.key2_len = key2_len;
            memcpy(req.key2, key2, key2_len);
            req.key2_hash = vemb_v16_xxh3_64_str(key2, key2_len);
        }

        switch (op_type) {
        case VEMB_V16_OP_VADD:
        case VEMB_V16_OP_VSIM_INLINE:
            req.vector_bytes = dim * sizeof(float);
            memcpy(req.vector, payload, req.vector_bytes);
            break;
        case VEMB_V16_OP_VSIM_KEY_KEY:
            req.vector_bytes = dim * sizeof(float);
            break;
        case VEMB_V16_OP_VEMB_HANDLE:
        case VEMB_V16_OP_VEMB_INLINE:
            req.vector_bytes = dim * sizeof(float);
            break;
        case VEMB_V16_OP_VREM:
            req.dim = 0;
            req.vector_bytes = 0;
            break;
        default:
            assert(0 && "unsupported SDK operation");
        }

        vemb_v16_transport_submission_t submission = {
            .operation_id = operation.operation_id,
            .wire_req_id = req.req_id,
            .owner_id = route.owner_id,
            .submit_epoch = route.topology_epoch,
            .request = &req,
            .inline_vector = out_inline,
            .inline_vector_cap = out_inline_cap,
        };
        if (sdk_backend_submit(target, &submission) != 0) {
            sdk_owner_channel_close(client, route.owner_id);
            vemb_v16_cluster_core_complete(&client->cluster, &operation,
                                           VEMB_V16_STATUS_ERR);
            return -1;
        }

        vemb_v16_transport_completion_t completion;
        if (sdk_backend_poll(target, VEMB_V16_TRANSPORT_WAIT_FOREVER,
                             &completion) !=
                VEMB_V16_TRANSPORT_POLL_COMPLETION ||
            completion.operation_id != operation.operation_id) {
            sdk_owner_channel_close(client, route.owner_id);
            vemb_v16_cluster_core_complete(&client->cluster, &operation,
                                           VEMB_V16_STATUS_ERR);
            return -1;
        }
        *out_resp = completion.response;
        if (out_inline_bytes)
            *out_inline_bytes = completion.inline_vector_bytes;

        vemb_v16_cluster_response_action_t action =
            vemb_v16_cluster_core_on_response(&client->cluster, &operation,
                                              out_resp);
        if (action == VEMB_V16_CLUSTER_RESPONSE_FINAL) {
            if (out_resp->status == VEMB_V16_STATUS_OK &&
                out_resp->op == VEMB_V16_OP_VEMB_HANDLE) {
                client->last_handle_channel = target;
                client->last_handle_generation = target->generation;
                client->last_handle_region_id = out_resp->region_id;
            }
            vemb_v16_cluster_core_complete(&client->cluster, &operation,
                                           out_resp->status);
            return 0;
        }
        if (action == VEMB_V16_CLUSTER_RESPONSE_ASK_RETRY)
            continue;

        if (fetch_topology_via_bootstrap_seeds(client) != 0) {
            out_resp->status = VEMB_V16_STATUS_ERR;
            vemb_v16_cluster_core_complete(&client->cluster, &operation,
                                           out_resp->status);
            return -1;
        }
    }
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
                               const char *key2, uint32_t key2_len,
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
    req->key_hash = vemb_v16_xxh3_64_str(req->key, key_len);
    req->topology_epoch = topology_epoch;
    req->dim = dim;
    if (key2_len > 0) {
        req->key2_len = key2_len;
        memcpy(req->key2, key2, key2_len);
        req->key2_hash = vemb_v16_xxh3_64_str(key2, key2_len);
    }

    switch (op_type) {
    case VEMB_V16_OP_VADD:
    case VEMB_V16_OP_VSIM_INLINE:
        req->vector_bytes = dim * sizeof(float);
        memcpy(req->vector, payload, req->vector_bytes);
        break;
    case VEMB_V16_OP_VSIM_KEY_KEY:
        req->vector_bytes = dim * sizeof(float);
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
        assert(0 && "unsupported pipeline operation");
    }
}

/* Fill out_entry_aux from a response. */
static void pipeline_fill_aux(vemb_v16_pipe_entry_aux_t *aux,
                              const vemb_v16_resp_t *resp,
                              uint32_t client_dim)
{
    RETURN_IF(aux == NULL);
    aux->offset    = resp->vector_offset;
    aux->bytes     = resp->vector_bytes;
    aux->dim       = resp->dim > 0 ? resp->dim : client_dim;
    aux->region_id = resp->region_id;
    aux->score     = resp->score;
}

/* Returns one when the owner group was published through v2 (including a
 * terminal wire failure), and zero when the unchanged v1 path must execute
 * the group. Preconditions: the caller owns a ready v1 owner channel and
 * group_indices contains only pending entries for that owner. */
static int sdk_owner_v2_execute_handle_batch(
        vemb_v16_client_t *client, uint32_t owner,
        const uint32_t *group_indices, uint32_t group_count,
        const char **keys, const uint32_t *key_lens,
        const uint64_t *topology_epochs, const uint32_t *request_flags,
        vemb_v16_cluster_operation_t *operations,
        uint8_t *out_entry_status, vemb_v16_pipe_entry_aux_t *out_entry_aux,
        int *any_refresh, int *any_retry, uint8_t *v1_fallback)
{
    if (group_count > VEMB_V16_BATCH_REQUEST_SIZE_MAX)
        return 0;

    uint32_t first = group_indices[0];
    uint64_t submit_epoch = topology_epochs[first];
    for (uint32_t i = 0; i < group_count; i++) {
        uint32_t entry = group_indices[i];
        if (topology_epochs[entry] != submit_epoch ||
            request_flags[entry] != 0) {
            return 0;
        }
    }

    if (!sdk_owner_v2_enable(client, owner))
        return 0;

    vemb_v16_owner_session_t *session = &client->owner_sessions[owner];
    vemb_v16_owner_session_identity_t identity = {
        .owner_id = owner,
        .topology_epoch = submit_epoch,
        .owner_generation = client->cluster.owner_channels[owner].generation,
    };
    if (vemb_v16_owner_session_select_submit_path(session, &identity) !=
        VEMB_V16_OWNER_SESSION_SUBMIT_V2) {
        return 0;
    }

    sdk_owner_v2_t *v2 = &client->owner_v2[owner];
    if (group_count > v2->effective_batch_size)
        return 0;

    const char *batch_keys[group_count];
    uint16_t batch_key_lens[group_count];
    for (uint32_t i = 0; i < group_count; i++) {
        uint32_t entry = group_indices[i];
        batch_keys[i] = keys[entry];
        batch_key_lens[i] = (uint16_t)key_lens[entry];
    }

    uint64_t batch_id = v2->next_batch_id++;
    if (batch_id == 0)
        batch_id = v2->next_batch_id++;
    int publish_rc = vemb_v16_aeron_batch_publish_handle_at_epoch(
        v2->channel, batch_id, submit_epoch, batch_keys, batch_key_lens,
        group_count);
    if (publish_rc == RING_ERR_FULL || publish_rc == RING_ERR_INVALID)
        return 0;
    if (publish_rc != RING_OK)
        return 0;
    if (batch_id <= 8 || batch_id % 1024 == 0) {
        fprintf(stderr,
                "[sdk] handle v2 batch submitted owner=%u batch=%llu "
                "items=%u epoch=%llu channel=%llu\n",
                owner, (unsigned long long)batch_id, group_count,
                (unsigned long long)submit_epoch,
                (unsigned long long)vemb_v16_aeron_batch_channel_id(
                    v2->channel));
    }
    vemb_v16_owner_session_v2_batch_published(session);

    vemb_v16_resp_t responses[VEMB_V16_BATCH_REQUEST_SIZE_MAX];
    uint64_t response_batch_id = 0;
    uint64_t response_epoch = 0;
    int response_count;
    do {
        response_count = vemb_v16_aeron_batch_poll_response(
            v2->channel, &response_batch_id, &response_epoch, responses);
        if (response_count == 0)
            usleep(1000);
    } while (response_count == 0);

    if (response_batch_id != batch_id ||
        response_count != (int)group_count) {
        fprintf(stderr,
                "[sdk] handle v2 batch malformed response owner=%u "
                "batch=%llu response_batch=%llu response_count=%d "
                "expected_count=%u submit_epoch=%llu response_epoch=%llu\n",
                owner, (unsigned long long)batch_id,
                (unsigned long long)response_batch_id, response_count,
                group_count, (unsigned long long)submit_epoch,
                (unsigned long long)response_epoch);
        for (uint32_t i = 0; i < group_count; i++)
            out_entry_status[group_indices[i]] = PIPE_ENTRY_ERR;
        (void)vemb_v16_owner_session_begin_quiesce(session);
        vemb_v16_owner_session_finish_quiescing(session);
        vemb_v16_owner_session_v2_batch_finished(session);
        sdk_owner_v2_stop(client, owner);
        return 1;
    }

    int stop_v2 = response_epoch != submit_epoch;
    if (response_epoch != submit_epoch || batch_id <= 8 || batch_id % 1024 == 0) {
        fprintf(stderr,
                "[sdk] handle v2 batch response owner=%u batch=%llu "
                "items=%d submit_epoch=%llu response_epoch=%llu "
                "epoch_match=%d\n",
                owner, (unsigned long long)batch_id, response_count,
                (unsigned long long)submit_epoch,
                (unsigned long long)response_epoch,
                response_epoch == submit_epoch);
    }
    for (uint32_t i = 0; i < group_count; i++) {
        uint32_t entry = group_indices[i];
        vemb_v16_resp_t response = responses[i];
        if (response_epoch != submit_epoch)
            response.status = VEMB_V16_STATUS_STALE_TOPOLOGY;
        vemb_v16_cluster_response_action_t action =
            vemb_v16_cluster_core_on_response(
                &client->cluster, &operations[entry], &response);
        if (action == VEMB_V16_CLUSTER_RESPONSE_FINAL) {
            if (response.status == VEMB_V16_STATUS_OK) {
                out_entry_status[entry] = PIPE_ENTRY_OK;
                pipeline_fill_aux(out_entry_aux ? &out_entry_aux[entry] : NULL,
                                  &response, client->dim);
            } else if (response.status == VEMB_V16_STATUS_NOT_FOUND) {
                out_entry_status[entry] = PIPE_ENTRY_NOT_FOUND;
            } else {
                out_entry_status[entry] = PIPE_ENTRY_ERR;
            }
        } else if (action == VEMB_V16_CLUSTER_RESPONSE_REFRESH) {
            *any_refresh = 1;
            stop_v2 = 1;
        } else {
            *any_retry = 1;
        }
    }

    if (stop_v2) {
        *v1_fallback = 1;
        (void)vemb_v16_owner_session_begin_quiesce(session);
        vemb_v16_owner_session_finish_quiescing(session);
    }
    vemb_v16_owner_session_v2_batch_finished(session);
    return 1;
}

static void pipeline_complete_operations(
        vemb_v16_client_t *client,
        vemb_v16_cluster_operation_t *operations,
        const uint8_t *entry_status,
        uint32_t count)
{
    for (uint32_t i = 0; i < count; i++) {
        uint8_t final_status = VEMB_V16_STATUS_ERR;
        if (entry_status[i] == PIPE_ENTRY_OK)
            final_status = VEMB_V16_STATUS_OK;
        else if (entry_status[i] == PIPE_ENTRY_NOT_FOUND)
            final_status = VEMB_V16_STATUS_NOT_FOUND;
        vemb_v16_cluster_core_complete(&client->cluster, &operations[i],
                                       final_status);
    }
}

static int pipeline_find_operation(const vemb_v16_cluster_operation_t *operations,
                                   uint32_t count,
                                   uint64_t operation_id,
                                   uint32_t *out_index)
{
    for (uint32_t i = 0; i < count; i++) {
        if (operations[i].operation_id == operation_id) {
            *out_index = i;
            return 0;
        }
    }
    return -1;
}

/* Execute a batch of pipeline entries with transparent per-entry redirect/retry.
 *
 * Algorithm:
 * 1. Lazy topology fetch on entry. Loop bounded by client->retry_budget.
 *    Group PENDING entries by active_owner, send each group as a pipelined
 *    burst, recv in send-order, classify each.
 * 2. Per-entry: OK→terminal; NOT_FOUND→terminal; ASK→one-shot resend to
 *    redirect_owner (final); MOVED/STALE→leave PENDING, refresh topology,
 *    retry; ERR→terminal.
 *
 * Returns 0 if the engine completed (caller inspects out_entry_status per
 * entry). Returns -1 only on local I/O catastrophe. On budget exhaustion,
 * remaining PENDING entries become ERR and return 0.
 *
 * Preconditions: public pipeline APIs have validated client/session state and
 * supplied nonempty, count-sized keys, key_lens, and out_entry_status arrays.
 */
static int client_pipeline_execute_with_redirect(
        vemb_v16_client_t *client,
        uint8_t  op_type,
        const char **keys, const uint32_t *key_lens,
        const char **keys2, const uint32_t *key2_lens,
        const float **payloads,        /* vectors for VADD/VSIM, NULL otherwise */
        uint32_t count,
        uint8_t  *out_entry_status,    /* [count], PIPE_ENTRY_* */
        vemb_v16_pipe_entry_aux_t *out_entry_aux,  /* [count], optional */
        uint32_t max_inflight,
        uint8_t  **out_inline_bufs)    /* [count], per-entry inline capture, NULL OK */
{
    if (max_inflight == 0) max_inflight = count;

    vemb_v16_cluster_operation_t operations[count];
    for (uint32_t i = 0; i < count; i++)
        out_entry_status[i] = PIPE_ENTRY_PENDING;
    for (uint32_t i = 0; i < count; i++) {
        vemb_v16_cluster_operation_init(
            &client->cluster, &operations[i],
            vemb_v16_xxh3_64_str(keys[i], key_lens[i]));
    }

    int result = 0;
    uint8_t v2_fallback_owners[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS] = {0};

    if (!vemb_v16_cluster_core_topology_ready(&client->cluster)) {
        if (fetch_topology_via_bootstrap_seeds(client) != 0) {
            for (uint32_t i = 0; i < count; i++)
                out_entry_status[i] = PIPE_ENTRY_ERR;
            result = -1;
            goto finish;
        }
    }

    for (;;) {
        /* Check if all entries are terminal. */
        uint32_t pending_count = 0;
        for (uint32_t i = 0; i < count; i++) {
            if (out_entry_status[i] == PIPE_ENTRY_PENDING)
                pending_count++;
        }
        if (pending_count == 0)
            goto finish;

        /* Compute a topology owner for each pending entry. */
        uint32_t owners[count];
        uint64_t topology_epochs[count];
        uint32_t request_flags[count];

        for (uint32_t i = 0; i < count; i++) {
            if (out_entry_status[i] != PIPE_ENTRY_PENDING)
                continue;
            vemb_v16_cluster_route_t route;
            vemb_v16_cluster_prepare_result_t prepare =
                vemb_v16_cluster_core_prepare(&client->cluster,
                                              &operations[i], &route);
            if (prepare != VEMB_V16_CLUSTER_PREPARE_READY) {
                out_entry_status[i] = PIPE_ENTRY_ERR;
                continue;
            }
            owners[i] = route.owner_id;
            topology_epochs[i] = route.topology_epoch;
            request_flags[i] = route.request_flags;
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
        int any_retry = 0;

        /* Process each distinct owner's group. */
        for (uint32_t oi = 0; oi < distinct_count; oi++) {
            uint32_t owner = distinct_owners[oi];

            if (ensure_owner_channel(client, owner) != 0) {
                /* Can't open channel — mark this owner's entries ERR. */
                for (uint32_t i = 0; i < count; i++) {
                    if (out_entry_status[i] == PIPE_ENTRY_PENDING &&
                        owners[i] == owner) {
                        out_entry_status[i] = PIPE_ENTRY_ERR;
                    }
                }
                continue;
            }
            sdk_backend_t *target = &client->owner_channels[owner];
            assert(sdk_backend_ready(target));

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

            if (op_type == VEMB_V16_OP_VEMB_HANDLE &&
                !v2_fallback_owners[owner] &&
                sdk_owner_v2_execute_handle_batch(
                    client, owner, group_indices, group_count,
                    keys, key_lens, topology_epochs, request_flags,
                    operations, out_entry_status, out_entry_aux,
                    &any_refresh, &any_retry,
                    &v2_fallback_owners[owner])) {
                continue;
            }

            /* A transport completion identifies a logical operation. Do not
             * associate it with pipeline send order: TCP workers may finish
             * requests out of order, and UB rings make that even more visible. */
            uint32_t inflight[count];
            uint32_t inflight_count = 0;
            uint32_t group_cursor = 0;

            while (group_cursor < group_count || inflight_count > 0) {
                while (group_cursor < group_count &&
                       inflight_count < max_inflight) {
                    uint32_t src_idx = group_indices[group_cursor];
                    vemb_v16_req_t req;
                    pipeline_build_req(&req, op_type,
                                       client->req_id++,
                                       target->channel_id,
                                       topology_epochs[src_idx],
                                       client->dim,
                                       keys[src_idx], key_lens[src_idx],
                                       keys2 ? keys2[src_idx] : NULL,
                                       key2_lens ? key2_lens[src_idx] : 0,
                                       payloads ? payloads[src_idx] : NULL,
                                       request_flags[src_idx]);
                    uint8_t *inline_vector = out_inline_bufs ?
                        out_inline_bufs[src_idx] : NULL;
                    vemb_v16_transport_submission_t submission = {
                        .operation_id = operations[src_idx].operation_id,
                        .wire_req_id = req.req_id,
                        .owner_id = owner,
                        .submit_epoch = topology_epochs[src_idx],
                        .request = &req,
                        .inline_vector = inline_vector,
                        .inline_vector_cap = inline_vector ?
                            client->dim * sizeof(float) : 0,
                    };
                    if (sdk_backend_submit(target, &submission) != 0) {
                        for (uint32_t k = group_cursor; k < group_count; k++)
                            out_entry_status[group_indices[k]] = PIPE_ENTRY_ERR;
                        for (uint32_t k = 0; k < inflight_count; k++)
                            out_entry_status[inflight[k]] = PIPE_ENTRY_ERR;
                        sdk_owner_channel_close(client, owner);
                        inflight_count = 0;
                        group_cursor = group_count;
                        break;
                    }
                    inflight[inflight_count++] = src_idx;
                    group_cursor++;
                }

                if (inflight_count == 0)
                    continue;

                vemb_v16_transport_completion_t completion;
                if (sdk_backend_poll(target, VEMB_V16_TRANSPORT_WAIT_FOREVER,
                                     &completion) !=
                    VEMB_V16_TRANSPORT_POLL_COMPLETION) {
                    for (uint32_t k = 0; k < inflight_count; k++)
                        out_entry_status[inflight[k]] = PIPE_ENTRY_ERR;
                    for (uint32_t k = group_cursor; k < group_count; k++)
                        out_entry_status[group_indices[k]] = PIPE_ENTRY_ERR;
                    sdk_owner_channel_close(client, owner);
                    inflight_count = 0;
                    group_cursor = group_count;
                    continue;
                }

                uint32_t src_idx = 0;
                uint32_t inflight_pos = inflight_count;
                if (pipeline_find_operation(operations, count,
                                            completion.operation_id,
                                            &src_idx) != 0) {
                    for (uint32_t k = 0; k < inflight_count; k++)
                        out_entry_status[inflight[k]] = PIPE_ENTRY_ERR;
                    result = -1;
                    break;
                }
                for (uint32_t k = 0; k < inflight_count; k++) {
                    if (inflight[k] == src_idx) {
                        inflight_pos = k;
                        break;
                    }
                }
                if (inflight_pos == inflight_count) {
                    out_entry_status[src_idx] = PIPE_ENTRY_ERR;
                    result = -1;
                    break;
                }
                inflight_count--;
                if (inflight_pos != inflight_count)
                    inflight[inflight_pos] = inflight[inflight_count];

                vemb_v16_resp_t resp = completion.response;
                vemb_v16_cluster_response_action_t action =
                    vemb_v16_cluster_core_on_response(
                        &client->cluster, &operations[src_idx], &resp);

                if (action == VEMB_V16_CLUSTER_RESPONSE_FINAL) {
                    if (resp.status == VEMB_V16_STATUS_OK) {
                        out_entry_status[src_idx] = PIPE_ENTRY_OK;
                        pipeline_fill_aux(
                            out_entry_aux ? &out_entry_aux[src_idx] : NULL,
                            &resp, client->dim);
                    } else if (resp.status == VEMB_V16_STATUS_NOT_FOUND) {
                        out_entry_status[src_idx] = PIPE_ENTRY_NOT_FOUND;
                    } else {
                        out_entry_status[src_idx] = PIPE_ENTRY_ERR;
                    }
                    continue;
                }

                if (action == VEMB_V16_CLUSTER_RESPONSE_REFRESH) {
                    any_refresh = 1;
                    continue;
                }

                vemb_v16_cluster_route_t ask_route;
                if (vemb_v16_cluster_core_prepare(
                        &client->cluster, &operations[src_idx],
                        &ask_route) != VEMB_V16_CLUSTER_PREPARE_READY ||
                    ask_route.owner_id == owner ||
                    ensure_owner_channel(client, ask_route.owner_id) != 0) {
                    out_entry_status[src_idx] = PIPE_ENTRY_ERR;
                    continue;
                }

                sdk_backend_t *ask_target =
                    &client->owner_channels[ask_route.owner_id];
                vemb_v16_req_t ask_req;
                pipeline_build_req(&ask_req, op_type,
                                   client->req_id++, ask_target->channel_id,
                                   ask_route.topology_epoch, client->dim,
                                   keys[src_idx], key_lens[src_idx],
                                   keys2 ? keys2[src_idx] : NULL,
                                   key2_lens ? key2_lens[src_idx] : 0,
                                   payloads ? payloads[src_idx] : NULL,
                                   ask_route.request_flags);
                uint8_t *inline_vector = out_inline_bufs ?
                    out_inline_bufs[src_idx] : NULL;
                vemb_v16_transport_submission_t ask_submission = {
                    .operation_id = operations[src_idx].operation_id,
                    .wire_req_id = ask_req.req_id,
                    .owner_id = ask_route.owner_id,
                    .submit_epoch = ask_route.topology_epoch,
                    .request = &ask_req,
                    .inline_vector = inline_vector,
                    .inline_vector_cap = inline_vector ?
                        client->dim * sizeof(float) : 0,
                };
                vemb_v16_transport_completion_t ask_completion;
                if (sdk_backend_submit(ask_target, &ask_submission) != 0 ||
                    sdk_backend_poll(ask_target,
                                     VEMB_V16_TRANSPORT_WAIT_FOREVER,
                                     &ask_completion) !=
                        VEMB_V16_TRANSPORT_POLL_COMPLETION ||
                    ask_completion.operation_id != operations[src_idx].operation_id) {
                    sdk_owner_channel_close(client, ask_route.owner_id);
                    out_entry_status[src_idx] = PIPE_ENTRY_ERR;
                    continue;
                }

                resp = ask_completion.response;
                action = vemb_v16_cluster_core_on_response(
                    &client->cluster, &operations[src_idx], &resp);
                if (action == VEMB_V16_CLUSTER_RESPONSE_FINAL) {
                    if (resp.status == VEMB_V16_STATUS_OK) {
                        out_entry_status[src_idx] = PIPE_ENTRY_OK;
                        pipeline_fill_aux(
                            out_entry_aux ? &out_entry_aux[src_idx] : NULL,
                            &resp, client->dim);
                    } else if (resp.status == VEMB_V16_STATUS_NOT_FOUND) {
                        out_entry_status[src_idx] = PIPE_ENTRY_NOT_FOUND;
                    } else {
                        out_entry_status[src_idx] = PIPE_ENTRY_ERR;
                    }
                } else if (action == VEMB_V16_CLUSTER_RESPONSE_REFRESH) {
                    any_refresh = 1;
                } else {
                    out_entry_status[src_idx] = PIPE_ENTRY_ERR;
                }
            }
        }

        /* If topology refresh needed, re-fetch and retry. */
        if (any_refresh) {
            if (fetch_topology_via_bootstrap_seeds(client) != 0) {
                /* Can't refresh — remaining PENDING become ERR. */
                for (uint32_t i = 0; i < count; i++) {
                    if (out_entry_status[i] == PIPE_ENTRY_PENDING)
                        out_entry_status[i] = PIPE_ENTRY_ERR;
                }
                goto finish;
            }
            /* Continue outer loop to re-route pending entries. */
            continue;
        }

        /* ASK is retried once with VEMB_V16_REQ_F_ASK_REDIRECT. The next
         * prepare() supplies that flag, which makes the group ineligible for
         * v2 and therefore preserves v1's redirect wire semantics. */
        if (any_retry)
            continue;

        /* No refresh needed — check if anything is still pending. */
        pending_count = 0;
        for (uint32_t i = 0; i < count; i++) {
            if (out_entry_status[i] == PIPE_ENTRY_PENDING)
                pending_count++;
        }
        if (pending_count == 0)
            goto finish;

        /* If nothing was refreshed but entries are still pending, they were
         * not sent (shouldn't happen in normal flow). Mark them ERR. */
        if (!any_refresh) {
            for (uint32_t i = 0; i < count; i++) {
                if (out_entry_status[i] == PIPE_ENTRY_PENDING)
                    out_entry_status[i] = PIPE_ENTRY_ERR;
            }
            goto finish;
        }
    }

finish:
    pipeline_complete_operations(client, operations, out_entry_status, count);
    return result;
}

int vemb_v16_client_vadd(vemb_v16_client_t *c,
                         const char *set_name,
                         const char *elem_name,
                         const float *vector,
                         uint32_t dim)
{
    assert(vector != NULL);
    RETURN_IF(dim != c->dim, -1);

    char combined[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                    set_name, elem_name, &key_len) != 0)
        return -1;

    vemb_v16_resp_t resp;
    if (client_execute_with_redirect(c, VEMB_V16_OP_VADD,
                                     combined, key_len,
                                     NULL, 0,
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
    RETURN_IF(c->vector_read_op != VEMB_V16_OP_VEMB_HANDLE, -1);

    char combined[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                    set_name, elem_name, &key_len) != 0)
        return -1;

    vemb_v16_resp_t resp;
    if (client_execute_with_redirect(c, VEMB_V16_OP_VEMB_HANDLE,
                                     combined, key_len,
                                     NULL, 0,
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
    char combined[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                    set_name, elem_name, &key_len) != 0)
        return -1;

    vemb_v16_resp_t resp;
    if (client_execute_with_redirect(c, VEMB_V16_OP_VREM,
                                     combined, key_len,
                                     NULL, 0,
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
    assert(out_vector != NULL && out_cap > 0);
    char combined[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                    set_name, elem_name, &key_len) != 0)
        return -1;

    vemb_v16_resp_t resp;
    uint32_t inline_bytes = 0;
    uint32_t inline_cap_bytes = out_cap * sizeof(float);
    if (client_execute_with_redirect(
            c, c->vector_read_op, combined, key_len, NULL, 0,
            NULL, c->dim,
            &resp, (uint8_t *)out_vector, inline_cap_bytes,
            &inline_bytes) != 0) {
        return -1;
    }

    if (resp.status == VEMB_V16_STATUS_NOT_FOUND)
        return 1;
    if (resp.status != VEMB_V16_STATUS_OK)
        return -1;

    if (resp.op == VEMB_V16_OP_VEMB_HANDLE) {
        if (vemb_v16_client_read_vector(c, resp.vector_offset,
                                        resp.vector_bytes, out_vector,
                                        out_cap) != 0)
            return -1;
        if (out_dim)
            *out_dim = resp.dim > 0 ? resp.dim : c->dim;
        return 0;
    }
    if (resp.op != VEMB_V16_OP_VEMB_INLINE)
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
    assert(query_vector != NULL && out_score != NULL);
    RETURN_IF(dim != c->dim, -1);

    char combined[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len;
    if (vemb_v16_build_combined_key(combined, sizeof(combined),
                                    set_name, elem_name, &key_len) != 0)
        return -1;

    vemb_v16_resp_t resp;
    if (client_execute_with_redirect(c, VEMB_V16_OP_VSIM_INLINE,
                                     combined, key_len,
                                     NULL, 0,
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

int vemb_v16_client_vsim_key_key(vemb_v16_client_t *c,
                                 const char *set_name,
                                 const char *elem1,
                                 const char *elem2,
                                 uint32_t dim,
                                 float *out_score)
{
    assert(out_score != NULL);
    RETURN_IF(dim != c->dim, -1);

    char combined1[VEMB_V16_MAX_KEY_LEN];
    char combined2[VEMB_V16_MAX_KEY_LEN];
    uint32_t key_len, key2_len;
    if (vemb_v16_build_combined_key(combined1, sizeof(combined1),
                                    set_name, elem1, &key_len) != 0)
        return -1;
    if (vemb_v16_build_combined_key(combined2, sizeof(combined2),
                                    set_name, elem2, &key2_len) != 0)
        return -1;

    vemb_v16_resp_t resp;
    if (client_execute_with_redirect(c, VEMB_V16_OP_VSIM_KEY_KEY,
                                     combined1, key_len,
                                     combined2, key2_len,
                                     NULL, dim,
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
    RETURN_IF(c->handle_session || c->vector_session || count == 0, -1);
    assert(set_names != NULL && elem_names != NULL && vectors != NULL);

    /* Build combined keys + flat arrays for the engine. */
    char combined_keys[count][VEMB_V16_MAX_KEY_LEN];
    uint32_t key_lens[count];
    const char *key_ptrs[count];
    const float *payload_ptrs[count];

    for (uint32_t i = 0; i < count; i++) {
        assert(vectors[i] != NULL);
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
            NULL, NULL,
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
    RETURN_IF(c->vector_read_op != VEMB_V16_OP_VEMB_INLINE ||
              c->handle_session || c->vector_session || count == 0, -1);
    assert(set_names != NULL && elem_names != NULL && out_resps != NULL);

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
    assert(inline_blob != NULL && inline_ptrs != NULL);
    for (uint32_t i = 0; i < count; i++)
        inline_ptrs[i] = inline_blob + (size_t)i * vec_bytes;

    uint8_t status[count];
    vemb_v16_pipe_entry_aux_t aux[count];
    int engine_rc = client_pipeline_execute_with_redirect(
            c, VEMB_V16_OP_VEMB_INLINE,
            key_ptrs, key_lens,
            NULL, NULL,
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

int vemb_v16_client_vemb_handle_pipeline(vemb_v16_client_t *c,
                                         const char **set_names,
                                         const char **elem_names,
                                         uint32_t count,
                                         vemb_v16_pipeline_resp_t *out_resps,
                                         uint32_t max_inflight)
{
    RETURN_IF(c->vector_read_op != VEMB_V16_OP_VEMB_HANDLE ||
              c->handle_session || c->vector_session || count == 0, -1);
    assert(set_names != NULL && elem_names != NULL && out_resps != NULL);

    char combined_keys[count][VEMB_V16_MAX_KEY_LEN];
    uint32_t key_lens[count];
    const char *key_ptrs[count];
    for (uint32_t i = 0; i < count; i++) {
        if (vemb_v16_build_combined_key(combined_keys[i],
                                        VEMB_V16_MAX_KEY_LEN,
                                        set_names[i], elem_names[i],
                                        &key_lens[i]) != 0) {
            return -1;
        }
        key_ptrs[i] = combined_keys[i];
    }

    uint8_t status[count];
    vemb_v16_pipe_entry_aux_t aux[count];
    if (client_pipeline_execute_with_redirect(
            c, VEMB_V16_OP_VEMB_HANDLE, key_ptrs, key_lens, NULL, NULL,
            NULL, count,
            status, aux, max_inflight, NULL) != 0) {
        return -1;
    }

    for (uint32_t i = 0; i < count; i++) {
        vemb_v16_pipeline_resp_t *out = &out_resps[i];
        memset(out, 0, sizeof(*out));
        if (status[i] == PIPE_ENTRY_OK) {
            out->status = 0;
            out->offset = aux[i].offset;
            out->bytes = aux[i].bytes;
            out->dim = aux[i].dim > 0 ? aux[i].dim : c->dim;
            out->region_id = aux[i].region_id;
        } else if (status[i] == PIPE_ENTRY_NOT_FOUND) {
            out->status = 1;
        } else {
            out->status = -1;
        }
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* Diagnostics / Control                                              */
/* ------------------------------------------------------------------ */

int vemb_v16_client_ping(vemb_v16_client_t *c)
{
    vemb_v16_cluster_operation_t operation;
    vemb_v16_cluster_operation_init(&c->cluster, &operation, 0);

    for (;;) {
        vemb_v16_cluster_route_t route;
        vemb_v16_cluster_prepare_result_t prepare =
            vemb_v16_cluster_core_prepare(&c->cluster, &operation, &route);
        if (prepare == VEMB_V16_CLUSTER_PREPARE_NEEDS_TOPOLOGY) {
            if (fetch_topology_via_bootstrap_seeds(c) != 0) {
                vemb_v16_cluster_core_complete(&c->cluster, &operation,
                                               VEMB_V16_STATUS_ERR);
                return -1;
            }
            continue;
        }
        if (prepare == VEMB_V16_CLUSTER_PREPARE_RETRY_EXHAUSTED) {
            vemb_v16_cluster_core_complete(&c->cluster, &operation,
                                           VEMB_V16_STATUS_ERR);
            return -1;
        }
        if (ensure_owner_channel(c, route.owner_id) != 0) {
            vemb_v16_cluster_core_complete(&c->cluster, &operation,
                                           VEMB_V16_STATUS_ERR);
            return -1;
        }

        sdk_backend_t *target = &c->owner_channels[route.owner_id];
        vemb_v16_req_t req = {
            .op = VEMB_V16_OP_PING,
            .flags = route.request_flags,
            .req_id = c->req_id++,
            .channel_id = target->channel_id,
            .key_hash = operation.key_hash,
            .topology_epoch = route.topology_epoch,
        };
        vemb_v16_transport_submission_t submission = {
            .operation_id = operation.operation_id,
            .wire_req_id = req.req_id,
            .owner_id = route.owner_id,
            .submit_epoch = route.topology_epoch,
            .request = &req,
        };
        if (sdk_backend_submit(target, &submission) != 0) {
            sdk_owner_channel_close(c, route.owner_id);
            vemb_v16_cluster_core_complete(&c->cluster, &operation,
                                           VEMB_V16_STATUS_ERR);
            return -1;
        }

        vemb_v16_transport_completion_t completion;
        if (sdk_backend_poll(target, VEMB_V16_TRANSPORT_WAIT_FOREVER,
                             &completion) != VEMB_V16_TRANSPORT_POLL_COMPLETION ||
            completion.operation_id != operation.operation_id) {
            sdk_owner_channel_close(c, route.owner_id);
            vemb_v16_cluster_core_complete(&c->cluster, &operation,
                                           VEMB_V16_STATUS_ERR);
            return -1;
        }

        vemb_v16_cluster_response_action_t action =
            vemb_v16_cluster_core_on_response(&c->cluster, &operation,
                                              &completion.response);
        if (action == VEMB_V16_CLUSTER_RESPONSE_FINAL) {
            vemb_v16_cluster_core_complete(&c->cluster, &operation,
                                           completion.response.status);
            return completion.response.status == VEMB_V16_STATUS_OK ? 0 : -1;
        }
        if (action == VEMB_V16_CLUSTER_RESPONSE_ASK_RETRY)
            continue;
        if (fetch_topology_via_bootstrap_seeds(c) != 0) {
            vemb_v16_cluster_core_complete(&c->cluster, &operation,
                                           VEMB_V16_STATUS_ERR);
            return -1;
        }
    }
}

int vemb_v16_client_stats(vemb_v16_client_t *c, vemb_v16_stats_t *out_stats)
{
    assert(out_stats != NULL);
    uint32_t start = c->next_topology_seed % c->bootstrap_seed_count;
    uint32_t timeout_ms = c->connect_timeout_ms ? c->connect_timeout_ms : 5000;
    for (uint32_t attempt = 0; attempt < c->bootstrap_seed_count; attempt++) {
        uint32_t index = (start + attempt) % c->bootstrap_seed_count;
        const sdk_bootstrap_seed_t *seed = &c->bootstrap_seeds[index];
        int fd = vemb_v16_net_connect(seed->host, seed->port, timeout_ms);
        if (fd < 0)
            continue;

        vemb_v16_net_hdr_t hdr = {
            .magic = VEMB_V16_MAGIC,
            .version = VEMB_V16_VERSION,
            .type = VEMB_V16_NET_STATS,
        };
        vemb_v16_net_hdr_t response;
        int succeeded = vemb_v16_net_write_full(fd, &hdr, sizeof(hdr)) == 0 &&
            vemb_v16_net_read_header(fd, &response) == 0 &&
            response.type == VEMB_V16_NET_STATS &&
            response.payload_len == sizeof(*out_stats) &&
            vemb_v16_net_read_full(fd, out_stats, sizeof(*out_stats)) == 0;
        close(fd);
        if (succeeded) {
            c->next_topology_seed =
                (index + 1) % c->bootstrap_seed_count;
            return 0;
        }
    }
    return -1;
}

/* ------------------------------------------------------------------ */
/* Advanced topology / observability APIs                              */
/* ------------------------------------------------------------------ */

int vemb_v16_client_topology_refresh(vemb_v16_client_t *client)
{
    return fetch_topology_via_bootstrap_seeds(client);
}

void vemb_v16_client_set_retry_budget(vemb_v16_client_t *client,
                                       uint32_t max_attempts)
{
    vemb_v16_cluster_core_set_retry_budget(&client->cluster, max_attempts);
}

void vemb_v16_client_get_redirect_stats(const vemb_v16_client_t *client,
                                         vemb_v16_redirect_stats_t *out)
{
    assert(out != NULL);
    const vemb_v16_cluster_stats_t *stats =
        vemb_v16_cluster_core_stats(&client->cluster);
    out->ask_redirects            = stats->ask_redirects;
    out->moved_redirects          = stats->moved_redirects;
    out->stale_topology_responses = stats->stale_topology_responses;
    out->topology_refresh_calls   = stats->topology_refresh_calls;
}

void vemb_v16_client_get_logical_stats(const vemb_v16_client_t *client,
                                       vemb_v16_logical_stats_t *out)
{
    assert(out != NULL);
    const vemb_v16_cluster_stats_t *stats =
        vemb_v16_cluster_core_stats(&client->cluster);
    *out = (vemb_v16_logical_stats_t){
        .successes = stats->logical_successes,
        .not_found = stats->logical_not_found,
        .errors = stats->logical_errors,
    };
}

void vemb_v16_client_get_fanout_stats(const vemb_v16_client_t *client,
                                      vemb_v16_fanout_stats_t *out)
{
    assert(out != NULL);
    *out = (vemb_v16_fanout_stats_t){0};
    for (uint32_t owner = 0;
         owner < VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS; owner++) {
        const vemb_v16_cli_l0_t *l0 = NULL;
        if (client->handle_session)
            l0 = client->owner_v2[owner].l0;
        else if (client->vector_session)
            l0 = client->vector_session->l0[owner];
        if (l0) {
            vemb_v16_cli_l0_stats_t stats;
            vemb_v16_cli_l0_get_stats(l0, &stats);
            out->leaders += stats.new_leader_groups;
            out->followers += stats.coalesced_followers;
        }
    }
}

int vemb_v16_client_read_vector(vemb_v16_client_t *c,
                                uint64_t offset,
                                uint32_t bytes,
                                float *out_vector,
                                uint32_t out_cap)
{
    assert(out_vector != NULL && out_cap > 0);
    RETURN_IF(c->vector_read_op != VEMB_V16_OP_VEMB_HANDLE || bytes == 0 ||
              out_cap > UINT32_MAX / sizeof(*out_vector), -1);
    sdk_backend_t *channel = c->last_handle_channel;
    RETURN_IF(channel == NULL || !sdk_backend_ready(channel) ||
              channel->generation != c->last_handle_generation ||
              channel->ops != &sdk_ub_data_transport_ops, -1);
    return sdk_backend_read_warm_vector(
        channel, c->last_handle_region_id, offset, bytes, out_vector,
        out_cap * sizeof(*out_vector));
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
    RETURN_IF(c->handle_session || c->vector_session || count == 0, -1);
    assert(set_names != NULL && elem_names != NULL && query_vector != NULL &&
           out_scores != NULL);

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
            NULL, NULL,
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

int vemb_v16_client_vsim_key_key_pipeline(vemb_v16_client_t *c,
                                          const char **set_names,
                                          const char **elem1_names,
                                          const char **elem2_names,
                                          uint32_t count,
                                          float *out_scores,
                                          uint32_t max_inflight)
{
    RETURN_IF(c->handle_session || c->vector_session || count == 0, -1);
    assert(set_names != NULL && elem1_names != NULL && elem2_names != NULL &&
           out_scores != NULL);

    char combined1[count][VEMB_V16_MAX_KEY_LEN];
    char combined2[count][VEMB_V16_MAX_KEY_LEN];
    uint32_t key_lens[count], key2_lens[count];
    const char *key_ptrs[count];
    const char *key2_ptrs[count];

    for (uint32_t i = 0; i < count; i++) {
        if (vemb_v16_build_combined_key(combined1[i], VEMB_V16_MAX_KEY_LEN,
                                        set_names[i], elem1_names[i],
                                        &key_lens[i]) != 0)
            return -1;
        if (vemb_v16_build_combined_key(combined2[i], VEMB_V16_MAX_KEY_LEN,
                                        set_names[i], elem2_names[i],
                                        &key2_lens[i]) != 0)
            return -1;
        key_ptrs[i] = combined1[i];
        key2_ptrs[i] = combined2[i];
    }

    uint8_t status[count];
    vemb_v16_pipe_entry_aux_t aux[count];
    if (client_pipeline_execute_with_redirect(
            c, VEMB_V16_OP_VSIM_KEY_KEY,
            key_ptrs, key_lens,
            key2_ptrs, key2_lens,
            NULL, count,
            status, aux, max_inflight, NULL) != 0)
        return -1;

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
    assert(str != NULL && expected_dim > 0);
    float *vec = malloc(expected_dim * sizeof(float));
    assert(vec != NULL);

    char *copy = strdup(str);
    assert(copy != NULL);

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

    if (unlikely(parsed != expected_dim)) {
        free(vec);
        return NULL;
    }
    return vec;
}

float *vemb_v16_parse_vector_argv(char **argv, int argc, int start_idx,
                                   uint32_t expected_dim, int *out_consumed)
{
    assert(argv != NULL && expected_dim > 0);
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
    assert(vec != NULL);
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
    RETURN_IF(repeat == 0, -1);

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
    RETURN_IF(repeat == 0, -1);

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
        int rc = c->vector_read_op == VEMB_V16_OP_VEMB_HANDLE ?
            vemb_v16_client_vemb_handle_pipeline(
                c, batch_sets, batch_elems, n, batch_resps, max_inflight) :
            vemb_v16_client_vemb_pipeline(
                c, batch_sets, batch_elems, n, NULL, batch_resps, max_inflight);
        if (rc != 0) {
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
    RETURN_IF(repeat == 0, -1);

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

#define VEMB_V16_AERON_CONTROL_TIMEOUT_MS 5000u

struct vemb_v16_aeron_channel {
    vemb_v16_channel_desc_t    desc;
    uint64_t                    resource_generation;
    vemb_v16_client_ring_t    *req_ring;
    void                      *req_ring_mapping;
    size_t                     req_ring_mapping_bytes;
    vemb_v16_client_ring_t    *resp_ring;
    void                      *resp_ring_mapping;
    size_t                     resp_ring_mapping_bytes;
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
    struct {
        uint32_t map_flags;
        uint32_t cache_policy;
        uint8_t  local_path;
    } warm_peer_views[VEMB_V16_MAX_DESC_WARM_REGIONS];
    uint32_t                   warm_count;
    int                        remote;
};

struct vemb_v16_aeron_batch_channel {
    uint64_t                   channel_id;
    uint64_t                   topology_epoch;
    uint32_t                   effective_batch_size;
    uint32_t                   max_batch_bytes;
    uint32_t                   descriptor_slot_size;
    uint32_t                   descriptor_ring_slots;
    vemb_v16_client_ring_t    *request_descriptor_ring;
    void                      *request_descriptor_mapping;
    size_t                     request_descriptor_mapping_bytes;
    vemb_v16_client_ring_t    *response_descriptor_ring;
    void                      *response_descriptor_mapping;
    size_t                     response_descriptor_mapping_bytes;
    void                      *request_arena_mapping;
    size_t                     request_arena_mapping_bytes;
    uint8_t                   *request_arena;
    void                      *response_arena_mapping;
    size_t                     response_arena_mapping_bytes;
    uint8_t                   *response_arena;
    batch_arena_producer_t request_arena_producer;
    char                       control_endpoint[256];
};

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

static int vemb_v16_aeron_control_connect(const char *endpoint) {
    char host[128];
    uint16_t port = 0;
    if (vemb_v16_aeron_parse_tcp_endpoint(endpoint, host, sizeof(host), &port) != 0)
        return -1;
    return vemb_v16_net_connect(host, port, VEMB_V16_AERON_CONTROL_TIMEOUT_MS);
}

/* TCP attach connection with the Aeron control timeout. */
static int vemb_v16_aeron_tcp_connect(const char *host, uint16_t port) {
    if (!host || !host[0]) return -1;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    vemb_v16_net_set_timeouts(fd, VEMB_V16_AERON_CONTROL_TIMEOUT_MS);

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

/* Open a same-host UB-backed ring encoded as <device>@off<bytes> + slot
 * size. Both rings share the local data plane and use ordinary O_RDWR;
 * remote peer-view mappings select their cache policy separately. */
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
    size_t bytes = vemb_v16_client_ring_bytes(slot_size);
    size_t map_bytes = 0, offset_delta = 0;
    void *base = vemb_v16_mmap_shmdev_region(
        path, VEMB_V16_REGION_UB, 0, 1, mmap_offset, bytes,
        &map_bytes, &offset_delta);
    if (!base) return -1;
    *out = (vemb_v16_client_ring_t *)((uint8_t *)base + offset_delta);
    return 0;
}

static void vemb_v16_aeron_ring_close(vemb_v16_client_ring_t *r,
                                      uint32_t slot_size) {
    if (!r || slot_size == 0) return;
    munmap(r, vemb_v16_client_ring_bytes(slot_size));
}

/* Map a shared region with explicitly selected process access. The UB driver
 * requires a read/write mapping for a consumer-only warm-region view and
 * rejects a later mprotect downgrade. The client read APIs expose that view as
 * const and never write it; rings and batch arenas remain read/write.
 * UB cacheability is selected by the direction owner, not by whether its local
 * device path was translated. */
static void *vemb_v16_mmap_shmdev_region(const char *path,
                                          uint32_t backend_type,
                                          int use_sync,
                                          int writable,
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

    int map_writable = writable || backend_type == VEMB_V16_REGION_UB;
    int flags = map_writable ? O_RDWR : O_RDONLY;
    int used_sync = 0;
    int fd = backend_type == VEMB_V16_REGION_LOCAL_SHM ?
        shm_open(path, flags, 0666) : open(path, flags);
    (void)use_sync;
    if (fd < 0 && backend_type == VEMB_V16_REGION_UB &&
        (errno == EPERM || errno == EACCES)) {
        fd = open(path, O_RDWR | O_SYNC);
        if (fd >= 0)
            used_sync = 1;
    }
    if (fd < 0) return NULL;
    int protection = map_writable ? (PROT_READ | PROT_WRITE) : PROT_READ;
    void *ptr = mmap(NULL, map_size, protection, MAP_SHARED,
                     fd, (off_t)aligned_offset);
    if (ptr == MAP_FAILED && backend_type == VEMB_V16_REGION_UB &&
        (errno == EPERM || errno == EACCES)) {
        close(fd);
        fd = open(path, O_RDWR | O_SYNC);
        if (fd >= 0) {
            used_sync = 1;
            ptr = mmap(NULL, map_size, protection, MAP_SHARED,
                       fd, (off_t)aligned_offset);
        }
    }
    close(fd);
    if (ptr == MAP_FAILED) ptr = NULL;
    (void)used_sync;
    if (!ptr) return NULL;
    if (out_map_bytes)    *out_map_bytes    = map_size;
    if (out_offset_delta) *out_offset_delta = offset_delta;
    return ptr;
}

static int vemb_v16_aeron_map_ub_resource(
    const vemb_v16_ub_peer_view_mapping_t *peer_view, uint64_t offset,
    size_t bytes, int writable,
    void **out_mapping, size_t *out_mapping_bytes, void **out_resource) {
    if (!peer_view || !peer_view->client_path[0] || bytes == 0 || !out_mapping ||
        !out_mapping_bytes || !out_resource)
        return -1;

    int map_from_start = peer_view->map_flags &
        VEMB_V16_UB_PEER_VIEW_MAP_F_FROM_START;
    uint64_t mapping_offset = map_from_start ? 0 : offset;
    size_t mapping_bytes = bytes;
    if (map_from_start) {
        if (offset > SIZE_MAX || bytes > SIZE_MAX - (size_t)offset)
            return -1;
        mapping_bytes += (size_t)offset;
    }

    size_t offset_delta = 0;
    void *mapping = vemb_v16_mmap_shmdev_region(
        peer_view->client_path, VEMB_V16_REGION_UB,
        0,
        writable, mapping_offset,
        mapping_bytes,
        out_mapping_bytes, &offset_delta);
    if (!mapping)
        return -1;

    *out_mapping = mapping;
    *out_resource = (uint8_t *)mapping + offset_delta +
        (map_from_start ? (size_t)offset : 0);
    return 0;
}

static int vemb_v16_aeron_ring_header_valid(
    const vemb_v16_client_ring_t *ring, uint32_t slot_size) {
    return ring && ring->slot_size ==
        align_up_size(slot_size, CACHELINE_SIZE) &&
        ring->slot_count == VEMB_V16_CLIENT_RING_SIZE &&
        ring->slot_mask == VEMB_V16_CLIENT_RING_MASK &&
        ring->slots_off == align_up_size(
            sizeof(*ring), CACHELINE_SIZE);
}

static int vemb_v16_aeron_batch_resource_valid(
    const vemb_v16_aeron_batch_resource_desc_t *resource) {
    return resource && resource->backend_type == VEMB_V16_REGION_UB &&
        resource->path_len > 0 &&
        resource->path_len <= VEMB_V16_AERON_SHMDEV_PATH_MAX &&
        resource->path[0] != '\0' &&
        resource->path[resource->path_len - 1] == '\0' &&
        resource->bytes > 0 && resource->bytes <= SIZE_MAX &&
        resource->mmap_offset <= INT64_MAX;
}

static void vemb_v16_aeron_batch_unmap(
    vemb_v16_aeron_batch_channel_t *ch) {
    if (ch->request_descriptor_mapping)
        munmap(ch->request_descriptor_mapping,
               ch->request_descriptor_mapping_bytes);
    if (ch->response_descriptor_mapping)
        munmap(ch->response_descriptor_mapping,
               ch->response_descriptor_mapping_bytes);
    if (ch->request_arena_mapping)
        munmap(ch->request_arena_mapping, ch->request_arena_mapping_bytes);
    if (ch->response_arena_mapping)
        munmap(ch->response_arena_mapping, ch->response_arena_mapping_bytes);
    ch->request_descriptor_ring = NULL;
    ch->response_descriptor_ring = NULL;
    ch->request_descriptor_mapping = NULL;
    ch->response_descriptor_mapping = NULL;
    ch->request_arena_mapping = NULL;
    ch->response_arena_mapping = NULL;
}

/* Best-effort server-side close notification. Errors are swallowed
 * because the rings are already unmapped locally by the caller. */
static void vemb_v16_aeron_notify_close(const char *endpoint,
                                        uint64_t channel_id) {
    if (!endpoint || !endpoint[0]) return;
    int fd = vemb_v16_aeron_control_connect(endpoint);
    if (fd < 0) return;
    vemb_v16_aeron_tcp_status_control(fd,
                                      VEMB_V16_NET_CLOSE_CHANNEL,
                                      channel_id,
                                      NULL);
    close(fd);
}

static vemb_v16_transport_resource_state_t
vemb_v16_aeron_check_resource(const vemb_v16_aeron_channel_t *ch)
{
    if (!ch->control_endpoint[0] || ch->resource_generation == 0)
        return VEMB_V16_TRANSPORT_RESOURCE_FAILED;
    int fd = vemb_v16_aeron_control_connect(ch->control_endpoint);
    if (fd < 0)
        return VEMB_V16_TRANSPORT_RESOURCE_FAILED;

    vemb_v16_transport_resource_state_t result =
        VEMB_V16_TRANSPORT_RESOURCE_FAILED;
    if (vemb_v16_net_write_frame(fd, VEMB_V16_NET_AERON_CHANNEL_STATUS, 0,
                                 ch->desc.channel_id, 0, NULL, 0) != 0)
        goto out;

    vemb_v16_net_hdr_t hdr;
    uint8_t payload[VEMB_V16_NET_STATUS_ENCODED_LEN];
    vemb_v16_net_status_t status;
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_CONTROL_STATUS ||
        hdr.payload_len != sizeof(payload) ||
        vemb_v16_net_read_full(fd, payload, sizeof(payload)) != 0 ||
        vemb_v16_net_status_decode(&status, payload, sizeof(payload)) != 0)
        goto out;

    if (status.status != VEMB_V16_STATUS_OK || status.value == 0 ||
        status.value != ch->resource_generation) {
        result = VEMB_V16_TRANSPORT_RESOURCE_REATTACH;
        goto out;
    }
    result = VEMB_V16_TRANSPORT_RESOURCE_CURRENT;
out:
    close(fd);
    return result;
}

vemb_v16_aeron_batch_channel_t *
vemb_v16_aeron_open_remote_batch_with_peer_view(
    const char *host, uint16_t port, uint32_t dim,
    uint32_t requested_batch_size, uint32_t requested_max_batch_bytes,
    const vemb_v16_ub_peer_view_manifest_t *peer_view_manifest,
    const char *client_host, uint32_t owner_id) {
    if (!host || !host[0] || port == 0 || dim == 0 ||
        requested_batch_size == 0 || !peer_view_manifest ||
        !client_host || !client_host[0])
        return NULL;

    int fd = vemb_v16_aeron_tcp_connect(host, port);
    if (fd < 0)
        return NULL;

    vemb_v16_aeron_attach_v2_req_t req;
    memset(&req, 0, sizeof(req));
    memcpy(req.magic, VEMB_V16_AERON_ATTACH_V2_MAGIC,
           VEMB_V16_AERON_ATTACH_V2_MAGIC_LEN);
    req.dim = dim;
    req.flags = VEMB_V16_AERON_ATTACH_F_REMOTE_PATH;
    req.requested_batch_size = requested_batch_size;
    req.max_batch_bytes = requested_max_batch_bytes;

    vemb_v16_aeron_attach_v2_resp_t resp;
    memset(&resp, 0, sizeof(resp));
    int exchange_ok = vemb_v16_net_write_full(fd, &req, sizeof(req)) == 0 &&
        vemb_v16_net_read_full(fd, &resp, sizeof(resp)) == 0 &&
        memcmp(resp.magic, VEMB_V16_AERON_ATTACHED_V2_MAGIC,
               VEMB_V16_AERON_ATTACHED_V2_MAGIC_LEN) == 0 &&
        resp.status == 0;
    close(fd);
    if (!exchange_ok)
        return NULL;

    char endpoint[256];
    snprintf(endpoint, sizeof(endpoint), "tcp://%s:%u", host, (unsigned)port);
    if (resp.channel_id == 0 ||
        resp.effective_batch_size == 0 ||
        resp.effective_batch_size > VEMB_V16_BATCH_REQUEST_SIZE_MAX ||
        resp.max_batch_bytes == 0 ||
        resp.max_batch_bytes > VEMB_V16_BATCH_MAX_BYTES_MAX ||
        resp.max_batch_bytes % CACHELINE_SIZE != 0 ||
        resp.descriptor_slot_size != VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE ||
        resp.descriptor_ring_slots != VEMB_V16_CLIENT_RING_SIZE ||
        !vemb_v16_aeron_batch_resource_valid(&resp.request_descriptor) ||
        !vemb_v16_aeron_batch_resource_valid(&resp.request_arena) ||
        !vemb_v16_aeron_batch_resource_valid(&resp.response_descriptor) ||
        !vemb_v16_aeron_batch_resource_valid(&resp.response_arena) ||
        resp.request_descriptor.bytes !=
            vemb_v16_client_ring_bytes(resp.descriptor_slot_size) ||
        resp.response_descriptor.bytes !=
            vemb_v16_client_ring_bytes(resp.descriptor_slot_size) ||
        resp.request_descriptor.bytes % CACHELINE_SIZE != 0 ||
        resp.response_descriptor.bytes % CACHELINE_SIZE != 0 ||
        resp.request_arena.bytes != resp.max_batch_bytes ||
        resp.response_arena.bytes != resp.max_batch_bytes ||
        resp.request_arena.bytes % CACHELINE_SIZE != 0 ||
        resp.response_arena.bytes % CACHELINE_SIZE != 0) {
        if (resp.channel_id != 0)
            vemb_v16_aeron_notify_close(endpoint, resp.channel_id);
        return NULL;
    }

    vemb_v16_ub_peer_view_mapping_t request_descriptor_view;
    vemb_v16_ub_peer_view_mapping_t request_arena_view;
    vemb_v16_ub_peer_view_mapping_t response_descriptor_view;
    vemb_v16_ub_peer_view_mapping_t response_arena_view;
    if (vemb_v16_ub_peer_view_manifest_resolve(
            peer_view_manifest, client_host, owner_id,
            VEMB_V16_UB_PEER_VIEW_V2_REQUEST_DESCRIPTOR,
            resp.request_descriptor.path, 0, &request_descriptor_view) != 0 ||
        vemb_v16_ub_peer_view_manifest_resolve(
            peer_view_manifest, client_host, owner_id,
            VEMB_V16_UB_PEER_VIEW_V2_REQUEST_ARENA,
            resp.request_arena.path, 0, &request_arena_view) != 0 ||
        vemb_v16_ub_peer_view_manifest_resolve(
            peer_view_manifest, client_host, owner_id,
            VEMB_V16_UB_PEER_VIEW_V2_RESPONSE_DESCRIPTOR,
            resp.response_descriptor.path, 0, &response_descriptor_view) != 0 ||
        vemb_v16_ub_peer_view_manifest_resolve(
            peer_view_manifest, client_host, owner_id,
            VEMB_V16_UB_PEER_VIEW_V2_RESPONSE_ARENA,
            resp.response_arena.path, 0, &response_arena_view) != 0) {
        vemb_v16_aeron_notify_close(endpoint, resp.channel_id);
        return NULL;
    }

    fprintf(stderr,
            "[sdk] peer-view batch ch: cid=%llu req_desc_off=%llu "
            "req_arena_off=%llu resp_desc_off=%llu resp_arena_off=%llu\n",
            (unsigned long long)resp.channel_id,
            (unsigned long long)resp.request_descriptor.mmap_offset,
            (unsigned long long)resp.request_arena.mmap_offset,
            (unsigned long long)resp.response_descriptor.mmap_offset,
            (unsigned long long)resp.response_arena.mmap_offset);

    vemb_v16_aeron_batch_channel_t *ch = calloc(1, sizeof(*ch));
    assert(ch != NULL);
    snprintf(ch->control_endpoint, sizeof(ch->control_endpoint),
             "tcp://%s:%u", host, (unsigned)port);
    ch->channel_id = resp.channel_id;
    ch->topology_epoch = resp.topology_epoch;
    ch->effective_batch_size = resp.effective_batch_size;
    ch->max_batch_bytes = resp.max_batch_bytes;
    ch->descriptor_slot_size = resp.descriptor_slot_size;
    ch->descriptor_ring_slots = resp.descriptor_ring_slots;

    void *request_descriptor = NULL;
    void *response_descriptor = NULL;
    void *request_arena = NULL;
    void *response_arena = NULL;
    if (vemb_v16_aeron_map_ub_resource(
            &request_descriptor_view, resp.request_descriptor.mmap_offset,
            (size_t)resp.request_descriptor.bytes, 1,
            &ch->request_descriptor_mapping,
            &ch->request_descriptor_mapping_bytes, &request_descriptor) != 0 ||
    vemb_v16_aeron_map_ub_resource(
            &response_descriptor_view, resp.response_descriptor.mmap_offset,
            (size_t)resp.response_descriptor.bytes, 1,
            &ch->response_descriptor_mapping,
            &ch->response_descriptor_mapping_bytes, &response_descriptor) != 0 ||
    vemb_v16_aeron_map_ub_resource(
            &request_arena_view, resp.request_arena.mmap_offset,
            (size_t)resp.request_arena.bytes, 1,
            &ch->request_arena_mapping,
            &ch->request_arena_mapping_bytes, &request_arena) != 0 ||
    vemb_v16_aeron_map_ub_resource(
            &response_arena_view, resp.response_arena.mmap_offset,
            (size_t)resp.response_arena.bytes, 1,
            &ch->response_arena_mapping,
            &ch->response_arena_mapping_bytes, &response_arena) != 0) {
        vemb_v16_aeron_batch_unmap(ch);
        vemb_v16_aeron_notify_close(ch->control_endpoint, ch->channel_id);
        free(ch);
        return NULL;
    }
    ch->request_descriptor_ring = request_descriptor;
    ch->response_descriptor_ring = response_descriptor;
    ch->request_arena = request_arena;
    ch->response_arena = response_arena;
    batch_arena_producer_init(&ch->request_arena_producer);
    return ch;
}

vemb_v16_aeron_batch_channel_t *vemb_v16_aeron_open_remote_batch(
    const char *host, uint16_t port, uint32_t dim,
    uint32_t requested_batch_size, uint32_t requested_max_batch_bytes) {
    (void)host;
    (void)port;
    (void)dim;
    (void)requested_batch_size;
    (void)requested_max_batch_bytes;
    errno = EINVAL;
    return NULL;
}

void vemb_v16_aeron_batch_close(vemb_v16_aeron_batch_channel_t *ch) {
    vemb_v16_aeron_batch_unmap(ch);
    vemb_v16_aeron_notify_close(ch->control_endpoint, ch->channel_id);
    free(ch);
}

uint64_t vemb_v16_aeron_batch_channel_id(
    const vemb_v16_aeron_batch_channel_t *ch) {
    return ch->channel_id;
}

uint64_t vemb_v16_aeron_batch_topology_epoch(
    const vemb_v16_aeron_batch_channel_t *ch) {
    return ch->topology_epoch;
}

void vemb_v16_aeron_batch_get_resources(
    const vemb_v16_aeron_batch_channel_t *ch,
    vemb_v16_aeron_batch_resources_t *out) {
    assert(out != NULL);
    *out = (vemb_v16_aeron_batch_resources_t){
        .request_descriptor_ring = ch->request_descriptor_ring,
        .request_arena = ch->request_arena,
        .response_descriptor_ring = ch->response_descriptor_ring,
        .response_arena = ch->response_arena,
        .descriptor_slot_size = ch->descriptor_slot_size,
        .descriptor_ring_slots = ch->descriptor_ring_slots,
        .effective_batch_size = ch->effective_batch_size,
        .max_batch_bytes = ch->max_batch_bytes,
    };
}

int vemb_v16_aeron_batch_publish_handle_at_epoch(
    vemb_v16_aeron_batch_channel_t *ch, uint64_t batch_id,
    uint64_t submit_epoch, const char *const *keys,
    const uint16_t *key_lens, uint32_t item_count) {
    assert(keys != NULL && key_lens != NULL);
    RETURN_IF(batch_id == 0 || item_count == 0 ||
              item_count > ch->effective_batch_size, RING_ERR_INVALID);

    uint32_t key_bytes = 0;
    for (uint32_t i = 0; i < item_count; i++) {
        assert(keys[i] != NULL);
        RETURN_IF(key_lens[i] == 0 || key_lens[i] > VEMB_V16_MAX_KEY_LEN,
                  RING_ERR_INVALID);
        key_bytes += key_lens[i];
    }
    size_t frame_len = batch_request_encoded_len(key_bytes, item_count);
    RETURN_IF(frame_len > VEMB_V16_BATCH_MAX_BYTES_MAX ||
              frame_len > ch->max_batch_bytes, RING_ERR_INVALID);
    uint8_t frame[VEMB_V16_BATCH_MAX_BYTES_MAX];
    batch_request_encode(frame, batch_id, submit_epoch,
                         keys, key_lens,
                         item_count, key_bytes);
    return batch_arena_publish(
        ch->request_descriptor_ring, ch->request_arena, ch->max_batch_bytes,
        &ch->request_arena_producer, frame, (uint32_t)frame_len,
        item_count, batch_id);
}

int vemb_v16_aeron_batch_publish_handle(
    vemb_v16_aeron_batch_channel_t *ch, uint64_t batch_id,
    const char *const *keys, const uint16_t *key_lens, uint32_t item_count) {
    return vemb_v16_aeron_batch_publish_handle_at_epoch(
        ch, batch_id, ch->topology_epoch, keys, key_lens, item_count);
}

int vemb_v16_aeron_batch_poll_response(
    vemb_v16_aeron_batch_channel_t *ch, uint64_t *batch_id,
    uint64_t *topology_epoch, vemb_v16_resp_t *entries) {
    assert(batch_id != NULL && topology_epoch != NULL && entries != NULL);
    batch_desc_t desc;
    int peek = batch_desc_peek(ch->response_descriptor_ring, &desc);
    RETURN_IF(!peek, 0);
    RETURN_IF(!batch_desc_is_current(ch->response_descriptor_ring, &desc), 0);
    if (desc.bytes == 0 || desc.bytes > ch->max_batch_bytes ||
        desc.start % ch->max_batch_bytes + desc.bytes > ch->max_batch_bytes) {
        fprintf(stderr,
                "[sdk][WARNING] v2 batch response descriptor invalid: "
                "channel_id=%llu start=%llu bytes=%u arena_bytes=%u\n",
                (unsigned long long)ch->channel_id,
                (unsigned long long)desc.start, desc.bytes,
                ch->max_batch_bytes);
        return 0;
    }
    batch_response_view_t response;
    const uint8_t *frame = ch->response_arena + (desc.start % ch->max_batch_bytes);
    /* A local CC mapping can observe the published descriptor before a
     * reused remote-NC arena line becomes readable. Keep the descriptor at
     * head and retry instead of consuming valid work and disabling v2. */
    int decode_rc = batch_response_decode(&response, entries, frame, desc.bytes);
    if (decode_rc != 0 || response.batch_id != desc.batch_id ||
        response.item_count != desc.item_count ||
        response.item_count > ch->effective_batch_size) {
        fprintf(stderr,
                "[sdk][WARNING] v2 batch response frame not ready or "
                "inconsistent: channel_id=%llu sequence=%llu batch_id=%llu "
                "item_count=%u decode_rc=%d\n",
                (unsigned long long)ch->channel_id,
                (unsigned long long)desc.sequence,
                (unsigned long long)desc.batch_id, desc.item_count, decode_rc);
        return 0;
    }
    *batch_id = response.batch_id;
    *topology_epoch = response.topology_epoch;
    vemb_v16_client_consume_batch(ch->response_descriptor_ring, 1);
    return (int)response.item_count;
}

vemb_v16_aeron_channel_t *vemb_v16_aeron_open(const char *control_endpoint,
                                              uint32_t dim) {
    if (!control_endpoint || !control_endpoint[0] || dim == 0) return NULL;

    vemb_v16_aeron_channel_t *ch = calloc(1, sizeof(*ch));
    assert(ch != NULL);
    strncpy(ch->control_endpoint, control_endpoint,
            sizeof(ch->control_endpoint) - 1);

    int fd = vemb_v16_aeron_control_connect(control_endpoint);
    if (fd < 0) { free(ch); return NULL; }
    int rc = vemb_v16_aeron_tcp_alloc(fd, dim, &ch->desc);
    close(fd);
    if (rc != 0) {
        free(ch);
        return NULL;
    }
    ch->resource_generation = ch->desc.channel_id;
    if (vemb_v16_aeron_ring_open(ch->desc.request_ring_name,
                                 ch->desc.request_ring_slot_size,
                                 &ch->req_ring) != 0) {
        vemb_v16_aeron_notify_close(control_endpoint, ch->desc.channel_id);
        free(ch);
        return NULL;
    }
    if (vemb_v16_aeron_ring_open(ch->desc.response_ring_name,
                                 ch->desc.response_ring_slot_size,
                                 &ch->resp_ring) != 0) {
        vemb_v16_aeron_ring_close(ch->req_ring, ch->desc.request_ring_slot_size);
        vemb_v16_aeron_notify_close(control_endpoint, ch->desc.channel_id);
        free(ch);
        return NULL;
    }
    return ch;
}

vemb_v16_aeron_channel_t *vemb_v16_aeron_open_remote_with_peer_view(
    const char *host, uint16_t port, uint32_t dim,
    const vemb_v16_ub_peer_view_manifest_t *peer_view_manifest,
    const char *client_host, uint32_t owner_id) {
    if (!host || !host[0] || dim == 0 || port == 0 ||
        !peer_view_manifest || !client_host || !client_host[0])
        return NULL;

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
        resp.req_slot_size == 0 ||
        resp.resp_slot_size == 0 ||
        resp.ring_size_slots != VEMB_V16_CLIENT_RING_SIZE ||
        resp.request_shmdev_path[0] == '\0' ||
        resp.response_shmdev_path[0] == '\0' ||
        resp.req_backend_type != VEMB_V16_REGION_UB ||
        resp.resp_backend_type != VEMB_V16_REGION_UB ||
        resp.warm_region_count > 1 ||
        (resp.warm_region_count == 1 &&
         (resp.warm_path_len == 0 ||
          resp.warm_path_len > VEMB_V16_AERON_SHMDEV_PATH_MAX ||
          resp.warm_path[0] == '\0' ||
          resp.warm_backend_type != VEMB_V16_REGION_UB ||
          resp.warm_region_bytes == 0))) {
        char rejected_endpoint[256];
        snprintf(rejected_endpoint, sizeof(rejected_endpoint),
                 "tcp://%s:%u", host, (unsigned)port);
        vemb_v16_aeron_notify_close(rejected_endpoint, resp.channel_id);
        return NULL;
    }
    char endpoint[256];
    snprintf(endpoint, sizeof(endpoint), "tcp://%s:%u", host, (unsigned)port);
    vemb_v16_ub_peer_view_mapping_t request_view;
    vemb_v16_ub_peer_view_mapping_t response_view;
    if (vemb_v16_ub_peer_view_manifest_resolve(
            peer_view_manifest, client_host, owner_id,
            VEMB_V16_UB_PEER_VIEW_V1_REQUEST_RING,
            resp.request_shmdev_path, 0, &request_view) != 0 ||
        vemb_v16_ub_peer_view_manifest_resolve(
            peer_view_manifest, client_host, owner_id,
            VEMB_V16_UB_PEER_VIEW_V1_RESPONSE_RING,
            resp.response_shmdev_path, 0, &response_view) != 0) {
        vemb_v16_aeron_notify_close(endpoint, resp.channel_id);
        return NULL;
    }

    vemb_v16_aeron_channel_t *ch = calloc(1, sizeof(*ch));
    assert(ch != NULL);
    memcpy(ch->control_endpoint, endpoint, sizeof(ch->control_endpoint));
    ch->remote = 1;
    ch->desc.channel_id          = resp.channel_id;
    ch->resource_generation      = resp.channel_id;
    ch->desc.vector_dim          = dim;
    ch->desc.vector_stride       = dim * sizeof(float);
    ch->desc.request_ring_slot_size  = resp.req_slot_size;
    ch->desc.response_ring_slot_size = resp.resp_slot_size;

    void *request_ring = NULL;
    void *response_ring = NULL;
    if (vemb_v16_aeron_map_ub_resource(
            &request_view, resp.req_ring_off,
            vemb_v16_client_ring_bytes(resp.req_slot_size), 1,
            &ch->req_ring_mapping,
            &ch->req_ring_mapping_bytes, &request_ring) != 0 ||
    vemb_v16_aeron_map_ub_resource(
            &response_view, resp.resp_ring_off,
            vemb_v16_client_ring_bytes(resp.resp_slot_size), 1,
            &ch->resp_ring_mapping,
            &ch->resp_ring_mapping_bytes, &response_ring) != 0) {
        if (ch->req_ring_mapping)
            munmap(ch->req_ring_mapping, ch->req_ring_mapping_bytes);
        if (ch->resp_ring_mapping)
            munmap(ch->resp_ring_mapping, ch->resp_ring_mapping_bytes);
        vemb_v16_aeron_notify_close(ch->control_endpoint, ch->desc.channel_id);
        free(ch);
        return NULL;
    }
    ch->req_ring = request_ring;
    ch->resp_ring = response_ring;
    if (!vemb_v16_aeron_ring_header_valid(ch->req_ring, resp.req_slot_size) ||
        !vemb_v16_aeron_ring_header_valid(ch->resp_ring, resp.resp_slot_size)) {
        munmap(ch->req_ring_mapping, ch->req_ring_mapping_bytes);
        munmap(ch->resp_ring_mapping, ch->resp_ring_mapping_bytes);
        vemb_v16_aeron_notify_close(ch->control_endpoint, ch->desc.channel_id);
        free(ch);
        return NULL;
    }

    /* Parse advertised warm region (if any) into channel desc so the
     * runner can mmap it for VEMB_HANDLE dereference. The path in the
     * response is server-side, so map it to the local UB view first. */
    vemb_v16_ub_peer_view_mapping_t warm_view;
    if (resp.warm_region_count > 0 && resp.warm_path[0] &&
        resp.warm_backend_type == VEMB_V16_REGION_UB &&
        vemb_v16_ub_peer_view_manifest_resolve(
            peer_view_manifest, client_host, owner_id,
            VEMB_V16_UB_PEER_VIEW_WARM_REGION,
            resp.warm_path, 0, &warm_view) == 0) {
        ch->desc.warm_region_count = 1;
        ch->desc.warm_regions[0].region_id   = resp.warm_region_id;
        ch->desc.warm_regions[0].backend_type = resp.warm_backend_type;
        ch->desc.warm_regions[0].region_bytes = resp.warm_region_bytes;
        ch->desc.warm_regions[0].mmap_offset  = resp.warm_mmap_offset;
        strncpy(ch->desc.warm_regions[0].path, warm_view.client_path,
                sizeof(ch->desc.warm_regions[0].path) - 1);
        ch->desc.warm_regions[0].path[sizeof(ch->desc.warm_regions[0].path) - 1] = 0;
        ch->warm_peer_views[0].map_flags = warm_view.map_flags;
        ch->warm_peer_views[0].cache_policy = warm_view.cache_policy;
        ch->warm_peer_views[0].local_path = warm_view.local_path;
    } else if (resp.warm_region_count > 0) {
        vemb_v16_aeron_notify_close(ch->control_endpoint, ch->desc.channel_id);
        munmap(ch->req_ring_mapping, ch->req_ring_mapping_bytes);
        munmap(ch->resp_ring_mapping, ch->resp_ring_mapping_bytes);
        free(ch);
        return NULL;
    }

    fprintf(stderr, "[sdk] peer-view ch ok: cid=%llu req_path=%s resp_path=%s req_off=%llu resp_off=%llu req_slot=%u resp_slot=%u\n",
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

vemb_v16_aeron_channel_t *vemb_v16_aeron_open_remote(const char *host,
                                                     uint16_t port,
                                                     uint32_t dim) {
    (void)host;
    (void)port;
    (void)dim;
    errno = EINVAL;
    return NULL;
}

void vemb_v16_aeron_close(vemb_v16_aeron_channel_t *ch) {
    for (uint32_t i = 0; i < ch->warm_count; i++) {
        if (ch->warm[i].valid && ch->warm[i].mapping_addr) {
            munmap(ch->warm[i].mapping_addr, ch->warm[i].mapping_bytes);
            ch->warm[i].mapping_addr = NULL;
            ch->warm[i].valid = 0;
        }
    }
    ch->warm_count = 0;
    if (ch->req_ring_mapping)
        munmap(ch->req_ring_mapping, ch->req_ring_mapping_bytes);
    else
        vemb_v16_aeron_ring_close(ch->req_ring,
                                   ch->desc.request_ring_slot_size);
    if (ch->resp_ring_mapping)
        munmap(ch->resp_ring_mapping, ch->resp_ring_mapping_bytes);
    else
        vemb_v16_aeron_ring_close(ch->resp_ring,
                                   ch->desc.response_ring_slot_size);
    vemb_v16_aeron_notify_close(ch->control_endpoint, ch->desc.channel_id);
    free(ch);
}

int vemb_v16_aeron_close_all(const char *control_endpoint) {
    if (!control_endpoint || !control_endpoint[0]) return -1;
    int fd = vemb_v16_aeron_control_connect(control_endpoint);
    if (fd < 0) return -1;
    uint64_t closed = 0;
    int rc = vemb_v16_aeron_tcp_status_control(fd,
                                                VEMB_V16_NET_CLOSE_ALL_CHANNELS,
                                                0,
                                                &closed);
    close(fd);
    if (rc != 0) return -1;
    return (int)(closed > INT_MAX ? INT_MAX : closed);
}

uint64_t vemb_v16_aeron_channel_id(const vemb_v16_aeron_channel_t *ch) {
    return ch->desc.channel_id;
}

uint64_t vemb_v16_aeron_channel_resource_generation(
    const vemb_v16_aeron_channel_t *ch)
{
    return ch->resource_generation;
}

int vemb_v16_aeron_publish_request(vemb_v16_aeron_channel_t *ch,
                                   const void *buf, uint32_t len) {
    assert(buf != NULL);
    static uint32_t s_diag_printed = 0;
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
    assert(count == 0 || (bufs != NULL && lens != NULL));
    return vemb_v16_client_publish_ptr_batch(ch->req_ring, bufs, lens, count);
}

int vemb_v16_aeron_poll_response(vemb_v16_aeron_channel_t *ch,
                                 void *buf, uint32_t max_len) {
    assert(buf != NULL);
    RETURN_IF(max_len < sizeof(vemb_v16_resp_t), -2);
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
    RETURN_IF(max_len < sizeof(vemb_v16_resp_t) || max_count == 0, 0);
    assert(slots != NULL);
    uint32_t local_lens[VEMB_V16_CLIENT_RING_SIZE];
    uint32_t *lengths = wire_lens ? wire_lens : local_lens;
    if (max_count > VEMB_V16_CLIENT_RING_SIZE)
        max_count = VEMB_V16_CLIENT_RING_SIZE;
    uint32_t got = vemb_v16_client_poll_batch(
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
                                  uint32_t map_flags,
                                  uint32_t cache_policy,
                                  uint64_t region_bytes, uint64_t mmap_offset,
                                  void **out_mapping_addr, size_t *out_mapping_bytes,
                                  const uint8_t **out_mapped_addr) {
    if (!path || !path[0] || region_bytes == 0) return -1;
    int map_from_start = map_flags & VEMB_V16_UB_PEER_VIEW_MAP_F_FROM_START;
    if (map_from_start &&
        (mmap_offset > SIZE_MAX || region_bytes > SIZE_MAX - (size_t)mmap_offset))
        return -1;
    size_t map_bytes = 0, offset_delta = 0;
    void *ptr = vemb_v16_mmap_shmdev_region(
        path, backend_type,
        cache_policy == VEMB_V16_UB_PEER_VIEW_NONCACHEABLE,
        0, map_from_start ? 0 : mmap_offset,
        (size_t)region_bytes + (map_from_start ? (size_t)mmap_offset : 0),
        &map_bytes, &offset_delta);
    if (!ptr) return -1;
    *out_mapping_addr   = ptr;
    *out_mapping_bytes  = map_bytes;
    *out_mapped_addr = (const uint8_t *)ptr + offset_delta +
        (map_from_start ? (size_t)mmap_offset : 0);
    return 0;
}

int vemb_v16_aeron_open_warm_region(vemb_v16_aeron_channel_t *ch) {
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
                                   ch->warm_peer_views[i].map_flags,
                                   VEMB_V16_UB_PEER_VIEW_CACHEABLE,
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
    assert(out != NULL);
    RETURN_IF(bytes == 0 || bytes > cap, -1);
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
    RETURN_IF(base == NULL, -1);
    /* Bounds check against the server-reported region size. */
    RETURN_IF(offset > region_bytes ||
              (uint64_t)bytes > region_bytes - offset, -1);
    sve_streaming_load_f32(base + offset, out, bytes);
    return (int)bytes;
}

/* ------------------------------------------------------------------ */
/* UB data-transport adapter                                          */
/* ------------------------------------------------------------------ */

static sdk_ub_channel_t *sdk_ub_state(const vemb_v16_data_channel_t *channel)
{
    return channel->state;
}

static int sdk_ub_pending_reserve(sdk_ub_channel_t *state)
{
    if (state->pending_count < state->pending_cap)
        return 0;
    uint32_t next_cap = state->pending_cap ? state->pending_cap * 2u : 32u;
    size_t pending_bytes = (size_t)next_cap * sizeof(*state->pending);
    if (next_cap < state->pending_cap ||
        pending_bytes / sizeof(*state->pending) != next_cap) {
        return -1;
    }
    sdk_ub_pending_t *pending = realloc(state->pending, pending_bytes);
    assert(pending != NULL);
    state->pending = pending;
    state->pending_cap = next_cap;
    return 0;
}

static sdk_ub_pending_t *sdk_ub_pending_find(sdk_ub_channel_t *state,
                                              uint32_t wire_req_id,
                                              uint32_t *out_index)
{
    for (uint32_t i = 0; i < state->pending_count; i++) {
        if (state->pending[i].wire_req_id == wire_req_id) {
            *out_index = i;
            return &state->pending[i];
        }
    }
    return NULL;
}

static void sdk_ub_pending_remove(sdk_ub_channel_t *state, uint32_t index)
{
    state->pending_count--;
    if (index != state->pending_count)
        state->pending[index] = state->pending[state->pending_count];
}

/* The resolved owner endpoint is a TCP control endpoint. After ATTACH, all
 * request and response traffic below is carried only by the client-side
 * peer-view mappings. The same resolver is used for local and remote owners;
 * a local manifest simply has identical provider_path and client_path. */
static int sdk_ub_open_owner_channel(
    vemb_v16_data_channel_t *channel,
    const vemb_v16_transport_open_spec_t *spec)
{
    assert(channel->state == NULL);

    const sdk_ub_owner_open_context_t *peer_view = spec->backend_context;
    assert(peer_view != NULL && peer_view->peer_view_manifest != NULL &&
           peer_view->client_host != NULL && peer_view->client_host[0]);
    vemb_v16_aeron_channel_t *aeron =
        vemb_v16_aeron_open_remote_with_peer_view(
            spec->host, spec->port, spec->vector_dim,
            peer_view->peer_view_manifest, peer_view->client_host,
            spec->owner_id);
    if (!aeron)
        return -1;

    sdk_ub_channel_t *state = calloc(1, sizeof(*state));
    assert(state != NULL);
    state->aeron = aeron;
    channel->state = state;
    channel->channel_id = vemb_v16_aeron_channel_id(aeron);
    channel->resource_generation =
        vemb_v16_aeron_channel_resource_generation(aeron);
    if (channel->resource_generation == 0) {
        vemb_v16_aeron_close(aeron);
        free(state);
        channel->state = NULL;
        channel->channel_id = 0;
        return -1;
    }
    channel->generation++;
    if (channel->generation == 0)
        channel->generation = 1;
    return 0;
}

static int sdk_ub_submit(vemb_v16_data_channel_t *channel,
                         const vemb_v16_transport_submission_t *submission)
{
    sdk_ub_channel_t *state = sdk_ub_state(channel);
    uint32_t unused_index = 0;
    uint8_t wire[VEMB_V16_AERON_REQ_WIRE_MAX_LEN];
    size_t wire_len = 0;

    if (sdk_ub_pending_find(state, submission->wire_req_id, &unused_index) ||
        vemb_v16_req_encode(wire, sizeof(wire), submission->request,
                            &wire_len) != 0 ||
        sdk_ub_pending_reserve(state) != 0) {
        return -1;
    }

    sdk_ub_pending_t *pending = &state->pending[state->pending_count++];
    *pending = (sdk_ub_pending_t){
        .operation_id = submission->operation_id,
        .submit_epoch = submission->submit_epoch,
        .owner_id = submission->owner_id,
        .wire_req_id = submission->wire_req_id,
        .expected_op = submission->request->op,
        .inline_vector = submission->inline_vector,
        .inline_vector_cap = submission->inline_vector_cap,
    };
    if (vemb_v16_aeron_publish_request(state->aeron, wire,
                                       (uint32_t)wire_len) == 0) {
        return 0;
    }
    state->pending_count--;
    return -1;
}

static vemb_v16_transport_poll_result_t sdk_ub_poll(
    vemb_v16_data_channel_t *channel,
    uint32_t timeout_ms,
    vemb_v16_transport_completion_t *out)
{
    sdk_ub_channel_t *state = sdk_ub_state(channel);
    uint32_t waited_ms = 0;
    /* Poll fast-path deadline: busy-spin (cpu_relax) for SPIN_WINDOW_NS
     * before falling back to 1 ms sleeps. Server batch RTT can reach
     * several hundred µs (cluster key-key via shard queues + remote
     * meta); a too-short window silently pays a full 1 ms sleep per
     * batch. Polite yields proved too coarse, so this is a time-bounded
     * relax spin. */
    uint64_t spin_until_ns = 0;

    for (;;) {
        vemb_v16_resp_t response;
        int poll_rc = vemb_v16_aeron_poll_response(state->aeron, &response,
                                                    sizeof(response));
        if (poll_rc < 0)
            goto failed;
        if (poll_rc > 0) {
            uint32_t pending_index = 0;
            sdk_ub_pending_t *pending = sdk_ub_pending_find(
                state, response.req_id, &pending_index);
            if (!pending || response.op != pending->expected_op)
                goto failed;

            *out = (vemb_v16_transport_completion_t){
                .operation_id = pending->operation_id,
                .channel_generation = channel->generation,
                .owner_id = pending->owner_id,
                .submit_epoch = pending->submit_epoch,
                .response = response,
                /* v1 UB response slots encode response metadata only. A
                 * VEMB_HANDLE is materialized from the channel warm view. */
                .inline_vector_bytes = 0,
            };
            sdk_ub_pending_remove(state, pending_index);
            return VEMB_V16_TRANSPORT_POLL_COMPLETION;
        }

        if (timeout_ms == 0)
            return VEMB_V16_TRANSPORT_POLL_EMPTY;
        if (timeout_ms != VEMB_V16_TRANSPORT_WAIT_FOREVER &&
            waited_ms >= timeout_ms) {
            return VEMB_V16_TRANSPORT_POLL_EMPTY;
        }

        /* UB has no readable fd. Time-bounded cpu_relax spin (default
         * 600 µs, VEMB_V16_UB_POLL_SPIN_US) covering typical server batch
         * RTT, then fall back to the 1 ms sleep; WAIT_FOREVER keeps the
         * same pull contract. */
        if (waited_ms == 0) {
            if (spin_until_ns == 0)
                spin_until_ns = vemb_v16_monotonic_ns() + ub_poll_spin_ns;
            if (vemb_v16_monotonic_ns() < spin_until_ns) {
                for (int i = 0; i < 32; i++)
                    cpu_relax();
                continue;
            }
        }

        (void)poll(NULL, 0, 1);
        if (timeout_ms != VEMB_V16_TRANSPORT_WAIT_FOREVER)
            waited_ms++;
    }

failed:
    sdk_ub_close_channel(channel);
    return VEMB_V16_TRANSPORT_POLL_FAILED;
}

static int sdk_ub_open_warm_region(vemb_v16_data_channel_t *channel,
                                   vemb_v16_warm_view_t **out)
{
    sdk_ub_channel_t *state = sdk_ub_state(channel);
    if (channel->warm.mapped_addr) {
        if (out)
            *out = &channel->warm;
        return 0;
    }
    if (state->aeron->desc.warm_region_count == 0 &&
        state->aeron->desc.vector_region_name[0] == '\0') {
        return -1;
    }
    if (vemb_v16_aeron_open_warm_region(state->aeron) != 0)
        return -1;

    for (uint32_t i = 0; i < state->aeron->warm_count; i++) {
        if (!state->aeron->warm[i].valid)
            continue;
        channel->warm = (vemb_v16_warm_view_t){
            .region_id = state->aeron->warm[i].region_id,
            .region_bytes = state->aeron->warm[i].region_bytes,
            .mapping_bytes = state->aeron->warm[i].mapping_bytes,
            .mapping_addr = state->aeron->warm[i].mapping_addr,
            .mapped_addr = (uint8_t *)state->aeron->warm[i].mapped_addr,
        };
        if (out)
            *out = &channel->warm;
        return 0;
    }
    return -1;
}

static int sdk_ub_read_warm_vector(vemb_v16_data_channel_t *channel,
                                   uint32_t region_id,
                                   uint64_t offset,
                                   uint32_t bytes,
                                   void *out,
                                   uint32_t out_cap)
{
    sdk_ub_channel_t *state = sdk_ub_state(channel);
    if (!channel->warm.mapped_addr &&
        sdk_ub_open_warm_region(channel, NULL) != 0) {
        return -1;
    }
    return vemb_v16_aeron_read_vector(state->aeron, region_id, offset, bytes,
                                      out, out_cap) == (int)bytes ? 0 : -1;
}

static vemb_v16_transport_resource_state_t sdk_ub_check_resource(
    vemb_v16_data_channel_t *channel)
{
    sdk_ub_channel_t *state = sdk_ub_state(channel);
    vemb_v16_transport_resource_state_t resource_state =
        vemb_v16_aeron_check_resource(state->aeron);
    if (resource_state == VEMB_V16_TRANSPORT_RESOURCE_CURRENT &&
        channel->resource_generation !=
            vemb_v16_aeron_channel_resource_generation(state->aeron)) {
        return VEMB_V16_TRANSPORT_RESOURCE_REATTACH;
    }
    return resource_state;
}

static int sdk_ub_fence(vemb_v16_data_channel_t *channel)
{
    return sdk_ub_state(channel)->pending_count == 0 ? 0 : -1;
}

static void sdk_ub_close_channel(vemb_v16_data_channel_t *channel)
{
    sdk_ub_channel_t *state = sdk_ub_state(channel);
    channel->warm = (vemb_v16_warm_view_t){0};
    vemb_v16_aeron_close(state->aeron);
    free(state->pending);
    free(state);
    channel->state = NULL;
    channel->channel_id = 0;
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
