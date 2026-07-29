#define _GNU_SOURCE
#include "vemb_v16_aeron_attach.h"
#include "vemb_v16_client_ring.h"
#include "vemb_v16_log.h"
#include "vemb_v16_proxy.h"
#include "vemb_v16_proxy_internal.h"
#include "vemb_v16_storage.h"

#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

/* Blocking write — mirrors vemb_v16_aeron_transport.c helpers. */
static int write_full(int fd, const void *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = write(fd, (const char *)buf + done, n - done);
        if (r <= 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        done += (size_t)r;
    }
    return 0;
}

static int read_full(int fd, void *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = read(fd, (char *)buf + done, n - done);
        if (r <= 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        done += (size_t)r;
    }
    return 0;
}

/* Default slot sizing — mirrors local-aeron path (proxy.c:3344-3346):
 * req slot fits VEMB_INLINE (header + dim*4 vector bytes), resp slot
 * fits the fixed response struct. */
static uint32_t default_req_slot_size(uint32_t dim) {
    return (uint32_t)vemb_v16_req_inline_len(dim * sizeof(float));
}

static uint32_t default_resp_slot_size(uint32_t dim) {
    (void)dim;
    return (uint32_t)sizeof(vemb_v16_resp_t);
}

int vemb_v16_aeron_attach_handle_fd(struct vemb_v16_proxy *proxy, int fd) {
    vemb_v16_aeron_attach_req_t req;
    memset(&req, 0, sizeof(req));
    /* The first 24 bytes (magic) were already consumed by the sniff
     * router. Re-stamp them here so the struct is complete for logging. */
    memcpy(req.magic, VEMB_V16_AERON_ATTACH_MAGIC,
           VEMB_V16_AERON_ATTACH_MAGIC_LEN);

    /* Read remaining fields (dim, req_slot_size, resp_slot_size). */
    if (read_full(fd, &req.dim, sizeof(req.dim)) != 0) return -1;
    if (read_full(fd, &req.req_slot_size, sizeof(req.req_slot_size)) != 0) return -1;
    if (read_full(fd, &req.resp_slot_size, sizeof(req.resp_slot_size)) != 0) return -1;
    if (read_full(fd, &req.flags, sizeof(req.flags)) != 0) return -1;

    if (req.dim == 0 || req.dim > 65536u) {
        serverLog(LL_WARNING, "aeron ATTACH rejected: dim=%u out of range", req.dim);
        vemb_v16_aeron_attach_resp_t rej;
        memset(&rej, 0, sizeof(rej));
        memcpy(rej.magic, VEMB_V16_AERON_ATTACHED_MAGIC,
               VEMB_V16_AERON_ATTACHED_MAGIC_LEN);
        rej.status = -1;
        write_full(fd, &rej, sizeof(rej));
        return -1;
    }

    uint32_t req_slot  = req.req_slot_size  ?: default_req_slot_size(req.dim);
    uint32_t resp_slot = req.resp_slot_size ?: default_resp_slot_size(req.dim);

    /* Allocate shmdev ring pair (server-local view). */
    char server_shmdev_path[256];
    uint64_t req_off = 0, resp_off = 0;
    void *req_map = NULL, *resp_map = NULL;
    size_t req_bytes = 0, resp_bytes = 0;
    if (vemb_v16_storage_alloc_aeron_channel(
                                             vemb_v16_proxy_aeron_ub_path(proxy),
                                             req_slot, resp_slot,
                                             VEMB_V16_CLIENT_RING_SIZE,
                                             server_shmdev_path,
                                             &req_off, &resp_off,
                                             &req_map, &resp_map,
                                             &req_bytes, &resp_bytes) != 0) {
        serverLog(LL_WARNING, "aeron ATTACH rejected: no shmdev slot (dim=%u)", req.dim);
        vemb_v16_aeron_attach_resp_t rej;
        memset(&rej, 0, sizeof(rej));
        memcpy(rej.magic, VEMB_V16_AERON_ATTACHED_MAGIC,
               VEMB_V16_AERON_ATTACHED_MAGIC_LEN);
        rej.status = -1;
        write_full(fd, &rej, sizeof(rej));
        return -1;
    }

    /* Allocate proxy channel that points at the shmdev rings. */
    uint64_t channel_id = 0;
    if (vemb_v16_proxy_attach_cross_node_channel(proxy,
                                                 req_map, resp_map,
                                                 req_slot, resp_slot,
                                                 server_shmdev_path,
                                                 req_off, resp_off,
                                                 &channel_id) != 0) {
        vemb_v16_storage_free_aeron_channel(req_map, req_bytes,
                                            resp_map, resp_bytes);
        serverLog(LL_WARNING, "aeron ATTACH rejected: proxy channel alloc failed");
        vemb_v16_aeron_attach_resp_t rej;
        memset(&rej, 0, sizeof(rej));
        memcpy(rej.magic, VEMB_V16_AERON_ATTACHED_MAGIC,
               VEMB_V16_AERON_ATTACHED_MAGIC_LEN);
        rej.status = -1;
        write_full(fd, &rej, sizeof(rej));
        return -1;
    }

    /* Write success response. */
    vemb_v16_aeron_attach_resp_t resp;
    memset(&resp, 0, sizeof(resp));
    memcpy(resp.magic, VEMB_V16_AERON_ATTACHED_MAGIC,
           VEMB_V16_AERON_ATTACHED_MAGIC_LEN);
    resp.status          = 0;
    resp.channel_id      = channel_id;
    resp.ring_size_slots = VEMB_V16_CLIENT_RING_SIZE;
    /* Return server-side paths. A remote client maps these paths to its
     * local UB view after ATTACH; the server must not encode a deployment
     * specific client device into the warm-region manifest. */
    resp.shmdev_path_len = (uint32_t)strnlen(server_shmdev_path, 255) + 1u;
    strncpy(resp.shmdev_path, server_shmdev_path, 255);
    resp.req_ring_off    = req_off;
    resp.resp_ring_off   = resp_off;
    resp.req_slot_size   = req_slot;
    resp.resp_slot_size  = resp_slot;
    /* Advertise the server-side warm region path. The remote client maps it
     * to its local UB view using the same path mapping as the rings. */
    vemb_v16_proxy_fill_attach_warm_region(proxy, &resp);

    if (write_full(fd, &resp, sizeof(resp)) != 0) {
        vemb_v16_storage_free_aeron_channel(req_map, req_bytes,
                                            resp_map, resp_bytes);
        return -1;
    }

    serverLog(LL_VERBOSE,
              "aeron ATTACH ok: channel_id=%llu dim=%u req_slot=%u resp_slot=%u "
              "server_shmdev=%s req_off=%llu resp_off=%llu "
              "warm_count=%u warm_path=%s",
              (unsigned long long)channel_id, req.dim, req_slot, resp_slot,
              server_shmdev_path,
              (unsigned long long)req_off, (unsigned long long)resp_off,
              resp.warm_region_count,
              resp.warm_region_count ? resp.warm_path : "(none)");
    return 0;
}

int vemb_v16_aeron_attach_client_exchange(int fd,
                                          const vemb_v16_aeron_attach_req_t *req,
                                          vemb_v16_aeron_attach_resp_t *resp) {
    if (write_full(fd, req, sizeof(*req)) != 0) return -1;
    if (read_full(fd, resp, sizeof(*resp)) != 0) return -1;
    if (memcmp(resp->magic, VEMB_V16_AERON_ATTACHED_MAGIC,
               VEMB_V16_AERON_ATTACHED_MAGIC_LEN) != 0) return -1;
    return resp->status == 0 ? 0 : -1;
}
