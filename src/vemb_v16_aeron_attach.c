#define _GNU_SOURCE
#include "vemb_v16_aeron_attach.h"
#include "vemb_v16_client_ring.h"
#include "vemb_v16_log.h"
#include "vemb_v16_proxy.h"
#include "vemb_v16_proxy_internal.h"
#include "vemb_v16_proxy_types.h"
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

/* Default slot sizing matches the compact Aeron wire frames. */
static uint32_t default_req_slot_size(uint32_t dim) {
    return vemb_v16_aeron_req_slot_size(dim);
}

static uint32_t default_resp_slot_size(uint32_t dim) {
    (void)dim;
    return vemb_v16_aeron_resp_slot_size();
}

static void vemb_v16_aeron_attach_v2_reject(int fd) {
    vemb_v16_aeron_attach_v2_resp_t rej;
    memset(&rej, 0, sizeof(rej));
    memcpy(rej.magic, VEMB_V16_AERON_ATTACHED_V2_MAGIC,
           VEMB_V16_AERON_ATTACHED_V2_MAGIC_LEN);
    rej.status = -1;
    (void)write_full(fd, &rej, sizeof(rej));
}

static void vemb_v16_aeron_attach_v2_fill_resource(
    vemb_v16_aeron_batch_resource_desc_t *resource,
    const char *path, uint64_t mmap_offset, size_t bytes) {
    resource->backend_type = VEMB_V16_REGION_UB;
    resource->path_len = (uint32_t)strnlen(path, 255) + 1u;
    resource->mmap_offset = mmap_offset;
    resource->bytes = bytes;
    strncpy(resource->path, path, sizeof(resource->path) - 1);
    resource->path[sizeof(resource->path) - 1] = '\0';
}

int vemb_v16_aeron_attach_v2_handle_fd(struct vemb_v16_proxy *proxy, int fd) {
    if (!proxy)
        return -1;

    vemb_v16_aeron_attach_v2_req_t req;
    memset(&req, 0, sizeof(req));
    memcpy(req.magic, VEMB_V16_AERON_ATTACH_V2_MAGIC,
           VEMB_V16_AERON_ATTACH_V2_MAGIC_LEN);
    if (read_full(fd, &req.dim, sizeof(req.dim)) != 0 ||
        read_full(fd, &req.flags, sizeof(req.flags)) != 0 ||
        read_full(fd, &req.requested_batch_size,
                  sizeof(req.requested_batch_size)) != 0 ||
        read_full(fd, &req.max_batch_bytes, sizeof(req.max_batch_bytes)) != 0)
        return -1;

    if (req.dim != proxy->vector_dim || req.requested_batch_size == 0 ||
        vemb_v16_storage_migration_active(proxy->storage)) {
        serverLog(LL_WARNING,
                  "aeron v2 ATTACH rejected: dim=%u requested_batch_size=%u migration_active=%d",
                  req.dim, req.requested_batch_size,
                  vemb_v16_storage_migration_active(proxy->storage));
        vemb_v16_aeron_attach_v2_reject(fd);
        return -1;
    }

    uint32_t effective_batch_size = vemb_v16_effective_batch_request_size(
        req.requested_batch_size,
        atomic_load_explicit(&proxy->batch_request_size, memory_order_acquire));
    uint32_t max_batch_bytes = req.max_batch_bytes ? req.max_batch_bytes :
        VEMB_V16_BATCH_MAX_BYTES_DEFAULT;
    if (max_batch_bytes > VEMB_V16_BATCH_MAX_BYTES_MAX)
        max_batch_bytes = VEMB_V16_BATCH_MAX_BYTES_MAX;
    max_batch_bytes = vemb_v16_batch_aligned_bytes(max_batch_bytes);
    if (effective_batch_size == 0 || max_batch_bytes == 0) {
        vemb_v16_aeron_attach_v2_reject(fd);
        return -1;
    }

    vemb_v16_aeron_batch_channel_allocation_t allocation;
    if (vemb_v16_storage_alloc_aeron_batch_channel(
            vemb_v16_proxy_aeron_ub_path(proxy),
            vemb_v16_proxy_aeron_response_ub_path(proxy),
            VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE,
            VEMB_V16_CLIENT_RING_SIZE,
            max_batch_bytes, max_batch_bytes, &allocation) != 0) {
        serverLog(LL_WARNING,
                  "aeron v2 ATTACH rejected: four-region UB allocation failed");
        vemb_v16_aeron_attach_v2_reject(fd);
        return -1;
    }

    uint64_t channel_id = 0;
    if (vemb_v16_proxy_attach_cross_node_batch_channel(
            proxy, &allocation, effective_batch_size, max_batch_bytes,
            &channel_id) != 0) {
        vemb_v16_storage_free_aeron_channel(
            allocation.request_desc_mapping, allocation.request_desc_bytes,
            allocation.response_desc_mapping, allocation.response_desc_bytes);
        vemb_v16_storage_free_aeron_channel(
            allocation.request_arena_mapping, allocation.request_arena_bytes,
            allocation.response_arena_mapping, allocation.response_arena_bytes);
        serverLog(LL_WARNING,
                  "aeron v2 ATTACH rejected: proxy batch channel allocation failed");
        vemb_v16_aeron_attach_v2_reject(fd);
        return -1;
    }

    vemb_v16_aeron_attach_v2_resp_t resp;
    memset(&resp, 0, sizeof(resp));
    memcpy(resp.magic, VEMB_V16_AERON_ATTACHED_V2_MAGIC,
           VEMB_V16_AERON_ATTACHED_V2_MAGIC_LEN);
    resp.status = 0;
    resp.channel_id = channel_id;
    vemb_v16_storage_epoch_get(proxy->storage, &resp.topology_epoch, NULL);
    resp.effective_batch_size = effective_batch_size;
    resp.max_batch_bytes = max_batch_bytes;
    resp.descriptor_slot_size = VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE;
    resp.descriptor_ring_slots = VEMB_V16_CLIENT_RING_SIZE;
    vemb_v16_aeron_attach_v2_fill_resource(
        &resp.request_descriptor, allocation.request_path,
        allocation.request_desc_off, allocation.request_desc_bytes);
    vemb_v16_aeron_attach_v2_fill_resource(
        &resp.request_arena, allocation.request_path,
        allocation.request_arena_off, allocation.request_arena_bytes);
    vemb_v16_aeron_attach_v2_fill_resource(
        &resp.response_descriptor, allocation.response_path,
        allocation.response_desc_off, allocation.response_desc_bytes);
    vemb_v16_aeron_attach_v2_fill_resource(
        &resp.response_arena, allocation.response_path,
        allocation.response_arena_off, allocation.response_arena_bytes);

    if (write_full(fd, &resp, sizeof(resp)) != 0) {
        (void)vemb_v16_proxy_close_channel_by_id(proxy, channel_id);
        return -1;
    }

    serverLog(LL_VERBOSE,
              "aeron v2 ATTACH ok: channel_id=%llu dim=%u batch_size=%u "
              "max_batch_bytes=%u req_desc_off=%llu req_arena_off=%llu "
              "resp_desc_off=%llu resp_arena_off=%llu",
              (unsigned long long)channel_id, req.dim, effective_batch_size,
              max_batch_bytes,
              (unsigned long long)allocation.request_desc_off,
              (unsigned long long)allocation.request_arena_off,
              (unsigned long long)allocation.response_desc_off,
              (unsigned long long)allocation.response_arena_off);
    return 0;
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

    uint32_t req_slot = req.req_slot_size ? req.req_slot_size :
        default_req_slot_size(req.dim);
    uint32_t resp_slot = req.resp_slot_size ? req.resp_slot_size :
        default_resp_slot_size(req.dim);
    req_slot = (uint32_t)align_up_size(req_slot, CACHELINE_SIZE);
    resp_slot = (uint32_t)align_up_size(resp_slot, CACHELINE_SIZE);

    /* Allocate shmdev ring pair (server-local view). */
    char server_request_shmdev_path[256];
    char server_response_shmdev_path[256];
    uint64_t req_off = 0, resp_off = 0;
    void *req_map = NULL, *resp_map = NULL;
    size_t req_bytes = 0, resp_bytes = 0;
    if (vemb_v16_storage_alloc_aeron_channel(
                                             vemb_v16_proxy_aeron_ub_path(proxy),
                                             vemb_v16_proxy_aeron_response_ub_path(proxy),
                                             req_slot, resp_slot,
                                             VEMB_V16_CLIENT_RING_SIZE,
                                             server_request_shmdev_path,
                                             server_response_shmdev_path,
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
                                                 server_request_shmdev_path,
                                                 server_response_shmdev_path,
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
    /* Return independent server-side paths. A remote client maps each path
     * to its local UB view after ATTACH. */
    resp.request_shmdev_path_len =
        (uint32_t)strnlen(server_request_shmdev_path, 255) + 1u;
    strncpy(resp.request_shmdev_path, server_request_shmdev_path, 255);
    resp.req_ring_off = req_off;
    resp.response_shmdev_path_len =
        (uint32_t)strnlen(server_response_shmdev_path, 255) + 1u;
    strncpy(resp.response_shmdev_path, server_response_shmdev_path, 255);
    resp.resp_ring_off = resp_off;
    resp.req_backend_type = VEMB_V16_REGION_UB;
    resp.resp_backend_type = VEMB_V16_REGION_UB;
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
              server_request_shmdev_path,
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
