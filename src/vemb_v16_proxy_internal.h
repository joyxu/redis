#ifndef __VEMB_V16_PROXY_INTERNAL_H
#define __VEMB_V16_PROXY_INTERNAL_H

#include "vemb_v16_proxy.h"
#include "vemb_v16_client_ring.h"
#include "vemb_v16_dataplane.h"

#include <stddef.h>
#include <stdint.h>

#define VEMB_V16_TCP_RESPONSE_BACKLOG_LIMIT (4u * 1024u * 1024u)
#ifndef PROXY_REQUEST_BATCH
#define PROXY_REQUEST_BATCH 32u
#endif
#ifndef PROXY_RESPONSE_BATCH
#define PROXY_RESPONSE_BATCH 32u
#endif
#ifndef PROXY_QUEUE_BATCH
#define PROXY_QUEUE_BATCH 32u
#endif
#define VEMB_V16_SUPERNODE_STATE_CLOSING (1u << 31)
#define VEMB_V16_PROXY_IO_STATE_CLOSING (1u << 31)

typedef struct vemb_v16_channel vemb_v16_channel_t;

typedef struct vemb_v16_transport_listener {
    const char *name;
    int fd;
    void (*handle_fd)(vemb_v16_proxy_t *proxy, int fd);
} vemb_v16_transport_listener_t;

uint64_t vemb_v16_channel_id(vemb_v16_channel_t *ch);
int vemb_v16_channel_active(vemb_v16_channel_t *ch);
int vemb_v16_channel_net_fd(vemb_v16_channel_t *ch);
int vemb_v16_channel_tcp_backpressure_enabled(vemb_v16_channel_t *ch);
int vemb_v16_channel_proxy_running(vemb_v16_channel_t *ch);
vemb_v16_client_ring_t *vemb_v16_channel_request_ring(vemb_v16_channel_t *ch);
vemb_v16_client_ring_t *vemb_v16_channel_response_ring(vemb_v16_channel_t *ch);
uint32_t vemb_v16_channel_request_slot_size(vemb_v16_channel_t *ch);
void vemb_v16_channel_add_proxy_response_ring_full(vemb_v16_channel_t *ch,
                                                   uint64_t n);
const char *vemb_v16_proxy_uds_path(vemb_v16_proxy_t *proxy);
const char *vemb_v16_proxy_aeron_ub_path(vemb_v16_proxy_t *proxy);
const char *vemb_v16_proxy_tcp_host(vemb_v16_proxy_t *proxy);
uint16_t vemb_v16_proxy_tcp_port(vemb_v16_proxy_t *proxy);
uint32_t vemb_v16_proxy_data_transport(vemb_v16_proxy_t *proxy);

size_t vemb_v16_tcp_input_pending_bytes(vemb_v16_channel_t *ch);
size_t vemb_v16_tcp_input_tailroom(vemb_v16_channel_t *ch);
uint8_t *vemb_v16_tcp_input_buffer(vemb_v16_channel_t *ch);
uint8_t *vemb_v16_tcp_input_pending_ptr(vemb_v16_channel_t *ch);
uint8_t *vemb_v16_tcp_input_tail_ptr(vemb_v16_channel_t *ch);
void vemb_v16_tcp_input_set_buffer(vemb_v16_channel_t *ch,
                                   uint8_t *buf,
                                   size_t cap);
void vemb_v16_tcp_input_append_done(vemb_v16_channel_t *ch, size_t len);
void vemb_v16_tcp_input_consume(vemb_v16_channel_t *ch, size_t len);
void vemb_v16_tcp_input_compact(vemb_v16_channel_t *ch);
void vemb_v16_tcp_input_reset(vemb_v16_channel_t *ch);

#ifdef __linux__
int vemb_v16_tcp_backlog_pending(vemb_v16_channel_t *ch);
size_t vemb_v16_tcp_backlog_pending_bytes(vemb_v16_channel_t *ch);
uint8_t *vemb_v16_tcp_backlog_pending_ptr(vemb_v16_channel_t *ch);
void vemb_v16_tcp_backlog_compact(vemb_v16_channel_t *ch, size_t pending);
size_t vemb_v16_tcp_backlog_capacity(vemb_v16_channel_t *ch);
uint8_t *vemb_v16_tcp_backlog_buffer(vemb_v16_channel_t *ch);
void vemb_v16_tcp_backlog_set_buffer(vemb_v16_channel_t *ch,
                                     uint8_t *buf,
                                     size_t cap);
void vemb_v16_tcp_backlog_append_done(vemb_v16_channel_t *ch, size_t len);
void vemb_v16_tcp_backlog_consume(vemb_v16_channel_t *ch, size_t len);
void vemb_v16_tcp_backlog_reset(vemb_v16_channel_t *ch);
#endif

int vemb_v16_proxy_tcp_response_vector_slice(vemb_v16_channel_t *ch,
                                             vemb_v16_resp_t *resp,
                                             const uint8_t **vector,
                                             uint32_t *vector_bytes);
void vemb_v16_make_response_from(vemb_v16_resp_t *resp,
                                 const vemb_v16_completion_t *completion);
void vemb_v16_proxy_handle_request(vemb_v16_channel_t *ch,
                                   const vemb_v16_req_t *req,
                                   int req_len,
                                   uint32_t proxy_io_worker_id);
void vemb_v16_proxy_handle_request_ptr_batch(
    vemb_v16_channel_t *ch,
    const vemb_v16_req_t *const *reqs,
    int req_len,
    uint32_t req_count,
    uint32_t proxy_io_worker_id);
int vemb_v16_proxy_alloc_shm_channel(vemb_v16_proxy_t *proxy,
                                     vemb_v16_channel_desc_t *desc);
int vemb_v16_proxy_alloc_tcp_channel(vemb_v16_proxy_t *proxy,
                                     int net_fd,
                                     vemb_v16_channel_desc_t *desc);

/* Allocate a proxy channel for cross-node aeron transport.
 *   proxy       - proxy context
 *   req_ring    - already-mmaped shmdev req ring (caller holds mapping)
 *   resp_ring   - already-mmaped shmdev resp ring
 *   req_slot    - req ring slot size
 *   resp_slot   - resp ring slot size
 *   shmdev_path - shmdev path (logged for diagnostics)
 *   req_off     - byte offset of req_ring within shmdev (logged)
 *   resp_off    - byte offset of resp_ring within shmdev (logged)
 *   out_channel_id - receives the assigned channel id
 * Returns 0 on success. The proxy adopts the ring mappings (does NOT
 * munmap them - caller's storage layer owns that). */
int vemb_v16_proxy_attach_cross_node_channel(vemb_v16_proxy_t *proxy,
                                             void *req_ring, void *resp_ring,
                                             uint32_t req_slot, uint32_t resp_slot,
                                             const char *shmdev_path,
                                             uint64_t req_off, uint64_t resp_off,
                                             uint64_t *out_channel_id);
int vemb_v16_proxy_close_channel_by_id(vemb_v16_proxy_t *proxy,
                                       uint64_t channel_id);
uint64_t vemb_v16_proxy_close_all_channels(vemb_v16_proxy_t *proxy);

#endif
