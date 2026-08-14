#ifndef __VEMB_V16_AERON_TRANSPORT_H
#define __VEMB_V16_AERON_TRANSPORT_H

#include "vemb_v16_proxy_internal.h"
#include "vemb_v16_batch_ring.h"

int vemb_v16_aeron_poll_shm_requests(vemb_v16_channel_t *ch,
                                     uint32_t proxy_io_worker_id);
int vemb_v16_aeron_listen(vemb_v16_proxy_t *proxy,
                          int backlog,
                          vemb_v16_transport_listener_t *listener);
void vemb_v16_aeron_handle_control_fd(vemb_v16_proxy_t *proxy, int fd);
int vemb_v16_aeron_publish_response(vemb_v16_channel_t *ch,
                                    const vemb_v16_resp_t *resp);
int vemb_v16_aeron_publish_response_batch(vemb_v16_channel_t *ch,
                                          const vemb_v16_resp_t *resps,
                                          uint32_t count);
int vemb_v16_aeron_publish_batch_response(
    vemb_v16_channel_t *ch, const batch_response_t *response);

#endif
