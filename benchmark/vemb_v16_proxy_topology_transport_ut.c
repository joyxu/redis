#include "vemb_v16_proxy_internal.h"
#include "vemb_v16_proxy_types.h"

#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void fill_mismatched_topology(vemb_v16_topology_control_req_t *req) {
    memset(req, 0, sizeof(*req));
    req->current_topology_epoch = 1;
    req->min_write_epoch = 1;
    req->vnode_count = VEMB_V16_TOPOLOGY_DEFAULT_VNODES;
    req->active_owner_count = 1;
    req->standby_owner_count = 1;
    req->active_owners[0] = 0;
    req->standby_owners[0] = 0;
    req->endpoint_count = 1;
    req->endpoints[0].owner_id = 0;
    req->endpoints[0].transport_type = VEMB_V16_TRANSPORT_AERON;
    req->endpoints[0].tcp_port = 6399;
    snprintf(req->endpoints[0].host, sizeof(req->endpoints[0].host),
             "%s", "127.0.0.1");
}

int main(void) {
    vemb_v16_proxy_t *proxy = calloc(1, sizeof(*proxy));
    assert(proxy);

    vemb_v16_storage_ctx_t storage = {0};
    pthread_mutex_init(&storage.topology_lock, NULL);
    atomic_init(&storage.current_topology_epoch, 0);
    atomic_init(&storage.min_write_epoch, 0);
    atomic_init(&storage.owner_resolver_active_snapshot, 0);
    storage.local_owner_id = 0;
    proxy->storage = &storage;
    assert(vemb_v16_proxy_enable_tcp(proxy, "127.0.0.1", 6399) == 0);

    vemb_v16_topology_control_req_t topology_req;
    vemb_v16_topology_control_resp_t topology_resp = {0};
    fill_mismatched_topology(&topology_req);
    assert(vemb_v16_proxy_topology_set(proxy, &topology_req,
                                       &topology_resp) != 0);
    assert(topology_resp.status == VEMB_V16_STATUS_ERR);
    assert(topology_resp.current_topology_epoch == 0);
    assert(!storage.published_topology_valid);

    vemb_v16_peer_view_topology_control_req_t combined_req = {0};
    vemb_v16_peer_view_topology_control_resp_t combined_resp = {0};
    combined_req.topology_req = topology_req;
    assert(vemb_v16_proxy_apply_peer_view_map_and_topology_set(
               proxy, &combined_req, &combined_resp) != 0);
    assert(combined_resp.status == VEMB_V16_STATUS_ERR);
    assert(combined_resp.peer_view_map_status == VEMB_V16_STATUS_ERR);
    assert(combined_resp.topology_status == VEMB_V16_STATUS_ERR);
    assert(!combined_resp.topology_attempted);
    assert(!storage.published_topology_valid);

    pthread_mutex_destroy(&storage.topology_lock);
    free(proxy);
    puts("vemb_v16_proxy_topology_transport_ut: all tests passed");
    return 0;
}
