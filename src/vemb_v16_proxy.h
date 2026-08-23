#ifndef __VEMB_V16_PROXY_H
#define __VEMB_V16_PROXY_H

#include "vemb_v16_aeron_attach.h"
#include "vemb_v16_protocol.h"
#include "vemb_v16_storage.h"

typedef struct vemb_v16_proxy vemb_v16_proxy_t;

int vemb_v16_proxy_create(vemb_v16_proxy_t **out,
                          uint32_t vector_dim,
                          uint32_t max_vectors,
                          vemb_v16_storage_ctx_t *storage,
                          const vemb_v16_warm_regions_manifest_t *manifest);
int vemb_v16_proxy_enable_tcp(vemb_v16_proxy_t *proxy,
                              const char *host,
                              uint16_t port);
int vemb_v16_proxy_enable_aeron_tcp_control(vemb_v16_proxy_t *proxy,
                                            const char *host,
                                            uint16_t port);
int vemb_v16_proxy_enable_aeron_tcp_inject_only(vemb_v16_proxy_t *proxy,
                                                const char *host,
                                                uint16_t port);
int vemb_v16_proxy_set_aeron_ub_path(vemb_v16_proxy_t *proxy,
                                     const char *ub_path);
int vemb_v16_proxy_set_aeron_response_ub_path(vemb_v16_proxy_t *proxy,
                                              const char *ub_path);
uint32_t vemb_v16_proxy_data_transport(vemb_v16_proxy_t *proxy);
int vemb_v16_proxy_enable_inject(vemb_v16_proxy_t *proxy);
int vemb_v16_proxy_enable_tcp_inject_only(vemb_v16_proxy_t *proxy,
                                          const char *host,
                                          uint16_t port);
int vemb_v16_proxy_inject_fd(vemb_v16_proxy_t *proxy, int fd);
int vemb_v16_proxy_set_proxy_io_threads(vemb_v16_proxy_t *proxy,
                                        uint32_t threads);
int vemb_v16_proxy_set_batch_request_size(vemb_v16_proxy_t *proxy,
                                          uint32_t size);
int vemb_v16_proxy_set_supernode_workers(vemb_v16_proxy_t *proxy,
                                         uint32_t workers);
void vemb_v16_proxy_destroy(vemb_v16_proxy_t *proxy);
int vemb_v16_proxy_run(vemb_v16_proxy_t *proxy);
void vemb_v16_proxy_stop(vemb_v16_proxy_t *proxy);
void vemb_v16_proxy_get_stats(vemb_v16_proxy_t *proxy, vemb_v16_stats_t *stats);
int vemb_v16_proxy_migration_mark_migrating(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_control_req_t *req,
    vemb_v16_migration_control_resp_t *resp);
int vemb_v16_proxy_migration_mark_cutover(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_control_req_t *req,
    vemb_v16_migration_control_resp_t *resp);
int vemb_v16_proxy_migration_mark_source_gc(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_control_req_t *req,
    vemb_v16_migration_control_resp_t *resp);
int vemb_v16_proxy_migration_barrier(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_control_req_t *req,
    vemb_v16_migration_control_resp_t *resp);
int vemb_v16_proxy_migration_mark_migrating_batch(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_control_batch_req_t *req,
    vemb_v16_migration_control_batch_resp_t *resp);
int vemb_v16_proxy_migration_range_barrier(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_range_control_req_t *req,
    vemb_v16_migration_range_control_resp_t *resp);
int vemb_v16_proxy_migration_range_mark_cutover(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_range_control_req_t *req,
    vemb_v16_migration_range_control_resp_t *resp);
int vemb_v16_proxy_migration_range_mark_source_gc(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_range_control_req_t *req,
    vemb_v16_migration_range_control_resp_t *resp);
int vemb_v16_proxy_epoch_set(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_epoch_control_req_t *req,
    vemb_v16_epoch_control_resp_t *resp);
int vemb_v16_proxy_epoch_get(
    vemb_v16_proxy_t *proxy,
    vemb_v16_epoch_control_resp_t *resp);
/* Topology-set callers must first fix the proxy data transport with one of
 * the enable_tcp/enable_aeron entry points. */
int vemb_v16_proxy_topology_set(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_topology_control_req_t *req,
    vemb_v16_topology_control_resp_t *resp);
int vemb_v16_proxy_topology_get(
    vemb_v16_proxy_t *proxy,
    vemb_v16_topology_control_resp_t *resp);
int vemb_v16_proxy_store_peer_view_map(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_peer_view_map_req_t *req,
    vemb_v16_peer_view_map_resp_t *resp);
int vemb_v16_proxy_apply_peer_view_map_and_topology_set(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_peer_view_topology_control_req_t *req,
    vemb_v16_peer_view_topology_control_resp_t *resp);

/* Aeron ATTACH support: advertise a local warm region using its server-side
 * path. Same-host clients open it directly; remote clients map UB paths. */
void vemb_v16_proxy_fill_attach_warm_region(
    vemb_v16_proxy_t *proxy,
    vemb_v16_aeron_attach_resp_t *resp);

int vemb_v16_proxy_attach_cross_node_batch_channel(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_aeron_batch_channel_allocation_t *allocation,
    uint32_t effective_batch_size, uint32_t max_batch_bytes,
    uint64_t *out_channel_id);

#endif
