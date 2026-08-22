#ifndef __VEMB_V16_CLIENT_TOPOLOGY_H
#define __VEMB_V16_CLIENT_TOPOLOGY_H

#include "vemb_v16_protocol.h"
#include "vemb_v16_topology.h"

#include <stdint.h>

typedef struct vemb_v16_client_topology {
    uint64_t current_topology_epoch;
    uint64_t min_write_epoch;
    uint32_t flags;
    uint32_t endpoint_count;
    vemb_v16_topology_ring_t active_ring;
    vemb_v16_topology_ring_t standby_ring;
    vemb_v16_topology_endpoint_t
        endpoints[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];
} vemb_v16_client_topology_t;

typedef struct vemb_v16_client_write_plan {
    uint64_t topology_epoch;
    uint32_t active_owner;
    uint32_t standby_owner;
    uint32_t needs_dual_write;
} vemb_v16_client_write_plan_t;

int vemb_v16_client_topology_from_response(
    const vemb_v16_topology_control_resp_t *resp,
    vemb_v16_client_topology_t *topology);
int vemb_v16_client_topology_plan_write(
    const vemb_v16_client_topology_t *topology,
    uint64_t key_hash,
    vemb_v16_client_write_plan_t *plan);
const vemb_v16_topology_endpoint_t *vemb_v16_client_topology_find_endpoint(
    const vemb_v16_client_topology_t *topology,
    uint32_t owner_id);
int vemb_v16_client_topology_fetch_tcp_fd(
    int fd,
    vemb_v16_client_topology_t *topology,
    vemb_v16_topology_control_resp_t *raw_resp);
int vemb_v16_client_topology_fetch_tcp(
    const char *host,
    uint16_t port,
    uint32_t timeout_ms,
    vemb_v16_client_topology_t *topology,
    vemb_v16_topology_control_resp_t *raw_resp);
#endif
