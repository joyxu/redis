#define _GNU_SOURCE

#include "vemb_v16_client_topology.h"

#include "vemb_v16_net.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int owner_subset(const vemb_v16_topology_ring_t *subset,
                        const vemb_v16_topology_ring_t *superset) {
    for (uint32_t i = 0; i < subset->owner_count; i++) {
        if (!vemb_v16_topology_owner_exists(superset, subset->owners[i]))
            return 0;
    }
    return 1;
}

static int endpoint_string_valid(const char *s, size_t n) {
    return s && memchr(s, '\0', n) != NULL;
}

static int endpoint_valid(const vemb_v16_topology_endpoint_t *endpoint,
                          const vemb_v16_topology_ring_t *standby_ring) {
    if (!endpoint ||
        endpoint->owner_id == UINT32_MAX ||
        !vemb_v16_topology_owner_exists(standby_ring,
                                        endpoint->owner_id)) {
        return 0;
    }
    if (endpoint->transport_type == VEMB_V16_TRANSPORT_TCP) {
        return endpoint->tcp_port != 0 &&
               endpoint_string_valid(endpoint->host,
                                     sizeof(endpoint->host)) &&
               endpoint->host[0] != '\0';
    }
    if (endpoint->transport_type == VEMB_V16_TRANSPORT_AERON) {
        return endpoint->tcp_port != 0 &&
               endpoint_string_valid(endpoint->host,
                                     sizeof(endpoint->host)) &&
               endpoint->host[0] != '\0';
    }
    return 0;
}

int vemb_v16_client_topology_from_response(
    const vemb_v16_topology_control_resp_t *resp,
    vemb_v16_client_topology_t *topology) {
    if (!resp || !topology || resp->status != VEMB_V16_STATUS_OK ||
        resp->active_owner_count == 0 ||
        resp->standby_owner_count == 0 ||
        resp->active_owner_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS ||
        resp->standby_owner_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS ||
        resp->endpoint_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS ||
        resp->min_write_epoch > resp->current_topology_epoch) {
        return -1;
    }

    vemb_v16_client_topology_t candidate;
    memset(&candidate, 0, sizeof(candidate));
    candidate.current_topology_epoch = resp->current_topology_epoch;
    candidate.min_write_epoch = resp->min_write_epoch;
    candidate.flags = resp->flags;
    uint32_t vnode_count = resp->vnode_count ?
        resp->vnode_count : VEMB_V16_TOPOLOGY_DEFAULT_VNODES;
    if (vemb_v16_topology_ring_build(&candidate.active_ring,
                                     resp->current_topology_epoch,
                                     resp->active_owners,
                                     resp->active_owner_count,
                                     vnode_count) != VEMB_V16_TOPOLOGY_OK ||
        vemb_v16_topology_ring_build(&candidate.standby_ring,
                                     resp->current_topology_epoch,
                                     resp->standby_owners,
                                     resp->standby_owner_count,
                                     vnode_count) != VEMB_V16_TOPOLOGY_OK ||
        !owner_subset(&candidate.active_ring, &candidate.standby_ring)) {
        return -1;
    }
    for (uint32_t i = 0; i < resp->endpoint_count; i++) {
        if (!endpoint_valid(&resp->endpoints[i],
                            &candidate.standby_ring)) {
            return -1;
        }
    }
    candidate.endpoint_count = resp->endpoint_count;
    if (resp->endpoint_count > 0) {
        memcpy(candidate.endpoints,
               resp->endpoints,
               sizeof(resp->endpoints[0]) * resp->endpoint_count);
    }

    *topology = candidate;
    return 0;
}

int vemb_v16_client_topology_plan_write(
    const vemb_v16_client_topology_t *topology,
    uint64_t key_hash,
    vemb_v16_client_write_plan_t *plan) {
    if (!topology || !plan ||
        topology->active_ring.node_count == 0 ||
        topology->standby_ring.node_count == 0) {
        return -1;
    }

    uint32_t active_owner =
        vemb_v16_topology_ring_owner(&topology->active_ring, key_hash);
    uint32_t standby_owner =
        vemb_v16_topology_ring_owner(&topology->standby_ring, key_hash);
    if (active_owner == UINT32_MAX || standby_owner == UINT32_MAX)
        return -1;

    *plan = (vemb_v16_client_write_plan_t){
        .topology_epoch = topology->current_topology_epoch,
        .active_owner = active_owner,
        .standby_owner = standby_owner,
        .needs_dual_write =
            (topology->flags &
             VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED) &&
            active_owner != standby_owner,
    };
    return 0;
}

const vemb_v16_topology_endpoint_t *vemb_v16_client_topology_find_endpoint(
    const vemb_v16_client_topology_t *topology,
    uint32_t owner_id) {
    if (!topology)
        return NULL;
    for (uint32_t i = 0; i < topology->endpoint_count; i++) {
        if (topology->endpoints[i].owner_id == owner_id)
            return &topology->endpoints[i];
    }
    return NULL;
}

int vemb_v16_client_topology_fetch_tcp_fd(
    int fd,
    vemb_v16_client_topology_t *topology,
    vemb_v16_topology_control_resp_t *raw_resp) {
    if (fd < 0 || !topology)
        return -1;
    if (vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_TOPOLOGY_GET,
                                 0,
                                 0,
                                 0,
                                 NULL,
                                 0) != 0) {
        return -1;
    }
    vemb_v16_net_hdr_t hdr;
    vemb_v16_topology_control_resp_t resp;
    uint8_t *payload = NULL;
    memset(&resp, 0, sizeof(resp));
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_TOPOLOGY_RESPONSE ||
        hdr.flags != 0) {
        return -1;
    }
    payload = malloc(hdr.payload_len);
    if (!payload ||
        vemb_v16_net_read_full(fd, payload, hdr.payload_len) != 0 ||
        vemb_v16_topology_control_resp_decode(&resp,
                                              payload,
                                              hdr.payload_len) != 0) {
        free(payload);
        return -1;
    }
    free(payload);
    if (raw_resp)
        *raw_resp = resp;
    return vemb_v16_client_topology_from_response(&resp, topology);
}

int vemb_v16_client_topology_fetch_tcp(
    const char *host,
    uint16_t port,
    uint32_t timeout_ms,
    vemb_v16_client_topology_t *topology,
    vemb_v16_topology_control_resp_t *raw_resp) {
    int fd = vemb_v16_net_connect(host, port, timeout_ms);
    if (fd < 0)
        return -1;
    int rc = vemb_v16_client_topology_fetch_tcp_fd(fd, topology, raw_resp);
    close(fd);
    return rc;
}
