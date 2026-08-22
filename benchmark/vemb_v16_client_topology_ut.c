#include "../src/vemb_v16_client_topology.h"

#include "../src/vemb_v16_hash.h"
#include "../src/vemb_v16_net.h"

#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

typedef struct fake_topology_server {
    int fd;
    vemb_v16_topology_control_resp_t resp;
} fake_topology_server_t;

static void fill_resp(vemb_v16_topology_control_resp_t *resp,
                      uint32_t flags) {
    const uint32_t active_owners[] = {0, 1};
    const uint32_t standby_owners[] = {0, 1, 2};
    memset(resp, 0, sizeof(*resp));
    resp->status = VEMB_V16_STATUS_OK;
    resp->current_topology_epoch = 9;
    resp->min_write_epoch = 9;
    resp->vnode_count = VEMB_V16_TOPOLOGY_DEFAULT_VNODES;
    resp->flags = flags;
    resp->active_owner_count = 2;
    resp->standby_owner_count = 3;
    memcpy(resp->active_owners,
           active_owners,
           sizeof(active_owners));
    memcpy(resp->standby_owners,
           standby_owners,
           sizeof(standby_owners));
    resp->endpoint_count = 3;
    for (uint32_t i = 0; i < resp->endpoint_count; i++) {
        resp->endpoints[i].owner_id = i;
        resp->endpoints[i].transport_type = VEMB_V16_TRANSPORT_TCP;
        resp->endpoints[i].tcp_port = (uint16_t)(6400 + i);
        snprintf(resp->endpoints[i].host,
                 sizeof(resp->endpoints[i].host),
                 "127.0.0.%u",
                 i + 1);
    }
}

static int find_dual_write_plan(
        const vemb_v16_client_topology_t *topology,
        vemb_v16_client_write_plan_t *plan) {
    for (uint32_t i = 0; i < 100000; i++) {
        char key[64];
        snprintf(key, sizeof(key), "client-topology:%u", i);
        uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
        assert(vemb_v16_client_topology_plan_write(topology,
                                                   key_hash,
                                                   plan) == 0);
        if (plan->needs_dual_write)
            return 0;
    }
    return -1;
}

static void assert_has_dual_write_plan(
        const vemb_v16_client_topology_t *topology) {
    vemb_v16_client_write_plan_t plan;
    memset(&plan, 0, sizeof(plan));
    assert(find_dual_write_plan(topology, &plan) == 0);
    assert(plan.topology_epoch == 9);
    assert(plan.active_owner != plan.standby_owner);
    assert(plan.standby_owner == 2);
}

static void *fake_tcp_topology_server(void *arg) {
    fake_topology_server_t *server = arg;
    vemb_v16_net_hdr_t hdr;
    uint8_t payload[2048];
    size_t payload_len = 0;
    assert(vemb_v16_net_read_header(server->fd, &hdr) == 0);
    assert(hdr.type == VEMB_V16_NET_TOPOLOGY_GET);
    assert(hdr.payload_len == 0);
    assert(vemb_v16_topology_control_resp_encode(payload,
                                                  sizeof(payload),
                                                  &server->resp,
                                                  &payload_len) == 0);
    assert(vemb_v16_net_write_frame(server->fd,
                                    VEMB_V16_NET_TOPOLOGY_RESPONSE,
                                    0,
                                    0,
                                    0,
                                    payload,
                                    (uint32_t)payload_len) == 0);
    close(server->fd);
    return NULL;
}

static void test_topology_from_response_and_plan(void) {
    vemb_v16_topology_control_resp_t resp;
    vemb_v16_client_topology_t topology;
    vemb_v16_client_write_plan_t plan;

    fill_resp(&resp, VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED);
    assert(vemb_v16_client_topology_from_response(&resp, &topology) == 0);
    assert(topology.current_topology_epoch == 9);
    assert(topology.min_write_epoch == 9);
    assert(topology.active_ring.owner_count == 2);
    assert(topology.standby_ring.owner_count == 3);
    assert(topology.endpoint_count == 3);
    const vemb_v16_topology_endpoint_t *endpoint =
        vemb_v16_client_topology_find_endpoint(&topology, 2);
    assert(endpoint != NULL);
    assert(endpoint->transport_type == VEMB_V16_TRANSPORT_TCP);
    assert(endpoint->tcp_port == 6402);
    assert(strcmp(endpoint->host, "127.0.0.3") == 0);
    assert_has_dual_write_plan(&topology);

    fill_resp(&resp, 0);
    assert(vemb_v16_client_topology_from_response(&resp, &topology) == 0);
    assert(find_dual_write_plan(&topology, &plan) != 0);

    fill_resp(&resp, VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED);
    resp.status = VEMB_V16_STATUS_ERR;
    assert(vemb_v16_client_topology_from_response(&resp, &topology) != 0);

    fill_resp(&resp, VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED);
    resp.active_owners[1] = 42;
    assert(vemb_v16_client_topology_from_response(&resp, &topology) != 0);

    fill_resp(&resp, VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED);
    resp.min_write_epoch = resp.current_topology_epoch + 1;
    assert(vemb_v16_client_topology_from_response(&resp, &topology) != 0);

    fill_resp(&resp, VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED);
    resp.endpoints[2].owner_id = 42;
    assert(vemb_v16_client_topology_from_response(&resp, &topology) != 0);
}

static void test_fetch_tcp_fd(void) {
    int sv[2];
    pthread_t thread;
    vemb_v16_client_topology_t topology;
    vemb_v16_topology_control_resp_t raw_resp;
    fake_topology_server_t server;

    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
    server.fd = sv[1];
    fill_resp(&server.resp, VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED);
    assert(pthread_create(&thread, NULL, fake_tcp_topology_server, &server) == 0);
    assert(vemb_v16_client_topology_fetch_tcp_fd(sv[0],
                                                 &topology,
                                                 &raw_resp) == 0);
    assert(pthread_join(thread, NULL) == 0);
    close(sv[0]);
    assert(raw_resp.current_topology_epoch == 9);
    assert_has_dual_write_plan(&topology);
}

int main(void) {
    test_topology_from_response_and_plan();
    test_fetch_tcp_fd();
    printf("vemb_v16_client_topology_ut: all tests passed\n");
    return 0;
}
