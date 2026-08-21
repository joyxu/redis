#include "../clients/c/vemb_v16_cluster_core.h"
#include "../src/vemb_v16_hash.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

static vemb_v16_client_topology_t make_topology(uint64_t epoch)
{
    const uint32_t owners[] = {0, 1};
    vemb_v16_topology_control_resp_t response;
    vemb_v16_client_topology_t topology;

    memset(&response, 0, sizeof(response));
    response.status = VEMB_V16_STATUS_OK;
    response.current_topology_epoch = epoch;
    response.min_write_epoch = epoch;
    response.vnode_count = VEMB_V16_TOPOLOGY_DEFAULT_VNODES;
    response.active_owner_count = 2;
    response.standby_owner_count = 2;
    response.endpoint_count = 2;
    memcpy(response.active_owners, owners, sizeof(owners));
    memcpy(response.standby_owners, owners, sizeof(owners));
    for (uint32_t i = 0; i < response.endpoint_count; i++) {
        response.endpoints[i].owner_id = i;
        response.endpoints[i].transport_type = VEMB_V16_TRANSPORT_TCP;
        response.endpoints[i].tcp_port = (uint16_t)(6400 + i);
        snprintf(response.endpoints[i].host,
                 sizeof(response.endpoints[i].host), "127.0.0.%u", i + 1);
    }
    assert(vemb_v16_client_topology_from_response(&response, &topology) == 0);
    return topology;
}

static void test_single_owner_route_and_completion(void)
{
    vemb_v16_cluster_core_t core;
    vemb_v16_cluster_operation_t operation;
    vemb_v16_cluster_operation_t second_operation;
    vemb_v16_cluster_route_t route;
    vemb_v16_resp_t response = {.status = VEMB_V16_STATUS_OK};

    vemb_v16_cluster_core_init(&core, 0, 1);
    vemb_v16_cluster_operation_init(&core, &operation, 42);
    vemb_v16_cluster_operation_init(&core, &second_operation, 43);
    assert(operation.operation_id != 0);
    assert(second_operation.operation_id != operation.operation_id);
    assert(vemb_v16_cluster_core_prepare(&core, &operation, &route) ==
           VEMB_V16_CLUSTER_PREPARE_READY);
    assert(route.owner_id == 0);
    assert(route.topology_epoch == 0);
    assert(vemb_v16_cluster_core_on_response(&core, &operation, &response) ==
           VEMB_V16_CLUSTER_RESPONSE_FINAL);
    vemb_v16_cluster_core_complete(&core, &operation, response.status);
    assert(vemb_v16_cluster_core_stats(&core)->logical_successes == 1);
}

static void test_dual_owner_retry_state_machine(void)
{
    vemb_v16_cluster_core_t core;
    vemb_v16_cluster_operation_t operation;
    vemb_v16_cluster_route_t route;
    vemb_v16_client_topology_t topology = make_topology(9);
    vemb_v16_resp_t response;

    vemb_v16_cluster_core_init(&core, 1, 2);
    vemb_v16_cluster_operation_init(&core, &operation, 0x12345678ULL);
    assert(vemb_v16_cluster_core_prepare(&core, &operation, &route) ==
           VEMB_V16_CLUSTER_PREPARE_NEEDS_TOPOLOGY);

    vemb_v16_cluster_core_publish_topology(&core, &topology);
    assert(vemb_v16_cluster_core_prepare(&core, &operation, &route) ==
           VEMB_V16_CLUSTER_PREPARE_READY);
    assert(route.topology_epoch == 9);
    assert(route.owner_id < 2);
    uint32_t active_owner = route.owner_id;
    assert(!vemb_v16_cluster_core_owner_channel_ready(&core,
                                                       active_owner));
    vemb_v16_cluster_core_owner_channel_opened(&core, active_owner);
    assert(vemb_v16_cluster_core_owner_channel_ready(&core,
                                                      active_owner));
    assert(vemb_v16_cluster_core_stats(&core)->topology_refresh_calls == 1);

    response = (vemb_v16_resp_t){
        .status = VEMB_V16_STATUS_ASK,
        .redirect_owner = route.owner_id == 0 ? 1 : 0,
    };
    assert(vemb_v16_cluster_core_on_response(&core, &operation, &response) ==
           VEMB_V16_CLUSTER_RESPONSE_ASK_RETRY);
    assert(vemb_v16_cluster_core_prepare(&core, &operation, &route) ==
           VEMB_V16_CLUSTER_PREPARE_READY);
    assert(route.request_flags == VEMB_V16_REQ_F_ASK_REDIRECT);
    assert(route.owner_id == response.redirect_owner);
    assert(route.topology_epoch == 9);

    response = (vemb_v16_resp_t){.status = VEMB_V16_STATUS_MOVED};
    assert(vemb_v16_cluster_core_on_response(&core, &operation, &response) ==
           VEMB_V16_CLUSTER_RESPONSE_REFRESH);
    assert(!vemb_v16_cluster_core_topology_ready(&core));
    assert(vemb_v16_cluster_core_stats(&core)->moved_redirects == 1);

    topology = make_topology(10);
    vemb_v16_cluster_core_publish_topology(&core, &topology);
    assert(vemb_v16_cluster_core_owner_channel_ready(&core,
                                                      active_owner));

    assert(vemb_v16_cluster_core_prepare(&core, &operation, &route) ==
           VEMB_V16_CLUSTER_PREPARE_READY);
    assert(route.topology_epoch == 10);
    assert(route.owner_id == active_owner);
    assert(route.owner_channel_generation == 1);
    vemb_v16_cluster_core_owner_channel_closed(&core, active_owner);
    assert(!vemb_v16_cluster_core_owner_channel_ready(&core, active_owner));
    vemb_v16_cluster_core_owner_channel_opened(&core, active_owner);
    assert(vemb_v16_cluster_core_owner_channel_ready(&core, active_owner));
    response = (vemb_v16_resp_t){.status = VEMB_V16_STATUS_STALE_TOPOLOGY};
    assert(vemb_v16_cluster_core_on_response(&core, &operation, &response) ==
           VEMB_V16_CLUSTER_RESPONSE_REFRESH);
    assert(vemb_v16_cluster_core_stats(&core)->stale_topology_responses == 1);

    topology = make_topology(11);
    vemb_v16_cluster_core_publish_topology(&core, &topology);
    assert(vemb_v16_cluster_core_prepare(&core, &operation, &route) ==
           VEMB_V16_CLUSTER_PREPARE_RETRY_EXHAUSTED);
    vemb_v16_cluster_core_complete(&core, &operation, VEMB_V16_STATUS_ERR);
    assert(vemb_v16_cluster_core_stats(&core)->retry_exhaustions == 1);
    assert(vemb_v16_cluster_core_stats(&core)->logical_errors == 1);
}

static void test_dual_owner_canonical_hash_distribution(void)
{
    vemb_v16_client_topology_t topology = make_topology(12);
    uint32_t routed[2] = {0};

    for (uint32_t i = 0; i < 4096; i++) {
        char key[64];
        vemb_v16_client_write_plan_t plan;
        int key_len = snprintf(key, sizeof(key), "cluster-route:%u", i);

        assert(key_len > 0 && (size_t)key_len < sizeof(key));
        assert(vemb_v16_client_topology_plan_write(
                   &topology,
                   vemb_v16_xxh3_64_str(key, (size_t)key_len),
                   &plan) == 0);
        assert(plan.active_owner < 2);
        routed[plan.active_owner]++;
    }

    assert(routed[0] != 0);
    assert(routed[1] != 0);
}

int main(void)
{
    test_single_owner_route_and_completion();
    test_dual_owner_retry_state_machine();
    test_dual_owner_canonical_hash_distribution();
    printf("vemb_v16_cluster_core_ut: all tests passed\n");
    return 0;
}
