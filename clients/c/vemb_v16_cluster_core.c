#include "vemb_v16_cluster_core.h"

#include <stdio.h>

void vemb_v16_cluster_core_init(vemb_v16_cluster_core_t *core,
                                int topology_enabled,
                                uint32_t retry_budget)
{
    *core = (vemb_v16_cluster_core_t){
        .retry_budget = retry_budget,
        .next_operation_id = 1,
        .topology_enabled = topology_enabled != 0,
        .topology_state = VEMB_V16_CLUSTER_TOPOLOGY_UNINITIALIZED,
    };
}

void vemb_v16_cluster_core_set_retry_budget(vemb_v16_cluster_core_t *core,
                                            uint32_t retry_budget)
{
    core->retry_budget = retry_budget;
}

void vemb_v16_cluster_core_enable_topology(vemb_v16_cluster_core_t *core)
{
    core->topology_enabled = 1;
    if (core->topology_state != VEMB_V16_CLUSTER_TOPOLOGY_READY)
        core->topology_state = VEMB_V16_CLUSTER_TOPOLOGY_UNINITIALIZED;
}

void vemb_v16_cluster_core_publish_topology(
    vemb_v16_cluster_core_t *core,
    const vemb_v16_client_topology_t *topology)
{
    core->topology = *topology;
    core->topology_state = VEMB_V16_CLUSTER_TOPOLOGY_READY;
    core->stats.topology_refresh_calls++;
}

void vemb_v16_cluster_core_mark_topology_stale(vemb_v16_cluster_core_t *core)
{
    if (core->topology_enabled)
        core->topology_state = VEMB_V16_CLUSTER_TOPOLOGY_STALE;
}

int vemb_v16_cluster_core_topology_ready(
    const vemb_v16_cluster_core_t *core)
{
    return !core->topology_enabled ||
        core->topology_state == VEMB_V16_CLUSTER_TOPOLOGY_READY;
}

const vemb_v16_client_topology_t *vemb_v16_cluster_core_topology(
    const vemb_v16_cluster_core_t *core)
{
    return &core->topology;
}

void vemb_v16_cluster_operation_init(vemb_v16_cluster_core_t *core,
                                     vemb_v16_cluster_operation_t *operation,
                                     uint64_t key_hash)
{
    *operation = (vemb_v16_cluster_operation_t){
        .operation_id = core->next_operation_id++,
        .key_hash = key_hash,
    };
    if (core->next_operation_id == 0)
        core->next_operation_id = 1;
}

vemb_v16_cluster_prepare_result_t vemb_v16_cluster_core_prepare(
    vemb_v16_cluster_core_t *core,
    vemb_v16_cluster_operation_t *operation,
    vemb_v16_cluster_route_t *route)
{
    if (operation->ask_retry_pending) {
        operation->ask_retry_pending = 0;
        *route = (vemb_v16_cluster_route_t){
            .topology_epoch = operation->topology_epoch,
            .owner_channel_generation =
                core->owner_channels[operation->target_owner].generation,
            .owner_id = operation->target_owner,
            .request_flags = VEMB_V16_REQ_F_ASK_REDIRECT,
        };
        return VEMB_V16_CLUSTER_PREPARE_READY;
    }

    if (!vemb_v16_cluster_core_topology_ready(core))
        return VEMB_V16_CLUSTER_PREPARE_NEEDS_TOPOLOGY;

    if (operation->attempts == core->retry_budget) {
        if (!operation->exhausted) {
            operation->exhausted = 1;
            core->stats.retry_exhaustions++;
        }
        return VEMB_V16_CLUSTER_PREPARE_RETRY_EXHAUSTED;
    }

    vemb_v16_client_write_plan_t plan;
    if (core->topology_enabled) {
        if (vemb_v16_client_topology_plan_write(&core->topology,
                                                operation->key_hash,
                                                &plan) != 0) {
            return VEMB_V16_CLUSTER_PREPARE_NEEDS_TOPOLOGY;
        }
        operation->target_owner = plan.active_owner;
        operation->topology_epoch = plan.topology_epoch;
    } else {
        operation->target_owner = 0;
        operation->topology_epoch = 0;
    }
    operation->attempts++;
    *route = (vemb_v16_cluster_route_t){
        .topology_epoch = operation->topology_epoch,
        .owner_channel_generation =
            core->owner_channels[operation->target_owner].generation,
        .owner_id = operation->target_owner,
        .request_flags = 0,
    };
    return VEMB_V16_CLUSTER_PREPARE_READY;
}

vemb_v16_cluster_response_action_t vemb_v16_cluster_core_on_response(
    vemb_v16_cluster_core_t *core,
    vemb_v16_cluster_operation_t *operation,
    const vemb_v16_resp_t *response)
{
    switch (response->status) {
    case VEMB_V16_STATUS_OK:
    case VEMB_V16_STATUS_NOT_FOUND:
    case VEMB_V16_STATUS_ERR:
        return VEMB_V16_CLUSTER_RESPONSE_FINAL;
    case VEMB_V16_STATUS_ASK:
        if (response->redirect_owner >= VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS)
            return VEMB_V16_CLUSTER_RESPONSE_FINAL;
        if (!operation->ask_retry_used) {
            operation->ask_retry_used = 1;
            operation->ask_retry_pending = 1;
            operation->target_owner = response->redirect_owner;
            core->stats.ask_redirects++;
            return VEMB_V16_CLUSTER_RESPONSE_ASK_RETRY;
        }
        vemb_v16_cluster_core_mark_topology_stale(core);
        return VEMB_V16_CLUSTER_RESPONSE_REFRESH;
    case VEMB_V16_STATUS_MOVED:
        core->stats.moved_redirects++;
        if (core->stats.moved_redirects <= 8 ||
            core->stats.moved_redirects % 1024 == 0) {
            fprintf(stderr,
                    "[cluster] MOVED #%llu key_hash=%llu from_owner=%u "
                    "to_owner=%u request_epoch=%llu\n",
                    (unsigned long long)core->stats.moved_redirects,
                    (unsigned long long)operation->key_hash,
                    operation->target_owner, response->redirect_owner,
                    (unsigned long long)operation->topology_epoch);
        }
        vemb_v16_cluster_core_mark_topology_stale(core);
        return VEMB_V16_CLUSTER_RESPONSE_REFRESH;
    case VEMB_V16_STATUS_STALE_TOPOLOGY:
        core->stats.stale_topology_responses++;
        if (core->stats.stale_topology_responses <= 8 ||
            core->stats.stale_topology_responses % 1024 == 0) {
            fprintf(stderr,
                    "[cluster] STALE_TOPOLOGY #%llu key_hash=%llu "
                    "owner=%u request_epoch=%llu\n",
                    (unsigned long long)core->stats.stale_topology_responses,
                    (unsigned long long)operation->key_hash,
                    operation->target_owner,
                    (unsigned long long)operation->topology_epoch);
        }
        vemb_v16_cluster_core_mark_topology_stale(core);
        return VEMB_V16_CLUSTER_RESPONSE_REFRESH;
    default:
        return VEMB_V16_CLUSTER_RESPONSE_FINAL;
    }
}

void vemb_v16_cluster_core_complete(vemb_v16_cluster_core_t *core,
                                    vemb_v16_cluster_operation_t *operation,
                                    uint8_t final_status)
{
    operation->completed = 1;
    if (final_status == VEMB_V16_STATUS_OK)
        core->stats.logical_successes++;
    else if (final_status == VEMB_V16_STATUS_NOT_FOUND)
        core->stats.logical_not_found++;
    else
        core->stats.logical_errors++;
}

int vemb_v16_cluster_core_owner_channel_ready(
    const vemb_v16_cluster_core_t *core,
    uint32_t owner_id)
{
    return core->owner_channels[owner_id].ready;
}

void vemb_v16_cluster_core_owner_channel_opened(
    vemb_v16_cluster_core_t *core,
    uint32_t owner_id)
{
    vemb_v16_cluster_owner_channel_t *channel =
        &core->owner_channels[owner_id];
    channel->ready = 1;
    channel->generation++;
}

void vemb_v16_cluster_core_owner_channel_closed(
    vemb_v16_cluster_core_t *core,
    uint32_t owner_id)
{
    core->owner_channels[owner_id].ready = 0;
}

const vemb_v16_cluster_stats_t *vemb_v16_cluster_core_stats(
    const vemb_v16_cluster_core_t *core)
{
    return &core->stats;
}
