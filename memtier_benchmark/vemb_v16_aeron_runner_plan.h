/*
 * Copyright (C) 2026 Redis Labs Ltd.
 *
 * This file is part of memtier_benchmark.
 */

#ifndef VEMB_V16_AERON_RUNNER_PLAN_H
#define VEMB_V16_AERON_RUNNER_PLAN_H

#include <stdint.h>

struct vemb_v16_aeron_runner_channel_plan {
    bool batch_sessions_enabled;
    uint32_t legacy_channels;
    uint32_t batch_sessions;
};

static inline vemb_v16_aeron_runner_channel_plan
vemb_v16_aeron_runner_plan_channels(bool cross_node, uint32_t set_ratio,
                                    uint32_t get_ratio, bool vsim, bool vrem,
                                    bool batch_disabled, uint32_t total_channels) {
    bool batch_eligible = cross_node && get_ratio > 0 &&
        set_ratio == 0 && !vsim && !vrem && !batch_disabled;
    bool batch_sessions_enabled = batch_eligible;
    return {
        batch_sessions_enabled,
        batch_eligible ? 0u : total_channels,
        batch_eligible ? total_channels : 0u,
    };
}

#endif /* VEMB_V16_AERON_RUNNER_PLAN_H */
