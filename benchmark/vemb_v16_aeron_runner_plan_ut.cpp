#include "../memtier_benchmark/vemb_v16_aeron_runner_plan.h"

#include <assert.h>
#include <stdio.h>

static void assert_plan(vemb_v16_aeron_runner_channel_plan plan,
                        bool batch, uint32_t legacy, uint32_t sessions) {
    assert(plan.batch_sessions_enabled == batch);
    assert(plan.legacy_channels == legacy);
    assert(plan.batch_sessions == sessions);
}

int main() {
    const uint32_t channels = 256;

    assert_plan(vemb_v16_aeron_runner_plan_channels(
                    true, 0, 1, false, false, false, channels),
                true, 0, channels);
    assert_plan(vemb_v16_aeron_runner_plan_channels(
                    false, 0, 1, false, false, false, channels),
                false, channels, 0);
    assert_plan(vemb_v16_aeron_runner_plan_channels(
                    true, 1, 1, false, false, false, channels),
                false, channels, 0);
    assert_plan(vemb_v16_aeron_runner_plan_channels(
                    true, 0, 1, true, false, false, channels),
                false, channels, 0);
    assert_plan(vemb_v16_aeron_runner_plan_channels(
                    true, 0, 1, false, true, false, channels),
                false, channels, 0);
    assert_plan(vemb_v16_aeron_runner_plan_channels(
                    true, 0, 1, false, false, true, channels),
                false, channels, 0);

    puts("vemb_v16_aeron_runner_plan_ut: all tests passed");
    return 0;
}
