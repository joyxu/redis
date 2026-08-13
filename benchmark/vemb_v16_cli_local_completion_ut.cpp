#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "../memtier_benchmark/vemb_v16_aeron_local_completion.h"

static vemb_v16_cli_l1_t *new_l1(void) {
    vemb_v16_cli_l1_config_t config = {2, 4, 0, 0};
    return vemb_v16_cli_l1_create(&config);
}

static void put_vector(vemb_v16_cli_l1_t *l1, const char *key,
                       uint64_t hash, float first) {
    const float vector[2] = {first, first + 0.5f};
    assert(vemb_v16_cli_l1_put(l1, key, (uint16_t)strlen(key), hash, vector,
                                sizeof(vector)) == VEMB_V16_CLI_L1_PUT_INSERTED);
}

static void test_fifo_and_queue_full(void) {
    vemb_v16_aeron_local_completion_queue queue;
    queue.init(2);
    vemb_v16_aeron_local_completion first = {
        .channel_index = 1,
        .req_id = 11,
        .caller_cookie = 111,
    };
    vemb_v16_aeron_local_completion second = {
        .channel_index = 2,
        .req_id = 22,
        .caller_cookie = 222,
    };
    assert(queue.push(first));
    assert(queue.push(second));
    assert(queue.full());
    assert(!queue.push(first));
    assert(queue.pop().req_id == 11);
    assert(queue.pop().req_id == 22);
    assert(queue.empty());
}

static void test_l1_pin_release_and_stale_generation(void) {
    vemb_v16_cli_l1_t *l1 = new_l1();
    assert(l1);
    put_vector(l1, "old", 0, 1.0f);

    vemb_v16_cli_l1_value_t value;
    assert(vemb_v16_cli_l1_lookup(l1, "old", 3, 0, &value) == 1);
    vemb_v16_aeron_local_completion_queue queue;
    queue.init(1);
    assert(queue.push({
        .channel_index = 0,
        .req_id = 1,
        .caller_cookie = 1,
        .vector = (const float *)value.vector,
        .vector_bytes = value.vector_bytes,
        .ref = value.ref,
    }));
    vemb_v16_aeron_local_completion completion = queue.pop();
    assert(completion.vector_bytes == 2 * sizeof(float));
    assert(completion.vector[0] == 1.0f);
    assert(vemb_v16_cli_l1_release(l1, &completion.ref) == 0);

    vemb_v16_cli_l1_ref_t stale_ref = completion.ref;
    vemb_v16_cli_l1_clear(l1);
    put_vector(l1, "new", 0, 2.0f);
    assert(vemb_v16_cli_l1_pin(l1, &stale_ref) == -1);
    vemb_v16_cli_l1_destroy(l1);
}

static void test_remote_batch_wave_holds_refill_until_remote_completion(void) {
    vemb_v16_aeron_remote_batch_wave wave;
    assert(!wave.active());

    wave.seal(31);
    assert(wave.active());
    assert(wave.pending_count() == 31);
    for (uint32_t i = 0; i < 30; i++)
        wave.complete_one();
    assert(wave.active());
    assert(wave.pending_count() == 1);

    wave.complete_one();
    assert(!wave.active());
    assert(wave.pending_count() == 0);
}

int main(void) {
    test_fifo_and_queue_full();
    test_l1_pin_release_and_stale_generation();
    test_remote_batch_wave_holds_refill_until_remote_completion();
    printf("vemb_v16_cli_local_completion_ut: all tests passed\n");
    return 0;
}
