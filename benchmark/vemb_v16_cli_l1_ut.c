#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "../clients/c/internal/vemb_v16_cli_l1.h"

static vemb_v16_cli_l1_t *new_l1(uint32_t entry_count,
                                  uint32_t key_slot_count,
                                  uint32_t vector_slot_count) {
    return vemb_v16_cli_l1_create(&(vemb_v16_cli_l1_config_t){
        .dim = 2,
        .entry_count = entry_count,
        .key_slot_count = key_slot_count,
        .vector_slot_count = vector_slot_count,
    });
}

static void put_vector(vemb_v16_cli_l1_t *l1, const char *key,
                       uint64_t hash, float first) {
    const float vector[2] = {first, first + 0.5f};
    assert(vemb_v16_cli_l1_put(l1, key, (uint16_t)strlen(key), hash, vector,
                                sizeof(vector)) == VEMB_V16_CLI_L1_PUT_INSERTED);
}

static void test_exact_key_collision(void) {
    vemb_v16_cli_l1_t *l1 = new_l1(4, 0, 0);
    assert(l1);
    const uint64_t hash = UINT64_C(0xab00000000000000);
    put_vector(l1, "key-a", hash, 1.0f);
    put_vector(l1, "key-b", hash, 2.0f);

    vemb_v16_cli_l1_value_t value;
    assert(vemb_v16_cli_l1_lookup(l1, "key-a", 5, hash, &value) == 1);
    assert(value.vector_bytes == 2 * sizeof(float));
    assert(((const float *)value.vector)[0] == 1.0f);
    assert(vemb_v16_cli_l1_release(l1, &value.ref) == 0);
    assert(vemb_v16_cli_l1_lookup(l1, "key-c", 5, hash, &value) == 0);

    vemb_v16_cli_l1_stats_t stats;
    vemb_v16_cli_l1_get_stats(l1, &stats);
    assert(stats.hits == 1 && stats.misses == 1 && stats.inserts == 2);
    assert(stats.exact_key_mismatch >= 2);
    vemb_v16_cli_l1_destroy(l1);
}

static void test_clock_eviction(void) {
    vemb_v16_cli_l1_t *l1 = new_l1(4, 0, 0);
    assert(l1);
    for (uint32_t i = 0; i < 4; i++) {
        char key[8];
        int n = snprintf(key, sizeof(key), "key%u", i);
        assert(n == 4);
        put_vector(l1, key, i, (float)i);
    }
    put_vector(l1, "key4", 4, 4.0f);

    vemb_v16_cli_l1_value_t value;
    assert(vemb_v16_cli_l1_lookup(l1, "key0", 4, 0, &value) == 0);
    assert(vemb_v16_cli_l1_lookup(l1, "key1", 4, 1, &value) == 1);
    assert(vemb_v16_cli_l1_release(l1, &value.ref) == 0);
    vemb_v16_cli_l1_stats_t stats;
    vemb_v16_cli_l1_get_stats(l1, &stats);
    assert(stats.inserts == 5 && stats.evicts == 1 && stats.live_entries == 4);
    assert(stats.live_vector_bytes == 4 * 2 * sizeof(float));
    vemb_v16_cli_l1_destroy(l1);
}

static void test_all_pinned_and_generation_aba(void) {
    vemb_v16_cli_l1_t *l1 = new_l1(4, 0, 0);
    assert(l1);
    vemb_v16_cli_l1_value_t values[4];
    for (uint32_t i = 0; i < 4; i++) {
        char key[8];
        int n = snprintf(key, sizeof(key), "pin%u", i);
        assert(n == 4);
        put_vector(l1, key, i, (float)i);
        assert(vemb_v16_cli_l1_lookup(l1, key, (uint16_t)n, i,
                                       &values[i]) == 1);
    }
    const float replacement[2] = {9.0f, 9.5f};
    assert(vemb_v16_cli_l1_put(l1, "next", 4, 4, replacement,
                                sizeof(replacement)) ==
           VEMB_V16_CLI_L1_PUT_ALL_PINNED);
    for (uint32_t i = 0; i < 4; i++)
        assert(vemb_v16_cli_l1_release(l1, &values[i].ref) == 0);

    vemb_v16_cli_l1_ref_t old_ref = values[0].ref;
    assert(vemb_v16_cli_l1_put(l1, "next", 4, 4, replacement,
                                sizeof(replacement)) ==
           VEMB_V16_CLI_L1_PUT_INSERTED);
    assert(vemb_v16_cli_l1_pin(l1, &old_ref) == -1);
    assert(vemb_v16_cli_l1_release(l1, &old_ref) == -1);

    vemb_v16_cli_l1_stats_t stats;
    vemb_v16_cli_l1_get_stats(l1, &stats);
    assert(stats.all_pinned == 1 && stats.evicts == 1 && stats.stale_ref == 2);
    vemb_v16_cli_l1_destroy(l1);
}

static void test_storage_exhaustion(void) {
    vemb_v16_cli_l1_t *key_l1 = new_l1(4, 1, 4);
    assert(key_l1);
    put_vector(key_l1, "one", 1, 1.0f);
    const float vector[2] = {2.0f, 2.5f};
    assert(vemb_v16_cli_l1_put(key_l1, "two", 2, 2, vector,
                                sizeof(vector)) ==
           VEMB_V16_CLI_L1_PUT_KEY_STORAGE_EXHAUSTED);
    vemb_v16_cli_l1_stats_t stats;
    vemb_v16_cli_l1_get_stats(key_l1, &stats);
    assert(stats.key_storage_exhausted == 1 && stats.live_entries == 1);
    vemb_v16_cli_l1_destroy(key_l1);

    vemb_v16_cli_l1_t *vector_l1 = new_l1(4, 4, 1);
    assert(vector_l1);
    put_vector(vector_l1, "one", 1, 1.0f);
    assert(vemb_v16_cli_l1_put(vector_l1, "two", 2, 2, vector,
                                sizeof(vector)) ==
           VEMB_V16_CLI_L1_PUT_VECTOR_STORAGE_EXHAUSTED);
    vemb_v16_cli_l1_get_stats(vector_l1, &stats);
    assert(stats.vector_storage_exhausted == 1 && stats.live_entries == 1);
    vemb_v16_cli_l1_destroy(vector_l1);
}

static void test_clear_invalidates_refs(void) {
    vemb_v16_cli_l1_t *l1 = new_l1(4, 0, 0);
    assert(l1);
    put_vector(l1, "old", 1, 1.0f);
    vemb_v16_cli_l1_value_t value;
    assert(vemb_v16_cli_l1_lookup(l1, "old", 3, 1, &value) == 1);
    assert(vemb_v16_cli_l1_release(l1, &value.ref) == 0);
    vemb_v16_cli_l1_clear(l1);
    assert(vemb_v16_cli_l1_pin(l1, &value.ref) == -1);
    vemb_v16_cli_l1_stats_t stats;
    vemb_v16_cli_l1_get_stats(l1, &stats);
    assert(stats.live_vector_bytes == 0);
    put_vector(l1, "new", 1, 2.0f);
    assert(vemb_v16_cli_l1_pin(l1, &value.ref) == -1);
    vemb_v16_cli_l1_destroy(l1);
}

int main(void) {
    test_exact_key_collision();
    test_clock_eviction();
    test_all_pinned_and_generation_aba();
    test_storage_exhaustion();
    test_clear_invalidates_refs();
    printf("vemb_v16_cli_l1_ut: all tests passed\n");
    return 0;
}
