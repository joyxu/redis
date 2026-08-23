#include "internal/vemb_v16_cli_l1.h"
#include "../../src/redisassert.h"

#include <limits.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#define VEMB_V16_CLI_L1_NONE UINT32_MAX
#define VEMB_V16_CLI_L1_SET_BYTES 64u

typedef struct vemb_v16_cli_l1_set {
    uint8_t occupied;
    uint8_t clock_hand;
    uint8_t h2[VEMB_V16_CLI_L1_WAYS];
    uint8_t reserved[VEMB_V16_CLI_L1_SET_BYTES - 2u -
                     VEMB_V16_CLI_L1_WAYS];
} vemb_v16_cli_l1_set_t;

_Static_assert(sizeof(vemb_v16_cli_l1_set_t) == VEMB_V16_CLI_L1_SET_BYTES,
               "L1 set must remain one cache line");

struct vemb_v16_cli_l1 {
    uint32_t entry_count;
    uint32_t set_count;
    uint32_t key_slot_count;
    uint32_t vector_slot_count;
    uint32_t key_free_head;
    uint32_t vector_free_head;
    uint16_t vector_bytes;
    vemb_v16_cli_l1_set_t *sets;
    vemb_v16_cli_l1_entry_t *entries;
    uint32_t *key_next;
    uint32_t *vector_next;
    char *key_slab;
    unsigned char *vector_pool;
    vemb_v16_cli_l1_stats_t stats;
};

static int vemb_v16_cli_l1_is_power_of_two(uint32_t value) {
    return value != 0 && (value & (value - 1u)) == 0;
}

static int vemb_v16_cli_l1_size_multiply_overflows(size_t count,
                                                    size_t size) {
    return count != 0 && size > SIZE_MAX / count;
}

static uint32_t vemb_v16_cli_l1_next_generation(uint32_t generation) {
    generation++;
    return generation ? generation : 1u;
}

static uint8_t vemb_v16_cli_l1_h2(uint64_t key_hash) {
    return (uint8_t)(key_hash >> 56);
}

static uint32_t vemb_v16_cli_l1_entry_id(uint32_t set_index,
                                          uint32_t way) {
    return set_index * VEMB_V16_CLI_L1_WAYS + way;
}

static char *vemb_v16_cli_l1_key_ptr(vemb_v16_cli_l1_t *l1,
                                      uint32_t key_slot) {
    return l1->key_slab + (size_t)key_slot * VEMB_V16_MAX_KEY_LEN;
}

static const char *vemb_v16_cli_l1_key_ptr_const(
    const vemb_v16_cli_l1_t *l1, uint32_t key_slot) {
    return l1->key_slab + (size_t)key_slot * VEMB_V16_MAX_KEY_LEN;
}

static void *vemb_v16_cli_l1_vector_ptr(vemb_v16_cli_l1_t *l1,
                                         uint32_t vector_slot) {
    return l1->vector_pool + (size_t)vector_slot * l1->vector_bytes;
}

static void vemb_v16_cli_l1_init_free_list(uint32_t *next, uint32_t count,
                                            uint32_t *free_head) {
    *free_head = count ? 0 : VEMB_V16_CLI_L1_NONE;
    for (uint32_t i = 0; i < count; i++)
        next[i] = i + 1u < count ? i + 1u : VEMB_V16_CLI_L1_NONE;
}

static uint32_t vemb_v16_cli_l1_slot_alloc(uint32_t *next,
                                            uint32_t *free_head) {
    uint32_t slot = *free_head;
    if (slot != VEMB_V16_CLI_L1_NONE)
        *free_head = next[slot];
    return slot;
}

static void vemb_v16_cli_l1_slot_release(uint32_t *next, uint32_t *free_head,
                                          uint32_t slot) {
    next[slot] = *free_head;
    *free_head = slot;
}

static int vemb_v16_cli_l1_entry_matches(
    const vemb_v16_cli_l1_t *l1, const vemb_v16_cli_l1_entry_t *entry,
    const char *final_key, uint16_t key_len, uint64_t key_hash) {
    return entry->key_hash == key_hash && entry->key_len == key_len &&
           memcmp(vemb_v16_cli_l1_key_ptr_const(l1, entry->key_slot),
                  final_key, key_len) == 0;
}

/* Returns a matching way, or VEMB_V16_CLI_L1_NONE. h2 is only a filter;
 * full hash and exact key bytes remain the correctness check. */
static uint32_t vemb_v16_cli_l1_find_way(vemb_v16_cli_l1_t *l1,
                                          uint32_t set_index,
                                          const char *final_key,
                                          uint16_t key_len,
                                          uint64_t key_hash) {
    vemb_v16_cli_l1_set_t *set = &l1->sets[set_index];
    uint8_t h2 = vemb_v16_cli_l1_h2(key_hash);

    for (uint32_t way = 0; way < VEMB_V16_CLI_L1_WAYS; way++) {
        if (!(set->occupied & (UINT8_C(1) << way)) || set->h2[way] != h2)
            continue;
        vemb_v16_cli_l1_entry_t *entry =
            &l1->entries[vemb_v16_cli_l1_entry_id(set_index, way)];
        if (vemb_v16_cli_l1_entry_matches(l1, entry, final_key, key_len,
                                           key_hash))
            return way;
        l1->stats.exact_key_mismatch++;
    }
    return VEMB_V16_CLI_L1_NONE;
}

static uint32_t vemb_v16_cli_l1_find_empty_way(
    const vemb_v16_cli_l1_set_t *set) {
    for (uint32_t way = 0; way < VEMB_V16_CLI_L1_WAYS; way++) {
        if (!(set->occupied & (UINT8_C(1) << way)))
            return way;
    }
    return VEMB_V16_CLI_L1_NONE;
}

/* CLOCK runs only within one fixed set. A pinned entry is never a victim. */
static uint32_t vemb_v16_cli_l1_choose_victim(vemb_v16_cli_l1_t *l1,
                                               uint32_t set_index) {
    vemb_v16_cli_l1_set_t *set = &l1->sets[set_index];
    for (uint32_t probe = 0; probe < VEMB_V16_CLI_L1_WAYS * 2u; probe++) {
        uint32_t way = set->clock_hand;
        set->clock_hand = (uint8_t)((way + 1u) % VEMB_V16_CLI_L1_WAYS);
        vemb_v16_cli_l1_entry_t *entry =
            &l1->entries[vemb_v16_cli_l1_entry_id(set_index, way)];
        if (entry->pin_count != 0)
            continue;
        if (entry->clock_ref) {
            entry->clock_ref = 0;
            continue;
        }
        return way;
    }
    return VEMB_V16_CLI_L1_NONE;
}

vemb_v16_cli_l1_t *vemb_v16_cli_l1_create(
    const vemb_v16_cli_l1_config_t *config) {
    assert(config != NULL);
    if (config->dim == 0 || config->dim > VEMB_V16_MAX_DIM ||
        config->entry_count < VEMB_V16_CLI_L1_WAYS ||
        config->entry_count % VEMB_V16_CLI_L1_WAYS != 0)
        return NULL;

    uint32_t set_count = config->entry_count / VEMB_V16_CLI_L1_WAYS;
    uint32_t key_slot_count = config->key_slot_count ?
        config->key_slot_count : config->entry_count;
    uint32_t vector_slot_count = config->vector_slot_count ?
        config->vector_slot_count : config->entry_count;
    uint32_t vector_bytes = config->dim * (uint32_t)sizeof(float);
    if (!vemb_v16_cli_l1_is_power_of_two(set_count) || key_slot_count == 0 ||
        vector_slot_count == 0 || vector_bytes > UINT16_MAX ||
        vemb_v16_cli_l1_size_multiply_overflows(
            config->entry_count, sizeof(vemb_v16_cli_l1_entry_t)) ||
        vemb_v16_cli_l1_size_multiply_overflows(
            set_count, sizeof(vemb_v16_cli_l1_set_t)) ||
        vemb_v16_cli_l1_size_multiply_overflows(
            key_slot_count, VEMB_V16_MAX_KEY_LEN) ||
        vemb_v16_cli_l1_size_multiply_overflows(vector_slot_count,
                                                 vector_bytes))
        return NULL;

    vemb_v16_cli_l1_t *l1 = calloc(1, sizeof(*l1));
    assert(l1 != NULL);
    l1->entry_count = config->entry_count;
    l1->set_count = set_count;
    l1->key_slot_count = key_slot_count;
    l1->vector_slot_count = vector_slot_count;
    l1->vector_bytes = (uint16_t)vector_bytes;
    l1->sets = calloc(set_count, sizeof(*l1->sets));
    l1->entries = calloc(config->entry_count, sizeof(*l1->entries));
    l1->key_next = malloc((size_t)key_slot_count * sizeof(*l1->key_next));
    l1->vector_next =
        malloc((size_t)vector_slot_count * sizeof(*l1->vector_next));
    l1->key_slab = malloc((size_t)key_slot_count * VEMB_V16_MAX_KEY_LEN);
    l1->vector_pool = malloc((size_t)vector_slot_count * vector_bytes);
    assert(l1->sets != NULL && l1->entries != NULL && l1->key_next != NULL &&
           l1->vector_next != NULL && l1->key_slab != NULL &&
           l1->vector_pool != NULL);
    vemb_v16_cli_l1_init_free_list(l1->key_next, key_slot_count,
                                    &l1->key_free_head);
    vemb_v16_cli_l1_init_free_list(l1->vector_next, vector_slot_count,
                                    &l1->vector_free_head);
    return l1;
}

void vemb_v16_cli_l1_destroy(vemb_v16_cli_l1_t *l1) {
    for (uint32_t i = 0; i < l1->entry_count; i++)
        assert(!l1->entries[i].valid || l1->entries[i].pin_count == 0);
    free(l1->sets);
    free(l1->entries);
    free(l1->key_next);
    free(l1->vector_next);
    free(l1->key_slab);
    free(l1->vector_pool);
    free(l1);
}

int vemb_v16_cli_l1_lookup(vemb_v16_cli_l1_t *l1, const char *final_key,
                           uint16_t key_len, uint64_t key_hash,
                           vemb_v16_cli_l1_value_t *value) {
    uint32_t set_index = (uint32_t)key_hash & (l1->set_count - 1u);
    uint32_t way = vemb_v16_cli_l1_find_way(l1, set_index, final_key, key_len,
                                            key_hash);
    if (way == VEMB_V16_CLI_L1_NONE) {
        l1->stats.misses++;
        return 0;
    }

    uint32_t entry_id = vemb_v16_cli_l1_entry_id(set_index, way);
    vemb_v16_cli_l1_entry_t *entry = &l1->entries[entry_id];
    assert(entry->pin_count < UINT16_MAX);
    entry->pin_count++;
    entry->clock_ref = 1;
    *value = (vemb_v16_cli_l1_value_t){
        .vector = vemb_v16_cli_l1_vector_ptr(l1, entry->vector_slot),
        .vector_bytes = entry->vector_bytes,
        .ref = {.entry_id = entry_id, .generation = entry->generation},
    };
    l1->stats.hits++;
    return 1;
}

int vemb_v16_cli_l1_put(vemb_v16_cli_l1_t *l1, const char *final_key,
                        uint16_t key_len, uint64_t key_hash,
                        const void *vector, uint16_t vector_bytes) {
    assert(key_len > 0 && key_len <= VEMB_V16_MAX_KEY_LEN);
    assert(vector_bytes == l1->vector_bytes);
    uint32_t set_index = (uint32_t)key_hash & (l1->set_count - 1u);
    vemb_v16_cli_l1_set_t *set = &l1->sets[set_index];
    uint32_t way = vemb_v16_cli_l1_find_way(l1, set_index, final_key, key_len,
                                            key_hash);
    if (way != VEMB_V16_CLI_L1_NONE) {
        l1->entries[vemb_v16_cli_l1_entry_id(set_index, way)].clock_ref = 1;
        return VEMB_V16_CLI_L1_PUT_ALREADY_PRESENT;
    }

    uint32_t key_slot;
    uint32_t vector_slot;
    int replaces_victim = 0;
    way = vemb_v16_cli_l1_find_empty_way(set);
    if (way != VEMB_V16_CLI_L1_NONE) {
        key_slot = vemb_v16_cli_l1_slot_alloc(l1->key_next,
                                               &l1->key_free_head);
        if (key_slot == VEMB_V16_CLI_L1_NONE) {
            l1->stats.key_storage_exhausted++;
            return VEMB_V16_CLI_L1_PUT_KEY_STORAGE_EXHAUSTED;
        }
        vector_slot = vemb_v16_cli_l1_slot_alloc(l1->vector_next,
                                                  &l1->vector_free_head);
        if (vector_slot == VEMB_V16_CLI_L1_NONE) {
            vemb_v16_cli_l1_slot_release(l1->key_next, &l1->key_free_head,
                                          key_slot);
            l1->stats.vector_storage_exhausted++;
            return VEMB_V16_CLI_L1_PUT_VECTOR_STORAGE_EXHAUSTED;
        }
    } else {
        replaces_victim = 1;
        way = vemb_v16_cli_l1_choose_victim(l1, set_index);
        if (way == VEMB_V16_CLI_L1_NONE) {
            l1->stats.all_pinned++;
            return VEMB_V16_CLI_L1_PUT_ALL_PINNED;
        }
        vemb_v16_cli_l1_entry_t *victim =
            &l1->entries[vemb_v16_cli_l1_entry_id(set_index, way)];
        key_slot = victim->key_slot;
        vector_slot = victim->vector_slot;
        victim->valid = 0;
        victim->clock_ref = 0;
        l1->stats.evicts++;
    }

    uint32_t entry_id = vemb_v16_cli_l1_entry_id(set_index, way);
    vemb_v16_cli_l1_entry_t *entry = &l1->entries[entry_id];
    uint32_t generation = vemb_v16_cli_l1_next_generation(entry->generation);
    memcpy(vemb_v16_cli_l1_key_ptr(l1, key_slot), final_key, key_len);
    memcpy(vemb_v16_cli_l1_vector_ptr(l1, vector_slot), vector, vector_bytes);
    *entry = (vemb_v16_cli_l1_entry_t){
        .key_hash = key_hash,
        .key_len = key_len,
        .vector_bytes = vector_bytes,
        .key_slot = key_slot,
        .vector_slot = vector_slot,
        .generation = generation,
        .valid = 1,
        .clock_ref = 1,
    };
    set->h2[way] = vemb_v16_cli_l1_h2(key_hash);
    set->occupied |= UINT8_C(1) << way;
    l1->stats.inserts++;
    if (!replaces_victim) {
        l1->stats.live_entries++;
        l1->stats.live_vector_bytes += vector_bytes;
    }
    return VEMB_V16_CLI_L1_PUT_INSERTED;
}

static vemb_v16_cli_l1_entry_t *vemb_v16_cli_l1_ref_entry(
    vemb_v16_cli_l1_t *l1, const vemb_v16_cli_l1_ref_t *ref) {
    assert(ref->entry_id < l1->entry_count);
    vemb_v16_cli_l1_entry_t *entry = &l1->entries[ref->entry_id];
    if (!entry->valid || entry->generation != ref->generation)
        return NULL;
    return entry;
}

int vemb_v16_cli_l1_pin(vemb_v16_cli_l1_t *l1,
                         const vemb_v16_cli_l1_ref_t *ref) {
    vemb_v16_cli_l1_entry_t *entry = vemb_v16_cli_l1_ref_entry(l1, ref);
    if (!entry) {
        l1->stats.stale_ref++;
        return -1;
    }
    assert(entry->pin_count < UINT16_MAX);
    entry->pin_count++;
    entry->clock_ref = 1;
    return 0;
}

int vemb_v16_cli_l1_release(vemb_v16_cli_l1_t *l1,
                             const vemb_v16_cli_l1_ref_t *ref) {
    vemb_v16_cli_l1_entry_t *entry = vemb_v16_cli_l1_ref_entry(l1, ref);
    if (!entry) {
        l1->stats.stale_ref++;
        return -1;
    }
    assert(entry->pin_count != 0);
    entry->pin_count--;
    return 0;
}

void vemb_v16_cli_l1_clear(vemb_v16_cli_l1_t *l1) {
    for (uint32_t i = 0; i < l1->entry_count; i++) {
        vemb_v16_cli_l1_entry_t *entry = &l1->entries[i];
        assert(!entry->valid || entry->pin_count == 0);
        if (entry->valid)
            entry->generation =
                vemb_v16_cli_l1_next_generation(entry->generation);
        entry->valid = 0;
        entry->clock_ref = 0;
    }
    memset(l1->sets, 0, (size_t)l1->set_count * sizeof(*l1->sets));
    vemb_v16_cli_l1_init_free_list(l1->key_next, l1->key_slot_count,
                                    &l1->key_free_head);
    vemb_v16_cli_l1_init_free_list(l1->vector_next, l1->vector_slot_count,
                                    &l1->vector_free_head);
    l1->stats.live_entries = 0;
    l1->stats.live_vector_bytes = 0;
}

void vemb_v16_cli_l1_get_stats(const vemb_v16_cli_l1_t *l1,
                                vemb_v16_cli_l1_stats_t *out) {
    *out = l1->stats;
}
