#include "internal/vemb_v16_cli_l0.h"
#include "../../src/redisassert.h"

#include <stdlib.h>
#include <string.h>

#define VEMB_V16_CLI_L0_NONE UINT32_MAX
#define VEMB_V16_CLI_L0_BATCH_HEADER_BYTES 28u

enum vemb_v16_cli_l0_state {
    VEMB_V16_CLI_L0_FREE = 0,
    VEMB_V16_CLI_L0_PENDING_SEND,
    VEMB_V16_CLI_L0_PUBLISHED,
    VEMB_V16_CLI_L0_FALLBACK_V1,
};

typedef struct vemb_v16_cli_l0_bucket {
    uint8_t fingerprint[VEMB_V16_CLI_L0_BUCKET_SLOTS];
    uint16_t occupied;
    uint16_t reserved;
    uint32_t entry_id[VEMB_V16_CLI_L0_BUCKET_SLOTS];
} vemb_v16_cli_l0_bucket_t;

_Static_assert(sizeof(vemb_v16_cli_l0_bucket_t) == 64,
               "L0 bucket must remain one cache line");

typedef struct vemb_v16_cli_l0_entry {
    uint64_t hash;
    uint64_t batch_id;
    uint64_t leader_cookie;
    vemb_v16_owner_session_identity_t identity;
    uint32_t generation;
    uint32_t pending_next;
    uint32_t follower_head;
    uint32_t follower_tail;
    uint32_t channel_index;
    uint32_t key_slot;
    uint16_t key_len;
    uint16_t batch_item_index;
    uint16_t bucket_index;
    uint8_t bucket_slot;
    uint8_t key_class;
    uint8_t state;
    uint8_t reserved[3];
} vemb_v16_cli_l0_entry_t;

typedef struct vemb_v16_cli_l0_follower {
    uint64_t caller_cookie;
    uint32_t next;
} vemb_v16_cli_l0_follower_t;

typedef struct vemb_v16_cli_l0_key_slab {
    uint32_t slot_size;
    uint32_t capacity;
    uint32_t free_head;
    uint32_t *next;
    char *bytes;
} vemb_v16_cli_l0_key_slab_t;

typedef struct vemb_v16_cli_l0_batch_record {
    uint64_t batch_id;
    vemb_v16_owner_session_identity_t identity;
    uint64_t completed[(VEMB_V16_BATCH_REQUEST_SIZE_MAX + 63u) / 64u];
    uint32_t channel_index;
    uint32_t item_count;
    uint32_t completed_count;
    uint8_t active;
    uint32_t entry_ids[VEMB_V16_BATCH_REQUEST_SIZE_MAX];
    uint32_t generations[VEMB_V16_BATCH_REQUEST_SIZE_MAX];
} vemb_v16_cli_l0_batch_record_t;

struct vemb_v16_cli_l0 {
    uint32_t channel_count;
    uint32_t entry_free_head;
    uint32_t follower_free_head;
    vemb_v16_cli_l0_bucket_t buckets[VEMB_V16_CLI_L0_BUCKET_COUNT];
    vemb_v16_cli_l0_entry_t entries[VEMB_V16_CLI_L0_MAX_ENTRIES];
    uint32_t entry_next[VEMB_V16_CLI_L0_MAX_ENTRIES];
    vemb_v16_cli_l0_follower_t followers[VEMB_V16_CLI_L0_MAX_FOLLOWERS];
    uint32_t follower_next[VEMB_V16_CLI_L0_MAX_FOLLOWERS];
    vemb_v16_cli_l0_key_slab_t key_slabs[4];
    vemb_v16_cli_l0_batch_record_t
        batch_records[VEMB_V16_CLI_L0_MAX_BATCH_RECORDS];
    uint32_t *pending_head;
    uint32_t *pending_tail;
    uint32_t *pending_count;
    uint32_t *pending_payload_bytes;
    vemb_v16_cli_l0_stats_t stats;
};

static const uint32_t vemb_v16_cli_l0_key_slab_sizes[4] = {
    16u, 32u, 64u, VEMB_V16_MAX_KEY_LEN,
};

static const uint32_t vemb_v16_cli_l0_key_slab_capacities[4] = {
    512u, 256u, 128u, 128u,
};

static uint32_t vemb_v16_cli_l0_key_class(uint16_t key_len) {
    for (uint32_t i = 0; i < 4; i++) {
        if (key_len <= vemb_v16_cli_l0_key_slab_sizes[i])
            return i;
    }
    return VEMB_V16_CLI_L0_NONE;
}

static char *vemb_v16_cli_l0_key_ptr(vemb_v16_cli_l0_t *l0,
                                      const vemb_v16_cli_l0_entry_t *entry) {
    vemb_v16_cli_l0_key_slab_t *slab = &l0->key_slabs[entry->key_class];
    return slab->bytes + (size_t)entry->key_slot * slab->slot_size;
}

static void vemb_v16_cli_l0_key_slab_init(vemb_v16_cli_l0_key_slab_t *slab,
                                           uint32_t slot_size,
                                           uint32_t capacity) {
    slab->slot_size = slot_size;
    slab->capacity = capacity;
    slab->free_head = capacity ? 0 : VEMB_V16_CLI_L0_NONE;
    slab->next = calloc(capacity, sizeof(*slab->next));
    slab->bytes = calloc(capacity, slot_size);
    assert(slab->next != NULL && slab->bytes != NULL);
    for (uint32_t i = 0; i < capacity; i++)
        slab->next[i] = i + 1u < capacity ? i + 1u : VEMB_V16_CLI_L0_NONE;
}

static void vemb_v16_cli_l0_key_slab_destroy(vemb_v16_cli_l0_key_slab_t *slab) {
    free(slab->next);
    free(slab->bytes);
    *slab = (vemb_v16_cli_l0_key_slab_t){0};
}

static uint32_t vemb_v16_cli_l0_entry_alloc(vemb_v16_cli_l0_t *l0) {
    uint32_t id = l0->entry_free_head;
    if (id == VEMB_V16_CLI_L0_NONE)
        return id;
    l0->entry_free_head = l0->entry_next[id];
    uint32_t generation = l0->entries[id].generation + 1u;
    if (generation == 0)
        generation = 1;
    l0->entries[id] = (vemb_v16_cli_l0_entry_t){
        .generation = generation,
        .pending_next = VEMB_V16_CLI_L0_NONE,
        .follower_head = VEMB_V16_CLI_L0_NONE,
        .follower_tail = VEMB_V16_CLI_L0_NONE,
    };
    return id;
}

static void vemb_v16_cli_l0_entry_release(vemb_v16_cli_l0_t *l0,
                                           uint32_t entry_id) {
    vemb_v16_cli_l0_entry_t *entry = &l0->entries[entry_id];
    vemb_v16_cli_l0_key_slab_t *slab = &l0->key_slabs[entry->key_class];
    slab->next[entry->key_slot] = slab->free_head;
    slab->free_head = entry->key_slot;
    entry->state = VEMB_V16_CLI_L0_FREE;
    l0->entry_next[entry_id] = l0->entry_free_head;
    l0->entry_free_head = entry_id;
}

static uint32_t vemb_v16_cli_l0_follower_alloc(vemb_v16_cli_l0_t *l0) {
    uint32_t id = l0->follower_free_head;
    if (id == VEMB_V16_CLI_L0_NONE)
        return id;
    l0->follower_free_head = l0->follower_next[id];
    l0->followers[id] = (vemb_v16_cli_l0_follower_t){
        .next = VEMB_V16_CLI_L0_NONE,
    };
    return id;
}

static void vemb_v16_cli_l0_follower_release(vemb_v16_cli_l0_t *l0,
                                              uint32_t follower_id) {
    l0->follower_next[follower_id] = l0->follower_free_head;
    l0->follower_free_head = follower_id;
}

static void vemb_v16_cli_l0_queue_append(vemb_v16_cli_l0_t *l0,
                                          uint32_t channel_index,
                                          uint32_t entry_id) {
    vemb_v16_cli_l0_entry_t *entry = &l0->entries[entry_id];
    entry->pending_next = VEMB_V16_CLI_L0_NONE;
    if (l0->pending_tail[channel_index] == VEMB_V16_CLI_L0_NONE)
        l0->pending_head[channel_index] = entry_id;
    else
        l0->entries[l0->pending_tail[channel_index]].pending_next = entry_id;
    l0->pending_tail[channel_index] = entry_id;
    l0->pending_count[channel_index]++;
    l0->pending_payload_bytes[channel_index] +=
        (uint32_t)sizeof(uint16_t) + entry->key_len;
}

static void vemb_v16_cli_l0_queue_remove(vemb_v16_cli_l0_t *l0,
                                          uint32_t entry_id) {
    vemb_v16_cli_l0_entry_t *entry = &l0->entries[entry_id];
    uint32_t channel_index = entry->channel_index;
    uint32_t previous = VEMB_V16_CLI_L0_NONE;
    uint32_t current = l0->pending_head[channel_index];
    while (current != VEMB_V16_CLI_L0_NONE) {
        if (current == entry_id) {
            uint32_t next = l0->entries[current].pending_next;
            if (previous == VEMB_V16_CLI_L0_NONE)
                l0->pending_head[channel_index] = next;
            else
                l0->entries[previous].pending_next = next;
            if (l0->pending_tail[channel_index] == current)
                l0->pending_tail[channel_index] = previous;
            entry->pending_next = VEMB_V16_CLI_L0_NONE;
            l0->pending_count[channel_index]--;
            l0->pending_payload_bytes[channel_index] -=
                (uint32_t)sizeof(uint16_t) + entry->key_len;
            return;
        }
        previous = current;
        current = l0->entries[current].pending_next;
    }
}

static void vemb_v16_cli_l0_index_remove(vemb_v16_cli_l0_t *l0,
                                          uint32_t entry_id) {
    vemb_v16_cli_l0_entry_t *entry = &l0->entries[entry_id];
    vemb_v16_cli_l0_bucket_t *bucket = &l0->buckets[entry->bucket_index];
    bucket->occupied &= (uint16_t)~(UINT16_C(1) << entry->bucket_slot);
}

static vemb_v16_cli_l0_batch_record_t *vemb_v16_cli_l0_batch_record_find(
    vemb_v16_cli_l0_t *l0, uint32_t channel_index, uint64_t batch_id) {
    for (uint32_t i = 0; i < VEMB_V16_CLI_L0_MAX_BATCH_RECORDS; i++) {
        vemb_v16_cli_l0_batch_record_t *record = &l0->batch_records[i];
        if (record->active && record->channel_index == channel_index &&
            record->batch_id == batch_id)
            return record;
    }
    return NULL;
}

static vemb_v16_cli_l0_batch_record_t *vemb_v16_cli_l0_batch_record_acquire(
    vemb_v16_cli_l0_t *l0) {
    for (uint32_t i = 0; i < VEMB_V16_CLI_L0_MAX_BATCH_RECORDS; i++) {
        if (!l0->batch_records[i].active)
            return &l0->batch_records[i];
    }
    return NULL;
}

vemb_v16_cli_l0_t *vemb_v16_cli_l0_create(uint32_t channel_count) {
    assert(channel_count > 0);
    vemb_v16_cli_l0_t *l0 = calloc(1, sizeof(*l0));
    assert(l0 != NULL);
    l0->channel_count = channel_count;
    l0->pending_head = malloc((size_t)channel_count * sizeof(*l0->pending_head));
    l0->pending_tail = malloc((size_t)channel_count * sizeof(*l0->pending_tail));
    l0->pending_count = calloc(channel_count, sizeof(*l0->pending_count));
    l0->pending_payload_bytes =
        calloc(channel_count, sizeof(*l0->pending_payload_bytes));
    assert(l0->pending_head != NULL && l0->pending_tail != NULL &&
           l0->pending_count != NULL && l0->pending_payload_bytes != NULL);
    for (uint32_t i = 0; i < channel_count; i++) {
        l0->pending_head[i] = VEMB_V16_CLI_L0_NONE;
        l0->pending_tail[i] = VEMB_V16_CLI_L0_NONE;
    }
    l0->entry_free_head = 0;
    for (uint32_t i = 0; i < VEMB_V16_CLI_L0_MAX_ENTRIES; i++)
        l0->entry_next[i] = i + 1u < VEMB_V16_CLI_L0_MAX_ENTRIES ?
            i + 1u : VEMB_V16_CLI_L0_NONE;
    l0->follower_free_head = 0;
    for (uint32_t i = 0; i < VEMB_V16_CLI_L0_MAX_FOLLOWERS; i++)
        l0->follower_next[i] = i + 1u < VEMB_V16_CLI_L0_MAX_FOLLOWERS ?
            i + 1u : VEMB_V16_CLI_L0_NONE;
    for (uint32_t i = 0; i < 4; i++) {
        vemb_v16_cli_l0_key_slab_init(&l0->key_slabs[i],
                                      vemb_v16_cli_l0_key_slab_sizes[i],
                                      vemb_v16_cli_l0_key_slab_capacities[i]);
    }
    return l0;
}

void vemb_v16_cli_l0_destroy(vemb_v16_cli_l0_t *l0) {
    for (uint32_t i = 0; i < 4; i++)
        vemb_v16_cli_l0_key_slab_destroy(&l0->key_slabs[i]);
    free(l0->pending_head);
    free(l0->pending_tail);
    free(l0->pending_count);
    free(l0->pending_payload_bytes);
    free(l0);
}

int vemb_v16_cli_l0_submit_with_identity(
    vemb_v16_cli_l0_t *l0, const char *final_key, uint16_t key_len,
    uint64_t hash, uint64_t caller_cookie,
    const vemb_v16_owner_session_identity_t *identity,
    uint32_t *out_entry_id, uint32_t *out_channel_index) {
    uint32_t channel_index = (uint32_t)(hash % l0->channel_count);
    uint32_t bucket_index = (uint32_t)(hash &
        (VEMB_V16_CLI_L0_BUCKET_COUNT - 1u));
    uint8_t fingerprint = (uint8_t)(hash >> 56);
    vemb_v16_cli_l0_bucket_t *bucket = &l0->buckets[bucket_index];
    uint32_t empty_slot = VEMB_V16_CLI_L0_NONE;

    for (uint32_t slot = 0; slot < VEMB_V16_CLI_L0_BUCKET_SLOTS; slot++) {
        if (!(bucket->occupied & (UINT16_C(1) << slot))) {
            if (empty_slot == VEMB_V16_CLI_L0_NONE)
                empty_slot = slot;
            continue;
        }
        if (bucket->fingerprint[slot] != fingerprint)
            continue;
        vemb_v16_cli_l0_entry_t *entry = &l0->entries[bucket->entry_id[slot]];
        if (entry->state == VEMB_V16_CLI_L0_FREE || entry->hash != hash)
            continue;
        if (!vemb_v16_owner_session_identity_equal(&entry->identity, identity) ||
            entry->key_len != key_len ||
            memcmp(vemb_v16_cli_l0_key_ptr(l0, entry), final_key, key_len) != 0) {
            l0->stats.exact_key_mismatch++;
            continue;
        }
        uint32_t follower_id = vemb_v16_cli_l0_follower_alloc(l0);
        if (follower_id == VEMB_V16_CLI_L0_NONE) {
            l0->stats.follower_exhausted++;
            return VEMB_V16_CLI_L0_FOLLOWER_EXHAUSTED;
        }
        l0->followers[follower_id].caller_cookie = caller_cookie;
        if (entry->follower_tail == VEMB_V16_CLI_L0_NONE)
            entry->follower_head = follower_id;
        else
            l0->followers[entry->follower_tail].next = follower_id;
        entry->follower_tail = follower_id;
        *out_entry_id = bucket->entry_id[slot];
        *out_channel_index = entry->channel_index;
        l0->stats.coalesced_followers++;
        return VEMB_V16_CLI_L0_COALESCED_FOLLOWER;
    }

    if (empty_slot == VEMB_V16_CLI_L0_NONE) {
        l0->stats.bucket_full++;
        return VEMB_V16_CLI_L0_BUCKET_FULL;
    }
    uint32_t key_class = vemb_v16_cli_l0_key_class(key_len);
    uint32_t entry_id = vemb_v16_cli_l0_entry_alloc(l0);
    if (entry_id == VEMB_V16_CLI_L0_NONE) {
        l0->stats.entry_exhausted++;
        return VEMB_V16_CLI_L0_ENTRY_EXHAUSTED;
    }
    vemb_v16_cli_l0_key_slab_t *slab = &l0->key_slabs[key_class];
    if (slab->free_head == VEMB_V16_CLI_L0_NONE) {
        l0->entry_next[entry_id] = l0->entry_free_head;
        l0->entry_free_head = entry_id;
        l0->stats.key_slab_exhausted++;
        return VEMB_V16_CLI_L0_KEY_SLAB_EXHAUSTED;
    }
    uint32_t key_slot = slab->free_head;
    slab->free_head = slab->next[key_slot];
    vemb_v16_cli_l0_entry_t *entry = &l0->entries[entry_id];
    entry->hash = hash;
    entry->leader_cookie = caller_cookie;
    entry->identity = *identity;
    entry->channel_index = channel_index;
    entry->key_len = key_len;
    entry->key_class = (uint8_t)key_class;
    entry->key_slot = key_slot;
    entry->bucket_index = (uint16_t)bucket_index;
    entry->bucket_slot = (uint8_t)empty_slot;
    entry->state = VEMB_V16_CLI_L0_PENDING_SEND;
    memcpy(vemb_v16_cli_l0_key_ptr(l0, entry), final_key, key_len);
    bucket->fingerprint[empty_slot] = fingerprint;
    bucket->entry_id[empty_slot] = entry_id;
    bucket->occupied |= (uint16_t)(UINT16_C(1) << empty_slot);
    vemb_v16_cli_l0_queue_append(l0, channel_index, entry_id);
    l0->stats.new_leader_groups++;
    l0->stats.active_groups++;
    *out_entry_id = entry_id;
    *out_channel_index = channel_index;
    return VEMB_V16_CLI_L0_NEW_LEADER;
}

int vemb_v16_cli_l0_submit(vemb_v16_cli_l0_t *l0, const char *final_key,
                            uint16_t key_len, uint64_t hash,
                            uint64_t caller_cookie, uint32_t *out_entry_id,
                            uint32_t *out_channel_index) {
    const vemb_v16_owner_session_identity_t identity = {0};
    return vemb_v16_cli_l0_submit_with_identity(
        l0, final_key, key_len, hash, caller_cookie, &identity,
        out_entry_id, out_channel_index);
}

int vemb_v16_cli_l0_prepare_batch(vemb_v16_cli_l0_t *l0,
                                   uint32_t channel_index,
                                   uint32_t max_items, uint32_t max_bytes,
                                   vemb_v16_cli_l0_batch_draft_t *out) {
    if (!vemb_v16_cli_l0_batch_record_acquire(l0))
        return -2;
    *out = (vemb_v16_cli_l0_batch_draft_t){
        .channel_index = channel_index,
        .oversized_entry_id = VEMB_V16_CLI_L0_NONE,
    };
    uint32_t bytes = VEMB_V16_CLI_L0_BATCH_HEADER_BYTES;
    uint32_t entry_id = l0->pending_head[channel_index];
    while (entry_id != VEMB_V16_CLI_L0_NONE && out->item_count < max_items) {
        vemb_v16_cli_l0_entry_t *entry = &l0->entries[entry_id];
        if (entry->state != VEMB_V16_CLI_L0_PENDING_SEND)
            return -1;
        if (out->item_count != 0 &&
            !vemb_v16_owner_session_identity_equal(&out->identity,
                                                    &entry->identity))
            break;
        uint32_t item_bytes = (uint32_t)sizeof(uint16_t) + entry->key_len;
        if (bytes + item_bytes > max_bytes) {
            if (out->item_count == 0) {
                out->oversized_entry_id = entry_id;
                out->identity = entry->identity;
            }
            break;
        }
        uint32_t item = out->item_count++;
        if (item == 0)
            out->identity = entry->identity;
        out->entry_ids[item] = entry_id;
        out->keys[item] = vemb_v16_cli_l0_key_ptr(l0, entry);
        out->key_lens[item] = entry->key_len;
        bytes += item_bytes;
        entry_id = entry->pending_next;
    }
    return (int)out->item_count;
}

uint32_t vemb_v16_cli_l0_pending_item_count(
    const vemb_v16_cli_l0_t *l0, uint32_t channel_index) {
    return l0->pending_count[channel_index];
}

uint32_t vemb_v16_cli_l0_pending_frame_bytes(
    const vemb_v16_cli_l0_t *l0, uint32_t channel_index) {
    if (l0->pending_count[channel_index] == 0)
        return 0;
    return VEMB_V16_CLI_L0_BATCH_HEADER_BYTES +
        l0->pending_payload_bytes[channel_index];
}

int vemb_v16_cli_l0_publish_batch(vemb_v16_cli_l0_t *l0,
                                   const vemb_v16_cli_l0_batch_draft_t *draft,
                                   uint64_t batch_id) {
    if (vemb_v16_cli_l0_batch_record_find(l0, draft->channel_index, batch_id))
        return -1;
    vemb_v16_cli_l0_batch_record_t *record =
        vemb_v16_cli_l0_batch_record_acquire(l0);
    if (!record)
        return -1;
    for (uint32_t i = 0; i < draft->item_count; i++) {
        if (l0->pending_head[draft->channel_index] != draft->entry_ids[i])
            return -1;
        vemb_v16_cli_l0_entry_t *entry = &l0->entries[draft->entry_ids[i]];
        if (entry->state != VEMB_V16_CLI_L0_PENDING_SEND)
            return -1;
        if (!vemb_v16_owner_session_identity_equal(&entry->identity,
                                                    &draft->identity))
            return -1;
        l0->pending_head[draft->channel_index] = entry->pending_next;
        if (l0->pending_tail[draft->channel_index] == draft->entry_ids[i])
            l0->pending_tail[draft->channel_index] = VEMB_V16_CLI_L0_NONE;
        l0->pending_count[draft->channel_index]--;
        l0->pending_payload_bytes[draft->channel_index] -=
            (uint32_t)sizeof(uint16_t) + entry->key_len;
        entry->pending_next = VEMB_V16_CLI_L0_NONE;
        entry->state = VEMB_V16_CLI_L0_PUBLISHED;
        entry->batch_id = batch_id;
        entry->batch_item_index = (uint16_t)i;
    }
    *record = (vemb_v16_cli_l0_batch_record_t){
        .batch_id = batch_id,
        .identity = draft->identity,
        .channel_index = draft->channel_index,
        .item_count = draft->item_count,
        .active = 1,
    };
    for (uint32_t i = 0; i < draft->item_count; i++) {
        vemb_v16_cli_l0_entry_t *entry = &l0->entries[draft->entry_ids[i]];
        record->entry_ids[i] = draft->entry_ids[i];
        record->generations[i] = entry->generation;
    }
    return 0;
}

int vemb_v16_cli_l0_peek_response(vemb_v16_cli_l0_t *l0,
                                  uint32_t channel_index, uint64_t batch_id,
                                  uint32_t item_index,
                                  vemb_v16_cli_l0_completion_t *out) {
    const vemb_v16_cli_l0_batch_record_t *record =
        vemb_v16_cli_l0_batch_record_find(l0, channel_index, batch_id);
    if (!record || item_index >= record->item_count)
        return -1;
    uint64_t mask = UINT64_C(1) << (item_index & 63u);
    if (record->completed[item_index >> 6] & mask)
        return 0;
    uint32_t entry_id = record->entry_ids[item_index];
    const vemb_v16_cli_l0_entry_t *entry = &l0->entries[entry_id];
    int valid = entry->state == VEMB_V16_CLI_L0_PUBLISHED &&
        entry->generation == record->generations[item_index] &&
        entry->channel_index == channel_index && entry->batch_id == batch_id &&
        entry->batch_item_index == item_index;
    if (!valid)
        return -1;
    *out = (vemb_v16_cli_l0_completion_t){
        .entry_id = entry_id,
        .generation = entry->generation,
    };
    return 1;
}

int vemb_v16_cli_l0_resolve_response(vemb_v16_cli_l0_t *l0,
                                     uint32_t channel_index, uint64_t batch_id,
                                     uint32_t item_index,
                                     vemb_v16_cli_l0_completion_t *out) {
    int rc = vemb_v16_cli_l0_peek_response(l0, channel_index, batch_id,
                                           item_index, out);
    if (rc != 1) {
        if (rc < 0)
            l0->stats.stale_response++;
        return rc;
    }
    vemb_v16_cli_l0_batch_record_t *record =
        vemb_v16_cli_l0_batch_record_find(l0, channel_index, batch_id);
    uint64_t mask = UINT64_C(1) << (item_index & 63u);
    uint64_t *word = &record->completed[item_index >> 6];
    *word |= mask;
    record->completed_count++;
    if (record->completed_count == record->item_count)
        record->active = 0;
    return 1;
}

int vemb_v16_cli_l0_finish(vemb_v16_cli_l0_t *l0,
                            const vemb_v16_cli_l0_completion_t *completion,
                            vemb_v16_cli_l0_fanout_cb cb, void *priv) {
    vemb_v16_cli_l0_entry_t *entry = &l0->entries[completion->entry_id];
    if (entry->state == VEMB_V16_CLI_L0_FREE ||
        entry->generation != completion->generation)
        return -1;
    if (entry->state == VEMB_V16_CLI_L0_PENDING_SEND)
        vemb_v16_cli_l0_queue_remove(l0, completion->entry_id);
    vemb_v16_cli_l0_index_remove(l0, completion->entry_id);
    if (cb)
        cb(priv, entry->leader_cookie);
    uint32_t follower_id = entry->follower_head;
    while (follower_id != VEMB_V16_CLI_L0_NONE) {
        vemb_v16_cli_l0_follower_t *follower = &l0->followers[follower_id];
        uint32_t next = follower->next;
        if (cb)
            cb(priv, follower->caller_cookie);
        vemb_v16_cli_l0_follower_release(l0, follower_id);
        follower_id = next;
    }
    vemb_v16_cli_l0_entry_release(l0, completion->entry_id);
    l0->stats.active_groups--;
    return 0;
}

int vemb_v16_cli_l0_mark_fallback_v1(vemb_v16_cli_l0_t *l0,
                                      uint32_t entry_id) {
    vemb_v16_cli_l0_entry_t *entry = &l0->entries[entry_id];
    if (entry->state == VEMB_V16_CLI_L0_PENDING_SEND)
        vemb_v16_cli_l0_queue_remove(l0, entry_id);
    else if (entry->state != VEMB_V16_CLI_L0_PUBLISHED)
        return -1;
    entry->state = VEMB_V16_CLI_L0_FALLBACK_V1;
    return 0;
}

int vemb_v16_cli_l0_get_group(vemb_v16_cli_l0_t *l0, uint32_t entry_id,
                               const char **out_key, uint16_t *out_key_len,
                               uint32_t *out_generation, uint8_t *out_state) {
    vemb_v16_cli_l0_entry_t *entry = &l0->entries[entry_id];
    if (entry->state == VEMB_V16_CLI_L0_FREE)
        return -1;
    *out_key = vemb_v16_cli_l0_key_ptr(l0, entry);
    *out_key_len = entry->key_len;
    *out_generation = entry->generation;
    if (out_state)
        *out_state = entry->state;
    return 0;
}

void vemb_v16_cli_l0_get_group_identity(
    const vemb_v16_cli_l0_t *l0, uint32_t entry_id,
    vemb_v16_owner_session_identity_t *out) {
    *out = l0->entries[entry_id].identity;
}

int vemb_v16_cli_l0_get_batch_identity(
    const vemb_v16_cli_l0_t *l0, uint32_t channel_index, uint64_t batch_id,
    vemb_v16_owner_session_identity_t *out) {
    for (uint32_t i = 0; i < VEMB_V16_CLI_L0_MAX_BATCH_RECORDS; i++) {
        const vemb_v16_cli_l0_batch_record_t *record = &l0->batch_records[i];
        if (record->active && record->channel_index == channel_index &&
            record->batch_id == batch_id) {
            *out = record->identity;
            return 0;
        }
    }
    return -1;
}

uint32_t vemb_v16_cli_l0_batch_item_count(
    const vemb_v16_cli_l0_t *l0, uint32_t channel_index,
    uint64_t batch_id) {
    for (uint32_t i = 0; i < VEMB_V16_CLI_L0_MAX_BATCH_RECORDS; i++) {
        const vemb_v16_cli_l0_batch_record_t *record = &l0->batch_records[i];
        if (record->active && record->channel_index == channel_index &&
            record->batch_id == batch_id)
            return record->item_count;
    }
    return 0;
}

uint32_t vemb_v16_cli_l0_group_fanout_count(
    const vemb_v16_cli_l0_t *l0,
    const vemb_v16_cli_l0_completion_t *completion) {
    const vemb_v16_cli_l0_entry_t *entry = &l0->entries[completion->entry_id];
    if (entry->state == VEMB_V16_CLI_L0_FREE ||
        entry->generation != completion->generation)
        return 0;
    uint32_t count = 1;
    for (uint32_t follower = entry->follower_head;
         follower != VEMB_V16_CLI_L0_NONE;
         follower = l0->followers[follower].next)
        count++;
    return count;
}

void vemb_v16_cli_l0_drain_pending(vemb_v16_cli_l0_t *l0,
                                   vemb_v16_cli_l0_fanout_cb cb, void *priv) {
    vemb_v16_cli_l0_completion_t
        pending[VEMB_V16_CLI_L0_MAX_ENTRIES];
    uint32_t count = 0;

    /* Snapshot first: the callback may schedule work for a later route. */
    for (uint32_t i = 0; i < VEMB_V16_CLI_L0_MAX_ENTRIES; i++) {
        const vemb_v16_cli_l0_entry_t *entry = &l0->entries[i];
        if (entry->state != VEMB_V16_CLI_L0_PENDING_SEND)
            continue;
        pending[count++] = (vemb_v16_cli_l0_completion_t){
            .entry_id = i,
            .generation = entry->generation,
        };
    }
    for (uint32_t i = 0; i < count; i++)
        (void)vemb_v16_cli_l0_finish(l0, &pending[i], cb, priv);
}

void vemb_v16_cli_l0_abort_all(vemb_v16_cli_l0_t *l0,
                                vemb_v16_cli_l0_fanout_cb cb, void *priv) {
    for (uint32_t i = 0; i < VEMB_V16_CLI_L0_MAX_BATCH_RECORDS; i++)
        l0->batch_records[i].active = 0;
    for (uint32_t i = 0; i < VEMB_V16_CLI_L0_MAX_ENTRIES; i++) {
        if (l0->entries[i].state == VEMB_V16_CLI_L0_FREE)
            continue;
        vemb_v16_cli_l0_completion_t completion = {
            .entry_id = i,
            .generation = l0->entries[i].generation,
        };
        vemb_v16_cli_l0_finish(l0, &completion, cb, priv);
    }
}

void vemb_v16_cli_l0_get_stats(const vemb_v16_cli_l0_t *l0,
                                vemb_v16_cli_l0_stats_t *out) {
    *out = l0->stats;
}
