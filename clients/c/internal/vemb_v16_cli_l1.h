#ifndef VEMB_V16_CLI_L1_H
#define VEMB_V16_CLI_L1_H

#include <stdint.h>

#include "../../../src/vemb_v16_protocol.h"

#ifdef __cplusplus
extern "C" {
#endif

#define VEMB_V16_CLI_L1_WAYS 4u

typedef struct vemb_v16_cli_l1 vemb_v16_cli_l1_t;

typedef struct vemb_v16_cli_l1_entry {
    uint64_t key_hash;      /* Hash used to select the cache set. */
    uint16_t key_len;       /* Exact final-key length in bytes. */
    uint16_t vector_bytes;  /* Materialized vector size in bytes. */
    uint32_t key_slot;      /* Index into the preallocated key slab. */
    uint32_t vector_slot;   /* Index into the preallocated vector pool. */
    uint32_t generation;    /* L1 slot generation used to reject stale refs. */
    uint16_t pin_count;     /* Number of local completions holding this entry. */
    uint8_t valid;          /* Entry contains a completed vector. */
    uint8_t clock_ref;      /* CLOCK replacement reference bit. */
    uint8_t reserved[4];    /* Reserved for future metadata fields. */
} vemb_v16_cli_l1_entry_t;

#ifdef __cplusplus
static_assert(sizeof(vemb_v16_cli_l1_entry_t) == 32,
              "L1 entry must remain 32 bytes");
#else
_Static_assert(sizeof(vemb_v16_cli_l1_entry_t) == 32,
               "L1 entry must remain 32 bytes");
#endif

typedef struct vemb_v16_cli_l1_config {
    uint32_t dim;
    uint32_t entry_count;
    /* Zero selects entry_count. Smaller explicit pools are supported for
     * resource-pressure tests and remain fixed for the cache lifetime. */
    uint32_t key_slot_count;
    uint32_t vector_slot_count;
} vemb_v16_cli_l1_config_t;

typedef struct vemb_v16_cli_l1_ref {
    uint32_t entry_id;
    uint32_t generation;
} vemb_v16_cli_l1_ref_t;

typedef struct vemb_v16_cli_l1_value {
    const void *vector;
    uint16_t vector_bytes;
    vemb_v16_cli_l1_ref_t ref;
} vemb_v16_cli_l1_value_t;

typedef enum vemb_v16_cli_l1_put_result {
    VEMB_V16_CLI_L1_PUT_INSERTED = 0,
    VEMB_V16_CLI_L1_PUT_ALREADY_PRESENT = 1,
    VEMB_V16_CLI_L1_PUT_ALL_PINNED = -2,
    VEMB_V16_CLI_L1_PUT_KEY_STORAGE_EXHAUSTED = -3,
    VEMB_V16_CLI_L1_PUT_VECTOR_STORAGE_EXHAUSTED = -4,
} vemb_v16_cli_l1_put_result_t;

typedef struct vemb_v16_cli_l1_stats {
    uint64_t hits;
    uint64_t misses;
    uint64_t inserts;
    uint64_t evicts;
    uint64_t exact_key_mismatch;
    uint64_t all_pinned;
    uint64_t key_storage_exhausted;
    uint64_t vector_storage_exhausted;
    uint64_t stale_ref;
    uint64_t live_vector_bytes;
    uint32_t live_entries;
} vemb_v16_cli_l1_stats_t;

/* Strict internal API contract: callers provide a live L1 object, valid
 * protocol-bounded final-key bytes, the matching key hash, and vectors whose
 * byte length equals the configured dim. Lookup returns a pinned value; the
 * caller must release the returned ref after its local completion is handled.
 * pin/release return -1 only for stale generation identities. */

/* All storage is allocated here. entry_count / VEMB_V16_CLI_L1_WAYS must be a
 * power of two; the production configuration uses equal entry/key/vector
 * capacities. */
vemb_v16_cli_l1_t *vemb_v16_cli_l1_create(
    const vemb_v16_cli_l1_config_t *config);
void vemb_v16_cli_l1_destroy(vemb_v16_cli_l1_t *l1);

/* Returns 1 for a pinned exact-key hit and 0 for a miss. vector remains valid
 * until vemb_v16_cli_l1_release() for value->ref. */
int vemb_v16_cli_l1_lookup(vemb_v16_cli_l1_t *l1, const char *final_key,
                           uint16_t key_len, uint64_t key_hash,
                           vemb_v16_cli_l1_value_t *value);

/* Copies a completed vector into the fixed L1 storage. Existing exact keys are
 * retained and report ALREADY_PRESENT. Resource-pressure outcomes leave the
 * existing cache contents unchanged. */
int vemb_v16_cli_l1_put(vemb_v16_cli_l1_t *l1, const char *final_key,
                        uint16_t key_len, uint64_t key_hash,
                        const void *vector, uint16_t vector_bytes);

int vemb_v16_cli_l1_pin(vemb_v16_cli_l1_t *l1,
                         const vemb_v16_cli_l1_ref_t *ref);
int vemb_v16_cli_l1_release(vemb_v16_cli_l1_t *l1,
                             const vemb_v16_cli_l1_ref_t *ref);

/* clear and destroy require all returned refs to have been released. */
void vemb_v16_cli_l1_clear(vemb_v16_cli_l1_t *l1);
void vemb_v16_cli_l1_get_stats(const vemb_v16_cli_l1_t *l1,
                                vemb_v16_cli_l1_stats_t *out);

#ifdef __cplusplus
}
#endif

#endif
