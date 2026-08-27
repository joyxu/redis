#ifndef VEMB_V16_CLI_L0_H
#define VEMB_V16_CLI_L0_H

#include <stdint.h>

#include "../vemb_v16_owner_session.h"
#include "../../../src/vemb_v16_aeron_attach.h"
#include "../../../src/vemb_v16_protocol.h"

#ifdef __cplusplus
extern "C" {
#endif

#define VEMB_V16_CLI_L0_MAX_ENTRIES 1024u
#define VEMB_V16_CLI_L0_BUCKET_COUNT 256u
#define VEMB_V16_CLI_L0_BUCKET_SLOTS 12u
#define VEMB_V16_CLI_L0_MAX_FOLLOWERS 4096u
#define VEMB_V16_CLI_L0_MAX_BATCH_RECORDS 64u

typedef struct vemb_v16_cli_l0 vemb_v16_cli_l0_t;

enum vemb_v16_cli_l0_group_state {
    VEMB_V16_CLI_L0_GROUP_PENDING_SEND = 1,
    VEMB_V16_CLI_L0_GROUP_PUBLISHED = 2,
    VEMB_V16_CLI_L0_GROUP_FALLBACK_V1 = 3,
};

typedef enum vemb_v16_cli_l0_submit_result {
    VEMB_V16_CLI_L0_NEW_LEADER = 0,
    VEMB_V16_CLI_L0_COALESCED_FOLLOWER = 1,
    VEMB_V16_CLI_L0_BUCKET_FULL = -2,
    VEMB_V16_CLI_L0_ENTRY_EXHAUSTED = -3,
    VEMB_V16_CLI_L0_FOLLOWER_EXHAUSTED = -4,
    VEMB_V16_CLI_L0_KEY_SLAB_EXHAUSTED = -5,
    VEMB_V16_CLI_L0_INVALID = -6,
} vemb_v16_cli_l0_submit_result_t;

typedef struct vemb_v16_cli_l0_stats {
    uint64_t new_leader_groups;
    uint64_t coalesced_followers;
    uint64_t exact_key_mismatch;
    uint64_t bucket_full;
    uint64_t entry_exhausted;
    uint64_t follower_exhausted;
    uint64_t key_slab_exhausted;
    uint64_t stale_response;
    uint32_t active_groups;
} vemb_v16_cli_l0_stats_t;

typedef struct vemb_v16_cli_l0_batch_draft {
    uint32_t channel_index;
    uint32_t item_count;
    uint32_t oversized_entry_id;
    /* Every v2 frame is homogeneous in the core-selected route identity. */
    vemb_v16_owner_session_identity_t identity;
    uint32_t entry_ids[VEMB_V16_BATCH_REQUEST_SIZE_MAX];
    const char *keys[VEMB_V16_BATCH_REQUEST_SIZE_MAX];
    uint16_t key_lens[VEMB_V16_BATCH_REQUEST_SIZE_MAX];
} vemb_v16_cli_l0_batch_draft_t;

typedef struct vemb_v16_cli_l0_completion {
    uint32_t entry_id;
    uint32_t generation;
} vemb_v16_cli_l0_completion_t;

typedef void (*vemb_v16_cli_l0_fanout_cb)(void *priv, uint64_t caller_cookie);

/* Strict internal API contract: callers provide a live L0 object, non-null
 * required pointers, a positive channel count, valid channel/entry indices,
 * and protocol-bounded key, batch, and response fields. Violations are
 * programming errors; return codes
 * below are reserved for resource exhaustion and L0 state/identity outcomes.
 */

/* channel_count is fixed for the lifetime of the object. All allocations are
 * performed here; submit, flush and completion paths do not allocate. */
vemb_v16_cli_l0_t *vemb_v16_cli_l0_create(uint32_t channel_count);
void vemb_v16_cli_l0_destroy(vemb_v16_cli_l0_t *l0);

/* final_key is the complete VEMB key. hash must be the caller's 64-bit hash
 * of those exact bytes. The selected channel is hash % channel_count. */
int vemb_v16_cli_l0_submit(vemb_v16_cli_l0_t *l0, const char *final_key,
                            uint16_t key_len, uint64_t hash,
                            uint64_t caller_cookie,
                            uint32_t *out_entry_id,
                            uint32_t *out_channel_index);

/* Identity-aware form used by owner sessions after common-core routing. Two
 * equal keys only coalesce when their owner, topology epoch and owner channel
 * generation are identical. The legacy form above supplies an all-zero
 * identity for existing single-endpoint callers. */
int vemb_v16_cli_l0_submit_with_identity(
    vemb_v16_cli_l0_t *l0, const char *final_key, uint16_t key_len,
    uint64_t hash, uint64_t caller_cookie,
    const vemb_v16_owner_session_identity_t *identity,
    uint32_t *out_entry_id, uint32_t *out_channel_index);

/* Builds a prefix of the pending queue that fits both limits. A nonzero
 * oversized_entry_id means the queue head cannot fit even as a one-item
 * frame and must be moved to v1 before another v2 draft is attempted. */
int vemb_v16_cli_l0_prepare_batch(vemb_v16_cli_l0_t *l0,
                                   uint32_t channel_index,
                                   uint32_t max_items,
                                   uint32_t max_bytes,
                                   vemb_v16_cli_l0_batch_draft_t *out);

uint32_t vemb_v16_cli_l0_pending_item_count(
    const vemb_v16_cli_l0_t *l0, uint32_t channel_index);
uint32_t vemb_v16_cli_l0_pending_frame_bytes(
    const vemb_v16_cli_l0_t *l0, uint32_t channel_index);

/* Commits a successfully published draft. A batch id must be nonzero and
 * unique among currently published batches in this L0 object. */
int vemb_v16_cli_l0_publish_batch(vemb_v16_cli_l0_t *l0,
                                   const vemb_v16_cli_l0_batch_draft_t *draft,
                                   uint64_t batch_id);
void vemb_v16_cli_l0_set_batch_publish_ns(vemb_v16_cli_l0_t *l0,
                                           uint32_t channel_index,
                                           uint64_t batch_id,
                                           uint64_t publish_ns);
uint64_t vemb_v16_cli_l0_get_batch_publish_ns(
    vemb_v16_cli_l0_t *l0, uint32_t channel_index,
    uint64_t batch_id);

/* Resolve one response item to a live group. Returns 1 once, 0 for a
 * duplicate item, and -1 for stale/unknown identity. */
int vemb_v16_cli_l0_resolve_response(vemb_v16_cli_l0_t *l0,
                                     uint32_t channel_index,
                                     uint64_t batch_id, uint32_t item_index,
                                     vemb_v16_cli_l0_completion_t *out);

/* Checks whether one response item still resolves to a live group without
 * consuming the response identity. It uses the same return convention as
 * resolve_response. */
int vemb_v16_cli_l0_peek_response(vemb_v16_cli_l0_t *l0,
                                  uint32_t channel_index,
                                  uint64_t batch_id, uint32_t item_index,
                                  vemb_v16_cli_l0_completion_t *out);

/* Removes the group from the index before invoking cb for leader and every
 * follower, then recycles all fixed-pool resources. */
int vemb_v16_cli_l0_finish(vemb_v16_cli_l0_t *l0,
                            const vemb_v16_cli_l0_completion_t *completion,
                            vemb_v16_cli_l0_fanout_cb cb, void *priv);

/* Moves a not-yet-published leader out of the v2 queue. It remains indexed
 * so the caller can send one v1 leader and still coalesce its followers. */
int vemb_v16_cli_l0_mark_fallback_v1(vemb_v16_cli_l0_t *l0,
                                      uint32_t entry_id);

/* Borrowed key view for one live group. The pointer remains valid until that
 * group is finished or the L0 object is destroyed. */
int vemb_v16_cli_l0_get_group(vemb_v16_cli_l0_t *l0, uint32_t entry_id,
                               const char **out_key, uint16_t *out_key_len,
                               uint32_t *out_generation, uint8_t *out_state);
void vemb_v16_cli_l0_get_group_identity(
    const vemb_v16_cli_l0_t *l0, uint32_t entry_id,
    vemb_v16_owner_session_identity_t *out);
int vemb_v16_cli_l0_get_batch_identity(
    const vemb_v16_cli_l0_t *l0, uint32_t channel_index, uint64_t batch_id,
    vemb_v16_owner_session_identity_t *out);
uint32_t vemb_v16_cli_l0_batch_item_count(
    const vemb_v16_cli_l0_t *l0, uint32_t channel_index,
    uint64_t batch_id);
uint32_t vemb_v16_cli_l0_group_fanout_count(
    const vemb_v16_cli_l0_t *l0,
    const vemb_v16_cli_l0_completion_t *completion);

/* Removes every group that has not reached a v2 publish record and fans out
 * its cookies. Published groups remain owned by their batch records and must
 * be resolved from the response path before the owner session can drain. */
void vemb_v16_cli_l0_drain_pending(vemb_v16_cli_l0_t *l0,
                                   vemb_v16_cli_l0_fanout_cb cb, void *priv);

/* Finishes every group with the supplied callback. Used only after submit
 * has stopped, such as channel teardown. */
void vemb_v16_cli_l0_abort_all(vemb_v16_cli_l0_t *l0,
                                vemb_v16_cli_l0_fanout_cb cb, void *priv);
void vemb_v16_cli_l0_get_stats(const vemb_v16_cli_l0_t *l0,
                                vemb_v16_cli_l0_stats_t *out);

#ifdef __cplusplus
}
#endif

#endif
