#ifndef __VEMB_V16_CLIENT_SDK_H
#define __VEMB_V16_CLIENT_SDK_H

#include <stdint.h>
#include <unistd.h>
#include <stddef.h>

/* Include shared wire-protocol definitions.
 * NOTE: the ../../src/ prefix is intentional — it lets hpc-redis's own
 * src/Makefile build find these headers without extra -I flags.  When the
 * SDK is installed to build/include/ (flat), the Makefile rewrites these
 * lines via sed so external consumers get a path-free #include. */
#include "../../src/vemb_v16_protocol.h"
#include "../../src/vemb_v16_client_topology.h"
#include "../../src/vemb_v16_ring_rc.h"

#ifdef __cplusplus
extern "C" {
#endif

/* opaque handle */
typedef struct vemb_v16_client vemb_v16_client_t;

/* =====================================================================
 *  Synchronous Blocking API (high-level, existing)
 * ===================================================================== */

/*
 * Multi-endpoint client — connect to N backends and route every operation
 * by key.  'endpoints' is an NULL-terminated array of "host:port" strings.
 * Routing uses a murmur3-based consistent-hash ring (10 vnodes per backend),
 * interoperable with benchmark/vemb_v16_bench.c so a set filled by one tool
 * is visible to the others.
 *
 * All keyed operations (vadd / vemb_vector / vsim / *_pipeline / *_repeat)
 * select the backend internally; callers do not pick a backend.  In
 * multi-endpoint mode the pipeline helpers route all entries in one call to
 * the backend picked by set_names[0] — group by backend if keys span nodes.
 *
 * offset-based helpers (vemb_handle + read_vector) follow the last routed
 * backend, which is correct because vemb_vector routes vemb_handle before
 * calling read_vector synchronously.
 */

/*
 * THREAD SAFETY: a vemb_v16_client_t handle is NOT thread-safe.
 * One handle must only be used by one thread. Multi-threaded
 * applications must create one client per thread.
 *
 * TOPOLOGY / REDIRECTS: all single-key and pipeline operations
 * transparently retry ASK / MOVED / STALE_TOPOLOGY redirects
 * internally. Callers see only OK / NOT_FOUND / ERR. For
 * observability of redirect activity, use
 * vemb_v16_client_get_redirect_stats().
 */
vemb_v16_client_t *vemb_v16_client_create_multi(const char *endpoints[],
                                                 int endpoint_count,
                                                 uint32_t dim,
                                                 uint32_t timeout_ms);

/*
 * Route-only helper: given the same endpoint list used by
 * vemb_v16_client_create_multi, return the backend index selected for key
 * by the murmur3 consistent-hash ring.  This does not open any connection.
 * Returns 0 on success, -1 on error.
 */
int vemb_v16_route_key(const char *endpoints[], int endpoint_count,
                       const char *key, int *out_backend_idx);

vemb_v16_client_t *vemb_v16_client_create(const char *host,
                                          uint16_t port,
                                          uint32_t dim,
                                          uint32_t timeout_ms);
void vemb_v16_client_destroy(vemb_v16_client_t *client);

int vemb_v16_client_vadd(vemb_v16_client_t *client,
                         const char *set_name,
                         const char *elem_name,
                         const float *vector,
                         uint32_t dim);

int vemb_v16_client_vemb_handle(vemb_v16_client_t *client,
                                const char *set_name,
                                const char *elem_name,
                                uint64_t *out_offset,
                                uint32_t *out_bytes,
                                uint32_t *out_dim,
                                uint32_t *out_region_id);

int vemb_v16_client_vemb_vector(vemb_v16_client_t *client,
                                const char *set_name,
                                const char *elem_name,
                                float *out_vector,
                                uint32_t out_cap,
                                uint32_t *out_dim);

/*
 * VSIM_INLINE — compute cosine similarity between the stored vector
 * for (set_name, elem_name) and the provided query_vector.
 * On success, *out_score receives the similarity score.
 * Returns 0 on success, 1 if key not found, -1 on error.
 */
int vemb_v16_client_vsim(vemb_v16_client_t *client,
                         const char *set_name,
                         const char *elem_name,
                         const float *query_vector,
                         uint32_t dim,
                         float *out_score);

/*
 * VREM — remove a vector by (set_name, elem_name).
 * Idempotent: returns 0 whether the key existed or not. Returns -1 on error.
 */
int vemb_v16_client_vrem(vemb_v16_client_t *client,
                         const char *set_name,
                         const char *elem_name);

int vemb_v16_client_read_vector(vemb_v16_client_t *client,
                                uint64_t offset,
                                uint32_t bytes,
                                float *out_vector,
                                uint32_t out_cap);

/*
 * Pipeline — batch send / batch recv, blocking.
 * Returns 0 on success, -1 on error (caller cannot tell which one failed).
 */
int vemb_v16_client_vadd_pipeline(vemb_v16_client_t *c,
                                  const char **set_names,
                                  const char **elem_names,
                                  const float **vectors,
                                  uint32_t count,
                                  uint32_t max_inflight);

/* Per-response status for vemb pipeline */
typedef struct vemb_v16_pipeline_resp {
    int      status;      /* 0=OK, 1=NOT_FOUND, -1=error */
    uint64_t offset;
    uint32_t bytes;
    uint32_t dim;
    uint32_t region_id;
} vemb_v16_pipeline_resp_t;

/* Response classification — used by sync retry engine and async callers. */
typedef enum {
    VEMB_V16_RESP_CLASS_OK,         /* op succeeded */
    VEMB_V16_RESP_CLASS_NOT_FOUND,  /* key absent */
    VEMB_V16_RESP_CLASS_ASK,        /* one-shot redirect to resp.redirect_owner */
    VEMB_V16_RESP_CLASS_REFRESH,    /* MOVED or STALE_TOPOLOGY: refresh topology, retry */
    VEMB_V16_RESP_CLASS_FATAL,      /* ERR or unknown: give up */
} vemb_v16_resp_class_t;

vemb_v16_resp_class_t vemb_v16_classify_resp_status(uint8_t status);

int vemb_v16_client_vemb_pipeline(vemb_v16_client_t *c,
                                  const char **set_names,
                                  const char **elem_names,
                                  uint32_t count,
                                  float *out_vectors,      /* [count * dim], caller-allocated */
                                  vemb_v16_pipeline_resp_t *out_resps,
                                  uint32_t max_inflight);

/* VSIM_INLINE pipeline — cosine similarity for many (set,elem) pairs.
 * query_vector must be valid for the duration of the call (read-only).
 * out_scores is filled with similarity scores for OK responses.
 * Returns 0 on success, -1 on network/protocol error. */
int vemb_v16_client_vsim_pipeline(vemb_v16_client_t *c,
                                  const char **set_names,
                                  const char **elem_names,
                                  const float *query_vector,
                                  uint32_t count,
                                  float *out_scores,
                                  uint32_t max_inflight);

/*
 * PING — data-plane heartbeat.
 * Returns 0 if server responds OK, -1 on error or timeout.
 */
int vemb_v16_client_ping(vemb_v16_client_t *client);

/*
 * STATS — fetch proxy runtime statistics.
 * Returns 0 on success, -1 on error.
 */
int vemb_v16_client_stats(vemb_v16_client_t *client,
                          vemb_v16_stats_t *out_stats);

/* -------------------------------------------------------------------
 * Topology / redirect observability (advanced)
 * ------------------------------------------------------------------- */

/* Explicit pre-warm of topology. Call right after create_multi to avoid
 * a first-op RTT spike in multi-endpoint mode. In single-endpoint mode
 * this is a no-op (topology is not used). Returns 0 on success, -1 on
 * failure (caller can proceed; the engine will retry lazily). */
int vemb_v16_client_topology_refresh(vemb_v16_client_t *client);

/* Configure max retry attempts per op for transparent redirect handling.
 * Default 256. Set to 0 to disable retry (first redirect surfaces as ERR —
 * debug only). Takes effect on the next op call. */
void vemb_v16_client_set_retry_budget(vemb_v16_client_t *client,
                                       uint32_t max_attempts);

/* Observability: cumulative redirect counters since client creation.
 * All fields are 0-initialized; this reports lifetime totals. */
typedef struct {
    uint64_t ask_redirects;
    uint64_t moved_redirects;
    uint64_t stale_topology_responses;
    uint64_t topology_refresh_calls;
} vemb_v16_redirect_stats_t;
void vemb_v16_client_get_redirect_stats(const vemb_v16_client_t *client,
                                         vemb_v16_redirect_stats_t *out);

/* =====================================================================
 *  Convenience helpers (caller-allocates or standalone)
 * ===================================================================== */

/* Parse a comma-separated vector string: "0.1,0.2,0.3"
 * Returns malloc'd float array on success, NULL on error.
 */
float *vemb_v16_parse_vector_csv(const char *str, uint32_t expected_dim);

/* Parse vector from argv array starting at start_idx.
 * Supports comma-separated single token or individual float tokens.
 * Returns malloc'd float array on success, NULL on error.
 * out_consumed receives the number of argv tokens consumed.
 */
float *vemb_v16_parse_vector_argv(char **argv, int argc, int start_idx,
                                   uint32_t expected_dim, int *out_consumed);

/* Repeat VSIM on the same (set_name, elem_name) pair 'repeat' times.
 * Returns 0 on success, -1 on error.
 * out_score receives the last response's score.
 * out_found receives 1 if last response was OK, 0 if NOT_FOUND.
 */
int vemb_v16_client_vsim_repeat(vemb_v16_client_t *c,
                                 const char *set_name, const char *elem_name,
                                 const float *query_vector, uint32_t repeat,
                                 float *out_score, int *out_found,
                                 uint32_t max_inflight);

/* Repeat VEMB_HANDLE on the same (set_name, elem_name) pair 'repeat' times.
 * Returns 0 on success, -1 on error.
 */
int vemb_v16_client_vemb_repeat(vemb_v16_client_t *c,
                                 const char *set_name, const char *elem_name,
                                 uint32_t repeat, uint32_t max_inflight);

/* Repeat VADD_INLINE on the same (set_name, elem_name) pair 'repeat' times
 * with the same vector.
 * Returns 0 on success, -1 on error.
 */
int vemb_v16_client_vadd_repeat(vemb_v16_client_t *c,
                                 const char *set_name, const char *elem_name,
                                 const float *vector, uint32_t repeat,
                                 uint32_t max_inflight);

/* Internal accessors for thin wrappers (e.g. redis-cli pipeline) */
int vemb_v16_client_fd(const vemb_v16_client_t *client);
uint64_t vemb_v16_client_channel_id(const vemb_v16_client_t *client);

/* =====================================================================
 *  Async / Buffer-based API (low-level, for event-loop callers)
 * =====================================================================
 *
 * These functions serialize / deserialize VEMB V16 frames into
 * caller-provided buffers.  The caller is responsible for transport
 * (e.g. libevent evbuffer_add, sendmsg, etc.).
 */

/*
 * Build combined key.
 *   set_name != NULL && != ""  →  key = set_name + '\0' + elem_name
 *   set_name == NULL || == ""  →  key = elem_name  (no separator)
 * Returns 0 on success, -1 on error.
 */
int vemb_v16_build_combined_key(char *out, size_t out_cap,
                                const char *set_name, const char *elem_name,
                                uint32_t *out_len);

/*
 * Serialize a complete HELLO frame into a user-provided buffer.
 * Returns bytes written (>0), or -1 if buf_cap too small.
 */
ssize_t vemb_v16_serialize_hello(void *buf, size_t buf_cap,
                                 uint32_t vector_dim, uint32_t flags);

/*
 * Serialize a complete VADD_INLINE frame into a user-provided buffer.
 * 'key' is the raw key as sent on the wire (already combined if needed).
 * Returns bytes written (>0), or -1 if buf_cap too small.
 */
ssize_t vemb_v16_serialize_vadd(void *buf, size_t buf_cap,
                                uint64_t channel_id, uint32_t req_id,
                                const char *key, uint32_t key_len,
                                const float *vector, uint32_t dim);

/*
 * Serialize a complete VEMB_HANDLE frame into a user-provided buffer.
 * Returns bytes written (>0), or -1 if buf_cap too small.
 */
ssize_t vemb_v16_serialize_vemb(void *buf, size_t buf_cap,
                                uint64_t channel_id, uint32_t req_id,
                                const char *key, uint32_t key_len,
                                uint32_t dim);

/*
 * Serialize a complete VSIM_INLINE frame into a user-provided buffer.
 * 'key' is the raw key as sent on the wire (already combined if needed).
 * Returns bytes written (>0), or -1 if buf_cap too small.
 */
ssize_t vemb_v16_serialize_vsim_inline(void *buf, size_t buf_cap,
                                       uint64_t channel_id, uint32_t req_id,
                                       const char *key, uint32_t key_len,
                                       const float *query_vector, uint32_t dim);

/*
 * Serialize a complete VEMB_INLINE frame.
 * This produces the same frame as vemb_v16_serialize_vemb() but requests the
 * server to return the vector inline, which is the default read path for TCP
 * clients that do not mmap the warm region.
 */
ssize_t vemb_v16_serialize_vemb_inline(void *buf, size_t buf_cap,
                                       uint64_t channel_id, uint32_t req_id,
                                       const char *key, uint32_t key_len,
                                       uint32_t dim);

/*
 * Serialize a complete VREM frame (key-only, no vector payload).
 * 'key' is the raw key as sent on the wire (already combined if needed).
 * Returns bytes written (>0), or -1 if buf_cap too small.
 */
ssize_t vemb_v16_serialize_vrem(void *buf, size_t buf_cap,
                                uint64_t channel_id, uint32_t req_id,
                                const char *key, uint32_t key_len);

/*
 * Parse a WELCOME frame from a caller-provided byte buffer.
 * Returns bytes consumed (>0) on success, 0 if the frame is incomplete,
 * and -1 on a protocol error.
 */
ssize_t vemb_v16_parse_welcome(const void *buf, size_t buf_len,
                               vemb_v16_channel_desc_t *out_desc);

/*
 * Parse a RESPONSE frame from a caller-provided byte buffer.
 * Returns bytes consumed (>0) on success, 0 if the frame is incomplete,
 * and -1 on a protocol error.
 * On success *out_inline_bytes receives the number of inline-vector payload
 * bytes contained in the returned frame (the caller drains them together with
 * the rest of the frame by removing `consumed` bytes from the buffer).
 */
ssize_t vemb_v16_parse_response(const void *buf, size_t buf_len,
                                vemb_v16_resp_t *out_resp,
                                size_t *out_inline_bytes);

/*
 * Warm-region mmap helpers.
 * Standalone — no client handle required.
 */
int vemb_v16_open_warm_region(const vemb_v16_channel_desc_t *desc,
                              void **out_mapping_addr,
                              size_t *out_mapping_bytes,
                              void **out_mapped_addr,
                              uint64_t *out_region_bytes);
void vemb_v16_close_warm_region(void *mapping_addr, size_t mapping_bytes);

/* =====================================================================
 *  Aeron transport (TCP control + UB-backed SPSC ring)
 * =====================================================================
 *
 * Side-channel transport that bypasses TCP/libevent. Each channel is a
 * bi-directional SPSC ring pair (req → server, resp ← server) backed by
 * POSIX shared memory. Channel allocation/deallocation goes through a
 * small UDS control connection (VEMB_V16_CTRL_*).
 *
 * The handle is opaque — callers never touch the ring layout. The
 * header is therefore safe to include from C++ translation units
 * without a C11 <stdatomic.h> dependency.
 *
 * Typical workflow:
 *   vemb_v16_aeron_close_all(uds);                 // best-effort cleanup
 *   ch = vemb_v16_aeron_open(uds, dim);            // alloc + map
 *   ... build vemb_v16_req_t with channel_id =
 *         vemb_v16_aeron_channel_id(ch) ...
 *   vemb_v16_aeron_publish_request(ch, &req, n);   // non-blocking
 *   n = vemb_v16_aeron_poll_response(ch, &resp,    // non-blocking
 *                                    sizeof(resp));
 *   vemb_v16_aeron_close(ch);
 */

typedef struct vemb_v16_aeron_channel vemb_v16_aeron_channel_t;
typedef struct vemb_v16_aeron_batch_channel vemb_v16_aeron_batch_channel_t;

/* Four mappings owned by a v2 batch channel. Descriptor ring pointers use
 * the existing fixed-slot client-ring layout; arenas contain contiguous v2
 * batch frames. The pointers stay valid until vemb_v16_aeron_batch_close(). */
typedef struct vemb_v16_aeron_batch_resources {
    void *request_descriptor_ring;
    void *request_arena;
    void *response_descriptor_ring;
    void *response_arena;
    uint32_t descriptor_slot_size;
    uint32_t descriptor_ring_slots;
    uint32_t effective_batch_size;
    uint32_t max_batch_bytes;
} vemb_v16_aeron_batch_resources_t;

/* Allocate and map one channel via the control plane.
 *   uds_path  — VEMB_V16_UDS_PATH ("/tmp/vemb_v16.sock") or
 *               "tcp://host:port" for remote control-plane allocation
 *   dim       — vector dimension; server uses it to size ring slots
 * Returns NULL on any failure (control endpoint missing, alloc rejected,
 * ring open error). Caller owns the returned handle and must release it with
 * vemb_v16_aeron_close(). */
vemb_v16_aeron_channel_t *vemb_v16_aeron_open(const char *uds_path,
                                              uint32_t dim);

/* Open an aeron channel via cross-node TCP attach. The transport is
 * still aeron (shmdev-backed SPSC ring), but the handshake goes over
 * TCP because UDS is AF_LOCAL (single-host only).
 *   host  — server hostname or IP (e.g. "192.168.1.111")
 *   port  — server TCP port (same port redis-server listens on)
 *   dim   — vector dimension; server uses it to size ring slots
 * Returns NULL on any failure (TCP error, ATTACH rejected, mmap
 * error). Caller owns the handle and must release it with
 * vemb_v16_aeron_close(). */
vemb_v16_aeron_channel_t *vemb_v16_aeron_open_remote(const char *host,
                                                     uint16_t port,
                                                     uint32_t dim);

/* Open the v2 batch channel through cross-node TCP ATTACH and map its
 * request/response descriptor rings plus byte arenas.
 *
 * requested_batch_size must be nonzero. requested_max_batch_bytes may be
 * zero to accept the server default. Returns NULL when v2 is unavailable,
 * the server is in migration, or any resource cannot be mapped. */
vemb_v16_aeron_batch_channel_t *vemb_v16_aeron_open_remote_batch(
    const char *host, uint16_t port, uint32_t dim,
    uint32_t requested_batch_size, uint32_t requested_max_batch_bytes);

/* Unmap all four v2 resources and best-effort notify the server to close the
 * v2 logical channel. Safe to call with NULL. */
void vemb_v16_aeron_batch_close(vemb_v16_aeron_batch_channel_t *ch);

uint64_t vemb_v16_aeron_batch_channel_id(
    const vemb_v16_aeron_batch_channel_t *ch);
uint64_t vemb_v16_aeron_batch_topology_epoch(
    const vemb_v16_aeron_batch_channel_t *ch);
int vemb_v16_aeron_batch_get_resources(
    const vemb_v16_aeron_batch_channel_t *ch,
    vemb_v16_aeron_batch_resources_t *out);

/* Publish one stable-topology VEMB_HANDLE batch. `ch` must be a live channel
 * returned by open_remote_batch(); batch_id and item_count must be nonzero;
 * keys and key_lens must point to item_count entries, each key must be
 * non-NULL, and each length must be in [1, VEMB_V16_MAX_KEY_LEN]. Violating
 * these preconditions is undefined behavior. The key array describes unique
 * items; CLI L0 coalescing remains the caller's next layer. Returns RING_OK,
 * RING_ERR_INVALID, or RING_ERR_FULL. */
int vemb_v16_aeron_batch_publish_handle(
    vemb_v16_aeron_batch_channel_t *ch, uint64_t batch_id,
    const char *const *keys, const uint16_t *key_lens, uint32_t item_count);

/* Poll one completed v2 batch. `ch` must be a live channel; batch_id and
 * topology_epoch must be writable; entries must reference at least
 * effective_batch_size writable responses. Violating these preconditions is
 * undefined behavior.
 * Returns the item count, or zero when no complete response is consumable.
 * Outputs are valid only for a positive return; entries are in request order. */
int vemb_v16_aeron_batch_poll_response(
    vemb_v16_aeron_batch_channel_t *ch, uint64_t *batch_id,
    uint64_t *topology_epoch, vemb_v16_resp_t *entries);

/* =====================================================================
 *  Reusable CLI VEMB_HANDLE batch session
 * =====================================================================
 *
 * One session is single-thread owned and contains a permanent v1 Aeron
 * channel plus a required v2 batch channel. submit_handle() coalesces only
 * identical unfinished final key bytes; all other operations stay on a
 * caller-managed v1 path. Completion callbacks retain the original caller
 * cookie for both leaders and coalesced followers.
 */
typedef struct vemb_v16_aeron_batch_client vemb_v16_aeron_batch_client_t;

typedef struct vemb_v16_aeron_batch_client_options {
    uint32_t requested_batch_size;       /* zero selects 32 */
    uint32_t requested_max_batch_bytes;  /* zero accepts server default */
    uint32_t max_batch_delay_us;          /* zero preserves eager flush */
} vemb_v16_aeron_batch_client_options_t;

typedef void (*vemb_v16_aeron_batch_completion_cb)(
    void *priv, uint64_t caller_cookie, const vemb_v16_resp_t *response);

/* The view is valid only for the duration of its completion callback. A
 * successful shared group dereferences the warm-region handle once, then
 * presents the same read-only buffer to its leader and followers. */
typedef struct vemb_v16_aeron_batch_vector_view {
    const float *data;
    uint32_t bytes;
    uint8_t attempted;
    uint8_t valid;
} vemb_v16_aeron_batch_vector_view_t;

typedef void (*vemb_v16_aeron_batch_vector_completion_cb)(
    void *priv, uint64_t caller_cookie, const vemb_v16_resp_t *response,
    const vemb_v16_aeron_batch_vector_view_t *vector_view);

typedef struct vemb_v16_aeron_batch_client_stats {
    uint64_t l0_new_leader_groups;
    uint64_t l0_coalesced_followers;
    uint64_t l0_exact_key_mismatch;
    uint64_t l0_bucket_full;
    uint64_t l0_entry_exhausted;
    uint64_t l0_follower_exhausted;
    uint64_t l0_key_slab_exhausted;
    uint64_t l0_stale_response;
    uint64_t l0_fallback_v1;
    uint64_t batch_frames;
    uint64_t batch_items;
    uint64_t batch_frame_bytes;
    uint64_t batch_flush_eager;
    uint64_t batch_flush_full;
    uint64_t batch_flush_deadline;
    uint64_t batch_flush_backpressure;
    uint64_t v2_stale_epochs;
    uint64_t v2_stale_responses;
    uint64_t v1_direct_requests;
    uint64_t shared_vector_group_reads;
    uint64_t shared_vector_group_read_failures;
    uint64_t shared_vector_group_bytes;
    uint64_t shared_vector_fanout;
    uint32_t l0_active_groups;
    uint32_t effective_batch_size;
    uint32_t max_batch_bytes;
    int batch_enabled;
} vemb_v16_aeron_batch_client_stats_t;

/* Opens the permanent v1 fallback channel and required v2 batch channel.
 * Returns NULL when v2 ATTACH, its resource mapping, or CLI L0 setup fails;
 * this API never returns a v1-only batch session. */
vemb_v16_aeron_batch_client_t *vemb_v16_aeron_batch_client_open_remote(
    const char *host, uint16_t port, uint32_t dim,
    const vemb_v16_aeron_batch_client_options_t *options);

/* Submits one logical VEMB_HANDLE read for final key bytes. Returns 0 once
 * the caller cookie is owned by the session, -2 if v1 fallback pressure
 * prevents acceptance, and -1 for invalid input or a closed session. */
int vemb_v16_aeron_batch_client_submit_handle(
    vemb_v16_aeron_batch_client_t *client, const char *final_key,
    uint16_t key_len, uint64_t caller_cookie);

/* Attempts to publish one v2 frame. A zero return means no error, including
 * an empty pending queue; -2 means v2 ring/arena pressure retained leaders
 * in PENDING_SEND for a later retry. */
int vemb_v16_aeron_batch_client_flush(vemb_v16_aeron_batch_client_t *client);

/* Returns the monotonic flush deadline for pending v2 leaders, or zero when
 * there is none. A zero max_batch_delay_us keeps eager poll-driven flushes. */
uint64_t vemb_v16_aeron_batch_client_next_flush_deadline_ns(
    const vemb_v16_aeron_batch_client_t *client);

/* Polls v1 and v2 completions and invokes cb once per logical caller. The
 * return value is the number of callbacks made, zero if no response is
 * available, or -1 on invalid input. poll flushes on the configured deadline
 * and retries a deadline-expired frame after v2 ring/arena backpressure. */
int vemb_v16_aeron_batch_client_poll(
    vemb_v16_aeron_batch_client_t *client,
    vemb_v16_aeron_batch_completion_cb cb, void *priv);

/* Like poll(), but for each completed L0 group the SDK materializes its
 * VEMB_HANDLE exactly once and fan-outs a temporary read-only vector view.
 * Direct v1 fallback requests receive an independently materialized view.
 * The callback must not retain vector_view->data after it returns. */
int vemb_v16_aeron_batch_client_poll_shared_vector(
    vemb_v16_aeron_batch_client_t *client,
    vemb_v16_aeron_batch_vector_completion_cb cb, void *priv);

/* Uses the v1 channel's warm-region mappings to dereference a handle from a
 * batch completion. The session does not retain completed vectors. */
int vemb_v16_aeron_batch_client_read_vector(
    vemb_v16_aeron_batch_client_t *client, uint32_t region_id,
    uint64_t offset, uint32_t bytes, float *out, uint32_t out_cap);

int vemb_v16_aeron_batch_client_batch_enabled(
    const vemb_v16_aeron_batch_client_t *client);
void vemb_v16_aeron_batch_client_get_stats(
    const vemb_v16_aeron_batch_client_t *client,
    vemb_v16_aeron_batch_client_stats_t *out);

/* Stops submit, reports every retained caller as ERR through cb, and releases
 * both channels plus all fixed-pool state. Safe with a NULL client. */
void vemb_v16_aeron_batch_client_close(
    vemb_v16_aeron_batch_client_t *client,
    vemb_v16_aeron_batch_completion_cb cb, void *priv);

/* Close one channel: unmaps both rings and notifies the server to
 * release its state. Safe to call with NULL (no-op). UDS errors are
 * swallowed — the rings are still unmapped locally. */
void vemb_v16_aeron_close(vemb_v16_aeron_channel_t *ch);

/* Close every channel currently registered on the given control endpoint.
 * Returns the server-reported count closed (>= 0) or -1 on protocol
 * error. Intended for best-effort stale-state cleanup before a run. */
int vemb_v16_aeron_close_all(const char *uds_path);

/* Channel id — stamp this into vemb_v16_req_t::channel_id when building
 * request frames via vemb_v16_serialize_* or by hand. Returns 0 if ch
 * is NULL. */
uint64_t vemb_v16_aeron_channel_id(const vemb_v16_aeron_channel_t *ch);

/* Non-blocking publish into the request ring.
 * Returns:
 *    0   on success
 *   -1   if the ring is full (drain responses and retry)
 *   -2   if len > slot_size (programming error)
 *   -3   if ch is NULL */
int vemb_v16_aeron_publish_request(vemb_v16_aeron_channel_t *ch,
                                   const void *buf, uint32_t len);

/* Non-blocking batch publish into the request ring. `bufs` may contain
 * variable-length request frames (VADD inline frames and handle frames can
 * be mixed). The call is all-or-none when the ring lacks capacity. */
int vemb_v16_aeron_publish_request_batch(vemb_v16_aeron_channel_t *ch,
                                         const void *const *bufs,
                                         const uint32_t *lens,
                                         uint32_t count);

/* Non-blocking poll from the response ring.
 * Returns bytes copied into buf (>0) on success, 0 if empty, -3 if
 * ch is NULL. If max_len exceeds the ring slot size, only slot_size
 * bytes are copied. */
int vemb_v16_aeron_poll_response(vemb_v16_aeron_channel_t *ch,
                                 void *buf, uint32_t max_len);

/* Non-blocking batch poll from the response ring. Responses are copied to
 * `slots` with `max_len` bytes reserved per response. Returns the number of
 * responses copied, or zero when empty/invalid. */
uint32_t vemb_v16_aeron_poll_response_batch(vemb_v16_aeron_channel_t *ch,
                                            void *slots,
                                            uint32_t max_len,
                                            uint32_t max_count);

/* Same as vemb_v16_aeron_poll_response_batch(), with the compact Aeron wire
 * length returned for every decoded response. `slots` still receives one
 * vemb_v16_resp_t at each `max_len` stride. */
uint32_t vemb_v16_aeron_poll_response_batch_ex(
    vemb_v16_aeron_channel_t *ch, void *slots, uint32_t *wire_lens,
    uint32_t max_len, uint32_t max_count);

/* Open the warm region referenced by this channel's server-provided
 * channel_desc (mmap of /dev/obmm_shmdev* or POSIX SHM). Required before
 * vemb_v16_aeron_read_vector() can dereference VEMB_HANDLE offsets.
 * Idempotent: a second call unmaps and re-maps.
 * Returns 0 on success, -1 on failure. */
int vemb_v16_aeron_open_warm_region(vemb_v16_aeron_channel_t *ch);

/* Read a vector via the (region_id, offset, bytes) tuple returned in a
 * VEMB_HANDLE response. Requires vemb_v16_aeron_open_warm_region() to have
 * succeeded.  When multiple warm regions are mapped, region_id selects
 * which mapping to dereference.
 *   ch        — channel with warm region(s) mapped
 *   region_id — resp.region_id from VEMB_HANDLE response
 *   offset    — resp.vector_offset from VEMB_HANDLE response
 *   bytes     — resp.vector_bytes (typically dim * sizeof(float))
 *   out       — caller buffer
 *   cap       — capacity of out in bytes
 * Returns bytes copied (>0) on success, -1 if region_id not found or
 * offset/bytes are out of range. */
int vemb_v16_aeron_read_vector(const vemb_v16_aeron_channel_t *ch,
                               uint32_t region_id,
                               uint64_t offset, uint32_t bytes,
                               void *out, uint32_t cap);

#ifdef __cplusplus
}
#endif

#endif
