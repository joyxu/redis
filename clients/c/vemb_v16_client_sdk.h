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
#include "vemb_v16_ub_peer_view.h"

#ifdef __cplusplus
extern "C" {
#endif

/* opaque handle */
typedef struct vemb_v16_client vemb_v16_client_t;

/* =====================================================================
 *  Synchronous Blocking API (high-level, existing)
 * ===================================================================== */

/*
 * Configure seed_count TCP bootstrap seeds. 'seeds' is an array of
 * "host:port" control addresses; creating the client does not open a data
 * channel. transport_type fixes this client to either TCP or AERON for its
 * lifetime. Every keyed operation obtains its authoritative owner routing
 * from server topology, fetched explicitly or lazily through the seed set;
 * every advertised owner endpoint must match the configured transport.
 *
 * All keyed operations (vadd / vemb_vector / vsim / *_pipeline / *_repeat)
 * select their owner internally; callers do not pick a backend. Pipeline
 * helpers group entries by their topology-selected owner.
 *
 * offset-based helpers (vemb_handle + read_vector) bind the returned handle
 * to its producing channel. A close/re-attach invalidates that binding, so a
 * stale handle cannot be read through a different owner's warm mapping.
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
 *
 * REQUIRED ARGUMENTS: except for create functions, every client/session
 * argument is a required live object and is not revalidated on the data path.
 * Per-operation key, vector and required output arguments must also be
 * non-NULL, and capacities must be positive; these call-local preconditions
 * assert at the public boundary. set_name, callbacks, and outputs explicitly
 * documented as optional may be NULL. Reusing an object after destroy / close
 * is a programming error. Create functions return NULL only for invalid
 * external configuration or a lifecycle conflict; allocation failure triggers
 * the Redis assertion path.
 */
vemb_v16_client_t *vemb_v16_client_create(const char *seeds[],
                                           int seed_count,
                                           uint32_t dim,
                                           uint32_t timeout_ms,
                                           uint32_t transport_type);

/* Configure fixed client-local UB peer views before the first UB owner
 * channel opens. owner_id in the manifest is the topology owner identity,
 * not a host-number alias. Every AERON owner requires matching entries;
 * same-host mappings use identical provider and client paths. */
int vemb_v16_client_configure_ub_peer_view(
    vemb_v16_client_t *client, const char *manifest_path,
    const char *client_host);

/* Select the requested v2 VEMB_HANDLE batch width before the first UB owner
 * session opens. The server may negotiate a smaller effective width. */
int vemb_v16_client_set_ub_batch_request_size(
    vemb_v16_client_t *client, uint32_t requested_batch_size);

void vemb_v16_client_destroy(vemb_v16_client_t *client);

int vemb_v16_client_vadd(vemb_v16_client_t *client,
                         const char *set_name,
                         const char *elem_name,
                         const float *vector,
                         uint32_t dim);

/* UB/AERON-only zero-copy read. A TCP client is rejected at this public API
 * boundary before topology or data-channel work begins. Result outputs are
 * optional; non-NULL outputs receive the corresponding handle fields. */
int vemb_v16_client_vemb_handle(vemb_v16_client_t *client,
                                const char *set_name,
                                const char *elem_name,
                                uint64_t *out_offset,
                                uint32_t *out_bytes,
                                uint32_t *out_dim,
                                uint32_t *out_region_id);

/* out_vector is required and out_cap is measured in floats. out_dim is
 * optional. */
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

/* Dereference the latest UB/AERON VEMB_HANDLE only. TCP never creates a
 * readable handle mapping. */
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
                                  float *out_vectors,      /* optional [count * dim] */
                                  vemb_v16_pipeline_resp_t *out_resps,
                                  uint32_t max_inflight);

/* UB/AERON-only VEMB_HANDLE pipeline. Stable remote-UB owner groups may use
 * the optional v2 batch channel; topology changes, migration, ATTACH/publish
 * failures, and ASK redirects always use the v1 owner channel. TCP-configured
 * clients are rejected at this API boundary. */
int vemb_v16_client_vemb_handle_pipeline(vemb_v16_client_t *c,
                                         const char **set_names,
                                         const char **elem_names,
                                         uint32_t count,
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

/* Explicit topology pre-warm. Call after create() to avoid a first-op RTT
 * spike. It tries every bootstrap seed before reporting failure; the engine
 * also retries lazily before its first keyed operation.
 * Returns 0 on success, -1 on failure. */
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
    uint64_t moved_override_applies;  /* ops routed via override_owner */
    uint64_t stale_topology_responses;
    uint64_t topology_refresh_calls;
} vemb_v16_redirect_stats_t;
void vemb_v16_client_get_redirect_stats(const vemb_v16_client_t *client,
                                         vemb_v16_redirect_stats_t *out);

/* Final logical outcomes are owned by cluster_core, independent of whether
 * an operation used v1, v2, or a redirect retry. */
typedef struct {
    uint64_t successes;
    uint64_t not_found;
    uint64_t errors;
} vemb_v16_logical_stats_t;
void vemb_v16_client_get_logical_stats(const vemb_v16_client_t *client,
                                       vemb_v16_logical_stats_t *out);

/* L0 grouping totals across the client's active owner sessions. */
typedef struct {
    uint64_t leaders;
    uint64_t followers;
} vemb_v16_fanout_stats_t;
void vemb_v16_client_get_fanout_stats(const vemb_v16_client_t *client,
                                      vemb_v16_fanout_stats_t *out);

/* -------------------------------------------------------------------
 * Event-loop VEMB_HANDLE session
 * -------------------------------------------------------------------
 *
 * This is the common-core asynchronous handle API. submit() copies the key,
 * assigns a logical operation before any L0 grouping, and returns immediately.
 * poll() drives routing, v1/v2 publication, completion fanout, and redirect
 * retry. Session creation accepts only an AERON-configured client; TCP clients
 * are rejected before topology or data I/O. A live session is exclusive with
 * the synchronous keyed APIs on its client because both own that client's
 * owner channels and core state.
 */
typedef struct vemb_v16_client_handle_session
    vemb_v16_client_handle_session_t;

typedef struct vemb_v16_client_handle_session_options {
    /* Zero flushes on the next poll(); a nonzero value bounds coalescing
     * latency after the first leader reaches an owner-local L0. */
    uint32_t max_batch_delay_us;
} vemb_v16_client_handle_session_options_t;

typedef void (*vemb_v16_client_handle_completion_cb)(
    void *priv, uint64_t caller_cookie,
    const vemb_v16_pipeline_resp_t *response);

vemb_v16_client_handle_session_t *vemb_v16_client_handle_session_create(
    vemb_v16_client_t *client,
    const vemb_v16_client_handle_session_options_t *options);

/* The session copies set_name and elem_name during this call. Completion is
 * delivered exactly once by poll() or close(). */
int vemb_v16_client_handle_session_submit(
    vemb_v16_client_handle_session_t *session, const char *set_name,
    const char *elem_name, uint64_t caller_cookie);

/* Publish all currently eligible L0 groups. It never waits for a response. */
int vemb_v16_client_handle_session_flush(
    vemb_v16_client_handle_session_t *session);

/* Drive nonblocking v1/v2 receives and retry work. Returns callback count,
 * zero when no completion is available, or -1 when the session is closing. */
int vemb_v16_client_handle_session_poll(
    vemb_v16_client_handle_session_t *session,
    vemb_v16_client_handle_completion_cb cb, void *priv);

/* Earliest owner-local L0 flush deadline in monotonic nanoseconds, or zero
 * when no v2 group is pending. */
uint64_t vemb_v16_client_handle_session_next_flush_deadline_ns(
    const vemb_v16_client_handle_session_t *session);

/* Completes every outstanding logical operation with ERR before releasing
 * owner-local L0 state. The session does not own the client. */
void vemb_v16_client_handle_session_close(
    vemb_v16_client_handle_session_t *session,
    vemb_v16_client_handle_completion_cb cb, void *priv);

/* -------------------------------------------------------------------
 * Event-loop logical vector-read session
 * -------------------------------------------------------------------
 *
 * submit() groups equal key-only vector reads after cluster_core selected the
 * owner route. The client-wide startup mode fixes every request to
 * VEMB_INLINE for TCP or VEMB_HANDLE for AERON; AERON materializes its
 * warm-region vector once. The callback's
 * vector view is valid only for the callback; callers must copy it to retain
 * it. This session deliberately does not coalesce writes or VSIM requests.
 *
 * A live vector session is exclusive with synchronous keyed APIs and the
 * handle session on the same client because it owns owner-channel polling and
 * the cluster-core operation lifecycle.
 */
typedef struct vemb_v16_client_vector_session
    vemb_v16_client_vector_session_t;

typedef enum vemb_v16_client_vector_cache_mode {
    VEMB_V16_CLIENT_VECTOR_CACHE_DISABLED = 0,
    /* The caller explicitly accepts a completed-vector snapshot until local
     * invalidation. This is not a linearizable cross-client cache. */
    VEMB_V16_CLIENT_VECTOR_CACHE_IMMUTABLE_SNAPSHOT = 1,
} vemb_v16_client_vector_cache_mode_t;

typedef struct vemb_v16_client_vector_session_options {
    vemb_v16_client_vector_cache_mode_t cache_mode;
    /* Entry count must satisfy the internal four-way cache geometry. Zero
     * keeps the cache disabled. */
    uint32_t cache_entries;
} vemb_v16_client_vector_session_options_t;

typedef void (*vemb_v16_client_vector_completion_cb)(
    void *priv, uint64_t caller_cookie,
    const vemb_v16_pipeline_resp_t *response,
    const float *vector);

vemb_v16_client_vector_session_t *vemb_v16_client_vector_session_create(
    vemb_v16_client_t *client);

vemb_v16_client_vector_session_t *
vemb_v16_client_vector_session_create_with_options(
    vemb_v16_client_t *client,
    const vemb_v16_client_vector_session_options_t *options);

/* The session copies set_name and elem_name. Completion is delivered exactly
 * once by poll() or close(). */
int vemb_v16_client_vector_session_submit(
    vemb_v16_client_vector_session_t *session, const char *set_name,
    const char *elem_name, uint64_t caller_cookie);

/* Submit all pending L0 leaders through their core-selected owner channels.
 * It never waits for a response. */
int vemb_v16_client_vector_session_flush(
    vemb_v16_client_vector_session_t *session);

/* Drive routing, nonblocking receives, completion fanout, and redirect retry.
 * Returns callback count, zero when no completion is available, or -1 when
 * the session is closing. */
int vemb_v16_client_vector_session_poll(
    vemb_v16_client_vector_session_t *session,
    vemb_v16_client_vector_completion_cb cb, void *priv);

/* Completes every outstanding request with ERR and releases L0 state. */
void vemb_v16_client_vector_session_close(
    vemb_v16_client_vector_session_t *session,
    vemb_v16_client_vector_completion_cb cb, void *priv);

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
 * Optional out_score receives the last response's score. Optional out_found
 * receives 1 if the last response was OK, 0 if NOT_FOUND.
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
 * Serialize a complete VSIM_INLINE frame into a user-provided buffer.
 * 'key' is the raw key as sent on the wire (already combined if needed).
 * Returns bytes written (>0), or -1 if buf_cap too small.
 */
ssize_t vemb_v16_serialize_vsim_inline(void *buf, size_t buf_cap,
                                       uint64_t channel_id, uint32_t req_id,
                                       const char *key, uint32_t key_len,
                                       const float *query_vector, uint32_t dim);

/*
 * Serialize a complete VEMB_INLINE frame, the TCP read operation. Its
 * successful response appends the vector bytes after response metadata.
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
 * UB-backed shared memory. Channel allocation/deallocation goes through the
 * server's TCP control endpoint. TCP is control-plane only; request and
 * response payloads remain on the mapped UB rings.
 *
 * The handle is opaque — callers never touch the ring layout. The
 * header is therefore safe to include from C++ translation units
 * without a C11 <stdatomic.h> dependency.
 *
 * Typical workflow:
 *   vemb_v16_aeron_close_all("tcp://127.0.0.1:6390");
 *   ch = vemb_v16_aeron_open("tcp://127.0.0.1:6390", dim);
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
 *   control_endpoint — TCP endpoint in the form "tcp://host:port" or
 *                       "host:port"
 *   dim       — vector dimension; server uses it to size ring slots
 * Returns NULL on any failure (control endpoint missing, alloc rejected,
 * ring open error). Caller owns the returned handle and must release it with
 * vemb_v16_aeron_close(). */
vemb_v16_aeron_channel_t *vemb_v16_aeron_open(const char *control_endpoint,
                                              uint32_t dim);

/* Open an Aeron channel via TCP ATTACH and the client-side peer-view resolver.
 * The transport is still UB-backed SPSC rings; TCP is only the control handshake.
 *   host  — server hostname or IP (e.g. "192.168.1.111")
 *   port  — server TCP port (same port redis-server listens on)
 *   dim   — vector dimension; server uses it to size ring slots
 * Returns NULL on any failure (TCP error, ATTACH rejected, mmap
 * error). Caller owns the handle and must release it with
 * vemb_v16_aeron_close(). */
vemb_v16_aeron_channel_t *vemb_v16_aeron_open_remote(const char *host,
                                                     uint16_t port,
                                                     uint32_t dim);

/* Open a channel with an explicit client-side peer-view resolver. The
 * resolver must map every ATTACH-advertised UB resource for client_host and
 * owner_id; missing mappings fail the ATTACH before data-plane publication.
 * The endpoint may be local or remote; local manifests use identical paths. */
vemb_v16_aeron_channel_t *vemb_v16_aeron_open_remote_with_peer_view(
    const char *host, uint16_t port, uint32_t dim,
    const vemb_v16_ub_peer_view_manifest_t *peer_view_manifest,
    const char *client_host, uint32_t owner_id);

/* Open the v2 batch channel through TCP ATTACH and map its
 * request/response descriptor rings plus byte arenas.
 *
 * requested_batch_size must be nonzero. requested_max_batch_bytes may be
 * zero to accept the server default. Returns NULL when v2 is unavailable,
 * the server is in migration, or any resource cannot be mapped. */
vemb_v16_aeron_batch_channel_t *vemb_v16_aeron_open_remote_batch(
    const char *host, uint16_t port, uint32_t dim,
    uint32_t requested_batch_size, uint32_t requested_max_batch_bytes);

vemb_v16_aeron_batch_channel_t *
vemb_v16_aeron_open_remote_batch_with_peer_view(
    const char *host, uint16_t port, uint32_t dim,
    uint32_t requested_batch_size, uint32_t requested_max_batch_bytes,
    const vemb_v16_ub_peer_view_manifest_t *peer_view_manifest,
    const char *client_host, uint32_t owner_id);

/* Unmap all four v2 resources and best-effort notify the server to close the
 * v2 logical channel. ch must be a live non-NULL channel. */
void vemb_v16_aeron_batch_close(vemb_v16_aeron_batch_channel_t *ch);

uint64_t vemb_v16_aeron_batch_channel_id(
    const vemb_v16_aeron_batch_channel_t *ch);
uint64_t vemb_v16_aeron_batch_topology_epoch(
    const vemb_v16_aeron_batch_channel_t *ch);
/* Copies the mappings negotiated by a live batch channel. */
void vemb_v16_aeron_batch_get_resources(
    const vemb_v16_aeron_batch_channel_t *ch,
    vemb_v16_aeron_batch_resources_t *out);

/* Publish one stable-topology VEMB_HANDLE batch with the topology epoch picked
 * by cluster core for this batch. submit_epoch is a property of this published
 * frame, not of the channel's ATTACH response. `ch` must be a live channel;
 * batch_id and item_count must be nonzero; keys and key_lens must point to
 * item_count entries, each key must be non-NULL, and each length must be in
 * [1, VEMB_V16_MAX_KEY_LEN]. Required pointer violations assert; invalid
 * batch ids, counts, or key lengths return RING_ERR_INVALID. The key array
 * describes unique items; CLI L0 coalescing remains the caller's next layer.
 * Returns RING_OK, RING_ERR_INVALID, or RING_ERR_FULL.
 */
int vemb_v16_aeron_batch_publish_handle_at_epoch(
    vemb_v16_aeron_batch_channel_t *ch, uint64_t batch_id,
    uint64_t submit_epoch, const char *const *keys,
    const uint16_t *key_lens, uint32_t item_count);

/* Compatibility wrapper for fixed-topology callers. New owner-session code
 * must use publish_handle_at_epoch() with the core-selected route epoch. */
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

/* Invoked once for a successfully materialized v2 L0 group before its L0
 * entry is finished and before leader/follower fan-out. final_key and
 * vector_view->data are borrowed only for the duration of this callback.
 * Direct v1 requests, v1 fallback groups, non-OK responses, and invalid
 * vector views do not invoke this callback. */
typedef void (*vemb_v16_aeron_batch_materialized_group_cb)(
    void *priv, const char *final_key, uint16_t key_len,
    const vemb_v16_resp_t *response,
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
 * Returns NULL when v1/v2 ATTACH or resource mapping fails; this API never
 * returns a v1-only batch session. Allocation failure asserts. */
vemb_v16_aeron_batch_client_t *vemb_v16_aeron_batch_client_open_remote(
    const char *host, uint16_t port, uint32_t dim,
    const vemb_v16_aeron_batch_client_options_t *options);

vemb_v16_aeron_batch_client_t *
vemb_v16_aeron_batch_client_open_remote_with_peer_view(
    const char *host, uint16_t port, uint32_t dim,
    const vemb_v16_aeron_batch_client_options_t *options,
    const vemb_v16_ub_peer_view_manifest_t *peer_view_manifest,
    const char *client_host, uint32_t owner_id);

/* Submits one logical VEMB_HANDLE read for required final key bytes. Returns 0
 * once the caller cookie is owned by the session, -2 if v1 fallback pressure
 * prevents acceptance, and -1 for invalid key length or a closing session. */
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
 * available, or -1 while closing. poll flushes on the configured deadline and
 * retries a deadline-expired frame after v2 ring/arena backpressure. */
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

/* Like poll_shared_vector(), with an optional once-per-v2-group hook after
 * successful materialization and before l0_finish() releases the borrowed
 * exact key. The hook is synchronous and must not retain either borrowed
 * pointer. Logical completion cb semantics are unchanged. */
int vemb_v16_aeron_batch_client_poll_shared_vector_with_materialization_hook(
    vemb_v16_aeron_batch_client_t *client,
    vemb_v16_aeron_batch_materialized_group_cb group_cb, void *group_priv,
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
 * both channels plus all fixed-pool state. client must be live and non-NULL. */
void vemb_v16_aeron_batch_client_close(
    vemb_v16_aeron_batch_client_t *client,
    vemb_v16_aeron_batch_completion_cb cb, void *priv);

/* Close one live channel: unmaps both rings and notifies the server through
 * TCP control to release its state. Control errors are swallowed because the
 * rings are already unmapped locally. */
void vemb_v16_aeron_close(vemb_v16_aeron_channel_t *ch);

/* Close every channel currently registered on the given control endpoint.
 * Returns the server-reported count closed (>= 0) or -1 on protocol
 * error. Intended for best-effort stale-state cleanup before a run. */
int vemb_v16_aeron_close_all(const char *control_endpoint);

/* Channel id — stamp this into vemb_v16_req_t::channel_id when building
 * request frames via vemb_v16_serialize_* or by hand. ch is required. */
uint64_t vemb_v16_aeron_channel_id(const vemb_v16_aeron_channel_t *ch);

/* Server-owned UB allocation identity for this required channel. It changes
 * when a channel is re-attached to different backing resources. */
uint64_t vemb_v16_aeron_channel_resource_generation(
    const vemb_v16_aeron_channel_t *ch);

/* Non-blocking publish into the request ring.
 * Returns:
 *    0   on success
 *   -1   if the ring is full (drain responses and retry)
 *   -2   if len > slot_size (programming error)
 * ch and buf are required. */
int vemb_v16_aeron_publish_request(vemb_v16_aeron_channel_t *ch,
                                   const void *buf, uint32_t len);

/* Non-blocking batch publish into the request ring. `bufs` may contain
 * variable-length request frames (VADD inline frames and handle frames can
 * be mixed). The call is all-or-none when the ring lacks capacity. */
int vemb_v16_aeron_publish_request_batch(vemb_v16_aeron_channel_t *ch,
                                         const void *const *bufs,
                                         const uint32_t *lens,
                                         uint32_t count);

/* Non-blocking poll from the response ring. ch and buf are required. Returns
 * bytes copied into buf (>0) on success or 0 if empty. If max_len exceeds the
 * ring slot size, only slot_size bytes are copied. */
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
