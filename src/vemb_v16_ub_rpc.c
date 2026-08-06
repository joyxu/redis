#define _GNU_SOURCE

#include "vemb_v16_ub_rpc.h"

#include "cpu_relax.h"
#include "vemb_v16_cacheline.h"
#include "vemb_v16_log.h"
#include "vemb_v16_mapped_region.h"
#include "vemb_v16_util.h"
#include "zmalloc.h"

#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <string.h>

#define VEMB_V16_UB_RPC_MAGIC 0x56315552u
#define VEMB_V16_UB_RPC_VERSION 1u
#define VEMB_V16_UB_RPC_RING_BITS 12u
#define VEMB_V16_UB_RPC_RING_SIZE (1u << VEMB_V16_UB_RPC_RING_BITS)
#define VEMB_V16_UB_RPC_RING_MASK (VEMB_V16_UB_RPC_RING_SIZE - 1u)
#define VEMB_V16_UB_RPC_PENDING_BITS 12u
#define VEMB_V16_UB_RPC_PENDING_SIZE (1u << VEMB_V16_UB_RPC_PENDING_BITS)
#define VEMB_V16_UB_RPC_PENDING_MASK (VEMB_V16_UB_RPC_PENDING_SIZE - 1u)
#define VEMB_V16_UB_RPC_DEFAULT_TIMEOUT_MS 100u
#define VEMB_V16_UB_RPC_LOG_LIMIT 32u
#define VEMB_V16_UB_RPC_LISTENER_BATCH 64u

typedef struct vemb_v16_ub_rpc_wire_req {
    uint32_t magic;
    uint32_t version;
    uint32_t kind;
    uint32_t reserved0;
    union {
        vemb_v16_ub_lookup_rpc_req_t lookup;
        vemb_v16_ub_migration_rpc_req_t migration;
    } u;
} vemb_v16_ub_rpc_wire_req_t;

typedef struct vemb_v16_ub_rpc_wire_resp {
    uint32_t magic;
    uint32_t version;
    uint32_t kind;
    uint32_t reserved0;
    union {
        vemb_v16_ub_lookup_rpc_resp_t lookup;
        vemb_v16_ub_migration_rpc_resp_t migration;
    } u;
} vemb_v16_ub_rpc_wire_resp_t;

typedef struct vemb_v16_ub_rpc_ring_slot {
    _Alignas(64) atomic_uint_fast64_t sequence;
    uint8_t payload[];
} vemb_v16_ub_rpc_ring_slot_t;

typedef struct vemb_v16_ub_rpc_shared_ring {
    uint32_t magic;
    uint32_t version;
    uint32_t slot_size;
    uint32_t slot_count;
    uint32_t slot_mask;
    uint32_t slot_stride;
    uint32_t reserved0[10];
    _Alignas(64) atomic_uint_fast64_t head;
    _Alignas(64) atomic_uint_fast64_t tail;
    uint8_t slots[];
} vemb_v16_ub_rpc_shared_ring_t;

typedef struct vemb_v16_ub_rpc_ring {
    vemb_v16_ub_rpc_ring_config_t config;
    vemb_v16_mapped_region_t mapping;
    vemb_v16_ub_rpc_shared_ring_t *ring;
    size_t bytes;
    uint32_t slot_size;
    uint32_t slot_stride;
} vemb_v16_ub_rpc_ring_t;

typedef enum vemb_v16_ub_rpc_pending_state {
    VEMB_V16_UB_RPC_PENDING_EMPTY = 0,
    VEMB_V16_UB_RPC_PENDING_CLAIMING = 1,
    VEMB_V16_UB_RPC_PENDING_WAITING = 2,
    VEMB_V16_UB_RPC_PENDING_WRITING = 3,
    VEMB_V16_UB_RPC_PENDING_READY = 4,
} vemb_v16_ub_rpc_pending_state_t;

typedef struct vemb_v16_ub_rpc_pending {
    atomic_uint state;
    uint32_t reserved0;
    atomic_uint_fast64_t request_id;
    vemb_v16_ub_rpc_wire_resp_t resp;
} vemb_v16_ub_rpc_pending_t;

typedef struct vemb_v16_ub_rpc_peer_state {
    uint32_t owner_id;
    vemb_v16_ub_rpc_ring_t request;
    vemb_v16_ub_rpc_ring_t response;
    vemb_v16_ub_rpc_ring_t inbound_request;
    vemb_v16_ub_rpc_ring_t outbound_response;
    vemb_v16_ub_rpc_pending_t pending[VEMB_V16_UB_RPC_PENDING_SIZE];
} vemb_v16_ub_rpc_peer_state_t;

struct vemb_v16_ub_rpc {
    vemb_v16_tlc_t *tlc;
    uint32_t local_owner_id;
    uint32_t timeout_ms;
    uint32_t peer_count;
    vemb_v16_ub_rpc_peer_state_t peers[VEMB_V16_UB_RPC_MAX_PEERS];
    pthread_t thread;
    int thread_started;
    atomic_int running;
    atomic_uint_fast32_t timeout_logs;
    atomic_uint_fast32_t ring_full_logs;
    atomic_uint_fast32_t error_logs;
    atomic_uint_fast32_t request_logs;
    atomic_uint_fast32_t response_logs;
    atomic_uint_fast32_t refcount;
    atomic_uint retired;
};

static void vemb_v16_ub_rpc_destroy_final(vemb_v16_ub_rpc_t *rpc);
void vemb_v16_ub_rpc_release(vemb_v16_ub_rpc_t *rpc);

static vemb_v16_ub_rpc_t *lookup_runtime_acquire(vemb_v16_tlc_t *tlc) {
    if (!tlc)
        return NULL;
    for (;;) {
        vemb_v16_ub_rpc_t *rpc = atomic_load_explicit(
            &tlc->current_lookup_rpc_runtime,
            memory_order_acquire);
        if (!rpc)
            return NULL;
        atomic_fetch_add_explicit(&rpc->refcount, 1, memory_order_acq_rel);
        if (rpc == atomic_load_explicit(&tlc->current_lookup_rpc_runtime,
                                        memory_order_acquire)) {
            return rpc;
        }
        vemb_v16_ub_rpc_release(rpc);
    }
}

void vemb_v16_ub_rpc_release(vemb_v16_ub_rpc_t *rpc) {
    if (!rpc)
        return;
    uint32_t prev = atomic_fetch_sub_explicit(&rpc->refcount,
                                              1,
                                              memory_order_acq_rel);
    if (prev == 1 &&
        atomic_load_explicit(&rpc->retired, memory_order_acquire)) {
        vemb_v16_ub_rpc_destroy_final(rpc);
    }
}

static void vemb_v16_ub_rpc_retire(vemb_v16_ub_rpc_t *rpc) {
    if (!rpc)
        return;
    atomic_store_explicit(&rpc->retired, 1, memory_order_release);
    vemb_v16_ub_rpc_release(rpc);
}

static uint64_t timeout_from_req_ns(const vemb_v16_ub_rpc_t *rpc,
                                    const vemb_v16_ub_lookup_rpc_req_t *req) {
    if (req && req->timeout_ns)
        return req->timeout_ns;
    uint32_t timeout_ms = rpc->timeout_ms ? rpc->timeout_ms :
        VEMB_V16_UB_RPC_DEFAULT_TIMEOUT_MS;
    return (uint64_t)timeout_ms * 1000000ull;
}

static uint64_t timeout_from_migration_req_ns(
        const vemb_v16_ub_rpc_t *rpc,
        const vemb_v16_ub_migration_rpc_req_t *req) {
    if (req && req->timeout_ns)
        return req->timeout_ns;
    uint32_t timeout_ms = rpc->timeout_ms ? rpc->timeout_ms :
        VEMB_V16_UB_RPC_DEFAULT_TIMEOUT_MS;
    return (uint64_t)timeout_ms * 1000000ull;
}

static uint64_t wire_req_request_id(
        const vemb_v16_ub_rpc_wire_req_t *wire_req) {
    if (wire_req->kind == VEMB_V16_UB_RPC_FRAME_MIGRATION)
        return wire_req->u.migration.request_id;
    return wire_req->u.lookup.request_id;
}

static uint64_t wire_resp_request_id(
        const vemb_v16_ub_rpc_wire_resp_t *wire_resp) {
    if (wire_resp->kind == VEMB_V16_UB_RPC_FRAME_MIGRATION)
        return wire_resp->u.migration.request_id;
    return wire_resp->u.lookup.request_id;
}

static uint64_t wire_req_key_hash(
        const vemb_v16_ub_rpc_wire_req_t *wire_req) {
    if (wire_req->kind == VEMB_V16_UB_RPC_FRAME_MIGRATION)
        return wire_req->u.migration.key_hash;
    return wire_req->u.lookup.key_hash;
}

static uint64_t wire_resp_key_hash(
        const vemb_v16_ub_rpc_wire_resp_t *wire_resp) {
    if (wire_resp->kind == VEMB_V16_UB_RPC_FRAME_MIGRATION)
        return wire_resp->u.migration.key_hash;
    return wire_resp->u.lookup.key_hash;
}

static uint32_t wire_req_src_owner(
        const vemb_v16_ub_rpc_wire_req_t *wire_req) {
    if (wire_req->kind == VEMB_V16_UB_RPC_FRAME_MIGRATION)
        return wire_req->u.migration.src_owner_id;
    return wire_req->u.lookup.src_owner_id;
}

static uint32_t wire_req_dst_owner(
        const vemb_v16_ub_rpc_wire_req_t *wire_req) {
    if (wire_req->kind == VEMB_V16_UB_RPC_FRAME_MIGRATION)
        return wire_req->u.migration.dst_owner_id;
    return wire_req->u.lookup.dst_owner_id;
}

static uint64_t timeout_from_wire_req_ns(
        const vemb_v16_ub_rpc_t *rpc,
        const vemb_v16_ub_rpc_wire_req_t *wire_req) {
    if (wire_req->kind == VEMB_V16_UB_RPC_FRAME_MIGRATION) {
        return timeout_from_migration_req_ns(rpc,
                                             &wire_req->u.migration);
    }
    return timeout_from_req_ns(rpc, &wire_req->u.lookup);
}

static void wire_resp_set_status(vemb_v16_ub_rpc_wire_resp_t *wire_resp,
                                 uint32_t status) {
    if (wire_resp->kind == VEMB_V16_UB_RPC_FRAME_MIGRATION)
        wire_resp->u.migration.status = status;
    else
        wire_resp->u.lookup.status = status;
}

static void tiny_pause(void) {
    for (uint32_t i = 0; i < 64; i++)
        cpu_relax();
    sched_yield();
}

static uint32_t rpc_ring_slot_stride(uint32_t slot_size) {
    return (uint32_t)align_up_size(
        sizeof(vemb_v16_ub_rpc_ring_slot_t) + (size_t)slot_size,
        CACHELINE_SIZE);
}

static size_t rpc_ring_bytes(uint32_t slot_size) {
    uint32_t stride = rpc_ring_slot_stride(slot_size);
    return sizeof(vemb_v16_ub_rpc_shared_ring_t) +
           (size_t)stride * VEMB_V16_UB_RPC_RING_SIZE;
}

static vemb_v16_ub_rpc_ring_slot_t *rpc_ring_slot(
        vemb_v16_ub_rpc_shared_ring_t *ring,
        uint64_t pos) {
    return (vemb_v16_ub_rpc_ring_slot_t *)
        (ring->slots + (pos & ring->slot_mask) * ring->slot_stride);
}

static void rpc_ring_init(vemb_v16_ub_rpc_shared_ring_t *ring,
                          uint32_t slot_size) {
    uint32_t stride = rpc_ring_slot_stride(slot_size);
    memset(ring, 0, rpc_ring_bytes(slot_size));
    ring->magic = VEMB_V16_UB_RPC_MAGIC;
    ring->version = VEMB_V16_UB_RPC_VERSION;
    ring->slot_size = slot_size;
    ring->slot_count = VEMB_V16_UB_RPC_RING_SIZE;
    ring->slot_mask = VEMB_V16_UB_RPC_RING_MASK;
    ring->slot_stride = stride;
    atomic_init(&ring->head, 0);
    atomic_init(&ring->tail, 0);
    for (uint32_t i = 0; i < VEMB_V16_UB_RPC_RING_SIZE; i++) {
        vemb_v16_ub_rpc_ring_slot_t *slot = rpc_ring_slot(ring, i);
        atomic_init(&slot->sequence, i);
    }
}

static int rpc_ring_ready(const vemb_v16_ub_rpc_ring_t *ring) {
    return ring &&
           ring->ring &&
           ring->ring->magic == VEMB_V16_UB_RPC_MAGIC &&
           ring->ring->version == VEMB_V16_UB_RPC_VERSION &&
           ring->ring->slot_size == ring->slot_size &&
           ring->ring->slot_count == VEMB_V16_UB_RPC_RING_SIZE &&
           ring->ring->slot_mask == VEMB_V16_UB_RPC_RING_MASK &&
           ring->ring->slot_stride == ring->slot_stride;
}

static int rpc_ring_publish(vemb_v16_ub_rpc_shared_ring_t *ring,
                            const void *payload) {
    uint64_t pos = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    vemb_v16_ub_rpc_ring_slot_t *slot = NULL;
    for (;;) {
        slot = rpc_ring_slot(ring, pos);
        uint64_t seq = atomic_load_explicit(&slot->sequence,
                                            memory_order_acquire);
        int64_t diff = (int64_t)seq - (int64_t)pos;
        if (diff == 0) {
            uint64_t desired = pos + 1;
            if (atomic_compare_exchange_weak_explicit(
                    &ring->tail,
                    &pos,
                    desired,
                    memory_order_relaxed,
                    memory_order_relaxed)) {
                break;
            }
        } else if (diff < 0) {
            return -1;
        } else {
            pos = atomic_load_explicit(&ring->tail,
                                       memory_order_relaxed);
        }
    }
    memcpy(slot->payload, payload, ring->slot_size);
    atomic_store_explicit(&slot->sequence, pos + 1, memory_order_release);
    return 0;
}

static int rpc_ring_poll(vemb_v16_ub_rpc_shared_ring_t *ring,
                         void *payload) {
    uint64_t pos = atomic_load_explicit(&ring->head, memory_order_relaxed);
    vemb_v16_ub_rpc_ring_slot_t *slot = NULL;
    for (;;) {
        slot = rpc_ring_slot(ring, pos);
        uint64_t seq = atomic_load_explicit(&slot->sequence,
                                            memory_order_acquire);
        int64_t diff = (int64_t)seq - (int64_t)(pos + 1);
        if (diff == 0) {
            uint64_t desired = pos + 1;
            if (atomic_compare_exchange_weak_explicit(
                    &ring->head,
                    &pos,
                    desired,
                    memory_order_relaxed,
                    memory_order_relaxed)) {
                break;
            }
        } else if (diff < 0) {
            return 0;
        } else {
            pos = atomic_load_explicit(&ring->head,
                                       memory_order_relaxed);
        }
    }
    memcpy(payload, slot->payload, ring->slot_size);
    atomic_store_explicit(&slot->sequence,
                          pos + ring->slot_count,
                          memory_order_release);
    return 1;
}

static void log_limited(atomic_uint_fast32_t *counter,
                        int level,
                        const char *fmt,
                        uint32_t owner_id,
                        uint64_t request_id,
                        uint64_t key_hash,
                        const char *path,
                        int err) {
    uint32_t n = atomic_fetch_add_explicit(counter, 1,
                                          memory_order_relaxed);
    if (n >= VEMB_V16_UB_RPC_LOG_LIMIT)
        return;
    serverLog(level,
              fmt,
              owner_id,
              (unsigned long long)request_id,
              (unsigned long long)key_hash,
              path ? path : "(none)",
              err,
              strerror(err));
}

/* Diagnostic: explain *why* rpc_ring_ready returned false.
 * Used to disambiguate "consumer never initialized" (magic=0) from
 * "consumer wrote but producer can't see it" (cache coherency / wrong path)
 * from "slot_size mismatch" (config bug). Gated by the same per-rpc
 * timeout_logs counter so the limit still applies. */
static void log_ring_not_ready_diag(vemb_v16_ub_rpc_t *rpc,
                                    const vemb_v16_ub_rpc_ring_t *ring,
                                    uint32_t owner_id,
                                    uint64_t request_id,
                                    uint64_t key_hash) {
    uint32_t n = atomic_fetch_add_explicit(&rpc->timeout_logs, 1,
                                          memory_order_relaxed);
    if (n >= VEMB_V16_UB_RPC_LOG_LIMIT)
        return;
    const char *reason = "null_ring_struct";
    uint32_t h_magic = 0, h_version = 0;
    uint32_t h_slot_size = 0, h_slot_count = 0;
    uint32_t h_slot_mask = 0, h_slot_stride = 0;
    if (ring) {
        if (!ring->ring) {
            reason = "null_mapped_ptr";
        } else {
            h_magic = ring->ring->magic;
            h_version = ring->ring->version;
            h_slot_size = ring->ring->slot_size;
            h_slot_count = ring->ring->slot_count;
            h_slot_mask = ring->ring->slot_mask;
            h_slot_stride = ring->ring->slot_stride;
            if (h_magic != VEMB_V16_UB_RPC_MAGIC) reason = "magic";
            else if (h_version != VEMB_V16_UB_RPC_VERSION) reason = "version";
            else if (h_slot_size != ring->slot_size) reason = "slot_size";
            else if (h_slot_count != VEMB_V16_UB_RPC_RING_SIZE) reason = "slot_count";
            else if (h_slot_mask != VEMB_V16_UB_RPC_RING_MASK) reason = "slot_mask";
            else if (h_slot_stride != ring->slot_stride) reason = "slot_stride";
            else reason = "race";
        }
    }
    serverLog(LL_NOTICE,
              "vemb_v16 ub rpc ring not ready diag: owner=%u request_id=%llu key_hash=%llu path=%s mmap_offset=%lld reason=%s "
              "header(magic=0x%x version=%u slot_size=%u slot_count=%u slot_mask=%u slot_stride=%u) "
              "expected(slot_size=%u slot_count=%u slot_mask=%u slot_stride=%u)",
              owner_id,
              (unsigned long long)request_id,
              (unsigned long long)key_hash,
              (ring && ring->config.path[0]) ? ring->config.path : "(none)",
              (long long)(ring ? (long)ring->config.mmap_offset : -1),
              reason,
              h_magic, h_version, h_slot_size, h_slot_count, h_slot_mask, h_slot_stride,
              ring ? ring->slot_size : 0,
              VEMB_V16_UB_RPC_RING_SIZE,
              VEMB_V16_UB_RPC_RING_MASK,
              ring ? ring->slot_stride : 0);
}

static int ring_config_valid(const vemb_v16_ub_rpc_ring_config_t *config) {
    return config &&
           (config->backend_type == VEMB_V16_REGION_LOCAL_SHM ||
            config->backend_type == VEMB_V16_REGION_UB) &&
           config->path[0] != '\0';
}

static int ring_open(vemb_v16_ub_rpc_ring_t *ring,
                     const vemb_v16_ub_rpc_ring_config_t *config,
                     uint32_t slot_size,
                     int init_on_open) {
    if (!ring_config_valid(config) || slot_size == 0)
        return -1;
    memset(ring, 0, sizeof(*ring));
    ring->mapping.fd = -1;
    ring->config = *config;
    ring->slot_size = slot_size;
    ring->slot_stride = rpc_ring_slot_stride(slot_size);
    ring->bytes = rpc_ring_bytes(slot_size);
    if (vemb_v16_mapped_region_open(&ring->mapping,
                                    config->backend_type,
                                    config->path,
                                    config->mmap_offset,
                                    ring->bytes) != 0) {
        return -1;
    }
    ring->ring = (vemb_v16_ub_rpc_shared_ring_t *)ring->mapping.mapped_addr;
    if (init_on_open && !rpc_ring_ready(ring))
        rpc_ring_init(ring->ring, slot_size);
    return 0;
}

static void ring_close(vemb_v16_ub_rpc_ring_t *ring) {
    if (!ring)
        return;
    vemb_v16_mapped_region_close(&ring->mapping);
    ring->ring = NULL;
    ring->bytes = 0;
}

static int ring_reset(const vemb_v16_ub_rpc_ring_config_t *config,
                      uint32_t slot_size) {
    if (!ring_config_valid(config) || slot_size == 0)
        return -1;
    vemb_v16_mapped_region_t mapping;
    memset(&mapping, 0, sizeof(mapping));
    mapping.fd = -1;
    size_t bytes = rpc_ring_bytes(slot_size);
    if (vemb_v16_mapped_region_open(&mapping,
                                    config->backend_type,
                                    config->path,
                                    config->mmap_offset,
                                    bytes) != 0) {
        return -1;
    }
    rpc_ring_init((vemb_v16_ub_rpc_shared_ring_t *)mapping.mapped_addr,
                  slot_size);
    vemb_v16_mapped_region_close(&mapping);
    return 0;
}

static vemb_v16_ub_rpc_peer_state_t *find_peer(vemb_v16_ub_rpc_t *rpc,
                                               uint32_t owner_id) {
    for (uint32_t i = 0; i < rpc->peer_count; i++) {
        if (rpc->peers[i].owner_id == owner_id)
            return &rpc->peers[i];
    }
    return NULL;
}

static vemb_v16_ub_rpc_pending_t *pending_claim(
        vemb_v16_ub_rpc_peer_state_t *peer,
        uint64_t request_id) {
    uint32_t start = (uint32_t)request_id & VEMB_V16_UB_RPC_PENDING_MASK;
    for (uint32_t i = 0; i < VEMB_V16_UB_RPC_PENDING_SIZE; i++) {
        vemb_v16_ub_rpc_pending_t *slot =
            &peer->pending[(start + i) & VEMB_V16_UB_RPC_PENDING_MASK];
        uint32_t expected = VEMB_V16_UB_RPC_PENDING_EMPTY;
        if (atomic_compare_exchange_strong_explicit(
                &slot->state,
                &expected,
                VEMB_V16_UB_RPC_PENDING_CLAIMING,
                memory_order_acquire,
                memory_order_relaxed)) {
            atomic_store_explicit(&slot->request_id,
                                  request_id,
                                  memory_order_relaxed);
            memset(&slot->resp, 0, sizeof(slot->resp));
            atomic_store_explicit(&slot->state,
                                  VEMB_V16_UB_RPC_PENDING_WAITING,
                                  memory_order_release);
            return slot;
        }
    }
    return NULL;
}

static void pending_release_waiting(vemb_v16_ub_rpc_pending_t *slot) {
    if (!slot)
        return;
    uint32_t expected = VEMB_V16_UB_RPC_PENDING_WAITING;
    if (atomic_compare_exchange_strong_explicit(
            &slot->state,
            &expected,
            VEMB_V16_UB_RPC_PENDING_EMPTY,
            memory_order_acq_rel,
            memory_order_relaxed)) {
    }
}

static vemb_v16_ub_rpc_pending_t *pending_find(
        vemb_v16_ub_rpc_peer_state_t *peer,
        uint64_t request_id) {
    uint32_t start = (uint32_t)request_id & VEMB_V16_UB_RPC_PENDING_MASK;
    for (uint32_t i = 0; i < VEMB_V16_UB_RPC_PENDING_SIZE; i++) {
        vemb_v16_ub_rpc_pending_t *slot =
            &peer->pending[(start + i) & VEMB_V16_UB_RPC_PENDING_MASK];
        uint32_t state = atomic_load_explicit(&slot->state,
                                              memory_order_acquire);
        if (state == VEMB_V16_UB_RPC_PENDING_WAITING &&
            atomic_load_explicit(&slot->request_id,
                                 memory_order_acquire) == request_id) {
            return slot;
        }
    }
    return NULL;
}

static int pending_complete(vemb_v16_ub_rpc_peer_state_t *peer,
                            const vemb_v16_ub_rpc_wire_resp_t *resp) {
    vemb_v16_ub_rpc_pending_t *slot =
        pending_find(peer, wire_resp_request_id(resp));
    if (!slot)
        return -1;
    uint32_t expected = VEMB_V16_UB_RPC_PENDING_WAITING;
    if (!atomic_compare_exchange_strong_explicit(
            &slot->state,
            &expected,
            VEMB_V16_UB_RPC_PENDING_WRITING,
            memory_order_acq_rel,
            memory_order_relaxed)) {
        return -1;
    }
    slot->resp = *resp;
    atomic_store_explicit(&slot->state,
                          VEMB_V16_UB_RPC_PENDING_READY,
                          memory_order_release);
    return 0;
}

static int pending_wait(vemb_v16_ub_rpc_t *rpc,
                        vemb_v16_ub_rpc_peer_state_t *peer,
                        vemb_v16_ub_rpc_pending_t *slot,
                        const vemb_v16_ub_rpc_wire_req_t *req,
                        vemb_v16_ub_rpc_wire_resp_t *resp,
                        uint64_t deadline_ns) {
    (void)peer;
    while (atomic_load_explicit(&rpc->running, memory_order_acquire)) {
        uint32_t state = atomic_load_explicit(&slot->state,
                                              memory_order_acquire);
        if (state == VEMB_V16_UB_RPC_PENDING_READY) {
            *resp = slot->resp;
            atomic_store_explicit(&slot->state,
                                  VEMB_V16_UB_RPC_PENDING_EMPTY,
                                  memory_order_release);
            return 0;
        }
        if (vemb_v16_monotonic_ns() >= deadline_ns) {
            uint32_t expected = VEMB_V16_UB_RPC_PENDING_WAITING;
            if (atomic_compare_exchange_strong_explicit(
                    &slot->state,
                    &expected,
                    VEMB_V16_UB_RPC_PENDING_EMPTY,
                    memory_order_acq_rel,
                    memory_order_relaxed)) {
                resp->magic = VEMB_V16_UB_RPC_MAGIC;
                resp->version = VEMB_V16_UB_RPC_VERSION;
                resp->kind = req->kind;
                if (resp->kind == VEMB_V16_UB_RPC_FRAME_MIGRATION) {
                    resp->u.migration.request_id =
                        req->u.migration.request_id;
                    resp->u.migration.key_hash = req->u.migration.key_hash;
                    resp->u.migration.op = req->u.migration.op;
                    wire_resp_set_status(
                        resp,
                        VEMB_V16_UB_MIGRATION_RPC_TIMEOUT);
                } else {
                    resp->u.lookup.request_id = req->u.lookup.request_id;
                    resp->u.lookup.key_hash = req->u.lookup.key_hash;
                    wire_resp_set_status(resp,
                                         VEMB_V16_UB_LOOKUP_RPC_TIMEOUT);
                }
                log_limited(&rpc->timeout_logs,
                            LL_NOTICE,
                            "vemb_v16 ub rpc response timeout: owner=%u request_id=%llu key_hash=%llu path=%s errno=%d error=%s",
                            wire_req_dst_owner(req),
                            wire_req_request_id(req),
                            wire_req_key_hash(req),
                            NULL,
                            ETIMEDOUT);
                return -1;
            }
        }
        tiny_pause();
    }
    resp->magic = VEMB_V16_UB_RPC_MAGIC;
    resp->version = VEMB_V16_UB_RPC_VERSION;
    resp->kind = req->kind;
    wire_resp_set_status(resp,
                         req->kind == VEMB_V16_UB_RPC_FRAME_MIGRATION ?
                             VEMB_V16_UB_MIGRATION_RPC_ERROR :
                             VEMB_V16_UB_LOOKUP_RPC_ERROR);
    return -1;
}

static int publish_with_deadline(vemb_v16_ub_rpc_t *rpc,
                                 vemb_v16_ub_rpc_ring_t *ring,
                                 const void *slot,
                                 uint64_t deadline_ns,
                                 uint32_t owner_id,
                                 uint64_t request_id,
                                 uint64_t key_hash,
                                 uint32_t *status) {
    int saw_full = 0;
    while (atomic_load_explicit(&rpc->running, memory_order_acquire)) {
        if (!rpc_ring_ready(ring)) {
            if (vemb_v16_monotonic_ns() >= deadline_ns) {
                if (status)
                    *status = VEMB_V16_UB_LOOKUP_RPC_TIMEOUT;
                /* Diagnostic: capture which field mismatched so we can
                 * tell apart "consumer never inited" (magic=0) from
                 * "consumer inited but producer can't see it" (cross-map
                 * cache / wrong path) from "slot_size config bug". */
                log_ring_not_ready_diag(rpc, ring, owner_id, request_id, key_hash);
                return -1;
            }
            tiny_pause();
            continue;
        }

        int rc = rpc_ring_publish(ring->ring, slot);
        if (rc == 0)
            return 0;
        saw_full = 1;
        if (vemb_v16_monotonic_ns() >= deadline_ns) {
            if (status)
                *status = VEMB_V16_UB_LOOKUP_RPC_BUSY;
            log_limited(&rpc->ring_full_logs,
                        LL_NOTICE,
                        "vemb_v16 ub rpc ring full: owner=%u request_id=%llu key_hash=%llu path=%s errno=%d error=%s",
                        owner_id,
                        request_id,
                        key_hash,
                        ring->config.path,
                        EAGAIN);
            return -1;
        }
        tiny_pause();
    }

    if (status)
        *status = saw_full ? VEMB_V16_UB_LOOKUP_RPC_BUSY :
            VEMB_V16_UB_LOOKUP_RPC_ERROR;
    return -1;
}

int vemb_v16_ub_rpc_lookup(void *arg,
                           const vemb_v16_ub_lookup_rpc_req_t *req,
                           vemb_v16_ub_lookup_rpc_resp_t *resp) {
    vemb_v16_tlc_t *tlc = arg;
    if (!tlc || !req || !resp)
        return -1;
    vemb_v16_ub_rpc_t *rpc = lookup_runtime_acquire(tlc);
    if (!rpc)
        return -1;
    memset(resp, 0, sizeof(*resp));
    resp->request_id = req->request_id;
    resp->key_hash = req->key_hash;

    if (req->dst_owner_id == rpc->local_owner_id) {
        int local_rc = vemb_v16_tlc_lookup_rpc_local_handler(rpc->tlc, req, resp);
        vemb_v16_ub_rpc_release(rpc);
        return local_rc;
    }

    vemb_v16_ub_rpc_peer_state_t *peer = find_peer(rpc, req->dst_owner_id);
    if (!peer) {
        resp->status = VEMB_V16_UB_LOOKUP_RPC_ERROR;
        log_limited(&rpc->error_logs,
                    LL_WARNING,
                    "vemb_v16 ub rpc peer missing: owner=%u request_id=%llu key_hash=%llu path=%s errno=%d error=%s",
                    req->dst_owner_id,
                    req->request_id,
                    req->key_hash,
                    NULL,
                    ENOENT);
        vemb_v16_ub_rpc_release(rpc);
        return 0;
    }

    vemb_v16_ub_rpc_pending_t *pending =
        pending_claim(peer, req->request_id);
    if (!pending) {
        resp->status = VEMB_V16_UB_LOOKUP_RPC_BUSY;
        log_limited(&rpc->ring_full_logs,
                    LL_NOTICE,
                    "vemb_v16 ub rpc pending full: owner=%u request_id=%llu key_hash=%llu path=%s errno=%d error=%s",
                    req->dst_owner_id,
                    req->request_id,
                    req->key_hash,
                    NULL,
                    EAGAIN);
        vemb_v16_ub_rpc_release(rpc);
        return 0;
    }

    vemb_v16_ub_rpc_wire_req_t wire_req = {
        .magic = VEMB_V16_UB_RPC_MAGIC,
        .version = VEMB_V16_UB_RPC_VERSION,
        .kind = VEMB_V16_UB_RPC_FRAME_LOOKUP,
        .u.lookup = *req,
    };
    uint64_t timeout_ns = timeout_from_req_ns(rpc, req);
    uint64_t deadline_ns = vemb_v16_monotonic_ns() + timeout_ns;
    uint32_t status = VEMB_V16_UB_LOOKUP_RPC_OK;
    int rc = publish_with_deadline(rpc,
                                   &peer->request,
                                   &wire_req,
                                   deadline_ns,
                                   req->dst_owner_id,
                                   req->request_id,
                                   req->key_hash,
                                   &status);
    if (rc != 0) {
        pending_release_waiting(pending);
        resp->status = status;
        vemb_v16_ub_rpc_release(rpc);
        return 0;
    }

    vemb_v16_ub_rpc_wire_resp_t wire_resp;
    memset(&wire_resp, 0, sizeof(wire_resp));
    (void)pending_wait(rpc, peer, pending, &wire_req, &wire_resp,
                       deadline_ns);
    if (wire_resp.kind == VEMB_V16_UB_RPC_FRAME_LOOKUP)
        *resp = wire_resp.u.lookup;
    else
        resp->status = VEMB_V16_UB_LOOKUP_RPC_ERROR;
    vemb_v16_ub_rpc_release(rpc);
    return 0;
}

int vemb_v16_ub_rpc_migrate_request(
    void *arg,
    const vemb_v16_ub_migration_rpc_req_t *req,
    vemb_v16_ub_migration_rpc_resp_t *resp) {
    vemb_v16_ub_rpc_t *rpc = arg;
    if (!rpc || !req || !resp)
        return -1;
    memset(resp, 0, sizeof(*resp));
    resp->request_id = req->request_id;
    resp->key_hash = req->key_hash;
    resp->op = req->op;

    if (req->dst_owner_id == rpc->local_owner_id) {
        return vemb_v16_tlc_migration_rpc_local_handler(rpc->tlc,
                                                        req,
                                                        resp);
    }

    vemb_v16_ub_rpc_peer_state_t *peer = find_peer(rpc, req->dst_owner_id);
    if (!peer) {
        resp->status = VEMB_V16_UB_MIGRATION_RPC_ERROR;
        log_limited(&rpc->error_logs,
                    LL_WARNING,
                    "vemb_v16 ub rpc peer missing: owner=%u request_id=%llu key_hash=%llu path=%s errno=%d error=%s",
                    req->dst_owner_id,
                    req->request_id,
                    req->key_hash,
                    NULL,
                    ENOENT);
        return 0;
    }

    vemb_v16_ub_rpc_pending_t *pending =
        pending_claim(peer, req->request_id);
    if (!pending) {
        resp->status = VEMB_V16_UB_MIGRATION_RPC_BUSY;
        log_limited(&rpc->ring_full_logs,
                    LL_NOTICE,
                    "vemb_v16 ub rpc pending full: owner=%u request_id=%llu key_hash=%llu path=%s errno=%d error=%s",
                    req->dst_owner_id,
                    req->request_id,
                    req->key_hash,
                    NULL,
                    EAGAIN);
        return 0;
    }

    vemb_v16_ub_rpc_wire_req_t wire_req = {
        .magic = VEMB_V16_UB_RPC_MAGIC,
        .version = VEMB_V16_UB_RPC_VERSION,
        .kind = VEMB_V16_UB_RPC_FRAME_MIGRATION,
        .u.migration = *req,
    };
    uint64_t timeout_ns = timeout_from_migration_req_ns(rpc, req);
    uint64_t deadline_ns = vemb_v16_monotonic_ns() + timeout_ns;
    uint32_t status = VEMB_V16_UB_MIGRATION_RPC_OK;
    int rc = publish_with_deadline(rpc,
                                   &peer->request,
                                   &wire_req,
                                   deadline_ns,
                                   req->dst_owner_id,
                                   req->request_id,
                                   req->key_hash,
                                   &status);
    if (rc != 0) {
        pending_release_waiting(pending);
        resp->status = status == VEMB_V16_UB_LOOKUP_RPC_BUSY ?
            VEMB_V16_UB_MIGRATION_RPC_BUSY :
            VEMB_V16_UB_MIGRATION_RPC_TIMEOUT;
        return 0;
    }

    vemb_v16_ub_rpc_wire_resp_t wire_resp;
    memset(&wire_resp, 0, sizeof(wire_resp));
    (void)pending_wait(rpc, peer, pending, &wire_req, &wire_resp,
                       deadline_ns);
    if (wire_resp.kind == VEMB_V16_UB_RPC_FRAME_MIGRATION)
        *resp = wire_resp.u.migration;
    else
        resp->status = VEMB_V16_UB_MIGRATION_RPC_ERROR;
    return 0;
}

int vemb_v16_ub_rpc_migrate_snapshot(
    void *arg,
    const vemb_v16_ub_migration_rpc_req_t *req,
    vemb_v16_ub_migration_rpc_resp_t *resp) {
    return vemb_v16_ub_rpc_migrate_request(arg, req, resp);
}

static void process_response(vemb_v16_ub_rpc_t *rpc,
                             vemb_v16_ub_rpc_peer_state_t *peer,
                             const vemb_v16_ub_rpc_wire_resp_t *wire_resp) {
    uint32_t response_log_count = atomic_fetch_add_explicit(
        &rpc->response_logs, 1, memory_order_relaxed);
    if (response_log_count < 16) {
        serverLog(LL_NOTICE,
                  "vemb_v16 ub rpc response observed: local_owner=%u peer_owner=%u request_id=%llu key_hash=%llu kind=%u status=%u response_path=%s",
                  rpc->local_owner_id,
                  peer->owner_id,
                  (unsigned long long)wire_resp_request_id(wire_resp),
                  (unsigned long long)wire_resp_key_hash(wire_resp),
                  wire_resp->kind,
                  wire_resp->kind == VEMB_V16_UB_RPC_FRAME_MIGRATION ?
                      wire_resp->u.migration.status :
                      wire_resp->u.lookup.status,
                  peer->response.config.path);
    }
    if (wire_resp->magic != VEMB_V16_UB_RPC_MAGIC ||
        wire_resp->version != VEMB_V16_UB_RPC_VERSION ||
        (wire_resp->kind != VEMB_V16_UB_RPC_FRAME_LOOKUP &&
         wire_resp->kind != VEMB_V16_UB_RPC_FRAME_MIGRATION)) {
        log_limited(&rpc->error_logs,
                    LL_WARNING,
                    "vemb_v16 ub rpc invalid response frame: owner=%u request_id=%llu key_hash=%llu path=%s errno=%d error=%s",
                    peer->owner_id,
                    wire_resp_request_id(wire_resp),
                    wire_resp_key_hash(wire_resp),
                    peer->response.config.path,
                    EPROTO);
        return;
    }
    if (pending_complete(peer, wire_resp) != 0) {
        log_limited(&rpc->error_logs,
                    LL_NOTICE,
                    "vemb_v16 ub rpc unmatched response dropped: owner=%u request_id=%llu key_hash=%llu path=%s errno=%d error=%s",
                    peer->owner_id,
                    wire_resp_request_id(wire_resp),
                    wire_resp_key_hash(wire_resp),
                    peer->response.config.path,
                    EAGAIN);
    }
}

static void process_request(vemb_v16_ub_rpc_t *rpc,
                            vemb_v16_ub_rpc_peer_state_t *peer,
                            const vemb_v16_ub_rpc_wire_req_t *wire_req) {
    uint32_t request_log_count = atomic_fetch_add_explicit(
        &rpc->request_logs, 1, memory_order_relaxed);
    if (request_log_count < 16) {
        serverLog(LL_NOTICE,
                  "vemb_v16 ub rpc request observed: local_owner=%u peer_owner=%u src_owner=%u dst_owner=%u request_id=%llu key_hash=%llu kind=%u inbound_path=%s outbound_path=%s",
                  rpc->local_owner_id,
                  peer->owner_id,
                  wire_req_src_owner(wire_req),
                  wire_req_dst_owner(wire_req),
                  (unsigned long long)wire_req_request_id(wire_req),
                  (unsigned long long)wire_req_key_hash(wire_req),
                  wire_req->kind,
                  peer->inbound_request.config.path,
                  peer->outbound_response.config.path);
    }
    vemb_v16_ub_rpc_wire_resp_t wire_resp;
    memset(&wire_resp, 0, sizeof(wire_resp));
    wire_resp.magic = VEMB_V16_UB_RPC_MAGIC;
    wire_resp.version = VEMB_V16_UB_RPC_VERSION;
    wire_resp.kind = wire_req->kind;
    if (wire_req->kind == VEMB_V16_UB_RPC_FRAME_MIGRATION) {
        wire_resp.u.migration.request_id =
            wire_req->u.migration.request_id;
        wire_resp.u.migration.key_hash = wire_req->u.migration.key_hash;
        wire_resp.u.migration.op = wire_req->u.migration.op;
    } else {
        wire_resp.u.lookup.request_id = wire_req->u.lookup.request_id;
        wire_resp.u.lookup.key_hash = wire_req->u.lookup.key_hash;
    }

    if (wire_req->magic != VEMB_V16_UB_RPC_MAGIC ||
        wire_req->version != VEMB_V16_UB_RPC_VERSION ||
        (wire_req->kind != VEMB_V16_UB_RPC_FRAME_LOOKUP &&
         wire_req->kind != VEMB_V16_UB_RPC_FRAME_MIGRATION) ||
        wire_req_dst_owner(wire_req) != rpc->local_owner_id ||
        wire_req_src_owner(wire_req) != peer->owner_id) {
        wire_resp_set_status(
            &wire_resp,
            wire_req->kind == VEMB_V16_UB_RPC_FRAME_MIGRATION ?
                VEMB_V16_UB_MIGRATION_RPC_ERROR :
                VEMB_V16_UB_LOOKUP_RPC_ERROR);
        if (wire_resp.kind == VEMB_V16_UB_RPC_FRAME_LOOKUP)
            wire_resp.u.lookup.kind = VEMB_V16_UB_LOOKUP_RPC_KIND_NONE;
    } else if (wire_req->kind == VEMB_V16_UB_RPC_FRAME_MIGRATION) {
        if (vemb_v16_tlc_migration_rpc_local_handler(
                rpc->tlc,
                &wire_req->u.migration,
                &wire_resp.u.migration) != 0) {
            wire_resp.u.migration.status =
                VEMB_V16_UB_MIGRATION_RPC_ERROR;
        }
    } else if (vemb_v16_tlc_lookup_rpc_local_handler(
                   rpc->tlc,
                   &wire_req->u.lookup,
                   &wire_resp.u.lookup) != 0) {
        wire_resp.u.lookup.status = VEMB_V16_UB_LOOKUP_RPC_ERROR;
        wire_resp.u.lookup.kind = VEMB_V16_UB_LOOKUP_RPC_KIND_NONE;
    }

    uint64_t timeout_ns = timeout_from_wire_req_ns(rpc, wire_req);
    uint64_t deadline_ns = vemb_v16_monotonic_ns() + timeout_ns;
    uint32_t status = VEMB_V16_UB_LOOKUP_RPC_OK;
    (void)publish_with_deadline(rpc,
                                &peer->outbound_response,
                                &wire_resp,
                                deadline_ns,
                                peer->owner_id,
                                wire_req_request_id(wire_req),
                                wire_req_key_hash(wire_req),
                                &status);
}

static void *listener_main(void *arg) {
    vemb_v16_ub_rpc_t *rpc = arg;
    serverLog(LL_NOTICE,
              "vemb_v16 ub rpc listener started: owner=%u peers=%u",
              rpc->local_owner_id,
              rpc->peer_count);
    while (atomic_load_explicit(&rpc->running, memory_order_acquire)) {
        uint32_t handled = 0;
        for (uint32_t i = 0; i < rpc->peer_count; i++) {
            vemb_v16_ub_rpc_peer_state_t *peer = &rpc->peers[i];
            if (rpc_ring_ready(&peer->inbound_request)) {
                for (uint32_t n = 0; n < VEMB_V16_UB_RPC_LISTENER_BATCH; n++) {
                    vemb_v16_ub_rpc_wire_req_t wire_req;
                    int got = rpc_ring_poll(peer->inbound_request.ring,
                                            &wire_req);
                    if (got <= 0)
                        break;
                    process_request(rpc, peer, &wire_req);
                    handled++;
                }
            }
            if (rpc_ring_ready(&peer->response)) {
                for (uint32_t n = 0; n < VEMB_V16_UB_RPC_LISTENER_BATCH; n++) {
                    vemb_v16_ub_rpc_wire_resp_t wire_resp;
                    int got = rpc_ring_poll(peer->response.ring,
                                            &wire_resp);
                    if (got <= 0)
                        break;
                    process_response(rpc, peer, &wire_resp);
                    handled++;
                }
            }
        }
        if (handled == 0)
            tiny_pause();
    }
    serverLog(LL_NOTICE,
              "vemb_v16 ub rpc listener stopped: owner=%u",
              rpc->local_owner_id);
    return NULL;
}

static int peer_open(vemb_v16_ub_rpc_peer_state_t *dst,
                     const vemb_v16_ub_rpc_peer_t *src) {
    memset(dst, 0, sizeof(*dst));
    dst->request.mapping.fd = -1;
    dst->response.mapping.fd = -1;
    dst->inbound_request.mapping.fd = -1;
    dst->outbound_response.mapping.fd = -1;
    dst->owner_id = src->owner_id;
    for (uint32_t i = 0; i < VEMB_V16_UB_RPC_PENDING_SIZE; i++) {
        atomic_init(&dst->pending[i].state,
                    VEMB_V16_UB_RPC_PENDING_EMPTY);
        atomic_init(&dst->pending[i].request_id, 0);
    }

    if (ring_open(&dst->request,
                  &src->request,
                  sizeof(vemb_v16_ub_rpc_wire_req_t),
                  0) != 0 ||
        ring_open(&dst->response,
                  &src->response,
                  sizeof(vemb_v16_ub_rpc_wire_resp_t),
                  1) != 0 ||
        ring_open(&dst->inbound_request,
                  &src->inbound_request,
                  sizeof(vemb_v16_ub_rpc_wire_req_t),
                  1) != 0 ||
        ring_open(&dst->outbound_response,
                  &src->outbound_response,
                  sizeof(vemb_v16_ub_rpc_wire_resp_t),
                  0) != 0) {
        return -1;
    }
    return 0;
}

static void peer_close(vemb_v16_ub_rpc_peer_state_t *peer) {
    if (!peer)
        return;
    ring_close(&peer->request);
    ring_close(&peer->response);
    ring_close(&peer->inbound_request);
    ring_close(&peer->outbound_response);
}

static void peer_state_export_config(vemb_v16_ub_rpc_peer_t *dst,
                                     const vemb_v16_ub_rpc_peer_state_t *src) {
    memset(dst, 0, sizeof(*dst));
    dst->owner_id = src->owner_id;
    dst->request = src->request.config;
    dst->response = src->response.config;
    dst->inbound_request = src->inbound_request.config;
    dst->outbound_response = src->outbound_response.config;
}

void vemb_v16_ub_rpc_install_lookup_runtime(vemb_v16_tlc_t *tlc,
                                            vemb_v16_ub_rpc_t *rpc,
                                            vemb_v16_ub_rpc_t **old_out) {
    vemb_v16_tlc_install_lookup_runtime(tlc,
                                        rpc,
                                        rpc ? vemb_v16_ub_rpc_lookup : NULL,
                                        tlc,
                                        old_out);
}

int vemb_v16_ub_rpc_create(vemb_v16_ub_rpc_t **out,
                           vemb_v16_tlc_t *tlc,
                           uint32_t local_owner_id,
                           uint32_t timeout_ms,
                           const vemb_v16_ub_rpc_peer_t *peers,
                           uint32_t peer_count) {
    if (!out || !tlc || peer_count > VEMB_V16_UB_RPC_MAX_PEERS ||
        (peer_count && !peers))
        return -1;
    *out = NULL;
    if (peer_count == 0)
        return 0;

    vemb_v16_ub_rpc_t *rpc = zcalloc(sizeof(*rpc));
    if (!rpc)
        return -1;
    rpc->tlc = tlc;
    rpc->local_owner_id = local_owner_id;
    rpc->timeout_ms = timeout_ms ? timeout_ms :
        VEMB_V16_UB_RPC_DEFAULT_TIMEOUT_MS;
    rpc->peer_count = 0;
    atomic_init(&rpc->running, 0);
    atomic_init(&rpc->timeout_logs, 0);
    atomic_init(&rpc->ring_full_logs, 0);
    atomic_init(&rpc->error_logs, 0);
    atomic_init(&rpc->request_logs, 0);
    atomic_init(&rpc->response_logs, 0);
    atomic_init(&rpc->refcount, 1);
    atomic_init(&rpc->retired, 0);

    for (uint32_t i = 0; i < peer_count; i++) {
        if (peer_open(&rpc->peers[rpc->peer_count], &peers[i]) != 0) {
            serverLog(LL_WARNING,
                      "failed to open vemb_v16 ub rpc peer rings: local_owner=%u peer_owner=%u",
                      local_owner_id,
                      peers[i].owner_id);
            vemb_v16_ub_rpc_destroy(rpc);
            return -1;
        }
        rpc->peer_count++;
    }

    atomic_store_explicit(&rpc->running, 1, memory_order_release);
    if (pthread_create(&rpc->thread, NULL, listener_main, rpc) != 0) {
        atomic_store_explicit(&rpc->running, 0, memory_order_release);
        vemb_v16_ub_rpc_destroy(rpc);
        return -1;
    }
    rpc->thread_started = 1;
    /*
     * Keep the historical create() behavior for standalone TLC tests:
     * first runtime install happens automatically, while later hot-swap
     * callers still publish explicitly through attach/install.
     */
    if (!atomic_load_explicit(&tlc->current_lookup_rpc_runtime,
                              memory_order_acquire)) {
        vemb_v16_ub_rpc_install_lookup_runtime(tlc, rpc, NULL);
    }
    *out = rpc;
    return 0;
}

int vemb_v16_ub_rpc_attach_peer(vemb_v16_ub_rpc_t **rpc_io,
                                vemb_v16_tlc_t *tlc,
                                uint32_t local_owner_id,
                                uint32_t timeout_ms,
                                const vemb_v16_ub_rpc_peer_t *peer) {
    RETURN_IF(!rpc_io || !tlc || !peer || peer->owner_id == UINT32_MAX, -1);
    vemb_v16_ub_rpc_peer_t peers[VEMB_V16_UB_RPC_MAX_PEERS];
    uint32_t peer_count = 0;
    vemb_v16_ub_rpc_t *rpc = *rpc_io;
    if (rpc) {
        for (uint32_t i = 0; i < rpc->peer_count; i++) {
            if (rpc->peers[i].owner_id == peer->owner_id)
                return 0;
            peer_state_export_config(&peers[peer_count++], &rpc->peers[i]);
        }
    }
    RETURN_IF(peer_count >= VEMB_V16_UB_RPC_MAX_PEERS, -1);
    peers[peer_count++] = *peer;
    vemb_v16_ub_rpc_t *new_rpc = NULL;
    RETURN_IF(vemb_v16_ub_rpc_create(&new_rpc,
                                     tlc,
                                     local_owner_id,
                                     timeout_ms,
                                     peers,
                                     peer_count) != 0,
              -1);
    vemb_v16_ub_rpc_t *old_rpc = NULL;
    vemb_v16_ub_rpc_install_lookup_runtime(tlc, new_rpc, &old_rpc);
    if (old_rpc && old_rpc != new_rpc)
        vemb_v16_ub_rpc_destroy(old_rpc);
    *rpc_io = new_rpc;
    return 0;
}

int vemb_v16_ub_rpc_reset_request_ring(
        const vemb_v16_ub_rpc_ring_config_t *config) {
    return ring_reset(config, sizeof(vemb_v16_ub_rpc_wire_req_t));
}

int vemb_v16_ub_rpc_reset_response_ring(
        const vemb_v16_ub_rpc_ring_config_t *config) {
    return ring_reset(config, sizeof(vemb_v16_ub_rpc_wire_resp_t));
}

int vemb_v16_ub_rpc_has_peer(vemb_v16_ub_rpc_t *rpc, uint32_t owner_id) {
    if (!rpc || owner_id == UINT32_MAX)
        return 0;
    return find_peer(rpc, owner_id) != NULL;
}

static void vemb_v16_ub_rpc_destroy_final(vemb_v16_ub_rpc_t *rpc) {
    if (!rpc)
        return;
    atomic_store_explicit(&rpc->running, 0, memory_order_release);
    if (rpc->thread_started)
        pthread_join(rpc->thread, NULL);
    for (uint32_t i = 0; i < rpc->peer_count; i++)
        peer_close(&rpc->peers[i]);
    zfree(rpc);
}

void vemb_v16_ub_rpc_destroy(vemb_v16_ub_rpc_t *rpc) {
    if (!rpc)
        return;
    if (rpc->tlc)
        vemb_v16_tlc_clear_lookup_runtime(rpc->tlc, rpc);
    vemb_v16_ub_rpc_retire(rpc);
}
