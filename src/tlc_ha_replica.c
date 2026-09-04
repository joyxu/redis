#include "tlc_ha_replica.h"
#include "cpu_relax.h"
#include "vemb_v16_hash.h"
#include "vemb_v16_log.h"
#include "vemb_v16_mapped_region.h"
#include "monotonic.h"
#include "zmalloc.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define TLC_HA_REPLICA_MAGIC UINT32_C(0x54485250) /* THRP */
#define TLC_HA_REPLICA_VERSION UINT16_C(1)
#define TLC_HA_REPLICA_KIND_EVENTS UINT16_C(1)
#define TLC_HA_REPLICA_KIND_ACK UINT16_C(2)
#define TLC_HA_REPLICA_KIND_HEARTBEAT UINT16_C(3)
#define TLC_HA_REPLICA_KIND_RESYNC_REQUEST UINT16_C(4)
#define TLC_HA_REPLICA_KIND_SNAPSHOT_BEGIN UINT16_C(5)
#define TLC_HA_REPLICA_KIND_SNAPSHOT_CHUNK UINT16_C(6)
#define TLC_HA_REPLICA_KIND_SNAPSHOT_END UINT16_C(7)
#define TLC_HA_REPLICA_KIND_SNAPSHOT_INSTALLED UINT16_C(8)
#define TLC_HA_REPLICA_KIND_RESYNC_ABORT UINT16_C(9)
#define TLC_HA_REPLICA_KIND_TAIL_REQUEST UINT16_C(10)
#define TLC_HA_REPLICA_KIND_TAIL_END UINT16_C(11)
#define TLC_HA_REPLICA_KIND_RESYNC_ACK UINT16_C(12)
#define TLC_HA_REPLICA_KIND_HANDOFF_COMMIT UINT16_C(13)
#define TLC_HA_REPLICA_KIND_TAIL_EVENTS UINT16_C(14)
#define TLC_HA_REPLICA_KIND_HANDOFF_ACK UINT16_C(15)
#define TLC_HA_REPLICA_KIND_RESYNC_REQUIRED UINT16_C(16)
#define TLC_HA_REPLICA_DEFAULT_QUEUE 256u
#define TLC_HA_REPLICA_DEFAULT_BATCH_EVENTS 32u
#define TLC_HA_REPLICA_DEFAULT_BATCH_BYTES (1024u * 1024u)
#define TLC_HA_REPLICA_DEFAULT_RESYNC_SNAPSHOT_BYTES (128u * 1024u * 1024u)
#define TLC_HA_REPLICA_RING_MAGIC UINT32_C(0x54485252) /* THRR */
#define TLC_HA_REPLICA_RING_VERSION UINT32_C(1)
#define TLC_HA_REPLICA_DEFAULT_RING_SLOTS 256u
#define TLC_HA_REPLICA_DEFAULT_RING_BYTES (64u * 1024u)
#define TLC_HA_REPLICA_DEFAULT_HEARTBEAT_INTERVAL_MS 1000u
#define TLC_HA_REPLICA_DEFAULT_HEARTBEAT_TIMEOUT_MS 3000u
#define TLC_HA_REPLICA_DEFAULT_RESYNC_TIMEOUT_MS 30000u
#define TLC_HA_REPLICA_DEFAULT_RESYNC_CHUNK_BYTES (32u * 1024u)
#define TLC_HA_REPLICA_RESYNC_REQUIRED_RETRY_NS UINT64_C(100000000)
#define TLC_HA_REPLICA_RETENTION_POLL_NS UINT64_C(10000000)
#define TLC_HA_REPLICA_RETENTION_RETRY_NS UINT64_C(100000000)
#define TLC_HA_REPLICA_REPLAY_QUEUE_FULL TLC_COLD_REPLAY_STOP
#define TLC_HA_RESYNC_BEGIN_WIRE_BYTES 92u
#define TLC_HA_RESYNC_CHUNK_HEADER_WIRE_BYTES 28u
#define TLC_HA_RESYNC_END_WIRE_BYTES 24u
#define TLC_HA_RESYNC_CONTROL_WIRE_BYTES 48u
#define TLC_HA_RESYNC_REQUIRED_WIRE_BYTES 40u

typedef struct tlc_ha_replica_ring_slot {
    _Alignas(64) atomic_uint_fast64_t sequence;
    uint8_t payload[];
} tlc_ha_replica_ring_slot_t;

typedef struct tlc_ha_replica_shared_ring {
    uint32_t magic;
    uint32_t version;
    uint32_t slot_bytes;
    uint32_t slot_count;
    uint32_t slot_mask;
    uint32_t slot_stride;
    uint32_t reserved[10];
    _Alignas(64) atomic_uint_fast64_t head;
    _Alignas(64) atomic_uint_fast64_t tail;
    uint8_t slots[];
} tlc_ha_replica_shared_ring_t;

typedef struct tlc_ha_replica_ring {
    tlc_ha_replica_ring_config_t config;
    vemb_v16_mapped_region_t mapping;
    tlc_ha_replica_shared_ring_t *ring;
    size_t bytes;
    uint32_t slot_stride;
} tlc_ha_replica_ring_t;

static uint32_t replica_ring_stride(uint32_t slot_bytes) {
    size_t stride = (size_t)sizeof(tlc_ha_replica_ring_slot_t) + slot_bytes;
    size_t aligned = (stride + 63u) & ~((size_t)63u);
    return aligned > UINT32_MAX ? 0 : (uint32_t)aligned;
}

static size_t replica_ring_bytes(uint32_t slot_bytes, uint32_t slot_count) {
    uint32_t stride = replica_ring_stride(slot_bytes);
    if (stride == 0 || slot_count == 0 ||
        (size_t)stride > (SIZE_MAX - sizeof(tlc_ha_replica_shared_ring_t)) /
                         slot_count)
        return 0;
    return sizeof(tlc_ha_replica_shared_ring_t) +
           (size_t)stride * slot_count;
}

static tlc_ha_replica_ring_slot_t *replica_ring_slot(
        tlc_ha_replica_shared_ring_t *ring, uint64_t position) {
    return (tlc_ha_replica_ring_slot_t *)
        (ring->slots + (position & ring->slot_mask) * ring->slot_stride);
}

static uint32_t replica_round_queue_capacity(uint32_t requested) {
    uint32_t capacity = 1;
    while (capacity < requested) {
        if (capacity > UINT32_MAX / 2u)
            return 0;
        capacity <<= 1;
    }
    return capacity;
}

static inline void replica_queue_wait(uint32_t *spins) {
    if (*spins < 256u) {
        cpu_relax();
    } else {
        struct timespec pause = {0, 10000}; /* 10us while genuinely idle */
        nanosleep(&pause, NULL);
    }
    if (*spins != UINT32_MAX)
        (*spins)++;
}

typedef struct tlc_ha_replica_frame_wire {
    uint32_t magic;
    uint16_t version;
    uint16_t kind;
    uint32_t header_bytes;
    uint64_t first_seq;
    uint32_t event_count;
    uint32_t payload_bytes;
    uint64_t checksum;
} tlc_ha_replica_frame_wire_t;

typedef struct tlc_ha_resync_control {
    uint64_t session_id;
    uint64_t generation;
    uint64_t checkpoint_seq;
    uint64_t durable_seq;
    uint64_t applied_seq;
    uint64_t durable_boundary_seq;
} tlc_ha_resync_control_t;

typedef struct tlc_ha_resync_required {
    uint64_t ha_term;
    uint64_t topology_epoch;
    uint64_t durable_seq;
    uint64_t applied_seq;
    uint32_t reason;
} tlc_ha_resync_required_t;

typedef enum tlc_ha_resync_session_state {
    TLC_HA_RESYNC_SESSION_IDLE = 0,
    TLC_HA_RESYNC_SESSION_LEADER_WAIT_INSTALLED = 1,
    TLC_HA_RESYNC_SESSION_LEADER_WAIT_REQUEST = 2,
    TLC_HA_RESYNC_SESSION_LEADER_WAIT_ACK = 3,
    TLC_HA_RESYNC_SESSION_FOLLOWER_WAIT_INSTALL = 4,
    TLC_HA_RESYNC_SESSION_FOLLOWER_WAIT_TAIL = 5,
    TLC_HA_RESYNC_SESSION_FOLLOWER_WAIT_COMMIT = 6,
    TLC_HA_RESYNC_SESSION_COMPLETE = 7,
    TLC_HA_RESYNC_SESSION_ABORTED = 8,
    TLC_HA_RESYNC_SESSION_LEADER_WAIT_HANDOFF_ACK = 9,
    TLC_HA_RESYNC_SESSION_FOLLOWER_WAIT_RESYNC = 10,
} tlc_ha_resync_session_state_t;

struct tlc_ha_resync_assembler {
    uint64_t local_node_id;
    uint64_t peer_node_id;
    uint64_t ha_term;
    uint64_t max_blob_bytes;
    uint64_t last_session_id;
    uint64_t received_bytes;
    int artifact_fd;
    char artifact_directory[PATH_MAX];
    char session_directory[PATH_MAX];
    char part_path[PATH_MAX];
    char blob_path[PATH_MAX];
    tlc_ha_resync_snapshot_begin_t begin;
    tlc_ha_resync_stage_t stage;
    tlc_ha_resync_reason_t reason;
};

static int replica_ring_config_valid(const tlc_ha_replica_ring_config_t *config) {
    return config && config->path[0] != '\0' &&
           (config->backend_type == VEMB_V16_REGION_LOCAL_SHM ||
            config->backend_type == VEMB_V16_REGION_UB) &&
           (config->cache_policy == VEMB_V16_UB_CACHE_POLICY_CACHEABLE ||
            config->cache_policy == VEMB_V16_UB_CACHE_POLICY_NONCACHEABLE) &&
           config->slot_count >= 2 &&
           (config->slot_count & (config->slot_count - 1u)) == 0 &&
           config->slot_bytes >= sizeof(tlc_ha_replica_frame_wire_t);
}

static void replica_ring_init(tlc_ha_replica_shared_ring_t *ring,
                              uint32_t slot_bytes,
                              uint32_t slot_count,
                              uint32_t slot_stride) {
    ring->magic = TLC_HA_REPLICA_RING_MAGIC;
    ring->version = TLC_HA_REPLICA_RING_VERSION;
    ring->slot_bytes = slot_bytes;
    ring->slot_count = slot_count;
    ring->slot_mask = slot_count - 1u;
    ring->slot_stride = slot_stride;
    atomic_init(&ring->head, 0);
    atomic_init(&ring->tail, 0);
    for (uint32_t i = 0; i < slot_count; i++)
        atomic_init(&replica_ring_slot(ring, i)->sequence, i);
}

static int replica_ring_ready(const tlc_ha_replica_ring_t *ring) {
    return ring && ring->ring &&
           ring->ring->magic == TLC_HA_REPLICA_RING_MAGIC &&
           ring->ring->version == TLC_HA_REPLICA_RING_VERSION &&
           ring->ring->slot_bytes == ring->config.slot_bytes &&
           ring->ring->slot_count == ring->config.slot_count &&
           ring->ring->slot_mask == ring->config.slot_count - 1u &&
           ring->ring->slot_stride == ring->slot_stride;
}

static void replica_ring_close(tlc_ha_replica_ring_t *ring);

static int replica_ring_open(tlc_ha_replica_ring_t *ring,
                             const tlc_ha_replica_ring_config_t *config) {
    if (!replica_ring_config_valid(config))
        return -1;
    size_t bytes = replica_ring_bytes(config->slot_bytes, config->slot_count);
    uint32_t stride = replica_ring_stride(config->slot_bytes);
    if (bytes == 0 || stride == 0)
        return -1;
    memset(ring, 0, sizeof(*ring));
    ring->mapping.fd = -1;
    ring->config = *config;
    ring->bytes = bytes;
    ring->slot_stride = stride;
    if (vemb_v16_mapped_region_open(&ring->mapping,
                                    config->backend_type,
                                    config->cache_policy,
                                    config->path,
                                    config->mmap_offset,
                                    bytes) != 0)
        return -1;
    ring->ring = (tlc_ha_replica_shared_ring_t *)ring->mapping.mapped_addr;
    if (!replica_ring_ready(ring)) {
        if (ring->ring->magic != 0 || ring->ring->version != 0) {
            replica_ring_close(ring);
            return -1;
        }
        memset(ring->ring, 0, bytes);
        replica_ring_init(ring->ring, config->slot_bytes,
                          config->slot_count, stride);
    }
    return 0;
}

static void replica_ring_close(tlc_ha_replica_ring_t *ring) {
    if (!ring)
        return;
    vemb_v16_mapped_region_close(&ring->mapping);
    ring->ring = NULL;
}

static int replica_ring_reset(const tlc_ha_replica_ring_config_t *config) {
    if (!replica_ring_config_valid(config))
        return -1;
    size_t bytes = replica_ring_bytes(config->slot_bytes, config->slot_count);
    uint32_t stride = replica_ring_stride(config->slot_bytes);
    if (bytes == 0 || stride == 0)
        return -1;
    vemb_v16_mapped_region_t mapping = {.fd = -1};
    if (vemb_v16_mapped_region_open(&mapping,
                                    config->backend_type,
                                    config->cache_policy,
                                    config->path,
                                    config->mmap_offset,
                                    bytes) != 0)
        return -1;
    tlc_ha_replica_shared_ring_t *ring =
        (tlc_ha_replica_shared_ring_t *)mapping.mapped_addr;
    memset(ring, 0, bytes);
    replica_ring_init(ring, config->slot_bytes, config->slot_count, stride);
    vemb_v16_mapped_region_close(&mapping);
    return 0;
}

int tlc_ha_replica_reset_ring(const tlc_ha_replica_ring_config_t *config) {
    return replica_ring_reset(config);
}

static tlc_ha_replica_ring_slot_t *replica_ring_reserve(
        tlc_ha_replica_ring_t *ring, uint64_t *position_out) {
    uint64_t position = atomic_load_explicit(&ring->ring->tail,
                                             memory_order_relaxed);
    tlc_ha_replica_ring_slot_t *slot;
    for (;;) {
        slot = replica_ring_slot(ring->ring, position);
        uint64_t sequence = atomic_load_explicit(&slot->sequence,
                                                 memory_order_acquire);
        int64_t diff = (int64_t)sequence - (int64_t)position;
        if (diff == 0) {
            uint64_t desired = position + 1;
            if (atomic_compare_exchange_weak_explicit(
                    &ring->ring->tail, &position, desired,
                    memory_order_relaxed, memory_order_relaxed)) {
                *position_out = position;
                return slot;
            }
        } else if (diff < 0) {
            return NULL;
        } else {
            position = atomic_load_explicit(&ring->ring->tail,
                                            memory_order_relaxed);
        }
    }
}

static void replica_ring_publish(tlc_ha_replica_ring_slot_t *slot,
                                 uint64_t position) {
    /* UB provides acquire/release visibility for the shared sequence word:
     * all producer-owned slot bytes are written before this release. */
    atomic_store_explicit(&slot->sequence, position + 1,
                          memory_order_release);
}

static int replica_ring_poll(tlc_ha_replica_ring_t *ring, void *frame) {
    uint64_t position = atomic_load_explicit(&ring->ring->head,
                                             memory_order_relaxed);
    tlc_ha_replica_ring_slot_t *slot;
    for (;;) {
        slot = replica_ring_slot(ring->ring, position);
        uint64_t sequence = atomic_load_explicit(&slot->sequence,
                                                 memory_order_acquire);
        int64_t diff = (int64_t)sequence - (int64_t)(position + 1);
        if (diff == 0) {
            uint64_t desired = position + 1;
            if (atomic_compare_exchange_weak_explicit(
                    &ring->ring->head, &position, desired,
                    memory_order_relaxed, memory_order_relaxed))
                break;
        } else if (diff < 0) {
            return 0;
        } else {
            position = atomic_load_explicit(&ring->ring->head,
                                            memory_order_relaxed);
        }
    }
    /* The acquire load above establishes happens-before with the producer's
     * payload writes before copying the published frame. */
    memcpy(frame, slot->payload, sizeof(tlc_ha_replica_frame_wire_t));
    tlc_ha_replica_frame_wire_t wire;
    memcpy(&wire, slot->payload, sizeof(wire));
    uint32_t payload_bytes = ntohl(wire.payload_bytes);
    if (payload_bytes > ring->config.slot_bytes -
                        sizeof(tlc_ha_replica_frame_wire_t)) {
        atomic_store_explicit(&slot->sequence,
                              position + ring->config.slot_count,
                              memory_order_release);
        return -1;
    }
    if (payload_bytes)
        memcpy((uint8_t *)frame + sizeof(wire),
               slot->payload + sizeof(wire), payload_bytes);
    atomic_store_explicit(&slot->sequence,
                          position + ring->config.slot_count,
                          memory_order_release);
    return 1;
}

typedef struct tlc_ha_replica_event_wire {
    uint64_t ha_term;
    uint64_t topology_epoch;
    uint32_t op;
    uint32_t meta_shard_id;
    uint64_t version;
    uint32_t key_len;
    uint32_t value_len;
} tlc_ha_replica_event_wire_t;

typedef struct tlc_ha_replica_event {
    tlc_cold_event_input_t input;
    uint8_t *key;
    uint8_t *value;
    uint64_t seq;
} tlc_ha_replica_event_t;

/* The Follower listener publishes one verified artifact; the resync
 * orchestrator is its only consumer. */
typedef struct tlc_ha_resync_artifact {
    char path[PATH_MAX];
    size_t blob_bytes;
    tlc_ha_resync_snapshot_begin_t begin;
} tlc_ha_resync_artifact_t;

typedef struct tlc_ha_replica_heartbeat {
    /* Sender and fixed peer identities for Node-group isolation. */
    uint64_t hpc_node_id;
    uint64_t peer_node_id;
    /* Owner fencing term; older terms are rejected. */
    uint64_t ha_term;
    /* Sender role and health result. */
    uint32_t role;
    uint32_t health;
    /* Sender monotonic timestamp, retained for diagnostics only. */
    uint64_t sent_at_ns;
    /* Sender's locally durable COLD prefix. */
    uint64_t durable_seq;
    /* Leader: replicated_seq; Follower: applied_seq. */
    uint64_t progress_seq;
} tlc_ha_replica_heartbeat_t;

_Static_assert(sizeof(tlc_ha_replica_heartbeat_t) == 56,
               "heartbeat wire layout must remain fixed-size");

struct tlc_ha_replica {
    tlc_core_t *core;
    tlc_cold_t *cold;
    int fd;
    tlc_ha_replica_role_t role;
    tlc_ha_replica_transport_t transport;
    tlc_ha_replica_ring_t tx_ring;
    tlc_ha_replica_ring_t rx_ring;
    uint8_t *rx_frame;
    uint32_t queue_capacity;
    uint32_t max_batch_events;
    uint32_t max_batch_bytes;
    uint64_t max_resync_snapshot_bytes;
    uint64_t hpc_node_id;
    uint64_t peer_node_id;
    uint64_t ha_term;
    uint64_t configured_topology_epoch;
    uint32_t heartbeat_interval_ms;
    uint32_t heartbeat_timeout_ms;
    uint32_t resync_timeout_ms;
    uint32_t resync_chunk_bytes;
    atomic_bool stopping;
    atomic_uint_fast64_t peer_accepted_seq;
    atomic_uint_fast64_t peer_durable_seq;
    atomic_uint_fast64_t peer_progress_seq;
    atomic_uint_fast64_t peer_ha_term;
    atomic_uint_fast64_t applied_seq;
    atomic_uint_fast64_t last_heartbeat_received_ns;
    atomic_uint peer_health;
    atomic_bool resync_fenced;
    atomic_uint_fast32_t ingress_inflight;
    atomic_uint_fast32_t apply_inflight;
    atomic_uint_fast32_t sender_inflight;
    atomic_bool resync_emission_gate;
    atomic_uint resync_session_state;
    atomic_uint_fast64_t resync_last_progress_ns;
    atomic_bool resync_required_pending;
    atomic_uint_fast64_t resync_required_last_sent_ns;
    atomic_uint resync_required_reason;
    atomic_uint_fast64_t resync_required_durable_seq;
    atomic_uint_fast64_t resync_required_applied_seq;
    atomic_uint_fast64_t next_resync_session_id;
    atomic_bool resync_install_pending;
    uint64_t resync_session_id;
    uint64_t resync_generation;
    uint64_t resync_checkpoint_seq;
    uint64_t resync_boundary_seq;
    uint64_t resync_next_seq;
    uint64_t resync_topology_epoch;
    uint64_t resync_captured_seq_checksum;
    uint32_t resync_meta_shard_count;
    uint64_t *resync_captured_seq;
    tlc_cold_resync_snapshot_t leader_resync_snapshot;
    tlc_ha_replica_event_t **queue;
    uint64_t queue_mask;
    _Alignas(64) atomic_uint_fast64_t queue_head;
    _Alignas(64) atomic_uint_fast64_t queue_tail;
    atomic_flag producer_admission_gate;
    atomic_uint_fast64_t replay_from_seq;
    atomic_bool replay_mode;
    atomic_bool replay_producer_active;
    atomic_bool replay_log_active;
    atomic_uint_fast64_t replay_log_start_seq;
    atomic_uint_fast64_t normal_min_seq;
    atomic_bool sender_discard_pending;
    pthread_t sender_thread;
    pthread_t receiver_thread;
    pthread_t apply_thread;
    pthread_t heartbeat_thread;
    pthread_t resync_controller_thread;
    uint32_t sender_started;
    uint32_t receiver_started;
    uint32_t apply_started;
    uint32_t heartbeat_started;
    uint32_t resync_controller_started;
    pthread_mutex_t stream_write_mutex;
    uint32_t stream_write_mutex_initialized;
    uint64_t last_progress_log_ns;
    /* Controller-thread only; normal COLD append/send/apply never touch these. */
    uint64_t retention_next_poll_ns;
    uint64_t retention_last_attempt_ns;
    uint64_t retention_maintained_floor_seq;
    tlc_ha_resync_assembler_t *resync_assembler;
    atomic_uintptr_t received_snapshot_artifact;
};

static int replica_resync_state_active(unsigned state) {
    return state != TLC_HA_RESYNC_SESSION_IDLE &&
           state != TLC_HA_RESYNC_SESSION_COMPLETE &&
           state != TLC_HA_RESYNC_SESSION_ABORTED;
}

static int replica_resync_required_reason_valid(uint32_t reason) {
    return reason >= TLC_HA_RESYNC_REQUIRED_GAP &&
           reason <= TLC_HA_RESYNC_REQUIRED_RETENTION;
}

static const char *replica_resync_required_reason_name(uint32_t reason) {
    switch (reason) {
    case TLC_HA_RESYNC_REQUIRED_GAP:
        return "gap";
    case TLC_HA_RESYNC_REQUIRED_CONFLICT:
        return "conflict";
    case TLC_HA_RESYNC_REQUIRED_RETENTION:
        return "retention";
    default:
        return "invalid";
    }
}

static void replica_resync_touch(tlc_ha_replica_t *replica) {
    atomic_store_explicit(&replica->resync_last_progress_ns, getMonotonicNs(),
                          memory_order_release);
}

static void replica_atomic_advance(atomic_uint_fast64_t *target,
                                   uint64_t seq) {
    uint64_t current = atomic_load_explicit(target, memory_order_relaxed);
    while (seq > current &&
           !atomic_compare_exchange_weak_explicit(target, &current, seq,
                                                  memory_order_release,
                                                  memory_order_relaxed)) {
    }
}

static int replica_resync_session_matches(const tlc_ha_replica_t *replica,
                                          const tlc_ha_resync_control_t *control) {
    return control->session_id == replica->resync_session_id &&
           control->generation == replica->resync_generation &&
           control->checkpoint_seq == replica->resync_checkpoint_seq;
}

static int replica_abort_resync_internal(tlc_ha_replica_t *replica,
                                         int notify_peer);
static void replica_leader_schedule_resync(tlc_ha_replica_t *replica);
static void replica_discard_sender_queue(tlc_ha_replica_t *replica);

static uint64_t replica_hton64(uint64_t value) {
    uint32_t hi = htonl((uint32_t)(value >> 32));
    uint32_t lo = htonl((uint32_t)value);
    return ((uint64_t)lo << 32) | hi;
}

static uint64_t replica_ntoh64(uint64_t value) {
    return replica_hton64(value);
}

static void resync_assembler_discard_part(
        tlc_ha_resync_assembler_t *assembler) {
    if (assembler->artifact_fd >= 0) {
        close(assembler->artifact_fd);
        assembler->artifact_fd = -1;
    }
    if (assembler->part_path[0] != '\0')
        unlink(assembler->part_path);
}

static void resync_assembler_fail(tlc_ha_resync_assembler_t *assembler,
                                  tlc_ha_resync_reason_t reason) {
    resync_assembler_discard_part(assembler);
    assembler->received_bytes = 0;
    assembler->stage = TLC_HA_RESYNC_FAILED;
    assembler->reason = reason;
}

static int replica_capture_resync_seq(uint32_t meta_shard_id,
                                      uint64_t captured_seq,
                                      const void *state,
                                      uint32_t state_len,
                                      void *arg) {
    uint64_t *captured = arg;
    captured[meta_shard_id] = captured_seq;
    (void)state;
    (void)state_len;
    return 0;
}

static int resync_mkdir(const char *path) {
    return mkdir(path, 0700) == 0 || errno == EEXIST ? 0 : -1;
}

static int resync_fsync_directory(const char *path) {
    int fd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0)
        return -1;
    int sync_rc = fsync(fd);
    int close_rc = close(fd);
    return sync_rc == 0 && close_rc == 0 ? 0 : -1;
}

static int resync_write_full_at(int fd, const void *data, size_t bytes,
                                uint64_t offset) {
    const uint8_t *cursor = data;
    while (bytes != 0) {
        ssize_t written = pwrite(fd, cursor, bytes, (off_t)offset);
        if (written < 0 && errno == EINTR)
            continue;
        if (written <= 0)
            return -1;
        cursor += (size_t)written;
        bytes -= (size_t)written;
        offset += (uint64_t)written;
    }
    return 0;
}

static int resync_hash_fd(int fd, size_t bytes, uint64_t *checksum) {
    uint8_t buffer[65536];
    XXH3_state_t *state = XXH3_createState();
    if (!state)
        return -1;
    XXH3_64bits_reset(state);
    size_t offset = 0;
    int rc = 0;
    while (offset < bytes) {
        size_t chunk = bytes - offset < sizeof(buffer) ?
            bytes - offset : sizeof(buffer);
        size_t received = 0;
        while (received < chunk) {
            ssize_t nread = pread(fd, buffer + received, chunk - received,
                                  (off_t)(offset + received));
            if (nread > 0) {
                received += (size_t)nread;
                continue;
            }
            if (nread < 0 && errno == EINTR)
                continue;
            if (nread == 0)
                errno = EIO;
            rc = -1;
            break;
        }
        if (rc != 0)
            break;
        XXH3_64bits_update(state, buffer, chunk);
        offset += chunk;
    }
    if (rc == 0)
        *checksum = (uint64_t)XXH3_64bits_digest(state);
    XXH3_freeState(state);
    return rc;
}

static int replica_follower_work_enter(tlc_ha_replica_t *replica,
                                       atomic_uint_fast32_t *inflight) {
    if (atomic_load_explicit(&replica->resync_fenced, memory_order_acquire))
        return -1;
    atomic_fetch_add_explicit(inflight, 1, memory_order_acq_rel);
    if (!atomic_load_explicit(&replica->resync_fenced, memory_order_acquire))
        return 0;
    atomic_fetch_sub_explicit(inflight, 1, memory_order_release);
    return -1;
}

static void replica_follower_work_leave(atomic_uint_fast32_t *inflight) {
    atomic_fetch_sub_explicit(inflight, 1, memory_order_release);
}

static void resync_artifact_free(tlc_ha_resync_artifact_t *artifact) {
    zfree(artifact);
}

static void resync_artifact_delete(tlc_ha_resync_artifact_t *artifact) {
    if (!artifact)
        return;
    unlink(artifact->path);
    resync_artifact_free(artifact);
}

static int replica_publish_resync_artifact(
        tlc_ha_replica_t *replica, tlc_ha_resync_artifact_t *artifact) {
    uintptr_t expected = (uintptr_t)NULL;
    return atomic_compare_exchange_strong_explicit(
        &replica->received_snapshot_artifact, &expected, (uintptr_t)artifact,
        memory_order_release, memory_order_acquire) ? 0 : -1;
}

int tlc_ha_resync_assembler_create(tlc_ha_resync_assembler_t **out,
                                   uint64_t local_node_id,
                                   uint64_t peer_node_id,
                                   uint64_t ha_term,
                                   uint64_t max_blob_bytes,
                                   const char *artifact_directory) {
    if (!out || local_node_id == 0 || peer_node_id == 0 ||
        local_node_id == peer_node_id || ha_term == 0 ||
        max_blob_bytes == 0 || max_blob_bytes > SIZE_MAX ||
        !artifact_directory || artifact_directory[0] == '\0')
        return -1;
    *out = NULL;
    tlc_ha_resync_assembler_t *assembler = zcalloc(sizeof(*assembler));
    if (!assembler)
        return -1;
    assembler->local_node_id = local_node_id;
    assembler->peer_node_id = peer_node_id;
    assembler->ha_term = ha_term;
    assembler->max_blob_bytes = max_blob_bytes;
    if (snprintf(assembler->artifact_directory,
                 sizeof(assembler->artifact_directory), "%s",
                 artifact_directory) < 0 ||
        strlen(artifact_directory) >= sizeof(assembler->artifact_directory)) {
        zfree(assembler);
        return -1;
    }
    assembler->artifact_fd = -1;
    assembler->stage = TLC_HA_RESYNC_IDLE;
    *out = assembler;
    return 0;
}

void tlc_ha_resync_assembler_destroy(tlc_ha_resync_assembler_t *assembler) {
    if (!assembler)
        return;
    resync_assembler_discard_part(assembler);
    zfree(assembler);
}

int tlc_ha_resync_assembler_begin(
    tlc_ha_resync_assembler_t *assembler,
    const tlc_ha_resync_snapshot_begin_t *begin) {
    if (!assembler || !begin)
        return -1;
    if (begin->leader_node_id != assembler->peer_node_id ||
        begin->follower_node_id != assembler->local_node_id) {
        resync_assembler_fail(assembler, TLC_HA_RESYNC_REASON_IDENTITY);
        return -1;
    }
    if (begin->ha_term != assembler->ha_term) {
        resync_assembler_fail(assembler, TLC_HA_RESYNC_REASON_TERM);
        return -1;
    }
    if (begin->session_id == 0 || begin->session_id == assembler->last_session_id ||
        begin->generation == 0 || begin->meta_shard_count == 0 ||
        begin->checkpoint_blob_bytes == 0 ||
        begin->checkpoint_blob_bytes > assembler->max_blob_bytes ||
        begin->checkpoint_seq == UINT64_MAX ||
        begin->durable_boundary_seq < begin->checkpoint_seq) {
        resync_assembler_fail(assembler, TLC_HA_RESYNC_REASON_GEOMETRY);
        return -1;
    }
    if (assembler->stage == TLC_HA_RESYNC_RECEIVING_SNAPSHOT) {
        resync_assembler_fail(assembler, TLC_HA_RESYNC_REASON_SESSION);
        return -1;
    }
    if (resync_mkdir(assembler->artifact_directory) != 0 ||
        snprintf(assembler->session_directory,
                 sizeof(assembler->session_directory), "%s/%016llx",
                 assembler->artifact_directory,
                 (unsigned long long)begin->session_id) < 0 ||
        strlen(assembler->artifact_directory) + 18u >=
            sizeof(assembler->session_directory) ||
        mkdir(assembler->session_directory, 0700) != 0 ||
        snprintf(assembler->part_path, sizeof(assembler->part_path),
                 "%s/checkpoint.blob.part", assembler->session_directory) < 0 ||
        snprintf(assembler->blob_path, sizeof(assembler->blob_path),
                 "%s/checkpoint.blob", assembler->session_directory) < 0) {
        resync_assembler_fail(assembler, TLC_HA_RESYNC_REASON_IO);
        return -1;
    }
    if (strlen(assembler->session_directory) + sizeof("/checkpoint.blob.part") >=
            sizeof(assembler->part_path) ||
        strlen(assembler->session_directory) + sizeof("/checkpoint.blob") >=
            sizeof(assembler->blob_path)) {
        resync_assembler_fail(assembler, TLC_HA_RESYNC_REASON_IO);
        return -1;
    }
    assembler->artifact_fd = open(assembler->part_path,
                                  O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC,
                                  0600);
    if (assembler->artifact_fd < 0) {
        resync_assembler_fail(assembler, TLC_HA_RESYNC_REASON_IO);
        return -1;
    }
    assembler->begin = *begin;
    assembler->last_session_id = begin->session_id;
    assembler->received_bytes = 0;
    assembler->stage = TLC_HA_RESYNC_RECEIVING_SNAPSHOT;
    assembler->reason = TLC_HA_RESYNC_REASON_NONE;
    return 0;
}

int tlc_ha_resync_assembler_append(
    tlc_ha_resync_assembler_t *assembler,
    const tlc_ha_resync_snapshot_chunk_t *chunk,
    const void *data) {
    if (!assembler || !chunk || !data)
        return -1;
    if (assembler->stage != TLC_HA_RESYNC_RECEIVING_SNAPSHOT ||
        chunk->session_id != assembler->begin.session_id) {
        resync_assembler_fail(assembler, TLC_HA_RESYNC_REASON_SESSION);
        return -1;
    }
    if (chunk->bytes == 0 || chunk->offset != assembler->received_bytes ||
        chunk->bytes > assembler->begin.checkpoint_blob_bytes -
                       assembler->received_bytes) {
        resync_assembler_fail(assembler, TLC_HA_RESYNC_REASON_OFFSET);
        return -1;
    }
    if (vemb_v16_xxh3_64(data, chunk->bytes) != chunk->checksum) {
        resync_assembler_fail(assembler, TLC_HA_RESYNC_REASON_CHECKSUM);
        return -1;
    }
    if (resync_write_full_at(assembler->artifact_fd, data, chunk->bytes,
                             chunk->offset) != 0) {
        resync_assembler_fail(assembler, TLC_HA_RESYNC_REASON_IO);
        return -1;
    }
    assembler->received_bytes += chunk->bytes;
    return 0;
}

int tlc_ha_resync_assembler_finish(
    tlc_ha_resync_assembler_t *assembler,
    const tlc_ha_resync_snapshot_end_t *end,
    char *path,
    size_t path_size,
    size_t *blob_bytes,
    tlc_ha_resync_snapshot_begin_t *begin) {
    if (!assembler || !end || !path || path_size == 0 || !blob_bytes || !begin)
        return -1;
    path[0] = '\0';
    *blob_bytes = 0;
    if (assembler->stage != TLC_HA_RESYNC_RECEIVING_SNAPSHOT ||
        end->session_id != assembler->begin.session_id) {
        resync_assembler_fail(assembler, TLC_HA_RESYNC_REASON_SESSION);
        return -1;
    }
    if (end->checkpoint_blob_bytes != assembler->begin.checkpoint_blob_bytes ||
        end->checkpoint_blob_checksum !=
            assembler->begin.checkpoint_blob_checksum ||
        assembler->received_bytes != assembler->begin.checkpoint_blob_bytes) {
        resync_assembler_fail(assembler, TLC_HA_RESYNC_REASON_GEOMETRY);
        return -1;
    }
    if (fsync(assembler->artifact_fd) != 0) {
        resync_assembler_fail(assembler, TLC_HA_RESYNC_REASON_IO);
        return -1;
    }
    uint64_t checksum = 0;
    if (resync_hash_fd(assembler->artifact_fd,
                       (size_t)assembler->received_bytes, &checksum) != 0) {
        resync_assembler_fail(assembler, TLC_HA_RESYNC_REASON_IO);
        return -1;
    }
    if (checksum != assembler->begin.checkpoint_blob_checksum) {
        resync_assembler_fail(assembler, TLC_HA_RESYNC_REASON_CHECKSUM);
        return -1;
    }
    if (close(assembler->artifact_fd) != 0) {
        assembler->artifact_fd = -1;
        resync_assembler_fail(assembler, TLC_HA_RESYNC_REASON_IO);
        return -1;
    }
    assembler->artifact_fd = -1;
    if (rename(assembler->part_path, assembler->blob_path) != 0 ||
        resync_fsync_directory(assembler->session_directory) != 0 ||
        strlen(assembler->blob_path) >= path_size) {
        unlink(assembler->blob_path);
        resync_assembler_fail(assembler, TLC_HA_RESYNC_REASON_IO);
        return -1;
    }
    memcpy(path, assembler->blob_path, strlen(assembler->blob_path) + 1u);
    *blob_bytes = (size_t)assembler->received_bytes;
    *begin = assembler->begin;
    assembler->received_bytes = 0;
    assembler->stage = TLC_HA_RESYNC_SNAPSHOT_COMPLETE;
    assembler->reason = TLC_HA_RESYNC_REASON_NONE;
    return 0;
}

tlc_ha_resync_stage_t tlc_ha_resync_assembler_stage(
    const tlc_ha_resync_assembler_t *assembler) {
    return assembler ? assembler->stage : TLC_HA_RESYNC_FAILED;
}

tlc_ha_resync_reason_t tlc_ha_resync_assembler_reason(
    const tlc_ha_resync_assembler_t *assembler) {
    return assembler ? assembler->reason : TLC_HA_RESYNC_REASON_SESSION;
}

static int replica_write_full(int fd, const void *data, size_t length) {
    const uint8_t *cursor = data;
    while (length != 0) {
        ssize_t written = write(fd, cursor, length);
        if (written < 0 && errno == EINTR)
            continue;
        if (written <= 0)
            return -1;
        cursor += (size_t)written;
        length -= (size_t)written;
    }
    return 0;
}

static int replica_read_full(int fd, void *data, size_t length) {
    uint8_t *cursor = data;
    while (length != 0) {
        ssize_t read_bytes = read(fd, cursor, length);
        if (read_bytes < 0 && errno == EINTR)
            continue;
        if (read_bytes <= 0)
            return -1;
        cursor += (size_t)read_bytes;
        length -= (size_t)read_bytes;
    }
    return 0;
}

static void replica_event_free(tlc_ha_replica_event_t *event) {
    if (!event)
        return;
    zfree(event->key);
    zfree(event->value);
    zfree(event);
}

static tlc_ha_replica_event_t *replica_event_copy(
        const tlc_cold_event_input_t *input,
        uint64_t seq) {
    tlc_ha_replica_event_t *event = zcalloc(sizeof(*event));
    if (!event)
        return NULL;
    event->input = *input;
    event->seq = seq;
    event->key = zmalloc(input->key_len);
    event->value = input->value_len ? zmalloc(input->value_len) : NULL;
    if (!event->key || (input->value_len && !event->value)) {
        replica_event_free(event);
        return NULL;
    }
    memcpy(event->key, input->key, input->key_len);
    if (input->value_len)
        memcpy(event->value, input->value, input->value_len);
    event->input.key = event->key;
    event->input.value = event->value;
    return event;
}

static void replica_stop_signal(tlc_ha_replica_t *replica) {
    /* A stopped standby must never resume normal COLD/Core work without a
     * new configured peer session, including after a lineage rejection. */
    if (replica->role == TLC_HA_REPLICA_FOLLOWER)
        atomic_store_explicit(&replica->resync_fenced, true,
                              memory_order_release);
    if (!atomic_exchange_explicit(&replica->stopping, true,
                                  memory_order_acq_rel))
        shutdown(replica->fd, SHUT_RDWR);
}

/* Serialize only the queue admission decision and tail publication.  The
 * potentially long AOF replay runs outside this gate. */
static void replica_producer_gate_lock(tlc_ha_replica_t *replica) {
    uint32_t spins = 0;
    while (atomic_flag_test_and_set_explicit(&replica->producer_admission_gate,
                                             memory_order_acquire))
        replica_queue_wait(&spins);
}

static void replica_producer_gate_unlock(tlc_ha_replica_t *replica) {
    atomic_flag_clear_explicit(&replica->producer_admission_gate,
                               memory_order_release);
}

static int replica_queue_try_push(tlc_ha_replica_t *replica,
                                   tlc_ha_replica_event_t *event) {
    uint64_t tail = atomic_load_explicit(&replica->queue_tail,
                                         memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&replica->queue_head,
                                         memory_order_acquire);
    if (tail - head >= replica->queue_capacity)
        return atomic_load_explicit(&replica->stopping, memory_order_acquire) ?
            -1 : TLC_HA_REPLICA_REPLAY_QUEUE_FULL;
    replica->queue[tail & replica->queue_mask] = event;
    atomic_store_explicit(&replica->queue_tail, tail + 1,
                          memory_order_release);
    return 0;
}

static int replica_queue_push(tlc_ha_replica_t *replica,
                              tlc_ha_replica_event_t *event) {
    uint32_t spins = 0;
    for (;;) {
        int rc = replica_queue_try_push(replica, event);
        if (rc == 0)
            return 0;
        if (rc < 0)
            return -1;
        replica_queue_wait(&spins);
    }
}

/* Returns non-zero only when seq becomes the earliest pending AOF replay. */
static int replica_record_replay_from(tlc_ha_replica_t *replica,
                                       uint64_t seq) {
    uint64_t current = atomic_load_explicit(&replica->replay_from_seq,
                                            memory_order_acquire);
    while (current == 0 || seq < current) {
        if (atomic_compare_exchange_weak_explicit(
                &replica->replay_from_seq, &current, seq,
                memory_order_acq_rel, memory_order_acquire))
            return 1;
    }
    return 0;
}

static tlc_ha_replica_event_t *replica_queue_try_pop(
        tlc_ha_replica_t *replica) {
    uint64_t head = atomic_load_explicit(&replica->queue_head,
                                         memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&replica->queue_tail,
                                         memory_order_acquire);
    if (head == tail)
        return NULL;
    tlc_ha_replica_event_t *event = replica->queue[head & replica->queue_mask];
    replica->queue[head & replica->queue_mask] = NULL;
    atomic_store_explicit(&replica->queue_head, head + 1,
                          memory_order_release);
    return event;
}

static tlc_ha_replica_event_t *replica_queue_peek(
        tlc_ha_replica_t *replica) {
    uint64_t head = atomic_load_explicit(&replica->queue_head,
                                         memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&replica->queue_tail,
                                         memory_order_acquire);
    return head == tail ? NULL : replica->queue[head & replica->queue_mask];
}

static uint32_t replica_queue_free_slots(const tlc_ha_replica_t *replica) {
    uint64_t head = atomic_load_explicit(&replica->queue_head,
                                         memory_order_acquire);
    uint64_t tail = atomic_load_explicit(&replica->queue_tail,
                                         memory_order_acquire);
    uint64_t used = tail - head;
    return used >= replica->queue_capacity ? 0 :
           (uint32_t)(replica->queue_capacity - used);
}

static tlc_ha_replica_event_t *replica_queue_pop(
        tlc_ha_replica_t *replica) {
    uint32_t spins = 0;
    for (;;) {
        tlc_ha_replica_event_t *event = replica_queue_try_pop(replica);
        if (event)
            return event;
        if (atomic_load_explicit(&replica->stopping, memory_order_acquire))
            return NULL;
        replica_queue_wait(&spins);
    }
}

static int replica_event_wire_size(const tlc_ha_replica_event_t *event,
                                   size_t *size_out) {
    size_t size = sizeof(tlc_ha_replica_event_wire_t);
    if (event->input.key_len > SIZE_MAX - size ||
        event->input.value_len > SIZE_MAX - size - event->input.key_len)
        return -1;
    *size_out = size + event->input.key_len + event->input.value_len;
    return 0;
}

static void replica_encode_frame_header(
        const tlc_ha_replica_frame_wire_t *header,
        tlc_ha_replica_frame_wire_t *wire) {
    *wire = (tlc_ha_replica_frame_wire_t){
        .magic = htonl(header->magic),
        .version = htons(header->version),
        .kind = htons(header->kind),
        .header_bytes = htonl(header->header_bytes),
        .first_seq = replica_hton64(header->first_seq),
        .event_count = htonl(header->event_count),
        .payload_bytes = htonl(header->payload_bytes),
        .checksum = replica_hton64(header->checksum),
    };
}

static void replica_decode_frame_header(
        const tlc_ha_replica_frame_wire_t *wire,
        tlc_ha_replica_frame_wire_t *header) {
    *header = (tlc_ha_replica_frame_wire_t){
        .magic = ntohl(wire->magic),
        .version = ntohs(wire->version),
        .kind = ntohs(wire->kind),
        .header_bytes = ntohl(wire->header_bytes),
        .first_seq = replica_ntoh64(wire->first_seq),
        .event_count = ntohl(wire->event_count),
        .payload_bytes = ntohl(wire->payload_bytes),
        .checksum = replica_ntoh64(wire->checksum),
    };
}

static void replica_encode_heartbeat(
        const tlc_ha_replica_heartbeat_t *heartbeat,
        tlc_ha_replica_heartbeat_t *wire) {
    *wire = (tlc_ha_replica_heartbeat_t){
        .hpc_node_id = replica_hton64(heartbeat->hpc_node_id),
        .peer_node_id = replica_hton64(heartbeat->peer_node_id),
        .ha_term = replica_hton64(heartbeat->ha_term),
        .role = htonl(heartbeat->role),
        .health = htonl(heartbeat->health),
        .sent_at_ns = replica_hton64(heartbeat->sent_at_ns),
        .durable_seq = replica_hton64(heartbeat->durable_seq),
        .progress_seq = replica_hton64(heartbeat->progress_seq),
    };
}

static void replica_decode_heartbeat(
        const tlc_ha_replica_heartbeat_t *wire,
        tlc_ha_replica_heartbeat_t *heartbeat) {
    *heartbeat = (tlc_ha_replica_heartbeat_t){
        .hpc_node_id = replica_ntoh64(wire->hpc_node_id),
        .peer_node_id = replica_ntoh64(wire->peer_node_id),
        .ha_term = replica_ntoh64(wire->ha_term),
        .role = ntohl(wire->role),
        .health = ntohl(wire->health),
        .sent_at_ns = replica_ntoh64(wire->sent_at_ns),
        .durable_seq = replica_ntoh64(wire->durable_seq),
        .progress_seq = replica_ntoh64(wire->progress_seq),
    };
}

static void resync_encode_u64(uint8_t **cursor, uint64_t value) {
    uint64_t wire = replica_hton64(value);
    memcpy(*cursor, &wire, sizeof(wire));
    *cursor += sizeof(wire);
}

static uint64_t resync_decode_u64(const uint8_t **cursor) {
    uint64_t wire;
    memcpy(&wire, *cursor, sizeof(wire));
    *cursor += sizeof(wire);
    return replica_ntoh64(wire);
}

static void resync_encode_u32(uint8_t **cursor, uint32_t value) {
    uint32_t wire = htonl(value);
    memcpy(*cursor, &wire, sizeof(wire));
    *cursor += sizeof(wire);
}

static uint32_t resync_decode_u32(const uint8_t **cursor) {
    uint32_t wire;
    memcpy(&wire, *cursor, sizeof(wire));
    *cursor += sizeof(wire);
    return ntohl(wire);
}

static void resync_encode_begin(const tlc_ha_resync_snapshot_begin_t *begin,
                                uint8_t payload[TLC_HA_RESYNC_BEGIN_WIRE_BYTES]) {
    uint8_t *cursor = payload;
    resync_encode_u64(&cursor, begin->session_id);
    resync_encode_u64(&cursor, begin->leader_node_id);
    resync_encode_u64(&cursor, begin->follower_node_id);
    resync_encode_u64(&cursor, begin->ha_term);
    resync_encode_u64(&cursor, begin->topology_epoch);
    resync_encode_u64(&cursor, begin->generation);
    resync_encode_u64(&cursor, begin->checkpoint_seq);
    resync_encode_u64(&cursor, begin->durable_boundary_seq);
    resync_encode_u64(&cursor, begin->checkpoint_blob_bytes);
    resync_encode_u64(&cursor, begin->checkpoint_blob_checksum);
    resync_encode_u64(&cursor, begin->captured_seq_checksum);
    resync_encode_u32(&cursor, begin->meta_shard_count);
}

static int resync_decode_begin(const uint8_t *payload, uint32_t bytes,
                               tlc_ha_resync_snapshot_begin_t *begin) {
    if (bytes != TLC_HA_RESYNC_BEGIN_WIRE_BYTES)
        return -1;
    const uint8_t *cursor = payload;
    *begin = (tlc_ha_resync_snapshot_begin_t){
        .session_id = resync_decode_u64(&cursor),
        .leader_node_id = resync_decode_u64(&cursor),
        .follower_node_id = resync_decode_u64(&cursor),
        .ha_term = resync_decode_u64(&cursor),
        .topology_epoch = resync_decode_u64(&cursor),
        .generation = resync_decode_u64(&cursor),
        .checkpoint_seq = resync_decode_u64(&cursor),
        .durable_boundary_seq = resync_decode_u64(&cursor),
        .checkpoint_blob_bytes = resync_decode_u64(&cursor),
        .checkpoint_blob_checksum = resync_decode_u64(&cursor),
        .captured_seq_checksum = resync_decode_u64(&cursor),
        .meta_shard_count = resync_decode_u32(&cursor),
    };
    return 0;
}

static void resync_encode_chunk_header(
    const tlc_ha_resync_snapshot_chunk_t *chunk,
    uint8_t payload[TLC_HA_RESYNC_CHUNK_HEADER_WIRE_BYTES]) {
    uint8_t *cursor = payload;
    resync_encode_u64(&cursor, chunk->session_id);
    resync_encode_u64(&cursor, chunk->offset);
    resync_encode_u32(&cursor, chunk->bytes);
    resync_encode_u64(&cursor, chunk->checksum);
}

static int resync_decode_chunk_header(const uint8_t *payload, uint32_t bytes,
                                      tlc_ha_resync_snapshot_chunk_t *chunk,
                                      const uint8_t **data) {
    if (bytes < TLC_HA_RESYNC_CHUNK_HEADER_WIRE_BYTES)
        return -1;
    const uint8_t *cursor = payload;
    *chunk = (tlc_ha_resync_snapshot_chunk_t){
        .session_id = resync_decode_u64(&cursor),
        .offset = resync_decode_u64(&cursor),
        .bytes = resync_decode_u32(&cursor),
        .checksum = resync_decode_u64(&cursor),
    };
    if (chunk->bytes != bytes - TLC_HA_RESYNC_CHUNK_HEADER_WIRE_BYTES)
        return -1;
    *data = cursor;
    return 0;
}

static void resync_encode_end(const tlc_ha_resync_snapshot_end_t *end,
                              uint8_t payload[TLC_HA_RESYNC_END_WIRE_BYTES]) {
    uint8_t *cursor = payload;
    resync_encode_u64(&cursor, end->session_id);
    resync_encode_u64(&cursor, end->checkpoint_blob_bytes);
    resync_encode_u64(&cursor, end->checkpoint_blob_checksum);
}

static int resync_decode_end(const uint8_t *payload, uint32_t bytes,
                             tlc_ha_resync_snapshot_end_t *end) {
    if (bytes != TLC_HA_RESYNC_END_WIRE_BYTES)
        return -1;
    const uint8_t *cursor = payload;
    *end = (tlc_ha_resync_snapshot_end_t){
        .session_id = resync_decode_u64(&cursor),
        .checkpoint_blob_bytes = resync_decode_u64(&cursor),
        .checkpoint_blob_checksum = resync_decode_u64(&cursor),
    };
    return 0;
}

static void resync_encode_control(const tlc_ha_resync_control_t *control,
                                  uint8_t payload[TLC_HA_RESYNC_CONTROL_WIRE_BYTES]) {
    uint8_t *cursor = payload;
    resync_encode_u64(&cursor, control->session_id);
    resync_encode_u64(&cursor, control->generation);
    resync_encode_u64(&cursor, control->checkpoint_seq);
    resync_encode_u64(&cursor, control->durable_seq);
    resync_encode_u64(&cursor, control->applied_seq);
    resync_encode_u64(&cursor, control->durable_boundary_seq);
}

static int resync_decode_control(const uint8_t *payload, uint32_t bytes,
                                 tlc_ha_resync_control_t *control) {
    if (bytes != TLC_HA_RESYNC_CONTROL_WIRE_BYTES)
        return -1;
    const uint8_t *cursor = payload;
    *control = (tlc_ha_resync_control_t){
        .session_id = resync_decode_u64(&cursor),
        .generation = resync_decode_u64(&cursor),
        .checkpoint_seq = resync_decode_u64(&cursor),
        .durable_seq = resync_decode_u64(&cursor),
        .applied_seq = resync_decode_u64(&cursor),
        .durable_boundary_seq = resync_decode_u64(&cursor),
    };
    return 0;
}

static void resync_encode_required(
        const tlc_ha_resync_required_t *required,
        uint8_t payload[TLC_HA_RESYNC_REQUIRED_WIRE_BYTES]) {
    uint8_t *cursor = payload;
    resync_encode_u64(&cursor, required->ha_term);
    resync_encode_u64(&cursor, required->topology_epoch);
    resync_encode_u64(&cursor, required->durable_seq);
    resync_encode_u64(&cursor, required->applied_seq);
    resync_encode_u32(&cursor, required->reason);
    resync_encode_u32(&cursor, 0);
}

static int resync_decode_required(const uint8_t *payload, uint32_t bytes,
                                  tlc_ha_resync_required_t *required) {
    if (bytes != TLC_HA_RESYNC_REQUIRED_WIRE_BYTES)
        return -1;
    const uint8_t *cursor = payload;
    *required = (tlc_ha_resync_required_t){
        .ha_term = resync_decode_u64(&cursor),
        .topology_epoch = resync_decode_u64(&cursor),
        .durable_seq = resync_decode_u64(&cursor),
        .applied_seq = resync_decode_u64(&cursor),
        .reason = resync_decode_u32(&cursor),
    };
    uint32_t reserved = resync_decode_u32(&cursor);
    return reserved == 0 && replica_resync_required_reason_valid(
                                required->reason) ? 0 : -1;
}

static int replica_send_frame(tlc_ha_replica_t *replica,
                              uint16_t kind,
                              uint64_t first_seq,
                              uint32_t event_count,
                              const void *payload,
                              uint32_t payload_bytes);

static int replica_send_resync_control(tlc_ha_replica_t *replica,
                                       uint16_t kind,
                                       const tlc_ha_resync_control_t *control) {
    uint8_t payload[TLC_HA_RESYNC_CONTROL_WIRE_BYTES];
    resync_encode_control(control, payload);
    return replica_send_frame(replica, kind, 0, 0, payload, sizeof(payload));
}

static int replica_send_resync_required(
        tlc_ha_replica_t *replica,
        const tlc_ha_resync_required_t *required) {
    uint8_t payload[TLC_HA_RESYNC_REQUIRED_WIRE_BYTES];
    resync_encode_required(required, payload);
    return replica_send_frame(replica, TLC_HA_REPLICA_KIND_RESYNC_REQUIRED,
                              0, 0, payload, sizeof(payload));
}

static int replica_send_frame(tlc_ha_replica_t *replica,
                              uint16_t kind,
                              uint64_t first_seq,
                              uint32_t event_count,
                              const void *payload,
                              uint32_t payload_bytes) {
    tlc_ha_replica_frame_wire_t header = {
        .magic = TLC_HA_REPLICA_MAGIC,
        .version = TLC_HA_REPLICA_VERSION,
        .kind = kind,
        .header_bytes = sizeof(header),
        .first_seq = first_seq,
        .event_count = event_count,
        .payload_bytes = payload_bytes,
        .checksum = 0,
    };
    tlc_ha_replica_frame_wire_t wire_header;
    replica_encode_frame_header(&header, &wire_header);
    size_t checksum_bytes = sizeof(wire_header) + payload_bytes;
    uint8_t *checksum_data = zmalloc(checksum_bytes);
    if (!checksum_data)
        return -1;
    memcpy(checksum_data, &wire_header, sizeof(wire_header));
    if (payload_bytes)
        memcpy(checksum_data + sizeof(wire_header), payload, payload_bytes);
    ((tlc_ha_replica_frame_wire_t *)checksum_data)->checksum = 0;
    header.checksum = vemb_v16_xxh3_64(checksum_data, checksum_bytes);
    zfree(checksum_data);
    replica_encode_frame_header(&header, &wire_header);

    int rc = 0;
    if (replica->transport == TLC_HA_REPLICA_TRANSPORT_UB) {
        size_t frame_bytes = sizeof(wire_header) + payload_bytes;
        if (frame_bytes > replica->tx_ring.config.slot_bytes) {
            rc = -1;
        } else {
            tlc_ha_replica_ring_slot_t *slot = NULL;
            uint64_t position = 0;
            uint32_t spins = 0;
            while (!atomic_load_explicit(&replica->stopping, memory_order_acquire)) {
                slot = replica_ring_reserve(&replica->tx_ring, &position);
                if (slot)
                    break;
                replica_queue_wait(&spins);
            }
            if (!slot) {
                rc = -1;
            } else {
                /* Each sender owns its reserved slot until release commit. */
                memcpy(slot->payload, &wire_header, sizeof(wire_header));
                if (payload_bytes)
                    memcpy(slot->payload + sizeof(wire_header), payload,
                           payload_bytes);
                replica_ring_publish(slot, position);
            }
        }
    } else {
        pthread_mutex_lock(&replica->stream_write_mutex);
        rc = replica_write_full(replica->fd, &wire_header, sizeof(wire_header));
        if (rc == 0 && payload_bytes)
            rc = replica_write_full(replica->fd, payload, payload_bytes);
        pthread_mutex_unlock(&replica->stream_write_mutex);
    }
    if (rc != 0)
        replica_stop_signal(replica);
    return rc;
}

int tlc_ha_replica_send_resync_snapshot(
    tlc_ha_replica_t *replica,
    uint64_t session_id,
    uint64_t topology_epoch,
    const tlc_cold_resync_snapshot_t *snapshot,
    uint32_t chunk_bytes) {
    if (!replica || !snapshot || replica->role != TLC_HA_REPLICA_LEADER ||
        session_id == 0 || snapshot->checkpoint_blob == NULL ||
        snapshot->checkpoint_blob_bytes == 0 ||
        snapshot->checkpoint_blob_bytes > UINT64_MAX ||
        snapshot->meta_shard_count == 0 || snapshot->checkpoint.generation == 0 ||
        snapshot->checkpoint.checkpoint_seq == UINT64_MAX ||
        snapshot->durable_boundary_seq < snapshot->checkpoint.checkpoint_seq ||
        snapshot->checkpoint_blob_bytes > replica->max_resync_snapshot_bytes ||
        replica->max_batch_bytes < TLC_HA_RESYNC_CHUNK_HEADER_WIRE_BYTES + 1u)
        return -1;
    if (vemb_v16_xxh3_64(snapshot->checkpoint_blob,
                         snapshot->checkpoint_blob_bytes) !=
        snapshot->checkpoint_blob_checksum)
        return -1;
    uint32_t max_chunk = replica->max_batch_bytes -
                         TLC_HA_RESYNC_CHUNK_HEADER_WIRE_BYTES;
    if (chunk_bytes == 0 || chunk_bytes > max_chunk)
        chunk_bytes = max_chunk;
    tlc_ha_resync_snapshot_begin_t begin = {
        .session_id = session_id,
        .leader_node_id = replica->hpc_node_id,
        .follower_node_id = replica->peer_node_id,
        .ha_term = replica->ha_term,
        .topology_epoch = topology_epoch,
        .generation = snapshot->checkpoint.generation,
        .checkpoint_seq = snapshot->checkpoint.checkpoint_seq,
        .durable_boundary_seq = snapshot->durable_boundary_seq,
        .checkpoint_blob_bytes = snapshot->checkpoint_blob_bytes,
        .checkpoint_blob_checksum = snapshot->checkpoint_blob_checksum,
        .captured_seq_checksum = snapshot->captured_seq_checksum,
        .meta_shard_count = snapshot->meta_shard_count,
    };
    uint8_t begin_payload[TLC_HA_RESYNC_BEGIN_WIRE_BYTES];
    resync_encode_begin(&begin, begin_payload);
    replica_resync_touch(replica);
    if (replica_send_frame(replica, TLC_HA_REPLICA_KIND_SNAPSHOT_BEGIN,
                           0, 0, begin_payload,
                           sizeof(begin_payload)) != 0)
        return -1;
    const uint8_t *blob = snapshot->checkpoint_blob;
    for (uint64_t offset = 0; offset < snapshot->checkpoint_blob_bytes;) {
        uint64_t remaining = snapshot->checkpoint_blob_bytes - offset;
        uint32_t bytes = remaining < chunk_bytes ? (uint32_t)remaining :
                                                   chunk_bytes;
        size_t payload_bytes = TLC_HA_RESYNC_CHUNK_HEADER_WIRE_BYTES + bytes;
        uint8_t *payload = zmalloc(payload_bytes);
        if (!payload)
            return -1;
        tlc_ha_resync_snapshot_chunk_t chunk = {
            .session_id = session_id,
            .offset = offset,
            .bytes = bytes,
            .checksum = vemb_v16_xxh3_64(blob + offset, bytes),
        };
        resync_encode_chunk_header(&chunk, payload);
        memcpy(payload + TLC_HA_RESYNC_CHUNK_HEADER_WIRE_BYTES,
               blob + offset, bytes);
        int rc = replica_send_frame(replica, TLC_HA_REPLICA_KIND_SNAPSHOT_CHUNK,
                                    0, 0, payload, (uint32_t)payload_bytes);
        zfree(payload);
        if (rc != 0)
            return -1;
        replica_resync_touch(replica);
        offset += bytes;
    }
    tlc_ha_resync_snapshot_end_t end = {
        .session_id = session_id,
        .checkpoint_blob_bytes = snapshot->checkpoint_blob_bytes,
        .checkpoint_blob_checksum = snapshot->checkpoint_blob_checksum,
    };
    uint8_t end_payload[TLC_HA_RESYNC_END_WIRE_BYTES];
    resync_encode_end(&end, end_payload);
    replica_resync_touch(replica);
    return replica_send_frame(replica, TLC_HA_REPLICA_KIND_SNAPSHOT_END,
                              0, 0, end_payload, sizeof(end_payload));
}

int tlc_ha_replica_begin_resync(tlc_ha_replica_t *replica,
                                 uint64_t session_id,
                                 uint64_t topology_epoch,
                                 uint32_t chunk_bytes) {
    RETURN_IF(!replica || replica->role != TLC_HA_REPLICA_LEADER ||
              session_id == 0 || topology_epoch == 0, -1);
    unsigned expected = TLC_HA_RESYNC_SESSION_IDLE;
    if (!atomic_compare_exchange_strong_explicit(
            &replica->resync_session_state, &expected,
            TLC_HA_RESYNC_SESSION_LEADER_WAIT_INSTALLED,
            memory_order_acq_rel, memory_order_acquire))
        return -1;
    if (tlc_cold_begin_resync_snapshot(replica->cold,
                                       tlc_core_meta_shard_count(replica->core),
                                       &replica->leader_resync_snapshot) != 0) {
        serverLog(LL_WARNING,
                  "HA Replica snapshot begin failed: session=%llu stage=checkpoint-export errno=%d (%s)",
                  (unsigned long long)session_id, errno, strerror(errno));
        atomic_store_explicit(&replica->resync_session_state,
                              TLC_HA_RESYNC_SESSION_IDLE,
                              memory_order_release);
        return -1;
    }
    tlc_cold_resync_snapshot_t *snapshot = &replica->leader_resync_snapshot;
    serverLog(LL_NOTICE,
              "HA Replica snapshot exported: session=%llu checkpoint=%llu boundary=%llu bytes=%zu",
              (unsigned long long)session_id,
              (unsigned long long)snapshot->checkpoint.checkpoint_seq,
              (unsigned long long)snapshot->durable_boundary_seq,
              snapshot->checkpoint_blob_bytes);
    replica->resync_session_id = session_id;
    replica->resync_generation = snapshot->checkpoint.generation;
    replica->resync_checkpoint_seq = snapshot->checkpoint.checkpoint_seq;
    replica->resync_boundary_seq = snapshot->durable_boundary_seq;
    replica->resync_next_seq = snapshot->tail_start_seq;
    replica->resync_topology_epoch = topology_epoch;
    replica->resync_meta_shard_count = snapshot->meta_shard_count;
    replica->resync_captured_seq_checksum = snapshot->captured_seq_checksum;
    replica_resync_touch(replica);
    tlc_ha_resync_control_t request = {
        .session_id = session_id,
        .generation = snapshot->checkpoint.generation,
        .checkpoint_seq = snapshot->checkpoint.checkpoint_seq,
        .durable_boundary_seq = snapshot->durable_boundary_seq,
    };
    if (replica_send_resync_control(replica, TLC_HA_REPLICA_KIND_RESYNC_REQUEST,
                                    &request) != 0) {
        serverLog(LL_WARNING,
                  "HA Replica snapshot begin failed: session=%llu stage=request-send errno=%d (%s)",
                  (unsigned long long)session_id, errno, strerror(errno));
        tlc_cold_end_resync_snapshot(replica->cold, snapshot);
        atomic_store_explicit(&replica->resync_session_state,
                              TLC_HA_RESYNC_SESSION_IDLE,
                              memory_order_release);
        return -1;
    }
    serverLog(LL_NOTICE,
              "HA Replica snapshot request sent: session=%llu",
              (unsigned long long)session_id);
    if (tlc_ha_replica_send_resync_snapshot(replica, session_id,
                                             topology_epoch, snapshot,
                                             chunk_bytes) != 0) {
        serverLog(LL_WARNING,
                  "HA Replica snapshot begin failed: session=%llu stage=artifact-send errno=%d (%s)",
                  (unsigned long long)session_id, errno, strerror(errno));
        replica_abort_resync_internal(replica, 1);
        return -1;
    }
    serverLog(LL_NOTICE,
              "HA Replica snapshot artifact sent: session=%llu",
              (unsigned long long)session_id);
    return 0;
}

int tlc_ha_replica_take_resync_snapshot(
    tlc_ha_replica_t *replica,
    char *path,
    size_t path_size,
    size_t *blob_bytes,
    tlc_ha_resync_snapshot_begin_t *begin) {
    if (!replica || !path || path_size == 0 || !blob_bytes || !begin ||
        replica->role != TLC_HA_REPLICA_FOLLOWER)
        return -1;
    path[0] = '\0';
    *blob_bytes = 0;
    uintptr_t published = atomic_exchange_explicit(
        &replica->received_snapshot_artifact, (uintptr_t)NULL,
        memory_order_acquire);
    if (published == (uintptr_t)NULL)
        return -1;
    atomic_store_explicit(&replica->resync_install_pending, false,
                          memory_order_release);
    tlc_ha_resync_artifact_t *artifact =
        (tlc_ha_resync_artifact_t *)published;
    if (strlen(artifact->path) >= path_size) {
        resync_artifact_delete(artifact);
        return -1;
    }
    memcpy(path, artifact->path, strlen(artifact->path) + 1u);
    *blob_bytes = artifact->blob_bytes;
    *begin = artifact->begin;
    zfree(artifact);
    return 0;
}

int tlc_ha_replica_install_resync_snapshot(
    tlc_ha_replica_t *replica,
    tlc_cold_checkpoint_result_t *result) {
    if (!replica || !result || replica->role != TLC_HA_REPLICA_FOLLOWER)
        return -1;
    char path[PATH_MAX];
    size_t blob_bytes = 0;
    tlc_ha_resync_snapshot_begin_t begin;
    if (tlc_ha_replica_take_resync_snapshot(replica, path, sizeof(path),
                                            &blob_bytes, &begin) != 0)
        return -1;

    int artifact_fd = open(path, O_RDONLY | O_CLOEXEC);
    struct stat artifact_stat;
    struct stat cold_directory_stat;
    if (artifact_fd < 0 || fstat(artifact_fd, &artifact_stat) != 0 ||
        !S_ISREG(artifact_stat.st_mode) || artifact_stat.st_size < 0 ||
        (uint64_t)artifact_stat.st_size != blob_bytes ||
        stat(tlc_cold_directory(tlc_core_get_cold(replica->core)),
             &cold_directory_stat) != 0 ||
        artifact_stat.st_dev != cold_directory_stat.st_dev) {
        if (artifact_fd >= 0)
            close(artifact_fd);
        unlink(path);
        return -1;
    }
    uint64_t artifact_checksum = 0;
    if (resync_hash_fd(artifact_fd, blob_bytes, &artifact_checksum) != 0 ||
        artifact_checksum != begin.checkpoint_blob_checksum) {
        close(artifact_fd);
        unlink(path);
        return -1;
    }

    atomic_store_explicit(&replica->resync_fenced, true, memory_order_release);
    while (atomic_load_explicit(&replica->ingress_inflight,
                                memory_order_acquire) != 0 ||
           atomic_load_explicit(&replica->apply_inflight,
                                memory_order_acquire) != 0)
        usleep(1000);
    for (;;) {
        tlc_ha_replica_event_t *event = replica_queue_try_pop(replica);
        if (!event)
            break;
        replica_event_free(event);
    }
    int rc = tlc_core_install_resync_checkpoint_file(replica->core,
                                                      artifact_fd, path,
                                                      blob_bytes, result);
    close(artifact_fd);
    if (rc == 0 && (result->generation != begin.generation ||
                    result->ha_term != begin.ha_term ||
                    result->checkpoint_seq != begin.checkpoint_seq))
        rc = -1;
    if (rc == 0) {
        atomic_store_explicit(&replica->applied_seq, result->checkpoint_seq,
                              memory_order_release);
        if (atomic_load_explicit(&replica->resync_session_state,
                                 memory_order_acquire) ==
                TLC_HA_RESYNC_SESSION_FOLLOWER_WAIT_INSTALL) {
            uint64_t *captured = zmalloc(sizeof(*captured) *
                                         begin.meta_shard_count);
            if (!captured || tlc_cold_load_checkpoint(
                    replica->cold, begin.meta_shard_count,
                    replica_capture_resync_seq, captured, NULL) != 0) {
                zfree(captured);
                return -1;
            }
            zfree(replica->resync_captured_seq);
            replica->resync_captured_seq = captured;
            replica->resync_session_id = begin.session_id;
            replica->resync_generation = begin.generation;
            replica->resync_checkpoint_seq = begin.checkpoint_seq;
            replica->resync_boundary_seq = begin.durable_boundary_seq;
            replica->resync_next_seq = begin.checkpoint_seq + 1u;
            replica->resync_topology_epoch = begin.topology_epoch;
            replica->resync_meta_shard_count = begin.meta_shard_count;
            replica->resync_captured_seq_checksum = begin.captured_seq_checksum;
        atomic_store_explicit(&replica->resync_session_state,
                              TLC_HA_RESYNC_SESSION_FOLLOWER_WAIT_TAIL,
                              memory_order_release);
        tlc_ha_resync_control_t request = {
            .session_id = begin.session_id,
            .generation = begin.generation,
            .checkpoint_seq = begin.checkpoint_seq,
            .durable_seq = begin.checkpoint_seq + 1u,
            .durable_boundary_seq = begin.durable_boundary_seq,
        };
        tlc_ha_resync_control_t installed = {
            .session_id = begin.session_id,
            .generation = begin.generation,
                .checkpoint_seq = begin.checkpoint_seq,
                .durable_seq = begin.checkpoint_seq,
                .applied_seq = begin.checkpoint_seq,
                .durable_boundary_seq = begin.durable_boundary_seq,
            };
            if (replica_send_resync_control(replica,
                                            TLC_HA_REPLICA_KIND_SNAPSHOT_INSTALLED,
                                            &installed) != 0)
                return -1;
            if (replica_send_resync_control(replica,
                                            TLC_HA_REPLICA_KIND_TAIL_REQUEST,
                                            &request) != 0)
                return -1;
        }
    }
    return rc;
}

static int replica_send_ack(tlc_ha_replica_t *replica, uint64_t accepted_seq) {
    uint64_t payload = replica_hton64(accepted_seq);
    return replica_send_frame(replica, TLC_HA_REPLICA_KIND_ACK, accepted_seq,
                              0, &payload, sizeof(payload));
}

static void replica_log_progress(tlc_ha_replica_t *replica) {
    uint64_t now = getMonotonicNs();
    if (now - replica->last_progress_log_ns < 1000000000ULL)
        return;
    replica->last_progress_log_ns = now;
    tlc_ha_replica_progress_t progress;
    if (tlc_ha_replica_get_progress(replica, &progress) != 0)
        return;
    serverLog(LL_NOTICE,
              "HA Replica progress: role=%u appended=%llu durable=%llu applied=%llu peer_accepted=%llu peer_durable=%llu peer_applied=%llu replay_from=%llu peer_health=%u",
              replica->role, (unsigned long long)progress.appended_seq,
              (unsigned long long)progress.durable_seq,
              (unsigned long long)progress.applied_seq,
              (unsigned long long)progress.peer_accepted_seq,
              (unsigned long long)progress.peer_durable_seq,
              (unsigned long long)progress.peer_applied_seq,
              (unsigned long long)progress.replay_from_seq,
              atomic_load_explicit(&replica->peer_health,
                                   memory_order_acquire));
}

static int replica_send_heartbeat(tlc_ha_replica_t *replica) {
    tlc_cold_progress_t progress = {0};
    tlc_ha_replica_health_t health = TLC_HA_REPLICA_HEALTHY;
    if (tlc_cold_get_progress(replica->cold, &progress) != 0)
        health = TLC_HA_REPLICA_DEGRADED;
    tlc_ha_replica_heartbeat_t heartbeat = {
        .hpc_node_id = replica->hpc_node_id,
        .peer_node_id = replica->peer_node_id,
        .ha_term = replica->ha_term,
        .role = replica->role,
        .health = health,
        .sent_at_ns = getMonotonicNs(),
        .durable_seq = progress.durable_seq,
        .progress_seq = replica->role == TLC_HA_REPLICA_LEADER ?
            atomic_load_explicit(&replica->peer_accepted_seq,
                                 memory_order_acquire) :
            atomic_load_explicit(&replica->applied_seq,
                                 memory_order_acquire),
    };
    tlc_ha_replica_heartbeat_t wire;
    replica_encode_heartbeat(&heartbeat, &wire);
    return replica_send_frame(replica, TLC_HA_REPLICA_KIND_HEARTBEAT, 0, 0,
                               &wire, sizeof(wire));
}

static int replica_event_sink(const tlc_cold_event_input_t *input,
                              uint64_t seq,
                              void *arg) {
    tlc_ha_replica_t *replica = arg;
    /* Fast path while replay is active: the event is already durable in the
     * Leader AOF, so only the earliest replay cursor needs updating. The
     * admission-gate check below closes the transition race. */
    if (atomic_load_explicit(&replica->resync_emission_gate,
                             memory_order_acquire) ||
        atomic_load_explicit(&replica->replay_mode, memory_order_acquire) ||
        atomic_load_explicit(&replica->replay_from_seq, memory_order_acquire) !=
            0) {
        replica_record_replay_from(replica, seq);
        return 0;
    }
    tlc_ha_replica_event_t *event = replica_event_copy(input, seq);
    if (!event) {
        replica_stop_signal(replica);
        return -1;
    }

    /* Admission and queue publication must be one linearizable operation;
     * otherwise sender replay can race this producer between the state check
     * and queue_tail publication. */
    replica_producer_gate_lock(replica);
    if (atomic_load_explicit(&replica->resync_emission_gate,
                             memory_order_acquire) ||
        atomic_load_explicit(&replica->replay_mode, memory_order_acquire) ||
        atomic_load_explicit(&replica->replay_from_seq, memory_order_acquire) !=
            0) {
        replica_record_replay_from(replica, seq);
        replica_producer_gate_unlock(replica);
        replica_event_free(event);
        return 0;
    }
    int queue_rc = replica_queue_try_push(replica, event);
    if (queue_rc == TLC_HA_REPLICA_REPLAY_QUEUE_FULL) {
        if (replica_record_replay_from(replica, seq))
            serverLog(LL_NOTICE,
                      "HA Replica sender queue full: replay_from=%llu; scheduling AOF replay",
                      (unsigned long long)seq);
        replica_producer_gate_unlock(replica);
        replica_event_free(event);
        return 0;
    }
    replica_producer_gate_unlock(replica);
    if (queue_rc != 0) {
        replica_event_free(event);
        return -1;
    }
    return 0;
}

static int replica_replay_queue_sink(const tlc_cold_event_input_t *input,
                                     uint64_t seq,
                                     void *arg) {
    tlc_ha_replica_t *replica = arg;
    tlc_ha_replica_event_t *event = replica_event_copy(input, seq);
    if (!event) {
        replica_stop_signal(replica);
        return -1;
    }
    replica_producer_gate_lock(replica);
    int queue_rc = replica_queue_try_push(replica, event);
    if (queue_rc == TLC_HA_REPLICA_REPLAY_QUEUE_FULL) {
        if (replica_record_replay_from(replica, seq))
            serverLog(LL_NOTICE,
                      "HA Replica sender queue full: replay_from=%llu; scheduling AOF replay",
                      (unsigned long long)seq);
        replica_producer_gate_unlock(replica);
        replica_event_free(event);
        return TLC_COLD_REPLAY_STOP;
    }
    replica_producer_gate_unlock(replica);
    if (queue_rc != 0) {
        replica_event_free(event);
        return -1;
    }
    return 0;
}

static int replica_send_tail_event(const tlc_cold_event_input_t *input,
                                   uint64_t seq, void *arg) {
    size_t payload_bytes = sizeof(tlc_ha_replica_event_wire_t) + input->key_len +
                           input->value_len;
    uint8_t *payload = zmalloc(payload_bytes);
    if (!payload)
        return -1;
    tlc_ha_replica_event_wire_t wire = {
        .ha_term = replica_hton64(input->ha_term),
        .topology_epoch = replica_hton64(input->topology_epoch),
        .op = htonl(input->op),
        .meta_shard_id = htonl(input->meta_shard_id),
        .version = replica_hton64(input->version),
        .key_len = htonl(input->key_len),
        .value_len = htonl(input->value_len),
    };
    memcpy(payload, &wire, sizeof(wire));
    memcpy(payload + sizeof(wire), input->key, input->key_len);
    if (input->value_len)
        memcpy(payload + sizeof(wire) + input->key_len, input->value,
               input->value_len);
    int rc = replica_send_frame(arg, TLC_HA_REPLICA_KIND_TAIL_EVENTS, seq, 1,
                                payload, (uint32_t)payload_bytes);
    zfree(payload);
    if (rc == 0)
        replica_resync_touch(arg);
    return rc;
}

static int replica_send_resync_tail(tlc_ha_replica_t *replica,
                                    uint64_t next_seq) {
    tlc_cold_resync_snapshot_t *snapshot = &replica->leader_resync_snapshot;
    if (next_seq < snapshot->tail_start_seq ||
        next_seq > snapshot->durable_boundary_seq + 1u)
        return -1;
    uint64_t cursor = next_seq;
    while (cursor <= snapshot->durable_boundary_seq) {
        uint32_t event_count = 0;
        int read_rc = tlc_cold_read_resync_snapshot(
            replica->cold, snapshot, &cursor, 1, replica_send_tail_event,
            replica, &event_count);
        if (read_rc != 0 || event_count != 1) {
            serverLog(LL_WARNING,
                      "HA Replica snapshot tail read failed: session=%llu next=%llu rc=%d count=%u errno=%d (%s)",
                      (unsigned long long)replica->resync_session_id,
                      (unsigned long long)cursor, read_rc, event_count,
                      errno, strerror(errno));
            return -1;
        }
    }
    replica->resync_boundary_seq = snapshot->durable_boundary_seq;
    replica->resync_next_seq = cursor;
    tlc_ha_resync_control_t end = {
        .session_id = replica->resync_session_id,
        .generation = replica->resync_generation,
        .checkpoint_seq = replica->resync_checkpoint_seq,
        .durable_seq = snapshot->durable_boundary_seq,
        .durable_boundary_seq = snapshot->durable_boundary_seq,
    };
    atomic_store_explicit(&replica->resync_session_state,
                          TLC_HA_RESYNC_SESSION_LEADER_WAIT_ACK,
                          memory_order_release);
    return replica_send_resync_control(replica, TLC_HA_REPLICA_KIND_TAIL_END,
                                       &end);
}

/* The sender replays a bounded AOF prefix into the normal outbound queue and
 * resumes later from the earliest unqueued seq. */
static int replica_replay_pending(tlc_ha_replica_t *replica) {
    replica_producer_gate_lock(replica);
    uint64_t start = atomic_exchange_explicit(&replica->replay_from_seq, 0,
                                              memory_order_acq_rel);
    replica_producer_gate_unlock(replica);
    if (start == 0) {
        if (atomic_exchange_explicit(&replica->replay_log_active, false,
                                     memory_order_acq_rel)) {
            uint64_t replay_start = atomic_exchange_explicit(
                &replica->replay_log_start_seq, 0, memory_order_acq_rel);
            tlc_cold_progress_t progress;
            if (tlc_cold_get_progress(replica->cold, &progress) == 0)
                serverLog(LL_NOTICE,
                          "HA Replica AOF replay complete: start=%llu end=%llu",
                          (unsigned long long)replay_start,
                          (unsigned long long)progress.appended_seq);
        }
        replica_producer_gate_lock(replica);
        if (atomic_load_explicit(&replica->replay_from_seq,
                                 memory_order_acquire) == 0 &&
            !atomic_load_explicit(&replica->replay_producer_active,
                                  memory_order_acquire)) {
            atomic_store_explicit(&replica->replay_mode, false,
                                  memory_order_release);
        }
        replica_producer_gate_unlock(replica);
        return 0;
    }
    tlc_cold_progress_t progress;
    if (tlc_cold_get_progress(replica->cold, &progress) != 0)
        return -1;
    if (start > progress.appended_seq) {
        replica_record_replay_from(replica, start);
        return 0;
    }
    uint32_t queue_slots = replica_queue_free_slots(replica);
    if (queue_slots == 0)
        return 0;
    if (!atomic_exchange_explicit(&replica->replay_log_active, true,
                                  memory_order_acq_rel)) {
        atomic_store_explicit(&replica->replay_log_start_seq, start,
                              memory_order_release);
        serverLog(LL_NOTICE,
                  "HA Replica AOF replay start: start=%llu end=%llu",
                  (unsigned long long)start,
                  (unsigned long long)progress.appended_seq);
    }
    atomic_fetch_add_explicit(&replica->sender_inflight, 1,
                              memory_order_acq_rel);
    uint64_t next_seq = start;
    uint32_t replayed_events = 0;
    int rc = tlc_cold_replay_range_limited(
        replica->cold, start, progress.appended_seq, queue_slots,
        replica_replay_queue_sink, replica, &next_seq, &replayed_events);
    atomic_fetch_sub_explicit(&replica->sender_inflight, 1,
                              memory_order_release);
    if (rc == TLC_COLD_RESYNC_REQUIRED) {
        serverLog(LL_NOTICE,
                  "HA Replica AOF retention missing: start=%llu end=%llu; scheduling snapshot resync",
                  (unsigned long long)start,
                  (unsigned long long)progress.appended_seq);
        replica_producer_gate_lock(replica);
        atomic_store_explicit(&replica->replay_mode, false,
                              memory_order_release);
        replica_producer_gate_unlock(replica);
        replica_leader_schedule_resync(replica);
        return 0;
    }
    if (rc != 0 && rc != TLC_HA_REPLICA_REPLAY_QUEUE_FULL)
        return -1;
    if (atomic_load_explicit(&replica->replay_from_seq,
                             memory_order_acquire) != 0)
        return 0;
    if (next_seq <= progress.appended_seq) {
        replica_record_replay_from(replica, next_seq);
        serverLog(LL_NOTICE,
                  "HA Replica AOF replay paused: next=%llu end=%llu queued=%u",
                  (unsigned long long)next_seq,
                  (unsigned long long)progress.appended_seq,
                  replayed_events);
        return 0;
    }
    replica_producer_gate_lock(replica);
    if (atomic_load_explicit(&replica->replay_from_seq,
                             memory_order_acquire) == 0)
        atomic_store_explicit(&replica->replay_mode, false,
                              memory_order_release);
    replica_producer_gate_unlock(replica);
    return 0;
}

int tlc_ha_replica_replay_from(tlc_ha_replica_t *replica,
                               uint64_t start_seq,
                               uint64_t end_seq) {
    RETURN_IF(!replica || replica->role != TLC_HA_REPLICA_LEADER ||
              !replica->cold, -1);
    atomic_store_explicit(&replica->replay_producer_active, true,
                          memory_order_release);
    int rc = tlc_cold_replay_range(replica->cold, start_seq, end_seq,
                                   replica_event_sink, replica);
    atomic_store_explicit(&replica->replay_producer_active, false,
                          memory_order_release);
    return rc;
}

static void *replica_sender_main(void *arg) {
    tlc_ha_replica_t *replica = arg;
    while (!atomic_load_explicit(&replica->stopping, memory_order_acquire)) {
        if (atomic_exchange_explicit(&replica->sender_discard_pending, false,
                                     memory_order_acq_rel))
            replica_discard_sender_queue(replica);
        if (atomic_load_explicit(&replica->resync_emission_gate,
                                 memory_order_acquire)) {
            usleep(1000);
            continue;
        }
        if (atomic_load_explicit(&replica->replay_from_seq,
                                 memory_order_acquire) != 0) {
            replica_producer_gate_lock(replica);
            atomic_store_explicit(&replica->replay_mode, true,
                                  memory_order_release);
            replica_producer_gate_unlock(replica);
        }
        if (atomic_load_explicit(&replica->replay_mode,
                                 memory_order_acquire) &&
            replica_queue_peek(replica) == NULL &&
            !atomic_load_explicit(&replica->replay_producer_active,
                                  memory_order_acquire)) {
            if (replica_replay_pending(replica) != 0) {
                replica_stop_signal(replica);
                break;
            }
            continue;
        }
        tlc_ha_replica_event_t *first = replica_queue_try_pop(replica);
        if (!first) {
            /* Keep the sender alive while the channel is idle.  Replay can
             * enqueue an AOF range after the initial queue drains; exiting
             * here would leave that range permanently unsent. */
            usleep(1000);
            continue;
        }
        if (first->seq < atomic_load_explicit(&replica->normal_min_seq,
                                               memory_order_acquire)) {
            replica_event_free(first);
            continue;
        }
        tlc_ha_replica_event_t **events =
            zcalloc_num(replica->max_batch_events, sizeof(*events));
        uint8_t *payload = zmalloc(replica->max_batch_bytes);
        if (!events || !payload) {
            replica_event_free(first);
            zfree(events);
            zfree(payload);
            replica_stop_signal(replica);
            break;
        }
        uint32_t count = 0;
        size_t payload_size = 0;
        events[count++] = first;
        size_t event_size = 0;
        if (replica_event_wire_size(first, &event_size) != 0 ||
            event_size > replica->max_batch_bytes) {
            replica_stop_signal(replica);
        } else {
            payload_size = event_size;
        }
        while (!atomic_load_explicit(&replica->stopping, memory_order_acquire) &&
               count < replica->max_batch_events) {
            tlc_ha_replica_event_t *next = replica_queue_peek(replica);
            if (!next)
                break;
            size_t next_size = 0;
            int size_rc = replica_event_wire_size(next, &next_size);
            if (size_rc != 0 || next_size > replica->max_batch_bytes ||
                payload_size > replica->max_batch_bytes - next_size) {
                break;
            }
            next = replica_queue_try_pop(replica);
            if (!next)
                break;
            events[count++] = next;
            payload_size += next_size;
        }
        size_t cursor = 0;
        for (uint32_t i = 0; i < count && !atomic_load_explicit(
                 &replica->stopping, memory_order_acquire); i++) {
            tlc_ha_replica_event_t *event = events[i];
            tlc_ha_replica_event_wire_t wire = {
                .ha_term = replica_hton64(event->input.ha_term),
                .topology_epoch = replica_hton64(event->input.topology_epoch),
                .op = htonl(event->input.op),
                .meta_shard_id = htonl(event->input.meta_shard_id),
                .version = replica_hton64(event->input.version),
                .key_len = htonl(event->input.key_len),
                .value_len = htonl(event->input.value_len),
            };
            memcpy(payload + cursor, &wire, sizeof(wire));
            cursor += sizeof(wire);
            memcpy(payload + cursor, event->key, event->input.key_len);
            cursor += event->input.key_len;
            if (event->input.value_len) {
                memcpy(payload + cursor, event->value, event->input.value_len);
                cursor += event->input.value_len;
            }
        }
        atomic_fetch_add_explicit(&replica->sender_inflight, 1,
                                  memory_order_acq_rel);
        if (!atomic_load_explicit(&replica->stopping, memory_order_acquire))
            replica_send_frame(replica, TLC_HA_REPLICA_KIND_EVENTS,
                               first->seq, count, payload, (uint32_t)cursor);
        atomic_fetch_sub_explicit(&replica->sender_inflight, 1,
                                  memory_order_release);
        for (uint32_t i = 0; i < count; i++)
            replica_event_free(events[i]);
        zfree(events);
        zfree(payload);
    }
    return NULL;
}

static int replica_decode_events(tlc_ha_replica_t *replica,
                                 const tlc_ha_replica_frame_wire_t *header,
                                 const uint8_t *payload,
                                 tlc_ha_replica_event_t **events_out) {
    tlc_ha_replica_event_t *events = zcalloc_num(header->event_count,
                                                  sizeof(*events));
    if (!events)
        return -1;
    size_t cursor = 0;
    for (uint32_t i = 0; i < header->event_count; i++) {
        if (header->payload_bytes - cursor < sizeof(tlc_ha_replica_event_wire_t))
            goto failed;
        tlc_ha_replica_event_wire_t wire;
        memcpy(&wire, payload + cursor, sizeof(wire));
        cursor += sizeof(wire);
        uint32_t key_len = ntohl(wire.key_len);
        uint32_t value_len = ntohl(wire.value_len);
        if (key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN ||
            key_len > header->payload_bytes - cursor ||
            value_len > header->payload_bytes - cursor - key_len)
            goto failed;
        tlc_cold_event_input_t input = {
            .ha_term = replica_ntoh64(wire.ha_term),
            .topology_epoch = replica_ntoh64(wire.topology_epoch),
            .op = ntohl(wire.op),
            .meta_shard_id = ntohl(wire.meta_shard_id),
            .version = replica_ntoh64(wire.version),
            .key = payload + cursor,
            .key_len = key_len,
            .value = value_len ? payload + cursor + key_len : NULL,
            .value_len = value_len,
        };
        if (input.op != TLC_COLD_OP_PUT && input.op != TLC_COLD_OP_DEL)
            goto failed;
        events[i].input = input;
        events[i].seq = header->first_seq + i;
        events[i].key = zmalloc(key_len);
        events[i].value = value_len ? zmalloc(value_len) : NULL;
        if (!events[i].key || (value_len && !events[i].value))
            goto failed;
        memcpy(events[i].key, input.key, key_len);
        if (value_len)
            memcpy(events[i].value, input.value, value_len);
        events[i].input.key = events[i].key;
        events[i].input.value = events[i].value;
        cursor += key_len + value_len;
    }
    if (cursor != header->payload_bytes)
        goto failed;
    *events_out = events;
    (void)replica;
    return 0;
failed:
    for (uint32_t i = 0; i < header->event_count; i++) {
        zfree(events[i].key);
        zfree(events[i].value);
    }
    zfree(events);
    return -1;
}

static void replica_decoded_events_free(tlc_ha_replica_event_t *events,
                                         uint32_t count) {
    for (uint32_t i = 0; i < count; i++) {
        zfree(events[i].key);
        zfree(events[i].value);
    }
    zfree(events);
}

static int replica_read_frame(tlc_ha_replica_t *replica,
                              tlc_ha_replica_frame_wire_t *header,
                              uint8_t **payload_out) {
    tlc_ha_replica_frame_wire_t wire;
    if (replica->transport == TLC_HA_REPLICA_TRANSPORT_UB) {
        uint32_t spins = 0;
        int got = 0;
        while (!atomic_load_explicit(&replica->stopping, memory_order_acquire)) {
            got = replica_ring_poll(&replica->rx_ring, replica->rx_frame);
            if (got != 0)
                break;
            replica_queue_wait(&spins);
        }
        if (got != 1)
            return -1;
        memcpy(&wire, replica->rx_frame, sizeof(wire));
    } else if (replica_read_full(replica->fd, &wire, sizeof(wire)) != 0) {
        return -1;
    }
    replica_decode_frame_header(&wire, header);
    if (header->magic != TLC_HA_REPLICA_MAGIC ||
        header->version != TLC_HA_REPLICA_VERSION ||
        header->header_bytes != sizeof(wire) ||
        header->payload_bytes > replica->max_batch_bytes ||
        (replica->transport == TLC_HA_REPLICA_TRANSPORT_UB &&
         header->payload_bytes > replica->rx_ring.config.slot_bytes -
                                  sizeof(wire)) ||
        (header->kind != TLC_HA_REPLICA_KIND_EVENTS &&
         header->kind != TLC_HA_REPLICA_KIND_ACK &&
         header->kind != TLC_HA_REPLICA_KIND_HEARTBEAT &&
         header->kind != TLC_HA_REPLICA_KIND_SNAPSHOT_BEGIN &&
         header->kind != TLC_HA_REPLICA_KIND_SNAPSHOT_CHUNK &&
         header->kind != TLC_HA_REPLICA_KIND_SNAPSHOT_END &&
         header->kind != TLC_HA_REPLICA_KIND_RESYNC_REQUEST &&
         header->kind != TLC_HA_REPLICA_KIND_SNAPSHOT_INSTALLED &&
         header->kind != TLC_HA_REPLICA_KIND_RESYNC_ABORT &&
         header->kind != TLC_HA_REPLICA_KIND_TAIL_REQUEST &&
         header->kind != TLC_HA_REPLICA_KIND_TAIL_END &&
         header->kind != TLC_HA_REPLICA_KIND_RESYNC_ACK &&
         header->kind != TLC_HA_REPLICA_KIND_HANDOFF_COMMIT &&
         header->kind != TLC_HA_REPLICA_KIND_HANDOFF_ACK &&
         header->kind != TLC_HA_REPLICA_KIND_RESYNC_REQUIRED &&
         header->kind != TLC_HA_REPLICA_KIND_TAIL_EVENTS))
        return -1;
    uint8_t *payload = header->payload_bytes ?
        zmalloc(header->payload_bytes) : NULL;
    if (header->payload_bytes && !payload)
        return -1;
    if (header->payload_bytes) {
        if (replica->transport == TLC_HA_REPLICA_TRANSPORT_UB)
            memcpy(payload, replica->rx_frame + sizeof(wire),
                   header->payload_bytes);
        else if (replica_read_full(replica->fd, payload,
                                   header->payload_bytes) != 0) {
            zfree(payload);
            return -1;
        }
    }
    tlc_ha_replica_frame_wire_t checksum_header = wire;
    checksum_header.checksum = 0;
    size_t checksum_bytes = sizeof(checksum_header) + header->payload_bytes;
    uint8_t *checksum_data = zmalloc(checksum_bytes);
    if (!checksum_data) {
        zfree(payload);
        return -1;
    }
    memcpy(checksum_data, &checksum_header, sizeof(checksum_header));
    if (header->payload_bytes)
        memcpy(checksum_data + sizeof(checksum_header), payload,
               header->payload_bytes);
    uint64_t checksum = vemb_v16_xxh3_64(checksum_data, checksum_bytes);
    zfree(checksum_data);
    if (checksum != header->checksum) {
        serverLog(LL_WARNING,
                  "HA Replica frame checksum mismatch: kind=%u first_seq=%llu event_count=%u payload_bytes=%u expected=%llu actual=%llu payload_hash=%llu",
                  header->kind, (unsigned long long)header->first_seq,
                  header->event_count, header->payload_bytes,
                  (unsigned long long)header->checksum,
                  (unsigned long long)checksum,
                  (unsigned long long)vemb_v16_xxh3_64(payload,
                                                      header->payload_bytes));
        zfree(payload);
        return -1;
    }
    *payload_out = payload;
    return 0;
}

/* This runs only recovery control work. Normal EVENTS never wait on it. */
static void replica_follower_schedule_resync(tlc_ha_replica_t *replica,
                                             uint32_t reason) {
    unsigned expected = TLC_HA_RESYNC_SESSION_IDLE;
    if (!atomic_compare_exchange_strong_explicit(
            &replica->resync_session_state, &expected,
            TLC_HA_RESYNC_SESSION_FOLLOWER_WAIT_RESYNC,
            memory_order_acq_rel, memory_order_acquire))
        return;
    tlc_cold_progress_t progress;
    if (tlc_cold_get_progress(replica->cold, &progress) != 0)
        progress = (tlc_cold_progress_t){0};
    /* A COLD GAP has not mutated local durable state. Keep the standby able
     * to accept the missing AOF prefix. Snapshot install alone fences it. */
    if (reason != TLC_HA_RESYNC_REQUIRED_GAP)
        atomic_store_explicit(&replica->resync_fenced, true,
                              memory_order_release);
    atomic_store_explicit(&replica->resync_required_reason, reason,
                          memory_order_release);
    atomic_store_explicit(&replica->resync_required_durable_seq,
                          progress.durable_seq, memory_order_release);
    atomic_store_explicit(&replica->resync_required_applied_seq,
                          atomic_load_explicit(&replica->applied_seq,
                                               memory_order_acquire),
                          memory_order_release);
    atomic_store_explicit(&replica->resync_required_pending, true,
                          memory_order_release);
    atomic_store_explicit(&replica->resync_required_last_sent_ns, 0,
                          memory_order_release);
    serverLog(LL_WARNING,
              "HA Replica Follower recovery requested: reason=%s durable=%llu applied=%llu fenced=%d",
              replica_resync_required_reason_name(reason),
              (unsigned long long)progress.durable_seq,
              (unsigned long long)atomic_load_explicit(
                  &replica->applied_seq, memory_order_acquire),
              atomic_load_explicit(&replica->resync_fenced,
                                   memory_order_acquire));
    replica_resync_touch(replica);
}

static void replica_leader_schedule_resync(tlc_ha_replica_t *replica) {
    unsigned state = atomic_load_explicit(&replica->resync_session_state,
                                          memory_order_acquire);
    if (state != TLC_HA_RESYNC_SESSION_IDLE) {
        serverLog(LL_WARNING,
                  "HA Replica snapshot resync deferred: active_session_state=%u",
                  state);
        return;
    }
    atomic_store_explicit(&replica->resync_emission_gate, true,
                          memory_order_release);
    atomic_store_explicit(&replica->resync_required_pending, true,
                          memory_order_release);
    serverLog(LL_NOTICE,
              "HA Replica snapshot resync scheduled: reason=AOF retention or lineage conflict");
}

/*
 * Low-frequency Leader maintenance for the exact COLD cursor ring. COLD's
 * io_mu stays the file consistency boundary; normal append/send/apply paths
 * neither take a new mutex nor participate in this controller-only state.
 */
static int replica_maintain_aof_retention(tlc_ha_replica_t *replica,
                                          uint64_t now) {
    if (now < replica->retention_next_poll_ns)
        return 0;
    replica->retention_next_poll_ns = now + TLC_HA_REPLICA_RETENTION_POLL_NS;

    tlc_cold_retention_window_t window;
    if (tlc_cold_get_retention_window(replica->cold, &window) != 0)
        return -1;
    if (window.retained_floor_seq == 0 ||
        window.appended_seq < window.retained_floor_seq)
        return 0;
    uint64_t used = window.appended_seq - window.retained_floor_seq + 1u;
    if (used < window.target_events)
        return 0;

    uint64_t hard_threshold = (window.ring_capacity / 100u) * 85u +
        ((window.ring_capacity % 100u) * 85u) / 100u;
    if (used < hard_threshold &&
        now - replica->retention_last_attempt_ns <
            TLC_HA_REPLICA_RETENTION_RETRY_NS)
        return 0;
    unsigned state = atomic_load_explicit(&replica->resync_session_state,
                                          memory_order_acquire);
    if (used >= hard_threshold && replica_resync_state_active(state)) {
        serverLog(LL_WARNING,
                  "HA Replica retention pressure aborts pinned resync: floor=%llu appended=%llu used=%llu target=%llu ring=%llu",
                  (unsigned long long)window.retained_floor_seq,
                  (unsigned long long)window.appended_seq,
                  (unsigned long long)used,
                  (unsigned long long)window.target_events,
                  (unsigned long long)window.ring_capacity);
        replica_abort_resync_internal(replica, 1);
    }

    int need_checkpoint = replica->retention_maintained_floor_seq !=
        window.retained_floor_seq;
    if (!need_checkpoint && used < hard_threshold)
        return 0;

    replica->retention_last_attempt_ns = now;
    tlc_cold_checkpoint_result_t checkpoint;
    int checkpoint_valid = tlc_cold_validate_checkpoint(
        replica->cold, 0, tlc_core_meta_shard_count(replica->core),
        &checkpoint) == 0;
    if (need_checkpoint) {
        uint64_t generation = checkpoint_valid ? checkpoint.generation + 1u : 1u;
        if (generation == 0 || tlc_core_publish_checkpoint(
                replica->core, generation, replica->ha_term, &checkpoint) != 0) {
            serverLog(LL_WARNING,
                      "HA Replica retention checkpoint failed: floor=%llu appended=%llu errno=%d (%s)",
                      (unsigned long long)window.retained_floor_seq,
                      (unsigned long long)window.appended_seq,
                      errno, strerror(errno));
            return -1;
        }
        checkpoint_valid = 1;
        if (tlc_cold_seal_segment(replica->cold) != 0) {
            serverLog(LL_WARNING,
                      "HA Replica retention segment seal failed: checkpoint=%llu errno=%d (%s)",
                      (unsigned long long)checkpoint.checkpoint_seq,
                      errno, strerror(errno));
            return -1;
        }
    }
    if (!checkpoint_valid || tlc_core_compact(replica->core,
                                               checkpoint.checkpoint_seq,
                                               UINT64_MAX, 1) != 0) {
        serverLog(LL_WARNING,
                  "HA Replica retention compact failed: floor=%llu checkpoint=%llu errno=%d (%s)",
                  (unsigned long long)window.retained_floor_seq,
                  (unsigned long long)checkpoint.checkpoint_seq,
                  errno, strerror(errno));
        return -1;
    }
    replica->retention_maintained_floor_seq = window.retained_floor_seq;
    tlc_cold_retention_window_t after = window;
    if (tlc_cold_get_retention_window(replica->cold, &after) == 0 &&
        after.retained_floor_seq != window.retained_floor_seq)
        replica->retention_maintained_floor_seq = after.retained_floor_seq;
    serverLog(LL_NOTICE,
              "HA Replica retention compacted: checkpoint=%llu old_floor=%llu new_floor=%llu used=%llu target=%llu ring=%llu",
              (unsigned long long)checkpoint.checkpoint_seq,
              (unsigned long long)window.retained_floor_seq,
              (unsigned long long)after.retained_floor_seq,
              (unsigned long long)used,
              (unsigned long long)window.target_events,
              (unsigned long long)window.ring_capacity);
    return 0;
}

static void *replica_resync_controller_main(void *arg) {
    tlc_ha_replica_t *replica = arg;
    while (!atomic_load_explicit(&replica->stopping, memory_order_acquire)) {
        if (replica->role == TLC_HA_REPLICA_LEADER) {
            replica_maintain_aof_retention(replica, getMonotonicNs());
            if (atomic_exchange_explicit(&replica->resync_required_pending,
                                         false, memory_order_acq_rel)) {
                uint64_t session_id = atomic_fetch_add_explicit(
                    &replica->next_resync_session_id, 1, memory_order_relaxed);
                if (session_id == 0)
                    session_id = atomic_fetch_add_explicit(
                        &replica->next_resync_session_id, 1,
                        memory_order_relaxed);
                serverLog(LL_NOTICE,
                          "HA Replica snapshot resync begin: session=%llu",
                          (unsigned long long)session_id);
                if (tlc_ha_replica_begin_resync(
                        replica, session_id, replica->configured_topology_epoch,
                        replica->resync_chunk_bytes) != 0) {
                    atomic_store_explicit(&replica->resync_required_pending,
                                          true, memory_order_release);
                    usleep(100000);
                }
            }
        } else {
            unsigned state = atomic_load_explicit(
                &replica->resync_session_state, memory_order_acquire);
            if (state == TLC_HA_RESYNC_SESSION_FOLLOWER_WAIT_INSTALL &&
                atomic_exchange_explicit(&replica->resync_install_pending,
                                         false, memory_order_acq_rel)) {
                tlc_cold_checkpoint_result_t result;
                int install_rc = tlc_ha_replica_install_resync_snapshot(
                    replica, &result);
                if (install_rc != 0) {
                    serverLog(LL_WARNING,
                              "HA Replica snapshot install failed: session=%llu rc=%d errno=%d (%s)",
                              (unsigned long long)replica->resync_session_id,
                              install_rc, errno, strerror(errno));
                    replica_abort_resync_internal(replica, 1);
                    replica_follower_schedule_resync(
                        replica, TLC_HA_RESYNC_REQUIRED_CONFLICT);
                } else {
                    serverLog(LL_NOTICE,
                              "HA Replica snapshot installed: session=%llu checkpoint=%llu",
                              (unsigned long long)replica->resync_session_id,
                              (unsigned long long)result.checkpoint_seq);
                }
                continue;
            }
            if (state == TLC_HA_RESYNC_SESSION_FOLLOWER_WAIT_RESYNC &&
                atomic_load_explicit(&replica->resync_required_pending,
                                     memory_order_acquire)) {
                uint64_t now = getMonotonicNs();
                uint64_t last = atomic_load_explicit(
                    &replica->resync_required_last_sent_ns,
                    memory_order_acquire);
                if (last == 0 || now - last >=
                                     TLC_HA_REPLICA_RESYNC_REQUIRED_RETRY_NS) {
                    tlc_ha_resync_required_t required = {
                        .ha_term = replica->ha_term,
                        .topology_epoch = replica->configured_topology_epoch,
                        .durable_seq = atomic_load_explicit(
                            &replica->resync_required_durable_seq,
                            memory_order_acquire),
                        .applied_seq = atomic_load_explicit(
                            &replica->resync_required_applied_seq,
                            memory_order_acquire),
                        .reason = atomic_load_explicit(
                            &replica->resync_required_reason,
                            memory_order_acquire),
                    };
                    if (replica_send_resync_required(replica, &required) == 0) {
                        atomic_store_explicit(
                            &replica->resync_required_last_sent_ns, now,
                            memory_order_release);
                        replica_resync_touch(replica);
                    }
                }
            }
        }
        usleep(1000);
    }
    return NULL;
}

static void *replica_apply_main(void *arg) {
    tlc_ha_replica_t *replica = arg;
    for (;;) {
        tlc_ha_replica_event_t *event = replica_queue_pop(replica);
        if (!event) {
            /* The receiver may start before the first replay frame arrives.
             * Keep the single apply worker alive until shutdown instead of
             * treating an initially empty queue as end-of-stream. */
            if (atomic_load_explicit(&replica->stopping,
                                     memory_order_acquire))
                break;
            usleep(1000);
            continue;
        }
        if (replica_follower_work_enter(replica, &replica->apply_inflight) != 0) {
            replica_event_free(event);
            continue;
        }
        tlc_core_replica_apply_status_t status;
        int rc = tlc_core_apply_replica_event(replica->core, &event->input,
                                              event->seq, &status);
        uint64_t event_seq = event->seq;
        if (rc == 0 && (status == TLC_CORE_REPLICA_APPLY_APPLIED ||
                        status == TLC_CORE_REPLICA_APPLY_DUPLICATE))
            replica_atomic_advance(&replica->applied_seq, event_seq);
        replica_event_free(event);
        replica_follower_work_leave(&replica->apply_inflight);
        if (status == TLC_CORE_REPLICA_APPLY_GAP ||
            status == TLC_CORE_REPLICA_APPLY_STALE) {
            serverLog(LL_WARNING,
                      "HA Replica Follower WARM %s: seq=%llu applied=%llu",
                      status == TLC_CORE_REPLICA_APPLY_GAP ? "GAP" : "STALE",
                      (unsigned long long)event_seq,
                      (unsigned long long)atomic_load_explicit(
                          &replica->applied_seq, memory_order_acquire));
            replica_follower_schedule_resync(
                replica, status == TLC_CORE_REPLICA_APPLY_GAP ?
                TLC_HA_RESYNC_REQUIRED_GAP : TLC_HA_RESYNC_REQUIRED_CONFLICT);
            continue;
        }
        if (rc != 0 || status == TLC_CORE_REPLICA_APPLY_ERROR) {
            replica_stop_signal(replica);
            break;
        }
    }
    return NULL;
}

static int replica_handle_heartbeat(tlc_ha_replica_t *replica,
                                    const tlc_ha_replica_frame_wire_t *header,
                                    const uint8_t *payload) {
    if (header->payload_bytes != sizeof(tlc_ha_replica_heartbeat_t) ||
        header->event_count != 0 || header->first_seq != 0)
        return -1;
    tlc_ha_replica_heartbeat_t heartbeat;
    tlc_ha_replica_heartbeat_t wire;
    memcpy(&wire, payload, sizeof(wire));
    replica_decode_heartbeat(&wire, &heartbeat);
    if (heartbeat.hpc_node_id != replica->peer_node_id ||
        heartbeat.peer_node_id != replica->hpc_node_id) {
        serverLog(LL_WARNING,
                  "HA Replica heartbeat identity mismatch: local=%llu peer=%llu/%llu",
                  (unsigned long long)replica->hpc_node_id,
                  (unsigned long long)heartbeat.hpc_node_id,
                  (unsigned long long)heartbeat.peer_node_id);
        return -1;
    }
    if (heartbeat.role != (replica->role == TLC_HA_REPLICA_LEADER ?
                           TLC_HA_REPLICA_FOLLOWER :
                           TLC_HA_REPLICA_LEADER)) {
        serverLog(LL_WARNING, "HA Replica heartbeat role mismatch: role=%u",
                  heartbeat.role);
        return -1;
    }
    if (heartbeat.health < TLC_HA_REPLICA_HEALTHY ||
        heartbeat.health > TLC_HA_REPLICA_UNAVAILABLE) {
        serverLog(LL_WARNING, "HA Replica heartbeat health invalid: health=%u",
                  heartbeat.health);
        return -1;
    }
    if (heartbeat.ha_term < replica->ha_term) {
        serverLog(LL_WARNING,
                  "HA Replica heartbeat term is stale: local=%llu peer=%llu",
                  (unsigned long long)replica->ha_term,
                  (unsigned long long)heartbeat.ha_term);
        return -1;
    }
    uint64_t previous_term = atomic_load_explicit(&replica->peer_ha_term,
                                                   memory_order_acquire);
    if (heartbeat.ha_term < previous_term) {
        serverLog(LL_WARNING,
                  "HA Replica heartbeat term regressed: previous=%llu peer=%llu",
                  (unsigned long long)previous_term,
                  (unsigned long long)heartbeat.ha_term);
        return -1;
    }
    atomic_store_explicit(&replica->peer_ha_term, heartbeat.ha_term,
                          memory_order_release);
    atomic_store_explicit(&replica->last_heartbeat_received_ns,
                          getMonotonicNs(), memory_order_release);
    atomic_store_explicit(&replica->peer_health, heartbeat.health,
                          memory_order_release);
    uint64_t old_durable = atomic_load_explicit(&replica->peer_durable_seq,
                                                memory_order_relaxed);
    while (heartbeat.durable_seq > old_durable &&
           !atomic_compare_exchange_weak_explicit(
               &replica->peer_durable_seq, &old_durable,
               heartbeat.durable_seq, memory_order_release,
               memory_order_relaxed)) {
    }
    uint64_t old_progress = atomic_load_explicit(&replica->peer_progress_seq,
                                                 memory_order_relaxed);
    while (heartbeat.progress_seq > old_progress &&
           !atomic_compare_exchange_weak_explicit(
               &replica->peer_progress_seq, &old_progress,
               heartbeat.progress_seq, memory_order_release,
               memory_order_relaxed)) {
    }
    return 0;
}

static int replica_handle_resync_snapshot(
    tlc_ha_replica_t *replica,
    const tlc_ha_replica_frame_wire_t *header,
    const uint8_t *payload) {
    if (replica->role != TLC_HA_REPLICA_FOLLOWER ||
        header->first_seq != 0 || header->event_count != 0)
        return -1;
    if (header->kind == TLC_HA_REPLICA_KIND_SNAPSHOT_BEGIN) {
        tlc_ha_resync_snapshot_begin_t begin;
        if (resync_decode_begin(payload, header->payload_bytes, &begin) != 0)
            return -1;
        if (atomic_load_explicit(&replica->resync_session_state,
                                 memory_order_acquire) ==
                TLC_HA_RESYNC_SESSION_FOLLOWER_WAIT_INSTALL &&
            (begin.session_id != replica->resync_session_id ||
             begin.generation != replica->resync_generation ||
             begin.checkpoint_seq != replica->resync_checkpoint_seq ||
             begin.durable_boundary_seq != replica->resync_boundary_seq))
            return -1;
        return tlc_ha_resync_assembler_begin(replica->resync_assembler, &begin);
    }
    if (header->kind == TLC_HA_REPLICA_KIND_SNAPSHOT_CHUNK) {
        tlc_ha_resync_snapshot_chunk_t chunk;
        const uint8_t *data = NULL;
        return resync_decode_chunk_header(payload, header->payload_bytes,
                                          &chunk, &data) == 0 ?
            tlc_ha_resync_assembler_append(replica->resync_assembler, &chunk,
                                            data) : -1;
    }
    if (header->kind == TLC_HA_REPLICA_KIND_SNAPSHOT_END) {
        tlc_ha_resync_snapshot_end_t end;
        if (resync_decode_end(payload, header->payload_bytes, &end) != 0)
            return -1;
        char path[PATH_MAX];
        size_t blob_bytes = 0;
        tlc_ha_resync_snapshot_begin_t begin;
        if (tlc_ha_resync_assembler_finish(replica->resync_assembler, &end,
                                            path, sizeof(path), &blob_bytes,
                                            &begin) != 0)
            return -1;
        tlc_ha_resync_artifact_t *artifact = zmalloc(sizeof(*artifact));
        if (!artifact)
            return -1;
        *artifact = (tlc_ha_resync_artifact_t){
            .blob_bytes = blob_bytes,
            .begin = begin,
        };
        memcpy(artifact->path, path, strlen(path) + 1u);
        if (replica_publish_resync_artifact(replica, artifact) == 0) {
            atomic_store_explicit(&replica->resync_install_pending, true,
                                  memory_order_release);
            serverLog(LL_NOTICE,
                      "HA Replica snapshot artifact received: session=%llu bytes=%zu",
                      (unsigned long long)begin.session_id, blob_bytes);
            return 0;
        }
        resync_artifact_delete(artifact);
        return -1;
    }
    return -1;
}

static void replica_discard_sender_queue(tlc_ha_replica_t *replica) {
    for (;;) {
        tlc_ha_replica_event_t *event = replica_queue_try_pop(replica);
        if (!event)
            return;
        replica_event_free(event);
    }
}

static void replica_reset_resync_artifact(tlc_ha_replica_t *replica) {
    tlc_ha_resync_assembler_t *assembler = replica->resync_assembler;
    if (assembler) {
        resync_assembler_discard_part(assembler);
        if (assembler->blob_path[0] != '\0')
            unlink(assembler->blob_path);
        if (assembler->session_directory[0] != '\0')
            rmdir(assembler->session_directory);
        assembler->received_bytes = 0;
        assembler->stage = TLC_HA_RESYNC_IDLE;
        assembler->reason = TLC_HA_RESYNC_REASON_NONE;
    }
    resync_artifact_delete((tlc_ha_resync_artifact_t *)
        atomic_exchange_explicit(&replica->received_snapshot_artifact,
                                 (uintptr_t)NULL, memory_order_acquire));
    atomic_store_explicit(&replica->resync_install_pending, false,
                          memory_order_release);
}

/* Only the winner that changes active -> ABORTED releases session resources. */
static int replica_abort_resync_internal(tlc_ha_replica_t *replica,
                                         int notify_peer) {
    unsigned state = atomic_load_explicit(&replica->resync_session_state,
                                          memory_order_acquire);
    while (replica_resync_state_active(state) &&
           !atomic_compare_exchange_weak_explicit(
               &replica->resync_session_state, &state,
               TLC_HA_RESYNC_SESSION_ABORTED, memory_order_acq_rel,
               memory_order_acquire)) {
    }
    if (!replica_resync_state_active(state))
        return 0;

    tlc_ha_resync_control_t abort = {
        .session_id = replica->resync_session_id,
        .generation = replica->resync_generation,
        .checkpoint_seq = replica->resync_checkpoint_seq,
        .durable_seq = replica->resync_boundary_seq,
        .durable_boundary_seq = replica->resync_boundary_seq,
    };
    if (replica->role == TLC_HA_REPLICA_LEADER)
        atomic_store_explicit(&replica->resync_emission_gate, true,
                              memory_order_release);
    else
        atomic_store_explicit(&replica->resync_fenced, true,
                              memory_order_release);
    int notify_rc = notify_peer ?
        replica_send_resync_control(replica, TLC_HA_REPLICA_KIND_RESYNC_ABORT,
                                    &abort) : 0;
    if (replica->role == TLC_HA_REPLICA_LEADER) {
        if (replica->leader_resync_snapshot.retention_pin_token != 0)
            tlc_cold_end_resync_snapshot(replica->cold,
                                         &replica->leader_resync_snapshot);
        atomic_store_explicit(&replica->sender_discard_pending, true,
                              memory_order_release);
    } else {
        replica_reset_resync_artifact(replica);
        zfree(replica->resync_captured_seq);
        replica->resync_captured_seq = NULL;
    }
    atomic_store_explicit(&replica->resync_session_state,
                          TLC_HA_RESYNC_SESSION_IDLE,
                          memory_order_release);
    serverLog(LL_NOTICE, "HA resync aborted: role=%u session=%llu reason=%s",
              replica->role, (unsigned long long)abort.session_id,
              notify_peer ? "local" : "peer");
    return notify_rc;
}

int tlc_ha_replica_abort_resync(tlc_ha_replica_t *replica) {
    RETURN_IF(!replica, -1);
    return replica_abort_resync_internal(replica, 1);
}

static int replica_handle_resync_tail_events(
    tlc_ha_replica_t *replica,
    const tlc_ha_replica_frame_wire_t *header,
    const uint8_t *payload) {
    unsigned state = atomic_load_explicit(&replica->resync_session_state,
                                          memory_order_acquire);
    if (replica->role != TLC_HA_REPLICA_FOLLOWER ||
        (state != TLC_HA_RESYNC_SESSION_FOLLOWER_WAIT_TAIL &&
         state != TLC_HA_RESYNC_SESSION_FOLLOWER_WAIT_COMMIT) ||
        header->first_seq != replica->resync_next_seq ||
        header->event_count == 0 ||
        header->event_count > replica->max_batch_events ||
        header->first_seq > UINT64_MAX - header->event_count ||
        !replica->resync_captured_seq)
        return -1;
    tlc_ha_replica_event_t *events = NULL;
    if (replica_decode_events(replica, header, payload, &events) != 0)
        return -1;
    tlc_cold_event_input_t *inputs = zcalloc_num(header->event_count,
                                                  sizeof(*inputs));
    if (!inputs) {
        replica_decoded_events_free(events, header->event_count);
        return -1;
    }
    for (uint32_t i = 0; i < header->event_count; i++)
        inputs[i] = events[i].input;
    tlc_cold_replica_batch_status_t batch_status;
    uint64_t durable_seq = 0;
    int rc = tlc_cold_submit_replica_batch(replica->cold, header->first_seq,
                                           inputs, header->event_count,
                                           TLC_COLD_ACK_DURABLE,
                                           &batch_status, &durable_seq);
    zfree(inputs);
    if (rc == 0 && (batch_status == TLC_COLD_REPLICA_BATCH_APPLIED ||
                    batch_status == TLC_COLD_REPLICA_BATCH_DUPLICATE)) {
        for (uint32_t i = 0; i < header->event_count && rc == 0; i++) {
            tlc_core_replica_apply_status_t status = TLC_CORE_REPLICA_APPLY_ERROR;
            rc = tlc_core_apply_resync_event(replica->core, &events[i].input,
                                             events[i].seq,
                                             replica->resync_captured_seq,
                                             replica->resync_meta_shard_count,
                                             &status);
        }
    }
    replica_decoded_events_free(events, header->event_count);
    if (rc != 0 || durable_seq != header->first_seq + header->event_count - 1u)
        return -1;
    replica->resync_next_seq = durable_seq + 1u;
    atomic_store_explicit(&replica->applied_seq, durable_seq,
                          memory_order_release);
    serverLog(LL_NOTICE,
              "HA Replica snapshot tail applied: session=%llu start=%llu end=%llu",
              (unsigned long long)replica->resync_session_id,
              (unsigned long long)header->first_seq,
              (unsigned long long)durable_seq);
    return 0;
}

static int replica_handle_resync_control(
    tlc_ha_replica_t *replica,
    const tlc_ha_replica_frame_wire_t *header,
    const uint8_t *payload) {
    if (header->first_seq != 0 || header->event_count != 0)
        return -1;
    if (header->kind == TLC_HA_REPLICA_KIND_RESYNC_REQUIRED) {
        tlc_ha_resync_required_t required;
        if (resync_decode_required(payload, header->payload_bytes,
                                   &required) != 0 ||
            replica->role != TLC_HA_REPLICA_LEADER ||
            required.ha_term != replica->ha_term ||
            required.topology_epoch != replica->configured_topology_epoch)
            return -1;
        serverLog(LL_WARNING,
                  "HA Replica Leader received resync request: reason=%s durable=%llu applied=%llu",
                  replica_resync_required_reason_name(required.reason),
                  (unsigned long long)required.durable_seq,
                  (unsigned long long)required.applied_seq);
        if (atomic_load_explicit(&replica->resync_session_state,
                                 memory_order_acquire) !=
            TLC_HA_RESYNC_SESSION_IDLE)
            return 0;
        if (required.reason == TLC_HA_RESYNC_REQUIRED_GAP) {
            tlc_cold_progress_t progress;
            if (tlc_cold_get_progress(replica->cold, &progress) != 0 ||
                required.durable_seq == UINT64_MAX ||
                required.durable_seq >= progress.appended_seq) {
                serverLog(LL_WARNING,
                          "HA Replica GAP cannot use AOF repair: follower_durable=%llu leader_appended=%llu; scheduling snapshot resync",
                          (unsigned long long)required.durable_seq,
                          (unsigned long long)progress.appended_seq);
                replica_leader_schedule_resync(replica);
                return 0;
            }
            uint64_t replay_start = required.durable_seq + 1u;
            replica_record_replay_from(replica, replay_start);
            serverLog(LL_NOTICE,
                      "HA Replica AOF repair scheduled: reason=gap start=%llu end=%llu follower_durable=%llu",
                      (unsigned long long)replay_start,
                      (unsigned long long)progress.appended_seq,
                      (unsigned long long)required.durable_seq);
            return 0;
        }
        serverLog(LL_NOTICE,
                  "HA Replica snapshot resync selected: reason=%s",
                  replica_resync_required_reason_name(required.reason));
        replica_leader_schedule_resync(replica);
        return 0;
    }
    tlc_ha_resync_control_t control;
    if (resync_decode_control(payload, header->payload_bytes, &control) != 0)
        return -1;
    unsigned state = atomic_load_explicit(&replica->resync_session_state,
                                          memory_order_acquire);
    if (header->kind == TLC_HA_REPLICA_KIND_RESYNC_REQUEST) {
        if (replica->role != TLC_HA_REPLICA_FOLLOWER ||
            (state != TLC_HA_RESYNC_SESSION_IDLE &&
             state != TLC_HA_RESYNC_SESSION_FOLLOWER_WAIT_RESYNC) ||
            control.session_id == 0 ||
            control.generation == 0 || control.checkpoint_seq == UINT64_MAX)
            return -1;
        replica->resync_session_id = control.session_id;
        replica->resync_generation = control.generation;
        replica->resync_checkpoint_seq = control.checkpoint_seq;
        replica->resync_boundary_seq = control.durable_boundary_seq;
        atomic_store_explicit(&replica->resync_fenced, true,
                              memory_order_release);
        atomic_store_explicit(&replica->resync_required_pending, false,
                              memory_order_release);
        atomic_store_explicit(&replica->resync_session_state,
                              TLC_HA_RESYNC_SESSION_FOLLOWER_WAIT_INSTALL,
                              memory_order_release);
        return 0;
    }
    if (header->kind == TLC_HA_REPLICA_KIND_SNAPSHOT_INSTALLED) {
        if (replica->role != TLC_HA_REPLICA_LEADER ||
            state != TLC_HA_RESYNC_SESSION_LEADER_WAIT_INSTALLED ||
            !replica_resync_session_matches(replica, &control))
            return -1;
        atomic_store_explicit(&replica->resync_session_state,
                              TLC_HA_RESYNC_SESSION_LEADER_WAIT_REQUEST,
                              memory_order_release);
        serverLog(LL_NOTICE,
                  "HA Replica snapshot install acknowledged: session=%llu",
                  (unsigned long long)control.session_id);
        return 0;
    }
    if (header->kind == TLC_HA_REPLICA_KIND_TAIL_REQUEST) {
        if (replica->role == TLC_HA_REPLICA_LEADER &&
            (state == TLC_HA_RESYNC_SESSION_LEADER_WAIT_HANDOFF_ACK ||
             state == TLC_HA_RESYNC_SESSION_IDLE) &&
            replica_resync_session_matches(replica, &control) &&
            control.durable_seq == replica->resync_boundary_seq + 1u)
            return 0;
        if (replica->role != TLC_HA_REPLICA_LEADER ||
            state != TLC_HA_RESYNC_SESSION_LEADER_WAIT_REQUEST ||
            !replica_resync_session_matches(replica, &control) ||
            control.durable_seq != replica->resync_next_seq)
            return state == TLC_HA_RESYNC_SESSION_COMPLETE ? 0 : -1;
        serverLog(LL_NOTICE,
                  "HA Replica snapshot tail requested: session=%llu next=%llu boundary=%llu",
                  (unsigned long long)control.session_id,
                  (unsigned long long)control.durable_seq,
                  (unsigned long long)replica->resync_boundary_seq);
        return replica_send_resync_tail(replica, control.durable_seq);
    }
    if (header->kind == TLC_HA_REPLICA_KIND_TAIL_END) {
        if (replica->role != TLC_HA_REPLICA_FOLLOWER ||
            (state != TLC_HA_RESYNC_SESSION_FOLLOWER_WAIT_TAIL &&
             state != TLC_HA_RESYNC_SESSION_FOLLOWER_WAIT_COMMIT) ||
            !replica_resync_session_matches(replica, &control) ||
            control.durable_boundary_seq != control.durable_seq ||
            control.durable_seq < replica->resync_boundary_seq ||
            replica->resync_next_seq != control.durable_seq + 1u)
            return -1;
        tlc_cold_progress_t progress;
        if (tlc_cold_get_progress(replica->cold, &progress) != 0 ||
            progress.durable_seq != control.durable_seq)
            return -1;
        uint64_t applied = atomic_load_explicit(&replica->applied_seq,
                                                memory_order_acquire);
        if (applied != control.durable_seq)
            return -1;
        replica->resync_boundary_seq = control.durable_boundary_seq;
        atomic_store_explicit(&replica->resync_session_state,
                              TLC_HA_RESYNC_SESSION_FOLLOWER_WAIT_COMMIT,
                              memory_order_release);
        tlc_ha_resync_control_t ack = control;
        ack.applied_seq = applied;
        if (replica_send_resync_control(replica, TLC_HA_REPLICA_KIND_RESYNC_ACK,
                                        &ack) != 0)
            return -1;
        ack.durable_seq = control.durable_seq + 1u;
        ack.applied_seq = 0;
        return replica_send_resync_control(replica,
                                           TLC_HA_REPLICA_KIND_TAIL_REQUEST,
                                           &ack);
    }
    if (header->kind == TLC_HA_REPLICA_KIND_RESYNC_ACK) {
        if (replica->role != TLC_HA_REPLICA_LEADER ||
            state != TLC_HA_RESYNC_SESSION_LEADER_WAIT_ACK ||
            !replica_resync_session_matches(replica, &control) ||
            control.durable_seq != replica->resync_boundary_seq ||
            control.applied_seq != replica->resync_boundary_seq)
            return -1;
        uint64_t peer_accepted = atomic_load_explicit(
            &replica->peer_accepted_seq, memory_order_relaxed);
        while (control.durable_seq > peer_accepted &&
               !atomic_compare_exchange_weak_explicit(
                   &replica->peer_accepted_seq, &peer_accepted,
                   control.durable_seq, memory_order_release,
                   memory_order_relaxed)) {
        }
        uint64_t peer_durable = atomic_load_explicit(
            &replica->peer_durable_seq, memory_order_relaxed);
        while (control.durable_seq > peer_durable &&
               !atomic_compare_exchange_weak_explicit(
                   &replica->peer_durable_seq, &peer_durable,
                   control.durable_seq, memory_order_release,
                   memory_order_relaxed)) {
        }
        uint64_t peer_applied = atomic_load_explicit(
            &replica->peer_progress_seq, memory_order_relaxed);
        while (control.applied_seq > peer_applied &&
               !atomic_compare_exchange_weak_explicit(
                   &replica->peer_progress_seq, &peer_applied,
                   control.applied_seq, memory_order_release,
                   memory_order_relaxed)) {
        }
        atomic_store_explicit(&replica->resync_emission_gate, true,
                              memory_order_release);
        while (atomic_load_explicit(&replica->sender_inflight,
                                    memory_order_acquire) != 0)
            usleep(1000);
        tlc_cold_progress_t progress;
        if (tlc_cold_get_progress(replica->cold, &progress) != 0)
            return -1;
        if (progress.durable_seq > replica->resync_boundary_seq) {
            if (tlc_cold_extend_resync_snapshot(
                    replica->cold, &replica->leader_resync_snapshot,
                    progress.durable_seq) != 0)
                return -1;
            replica->resync_boundary_seq = progress.durable_seq;
            atomic_store_explicit(&replica->resync_session_state,
                                  TLC_HA_RESYNC_SESSION_LEADER_WAIT_REQUEST,
                                  memory_order_release);
            return 0;
        }
        tlc_ha_resync_control_t commit = control;
        atomic_store_explicit(&replica->resync_session_state,
                              TLC_HA_RESYNC_SESSION_LEADER_WAIT_HANDOFF_ACK,
                              memory_order_release);
        serverLog(LL_NOTICE, "HA resync handoff commit: session=%llu H=%llu",
                  (unsigned long long)control.session_id,
                  (unsigned long long)control.durable_seq);
        if (replica_send_resync_control(replica,
                                        TLC_HA_REPLICA_KIND_HANDOFF_COMMIT,
                                        &commit) != 0)
            return -1;
        return 0;
    }
    if (header->kind == TLC_HA_REPLICA_KIND_HANDOFF_COMMIT) {
        if (replica->role != TLC_HA_REPLICA_FOLLOWER ||
            state != TLC_HA_RESYNC_SESSION_FOLLOWER_WAIT_COMMIT ||
            !replica_resync_session_matches(replica, &control) ||
            control.durable_seq != replica->resync_boundary_seq)
            return -1;
        zfree(replica->resync_captured_seq);
        replica->resync_captured_seq = NULL;
        atomic_store_explicit(&replica->resync_session_state,
                              TLC_HA_RESYNC_SESSION_IDLE,
                              memory_order_release);
        atomic_store_explicit(&replica->resync_fenced, false,
                              memory_order_release);
        serverLog(LL_NOTICE, "HA resync handoff applied: session=%llu H=%llu",
                  (unsigned long long)control.session_id,
                  (unsigned long long)control.durable_seq);
        return replica_send_resync_control(replica,
                                           TLC_HA_REPLICA_KIND_HANDOFF_ACK,
                                           &control);
    }
    if (header->kind == TLC_HA_REPLICA_KIND_HANDOFF_ACK) {
        if (replica->role != TLC_HA_REPLICA_LEADER ||
            state != TLC_HA_RESYNC_SESSION_LEADER_WAIT_HANDOFF_ACK ||
            !replica_resync_session_matches(replica, &control) ||
            control.durable_seq != replica->resync_boundary_seq ||
            control.applied_seq != replica->resync_boundary_seq)
            return -1;
        tlc_cold_end_resync_snapshot(replica->cold,
                                     &replica->leader_resync_snapshot);
        /* Only actual COLD appends while the final gate was closed need a
         * normal replay. A synthetic H + 1 would make an idle handoff look
         * like a missing AOF record and can strand the sender in replay mode. */
        uint64_t replay_from = atomic_exchange_explicit(
            &replica->replay_from_seq, 0, memory_order_acq_rel);
        if (replay_from > replica->resync_boundary_seq)
            replica_record_replay_from(replica, replay_from);
        atomic_store_explicit(&replica->normal_min_seq,
                              replica->resync_boundary_seq + 1u,
                              memory_order_release);
        atomic_store_explicit(&replica->resync_session_state,
                              TLC_HA_RESYNC_SESSION_IDLE,
                              memory_order_release);
        atomic_store_explicit(&replica->resync_emission_gate, false,
                              memory_order_release);
        serverLog(LL_NOTICE, "HA resync handoff complete: session=%llu H=%llu",
                  (unsigned long long)control.session_id,
                  (unsigned long long)control.durable_seq);
        return 0;
    }
    if (header->kind == TLC_HA_REPLICA_KIND_RESYNC_ABORT) {
        if (!replica_resync_state_active(state) ||
            !replica_resync_session_matches(replica, &control))
            return 0;
        return replica_abort_resync_internal(replica, 0);
    }
    return -1;
}

static void *replica_heartbeat_main(void *arg) {
    tlc_ha_replica_t *replica = arg;
    uint32_t interval_ms = replica->heartbeat_interval_ms;
    struct timespec pause = {
        .tv_sec = interval_ms / 1000u,
        .tv_nsec = (long)(interval_ms % 1000u) * 1000000L,
    };
    while (!atomic_load_explicit(&replica->stopping, memory_order_acquire)) {
        nanosleep(&pause, NULL);
        if (atomic_load_explicit(&replica->stopping, memory_order_acquire))
            break;
        if (replica_send_heartbeat(replica) != 0)
            atomic_store_explicit(&replica->peer_health,
                                  TLC_HA_REPLICA_UNAVAILABLE,
                                  memory_order_release);
        replica_log_progress(replica);
        uint64_t last = atomic_load_explicit(
            &replica->last_heartbeat_received_ns, memory_order_acquire);
        if (last != 0 && elapsedNs(last) >=
                         (uint64_t)replica->heartbeat_timeout_ms * 1000000u)
            atomic_store_explicit(&replica->peer_health,
                                  TLC_HA_REPLICA_UNAVAILABLE,
                                  memory_order_release);
        unsigned state = atomic_load_explicit(&replica->resync_session_state,
                                              memory_order_acquire);
        uint64_t last_resync = atomic_load_explicit(
            &replica->resync_last_progress_ns, memory_order_acquire);
        if (replica_resync_state_active(state) && last_resync != 0 &&
            elapsedNs(last_resync) >=
                (uint64_t)replica->resync_timeout_ms * 1000000u)
            replica_abort_resync_internal(replica, 1);
    }
    return NULL;
}

/*
 * Replica listener mirrors UB RPC listener_main's batch poll/dispatch loop,
 * but consumes only the dedicated Replica channel and never uses RPC pending
 * request state. On a Leader it dispatches ACK frames; on a Follower it
 * dispatches EVENTS frames before enqueueing them for asynchronous apply.
 */
static void *replica_listener_main(void *arg) {
    tlc_ha_replica_t *replica = arg;
    while (!atomic_load_explicit(&replica->stopping, memory_order_acquire)) {
        tlc_ha_replica_frame_wire_t header;
        uint8_t *payload = NULL;
        if (replica_read_frame(replica, &header, &payload) != 0) {
            replica_stop_signal(replica);
            break;
        }
        if (header.kind == TLC_HA_REPLICA_KIND_HEARTBEAT) {
            int heartbeat_rc = replica_handle_heartbeat(replica, &header,
                                                        payload);
            zfree(payload);
            if (heartbeat_rc != 0) {
                replica_stop_signal(replica);
                break;
            }
            continue;
        }
        if (header.kind == TLC_HA_REPLICA_KIND_SNAPSHOT_BEGIN ||
            header.kind == TLC_HA_REPLICA_KIND_SNAPSHOT_CHUNK ||
            header.kind == TLC_HA_REPLICA_KIND_SNAPSHOT_END) {
            int resync_rc = replica_handle_resync_snapshot(replica, &header,
                                                            payload);
            zfree(payload);
            if (resync_rc != 0) {
                replica_stop_signal(replica);
                break;
            }
            replica_resync_touch(replica);
            continue;
        }
        if (header.kind == TLC_HA_REPLICA_KIND_RESYNC_REQUEST ||
            header.kind == TLC_HA_REPLICA_KIND_SNAPSHOT_INSTALLED ||
            header.kind == TLC_HA_REPLICA_KIND_RESYNC_ABORT ||
            header.kind == TLC_HA_REPLICA_KIND_RESYNC_REQUIRED ||
            header.kind == TLC_HA_REPLICA_KIND_TAIL_REQUEST ||
            header.kind == TLC_HA_REPLICA_KIND_TAIL_END ||
            header.kind == TLC_HA_REPLICA_KIND_RESYNC_ACK ||
            header.kind == TLC_HA_REPLICA_KIND_HANDOFF_COMMIT ||
            header.kind == TLC_HA_REPLICA_KIND_HANDOFF_ACK) {
            int control_rc = replica_handle_resync_control(replica, &header,
                                                            payload);
            zfree(payload);
            if (control_rc != 0) {
                serverLog(LL_WARNING,
                          "HA Replica resync control rejected: role=%u kind=%u state=%u",
                          replica->role, header.kind,
                          atomic_load_explicit(&replica->resync_session_state,
                                               memory_order_acquire));
                replica_stop_signal(replica);
                break;
            }
            replica_resync_touch(replica);
            continue;
        }
        if (header.kind == TLC_HA_REPLICA_KIND_TAIL_EVENTS) {
            int tail_rc = replica_handle_resync_tail_events(replica, &header,
                                                             payload);
            zfree(payload);
            if (tail_rc != 0) {
                serverLog(LL_WARNING,
                          "HA Replica snapshot tail rejected: role=%u state=%u start=%llu count=%u",
                          replica->role,
                          atomic_load_explicit(&replica->resync_session_state,
                                               memory_order_acquire),
                          (unsigned long long)header.first_seq,
                          header.event_count);
                replica_stop_signal(replica);
                break;
            }
            replica_resync_touch(replica);
            continue;
        }
        if (replica->role == TLC_HA_REPLICA_LEADER) {
            if (header.kind != TLC_HA_REPLICA_KIND_ACK ||
                header.payload_bytes != sizeof(uint64_t)) {
                zfree(payload);
                replica_stop_signal(replica);
                break;
            }
            uint64_t accepted_seq;
            memcpy(&accepted_seq, payload, sizeof(accepted_seq));
            accepted_seq = replica_ntoh64(accepted_seq);
            uint64_t old_accepted = atomic_load_explicit(
                &replica->peer_accepted_seq, memory_order_relaxed);
            while (accepted_seq > old_accepted &&
                   !atomic_compare_exchange_weak_explicit(
                       &replica->peer_accepted_seq, &old_accepted, accepted_seq,
                       memory_order_release, memory_order_relaxed)) {
            }
            zfree(payload);
            continue;
        }
        if (header.kind != TLC_HA_REPLICA_KIND_EVENTS ||
            header.event_count == 0 ||
            header.event_count > replica->max_batch_events ||
            header.first_seq == 0 ||
            header.first_seq > UINT64_MAX - header.event_count) {
            zfree(payload);
            replica_stop_signal(replica);
            break;
        }
        if (replica_follower_work_enter(replica,
                                        &replica->ingress_inflight) != 0) {
            zfree(payload);
            continue;
        }
        tlc_ha_replica_event_t *events = NULL;
        if (replica_decode_events(replica, &header, payload, &events) != 0) {
            zfree(payload);
            replica_follower_work_leave(&replica->ingress_inflight);
            replica_stop_signal(replica);
            break;
        }
        tlc_cold_event_input_t *inputs =
            zcalloc_num(header.event_count, sizeof(*inputs));
        if (!inputs) {
            replica_decoded_events_free(events, header.event_count);
            zfree(payload);
            replica_follower_work_leave(&replica->ingress_inflight);
            replica_stop_signal(replica);
            break;
        }
        for (uint32_t i = 0; i < header.event_count; i++)
            inputs[i] = events[i].input;
        tlc_cold_replica_batch_status_t status;
        uint64_t accepted_seq = 0;
        int rc = tlc_cold_submit_replica_batch(
            replica->cold, header.first_seq, inputs, header.event_count,
            TLC_COLD_ACK_ACCEPTED, &status, &accepted_seq);
        zfree(inputs);
        zfree(payload);
        if (status == TLC_COLD_REPLICA_BATCH_GAP ||
            status == TLC_COLD_REPLICA_BATCH_CONFLICT) {
            tlc_cold_progress_t progress;
            if (tlc_cold_get_progress(replica->cold, &progress) != 0)
                progress = (tlc_cold_progress_t){0};
            serverLog(LL_WARNING,
                      "HA Replica Follower COLD %s: first_seq=%llu count=%u expected_seq=%llu",
                      status == TLC_COLD_REPLICA_BATCH_GAP ? "GAP" : "CONFLICT",
                      (unsigned long long)header.first_seq,
                      header.event_count,
                      (unsigned long long)(progress.durable_seq + 1u));
            replica_decoded_events_free(events, header.event_count);
            replica_follower_work_leave(&replica->ingress_inflight);
            replica_follower_schedule_resync(
                replica, status == TLC_COLD_REPLICA_BATCH_GAP ?
                TLC_HA_RESYNC_REQUIRED_GAP : TLC_HA_RESYNC_REQUIRED_CONFLICT);
            continue;
        }
        if (rc != 0 || (status != TLC_COLD_REPLICA_BATCH_APPLIED &&
                         status != TLC_COLD_REPLICA_BATCH_DUPLICATE) ||
            replica_send_ack(replica, accepted_seq != 0 ? accepted_seq :
                             header.first_seq + header.event_count - 1u) != 0) {
            replica_decoded_events_free(events, header.event_count);
            replica_follower_work_leave(&replica->ingress_inflight);
            replica_stop_signal(replica);
            break;
        }
        if ((status == TLC_COLD_REPLICA_BATCH_APPLIED ||
             status == TLC_COLD_REPLICA_BATCH_DUPLICATE) &&
            atomic_load_explicit(&replica->resync_session_state,
                                 memory_order_acquire) ==
                TLC_HA_RESYNC_SESSION_FOLLOWER_WAIT_RESYNC &&
            atomic_load_explicit(&replica->resync_required_reason,
                                 memory_order_acquire) ==
                TLC_HA_RESYNC_REQUIRED_GAP) {
            uint64_t required_durable = atomic_load_explicit(
                &replica->resync_required_durable_seq, memory_order_acquire);
            tlc_cold_progress_t progress;
            if (tlc_cold_get_progress(replica->cold, &progress) == 0 &&
                progress.durable_seq > required_durable) {
                atomic_store_explicit(&replica->resync_required_pending, false,
                                      memory_order_release);
                atomic_store_explicit(&replica->resync_session_state,
                                      TLC_HA_RESYNC_SESSION_IDLE,
                                      memory_order_release);
                serverLog(LL_NOTICE,
                          "HA Replica Follower AOF repair prefix accepted: start=%llu durable=%llu",
                          (unsigned long long)(required_durable + 1u),
                          (unsigned long long)progress.durable_seq);
            }
        }
        for (uint32_t i = 0; i < header.event_count; i++) {
            tlc_ha_replica_event_t *queued =
                replica_event_copy(&events[i].input, events[i].seq);
            if (!queued || replica_queue_push(replica, queued) != 0) {
                replica_event_free(queued);
                replica_decoded_events_free(events, header.event_count);
                replica_follower_work_leave(&replica->ingress_inflight);
                replica_stop_signal(replica);
                return NULL;
            }
        }
        replica_decoded_events_free(events, header.event_count);
        replica_follower_work_leave(&replica->ingress_inflight);
    }
    return NULL;
}

int tlc_ha_replica_start(tlc_ha_replica_t **out,
                         const tlc_ha_replica_config_t *config) {
    if (!out || !config || !config->core || !config->cold ||
        (config->transport == TLC_HA_REPLICA_TRANSPORT_STREAM &&
         config->fd < 0) ||
        (config->transport != TLC_HA_REPLICA_TRANSPORT_STREAM &&
         config->transport != TLC_HA_REPLICA_TRANSPORT_UB) ||
        (config->role != TLC_HA_REPLICA_LEADER &&
         config->role != TLC_HA_REPLICA_FOLLOWER))
        return -1;
    *out = NULL;
    tlc_ha_replica_t *replica = zcalloc(sizeof(*replica));
    if (!replica) {
        serverLog(LL_WARNING, "HA Replica start failed: allocation");
        return -1;
    }
    const char *failure = NULL;
    int failure_errno = 0;
    replica->core = config->core;
    replica->cold = config->cold;
    replica->fd = config->fd;
    replica->role = config->role;
    replica->transport = config->transport;
    atomic_init(&replica->received_snapshot_artifact, (uintptr_t)NULL);
    replica->queue_capacity = config->queue_capacity ? config->queue_capacity :
                              TLC_HA_REPLICA_DEFAULT_QUEUE;
    replica->max_batch_events = config->max_batch_events ?
                                 config->max_batch_events :
                                 TLC_HA_REPLICA_DEFAULT_BATCH_EVENTS;
    replica->max_batch_bytes = config->max_batch_bytes ?
                               config->max_batch_bytes :
                               TLC_HA_REPLICA_DEFAULT_BATCH_BYTES;
    replica->max_resync_snapshot_bytes = config->max_resync_snapshot_bytes ?
        config->max_resync_snapshot_bytes :
        TLC_HA_REPLICA_DEFAULT_RESYNC_SNAPSHOT_BYTES;
    replica->hpc_node_id = config->hpc_node_id;
    replica->peer_node_id = config->peer_node_id;
    replica->ha_term = config->ha_term;
    replica->configured_topology_epoch = config->topology_epoch ?
        config->topology_epoch : 1;
    replica->heartbeat_interval_ms = config->heartbeat_interval_ms ?
        config->heartbeat_interval_ms :
        TLC_HA_REPLICA_DEFAULT_HEARTBEAT_INTERVAL_MS;
    replica->heartbeat_timeout_ms = config->heartbeat_timeout_ms ?
        config->heartbeat_timeout_ms :
        TLC_HA_REPLICA_DEFAULT_HEARTBEAT_TIMEOUT_MS;
    replica->resync_timeout_ms = config->resync_timeout_ms ?
        config->resync_timeout_ms : TLC_HA_REPLICA_DEFAULT_RESYNC_TIMEOUT_MS;
    replica->resync_chunk_bytes = config->resync_chunk_bytes ?
        config->resync_chunk_bytes : TLC_HA_REPLICA_DEFAULT_RESYNC_CHUNK_BYTES;
    if (replica->heartbeat_timeout_ms < replica->heartbeat_interval_ms) {
        failure = "heartbeat timeout is shorter than interval";
        goto failed;
    }
    replica->queue_capacity = replica_round_queue_capacity(
        replica->queue_capacity);
    if (replica->queue_capacity == 0 || replica->max_batch_events == 0 ||
        replica->max_batch_bytes == 0 ||
        replica->max_resync_snapshot_bytes == 0 ||
        replica->max_resync_snapshot_bytes > SIZE_MAX ||
        replica->configured_topology_epoch == 0) {
        failure = "invalid queue or batch configuration";
        goto failed;
    }
    replica->queue_mask = replica->queue_capacity - 1u;
    replica->queue = zcalloc_num(replica->queue_capacity,
                                 sizeof(*replica->queue));
    if (!replica->queue) {
        failure = "queue allocation";
        goto failed;
    }
    if (replica->transport == TLC_HA_REPLICA_TRANSPORT_UB) {
        if (replica_ring_open(&replica->tx_ring, &config->tx_ring) != 0) {
            failure = "TX ring open";
            goto failed;
        }
        if (replica_ring_open(&replica->rx_ring, &config->rx_ring) != 0) {
            failure = "RX ring open";
            goto failed;
        }
        if (replica->tx_ring.config.slot_bytes !=
                replica->rx_ring.config.slot_bytes ||
            replica->tx_ring.config.slot_bytes <
                sizeof(tlc_ha_replica_frame_wire_t) +
                replica->max_batch_bytes ||
            replica->rx_ring.config.slot_bytes <
                sizeof(tlc_ha_replica_frame_wire_t) +
                replica->max_batch_bytes) {
            failure = "ring slot is smaller than one configured batch";
            goto failed;
        }
        replica->rx_frame = zmalloc(replica->rx_ring.config.slot_bytes);
        if (!replica->rx_frame) {
            failure = "UB frame allocation";
            goto failed;
        }
    }
    atomic_init(&replica->stopping, false);
    atomic_init(&replica->peer_accepted_seq, 0);
    atomic_init(&replica->peer_durable_seq, 0);
    atomic_init(&replica->peer_progress_seq, 0);
    atomic_init(&replica->peer_ha_term, 0);
    atomic_init(&replica->applied_seq, 0);
    atomic_init(&replica->last_heartbeat_received_ns, getMonotonicNs());
    atomic_init(&replica->peer_health, TLC_HA_REPLICA_UNAVAILABLE);
    atomic_init(&replica->resync_fenced, false);
    atomic_init(&replica->ingress_inflight, 0);
    atomic_init(&replica->apply_inflight, 0);
    atomic_init(&replica->sender_inflight, 0);
    atomic_init(&replica->resync_emission_gate, false);
    atomic_init(&replica->resync_session_state, TLC_HA_RESYNC_SESSION_IDLE);
    atomic_init(&replica->resync_last_progress_ns, getMonotonicNs());
    atomic_init(&replica->resync_required_pending, false);
    atomic_init(&replica->resync_required_last_sent_ns, 0);
    atomic_init(&replica->resync_required_reason, TLC_HA_RESYNC_REQUIRED_GAP);
    atomic_init(&replica->resync_required_durable_seq, 0);
    atomic_init(&replica->resync_required_applied_seq, 0);
    atomic_init(&replica->next_resync_session_id, getMonotonicNs());
    atomic_init(&replica->resync_install_pending, false);
    if (replica->transport == TLC_HA_REPLICA_TRANSPORT_STREAM) {
        failure_errno = pthread_mutex_init(&replica->stream_write_mutex, NULL);
        if (failure_errno != 0) {
            failure = "stream write mutex initialization";
            goto failed;
        }
        replica->stream_write_mutex_initialized = 1;
    }
    if (replica->role == TLC_HA_REPLICA_FOLLOWER) {
        char artifact_directory[PATH_MAX];
        const char *cold_directory = tlc_cold_directory(replica->cold);
        if (!cold_directory ||
            snprintf(artifact_directory, sizeof(artifact_directory), "%s.resync",
                     cold_directory) < 0 ||
            strlen(cold_directory) + sizeof(".resync") >=
                sizeof(artifact_directory)) {
            failure = "resync artifact directory";
            goto failed;
        }
        if (tlc_ha_resync_assembler_create(
                &replica->resync_assembler, replica->hpc_node_id,
                replica->peer_node_id, replica->ha_term,
                replica->max_resync_snapshot_bytes, artifact_directory) != 0) {
            failure = "resync assembler allocation";
            goto failed;
        }
    }
    atomic_init(&replica->queue_head, 0);
    atomic_init(&replica->queue_tail, 0);
    atomic_flag_clear(&replica->producer_admission_gate);
    atomic_init(&replica->replay_from_seq, 0);
    atomic_init(&replica->replay_mode, false);
    atomic_init(&replica->replay_producer_active, false);
    atomic_init(&replica->replay_log_active, false);
    atomic_init(&replica->replay_log_start_seq, 0);
    atomic_init(&replica->normal_min_seq, 0);
    atomic_init(&replica->sender_discard_pending, false);
    if (replica->role == TLC_HA_REPLICA_LEADER) {
        if (tlc_core_set_replica_event_sink(replica->core,
                                            replica_event_sink, replica) != 0) {
            failure = "Leader replica event sink binding";
            goto failed;
        }
        failure_errno = pthread_create(&replica->sender_thread, NULL,
                                       replica_sender_main, replica);
        if (failure_errno != 0) {
            failure = "Leader sender thread creation";
            goto failed_started;
        }
        replica->sender_started = 1;
        failure_errno = pthread_create(&replica->receiver_thread, NULL,
                                       replica_listener_main, replica);
        if (failure_errno != 0) {
            failure = "Leader receiver thread creation";
            goto failed_started;
        }
        replica->receiver_started = 1;
    } else {
        failure_errno = pthread_create(&replica->receiver_thread, NULL,
                                       replica_listener_main, replica);
        if (failure_errno != 0) {
            failure = "Follower receiver thread creation";
            goto failed_started;
        }
        replica->receiver_started = 1;
        failure_errno = pthread_create(&replica->apply_thread, NULL,
                                       replica_apply_main, replica);
        if (failure_errno != 0) {
            failure = "Follower apply thread creation";
            goto failed_started;
        }
        replica->apply_started = 1;
    }
    failure_errno = pthread_create(&replica->heartbeat_thread, NULL,
                                   replica_heartbeat_main, replica);
    if (failure_errno != 0) {
        failure = "heartbeat thread creation";
        goto failed_started;
    }
    replica->heartbeat_started = 1;
    failure_errno = pthread_create(&replica->resync_controller_thread, NULL,
                                   replica_resync_controller_main, replica);
    if (failure_errno != 0) {
        failure = "resync controller thread creation";
        goto failed_started;
    }
    replica->resync_controller_started = 1;
    *out = replica;
    return 0;

failed_started:
    replica_stop_signal(replica);
    if (replica->resync_controller_started)
        pthread_join(replica->resync_controller_thread, NULL);
    if (replica->sender_started)
        pthread_join(replica->sender_thread, NULL);
    if (replica->receiver_started)
        pthread_join(replica->receiver_thread, NULL);
    if (replica->apply_started)
        pthread_join(replica->apply_thread, NULL);
    if (replica->heartbeat_started)
        pthread_join(replica->heartbeat_thread, NULL);
    if (replica->role == TLC_HA_REPLICA_LEADER)
        tlc_core_set_replica_event_sink(replica->core, NULL, NULL);
failed:
    serverLog(LL_WARNING,
              "HA Replica start failed: stage=%s role=%u transport=%u "
              "queue=%u batch_events=%u batch_bytes=%u tx_slot_bytes=%u "
              "rx_slot_bytes=%u error=%d",
              failure ? failure : "unknown", replica->role,
              replica->transport, replica->queue_capacity,
              replica->max_batch_events, replica->max_batch_bytes,
              replica->tx_ring.config.slot_bytes,
              replica->rx_ring.config.slot_bytes, failure_errno);
    tlc_ha_resync_assembler_destroy(replica->resync_assembler);
    if (replica->leader_resync_snapshot.retention_pin_token != 0)
        tlc_cold_end_resync_snapshot(replica->cold,
                                     &replica->leader_resync_snapshot);
    zfree(replica->resync_captured_seq);
    resync_artifact_free((tlc_ha_resync_artifact_t *)
        atomic_exchange_explicit(&replica->received_snapshot_artifact,
                                 (uintptr_t)NULL, memory_order_acquire));
    if (replica->stream_write_mutex_initialized)
        pthread_mutex_destroy(&replica->stream_write_mutex);
    replica_ring_close(&replica->tx_ring);
    replica_ring_close(&replica->rx_ring);
    zfree(replica->rx_frame);
    zfree(replica->queue);
    zfree(replica);
    return -1;
}

void tlc_ha_replica_stop(tlc_ha_replica_t *replica) {
    if (!replica)
        return;
    replica_stop_signal(replica);
    if (replica->resync_controller_started)
        pthread_join(replica->resync_controller_thread, NULL);
    if (replica->sender_started)
        pthread_join(replica->sender_thread, NULL);
    if (replica->receiver_started)
        pthread_join(replica->receiver_thread, NULL);
    if (replica->apply_started)
        pthread_join(replica->apply_thread, NULL);
    if (replica->heartbeat_started)
        pthread_join(replica->heartbeat_thread, NULL);
    if (replica->role == TLC_HA_REPLICA_LEADER)
        tlc_core_set_replica_event_sink(replica->core, NULL, NULL);
    if (replica->fd >= 0)
        close(replica->fd);
    for (uint32_t i = 0; i < replica->queue_capacity; i++)
        replica_event_free(replica->queue[i]);
    zfree(replica->queue);
    replica_ring_close(&replica->tx_ring);
    replica_ring_close(&replica->rx_ring);
    zfree(replica->rx_frame);
    tlc_ha_resync_assembler_destroy(replica->resync_assembler);
    if (replica->leader_resync_snapshot.retention_pin_token != 0)
        tlc_cold_end_resync_snapshot(replica->cold,
                                     &replica->leader_resync_snapshot);
    zfree(replica->resync_captured_seq);
    resync_artifact_free((tlc_ha_resync_artifact_t *)
        atomic_exchange_explicit(&replica->received_snapshot_artifact,
                                 (uintptr_t)NULL, memory_order_acquire));
    if (replica->stream_write_mutex_initialized)
        pthread_mutex_destroy(&replica->stream_write_mutex);
    zfree(replica);
}

uint64_t tlc_ha_replica_peer_accepted_seq(
        const tlc_ha_replica_t *replica) {
    return replica ? atomic_load_explicit(&replica->peer_accepted_seq,
                                          memory_order_acquire) : 0;
}

int tlc_ha_replica_get_progress(const tlc_ha_replica_t *replica,
                                tlc_ha_replica_progress_t *progress) {
    RETURN_IF(!replica || !progress, -1);
    tlc_cold_progress_t cold_progress;
    if (tlc_cold_get_progress(replica->cold, &cold_progress) != 0)
        return -1;
    *progress = (tlc_ha_replica_progress_t){
        .appended_seq = cold_progress.appended_seq,
        .durable_seq = cold_progress.durable_seq,
        .applied_seq = atomic_load_explicit(&replica->applied_seq,
                                            memory_order_acquire),
        .peer_accepted_seq = atomic_load_explicit(&replica->peer_accepted_seq,
                                                   memory_order_acquire),
        .peer_durable_seq = atomic_load_explicit(&replica->peer_durable_seq,
                                                  memory_order_acquire),
        .peer_applied_seq = atomic_load_explicit(&replica->peer_progress_seq,
                                                 memory_order_acquire),
        .replay_from_seq = atomic_load_explicit(&replica->replay_from_seq,
                                                memory_order_acquire),
    };
    return 0;
}

tlc_ha_replica_health_t tlc_ha_replica_peer_health(
        const tlc_ha_replica_t *replica) {
    return replica ? (tlc_ha_replica_health_t)atomic_load_explicit(
        &replica->peer_health, memory_order_acquire) :
        TLC_HA_REPLICA_UNAVAILABLE;
}

uint64_t tlc_ha_replica_last_heartbeat_ns(
        const tlc_ha_replica_t *replica) {
    return replica ? atomic_load_explicit(
        &replica->last_heartbeat_received_ns, memory_order_acquire) : 0;
}
