#include "tlc_ha_replica.h"
#include "cpu_relax.h"
#include "vemb_v16_hash.h"
#include "vemb_v16_log.h"
#include "vemb_v16_mapped_region.h"
#include "monotonic.h"
#include "zmalloc.h"

#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define TLC_HA_REPLICA_MAGIC UINT32_C(0x54485250) /* THRP */
#define TLC_HA_REPLICA_VERSION UINT16_C(1)
#define TLC_HA_REPLICA_KIND_EVENTS UINT16_C(1)
#define TLC_HA_REPLICA_KIND_ACK UINT16_C(2)
#define TLC_HA_REPLICA_KIND_HEARTBEAT UINT16_C(3)
#define TLC_HA_REPLICA_DEFAULT_QUEUE 256u
#define TLC_HA_REPLICA_DEFAULT_BATCH_EVENTS 32u
#define TLC_HA_REPLICA_DEFAULT_BATCH_BYTES (1024u * 1024u)
#define TLC_HA_REPLICA_RING_MAGIC UINT32_C(0x54485252) /* THRR */
#define TLC_HA_REPLICA_RING_VERSION UINT32_C(1)
#define TLC_HA_REPLICA_DEFAULT_RING_SLOTS 256u
#define TLC_HA_REPLICA_DEFAULT_RING_BYTES (64u * 1024u)
#define TLC_HA_REPLICA_DEFAULT_HEARTBEAT_INTERVAL_MS 1000u
#define TLC_HA_REPLICA_DEFAULT_HEARTBEAT_TIMEOUT_MS 3000u

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
        struct timespec pause = {0, 1000}; /* 1us while genuinely idle */
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

static int replica_ring_publish(tlc_ha_replica_ring_t *ring,
                                const void *frame, size_t frame_bytes) {
    if (frame_bytes > ring->config.slot_bytes)
        return -1;
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
                    memory_order_relaxed, memory_order_relaxed))
                break;
        } else if (diff < 0) {
            return -1;
        } else {
            position = atomic_load_explicit(&ring->ring->tail,
                                            memory_order_relaxed);
        }
    }
    /* UB provides acquire/release visibility for the shared sequence word:
     * all frame bytes are written before this release publication. */
    memcpy(slot->payload, frame, frame_bytes);
    atomic_store_explicit(&slot->sequence, position + 1,
                          memory_order_release);
    return 0;
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
    uint8_t *tx_frame;
    uint8_t *rx_frame;
    uint32_t queue_capacity;
    uint32_t max_batch_events;
    uint32_t max_batch_bytes;
    uint64_t hpc_node_id;
    uint64_t peer_node_id;
    uint64_t ha_term;
    uint32_t heartbeat_interval_ms;
    uint32_t heartbeat_timeout_ms;
    atomic_bool stopping;
    atomic_uint_fast64_t peer_accepted_seq;
    atomic_uint_fast64_t peer_durable_seq;
    atomic_uint_fast64_t peer_progress_seq;
    atomic_uint_fast64_t peer_ha_term;
    atomic_uint_fast64_t applied_seq;
    atomic_uint_fast64_t last_heartbeat_received_ns;
    atomic_uint peer_health;
    tlc_ha_replica_event_t **queue;
    uint64_t queue_mask;
    _Alignas(64) atomic_uint_fast64_t queue_head;
    _Alignas(64) atomic_uint_fast64_t queue_tail;
    pthread_t sender_thread;
    pthread_t receiver_thread;
    pthread_t apply_thread;
    pthread_t heartbeat_thread;
    uint32_t sender_started;
    uint32_t receiver_started;
    uint32_t apply_started;
    uint32_t heartbeat_started;
    pthread_mutex_t stream_write_mutex;
    uint32_t stream_write_mutex_initialized;
    uint64_t last_progress_log_ns;
};

static uint64_t replica_hton64(uint64_t value) {
    uint32_t hi = htonl((uint32_t)(value >> 32));
    uint32_t lo = htonl((uint32_t)value);
    return ((uint64_t)lo << 32) | hi;
}

static uint64_t replica_ntoh64(uint64_t value) {
    return replica_hton64(value);
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
    if (!atomic_exchange_explicit(&replica->stopping, true,
                                  memory_order_acq_rel))
        shutdown(replica->fd, SHUT_RDWR);
}

static int replica_queue_push(tlc_ha_replica_t *replica,
                              tlc_ha_replica_event_t *event) {
    uint32_t spins = 0;
    for (;;) {
        uint64_t tail = atomic_load_explicit(&replica->queue_tail,
                                             memory_order_relaxed);
        uint64_t head = atomic_load_explicit(&replica->queue_head,
                                             memory_order_acquire);
        if (tail - head < replica->queue_capacity) {
            replica->queue[tail & replica->queue_mask] = event;
            atomic_store_explicit(&replica->queue_tail, tail + 1,
                                  memory_order_release);
            return 0;
        }
        if (atomic_load_explicit(&replica->stopping, memory_order_acquire))
            return -1;
        replica_queue_wait(&spins);
    }
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
    if (replica->stream_write_mutex_initialized)
        pthread_mutex_lock(&replica->stream_write_mutex);
    if (replica->transport == TLC_HA_REPLICA_TRANSPORT_UB) {
        size_t frame_bytes = sizeof(wire_header) + payload_bytes;
        if (frame_bytes > replica->tx_ring.config.slot_bytes)
            return -1;
        memset(replica->tx_frame, 0, replica->tx_ring.config.slot_bytes);
        memcpy(replica->tx_frame, &wire_header, sizeof(wire_header));
        if (payload_bytes)
            memcpy(replica->tx_frame + sizeof(wire_header), payload,
                   payload_bytes);
        uint32_t spins = 0;
        while (!atomic_load_explicit(&replica->stopping, memory_order_acquire)) {
            rc = replica_ring_publish(&replica->tx_ring, replica->tx_frame,
                                      frame_bytes);
            if (rc == 0)
                break;
            replica_queue_wait(&spins);
        }
        if (rc != 0)
            rc = -1;
    } else {
        rc = replica_write_full(replica->fd, &wire_header, sizeof(wire_header));
        if (rc == 0 && payload_bytes)
            rc = replica_write_full(replica->fd, payload, payload_bytes);
    }
    if (replica->stream_write_mutex_initialized)
        pthread_mutex_unlock(&replica->stream_write_mutex);
    if (rc != 0)
        replica_stop_signal(replica);
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
              "HA Replica progress: role=%u appended=%llu durable=%llu applied=%llu peer_accepted=%llu peer_durable=%llu peer_applied=%llu peer_health=%u",
              replica->role, (unsigned long long)progress.appended_seq,
              (unsigned long long)progress.durable_seq,
              (unsigned long long)progress.applied_seq,
              (unsigned long long)progress.peer_accepted_seq,
              (unsigned long long)progress.peer_durable_seq,
              (unsigned long long)progress.peer_applied_seq,
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
    tlc_ha_replica_event_t *event = replica_event_copy(input, seq);
    if (!event) {
        replica_stop_signal(replica);
        return -1;
    }
    if (replica_queue_push(replica, event) != 0) {
        replica_event_free(event);
        return -1;
    }
    return 0;
}

int tlc_ha_replica_replay_from(tlc_ha_replica_t *replica,
                               uint64_t start_seq,
                               uint64_t end_seq) {
    RETURN_IF(!replica || replica->role != TLC_HA_REPLICA_LEADER ||
              !replica->cold, -1);
    return tlc_cold_replay_range(replica->cold, start_seq, end_seq,
                                 replica_event_sink, replica);
}

static void *replica_sender_main(void *arg) {
    tlc_ha_replica_t *replica = arg;
    while (!atomic_load_explicit(&replica->stopping, memory_order_acquire)) {
        tlc_ha_replica_event_t *first = replica_queue_pop(replica);
        if (!first) {
            /* Keep the sender alive while the channel is idle.  Replay can
             * enqueue an AOF range after the initial queue drains; exiting
             * here would leave that range permanently unsent. */
            usleep(1000);
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
        if (!atomic_load_explicit(&replica->stopping, memory_order_acquire))
            replica_send_frame(replica, TLC_HA_REPLICA_KIND_EVENTS,
                               first->seq, count, payload, (uint32_t)cursor);
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
         header->kind != TLC_HA_REPLICA_KIND_HEARTBEAT))
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

static void *replica_apply_main(void *arg) {
    tlc_ha_replica_t *replica = arg;
    for (;;) {
        tlc_ha_replica_event_t *event = replica_queue_pop(replica);
        if (!event)
            break;
        tlc_core_replica_apply_status_t status;
        int rc = tlc_core_apply_replica_event(replica->core, &event->input,
                                              event->seq, &status);
        if (rc == 0 && (status == TLC_CORE_REPLICA_APPLY_APPLIED ||
                        status == TLC_CORE_REPLICA_APPLY_DUPLICATE))
            atomic_store_explicit(&replica->applied_seq, event->seq,
                                  memory_order_release);
        replica_event_free(event);
        if (rc != 0 || status == TLC_CORE_REPLICA_APPLY_GAP ||
            status == TLC_CORE_REPLICA_APPLY_ERROR ||
            status == TLC_CORE_REPLICA_APPLY_STALE) {
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
            break;
        replica_log_progress(replica);
        uint64_t last = atomic_load_explicit(
            &replica->last_heartbeat_received_ns, memory_order_acquire);
        if (last != 0 && elapsedNs(last) >=
                         (uint64_t)replica->heartbeat_timeout_ms * 1000000u)
            atomic_store_explicit(&replica->peer_health,
                                  TLC_HA_REPLICA_UNAVAILABLE,
                                  memory_order_release);
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
        tlc_ha_replica_event_t *events = NULL;
        if (replica_decode_events(replica, &header, payload, &events) != 0) {
            zfree(payload);
            replica_stop_signal(replica);
            break;
        }
        tlc_cold_event_input_t *inputs =
            zcalloc_num(header.event_count, sizeof(*inputs));
        if (!inputs) {
            replica_decoded_events_free(events, header.event_count);
            zfree(payload);
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
        if (rc != 0 || (status != TLC_COLD_REPLICA_BATCH_APPLIED &&
                         status != TLC_COLD_REPLICA_BATCH_DUPLICATE) ||
            replica_send_ack(replica, accepted_seq != 0 ? accepted_seq :
                             header.first_seq + header.event_count - 1u) != 0) {
            replica_decoded_events_free(events, header.event_count);
            replica_stop_signal(replica);
            break;
        }
        for (uint32_t i = 0; i < header.event_count; i++) {
            tlc_ha_replica_event_t *queued =
                replica_event_copy(&events[i].input, events[i].seq);
            if (!queued || replica_queue_push(replica, queued) != 0) {
                replica_event_free(queued);
                replica_decoded_events_free(events, header.event_count);
                replica_stop_signal(replica);
                return NULL;
            }
        }
        replica_decoded_events_free(events, header.event_count);
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
    replica->queue_capacity = config->queue_capacity ? config->queue_capacity :
                              TLC_HA_REPLICA_DEFAULT_QUEUE;
    replica->max_batch_events = config->max_batch_events ?
                                 config->max_batch_events :
                                 TLC_HA_REPLICA_DEFAULT_BATCH_EVENTS;
    replica->max_batch_bytes = config->max_batch_bytes ?
                               config->max_batch_bytes :
                               TLC_HA_REPLICA_DEFAULT_BATCH_BYTES;
    replica->hpc_node_id = config->hpc_node_id;
    replica->peer_node_id = config->peer_node_id;
    replica->ha_term = config->ha_term;
    replica->heartbeat_interval_ms = config->heartbeat_interval_ms ?
        config->heartbeat_interval_ms :
        TLC_HA_REPLICA_DEFAULT_HEARTBEAT_INTERVAL_MS;
    replica->heartbeat_timeout_ms = config->heartbeat_timeout_ms ?
        config->heartbeat_timeout_ms :
        TLC_HA_REPLICA_DEFAULT_HEARTBEAT_TIMEOUT_MS;
    if (replica->heartbeat_timeout_ms < replica->heartbeat_interval_ms) {
        failure = "heartbeat timeout is shorter than interval";
        goto failed;
    }
    replica->queue_capacity = replica_round_queue_capacity(
        replica->queue_capacity);
    if (replica->queue_capacity == 0 || replica->max_batch_events == 0 ||
        replica->max_batch_bytes == 0) {
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
        replica->tx_frame = zmalloc(replica->tx_ring.config.slot_bytes);
        replica->rx_frame = zmalloc(replica->rx_ring.config.slot_bytes);
        if (!replica->tx_frame || !replica->rx_frame) {
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
    failure_errno = pthread_mutex_init(&replica->stream_write_mutex, NULL);
    if (failure_errno != 0) {
        failure = "stream write mutex initialization";
        goto failed;
    }
    replica->stream_write_mutex_initialized = 1;
    atomic_init(&replica->queue_head, 0);
    atomic_init(&replica->queue_tail, 0);
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
    *out = replica;
    return 0;

failed_started:
    replica_stop_signal(replica);
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
    if (replica->stream_write_mutex_initialized)
        pthread_mutex_destroy(&replica->stream_write_mutex);
    replica_ring_close(&replica->tx_ring);
    replica_ring_close(&replica->rx_ring);
    zfree(replica->rx_frame);
    zfree(replica->tx_frame);
    zfree(replica->queue);
    zfree(replica);
    return -1;
}

void tlc_ha_replica_stop(tlc_ha_replica_t *replica) {
    if (!replica)
        return;
    replica_stop_signal(replica);
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
    zfree(replica->tx_frame);
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
