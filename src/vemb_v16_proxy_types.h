#ifndef __VEMB_V16_PROXY_TYPES_H
#define __VEMB_V16_PROXY_TYPES_H

#include "vemb_v16_proxy_internal.h"
#include "vemb_v16_aeron_ring.h"
#include "vemb_v16_client_ring.h"
#include "vemb_v16_storage.h"
#include "vemb_v16_supernode.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

/* Forward decl: defined in vemb_v16_proxy.c (non-blocking handshake state machine). */
struct vemb_v16_handshake_ctx;

typedef struct vemb_v16_aeron_channel_snapshot {
    uint32_t count;
    uint32_t indices[];
} vemb_v16_aeron_channel_snapshot_t;

typedef struct vemb_v16_proxy_io_worker {
    uint32_t worker_id;
    struct vemb_v16_proxy *proxy;
    pthread_t thread;
    vemb_v16_job_pool_t job_pools[VEMB_V16_JOB_POOL_COUNT];
    vemb_v16_mapped_region_t job_pool_slot_regions[VEMB_V16_JOB_POOL_COUNT];
    _Atomic(vemb_v16_aeron_channel_snapshot_t *) aeron_snapshot;
    atomic_uint_fast32_t aeron_snapshot_readers;
    vemb_v16_aeron_channel_snapshot_t *aeron_snapshot_buffers[2];
    uint32_t aeron_snapshot_capacity;
#ifdef __linux__
    int notify_fd;
#endif
} vemb_v16_proxy_io_worker_t;

typedef struct vemb_v16_supernode_pool_worker {
    uint32_t worker_id;
    struct vemb_v16_proxy *proxy;
    pthread_t thread;
#ifdef __linux__
    atomic_int job_notify_armed;
    int job_eventfd;
#endif
} vemb_v16_supernode_pool_worker_t;

typedef struct vemb_v16_shard_queue {
    vemb_v16_aeron_ring_t ring;
    void *slots;
} vemb_v16_shard_queue_t;

struct vemb_v16_channel {
    uint64_t channel_id;
    atomic_uint_fast64_t slot_channel_id;
    uint32_t index;
    atomic_int active;
    char request_ring_name[64];
    char response_ring_name[64];
    vemb_v16_client_ring_t *request_ring;
    vemb_v16_client_ring_t *response_ring;
    size_t request_ring_bytes;
    size_t response_ring_bytes;
    uint32_t transport_type;
    int net_fd;
    atomic_int proxy_io_registered;
    atomic_uint_fast32_t proxy_io_state;
    atomic_uint_fast32_t supernode_state;
    atomic_int completion_notify_armed;
    int tcp_backpressure_enabled;
    uint8_t *tcp_input_buf;
    size_t tcp_input_cap;
    size_t tcp_input_len;
    size_t tcp_input_pos;
    uint8_t *tcp_response_backlog;
    size_t tcp_response_backlog_cap;
    size_t tcp_response_backlog_len;
    size_t tcp_response_backlog_sent;
    vemb_v16_aeron_ring_t completion_ring;
    void *completion_slots;
    vemb_v16_supernode_ctx_t supernode_ctx;
    struct vemb_v16_proxy *proxy;
    vemb_v16_channel_counters_t stats;
};

struct vemb_v16_proxy {
    char uds_path[108];
    char tcp_host[64];
    char aeron_ub_path[256];
    uint32_t vector_dim;
    uint32_t vector_stride;
    uint32_t request_ring_slot_size;
    uint32_t response_ring_slot_size;
    uint32_t max_vectors;
    vemb_v16_storage_ctx_t *storage;
    vemb_v16_channel_t channels[VEMB_V16_MAX_CHANNELS];
    atomic_uint_fast64_t next_channel_id;
    atomic_uint_fast32_t next_channel_index;
    atomic_int running;
    int listen_fd;
    uint16_t tcp_port;
    int uds_enabled;
    int tcp_enabled;
    int inject_only;
    uint32_t data_transport_type;
    uint32_t proxy_io_worker_count;
    int proxy_io_pool_started;
    vemb_v16_proxy_io_worker_t proxy_io_workers[VEMB_V16_MAX_CHANNELS];
    uint32_t supernode_worker_count;
    int supernode_pool_started;
    vemb_v16_supernode_pool_worker_t supernode_workers[VEMB_V16_MAX_CHANNELS];
    pthread_t scaleout_notify_thread;
    int scaleout_notify_thread_started;
    uint32_t scaleout_notify_interval_us;
    atomic_int scaleout_notify_stop;
    uint32_t job_shard_proxy_count;
    uint32_t job_shard_supernode_count;
    uint32_t job_pool_slots_backend_type;
    uint64_t job_pool_slots_mmap_offset;
    char job_pool_slots_path[256];
    vemb_v16_shard_queue_t *job_shard_queues;
    vemb_v16_shard_queue_t *job_return_queues;
    atomic_uint_fast64_t read_pool_alloc_ok;
    atomic_uint_fast64_t read_pool_alloc_fail;
    atomic_uint_fast64_t read_pool_inuse;
    atomic_uint_fast64_t read_pool_inuse_peak;
    uint64_t read_pool_slot_total;
    pthread_mutex_t stats_lock;
    vemb_v16_stats_t closed_stats;
    int inject_pipe_rd;   /* read by proxy thread to receive injected fds */
    int inject_pipe_wr;   /* written by Redis main thread to inject fds  */
    struct vemb_v16_handshake_ctx *pending_handshakes;  /* non-blocking handshake state list (proxy main only) */
};

#endif
