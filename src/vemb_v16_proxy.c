#define _GNU_SOURCE

#include "vemb_v16_hash.h"

#include "cpu_relax.h"
#include "futex.h"
#include "monotonic.h"
#include "vemb_v16_cacheline.h"
#include "vemb_v16_aeron_transport.h"
#include "vemb_v16_proxy_types.h"
#include "vemb_v16_tcp_transport.h"
#include "vemb_v16_log.h"
#include "vemb_v16_net.h"
#include "vemb_v16_stats.h"
#include "vemb_v16_util.h"
#include "macro.h"
#include "redisassert.h"
#include "util.h"
#include "zmalloc.h"

#include <errno.h>
#include <pthread.h>
#include <poll.h>
#include <sched.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef __linux__
#include <sys/eventfd.h>
#include <sys/epoll.h>
#endif
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>

#define VEMB_V16_JOB_SHARD_RING_SIZE 256u
#define VEMB_V16_JOB_RETURN_RING_SIZE 256u
#define VEMB_V16_COMPLETION_RING_SIZE VEMB_V16_AERON_RING_SIZE
#define VEMB_V16_SCALEOUT_NOTIFY_INTERVAL_US 100000u
#define VEMB_V16_SCALEOUT_NOTIFY_TIMEOUT_MS 1000u
#define VEMB_V16_READ_JOB_POOL_SLOTS 1024u
#define VEMB_V16_VSIM_JOB_POOL_SLOTS 256u
#define VEMB_V16_INLINE_JOB_POOL_SLOTS 512u
#define VEMB_V16_SUPERNODE_IDLE_SPIN_NS 5000ULL
#define VEMB_V16_SUPERNODE_IDLE_CLOCK_CHECK_ROUNDS 32u

static void completion_release_payload(vemb_v16_completion_t *completion) {
    vemb_v16_completion_release_inline_snapshot(completion);
}

uint64_t vemb_v16_channel_id(vemb_v16_channel_t *ch) {
    return ch->channel_id;
}

int vemb_v16_channel_active(vemb_v16_channel_t *ch) {
    return atomic_load_explicit(&ch->active, memory_order_acquire);
}

int vemb_v16_channel_net_fd(vemb_v16_channel_t *ch) {
    return ch->net_fd;
}

int vemb_v16_channel_tcp_backpressure_enabled(vemb_v16_channel_t *ch) {
    return ch->tcp_backpressure_enabled;
}

int vemb_v16_channel_proxy_running(vemb_v16_channel_t *ch) {
    return atomic_load_explicit(&ch->proxy->running, memory_order_relaxed);
}

vemb_v16_client_ring_t *vemb_v16_channel_request_ring(vemb_v16_channel_t *ch) {
    return ch->request_ring;
}

vemb_v16_client_ring_t *vemb_v16_channel_response_ring(vemb_v16_channel_t *ch) {
    return ch->response_ring;
}

uint32_t vemb_v16_channel_request_slot_size(vemb_v16_channel_t *ch) {
    return ch->request_ring->slot_size;
}

void vemb_v16_channel_add_proxy_response_ring_full(vemb_v16_channel_t *ch,
                                                   uint64_t n) {
    (void)ch;
    (void)n;
}

const char *vemb_v16_proxy_aeron_ub_path(vemb_v16_proxy_t *proxy) {
    return proxy->aeron_ub_path;
}

const char *vemb_v16_proxy_aeron_response_ub_path(vemb_v16_proxy_t *proxy) {
    return proxy->aeron_response_ub_path;
}

const char *vemb_v16_proxy_tcp_host(vemb_v16_proxy_t *proxy) {
    return proxy->tcp_host;
}

uint16_t vemb_v16_proxy_tcp_port(vemb_v16_proxy_t *proxy) {
    return proxy->tcp_port;
}

size_t vemb_v16_tcp_input_pending_bytes(vemb_v16_channel_t *ch) {
    return ch->tcp_input_len - ch->tcp_input_pos;
}

size_t vemb_v16_tcp_input_tailroom(vemb_v16_channel_t *ch) {
    return ch->tcp_input_cap - ch->tcp_input_len;
}

uint8_t *vemb_v16_tcp_input_buffer(vemb_v16_channel_t *ch) {
    return ch->tcp_input_buf;
}

uint8_t *vemb_v16_tcp_input_pending_ptr(vemb_v16_channel_t *ch) {
    return ch->tcp_input_buf + ch->tcp_input_pos;
}

uint8_t *vemb_v16_tcp_input_tail_ptr(vemb_v16_channel_t *ch) {
    return ch->tcp_input_buf + ch->tcp_input_len;
}

void vemb_v16_tcp_input_set_buffer(vemb_v16_channel_t *ch,
                                   uint8_t *buf,
                                   size_t cap) {
    ch->tcp_input_buf = buf;
    ch->tcp_input_cap = cap;
}

void vemb_v16_tcp_input_append_done(vemb_v16_channel_t *ch, size_t len) {
    ch->tcp_input_len += len;
}

void vemb_v16_tcp_input_consume(vemb_v16_channel_t *ch, size_t len) {
    ch->tcp_input_pos += len;
    if (ch->tcp_input_pos == ch->tcp_input_len)
        vemb_v16_tcp_input_reset(ch);
}

void vemb_v16_tcp_input_compact(vemb_v16_channel_t *ch) {
    size_t pending = vemb_v16_tcp_input_pending_bytes(ch);
    if (ch->tcp_input_pos != 0 && pending != 0) {
        memmove(ch->tcp_input_buf,
                ch->tcp_input_buf + ch->tcp_input_pos,
                pending);
    }
    ch->tcp_input_pos = 0;
    ch->tcp_input_len = pending;
}

void vemb_v16_tcp_input_reset(vemb_v16_channel_t *ch) {
    ch->tcp_input_len = 0;
    ch->tcp_input_pos = 0;
}

#ifdef __linux__
int vemb_v16_tcp_backlog_pending(vemb_v16_channel_t *ch) {
    return ch->tcp_response_backlog_len > ch->tcp_response_backlog_sent;
}

size_t vemb_v16_tcp_backlog_pending_bytes(vemb_v16_channel_t *ch) {
    return ch->tcp_response_backlog_len - ch->tcp_response_backlog_sent;
}

uint8_t *vemb_v16_tcp_backlog_pending_ptr(vemb_v16_channel_t *ch) {
    return ch->tcp_response_backlog + ch->tcp_response_backlog_sent;
}

void vemb_v16_tcp_backlog_compact(vemb_v16_channel_t *ch, size_t pending) {
    if (ch->tcp_response_backlog_sent != 0 && pending != 0) {
        memmove(ch->tcp_response_backlog,
                ch->tcp_response_backlog + ch->tcp_response_backlog_sent,
                pending);
    }
    ch->tcp_response_backlog_len = pending;
    ch->tcp_response_backlog_sent = 0;
}

size_t vemb_v16_tcp_backlog_capacity(vemb_v16_channel_t *ch) {
    return ch->tcp_response_backlog_cap;
}

uint8_t *vemb_v16_tcp_backlog_buffer(vemb_v16_channel_t *ch) {
    return ch->tcp_response_backlog;
}

void vemb_v16_tcp_backlog_set_buffer(vemb_v16_channel_t *ch,
                                     uint8_t *buf,
                                     size_t cap) {
    ch->tcp_response_backlog = buf;
    ch->tcp_response_backlog_cap = cap;
}

void vemb_v16_tcp_backlog_append_done(vemb_v16_channel_t *ch, size_t len) {
    ch->tcp_response_backlog_len += len;
}

void vemb_v16_tcp_backlog_consume(vemb_v16_channel_t *ch, size_t len) {
    ch->tcp_response_backlog_sent += len;
}

void vemb_v16_tcp_backlog_reset(vemb_v16_channel_t *ch) {
    ch->tcp_response_backlog_len = 0;
    ch->tcp_response_backlog_sent = 0;
}
#endif

static void vemb_v16_channel_free_slots(vemb_v16_channel_t *ch) {
    assert(ch != NULL);
    free(ch->completion_slots);
    zfree(ch->tcp_input_buf);
    zfree(ch->tcp_response_backlog);
    ch->completion_slots = NULL;
    ch->tcp_input_buf = NULL;
    ch->tcp_input_cap = 0;
    ch->tcp_input_len = 0;
    ch->tcp_input_pos = 0;
    ch->tcp_response_backlog = NULL;
    ch->tcp_response_backlog_cap = 0;
    ch->tcp_response_backlog_len = 0;
    ch->tcp_response_backlog_sent = 0;
}

static void free_shard_queue_array(vemb_v16_shard_queue_t *queues,
                                   uint32_t count) {
    if (!queues)
        return;
    for (uint32_t i = 0; i < count; i++) {
        free(queues[i].slots);
        queues[i].slots = NULL;
    }
    zfree(queues);
}

static int init_shard_queue_array(vemb_v16_shard_queue_t **out,
                                  uint32_t count,
                                  size_t slot_size,
                                  uint32_t ring_size) {
    *out = NULL;

    vemb_v16_shard_queue_t *queues = zcalloc(sizeof(*queues) * count);
    if (!queues)
        return -1;

    for (uint32_t i = 0; i < count; i++) {
        if (posix_memalign(&queues[i].slots, 64, slot_size * ring_size) != 0 ||
            vemb_v16_aeron_ring_init(&queues[i].ring,
                                     queues[i].slots,
                                     (uint32_t)slot_size,
                                     ring_size) != 0) {
            free_shard_queue_array(queues, count);
            return -1;
        }
    }

    *out = queues;
    return 0;
}

static void free_job_shard_queues(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    uint32_t count = proxy->job_shard_proxy_count *
        proxy->job_shard_supernode_count;
    free_shard_queue_array(proxy->job_shard_queues, count);
    proxy->job_shard_queues = NULL;
    proxy->job_shard_proxy_count = 0;
    proxy->job_shard_supernode_count = 0;
}

static int validate_pooled_worker_config(vemb_v16_proxy_t *proxy);
static int drain_job_return_queues(vemb_v16_proxy_t *proxy,
                                   uint32_t proxy_worker_id);
static int tcp_vemb_read_requires_inline_op(vemb_v16_channel_t *ch,
                                            const vemb_v16_req_t *req);

/* Assign ownership once, before a channel becomes visible to any poller.
 * v2 channels must not derive their lane from the channel-table slot because
 * a v1 fallback channel is created adjacent to each v2 channel. */
static void assign_channel_owners(vemb_v16_proxy_t *proxy,
                                  vemb_v16_channel_t *ch,
                                  int dedicated_v2) {
    assert(proxy != NULL);
    assert(ch != NULL);
    uint32_t pio_count = proxy->proxy_io_worker_count;
    uint32_t snw_count = proxy->supernode_worker_count;
    if (dedicated_v2) {
        uint32_t sequence = atomic_fetch_add_explicit(
            &proxy->next_v2_lane_index, 1, memory_order_relaxed);
        ch->proxy_io_worker_id = pio_count ? sequence % pio_count : 0;
        ch->supernode_worker_id = snw_count ? sequence % snw_count : 0;
    } else {
        ch->proxy_io_worker_id = pio_count ? ch->index % pio_count : 0;
        ch->supernode_worker_id = snw_count ? ch->index % snw_count : 0;
    }
}

static size_t aeron_channel_snapshot_bytes(uint32_t capacity) {
    return offsetof(vemb_v16_aeron_channel_snapshot_t, indices) +
           (size_t)capacity * sizeof(uint32_t);
}

static int aeron_channel_snapshot_init(vemb_v16_proxy_io_worker_t *worker,
                                       uint32_t worker_count) {
    uint32_t capacity = (VEMB_V16_MAX_CHANNELS + worker_count - 1) /
                        worker_count;
    size_t bytes = aeron_channel_snapshot_bytes(capacity);
    for (uint32_t i = 0; i < 2; i++) {
        worker->aeron_snapshot_buffers[i] = zmalloc(bytes);
        if (!worker->aeron_snapshot_buffers[i]) {
            for (uint32_t j = 0; j < i; j++)
                zfree(worker->aeron_snapshot_buffers[j]);
            worker->aeron_snapshot_buffers[0] = NULL;
            worker->aeron_snapshot_buffers[1] = NULL;
            return -1;
        }
        worker->aeron_snapshot_buffers[i]->count = 0;
    }
    worker->aeron_snapshot_capacity = capacity;
    atomic_init(&worker->aeron_snapshot_readers, 0);
    atomic_init(&worker->aeron_snapshot,
                worker->aeron_snapshot_buffers[0]);
    return 0;
}

static void aeron_channel_snapshot_cleanup(vemb_v16_proxy_io_worker_t *worker) {
    zfree(worker->aeron_snapshot_buffers[0]);
    zfree(worker->aeron_snapshot_buffers[1]);
    worker->aeron_snapshot_buffers[0] = NULL;
    worker->aeron_snapshot_buffers[1] = NULL;
    worker->aeron_snapshot_capacity = 0;
}

/* Control-plane updates are serialized by the proxy control loop.  The
 * poller only reads the atomic snapshot pointer and never takes a lock. */
static int proxy_aeron_channel_snapshot_update(vemb_v16_proxy_t *proxy,
                                                vemb_v16_channel_t *ch,
                                                int add) {
    if (ch->transport_type != VEMB_V16_TRANSPORT_AERON)
        return 0;
    if (proxy->proxy_io_worker_count == 0 ||
        !proxy->proxy_io_pool_started)
        return 0;

    uint32_t worker_id = ch->proxy_io_worker_id;
    assert(worker_id < proxy->proxy_io_worker_count);
    vemb_v16_proxy_io_worker_t *worker =
        &proxy->proxy_io_workers[worker_id];
    vemb_v16_aeron_channel_snapshot_t *current =
        atomic_load_explicit(&worker->aeron_snapshot, memory_order_seq_cst);
    vemb_v16_aeron_channel_snapshot_t *next =
        current == worker->aeron_snapshot_buffers[0]
            ? worker->aeron_snapshot_buffers[1]
            : worker->aeron_snapshot_buffers[0];

    /* There is one poller per lane.  Readers are incremented before loading
     * the pointer, so waiting here makes it safe to reuse the other buffer. */
    while (atomic_load_explicit(&worker->aeron_snapshot_readers,
                                memory_order_seq_cst) != 0)
        cpu_relax();

    uint32_t found = worker->aeron_snapshot_capacity;
    for (uint32_t i = 0; i < current->count; i++) {
        if (current->indices[i] == ch->index) {
            found = i;
            break;
        }
    }

    uint32_t next_count = current->count;
    if (add) {
        if (found != worker->aeron_snapshot_capacity)
            return 0;
        if (next_count >= worker->aeron_snapshot_capacity)
            return -1;
        memcpy(next->indices,
               current->indices,
               (size_t)current->count * sizeof(uint32_t));
        next->indices[next_count++] = ch->index;
    } else {
        if (found == worker->aeron_snapshot_capacity)
            return 0;
        next_count--;
        memcpy(next->indices,
               current->indices,
               (size_t)current->count * sizeof(uint32_t));
        next->indices[found] = next->indices[next_count];
    }
    next->count = next_count;
    atomic_exchange_explicit(&worker->aeron_snapshot,
                             next,
                             memory_order_seq_cst);
    serverLog(LL_NOTICE,
              "vemb_v16 aeron snapshot %s: channel=%u worker=%u count=%u",
              add ? "add" : "remove", ch->index, worker_id, next_count);
    return 0;
}

static void free_job_return_queues(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    uint32_t count = proxy->job_shard_proxy_count *
        proxy->job_shard_supernode_count;
    free_shard_queue_array(proxy->job_return_queues, count);
    proxy->job_return_queues = NULL;
}

static int init_job_return_queues(vemb_v16_proxy_t *proxy) {
    if (validate_pooled_worker_config(proxy) != 0)
        return -1;
    if (proxy->job_return_queues)
        return 0;

    uint32_t count = proxy->proxy_io_worker_count *
        proxy->supernode_worker_count;
    if (init_shard_queue_array(&proxy->job_return_queues,
                               count,
                               sizeof(vemb_v16_job_return_t),
                               VEMB_V16_JOB_RETURN_RING_SIZE) != 0) {
        free_job_return_queues(proxy);
        return -1;
    }
    return 0;
}

static uint32_t job_pool_slot_count(const vemb_v16_proxy_t *proxy,
                                    uint16_t pool_type) {
    assert(proxy != NULL);
    switch (pool_type) {
    case VEMB_V16_JOB_POOL_READ:
        return VEMB_V16_READ_JOB_POOL_SLOTS;
    case VEMB_V16_JOB_POOL_VSIM_KEY_KEY:
        return VEMB_V16_VSIM_JOB_POOL_SLOTS;
    case VEMB_V16_JOB_POOL_INLINE_VECTOR:
        return VEMB_V16_INLINE_JOB_POOL_SLOTS;
    default:
        return 0;
    }
}

static size_t job_pool_slot_stride(uint16_t pool_type) {
    switch (pool_type) {
    case VEMB_V16_JOB_POOL_READ:
        return offsetof(vemb_v16_job_slot_t, u) + sizeof(vemb_v16_vemb_job_t);
    case VEMB_V16_JOB_POOL_VSIM_KEY_KEY:
        return offsetof(vemb_v16_job_slot_t, u) + sizeof(vemb_v16_vsim_key_key_job_t);
    case VEMB_V16_JOB_POOL_INLINE_VECTOR:
        return offsetof(vemb_v16_job_slot_t, u) + sizeof(vemb_v16_vadd_job_t);
    default:
        return 0;
    }
}

static vemb_v16_job_slot_t *job_pool_slot(vemb_v16_job_pool_t *pool,
                                          uint32_t slot_id) {
    assert(pool != NULL);
    assert(slot_id < pool->slot_count);
    return (vemb_v16_job_slot_t *)(
        (uint8_t *)pool->slots + (size_t)slot_id * pool->slot_stride);
}

static void job_pool_slot_reset(vemb_v16_job_pool_t *pool, uint32_t slot_id) {
    vemb_v16_job_slot_t *slot = job_pool_slot(pool, slot_id);
    /* A reserved slot is unpublished. fill_job_slot() overwrites every
     * semantic payload field before publication, so do not clear the large
     * inline-vector payload on every request. */
    slot->hdr.op = 0;
}

static size_t job_pool_slots_region_bytes(const vemb_v16_proxy_t *proxy,
                                          uint16_t pool_type) {
    return align_up_size((size_t)job_pool_slot_count(proxy, pool_type) *
                             job_pool_slot_stride(pool_type),
                         CACHELINE_SIZE);
}

static uint64_t job_pool_slots_region_offset(const vemb_v16_proxy_t *proxy,
                                             uint32_t worker_id,
                                             uint16_t pool_type) {
    uint64_t offset = proxy->job_pool_slots_mmap_offset;
    for (uint32_t i = 0; i < worker_id; i++) {
        for (uint16_t t = 0; t < VEMB_V16_JOB_POOL_COUNT; t++)
            offset += job_pool_slots_region_bytes(proxy, t);
    }
    for (uint16_t t = 0; t < pool_type; t++)
        offset += job_pool_slots_region_bytes(proxy, t);
    return offset;
}

static int init_worker_job_pool(vemb_v16_job_pool_t *pool,
                                vemb_v16_proxy_t *proxy,
                                uint32_t worker_id,
                                uint16_t pool_type) {
    memset(pool, 0, sizeof(*pool));
    pool->pool_type = pool_type;
    pool->slot_count = job_pool_slot_count(proxy, pool_type);
    pool->slot_stride = (uint32_t)job_pool_slot_stride(pool_type);
    RETURN_IF(pool->slot_count == 0 || pool->slot_stride == 0, -1);
    if (proxy->job_pool_slots_path[0] != '\0') {
        vemb_v16_mapped_region_t *region =
            &proxy->proxy_io_workers[worker_id].job_pool_slot_regions[pool_type];
        if (vemb_v16_mapped_region_open(region,
                                        proxy->job_pool_slots_backend_type,
                                        VEMB_V16_UB_CACHE_POLICY_CACHEABLE,
                                        proxy->job_pool_slots_path,
                                        job_pool_slots_region_offset(proxy,
                                                                     worker_id,
                                                                     pool_type),
                                        job_pool_slots_region_bytes(proxy,
                                                                    pool_type)) != 0) {
            return -1;
        }
        pool->slots = region->mapped_addr;
    } else if (posix_memalign(&pool->slots,
                              64,
                              (size_t)pool->slot_count * pool->slot_stride) != 0) {
        return -1;
    }
    pool->free_stack = zmalloc(sizeof(*pool->free_stack) * pool->slot_count);
    if (pool->free_stack == NULL) {
        if (proxy->job_pool_slots_path[0] != '\0') {
            vemb_v16_mapped_region_close(
                &proxy->proxy_io_workers[worker_id].job_pool_slot_regions[pool_type]);
        } else {
            zfree(pool->slots);
        }
        pool->slots = NULL;
        return -1;
    }
    pool->free_count = pool->slot_count;
    for (uint32_t i = 0; i < pool->slot_count; i++) {
        vemb_v16_job_slot_t *slot = job_pool_slot(pool, i);
        memset(slot, 0, pool->slot_stride);
        atomic_init(&slot->hdr.state, VEMB_V16_JOB_SLOT_FREE);
        slot->hdr.generation = 1;
        pool->free_stack[i] = pool->slot_count - 1 - i;
    }
    return 0;
}

static void cleanup_worker_job_pool(vemb_v16_job_pool_t *pool) {
    RETURN_IF(!pool);
    zfree(pool->free_stack);
    memset(pool, 0, sizeof(*pool));
}

static int init_proxy_io_job_pools(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    for (uint32_t i = 0; i < proxy->proxy_io_worker_count; i++) {
        for (uint16_t pool_type = 0; pool_type < VEMB_V16_JOB_POOL_COUNT; pool_type++) {
            if (init_worker_job_pool(
                    &proxy->proxy_io_workers[i].job_pools[pool_type],
                    proxy,
                    i,
                    pool_type) != 0) {
                return -1;
            }
        }
    }
    return 0;
}

static void cleanup_proxy_io_job_pools(vemb_v16_proxy_t *proxy) {
    RETURN_IF(!proxy);
    for (uint32_t i = 0; i < proxy->proxy_io_worker_count; i++) {
        for (uint16_t pool_type = 0; pool_type < VEMB_V16_JOB_POOL_COUNT; pool_type++) {
            vemb_v16_mapped_region_close(
                &proxy->proxy_io_workers[i].job_pool_slot_regions[pool_type]);
            if (proxy->job_pool_slots_path[0] == '\0') {
                zfree(proxy->proxy_io_workers[i].job_pools[pool_type].slots);
                proxy->proxy_io_workers[i].job_pools[pool_type].slots = NULL;
            }
            cleanup_worker_job_pool(&proxy->proxy_io_workers[i].job_pools[pool_type]);
        }
    }
}

/// Scheduling plane: allocate the proxy-IO -> SuperNode shard queues.
static int init_job_shard_queues(vemb_v16_proxy_t *proxy) {
    if (validate_pooled_worker_config(proxy) != 0)
        return -1;
    if (proxy->job_shard_queues)
        return 0;

    uint32_t proxy_count = proxy->proxy_io_worker_count;
    uint32_t supernode_count = proxy->supernode_worker_count;
    uint32_t count = proxy_count * supernode_count;
    proxy->job_shard_proxy_count = proxy_count;
    proxy->job_shard_supernode_count = supernode_count;
    if (init_shard_queue_array(&proxy->job_shard_queues,
                               count,
                               sizeof(vemb_v16_job_ref_t),
                               VEMB_V16_JOB_SHARD_RING_SIZE) != 0) {
        free_job_shard_queues(proxy);
        return -1;
    }
    return 0;
}

#ifdef __linux__
static uint32_t proxy_available_cpu_count(void);
#endif

static int validate_pooled_worker_config(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    if (proxy->proxy_io_worker_count == 0)
        return -1;
    if (proxy->supernode_worker_count == 0)
        return -1;
    return 0;
}

static uint32_t default_balanced_worker_count(void) {
#ifdef __linux__
    uint32_t cpus = proxy_available_cpu_count();
#else
    long online = sysconf(_SC_NPROCESSORS_ONLN);
    uint32_t cpus = online > 0 ? (uint32_t)online : 1u;
#endif
    uint32_t target = cpus / 4u;
    if (target < 1u)
        target = 1u;
    if (target > 32u)
        target = 32u;
    if (target > VEMB_V16_MAX_CHANNELS)
        target = VEMB_V16_MAX_CHANNELS;
    return target;
}

static void apply_default_worker_counts(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    if (proxy->proxy_io_worker_count != 0 &&
        proxy->supernode_worker_count != 0)
        return;

    uint32_t target = default_balanced_worker_count();
    if (proxy->proxy_io_worker_count == 0)
        proxy->proxy_io_worker_count = target;
    if (proxy->supernode_worker_count == 0)
        proxy->supernode_worker_count = target;
#ifdef __linux__
    uint32_t available_cpus = proxy_available_cpu_count();
#else
    uint32_t available_cpus = target * 4u;
#endif
    serverLog(LL_NOTICE,
              "vemb_v16 auto worker counts: available_cpus=%u proxy_io_threads=%u supernode_workers=%u",
              available_cpus,
              proxy->proxy_io_worker_count,
              proxy->supernode_worker_count);
}

static int shard_queue_topology_ready(vemb_v16_proxy_t *proxy) {
    if (validate_pooled_worker_config(proxy) != 0)
        return 0;
    if (!proxy->job_shard_queues)
        return 0;
    if (proxy->job_shard_proxy_count != proxy->proxy_io_worker_count)
        return 0;
    if (proxy->job_shard_supernode_count != proxy->supernode_worker_count)
        return 0;
    return 1;
}

static uint32_t shard_queue_index(vemb_v16_proxy_t *proxy,
                                  uint32_t proxy_worker_id,
                                  uint32_t sn_work_id) {
    assert(proxy != NULL);
    assert(shard_queue_topology_ready(proxy));
    assert(proxy_worker_id < proxy->job_shard_proxy_count);
    assert(sn_work_id < proxy->job_shard_supernode_count);
    return proxy_worker_id * proxy->job_shard_supernode_count + sn_work_id;
}

static int pooled_workers_have_1to1_pairing(const vemb_v16_proxy_t *proxy) {
    return proxy->proxy_io_worker_count != 0 &&
           proxy->proxy_io_worker_count == proxy->supernode_worker_count &&
           proxy->job_shard_proxy_count == proxy->job_shard_supernode_count;
}

static vemb_v16_storage_ctx_t *proxy_storage(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    assert(proxy->storage != NULL);
    return proxy->storage;
}

/* Populate ATTACH resp with the first local warm region. The response always
 * carries the server-side path; a same-host client opens it directly, while
 * a remote client maps UB paths to its local device view. */
void vemb_v16_proxy_fill_attach_warm_region(
    vemb_v16_proxy_t *proxy,
    vemb_v16_aeron_attach_resp_t *resp) {
    if (!proxy || !resp) return;
    vemb_v16_channel_desc_t desc = {0};
    vemb_v16_storage_fill_channel_desc(proxy_storage(proxy), &desc);
    const vemb_v16_manifest_region_t *region =
        vemb_v16_storage_first_local_region(proxy_storage(proxy));
    if (!region) {
        resp->warm_region_count = 0;
        return;
    }
    for (uint32_t i = 0; i < desc.warm_region_count; i++) {
        if (desc.warm_regions[i].region_id != region->region_id)
            continue;
        resp->warm_region_count = 1;
        resp->warm_region_id    = desc.warm_regions[i].region_id;
        resp->warm_backend_type = desc.warm_regions[i].backend_type;
        resp->warm_region_bytes = desc.warm_regions[i].region_bytes;
        resp->warm_mmap_offset  = desc.warm_regions[i].mmap_offset;
        resp->warm_path_len =
            (uint32_t)strnlen(desc.warm_regions[i].path, 255) + 1u;
        strncpy(resp->warm_path, desc.warm_regions[i].path, 255);
        resp->warm_path[255] = 0;
        return;
    }
    resp->warm_region_count = 0;
}

static int migration_control_req_valid(
        const vemb_v16_migration_control_req_t *req) {
    return req->key_len > 0 &&
           req->key_len <= VEMB_V16_MAX_KEY_LEN &&
           req->target_owner != UINT32_MAX;
}

static void migration_control_fill_resp(
        vemb_v16_migration_control_resp_t *resp,
        uint8_t status,
        const tlc_core_key_migration_info_t *info) {
    *resp = (vemb_v16_migration_control_resp_t){
        .status = status,
    };
    if (!info)
        return;
    resp->key_hash = info->key_hash;
    resp->key_version = info->key_version;
    resp->topology_epoch = info->topology_epoch;
    resp->owner_epoch = info->owner_epoch;
    resp->migration_state = info->migration_state;
    resp->target_owner = info->target_owner;
    resp->tombstone = info->tombstone;
    resp->shard_id = info->shard_id;
}

static void migration_control_fill_outbox(
        vemb_v16_migration_control_resp_t *resp,
        const vemb_v16_migration_outbox_stats_t *stats) {
    resp->applied_seq = stats->acked_seq;
    resp->barrier_seq = stats->barrier_seq;
    resp->pending_delta = stats->pending_count;
    resp->outbox_state = stats->state;
}

static void epoch_control_fill_resp(vemb_v16_proxy_t *proxy,
                                    vemb_v16_epoch_control_resp_t *resp,
                                    uint8_t status) {
    resp->status = status;
    vemb_v16_storage_epoch_get(proxy_storage(proxy),
                               &resp->current_topology_epoch,
                               &resp->min_write_epoch);
}

static void topology_resp_add_local_endpoint(
        vemb_v16_proxy_t *proxy,
        vemb_v16_topology_control_resp_t *resp);

static void topology_control_fill_resp(
        vemb_v16_proxy_t *proxy,
        vemb_v16_topology_control_resp_t *resp,
        uint8_t status) {
    vemb_v16_storage_topology_get(proxy_storage(proxy), resp);
    topology_resp_add_local_endpoint(proxy, resp);
    resp->status = status;
}

static void topology_resp_upsert_endpoint(
        vemb_v16_topology_control_resp_t *resp,
        const vemb_v16_topology_endpoint_t *endpoint) {
    if (endpoint->owner_id == UINT32_MAX)
        return;
    for (uint32_t i = 0; i < resp->endpoint_count; i++) {
        if (resp->endpoints[i].owner_id == endpoint->owner_id) {
            resp->endpoints[i] = *endpoint;
            return;
        }
    }
    if (resp->endpoint_count >= VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS)
        return;
    resp->endpoints[resp->endpoint_count++] = *endpoint;
}

static int topology_resp_has_endpoint_for_owner(
        const vemb_v16_topology_control_resp_t *resp,
        uint32_t owner_id) {
    for (uint32_t i = 0; i < resp->endpoint_count; i++) {
        if (resp->endpoints[i].owner_id == owner_id)
            return 1;
    }
    return 0;
}

static void topology_resp_add_local_endpoint(
        vemb_v16_proxy_t *proxy,
        vemb_v16_topology_control_resp_t *resp) {
    if (topology_resp_has_endpoint_for_owner(
            resp, proxy_storage(proxy)->local_owner_id)) {
        return;
    }

    vemb_v16_topology_endpoint_t endpoint = {
        .owner_id = proxy_storage(proxy)->local_owner_id,
    };
    if (!proxy->tcp_enabled && !proxy->inject_only)
        return;
    endpoint.transport_type = proxy->data_transport_type;
    endpoint.tcp_port = proxy->tcp_port;
    strncpy(endpoint.host, proxy->tcp_host, sizeof(endpoint.host) - 1);
    endpoint.host[sizeof(endpoint.host) - 1] = '\0';
    topology_resp_upsert_endpoint(resp, &endpoint);
}

/// Lifecycle synchronization: reset a channel after TCP or UB/SHM teardown.
static void reset_closed_channel(vemb_v16_channel_t *ch) {
    assert(ch != NULL);
    atomic_store_explicit(&ch->slot_channel_id, 0, memory_order_release);
    atomic_store_explicit(&ch->resource_generation, 0,
                          memory_order_release);
    atomic_store_explicit(&ch->active, 0, memory_order_release);
    atomic_store_explicit(&ch->proxy_io_registered, 0, memory_order_release);
    atomic_store_explicit(&ch->proxy_io_state, 0, memory_order_release);
    atomic_store_explicit(&ch->supernode_state, 0, memory_order_release);
    atomic_store_explicit(&ch->completion_notify_armed, 0,
                          memory_order_release);
    ch->channel_id = 0;
    ch->index = 0;
    ch->proxy_io_worker_id = 0;
    ch->supernode_worker_id = 0;
    memset(ch->request_ring_name, 0, sizeof(ch->request_ring_name));
    memset(ch->response_ring_name, 0, sizeof(ch->response_ring_name));
    ch->request_ring = NULL;
    ch->response_ring = NULL;
    ch->request_ring_bytes = 0;
    ch->response_ring_bytes = 0;
    ch->batch_v2 = 0;
    memset(&ch->batch_allocation, 0, sizeof(ch->batch_allocation));
    ch->batch_effective_size = 0;
    ch->batch_max_bytes = 0;
    memset(&ch->batch_response_producer, 0, sizeof(ch->batch_response_producer));
    ch->transport_type = 0;
    ch->net_fd = -1;
    ch->tcp_backpressure_enabled = 0;
    ch->tcp_input_buf = NULL;
    ch->tcp_input_cap = 0;
    ch->tcp_input_len = 0;
    ch->tcp_input_pos = 0;
    ch->tcp_response_backlog = NULL;
    ch->tcp_response_backlog_cap = 0;
    ch->tcp_response_backlog_len = 0;
    ch->tcp_response_backlog_sent = 0;
    memset(&ch->completion_ring, 0, sizeof(ch->completion_ring));
    ch->completion_slots = NULL;
    memset(&ch->supernode_ctx, 0, sizeof(ch->supernode_ctx));
    ch->proxy = NULL;
    memset(&ch->stats, 0, sizeof(ch->stats));
}

/// Execution-side ownership: let one SuperNode worker safely touch a channel.
static int supernode_channel_acquire(vemb_v16_channel_t *ch) {
    if (atomic_load_explicit(&ch->slot_channel_id, memory_order_acquire) == 0 ||
        !atomic_load_explicit(&ch->active, memory_order_acquire)) {
        return 0;
    }
    uint_fast32_t state =
        atomic_load_explicit(&ch->supernode_state, memory_order_acquire);
    for (;;) {
        if (state & VEMB_V16_SUPERNODE_STATE_CLOSING)
            return 0;
        if (atomic_compare_exchange_weak_explicit(&ch->supernode_state,
                                                  &state,
                                                  state + 1,
                                                  memory_order_acquire,
                                                  memory_order_relaxed)) {
            return 1;
        }
    }
}

/// Scheduling-side ownership: let one proxy IO worker safely touch a channel.
static int proxy_io_channel_acquire(vemb_v16_channel_t *ch) {
    if (atomic_load_explicit(&ch->slot_channel_id, memory_order_acquire) == 0 ||
        !atomic_load_explicit(&ch->active, memory_order_acquire)) {
        return 0;
    }
    uint_fast32_t state =
        atomic_load_explicit(&ch->proxy_io_state, memory_order_acquire);
    for (;;) {
        if (state & VEMB_V16_PROXY_IO_STATE_CLOSING)
            return 0;
        if (atomic_compare_exchange_weak_explicit(&ch->proxy_io_state,
                                                  &state,
                                                  state + 1,
                                                  memory_order_acquire,
                                                  memory_order_relaxed)) {
            return 1;
        }
    }
}

static void proxy_io_channel_release(vemb_v16_channel_t *ch) {
    atomic_fetch_sub_explicit(&ch->proxy_io_state, 1, memory_order_release);
}

static void proxy_io_channel_disarm_completion_notify(vemb_v16_channel_t *ch) {
    assert(ch != NULL);
    atomic_store_explicit(&ch->completion_notify_armed, 0,
                          memory_order_release);
}

#ifdef __linux__
static int proxy_io_channel_arm_completion_notify(vemb_v16_channel_t *ch) {
    if (!atomic_load_explicit(&ch->active, memory_order_acquire)) {
        return 0;
    }

    atomic_store_explicit(&ch->completion_notify_armed, 1,
                          memory_order_release);
    if (vemb_v16_aeron_available(&ch->completion_ring) != 0) {
        atomic_store_explicit(&ch->completion_notify_armed, 0,
                              memory_order_release);
        return -1;
    }
    return 1;
}
#endif

static void proxy_io_channel_close_begin(vemb_v16_channel_t *ch) {
    atomic_fetch_or_explicit(&ch->proxy_io_state,
                             VEMB_V16_PROXY_IO_STATE_CLOSING,
                             memory_order_acq_rel);
}

static void proxy_io_channel_wait_closed(vemb_v16_channel_t *ch) {
    for (;;) {
        uint_fast32_t state =
            atomic_load_explicit(&ch->proxy_io_state, memory_order_acquire);
        if ((state & ~VEMB_V16_PROXY_IO_STATE_CLOSING) == 0 &&
            !atomic_load_explicit(&ch->proxy_io_registered,
                                  memory_order_acquire)) {
            return;
        }
        cpu_relax();
    }
}

static void supernode_channel_release(vemb_v16_channel_t *ch) {
    atomic_fetch_sub_explicit(&ch->supernode_state, 1, memory_order_release);
}

static void supernode_channel_close_begin(vemb_v16_channel_t *ch) {
    atomic_fetch_or_explicit(&ch->supernode_state,
                             VEMB_V16_SUPERNODE_STATE_CLOSING,
                             memory_order_acq_rel);
}

static void supernode_channel_wait_closed(vemb_v16_channel_t *ch) {
    for (;;) {
        uint_fast32_t state =
            atomic_load_explicit(&ch->supernode_state, memory_order_acquire);
        if ((state & ~VEMB_V16_SUPERNODE_STATE_CLOSING) == 0)
            return;
        cpu_relax();
    }
}

int vemb_v16_proxy_tcp_response_vector_slice(vemb_v16_channel_t *ch,
                              vemb_v16_resp_t *resp,
                              const uint8_t **vector,
                              uint32_t *vector_bytes) {
    return vemb_v16_storage_vector_slice(proxy_storage(ch->proxy),
                                         resp,
                                         vector,
                                         vector_bytes);
}

void vemb_v16_make_response_from(vemb_v16_resp_t *resp,
                                 const vemb_v16_completion_t *completion) {
    *resp = (vemb_v16_resp_t){
        .status = completion->status,
        .op = completion->op,
        .flags = completion->flags,
        .req_id = completion->req_id,
        .key_hash = completion->key_hash,
        .vector_offset = completion->vector_offset,
        .vector_bytes = completion->vector_bytes,
        .dim = completion->dim,
        .region_id = completion->region_id,
        .local_slot = completion->local_slot,
        .owner_generation = completion->owner_generation,
        .redirect_owner = completion->redirect_owner,
        .score = completion->score,
    };
}

/// Response scheduling: route a completion back to TCP or UB/SHM clients.
static void publish_response(vemb_v16_channel_t *ch,
                             const vemb_v16_completion_t *completion) {
    vemb_v16_resp_t resp;
    vemb_v16_make_response_from(&resp, completion);
    if (ch->transport_type == VEMB_V16_TRANSPORT_TCP) {
        if (ch->net_fd < 0 ||
            vemb_v16_tcp_publish_response(ch, &resp) != 0) {
            serverLog(LL_WARNING,
                      "vemb_v16 tcp publish response failed: channel_index=%u channel_id=%llu req_id=%u op=%u status=%u flags=%u net_fd=%d active=%d",
                      ch->index,
                      (unsigned long long)ch->channel_id,
                      resp.req_id,
                      resp.op,
                      resp.status,
                      resp.flags,
                      ch->net_fd,
                      atomic_load_explicit(&ch->active, memory_order_acquire));
            if (ch->net_fd >= 0) {
                shutdown(ch->net_fd, SHUT_RDWR);
                close(ch->net_fd);
                ch->net_fd = -1;
            }
            return;
        }
    } else {
        if (vemb_v16_aeron_publish_response(ch, &resp) != 0)
            return;
    }
}

static vemb_v16_completion_t make_completion(vemb_v16_channel_t *ch,
                                             const vemb_v16_req_t *req,
                                             uint8_t status) {
    return (vemb_v16_completion_t){
        .status = status,
        .op = req->op,
        .req_id = req->req_id,
        .channel_index = ch->index,
        .channel_id = ch->channel_id,
    };
}

static void publish_status_response(vemb_v16_channel_t *ch,
                                    const vemb_v16_req_t *req,
                                    uint8_t status) {
    vemb_v16_completion_t completion = make_completion(ch, req, status);
    publish_response(ch, &completion);
}

#ifdef __linux__
static int flush_tcp_response_backlog(vemb_v16_channel_t *ch) {
    return vemb_v16_tcp_flush_response_backlog(ch);
}
#endif

static int publish_completion_batch(vemb_v16_channel_t *ch,
                                    vemb_v16_completion_t *completions,
                                    const uint16_t *ready_indices,
                                    uint32_t ready_count) {
    if (ready_count == 0)
        return 0;

    if (ch->transport_type == VEMB_V16_TRANSPORT_TCP) {
        uint32_t published = 0;
        if (ch->net_fd < 0 ||
            vemb_v16_tcp_publish_response_batch(ch,
                                                completions,
                                                ready_indices,
                                                ready_count,
                                                &published) != 0) {
            uint16_t first = ready_count ? ready_indices[0] : 0;
            const vemb_v16_completion_t *completion =
                ready_count ? &completions[first] : NULL;
            serverLog(LL_WARNING,
                      "vemb_v16 tcp publish response batch failed: channel_index=%u channel_id=%llu ready_count=%u published=%u first_req_id=%u first_op=%u first_status=%u net_fd=%d active=%d",
                      ch->index,
                      (unsigned long long)ch->channel_id,
                      ready_count,
                      published,
                      completion ? completion->req_id : 0,
                      completion ? completion->op : 0,
                      completion ? completion->status : 0,
                      ch->net_fd,
                      atomic_load_explicit(&ch->active, memory_order_acquire));
            for (uint32_t i = 0; i < ready_count; i++)
                completion_release_payload(&completions[ready_indices[i]]);
            if (ch->net_fd >= 0) {
                shutdown(ch->net_fd, SHUT_RDWR);
                close(ch->net_fd);
                ch->net_fd = -1;
            }
            return -1;
        }
        for (uint32_t i = 0; i < ready_count; i++)
            completion_release_payload(&completions[ready_indices[i]]);
    } else {
        vemb_v16_resp_t resps[PROXY_RESPONSE_BATCH];
        for (uint32_t i = 0; i < ready_count; i++)
            vemb_v16_make_response_from(&resps[i],
                                        &completions[ready_indices[i]]);
        if (vemb_v16_aeron_publish_response_batch(ch,
                                                  resps,
                                                  ready_count) != 0) {
            for (uint32_t i = 0; i < ready_count; i++)
                completion_release_payload(&completions[ready_indices[i]]);
            return -1;
        }
        for (uint32_t i = 0; i < ready_count; i++)
            completion_release_payload(&completions[ready_indices[i]]);
    }
    return 0;
}

/// Job scheduling: route VADD/VEMB work from proxy IO to a SuperNode shard queue.
static int publish_shard_job(vemb_v16_channel_t *ch,
                             const vemb_v16_job_ref_t *ref,
                             uint32_t proxy_io_worker_id,
                             vemb_v16_shard_queue_t *queues) {
    assert(ch != NULL);
    assert(ref != NULL);
    vemb_v16_proxy_t *proxy = ch->proxy;
    assert(shard_queue_topology_ready(proxy));
    assert(queues != NULL);
    assert(proxy_io_worker_id < proxy->job_shard_proxy_count);

    uint32_t supernode_id = ch->supernode_worker_id;
    assert(supernode_id < proxy->job_shard_supernode_count);
    uint32_t queue_index =
        shard_queue_index(proxy, proxy_io_worker_id, supernode_id);
    vemb_v16_aeron_ring_t *ring = &queues[queue_index].ring;

    while (vemb_v16_aeron_publish(ring, ref) != 0 &&
           atomic_load_explicit(&proxy->running, memory_order_relaxed) &&
           atomic_load_explicit(&ch->active, memory_order_acquire)) {
        /* While waiting for a supernode worker to drain this shard queue,
         * drain our return queue so the supernode worker can publish
         * completions and make forward progress. Without this, pio and snw
         * can spin forever with both rings full. */
        (void)drain_job_return_queues(proxy, proxy_io_worker_id);
        cpu_relax();
    }
#ifdef __linux__
    vemb_v16_supernode_pool_worker_t *worker =
        &proxy->supernode_workers[supernode_id];
    int expected = 1;
    if (atomic_compare_exchange_strong_explicit(&worker->job_notify_armed,
                                                &expected,
                                                0,
                                                memory_order_acq_rel,
                                                memory_order_relaxed))
        futex_notify(&worker->job_notify_armed);
#endif
    return atomic_load_explicit(&ch->active, memory_order_acquire) ? 0 : -1;
}

static int publish_shard_job_batch(vemb_v16_channel_t *ch,
                                   const vemb_v16_job_ref_t *refs,
                                   uint32_t ref_count,
                                   uint32_t proxy_io_worker_id,
                                   vemb_v16_shard_queue_t *queues) {
    assert(ch != NULL);
    assert(refs != NULL || ref_count == 0);
    vemb_v16_proxy_t *proxy = ch->proxy;
    assert(shard_queue_topology_ready(proxy));
    assert(queues != NULL);
    assert(proxy_io_worker_id < proxy->job_shard_proxy_count);
    RETURN_IF(ref_count == 0, 0);

    uint32_t supernode_id = ch->supernode_worker_id;
    assert(supernode_id < proxy->job_shard_supernode_count);
    uint32_t queue_index =
        shard_queue_index(proxy, proxy_io_worker_id, supernode_id);
    vemb_v16_aeron_ring_t *ring = &queues[queue_index].ring;

    while (vemb_v16_aeron_publish_batch(ring, refs, ref_count) != 0 &&
           atomic_load_explicit(&proxy->running, memory_order_relaxed) &&
           atomic_load_explicit(&ch->active, memory_order_acquire)) {
        (void)drain_job_return_queues(proxy, proxy_io_worker_id);
        cpu_relax();
    }
#ifdef __linux__
    vemb_v16_supernode_pool_worker_t *worker =
        &proxy->supernode_workers[supernode_id];
    int expected = 1;
    if (atomic_compare_exchange_strong_explicit(&worker->job_notify_armed,
                                                &expected,
                                                0,
                                                memory_order_acq_rel,
                                                memory_order_relaxed))
        futex_notify(&worker->job_notify_armed);
#endif
    return atomic_load_explicit(&ch->active, memory_order_acquire) ? 0 : -1;
}

static void fill_job_base(vemb_v16_job_base_t *base,
                          vemb_v16_job_kind_t kind,
                          vemb_v16_channel_t *ch,
                          const vemb_v16_req_t *req) {
    *base = (vemb_v16_job_base_t){
        .kind = (uint8_t)kind,
        .op = req->op,
        .flags = req->flags,
        .req_id = req->req_id,
        .channel_index = ch->index,
        .channel_id = ch->channel_id,
        .key_hash = req->key_hash,
        .topology_epoch = req->topology_epoch,
    };
}

static uint16_t job_pool_type_for_op(uint8_t op) {
    switch (op) {
    case VEMB_V16_OP_PING:
    case VEMB_V16_OP_VEMB_HANDLE:
    case VEMB_V16_OP_VEMB_INLINE:
    case VEMB_V16_OP_VREM:
        return VEMB_V16_JOB_POOL_READ;
    case VEMB_V16_OP_VSIM_KEY_KEY:
        return VEMB_V16_JOB_POOL_VSIM_KEY_KEY;
    case VEMB_V16_OP_VADD:
    case VEMB_V16_OP_VSIM_INLINE:
        return VEMB_V16_JOB_POOL_INLINE_VECTOR;
    default:
        return UINT16_MAX;
    }
}

static int job_pool_alloc_slot(vemb_v16_job_pool_t *pool, uint32_t *slot_id) {
    assert(pool != NULL);
    assert(slot_id != NULL);
    RETURN_IF(pool->free_count == 0, -1);
    *slot_id = pool->free_stack[--pool->free_count];
    vemb_v16_job_slot_t *slot = job_pool_slot(pool, *slot_id);
    atomic_store_explicit(&slot->hdr.state,
                          VEMB_V16_JOB_SLOT_RESERVED,
                          memory_order_relaxed);
    job_pool_slot_reset(pool, *slot_id);
    return 0;
}

static void job_pool_release_slot(vemb_v16_job_pool_t *pool,
                                  uint32_t slot_id,
                                  int bump_generation) {
    assert(pool != NULL);
    assert(slot_id < pool->slot_count);
    vemb_v16_job_slot_t *slot = job_pool_slot(pool, slot_id);
    if (bump_generation)
        slot->hdr.generation++;
    slot->hdr.op = 0;
    atomic_store_explicit(&slot->hdr.state,
                          VEMB_V16_JOB_SLOT_FREE,
                          memory_order_release);
    assert(pool->free_count < pool->slot_count);
    pool->free_stack[pool->free_count++] = slot_id;
}

static const vemb_v16_job_base_t *job_slot_payload_base(
    const vemb_v16_job_pool_t *pool,
    const vemb_v16_job_slot_t *slot) {
    switch (pool->pool_type) {
    case VEMB_V16_JOB_POOL_READ:
        return &slot->u.read_job.base;
    case VEMB_V16_JOB_POOL_VSIM_KEY_KEY:
        return &slot->u.vsim_job.base;
    case VEMB_V16_JOB_POOL_INLINE_VECTOR:
        return &slot->u.inline_job.base;
    default:
        return &slot->u.base_job;
    }
}

static int fill_job_slot(vemb_v16_job_pool_t *pool,
                         uint32_t slot_id,
                         vemb_v16_channel_t *ch,
                         const vemb_v16_req_t *req,
                         uint32_t key_len,
                         int *has_inline_vector,
                         int *is_vemb_like) {
    vemb_v16_job_slot_t *slot = job_pool_slot(pool, slot_id);
    assert(has_inline_vector != NULL);
    assert(is_vemb_like != NULL);
    *has_inline_vector = 0;
    *is_vemb_like = 0;

    switch (req->op) {
    case VEMB_V16_OP_PING:
        fill_job_base(&slot->u.base_job, VEMB_V16_JOB_KIND_BASE, ch, req);
        break;
    case VEMB_V16_OP_VEMB_HANDLE:
    case VEMB_V16_OP_VEMB_INLINE:
    case VEMB_V16_OP_VREM:
        fill_job_base(&slot->u.read_job.base, VEMB_V16_JOB_KIND_READ, ch, req);
        slot->u.read_job.key_len = key_len;
        slot->u.read_job.dim = req->dim;
        slot->u.read_job.vector_bytes = req->vector_bytes;
        memcpy(slot->u.read_job.key, req->key, key_len);
        *is_vemb_like = 1;
        break;
    case VEMB_V16_OP_VSIM_KEY_KEY:
        fill_job_base(&slot->u.vsim_job.base,
                      VEMB_V16_JOB_KIND_VSIM_KEY_KEY,
                      ch,
                      req);
        slot->u.vsim_job.key_len = key_len;
        slot->u.vsim_job.key2_len = req->key2_len;
        slot->u.vsim_job.dim = req->dim;
        slot->u.vsim_job.vector_bytes = req->vector_bytes;
        slot->u.vsim_job.key2_hash = req->key2_hash;
        memcpy(slot->u.vsim_job.key, req->key, key_len);
        memcpy(slot->u.vsim_job.key2, req->key2, req->key2_len);
        *is_vemb_like = 1;
        break;
    case VEMB_V16_OP_VADD:
    case VEMB_V16_OP_VSIM_INLINE:
        fill_job_base(&slot->u.inline_job.base,
                      VEMB_V16_JOB_KIND_INLINE_VECTOR,
                      ch,
                      req);
        slot->u.inline_job.key_len = key_len;
        slot->u.inline_job.dim = req->dim;
        slot->u.inline_job.vector_bytes = req->vector_bytes;
        memcpy(slot->u.inline_job.key, req->key, key_len);
        memcpy(slot->u.inline_job.vector, req->vector, req->vector_bytes);
        *has_inline_vector = 1;
        break;
    default:
        return -1;
    }

    slot->hdr.op = req->op;
    return 0;
}

typedef struct vemb_v16_pending_job_publish {
    vemb_v16_job_pool_t *pool;
    uint32_t slot_id;
    vemb_v16_job_ref_t ref;
} vemb_v16_pending_job_publish_t;

static int prepare_request_job(vemb_v16_channel_t *ch,
                               const vemb_v16_req_t *req,
                               uint32_t key_len,
                               uint32_t proxy_io_worker_id,
                               vemb_v16_pending_job_publish_t *pending) {
    int is_ping = req->op == VEMB_V16_OP_PING;
    int has_inline_vector = 0;
    int is_vemb_like = 0;
    uint16_t pool_type = job_pool_type_for_op(req->op);
    RETURN_IF(pool_type == UINT16_MAX, -1);
    if (pool_type == VEMB_V16_JOB_POOL_READ &&
        !is_ping &&
        tcp_vemb_read_requires_inline_op(ch, req) != 0) {
        return -1;
    }
    vemb_v16_job_pool_t *pool =
        &ch->proxy->proxy_io_workers[proxy_io_worker_id].job_pools[pool_type];
    uint32_t slot_id = 0;
    if (job_pool_alloc_slot(pool, &slot_id) != 0) {
        return -1;
    }
    GOTO_IF(fill_job_slot(pool,
                          slot_id,
                          ch,
                          req,
                          key_len,
                          &has_inline_vector,
                          &is_vemb_like) != 0, release_slot);
    GOTO_IF(!is_ping && !has_inline_vector && !is_vemb_like, release_slot);
    vemb_v16_job_slot_t *slot = job_pool_slot(pool, slot_id);
    pending->pool = pool;
    pending->slot_id = slot_id;
    pending->ref = (vemb_v16_job_ref_t){
        .proxy_worker_id = (uint16_t)proxy_io_worker_id,
        .pool_type = pool_type,
        .slot_id = slot_id,
        .generation = slot->hdr.generation,
        .req_id = req->req_id,
        .op = req->op,
    };
    return 0;
release_slot:
    job_pool_release_slot(pool, slot_id, 0);
    return -1;
}

static int publish_request_job(vemb_v16_channel_t *ch,
                               const vemb_v16_req_t *req,
                               uint32_t key_len,
                               uint32_t proxy_io_worker_id) {
    vemb_v16_pending_job_publish_t pending;
    if (prepare_request_job(ch, req, key_len, proxy_io_worker_id, &pending) != 0)
        return -1;

    vemb_v16_job_slot_t *slot = job_pool_slot(pending.pool, pending.slot_id);
    atomic_store_explicit(&slot->hdr.state,
                          VEMB_V16_JOB_SLOT_PUBLISHED,
                          memory_order_release);
    if (likely(publish_shard_job(ch,
                                 &pending.ref,
                                 proxy_io_worker_id,
                                 ch->proxy->job_shard_queues) == 0)) {
        return 0;
    }
    job_pool_release_slot(pending.pool, pending.slot_id, 0);
    return -1;
}

static int tcp_vemb_read_requires_inline_op(vemb_v16_channel_t *ch,
                                            const vemb_v16_req_t *req) {
    return ch->transport_type == VEMB_V16_TRANSPORT_TCP &&
        req->op == VEMB_V16_OP_VEMB_HANDLE;
}

static void vemb_v16_proxy_handle_request_ptr_batch_internal(
    vemb_v16_channel_t *ch,
    const vemb_v16_req_t *const *reqs,
    const int *req_lens,
    int common_req_len,
    uint32_t req_count,
    uint32_t proxy_io_worker_id) {
    vemb_v16_pending_job_publish_t pending[PROXY_REQUEST_BATCH];
    vemb_v16_job_ref_t refs[PROXY_REQUEST_BATCH];
    const vemb_v16_req_t *pending_reqs[PROXY_REQUEST_BATCH];
    uint32_t pending_count = 0;

    assert(req_count <= PROXY_REQUEST_BATCH);
    for (uint32_t i = 0; i < req_count; i++) {
        const vemb_v16_req_t *req = reqs[i];
        int req_len = req_lens ? req_lens[i] : common_req_len;

        if (req->op == VEMB_V16_OP_PING) {
            if (prepare_request_job(ch, req, 0, proxy_io_worker_id,
                                    &pending[pending_count]) == 0) {
                refs[pending_count] = pending[pending_count].ref;
                pending_reqs[pending_count] = req;
                pending_count++;
            } else {
                publish_status_response(ch, req, VEMB_V16_STATUS_ERR);
            }
            continue;
        }

        uint32_t key_len = req->key_len;
        if (key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN ||
            req->channel_id != ch->channel_id) {
            publish_status_response(ch, req, VEMB_V16_STATUS_ERR);
            continue;
        }
        if (req->op == VEMB_V16_OP_VSIM_KEY_KEY &&
            (req->key2_len == 0 || req->key2_len > VEMB_V16_MAX_KEY_LEN)) {
            publish_status_response(ch, req, VEMB_V16_STATUS_ERR);
            continue;
        }

        size_t min_len = vemb_v16_req_encoded_len(req);
        if (unlikely((size_t)req_len < min_len ||
                     req->dim > VEMB_V16_MAX_DIM ||
                     req->vector_bytes > sizeof(req->vector))) {
            publish_status_response(ch, req, VEMB_V16_STATUS_ERR);
            continue;
        }
        if (prepare_request_job(ch, req, key_len, proxy_io_worker_id,
                                &pending[pending_count]) == 0) {
            refs[pending_count] = pending[pending_count].ref;
            pending_reqs[pending_count] = req;
            pending_count++;
        } else {
            publish_status_response(ch, req, VEMB_V16_STATUS_ERR);
        }
    }

    if (pending_count == 0)
        return;

    for (uint32_t i = 0; i < pending_count; i++) {
        vemb_v16_job_slot_t *slot =
            job_pool_slot(pending[i].pool, pending[i].slot_id);
        atomic_store_explicit(&slot->hdr.state,
                              VEMB_V16_JOB_SLOT_PUBLISHED,
                              memory_order_release);
    }
    if (likely(publish_shard_job_batch(ch,
                                       refs,
                                       pending_count,
                                       proxy_io_worker_id,
                                       ch->proxy->job_shard_queues) == 0)) {
        return;
    }

    for (uint32_t i = 0; i < pending_count; i++)
        job_pool_release_slot(pending[i].pool, pending[i].slot_id, 0);
    for (uint32_t i = 0; i < pending_count; i++)
        publish_status_response(ch,
                                pending_reqs[i],
                                VEMB_V16_STATUS_ERR);
}

void vemb_v16_proxy_handle_request_ptr_batch(
    vemb_v16_channel_t *ch,
    const vemb_v16_req_t *const *reqs,
    int req_len,
    uint32_t req_count,
    uint32_t proxy_io_worker_id) {
    vemb_v16_proxy_handle_request_ptr_batch_internal(ch,
                                                     reqs,
                                                     NULL,
                                                     req_len,
                                                     req_count,
                                                     proxy_io_worker_id);
}

void vemb_v16_proxy_handle_request(vemb_v16_channel_t *ch,
                    const vemb_v16_req_t *req,
                    int req_len,
                    uint32_t proxy_io_worker_id) {
    if (req->op == VEMB_V16_OP_PING) {
        if (publish_request_job(ch, req, 0, proxy_io_worker_id) != 0)
            goto error_response;
        return;
    }

    uint32_t key_len = req->key_len;
    if (key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN ||
        req->channel_id != ch->channel_id) {
        goto error_response;
    }
    if (req->op == VEMB_V16_OP_VSIM_KEY_KEY &&
        (req->key2_len == 0 || req->key2_len > VEMB_V16_MAX_KEY_LEN)) {
        goto error_response;
    }

    size_t min_len = vemb_v16_req_encoded_len(req);
    if (unlikely((size_t)req_len < min_len ||
                 req->dim > VEMB_V16_MAX_DIM ||
                 req->vector_bytes > sizeof(req->vector))) {
        goto error_response;
    }
    if (publish_request_job(ch, req, key_len, proxy_io_worker_id) != 0) {
        goto error_response;
    }
    return;

error_response:
    publish_status_response(ch, req, VEMB_V16_STATUS_ERR);
}

static void batch_context_abort_channel(vemb_v16_channel_t *ch) {
    for (uint32_t worker_id = 0;
         worker_id < ch->proxy->proxy_io_worker_count; worker_id++) {
        vemb_v16_proxy_io_worker_t *worker =
            &ch->proxy->proxy_io_workers[worker_id];
        vemb_v16_batch_context_abort_channel(
            worker->batch_contexts, VEMB_V16_BATCH_CONTEXTS_PER_PROXY_WORKER,
            ch->channel_id);
    }
}

static int publish_batch_response(vemb_v16_channel_t *ch,
                                  vemb_v16_batch_context_t *context) {
    for (uint32_t i = 0; i < context->response.item_count; i++)
        context->response.entries[i].req_id = i;
    if (vemb_v16_aeron_publish_batch_response(ch, &context->response) != 0)
        return -1;
    vemb_v16_batch_context_release(context);
    return 0;
}

static int batch_context_complete(vemb_v16_channel_t *ch,
                                  const vemb_v16_completion_t *completion) {
    uint32_t worker_id = 0;
    uint32_t context_slot = 0;
    uint32_t generation = 0;
    uint32_t item_index = 0;
    if (vemb_v16_batch_token_decode(completion->batch_token, &worker_id,
                                    &context_slot, &generation,
                                    &item_index) != 0 ||
        worker_id >= ch->proxy->proxy_io_worker_count)
        return 0;
    vemb_v16_batch_context_t *context =
        vemb_v16_batch_context_lookup(
            ch->proxy->proxy_io_workers[worker_id].batch_contexts,
            VEMB_V16_BATCH_CONTEXTS_PER_PROXY_WORKER, worker_id,
            ch->channel_id, completion->batch_token, &item_index);
    if (!context)
        return 0;
    int completed = vemb_v16_batch_context_mark_complete(context, item_index);
    if (completed <= 0)
        return 0;
    vemb_v16_make_response_from(&context->response.entries[item_index],
                                 completion);
    if (completed == 2)
        return publish_batch_response(ch, context);
    return 0;
}

int vemb_v16_proxy_handle_batch_request(
    vemb_v16_channel_t *ch, const batch_request_view_t *view,
    uint32_t proxy_io_worker_id) {
    uint64_t epoch = 0;
    vemb_v16_storage_epoch_get(ch->proxy->storage, &epoch, NULL);
    uint32_t context_slot = 0;
    vemb_v16_batch_context_t *context = vemb_v16_batch_context_acquire(
        ch->proxy->proxy_io_workers[proxy_io_worker_id].batch_contexts,
        VEMB_V16_BATCH_CONTEXTS_PER_PROXY_WORKER, ch->channel_id,
        view->batch_id, epoch, view->item_count, &context_slot);
    if (!context)
        return 0;
    if (view->item_count > ch->batch_effective_size ||
        view->topology_epoch != epoch ||
        vemb_v16_storage_migration_active(ch->proxy->storage)) {
        static _Atomic uint64_t stale_batch_rejections;
        uint64_t rejection_no = atomic_fetch_add_explicit(
            &stale_batch_rejections, 1, memory_order_relaxed) + 1;
        if (rejection_no <= 8 || rejection_no % 1024 == 0) {
            serverLog(LL_NOTICE,
                      "vemb_v16 batch rejected: no=%llu channel=%llu "
                      "batch=%llu items=%u max=%u request_epoch=%llu "
                      "storage_epoch=%llu migration_active=%d",
                      (unsigned long long)rejection_no,
                      (unsigned long long)ch->channel_id,
                      (unsigned long long)view->batch_id, view->item_count,
                      ch->batch_effective_size,
                      (unsigned long long)view->topology_epoch,
                      (unsigned long long)epoch,
                      vemb_v16_storage_migration_active(ch->proxy->storage));
        }
        for (uint32_t i = 0; i < view->item_count; i++)
            context->response.entries[i] = (vemb_v16_resp_t){
                .status = VEMB_V16_STATUS_STALE_TOPOLOGY,
                .op = VEMB_V16_OP_VEMB_HANDLE,
            };
        return publish_batch_response(ch, context) == 0 ? 1 : -1;
    }

    vemb_v16_pending_job_publish_t pending[VEMB_V16_BATCH_REQUEST_SIZE_MAX];
    vemb_v16_job_ref_t refs[VEMB_V16_BATCH_REQUEST_SIZE_MAX];
    uint16_t pending_indices[VEMB_V16_BATCH_REQUEST_SIZE_MAX];
    uint32_t pending_count = 0;
    const uint8_t *key = view->keys;
    for (uint32_t i = 0; i < view->item_count; i++) {
        uint16_t key_len = batch_request_key_len_at(view, i);
        vemb_v16_req_t req = {
            .op = VEMB_V16_OP_VEMB_HANDLE,
            .req_id = i,
            .channel_id = ch->channel_id,
            .key_len = key_len,
            .topology_epoch = epoch,
            .dim = ch->proxy->vector_dim,
            .vector_bytes = ch->proxy->vector_stride,
        };
        memcpy(req.key, key, key_len);
        req.key_hash = vemb_v16_xxh3_64_str(req.key, key_len);
        key += key_len;
        if (prepare_request_job(ch, &req, key_len, proxy_io_worker_id,
                                &pending[pending_count]) == 0) {
            vemb_v16_job_slot_t *slot =
                job_pool_slot(pending[pending_count].pool,
                              pending[pending_count].slot_id);
            slot->u.read_job.base.batch_token = vemb_v16_batch_token_make(
                proxy_io_worker_id, context_slot, context->generation, i);
            refs[pending_count] = pending[pending_count].ref;
            pending_indices[pending_count++] = (uint16_t)i;
            context->pending_count++;
        } else {
            context->response.entries[i] = (vemb_v16_resp_t){
                .status = VEMB_V16_STATUS_ERR,
                .op = VEMB_V16_OP_VEMB_HANDLE,
            };
        }
    }
    for (uint32_t i = 0; i < pending_count; i++) {
        vemb_v16_job_slot_t *slot =
            job_pool_slot(pending[i].pool, pending[i].slot_id);
        atomic_store_explicit(&slot->hdr.state, VEMB_V16_JOB_SLOT_PUBLISHED,
                              memory_order_release);
    }
    if (pending_count != 0 && publish_shard_job_batch(
            ch, refs, pending_count, proxy_io_worker_id,
            ch->proxy->job_shard_queues) != 0) {
        for (uint32_t i = 0; i < pending_count; i++) {
            job_pool_release_slot(pending[i].pool, pending[i].slot_id, 0);
            context->response.entries[pending_indices[i]] =
                (vemb_v16_resp_t){ .status = VEMB_V16_STATUS_ERR,
                                    .op = VEMB_V16_OP_VEMB_HANDLE };
        }
        pending_count = 0;
        context->pending_count = 0;
    }
    if (pending_count == 0)
        return publish_batch_response(ch, context) == 0 ? 1 : -1;
    return 1;
}

/// Response scheduling: drain SuperNode completions and publish by transport.
static int drain_completions(vemb_v16_channel_t *ch) {
    vemb_v16_completion_t completions[PROXY_RESPONSE_BATCH];
    uint16_t ready_indices[PROXY_RESPONSE_BATCH];
    uint32_t n;
    uint32_t total = 0;
    while ((n = vemb_v16_aeron_poll_batch(&ch->completion_ring,
                                          completions,
                                          PROXY_RESPONSE_BATCH)) != 0) {
        total += n;
        if (ch->batch_v2) {
            for (uint32_t i = 0; i < n; i++) {
                vemb_v16_completion_t *completion = &completions[i];
                int rc = batch_context_complete(ch, completion);
                completion_release_payload(completion);
                if (rc != 0)
                    return -1;
            }
            continue;
        }
        uint32_t ready_count = 0;
        for (uint32_t i = 0; i < n; i++) {
            if (completions[i].channel_id != ch->channel_id ||
                !atomic_load_explicit(&ch->active, memory_order_acquire)) {
                completion_release_payload(&completions[i]);
                continue;
            }
            ready_indices[ready_count++] = (uint16_t)i;
        }
        if (publish_completion_batch(ch,
                                     completions,
                                     ready_indices,
                                     ready_count) != 0) {
            return -1;
        }
    }
    return (int)total;
}

/// Control plane: fill the channel descriptor returned to TCP or UB/SHM clients.
static void fill_channel_desc(vemb_v16_proxy_t *proxy,
                              vemb_v16_channel_t *ch,
                              vemb_v16_channel_desc_t *desc) {
    memset(desc, 0, sizeof(*desc));
    desc->magic = VEMB_V16_MAGIC;
    desc->version = VEMB_V16_VERSION;
    desc->channel_id = ch->channel_id;
    desc->channel_index = ch->index;
    desc->vector_dim = proxy->vector_dim;
    desc->vector_stride = proxy->vector_stride;
    desc->max_vectors = proxy->max_vectors;
    desc->request_ring_slot_size = proxy->request_ring_slot_size;
    desc->response_ring_slot_size = proxy->response_ring_slot_size;
    strncpy(desc->request_ring_name, ch->request_ring_name,
            sizeof(desc->request_ring_name) - 1);
    strncpy(desc->response_ring_name, ch->response_ring_name,
            sizeof(desc->response_ring_name) - 1);
    vemb_v16_storage_fill_channel_desc(proxy_storage(proxy), desc);
}

static void cleanup_unstarted_channel(vemb_v16_channel_t *ch) {
    assert(ch != NULL);
    atomic_store_explicit(&ch->slot_channel_id, 0, memory_order_release);
    atomic_store_explicit(&ch->active, 0, memory_order_release);
    if (ch->transport_type == VEMB_V16_TRANSPORT_AERON) {
        vemb_v16_storage_free_aeron_channel(ch->request_ring,
                                            ch->request_ring_bytes,
                                            ch->response_ring,
                                            ch->response_ring_bytes);
        if (ch->batch_v2) {
            vemb_v16_storage_free_aeron_channel(
                ch->batch_allocation.request_arena_mapping,
                ch->batch_allocation.request_arena_bytes,
                ch->batch_allocation.response_arena_mapping,
                ch->batch_allocation.response_arena_bytes);
        }
    }
    if (ch->net_fd >= 0) {
        shutdown(ch->net_fd, SHUT_RDWR);
        close(ch->net_fd);
    }
    vemb_v16_channel_free_slots(ch);
    reset_closed_channel(ch);
}

/// Control plane: allocate channel state for either TCP sockets or UB rings.
static int alloc_channel_common_locked(vemb_v16_proxy_t *proxy,
                                        uint32_t transport_type,
                                        int net_fd,
                                        vemb_v16_channel_desc_t *desc) {
    uint32_t idx = VEMB_V16_MAX_CHANNELS;
    uint32_t start = atomic_fetch_add_explicit(&proxy->next_channel_index, 1,
                                               memory_order_relaxed);
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        uint32_t candidate = (start + i) % VEMB_V16_MAX_CHANNELS;
        if (atomic_load_explicit(&proxy->channels[candidate].slot_channel_id,
                                 memory_order_acquire) == 0) {
            idx = candidate;
            break;
        }
    }
    if (idx >= VEMB_V16_MAX_CHANNELS) {
        if (transport_type == VEMB_V16_TRANSPORT_TCP && net_fd >= 0)
            close(net_fd);
        return -1;
    }

    vemb_v16_channel_t *ch = &proxy->channels[idx];
    reset_closed_channel(ch);
    ch->index = idx;
    ch->channel_id = atomic_fetch_add_explicit(&proxy->next_channel_id, 1,
                                               memory_order_relaxed);
    ch->proxy = proxy;
    ch->transport_type = transport_type;
    ch->net_fd = net_fd;
    assign_channel_owners(proxy, ch, 0);
    atomic_store_explicit(&ch->active, 0, memory_order_release);
    atomic_store_explicit(&ch->proxy_io_registered, 0, memory_order_release);
    atomic_store_explicit(&ch->proxy_io_state, 0, memory_order_release);
    atomic_store_explicit(&ch->supernode_state, 0, memory_order_release);
    if (posix_memalign(&ch->completion_slots, 64,
                       sizeof(vemb_v16_completion_t) *
                       VEMB_V16_COMPLETION_RING_SIZE) != 0) {
        cleanup_unstarted_channel(ch);
        return -1;
    }
    if (vemb_v16_aeron_ring_init(&ch->completion_ring,
                                 ch->completion_slots,
                                 sizeof(vemb_v16_completion_t),
                                 VEMB_V16_COMPLETION_RING_SIZE) != 0) {
        cleanup_unstarted_channel(ch);
        return -1;
    }
    if (transport_type == VEMB_V16_TRANSPORT_AERON) {
        char request_ub_path[256], response_ub_path[256];
        uint64_t req_off = 0, resp_off = 0;
        void *req_ring = NULL, *resp_ring = NULL;
        size_t req_bytes = 0, resp_bytes = 0;
        if (vemb_v16_storage_alloc_aeron_channel(
                proxy->aeron_ub_path,
                proxy->aeron_response_ub_path,
                VEMB_V16_UB_CACHE_POLICY_CACHEABLE,
                proxy->request_ring_slot_size,
                proxy->response_ring_slot_size,
                VEMB_V16_CLIENT_RING_SIZE,
                request_ub_path,
                response_ub_path,
                &req_off,
                &resp_off,
                &req_ring,
                &resp_ring,
                &req_bytes,
                &resp_bytes) != 0) {
            serverLog(LL_WARNING,
                      "vemb_v16 aeron UB ring allocation failed: path=%s",
                      proxy->aeron_ub_path);
            cleanup_unstarted_channel(ch);
            return -1;
        }
        ch->request_ring = (vemb_v16_client_ring_t *)req_ring;
        ch->response_ring = (vemb_v16_client_ring_t *)resp_ring;
        ch->request_ring_bytes = req_bytes;
        ch->response_ring_bytes = resp_bytes;
        snprintf(ch->request_ring_name, sizeof(ch->request_ring_name),
                 "%s@off%llu", request_ub_path, (unsigned long long)req_off);
        snprintf(ch->response_ring_name, sizeof(ch->response_ring_name),
                 "%s@off%llu", response_ub_path, (unsigned long long)resp_off);
    }

    ch->supernode_ctx = (vemb_v16_supernode_ctx_t){
        .worker_id = ch->supernode_worker_id,
        .channel_active = &ch->active,
        .running = &proxy->running,
        .completion_notify_armed = NULL,
        .completion_notify_fd = NULL,
        .completion_ring = &ch->completion_ring,
        .storage = proxy->storage,
        .stats = &ch->stats,
    };

    atomic_store_explicit(&ch->active, 1, memory_order_release);

    int pooled_proxy_io = proxy->proxy_io_worker_count != 0;
#ifdef __linux__
    ch->tcp_backpressure_enabled =
        transport_type == VEMB_V16_TRANSPORT_TCP && pooled_proxy_io;
#else
    ch->tcp_backpressure_enabled = 0;
#endif
    if (pooled_proxy_io && transport_type == VEMB_V16_TRANSPORT_TCP) {
        ch->supernode_ctx.completion_notify_armed =
            &ch->completion_notify_armed;
#ifdef __linux__
        uint32_t worker_id = ch->proxy_io_worker_id;
        assert(worker_id < proxy->proxy_io_worker_count);
        ch->supernode_ctx.completion_notify_fd =
            &proxy->proxy_io_workers[worker_id].notify_fd;
#endif
    }
    atomic_store_explicit(&ch->resource_generation,
                          transport_type == VEMB_V16_TRANSPORT_AERON ?
                              ch->channel_id : 0,
                          memory_order_release);
    atomic_store_explicit(&ch->slot_channel_id, ch->channel_id,
                          memory_order_release);
    if (proxy_aeron_channel_snapshot_update(proxy, ch, 1) != 0) {
        serverLog(LL_WARNING,
                  "vemb_v16 aeron channel snapshot is full: index=%u",
                  ch->index);
        cleanup_unstarted_channel(ch);
        return -1;
    }

    serverLog(LL_VERBOSE, "vemb_v16 channel allocated: index=%u channel_id=%llu transport=%s req=%s resp=%s",
              ch->index,
              (unsigned long long)ch->channel_id,
              vemb_v16_transport_name(ch->transport_type),
              ch->request_ring_name,
              ch->response_ring_name);

    if (desc) fill_channel_desc(proxy, ch, desc);
    return 0;
}

/// UB/SHM control plane: allocate a shared-memory client channel.
int vemb_v16_proxy_alloc_shm_channel(vemb_v16_proxy_t *proxy, vemb_v16_channel_desc_t *desc) {
    pthread_mutex_lock(&proxy->channel_lifecycle_lock);
    int rc = alloc_channel_common_locked(proxy, VEMB_V16_TRANSPORT_AERON,
                                          -1, desc);
    pthread_mutex_unlock(&proxy->channel_lifecycle_lock);
    return rc;
}

/// TCP control plane: attach an accepted socket to a channel.
int vemb_v16_proxy_alloc_tcp_channel(vemb_v16_proxy_t *proxy,
                      int net_fd,
                      vemb_v16_channel_desc_t *desc) {
    pthread_mutex_lock(&proxy->channel_lifecycle_lock);
    int rc = alloc_channel_common_locked(proxy,
                                          VEMB_V16_TRANSPORT_TCP,
                                          net_fd,
                                          desc);
    pthread_mutex_unlock(&proxy->channel_lifecycle_lock);
    return rc;
}

/// Cross-node aeron control plane: adopt pre-built shmdev rings into a
/// new proxy channel.  Mirrors alloc_channel_common minus the ring
/// creation step (rings already exist in the shmdev mapping).
static int attach_cross_node_channel_locked(
                                             vemb_v16_proxy_t *proxy,
                                             void *req_ring, void *resp_ring,
                                             uint32_t req_slot, uint32_t resp_slot,
                                             const char *req_path,
                                             const char *resp_path,
                                             uint64_t req_off, uint64_t resp_off,
                                             uint64_t *out_channel_id) {
    /* Slot hunt - same logic as alloc_channel_common. We can't easily
     * refactor alloc_channel_common to accept pre-built rings, so
     * duplicate the slot hunt + channel setup minus the ring creation. */
    uint32_t idx = VEMB_V16_MAX_CHANNELS;
    uint32_t start = atomic_fetch_add_explicit(&proxy->next_channel_index, 1,
                                               memory_order_relaxed);
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        uint32_t candidate = (start + i) % VEMB_V16_MAX_CHANNELS;
        if (atomic_load_explicit(&proxy->channels[candidate].slot_channel_id,
                                 memory_order_acquire) == 0) {
            idx = candidate; break;
        }
    }
    if (idx >= VEMB_V16_MAX_CHANNELS) return -1;

    vemb_v16_channel_t *ch = &proxy->channels[idx];
    reset_closed_channel(ch);
    ch->index = idx;
    ch->channel_id = atomic_fetch_add_explicit(&proxy->next_channel_id, 1,
                                               memory_order_relaxed);
    ch->proxy = proxy;
    ch->transport_type = VEMB_V16_TRANSPORT_AERON;  /* reuse aeron data path */
    ch->net_fd = -1;
    assign_channel_owners(proxy, ch, 0);
    atomic_store_explicit(&ch->active, 0, memory_order_release);
    atomic_store_explicit(&ch->proxy_io_registered, 0, memory_order_release);
    atomic_store_explicit(&ch->proxy_io_state, 0, memory_order_release);
    atomic_store_explicit(&ch->supernode_state, 0, memory_order_release);

    if (posix_memalign(&ch->completion_slots, 64,
                       sizeof(vemb_v16_completion_t) *
                       VEMB_V16_COMPLETION_RING_SIZE) != 0) {
        cleanup_unstarted_channel(ch); return -1;
    }
    if (vemb_v16_aeron_ring_init(&ch->completion_ring,
                                 ch->completion_slots,
                                 sizeof(vemb_v16_completion_t),
                                 VEMB_V16_COMPLETION_RING_SIZE) != 0) {
        cleanup_unstarted_channel(ch); return -1;
    }

    /* Cross-node path: rings were already created by storage layer.
     * Adopt the mappings directly. */
    ch->request_ring       = (vemb_v16_client_ring_t *)req_ring;
    ch->response_ring      = (vemb_v16_client_ring_t *)resp_ring;
    ch->request_ring_bytes  = vemb_v16_client_ring_bytes(req_slot);
    ch->response_ring_bytes = vemb_v16_client_ring_bytes(resp_slot);
    snprintf(ch->request_ring_name, sizeof(ch->request_ring_name),
             "%s@off%llu", req_path, (unsigned long long)req_off);
    snprintf(ch->response_ring_name, sizeof(ch->response_ring_name),
             "%s@off%llu", resp_path, (unsigned long long)resp_off);

    ch->supernode_ctx = (vemb_v16_supernode_ctx_t){
        .worker_id = ch->supernode_worker_id,
        .channel_active = &ch->active,
        .running = &proxy->running,
        .completion_notify_armed = NULL,
        .completion_notify_fd = NULL,
        .completion_ring = &ch->completion_ring,
        .storage = proxy->storage,
        .stats = &ch->stats,
    };
    atomic_store_explicit(&ch->active, 1, memory_order_release);

    int pooled_proxy_io = proxy->proxy_io_worker_count != 0;
    ch->tcp_backpressure_enabled = 0;  /* not TCP transport */
    if (pooled_proxy_io && ch->transport_type == VEMB_V16_TRANSPORT_TCP) {
        ch->supernode_ctx.completion_notify_armed = &ch->completion_notify_armed;
#ifdef __linux__
        uint32_t worker_id = ch->proxy_io_worker_id;
        assert(worker_id < proxy->proxy_io_worker_count);
        ch->supernode_ctx.completion_notify_fd =
            &proxy->proxy_io_workers[worker_id].notify_fd;
#endif
    }
    atomic_store_explicit(&ch->resource_generation, ch->channel_id,
                          memory_order_release);
    atomic_store_explicit(&ch->slot_channel_id, ch->channel_id,
                          memory_order_release);
    if (proxy_aeron_channel_snapshot_update(proxy, ch, 1) != 0) {
        serverLog(LL_WARNING,
                  "vemb_v16 cross-node aeron channel snapshot is full: index=%u",
                  ch->index);
        cleanup_unstarted_channel(ch);
        return -1;
    }

    if (out_channel_id) *out_channel_id = ch->channel_id;
    serverLog(LL_VERBOSE,
              "vemb_v16 cross-node channel allocated: idx=%u cid=%llu shmdev=%s "
              "req_off=%llu resp_off=%llu",
              ch->index, (unsigned long long)ch->channel_id,
              req_path, (unsigned long long)req_off,
              (unsigned long long)resp_off);
    return 0;
}

int vemb_v16_proxy_attach_cross_node_channel(vemb_v16_proxy_t *proxy,
                                             void *req_ring, void *resp_ring,
                                             uint32_t req_slot, uint32_t resp_slot,
                                             const char *req_path,
                                             const char *resp_path,
                                             uint64_t req_off, uint64_t resp_off,
                                             uint64_t *out_channel_id) {
    pthread_mutex_lock(&proxy->channel_lifecycle_lock);
    int rc = attach_cross_node_channel_locked(
        proxy, req_ring, resp_ring, req_slot, resp_slot, req_path, resp_path,
        req_off, resp_off, out_channel_id);
    pthread_mutex_unlock(&proxy->channel_lifecycle_lock);
    return rc;
}

static int attach_cross_node_batch_channel_locked(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_aeron_batch_channel_allocation_t *allocation,
    uint32_t effective_batch_size, uint32_t max_batch_bytes,
    uint64_t *out_channel_id) {
    if (!proxy || !allocation || !allocation->request_desc_mapping ||
        !allocation->request_arena_mapping ||
        !allocation->response_desc_mapping ||
        !allocation->response_arena_mapping ||
        allocation->request_desc_bytes == 0 ||
        allocation->request_arena_bytes == 0 ||
        allocation->response_desc_bytes == 0 ||
        allocation->response_arena_bytes == 0 ||
        effective_batch_size == 0 ||
        effective_batch_size > VEMB_V16_BATCH_REQUEST_SIZE_MAX ||
        max_batch_bytes == 0 ||
        max_batch_bytes != allocation->request_arena_bytes ||
        max_batch_bytes != allocation->response_arena_bytes)
        return -1;
    uint32_t idx = VEMB_V16_MAX_CHANNELS;
    uint32_t start = atomic_fetch_add_explicit(&proxy->next_channel_index, 1,
                                               memory_order_relaxed);
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        uint32_t candidate = (start + i) % VEMB_V16_MAX_CHANNELS;
        if (atomic_load_explicit(&proxy->channels[candidate].slot_channel_id,
                                 memory_order_acquire) == 0) {
            idx = candidate;
            break;
        }
    }
    if (idx == VEMB_V16_MAX_CHANNELS)
        return -1;

    vemb_v16_channel_t *ch = &proxy->channels[idx];
    reset_closed_channel(ch);
    ch->index = idx;
    ch->channel_id = atomic_fetch_add_explicit(&proxy->next_channel_id, 1,
                                               memory_order_relaxed);
    ch->proxy = proxy;
    ch->transport_type = VEMB_V16_TRANSPORT_AERON;
    ch->net_fd = -1;
    assign_channel_owners(proxy, ch, 1);
    atomic_store_explicit(&ch->active, 0, memory_order_release);
    atomic_store_explicit(&ch->proxy_io_registered, 0, memory_order_release);
    atomic_store_explicit(&ch->proxy_io_state, 0, memory_order_release);
    atomic_store_explicit(&ch->supernode_state, 0, memory_order_release);
    if (posix_memalign(&ch->completion_slots, 64,
                       sizeof(vemb_v16_completion_t) *
                       VEMB_V16_COMPLETION_RING_SIZE) != 0) {
        cleanup_unstarted_channel(ch);
        return -1;
    }
    if (vemb_v16_aeron_ring_init(&ch->completion_ring, ch->completion_slots,
                                 sizeof(vemb_v16_completion_t),
                                 VEMB_V16_COMPLETION_RING_SIZE) != 0) {
        cleanup_unstarted_channel(ch);
        return -1;
    }
    ch->request_ring = allocation->request_desc_mapping;
    ch->response_ring = allocation->response_desc_mapping;
    ch->request_ring_bytes = allocation->request_desc_bytes;
    ch->response_ring_bytes = allocation->response_desc_bytes;
    ch->batch_v2 = 1;
    ch->batch_allocation = *allocation;
    ch->batch_effective_size = effective_batch_size;
    ch->batch_max_bytes = max_batch_bytes;
    batch_arena_producer_init(&ch->batch_response_producer);
    ch->supernode_ctx = (vemb_v16_supernode_ctx_t){
        .worker_id = ch->supernode_worker_id,
        .channel_active = &ch->active,
        .running = &proxy->running,
        .completion_ring = &ch->completion_ring,
        .storage = proxy->storage,
        .stats = &ch->stats,
    };
    atomic_store_explicit(&ch->active, 1, memory_order_release);
    atomic_store_explicit(&ch->resource_generation, ch->channel_id,
                          memory_order_release);
    atomic_store_explicit(&ch->slot_channel_id, ch->channel_id,
                          memory_order_release);
    if (proxy_aeron_channel_snapshot_update(proxy, ch, 1) != 0) {
        cleanup_unstarted_channel(ch);
        return -1;
    }
    if (out_channel_id)
        *out_channel_id = ch->channel_id;
    return 0;
}

int vemb_v16_proxy_attach_cross_node_batch_channel(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_aeron_batch_channel_allocation_t *allocation,
    uint32_t effective_batch_size, uint32_t max_batch_bytes,
    uint64_t *out_channel_id) {
    pthread_mutex_lock(&proxy->channel_lifecycle_lock);
    int rc = attach_cross_node_batch_channel_locked(
        proxy, allocation, effective_batch_size, max_batch_bytes,
        out_channel_id);
    pthread_mutex_unlock(&proxy->channel_lifecycle_lock);
    return rc;
}

/// Control plane: close a channel and wait for proxy IO/SuperNode users to leave.
/// The caller holds proxy->channel_lifecycle_lock.
static void close_channel_locked(vemb_v16_channel_t *ch) {
    RETURN_IF(atomic_load_explicit(&ch->slot_channel_id,
                                  memory_order_acquire) == 0);
    serverLog(LL_NOTICE,
              "vemb_v16 channel closing: index=%u channel_id=%llu transport=%u net_fd=%d active=%d",
              ch->index,
              (unsigned long long)ch->channel_id,
              ch->transport_type,
              ch->net_fd,
              atomic_load_explicit(&ch->active, memory_order_acquire));
    int pooled_proxy_io = ch->proxy->proxy_io_worker_count != 0;
    int pooled_supernode = ch->proxy->supernode_worker_count != 0;
    atomic_exchange_explicit(&ch->active, 0, memory_order_acq_rel);
    if (pooled_proxy_io)
        proxy_io_channel_close_begin(ch);
    if (pooled_supernode)
        supernode_channel_close_begin(ch);
    if (ch->transport_type == VEMB_V16_TRANSPORT_AERON)
        (void)proxy_aeron_channel_snapshot_update(ch->proxy, ch, 0);
    if (ch->transport_type == VEMB_V16_TRANSPORT_TCP && ch->net_fd >= 0)
        shutdown(ch->net_fd, SHUT_RDWR);
    if (pooled_proxy_io)
        proxy_io_channel_wait_closed(ch);
    if (pooled_supernode) {
        supernode_channel_wait_closed(ch);
    }
    if (ch->batch_v2)
        batch_context_abort_channel(ch);
    pthread_mutex_lock(&ch->proxy->stats_lock);
    vemb_v16_stats_add_channel_counters(&ch->proxy->closed_stats, &ch->stats);
    pthread_mutex_unlock(&ch->proxy->stats_lock);
    if (ch->transport_type == VEMB_V16_TRANSPORT_AERON) {
        vemb_v16_storage_free_aeron_channel(ch->request_ring,
                                            ch->request_ring_bytes,
                                            ch->response_ring,
                                            ch->response_ring_bytes);
        if (ch->batch_v2) {
            vemb_v16_storage_free_aeron_channel(
                ch->batch_allocation.request_arena_mapping,
                ch->batch_allocation.request_arena_bytes,
                ch->batch_allocation.response_arena_mapping,
                ch->batch_allocation.response_arena_bytes);
        }
    }
    if (ch->net_fd >= 0) {
        close(ch->net_fd);
        ch->net_fd = -1;
    }
    vemb_v16_channel_free_slots(ch);
    reset_closed_channel(ch);
}

static void close_channel(vemb_v16_channel_t *ch) {
    pthread_mutex_lock(&ch->proxy->channel_lifecycle_lock);
    close_channel_locked(ch);
    pthread_mutex_unlock(&ch->proxy->channel_lifecycle_lock);
}

int vemb_v16_proxy_close_channel_by_id(vemb_v16_proxy_t *proxy, uint64_t channel_id) {
    assert(proxy != NULL);
    RETURN_IF(channel_id == 0, -1);
    pthread_mutex_lock(&proxy->channel_lifecycle_lock);
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        vemb_v16_channel_t *ch = &proxy->channels[i];
        if (atomic_load_explicit(&ch->slot_channel_id,
                                 memory_order_acquire) == channel_id) {
            close_channel_locked(ch);
            pthread_mutex_unlock(&proxy->channel_lifecycle_lock);
            return 0;
        }
    }
    pthread_mutex_unlock(&proxy->channel_lifecycle_lock);
    return -1;
}

int vemb_v16_proxy_aeron_channel_resource_generation(
    vemb_v16_proxy_t *proxy, uint64_t channel_id,
    uint64_t *out_resource_generation)
{
    assert(proxy != NULL);
    assert(out_resource_generation != NULL);
    RETURN_IF(channel_id == 0, -1);
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        vemb_v16_channel_t *ch = &proxy->channels[i];
        if (atomic_load_explicit(&ch->slot_channel_id,
                                 memory_order_acquire) != channel_id)
            continue;
        uint64_t generation = atomic_load_explicit(&ch->resource_generation,
                                                    memory_order_acquire);
        if (generation == 0 ||
            !atomic_load_explicit(&ch->active, memory_order_acquire))
            return -1;
        *out_resource_generation = generation;
        return 0;
    }
    return -1;
}

uint64_t vemb_v16_proxy_close_all_channels(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    pthread_mutex_lock(&proxy->channel_lifecycle_lock);
    uint64_t closed = 0;
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        vemb_v16_channel_t *ch = &proxy->channels[i];
        if (atomic_load_explicit(&ch->slot_channel_id,
                                 memory_order_acquire) != 0) {
            close_channel_locked(ch);
            closed++;
        }
    }
    pthread_mutex_unlock(&proxy->channel_lifecycle_lock);
    return closed;
}

static void reap_inactive_tcp_channels(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        vemb_v16_channel_t *ch = &proxy->channels[i];
        if (atomic_load_explicit(&ch->slot_channel_id,
                                 memory_order_acquire) != 0 &&
            ch->transport_type == VEMB_V16_TRANSPORT_TCP &&
            !atomic_load_explicit(&ch->active, memory_order_acquire)) {
            close_channel(ch);
        }
    }
}

/// Scheduling cleanup: mark a TCP or UB/SHM channel inactive from proxy IO.
static void proxy_io_channel_deactivate(vemb_v16_channel_t *ch) {
    assert(ch != NULL);
    proxy_io_channel_disarm_completion_notify(ch);
    atomic_store_explicit(&ch->active, 0, memory_order_release);
    if (ch->transport_type == VEMB_V16_TRANSPORT_TCP && ch->net_fd >= 0)
        shutdown(ch->net_fd, SHUT_RDWR);
}

#define VEMB_V16_PROXY_AFFINITY_INTERLEAVED 0
#define VEMB_V16_PROXY_AFFINITY_GROUPED 1

#ifndef VEMB_V16_PROXY_AFFINITY_MODE
#define VEMB_V16_PROXY_AFFINITY_MODE VEMB_V16_PROXY_AFFINITY_INTERLEAVED
#endif

#if VEMB_V16_PROXY_AFFINITY_MODE != VEMB_V16_PROXY_AFFINITY_INTERLEAVED && \
    VEMB_V16_PROXY_AFFINITY_MODE != VEMB_V16_PROXY_AFFINITY_GROUPED
#error "VEMB_V16_PROXY_AFFINITY_MODE must be 0 (interleaved) or 1 (grouped)"
#endif

static const char *proxy_worker_affinity_mode_name(void) {
#if VEMB_V16_PROXY_AFFINITY_MODE == VEMB_V16_PROXY_AFFINITY_GROUPED
    return "grouped";
#else
    return "interleaved";
#endif
}

#ifdef __linux__

static uint32_t proxy_get_available_cpus(int *cpus, uint32_t cap) {
    if (cap == 0)
        return 0;

    cpu_set_t allowed;
    if (sched_getaffinity(0, sizeof(allowed), &allowed) != 0)
        return 0;

    uint32_t count = 0;
    for (int cpu = 0; cpu < CPU_SETSIZE && count < cap; cpu++) {
        if (CPU_ISSET(cpu, &allowed))
            cpus[count++] = cpu;
    }
    return count;
}

static uint32_t proxy_available_cpu_count(void) {
    int cpus[CPU_SETSIZE];
    uint32_t count = proxy_get_available_cpus(cpus, CPU_SETSIZE);
    if (count != 0)
        return count;

    long online = sysconf(_SC_NPROCESSORS_ONLN);
    return online > 0 ? (uint32_t)online : 1u;
}

static void proxy_set_worker_affinity(vemb_v16_proxy_t *proxy,
                                      uint32_t worker_id,
                                      uint32_t lane) {
    int cpus[CPU_SETSIZE];
    uint32_t cpu_count = proxy_get_available_cpus(cpus, CPU_SETSIZE);
    uint32_t required;
#if VEMB_V16_PROXY_AFFINITY_MODE == VEMB_V16_PROXY_AFFINITY_GROUPED
    required = proxy->proxy_io_worker_count + proxy->supernode_worker_count;
#else
    required = 2u * proxy->proxy_io_worker_count;
    if (proxy->supernode_worker_count > proxy->proxy_io_worker_count)
        required = 2u * proxy->supernode_worker_count;
#endif
    if (cpu_count == 0 || required > cpu_count)
        return;

#if VEMB_V16_PROXY_AFFINITY_MODE == VEMB_V16_PROXY_AFFINITY_GROUPED
    uint32_t cpu_index = lane == 0
        ? worker_id
        : proxy->proxy_io_worker_count + worker_id;
#else
    uint32_t cpu_index = worker_id * 2u + lane;
#endif
    if (cpu_index >= cpu_count)
        return;

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpus[cpu_index], &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
}

#endif

static void proxy_io_set_affinity(vemb_v16_proxy_t *proxy, uint32_t worker_id) {
#ifdef __linux__
    proxy_set_worker_affinity(proxy, worker_id, 0);
#else
    (void)proxy;
    (void)worker_id;
#endif
}

static void supernode_worker_set_affinity(vemb_v16_proxy_t *proxy,
                                          uint32_t worker_id) {
#ifdef __linux__
    proxy_set_worker_affinity(proxy, worker_id, 1);
#else
    (void)proxy;
    (void)worker_id;
#endif
}

/* Aeron workers own dedicated polling lanes.  Yielding to the scheduler on
 * every empty channel sweep turns an otherwise userspace poll into a syscall
 * storm.  Keep the hot idle phase in userspace, then use a short sleep only
 * after a genuinely idle run so shutdown and sparse traffic still make
 * progress without burning a full core indefinitely. */
#define VEMB_V16_AERON_IO_SPIN_ROUNDS 256u

static void aeron_io_tiny_pause(uint32_t idle_rounds) {
    if (idle_rounds < VEMB_V16_AERON_IO_SPIN_ROUNDS) {
        for (uint32_t i = 0; i < 64; i++)
            cpu_relax();
        return;
    }

    struct timespec ts = {0, 1000}; /* 1 us: no epoll or scheduler yield */
    nanosleep(&ts, NULL);
}

/// Proxy IO scheduling: directly poll UB/SHM client rings.
static void *proxy_io_aeron_poll_thread_main(void *arg) {
    vemb_v16_proxy_io_worker_t *worker = arg;
    vemb_v16_proxy_t *proxy = worker->proxy;
#ifdef __linux__
    char thread_name[16];
    snprintf(thread_name, sizeof(thread_name), "vemb-io-%02u", worker->worker_id);
    (void)pthread_setname_np(pthread_self(), thread_name);
#endif
    proxy_io_set_affinity(proxy, worker->worker_id);
    serverLog(LL_VERBOSE,
              "vemb_v16 aeron io poll worker started: worker_id=%u",
              worker->worker_id);

    uint32_t idle_rounds = 0;
    while (atomic_load_explicit(&proxy->running, memory_order_relaxed)) {
        int did_work = 0;
        uint32_t worker_count = proxy->proxy_io_worker_count;
        if (worker_count == 0)
            break;

        if (drain_job_return_queues(proxy, worker->worker_id) > 0)
            did_work = 1;

        atomic_fetch_add_explicit(&worker->aeron_snapshot_readers,
                                  1,
                                  memory_order_seq_cst);
        vemb_v16_aeron_channel_snapshot_t *snapshot =
            atomic_load_explicit(&worker->aeron_snapshot,
                                 memory_order_seq_cst);
        for (uint32_t i = 0; i < snapshot->count; i++) {
            uint32_t channel_index = snapshot->indices[i];
            /* Snapshot updates place each Aeron channel in the buffer owned
             * by its lane.  Do not repeat the modulo dispatch check here. */
            if (channel_index >= VEMB_V16_MAX_CHANNELS)
                continue;
            vemb_v16_channel_t *ch = &proxy->channels[channel_index];
            if (!proxy_io_channel_acquire(ch))
                continue;

            if (ch->transport_type != VEMB_V16_TRANSPORT_AERON) {
                proxy_io_channel_release(ch);
                continue;
            }

            int n = drain_completions(ch);
            if (n < 0) {
                proxy_io_channel_deactivate(ch);
                did_work = 1;
                proxy_io_channel_release(ch);
                continue;
            }
            if (n > 0)
                did_work = 1;

            if (atomic_load_explicit(&ch->active, memory_order_acquire)) {
                int rc = vemb_v16_aeron_poll_shm_requests(
                    ch, worker->worker_id);
                if (rc < 0) {
                    proxy_io_channel_deactivate(ch);
                    did_work = 1;
                } else if (rc > 0) {
                    did_work = 1;
                }
            }
            proxy_io_channel_release(ch);
        }
        atomic_fetch_sub_explicit(&worker->aeron_snapshot_readers,
                                  1,
                                  memory_order_seq_cst);

        if (!did_work) {
            aeron_io_tiny_pause(idle_rounds);
            if (idle_rounds != UINT32_MAX)
                idle_rounds++;
        } else {
            idle_rounds = 0;
        }
    }

    serverLog(LL_VERBOSE,
              "vemb_v16 aeron io poll worker stopped: worker_id=%u",
              worker->worker_id);
    return NULL;
}

/// Proxy IO scheduling: poll-based fallback for TCP sockets and UB rings.
static void *proxy_io_poll_thread_main(void *arg) {
    vemb_v16_proxy_io_worker_t *worker = arg;
    vemb_v16_proxy_t *proxy = worker->proxy;
    proxy_io_set_affinity(proxy, worker->worker_id);
    serverLog(LL_VERBOSE, "vemb_v16 proxy io poll worker started: worker_id=%u",
              worker->worker_id);
    while (atomic_load_explicit(&proxy->running, memory_order_relaxed)) {
        struct pollfd pfds[VEMB_V16_MAX_CHANNELS];
        vemb_v16_channel_t *poll_channels[VEMB_V16_MAX_CHANNELS];
        nfds_t nfds = 0;
        int did_work = 0;
        uint32_t worker_count = proxy->proxy_io_worker_count;
        if (worker_count == 0)
            break;

        if (drain_job_return_queues(proxy, worker->worker_id) > 0)
            did_work = 1;

        for (uint32_t i = worker->worker_id; i < VEMB_V16_MAX_CHANNELS; i += worker_count) {
            vemb_v16_channel_t *ch = &proxy->channels[i];
            if (!proxy_io_channel_acquire(ch))
                continue;

            int n = drain_completions(ch);
            if (n < 0) {
                proxy_io_channel_deactivate(ch);
                did_work = 1;
                proxy_io_channel_release(ch);
                continue;
            }
            if (n > 0) {
                did_work = 1;
#ifdef __linux__
                if (ch->transport_type == VEMB_V16_TRANSPORT_TCP &&
                    flush_tcp_response_backlog(ch) < 0) {
                    proxy_io_channel_deactivate(ch);
                    proxy_io_channel_release(ch);
                    continue;
                }
#endif
            }

            if (atomic_load_explicit(&ch->active, memory_order_acquire) &&
                ch->transport_type == VEMB_V16_TRANSPORT_AERON) {
                int rc = vemb_v16_aeron_poll_shm_requests(ch, worker->worker_id);
                if (rc < 0) {
                    proxy_io_channel_deactivate(ch);
                    did_work = 1;
                } else if (rc > 0) {
                    did_work = 1;
                }
                proxy_io_channel_release(ch);
                continue;
            }

            if (atomic_load_explicit(&ch->active, memory_order_acquire) &&
                ch->net_fd >= 0) {
                if (vemb_v16_tcp_has_buffered_requests(ch)) {
                    int rc = vemb_v16_tcp_read_ready_requests(ch,
                                                             worker->worker_id);
                    if (rc < 0) {
                        proxy_io_channel_deactivate(ch);
                        did_work = 1;
                        proxy_io_channel_release(ch);
                        continue;
                    }
                    if (rc > 0)
                        did_work = 1;
                }
                pfds[nfds] = (struct pollfd){
                    .fd = ch->net_fd,
                    .events = POLLIN
#ifdef __linux__
                        | (vemb_v16_tcp_backlog_pending(ch) ? POLLOUT : 0)
#endif
                    ,
                    .revents = 0,
                };
                poll_channels[nfds] = ch;
                nfds++;
            } else {
                proxy_io_channel_release(ch);
            }
        }

        int pr = 0;
        if (nfds > 0) {
            pr = poll(pfds, nfds, did_work ? 0 : 1);
            if (pr < 0 && errno == EINTR)
                pr = 0;
        }

        for (nfds_t i = 0; i < nfds; i++) {
            vemb_v16_channel_t *ch = poll_channels[i];
            short revents = pfds[i].revents;
            if (pr < 0) {
                proxy_io_channel_deactivate(ch);
                did_work = 1;
            } else if (revents & POLLIN) {
                int rc = vemb_v16_tcp_read_ready_requests(ch,
                                                         worker->worker_id);
                if (rc < 0) {
                    proxy_io_channel_deactivate(ch);
                } else if (rc > 0) {
                    did_work = 1;
                }
#ifdef __linux__
            } else if (revents & POLLOUT) {
                if (flush_tcp_response_backlog(ch) < 0) {
                    proxy_io_channel_deactivate(ch);
                } else {
                    did_work = 1;
                }
#endif
            } else if (revents & (POLLERR | POLLHUP | POLLNVAL)) {
                proxy_io_channel_deactivate(ch);
                did_work = 1;
            }
            proxy_io_channel_release(ch);
        }

        if (!did_work && nfds == 0)
            cpu_relax();
    }
    serverLog(LL_VERBOSE, "vemb_v16 proxy io poll worker stopped: worker_id=%u",
              worker->worker_id);
    return NULL;
}

#ifdef __linux__
/// Proxy IO scheduling: unregister one TCP fd from the Linux epoll worker.
static void proxy_io_epoll_unregister(vemb_v16_channel_t *ch,
                                      int epfd,
                                      uint64_t *registered_ids,
                                      int *registered_fds,
                                      uint32_t *registered_events,
                                      uint32_t index) {
    if (registered_ids[index] == 0)
        return;
    if (registered_fds[index] >= 0)
        epoll_ctl(epfd, EPOLL_CTL_DEL, registered_fds[index], NULL);
    registered_ids[index] = 0;
    registered_fds[index] = -1;
    registered_events[index] = 0;
    atomic_store_explicit(&ch->proxy_io_registered, 0, memory_order_release);
}

/// Proxy IO scheduling: epoll TCP sockets, poll UB rings, and drain completions.
static void *proxy_io_epoll_thread_main(void *arg) {
    vemb_v16_proxy_io_worker_t *worker = arg;
    vemb_v16_proxy_t *proxy = worker->proxy;
    uint64_t registered_ids[VEMB_V16_MAX_CHANNELS] = {0};
    int registered_fds[VEMB_V16_MAX_CHANNELS];
    uint32_t registered_events[VEMB_V16_MAX_CHANNELS];
    struct epoll_event events[VEMB_V16_MAX_CHANNELS];
    const uint64_t notify_token = UINT64_MAX;
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        registered_fds[i] = -1;
        registered_events[i] = 0;
    }

    int epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd < 0) {
        serverLog(LL_WARNING, "vemb_v16 proxy io epoll create failed: worker_id=%u errno=%d error=%s",
                  worker->worker_id, errno, strerror(errno));
        atomic_store_explicit(&proxy->running, 0, memory_order_relaxed);
        return NULL;
    }
    struct epoll_event notify_ev = {
        .events = EPOLLIN,
        .data = {.u64 = notify_token},
    };
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, worker->notify_fd, &notify_ev) != 0) {
        serverLog(LL_WARNING,
                  "vemb_v16 FATAL: proxy io notify eventfd epoll add failed: worker_id=%u fd=%d errno=%d error=%s",
                  worker->worker_id,
                  worker->notify_fd,
                  errno,
                  strerror(errno));
        atomic_store_explicit(&proxy->running, 0, memory_order_relaxed);
        close(epfd);
        return NULL;
    }

    proxy_io_set_affinity(proxy, worker->worker_id);

    serverLog(LL_VERBOSE, "vemb_v16 proxy io epoll worker started: worker_id=%u",
              worker->worker_id);
    while (atomic_load_explicit(&proxy->running, memory_order_relaxed)) {
        int did_work = 0;
        uint32_t worker_count = proxy->proxy_io_worker_count;
        if (worker_count == 0)
            break;

        if (drain_job_return_queues(proxy, worker->worker_id) > 0)
            did_work = 1;

        for (uint32_t i = worker->worker_id;
             i < VEMB_V16_MAX_CHANNELS;
             i += worker_count) {
            vemb_v16_channel_t *ch = &proxy->channels[i];
            uint64_t channel_id =
                atomic_load_explicit(&ch->slot_channel_id,
                                     memory_order_acquire);
            int channel_active = channel_id != 0 &&
                atomic_load_explicit(&ch->active, memory_order_acquire);
            int tcp_active = channel_active &&
                ch->transport_type == VEMB_V16_TRANSPORT_TCP &&
                ch->net_fd >= 0;
            int aeron_active = channel_active &&
                ch->transport_type == VEMB_V16_TRANSPORT_AERON;
            int active = tcp_active;
            int fd = tcp_active ? ch->net_fd : -1;

            if (registered_ids[i] != 0 &&
                (!tcp_active ||
                 registered_ids[i] != channel_id ||
                 registered_fds[i] != fd)) {
                proxy_io_epoll_unregister(ch,
                                          epfd,
                                          registered_ids,
                                          registered_fds,
                                          registered_events,
                                          i);
                did_work = 1;
            }

            if (!tcp_active && !aeron_active)
                continue;

            if (!proxy_io_channel_acquire(ch))
                continue;
            proxy_io_channel_disarm_completion_notify(ch);

            channel_id = atomic_load_explicit(&ch->slot_channel_id,
                                              memory_order_acquire);
            channel_active = channel_id != 0 &&
                atomic_load_explicit(&ch->active, memory_order_acquire);
            if (!channel_active) {
                proxy_io_channel_release(ch);
                continue;
            }

            if (ch->transport_type == VEMB_V16_TRANSPORT_AERON) {
                int n = drain_completions(ch);
                if (n < 0) {
                    proxy_io_channel_deactivate(ch);
                    did_work = 1;
                } else if (n > 0) {
                    did_work = 1;
                }
                int rc = vemb_v16_aeron_poll_shm_requests(ch, worker->worker_id);
                if (rc < 0) {
                    proxy_io_channel_deactivate(ch);
                    did_work = 1;
                } else if (rc > 0) {
                    did_work = 1;
                } else if (!did_work) {
                    if (proxy_io_channel_arm_completion_notify(ch) < 0)
                        did_work = 1;
                }
                proxy_io_channel_release(ch);
                continue;
            }

            active = ch->transport_type == VEMB_V16_TRANSPORT_TCP &&
                ch->net_fd >= 0;
            fd = active ? ch->net_fd : -1;
            if (!active) {
                proxy_io_channel_release(ch);
                continue;
            }

            if (registered_ids[i] == 0) {
                uint32_t desired_events = EPOLLIN | EPOLLERR | EPOLLHUP;
                if (vemb_v16_tcp_backlog_pending(ch))
                    desired_events |= EPOLLOUT;
                struct epoll_event ev = {
                    .events = desired_events,
                    .data = {.u32 = i},
                };
                if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) == 0) {
                    registered_ids[i] = channel_id;
                    registered_fds[i] = fd;
                    registered_events[i] = desired_events;
                    atomic_store_explicit(&ch->proxy_io_registered, 1,
                                          memory_order_release);
                } else {
                    proxy_io_channel_deactivate(ch);
                    did_work = 1;
                    proxy_io_channel_release(ch);
                    continue;
                }
            }

            uint32_t desired_events = EPOLLIN | EPOLLERR | EPOLLHUP;
            if (vemb_v16_tcp_backlog_pending(ch))
                desired_events |= EPOLLOUT;
            if (registered_ids[i] != 0 &&
                registered_events[i] != desired_events) {
                struct epoll_event ev = {
                    .events = desired_events,
                    .data = {.u32 = i},
                };
                if (epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev) == 0) {
                    registered_events[i] = desired_events;
                } else {
                    proxy_io_channel_deactivate(ch);
                    did_work = 1;
                    proxy_io_channel_release(ch);
                    continue;
                }
            }

            if (vemb_v16_tcp_backlog_pending(ch)) {
                int flush = flush_tcp_response_backlog(ch);
                if (flush < 0) {
                    proxy_io_channel_deactivate(ch);
                    did_work = 1;
                    proxy_io_channel_release(ch);
                    continue;
                }
                if (flush == 1)
                    did_work = 1;
            }
            if (vemb_v16_tcp_has_buffered_requests(ch)) {
                int rc = vemb_v16_tcp_read_ready_requests(ch,
                                                         worker->worker_id);
                if (rc < 0) {
                    proxy_io_channel_deactivate(ch);
                    did_work = 1;
                    proxy_io_channel_release(ch);
                    continue;
                }
                if (rc > 0)
                    did_work = 1;
            }
            int n = drain_completions(ch);
            if (n < 0) {
                proxy_io_channel_deactivate(ch);
                did_work = 1;
            } else if (n > 0) {
                did_work = 1;
                if (flush_tcp_response_backlog(ch) < 0) {
                    proxy_io_channel_deactivate(ch);
                    proxy_io_channel_release(ch);
                    continue;
                }
            } else if (proxy_io_channel_arm_completion_notify(ch) < 0) {
                did_work = 1;
            }
            proxy_io_channel_release(ch);
        }

        /* UB rings have no readable fd; keep the polling cadence bounded
         * independently of deployment location. */
        int timeout_ms = did_work ? 0 : 1;
        int nready = epoll_wait(epfd,
                                events,
                                VEMB_V16_MAX_CHANNELS,
                                timeout_ms);
        if (nready < 0) {
            if (errno == EINTR)
                continue;
            break;
        }

        for (int i = 0; i < nready; i++) {
            if (events[i].data.u64 == notify_token) {
                eventfd_t value = 0;
                while (eventfd_read(worker->notify_fd, &value) != 0 &&
                       errno == EINTR) {
                }
                did_work = 1;
                continue;
            }

            uint32_t index = events[i].data.u32;
            if (index >= VEMB_V16_MAX_CHANNELS)
                continue;
            vemb_v16_channel_t *ch = &proxy->channels[index];
            if (!proxy_io_channel_acquire(ch))
                continue;
            proxy_io_channel_disarm_completion_notify(ch);
            uint64_t channel_id =
                atomic_load_explicit(&ch->slot_channel_id,
                                     memory_order_acquire);
            if (registered_ids[index] != channel_id) {
                proxy_io_channel_release(ch);
                continue;
            }

            uint32_t revents = events[i].events;
            if (revents & EPOLLIN) {
                int rc = vemb_v16_tcp_read_ready_requests(ch,
                                                         worker->worker_id);
                if (rc < 0) {
                    proxy_io_channel_deactivate(ch);
                } else if (rc > 0) {
                    did_work = 1;
                }
            }
            if ((revents & EPOLLOUT) && atomic_load_explicit(&ch->active, memory_order_acquire)) {
                if (flush_tcp_response_backlog(ch) < 0)
                    proxy_io_channel_deactivate(ch);
            }
            if (revents & (EPOLLERR | EPOLLHUP)) {
                proxy_io_channel_deactivate(ch);
            }
            proxy_io_channel_release(ch);
        }
    }

    for (uint32_t i = worker->worker_id;
         i < VEMB_V16_MAX_CHANNELS;
         i += proxy->proxy_io_worker_count) {
        proxy_io_epoll_unregister(&proxy->channels[i],
                                  epfd,
                                  registered_ids,
                                  registered_fds,
                                  registered_events,
                                  i);
    }
    close(epfd);
    serverLog(LL_VERBOSE, "vemb_v16 proxy io epoll worker stopped: worker_id=%u",
              worker->worker_id);
    return NULL;
}
#endif

static void *proxy_io_pool_thread_main(void *arg) {
    vemb_v16_proxy_io_worker_t *worker = arg;
    if (vemb_v16_proxy_data_transport(worker->proxy) ==
        VEMB_V16_TRANSPORT_AERON)
        return proxy_io_aeron_poll_thread_main(arg);
#ifdef __linux__
    return proxy_io_epoll_thread_main(arg);
#else
    return proxy_io_poll_thread_main(arg);
#endif
}

#ifdef __linux__
static int proxy_io_worker_open_notify_fd(vemb_v16_proxy_io_worker_t *worker) {
    worker->notify_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (worker->notify_fd < 0) {
        serverLog(LL_WARNING,
                  "vemb_v16 FATAL: proxy io notify eventfd create failed: worker_id=%u errno=%d error=%s",
                  worker->worker_id, errno, strerror(errno));
        return -1;
    }
    return 0;
}

static void proxy_io_worker_wake(vemb_v16_proxy_io_worker_t *worker) {
    if (worker->notify_fd >= 0)
        (void)eventfd_write(worker->notify_fd, 1);
}

static void proxy_io_worker_close_notify_fd(vemb_v16_proxy_io_worker_t *worker) {
    if (worker->notify_fd >= 0) {
        close(worker->notify_fd);
        worker->notify_fd = -1;
    }
}
#endif

static int start_proxy_io_pool(vemb_v16_proxy_t *proxy) {
    if (validate_pooled_worker_config(proxy) != 0)
        return -1;
    for (uint32_t i = 0; i < proxy->proxy_io_worker_count; i++) {
        proxy->proxy_io_workers[i].worker_id = i;
        proxy->proxy_io_workers[i].proxy = proxy;
        if (proxy->data_transport_type == VEMB_V16_TRANSPORT_AERON &&
            aeron_channel_snapshot_init(&proxy->proxy_io_workers[i],
                                        proxy->proxy_io_worker_count) != 0) {
            serverLog(LL_WARNING,
                      "vemb_v16 aeron channel snapshot init failed: worker_id=%u",
                      i);
            for (uint32_t j = 0; j < i; j++)
                aeron_channel_snapshot_cleanup(&proxy->proxy_io_workers[j]);
            return -1;
        }
#ifdef __linux__
        proxy->proxy_io_workers[i].notify_fd = -1;
        if (proxy->data_transport_type == VEMB_V16_TRANSPORT_TCP &&
            proxy_io_worker_open_notify_fd(&proxy->proxy_io_workers[i]) != 0) {
            atomic_store_explicit(&proxy->running, 0, memory_order_relaxed);
            for (uint32_t j = 0; j < i; j++)
                proxy_io_worker_wake(&proxy->proxy_io_workers[j]);
            for (uint32_t j = 0; j < i; j++) {
                pthread_join(proxy->proxy_io_workers[j].thread, NULL);
                proxy_io_worker_close_notify_fd(&proxy->proxy_io_workers[j]);
                aeron_channel_snapshot_cleanup(&proxy->proxy_io_workers[j]);
            }
            aeron_channel_snapshot_cleanup(&proxy->proxy_io_workers[i]);
            proxy->proxy_io_pool_started = 0;
            return -1;
        }
#endif
        if (pthread_create(&proxy->proxy_io_workers[i].thread,
                           NULL,
                           proxy_io_pool_thread_main,
                           &proxy->proxy_io_workers[i]) != 0) {
            atomic_store_explicit(&proxy->running, 0, memory_order_relaxed);
#ifdef __linux__
            for (uint32_t j = 0; j < i; j++)
                proxy_io_worker_wake(&proxy->proxy_io_workers[j]);
#endif
            for (uint32_t j = 0; j < i; j++) {
                pthread_join(proxy->proxy_io_workers[j].thread, NULL);
#ifdef __linux__
                proxy_io_worker_close_notify_fd(&proxy->proxy_io_workers[j]);
#endif
                aeron_channel_snapshot_cleanup(&proxy->proxy_io_workers[j]);
            }
#ifdef __linux__
            proxy_io_worker_close_notify_fd(&proxy->proxy_io_workers[i]);
#endif
            aeron_channel_snapshot_cleanup(&proxy->proxy_io_workers[i]);
            proxy->proxy_io_pool_started = 0;
            return -1;
        }
    }
    proxy->proxy_io_pool_started = 1;
    return 0;
}

static void stop_proxy_io_pool(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    if (!proxy->proxy_io_pool_started)
        return;
#ifdef __linux__
    for (uint32_t i = 0; i < proxy->proxy_io_worker_count; i++)
        proxy_io_worker_wake(&proxy->proxy_io_workers[i]);
#endif
    for (uint32_t i = 0; i < proxy->proxy_io_worker_count; i++)
        pthread_join(proxy->proxy_io_workers[i].thread, NULL);
#ifdef __linux__
    for (uint32_t i = 0; i < proxy->proxy_io_worker_count; i++)
        proxy_io_worker_close_notify_fd(&proxy->proxy_io_workers[i]);
#endif
    for (uint32_t i = 0; i < proxy->proxy_io_worker_count; i++)
        aeron_channel_snapshot_cleanup(&proxy->proxy_io_workers[i]);
    proxy->proxy_io_pool_started = 0;
}

/// Execution: run one VEMB job on the SuperNode storage/backend path.
static void apply_vemb_job(vemb_v16_supernode_ctx_t *ctx,
                           const vemb_v16_vemb_job_t *job,
                           vemb_v16_supernode_scratch_t *scratch,
                           vemb_v16_channel_t *ch) {
    (void)scratch;
    (void)ch;
    vemb_v16_supernode_handle_vemb_job(ctx, job);
}

static void apply_vsim_key_key_job(vemb_v16_supernode_ctx_t *ctx,
                                   const vemb_v16_vsim_key_key_job_t *job,
                                   vemb_v16_supernode_scratch_t *scratch,
                                   vemb_v16_channel_t *ch) {
    (void)scratch;
    (void)ch;
    vemb_v16_supernode_handle_vsim_key_key_job(ctx, job);
}

static void apply_vrem_job(vemb_v16_supernode_ctx_t *ctx,
                           const vemb_v16_vemb_job_t *job,
                           vemb_v16_supernode_scratch_t *scratch,
                           vemb_v16_channel_t *ch) {
    (void)scratch;
    (void)ch;
    vemb_v16_supernode_handle_vrem_job(ctx, job);
}

/// Execution: run one VADD job on the SuperNode storage/backend path.
static void apply_vadd_job(vemb_v16_supernode_ctx_t *ctx,
                           const vemb_v16_vadd_job_t *job,
                           vemb_v16_supernode_scratch_t *scratch,
                           vemb_v16_channel_t *ch) {
    (void)scratch;
    (void)ch;
    vemb_v16_supernode_handle_vadd_job(ctx, job);
}

static void notify_completion_consumer_from_proxy(vemb_v16_supernode_ctx_t *ctx) {
#ifdef __linux__
    assert(ctx->completion_notify_armed != NULL);
    assert(ctx->completion_notify_fd != NULL);
    int expected = 1;
    if (!atomic_compare_exchange_strong_explicit(ctx->completion_notify_armed,
                                                 &expected,
                                                 0,
                                                 memory_order_acq_rel,
                                                 memory_order_relaxed)) {
        return;
    }
    int notify_fd = *ctx->completion_notify_fd;
    (void)eventfd_write(notify_fd, 1);
#else
    (void)ctx;
#endif
}

static void publish_synthetic_completion(vemb_v16_supernode_ctx_t *ctx,
                                         const vemb_v16_completion_t *completion) {
    uint32_t spins = 0;
    int use_backoff = 1;
    while (vemb_v16_aeron_publish(ctx->completion_ring, completion) != 0 &&
           atomic_load_explicit(ctx->running, memory_order_relaxed) &&
           atomic_load_explicit(ctx->channel_active, memory_order_acquire)) {
        if (use_backoff) vemb_v16_aeron_backoff(spins++);
        else             cpu_relax();
    }
    notify_completion_consumer_from_proxy(ctx);
}

static void apply_unified_shard_job(vemb_v16_supernode_ctx_t *ctx,
                                    const vemb_v16_job_base_t *job,
                                    vemb_v16_supernode_scratch_t *scratch,
                                    vemb_v16_channel_t *ch) {
    (void)scratch;
    switch (job->kind) {
    case VEMB_V16_JOB_KIND_BASE:
        vemb_v16_supernode_handle_base_job(ctx, job);
        break;
    case VEMB_V16_JOB_KIND_READ:
        if (job->op == VEMB_V16_OP_VREM) {
            apply_vrem_job(ctx, (const vemb_v16_vemb_job_t *)job, scratch, ch);
        } else {
            apply_vemb_job(ctx, (const vemb_v16_vemb_job_t *)job, scratch, ch);
        }
        break;
    case VEMB_V16_JOB_KIND_VSIM_KEY_KEY:
        apply_vsim_key_key_job(ctx,
                               (const vemb_v16_vsim_key_key_job_t *)job,
                               scratch,
                               ch);
        break;
    case VEMB_V16_JOB_KIND_INLINE_VECTOR:
        apply_vadd_job(ctx, (const vemb_v16_vadd_job_t *)job, scratch, ch);
        break;
    default: {
        vemb_v16_completion_t completion = {
            .op = job->op,
            .req_id = job->req_id,
            .channel_id = job->channel_id,
            .channel_index = job->channel_index,
            .batch_token = job->batch_token,
            .status = job->op == VEMB_V16_OP_PING ?
                VEMB_V16_STATUS_OK :
                VEMB_V16_STATUS_ERR,
        };
        publish_synthetic_completion(ctx, &completion);
        break;
    }
    }
}

static void publish_job_return_batch(vemb_v16_proxy_t *proxy,
                                     uint32_t proxy_worker_id,
                                     uint32_t sn_work_id,
                                     const vemb_v16_job_return_t *rets,
                                     uint32_t ret_count) {
    assert(proxy != NULL);
    assert(rets != NULL || ret_count == 0);
    if (ret_count == 0)
        return;
    uint32_t queue_index = shard_queue_index(proxy, proxy_worker_id, sn_work_id);
    vemb_v16_aeron_ring_t *ring = &proxy->job_return_queues[queue_index].ring;
    while (vemb_v16_aeron_publish_batch(ring, rets, ret_count) != 0 &&
           atomic_load_explicit(&proxy->running, memory_order_relaxed)) {
        cpu_relax();
    }
}

/// Execution scheduling: drain shard queues assigned to one SuperNode worker.
static int drain_shard_queues(vemb_v16_proxy_t *proxy,
                              uint32_t sn_work_id,
                              vemb_v16_supernode_scratch_t *scratch,
                              vemb_v16_shard_queue_t *queues,
                              vemb_v16_job_ref_t *job_refs) {
    assert(proxy != NULL);
    assert(scratch != NULL);
    assert(queues != NULL);
    assert(job_refs != NULL);
    RETURN_IF(!shard_queue_topology_ready(proxy) ||
              sn_work_id >= proxy->job_shard_supernode_count, 0);

    int did_work = 0;
    uint32_t work_start = 0;
    uint32_t work_end = proxy->job_shard_proxy_count;
    if (pooled_workers_have_1to1_pairing(proxy)) {
        work_start = sn_work_id;
        work_end = sn_work_id + 1;
    }
    for (uint32_t work_id = work_start; work_id < work_end; work_id++) {
        vemb_v16_channel_t *batched_ch = NULL;
        vemb_v16_supernode_ctx_t batched_ctx;
        uint32_t batched_completion_count = 0;
        uint32_t queue_index = shard_queue_index(proxy, work_id, sn_work_id);
        vemb_v16_aeron_ring_t *ring = &queues[queue_index].ring;
        uint32_t n = vemb_v16_aeron_poll_batch(ring, job_refs, PROXY_QUEUE_BATCH);
        vemb_v16_job_return_t job_returns[PROXY_QUEUE_BATCH];
        uint32_t return_count = 0;
        if (!n) continue;

        did_work += (int)n;
        for (uint32_t i = 0; i < n; i++) {
            vemb_v16_job_ref_t *ref = &job_refs[i];
            if (ref->proxy_worker_id >= proxy->proxy_io_worker_count ||
                ref->pool_type >= VEMB_V16_JOB_POOL_COUNT)
                continue;
            vemb_v16_job_pool_t *pool =
                &proxy->proxy_io_workers[ref->proxy_worker_id].job_pools[ref->pool_type];
            if (ref->slot_id >= pool->slot_count)
                continue;
            vemb_v16_job_slot_t *slot = job_pool_slot(pool, ref->slot_id);
            if (slot->hdr.generation != ref->generation ||
                atomic_load_explicit(&slot->hdr.state, memory_order_acquire) !=
                    VEMB_V16_JOB_SLOT_PUBLISHED)
                continue;
            const vemb_v16_job_base_t *job_base = job_slot_payload_base(pool, slot);
            if (!job_base || job_base->op != ref->op)
                continue;
            atomic_store_explicit(&slot->hdr.state,
                                  VEMB_V16_JOB_SLOT_RUNNING,
                                  memory_order_release);
            vemb_v16_job_return_t job_return = {
                .pool_type = ref->pool_type,
                .slot_id = ref->slot_id,
                .generation = ref->generation,
            };
            if (job_base->channel_index >= VEMB_V16_MAX_CHANNELS) {
                job_returns[return_count++] = job_return;
                continue;
            }
            vemb_v16_channel_t *ch = &proxy->channels[job_base->channel_index];
            if (batched_ch != ch) {
                if (batched_ch != NULL) {
                    vemb_v16_supernode_flush_completion_batch(&batched_ctx, 1);
                    supernode_channel_release(batched_ch);
                    batched_ch = NULL;
                }
                if (!supernode_channel_acquire(ch)) {
                    job_returns[return_count++] = job_return;
                    continue;
                }
                batched_ch = ch;
                batched_completion_count = 0;
                batched_ctx = ch->supernode_ctx;
                batched_ctx.worker_id = sn_work_id;
                batched_ctx.completion_batch = scratch->completion_batch;
                batched_ctx.completion_batch_count = &batched_completion_count;
                batched_ctx.completion_batch_capacity = PROXY_QUEUE_BATCH;
            }
            if (atomic_load_explicit(
                &ch->slot_channel_id, memory_order_acquire) == job_base->channel_id &&
                atomic_load_explicit(&ch->active, memory_order_acquire)) {
                apply_unified_shard_job(&batched_ctx, job_base, scratch, ch);
            }
            job_returns[return_count++] = job_return;
        }
        if (batched_ch != NULL) {
            vemb_v16_supernode_flush_completion_batch(&batched_ctx, 1);
            supernode_channel_release(batched_ch);
        }
        publish_job_return_batch(proxy, work_id, sn_work_id, job_returns, return_count);
    }
    return did_work;
}

/// Execution scheduling: drain FIFO job queues for one SuperNode worker.
static int drain_job_shard_queues(vemb_v16_proxy_t *proxy,
                                  uint32_t sn_work_id,
                                  vemb_v16_supernode_scratch_t *scratch) {
    assert(proxy != NULL);
    assert(scratch != NULL);
    return drain_shard_queues(proxy,
                              sn_work_id,
                              scratch,
                              proxy->job_shard_queues,
                              scratch->job_refs);
}

static int drain_job_return_queues(vemb_v16_proxy_t *proxy,
                                   uint32_t proxy_worker_id) {
    assert(proxy != NULL);
    RETURN_IF(!shard_queue_topology_ready(proxy) ||
              !proxy->job_return_queues ||
              proxy_worker_id >= proxy->job_shard_proxy_count,
              0);

    int reclaimed = 0;
    vemb_v16_job_return_t returns[PROXY_QUEUE_BATCH];
    uint32_t sn_start = 0;
    uint32_t sn_end = proxy->job_shard_supernode_count;
    if (pooled_workers_have_1to1_pairing(proxy)) {
        sn_start = proxy_worker_id;
        sn_end = proxy_worker_id + 1;
    }
    for (uint32_t sn_id = sn_start; sn_id < sn_end; sn_id++) {
        uint32_t queue_index = shard_queue_index(proxy, proxy_worker_id, sn_id);
        vemb_v16_aeron_ring_t *ring = &proxy->job_return_queues[queue_index].ring;
        uint32_t n;
        while ((n = vemb_v16_aeron_poll_batch(ring,
                                              returns,
                                              PROXY_QUEUE_BATCH)) != 0) {
            reclaimed += (int)n;
            for (uint32_t i = 0; i < n; i++) {
                vemb_v16_job_return_t *ret = &returns[i];
                if (ret->pool_type >= VEMB_V16_JOB_POOL_COUNT)
                    continue;
                vemb_v16_job_pool_t *pool =
                    &proxy->proxy_io_workers[proxy_worker_id].job_pools[ret->pool_type];
                if (ret->slot_id >= pool->slot_count)
                    continue;
                vemb_v16_job_slot_t *slot = job_pool_slot(pool, ret->slot_id);
                if (slot->hdr.generation != ret->generation)
                    continue;
                if (atomic_load_explicit(&slot->hdr.state, memory_order_acquire) !=
                    VEMB_V16_JOB_SLOT_RUNNING) {
                    continue;
                }
                job_pool_release_slot(pool, ret->slot_id, 1);
            }
        }
    }
    return reclaimed;
}

#ifdef __linux__
/// Execution scheduling: check whether a SuperNode worker can sleep.
static int supernode_worker_has_pending(vemb_v16_proxy_t *proxy,
                                        uint32_t worker_id) {
    RETURN_IF(!shard_queue_topology_ready(proxy) ||
        worker_id >= proxy->job_shard_supernode_count, 0);

    uint32_t proxy_start = 0;
    uint32_t proxy_end = proxy->job_shard_proxy_count;
    if (pooled_workers_have_1to1_pairing(proxy)) {
        proxy_start = worker_id;
        proxy_end = worker_id + 1;
    }
    for (uint32_t proxy_id = proxy_start; proxy_id < proxy_end; proxy_id++) {
        uint32_t queue_index = shard_queue_index(proxy, proxy_id, worker_id);
        if (vemb_v16_aeron_available(&proxy->job_shard_queues[queue_index].ring) != 0)
            return 1;
    }

    return 0;
}

static void supernode_worker_wait_for_jobs(vemb_v16_supernode_pool_worker_t *worker) {
    atomic_store_explicit(&worker->job_notify_armed, 1, memory_order_release);
    if (supernode_worker_has_pending(worker->proxy, worker->worker_id)) {
        atomic_store_explicit(&worker->job_notify_armed, 0, memory_order_release);
        return;
    }

    futex_wait(&worker->job_notify_armed, 1);
    atomic_store_explicit(&worker->job_notify_armed, 0, memory_order_release);
}

static void supernode_worker_wake(vemb_v16_supernode_pool_worker_t *worker) {
    atomic_store_explicit(&worker->job_notify_armed, 0, memory_order_release);
    futex_notify(&worker->job_notify_armed);
}
#endif

/// Execution worker: consume scheduled VEMB/VADD jobs and publish completions.
static void *supernode_pool_thread_main(void *arg) {
    vemb_v16_supernode_pool_worker_t *worker = arg;
    vemb_v16_proxy_t *proxy = worker->proxy;
    vemb_v16_supernode_scratch_t scratch;
    if (vemb_v16_supernode_scratch_init(&scratch) != 0)
        return NULL;
#ifdef __linux__
    char thread_name[16];
    snprintf(thread_name, sizeof(thread_name), "vemb-sn-%02u", worker->worker_id);
    (void)pthread_setname_np(pthread_self(), thread_name);
    atomic_store_explicit(&worker->job_notify_armed, 0, memory_order_release);
    monotime idle_spin_start;
    int idle_spinning = 0;
    uint32_t idle_spin_rounds = 0;
#endif

    supernode_worker_set_affinity(proxy, worker->worker_id);

    serverLog(LL_VERBOSE, "vemb_v16 pooled supernode worker started: worker_id=%u",
              worker->worker_id);
    while (atomic_load_explicit(&proxy->running, memory_order_relaxed)) {
        int did_work = 0;
        if (drain_job_shard_queues(proxy, worker->worker_id, &scratch) > 0)
            did_work = 1;
        if (!did_work) {
#ifdef __linux__
            if (!idle_spinning) {
                elapsedStartNs(&idle_spin_start);
                idle_spinning = 1;
            }
            if (++idle_spin_rounds < VEMB_V16_SUPERNODE_IDLE_CLOCK_CHECK_ROUNDS) {
                cpu_relax();
                continue;
            }
            idle_spin_rounds = 0;
            if (elapsedNs(idle_spin_start) < VEMB_V16_SUPERNODE_IDLE_SPIN_NS) {
                cpu_relax();
                continue;
            }
            supernode_worker_wait_for_jobs(worker);
            idle_spinning = 0;
            idle_spin_rounds = 0;
#else
            cpu_relax();
#endif
        } else {
#ifdef __linux__
            idle_spinning = 0;
            idle_spin_rounds = 0;
#endif
        }
    }
    serverLog(LL_VERBOSE, "vemb_v16 pooled supernode worker stopped: worker_id=%u",
              worker->worker_id);
    vemb_v16_supernode_scratch_cleanup(&scratch);
    return NULL;
}

static int start_supernode_pool(vemb_v16_proxy_t *proxy) {
    if (validate_pooled_worker_config(proxy) != 0)
        return -1;
    for (uint32_t i = 0; i < proxy->supernode_worker_count; i++) {
        proxy->supernode_workers[i] = (vemb_v16_supernode_pool_worker_t){
            .worker_id = i,
            .proxy = proxy,
#ifdef __linux__
            .job_notify_armed = 0,
#endif
        };
        if (pthread_create(&proxy->supernode_workers[i].thread,
                           NULL,
                           supernode_pool_thread_main,
                           &proxy->supernode_workers[i]) != 0) {
            atomic_store_explicit(&proxy->running, 0, memory_order_relaxed);
#ifdef __linux__
            for (uint32_t j = 0; j < i; j++)
                supernode_worker_wake(&proxy->supernode_workers[j]);
#endif
            for (uint32_t j = 0; j < i; j++) {
                pthread_join(proxy->supernode_workers[j].thread, NULL);
            }
            proxy->supernode_pool_started = 0;
            return -1;
        }
    }
    proxy->supernode_pool_started = 1;
    return 0;
}

static void stop_supernode_pool(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    if (!proxy->supernode_pool_started)
        return;
#ifdef __linux__
    for (uint32_t i = 0; i < proxy->supernode_worker_count; i++)
        supernode_worker_wake(&proxy->supernode_workers[i]);
#endif
    for (uint32_t i = 0; i < proxy->supernode_worker_count; i++) {
        pthread_join(proxy->supernode_workers[i].thread, NULL);
    }
    proxy->supernode_pool_started = 0;
}

static void scaleout_notify_sleep(uint32_t interval_us) {
    struct timespec ts = {
        .tv_sec = interval_us / 1000000u,
        .tv_nsec = (long)(interval_us % 1000000u) * 1000L,
    };
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
    }
}

static int scaleout_notify_send_tcp(
        const vemb_v16_topology_endpoint_t *endpoint,
        const vemb_v16_scaleout_local_done_req_t *req,
        vemb_v16_scaleout_local_done_resp_t *resp) {
    int fd = vemb_v16_net_connect(endpoint->host,
                                  endpoint->tcp_port,
                                  VEMB_V16_SCALEOUT_NOTIFY_TIMEOUT_MS);
    if (fd < 0)
        return -1;
    uint8_t req_buf[64];
    size_t req_len = 0;
    if (vemb_v16_scaleout_local_done_req_encode(req_buf,
                                                sizeof(req_buf),
                                                req,
                                                &req_len) != 0 ||
        vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_SCALEOUT_LOCAL_DONE,
                                 0,
                                 0,
                                 0,
                                 req_buf,
                                 (uint32_t)req_len) != 0) {
        close(fd);
        return -1;
    }
    vemb_v16_net_hdr_t hdr;
    uint8_t resp_buf[64];
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_SCALEOUT_LOCAL_DONE_RESPONSE ||
        hdr.flags != 0 ||
        hdr.payload_len != vemb_v16_scaleout_local_done_resp_encoded_len() ||
        vemb_v16_net_read_full(fd, resp_buf, hdr.payload_len) != 0 ||
        vemb_v16_scaleout_local_done_resp_decode(resp,
                                                 resp_buf,
                                                 hdr.payload_len) != 0) {
        close(fd);
        return -1;
    }
    close(fd);
    return 0;
}

static int scaleout_notify_send(
        const vemb_v16_topology_endpoint_t *endpoint,
        const vemb_v16_scaleout_local_done_req_t *req,
        vemb_v16_scaleout_local_done_resp_t *resp) {
    if (endpoint->transport_type != VEMB_V16_TRANSPORT_TCP &&
        endpoint->transport_type != VEMB_V16_TRANSPORT_AERON)
        return -1;
    return scaleout_notify_send_tcp(endpoint, req, resp);
}

static int scaleout_notify_resp_matches(
        const vemb_v16_scaleout_local_done_req_t *req,
        const vemb_v16_scaleout_local_done_resp_t *resp) {
    return resp->status == VEMB_V16_STATUS_OK &&
           resp->migration_topology_epoch ==
               req->migration_topology_epoch &&
           resp->cutover_topology_epoch ==
               req->cutover_topology_epoch &&
           resp->source_owner == req->source_owner &&
           resp->notify_seq == req->notify_seq;
}

static void *scaleout_notify_main(void *arg) {
    vemb_v16_proxy_t *proxy = arg;
    int notify_failure_logged = 0;
    serverLog(LL_NOTICE,
              "vemb_v16 scaleout notify worker started: interval_us=%u",
              proxy->scaleout_notify_interval_us);
    while (atomic_load_explicit(&proxy->running, memory_order_relaxed) &&
           !atomic_load_explicit(&proxy->scaleout_notify_stop,
                                 memory_order_acquire)) {
        vemb_v16_storage_scaleout_auto_status_t status;
        memset(&status, 0, sizeof(status));
        if (vemb_v16_storage_scaleout_auto_get_status(proxy_storage(proxy),
                                                      &status) == 0 &&
            status.enabled &&
            status.coordinated &&
            status.phase == VEMB_V16_STORAGE_SCALEOUT_NOTIFY_PENDING &&
            status.coordinator_endpoint_valid) {
            vemb_v16_scaleout_local_done_req_t req = {
                .migration_topology_epoch = status.migration_epoch,
                .cutover_topology_epoch = status.cutover_epoch,
                .notify_seq = status.notify_seq,
                .source_owner = status.source_owner,
                .phase = status.phase,
                .error_code = status.last_error,
                .pending_delta = status.pending_delta,
                .baseline_retry_pending = status.baseline_retry_pending,
                .migrating_key_count = status.migrating_key_count,
                .range_count = status.range_count,
            };
            vemb_v16_scaleout_local_done_resp_t resp;
            int notify_rc = scaleout_notify_send(&status.coordinator_endpoint,
                                                  &req, &resp);
            if (notify_rc == 0 &&
                scaleout_notify_resp_matches(&req, &resp)) {
                (void)vemb_v16_storage_scaleout_auto_mark_notified(
                    proxy_storage(proxy),
                    req.migration_topology_epoch,
                    req.source_owner,
                    req.notify_seq);
                notify_failure_logged = 0;
            } else if (!notify_failure_logged) {
                serverLog(LL_WARNING,
                          "vemb_v16 scaleout notify failed: owner=%u endpoint=%s:%u rc=%d errno=%d",
                          req.source_owner, status.coordinator_endpoint.host,
                          status.coordinator_endpoint.tcp_port, notify_rc,
                          errno);
                notify_failure_logged = 1;
            }
        }
        scaleout_notify_sleep(proxy->scaleout_notify_interval_us);
    }
    serverLog(LL_NOTICE, "vemb_v16 scaleout notify worker stopped");
    return NULL;
}

static int start_scaleout_notify_worker(vemb_v16_proxy_t *proxy) {
    if (proxy->scaleout_notify_thread_started)
        return 0;
    proxy->scaleout_notify_interval_us =
        proxy->scaleout_notify_interval_us ?
            proxy->scaleout_notify_interval_us :
            VEMB_V16_SCALEOUT_NOTIFY_INTERVAL_US;
    atomic_store_explicit(&proxy->scaleout_notify_stop,
                          0,
                          memory_order_release);
    if (pthread_create(&proxy->scaleout_notify_thread,
                       NULL,
                       scaleout_notify_main,
                       proxy) != 0) {
        return -1;
    }
    proxy->scaleout_notify_thread_started = 1;
    return 0;
}

static void stop_scaleout_notify_worker(vemb_v16_proxy_t *proxy) {
    if (!proxy->scaleout_notify_thread_started)
        return;
    atomic_store_explicit(&proxy->scaleout_notify_stop,
                          1,
                          memory_order_release);
    pthread_join(proxy->scaleout_notify_thread, NULL);
    proxy->scaleout_notify_thread_started = 0;
}

/// TCP control plane: write a small status response frame.
int vemb_v16_proxy_create(vemb_v16_proxy_t **out,
                          uint32_t vector_dim,
                          uint32_t max_vectors,
                          vemb_v16_storage_ctx_t *storage,
                          const vemb_v16_warm_regions_manifest_t *manifest) {
    assert(out != NULL);
    assert(storage != NULL);
    assert(vector_dim != 0);
    assert(vector_dim <= VEMB_V16_MAX_DIM);
    assert(max_vectors != 0);

    vemb_v16_proxy_t *proxy = zcalloc(sizeof(*proxy));
    RETURN_IF(!proxy, -1);
    proxy->vector_dim = vector_dim;
    proxy->vector_stride = vector_dim * sizeof(float);
    proxy->request_ring_slot_size =
        vemb_v16_aeron_req_slot_size(vector_dim);
    proxy->response_ring_slot_size = vemb_v16_aeron_resp_slot_size();
    proxy->max_vectors = max_vectors;
    proxy->listen_fd = -1;
    proxy->inject_pipe_rd = -1;
    proxy->inject_pipe_wr = -1;
    proxy->tcp_port = VEMB_V16_TCP_PORT;
    strncpy(proxy->tcp_host, VEMB_V16_TCP_HOST, sizeof(proxy->tcp_host) - 1);
    strncpy(proxy->aeron_ub_path,
            VEMB_V16_DEFAULT_AERON_UB_PATH,
            sizeof(proxy->aeron_ub_path) - 1);
    strncpy(proxy->aeron_response_ub_path,
            VEMB_V16_DEFAULT_AERON_RESPONSE_UB_PATH,
            sizeof(proxy->aeron_response_ub_path) - 1);
    atomic_init(&proxy->running, 0);
    atomic_init(&proxy->batch_request_size,
                VEMB_V16_BATCH_REQUEST_SIZE_DEFAULT);
    atomic_init(&proxy->next_channel_id, 1);
    atomic_init(&proxy->next_channel_index, 0);
    atomic_init(&proxy->next_v2_lane_index, 0);
    atomic_init(&proxy->scaleout_notify_stop, 0);
    proxy->scaleout_notify_interval_us =
        VEMB_V16_SCALEOUT_NOTIFY_INTERVAL_US;
    pthread_mutex_init(&proxy->channel_lifecycle_lock, NULL);
    pthread_mutex_init(&proxy->stats_lock, NULL);
    proxy->storage = storage;
    if (manifest &&
        manifest->has_job_plane_backend_type &&
        manifest->job_plane_path[0] != '\0') {
        proxy->job_pool_slots_backend_type = manifest->job_plane_backend_type;
        proxy->job_pool_slots_mmap_offset = manifest->job_plane_mmap_offset;
        strncpy(proxy->job_pool_slots_path,
                manifest->job_plane_path,
                sizeof(proxy->job_pool_slots_path) - 1);
        proxy->job_pool_slots_path[sizeof(proxy->job_pool_slots_path) - 1] = '\0';
    }

    serverLog(LL_NOTICE, "vemb_v16 proxy created: dim=%u max_vectors=%u vector_region=%s size=%zu job_pool_slots=%s backend=%u offset=%llu",
              proxy->vector_dim,
              proxy->max_vectors,
              vemb_v16_storage_vector_region_name(proxy_storage(proxy)),
              vemb_v16_storage_vector_region_size(proxy_storage(proxy)),
              proxy->job_pool_slots_path[0] ? proxy->job_pool_slots_path : "(heap)",
              proxy->job_pool_slots_backend_type,
              (unsigned long long)proxy->job_pool_slots_mmap_offset);
    *out = proxy;
    return 0;
}

int vemb_v16_proxy_enable_tcp(vemb_v16_proxy_t *proxy,
                              const char *host,
                              uint16_t port) {
    assert(proxy != NULL);
    assert(host != NULL);
    assert(host[0] != '\0');
    assert(strlen(host) < sizeof(proxy->tcp_host));
    if (proxy->tcp_enabled)
        return -1;
    strncpy(proxy->tcp_host, host, sizeof(proxy->tcp_host) - 1);
    proxy->tcp_host[sizeof(proxy->tcp_host) - 1] = '\0';
    proxy->tcp_port = port ? port : VEMB_V16_TCP_PORT;
    proxy->tcp_enabled = 1;
    proxy->data_transport_type = VEMB_V16_TRANSPORT_TCP;
    return 0;
}

int vemb_v16_proxy_enable_aeron_tcp_control(vemb_v16_proxy_t *proxy,
                                            const char *host,
                                            uint16_t port) {
    assert(proxy != NULL);
    assert(host != NULL);
    assert(host[0] != '\0');
    assert(strlen(host) < sizeof(proxy->tcp_host));
    if (proxy->tcp_enabled)
        return -1;
    strncpy(proxy->tcp_host, host, sizeof(proxy->tcp_host) - 1);
    proxy->tcp_host[sizeof(proxy->tcp_host) - 1] = '\0';
    proxy->tcp_port = port ? port : VEMB_V16_TCP_PORT;
    proxy->tcp_enabled = 1;
    proxy->data_transport_type = VEMB_V16_TRANSPORT_AERON;
    return 0;
}

int vemb_v16_proxy_set_aeron_ub_path(vemb_v16_proxy_t *proxy,
                                     const char *ub_path) {
    assert(proxy != NULL);
    if (!ub_path || !ub_path[0] || strlen(ub_path) >= sizeof(proxy->aeron_ub_path))
        return -1;
    strncpy(proxy->aeron_ub_path, ub_path, sizeof(proxy->aeron_ub_path) - 1);
    proxy->aeron_ub_path[sizeof(proxy->aeron_ub_path) - 1] = '\0';
    return 0;
}

int vemb_v16_proxy_set_aeron_response_ub_path(vemb_v16_proxy_t *proxy,
                                              const char *ub_path) {
    assert(proxy != NULL);
    if (!ub_path || !ub_path[0] ||
        strlen(ub_path) >= sizeof(proxy->aeron_response_ub_path))
        return -1;
    strncpy(proxy->aeron_response_ub_path, ub_path,
            sizeof(proxy->aeron_response_ub_path) - 1);
    proxy->aeron_response_ub_path[
        sizeof(proxy->aeron_response_ub_path) - 1] = '\0';
    return 0;
}

uint32_t vemb_v16_proxy_data_transport(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    return proxy->data_transport_type;
}

/* Enable the inject pipe so the Redis main thread can hand off VEMB fds that
 * it sniffed off its own listening ports.  Same-process cross-thread fd-pass:
 * the integer fd is valid in both threads (redis stole it from the conn by
 * setting conn->fd=-1, so the kernel descriptor stays open until the proxy's
 * side closes it). */
int vemb_v16_proxy_enable_inject(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    if (proxy->inject_pipe_rd >= 0) return 0; /* already enabled */
    int pipefd[2];
    if (pipe(pipefd) != 0) return -1;
    fcntl(pipefd[0], F_SETFL, fcntl(pipefd[0], F_GETFL, 0) | O_NONBLOCK);
    fcntl(pipefd[1], F_SETFL, fcntl(pipefd[1], F_GETFL, 0) | O_NONBLOCK);
    proxy->inject_pipe_rd = pipefd[0];
    proxy->inject_pipe_wr = pipefd[1];
    return 0;
}

int vemb_v16_proxy_enable_tcp_inject_only(vemb_v16_proxy_t *proxy,
                                          const char *host,
                                          uint16_t port) {
    assert(proxy != NULL);
    assert(host != NULL);
    assert(host[0] != '\0');
    assert(strlen(host) < sizeof(proxy->tcp_host));
    if (proxy->tcp_enabled || proxy->inject_only)
        return -1;
    if (vemb_v16_proxy_enable_inject(proxy) != 0)
        return -1;
    strncpy(proxy->tcp_host, host, sizeof(proxy->tcp_host) - 1);
    proxy->tcp_host[sizeof(proxy->tcp_host) - 1] = '\0';
    proxy->tcp_port = port ? port : VEMB_V16_TCP_PORT;
    proxy->data_transport_type = VEMB_V16_TRANSPORT_TCP;
    proxy->inject_only = 1;
    return 0;
}

int vemb_v16_proxy_enable_aeron_tcp_inject_only(vemb_v16_proxy_t *proxy,
                                                const char *host,
                                                uint16_t port) {
    assert(proxy != NULL);
    assert(host != NULL);
    assert(host[0] != '\0');
    assert(strlen(host) < sizeof(proxy->tcp_host));
    if (proxy->tcp_enabled || proxy->inject_only)
        return -1;
    if (vemb_v16_proxy_enable_inject(proxy) != 0)
        return -1;
    strncpy(proxy->tcp_host, host, sizeof(proxy->tcp_host) - 1);
    proxy->tcp_host[sizeof(proxy->tcp_host) - 1] = '\0';
    proxy->tcp_port = port ? port : VEMB_V16_TCP_PORT;
    proxy->data_transport_type = VEMB_V16_TRANSPORT_AERON;
    proxy->inject_only = 1;
    return 0;
}

int vemb_v16_proxy_inject_fd(vemb_v16_proxy_t *proxy, int fd) {
    if (!proxy || proxy->inject_pipe_wr < 0 || fd < 0) return -1;
    /* Make fd blocking for proxy's blocking reads */
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
    vemb_v16_net_set_tcp_nodelay(fd);
    vemb_v16_net_set_timeouts(fd, 10000);
    ssize_t w = write(proxy->inject_pipe_wr, &fd, sizeof(fd));
    return (w == sizeof(fd)) ? 0 : -1;
}

static int proxy_set_worker_count(vemb_v16_proxy_t *proxy,
                                  int pool_started,
                                  uint32_t count,
                                  uint32_t *dst) {
    assert(proxy != NULL);
    assert(dst != NULL);
    assert(!pool_started);
    if (count == 0)
        return -1;
    if (count > VEMB_V16_MAX_CHANNELS)
        return -1;
    *dst = count;
    return 0;
}

int vemb_v16_proxy_set_supernode_workers(vemb_v16_proxy_t *proxy,
                                         uint32_t workers) {
    return proxy_set_worker_count(proxy,
                                  proxy->supernode_pool_started,
                                  workers,
                                  &proxy->supernode_worker_count);
}

int vemb_v16_proxy_set_proxy_io_threads(vemb_v16_proxy_t *proxy,
                                         uint32_t threads) {
    return proxy_set_worker_count(proxy,
                                  proxy->proxy_io_pool_started,
                                  threads,
                                  &proxy->proxy_io_worker_count);
}

int vemb_v16_proxy_set_batch_request_size(vemb_v16_proxy_t *proxy,
                                          uint32_t size) {
    if (!proxy || size == 0 || size > VEMB_V16_BATCH_REQUEST_SIZE_MAX)
        return -1;
    atomic_store_explicit(&proxy->batch_request_size, size,
                          memory_order_release);
    return 0;
}

void vemb_v16_proxy_destroy(vemb_v16_proxy_t *proxy) {
    assert(proxy != NULL);
    vemb_v16_proxy_stop(proxy);
    stop_scaleout_notify_worker(proxy);
    stop_proxy_io_pool(proxy);
    stop_supernode_pool(proxy);
    free_job_shard_queues(proxy);
    free_job_return_queues(proxy);
    cleanup_proxy_io_job_pools(proxy);
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++)
        close_channel(&proxy->channels[i]);
    if (proxy->listen_fd >= 0) close(proxy->listen_fd);
    pthread_mutex_destroy(&proxy->channel_lifecycle_lock);
    pthread_mutex_destroy(&proxy->stats_lock);
    zfree(proxy);
}

/// Top-level control loop: TCP control with either TCP or UB data plane.
int vemb_v16_proxy_run(vemb_v16_proxy_t *proxy) {
    int rc = -1;
    apply_default_worker_counts(proxy);
    if (validate_pooled_worker_config(proxy) != 0)
        return -1;
    if (!proxy->tcp_enabled && !proxy->inject_only)
        return -1;
    if (proxy->inject_only &&
        proxy->data_transport_type != VEMB_V16_TRANSPORT_AERON &&
        proxy->data_transport_type != VEMB_V16_TRANSPORT_TCP)
        return -1;
    if (proxy->data_transport_type != VEMB_V16_TRANSPORT_AERON &&
        proxy->data_transport_type != VEMB_V16_TRANSPORT_TCP)
        return -1;
    atomic_store_explicit(&proxy->running, 1, memory_order_relaxed);

    vemb_v16_transport_listener_t listener = {0};
    if (proxy->inject_only) {
        listener = (vemb_v16_transport_listener_t){
            .name = "tcp-inject",
            .fd = -1,
            .handle_fd = NULL,
        };
    } else {
        if (vemb_v16_tcp_listen(proxy, 4096, &listener) != 0)
            goto cleanup;
    }
    proxy->listen_fd = listener.fd;
    if (init_job_shard_queues(proxy) != 0) {
        serverLog(LL_WARNING, "vemb_v16 job shard queue init failed: proxy_io_threads=%u supernode_workers=%u",
                  proxy->proxy_io_worker_count,
                  proxy->supernode_worker_count);
        goto cleanup;
    }
    if (init_job_return_queues(proxy) != 0) {
        serverLog(LL_WARNING, "vemb_v16 job return queue init failed: proxy_io_threads=%u supernode_workers=%u",
                  proxy->proxy_io_worker_count,
                  proxy->supernode_worker_count);
        goto cleanup;
    }
    if (init_proxy_io_job_pools(proxy) != 0) {
        serverLog(LL_WARNING, "vemb_v16 job pool init failed: proxy_io_threads=%u",
                  proxy->proxy_io_worker_count);
        goto cleanup;
    }
    if (start_supernode_pool(proxy) != 0) {
        serverLog(LL_WARNING, "vemb_v16 pooled supernode start failed: workers=%u",
                  proxy->supernode_worker_count);
        goto cleanup;
    }
    if (start_proxy_io_pool(proxy) != 0) {
        serverLog(LL_WARNING, "vemb_v16 proxy io pool start failed: threads=%u",
                  proxy->proxy_io_worker_count);
        goto cleanup;
    }
    if (start_scaleout_notify_worker(proxy) != 0) {
        serverLog(LL_WARNING, "vemb_v16 scaleout notify worker start failed");
        goto cleanup;
    }

    serverLog(LL_NOTICE, "vemb_v16 server ready: tcp_enabled=%s tcp=%s:%u data_transport=%u proxy_io_threads=%u supernode_workers=%u affinity=%s dim=%u max_vectors=%u vector_region=%s",
              proxy->tcp_enabled ? "yes" : "no",
              proxy->tcp_host,
              proxy->tcp_port,
              proxy->data_transport_type,
              proxy->proxy_io_worker_count,
              proxy->supernode_worker_count,
              proxy_worker_affinity_mode_name(),
              proxy->vector_dim,
              proxy->max_vectors,
              vemb_v16_storage_vector_region_name(proxy_storage(proxy)));

    if (!proxy->inject_only) {
        assert(listener.fd >= 0);
        assert(listener.name != NULL);
        assert(listener.handle_fd != NULL);
    }

    while (atomic_load_explicit(&proxy->running, memory_order_relaxed)) {
        int did_work = 0;
        if (!proxy->inject_only) {
            for (;;) {
                int cfd = accept(listener.fd, NULL, NULL);
                if (cfd < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                        break;
                    serverLog(LL_WARNING, "vemb_v16 %s accept failed: fd=%d errno=%d error=%s",
                              listener.name, listener.fd, errno, strerror(errno));
                    goto cleanup;
                }
                did_work = 1;
                listener.handle_fd(proxy, cfd);
            }
        }
        /* Drain fds injected from Redis main thread (protocol sniffing).  These
         * are always data-plane HELLO frames — control frames never go through
         * the pipe (server_integration.c dispatches them directly). */
        if (proxy->inject_pipe_rd >= 0) {
            for (;;) {
                int injected_fd;
                ssize_t r = read(proxy->inject_pipe_rd, &injected_fd, sizeof(injected_fd));
                if (r != sizeof(injected_fd)) break;
                did_work = 1;
                vemb_v16_tcp_handle_fd(proxy, injected_fd);
            }
        }
        if (!did_work) {
            struct timespec ts = {0, 1000000};
            nanosleep(&ts, NULL);
        }
        reap_inactive_tcp_channels(proxy);
    }
    rc = 0;

cleanup:
    atomic_store_explicit(&proxy->running, 0, memory_order_relaxed);
    stop_scaleout_notify_worker(proxy);
    stop_proxy_io_pool(proxy);
    stop_supernode_pool(proxy);
    free_job_shard_queues(proxy);
    free_job_return_queues(proxy);
    cleanup_proxy_io_job_pools(proxy);
    if (proxy->listen_fd >= 0) {
        close(proxy->listen_fd);
        proxy->listen_fd = -1;
    }
    if (proxy->inject_pipe_rd >= 0) {
        close(proxy->inject_pipe_rd);
        proxy->inject_pipe_rd = -1;
    }
    if (proxy->inject_pipe_wr >= 0) {
        close(proxy->inject_pipe_wr);
        proxy->inject_pipe_wr = -1;
    }
    return rc;
}

void vemb_v16_proxy_stop(vemb_v16_proxy_t *proxy) {
    RETURN_IF(!proxy);
    int was_running = atomic_exchange_explicit(&proxy->running, 0,
                                               memory_order_relaxed);
    if (!was_running) return;
    if (proxy->listen_fd >= 0) shutdown(proxy->listen_fd, SHUT_RDWR);
}

int vemb_v16_proxy_migration_mark_migrating(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_control_req_t *req,
    vemb_v16_migration_control_resp_t *resp) {
    if (!migration_control_req_valid(req)) {
        migration_control_fill_resp(resp, VEMB_V16_STATUS_ERR, NULL);
        return -1;
    }

    tlc_core_key_migration_info_t info = {0};
    int rc = vemb_v16_storage_migration_mark_migrating_in_shard(
        proxy_storage(proxy),
        req->key,
        req->key_len,
        req->key_hash,
        req->topology_epoch,
        req->target_owner,
        req->shard_id,
        &info);
    migration_control_fill_resp(resp,
                                rc == 0 ? VEMB_V16_STATUS_OK :
                                    VEMB_V16_STATUS_ERR,
                                rc == 0 ? &info : NULL);
    return rc;
}

int vemb_v16_proxy_migration_mark_cutover(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_control_req_t *req,
    vemb_v16_migration_control_resp_t *resp) {
    if (!migration_control_req_valid(req)) {
        migration_control_fill_resp(resp, VEMB_V16_STATUS_ERR, NULL);
        return -1;
    }

    tlc_core_key_migration_info_t info = {0};
    int rc = vemb_v16_storage_migration_mark_cutover_in_shard(
        proxy_storage(proxy),
        req->key,
        req->key_len,
        req->key_hash,
        req->topology_epoch,
        req->target_owner,
        req->shard_id,
        &info);
    migration_control_fill_resp(resp,
                                rc == 0 ? VEMB_V16_STATUS_OK :
                                    VEMB_V16_STATUS_ERR,
                                rc == 0 ? &info : NULL);
    return rc;
}

int vemb_v16_proxy_migration_mark_source_gc(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_control_req_t *req,
    vemb_v16_migration_control_resp_t *resp) {
    if (!migration_control_req_valid(req)) {
        migration_control_fill_resp(resp, VEMB_V16_STATUS_ERR, NULL);
        return -1;
    }

    tlc_core_key_migration_info_t info = {0};
    int rc = vemb_v16_storage_migration_mark_source_gc_in_shard(
        proxy_storage(proxy),
        req->key,
        req->key_len,
        req->key_hash,
        req->topology_epoch,
        req->target_owner,
        req->shard_id,
        &info);
    migration_control_fill_resp(resp,
                                rc == 0 ? VEMB_V16_STATUS_OK :
                                    VEMB_V16_STATUS_ERR,
                                rc == 0 ? &info : NULL);
    return rc;
}

int vemb_v16_proxy_migration_barrier(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_control_req_t *req,
    vemb_v16_migration_control_resp_t *resp) {
    if (!migration_control_req_valid(req)) {
        migration_control_fill_resp(resp, VEMB_V16_STATUS_ERR, NULL);
        return -1;
    }

    tlc_core_key_migration_info_t info = {0};
    vemb_v16_migration_outbox_stats_t outbox_stats = {0};
    int rc = vemb_v16_storage_migration_barrier_in_shard(
        proxy_storage(proxy),
        req->key,
        req->key_len,
        req->key_hash,
        req->topology_epoch,
        req->target_owner,
        req->shard_id,
        &info,
        &outbox_stats);
    migration_control_fill_resp(resp,
                                rc == 0 ? VEMB_V16_STATUS_OK :
                                    VEMB_V16_STATUS_ERR,
                                rc == 0 ? &info : NULL);
    if (rc == 0)
        migration_control_fill_outbox(resp, &outbox_stats);
    return rc;
}

int vemb_v16_proxy_migration_mark_migrating_batch(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_control_batch_req_t *req,
    vemb_v16_migration_control_batch_resp_t *resp) {
    resp->status = VEMB_V16_STATUS_ERR;

    if (req->entry_count == 0 ||
        req->entry_count > VEMB_V16_MIGRATION_CONTROL_MAX_BATCH) {
        return -1;
    }

    resp->entry_count = req->entry_count;
    int rc = 0;
    for (uint32_t i = 0; i < req->entry_count; i++) {
        if (vemb_v16_proxy_migration_mark_migrating(proxy,
                                                    &req->entries[i],
                                                    &resp->entries[i]) == 0) {
            resp->success_count++;
        } else {
            resp->error_count++;
            rc = -1;
        }
    }
    resp->status = rc == 0 ? VEMB_V16_STATUS_OK : VEMB_V16_STATUS_ERR;
    return rc;
}

static void migration_range_control_fill_error(
        vemb_v16_migration_range_control_resp_t *resp,
        const vemb_v16_migration_range_control_req_t *req) {
    resp->status = VEMB_V16_STATUS_ERR;
    resp->migration_topology_epoch = req->migration_topology_epoch;
    resp->cutover_topology_epoch = req->cutover_topology_epoch;
    resp->owner_epoch = req->cutover_topology_epoch;
    resp->target_owner = req->target_owner;
    resp->shard_id = req->shard_id;
    resp->page_limit = req->page_limit;
}

static int migration_range_control_req_valid(
        const vemb_v16_migration_range_control_req_t *req,
        int need_cutover_epoch) {
    return req->migration_topology_epoch != 0 &&
           req->target_owner != UINT32_MAX &&
           (!need_cutover_epoch ||
            req->cutover_topology_epoch >= req->migration_topology_epoch);
}

int vemb_v16_proxy_migration_range_barrier(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_range_control_req_t *req,
    vemb_v16_migration_range_control_resp_t *resp) {
    if (!migration_range_control_req_valid(req, 0)) {
        migration_range_control_fill_error(resp, req);
        return -1;
    }
    return vemb_v16_storage_migration_range_barrier(
        proxy_storage(proxy),
        req->migration_topology_epoch,
        req->target_owner,
        req->shard_id,
        req->page_limit,
        resp);
}

int vemb_v16_proxy_migration_range_mark_cutover(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_range_control_req_t *req,
    vemb_v16_migration_range_control_resp_t *resp) {
    if (!migration_range_control_req_valid(req, 1)) {
        migration_range_control_fill_error(resp, req);
        return -1;
    }
    return vemb_v16_storage_migration_range_mark_cutover(
        proxy_storage(proxy),
        req->migration_topology_epoch,
        req->cutover_topology_epoch,
        req->target_owner,
        req->shard_id,
        req->page_limit,
        resp);
}

int vemb_v16_proxy_migration_range_mark_source_gc(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_migration_range_control_req_t *req,
    vemb_v16_migration_range_control_resp_t *resp) {
    if (!migration_range_control_req_valid(req, 1)) {
        migration_range_control_fill_error(resp, req);
        return -1;
    }
    return vemb_v16_storage_migration_range_mark_source_gc(
        proxy_storage(proxy),
        req->migration_topology_epoch,
        req->cutover_topology_epoch,
        req->target_owner,
        req->shard_id,
        req->page_limit,
        resp);
}

int vemb_v16_proxy_epoch_set(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_epoch_control_req_t *req,
    vemb_v16_epoch_control_resp_t *resp) {
    int rc = (req->min_write_epoch > req->current_topology_epoch) ?
        -1 :
        vemb_v16_storage_epoch_set(proxy_storage(proxy),
                                   req->current_topology_epoch,
                                   req->min_write_epoch);
    epoch_control_fill_resp(proxy,
                            resp,
                            rc == 0 ? VEMB_V16_STATUS_OK :
                                VEMB_V16_STATUS_ERR);
    return rc;
}

int vemb_v16_proxy_epoch_get(
    vemb_v16_proxy_t *proxy,
    vemb_v16_epoch_control_resp_t *resp) {
    epoch_control_fill_resp(proxy, resp, VEMB_V16_STATUS_OK);
    return 0;
}


static int proxy_peer_view_map_req_valid(vemb_v16_proxy_t *proxy,
                                         const vemb_v16_peer_view_map_req_t *req) {
    vemb_v16_storage_ctx_t *storage = proxy_storage(proxy);
    RETURN_IF(req->expected_local_owner_valid &&
              req->expected_local_owner_id != storage->local_owner_id,
              -1);
    RETURN_IF(req->region_count > VEMB_V16_PEER_VIEW_MAP_MAX_REGIONS ||
              req->remote_meta_view_count > VEMB_V16_PEER_VIEW_MAP_MAX_REMOTE_META_VIEWS ||
              req->ub_rpc_peer_count > VEMB_V16_PEER_VIEW_MAP_MAX_UB_RPC_PEERS,
              -1);
    return 0;
}

static int proxy_topology_transport_matches_startup(
        vemb_v16_proxy_t *proxy,
        const vemb_v16_topology_control_req_t *req) {
    RETURN_IF(req->endpoint_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS,
              -1);
    for (uint32_t i = 0; i < req->endpoint_count; i++) {
        const vemb_v16_topology_endpoint_t *endpoint = &req->endpoints[i];
        if (endpoint->transport_type == proxy->data_transport_type)
            continue;
        serverLog(LL_WARNING,
                  "vemb_v16 topology endpoint transport rejected: "
                  "server=%s endpoint=%s owner=%u port=%u",
                  vemb_v16_transport_name(proxy->data_transport_type),
                  vemb_v16_transport_name(endpoint->transport_type),
                  endpoint->owner_id, endpoint->tcp_port);
        return -1;
    }
    return 0;
}

// Attach peer owners from stored mapping before the next topology publish.
static int proxy_topology_attach_peer_owners_from_mapping(
        vemb_v16_proxy_t *proxy,
        const vemb_v16_topology_control_req_t *req,
        const vemb_v16_topology_ring_t *standby_ring) {
    RETURN_IF(req->endpoint_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS, -1);
    vemb_v16_storage_ctx_t *storage = proxy_storage(proxy);
    for (uint32_t i = 0; i < req->endpoint_count; i++) {
        const vemb_v16_topology_endpoint_t *endpoint = &req->endpoints[i];
        if (!vemb_v16_topology_owner_exists(standby_ring, endpoint->owner_id) ||
            endpoint->owner_id == storage->local_owner_id ||
            vemb_v16_storage_has_region_for_owner(storage, endpoint->owner_id)) {
            continue;
        }
        if (vemb_v16_storage_attach_peer_owner_from_mapping(storage, endpoint->owner_id) != 0 ||
            !vemb_v16_storage_has_region_for_owner(storage, endpoint->owner_id)) {
            serverLog(LL_WARNING,
                      "vemb_v16 peer-view map attach failed: local_owner=%u peer_owner=%u transport=%u",
                      storage->local_owner_id,
                      endpoint->owner_id,
                      endpoint->transport_type);
            return -1;
        }
    }
    return 0;
}

int vemb_v16_proxy_store_peer_view_map(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_peer_view_map_req_t *req,
    vemb_v16_peer_view_map_resp_t *resp) {
    if (proxy_peer_view_map_req_valid(proxy, req) != 0) {
        resp->status = VEMB_V16_STATUS_ERR;
        return -1;
    }
    int rc = vemb_v16_storage_store_peer_view_map(proxy_storage(proxy),
                                                  req,
                                                  resp);
    if (rc != 0)
        resp->status = VEMB_V16_STATUS_ERR;
    return rc;
}

// Call after `vemb_v16_proxy_store_peer_view_map()` for new peer owners.
int vemb_v16_proxy_topology_set(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_topology_control_req_t *req,
    vemb_v16_topology_control_resp_t *resp) {
    vemb_v16_topology_ring_t active_ring;
    vemb_v16_topology_ring_t standby_ring;
    int rc = proxy_topology_transport_matches_startup(proxy, req);
    GOTO_IF(rc != 0, label);
    rc = vemb_v16_storage_build_topology_rings(req,
                                              &active_ring,
                                              &standby_ring);
    GOTO_IF(rc != 0, label);
    rc = proxy_topology_attach_peer_owners_from_mapping(proxy,
                                                        req,
                                                        &standby_ring);
    GOTO_IF(rc != 0, label);
    rc = vemb_v16_storage_topology_set_with_rings(proxy_storage(proxy),
                                                  req,
                                                  &active_ring,
                                                  &standby_ring);
label:
    topology_control_fill_resp(proxy,
                               resp,
                               rc == 0
                                ? VEMB_V16_STATUS_OK
                                : VEMB_V16_STATUS_ERR);
    return rc;
}

int vemb_v16_proxy_topology_get(
    vemb_v16_proxy_t *proxy,
    vemb_v16_topology_control_resp_t *resp) {
    topology_control_fill_resp(proxy, resp, VEMB_V16_STATUS_OK);
    return 0;
}

int vemb_v16_proxy_apply_peer_view_map_and_topology_set(
    vemb_v16_proxy_t *proxy,
    const vemb_v16_peer_view_topology_control_req_t *req,
    vemb_v16_peer_view_topology_control_resp_t *resp) {
    resp->status = VEMB_V16_STATUS_ERR;
    resp->peer_view_map_status = VEMB_V16_STATUS_ERR;
    resp->topology_status = VEMB_V16_STATUS_ERR;

    if (proxy_topology_transport_matches_startup(
            proxy, &req->topology_req) != 0)
        return -1;
    if (proxy_peer_view_map_req_valid(proxy, &req->peer_view_map_req) != 0)
        return -1;

    // Stage-1: store peer-view mapping before any topology publish.
    int rc = vemb_v16_storage_store_peer_view_map(proxy_storage(proxy),
                                                  &req->peer_view_map_req,
                                                  &resp->peer_view_map_resp);
    if (rc != 0) {
        resp->peer_view_map_resp.status = VEMB_V16_STATUS_ERR;
        return -1;
    }
    resp->peer_view_map_status = resp->peer_view_map_resp.status;
    if (resp->peer_view_map_status != VEMB_V16_STATUS_OK)
        return -1;

    vemb_v16_topology_ring_t active_ring;
    vemb_v16_topology_ring_t standby_ring;
    if (vemb_v16_storage_build_topology_rings(&req->topology_req,
                                              &active_ring,
                                              &standby_ring) != 0)
        return -1;

    // Stage-2: attach peer owners from stored mapping.
    if (proxy_topology_attach_peer_owners_from_mapping(proxy,
                                                       &req->topology_req,
                                                       &standby_ring) != 0)
        return -1;

    resp->topology_attempted = 1;

    // Stage-3: publish topology into storage runtime state.
    rc = vemb_v16_storage_topology_set_with_rings(proxy_storage(proxy),
                                                  &req->topology_req,
                                                  &active_ring,
                                                  &standby_ring);
    topology_control_fill_resp(proxy,
                               &resp->topology_resp,
                               rc == 0 ? 
                                  VEMB_V16_STATUS_OK :
                                  VEMB_V16_STATUS_ERR);
    resp->topology_status = resp->topology_resp.status;
    if (rc != 0)
        return -1;
    resp->status = VEMB_V16_STATUS_OK;
    return 0;
}

void vemb_v16_proxy_get_stats(vemb_v16_proxy_t *proxy, vemb_v16_stats_t *stats) {
    assert(proxy != NULL);
    assert(stats != NULL);
    memset(stats, 0, sizeof(*stats));
    pthread_mutex_lock(&proxy->stats_lock);
    vemb_v16_stats_add(stats, &proxy->closed_stats);
    pthread_mutex_unlock(&proxy->stats_lock);
    sve_operation_stats_t *sve_stats =
        vemb_v16_storage_sve_stats(proxy_storage(proxy));
    if (sve_stats) {
        stats->bitmap_lock_success =
            atomic_load_explicit(&sve_stats->lock_success, memory_order_relaxed);
        stats->bitmap_lock_failure =
            atomic_load_explicit(&sve_stats->lock_failure, memory_order_relaxed);
    }
    tlc_core_stats_t core_stats;
    tlc_core_get_stats(proxy_storage(proxy)->tlc->core, &core_stats);
    stats->warm_region_count = core_stats.warm_region_count;
    stats->warm_region_full_count = core_stats.warm_region_full_count;
    stats->warm_alloc_local = core_stats.warm_alloc_local;
    stats->warm_alloc_remote = core_stats.warm_alloc_remote;
    stats->warm_alloc_fallback = core_stats.warm_alloc_fallback;
    stats->warm_alloc_cold_spill = core_stats.warm_alloc_cold_spill;
    stats->warm_alloc_fail = core_stats.warm_alloc_fail;
    stats->warm_eviction_success = core_stats.warm_eviction_success;
    stats->warm_eviction_fail = core_stats.warm_eviction_fail;
    stats->warm_same_key_overwrite = core_stats.warm_same_key_overwrite;
    stats->warm_stale_handle_reject = core_stats.warm_stale_handle_reject;
    stats->remote_meta_stale = core_stats.remote_meta_stale;
    stats->warm_region_hash_local_pct = core_stats.warm_region_hash_local_pct;
    vemb_v16_stats_t tlc_runtime_stats;
    memset(&tlc_runtime_stats, 0, sizeof(tlc_runtime_stats));
    vemb_v16_tlc_get_runtime_stats(proxy_storage(proxy)->tlc,
                                   &tlc_runtime_stats);
    vemb_v16_stats_add(stats, &tlc_runtime_stats);
    stats->source_gc_count += atomic_load_explicit(
        &proxy_storage(proxy)->migration_source_gc_count,
        memory_order_relaxed);
    uint64_t gc_safe_watermark = atomic_load_explicit(
        &proxy_storage(proxy)->migration_gc_safe_watermark,
        memory_order_relaxed);
    if (stats->gc_safe_watermark < gc_safe_watermark)
        stats->gc_safe_watermark = gc_safe_watermark;
    stats->migration_baseline_sent += atomic_load_explicit(
        &proxy_storage(proxy)->migration_baseline_sent_count,
        memory_order_relaxed);
    stats->migration_baseline_skipped += atomic_load_explicit(
        &proxy_storage(proxy)->migration_baseline_skipped_count,
        memory_order_relaxed);
    stats->migration_baseline_error += atomic_load_explicit(
        &proxy_storage(proxy)->migration_baseline_error_count,
        memory_order_relaxed);
    stats->migration_baseline_retry_queued += atomic_load_explicit(
        &proxy_storage(proxy)->migration_baseline_retry_queued_count,
        memory_order_relaxed);
    stats->migration_baseline_retry_sent += atomic_load_explicit(
        &proxy_storage(proxy)->migration_baseline_retry_sent_count,
        memory_order_relaxed);
    pthread_mutex_lock(&proxy_storage(proxy)->migration_outbox_lock);
    stats->migration_baseline_retry_pending +=
        proxy_storage(proxy)->migration_baseline_retry_count;
    pthread_mutex_unlock(&proxy_storage(proxy)->migration_outbox_lock);
    if (proxy->job_shard_queues) {
        uint32_t count = proxy->job_shard_proxy_count *
            proxy->job_shard_supernode_count;
        for (uint32_t i = 0; i < count; i++) {
            stats->job_shard_queue_depth +=
                vemb_v16_aeron_available(&proxy->job_shard_queues[i].ring);
        }
    }
    pthread_mutex_lock(&proxy->channel_lifecycle_lock);
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        vemb_v16_channel_t *ch = &proxy->channels[i];
        if (atomic_load_explicit(&ch->active, memory_order_acquire)) {
            stats->active_channels++;
            vemb_v16_stats_add_channel_counters(stats, &ch->stats);
            if (ch->request_ring)
                stats->request_ring_depth += vemb_v16_client_available(ch->request_ring);
            if (ch->response_ring)
                stats->response_ring_depth += vemb_v16_client_available(ch->response_ring);
            stats->completion_ring_depth += vemb_v16_aeron_available(&ch->completion_ring);
        }
    }
    pthread_mutex_unlock(&proxy->channel_lifecycle_lock);
}

void vemb_v16_proxy_get_diagnostic_stats(
        vemb_v16_proxy_t *proxy, vemb_v16_diagnostic_stats_t *stats) {
    assert(proxy != NULL);
    assert(stats != NULL);
    *stats = (vemb_v16_diagnostic_stats_t){
        .version = VEMB_V16_DIAGNOSTIC_STATS_VERSION,
        .bytes = sizeof(*stats),
    };

    tlc_core_stats_t core_stats;
    tlc_core_get_stats(proxy_storage(proxy)->tlc->core, &core_stats);
    stats->lookup_cache_hit = core_stats.lookup_cache_hit;
    stats->lookup_cache_miss = core_stats.lookup_cache_miss;
    stats->warm_local_hit = core_stats.lookup_warm_local_hit;
    stats->warm_imported_hit = core_stats.lookup_warm_imported_hit;
    stats->cold_promote = core_stats.lookup_cold_promote;
    stats->lookup_final_miss = core_stats.lookup_final_miss;

    vemb_v16_tlc_lookup_diagnostic_stats_t handle_stats;
    vemb_v16_tlc_get_lookup_diagnostic_stats(proxy_storage(proxy)->tlc,
                                             &handle_stats);
    stats->handle_lookup_miss = handle_stats.handle_lookup_miss;
    stats->handle_lookup_miss_not_found =
        handle_stats.handle_lookup_miss_not_found;
    stats->handle_lookup_miss_moved = handle_stats.handle_lookup_miss_moved;
    stats->handle_lookup_miss_stale = handle_stats.handle_lookup_miss_stale;

    tlc_core_region_stats_t regions[TLC_CORE_MAX_TOTAL_WARM_REGIONS];
    uint32_t region_count = tlc_core_get_region_stats(
        proxy_storage(proxy)->tlc->core, regions,
        TLC_CORE_MAX_TOTAL_WARM_REGIONS);
    if (region_count > VEMB_V16_DIAGNOSTIC_MAX_REGIONS)
        region_count = VEMB_V16_DIAGNOSTIC_MAX_REGIONS;
    stats->region_count = region_count;
    for (uint32_t i = 0; i < region_count; i++) {
        stats->regions[i] = (vemb_v16_diagnostic_region_stats_t){
            .region_id = regions[i].region_id,
            .region_index = i,
            .is_local = regions[i].is_local,
            .lookup_hits = regions[i].lookup_hits,
            .cold_promotes = regions[i].cold_promotes,
        };
    }

    pthread_mutex_lock(&proxy->channel_lifecycle_lock);
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        vemb_v16_channel_t *ch = &proxy->channels[i];
        uint64_t channel_id = atomic_load_explicit(
            &ch->slot_channel_id, memory_order_acquire);
        if (channel_id == 0)
            continue;
        if (stats->channel_count < VEMB_V16_DIAGNOSTIC_MAX_CHANNELS) {
            uint32_t out_index = stats->channel_count++;
            uint_fast32_t proxy_io_state = atomic_load_explicit(
                &ch->proxy_io_state, memory_order_acquire);
            uint_fast32_t supernode_state = atomic_load_explicit(
                &ch->supernode_state, memory_order_acquire);
            vemb_v16_diagnostic_channel_stats_t *out =
                &stats->channels[out_index];
            *out = (vemb_v16_diagnostic_channel_stats_t){
                .channel_id = channel_id,
                .index = i,
                .transport_type = ch->transport_type,
                .proxy_io_worker_id = ch->proxy_io_worker_id,
                .supernode_worker_id = ch->supernode_worker_id,
                .active = atomic_load_explicit(&ch->active,
                                               memory_order_acquire) != 0,
                .proxy_io_closing =
                    (proxy_io_state & VEMB_V16_PROXY_IO_STATE_CLOSING) != 0,
                .supernode_closing =
                    (supernode_state & VEMB_V16_SUPERNODE_STATE_CLOSING) != 0,
                .request_ring_depth = ch->request_ring
                    ? vemb_v16_client_available(ch->request_ring) : 0,
                .response_ring_depth = ch->response_ring
                    ? vemb_v16_client_available(ch->response_ring) : 0,
                .completion_ring_depth = vemb_v16_aeron_available(
                    &ch->completion_ring),
            };
        } else {
            stats->channel_stats_truncated = 1;
        }
        if (atomic_load_explicit(&ch->active, memory_order_acquire))
            stats->active_channel_count++;
        if ((atomic_load_explicit(&ch->proxy_io_state,
                                  memory_order_acquire) &
             VEMB_V16_PROXY_IO_STATE_CLOSING) != 0 ||
            (atomic_load_explicit(&ch->supernode_state,
                                  memory_order_acquire) &
             VEMB_V16_SUPERNODE_STATE_CLOSING) != 0)
            stats->closing_channel_count++;
    }
    pthread_mutex_unlock(&proxy->channel_lifecycle_lock);
}
