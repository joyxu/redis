#include "tlc_core.h"
#include "vemb_v16_tlc.h"
#include "vemb_v16_remote_meta.h"
#include "cpu_relax.h"
#include "vemb_v16_log.h"
#include "vemb_v16_util.h"
#include "zmalloc.h"
#include "macro.h"

#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#define VEMB_V16_REMOTE_META_PUBLISH_QUEUE_CAP 1024u
#define VEMB_V16_TLC_LOG_LIMIT 32u
#define VEMB_V16_TLC_VECTOR_COPY_RETRIES 1024u

typedef struct vemb_v16_remote_meta_publish_event {
    vemb_v16_remote_meta_view_t *target_view;
    uint64_t key_hash;
    uint32_t key_len;
    uint32_t is_repair;
    char key[VEMB_V16_MAX_KEY_LEN];
    vemb_v16_vector_handle_t handle;
} vemb_v16_remote_meta_publish_event_t;

typedef struct vemb_v16_remote_meta_publish_slot {
    _Alignas(64) atomic_uint_fast64_t sequence;
    vemb_v16_remote_meta_publish_event_t event;
} vemb_v16_remote_meta_publish_slot_t;

struct vemb_v16_tlc_remote_meta_publisher {
    pthread_t thread;
    int thread_started;
    atomic_int stop;
    _Alignas(64) atomic_uint_fast64_t head;
    _Alignas(64) atomic_uint_fast64_t tail;
    atomic_uint_fast32_t active_consumers;
    uint32_t capacity;
    uint32_t mask;
    vemb_v16_remote_meta_publish_slot_t *slots;
    vemb_v16_tlc_t *tlc;
};

static atomic_uint_fast32_t remote_meta_stale_logs = ATOMIC_VAR_INIT(0);
static atomic_uint_fast32_t remote_meta_async_drop_logs = ATOMIC_VAR_INIT(0);
static atomic_uint_fast32_t remote_meta_publish_busy_logs = ATOMIC_VAR_INIT(0);
static atomic_uint_fast32_t remote_meta_publish_evict_logs = ATOMIC_VAR_INIT(0);
static atomic_uint_fast32_t remote_meta_set_conflict_logs = ATOMIC_VAR_INIT(0);

struct vemb_v16_tlc_access_snapshot {
    atomic_uint_fast32_t refcount;
    atomic_uint retired;
    uint32_t remote_meta_view_count;
    vemb_v16_tlc_remote_meta_owner_view_t
        remote_meta_views[VEMB_V16_TLC_MAX_REMOTE_META_VIEWS];
    vemb_v16_tlc_lookup_rpc_fn lookup_rpc;
    void *lookup_rpc_arg;
    vemb_v16_ub_rpc_t *lookup_rpc_runtime;
};

static void access_snapshot_release(vemb_v16_tlc_access_snapshot_t *snapshot);

static vemb_v16_tlc_access_snapshot_t *access_snapshot_create_empty(void) {
    vemb_v16_tlc_access_snapshot_t *snapshot = zcalloc(sizeof(*snapshot));
    if (!snapshot)
        return NULL;
    atomic_init(&snapshot->refcount, 1);
    atomic_init(&snapshot->retired, 0);
    return snapshot;
}

static vemb_v16_tlc_access_snapshot_t *access_snapshot_clone(
        vemb_v16_tlc_access_snapshot_t *src) {
    vemb_v16_tlc_access_snapshot_t *snapshot = access_snapshot_create_empty();
    if (!snapshot)
        return NULL;
    if (!src)
        return snapshot;
    snapshot->remote_meta_view_count = src->remote_meta_view_count;
    memcpy(snapshot->remote_meta_views,
           src->remote_meta_views,
           sizeof(snapshot->remote_meta_views));
    snapshot->lookup_rpc = src->lookup_rpc;
    snapshot->lookup_rpc_arg = src->lookup_rpc_arg;
    snapshot->lookup_rpc_runtime = src->lookup_rpc_runtime;
    return snapshot;
}

static void access_snapshot_retire(vemb_v16_tlc_access_snapshot_t *snapshot) {
    if (!snapshot)
        return;
    atomic_store_explicit(&snapshot->retired, 1, memory_order_release);
    access_snapshot_release(snapshot);
}

static vemb_v16_tlc_access_snapshot_t *access_snapshot_acquire(
        vemb_v16_tlc_t *tlc) {
    if (!tlc)
        return NULL;
    for (;;) {
        vemb_v16_tlc_access_snapshot_t *snapshot = atomic_load_explicit(
            &tlc->access_snapshot,
            memory_order_acquire);
        if (!snapshot)
            return NULL;
        atomic_fetch_add_explicit(&snapshot->refcount, 1, memory_order_acq_rel);
        if (snapshot == atomic_load_explicit(&tlc->access_snapshot,
                                             memory_order_acquire)) {
            return snapshot;
        }
        access_snapshot_release(snapshot);
    }
}

static void access_snapshot_release(vemb_v16_tlc_access_snapshot_t *snapshot) {
    if (!snapshot)
        return;
    uint32_t prev = atomic_fetch_sub_explicit(&snapshot->refcount,
                                              1,
                                              memory_order_acq_rel);
    if (prev == 1 &&
        atomic_load_explicit(&snapshot->retired, memory_order_acquire)) {
        zfree(snapshot);
    }
}

static void tlc_sync_access_snapshot_legacy_fields(
        vemb_v16_tlc_t *tlc,
        const vemb_v16_tlc_access_snapshot_t *snapshot) {
    if (!tlc || !snapshot)
        return;
    tlc->remote_meta_view_count = snapshot->remote_meta_view_count;
    memcpy(tlc->remote_meta_views,
           snapshot->remote_meta_views,
           sizeof(tlc->remote_meta_views));
    tlc->lookup_rpc = snapshot->lookup_rpc;
    tlc->lookup_rpc_arg = snapshot->lookup_rpc_arg;
    tlc->lookup_rpc_runtime = snapshot->lookup_rpc_runtime;
    atomic_store_explicit(&tlc->current_lookup_rpc_runtime,
                          snapshot->lookup_rpc_runtime,
                          memory_order_release);
}

static vemb_v16_tlc_access_snapshot_t *access_snapshot_publish(
        vemb_v16_tlc_t *tlc,
        vemb_v16_tlc_access_snapshot_t *snapshot) {
    vemb_v16_tlc_access_snapshot_t *old_snapshot = atomic_exchange_explicit(
        &tlc->access_snapshot,
        snapshot,
        memory_order_acq_rel);
    tlc_sync_access_snapshot_legacy_fields(tlc, snapshot);
    return old_snapshot;
}

static int access_snapshot_update(vemb_v16_tlc_t *tlc,
                                  int (*mutate)(vemb_v16_tlc_access_snapshot_t *snapshot,
                                                void *arg),
                                  void *arg) {
    RETURN_IF(!tlc || !mutate, -1);
    pthread_mutex_lock(&tlc->access_snapshot_update_lock);
    vemb_v16_tlc_access_snapshot_t *current = atomic_load_explicit(
        &tlc->access_snapshot,
        memory_order_acquire);
    vemb_v16_tlc_access_snapshot_t *next = access_snapshot_clone(current);
    if (!next) {
        pthread_mutex_unlock(&tlc->access_snapshot_update_lock);
        return -1;
    }
    int rc = mutate(next, arg);
    if (rc == 0) {
        vemb_v16_tlc_access_snapshot_t *old_snapshot =
            access_snapshot_publish(tlc, next);
        access_snapshot_retire(old_snapshot);
    } else {
        access_snapshot_retire(next);
    }
    pthread_mutex_unlock(&tlc->access_snapshot_update_lock);
    return rc;
}

static int tlc_log_should(atomic_uint_fast32_t *counter) {
    uint32_t n = atomic_fetch_add_explicit(counter, 1,
                                          memory_order_relaxed);
    return n < VEMB_V16_TLC_LOG_LIMIT;
}

static void tlc_queue_pause(void) {
    for (uint32_t i = 0; i < 64; i++)
        cpu_relax();
    sched_yield();
}

static uint32_t default_remote_meta_owner_resolver(uint64_t key_hash,
                                                   const char *key,
                                                   uint32_t key_len,
                                                   void *arg) {
    (void)key_hash;
    (void)key;
    (void)key_len;
    vemb_v16_remote_meta_view_t *view = arg;
    return view->header->owner_supernode_id;
}

static void make_handle(vemb_v16_tlc_t *tlc,
                        uint64_t key_hash,
                        const tlc_warm_location_t *location,
                        vemb_v16_vector_handle_t *handle) {
    (void)tlc;
    handle->region_id = location->region_id;
    handle->bytes = location->bytes;
    handle->local_slot = location->local_slot;
    handle->reserved0 = 0;
    handle->offset = location->offset;
    handle->key_hash = key_hash;
    handle->owner_generation = location->owner_generation;
}

static void tlc_counter_add(atomic_uint_fast64_t *counter, uint64_t value) {
    atomic_fetch_add_explicit(counter, value, memory_order_relaxed);
}

static uint64_t tlc_counter_load(atomic_uint_fast64_t *counter) {
    return atomic_load_explicit(counter, memory_order_relaxed);
}

static void tlc_init_counters(vemb_v16_tlc_t *tlc) {
    atomic_init(&tlc->ub_lookup_rpc_next_request_id, 1);
    atomic_init(&tlc->remote_meta_lookup_hit, 0);
    atomic_init(&tlc->remote_meta_lookup_miss, 0);
    atomic_init(&tlc->remote_meta_lookup_busy, 0);
    atomic_init(&tlc->remote_meta_lookup_way_probe, 0);
    atomic_init(&tlc->remote_meta_lookup_set_conflict, 0);
    atomic_init(&tlc->remote_meta_publish_async_enqueue, 0);
    atomic_init(&tlc->remote_meta_publish_async_drop, 0);
    atomic_init(&tlc->remote_meta_publish_async_coalesce, 0);
    atomic_init(&tlc->remote_meta_publish_ok, 0);
    atomic_init(&tlc->remote_meta_publish_busy, 0);
    atomic_init(&tlc->remote_meta_publish_insert, 0);
    atomic_init(&tlc->remote_meta_publish_update, 0);
    atomic_init(&tlc->remote_meta_publish_evict, 0);
    atomic_init(&tlc->remote_meta_publish_ns, 0);
    atomic_init(&tlc->ub_lookup_rpc_count, 0);
    atomic_init(&tlc->ub_lookup_rpc_ok, 0);
    atomic_init(&tlc->ub_lookup_rpc_not_found, 0);
    atomic_init(&tlc->ub_lookup_rpc_busy, 0);
    atomic_init(&tlc->ub_lookup_rpc_timeout, 0);
    atomic_init(&tlc->ub_lookup_rpc_error, 0);
    atomic_init(&tlc->ub_lookup_rpc_handle, 0);
    atomic_init(&tlc->ub_lookup_rpc_snapshot, 0);
    atomic_init(&tlc->ub_lookup_rpc_ns, 0);
    atomic_init(&tlc->remote_meta_repair_enqueue, 0);
    atomic_init(&tlc->remote_meta_repair_ok, 0);
    atomic_init(&tlc->remote_meta_repair_drop, 0);
    atomic_init(&tlc->handle_lookup_miss, 0);
    atomic_init(&tlc->handle_lookup_miss_not_found, 0);
    atomic_init(&tlc->handle_lookup_miss_moved, 0);
    atomic_init(&tlc->handle_lookup_miss_stale, 0);
}

int publish_remote_meta_to_view(vemb_v16_tlc_t *tlc,
                                vemb_v16_remote_meta_view_t *view,
                                const char *key,
                                uint32_t key_len,
                                uint64_t key_hash,
                                const vemb_v16_vector_handle_t *handle,
                                int is_repair) {
    RETURN_IF(!tlc || !view || !key || !handle || key_len == 0 ||
              handle->bytes == 0,
              -1);
    vemb_v16_remote_meta_handle_t remote_handle = {
        .region_id = handle->region_id,
        .bytes = handle->bytes,
        .local_slot = handle->local_slot,
        .offset = handle->offset,
        .key_hash = key_hash,
        .owner_generation = handle->owner_generation,
    };
    vemb_v16_remote_meta_publish_result_t result = {0};
    int rc = vemb_v16_remote_meta_publish_with_result(view,
                                                      key,
                                                      key_len,
                                                      key_hash,
                                                      &remote_handle,
                                                      &result);
    if (rc == VEMB_V16_REMOTE_META_OK) {
        tlc_counter_add(&tlc->remote_meta_publish_ok, 1);
        if (is_repair)
            tlc_counter_add(&tlc->remote_meta_repair_ok, 1);
        if (result.action == VEMB_V16_REMOTE_META_PUBLISH_INSERT)
            tlc_counter_add(&tlc->remote_meta_publish_insert, 1);
        else if (result.action == VEMB_V16_REMOTE_META_PUBLISH_UPDATE)
            tlc_counter_add(&tlc->remote_meta_publish_update, 1);
        else if (result.action == VEMB_V16_REMOTE_META_PUBLISH_EVICT) {
            tlc_counter_add(&tlc->remote_meta_publish_evict, 1);
            if (tlc_log_should(&remote_meta_publish_evict_logs)) {
                serverLog(LL_NOTICE,
                          "vemb_v16 remote_meta publish evict: owner=%u key_hash=%llu set=%u way=%u repair=%d",
                          view->header->owner_supernode_id,
                          (unsigned long long)key_hash,
                          result.set_id,
                          result.way,
                          is_repair);
            }
        }
        return 0;
    }
    if (rc == VEMB_V16_REMOTE_META_BUSY) {
        tlc_counter_add(&tlc->remote_meta_publish_busy, 1);
        if (tlc_log_should(&remote_meta_publish_busy_logs)) {
            serverLog(LL_NOTICE,
                      "vemb_v16 remote_meta publish busy: owner=%u key_hash=%llu repair=%d",
                      view->header->owner_supernode_id,
                      (unsigned long long)key_hash,
                      is_repair);
        }
    }
    return -1;
}

static int remote_meta_publish_ring_init(
    vemb_v16_tlc_remote_meta_publisher_t *publisher,
    uint32_t capacity) {
    RETURN_IF(!publisher || capacity == 0 ||
              (capacity & (capacity - 1u)) != 0,
              -1);
    publisher->capacity = capacity;
    publisher->mask = capacity - 1u;
    atomic_init(&publisher->head, 0);
    atomic_init(&publisher->tail, 0);
    atomic_init(&publisher->active_consumers, 0);
    atomic_init(&publisher->stop, 0);
    if (posix_memalign((void **)&publisher->slots,
                       64,
                       sizeof(*publisher->slots) * capacity) != 0) {
        publisher->slots = NULL;
        return -1;
    }
    memset(publisher->slots, 0, sizeof(*publisher->slots) * capacity);
    for (uint32_t i = 0; i < capacity; i++)
        atomic_init(&publisher->slots[i].sequence, i);
    return 0;
}

static void remote_meta_publish_ring_destroy(
    vemb_v16_tlc_remote_meta_publisher_t *publisher) {
    if (!publisher)
        return;
    free(publisher->slots);
    publisher->slots = NULL;
}

static int remote_meta_publish_ring_offer(
    vemb_v16_tlc_remote_meta_publisher_t *publisher,
    const vemb_v16_remote_meta_publish_event_t *event) {
    uint64_t pos =
        atomic_load_explicit(&publisher->tail, memory_order_relaxed);
    for (;;) {
        vemb_v16_remote_meta_publish_slot_t *slot =
            &publisher->slots[pos & publisher->mask];
        uint64_t seq = atomic_load_explicit(&slot->sequence,
                                            memory_order_acquire);
        int64_t diff = (int64_t)seq - (int64_t)pos;
        if (diff == 0) {
            uint64_t desired = pos + 1;
            if (atomic_compare_exchange_weak_explicit(
                    &publisher->tail,
                    &pos,
                    desired,
                    memory_order_relaxed,
                    memory_order_relaxed)) {
                slot->event = *event;
                atomic_store_explicit(&slot->sequence,
                                      pos + 1,
                                      memory_order_release);
                return 0;
            }
        } else if (diff < 0) {
            return -1;
        } else {
            pos = atomic_load_explicit(&publisher->tail,
                                       memory_order_relaxed);
        }
    }
}

static int remote_meta_publish_ring_poll(
    vemb_v16_tlc_remote_meta_publisher_t *publisher,
    vemb_v16_remote_meta_publish_event_t *event) {
    uint64_t pos =
        atomic_load_explicit(&publisher->head, memory_order_relaxed);
    for (;;) {
        vemb_v16_remote_meta_publish_slot_t *slot =
            &publisher->slots[pos & publisher->mask];
        uint64_t seq = atomic_load_explicit(&slot->sequence,
                                            memory_order_acquire);
        int64_t diff = (int64_t)seq - (int64_t)(pos + 1);
        if (diff == 0) {
            uint64_t desired = pos + 1;
            if (atomic_compare_exchange_weak_explicit(
                    &publisher->head,
                    &pos,
                    desired,
                    memory_order_relaxed,
                    memory_order_relaxed)) {
                *event = slot->event;
                atomic_store_explicit(&slot->sequence,
                                      pos + publisher->capacity,
                                      memory_order_release);
                return 1;
            }
        } else if (diff < 0) {
            return 0;
        } else {
            pos = atomic_load_explicit(&publisher->head,
                                       memory_order_relaxed);
        }
    }
}

static uint64_t remote_meta_publish_ring_available(
    vemb_v16_tlc_remote_meta_publisher_t *publisher) {
    uint64_t head = atomic_load_explicit(&publisher->head,
                                         memory_order_acquire);
    uint64_t tail = atomic_load_explicit(&publisher->tail,
                                         memory_order_acquire);
    return tail - head;
}

static int remote_meta_publisher_drain_one(
    vemb_v16_tlc_remote_meta_publisher_t *publisher,
    vemb_v16_tlc_t *tlc) {
    vemb_v16_remote_meta_publish_event_t event;
    atomic_fetch_add_explicit(&publisher->active_consumers,
                              1,
                              memory_order_acq_rel);
    int got = remote_meta_publish_ring_poll(publisher, &event);
    if (got <= 0) {
        atomic_fetch_sub_explicit(&publisher->active_consumers,
                                  1,
                                  memory_order_acq_rel);
        return got;
    }

    (void)publish_remote_meta_to_view(tlc,
                                      event.target_view,
                                      event.key,
                                      event.key_len,
                                      event.key_hash,
                                      &event.handle,
                                      event.is_repair);
    atomic_fetch_sub_explicit(&publisher->active_consumers,
                              1,
                              memory_order_acq_rel);
    return 1;
}

static void *remote_meta_publisher_main(void *arg) {
    vemb_v16_tlc_remote_meta_publisher_t *publisher = arg;
    vemb_v16_tlc_t *tlc = publisher->tlc;
    for (;;) {
        int got = remote_meta_publisher_drain_one(publisher, tlc);
        if (got > 0)
            continue;
        if (atomic_load_explicit(&publisher->stop, memory_order_acquire))
            break;
        tlc_queue_pause();
    }
    while (remote_meta_publisher_drain_one(publisher, tlc) > 0) {
    }
    return NULL;
}

static int remote_meta_publisher_start(vemb_v16_tlc_t *tlc) {
    if (atomic_load_explicit(&tlc->remote_meta_publisher,
                             memory_order_acquire))
        return 0;
    vemb_v16_tlc_remote_meta_publisher_t *publisher =
        zcalloc(sizeof(*publisher));
    RETURN_IF(!publisher, -1);
    publisher->tlc = tlc;
    if (remote_meta_publish_ring_init(
            publisher,
            VEMB_V16_REMOTE_META_PUBLISH_QUEUE_CAP) != 0) {
        zfree(publisher);
        return -1;
    }
    if (pthread_create(&publisher->thread,
                       NULL,
                       remote_meta_publisher_main,
                       publisher) != 0) {
        remote_meta_publish_ring_destroy(publisher);
        zfree(publisher);
        return -1;
    }
    publisher->thread_started = 1;

    vemb_v16_tlc_remote_meta_publisher_t *expected = NULL;
    if (atomic_compare_exchange_strong_explicit(
            &tlc->remote_meta_publisher,
            &expected,
            publisher,
            memory_order_release,
            memory_order_acquire)) {
        return 0;
    }

    atomic_store_explicit(&publisher->stop, 1, memory_order_release);
    pthread_join(publisher->thread, NULL);
    remote_meta_publish_ring_destroy(publisher);
    zfree(publisher);
    return 0;
}

static void remote_meta_publisher_stop(vemb_v16_tlc_t *tlc) {
    vemb_v16_tlc_remote_meta_publisher_t *publisher =
        atomic_exchange_explicit(&tlc->remote_meta_publisher,
                                 NULL,
                                 memory_order_acq_rel);
    if (!publisher)
        return;
    atomic_store_explicit(&publisher->stop, 1, memory_order_release);
    if (publisher->thread_started)
        pthread_join(publisher->thread, NULL);
    remote_meta_publish_ring_destroy(publisher);
    zfree(publisher);
}

static uint32_t region_index_id_mapping(const vemb_v16_tlc_t *tlc,
                                        uint32_t region_id) {
    for (uint32_t i = 0; i < tlc->region_index_id_mapping_count; i++) {
        if (tlc->region_index_id_mappings[i].region_id == region_id)
            return tlc->region_index_id_mappings[i].region_index;
    }
    uint32_t runtime_count = (uint32_t)atomic_load_explicit(
        (const atomic_uint_fast32_t *)&tlc->runtime_warm_region_count,
        memory_order_acquire);
    for (uint32_t i = 0; i < runtime_count; i++) {
        if (tlc->runtime_region_index_id_mappings[i].region_id == region_id)
            return tlc->runtime_region_index_id_mappings[i].region_index;
    }
    return UINT32_MAX;
}

int enqueue_remote_meta_publish(vemb_v16_tlc_t *tlc,
                                vemb_v16_remote_meta_view_t *target_view,
                                const char *key,
                                uint32_t key_len,
                                uint64_t key_hash,
                                const vemb_v16_vector_handle_t *handle,
                                int is_repair) {
    if (!tlc || !target_view || !key || !handle ||
        key_len == 0 || key_len > VEMB_V16_MAX_KEY_LEN ||
        handle->bytes == 0 ||
        remote_meta_publisher_start(tlc) != 0) {
        if (is_repair)
            tlc_counter_add(&tlc->remote_meta_repair_drop, 1);
        else
            tlc_counter_add(&tlc->remote_meta_publish_async_drop, 1);
        if (tlc_log_should(&remote_meta_async_drop_logs)) {
            serverLog(LL_NOTICE,
                      "vemb_v16 remote_meta async publish drop: key_hash=%llu repair=%d reason=invalid_or_start_failed",
                      (unsigned long long)key_hash,
                      is_repair);
        }
        return -1;
    }

    vemb_v16_tlc_remote_meta_publisher_t *publisher =
        atomic_load_explicit(&tlc->remote_meta_publisher,
                             memory_order_acquire);
    if (!publisher) {
        if (is_repair)
            tlc_counter_add(&tlc->remote_meta_repair_drop, 1);
        else
            tlc_counter_add(&tlc->remote_meta_publish_async_drop, 1);
        if (tlc_log_should(&remote_meta_async_drop_logs)) {
            serverLog(LL_NOTICE,
                      "vemb_v16 remote_meta async publish drop: key_hash=%llu repair=%d reason=publisher_missing",
                      (unsigned long long)key_hash,
                      is_repair);
        }
        return -1;
    }

    vemb_v16_remote_meta_publish_event_t event;
    memset(&event, 0, sizeof(event));
    event.target_view = target_view;
    event.key_hash = key_hash;
    event.key_len = key_len;
    event.is_repair = is_repair;
    memcpy(event.key, key, key_len);
    event.handle = *handle;
    if (remote_meta_publish_ring_offer(publisher, &event) != 0) {
        if (is_repair)
            tlc_counter_add(&tlc->remote_meta_repair_drop, 1);
        else
            tlc_counter_add(&tlc->remote_meta_publish_async_drop, 1);
        if (tlc_log_should(&remote_meta_async_drop_logs)) {
            serverLog(LL_NOTICE,
                      "vemb_v16 remote_meta async publish drop: key_hash=%llu repair=%d reason=queue_full",
                      (unsigned long long)key_hash,
                      is_repair);
        }
        return -1;
    }

    if (is_repair)
        tlc_counter_add(&tlc->remote_meta_repair_enqueue, 1);
    else
        tlc_counter_add(&tlc->remote_meta_publish_async_enqueue, 1);
    return 0;
}

int vemb_v16_tlc_create(vemb_v16_tlc_t **out,
                        uint32_t vector_dim,
                        uint32_t max_vectors,
                        const vemb_v16_tlc_warm_region_t *warm_regions,
                        uint32_t warm_region_count,
                        uint32_t local_region_weight) {
    RETURN_IF(!out || warm_region_count == 0 ||
        warm_region_count > TLC_CORE_MAX_WARM_REGIONS ||
        vector_dim == 0 || max_vectors == 0, -1);
    /* Region ids must stay unique because vector handles address data by region_id. */
    for (uint32_t i = 0; i < warm_region_count; i++) {
        for (uint32_t j = 0; j < i; j++) {
            if (warm_regions[j].region_id == warm_regions[i].region_id)
                return -1;
        }
    }

    vemb_v16_tlc_t *tlc = zcalloc(sizeof(vemb_v16_tlc_t));
    RETURN_IF(!tlc, -1);
    tlc->vector_dim = vector_dim;
    tlc->value_size = vector_dim * sizeof(float);
    tlc->max_vectors = max_vectors;
    tlc->region_id = warm_regions[0].region_id;
    tlc->backend_type = warm_regions[0].backend_type;
    tlc->warm_region_count = warm_region_count;
    tlc_init_counters(tlc);
    atomic_init(&tlc->remote_meta_publisher, NULL);
    atomic_init(&tlc->runtime_warm_region_count, 0);
    atomic_init(&tlc->current_lookup_rpc_runtime, NULL);
    vemb_v16_tlc_access_snapshot_t *initial_snapshot =
        access_snapshot_create_empty();
    if (!initial_snapshot) {
        vemb_v16_tlc_destroy(tlc);
        return -1;
    }
    atomic_init(&tlc->access_snapshot, initial_snapshot);
    if (pthread_mutex_init(&tlc->migration_progress_lock, NULL) != 0) {
        vemb_v16_tlc_destroy(tlc);
        return -1;
    }
    tlc->migration_progress_lock_init = 1;
    if (pthread_mutex_init(&tlc->access_snapshot_update_lock, NULL) != 0) {
        vemb_v16_tlc_destroy(tlc);
        return -1;
    }
    tlc->access_snapshot_update_lock_init = 1;
    tlc->warm_regions =
        zcalloc(sizeof(vemb_v16_tlc_warm_region_t) * TLC_CORE_MAX_WARM_REGIONS);
    if (!tlc->warm_regions) {
        vemb_v16_tlc_destroy(tlc);
        return -1;
    }
    memcpy(tlc->warm_regions, warm_regions,
           sizeof(vemb_v16_tlc_warm_region_t) * warm_region_count);
    tlc->region_index_id_mapping_count = warm_region_count;
    for (uint32_t i = 0; i < warm_region_count; i++) {
        tlc->region_index_id_mappings[i] =
            (vemb_v16_tlc_region_index_id_mapping_t){
                .region_id = warm_regions[i].region_id,
                .region_index = i,
            };
    }
    atomic_init(&tlc->sve_stats.lock_success, 0);
    atomic_init(&tlc->sve_stats.lock_failure, 0);

    tlc_core_warm_region_config_t core_regions[TLC_CORE_MAX_WARM_REGIONS];
    memset(core_regions, 0, sizeof(core_regions));
    for (uint32_t i = 0; i < warm_region_count; i++) {
        core_regions[i] = (tlc_core_warm_region_config_t){
            .region_id = warm_regions[i].region_id,
            .backend_type = warm_regions[i].backend_type,
            .is_local = warm_regions[i].is_local,
            .weight = warm_regions[i].weight,
            .value_size = warm_regions[i].value_size,
            .region_bytes = warm_regions[i].region_bytes,
            .mapped_addr = warm_regions[i].mapped_addr,
            .slot_meta = warm_regions[i].slot_meta,
        };
    }
    tlc_core_config_t core_config = {
        .value_size = tlc->value_size,
        .warm_capacity = max_vectors,
        .hot_capacity = TLC_CORE_DEFAULT_HOT_CAPACITY,
        .cold_max_segments = TLC_CORE_DEFAULT_COLD_MAX_SEGMENTS,
        .cold_segment_records = TLC_CORE_DEFAULT_COLD_SEGMENT_RECORDS,
        .warm_regions = core_regions,
        .warm_region_count = warm_region_count,
        .local_region_weight = local_region_weight,
    };
    if (bitmap_init(&tlc->bitmap, max_vectors) != 0 ||
        tlc_core_create(&tlc->core, &core_config) != 0) {
        vemb_v16_tlc_destroy(tlc);
        return -1;
    }

    *out = tlc;
    return 0;
}

int vemb_v16_tlc_attach_warm_region(vemb_v16_tlc_t *tlc,
                                    const vemb_v16_tlc_warm_region_t *warm_region) {
    RETURN_IF(!tlc || !warm_region, -1);
    RETURN_IF(warm_region->backend_type != VEMB_V16_REGION_UB, -1);
    if (region_index_id_mapping(tlc, warm_region->region_id) != UINT32_MAX)
        return 0;

    uint32_t runtime_count = (uint32_t)atomic_load_explicit(
        &tlc->runtime_warm_region_count, memory_order_acquire);
    RETURN_IF(runtime_count >= TLC_CORE_MAX_WARM_REGIONS, -1);

    tlc_core_warm_region_config_t core_region = {
        .region_id = warm_region->region_id,
        .backend_type = warm_region->backend_type,
        .is_local = warm_region->is_local,
        .weight = warm_region->weight,
        .value_size = warm_region->value_size,
        .region_bytes = warm_region->region_bytes,
        .mapped_addr = warm_region->mapped_addr,
        .slot_meta = warm_region->slot_meta,
    };
    uint32_t region_index = UINT32_MAX;
    RETURN_IF(tlc_core_attach_warm_region(tlc->core,
                                          &core_region,
                                          &region_index) != 0,
              -1);

    tlc->runtime_warm_regions[runtime_count] = *warm_region;
    tlc->runtime_region_index_id_mappings[runtime_count] =
        (vemb_v16_tlc_region_index_id_mapping_t){
            .region_id = warm_region->region_id,
            .region_index = region_index};
    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&tlc->runtime_warm_region_count,
                          runtime_count + 1u,
                          memory_order_release);
    return 0;
}

void vemb_v16_tlc_destroy(vemb_v16_tlc_t *tlc) {
    RETURN_IF(!tlc);
    remote_meta_publisher_stop(tlc);
    bitmap_destroy(&tlc->bitmap);
    tlc_core_destroy(tlc->core);
    if (tlc->migration_progress_lock_init)
        pthread_mutex_destroy(&tlc->migration_progress_lock);
    if (tlc->access_snapshot_update_lock_init)
        pthread_mutex_destroy(&tlc->access_snapshot_update_lock);
    vemb_v16_tlc_access_snapshot_t *snapshot = atomic_load_explicit(
        &tlc->access_snapshot,
        memory_order_acquire);
    atomic_store_explicit(&tlc->access_snapshot, NULL, memory_order_release);
    access_snapshot_retire(snapshot);
    if (tlc->warm_regions) zfree(tlc->warm_regions);
    zfree(tlc);
}

int vemb_v16_tlc_get_handle(vemb_v16_tlc_t *tlc,
                            const char *key,
                            uint32_t key_len,
                            uint64_t key_hash,
                            vemb_v16_vector_handle_t *handle,
                            uint32_t *warm_slot) {
    tlc_warm_location_t location = {0};
    if (tlc_core_get_warm_location(tlc->core, key, key_len,
                                   key_hash, &location) != 0) {
        return -1;
    }
    if (warm_slot) *warm_slot = location.local_slot;
    make_handle(tlc, key_hash, &location, handle);
    return 0;
}

int vemb_v16_tlc_get_handle_stable_read(vemb_v16_tlc_t *tlc,
                                        const char *key,
                                        uint32_t key_len,
                                        uint64_t key_hash,
                                        vemb_v16_vector_handle_t *handle,
                                        uint32_t *warm_slot) {
    tlc_warm_location_t location = {0};
    if (tlc_core_get_warm_location_stable_read(tlc->core, key, key_len,
                                               key_hash, &location) != 0) {
        return -1;
    }
    if (warm_slot) *warm_slot = location.local_slot;
    make_handle(tlc, key_hash, &location, handle);
    return 0;
}

int vemb_v16_tlc_get_cached_handle(vemb_v16_tlc_t *tlc,
                                   const char *key,
                                   uint32_t key_len,
                                   uint64_t key_hash,
                                   vemb_v16_vector_handle_t *handle,
                                   uint32_t *warm_slot) {
    tlc_warm_location_t location = {0};
    if (tlc_core_get_cached_warm_location(tlc->core, key, key_len,
                                          key_hash, &location) != 0) {
        return -1;
    }
    if (warm_slot) *warm_slot = location.local_slot;
    make_handle(tlc, key_hash, &location, handle);
    return 0;
}

void vemb_v16_tlc_set_remote_meta_view(vemb_v16_tlc_t *tlc,
                                       vemb_v16_remote_meta_view_t *view,
                                       uint32_t retry_budget) {
    tlc->remote_meta_view = view;
    tlc->remote_meta_retry_budget = retry_budget;
    if (!tlc->owner_resolver) {
        tlc->owner_resolver = default_remote_meta_owner_resolver;
        tlc->owner_resolver_arg = view;
    }
    (void)vemb_v16_tlc_set_remote_meta_owner_view(
        tlc,
        view->header->owner_supernode_id,
        view);
}

typedef struct vemb_v16_tlc_owner_view_update {
    uint32_t owner_id;
    vemb_v16_remote_meta_view_t *view;
} vemb_v16_tlc_owner_view_update_t;

typedef struct vemb_v16_tlc_lookup_rpc_update {
    vemb_v16_tlc_lookup_rpc_fn fn;
    void *arg;
} vemb_v16_tlc_lookup_rpc_update_t;

typedef struct vemb_v16_tlc_lookup_runtime_update {
    vemb_v16_ub_rpc_t *rpc;
    vemb_v16_tlc_lookup_rpc_fn fn;
    void *arg;
    vemb_v16_ub_rpc_t *old_rpc;
} vemb_v16_tlc_lookup_runtime_update_t;

static int mutate_remote_meta_owner_view(
        vemb_v16_tlc_access_snapshot_t *snapshot,
        void *arg) {
    vemb_v16_tlc_owner_view_update_t *update = arg;
    for (uint32_t i = 0; i < snapshot->remote_meta_view_count; i++) {
        if (snapshot->remote_meta_views[i].owner_id == update->owner_id) {
            snapshot->remote_meta_views[i].view = update->view;
            return 0;
        }
    }
    RETURN_IF(snapshot->remote_meta_view_count >=
              VEMB_V16_TLC_MAX_REMOTE_META_VIEWS,
              -1);
    snapshot->remote_meta_views[snapshot->remote_meta_view_count++] =
        (vemb_v16_tlc_remote_meta_owner_view_t){
            .owner_id = update->owner_id,
            .view = update->view,
        };
    return 0;
}

static int mutate_lookup_rpc(vemb_v16_tlc_access_snapshot_t *snapshot,
                             void *arg) {
    vemb_v16_tlc_lookup_rpc_update_t *update = arg;
    snapshot->lookup_rpc = update->fn;
    snapshot->lookup_rpc_arg = update->arg;
    snapshot->lookup_rpc_runtime = NULL;
    return 0;
}

static int mutate_lookup_runtime(vemb_v16_tlc_access_snapshot_t *snapshot,
                                 void *arg) {
    vemb_v16_tlc_lookup_runtime_update_t *update = arg;
    update->old_rpc = snapshot->lookup_rpc_runtime;
    snapshot->lookup_rpc_runtime = update->rpc;
    snapshot->lookup_rpc = update->fn;
    snapshot->lookup_rpc_arg = update->arg;
    return 0;
}

static int mutate_clear_lookup_runtime(vemb_v16_tlc_access_snapshot_t *snapshot,
                                       void *arg) {
    vemb_v16_ub_rpc_t *rpc = arg;
    if (snapshot->lookup_rpc_runtime != rpc)
        return 1;
    snapshot->lookup_rpc_runtime = NULL;
    snapshot->lookup_rpc = NULL;
    snapshot->lookup_rpc_arg = NULL;
    return 0;
}

int vemb_v16_tlc_set_remote_meta_owner_view(vemb_v16_tlc_t *tlc,
                                            uint32_t owner_id,
                                            vemb_v16_remote_meta_view_t *view) {
    vemb_v16_tlc_owner_view_update_t update = {
        .owner_id = owner_id,
        .view = view,
    };
    return access_snapshot_update(tlc, mutate_remote_meta_owner_view, &update);
}

void vemb_v16_tlc_set_owner_resolver(vemb_v16_tlc_t *tlc,
                                     vemb_v16_tlc_owner_resolver_fn resolver,
                                     void *arg) {
    tlc->owner_resolver = resolver;
    tlc->owner_resolver_arg = arg;
}

static vemb_v16_remote_meta_view_t *remote_meta_view_for_key(
        vemb_v16_tlc_t *tlc,
        vemb_v16_tlc_access_snapshot_t *snapshot,
        const char *key,
        uint32_t key_len,
        uint64_t key_hash) {
    RETURN_IF(!snapshot || snapshot->remote_meta_view_count == 0, NULL);
    uint32_t owner_id = tlc->owner_resolver(key_hash,
                                            key,
                                            key_len,
                                            tlc->owner_resolver_arg);
    for (uint32_t i = 0; i < snapshot->remote_meta_view_count; i++) {
        if (snapshot->remote_meta_views[i].owner_id == owner_id)
            return snapshot->remote_meta_views[i].view;
    }
    return NULL;
}

static vemb_v16_remote_meta_view_t *remote_meta_view_for_owner(
        vemb_v16_tlc_access_snapshot_t *snapshot,
        uint32_t owner_id) {
    if (!snapshot)
        return NULL;
    for (uint32_t i = 0; i < snapshot->remote_meta_view_count; i++) {
        if (snapshot->remote_meta_views[i].owner_id == owner_id)
            return snapshot->remote_meta_views[i].view;
    }
    return NULL;
}

uint32_t vemb_v16_tlc_flush_remote_meta_publishes(vemb_v16_tlc_t *tlc,
                                                  uint32_t budget) {
    vemb_v16_tlc_remote_meta_publisher_t *publisher =
        atomic_load_explicit(&tlc->remote_meta_publisher,
                             memory_order_acquire);
    if (!publisher)
        return 0;
    uint32_t done = 0;
    for (;;) {
        if (budget && done >= budget)
            break;
        int got = remote_meta_publisher_drain_one(publisher, tlc);
        if (got > 0) {
            done++;
            continue;
        }
        if (budget)
            break;
        if (remote_meta_publish_ring_available(publisher) == 0 &&
            atomic_load_explicit(&publisher->active_consumers,
                                 memory_order_acquire) == 0) {
            break;
        }
        tlc_queue_pause();
    }
    return done;
}

uint32_t vemb_v16_tlc_remote_meta_owner_view_count(vemb_v16_tlc_t *tlc) {
    vemb_v16_tlc_access_snapshot_t *snapshot = access_snapshot_acquire(tlc);
    uint32_t count = snapshot ? snapshot->remote_meta_view_count : 0;
    access_snapshot_release(snapshot);
    return count;
}

void vemb_v16_tlc_set_lookup_rpc(vemb_v16_tlc_t *tlc,
                                 vemb_v16_tlc_lookup_rpc_fn fn,
                                 void *arg) {
    vemb_v16_tlc_lookup_rpc_update_t update = {
        .fn = fn,
        .arg = arg,
    };
    (void)access_snapshot_update(tlc, mutate_lookup_rpc, &update);
}

void vemb_v16_tlc_install_lookup_runtime(vemb_v16_tlc_t *tlc,
                                         vemb_v16_ub_rpc_t *rpc,
                                         vemb_v16_tlc_lookup_rpc_fn fn,
                                         void *arg,
                                         vemb_v16_ub_rpc_t **old_out) {
    if (old_out)
        *old_out = NULL;
    if (!tlc)
        return;
    vemb_v16_tlc_lookup_runtime_update_t update = {
        .rpc = rpc,
        .fn = fn,
        .arg = arg,
        .old_rpc = NULL,
    };
    if (access_snapshot_update(tlc, mutate_lookup_runtime, &update) != 0)
        return;
    if (old_out)
        *old_out = update.old_rpc;
}

void vemb_v16_tlc_clear_lookup_runtime(vemb_v16_tlc_t *tlc,
                                       vemb_v16_ub_rpc_t *rpc) {
    if (!tlc || !rpc)
        return;
    (void)access_snapshot_update(tlc, mutate_clear_lookup_runtime, rpc);
}

int vemb_v16_tlc_lookup_rpc_local_handler(
    void *arg,
    const vemb_v16_ub_lookup_rpc_req_t *req,
    vemb_v16_ub_lookup_rpc_resp_t *resp) {
    vemb_v16_tlc_t *owner = arg;
    RETURN_IF(!owner || !req || !resp ||
              req->op != VEMB_V16_UB_LOOKUP_RPC_LOOKUP_HANDLE ||
              req->key_len == 0 ||
              req->key_len > VEMB_V16_MAX_KEY_LEN,
              -1);
    memset(resp, 0, sizeof(*resp));
    resp->request_id = req->request_id;
    resp->key_hash = req->key_hash;

    vemb_v16_vector_handle_t handle = {0};
    if (vemb_v16_tlc_get_handle(owner,
                                req->key,
                                req->key_len,
                                req->key_hash,
                                &handle,
                                NULL) != 0) {
        resp->status = VEMB_V16_UB_LOOKUP_RPC_NOT_FOUND;
        resp->kind = VEMB_V16_UB_LOOKUP_RPC_KIND_NONE;
        return 0;
    }
    resp->status = VEMB_V16_UB_LOOKUP_RPC_OK;
    resp->kind = VEMB_V16_UB_LOOKUP_RPC_KIND_HANDLE;
    resp->region_id = handle.region_id;
    resp->local_slot = handle.local_slot;
    resp->offset = handle.offset;
    resp->bytes = handle.bytes;
    resp->owner_generation = handle.owner_generation;
    return 0;
}

static void migration_snapshot_to_desc(
        const tlc_core_migration_snapshot_t *snapshot,
        vemb_v16_ub_migration_snapshot_desc_t *desc) {
    memset(desc, 0, sizeof(*desc));
    desc->key_hash = snapshot->key_hash;
    desc->key_version = snapshot->key_version;
    desc->topology_epoch = snapshot->topology_epoch;
    desc->owner_epoch = snapshot->owner_epoch;
    desc->key_len = snapshot->key_len;
    desc->migration_state = snapshot->migration_state;
    desc->source_owner = snapshot->source_owner;
    desc->target_owner = snapshot->target_owner;
    desc->tombstone = snapshot->tombstone;
    desc->value_size = snapshot->value_size;
    desc->shard_id = snapshot->shard_id;
    desc->region_id = snapshot->location.region_id;
    desc->local_slot = snapshot->location.local_slot;
    desc->bytes = snapshot->location.bytes;
    desc->offset = snapshot->location.offset;
    desc->owner_generation = snapshot->location.owner_generation;
    memcpy(desc->key, snapshot->key, snapshot->key_len);
}

static void migration_snapshot_desc_to_core(
        const vemb_v16_ub_migration_snapshot_desc_t *desc,
        tlc_core_migration_snapshot_t *snapshot) {
    memset(snapshot, 0, sizeof(*snapshot));
    snapshot->key_hash = desc->key_hash;
    snapshot->key_version = desc->key_version;
    snapshot->topology_epoch = desc->topology_epoch;
    snapshot->owner_epoch = desc->owner_epoch;
    snapshot->key_len = desc->key_len;
    snapshot->migration_state = desc->migration_state;
    snapshot->source_owner = desc->source_owner;
    snapshot->target_owner = desc->target_owner;
    snapshot->tombstone = desc->tombstone;
    snapshot->value_size = desc->value_size;
    snapshot->shard_id = desc->shard_id;
    snapshot->location = (tlc_warm_location_t){
        .region_id = desc->region_id,
        .region_index = UINT32_MAX,
        .local_slot = desc->local_slot,
        .bytes = desc->bytes,
        .offset = desc->offset,
        .owner_generation = desc->owner_generation,
    };
    memcpy(snapshot->key, desc->key, desc->key_len);
}

static void migration_delta_to_snapshot(
        const vemb_v16_ub_migration_delta_desc_t *delta,
        tlc_core_migration_snapshot_t *snapshot) {
    memset(snapshot, 0, sizeof(*snapshot));
    snapshot->key_hash = delta->key_hash;
    snapshot->key_version = delta->key_version;
    snapshot->topology_epoch = delta->topology_epoch;
    snapshot->owner_epoch = delta->owner_epoch;
    snapshot->key_len = delta->key_len;
    snapshot->migration_state = TLC_CORE_KEY_MIGRATING;
    snapshot->source_owner = delta->source_owner;
    snapshot->target_owner = delta->target_owner;
    snapshot->shard_id = delta->shard_id;
    snapshot->tombstone =
        delta->tombstone ||
        delta->op == VEMB_V16_UB_MIGRATION_RPC_DELTA_DELETE;
    snapshot->value_size = snapshot->tombstone ? 0 : delta->value_size;
    snapshot->location = (tlc_warm_location_t){
        .region_id = delta->region_id,
        .region_index = UINT32_MAX,
        .local_slot = delta->local_slot,
        .bytes = delta->bytes,
        .offset = delta->offset,
        .owner_generation = delta->owner_generation,
    };
    memcpy(snapshot->key, delta->key, delta->key_len);
}

static uint32_t migration_apply_status_to_rpc(
        tlc_core_migration_apply_status_t status) {
    switch (status) {
    case TLC_CORE_MIGRATION_APPLIED:
        return VEMB_V16_UB_MIGRATION_RPC_OK;
    case TLC_CORE_MIGRATION_DUPLICATE:
        return VEMB_V16_UB_MIGRATION_RPC_DUPLICATE;
    case TLC_CORE_MIGRATION_STALE_REJECTED:
        return VEMB_V16_UB_MIGRATION_RPC_STALE_REJECTED;
    case TLC_CORE_MIGRATION_RETRY:
        return VEMB_V16_UB_MIGRATION_RPC_RETRY;
    case TLC_CORE_MIGRATION_ERROR:
    default:
        return VEMB_V16_UB_MIGRATION_RPC_ERROR;
    }
}

static int migration_rpc_status_advances_progress(uint32_t status) {
    return status == VEMB_V16_UB_MIGRATION_RPC_OK ||
           status == VEMB_V16_UB_MIGRATION_RPC_DUPLICATE ||
           status == VEMB_V16_UB_MIGRATION_RPC_STALE_REJECTED;
}

static int migration_progress_matches(
        const vemb_v16_tlc_migration_progress_t *progress,
        uint64_t topology_epoch,
        uint32_t source_owner,
        uint32_t target_owner,
        uint32_t shard_id) {
    return progress &&
           progress->valid &&
           progress->topology_epoch == topology_epoch &&
           progress->source_owner == source_owner &&
           progress->target_owner == target_owner &&
           progress->shard_id == shard_id;
}

static vemb_v16_tlc_migration_progress_t *migration_progress_get_locked(
        vemb_v16_tlc_t *tlc,
        uint64_t topology_epoch,
        uint32_t source_owner,
        uint32_t target_owner,
        uint32_t shard_id,
        int create) {
    for (uint32_t i = 0; i < tlc->migration_progress_count; i++) {
        if (migration_progress_matches(&tlc->migration_progress[i],
                                       topology_epoch,
                                       source_owner,
                                       target_owner,
                                       shard_id)) {
            return &tlc->migration_progress[i];
        }
    }
    if (!create ||
        tlc->migration_progress_count >= VEMB_V16_TLC_MAX_MIGRATION_PROGRESS) {
        return NULL;
    }

    vemb_v16_tlc_migration_progress_t *progress =
        &tlc->migration_progress[tlc->migration_progress_count++];
    memset(progress, 0, sizeof(*progress));
    progress->valid = 1;
    progress->topology_epoch = topology_epoch;
    progress->source_owner = source_owner;
    progress->target_owner = target_owner;
    progress->shard_id = shard_id;
    return progress;
}

static void migration_fill_delta_ack(
        vemb_v16_ub_migration_delta_ack_desc_t *ack,
        const vemb_v16_ub_migration_delta_desc_t *delta,
        uint32_t status,
        uint64_t applied_seq,
        uint64_t barrier_seq) {
    memset(ack, 0, sizeof(*ack));
    ack->topology_epoch = delta->topology_epoch;
    ack->applied_seq = applied_seq;
    ack->barrier_seq = barrier_seq;
    ack->source_owner = delta->source_owner;
    ack->target_owner = delta->target_owner;
    ack->shard_id = delta->shard_id;
    ack->status = status;
}

static int migration_delta_payload(
        vemb_v16_tlc_t *tlc,
        const vemb_v16_ub_migration_delta_desc_t *delta,
        const void **value,
        uint32_t *value_size) {
    *value = NULL;
    *value_size = 0;
    if (delta->tombstone ||
        delta->op == VEMB_V16_UB_MIGRATION_RPC_DELTA_DELETE) {
        return 0;
    }
    if (unlikely(delta->value_size != tlc->value_size ||
                 delta->bytes != tlc->value_size) ||
        delta->local_slot == TLC_CORE_INVALID_SLOT ||
        delta->region_id == TLC_CORE_INVALID_REGION_ID) {
        return -1;
    }

    const vemb_v16_tlc_warm_region_t *region =
        vemb_v16_tlc_find_region(tlc, delta->region_id);
    if (!region || !region->mapped_addr ||
        delta->offset > region->region_bytes ||
        delta->bytes > region->region_bytes - delta->offset) {
        return -1;
    }
    *value = (const uint8_t *)region->mapped_addr + delta->offset;
    *value_size = delta->bytes;
    return 0;
}

static int migration_snapshot_payload(
        vemb_v16_tlc_t *tlc,
        const vemb_v16_ub_migration_snapshot_desc_t *snapshot,
        const void **value,
        uint32_t *value_size) {
    *value = NULL;
    *value_size = 0;
    if (snapshot->tombstone)
        return 0;
    if (unlikely(snapshot->value_size != tlc->value_size ||
                 snapshot->bytes != tlc->value_size) ||
        snapshot->local_slot == TLC_CORE_INVALID_SLOT ||
        snapshot->region_id == TLC_CORE_INVALID_REGION_ID) {
        return -1;
    }

    const vemb_v16_tlc_warm_region_t *region =
        vemb_v16_tlc_find_region(tlc, snapshot->region_id);
    if (!region || !region->mapped_addr ||
        snapshot->offset > region->region_bytes ||
        snapshot->bytes > region->region_bytes - snapshot->offset) {
        return -1;
    }
    *value = (const uint8_t *)region->mapped_addr + snapshot->offset;
    *value_size = snapshot->bytes;
    return 0;
}

static int migration_handle_baseline_put(
        vemb_v16_tlc_t *owner,
        const vemb_v16_ub_migration_rpc_req_t *req,
        vemb_v16_ub_migration_rpc_resp_t *resp) {
    const vemb_v16_ub_migration_snapshot_desc_t *desc = &req->snapshot;
    if (desc->key_len == 0 ||
        desc->key_len > VEMB_V16_MAX_KEY_LEN ||
        desc->key_len != req->key_len ||
        desc->key_hash != req->key_hash ||
        desc->topology_epoch != req->topology_epoch ||
        desc->source_owner == UINT32_MAX ||
        desc->target_owner == UINT32_MAX ||
        desc->source_owner != req->src_owner_id ||
        desc->target_owner != req->dst_owner_id ||
        req->target_owner_id != desc->target_owner ||
        memcmp(desc->key, req->key, desc->key_len) != 0) {
        resp->status = VEMB_V16_UB_MIGRATION_RPC_ERROR;
        return 0;
    }

    const void *value = NULL;
    uint32_t value_size = 0;
    if (migration_snapshot_payload(owner, desc, &value, &value_size) != 0) {
        resp->status = VEMB_V16_UB_MIGRATION_RPC_ERROR;
        return 0;
    }

    tlc_core_migration_snapshot_t snapshot = {0};
    migration_snapshot_desc_to_core(desc, &snapshot);
    tlc_core_migration_apply_status_t apply_status =
        TLC_CORE_MIGRATION_ERROR;
    vemb_v16_vector_handle_t handle = {0};
    if (vemb_v16_tlc_apply_migration(owner,
                                     &snapshot,
                                     value,
                                     value_size,
                                     &apply_status,
                                     &handle) != 0) {
        resp->status = VEMB_V16_UB_MIGRATION_RPC_ERROR;
        return 0;
    }

    resp->topology_epoch = desc->topology_epoch;
    resp->key_version = desc->key_version;
    resp->source_owner_id = desc->source_owner;
    resp->target_owner_id = desc->target_owner;
    resp->snapshot = *desc;
    resp->status = migration_apply_status_to_rpc(apply_status);
    return 0;
}

static int migration_handle_delta(
        vemb_v16_tlc_t *owner,
        const vemb_v16_ub_migration_rpc_req_t *req,
        vemb_v16_ub_migration_rpc_resp_t *resp) {
    const vemb_v16_ub_migration_delta_desc_t *delta = &req->delta;
    if (delta->key_len == 0 ||
        delta->key_len > VEMB_V16_MAX_KEY_LEN ||
        delta->delta_seq == 0 ||
        delta->key_hash != req->key_hash ||
        delta->source_owner == UINT32_MAX ||
        delta->target_owner == UINT32_MAX ||
        (delta->op != VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT &&
         delta->op != VEMB_V16_UB_MIGRATION_RPC_DELTA_DELETE)) {
        resp->status = VEMB_V16_UB_MIGRATION_RPC_ERROR;
        return 0;
    }

    uint64_t progress_applied_seq = 0;
    uint64_t progress_barrier_seq = 0;
    pthread_mutex_lock(&owner->migration_progress_lock);
    vemb_v16_tlc_migration_progress_t *progress =
        migration_progress_get_locked(owner,
                                      delta->topology_epoch,
                                      delta->source_owner,
                                      delta->target_owner,
                                      delta->shard_id,
                                      1);
    if (!progress) {
        pthread_mutex_unlock(&owner->migration_progress_lock);
        resp->status = VEMB_V16_UB_MIGRATION_RPC_ERROR;
        return 0;
    }
    progress_applied_seq = progress->applied_seq;
    progress_barrier_seq = progress->barrier_seq;
    if (delta->delta_seq <= progress->applied_seq) {
        migration_fill_delta_ack(&resp->delta_ack,
                                 delta,
                                 VEMB_V16_UB_MIGRATION_RPC_DUPLICATE,
                                 progress->applied_seq,
                                 progress->barrier_seq);
        pthread_mutex_unlock(&owner->migration_progress_lock);
        resp->topology_epoch = delta->topology_epoch;
        resp->key_version = delta->key_version;
        resp->source_owner_id = delta->source_owner;
        resp->target_owner_id = delta->target_owner;
        resp->status = resp->delta_ack.status;
        return 0;
    }
    if (progress->applied_seq != 0 &&
        delta->delta_seq > progress->applied_seq + 1) {
        migration_fill_delta_ack(&resp->delta_ack,
                                 delta,
                                 VEMB_V16_UB_MIGRATION_RPC_RETRY,
                                 progress->applied_seq,
                                 progress->barrier_seq);
        pthread_mutex_unlock(&owner->migration_progress_lock);
        resp->topology_epoch = delta->topology_epoch;
        resp->key_version = delta->key_version;
        resp->source_owner_id = delta->source_owner;
        resp->target_owner_id = delta->target_owner;
        resp->status = resp->delta_ack.status;
        return 0;
    }
    pthread_mutex_unlock(&owner->migration_progress_lock);

    const void *value = NULL;
    uint32_t value_size = 0;
    if (migration_delta_payload(owner, delta, &value, &value_size) != 0) {
        resp->status = VEMB_V16_UB_MIGRATION_RPC_ERROR;
        return 0;
    }

    tlc_core_migration_snapshot_t snapshot = {0};
    migration_delta_to_snapshot(delta, &snapshot);
    tlc_core_migration_apply_status_t apply_status =
        TLC_CORE_MIGRATION_ERROR;
    vemb_v16_vector_handle_t handle = {0};
    if (vemb_v16_tlc_apply_migration(owner,
                                     &snapshot,
                                     value,
                                     value_size,
                                     &apply_status,
                                     &handle) != 0) {
        resp->status = VEMB_V16_UB_MIGRATION_RPC_ERROR;
        return 0;
    }
    resp->topology_epoch = delta->topology_epoch;
    resp->key_version = delta->key_version;
    resp->source_owner_id = delta->source_owner;
    resp->target_owner_id = delta->target_owner;
    uint32_t rpc_status = migration_apply_status_to_rpc(apply_status);
    if (migration_rpc_status_advances_progress(rpc_status)) {
        pthread_mutex_lock(&owner->migration_progress_lock);
        progress = migration_progress_get_locked(owner,
                                                 delta->topology_epoch,
                                                 delta->source_owner,
                                                 delta->target_owner,
                                                 delta->shard_id,
                                                 1);
        if (progress) {
            if (delta->delta_seq > progress->applied_seq)
                progress->applied_seq = delta->delta_seq;
            progress_applied_seq = progress->applied_seq;
            progress_barrier_seq = progress->barrier_seq;
        }
        pthread_mutex_unlock(&owner->migration_progress_lock);
    }
    migration_fill_delta_ack(&resp->delta_ack,
                             delta,
                             rpc_status,
                             progress_applied_seq,
                             progress_barrier_seq);
    resp->status = resp->delta_ack.status;
    return 0;
}

static int migration_handle_barrier(
        vemb_v16_tlc_t *owner,
        const vemb_v16_ub_migration_rpc_req_t *req,
        vemb_v16_ub_migration_rpc_resp_t *resp) {
    const vemb_v16_ub_migration_barrier_desc_t *barrier = &req->barrier;
    if (barrier->source_owner == UINT32_MAX ||
        barrier->target_owner == UINT32_MAX ||
        req->key_len > VEMB_V16_MAX_KEY_LEN) {
        resp->status = VEMB_V16_UB_MIGRATION_RPC_ERROR;
        return 0;
    }

    pthread_mutex_lock(&owner->migration_progress_lock);
    vemb_v16_tlc_migration_progress_t *progress =
        migration_progress_get_locked(owner,
                                      barrier->topology_epoch,
                                      barrier->source_owner,
                                      barrier->target_owner,
                                      barrier->shard_id,
                                      1);
    if (!progress) {
        pthread_mutex_unlock(&owner->migration_progress_lock);
        resp->status = VEMB_V16_UB_MIGRATION_RPC_ERROR;
        return 0;
    }
    if (barrier->barrier_seq > progress->barrier_seq)
        progress->barrier_seq = barrier->barrier_seq;
    uint64_t applied_seq = progress->applied_seq;
    uint64_t barrier_seq = progress->barrier_seq;
    uint32_t status = applied_seq >= barrier_seq ?
        VEMB_V16_UB_MIGRATION_RPC_OK :
        VEMB_V16_UB_MIGRATION_RPC_RETRY;
    pthread_mutex_unlock(&owner->migration_progress_lock);

    if (status == VEMB_V16_UB_MIGRATION_RPC_OK && req->key_len > 0) {
        tlc_core_key_migration_info_t info = {0};
        if (tlc_core_get_migration_info(owner->core,
                                            req->key,
                                            req->key_len,
                                            req->key_hash,
                                            &info) != 0 ||
            info.migration_state != TLC_CORE_KEY_DEST_COMMITTED ||
            info.target_owner != barrier->target_owner ||
            info.topology_epoch < barrier->topology_epoch) {
            status = VEMB_V16_UB_MIGRATION_RPC_RETRY;
        } else {
            resp->key_hash = req->key_hash;
            resp->key_version = info.key_version;
        }
    }

    resp->topology_epoch = barrier->topology_epoch;
    resp->source_owner_id = barrier->source_owner;
    resp->target_owner_id = barrier->target_owner;
    resp->barrier = *barrier;
    resp->barrier.barrier_seq = barrier_seq;
    resp->delta_ack = (vemb_v16_ub_migration_delta_ack_desc_t){
        .topology_epoch = barrier->topology_epoch,
        .applied_seq = applied_seq,
        .barrier_seq = barrier_seq,
        .source_owner = barrier->source_owner,
        .target_owner = barrier->target_owner,
        .shard_id = barrier->shard_id,
        .status = status,
    };
    resp->status = status;
    return 0;
}

static int migration_handle_lease_commit(
        vemb_v16_tlc_t *owner,
        const vemb_v16_ub_migration_rpc_req_t *req,
        vemb_v16_ub_migration_rpc_resp_t *resp) {
    const vemb_v16_ub_migration_lease_desc_t *lease = &req->lease;
    if (req->key_len == 0 ||
        req->key_len > VEMB_V16_MAX_KEY_LEN ||
        lease->source_owner == UINT32_MAX ||
        lease->target_owner == UINT32_MAX ||
        lease->owner_epoch == 0) {
        resp->status = VEMB_V16_UB_MIGRATION_RPC_ERROR;
        return 0;
    }

    tlc_core_key_migration_info_t info = {0};
    if (tlc_core_accept_owner_lease(owner->core,
                                        req->key,
                                        req->key_len,
                                        req->key_hash,
                                        lease->topology_epoch,
                                        lease->owner_epoch,
                                        lease->target_owner,
                                        &info) != 0) {
        tlc_core_key_migration_info_t current = {0};
        int current_rc = tlc_core_get_migration_info(owner->core,
                                                     req->key,
                                                     req->key_len,
                                                     req->key_hash,
                                                     &current);
        serverLog(LL_NOTICE,
                  "vemb_v16 lease commit rejected: source_owner=%u target_owner=%u key_hash=%llu lease_topology_epoch=%llu lease_owner_epoch=%llu current_rc=%d current_state=%u current_target=%u current_topology_epoch=%llu current_owner_epoch=%llu",
                  lease->source_owner,
                  lease->target_owner,
                  (unsigned long long)req->key_hash,
                  (unsigned long long)lease->topology_epoch,
                  (unsigned long long)lease->owner_epoch,
                  current_rc,
                  current.migration_state,
                  current.target_owner,
                  (unsigned long long)current.topology_epoch,
                  (unsigned long long)current.owner_epoch);
        resp->status = VEMB_V16_UB_MIGRATION_RPC_RETRY;
        return 0;
    }

    resp->topology_epoch = info.topology_epoch;
    resp->key_version = info.key_version;
    resp->source_owner_id = lease->source_owner;
    resp->target_owner_id = lease->target_owner;
    resp->lease = *lease;
    resp->lease.owner_epoch = info.owner_epoch;
    resp->status = VEMB_V16_UB_MIGRATION_RPC_OK;
    return 0;
}

int vemb_v16_tlc_migration_rpc_local_handler(
    void *arg,
    const vemb_v16_ub_migration_rpc_req_t *req,
    vemb_v16_ub_migration_rpc_resp_t *resp) {
    vemb_v16_tlc_t *owner = arg;
    RETURN_IF(!owner || !req || !resp, -1);
    memset(resp, 0, sizeof(*resp));
    resp->request_id = req->request_id;
    resp->op = req->op;
    resp->key_hash = req->key_hash;

    if (req->op == VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT ||
        req->op == VEMB_V16_UB_MIGRATION_RPC_DELTA_DELETE) {
        return migration_handle_delta(owner, req, resp);
    }

    if (req->op == VEMB_V16_UB_MIGRATION_RPC_BASELINE_PUT) {
        return migration_handle_baseline_put(owner, req, resp);
    }

    if (req->op == VEMB_V16_UB_MIGRATION_RPC_BARRIER_REQ) {
        return migration_handle_barrier(owner, req, resp);
    }

    if (req->op == VEMB_V16_UB_MIGRATION_RPC_LEASE_COMMIT_REQ) {
        return migration_handle_lease_commit(owner, req, resp);
    }

    if (req->op == VEMB_V16_UB_MIGRATION_RPC_DELTA_ACK) {
        resp->status = VEMB_V16_UB_MIGRATION_RPC_OK;
        resp->delta_ack = req->delta_ack;
        return 0;
    }

    if (req->op == VEMB_V16_UB_MIGRATION_RPC_BARRIER_RESP) {
        resp->status = VEMB_V16_UB_MIGRATION_RPC_OK;
        resp->barrier = req->barrier;
        return 0;
    }

    if (req->op == VEMB_V16_UB_MIGRATION_RPC_LEASE_COMMIT_RESP) {
        resp->status = VEMB_V16_UB_MIGRATION_RPC_OK;
        resp->lease = req->lease;
        return 0;
    }

    if (req->op != VEMB_V16_UB_MIGRATION_RPC_SNAPSHOT_REQ) {
        resp->status = VEMB_V16_UB_MIGRATION_RPC_ERROR;
        return 0;
    }
    if (req->key_len == 0 || req->key_len > VEMB_V16_MAX_KEY_LEN) {
        resp->status = VEMB_V16_UB_MIGRATION_RPC_ERROR;
        return 0;
    }

    uint8_t snapshot_value[VEMB_V16_MAX_DIM * sizeof(float)];
    tlc_core_migration_snapshot_t snapshot = {0};
    uint32_t target_owner = req->target_owner_id;
    if (target_owner == UINT32_MAX)
        target_owner = req->src_owner_id;
    int rc = tlc_core_snapshot(owner->core,
                                   req->key,
                                   req->key_len,
                                   req->key_hash,
                                   req->dst_owner_id,
                                   target_owner,
                                   &snapshot,
                                   snapshot_value,
                                   sizeof(snapshot_value));
    if (rc != 0) {
        resp->status = VEMB_V16_UB_MIGRATION_RPC_NOT_FOUND;
        return 0;
    }
    migration_snapshot_to_desc(&snapshot, &resp->snapshot);
    resp->topology_epoch = snapshot.topology_epoch;
    resp->key_version = snapshot.key_version;
    resp->source_owner_id = snapshot.source_owner;
    resp->target_owner_id = snapshot.target_owner;
    resp->status = VEMB_V16_UB_MIGRATION_RPC_OK;
    return 0;
}

static int validate_remote_handle(vemb_v16_tlc_t *tlc,
                                  uint64_t key_hash,
                                  const vemb_v16_vector_handle_t *candidate) {
    tlc_warm_location_t location = {
        .region_id = candidate->region_id,
        .region_index = UINT32_MAX,
        .local_slot = candidate->local_slot,
        .bytes = candidate->bytes,
        .offset = candidate->offset,
        .owner_generation = candidate->owner_generation,
    };
    return tlc_core_validate_warm_location(tlc->core, key_hash, &location);
}

static int repair_remote_meta_async(vemb_v16_tlc_t *tlc,
                                    uint32_t owner_id,
                                    const char *key,
                                    uint32_t key_len,
                                    uint64_t key_hash,
                                    const vemb_v16_vector_handle_t *handle) {
    vemb_v16_tlc_access_snapshot_t *snapshot = access_snapshot_acquire(tlc);
    vemb_v16_remote_meta_view_t *target_view =
        remote_meta_view_for_owner(snapshot, owner_id);
    int rc = enqueue_remote_meta_publish(tlc,
                                         target_view,
                                         key,
                                         key_len,
                                         key_hash,
                                         handle,
                                         1);
    access_snapshot_release(snapshot);
    return rc;
}

static int lookup_vsim_key2_via_rpc(vemb_v16_tlc_t *tlc,
                                    vemb_v16_tlc_access_snapshot_t *snapshot,
                                    uint32_t local_owner_id,
                                    uint32_t owner_id,
                                    const char *key2,
                                    uint32_t key2_len,
                                    uint64_t key2_hash,
                                    vemb_v16_vector_handle_t *handle,
                                    vemb_v16_tlc_lookup_source_t *source) {
    RETURN_IF(!snapshot || !snapshot->lookup_rpc ||
              owner_id == local_owner_id ||
              !key2 || key2_len == 0 || key2_len > VEMB_V16_MAX_KEY_LEN,
              -1);
    vemb_v16_ub_lookup_rpc_req_t req = {
        .request_id = atomic_fetch_add_explicit(
            &tlc->ub_lookup_rpc_next_request_id,
            1,
            memory_order_relaxed),
        .src_owner_id = local_owner_id,
        .dst_owner_id = owner_id,
        .op = VEMB_V16_UB_LOOKUP_RPC_LOOKUP_HANDLE,
        .flags = 0,
        .key_hash = key2_hash,
        .key_len = key2_len,
        .timeout_ns = 0,
    };
    memcpy(req.key, key2, key2_len);
    vemb_v16_ub_lookup_rpc_resp_t resp = {0};
    tlc_counter_add(&tlc->ub_lookup_rpc_count, 1);
    int rc = snapshot->lookup_rpc(snapshot->lookup_rpc_arg, &req, &resp);
    if (rc != 0) {
        tlc_counter_add(&tlc->ub_lookup_rpc_error, 1);
        return -1;
    }
    if (resp.status == VEMB_V16_UB_LOOKUP_RPC_NOT_FOUND) {
        tlc_counter_add(&tlc->ub_lookup_rpc_not_found, 1);
        return -1;
    }
    if (resp.status == VEMB_V16_UB_LOOKUP_RPC_BUSY) {
        tlc_counter_add(&tlc->ub_lookup_rpc_busy, 1);
        return -1;
    }
    if (resp.status == VEMB_V16_UB_LOOKUP_RPC_TIMEOUT) {
        tlc_counter_add(&tlc->ub_lookup_rpc_timeout, 1);
        return -1;
    }
    if (resp.status != VEMB_V16_UB_LOOKUP_RPC_OK ||
        resp.key_hash != key2_hash) {
        tlc_counter_add(&tlc->ub_lookup_rpc_error, 1);
        return -1;
    }
    tlc_counter_add(&tlc->ub_lookup_rpc_ok, 1);

    if (resp.kind == VEMB_V16_UB_LOOKUP_RPC_KIND_HANDLE) {
        tlc_counter_add(&tlc->ub_lookup_rpc_handle, 1);
        vemb_v16_vector_handle_t candidate = {
            .region_id = resp.region_id,
            .bytes = resp.bytes,
            .local_slot = resp.local_slot,
            .offset = resp.offset,
            .key_hash = key2_hash,
            .owner_generation = resp.owner_generation,
        };
        if (validate_remote_handle(tlc, key2_hash, &candidate) != 0) {
            tlc_core_note_remote_meta_stale(tlc->core);
            if (tlc_log_should(&remote_meta_stale_logs)) {
                serverLog(LL_NOTICE,
                          "vemb_v16 ub rpc handle stale: owner=%u key_hash=%llu region=%u slot=%u generation=%llu",
                          owner_id,
                          (unsigned long long)key2_hash,
                          candidate.region_id,
                          candidate.local_slot,
                          (unsigned long long)candidate.owner_generation);
            }
            return -1;
        }
        *handle = candidate;
        *source = VEMB_V16_TLC_LOOKUP_SOURCE_UB_RPC;
        (void)repair_remote_meta_async(tlc,
                                       owner_id,
                                       key2,
                                       key2_len,
                                       key2_hash,
                                       &candidate);
        return 0;
    }
    if (resp.kind == VEMB_V16_UB_LOOKUP_RPC_KIND_SNAPSHOT)
        tlc_counter_add(&tlc->ub_lookup_rpc_snapshot, 1);
    tlc_counter_add(&tlc->ub_lookup_rpc_error, 1);
    return -1;
}

int vemb_v16_tlc_lookup_vsim_key2(vemb_v16_tlc_t *tlc,
                                  const char *key2,
                                  uint32_t key2_len,
                                  uint64_t key2_hash,
                                  vemb_v16_vector_handle_t *handle,
                                  vemb_v16_tlc_lookup_source_t *source) {
    *source = VEMB_V16_TLC_LOOKUP_SOURCE_NONE;

    vemb_v16_tlc_access_snapshot_t *snapshot = access_snapshot_acquire(tlc);
    int try_local = 1;
    uint32_t owner_id = UINT32_MAX;
    uint32_t local_owner_id = UINT32_MAX;
    vemb_v16_remote_meta_view_t *remote_meta = NULL;
    if (snapshot &&
        snapshot->remote_meta_view_count > 0 &&
        tlc->owner_resolver &&
        tlc->remote_meta_view) {
        owner_id = tlc->owner_resolver(key2_hash,
                                       key2,
                                       key2_len,
                                       tlc->owner_resolver_arg);
        local_owner_id = tlc->remote_meta_view->header->owner_supernode_id;
        remote_meta = remote_meta_view_for_owner(snapshot, owner_id);
        try_local = owner_id == local_owner_id;
    }

    if (try_local &&
        vemb_v16_tlc_get_handle(tlc,
                                key2,
                                key2_len,
                                key2_hash,
                                handle,
                                NULL) == 0) {
        *source = VEMB_V16_TLC_LOOKUP_SOURCE_LOCAL;
        access_snapshot_release(snapshot);
        return 0;
    }

    if (!remote_meta)
        remote_meta = remote_meta_view_for_key(tlc,
                                               snapshot,
                                               key2,
                                               key2_len,
                                               key2_hash);
    if (remote_meta) {
        vemb_v16_remote_meta_handle_t remote_handle = {0};
        vemb_v16_remote_meta_lookup_result_t lookup_result = {0};
        int remote_rc = vemb_v16_remote_meta_lookup_with_result(
            remote_meta,
            key2,
            key2_len,
            key2_hash,
            tlc->remote_meta_retry_budget,
            &remote_handle,
            &lookup_result);
        tlc_counter_add(&tlc->remote_meta_lookup_way_probe, lookup_result.probes);
        if (remote_rc == VEMB_V16_REMOTE_META_OK) {
            tlc_counter_add(&tlc->remote_meta_lookup_hit, 1);
            vemb_v16_vector_handle_t candidate = {
                .region_id = remote_handle.region_id,
                .bytes = remote_handle.bytes,
                .local_slot = remote_handle.local_slot,
                .offset = remote_handle.offset,
                .key_hash = key2_hash,
                .owner_generation = remote_handle.owner_generation,
            };
            if (validate_remote_handle(tlc, key2_hash, &candidate) != 0) {
                tlc_core_note_remote_meta_stale(tlc->core);
                if (tlc_log_should(&remote_meta_stale_logs)) {
                    serverLog(LL_NOTICE,
                              "vemb_v16 remote_meta stale: owner=%u key_hash=%llu region=%u slot=%u generation=%llu",
                              owner_id,
                              (unsigned long long)key2_hash,
                              candidate.region_id,
                              candidate.local_slot,
                              (unsigned long long)candidate.owner_generation);
                }
                if (lookup_vsim_key2_via_rpc(tlc,
                                             snapshot,
                                             local_owner_id,
                                             owner_id,
                                             key2,
                                             key2_len,
                                             key2_hash,
                                             handle,
                                             source) == 0) {
                    access_snapshot_release(snapshot);
                    return 0;
                }
                access_snapshot_release(snapshot);
                return -1;
            }
            *handle = candidate;
            *source = VEMB_V16_TLC_LOOKUP_SOURCE_REMOTE;
            access_snapshot_release(snapshot);
            return 0;
        } else if (remote_rc == VEMB_V16_REMOTE_META_BUSY) {
            tlc_counter_add(&tlc->remote_meta_lookup_busy, 1);
        } else {
            tlc_counter_add(&tlc->remote_meta_lookup_miss, 1);
            if (remote_meta->header->ways &&
                lookup_result.probes >= remote_meta->header->ways) {
                tlc_counter_add(&tlc->remote_meta_lookup_set_conflict, 1);
                if (tlc_log_should(&remote_meta_set_conflict_logs)) {
                    serverLog(LL_NOTICE,
                              "vemb_v16 remote_meta set conflict miss: owner=%u key_hash=%llu set=%u probes=%u ways=%u",
                              remote_meta->header->owner_supernode_id,
                              (unsigned long long)key2_hash,
                              lookup_result.set_id,
                              lookup_result.probes,
                              remote_meta->header->ways);
                }
            }
        }
    }

    if (lookup_vsim_key2_via_rpc(tlc,
                                 snapshot,
                                 local_owner_id,
                                 owner_id,
                                 key2,
                                 key2_len,
                                 key2_hash,
                                 handle,
                                 source) == 0) {
        access_snapshot_release(snapshot);
        return 0;
    }
    access_snapshot_release(snapshot);
    return -1;
}

int vemb_v16_tlc_put(vemb_v16_tlc_t *tlc,
                     const char *key,
                     uint32_t key_len,
                     uint64_t key_hash,
                     const float *vector,
                     uint32_t vector_bytes,
                     vemb_v16_vector_handle_t *handle,
                     uint32_t *warm_slot) {
    tlc_warm_location_t location = {0};
    if (tlc_core_put_location_epoch(tlc->core,
                                    key,
                                    key_len,
                                    key_hash,
                                    vector,
                                    vector_bytes,
                                    0,
                                    0,
                                    &location) != 0) {
        return -1;
    }
    *warm_slot = location.local_slot;
    if (location.local_slot == TLC_CORE_INVALID_SLOT ||
        location.region_id == TLC_CORE_INVALID_REGION_ID) {
        memset(handle, 0, sizeof(*handle));
        return 0;
    }
    make_handle(tlc, key_hash, &location, handle);
    return 0;
}

int vemb_v16_tlc_put_with_epoch(vemb_v16_tlc_t *tlc,
                                const char *key,
                                uint32_t key_len,
                                uint64_t key_hash,
                                const float *vector,
                                uint32_t vector_bytes,
                                uint64_t topology_epoch,
                                vemb_v16_vector_handle_t *handle,
                                uint32_t *warm_slot) {
    tlc_warm_location_t location = {0};
    if (tlc_core_put_location_epoch(tlc->core,
                                    key,
                                    key_len,
                                    key_hash,
                                    vector,
                                    vector_bytes,
                                    topology_epoch,
                                    1,
                                    &location) != 0) {
        return -1;
    }
    *warm_slot = location.local_slot;
    if (IS_INVALID_LOCATION(location)) {
        memset(handle, 0, sizeof(*handle));
        return 0;
    }
    make_handle(tlc, key_hash, &location, handle);
    return 0;
}

int vemb_v16_tlc_migration_progress_ready(vemb_v16_tlc_t *tlc,
                                          uint32_t source_owner,
                                          uint32_t target_owner,
                                          uint32_t shard_id,
                                          uint64_t max_topology_epoch,
                                          uint64_t *topology_epoch,
                                          uint64_t *applied_seq,
                                          uint64_t *barrier_seq) {
    RETURN_IF(!tlc || source_owner == UINT32_MAX ||
              target_owner == UINT32_MAX,
              0);
    if (topology_epoch)
        *topology_epoch = 0;
    if (applied_seq)
        *applied_seq = 0;
    if (barrier_seq)
        *barrier_seq = 0;

    uint32_t found = 0;
    vemb_v16_tlc_migration_progress_t best;
    memset(&best, 0, sizeof(best));
    pthread_mutex_lock(&tlc->migration_progress_lock);
    for (uint32_t i = 0; i < tlc->migration_progress_count; i++) {
        const vemb_v16_tlc_migration_progress_t *progress =
            &tlc->migration_progress[i];
        if (!progress->valid ||
            progress->source_owner != source_owner ||
            progress->target_owner != target_owner ||
            progress->shard_id != shard_id ||
            progress->topology_epoch > max_topology_epoch) {
            continue;
        }
        if (!found || progress->topology_epoch > best.topology_epoch) {
            best = *progress;
            found = 1;
        }
    }
    pthread_mutex_unlock(&tlc->migration_progress_lock);
    if (!found)
        return 0;

    if (topology_epoch)
        *topology_epoch = best.topology_epoch;
    if (applied_seq)
        *applied_seq = best.applied_seq;
    if (barrier_seq)
        *barrier_seq = best.barrier_seq;
    return best.applied_seq >= best.barrier_seq;
}

int vemb_v16_tlc_apply_migration(vemb_v16_tlc_t *tlc,
                                 const tlc_core_migration_snapshot_t *snapshot,
                                 const void *value,
                                 uint32_t value_size,
                                 tlc_core_migration_apply_status_t *status,
                                 vemb_v16_vector_handle_t *handle) {
    RETURN_IF(!tlc, -1);
    tlc_warm_location_t location = {0};
    int rc = tlc_core_apply_migration(tlc->core,
                                      snapshot,
                                      value,
                                      value_size,
                                      status,
                                      &location);
    if (rc == 0 && handle &&
        status &&
        *status == TLC_CORE_MIGRATION_APPLIED &&
        !snapshot->tombstone) {
        make_handle(tlc, snapshot->key_hash, &location, handle);
    } else if (handle) {
        memset(handle, 0, sizeof(*handle));
    }
    return rc;
}

const vemb_v16_tlc_warm_region_t *vemb_v16_tlc_find_region(
    const vemb_v16_tlc_t *tlc, uint32_t region_id) {
    uint32_t region_index = region_index_id_mapping(tlc, region_id);
    if (region_index < tlc->warm_region_count &&
        tlc->warm_regions[region_index].region_id == region_id)
        return &tlc->warm_regions[region_index];
    uint32_t runtime_count = (uint32_t)atomic_load_explicit(
        (const atomic_uint_fast32_t *)&tlc->runtime_warm_region_count,
        memory_order_acquire);
    if (region_index >= tlc->warm_region_count) {
        uint32_t runtime_index = region_index - tlc->warm_region_count;
        if (runtime_index < runtime_count &&
            tlc->runtime_warm_regions[runtime_index].region_id == region_id) {
            return &tlc->runtime_warm_regions[runtime_index];
        }
    }
    return NULL;
}

int vemb_v16_tlc_vector_slice(const vemb_v16_tlc_t *tlc,
                              const vemb_v16_vector_handle_t *handle,
                              const uint8_t **vector,
                              uint32_t *vector_bytes) {
    *vector = NULL;
    *vector_bytes = 0;
    const vemb_v16_tlc_warm_region_t *region =
        vemb_v16_tlc_find_region(tlc, handle->region_id);
    if (!region || !region->mapped_addr ||
        handle->local_slot == TLC_CORE_INVALID_SLOT ||
        handle->offset > region->region_bytes ||
        handle->bytes > region->region_bytes - handle->offset) {
        return -1;
    }
    tlc_warm_location_t location = {
        .region_id = handle->region_id,
        .region_index = region_index_id_mapping(tlc, handle->region_id),
        .local_slot = handle->local_slot,
        .bytes = handle->bytes,
        .offset = handle->offset,
        .owner_generation = handle->owner_generation,
    };
    if (tlc_core_validate_warm_location(tlc->core,
                                        handle->key_hash,
                                        &location) != 0) {
        return -1;
    }
    *vector = (const uint8_t *)region->mapped_addr + handle->offset;
    *vector_bytes = handle->bytes;
    return 0;
}

int vemb_v16_tlc_load_vector(const vemb_v16_tlc_t *tlc,
                             const vemb_v16_vector_handle_t *handle,
                             void *dst,
                             uint32_t dst_bytes,
                             uint32_t *vector_bytes) {
    RETURN_IF(handle->local_slot == TLC_CORE_INVALID_SLOT ||
              handle->region_id == TLC_CORE_INVALID_REGION_ID ||
              handle->bytes == 0 ||
              dst_bytes < handle->bytes,
              -1);
    tlc_warm_location_t location = {
        .region_id = handle->region_id,
        .region_index = region_index_id_mapping(tlc, handle->region_id),
        .local_slot = handle->local_slot,
        .bytes = handle->bytes,
        .offset = handle->offset,
        .owner_generation = handle->owner_generation,
    };
    if (tlc_core_copy_warm_location_value(tlc->core,
                                          handle->key_hash,
                                          &location,
                                          dst,
                                          dst_bytes,
                                          VEMB_V16_TLC_VECTOR_COPY_RETRIES) != 0) {
        return -1;
    }
    *vector_bytes = handle->bytes;
    return 0;
}

void vemb_v16_tlc_get_runtime_stats(vemb_v16_tlc_t *tlc,
                                    vemb_v16_stats_t *stats) {
    stats->remote_meta_lookup_hit = tlc_counter_load(&tlc->remote_meta_lookup_hit);
    stats->remote_meta_lookup_miss = tlc_counter_load(&tlc->remote_meta_lookup_miss);
    stats->remote_meta_lookup_busy = tlc_counter_load(&tlc->remote_meta_lookup_busy);
    stats->remote_meta_lookup_way_probe = tlc_counter_load(&tlc->remote_meta_lookup_way_probe);
    stats->remote_meta_lookup_set_conflict = tlc_counter_load(&tlc->remote_meta_lookup_set_conflict);
    stats->remote_meta_publish_async_enqueue = tlc_counter_load(&tlc->remote_meta_publish_async_enqueue);
    stats->remote_meta_publish_async_drop = tlc_counter_load(&tlc->remote_meta_publish_async_drop);
    stats->remote_meta_publish_async_coalesce = tlc_counter_load(&tlc->remote_meta_publish_async_coalesce);
    stats->remote_meta_publish_ok = tlc_counter_load(&tlc->remote_meta_publish_ok);
    stats->remote_meta_publish_busy = tlc_counter_load(&tlc->remote_meta_publish_busy);
    stats->remote_meta_publish_insert = tlc_counter_load(&tlc->remote_meta_publish_insert);
    stats->remote_meta_publish_update = tlc_counter_load(&tlc->remote_meta_publish_update);
    stats->remote_meta_publish_evict = tlc_counter_load(&tlc->remote_meta_publish_evict);
    stats->remote_meta_publish_ns = tlc_counter_load(&tlc->remote_meta_publish_ns);
    stats->ub_lookup_rpc_count = tlc_counter_load(&tlc->ub_lookup_rpc_count);
    stats->ub_lookup_rpc_ok = tlc_counter_load(&tlc->ub_lookup_rpc_ok);
    stats->ub_lookup_rpc_not_found = tlc_counter_load(&tlc->ub_lookup_rpc_not_found);
    stats->ub_lookup_rpc_busy = tlc_counter_load(&tlc->ub_lookup_rpc_busy);
    stats->ub_lookup_rpc_timeout = tlc_counter_load(&tlc->ub_lookup_rpc_timeout);
    stats->ub_lookup_rpc_error = tlc_counter_load(&tlc->ub_lookup_rpc_error);
    stats->ub_lookup_rpc_handle = tlc_counter_load(&tlc->ub_lookup_rpc_handle);
    stats->ub_lookup_rpc_snapshot = tlc_counter_load(&tlc->ub_lookup_rpc_snapshot);
    stats->ub_lookup_rpc_ns = tlc_counter_load(&tlc->ub_lookup_rpc_ns);
    stats->remote_meta_repair_enqueue = tlc_counter_load(&tlc->remote_meta_repair_enqueue);
    stats->remote_meta_repair_ok = tlc_counter_load(&tlc->remote_meta_repair_ok);
    stats->remote_meta_repair_drop = tlc_counter_load(&tlc->remote_meta_repair_drop);
}

void vemb_v16_tlc_note_handle_lookup_miss(vemb_v16_tlc_t *tlc,
                                          uint8_t status) {
    tlc_counter_add(&tlc->handle_lookup_miss, 1);
    if (status == VEMB_V16_STATUS_MOVED)
        tlc_counter_add(&tlc->handle_lookup_miss_moved, 1);
    else if (status == VEMB_V16_STATUS_STALE_TOPOLOGY)
        tlc_counter_add(&tlc->handle_lookup_miss_stale, 1);
    else if (status == VEMB_V16_STATUS_NOT_FOUND)
        tlc_counter_add(&tlc->handle_lookup_miss_not_found, 1);
}

void vemb_v16_tlc_get_lookup_diagnostic_stats(
    vemb_v16_tlc_t *tlc,
    vemb_v16_tlc_lookup_diagnostic_stats_t *stats) {
    *stats = (vemb_v16_tlc_lookup_diagnostic_stats_t){
        .handle_lookup_miss = tlc_counter_load(&tlc->handle_lookup_miss),
        .handle_lookup_miss_not_found = tlc_counter_load(
            &tlc->handle_lookup_miss_not_found),
        .handle_lookup_miss_moved = tlc_counter_load(
            &tlc->handle_lookup_miss_moved),
        .handle_lookup_miss_stale = tlc_counter_load(
            &tlc->handle_lookup_miss_stale),
    };
}
