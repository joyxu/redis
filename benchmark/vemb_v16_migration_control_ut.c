#include "../src/monotonic.h"
#include "../src/vemb_v16_proxy.h"
#include "../src/vemb_v16_aeron_transport.h"
#include "../src/vemb_v16_net.h"
#include "../src/vemb_v16_supernode.h"
#include "../src/vemb_v16_tcp_transport.h"
#include "../src/vemb_v16_topology.h"

#include <assert.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

static void fill_vector(float *vector, uint32_t dim, uint32_t seed) {
    for (uint32_t i = 0; i < dim; i++)
        vector[i] = (float)(seed + i);
}

static void init_test_allocator(vemb_v16_warm_region_header_t *allocator,
                                uint32_t region_id,
                                uint32_t capacity_slots) {
    memset(allocator, 0, sizeof(*allocator));
    atomic_init(&allocator->magic, VEMB_V16_WARM_REGION_LAYOUT_MAGIC);
    allocator->version = VEMB_V16_WARM_REGION_LAYOUT_VERSION;
    allocator->region_id = region_id;
    allocator->capacity_slots = capacity_slots;
}

typedef struct migration_ut_slot_meta_entry {
    void *mapped_addr;
    uint32_t region_id;
    uint32_t capacity_slots;
    vemb_v16_warm_slot_meta_t *slot_meta;
} migration_ut_slot_meta_entry_t;

#define MIGRATION_UT_SLOT_META_REGISTRY_MAX 128u

static migration_ut_slot_meta_entry_t
    migration_ut_slot_meta_registry[MIGRATION_UT_SLOT_META_REGISTRY_MAX];

static vemb_v16_warm_slot_meta_t *migration_ut_slot_meta_acquire(
        void *mapped_addr,
        uint32_t region_id,
        uint32_t capacity_slots) {
    for (uint32_t i = 0; i < MIGRATION_UT_SLOT_META_REGISTRY_MAX; i++) {
        migration_ut_slot_meta_entry_t *entry = &migration_ut_slot_meta_registry[i];
        if (entry->mapped_addr == mapped_addr &&
            entry->region_id == region_id &&
            entry->capacity_slots == capacity_slots) {
            return entry->slot_meta;
        }
    }

    for (uint32_t i = 0; i < MIGRATION_UT_SLOT_META_REGISTRY_MAX; i++) {
        migration_ut_slot_meta_entry_t *entry = &migration_ut_slot_meta_registry[i];
        if (entry->mapped_addr)
            continue;

        vemb_v16_warm_slot_meta_t *slots =
            calloc(capacity_slots, sizeof(*slots));
        assert(slots);
        for (uint32_t slot = 0; slot < capacity_slots; slot++) {
            slots[slot].region_id = region_id;
            slots[slot].local_slot = slot;
            atomic_init(&slots[slot].state, VEMB_V16_WARM_SLOT_FREE);
            atomic_init(&slots[slot].owner_generation, 0);
            atomic_init(&slots[slot].write_seq, 0);
            atomic_init(&slots[slot].last_access_ns, 0);
            atomic_init(&slots[slot].clock_bit, 0);
            atomic_init(&slots[slot].cold_state, VEMB_V16_WARM_SLOT_COLD_NONE);
        }
        *entry = (migration_ut_slot_meta_entry_t){
            .mapped_addr = mapped_addr,
            .region_id = region_id,
            .capacity_slots = capacity_slots,
            .slot_meta = slots,
        };
        return slots;
    }

    assert(!"migration_ut slot_meta registry exhausted");
    return NULL;
}

static void migration_ut_ensure_slot_meta(
        vemb_v16_tlc_warm_region_t *warm_region) {
    if (warm_region->slot_meta)
        return;
    assert(warm_region->mapped_addr);
    assert(warm_region->value_size > 0);
    assert(warm_region->region_bytes >= warm_region->value_size);
    uint32_t capacity_slots =
        (uint32_t)(warm_region->region_bytes / warm_region->value_size);
    warm_region->slot_meta =
        migration_ut_slot_meta_acquire(warm_region->mapped_addr,
                                       warm_region->region_id,
                                       capacity_slots);
}

static void migration_ut_ensure_regions_slot_meta(
        vemb_v16_tlc_warm_region_t *warm_regions,
        uint32_t warm_region_count) {
    for (uint32_t i = 0; i < warm_region_count; i++)
        migration_ut_ensure_slot_meta(&warm_regions[i]);
}

static int migration_ut_create(vemb_v16_tlc_t **out,
                               uint32_t vector_dim,
                               uint32_t max_vectors,
                               vemb_v16_tlc_warm_region_t *warm_regions,
                               uint32_t warm_region_count,
                               uint32_t local_region_weight) {
    migration_ut_ensure_regions_slot_meta(warm_regions, warm_region_count);
    return vemb_v16_tlc_create(out,
                               vector_dim,
                               max_vectors,
                               warm_regions,
                               warm_region_count,
                               local_region_weight);
}

#define vemb_v16_tlc_create migration_ut_create

static void set_rpc_ring(vemb_v16_ub_rpc_ring_config_t *ring,
                         const char *path) {
    memset(ring, 0, sizeof(*ring));
    ring->backend_type = VEMB_V16_REGION_LOCAL_SHM;
    snprintf(ring->path, sizeof(ring->path), "%s", path);
}

static void cleanup_rpc_rings(const char *req_a_b,
                              const char *req_b_a,
                              const char *resp_a_b,
                              const char *resp_b_a) {
    shm_unlink(req_a_b);
    shm_unlink(req_b_a);
    shm_unlink(resp_a_b);
    shm_unlink(resp_b_a);
}

static void make_rpc_peers(vemb_v16_ub_rpc_peer_t *peer_a_to_b,
                           uint32_t owner_b,
                           vemb_v16_ub_rpc_peer_t *peer_b_to_a,
                           uint32_t owner_a,
                           const char *req_a_b,
                           const char *req_b_a,
                           const char *resp_a_b,
                           const char *resp_b_a) {
    memset(peer_a_to_b, 0, sizeof(*peer_a_to_b));
    memset(peer_b_to_a, 0, sizeof(*peer_b_to_a));
    peer_a_to_b->owner_id = owner_b;
    set_rpc_ring(&peer_a_to_b->request, req_a_b);
    set_rpc_ring(&peer_a_to_b->response, resp_b_a);
    set_rpc_ring(&peer_a_to_b->inbound_request, req_b_a);
    set_rpc_ring(&peer_a_to_b->outbound_response, resp_a_b);

    peer_b_to_a->owner_id = owner_a;
    set_rpc_ring(&peer_b_to_a->request, req_b_a);
    set_rpc_ring(&peer_b_to_a->response, resp_a_b);
    set_rpc_ring(&peer_b_to_a->inbound_request, req_a_b);
    set_rpc_ring(&peer_b_to_a->outbound_response, resp_b_a);
}

static void cleanup_region_and_layout(const char *path,
                                         uint32_t region_id) {
    char layout_name[VEMB_V16_WARM_REGION_LAYOUT_NAME_MAX];
    shm_unlink(path);
    if (vemb_v16_warm_region_layout_name_from_region_path(
            path, region_id, layout_name, sizeof(layout_name)) == 0) {
        shm_unlink(layout_name);
    }
}

static void create_test_ub_region_file(const char *path,
                                       uint32_t region_id,
                                       uint32_t capacity_slots,
                                       uint32_t value_size) {
    size_t layout_bytes = vemb_v16_warm_region_layout_bytes(capacity_slots);
    size_t total_bytes = layout_bytes + ((size_t)capacity_slots * value_size);
    int fd = open(path, O_CREAT | O_RDWR | O_TRUNC, 0666);
    assert(fd >= 0);
    assert(ftruncate(fd, (off_t)total_bytes) == 0);
    close(fd);
    assert(vemb_v16_warm_region_layout_reset(VEMB_V16_REGION_UB,
                                             VEMB_V16_UB_CACHE_POLICY_CACHEABLE,
                                             path,
                                             0,
                                             region_id,
                                             capacity_slots) == 0);
}

static int wait_for_outbox_progress(vemb_v16_storage_ctx_t *storage,
                                    uint64_t min_acked_seq,
                                    uint32_t wanted_pending_count,
                                    vemb_v16_migration_outbox_stats_t *stats) {
    for (uint32_t i = 0; i < 500; i++) {
        memset(stats, 0, sizeof(*stats));
        pthread_mutex_lock(&storage->migration_outbox_lock);
        if (storage->migration_outbox_count > 0) {
            vemb_v16_migration_outbox_get_stats(
                storage->migration_outboxes[0],
                stats);
        }
        pthread_mutex_unlock(&storage->migration_outbox_lock);
        if (stats->acked_seq >= min_acked_seq &&
            stats->pending_count == wanted_pending_count) {
            return 0;
        }
        usleep(1000);
    }
    return -1;
}

static void init_storage_runtime_fields(vemb_v16_storage_ctx_t *storage,
                                        uint64_t current_epoch,
                                        uint64_t min_write_epoch) {
    atomic_init(&storage->current_topology_epoch, current_epoch);
    atomic_init(&storage->min_write_epoch, min_write_epoch);
    atomic_init(&storage->migration_active_count, 0);
    atomic_init(&storage->migration_delta_request_id, 1);
    atomic_init(&storage->migration_source_gc_count, 0);
    atomic_init(&storage->migration_gc_safe_watermark, 0);
    atomic_init(&storage->migration_baseline_sent_count, 0);
    atomic_init(&storage->migration_baseline_skipped_count, 0);
    atomic_init(&storage->migration_baseline_error_count, 0);
    atomic_init(&storage->migration_baseline_retry_queued_count, 0);
    atomic_init(&storage->migration_baseline_retry_sent_count, 0);
    atomic_init(&storage->migration_retry_stop, 0);
    storage->scaleout_auto_enabled = 0;
    storage->scaleout_auto_phase = VEMB_V16_STORAGE_SCALEOUT_IDLE;
    storage->scaleout_auto_last_error = 0;
    storage->scaleout_auto_migration_epoch = 0;
    storage->scaleout_auto_cutover_epoch = 0;
    storage->scaleout_auto_coordinated = 0;
    storage->scaleout_auto_coordinator_endpoint_valid = 0;
    storage->scaleout_auto_notify_seq = 0;
}

static void test_scaleout_local_done_wire_lengths(void) {
    vemb_v16_scaleout_local_done_req_t req = {
        .migration_topology_epoch = 23,
        .cutover_topology_epoch = 24,
        .notify_seq = 23,
        .source_owner = 1,
        .phase = VEMB_V16_STORAGE_SCALEOUT_NOTIFY_PENDING,
        .pending_delta = 2,
        .baseline_retry_pending = 3,
        .migrating_key_count = 4,
        .range_count = 5,
    };
    vemb_v16_scaleout_local_done_req_t decoded_req = {0};
    uint8_t req_buf[VEMB_V16_SCALEOUT_LOCAL_DONE_REQ_ENCODED_LEN];
    size_t req_len = 0;
    assert(vemb_v16_scaleout_local_done_req_encode(req_buf,
                                                    sizeof(req_buf),
                                                    &req,
                                                    &req_len) == 0);
    assert(req_len == VEMB_V16_SCALEOUT_LOCAL_DONE_REQ_ENCODED_LEN);
    assert(vemb_v16_scaleout_local_done_req_decode(&decoded_req,
                                                    req_buf,
                                                    req_len) == 0);
    assert(decoded_req.notify_seq == req.notify_seq);
    assert(decoded_req.range_count == req.range_count);

    vemb_v16_scaleout_local_done_resp_t resp = {
        .status = VEMB_V16_STATUS_OK,
        .migration_topology_epoch = req.migration_topology_epoch,
        .cutover_topology_epoch = req.cutover_topology_epoch,
        .notify_seq = req.notify_seq,
        .source_owner = req.source_owner,
    };
    vemb_v16_scaleout_local_done_resp_t decoded_resp = {0};
    uint8_t resp_buf[VEMB_V16_SCALEOUT_LOCAL_DONE_RESP_ENCODED_LEN];
    size_t resp_len = 0;
    assert(vemb_v16_scaleout_local_done_resp_encode(resp_buf,
                                                     sizeof(resp_buf),
                                                     &resp,
                                                     &resp_len) == 0);
    assert(resp_len == VEMB_V16_SCALEOUT_LOCAL_DONE_RESP_ENCODED_LEN);
    assert(vemb_v16_scaleout_local_done_resp_decode(&decoded_resp,
                                                     resp_buf,
                                                     resp_len) == 0);
    assert(decoded_resp.status == VEMB_V16_STATUS_OK);
    assert(decoded_resp.cutover_topology_epoch ==
           req.cutover_topology_epoch);
}

static void fill_control_req(vemb_v16_migration_control_req_t *req,
                             const char *key,
                             uint64_t key_hash,
                             uint64_t topology_epoch,
                             uint32_t target_owner) {
    uint32_t key_len = (uint32_t)strlen(key);
    memset(req, 0, sizeof(*req));
    req->key_hash = key_hash;
    req->topology_epoch = topology_epoch;
    req->key_len = key_len;
    req->target_owner = target_owner;
    memcpy(req->key, key, key_len);
}

typedef struct control_thread_arg {
    vemb_v16_proxy_t *proxy;
    int fd;
} control_thread_arg_t;

static void *tcp_control_thread_main(void *arg) {
    control_thread_arg_t *control = arg;
    vemb_v16_tcp_handle_fd(control->proxy, control->fd);
    return NULL;
}

static int start_tcp_control_thread(control_thread_arg_t *arg,
                                    pthread_t *thread,
                                    vemb_v16_proxy_t *proxy,
                                    int fd) {
    arg->proxy = proxy;
    arg->fd = fd;
    return pthread_create(thread, NULL, tcp_control_thread_main, arg);
}

static int tcp_migration_control(vemb_v16_proxy_t *proxy,
                                 uint8_t type,
                                 const vemb_v16_migration_control_req_t *req,
                                 vemb_v16_migration_control_resp_t *resp) {
    int sv[2];
    control_thread_arg_t handler_arg;
    pthread_t handler;
    uint8_t req_buf[128];
    uint8_t resp_buf[128];
    size_t req_len = 0;
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0)
        return -1;
    if (start_tcp_control_thread(&handler_arg, &handler, proxy, sv[1]) != 0) {
        close(sv[0]);
        close(sv[1]);
        return -1;
    }
    if (vemb_v16_migration_control_req_encode(req_buf,
                                              sizeof(req_buf),
                                              req,
                                              &req_len) != 0) {
        close(sv[0]);
        pthread_join(handler, NULL);
        return -1;
    }
    if (vemb_v16_net_write_frame(sv[0],
                                 type,
                                 0,
                                 0,
                                 0,
                                 req_buf,
                                 (uint32_t)req_len) != 0) {
        close(sv[0]);
        pthread_join(handler, NULL);
        return -1;
    }

    vemb_v16_net_hdr_t hdr;
    int rc = vemb_v16_net_read_header(sv[0], &hdr);
    if (rc == 0 &&
        (hdr.type != VEMB_V16_NET_MIGRATION_CONTROL_RESPONSE ||
         hdr.payload_len > sizeof(resp_buf))) {
        rc = -1;
    }
    if (rc == 0)
        rc = vemb_v16_net_read_full(sv[0], resp_buf, hdr.payload_len);
    if (rc == 0)
        rc = vemb_v16_migration_control_resp_decode(resp,
                                                    resp_buf,
                                                    hdr.payload_len);
    close(sv[0]);
    pthread_join(handler, NULL);
    return rc;
}

static int tcp_migration_batch_control(
        vemb_v16_proxy_t *proxy,
        const vemb_v16_migration_control_batch_req_t *req,
        vemb_v16_migration_control_batch_resp_t *resp) {
    int sv[2];
    control_thread_arg_t handler_arg;
    pthread_t handler;
    uint8_t *req_buf = NULL;
    uint8_t *resp_buf = NULL;
    size_t req_len = 0;
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0)
        return -1;
    if (start_tcp_control_thread(&handler_arg, &handler, proxy, sv[1]) != 0) {
        close(sv[0]);
        close(sv[1]);
        return -1;
    }
    req_buf = malloc(vemb_v16_migration_control_batch_req_encoded_len(req));
    if (!req_buf ||
        vemb_v16_migration_control_batch_req_encode(
            req_buf,
            vemb_v16_migration_control_batch_req_encoded_len(req),
            req,
            &req_len) != 0) {
        free(req_buf);
        close(sv[0]);
        pthread_join(handler, NULL);
        return -1;
    }
    if (vemb_v16_net_write_frame(sv[0],
                                 VEMB_V16_NET_MIGRATION_MARK_MIGRATING_BATCH,
                                 0,
                                 0,
                                 0,
                                 req_buf,
                                 (uint32_t)req_len) != 0) {
        free(req_buf);
        close(sv[0]);
        pthread_join(handler, NULL);
        return -1;
    }
    free(req_buf);

    vemb_v16_net_hdr_t hdr;
    int rc = vemb_v16_net_read_header(sv[0], &hdr);
    if (rc == 0 &&
        hdr.type != VEMB_V16_NET_MIGRATION_CONTROL_BATCH_RESPONSE) {
        rc = -1;
    }
    if (rc == 0) {
        resp_buf = malloc(hdr.payload_len);
        if (!resp_buf)
            rc = -1;
    }
    if (rc == 0)
        rc = vemb_v16_net_read_full(sv[0], resp_buf, hdr.payload_len);
    if (rc == 0)
        rc = vemb_v16_migration_control_batch_resp_decode(resp,
                                                          resp_buf,
                                                          hdr.payload_len);
    free(resp_buf);
    close(sv[0]);
    pthread_join(handler, NULL);
    return rc;
}

static void fill_range_control_req(
        vemb_v16_migration_range_control_req_t *req,
        uint64_t migration_topology_epoch,
        uint64_t cutover_topology_epoch,
        uint32_t target_owner,
        uint32_t shard_id) {
    memset(req, 0, sizeof(*req));
    req->migration_topology_epoch = migration_topology_epoch;
    req->cutover_topology_epoch = cutover_topology_epoch;
    req->target_owner = target_owner;
    req->shard_id = shard_id;
}

static int tcp_migration_range_control(
        vemb_v16_proxy_t *proxy,
        uint8_t type,
        const vemb_v16_migration_range_control_req_t *req,
        vemb_v16_migration_range_control_resp_t *resp) {
    int sv[2];
    control_thread_arg_t handler_arg;
    pthread_t handler;
    uint8_t req_buf[32];
    uint8_t resp_buf[128];
    size_t req_len = 0;
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0)
        return -1;
    if (start_tcp_control_thread(&handler_arg, &handler, proxy, sv[1]) != 0) {
        close(sv[0]);
        close(sv[1]);
        return -1;
    }
    if (vemb_v16_migration_range_control_req_encode(req_buf,
                                                    sizeof(req_buf),
                                                    req,
                                                    &req_len) != 0) {
        close(sv[0]);
        pthread_join(handler, NULL);
        return -1;
    }
    if (vemb_v16_net_write_frame(sv[0],
                                 type,
                                 0,
                                 0,
                                 0,
                                 req_buf,
                                 (uint32_t)req_len) != 0) {
        close(sv[0]);
        pthread_join(handler, NULL);
        return -1;
    }

    vemb_v16_net_hdr_t hdr;
    int rc = vemb_v16_net_read_header(sv[0], &hdr);
    if (rc == 0 &&
        (hdr.type != VEMB_V16_NET_MIGRATION_RANGE_CONTROL_RESPONSE ||
         hdr.payload_len > sizeof(resp_buf))) {
        rc = -1;
    }
    if (rc == 0)
        rc = vemb_v16_net_read_full(sv[0], resp_buf, hdr.payload_len);
    if (rc == 0)
        rc = vemb_v16_migration_range_control_resp_decode(resp,
                                                          resp_buf,
                                                          hdr.payload_len);
    close(sv[0]);
    pthread_join(handler, NULL);
    return rc;
}

static int tcp_epoch_control(vemb_v16_proxy_t *proxy,
                             uint8_t type,
                             const vemb_v16_epoch_control_req_t *req,
                             vemb_v16_epoch_control_resp_t *resp) {
    int sv[2];
    control_thread_arg_t handler_arg;
    pthread_t handler;
    const void *payload = NULL;
    uint32_t payload_len = 0;
    uint8_t req_buf[32];
    uint8_t resp_buf[32];
    size_t req_len = 0;
    if (type == VEMB_V16_NET_EPOCH_SET) {
        if (vemb_v16_epoch_control_req_encode(req_buf,
                                              sizeof(req_buf),
                                              req,
                                              &req_len) != 0) {
            return -1;
        }
        payload = req_buf;
        payload_len = (uint32_t)req_len;
    }
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0)
        return -1;
    if (start_tcp_control_thread(&handler_arg, &handler, proxy, sv[1]) != 0) {
        close(sv[0]);
        close(sv[1]);
        return -1;
    }
    if (vemb_v16_net_write_frame(sv[0],
                                 type,
                                 0,
                                 0,
                                 0,
                                 payload,
                                 payload_len) != 0) {
        close(sv[0]);
        pthread_join(handler, NULL);
        return -1;
    }

    vemb_v16_net_hdr_t hdr;
    int rc = vemb_v16_net_read_header(sv[0], &hdr);
    if (rc == 0 &&
        (hdr.type != VEMB_V16_NET_EPOCH_CONTROL_RESPONSE ||
         hdr.payload_len != vemb_v16_epoch_control_resp_encoded_len())) {
        rc = -1;
    }
    if (rc == 0)
        rc = vemb_v16_net_read_full(sv[0], resp_buf, hdr.payload_len);
    if (rc == 0)
        rc = vemb_v16_epoch_control_resp_decode(resp,
                                                resp_buf,
                                                hdr.payload_len);
    close(sv[0]);
    pthread_join(handler, NULL);
    return rc;
}

static void fill_topology_req(vemb_v16_topology_control_req_t *req,
                              uint64_t current_topology_epoch,
                              uint64_t min_write_epoch,
                              uint32_t flags,
                              const uint32_t *active_owners,
                              uint32_t active_owner_count,
                              const uint32_t *standby_owners,
                              uint32_t standby_owner_count) {
    memset(req, 0, sizeof(*req));
    req->current_topology_epoch = current_topology_epoch;
    req->min_write_epoch = min_write_epoch;
    req->vnode_count = VEMB_V16_TOPOLOGY_DEFAULT_VNODES;
    req->flags = flags;
    req->active_owner_count = active_owner_count;
    req->standby_owner_count = standby_owner_count;
    memcpy(req->active_owners,
           active_owners,
           sizeof(uint32_t) * active_owner_count);
    memcpy(req->standby_owners,
           standby_owners,
           sizeof(uint32_t) * standby_owner_count);
}

static int tcp_topology_control(vemb_v16_proxy_t *proxy,
                                uint8_t type,
                                const vemb_v16_topology_control_req_t *req,
                                vemb_v16_topology_control_resp_t *resp) {
    int sv[2];
    control_thread_arg_t handler_arg;
    pthread_t handler;
    const void *payload = NULL;
    uint32_t payload_len = 0;
    uint8_t *req_buf = NULL;
    uint8_t *resp_buf = NULL;
    size_t req_len = 0;
    if (type == VEMB_V16_NET_TOPOLOGY_SET) {
        req_buf = malloc(vemb_v16_topology_control_req_encoded_len(req));
        if (!req_buf ||
            vemb_v16_topology_control_req_encode(
                req_buf,
                vemb_v16_topology_control_req_encoded_len(req),
                req,
                &req_len) != 0) {
            free(req_buf);
            return -1;
        }
        payload = req_buf;
        payload_len = (uint32_t)req_len;
    }
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0)
        return -1;
    if (start_tcp_control_thread(&handler_arg, &handler, proxy, sv[1]) != 0) {
        close(sv[0]);
        close(sv[1]);
        return -1;
    }
    if (vemb_v16_net_write_frame(sv[0],
                                 type,
                                 0,
                                 0,
                                 0,
                                 payload,
                                 payload_len) != 0) {
        free(req_buf);
        close(sv[0]);
        pthread_join(handler, NULL);
        return -1;
    }
    free(req_buf);

    vemb_v16_net_hdr_t hdr;
    int rc = vemb_v16_net_read_header(sv[0], &hdr);
    if (rc == 0 && hdr.type != VEMB_V16_NET_TOPOLOGY_RESPONSE) {
        rc = -1;
    }
    if (rc == 0) {
        resp_buf = malloc(hdr.payload_len);
        if (!resp_buf)
            rc = -1;
    }
    if (rc == 0)
        rc = vemb_v16_net_read_full(sv[0], resp_buf, hdr.payload_len);
    if (rc == 0)
        rc = vemb_v16_topology_control_resp_decode(resp,
                                                   resp_buf,
                                                   hdr.payload_len);
    free(resp_buf);
    close(sv[0]);
    pthread_join(handler, NULL);
    return rc;
}

static int tcp_peer_view_topology_control(
        vemb_v16_proxy_t *proxy,
        const vemb_v16_peer_view_topology_control_req_t *req,
        vemb_v16_peer_view_topology_control_resp_t *resp) {
    int sv[2];
    control_thread_arg_t handler_arg;
    pthread_t handler;
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0)
        return -1;
    if (start_tcp_control_thread(&handler_arg, &handler, proxy, sv[1]) != 0) {
        close(sv[0]);
        close(sv[1]);
        return -1;
    }
    if (vemb_v16_net_write_frame(
            sv[0], VEMB_V16_NET_PEER_VIEW_MAP_TOPOLOGY_SET, 0, 0, 0,
            req, sizeof(*req)) != 0) {
        close(sv[0]);
        pthread_join(handler, NULL);
        return -1;
    }
    vemb_v16_net_hdr_t hdr;
    int rc = vemb_v16_net_read_header(sv[0], &hdr);
    if (rc == 0 &&
        (hdr.type != VEMB_V16_NET_PEER_VIEW_MAP_TOPOLOGY_RESPONSE ||
         hdr.payload_len != sizeof(*resp))) {
        rc = -1;
    }
    if (rc == 0)
        rc = vemb_v16_net_read_full(sv[0], resp, sizeof(*resp));
    close(sv[0]);
    pthread_join(handler, NULL);
    return rc;
}

static void assert_owner_list(const uint32_t *actual,
                              const uint32_t *expected,
                              uint32_t count) {
    for (uint32_t i = 0; i < count; i++)
        assert(actual[i] == expected[i]);
}

static void find_key_for_ring_owner(const vemb_v16_topology_ring_t *ring,
                                    uint32_t owner,
                                    const char *prefix,
                                    char *key,
                                    size_t key_size,
                                    uint64_t *key_hash) {
    for (uint32_t i = 0; i < 100000; i++) {
        snprintf(key, key_size, "%s:%u", prefix, i);
        uint32_t key_len = (uint32_t)strlen(key);
        uint64_t hash = vemb_v16_xxh3_64_str(key, key_len);
        if (vemb_v16_topology_ring_owner(ring, hash) == owner) {
            *key_hash = hash;
            return;
        }
    }
    assert(0);
}

static void assert_topology_dual_write_key(
        const vemb_v16_topology_control_resp_t *resp,
        uint32_t added_owner) {
    vemb_v16_topology_ring_t active_ring;
    vemb_v16_topology_ring_t standby_ring;
    assert(vemb_v16_topology_ring_build(&active_ring,
                                        resp->current_topology_epoch,
                                        resp->active_owners,
                                        resp->active_owner_count,
                                        resp->vnode_count) ==
           VEMB_V16_TOPOLOGY_OK);
    assert(vemb_v16_topology_ring_build(&standby_ring,
                                        resp->current_topology_epoch,
                                        resp->standby_owners,
                                        resp->standby_owner_count,
                                        resp->vnode_count) ==
           VEMB_V16_TOPOLOGY_OK);
    for (uint32_t i = 0; i < 100000; i++) {
        char key[64];
        snprintf(key, sizeof(key), "p5-topology-key:%u", i);
        uint64_t key_hash = vemb_v16_xxh3_64_str(key, strlen(key));
        uint32_t active_owner =
            vemb_v16_topology_ring_owner(&active_ring, key_hash);
        uint32_t standby_owner =
            vemb_v16_topology_ring_owner(&standby_ring, key_hash);
        if (active_owner != standby_owner) {
            assert(standby_owner == added_owner);
            return;
        }
    }
    assert(0);
}

static void run_write_job(vemb_v16_storage_ctx_t *storage,
                          uint8_t op,
                          const char *key,
                          uint64_t key_hash,
                          uint64_t topology_epoch,
                          const float *vector,
                          uint32_t dim,
                          uint8_t flags,
                          vemb_v16_completion_t *completion) {
    vemb_v16_completion_t completion_slots[4];
    vemb_v16_aeron_ring_t completion_ring;
    atomic_int active;
    atomic_int running;
    atomic_int notify_armed;
    int notify_fd = -1;
    vemb_v16_channel_counters_t stats;
    vemb_v16_vadd_job_t job;
    uint32_t key_len = (uint32_t)strlen(key);

    memset(completion_slots, 0, sizeof(completion_slots));
    memset(&completion_ring, 0, sizeof(completion_ring));
    memset(&stats, 0, sizeof(stats));
    memset(&job, 0, sizeof(job));
    atomic_init(&active, 1);
    atomic_init(&running, 1);
    atomic_init(&notify_armed, 0);
    assert(vemb_v16_aeron_ring_init(&completion_ring,
                                    completion_slots,
                                    sizeof(completion_slots[0]),
                                    4) == 0);

    vemb_v16_supernode_ctx_t ctx = {
        .channel_active = &active,
        .running = &running,
        .completion_notify_armed = &notify_armed,
        .completion_notify_fd = &notify_fd,
        .completion_ring = &completion_ring,
        .storage = storage,
        .stats = &stats,
    };
    job.base.op = op;
    job.base.flags = flags;
    job.base.req_id = 7001;
    job.base.channel_id = 1001;
    job.base.key_hash = key_hash;
    job.base.topology_epoch = topology_epoch;
    job.key_len = key_len;
    job.dim = op == VEMB_V16_OP_VREM ? 0 : dim;
    job.vector_bytes = op == VEMB_V16_OP_VREM ? 0 : dim * sizeof(float);
    memcpy(job.key, key, key_len);
    if (job.vector_bytes > 0)
        memcpy(job.vector, vector, job.vector_bytes);

    if (op == VEMB_V16_OP_VREM)
        vemb_v16_supernode_handle_vrem_job(&ctx,
                                           (const vemb_v16_vemb_job_t *)&job);
    else
        vemb_v16_supernode_handle_vadd_job(&ctx, &job);
    assert(vemb_v16_aeron_poll(&completion_ring, completion) == 1);
}

static void run_vadd_job(vemb_v16_storage_ctx_t *storage,
                         const char *key,
                         uint64_t key_hash,
                         uint64_t topology_epoch,
                         const float *vector,
                         uint32_t dim,
                         vemb_v16_completion_t *completion) {
    run_write_job(storage,
                  VEMB_V16_OP_VADD,
                  key,
                  key_hash,
                  topology_epoch,
                  vector,
                  dim,
                  0,
                  completion);
}

static void run_vadd_job_with_flags(vemb_v16_storage_ctx_t *storage,
                                    const char *key,
                                    uint64_t key_hash,
                                    uint64_t topology_epoch,
                                    const float *vector,
                                    uint32_t dim,
                                    uint8_t flags,
                                    vemb_v16_completion_t *completion) {
    run_write_job(storage,
                  VEMB_V16_OP_VADD,
                  key,
                  key_hash,
                  topology_epoch,
                  vector,
                  dim,
                  flags,
                  completion);
}

static void run_vrem_job(vemb_v16_storage_ctx_t *storage,
                         const char *key,
                         uint64_t key_hash,
                         uint64_t topology_epoch,
                         vemb_v16_completion_t *completion) {
    run_write_job(storage,
                  VEMB_V16_OP_VREM,
                  key,
                  key_hash,
                  topology_epoch,
                  NULL,
                  0,
                  0,
                  completion);
}

static void run_vrem_job_with_flags(vemb_v16_storage_ctx_t *storage,
                                    const char *key,
                                    uint64_t key_hash,
                                    uint64_t topology_epoch,
                                    uint8_t flags,
                                    vemb_v16_completion_t *completion) {
    run_write_job(storage,
                  VEMB_V16_OP_VREM,
                  key,
                  key_hash,
                  topology_epoch,
                  NULL,
                  0,
                  flags,
                  completion);
}

static void run_vemb_handle_job(vemb_v16_storage_ctx_t *storage,
                                const char *key,
                                uint64_t key_hash,
                                uint64_t topology_epoch,
                                uint32_t dim,
                                vemb_v16_completion_t *completion) {
    vemb_v16_completion_t completion_slots[4];
    vemb_v16_aeron_ring_t completion_ring;
    atomic_int active;
    atomic_int running;
    atomic_int notify_armed;
    int notify_fd = -1;
    vemb_v16_channel_counters_t stats;
    vemb_v16_vemb_job_t job;
    uint32_t key_len = (uint32_t)strlen(key);

    memset(completion_slots, 0, sizeof(completion_slots));
    memset(&completion_ring, 0, sizeof(completion_ring));
    memset(&stats, 0, sizeof(stats));
    memset(&job, 0, sizeof(job));
    atomic_init(&active, 1);
    atomic_init(&running, 1);
    atomic_init(&notify_armed, 0);
    assert(vemb_v16_aeron_ring_init(&completion_ring,
                                    completion_slots,
                                    sizeof(completion_slots[0]),
                                    4) == 0);

    vemb_v16_supernode_ctx_t ctx = {
        .channel_active = &active,
        .running = &running,
        .completion_notify_armed = &notify_armed,
        .completion_notify_fd = &notify_fd,
        .completion_ring = &completion_ring,
        .storage = storage,
        .stats = &stats,
    };
    job.base.op = VEMB_V16_OP_VEMB_HANDLE;
    job.base.req_id = 7101;
    job.base.channel_id = 1101;
    job.base.key_hash = key_hash;
    job.base.topology_epoch = topology_epoch;
    job.key_len = key_len;
    job.dim = dim;
    job.vector_bytes = dim * sizeof(float);
    memcpy(job.key, key, key_len);

    vemb_v16_supernode_handle_vemb_job(&ctx, &job);
    assert(vemb_v16_aeron_poll(&completion_ring, completion) == 1);
}

static void test_supernode_lookup_miss_classification(void) {
    enum { dim = 2, max_vectors = 16 };
    float region[dim * max_vectors];
    float value[dim];
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 991,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .value_size = sizeof(value),
        .region_bytes = sizeof(region),
        .mapped_addr = (uint8_t *)region,
        .slot_meta = NULL,
    };
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_storage_ctx_t storage;
    vemb_v16_vector_handle_t handle = {0};
    tlc_core_key_migration_info_t info = {0};
    vemb_v16_completion_t completion = {0};
    uint32_t warm_slot = UINT32_MAX;

    memset(region, 0, sizeof(region));
    fill_vector(value, dim, 9100);
    assert(vemb_v16_tlc_create(&tlc,
                               dim,
                               max_vectors,
                               &warm,
                               1,
                               1) == 0);
    memset(&storage, 0, sizeof(storage));
    storage.local_owner_id = 0;
    storage.tlc = tlc;
    init_storage_runtime_fields(&storage, 0, 0);
    assert(pthread_mutex_init(&storage.topology_lock, NULL) == 0);
    assert(pthread_mutex_init(&storage.migration_outbox_lock, NULL) == 0);

    const char *missing_key = "lookup-miss-no-metadata";
    uint64_t missing_hash = vemb_v16_xxh3_64_str(missing_key,
                                                 strlen(missing_key));
    run_vemb_handle_job(&storage,
                        missing_key,
                        missing_hash,
                        10,
                        dim,
                        &completion);
    assert(completion.status == VEMB_V16_STATUS_NOT_FOUND);

    const char *migrating_key = "lookup-miss-migrating";
    uint64_t migrating_hash = vemb_v16_xxh3_64_str(migrating_key,
                                                   strlen(migrating_key));
    assert(vemb_v16_tlc_put(tlc,
                            migrating_key,
                            strlen(migrating_key),
                            migrating_hash,
                            value,
                            sizeof(value),
                            &handle,
                            &warm_slot) == 0);
    assert(tlc_core_mark_migrating_in_shard(tlc->core,
                                            migrating_key,
                                            strlen(migrating_key),
                                            migrating_hash,
                                            10,
                                            2,
                                            0,
                                            &info) == 0);
    assert(tlc_core_delete_with_epoch(tlc->core,
                                      migrating_key,
                                      strlen(migrating_key),
                                      migrating_hash,
                                      10,
                                      &info) == 0);
    memset(&completion, 0, sizeof(completion));
    run_vemb_handle_job(&storage,
                        migrating_key,
                        migrating_hash,
                        10,
                        dim,
                        &completion);
    assert(completion.status == VEMB_V16_STATUS_STALE_TOPOLOGY);

    const char *cutover_key = "lookup-miss-cutover";
    uint64_t cutover_hash = vemb_v16_xxh3_64_str(cutover_key,
                                                 strlen(cutover_key));
    warm_slot = UINT32_MAX;
    assert(vemb_v16_tlc_put(tlc,
                            cutover_key,
                            strlen(cutover_key),
                            cutover_hash,
                            value,
                            sizeof(value),
                            &handle,
                            &warm_slot) == 0);
    assert(tlc_core_mark_migrating_in_shard(tlc->core,
                                            cutover_key,
                                            strlen(cutover_key),
                                            cutover_hash,
                                            10,
                                            2,
                                            0,
                                            &info) == 0);
    assert(tlc_core_delete_with_epoch(tlc->core,
                                      cutover_key,
                                      strlen(cutover_key),
                                      cutover_hash,
                                      10,
                                      &info) == 0);
    assert(tlc_core_mark_cutover(tlc->core,
                                 cutover_key,
                                 strlen(cutover_key),
                                 cutover_hash,
                                 11,
                                 2,
                                 &info) == 0);
    memset(&completion, 0, sizeof(completion));
    run_vemb_handle_job(&storage,
                        cutover_key,
                        cutover_hash,
                        11,
                        dim,
                        &completion);
    assert(completion.status == VEMB_V16_STATUS_MOVED);
    assert(completion.redirect_owner == 2);

    const char *source_gc_key = "lookup-miss-source-gc";
    uint64_t source_gc_hash = vemb_v16_xxh3_64_str(source_gc_key,
                                                   strlen(source_gc_key));
    warm_slot = UINT32_MAX;
    assert(vemb_v16_tlc_put(tlc,
                            source_gc_key,
                            strlen(source_gc_key),
                            source_gc_hash,
                            value,
                            sizeof(value),
                            &handle,
                            &warm_slot) == 0);
    assert(vemb_v16_storage_migration_mark_migrating(&storage,
                                                     source_gc_key,
                                                     strlen(source_gc_key),
                                                     source_gc_hash,
                                                     10,
                                                     2,
                                                     &info) == 0);
    assert(vemb_v16_storage_migration_active(&storage));
    assert(tlc_core_delete_with_epoch(tlc->core,
                                      source_gc_key,
                                      strlen(source_gc_key),
                                      source_gc_hash,
                                      10,
                                      &info) == 0);
    assert(tlc_core_mark_cutover(tlc->core,
                                 source_gc_key,
                                 strlen(source_gc_key),
                                 source_gc_hash,
                                 11,
                                 2,
                                 &info) == 0);
    assert(vemb_v16_storage_migration_mark_source_gc(&storage,
                                                     source_gc_key,
                                                     strlen(source_gc_key),
                                                     source_gc_hash,
                                                     11,
                                                     2,
                                                     &info) == 0);
    assert(!vemb_v16_storage_migration_active(&storage));
    memset(&completion, 0, sizeof(completion));
    run_vemb_handle_job(&storage,
                        source_gc_key,
                        source_gc_hash,
                        11,
                        dim,
                        &completion);
    assert(completion.status == VEMB_V16_STATUS_MOVED);
    assert(completion.redirect_owner == 2);

    pthread_mutex_destroy(&storage.migration_outbox_lock);
    pthread_mutex_destroy(&storage.topology_lock);
    vemb_v16_tlc_destroy(tlc);
}

static void test_proxy_migration_control_primitives(void) {
    enum { dim = 2, max_vectors = 4 };
    char manifest_path[128];
    char shm1[64];
    char remote_meta_default[64];
    char req0_1[64];
    char req1_0[64];
    char resp0_1[64];
    char resp1_0[64];
    vemb_v16_storage_ctx_t *storage = NULL;
    vemb_v16_proxy_t *proxy = NULL;
    const char *key = "control-migration-key";
    uint32_t key_len = (uint32_t)strlen(key);
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, key_len);
    const char *transport_key = "transport-control-key";
    uint32_t transport_key_len = (uint32_t)strlen(transport_key);
    uint64_t transport_key_hash =
        vemb_v16_xxh3_64_str(transport_key, transport_key_len);
    const char *batch_key1 = "batch-migration-key-1";
    const char *batch_key2 = "batch-migration-key-2";
    const char *batch_missing_key = "batch-migration-missing";
    uint32_t batch_key1_len = (uint32_t)strlen(batch_key1);
    uint32_t batch_key2_len = (uint32_t)strlen(batch_key2);
    uint64_t batch_key1_hash = vemb_v16_xxh3_64_str(batch_key1, batch_key1_len);
    uint64_t batch_key2_hash = vemb_v16_xxh3_64_str(batch_key2, batch_key2_len);
    uint64_t batch_missing_hash =
        vemb_v16_xxh3_64_str(batch_missing_key, strlen(batch_missing_key));
    const char *epoch_key = "epoch-write-key";
    uint32_t epoch_key_len = (uint32_t)strlen(epoch_key);
    uint64_t epoch_key_hash = vemb_v16_xxh3_64_str(epoch_key, epoch_key_len);
    const char *pending_key = "pending-delta-cutover-key";
    uint32_t pending_key_len = (uint32_t)strlen(pending_key);
    uint64_t pending_key_hash = vemb_v16_xxh3_64_str(pending_key,
                                                 pending_key_len);
    float vector[dim];
    float vector2[dim];
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    tlc_core_key_migration_info_t info = {0};
    vemb_v16_completion_t completion = {0};
    vemb_v16_migration_control_req_t req = {0};
    vemb_v16_migration_control_resp_t resp = {0};
    vemb_v16_migration_control_batch_req_t batch_req = {0};
    vemb_v16_migration_control_batch_resp_t batch_resp = {0};
    vemb_v16_epoch_control_req_t epoch_req = {0};
    vemb_v16_epoch_control_resp_t epoch_resp = {0};
    vemb_v16_topology_control_req_t topology_req = {0};
    vemb_v16_topology_control_resp_t topology_resp = {0};
    const uint32_t default_owners[] = {0, 1};
    const uint32_t active_owners[] = {0, 1};
    const uint32_t standby_owners[] = {0, 1, 2};
    const uint32_t invalid_active_owners[] = {0, 2};
    const uint32_t invalid_standby_owners[] = {0, 1};

    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_migration_control_%ld.yaml", (long)getpid());
    snprintf(shm1, sizeof(shm1), "/v16ctlmr_%ld", (long)getpid());
    snprintf(remote_meta_default, sizeof(remote_meta_default),
             "/v16ctlmd_%ld", (long)getpid());
    snprintf(req0_1, sizeof(req0_1), "/v16ctl_%ld_req_0_1", (long)getpid());
    snprintf(req1_0, sizeof(req1_0), "/v16ctl_%ld_req_1_0", (long)getpid());
    snprintf(resp0_1, sizeof(resp0_1), "/v16ctl_%ld_resp_0_1",
             (long)getpid());
    snprintf(resp1_0, sizeof(resp1_0), "/v16ctl_%ld_resp_1_0",
             (long)getpid());

    cleanup_region_and_layout(shm1, 901);
    shm_unlink(remote_meta_default);
    shm_unlink(req0_1);
    shm_unlink(req1_0);
    shm_unlink(resp0_1);
    shm_unlink(resp1_0);

    FILE *fp = fopen(manifest_path, "w");
    assert(fp != NULL);
    fprintf(fp,
            "local_ub_node_id: 0\n"
            "remote_meta_provider: shm\n"
            "remote_meta_path: %s\n"
            "remote_meta_entries: %u\n"
            "remote_meta_buckets: %u\n"
            "ub_rpc_timeout_ms: 123\n"
            "warm_regions:\n"
            "  - region_id: 901\n"
            "    provider: shm\n"
            "    path: %s\n"
            "    mmap_offset: 0\n"
            "    bytes: %u\n"
            "    value_size: %u\n"
            "    home_ub_node_id: 0\n"
            "    is_local: true\n"
            "    weight: 1\n"
            "ub_rpc_peers:\n"
            "  - owner_id: 1\n"
            "    provider: shm\n"
            "    request_path: %s\n"
            "    response_path: %s\n"
            "    inbound_request_path: %s\n"
            "    outbound_response_path: %s\n",
            remote_meta_default,
            max_vectors,
            max_vectors * 2,
            shm1,
            (unsigned)(sizeof(float) * dim * max_vectors),
            (unsigned)(sizeof(float) * dim),
            req0_1,
            resp1_0,
            req1_0,
            resp0_1);
    fclose(fp);

    vemb_v16_warm_regions_manifest_t manifest;
    assert(vemb_v16_parse_warm_regions_manifest(manifest_path,
                                                dim * sizeof(float),
                                                &manifest) == 0);
    assert(vemb_v16_storage_ctx_create_from_manifest(&storage,
                                                     dim,
                                                     dim * sizeof(float),
                                                     max_vectors,
                                                     &manifest) == 0);
    assert(vemb_v16_proxy_create(&proxy,
                                 dim,
                                 max_vectors,
                                 storage,
                                 &manifest) == 0);

    assert(vemb_v16_proxy_epoch_get(proxy, &epoch_resp) == 0);
    assert(epoch_resp.status == VEMB_V16_STATUS_OK);
    assert(epoch_resp.current_topology_epoch == 0);
    assert(epoch_resp.min_write_epoch == 0);

    epoch_req.current_topology_epoch = 2;
    epoch_req.min_write_epoch = 3;
    assert(vemb_v16_proxy_epoch_set(proxy, &epoch_req, &epoch_resp) != 0);
    assert(epoch_resp.status == VEMB_V16_STATUS_ERR);
    assert(epoch_resp.current_topology_epoch == 0);
    assert(epoch_resp.min_write_epoch == 0);

    epoch_req.current_topology_epoch = 2;
    epoch_req.min_write_epoch = 2;
    assert(vemb_v16_proxy_epoch_set(proxy, &epoch_req, &epoch_resp) == 0);
    assert(epoch_resp.status == VEMB_V16_STATUS_OK);
    assert(epoch_resp.current_topology_epoch == 2);
    assert(epoch_resp.min_write_epoch == 2);

    epoch_req.current_topology_epoch = 3;
    epoch_req.min_write_epoch = 2;
    memset(&epoch_resp, 0, sizeof(epoch_resp));
    assert(tcp_epoch_control(proxy,
                             VEMB_V16_NET_EPOCH_SET,
                             &epoch_req,
                             &epoch_resp) == 0);
    assert(epoch_resp.status == VEMB_V16_STATUS_OK);
    assert(epoch_resp.current_topology_epoch == 3);
    assert(epoch_resp.min_write_epoch == 2);

    memset(&epoch_resp, 0, sizeof(epoch_resp));
    assert(tcp_epoch_control(proxy,
                             VEMB_V16_NET_EPOCH_GET,
                             NULL,
                             &epoch_resp) == 0);
    assert(epoch_resp.status == VEMB_V16_STATUS_OK);
    assert(epoch_resp.current_topology_epoch == 3);
    assert(epoch_resp.min_write_epoch == 2);

    epoch_req.current_topology_epoch = 4;
    epoch_req.min_write_epoch = 4;
    memset(&epoch_resp, 0, sizeof(epoch_resp));
    assert(tcp_epoch_control(proxy,
                             VEMB_V16_NET_EPOCH_SET,
                             &epoch_req,
                             &epoch_resp) == 0);
    assert(epoch_resp.status == VEMB_V16_STATUS_OK);
    assert(epoch_resp.current_topology_epoch == 4);
    assert(epoch_resp.min_write_epoch == 4);

    memset(&epoch_resp, 0, sizeof(epoch_resp));
    assert(tcp_epoch_control(proxy,
                             VEMB_V16_NET_EPOCH_GET,
                             NULL,
                             &epoch_resp) == 0);
    assert(epoch_resp.status == VEMB_V16_STATUS_OK);
    assert(epoch_resp.current_topology_epoch == 4);
    assert(epoch_resp.min_write_epoch == 4);

    assert(vemb_v16_proxy_topology_get(proxy, &topology_resp) == 0);
    assert(topology_resp.status == VEMB_V16_STATUS_OK);
    assert(topology_resp.current_topology_epoch == 4);
    assert(topology_resp.min_write_epoch == 4);
    assert(topology_resp.active_owner_count == 2);
    assert(topology_resp.standby_owner_count == 2);
    assert_owner_list(topology_resp.active_owners, default_owners, 2);
    assert_owner_list(topology_resp.standby_owners, default_owners, 2);

    /* A delayed candidate must not roll topology state back to an older
     * epoch after a newer publish has already been accepted. */
    fill_topology_req(&topology_req,
                      3,
                      3,
                      VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED,
                      default_owners,
                      2,
                      default_owners,
                      2);
    memset(&topology_resp, 0, sizeof(topology_resp));
    assert(vemb_v16_proxy_topology_set(proxy,
                                       &topology_req,
                                       &topology_resp) != 0);
    assert(topology_resp.status == VEMB_V16_STATUS_ERR);
    assert(topology_resp.current_topology_epoch == 4);
    assert(topology_resp.min_write_epoch == 4);
    assert(topology_resp.active_owner_count == 2);
    assert(topology_resp.standby_owner_count == 2);

    fill_topology_req(&topology_req,
                      5,
                      6,
                      VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED,
                      active_owners,
                      2,
                      standby_owners,
                      3);
    assert(vemb_v16_proxy_topology_set(proxy,
                                       &topology_req,
                                       &topology_resp) != 0);
    assert(topology_resp.status == VEMB_V16_STATUS_ERR);
    assert(topology_resp.current_topology_epoch == 4);
    assert(topology_resp.min_write_epoch == 4);
    assert(topology_resp.active_owner_count == 2);
    assert(topology_resp.standby_owner_count == 2);

    fill_topology_req(&topology_req,
                      4,
                      4,
                      VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED,
                      invalid_active_owners,
                      2,
                      invalid_standby_owners,
                      2);
    assert(vemb_v16_proxy_topology_set(proxy,
                                       &topology_req,
                                       &topology_resp) != 0);
    assert(topology_resp.status == VEMB_V16_STATUS_ERR);
    assert(topology_resp.current_topology_epoch == 4);
    assert(topology_resp.min_write_epoch == 4);
    assert(topology_resp.active_owner_count == 2);
    assert(topology_resp.standby_owner_count == 2);

    fill_topology_req(&topology_req,
                      4,
                      4,
                      VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED,
                      active_owners,
                      2,
                      standby_owners,
                      3);
    assert(vemb_v16_proxy_topology_set(proxy,
                                       &topology_req,
                                       &topology_resp) == 0);
    assert(topology_resp.status == VEMB_V16_STATUS_OK);
    assert(topology_resp.current_topology_epoch == 4);
    assert(topology_resp.min_write_epoch == 4);
    assert(topology_resp.flags & VEMB_V16_TOPOLOGY_CONTROL_F_PUBLISHED);
    assert(topology_resp.flags &
           VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED);
    assert(topology_resp.active_owner_count == 2);
    assert(topology_resp.standby_owner_count == 3);
    assert_owner_list(topology_resp.active_owners, active_owners, 2);
    assert_owner_list(topology_resp.standby_owners, standby_owners, 3);

    memset(&topology_resp, 0, sizeof(topology_resp));
    assert(tcp_topology_control(proxy,
                                VEMB_V16_NET_TOPOLOGY_GET,
                                NULL,
                                &topology_resp) == 0);
    assert(topology_resp.status == VEMB_V16_STATUS_OK);
    assert(topology_resp.active_owner_count == 2);
    assert(topology_resp.standby_owner_count == 3);
    assert_owner_list(topology_resp.active_owners, active_owners, 2);
    assert_owner_list(topology_resp.standby_owners, standby_owners, 3);

    memset(&topology_resp, 0, sizeof(topology_resp));
    assert(tcp_topology_control(proxy,
                                VEMB_V16_NET_TOPOLOGY_SET,
                                &topology_req,
                                &topology_resp) == 0);
    assert(topology_resp.status == VEMB_V16_STATUS_OK);
    assert(topology_resp.active_owner_count == 2);
    assert(topology_resp.standby_owner_count == 3);

    memset(&topology_resp, 0, sizeof(topology_resp));
    assert(tcp_topology_control(proxy,
                                VEMB_V16_NET_TOPOLOGY_GET,
                                NULL,
                                &topology_resp) == 0);
    assert(topology_resp.status == VEMB_V16_STATUS_OK);
    assert(topology_resp.active_owner_count == 2);
    assert(topology_resp.standby_owner_count == 3);
    assert_owner_list(topology_resp.active_owners, active_owners, 2);
    assert_owner_list(topology_resp.standby_owners, standby_owners, 3);

    memset(&topology_resp, 0, sizeof(topology_resp));
    assert(tcp_topology_control(proxy,
                                VEMB_V16_NET_TOPOLOGY_SET,
                                &topology_req,
                                &topology_resp) == 0);
    assert(topology_resp.status == VEMB_V16_STATUS_OK);
    assert(topology_resp.current_topology_epoch == 4);
    assert(topology_resp.min_write_epoch == 4);
    assert_topology_dual_write_key(&topology_resp, 2);

    fill_vector(vector, dim, 1000);
    assert(vemb_v16_tlc_put(storage->tlc,
                            epoch_key,
                            epoch_key_len,
                            epoch_key_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);
    assert(tlc_core_get_migration_info(storage->tlc->core,
                                           epoch_key,
                                           epoch_key_len,
                                           epoch_key_hash,
                                           &info) == 0);
    assert(info.key_version == 1);
    assert(info.topology_epoch == 0);

    fill_vector(vector2, dim, 2000);
    memset(&completion, 0, sizeof(completion));
    run_vadd_job(storage,
                 epoch_key,
                 epoch_key_hash,
                 3,
                 vector2,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);
    assert(tlc_core_get_migration_info(storage->tlc->core,
                                           epoch_key,
                                           epoch_key_len,
                                           epoch_key_hash,
                                           &info) == 0);
    assert(info.key_version == 2);
    assert(info.topology_epoch == 3);

    memset(&completion, 0, sizeof(completion));
    run_vadd_job(storage,
                 epoch_key,
                 epoch_key_hash,
                 4,
                 vector2,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);
    assert(tlc_core_get_migration_info(storage->tlc->core,
                                           epoch_key,
                                           epoch_key_len,
                                           epoch_key_hash,
                                           &info) == 0);
    assert(info.key_version == 3);
    assert(info.topology_epoch == 4);

    assert(vemb_v16_proxy_migration_mark_migrating(proxy, &req, &resp) != 0);
    assert(resp.status == VEMB_V16_STATUS_ERR);

    fill_vector(vector, dim, 2026);
    assert(vemb_v16_tlc_put(storage->tlc,
                            key,
                            key_len,
                            key_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);

    fill_control_req(&req, key, key_hash, 11, 1);

    assert(vemb_v16_proxy_migration_mark_migrating(proxy, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_STATUS_OK);
    assert(resp.key_hash == key_hash);
    assert(resp.topology_epoch == 11);
    assert(resp.migration_state == TLC_CORE_KEY_MIGRATING);
    assert(resp.target_owner == 1);
    assert(resp.tombstone == 0);
    memset(&completion, 0, sizeof(completion));
    run_vadd_job(storage,
                 key,
                 key_hash,
                 3,
                 vector2,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_STALE_TOPOLOGY);
    assert(tlc_core_get_migration_info(storage->tlc->core,
                                           key,
                                           key_len,
                                           key_hash,
                                           &info) == 0);
    assert(info.key_version == 1);
    assert(info.topology_epoch == 11);

    req.target_owner = 2;
    req.topology_epoch = 12;
    assert(vemb_v16_proxy_migration_mark_cutover(proxy, &req, &resp) != 0);
    assert(resp.status == VEMB_V16_STATUS_ERR);

    req.target_owner = 1;
    assert(vemb_v16_proxy_migration_mark_cutover(proxy, &req, &resp) != 0);
    assert(resp.status == VEMB_V16_STATUS_ERR);

    req.topology_epoch = 11;
    assert(vemb_v16_proxy_migration_barrier(proxy, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_STATUS_OK);
    assert(resp.key_hash == key_hash);
    assert(resp.topology_epoch == 11);
    assert(resp.migration_state == TLC_CORE_KEY_MIGRATING);
    assert(resp.target_owner == 1);
    assert(resp.pending_delta == 0);
    assert(resp.barrier_seq == 0);
    assert(resp.applied_seq == 0);
    assert(resp.outbox_state == VEMB_V16_MIGRATION_OUTBOX_OPEN);

    fill_vector(vector2, dim, 2121);
    memset(&completion, 0, sizeof(completion));
    run_vadd_job(storage,
                 key,
                 key_hash,
                 11,
                 vector2,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);
    assert(tlc_core_get_migration_info(storage->tlc->core,
                                           key,
                                           key_len,
                                           key_hash,
                                           &info) == 0);
    assert(info.key_version == 2);

    req.topology_epoch = 12;
    assert(vemb_v16_proxy_migration_mark_cutover(proxy, &req, &resp) != 0);
    assert(resp.status == VEMB_V16_STATUS_ERR);
    assert(vemb_v16_tlc_get_handle(storage->tlc,
                                   key,
                                   key_len,
                                   key_hash,
                                   &handle,
                                   &warm_slot) == 0);
    req.target_owner = 2;
    assert(vemb_v16_proxy_migration_mark_migrating(proxy, &req, &resp) != 0);
    assert(resp.status == VEMB_V16_STATUS_ERR);
    req.target_owner = 1;

    warm_slot = UINT32_MAX;
    fill_vector(vector, dim, 3030);
    assert(vemb_v16_tlc_put(storage->tlc,
                            transport_key,
                            transport_key_len,
                            transport_key_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);

    fill_control_req(&req, transport_key, transport_key_hash, 21, 1);
    assert(tcp_migration_control(proxy,
                                 VEMB_V16_NET_MIGRATION_MARK_MIGRATING,
                                 &req,
                                 &resp) == 0);
    assert(resp.status == VEMB_V16_STATUS_OK);
    assert(resp.key_hash == transport_key_hash);
    assert(resp.topology_epoch == 21);
    assert(resp.migration_state == TLC_CORE_KEY_MIGRATING);
    assert(resp.target_owner == 1);

    fill_control_req(&req, transport_key, transport_key_hash, 22, 1);
    assert(tcp_migration_control(proxy,
                                 VEMB_V16_NET_MIGRATION_MARK_CUTOVER,
                                 &req,
                                 &resp) == 0);
    assert(resp.status == VEMB_V16_STATUS_ERR);

    fill_control_req(&req, transport_key, transport_key_hash, 21, 1);
    assert(tcp_migration_control(proxy,
                                 VEMB_V16_NET_MIGRATION_BARRIER,
                                 &req,
                                 &resp) == 0);
    assert(resp.status == VEMB_V16_STATUS_OK);
    assert(resp.key_hash == transport_key_hash);
    assert(resp.topology_epoch == 21);
    assert(resp.outbox_state == VEMB_V16_MIGRATION_OUTBOX_OPEN);

    fill_control_req(&req, transport_key, transport_key_hash, 22, 1);
    assert(tcp_migration_control(proxy,
                                 VEMB_V16_NET_MIGRATION_MARK_CUTOVER,
                                 &req,
                                 &resp) == 0);
    assert(resp.status == VEMB_V16_STATUS_ERR);
    assert(vemb_v16_tlc_get_handle(storage->tlc,
                                   transport_key,
                                   transport_key_len,
                                   transport_key_hash,
                                   &handle,
                                   &warm_slot) == 0);

    warm_slot = UINT32_MAX;
    fill_vector(vector, dim, 4141);
    assert(vemb_v16_tlc_put(storage->tlc,
                            pending_key,
                            pending_key_len,
                            pending_key_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);
    fill_control_req(&req, pending_key, pending_key_hash, 41, 1);
    assert(vemb_v16_proxy_migration_mark_migrating(proxy, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_STATUS_OK);
    assert(resp.migration_state == TLC_CORE_KEY_MIGRATING);

    vemb_v16_migration_outbox_t *pending_outbox = NULL;
    vemb_v16_migration_outbox_config_t pending_outbox_config = {
        .source_owner = storage->local_owner_id,
        .target_owner = 1,
        .shard_id = 0,
        .capacity = 4,
        .topology_epoch = 41,
    };
    assert(vemb_v16_migration_outbox_create(&pending_outbox,
                                            &pending_outbox_config) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    vemb_v16_ub_migration_delta_desc_t pending_delta = {
        .key_hash = pending_key_hash,
        .key_version = 2,
        .topology_epoch = 41,
        .op = VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT,
        .key_len = pending_key_len,
        .source_owner = storage->local_owner_id,
        .target_owner = 1,
        .value_size = sizeof(vector),
        .region_id = handle.region_id,
        .local_slot = handle.local_slot,
        .bytes = handle.bytes,
        .offset = handle.offset,
        .owner_generation = handle.owner_generation,
    };
    memcpy(pending_delta.key, pending_key, pending_key_len);
    assert(vemb_v16_migration_outbox_append(pending_outbox,
                                            &pending_delta,
                                            NULL) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    pthread_mutex_lock(&storage->migration_outbox_lock);
    assert(storage->migration_outbox_count <
           VEMB_V16_STORAGE_MAX_MIGRATION_OUTBOXES);
    storage->migration_outboxes[storage->migration_outbox_count++] =
        pending_outbox;
    pthread_mutex_unlock(&storage->migration_outbox_lock);

    assert(vemb_v16_proxy_migration_barrier(proxy, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_STATUS_OK);
    assert(resp.pending_delta == 1);
    assert(resp.barrier_seq == 1);
    assert(resp.applied_seq == 0);
    assert(resp.outbox_state == VEMB_V16_MIGRATION_OUTBOX_OPEN);

    req.topology_epoch = 42;
    assert(vemb_v16_proxy_migration_mark_cutover(proxy, &req, &resp) != 0);
    assert(resp.status == VEMB_V16_STATUS_ERR);
    assert(vemb_v16_migration_outbox_ack(pending_outbox, 1) ==
           VEMB_V16_MIGRATION_OUTBOX_OK);
    assert(vemb_v16_proxy_migration_mark_cutover(proxy, &req, &resp) != 0);
    assert(resp.status == VEMB_V16_STATUS_ERR);

    warm_slot = UINT32_MAX;
    fill_vector(vector, dim, 4040);
    assert(vemb_v16_tlc_put(storage->tlc,
                            batch_key1,
                            batch_key1_len,
                            batch_key1_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);
    warm_slot = UINT32_MAX;
    fill_vector(vector, dim, 5050);
    assert(vemb_v16_tlc_put(storage->tlc,
                            batch_key2,
                            batch_key2_len,
                            batch_key2_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);

    batch_req.entry_count = 3;
    fill_control_req(&batch_req.entries[0],
                     batch_key1,
                     batch_key1_hash,
                     31,
                     1);
    fill_control_req(&batch_req.entries[1],
                     batch_missing_key,
                     batch_missing_hash,
                     31,
                     1);
    fill_control_req(&batch_req.entries[2],
                     batch_key2,
                     batch_key2_hash,
                     32,
                     1);

    assert(vemb_v16_proxy_migration_mark_migrating_batch(proxy,
                                                         &batch_req,
                                                         &batch_resp) != 0);
    assert(batch_resp.status == VEMB_V16_STATUS_ERR);
    assert(batch_resp.entry_count == 3);
    assert(batch_resp.success_count == 2);
    assert(batch_resp.error_count == 1);
    assert(batch_resp.entries[0].status == VEMB_V16_STATUS_OK);
    assert(batch_resp.entries[0].migration_state == TLC_CORE_KEY_MIGRATING);
    assert(batch_resp.entries[0].topology_epoch == 31);
    assert(batch_resp.entries[1].status == VEMB_V16_STATUS_ERR);
    assert(batch_resp.entries[2].status == VEMB_V16_STATUS_OK);
    assert(batch_resp.entries[2].migration_state == TLC_CORE_KEY_MIGRATING);
    assert(batch_resp.entries[2].topology_epoch == 32);

    memset(&batch_resp, 0, sizeof(batch_resp));
    assert(tcp_migration_batch_control(proxy, &batch_req, &batch_resp) == 0);
    assert(batch_resp.status == VEMB_V16_STATUS_ERR);
    assert(batch_resp.entry_count == 3);
    assert(batch_resp.success_count == 2);
    assert(batch_resp.error_count == 1);
    assert(batch_resp.entries[0].status == VEMB_V16_STATUS_OK);
    assert(batch_resp.entries[1].status == VEMB_V16_STATUS_ERR);
    assert(batch_resp.entries[2].status == VEMB_V16_STATUS_OK);

    memset(&batch_resp, 0, sizeof(batch_resp));
    assert(tcp_migration_batch_control(proxy, &batch_req, &batch_resp) == 0);
    assert(batch_resp.status == VEMB_V16_STATUS_ERR);
    assert(batch_resp.entry_count == 3);
    assert(batch_resp.success_count == 2);
    assert(batch_resp.error_count == 1);
    assert(batch_resp.entries[0].status == VEMB_V16_STATUS_OK);
    assert(batch_resp.entries[1].status == VEMB_V16_STATUS_ERR);
    assert(batch_resp.entries[2].status == VEMB_V16_STATUS_OK);

    vemb_v16_proxy_destroy(proxy);
    vemb_v16_storage_ctx_destroy(storage);
    assert(vemb_v16_storage_reset_manifest_regions(&manifest) == 0);
    unlink(manifest_path);
    shm_unlink(remote_meta_default);
    shm_unlink(req0_1);
    shm_unlink(req1_0);
    shm_unlink(resp0_1);
    shm_unlink(resp1_0);
    cleanup_region_and_layout(shm1, 901);
}

static void test_storage_topology_auto_marks_migrating_keys(void) {
    enum { dim = 2, max_vectors = 16 };
    float region[dim * max_vectors];
    float vector[dim];
    vemb_v16_warm_region_header_t allocator;
    vemb_v16_tlc_t *tlc = NULL;
    vemb_v16_tlc_warm_region_t warm = {
        .region_id = 911,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = region,
        .region_bytes = sizeof(region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_storage_ctx_t storage;
    vemb_v16_topology_control_req_t topology_req = {0};
    vemb_v16_topology_ring_t standby_ring;
    const uint32_t active_owners[] = {0};
    const uint32_t standby_owners[] = {0, 1};
    char migrate_key[64];
    char stay_key[64];
    uint64_t migrate_key_hash = 0;
    uint64_t stay_key_hash = 0;
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    tlc_core_key_migration_info_t info = {0};

    memset(region, 0, sizeof(region));
    init_test_allocator(&allocator, 911, max_vectors);
    assert(vemb_v16_tlc_create(&tlc,
                               dim,
                               max_vectors,
                               &warm,
                               1,
                               4) == 0);
    assert(vemb_v16_topology_ring_build(&standby_ring,
                                        7,
                                        standby_owners,
                                        2,
                                        VEMB_V16_TOPOLOGY_DEFAULT_VNODES) ==
           VEMB_V16_TOPOLOGY_OK);
    find_key_for_ring_owner(&standby_ring,
                            1,
                            "auto-plan-migrate",
                            migrate_key,
                            sizeof(migrate_key),
                            &migrate_key_hash);
    find_key_for_ring_owner(&standby_ring,
                            0,
                            "auto-plan-stay",
                            stay_key,
                            sizeof(stay_key),
                            &stay_key_hash);

    memset(&storage, 0, sizeof(storage));
    storage.local_owner_id = 0;
    storage.tlc = tlc;
    init_storage_runtime_fields(&storage, 0, 0);
    assert(pthread_mutex_init(&storage.topology_lock, NULL) == 0);
    assert(pthread_mutex_init(&storage.migration_outbox_lock, NULL) == 0);

    fill_vector(vector, dim, 7400);
    assert(vemb_v16_tlc_put(tlc,
                            migrate_key,
                            (uint32_t)strlen(migrate_key),
                            migrate_key_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);
    warm_slot = UINT32_MAX;
    fill_vector(vector, dim, 7500);
    assert(vemb_v16_tlc_put(tlc,
                            stay_key,
                            (uint32_t)strlen(stay_key),
                            stay_key_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);

    fill_topology_req(&topology_req,
                      7,
                      7,
                      VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED,
                      active_owners,
                      1,
                      standby_owners,
                      2);
    assert(vemb_v16_storage_topology_set(&storage, &topology_req) == 0);
    assert(vemb_v16_storage_migration_active(&storage));

    assert(tlc_core_get_migration_info(tlc->core,
                                           migrate_key,
                                           (uint32_t)strlen(migrate_key),
                                           migrate_key_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_MIGRATING);
    assert(info.target_owner == 1);
    assert(info.topology_epoch == 7);
    assert(info.shard_id == 0);

    memset(&info, 0, sizeof(info));
    assert(tlc_core_get_migration_info(tlc->core,
                                           stay_key,
                                           (uint32_t)strlen(stay_key),
                                           stay_key_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_SOURCE_ACTIVE);
    assert(info.target_owner == UINT32_MAX);

    pthread_mutex_destroy(&storage.migration_outbox_lock);
    pthread_mutex_destroy(&storage.topology_lock);
    vemb_v16_tlc_destroy(tlc);
}

static void test_proxy_peer_view_topology_set_applies_mapping_first(void) {
    enum { dim = 2, max_vectors = 4, slots = 2 };
    char manifest_path[128];
    char shm1[64];
    char combo_region[64];
    char remote_meta_default[64];
    vemb_v16_storage_ctx_t *storage = NULL;
    vemb_v16_proxy_t *proxy = NULL;
    vemb_v16_warm_regions_manifest_t manifest;
    vemb_v16_peer_view_topology_control_req_t req;
    vemb_v16_peer_view_topology_control_resp_t resp;
    vemb_v16_topology_control_resp_t topology_resp;
    const uint32_t active_owners[] = {0, 1};
    const uint32_t standby_owners[] = {0, 1, 2};

    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_combo_%ld.yaml",
             (long)getpid());
    snprintf(shm1, sizeof(shm1), "/v16combo_r1_%ld", (long)getpid());
    snprintf(combo_region, sizeof(combo_region),
             "/tmp/v16combo_r2_%ld.ub",
             (long)getpid());
    snprintf(remote_meta_default, sizeof(remote_meta_default),
             "/v16combo_meta_%ld", (long)getpid());
    unlink(manifest_path);
    cleanup_region_and_layout(shm1, 801);
    unlink(combo_region);
    shm_unlink(remote_meta_default);
    create_test_ub_region_file(combo_region, 802, slots, dim * sizeof(float));

    FILE *fp = fopen(manifest_path, "w");
    assert(fp != NULL);
    fprintf(fp,
            "local_ub_node_id: 0\n"
            "remote_meta_provider: shm\n"
            "remote_meta_path: %s\n"
            "remote_meta_entries: %u\n"
            "remote_meta_buckets: %u\n"
            "warm_regions:\n"
            "  - region_id: 801\n"
            "    provider: shm\n"
            "    path: %s\n"
            "    mmap_offset: 0\n"
            "    bytes: %u\n"
            "    value_size: %u\n"
            "    home_ub_node_id: 0\n"
            "    is_local: true\n"
            "    weight: 1\n",
            remote_meta_default,
            max_vectors,
            max_vectors * 2,
            shm1,
            (unsigned)(sizeof(float) * dim * slots),
            (unsigned)(sizeof(float) * dim));
    fclose(fp);

    assert(vemb_v16_parse_warm_regions_manifest(manifest_path,
                                                dim * sizeof(float),
                                                &manifest) == 0);
    assert(vemb_v16_storage_ctx_create_from_manifest(&storage,
                                                     dim,
                                                     dim * sizeof(float),
                                                     max_vectors,
                                                     &manifest) == 0);
    assert(vemb_v16_proxy_create(&proxy,
                                 dim,
                                 max_vectors,
                                 storage,
                                 &manifest) == 0);

    memset(&req, 0, sizeof(req));
    req.peer_view_map_req.flags = VEMB_V16_PEER_VIEW_MAP_F_ATTACH_NOW;
    req.peer_view_map_req.expected_local_owner_id = 0;
    req.peer_view_map_req.expected_local_owner_valid = 1;
    req.peer_view_map_req.region_count = 1;
    req.peer_view_map_req.regions[0].region_id = 802;
    req.peer_view_map_req.regions[0].backend_type = VEMB_V16_REGION_UB;
    req.peer_view_map_req.regions[0].home_ub_node_id = 2;
    req.peer_view_map_req.regions[0].weight = 1;
    req.peer_view_map_req.regions[0].value_size = dim * sizeof(float);
    req.peer_view_map_req.regions[0].region_bytes =
        sizeof(float) * dim * slots;
    snprintf(req.peer_view_map_req.regions[0].path,
             sizeof(req.peer_view_map_req.regions[0].path),
             "%s",
             combo_region);

    fill_topology_req(&req.topology_req,
                      1,
                      1,
                      0,
                      active_owners,
                      2,
                      standby_owners,
                      3);
    req.topology_req.endpoint_count = 1;
    req.topology_req.endpoints[0].owner_id = 2;
    req.topology_req.endpoints[0].transport_type = VEMB_V16_TRANSPORT_TCP;
    snprintf(req.topology_req.endpoints[0].host,
             sizeof(req.topology_req.endpoints[0].host),
             "%s",
             "127.0.0.1");
    req.topology_req.endpoints[0].tcp_port = 6399;

    memset(&resp, 0, sizeof(resp));
    assert(tcp_peer_view_topology_control(proxy, &req, &resp) == 0);
    assert(resp.status == VEMB_V16_STATUS_OK);
    assert(resp.peer_view_map_status == VEMB_V16_STATUS_OK);
    assert(resp.topology_attempted == 1);
    assert(resp.topology_status == VEMB_V16_STATUS_OK);
    assert(resp.peer_view_map_resp.applied_region_count == 1);

    memset(&topology_resp, 0, sizeof(topology_resp));
    assert(vemb_v16_proxy_topology_get(proxy, &topology_resp) == 0);
    assert(topology_resp.status == VEMB_V16_STATUS_OK);
    assert(topology_resp.active_owner_count == 2);
    assert(topology_resp.standby_owner_count == 3);
    assert_owner_list(topology_resp.active_owners, active_owners, 2);
    assert_owner_list(topology_resp.standby_owners, standby_owners, 3);
    assert(vemb_v16_tlc_find_region(storage->tlc, 802) != NULL);

    vemb_v16_proxy_destroy(proxy);
    unlink(manifest_path);
    cleanup_region_and_layout(shm1, 801);
    unlink(combo_region);
    shm_unlink(remote_meta_default);
}

static void test_storage_topology_auto_pushes_baseline_snapshot(void) {
    enum { dim = 2, max_vectors = 16 };
    float source_region[dim * max_vectors];
    float target_region[dim * max_vectors];
    float vector[dim];
    vemb_v16_warm_region_header_t source_allocator;
    vemb_v16_warm_region_header_t target_allocator;
    vemb_v16_tlc_t *source = NULL;
    vemb_v16_tlc_t *target = NULL;
    vemb_v16_ub_rpc_t *source_rpc = NULL;
    vemb_v16_ub_rpc_t *target_rpc = NULL;
    vemb_v16_tlc_warm_region_t source_warm = {
        .region_id = 921,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = source_region,
        .region_bytes = sizeof(source_region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_tlc_warm_region_t target_regions[] = {
        {
            .region_id = 921,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 0,
            .weight = 1,
            .mapped_addr = source_region,
            .region_bytes = sizeof(source_region),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
        {
            .region_id = 923,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = target_region,
            .region_bytes = sizeof(target_region),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
    };
    char req_source_target[64];
    char req_target_source[64];
    char resp_source_target[64];
    char resp_target_source[64];
    vemb_v16_ub_rpc_peer_t source_peer;
    vemb_v16_ub_rpc_peer_t target_peer;
    vemb_v16_storage_ctx_t source_storage;
    vemb_v16_topology_control_req_t topology_req = {0};
    vemb_v16_topology_ring_t standby_ring;
    const uint32_t active_owners[] = {0};
    const uint32_t standby_owners[] = {0, 1};
    char key[64];
    uint64_t key_hash = 0;
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    tlc_core_key_migration_info_t info = {0};
    const uint8_t *target_value = NULL;
    uint32_t target_value_size = 0;

    snprintf(req_source_target,
             sizeof(req_source_target),
             "/v16ab_%ld_q01",
             (long)getpid());
    snprintf(req_target_source,
             sizeof(req_target_source),
             "/v16ab_%ld_q10",
             (long)getpid());
    snprintf(resp_source_target,
             sizeof(resp_source_target),
             "/v16ab_%ld_s01",
             (long)getpid());
    snprintf(resp_target_source,
             sizeof(resp_target_source),
             "/v16ab_%ld_s10",
             (long)getpid());
    cleanup_rpc_rings(req_source_target,
                      req_target_source,
                      resp_source_target,
                      resp_target_source);

    memset(source_region, 0, sizeof(source_region));
    memset(target_region, 0, sizeof(target_region));
    init_test_allocator(&source_allocator, 921, max_vectors);
    init_test_allocator(&target_allocator, 923, max_vectors);
    assert(vemb_v16_tlc_create(&source,
                               dim,
                               max_vectors,
                               &source_warm,
                               1,
                               4) == 0);
    assert(vemb_v16_tlc_create(&target,
                               dim,
                               max_vectors,
                               target_regions,
                               2,
                               4) == 0);
    make_rpc_peers(&source_peer,
                   1,
                   &target_peer,
                   0,
                   req_source_target,
                   req_target_source,
                   resp_source_target,
                   resp_target_source);
    assert(vemb_v16_ub_rpc_create(&source_rpc,
                                  source,
                                  0,
                                  100,
                                  &source_peer,
                                  1) == 0);
    assert(vemb_v16_ub_rpc_create(&target_rpc,
                                  target,
                                  1,
                                  100,
                                  &target_peer,
                                  1) == 0);
    assert(vemb_v16_topology_ring_build(&standby_ring,
                                        9,
                                        standby_owners,
                                        2,
                                        VEMB_V16_TOPOLOGY_DEFAULT_VNODES) ==
           VEMB_V16_TOPOLOGY_OK);
    find_key_for_ring_owner(&standby_ring,
                            1,
                            "auto-baseline-migrate",
                            key,
                            sizeof(key),
                            &key_hash);

    memset(&source_storage, 0, sizeof(source_storage));
    source_storage.local_owner_id = 0;
    source_storage.tlc = source;
    source_storage.ub_rpc = source_rpc;
    init_storage_runtime_fields(&source_storage, 0, 0);
    assert(pthread_mutex_init(&source_storage.topology_lock, NULL) == 0);
    assert(pthread_mutex_init(&source_storage.migration_outbox_lock, NULL) == 0);

    fill_vector(vector, dim, 7600);
    assert(vemb_v16_tlc_put(source,
                            key,
                            (uint32_t)strlen(key),
                            key_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);
    fill_topology_req(&topology_req,
                      9,
                      9,
                      VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED,
                      active_owners,
                      1,
                      standby_owners,
                      2);
    assert(vemb_v16_storage_topology_set(&source_storage,
                                         &topology_req) == 0);

    assert(tlc_core_get_migration_info(source->core,
                                           key,
                                           (uint32_t)strlen(key),
                                           key_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_MIGRATING);
    assert(info.target_owner == 1);
    assert(info.topology_epoch == 9);

    memset(&info, 0, sizeof(info));
    assert(tlc_core_get_migration_info(target->core,
                                           key,
                                           (uint32_t)strlen(key),
                                           key_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_DEST_COMMITTED);
    assert(info.target_owner == 1);
    assert(info.topology_epoch == 9);
    assert(info.key_version == 1);
    assert(info.tombstone == 0);
    assert(vemb_v16_tlc_get_handle(target,
                                   key,
                                   (uint32_t)strlen(key),
                                   key_hash,
                                   &handle,
                                   &warm_slot) == 0);
    assert(vemb_v16_tlc_vector_slice(target,
                                     &handle,
                                     &target_value,
                                     &target_value_size) == 0);
    assert(target_value_size == sizeof(vector));
    assert(memcmp(target_value, vector, sizeof(vector)) == 0);

    pthread_mutex_destroy(&source_storage.migration_outbox_lock);
    pthread_mutex_destroy(&source_storage.topology_lock);
    vemb_v16_ub_rpc_destroy(target_rpc);
    vemb_v16_ub_rpc_destroy(source_rpc);
    vemb_v16_tlc_destroy(target);
    vemb_v16_tlc_destroy(source);
    cleanup_rpc_rings(req_source_target,
                      req_target_source,
                      resp_source_target,
                      resp_target_source);
}

static void test_storage_baseline_retry_drains_after_target_ready(void) {
    enum { dim = 2, max_vectors = 16 };
    float source_region[dim * max_vectors];
    float target_region[dim * max_vectors];
    float vector[dim];
    vemb_v16_warm_region_header_t source_allocator;
    vemb_v16_warm_region_header_t target_allocator;
    vemb_v16_tlc_t *source = NULL;
    vemb_v16_tlc_t *target = NULL;
    vemb_v16_ub_rpc_t *source_rpc = NULL;
    vemb_v16_ub_rpc_t *target_rpc = NULL;
    vemb_v16_tlc_warm_region_t source_warm = {
        .region_id = 931,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = source_region,
        .region_bytes = sizeof(source_region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_tlc_warm_region_t target_regions[] = {
        {
            .region_id = 931,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 0,
            .weight = 1,
            .mapped_addr = source_region,
            .region_bytes = sizeof(source_region),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
        {
            .region_id = 933,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = target_region,
            .region_bytes = sizeof(target_region),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
    };
    char req_source_target[64];
    char req_target_source[64];
    char resp_source_target[64];
    char resp_target_source[64];
    vemb_v16_ub_rpc_peer_t source_peer;
    vemb_v16_ub_rpc_peer_t target_peer;
    vemb_v16_storage_ctx_t source_storage;
    vemb_v16_topology_control_req_t topology_req = {0};
    vemb_v16_topology_ring_t standby_ring;
    const uint32_t active_owners[] = {0};
    const uint32_t standby_owners[] = {0, 1};
    char key[64];
    uint64_t key_hash = 0;
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    tlc_core_key_migration_info_t info = {0};
    const uint8_t *target_value = NULL;
    uint32_t target_value_size = 0;
    uint32_t sent_count = 0;
    uint32_t acked_count = 0;
    uint32_t total_acked = 0;

    snprintf(req_source_target,
             sizeof(req_source_target),
             "/v16br_%ld_q01",
             (long)getpid());
    snprintf(req_target_source,
             sizeof(req_target_source),
             "/v16br_%ld_q10",
             (long)getpid());
    snprintf(resp_source_target,
             sizeof(resp_source_target),
             "/v16br_%ld_s01",
             (long)getpid());
    snprintf(resp_target_source,
             sizeof(resp_target_source),
             "/v16br_%ld_s10",
             (long)getpid());
    cleanup_rpc_rings(req_source_target,
                      req_target_source,
                      resp_source_target,
                      resp_target_source);

    memset(source_region, 0, sizeof(source_region));
    memset(target_region, 0, sizeof(target_region));
    init_test_allocator(&source_allocator, 931, max_vectors);
    init_test_allocator(&target_allocator, 933, max_vectors);
    assert(vemb_v16_tlc_create(&source,
                               dim,
                               max_vectors,
                               &source_warm,
                               1,
                               4) == 0);
    assert(vemb_v16_tlc_create(&target,
                               dim,
                               max_vectors,
                               target_regions,
                               2,
                               4) == 0);
    make_rpc_peers(&source_peer,
                   1,
                   &target_peer,
                   0,
                   req_source_target,
                   req_target_source,
                   resp_source_target,
                   resp_target_source);
    assert(vemb_v16_ub_rpc_create(&source_rpc,
                                  source,
                                  0,
                                  5,
                                  &source_peer,
                                  1) == 0);
    assert(vemb_v16_topology_ring_build(&standby_ring,
                                        11,
                                        standby_owners,
                                        2,
                                        VEMB_V16_TOPOLOGY_DEFAULT_VNODES) ==
           VEMB_V16_TOPOLOGY_OK);
    find_key_for_ring_owner(&standby_ring,
                            1,
                            "auto-baseline-retry",
                            key,
                            sizeof(key),
                            &key_hash);

    memset(&source_storage, 0, sizeof(source_storage));
    source_storage.local_owner_id = 0;
    source_storage.tlc = source;
    source_storage.ub_rpc = source_rpc;
    init_storage_runtime_fields(&source_storage, 0, 0);
    assert(pthread_mutex_init(&source_storage.topology_lock, NULL) == 0);
    assert(pthread_mutex_init(&source_storage.migration_outbox_lock,
                              NULL) == 0);

    fill_vector(vector, dim, 7700);
    assert(vemb_v16_tlc_put(source,
                            key,
                            (uint32_t)strlen(key),
                            key_hash,
                            vector,
                            sizeof(vector),
                            &handle,
                            &warm_slot) == 0);
    fill_topology_req(&topology_req,
                      11,
                      11,
                      VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED,
                      active_owners,
                      1,
                      standby_owners,
                      2);
    assert(vemb_v16_storage_topology_set(&source_storage,
                                         &topology_req) == 0);

    assert(tlc_core_get_migration_info(source->core,
                                           key,
                                           (uint32_t)strlen(key),
                                           key_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_MIGRATING);
    assert(info.target_owner == 1);
    assert(source_storage.migration_baseline_retry_count == 1);
    assert(atomic_load_explicit(
               &source_storage.migration_baseline_error_count,
               memory_order_relaxed) == 1);
    assert(atomic_load_explicit(
               &source_storage.migration_baseline_retry_queued_count,
               memory_order_relaxed) == 1);
    assert(atomic_load_explicit(
               &source_storage.migration_baseline_sent_count,
               memory_order_relaxed) == 0);

    assert(vemb_v16_ub_rpc_create(&target_rpc,
                                  target,
                                  1,
                                  5,
                                  &target_peer,
                                  1) == 0);
    for (uint32_t i = 0; i < 200; i++) {
        sent_count = 0;
        acked_count = 0;
        assert(vemb_v16_storage_migration_drain_baselines(&source_storage,
                                                          1,
                                                          &sent_count,
                                                          &acked_count) == 0);
        total_acked += acked_count;
        if (source_storage.migration_baseline_retry_count == 0)
            break;
        usleep(1000);
    }
    assert(source_storage.migration_baseline_retry_count == 0);
    assert(total_acked == 1);
    assert(atomic_load_explicit(
               &source_storage.migration_baseline_sent_count,
               memory_order_relaxed) == 1);
    assert(atomic_load_explicit(
               &source_storage.migration_baseline_retry_sent_count,
               memory_order_relaxed) == 1);

    memset(&info, 0, sizeof(info));
    assert(tlc_core_get_migration_info(target->core,
                                           key,
                                           (uint32_t)strlen(key),
                                           key_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_DEST_COMMITTED);
    assert(info.target_owner == 1);
    assert(info.topology_epoch == 11);
    assert(vemb_v16_tlc_get_handle(target,
                                   key,
                                   (uint32_t)strlen(key),
                                   key_hash,
                                   &handle,
                                   &warm_slot) == 0);
    assert(vemb_v16_tlc_vector_slice(target,
                                     &handle,
                                     &target_value,
                                     &target_value_size) == 0);
    assert(target_value_size == sizeof(vector));
    assert(memcmp(target_value, vector, sizeof(vector)) == 0);

    pthread_mutex_destroy(&source_storage.migration_outbox_lock);
    pthread_mutex_destroy(&source_storage.topology_lock);
    vemb_v16_ub_rpc_destroy(target_rpc);
    vemb_v16_ub_rpc_destroy(source_rpc);
    vemb_v16_tlc_destroy(target);
    vemb_v16_tlc_destroy(source);
    cleanup_rpc_rings(req_source_target,
                      req_target_source,
                      resp_source_target,
                      resp_target_source);
}

static void test_storage_auto_scaleout_state_machine_cutover(void) {
    enum { dim = 2, max_vectors = 16 };
    float source_region[dim * max_vectors];
    float target_region[dim * max_vectors];
    float initial[dim];
    float updated[dim];
    vemb_v16_warm_region_header_t source_allocator;
    vemb_v16_warm_region_header_t target_allocator;
    vemb_v16_tlc_t *source = NULL;
    vemb_v16_tlc_t *target = NULL;
    vemb_v16_ub_rpc_t *source_rpc = NULL;
    vemb_v16_ub_rpc_t *target_rpc = NULL;
    vemb_v16_tlc_warm_region_t source_warm = {
        .region_id = 941,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = source_region,
        .region_bytes = sizeof(source_region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_tlc_warm_region_t target_regions[] = {
        {
            .region_id = 941,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 0,
            .weight = 1,
            .mapped_addr = source_region,
            .region_bytes = sizeof(source_region),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
        {
            .region_id = 943,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = target_region,
            .region_bytes = sizeof(target_region),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
    };
    char req_source_target[64];
    char req_target_source[64];
    char resp_source_target[64];
    char resp_target_source[64];
    vemb_v16_ub_rpc_peer_t source_peer;
    vemb_v16_ub_rpc_peer_t target_peer;
    vemb_v16_storage_ctx_t source_storage;
    vemb_v16_topology_control_req_t topology_req = {0};
    vemb_v16_topology_control_resp_t topology_resp = {0};
    vemb_v16_topology_ring_t standby_ring;
    const uint32_t active_owners[] = {0};
    const uint32_t standby_owners[] = {0, 1};
    char key[64];
    uint64_t key_hash = 0;
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    tlc_core_key_migration_info_t info = {0};
    vemb_v16_completion_t completion = {0};
    const uint8_t *target_value = NULL;
    uint32_t target_value_size = 0;

    snprintf(req_source_target,
             sizeof(req_source_target),
             "/v16as_%ld_q01",
             (long)getpid());
    snprintf(req_target_source,
             sizeof(req_target_source),
             "/v16as_%ld_q10",
             (long)getpid());
    snprintf(resp_source_target,
             sizeof(resp_source_target),
             "/v16as_%ld_s01",
             (long)getpid());
    snprintf(resp_target_source,
             sizeof(resp_target_source),
             "/v16as_%ld_s10",
             (long)getpid());
    cleanup_rpc_rings(req_source_target,
                      req_target_source,
                      resp_source_target,
                      resp_target_source);

    memset(source_region, 0, sizeof(source_region));
    memset(target_region, 0, sizeof(target_region));
    init_test_allocator(&source_allocator, 941, max_vectors);
    init_test_allocator(&target_allocator, 943, max_vectors);
    assert(vemb_v16_tlc_create(&source,
                               dim,
                               max_vectors,
                               &source_warm,
                               1,
                               4) == 0);
    assert(vemb_v16_tlc_create(&target,
                               dim,
                               max_vectors,
                               target_regions,
                               2,
                               4) == 0);
    make_rpc_peers(&source_peer,
                   1,
                   &target_peer,
                   0,
                   req_source_target,
                   req_target_source,
                   resp_source_target,
                   resp_target_source);
    assert(vemb_v16_ub_rpc_create(&source_rpc,
                                  source,
                                  0,
                                  50,
                                  &source_peer,
                                  1) == 0);
    assert(vemb_v16_ub_rpc_create(&target_rpc,
                                  target,
                                  1,
                                  50,
                                  &target_peer,
                                  1) == 0);
    assert(vemb_v16_topology_ring_build(&standby_ring,
                                        13,
                                        standby_owners,
                                        2,
                                        VEMB_V16_TOPOLOGY_DEFAULT_VNODES) ==
           VEMB_V16_TOPOLOGY_OK);
    find_key_for_ring_owner(&standby_ring,
                            1,
                            "auto-scaleout",
                            key,
                            sizeof(key),
                            &key_hash);

    memset(&source_storage, 0, sizeof(source_storage));
    source_storage.local_owner_id = 0;
    source_storage.tlc = source;
    source_storage.ub_rpc = source_rpc;
    init_storage_runtime_fields(&source_storage, 0, 0);
    assert(pthread_mutex_init(&source_storage.topology_lock, NULL) == 0);
    assert(pthread_mutex_init(&source_storage.migration_outbox_lock,
                              NULL) == 0);

    fill_vector(initial, dim, 7800);
    assert(vemb_v16_tlc_put(source,
                            key,
                            (uint32_t)strlen(key),
                            key_hash,
                            initial,
                            sizeof(initial),
                            &handle,
                            &warm_slot) == 0);
    fill_topology_req(&topology_req,
                      13,
                      13,
                      VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED |
                          VEMB_V16_TOPOLOGY_CONTROL_F_AUTO_SCALEOUT,
                      active_owners,
                      1,
                      standby_owners,
                      2);
    assert(vemb_v16_storage_topology_set(&source_storage,
                                         &topology_req) == 0);
    assert(source_storage.scaleout_auto_enabled == 1);
    assert(source_storage.scaleout_auto_phase ==
           VEMB_V16_STORAGE_SCALEOUT_DRAINING);

    fill_vector(updated, dim, 7900);
    run_vadd_job(&source_storage,
                 key,
                 key_hash,
                 13,
                 updated,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);

    for (uint32_t i = 0; i < 50; i++) {
        assert(vemb_v16_storage_scaleout_auto_step(&source_storage) == 0);
        if (source_storage.scaleout_auto_phase ==
            VEMB_V16_STORAGE_SCALEOUT_DONE) {
            break;
        }
        usleep(1000);
    }
    assert(source_storage.scaleout_auto_phase ==
           VEMB_V16_STORAGE_SCALEOUT_DONE);
    assert(!vemb_v16_storage_migration_active(&source_storage));

    vemb_v16_storage_topology_get(&source_storage, &topology_resp);
    assert(topology_resp.status == VEMB_V16_STATUS_OK);
    assert(topology_resp.current_topology_epoch == 14);
    assert(topology_resp.min_write_epoch == 14);
    assert(topology_resp.active_owner_count == 2);
    assert(topology_resp.standby_owner_count == 2);
    assert((topology_resp.flags &
            VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED) == 0);
    assert((topology_resp.flags &
            VEMB_V16_TOPOLOGY_CONTROL_F_AUTO_SCALEOUT) == 0);

    assert(tlc_core_get_migration_info(source->core,
                                           key,
                                           (uint32_t)strlen(key),
                                           key_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_SOURCE_GC);
    assert(info.target_owner == 1);
    assert(info.topology_epoch == 14);

    memset(&info, 0, sizeof(info));
    assert(tlc_core_get_migration_info(target->core,
                                           key,
                                           (uint32_t)strlen(key),
                                           key_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_DEST_COMMITTED);
    assert(info.target_owner == 1);
    assert(info.topology_epoch == 14);
    assert(vemb_v16_tlc_get_handle(target,
                                   key,
                                   (uint32_t)strlen(key),
                                   key_hash,
                                   &handle,
                                   &warm_slot) == 0);
    assert(vemb_v16_tlc_vector_slice(target,
                                     &handle,
                                     &target_value,
                                     &target_value_size) == 0);
    assert(target_value_size == sizeof(updated));
    assert(memcmp(target_value, updated, sizeof(updated)) == 0);

    for (uint32_t i = 0; i < source_storage.migration_outbox_count; i++)
        vemb_v16_migration_outbox_destroy(
            source_storage.migration_outboxes[i]);
    pthread_mutex_destroy(&source_storage.migration_outbox_lock);
    pthread_mutex_destroy(&source_storage.topology_lock);
    vemb_v16_ub_rpc_destroy(target_rpc);
    vemb_v16_ub_rpc_destroy(source_rpc);
    vemb_v16_tlc_destroy(target);
    vemb_v16_tlc_destroy(source);
    cleanup_rpc_rings(req_source_target,
                      req_target_source,
                      resp_source_target,
                      resp_target_source);
}

static void test_storage_coordinated_scaleout_waits_for_full_active(void) {
    enum { dim = 2, max_vectors = 16 };
    float source_region[dim * max_vectors];
    float target_region[dim * max_vectors];
    float initial[dim];
    float updated[dim];
    vemb_v16_warm_region_header_t source_allocator;
    vemb_v16_warm_region_header_t target_allocator;
    vemb_v16_tlc_t *source = NULL;
    vemb_v16_tlc_t *target = NULL;
    vemb_v16_ub_rpc_t *source_rpc = NULL;
    vemb_v16_ub_rpc_t *target_rpc = NULL;
    vemb_v16_tlc_warm_region_t source_warm = {
        .region_id = 945,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = source_region,
        .region_bytes = sizeof(source_region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_tlc_warm_region_t target_regions[] = {
        {
            .region_id = 945,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 0,
            .weight = 1,
            .mapped_addr = source_region,
            .region_bytes = sizeof(source_region),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
        {
            .region_id = 947,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = target_region,
            .region_bytes = sizeof(target_region),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
    };
    char req_source_target[64];
    char req_target_source[64];
    char resp_source_target[64];
    char resp_target_source[64];
    vemb_v16_ub_rpc_peer_t source_peer;
    vemb_v16_ub_rpc_peer_t target_peer;
    vemb_v16_storage_ctx_t source_storage;
    vemb_v16_topology_control_req_t topology_req = {0};
    vemb_v16_topology_control_resp_t topology_resp = {0};
    vemb_v16_topology_ring_t standby_ring;
    const uint32_t active_owners[] = {0};
    const uint32_t standby_owners[] = {0, 1};
    char key[64];
    uint64_t key_hash = 0;
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    tlc_core_key_migration_info_t info = {0};
    vemb_v16_completion_t completion = {0};
    vemb_v16_storage_scaleout_auto_status_t status;

    snprintf(req_source_target,
             sizeof(req_source_target),
             "/v16cs_%ld_q01",
             (long)getpid());
    snprintf(req_target_source,
             sizeof(req_target_source),
             "/v16cs_%ld_q10",
             (long)getpid());
    snprintf(resp_source_target,
             sizeof(resp_source_target),
             "/v16cs_%ld_s01",
             (long)getpid());
    snprintf(resp_target_source,
             sizeof(resp_target_source),
             "/v16cs_%ld_s10",
             (long)getpid());
    cleanup_rpc_rings(req_source_target,
                      req_target_source,
                      resp_source_target,
                      resp_target_source);

    memset(source_region, 0, sizeof(source_region));
    memset(target_region, 0, sizeof(target_region));
    init_test_allocator(&source_allocator, 945, max_vectors);
    init_test_allocator(&target_allocator, 947, max_vectors);
    assert(vemb_v16_tlc_create(&source,
                               dim,
                               max_vectors,
                               &source_warm,
                               1,
                               4) == 0);
    assert(vemb_v16_tlc_create(&target,
                               dim,
                               max_vectors,
                               target_regions,
                               2,
                               4) == 0);
    make_rpc_peers(&source_peer,
                   1,
                   &target_peer,
                   0,
                   req_source_target,
                   req_target_source,
                   resp_source_target,
                   resp_target_source);
    assert(vemb_v16_ub_rpc_create(&source_rpc,
                                  source,
                                  0,
                                  50,
                                  &source_peer,
                                  1) == 0);
    assert(vemb_v16_ub_rpc_create(&target_rpc,
                                  target,
                                  1,
                                  50,
                                  &target_peer,
                                  1) == 0);
    assert(vemb_v16_topology_ring_build(&standby_ring,
                                        23,
                                        standby_owners,
                                        2,
                                        VEMB_V16_TOPOLOGY_DEFAULT_VNODES) ==
           VEMB_V16_TOPOLOGY_OK);
    find_key_for_ring_owner(&standby_ring,
                            1,
                            "coordinated-scaleout",
                            key,
                            sizeof(key),
                            &key_hash);

    memset(&source_storage, 0, sizeof(source_storage));
    source_storage.local_owner_id = 0;
    source_storage.tlc = source;
    source_storage.ub_rpc = source_rpc;
    init_storage_runtime_fields(&source_storage, 0, 0);
    assert(pthread_mutex_init(&source_storage.topology_lock, NULL) == 0);
    assert(pthread_mutex_init(&source_storage.migration_outbox_lock,
                              NULL) == 0);

    fill_vector(initial, dim, 8800);
    assert(vemb_v16_tlc_put(source,
                            key,
                            (uint32_t)strlen(key),
                            key_hash,
                            initial,
                            sizeof(initial),
                            &handle,
                            &warm_slot) == 0);
    fill_topology_req(&topology_req,
                      23,
                      23,
                      VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED |
                          VEMB_V16_TOPOLOGY_CONTROL_F_AUTO_SCALEOUT |
                          VEMB_V16_TOPOLOGY_CONTROL_F_COORDINATED_SCALEOUT,
                      active_owners,
                      1,
                      standby_owners,
                      2);
    topology_req.coordinator_endpoint_valid = 1;
    topology_req.coordinator_endpoint.owner_id = UINT32_MAX;
    topology_req.coordinator_endpoint.transport_type = VEMB_V16_TRANSPORT_TCP;
    topology_req.coordinator_endpoint.tcp_port = 7399;
    snprintf(topology_req.coordinator_endpoint.host,
             sizeof(topology_req.coordinator_endpoint.host),
             "127.0.0.1");
    assert(vemb_v16_storage_topology_set(&source_storage,
                                         &topology_req) == 0);
    assert(source_storage.scaleout_auto_enabled == 1);
    assert(source_storage.scaleout_auto_coordinated == 1);
    assert(source_storage.scaleout_auto_phase ==
           VEMB_V16_STORAGE_SCALEOUT_DRAINING);

    fill_vector(updated, dim, 8900);
    run_vadd_job(&source_storage,
                 key,
                 key_hash,
                 23,
                 updated,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);

    for (uint32_t i = 0; i < 50; i++) {
        assert(vemb_v16_storage_scaleout_auto_step(&source_storage) == 0);
        if (source_storage.scaleout_auto_phase ==
            VEMB_V16_STORAGE_SCALEOUT_NOTIFY_PENDING) {
            break;
        }
        usleep(1000);
    }
    assert(source_storage.scaleout_auto_phase ==
           VEMB_V16_STORAGE_SCALEOUT_NOTIFY_PENDING);
    assert(vemb_v16_storage_scaleout_auto_get_status(&source_storage,
                                                     &status) == 0);
    assert(status.enabled == 1);
    assert(status.coordinated == 1);
    assert(status.phase == VEMB_V16_STORAGE_SCALEOUT_NOTIFY_PENDING);
    assert(status.source_owner == 0);
    assert(status.migration_epoch == 23);
    assert(status.cutover_epoch == 24);
    assert(status.notify_seq == 23);
    assert(status.coordinator_endpoint_valid == 1);
    assert(status.pending_delta == 0);
    assert(status.migrating_key_count == 0);

    vemb_v16_storage_topology_get(&source_storage, &topology_resp);
    assert(topology_resp.status == VEMB_V16_STATUS_OK);
    assert(topology_resp.current_topology_epoch == 23);
    assert(topology_resp.min_write_epoch == 23);
    assert(topology_resp.active_owner_count == 1);
    assert(topology_resp.standby_owner_count == 2);
    assert((topology_resp.flags &
            VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED) != 0);
    assert((topology_resp.flags &
            VEMB_V16_TOPOLOGY_CONTROL_F_COORDINATED_SCALEOUT) != 0);
    assert(topology_resp.coordinator_endpoint_valid == 1);

    assert(tlc_core_get_migration_info(source->core,
                                           key,
                                           (uint32_t)strlen(key),
                                           key_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_CUTOVER);
    assert(info.target_owner == 1);
    assert(info.topology_epoch == 24);

    assert(vemb_v16_storage_scaleout_auto_mark_notified(&source_storage,
                                                        23,
                                                        0,
                                                        23) == 0);
    assert(source_storage.scaleout_auto_phase ==
           VEMB_V16_STORAGE_SCALEOUT_NOTIFIED);
    assert(vemb_v16_storage_scaleout_auto_step(&source_storage) == 0);
    assert(source_storage.scaleout_auto_phase ==
           VEMB_V16_STORAGE_SCALEOUT_GLOBAL_CUTOVER_WAIT);

    fill_topology_req(&topology_req,
                      24,
                      24,
                      0,
                      standby_owners,
                      2,
                      standby_owners,
                      2);
    assert(vemb_v16_storage_topology_set(&source_storage,
                                         &topology_req) == 0);
    for (uint32_t i = 0; i < 50; i++) {
        assert(vemb_v16_storage_scaleout_auto_step(&source_storage) == 0);
        if (source_storage.scaleout_auto_phase ==
            VEMB_V16_STORAGE_SCALEOUT_DONE) {
            break;
        }
        usleep(1000);
    }
    assert(source_storage.scaleout_auto_phase ==
           VEMB_V16_STORAGE_SCALEOUT_DONE);

    vemb_v16_storage_topology_get(&source_storage, &topology_resp);
    assert(topology_resp.status == VEMB_V16_STATUS_OK);
    assert(topology_resp.current_topology_epoch == 24);
    assert(topology_resp.min_write_epoch == 24);
    assert(topology_resp.active_owner_count == 2);
    assert(topology_resp.standby_owner_count == 2);
    assert((topology_resp.flags &
            VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED) == 0);
    assert((topology_resp.flags &
            VEMB_V16_TOPOLOGY_CONTROL_F_COORDINATED_SCALEOUT) == 0);
    assert(topology_resp.coordinator_endpoint_valid == 0);

    memset(&info, 0, sizeof(info));
    assert(tlc_core_get_migration_info(source->core,
                                           key,
                                           (uint32_t)strlen(key),
                                           key_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_SOURCE_GC);
    assert(info.target_owner == 1);
    assert(info.topology_epoch == 24);
    assert(vemb_v16_storage_migration_mark_source_gc(&source_storage,
                                                      key,
                                                      (uint32_t)strlen(key),
                                                      key_hash,
                                                      24,
                                                      1,
                                                      &info) == 0);
    assert(!vemb_v16_storage_migration_active(&source_storage));

    for (uint32_t i = 0; i < source_storage.migration_outbox_count; i++)
        vemb_v16_migration_outbox_destroy(
            source_storage.migration_outboxes[i]);
    pthread_mutex_destroy(&source_storage.migration_outbox_lock);
    pthread_mutex_destroy(&source_storage.topology_lock);
    vemb_v16_ub_rpc_destroy(target_rpc);
    vemb_v16_ub_rpc_destroy(source_rpc);
    vemb_v16_tlc_destroy(target);
    vemb_v16_tlc_destroy(source);
    cleanup_rpc_rings(req_source_target,
                      req_target_source,
                      resp_source_target,
                      resp_target_source);
}

static void test_supernode_vadd_pushes_migration_delta(void) {
    enum {
        dim = 2,
        max_vectors = 256,
        large_range_key_count = 129,
        large_range_page_limit = 17,
    };
    float source_region[dim * max_vectors];
    float dest_region[dim * max_vectors];
    float baseline[dim];
    float baseline_snapshot_value[dim];
    float initial[dim];
    float updated[dim];
    float updated2[dim];
    float updated3[dim];
    float deleted_value[dim];
    float deleted_snapshot_value[dim];
    float vrem_value[dim];
    float vrem_snapshot_value[dim];
    float lease_fail_value[dim];
    float lease_fail_snapshot_value[dim];
    float range_abort_value1[dim];
    float range_abort_value2[dim];
    float range_abort_update[dim];
    float range_value1[dim];
    float range_value2[dim];
    float range_update1[dim];
    float range_update2[dim];
    float range_ask_update[dim];
    float range_snapshot_value1[dim];
    float range_snapshot_value2[dim];
    float range_tcp_value1[dim];
    float range_tcp_value2[dim];
    float range_tcp_update1[dim];
    float range_tcp_update2[dim];
    float range_tcp_snapshot_value1[dim];
    float range_tcp_snapshot_value2[dim];
    float live_range_value1[dim];
    float live_range_value2[dim];
    float live_range_update1[dim];
    float live_range_redirect_update[dim];
    float live_range_snapshot_value1[dim];
    float live_range_snapshot_value2[dim];
    float large_range_value[dim];
    float large_range_snapshot_value[dim];
    char large_range_keys[large_range_key_count][64];
    uint32_t large_range_key_lens[large_range_key_count];
    uint64_t large_range_key_hashes[large_range_key_count];
    vemb_v16_warm_region_header_t source_allocator;
    vemb_v16_warm_region_header_t dest_allocator;
    vemb_v16_tlc_t *source = NULL;
    vemb_v16_tlc_t *dest = NULL;
    vemb_v16_ub_rpc_t *source_rpc = NULL;
    vemb_v16_ub_rpc_t *dest_rpc = NULL;
    vemb_v16_proxy_t *source_proxy = NULL;
    vemb_v16_tlc_warm_region_t source_warm = {
        .region_id = 951,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = source_region,
        .region_bytes = sizeof(source_region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_tlc_warm_region_t dest_regions[] = {
        {
            .region_id = 951,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 0,
            .weight = 1,
            .mapped_addr = source_region,
            .region_bytes = sizeof(source_region),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
        {
            .region_id = 953,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = dest_region,
            .region_bytes = sizeof(dest_region),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
    };
    char req_source_dest[64];
    char req_dest_source[64];
    char resp_source_dest[64];
    char resp_dest_source[64];
    vemb_v16_ub_rpc_peer_t source_peer;
    vemb_v16_ub_rpc_peer_t dest_peer;
    const char *key = "supernode:migration-delta";
    uint32_t key_len = (uint32_t)strlen(key);
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, key_len);
    const char *baseline_key = "supernode:migration-baseline-only";
    uint32_t baseline_key_len = (uint32_t)strlen(baseline_key);
    uint64_t baseline_key_hash =
        vemb_v16_xxh3_64_str(baseline_key, baseline_key_len);
    const char *lease_fail_key = "supernode:migration-lease-fail";
    uint32_t lease_fail_key_len = (uint32_t)strlen(lease_fail_key);
    uint64_t lease_fail_key_hash =
        vemb_v16_xxh3_64_str(lease_fail_key, lease_fail_key_len);
    const char *range_abort_key1 = "supernode:migration-range-abort-1";
    const char *range_abort_key2 = "supernode:migration-range-abort-2";
    uint32_t range_abort_key1_len = (uint32_t)strlen(range_abort_key1);
    uint32_t range_abort_key2_len = (uint32_t)strlen(range_abort_key2);
    uint64_t range_abort_key1_hash =
        vemb_v16_xxh3_64_str(range_abort_key1, range_abort_key1_len);
    uint64_t range_abort_key2_hash =
        vemb_v16_xxh3_64_str(range_abort_key2, range_abort_key2_len);
    const char *range_key1 = "supernode:migration-range-1";
    const char *range_key2 = "supernode:migration-range-2";
    uint32_t range_key1_len = (uint32_t)strlen(range_key1);
    uint32_t range_key2_len = (uint32_t)strlen(range_key2);
    uint64_t range_key1_hash = vemb_v16_xxh3_64_str(range_key1,
                                                range_key1_len);
    uint64_t range_key2_hash = vemb_v16_xxh3_64_str(range_key2,
                                                range_key2_len);
    const char *range_tcp_key1 = "supernode:migration-range-tcp-1";
    const char *range_tcp_key2 = "supernode:migration-range-tcp-2";
    uint32_t range_tcp_key1_len = (uint32_t)strlen(range_tcp_key1);
    uint32_t range_tcp_key2_len = (uint32_t)strlen(range_tcp_key2);
    uint64_t range_tcp_key1_hash =
        vemb_v16_xxh3_64_str(range_tcp_key1, range_tcp_key1_len);
    uint64_t range_tcp_key2_hash =
        vemb_v16_xxh3_64_str(range_tcp_key2, range_tcp_key2_len);
    const char *live_range_key1 = "supernode:migration-live-range-1";
    const char *live_range_key2 = "supernode:migration-live-range-2";
    uint32_t live_range_key1_len = (uint32_t)strlen(live_range_key1);
    uint32_t live_range_key2_len = (uint32_t)strlen(live_range_key2);
    uint64_t live_range_key1_hash =
        vemb_v16_xxh3_64_str(live_range_key1, live_range_key1_len);
    uint64_t live_range_key2_hash =
        vemb_v16_xxh3_64_str(live_range_key2, live_range_key2_len);
    const char *deleted_key = "supernode:migration-delete";
    uint32_t deleted_key_len = (uint32_t)strlen(deleted_key);
    uint64_t deleted_key_hash =
        vemb_v16_xxh3_64_str(deleted_key, deleted_key_len);
    const char *vrem_key = "supernode:migration-vrem";
    uint32_t vrem_key_len = (uint32_t)strlen(vrem_key);
    uint64_t vrem_key_hash =
        vemb_v16_xxh3_64_str(vrem_key, vrem_key_len);
    vemb_v16_storage_ctx_t source_storage;
    vemb_v16_storage_ctx_t dest_storage;
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    tlc_core_key_migration_info_t info = {0};
    tlc_core_migration_snapshot_t baseline_snapshot = {0};
    tlc_core_migration_snapshot_t lease_fail_snapshot = {0};
    tlc_core_migration_snapshot_t range_snapshot1 = {0};
    tlc_core_migration_snapshot_t range_snapshot2 = {0};
    tlc_core_migration_snapshot_t range_tcp_snapshot1 = {0};
    tlc_core_migration_snapshot_t range_tcp_snapshot2 = {0};
    tlc_core_migration_snapshot_t live_range_snapshot1 = {0};
    tlc_core_migration_snapshot_t live_range_snapshot2 = {0};
    tlc_core_migration_snapshot_t deleted_snapshot = {0};
    tlc_core_migration_snapshot_t vrem_snapshot = {0};
    tlc_core_migration_apply_status_t apply_status =
        TLC_CORE_MIGRATION_ERROR;
    vemb_v16_completion_t completion = {0};
    const uint8_t *stored = NULL;
    uint32_t stored_len = 0;
    vemb_v16_migration_outbox_stats_t outbox_stats = {0};
    uint32_t sent_count = 0;
    uint32_t acked_count = 0;
    vemb_v16_topology_control_req_t topology_req = {0};
    vemb_v16_migration_control_req_t control_req = {0};
    vemb_v16_migration_control_resp_t control_resp = {0};
    vemb_v16_migration_range_control_req_t range_req = {0};
    vemb_v16_migration_range_control_resp_t range_resp = {0};
    const uint32_t dual_active_owners[] = {1};
    const uint32_t full_active_owners[] = {1, 3};
    const uint32_t range_abort_shard_id = 5;
    const uint32_t range_shard_id = 7;
    const uint32_t range_tcp_shard_id = 9;
    const uint32_t live_range_shard_id = 13;
    const uint32_t large_range_shard_id = 11;

    snprintf(req_source_dest, sizeof(req_source_dest),
             "/v16ctl_delta_%ld_req_1_3", (long)getpid());
    snprintf(req_dest_source, sizeof(req_dest_source),
             "/v16ctl_delta_%ld_req_3_1", (long)getpid());
    snprintf(resp_source_dest, sizeof(resp_source_dest),
             "/v16ctl_delta_%ld_resp_1_3", (long)getpid());
    snprintf(resp_dest_source, sizeof(resp_dest_source),
             "/v16ctl_delta_%ld_resp_3_1", (long)getpid());
    cleanup_rpc_rings(req_source_dest,
                      req_dest_source,
                      resp_source_dest,
                      resp_dest_source);

    memset(source_region, 0, sizeof(source_region));
    memset(dest_region, 0, sizeof(dest_region));
    init_test_allocator(&source_allocator, 951, max_vectors);
    init_test_allocator(&dest_allocator, 953, max_vectors);
    assert(vemb_v16_tlc_create(&source,
                               dim,
                               max_vectors,
                               &source_warm,
                               1,
                               4) == 0);
    assert(vemb_v16_tlc_create(&dest,
                               dim,
                               max_vectors,
                               dest_regions,
                               2,
                               4) == 0);
    make_rpc_peers(&source_peer,
                   3,
                   &dest_peer,
                   1,
                   req_source_dest,
                   req_dest_source,
                   resp_source_dest,
                   resp_dest_source);
    assert(vemb_v16_ub_rpc_create(&source_rpc,
                                  source,
                                  1,
                                  100,
                                  &source_peer,
                                  1) == 0);
    assert(vemb_v16_ub_rpc_create(&dest_rpc,
                                  dest,
                                  3,
                                  100,
                                  &dest_peer,
                                  1) == 0);

    memset(&source_storage, 0, sizeof(source_storage));
    source_storage.local_owner_id = 1;
    source_storage.tlc = source;
    source_storage.ub_rpc = source_rpc;
    init_storage_runtime_fields(&source_storage, 60, 60);
    assert(pthread_mutex_init(&source_storage.topology_lock, NULL) == 0);
    assert(pthread_mutex_init(&source_storage.migration_outbox_lock,
                              NULL) == 0);
    memset(&dest_storage, 0, sizeof(dest_storage));
    dest_storage.local_owner_id = 3;
    dest_storage.tlc = dest;
    dest_storage.ub_rpc = dest_rpc;
    init_storage_runtime_fields(&dest_storage, 60, 60);
    assert(pthread_mutex_init(&dest_storage.topology_lock, NULL) == 0);
    assert(pthread_mutex_init(&dest_storage.migration_outbox_lock,
                              NULL) == 0);
    assert(vemb_v16_proxy_create(&source_proxy,
                                 dim,
                                 max_vectors,
                                 &source_storage,
                                 NULL) == 0);

    fill_topology_req(&topology_req,
                      60,
                      50,
                      VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED,
                      dual_active_owners,
                      1,
                      full_active_owners,
                      2);
    assert(vemb_v16_storage_topology_set(&source_storage,
                                         &topology_req) == 0);

    fill_vector(baseline, dim, 6000);
    assert(vemb_v16_tlc_put_with_epoch(source,
                                       baseline_key,
                                       baseline_key_len,
                                       baseline_key_hash,
                                       baseline,
                                       sizeof(baseline),
                                       50,
                                       &handle,
                                       &warm_slot) == 0);
    assert(tlc_core_mark_migrating_in_shard(source->core,
                                            baseline_key,
                                            baseline_key_len,
                                            baseline_key_hash,
                                            50,
                                            3,
                                            0,
                                            &info) == 0);
    fill_topology_req(&topology_req,
                      60,
                      50,
                      0,
                      full_active_owners,
                      2,
                      full_active_owners,
                      2);
    assert(vemb_v16_storage_topology_set(&source_storage,
                                         &topology_req) != 0);
    assert(tlc_core_snapshot(source->core,
                                 baseline_key,
                                 baseline_key_len,
                                 baseline_key_hash,
                                 1,
                                 3,
                                 &baseline_snapshot,
                                 baseline_snapshot_value,
                                 sizeof(baseline_snapshot_value)) == 0);
    assert(memcmp(baseline_snapshot_value,
                  baseline,
                  sizeof(baseline)) == 0);
    assert(vemb_v16_tlc_apply_migration(dest,
                                        &baseline_snapshot,
                                        baseline_snapshot_value,
                                        sizeof(baseline_snapshot_value),
                                        &apply_status,
                                        &handle) == 0);
    assert(apply_status == TLC_CORE_MIGRATION_APPLIED);
    memset(&info, 0, sizeof(info));
    memset(&outbox_stats, 0, sizeof(outbox_stats));
    assert(vemb_v16_storage_migration_barrier(&source_storage,
                                              baseline_key,
                                              baseline_key_len,
                                              baseline_key_hash,
                                              50,
                                              3,
                                              &info,
                                              &outbox_stats) == 0);
    assert(outbox_stats.barrier_seq == 0);
    assert(outbox_stats.state == VEMB_V16_MIGRATION_OUTBOX_OPEN);
    memset(&info, 0, sizeof(info));
    assert(vemb_v16_storage_migration_mark_cutover(&source_storage,
                                                   baseline_key,
                                                   baseline_key_len,
                                                   baseline_key_hash,
                                                   51,
                                                   3,
                                                   &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_CUTOVER);
    assert(info.owner_epoch == 51);
    memset(&info, 0, sizeof(info));
    assert(tlc_core_get_migration_info(dest->core,
                                           baseline_key,
                                           baseline_key_len,
                                           baseline_key_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_DEST_COMMITTED);
    assert(info.owner_epoch == 51);
    assert(vemb_v16_storage_topology_set(&source_storage,
                                         &topology_req) == 0);
    assert(vemb_v16_tlc_get_handle(source,
                                   baseline_key,
                                   baseline_key_len,
                                   baseline_key_hash,
                                   &handle,
                                   &warm_slot) != 0);
    assert(vemb_v16_tlc_get_handle(dest,
                                   baseline_key,
                                   baseline_key_len,
                                   baseline_key_hash,
                                   &handle,
                                   &warm_slot) == 0);
    assert(handle.region_id == 953);
    assert(vemb_v16_tlc_vector_slice(dest,
                                     &handle,
                                     &stored,
                                     &stored_len) == 0);
    assert(stored_len == sizeof(baseline));
    assert(memcmp(stored, baseline, sizeof(baseline)) == 0);
    pthread_mutex_lock(&source_storage.migration_outbox_lock);
    assert(source_storage.migration_outbox_count == 1);
    vemb_v16_migration_outbox_destroy(source_storage.migration_outboxes[0]);
    source_storage.migration_outboxes[0] = NULL;
    source_storage.migration_outbox_count = 0;
    pthread_mutex_unlock(&source_storage.migration_outbox_lock);

    fill_vector(lease_fail_value, dim, 6050);
    warm_slot = UINT32_MAX;
    assert(vemb_v16_tlc_put_with_epoch(source,
                                       lease_fail_key,
                                       lease_fail_key_len,
                                       lease_fail_key_hash,
                                       lease_fail_value,
                                       sizeof(lease_fail_value),
                                       55,
                                       &handle,
                                       &warm_slot) == 0);
    assert(tlc_core_mark_migrating_in_shard(source->core,
                                            lease_fail_key,
                                            lease_fail_key_len,
                                            lease_fail_key_hash,
                                            55,
                                            3,
                                            0,
                                            &info) == 0);
    assert(tlc_core_snapshot(source->core,
                                 lease_fail_key,
                                 lease_fail_key_len,
                                 lease_fail_key_hash,
                                 1,
                                 3,
                                 &lease_fail_snapshot,
                                 lease_fail_snapshot_value,
                                 sizeof(lease_fail_snapshot_value)) == 0);
    assert(vemb_v16_tlc_apply_migration(dest,
                                        &lease_fail_snapshot,
                                        lease_fail_snapshot_value,
                                        sizeof(lease_fail_snapshot_value),
                                        &apply_status,
                                        &handle) == 0);
    assert(apply_status == TLC_CORE_MIGRATION_APPLIED);
    assert(tlc_core_accept_owner_lease(dest->core,
                                           lease_fail_key,
                                           lease_fail_key_len,
                                           lease_fail_key_hash,
                                           90,
                                           90,
                                           3,
                                           &info) == 0);
    assert(info.owner_epoch == 90);
    memset(&info, 0, sizeof(info));
    memset(&outbox_stats, 0, sizeof(outbox_stats));
    assert(vemb_v16_storage_migration_barrier(&source_storage,
                                              lease_fail_key,
                                              lease_fail_key_len,
                                              lease_fail_key_hash,
                                              55,
                                              3,
                                              &info,
                                              &outbox_stats) == 0);
    assert(outbox_stats.state == VEMB_V16_MIGRATION_OUTBOX_OPEN);
    assert(vemb_v16_storage_migration_mark_cutover(&source_storage,
                                                   lease_fail_key,
                                                   lease_fail_key_len,
                                                   lease_fail_key_hash,
                                                   56,
                                                   3,
                                                   &info) != 0);
    assert(tlc_core_get_migration_info(source->core,
                                           lease_fail_key,
                                           lease_fail_key_len,
                                           lease_fail_key_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_MIGRATING);
    assert(info.owner_epoch == 0);
    pthread_mutex_lock(&source_storage.migration_outbox_lock);
    for (uint32_t i = 0; i < source_storage.migration_outbox_count; i++) {
        vemb_v16_migration_outbox_destroy(
            source_storage.migration_outboxes[i]);
        source_storage.migration_outboxes[i] = NULL;
    }
    source_storage.migration_outbox_count = 0;
    pthread_mutex_unlock(&source_storage.migration_outbox_lock);

    fill_vector(range_abort_value1, dim, 6052);
    fill_vector(range_abort_value2, dim, 6054);
    warm_slot = UINT32_MAX;
    assert(vemb_v16_tlc_put_with_epoch(source,
                                       range_abort_key1,
                                       range_abort_key1_len,
                                       range_abort_key1_hash,
                                       range_abort_value1,
                                       sizeof(range_abort_value1),
                                       70,
                                       &handle,
                                       &warm_slot) == 0);
    warm_slot = UINT32_MAX;
    assert(vemb_v16_tlc_put_with_epoch(source,
                                       range_abort_key2,
                                       range_abort_key2_len,
                                       range_abort_key2_hash,
                                       range_abort_value2,
                                       sizeof(range_abort_value2),
                                       70,
                                       &handle,
                                       &warm_slot) == 0);
    assert(vemb_v16_storage_migration_mark_migrating_in_shard(
               &source_storage,
               range_abort_key1,
               range_abort_key1_len,
               range_abort_key1_hash,
               70,
               3,
               range_abort_shard_id,
               &info) == 0);
    assert(vemb_v16_storage_migration_mark_migrating_in_shard(
               &source_storage,
               range_abort_key2,
               range_abort_key2_len,
               range_abort_key2_hash,
               70,
               3,
               range_abort_shard_id,
               &info) == 0);
    assert(vemb_v16_storage_migration_range_barrier(
               &source_storage,
               70,
               3,
               range_abort_shard_id,
               0,
               &range_resp) == 0);
    assert(range_resp.status == VEMB_V16_STATUS_OK);
    assert(range_resp.outbox_state == VEMB_V16_MIGRATION_OUTBOX_OPEN);
    assert(vemb_v16_storage_migration_range_mark_cutover(
               &source_storage,
               70,
               71,
               3,
               range_abort_shard_id,
               0,
               &range_resp) != 0);
    assert(range_resp.status == VEMB_V16_STATUS_ERR);
    assert(range_resp.success_count == 0);
    assert(range_resp.error_count == 2);
    assert(range_resp.remaining_keys == 2);
    assert(range_resp.outbox_state == VEMB_V16_MIGRATION_OUTBOX_OPEN);
    fill_vector(range_abort_update, dim, 6056);
    memset(&completion, 0, sizeof(completion));
    run_vadd_job(&source_storage,
                 range_abort_key1,
                 range_abort_key1_hash,
                 70,
                 range_abort_update,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);
    pthread_mutex_lock(&source_storage.migration_outbox_lock);
    for (uint32_t i = 0; i < source_storage.migration_outbox_count; i++) {
        vemb_v16_migration_outbox_destroy(
            source_storage.migration_outboxes[i]);
        source_storage.migration_outboxes[i] = NULL;
    }
    source_storage.migration_outbox_count = 0;
    pthread_mutex_unlock(&source_storage.migration_outbox_lock);

    fill_vector(range_value1, dim, 6060);
    fill_vector(range_value2, dim, 6070);
    warm_slot = UINT32_MAX;
    assert(vemb_v16_tlc_put_with_epoch(source,
                                       range_key1,
                                       range_key1_len,
                                       range_key1_hash,
                                       range_value1,
                                       sizeof(range_value1),
                                       80,
                                       &handle,
                                       &warm_slot) == 0);
    warm_slot = UINT32_MAX;
    assert(vemb_v16_tlc_put_with_epoch(source,
                                       range_key2,
                                       range_key2_len,
                                       range_key2_hash,
                                       range_value2,
                                       sizeof(range_value2),
                                       80,
                                       &handle,
                                       &warm_slot) == 0);
    assert(vemb_v16_storage_migration_mark_migrating_in_shard(
               &source_storage,
               range_key1,
               range_key1_len,
               range_key1_hash,
               80,
               3,
               range_shard_id,
               &info) == 0);
    assert(info.shard_id == range_shard_id);
    assert(vemb_v16_storage_migration_mark_migrating_in_shard(
               &source_storage,
               range_key2,
               range_key2_len,
               range_key2_hash,
               80,
               3,
               range_shard_id,
               &info) == 0);
    assert(info.shard_id == range_shard_id);
    assert(tlc_core_snapshot(source->core,
                                 range_key1,
                                 range_key1_len,
                                 range_key1_hash,
                                 1,
                                 3,
                                 &range_snapshot1,
                                 range_snapshot_value1,
                                 sizeof(range_snapshot_value1)) == 0);
    assert(range_snapshot1.shard_id == range_shard_id);
    assert(tlc_core_snapshot(source->core,
                                 range_key2,
                                 range_key2_len,
                                 range_key2_hash,
                                 1,
                                 3,
                                 &range_snapshot2,
                                 range_snapshot_value2,
                                 sizeof(range_snapshot_value2)) == 0);
    assert(range_snapshot2.shard_id == range_shard_id);
    assert(vemb_v16_tlc_apply_migration(dest,
                                        &range_snapshot1,
                                        range_snapshot_value1,
                                        sizeof(range_snapshot_value1),
                                        &apply_status,
                                        &handle) == 0);
    assert(apply_status == TLC_CORE_MIGRATION_APPLIED);
    assert(vemb_v16_tlc_apply_migration(dest,
                                        &range_snapshot2,
                                        range_snapshot_value2,
                                        sizeof(range_snapshot_value2),
                                        &apply_status,
                                        &handle) == 0);
    assert(apply_status == TLC_CORE_MIGRATION_APPLIED);

    fill_vector(range_update1, dim, 6080);
    memset(&completion, 0, sizeof(completion));
    run_vadd_job(&source_storage,
                 range_key1,
                 range_key1_hash,
                 80,
                 range_update1,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);
    fill_vector(range_update2, dim, 6090);
    memset(&completion, 0, sizeof(completion));
    run_vadd_job(&source_storage,
                 range_key2,
                 range_key2_hash,
                 80,
                 range_update2,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);
    assert(source_storage.migration_outbox_count == 1);
    vemb_v16_migration_outbox_get_stats(
        source_storage.migration_outboxes[0],
        &outbox_stats);
    assert(outbox_stats.shard_id == range_shard_id);
    assert(outbox_stats.acked_seq == 2);
    assert(outbox_stats.pending_count == 0);

    fill_range_control_req(&range_req, 80, 0, 3, range_shard_id);
    memset(&range_resp, 0, sizeof(range_resp));
    assert(tcp_migration_range_control(
               source_proxy,
               VEMB_V16_NET_MIGRATION_RANGE_BARRIER,
               &range_req,
               &range_resp) == 0);
    assert(range_resp.status == VEMB_V16_STATUS_OK);
    assert(range_resp.migration_topology_epoch == 80);
    assert(range_resp.target_owner == 3);
    assert(range_resp.shard_id == range_shard_id);
    assert(range_resp.key_count == 2);
    assert(range_resp.success_count == 2);
    assert(range_resp.barrier_seq == 2);
    assert(range_resp.source_seq == 2);
    assert(range_resp.retry_delta == 0);
    assert(range_resp.outbox_state == VEMB_V16_MIGRATION_OUTBOX_OPEN);

    memset(&completion, 0, sizeof(completion));
    run_vadd_job(&source_storage,
                 range_key1,
                 range_key1_hash,
                 80,
                 range_update1,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);

    fill_vector(range_ask_update, dim, 6100);
    memset(&completion, 0, sizeof(completion));
    run_vadd_job_with_flags(&dest_storage,
                            range_key1,
                            range_key1_hash,
                            80,
                            range_ask_update,
                            dim,
                            VEMB_V16_REQ_F_ASK_REDIRECT,
                            &completion);
    assert(completion.status == VEMB_V16_STATUS_ASK);
    assert(completion.redirect_owner == 3);
    memset(&completion, 0, sizeof(completion));
    run_vrem_job_with_flags(&dest_storage,
                            range_key2,
                            range_key2_hash,
                            80,
                            VEMB_V16_REQ_F_ASK_REDIRECT,
                            &completion);
    assert(completion.status == VEMB_V16_STATUS_ASK);
    assert(completion.redirect_owner == 3);

    fill_range_control_req(&range_req, 80, 81, 3, range_shard_id);
    memset(&range_resp, 0, sizeof(range_resp));
    assert(tcp_migration_range_control(
               source_proxy,
               VEMB_V16_NET_MIGRATION_RANGE_MARK_CUTOVER,
               &range_req,
               &range_resp) == 0);
    assert(range_resp.status == VEMB_V16_STATUS_OK);
    assert(range_resp.migration_topology_epoch == 80);
    assert(range_resp.cutover_topology_epoch == 81);
    assert(range_resp.target_owner == 3);
    assert(range_resp.shard_id == range_shard_id);
    assert(range_resp.key_count == 2);
    assert(range_resp.success_count == 2);
    assert(range_resp.error_count == 0);
    assert(range_resp.owner_epoch == 81);
    assert(vemb_v16_tlc_get_handle(source,
                                   range_key1,
                                   range_key1_len,
                                   range_key1_hash,
                                   &handle,
                                   &warm_slot) != 0);
    assert(vemb_v16_tlc_get_handle(source,
                                   range_key2,
                                   range_key2_len,
                                   range_key2_hash,
                                   &handle,
                                   &warm_slot) != 0);
    assert(tlc_core_get_migration_info(source->core,
                                           range_key1,
                                           range_key1_len,
                                           range_key1_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_CUTOVER);
    assert(info.owner_epoch == 81);
    assert(info.shard_id == range_shard_id);
    assert(tlc_core_get_migration_info(dest->core,
                                           range_key1,
                                           range_key1_len,
                                           range_key1_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_DEST_COMMITTED);
    assert(info.owner_epoch == 81);
    assert(info.shard_id == range_shard_id);
    assert(tlc_core_get_migration_info(dest->core,
                                           range_key2,
                                           range_key2_len,
                                           range_key2_hash,
                                           &info) == 0);
    assert(info.owner_epoch == 81);
    assert(info.shard_id == range_shard_id);

    memset(&completion, 0, sizeof(completion));
    run_vadd_job_with_flags(&dest_storage,
                            range_key1,
                            range_key1_hash,
                            80,
                            range_ask_update,
                            dim,
                            VEMB_V16_REQ_F_ASK_REDIRECT,
                            &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);
    warm_slot = UINT32_MAX;
    assert(vemb_v16_tlc_get_handle(dest,
                                   range_key1,
                                   range_key1_len,
                                   range_key1_hash,
                                   &handle,
                                   &warm_slot) == 0);
    assert(vemb_v16_tlc_vector_slice(dest,
                                     &handle,
                                     &stored,
                                     &stored_len) == 0);
    assert(stored_len == sizeof(range_ask_update));
    assert(memcmp(stored,
                  range_ask_update,
                  sizeof(range_ask_update)) == 0);
    assert(tlc_core_get_migration_info(dest->core,
                                           range_key1,
                                           range_key1_len,
                                           range_key1_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_DEST_COMMITTED);
    assert(info.owner_epoch == 81);
    assert(info.shard_id == range_shard_id);

    memset(&completion, 0, sizeof(completion));
    run_vrem_job_with_flags(&dest_storage,
                            range_key2,
                            range_key2_hash,
                            80,
                            VEMB_V16_REQ_F_ASK_REDIRECT,
                            &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);
    assert(vemb_v16_tlc_get_handle(dest,
                                   range_key2,
                                   range_key2_len,
                                   range_key2_hash,
                                   &handle,
                                   &warm_slot) != 0);
    assert(tlc_core_get_migration_info(dest->core,
                                           range_key2,
                                           range_key2_len,
                                           range_key2_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_DEST_COMMITTED);
    assert(info.tombstone == 1);
    assert(info.owner_epoch == 81);
    assert(info.shard_id == range_shard_id);

    fill_range_control_req(&range_req, 80, 81, 3, range_shard_id);
    memset(&range_resp, 0, sizeof(range_resp));
    assert(tcp_migration_range_control(
               source_proxy,
               VEMB_V16_NET_MIGRATION_RANGE_SOURCE_GC,
               &range_req,
               &range_resp) == 0);
    assert(range_resp.status == VEMB_V16_STATUS_OK);
    assert(range_resp.key_count == 2);
    assert(range_resp.success_count == 2);
    assert(range_resp.source_seq == 3);
    assert(range_resp.retry_delta == 0);
    assert(tlc_core_get_migration_info(source->core,
                                           range_key1,
                                           range_key1_len,
                                           range_key1_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_SOURCE_GC);
    assert(info.owner_epoch == 81);
    assert(tlc_core_get_migration_info(source->core,
                                           range_key2,
                                           range_key2_len,
                                           range_key2_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_SOURCE_GC);
    memset(&completion, 0, sizeof(completion));
    run_vadd_job(&source_storage,
                 range_key1,
                 range_key1_hash,
                 81,
                 range_update1,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_MOVED);
    assert(completion.redirect_owner == 3);

    pthread_mutex_lock(&source_storage.migration_outbox_lock);
    for (uint32_t i = 0; i < source_storage.migration_outbox_count; i++) {
        vemb_v16_migration_outbox_destroy(
            source_storage.migration_outboxes[i]);
        source_storage.migration_outboxes[i] = NULL;
    }
    source_storage.migration_outbox_count = 0;
    pthread_mutex_unlock(&source_storage.migration_outbox_lock);

    fill_vector(range_tcp_value1, dim, 6110);
    fill_vector(range_tcp_value2, dim, 6120);
    warm_slot = UINT32_MAX;
    assert(vemb_v16_tlc_put_with_epoch(source,
                                       range_tcp_key1,
                                       range_tcp_key1_len,
                                       range_tcp_key1_hash,
                                       range_tcp_value1,
                                       sizeof(range_tcp_value1),
                                       82,
                                       &handle,
                                       &warm_slot) == 0);
    warm_slot = UINT32_MAX;
    assert(vemb_v16_tlc_put_with_epoch(source,
                                       range_tcp_key2,
                                       range_tcp_key2_len,
                                       range_tcp_key2_hash,
                                       range_tcp_value2,
                                       sizeof(range_tcp_value2),
                                       82,
                                       &handle,
                                       &warm_slot) == 0);
    assert(vemb_v16_storage_migration_mark_migrating_in_shard(
               &source_storage,
               range_tcp_key1,
               range_tcp_key1_len,
               range_tcp_key1_hash,
               82,
               3,
               range_tcp_shard_id,
               &info) == 0);
    assert(info.shard_id == range_tcp_shard_id);
    assert(vemb_v16_storage_migration_mark_migrating_in_shard(
               &source_storage,
               range_tcp_key2,
               range_tcp_key2_len,
               range_tcp_key2_hash,
               82,
               3,
               range_tcp_shard_id,
               &info) == 0);
    assert(info.shard_id == range_tcp_shard_id);
    assert(tlc_core_snapshot(source->core,
                                 range_tcp_key1,
                                 range_tcp_key1_len,
                                 range_tcp_key1_hash,
                                 1,
                                 3,
                                 &range_tcp_snapshot1,
                                 range_tcp_snapshot_value1,
                                 sizeof(range_tcp_snapshot_value1)) == 0);
    assert(range_tcp_snapshot1.shard_id == range_tcp_shard_id);
    assert(tlc_core_snapshot(source->core,
                                 range_tcp_key2,
                                 range_tcp_key2_len,
                                 range_tcp_key2_hash,
                                 1,
                                 3,
                                 &range_tcp_snapshot2,
                                 range_tcp_snapshot_value2,
                                 sizeof(range_tcp_snapshot_value2)) == 0);
    assert(range_tcp_snapshot2.shard_id == range_tcp_shard_id);
    assert(vemb_v16_tlc_apply_migration(dest,
                                        &range_tcp_snapshot1,
                                        range_tcp_snapshot_value1,
                                        sizeof(range_tcp_snapshot_value1),
                                        &apply_status,
                                        &handle) == 0);
    assert(apply_status == TLC_CORE_MIGRATION_APPLIED);
    assert(vemb_v16_tlc_apply_migration(dest,
                                        &range_tcp_snapshot2,
                                        range_tcp_snapshot_value2,
                                        sizeof(range_tcp_snapshot_value2),
                                        &apply_status,
                                        &handle) == 0);
    assert(apply_status == TLC_CORE_MIGRATION_APPLIED);

    fill_vector(range_tcp_update1, dim, 6130);
    memset(&completion, 0, sizeof(completion));
    run_vadd_job(&source_storage,
                 range_tcp_key1,
                 range_tcp_key1_hash,
                 82,
                 range_tcp_update1,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);
    fill_vector(range_tcp_update2, dim, 6140);
    memset(&completion, 0, sizeof(completion));
    run_vadd_job(&source_storage,
                 range_tcp_key2,
                 range_tcp_key2_hash,
                 82,
                 range_tcp_update2,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);
    assert(source_storage.migration_outbox_count == 1);
    vemb_v16_migration_outbox_get_stats(
        source_storage.migration_outboxes[0],
        &outbox_stats);
    assert(outbox_stats.shard_id == range_tcp_shard_id);
    assert(outbox_stats.acked_seq == 2);
    assert(outbox_stats.pending_count == 0);

    fill_range_control_req(&range_req, 82, 0, 3, range_tcp_shard_id);
    memset(&range_resp, 0, sizeof(range_resp));
    assert(tcp_migration_range_control(
               source_proxy,
               VEMB_V16_NET_MIGRATION_RANGE_BARRIER,
               &range_req,
               &range_resp) == 0);
    assert(range_resp.status == VEMB_V16_STATUS_OK);
    assert(range_resp.migration_topology_epoch == 82);
    assert(range_resp.target_owner == 3);
    assert(range_resp.shard_id == range_tcp_shard_id);
    assert(range_resp.key_count == 2);
    assert(range_resp.success_count == 2);
    assert(range_resp.barrier_seq == 2);
    assert(range_resp.source_seq == 2);
    assert(range_resp.retry_delta == 0);
    assert(range_resp.outbox_state == VEMB_V16_MIGRATION_OUTBOX_OPEN);

    memset(&completion, 0, sizeof(completion));
    run_vadd_job(&source_storage,
                 range_tcp_key2,
                 range_tcp_key2_hash,
                 82,
                 range_tcp_update2,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);

    fill_range_control_req(&range_req, 82, 83, 3, range_tcp_shard_id);
    memset(&range_resp, 0, sizeof(range_resp));
    assert(tcp_migration_range_control(
               source_proxy,
               VEMB_V16_NET_MIGRATION_RANGE_MARK_CUTOVER,
               &range_req,
               &range_resp) == 0);
    assert(range_resp.status == VEMB_V16_STATUS_OK);
    assert(range_resp.migration_topology_epoch == 82);
    assert(range_resp.cutover_topology_epoch == 83);
    assert(range_resp.target_owner == 3);
    assert(range_resp.shard_id == range_tcp_shard_id);
    assert(range_resp.key_count == 2);
    assert(range_resp.success_count == 2);
    assert(range_resp.error_count == 0);
    assert(range_resp.owner_epoch == 83);
    assert(vemb_v16_tlc_get_handle(source,
                                   range_tcp_key1,
                                   range_tcp_key1_len,
                                   range_tcp_key1_hash,
                                   &handle,
                                   &warm_slot) != 0);
    assert(vemb_v16_tlc_get_handle(source,
                                   range_tcp_key2,
                                   range_tcp_key2_len,
                                   range_tcp_key2_hash,
                                   &handle,
                                   &warm_slot) != 0);
    assert(tlc_core_get_migration_info(source->core,
                                           range_tcp_key1,
                                           range_tcp_key1_len,
                                           range_tcp_key1_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_CUTOVER);
    assert(info.owner_epoch == 83);
    assert(info.shard_id == range_tcp_shard_id);
    assert(tlc_core_get_migration_info(dest->core,
                                           range_tcp_key1,
                                           range_tcp_key1_len,
                                           range_tcp_key1_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_DEST_COMMITTED);
    assert(info.owner_epoch == 83);
    assert(info.shard_id == range_tcp_shard_id);
    assert(tlc_core_get_migration_info(dest->core,
                                           range_tcp_key2,
                                           range_tcp_key2_len,
                                           range_tcp_key2_hash,
                                           &info) == 0);
    assert(info.owner_epoch == 83);
    assert(info.shard_id == range_tcp_shard_id);

    fill_range_control_req(&range_req, 82, 83, 3, range_tcp_shard_id);
    memset(&range_resp, 0, sizeof(range_resp));
    assert(tcp_migration_range_control(
               source_proxy,
               VEMB_V16_NET_MIGRATION_RANGE_SOURCE_GC,
               &range_req,
               &range_resp) == 0);
    assert(range_resp.status == VEMB_V16_STATUS_OK);
    assert(range_resp.key_count == 2);
    assert(range_resp.success_count == 2);
    assert(range_resp.source_seq == 3);
    assert(range_resp.retry_delta == 0);
    assert(tlc_core_get_migration_info(source->core,
                                           range_tcp_key1,
                                           range_tcp_key1_len,
                                           range_tcp_key1_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_SOURCE_GC);
    assert(info.owner_epoch == 83);
    assert(tlc_core_get_migration_info(source->core,
                                           range_tcp_key2,
                                           range_tcp_key2_len,
                                           range_tcp_key2_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_SOURCE_GC);
    memset(&completion, 0, sizeof(completion));
    run_vadd_job(&source_storage,
                 range_tcp_key2,
                 range_tcp_key2_hash,
                 83,
                 range_tcp_update2,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_MOVED);
    assert(completion.redirect_owner == 3);

    pthread_mutex_lock(&source_storage.migration_outbox_lock);
    for (uint32_t i = 0; i < source_storage.migration_outbox_count; i++) {
        vemb_v16_migration_outbox_destroy(
            source_storage.migration_outboxes[i]);
        source_storage.migration_outboxes[i] = NULL;
    }
    source_storage.migration_outbox_count = 0;
    pthread_mutex_unlock(&source_storage.migration_outbox_lock);

    fill_vector(live_range_value1, dim, 8200);
    fill_vector(live_range_value2, dim, 8210);
    warm_slot = UINT32_MAX;
    assert(vemb_v16_tlc_put_with_epoch(source,
                                       live_range_key1,
                                       live_range_key1_len,
                                       live_range_key1_hash,
                                       live_range_value1,
                                       sizeof(live_range_value1),
                                       86,
                                       &handle,
                                       &warm_slot) == 0);
    warm_slot = UINT32_MAX;
    assert(vemb_v16_tlc_put_with_epoch(source,
                                       live_range_key2,
                                       live_range_key2_len,
                                       live_range_key2_hash,
                                       live_range_value2,
                                       sizeof(live_range_value2),
                                       86,
                                       &handle,
                                       &warm_slot) == 0);
    assert(vemb_v16_storage_migration_mark_migrating_in_shard(
               &source_storage,
               live_range_key1,
               live_range_key1_len,
               live_range_key1_hash,
               86,
               3,
               live_range_shard_id,
               &info) == 0);
    assert(info.shard_id == live_range_shard_id);
    assert(vemb_v16_storage_migration_mark_migrating_in_shard(
               &source_storage,
               live_range_key2,
               live_range_key2_len,
               live_range_key2_hash,
               86,
               3,
               live_range_shard_id,
               &info) == 0);
    assert(info.shard_id == live_range_shard_id);
    assert(tlc_core_snapshot(source->core,
                                 live_range_key1,
                                 live_range_key1_len,
                                 live_range_key1_hash,
                                 1,
                                 3,
                                 &live_range_snapshot1,
                                 live_range_snapshot_value1,
                                 sizeof(live_range_snapshot_value1)) == 0);
    assert(live_range_snapshot1.shard_id == live_range_shard_id);
    assert(tlc_core_snapshot(source->core,
                                 live_range_key2,
                                 live_range_key2_len,
                                 live_range_key2_hash,
                                 1,
                                 3,
                                 &live_range_snapshot2,
                                 live_range_snapshot_value2,
                                 sizeof(live_range_snapshot_value2)) == 0);
    assert(live_range_snapshot2.shard_id == live_range_shard_id);
    assert(vemb_v16_tlc_apply_migration(dest,
                                        &live_range_snapshot1,
                                        live_range_snapshot_value1,
                                        sizeof(live_range_snapshot_value1),
                                        &apply_status,
                                        &handle) == 0);
    assert(apply_status == TLC_CORE_MIGRATION_APPLIED);
    assert(vemb_v16_tlc_apply_migration(dest,
                                        &live_range_snapshot2,
                                        live_range_snapshot_value2,
                                        sizeof(live_range_snapshot_value2),
                                        &apply_status,
                                        &handle) == 0);
    assert(apply_status == TLC_CORE_MIGRATION_APPLIED);

    fill_range_control_req(&range_req, 86, 0, 3, live_range_shard_id);
    memset(&range_resp, 0, sizeof(range_resp));
    assert(tcp_migration_range_control(
               source_proxy,
               VEMB_V16_NET_MIGRATION_RANGE_BARRIER,
               &range_req,
               &range_resp) == 0);
    assert(range_resp.status == VEMB_V16_STATUS_OK);
    assert(range_resp.key_count == 2);
    assert(range_resp.barrier_seq == 0);
    assert(range_resp.source_seq == 0);
    assert(range_resp.outbox_state == VEMB_V16_MIGRATION_OUTBOX_OPEN);

    fill_vector(live_range_update1, dim, 8220);
    memset(&completion, 0, sizeof(completion));
    run_vadd_job(&source_storage,
                 live_range_key1,
                 live_range_key1_hash,
                 86,
                 live_range_update1,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);
    memset(&completion, 0, sizeof(completion));
    run_vrem_job(&source_storage,
                 live_range_key2,
                 live_range_key2_hash,
                 86,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);
    assert(completion.op == VEMB_V16_OP_VREM);
    assert(source_storage.migration_outbox_count == 1);
    vemb_v16_migration_outbox_get_stats(
        source_storage.migration_outboxes[0],
        &outbox_stats);
    assert(outbox_stats.shard_id == live_range_shard_id);
    assert(outbox_stats.acked_seq == 2);
    assert(outbox_stats.next_delta_seq == 3);
    assert(outbox_stats.barrier_seq == 0);
    assert(outbox_stats.pending_count == 0);
    assert(outbox_stats.state == VEMB_V16_MIGRATION_OUTBOX_OPEN);
    assert(vemb_v16_tlc_get_handle(dest,
                                   live_range_key1,
                                   live_range_key1_len,
                                   live_range_key1_hash,
                                   &handle,
                                   &warm_slot) == 0);
    assert(vemb_v16_tlc_vector_slice(dest,
                                     &handle,
                                     &stored,
                                     &stored_len) == 0);
    assert(stored_len == sizeof(live_range_update1));
    assert(memcmp(stored,
                  live_range_update1,
                  sizeof(live_range_update1)) == 0);
    assert(vemb_v16_tlc_get_handle(dest,
                                   live_range_key2,
                                   live_range_key2_len,
                                   live_range_key2_hash,
                                   &handle,
                                   &warm_slot) != 0);
    assert(tlc_core_get_migration_info(dest->core,
                                           live_range_key2,
                                           live_range_key2_len,
                                           live_range_key2_hash,
                                           &info) == 0);
    assert(info.key_version == 2);
    assert(info.tombstone == 1);
    assert(info.migration_state == TLC_CORE_KEY_DEST_COMMITTED);

    fill_range_control_req(&range_req, 86, 87, 3, live_range_shard_id);
    memset(&range_resp, 0, sizeof(range_resp));
    assert(tcp_migration_range_control(
               source_proxy,
               VEMB_V16_NET_MIGRATION_RANGE_MARK_CUTOVER,
               &range_req,
               &range_resp) == 0);
    assert(range_resp.status == VEMB_V16_STATUS_OK);
    assert(range_resp.key_count == 2);
    assert(range_resp.success_count == 2);
    assert(range_resp.error_count == 0);
    assert(range_resp.source_seq == 2);
    assert(range_resp.barrier_seq == 2);
    assert(range_resp.owner_epoch == 87);
    assert(tlc_core_get_migration_info(source->core,
                                           live_range_key1,
                                           live_range_key1_len,
                                           live_range_key1_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_CUTOVER);
    assert(info.owner_epoch == 87);
    assert(tlc_core_get_migration_info(source->core,
                                           live_range_key2,
                                           live_range_key2_len,
                                           live_range_key2_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_CUTOVER);
    assert(info.tombstone == 1);
    assert(info.owner_epoch == 87);
    assert(tlc_core_get_migration_info(dest->core,
                                           live_range_key1,
                                           live_range_key1_len,
                                           live_range_key1_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_DEST_COMMITTED);
    assert(info.owner_epoch == 87);
    assert(tlc_core_get_migration_info(dest->core,
                                           live_range_key2,
                                           live_range_key2_len,
                                           live_range_key2_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_DEST_COMMITTED);
    assert(info.tombstone == 1);
    assert(info.owner_epoch == 87);

    memset(&completion, 0, sizeof(completion));
    run_vadd_job(&source_storage,
                 live_range_key1,
                 live_range_key1_hash,
                 87,
                 live_range_update1,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_MOVED);
    assert(completion.redirect_owner == 3);
    fill_vector(live_range_redirect_update, dim, 8230);
    memset(&completion, 0, sizeof(completion));
    run_vadd_job_with_flags(&dest_storage,
                            live_range_key1,
                            live_range_key1_hash,
                            86,
                            live_range_redirect_update,
                            dim,
                            VEMB_V16_REQ_F_ASK_REDIRECT,
                            &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);
    assert(vemb_v16_tlc_get_handle(dest,
                                   live_range_key1,
                                   live_range_key1_len,
                                   live_range_key1_hash,
                                   &handle,
                                   &warm_slot) == 0);
    assert(vemb_v16_tlc_vector_slice(dest,
                                     &handle,
                                     &stored,
                                     &stored_len) == 0);
    assert(stored_len == sizeof(live_range_redirect_update));
    assert(memcmp(stored,
                  live_range_redirect_update,
                  sizeof(live_range_redirect_update)) == 0);

    pthread_mutex_lock(&source_storage.migration_outbox_lock);
    for (uint32_t i = 0; i < source_storage.migration_outbox_count; i++) {
        vemb_v16_migration_outbox_destroy(
            source_storage.migration_outboxes[i]);
        source_storage.migration_outboxes[i] = NULL;
    }
    source_storage.migration_outbox_count = 0;
    pthread_mutex_unlock(&source_storage.migration_outbox_lock);

    for (uint32_t i = 0; i < large_range_key_count; i++) {
        snprintf(large_range_keys[i],
                 sizeof(large_range_keys[i]),
                 "supernode:migration-large-range:%03u",
                 i);
        large_range_key_lens[i] =
            (uint32_t)strlen(large_range_keys[i]);
        large_range_key_hashes[i] =
            vemb_v16_xxh3_64_str(large_range_keys[i],
                             large_range_key_lens[i]);
        fill_vector(large_range_value, dim, 8400 + i);
        warm_slot = UINT32_MAX;
        assert(vemb_v16_tlc_put_with_epoch(source,
                                           large_range_keys[i],
                                           large_range_key_lens[i],
                                           large_range_key_hashes[i],
                                           large_range_value,
                                           sizeof(large_range_value),
                                           84,
                                           &handle,
                                           &warm_slot) == 0);
        assert(vemb_v16_storage_migration_mark_migrating_in_shard(
                   &source_storage,
                   large_range_keys[i],
                   large_range_key_lens[i],
                   large_range_key_hashes[i],
                   84,
                   3,
                   large_range_shard_id,
                   &info) == 0);
        assert(info.shard_id == large_range_shard_id);

        tlc_core_migration_snapshot_t large_range_snapshot = {0};
        assert(tlc_core_snapshot(source->core,
                                     large_range_keys[i],
                                     large_range_key_lens[i],
                                     large_range_key_hashes[i],
                                     1,
                                     3,
                                     &large_range_snapshot,
                                     large_range_snapshot_value,
                                     sizeof(large_range_snapshot_value)) == 0);
        assert(large_range_snapshot.shard_id == large_range_shard_id);
        assert(vemb_v16_tlc_apply_migration(dest,
                                            &large_range_snapshot,
                                            large_range_snapshot_value,
                                            sizeof(large_range_snapshot_value),
                                            &apply_status,
                                            &handle) == 0);
        assert(apply_status == TLC_CORE_MIGRATION_APPLIED);
    }

    fill_range_control_req(&range_req, 84, 0, 3, large_range_shard_id);
    range_req.page_limit = large_range_page_limit;
    memset(&range_resp, 0, sizeof(range_resp));
    assert(tcp_migration_range_control(
               source_proxy,
               VEMB_V16_NET_MIGRATION_RANGE_BARRIER,
               &range_req,
               &range_resp) == 0);
    assert(range_resp.status == VEMB_V16_STATUS_OK);
    assert(range_resp.key_count == large_range_key_count);
    assert(range_resp.page_key_count == large_range_key_count);
    assert(range_resp.success_count == large_range_key_count);
    assert(range_resp.remaining_keys == 0);
    assert(range_resp.range_ready == 1);
    assert(range_resp.range_done == 1);
    assert(range_resp.page_limit == large_range_page_limit);

    fill_range_control_req(&range_req, 84, 85, 3, large_range_shard_id);
    range_req.page_limit = large_range_page_limit;
    uint32_t total_cutover = 0;
    for (uint32_t page = 0; page < large_range_key_count; page++) {
        memset(&range_resp, 0, sizeof(range_resp));
        assert(tcp_migration_range_control(
                   source_proxy,
                   VEMB_V16_NET_MIGRATION_RANGE_MARK_CUTOVER,
                   &range_req,
                   &range_resp) == 0);
        assert(range_resp.status == VEMB_V16_STATUS_OK);
        assert(range_resp.page_limit == large_range_page_limit);
        assert(range_resp.key_count == range_resp.page_key_count);
        assert(range_resp.key_count <= large_range_page_limit);
        assert(range_resp.success_count == range_resp.key_count);
        assert(range_resp.error_count == 0);
        total_cutover += range_resp.success_count;
        assert(range_resp.remaining_keys ==
               large_range_key_count - total_cutover);
        if (range_resp.range_done)
            break;
    }
    assert(total_cutover == large_range_key_count);
    assert(range_resp.range_done == 1);
    memset(&range_resp, 0, sizeof(range_resp));
    assert(tcp_migration_range_control(
               source_proxy,
               VEMB_V16_NET_MIGRATION_RANGE_MARK_CUTOVER,
               &range_req,
               &range_resp) == 0);
    assert(range_resp.status == VEMB_V16_STATUS_OK);
    assert(range_resp.key_count == 0);
    assert(range_resp.range_done == 1);

    uint32_t total_source_gc = 0;
    for (uint32_t page = 0; page < large_range_key_count; page++) {
        memset(&range_resp, 0, sizeof(range_resp));
        assert(tcp_migration_range_control(
                   source_proxy,
                   VEMB_V16_NET_MIGRATION_RANGE_SOURCE_GC,
                   &range_req,
                   &range_resp) == 0);
        assert(range_resp.status == VEMB_V16_STATUS_OK);
        assert(range_resp.page_limit == large_range_page_limit);
        assert(range_resp.key_count == range_resp.page_key_count);
        assert(range_resp.key_count <= large_range_page_limit);
        assert(range_resp.success_count == range_resp.key_count);
        assert(range_resp.error_count == 0);
        total_source_gc += range_resp.success_count;
        assert(range_resp.remaining_keys ==
               large_range_key_count - total_source_gc);
        if (range_resp.range_done)
            break;
    }
    assert(total_source_gc == large_range_key_count);
    assert(range_resp.range_done == 1);
    memset(&range_resp, 0, sizeof(range_resp));
    assert(tcp_migration_range_control(
               source_proxy,
               VEMB_V16_NET_MIGRATION_RANGE_SOURCE_GC,
               &range_req,
               &range_resp) == 0);
    assert(range_resp.status == VEMB_V16_STATUS_OK);
    assert(range_resp.key_count == 0);
    assert(range_resp.range_done == 1);

    for (uint32_t i = 0; i < large_range_key_count; i++) {
        assert(tlc_core_get_migration_info(source->core,
                                               large_range_keys[i],
                                               large_range_key_lens[i],
                                               large_range_key_hashes[i],
                                               &info) == 0);
        assert(info.migration_state == TLC_CORE_KEY_SOURCE_GC);
        assert(info.owner_epoch == 85);
        assert(info.shard_id == large_range_shard_id);
    }

    pthread_mutex_lock(&source_storage.migration_outbox_lock);
    for (uint32_t i = 0; i < source_storage.migration_outbox_count; i++) {
        vemb_v16_migration_outbox_destroy(
            source_storage.migration_outboxes[i]);
        source_storage.migration_outboxes[i] = NULL;
    }
    source_storage.migration_outbox_count = 0;
    pthread_mutex_unlock(&source_storage.migration_outbox_lock);

    fill_vector(initial, dim, 6100);
    assert(vemb_v16_tlc_put_with_epoch(source,
                                       key,
                                       key_len,
                                       key_hash,
                                       initial,
                                       sizeof(initial),
                                       60,
                                       &handle,
                                       &warm_slot) == 0);
    assert(vemb_v16_storage_migration_mark_migrating(&source_storage,
                                                     key,
                                                     key_len,
                                                     key_hash,
                                                     60,
                                                     3,
                                                     &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_MIGRATING);

    fill_vector(updated, dim, 6200);
    run_vadd_job(&source_storage,
                 key,
                 key_hash,
                 60,
                 updated,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);
    assert(source_storage.migration_outbox_count == 1);
    vemb_v16_migration_outbox_get_stats(
        source_storage.migration_outboxes[0],
        &outbox_stats);
    assert(outbox_stats.pending_count == 0);
    assert(outbox_stats.acked_seq == 1);
    assert(outbox_stats.next_delta_seq == 2);
    assert(outbox_stats.source_owner == 1);
    assert(outbox_stats.target_owner == 3);
    assert(outbox_stats.topology_epoch == 60);

    assert(vemb_v16_tlc_get_handle(dest,
                                   key,
                                   key_len,
                                   key_hash,
                                   &handle,
                                   &warm_slot) == 0);
    assert(handle.region_id == 953);
    assert(vemb_v16_tlc_vector_slice(dest,
                                     &handle,
                                     &stored,
                                     &stored_len) == 0);
    assert(stored_len == sizeof(updated));
    assert(memcmp(stored, updated, sizeof(updated)) == 0);
    assert(tlc_core_get_migration_info(dest->core,
                                           key,
                                           key_len,
                                           key_hash,
                                           &info) == 0);
    assert(info.key_version == 2);
    assert(info.migration_state == TLC_CORE_KEY_DEST_COMMITTED);

    fill_vector(updated2, dim, 6300);
    warm_slot = UINT32_MAX;
    assert(vemb_v16_tlc_put_with_epoch(source,
                                       key,
                                       key_len,
                                       key_hash,
                                       updated2,
                                       sizeof(updated2),
                                       60,
                                       &handle,
                                       &warm_slot) == 0);
    assert(tlc_core_get_migration_info(source->core,
                                           key,
                                           key_len,
                                           key_hash,
                                           &info) == 0);
    assert(info.key_version == 3);
    vemb_v16_ub_migration_delta_desc_t pending_delta = {
        .key_hash = key_hash,
        .key_version = info.key_version,
        .topology_epoch = info.topology_epoch,
        .op = VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT,
        .key_len = key_len,
        .source_owner = source_storage.local_owner_id,
        .target_owner = info.target_owner,
        .value_size = handle.bytes,
        .region_id = handle.region_id,
        .local_slot = handle.local_slot,
        .bytes = handle.bytes,
        .offset = handle.offset,
        .owner_generation = handle.owner_generation,
    };
    memcpy(pending_delta.key, key, key_len);
    pthread_mutex_lock(&source_storage.migration_outbox_lock);
    assert(vemb_v16_migration_outbox_append(
               source_storage.migration_outboxes[0],
               &pending_delta,
               NULL) == VEMB_V16_MIGRATION_OUTBOX_OK);
    pthread_mutex_unlock(&source_storage.migration_outbox_lock);

    assert(vemb_v16_storage_migration_drain_outboxes(&source_storage,
                                                     8,
                                                     &sent_count,
                                                     &acked_count) == 0);
    assert(sent_count == 1);
    assert(acked_count == 1);
    vemb_v16_migration_outbox_get_stats(
        source_storage.migration_outboxes[0],
        &outbox_stats);
    assert(outbox_stats.pending_count == 0);
    assert(outbox_stats.acked_seq == 2);
    assert(vemb_v16_tlc_get_handle(dest,
                                   key,
                                   key_len,
                                   key_hash,
                                   &handle,
                                   &warm_slot) == 0);
    assert(vemb_v16_tlc_vector_slice(dest,
                                     &handle,
                                     &stored,
                                     &stored_len) == 0);
    assert(stored_len == sizeof(updated2));
    assert(memcmp(stored, updated2, sizeof(updated2)) == 0);

    fill_vector(updated3, dim, 6400);
    warm_slot = UINT32_MAX;
    assert(vemb_v16_tlc_put_with_epoch(source,
                                       key,
                                       key_len,
                                       key_hash,
                                       updated3,
                                       sizeof(updated3),
                                       60,
                                       &handle,
                                       &warm_slot) == 0);
    assert(tlc_core_get_migration_info(source->core,
                                           key,
                                           key_len,
                                           key_hash,
                                           &info) == 0);
    assert(info.key_version == 4);
    memset(&pending_delta, 0, sizeof(pending_delta));
    pending_delta.key_hash = key_hash;
    pending_delta.key_version = info.key_version;
    pending_delta.topology_epoch = info.topology_epoch;
    pending_delta.op = VEMB_V16_UB_MIGRATION_RPC_DELTA_PUT;
    pending_delta.key_len = key_len;
    pending_delta.source_owner = source_storage.local_owner_id;
    pending_delta.target_owner = info.target_owner;
    pending_delta.value_size = handle.bytes;
    pending_delta.region_id = handle.region_id;
    pending_delta.local_slot = handle.local_slot;
    pending_delta.bytes = handle.bytes;
    pending_delta.offset = handle.offset;
    pending_delta.owner_generation = handle.owner_generation;
    memcpy(pending_delta.key, key, key_len);
    pthread_mutex_lock(&source_storage.migration_outbox_lock);
    assert(vemb_v16_migration_outbox_append(
               source_storage.migration_outboxes[0],
               &pending_delta,
               NULL) == VEMB_V16_MIGRATION_OUTBOX_OK);
    pthread_mutex_unlock(&source_storage.migration_outbox_lock);

    memset(&info, 0, sizeof(info));
    memset(&outbox_stats, 0, sizeof(outbox_stats));
    assert(vemb_v16_storage_migration_barrier(&source_storage,
                                              key,
                                              key_len,
                                              key_hash,
                                              60,
                                              3,
                                              &info,
                                              &outbox_stats) == 0);
    assert(outbox_stats.pending_count == 0);
    assert(outbox_stats.acked_seq == 3);
    assert(outbox_stats.barrier_seq == 3);
    assert(outbox_stats.state == VEMB_V16_MIGRATION_OUTBOX_OPEN);
    assert(vemb_v16_tlc_get_handle(dest,
                                   key,
                                   key_len,
                                   key_hash,
                                   &handle,
                                   &warm_slot) == 0);
    assert(vemb_v16_tlc_vector_slice(dest,
                                     &handle,
                                     &stored,
                                     &stored_len) == 0);
    assert(stored_len == sizeof(updated3));
    assert(memcmp(stored, updated3, sizeof(updated3)) == 0);

    memset(&info, 0, sizeof(info));
    assert(vemb_v16_storage_migration_mark_cutover(&source_storage,
                                                   key,
                                                   key_len,
                                                   key_hash,
                                                   61,
                                                   3,
                                                   &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_CUTOVER);
    assert(info.owner_epoch == 61);
    assert(info.target_owner == 3);

    fill_vector(deleted_value, dim, 6500);
    assert(vemb_v16_tlc_put_with_epoch(source,
                                       deleted_key,
                                       deleted_key_len,
                                       deleted_key_hash,
                                       deleted_value,
                                       sizeof(deleted_value),
                                       70,
                                       &handle,
                                       &warm_slot) == 0);
    assert(vemb_v16_storage_migration_mark_migrating(&source_storage,
                                                     deleted_key,
                                                     deleted_key_len,
                                                     deleted_key_hash,
                                                     70,
                                                     3,
                                                     &info) == 0);
    assert(tlc_core_snapshot(source->core,
                                 deleted_key,
                                 deleted_key_len,
                                 deleted_key_hash,
                                 1,
                                 3,
                                 &deleted_snapshot,
                                 deleted_snapshot_value,
                                 sizeof(deleted_snapshot_value)) == 0);
    assert(vemb_v16_tlc_apply_migration(dest,
                                        &deleted_snapshot,
                                        deleted_snapshot_value,
                                        sizeof(deleted_snapshot_value),
                                        &apply_status,
                                        &handle) == 0);
    assert(apply_status == TLC_CORE_MIGRATION_APPLIED);
    assert(vemb_v16_tlc_get_handle(dest,
                                   deleted_key,
                                   deleted_key_len,
                                   deleted_key_hash,
                                   &handle,
                                   &warm_slot) == 0);

    vemb_v16_ub_migration_delta_ack_desc_t deleted_ack = {0};
    memset(&info, 0, sizeof(info));
    assert(vemb_v16_storage_delete_with_epoch(&source_storage,
                                              deleted_key,
                                              deleted_key_len,
                                              deleted_key_hash,
                                              70,
                                              &info,
                                              &deleted_ack) == 0);
    assert(info.key_version == 2);
    assert(info.migration_state == TLC_CORE_KEY_MIGRATING);
    assert(info.tombstone == 1);
    assert(deleted_ack.applied_seq == 1);
    assert(vemb_v16_tlc_get_handle(source,
                                   deleted_key,
                                   deleted_key_len,
                                   deleted_key_hash,
                                   &handle,
                                   &warm_slot) != 0);
    assert(vemb_v16_tlc_get_handle(dest,
                                   deleted_key,
                                   deleted_key_len,
                                   deleted_key_hash,
                                   &handle,
                                   &warm_slot) != 0);
    assert(tlc_core_get_migration_info(dest->core,
                                           deleted_key,
                                           deleted_key_len,
                                           deleted_key_hash,
                                           &info) == 0);
    assert(info.key_version == 2);
    assert(info.tombstone == 1);
    assert(info.migration_state == TLC_CORE_KEY_DEST_COMMITTED);

    memset(&info, 0, sizeof(info));
    memset(&outbox_stats, 0, sizeof(outbox_stats));
    assert(vemb_v16_storage_migration_barrier(&source_storage,
                                              deleted_key,
                                              deleted_key_len,
                                              deleted_key_hash,
                                              70,
                                              3,
                                              &info,
                                              &outbox_stats) == 0);
    assert(outbox_stats.pending_count == 0);
    assert(outbox_stats.acked_seq == 1);
    assert(outbox_stats.barrier_seq == 1);
    assert(outbox_stats.state == VEMB_V16_MIGRATION_OUTBOX_OPEN);
    memset(&info, 0, sizeof(info));
    assert(vemb_v16_storage_migration_mark_cutover(&source_storage,
                                                   deleted_key,
                                                   deleted_key_len,
                                                   deleted_key_hash,
                                                   71,
                                                   3,
                                                   &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_CUTOVER);
    assert(info.owner_epoch == 71);
    assert(info.tombstone == 1);

    fill_vector(vrem_value, dim, 6260);
    warm_slot = UINT32_MAX;
    assert(vemb_v16_tlc_put_with_epoch(source,
                                       vrem_key,
                                       vrem_key_len,
                                       vrem_key_hash,
                                       vrem_value,
                                       sizeof(vrem_value),
                                       72,
                                       &handle,
                                       &warm_slot) == 0);
    assert(vemb_v16_storage_migration_mark_migrating(&source_storage,
                                                     vrem_key,
                                                     vrem_key_len,
                                                     vrem_key_hash,
                                                     72,
                                                     3,
                                                     &info) == 0);
    assert(tlc_core_snapshot(source->core,
                                 vrem_key,
                                 vrem_key_len,
                                 vrem_key_hash,
                                 1,
                                 3,
                                 &vrem_snapshot,
                                 vrem_snapshot_value,
                                 sizeof(vrem_snapshot_value)) == 0);
    assert(vemb_v16_tlc_apply_migration(dest,
                                        &vrem_snapshot,
                                        vrem_snapshot_value,
                                        sizeof(vrem_snapshot_value),
                                        &apply_status,
                                        &handle) == 0);
    assert(apply_status == TLC_CORE_MIGRATION_APPLIED);
    assert(vemb_v16_tlc_get_handle(dest,
                                   vrem_key,
                                   vrem_key_len,
                                   vrem_key_hash,
                                   &handle,
                                   &warm_slot) == 0);

    memset(&completion, 0, sizeof(completion));
    run_vrem_job(&source_storage,
                 vrem_key,
                 vrem_key_hash,
                 72,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);
    assert(completion.op == VEMB_V16_OP_VREM);
    assert(completion.vector_bytes == 0);
    assert(vemb_v16_tlc_get_handle(source,
                                   vrem_key,
                                   vrem_key_len,
                                   vrem_key_hash,
                                   &handle,
                                   &warm_slot) != 0);
    assert(vemb_v16_tlc_get_handle(dest,
                                   vrem_key,
                                   vrem_key_len,
                                   vrem_key_hash,
                                   &handle,
                                   &warm_slot) != 0);
    assert(tlc_core_get_migration_info(dest->core,
                                           vrem_key,
                                           vrem_key_len,
                                           vrem_key_hash,
                                           &info) == 0);
    assert(info.key_version == 2);
    assert(info.tombstone == 1);
    assert(info.migration_state == TLC_CORE_KEY_DEST_COMMITTED);

    memset(&info, 0, sizeof(info));
    memset(&outbox_stats, 0, sizeof(outbox_stats));
    assert(vemb_v16_storage_migration_barrier(&source_storage,
                                              vrem_key,
                                              vrem_key_len,
                                              vrem_key_hash,
                                              72,
                                              3,
                                              &info,
                                              &outbox_stats) == 0);
    assert(outbox_stats.pending_count == 0);
    assert(outbox_stats.acked_seq == 1);
    assert(outbox_stats.barrier_seq == 1);
    assert(outbox_stats.state == VEMB_V16_MIGRATION_OUTBOX_OPEN);
    memset(&info, 0, sizeof(info));
    assert(vemb_v16_storage_migration_mark_cutover(&source_storage,
                                                   vrem_key,
                                                   vrem_key_len,
                                                   vrem_key_hash,
                                                   73,
                                                   3,
                                                   &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_CUTOVER);
    assert(info.owner_epoch == 73);
    assert(info.tombstone == 1);

    memset(&completion, 0, sizeof(completion));
    run_vadd_job(&source_storage,
                 vrem_key,
                 vrem_key_hash,
                 73,
                 updated3,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_MOVED);
    assert(completion.redirect_owner == 3);

    fill_control_req(&control_req, vrem_key, vrem_key_hash, 73, 3);
    memset(&control_resp, 0, sizeof(control_resp));
    assert(tcp_migration_control(source_proxy,
                                 VEMB_V16_NET_MIGRATION_MARK_SOURCE_GC,
                                 &control_req,
                                 &control_resp) == 0);
    assert(control_resp.status == VEMB_V16_STATUS_OK);
    assert(control_resp.migration_state == TLC_CORE_KEY_SOURCE_GC);
    assert(control_resp.tombstone == 1);
    assert(control_resp.target_owner == 3);
    assert(tlc_core_get_migration_info(source->core,
                                           vrem_key,
                                           vrem_key_len,
                                           vrem_key_hash,
                                           &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_SOURCE_GC);
    assert(info.tombstone == 1);
    memset(&completion, 0, sizeof(completion));
    run_vadd_job(&source_storage,
                 vrem_key,
                 vrem_key_hash,
                 73,
                 updated3,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_MOVED);
    assert(completion.redirect_owner == 3);

    vemb_v16_proxy_destroy(source_proxy);
    source_proxy = NULL;

    for (uint32_t i = 0; i < source_storage.migration_outbox_count; i++)
        vemb_v16_migration_outbox_destroy(
            source_storage.migration_outboxes[i]);
    pthread_mutex_destroy(&dest_storage.migration_outbox_lock);
    pthread_mutex_destroy(&dest_storage.topology_lock);
    pthread_mutex_destroy(&source_storage.migration_outbox_lock);
    pthread_mutex_destroy(&source_storage.topology_lock);
    vemb_v16_ub_rpc_destroy(dest_rpc);
    vemb_v16_ub_rpc_destroy(source_rpc);
    vemb_v16_tlc_destroy(dest);
    vemb_v16_tlc_destroy(source);
    cleanup_rpc_rings(req_source_dest,
                      req_dest_source,
                      resp_source_dest,
                      resp_dest_source);
}

static void test_migration_retry_worker_drains_when_target_becomes_ready(void) {
    enum { dim = 2, max_vectors = 8 };
    float source_region[dim * max_vectors];
    float dest_region[dim * max_vectors];
    float initial[dim];
    float updated[dim];
    vemb_v16_warm_region_header_t source_allocator;
    vemb_v16_warm_region_header_t dest_allocator;
    vemb_v16_tlc_t *source = NULL;
    vemb_v16_tlc_t *dest = NULL;
    vemb_v16_ub_rpc_t *source_rpc = NULL;
    vemb_v16_ub_rpc_t *dest_rpc = NULL;
    vemb_v16_tlc_warm_region_t source_warm = {
        .region_id = 971,
        .backend_type = VEMB_V16_REGION_LOCAL_SHM,
        .is_local = 1,
        .weight = 1,
        .mapped_addr = source_region,
        .region_bytes = sizeof(source_region),
        .value_size = dim * sizeof(float),
        .slot_meta = NULL,
    };
    vemb_v16_tlc_warm_region_t dest_regions[] = {
        {
            .region_id = 971,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 0,
            .weight = 1,
            .mapped_addr = source_region,
            .region_bytes = sizeof(source_region),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
        {
            .region_id = 973,
            .backend_type = VEMB_V16_REGION_LOCAL_SHM,
            .is_local = 1,
            .weight = 1,
            .mapped_addr = dest_region,
            .region_bytes = sizeof(dest_region),
            .value_size = dim * sizeof(float),
            .slot_meta = NULL,
        },
    };
    char req_source_dest[64];
    char req_dest_source[64];
    char resp_source_dest[64];
    char resp_dest_source[64];
    vemb_v16_ub_rpc_peer_t source_peer;
    vemb_v16_ub_rpc_peer_t dest_peer;
    const char *key = "supernode:migration-retry";
    uint32_t key_len = (uint32_t)strlen(key);
    uint64_t key_hash = vemb_v16_xxh3_64_str(key, key_len);
    vemb_v16_storage_ctx_t source_storage;
    vemb_v16_vector_handle_t handle = {0};
    uint32_t warm_slot = UINT32_MAX;
    tlc_core_key_migration_info_t info = {0};
    vemb_v16_completion_t completion = {0};
    const uint8_t *stored = NULL;
    uint32_t stored_len = 0;
    vemb_v16_migration_outbox_stats_t outbox_stats = {0};

    snprintf(req_source_dest, sizeof(req_source_dest),
             "/v16ctl_retry_%ld_req_1_3", (long)getpid());
    snprintf(req_dest_source, sizeof(req_dest_source),
             "/v16ctl_retry_%ld_req_3_1", (long)getpid());
    snprintf(resp_source_dest, sizeof(resp_source_dest),
             "/v16ctl_retry_%ld_resp_1_3", (long)getpid());
    snprintf(resp_dest_source, sizeof(resp_dest_source),
             "/v16ctl_retry_%ld_resp_3_1", (long)getpid());
    cleanup_rpc_rings(req_source_dest,
                      req_dest_source,
                      resp_source_dest,
                      resp_dest_source);

    memset(source_region, 0, sizeof(source_region));
    memset(dest_region, 0, sizeof(dest_region));
    init_test_allocator(&source_allocator, 971, max_vectors);
    init_test_allocator(&dest_allocator, 973, max_vectors);
    assert(vemb_v16_tlc_create(&source,
                               dim,
                               max_vectors,
                               &source_warm,
                               1,
                               4) == 0);
    assert(vemb_v16_tlc_create(&dest,
                               dim,
                               max_vectors,
                               dest_regions,
                               2,
                               4) == 0);
    make_rpc_peers(&source_peer,
                   3,
                   &dest_peer,
                   1,
                   req_source_dest,
                   req_dest_source,
                   resp_source_dest,
                   resp_dest_source);
    assert(vemb_v16_ub_rpc_create(&source_rpc,
                                  source,
                                  1,
                                  5,
                                  &source_peer,
                                  1) == 0);

    memset(&source_storage, 0, sizeof(source_storage));
    source_storage.local_owner_id = 1;
    source_storage.tlc = source;
    source_storage.ub_rpc = source_rpc;
    init_storage_runtime_fields(&source_storage, 70, 70);
    assert(pthread_mutex_init(&source_storage.topology_lock, NULL) == 0);
    assert(pthread_mutex_init(&source_storage.migration_outbox_lock,
                              NULL) == 0);
    assert(vemb_v16_storage_migration_retry_start(&source_storage,
                                                  1000,
                                                  8) == 0);

    fill_vector(initial, dim, 7100);
    assert(vemb_v16_tlc_put_with_epoch(source,
                                       key,
                                       key_len,
                                       key_hash,
                                       initial,
                                       sizeof(initial),
                                       70,
                                       &handle,
                                       &warm_slot) == 0);
    assert(vemb_v16_storage_migration_mark_migrating(&source_storage,
                                                     key,
                                                     key_len,
                                                     key_hash,
                                                     70,
                                                     3,
                                                     &info) == 0);
    assert(info.migration_state == TLC_CORE_KEY_MIGRATING);

    fill_vector(updated, dim, 7200);
    run_vadd_job(&source_storage,
                 key,
                 key_hash,
                 70,
                 updated,
                 dim,
                 &completion);
    assert(completion.status == VEMB_V16_STATUS_OK);
    assert(source_storage.migration_outbox_count == 1);
    pthread_mutex_lock(&source_storage.migration_outbox_lock);
    vemb_v16_migration_outbox_get_stats(
        source_storage.migration_outboxes[0],
        &outbox_stats);
    pthread_mutex_unlock(&source_storage.migration_outbox_lock);
    assert(outbox_stats.pending_count == 1);
    assert(outbox_stats.acked_seq == 0);
    if (vemb_v16_tlc_get_handle(dest,
                                key,
                                key_len,
                                key_hash,
                                &handle,
                                &warm_slot) == 0) {
        assert(handle.region_id != 973);
    }

    assert(vemb_v16_ub_rpc_create(&dest_rpc,
                                  dest,
                                  3,
                                  5,
                                  &dest_peer,
                                  1) == 0);
    assert(wait_for_outbox_progress(&source_storage,
                                    1,
                                    0,
                                    &outbox_stats) == 0);
    assert(outbox_stats.acked_seq == 1);
    assert(vemb_v16_tlc_get_handle(dest,
                                   key,
                                   key_len,
                                   key_hash,
                                   &handle,
                                   &warm_slot) == 0);
    assert(handle.region_id == 973);
    assert(vemb_v16_tlc_vector_slice(dest,
                                     &handle,
                                     &stored,
                                     &stored_len) == 0);
    assert(stored_len == sizeof(updated));
    assert(memcmp(stored, updated, sizeof(updated)) == 0);

    vemb_v16_storage_migration_retry_stop(&source_storage);
    for (uint32_t i = 0; i < source_storage.migration_outbox_count; i++)
        vemb_v16_migration_outbox_destroy(
            source_storage.migration_outboxes[i]);
    pthread_mutex_destroy(&source_storage.migration_outbox_lock);
    pthread_mutex_destroy(&source_storage.topology_lock);
    vemb_v16_ub_rpc_destroy(dest_rpc);
    vemb_v16_ub_rpc_destroy(source_rpc);
    vemb_v16_tlc_destroy(dest);
    vemb_v16_tlc_destroy(source);
    cleanup_rpc_rings(req_source_dest,
                      req_dest_source,
                      resp_source_dest,
                      resp_dest_source);
}

int main(void) {
    monotonicInit();

    test_scaleout_local_done_wire_lengths();
    test_proxy_migration_control_primitives();
    test_proxy_peer_view_topology_set_applies_mapping_first();
    test_storage_topology_auto_marks_migrating_keys();
    test_storage_topology_auto_pushes_baseline_snapshot();
    test_storage_baseline_retry_drains_after_target_ready();
    test_storage_auto_scaleout_state_machine_cutover();
    test_storage_coordinated_scaleout_waits_for_full_active();
    test_supernode_vadd_pushes_migration_delta();
    test_migration_retry_worker_drains_when_target_becomes_ready();
    test_supernode_lookup_miss_classification();
    printf("vemb_v16_migration_control_ut: all tests passed\n");
    return 0;
}
