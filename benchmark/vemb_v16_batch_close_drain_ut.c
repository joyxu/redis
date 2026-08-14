#include "vemb_v16_proxy_internal.h"
#include "vemb_v16_proxy_types.h"
#include "vemb_v16_supernode.h"

#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct batch_close_task {
    vemb_v16_proxy_t *proxy;
    uint64_t channel_id;
    atomic_int done;
    int rc;
} batch_close_task_t;

static void *close_channel_main(void *arg) {
    batch_close_task_t *task = arg;
    task->rc = vemb_v16_proxy_close_channel_by_id(task->proxy,
                                                   task->channel_id);
    atomic_store_explicit(&task->done, 1, memory_order_release);
    return NULL;
}

int main(void) {
    vemb_v16_proxy_t *proxy = calloc(1, sizeof(*proxy));
    assert(proxy);
    atomic_init(&proxy->next_channel_id, 1);
    atomic_init(&proxy->next_channel_index, 0);
    atomic_init(&proxy->next_v2_lane_index, 0);
    atomic_store_explicit(&proxy->next_channel_index, 1,
                          memory_order_relaxed);
    atomic_init(&proxy->running, 1);
    pthread_mutex_init(&proxy->stats_lock, NULL);
    proxy->proxy_io_worker_count = 2;
    proxy->supernode_worker_count = 2;
    for (uint32_t i = 0; i < VEMB_V16_MAX_CHANNELS; i++) {
        vemb_v16_channel_t *channel = &proxy->channels[i];
        atomic_init(&channel->slot_channel_id, 0);
        atomic_init(&channel->active, 0);
        atomic_init(&channel->proxy_io_registered, 0);
        atomic_init(&channel->proxy_io_state, 0);
        atomic_init(&channel->supernode_state, 0);
        atomic_init(&channel->completion_notify_armed, 0);
    }

    size_t desc_bytes = vemb_v16_client_ring_bytes(
        VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE);
    void *request_desc = calloc(1, desc_bytes);
    void *response_desc = calloc(1, desc_bytes);
    void *request_arena = calloc(1, CACHELINE_SIZE);
    void *response_arena = calloc(1, CACHELINE_SIZE);
    assert(request_desc && response_desc && request_arena && response_arena);
    vemb_v16_client_ring_init(request_desc, VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE);
    vemb_v16_client_ring_init(response_desc, VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE);

    vemb_v16_aeron_batch_channel_allocation_t allocation = {
        .request_desc_mapping = request_desc,
        .request_arena_mapping = request_arena,
        .response_desc_mapping = response_desc,
        .response_arena_mapping = response_arena,
        .request_desc_bytes = desc_bytes,
        .request_arena_bytes = CACHELINE_SIZE,
        .response_desc_bytes = desc_bytes,
        .response_arena_bytes = CACHELINE_SIZE,
    };
    uint64_t channel_id = 0;
    assert(vemb_v16_proxy_attach_cross_node_batch_channel(
               proxy, &allocation, 1, CACHELINE_SIZE,
               &channel_id) == 0);
    vemb_v16_channel_t *channel = &proxy->channels[1];
    assert(channel_id == channel->channel_id && channel->batch_v2);
    assert(channel->proxy_io_worker_id == 0);
    assert(channel->supernode_worker_id == 0);

    /* Model a job already dequeued by SuperNode before close starts. */
    atomic_store_explicit(&channel->supernode_state, 1, memory_order_release);
    batch_close_task_t task = {
        .proxy = proxy,
        .channel_id = channel_id,
    };
    atomic_init(&task.done, 0);
    pthread_t close_thread;
    assert(pthread_create(&close_thread, NULL, close_channel_main, &task) == 0);

    for (uint32_t spins = 0; spins < 1000; spins++) {
        uint_fast32_t state = atomic_load_explicit(&channel->supernode_state,
                                                    memory_order_acquire);
        if (!atomic_load_explicit(&channel->active, memory_order_acquire) &&
            (state & VEMB_V16_SUPERNODE_STATE_CLOSING) != 0)
            break;
        struct timespec pause = {0, 1000000};
        nanosleep(&pause, NULL);
    }
    assert(!atomic_load_explicit(&channel->active, memory_order_acquire));
    assert(!atomic_load_explicit(&task.done, memory_order_acquire));

    vemb_v16_job_base_t job = {
        .kind = VEMB_V16_JOB_KIND_BASE,
        .op = VEMB_V16_OP_PING,
        .req_id = 7,
        .channel_index = channel->index,
        .channel_id = channel_id,
        .batch_token = vemb_v16_batch_token_make(0, 0, 1, 0),
    };
    vemb_v16_supernode_handle_base_job(&channel->supernode_ctx, &job);
    assert(vemb_v16_aeron_available(&channel->completion_ring) == 1);

    atomic_fetch_sub_explicit(&channel->supernode_state, 1,
                              memory_order_release);
    assert(pthread_join(close_thread, NULL) == 0);
    assert(task.rc == 0 && atomic_load_explicit(&task.done, memory_order_acquire));
    assert(atomic_load_explicit(&channel->slot_channel_id,
                                memory_order_acquire) == 0);
    assert(channel->completion_slots == NULL && !channel->batch_v2);

    pthread_mutex_destroy(&proxy->stats_lock);
    free(proxy);
    free(request_desc);
    free(response_desc);
    free(request_arena);
    free(response_arena);
    puts("vemb_v16_batch_close_drain_ut: PASS");
    return 0;
}
