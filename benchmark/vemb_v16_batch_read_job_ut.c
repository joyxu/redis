/* Include the implementation to compare the two internal constructors without
 * exposing pool internals through a production API. Unused paths are GC'd. */
#include "../src/vemb_v16_proxy.c"

int main(void) {
    vemb_v16_proxy_t *proxy = calloc(1, sizeof(*proxy));
    assert(proxy);
    proxy->vector_dim = 300;
    proxy->vector_stride = 1200;
    vemb_v16_channel_t *ch = &proxy->channels[3];
    ch->proxy = proxy;
    ch->index = 3;
    ch->channel_id = 123;
    ch->transport_type = VEMB_V16_TRANSPORT_AERON;
    vemb_v16_job_pool_t *pool = &proxy->proxy_io_workers[0].job_pools[VEMB_V16_JOB_POOL_READ];
    pool->pool_type = VEMB_V16_JOB_POOL_READ;
    pool->slot_count = 2;
    pool->slot_stride = job_pool_slot_stride(pool->pool_type);
    pool->slots = malloc(pool->slot_stride * pool->slot_count);
    pool->free_stack = malloc(sizeof(uint32_t) * pool->slot_count);
    assert(pool->slots && pool->free_stack);
    memset(pool->slots, 0xa5, pool->slot_stride * pool->slot_count);
    pool->free_stack[0] = 0;
    pool->free_stack[1] = 1;
    pool->free_count = 2;
    for (uint32_t i = 0; i < 2; i++) {
        vemb_v16_job_slot_t *slot = job_pool_slot(pool, i);
        slot->hdr.generation = 7;
        atomic_init(&slot->hdr.state, VEMB_V16_JOB_SLOT_FREE);
    }

    const uint32_t lengths[] = {1, 9, VEMB_V16_MAX_KEY_LEN};
    for (size_t n = 0; n < sizeof(lengths) / sizeof(lengths[0]); n++) {
        vemb_v16_req_t req = {
            .op = VEMB_V16_OP_VEMB_HANDLE,
            .req_id = (uint32_t)n,
            .channel_id = ch->channel_id,
            .key_len = lengths[n],
            .topology_epoch = 42 + n,
            .dim = 300,
            .vector_bytes = 1200,
        };
        for (uint32_t i = 0; i < req.key_len; i++)
            req.key[i] = (char)i;
        req.key_hash = vemb_v16_xxh3_64_str(req.key, req.key_len);
        uint64_t token = vemb_v16_batch_token_make(0, 1, 8, (uint32_t)n);
        vemb_v16_pending_job_publish_t generic, direct, failed;
        assert(prepare_request_job(ch, &req, req.key_len, 0, &generic) == 0);
        assert(prepare_batch_read_job(ch, (const uint8_t *)req.key,
                   req.key_len, req.req_id, req.topology_epoch, token, 0, &direct) == 0);
        vemb_v16_job_slot_t *a = job_pool_slot(pool, generic.slot_id);
        vemb_v16_job_slot_t *b = job_pool_slot(pool, direct.slot_id);
        a->u.read_job.base.batch_token = token;
        assert(memcmp(&a->u.read_job.base, &b->u.read_job.base,
                      sizeof(a->u.read_job.base)) == 0);
        assert(a->u.read_job.key_len == b->u.read_job.key_len);
        assert(a->u.read_job.dim == b->u.read_job.dim);
        assert(a->u.read_job.vector_bytes == b->u.read_job.vector_bytes);
        assert(memcmp(a->u.read_job.key, b->u.read_job.key, req.key_len) == 0);
        assert(b->hdr.op == VEMB_V16_OP_VEMB_HANDLE);
        assert(atomic_load(&b->hdr.state) == VEMB_V16_JOB_SLOT_RESERVED);
        assert(direct.ref.generation == b->hdr.generation);
        assert(direct.ref.req_id == req.req_id && direct.ref.pool_type == pool->pool_type);
        assert(prepare_batch_read_job(ch, (const uint8_t *)req.key,
                   req.key_len, req.req_id, 42, token, 0, &failed) == -1);
        assert(pool->free_count == 0);
        memset(req.key, 0xff, req.key_len);
        assert((uint8_t)b->u.read_job.key[0] == 0);
        job_pool_release_slot(pool, generic.slot_id, 1);
        job_pool_release_slot(pool, direct.slot_id, 1);
        assert(pool->free_count == 2);
    }
    free(pool->slots);
    free(pool->free_stack);
    free(proxy);
    puts("vemb_v16_batch_read_job_ut: PASS");
    return 0;
}
