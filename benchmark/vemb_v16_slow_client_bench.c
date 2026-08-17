#define _GNU_SOURCE

#include "../src/vemb_v16_net.h"
#include "../src/vemb_v16_protocol.h"
#include "../src/zmalloc.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef struct bench_cfg {
    const char *host;
    uint16_t port;
    uint32_t dim;
    uint32_t prefill;
    uint32_t slow_ops;
    uint32_t probe_ops;
    uint32_t stall_ms;
    uint32_t timeout_ms;
    uint32_t assumed_proxy_io_threads;
} bench_cfg_t;

typedef struct tcp_channel {
    vemb_v16_channel_desc_t desc;
    int fd;
} tcp_channel_t;

typedef struct slow_thread_arg {
    const bench_cfg_t *cfg;
    tcp_channel_t *channel;
    char key[VEMB_V16_MAX_KEY_LEN];
    atomic_int *burst_ready;
    uint64_t ok;
    uint64_t fail;
} slow_thread_arg_t;

typedef struct probe_thread_arg {
    const bench_cfg_t *cfg;
    tcp_channel_t *channel;
    atomic_int *burst_ready;
    uint64_t *latency_ns;
    uint64_t elapsed_ns;
    uint64_t ok;
    uint64_t fail;
} probe_thread_arg_t;

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void sleep_ms(uint32_t ms) {
    struct timespec ts = {
        .tv_sec = (time_t)(ms / 1000u),
        .tv_nsec = (long)(ms % 1000u) * 1000000l,
    };
    nanosleep(&ts, NULL);
}

static int tcp_control_request(const bench_cfg_t *cfg,
                               uint16_t type,
                               uint64_t channel_id,
                               uint16_t expect_type,
                               void *payload,
                               uint32_t payload_len) {
    int fd = vemb_v16_net_connect(cfg->host, cfg->port, cfg->timeout_ms);
    if (fd < 0) return -1;
    if (vemb_v16_net_write_frame(fd,
                                 type,
                                 0,
                                 channel_id,
                                 0,
                                 NULL,
                                 0) != 0) {
        close(fd);
        return -1;
    }
    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != expect_type ||
        hdr.payload_len != payload_len ||
        (payload_len &&
         vemb_v16_net_read_full(fd, payload, payload_len) != 0)) {
        close(fd);
        return -1;
    }
    close(fd);
    return 0;
}

static int fetch_stats_tcp(const bench_cfg_t *cfg, vemb_v16_stats_t *stats) {
    return tcp_control_request(cfg,
                               VEMB_V16_NET_STATS,
                               0,
                               VEMB_V16_NET_STATS,
                               stats,
                               sizeof(*stats));
}

static int close_channel_tcp(const bench_cfg_t *cfg, uint64_t channel_id) {
    vemb_v16_net_status_t status;
    if (tcp_control_request(cfg,
                            VEMB_V16_NET_CLOSE_CHANNEL,
                            channel_id,
                            VEMB_V16_NET_CONTROL_STATUS,
                            &status,
                            sizeof(status)) != 0) {
        return -1;
    }
    return status.status == VEMB_V16_STATUS_OK ? 0 : -1;
}

static int alloc_tcp_channel(const bench_cfg_t *cfg, tcp_channel_t *channel) {
    if (!cfg || !channel)
        return -1;
    memset(channel, 0, sizeof(*channel));
    channel->fd = -1;
    int fd = vemb_v16_net_connect(cfg->host, cfg->port, cfg->timeout_ms);
    if (fd < 0) return -1;
    vemb_v16_alloc_req_t req = {.vector_dim = cfg->dim};
    uint8_t req_buf[8];
    const void *payload = &req;
    uint32_t payload_len = (uint32_t)sizeof(req);
    uint32_t net_flags = 0;
    size_t encoded_len = 0;
    if (vemb_v16_alloc_req_encode(req_buf,
                                  sizeof(req_buf),
                                  &req,
                                  &encoded_len) != 0) {
        close(fd);
        return -1;
    }
    payload = req_buf;
    payload_len = (uint32_t)encoded_len;
    if (vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_HELLO,
                                 net_flags,
                                 0,
                                 0,
                                 payload,
                                 payload_len) != 0) {
        close(fd);
        return -1;
    }
    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_WELCOME) {
        close(fd);
        return -1;
    }
    uint8_t *desc_buf = zmalloc(hdr.payload_len);
    if (!desc_buf ||
        hdr.flags != 0 ||
        vemb_v16_net_read_full(fd, desc_buf, hdr.payload_len) != 0 ||
        vemb_v16_channel_desc_decode(&channel->desc,
                                     desc_buf,
                                     hdr.payload_len) != 0) {
        zfree(desc_buf);
        close(fd);
        return -1;
    }
        zfree(desc_buf);
    if (channel->desc.magic != VEMB_V16_MAGIC ||
        channel->desc.version != VEMB_V16_VERSION) {
        close(fd);
        return -1;
    }
    channel->fd = fd;
    return 0;
}

static void close_tcp_channel_local(tcp_channel_t *channel) {
    if (!channel || channel->fd < 0)
        return;
    close(channel->fd);
    channel->fd = -1;
}

static void make_key(char *buf, size_t len, uint32_t id) {
    snprintf(buf, len, "item:%u", id);
}

static void fill_vector(float *vector, uint32_t dim, uint32_t seed) {
    for (uint32_t i = 0; i < dim; i++)
        vector[i] = (float)((seed + i) & 1023u) / 1024.0f;
}

static void prepare_req(vemb_v16_req_t *req,
                        uint8_t op,
                        uint32_t req_id,
                        uint64_t channel_id,
                        const char *key,
                        uint32_t dim) {
    memset(req, 0, sizeof(*req));
    req->op = op;
    req->req_id = req_id;
    req->channel_id = channel_id;
    if (key) {
        req->key_len = (uint32_t)strlen(key);
        req->key_hash = vemb_v16_xxh3_64_str(key, req->key_len);
        memcpy(req->key, key, req->key_len);
    }
    req->dim = dim;
    req->vector_bytes = dim * sizeof(float);
}

static int send_req_tcp(tcp_channel_t *channel,
                        const vemb_v16_req_t *req,
                        size_t req_len) {
    if (!channel || channel->fd < 0)
        return -1;
    size_t encoded_len = vemb_v16_req_encoded_len(req);
    uint8_t *req_buf = NULL;
    uint32_t net_flags = 0;
    (void)req_len;
    req_buf = zmalloc(encoded_len);
    if (!req_buf ||
        vemb_v16_req_encode(req_buf, encoded_len, req, &encoded_len) != 0) {
        zfree(req_buf);
        return -1;
    }
    int rc = vemb_v16_net_write_frame(channel->fd,
                                      VEMB_V16_NET_REQUEST,
                                      net_flags,
                                      channel->desc.channel_id,
                                      req->req_id,
                                      req_buf,
                                      (uint32_t)encoded_len);
    zfree(req_buf);
    return rc;
}

static int recv_resp_tcp(tcp_channel_t *channel,
                         vemb_v16_resp_t *resp,
                         uint8_t *inline_vector,
                         uint32_t inline_vector_cap,
                         uint32_t *inline_vector_bytes) {
    if (!channel || channel->fd < 0 || !resp)
        return -1;
    if (inline_vector_bytes) *inline_vector_bytes = 0;
    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(channel->fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_RESPONSE ||
        hdr.channel_id != channel->desc.channel_id) {
        return -1;
    }
    uint8_t resp_buf[64];
    uint32_t base_bytes = (uint32_t)vemb_v16_resp_encoded_base_len();
    if (hdr.flags != 0 ||
        hdr.payload_len < base_bytes ||
        vemb_v16_net_read_full(channel->fd, resp_buf, base_bytes) != 0) {
        return -1;
    }
    size_t resp_bytes = vemb_v16_resp_encoded_len_for_fields(
        resp_buf[0],
        (uint8_t)(resp_buf[1] & VEMB_V16_TCP_RESP_OP_MASK));
    if (resp_bytes > sizeof(resp_buf) ||
        hdr.payload_len < resp_bytes ||
        vemb_v16_net_read_full(channel->fd,
                               resp_buf + base_bytes,
                               resp_bytes - base_bytes) != 0 ||
        vemb_v16_resp_decode(resp, resp_buf, resp_bytes) != 0) {
        return -1;
    }
    uint32_t extra = hdr.payload_len - (uint32_t)resp_bytes;
    if (extra != 0) {
        if (!inline_vector || extra > inline_vector_cap ||
            vemb_v16_net_read_full(channel->fd, inline_vector, extra) != 0) {
            return -1;
        }
        if (inline_vector_bytes) *inline_vector_bytes = extra;
    }
    return 0;
}

static int prefill_vectors(const bench_cfg_t *cfg, tcp_channel_t *channel) {
    vemb_v16_req_t req;
    vemb_v16_resp_t resp;
    char key[VEMB_V16_MAX_KEY_LEN];
    for (uint32_t i = 0; i < cfg->prefill; i++) {
        make_key(key, sizeof(key), i);
        prepare_req(&req,
                    VEMB_V16_OP_VADD,
                    i + 1,
                    channel->desc.channel_id,
                    key,
                    cfg->dim);
        fill_vector(req.vector, cfg->dim, i);
        if (send_req_tcp(channel,
                         &req,
                         vemb_v16_req_inline_len(req.vector_bytes)) != 0 ||
            recv_resp_tcp(channel, &resp, NULL, 0, NULL) != 0 ||
            resp.status != VEMB_V16_STATUS_OK) {
            return -1;
        }
    }
    return 0;
}

static int cmp_u64(const void *a, const void *b) {
    uint64_t va = *(const uint64_t *)a;
    uint64_t vb = *(const uint64_t *)b;
    if (va < vb) return -1;
    if (va > vb) return 1;
    return 0;
}

static uint64_t percentile(uint64_t *values, uint32_t n, double p) {
    if (!values || n == 0)
        return 0;
    double rank = p * (double)(n - 1);
    uint32_t idx = (uint32_t)(rank + 0.5);
    if (idx >= n) idx = n - 1;
    return values[idx];
}

static void *slow_thread_main(void *arg) {
    slow_thread_arg_t *ctx = arg;
    const bench_cfg_t *cfg = ctx->cfg;
    tcp_channel_t *channel = ctx->channel;
    vemb_v16_req_t req;
    vemb_v16_resp_t resp;
    uint32_t vector_cap = cfg->dim * sizeof(float);
    uint8_t *inline_vector = zmalloc(vector_cap);
    if (!inline_vector) {
        ctx->fail = cfg->slow_ops;
        atomic_store_explicit(ctx->burst_ready, 1, memory_order_release);
        return NULL;
    }

    for (uint32_t i = 0; i < cfg->slow_ops; i++) {
        prepare_req(&req,
                    VEMB_V16_OP_VEMB_INLINE,
                    i + 1,
                    channel->desc.channel_id,
                    ctx->key,
                    cfg->dim);
        if (send_req_tcp(channel,
                         &req,
                         vemb_v16_req_handle_len()) != 0) {
            ctx->fail += cfg->slow_ops - i;
            atomic_store_explicit(ctx->burst_ready, 1, memory_order_release);
            zfree(inline_vector);
            return NULL;
        }
    }

    atomic_store_explicit(ctx->burst_ready, 1, memory_order_release);
    sleep_ms(cfg->stall_ms);

    for (uint32_t i = 0; i < cfg->slow_ops; i++) {
        uint32_t inline_bytes = 0;
        if (recv_resp_tcp(channel, &resp, inline_vector, vector_cap, &inline_bytes) != 0 ||
            resp.status != VEMB_V16_STATUS_OK ||
            inline_bytes != vector_cap) {
            ctx->fail += cfg->slow_ops - i;
            break;
        }
        ctx->ok++;
    }

    zfree(inline_vector);
    return NULL;
}

static void *probe_thread_main(void *arg) {
    probe_thread_arg_t *ctx = arg;
    const bench_cfg_t *cfg = ctx->cfg;
    tcp_channel_t *channel = ctx->channel;
    while (!atomic_load_explicit(ctx->burst_ready, memory_order_acquire))
        sched_yield();

    vemb_v16_req_t req;
    vemb_v16_resp_t resp;
    uint64_t started = now_ns();
    for (uint32_t i = 0; i < cfg->probe_ops; i++) {
        memset(&req, 0, sizeof(req));
        req.op = VEMB_V16_OP_PING;
        req.req_id = i + 1;
        req.channel_id = channel->desc.channel_id;
        uint64_t start = now_ns();
        if (send_req_tcp(channel,
                         &req,
                         vemb_v16_req_handle_len()) != 0 ||
            recv_resp_tcp(channel, &resp, NULL, 0, NULL) != 0 ||
            resp.status != VEMB_V16_STATUS_OK) {
            ctx->fail += cfg->probe_ops - i;
            return NULL;
        }
        ctx->latency_ns[i] = now_ns() - start;
        ctx->ok++;
    }
    ctx->elapsed_ns = now_ns() - started;
    return NULL;
}

static void stats_diff(vemb_v16_stats_t *dst,
                       const vemb_v16_stats_t *after,
                       const vemb_v16_stats_t *before) {
    const uint64_t *a = (const uint64_t *)after;
    const uint64_t *b = (const uint64_t *)before;
    uint64_t *d = (uint64_t *)dst;
    size_t count = sizeof(*dst) / sizeof(uint64_t);
    for (size_t i = 0; i < count; i++)
        d[i] = a[i] - b[i];
}

int main(int argc, char **argv) {
    bench_cfg_t cfg = {
        .host = VEMB_V16_TCP_HOST,
        .port = VEMB_V16_TCP_PORT,
        .dim = 0,
        .prefill = 1024,
        .slow_ops = 8192,
        .probe_ops = 1000,
        .stall_ms = 2000,
        .timeout_ms = 10000,
        .assumed_proxy_io_threads = 1,
    };

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--host") && i + 1 < argc) cfg.host = argv[++i];
        else if (!strcmp(argv[i], "--port") && i + 1 < argc) cfg.port = (uint16_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--dim") && i + 1 < argc) cfg.dim = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--prefill") && i + 1 < argc) cfg.prefill = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--slow-ops") && i + 1 < argc) cfg.slow_ops = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--probe-ops") && i + 1 < argc) cfg.probe_ops = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--stall-ms") && i + 1 < argc) cfg.stall_ms = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--timeout-ms") && i + 1 < argc) cfg.timeout_ms = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--proxy-io-threads") && i + 1 < argc) cfg.assumed_proxy_io_threads = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--help")) {
            printf("usage: %s [--host HOST] [--port PORT] [--dim N] [--prefill N] [--slow-ops N] [--probe-ops N] [--stall-ms N] [--timeout-ms N] [--proxy-io-threads N]\n", argv[0]);
            return 0;
        }
    }

    if (cfg.dim == 0 || cfg.dim > VEMB_V16_MAX_DIM ||
        cfg.assumed_proxy_io_threads == 0 || cfg.slow_ops == 0 ||
        cfg.probe_ops == 0) {
        fprintf(stderr, "invalid arguments\n");
        return 1;
    }

    uint32_t channel_count = cfg.assumed_proxy_io_threads + 1;
    if (channel_count < 2) channel_count = 2;
    if (channel_count > VEMB_V16_MAX_CHANNELS) {
        fprintf(stderr, "too many channels requested\n");
        return 1;
    }

    tcp_channel_t *channels = zcalloc(channel_count * sizeof(*channels));
    uint64_t *latency_ns = zcalloc(cfg.probe_ops * sizeof(*latency_ns));
    pthread_t slow_tid, probe_tid;
    int slow_started = 0;
    int probe_started = 0;
    if (!channels || !latency_ns) {
        zfree(channels);
        zfree(latency_ns);
        fprintf(stderr, "allocation failed\n");
        return 1;
    }
    for (uint32_t i = 0; i < channel_count; i++)
        channels[i].fd = -1;

    for (uint32_t i = 0; i < channel_count; i++) {
        if (alloc_tcp_channel(&cfg, &channels[i]) != 0) {
            fprintf(stderr, "failed to allocate tcp channel %u\n", i);
            goto fail;
        }
    }

    tcp_channel_t *slow = &channels[0];
    tcp_channel_t *probe = &channels[channel_count - 1];
    printf("[setup] slow_channel_id=%llu index=%u probe_channel_id=%llu index=%u assumed_proxy_io_threads=%u\n",
           (unsigned long long)slow->desc.channel_id, slow->desc.channel_index,
           (unsigned long long)probe->desc.channel_id, probe->desc.channel_index,
           cfg.assumed_proxy_io_threads);
    if (probe->desc.channel_index != slow->desc.channel_index + cfg.assumed_proxy_io_threads) {
        printf("[warn] channel indexes are not spaced by proxy_io_threads; same-worker assumption may not hold\n");
    }

    if (prefill_vectors(&cfg, slow) != 0) {
        fprintf(stderr, "prefill failed\n");
        goto fail;
    }

    vemb_v16_stats_t stats_before, stats_after, stats_delta;
    memset(&stats_before, 0, sizeof(stats_before));
    memset(&stats_after, 0, sizeof(stats_after));
    memset(&stats_delta, 0, sizeof(stats_delta));
    if (fetch_stats_tcp(&cfg, &stats_before) != 0) {
        fprintf(stderr, "failed to fetch stats before run\n");
        goto fail;
    }

    atomic_int burst_ready;
    atomic_init(&burst_ready, 0);
    slow_thread_arg_t slow_arg = {
        .cfg = &cfg,
        .channel = slow,
        .burst_ready = &burst_ready,
    };
    probe_thread_arg_t probe_arg = {
        .cfg = &cfg,
        .channel = probe,
        .burst_ready = &burst_ready,
        .latency_ns = latency_ns,
    };
    make_key(slow_arg.key, sizeof(slow_arg.key), 0);

    if (pthread_create(&slow_tid, NULL, slow_thread_main, &slow_arg) != 0) {
        fprintf(stderr, "failed to create worker threads\n");
        goto fail;
    }
    slow_started = 1;
    if (pthread_create(&probe_tid, NULL, probe_thread_main, &probe_arg) != 0) {
        fprintf(stderr, "failed to create worker threads\n");
        goto fail;
    }
    probe_started = 1;
    pthread_join(slow_tid, NULL);
    pthread_join(probe_tid, NULL);

    if (fetch_stats_tcp(&cfg, &stats_after) != 0) {
        fprintf(stderr, "failed to fetch stats after run\n");
        goto fail;
    }
    stats_diff(&stats_delta, &stats_after, &stats_before);

    qsort(latency_ns, cfg.probe_ops, sizeof(*latency_ns), cmp_u64);
    double probe_qps = probe_arg.elapsed_ns ?
        (double)probe_arg.ok * 1e9 / (double)probe_arg.elapsed_ns : 0.0;
    printf("[slow] ok=%llu fail=%llu stall_ms=%u slow_ops=%u\n",
           (unsigned long long)slow_arg.ok,
           (unsigned long long)slow_arg.fail,
           cfg.stall_ms,
           cfg.slow_ops);
    printf("[probe] ok=%llu fail=%llu ops=%u p50=%.3fms p95=%.3fms p99=%.3fms max=%.3fms approx_qps=%.1f\n",
           (unsigned long long)probe_arg.ok,
           (unsigned long long)probe_arg.fail,
           cfg.probe_ops,
           (double)percentile(latency_ns, cfg.probe_ops, 0.50) / 1e6,
           (double)percentile(latency_ns, cfg.probe_ops, 0.95) / 1e6,
           (double)percentile(latency_ns, cfg.probe_ops, 0.99) / 1e6,
           (double)latency_ns[cfg.probe_ops - 1] / 1e6,
           probe_qps);
    printf("[server] delta total=%llu vemb=%llu completed=%llu completion_publish=%llu response_publish=%llu completion_poll=%llu active_channels=%llu\n",
           (unsigned long long)stats_delta.total_requests,
           (unsigned long long)stats_delta.vemb_requests,
           (unsigned long long)stats_delta.completed_jobs,
           (unsigned long long)stats_delta.supernode_completion_publish,
           (unsigned long long)stats_delta.proxy_response_publish,
           (unsigned long long)stats_delta.proxy_completion_poll,
           (unsigned long long)stats_after.active_channels);

    for (uint32_t i = 0; i < channel_count; i++) {
        if (channels[i].desc.channel_id)
            (void)close_channel_tcp(&cfg, channels[i].desc.channel_id);
        close_tcp_channel_local(&channels[i]);
    }
    zfree(channels);
    zfree(latency_ns);
    return (slow_arg.fail == 0 && probe_arg.fail == 0) ? 0 : 1;

fail:
    if (slow_started)
        pthread_join(slow_tid, NULL);
    if (probe_started)
        pthread_join(probe_tid, NULL);
    for (uint32_t i = 0; i < channel_count; i++) {
        if (channels[i].desc.channel_id)
            (void)close_channel_tcp(&cfg, channels[i].desc.channel_id);
        close_tcp_channel_local(&channels[i]);
    }
    zfree(channels);
    zfree(latency_ns);
    return 1;
}
