#define _GNU_SOURCE

#include "vemb_v16_tcp_transport.h"
#include "vemb_v16_aeron_attach.h"
#include "vemb_v16_log.h"
#include "vemb_v16_net.h"
#include "vemb_v16_proxy_types.h"
#include "redisassert.h"
#include "zmalloc.h"

#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

/// TCP transport implementation.

#define VEMB_V16_TCP_INPUT_INITIAL_CAP (64u * 1024u)
#define VEMB_V16_TCP_INPUT_READ_CHUNK (64u * 1024u)
#define VEMB_V16_TCP_RESPONSE_PREFIX_CAP (sizeof(vemb_v16_net_hdr_t) + 64u)
#define VEMB_V16_TCP_INPUT_BUFFER_LIMIT (4u * 1024u * 1024u)

static int tcp_response_needs_inline_snapshot(const vemb_v16_resp_t *resp) {
    return resp->status == VEMB_V16_STATUS_OK &&
        resp->op == VEMB_V16_OP_VEMB_INLINE &&
        resp->vector_bytes != 0;
}

static int tcp_completion_inline_vector(const vemb_v16_completion_t *completion,
                                        const vemb_v16_resp_t *resp,
                                        const uint8_t **vector,
                                        uint32_t *vector_bytes) {
    *vector = NULL;
    *vector_bytes = 0;
    if (!tcp_response_needs_inline_snapshot(resp))
        return 0;
    if (completion->inline_vector &&
        completion->inline_vector_bytes == resp->vector_bytes) {
        *vector = completion->inline_vector;
        *vector_bytes = completion->inline_vector_bytes;
        return 0;
    }
    serverLog(LL_WARNING,
              "vemb_v16 tcp inline response missing completion snapshot: req_id=%u op=%u status=%u flags=%u resp_vector_bytes=%u inline_vector_bytes=%u region_id=%u local_slot=%u owner_generation=%llu",
              resp->req_id,
              resp->op,
              resp->status,
              resp->flags,
              resp->vector_bytes,
              completion->inline_vector_bytes,
              resp->region_id,
              resp->local_slot,
              (unsigned long long)resp->owner_generation);
    return -1;
}

static void tcp_mark_inline_snapshot_error(vemb_v16_resp_t *resp) {
    resp->status = VEMB_V16_STATUS_ERR;
    resp->vector_bytes = 0;
    resp->vector_offset = 0;
}

int vemb_v16_tcp_listen(vemb_v16_proxy_t *proxy,
                        int backlog,
                        vemb_v16_transport_listener_t *listener) {
    const char *host = vemb_v16_proxy_tcp_host(proxy);
    uint16_t port = vemb_v16_proxy_tcp_port(proxy);
    int fd = vemb_v16_net_listen(host, port, backlog);
    if (fd < 0) {
        serverLog(LL_WARNING, "vemb_v16 tcp listen failed: %s:%u errno=%d error=%s",
                  host, port, errno, strerror(errno));
        return -1;
    }
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);

    *listener = (vemb_v16_transport_listener_t){
        .name = "tcp",
        .fd = fd,
        .handle_fd = vemb_v16_tcp_handle_fd,
    };
    return 0;
}

#ifdef __linux__
/// TCP transport: queue partial response writes when clients apply backpressure.
static int tcp_response_backlog_pending(vemb_v16_channel_t *ch) {
    return vemb_v16_tcp_backlog_pending(ch);
}

static int ensure_tcp_response_backlog_capacity(vemb_v16_channel_t *ch,
                                                size_t append_bytes) {
    size_t pending = vemb_v16_tcp_backlog_pending_bytes(ch);
    if (append_bytes > VEMB_V16_TCP_RESPONSE_BACKLOG_LIMIT ||
        pending > VEMB_V16_TCP_RESPONSE_BACKLOG_LIMIT - append_bytes) {
        return -1;
    }
    vemb_v16_tcp_backlog_compact(ch, pending);
    if (vemb_v16_tcp_backlog_capacity(ch) >= pending + append_bytes)
        return 0;

    size_t next_cap = vemb_v16_tcp_backlog_capacity(ch) ?
        vemb_v16_tcp_backlog_capacity(ch) : 4096u;
    while (next_cap < pending + append_bytes) {
        next_cap <<= 1;
        if (next_cap > VEMB_V16_TCP_RESPONSE_BACKLOG_LIMIT) {
            next_cap = VEMB_V16_TCP_RESPONSE_BACKLOG_LIMIT;
            break;
        }
    }
    if (next_cap < pending + append_bytes)
        return -1;
    uint8_t *next = zrealloc(vemb_v16_tcp_backlog_buffer(ch), next_cap);
    if (!next)
        return -1;
    vemb_v16_tcp_backlog_set_buffer(ch, next, next_cap);
    return 0;
}

static int append_tcp_response_backlog(vemb_v16_channel_t *ch,
                                       const void *buf,
                                       size_t len) {
    RETURN_IF(!buf && len != 0, -1);
    RETURN_IF(ensure_tcp_response_backlog_capacity(ch, len) != 0, -1);
    memcpy(vemb_v16_tcp_backlog_buffer(ch) +
               vemb_v16_tcp_backlog_pending_bytes(ch),
           buf,
           len);
    vemb_v16_tcp_backlog_append_done(ch, len);
    return 0;
}

static int append_tcp_response_backlog_iov(vemb_v16_channel_t *ch,
                                           const struct iovec *iov,
                                           int iovcnt,
                                           size_t skip_bytes) {
    size_t remaining = skip_bytes;
    for (int i = 0; i < iovcnt; i++) {
        const uint8_t *base = (const uint8_t *)iov[i].iov_base;
        size_t len = iov[i].iov_len;
        if (len == 0)
            continue;
        if (remaining >= len) {
            remaining -= len;
            continue;
        }
        base += remaining;
        len -= remaining;
        remaining = 0;
        if (append_tcp_response_backlog(ch, base, len) != 0)
            return -1;
    }
    return remaining == 0 ? 0 : -1;
}

static ssize_t tcp_send_nonblocking(int fd, const void *buf, size_t len) {
    if (len == 0)
        return 0;
    ssize_t n = send(fd, buf, len, MSG_DONTWAIT);
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        return 0;
    return n;
}

static ssize_t tcp_writev_nonblocking(int fd,
                                      const struct iovec *iov,
                                      int iovcnt) {
    ssize_t n = writev(fd, iov, iovcnt);
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        return 0;
    return n;
}

int vemb_v16_tcp_flush_response_backlog(vemb_v16_channel_t *ch) {
    if (vemb_v16_channel_net_fd(ch) < 0 || !tcp_response_backlog_pending(ch))
        return 1;

    size_t pending = vemb_v16_tcp_backlog_pending_bytes(ch);
    ssize_t n = tcp_send_nonblocking(vemb_v16_channel_net_fd(ch),
                                     vemb_v16_tcp_backlog_pending_ptr(ch),
                                     pending);
    if (n < 0)
        return -1;
    vemb_v16_tcp_backlog_consume(ch, (size_t)n);
    if (!tcp_response_backlog_pending(ch)) {
        vemb_v16_tcp_backlog_reset(ch);
        return 1;
    }
    return 0;
}

/// TCP transport: compute one response frame size.
static size_t tcp_response_wire_size(vemb_v16_resp_t *resp,
                                     uint32_t net_flags,
                                     uint32_t vector_bytes) {
    size_t resp_bytes = vemb_v16_resp_encoded_len(resp);
    (void)resp;
    (void)net_flags;
    return sizeof(vemb_v16_net_hdr_t) + resp_bytes + vector_bytes;
}

/// TCP transport: encode one response frame, optionally including inline vector bytes.
static uint8_t *encode_tcp_response_bytes(vemb_v16_channel_t *ch,
                                          vemb_v16_resp_t *resp,
                                          size_t *out_len) {
    uint32_t net_flags = 0;
    const uint8_t *vector = NULL;
    uint32_t vector_bytes = 0;
    if (tcp_response_needs_inline_snapshot(resp)) {
        serverLog(LL_WARNING,
                  "vemb_v16 tcp direct inline response has no completion snapshot: channel_id=%llu req_id=%u op=%u flags=%u vector_bytes=%u",
                  (unsigned long long)vemb_v16_channel_id(ch),
                  resp->req_id,
                  resp->op,
                  resp->flags,
                  resp->vector_bytes);
        tcp_mark_inline_snapshot_error(resp);
    }

    size_t resp_bytes = vemb_v16_resp_encoded_len(resp);
    size_t bytes = tcp_response_wire_size(resp, net_flags, vector_bytes);
    uint8_t *buf = zmalloc(bytes);
    RETURN_IF(!buf, NULL);

    vemb_v16_net_hdr_t hdr = {
        .magic = VEMB_V16_MAGIC,
        .version = VEMB_V16_VERSION,
        .type = VEMB_V16_NET_RESPONSE,
        .flags = net_flags,
        .payload_len = (uint32_t)resp_bytes + vector_bytes,
        .channel_id = vemb_v16_channel_id(ch),
        .req_id = resp->req_id,
    };
    size_t off = 0;
    memcpy(buf + off, &hdr, sizeof(hdr));
    off += sizeof(hdr);
    size_t encoded_len = 0;
    if (vemb_v16_resp_encode(buf + off,
                             bytes - off,
                             resp,
                             &encoded_len) != 0) {
        zfree(buf);
        return NULL;
    }
    off += encoded_len;
    if (vector_bytes) {
        memcpy(buf + off, vector, vector_bytes);
        off += vector_bytes;
    }
    if (out_len) *out_len = off;
    return buf;
}

/// TCP transport: encode a batch of response frames for nonblocking writes.
static uint8_t *encode_tcp_response_batch(vemb_v16_channel_t *ch,
                                          const vemb_v16_completion_t *completions,
                                          uint32_t n,
                                          uint32_t *published,
                                          size_t *out_len) {
    uint32_t net_flags = 0;
    vemb_v16_resp_t responses[PROXY_RESPONSE_BATCH];
    const uint8_t *vectors[PROXY_RESPONSE_BATCH];
    uint32_t vector_bytes[PROXY_RESPONSE_BATCH];
    vemb_v16_net_hdr_t headers[PROXY_RESPONSE_BATCH];
    uint32_t out = 0;
    size_t total_bytes = 0;

    for (uint32_t i = 0; i < n; i++) {
        if (completions[i].channel_id != vemb_v16_channel_id(ch) ||
            !vemb_v16_channel_active(ch)) {
            continue;
        }
        vemb_v16_make_response_from(&responses[out], &completions[i]);
        vectors[out] = NULL;
        vector_bytes[out] = 0;
        if (tcp_completion_inline_vector(&completions[i],
                                         &responses[out],
                                         &vectors[out],
                                         &vector_bytes[out]) != 0)
            tcp_mark_inline_snapshot_error(&responses[out]);
        headers[out] = (vemb_v16_net_hdr_t){
            .magic = VEMB_V16_MAGIC,
            .version = VEMB_V16_VERSION,
            .type = VEMB_V16_NET_RESPONSE,
            .flags = net_flags,
            .payload_len = (uint32_t)vemb_v16_resp_encoded_len(
                &responses[out]) + vector_bytes[out],
            .channel_id = vemb_v16_channel_id(ch),
            .req_id = responses[out].req_id,
        };
        total_bytes += sizeof(headers[out]) + headers[out].payload_len;
        out++;
    }

    if (published) *published = out;
    if (out_len) *out_len = total_bytes;
    if (out == 0)
        return NULL;

    uint8_t *buf = zmalloc(total_bytes);
    if (!buf)
        return NULL;
    size_t off = 0;
    for (uint32_t i = 0; i < out; i++) {
        memcpy(buf + off, &headers[i], sizeof(headers[i]));
        off += sizeof(headers[i]);
        size_t encoded_len = 0;
        if (vemb_v16_resp_encode(buf + off,
                                 total_bytes - off,
                                 &responses[i],
                                 &encoded_len) != 0) {
            zfree(buf);
            return NULL;
        }
        off += encoded_len;
        if (vector_bytes[i]) {
            memcpy(buf + off, vectors[i], vector_bytes[i]);
            off += vector_bytes[i];
        }
    }
    return buf;
}
#endif

/// TCP transport: write one completion response to the socket.
int vemb_v16_tcp_publish_response(vemb_v16_channel_t *ch, vemb_v16_resp_t *resp) {
    if (!vemb_v16_channel_tcp_backpressure_enabled(ch)) {
        uint32_t net_flags = 0;
        uint8_t encoded_resp[64];
        const void *resp_payload = encoded_resp;
        uint32_t resp_payload_len = 0;
        if (tcp_response_needs_inline_snapshot(resp)) {
            serverLog(LL_WARNING,
                      "vemb_v16 tcp direct inline response has no completion snapshot: channel_id=%llu req_id=%u op=%u flags=%u vector_bytes=%u",
                      (unsigned long long)vemb_v16_channel_id(ch),
                      resp->req_id,
                      resp->op,
                      resp->flags,
                      resp->vector_bytes);
            tcp_mark_inline_snapshot_error(resp);
        }
        size_t encoded_len = 0;
        if (vemb_v16_resp_encode(encoded_resp,
                                 sizeof(encoded_resp),
                                 resp,
                                 &encoded_len) != 0) {
            return -1;
        }
        resp_payload_len = (uint32_t)encoded_len;
        return vemb_v16_net_write_frame(vemb_v16_channel_net_fd(ch),
                                        VEMB_V16_NET_RESPONSE,
                                        net_flags,
                                        vemb_v16_channel_id(ch),
                                        resp->req_id,
                                        resp_payload,
                                        resp_payload_len);
    }

#ifdef __linux__
    uint32_t net_flags = 0;
    const uint8_t *vector = NULL;
    uint32_t vector_bytes = 0;
    uint8_t encoded_resp[64];
    const void *resp_payload = encoded_resp;
    uint32_t resp_payload_len = 0;
    if (tcp_response_needs_inline_snapshot(resp)) {
        serverLog(LL_WARNING,
                  "vemb_v16 tcp direct inline response has no completion snapshot: channel_id=%llu req_id=%u op=%u flags=%u vector_bytes=%u",
                  (unsigned long long)vemb_v16_channel_id(ch),
                  resp->req_id,
                  resp->op,
                  resp->flags,
                  resp->vector_bytes);
        tcp_mark_inline_snapshot_error(resp);
    }
    size_t encoded_len = 0;
    if (vemb_v16_resp_encode(encoded_resp,
                             sizeof(encoded_resp),
                             resp,
                             &encoded_len) != 0) {
        return -1;
    }
    resp_payload_len = (uint32_t)encoded_len;

    vemb_v16_net_hdr_t hdr = {
        .magic = VEMB_V16_MAGIC,
        .version = VEMB_V16_VERSION,
        .type = VEMB_V16_NET_RESPONSE,
        .flags = net_flags,
        .payload_len = resp_payload_len + vector_bytes,
        .channel_id = vemb_v16_channel_id(ch),
        .req_id = resp->req_id,
    };
    struct iovec iov[3] = {
        {.iov_base = &hdr, .iov_len = sizeof(hdr)},
        {.iov_base = (void *)resp_payload, .iov_len = resp_payload_len},
        {.iov_base = (void *)vector, .iov_len = vector_bytes},
    };
    size_t total_bytes = sizeof(hdr) + resp_payload_len + vector_bytes;
    if (tcp_response_backlog_pending(ch))
        return append_tcp_response_backlog_iov(ch, iov, 3, 0);

    ssize_t sent = tcp_writev_nonblocking(vemb_v16_channel_net_fd(ch), iov, 3);
    if (sent < 0)
        return -1;
    if ((size_t)sent == total_bytes)
        return 0;
    return append_tcp_response_backlog_iov(ch, iov, 3, (size_t)sent);
#else
    return -1;
#endif
}

/// TCP transport: batch completion responses into writev/backlog output.
int vemb_v16_tcp_publish_response_batch(vemb_v16_channel_t *ch,
                                        const vemb_v16_completion_t *completions,
                                        const uint16_t *completion_indices,
                                        uint32_t ready_count,
                                        uint32_t *published) {
    if (!vemb_v16_channel_tcp_backpressure_enabled(ch)) {
        uint32_t net_flags = 0;
        vemb_v16_resp_t responses[PROXY_RESPONSE_BATCH];
        uint8_t frame_prefixes[PROXY_RESPONSE_BATCH][VEMB_V16_TCP_RESPONSE_PREFIX_CAP];
        struct iovec iov[PROXY_RESPONSE_BATCH * 3u];
        int iovcnt = 0;
        uint32_t out = 0;

        for (uint32_t i = 0; i < ready_count; i++) {
            uint16_t completion_index = completion_indices[i];
            const vemb_v16_completion_t *completion =
                &completions[completion_index];

            vemb_v16_make_response_from(&responses[out], completion);
            const uint8_t *vector = NULL;
            uint32_t vector_bytes = 0;
            if (tcp_completion_inline_vector(completion,
                                             &responses[out],
                                             &vector,
                                             &vector_bytes) != 0)
                tcp_mark_inline_snapshot_error(&responses[out]);

            size_t resp_payload_len = vemb_v16_resp_encoded_len(&responses[out]);
            vemb_v16_net_hdr_t hdr = {
                .magic = VEMB_V16_MAGIC,
                .version = VEMB_V16_VERSION,
                .type = VEMB_V16_NET_RESPONSE,
                .flags = net_flags,
                .payload_len = (uint32_t)resp_payload_len + vector_bytes,
                .channel_id = vemb_v16_channel_id(ch),
                .req_id = responses[out].req_id,
            };
            uint8_t *prefix = frame_prefixes[out];
            memcpy(prefix, &hdr, sizeof(hdr));
            size_t encoded_len = 0;
            if (vemb_v16_resp_encode(prefix + sizeof(hdr),
                                     VEMB_V16_TCP_RESPONSE_PREFIX_CAP - sizeof(hdr),
                                     &responses[out],
                                     &encoded_len) != 0) {
                return -1;
            }
            iov[iovcnt++] = (struct iovec){
                .iov_base = prefix,
                .iov_len = sizeof(hdr) + encoded_len,
            };
            if (vector_bytes) {
                iov[iovcnt++] = (struct iovec){ .iov_base = (void *)vector, .iov_len = vector_bytes };
            }
            out++;
        }

        if (published) *published = out;
        if (out == 0) {
            return 0;
        }
        return vemb_v16_net_writev_full(vemb_v16_channel_net_fd(ch), iov, iovcnt);
    }

#ifdef __linux__
    uint32_t net_flags = 0;
    vemb_v16_resp_t responses[PROXY_RESPONSE_BATCH];
    uint8_t frame_prefixes[PROXY_RESPONSE_BATCH][VEMB_V16_TCP_RESPONSE_PREFIX_CAP];
    struct iovec iov[PROXY_RESPONSE_BATCH * 3u];
    int iovcnt = 0;
    uint32_t out = 0;
    size_t total_bytes = 0;

    for (uint32_t i = 0; i < ready_count; i++) {
        uint16_t completion_index = completion_indices[i];
        const vemb_v16_completion_t *completion =
            &completions[completion_index];

        vemb_v16_make_response_from(&responses[out], completion);
        const uint8_t *vector = NULL;
        uint32_t vector_bytes = 0;
        if (tcp_completion_inline_vector(completion,
                                         &responses[out],
                                         &vector,
                                         &vector_bytes) != 0)
            tcp_mark_inline_snapshot_error(&responses[out]);

        size_t resp_payload_len = vemb_v16_resp_encoded_len(&responses[out]);
        vemb_v16_net_hdr_t hdr = {
            .magic = VEMB_V16_MAGIC,
            .version = VEMB_V16_VERSION,
            .type = VEMB_V16_NET_RESPONSE,
            .flags = net_flags,
            .payload_len = (uint32_t)resp_payload_len + vector_bytes,
            .channel_id = vemb_v16_channel_id(ch),
            .req_id = responses[out].req_id,
        };
        uint8_t *prefix = frame_prefixes[out];
        memcpy(prefix, &hdr, sizeof(hdr));
        size_t encoded_len = 0;
        if (vemb_v16_resp_encode(prefix + sizeof(hdr),
                                 VEMB_V16_TCP_RESPONSE_PREFIX_CAP - sizeof(hdr),
                                 &responses[out],
                                 &encoded_len) != 0) {
            return -1;
        }
        iov[iovcnt++] = (struct iovec){
            .iov_base = prefix,
            .iov_len = sizeof(hdr) + encoded_len,
        };
        resp_payload_len = encoded_len;
        if (vector_bytes) {
            iov[iovcnt++] = (struct iovec){
                .iov_base = (void *)vector,
                .iov_len = vector_bytes,
            };
        }
        total_bytes += sizeof(hdr) + resp_payload_len + vector_bytes;
        out++;
    }

    if (published) *published = out;
    if (out == 0)
        return 0;
    if (tcp_response_backlog_pending(ch))
        return append_tcp_response_backlog_iov(ch, iov, iovcnt, 0);

    ssize_t sent = tcp_writev_nonblocking(vemb_v16_channel_net_fd(ch),
                                          iov,
                                          iovcnt);
    if (sent < 0)
        return -1;
    if ((size_t)sent == total_bytes)
        return 0;
    return append_tcp_response_backlog_iov(ch, iov, iovcnt, (size_t)sent);
#else
    if (published) *published = 0;
    return -1;
#endif
}

static int ensure_tcp_input_tailroom(vemb_v16_channel_t *ch,
                                     size_t min_tailroom) {
    if (vemb_v16_tcp_input_tailroom(ch) >= min_tailroom)
        return 0;

    vemb_v16_tcp_input_compact(ch);
    if (vemb_v16_tcp_input_tailroom(ch) >= min_tailroom)
        return 0;

    size_t pending = vemb_v16_tcp_input_pending_bytes(ch);
    if (min_tailroom > VEMB_V16_TCP_INPUT_BUFFER_LIMIT ||
        pending > VEMB_V16_TCP_INPUT_BUFFER_LIMIT - min_tailroom) {
        return -1;
    }

    size_t needed = pending + min_tailroom;
    size_t next_cap = vemb_v16_tcp_input_tailroom(ch) ?
        pending + vemb_v16_tcp_input_tailroom(ch) :
        VEMB_V16_TCP_INPUT_INITIAL_CAP;
    while (next_cap < needed) {
        next_cap <<= 1;
        if (next_cap > VEMB_V16_TCP_INPUT_BUFFER_LIMIT) {
            next_cap = VEMB_V16_TCP_INPUT_BUFFER_LIMIT;
            break;
        }
    }
    if (next_cap < needed)
        return -1;

    uint8_t *next = zrealloc(vemb_v16_tcp_input_buffer(ch), next_cap);
    if (!next)
        return -1;
    vemb_v16_tcp_input_set_buffer(ch, next, next_cap);
    return 0;
}

static int fill_tcp_input_buffer(vemb_v16_channel_t *ch) {
    if (vemb_v16_channel_net_fd(ch) < 0)
        return -1;
    if (ensure_tcp_input_tailroom(ch, VEMB_V16_TCP_INPUT_READ_CHUNK) != 0)
        return -1;

    for (;;) {
        size_t tailroom = vemb_v16_tcp_input_tailroom(ch);
        size_t read_len = tailroom > VEMB_V16_TCP_INPUT_READ_CHUNK ?
            VEMB_V16_TCP_INPUT_READ_CHUNK : tailroom;
        ssize_t n = recv(vemb_v16_channel_net_fd(ch),
                         vemb_v16_tcp_input_tail_ptr(ch),
                         read_len,
                         MSG_DONTWAIT);
        if (n > 0) {
            vemb_v16_tcp_input_append_done(ch, (size_t)n);
            return 1;
        }
        if (n == 0)
            return -1;
        if (errno == EINTR)
            continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return 0;
        return -1;
    }
}

int vemb_v16_tcp_has_buffered_requests(vemb_v16_channel_t *ch) {
    size_t pending = vemb_v16_tcp_input_pending_bytes(ch);
    if (pending < sizeof(vemb_v16_net_hdr_t))
        return 0;

    vemb_v16_net_hdr_t hdr;
    memcpy(&hdr, vemb_v16_tcp_input_pending_ptr(ch), sizeof(hdr));
    if (hdr.magic != VEMB_V16_MAGIC ||
        hdr.version != VEMB_V16_VERSION ||
        hdr.payload_len > sizeof(vemb_v16_req_t)) {
        return 1;
    }
    return pending >= sizeof(hdr) + (size_t)hdr.payload_len;
}

static int decode_tcp_key_only_request(vemb_v16_req_t *req,
                                       const uint8_t *src,
                                       size_t len) {
    RETURN_IF(len < 24u, -1);
    const uint8_t *p = src;
    uint8_t op_flags = vemb_v16_proto_get_u8(&p);
    uint8_t op = op_flags & VEMB_V16_TCP_REQ_OP_MASK;
    if (op != VEMB_V16_OP_VEMB_INLINE &&
        op != VEMB_V16_OP_VREM &&
        op != VEMB_V16_OP_PING) {
        return 0;
    }

    uint32_t key_len = vemb_v16_proto_get_u8(&p);
    uint32_t dim = vemb_v16_proto_get_u16(&p);
    RETURN_IF(key_len > VEMB_V16_MAX_KEY_LEN || dim > VEMB_V16_MAX_DIM, -1);
    RETURN_IF(len != 24u + (size_t)key_len, -1);
    if (op == VEMB_V16_OP_PING) {
        RETURN_IF(key_len != 0 || dim != 0, -1);
    } else if (op == VEMB_V16_OP_VREM) {
        RETURN_IF(key_len == 0 || dim != 0, -1);
    } else {
        RETURN_IF(key_len == 0 || dim == 0, -1);
    }

    req->op = op;
    req->flags = vemb_v16_req_flags_from_wire(op_flags &
                                              VEMB_V16_TCP_REQ_FLAG_MASK);
    req->reserved0 = 0;
    req->req_id = vemb_v16_proto_get_u32(&p);
    req->channel_id = vemb_v16_proto_get_u64(&p);
    req->key_hash = 0;
    req->key_len = key_len;
    req->key2_len = 0;
    req->key2_hash = 0;
    req->topology_epoch = vemb_v16_proto_get_u64(&p);
    req->dim = dim;
    req->vector_bytes = (op == VEMB_V16_OP_VREM ||
                         op == VEMB_V16_OP_PING) ? 0 : dim * sizeof(float);
    req->reserved1 = 0;
    memcpy(req->key, p, key_len);
    req->key_hash = vemb_v16_xxh3_64_str(req->key, req->key_len);
    return 1;
}

/// TCP transport: decode one buffered request frame into a request slot.
static int channel_read_tcp_request_from_input(vemb_v16_channel_t *ch,
                                               vemb_v16_req_t *req) {
    size_t pending = vemb_v16_tcp_input_pending_bytes(ch);
    if (pending < sizeof(vemb_v16_net_hdr_t))
        return 0;

    const uint8_t *frame = vemb_v16_tcp_input_pending_ptr(ch);
    vemb_v16_net_hdr_t hdr;
    memcpy(&hdr, frame, sizeof(hdr));
    if (hdr.magic != VEMB_V16_MAGIC || hdr.version != VEMB_V16_VERSION)
        return -1;
    if (hdr.type == VEMB_V16_NET_CLOSE)
        return -1;
    if (hdr.type != VEMB_V16_NET_REQUEST ||
        hdr.channel_id != vemb_v16_channel_id(ch) ||
        hdr.flags != 0 ||
        hdr.payload_len == 0 ||
        hdr.payload_len > sizeof(vemb_v16_req_t)) {
        return -1;
    }

    size_t frame_len = sizeof(hdr) + (size_t)hdr.payload_len;
    if (pending < frame_len)
        return 0;

    int decode_rc = decode_tcp_key_only_request(req,
                                                frame + sizeof(hdr),
                                                hdr.payload_len);
    if (decode_rc < 0)
        return -1;
    if (decode_rc == 0) {
        if (vemb_v16_req_decode(req,
                                frame + sizeof(hdr),
                                hdr.payload_len) != 0) {
            return -1;
        }
    }
    if (req->channel_id == 0)
        req->channel_id = vemb_v16_channel_id(ch);
    vemb_v16_tcp_input_consume(ch, frame_len);
    return 1;
}

/// TCP transport: read a bounded batch of already-ready request frames.
int vemb_v16_tcp_read_ready_requests(vemb_v16_channel_t *ch,
                                    uint32_t proxy_io_worker_id) {
    vemb_v16_req_t reqs[PROXY_REQUEST_BATCH];
    const vemb_v16_req_t *req_ptrs[PROXY_REQUEST_BATCH];
    uint32_t count = 0;
    while (count < PROXY_REQUEST_BATCH) {
        int rc = channel_read_tcp_request_from_input(ch,
                                                     &reqs[count]);
        if (rc < 0)
            return -1;
        if (rc == 0)
            break;
        req_ptrs[count] = &reqs[count];
        count++;
    }
    if (count > 0) {
        vemb_v16_proxy_handle_request_ptr_batch(ch,
                                                req_ptrs,
                                                sizeof(vemb_v16_req_t),
                                                count,
                                                proxy_io_worker_id);
        if (vemb_v16_channel_net_fd(ch) < 0)
            return -1;
    }
    if (count >= PROXY_REQUEST_BATCH)
        return (int)count;

    int fill_rc = fill_tcp_input_buffer(ch);
    if (fill_rc < 0)
        return -1;

    uint32_t total = count;
    count = 0;
    while (count < PROXY_REQUEST_BATCH) {
        int rc = channel_read_tcp_request_from_input(ch,
                                                     &reqs[count]);
        if (rc < 0)
            return -1;
        if (rc == 0)
            break;
        req_ptrs[count] = &reqs[count];
        count++;
    }

    if (count > 0) {
        vemb_v16_proxy_handle_request_ptr_batch(ch,
                                                req_ptrs,
                                                sizeof(vemb_v16_req_t),
                                                count,
                                                proxy_io_worker_id);
        if (vemb_v16_channel_net_fd(ch) < 0)
            return -1;
    }

    return (int)(total + count);
}

static void tcp_write_status(int fd, uint8_t status, uint64_t value) {
    vemb_v16_net_status_t st = {
        .status = status,
        .value = value,
    };
    uint8_t payload[16];
    size_t payload_len = 0;
    if (vemb_v16_net_status_encode(payload, sizeof(payload), &st,
                                   &payload_len) == 0) {
        vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_CONTROL_STATUS,
                                 0,
                                 0,
                                 0,
                                 payload,
                                 (uint32_t)payload_len);
    }
}

static void tcp_handle_migration_control(vemb_v16_proxy_t *proxy,
                                         int fd,
                                         uint8_t type,
                                         uint32_t payload_len) {
    vemb_v16_migration_control_req_t req;
    vemb_v16_migration_control_resp_t resp;
    uint8_t *payload = NULL;
    uint8_t resp_buf[96];
    size_t resp_len = 0;
    memset(&req, 0, sizeof(req));
    memset(&resp, 0, sizeof(resp));
    payload = zmalloc(payload_len);
    if (!payload ||
        vemb_v16_net_read_full(fd, payload, payload_len) != 0 ||
        vemb_v16_migration_control_req_decode(&req, payload, payload_len) != 0) {
        zfree(payload);
        close(fd);
        return;
    }
    zfree(payload);
    if (type == VEMB_V16_NET_MIGRATION_MARK_MIGRATING) {
        (void)vemb_v16_proxy_migration_mark_migrating(proxy, &req, &resp);
    } else if (type == VEMB_V16_NET_MIGRATION_MARK_CUTOVER) {
        (void)vemb_v16_proxy_migration_mark_cutover(proxy, &req, &resp);
    } else if (type == VEMB_V16_NET_MIGRATION_MARK_SOURCE_GC) {
        (void)vemb_v16_proxy_migration_mark_source_gc(proxy, &req, &resp);
    } else {
        (void)vemb_v16_proxy_migration_barrier(proxy, &req, &resp);
    }
    if (vemb_v16_migration_control_resp_encode(resp_buf,
                                               sizeof(resp_buf),
                                               &resp,
                                               &resp_len) == 0) {
        vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_MIGRATION_CONTROL_RESPONSE,
                                 0,
                                 0,
                                 0,
                                 resp_buf,
                                 (uint32_t)resp_len);
    }
    close(fd);
}

static void tcp_handle_migration_control_batch(vemb_v16_proxy_t *proxy,
                                               int fd,
                                               uint32_t payload_len) {
    vemb_v16_migration_control_batch_req_t req;
    vemb_v16_migration_control_batch_resp_t resp;
    uint8_t *payload = NULL;
    size_t resp_len = 0;
    memset(&req, 0, sizeof(req));
    memset(&resp, 0, sizeof(resp));
    payload = zmalloc(payload_len);
    if (!payload ||
        vemb_v16_net_read_full(fd, payload, payload_len) != 0 ||
        vemb_v16_migration_control_batch_req_decode(&req,
                                                    payload,
                                                    payload_len) != 0) {
        zfree(payload);
        close(fd);
        return;
    }
    zfree(payload);
    (void)vemb_v16_proxy_migration_mark_migrating_batch(proxy, &req, &resp);
    uint8_t *resp_buf =
        zmalloc(vemb_v16_migration_control_batch_resp_encoded_len(&resp));
    if (resp_buf &&
        vemb_v16_migration_control_batch_resp_encode(
            resp_buf,
            vemb_v16_migration_control_batch_resp_encoded_len(&resp),
            &resp,
            &resp_len) == 0) {
        vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_MIGRATION_CONTROL_BATCH_RESPONSE,
                                 0,
                                 0,
                                 0,
                                 resp_buf,
                                 (uint32_t)resp_len);
    }
    zfree(resp_buf);
    close(fd);
}

static void tcp_handle_migration_range_control(vemb_v16_proxy_t *proxy,
                                               int fd,
                                               uint8_t type,
                                               uint32_t payload_len) {
    vemb_v16_migration_range_control_req_t req;
    vemb_v16_migration_range_control_resp_t resp;
    uint8_t payload[32];
    uint8_t resp_buf[128];
    size_t resp_len = 0;
    memset(&req, 0, sizeof(req));
    memset(&resp, 0, sizeof(resp));
    if (payload_len != sizeof(payload) ||
        vemb_v16_net_read_full(fd, payload, sizeof(payload)) != 0 ||
        vemb_v16_migration_range_control_req_decode(&req,
                                                    payload,
                                                    sizeof(payload)) != 0) {
        close(fd);
        return;
    }
    if (type == VEMB_V16_NET_MIGRATION_RANGE_BARRIER) {
        (void)vemb_v16_proxy_migration_range_barrier(proxy, &req, &resp);
    } else if (type == VEMB_V16_NET_MIGRATION_RANGE_MARK_CUTOVER) {
        (void)vemb_v16_proxy_migration_range_mark_cutover(proxy, &req, &resp);
    } else {
        (void)vemb_v16_proxy_migration_range_mark_source_gc(proxy, &req, &resp);
    }
    if (vemb_v16_migration_range_control_resp_encode(
            resp_buf, sizeof(resp_buf), &resp, &resp_len) == 0) {
        vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_MIGRATION_RANGE_CONTROL_RESPONSE,
                                 0,
                                 0,
                                 0,
                                 resp_buf,
                                 (uint32_t)resp_len);
    }
    close(fd);
}

static void tcp_handle_epoch_control(vemb_v16_proxy_t *proxy,
                                     int fd,
                                     uint8_t type,
                                     uint32_t payload_len) {
    vemb_v16_epoch_control_req_t req;
    vemb_v16_epoch_control_resp_t resp;
    uint8_t req_buf[32];
    uint8_t resp_buf[32];
    size_t resp_len = 0;
    memset(&req, 0, sizeof(req));
    memset(&resp, 0, sizeof(resp));
    if (type == VEMB_V16_NET_EPOCH_SET) {
        if (payload_len != vemb_v16_epoch_control_req_encoded_len() ||
            vemb_v16_net_read_full(fd, req_buf, payload_len) != 0 ||
            vemb_v16_epoch_control_req_decode(&req, req_buf, payload_len) != 0) {
            close(fd);
            return;
        }
        (void)vemb_v16_proxy_epoch_set(proxy, &req, &resp);
    } else {
        if (payload_len != 0) {
            close(fd);
            return;
        }
        (void)vemb_v16_proxy_epoch_get(proxy, &resp);
    }
    if (vemb_v16_epoch_control_resp_encode(resp_buf,
                                           sizeof(resp_buf),
                                           &resp,
                                           &resp_len) == 0) {
        vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_EPOCH_CONTROL_RESPONSE,
                                 0,
                                 0,
                                 0,
                                 resp_buf,
                                 (uint32_t)resp_len);
    }
    close(fd);
}

static void tcp_handle_topology_control(vemb_v16_proxy_t *proxy,
                                        int fd,
                                        uint8_t type,
                                        uint32_t payload_len) {
    vemb_v16_topology_control_req_t req;
    vemb_v16_topology_control_resp_t resp;
    size_t resp_len = 0;
    uint8_t *payload = NULL;
    memset(&req, 0, sizeof(req));
    memset(&resp, 0, sizeof(resp));
    if (type == VEMB_V16_NET_TOPOLOGY_SET) {
        payload = zmalloc(payload_len);
        if (!payload ||
            vemb_v16_net_read_full(fd, payload, payload_len) != 0 ||
            vemb_v16_topology_control_req_decode(&req,
                                                 payload,
                                                 payload_len) != 0) {
            zfree(payload);
            close(fd);
            return;
        }
        zfree(payload);
        (void)vemb_v16_proxy_topology_set(proxy, &req, &resp);
    } else {
        if (payload_len != 0) {
            close(fd);
            return;
        }
        (void)vemb_v16_proxy_topology_get(proxy, &resp);
    }
    uint8_t *resp_buf = zmalloc(vemb_v16_topology_control_resp_encoded_len(
        &resp));
    if (resp_buf &&
        vemb_v16_topology_control_resp_encode(
            resp_buf,
            vemb_v16_topology_control_resp_encoded_len(&resp),
            &resp,
            &resp_len) == 0) {
        vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_TOPOLOGY_RESPONSE,
                                 0,
                                 0,
                                 0,
                                 resp_buf,
                                 (uint32_t)resp_len);
    }
    zfree(resp_buf);
    close(fd);
}

static void tcp_handle_peer_view_map_control(vemb_v16_proxy_t *proxy,
                                             int fd,
                                             uint32_t payload_len) {
    vemb_v16_peer_view_map_req_t req;
    vemb_v16_peer_view_map_resp_t resp;
    memset(&req, 0, sizeof(req));
    memset(&resp, 0, sizeof(resp));
    if (payload_len != sizeof(req) ||
        vemb_v16_net_read_full(fd, &req, sizeof(req)) != 0) {
        close(fd);
        return;
    }
    (void)vemb_v16_proxy_store_peer_view_map(proxy, &req, &resp);
    vemb_v16_net_write_frame(fd,
                             VEMB_V16_NET_PEER_VIEW_MAP_RESPONSE,
                             0,
                             0,
                             0,
                             &resp,
                             sizeof(resp));
    close(fd);
}

static void tcp_handle_peer_view_topology_control(vemb_v16_proxy_t *proxy,
                                                  int fd,
                                                  uint32_t payload_len) {
    vemb_v16_peer_view_topology_control_req_t req;
    vemb_v16_peer_view_topology_control_resp_t resp;
    memset(&req, 0, sizeof(req));
    memset(&resp, 0, sizeof(resp));
    if (payload_len != sizeof(req) ||
        vemb_v16_net_read_full(fd, &req, sizeof(req)) != 0) {
        close(fd);
        return;
    }
    (void)vemb_v16_proxy_apply_peer_view_map_and_topology_set(proxy,
                                                              &req,
                                                              &resp);
    vemb_v16_net_write_frame(fd,
                             VEMB_V16_NET_PEER_VIEW_MAP_TOPOLOGY_RESPONSE,
                             0,
                             0,
                             0,
                             &resp,
                             sizeof(resp));
    close(fd);
}

static void tcp_handle_aeron_alloc_channel(vemb_v16_proxy_t *proxy,
                                           int fd,
                                           uint32_t payload_len) {
    vemb_v16_alloc_req_t req;
    memset(&req, 0, sizeof(req));
    uint8_t payload[8];
    if (payload_len != vemb_v16_alloc_req_encoded_len() ||
        vemb_v16_net_read_full(fd, payload, sizeof(payload)) != 0 ||
        vemb_v16_alloc_req_decode(&req, payload, sizeof(payload)) != 0) {
        close(fd);
        return;
    }
    if (unlikely(req.vector_dim != proxy->vector_dim)) {
        serverLog(LL_WARNING,
                  "aeron channel allocation rejected: client_dim=%u server_dim=%u",
                  req.vector_dim, proxy->vector_dim);
        tcp_write_status(fd, VEMB_V16_STATUS_ERR, 0);
        close(fd);
        return;
    }

    vemb_v16_channel_desc_t desc;
    if (vemb_v16_proxy_alloc_shm_channel(proxy, &desc) != 0) {
        tcp_write_status(fd, VEMB_V16_STATUS_ERR, 0);
        close(fd);
        return;
    }

    size_t desc_len = vemb_v16_channel_desc_encoded_len(&desc);
    uint8_t *desc_buf = zmalloc(desc_len);
    int write_rc = -1;
    if (desc_buf &&
        vemb_v16_channel_desc_encode(desc_buf,
                                     desc_len,
                                     &desc,
                                     &desc_len) == 0) {
        write_rc = vemb_v16_net_write_frame(fd,
                                            VEMB_V16_NET_WELCOME,
                                            0,
                                            desc.channel_id,
                                            0,
                                            desc_buf,
                                            (uint32_t)desc_len);
    }
    zfree(desc_buf);
    if (write_rc != 0)
        vemb_v16_proxy_close_channel_by_id(proxy, desc.channel_id);
    close(fd);
}

/// TCP control plane: process one accepted TCP control or channel setup socket.
void vemb_v16_tcp_handle_fd(vemb_v16_proxy_t *proxy, int fd) {
    assert(proxy != NULL);
    if (fd < 0) return;
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0)
        fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
    vemb_v16_net_set_tcp_nodelay(fd);
    vemb_v16_net_set_timeouts(fd, 10000);

    /* Aeron-TCP control uses the existing cross-node ATTACH ABI.  Keep this
     * sniff before framed control parsing because ATTACH is a raw request,
     * while the same listener also serves framed stats/topology/close calls. */
    char attach_magic[VEMB_V16_AERON_ATTACH_MAGIC_LEN];
    ssize_t attach_peek = recv(fd,
                               attach_magic,
                               sizeof(attach_magic),
                               MSG_PEEK | MSG_WAITALL);
    int attach_v1 = attach_peek == (ssize_t)sizeof(attach_magic) &&
        memcmp(attach_magic, VEMB_V16_AERON_ATTACH_MAGIC,
               VEMB_V16_AERON_ATTACH_MAGIC_LEN) == 0;
    int attach_v2 = attach_peek == (ssize_t)sizeof(attach_magic) &&
        memcmp(attach_magic, VEMB_V16_AERON_ATTACH_V2_MAGIC,
               VEMB_V16_AERON_ATTACH_V2_MAGIC_LEN) == 0;
    if (attach_v1 || attach_v2) {
        if (vemb_v16_proxy_data_transport(proxy) != VEMB_V16_TRANSPORT_AERON) {
            close(fd);
            return;
        }
        if (vemb_v16_net_read_full(fd,
                                   attach_magic,
                                   sizeof(attach_magic)) != 0 ||
            (attach_v2 ?
                vemb_v16_aeron_attach_v2_handle_fd(proxy, fd) :
                vemb_v16_aeron_attach_handle_fd(proxy, fd)) != 0) {
            serverLog(LL_WARNING,
                      "vemb_v16 aeron TCP attach v%d rejected: fd=%d",
                      attach_v2 ? 2 : 1, fd);
        }
        close(fd);
        return;
    }

    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(fd, &hdr) != 0) {
        close(fd);
        return;
    }

    if (hdr.type == VEMB_V16_NET_STATS) {
        if (hdr.payload_len != 0) {
            close(fd);
            return;
        }
        vemb_v16_stats_t stats;
        vemb_v16_proxy_get_stats(proxy, &stats);
        vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_STATS,
                                 0,
                                 0,
                                 0,
                                 &stats,
                                 sizeof(stats));
        close(fd);
        return;
    }

    if (hdr.type == VEMB_V16_NET_DIAGNOSTIC_STATS) {
        if (hdr.payload_len != 0) {
            close(fd);
            return;
        }
        vemb_v16_diagnostic_stats_t stats;
        vemb_v16_proxy_get_diagnostic_stats(proxy, &stats);
        vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_DIAGNOSTIC_STATS,
                                 0,
                                 0,
                                 0,
                                 &stats,
                                 sizeof(stats));
        close(fd);
        return;
    }

    if (hdr.type == VEMB_V16_NET_CLOSE_CHANNEL) {
        if (hdr.payload_len != 0) {
            close(fd);
            return;
        }
        uint8_t status = vemb_v16_proxy_close_channel_by_id(proxy, hdr.channel_id) == 0 ?
            VEMB_V16_STATUS_OK : VEMB_V16_STATUS_ERR;
        tcp_write_status(fd, status, 0);
        close(fd);
        return;
    }

    if (hdr.type == VEMB_V16_NET_CLOSE_ALL_CHANNELS) {
        if (hdr.payload_len != 0) {
            close(fd);
            return;
        }
        uint64_t closed = vemb_v16_proxy_close_all_channels(proxy);
        tcp_write_status(fd, VEMB_V16_STATUS_OK, closed);
        close(fd);
        return;
    }

    if (hdr.type == VEMB_V16_NET_AERON_CHANNEL_STATUS) {
        if (hdr.payload_len != 0) {
            close(fd);
            return;
        }
        uint64_t resource_generation = 0;
        uint8_t status = vemb_v16_proxy_aeron_channel_resource_generation(
            proxy, hdr.channel_id, &resource_generation) == 0 ?
            VEMB_V16_STATUS_OK : VEMB_V16_STATUS_ERR;
        tcp_write_status(fd, status, resource_generation);
        close(fd);
        return;
    }

    if (hdr.type == VEMB_V16_NET_MIGRATION_MARK_MIGRATING ||
        hdr.type == VEMB_V16_NET_MIGRATION_MARK_CUTOVER ||
        hdr.type == VEMB_V16_NET_MIGRATION_MARK_SOURCE_GC ||
        hdr.type == VEMB_V16_NET_MIGRATION_BARRIER) {
        tcp_handle_migration_control(proxy,
                                     fd,
                                     hdr.type,
                                     hdr.payload_len);
        return;
    }

    if (hdr.type == VEMB_V16_NET_MIGRATION_MARK_MIGRATING_BATCH) {
        tcp_handle_migration_control_batch(proxy, fd, hdr.payload_len);
        return;
    }

    if (hdr.type == VEMB_V16_NET_MIGRATION_RANGE_BARRIER ||
        hdr.type == VEMB_V16_NET_MIGRATION_RANGE_MARK_CUTOVER ||
        hdr.type == VEMB_V16_NET_MIGRATION_RANGE_SOURCE_GC) {
        tcp_handle_migration_range_control(proxy,
                                           fd,
                                           hdr.type,
                                           hdr.payload_len);
        return;
    }

    if (hdr.type == VEMB_V16_NET_EPOCH_SET ||
        hdr.type == VEMB_V16_NET_EPOCH_GET) {
        tcp_handle_epoch_control(proxy, fd, hdr.type, hdr.payload_len);
        return;
    }

    if (hdr.type == VEMB_V16_NET_TOPOLOGY_SET ||
        hdr.type == VEMB_V16_NET_TOPOLOGY_GET) {
        tcp_handle_topology_control(proxy, fd, hdr.type, hdr.payload_len);
        return;
    }
    if (hdr.type == VEMB_V16_NET_PEER_VIEW_MAP_APPLY) {
        tcp_handle_peer_view_map_control(proxy, fd, hdr.payload_len);
        return;
    }
    if (hdr.type == VEMB_V16_NET_PEER_VIEW_MAP_TOPOLOGY_SET) {
        tcp_handle_peer_view_topology_control(proxy, fd, hdr.payload_len);
        return;
    }
    if (hdr.type == VEMB_V16_NET_ALLOC_AERON_CHANNEL) {
        if (vemb_v16_proxy_data_transport(proxy) != VEMB_V16_TRANSPORT_AERON) {
            tcp_write_status(fd, VEMB_V16_STATUS_ERR, 0);
            close(fd);
            return;
        }
        tcp_handle_aeron_alloc_channel(proxy, fd, hdr.payload_len);
        return;
    }

    vemb_v16_alloc_req_t req;
    if (vemb_v16_proxy_data_transport(proxy) != VEMB_V16_TRANSPORT_TCP ||
        hdr.type != VEMB_V16_NET_HELLO ||
        hdr.flags != 0 ||
        hdr.payload_len != vemb_v16_alloc_req_encoded_len()) {
        close(fd);
        return;
    }
    memset(&req, 0, sizeof(req));
    uint8_t payload[8];
    if (vemb_v16_net_read_full(fd, payload, sizeof(payload)) != 0 ||
        vemb_v16_alloc_req_decode(&req, payload, sizeof(payload)) != 0) {
        close(fd);
        return;
    }
    if (unlikely(req.vector_dim != proxy->vector_dim)) {
        serverLog(LL_WARNING,
                  "HELLO rejected: client_dim=%u server_dim=%u",
                  req.vector_dim, proxy->vector_dim);
        close(fd);
        return;
    }

    vemb_v16_channel_desc_t desc;
    if (vemb_v16_proxy_alloc_tcp_channel(proxy, fd, &desc) != 0) {
        return;
    }
    size_t welcome_len = vemb_v16_channel_desc_encoded_len(&desc);
    uint8_t *welcome = zmalloc(welcome_len);
    int write_rc = -1;
    if (welcome &&
        vemb_v16_channel_desc_encode(welcome,
                                     welcome_len,
                                     &desc,
                                     &welcome_len) == 0) {
        write_rc = vemb_v16_net_write_frame(fd,
                                            VEMB_V16_NET_WELCOME,
                                            0,
                                            desc.channel_id,
                                            0,
                                            welcome,
                                            (uint32_t)welcome_len);
    }
    zfree(welcome);
    if (write_rc != 0) {
        vemb_v16_proxy_close_channel_by_id(proxy, desc.channel_id);
        return;
    }
    flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0)
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}
