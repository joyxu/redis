#define _GNU_SOURCE

#include "vemb_v16_server_tcp_client.h"
#include "vemb_v16_net.h"
#include "vemb_v16_protocol.h"

#include <string.h>
#include <unistd.h>
#include <sys/socket.h>

static int g_stc_fd = -1;
static uint64_t g_stc_channel_id = 0;
static uint32_t g_stc_dim = 0;
static uint32_t g_stc_req_id = 1;

/* reconnect state */
static char g_stc_host[64];
static uint16_t g_stc_port = 0;
static int g_stc_reconnecting = 0;

static int stc_do_connect(const char *host, uint16_t port, uint32_t dim) {
    int fd = -1;
    for (int retry = 0; retry < 50; retry++) {
        fd = vemb_v16_net_connect(host, port, 10000);
        if (fd >= 0) break;
        usleep(100000); /* 100ms × 50 = 5s max */
    }
    if (fd < 0) return -1;

    vemb_v16_alloc_req_t req = {.vector_dim = dim, .flags = 0};
    if (vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_HELLO,
                                 0,
                                 0,
                                 0,
                                 &req,
                                 sizeof(req)) != 0) {
        close(fd);
        return -1;
    }

    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_WELCOME ||
        hdr.payload_len != sizeof(vemb_v16_channel_desc_t)) {
        close(fd);
        return -1;
    }

    vemb_v16_channel_desc_t desc;
    memset(&desc, 0, sizeof(desc));
    if (vemb_v16_net_read_full(fd, &desc, sizeof(desc)) != 0 ||
        desc.magic != VEMB_V16_MAGIC ||
        desc.version != VEMB_V16_VERSION) {
        close(fd);
        return -1;
    }

    g_stc_fd = fd;
    g_stc_channel_id = desc.channel_id;
    g_stc_dim = dim;
    g_stc_req_id = 1;
    return 0;
}

static int stc_check_and_reconnect(void) {
    if (g_stc_fd >= 0) {
        int so_error = 0;
        socklen_t len = sizeof(so_error);
        if (getsockopt(g_stc_fd, SOL_SOCKET, SO_ERROR, &so_error, &len) == 0
            && so_error != 0) {
            vemb_v16_stc_cleanup();
        }
    }
    if (g_stc_fd < 0) {
        if (!g_stc_host[0] || g_stc_port == 0) return -1;
        return stc_do_connect(g_stc_host, g_stc_port, g_stc_dim);
    }
    return 0;
}

static int stc_reconnect(void) {
    if (g_stc_reconnecting) return -1; /* avoid nested reconnect */
    if (!g_stc_host[0] || g_stc_port == 0) return -1;

    g_stc_reconnecting = 1;
    vemb_v16_stc_cleanup();
    int rc = stc_do_connect(g_stc_host, g_stc_port, g_stc_dim);
    g_stc_reconnecting = 0;
    return rc;
}

int vemb_v16_stc_init(const char *host, uint16_t port, uint32_t dim) {
    if (g_stc_fd >= 0) return 0;

    strncpy(g_stc_host, host, sizeof(g_stc_host) - 1);
    g_stc_host[sizeof(g_stc_host) - 1] = '\0';
    g_stc_port = port;
    g_stc_dim = dim;

    return stc_do_connect(host, port, dim);
}

int vemb_v16_stc_vadd(const char *key, uint32_t key_len,
                      const float *vector, uint32_t dim,
                      vemb_v16_resp_t *resp) {
    if (!resp) return -1;
    if (stc_check_and_reconnect() != 0) return -1;

    vemb_v16_req_t req = {0};
    req.op = VEMB_V16_OP_VADD;
    req.flags = 0;
    req.req_id = g_stc_req_id++;
    req.channel_id = g_stc_channel_id;
    req.dim = dim;
    req.vector_bytes = dim * sizeof(float);

    if (key_len >= VEMB_V16_MAX_KEY_LEN) key_len = VEMB_V16_MAX_KEY_LEN - 1;
    req.key_len = key_len;
    memcpy(req.key, key, key_len);
    req.key[key_len] = '\0';
    req.key_hash = vemb_v16_murmur3(req.key, key_len);

    if (vector && dim > 0) {
        memcpy(req.vector, vector, dim * sizeof(float));
    }

    uint8_t enc_buf[24u + 4u + VEMB_V16_MAX_KEY_LEN +
                    VEMB_V16_MAX_DIM * sizeof(float) + 64u];
    size_t enc_len = 0;
    if (vemb_v16_req_encode(enc_buf, sizeof(enc_buf), &req, &enc_len) != 0) {
        return -1;
    }
    if (vemb_v16_net_write_frame(g_stc_fd,
                                 VEMB_V16_NET_REQUEST,
                                 0,
                                 g_stc_channel_id,
                                 req.req_id,
                                 enc_buf,
                                 (uint32_t)enc_len) != 0) {
        if (stc_reconnect() != 0) return -1;
        /* retry once after reconnect */
        req.req_id = g_stc_req_id++;
        if (vemb_v16_req_encode(enc_buf, sizeof(enc_buf), &req, &enc_len) != 0) {
            return -1;
        }
        if (vemb_v16_net_write_frame(g_stc_fd,
                                     VEMB_V16_NET_REQUEST,
                                     0,
                                     g_stc_channel_id,
                                     req.req_id,
                                     enc_buf,
                                     (uint32_t)enc_len) != 0) {
            return -1;
        }
    }

    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(g_stc_fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_RESPONSE ||
        hdr.channel_id != g_stc_channel_id ||
        hdr.payload_len < vemb_v16_resp_encoded_base_len()) {
        if (stc_reconnect() != 0) return -1;
        /* retry once after reconnect */
        req.req_id = g_stc_req_id++;
        if (vemb_v16_req_encode(enc_buf, sizeof(enc_buf), &req, &enc_len) != 0 ||
            vemb_v16_net_write_frame(g_stc_fd,
                                     VEMB_V16_NET_REQUEST,
                                     0,
                                     g_stc_channel_id,
                                     req.req_id,
                                     enc_buf,
                                     (uint32_t)enc_len) != 0 ||
            vemb_v16_net_read_header(g_stc_fd, &hdr) != 0 ||
            hdr.type != VEMB_V16_NET_RESPONSE ||
            hdr.channel_id != g_stc_channel_id ||
            hdr.payload_len < vemb_v16_resp_encoded_base_len()) {
            return -1;
        }
    }

    uint8_t resp_buf[128];
    if (hdr.payload_len > sizeof(resp_buf)) return -1;
    if (vemb_v16_net_read_full(g_stc_fd, resp_buf, hdr.payload_len) != 0) {
        return -1;
    }
    if (vemb_v16_resp_decode(resp, resp_buf, hdr.payload_len) != 0) {
        return -1;
    }
    return 0;
}

void vemb_v16_stc_cleanup(void) {
    if (g_stc_fd < 0) return;

    vemb_v16_net_write_frame(g_stc_fd,
                             VEMB_V16_NET_CLOSE_CHANNEL,
                             0,
                             g_stc_channel_id,
                             0,
                             NULL,
                             0);
    close(g_stc_fd);
    g_stc_fd = -1;
    g_stc_channel_id = 0;
}
