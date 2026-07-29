#include "vemb_v16_client_sdk.h"
#include <errno.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * UINT64_C(1000000000) + (uint64_t)ts.tv_nsec;
}

static void build_req(vemb_v16_req_t *req, uint8_t op, uint32_t req_id,
                      uint64_t channel_id, const char *key, uint32_t dim) {
    size_t key_len = strlen(key);
    memset(req, 0, sizeof(*req));
    req->op = op;
    req->req_id = req_id;
    req->channel_id = channel_id;
    req->key_len = (uint32_t)key_len;
    req->key_hash = vemb_v16_xxh3_64_str(key, key_len);
    req->dim = dim;
    req->vector_bytes = dim * sizeof(float);
    memcpy(req->key, key, key_len);
}

static int wait_response(vemb_v16_aeron_channel_t *ch, uint32_t req_id,
                         vemb_v16_resp_t *resp) {
    uint64_t deadline = now_ns() + UINT64_C(5000000000);
    while (now_ns() < deadline) {
        int got = vemb_v16_aeron_poll_response(ch, resp, sizeof(*resp));
        if (got == (int)sizeof(*resp) && resp->req_id == req_id)
            return 0;
    }
    return -1;
}

static int compare_double(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    return da < db ? -1 : da > db ? 1 : 0;
}

int main(int argc, char **argv) {
    const char *host = argc > 1 ? argv[1] : "192.168.90.112";
    uint16_t port = (uint16_t)(argc > 2 ? atoi(argv[2]) : 6391);
    unsigned int warm_path = (unsigned int)(argc > 3 ? atoi(argv[3]) : 1);
    unsigned int req_path = (unsigned int)(argc > 4 ? atoi(argv[4]) : 2);
    const uint32_t dim = 300;
    const uint32_t samples = 1000;
    char key[64];
    float *vector = calloc(dim, sizeof(*vector));
    float *out = calloc(dim, sizeof(*out));
    double *lat_us = calloc(samples, sizeof(*lat_us));
    vemb_v16_req_t *req = calloc(1, sizeof(*req));
    vemb_v16_resp_t resp;
    if (!vector || !out || !lat_us || !req || warm_path < 1 || warm_path > 4 ||
        req_path < 1 || req_path > 4 || warm_path == req_path) {
        fprintf(stderr, "invalid arguments or allocation failure\n");
        return 2;
    }
    for (uint32_t i = 0; i < dim; i++) vector[i] = (float)(i + 1) / dim;
    snprintf(key, sizeof(key), "rtt:path%u", warm_path);

    vemb_v16_aeron_channel_t *ch =
        vemb_v16_aeron_open_remote(host, port, dim);
    if (!ch) {
        fprintf(stderr, "open_remote failed warm_path=%u req_path=%u errno=%d\n",
                warm_path, req_path, errno);
        return 1;
    }
    if (vemb_v16_aeron_open_warm_region(ch) != 0) {
        fprintf(stderr, "open_warm_region failed warm_path=%u req_path=%u errno=%d\n",
                warm_path, req_path, errno);
        vemb_v16_aeron_close(ch);
        return 1;
    }

    build_req(req, VEMB_V16_OP_VADD, 1, vemb_v16_aeron_channel_id(ch),
              key, dim);
    memcpy(req->vector, vector, dim * sizeof(*vector));
    if (vemb_v16_aeron_publish_request(ch, req,
                                       (uint32_t)vemb_v16_req_inline_len(req->vector_bytes)) != 0 ||
        wait_response(ch, 1, &resp) != 0 || resp.status != VEMB_V16_STATUS_OK) {
        fprintf(stderr, "VADD failed warm_path=%u req_path=%u status=%u\n",
                warm_path, req_path, resp.status);
        vemb_v16_aeron_close(ch);
        return 1;
    }

    uint32_t deref_ok = 0;
    for (uint32_t i = 0; i < samples; i++) {
        uint32_t req_id = i + 2;
        build_req(req, VEMB_V16_OP_VEMB_HANDLE, req_id,
                  vemb_v16_aeron_channel_id(ch), key, dim);
        uint64_t start = now_ns();
        if (vemb_v16_aeron_publish_request(ch, req,
                                           (uint32_t)vemb_v16_req_handle_len()) != 0 ||
            wait_response(ch, req_id, &resp) != 0 ||
            resp.status != VEMB_V16_STATUS_OK) {
            fprintf(stderr, "GET failed warm_path=%u req_path=%u id=%u status=%u\n",
                    warm_path, req_path, req_id, resp.status);
            vemb_v16_aeron_close(ch);
            return 1;
        }
        int n = vemb_v16_aeron_read_vector(ch, resp.region_id,
                                           resp.vector_offset, resp.vector_bytes,
                                           out, dim * sizeof(*out));
        uint64_t end = now_ns();
        if (n <= 0) {
            fprintf(stderr, "deref failed warm_path=%u req_path=%u id=%u\n",
                    warm_path, req_path, req_id);
            vemb_v16_aeron_close(ch);
            return 1;
        }
        deref_ok++;
        lat_us[i] = (double)(end - start) / 1000.0;
    }

    qsort(lat_us, samples, sizeof(*lat_us), compare_double);
    double sum = 0.0;
    for (uint32_t i = 0; i < samples; i++) sum += lat_us[i];
    printf("warm_path=%u->client%u req_path=%u->client%u samples=%u "
           "min_us=%.3f avg_us=%.3f p50_us=%.3f p99_us=%.3f "
           "max_us=%.3f deref_ok=%u/%u\n",
           warm_path, warm_path + 4, req_path, req_path + 4, samples,
           lat_us[0], sum / samples, lat_us[samples / 2],
           lat_us[(samples * 99) / 100], lat_us[samples - 1],
           deref_ok, samples);
    vemb_v16_aeron_close(ch);
    free(req);
    free(lat_us);
    free(out);
    free(vector);
    return 0;
}
