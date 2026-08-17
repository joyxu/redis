/*
 * sdk_multi_region_correctness.c — Prove Aeron multi-region reads are correct.
 *
 * 1. TCP prefill: write N keys with deterministic vector content
 * 2. Aeron read: VEMB_HANDLE each key, dereference via warm region mmap
 * 3. Compare returned vector to expected (byte-exact)
 * 4. Tally region_id distribution (proves vnode ratio is real)
 *
 * Usage: sdk_multi_region_correctness [host] [port] <dim> [num_keys]
 * Defaults: 127.0.0.1 6390 10000 (dim is required)
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include "vemb_v16_client_sdk.h"
#include "vemb_v16_hash.h"

#define DEFAULT_KEYS    10000
#define UDS_PATH        "/tmp/vemb_v16.sock"
#define MAX_REGION_SLOTS 256

/* Deterministic vector from key index — identical to what TCP VADD writes. */
static void gen_expected_vector(float *vec, uint32_t dim, uint32_t key_idx) {
    for (uint32_t j = 0; j < dim; j++)
        vec[j] = (float)((key_idx * 31u + j * 7u) % 1000u) / 1000.0f;
}

int main(int argc, char **argv) {
    const char *host     = argc > 1 ? argv[1] : "127.0.0.1";
    uint16_t    port     = (uint16_t)(argc > 2 ? atoi(argv[2]) : 6390);
    if (argc < 4) {
        fprintf(stderr, "usage: %s [host] [port] <dim> [num_keys]\n", argv[0]);
        return 2;
    }
    uint32_t    dim      = (uint32_t)atoi(argv[3]);
    uint32_t    num_keys = (uint32_t)(argc > 4 ? atoi(argv[4]) : DEFAULT_KEYS);

    if (dim == 0 || dim > VEMB_V16_MAX_DIM) {
        fprintf(stderr, "invalid dim %u\n", dim);
        return 2;
    }

    /* ---- buffers ---- */
    float *vec_out = (float *)malloc((size_t)dim * sizeof(float));
    float *vec_exp = (float *)malloc((size_t)dim * sizeof(float));
    vemb_v16_req_t *req = (vemb_v16_req_t *)calloc(1, sizeof(*req));
    if (!vec_out || !vec_exp || !req) {
        fprintf(stderr, "oom\n");
        return 2;
    }

    /* ================================================================ */
    /*  Phase 1: TCP prefill — write N keys with known vector content   */
    /* ================================================================ */
    printf("Phase 1: TCP prefill %u keys (dim=%u) to %s:%u\n",
           num_keys, dim, host, port);

    vemb_v16_client_t *tcp = vemb_v16_client_create(host, port, dim, 10000);
    if (!tcp) {
        fprintf(stderr, "FAIL: TCP connect to %s:%u\n", host, port);
        return 1;
    }

    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    uint32_t prefill_fail = 0;
    for (uint32_t i = 0; i < num_keys; i++) {
        char key[32];
        int kl = snprintf(key, sizeof(key), "item:%u", i + 1);
        gen_expected_vector(vec_exp, dim, i);
        if (vemb_v16_client_vadd(tcp, NULL, key, vec_exp, dim) != 0) {
            prefill_fail++;  /* set-associative hash collision — tolerate */
        }
        (void)kl;
    }

    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double prefill_s = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
    printf("  Prefill done in %.1f s (%.0f keys/s) — %u failures tolerated\n",
           prefill_s, num_keys / prefill_s, prefill_fail);

    /* ================================================================ */
    /*  Phase 2: Aeron read — VEMB_HANDLE each key, dereference via mmap */
    /* ================================================================ */
    printf("Phase 2: Aeron VEMB_HANDLE read-back + content verification\n");

    vemb_v16_aeron_channel_t *ch = vemb_v16_aeron_open(UDS_PATH, dim);
    if (!ch) {
        fprintf(stderr, "FAIL: Aeron open %s\n", UDS_PATH);
        vemb_v16_client_destroy(tcp);
        return 1;
    }

    if (vemb_v16_aeron_open_warm_region(ch) != 0) {
        fprintf(stderr, "FAIL: Aeron open warm region(s)\n");
        vemb_v16_aeron_close(ch);
        vemb_v16_client_destroy(tcp);
        return 1;
    }
    printf("  Aeron channel open, warm region(s) mapped OK\n");

    vemb_v16_resp_t resp;
    uint32_t content_match = 0, content_mismatch = 0;
    uint32_t not_found = 0, deref_fail = 0, poll_timeout = 0;
    uint32_t status_err = 0, status_moved = 0, status_ask = 0, status_stale = 0, status_other = 0;
    uint32_t region_dist[MAX_REGION_SLOTS];
    memset(region_dist, 0, sizeof(region_dist));

    /* Pipelined read: sliding window of WINDOW in-flight requests.
     * The SHM ring is SPSC + finite; backpressure naturally caps in-flight.
     * Interleave publish + poll so the proxy threads stay fed. */
    const uint32_t WINDOW = 128;
    uint32_t next_publish = 1;       /* next req_id to publish */
    uint32_t next_expect  = 1;       /* next req_id we want to consume (in-order) */
    uint32_t verified     = 0;

    clock_gettime(CLOCK_MONOTONIC, &t0);

    while (next_expect <= num_keys) {
        /* Publish as many as the window allows */
        while (next_publish <= num_keys &&
               (next_publish - next_expect) < WINDOW) {
            char key[32];
            uint32_t key_len = (uint32_t)snprintf(key, sizeof(key), "item:%u", next_publish);
            memset(req, 0, offsetof(vemb_v16_req_t, key));
            req->op           = VEMB_V16_OP_VEMB_HANDLE;
            req->req_id       = next_publish;
            req->channel_id   = vemb_v16_aeron_channel_id(ch);
            req->key_hash     = vemb_v16_xxh3_64_str(key, key_len);
            req->key_len      = key_len;
            req->dim          = dim;
            req->vector_bytes = dim * sizeof(float);
            memcpy(req->key, key, key_len);
            uint8_t wire[VEMB_V16_AERON_REQ_WIRE_MAX_LEN];
            size_t wire_len = 0;
            if (vemb_v16_req_encode(wire, sizeof(wire), req, &wire_len) != 0 ||
                vemb_v16_aeron_publish_request(ch, wire,
                                                (uint32_t)wire_len) != 0) {
                /* Ring full — break to drain */
                break;
            }
            next_publish++;
        }

        /* Poll for any response (may be out-of-order due to multi-worker) */
        int got = vemb_v16_aeron_poll_response(ch, &resp, sizeof(resp));
        if (got <= 0) {
            /* No response yet — yield briefly and retry */
            struct timespec ts = {0, 200};
            nanosleep(&ts, NULL);
            continue;
        }

        /* resp.req_id tells us which key this is for */
        uint32_t idx = resp.req_id;  /* 1-based */
        if (idx == 0 || idx > num_keys) {
            /* Stale from prior run — drop */
            continue;
        }

        /* Debug: first 3 */
        if (idx <= 3) {
            printf("  [debug] key=item:%u status=%u op=%u region_id=%u offset=%llu bytes=%u dim=%u\n",
                   idx, resp.status, resp.op, resp.region_id,
                   (unsigned long long)resp.vector_offset, resp.vector_bytes,
                   resp.dim);
        }

        if (resp.status != VEMB_V16_STATUS_OK) {
            switch (resp.status) {
            case VEMB_V16_STATUS_NOT_FOUND:     not_found++; break;
            case VEMB_V16_STATUS_ERR:           status_err++; break;
            case VEMB_V16_STATUS_MOVED:         status_moved++; break;
            case VEMB_V16_STATUS_ASK:           status_ask++; break;
            case VEMB_V16_STATUS_STALE_TOPOLOGY: status_stale++; break;
            default:                            status_other++; break;
            }
            next_expect++;
            continue;
        }

        if (resp.region_id < MAX_REGION_SLOTS)
            region_dist[resp.region_id]++;
        else
            region_dist[MAX_REGION_SLOTS - 1]++;

        int n = vemb_v16_aeron_read_vector(ch, resp.region_id,
                                           resp.vector_offset,
                                           resp.vector_bytes,
                                           vec_out,
                                           (uint32_t)(dim * sizeof(float)));
        if (n <= 0) { deref_fail++; next_expect++; continue; }

        gen_expected_vector(vec_exp, dim, idx - 1);
        if (memcmp(vec_out, vec_exp, (size_t)dim * sizeof(float)) == 0)
            content_match++;
        else
            content_mismatch++;

        verified++;
        if (verified % 10000 == 0)
            printf("  ... %u/%u keys verified\n", verified, num_keys);

        next_expect++;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double read_s = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;

    /* ================================================================ */
    /*  Phase 3: Summary                                                */
    /* ================================================================ */
    printf("\n========== Correctness Summary ==========\n");
    printf("Total keys:       %u\n", num_keys);
    printf("Content match:    %u  (%.2f%%)\n", content_match,
           num_keys ? 100.0 * content_match / num_keys : 0.0);
    printf("Content mismatch: %u  (%.2f%%)\n", content_mismatch,
           num_keys ? 100.0 * content_mismatch / num_keys : 0.0);
    printf("Not found:        %u\n", not_found);
    printf("Deref failed:     %u\n", deref_fail);
    printf("Poll timeout:     %u\n", poll_timeout);
    printf("Status ERR:       %u\n", status_err);
    printf("Status MOVED:     %u\n", status_moved);
    printf("Status ASK:       %u\n", status_ask);
    printf("Status STALE:     %u\n", status_stale);
    printf("Status OTHER:     %u\n", status_other);
    printf("Read time:        %.1f s  (%.0f keys/s)\n",
           read_s, num_keys / read_s);

    printf("\n========== Region Distribution (ratio proof) ==========\n");
    for (uint32_t r = 0; r < MAX_REGION_SLOTS; r++) {
        if (region_dist[r] > 0)
            printf("  region_id=%-4u  %u keys  (%.2f%%)\n",
                   r, region_dist[r],
                   num_keys ? 100.0 * region_dist[r] / num_keys : 0.0);
    }

    int all_ok = (content_match == num_keys);
    printf("\n>>> %s <<<\n",
           all_ok ? "PASS: all keys content-correct" : "FAIL: see mismatch count above");

    vemb_v16_aeron_close(ch);
    vemb_v16_client_destroy(tcp);
    free(vec_out);
    free(vec_exp);
    free(req);
    return all_ok ? 0 : 1;
}
