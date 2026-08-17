#define _GNU_SOURCE

#include "vemb_v16_cli_tcp.h"
#include "vemb_v16_protocol.h"
#include "vemb_v16_net.h"
#include "../clients/c/vemb_v16_client_sdk.h"

#ifndef REDIS_OK
#define REDIS_OK 0
#define REDIS_ERR 1
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Single SDK client handle. Multi-endpoint routing is owned by the SDK
 * (vemb_v16_client_create_multi builds a consistent-hash ring); redis-cli
 * just forwards operations and does not pick a backend itself. */
static vemb_v16_client_t *g_client  = NULL;
static uint32_t           g_cli_dim = 0;

int vemb_v16_cli_tcp_init_multi(const char **endpoints,
                                int endpoint_count,
                                uint32_t dim) {
    if (endpoint_count <= 0 || dim == 0)
        return -1;

    vemb_v16_cli_tcp_cleanup();

    g_client = vemb_v16_client_create_multi(endpoints, endpoint_count, dim, 0);
    if (!g_client) {
        fprintf(stderr, "vemb_v16_cli_tcp_init_multi: connect failed\n");
        return -1;
    }
    g_cli_dim = dim;
    return 0;
}

int vemb_v16_cli_tcp_init(const char *host, uint16_t port, uint32_t dim) {
    char ep[80];
    snprintf(ep, sizeof(ep), "%s:%u", host, port);
    const char *endpoints[1] = { ep };
    return vemb_v16_cli_tcp_init_multi(endpoints, 1, dim);
}

int vemb_v16_cli_tcp_vadd(int argc, char **argv) {
    if (!g_client || argc < 4) return -1;

    const char *set_name = argv[1];
    int idx = 2;

    /* Skip optional REDUCE dim */
    if (!strcasecmp(argv[idx], "REDUCE")) {
        if (idx + 2 >= argc) return -1;
        idx += 2;
    }

    /* Expect FP32 or VALUES */
    if (idx >= argc) return -1;
    int is_values = !strcasecmp(argv[idx], "VALUES");
    int is_fp32   = !strcasecmp(argv[idx], "FP32");
    if (!is_values && !is_fp32) {
        /* Simplified format: VADD set_name elem_name vector_str */
        const char *elem_name = argv[idx];
        if (idx + 1 >= argc) return -1;
        float *vec = vemb_v16_parse_vector_csv(argv[idx + 1], g_cli_dim);
        if (!vec) return -1;
        int rc = vemb_v16_client_vadd(g_client, set_name, elem_name,
                                      vec, g_cli_dim);
        free(vec);
        if (rc == 0) {
            printf("OK\n");
            return REDIS_OK;
        }
        return REDIS_ERR;
    }

    /* Standard format: VADD set_name VALUES dim v1 v2 ... elem_name */
    if (is_values) {
        if (idx + 2 >= argc) return -1;
        long long vdim = 0;
        char *endptr = NULL;
        vdim = strtoll(argv[idx + 1], &endptr, 10);
        if (endptr == argv[idx + 1] || *endptr != '\0' ||
            unlikely(vdim != (long long)g_cli_dim)) {
            return -1;
        }
        int vec_consumed = 0;
        float *vec = vemb_v16_parse_vector_argv(argv, argc, idx + 2,
                                                 g_cli_dim, &vec_consumed);
        if (!vec) return -1;
        int elem_idx = idx + 2 + vec_consumed;
        if (elem_idx >= argc) {
            free(vec);
            return -1;
        }
        const char *elem_name = argv[elem_idx];
        int rc = vemb_v16_client_vadd(g_client, set_name, elem_name,
                                      vec, g_cli_dim);
        free(vec);
        if (rc == 0) {
            printf("OK\n");
            return REDIS_OK;
        }
        return REDIS_ERR;
    }

    /* FP32 format not supported in fast path */
    return -1;
}

int vemb_v16_cli_tcp_vemb(int argc, char **argv, int raw_output) {
    if (!g_client || argc < 3) return -1;

    const char *set_name  = argv[1];
    const char *elem_name = argv[2];

    float *vec = malloc(g_cli_dim * sizeof(float));
    if (!vec) return -1;

    uint32_t out_dim = 0;
    int rc = vemb_v16_client_vemb_vector(g_client, set_name, elem_name,
                                         vec, g_cli_dim, &out_dim);

    if (rc == 1) {
        if (!raw_output) printf("(nil)\n");
        free(vec);
        return REDIS_OK;
    }
    if (rc != 0) {
        if (!raw_output) printf("(error) VEMB failed\n");
        free(vec);
        return REDIS_ERR;
    }

    if (!raw_output) {
        for (uint32_t i = 0; i < out_dim; i++) {
            printf("%f\n", vec[i]);
        }
    }
    free(vec);
    return REDIS_OK;
}

int vemb_v16_cli_tcp_vemb_pipeline(int argc, char **argv,
                                   int raw_output, int repeat) {
    if (!g_client || argc < 3 || repeat <= 0) return -1;

    if (vemb_v16_client_vemb_repeat(g_client, argv[1], argv[2],
                                    (uint32_t)repeat, 64) != 0) {
        return -1;
    }

    if (!raw_output) {
        printf("OK\n");
    }
    return REDIS_OK;
}

int vemb_v16_cli_tcp_vsim(int argc, char **argv, int raw_output) {
    (void)raw_output;
    if (!g_client || argc < 4) return -1;

    const char *set_name  = argv[1];
    const char *elem_name = argv[2];
    const char *vector_str = argv[3];

    float *vec = vemb_v16_parse_vector_csv(vector_str, g_cli_dim);
    if (!vec) {
        fprintf(stderr, "VSIM vector parse failed\n");
        return -1;
    }

    float score = 0.0f;
    int rc = vemb_v16_client_vsim(g_client, set_name, elem_name,
                                  vec, g_cli_dim, &score);
    free(vec);

    if (rc == 1) {
        printf("(nil)\n");
        return REDIS_OK;
    }
    if (rc != 0) {
        printf("(error) VSIM failed\n");
        return REDIS_ERR;
    }

    printf("%.6f\n", score);
    return REDIS_OK;
}

int vemb_v16_cli_tcp_vsim_pipeline(int argc, char **argv, int raw_output, int repeat) {
    (void)raw_output;
    if (!g_client || argc < 4 || repeat <= 0) return -1;

    const char *set_name  = argv[1];
    const char *elem_name = argv[2];
    const char *vector_str = argv[3];

    float *vec = vemb_v16_parse_vector_csv(vector_str, g_cli_dim);
    if (!vec) {
        fprintf(stderr, "VSIM vector parse failed\n");
        return -1;
    }

    float score = 0.0f;
    int found = 0;
    int rc = vemb_v16_client_vsim_repeat(g_client, set_name, elem_name,
                                         vec, (uint32_t)repeat,
                                         &score, &found, 64);
    free(vec);

    if (rc != 0) {
        return -1;
    }

    if (!raw_output) {
        if (found) {
            printf("%.6f\n", score);
        } else {
            printf("(nil)\n");
        }
    }
    return REDIS_OK;
}

void vemb_v16_cli_tcp_cleanup(void) {
    if (g_client) {
        vemb_v16_client_destroy(g_client);
        g_client = NULL;
    }
    g_cli_dim = 0;
}
