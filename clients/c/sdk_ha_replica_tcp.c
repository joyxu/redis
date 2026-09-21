#include "vemb_v16_client_sdk.h"

#include <errno.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void fill_vector(float *vector, uint32_t dim, uint32_t seed) {
    for (uint32_t i = 0; i < dim; i++)
        vector[i] = (float)(seed + i);
}

static int vector_matches(const float *actual, const float *expected,
                          uint32_t dim) {
    for (uint32_t i = 0; i < dim; i++) {
        if (fabsf(actual[i] - expected[i]) > 0.0001f)
            return 0;
    }
    return 1;
}

static int follower_matches(vemb_v16_client_t *follower,
                            uint32_t dim,
                            uint32_t count,
                            const float *expected) {
    float *actual = calloc(dim, sizeof(*actual));
    if (!actual)
        return -1;
    int ready = 1;
    for (uint32_t i = 0; i < count; i++) {
        char element[32];
        snprintf(element, sizeof(element), "ha-tcp:%u", i);
        int rc = vemb_v16_client_vemb_vector(follower, "ha-tcp", element,
                                             actual, dim, NULL);
        if (i == count - 1u) {
            if (rc != 1) {
                ready = 0;
                break;
            }
        } else if (rc != 0 || !vector_matches(actual,
                                                expected + (size_t)i * dim,
                                                dim)) {
            ready = 0;
            break;
        }
    }
    free(actual);
    return ready ? 0 : 1;
}

static int parse_u32(const char *text, uint32_t *out) {
    char *end = NULL;
    errno = 0;
    unsigned long value = strtoul(text, &end, 10);
    if (errno || end == text || *end != '\0' || value > UINT32_MAX)
        return -1;
    *out = (uint32_t)value;
    return 0;
}

static float *create_expected(uint32_t dim, uint32_t count) {
    float *expected = calloc((size_t)count * dim, sizeof(*expected));
    if (!expected)
        return NULL;
    for (uint32_t i = 0; i < count; i++)
        fill_vector(expected + (size_t)i * dim, dim, 1000u + i);
    fill_vector(expected, dim, 9000u);
    return expected;
}

static int wait_for_follower(vemb_v16_client_t *follower,
                             uint32_t dim,
                             uint32_t count,
                             const float *expected) {
    for (uint32_t attempt = 0; attempt < 600; attempt++) {
        if (follower_matches(follower, dim, count, expected) == 0)
            return 0;
        usleep(100000);
    }
    return -1;
}

static vemb_v16_client_t *create_tcp_client(const char *host,
                                             uint32_t port,
                                             uint32_t dim) {
    char seed[320];
    snprintf(seed, sizeof(seed), "%s:%u", host, port);
    const char *seeds[] = {seed};
    return vemb_v16_client_create(seeds, 1, dim, 5000,
                                  VEMB_V16_TRANSPORT_TCP);
}

static int run_verify(const char *follower_host, uint32_t follower_port,
                      uint32_t dim, uint32_t count) {
    vemb_v16_client_t *follower = create_tcp_client(follower_host,
                                                    follower_port, dim);
    float *expected = create_expected(dim, count);
    if (!follower || !expected) {
        fprintf(stderr, "client or vector allocation failed\n");
        vemb_v16_client_destroy(follower);
        free(expected);
        return 1;
    }
    int rc = wait_for_follower(follower, dim, count, expected);
    vemb_v16_client_destroy(follower);
    free(expected);
    if (rc != 0) {
        fprintf(stderr, "follower did not converge within 60 seconds\n");
        return 1;
    }
    printf("sdk_ha_replica_tcp: VERIFY PASS events=%u follower=%s:%u\n",
           count + 4u, follower_host, follower_port);
    return 0;
}

static int run_write(const char *leader_host, uint32_t leader_port,
                     const char *follower_host, uint32_t follower_port,
                     uint32_t dim, uint32_t count) {
    vemb_v16_client_t *leader = create_tcp_client(leader_host, leader_port,
                                                  dim);
    vemb_v16_client_t *follower = create_tcp_client(follower_host,
                                                    follower_port, dim);
    float *expected = create_expected(dim, count);
    if (!leader || !follower || !expected) {
        fprintf(stderr, "client or vector allocation failed\n");
        vemb_v16_client_destroy(leader);
        vemb_v16_client_destroy(follower);
        free(expected);
        return 1;
    }

    for (uint32_t i = 0; i < count; i++) {
        char element[32];
        float *vector = expected + (size_t)i * dim;
        snprintf(element, sizeof(element), "ha-tcp:%u", i);
        if (i == 0)
            fill_vector(vector, dim, 1000u);
        if (vemb_v16_client_vadd(leader, "ha-tcp", element, vector, dim) != 0) {
            fprintf(stderr, "leader VADD failed at %u\n", i);
            goto failed;
        }
    }
    fill_vector(expected, dim, 9000u);
    char deleted_element[32];
    snprintf(deleted_element, sizeof(deleted_element), "ha-tcp:%u", count - 1u);
    if (vemb_v16_client_vadd(leader, "ha-tcp", "ha-tcp:0", expected, dim) != 0 ||
        vemb_v16_client_vrem(leader, "ha-tcp", "ha-tcp:0") != 0 ||
        vemb_v16_client_vadd(leader, "ha-tcp", "ha-tcp:0", expected, dim) != 0 ||
        vemb_v16_client_vrem(leader, "ha-tcp", deleted_element) != 0) {
        fprintf(stderr, "leader update/delete sequence failed\n");
        goto failed;
    }
    if (wait_for_follower(follower, dim, count, expected) != 0) {
        fprintf(stderr, "follower did not converge within 60 seconds\n");
        goto failed;
    }
    printf("sdk_ha_replica_tcp: PASS events=%u follower=%s:%u\n",
           count + 4u, follower_host, follower_port);
    vemb_v16_client_destroy(leader);
    vemb_v16_client_destroy(follower);
    free(expected);
    return 0;

failed:
    vemb_v16_client_destroy(leader);
    vemb_v16_client_destroy(follower);
    free(expected);
    return 1;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr,
                "usage: %s write <leader-host> <leader-port> <follower-host> "
                "<follower-port> <dim> <count>\n"
                "       %s verify <follower-host> <follower-port> <dim> <count>\n",
                argv[0], argv[0]);
        return 2;
    }
    uint32_t follower_port = 0;
    uint32_t dim = 0;
    uint32_t count = 0;
    if (!strcmp(argv[1], "write") && argc == 8 &&
        parse_u32(argv[5], &follower_port) == 0 &&
        parse_u32(argv[6], &dim) == 0 && parse_u32(argv[7], &count) == 0) {
        uint32_t leader_port = 0;
        if (parse_u32(argv[3], &leader_port) == 0 && leader_port > 0 &&
            leader_port <= UINT16_MAX && follower_port > 0 &&
            follower_port <= UINT16_MAX && dim > 0 && dim <= VEMB_V16_MAX_DIM &&
            count >= 2 && count <= UINT32_MAX - 4u &&
            (size_t)count <= SIZE_MAX / dim)
            return run_write(argv[2], leader_port, argv[4], follower_port,
                             dim, count);
    } else if (!strcmp(argv[1], "verify") && argc == 6 &&
               parse_u32(argv[3], &follower_port) == 0 &&
               parse_u32(argv[4], &dim) == 0 && parse_u32(argv[5], &count) == 0 &&
               follower_port > 0 && follower_port <= UINT16_MAX &&
               dim > 0 && dim <= VEMB_V16_MAX_DIM && count >= 2 &&
               count <= UINT32_MAX - 4u && (size_t)count <= SIZE_MAX / dim) {
        return run_verify(argv[2], follower_port, dim, count);
    }
    fprintf(stderr, "invalid mode, endpoint, dim, or count\n");
    return 2;
}
