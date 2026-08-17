#include "../clients/c/vemb_v16_client_sdk.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv) {
    const char *host = argc > 1 ? argv[1] : "192.168.90.111";
    uint16_t port = argc > 2 ? (uint16_t)strtoul(argv[2], NULL, 10) : 6395u;
    if (argc < 4) {
        fprintf(stderr, "usage: %s <host> <port> <dim>\n", argv[0]);
        return 2;
    }
    uint32_t dim = (uint32_t)strtoul(argv[3], NULL, 10);
    if (dim == 0 || dim > VEMB_V16_MAX_DIM) {
        fprintf(stderr, "invalid dim %u (range: 1..%u)\n",
                dim, VEMB_V16_MAX_DIM);
        return 2;
    }

    vemb_v16_aeron_channel_t *first =
        vemb_v16_aeron_open_remote(host, port, dim);
    vemb_v16_aeron_channel_t *second =
        vemb_v16_aeron_open_remote(host, port, dim);
    if (!first || !second) {
        fprintf(stderr, "v1 ATTACH failed: %s:%u dim=%u\n",
                host, (unsigned)port, dim);
        vemb_v16_aeron_close(first);
        vemb_v16_aeron_close(second);
        return 1;
    }

    int first_rc = vemb_v16_aeron_publish_request(first, "", 0);
    int second_rc = vemb_v16_aeron_publish_request(second, "", 0);
    if (first_rc != 0 || second_rc != 0) {
        fprintf(stderr, "v1 request ring validation failed: first=%d second=%d\n",
                first_rc, second_rc);
        vemb_v16_aeron_close(first);
        vemb_v16_aeron_close(second);
        return 1;
    }

    printf("vemb_v16_attach_smoke: PASS first_channel_id=%llu "
           "second_channel_id=%llu\n",
           (unsigned long long)vemb_v16_aeron_channel_id(first),
           (unsigned long long)vemb_v16_aeron_channel_id(second));
    vemb_v16_aeron_close(first);
    vemb_v16_aeron_close(second);
    return 0;
}
