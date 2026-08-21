#include "../clients/c/vemb_v16_client_sdk.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int parse_u32(const char *text, uint32_t *out) {
    char *end = NULL;
    unsigned long value = strtoul(text, &end, 10);

    if (!text[0] || *end != '\0' || value > UINT32_MAX)
        return -1;
    *out = (uint32_t)value;
    return 0;
}

static int wait_response(vemb_v16_aeron_channel_t *channel,
                         const vemb_v16_req_t *request, uint32_t timeout_ms,
                         vemb_v16_resp_t *out_response) {
    uint8_t wire[VEMB_V16_AERON_REQ_WIRE_MAX_LEN];
    size_t wire_len = 0;

    if (vemb_v16_req_encode(wire, sizeof(wire), request, &wire_len) != 0)
        return -1;
    for (uint32_t elapsed_ms = 0; elapsed_ms < timeout_ms; elapsed_ms++) {
        int publish_rc = vemb_v16_aeron_publish_request(
            channel, wire, (uint32_t)wire_len);
        if (publish_rc == 0)
            break;
        if (publish_rc != -1)
            return -1;
        struct timespec pause = {0, 1000000};
        nanosleep(&pause, NULL);
        if (elapsed_ms + 1 == timeout_ms)
            return -1;
    }

    for (uint32_t elapsed_ms = 0; elapsed_ms < timeout_ms; elapsed_ms++) {
        int poll_rc = vemb_v16_aeron_poll_response(channel, out_response,
                                                    sizeof(*out_response));
        if (poll_rc < 0)
            return -1;
        if (poll_rc > 0) {
            return out_response->status == VEMB_V16_STATUS_OK &&
                out_response->op == request->op &&
                out_response->req_id == request->req_id ? 0 : -1;
        }
        struct timespec pause = {0, 1000000};
        nanosleep(&pause, NULL);
    }
    return -1;
}

static void make_request(vemb_v16_req_t *request, uint8_t op,
                         uint32_t req_id, uint64_t channel_id,
                         const char *key, uint32_t dim, const float *vector) {
    size_t key_len = strlen(key);

    *request = (vemb_v16_req_t){
        .op = op,
        .req_id = req_id,
        .channel_id = channel_id,
        .key_hash = vemb_v16_xxh3_64_str(key, key_len),
        .key_len = (uint32_t)key_len,
        .topology_epoch = 0,
        .dim = dim,
        .vector_bytes = dim * sizeof(float),
    };
    memcpy(request->key, key, key_len);
    if (op == VEMB_V16_OP_VADD)
        memcpy(request->vector, vector, request->vector_bytes);
}

int main(int argc, char **argv) {
    if (argc < 7 || argc > 9) {
        fprintf(stderr,
                "usage: %s <server-host> <port> <manifest> <client-host> "
                "<owner-id> <dim> [cycles] [timeout-ms]\n",
                argv[0]);
        return 2;
    }

    uint32_t port = 0, owner_id = 0, dim = 0, cycles = 2, timeout_ms = 10000;
    if (parse_u32(argv[2], &port) != 0 || port == 0 || port > UINT16_MAX ||
        parse_u32(argv[5], &owner_id) != 0 ||
        parse_u32(argv[6], &dim) != 0 || dim == 0 ||
        dim > VEMB_V16_MAX_DIM ||
        (argc >= 8 && parse_u32(argv[7], &cycles) != 0) || cycles == 0 ||
        (argc == 9 && parse_u32(argv[8], &timeout_ms) != 0) || timeout_ms == 0) {
        fprintf(stderr, "invalid peer-view smoke argument\n");
        return 2;
    }

    vemb_v16_ub_peer_view_manifest_t manifest;
    if (vemb_v16_ub_peer_view_manifest_load(argv[3], &manifest) != 0) {
        fprintf(stderr, "failed to load peer-view manifest: %s\n", argv[3]);
        return 1;
    }

    float *expected = calloc(dim, sizeof(*expected));
    float *actual = calloc(dim, sizeof(*actual));
    if (!expected || !actual) {
        fprintf(stderr, "out of memory\n");
        free(expected);
        free(actual);
        return 1;
    }

    uint64_t previous_generation = 0;
    for (uint32_t cycle = 0; cycle < cycles; cycle++) {
        vemb_v16_aeron_channel_t *channel =
            vemb_v16_aeron_open_remote_with_peer_view(
                argv[1], (uint16_t)port, dim, &manifest, argv[4], owner_id);
        if (!channel || vemb_v16_aeron_open_warm_region(channel) != 0) {
            fprintf(stderr, "peer-view ATTACH/warm map failed: cycle=%u\n", cycle);
            vemb_v16_aeron_close(channel);
            free(expected);
            free(actual);
            return 1;
        }

        uint64_t channel_id = vemb_v16_aeron_channel_id(channel);
        uint64_t generation =
            vemb_v16_aeron_channel_resource_generation(channel);
        if (channel_id == 0 || generation == 0 ||
            (cycle != 0 && generation == previous_generation)) {
            fprintf(stderr,
                    "invalid reattach generation: cycle=%u channel=%llu generation=%llu previous=%llu\n",
                    cycle, (unsigned long long)channel_id,
                    (unsigned long long)generation,
                    (unsigned long long)previous_generation);
            vemb_v16_aeron_close(channel);
            free(expected);
            free(actual);
            return 1;
        }
        previous_generation = generation;

        char key[128];
        int key_len = snprintf(key, sizeof(key), "stage5-peer-view-%ld-%u",
                               (long)getpid(), cycle);
        if (key_len <= 0 || (size_t)key_len >= sizeof(key)) {
            vemb_v16_aeron_close(channel);
            free(expected);
            free(actual);
            return 1;
        }
        for (uint32_t i = 0; i < dim; i++)
            expected[i] = (float)(cycle * 100u + i) / 100.0f;

        vemb_v16_req_t request;
        vemb_v16_resp_t response;
        make_request(&request, VEMB_V16_OP_VADD, cycle * 2 + 1, channel_id,
                     key, dim, expected);
        if (wait_response(channel, &request, timeout_ms, &response) != 0) {
            fprintf(stderr, "VADD publish/response identity failed: cycle=%u\n",
                    cycle);
            vemb_v16_aeron_close(channel);
            free(expected);
            free(actual);
            return 1;
        }

        make_request(&request, VEMB_V16_OP_VEMB_HANDLE, cycle * 2 + 2,
                     channel_id, key, dim, NULL);
        if (wait_response(channel, &request, timeout_ms, &response) != 0 ||
            response.vector_bytes != dim * sizeof(float) ||
            response.region_id == 0 ||
            vemb_v16_aeron_read_vector(channel, response.region_id,
                                       response.vector_offset,
                                       response.vector_bytes, actual,
                                       dim * sizeof(float)) !=
                (int)(dim * sizeof(float)) ||
            memcmp(expected, actual, dim * sizeof(float)) != 0) {
            fprintf(stderr,
                    "VEMB_HANDLE response identity or warm read failed: cycle=%u\n",
                    cycle);
            vemb_v16_aeron_close(channel);
            free(expected);
            free(actual);
            return 1;
        }
        printf("cycle=%u channel_id=%llu resource_generation=%llu ok\n",
               cycle, (unsigned long long)channel_id,
               (unsigned long long)generation);
        vemb_v16_aeron_close(channel);
    }

    printf("vemb_v16_peer_view_v1_smoke: PASS cycles=%u dim=%u\n", cycles,
           dim);
    free(expected);
    free(actual);
    return 0;
}
