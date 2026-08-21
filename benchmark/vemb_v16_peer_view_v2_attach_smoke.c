#include "../clients/c/vemb_v16_client_sdk.h"
#include "../src/vemb_v16_aeron_attach.h"
#include "../src/vemb_v16_client_ring.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

static int parse_u32(const char *text, uint32_t *out) {
    char *end = NULL;
    unsigned long value = strtoul(text, &end, 10);

    if (!text[0] || *end != '\0' || value > UINT32_MAX)
        return -1;
    *out = (uint32_t)value;
    return 0;
}

int main(int argc, char **argv) {
    if (argc != 7) {
        fprintf(stderr,
                "usage: %s <server-host> <port> <manifest> <client-host> "
                "<owner-id> <dim>\n",
                argv[0]);
        return 2;
    }

    uint32_t port = 0, owner_id = 0, dim = 0;
    if (parse_u32(argv[2], &port) != 0 || port == 0 || port > UINT16_MAX ||
        parse_u32(argv[5], &owner_id) != 0 ||
        parse_u32(argv[6], &dim) != 0 || dim == 0 ||
        dim > VEMB_V16_MAX_DIM) {
        fprintf(stderr, "invalid peer-view v2 attach argument\n");
        return 2;
    }

    vemb_v16_ub_peer_view_manifest_t manifest;
    if (vemb_v16_ub_peer_view_manifest_load(argv[3], &manifest) != 0) {
        fprintf(stderr, "failed to load peer-view manifest: %s\n", argv[3]);
        return 1;
    }

    vemb_v16_aeron_batch_channel_t *channel =
        vemb_v16_aeron_open_remote_batch_with_peer_view(
            argv[1], (uint16_t)port, dim, 1,
            VEMB_V16_BATCH_MAX_BYTES_DEFAULT, &manifest, argv[4], owner_id);
    if (!channel) {
        fprintf(stderr, "peer-view v2 ATTACH/mapping failed\n");
        return 1;
    }

    vemb_v16_aeron_batch_resources_t resources;
    if (vemb_v16_aeron_batch_get_resources(channel, &resources) != 0 ||
        !resources.request_descriptor_ring || !resources.request_arena ||
        !resources.response_descriptor_ring || !resources.response_arena ||
        resources.descriptor_slot_size != VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE ||
        resources.descriptor_ring_slots != VEMB_V16_CLIENT_RING_SIZE ||
        resources.effective_batch_size == 0 || resources.max_batch_bytes == 0) {
        fprintf(stderr, "peer-view v2 resources are invalid\n");
        vemb_v16_aeron_batch_close(channel);
        return 1;
    }

    printf("vemb_v16_peer_view_v2_attach_smoke: PASS channel_id=%llu "
           "epoch=%llu batch_size=%u arena_bytes=%u\n",
           (unsigned long long)vemb_v16_aeron_batch_channel_id(channel),
           (unsigned long long)vemb_v16_aeron_batch_topology_epoch(channel),
           resources.effective_batch_size, resources.max_batch_bytes);
    vemb_v16_aeron_batch_close(channel);
    return 0;
}
