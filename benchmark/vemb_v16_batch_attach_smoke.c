#include "../clients/c/vemb_v16_client_sdk.h"
#include "../src/vemb_v16_aeron_attach.h"
#include "../src/vemb_v16_client_ring.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

int main(int argc, char **argv) {
    const char *host = argc > 1 ? argv[1] : "192.168.90.111";
    uint16_t port = argc > 2 ? (uint16_t)strtoul(argv[2], NULL, 10) : 6395u;
    uint32_t dim = argc > 3 ? (uint32_t)strtoul(argv[3], NULL, 10) : 16u;
    uint32_t requested_batch_size = argc > 4 ?
        (uint32_t)strtoul(argv[4], NULL, 10) : 64u;
    uint32_t requested_max_batch_bytes = argc > 5 ?
        (uint32_t)strtoul(argv[5], NULL, 10) : 32768u;

    vemb_v16_aeron_batch_channel_t *channel =
        vemb_v16_aeron_open_remote_batch(host, port, dim,
                                          requested_batch_size,
                                          requested_max_batch_bytes);
    if (!channel) {
        fprintf(stderr, "v2 batch ATTACH failed: %s:%u dim=%u\n",
                host, (unsigned)port, dim);
        return 1;
    }

    vemb_v16_aeron_batch_resources_t resources;
    vemb_v16_aeron_batch_get_resources(channel, &resources);
    if (!resources.request_descriptor_ring || !resources.request_arena ||
        !resources.response_descriptor_ring || !resources.response_arena ||
        resources.descriptor_slot_size != VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE ||
        resources.descriptor_ring_slots != VEMB_V16_CLIENT_RING_SIZE ||
        resources.effective_batch_size == 0 || resources.max_batch_bytes == 0 ||
        resources.max_batch_bytes % CACHELINE_SIZE != 0) {
        fprintf(stderr, "v2 batch ATTACH returned invalid resource mappings\n");
        vemb_v16_aeron_batch_close(channel);
        return 1;
    }

    const vemb_v16_client_ring_t *request_ring =
        resources.request_descriptor_ring;
    const vemb_v16_client_ring_t *response_ring =
        resources.response_descriptor_ring;
    if (request_ring->slot_size != resources.descriptor_slot_size ||
        request_ring->slot_count != resources.descriptor_ring_slots ||
        response_ring->slot_size != resources.descriptor_slot_size ||
        response_ring->slot_count != resources.descriptor_ring_slots) {
        fprintf(stderr,
                "v2 batch descriptor ring header mismatch: expected slot_size=%u "
                "slot_count=%u; request={slot_size=%u slot_count=%u slot_mask=%u "
                "slots_off=%" PRIu64 "}; response={slot_size=%u slot_count=%u "
                "slot_mask=%u slots_off=%" PRIu64 "}\n",
                resources.descriptor_slot_size, resources.descriptor_ring_slots,
                request_ring->slot_size, request_ring->slot_count,
                request_ring->slot_mask, request_ring->slots_off,
                response_ring->slot_size, response_ring->slot_count,
                response_ring->slot_mask, response_ring->slots_off);
        vemb_v16_aeron_batch_close(channel);
        return 1;
    }

    const char *keys[] = {"v2-batch-smoke-a", "v2-batch-smoke-b"};
    const uint16_t key_lens[] = {
        (uint16_t)strlen(keys[0]), (uint16_t)strlen(keys[1]),
    };
    vemb_v16_resp_t entries[2];
    uint64_t response_batch_id = 0, response_epoch = 0;
    for (uint64_t batch_id = 1; batch_id <= 4; batch_id++) {
        if (vemb_v16_aeron_batch_publish_handle(channel, batch_id, keys,
                                                 key_lens, 2) != 0) {
            fprintf(stderr, "v2 batch publish failed: batch_id=%" PRIu64 "\n",
                    batch_id);
            vemb_v16_aeron_batch_close(channel);
            return 1;
        }
    }
    uint8_t seen[5] = {0};
    for (uint32_t received = 0; received < 4; received++) {
        int got = 0;
        for (uint32_t spins = 0; spins < 10000; spins++) {
            got = vemb_v16_aeron_batch_poll_response(
                channel, &response_batch_id, &response_epoch, entries);
            if (got != 0)
                break;
            struct timespec pause = {0, 1000000};
            nanosleep(&pause, NULL);
        }
        if (got != 2 || response_batch_id == 0 || response_batch_id > 4 ||
            seen[response_batch_id] ||
            response_epoch != vemb_v16_aeron_batch_topology_epoch(channel) ||
            entries[0].req_id != 0 || entries[1].req_id != 1 ||
            entries[0].op != VEMB_V16_OP_VEMB_HANDLE ||
            entries[1].op != VEMB_V16_OP_VEMB_HANDLE) {
            fprintf(stderr, "v2 batch response invalid: got=%d batch_id=%" PRIu64
                    " epoch=%" PRIu64 " entries={%u/%u,%u/%u}\n", got,
                    response_batch_id, response_epoch, entries[0].status,
                    entries[0].req_id, entries[1].status, entries[1].req_id);
            vemb_v16_aeron_batch_close(channel);
            return 1;
        }
        seen[response_batch_id] = 1;
    }

    printf("vemb_v16_batch_attach_smoke: PASS channel_id=%" PRIu64
           " epoch=%" PRIu64 " effective_batch_size=%u max_batch_bytes=%u\n",
           vemb_v16_aeron_batch_channel_id(channel),
           vemb_v16_aeron_batch_topology_epoch(channel),
           resources.effective_batch_size, resources.max_batch_bytes);
    vemb_v16_aeron_batch_close(channel);
    return 0;
}
