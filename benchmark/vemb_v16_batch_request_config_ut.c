#include "vemb_v16_aeron_attach.h"

#include <assert.h>
#include <stdio.h>

int main(void) {
    assert(sizeof(VEMB_V16_AERON_ATTACH_V2_MAGIC) - 1u ==
           VEMB_V16_AERON_ATTACH_V2_MAGIC_LEN);
    assert(sizeof(VEMB_V16_AERON_ATTACHED_V2_MAGIC) - 1u ==
           VEMB_V16_AERON_ATTACHED_V2_MAGIC_LEN);
    assert(sizeof(((vemb_v16_aeron_attach_v2_req_t *)0)->requested_batch_size) ==
           sizeof(uint32_t));
    assert(sizeof(vemb_v16_aeron_attach_v2_resp_t) >
           sizeof(vemb_v16_aeron_attach_resp_t));
    assert(VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE >= 16u);
    assert(VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE % CACHELINE_SIZE == 0);
    assert(VEMB_V16_BATCH_MAX_BYTES_DEFAULT > 0);
    assert(VEMB_V16_BATCH_MAX_BYTES_DEFAULT <=
           VEMB_V16_BATCH_MAX_BYTES_MAX);
    assert(VEMB_V16_BATCH_MAX_BYTES_DEFAULT % CACHELINE_SIZE == 0);
    assert(vemb_v16_batch_aligned_bytes(1) == CACHELINE_SIZE);
    assert(vemb_v16_batch_aligned_bytes(32769) == 32832u);
    assert(vemb_v16_batch_aligned_bytes(VEMB_V16_BATCH_MAX_BYTES_MAX) ==
           VEMB_V16_BATCH_MAX_BYTES_MAX);
    assert(vemb_v16_effective_batch_request_size(32, 64) == 32);
    assert(vemb_v16_effective_batch_request_size(64, 32) == 32);
    assert(vemb_v16_effective_batch_request_size(0, 32) == 0);
    assert(vemb_v16_effective_batch_request_size(32, 0) == 0);
    assert(vemb_v16_effective_batch_request_size(
               VEMB_V16_BATCH_REQUEST_SIZE_MAX + 1u,
               VEMB_V16_BATCH_REQUEST_SIZE_MAX + 1u) ==
           VEMB_V16_BATCH_REQUEST_SIZE_MAX);
    puts("vemb_v16_batch_request_config_ut: PASS");
    return 0;
}
