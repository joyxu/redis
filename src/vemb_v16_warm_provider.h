#ifndef __VEMB_V16_WARM_PROVIDER_H
#define __VEMB_V16_WARM_PROVIDER_H

#include "vemb_v16_mapped_region.h"
#include "vemb_v16_tlc.h"

#include <stddef.h>
#include <stdint.h>

typedef struct vemb_v16_warm_provider {
    const vemb_v16_mapped_region_t *mapping;
    vemb_v16_mapped_region_t owned_mapping;
    int owns_mapping;
    uint32_t region_id;
    uint32_t backend_type;
    char path[256];
    uint64_t mmap_offset;
    uint64_t mmap_aligned_offset;
    size_t mapping_size;
    size_t mapping_bytes;
    void *mapping_addr;
    int fd;
    int unlink_on_destroy;
    vemb_v16_tlc_warm_region_t region;
} vemb_v16_warm_provider_t;

int vemb_v16_warm_provider_open(vemb_v16_warm_provider_t *provider,
                                uint32_t region_id,
                                uint32_t backend_type,
                                uint32_t cache_policy,
                                const char *path,
                                uint64_t mmap_offset,
                                uint32_t value_size,
                                uint64_t region_bytes,
                                uint32_t home_ub_node_id,
                                uint32_t is_local,
                                uint32_t weight);
int vemb_v16_warm_provider_attach(vemb_v16_warm_provider_t *provider,
                                  const vemb_v16_mapped_region_t *mapping,
                                  uint64_t view_offset,
                                  uint32_t region_id,
                                  uint32_t backend_type,
                                  const char *path,
                                  uint64_t mmap_offset,
                                  uint32_t value_size,
                                  uint64_t region_bytes,
                                  uint32_t home_ub_node_id,
                                  uint32_t is_local,
                                  uint32_t weight);
void vemb_v16_warm_provider_close(vemb_v16_warm_provider_t *provider);

#endif
