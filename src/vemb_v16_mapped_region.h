#ifndef __VEMB_V16_MAPPED_REGION_H
#define __VEMB_V16_MAPPED_REGION_H

#include "vemb_v16_protocol.h"

#include <stddef.h>
#include <stdint.h>

typedef struct vemb_v16_mapped_region {
    int fd;
    uint32_t backend_type;
    uint32_t cache_policy;
    int unlink_on_destroy;
    int created;
    size_t requested_size;
    size_t mapping_bytes;
    uint64_t mmap_offset;
    uint64_t mmap_aligned_offset;
    void *mapping_addr;
    uint8_t *mapped_addr;
    char path[256];
} vemb_v16_mapped_region_t;

int vemb_v16_mapped_region_open(vemb_v16_mapped_region_t *region,
                                uint32_t backend_type,
                                uint32_t cache_policy,
                                const char *path,
                                uint64_t mmap_offset,
                                size_t requested_size);
void vemb_v16_mapped_region_close(vemb_v16_mapped_region_t *region);

#endif
