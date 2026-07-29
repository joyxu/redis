#ifndef VEMB_V16_CACHELINE_H
#define VEMB_V16_CACHELINE_H

#include <stddef.h>
#include <stdint.h>

#define VEMB_V16_CACHELINE_SIZE 64u

static inline size_t vemb_v16_align_up_size(size_t value, size_t alignment) {
    return (value + alignment - 1u) / alignment * alignment;
}

static inline uint32_t vemb_v16_align_up_u32(uint32_t value,
                                             uint32_t alignment) {
    return (uint32_t)vemb_v16_align_up_size(value, alignment);
}

#endif
