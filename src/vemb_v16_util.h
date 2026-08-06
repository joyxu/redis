#ifndef __VEMB_V16_UTIL_H
#define __VEMB_V16_UTIL_H

#include "monotonic.h"

#include <stddef.h>
#include <stdint.h>

static inline uint64_t vemb_v16_monotonic_ns(void) {
    return getMonotonicNs();
}

static const uint32_t VEMB_V16_MAX_POWER_U32 = UINT32_C(1) << 30;
static inline uint32_t vemb_v16_pow2_ceil_u32(uint64_t value) {
    if (value <= 1)
        return 1;
    if (value >= VEMB_V16_MAX_POWER_U32)
        return VEMB_V16_MAX_POWER_U32;

    uint32_t v = (uint32_t)(value - 1);
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    return v + 1;
}

static inline size_t align_up_size(size_t value, size_t alignment) {
    return (value + alignment - 1u) & ~(alignment - 1u);
}

#endif
