#define _GNU_SOURCE

#include "vemb_v16_warm_provider.h"
#include "macro.h"
#include "vemb_v16_log.h"

#include <string.h>

static const char *warm_backend_name(uint32_t backend_type) {
    switch (backend_type) {
    case VEMB_V16_REGION_LOCAL_SHM:
        return "shm";
    case VEMB_V16_REGION_UB:
        return "ub";
    default:
        return "unknown";
    }
}

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
                                  uint32_t weight) {
    RETURN_IF(!path[0], -1);
    RETURN_IF(region_bytes == 0 || region_bytes > SIZE_MAX || value_size == 0, -1);
    RETURN_IF(strlen(path) >= sizeof(provider->path), -1);
    RETURN_IF(view_offset > mapping->requested_size ||
              region_bytes > mapping->requested_size - (size_t)view_offset,
              -1);

    memset(provider, 0, sizeof(*provider));
    provider->fd = -1;
    provider->mapping = mapping;
    provider->region_id = region_id;
    provider->backend_type = backend_type ? backend_type : VEMB_V16_REGION_LOCAL_SHM;
    provider->mmap_offset = mmap_offset;
    provider->mmap_aligned_offset = mapping->mmap_aligned_offset;
    provider->mapping_size = (size_t)region_bytes;
    provider->mapping_bytes = mapping->mapping_bytes;
    provider->mapping_addr = mapping->mapping_addr;
    memcpy(provider->path, path, strlen(path) + 1);
    provider->region = (vemb_v16_tlc_warm_region_t){
        .region_id = region_id,
        .backend_type = provider->backend_type,
        .home_ub_node_id = home_ub_node_id,
        .is_local = is_local,
        .weight = weight ? weight : 1,
        .mapped_addr = mapping->mapped_addr + view_offset,
        .region_bytes = region_bytes,
        .mmap_offset = mmap_offset,
        .value_size = value_size,
    };
    serverLog(LL_NOTICE,
              "vemb_v16 warm provider attach ok: region_id=%u backend=%s path=%s mapping_addr=%p mapped_addr=%p mapping_bytes=%zu region_bytes=%llu value_size=%u view_offset=%llu home_ub_node_id=%u is_local=%u weight=%u",
              region_id,
              warm_backend_name(provider->backend_type),
              path,
              provider->mapping_addr,
              provider->region.mapped_addr,
              provider->mapping_bytes,
              (unsigned long long)provider->region.region_bytes,
              provider->region.value_size,
              (unsigned long long)view_offset,
              provider->region.home_ub_node_id,
              provider->region.is_local,
              provider->region.weight);
    return 0;
}

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
                                uint32_t weight) {
    RETURN_IF(!path[0], -1);
    RETURN_IF(region_bytes == 0 || region_bytes > SIZE_MAX || value_size == 0, -1);

    size_t requested_size = (size_t)region_bytes;
    vemb_v16_mapped_region_t mapping;
    if (vemb_v16_mapped_region_open(&mapping,
                                    backend_type,
                                    cache_policy,
                                    path,
                                    mmap_offset,
                                    requested_size) != 0) {
        serverLog(LL_WARNING,
                  "vemb_v16 warm provider open failed: region_id=%u backend=%s path=%s bytes=%zu mmap_offset=%llu",
                  region_id,
                  warm_backend_name(backend_type),
                  path,
                  requested_size,
                  (unsigned long long)mmap_offset);
        return -1;
    }

    if (vemb_v16_warm_provider_attach(provider,
                                      &mapping,
                                      0,
                                      region_id,
                                      backend_type,
                                      path,
                                      mmap_offset,
                                      value_size,
                                      region_bytes,
                                      home_ub_node_id,
                                      is_local,
                                      weight) != 0) {
        vemb_v16_mapped_region_close(&mapping);
        return -1;
    }
    provider->owned_mapping = mapping;
    provider->mapping = &provider->owned_mapping;
    provider->owns_mapping = 1;
    provider->mmap_aligned_offset = provider->owned_mapping.mmap_aligned_offset;
    provider->mapping_addr = provider->owned_mapping.mapping_addr;
    provider->mapping_bytes = provider->owned_mapping.mapping_bytes;
    return 0;
}

void vemb_v16_warm_provider_close(vemb_v16_warm_provider_t *provider) {
    if (provider->owns_mapping)
        vemb_v16_mapped_region_close(&provider->owned_mapping);
    memset(provider, 0, sizeof(*provider));
    provider->fd = -1;
}
