#define _GNU_SOURCE

#include "vemb_v16_mapped_region.h"
#include "macro.h"
#include "vemb_v16_log.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

static uint64_t page_align_down(uint64_t value) {
    long page_size = sysconf(_SC_PAGESIZE);
    uint64_t page_mask = (uint64_t)(page_size > 0 ? page_size : 4096) - 1u;
    return value & ~page_mask;
}

static int open_ub_with_fallback(const char *path, int *used_sync) {
    int fd = open(path, O_RDWR);
    if (fd >= 0) {
        *used_sync = 0;
        return fd;
    }
    if (errno != EPERM && errno != EACCES)
        return -1;
    fd = open(path, O_RDWR | O_SYNC);
    if (fd >= 0)
        *used_sync = 1;
    return fd;
}

static void *map_ub_with_fallback(const char *path, int *fd,
                                  size_t bytes, uint64_t offset,
                                  int *used_sync) {
    void *mapping = mmap(NULL, bytes, PROT_READ | PROT_WRITE,
                         MAP_SHARED, *fd, (off_t)offset);
    if (mapping != MAP_FAILED)
        return mapping;
    if (errno != EPERM && errno != EACCES)
        return MAP_FAILED;

    close(*fd);
    *fd = open(path, O_RDWR | O_SYNC);
    if (*fd < 0)
        return MAP_FAILED;
    *used_sync = 1;
    return mmap(NULL, bytes, PROT_READ | PROT_WRITE,
                MAP_SHARED, *fd, (off_t)offset);
}

static int open_or_attach_local_shm(vemb_v16_mapped_region_t *region,
                                    const char *path,
                                    size_t required_size,
                                    int *created) {
    region->fd = shm_open(path, O_RDWR | O_CREAT | O_EXCL, 0666);
    if (region->fd >= 0) {
        *created = 1;
        if (ftruncate(region->fd, (off_t)required_size) != 0) {
            serverLog(LL_WARNING,
                      "vemb_v16 mapped region shm resize failed: path=%s fd=%d request_size=%zu error=%s",
                      path,
                      region->fd,
                      required_size,
                      strerror(errno));
            close(region->fd);
            region->fd = -1;
            shm_unlink(path);
            return -1;
        }
        serverLog(LL_NOTICE,
                  "vemb_v16 mapped region shm created: path=%s fd=%d request_size=%zu open_size=%zu",
                  path,
                  region->fd,
                  required_size,
                  required_size);
        return 0;
    }
    if (errno != EEXIST) {
        serverLog(LL_WARNING,
                  "vemb_v16 mapped region shm create failed: path=%s request_size=%zu error=%s",
                  path,
                  required_size,
                  strerror(errno));
        return -1;
    }

    region->fd = shm_open(path, O_RDWR, 0666);
    if (region->fd < 0) {
        serverLog(LL_WARNING,
                  "vemb_v16 mapped region shm attach failed: path=%s request_size=%zu error=%s",
                  path,
                  required_size,
                  strerror(errno));
        return -1;
    }

    struct stat st;
    if (fstat(region->fd, &st) != 0) {
        serverLog(LL_WARNING,
                  "vemb_v16 mapped region shm stat failed: path=%s fd=%d request_size=%zu error=%s",
                  path,
                  region->fd,
                  required_size,
                  strerror(errno));
        close(region->fd);
        region->fd = -1;
        return -1;
    }
    if (st.st_size < 0 || (uint64_t)st.st_size < (uint64_t)required_size) {
        serverLog(LL_WARNING,
                  "vemb_v16 mapped region shm too small: path=%s fd=%d request_size=%zu open_size=%llu",
                  path,
                  region->fd,
                  required_size,
                  st.st_size < 0 ? 0ULL : (unsigned long long)st.st_size);
        close(region->fd);
        region->fd = -1;
        errno = ENOSPC;
        return -1;
    }
    serverLog(LL_NOTICE,
              "vemb_v16 mapped region shm attached: path=%s fd=%d request_size=%zu open_size=%llu",
              path,
              region->fd,
              required_size,
              (unsigned long long)st.st_size);
    return 0;
}

int vemb_v16_mapped_region_open(vemb_v16_mapped_region_t *region,
                                uint32_t backend_type,
                                uint32_t cache_policy,
                                const char *path,
                                uint64_t mmap_offset,
                                size_t requested_size) {
    RETURN_IF(!region || !path || !path[0] || requested_size == 0, -1);
    RETURN_IF(strlen(path) >= sizeof(region->path), -1);
    memset(region, 0, sizeof(*region));
    region->fd = -1;
    region->backend_type = backend_type ? backend_type : VEMB_V16_REGION_LOCAL_SHM;
    RETURN_IF(region->backend_type != VEMB_V16_REGION_LOCAL_SHM &&
              region->backend_type != VEMB_V16_REGION_UB,
              -1);
    (void)cache_policy;
    region->cache_policy = VEMB_V16_UB_CACHE_POLICY_CACHEABLE;
    region->requested_size = requested_size;
    region->mmap_offset = mmap_offset;
    region->mmap_aligned_offset = page_align_down(mmap_offset);
    memcpy(region->path, path, strlen(path) + 1);

    int created = 0;
    int used_sync = 0;
    if (region->backend_type == VEMB_V16_REGION_LOCAL_SHM) {
        RETURN_IF(path[0] != '/', -1);
        size_t required_size = requested_size + (size_t)mmap_offset;
        if (open_or_attach_local_shm(region, path, required_size, &created) != 0)
            return -1;
    } else {
        region->fd = open_ub_with_fallback(path, &used_sync);
        if (region->fd < 0) {
            serverLog(LL_WARNING,
                      "vemb_v16 mapped region ub open failed: path=%s access_mode=%u request_size=%zu offset=%llu error=%s",
                      path,
                      region->cache_policy,
                      requested_size,
                      (unsigned long long)mmap_offset,
                      strerror(errno));
            return -1;
        }
        region->cache_policy = used_sync ?
            VEMB_V16_UB_CACHE_POLICY_NONCACHEABLE :
            VEMB_V16_UB_CACHE_POLICY_CACHEABLE;
        struct stat st;
        if (fstat(region->fd, &st) == 0) {
            uint64_t required_size = mmap_offset + (uint64_t)requested_size;
            if (S_ISREG(st.st_mode) &&
                (st.st_size < 0 || (uint64_t)st.st_size < required_size)) {
                serverLog(LL_WARNING,
                          "vemb_v16 mapped region ub file too small: path=%s fd=%d request_size=%zu offset=%llu open_size=%llu",
                          path,
                          region->fd,
                          requested_size,
                          (unsigned long long)mmap_offset,
                          st.st_size < 0 ? 0ULL : (unsigned long long)st.st_size);
                close(region->fd);
                region->fd = -1;
                errno = ENOSPC;
                return -1;
            }
            if (S_ISREG(st.st_mode)) {
                serverLog(LL_NOTICE,
                          "vemb_v16 mapped region ub file opened: path=%s access_mode=%u fd=%d request_size=%zu offset=%llu open_size=%llu",
                          path,
                          region->cache_policy,
                          region->fd,
                          requested_size,
                          (unsigned long long)mmap_offset,
                          (unsigned long long)st.st_size);
            } else {
                serverLog(LL_NOTICE,
                          "vemb_v16 mapped region ub device opened: path=%s access_mode=%u fd=%d request_size=%zu offset=%llu mode=%o",
                          path,
                          region->cache_policy,
                          region->fd,
                          requested_size,
                          (unsigned long long)mmap_offset,
                          (unsigned)st.st_mode);
            }
        } else {
            serverLog(LL_WARNING,
                      "vemb_v16 mapped region ub stat failed: path=%s fd=%d request_size=%zu offset=%llu error=%s",
                      path,
                      region->fd,
                      requested_size,
                      (unsigned long long)mmap_offset,
                      strerror(errno));
        }
    }

    size_t offset_delta = (size_t)(mmap_offset - region->mmap_aligned_offset);
    region->mapping_bytes = requested_size + offset_delta;
#if defined(__linux__) && defined(MAP_HUGETLB)
    if (region->backend_type == VEMB_V16_REGION_UB) {
        region->mapping_addr = mmap(NULL,
                                    region->mapping_bytes,
                                    PROT_READ | PROT_WRITE,
                                    MAP_SHARED | MAP_HUGETLB,
                                    region->fd,
                                    (off_t)region->mmap_aligned_offset);
        if (region->mapping_addr == MAP_FAILED) {
            serverLog(LL_WARNING,
                      "vemb_v16 mapped region huge tlb mmap failed, fallback to regular mmap: backend=%u path=%s fd=%d request_size=%zu offset=%llu aligned_offset=%llu mapping_bytes=%zu error=%s",
                      region->backend_type,
                      path,
                      region->fd,
                      requested_size,
                      (unsigned long long)mmap_offset,
                      (unsigned long long)region->mmap_aligned_offset,
                      region->mapping_bytes,
                      strerror(errno));
        }
    } else {
        region->mapping_addr = MAP_FAILED;
    }
#else
    region->mapping_addr = MAP_FAILED;
#endif

    if (region->mapping_addr == MAP_FAILED) {
        if (region->backend_type == VEMB_V16_REGION_UB) {
            region->mapping_addr = map_ub_with_fallback(
                path, &region->fd, region->mapping_bytes,
                region->mmap_aligned_offset, &used_sync);
        } else {
            region->mapping_addr = mmap(NULL, region->mapping_bytes,
                                        PROT_READ | PROT_WRITE,
                                        MAP_SHARED, region->fd,
                                        (off_t)region->mmap_aligned_offset);
        }
        if (used_sync)
            region->cache_policy = VEMB_V16_UB_CACHE_POLICY_NONCACHEABLE;
    }
    if (region->mapping_addr == MAP_FAILED) {
        serverLog(LL_WARNING,
                  "vemb_v16 mapped region mmap failed: backend=%u path=%s fd=%d request_size=%zu offset=%llu aligned_offset=%llu mapping_bytes=%zu error=%s",
                  region->backend_type,
                  path,
                  region->fd,
                  requested_size,
                  (unsigned long long)mmap_offset,
                  (unsigned long long)region->mmap_aligned_offset,
                  region->mapping_bytes,
                  strerror(errno));
        if (created)
            shm_unlink(path);
        close(region->fd);
        region->fd = -1;
        region->mapping_addr = NULL;
        return -1;
    }
    region->mapped_addr = (uint8_t *)region->mapping_addr + offset_delta;
    serverLog(LL_NOTICE,
              "vemb_v16 mapped region mmap ok: backend=%u path=%s fd=%d request_size=%zu offset=%llu aligned_offset=%llu mapping_bytes=%zu mapping_addr=%p mapped_addr=%p created=%d",
              region->backend_type,
              path,
              region->fd,
              requested_size,
              (unsigned long long)mmap_offset,
              (unsigned long long)region->mmap_aligned_offset,
              region->mapping_bytes,
              region->mapping_addr,
              region->mapped_addr,
              created);
    close(region->fd);
    region->fd = -1;
    return 0;
}

void vemb_v16_mapped_region_close(vemb_v16_mapped_region_t *region) {
    RETURN_IF(!region);
    if (region->mapping_addr)
        munmap(region->mapping_addr, region->mapping_bytes);
    if (region->fd >= 0)
        close(region->fd);
    if (region->unlink_on_destroy && region->path[0])
        shm_unlink(region->path);
    memset(region, 0, sizeof(*region));
    region->fd = -1;
}
