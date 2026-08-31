/* kvc_ub.c — shmdev mmap（O_SYNC fallback 实测路径）+ 匿名映射 */
#include "kvc_ub.h"
#include "kvc/kvc_common.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

void *kvc_map_memory(const char *path, uint64_t offset, uint64_t bytes,
                     uint32_t flags)
{
    if (bytes == 0)
        return NULL;

    if (path == NULL || path[0] == '\0') {
        void *p = mmap(NULL, bytes, PROT_READ | PROT_WRITE,
                       MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        return p == MAP_FAILED ? NULL : p;
    }

    int fd = -1;
    int last_errno = 0;
    int open_flags[2] = {O_RDWR, O_RDWR | O_SYNC};
    int nflags = 1;
    if (flags & KVC_REGION_O_SYNC_FALLBACK)
        nflags = 2;

    for (int i = 0; i < nflags; i++) {
        fd = open(path, open_flags[i]);
        if (fd >= 0)
            break;
        last_errno = errno;
    }
    if (fd < 0) {
        (void)last_errno;
        return NULL;
    }

    void *p = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
                   (off_t)offset);
    int saved = errno;
    close(fd);
    if (p == MAP_FAILED) {
        (void)saved;
        return NULL;
    }
    return p;
}

int kvc_unmap_memory(void *addr, uint64_t bytes)
{
    if (!addr || bytes == 0)
        return KVC_EINVAL;
    return munmap(addr, bytes) == 0 ? KVC_OK : KVC_EINVAL;
}

void kvc_copy_remote(void *dst, const void *src, size_t n)
{
    const uint64_t *s = (const uint64_t *)src;
    uint64_t *d = (uint64_t *)dst;
    while (n >= 64) {
        /* 8 个独立 load（无地址/数据依赖）→ 乱序引擎可 8 深度流水发出 */
        uint64_t a0 = s[0], a1 = s[1], a2 = s[2], a3 = s[3];
        uint64_t a4 = s[4], a5 = s[5], a6 = s[6], a7 = s[7];
        d[0] = a0; d[1] = a1; d[2] = a2; d[3] = a3;
        d[4] = a4; d[5] = a5; d[6] = a6; d[7] = a7;
        s += 8; d += 8; n -= 64;
    }
    while (n >= 8) {
        *d++ = *s++;
        n -= 8;
    }
    if (n > 0) {
        const unsigned char *b = (const unsigned char *)s;
        unsigned char *e = (unsigned char *)d;
        while (n--)
            *e++ = *b++;
    }
}
