#define _GNU_SOURCE
#include "sve_operation.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>

static unsigned char *guarded_page(size_t page) {
    unsigned char *mapping = mmap(NULL, page * 3, PROT_NONE,
                                  MAP_PRIVATE | MAP_ANON, -1, 0);
    if (mapping == MAP_FAILED ||
        mprotect(mapping + page, page, PROT_READ | PROT_WRITE) != 0) {
        perror("guarded mapping");
        exit(1);
    }
    return mapping + page;
}

int main(void) {
    size_t page = (size_t)sysconf(_SC_PAGESIZE);
    unsigned char *src_page = guarded_page(page);
    unsigned char *dst_page = guarded_page(page);
    const size_t sizes[] = {0, 1, 3, 4, 12, 28, 32, 60, 64, 68,
                            124, 128, 132, 300, 1200, 2048};
    for (size_t i = 0; i < page; i++)
        src_page[i] = (unsigned char)(i * 17u + 31u);
    size_t cases = 0;
    for (unsigned edge = 0; edge < 2; edge++) {
        for (size_t s = 0; s < sizeof(sizes) / sizeof(sizes[0]); s++) {
            size_t bytes = sizes[s];
            for (size_t so = 0; so < 64; so++) {
                for (size_t d = 0; d < 64; d++) {
                    unsigned char *src = edge ? src_page + page - bytes - so : src_page + so;
                    unsigned char *dst = edge ? dst_page + page - bytes - d : dst_page + d;
                    memset(dst_page, 0xa5, page);
                    sve_streaming_load_f32(src, dst, bytes);
                    if (memcmp(src, dst, bytes) != 0) {
                        fprintf(stderr, "copy mismatch edge=%u size=%zu src=%zu dst=%zu\n",
                                edge, bytes, so, d);
                        return 1;
                    }
                    for (size_t i = 0; i < (size_t)(dst - dst_page); i++) {
                        if (dst_page[i] != 0xa5) {
                            fprintf(stderr, "write before requested span\n");
                            return 1;
                        }
                    }
                    for (size_t i = (size_t)(dst - dst_page) + bytes; i < page; i++) {
                        if (dst_page[i] != 0xa5) {
                            fprintf(stderr, "write after requested span\n");
                            return 1;
                        }
                    }
                    cases++;
                }
            }
        }
    }
    munmap(src_page - page, page * 3);
    munmap(dst_page - page, page * 3);
    printf("sve_streaming_copy_ut: PASS cases=%zu sve=%s\n", cases,
#ifdef USE_ARM_SVE
           "enabled"
#else
           "disabled (fallback only)"
#endif
    );
    return 0;
}
