/* kvc_nc_bw.c — noncacheable 远端内存的多线程读带宽测试
 *
 * 问题: 单线程 nc 读 = 67MB/s（逐 line 往返）。多线程独立 load 流水能否叠加？
 * 用法: ./kvc_nc_bw <device> <bytes> <threads>
 */
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

static char *g_base;
static uint64_t g_bytes;
static uint64_t g_chunk;          /* 每线程读取区间 */
static uint64_t g_done[64];
static uint64_t g_sink;

static double now_s(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

static int g_memcpy_mode = 0;

static void *reader(void *arg)
{
    long id = (long)arg;
    const char *p = g_base + id * g_chunk;
    uint64_t remaining = g_chunk;
    unsigned long sink = 0;
    static _Thread_local char buf[4096];
    while (remaining >= 4096) {
        if (g_memcpy_mode) {
            /* SDK direct_read 实际路径: 整块 memcpy */
            memcpy(buf, p, 4096);
            sink += *(unsigned long *)buf;
        } else {
            for (int off = 0; off < 4096; off += 64)
                sink += *(const unsigned long *)(p + off);
        }
        p += 4096;
        remaining -= 4096;
        g_done[id] += 4096;
    }
    g_sink ^= sink;
    return NULL;
}

int main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr, "usage: %s <device> <bytes> <threads> [memcpy]\n", argv[0]);
        return 2;
    }
    g_memcpy_mode = (argc >= 5);
    const char *dev = argv[1];
    g_bytes = strtoull(argv[2], NULL, 0);
    int nthreads = atoi(argv[3]);
    if (nthreads > 64) nthreads = 64;

    int fd = open(dev, O_RDWR | O_SYNC);   /* nc 映射必须 O_SYNC */
    if (fd < 0) { perror("open"); return 1; }
    g_base = mmap(NULL, g_bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (g_base == MAP_FAILED) { perror("mmap"); return 1; }
    close(fd);

    g_chunk = g_bytes / nthreads;
    pthread_t th[64];
    double t0 = now_s();
    for (long i = 0; i < nthreads; i++)
        pthread_create(&th[i], NULL, reader, (void *)i);
    uint64_t total = 0;
    for (int i = 0; i < nthreads; i++) {
        pthread_join(th[i], NULL);
        total += g_done[i];
    }
    double dt = now_s() - t0;
    printf("threads=%d total=%lluMB wall=%.3fs bw=%.1f MB/s (sink=%lx)\n",
           nthreads, (unsigned long long)(total >> 20), dt,
           total / dt / (1024.0 * 1024.0), g_sink);
    munmap(g_base, g_bytes);
    return 0;
}
