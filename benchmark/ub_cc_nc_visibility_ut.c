/*
 * Minimal cross-node UB visibility reproducer for the VSIM access pattern.
 *
 * Writer: local export, cacheable mapping via open(O_RDWR).
 * Reader: remote import, non-cacheable mapping via open(O_RDWR | O_SYNC).
 *
 * This test intentionally does not use ownership APIs, atomics, msync, or any
 * VSIM code. It writes and reads one aligned 64-byte cache line.
 */

#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#define CACHELINE_BYTES 64u

typedef struct options {
    const char *mode;
    const char *path;
    uint64_t offset;
    uint64_t seed;
    unsigned hold_seconds;
    unsigned watch_seconds;
    int have_offset;
    int have_seed;
} options_t;

static void usage(const char *program) {
    fprintf(stderr,
            "Usage:\n"
            "  %s writer --path PATH --offset BYTES --seed VALUE [--hold-seconds N]\n"
            "  %s writer-nc --path PATH --offset BYTES --seed VALUE [--hold-seconds N]\n"
            "  %s reader --path PATH --offset BYTES --seed VALUE [--watch-seconds N]\n"
            "  %s reader-cc --path PATH --offset BYTES --seed VALUE [--watch-seconds N]\n"
            "\n"
            "writer/reader use local CC writer and remote NC reader. writer-nc/reader-cc\n"
            "use remote NC writer and local CC reader.\n"
            "Offset must be 64-byte aligned. VALUE and BYTES accept decimal or 0x hex.\n",
            program, program, program, program);
}

static int parse_u64(const char *text, uint64_t *value) {
    char *end = NULL;
    errno = 0;
    unsigned long long parsed = strtoull(text, &end, 0);
    if (errno != 0 || !end || *end != '\0')
        return -1;
    *value = (uint64_t)parsed;
    return 0;
}

static int parse_options(int argc, char **argv, options_t *options) {
    memset(options, 0, sizeof(*options));
    options->hold_seconds = 60;
    if (argc < 2)
        return -1;
    options->mode = argv[1];

    for (int i = 2; i < argc; i++) {
        if (!strcmp(argv[i], "--path") && i + 1 < argc) {
            options->path = argv[++i];
        } else if (!strcmp(argv[i], "--offset") && i + 1 < argc) {
            if (parse_u64(argv[++i], &options->offset) != 0)
                return -1;
            options->have_offset = 1;
        } else if (!strcmp(argv[i], "--seed") && i + 1 < argc) {
            if (parse_u64(argv[++i], &options->seed) != 0)
                return -1;
            options->have_seed = 1;
        } else if (!strcmp(argv[i], "--hold-seconds") && i + 1 < argc) {
            uint64_t value = 0;
            if (parse_u64(argv[++i], &value) != 0 || value > UINT32_MAX)
                return -1;
            options->hold_seconds = (unsigned)value;
        } else if (!strcmp(argv[i], "--watch-seconds") && i + 1 < argc) {
            uint64_t value = 0;
            if (parse_u64(argv[++i], &value) != 0 || value > UINT32_MAX)
                return -1;
            options->watch_seconds = (unsigned)value;
        } else {
            return -1;
        }
    }

    if ((!strcmp(options->mode, "writer") ||
         !strcmp(options->mode, "writer-nc") ||
         !strcmp(options->mode, "reader") ||
         !strcmp(options->mode, "reader-cc")) &&
        options->path && options->have_offset && options->have_seed &&
        (options->offset % CACHELINE_BYTES) == 0) {
        return 0;
    }
    return -1;
}

static void fill_expected(uint64_t line[8], uint64_t seed) {
    for (uint64_t i = 0; i < 8; i++)
        line[i] = seed + i;
}

static void print_line(const char *label, const uint64_t line[8]) {
    printf("%s", label);
    for (size_t i = 0; i < 8; i++)
        printf(" %016llx", (unsigned long long)line[i]);
    printf("\n");
}

static int map_line(const options_t *options,
                    int flags,
                    int *fd_out,
                    void **mapping_out,
                    size_t *mapping_bytes_out,
                    uint8_t **line_out) {
    long page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0) {
        fprintf(stderr, "sysconf(_SC_PAGESIZE) failed\n");
        return -1;
    }

    uint64_t page_mask = (uint64_t)page_size - 1u;
    uint64_t mapping_offset = options->offset & ~page_mask;
    size_t line_offset = (size_t)(options->offset - mapping_offset);
    if (line_offset + CACHELINE_BYTES > (size_t)page_size) {
        fprintf(stderr, "64-byte line crosses a page boundary\n");
        return -1;
    }

    int fd = open(options->path, flags);
    if (fd < 0) {
        fprintf(stderr, "open(%s, flags=0x%x) failed: %s\n",
                options->path, flags, strerror(errno));
        return -1;
    }

    void *mapping = mmap(NULL, (size_t)page_size, PROT_READ | PROT_WRITE,
                         MAP_SHARED, fd, (off_t)mapping_offset);
    if (mapping == MAP_FAILED) {
        fprintf(stderr, "mmap(%s, offset=%llu) failed: %s\n",
                options->path, (unsigned long long)mapping_offset,
                strerror(errno));
        close(fd);
        return -1;
    }

    *fd_out = fd;
    *mapping_out = mapping;
    *mapping_bytes_out = (size_t)page_size;
    *line_out = (uint8_t *)mapping + line_offset;
    return 0;
}

static int run_writer(const options_t *options, int flags, const char *mode) {
    int fd = -1;
    void *mapping = MAP_FAILED;
    size_t mapping_bytes = 0;
    uint8_t *line = NULL;
    uint64_t expected[8];

    if (map_line(options, flags, &fd, &mapping, &mapping_bytes, &line) != 0)
        return 1;

    fill_expected(expected, options->seed);
    memcpy(line, expected, CACHELINE_BYTES);
    atomic_thread_fence(memory_order_seq_cst);

    printf("WRITER_READY path=%s flags=%s offset=%llu hold_seconds=%u\n",
           options->path, mode, (unsigned long long)options->offset,
           options->hold_seconds);
    print_line("written:", expected);
    printf("Run the reader while this process is sleeping.\n");
    fflush(stdout);

    sleep(options->hold_seconds);
    printf("WRITER_UNMAP\n");
    munmap(mapping, mapping_bytes);
    close(fd);
    return 0;
}

static int line_matches(const uint64_t actual[8], const uint64_t expected[8]) {
    return memcmp(actual, expected, CACHELINE_BYTES) == 0;
}

static int run_reader(const options_t *options, int flags, const char *mode) {
    int fd = -1;
    void *mapping = MAP_FAILED;
    size_t mapping_bytes = 0;
    uint8_t *line = NULL;
    uint64_t expected[8];
    uint64_t actual[8];
    unsigned attempts = options->watch_seconds ? options->watch_seconds + 1 : 1;
    int matched = 0;

    if (map_line(options, flags, &fd, &mapping,
                 &mapping_bytes, &line) != 0) {
        return 1;
    }

    fill_expected(expected, options->seed);
    printf("READER_READY path=%s flags=%s offset=%llu watch_seconds=%u\n",
           options->path, mode, (unsigned long long)options->offset,
           options->watch_seconds);
    print_line("expected:", expected);

    for (unsigned i = 0; i < attempts; i++) {
        atomic_thread_fence(memory_order_seq_cst);
        memcpy(actual, line, CACHELINE_BYTES);
        atomic_thread_fence(memory_order_seq_cst);
        matched = line_matches(actual, expected);
        printf("attempt=%u result=%s\n", i, matched ? "VISIBLE" : "STALE");
        print_line("actual:  ", actual);
        fflush(stdout);
        if (i + 1 < attempts)
            sleep(1);
    }

    munmap(mapping, mapping_bytes);
    close(fd);
    return matched ? 0 : 2;
}

int main(int argc, char **argv) {
    options_t options;
    if (parse_options(argc, argv, &options) != 0) {
        usage(argv[0]);
        return 1;
    }
    if (!strcmp(options.mode, "writer"))
        return run_writer(&options, O_RDWR, "O_RDWR(CC)");
    if (!strcmp(options.mode, "writer-nc"))
        return run_writer(&options, O_RDWR | O_SYNC, "O_RDWR|O_SYNC(NC)");
    if (!strcmp(options.mode, "reader"))
        return run_reader(&options, O_RDWR | O_SYNC, "O_RDWR|O_SYNC(NC)");
    return run_reader(&options, O_RDWR, "O_RDWR(CC)");
}
