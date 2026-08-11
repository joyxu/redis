/*
 * Minimal cross-node UB visibility reproducer for the VSIM access pattern.
 *
 * The original line modes check one aligned cache line. The frame modes reuse
 * a multi-cacheline range and publish a descriptor after the body. A reverse
 * UB ack prevents the writer from overwriting a frame before inspection.
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

#include "cpu_relax.h"
#include "monotonic.h"

#define CACHELINE_BYTES 64u
#define CACHELINE_WORDS (CACHELINE_BYTES / sizeof(uint64_t))
#define DEFAULT_FRAME_BYTES 4096u
#define DEFAULT_FRAME_ITERATIONS 1000000u
#define DEFAULT_TIMEOUT_SECONDS 30u

typedef struct options {
    const char *mode;
    const char *path;
    const char *ack_path;
    uint64_t offset;
    uint64_t seed;
    uint64_t frame_bytes;
    uint64_t iterations;
    uint64_t inject_mixed_at;
    unsigned hold_seconds;
    unsigned watch_seconds;
    unsigned timeout_seconds;
    uint64_t read_delay_ns;
    int have_offset;
    int have_seed;
    int expect_mixed;
    int expect_visibility_failure;
} options_t;

typedef struct mapped_range {
    int fd;
    void *mapping;
    size_t mapping_bytes;
    uint8_t *data;
} mapped_range_t;

typedef _Atomic(uint64_t) atomic_u64_t;

_Static_assert(sizeof(atomic_u64_t) == sizeof(uint64_t),
               "UB descriptor and ack must occupy one 64-bit word");

static void usage(const char *program) {
    fprintf(stderr,
            "Usage:\n"
            "  %s writer --path PATH --offset BYTES --seed VALUE [--hold-seconds N]\n"
            "  %s writer-nc --path PATH --offset BYTES --seed VALUE [--hold-seconds N]\n"
            "  %s reader --path PATH --offset BYTES --seed VALUE [--watch-seconds N]\n"
            "  %s reader-cc --path PATH --offset BYTES --seed VALUE [--watch-seconds N]\n"
            "  %s frame-writer-nc --path DATA --ack-path ACK --offset BYTES --seed VALUE\n"
            "      [--frame-bytes N] [--iterations N] [--timeout-seconds N] [--inject-mixed-at N]\n"
            "  %s frame-reader-cc --path DATA --ack-path ACK --offset BYTES --seed VALUE\n"
            "      [--frame-bytes N] [--iterations N] [--timeout-seconds N]\n"
            "      [--expect-mixed|--expect-visibility-failure]\n"
            "      [--read-delay-us N|--read-delay-ns N]\n"
            "\n"
            "writer/reader use local CC writer and remote NC reader. writer-nc/reader-cc\n"
            "use remote NC writer and local CC reader. Frame modes model a server NC\n"
            "response writer and a client CC response reader; the reader writes its ack\n"
            "through the reverse import path. Offset and frame bytes must be 64-byte aligned.\n"
            "VALUE and BYTES accept decimal or 0x hex.\n",
            program, program, program, program, program, program);
}

static int parse_u64(const char *text, uint64_t *value) {
    char *end = NULL;
    unsigned long long parsed;

    errno = 0;
    parsed = strtoull(text, &end, 0);
    if (errno != 0 || !end || *end != '\0')
        return -1;
    *value = (uint64_t)parsed;
    return 0;
}

static int is_line_mode(const char *mode) {
    return !strcmp(mode, "writer") || !strcmp(mode, "writer-nc") ||
           !strcmp(mode, "reader") || !strcmp(mode, "reader-cc");
}

static int is_frame_mode(const char *mode) {
    return !strcmp(mode, "frame-writer-nc") ||
           !strcmp(mode, "frame-reader-cc");
}

static int parse_options(int argc, char **argv, options_t *options) {
    memset(options, 0, sizeof(*options));
    options->hold_seconds = 60;
    options->frame_bytes = DEFAULT_FRAME_BYTES;
    options->iterations = DEFAULT_FRAME_ITERATIONS;
    options->timeout_seconds = DEFAULT_TIMEOUT_SECONDS;
    if (argc < 2)
        return -1;
    options->mode = argv[1];

    for (int i = 2; i < argc; i++) {
        if (!strcmp(argv[i], "--path") && i + 1 < argc) {
            options->path = argv[++i];
        } else if (!strcmp(argv[i], "--ack-path") && i + 1 < argc) {
            options->ack_path = argv[++i];
        } else if (!strcmp(argv[i], "--offset") && i + 1 < argc) {
            if (parse_u64(argv[++i], &options->offset) != 0)
                return -1;
            options->have_offset = 1;
        } else if (!strcmp(argv[i], "--seed") && i + 1 < argc) {
            if (parse_u64(argv[++i], &options->seed) != 0)
                return -1;
            options->have_seed = 1;
        } else if (!strcmp(argv[i], "--frame-bytes") && i + 1 < argc) {
            if (parse_u64(argv[++i], &options->frame_bytes) != 0)
                return -1;
        } else if (!strcmp(argv[i], "--iterations") && i + 1 < argc) {
            if (parse_u64(argv[++i], &options->iterations) != 0)
                return -1;
        } else if (!strcmp(argv[i], "--inject-mixed-at") && i + 1 < argc) {
            if (parse_u64(argv[++i], &options->inject_mixed_at) != 0)
                return -1;
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
        } else if (!strcmp(argv[i], "--timeout-seconds") && i + 1 < argc) {
            uint64_t value = 0;
            if (parse_u64(argv[++i], &value) != 0 || value > UINT32_MAX)
                return -1;
            options->timeout_seconds = (unsigned)value;
        } else if (!strcmp(argv[i], "--read-delay-us") && i + 1 < argc) {
            uint64_t value = 0;
            if (parse_u64(argv[++i], &value) != 0 || value > UINT32_MAX)
                return -1;
            options->read_delay_ns = value * 1000u;
        } else if (!strcmp(argv[i], "--read-delay-ns") && i + 1 < argc) {
            if (parse_u64(argv[++i], &options->read_delay_ns) != 0)
                return -1;
        } else if (!strcmp(argv[i], "--expect-mixed")) {
            options->expect_mixed = 1;
        } else if (!strcmp(argv[i], "--expect-visibility-failure")) {
            options->expect_visibility_failure = 1;
        } else {
            return -1;
        }
    }

    if (!options->path || !options->have_offset || !options->have_seed ||
        (options->offset % CACHELINE_BYTES) != 0)
        return -1;
    if (is_line_mode(options->mode))
        return options->ack_path || options->expect_mixed ||
               options->expect_visibility_failure ||
               options->inject_mixed_at != 0 ? -1 : 0;
    if (!is_frame_mode(options->mode) || !options->ack_path ||
        options->frame_bytes < 3 * CACHELINE_BYTES ||
        (options->frame_bytes % CACHELINE_BYTES) != 0 ||
        options->iterations == 0 || options->frame_bytes > SIZE_MAX - CACHELINE_BYTES ||
        (!strcmp(options->mode, "frame-writer-nc") &&
         (options->expect_mixed || options->expect_visibility_failure ||
          options->inject_mixed_at > options->iterations)) ||
        (!strcmp(options->mode, "frame-reader-cc") &&
         (options->inject_mixed_at != 0 ||
          (options->expect_mixed && options->expect_visibility_failure))))
        return -1;
    return 0;
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

static int map_range(const char *path,
                     uint64_t offset,
                     size_t bytes,
                     int flags,
                     mapped_range_t *range) {
    long page_size = sysconf(_SC_PAGESIZE);
    uint64_t page_mask;
    uint64_t mapping_offset;
    size_t data_offset;
    size_t mapping_bytes;
    int fd;
    void *mapping;

    if (page_size <= 0) {
        fprintf(stderr, "sysconf(_SC_PAGESIZE) failed\n");
        return -1;
    }
    page_mask = (uint64_t)page_size - 1u;
    mapping_offset = offset & ~page_mask;
    data_offset = (size_t)(offset - mapping_offset);
    if (bytes > SIZE_MAX - data_offset) {
        fprintf(stderr, "mapping range overflows size_t\n");
        return -1;
    }
    mapping_bytes = data_offset + bytes;

    fd = open(path, flags);
    if (fd < 0) {
        fprintf(stderr, "open(%s, flags=0x%x) failed: %s\n",
                path, flags, strerror(errno));
        return -1;
    }
    mapping = mmap(NULL, mapping_bytes, PROT_READ | PROT_WRITE,
                   MAP_SHARED, fd, (off_t)mapping_offset);
    if (mapping == MAP_FAILED) {
        fprintf(stderr, "mmap(%s, offset=%llu, bytes=%zu) failed: %s\n",
                path, (unsigned long long)mapping_offset, mapping_bytes,
                strerror(errno));
        close(fd);
        return -1;
    }

    range->fd = fd;
    range->mapping = mapping;
    range->mapping_bytes = mapping_bytes;
    range->data = (uint8_t *)mapping + data_offset;
    return 0;
}

static void unmap_range(mapped_range_t *range) {
    if (range->mapping != MAP_FAILED)
        munmap(range->mapping, range->mapping_bytes);
    if (range->fd >= 0)
        close(range->fd);
}

static int run_writer(const options_t *options, int flags, const char *mode) {
    mapped_range_t range = {.fd = -1, .mapping = MAP_FAILED};
    uint64_t expected[8];

    if (map_range(options->path, options->offset, CACHELINE_BYTES, flags,
                  &range) != 0)
        return 1;

    fill_expected(expected, options->seed);
    memcpy(range.data, expected, CACHELINE_BYTES);
    atomic_thread_fence(memory_order_seq_cst);

    printf("WRITER_READY path=%s flags=%s offset=%llu hold_seconds=%u\n",
           options->path, mode, (unsigned long long)options->offset,
           options->hold_seconds);
    print_line("written:", expected);
    printf("Run the reader while this process is sleeping.\n");
    fflush(stdout);

    sleep(options->hold_seconds);
    printf("WRITER_UNMAP\n");
    unmap_range(&range);
    return 0;
}

static int line_matches(const uint64_t actual[8], const uint64_t expected[8]) {
    return memcmp(actual, expected, CACHELINE_BYTES) == 0;
}

/*
 * Descriptor and ack are C11 atomic state words. Frame payload remains a
 * volatile range: the ack protocol prevents writer reuse while the reader is
 * inspecting it. On aarch64, these operations compile to acquire/release
 * loads and stores for this 64-bit state word.
 */
static uint64_t load_acquire(const volatile atomic_u64_t *word) {
    return atomic_load_explicit(word, memory_order_acquire); // c11
}

static void store_release(volatile atomic_u64_t *word, uint64_t value) {
    atomic_store_explicit(word, value, memory_order_release);
}

static int run_reader(const options_t *options, int flags, const char *mode) {
    mapped_range_t range = {.fd = -1, .mapping = MAP_FAILED};
    uint64_t expected[8];
    uint64_t actual[8];
    unsigned attempts = options->watch_seconds ? options->watch_seconds + 1 : 1;
    int matched = 0;

    if (map_range(options->path, options->offset, CACHELINE_BYTES, flags,
                  &range) != 0)
        return 1;

    fill_expected(expected, options->seed);
    printf("READER_READY path=%s flags=%s offset=%llu watch_seconds=%u\n",
           options->path, mode, (unsigned long long)options->offset,
           options->watch_seconds);
    print_line("expected:", expected);

    for (unsigned i = 0; i < attempts; i++) {
        atomic_thread_fence(memory_order_seq_cst);
        memcpy(actual, range.data, CACHELINE_BYTES);
        atomic_thread_fence(memory_order_seq_cst);
        matched = line_matches(actual, expected);
        printf("attempt=%u result=%s\n", i, matched ? "VISIBLE" : "STALE");
        print_line("actual:  ", actual);
        fflush(stdout);
        if (i + 1 < attempts)
            sleep(1);
    }

    unmap_range(&range);
    return matched ? 0 : 2;
}

static uint64_t frame_pattern(uint64_t seed, uint64_t generation, size_t index) {
    uint64_t value = seed ^ (generation * UINT64_C(0x9e3779b97f4a7c15)) ^
                     ((uint64_t)index * UINT64_C(0xbf58476d1ce4e5b9));
    value ^= value >> 30;
    value *= UINT64_C(0xbf58476d1ce4e5b9);
    value ^= value >> 27;
    value *= UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31);
}

static int wait_for_value(const volatile atomic_u64_t *word,
                          uint64_t value,
                          unsigned timeout_seconds,
                          const char *name) {
    uint64_t deadline = getMonotonicNs() +
                        (uint64_t)timeout_seconds * UINT64_C(1000000000);

    for (;;) {
        if (load_acquire(word) == value)
            return 0;
        if (getMonotonicNs() >= deadline) {
            fprintf(stderr, "TIMEOUT waiting for %s=%llu actual=%llu\n",
                    name, (unsigned long long)value,
                    (unsigned long long)load_acquire(word));
            return -1;
        }
    }
}

static void delay_after_descriptor(uint64_t delay_ns) {
    if (delay_ns == 0)
        return;
    monotime start;
    elapsedStartNs(&start);
    while (elapsedNs(start) < delay_ns)
        cpu_relax();
}

static void write_frame(volatile uint64_t *frame,
                        size_t words,
                        uint64_t seed,
                        uint64_t generation,
                        int inject_mixed) {
    /*
     * 4KiB frame layout (512 uint64_t words):
     *
     *   word 0..7       first 64B cacheline: header
     *   word 8..503     body
     *   word 504..511   final 64B cacheline: trailer/commit
     *
     * Write order:
     *
     *   frame[0]        = generation;  // header marker
     *   frame[1..7]     = 0;
     *   frame[8..503]   = frame_pattern(seed, generation, index);
     *   frame[504..510] = 0;
     *   frame[511]      = generation;  // trailer marker
     *
     * The descriptor occupies the following cacheline. The caller performs
     * its release publication only after this function has written the frame.
     */
    frame[0] = generation;
    for (size_t i = 1; i < CACHELINE_WORDS; i++)
        frame[i] = 0;

    for (size_t i = CACHELINE_WORDS; i + CACHELINE_WORDS < words; i++) {
        /* Preserve body word 8 from the prior generation for UT self-checking. */
        if (inject_mixed && i == CACHELINE_WORDS)
            continue;
        frame[i] = frame_pattern(seed, generation, i);
    }

    for (size_t i = words - CACHELINE_WORDS; i + 1 < words; i++)
        frame[i] = 0;
    frame[words - 1] = generation;
}

static int inspect_frame(const volatile uint64_t *frame,
                         size_t words,
                         uint64_t seed,
                         uint64_t generation,
                         uint64_t descriptor) {
    uint64_t header = frame[0];
    uint64_t trailer = frame[words - 1];

    if (header != generation || trailer != generation) {
        printf("FRAME_VISIBILITY_FAILURE type=marker generation=%llu descriptor=%llu header=%llu trailer=%llu\n",
               (unsigned long long)generation, (unsigned long long)descriptor,
               (unsigned long long)header, (unsigned long long)trailer);
        return 2;
    }
    for (size_t i = CACHELINE_WORDS; i + CACHELINE_WORDS < words; i++) {
        uint64_t actual = frame[i];
        uint64_t expected = frame_pattern(seed, generation, i);

        if (actual != expected) {
            printf("FRAME_VISIBILITY_FAILURE type=mixed generation=%llu descriptor=%llu index=%zu expected=%016llx actual=%016llx header=%llu trailer=%llu\n",
                   (unsigned long long)generation,
                   (unsigned long long)descriptor, i,
                   (unsigned long long)expected, (unsigned long long)actual,
                   (unsigned long long)header, (unsigned long long)trailer);
            return 1;
        }
    }
    return 0;
}

static int run_frame_writer_nc(const options_t *options) {
    mapped_range_t data = {.fd = -1, .mapping = MAP_FAILED};
    mapped_range_t ack = {.fd = -1, .mapping = MAP_FAILED};
    volatile uint64_t *frame;
    volatile atomic_u64_t *descriptor;
    volatile atomic_u64_t *ack_word;
    size_t words = (size_t)(options->frame_bytes / sizeof(uint64_t));
    int rc = 1;

    if (map_range(options->path, options->offset,
                  (size_t)options->frame_bytes + CACHELINE_BYTES,
                  O_RDWR | O_SYNC, &data) != 0 ||
        map_range(options->ack_path, options->offset, CACHELINE_BYTES,
                  O_RDWR, &ack) != 0)
        goto done;

    frame = (volatile uint64_t *)data.data;
    descriptor = (volatile atomic_u64_t *)(data.data + options->frame_bytes);
    ack_word = (volatile atomic_u64_t *)ack.data;
    store_release(descriptor, 0);
    store_release(ack_word, 0);
    printf("FRAME_WRITER_READY data=%s ack=%s offset=%llu frame_bytes=%llu iterations=%llu\n",
           options->path, options->ack_path,
           (unsigned long long)options->offset,
           (unsigned long long)options->frame_bytes,
           (unsigned long long)options->iterations);
    fflush(stdout);

    /*
     * Per-generation visibility order:
     *
     * NC writer                                      CC reader
     * ---------                                      ---------
     * write header/body/trailer
     * store-release descriptor = generation  ----->  load-acquire descriptor
     *                                                inspect header/body/trailer
     * load-acquire ack              <-------------  store-release ack = generation
     * write next generation
     *
     * These acquire/release operations order each endpoint's accesses around
     * descriptor/ack publication. They do not make all frame cachelines
     * atomically visible at the receiver; detecting that violation is this
     * UT's purpose.
     */
    for (uint64_t generation = 1; generation <= options->iterations; generation++) {
        /* Do not reuse the frame until the reader has inspected the prior generation. */
        if (wait_for_value(ack_word, generation - 1,
                           options->timeout_seconds, "ack") != 0)
            goto done;

        /* Construct all header/body/trailer cachelines before publication. */
        write_frame(frame, words, options->seed, generation,
                    generation == options->inject_mixed_at);

        /* Release-publish the descriptor after the complete frame write. */
        store_release(descriptor, generation);

        /*
         * The reader acquire-observes this descriptor, inspects the frame,
         * then release-publishes this ack. The ack prevents writer reuse; it
         * does not itself guarantee range-atomic visibility to the reader.
         */
        if (wait_for_value(ack_word, generation,
                           options->timeout_seconds, "ack") != 0)
            goto done;
    }
    printf("FRAME_WRITER_COMPLETE iterations=%llu inject_mixed_at=%llu\n",
           (unsigned long long)options->iterations,
           (unsigned long long)options->inject_mixed_at);
    rc = 0;

done:
    unmap_range(&ack);
    unmap_range(&data);
    return rc;
}

static int run_frame_reader_cc(const options_t *options) {
    mapped_range_t data = {.fd = -1, .mapping = MAP_FAILED};
    mapped_range_t ack = {.fd = -1, .mapping = MAP_FAILED};
    const volatile uint64_t *frame;
    const volatile atomic_u64_t *descriptor;
    volatile atomic_u64_t *ack_word;
    size_t words = (size_t)(options->frame_bytes / sizeof(uint64_t));
    int rc = 1;

    if (map_range(options->path, options->offset,
                  (size_t)options->frame_bytes + CACHELINE_BYTES,
                  O_RDWR, &data) != 0 ||
        map_range(options->ack_path, options->offset, CACHELINE_BYTES,
                  O_RDWR | O_SYNC, &ack) != 0)
        goto done;

    frame = (const volatile uint64_t *)data.data;
    descriptor = (const volatile atomic_u64_t *)(data.data + options->frame_bytes);
    ack_word = (volatile atomic_u64_t *)ack.data;
    printf("FRAME_READER_READY data=%s ack=%s offset=%llu frame_bytes=%llu iterations=%llu expect_mixed=%d expect_visibility_failure=%d read_delay_ns=%llu\n",
           options->path, options->ack_path,
           (unsigned long long)options->offset,
           (unsigned long long)options->frame_bytes,
           (unsigned long long)options->iterations, options->expect_mixed,
           options->expect_visibility_failure,
           (unsigned long long)options->read_delay_ns);
    fflush(stdout);

    for (uint64_t generation = 1; generation <= options->iterations; generation++) {
        int inspection;

        if (wait_for_value(descriptor, generation,
                           options->timeout_seconds, "descriptor") != 0)
            goto done;
        delay_after_descriptor(options->read_delay_ns);

        inspection = inspect_frame(frame, words, options->seed,
                                   generation, load_acquire(descriptor));
        store_release(ack_word, generation);
        if (inspection == 1) {
            rc = options->expect_mixed || options->expect_visibility_failure ? 0 : 2;
            goto done;
        }
        if (inspection != 0) {
            rc = options->expect_visibility_failure ? 0 : 2;
            goto done;
        }
    }
    printf("NOT_REPRODUCED iterations=%llu\n",
           (unsigned long long)options->iterations);
    rc = options->expect_mixed || options->expect_visibility_failure ? 3 : 0;

done:
    unmap_range(&ack);
    unmap_range(&data);
    return rc;
}

int main(int argc, char **argv) {
    options_t options;

    monotonicInit();
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
    if (!strcmp(options.mode, "reader-cc"))
        return run_reader(&options, O_RDWR, "O_RDWR(CC)");
    if (!strcmp(options.mode, "frame-writer-nc"))
        return run_frame_writer_nc(&options);
    return run_frame_reader_cc(&options);
}
