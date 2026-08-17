#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include "monotonic.h"
#include "sve_operation.h"
#include "vemb_v16_protocol.h"

#define DEFAULT_ROWS 131072u
#define DEFAULT_BATCH 64u
#define DEFAULT_ITERS 2000u
#define DEFAULT_WINDOW_LINES 512u
#ifndef UB_BATCH_PREFETCH_BUDGET_BYTES
#define UB_BATCH_PREFETCH_BUDGET_BYTES (16u * 1024u)
#endif
#ifndef UB_BATCH_PREFETCH_BUCKET_SHIFT
#define UB_BATCH_PREFETCH_BUCKET_SHIFT 16u
#endif
#ifndef UB_BATCH_PREFETCH_WINDOW_VECTORS
#define UB_BATCH_PREFETCH_WINDOW_VECTORS 16u
#endif
#ifndef UB_BATCH_PREFETCH_HOT_BUCKET_SPAN
#define UB_BATCH_PREFETCH_HOT_BUCKET_SPAN 8u
#endif
#ifndef UB_BATCH_PREFETCH_SAMPLE_VECTORS
#define UB_BATCH_PREFETCH_SAMPLE_VECTORS 16u
#endif
#ifndef UB_BATCH_PREFETCH_TOPK_BUDGET_LINES
#define UB_BATCH_PREFETCH_TOPK_BUDGET_LINES 128u
#endif
#ifndef UB_BATCH_CLUSTER_BUCKETS
#define UB_BATCH_CLUSTER_BUCKETS 16u
#endif
#ifndef UB_BATCH_ENABLE_CHECKSUM
#define UB_BATCH_ENABLE_CHECKSUM 0
#endif
#ifndef UB_BATCH_CACHELINE
#define UB_BATCH_CACHELINE 64u
#endif
#if UB_BATCH_CACHELINE == 0 || (UB_BATCH_CACHELINE & (UB_BATCH_CACHELINE - 1u)) != 0
#error "UB_BATCH_CACHELINE must be a non-zero power of two"
#endif

typedef enum access_pattern {
    ACCESS_SEQ = 0,
    ACCESS_RANDOM = 1,
    ACCESS_HOT = 2,
    ACCESS_CLUSTER = 3,
} access_pattern_t;

typedef enum copy_mode {
    COPY_MEMCPY = 0,
    COPY_SVE_STREAM = 1,
} copy_mode_t;

typedef struct bench_opts {
    const char *ub_path;
    size_t map_size;
    size_t table_offset;
    size_t rows;
    size_t dim;
    size_t stride;
    size_t batch;
    size_t iters;
    size_t window_lines;
    access_pattern_t pattern;
    copy_mode_t copy_mode;
    int cacheable;
    int fill;
} bench_opts_t;

typedef struct ub_region {
    uint8_t *base;
    uint8_t *mapping;
    size_t mapping_size;
    int fd;
    int owned;
} ub_region_t;

typedef struct ub_handle {
    uint32_t region_index;
    size_t offset;
    size_t bytes;
} ub_handle_t;

typedef struct cacheline_run {
    uint32_t region_index;
    uintptr_t begin;
    uintptr_t end;
} cacheline_run_t;

// One request's vector payload range, expanded to cacheline-aligned addresses.
typedef struct prefetch_span {
    uint32_t region_index;   // Memory region that owns this vector payload.
    uint64_t bucket_prefix;  // Coarse address bucket key, derived from begin.
    uintptr_t begin;         // Cacheline-aligned inclusive start address.
    uintptr_t end;           // Cacheline-aligned exclusive end address.
    size_t lines;            // Cachelines covered by [begin, end).
    size_t request_index;    // Original request position in the batch.
    size_t bucket_index;     // Assigned bucket index after grouping.
    int64_t score;           // Reserved for span-level scoring experiments.
} prefetch_span_t;

typedef struct prefetch_bucket {
    uint32_t region_index;
    uint64_t prefix;
    size_t span_count;
    size_t line_estimate;
    uintptr_t min_begin;
    uintptr_t max_end;
    size_t first_request_index;
    int64_t score;
    int selected;
} prefetch_bucket_t;

typedef struct prefetch_plan_stats {
    uint64_t candidate_spans;
    uint64_t candidate_lines;
    uint64_t candidate_buckets;
    uint64_t candidate_bucket_span;
    uint64_t selected_spans;
    uint64_t selected_lines;
    uint64_t selected_buckets;
    uint64_t prefetch_budget_lines;
    const char *plan_policy;
} prefetch_plan_stats_t;

typedef struct bench_result {
    const char *name;
    uint64_t ns;
    uint64_t plan_ns;
    uint64_t prefetch_ns;
    uint64_t total_runs;
    uint64_t total_prefetch_lines;
    uint64_t candidate_spans;
    uint64_t candidate_lines;
    uint64_t candidate_buckets;
    uint64_t candidate_bucket_span;
    uint64_t selected_spans;
    uint64_t selected_lines;
    uint64_t selected_buckets;
    uint64_t total_budget_lines;
    const char *plan_policy;
    double checksum;
} bench_result_t;

static size_t parse_size(const char *text) {
    char *end = NULL;
    unsigned long long value = strtoull(text, &end, 0);
    if (end == text)
        return 0;
    switch (*end) {
    case 'k':
    case 'K':
        return (size_t)(value * 1024ull);
    case 'm':
    case 'M':
        return (size_t)(value * 1024ull * 1024ull);
    case 'g':
    case 'G':
        return (size_t)(value * 1024ull * 1024ull * 1024ull);
    default:
        return (size_t)value;
    }
}

static uint64_t xorshift64(uint64_t *state) {
    uint64_t x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    return x;
}

static int cmp_run(const void *a, const void *b) {
    const cacheline_run_t *ra = a;
    const cacheline_run_t *rb = b;
    if (ra->region_index != rb->region_index)
        return (ra->region_index < rb->region_index) ? -1 : 1;
    if (ra->begin != rb->begin)
        return (ra->begin < rb->begin) ? -1 : 1;
    if (ra->end != rb->end)
        return (ra->end < rb->end) ? -1 : 1;
    return 0;
}

static uintptr_t align_down_uintptr(uintptr_t value, size_t align) {
    return value & ~(uintptr_t)(align - 1u);
}

static uintptr_t align_up_uintptr(uintptr_t value, size_t align) {
    return (value + align - 1u) & ~(uintptr_t)(align - 1u);
}

static float expected_value(size_t row, size_t col) {
    return (float)((row * 131u + col * 17u) & 0xffffu) * 0.001f;
}

static void fill_region(ub_region_t *region, const bench_opts_t *opts) {
    for (size_t row = 0; row < opts->rows; row++) {
        float *dst = (float *)(void *)(region->base + row * opts->stride);
        for (size_t col = 0; col < opts->dim; col++)
            dst[col] = expected_value(row, col);
    }
}

static int setup_region(ub_region_t *region, const bench_opts_t *opts) {
    memset(region, 0, sizeof(*region));
    region->fd = -1;
    region->mapping_size = opts->table_offset + opts->rows * opts->stride;
    if (opts->map_size && opts->map_size > region->mapping_size)
        region->mapping_size = opts->map_size;

    if (opts->ub_path) {
        int flags = O_RDWR;
        if (!opts->cacheable)
            flags |= O_SYNC;
        region->fd = open(opts->ub_path, flags);
        if (region->fd < 0) {
            fprintf(stderr, "open %s failed: %s\n", opts->ub_path, strerror(errno));
            return -1;
        }
        int prot = (opts->fill || !opts->cacheable) ? (PROT_READ | PROT_WRITE) : PROT_READ;
        region->mapping = mmap(NULL,
                               region->mapping_size,
                               prot,
                               MAP_SHARED,
                               region->fd,
                               0);
        if (region->mapping == MAP_FAILED) {
            fprintf(stderr, "mmap %s failed: %s\n", opts->ub_path, strerror(errno));
            close(region->fd);
            return -1;
        }
        region->owned = 0;
    } else {
        if (posix_memalign((void **)&region->mapping, UB_BATCH_CACHELINE, region->mapping_size) != 0)
            return -1;
        memset(region->mapping, 0, region->mapping_size);
        region->owned = 1;
    }

    region->base = region->mapping + opts->table_offset;
    return 0;
}

static void teardown_region(ub_region_t *region) {
    if (region->owned)
        free(region->mapping);
    else if (region->mapping && region->mapping != MAP_FAILED)
        munmap(region->mapping, region->mapping_size);
    if (region->fd >= 0)
        close(region->fd);
}

static void build_handles(ub_handle_t *handles, const bench_opts_t *opts) {
    for (size_t row = 0; row < opts->rows; row++) {
        handles[row] = (ub_handle_t){
            .region_index = 0,
            .offset = row * opts->stride,
            .bytes = opts->dim * sizeof(float),
        };
    }
}

static void build_batch_request(uint32_t *requests, const bench_opts_t *opts) {
    size_t hot_rows = opts->rows / 32u;
    size_t rows_per_bucket = ((size_t)1u << UB_BATCH_PREFETCH_BUCKET_SHIFT) / opts->stride;
    size_t cluster_buckets = UB_BATCH_CLUSTER_BUCKETS;
    size_t cluster_rows;
    uint64_t rng = 0x123456789abcdef0ull;

    if (hot_rows < opts->batch)
        hot_rows = opts->batch;
    if (hot_rows > opts->rows)
        hot_rows = opts->rows;
    if (rows_per_bucket == 0)
        rows_per_bucket = 1;
    if (cluster_buckets == 0)
        cluster_buckets = 1;
    cluster_rows = rows_per_bucket * cluster_buckets;
    if (cluster_rows > opts->rows) {
        cluster_rows = opts->rows;
        cluster_buckets = (cluster_rows + rows_per_bucket - 1u) / rows_per_bucket;
        if (cluster_buckets == 0)
            cluster_buckets = 1;
    }

    for (size_t iter = 0; iter < opts->iters; iter++) {
        uint32_t *ids = requests + iter * opts->batch;
        size_t cluster_base = 0;
        if (opts->pattern == ACCESS_CLUSTER && opts->rows > cluster_rows)
            cluster_base = (size_t)(xorshift64(&rng) % (opts->rows - cluster_rows));
        for (size_t i = 0; i < opts->batch; i++) {
            size_t row;
            switch (opts->pattern) {
            case ACCESS_RANDOM:
                row = (size_t)(xorshift64(&rng) % opts->rows);
                break;
            case ACCESS_HOT:
                row = (size_t)(xorshift64(&rng) % hot_rows);
                break;
            case ACCESS_CLUSTER: {
                size_t bucket = cluster_buckets ? (i % cluster_buckets) : 0;
                size_t in_bucket = (size_t)(xorshift64(&rng) % rows_per_bucket);
                row = cluster_base + bucket * rows_per_bucket + in_bucket;
                if (row >= opts->rows)
                    row = opts->rows - 1u;
                break;
            }
            case ACCESS_SEQ:
            default:
                row = (iter * opts->batch + i) % opts->rows;
                break;
            }
            ids[i] = (uint32_t)row;
        }
    }
}

static size_t run_lines(const cacheline_run_t *run) {
    return (size_t)((run->end - run->begin) / UB_BATCH_CACHELINE);
}

static size_t effective_prefetch_budget_lines(const bench_opts_t *opts) {
    size_t budget = UB_BATCH_PREFETCH_BUDGET_BYTES / UB_BATCH_CACHELINE;
    if (opts->window_lines && opts->window_lines < budget)
        budget = opts->window_lines;
    return budget;
}

static size_t find_or_add_bucket(prefetch_bucket_t *buckets,
                                 size_t *bucket_count,
                                 uint32_t region_index,
                                 uint64_t prefix,
                                 uintptr_t begin,
                                 uintptr_t end,
                                 size_t lines,
                                 size_t request_index) {
    for (size_t i = 0; i < *bucket_count; i++) {
        if (buckets[i].region_index != region_index || buckets[i].prefix != prefix)
            continue;
        buckets[i].span_count++;
        buckets[i].line_estimate += lines;
        if (begin < buckets[i].min_begin)
            buckets[i].min_begin = begin;
        if (end > buckets[i].max_end)
            buckets[i].max_end = end;
        if (request_index < buckets[i].first_request_index)
            buckets[i].first_request_index = request_index;
        return i;
    }

    size_t bucket_index = (*bucket_count)++;
    buckets[bucket_index] = (prefetch_bucket_t){
        .region_index = region_index,
        .prefix = prefix,
        .span_count = 1,
        .line_estimate = lines,
        .min_begin = begin,
        .max_end = end,
        .first_request_index = request_index,
    };
    return bucket_index;
}

static void add_span_to_bucket(prefetch_bucket_t *bucket,
                               uintptr_t begin,
                               uintptr_t end,
                               size_t lines,
                               size_t request_index) {
    bucket->span_count++;
    bucket->line_estimate += lines;
    if (begin < bucket->min_begin)
        bucket->min_begin = begin;
    if (end > bucket->max_end)
        bucket->max_end = end;
    if (request_index < bucket->first_request_index)
        bucket->first_request_index = request_index;
}

static size_t merge_runs(cacheline_run_t *runs, size_t run_count) {
    qsort(runs, run_count, sizeof(runs[0]), cmp_run);

    size_t out = 0;
    for (size_t i = 0; i < run_count; i++) {
        if (out > 0 &&
            runs[out - 1u].region_index == runs[i].region_index &&
            runs[i].begin <= runs[out - 1u].end) {
            if (runs[i].end > runs[out - 1u].end)
                runs[out - 1u].end = runs[i].end;
            continue;
        }
        runs[out++] = runs[i];
    }
    return out;
}

static size_t merge_runs_small(cacheline_run_t *runs, size_t run_count) {
    for (size_t i = 1; i < run_count; i++) {
        cacheline_run_t run = runs[i];
        size_t j = i;
        while (j > 0 && cmp_run(&run, &runs[j - 1u]) < 0) {
            runs[j] = runs[j - 1u];
            j--;
        }
        runs[j] = run;
    }

    size_t out = 0;
    for (size_t i = 0; i < run_count; i++) {
        if (out > 0 &&
            runs[out - 1u].region_index == runs[i].region_index &&
            runs[i].begin <= runs[out - 1u].end) {
            if (runs[i].end > runs[out - 1u].end)
                runs[out - 1u].end = runs[i].end;
            continue;
        }
        runs[out++] = runs[i];
    }
    return out;
}

static int spans_are_request_order_contiguous(const prefetch_span_t *spans, size_t span_count) {
    if (span_count <= 1)
        return 1;
    for (size_t i = 1; i < span_count; i++) {
        if (spans[i].region_index != spans[i - 1u].region_index)
            return 0;
        if (spans[i].begin > spans[i - 1u].end)
            return 0;
        if (spans[i].end < spans[i - 1u].end)
            return 0;
    }
    return 1;
}

static int bucket_better_for_prefetch(const prefetch_bucket_t *candidate,
                                      const prefetch_bucket_t *current) {
    // Prefer the bucket with the highest reuse/urgency/cost score.
    if (candidate->score != current->score)
        return candidate->score > current->score;
    // If scores tie, choose the bucket needed earlier in request order.
    if (candidate->first_request_index != current->first_request_index)
        return candidate->first_request_index < current->first_request_index;
    // If urgency also ties, spend budget on the smaller cacheline footprint.
    if (candidate->line_estimate != current->line_estimate)
        return candidate->line_estimate < current->line_estimate;
    // Finally, use prefix as a stable deterministic tie-breaker.
    return candidate->prefix < current->prefix;
}

static prefetch_span_t make_prefetch_span(const ub_region_t *region,
                                          const ub_handle_t *handle,
                                          size_t request_index) {
    uintptr_t begin = align_down_uintptr((uintptr_t)(region->base + handle->offset),
                                         UB_BATCH_CACHELINE);
    uintptr_t end = align_up_uintptr((uintptr_t)(region->base + handle->offset + handle->bytes),
                                     UB_BATCH_CACHELINE);
    return (prefetch_span_t){
        .region_index = handle->region_index,
        .bucket_prefix = (uint64_t)(begin >> UB_BATCH_PREFETCH_BUCKET_SHIFT),
        .begin = begin,
        .end = end,
        .lines = (size_t)((end - begin) / UB_BATCH_CACHELINE),
        .request_index = request_index,
    };
}

static void collect_prefetch_spans(const ub_region_t *region,
                                   const ub_handle_t *handles,
                                   const uint32_t *ids,
                                   size_t begin_index,
                                   size_t end_index,
                                   prefetch_span_t *spans,
                                   uint32_t *first_region_index,
                                   uint64_t *min_prefix,
                                   uint64_t *max_prefix,
                                   int *single_region,
                                   int *request_order_contiguous,
                                   uintptr_t *prev_end,
                                   prefetch_plan_stats_t *stats) {
    for (size_t i = begin_index; i < end_index; i++) {
        const ub_handle_t *handle = &handles[ids[i]];
        prefetch_span_t span = make_prefetch_span(region, handle, i);

        if (i == 0)
            *first_region_index = span.region_index;
        else {
            if (span.region_index != *first_region_index)
                *single_region = 0;
            if (request_order_contiguous) {
                // Treat contiguous as a forward stream, not arbitrary overlap with the previous span.
                if (span.region_index != *first_region_index ||
                    span.begin > *prev_end ||
                    span.end < *prev_end)
                    *request_order_contiguous = 0;
            }
        }
        if (span.bucket_prefix < *min_prefix)
            *min_prefix = span.bucket_prefix;
        if (span.bucket_prefix > *max_prefix)
            *max_prefix = span.bucket_prefix;
        if (prev_end)
            *prev_end = span.end;
        spans[i] = span;

        stats->candidate_spans++;
        stats->candidate_lines += span.lines;
    }
}

static int sample_prefetch_skip_policy(const ub_region_t *region,
                                       const ub_handle_t *handles,
                                       const uint32_t *ids,
                                       const bench_opts_t *opts,
                                       size_t budget_lines,
                                       prefetch_span_t *spans,
                                       size_t *sample_count_out,
                                       uint32_t *first_region_index_out,
                                       uint64_t *min_prefix_out,
                                       uint64_t *max_prefix_out,
                                       int *single_region_out,
                                       prefetch_plan_stats_t *stats) {
    // Sample vectors only bound the fast-skip probe, not the emitted prefetch lines.
    size_t sample_count = opts->batch < UB_BATCH_PREFETCH_SAMPLE_VECTORS ?
        opts->batch : UB_BATCH_PREFETCH_SAMPLE_VECTORS;
    uint32_t first_region_index = 0;
    uint64_t min_prefix = UINT64_MAX;
    uint64_t max_prefix = 0;
    int single_region = 1;
    int sample_contiguous = 1;
    uintptr_t prev_end = 0;

    if (budget_lines == 0) {
        // No cacheline budget means there is nothing useful to prefetch.
        *sample_count_out = 0;
        stats->plan_policy = "no_prefetch";
        return 1;
    }
    if (!opts->cacheable) {
        // Non-cacheable mappings should not spend cycles on cache prefetch.
        *sample_count_out = 0;
        stats->plan_policy = "no_prefetch_nc";
        return 1;
    }
    if (sample_count == 0) {
        // No sample is available, so let the full planner decide.
        *sample_count_out = 0;
        return 0;
    }

    *sample_count_out = sample_count;
    // Materialize sampled spans once so full TopK planning can reuse them.
    collect_prefetch_spans(region,
                           handles,
                           ids,
                           0,
                           sample_count,
                           spans,
                           &first_region_index,
                           &min_prefix,
                           &max_prefix,
                           &single_region,
                           &sample_contiguous,
                           &prev_end,
                           stats);

    size_t bucket_span = max_prefix >= min_prefix ? (size_t)(max_prefix - min_prefix + 1u) : 0;
    stats->candidate_bucket_span = bucket_span;
    *first_region_index_out = first_region_index;
    *min_prefix_out = min_prefix;
    *max_prefix_out = max_prefix;
    *single_region_out = single_region;
    if (sample_contiguous) {
        // Forward-contiguous streams are already friendly to hardware prefetch.
        stats->plan_policy = "no_prefetch_contig_sample";
        return 1;
    }
    if (single_region && bucket_span <= UB_BATCH_PREFETCH_HOT_BUCKET_SPAN) {
        // A tiny bucket span is likely hot/cache-resident, so manual prefetch is overhead.
        stats->plan_policy = "no_prefetch_hot_sample";
        return 1;
    }
    if (single_region && bucket_span >= opts->batch) {
        // A wide bucket span looks sparse, where manual prefetch is likely pollution.
        stats->plan_policy = "no_prefetch_sparse_sample";
        return 1;
    }
    // The sample did not prove prefetch should be skipped, so continue full TopK planning.
    return 0;
}

static size_t build_sort_merge_plan(cacheline_run_t *runs,
                                    const ub_region_t *region,
                                    const ub_handle_t *handles,
                                    const uint32_t *ids,
                                    const bench_opts_t *opts,
                                    prefetch_plan_stats_t *stats) {
    for (size_t i = 0; i < opts->batch; i++) {
        const ub_handle_t *handle = &handles[ids[i]];
        uintptr_t begin = (uintptr_t)(region->base + handle->offset);
        uintptr_t end = begin + handle->bytes;
        runs[i] = (cacheline_run_t){
            .region_index = handle->region_index,
            .begin = align_down_uintptr(begin, UB_BATCH_CACHELINE),
            .end = align_up_uintptr(end, UB_BATCH_CACHELINE),
        };
        if (stats) {
            stats->candidate_spans++;
            stats->candidate_lines += run_lines(&runs[i]);
        }
    }

    if (stats) {
        stats->selected_spans = stats->candidate_spans;
        stats->selected_lines = stats->candidate_lines;
        stats->prefetch_budget_lines = effective_prefetch_budget_lines(opts);
        stats->plan_policy = "sort_merge";
    }
    return merge_runs(runs, opts->batch);
}

static size_t build_budgeted_topk_plan(cacheline_run_t *runs,
                                       prefetch_span_t *spans,
                                       prefetch_bucket_t *buckets,
                                       const ub_region_t *region,
                                       const ub_handle_t *handles,
                                       const uint32_t *ids,
                                       const bench_opts_t *opts,
                                       prefetch_plan_stats_t *stats) {
    size_t bucket_count = 0;
    size_t budget_lines = effective_prefetch_budget_lines(opts);
    uint32_t first_region_index = 0;
    uint64_t min_prefix = UINT64_MAX;
    uint64_t max_prefix = 0;
    int single_region = 1;
    size_t sample_count = 0;

    stats->prefetch_budget_lines = budget_lines;
    // Fast-skip obvious no-prefetch batches before paying for full TopK planning.
    if (sample_prefetch_skip_policy(region,
                                    handles,
                                    ids,
                                    opts,
                                    budget_lines,
                                    spans,
                                    &sample_count,
                                    &first_region_index,
                                    &min_prefix,
                                    &max_prefix,
                                    &single_region,
                                    stats)) {
        return 0;
    }

    // Fill only the suffix not already populated by the sample probe.
    collect_prefetch_spans(region,
                           handles,
                           ids,
                           sample_count,
                           opts->batch,
                           spans,
                           &first_region_index,
                           &min_prefix,
                           &max_prefix,
                           &single_region,
                           NULL,
                           NULL,
                           stats);

    size_t bucket_span = max_prefix >= min_prefix ? (size_t)(max_prefix - min_prefix + 1u) : 0;
    stats->candidate_bucket_span = bucket_span;
    if (spans_are_request_order_contiguous(spans, opts->batch)) {
        stats->plan_policy = "no_prefetch_contig";
        return 0;
    }
    if (single_region && bucket_span <= UB_BATCH_PREFETCH_HOT_BUCKET_SPAN) {
        stats->plan_policy = "no_prefetch_hot_fast";
        return 0;
    }
    if (single_region && bucket_span >= opts->batch) {
        stats->plan_policy = "no_prefetch_sparse_fast";
        return 0;
    }

    // Single-region compact prefixes can use direct slots instead of bucket lookup.
    if (single_region && bucket_span > 0 && bucket_span <= opts->batch) {
        // Fill every byte with 0xff so each size_t slot becomes SIZE_MAX.
        size_t prefix_slots[bucket_span];
        memset(prefix_slots, 0xff, sizeof(prefix_slots));
        for (size_t i = 0; i < opts->batch; i++) {
            size_t slot = (size_t)(spans[i].bucket_prefix - min_prefix);
            if (prefix_slots[slot] == SIZE_MAX) {
                size_t bucket_index = bucket_count++;
                prefix_slots[slot] = bucket_index;
                buckets[bucket_index] = (prefetch_bucket_t){
                    .region_index = spans[i].region_index,
                    .prefix = spans[i].bucket_prefix,
                    .span_count = 1,
                    .line_estimate = spans[i].lines,
                    .min_begin = spans[i].begin,
                    .max_end = spans[i].end,
                    .first_request_index = spans[i].request_index,
                };
                spans[i].bucket_index = bucket_index;
            } else {
                spans[i].bucket_index = prefix_slots[slot];
                add_span_to_bucket(&buckets[spans[i].bucket_index],
                                   spans[i].begin,
                                   spans[i].end,
                                   spans[i].lines,
                                   spans[i].request_index);
            }
        }
    } else {
        // Multi-region or sparse prefixes need generic region+prefix bucket lookup.
        for (size_t i = 0; i < opts->batch; i++) {
            spans[i].bucket_index = find_or_add_bucket(buckets,
                                                       &bucket_count,
                                                       spans[i].region_index,
                                                       spans[i].bucket_prefix,
                                                       spans[i].begin,
                                                       spans[i].end,
                                                       spans[i].lines,
                                                       spans[i].request_index);
        }
    }
    stats->candidate_buckets = bucket_count;

    // Too few buckets means the batch is already hot/local enough.
    if (bucket_count <= 8u) {
        stats->plan_policy = "no_prefetch_hot";
        return 0;
    }

    // Too many buckets means the batch is sparse and likely to pollute cache.
    if (bucket_count * 2u >= opts->batch) {
        stats->plan_policy = "no_prefetch_sparse";
        return 0;
    }

    if (budget_lines > UB_BATCH_PREFETCH_TOPK_BUDGET_LINES)
        budget_lines = UB_BATCH_PREFETCH_TOPK_BUDGET_LINES;
    stats->prefetch_budget_lines = budget_lines;

    size_t selected_lines = 0;
    size_t selected_runs = 0;
    size_t selected_buckets = 0;
    // Score buckets by rewarding reuse and early access, then charging cacheline cost.
    for (size_t i = 0; i < bucket_count; i++) {
        int64_t locality_score = (int64_t)buckets[i].span_count * 8;
        int64_t urgency_score = buckets[i].first_request_index < UB_BATCH_PREFETCH_WINDOW_VECTORS ?
            (int64_t)(UB_BATCH_PREFETCH_WINDOW_VECTORS - buckets[i].first_request_index) : 0;
        int64_t cost_score = (int64_t)buckets[i].line_estimate;
        buckets[i].score = locality_score + urgency_score - cost_score;
    }

    // Greedily select the best-scored buckets that still fit in the prefetch budget.
    while (selected_lines < budget_lines && selected_buckets < bucket_count) {
        size_t best_index = SIZE_MAX;
        for (size_t i = 0; i < bucket_count; i++) {
            if (buckets[i].selected)
                continue;
            if (buckets[i].line_estimate > budget_lines - selected_lines)
                continue;
            if (best_index == SIZE_MAX ||
                bucket_better_for_prefetch(&buckets[i], &buckets[best_index]))
                best_index = i;
        }
        if (best_index == SIZE_MAX)
            break;
        buckets[best_index].selected = 1;
        selected_lines += buckets[best_index].line_estimate;
        selected_buckets++;
    }

    selected_lines = 0;
    // Expand selected buckets back into request-order spans for final prefetch runs.
    for (size_t i = 0; i < opts->batch && selected_lines < budget_lines; i++) {
        prefetch_span_t *span = &spans[i];
        if (!buckets[span->bucket_index].selected)
            continue;
        if (span->lines > budget_lines - selected_lines)
            continue;
        selected_lines += span->lines;
        runs[selected_runs++] = (cacheline_run_t){
            .region_index = span->region_index,
            .begin = span->begin,
            .end = span->end,
        };
    }

    // Merge adjacent or overlapping spans to reduce the final prefetch run count.
    size_t run_count = selected_runs ? merge_runs_small(runs, selected_runs) : 0;
    size_t merged_lines = 0;
    for (size_t i = 0; i < run_count; i++)
        merged_lines += run_lines(&runs[i]);

    stats->selected_spans = selected_runs;
    stats->selected_lines = merged_lines;
    stats->selected_buckets = selected_buckets;
    stats->plan_policy = "bucket_topk";

    return run_count;
}

static inline const char *prefetch_mode_name(void) {
#ifdef UB_BATCH_PREFETCH_PLDL1KEEP
    return "pld";
#else
    return "builtin";
#endif
}

static const char *access_pattern_name(access_pattern_t pattern) {
    switch (pattern) {
    case ACCESS_SEQ:
        return "seq";
    case ACCESS_RANDOM:
        return "random";
    case ACCESS_HOT:
        return "hot";
    case ACCESS_CLUSTER:
        return "cluster";
    default:
        return "unknown";
    }
}

static inline void prefetch_cacheline(const void *addr) {
#if defined(UB_BATCH_PREFETCH_PLDL1KEEP) && defined(__aarch64__)
    __asm__ volatile("prfm pldl1keep, [%0]" :: "r"(addr) : "memory");
#else
    __builtin_prefetch(addr, 0, 3);
#endif
}

static size_t prefetch_runs(const cacheline_run_t *runs,
                            size_t run_count,
                            const bench_opts_t *opts) {
    size_t lines = 0;
    size_t budget = opts->window_lines;
    for (size_t r = 0; r < run_count && lines < budget; r++) {
        for (uintptr_t addr = runs[r].begin; addr < runs[r].end && lines < budget;
             addr += UB_BATCH_CACHELINE) {
            prefetch_cacheline((const void *)addr);
            lines++;
        }
    }
    return lines;
}

static void load_vector(uint8_t *out,
                         const ub_region_t *region,
                         const ub_handle_t *handles,
                         const uint32_t *ids,
                         const bench_opts_t *opts) {
    const size_t size = opts->dim * sizeof(float);
    for (size_t i = 0; i < opts->batch; i++) {
        const ub_handle_t *handle = &handles[ids[i]];
        void *dst = out + i * size;
        const void *src = region->base + handle->offset;
        sve_streaming_load_f32(src, dst, size);
    }
}

#if UB_BATCH_ENABLE_CHECKSUM
static double checksum_batch(const uint8_t *out, const bench_opts_t *opts) {
    const float *values = (const float *)(const void *)out;
    double sum = 0.0;
    for (size_t i = 0; i < opts->batch; i++) {
        const float *row = values + i * opts->dim;
        sum += row[0];
        sum += row[opts->dim / 2u] * 0.5;
        sum += row[opts->dim - 1u] * 0.25;
    }
    return sum;
}
#endif

static bench_result_t run_scalar(const ub_region_t *region,
                                 const ub_handle_t *handles,
                                 const uint32_t *requests,
                                 uint8_t *out,
                                 const bench_opts_t *opts) {
    double checksum = 0.0;
    monotime start;
    elapsedStartNs(&start);
    for (size_t iter = 0; iter < opts->iters; iter++) {
        const uint32_t *ids = requests + iter * opts->batch;
        load_vector(out, region, handles, ids, opts);
#if UB_BATCH_ENABLE_CHECKSUM
        checksum += checksum_batch(out, opts);
#endif
    }
    return (bench_result_t){
        .name = opts->copy_mode == COPY_SVE_STREAM ? "scalar-sve" : "scalar-memcpy",
        .ns = elapsedNs(start),
        .checksum = checksum,
    };
}

static bench_result_t run_planned(const ub_region_t *region,
                                  const ub_handle_t *handles,
                                  const uint32_t *requests,
                                  uint8_t *out,
                                  cacheline_run_t *runs,
                                  prefetch_span_t *spans,
                                  prefetch_bucket_t *buckets,
                                  const bench_opts_t *opts,
                                  int do_prefetch) {
    double checksum = 0.0;
    uint64_t plan_ns = 0;
    uint64_t prefetch_ns = 0;
    uint64_t total_runs = 0;
    uint64_t total_prefetch_lines = 0;
    prefetch_plan_stats_t total_stats = {0};

    monotime start;
    elapsedStartNs(&start);
    if (do_prefetch) {
        for (size_t iter = 0; iter < opts->iters; iter++) {
            const uint32_t *ids = requests + iter * opts->batch;
            prefetch_plan_stats_t stats = {0};
            monotime plan_start;
            elapsedStartNs(&plan_start);
            size_t run_count = build_budgeted_topk_plan(runs,
                                                        spans,
                                                        buckets,
                                                        region,
                                                        handles,
                                                        ids,
                                                        opts,
                                                        &stats);
            plan_ns += elapsedNs(plan_start);
            total_runs += run_count;
            total_stats.candidate_spans += stats.candidate_spans;
            total_stats.candidate_lines += stats.candidate_lines;
            total_stats.candidate_buckets += stats.candidate_buckets;
            total_stats.candidate_bucket_span += stats.candidate_bucket_span;
            total_stats.selected_spans += stats.selected_spans;
            total_stats.selected_lines += stats.selected_lines;
            total_stats.selected_buckets += stats.selected_buckets;
            total_stats.prefetch_budget_lines += stats.prefetch_budget_lines;
            total_stats.plan_policy = stats.plan_policy;

            if (run_count > 0) {
                monotime prefetch_start;
                elapsedStartNs(&prefetch_start);
                total_prefetch_lines += prefetch_runs(runs, run_count, opts);
                prefetch_ns += elapsedNs(prefetch_start);
            }

            load_vector(out, region, handles, ids, opts);
#if UB_BATCH_ENABLE_CHECKSUM
            checksum += checksum_batch(out, opts);
#endif
        }
    } else {
        for (size_t iter = 0; iter < opts->iters; iter++) {
            const uint32_t *ids = requests + iter * opts->batch;
            prefetch_plan_stats_t stats = {0};
            monotime plan_start;
            elapsedStartNs(&plan_start);
            size_t run_count = build_sort_merge_plan(runs, region, handles, ids, opts, &stats);
            plan_ns += elapsedNs(plan_start);
            total_runs += run_count;
            total_stats.candidate_spans += stats.candidate_spans;
            total_stats.candidate_lines += stats.candidate_lines;
            total_stats.candidate_buckets += stats.candidate_buckets;
            total_stats.candidate_bucket_span += stats.candidate_bucket_span;
            total_stats.selected_spans += stats.selected_spans;
            total_stats.selected_lines += stats.selected_lines;
            total_stats.prefetch_budget_lines += stats.prefetch_budget_lines;
            total_stats.plan_policy = stats.plan_policy;

            load_vector(out, region, handles, ids, opts);
#if UB_BATCH_ENABLE_CHECKSUM
            checksum += checksum_batch(out, opts);
#endif
        }
    }

    return (bench_result_t){
        .name = do_prefetch ?
            (opts->copy_mode == COPY_SVE_STREAM ? "batch-sve-prefetch" : "batch-prefetch") :
            (opts->copy_mode == COPY_SVE_STREAM ? "batch-sve-plan" : "batch-plan"),
        .ns = elapsedNs(start),
        .plan_ns = plan_ns,
        .prefetch_ns = prefetch_ns,
        .total_runs = total_runs,
        .total_prefetch_lines = total_prefetch_lines,
        .candidate_spans = total_stats.candidate_spans,
        .candidate_lines = total_stats.candidate_lines,
        .candidate_buckets = total_stats.candidate_buckets,
        .candidate_bucket_span = total_stats.candidate_bucket_span,
        .selected_spans = total_stats.selected_spans,
        .selected_lines = total_stats.selected_lines,
        .selected_buckets = total_stats.selected_buckets,
        .total_budget_lines = total_stats.prefetch_budget_lines,
        .plan_policy = total_stats.plan_policy,
        .checksum = checksum,
    };
}

static void print_result(const bench_result_t *result, const bench_opts_t *opts) {
    const double ops = (double)opts->iters * (double)opts->batch;
    const double bytes = ops * (double)opts->dim * sizeof(float);
    const double seconds = (double)result->ns / 1000000000.0;
    const double avg_runs = opts->iters ? (double)result->total_runs / (double)opts->iters : 0.0;
    const double avg_prefetch_lines =
        opts->iters ? (double)result->total_prefetch_lines / (double)opts->iters : 0.0;
    const double avg_candidate_lines =
        opts->iters ? (double)result->candidate_lines / (double)opts->iters : 0.0;
    const double avg_selected_lines =
        opts->iters ? (double)result->selected_lines / (double)opts->iters : 0.0;
    const double avg_candidate_buckets =
        opts->iters ? (double)result->candidate_buckets / (double)opts->iters : 0.0;
    const double avg_candidate_bucket_span =
        opts->iters ? (double)result->candidate_bucket_span / (double)opts->iters : 0.0;
    const double avg_selected_buckets =
        opts->iters ? (double)result->selected_buckets / (double)opts->iters : 0.0;
    const double avg_budget_lines =
        opts->iters ? (double)result->total_budget_lines / (double)opts->iters : 0.0;

    printf("%-15s %10.2f ns/vector %8.2f GiB/s",
           result->name,
           (double)result->ns / ops,
           bytes / seconds / (1024.0 * 1024.0 * 1024.0));
    if (result->plan_ns)
        printf(" plan=%6.2f ns/batch", (double)result->plan_ns / (double)opts->iters);
    if (result->prefetch_ns)
        printf(" prefetch=%6.2f ns/batch", (double)result->prefetch_ns / (double)opts->iters);
    if (result->total_runs)
        printf(" runs=%5.1f", avg_runs);
    if (result->total_prefetch_lines)
        printf(" lines=%6.1f", avg_prefetch_lines);
    if (result->plan_policy)
        printf(" policy=%s", result->plan_policy);
    if (result->candidate_lines)
        printf(" cand_lines=%6.1f sel_lines=%6.1f budget=%5.1f",
               avg_candidate_lines,
               avg_selected_lines,
               avg_budget_lines);
    if (result->candidate_buckets)
        printf(" cand_buckets=%4.1f", avg_candidate_buckets);
    if (result->candidate_bucket_span)
        printf(" bucket_span=%5.1f", avg_candidate_bucket_span);
    if (result->selected_buckets)
        printf(" sel_buckets=%4.1f", avg_selected_buckets);
    printf(" checksum=%0.3f\n", result->checksum);
}

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s [options]\n"
            "\n"
            "Source:\n"
            "  --ub-path <path>       mmap this UB/SHM device or file directly\n"
            "  --map-size <bytes>     mapping size, e.g. 512M\n"
            "  --table-offset <bytes> table offset inside mapping\n"
            "\n"
            "Benchmark:\n"
            "  --rows <n>             vector rows (default: %u)\n"
            "  --dim <n>              required float dimensions per vector\n"
            "  --stride <bytes>       row stride (default: dim * sizeof(float))\n"
            "  --batch <n>            vectors per batch (default: %u)\n"
            "  --iters <n>            measurement iterations (default: %u)\n"
            "  --window-lines <n>     cap prefetched lines per batch; must be > 0 (default: %u)\n"
            "  prefetch budget is compile-time selected with UB_BATCH_PREFETCH_BUDGET_BYTES (default: %u)\n"
            "  cacheline size is compile-time selected with CACHELINE=<bytes> (default: %u)\n"
            "  --pattern seq|random|hot|cluster\n"
            "  --copy memcpy|sve      copy vector with memcpy or sve_streaming_load_f32\n"
            "  prefetch mode is compile-time selected: make ... PREFETCH=builtin|pld\n"
            "  --cacheable yes|no     yes: O_RDWR/CC, no: O_RDWR|O_SYNC/NC (default: yes)\n"
            "  --fill                 write fixture vectors before reading (mock always fills)\n",
            prog,
            DEFAULT_ROWS,
            DEFAULT_BATCH,
            DEFAULT_ITERS,
            DEFAULT_WINDOW_LINES,
            UB_BATCH_PREFETCH_BUDGET_BYTES,
            UB_BATCH_CACHELINE);
}

static int parse_args(int argc, char **argv, bench_opts_t *opts) {
    *opts = (bench_opts_t){
        .rows = DEFAULT_ROWS,
        .dim = 0,
        .batch = DEFAULT_BATCH,
        .iters = DEFAULT_ITERS,
        .window_lines = DEFAULT_WINDOW_LINES,
        .pattern = ACCESS_RANDOM,
#if defined(USE_ARM_SVE) || defined(USE_SVE)
        .copy_mode = COPY_SVE_STREAM,
#else
        .copy_mode = COPY_MEMCPY,
#endif
        .cacheable = 1,
    };

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--help") == 0) {
            usage(argv[0]);
            exit(0);
        } else if (strcmp(argv[i], "--ub-path") == 0 && i + 1 < argc) {
            opts->ub_path = argv[++i];
        } else if (strcmp(argv[i], "--map-size") == 0 && i + 1 < argc) {
            opts->map_size = parse_size(argv[++i]);
        } else if (strcmp(argv[i], "--table-offset") == 0 && i + 1 < argc) {
            opts->table_offset = parse_size(argv[++i]);
        } else if (strcmp(argv[i], "--rows") == 0 && i + 1 < argc) {
            opts->rows = parse_size(argv[++i]);
        } else if (strcmp(argv[i], "--dim") == 0 && i + 1 < argc) {
            opts->dim = parse_size(argv[++i]);
        } else if (strcmp(argv[i], "--stride") == 0 && i + 1 < argc) {
            opts->stride = parse_size(argv[++i]);
        } else if (strcmp(argv[i], "--batch") == 0 && i + 1 < argc) {
            opts->batch = parse_size(argv[++i]);
        } else if (strcmp(argv[i], "--iters") == 0 && i + 1 < argc) {
            opts->iters = parse_size(argv[++i]);
        } else if (strcmp(argv[i], "--window-lines") == 0 && i + 1 < argc) {
            opts->window_lines = parse_size(argv[++i]);
        } else if (strcmp(argv[i], "--pattern") == 0 && i + 1 < argc) {
            const char *pattern = argv[++i];
            if (strcmp(pattern, "seq") == 0)
                opts->pattern = ACCESS_SEQ;
            else if (strcmp(pattern, "random") == 0)
                opts->pattern = ACCESS_RANDOM;
            else if (strcmp(pattern, "hot") == 0)
                opts->pattern = ACCESS_HOT;
            else if (strcmp(pattern, "cluster") == 0)
                opts->pattern = ACCESS_CLUSTER;
            else
                return -1;
        } else if (strcmp(argv[i], "--copy") == 0 && i + 1 < argc) {
            const char *copy = argv[++i];
            if (strcmp(copy, "memcpy") == 0)
                opts->copy_mode = COPY_MEMCPY;
            else if (strcmp(copy, "sve") == 0)
                opts->copy_mode = COPY_SVE_STREAM;
            else
                return -1;
        } else if (strcmp(argv[i], "--cacheable") == 0 && i + 1 < argc) {
            const char *cacheable = argv[++i];
            if (strcmp(cacheable, "yes") == 0)
                opts->cacheable = 1;
            else if (strcmp(cacheable, "no") == 0)
                opts->cacheable = 0;
            else
                return -1;
        } else if (strcmp(argv[i], "--fill") == 0) {
            opts->fill = 1;
        } else {
            return -1;
        }
    }

    if (opts->dim == 0 || opts->dim > VEMB_V16_MAX_DIM ||
        opts->rows == 0 || opts->batch == 0 || opts->iters == 0 ||
        opts->window_lines == 0)
        return -1;
    if (opts->batch > opts->rows)
        return -1;
    if (opts->stride == 0)
        opts->stride = opts->dim * sizeof(float);
    if (opts->stride < opts->dim * sizeof(float))
        return -1;
    if (opts->rows > UINT32_MAX)
        return -1;
    if (opts->iters > SIZE_MAX / opts->batch)
        return -1;
    return 0;
}

int main(int argc, char **argv) {
    bench_opts_t opts;
    ub_region_t region;
    ub_handle_t *handles = NULL;
    uint32_t *requests = NULL;
    uint8_t *out = NULL;
    cacheline_run_t *runs = NULL;
    prefetch_span_t *spans = NULL;
    prefetch_bucket_t *buckets = NULL;

    monotonicInit();

    if (parse_args(argc, argv, &opts) != 0) {
        usage(argv[0]);
        return 1;
    }

    printf("UB batch load standalone demo: source=%s mode=%s copy=%s prefetch=%s rows=%zu dim=%zu stride=%zu batch=%zu iters=%zu pattern=%s budget_lines=%zu window_lines=%zu\n",
           opts.ub_path ? opts.ub_path : "aligned-memory",
           opts.ub_path ? (opts.cacheable ? "CC/O_RDWR" : "NC/O_RDWR|O_SYNC") : "mock",
           opts.copy_mode == COPY_SVE_STREAM ? "sve_streaming_load_f32" : "memcpy",
           prefetch_mode_name(),
           opts.rows,
           opts.dim,
           opts.stride,
           opts.batch,
           opts.iters,
           access_pattern_name(opts.pattern),
           effective_prefetch_budget_lines(&opts),
           opts.window_lines);

    if (setup_region(&region, &opts) != 0)
        return 1;
    if (!opts.ub_path || opts.fill)
        fill_region(&region, &opts);

    handles = calloc(opts.rows, sizeof(*handles));
    requests = calloc(opts.iters * opts.batch, sizeof(*requests));
    runs = calloc(opts.batch, sizeof(*runs));
    spans = calloc(opts.batch, sizeof(*spans));
    buckets = calloc(opts.batch, sizeof(*buckets));
    if (!handles || !requests || !runs || !spans || !buckets ||
        posix_memalign((void **)&out, UB_BATCH_CACHELINE, opts.batch * opts.dim * sizeof(float)) != 0) {
        fprintf(stderr, "allocation failed\n");
        free(handles);
        free(requests);
        free(runs);
        free(spans);
        free(buckets);
        teardown_region(&region);
        return 1;
    }
    build_handles(handles, &opts);
    build_batch_request(requests, &opts);

    bench_result_t scalar = run_scalar(&region, handles, requests, out, &opts);
    bench_result_t plan = run_planned(&region, handles, requests, out, runs, spans, buckets, &opts, 0);
    bench_result_t prefetch = run_planned(&region, handles, requests, out, runs, spans, buckets, &opts, 1);

    print_result(&scalar, &opts);
    print_result(&plan, &opts);
    print_result(&prefetch, &opts);
    printf("speedup batch-prefetch/scalar = %.3fx\n",
           prefetch.ns ? (double)scalar.ns / (double)prefetch.ns : 0.0);

    free(out);
    free(handles);
    free(requests);
    free(runs);
    free(spans);
    free(buckets);
    teardown_region(&region);
    return 0;
}
