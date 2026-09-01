#include "tlc_cold.h"
#include "macro.h"
#include "vemb_v16_hash.h"
#include "vemb_v16_util.h"
#include "vemb_v16_log.h"
#include "zmalloc.h"
#include "monotonic.h"

#include <errno.h>
#include <fcntl.h>
#include <dirent.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define TLC_COLD_MAGIC UINT32_C(0x544C4345) /* TLCE */
#define TLC_COLD_FORMAT_VERSION UINT16_C(1)
#define TLC_COLD_CHECKPOINT_MAGIC UINT32_C(0x544C434B) /* TLCK */
#define TLC_COLD_CHECKPOINT_VERSION UINT16_C(1)

typedef struct tlc_cold_frame_header {
    uint32_t magic;
    uint16_t format_version;
    uint16_t header_bytes;
    uint64_t term;
    uint64_t seq;
    uint32_t op;
    uint32_t meta_shard_id;
    uint32_t key_len;
    uint32_t value_len;
    uint64_t version;
    uint64_t checksum;
} tlc_cold_frame_header_t;

typedef struct tlc_cold_checkpoint_header {
    uint32_t magic;
    uint16_t format_version;
    uint16_t header_bytes;
    uint64_t generation;
    uint64_t term;
    uint64_t checkpoint_seq;
    uint32_t meta_shard_count;
    uint32_t record_count;
    uint64_t generation_checksum;
} tlc_cold_checkpoint_header_t;

typedef struct tlc_cold_checkpoint_record_header {
    uint32_t meta_shard_id;
    uint32_t state_len;
    uint64_t captured_seq;
    uint64_t state_checksum;
} tlc_cold_checkpoint_record_header_t;

typedef struct tlc_cold_checkpoint_manifest {
    uint32_t magic;
    uint16_t format_version;
    uint16_t header_bytes;
    uint64_t generation;
    uint64_t checkpoint_seq;
    uint32_t meta_shard_count;
    uint32_t record_count;
    uint64_t generation_checksum;
} tlc_cold_checkpoint_manifest_t;

typedef struct tlc_cold_request {
    tlc_cold_event_input_t input;
    uint8_t *key;
    uint8_t *value;
    uint64_t seq;
    int append_rc;
    int durable_rc;
    bool appended;
    bool durable;
    atomic_uint refs;
    pthread_mutex_t mu;
    pthread_cond_t cv;
    struct tlc_cold_request *next;
} tlc_cold_request_t;

struct tlc_cold {
    char *directory;
    uint64_t segment_bytes;
    uint32_t queue_capacity;
    uint32_t group_max_entries;
    uint64_t group_max_delay_us;

    pthread_mutex_t queue_mu;
    pthread_cond_t queue_not_empty;
    pthread_cond_t queue_not_full;
    tlc_cold_request_t **queue;
    uint32_t queue_head;
    uint32_t queue_tail;
    uint32_t queue_count;

    pthread_mutex_t pending_mu;
    pthread_cond_t pending_cv;
    tlc_cold_request_t *pending_head;
    tlc_cold_request_t *pending_tail;
    uint32_t pending_count;
    uint64_t pending_since_ns;

    pthread_t writer_thread;
    pthread_t flush_thread;
    atomic_bool stopping;
    atomic_bool writer_done;
    atomic_int io_error;

    int fd;
    atomic_uint_fast64_t segment_id;
    atomic_uint_fast64_t segment_offset;
    uint64_t next_seq;
    atomic_uint_fast64_t appended_seq;
    atomic_uint_fast64_t durable_seq;
    pthread_mutex_t io_mu;
    pthread_mutex_t checkpoint_mu;
#ifdef TLC_COLD_ENABLE_FAILPOINT
    tlc_cold_checkpoint_failpoint_t checkpoint_failpoint;
    tlc_cold_compact_failpoint_t compact_failpoint;
    tlc_cold_io_failpoint_t io_failpoint;
#endif
    XXH3_state_t *checksum_state;
};

static uint64_t cold_now_ns(void) {
    return vemb_v16_monotonic_ns();
}

static int cold_read_full(int fd, void *buffer, size_t length);

#ifdef TLC_COLD_ENABLE_FAILPOINT
static int cold_checkpoint_failpoint(tlc_cold_t *cold,
                                     tlc_cold_checkpoint_failpoint_t failpoint) {
    if (cold->checkpoint_failpoint != failpoint)
        return 0;
    cold->checkpoint_failpoint = TLC_COLD_CHECKPOINT_FAIL_NONE;
    errno = EIO;
    return -1;
}
#define COLD_CHECKPOINT_FAIL(cold, point) cold_checkpoint_failpoint((cold), (point))
static int cold_compact_failpoint(tlc_cold_t *cold,
                                  tlc_cold_compact_failpoint_t failpoint) {
    if (cold->compact_failpoint != failpoint)
        return 0;
    cold->compact_failpoint = TLC_COLD_COMPACT_FAIL_NONE;
    errno = EIO;
    return -1;
}
#define COLD_COMPACT_FAIL(cold, point) cold_compact_failpoint((cold), (point))
static int cold_io_failpoint(tlc_cold_t *cold,
                             tlc_cold_io_failpoint_t failpoint) {
    if (cold->io_failpoint != failpoint)
        return 0;
    cold->io_failpoint = TLC_COLD_IO_FAIL_NONE;
    errno = EIO;
    return -1;
}
#define COLD_IO_FAIL(cold, point) cold_io_failpoint((cold), (point))
#else
/* Test instrumentation is compiled out of production COLD binaries. */
#define COLD_CHECKPOINT_FAIL(cold, point) 0
#define COLD_COMPACT_FAIL(cold, point) 0
#define COLD_IO_FAIL(cold, point) 0
#endif

static uint64_t cold_event_checksum(tlc_cold_t *cold,
                                    const tlc_cold_frame_header_t *header,
                                    const void *key,
                                    uint32_t key_len,
                                    const void *value,
                                    uint32_t value_len) {
    /* Stream the three existing buffers; concatenating them would add a copy. */
    XXH3_64bits_reset(cold->checksum_state);
    XXH3_64bits_update(cold->checksum_state, header,
                       offsetof(tlc_cold_frame_header_t, checksum));
    XXH3_64bits_update(cold->checksum_state, key, key_len);
    XXH3_64bits_update(cold->checksum_state, value, value_len);
    return (uint64_t)XXH3_64bits_digest(cold->checksum_state);
}

static int cold_write_full(int fd, const void *buffer, size_t length) {
    const uint8_t *cursor = (const uint8_t *)buffer;
    while (length != 0) {
        ssize_t written = write(fd, cursor, length);
        if (written < 0 && errno == EINTR)
            continue;
        if (written <= 0)
            return -1;
        cursor += (size_t)written;
        length -= (size_t)written;
    }
    return 0;
}

/* Returns 0 for a full read, 1 for clean EOF, 2 for a partial EOF, -1 on I/O. */
static int cold_read_full(int fd, void *buffer, size_t length) {
    uint8_t *cursor = buffer;
    size_t total = 0;
    while (total < length) {
        ssize_t nread = read(fd, cursor + total, length - total);
        if (nread < 0 && errno == EINTR)
            continue;
        if (nread < 0)
            return -1;
        if (nread == 0)
            return total == 0 ? 1 : 2;
        total += (size_t)nread;
    }
    return 0;
}

static int cold_make_segment_path(const tlc_cold_t *cold,
                                  uint64_t segment_id,
                                  char *path,
                                  size_t path_size) {
    int written = snprintf(path, path_size, "%s/aof-%020llu.log",
                           cold->directory,
                           (unsigned long long)segment_id);
    return written < 0 || (size_t)written >= path_size ? -1 : 0;
}

static int cold_make_checkpoint_path(const tlc_cold_t *cold,
                                     uint64_t generation,
                                     const char *suffix,
                                     char *path,
                                     size_t path_size) {
    int written = snprintf(path, path_size, "%s/checkpoint-%020llu%s",
                           cold->directory,
                           (unsigned long long)generation,
                           suffix);
    return written < 0 || (size_t)written >= path_size ? -1 : 0;
}

static int cold_fsync_directory(const tlc_cold_t *cold) {
    int fd = open(cold->directory, O_RDONLY | O_DIRECTORY);
    if (fd < 0)
        return -1;
    int rc = fsync(fd);
    int saved_errno = errno;
    close(fd);
    errno = saved_errno;
    return rc;
}

static int cold_read_checkpoint_manifest(const tlc_cold_t *cold,
                                         tlc_cold_checkpoint_manifest_t *manifest) {
    char path[4096];
    int written = snprintf(path, sizeof(path), "%s/checkpoint.manifest",
                           cold->directory);
    if (written < 0 || (size_t)written >= sizeof(path)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return errno == ENOENT ? 1 : -1;
    int rc = cold_read_full(fd, manifest, sizeof(*manifest));
    int saved_errno = errno;
    if (rc == 0) {
        uint8_t extra;
        int extra_rc = cold_read_full(fd, &extra, sizeof(extra));
        if (extra_rc != 1)
            rc = -1;
    } else {
        rc = -1;
    }
    if (close(fd) != 0 && rc == 0)
        rc = -1;
    errno = saved_errno;
    if (rc != 0 || manifest->magic != TLC_COLD_CHECKPOINT_MAGIC ||
        manifest->format_version != TLC_COLD_CHECKPOINT_VERSION ||
        manifest->header_bytes != sizeof(*manifest) ||
        manifest->generation == 0 || manifest->meta_shard_count == 0 ||
        manifest->record_count != manifest->meta_shard_count) {
        errno = EINVAL;
        return -1;
    }
    return 0;
}

static int cold_validate_checkpoint_locked(
        tlc_cold_t *cold,
        uint64_t generation,
        uint32_t expected_meta_shard_count,
        tlc_cold_checkpoint_result_t *result) {
    tlc_cold_checkpoint_manifest_t manifest = {0};
    int manifest_rc = cold_read_checkpoint_manifest(cold, &manifest);
    if (manifest_rc != 0 ||
        (expected_meta_shard_count &&
         manifest.meta_shard_count != expected_meta_shard_count)) {
        errno = EINVAL;
        return -1;
    }
    bool validate_active = generation == 0 || generation == manifest.generation;
    char path[4096];
    if (generation == 0)
        generation = manifest.generation;
    if (cold_make_checkpoint_path(cold, generation, "",
                                  path, sizeof(path)) != 0) {
        errno = ENAMETOOLONG;
        return -1;
    }
    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return -1;
    tlc_cold_checkpoint_header_t header;
    if (cold_read_full(fd, &header, sizeof(header)) != 0 ||
        header.magic != TLC_COLD_CHECKPOINT_MAGIC ||
        header.format_version != TLC_COLD_CHECKPOINT_VERSION ||
        header.header_bytes != sizeof(header) ||
        header.generation != generation ||
        header.meta_shard_count != manifest.meta_shard_count ||
        header.record_count != manifest.record_count) {
        close(fd);
        errno = EINVAL;
        return -1;
    }
    XXH3_state_t *checksum = XXH3_createState();
    XXH3_state_t *state_checksum = XXH3_createState();
    if (!checksum || !state_checksum) {
        XXH3_freeState(checksum);
        XXH3_freeState(state_checksum);
        close(fd);
        errno = ENOMEM;
        return -1;
    }
    XXH3_64bits_reset(checksum);
    XXH3_64bits_update(checksum, &header,
                       offsetof(tlc_cold_checkpoint_header_t,
                                generation_checksum));
    uint8_t buffer[65536];
    uint64_t checkpoint_seq = UINT64_MAX;
    int rc = 0;
    for (uint32_t i = 0; i < header.record_count; i++) {
        tlc_cold_checkpoint_record_header_t record_header;
        if (cold_read_full(fd, &record_header, sizeof(record_header)) != 0 ||
            record_header.meta_shard_id != i ||
            record_header.captured_seq > atomic_load_explicit(
                &cold->durable_seq, memory_order_acquire)) {
            rc = -1;
            break;
        }
        if (record_header.captured_seq < checkpoint_seq)
            checkpoint_seq = record_header.captured_seq;
        XXH3_64bits_update(checksum, &record_header,
                           sizeof(record_header));
        XXH3_64bits_reset(state_checksum);
        uint32_t remaining = record_header.state_len;
        while (remaining != 0) {
            size_t chunk = remaining < sizeof(buffer) ? remaining : sizeof(buffer);
            if (cold_read_full(fd, buffer, chunk) != 0) {
                rc = -1;
                break;
            }
            XXH3_64bits_update(checksum, buffer, chunk);
            XXH3_64bits_update(state_checksum, buffer, chunk);
            remaining -= (uint32_t)chunk;
        }
        if (rc != 0 ||
            (uint64_t)XXH3_64bits_digest(state_checksum) !=
                record_header.state_checksum) {
            rc = -1;
            break;
        }
    }
    if (rc == 0) {
        uint8_t extra;
        if (cold_read_full(fd, &extra, sizeof(extra)) != 1 ||
            checkpoint_seq != header.checkpoint_seq ||
            (uint64_t)XXH3_64bits_digest(checksum) !=
                header.generation_checksum ||
            (validate_active &&
             header.generation_checksum != manifest.generation_checksum))
            rc = -1;
    }
    if (close(fd) != 0)
        rc = -1;
    XXH3_freeState(checksum);
    XXH3_freeState(state_checksum);
    if (rc != 0) {
        errno = EINVAL;
        return -1;
    }
    if (result) {
        result->generation = header.generation;
        result->checkpoint_seq = header.checkpoint_seq;
        result->generation_checksum = header.generation_checksum;
    }
    return 0;
}

static int cold_open_segment(tlc_cold_t *cold, uint64_t segment_id) {
    char path[4096];
    if (cold_make_segment_path(cold, segment_id, path, sizeof(path)) != 0)
        return -1;
    int fd = open(path, O_CREAT | O_APPEND | O_WRONLY, 0644);
    if (fd < 0) {
        serverLog(LL_WARNING,
                  "COLD segment open failed: path=%s errno=%d (%s)",
                  path, errno, strerror(errno));
        return -1;
    }
    struct stat st;
    if (fstat(fd, &st) != 0) {
        serverLog(LL_WARNING,
                  "COLD segment stat failed: path=%s errno=%d (%s)",
                  path, errno, strerror(errno));
        close(fd);
        return -1;
    }
    cold->fd = fd;
    atomic_store_explicit(&cold->segment_id, segment_id, memory_order_release);
    atomic_store_explicit(&cold->segment_offset, (uint64_t)st.st_size,
                          memory_order_release);
    return 0;
}

typedef struct tlc_cold_segment_id {
    uint64_t id;
} tlc_cold_segment_id_t;

typedef struct tlc_cold_generation_id {
    uint64_t id;
} tlc_cold_generation_id_t;

static int cold_segment_id_compare(const void *left, const void *right) {
    const tlc_cold_segment_id_t *a = left;
    const tlc_cold_segment_id_t *b = right;
    return a->id < b->id ? -1 : a->id > b->id;
}

static int cold_generation_id_compare(const void *left, const void *right) {
    const tlc_cold_generation_id_t *a = left;
    const tlc_cold_generation_id_t *b = right;
    return a->id < b->id ? -1 : a->id > b->id;
}

static int cold_parse_generation_id(const char *name, uint64_t *generation_id) {
    const char prefix[] = "checkpoint-";
    size_t prefix_len = sizeof(prefix) - 1;
    size_t name_len = strlen(name);
    if (name_len <= prefix_len || strncmp(name, prefix, prefix_len) != 0)
        return 0;
    const char *suffix = name + name_len - 5;
    if (strcmp(suffix, ".tmp") == 0)
        return 0;
    char number[64];
    size_t number_len = name_len - prefix_len;
    if (number_len >= sizeof(number))
        return -1;
    memcpy(number, name + prefix_len, number_len);
    number[number_len] = '\0';
    errno = 0;
    char *end = NULL;
    unsigned long long parsed = strtoull(number, &end, 10);
    if (errno == ERANGE || end == number || *end != '\0')
        return -1;
    *generation_id = (uint64_t)parsed;
    return 1;
}

static int cold_collect_generations(const tlc_cold_t *cold,
                                    tlc_cold_generation_id_t **ids_out,
                                    size_t *count_out) {
    DIR *directory = opendir(cold->directory);
    if (!directory)
        return -1;
    tlc_cold_generation_id_t *ids = NULL;
    size_t count = 0;
    size_t capacity = 0;
    struct dirent *entry;
    while ((entry = readdir(directory)) != NULL) {
        uint64_t generation_id = 0;
        int parsed = cold_parse_generation_id(entry->d_name, &generation_id);
        if (parsed < 0) {
            closedir(directory);
            zfree(ids);
            return -1;
        }
        if (parsed == 0)
            continue;
        if (count == capacity) {
            size_t next_capacity = capacity ? capacity * 2 : 8;
            tlc_cold_generation_id_t *next = zmalloc(
                next_capacity * sizeof(*next));
            if (!next) {
                closedir(directory);
                zfree(ids);
                return -1;
            }
            if (count)
                memcpy(next, ids, count * sizeof(*next));
            zfree(ids);
            ids = next;
            capacity = next_capacity;
        }
        ids[count++].id = generation_id;
    }
    closedir(directory);
    qsort(ids, count, sizeof(*ids), cold_generation_id_compare);
    *ids_out = ids;
    *count_out = count;
    return 0;
}

static int cold_parse_segment_id(const char *name, uint64_t *segment_id) {
    const char prefix[] = "aof-";
    const char suffix[] = ".log";
    size_t name_len = strlen(name);
    size_t prefix_len = sizeof(prefix) - 1;
    size_t suffix_len = sizeof(suffix) - 1;
    if (name_len <= prefix_len + suffix_len ||
        strncmp(name, prefix, prefix_len) != 0 ||
        strcmp(name + name_len - suffix_len, suffix) != 0)
        return 0;
    char number[32];
    size_t number_len = name_len - prefix_len - suffix_len;
    if (number_len >= sizeof(number))
        return -1;
    memcpy(number, name + prefix_len, number_len);
    number[number_len] = '\0';
    char *end = NULL;
    errno = 0;
    unsigned long long parsed = strtoull(number, &end, 10);
    if (errno == ERANGE || end == number || *end != '\0')
        return -1;
    *segment_id = (uint64_t)parsed;
    return 1;
}

static int cold_collect_segments(const tlc_cold_t *cold,
                                 tlc_cold_segment_id_t **segments_out,
                                 size_t *count_out) {
    DIR *directory = opendir(cold->directory);
    if (!directory)
        return -1;
    tlc_cold_segment_id_t *segments = NULL;
    size_t count = 0;
    size_t capacity = 0;
    struct dirent *entry;
    while ((entry = readdir(directory)) != NULL) {
        uint64_t segment_id = 0;
        int parsed = cold_parse_segment_id(entry->d_name, &segment_id);
        if (parsed < 0) {
            closedir(directory);
            zfree(segments);
            return -1;
        }
        if (parsed == 0)
            continue;
        if (count == capacity) {
            size_t next_capacity = capacity ? capacity * 2 : 8;
            tlc_cold_segment_id_t *next = zmalloc(
                next_capacity * sizeof(*next));
            if (!next) {
                closedir(directory);
                zfree(segments);
                return -1;
            }
            if (count)
                memcpy(next, segments, count * sizeof(*next));
            zfree(segments);
            segments = next;
            capacity = next_capacity;
        }
        segments[count++].id = segment_id;
    }
    closedir(directory);
    qsort(segments, count, sizeof(*segments), cold_segment_id_compare);
    *segments_out = segments;
    *count_out = count;
    return 0;
}

static int cold_recover_segment(tlc_cold_t *cold,
                                uint64_t segment_id,
                                uint64_t *expected_seq,
                                uint64_t *segment_offset,
                                bool allow_tail_truncate,
                                bool *truncated_tail) {
    char path[4096];
    if (cold_make_segment_path(cold, segment_id, path, sizeof(path)) != 0)
        return -1;
    int fd = open(path, O_RDWR);
    if (fd < 0)
        return -1;
    *truncated_tail = false;
    uint64_t offset = 0;
    for (;;) {
        tlc_cold_frame_header_t header;
        int header_rc = cold_read_full(fd, &header, sizeof(header));
        if (header_rc == 1)
            break;
        if (header_rc == 2) {
            if (!allow_tail_truncate)
                goto failed;
            if (ftruncate(fd, (off_t)offset) != 0 || fsync(fd) != 0)
                goto failed;
            *truncated_tail = true;
            break;
        }
        if (header_rc != 0)
            goto failed;
        if (header.magic != TLC_COLD_MAGIC ||
            header.format_version != TLC_COLD_FORMAT_VERSION ||
            header.header_bytes != sizeof(header) ||
            header.key_len == 0 ||
            (header.op != TLC_COLD_OP_PUT && header.op != TLC_COLD_OP_DEL) ||
            (header.op == TLC_COLD_OP_DEL && header.value_len != 0) ||
            header.key_len > SIZE_MAX - header.value_len - sizeof(header))
            goto failed;
        if (*expected_seq == 0)
            *expected_seq = header.seq;
        if (header.seq != *expected_seq)
            goto failed;
        uint8_t *key = zmalloc(header.key_len);
        uint8_t *value = header.value_len ? zmalloc(header.value_len) : NULL;
        if (!key || (header.value_len && !value)) {
            zfree(key);
            zfree(value);
            goto failed;
        }
        int key_rc = cold_read_full(fd, key, header.key_len);
        int value_rc = key_rc == 0 ? cold_read_full(fd, value, header.value_len) : -1;
        if (key_rc == 1 || key_rc == 2 || value_rc == 1 || value_rc == 2) {
            zfree(key);
            zfree(value);
            if (!allow_tail_truncate)
                goto failed;
            if (ftruncate(fd, (off_t)offset) != 0 || fsync(fd) != 0)
                goto failed;
            *truncated_tail = true;
            break;
        }
        if (key_rc != 0 || value_rc != 0 ||
            cold_event_checksum(cold, &header, key, header.key_len,
                                value, header.value_len) != header.checksum) {
            zfree(key);
            zfree(value);
            goto failed;
        }
        zfree(key);
        zfree(value);
        offset += sizeof(header) + header.key_len + header.value_len;
        *expected_seq = header.seq == UINT64_MAX ? 0 : header.seq + 1;
        if (*expected_seq == 0)
            goto failed;
    }
    if (fsync(fd) != 0 || close(fd) != 0)
        return -1;
    *segment_offset = offset;
    return 0;

failed:
    close(fd);
    return -1;
}

static int cold_recover_segments(tlc_cold_t *cold) {
    tlc_cold_segment_id_t *segments = NULL;
    size_t count = 0;
    if (cold_collect_segments(cold, &segments, &count) != 0) {
        serverLog(LL_WARNING,
                  "COLD AOF segment enumeration failed: directory=%s errno=%d (%s)",
                  cold->directory, errno, strerror(errno));
        return -1;
    }
    uint64_t expected_seq = 0;
    uint64_t latest_id = 0;
    uint64_t latest_offset = 0;
    for (size_t i = 0; i < count; i++) {
        latest_id = segments[i].id;
        bool truncated_tail = false;
        if (cold_recover_segment(cold, segments[i].id, &expected_seq,
                                 &latest_offset, i + 1 == count,
                                 &truncated_tail) != 0)
            goto failed;
    }
    zfree(segments);
    if (cold_open_segment(cold, latest_id) != 0)
        return -1;
    atomic_store_explicit(&cold->segment_offset, latest_offset,
                          memory_order_release);
    cold->next_seq = expected_seq ? expected_seq : 1;
    uint64_t last_seq = expected_seq ? expected_seq - 1 : 0;
    atomic_store_explicit(&cold->appended_seq, last_seq, memory_order_release);
    atomic_store_explicit(&cold->durable_seq, last_seq, memory_order_release);
    return 0;

failed:
    serverLog(LL_WARNING,
              "COLD AOF recovery scan failed: directory=%s", cold->directory);
    zfree(segments);
    return -1;
}

static int cold_replay_segment(tlc_cold_t *cold,
                               uint64_t segment_id,
                               uint64_t *expected_seq,
                               const uint64_t *captured_seq,
                               uint32_t meta_shard_count,
                               tlc_cold_replay_fn callback,
                               void *arg) {
    char path[4096];
    if (cold_make_segment_path(cold, segment_id, path, sizeof(path)) != 0)
        return -1;
    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return -1;
    for (;;) {
        tlc_cold_frame_header_t header;
        int header_rc = cold_read_full(fd, &header, sizeof(header));
        if (header_rc == 1)
            break;
        if (header_rc != 0 ||
            header.magic != TLC_COLD_MAGIC ||
            header.format_version != TLC_COLD_FORMAT_VERSION ||
            header.header_bytes != sizeof(header) ||
            header.key_len == 0 ||
            (header.op != TLC_COLD_OP_PUT && header.op != TLC_COLD_OP_DEL) ||
            (header.op == TLC_COLD_OP_DEL && header.value_len != 0) ||
            header.key_len > SIZE_MAX - header.value_len - sizeof(header)) {
            close(fd);
            return -1;
        }
        if (*expected_seq == 0)
            *expected_seq = header.seq;
        if (header.seq != *expected_seq) {
            close(fd);
            return -1;
        }
        uint8_t *key = zmalloc(header.key_len);
        uint8_t *value = header.value_len ? zmalloc(header.value_len) : NULL;
        if (!key || (header.value_len && !value)) {
            zfree(key);
            zfree(value);
            close(fd);
            return -1;
        }
        int key_rc = cold_read_full(fd, key, header.key_len);
        int value_rc = key_rc == 0 ? cold_read_full(fd, value, header.value_len) : -1;
        int valid = key_rc == 0 && value_rc == 0 &&
            cold_event_checksum(cold, &header, key, header.key_len,
                                value, header.value_len) == header.checksum;
        if (!valid) {
            zfree(key);
            zfree(value);
            close(fd);
            return -1;
        }
        tlc_cold_event_input_t input = {
            .term = header.term,
            .op = header.op,
            .meta_shard_id = header.meta_shard_id,
            .version = header.version,
            .key = key,
            .key_len = header.key_len,
            .value = value,
            .value_len = header.value_len,
        };
        if (meta_shard_count != 0 && header.meta_shard_id >= meta_shard_count) {
            zfree(key);
            zfree(value);
            close(fd);
            errno = EINVAL;
            return -1;
        }
        if (!captured_seq || header.seq > captured_seq[header.meta_shard_id]) {
            int callback_rc = callback(&input, header.seq, arg);
            if (callback_rc != 0) {
                zfree(key);
                zfree(value);
                close(fd);
                return callback_rc;
            }
        }
        zfree(key);
        zfree(value);
        *expected_seq = header.seq == UINT64_MAX ? 0 : header.seq + 1;
        if (*expected_seq == 0) {
            close(fd);
            return -1;
        }
    }
    return close(fd) == 0 ? 0 : -1;
}

static int cold_segment_last_seq(tlc_cold_t *cold,
                                 uint64_t segment_id,
                                 uint64_t *last_seq) {
    char path[4096];
    if (cold_make_segment_path(cold, segment_id, path, sizeof(path)) != 0)
        return -1;
    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return -1;
    uint64_t expected_seq = 0;
    uint64_t last = 0;
    bool has_entry = false;
    for (;;) {
        tlc_cold_frame_header_t header;
        int header_rc = cold_read_full(fd, &header, sizeof(header));
        if (header_rc == 1)
            break;
        if (header_rc != 0 || header.magic != TLC_COLD_MAGIC ||
            header.format_version != TLC_COLD_FORMAT_VERSION ||
            header.header_bytes != sizeof(header) || header.key_len == 0 ||
            (header.op != TLC_COLD_OP_PUT && header.op != TLC_COLD_OP_DEL) ||
            (header.op == TLC_COLD_OP_DEL && header.value_len != 0) ||
            header.key_len > SIZE_MAX - header.value_len - sizeof(header) ||
            (has_entry && header.seq != expected_seq)) {
            close(fd);
            errno = EINVAL;
            return -1;
        }
        uint8_t *key = zmalloc(header.key_len);
        uint8_t *value = header.value_len ? zmalloc(header.value_len) : NULL;
        if (!key || (header.value_len && !value)) {
            zfree(key);
            zfree(value);
            close(fd);
            return -1;
        }
        int valid = cold_read_full(fd, key, header.key_len) == 0 &&
            cold_read_full(fd, value, header.value_len) == 0 &&
            cold_event_checksum(cold, &header, key, header.key_len,
                                value, header.value_len) == header.checksum;
        zfree(key);
        zfree(value);
        if (!valid) {
            close(fd);
            errno = EINVAL;
            return -1;
        }
        has_entry = true;
        last = header.seq;
        expected_seq = header.seq == UINT64_MAX ? 0 : header.seq + 1;
        if (expected_seq == 0) {
            close(fd);
            errno = EOVERFLOW;
            return -1;
        }
    }
    if (close(fd) != 0)
        return -1;
    *last_seq = has_entry ? last : 0;
    return 0;
}

static int cold_rotate_if_needed(tlc_cold_t *cold, size_t frame_bytes) {
    uint64_t segment_offset = atomic_load_explicit(&cold->segment_offset,
                                                   memory_order_relaxed);
    if (segment_offset > cold->segment_bytes)
        return -1;
    if (segment_offset == 0 ||
        frame_bytes <= cold->segment_bytes - segment_offset)
        return 0;
    if (fsync(cold->fd) != 0 || close(cold->fd) != 0)
        return -1;
    cold->fd = -1;
    uint64_t segment_id = atomic_load_explicit(&cold->segment_id,
                                               memory_order_relaxed);
    return cold_open_segment(cold, segment_id + 1);
}

static tlc_cold_request_t *cold_request_create(
        const tlc_cold_event_input_t *input) {
    tlc_cold_request_t *request = zcalloc(sizeof(*request));
    if (!request) {
        serverLog(LL_WARNING,
                  "COLD request allocation failed: key_len=%u value_len=%u",
                  input->key_len, input->value_len);
        return NULL;
    }
    request->input = *input;
    request->key = zmalloc(input->key_len);
    request->value = input->value_len ? zmalloc(input->value_len) : NULL;
    if (!request->key || (input->value_len && !request->value)) {
        serverLog(LL_WARNING,
                  "COLD event payload allocation failed: key_len=%u value_len=%u",
                  input->key_len, input->value_len);
        zfree(request->key);
        zfree(request->value);
        zfree(request);
        return NULL;
    }
    memcpy(request->key, input->key, input->key_len);
    if (input->value_len)
        memcpy(request->value, input->value, input->value_len);
    request->input.key = request->key;
    request->input.value = request->value;
    atomic_init(&request->refs, 2);
    pthread_mutex_init(&request->mu, NULL);
    pthread_cond_init(&request->cv, NULL);
    return request;
}

static void cold_request_release(tlc_cold_request_t *request) {
    if (atomic_fetch_sub_explicit(&request->refs, 1, memory_order_acq_rel) != 1)
        return;
    pthread_cond_destroy(&request->cv);
    pthread_mutex_destroy(&request->mu);
    zfree(request->key);
    zfree(request->value);
    zfree(request);
}

static void cold_signal_append(tlc_cold_request_t *request, int rc, uint64_t seq) {
    pthread_mutex_lock(&request->mu);
    request->append_rc = rc;
    request->seq = seq;
    request->appended = true;
    pthread_cond_broadcast(&request->cv);
    pthread_mutex_unlock(&request->mu);
}

static void cold_pending_push(tlc_cold_t *cold, tlc_cold_request_t *request) {
    pthread_mutex_lock(&cold->pending_mu);
    /* Keep the post-append pending batch bounded while flush owns io_mu. */
    while (cold->pending_count == cold->queue_capacity)
        pthread_cond_wait(&cold->pending_cv, &cold->pending_mu);
    request->next = NULL;
    if (cold->pending_tail)
        cold->pending_tail->next = request;
    else
        cold->pending_head = request;
    cold->pending_tail = request;
    if (cold->pending_count++ == 0)
        cold->pending_since_ns = cold_now_ns();
    pthread_cond_signal(&cold->pending_cv);
    pthread_mutex_unlock(&cold->pending_mu);
}

static int cold_append_request(tlc_cold_t *cold, tlc_cold_request_t *request) {
    size_t frame_bytes = sizeof(tlc_cold_frame_header_t) +
                         request->input.key_len + request->input.value_len;
    if (frame_bytes < sizeof(tlc_cold_frame_header_t) ||
        cold->next_seq == 0 || cold->next_seq == UINT64_MAX) {
        errno = EOVERFLOW;
        return -1;
    }
    if (cold_rotate_if_needed(cold, frame_bytes) != 0)
        return -1;

    tlc_cold_frame_header_t header = {
        .magic = TLC_COLD_MAGIC,
        .format_version = TLC_COLD_FORMAT_VERSION,
        .header_bytes = sizeof(header),
        .term = request->input.term,
        .seq = cold->next_seq++,
        .op = request->input.op,
        .meta_shard_id = request->input.meta_shard_id,
        .key_len = request->input.key_len,
        .value_len = request->input.value_len,
        .version = request->input.version,
        .checksum = 0,
    };
    header.checksum = cold_event_checksum(cold, &header,
                                          request->key, request->input.key_len,
                                          request->value, request->input.value_len);
    if (COLD_IO_FAIL(cold, TLC_COLD_IO_FAIL_APPEND_WRITE) != 0)
        return -1;
    if (cold_write_full(cold->fd, &header, sizeof(header)) != 0 ||
        cold_write_full(cold->fd, request->key, request->input.key_len) != 0 ||
        cold_write_full(cold->fd, request->value, request->input.value_len) != 0)
        return -1;
    atomic_fetch_add_explicit(&cold->segment_offset, frame_bytes,
                              memory_order_release);
    atomic_store_explicit(&cold->appended_seq, header.seq, memory_order_release);
    cold_signal_append(request, 0, header.seq);
    return 0;
}

static void *cold_writer_main(void *arg) {
    tlc_cold_t *cold = arg;
    for (;;) {
        pthread_mutex_lock(&cold->queue_mu);
        while (cold->queue_count == 0 &&
               !atomic_load_explicit(&cold->stopping, memory_order_acquire))
            pthread_cond_wait(&cold->queue_not_empty, &cold->queue_mu);
        if (cold->queue_count == 0 &&
            atomic_load_explicit(&cold->stopping, memory_order_acquire)) {
            pthread_mutex_unlock(&cold->queue_mu);
            break;
        }
        tlc_cold_request_t *request = cold->queue[cold->queue_head];
        cold->queue[cold->queue_head] = NULL;
        cold->queue_head = (cold->queue_head + 1) % cold->queue_capacity;
        cold->queue_count--;
        pthread_cond_signal(&cold->queue_not_full);
        pthread_mutex_unlock(&cold->queue_mu);

        pthread_mutex_lock(&cold->io_mu);
        int io_error = atomic_load_explicit(&cold->io_error, memory_order_acquire);
        int rc = io_error == 0 ? cold_append_request(cold, request) : -1;
        if (rc != 0 && io_error == 0)
            atomic_store_explicit(&cold->io_error, errno ? errno : EIO,
                                  memory_order_release);
        pthread_mutex_unlock(&cold->io_mu);
        if (rc != 0) {
            if (io_error != 0)
                errno = io_error;
            else if (errno == 0)
                errno = EIO;
            serverLog(LL_WARNING,
                      "COLD AOF append failed: directory=%s errno=%d (%s)",
                      cold->directory, errno, strerror(errno));
            cold_signal_append(request, -1, 0);
            cold_request_release(request);
        } else {
            cold_pending_push(cold, request);
        }
    }
    pthread_mutex_lock(&cold->pending_mu);
    atomic_store_explicit(&cold->writer_done, true, memory_order_release);
    pthread_cond_broadcast(&cold->pending_cv);
    pthread_mutex_unlock(&cold->pending_mu);
    return NULL;
}

static void cold_add_us_to_realtime(struct timespec *ts, uint64_t usec) {
    ts->tv_sec += (time_t)(usec / UINT64_C(1000000));
    long nsec = ts->tv_nsec + (long)((usec % UINT64_C(1000000)) * 1000);
    ts->tv_sec += nsec / 1000000000L;
    ts->tv_nsec = nsec % 1000000000L;
}

static void *cold_flush_main(void *arg) {
    tlc_cold_t *cold = arg;
    for (;;) {
        pthread_mutex_lock(&cold->pending_mu);
        while (!cold->pending_head &&
               !(atomic_load_explicit(&cold->stopping, memory_order_acquire) &&
                 atomic_load_explicit(&cold->writer_done, memory_order_acquire)))
            pthread_cond_wait(&cold->pending_cv, &cold->pending_mu);
        if (!cold->pending_head &&
            atomic_load_explicit(&cold->stopping, memory_order_acquire) &&
            atomic_load_explicit(&cold->writer_done, memory_order_acquire)) {
            pthread_mutex_unlock(&cold->pending_mu);
            break;
        }
        bool flush_now =
            atomic_load_explicit(&cold->stopping, memory_order_acquire);
        if (!flush_now && cold->pending_count < cold->group_max_entries) {
            uint64_t elapsed = cold_now_ns() - cold->pending_since_ns;
            if (elapsed < cold->group_max_delay_us * UINT64_C(1000)) {
                uint64_t remaining_us =
                    (cold->group_max_delay_us * UINT64_C(1000) - elapsed + 999) / 1000;
                struct timespec deadline;
                clock_gettime(CLOCK_REALTIME, &deadline);
                cold_add_us_to_realtime(&deadline, remaining_us);
                (void)pthread_cond_timedwait(&cold->pending_cv,
                                             &cold->pending_mu, &deadline);
                pthread_mutex_unlock(&cold->pending_mu);
                continue;
            }
        }
        tlc_cold_request_t *list = cold->pending_head;
        cold->pending_head = NULL;
        cold->pending_tail = NULL;
        cold->pending_count = 0;
        cold->pending_since_ns = 0;
        pthread_cond_broadcast(&cold->pending_cv);
        pthread_mutex_unlock(&cold->pending_mu);

        pthread_mutex_lock(&cold->io_mu);
        int io_error = atomic_load_explicit(&cold->io_error, memory_order_acquire);
        int rc = io_error == 0 ? COLD_IO_FAIL(cold,
                                              TLC_COLD_IO_FAIL_GROUP_FSYNC) : -1;
        if (rc == 0)
            rc = fsync(cold->fd);
        if (rc != 0 && io_error == 0)
            atomic_store_explicit(&cold->io_error, errno ? errno : EIO,
                                  memory_order_release);
        if (rc != 0) {
            if (io_error != 0)
                errno = io_error;
            else if (errno == 0)
                errno = EIO;
            serverLog(LL_WARNING,
                      "COLD group commit fsync failed: directory=%s errno=%d (%s)",
                      cold->directory, errno, strerror(errno));
        }
        pthread_mutex_unlock(&cold->io_mu);
        for (tlc_cold_request_t *request = list; request;) {
            tlc_cold_request_t *next = request->next;
            pthread_mutex_lock(&request->mu);
            request->durable_rc = rc == 0 ? 0 : -1;
            request->durable = true;
            if (rc == 0)
                atomic_store_explicit(&cold->durable_seq, request->seq, memory_order_release);
            pthread_cond_broadcast(&request->cv);
            pthread_mutex_unlock(&request->mu);
            cold_request_release(request);
            request = next;
        }
    }
    return NULL;
}

static int cold_validate_input(const tlc_cold_event_input_t *input) {
    RETURN_IF(!input, -1);
    RETURN_IF(!input->key || input->key_len == 0 ||
              (input->value_len && !input->value) ||
              (input->op == TLC_COLD_OP_DEL && input->value_len != 0) ||
              (input->op != TLC_COLD_OP_PUT && input->op != TLC_COLD_OP_DEL), -1);
    return 0;
}

int tlc_cold_open(tlc_cold_t **out, const tlc_cold_config_t *config) {
    RETURN_IF(!out || !config || !config->directory || !getMonotonicNs ||
              config->segment_bytes == 0 || config->queue_capacity == 0 ||
              config->group_max_entries == 0 || config->group_max_delay_us == 0, -1);
    *out = NULL;
    tlc_cold_t *cold = zcalloc(sizeof(*cold));
    if (!cold) {
        serverLog(LL_WARNING, "COLD runtime allocation failed: directory=%s",
                  config->directory);
        return -1;
    }
    cold->directory = zstrdup(config->directory);
    cold->segment_bytes = config->segment_bytes;
    cold->queue_capacity = config->queue_capacity;
    cold->group_max_entries = config->group_max_entries;
    cold->group_max_delay_us = config->group_max_delay_us;
    cold->fd = -1;
    if (!cold->directory || (mkdir(cold->directory, 0755) != 0 && errno != EEXIST)) {
        serverLog(LL_WARNING,
                  "COLD directory setup failed: directory=%s errno=%d (%s)",
                  config->directory, errno, strerror(errno));
        goto failed;
    }
    cold->queue = zcalloc_num(cold->queue_capacity, sizeof(*cold->queue));
    if (!cold->queue) {
        serverLog(LL_WARNING,
                  "COLD queue allocation failed: directory=%s capacity=%u",
                  config->directory, cold->queue_capacity);
        goto failed;
    }
    cold->checksum_state = XXH3_createState();
    if (!cold->checksum_state)
        goto failed;
    pthread_mutex_init(&cold->queue_mu, NULL);
    pthread_cond_init(&cold->queue_not_empty, NULL);
    pthread_cond_init(&cold->queue_not_full, NULL);
    pthread_mutex_init(&cold->pending_mu, NULL);
    pthread_cond_init(&cold->pending_cv, NULL);
    pthread_mutex_init(&cold->io_mu, NULL);
    pthread_mutex_init(&cold->checkpoint_mu, NULL);
    atomic_init(&cold->stopping, false);
    atomic_init(&cold->writer_done, false);
    atomic_init(&cold->io_error, 0);
    atomic_init(&cold->appended_seq, 0);
    atomic_init(&cold->durable_seq, 0);
    cold->next_seq = 1;
    if (cold_recover_segments(cold) != 0)
        goto failed_initialized;
    if (pthread_create(&cold->writer_thread, NULL, cold_writer_main, cold) != 0)
        goto failed_fd;
    if (pthread_create(&cold->flush_thread, NULL, cold_flush_main, cold) != 0) {
        pthread_mutex_lock(&cold->queue_mu);
        atomic_store_explicit(&cold->stopping, true, memory_order_release);
        pthread_cond_broadcast(&cold->queue_not_empty);
        pthread_mutex_unlock(&cold->queue_mu);
        pthread_join(cold->writer_thread, NULL);
        goto failed_fd;
    }
    *out = cold;
    return 0;

failed_fd:
    close(cold->fd);
    cold->fd = -1;
failed_initialized:
    pthread_mutex_destroy(&cold->checkpoint_mu);
    pthread_mutex_destroy(&cold->io_mu);
    pthread_cond_destroy(&cold->pending_cv);
    pthread_mutex_destroy(&cold->pending_mu);
    pthread_cond_destroy(&cold->queue_not_full);
    pthread_cond_destroy(&cold->queue_not_empty);
    pthread_mutex_destroy(&cold->queue_mu);
failed:
    serverLog(LL_WARNING,
              "COLD runtime initialization failed: directory=%s",
              config->directory);
    if (cold->checksum_state)
        XXH3_freeState(cold->checksum_state);
    zfree(cold->queue);
    zfree(cold->directory);
    zfree(cold);
    return -1;
}

void tlc_cold_close(tlc_cold_t *cold) {
    RETURN_IF(!cold);
    pthread_mutex_lock(&cold->queue_mu);
    atomic_store_explicit(&cold->stopping, true, memory_order_release);
    pthread_cond_broadcast(&cold->queue_not_empty);
    pthread_cond_broadcast(&cold->queue_not_full);
    pthread_mutex_unlock(&cold->queue_mu);
    pthread_join(cold->writer_thread, NULL);
    pthread_mutex_lock(&cold->pending_mu);
    pthread_cond_broadcast(&cold->pending_cv);
    pthread_mutex_unlock(&cold->pending_mu);
    pthread_join(cold->flush_thread, NULL);
    if (cold->fd >= 0) {
        if (fsync(cold->fd) != 0)
            serverLog(LL_WARNING,
                      "COLD shutdown fsync failed: directory=%s errno=%d (%s)",
                      cold->directory, errno, strerror(errno));
        close(cold->fd);
    }
    for (uint32_t i = 0; i < cold->queue_capacity; i++) {
        if (cold->queue[i])
            cold_request_release(cold->queue[i]);
    }
    pthread_mutex_destroy(&cold->io_mu);
    pthread_mutex_destroy(&cold->checkpoint_mu);
    pthread_cond_destroy(&cold->pending_cv);
    pthread_mutex_destroy(&cold->pending_mu);
    pthread_cond_destroy(&cold->queue_not_full);
    pthread_cond_destroy(&cold->queue_not_empty);
    pthread_mutex_destroy(&cold->queue_mu);
    XXH3_freeState(cold->checksum_state);
    zfree(cold->queue);
    zfree(cold->directory);
    zfree(cold);
}

int tlc_cold_submit(tlc_cold_t *cold,
                    const tlc_cold_event_input_t *input,
                    tlc_cold_ack_mode_t ack_mode,
                    uint64_t *seq) {
    RETURN_IF(!cold, -1);
    RETURN_IF(cold_validate_input(input) != 0, -1);
    RETURN_IF(ack_mode != TLC_COLD_ACK_ACCEPTED &&
              ack_mode != TLC_COLD_ACK_DURABLE, -1);
    tlc_cold_request_t *request = cold_request_create(input);
    if (!request)
        return -1;
    pthread_mutex_lock(&cold->queue_mu);
    while (cold->queue_count == cold->queue_capacity &&
           !atomic_load_explicit(&cold->stopping, memory_order_acquire))
        pthread_cond_wait(&cold->queue_not_full, &cold->queue_mu);
    if (atomic_load_explicit(&cold->stopping, memory_order_acquire)) {
        serverLog(LL_WARNING,
                  "COLD submit rejected during shutdown: directory=%s",
                  cold->directory);
        pthread_mutex_unlock(&cold->queue_mu);
        cold_request_release(request);
        cold_request_release(request);
        return -1;
    }
    cold->queue[cold->queue_tail] = request;
    cold->queue_tail = (cold->queue_tail + 1) % cold->queue_capacity;
    cold->queue_count++;
    pthread_cond_signal(&cold->queue_not_empty);
    pthread_mutex_unlock(&cold->queue_mu);

    pthread_mutex_lock(&request->mu);
    while (!request->appended)
        pthread_cond_wait(&request->cv, &request->mu);
    int rc = request->append_rc;
    uint64_t request_seq = request->seq;
    if (rc == 0 && ack_mode == TLC_COLD_ACK_DURABLE)
        while (!request->durable)
            pthread_cond_wait(&request->cv, &request->mu);
    if (rc == 0 && ack_mode == TLC_COLD_ACK_DURABLE)
        rc = request->durable_rc;
    pthread_mutex_unlock(&request->mu);
    if (seq)
        *seq = request_seq;
    cold_request_release(request);
    return rc;
}

int tlc_cold_get_progress(const tlc_cold_t *cold,
                          tlc_cold_progress_t *progress) {
    RETURN_IF(!cold || !progress, -1);
    progress->appended_seq = atomic_load_explicit(&cold->appended_seq, memory_order_acquire);
    progress->durable_seq = atomic_load_explicit(&cold->durable_seq, memory_order_acquire);
    progress->segment_id = atomic_load_explicit(&cold->segment_id,
                                                memory_order_acquire);
    progress->segment_offset = atomic_load_explicit(&cold->segment_offset,
                                                    memory_order_acquire);
    return 0;
}

static int cold_replay_internal(tlc_cold_t *cold,
                                const uint64_t *captured_seq,
                                uint32_t meta_shard_count,
                                tlc_cold_replay_fn callback,
                                void *arg) {
    pthread_mutex_lock(&cold->io_mu);
    tlc_cold_segment_id_t *segments = NULL;
    size_t count = 0;
    int rc = cold_collect_segments(cold, &segments, &count);
    uint64_t expected_seq = 0;
    if (rc == 0) {
        for (size_t i = 0; i < count; i++) {
            rc = cold_replay_segment(cold,
                                     segments[i].id,
                                     &expected_seq,
                                     captured_seq,
                                     meta_shard_count,
                                     callback,
                                     arg);
            if (rc != 0)
                break;
        }
    }
    zfree(segments);
    pthread_mutex_unlock(&cold->io_mu);
    if (rc != 0) {
        serverLog(LL_WARNING,
                  "COLD AOF replay failed: directory=%s rc=%d expected_seq=%llu",
                  cold->directory,
                  rc,
                  (unsigned long long)expected_seq);
    }
    return rc;
}

int tlc_cold_replay(tlc_cold_t *cold, tlc_cold_replay_fn callback, void *arg) {
    RETURN_IF(!cold || !callback, -1);
    return cold_replay_internal(cold, NULL, 0, callback, arg);
}

int tlc_cold_replay_after(tlc_cold_t *cold,
                          const uint64_t *captured_seq,
                          uint32_t meta_shard_count,
                          tlc_cold_replay_fn callback,
                          void *arg) {
    RETURN_IF(!cold || !captured_seq || meta_shard_count == 0 || !callback, -1);
    return cold_replay_internal(cold, captured_seq, meta_shard_count,
                                callback, arg);
}

static int cold_publish_checkpoint_locked(
    tlc_cold_t *cold,
    uint64_t generation,
    uint64_t term,
    uint32_t meta_shard_count,
    const tlc_cold_checkpoint_record_t *records,
    uint32_t record_count,
    tlc_cold_checkpoint_result_t *result) {
    /* Preconditions are validated by tlc_core_publish_checkpoint(). */
    uint64_t durable_seq = atomic_load_explicit(&cold->durable_seq,
                                                memory_order_acquire);
    for (uint32_t i = 0; i < record_count; i++) {
        RETURN_IF(records[i].meta_shard_id != i ||
                  records[i].captured_seq > durable_seq ||
                  (records[i].state_len && !records[i].state), -1);
    }

    XXH3_state_t *state = XXH3_createState();
    if (!state) {
        serverLog(LL_WARNING,
                  "COLD checkpoint checksum state allocation failed: generation=%llu",
                  (unsigned long long)generation);
        return -1;
    }
    tlc_cold_checkpoint_header_t header = {
        .magic = TLC_COLD_CHECKPOINT_MAGIC,
        .format_version = TLC_COLD_CHECKPOINT_VERSION,
        .header_bytes = sizeof(header),
        .generation = generation,
        .term = term,
        .checkpoint_seq = UINT64_MAX,
        .meta_shard_count = meta_shard_count,
        .record_count = record_count,
        .generation_checksum = 0,
    };
    for (uint32_t i = 0; i < record_count; i++) {
        if (records[i].captured_seq < header.checkpoint_seq)
            header.checkpoint_seq = records[i].captured_seq;
    }
    XXH3_64bits_reset(state);
    XXH3_64bits_update(state, &header,
                       offsetof(tlc_cold_checkpoint_header_t,
                                generation_checksum));
    for (uint32_t i = 0; i < record_count; i++) {
        tlc_cold_checkpoint_record_header_t record_header = {
            .meta_shard_id = records[i].meta_shard_id,
            .state_len = records[i].state_len,
            .captured_seq = records[i].captured_seq,
            .state_checksum = XXH3_64bits(records[i].state,
                                           records[i].state_len),
        };
        XXH3_64bits_update(state, &record_header, sizeof(record_header));
        XXH3_64bits_update(state, records[i].state, records[i].state_len);
    }
    header.generation_checksum = (uint64_t)XXH3_64bits_digest(state);
    char tmp_path[4096];
    char final_path[4096];
    if (cold_make_checkpoint_path(cold, generation, ".tmp",
                                  tmp_path, sizeof(tmp_path)) != 0 ||
        cold_make_checkpoint_path(cold, generation, "",
                                  final_path, sizeof(final_path)) != 0) {
        XXH3_freeState(state);
        return -1;
    }
    int existing_fd = open(final_path, O_RDONLY);
    if (existing_fd >= 0) {
        close(existing_fd);
        serverLog(LL_WARNING,
                  "COLD checkpoint generation already exists: generation=%llu",
                  (unsigned long long)generation);
        XXH3_freeState(state);
        return -1;
    }
    if (errno != ENOENT) {
        serverLog(LL_WARNING,
                  "COLD checkpoint generation probe failed: generation=%llu errno=%d (%s)",
                  (unsigned long long)generation, errno, strerror(errno));
        XXH3_freeState(state);
        return -1;
    }
    tlc_cold_checkpoint_manifest_t prior_manifest;
    int manifest_rc = cold_read_checkpoint_manifest(cold, &prior_manifest);
    if (manifest_rc < 0) {
        serverLog(LL_WARNING,
                  "COLD checkpoint manifest validation failed: generation=%llu errno=%d (%s)",
                  (unsigned long long)generation, errno, strerror(errno));
        XXH3_freeState(state);
        return -1;
    }
    if (manifest_rc == 0 && prior_manifest.generation >= generation) {
        serverLog(LL_WARNING,
                  "COLD checkpoint generation is not increasing: current=%llu requested=%llu",
                  (unsigned long long)prior_manifest.generation,
                  (unsigned long long)generation);
        XXH3_freeState(state);
        return -1;
    }
    if (manifest_rc == 0 && cold_validate_checkpoint_locked(
            cold, prior_manifest.generation, meta_shard_count, NULL) != 0) {
        serverLog(LL_WARNING,
                  "COLD previous checkpoint validation failed: generation=%llu errno=%d (%s)",
                  (unsigned long long)prior_manifest.generation,
                  errno, strerror(errno));
        XXH3_freeState(state);
        return -1;
    }
    int fd = open(tmp_path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0 || COLD_CHECKPOINT_FAIL(
            cold, TLC_COLD_CHECKPOINT_FAIL_FILE_WRITE) != 0 ||
        cold_write_full(fd, &header, sizeof(header)) != 0) {
        serverLog(LL_WARNING,
                  "COLD checkpoint temporary file open/write failed: generation=%llu errno=%d (%s)",
                  (unsigned long long)generation, errno, strerror(errno));
        if (fd >= 0)
            close(fd);
        XXH3_freeState(state);
        return -1;
    }
    for (uint32_t i = 0; i < record_count; i++) {
        tlc_cold_checkpoint_record_header_t record_header = {
            .meta_shard_id = records[i].meta_shard_id,
            .state_len = records[i].state_len,
            .captured_seq = records[i].captured_seq,
            .state_checksum = XXH3_64bits(records[i].state,
                                           records[i].state_len),
        };
        if (cold_write_full(fd, &record_header, sizeof(record_header)) != 0 ||
            cold_write_full(fd, records[i].state, records[i].state_len) != 0) {
            serverLog(LL_WARNING,
                      "COLD checkpoint record write failed: generation=%llu meta_shard_id=%u errno=%d (%s)",
                      (unsigned long long)generation,
                      records[i].meta_shard_id,
                      errno, strerror(errno));
            close(fd);
            XXH3_freeState(state);
            return -1;
        }
    }
    if (COLD_CHECKPOINT_FAIL(cold,
                                  TLC_COLD_CHECKPOINT_FAIL_FILE_FSYNC) != 0 ||
        fsync(fd) != 0 || close(fd) != 0 ||
        COLD_CHECKPOINT_FAIL(cold,
                                  TLC_COLD_CHECKPOINT_FAIL_FILE_RENAME) != 0 ||
        rename(tmp_path, final_path) != 0 ||
        COLD_CHECKPOINT_FAIL(
            cold, TLC_COLD_CHECKPOINT_FAIL_FILE_DIRECTORY_FSYNC) != 0 ||
        cold_fsync_directory(cold) != 0) {
        serverLog(LL_WARNING,
                  "COLD checkpoint publish failed: generation=%llu errno=%d (%s)",
                  (unsigned long long)generation, errno, strerror(errno));
        if (fd >= 0)
            close(fd);
        XXH3_freeState(state);
        return -1;
    }

    tlc_cold_checkpoint_manifest_t manifest = {
        .magic = TLC_COLD_CHECKPOINT_MAGIC,
        .format_version = TLC_COLD_CHECKPOINT_VERSION,
        .header_bytes = sizeof(manifest),
        .generation = generation,
        .checkpoint_seq = header.checkpoint_seq,
        .meta_shard_count = meta_shard_count,
        .record_count = record_count,
        .generation_checksum = header.generation_checksum,
    };
    char manifest_tmp[4096];
    char manifest_path[4096];
    int manifest_path_rc = snprintf(manifest_tmp, sizeof(manifest_tmp),
                                    "%s/checkpoint.manifest.tmp",
                                    cold->directory);
    int manifest_final_rc = snprintf(manifest_path, sizeof(manifest_path),
                                     "%s/checkpoint.manifest",
                                     cold->directory);
    if (manifest_path_rc < 0 || (size_t)manifest_path_rc >= sizeof(manifest_tmp) ||
        manifest_final_rc < 0 || (size_t)manifest_final_rc >= sizeof(manifest_path)) {
        XXH3_freeState(state);
        return -1;
    }
    int manifest_fd = open(manifest_tmp, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    int manifest_ok = manifest_fd >= 0 &&
        COLD_CHECKPOINT_FAIL(cold,
                                  TLC_COLD_CHECKPOINT_FAIL_MANIFEST_WRITE) == 0 &&
        cold_write_full(manifest_fd, &manifest, sizeof(manifest)) == 0 &&
        COLD_CHECKPOINT_FAIL(cold,
                                  TLC_COLD_CHECKPOINT_FAIL_MANIFEST_FSYNC) == 0 &&
        fsync(manifest_fd) == 0;
    if (manifest_fd >= 0)
        close(manifest_fd);
    if (!manifest_ok ||
        COLD_CHECKPOINT_FAIL(cold,
                                  TLC_COLD_CHECKPOINT_FAIL_MANIFEST_RENAME) != 0 ||
        rename(manifest_tmp, manifest_path) != 0 ||
        COLD_CHECKPOINT_FAIL(
            cold, TLC_COLD_CHECKPOINT_FAIL_MANIFEST_DIRECTORY_FSYNC) != 0 ||
        cold_fsync_directory(cold) != 0) {
        serverLog(LL_WARNING,
                  "COLD checkpoint manifest publish failed: generation=%llu errno=%d (%s)",
                  (unsigned long long)generation, errno, strerror(errno));
        XXH3_freeState(state);
        return -1;
    }
    result->generation = generation;
    result->checkpoint_seq = header.checkpoint_seq;
    result->generation_checksum = header.generation_checksum;
    XXH3_freeState(state);
    return 0;
}

int tlc_cold_publish_checkpoint(
    tlc_cold_t *cold,
    uint64_t generation,
    uint64_t term,
    uint32_t meta_shard_count,
    const tlc_cold_checkpoint_record_t *records,
    uint32_t record_count,
    tlc_cold_checkpoint_result_t *result) {
    /* Shape is a strict caller contract; this function only serializes it. */
    pthread_mutex_lock(&cold->checkpoint_mu);
    int rc = cold_publish_checkpoint_locked(cold, generation, term,
                                            meta_shard_count, records,
                                            record_count, result);
    pthread_mutex_unlock(&cold->checkpoint_mu);
    return rc;
}

#ifdef TLC_COLD_ENABLE_FAILPOINT
int tlc_cold_set_checkpoint_failpoint(
    tlc_cold_t *cold,
    tlc_cold_checkpoint_failpoint_t failpoint) {
    RETURN_IF(!cold || failpoint > TLC_COLD_CHECKPOINT_FAIL_MANIFEST_DIRECTORY_FSYNC,
              -1);
    pthread_mutex_lock(&cold->checkpoint_mu);
    cold->checkpoint_failpoint = failpoint;
    pthread_mutex_unlock(&cold->checkpoint_mu);
    return 0;
}

int tlc_cold_set_io_failpoint(tlc_cold_t *cold,
                              tlc_cold_io_failpoint_t failpoint) {
    RETURN_IF(!cold || failpoint > TLC_COLD_IO_FAIL_GROUP_FSYNC, -1);
    pthread_mutex_lock(&cold->io_mu);
    cold->io_failpoint = failpoint;
    pthread_mutex_unlock(&cold->io_mu);
    return 0;
}

int tlc_cold_set_compact_failpoint(
    tlc_cold_t *cold,
    tlc_cold_compact_failpoint_t failpoint) {
    RETURN_IF(!cold || failpoint > TLC_COLD_COMPACT_FAIL_BEFORE_DIRECTORY_FSYNC,
              -1);
    pthread_mutex_lock(&cold->checkpoint_mu);
    cold->compact_failpoint = failpoint;
    pthread_mutex_unlock(&cold->checkpoint_mu);
    return 0;
}
#endif

int tlc_cold_validate_checkpoint(tlc_cold_t *cold,
                                 uint64_t generation,
                                 uint32_t expected_meta_shard_count,
                                 tlc_cold_checkpoint_result_t *result) {
    RETURN_IF(!cold, -1);
    pthread_mutex_lock(&cold->checkpoint_mu);
    int rc = cold_validate_checkpoint_locked(cold, generation,
                                             expected_meta_shard_count,
                                             result);
    pthread_mutex_unlock(&cold->checkpoint_mu);
    if (rc != 0)
        serverLog(LL_WARNING,
                  "COLD checkpoint validation failed: generation=%llu errno=%d (%s)",
                  (unsigned long long)generation, errno, strerror(errno));
    return rc;
}

int tlc_cold_load_checkpoint(tlc_cold_t *cold,
                             uint32_t expected_meta_shard_count,
                             tlc_cold_checkpoint_load_fn callback,
                             void *arg,
                             tlc_cold_checkpoint_result_t *result) {
    RETURN_IF(!cold || !callback, -1);
    pthread_mutex_lock(&cold->checkpoint_mu);
    tlc_cold_checkpoint_manifest_t manifest;
    int rc = cold_read_checkpoint_manifest(cold, &manifest);
    if (rc != 0 || (expected_meta_shard_count &&
                    manifest.meta_shard_count != expected_meta_shard_count)) {
        pthread_mutex_unlock(&cold->checkpoint_mu);
        return -1;
    }
    rc = cold_validate_checkpoint_locked(cold, manifest.generation,
                                         expected_meta_shard_count, NULL);
    if (rc != 0) {
        pthread_mutex_unlock(&cold->checkpoint_mu);
        return -1;
    }
    char path[4096];
    if (cold_make_checkpoint_path(cold, manifest.generation, "",
                                  path, sizeof(path)) != 0) {
        pthread_mutex_unlock(&cold->checkpoint_mu);
        return -1;
    }
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        pthread_mutex_unlock(&cold->checkpoint_mu);
        return -1;
    }
    tlc_cold_checkpoint_header_t header;
    if (cold_read_full(fd, &header, sizeof(header)) != 0) {
        close(fd);
        pthread_mutex_unlock(&cold->checkpoint_mu);
        return -1;
    }
    for (uint32_t i = 0; i < header.record_count && rc == 0; i++) {
        tlc_cold_checkpoint_record_header_t record_header;
        if (cold_read_full(fd, &record_header, sizeof(record_header)) != 0 ||
            record_header.meta_shard_id != i) {
            rc = -1;
            break;
        }
        uint8_t *state = record_header.state_len ?
            zmalloc(record_header.state_len) : NULL;
        if (record_header.state_len && !state) {
            rc = -1;
            break;
        }
        if (record_header.state_len &&
            cold_read_full(fd, state, record_header.state_len) != 0) {
            zfree(state);
            rc = -1;
            break;
        }
        rc = callback(record_header.meta_shard_id,
                      record_header.captured_seq,
                      state,
                      record_header.state_len,
                      arg);
        zfree(state);
    }
    if (close(fd) != 0)
        rc = -1;
    if (rc == 0 && result) {
        result->generation = manifest.generation;
        result->checkpoint_seq = manifest.checkpoint_seq;
        result->generation_checksum = manifest.generation_checksum;
    }
    pthread_mutex_unlock(&cold->checkpoint_mu);
    if (rc != 0)
        serverLog(LL_WARNING,
                  "COLD checkpoint load failed: generation=%llu errno=%d (%s)",
                  (unsigned long long)manifest.generation, errno, strerror(errno));
    return rc;
}

int tlc_cold_compact(tlc_cold_t *cold,
                     uint64_t checkpoint_floor_seq,
                     uint64_t ha_safe_point_seq,
                     uint32_t checkpoint_retention_count) {
    RETURN_IF(!cold || checkpoint_retention_count == 0, -1);
    uint64_t compact_through = checkpoint_floor_seq < ha_safe_point_seq ?
        checkpoint_floor_seq : ha_safe_point_seq;
    pthread_mutex_lock(&cold->checkpoint_mu);
    tlc_cold_checkpoint_manifest_t manifest = {0};
    int manifest_rc = cold_read_checkpoint_manifest(cold, &manifest);
    if (manifest_rc != 0) {
        errno = manifest_rc > 0 ? ENOENT : errno;
        serverLog(LL_WARNING,
                  "COLD compact rejected: valid checkpoint manifest unavailable: floor=%llu errno=%d (%s)",
                  (unsigned long long)checkpoint_floor_seq,
                  errno, strerror(errno));
        pthread_mutex_unlock(&cold->checkpoint_mu);
        return -1;
    }
    if (checkpoint_floor_seq > manifest.checkpoint_seq) {
        errno = EINVAL;
        serverLog(LL_WARNING,
                  "COLD compact rejected: checkpoint floor exceeds valid manifest: floor=%llu manifest_seq=%llu errno=%d (%s)",
                  (unsigned long long)checkpoint_floor_seq,
                  (unsigned long long)(manifest.checkpoint_seq),
                  errno, strerror(errno));
        pthread_mutex_unlock(&cold->checkpoint_mu);
        return -1;
    }
    if (cold_validate_checkpoint_locked(cold, manifest.generation,
                                        manifest.meta_shard_count, NULL) != 0) {
        serverLog(LL_WARNING,
                  "COLD compact rejected: active checkpoint validation failed: generation=%llu errno=%d (%s)",
                  (unsigned long long)manifest.generation,
                  errno, strerror(errno));
        pthread_mutex_unlock(&cold->checkpoint_mu);
        return -1;
    }
    tlc_cold_generation_id_t *generations = NULL;
    size_t generation_count = 0;
    if (cold_collect_generations(cold, &generations, &generation_count) != 0) {
        serverLog(LL_WARNING,
                  "COLD compact generation enumeration failed: directory=%s errno=%d (%s)",
                  cold->directory, errno, strerror(errno));
        pthread_mutex_unlock(&cold->checkpoint_mu);
        return -1;
    }
    uint64_t retained_floor_seq = UINT64_MAX;
    uint32_t kept = 0;
    for (size_t i = generation_count; i > 0; i--) {
        uint64_t generation = generations[i - 1].id;
        if (generation != manifest.generation &&
            kept >= checkpoint_retention_count)
            continue;
        tlc_cold_checkpoint_result_t retained_result;
        if (cold_validate_checkpoint_locked(cold, generation,
                                            manifest.meta_shard_count,
                                            &retained_result) != 0) {
            serverLog(LL_WARNING,
                      "COLD compact rejected: retained checkpoint validation failed: generation=%llu errno=%d (%s)",
                      (unsigned long long)generation, errno, strerror(errno));
            zfree(generations);
            pthread_mutex_unlock(&cold->checkpoint_mu);
            return -1;
        }
        if (retained_result.checkpoint_seq < retained_floor_seq)
            retained_floor_seq = retained_result.checkpoint_seq;
        kept++;
    }
    if (retained_floor_seq == UINT64_MAX ||
        checkpoint_floor_seq > retained_floor_seq) {
        errno = EINVAL;
        serverLog(LL_WARNING,
                  "COLD compact rejected: floor exceeds retained checkpoint floor: floor=%llu retained_floor=%llu",
                  (unsigned long long)checkpoint_floor_seq,
                  (unsigned long long)retained_floor_seq);
        zfree(generations);
        pthread_mutex_unlock(&cold->checkpoint_mu);
        return -1;
    }
    compact_through = retained_floor_seq < ha_safe_point_seq ?
        retained_floor_seq : ha_safe_point_seq;
    pthread_mutex_lock(&cold->io_mu);
    int rc = 0;
    uint64_t active_segment = atomic_load_explicit(&cold->segment_id,
                                                   memory_order_acquire);
    tlc_cold_segment_id_t *segments = NULL;
    size_t segment_count = 0;
    if (cold_collect_segments(cold, &segments, &segment_count) != 0) {
        serverLog(LL_WARNING,
                  "COLD compact segment enumeration failed: directory=%s errno=%d (%s)",
                  cold->directory, errno, strerror(errno));
        rc = -1;
    } else {
        for (size_t i = 0; i < segment_count && rc == 0; i++) {
            if (segments[i].id >= active_segment)
                continue;
            uint64_t last_seq = 0;
            if (cold_segment_last_seq(cold, segments[i].id, &last_seq) != 0) {
                serverLog(LL_WARNING,
                          "COLD compact segment validation failed: segment=%llu errno=%d (%s)",
                          (unsigned long long)segments[i].id,
                          errno, strerror(errno));
                rc = -1;
                break;
            }
            if (last_seq != 0 && last_seq <= compact_through) {
                char path[4096];
                if (cold_make_segment_path(cold, segments[i].id,
                                           path, sizeof(path)) != 0 ||
                    unlink(path) != 0 ||
                    COLD_COMPACT_FAIL(
                        cold, TLC_COLD_COMPACT_FAIL_AFTER_SEGMENT_DELETE) != 0) {
                    serverLog(LL_WARNING,
                              "COLD compact segment removal failed: segment=%llu errno=%d (%s)",
                              (unsigned long long)segments[i].id,
                              errno, strerror(errno));
                    rc = -1;
                }
            }
        }
    }
    zfree(segments);
    if (rc == 0) {
        kept = 0;
        for (size_t i = generation_count; i > 0; i--) {
            uint64_t generation = generations[i - 1].id;
            if (generation == manifest.generation ||
                kept < checkpoint_retention_count) {
                kept++;
                continue;
            }
            char path[4096];
            if (cold_make_checkpoint_path(cold, generation, "",
                                          path, sizeof(path)) != 0 ||
                unlink(path) != 0 ||
                COLD_COMPACT_FAIL(
                    cold, TLC_COLD_COMPACT_FAIL_AFTER_GENERATION_DELETE) != 0) {
                serverLog(LL_WARNING,
                          "COLD compact checkpoint removal failed: generation=%llu errno=%d (%s)",
                          (unsigned long long)generation,
                          errno, strerror(errno));
                rc = -1;
                break;
            }
        }
    }
    zfree(generations);
    if (rc == 0 &&
        (COLD_COMPACT_FAIL(cold,
                           TLC_COLD_COMPACT_FAIL_BEFORE_DIRECTORY_FSYNC) != 0 ||
         cold_fsync_directory(cold) != 0)) {
        serverLog(LL_WARNING,
                  "COLD compact directory fsync failed: errno=%d (%s)",
                  errno, strerror(errno));
        rc = -1;
    }
    pthread_mutex_unlock(&cold->io_mu);
    pthread_mutex_unlock(&cold->checkpoint_mu);
    return rc;
}
