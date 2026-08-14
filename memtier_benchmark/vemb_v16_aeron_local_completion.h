/*
 * Copyright (C) 2026 Redis Labs Ltd.
 *
 * This file is part of memtier_benchmark.
 */

#ifndef VEMB_V16_AERON_LOCAL_COMPLETION_H
#define VEMB_V16_AERON_LOCAL_COMPLETION_H

#include <assert.h>
#include <stdint.h>
#include <sys/time.h>
#include <vector>

extern "C" {
#include "../clients/c/internal/vemb_v16_cli_l1.h"
}

/* This is runner-only accounting state. L1 ownership and storage remain in
 * the C SDK; the pinned vector remains valid until ref is released. */
struct vemb_v16_aeron_local_completion {
    uint32_t channel_index;
    uint32_t req_id;
    uint64_t caller_cookie;
    struct timeval sent_time;
    const float *vector;
    uint32_t vector_bytes;
    vemb_v16_cli_l1_ref_t ref;
};

class vemb_v16_aeron_local_completion_queue {
public:
    vemb_v16_aeron_local_completion_queue() : head(0), tail(0), count(0) {
    }

    void init(uint32_t capacity) {
        entries.resize(capacity);
        head = 0;
        tail = 0;
        count = 0;
    }

    bool empty() const {
        return count == 0;
    }

    bool full() const {
        return count == entries.size();
    }

    bool push(const vemb_v16_aeron_local_completion &completion) {
        if (full())
            return false;
        entries[tail] = completion;
        tail = (tail + 1) % entries.size();
        count++;
        return true;
    }

    vemb_v16_aeron_local_completion pop() {
        assert(!empty());
        vemb_v16_aeron_local_completion completion = entries[head];
        head = (head + 1) % entries.size();
        count--;
        return completion;
    }

private:
    std::vector<vemb_v16_aeron_local_completion> entries;
    uint32_t head;
    uint32_t tail;
    uint32_t count;
};

/* An L1-hit completion must not refill a channel while its remote peers from
 * the same pipeline wave are still outstanding.  Otherwise max-delay=0 can
 * flush the refill as a one-item L0 batch. */
class vemb_v16_aeron_remote_batch_wave {
public:
    vemb_v16_aeron_remote_batch_wave() : pending(0) {
    }

    bool active() const {
        return pending != 0;
    }

    void seal(uint32_t submitted) {
        assert(pending == 0);
        assert(submitted != 0);
        pending = submitted;
    }

    void complete_one() {
        assert(pending != 0);
        pending--;
    }

    uint32_t pending_count() const {
        return pending;
    }

private:
    uint32_t pending;
};

#endif /* VEMB_V16_AERON_LOCAL_COMPLETION_H */
