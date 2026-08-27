#ifndef VEMB_V16_CLI_DEADLINE_H
#define VEMB_V16_CLI_DEADLINE_H

#include <stdint.h>

typedef struct vemb_v16_cli_deadline {
    uint64_t delay_ns;
    uint64_t deadline_ns;
    uint64_t deadline_set_ns;
} vemb_v16_cli_deadline_t;

static inline void vemb_v16_cli_deadline_init(
    vemb_v16_cli_deadline_t *deadline, uint64_t delay_ns) {
    deadline->delay_ns = delay_ns;
    deadline->deadline_ns = 0;
    deadline->deadline_set_ns = 0;
}

static inline void vemb_v16_cli_deadline_on_new_leader(
    vemb_v16_cli_deadline_t *deadline, uint64_t now_ns) {
    if (deadline->delay_ns != 0 && deadline->deadline_ns == 0) {
        deadline->deadline_ns = now_ns + deadline->delay_ns;
        deadline->deadline_set_ns = now_ns;
    }
}

static inline int vemb_v16_cli_deadline_flush_due(
    const vemb_v16_cli_deadline_t *deadline, uint32_t pending_count,
    uint64_t now_ns) {
    if (pending_count == 0)
        return 0;
    if (deadline->delay_ns == 0)
        return 1;
    return deadline->deadline_ns != 0 && now_ns >= deadline->deadline_ns;
}

static inline void vemb_v16_cli_deadline_after_progress(
    vemb_v16_cli_deadline_t *deadline, uint32_t pending_count,
    uint64_t now_ns) {
    deadline->deadline_ns = pending_count == 0 ? 0 :
        now_ns + deadline->delay_ns;
    deadline->deadline_set_ns = pending_count == 0 ? 0 : now_ns;
}

static inline void vemb_v16_cli_deadline_clear(
    vemb_v16_cli_deadline_t *deadline) {
    deadline->deadline_ns = 0;
    deadline->deadline_set_ns = 0;
}

#endif
