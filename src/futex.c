#define _GNU_SOURCE

#include "futex.h"

#ifdef __linux__
#include <errno.h>
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

void futex_wait(atomic_int *word, int expected) {
    while (syscall(SYS_futex, word, FUTEX_WAIT_PRIVATE, expected,
                   NULL, NULL, 0) == -1 && errno == EINTR) {
    }
}

void futex_notify(atomic_int *word) {
    (void)syscall(SYS_futex, word, FUTEX_WAKE_PRIVATE, 1, NULL, NULL, 0);
}
#endif
