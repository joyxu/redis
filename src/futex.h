#ifndef VEMB_FUTEX_H
#define VEMB_FUTEX_H

#include <stdatomic.h>

#ifdef __linux__
void futex_wait(atomic_int *word, int expected);
void futex_notify(atomic_int *word);
#endif

#endif /* VEMB_FUTEX_H */
