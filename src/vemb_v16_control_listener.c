#define _GNU_SOURCE

#include "vemb_v16_control_listener.h"
#include "vemb_v16_log.h"
#include "vemb_v16_tcp_transport.h"
#include "vemb_v16_protocol.h"
#include "vemb_v16_net.h"
#include "zmalloc.h"

#include <pthread.h>
#include <stdatomic.h>
#include <unistd.h>
#include <sys/socket.h>

/*
 * Needs access to server.vemb_v16_proxy. server.h is heavy but is the
 * canonical way to reach the proxy handle from redis-server integration
 * code (the resp_commands.c predecessor did the same).
 */
#include "server.h"

#define VEMB_V16_CONTROL_LISTENER_MAX_CONCURRENT 64

static atomic_int g_control_active_count = 0;

static void *control_fd_worker(void *arg) {
    int fd = *(int *)arg;
    zfree(arg);

    int prev = atomic_fetch_sub(&g_control_active_count, 1);
    (void)prev;

    serverLog(LL_NOTICE,
              "vemb_v16 control fd worker start: fd=%d",
              fd);

    /* vemb_v16_tcp_handle_fd takes ownership of fd and closes it. */
    vemb_v16_tcp_handle_fd(server.vemb_v16_proxy, fd);

    return NULL;
}

int vemb_v16_control_inject_fd(int fd) {
    if (fd < 0)
        return -1;

    int active = atomic_fetch_add(&g_control_active_count, 1) + 1;
    if (active > VEMB_V16_CONTROL_LISTENER_MAX_CONCURRENT) {
        atomic_fetch_sub(&g_control_active_count, 1);
        serverLog(LL_WARNING,
                  "vemb_v16 control listener rejected fd=%d: concurrent=%d > cap=%d",
                  fd,
                  active,
                  VEMB_V16_CONTROL_LISTENER_MAX_CONCURRENT);
        close(fd);
        return -1;
    }

    int *fd_arg = zmalloc(sizeof(int));
    if (!fd_arg) {
        atomic_fetch_sub(&g_control_active_count, 1);
        close(fd);
        return -1;
    }
    *fd_arg = fd;

    pthread_t tid;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    int rc = pthread_create(&tid, &attr, control_fd_worker, fd_arg);
    pthread_attr_destroy(&attr);

    if (rc != 0) {
        zfree(fd_arg);
        atomic_fetch_sub(&g_control_active_count, 1);
        close(fd);
        serverLog(LL_WARNING,
                  "vemb_v16 control listener pthread_create failed fd=%d rc=%d",
                  fd,
                  rc);
        return -1;
    }
    return 0;
}
