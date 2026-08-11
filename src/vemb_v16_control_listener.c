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
#include <string.h>
#include <sys/socket.h>

/*
 * Needs access to server.vemb_v16_proxy. server.h is heavy but is the
 * canonical way to reach the proxy handle from redis-server integration
 * code (the resp_commands.c predecessor did the same).
 */
#include "server.h"

#define VEMB_V16_CONTROL_LISTENER_MAX_CONCURRENT 64

static atomic_int g_control_active_count = 0;

/* Stub-respond to channel-management frames (CLOSE_CHANNEL,
 * CLOSE_ALL_CHANNELS) without touching proxy->channels[] from a control
 * pthread. The data-plane channels are owned by the proxy IO thread; calling
 * vemb_v16_proxy_close_channel_by_id from a detached pthread races with
 * the proxy IO thread's own channel lifecycle ops and crashes in free().
 *
 * These frames are pure client-side cleanup courtesy: the proxy's
 * reap_inactive_tcp_channels path will reclaim the channel once the TCP
 * connection drops. Stub OK is sufficient — clients get a clean ACK and
 * proceed with their own shutdown.
 *
 * Mirrors tcp_write_status() in vemb_v16_tcp_transport.c (which we can't
 * call directly because it's static). */
static void stub_close_frame_response(int fd, uint64_t closed_count) {
    vemb_v16_net_status_t st;
    memset(&st, 0, sizeof(st));
    st.status = VEMB_V16_STATUS_OK;
    st.value = closed_count;
    uint8_t payload[16];
    size_t payload_len = 0;
    if (vemb_v16_net_status_encode(payload, sizeof(payload), &st,
                                   &payload_len) != 0)
        return;
    vemb_v16_net_write_frame(fd,
                             VEMB_V16_NET_CONTROL_STATUS,
                             0, 0, 0,
                             payload, (uint32_t)payload_len);
}

static void *control_fd_worker(void *arg) {
    int fd = *(int *)arg;
    zfree(arg);

    int prev = atomic_fetch_sub(&g_control_active_count, 1);
    (void)prev;

    /* Peek the frame type (offset 6 in vemb_v16_net_hdr_t: magic[0-3] +
     * version[4-5] + type[6-7]). Intercept CLOSE_CHANNEL and
     * CLOSE_ALL_CHANNELS — see stub_close_frame_response comment for why. */
    uint8_t peek[8];
    ssize_t n = recv(fd, peek, sizeof(peek), MSG_PEEK);
    if (n >= (ssize_t)sizeof(peek)) {
        uint16_t ftype;
        memcpy(&ftype, peek + 6, sizeof(ftype));
        if (ftype == VEMB_V16_NET_CLOSE_CHANNEL) {
            serverLog(LL_VERBOSE,
                      "vemb_v16 control CLOSE_CHANNEL on fd=%d stub-ack (proxy reaper handles cleanup)",
                      fd);
            stub_close_frame_response(fd, 1);
            close(fd);
            return NULL;
        }
        if (ftype == VEMB_V16_NET_CLOSE_ALL_CHANNELS) {
            serverLog(LL_VERBOSE,
                      "vemb_v16 control CLOSE_ALL_CHANNELS on fd=%d stub-ack (proxy reaper handles cleanup)",
                      fd);
            stub_close_frame_response(fd, 0);
            close(fd);
            return NULL;
        }
    }

    serverLog(LL_DEBUG,
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
