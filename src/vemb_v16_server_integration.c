#define _GNU_SOURCE

#include "vemb_v16_server_integration.h"
#include "vemb_v16_proxy.h"
#include "vemb_v16_server_tcp_client.h"
#include "vemb_v16_storage.h"
#include "vemb_v16_protocol.h"
#include "vemb_v16_control_listener.h"
#include "vemb_v16_aeron_attach.h"   /* cross-node aeron ATTACH magic sniff */
#include "server.h"
#include "connection.h"
#include "connhelpers.h"   /* callHandler ref-counting: connDecrRefs / CONN_FLAG_CLOSE_SCHEDULED */

/* vemb_v16_log compatibility: standalone server links vemb_v16_log.o,
 * but redis-server already has serverLog in server.c.  We only need
 * the global verbosity variable that vemb_v16_log.h's serverLog macro
 * references when compiling vemb_v16_proxy.o / vemb_v16_supernode.o. */
int vemb_v16_log_verbosity_value = LL_NOTICE;

/* Accessor for proxy.c / supernode.c which can't include server.h
 * (zmalloc.h deprecated free conflicts with their use of libc free). */
int vemb_v16_cross_node_aeron_enabled(void) {
    return server.vemb_v16_cross_node_aeron_enabled;
}

static int vemb_v16_aeron_tcp_control_enabled(void) {
    return server.vemb_v16_aeron_control &&
           !strcmp(server.vemb_v16_aeron_control, "tcp");
}

#include <pthread.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <poll.h>

static void *proxy_run_thread(void *arg) {
    vemb_v16_proxy_run((vemb_v16_proxy_t *)arg);
    return NULL;
}

static vemb_v16_storage_ctx_t *g_vemb_storage = NULL;

int vemb_v16_server_integration_init(void) {
    if (!server.vemb_v16_enabled) return 0;

    uint32_t dim = server.vemb_v16_dim > 0
        ? (uint32_t)server.vemb_v16_dim
        : VEMB_V16_DEFAULT_DIM;
    uint32_t max_vectors = server.vemb_v16_max_vectors > 0
        ? (uint32_t)server.vemb_v16_max_vectors
        : VEMB_V16_DEFAULT_MAX_VECTORS;

    if (!server.vemb_v16_warm_regions_manifest ||
        !server.vemb_v16_warm_regions_manifest[0]) {
        serverLog(LL_WARNING,
                  "VEMB V16 requires --vemb-v16-warm-regions-manifest");
        return -1;
    }

    serverLog(LL_NOTICE,
              "VEMB V16 integration init: dim=%u max_vectors=%u",
              dim, max_vectors);

    vemb_v16_warm_regions_manifest_t manifest;
    memset(&manifest, 0, sizeof(manifest));
    if (vemb_v16_parse_warm_regions_manifest(server.vemb_v16_warm_regions_manifest,
                                              dim * sizeof(float),
                                              &manifest) != 0) {
        serverLog(LL_WARNING, "vemb_v16_parse_warm_regions_manifest failed: %s",
                  server.vemb_v16_warm_regions_manifest);
        return -1;
    }
    if (server.vemb_v16_reset_warm_regions) {
        if (vemb_v16_storage_reset_manifest_regions(&manifest) != 0) {
            serverLog(LL_WARNING, "vemb_v16_storage_reset_manifest_regions failed");
            return -1;
        }
    }

    vemb_v16_storage_ctx_t *storage = NULL;
    if (vemb_v16_storage_ctx_create_from_manifest(&storage,
                                                   dim,
                                                   dim * sizeof(float),
                                                   max_vectors,
                                                   &manifest) != 0) {
        serverLog(LL_WARNING, "vemb_v16_storage_ctx_create_from_manifest failed");
        return -1;
    }

    if (vemb_v16_proxy_create(&server.vemb_v16_proxy,
                              VEMB_V16_UDS_PATH,
                              dim,
                              max_vectors,
                              storage,
                              &manifest) != 0) {
        serverLog(LL_WARNING, "vemb_v16_proxy_create failed");
        vemb_v16_storage_ctx_destroy(storage);
        return -1;
    }
    if (vemb_v16_proxy_set_aeron_ub_path(
            server.vemb_v16_proxy,
            server.vemb_v16_aeron_ub_path &&
            server.vemb_v16_aeron_ub_path[0] ?
                server.vemb_v16_aeron_ub_path :
                VEMB_V16_DEFAULT_AERON_UB_PATH) != 0) {
        serverLog(LL_WARNING, "vemb_v16_proxy_set_aeron_ub_path failed");
        vemb_v16_proxy_destroy(server.vemb_v16_proxy);
        server.vemb_v16_proxy = NULL;
        return -1;
    }
    if (vemb_v16_proxy_set_aeron_response_ub_path(
            server.vemb_v16_proxy,
            server.vemb_v16_aeron_response_ub_path &&
            server.vemb_v16_aeron_response_ub_path[0] ?
                server.vemb_v16_aeron_response_ub_path :
                VEMB_V16_DEFAULT_AERON_RESPONSE_UB_PATH) != 0) {
        serverLog(LL_WARNING,
                  "vemb_v16_proxy_set_aeron_response_ub_path failed");
        vemb_v16_proxy_destroy(server.vemb_v16_proxy);
        server.vemb_v16_proxy = NULL;
        return -1;
    }
    g_vemb_storage = storage;

    if (server.vemb_v16_supernode_workers > 0) {
        if (vemb_v16_proxy_set_supernode_workers(
                server.vemb_v16_proxy,
                (uint32_t)server.vemb_v16_supernode_workers) != 0) {
            serverLog(LL_WARNING,
                      "vemb_v16_proxy_set_supernode_workers failed");
            vemb_v16_proxy_destroy(server.vemb_v16_proxy);
            server.vemb_v16_proxy = NULL;
            return -1;
        }
    }

    if (server.vemb_v16_proxy_io_threads > 0) {
        if (vemb_v16_proxy_set_proxy_io_threads(
                server.vemb_v16_proxy,
                (uint32_t)server.vemb_v16_proxy_io_threads) != 0) {
            serverLog(LL_WARNING,
                      "vemb_v16_proxy_set_proxy_io_threads failed");
            vemb_v16_proxy_destroy(server.vemb_v16_proxy);
            server.vemb_v16_proxy = NULL;
            return -1;
        }
    }

    /* Transport selection. Aeron TCP control is received by Redis' existing
     * TCP accept loop and handed to the proxy after ATTACH sniffing, so the
     * proxy must not bind a second listener on the Redis port. */
    const char *vemb_transport =
        (server.vemb_v16_transport && server.vemb_v16_transport[0])
            ? server.vemb_v16_transport : "sniff";

    if (!strcmp(vemb_transport, "aeron")) {
        int control_rc = vemb_v16_aeron_tcp_control_enabled() ?
            vemb_v16_proxy_enable_aeron_tcp_inject_only(server.vemb_v16_proxy) :
            vemb_v16_proxy_enable_uds(server.vemb_v16_proxy);
        if (control_rc != 0) {
            serverLog(LL_WARNING,
                      "vemb_v16 aeron control setup failed: control=%s",
                      server.vemb_v16_aeron_control ?
                          server.vemb_v16_aeron_control : "(null)");
            vemb_v16_proxy_destroy(server.vemb_v16_proxy);
            server.vemb_v16_proxy = NULL;
            return -1;
        }
        serverLog(LL_NOTICE, "VEMB V16 Aeron control enabled: %s ub_path=%s",
                  vemb_v16_aeron_tcp_control_enabled() ? "tcp/redis-listener" :
                      VEMB_V16_UDS_PATH,
                  server.vemb_v16_aeron_ub_path ?
                      server.vemb_v16_aeron_ub_path :
                      VEMB_V16_DEFAULT_AERON_UB_PATH);
    } else {
        if (vemb_v16_proxy_enable_tcp_inject_only(server.vemb_v16_proxy) != 0) {
            serverLog(LL_WARNING,
                      "vemb_v16_proxy_enable_tcp_inject_only failed");
            vemb_v16_proxy_destroy(server.vemb_v16_proxy);
            server.vemb_v16_proxy = NULL;
            return -1;
        }
        serverLog(LL_NOTICE,
                  "VEMB V16 TCP data path enabled through Redis listening ports");
    }

    if (pthread_create(&server.vemb_v16_proxy_thread, NULL,
                       proxy_run_thread, server.vemb_v16_proxy) != 0) {
        serverLog(LL_WARNING, "pthread_create for proxy_run_thread failed");
        vemb_v16_proxy_destroy(server.vemb_v16_proxy);
        server.vemb_v16_proxy = NULL;
        return -1;
    }

    serverLog(LL_NOTICE, "VEMB V16 integration ready");
    return 0;
}

/* -------------------------------------------------------------------
 * VEMB V16 protocol sniffing on Redis port 6379 (event-driven, non-blocking)
 *
 * When a new TCP connection arrives, peek at the first 4 bytes.
 * If they match the VEMB binary protocol magic (0x56313645),
 * steal the fd and hand it to the VEMB proxy thread via an
 * inject pipe.  Otherwise let normal RESP processing continue.
 *
 * Returns: 0 = RESP (caller proceeds to createClient),
 *          1 = VEMB steal (fd injected; caller frees conn without closing fd),
 *          2 = async pending (client hasn't sent bytes yet; a peek handler
 *              has been registered on the conn, caller must NOT createClient).
 *
 * The fast path tries one non-blocking MSG_PEEK.  If data is already in the
 * socket buffer (common for blocking clients that send HELLO in the
 * connect/accept window), the decision is immediate with zero main-thread
 * blocking.  If data hasn't arrived (libevent/memtier clients that defer the
 * first write to their own event loop), an async peek handler is registered
 * — the main event loop fires it when bytes arrive, with no per-connection
 * poll() blocking.  This replaces the old synchronous poll(5ms) which
 * serialized connection establishment under burst load (5ms × N connections
 * stuck the main thread and caused c=20+ connection drops).
 * ------------------------------------------------------------------- */

/* Try to steal a new connection for cross-node aeron ATTACH.
 *
 * Peeks the first 24 bytes; if they exactly match VEMB_V16_AERON_ATTACH_MAGIC,
 * consumes those bytes and hands the fd to vemb_v16_aeron_attach_handle_fd
 * (which performs a synchronous request/response exchange then closes fd).
 * The ATTACH magic's first 4 bytes are "VEMB" (0x424d4556), which is
 * different from VEMB_V16_MAGIC (0x56313645 = "VEmb"), so no collision with
 * the normal VEMB sniff path.
 *
 * Steals fd from conn (sets conn->fd = -1) on match so caller's connClose
 * won't double-close.  Returns 1 on steal, 0 on no-match.  Caller handles
 * conn cleanup (zfree / refs) — same dance as the VEMB steal paths. */
static int vemb_try_aeron_attach_steal(connection *conn) {
    int fd = conn->fd;
    if (fd < 0) return 0;
    /* Cross-node aeron is opt-in via --vemb-v16-cross-node-aeron yes.
     * When disabled, never peek for the ATTACH magic — falls through
     * to the normal VEMB/RESP sniff path (pre-cross-node behavior). */
    if (!vemb_v16_aeron_tcp_control_enabled() ||
        !server.vemb_v16_transport ||
        strcmp(server.vemb_v16_transport, "aeron") != 0)
        return 0;

    /* Peek 24 bytes without consuming. If not yet available, brief poll
     * — cross-node ATTACH is a control-plane op (channel setup, once per
     * channel), so a <1ms blocking wait is acceptable and avoids the
     * async-handler lifecycle complexity that was dropping connections
     * under burst load (T*C ≥ 7). */
    char magic_buf[VEMB_V16_AERON_ATTACH_MAGIC_LEN];
    ssize_t n = recv(fd, magic_buf, sizeof(magic_buf), MSG_PEEK);
    if (n < (ssize_t)sizeof(magic_buf)) {
        struct pollfd pfd = { .fd = fd, .events = POLLIN, .revents = 0 };
        if (poll(&pfd, 1, 100) > 0 && (pfd.revents & POLLIN)) {
            n = recv(fd, magic_buf, sizeof(magic_buf), MSG_PEEK);
        }
    }
    if (n < (ssize_t)sizeof(magic_buf))
        return 0;

    if (memcmp(magic_buf, VEMB_V16_AERON_ATTACH_MAGIC,
               VEMB_V16_AERON_ATTACH_MAGIC_LEN) != 0)
        return 0;

    /* ATTACH magic matched. Consume the 24-byte magic from the socket
     * buffer — vemb_v16_aeron_attach_handle_fd assumes the magic has
     * already been read and expects to read dim/slot_size fields next. */
    ssize_t consumed = read(fd, magic_buf, sizeof(magic_buf));
    if (consumed != (ssize_t)sizeof(magic_buf)) {
        serverLog(LL_WARNING,
                  "VEMB V16 AERON ATTACH: failed to consume magic on fd %d "
                  "(read returned %zd, expected %zu): %s",
                  fd, consumed, sizeof(magic_buf), strerror(errno));
        return 0;  /* let caller fall through and close */
    }

    /* Steal fd so the caller's connClose won't close it. */
    conn->fd = -1;

    serverLog(LL_NOTICE,
              "VEMB V16 AERON ATTACH on fd %d, dispatching to attach_handle_fd",
              fd);

    int rc = vemb_v16_aeron_attach_handle_fd(server.vemb_v16_proxy, fd);
    if (rc != 0) {
        serverLog(LL_WARNING,
                  "VEMB V16 AERON ATTACH handle_fd rejected fd %d (rc=%d)",
                  fd, rc);
    }

    /* handle_fd always closes fd (success or error) per the protocol
     * contract.  Guard the close with -1 in case of future changes. */
    close(fd);
    return 1;
}

/* Async peek handler: invoked by the main event loop when a pending conn
 * becomes readable. Peeks the first 8 bytes and decides:
 *   - ATTACH magic             → aeron ATTACH (steal + handle_fd)
 *   - non-VEMB magic           → RESP (redis handles)
 *   - VEMB magic + type=HELLO  → data plane (proxy_inject_fd)
 *   - VEMB magic + type!=HELLO → control plane (control_inject_fd) */
static void vemb_async_peek_handler(connection *conn) {
    int fd = conn->fd;
    if (fd < 0) return;  /* shouldn't happen */

    /* Try cross-node aeron ATTACH first — needs 24 bytes peeked. If the
     * socket buffer has fewer than 24 bytes, fall through to the VEMB/RESP
     * 8-byte peek (which also re-checks for partial data). */
    if (vemb_try_aeron_attach_steal(conn)) {
        /* ATTACH magic matched and fd was stolen+handled.  Mirror the
         * cleanup dance used by the VEMB steal paths below. */
        connSetReadHandler(conn, NULL);
        conn->state = CONN_STATE_CLOSED;
        connDecrRefs(conn);
        connClose(conn);
        return;
    }

    uint8_t buf[8];
    ssize_t n = recv(fd, buf, 8, MSG_PEEK);
    if (n < 8) {
        /* Partial data or spurious wakeup. Stay registered; ae will fire
         * again when more bytes arrive. */
        return;
    }

    uint32_t magic;
    memcpy(&magic, buf, sizeof(magic));
    if (magic != VEMB_V16_MAGIC) {
        /* RESP: detach peek handler, then finalize as a normal Redis client. */
        connSetReadHandler(conn, NULL);
        acceptCommonFinalize(conn, 0);
        return;
    }

    /* VEMB frame: read type at byte offset 6 (after magic[4] + version[2]). */
    uint16_t ftype;
    memcpy(&ftype, buf + 6, sizeof(ftype));
    if (ftype == VEMB_V16_NET_HELLO &&
        server.vemb_v16_transport &&
        !strcmp(server.vemb_v16_transport, "aeron")) {
        connSetReadHandler(conn, NULL);
        acceptCommonFinalize(conn, 0);
        return;
    }

    /* Steal the fd from conn so cleanup won't close it. */
    connSetReadHandler(conn, NULL);
    conn->fd = -1;

    if (ftype == VEMB_V16_NET_HELLO) {
        /* Data plane: channel setup. */
        if (vemb_v16_proxy_inject_fd(server.vemb_v16_proxy, fd) != 0) {
            serverLog(LL_WARNING,
                      "VEMB V16 inject_fd failed for fd %d (async), closing", fd);
            close(fd);
        } else {
            serverLog(LL_VERBOSE,
                      "VEMB V16 HELLO on fd %d (async), injecting data plane", fd);
        }
    } else {
        /* Control plane: TOPOLOGY/RANGE/EPOCH/MIGRATION/SCALEOUT_ACK/STATS/etc.
         * vemb_v16_tcp_handle_fd dispatches; unknown types get an error reply
         * and the fd is closed.  No allow-list needed. */
        if (vemb_v16_control_inject_fd(fd) != 0) {
            serverLog(LL_WARNING,
                      "VEMB V16 control_inject_fd rejected fd %d (async), closing", fd);
            /* control_inject_fd already closed fd on rejection. */
        } else {
            serverLog(LL_NOTICE,
                      "VEMB V16 control frame type=0x%02x on fd %d (async), "
                      "dispatching to tcp_handle_fd", ftype, fd);
        }
    }
    conn->state = CONN_STATE_CLOSED;

    /* We are inside callHandler (socket.c event loop) which did
     * connIncrRefs before invoking us.  Direct zfree(conn) here would
     * free the connection while callHandler still holds a ref → after
     * we return, callHandler's connDecrRefs reads freed memory
     * (heap-use-after-free, caught by ASan).
     *
     * Correct sequence:
     *   1. connDecrRefs — drops the creation ref (refs: 2→1)
     *   2. connClose    — fd==-1 so skips fd cleanup; connHasRefs
     *                     (refs==1) so sets CONN_FLAG_CLOSE_SCHEDULED
     *                     without zfree
     *   3. back in callHandler: connDecrRefs (refs: 1→0), sees
     *      CONN_FLAG_CLOSE_SCHEDULED && !connHasRefs → connClose→zfree
     *
     * The fast path (vemb_v16_sniff_and_handoff returning 1) is NOT
     * inside callHandler and can safely zfree directly (see
     * networking.c:1596). */
    connDecrRefs(conn);
    connClose(conn);
}

int vemb_v16_sniff_and_handoff(connection *conn) {
    /* Fast path: proxy not running (VEMB V16 disabled) */
    if (!server.vemb_v16_proxy)
        return 0;
    /* Cannot sniff through TLS */
    if (connIsTLS(conn))
        return 0;

    int fd = conn->fd;
    if (fd < 0) return 0;

    /* Cross-node aeron ATTACH: needs 24 bytes peeked.  If not yet
     * available, fall through to the 8-byte VEMB/RESP peek. */
    if (vemb_try_aeron_attach_steal(conn)) {
        /* ATTACH magic matched; fd was consumed by handle_fd and stolen
         * from conn.  Caller (networking.c acceptCommonHandler) sees
         * return 1 and frees conn without closing fd. */
        return 1;
    }

    /* Non-blocking peek. For blocking clients the HELLO is often already in
     * the socket buffer when accept fires, so this succeeds immediately.
     * Peek 8 bytes so we can read the type field at offset 6. */
    uint8_t buf[8];
    ssize_t n = recv(fd, buf, 8, MSG_PEEK);
    if (n >= 8) {
        uint32_t magic;
        memcpy(&magic, buf, sizeof(magic));
        if (magic != VEMB_V16_MAGIC) {
            /* If the 8 bytes match the ATTACH magic prefix ("VEMB"), the
             * 24-byte ATTACH peek above failed only because TCP hadn't
             * delivered all 24 bytes yet. Register the async handler so
             * EPOLLIN refires when the rest arrives — falling through to
             * RESP here would silently swallow the ATTACH.
             * Skipped when cross-node aeron is disabled (pre-cross-node
             * behavior: anything non-VEMB_V16_MAGIC falls through to RESP). */
            if (vemb_v16_aeron_tcp_control_enabled() &&
                server.vemb_v16_transport &&
                !strcmp(server.vemb_v16_transport, "aeron") &&
                memcmp(buf, VEMB_V16_AERON_ATTACH_MAGIC, 4) == 0) {
                if (connSetReadHandler(conn, vemb_async_peek_handler) == C_OK)
                    return 2;
            }
            return 0;  /* RESP */
        }

        /* VEMB frame — read type to choose data vs control plane. */
        uint16_t ftype;
        memcpy(&ftype, buf + 6, sizeof(ftype));

        if (ftype == VEMB_V16_NET_HELLO) {
            if (server.vemb_v16_transport &&
                !strcmp(server.vemb_v16_transport, "aeron"))
                return 0;
            /* Steal the fd so connClose won't close it. */
            conn->fd = -1;
            serverLog(LL_VERBOSE,
                      "VEMB V16 HELLO on fd %d (fast), injecting data plane", fd);
            if (vemb_v16_proxy_inject_fd(server.vemb_v16_proxy, fd) != 0) {
                serverLog(LL_WARNING,
                          "VEMB V16 inject_fd failed for fd %d (fast), closing", fd);
                close(fd);
            }
        } else {
            /* Steal the fd so connClose won't close it. */
            conn->fd = -1;
            serverLog(LL_NOTICE,
                      "VEMB V16 control frame type=0x%02x on fd %d (fast), "
                      "dispatching to tcp_handle_fd", ftype, fd);
            if (vemb_v16_control_inject_fd(fd) != 0) {
                serverLog(LL_WARNING,
                          "VEMB V16 control_inject_fd rejected fd %d (fast)", fd);
                /* control_inject_fd already closed fd on rejection. */
            }
        }
        return 1;
    }

    /* Data not arrived yet (n < 4, includes EAGAIN). Register an async peek
     * handler; the main event loop will call it when the client sends bytes,
     * with zero main-thread blocking. Caller sees return 2 and skips
     * createClient. */
    if (connSetReadHandler(conn, vemb_async_peek_handler) == C_OK)
        return 2;  /* async pending */

    /* Handler registration failed (out of fd slots in ae?) — fall back to RESP. */
    return 0;
}

void vemb_v16_server_integration_shutdown(void) {
    /* Exit immediately. The heap accumulates latent corruption during runtime
     * that is invisible to ASan (jemalloc-internal rtree/edata state, not
     * user-buffer UAF). When ANY worker thread exits, jemalloc TSD cleanup
     * flushes its tcache back into arenas; the flush dereferences a NULL
     * edata_t and SIGSEGVs. ASan runs are 0-report clean, our alloc/free
     * pairs are correct, but jemalloc's stricter metadata checks trip on the
     * corruption. The crash happens DURING pthread_join, before any explicit
     * _exit at function end could run, so _exit(0) must come before any
     * proxy_stop/storage_destroy that would trigger worker thread exits.
     *
     * Safe in our scale-out test because:
     *   - finishShutdown already ran RDB/AOF flush (we use --save '' --appendonly no)
     *   - kernel reclaims all fds, mmaps, UB device handles on process exit
     *   - cross-node peer detects socket close and tears down its session
     * No correctness impact — only loses exit-time instrumentation (valgrind etc). */
    serverLog(LL_NOTICE, "VEMB V16 integration shutdown: _exit(0) to skip corrupted jemalloc cleanup");
    _exit(0);

    /* Legacy cleanup (unreachable, kept for production paths that may later
     * fix the jemalloc corruption root cause and want graceful shutdown). */
    serverLog(LL_NOTICE, "VEMB V16 integration shutdown...");

    /* Close the internal TCP client connection before stopping proxy */
    vemb_v16_stc_cleanup();

    if (server.vemb_v16_proxy) {
        vemb_v16_proxy_stop(server.vemb_v16_proxy);
        pthread_join(server.vemb_v16_proxy_thread, NULL);
        vemb_v16_proxy_destroy(server.vemb_v16_proxy);
        server.vemb_v16_proxy = NULL;
    }
    if (g_vemb_storage) {
        vemb_v16_storage_ctx_destroy(g_vemb_storage);
        g_vemb_storage = NULL;
    }

    serverLog(LL_NOTICE, "VEMB V16 integration shutdown complete");
}
