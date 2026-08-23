#define _GNU_SOURCE

/*
 * Deprecated compatibility harness. Keep it for focused protocol smoke tests;
 * all VEMB performance scripts must use memtier against redis-server.
 */

#include "../src/vemb_v16_client_topology.h"
#include "../src/vemb_v16_aeron_attach.h"
#include "../src/vemb_v16_client_ring.h"
#include "../src/vemb_v16_net.h"
#include "../src/vemb_v16_protocol.h"
#include "../clients/c/vemb_v16_client_sdk.h"
#include "../clients/c/vemb_v16_ub_peer_view.h"
#include "../src/zmalloc.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define VEMB_V16_BENCH_MAX_NODES 16
#define VEMB_V16_BENCH_HASH_VNODES 10
#define VEMB_V16_BENCH_PATH_MAX 256

typedef struct bench_hash_node {
    uint64_t hash_value;
    uint32_t node_index;
} bench_hash_node_t;

typedef struct bench_cfg {
    const char *tcp_host;
    char tcp_hosts[VEMB_V16_BENCH_MAX_NODES][VEMB_V16_BENCH_PATH_MAX];
    uint16_t tcp_ports[VEMB_V16_BENCH_MAX_NODES];
    uint32_t node_count;
    bench_hash_node_t hash_nodes[
        VEMB_V16_BENCH_MAX_NODES * VEMB_V16_BENCH_HASH_VNODES];
    uint32_t hash_node_count;
    uint32_t dim;
    uint32_t prefill;
    uint32_t keyspace;
    uint32_t ops;
    int threads;
    const char *threads_arg;
    int mode;
    int hot_key_enabled;
    uint32_t hot_key_id;
    int random_key_pattern;
    uint32_t timeout_ms;
    int pin_threads;
    uint32_t pipeline;
    uint32_t transport_type;
    uint16_t tcp_port;
    int vsim_key2_owner;
    int client_topology_enabled;
    int client_topology_valid;
    vemb_v16_client_topology_t client_topology;
    const char *ub_peer_view_manifest_path;
    const char *ub_peer_view_client_host;
    uint32_t ub_peer_view_owner_id;
    int ub_peer_view_ready;
    vemb_v16_ub_peer_view_manifest_t ub_peer_view_manifest;
} bench_cfg_t;

typedef struct bench_region_map {
    uint32_t region_id;
    uint32_t backend_type;
    uint32_t value_size;
    uint64_t region_bytes;
    uint64_t mmap_offset;
    uint64_t mmap_aligned_offset;
    uint64_t mapping_bytes;
    uint8_t *mapping_addr;
    uint8_t *mapped_addr;
} bench_region_map_t;

typedef struct bench_node_channel {
    vemb_v16_channel_desc_t desc;
    vemb_v16_client_ring_t *req_ring;
    void *req_ring_mapping;
    size_t req_ring_mapping_bytes;
    vemb_v16_client_ring_t *resp_ring;
    void *resp_ring_mapping;
    size_t resp_ring_mapping_bytes;
    int net_fd;
    uint32_t transport_type;
    const char *tcp_host;
    uint16_t tcp_port;
    uint32_t timeout_ms;
    bench_region_map_t warm_region;
    uint32_t warm_region_count;
    bench_region_map_t warm_regions[VEMB_V16_MAX_DESC_WARM_REGIONS];
    vemb_v16_ub_peer_view_mapping_t request_peer_view;
    vemb_v16_ub_peer_view_mapping_t response_peer_view;
    vemb_v16_ub_peer_view_mapping_t warm_peer_view;
} bench_node_channel_t;

typedef struct worker_arg {
    int tid;
    bench_cfg_t cfg;
    uint32_t node_count;
    bench_node_channel_t nodes[VEMB_V16_BENCH_MAX_NODES];
    uint64_t ok;
    uint64_t fail;
    uint64_t read_bytes;
    uint64_t vemb_sent;
    uint64_t vadd_sent;
    uint64_t vsim_sent;
    uint64_t dual_write_sent;
    uint64_t stale_topology_refreshes;
    uint64_t ask_redirects;
    uint64_t moved_redirects;
    uint64_t topology_refresh_calls;
    uint64_t request_publish_spins;
    uint64_t response_empty_polls;
    double score_sum;
    uint64_t ns;
    atomic_int stop;
    atomic_int done;
} worker_arg_t;

enum {
    MODE_PING = 0,
    MODE_VEMB_HANDLE = 1,
    MODE_VADD = 2,
    MODE_MIXED_80R20W = 3,
    MODE_VEMB_INLINE = 4,
    MODE_VSIM_INLINE = 5,
    MODE_VSIM_KEY_KEY = 6,
    MODE_VREM = 7,
};

enum {
    VSIM_KEY2_OWNER_SAME = 0,
    VSIM_KEY2_OWNER_REMOTE = 1,
};

typedef struct pending_req {
    uint32_t op_index;
    uint32_t req_id;
    uint32_t key_id;
    uint32_t node_index;
    uint8_t op;
    int expect_inline_vector;
} pending_req_t;

static const char *mode_name(int mode);
static void close_node_channel(bench_node_channel_t *node);
static int setup_node_channel(const bench_cfg_t *cfg,
                              uint32_t node_index,
                              int open_region,
                              bench_node_channel_t *node);
static void print_stats_delta_node(uint32_t node_index,
                                   const vemb_v16_stats_t *before,
                                   const vemb_v16_stats_t *after);
static int fetch_stats_for_node(const bench_cfg_t *cfg,
                                uint32_t node_index,
                                vemb_v16_stats_t *stats);

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int wait_timed_out(uint64_t start_ns, uint32_t timeout_ms) {
    if (timeout_ms == 0) return 0;
    return now_ns() - start_ns >= (uint64_t)timeout_ms * 1000000ULL;
}

static int read_full(int fd, void *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = read(fd, (char *)buf + done, n - done);
        if (r <= 0) return -1;
        done += (size_t)r;
    }
    return 0;
}

static int write_full(int fd, const void *buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = write(fd, (const char *)buf + done, n - done);
        if (r <= 0) return -1;
        done += (size_t)r;
    }
    return 0;
}

static const char *tcp_host_for_node(const bench_cfg_t *cfg,
                                     uint32_t node_index);
static uint16_t tcp_port_for_node(const bench_cfg_t *cfg,
                                  uint32_t node_index);

static int aeron_attach_client_exchange(
    int fd,
    const vemb_v16_aeron_attach_req_t *req,
    vemb_v16_aeron_attach_resp_t *resp) {
    if (write_full(fd, req, sizeof(*req)) != 0 ||
        read_full(fd, resp, sizeof(*resp)) != 0 ||
        memcmp(resp->magic,
               VEMB_V16_AERON_ATTACHED_MAGIC,
               VEMB_V16_AERON_ATTACHED_MAGIC_LEN) != 0 ||
        resp->status != 0) {
        return -1;
    }
    return 0;
}

static int alloc_aeron_channel_tcp(const bench_cfg_t *cfg,
                                   uint32_t node_index,
                                   int remote_path,
                                   vemb_v16_channel_desc_t *desc,
                                   vemb_v16_ub_peer_view_mapping_t *request_view,
                                   vemb_v16_ub_peer_view_mapping_t *response_view,
                                   vemb_v16_ub_peer_view_mapping_t *warm_view) {
    int fd = vemb_v16_net_connect(tcp_host_for_node(cfg, node_index),
                                  tcp_port_for_node(cfg, node_index),
                                  cfg->timeout_ms);
    if (fd < 0)
        return -1;

    vemb_v16_aeron_attach_req_t req;
    memset(&req, 0, sizeof(req));
    memcpy(req.magic,
           VEMB_V16_AERON_ATTACH_MAGIC,
           VEMB_V16_AERON_ATTACH_MAGIC_LEN);
    req.dim = cfg->dim;
    req.flags = remote_path ? VEMB_V16_AERON_ATTACH_F_REMOTE_PATH : 0;
    vemb_v16_aeron_attach_resp_t resp;
    if (aeron_attach_client_exchange(fd, &req, &resp) != 0) {
        close(fd);
        return -1;
    }
    close(fd);

    if (resp.request_shmdev_path_len == 0 ||
        resp.request_shmdev_path_len > VEMB_V16_AERON_SHMDEV_PATH_MAX ||
        resp.response_shmdev_path_len == 0 ||
        resp.response_shmdev_path_len > VEMB_V16_AERON_SHMDEV_PATH_MAX ||
        resp.req_slot_size == 0 ||
        resp.resp_slot_size == 0 ||
        resp.ring_size_slots != VEMB_V16_CLIENT_RING_SIZE ||
        resp.request_shmdev_path[0] == '\0' ||
        resp.response_shmdev_path[0] == '\0') {
        return -1;
    }

    char client_request_shmdev_path[VEMB_V16_AERON_SHMDEV_PATH_MAX];
    char client_response_shmdev_path[VEMB_V16_AERON_SHMDEV_PATH_MAX];
    if (remote_path) {
        if (!cfg->ub_peer_view_ready ||
            vemb_v16_ub_peer_view_manifest_resolve(
                &cfg->ub_peer_view_manifest, cfg->ub_peer_view_client_host,
                cfg->ub_peer_view_owner_id,
                VEMB_V16_UB_PEER_VIEW_V1_REQUEST_RING,
                resp.request_shmdev_path, 0, request_view) != 0 ||
            vemb_v16_ub_peer_view_manifest_resolve(
                &cfg->ub_peer_view_manifest, cfg->ub_peer_view_client_host,
                cfg->ub_peer_view_owner_id,
                VEMB_V16_UB_PEER_VIEW_V1_RESPONSE_RING,
                resp.response_shmdev_path, 0, response_view) != 0)
            return -1;
        memcpy(client_request_shmdev_path, request_view->client_path,
               sizeof(client_request_shmdev_path));
        memcpy(client_response_shmdev_path, response_view->client_path,
               sizeof(client_response_shmdev_path));
    } else {
        strncpy(client_request_shmdev_path, resp.request_shmdev_path,
                sizeof(client_request_shmdev_path) - 1);
        client_request_shmdev_path[sizeof(client_request_shmdev_path) - 1] = '\0';
        strncpy(client_response_shmdev_path, resp.response_shmdev_path,
                sizeof(client_response_shmdev_path) - 1);
        client_response_shmdev_path[sizeof(client_response_shmdev_path) - 1] = '\0';
    }

    memset(desc, 0, sizeof(*desc));
    desc->magic = VEMB_V16_MAGIC;
    desc->version = VEMB_V16_VERSION;
    desc->channel_id = resp.channel_id;
    desc->vector_dim = cfg->dim;
    desc->vector_stride = cfg->dim * sizeof(float);
    desc->request_ring_slot_size = resp.req_slot_size;
    desc->response_ring_slot_size = resp.resp_slot_size;
    snprintf(desc->request_ring_name,
             sizeof(desc->request_ring_name),
             "%s@off%llu",
             client_request_shmdev_path,
             (unsigned long long)resp.req_ring_off);
    snprintf(desc->response_ring_name,
             sizeof(desc->response_ring_name),
             "%s@off%llu",
             client_response_shmdev_path,
             (unsigned long long)resp.resp_ring_off);
    if (resp.warm_region_count > 1 ||
        (resp.warm_region_count != 0 &&
         (resp.warm_path_len == 0 ||
          resp.warm_path_len > VEMB_V16_AERON_SHMDEV_PATH_MAX))) {
        return -1;
    }
    char client_warm_path[VEMB_V16_AERON_SHMDEV_PATH_MAX];
    int warm_path_ok = resp.warm_region_count == 1 &&
        resp.warm_path[0] != '\0';
    if (warm_path_ok && remote_path &&
        resp.warm_backend_type == VEMB_V16_REGION_UB) {
        warm_path_ok = cfg->ub_peer_view_ready &&
            vemb_v16_ub_peer_view_manifest_resolve(
                &cfg->ub_peer_view_manifest, cfg->ub_peer_view_client_host,
                cfg->ub_peer_view_owner_id, VEMB_V16_UB_PEER_VIEW_WARM_REGION,
                resp.warm_path, 0, warm_view) == 0;
        if (warm_path_ok)
            memcpy(client_warm_path, warm_view->client_path,
                   sizeof(client_warm_path));
    } else if (warm_path_ok) {
        strncpy(client_warm_path, resp.warm_path,
                sizeof(client_warm_path) - 1);
        client_warm_path[sizeof(client_warm_path) - 1] = '\0';
    }
    if (warm_path_ok) {
        desc->warm_region_count = 1;
        desc->warm_regions[0].region_id = resp.warm_region_id;
        desc->warm_regions[0].backend_type = resp.warm_backend_type;
        desc->warm_regions[0].region_bytes = resp.warm_region_bytes;
        desc->warm_regions[0].mmap_offset = resp.warm_mmap_offset;
        strncpy(desc->warm_regions[0].path,
                client_warm_path,
                sizeof(desc->warm_regions[0].path) - 1);
    }
    return 0;
}

static const char *tcp_host_for_node(const bench_cfg_t *cfg,
                                     uint32_t node_index) {
    if (node_index < VEMB_V16_BENCH_MAX_NODES &&
        cfg->tcp_hosts[node_index][0]) {
        return cfg->tcp_hosts[node_index];
    }
    return cfg->tcp_host;
}

static uint16_t tcp_port_for_node(const bench_cfg_t *cfg,
                                  uint32_t node_index) {
    if (node_index < VEMB_V16_BENCH_MAX_NODES &&
        cfg->tcp_ports[node_index] != 0) {
        return cfg->tcp_ports[node_index];
    }
    return cfg->tcp_port;
}

static int alloc_tcp_channel(const bench_cfg_t *cfg,
                             uint32_t node_index,
                             vemb_v16_channel_desc_t *desc,
                             int *net_fd) {
    int fd = vemb_v16_net_connect(tcp_host_for_node(cfg, node_index),
                                  tcp_port_for_node(cfg, node_index),
                                  cfg->timeout_ms);
    if (fd < 0) return -1;
    vemb_v16_alloc_req_t req = {.vector_dim = cfg->dim};
    uint8_t req_buf[8];
    const void *payload = &req;
    uint32_t payload_len = (uint32_t)sizeof(req);
    uint32_t net_flags = 0;
    size_t encoded_len = 0;
    if (vemb_v16_alloc_req_encode(req_buf,
                                  sizeof(req_buf),
                                  &req,
                                  &encoded_len) != 0) {
        close(fd);
        return -1;
    }
    payload = req_buf;
    payload_len = (uint32_t)encoded_len;
    if (vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_HELLO,
                                 net_flags,
                                 0,
                                 0,
                                 payload,
                                 payload_len) != 0) {
        close(fd);
        return -1;
    }

    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_WELCOME) {
        close(fd);
        return -1;
    }
    uint8_t *desc_buf = zmalloc(hdr.payload_len);
    if (!desc_buf ||
        hdr.flags != 0 ||
        vemb_v16_net_read_full(fd, desc_buf, hdr.payload_len) != 0 ||
        vemb_v16_channel_desc_decode(desc, desc_buf, hdr.payload_len) != 0) {
        zfree(desc_buf);
        close(fd);
        return -1;
    }
        zfree(desc_buf);
    if (desc->magic != VEMB_V16_MAGIC || desc->version != VEMB_V16_VERSION) {
        close(fd);
        return -1;
    }
    *net_fd = fd;
    return 0;
}

static int tcp_control_request(const char *host,
                               uint16_t port,
                               uint32_t timeout_ms,
                               uint16_t type,
                               uint64_t channel_id,
                               uint16_t expect_type,
                               void *payload,
                               uint32_t payload_len) {
    int fd = vemb_v16_net_connect(host, port, timeout_ms);
    if (fd < 0) return -1;
    if (vemb_v16_net_write_frame(fd,
                                 type,
                                 0,
                                 channel_id,
                                 0,
                                 NULL,
                                 0) != 0) {
        close(fd);
        return -1;
    }
    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != expect_type ||
        hdr.payload_len != payload_len ||
        (payload_len &&
         vemb_v16_net_read_full(fd, payload, payload_len) != 0)) {
        close(fd);
        return -1;
    }
    close(fd);
    return 0;
}

static int fetch_stats_tcp(const bench_cfg_t *cfg,
                           uint32_t node_index,
                           vemb_v16_stats_t *stats) {
    return tcp_control_request(tcp_host_for_node(cfg, node_index),
                               tcp_port_for_node(cfg, node_index),
                               cfg->timeout_ms,
                               VEMB_V16_NET_STATS,
                               0,
                               VEMB_V16_NET_STATS,
                               stats,
                               sizeof(*stats));
}

static int close_channel_tcp(const char *host,
                             uint16_t port,
                             uint32_t timeout_ms,
                             uint64_t channel_id) {
    vemb_v16_net_status_t status;
    if (tcp_control_request(host,
                            port,
                            timeout_ms,
                            VEMB_V16_NET_CLOSE_CHANNEL,
                            channel_id,
                            VEMB_V16_NET_CONTROL_STATUS,
                            &status,
                            sizeof(status)) != 0) {
        return -1;
    }
    return status.status == VEMB_V16_STATUS_OK ? 0 : -1;
}

static int close_all_channels_tcp(const bench_cfg_t *cfg,
                                  uint32_t node_index,
                                  uint64_t *closed) {
    vemb_v16_net_status_t status;
    if (tcp_control_request(tcp_host_for_node(cfg, node_index),
                            tcp_port_for_node(cfg, node_index),
                            cfg->timeout_ms,
                            VEMB_V16_NET_CLOSE_ALL_CHANNELS,
                            0,
                            VEMB_V16_NET_CONTROL_STATUS,
                            &status,
                            sizeof(status)) != 0) {
        return -1;
    }
    if (status.status != VEMB_V16_STATUS_OK)
        return -1;
    if (closed) *closed = status.value;
    return 0;
}

static int client_topology_owners_fit_nodes(const bench_cfg_t *cfg) {
    const vemb_v16_client_topology_t *topology = &cfg->client_topology;
    for (uint32_t i = 0; i < topology->active_ring.owner_count; i++) {
        if (topology->active_ring.owners[i] >= cfg->node_count)
            return 0;
    }
    for (uint32_t i = 0; i < topology->standby_ring.owner_count; i++) {
        if (topology->standby_ring.owners[i] >= cfg->node_count)
            return 0;
    }
    return 1;
}

static int apply_client_topology_endpoints(bench_cfg_t *cfg) {
    if (!cfg)
        return -1;
    const vemb_v16_client_topology_t *topology = &cfg->client_topology;
    for (uint32_t i = 0; i < topology->endpoint_count; i++) {
        const vemb_v16_topology_endpoint_t *endpoint =
            &topology->endpoints[i];
        uint32_t owner = endpoint->owner_id;
        if (owner >= VEMB_V16_BENCH_MAX_NODES)
            return -1;
        if ((cfg->transport_type == VEMB_V16_TRANSPORT_TCP &&
             endpoint->transport_type != VEMB_V16_TRANSPORT_TCP) ||
            (cfg->transport_type == VEMB_V16_TRANSPORT_AERON &&
             endpoint->transport_type != VEMB_V16_TRANSPORT_AERON) ||
            endpoint->host[0] == '\0' || endpoint->tcp_port == 0) {
            return -1;
        }
        strncpy(cfg->tcp_hosts[owner], endpoint->host,
                sizeof(cfg->tcp_hosts[owner]) - 1);
        cfg->tcp_hosts[owner][sizeof(cfg->tcp_hosts[owner]) - 1] = '\0';
        cfg->tcp_ports[owner] = endpoint->tcp_port;
        if (owner + 1 > cfg->node_count)
            cfg->node_count = owner + 1;
    }
    if (cfg->node_count > 0) {
        cfg->tcp_host = cfg->tcp_hosts[0];
        cfg->tcp_port = cfg->tcp_ports[0];
    }
    return 0;
}

static int refresh_client_topology(bench_cfg_t *cfg) {
    if (!cfg || !cfg->client_topology_enabled)
        return 0;
    vemb_v16_topology_control_resp_t resp;
    memset(&resp, 0, sizeof(resp));
    int rc = vemb_v16_client_topology_fetch_tcp(
        tcp_host_for_node(cfg, 0), tcp_port_for_node(cfg, 0),
        cfg->timeout_ms, &cfg->client_topology, &resp);
    if (rc != 0 ||
        apply_client_topology_endpoints(cfg) != 0 ||
        !client_topology_owners_fit_nodes(cfg)) {
        cfg->client_topology_valid = 0;
        return -1;
    }
    cfg->client_topology_valid = 1;
    return 0;
}

static int open_ring(const char *name,
                     uint32_t slot_size,
                     vemb_v16_client_ring_t **ring) {
    const char *offset_marker = strstr(name, "@off");
    if (!offset_marker)
        return -1;
    uint64_t mmap_offset = 0;
    char path[VEMB_V16_BENCH_PATH_MAX];
    if (offset_marker) {
        size_t path_len = (size_t)(offset_marker - name);
        if (path_len == 0 || path_len >= sizeof(path))
            return -1;
        memcpy(path, name, path_len);
        path[path_len] = '\0';
        char *end = NULL;
        mmap_offset = strtoull(offset_marker + 4, &end, 10);
        if (end == offset_marker + 4 || *end != '\0')
            return -1;
    }
    int fd = open(path, O_RDWR);
    if (fd < 0) return -1;
    size_t bytes = vemb_v16_client_ring_bytes(slot_size);
    long page_size = sysconf(_SC_PAGESIZE);
    uint64_t page_mask = (uint64_t)(page_size > 0 ? page_size : 4096) - 1u;
    uint64_t aligned_offset = mmap_offset & ~page_mask;
    size_t offset_delta = (size_t)(mmap_offset - aligned_offset);
    size_t map_size = bytes + offset_delta;
    void *base = mmap(NULL, map_size, PROT_READ | PROT_WRITE,
                      MAP_SHARED, fd, (off_t)aligned_offset);
    close(fd);
    if (base == MAP_FAILED) return -1;
    *ring = (vemb_v16_client_ring_t *)((uint8_t *)base + offset_delta);
    return 0;
}

static int open_peer_view_ring(
    const char *name, uint32_t slot_size,
    const vemb_v16_ub_peer_view_mapping_t *peer_view,
    void **out_mapping, size_t *out_mapping_bytes,
    vemb_v16_client_ring_t **ring) {
    const char *offset_marker = strstr(name, "@off");
    if (!offset_marker || !peer_view || !peer_view->client_path[0] ||
        !out_mapping || !out_mapping_bytes || !ring)
        return -1;
    char *end = NULL;
    uint64_t offset = strtoull(offset_marker + 4, &end, 10);
    if (end == offset_marker + 4 || *end != '\0')
        return -1;
    size_t bytes = vemb_v16_client_ring_bytes(slot_size);
    int map_from_start = peer_view->map_flags &
        VEMB_V16_UB_PEER_VIEW_MAP_F_FROM_START;
    if (map_from_start &&
        (offset > SIZE_MAX || bytes > SIZE_MAX - (size_t)offset))
        return -1;
    uint64_t mapping_offset = map_from_start ? 0 : offset;
    size_t mapping_bytes = bytes + (map_from_start ? (size_t)offset : 0);
    int flags = O_RDWR;
    if (peer_view->cache_policy == VEMB_V16_UB_PEER_VIEW_NONCACHEABLE)
        flags |= O_SYNC;
    int fd = open(peer_view->client_path, flags);
    if (fd < 0)
        return -1;
    long page_size = sysconf(_SC_PAGESIZE);
    uint64_t page_mask = (uint64_t)(page_size > 0 ? page_size : 4096) - 1u;
    uint64_t aligned_offset = mapping_offset & ~page_mask;
    size_t offset_delta = (size_t)(mapping_offset - aligned_offset);
    void *base = mmap(NULL, mapping_bytes + offset_delta,
                      PROT_READ | PROT_WRITE, MAP_SHARED, fd,
                      (off_t)aligned_offset);
    close(fd);
    if (base == MAP_FAILED)
        return -1;
    *out_mapping = base;
    *out_mapping_bytes = mapping_bytes + offset_delta;
    *ring = (vemb_v16_client_ring_t *)((uint8_t *)base + offset_delta +
        (map_from_start ? (size_t)offset : 0));
    return 0;
}

static int aeron_endpoint_is_local(const char *host) {
    return host && (!strcmp(host, "127.0.0.1") ||
                    !strcmp(host, "localhost") ||
                    !strcmp(host, "::1"));
}

static int open_warm_region_desc(uint32_t region_id,
                                 uint32_t backend_type,
                                 uint64_t region_bytes,
                                 uint64_t mmap_offset,
                                 uint32_t value_size,
                                 uint32_t max_vectors,
                                 const char *path,
                                 uint32_t map_flags,
                                 uint32_t cache_policy,
                                 bench_region_map_t *region) {
    if (!path || !region)
        return -1;
    int fd = -1;
    if (backend_type == VEMB_V16_REGION_LOCAL_SHM) {
        fd = shm_open(path, O_RDWR, 0666);
    } else if (backend_type == VEMB_V16_REGION_UB) {
        int flags = O_RDWR;
        if (cache_policy == VEMB_V16_UB_PEER_VIEW_NONCACHEABLE)
            flags |= O_SYNC;
        fd = open(path, flags);
    } else {
        return -1;
    }
    if (fd < 0) return -1;
    size_t size = region_bytes ?
        (size_t)region_bytes :
        (size_t)value_size * max_vectors;
    long page_size = sysconf(_SC_PAGESIZE);
    uint64_t page_mask = (uint64_t)(page_size > 0 ? page_size : 4096) - 1u;
    int map_from_start = map_flags & VEMB_V16_UB_PEER_VIEW_MAP_F_FROM_START;
    if (map_from_start &&
        (mmap_offset > SIZE_MAX || size > SIZE_MAX - (size_t)mmap_offset)) {
        close(fd);
        return -1;
    }
    uint64_t mapping_offset = map_from_start ? 0 : mmap_offset;
    uint64_t aligned_offset = mapping_offset & ~page_mask;
    size_t offset_delta = (size_t)(mapping_offset - aligned_offset);
    size_t map_size = size + offset_delta +
        (map_from_start ? (size_t)mmap_offset : 0);
    void *ptr = mmap(NULL, map_size, PROT_READ, MAP_SHARED, fd,
                     (off_t)aligned_offset);
    close(fd);
    if (ptr == MAP_FAILED) return -1;
    region->region_id = region_id;
    region->backend_type = backend_type;
    region->value_size = value_size;
    region->region_bytes = size;
    region->mmap_offset = mmap_offset;
    region->mmap_aligned_offset = aligned_offset;
    region->mapping_bytes = map_size;
    region->mapping_addr = ptr;
    region->mapped_addr = (uint8_t *)ptr + offset_delta +
        (map_from_start ? (size_t)mmap_offset : 0);
    return 0;
}

static int open_warm_region(const vemb_v16_channel_desc_t *desc,
                            bench_region_map_t *region) {
    if (!desc || !region)
        return -1;
    return open_warm_region_desc(desc->warm_region_id,
                                 desc->warm_backend_type,
                                 desc->warm_region_bytes,
                                 desc->warm_mmap_offset,
                                 desc->vector_stride,
                                 desc->max_vectors,
                                 desc->vector_region_name,
                                 0,
                                 VEMB_V16_UB_PEER_VIEW_CACHEABLE,
                                 region);
}

static int open_warm_regions(const vemb_v16_channel_desc_t *desc,
                             bench_node_channel_t *node) {
    uint32_t count = desc->warm_region_count;
    if (count == 0)
        count = 1;
    if (count > VEMB_V16_MAX_DESC_WARM_REGIONS)
        return -1;
    for (uint32_t i = 0; i < count; i++) {
        int rc;
        if (desc->warm_region_count == 0) {
            rc = open_warm_region(desc, &node->warm_regions[i]);
        } else {
            rc = open_warm_region_desc(desc->warm_regions[i].region_id,
                                       desc->warm_regions[i].backend_type,
                                       desc->warm_regions[i].region_bytes,
                                       desc->warm_regions[i].mmap_offset,
                                       desc->vector_stride,
                                       desc->max_vectors,
                                       desc->warm_regions[i].path,
                                       node->warm_peer_view.map_flags,
                                       node->warm_peer_view.cache_policy,
                                       &node->warm_regions[i]);
        }
        if (rc != 0)
            return -1;
    }
    node->warm_region_count = count;
    node->warm_region = node->warm_regions[0];
    return 0;
}

static void close_warm_region(bench_region_map_t *region) {
    if (!region || !region->mapping_addr) return;
    munmap(region->mapping_addr, (size_t)region->mapping_bytes);
    memset(region, 0, sizeof(*region));
}

static void close_warm_regions(bench_node_channel_t *node) {
    if (!node)
        return;
    for (uint32_t i = 0; i < node->warm_region_count; i++)
        close_warm_region(&node->warm_regions[i]);
    memset(&node->warm_region, 0, sizeof(node->warm_region));
    node->warm_region_count = 0;
}

static void fill_vector(float *vector, uint32_t dim, uint32_t seed) {
    for (uint32_t i = 0; i < dim; i++)
        vector[i] = (float)((seed + i) & 1023) / 1024.0f;
}

static void make_key(char *buf, size_t len, uint32_t id) {
    snprintf(buf, len, "item:%u", id);
}

static int hash_node_cmp(const void *a, const void *b) {
    const bench_hash_node_t *ha = a;
    const bench_hash_node_t *hb = b;
    if (ha->hash_value < hb->hash_value) return -1;
    if (ha->hash_value > hb->hash_value) return 1;
    if (ha->node_index < hb->node_index) return -1;
    if (ha->node_index > hb->node_index) return 1;
    return 0;
}

static int build_hash_ring(bench_cfg_t *cfg) {
    if (!cfg || cfg->node_count == 0 ||
        cfg->node_count > VEMB_V16_BENCH_MAX_NODES) {
        return -1;
    }
    cfg->hash_node_count = 0;
    for (uint32_t node = 0; node < cfg->node_count; node++) {
        for (uint32_t vnode = 0; vnode < VEMB_V16_BENCH_HASH_VNODES; vnode++) {
            char vnode_key[64];
            uint32_t vnode_id = cfg->hash_node_count;
            snprintf(vnode_key, sizeof(vnode_key),
                     "supernode_%u_vnode_%u", node, vnode_id);
            cfg->hash_nodes[cfg->hash_node_count++] = (bench_hash_node_t){
                .hash_value = vemb_v16_xxh3_64_str(vnode_key,
                                                   strlen(vnode_key)),
                .node_index = node,
            };
        }
    }
    qsort(cfg->hash_nodes,
          cfg->hash_node_count,
          sizeof(cfg->hash_nodes[0]),
          hash_node_cmp);
    return 0;
}

static uint32_t route_hash(const bench_cfg_t *cfg, uint64_t hash) {
    if (cfg && cfg->client_topology_valid) {
        uint32_t owner =
            vemb_v16_topology_ring_owner(&cfg->client_topology.active_ring,
                                         hash);
        return owner == UINT32_MAX ? 0 : owner;
    }
    if (!cfg || cfg->node_count <= 1 || cfg->hash_node_count == 0)
        return 0;
    uint32_t left = 0;
    uint32_t right = cfg->hash_node_count;
    while (left < right) {
        uint32_t mid = left + (right - left) / 2;
        if (cfg->hash_nodes[mid].hash_value < hash)
            left = mid + 1;
        else
            right = mid;
    }
    if (left >= cfg->hash_node_count) left = 0;
    return cfg->hash_nodes[left].node_index;
}

static uint32_t route_key(const bench_cfg_t *cfg, const char *key) {
    return route_hash(cfg, vemb_v16_xxh3_64_str(key, strlen(key)));
}

static uint32_t workload_keyspace(const bench_cfg_t *cfg) {
    if (cfg && cfg->keyspace)
        return cfg->keyspace;
    return cfg ? cfg->prefill : 0;
}

static const char *workload_key_pattern(const bench_cfg_t *cfg) {
    return cfg->random_key_pattern ? "random" : "sequential";
}

static uint32_t workload_key_id(const bench_cfg_t *cfg, uint32_t global_id) {
    uint32_t keyspace = workload_keyspace(cfg);
    if (!cfg->random_key_pattern)
        return keyspace ? global_id % keyspace : global_id;

    uint64_t mixed = (uint64_t)global_id + 0x9e3779b97f4a7c15ULL;
    mixed = (mixed ^ (mixed >> 30)) * 0xbf58476d1ce4e5b9ULL;
    mixed = (mixed ^ (mixed >> 27)) * 0x94d049bb133111ebULL;
    mixed ^= mixed >> 31;
    uint32_t key_id = (uint32_t)(mixed ^ (mixed >> 32));
    return keyspace ? key_id % keyspace : key_id;
}

static uint32_t choose_vsim_key2_id(const bench_cfg_t *cfg,
                                    uint32_t key_id,
                                    uint32_t key1_node_index) {
    uint32_t keyspace = workload_keyspace(cfg);
    if (!cfg || keyspace <= 1)
        return key_id;

    char key2[VEMB_V16_MAX_KEY_LEN];
    for (uint32_t step = 1; step < keyspace; step++) {
        uint32_t candidate = (key_id + step) % keyspace;
        make_key(key2, sizeof(key2), candidate);
        uint32_t key2_node_index = route_key(cfg, key2);
        if (cfg->vsim_key2_owner == VSIM_KEY2_OWNER_REMOTE) {
            if (key2_node_index != key1_node_index)
                return candidate;
        } else if (key2_node_index == key1_node_index) {
            return candidate;
        }
    }
    return key_id;
}

static bench_region_map_t *find_warm_region(worker_arg_t *w,
                                             uint32_t region_id) {
    for (uint32_t i = 0; i < w->node_count; i++) {
        for (uint32_t r = 0; r < w->nodes[i].warm_region_count; r++) {
            if (w->nodes[i].warm_regions[r].mapped_addr &&
                w->nodes[i].warm_regions[r].region_id == region_id) {
                return &w->nodes[i].warm_regions[r];
            }
        }
    }
    return NULL;
}

static void prepare_req(vemb_v16_req_t *req,
                        uint8_t op,
                        uint32_t req_id,
                        uint64_t channel_id,
                        const char *key,
                        uint32_t dim) {
    memset(req, 0, sizeof(*req));
    req->op = op;
    req->req_id = req_id;
    req->channel_id = channel_id;
    req->key_len = (uint32_t)strlen(key);
    req->key_hash = vemb_v16_xxh3_64_str(key, req->key_len);
    req->dim = dim;
    req->vector_bytes = dim * sizeof(float);
    memcpy(req->key, key, req->key_len);
}

static void prepare_req_key2(vemb_v16_req_t *req, const char *key2) {
    req->key2_len = (uint32_t)strlen(key2);
    req->key2_hash = vemb_v16_xxh3_64_str(key2, req->key2_len);
    memcpy(req->key2, key2, req->key2_len);
}

static int send_req(vemb_v16_client_ring_t *ring, const vemb_v16_req_t *req, size_t len,
                    uint64_t *publish_spins, uint32_t timeout_ms) {
    uint8_t wire[VEMB_V16_AERON_REQ_WIRE_MAX_LEN];
    size_t wire_len = 0;
    if (vemb_v16_req_encode(wire, sizeof(wire), req, &wire_len) != 0)
        return -1;
    uint64_t spins = 0;
    uint64_t start = now_ns();
    (void)len;
    while (vemb_v16_client_publish(ring, wire, (uint32_t)wire_len) != 0) {
        spins++;
        if ((spins & 0xfffu) == 0 && wait_timed_out(start, timeout_ms)) {
            if (publish_spins) *publish_spins += spins;
            return -1;
        }
        __asm__ volatile("" ::: "memory");
    }
    if (publish_spins) *publish_spins += spins;
    return 0;
}

static int recv_resp(vemb_v16_client_ring_t *ring, vemb_v16_resp_t *resp,
                     uint64_t *empty_polls, uint32_t timeout_ms) {
    uint8_t wire[VEMB_V16_AERON_RESP_WIRE_MAX_LEN];
    int got;
    uint64_t polls = 0;
    uint64_t start = now_ns();
    while ((got = vemb_v16_client_poll(ring, wire, sizeof(wire))) <= 0) {
        polls++;
        if ((polls & 0xfffu) == 0 && wait_timed_out(start, timeout_ms)) {
            if (empty_polls) *empty_polls += polls;
            return -1;
        }
        __asm__ volatile("" ::: "memory");
    }
    if (empty_polls) *empty_polls += polls;
    return vemb_v16_resp_decode(resp, wire, (size_t)got);
}

static int send_channel_req(bench_node_channel_t *node,
                            const vemb_v16_req_t *req,
                            size_t len,
                            uint64_t *publish_spins,
                            uint32_t timeout_ms) {
    if (node->transport_type == VEMB_V16_TRANSPORT_TCP) {
        size_t encoded_len = vemb_v16_req_encoded_len(req);
        uint8_t *req_buf = NULL;
        uint32_t net_flags = 0;
        (void)publish_spins;
        (void)timeout_ms;
        (void)len;
        if (node->net_fd < 0) return -1;
        req_buf = zmalloc(encoded_len);
        if (!req_buf ||
            vemb_v16_req_encode(req_buf, encoded_len, req, &encoded_len) != 0) {
            zfree(req_buf);
            return -1;
        }
        int rc = vemb_v16_net_write_frame(node->net_fd,
                                          VEMB_V16_NET_REQUEST,
                                          net_flags,
                                          node->desc.channel_id,
                                          req->req_id,
                                          req_buf,
                                          (uint32_t)encoded_len);
        zfree(req_buf);
        return rc;
    }
    return send_req(node->req_ring, req, len, publish_spins, timeout_ms);
}

static int recv_channel_resp(bench_node_channel_t *node,
                             vemb_v16_resp_t *resp,
                             uint8_t *inline_vector,
                             uint32_t inline_vector_cap,
                             uint32_t *inline_vector_bytes,
                             uint64_t *empty_polls,
                             uint32_t timeout_ms) {
    if (inline_vector_bytes) *inline_vector_bytes = 0;
    if (node->transport_type == VEMB_V16_TRANSPORT_TCP) {
        (void)empty_polls;
        (void)timeout_ms;
        if (node->net_fd < 0) return -1;
        vemb_v16_net_hdr_t hdr;
        if (vemb_v16_net_read_header(node->net_fd, &hdr) != 0) {
            fprintf(stderr,
                    "tcp recv failed: stage=read_header host=%s port=%u channel=%llu\n",
                    node->tcp_host ? node->tcp_host : "(null)",
                    node->tcp_port,
                    (unsigned long long)node->desc.channel_id);
            return -1;
        }
        if (hdr.type != VEMB_V16_NET_RESPONSE ||
            hdr.channel_id != node->desc.channel_id) {
            fprintf(stderr,
                    "tcp recv failed: stage=bad_header host=%s port=%u expected_channel=%llu got_type=%u got_channel=%llu payload_len=%u req_id=%u flags=%u\n",
                    node->tcp_host ? node->tcp_host : "(null)",
                    node->tcp_port,
                    (unsigned long long)node->desc.channel_id,
                    hdr.type,
                    (unsigned long long)hdr.channel_id,
                    hdr.payload_len,
                    hdr.req_id,
                    hdr.flags);
            return -1;
        }
        uint8_t resp_buf[64];
        uint32_t base_bytes = (uint32_t)vemb_v16_resp_encoded_base_len();
        if (hdr.flags != 0 ||
            hdr.payload_len < base_bytes ||
            vemb_v16_net_read_full(node->net_fd, resp_buf, base_bytes) != 0) {
            fprintf(stderr,
                    "tcp recv failed: stage=read_base host=%s port=%u channel=%llu payload_len=%u base_bytes=%u flags=%u\n",
                    node->tcp_host ? node->tcp_host : "(null)",
                    node->tcp_port,
                    (unsigned long long)node->desc.channel_id,
                    hdr.payload_len,
                    base_bytes,
                    hdr.flags);
            return -1;
        }
        size_t resp_bytes = vemb_v16_resp_encoded_len_for_fields(
            resp_buf[0],
            (uint8_t)(resp_buf[1] & VEMB_V16_TCP_RESP_OP_MASK));
        if (resp_bytes > sizeof(resp_buf) ||
            hdr.payload_len < resp_bytes ||
            vemb_v16_net_read_full(node->net_fd,
                                   resp_buf + base_bytes,
                                   resp_bytes - base_bytes) != 0 ||
            vemb_v16_resp_decode(resp, resp_buf, resp_bytes) != 0) {
            fprintf(stderr,
                    "tcp recv failed: stage=decode_resp host=%s port=%u channel=%llu status=%u op=%u payload_len=%u resp_bytes=%zu base_bytes=%u\n",
                    node->tcp_host ? node->tcp_host : "(null)",
                    node->tcp_port,
                    (unsigned long long)node->desc.channel_id,
                    resp_buf[0],
                    (unsigned)(resp_buf[1] & VEMB_V16_TCP_RESP_OP_MASK),
                    hdr.payload_len,
                    resp_bytes,
                    base_bytes);
            return -1;
        }
        uint32_t extra = hdr.payload_len - (uint32_t)resp_bytes;
        if (extra) {
            if (!inline_vector || extra > inline_vector_cap ||
                vemb_v16_net_read_full(node->net_fd, inline_vector, extra) != 0) {
                fprintf(stderr,
                        "tcp recv failed: stage=read_inline host=%s port=%u channel=%llu extra=%u inline_cap=%u resp_op=%u resp_status=%u\n",
                        node->tcp_host ? node->tcp_host : "(null)",
                        node->tcp_port,
                        (unsigned long long)node->desc.channel_id,
                        extra,
                        inline_vector_cap,
                        resp->op,
                        resp->status);
                return -1;
            }
            if (inline_vector_bytes) *inline_vector_bytes = extra;
        }
        return 0;
    }
    (void)inline_vector;
    (void)inline_vector_cap;
    return recv_resp(node->resp_ring, resp, empty_polls, timeout_ms);
}

static int send_write_single_and_wait(bench_node_channel_t *node,
                                      vemb_v16_req_t *req,
                                      size_t req_len,
                                      uint32_t timeout_ms,
                                      vemb_v16_resp_t *resp) {
    req->channel_id = node->desc.channel_id;
    if (send_channel_req(node, req, req_len, NULL, timeout_ms) != 0)
        return -1;
    if (recv_channel_resp(node,
                          resp,
                          NULL,
                          0,
                          NULL,
                          NULL,
                          timeout_ms) != 0) {
        return -1;
    }
    return 0;
}

static int send_read_single_and_wait(bench_node_channel_t *node,
                                     vemb_v16_req_t *req,
                                     size_t req_len,
                                     uint8_t *inline_vector,
                                     uint32_t inline_vector_cap,
                                     uint32_t *inline_vector_bytes,
                                     uint32_t timeout_ms,
                                     vemb_v16_resp_t *resp) {
    req->channel_id = node->desc.channel_id;
    if (send_channel_req(node, req, req_len, NULL, timeout_ms) != 0)
        return -1;
    if (recv_channel_resp(node,
                          resp,
                          inline_vector,
                          inline_vector_cap,
                          inline_vector_bytes,
                          NULL,
                          timeout_ms) != 0) {
        return -1;
    }
    return 0;
}

static int node_channel_open(const bench_node_channel_t *node) {
    if (!node || node->desc.channel_id == 0)
        return 0;
    if (node->transport_type == VEMB_V16_TRANSPORT_TCP)
        return node->net_fd >= 0;
    return node->req_ring != NULL && node->resp_ring != NULL;
}

static int ensure_client_topology_channels(bench_cfg_t *cfg,
                                           bench_node_channel_t *nodes,
                                           uint32_t *node_count,
                                           int open_region) {
    if (!cfg || !nodes || !node_count ||
        cfg->node_count > VEMB_V16_BENCH_MAX_NODES) {
        return -1;
    }
    for (uint32_t n = 0; n < cfg->node_count; n++) {
        if (node_channel_open(&nodes[n]))
            continue;
        if (setup_node_channel(cfg, n, open_region, &nodes[n]) != 0)
            return -1;
        if (n + 1 > *node_count)
            *node_count = n + 1;
    }
    if (*node_count < cfg->node_count)
        *node_count = cfg->node_count;
    return 0;
}

static void close_client_topology_channels(bench_cfg_t *cfg,
                                           bench_node_channel_t *nodes,
                                           uint32_t *node_count) {
    if (!cfg || !nodes || !node_count)
        return;
    for (uint32_t n = 0; n < *node_count && n < cfg->node_count; n++) {
        if (node_channel_open(&nodes[n]))
            close_node_channel(&nodes[n]);
    }
    *node_count = 0;
}

static int reconnect_client_topology_channel(bench_cfg_t *cfg,
                                             bench_node_channel_t *nodes,
                                             uint32_t *node_count,
                                             uint32_t node_index,
                                             int open_region) {
    if (!cfg || !nodes || !node_count || node_index >= cfg->node_count)
        return -1;
    close_node_channel(&nodes[node_index]);
    if (setup_node_channel(cfg, node_index, open_region, &nodes[node_index]) != 0)
        return -1;
    if (node_index + 1 > *node_count)
        *node_count = node_index + 1;
    return 0;
}

static int refresh_client_topology_and_channels(
        bench_cfg_t *cfg,
        bench_node_channel_t *nodes,
        uint32_t *node_count,
        int open_region,
        const char *reason) {
    int had_topology = 0;
    uint64_t old_epoch = 0;
    if (!cfg)
        return -1;
    had_topology = cfg->client_topology_valid;
    if (had_topology)
        old_epoch = cfg->client_topology.current_topology_epoch;
    if (refresh_client_topology(cfg) != 0)
        return -1;
    if (cfg->transport_type == VEMB_V16_TRANSPORT_TCP &&
        nodes &&
        node_count &&
        (*node_count != 0 ||
         (had_topology &&
          old_epoch != cfg->client_topology.current_topology_epoch))) {
        fprintf(stderr,
                "client-topology channel reopen reason=%s old_epoch=%llu new_epoch=%llu open_region=%d\n",
                reason ? reason : "refresh",
                (unsigned long long)old_epoch,
                (unsigned long long)cfg->client_topology.current_topology_epoch,
                open_region);
        close_client_topology_channels(cfg, nodes, node_count);
    }
    if (nodes && node_count) {
        if (ensure_client_topology_channels(cfg,
                                            nodes,
                                            node_count,
                                            open_region) != 0) {
            return -1;
        }
    }
    return 0;
}

static int status_needs_topology_refresh(uint8_t status) {
    return status == VEMB_V16_STATUS_STALE_TOPOLOGY ||
           status == VEMB_V16_STATUS_MOVED ||
           status == VEMB_V16_STATUS_ASK;
}

static int send_write_ask_redirect_once(
        bench_cfg_t *cfg,
        bench_node_channel_t *nodes,
        uint32_t *node_count,
        vemb_v16_req_t *req,
        size_t req_len,
        uint32_t redirect_owner,
        uint64_t topology_epoch,
        vemb_v16_resp_t *resp) {
    if (!cfg || !nodes || !node_count || !req || !resp ||
        redirect_owner == UINT32_MAX ||
        redirect_owner >= VEMB_V16_BENCH_MAX_NODES) {
        return -1;
    }
    if (redirect_owner >= *node_count ||
        !node_channel_open(&nodes[redirect_owner])) {
        if (ensure_client_topology_channels(cfg, nodes, node_count, 0) != 0)
            return -1;
    }
    if (redirect_owner >= *node_count ||
        !node_channel_open(&nodes[redirect_owner])) {
        return -1;
    }

    uint8_t original_flags = req->flags;
    uint64_t original_topology_epoch = req->topology_epoch;
    req->flags = (uint8_t)(req->flags | VEMB_V16_REQ_F_ASK_REDIRECT);
    req->topology_epoch = topology_epoch;
    memset(resp, 0, sizeof(*resp));
    int rc = send_write_single_and_wait(&nodes[redirect_owner],
                                        req,
                                        req_len,
                                        cfg->timeout_ms,
                                        resp);
    req->flags = original_flags;
    req->topology_epoch = original_topology_epoch;
    return rc;
}

static uint8_t send_write_with_client_topology(
        bench_cfg_t *cfg,
        bench_node_channel_t *nodes,
        uint32_t *node_count,
        vemb_v16_req_t *req,
        size_t req_len,
        uint64_t *dual_write_sent,
        uint64_t *stale_topology_refreshes) {
    vemb_v16_client_write_plan_t plan;
    memset(&plan, 0, sizeof(plan));
    if (!cfg->client_topology_valid &&
        refresh_client_topology_and_channels(cfg,
                                             nodes,
                                             node_count,
                                             0,
                                             "write-initial") != 0) {
        return VEMB_V16_STATUS_ERR;
    }
    if (ensure_client_topology_channels(cfg, nodes, node_count, 0) != 0)
        return VEMB_V16_STATUS_ERR;

    for (uint32_t attempt = 0; attempt < 256; attempt++) {
        if (vemb_v16_client_topology_plan_write(&cfg->client_topology,
                                                req->key_hash,
                                                &plan) != 0 ||
            plan.active_owner >= *node_count) {
            return VEMB_V16_STATUS_ERR;
        }
        req->topology_epoch = plan.topology_epoch;
        vemb_v16_resp_t resp;
        memset(&resp, 0, sizeof(resp));
        if (send_write_single_and_wait(&nodes[plan.active_owner],
                                       req,
                                       req_len,
                                       cfg->timeout_ms,
                                       &resp) != 0) {
            fprintf(stderr,
                    "client-topology write send/recv failed req_id=%u key_hash=%llu op=%u owner=%u epoch=%llu attempt=%u, reconnecting\n",
                    req->req_id,
                    (unsigned long long)req->key_hash,
                    req->op,
                    plan.active_owner,
                    (unsigned long long)req->topology_epoch,
                    attempt);
            if (reconnect_client_topology_channel(cfg,
                                                  nodes,
                                                  node_count,
                                                  plan.active_owner,
                                                  0) != 0) {
                fprintf(stderr,
                        "client-topology write reconnect failed req_id=%u key_hash=%llu owner=%u attempt=%u\n",
                        req->req_id,
                        (unsigned long long)req->key_hash,
                        plan.active_owner,
                        attempt);
                return VEMB_V16_STATUS_ERR;
            }
            memset(&resp, 0, sizeof(resp));
            if (send_write_single_and_wait(&nodes[plan.active_owner],
                                           req,
                                           req_len,
                                           cfg->timeout_ms,
                                           &resp) != 0) {
                fprintf(stderr,
                        "client-topology write send/recv failed after reconnect req_id=%u key_hash=%llu op=%u owner=%u epoch=%llu attempt=%u\n",
                        req->req_id,
                        (unsigned long long)req->key_hash,
                        req->op,
                        plan.active_owner,
                        (unsigned long long)req->topology_epoch,
                        attempt);
                return VEMB_V16_STATUS_ERR;
            }
        }
        if (resp.status == VEMB_V16_STATUS_ASK) {
            vemb_v16_resp_t redirect_resp;
            if (send_write_ask_redirect_once(cfg,
                                             nodes,
                                             node_count,
                                             req,
                                             req_len,
                                             resp.redirect_owner,
                                             plan.topology_epoch,
                                             &redirect_resp) == 0) {
                if (redirect_resp.status == VEMB_V16_STATUS_OK) {
                    (void)dual_write_sent;
                    return VEMB_V16_STATUS_OK;
                }
                if (!status_needs_topology_refresh(
                        redirect_resp.status)) {
                    return redirect_resp.status;
                }
            }
            if (stale_topology_refreshes)
                (*stale_topology_refreshes)++;
            if (refresh_client_topology_and_channels(cfg,
                                                     nodes,
                                                     node_count,
                                                     0,
                                                     "write-ask") != 0)
                return resp.status;
            usleep(1000);
            continue;
        }
        if (status_needs_topology_refresh(resp.status)) {
            if (stale_topology_refreshes)
                (*stale_topology_refreshes)++;
            if (refresh_client_topology_and_channels(cfg,
                                                     nodes,
                                                     node_count,
                                                     0,
                                                     "write-status") != 0)
                return resp.status;
            usleep(1000);
            continue;
        }
        if (resp.status != VEMB_V16_STATUS_OK)
            return resp.status;

        (void)dual_write_sent;
        return VEMB_V16_STATUS_OK;
    }
    return VEMB_V16_STATUS_STALE_TOPOLOGY;
}

static uint8_t send_read_with_client_topology(
        bench_cfg_t *cfg,
        bench_node_channel_t *nodes,
        uint32_t *node_count,
        vemb_v16_req_t *req,
        size_t req_len,
        uint8_t *inline_vector,
        uint32_t inline_vector_cap,
        uint32_t *inline_vector_bytes,
        vemb_v16_resp_t *resp,
        uint64_t *stale_topology_refreshes) {
    int open_region = req->op == VEMB_V16_OP_VEMB_HANDLE;
    if (!cfg->client_topology_valid &&
        refresh_client_topology_and_channels(cfg,
                                             nodes,
                                             node_count,
                                             open_region,
                                             "read-initial") != 0) {
        fprintf(stderr,
                "client-topology read refresh failed before send req_id=%u key_hash=%llu op=%u\n",
                req->req_id,
                (unsigned long long)req->key_hash,
                req->op);
        return VEMB_V16_STATUS_ERR;
    }
    if (ensure_client_topology_channels(cfg,
                                        nodes,
                                        node_count,
                                        open_region) != 0) {
        fprintf(stderr,
                "client-topology read ensure channels failed before send req_id=%u key_hash=%llu op=%u open_region=%d\n",
                req->req_id,
                (unsigned long long)req->key_hash,
                req->op,
                open_region);
        return VEMB_V16_STATUS_ERR;
    }

    for (uint32_t attempt = 0; attempt < 256; attempt++) {
        uint32_t active_owner = route_hash(cfg, req->key_hash);
        if (active_owner >= *node_count)
            return VEMB_V16_STATUS_ERR;
        req->topology_epoch = cfg->client_topology.current_topology_epoch;
        memset(resp, 0, sizeof(*resp));
        if (send_read_single_and_wait(&nodes[active_owner],
                                      req,
                                      req_len,
                                      inline_vector,
                                      inline_vector_cap,
                                      inline_vector_bytes,
                                      cfg->timeout_ms,
                                      resp) != 0) {
            fprintf(stderr,
                    "client-topology read send/recv failed req_id=%u key_hash=%llu op=%u owner=%u epoch=%llu attempt=%u, reconnecting\n",
                    req->req_id,
                    (unsigned long long)req->key_hash,
                    req->op,
                    active_owner,
                    (unsigned long long)req->topology_epoch,
                    attempt);
            if (reconnect_client_topology_channel(cfg,
                                                  nodes,
                                                  node_count,
                                                  active_owner,
                                                  open_region) != 0) {
                fprintf(stderr,
                        "client-topology read reconnect failed req_id=%u key_hash=%llu owner=%u attempt=%u\n",
                        req->req_id,
                        (unsigned long long)req->key_hash,
                        active_owner,
                        attempt);
                return VEMB_V16_STATUS_ERR;
            }
            memset(resp, 0, sizeof(*resp));
            if (send_read_single_and_wait(&nodes[active_owner],
                                          req,
                                          req_len,
                                          inline_vector,
                                          inline_vector_cap,
                                          inline_vector_bytes,
                                          cfg->timeout_ms,
                                          resp) != 0) {
                fprintf(stderr,
                        "client-topology read send/recv failed after reconnect req_id=%u key_hash=%llu op=%u owner=%u epoch=%llu attempt=%u\n",
                        req->req_id,
                        (unsigned long long)req->key_hash,
                        req->op,
                        active_owner,
                        (unsigned long long)req->topology_epoch,
                        attempt);
                return VEMB_V16_STATUS_ERR;
            }
        }
        if (resp->status == VEMB_V16_STATUS_OK)
            return VEMB_V16_STATUS_OK;
        if (resp->status == VEMB_V16_STATUS_ASK &&
            resp->redirect_owner != UINT32_MAX &&
            resp->redirect_owner < VEMB_V16_BENCH_MAX_NODES) {
            uint8_t original_flags = req->flags;
            uint64_t original_topology_epoch = req->topology_epoch;
            req->flags = (uint8_t)(req->flags | VEMB_V16_REQ_F_ASK_REDIRECT);
            req->topology_epoch = cfg->client_topology.current_topology_epoch;
            if (resp->redirect_owner >= *node_count ||
                !node_channel_open(&nodes[resp->redirect_owner])) {
                if (ensure_client_topology_channels(cfg,
                                                    nodes,
                                                    node_count,
                                                    open_region) != 0) {
                    fprintf(stderr,
                            "client-topology read ensure channels failed for ask redirect req_id=%u key_hash=%llu redirect_owner=%u attempt=%u\n",
                            req->req_id,
                            (unsigned long long)req->key_hash,
                            resp->redirect_owner,
                            attempt);
                    req->flags = original_flags;
                    req->topology_epoch = original_topology_epoch;
                    return VEMB_V16_STATUS_ERR;
                }
            }
            if (resp->redirect_owner < *node_count &&
                node_channel_open(&nodes[resp->redirect_owner])) {
                vemb_v16_resp_t redirect_resp;
                memset(&redirect_resp, 0, sizeof(redirect_resp));
                if (send_read_single_and_wait(&nodes[resp->redirect_owner],
                                              req,
                                              req_len,
                                              inline_vector,
                                              inline_vector_cap,
                                              inline_vector_bytes,
                                              cfg->timeout_ms,
                                              &redirect_resp) != 0) {
                    fprintf(stderr,
                            "client-topology read ask redirect send/recv failed req_id=%u key_hash=%llu redirect_owner=%u epoch=%llu attempt=%u\n",
                            req->req_id,
                            (unsigned long long)req->key_hash,
                            resp->redirect_owner,
                            (unsigned long long)req->topology_epoch,
                            attempt);
                    req->flags = original_flags;
                    req->topology_epoch = original_topology_epoch;
                    return VEMB_V16_STATUS_ERR;
                }
                req->flags = original_flags;
                req->topology_epoch = original_topology_epoch;
                *resp = redirect_resp;
                if (resp->status == VEMB_V16_STATUS_OK)
                    return VEMB_V16_STATUS_OK;
                if (!status_needs_topology_refresh(resp->status))
                    return resp->status;
            } else {
                fprintf(stderr,
                        "client-topology read redirect owner unavailable req_id=%u key_hash=%llu redirect_owner=%u attempt=%u\n",
                        req->req_id,
                        (unsigned long long)req->key_hash,
                        resp->redirect_owner,
                        attempt);
                req->flags = original_flags;
                req->topology_epoch = original_topology_epoch;
                return VEMB_V16_STATUS_ERR;
            }
        } else if (!status_needs_topology_refresh(resp->status)) {
            return resp->status;
        }
        if (stale_topology_refreshes)
            (*stale_topology_refreshes)++;
        if (refresh_client_topology_and_channels(cfg,
                                                 nodes,
                                                 node_count,
                                                 open_region,
                                                 "read-status") != 0) {
            fprintf(stderr,
                    "client-topology read refresh failed after status=%u req_id=%u key_hash=%llu op=%u owner=%u attempt=%u\n",
                    resp->status,
                    req->req_id,
                    (unsigned long long)req->key_hash,
                    req->op,
                    active_owner,
                    attempt);
            return resp->status;
        }
        usleep(1000);
    }
    return VEMB_V16_STATUS_STALE_TOPOLOGY;
}

static int prefill_multi(bench_cfg_t *cfg,
                         bench_node_channel_t *nodes,
                         uint32_t node_count) {
    vemb_v16_req_t req;
    vemb_v16_resp_t resp;
    char key[VEMB_V16_MAX_KEY_LEN];
    uint64_t start = now_ns();
    uint32_t topology_node_count = node_count;
    for (uint32_t i = 0; i < cfg->prefill; i++) {
        make_key(key, sizeof(key), i);
        uint32_t node_index = route_key(cfg, key);
        if (node_index >= topology_node_count)
            return -1;
        bench_node_channel_t *node = &nodes[node_index];
        prepare_req(&req, VEMB_V16_OP_VADD, i + 1,
                    node->desc.channel_id, key, cfg->dim);
        fill_vector(req.vector, cfg->dim, i);
        size_t req_len = vemb_v16_req_inline_len(req.vector_bytes);
        if (cfg->client_topology_enabled) {
            uint8_t status = send_write_with_client_topology(cfg,
                                                             nodes,
                                                             &topology_node_count,
                                                             &req,
                                                             req_len,
                                                             NULL,
                                                             NULL);
            if (status != VEMB_V16_STATUS_OK) {
                fprintf(stderr,
                        "prefill topology write error at item=%u status=%u\n",
                        i,
                        status);
                return -1;
            }
        } else {
            if (send_channel_req(node, &req, req_len,
                                 NULL, cfg->timeout_ms) != 0) {
                fprintf(stderr, "prefill send timeout at item=%u node=%u\n",
                        i, node_index);
                return -1;
            }
            if (recv_channel_resp(node, &resp, NULL, 0, NULL, NULL, cfg->timeout_ms) != 0 ||
                resp.status != VEMB_V16_STATUS_OK) {
                fprintf(stderr, "prefill response timeout/error at item=%u node=%u status=%u\n",
                        i, node_index, resp.status);
                return -1;
            }
        }
        if ((i + 1) % 10000 == 0)
            printf("[prefill] inserted=%u elapsed=%.3fs\n",
                   i + 1, (double)(now_ns() - start) / 1e9);
    }
    if (cfg->prefill > 0)
        printf("[prefill] inserted=%u elapsed=%.3fs\n",
               cfg->prefill, (double)(now_ns() - start) / 1e9);
    return 0;
}

/* The benchmark supplies workload only. Topology, owner routing, redirect
 * retry and data-channel ownership belong exclusively to the SDK common
 * core. Every configured TCP endpoint is a bootstrap seed, never a data
 * backend selection. */
static vemb_v16_client_t *open_common_core_client(const bench_cfg_t *cfg)
{
    char seed_storage[VEMB_V16_BENCH_MAX_NODES]
                     [VEMB_V16_BENCH_PATH_MAX + 8];
    const char *seeds[VEMB_V16_BENCH_MAX_NODES];

    for (uint32_t i = 0; i < cfg->node_count; i++) {
        int written = snprintf(seed_storage[i], sizeof(seed_storage[i]),
                               "%s:%u", tcp_host_for_node(cfg, i),
                               (unsigned)tcp_port_for_node(cfg, i));
        if (written < 0 || (size_t)written >= sizeof(seed_storage[i]))
            return NULL;
        seeds[i] = seed_storage[i];
    }

    vemb_v16_client_t *client = vemb_v16_client_create(
        seeds, (int)cfg->node_count, cfg->dim, cfg->timeout_ms,
        cfg->transport_type);
    if (!client)
        return NULL;
    if ((cfg->ub_peer_view_manifest_path || cfg->ub_peer_view_client_host) &&
        (!cfg->ub_peer_view_manifest_path ||
         !cfg->ub_peer_view_client_host ||
         vemb_v16_client_configure_ub_peer_view(
             client, cfg->ub_peer_view_manifest_path,
             cfg->ub_peer_view_client_host) != 0)) {
        vemb_v16_client_destroy(client);
        return NULL;
    }
    if (vemb_v16_client_topology_refresh(client) != 0) {
        vemb_v16_client_destroy(client);
        return NULL;
    }
    return client;
}

static int common_core_read_vector(vemb_v16_client_t *client,
                                   const char *key,
                                   float *out_vector,
                                   uint32_t dim,
                                   uint64_t *read_bytes)
{
    uint64_t offset = 0;
    uint32_t bytes = 0;
    uint32_t response_dim = 0;
    int rc = vemb_v16_client_vemb_handle(client, NULL, key,
                                         &offset, &bytes, &response_dim,
                                         NULL);
    if (rc != 0 || response_dim != dim || bytes != dim * sizeof(float) ||
        vemb_v16_client_read_vector(client, offset, bytes,
                                    out_vector, dim) != 0) {
        return -1;
    }
    *read_bytes += bytes;
    return 0;
}

static int common_core_prefill(const bench_cfg_t *cfg)
{
    vemb_v16_client_t *client = open_common_core_client(cfg);
    float *vector = zmalloc((size_t)cfg->dim * sizeof(*vector));
    char key[VEMB_V16_MAX_KEY_LEN];
    uint64_t start = now_ns();

    if (!client || !vector) {
        vemb_v16_client_destroy(client);
        zfree(vector);
        return -1;
    }
    for (uint32_t i = 0; i < cfg->prefill; i++) {
        make_key(key, sizeof(key), i);
        fill_vector(vector, cfg->dim, i);
        if (vemb_v16_client_vadd(client, NULL, key, vector, cfg->dim) != 0) {
            fprintf(stderr, "prefill common-core write failed at item=%u\n", i);
            vemb_v16_client_destroy(client);
            zfree(vector);
            return -1;
        }
        if ((i + 1) % 10000 == 0)
            printf("[prefill] inserted=%u elapsed=%.3fs\n",
                   i + 1, (double)(now_ns() - start) / 1e9);
    }
    if (cfg->prefill > 0)
        printf("[prefill] inserted=%u elapsed=%.3fs\n",
               cfg->prefill, (double)(now_ns() - start) / 1e9);
    vemb_v16_client_destroy(client);
    zfree(vector);
    return 0;
}

static void *common_core_worker_main(void *arg)
{
    worker_arg_t *w = arg;
#ifdef __linux__
    if (w->cfg.pin_threads) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET((w->tid * 2 + 3) % 64, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
    }
#endif

    vemb_v16_client_t *client = open_common_core_client(&w->cfg);
    float *vector = zmalloc((size_t)w->cfg.dim * sizeof(*vector));
    char key[VEMB_V16_MAX_KEY_LEN];
    uint64_t start = now_ns();

    if (!client || !vector) {
        w->fail = w->cfg.ops;
        vemb_v16_client_destroy(client);
        zfree(vector);
        w->ns = now_ns() - start;
        atomic_store_explicit(&w->done, 1, memory_order_release);
        return NULL;
    }

    for (uint32_t i = 0; i < w->cfg.ops &&
                         !atomic_load_explicit(&w->stop,
                                               memory_order_acquire); i++) {
        uint32_t global_id = i + (uint32_t)w->tid * w->cfg.ops;
        uint32_t key_id = workload_key_id(&w->cfg, global_id);
        int mixed_write = w->cfg.mode == MODE_MIXED_80R20W &&
                          (i % 5u) == 0;
        int rc = -1;

        if (w->cfg.hot_key_enabled)
            key_id = w->cfg.hot_key_id;
        if (w->cfg.mode == MODE_PING) {
            rc = vemb_v16_client_ping(client);
        } else if (w->cfg.mode == MODE_VADD || mixed_write) {
            uint32_t write_key_id = mixed_write ? key_id :
                (w->cfg.keyspace ? key_id : global_id + 100000000u);
            make_key(key, sizeof(key), write_key_id);
            fill_vector(vector, w->cfg.dim, global_id);
            rc = vemb_v16_client_vadd(client, NULL, key, vector, w->cfg.dim);
            w->vadd_sent++;
        } else if (w->cfg.mode == MODE_VREM) {
            make_key(key, sizeof(key), key_id);
            rc = vemb_v16_client_vrem(client, NULL, key);
            w->vadd_sent++;
        } else if (w->cfg.mode == MODE_VSIM_INLINE) {
            float score = 0.0f;
            make_key(key, sizeof(key), key_id);
            fill_vector(vector, w->cfg.dim, global_id + 0x9e3779b9u);
            rc = vemb_v16_client_vsim(client, NULL, key, vector,
                                      w->cfg.dim, &score);
            if (rc == 0)
                w->score_sum += score;
            w->vsim_sent++;
        } else if (w->cfg.mode == MODE_VEMB_HANDLE ||
                   w->cfg.mode == MODE_MIXED_80R20W) {
            make_key(key, sizeof(key), key_id);
            rc = common_core_read_vector(client, key, vector, w->cfg.dim,
                                         &w->read_bytes);
            w->vemb_sent++;
        } else if (w->cfg.mode == MODE_VEMB_INLINE) {
            uint32_t response_dim = 0;
            make_key(key, sizeof(key), key_id);
            rc = vemb_v16_client_vemb_vector(client, NULL, key, vector,
                                              w->cfg.dim, &response_dim);
            if (rc == 0 && response_dim == w->cfg.dim)
                w->read_bytes += w->cfg.dim * sizeof(*vector);
            else if (rc == 0)
                rc = -1;
            w->vemb_sent++;
        }

        if (rc == 0)
            w->ok++;
        else
            w->fail++;
    }

    vemb_v16_redirect_stats_t redirect_stats;
    vemb_v16_client_get_redirect_stats(client, &redirect_stats);
    w->ask_redirects = redirect_stats.ask_redirects;
    w->moved_redirects = redirect_stats.moved_redirects;
    w->stale_topology_refreshes = redirect_stats.stale_topology_responses;
    w->topology_refresh_calls = redirect_stats.topology_refresh_calls;
    vemb_v16_client_destroy(client);
    zfree(vector);
    w->ns = now_ns() - start;
    atomic_store_explicit(&w->done, 1, memory_order_release);
    return NULL;
}

static int run_common_core_once(bench_cfg_t cfg)
{
    if (cfg.pipeline != 1) {
        fprintf(stderr, "common-core benchmark currently requires --pipeline 1\n");
        return 1;
    }
    if (cfg.mode == MODE_VSIM_KEY_KEY) {
        fprintf(stderr, "vsim-key-key is not yet available through the common SDK core\n");
        return 1;
    }

    printf("[setup] bootstrap=tcp topology-data=owner-fixed mode=%s dim=%u prefill=%u keyspace=%u key-pattern=%s ops/thread=%u threads=%d pipeline=%u pin=%s\n",
           mode_name(cfg.mode), cfg.dim, cfg.prefill, workload_keyspace(&cfg),
           workload_key_pattern(&cfg), cfg.ops, cfg.threads, cfg.pipeline,
           cfg.pin_threads ? "yes" : "no");
    if (cfg.prefill && cfg.mode != MODE_PING && common_core_prefill(&cfg) != 0) {
        fprintf(stderr, "common-core prefill failed\n");
        return 1;
    }

    vemb_v16_stats_t before[VEMB_V16_BENCH_MAX_NODES] = {{0}};
    vemb_v16_stats_t after[VEMB_V16_BENCH_MAX_NODES] = {{0}};
    for (uint32_t n = 0; n < cfg.node_count; n++) {
        if (fetch_stats_for_node(&cfg, n, &before[n]) != 0)
            fprintf(stderr, "warning: fetch stats before run failed for node=%u\n", n);
    }

    worker_arg_t *args = zcalloc_num((size_t)cfg.threads, sizeof(*args));
    pthread_t *threads = zcalloc_num((size_t)cfg.threads, sizeof(*threads));
    if (!args || !threads) {
        zfree(args);
        zfree(threads);
        return 1;
    }
    for (int i = 0; i < cfg.threads; i++) {
        args[i].tid = i;
        args[i].cfg = cfg;
        atomic_init(&args[i].stop, 0);
        atomic_init(&args[i].done, 0);
    }

    uint64_t start = now_ns();
    printf("[run] common-core mode=%s threads=%d requests=%llu\n",
           mode_name(cfg.mode), cfg.threads,
           (unsigned long long)cfg.ops * (unsigned long long)cfg.threads);
    fflush(stdout);
    for (int i = 0; i < cfg.threads; i++)
        pthread_create(&threads[i], NULL, common_core_worker_main, &args[i]);

    uint64_t join_start = now_ns();
    int timed_out = 0;
    for (;;) {
        int done = 0;
        for (int i = 0; i < cfg.threads; i++)
            done += atomic_load_explicit(&args[i].done, memory_order_acquire);
        if (done == cfg.threads)
            break;
        if (wait_timed_out(join_start, cfg.timeout_ms)) {
            fprintf(stderr, "run timeout: done_workers=%d/%d timeout_ms=%u\n",
                    done, cfg.threads, cfg.timeout_ms);
            timed_out = 1;
            for (int i = 0; i < cfg.threads; i++)
                atomic_store_explicit(&args[i].stop, 1, memory_order_release);
            break;
        }
        struct timespec ts = {0, 1000000};
        nanosleep(&ts, NULL);
    }
    for (int i = 0; i < cfg.threads; i++)
        pthread_join(threads[i], NULL);

    uint64_t ok = 0, fail = 0, read_bytes = 0, vemb_sent = 0;
    uint64_t vadd_sent = 0, vsim_sent = 0, ask_redirects = 0;
    uint64_t moved_redirects = 0, stale_refreshes = 0, refresh_calls = 0;
    uint64_t max_ns = 0;
    double score_sum = 0.0;
    for (int i = 0; i < cfg.threads; i++) {
        ok += args[i].ok;
        fail += args[i].fail;
        read_bytes += args[i].read_bytes;
        vemb_sent += args[i].vemb_sent;
        vadd_sent += args[i].vadd_sent;
        vsim_sent += args[i].vsim_sent;
        ask_redirects += args[i].ask_redirects;
        moved_redirects += args[i].moved_redirects;
        stale_refreshes += args[i].stale_topology_refreshes;
        refresh_calls += args[i].topology_refresh_calls;
        score_sum += args[i].score_sum;
        if (args[i].ns > max_ns)
            max_ns = args[i].ns;
    }
    uint64_t wall = now_ns() - start;
    uint64_t total_ops = ok + fail;
    printf("[done] mode=%s threads=%d ok=%llu fail=%llu qps=%.2f avg_thread_ns/op=%.1f read_bytes=%llu\n",
           mode_name(cfg.mode), cfg.threads, (unsigned long long)ok,
           (unsigned long long)fail,
           total_ops ? (double)total_ops / ((double)wall / 1e9) : 0.0,
           total_ops ? (double)max_ns / total_ops : 0.0,
           (unsigned long long)read_bytes);
    printf("[common-core] ask=%llu moved=%llu stale=%llu topology_refresh=%llu\n",
           (unsigned long long)ask_redirects,
           (unsigned long long)moved_redirects,
           (unsigned long long)stale_refreshes,
           (unsigned long long)refresh_calls);
    if (vemb_sent || vadd_sent || vsim_sent) {
        printf("[client] sent_vemb=%llu sent_vadd=%llu sent_vsim=%llu score_sum=%.6f\n",
               (unsigned long long)vemb_sent, (unsigned long long)vadd_sent,
               (unsigned long long)vsim_sent, score_sum);
    }
    for (uint32_t n = 0; n < cfg.node_count; n++) {
        if (fetch_stats_for_node(&cfg, n, &after[n]) == 0)
            print_stats_delta_node(n, &before[n], &after[n]);
        else
            fprintf(stderr, "warning: fetch stats after run failed for node=%u\n", n);
    }
    zfree(args);
    zfree(threads);
    return fail == 0 && !timed_out ? 0 : 1;
}

static void *worker_main(void *arg) {
    worker_arg_t *w = arg;
#ifdef __linux__
    if (w->cfg.pin_threads) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET((w->tid * 2 + 3) % 64, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
    }
#endif

    uint32_t pipeline = w->cfg.pipeline ? w->cfg.pipeline : 1;
    vemb_v16_req_t req;
    pending_req_t *pending = zcalloc_num(pipeline, sizeof(*pending));
    if (!pending) {
        zfree(pending);
        w->fail = w->cfg.ops;
        atomic_store_explicit(&w->done, 1, memory_order_release);
        return NULL;
    }
    uint32_t inline_vector_cap = w->cfg.dim * sizeof(float);
    uint8_t *inline_vector = NULL;
    if (w->cfg.mode == MODE_VEMB_INLINE ||
        (w->cfg.transport_type == VEMB_V16_TRANSPORT_TCP &&
         w->cfg.mode == MODE_MIXED_80R20W)) {
        inline_vector = zmalloc(inline_vector_cap);
        if (!inline_vector) {
            zfree(pending);
            w->fail = w->cfg.ops;
            atomic_store_explicit(&w->done, 1, memory_order_release);
            return NULL;
        }
    }
    char key[VEMB_V16_MAX_KEY_LEN];
    uint64_t start = now_ns();
    uint32_t sent = 0;
    uint32_t completed = 0;
    uint32_t pending_head = 0;
    uint32_t pending_tail = 0;
    uint32_t pending_count = 0;
    while (completed < w->cfg.ops &&
           !atomic_load_explicit(&w->stop, memory_order_acquire)) {
            while (sent < w->cfg.ops && pending_count < pipeline) {
            if (atomic_load_explicit(&w->stop, memory_order_acquire))
                break;
            uint32_t i = sent;
            uint32_t global_id = (uint32_t)(i + (uint32_t)w->tid * w->cfg.ops);
            uint32_t key_id = workload_key_id(&w->cfg, global_id);
            if (w->cfg.hot_key_enabled) key_id = w->cfg.hot_key_id;
            size_t req_len = vemb_v16_req_handle_len();
            int send_failed = 0;
            int mixed_write = w->cfg.mode == MODE_MIXED_80R20W &&
                              (i % 5u) == 0;
            int expect_inline_vector = 0;
            uint8_t op =
                (w->cfg.mode == MODE_VEMB_INLINE ||
                 (w->cfg.transport_type == VEMB_V16_TRANSPORT_TCP &&
                  w->cfg.mode == MODE_MIXED_80R20W)) ?
                VEMB_V16_OP_VEMB_INLINE : VEMB_V16_OP_VEMB_HANDLE;
            if (w->cfg.mode == MODE_PING) {
                bench_node_channel_t *node = &w->nodes[0];
                memset(&req, 0, sizeof(req));
                req.op = VEMB_V16_OP_PING;
                req.req_id = i + 1;
                req.channel_id = node->desc.channel_id;
                if (send_channel_req(node, &req, req_len,
                                     &w->request_publish_spins,
                                     w->cfg.timeout_ms) != 0) {
                    fprintf(stderr, "worker %d request publish timeout at op=%u mode=ping\n",
                            w->tid, i);
                    send_failed = 1;
                }
            } else if (w->cfg.mode == MODE_VADD ||
                       w->cfg.mode == MODE_VREM ||
                       mixed_write) {
                int is_vrem = w->cfg.mode == MODE_VREM;
                uint32_t write_key_id = mixed_write ? key_id :
                    (is_vrem ? key_id :
                        (w->cfg.keyspace ? key_id :
                            global_id + 100000000u));
                make_key(key, sizeof(key), write_key_id);
                uint32_t node_index = route_key(&w->cfg, key);
                bench_node_channel_t *node = &w->nodes[node_index];
                prepare_req(&req,
                            is_vrem ? VEMB_V16_OP_VREM :
                                VEMB_V16_OP_VADD,
                            i + 1,
                            node->desc.channel_id, key, w->cfg.dim);
                if (is_vrem) {
                    req.dim = 0;
                    req.vector_bytes = 0;
                    req_len = vemb_v16_req_handle_len();
                } else {
                    fill_vector(req.vector, w->cfg.dim, global_id);
                    req_len = vemb_v16_req_inline_len(req.vector_bytes);
                }
                w->vadd_sent++;
                if (w->cfg.client_topology_enabled) {
                    uint8_t status = send_write_with_client_topology(
                        &w->cfg,
                        w->nodes,
                        &w->node_count,
                        &req,
                        req_len,
                        &w->dual_write_sent,
                        &w->stale_topology_refreshes);
                    if (status == VEMB_V16_STATUS_OK) {
                        w->ok++;
                    } else {
                        fprintf(stderr,
                                "worker %d topology write error at op=%u key_id=%u status=%u\n",
                                w->tid,
                                i,
                                write_key_id,
                                status);
                        w->fail++;
                    }
                    sent++;
                    completed++;
                    continue;
                }
                if (send_channel_req(node, &req, req_len,
                                     &w->request_publish_spins,
                                     w->cfg.timeout_ms) != 0) {
                    fprintf(stderr, "worker %d request publish timeout at op=%u mode=%s\n",
                            w->tid, i, mode_name(w->cfg.mode));
                    send_failed = 1;
                }
                pending[pending_tail].node_index = node_index;
            } else if (w->cfg.mode == MODE_VSIM_INLINE) {
                make_key(key, sizeof(key), key_id);
                uint32_t node_index = route_key(&w->cfg, key);
                bench_node_channel_t *node = &w->nodes[node_index];
                prepare_req(&req, VEMB_V16_OP_VSIM_INLINE, i + 1,
                            node->desc.channel_id, key, w->cfg.dim);
                fill_vector(req.vector, w->cfg.dim, global_id + 0x9e3779b9u);
                req_len = vemb_v16_req_inline_len(req.vector_bytes);
                w->vsim_sent++;
                if (send_channel_req(node, &req, req_len,
                                     &w->request_publish_spins,
                                     w->cfg.timeout_ms) != 0) {
                    fprintf(stderr, "worker %d request publish timeout at op=%u mode=vsim-inline key_id=%u\n",
                            w->tid, i, key_id);
                    send_failed = 1;
                }
                pending[pending_tail].node_index = node_index;
            } else if (w->cfg.mode == MODE_VSIM_KEY_KEY) {
                make_key(key, sizeof(key), key_id);
                uint32_t node_index = route_key(&w->cfg, key);
                uint32_t key2_id =
                    choose_vsim_key2_id(&w->cfg, key_id, node_index);
                char key2[VEMB_V16_MAX_KEY_LEN];
                make_key(key2, sizeof(key2), key2_id);
                bench_node_channel_t *node = &w->nodes[node_index];
                prepare_req(&req, VEMB_V16_OP_VSIM_KEY_KEY, i + 1,
                            node->desc.channel_id, key, w->cfg.dim);
                prepare_req_key2(&req, key2);
                w->vsim_sent++;
                if (send_channel_req(node, &req, req_len,
                                     &w->request_publish_spins,
                                     w->cfg.timeout_ms) != 0) {
                    fprintf(stderr, "worker %d request publish timeout at op=%u mode=vsim-key-key key_id=%u key2_id=%u\n",
                            w->tid, i, key_id, key2_id);
                    send_failed = 1;
                }
                pending[pending_tail].node_index = node_index;
            } else {
                make_key(key, sizeof(key), key_id);
                uint32_t node_index = route_key(&w->cfg, key);
                bench_node_channel_t *node = &w->nodes[node_index];
                prepare_req(&req, op, i + 1, node->desc.channel_id, key,
                            w->cfg.dim);
                if (req.op == VEMB_V16_OP_VEMB_INLINE) {
                    expect_inline_vector = 1;
                }
                w->vemb_sent++;
                if (w->cfg.client_topology_enabled) {
                    vemb_v16_resp_t resp;
                    uint32_t inline_vector_bytes = 0;
                    uint8_t status = send_read_with_client_topology(
                        &w->cfg,
                        w->nodes,
                        &w->node_count,
                        &req,
                        req_len,
                        inline_vector,
                        inline_vector_cap,
                        &inline_vector_bytes,
                        &resp,
                        &w->stale_topology_refreshes);
                    if (status != VEMB_V16_STATUS_OK) {
                        fprintf(stderr,
                                "worker %d topology read error at op=%u key_id=%u status=%u\n",
                                w->tid,
                                i,
                                key_id,
                                status);
                        w->fail++;
                        sent++;
                        completed++;
                        continue;
                    }
                    if (req.op == VEMB_V16_OP_VEMB_HANDLE) {
                        bench_region_map_t *warm_region =
                            find_warm_region(w, resp.region_id);
                        if (!warm_region ||
                            resp.vector_offset + resp.vector_bytes >
                                warm_region->region_bytes ||
                            resp.vector_bytes != warm_region->value_size) {
                            fprintf(stderr,
                                    "worker %d invalid vector handle at op=%u region=%u offset=%llu bytes=%u expected_req_id=%u expected_op=%u resp_req_id=%u resp_op=%u\n",
                                    w->tid,
                                    i,
                                    resp.region_id,
                                    (unsigned long long)resp.vector_offset,
                                    resp.vector_bytes,
                                    req.req_id,
                                    req.op,
                                    resp.req_id,
                                    resp.op);
                            w->fail += w->cfg.ops - completed;
                            goto worker_done;
                        }
                        volatile const uint8_t *p =
                            warm_region->mapped_addr + resp.vector_offset;
                        uint8_t checksum = 0;
                        for (uint32_t j = 0; j < resp.vector_bytes; j += 64)
                            checksum ^= p[j];
                        w->read_bytes += resp.vector_bytes + checksum * 0u;
                    } else {
                        if (inline_vector_bytes != resp.vector_bytes ||
                            resp.vector_bytes != inline_vector_cap) {
                            fprintf(stderr,
                                    "worker %d invalid inline vector at op=%u bytes=%u expected=%u resp_bytes=%u expected_req_id=%u expected_op=%u resp_req_id=%u resp_op=%u pending_expect_inline=%d\n",
                                    w->tid,
                                    i,
                                    inline_vector_bytes,
                                    inline_vector_cap,
                                    resp.vector_bytes,
                                    req.req_id,
                                    req.op,
                                    resp.req_id,
                                    resp.op,
                                    1);
                            w->fail += w->cfg.ops - completed;
                            goto worker_done;
                        }
                        {
                            uint8_t checksum = 0;
                            for (uint32_t j = 0; j < inline_vector_bytes; j += 64)
                                checksum ^= inline_vector[j];
                            w->read_bytes += inline_vector_bytes + checksum * 0u;
                        }
                    }
                    w->ok++;
                    sent++;
                    completed++;
                    continue;
                }
                if (send_channel_req(node, &req, req_len,
                                     &w->request_publish_spins,
                                     w->cfg.timeout_ms) != 0) {
                    fprintf(stderr, "worker %d request publish timeout at op=%u key_id=%u\n",
                            w->tid, i, key_id);
                    send_failed = 1;
                }
                pending[pending_tail].node_index = node_index;
            }
            if (send_failed) {
                if (atomic_load_explicit(&w->stop, memory_order_acquire))
                    goto worker_done;
                w->fail += w->cfg.ops - completed;
                goto worker_done;
            }
            pending[pending_tail].op_index = i;
            pending[pending_tail].req_id = req.req_id;
            pending[pending_tail].key_id = key_id;
            pending[pending_tail].op = req.op;
            pending[pending_tail].expect_inline_vector = expect_inline_vector;
            pending_tail = (pending_tail + 1) % pipeline;
            pending_count++;
            sent++;
        }

        if (pending_count == 0)
            continue;

        vemb_v16_resp_t resp;
        uint32_t inline_vector_bytes = 0;
        uint32_t recv_node_index = pending[pending_head].node_index;
        bench_node_channel_t *recv_node = &w->nodes[recv_node_index];
        if (recv_channel_resp(recv_node,
                              &resp,
                              inline_vector,
                              inline_vector_cap,
                              &inline_vector_bytes,
                              &w->response_empty_polls,
                              w->cfg.timeout_ms) != 0) {
            if (atomic_load_explicit(&w->stop, memory_order_acquire))
                goto worker_done;
            uint32_t key_id = pending_count ? pending[pending_head].key_id : 0;
            uint32_t op_index = pending_count ? pending[pending_head].op_index : completed;
            fprintf(stderr, "worker %d response timeout at op=%u key_id=%u\n",
                    w->tid, op_index, key_id);
            w->fail += w->cfg.ops - completed;
            goto worker_done;
        }

        pending_req_t done_req = pending[pending_head];
        pending_head = (pending_head + 1) % pipeline;
        pending_count--;
        if (resp.status != VEMB_V16_STATUS_OK) {
            fprintf(stderr,
                    "worker %d response error at op=%u status=%u key_id=%u expected_req_id=%u expected_op=%u resp_req_id=%u resp_op=%u resp_bytes=%u inline_bytes=%u\n",
                    w->tid,
                    done_req.op_index,
                    resp.status,
                    done_req.key_id,
                    done_req.req_id,
                    done_req.op,
                    resp.req_id,
                    resp.op,
                    resp.vector_bytes,
                    inline_vector_bytes);
            w->fail++;
            completed++;
            continue;
        }
        int should_read_handle_vector =
            w->cfg.mode == MODE_VEMB_HANDLE ||
            (w->cfg.mode == MODE_MIXED_80R20W &&
             w->cfg.transport_type != VEMB_V16_TRANSPORT_TCP &&
             done_req.op == VEMB_V16_OP_VEMB_HANDLE);
        if (should_read_handle_vector) {
            bench_region_map_t *warm_region = find_warm_region(w, resp.region_id);
            if (!warm_region ||
                resp.vector_offset + resp.vector_bytes >
                    warm_region->region_bytes ||
                resp.vector_bytes != warm_region->value_size) {
                fprintf(stderr,
                        "worker %d invalid vector handle at op=%u region=%u offset=%llu bytes=%u expected_req_id=%u expected_op=%u resp_req_id=%u resp_op=%u\n",
                        w->tid,
                        done_req.op_index,
                        resp.region_id,
                        (unsigned long long)resp.vector_offset,
                        resp.vector_bytes,
                        done_req.req_id,
                        done_req.op,
                        resp.req_id,
                        resp.op);
                w->fail += w->cfg.ops - completed;
                goto worker_done;
            }
            volatile const uint8_t *p =
                warm_region->mapped_addr + resp.vector_offset;
            uint8_t checksum = 0;
            for (uint32_t j = 0; j < resp.vector_bytes; j += 64)
                checksum ^= p[j];
            w->read_bytes += resp.vector_bytes + checksum * 0u;
        } else if (done_req.expect_inline_vector) {
            if (inline_vector_bytes != resp.vector_bytes ||
                resp.vector_bytes != inline_vector_cap) {
                fprintf(stderr,
                        "worker %d invalid inline vector at op=%u bytes=%u expected=%u resp_bytes=%u expected_req_id=%u expected_op=%u resp_req_id=%u resp_op=%u pending_expect_inline=%d\n",
                        w->tid,
                        done_req.op_index,
                        inline_vector_bytes,
                        inline_vector_cap,
                        resp.vector_bytes,
                        done_req.req_id,
                        done_req.op,
                        resp.req_id,
                        resp.op,
                        done_req.expect_inline_vector);
                w->fail += w->cfg.ops - completed;
                goto worker_done;
            }
            uint8_t checksum = 0;
            for (uint32_t j = 0; j < inline_vector_bytes; j += 64)
                checksum ^= inline_vector[j];
            w->read_bytes += inline_vector_bytes + checksum * 0u;
        } else if (w->cfg.mode == MODE_VSIM_INLINE ||
                   w->cfg.mode == MODE_VSIM_KEY_KEY) {
            w->score_sum += (double)resp.score;
        }
        w->ok++;
        completed++;
    }
worker_done:
    w->ns = now_ns() - start;
    zfree(inline_vector);
    zfree(pending);
    atomic_store_explicit(&w->done, 1, memory_order_release);
    return NULL;
}

static int mode_from_string(const char *s) {
    if (!strcmp(s, "ping")) return MODE_PING;
    if (!strcmp(s, "vemb-handle")) return MODE_VEMB_HANDLE;
    if (!strcmp(s, "vemb-inline")) return MODE_VEMB_INLINE;
    if (!strcmp(s, "vadd")) return MODE_VADD;
    if (!strcmp(s, "vrem")) return MODE_VREM;
    if (!strcmp(s, "mixed-80r20w")) return MODE_MIXED_80R20W;
    if (!strcmp(s, "vsim-inline")) return MODE_VSIM_INLINE;
    if (!strcmp(s, "vsim-key-key")) return MODE_VSIM_KEY_KEY;
    return -1;
}

static const char *mode_name(int mode) {
    switch (mode) {
    case MODE_PING: return "ping";
    case MODE_VEMB_HANDLE: return "vemb-handle";
    case MODE_VEMB_INLINE: return "vemb-inline";
    case MODE_VADD: return "vadd";
    case MODE_VREM: return "vrem";
    case MODE_MIXED_80R20W: return "mixed-80r20w";
    case MODE_VSIM_INLINE: return "vsim-inline";
    case MODE_VSIM_KEY_KEY: return "vsim-key-key";
    default: return "unknown";
    }
}

static int mode_is_read(int mode) {
    return mode == MODE_VEMB_HANDLE ||
           mode == MODE_VEMB_INLINE ||
           mode == MODE_MIXED_80R20W;
}

static int mode_has_write(int mode) {
    return mode == MODE_VADD ||
           mode == MODE_VREM ||
           mode == MODE_MIXED_80R20W;
}

static int append_thread_count(int **threads,
                               int *count,
                               int *capacity,
                               int value) {
    if (*count == *capacity) {
        int next_capacity = *capacity ? *capacity * 2 : 8;
        if (next_capacity < *capacity ||
            (size_t)next_capacity > SIZE_MAX / sizeof(**threads))
            return -1;
        int *next = zrealloc(*threads, sizeof(**threads) * (size_t)next_capacity);
        if (!next)
            return -1;
        *threads = next;
        *capacity = next_capacity;
    }
    (*threads)[(*count)++] = value;
    return 0;
}

static int parse_thread_list(const bench_cfg_t *cfg, int **threads_out) {
    int *threads = NULL;
    int count = 0;
    int capacity = 0;
    if (!cfg->threads_arg) {
        if (cfg->threads <= 0)
            return -1;
        if (append_thread_count(&threads, &count, &capacity, cfg->threads) != 0)
            return -1;
        *threads_out = threads;
        return count;
    }
    const char *p = cfg->threads_arg;
    while (*p) {
        while (*p == ' ' || *p == '\t')
            p++;
        if (*p == '\0') {
            zfree(threads);
            return -1;
        }
        char *end = NULL;
        errno = 0;
        long v = strtol(p, &end, 10);
        if (end == p || errno == ERANGE || v <= 0 || v > INT_MAX) {
            zfree(threads);
            return -1;
        }
        if (append_thread_count(&threads, &count, &capacity, (int)v) != 0) {
            zfree(threads);
            return -1;
        }
        while (*end == ' ' || *end == '\t')
            end++;
        if (*end == ',') {
            p = end + 1;
        } else if (*end == '\0') {
            break;
        } else {
            zfree(threads);
            return -1;
        }
    }
    if (count <= 0) {
        zfree(threads);
        return -1;
    }
    *threads_out = threads;
    return count;
}

static int parse_endpoint_list(bench_cfg_t *cfg, const char *arg) {
    if (!cfg || !arg || !arg[0])
        return -1;
    cfg->node_count = 0;
    const char *p = arg;
    while (*p) {
        if (cfg->node_count >= VEMB_V16_BENCH_MAX_NODES)
            return -1;
        const char *comma = strchr(p, ',');
        size_t len = comma ? (size_t)(comma - p) : strlen(p);
        if (len == 0 || len >= VEMB_V16_BENCH_PATH_MAX)
            return -1;
        const char *colon = NULL;
        for (const char *q = p; q < p + len; q++) {
            if (*q == ':') colon = q;
        }
        if (!colon || colon == p || colon + 1 >= p + len)
            return -1;
        size_t host_len = (size_t)(colon - p);
        size_t port_len = len - host_len - 1;
        if (host_len >= VEMB_V16_BENCH_PATH_MAX || port_len == 0)
            return -1;
        char port_buf[16];
        if (port_len >= sizeof(port_buf))
            return -1;
        memcpy(cfg->tcp_hosts[cfg->node_count], p, host_len);
        cfg->tcp_hosts[cfg->node_count][host_len] = '\0';
        memcpy(port_buf, colon + 1, port_len);
        port_buf[port_len] = '\0';
        char *end = NULL;
        unsigned long port = strtoul(port_buf, &end, 10);
        if (!end || *end != '\0' || port == 0 || port > UINT16_MAX)
            return -1;
        cfg->tcp_ports[cfg->node_count] = (uint16_t)port;
        cfg->node_count++;
        if (!comma) break;
        p = comma + 1;
    }
    if (cfg->node_count == 0)
        return -1;
    cfg->tcp_host = cfg->tcp_hosts[0];
    cfg->tcp_port = cfg->tcp_ports[0];
    return build_hash_ring(cfg);
}

static void print_stats_delta(const vemb_v16_stats_t *before,
                              const vemb_v16_stats_t *after) {
#define D(field) (unsigned long long)(after->field - before->field)
    printf("[stats] total=%llu vadd=%llu vemb=%llu vsim=%llu not_found=%llu published=%llu completed=%llu active_channels=%llu\n",
           D(total_requests), D(vadd_requests), D(vemb_requests),
           D(vsim_requests),
           D(not_found), D(published_jobs), D(completed_jobs),
           (unsigned long long)after->active_channels);
    printf("[stats] read_pool alloc_ok=%llu alloc_fail=%llu inuse_peak=%llu free_min=%llu\n",
           D(read_pool_alloc_ok),
           D(read_pool_alloc_fail),
           (unsigned long long)after->read_pool_inuse_peak,
           (unsigned long long)after->read_pool_free_min);
    printf("[stats] full job_ring_vemb=%llu job_ring_vadd=%llu response_ring=%llu completion_ring=%llu\n",
           D(proxy_vemb_ring_full), D(proxy_vadd_ring_full),
           D(proxy_response_ring_full), D(supernode_completion_ring_full));
    printf("[stats] supernode completion_publish=%llu\n",
           D(supernode_completion_publish));
    printf("[stats] migration moved=%llu stale=%llu ask=%llu forward=%llu duplicate=%llu source_gc=%llu gc_safe_watermark=%llu baseline_sent=%llu baseline_skipped=%llu baseline_error=%llu baseline_retry_queued=%llu baseline_retry_sent=%llu baseline_retry_pending=%llu\n",
           D(moved_count),
           D(stale_count),
           D(ask_count),
           D(forward_count),
           D(duplicate_request_count),
           D(source_gc_count),
           (unsigned long long)after->gc_safe_watermark,
           D(migration_baseline_sent),
           D(migration_baseline_skipped),
           D(migration_baseline_error),
           D(migration_baseline_retry_queued),
           D(migration_baseline_retry_sent),
           (unsigned long long)after->migration_baseline_retry_pending);
    printf("[stats] bitmap lock_success=%llu lock_failure=%llu\n",
           D(bitmap_lock_success), D(bitmap_lock_failure));
    printf("[stats] warm regions=%llu full=%llu alloc_local=%llu alloc_remote=%llu fallback=%llu cold_spill=%llu fail=%llu evict_ok=%llu evict_fail=%llu overwrite=%llu stale_handle=%llu remote_meta_stale=%llu local_pct=%llu\n",
           (unsigned long long)after->warm_region_count,
           (unsigned long long)after->warm_region_full_count,
           D(warm_alloc_local),
           D(warm_alloc_remote),
           D(warm_alloc_fallback),
           D(warm_alloc_cold_spill),
           D(warm_alloc_fail),
           D(warm_eviction_success),
           D(warm_eviction_fail),
           D(warm_same_key_overwrite),
           D(warm_stale_handle_reject),
           D(remote_meta_stale),
           (unsigned long long)after->warm_region_hash_local_pct);
    printf("[stats] depth request=%llu response=%llu job_shard=%llu completion=%llu\n",
           (unsigned long long)after->request_ring_depth,
           (unsigned long long)after->response_ring_depth,
           (unsigned long long)after->job_shard_queue_depth,
           (unsigned long long)after->completion_ring_depth);
    printf("[stats] remote_meta hit=%llu miss=%llu busy=%llu probes=%llu async_enqueue=%llu async_drop=%llu publish_ok=%llu insert=%llu update=%llu evict=%llu repair_enqueue=%llu repair_ok=%llu rpc_count=%llu rpc_ok=%llu rpc_not_found=%llu rpc_busy=%llu rpc_timeout=%llu rpc_error=%llu rpc_handle=%llu rpc_snapshot=%llu\n",
           D(remote_meta_lookup_hit),
           D(remote_meta_lookup_miss),
           D(remote_meta_lookup_busy),
           D(remote_meta_lookup_way_probe),
           D(remote_meta_publish_async_enqueue),
           D(remote_meta_publish_async_drop),
           D(remote_meta_publish_ok),
           D(remote_meta_publish_insert),
           D(remote_meta_publish_update),
           D(remote_meta_publish_evict),
           D(remote_meta_repair_enqueue),
           D(remote_meta_repair_ok),
           D(ub_lookup_rpc_count),
           D(ub_lookup_rpc_ok),
           D(ub_lookup_rpc_not_found),
           D(ub_lookup_rpc_busy),
           D(ub_lookup_rpc_timeout),
           D(ub_lookup_rpc_error),
           D(ub_lookup_rpc_handle),
           D(ub_lookup_rpc_snapshot));
#undef D
}

static void print_stats_delta_node(uint32_t node_index,
                                   const vemb_v16_stats_t *before,
                                   const vemb_v16_stats_t *after) {
    printf("[stats node=%u]\n", node_index);
    print_stats_delta(before, after);
}

static int fetch_stats_for_node(const bench_cfg_t *cfg,
                                uint32_t node_index,
                                vemb_v16_stats_t *stats) {
    return fetch_stats_tcp(cfg, node_index, stats);
}

static int close_all_for_node(const bench_cfg_t *cfg,
                              uint32_t node_index,
                              uint64_t *closed) {
    return close_all_channels_tcp(cfg, node_index, closed);
}

static void close_node_channel(bench_node_channel_t *node) {
    if (!node) return;
    if (node->net_fd >= 0) {
        vemb_v16_net_write_frame(node->net_fd,
                                 VEMB_V16_NET_CLOSE,
                                 0,
                                 node->desc.channel_id,
                                 0,
                                 NULL,
                                 0);
        close(node->net_fd);
    }
    if (node->desc.channel_id)
        close_channel_tcp(node->tcp_host,
                          node->tcp_port,
                          node->timeout_ms,
                          node->desc.channel_id);
    if (node->req_ring) {
        if (node->req_ring_mapping)
            munmap(node->req_ring_mapping, node->req_ring_mapping_bytes);
        else
            munmap(node->req_ring,
                   vemb_v16_client_ring_bytes(node->desc.request_ring_slot_size));
    }
    if (node->resp_ring) {
        if (node->resp_ring_mapping)
            munmap(node->resp_ring_mapping, node->resp_ring_mapping_bytes);
        else
            munmap(node->resp_ring,
                   vemb_v16_client_ring_bytes(node->desc.response_ring_slot_size));
    }
    close_warm_regions(node);
    memset(node, 0, sizeof(*node));
    node->net_fd = -1;
}

static int setup_node_channel(const bench_cfg_t *cfg,
                              uint32_t node_index,
                              int open_region,
                              bench_node_channel_t *node) {
    if (node_index >= cfg->node_count)
        return -1;
    memset(node, 0, sizeof(*node));
    node->net_fd = -1;
    node->transport_type = cfg->transport_type;
    node->tcp_host = tcp_host_for_node(cfg, node_index);
    node->tcp_port = tcp_port_for_node(cfg, node_index);
    node->timeout_ms = cfg->timeout_ms;
    int remote_path = !aeron_endpoint_is_local(node->tcp_host);
    if (cfg->transport_type == VEMB_V16_TRANSPORT_TCP) {
        if (alloc_tcp_channel(cfg, node_index, &node->desc, &node->net_fd) != 0) {
            close_node_channel(node);
            return -1;
        }
        return 0;
    }
    int alloc_rc = alloc_aeron_channel_tcp(cfg, node_index, remote_path,
                                           &node->desc,
                                           &node->request_peer_view,
                                           &node->response_peer_view,
                                           &node->warm_peer_view);
    if (alloc_rc != 0) {
        close_node_channel(node);
        return -1;
    }
    int request_open_rc = remote_path ?
        open_peer_view_ring(node->desc.request_ring_name,
                            node->desc.request_ring_slot_size,
                            &node->request_peer_view,
                            &node->req_ring_mapping,
                            &node->req_ring_mapping_bytes,
                            &node->req_ring) :
        open_ring(node->desc.request_ring_name,
                  node->desc.request_ring_slot_size, &node->req_ring);
    int response_open_rc = remote_path ?
        open_peer_view_ring(node->desc.response_ring_name,
                            node->desc.response_ring_slot_size,
                            &node->response_peer_view,
                            &node->resp_ring_mapping,
                            &node->resp_ring_mapping_bytes,
                            &node->resp_ring) :
        open_ring(node->desc.response_ring_name,
                  node->desc.response_ring_slot_size, &node->resp_ring);
    if (request_open_rc != 0 || response_open_rc != 0) {
        close_node_channel(node);
        return -1;
    }
    if (open_region && open_warm_regions(&node->desc, node) != 0) {
        close_node_channel(node);
        return -1;
    }
    return 0;
}

static int __attribute__((unused)) run_legacy_transport_once(bench_cfg_t cfg) {
    bench_node_channel_t pre_nodes[VEMB_V16_BENCH_MAX_NODES];
    memset(pre_nodes, 0, sizeof(pre_nodes));
    if (cfg.transport_type == VEMB_V16_TRANSPORT_TCP &&
        mode_is_read(cfg.mode) &&
        cfg.mode != MODE_VEMB_INLINE &&
        cfg.mode != MODE_MIXED_80R20W) {
        fprintf(stderr, "tcp transport read modes require --mode vemb-inline or --mode mixed-80r20w\n");
        return 1;
    }
    if (cfg.transport_type != VEMB_V16_TRANSPORT_TCP &&
        cfg.mode == MODE_VEMB_INLINE) {
        fprintf(stderr, "vemb-inline requires --transport tcp\n");
        return 1;
    }
    if (cfg.client_topology_enabled) {
        if (mode_has_write(cfg.mode) && cfg.pipeline != 1) {
            fprintf(stderr, "legacy transport write modes require --pipeline 1\n");
            return 1;
        }
        if (refresh_client_topology(&cfg) != 0) {
            fprintf(stderr, "failed to refresh client topology\n");
            return 1;
        }
        printf("[topology] epoch=%llu min_write_epoch=%llu active_owners=%u standby_owners=%u flags=0x%x\n",
               (unsigned long long)cfg.client_topology.current_topology_epoch,
               (unsigned long long)cfg.client_topology.min_write_epoch,
               cfg.client_topology.active_ring.owner_count,
               cfg.client_topology.standby_ring.owner_count,
               cfg.client_topology.flags);
    }
    printf("[setup] transport=%s mode=%s dim=%u prefill=%u keyspace=%u key-pattern=%s ops/thread=%u threads=%d pipeline=%u pin=%s\n",
           vemb_v16_transport_name(cfg.transport_type),
           mode_name(cfg.mode), cfg.dim, cfg.prefill,
           workload_keyspace(&cfg), workload_key_pattern(&cfg), cfg.ops,
           cfg.threads, cfg.pipeline,
           cfg.pin_threads ? "yes" : "no");
    if (cfg.prefill && cfg.mode != MODE_PING) {
        for (uint32_t n = 0; n < cfg.node_count; n++) {
            if (setup_node_channel(&cfg, n, 0, &pre_nodes[n]) != 0) {
                fprintf(stderr, "failed to setup prefill channel node=%u\n", n);
                for (uint32_t c = 0; c < cfg.node_count; c++)
                    close_node_channel(&pre_nodes[c]);
                return 1;
            }
        }
        if (prefill_multi(&cfg, pre_nodes, cfg.node_count) != 0) {
            fprintf(stderr, "prefill failed\n");
            for (uint32_t n = 0; n < cfg.node_count; n++)
                close_node_channel(&pre_nodes[n]);
            return 1;
        }
    }
    for (uint32_t n = 0; n < cfg.node_count; n++)
        close_node_channel(&pre_nodes[n]);
    printf("[run] preparing mode=%s threads=%d ops/thread=%u timeout_ms=%u\n",
           mode_name(cfg.mode), cfg.threads, cfg.ops, cfg.timeout_ms);
    fflush(stdout);

    vemb_v16_stats_t before[VEMB_V16_BENCH_MAX_NODES];
    vemb_v16_stats_t after[VEMB_V16_BENCH_MAX_NODES];
    memset(before, 0, sizeof(before));
    memset(after, 0, sizeof(after));
    for (uint32_t n = 0; n < cfg.node_count; n++) {
        if (fetch_stats_for_node(&cfg, n, &before[n]) != 0)
            fprintf(stderr, "warning: fetch stats before run failed for node=%u\n", n);
    }

    worker_arg_t *args = zcalloc_num((size_t)cfg.threads, sizeof(*args));
    pthread_t *threads = zcalloc_num((size_t)cfg.threads, sizeof(*threads));
    if (!args || !threads) return 1;

    for (int i = 0; i < cfg.threads; i++) {
        args[i].tid = i;
        args[i].cfg = cfg;
        args[i].node_count = cfg.node_count;
        atomic_init(&args[i].stop, 0);
        atomic_init(&args[i].done, 0);
        for (uint32_t n = 0; n < cfg.node_count; n++) {
            if (setup_node_channel(&cfg,
                                   n,
                                   cfg.mode == MODE_VEMB_HANDLE ||
                                       (cfg.mode == MODE_MIXED_80R20W &&
                                        cfg.transport_type != VEMB_V16_TRANSPORT_TCP),
                                   &args[i].nodes[n]) != 0) {
                fprintf(stderr, "worker %d channel setup failed node=%u\n", i, n);
                return 1;
            }
        }
    }

    uint64_t start = now_ns();
    printf("[run] mode=%s threads=%d requests=%llu\n",
           mode_name(cfg.mode), cfg.threads,
           (unsigned long long)cfg.ops * (unsigned long long)cfg.threads);
    fflush(stdout);
    for (int i = 0; i < cfg.threads; i++)
        pthread_create(&threads[i], NULL, worker_main, &args[i]);
    uint64_t join_start = now_ns();
    int timed_out = 0;
    for (;;) {
        int done = 0;
        for (int i = 0; i < cfg.threads; i++)
            done += atomic_load_explicit(&args[i].done, memory_order_acquire);
        if (done == cfg.threads) break;
        if (wait_timed_out(join_start, cfg.timeout_ms ? cfg.timeout_ms : 0)) {
            fprintf(stderr, "run timeout: done_workers=%d/%d timeout_ms=%u\n",
                    done, cfg.threads, cfg.timeout_ms);
            for (uint32_t n = 0; n < cfg.node_count; n++) {
                if (fetch_stats_for_node(&cfg, n, &after[n]) == 0)
                    print_stats_delta_node(n, &before[n], &after[n]);
            }
            timed_out = 1;
            for (int i = 0; i < cfg.threads; i++)
                atomic_store_explicit(&args[i].stop, 1, memory_order_release);
            break;
        }
        struct timespec ts = {0, 1000000};
        nanosleep(&ts, NULL);
    }
    for (int i = 0; i < cfg.threads; i++)
        pthread_join(threads[i], NULL);
    uint64_t wall = now_ns() - start;

    uint64_t ok = 0, fail = 0, read_bytes = 0, vemb_sent = 0, vadd_sent = 0, vsim_sent = 0;
    uint64_t dual_write_sent = 0, stale_topology_refreshes = 0;
    uint64_t request_publish_spins = 0, response_empty_polls = 0;
    double score_sum = 0.0;
    uint64_t max_ns = 0;
    for (int i = 0; i < cfg.threads; i++) {
        ok += args[i].ok;
        fail += args[i].fail;
        read_bytes += args[i].read_bytes;
        vemb_sent += args[i].vemb_sent;
        vadd_sent += args[i].vadd_sent;
        vsim_sent += args[i].vsim_sent;
        dual_write_sent += args[i].dual_write_sent;
        stale_topology_refreshes += args[i].stale_topology_refreshes;
        request_publish_spins += args[i].request_publish_spins;
        response_empty_polls += args[i].response_empty_polls;
        score_sum += args[i].score_sum;
        if (args[i].ns > max_ns) max_ns = args[i].ns;
    }
    uint64_t total_ops = ok + fail;
    double qps = (double)total_ops / ((double)wall / 1e9);
    double avg_ns = total_ops ? (double)max_ns / (double)total_ops : 0.0;
    printf("[done] mode=%s threads=%d ok=%llu fail=%llu qps=%.2f avg_thread_ns/op=%.1f read_bytes=%llu\n",
           mode_name(cfg.mode),
           cfg.threads,
           (unsigned long long)ok,
           (unsigned long long)fail,
           qps,
           avg_ns,
           (unsigned long long)read_bytes);
    printf("[client] request_publish_spins=%llu response_empty_polls=%llu\n",
           (unsigned long long)request_publish_spins,
           (unsigned long long)response_empty_polls);
    if (cfg.client_topology_enabled) {
        printf("[client-topology] dual_write_sent=%llu stale_refreshes=%llu\n",
               (unsigned long long)dual_write_sent,
               (unsigned long long)stale_topology_refreshes);
    }
    if (vemb_sent || vadd_sent || vsim_sent) {
        printf("[client] sent_vemb=%llu sent_vadd=%llu sent_vsim=%llu write_ratio=%.2f%% score_sum=%.6f\n",
               (unsigned long long)vemb_sent,
               (unsigned long long)vadd_sent,
               (unsigned long long)vsim_sent,
               (double)vadd_sent * 100.0 / (double)(vemb_sent + vadd_sent + vsim_sent),
               score_sum);
    }
    for (uint32_t n = 0; n < cfg.node_count; n++) {
        if (fetch_stats_for_node(&cfg, n, &after[n]) == 0)
            print_stats_delta_node(n, &before[n], &after[n]);
        else
            fprintf(stderr, "warning: fetch stats after run failed for node=%u\n", n);
    }
    for (int i = 0; i < cfg.threads; i++) {
        for (uint32_t n = 0; n < args[i].node_count; n++)
            close_node_channel(&args[i].nodes[n]);
    }
    zfree(args);
    zfree(threads);
    return fail == 0 && !timed_out ? 0 : 1;
}

int main(int argc, char **argv) {
    bench_cfg_t cfg = {
        .tcp_host = VEMB_V16_TCP_HOST,
        .dim = 0,
        .prefill = 65536,
        .ops = 200000,
        .threads = 8,
        .mode = MODE_VEMB_HANDLE,
        .timeout_ms = 10000,
        .pipeline = 1,
        .transport_type = VEMB_V16_TRANSPORT_AERON,
        .tcp_port = VEMB_V16_TCP_PORT,
        .vsim_key2_owner = VSIM_KEY2_OWNER_SAME,
    };
    cfg.node_count = 1;
    strncpy(cfg.tcp_hosts[0], cfg.tcp_host, sizeof(cfg.tcp_hosts[0]) - 1);
    cfg.tcp_ports[0] = cfg.tcp_port;
    build_hash_ring(&cfg);
    signal(SIGPIPE, SIG_IGN);

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--endpoints") && i + 1 < argc) {
            if (parse_endpoint_list(&cfg, argv[++i]) != 0) {
                fprintf(stderr, "invalid endpoint list\n");
                return 1;
            }
        }
        else if (!strcmp(argv[i], "--transport") && i + 1 < argc) {
            const char *transport = argv[++i];
            if (!strcmp(transport, "aeron")) {
                cfg.transport_type = VEMB_V16_TRANSPORT_AERON;
            } else if (!strcmp(transport, "tcp")) {
                cfg.transport_type = VEMB_V16_TRANSPORT_TCP;
            } else {
                fprintf(stderr, "invalid transport: %s\n", transport);
                return 1;
            }
        }
        else if ((!strcmp(argv[i], "--host") ||
                  !strcmp(argv[i], "--tcp-host")) && i + 1 < argc) {
            cfg.tcp_host = argv[++i];
            strncpy(cfg.tcp_hosts[0], cfg.tcp_host,
                    sizeof(cfg.tcp_hosts[0]) - 1);
            cfg.tcp_hosts[0][sizeof(cfg.tcp_hosts[0]) - 1] = '\0';
        }
        else if ((!strcmp(argv[i], "--port") ||
                  !strcmp(argv[i], "--tcp-port")) && i + 1 < argc) {
            cfg.tcp_port = (uint16_t)strtoul(argv[++i], NULL, 10);
            cfg.tcp_ports[0] = cfg.tcp_port;
        }
        else if (!strcmp(argv[i], "--ub-peer-view-manifest") && i + 1 < argc) {
            cfg.ub_peer_view_manifest_path = argv[++i];
        }
        else if (!strcmp(argv[i], "--ub-peer-view-client-host") && i + 1 < argc) {
            cfg.ub_peer_view_client_host = argv[++i];
        }
        else if (!strcmp(argv[i], "--dim") && i + 1 < argc) cfg.dim = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--prefill") && i + 1 < argc) cfg.prefill = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--keyspace") && i + 1 < argc) cfg.keyspace = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--ops") && i + 1 < argc) cfg.ops = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--timeout-ms") && i + 1 < argc) cfg.timeout_ms = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--pipeline") && i + 1 < argc) cfg.pipeline = (uint32_t)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--threads") && i + 1 < argc) {
            const char *arg = argv[++i];
            cfg.threads_arg = arg;
        }
        else if (!strcmp(argv[i], "--hot-key-id") && i + 1 < argc) {
            cfg.hot_key_enabled = 1;
            cfg.hot_key_id = (uint32_t)strtoul(argv[++i], NULL, 10);
        }
        else if (!strcmp(argv[i], "--key-pattern") && i + 1 < argc) {
            const char *pattern = argv[++i];
            if (!strcmp(pattern, "sequential")) {
                cfg.random_key_pattern = 0;
            } else if (!strcmp(pattern, "random")) {
                cfg.random_key_pattern = 1;
            } else {
                fprintf(stderr, "invalid --key-pattern: %s\n", pattern);
                return 1;
            }
        }
        else if (!strcmp(argv[i], "--pin")) {
            cfg.pin_threads = 1;
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                const char *v = argv[++i];
                cfg.pin_threads = !strcmp(v, "yes") || !strcmp(v, "1") ||
                                  !strcmp(v, "true") || !strcmp(v, "on");
            }
        }
        else if (!strcmp(argv[i], "--no-pin")) {
            cfg.pin_threads = 0;
        }
        else if (!strcmp(argv[i], "--mode") && i + 1 < argc) cfg.mode = mode_from_string(argv[++i]);
        else if (!strcmp(argv[i], "--vsim-key2-owner") && i + 1 < argc) {
            const char *owner = argv[++i];
            if (!strcmp(owner, "same")) {
                cfg.vsim_key2_owner = VSIM_KEY2_OWNER_SAME;
            } else if (!strcmp(owner, "remote")) {
                cfg.vsim_key2_owner = VSIM_KEY2_OWNER_REMOTE;
            } else {
                fprintf(stderr, "invalid --vsim-key2-owner: %s\n", owner);
                return 1;
            }
        }
        else if (!strcmp(argv[i], "--help")) {
            printf("usage: %s [--endpoints HOST:PORT[,HOST:PORT...]] [--host HOST] [--port PORT] [--dim N] [--prefill N] [--keyspace N] [--key-pattern sequential|random] [--ops N] [--timeout-ms N] [--pipeline 1] [--threads N[,N...]] [--pin [yes|no]] [--no-pin] [--hot-key-id N] [--ub-peer-view-manifest FILE --ub-peer-view-client-host HOST] [--mode ping|vemb-handle|vemb-inline|vadd|vrem|mixed-80r20w|vsim-inline]\n", argv[0]);
            return 0;
        }
        else {
            fprintf(stderr, "unknown argument: %s\n", argv[i]);
            return 1;
        }
    }
    if (cfg.mode < 0 ||
        cfg.pipeline == 0 || cfg.pipeline > VEMB_V16_CLIENT_RING_SIZE ||
        cfg.node_count == 0 || cfg.node_count > VEMB_V16_BENCH_MAX_NODES) {
        fprintf(stderr, "invalid arguments\n");
        return 1;
    }
    if (cfg.dim == 0 || cfg.dim > VEMB_V16_MAX_DIM) {
        fprintf(stderr, "--dim is required and must be in [1, %u]\n",
                VEMB_V16_MAX_DIM);
        return 1;
    }
    if (cfg.transport_type == VEMB_V16_TRANSPORT_TCP &&
        cfg.mode == MODE_VEMB_HANDLE) {
        fprintf(stderr,
                "--transport tcp does not support --mode vemb-handle; use vemb-inline\n");
        return 1;
    }
    for (uint32_t n = 0; n < cfg.node_count; n++) {
        if (!cfg.tcp_hosts[n][0] || cfg.tcp_ports[n] == 0) {
            fprintf(stderr, "transport control requires --endpoints HOST:PORT[,HOST:PORT...]\n");
            return 1;
        }
    }
    for (uint32_t n = 0; n < cfg.node_count; n++) {
        uint64_t closed = 0;
        if (close_all_for_node(&cfg, n, &closed) == 0 && closed)
            printf("[setup] node=%u closed stale channels=%llu\n",
                   n, (unsigned long long)closed);
    }

    int *thread_list = NULL;
    int thread_count = parse_thread_list(&cfg, &thread_list);
    if (thread_count <= 0) {
        fprintf(stderr, "invalid thread list\n");
        return 1;
    }
    int ret = 0;
    for (int i = 0; i < thread_count; i++) {
        bench_cfg_t run_cfg = cfg;
        run_cfg.threads = thread_list[i];
        ret = run_common_core_once(run_cfg);
        if (ret != 0) break;
    }
    zfree(thread_list);
    return ret;
}
