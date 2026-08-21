#define _GNU_SOURCE

#include "../src/vemb_v16_net.h"
#include "../src/vemb_v16_migration_outbox.h"
#include "../src/vemb_v16_protocol.h"
#include "../src/vemb_v16_topology.h"

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

typedef enum topology_ctl_action {
    TOPOLOGY_CTL_GET = 0,
    TOPOLOGY_CTL_SET = 1,
    TOPOLOGY_CTL_RANGE_BARRIER = 2,
    TOPOLOGY_CTL_RANGE_CUTOVER = 3,
    TOPOLOGY_CTL_RANGE_WAIT_READY = 4,
    TOPOLOGY_CTL_RANGE_WAIT_CUTOVER = 5,
    TOPOLOGY_CTL_RANGE_SOURCE_GC = 6,
    TOPOLOGY_CTL_COORDINATOR_LISTEN = 7,
    TOPOLOGY_CTL_APPLY_PEER_VIEW_MAP = 8,
    TOPOLOGY_CTL_SET_WITH_PEER_VIEW_MAP = 9,
} topology_ctl_action_t;

typedef struct topology_ctl_cfg {
    uint32_t transport_type;
    uint32_t endpoint_transport_type;
    const char *host;
    uint16_t port;
    uint32_t timeout_ms;
    topology_ctl_action_t action;
    int has_epoch;
    int has_min_write_epoch;
    uint64_t current_topology_epoch;
    uint64_t min_write_epoch;
    uint32_t flags;
    uint32_t vnode_count;
    uint32_t active_owners[VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS];
    uint32_t standby_owners[VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS];
    uint32_t active_owner_count;
    uint32_t standby_owner_count;
    int has_migration_epoch;
    int has_cutover_epoch;
    uint64_t migration_topology_epoch;
    uint64_t cutover_topology_epoch;
    uint32_t target_owner;
    uint32_t shard_id;
    uint32_t page_limit;
    uint32_t wait_ms;
    uint32_t poll_ms;
    uint32_t endpoint_count;
    vemb_v16_topology_endpoint_t
        endpoints[VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS];
    uint32_t coordinator_endpoint_valid;
    vemb_v16_topology_endpoint_t coordinator_endpoint;
    uint32_t expected_source_count;
    uint32_t expected_sources[VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS];
    vemb_v16_peer_view_map_req_t peer_view_map_req;
} topology_ctl_cfg_t;

static void usage(const char *prog) {
    fprintf(stderr,
            "usage: %s --get|--set [--ctl-endpoint tcp|aeron] [--data-endpoint tcp|aeron] "
            "[--host HOST --port PORT] "
            "[--epoch N] [--min-write-epoch N] "
            "[--active 0,1] [--standby 0,1,2] [--dual-write] "
            "[--auto-scaleout] [--coordinated-scaleout] "
            "[--owner-endpoints 0=HOST:PORT,1=HOST:PORT] "
            "[--coordinator-endpoint HOST:PORT] "
            "[--vnode-count N] [--timeout-ms N]\n"
            "       %s --set-with-peer-view-map FILE [--ctl-endpoint tcp|aeron] [--data-endpoint tcp|aeron] "
            "[--host HOST --port PORT] "
            "[--epoch N] [--min-write-epoch N] "
            "[--active 0,1] [--standby 0,1,2] [--dual-write] "
            "[--auto-scaleout] [--coordinated-scaleout] "
            "[--owner-endpoints 0=HOST:PORT,1=HOST:PORT] "
            "[--coordinator-endpoint HOST:PORT] "
            "[--vnode-count N] [--timeout-ms N]\n"
            "       %s --range-barrier|--range-cutover|--range-source-gc|--range-wait-ready|--range-wait-cutover "
            "[--ctl-endpoint tcp|aeron] [--host HOST --port PORT] "
            "--migration-epoch N [--cutover-epoch N] --target-owner N "
            "[--shard-id N] [--page-limit N] [--wait-ms N] [--poll-ms N] [--timeout-ms N]\n"
            "       %s --coordinator-listen [--ctl-endpoint tcp|aeron] "
            "[--host HOST --port PORT] "
            "--expected-sources 0,1 [--migration-epoch N] [--cutover-epoch N] "
            "[--active 0,1,2 | --standby 0,1,2] "
            "[--owner-endpoints 0=HOST:PORT,1=HOST:PORT] "
            "[--wait-ms N] [--timeout-ms N]\n"
            "       %s --apply-peer-view-map FILE [--attach-now] "
            "[--ctl-endpoint tcp|aeron] [--host HOST --port PORT] "
            "[--timeout-ms N]\n",
            prog,
            prog,
            prog,
            prog,
            prog);
}

static int parse_u64_arg(const char *arg, uint64_t *out) {
    char *end = NULL;
    errno = 0;
    unsigned long long value = strtoull(arg, &end, 10);
    if (errno != 0 || !end || *end != '\0')
        return -1;
    *out = (uint64_t)value;
    return 0;
}

static int parse_u32_arg(const char *arg, uint32_t *out) {
    uint64_t value = 0;
    if (parse_u64_arg(arg, &value) != 0 || value > UINT32_MAX)
        return -1;
    *out = (uint32_t)value;
    return 0;
}

static int parse_u16_arg(const char *arg, uint16_t *out) {
    uint32_t value = 0;
    if (parse_u32_arg(arg, &value) != 0 || value == 0 || value > UINT16_MAX)
        return -1;
    *out = (uint16_t)value;
    return 0;
}

static void trim_trailing_ws(char *s) {
    size_t len = strlen(s);
    while (len > 0) {
        char c = s[len - 1];
        if (c != ' ' && c != '\t' && c != '\r' && c != '\n')
            break;
        s[--len] = '\0';
    }
}

static char *trim_ws(char *s) {
    while (*s == ' ' || *s == '\t' || *s == '\r' || *s == '\n')
        s++;
    trim_trailing_ws(s);
    return s;
}

static void strip_comment(char *line) {
    char *hash = strchr(line, '#');
    if (hash)
        *hash = '\0';
}

static int parse_backend_name(const char *value, uint32_t *out) {
    if (!strcmp(value, "shm") || !strcmp(value, "local_shm")) {
        *out = VEMB_V16_REGION_LOCAL_SHM;
        return 0;
    }
    if (!strcmp(value, "ub")) {
        *out = VEMB_V16_REGION_UB;
        return 0;
    }
    return -1;
}

static int parse_u64_value(const char *value, uint64_t *out) {
    return parse_u64_arg(value, out);
}

static int parse_u32_value(const char *value, uint32_t *out) {
    return parse_u32_arg(value, out);
}

static int parse_ub_cache_policy_value(const char *value, uint32_t *out) {
    if (!strcmp(value, "cacheable") || !strcmp(value, "cc")) {
        *out = VEMB_V16_UB_CACHE_POLICY_CACHEABLE;
        return 0;
    }
    if (!strcmp(value, "noncacheable") || !strcmp(value, "nc")) {
        *out = VEMB_V16_UB_CACHE_POLICY_NONCACHEABLE;
        return 0;
    }
    return -1;
}

static int parse_peer_view_ring_field(vemb_v16_peer_view_ring_desc_t *ring,
                                      const char *prefix,
                                      const char *key,
                                      const char *value) {
    size_t prefix_len = strlen(prefix);
    if (strncmp(key, prefix, prefix_len) != 0 || key[prefix_len] != '_')
        return 1;
    const char *suffix = key + prefix_len + 1;
    if (!strcmp(suffix, "path")) {
        if (strlen(value) >= sizeof(ring->path))
            return -1;
        strcpy(ring->path, value);
        return 0;
    }
    if (!strcmp(suffix, "mmap_offset"))
        return parse_u64_value(value, &ring->mmap_offset);
    if (!strcmp(suffix, "cache_policy"))
        return parse_ub_cache_policy_value(value, &ring->cache_policy);
    return 1;
}

static int parse_peer_view_map_file(
        const char *path,
        vemb_v16_peer_view_map_req_t *req) {
    FILE *fp = fopen(path, "r");
    if (!fp)
        return -1;
    memset(req, 0, sizeof(*req));
    req->flags = VEMB_V16_PEER_VIEW_MAP_F_ATTACH_NOW;
    char line[512];
    enum {
        MAP_SECTION_TOP = 0,
        MAP_SECTION_WARM_REGIONS,
        MAP_SECTION_REMOTE_META_VIEWS,
        MAP_SECTION_UB_RPC_PEERS,
    } section = MAP_SECTION_TOP;
    vemb_v16_peer_view_region_desc_t current_region;
    vemb_v16_peer_view_remote_meta_desc_t current_meta;
    vemb_v16_peer_view_ub_rpc_peer_desc_t current_rpc;
    int in_region = 0, in_meta = 0, in_rpc = 0;
    memset(&current_region, 0, sizeof(current_region));
    memset(&current_meta, 0, sizeof(current_meta));
    memset(&current_rpc, 0, sizeof(current_rpc));
    while (fgets(line, sizeof(line), fp)) {
        strip_comment(line);
        char *p = trim_ws(line);
        if (!p[0])
            continue;
        if (!strncmp(p, "- ", 2)) {
            if (section == MAP_SECTION_WARM_REGIONS) {
                if (in_region) {
                    if (req->region_count >= VEMB_V16_PEER_VIEW_MAP_MAX_REGIONS) {
                        fclose(fp);
                        return -1;
                    }
                    req->regions[req->region_count++] = current_region;
                }
                memset(&current_region, 0, sizeof(current_region));
                current_region.weight = 1;
                in_region = 1;
            } else if (section == MAP_SECTION_REMOTE_META_VIEWS) {
                if (in_meta) {
                    if (req->remote_meta_view_count >=
                        VEMB_V16_PEER_VIEW_MAP_MAX_REMOTE_META_VIEWS) {
                        fclose(fp);
                        return -1;
                    }
                    req->remote_meta_views[req->remote_meta_view_count++] =
                        current_meta;
                }
                memset(&current_meta, 0, sizeof(current_meta));
                in_meta = 1;
            } else if (section == MAP_SECTION_UB_RPC_PEERS) {
                if (in_rpc) {
                    if (req->ub_rpc_peer_count >=
                        VEMB_V16_PEER_VIEW_MAP_MAX_UB_RPC_PEERS) {
                        fclose(fp);
                        return -1;
                    }
                    req->ub_rpc_peers[req->ub_rpc_peer_count++] = current_rpc;
                }
                memset(&current_rpc, 0, sizeof(current_rpc));
                in_rpc = 1;
            } else {
                fclose(fp);
                return -1;
            }
            p = trim_ws(p + 2);
        }

        char *colon = strchr(p, ':');
        if (!colon)
            continue;
        *colon = '\0';
        char *key = trim_ws(p);
        char *value = trim_ws(colon + 1);
        if (!*key)
            continue;
        if (!strcmp(key, "warm_regions")) {
            section = MAP_SECTION_WARM_REGIONS;
            continue;
        }
        if (!strcmp(key, "remote_meta_views")) {
            section = MAP_SECTION_REMOTE_META_VIEWS;
            continue;
        }
        if (!strcmp(key, "ub_rpc_peers")) {
            section = MAP_SECTION_UB_RPC_PEERS;
            continue;
        }
        if (!strcmp(key, "attach_now")) {
            if (!strcmp(value, "false") || !strcmp(value, "0"))
                req->flags &= ~VEMB_V16_PEER_VIEW_MAP_F_ATTACH_NOW;
            else
                req->flags |= VEMB_V16_PEER_VIEW_MAP_F_ATTACH_NOW;
            continue;
        }
        if (!strcmp(key, "expected_local_owner_id")) {
            if (parse_u32_value(value, &req->expected_local_owner_id) != 0) {
                fclose(fp);
                return -1;
            }
            req->expected_local_owner_valid = 1;
            continue;
        }
        if (!strcmp(key, "ub_rpc_timeout_ms")) {
            if (parse_u32_value(value, &req->ub_rpc_timeout_ms) != 0) {
                fclose(fp);
                return -1;
            }
            continue;
        }

        if (section == MAP_SECTION_WARM_REGIONS && in_region) {
            if (!strcmp(key, "region_id")) {
                if (parse_u32_value(value, &current_region.region_id) != 0) {
                    fclose(fp);
                    return -1;
                }
            } else if (!strcmp(key, "provider") || !strcmp(key, "backend")) {
                if (parse_backend_name(value, &current_region.backend_type) != 0) {
                    fclose(fp);
                    return -1;
                }
            } else if (!strcmp(key, "path")) {
                if (strlen(value) >= sizeof(current_region.path)) {
                    fclose(fp);
                    return -1;
                }
                strcpy(current_region.path, value);
            } else if (!strcmp(key, "mmap_offset")) {
                if (parse_u64_value(value, &current_region.mmap_offset) != 0) {
                    fclose(fp);
                    return -1;
                }
            } else if (!strcmp(key, "bytes")) {
                if (parse_u64_value(value, &current_region.region_bytes) != 0) {
                    fclose(fp);
                    return -1;
                }
            } else if (!strcmp(key, "value_size")) {
                if (parse_u32_value(value, &current_region.value_size) != 0) {
                    fclose(fp);
                    return -1;
                }
            } else if (!strcmp(key, "home_ub_node_id")) {
                if (parse_u32_value(value,
                                    &current_region.home_ub_node_id) != 0) {
                    fclose(fp);
                    return -1;
                }
            } else if (!strcmp(key, "weight")) {
                if (parse_u32_value(value, &current_region.weight) != 0) {
                    fclose(fp);
                    return -1;
                }
            } else if (!strcmp(key, "cache_policy")) {
                if (parse_ub_cache_policy_value(value,
                                                &current_region.cache_policy) != 0) {
                    fclose(fp);
                    return -1;
                }
            }
            continue;
        }

        if (section == MAP_SECTION_REMOTE_META_VIEWS && in_meta) {
            if (!strcmp(key, "owner_id")) {
                if (parse_u32_value(value, &current_meta.owner_id) != 0) {
                    fclose(fp);
                    return -1;
                }
            } else if (!strcmp(key, "provider") || !strcmp(key, "backend")) {
                if (parse_backend_name(value, &current_meta.backend_type) != 0) {
                    fclose(fp);
                    return -1;
                }
            } else if (!strcmp(key, "path")) {
                if (strlen(value) >= sizeof(current_meta.path)) {
                    fclose(fp);
                    return -1;
                }
                strcpy(current_meta.path, value);
            } else if (!strcmp(key, "mmap_offset")) {
                if (parse_u64_value(value, &current_meta.mmap_offset) != 0) {
                    fclose(fp);
                    return -1;
                }
            } else if (!strcmp(key, "entries")) {
                if (parse_u32_value(value, &current_meta.entry_count) != 0) {
                    fclose(fp);
                    return -1;
                }
            } else if (!strcmp(key, "buckets")) {
                if (parse_u32_value(value, &current_meta.bucket_count) != 0) {
                    fclose(fp);
                    return -1;
                }
            } else if (!strcmp(key, "sets")) {
                if (parse_u32_value(value, &current_meta.set_count) != 0) {
                    fclose(fp);
                    return -1;
                }
            } else if (!strcmp(key, "ways")) {
                if (parse_u32_value(value, &current_meta.ways) != 0) {
                    fclose(fp);
                    return -1;
                }
            } else if (!strcmp(key, "cache_policy")) {
                if (parse_ub_cache_policy_value(value,
                                                &current_meta.cache_policy) != 0) {
                    fclose(fp);
                    return -1;
                }
            }
            continue;
        }

        if (section == MAP_SECTION_UB_RPC_PEERS && in_rpc) {
            if (!strcmp(key, "owner_id")) {
                if (parse_u32_value(value, &current_rpc.owner_id) != 0) {
                    fclose(fp);
                    return -1;
                }
            } else if (!strcmp(key, "provider") || !strcmp(key, "backend")) {
                uint32_t backend_type = 0;
                if (parse_backend_name(value, &backend_type) != 0) {
                    fclose(fp);
                    return -1;
                }
                current_rpc.request.backend_type = backend_type;
                current_rpc.response.backend_type = backend_type;
                current_rpc.inbound_request.backend_type = backend_type;
                current_rpc.outbound_response.backend_type = backend_type;
            } else {
                int parsed = parse_peer_view_ring_field(&current_rpc.request,
                                                        "request",
                                                        key,
                                                        value);
                if (parsed < 0) {
                    fclose(fp);
                    return -1;
                }
                if (parsed == 0)
                    continue;
                parsed = parse_peer_view_ring_field(&current_rpc.response,
                                                    "response",
                                                    key,
                                                    value);
                if (parsed < 0) {
                    fclose(fp);
                    return -1;
                }
                if (parsed == 0)
                    continue;
                parsed = parse_peer_view_ring_field(&current_rpc.inbound_request,
                                                    "inbound_request",
                                                    key,
                                                    value);
                if (parsed < 0) {
                    fclose(fp);
                    return -1;
                }
                if (parsed == 0)
                    continue;
                parsed = parse_peer_view_ring_field(
                    &current_rpc.outbound_response,
                    "outbound_response",
                    key,
                    value);
                if (parsed < 0) {
                    fclose(fp);
                    return -1;
                }
            }
        }
    }
    fclose(fp);
    if (in_region) {
        if (req->region_count >= VEMB_V16_PEER_VIEW_MAP_MAX_REGIONS)
            return -1;
        req->regions[req->region_count++] = current_region;
    }
    if (in_meta) {
        if (req->remote_meta_view_count >=
            VEMB_V16_PEER_VIEW_MAP_MAX_REMOTE_META_VIEWS)
            return -1;
        req->remote_meta_views[req->remote_meta_view_count++] = current_meta;
    }
    if (in_rpc) {
        if (req->ub_rpc_peer_count >= VEMB_V16_PEER_VIEW_MAP_MAX_UB_RPC_PEERS)
            return -1;
        req->ub_rpc_peers[req->ub_rpc_peer_count++] = current_rpc;
    }
    return 0;
}

static int parse_owner_list(const char *arg,
                            uint32_t *owners,
                            uint32_t *owner_count) {
    const char *p = arg;
    uint32_t count = 0;
    if (!arg || !arg[0] || !owners || !owner_count)
        return -1;
    while (*p) {
        if (count >= VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS)
            return -1;
        char *end = NULL;
        errno = 0;
        unsigned long value = strtoul(p, &end, 10);
        if (errno != 0 || !end || end == p || value > UINT32_MAX)
            return -1;
        owners[count++] = (uint32_t)value;
        if (*end == '\0')
            break;
        if (*end != ',')
            return -1;
        p = end + 1;
        if (!*p)
            return -1;
    }
    *owner_count = count;
    return count > 0 ? 0 : -1;
}

static int append_endpoint(topology_ctl_cfg_t *cfg,
                           const vemb_v16_topology_endpoint_t *endpoint) {
    if (!cfg || !endpoint ||
        cfg->endpoint_count >= VEMB_V16_TOPOLOGY_CONTROL_MAX_ENDPOINTS) {
        return -1;
    }
    cfg->endpoints[cfg->endpoint_count++] = *endpoint;
    return 0;
}

static int parse_owner_endpoint_list(topology_ctl_cfg_t *cfg,
                                     const char *arg,
                                     uint32_t transport_type) {
    if (!cfg || !arg || !arg[0])
        return -1;
    const char *p = arg;
    while (*p) {
        const char *comma = strchr(p, ',');
        size_t len = comma ? (size_t)(comma - p) : strlen(p);
        if (len == 0)
            return -1;
        const char *eq = memchr(p, '=', len);
        if (!eq || eq == p || eq + 1 >= p + len)
            return -1;
        char owner_buf[16];
        size_t owner_len = (size_t)(eq - p);
        if (owner_len >= sizeof(owner_buf))
            return -1;
        memcpy(owner_buf, p, owner_len);
        owner_buf[owner_len] = '\0';
        uint32_t owner = 0;
        if (parse_u32_arg(owner_buf, &owner) != 0)
            return -1;

        vemb_v16_topology_endpoint_t endpoint = {
            .owner_id = owner,
            .transport_type = transport_type,
        };
        const char *value = eq + 1;
        size_t value_len = len - owner_len - 1;
        const char *colon = NULL;
        for (const char *q = value; q < value + value_len; q++) {
            if (*q == ':') colon = q;
        }
        if (!colon || colon == value || colon + 1 >= value + value_len)
            return -1;
        size_t host_len = (size_t)(colon - value);
        size_t port_len = value_len - host_len - 1;
        if (host_len >= sizeof(endpoint.host))
            return -1;
        char port_buf[16];
        if (port_len >= sizeof(port_buf))
            return -1;
        memcpy(endpoint.host, value, host_len);
        endpoint.host[host_len] = '\0';
        memcpy(port_buf, colon + 1, port_len);
        port_buf[port_len] = '\0';
        if (parse_u16_arg(port_buf, &endpoint.tcp_port) != 0)
            return -1;
        if (append_endpoint(cfg, &endpoint) != 0)
            return -1;
        if (!comma)
            break;
        p = comma + 1;
    }
    return 0;
}

static int parse_coordinator_endpoint(topology_ctl_cfg_t *cfg,
                                      const char *arg,
                                      uint32_t transport_type) {
    if (!cfg || !arg || !arg[0])
        return -1;
    memset(&cfg->coordinator_endpoint, 0,
           sizeof(cfg->coordinator_endpoint));
    cfg->coordinator_endpoint.owner_id = UINT32_MAX;
    cfg->coordinator_endpoint.transport_type = transport_type;
    const char *colon = strrchr(arg, ':');
    if (!colon || colon == arg || colon[1] == '\0')
        return -1;
    size_t host_len = (size_t)(colon - arg);
    if (host_len >= sizeof(cfg->coordinator_endpoint.host))
        return -1;
    char port_buf[16];
    size_t port_len = strlen(colon + 1);
    if (port_len >= sizeof(port_buf))
        return -1;
    memcpy(cfg->coordinator_endpoint.host, arg, host_len);
    cfg->coordinator_endpoint.host[host_len] = '\0';
    memcpy(port_buf, colon + 1, port_len);
    port_buf[port_len] = '\0';
    if (parse_u16_arg(port_buf,
                      &cfg->coordinator_endpoint.tcp_port) != 0) {
        return -1;
    }
    cfg->coordinator_endpoint_valid = 1;
    return 0;
}

static int topology_control_tcp(const topology_ctl_cfg_t *cfg,
                                const vemb_v16_topology_control_req_t *req,
                                vemb_v16_topology_control_resp_t *resp) {
    int fd = vemb_v16_net_connect(cfg->host, cfg->port, cfg->timeout_ms);
    if (fd < 0)
        return -1;
    uint16_t type = req ? VEMB_V16_NET_TOPOLOGY_SET :
                          VEMB_V16_NET_TOPOLOGY_GET;
    size_t payload_len = 0;
    uint8_t *payload = NULL;
    if (req) {
        payload_len = vemb_v16_topology_control_req_encoded_len(req);
        payload = malloc(payload_len);
        if (!payload ||
            vemb_v16_topology_control_req_encode(payload,
                                                 payload_len,
                                                 req,
                                                 &payload_len) != 0) {
            free(payload);
            close(fd);
            return -1;
        }
    }
    if (vemb_v16_net_write_frame(fd,
                                 type,
                                 0,
                                 0,
                                 0,
                                 payload,
                                 (uint32_t)payload_len) != 0) {
        free(payload);
        close(fd);
        return -1;
    }
    free(payload);

    vemb_v16_net_hdr_t hdr;
    uint8_t *resp_buf = NULL;
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_TOPOLOGY_RESPONSE ||
        hdr.flags != 0) {
        close(fd);
        return -1;
    }
    resp_buf = malloc(hdr.payload_len);
    if (!resp_buf ||
        vemb_v16_net_read_full(fd, resp_buf, hdr.payload_len) != 0 ||
        vemb_v16_topology_control_resp_decode(resp,
                                              resp_buf,
                                              hdr.payload_len) != 0) {
        free(resp_buf);
        close(fd);
        return -1;
    }
    free(resp_buf);
    close(fd);
    return 0;
}

static int topology_control_endpoint(
        const topology_ctl_cfg_t *cfg,
        const vemb_v16_topology_endpoint_t *endpoint,
        const vemb_v16_topology_control_req_t *req,
        vemb_v16_topology_control_resp_t *resp) {
    if (!cfg || !endpoint || !req || !resp)
        return -1;
    topology_ctl_cfg_t endpoint_cfg = *cfg;
    if (endpoint->transport_type != VEMB_V16_TRANSPORT_TCP &&
        endpoint->transport_type != VEMB_V16_TRANSPORT_AERON)
        return -1;
    endpoint_cfg.host = endpoint->host;
    endpoint_cfg.port = endpoint->tcp_port;
    return topology_control_tcp(&endpoint_cfg, req, resp);
}

static int peer_view_map_control_tcp(
        const topology_ctl_cfg_t *cfg,
        const vemb_v16_peer_view_map_req_t *req,
        vemb_v16_peer_view_map_resp_t *resp) {
    int fd = vemb_v16_net_connect(cfg->host, cfg->port, cfg->timeout_ms);
    if (fd < 0)
        return -1;
    if (vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_PEER_VIEW_MAP_APPLY,
                                 0,
                                 0,
                                 0,
                                 req,
                                 sizeof(*req)) != 0) {
        close(fd);
        return -1;
    }
    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_PEER_VIEW_MAP_RESPONSE ||
        hdr.payload_len != sizeof(*resp) ||
        vemb_v16_net_read_full(fd, resp, sizeof(*resp)) != 0) {
        close(fd);
        return -1;
    }
    close(fd);
    return 0;
}

static int peer_view_topology_control_tcp(
        const topology_ctl_cfg_t *cfg,
        const vemb_v16_peer_view_topology_control_req_t *req,
        vemb_v16_peer_view_topology_control_resp_t *resp) {
    int fd = vemb_v16_net_connect(cfg->host, cfg->port, cfg->timeout_ms);
    if (fd < 0)
        return -1;
    if (vemb_v16_net_write_frame(fd,
                                 VEMB_V16_NET_PEER_VIEW_MAP_TOPOLOGY_SET,
                                 0,
                                 0,
                                 0,
                                 req,
                                 sizeof(*req)) != 0) {
        close(fd);
        return -1;
    }
    vemb_v16_net_hdr_t hdr;
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_PEER_VIEW_MAP_TOPOLOGY_RESPONSE ||
        hdr.payload_len != sizeof(*resp) ||
        vemb_v16_net_read_full(fd, resp, sizeof(*resp)) != 0) {
        close(fd);
        return -1;
    }
    close(fd);
    return 0;
}

static uint64_t monotonic_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

static void sleep_ms(uint32_t ms) {
    struct timespec ts = {
        .tv_sec = ms / 1000u,
        .tv_nsec = (long)(ms % 1000u) * 1000000L,
    };
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
    }
}

static int migration_range_control_tcp(
        const topology_ctl_cfg_t *cfg,
        uint16_t type,
        const vemb_v16_migration_range_control_req_t *req,
        vemb_v16_migration_range_control_resp_t *resp) {
    int fd = vemb_v16_net_connect(cfg->host, cfg->port, cfg->timeout_ms);
    if (fd < 0)
        return -1;
    uint8_t req_buf[32];
    size_t req_len = 0;
    if (vemb_v16_migration_range_control_req_encode(req_buf,
                                                    sizeof(req_buf),
                                                    req,
                                                    &req_len) != 0) {
        close(fd);
        return -1;
    }
    if (vemb_v16_net_write_frame(fd,
                                 type,
                                 0,
                                 0,
                                 0,
                                 req_buf,
                                 (uint32_t)req_len) != 0) {
        close(fd);
        return -1;
    }
    vemb_v16_net_hdr_t hdr;
    uint8_t resp_buf[128];
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_MIGRATION_RANGE_CONTROL_RESPONSE ||
        hdr.flags != 0 ||
        hdr.payload_len != vemb_v16_migration_range_control_resp_encoded_len() ||
        vemb_v16_net_read_full(fd, resp_buf, hdr.payload_len) != 0 ||
        vemb_v16_migration_range_control_resp_decode(resp,
                                                     resp_buf,
                                                     hdr.payload_len) != 0) {
        close(fd);
        return -1;
    }
    close(fd);
    return 0;
}

static void print_owner_list(const char *name,
                             const uint32_t *owners,
                             uint32_t owner_count) {
    printf("%s=", name);
    for (uint32_t i = 0; i < owner_count; i++) {
        printf("%s%u", i ? "," : "", owners[i]);
    }
    printf("\n");
}

static void print_response(const vemb_v16_topology_control_resp_t *resp) {
    printf("status=%u\n", resp->status);
    printf("current_topology_epoch=%llu\n",
           (unsigned long long)resp->current_topology_epoch);
    printf("min_write_epoch=%llu\n",
           (unsigned long long)resp->min_write_epoch);
    printf("vnode_count=%u\n", resp->vnode_count);
    printf("flags=0x%x\n", resp->flags);
    print_owner_list("active_owners",
                     resp->active_owners,
                     resp->active_owner_count);
    print_owner_list("standby_owners",
                     resp->standby_owners,
                     resp->standby_owner_count);
    printf("coordinator_endpoint_valid=%u\n",
           resp->coordinator_endpoint_valid);
    if (resp->coordinator_endpoint_valid) {
        const vemb_v16_topology_endpoint_t *endpoint =
            &resp->coordinator_endpoint;
        printf("coordinator_endpoint=transport:%s host:%s port:%u\n",
               vemb_v16_transport_name(endpoint->transport_type),
               endpoint->host,
               endpoint->tcp_port);
    }
    printf("endpoint_count=%u\n", resp->endpoint_count);
    for (uint32_t i = 0; i < resp->endpoint_count; i++) {
        const vemb_v16_topology_endpoint_t *endpoint = &resp->endpoints[i];
        printf("endpoint[%u]=owner:%u transport:%s host:%s port:%u\n",
               i,
               endpoint->owner_id,
               vemb_v16_transport_name(endpoint->transport_type),
               endpoint->host,
               endpoint->tcp_port);
    }
}

static void print_range_response(
        const vemb_v16_migration_range_control_resp_t *resp) {
    printf("status=%u\n", resp->status);
    printf("migration_topology_epoch=%llu\n",
           (unsigned long long)resp->migration_topology_epoch);
    printf("cutover_topology_epoch=%llu\n",
           (unsigned long long)resp->cutover_topology_epoch);
    printf("owner_epoch=%llu\n",
           (unsigned long long)resp->owner_epoch);
    printf("target_owner=%u\n", resp->target_owner);
    printf("shard_id=%u\n", resp->shard_id);
    printf("key_count=%u\n", resp->key_count);
    printf("success_count=%u\n", resp->success_count);
    printf("error_count=%u\n", resp->error_count);
    printf("applied_seq=%llu\n", (unsigned long long)resp->applied_seq);
    printf("barrier_seq=%llu\n", (unsigned long long)resp->barrier_seq);
    printf("source_seq=%llu\n", (unsigned long long)resp->source_seq);
    printf("retry_delta=%llu\n", (unsigned long long)resp->retry_delta);
    printf("pending_delta=%u\n", resp->pending_delta);
    printf("outbox_state=%u\n", resp->outbox_state);
    printf("remaining_keys=%u\n", resp->remaining_keys);
    printf("page_key_count=%u\n", resp->page_key_count);
    printf("range_done=%u\n", resp->range_done);
    printf("range_ready=%u\n", resp->range_ready);
    printf("page_limit=%u\n", resp->page_limit);
}

static void print_peer_view_map_response(
        const vemb_v16_peer_view_map_resp_t *resp) {
    printf("status=%u\n", resp->status);
    printf("applied_region_count=%u\n", resp->applied_region_count);
    printf("applied_remote_meta_view_count=%u\n",
           resp->applied_remote_meta_view_count);
    printf("applied_ub_rpc_peer_count=%u\n",
           resp->applied_ub_rpc_peer_count);
}

static void print_peer_view_topology_response(
        const vemb_v16_peer_view_topology_control_resp_t *resp) {
    printf("status=%u\n", resp->status);
    printf("peer_view_map_status=%u\n", resp->peer_view_map_status);
    printf("topology_status=%u\n", resp->topology_status);
    printf("topology_attempted=%u\n", resp->topology_attempted);
    printf("peer_view_map_response:\n");
    print_peer_view_map_response(&resp->peer_view_map_resp);
    printf("topology_response:\n");
    print_response(&resp->topology_resp);
}

static int range_resp_cutover_ready(
        const vemb_v16_migration_range_control_resp_t *resp) {
    return resp &&
           resp->status == VEMB_V16_STATUS_OK &&
           resp->range_ready != 0;
}

static int build_request(const topology_ctl_cfg_t *cfg,
                         vemb_v16_topology_control_req_t *req) {
    if (!cfg->has_epoch ||
        cfg->active_owner_count == 0 ||
        cfg->standby_owner_count == 0) {
        return -1;
    }
    memset(req, 0, sizeof(*req));
    req->current_topology_epoch = cfg->current_topology_epoch;
    req->min_write_epoch = cfg->has_min_write_epoch ?
        cfg->min_write_epoch : cfg->current_topology_epoch;
    req->active_owner_count = cfg->active_owner_count;
    req->standby_owner_count = cfg->standby_owner_count;
    req->vnode_count = cfg->vnode_count ?
        cfg->vnode_count : VEMB_V16_TOPOLOGY_DEFAULT_VNODES;
    req->flags = cfg->flags;
    memcpy(req->active_owners,
           cfg->active_owners,
           sizeof(uint32_t) * cfg->active_owner_count);
    memcpy(req->standby_owners,
           cfg->standby_owners,
           sizeof(uint32_t) * cfg->standby_owner_count);
    req->endpoint_count = cfg->endpoint_count;
    req->coordinator_endpoint_valid = cfg->coordinator_endpoint_valid;
    if (cfg->coordinator_endpoint_valid) {
        req->coordinator_endpoint = cfg->coordinator_endpoint;
    }
    if (cfg->endpoint_count > 0) {
        memcpy(req->endpoints,
               cfg->endpoints,
               sizeof(req->endpoints[0]) * cfg->endpoint_count);
    }
    return 0;
}

static int build_range_request(
        const topology_ctl_cfg_t *cfg,
        vemb_v16_migration_range_control_req_t *req,
        int need_cutover_epoch) {
    if (!cfg->has_migration_epoch ||
        cfg->target_owner == UINT32_MAX ||
        (need_cutover_epoch && !cfg->has_cutover_epoch)) {
        return -1;
    }
    memset(req, 0, sizeof(*req));
    req->migration_topology_epoch = cfg->migration_topology_epoch;
    req->cutover_topology_epoch = cfg->has_cutover_epoch ?
        cfg->cutover_topology_epoch : 0;
    req->target_owner = cfg->target_owner;
    req->shard_id = cfg->shard_id;
    req->page_limit = cfg->page_limit;
    return 0;
}

static int send_range_control(
        const topology_ctl_cfg_t *cfg,
        topology_ctl_action_t action,
        const vemb_v16_migration_range_control_req_t *req,
        vemb_v16_migration_range_control_resp_t *resp) {
    uint16_t type = VEMB_V16_NET_MIGRATION_RANGE_BARRIER;
    if (action == TOPOLOGY_CTL_RANGE_CUTOVER ||
        action == TOPOLOGY_CTL_RANGE_WAIT_CUTOVER) {
        type = VEMB_V16_NET_MIGRATION_RANGE_MARK_CUTOVER;
    } else if (action == TOPOLOGY_CTL_RANGE_SOURCE_GC) {
        type = VEMB_V16_NET_MIGRATION_RANGE_SOURCE_GC;
    }
    return migration_range_control_tcp(cfg, type, req, resp);
}

static int run_range_action(const topology_ctl_cfg_t *cfg) {
    int direct_cutover = cfg->action == TOPOLOGY_CTL_RANGE_CUTOVER;
    int direct_source_gc = cfg->action == TOPOLOGY_CTL_RANGE_SOURCE_GC;
    int wait_ready = cfg->action == TOPOLOGY_CTL_RANGE_WAIT_READY ||
                     cfg->action == TOPOLOGY_CTL_RANGE_WAIT_CUTOVER;
    int wait_cutover = cfg->action == TOPOLOGY_CTL_RANGE_WAIT_CUTOVER;
    vemb_v16_migration_range_control_req_t req;
    if (build_range_request(cfg,
                            &req,
                            direct_cutover ||
                            direct_source_gc ||
                            wait_cutover) != 0) {
        usage("vemb_v16_topology_ctl");
        return 1;
    }

    vemb_v16_migration_range_control_resp_t resp;
    memset(&resp, 0, sizeof(resp));
    if (!wait_ready) {
        if (direct_cutover || direct_source_gc) {
            uint64_t start_ms = monotonic_ms();
            uint64_t deadline_ms = start_ms + cfg->wait_ms;
            for (;;) {
                memset(&resp, 0, sizeof(resp));
                if (send_range_control(cfg, cfg->action, &req, &resp) != 0) {
                    fprintf(stderr, "migration range control request failed\n");
                    return 1;
                }
                if (resp.status != VEMB_V16_STATUS_OK ||
                    resp.range_done != 0) {
                    print_range_response(&resp);
                    return resp.status == VEMB_V16_STATUS_OK &&
                        resp.range_done != 0 ? 0 : 1;
                }
                uint64_t now = monotonic_ms();
                if (cfg->wait_ms == 0 || now >= deadline_ms) {
                    print_range_response(&resp);
                    return 1;
                }
                uint32_t remaining = (uint32_t)(deadline_ms - now);
                sleep_ms(cfg->poll_ms < remaining ?
                         cfg->poll_ms : remaining);
            }
        }
        if (send_range_control(cfg, cfg->action, &req, &resp) != 0) {
            fprintf(stderr, "migration range control request failed\n");
            return 1;
        }
        print_range_response(&resp);
        return resp.status == VEMB_V16_STATUS_OK ? 0 : 1;
    }

    uint64_t start_ms = monotonic_ms();
    uint64_t deadline_ms = start_ms + cfg->wait_ms;
    int ready = 0;
    for (;;) {
        memset(&resp, 0, sizeof(resp));
        if (send_range_control(cfg,
                               TOPOLOGY_CTL_RANGE_BARRIER,
                               &req,
                               &resp) != 0) {
            fprintf(stderr, "migration range barrier request failed\n");
            return 1;
        }
        if (range_resp_cutover_ready(&resp)) {
            ready = 1;
            break;
        }
        uint64_t now = monotonic_ms();
        if (cfg->wait_ms == 0 || now >= deadline_ms)
            break;
        uint32_t remaining = (uint32_t)(deadline_ms - now);
        sleep_ms(cfg->poll_ms < remaining ? cfg->poll_ms : remaining);
    }

    if (!ready) {
        print_range_response(&resp);
        return 1;
    }
    if (!wait_cutover) {
        print_range_response(&resp);
        return 0;
    }

    for (;;) {
        memset(&resp, 0, sizeof(resp));
        if (send_range_control(cfg,
                               TOPOLOGY_CTL_RANGE_CUTOVER,
                               &req,
                               &resp) != 0) {
            fprintf(stderr, "migration range cutover request failed\n");
            return 1;
        }
        if (resp.status != VEMB_V16_STATUS_OK ||
            resp.range_done != 0) {
            print_range_response(&resp);
            return resp.status == VEMB_V16_STATUS_OK &&
                resp.range_done != 0 ? 0 : 1;
        }
        uint64_t now = monotonic_ms();
        if (cfg->wait_ms == 0 || now >= deadline_ms) {
            print_range_response(&resp);
            return 1;
        }
        uint32_t remaining = (uint32_t)(deadline_ms - now);
        sleep_ms(cfg->poll_ms < remaining ? cfg->poll_ms : remaining);
    }
}

static int expected_source_index(const topology_ctl_cfg_t *cfg,
                                 uint32_t source_owner) {
    for (uint32_t i = 0; i < cfg->expected_source_count; i++) {
        if (cfg->expected_sources[i] == source_owner)
            return (int)i;
    }
    return -1;
}

static uint8_t coordinator_record_done(
        const topology_ctl_cfg_t *cfg,
        const vemb_v16_scaleout_local_done_req_t *req,
        uint8_t *done,
        uint32_t *done_count) {
    if (!cfg || !req || !done || !done_count)
        return VEMB_V16_STATUS_ERR;
    if (cfg->has_migration_epoch &&
        req->migration_topology_epoch != cfg->migration_topology_epoch) {
        return VEMB_V16_STATUS_ERR;
    }
    if (cfg->has_cutover_epoch &&
        req->cutover_topology_epoch != cfg->cutover_topology_epoch) {
        return VEMB_V16_STATUS_ERR;
    }
    int idx = expected_source_index(cfg, req->source_owner);
    if (idx < 0)
        return VEMB_V16_STATUS_ERR;
    if (!done[idx]) {
        done[idx] = 1;
        (*done_count)++;
        printf("scaleout_local_done source_owner=%u migration_epoch=%llu cutover_epoch=%llu notify_seq=%llu done=%u/%u\n",
               req->source_owner,
               (unsigned long long)req->migration_topology_epoch,
               (unsigned long long)req->cutover_topology_epoch,
               (unsigned long long)req->notify_seq,
               *done_count,
               cfg->expected_source_count);
        fflush(stdout);
    }
    return VEMB_V16_STATUS_OK;
}

static void coordinator_fill_resp(
        const vemb_v16_scaleout_local_done_req_t *req,
        uint8_t status,
        vemb_v16_scaleout_local_done_resp_t *resp) {
    memset(resp, 0, sizeof(*resp));
    resp->status = status;
    if (!req)
        return;
    resp->migration_topology_epoch = req->migration_topology_epoch;
    resp->cutover_topology_epoch = req->cutover_topology_epoch;
    resp->notify_seq = req->notify_seq;
    resp->source_owner = req->source_owner;
}

static int coordinator_handle_tcp_fd(const topology_ctl_cfg_t *cfg,
                                     int fd,
                                     uint8_t *done,
                                     uint32_t *done_count) {
    vemb_v16_net_set_timeouts(fd, cfg->timeout_ms);
    vemb_v16_net_hdr_t hdr;
    vemb_v16_scaleout_local_done_req_t req;
    vemb_v16_scaleout_local_done_resp_t resp;
    uint8_t req_buf[VEMB_V16_SCALEOUT_LOCAL_DONE_REQ_ENCODED_LEN];
    uint8_t resp_buf[VEMB_V16_SCALEOUT_LOCAL_DONE_RESP_ENCODED_LEN];
    size_t resp_len = 0;
    memset(&req, 0, sizeof(req));
    if (vemb_v16_net_read_header(fd, &hdr) != 0 ||
        hdr.type != VEMB_V16_NET_SCALEOUT_LOCAL_DONE ||
        hdr.payload_len != VEMB_V16_SCALEOUT_LOCAL_DONE_REQ_ENCODED_LEN ||
        vemb_v16_net_read_full(fd, req_buf, hdr.payload_len) != 0 ||
        vemb_v16_scaleout_local_done_req_decode(&req,
                                                req_buf,
                                                hdr.payload_len) != 0) {
        coordinator_fill_resp(NULL, VEMB_V16_STATUS_ERR, &resp);
    } else {
        uint8_t status = coordinator_record_done(cfg,
                                                 &req,
                                                 done,
                                                 done_count);
        coordinator_fill_resp(&req, status, &resp);
    }
    if (vemb_v16_scaleout_local_done_resp_encode(resp_buf,
                                                 sizeof(resp_buf),
                                                 &resp,
                                                 &resp_len) != 0) {
        return -1;
    }
    (void)vemb_v16_net_write_frame(fd,
                                   VEMB_V16_NET_SCALEOUT_LOCAL_DONE_RESPONSE,
                                   0,
                                   0,
                                   0,
                                   resp_buf,
                                   (uint32_t)resp_len);
    return resp.status == VEMB_V16_STATUS_OK ? 0 : -1;
}

static const uint32_t *coordinator_full_active_owners(
        const topology_ctl_cfg_t *cfg,
        uint32_t *owner_count) {
    if (!cfg || !owner_count)
        return NULL;
    if (cfg->standby_owner_count > 0) {
        *owner_count = cfg->standby_owner_count;
        return cfg->standby_owners;
    }
    if (cfg->active_owner_count > 0) {
        *owner_count = cfg->active_owner_count;
        return cfg->active_owners;
    }
    *owner_count = 0;
    return NULL;
}

static int coordinator_build_full_active_req(
        const topology_ctl_cfg_t *cfg,
        vemb_v16_topology_control_req_t *req) {
    if (!cfg || !req || !cfg->has_cutover_epoch)
        return -1;
    uint32_t owner_count = 0;
    const uint32_t *owners =
        coordinator_full_active_owners(cfg, &owner_count);
    if (!owners || owner_count == 0 ||
        owner_count > VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS ||
        cfg->endpoint_count == 0) {
        return -1;
    }

    memset(req, 0, sizeof(*req));
    req->current_topology_epoch = cfg->cutover_topology_epoch;
    req->min_write_epoch = cfg->has_min_write_epoch ?
        cfg->min_write_epoch : cfg->cutover_topology_epoch;
    req->active_owner_count = owner_count;
    req->standby_owner_count = owner_count;
    req->vnode_count = cfg->vnode_count ?
        cfg->vnode_count : VEMB_V16_TOPOLOGY_DEFAULT_VNODES;
    req->flags =
        cfg->flags &
        ~(VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED |
          VEMB_V16_TOPOLOGY_CONTROL_F_AUTO_SCALEOUT |
          VEMB_V16_TOPOLOGY_CONTROL_F_COORDINATED_SCALEOUT);
    memcpy(req->active_owners, owners, sizeof(uint32_t) * owner_count);
    memcpy(req->standby_owners, owners, sizeof(uint32_t) * owner_count);
    req->endpoint_count = cfg->endpoint_count;
    memcpy(req->endpoints,
           cfg->endpoints,
           sizeof(req->endpoints[0]) * cfg->endpoint_count);
    return 0;
}

static int coordinator_publish_full_active(const topology_ctl_cfg_t *cfg) {
    vemb_v16_topology_control_req_t req;
    if (coordinator_build_full_active_req(cfg, &req) != 0) {
        printf("scaleout_full_active_publish=skipped\n");
        return 0;
    }

    uint32_t success_count = 0;
    uint32_t error_count = 0;
    for (uint32_t i = 0; i < cfg->endpoint_count; i++) {
        const vemb_v16_topology_endpoint_t *endpoint = &cfg->endpoints[i];
        vemb_v16_topology_control_resp_t resp;
        memset(&resp, 0, sizeof(resp));
        if (topology_control_endpoint(cfg, endpoint, &req, &resp) == 0 &&
            resp.status == VEMB_V16_STATUS_OK) {
            success_count++;
            printf("scaleout_full_active_publish owner=%u status=ok epoch=%llu\n",
                   endpoint->owner_id,
                   (unsigned long long)req.current_topology_epoch);
        } else {
            error_count++;
            printf("scaleout_full_active_publish owner=%u status=error epoch=%llu\n",
                   endpoint->owner_id,
                   (unsigned long long)req.current_topology_epoch);
        }
        fflush(stdout);
    }
    printf("scaleout_full_active_published=%u errors=%u targets=%u\n",
           success_count,
           error_count,
           cfg->endpoint_count);
    return error_count == 0 ? 0 : -1;
}

static int run_coordinator_listen(const topology_ctl_cfg_t *cfg) {
    if (!cfg || cfg->expected_source_count == 0)
        return 1;
    int listen_fd = vemb_v16_net_listen(cfg->host, cfg->port, 128);
    if (listen_fd < 0) {
        fprintf(stderr, "coordinator listen failed\n");
        return 1;
    }
    uint8_t done[VEMB_V16_TOPOLOGY_CONTROL_MAX_OWNERS];
    memset(done, 0, sizeof(done));
    uint32_t done_count = 0;
    uint64_t start_ms = monotonic_ms();
    uint64_t deadline_ms = cfg->wait_ms ? start_ms + cfg->wait_ms : 0;
    printf("coordinator_listen transport=%s expected_sources=%u\n",
           vemb_v16_transport_name(cfg->transport_type),
           cfg->expected_source_count);
    fflush(stdout);

    while (done_count < cfg->expected_source_count) {
        struct timeval tv;
        struct timeval *tvp = NULL;
        if (deadline_ms != 0) {
            uint64_t now = monotonic_ms();
            if (now >= deadline_ms)
                break;
            uint64_t remain_ms = deadline_ms - now;
            tv.tv_sec = (time_t)(remain_ms / 1000u);
            tv.tv_usec = (suseconds_t)((remain_ms % 1000u) * 1000u);
            tvp = &tv;
        }

        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(listen_fd, &rfds);
        int ready = select(listen_fd + 1, &rfds, NULL, NULL, tvp);
        if (ready < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        if (ready == 0)
            break;

        int fd = accept(listen_fd, NULL, NULL);
        if (fd < 0) {
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            break;
        }
        (void)coordinator_handle_tcp_fd(cfg, fd, done, &done_count);
        close(fd);
    }

    close(listen_fd);
    if (done_count == cfg->expected_source_count) {
        printf("scaleout_all_sources_done=%u\n", done_count);
        return coordinator_publish_full_active(cfg) == 0 ? 0 : 1;
    }
    printf("scaleout_all_sources_done=%u expected=%u\n",
           done_count,
           cfg->expected_source_count);
    return 1;
}

int main(int argc, char **argv) {
    topology_ctl_cfg_t cfg = {
        .transport_type = VEMB_V16_TRANSPORT_TCP,
        .endpoint_transport_type = VEMB_V16_TRANSPORT_TCP,
        .host = VEMB_V16_TCP_HOST,
        .port = VEMB_V16_TCP_PORT,
        .timeout_ms = 5000,
        .action = TOPOLOGY_CTL_GET,
        .target_owner = UINT32_MAX,
        .wait_ms = 5000,
        .poll_ms = 100,
        .vnode_count = VEMB_V16_TOPOLOGY_DEFAULT_VNODES,
    };

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--get")) {
            cfg.action = TOPOLOGY_CTL_GET;
        } else if (!strcmp(argv[i], "--set")) {
            cfg.action = TOPOLOGY_CTL_SET;
        } else if (!strcmp(argv[i], "--set-with-peer-view-map") &&
                   i + 1 < argc) {
            cfg.action = TOPOLOGY_CTL_SET_WITH_PEER_VIEW_MAP;
            if (parse_peer_view_map_file(argv[++i],
                                         &cfg.peer_view_map_req) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--range-barrier")) {
            cfg.action = TOPOLOGY_CTL_RANGE_BARRIER;
        } else if (!strcmp(argv[i], "--range-cutover")) {
            cfg.action = TOPOLOGY_CTL_RANGE_CUTOVER;
        } else if (!strcmp(argv[i], "--range-source-gc")) {
            cfg.action = TOPOLOGY_CTL_RANGE_SOURCE_GC;
        } else if (!strcmp(argv[i], "--range-wait-ready")) {
            cfg.action = TOPOLOGY_CTL_RANGE_WAIT_READY;
        } else if (!strcmp(argv[i], "--range-wait-cutover")) {
            cfg.action = TOPOLOGY_CTL_RANGE_WAIT_CUTOVER;
        } else if (!strcmp(argv[i], "--coordinator-listen")) {
            cfg.action = TOPOLOGY_CTL_COORDINATOR_LISTEN;
        } else if (!strcmp(argv[i], "--apply-peer-view-map") &&
                   i + 1 < argc) {
            cfg.action = TOPOLOGY_CTL_APPLY_PEER_VIEW_MAP;
            if (parse_peer_view_map_file(argv[++i],
                                         &cfg.peer_view_map_req) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--attach-now")) {
            cfg.peer_view_map_req.flags |= VEMB_V16_PEER_VIEW_MAP_F_ATTACH_NOW;
        } else if (!strcmp(argv[i], "--no-attach-now")) {
            cfg.peer_view_map_req.flags &= ~VEMB_V16_PEER_VIEW_MAP_F_ATTACH_NOW;
        } else if ((!strcmp(argv[i], "--ctl-endpoint") ||
                    !strcmp(argv[i], "--transport")) && i + 1 < argc) {
            const char *transport = argv[++i];
            if (!strcmp(transport, "tcp")) {
                cfg.transport_type = VEMB_V16_TRANSPORT_TCP;
            } else if (!strcmp(transport, "aeron")) {
                cfg.transport_type = VEMB_V16_TRANSPORT_AERON;
            } else {
                usage(argv[0]);
                return 1;
            }
        } else if ((!strcmp(argv[i], "--data-endpoint") ||
                    !strcmp(argv[i], "--endpoint-transport")) && i + 1 < argc) {
            const char *transport = argv[++i];
            if (!strcmp(transport, "tcp")) {
                cfg.endpoint_transport_type = VEMB_V16_TRANSPORT_TCP;
            } else if (!strcmp(transport, "aeron")) {
                cfg.endpoint_transport_type = VEMB_V16_TRANSPORT_AERON;
            } else {
                usage(argv[0]);
                return 1;
            }
        } else if ((!strcmp(argv[i], "--host") ||
                    !strcmp(argv[i], "--tcp-host")) && i + 1 < argc) {
            cfg.host = argv[++i];
        } else if ((!strcmp(argv[i], "--port") ||
                    !strcmp(argv[i], "--tcp-port")) && i + 1 < argc) {
            if (parse_u16_arg(argv[++i], &cfg.port) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--epoch") && i + 1 < argc) {
            if (parse_u64_arg(argv[++i], &cfg.current_topology_epoch) != 0) {
                usage(argv[0]);
                return 1;
            }
            cfg.has_epoch = 1;
        } else if (!strcmp(argv[i], "--min-write-epoch") && i + 1 < argc) {
            if (parse_u64_arg(argv[++i], &cfg.min_write_epoch) != 0) {
                usage(argv[0]);
                return 1;
            }
            cfg.has_min_write_epoch = 1;
        } else if (!strcmp(argv[i], "--active") && i + 1 < argc) {
            if (parse_owner_list(argv[++i],
                                 cfg.active_owners,
                                 &cfg.active_owner_count) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--standby") && i + 1 < argc) {
            if (parse_owner_list(argv[++i],
                                 cfg.standby_owners,
                                 &cfg.standby_owner_count) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--dual-write")) {
            cfg.flags |= VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED;
        } else if (!strcmp(argv[i], "--no-dual-write")) {
            cfg.flags &= ~VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED;
        } else if (!strcmp(argv[i], "--auto-scaleout")) {
            cfg.flags |= VEMB_V16_TOPOLOGY_CONTROL_F_AUTO_SCALEOUT;
        } else if (!strcmp(argv[i], "--no-auto-scaleout")) {
            cfg.flags &= ~VEMB_V16_TOPOLOGY_CONTROL_F_AUTO_SCALEOUT;
        } else if (!strcmp(argv[i], "--coordinated-scaleout")) {
            cfg.flags |= VEMB_V16_TOPOLOGY_CONTROL_F_COORDINATED_SCALEOUT;
        } else if (!strcmp(argv[i], "--no-coordinated-scaleout")) {
            cfg.flags &= ~VEMB_V16_TOPOLOGY_CONTROL_F_COORDINATED_SCALEOUT;
        } else if (!strcmp(argv[i], "--owner-endpoints") && i + 1 < argc) {
            if (parse_owner_endpoint_list(&cfg,
                                          argv[++i],
                                          cfg.endpoint_transport_type) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--coordinator-endpoint") &&
                   i + 1 < argc) {
            if (parse_coordinator_endpoint(&cfg,
                                           argv[++i],
                                           cfg.transport_type) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--vnode-count") && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg.vnode_count) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--migration-epoch") && i + 1 < argc) {
            if (parse_u64_arg(argv[++i],
                              &cfg.migration_topology_epoch) != 0) {
                usage(argv[0]);
                return 1;
            }
            cfg.has_migration_epoch = 1;
        } else if (!strcmp(argv[i], "--cutover-epoch") && i + 1 < argc) {
            if (parse_u64_arg(argv[++i],
                              &cfg.cutover_topology_epoch) != 0) {
                usage(argv[0]);
                return 1;
            }
            cfg.has_cutover_epoch = 1;
        } else if ((!strcmp(argv[i], "--target-owner") ||
                    !strcmp(argv[i], "--target")) && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg.target_owner) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--shard-id") && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg.shard_id) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--page-limit") && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg.page_limit) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--expected-sources") &&
                   i + 1 < argc) {
            if (parse_owner_list(argv[++i],
                                 cfg.expected_sources,
                                 &cfg.expected_source_count) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--wait-ms") && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg.wait_ms) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--poll-ms") && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg.poll_ms) != 0 ||
                cfg.poll_ms == 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--timeout-ms") && i + 1 < argc) {
            if (parse_u32_arg(argv[++i], &cfg.timeout_ms) != 0) {
                usage(argv[0]);
                return 1;
            }
        } else if (!strcmp(argv[i], "--help")) {
            usage(argv[0]);
            return 0;
        } else {
            usage(argv[0]);
            return 1;
        }
    }

    if (cfg.action == TOPOLOGY_CTL_RANGE_BARRIER ||
        cfg.action == TOPOLOGY_CTL_RANGE_CUTOVER ||
        cfg.action == TOPOLOGY_CTL_RANGE_SOURCE_GC ||
        cfg.action == TOPOLOGY_CTL_RANGE_WAIT_READY ||
        cfg.action == TOPOLOGY_CTL_RANGE_WAIT_CUTOVER) {
        return run_range_action(&cfg);
    }
    if (cfg.action == TOPOLOGY_CTL_COORDINATOR_LISTEN) {
        return run_coordinator_listen(&cfg);
    }
    if (cfg.action == TOPOLOGY_CTL_APPLY_PEER_VIEW_MAP) {
        vemb_v16_peer_view_map_resp_t resp;
        int rc = peer_view_map_control_tcp(&cfg, &cfg.peer_view_map_req,
                                           &resp);
        if (rc != 0) {
            fprintf(stderr, "peer view map apply failed\n");
            return 1;
        }
        print_peer_view_map_response(&resp);
        return resp.status == VEMB_V16_STATUS_OK ? 0 : 1;
    }

    vemb_v16_topology_control_req_t req;
    vemb_v16_topology_control_req_t *req_ptr = NULL;
    if (cfg.action == TOPOLOGY_CTL_SET ||
        cfg.action == TOPOLOGY_CTL_SET_WITH_PEER_VIEW_MAP) {
        if (build_request(&cfg, &req) != 0) {
            usage(argv[0]);
            return 1;
        }
        req_ptr = &req;
    }

    if (cfg.action == TOPOLOGY_CTL_SET_WITH_PEER_VIEW_MAP) {
        vemb_v16_peer_view_topology_control_req_t combo_req;
        vemb_v16_peer_view_topology_control_resp_t combo_resp;
        memset(&combo_req, 0, sizeof(combo_req));
        memset(&combo_resp, 0, sizeof(combo_resp));
        combo_req.peer_view_map_req = cfg.peer_view_map_req;
        combo_req.topology_req = *req_ptr;
        int rc = peer_view_topology_control_tcp(&cfg, &combo_req, &combo_resp);
        if (rc != 0) {
            fprintf(stderr, "peer view + topology control request failed\n");
            return 1;
        }
        print_peer_view_topology_response(&combo_resp);
        return combo_resp.status == VEMB_V16_STATUS_OK ? 0 : 1;
    }

    vemb_v16_topology_control_resp_t resp;
    memset(&resp, 0, sizeof(resp));
    int rc = topology_control_tcp(&cfg, req_ptr, &resp);
    if (rc != 0) {
        fprintf(stderr, "topology control request failed\n");
        return 1;
    }

    print_response(&resp);
    return resp.status == VEMB_V16_STATUS_OK ? 0 : 1;
}
