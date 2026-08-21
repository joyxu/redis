#include "../clients/c/vemb_v16_client_sdk.h"
#include "../src/vemb_v16_aeron_attach.h"
#include "../src/vemb_v16_batch_ring.h"
#include "../src/vemb_v16_client_ring.h"
#include "../src/vemb_v16_net.h"

#include <assert.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define TEST_DIM 1u
#define TEST_OWNER 1u
#define TEST_DIRECT_OWNER 0u
#define TEST_CLIENT_HOST "111"

typedef struct peer_view_fake_server {
    int listen_fd;
    uint16_t port;
    uint32_t request_slot_size;
    uint32_t response_slot_size;
    char request_path[256];
    char response_path[256];
    char warm_path[256];
    void *request_mapping;
    size_t request_mapping_bytes;
    void *response_mapping;
    size_t response_mapping_bytes;
    void *warm_mapping;
    size_t warm_mapping_bytes;
    uint32_t cycles;
    int remote_attach;
} peer_view_fake_server_t;

typedef struct peer_view_v2_fake_server {
    int listen_fd;
    uint16_t port;
    char request_descriptor_path[256];
    char request_arena_path[256];
    char response_descriptor_path[256];
    char response_arena_path[256];
} peer_view_v2_fake_server_t;

typedef struct sdk_v2_fake_server {
    peer_view_fake_server_t v1;
    char request_descriptor_path[256];
    char request_arena_path[256];
    char response_descriptor_path[256];
    char response_arena_path[256];
    void *request_descriptor_mapping;
    void *request_arena_mapping;
    void *response_descriptor_mapping;
    void *response_arena_mapping;
} sdk_v2_fake_server_t;

static void setup_v1_server(peer_view_fake_server_t *server);
static void teardown_v1_server(peer_view_fake_server_t *server);

static int accept_connection(int listen_fd) {
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);
    int fd = accept(listen_fd, (struct sockaddr *)&addr, &addr_len);

    assert(fd >= 0);
    return fd;
}

static uint16_t listener_port(int fd) {
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);

    assert(getsockname(fd, (struct sockaddr *)&addr, &addr_len) == 0);
    return ntohs(addr.sin_port);
}

static void create_ring_file(char *path, size_t path_cap, const char *prefix,
                             uint32_t slot_size, void **out_mapping,
                             size_t *out_mapping_bytes) {
    assert(snprintf(path, path_cap, "/tmp/%s_XXXXXX", prefix) > 0);
    int fd = mkstemp(path);
    assert(fd >= 0);

    size_t bytes = vemb_v16_client_ring_bytes(slot_size);
    assert(ftruncate(fd, (off_t)bytes) == 0);
    void *mapping = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    assert(mapping != MAP_FAILED);
    close(fd);
    memset(mapping, 0, bytes);
    vemb_v16_client_ring_init(mapping, slot_size);
    *out_mapping = mapping;
    *out_mapping_bytes = bytes;
}

static void create_file(char *path, size_t path_cap, const char *prefix,
                        size_t bytes, int ring, uint32_t slot_size) {
    assert(snprintf(path, path_cap, "/tmp/%s_XXXXXX", prefix) > 0);
    int fd = mkstemp(path);
    assert(fd >= 0);
    assert(ftruncate(fd, (off_t)bytes) == 0);
    if (ring) {
        void *mapping = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED,
                             fd, 0);
        assert(mapping != MAP_FAILED);
        memset(mapping, 0, bytes);
        vemb_v16_client_ring_init(mapping, slot_size);
        assert(munmap(mapping, bytes) == 0);
    }
    close(fd);
}

static void create_warm_file(char *path, size_t path_cap, void **out_mapping,
                             size_t *out_mapping_bytes, float value) {
    create_file(path, path_cap, "vemb_v16_peer_view_warm", sizeof(value), 0,
                0);
    int fd = open(path, O_RDWR);
    assert(fd >= 0);
    void *mapping = mmap(NULL, sizeof(value), PROT_READ | PROT_WRITE,
                         MAP_SHARED, fd, 0);
    assert(mapping != MAP_FAILED);
    close(fd);
    *(float *)mapping = value;
    *out_mapping = mapping;
    *out_mapping_bytes = sizeof(value);
}

static void write_manifest_entry(FILE *fp, const char *role,
                                 const char *resource_id,
                                 const char *provider_path,
                                 const char *client_path,
                                 const char *cache_policy) {
    fprintf(fp,
            "  - client_host: " TEST_CLIENT_HOST "\n"
            "    owner_id: %u\n"
            "    resource_role: %s\n"
            "    resource_id: %s\n"
            "    generation: 1\n"
            "    provider_path: %s\n"
            "    client_path: %s\n"
            "    map_from_start: true\n"
            "    cache_policy: %s\n",
            TEST_OWNER, role, resource_id, provider_path, client_path,
            cache_policy);
}

static void write_v1_manifest(const char *path, const char *request_path,
                              const char *response_path, const char *warm_path,
                              int include_request) {
    FILE *fp = fopen(path, "w");
    assert(fp != NULL);
    fprintf(fp, "version: 1\npeer_views:\n");
    if (include_request) {
        write_manifest_entry(fp, "v1_request_ring", "request",
                             "/provider/v1-request", request_path,
                             "noncacheable");
    }
    write_manifest_entry(fp, "v1_response_ring", "response",
                         "/provider/v1-response", response_path,
                         "cacheable");
    write_manifest_entry(fp, "warm_region", "warm", "/provider/v1-warm",
                         warm_path, "noncacheable");
    assert(fclose(fp) == 0);
}

static void write_v2_manifest(const char *path,
                              const peer_view_v2_fake_server_t *server) {
    FILE *fp = fopen(path, "w");
    assert(fp != NULL);
    fprintf(fp, "version: 1\npeer_views:\n");
    write_manifest_entry(fp, "v2_request_descriptor", "request-desc",
                         "/provider/v2-request-desc",
                         server->request_descriptor_path, "noncacheable");
    write_manifest_entry(fp, "v2_request_arena", "request-arena",
                         "/provider/v2-request-arena",
                         server->request_arena_path, "noncacheable");
    write_manifest_entry(fp, "v2_response_descriptor", "response-desc",
                         "/provider/v2-response-desc",
                         server->response_descriptor_path, "cacheable");
    write_manifest_entry(fp, "v2_response_arena", "response-arena",
                         "/provider/v2-response-arena",
                         server->response_arena_path, "cacheable");
    assert(fclose(fp) == 0);
}

static void write_sdk_v2_manifest(const char *path,
                                  const sdk_v2_fake_server_t *server) {
    FILE *fp = fopen(path, "w");
    assert(fp != NULL);
    fprintf(fp, "version: 1\npeer_views:\n");
    write_manifest_entry(fp, "v1_request_ring", "request",
                         "/provider/v1-request", server->v1.request_path,
                         "noncacheable");
    write_manifest_entry(fp, "v1_response_ring", "response",
                         "/provider/v1-response", server->v1.response_path,
                         "cacheable");
    write_manifest_entry(fp, "warm_region", "warm", "/provider/v1-warm",
                         server->v1.warm_path, "noncacheable");
    write_manifest_entry(fp, "v2_request_descriptor", "request-desc",
                         "/provider/v2-request-desc",
                         server->request_descriptor_path, "noncacheable");
    write_manifest_entry(fp, "v2_request_arena", "request-arena",
                         "/provider/v2-request-arena",
                         server->request_arena_path, "noncacheable");
    write_manifest_entry(fp, "v2_response_descriptor", "response-desc",
                         "/provider/v2-response-desc",
                         server->response_descriptor_path, "cacheable");
    write_manifest_entry(fp, "v2_response_arena", "response-arena",
                         "/provider/v2-response-arena",
                         server->response_arena_path, "cacheable");
    assert(fclose(fp) == 0);
}

static void write_v1_attach_response(int fd,
                                     const peer_view_fake_server_t *server,
                                     uint64_t channel_id) {
    const char *request_path = server->remote_attach ?
        "/provider/v1-request" : server->request_path;
    const char *response_path = server->remote_attach ?
        "/provider/v1-response" : server->response_path;
    const char *warm_path = server->remote_attach ?
        "/provider/v1-warm" : server->warm_path;
    vemb_v16_aeron_attach_req_t request;
    vemb_v16_aeron_attach_resp_t response = {
        .status = 0,
        .channel_id = channel_id,
        .ring_size_slots = VEMB_V16_CLIENT_RING_SIZE,
        .request_shmdev_path_len = (uint32_t)(strlen(request_path) + 1),
        .response_shmdev_path_len = (uint32_t)(strlen(response_path) + 1),
        .req_backend_type = VEMB_V16_REGION_UB,
        .resp_backend_type = VEMB_V16_REGION_UB,
        .req_slot_size = server->request_slot_size,
        .resp_slot_size = server->response_slot_size,
        .warm_region_count = 1,
        .warm_region_id = 7,
        .warm_backend_type = VEMB_V16_REGION_UB,
        .warm_region_bytes = server->warm_mapping_bytes,
        .warm_path_len = (uint32_t)(strlen(warm_path) + 1),
    };

    assert(vemb_v16_net_read_full(fd, &request, sizeof(request)) == 0);
    assert(memcmp(request.magic, VEMB_V16_AERON_ATTACH_MAGIC,
                  VEMB_V16_AERON_ATTACH_MAGIC_LEN) == 0);
    assert(request.dim == TEST_DIM);
    assert(request.flags == (server->remote_attach ?
                             VEMB_V16_AERON_ATTACH_F_REMOTE_PATH : 0));
    memcpy(response.magic, VEMB_V16_AERON_ATTACHED_MAGIC,
           VEMB_V16_AERON_ATTACHED_MAGIC_LEN);
    memcpy(response.request_shmdev_path, request_path,
           response.request_shmdev_path_len);
    memcpy(response.response_shmdev_path, response_path,
           response.response_shmdev_path_len);
    memcpy(response.warm_path, warm_path, response.warm_path_len);
    assert(vemb_v16_net_write_full(fd, &response, sizeof(response)) == 0);
}

static void read_close_channel(int fd, uint64_t expected_channel_id) {
    vemb_v16_net_hdr_t header;

    assert(vemb_v16_net_read_header(fd, &header) == 0);
    assert(header.type == VEMB_V16_NET_CLOSE_CHANNEL);
    assert(header.channel_id == expected_channel_id);
    assert(header.payload_len == 0);
}

static void read_one_request(vemb_v16_client_ring_t *request_ring,
                             vemb_v16_req_t *out) {
    for (uint32_t tries = 0; tries < 10000; tries++) {
        uint8_t wire[VEMB_V16_AERON_REQ_WIRE_MAX_LEN];
        int got = vemb_v16_client_poll(request_ring, wire, sizeof(wire));
        if (got == 0) {
            usleep(1000);
            continue;
        }
        assert(got > 0);
        assert(vemb_v16_req_decode(out, wire, (size_t)got) == 0);
        return;
    }
    assert(!"timed out waiting for peer-view request publication");
}

static void publish_response(vemb_v16_client_ring_t *response_ring,
                             const vemb_v16_req_t *request, int handle) {
    vemb_v16_resp_t response = {
        .status = VEMB_V16_STATUS_OK,
        .op = request->op,
        .req_id = request->req_id,
    };
    if (handle) {
        response.vector_bytes = sizeof(float);
        response.vector_offset = 0;
        response.region_id = 7;
        response.owner_generation = request->channel_id;
    }
    uint8_t wire[VEMB_V16_AERON_RESP_WIRE_MAX_LEN];
    size_t wire_len = 0;

    assert(vemb_v16_resp_encode(wire, sizeof(wire), &response, &wire_len) == 0);
    assert(vemb_v16_client_publish(response_ring, wire, (uint32_t)wire_len) == 0);
}

static void *v1_data_server_main(void *arg) {
    peer_view_fake_server_t *server = arg;
    vemb_v16_client_ring_t *request_ring = server->request_mapping;
    vemb_v16_client_ring_t *response_ring = server->response_mapping;

    for (uint32_t cycle = 0; cycle < server->cycles; cycle++) {
        uint64_t channel_id = 0x1100u + cycle;
        int attach_fd = accept_connection(server->listen_fd);
        write_v1_attach_response(attach_fd, server, channel_id);
        close(attach_fd);

        vemb_v16_req_t request;
        read_one_request(request_ring, &request);
        assert(request.op == VEMB_V16_OP_VADD);
        assert(request.channel_id == channel_id);
        assert(request.dim == TEST_DIM);
        assert(request.vector_bytes == sizeof(float));
        publish_response(response_ring, &request, 0);

        read_one_request(request_ring, &request);
        assert(request.op == VEMB_V16_OP_VEMB_HANDLE);
        assert(request.channel_id == channel_id);
        assert(request.dim == TEST_DIM);
        assert(request.vector_bytes == sizeof(float));
        publish_response(response_ring, &request, 1);

        int close_fd = accept_connection(server->listen_fd);
        read_close_channel(close_fd, channel_id);
        close(close_fd);
    }
    close(server->listen_fd);
    return NULL;
}

static void write_aeron_topology_snapshot(int fd, uint16_t port,
                                          uint32_t owner_id, uint64_t epoch,
                                          uint32_t flags) {
    vemb_v16_net_hdr_t header;
    vemb_v16_topology_control_resp_t response = {
        .status = VEMB_V16_STATUS_OK,
        .flags = flags,
        .current_topology_epoch = epoch,
        .min_write_epoch = epoch,
        .vnode_count = VEMB_V16_TOPOLOGY_DEFAULT_VNODES,
        .active_owner_count = 1,
        .standby_owner_count = 1,
        .endpoint_count = 1,
    };
    uint8_t payload[4096];
    size_t payload_len = 0;

    assert(vemb_v16_net_read_header(fd, &header) == 0);
    assert(header.type == VEMB_V16_NET_TOPOLOGY_GET);
    assert(header.payload_len == 0);
    response.active_owners[0] = owner_id;
    response.standby_owners[0] = owner_id;
    response.endpoints[0].owner_id = owner_id;
    response.endpoints[0].transport_type = VEMB_V16_TRANSPORT_AERON;
    response.endpoints[0].tcp_port = port;
    snprintf(response.endpoints[0].host,
             sizeof(response.endpoints[0].host), "127.0.0.1");
    assert(vemb_v16_topology_control_resp_encode(
               payload, sizeof(payload), &response, &payload_len) == 0);
    assert(vemb_v16_net_write_frame(fd, VEMB_V16_NET_TOPOLOGY_RESPONSE, 0,
                                    0, 0, payload,
                                    (uint32_t)payload_len) == 0);
}

static void write_aeron_topology_epoch(int fd, uint16_t port,
                                       uint32_t owner_id, uint64_t epoch) {
    write_aeron_topology_snapshot(fd, port, owner_id, epoch, 0);
}

static void write_aeron_topology(int fd, uint16_t port, uint32_t owner_id) {
    write_aeron_topology_epoch(fd, port, owner_id, 41);
}

static void write_control_status(int fd, uint8_t status, uint64_t value) {
    vemb_v16_net_status_t response = {
        .status = status,
        .value = value,
    };
    uint8_t payload[VEMB_V16_NET_STATUS_ENCODED_LEN];
    size_t payload_len = 0;

    assert(vemb_v16_net_status_encode(payload, sizeof(payload), &response,
                                       &payload_len) == 0);
    assert(vemb_v16_net_write_frame(fd, VEMB_V16_NET_CONTROL_STATUS, 0,
                                    0, 0, payload,
                                    (uint32_t)payload_len) == 0);
}

static void read_resource_status(int fd, uint64_t expected_channel_id,
                                 uint64_t resource_generation) {
    vemb_v16_net_hdr_t header;

    assert(vemb_v16_net_read_header(fd, &header) == 0);
    assert(header.type == VEMB_V16_NET_AERON_CHANNEL_STATUS);
    assert(header.channel_id == expected_channel_id);
    assert(header.payload_len == 0);
    write_control_status(fd, VEMB_V16_STATUS_OK, resource_generation);
}

static void *v1_sdk_remote_server_main(void *arg) {
    peer_view_fake_server_t *server = arg;
    int topology_fd = accept_connection(server->listen_fd);

    write_aeron_topology(topology_fd, server->port, TEST_OWNER);
    close(topology_fd);
    return v1_data_server_main(arg);
}

static void *v1_sdk_direct_server_main(void *arg) {
    peer_view_fake_server_t *server = arg;
    int topology_fd = accept_connection(server->listen_fd);

    write_aeron_topology(topology_fd, server->port, TEST_DIRECT_OWNER);
    close(topology_fd);
    return v1_data_server_main(arg);
}

static void *v1_failure_server_main(void *arg) {
    peer_view_fake_server_t *server = arg;
    int attach_fd = accept_connection(server->listen_fd);
    write_v1_attach_response(attach_fd, server, 0x2200u);
    close(attach_fd);

    int close_fd = accept_connection(server->listen_fd);
    read_close_channel(close_fd, 0x2200u);
    close(close_fd);
    close(server->listen_fd);
    return NULL;
}

static void set_v2_resource(vemb_v16_aeron_batch_resource_desc_t *resource,
                            const char *provider_path, uint64_t bytes) {
    resource->backend_type = VEMB_V16_REGION_UB;
    resource->path_len = (uint32_t)(strlen(provider_path) + 1);
    resource->mmap_offset = 0;
    resource->bytes = bytes;
    memcpy(resource->path, provider_path, resource->path_len);
}

static void *v2_attach_server_main(void *arg) {
    peer_view_v2_fake_server_t *server = arg;
    int attach_fd = accept_connection(server->listen_fd);
    vemb_v16_aeron_attach_v2_req_t request;
    vemb_v16_aeron_attach_v2_resp_t response = {
        .status = 0,
        .channel_id = 0x3300u,
        .topology_epoch = 91,
        .effective_batch_size = 1,
        .max_batch_bytes = VEMB_V16_BATCH_MAX_BYTES_DEFAULT,
        .descriptor_slot_size = VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE,
        .descriptor_ring_slots = VEMB_V16_CLIENT_RING_SIZE,
    };

    assert(vemb_v16_net_read_full(attach_fd, &request, sizeof(request)) == 0);
    assert(memcmp(request.magic, VEMB_V16_AERON_ATTACH_V2_MAGIC,
                  VEMB_V16_AERON_ATTACH_V2_MAGIC_LEN) == 0);
    assert(request.dim == TEST_DIM);
    assert(request.flags == VEMB_V16_AERON_ATTACH_F_REMOTE_PATH);
    memcpy(response.magic, VEMB_V16_AERON_ATTACHED_V2_MAGIC,
           VEMB_V16_AERON_ATTACHED_V2_MAGIC_LEN);
    set_v2_resource(&response.request_descriptor,
                    "/provider/v2-request-desc",
                    vemb_v16_client_ring_bytes(
                        VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE));
    set_v2_resource(&response.response_descriptor,
                    "/provider/v2-response-desc",
                    vemb_v16_client_ring_bytes(
                        VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE));
    set_v2_resource(&response.request_arena, "/provider/v2-request-arena",
                    VEMB_V16_BATCH_MAX_BYTES_DEFAULT);
    set_v2_resource(&response.response_arena, "/provider/v2-response-arena",
                    VEMB_V16_BATCH_MAX_BYTES_DEFAULT);
    assert(vemb_v16_net_write_full(attach_fd, &response, sizeof(response)) == 0);
    close(attach_fd);

    int close_fd = accept_connection(server->listen_fd);
    read_close_channel(close_fd, response.channel_id);
    close(close_fd);
    close(server->listen_fd);
    return NULL;
}

static void map_file_rw(const char *path, size_t bytes, void **out) {
    int fd = open(path, O_RDWR);
    assert(fd >= 0);
    void *mapping = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED,
                         fd, 0);
    assert(mapping != MAP_FAILED);
    close(fd);
    *out = mapping;
}

static void setup_sdk_v2_server(sdk_v2_fake_server_t *server) {
    memset(server, 0, sizeof(*server));
    setup_v1_server(&server->v1);
    size_t descriptor_bytes = vemb_v16_client_ring_bytes(
        VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE);
    create_file(server->request_descriptor_path,
                sizeof(server->request_descriptor_path),
                "vemb_v16_sdk_v2_req_desc", descriptor_bytes, 1,
                VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE);
    create_file(server->response_descriptor_path,
                sizeof(server->response_descriptor_path),
                "vemb_v16_sdk_v2_resp_desc", descriptor_bytes, 1,
                VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE);
    create_file(server->request_arena_path, sizeof(server->request_arena_path),
                "vemb_v16_sdk_v2_req_arena",
                VEMB_V16_BATCH_MAX_BYTES_DEFAULT, 0, 0);
    create_file(server->response_arena_path,
                sizeof(server->response_arena_path),
                "vemb_v16_sdk_v2_resp_arena",
                VEMB_V16_BATCH_MAX_BYTES_DEFAULT, 0, 0);
    map_file_rw(server->request_descriptor_path, descriptor_bytes,
                &server->request_descriptor_mapping);
    map_file_rw(server->response_descriptor_path, descriptor_bytes,
                &server->response_descriptor_mapping);
    map_file_rw(server->request_arena_path, VEMB_V16_BATCH_MAX_BYTES_DEFAULT,
                &server->request_arena_mapping);
    map_file_rw(server->response_arena_path, VEMB_V16_BATCH_MAX_BYTES_DEFAULT,
                &server->response_arena_mapping);
}

static void teardown_sdk_v2_server(sdk_v2_fake_server_t *server) {
    size_t descriptor_bytes = vemb_v16_client_ring_bytes(
        VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE);
    assert(munmap(server->request_descriptor_mapping, descriptor_bytes) == 0);
    assert(munmap(server->response_descriptor_mapping, descriptor_bytes) == 0);
    assert(munmap(server->request_arena_mapping,
                  VEMB_V16_BATCH_MAX_BYTES_DEFAULT) == 0);
    assert(munmap(server->response_arena_mapping,
                  VEMB_V16_BATCH_MAX_BYTES_DEFAULT) == 0);
    assert(unlink(server->request_descriptor_path) == 0);
    assert(unlink(server->response_descriptor_path) == 0);
    assert(unlink(server->request_arena_path) == 0);
    assert(unlink(server->response_arena_path) == 0);
    teardown_v1_server(&server->v1);
}

static void write_sdk_v2_attach_response(int fd) {
    vemb_v16_aeron_attach_v2_req_t request;
    vemb_v16_aeron_attach_v2_resp_t response = {
        .status = 0,
        .channel_id = 0x3300u,
        .topology_epoch = 41,
        .effective_batch_size = VEMB_V16_BATCH_REQUEST_SIZE_DEFAULT,
        .max_batch_bytes = VEMB_V16_BATCH_MAX_BYTES_DEFAULT,
        .descriptor_slot_size = VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE,
        .descriptor_ring_slots = VEMB_V16_CLIENT_RING_SIZE,
    };

    assert(vemb_v16_net_read_full(fd, &request, sizeof(request)) == 0);
    assert(memcmp(request.magic, VEMB_V16_AERON_ATTACH_V2_MAGIC,
                  VEMB_V16_AERON_ATTACH_V2_MAGIC_LEN) == 0);
    assert(request.dim == TEST_DIM);
    assert(request.flags == VEMB_V16_AERON_ATTACH_F_REMOTE_PATH);
    memcpy(response.magic, VEMB_V16_AERON_ATTACHED_V2_MAGIC,
           VEMB_V16_AERON_ATTACHED_V2_MAGIC_LEN);
    set_v2_resource(&response.request_descriptor,
                    "/provider/v2-request-desc",
                    vemb_v16_client_ring_bytes(
                        VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE));
    set_v2_resource(&response.response_descriptor,
                    "/provider/v2-response-desc",
                    vemb_v16_client_ring_bytes(
                        VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE));
    set_v2_resource(&response.request_arena, "/provider/v2-request-arena",
                    VEMB_V16_BATCH_MAX_BYTES_DEFAULT);
    set_v2_resource(&response.response_arena, "/provider/v2-response-arena",
                    VEMB_V16_BATCH_MAX_BYTES_DEFAULT);
    assert(vemb_v16_net_write_full(fd, &response, sizeof(response)) == 0);
}

static void sdk_v2_publish_handle_response(sdk_v2_fake_server_t *server,
                                           uint32_t expected_items,
                                           uint64_t request_epoch,
                                           uint64_t response_epoch,
                                           int special_item,
                                           uint8_t special_status) {
    vemb_v16_client_ring_t *request_ring =
        server->request_descriptor_mapping;
    batch_desc_t request_desc;
    for (uint32_t tries = 0; tries < 10000; tries++) {
        if (batch_desc_peek(request_ring, &request_desc) &&
            batch_desc_is_current(request_ring, &request_desc)) {
            break;
        }
        usleep(1000);
    }
    assert(batch_desc_is_current(request_ring, &request_desc));
    assert(request_desc.bytes > 0);
    const uint8_t *request_frame = (const uint8_t *)server->request_arena_mapping +
        request_desc.start % VEMB_V16_BATCH_MAX_BYTES_DEFAULT;
    batch_request_view_t request;
    assert(batch_request_decode(&request, request_frame,
                                request_desc.bytes) == 0);
    assert(request.batch_id == request_desc.batch_id);
    assert(request.topology_epoch == request_epoch);
    assert(request.item_count == expected_items);
    if (expected_items == 2 && special_item < 0) {
        assert(batch_request_key_len_at(&request, 0) == strlen("set") + 1 +
               strlen("first"));
        assert(batch_request_key_len_at(&request, 1) == strlen("set") + 1 +
               strlen("second"));
    }
    vemb_v16_client_consume_batch(request_ring, 1);

    batch_response_t response = {
        .batch_id = request.batch_id,
        .topology_epoch = response_epoch,
        .item_count = request.item_count,
    };
    for (uint32_t i = 0; i < response.item_count; i++) {
        response.entries[i] = (vemb_v16_resp_t){
            .status = (int)i == special_item ? special_status :
                VEMB_V16_STATUS_OK,
            .op = VEMB_V16_OP_VEMB_HANDLE,
            .vector_offset = (uint64_t)i * sizeof(float),
            .vector_bytes = sizeof(float),
            .dim = TEST_DIM,
            .region_id = 7,
        };
        if (response.entries[i].status == VEMB_V16_STATUS_ASK)
            response.entries[i].redirect_owner = TEST_OWNER;
    }
    uint8_t frame[VEMB_V16_BATCH_MAX_BYTES_MAX];
    size_t frame_bytes = batch_response_encode(frame, &response);
    batch_arena_producer_t producer;
    batch_arena_producer_init(&producer);
    assert(batch_arena_publish(server->response_descriptor_mapping,
                               server->response_arena_mapping,
                               VEMB_V16_BATCH_MAX_BYTES_DEFAULT, &producer,
                               frame, (uint32_t)frame_bytes,
                               response.item_count,
                               response.batch_id) == RING_OK);
}

static void *sdk_v2_server_main(void *arg) {
    sdk_v2_fake_server_t *server = arg;
    int topology_fd = accept_connection(server->v1.listen_fd);
    write_aeron_topology(topology_fd, server->v1.port, TEST_OWNER);
    close(topology_fd);

    int v1_attach_fd = accept_connection(server->v1.listen_fd);
    write_v1_attach_response(v1_attach_fd, &server->v1, 0x1100u);
    close(v1_attach_fd);

    int v2_attach_fd = accept_connection(server->v1.listen_fd);
    write_sdk_v2_attach_response(v2_attach_fd);
    close(v2_attach_fd);

    sdk_v2_publish_handle_response(server, 2, 41, 41, -1,
                                   VEMB_V16_STATUS_OK);

    int v2_close_fd = accept_connection(server->v1.listen_fd);
    read_close_channel(v2_close_fd, 0x3300u);
    close(v2_close_fd);
    int v1_close_fd = accept_connection(server->v1.listen_fd);
    read_close_channel(v1_close_fd, 0x1100u);
    close(v1_close_fd);
    close(server->v1.listen_fd);
    return NULL;
}

static void *sdk_v2_l0_coalesce_server_main(void *arg) {
    sdk_v2_fake_server_t *server = arg;
    int topology_fd = accept_connection(server->v1.listen_fd);
    write_aeron_topology(topology_fd, server->v1.port, TEST_OWNER);
    close(topology_fd);

    int v1_attach_fd = accept_connection(server->v1.listen_fd);
    write_v1_attach_response(v1_attach_fd, &server->v1, 0x1100u);
    close(v1_attach_fd);

    int v2_attach_fd = accept_connection(server->v1.listen_fd);
    write_sdk_v2_attach_response(v2_attach_fd);
    close(v2_attach_fd);
    /* Two independently submitted equal keys must occupy one L0 batch item. */
    sdk_v2_publish_handle_response(server, 1, 41, 41, -1,
                                   VEMB_V16_STATUS_OK);

    int v2_close_fd = accept_connection(server->v1.listen_fd);
    read_close_channel(v2_close_fd, 0x3300u);
    close(v2_close_fd);
    int v1_close_fd = accept_connection(server->v1.listen_fd);
    read_close_channel(v1_close_fd, 0x1100u);
    close(v1_close_fd);
    close(server->v1.listen_fd);
    return NULL;
}

static void *sdk_v2_stale_server_main(void *arg) {
    sdk_v2_fake_server_t *server = arg;
    int topology_fd = accept_connection(server->v1.listen_fd);
    write_aeron_topology(topology_fd, server->v1.port, TEST_OWNER);
    close(topology_fd);

    int v1_attach_fd = accept_connection(server->v1.listen_fd);
    write_v1_attach_response(v1_attach_fd, &server->v1, 0x1100u);
    close(v1_attach_fd);

    int v2_attach_fd = accept_connection(server->v1.listen_fd);
    write_sdk_v2_attach_response(v2_attach_fd);
    close(v2_attach_fd);
    sdk_v2_publish_handle_response(server, 1, 41, 42, -1,
                                   VEMB_V16_STATUS_OK);

    topology_fd = accept_connection(server->v1.listen_fd);
    write_aeron_topology_epoch(topology_fd, server->v1.port, TEST_OWNER, 42);
    close(topology_fd);

    int status_fd = accept_connection(server->v1.listen_fd);
    read_resource_status(status_fd, 0x1100u, 0x1100u);
    close(status_fd);

    vemb_v16_req_t request;
    read_one_request(server->v1.request_mapping, &request);
    assert(request.op == VEMB_V16_OP_VEMB_HANDLE);
    assert(request.flags == 0);
    assert(request.topology_epoch == 42);
    publish_response(server->v1.response_mapping, &request, 1);

    int v2_close_fd = accept_connection(server->v1.listen_fd);
    read_close_channel(v2_close_fd, 0x3300u);
    close(v2_close_fd);
    int v1_close_fd = accept_connection(server->v1.listen_fd);
    read_close_channel(v1_close_fd, 0x1100u);
    close(v1_close_fd);
    close(server->v1.listen_fd);
    return NULL;
}

static void *sdk_v2_mixed_response_server_main(void *arg) {
    sdk_v2_fake_server_t *server = arg;
    int topology_fd = accept_connection(server->v1.listen_fd);
    write_aeron_topology(topology_fd, server->v1.port, TEST_OWNER);
    close(topology_fd);

    int v1_attach_fd = accept_connection(server->v1.listen_fd);
    write_v1_attach_response(v1_attach_fd, &server->v1, 0x1100u);
    close(v1_attach_fd);

    int v2_attach_fd = accept_connection(server->v1.listen_fd);
    write_sdk_v2_attach_response(v2_attach_fd);
    close(v2_attach_fd);
    sdk_v2_publish_handle_response(server, 2, 41, 41, 1,
                                   VEMB_V16_STATUS_STALE_TOPOLOGY);

    topology_fd = accept_connection(server->v1.listen_fd);
    write_aeron_topology_epoch(topology_fd, server->v1.port, TEST_OWNER, 42);
    close(topology_fd);

    int status_fd = accept_connection(server->v1.listen_fd);
    read_resource_status(status_fd, 0x1100u, 0x1100u);
    close(status_fd);

    vemb_v16_req_t request;
    read_one_request(server->v1.request_mapping, &request);
    assert(request.op == VEMB_V16_OP_VEMB_HANDLE);
    assert(request.flags == 0);
    assert(request.topology_epoch == 42);
    publish_response(server->v1.response_mapping, &request, 1);

    int v2_close_fd = accept_connection(server->v1.listen_fd);
    read_close_channel(v2_close_fd, 0x3300u);
    close(v2_close_fd);
    int v1_close_fd = accept_connection(server->v1.listen_fd);
    read_close_channel(v1_close_fd, 0x1100u);
    close(v1_close_fd);
    close(server->v1.listen_fd);
    return NULL;
}

static void *sdk_v2_ask_server_main(void *arg) {
    sdk_v2_fake_server_t *server = arg;
    int topology_fd = accept_connection(server->v1.listen_fd);
    write_aeron_topology(topology_fd, server->v1.port, TEST_OWNER);
    close(topology_fd);

    int v1_attach_fd = accept_connection(server->v1.listen_fd);
    write_v1_attach_response(v1_attach_fd, &server->v1, 0x1100u);
    close(v1_attach_fd);

    int v2_attach_fd = accept_connection(server->v1.listen_fd);
    write_sdk_v2_attach_response(v2_attach_fd);
    close(v2_attach_fd);
    sdk_v2_publish_handle_response(server, 1, 41, 41, 0,
                                   VEMB_V16_STATUS_ASK);

    vemb_v16_req_t request;
    read_one_request(server->v1.request_mapping, &request);
    assert(request.op == VEMB_V16_OP_VEMB_HANDLE);
    assert(request.flags == VEMB_V16_REQ_F_ASK_REDIRECT);
    assert(request.topology_epoch == 41);
    publish_response(server->v1.response_mapping, &request, 1);

    int v2_close_fd = accept_connection(server->v1.listen_fd);
    read_close_channel(v2_close_fd, 0x3300u);
    close(v2_close_fd);
    int v1_close_fd = accept_connection(server->v1.listen_fd);
    read_close_channel(v1_close_fd, 0x1100u);
    close(v1_close_fd);
    close(server->v1.listen_fd);
    return NULL;
}

static void *sdk_v2_migration_recovery_server_main(void *arg) {
    sdk_v2_fake_server_t *server = arg;
    int topology_fd = accept_connection(server->v1.listen_fd);
    write_aeron_topology_snapshot(
        topology_fd, server->v1.port, TEST_OWNER, 41,
        VEMB_V16_TOPOLOGY_CONTROL_F_DUAL_WRITE_REQUIRED);
    close(topology_fd);

    int v1_attach_fd = accept_connection(server->v1.listen_fd);
    write_v1_attach_response(v1_attach_fd, &server->v1, 0x1100u);
    close(v1_attach_fd);

    vemb_v16_req_t request;
    read_one_request(server->v1.request_mapping, &request);
    assert(request.op == VEMB_V16_OP_VEMB_HANDLE);
    assert(request.flags == 0);
    assert(request.topology_epoch == 41);
    publish_response(server->v1.response_mapping, &request, 1);

    topology_fd = accept_connection(server->v1.listen_fd);
    write_aeron_topology_epoch(topology_fd, server->v1.port, TEST_OWNER, 42);
    close(topology_fd);

    int status_fd = accept_connection(server->v1.listen_fd);
    read_resource_status(status_fd, 0x1100u, 0x1100u);
    close(status_fd);

    int v2_attach_fd = accept_connection(server->v1.listen_fd);
    write_sdk_v2_attach_response(v2_attach_fd);
    close(v2_attach_fd);
    sdk_v2_publish_handle_response(server, 1, 42, 42, -1,
                                   VEMB_V16_STATUS_OK);

    int v2_close_fd = accept_connection(server->v1.listen_fd);
    read_close_channel(v2_close_fd, 0x3300u);
    close(v2_close_fd);
    int v1_close_fd = accept_connection(server->v1.listen_fd);
    read_close_channel(v1_close_fd, 0x1100u);
    close(v1_close_fd);
    close(server->v1.listen_fd);
    return NULL;
}

static void write_v2_attach_rejected(int fd) {
    vemb_v16_aeron_attach_v2_req_t request;
    vemb_v16_aeron_attach_v2_resp_t response = {
        .status = VEMB_V16_STATUS_ERR,
    };

    assert(vemb_v16_net_read_full(fd, &request, sizeof(request)) == 0);
    assert(memcmp(request.magic, VEMB_V16_AERON_ATTACH_V2_MAGIC,
                  VEMB_V16_AERON_ATTACH_V2_MAGIC_LEN) == 0);
    assert(request.dim == TEST_DIM);
    assert(request.flags == VEMB_V16_AERON_ATTACH_F_REMOTE_PATH);
    memcpy(response.magic, VEMB_V16_AERON_ATTACHED_V2_MAGIC,
           VEMB_V16_AERON_ATTACHED_V2_MAGIC_LEN);
    assert(vemb_v16_net_write_full(fd, &response, sizeof(response)) == 0);
}

static void *sdk_v2_attach_reject_server_main(void *arg) {
    peer_view_fake_server_t *server = arg;
    int topology_fd = accept_connection(server->listen_fd);
    write_aeron_topology(topology_fd, server->port, TEST_OWNER);
    close(topology_fd);

    int v1_attach_fd = accept_connection(server->listen_fd);
    write_v1_attach_response(v1_attach_fd, server, 0x1100u);
    close(v1_attach_fd);

    int v2_attach_fd = accept_connection(server->listen_fd);
    write_v2_attach_rejected(v2_attach_fd);
    close(v2_attach_fd);

    vemb_v16_req_t request;
    read_one_request(server->request_mapping, &request);
    assert(request.op == VEMB_V16_OP_VEMB_HANDLE);
    assert(request.topology_epoch == 41);
    publish_response(server->response_mapping, &request, 1);

    int v1_close_fd = accept_connection(server->listen_fd);
    read_close_channel(v1_close_fd, 0x1100u);
    close(v1_close_fd);
    close(server->listen_fd);
    return NULL;
}

static void *sdk_handle_session_v2_reject_server_main(void *arg) {
    peer_view_fake_server_t *server = arg;
    int topology_fd = accept_connection(server->listen_fd);
    write_aeron_topology(topology_fd, server->port, TEST_OWNER);
    close(topology_fd);

    int v1_attach_fd = accept_connection(server->listen_fd);
    write_v1_attach_response(v1_attach_fd, server, 0x1100u);
    close(v1_attach_fd);

    int v2_attach_fd = accept_connection(server->listen_fd);
    write_v2_attach_rejected(v2_attach_fd);
    close(v2_attach_fd);

    for (uint32_t i = 0; i < 2; i++) {
        vemb_v16_req_t request;
        read_one_request(server->request_mapping, &request);
        assert(request.op == VEMB_V16_OP_VEMB_HANDLE);
        assert(request.topology_epoch == 41);
        publish_response(server->response_mapping, &request, 1);
    }

    int v1_close_fd = accept_connection(server->listen_fd);
    read_close_channel(v1_close_fd, 0x1100u);
    close(v1_close_fd);
    close(server->listen_fd);
    return NULL;
}

static void *sdk_handle_session_quiesce_server_main(void *arg) {
    sdk_v2_fake_server_t *server = arg;
    int topology_fd = accept_connection(server->v1.listen_fd);
    write_aeron_topology(topology_fd, server->v1.port, TEST_OWNER);
    close(topology_fd);

    int v1_attach_fd = accept_connection(server->v1.listen_fd);
    write_v1_attach_response(v1_attach_fd, &server->v1, 0x1100u);
    close(v1_attach_fd);

    int v2_attach_fd = accept_connection(server->v1.listen_fd);
    write_sdk_v2_attach_response(v2_attach_fd);
    close(v2_attach_fd);

    topology_fd = accept_connection(server->v1.listen_fd);
    write_aeron_topology_epoch(topology_fd, server->v1.port, TEST_OWNER, 42);
    close(topology_fd);

    int status_fd = accept_connection(server->v1.listen_fd);
    read_resource_status(status_fd, 0x1100u, 0x1100u);
    close(status_fd);

    vemb_v16_req_t request;
    read_one_request(server->v1.request_mapping, &request);
    assert(request.op == VEMB_V16_OP_VEMB_HANDLE);
    assert(request.flags == 0);
    assert(request.topology_epoch == 42);
    publish_response(server->v1.response_mapping, &request, 1);

    int v2_close_fd = accept_connection(server->v1.listen_fd);
    read_close_channel(v2_close_fd, 0x3300u);
    close(v2_close_fd);
    int v1_close_fd = accept_connection(server->v1.listen_fd);
    read_close_channel(v1_close_fd, 0x1100u);
    close(v1_close_fd);
    close(server->v1.listen_fd);
    return NULL;
}

static int publish_and_wait(vemb_v16_aeron_channel_t *channel,
                            const vemb_v16_req_t *request,
                            vemb_v16_resp_t *out_response) {
    uint8_t wire[VEMB_V16_AERON_REQ_WIRE_MAX_LEN];
    size_t wire_len = 0;

    if (vemb_v16_req_encode(wire, sizeof(wire), request, &wire_len) != 0 ||
        vemb_v16_aeron_publish_request(channel, wire, (uint32_t)wire_len) != 0)
        return -1;
    for (uint32_t tries = 0; tries < 10000; tries++) {
        int got = vemb_v16_aeron_poll_response(channel, out_response,
                                                sizeof(*out_response));
        if (got < 0)
            return -1;
        if (got == 0) {
            usleep(1000);
            continue;
        }
        return out_response->status == VEMB_V16_STATUS_OK &&
            out_response->op == request->op &&
            out_response->req_id == request->req_id ? 0 : -1;
    }
    return -1;
}

static void init_request(vemb_v16_req_t *request, uint8_t op,
                         uint32_t req_id, uint64_t channel_id,
                         const char *key, float value) {
    size_t key_len = strlen(key);

    assert(key_len > 0 && key_len <= VEMB_V16_MAX_KEY_LEN);
    *request = (vemb_v16_req_t){
        .op = op,
        .req_id = req_id,
        .channel_id = channel_id,
        .key_hash = vemb_v16_xxh3_64_str(key, key_len),
        .key_len = (uint32_t)key_len,
        .topology_epoch = 0,
        .dim = TEST_DIM,
        .vector_bytes = sizeof(value),
    };
    memcpy(request->key, key, key_len);
    if (op == VEMB_V16_OP_VADD)
        request->vector[0] = value;
}

static void setup_v1_server(peer_view_fake_server_t *server) {
    *server = (peer_view_fake_server_t){
        .listen_fd = vemb_v16_net_listen("127.0.0.1", 0, 8),
        .request_slot_size = vemb_v16_aeron_req_slot_size(TEST_DIM),
        .response_slot_size = vemb_v16_aeron_resp_slot_size(),
        .remote_attach = 1,
    };
    assert(server->listen_fd >= 0);
    server->port = listener_port(server->listen_fd);
    create_ring_file(server->request_path, sizeof(server->request_path),
                     "vemb_v16_peer_view_req", server->request_slot_size,
                     &server->request_mapping, &server->request_mapping_bytes);
    create_ring_file(server->response_path, sizeof(server->response_path),
                     "vemb_v16_peer_view_resp", server->response_slot_size,
                     &server->response_mapping, &server->response_mapping_bytes);
    create_warm_file(server->warm_path, sizeof(server->warm_path),
                     &server->warm_mapping, &server->warm_mapping_bytes, 19.0f);
}

static void teardown_v1_server(peer_view_fake_server_t *server) {
    assert(munmap(server->request_mapping, server->request_mapping_bytes) == 0);
    assert(munmap(server->response_mapping, server->response_mapping_bytes) == 0);
    assert(munmap(server->warm_mapping, server->warm_mapping_bytes) == 0);
    assert(unlink(server->request_path) == 0);
    assert(unlink(server->response_path) == 0);
    assert(unlink(server->warm_path) == 0);
}

static void test_v1_peer_view_data_and_reattach(void) {
    peer_view_fake_server_t server;
    setup_v1_server(&server);
    server.cycles = 2;

    char manifest_path[128];
    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_peer_view_v1_%ld.yaml", (long)getpid());
    write_v1_manifest(manifest_path, server.request_path, server.response_path,
                      server.warm_path, 1);
    vemb_v16_ub_peer_view_manifest_t manifest;
    assert(vemb_v16_ub_peer_view_manifest_load(manifest_path, &manifest) == 0);

    vemb_v16_ub_peer_view_mapping_t mapping;
    assert(vemb_v16_ub_peer_view_manifest_resolve(
               &manifest, TEST_CLIENT_HOST, TEST_OWNER,
               VEMB_V16_UB_PEER_VIEW_V1_REQUEST_RING,
               "/provider/v1-request", 2, &mapping) != 0);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, v1_data_server_main, &server) == 0);

    uint64_t previous_generation = 0;
    for (uint32_t cycle = 0; cycle < server.cycles; cycle++) {
        vemb_v16_aeron_channel_t *channel =
            vemb_v16_aeron_open_remote_with_peer_view(
                "127.0.0.1", server.port, TEST_DIM, &manifest,
                TEST_CLIENT_HOST, TEST_OWNER);
        assert(channel != NULL);
        uint64_t generation =
            vemb_v16_aeron_channel_resource_generation(channel);
        assert(generation != 0);
        assert(generation != previous_generation);
        previous_generation = generation;
        assert(vemb_v16_aeron_open_warm_region(channel) == 0);

        char key[64];
        snprintf(key, sizeof(key), "peer-view-v1-%u", cycle);
        vemb_v16_req_t request;
        vemb_v16_resp_t response;
        init_request(&request, VEMB_V16_OP_VADD, cycle * 2 + 1,
                     vemb_v16_aeron_channel_id(channel), key, 3.0f + cycle);
        assert(publish_and_wait(channel, &request, &response) == 0);

        init_request(&request, VEMB_V16_OP_VEMB_HANDLE, cycle * 2 + 2,
                     vemb_v16_aeron_channel_id(channel), key, 0);
        assert(publish_and_wait(channel, &request, &response) == 0);
        assert(response.vector_bytes == sizeof(float));
        assert(response.region_id == 7);
        float value = 0;
        assert(vemb_v16_aeron_read_vector(channel, response.region_id,
                                           response.vector_offset,
                                           response.vector_bytes, &value,
                                           sizeof(value)) == (int)sizeof(value));
        assert(value == 19.0f);
        vemb_v16_aeron_close(channel);
    }

    assert(pthread_join(thread, NULL) == 0);
    assert(unlink(manifest_path) == 0);
    teardown_v1_server(&server);
}

static void test_sdk_cluster_uses_peer_view_for_matching_owner(void) {
    peer_view_fake_server_t server;
    setup_v1_server(&server);
    server.cycles = 1;

    char manifest_path[128];
    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_peer_view_sdk_%ld.yaml", (long)getpid());
    write_v1_manifest(manifest_path, server.request_path, server.response_path,
                      server.warm_path, 1);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, v1_sdk_remote_server_main,
                          &server) == 0);

    char seed[64];
    snprintf(seed, sizeof(seed), "127.0.0.1:%u", (unsigned)server.port);
    const char *seeds[] = {seed};
    vemb_v16_client_t *client = vemb_v16_client_create(seeds, 1, TEST_DIM,
                                                        1000);
    assert(client != NULL);
    assert(vemb_v16_client_configure_ub_peer_view(
               client, manifest_path, TEST_CLIENT_HOST) == 0);

    float value = 19.0f;
    float readback = 0.0f;
    uint32_t read_dim = 0;
    assert(vemb_v16_client_vadd(client, "set", "remote", &value,
                                TEST_DIM) == 0);
    assert(vemb_v16_client_configure_ub_peer_view(
               client, manifest_path, TEST_CLIENT_HOST) != 0);
    assert(vemb_v16_client_vemb_vector(client, "set", "remote", &readback,
                                       1, &read_dim) == 0);
    assert(readback == value);
    assert(read_dim == TEST_DIM);
    vemb_v16_client_destroy(client);

    assert(pthread_join(thread, NULL) == 0);
    assert(unlink(manifest_path) == 0);
    teardown_v1_server(&server);
}

static void test_sdk_cluster_keeps_unmapped_owner_direct(void) {
    peer_view_fake_server_t server;
    setup_v1_server(&server);
    server.cycles = 1;
    server.remote_attach = 0;

    char manifest_path[128];
    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_peer_view_sdk_direct_%ld.yaml", (long)getpid());
    write_v1_manifest(manifest_path, server.request_path, server.response_path,
                      server.warm_path, 1);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, v1_sdk_direct_server_main,
                          &server) == 0);

    char seed[64];
    snprintf(seed, sizeof(seed), "127.0.0.1:%u", (unsigned)server.port);
    const char *seeds[] = {seed};
    vemb_v16_client_t *client = vemb_v16_client_create(seeds, 1, TEST_DIM,
                                                        1000);
    assert(client != NULL);
    assert(vemb_v16_client_configure_ub_peer_view(
               client, manifest_path, TEST_CLIENT_HOST) == 0);

    float value = 19.0f;
    float readback = 0.0f;
    assert(vemb_v16_client_vadd(client, "set", "direct", &value,
                                TEST_DIM) == 0);
    assert(vemb_v16_client_vemb_vector(client, "set", "direct", &readback,
                                       1, NULL) == 0);
    assert(readback == value);
    vemb_v16_client_destroy(client);

    assert(pthread_join(thread, NULL) == 0);
    assert(unlink(manifest_path) == 0);
    teardown_v1_server(&server);
}

static void test_v1_peer_view_failure_closes_before_publication(void) {
    peer_view_fake_server_t server;
    setup_v1_server(&server);
    char manifest_path[128];
    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_peer_view_missing_%ld.yaml", (long)getpid());
    write_v1_manifest(manifest_path, server.request_path, server.response_path,
                      server.warm_path, 0);
    vemb_v16_ub_peer_view_manifest_t manifest;
    assert(vemb_v16_ub_peer_view_manifest_load(manifest_path, &manifest) == 0);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, v1_failure_server_main, &server) == 0);
    assert(vemb_v16_aeron_open_remote_with_peer_view(
               "127.0.0.1", server.port, TEST_DIM, &manifest,
               TEST_CLIENT_HOST, TEST_OWNER) == NULL);
    assert(pthread_join(thread, NULL) == 0);
    assert(unlink(manifest_path) == 0);
    teardown_v1_server(&server);

    setup_v1_server(&server);
    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_peer_view_bad_path_%ld.yaml", (long)getpid());
    write_v1_manifest(manifest_path, "/tmp/vemb-v16-peer-view-missing",
                      server.response_path, server.warm_path, 1);
    assert(vemb_v16_ub_peer_view_manifest_load(manifest_path, &manifest) == 0);
    assert(pthread_create(&thread, NULL, v1_failure_server_main, &server) == 0);
    assert(vemb_v16_aeron_open_remote_with_peer_view(
               "127.0.0.1", server.port, TEST_DIM, &manifest,
               TEST_CLIENT_HOST, TEST_OWNER) == NULL);
    assert(pthread_join(thread, NULL) == 0);
    assert(unlink(manifest_path) == 0);
    teardown_v1_server(&server);
}

static void test_v2_peer_view_maps_all_resources(void) {
    peer_view_v2_fake_server_t server = {
        .listen_fd = vemb_v16_net_listen("127.0.0.1", 0, 8),
    };
    assert(server.listen_fd >= 0);
    server.port = listener_port(server.listen_fd);
    size_t descriptor_bytes = vemb_v16_client_ring_bytes(
        VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE);
    create_file(server.request_descriptor_path,
                sizeof(server.request_descriptor_path),
                "vemb_v16_peer_view_v2_req_desc", descriptor_bytes, 1,
                VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE);
    create_file(server.response_descriptor_path,
                sizeof(server.response_descriptor_path),
                "vemb_v16_peer_view_v2_resp_desc", descriptor_bytes, 1,
                VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE);
    create_file(server.request_arena_path, sizeof(server.request_arena_path),
                "vemb_v16_peer_view_v2_req_arena",
                VEMB_V16_BATCH_MAX_BYTES_DEFAULT, 0, 0);
    create_file(server.response_arena_path, sizeof(server.response_arena_path),
                "vemb_v16_peer_view_v2_resp_arena",
                VEMB_V16_BATCH_MAX_BYTES_DEFAULT, 0, 0);

    char manifest_path[128];
    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_peer_view_v2_%ld.yaml", (long)getpid());
    write_v2_manifest(manifest_path, &server);
    vemb_v16_ub_peer_view_manifest_t manifest;
    assert(vemb_v16_ub_peer_view_manifest_load(manifest_path, &manifest) == 0);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, v2_attach_server_main, &server) == 0);
    vemb_v16_aeron_batch_channel_t *channel =
        vemb_v16_aeron_open_remote_batch_with_peer_view(
            "127.0.0.1", server.port, TEST_DIM, 1,
            VEMB_V16_BATCH_MAX_BYTES_DEFAULT, &manifest, TEST_CLIENT_HOST,
            TEST_OWNER);
    assert(channel != NULL);
    vemb_v16_aeron_batch_resources_t resources;
    assert(vemb_v16_aeron_batch_get_resources(channel, &resources) == 0);
    assert(resources.request_descriptor_ring != NULL);
    assert(resources.request_arena != NULL);
    assert(resources.response_descriptor_ring != NULL);
    assert(resources.response_arena != NULL);
    assert(resources.descriptor_slot_size == VEMB_V16_BATCH_DESCRIPTOR_SLOT_SIZE);
    assert(resources.descriptor_ring_slots == VEMB_V16_CLIENT_RING_SIZE);
    assert(resources.effective_batch_size == 1);
    assert(resources.max_batch_bytes == VEMB_V16_BATCH_MAX_BYTES_DEFAULT);
    vemb_v16_aeron_batch_close(channel);
    assert(pthread_join(thread, NULL) == 0);

    assert(unlink(manifest_path) == 0);
    assert(unlink(server.request_descriptor_path) == 0);
    assert(unlink(server.response_descriptor_path) == 0);
    assert(unlink(server.request_arena_path) == 0);
    assert(unlink(server.response_arena_path) == 0);
}

static void test_sdk_cluster_handle_pipeline_uses_v2(void) {
    sdk_v2_fake_server_t server;
    setup_sdk_v2_server(&server);

    char manifest_path[128];
    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_peer_view_sdk_v2_%ld.yaml", (long)getpid());
    write_sdk_v2_manifest(manifest_path, &server);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, sdk_v2_server_main, &server) == 0);

    char seed[64];
    snprintf(seed, sizeof(seed), "127.0.0.1:%u",
             (unsigned)server.v1.port);
    const char *seeds[] = {seed};
    vemb_v16_client_t *client = vemb_v16_client_create(seeds, 1, TEST_DIM,
                                                        1000);
    assert(client != NULL);
    assert(vemb_v16_client_configure_ub_peer_view(
               client, manifest_path, TEST_CLIENT_HOST) == 0);

    const char *set_names[] = {"set", "set"};
    const char *elem_names[] = {"first", "second"};
    vemb_v16_pipeline_resp_t responses[2];
    assert(vemb_v16_client_vemb_handle_pipeline(
               client, set_names, elem_names, 2, responses, 2) == 0);
    assert(responses[0].status == 0);
    assert(responses[0].offset == 0);
    assert(responses[0].bytes == sizeof(float));
    assert(responses[0].dim == TEST_DIM);
    assert(responses[0].region_id == 7);
    assert(responses[1].status == 0);
    assert(responses[1].offset == sizeof(float));
    assert(responses[1].bytes == sizeof(float));
    assert(responses[1].dim == TEST_DIM);
    assert(responses[1].region_id == 7);
    vemb_v16_logical_stats_t logical_stats;
    vemb_v16_client_get_logical_stats(client, &logical_stats);
    assert(logical_stats.successes == 2);
    assert(logical_stats.not_found == 0);
    assert(logical_stats.errors == 0);
    vemb_v16_client_destroy(client);

    assert(pthread_join(thread, NULL) == 0);
    assert(unlink(manifest_path) == 0);
    teardown_sdk_v2_server(&server);
}

typedef struct handle_session_callbacks {
    uint64_t cookies[2];
    vemb_v16_pipeline_resp_t responses[2];
    uint32_t count;
} handle_session_callbacks_t;

static void handle_session_completion(void *priv, uint64_t caller_cookie,
                                      const vemb_v16_pipeline_resp_t *response)
{
    handle_session_callbacks_t *callbacks = priv;
    assert(callbacks->count < 2);
    callbacks->cookies[callbacks->count] = caller_cookie;
    callbacks->responses[callbacks->count++] = *response;
}

static void test_sdk_handle_session_coalesces_cross_call_l0(void) {
    sdk_v2_fake_server_t server;
    setup_sdk_v2_server(&server);

    char manifest_path[128];
    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_peer_view_sdk_l0_%ld.yaml", (long)getpid());
    write_sdk_v2_manifest(manifest_path, &server);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, sdk_v2_l0_coalesce_server_main,
                          &server) == 0);

    char seed[64];
    snprintf(seed, sizeof(seed), "127.0.0.1:%u",
             (unsigned)server.v1.port);
    const char *seeds[] = {seed};
    vemb_v16_client_t *client = vemb_v16_client_create(seeds, 1, TEST_DIM,
                                                        1000);
    assert(client != NULL);
    assert(vemb_v16_client_configure_ub_peer_view(
               client, manifest_path, TEST_CLIENT_HOST) == 0);
    vemb_v16_client_handle_session_t *session =
        vemb_v16_client_handle_session_create(client, NULL);
    assert(session != NULL);
    assert(vemb_v16_client_handle_session_submit(session, "set", "same", 11) ==
           0);
    assert(vemb_v16_client_handle_session_submit(session, "set", "same", 22) ==
           0);

    handle_session_callbacks_t callbacks = {0};
    for (uint32_t tries = 0; callbacks.count != 2 && tries < 10000; tries++) {
        assert(vemb_v16_client_handle_session_poll(
                   session, handle_session_completion, &callbacks) >= 0);
        if (callbacks.count != 2)
            usleep(1000);
    }
    assert(callbacks.count == 2);
    assert(callbacks.cookies[0] == 11);
    assert(callbacks.cookies[1] == 22);
    assert(callbacks.responses[0].status == 0);
    assert(callbacks.responses[1].status == 0);
    assert(callbacks.responses[0].offset == 0);
    assert(callbacks.responses[1].offset == 0);
    vemb_v16_logical_stats_t logical_stats;
    vemb_v16_client_get_logical_stats(client, &logical_stats);
    assert(logical_stats.successes == 2);
    assert(logical_stats.not_found == 0);
    assert(logical_stats.errors == 0);

    vemb_v16_client_handle_session_close(session, NULL, NULL);
    vemb_v16_client_destroy(client);
    assert(pthread_join(thread, NULL) == 0);
    assert(unlink(manifest_path) == 0);
    teardown_sdk_v2_server(&server);
}

static void test_sdk_handle_session_v2_attach_falls_back_to_ub_v1(void) {
    peer_view_fake_server_t server;
    setup_v1_server(&server);

    char manifest_path[128];
    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_peer_view_sdk_l0_reject_%ld.yaml",
             (long)getpid());
    write_v1_manifest(manifest_path, server.request_path, server.response_path,
                      server.warm_path, 1);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, sdk_handle_session_v2_reject_server_main,
                          &server) == 0);

    char seed[64];
    snprintf(seed, sizeof(seed), "127.0.0.1:%u", (unsigned)server.port);
    const char *seeds[] = {seed};
    vemb_v16_client_t *client = vemb_v16_client_create(seeds, 1, TEST_DIM,
                                                        1000);
    assert(client != NULL);
    assert(vemb_v16_client_configure_ub_peer_view(
               client, manifest_path, TEST_CLIENT_HOST) == 0);
    vemb_v16_client_handle_session_t *session =
        vemb_v16_client_handle_session_create(client, NULL);
    assert(session != NULL);
    assert(vemb_v16_client_handle_session_submit(session, "set", "first", 1) ==
           0);
    assert(vemb_v16_client_handle_session_submit(session, "set", "second", 2) ==
           0);

    handle_session_callbacks_t callbacks = {0};
    for (uint32_t tries = 0; callbacks.count != 2 && tries < 10000; tries++) {
        assert(vemb_v16_client_handle_session_poll(
                   session, handle_session_completion, &callbacks) >= 0);
        if (callbacks.count != 2)
            usleep(1000);
    }
    assert(callbacks.count == 2);
    assert(callbacks.responses[0].status == 0);
    assert(callbacks.responses[1].status == 0);
    vemb_v16_logical_stats_t logical_stats;
    vemb_v16_client_get_logical_stats(client, &logical_stats);
    assert(logical_stats.successes == 2);
    assert(logical_stats.not_found == 0);
    assert(logical_stats.errors == 0);

    vemb_v16_client_handle_session_close(session, NULL, NULL);
    vemb_v16_client_destroy(client);
    assert(pthread_join(thread, NULL) == 0);
    assert(unlink(manifest_path) == 0);
    teardown_v1_server(&server);
}

static void test_sdk_handle_session_quiesce_drains_pending_l0_to_ub_v1(void) {
    sdk_v2_fake_server_t server;
    setup_sdk_v2_server(&server);

    char manifest_path[128];
    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_peer_view_sdk_l0_quiesce_%ld.yaml",
             (long)getpid());
    write_sdk_v2_manifest(manifest_path, &server);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, sdk_handle_session_quiesce_server_main,
                          &server) == 0);

    char seed[64];
    snprintf(seed, sizeof(seed), "127.0.0.1:%u",
             (unsigned)server.v1.port);
    const char *seeds[] = {seed};
    vemb_v16_client_t *client = vemb_v16_client_create(seeds, 1, TEST_DIM,
                                                        1000);
    assert(client != NULL);
    assert(vemb_v16_client_configure_ub_peer_view(
               client, manifest_path, TEST_CLIENT_HOST) == 0);
    vemb_v16_client_handle_session_options_t options = {
        .max_batch_delay_us = 1000000,
    };
    vemb_v16_client_handle_session_t *session =
        vemb_v16_client_handle_session_create(client, &options);
    assert(session != NULL);
    assert(vemb_v16_client_handle_session_submit(session, "set", "pending", 9) ==
           0);
    handle_session_callbacks_t callbacks = {0};
    assert(vemb_v16_client_handle_session_poll(
               session, handle_session_completion, &callbacks) == 0);
    assert(callbacks.count == 0);

    assert(vemb_v16_client_topology_refresh(client) == 0);
    for (uint32_t tries = 0; callbacks.count != 1 && tries < 10000; tries++) {
        assert(vemb_v16_client_handle_session_poll(
                   session, handle_session_completion, &callbacks) >= 0);
        if (callbacks.count != 1)
            usleep(1000);
    }
    assert(callbacks.count == 1);
    assert(callbacks.cookies[0] == 9);
    assert(callbacks.responses[0].status == 0);
    vemb_v16_logical_stats_t logical_stats;
    vemb_v16_client_get_logical_stats(client, &logical_stats);
    assert(logical_stats.successes == 1);
    assert(logical_stats.not_found == 0);
    assert(logical_stats.errors == 0);

    vemb_v16_client_handle_session_close(session, NULL, NULL);
    vemb_v16_client_destroy(client);
    assert(pthread_join(thread, NULL) == 0);
    assert(unlink(manifest_path) == 0);
    teardown_sdk_v2_server(&server);
}

static void test_sdk_cluster_handle_pipeline_v2_attach_falls_back_v1(void) {
    peer_view_fake_server_t server;
    setup_v1_server(&server);

    char manifest_path[128];
    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_peer_view_sdk_v2_reject_%ld.yaml",
             (long)getpid());
    write_v1_manifest(manifest_path, server.request_path, server.response_path,
                      server.warm_path, 1);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, sdk_v2_attach_reject_server_main,
                          &server) == 0);

    char seed[64];
    snprintf(seed, sizeof(seed), "127.0.0.1:%u", (unsigned)server.port);
    const char *seeds[] = {seed};
    vemb_v16_client_t *client = vemb_v16_client_create(seeds, 1, TEST_DIM,
                                                        1000);
    assert(client != NULL);
    assert(vemb_v16_client_configure_ub_peer_view(
               client, manifest_path, TEST_CLIENT_HOST) == 0);

    const char *set_names[] = {"set"};
    const char *elem_names[] = {"fallback"};
    vemb_v16_pipeline_resp_t response;
    assert(vemb_v16_client_vemb_handle_pipeline(
               client, set_names, elem_names, 1, &response, 1) == 0);
    assert(response.status == 0);
    assert(response.offset == 0);
    assert(response.bytes == sizeof(float));
    assert(response.dim == TEST_DIM);
    assert(response.region_id == 7);
    vemb_v16_logical_stats_t logical_stats;
    vemb_v16_client_get_logical_stats(client, &logical_stats);
    assert(logical_stats.successes == 1);
    assert(logical_stats.not_found == 0);
    assert(logical_stats.errors == 0);
    vemb_v16_client_destroy(client);

    assert(pthread_join(thread, NULL) == 0);
    assert(unlink(manifest_path) == 0);
    teardown_v1_server(&server);
}

static void test_sdk_cluster_handle_pipeline_stale_retries_v1(void) {
    sdk_v2_fake_server_t server;
    setup_sdk_v2_server(&server);

    char manifest_path[128];
    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_peer_view_sdk_v2_stale_%ld.yaml",
             (long)getpid());
    write_sdk_v2_manifest(manifest_path, &server);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, sdk_v2_stale_server_main,
                          &server) == 0);

    char seed[64];
    snprintf(seed, sizeof(seed), "127.0.0.1:%u",
             (unsigned)server.v1.port);
    const char *seeds[] = {seed};
    vemb_v16_client_t *client = vemb_v16_client_create(seeds, 1, TEST_DIM,
                                                        1000);
    assert(client != NULL);
    assert(vemb_v16_client_configure_ub_peer_view(
               client, manifest_path, TEST_CLIENT_HOST) == 0);

    const char *set_names[] = {"set"};
    const char *elem_names[] = {"stale"};
    vemb_v16_pipeline_resp_t response;
    assert(vemb_v16_client_vemb_handle_pipeline(
               client, set_names, elem_names, 1, &response, 1) == 0);
    assert(response.status == 0);
    assert(response.offset == 0);
    assert(response.bytes == sizeof(float));
    assert(response.dim == TEST_DIM);
    assert(response.region_id == 7);
    vemb_v16_logical_stats_t logical_stats;
    vemb_v16_client_get_logical_stats(client, &logical_stats);
    assert(logical_stats.successes == 1);
    assert(logical_stats.not_found == 0);
    assert(logical_stats.errors == 0);
    vemb_v16_client_destroy(client);

    assert(pthread_join(thread, NULL) == 0);
    assert(unlink(manifest_path) == 0);
    teardown_sdk_v2_server(&server);
}

static void test_sdk_cluster_handle_pipeline_mixed_response_retries_only_stale(void) {
    sdk_v2_fake_server_t server;
    setup_sdk_v2_server(&server);

    char manifest_path[128];
    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_peer_view_sdk_v2_mixed_%ld.yaml",
             (long)getpid());
    write_sdk_v2_manifest(manifest_path, &server);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, sdk_v2_mixed_response_server_main,
                          &server) == 0);

    char seed[64];
    snprintf(seed, sizeof(seed), "127.0.0.1:%u",
             (unsigned)server.v1.port);
    const char *seeds[] = {seed};
    vemb_v16_client_t *client = vemb_v16_client_create(seeds, 1, TEST_DIM,
                                                        1000);
    assert(client != NULL);
    assert(vemb_v16_client_configure_ub_peer_view(
               client, manifest_path, TEST_CLIENT_HOST) == 0);

    const char *set_names[] = {"set", "set"};
    const char *elem_names[] = {"terminal", "stale"};
    vemb_v16_pipeline_resp_t responses[2];
    assert(vemb_v16_client_vemb_handle_pipeline(
               client, set_names, elem_names, 2, responses, 2) == 0);
    assert(responses[0].status == 0);
    assert(responses[0].offset == 0);
    assert(responses[1].status == 0);
    assert(responses[1].offset == 0);
    vemb_v16_logical_stats_t logical_stats;
    vemb_v16_client_get_logical_stats(client, &logical_stats);
    assert(logical_stats.successes == 2);
    assert(logical_stats.not_found == 0);
    assert(logical_stats.errors == 0);
    vemb_v16_client_destroy(client);

    assert(pthread_join(thread, NULL) == 0);
    assert(unlink(manifest_path) == 0);
    teardown_sdk_v2_server(&server);
}

static void test_sdk_cluster_handle_pipeline_ask_retries_v1(void) {
    sdk_v2_fake_server_t server;
    setup_sdk_v2_server(&server);

    char manifest_path[128];
    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_peer_view_sdk_v2_ask_%ld.yaml", (long)getpid());
    write_sdk_v2_manifest(manifest_path, &server);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, sdk_v2_ask_server_main, &server) == 0);

    char seed[64];
    snprintf(seed, sizeof(seed), "127.0.0.1:%u",
             (unsigned)server.v1.port);
    const char *seeds[] = {seed};
    vemb_v16_client_t *client = vemb_v16_client_create(seeds, 1, TEST_DIM,
                                                        1000);
    assert(client != NULL);
    assert(vemb_v16_client_configure_ub_peer_view(
               client, manifest_path, TEST_CLIENT_HOST) == 0);

    const char *set_names[] = {"set"};
    const char *elem_names[] = {"ask"};
    vemb_v16_pipeline_resp_t response;
    assert(vemb_v16_client_vemb_handle_pipeline(
               client, set_names, elem_names, 1, &response, 1) == 0);
    assert(response.status == 0);
    assert(response.offset == 0);
    vemb_v16_logical_stats_t logical_stats;
    vemb_v16_client_get_logical_stats(client, &logical_stats);
    assert(logical_stats.successes == 1);
    assert(logical_stats.not_found == 0);
    assert(logical_stats.errors == 0);
    vemb_v16_client_destroy(client);

    assert(pthread_join(thread, NULL) == 0);
    assert(unlink(manifest_path) == 0);
    teardown_sdk_v2_server(&server);
}

static void test_sdk_cluster_handle_pipeline_migration_recovers_v2(void) {
    sdk_v2_fake_server_t server;
    setup_sdk_v2_server(&server);

    char manifest_path[128];
    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_peer_view_sdk_v2_migration_%ld.yaml",
             (long)getpid());
    write_sdk_v2_manifest(manifest_path, &server);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, sdk_v2_migration_recovery_server_main,
                          &server) == 0);

    char seed[64];
    snprintf(seed, sizeof(seed), "127.0.0.1:%u",
             (unsigned)server.v1.port);
    const char *seeds[] = {seed};
    vemb_v16_client_t *client = vemb_v16_client_create(seeds, 1, TEST_DIM,
                                                        1000);
    assert(client != NULL);
    assert(vemb_v16_client_configure_ub_peer_view(
               client, manifest_path, TEST_CLIENT_HOST) == 0);

    const char *set_names[] = {"set"};
    const char *elem_names[] = {"migration"};
    vemb_v16_pipeline_resp_t response;
    assert(vemb_v16_client_vemb_handle_pipeline(
               client, set_names, elem_names, 1, &response, 1) == 0);
    assert(response.status == 0);
    assert(vemb_v16_client_topology_refresh(client) == 0);
    assert(vemb_v16_client_vemb_handle_pipeline(
               client, set_names, elem_names, 1, &response, 1) == 0);
    assert(response.status == 0);
    assert(response.offset == 0);
    vemb_v16_logical_stats_t logical_stats;
    vemb_v16_client_get_logical_stats(client, &logical_stats);
    assert(logical_stats.successes == 2);
    assert(logical_stats.not_found == 0);
    assert(logical_stats.errors == 0);
    vemb_v16_client_destroy(client);

    assert(pthread_join(thread, NULL) == 0);
    assert(unlink(manifest_path) == 0);
    teardown_sdk_v2_server(&server);
}

int main(void) {
    test_v1_peer_view_data_and_reattach();
    test_sdk_cluster_uses_peer_view_for_matching_owner();
    test_sdk_cluster_keeps_unmapped_owner_direct();
    test_v1_peer_view_failure_closes_before_publication();
    test_v2_peer_view_maps_all_resources();
    test_sdk_cluster_handle_pipeline_uses_v2();
    test_sdk_handle_session_coalesces_cross_call_l0();
    test_sdk_handle_session_v2_attach_falls_back_to_ub_v1();
    test_sdk_handle_session_quiesce_drains_pending_l0_to_ub_v1();
    test_sdk_cluster_handle_pipeline_v2_attach_falls_back_v1();
    test_sdk_cluster_handle_pipeline_stale_retries_v1();
    test_sdk_cluster_handle_pipeline_mixed_response_retries_only_stale();
    test_sdk_cluster_handle_pipeline_ask_retries_v1();
    test_sdk_cluster_handle_pipeline_migration_recovers_v2();
    printf("vemb_v16_peer_view_transport_ut: all tests passed\n");
    return 0;
}
