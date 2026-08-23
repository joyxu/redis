#include "../clients/c/vemb_v16_client_sdk.h"
#include "../src/vemb_v16_aeron_attach.h"
#include "../src/vemb_v16_client_ring.h"
#include "../src/vemb_v16_net.h"

#include <assert.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct fake_ub_server {
    int listen_fd;
    uint16_t port;
    uint32_t request_slot_size;
    uint32_t response_slot_size;
    char request_path[256];
    char response_path[256];
    void *request_mapping;
    size_t request_mapping_bytes;
    void *response_mapping;
    size_t response_mapping_bytes;
    char warm_path[256];
    void *warm_mapping;
    size_t warm_mapping_bytes;
    uint32_t warm_region_id;
    int corrupt_response_identity;
    int serve_handle_read;
    uint8_t redirect_status;
} fake_ub_server_t;

static int accept_connection(int listen_fd)
{
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);
    int fd = accept(listen_fd, (struct sockaddr *)&addr, &addr_len);

    assert(fd >= 0);
    return fd;
}

static uint16_t listener_port(int fd)
{
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);

    assert(getsockname(fd, (struct sockaddr *)&addr, &addr_len) == 0);
    return ntohs(addr.sin_port);
}

static void write_local_peer_view_manifest(
    const fake_ub_server_t *server, const char *path)
{
    FILE *fp = fopen(path, "w");
    assert(fp != NULL);
    fprintf(fp,
            "version: 1\n"
            "peer_views:\n"
            "  - client_host: local\n"
            "    owner_id: 0\n"
            "    resource_role: v1_request_ring\n"
            "    resource_id: request\n"
            "    generation: 1\n"
            "    provider_path: %s\n"
            "    client_path: %s\n"
            "    map_from_start: true\n"
            "    cache_policy: noncacheable\n"
            "  - client_host: local\n"
            "    owner_id: 0\n"
            "    resource_role: v1_response_ring\n"
            "    resource_id: response\n"
            "    generation: 1\n"
            "    provider_path: %s\n"
            "    client_path: %s\n"
            "    map_from_start: true\n"
            "    cache_policy: cacheable\n",
            server->request_path, server->request_path,
            server->response_path, server->response_path);
    if (server->serve_handle_read) {
        fprintf(fp,
                "  - client_host: local\n"
                "    owner_id: 0\n"
                "    resource_role: warm_region\n"
                "    resource_id: warm\n"
                "    generation: 1\n"
                "    provider_path: %s\n"
                "    client_path: %s\n"
                "    map_from_start: true\n"
                "    cache_policy: noncacheable\n",
                server->warm_path, server->warm_path);
    }
    assert(fclose(fp) == 0);
}

static vemb_v16_client_t *create_ub_client(const fake_ub_server_t *server)
{
    char seed[64];
    const char *seeds[] = {seed};
    char manifest_path[128];

    snprintf(seed, sizeof(seed), "127.0.0.1:%u", (unsigned)server->port);
    snprintf(manifest_path, sizeof(manifest_path),
             "/tmp/vemb_v16_ub_transport_%ld_%u.yaml", (long)getpid(),
             (unsigned)server->port);
    write_local_peer_view_manifest(server, manifest_path);
    vemb_v16_client_t *client = vemb_v16_client_create(
        seeds, 1, 1, 1000, VEMB_V16_TRANSPORT_AERON);
    if (client && vemb_v16_client_configure_ub_peer_view(
                      client, manifest_path, "local") != 0) {
        vemb_v16_client_destroy(client);
        client = NULL;
    }
    assert(unlink(manifest_path) == 0);
    return client;
}

static void write_aeron_topology_epoch(int fd, uint16_t port, uint64_t epoch)
{
    vemb_v16_net_hdr_t header;
    vemb_v16_topology_control_resp_t response = {
        .status = VEMB_V16_STATUS_OK,
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
    response.active_owners[0] = 0;
    response.standby_owners[0] = 0;
    response.endpoints[0].owner_id = 0;
    response.endpoints[0].transport_type = VEMB_V16_TRANSPORT_AERON;
    response.endpoints[0].tcp_port = port;
    snprintf(response.endpoints[0].host,
             sizeof(response.endpoints[0].host), "127.0.0.1");
    assert(vemb_v16_topology_control_resp_encode(payload, sizeof(payload),
                                                  &response, &payload_len) == 0);
    assert(vemb_v16_net_write_frame(fd, VEMB_V16_NET_TOPOLOGY_RESPONSE, 0,
                                    0, 0, payload, (uint32_t)payload_len) == 0);
}

static void write_aeron_topology(int fd, uint16_t port)
{
    write_aeron_topology_epoch(fd, port, 42);
}

static void write_attach_response_with_channel(int fd,
                                               const fake_ub_server_t *server,
                                               uint64_t channel_id)
{
    vemb_v16_aeron_attach_req_t request;
    vemb_v16_aeron_attach_resp_t response = {
        .status = 0,
        .channel_id = channel_id,
        .ring_size_slots = VEMB_V16_CLIENT_RING_SIZE,
        .request_shmdev_path_len = (uint32_t)(strlen(server->request_path) + 1),
        .response_shmdev_path_len = (uint32_t)(strlen(server->response_path) + 1),
        .req_backend_type = VEMB_V16_REGION_UB,
        .resp_backend_type = VEMB_V16_REGION_UB,
        .req_slot_size = server->request_slot_size,
        .resp_slot_size = server->response_slot_size,
    };

    assert(vemb_v16_net_read_full(fd, &request, sizeof(request)) == 0);
    assert(memcmp(request.magic, VEMB_V16_AERON_ATTACH_MAGIC,
                  VEMB_V16_AERON_ATTACH_MAGIC_LEN) == 0);
    assert(request.dim == 1);
    memcpy(response.magic, VEMB_V16_AERON_ATTACHED_MAGIC,
           VEMB_V16_AERON_ATTACHED_MAGIC_LEN);
    memcpy(response.request_shmdev_path, server->request_path,
           response.request_shmdev_path_len);
    memcpy(response.response_shmdev_path, server->response_path,
           response.response_shmdev_path_len);
    if (server->serve_handle_read) {
        response.warm_region_count = 1;
        response.warm_region_id = server->warm_region_id;
        response.warm_backend_type = VEMB_V16_REGION_UB;
        response.warm_region_bytes = server->warm_mapping_bytes;
        response.warm_path_len = (uint32_t)(strlen(server->warm_path) + 1);
        memcpy(response.warm_path, server->warm_path, response.warm_path_len);
    }
    assert(vemb_v16_net_write_full(fd, &response, sizeof(response)) == 0);
}

static void write_attach_response(int fd, const fake_ub_server_t *server)
{
    write_attach_response_with_channel(fd, server, 0xabcdu);
}

static void publish_vsim_response(vemb_v16_client_ring_t *response_ring,
                                  const vemb_v16_req_t *request,
                                  uint32_t req_id, float score)
{
    vemb_v16_resp_t response = {
        .status = VEMB_V16_STATUS_OK,
        .op = VEMB_V16_OP_VSIM_INLINE,
        .req_id = req_id,
        .score = score,
    };
    uint8_t wire[VEMB_V16_AERON_RESP_WIRE_MAX_LEN];
    size_t wire_len = 0;

    assert(request->op == VEMB_V16_OP_VSIM_INLINE);
    assert(vemb_v16_resp_encode(wire, sizeof(wire), &response, &wire_len) == 0);
    assert(vemb_v16_client_publish(response_ring, wire, (uint32_t)wire_len) == 0);
}

static void publish_redirect_response(vemb_v16_client_ring_t *response_ring,
                                      const vemb_v16_req_t *request,
                                      uint8_t status)
{
    vemb_v16_resp_t response = {
        .status = status,
        .op = VEMB_V16_OP_VSIM_INLINE,
        .req_id = request->req_id,
        .redirect_owner = 0,
    };
    uint8_t wire[VEMB_V16_AERON_RESP_WIRE_MAX_LEN];
    size_t wire_len = 0;

    assert(status == VEMB_V16_STATUS_ASK ||
           status == VEMB_V16_STATUS_MOVED ||
           status == VEMB_V16_STATUS_STALE_TOPOLOGY);
    assert(request->op == VEMB_V16_OP_VSIM_INLINE);
    assert(vemb_v16_resp_encode(wire, sizeof(wire), &response, &wire_len) == 0);
    assert(vemb_v16_client_publish(response_ring, wire, (uint32_t)wire_len) == 0);
}

static void publish_handle_response(vemb_v16_client_ring_t *response_ring,
                                    const vemb_v16_req_t *request,
                                    uint32_t region_id)
{
    vemb_v16_resp_t response = {
        .status = VEMB_V16_STATUS_OK,
        .op = VEMB_V16_OP_VEMB_HANDLE,
        .req_id = request->req_id,
        .vector_bytes = sizeof(float),
        .dim = 1,
        .region_id = region_id,
        .owner_generation = 1,
    };
    uint8_t wire[VEMB_V16_AERON_RESP_WIRE_MAX_LEN];
    size_t wire_len = 0;

    assert(request->op == VEMB_V16_OP_VEMB_HANDLE);
    assert(vemb_v16_resp_encode(wire, sizeof(wire), &response, &wire_len) == 0);
    assert(vemb_v16_client_publish(response_ring, wire, (uint32_t)wire_len) == 0);
}

static void read_close_channel(int fd, uint64_t expected_channel_id)
{
    vemb_v16_net_hdr_t header;

    assert(vemb_v16_net_read_header(fd, &header) == 0);
    assert(header.type == VEMB_V16_NET_CLOSE_CHANNEL);
    assert(header.channel_id == expected_channel_id);
    assert(header.payload_len == 0);
}

static void write_control_status(int fd, uint8_t status, uint64_t value)
{
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
                                 uint64_t resource_generation)
{
    vemb_v16_net_hdr_t header;

    assert(vemb_v16_net_read_header(fd, &header) == 0);
    assert(header.type == VEMB_V16_NET_AERON_CHANNEL_STATUS);
    assert(header.channel_id == expected_channel_id);
    assert(header.payload_len == 0);
    write_control_status(fd, VEMB_V16_STATUS_OK, resource_generation);
}

static void read_one_request(vemb_v16_client_ring_t *request_ring,
                             vemb_v16_req_t *out)
{
    for (;;) {
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
}

static void *fake_ub_server_main(void *arg)
{
    fake_ub_server_t *server = arg;

    /* The seed carries control only. Its first connection must be a topology
     * request, proving create()/topology_refresh() did not send TCP HELLO. */
    int topology_fd = accept_connection(server->listen_fd);
    write_aeron_topology(topology_fd, server->port);
    close(topology_fd);

    int attach_fd = accept_connection(server->listen_fd);
    write_attach_response(attach_fd, server);
    close(attach_fd);

    vemb_v16_client_ring_t *request_ring = server->request_mapping;
    vemb_v16_client_ring_t *response_ring = server->response_mapping;
    vemb_v16_req_t requests[2];
    uint32_t request_count = 0;
    uint32_t expected_requests = server->serve_handle_read ? 1 : 2;
    while (request_count < expected_requests) {
        uint8_t wire[VEMB_V16_AERON_REQ_WIRE_MAX_LEN];
        int got = vemb_v16_client_poll(request_ring, wire, sizeof(wire));
        if (got == 0) {
            usleep(1000);
            continue;
        }
        assert(got > 0);
        assert(vemb_v16_req_decode(&requests[request_count], wire,
                                   (size_t)got) == 0);
        request_count++;
    }

    if (server->serve_handle_read) {
        publish_handle_response(response_ring, &requests[0],
                                server->warm_region_id);
    } else if (server->corrupt_response_identity) {
        publish_vsim_response(response_ring, &requests[0],
                              requests[0].req_id + 100u, 1.0f);
    } else {
        publish_vsim_response(response_ring, &requests[1], requests[1].req_id,
                              22.0f);
        publish_vsim_response(response_ring, &requests[0], requests[0].req_id,
                              11.0f);
    }

    int close_fd = accept_connection(server->listen_fd);
    read_close_channel(close_fd, 0xabcdu);
    close(close_fd);
    close(server->listen_fd);
    return NULL;
}

/* The resource status is only queried after a topology refresh. This server
 * proves that a stable generation keeps the same ATTACH, while a changed
 * generation fences the already-drained owner channel, closes it and opens a
 * new UB allocation without replaying an earlier request. */
static void *fake_ub_resource_lifecycle_server_main(void *arg)
{
    fake_ub_server_t *server = arg;
    int topology_fd = accept_connection(server->listen_fd);
    write_aeron_topology_epoch(topology_fd, server->port, 42);
    close(topology_fd);

    int attach_fd = accept_connection(server->listen_fd);
    write_attach_response_with_channel(attach_fd, server, 0xabcdu);
    close(attach_fd);

    vemb_v16_client_ring_t *request_ring = server->request_mapping;
    vemb_v16_client_ring_t *response_ring = server->response_mapping;
    vemb_v16_req_t request;
    read_one_request(request_ring, &request);
    publish_handle_response(response_ring, &request, server->warm_region_id);

    topology_fd = accept_connection(server->listen_fd);
    write_aeron_topology_epoch(topology_fd, server->port, 43);
    close(topology_fd);

    int status_fd = accept_connection(server->listen_fd);
    read_resource_status(status_fd, 0xabcdu, 0xabcdu);
    close(status_fd);

    read_one_request(request_ring, &request);
    publish_vsim_response(response_ring, &request, request.req_id, 11.0f);

    topology_fd = accept_connection(server->listen_fd);
    write_aeron_topology_epoch(topology_fd, server->port, 44);
    close(topology_fd);

    status_fd = accept_connection(server->listen_fd);
    read_resource_status(status_fd, 0xabcdu, 0xdef0u);
    close(status_fd);

    int close_fd = accept_connection(server->listen_fd);
    read_close_channel(close_fd, 0xabcdu);
    close(close_fd);

    attach_fd = accept_connection(server->listen_fd);
    write_attach_response_with_channel(attach_fd, server, 0xabceu);
    close(attach_fd);

    read_one_request(request_ring, &request);
    publish_vsim_response(response_ring, &request, request.req_id, 22.0f);

    close_fd = accept_connection(server->listen_fd);
    read_close_channel(close_fd, 0xabceu);
    close(close_fd);
    close(server->listen_fd);
    return NULL;
}

/* Redirect responses are intermediate protocol state. The terminal request
 * must reuse the live UB allocation; MOVED/STALE additionally refresh owner
 * routing through the TCP bootstrap control path. */
static void *fake_ub_redirect_server_main(void *arg)
{
    fake_ub_server_t *server = arg;
    int topology_fd = accept_connection(server->listen_fd);
    write_aeron_topology_epoch(topology_fd, server->port, 42);
    close(topology_fd);

    int attach_fd = accept_connection(server->listen_fd);
    write_attach_response(attach_fd, server);
    close(attach_fd);

    vemb_v16_req_t request;
    read_one_request(server->request_mapping, &request);
    assert(request.flags == 0);
    assert(request.topology_epoch == 42);
    publish_redirect_response(server->response_mapping, &request,
                              server->redirect_status);

    if (server->redirect_status == VEMB_V16_STATUS_ASK) {
        read_one_request(server->request_mapping, &request);
        assert(request.flags == VEMB_V16_REQ_F_ASK_REDIRECT);
        assert(request.topology_epoch == 42);
    } else {
        topology_fd = accept_connection(server->listen_fd);
        write_aeron_topology_epoch(topology_fd, server->port, 43);
        close(topology_fd);

        int status_fd = accept_connection(server->listen_fd);
        read_resource_status(status_fd, 0xabcdu, 0xabcdu);
        close(status_fd);

        read_one_request(server->request_mapping, &request);
        assert(request.flags == 0);
        assert(request.topology_epoch == 43);
    }
    publish_vsim_response(server->response_mapping, &request, request.req_id,
                          37.0f);

    int close_fd = accept_connection(server->listen_fd);
    read_close_channel(close_fd, 0xabcdu);
    close(close_fd);
    close(server->listen_fd);
    return NULL;
}

static void create_ring_file(char *path, size_t path_cap, const char *prefix,
                             uint32_t slot_size, void **out_mapping,
                             size_t *out_mapping_bytes)
{
    int path_len = snprintf(path, path_cap, "/tmp/%s_XXXXXX", prefix);
    assert(path_len > 0 && (size_t)path_len < path_cap);
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

static void create_warm_file(char *path, size_t path_cap, void **out_mapping,
                             size_t *out_mapping_bytes, float value)
{
    int path_len = snprintf(path, path_cap, "/tmp/vemb_v16_ub_warm_XXXXXX");
    assert(path_len > 0 && (size_t)path_len < path_cap);
    int fd = mkstemp(path);
    assert(fd >= 0);
    assert(ftruncate(fd, (off_t)sizeof(value)) == 0);

    void *mapping = mmap(NULL, sizeof(value), PROT_READ | PROT_WRITE,
                         MAP_SHARED, fd, 0);
    assert(mapping != MAP_FAILED);
    close(fd);
    *(float *)mapping = value;
    *out_mapping = mapping;
    *out_mapping_bytes = sizeof(value);
}

static void test_aeron_backend_routes_and_matches_completions(int corrupt)
{
    fake_ub_server_t server = {
        .listen_fd = vemb_v16_net_listen("127.0.0.1", 0, 8),
        .request_slot_size = vemb_v16_aeron_req_slot_size(1),
        .response_slot_size = vemb_v16_aeron_resp_slot_size(),
        .corrupt_response_identity = corrupt,
    };
    if (server.listen_fd < 0)
        perror("vemb_v16_net_listen");
    assert(server.listen_fd >= 0);
    server.port = listener_port(server.listen_fd);
    create_ring_file(server.request_path, sizeof(server.request_path),
                     "vemb_v16_ub_req", server.request_slot_size,
                     &server.request_mapping, &server.request_mapping_bytes);
    create_ring_file(server.response_path, sizeof(server.response_path),
                     "vemb_v16_ub_resp", server.response_slot_size,
                     &server.response_mapping, &server.response_mapping_bytes);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, fake_ub_server_main, &server) == 0);

    vemb_v16_client_t *client = create_ub_client(&server);
    assert(client != NULL);
    assert(vemb_v16_client_topology_refresh(client) == 0);

    const char *sets[] = {"set", "set"};
    const char *elements[] = {"first", "second"};
    const float query[] = {1.0f};
    float scores[2] = {0};
    int rc = vemb_v16_client_vsim_pipeline(client, sets, elements, query, 2,
                                            scores, 2);
    if (corrupt) {
        assert(rc != 0);
    } else {
        assert(rc == 0);
        assert(scores[0] == 11.0f);
        assert(scores[1] == 22.0f);
    }

    vemb_v16_client_destroy(client);
    assert(pthread_join(thread, NULL) == 0);
    assert(munmap(server.request_mapping, server.request_mapping_bytes) == 0);
    assert(munmap(server.response_mapping, server.response_mapping_bytes) == 0);
    assert(unlink(server.request_path) == 0);
    assert(unlink(server.response_path) == 0);
}

static void test_aeron_backend_materializes_owner_warm_handle(void)
{
    fake_ub_server_t server = {
        .listen_fd = vemb_v16_net_listen("127.0.0.1", 0, 8),
        .request_slot_size = vemb_v16_aeron_req_slot_size(1),
        .response_slot_size = vemb_v16_aeron_resp_slot_size(),
        .warm_region_id = 7,
        .serve_handle_read = 1,
    };
    assert(server.listen_fd >= 0);
    server.port = listener_port(server.listen_fd);
    create_ring_file(server.request_path, sizeof(server.request_path),
                     "vemb_v16_ub_req", server.request_slot_size,
                     &server.request_mapping, &server.request_mapping_bytes);
    create_ring_file(server.response_path, sizeof(server.response_path),
                     "vemb_v16_ub_resp", server.response_slot_size,
                     &server.response_mapping, &server.response_mapping_bytes);
    create_warm_file(server.warm_path, sizeof(server.warm_path),
                     &server.warm_mapping, &server.warm_mapping_bytes, 73.0f);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, fake_ub_server_main, &server) == 0);

    vemb_v16_client_t *client = create_ub_client(&server);
    assert(client != NULL);
    assert(vemb_v16_client_topology_refresh(client) == 0);
    float vector = 0;
    uint32_t dim = 0;
    assert(vemb_v16_client_vemb_vector(client, "set", "warm", &vector, 1,
                                       &dim) == 0);
    assert(vector == 73.0f);
    assert(dim == 1);

    vemb_v16_client_destroy(client);
    assert(pthread_join(thread, NULL) == 0);
    assert(munmap(server.request_mapping, server.request_mapping_bytes) == 0);
    assert(munmap(server.response_mapping, server.response_mapping_bytes) == 0);
    assert(munmap(server.warm_mapping, server.warm_mapping_bytes) == 0);
    assert(unlink(server.request_path) == 0);
    assert(unlink(server.response_path) == 0);
    assert(unlink(server.warm_path) == 0);
}

typedef struct ub_vector_session_completion {
    uint64_t cookies[2];
    float vectors[2];
    uint32_t count;
} ub_vector_session_completion_t;

static void record_ub_vector_session_completion(
    void *priv, uint64_t caller_cookie,
    const vemb_v16_pipeline_resp_t *response, const float *vector)
{
    ub_vector_session_completion_t *completion = priv;
    assert(completion->count < 2);
    assert(response->status == 0);
    assert(response->dim == 1);
    assert(vector != NULL);
    completion->cookies[completion->count] = caller_cookie;
    completion->vectors[completion->count] = vector[0];
    completion->count++;
}

static void test_ub_vector_session_coalesces_handle_read(void)
{
    fake_ub_server_t server = {
        .listen_fd = vemb_v16_net_listen("127.0.0.1", 0, 8),
        .request_slot_size = vemb_v16_aeron_req_slot_size(1),
        .response_slot_size = vemb_v16_aeron_resp_slot_size(),
        .warm_region_id = 7,
        .serve_handle_read = 1,
    };
    assert(server.listen_fd >= 0);
    server.port = listener_port(server.listen_fd);
    create_ring_file(server.request_path, sizeof(server.request_path),
                     "vemb_v16_ub_vector_session_req",
                     server.request_slot_size, &server.request_mapping,
                     &server.request_mapping_bytes);
    create_ring_file(server.response_path, sizeof(server.response_path),
                     "vemb_v16_ub_vector_session_resp",
                     server.response_slot_size, &server.response_mapping,
                     &server.response_mapping_bytes);
    create_warm_file(server.warm_path, sizeof(server.warm_path),
                     &server.warm_mapping, &server.warm_mapping_bytes, 79.0f);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, fake_ub_server_main, &server) == 0);

    vemb_v16_client_t *client = create_ub_client(&server);
    assert(client != NULL);
    vemb_v16_client_vector_session_t *session =
        vemb_v16_client_vector_session_create(client);
    assert(session != NULL);
    assert(vemb_v16_client_vector_session_submit(session, "set", "warm",
                                                  31) == 0);
    assert(vemb_v16_client_vector_session_submit(session, "set", "warm",
                                                  32) == 0);

    ub_vector_session_completion_t completion = {0};
    for (uint32_t attempt = 0; attempt < 1000 && completion.count != 2;
         attempt++) {
        assert(vemb_v16_client_vector_session_poll(
                   session, record_ub_vector_session_completion, &completion) >=
               0);
        if (completion.count != 2)
            usleep(1000);
    }
    assert(completion.count == 2);
    assert(completion.cookies[0] == 31);
    assert(completion.cookies[1] == 32);
    assert(completion.vectors[0] == 79.0f);
    assert(completion.vectors[1] == 79.0f);
    vemb_v16_logical_stats_t stats;
    vemb_v16_client_get_logical_stats(client, &stats);
    assert(stats.successes == 2);
    assert(stats.errors == 0);

    vemb_v16_client_vector_session_close(session, NULL, NULL);
    vemb_v16_client_destroy(client);
    assert(pthread_join(thread, NULL) == 0);
    assert(munmap(server.request_mapping, server.request_mapping_bytes) == 0);
    assert(munmap(server.response_mapping, server.response_mapping_bytes) == 0);
    assert(munmap(server.warm_mapping, server.warm_mapping_bytes) == 0);
    assert(unlink(server.request_path) == 0);
    assert(unlink(server.response_path) == 0);
    assert(unlink(server.warm_path) == 0);
}

static void test_aeron_backend_rechecks_resource_on_new_topology_epoch(void)
{
    fake_ub_server_t server = {
        .listen_fd = vemb_v16_net_listen("127.0.0.1", 0, 8),
        .request_slot_size = vemb_v16_aeron_req_slot_size(1),
        .response_slot_size = vemb_v16_aeron_resp_slot_size(),
        .warm_region_id = 7,
        .serve_handle_read = 1,
    };
    assert(server.listen_fd >= 0);
    server.port = listener_port(server.listen_fd);
    create_ring_file(server.request_path, sizeof(server.request_path),
                     "vemb_v16_ub_lifecycle_req", server.request_slot_size,
                     &server.request_mapping, &server.request_mapping_bytes);
    create_ring_file(server.response_path, sizeof(server.response_path),
                     "vemb_v16_ub_lifecycle_resp", server.response_slot_size,
                     &server.response_mapping, &server.response_mapping_bytes);
    create_warm_file(server.warm_path, sizeof(server.warm_path),
                     &server.warm_mapping, &server.warm_mapping_bytes, 73.0f);

    pthread_t thread;
    assert(pthread_create(&thread, NULL,
                          fake_ub_resource_lifecycle_server_main,
                          &server) == 0);

    vemb_v16_client_t *client = create_ub_client(&server);
    assert(client != NULL);
    assert(vemb_v16_client_topology_refresh(client) == 0);
    uint64_t offset = 0;
    uint32_t bytes = 0;
    uint32_t dim = 0;
    assert(vemb_v16_client_vemb_handle(client, "set", "warm", &offset,
                                       &bytes, &dim, NULL) == 0);
    assert(offset == 0 && bytes == sizeof(float) && dim == 1);

    assert(vemb_v16_client_topology_refresh(client) == 0);
    float query[] = {1.0f};
    float score = 0.0f;
    assert(vemb_v16_client_vsim(client, "set", "stable", query, 1,
                                &score) == 0);
    assert(score == 11.0f);

    assert(vemb_v16_client_topology_refresh(client) == 0);
    assert(vemb_v16_client_vsim(client, "set", "reattach", query, 1,
                                &score) == 0);
    assert(score == 22.0f);
    float stale = 0.0f;
    assert(vemb_v16_client_read_vector(client, offset, bytes, &stale, 1) != 0);

    vemb_v16_client_destroy(client);
    assert(pthread_join(thread, NULL) == 0);
    assert(munmap(server.request_mapping, server.request_mapping_bytes) == 0);
    assert(munmap(server.response_mapping, server.response_mapping_bytes) == 0);
    assert(munmap(server.warm_mapping, server.warm_mapping_bytes) == 0);
    assert(unlink(server.request_path) == 0);
    assert(unlink(server.response_path) == 0);
    assert(unlink(server.warm_path) == 0);
}

static void test_aeron_backend_redirect(uint8_t redirect_status)
{
    fake_ub_server_t server = {
        .listen_fd = vemb_v16_net_listen("127.0.0.1", 0, 8),
        .request_slot_size = vemb_v16_aeron_req_slot_size(1),
        .response_slot_size = vemb_v16_aeron_resp_slot_size(),
        .redirect_status = redirect_status,
    };
    assert(server.listen_fd >= 0);
    server.port = listener_port(server.listen_fd);
    create_ring_file(server.request_path, sizeof(server.request_path),
                     "vemb_v16_ub_redirect_req", server.request_slot_size,
                     &server.request_mapping, &server.request_mapping_bytes);
    create_ring_file(server.response_path, sizeof(server.response_path),
                     "vemb_v16_ub_redirect_resp", server.response_slot_size,
                     &server.response_mapping, &server.response_mapping_bytes);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, fake_ub_redirect_server_main,
                          &server) == 0);
    vemb_v16_client_t *client = create_ub_client(&server);
    assert(client != NULL);
    assert(vemb_v16_client_topology_refresh(client) == 0);
    float query[] = {1.0f};
    float score = 0.0f;
    assert(vemb_v16_client_vsim(client, "set", "redirect", query, 1,
                                &score) == 0);
    assert(score == 37.0f);

    vemb_v16_redirect_stats_t stats = {0};
    vemb_v16_client_get_redirect_stats(client, &stats);
    assert(stats.ask_redirects ==
           (redirect_status == VEMB_V16_STATUS_ASK ? 1 : 0));
    assert(stats.moved_redirects ==
           (redirect_status == VEMB_V16_STATUS_MOVED ? 1 : 0));
    assert(stats.stale_topology_responses ==
           (redirect_status == VEMB_V16_STATUS_STALE_TOPOLOGY ? 1 : 0));
    assert(stats.topology_refresh_calls ==
           (redirect_status == VEMB_V16_STATUS_ASK ? 1 : 2));

    vemb_v16_client_destroy(client);
    assert(pthread_join(thread, NULL) == 0);
    assert(munmap(server.request_mapping, server.request_mapping_bytes) == 0);
    assert(munmap(server.response_mapping, server.response_mapping_bytes) == 0);
    assert(unlink(server.request_path) == 0);
    assert(unlink(server.response_path) == 0);
}

int main(void)
{
    test_aeron_backend_routes_and_matches_completions(0);
    test_aeron_backend_routes_and_matches_completions(1);
    test_aeron_backend_materializes_owner_warm_handle();
    test_ub_vector_session_coalesces_handle_read();
    test_aeron_backend_rechecks_resource_on_new_topology_epoch();
    test_aeron_backend_redirect(VEMB_V16_STATUS_ASK);
    test_aeron_backend_redirect(VEMB_V16_STATUS_MOVED);
    test_aeron_backend_redirect(VEMB_V16_STATUS_STALE_TOPOLOGY);
    printf("vemb_v16_ub_data_transport_ut: all tests passed\n");
    return 0;
}
