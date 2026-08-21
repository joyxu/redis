#include "../clients/c/vemb_v16_client_sdk.h"
#include "../src/vemb_v16_net.h"

#include <assert.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

typedef struct fake_tcp_server {
    int listen_fd;
    uint16_t port;
    uint64_t channel_id;
} fake_tcp_server_t;

static void read_topology_get(int fd);
static void write_topology_response(int fd, uint64_t epoch, uint16_t port);

static void read_request(int fd, vemb_v16_req_t *request)
{
    vemb_v16_net_hdr_t header;
    uint8_t payload[512];

    assert(vemb_v16_net_read_header(fd, &header) == 0);
    assert(header.type == VEMB_V16_NET_REQUEST);
    assert(header.payload_len <= sizeof(payload));
    assert(vemb_v16_net_read_full(fd, payload, header.payload_len) == 0);
    assert(vemb_v16_req_decode(request, payload, header.payload_len) == 0);
    assert(request->channel_id == header.channel_id);
    assert(request->req_id == header.req_id);
}

static void write_inline_response(int fd,
                                  uint64_t channel_id,
                                  uint32_t req_id,
                                  float vector)
{
    vemb_v16_resp_t response = {
        .status = VEMB_V16_STATUS_OK,
        .op = VEMB_V16_OP_VEMB_INLINE,
        .req_id = req_id,
        .vector_bytes = sizeof(vector),
    };
    uint8_t payload[64];
    size_t metadata_len = 0;

    assert(vemb_v16_resp_encode(payload, sizeof(payload), &response,
                                &metadata_len) == 0);
    memcpy(payload + metadata_len, &vector, sizeof(vector));
    assert(vemb_v16_net_write_frame(fd, VEMB_V16_NET_RESPONSE, 0,
                                    channel_id, req_id, payload,
                                    (uint32_t)(metadata_len + sizeof(vector))) == 0);
}

static void write_welcome(int fd, uint64_t channel_id)
{
    vemb_v16_channel_desc_t desc = {
        .magic = VEMB_V16_MAGIC,
        .version = VEMB_V16_VERSION,
        .channel_id = channel_id,
        .vector_dim = 1,
        .vector_stride = sizeof(float),
        .request_ring_slot_size = 64,
        .response_ring_slot_size = 64,
    };
    uint8_t welcome[1024];
    size_t welcome_len = 0;

    assert(vemb_v16_channel_desc_encode(welcome, sizeof(welcome), &desc,
                                        &welcome_len) == 0);
    assert(vemb_v16_net_write_frame(fd, VEMB_V16_NET_WELCOME, 0,
                                    channel_id, 0, welcome,
                                    (uint32_t)welcome_len) == 0);
}

static void read_hello(int fd)
{
    vemb_v16_net_hdr_t header;
    uint8_t hello[64];

    assert(vemb_v16_net_read_header(fd, &header) == 0);
    assert(header.type == VEMB_V16_NET_HELLO);
    assert(header.payload_len <= sizeof(hello));
    assert(vemb_v16_net_read_full(fd, hello, header.payload_len) == 0);
}

static void *fake_tcp_server(void *arg)
{
    fake_tcp_server_t *server = arg;
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);
    int fd = accept(server->listen_fd, (struct sockaddr *)&addr, &addr_len);
    assert(fd >= 0);

    read_topology_get(fd);
    write_topology_response(fd, 1, server->port);
    close(fd);

    fd = accept(server->listen_fd, (struct sockaddr *)&addr, &addr_len);
    assert(fd >= 0);

    read_hello(fd);
    write_welcome(fd, server->channel_id);

    vemb_v16_req_t requests[2];
    read_request(fd, &requests[0]);
    read_request(fd, &requests[1]);
    assert(requests[0].op == VEMB_V16_OP_VEMB_INLINE);
    assert(requests[1].op == VEMB_V16_OP_VEMB_INLINE);

    /* Reverse wire completion order. The client must route each payload by
     * logical operation identity rather than pipeline submission order. */
    write_inline_response(fd, server->channel_id, requests[1].req_id, 22.0f);
    write_inline_response(fd, server->channel_id, requests[0].req_id, 11.0f);

    vemb_v16_net_hdr_t header;
    assert(vemb_v16_net_read_header(fd, &header) == 0);
    assert(header.type == VEMB_V16_NET_CLOSE_CHANNEL);
    close(fd);
    close(server->listen_fd);
    return NULL;
}

static uint16_t listener_port(int fd)
{
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);
    assert(getsockname(fd, (struct sockaddr *)&addr, &addr_len) == 0);
    return ntohs(addr.sin_port);
}

static void test_pipeline_routes_out_of_order_tcp_completions(void)
{
    fake_tcp_server_t server = {
        .listen_fd = vemb_v16_net_listen("127.0.0.1", 0, 2),
        .channel_id = 0x4d2u,
    };
    assert(server.listen_fd >= 0);
    server.port = listener_port(server.listen_fd);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, fake_tcp_server, &server) == 0);

    char endpoint[64];
    snprintf(endpoint, sizeof(endpoint), "127.0.0.1:%u", server.port);
    const char *seeds[] = {endpoint};
    vemb_v16_client_t *client = vemb_v16_client_create(seeds, 1, 1, 1000);
    assert(client != NULL);

    const char *sets[] = {"set", "set"};
    const char *elements[] = {"first", "second"};
    float vectors[2] = {0};
    vemb_v16_pipeline_resp_t responses[2] = {0};

    assert(vemb_v16_client_vemb_pipeline(client, sets, elements, 2, vectors,
                                         responses, 2) == 0);
    assert(responses[0].status == 0);
    assert(responses[1].status == 0);
    assert(responses[0].dim == 1);
    assert(responses[1].dim == 1);
    assert(vectors[0] == 11.0f);
    assert(vectors[1] == 22.0f);

    vemb_v16_client_destroy(client);
    assert(pthread_join(thread, NULL) == 0);
}

typedef struct epoch_reuse_server {
    int listen_fd;
    uint16_t port;
} epoch_reuse_server_t;

static int accept_connection(int listen_fd)
{
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);
    int fd = accept(listen_fd, (struct sockaddr *)&addr, &addr_len);
    assert(fd >= 0);
    return fd;
}

static void read_topology_get(int fd)
{
    vemb_v16_net_hdr_t header;

    assert(vemb_v16_net_read_header(fd, &header) == 0);
    assert(header.type == VEMB_V16_NET_TOPOLOGY_GET);
    assert(header.payload_len == 0);
}

static void write_topology_response(int fd, uint64_t epoch, uint16_t port)
{
    vemb_v16_topology_control_resp_t response = {
        .status = VEMB_V16_STATUS_OK,
        .current_topology_epoch = epoch,
        .min_write_epoch = epoch,
        .vnode_count = VEMB_V16_TOPOLOGY_DEFAULT_VNODES,
        .active_owner_count = 2,
        .standby_owner_count = 2,
        .endpoint_count = 2,
    };
    uint8_t payload[4096];
    size_t payload_len = 0;

    for (uint32_t i = 0; i < response.endpoint_count; i++) {
        response.active_owners[i] = i;
        response.standby_owners[i] = i;
        response.endpoints[i].owner_id = i;
        response.endpoints[i].transport_type = VEMB_V16_TRANSPORT_TCP;
        response.endpoints[i].tcp_port = port;
        snprintf(response.endpoints[i].host,
                 sizeof(response.endpoints[i].host), "127.0.0.1");
    }
    assert(vemb_v16_topology_control_resp_encode(payload, sizeof(payload),
                                                  &response, &payload_len) == 0);
    assert(vemb_v16_net_write_frame(fd, VEMB_V16_NET_TOPOLOGY_RESPONSE, 0,
                                    0, 0, payload, (uint32_t)payload_len) == 0);
}

/* Keep owner 0 as the normal route, while owner 1 remains addressable for
 * an ASK retry. */
static void write_vector_session_topology_response(
    int fd, uint64_t epoch, uint16_t port)
{
    vemb_v16_topology_control_resp_t response = {
        .status = VEMB_V16_STATUS_OK,
        .current_topology_epoch = epoch,
        .min_write_epoch = epoch,
        .vnode_count = VEMB_V16_TOPOLOGY_DEFAULT_VNODES,
        .active_owner_count = 1,
        .standby_owner_count = 2,
        .endpoint_count = 2,
    };
    uint8_t payload[4096];
    size_t payload_len = 0;

    response.active_owners[0] = 0;
    response.standby_owners[0] = 0;
    response.standby_owners[1] = 1;
    for (uint32_t i = 0; i < response.endpoint_count; i++) {
        response.endpoints[i].owner_id = i;
        response.endpoints[i].transport_type = VEMB_V16_TRANSPORT_TCP;
        response.endpoints[i].tcp_port = port;
        snprintf(response.endpoints[i].host,
                 sizeof(response.endpoints[i].host), "127.0.0.1");
    }
    assert(vemb_v16_topology_control_resp_encode(payload, sizeof(payload),
                                                  &response, &payload_len) == 0);
    assert(vemb_v16_net_write_frame(fd, VEMB_V16_NET_TOPOLOGY_RESPONSE, 0,
                                    0, 0, payload, (uint32_t)payload_len) == 0);
}

static void write_status_response(int fd,
                                  uint64_t channel_id,
                                  const vemb_v16_req_t *request,
                                  uint8_t status)
{
    vemb_v16_resp_t response = {
        .status = status,
        .op = request->op,
        .req_id = request->req_id,
    };
    uint8_t payload[64];
    size_t payload_len = 0;

    assert(vemb_v16_resp_encode(payload, sizeof(payload), &response,
                                &payload_len) == 0);
    assert(vemb_v16_net_write_frame(fd, VEMB_V16_NET_RESPONSE, 0,
                                    channel_id, request->req_id, payload,
                                    (uint32_t)payload_len) == 0);
}

static void write_redirect_response(int fd,
                                    uint64_t channel_id,
                                    const vemb_v16_req_t *request,
                                    uint8_t status,
                                    uint32_t redirect_owner)
{
    vemb_v16_resp_t response = {
        .status = status,
        .op = request->op,
        .req_id = request->req_id,
        .redirect_owner = redirect_owner,
    };
    uint8_t payload[64];
    size_t payload_len = 0;

    assert(status == VEMB_V16_STATUS_ASK || status == VEMB_V16_STATUS_MOVED ||
           status == VEMB_V16_STATUS_STALE_TOPOLOGY);
    assert(vemb_v16_resp_encode(payload, sizeof(payload), &response,
                                &payload_len) == 0);
    assert(vemb_v16_net_write_frame(fd, VEMB_V16_NET_RESPONSE, 0,
                                    channel_id, request->req_id, payload,
                                    (uint32_t)payload_len) == 0);
}

static void read_close_channel(int fd)
{
    vemb_v16_net_hdr_t header;

    assert(vemb_v16_net_read_header(fd, &header) == 0);
    assert(header.type == VEMB_V16_NET_CLOSE_CHANNEL);
    assert(header.payload_len == 0);
}

static void *epoch_reuse_server(void *arg)
{
    epoch_reuse_server_t *server = arg;
    /* create() keeps these addresses as bootstrap seeds. It does not
     * open a data channel until topology has selected an owner. */
    int topology_fd = accept_connection(server->listen_fd);
    read_topology_get(topology_fd);
    write_topology_response(topology_fd, 9, server->port);
    close(topology_fd);

    int owner_fd = accept_connection(server->listen_fd);
    read_hello(owner_fd);
    write_welcome(owner_fd, 0x200u);

    vemb_v16_req_t first_request;
    read_request(owner_fd, &first_request);
    assert(first_request.op == VEMB_V16_OP_VADD);
    write_status_response(owner_fd, 0x200u, &first_request,
                          VEMB_V16_STATUS_STALE_TOPOLOGY);

    topology_fd = accept_connection(server->listen_fd);
    read_topology_get(topology_fd);
    write_topology_response(topology_fd, 10, server->port);
    close(topology_fd);

    struct pollfd wait_fds[] = {
        {.fd = owner_fd, .events = POLLIN},
        {.fd = server->listen_fd, .events = POLLIN},
    };
    assert(poll(wait_fds, 2, 1000) > 0);
    assert((wait_fds[0].revents & POLLIN) != 0);
    assert((wait_fds[1].revents & POLLIN) == 0);

    vemb_v16_req_t retry_request;
    read_request(owner_fd, &retry_request);
    assert(retry_request.op == VEMB_V16_OP_VADD);
    assert(retry_request.channel_id == 0x200u);
    write_status_response(owner_fd, 0x200u, &retry_request,
                          VEMB_V16_STATUS_OK);

    read_close_channel(owner_fd);
    close(owner_fd);
    close(server->listen_fd);
    return NULL;
}

static void test_same_endpoint_survives_topology_epoch_change(void)
{
    epoch_reuse_server_t server = {
        .listen_fd = vemb_v16_net_listen("127.0.0.1", 0, 4),
    };
    assert(server.listen_fd >= 0);
    server.port = listener_port(server.listen_fd);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, epoch_reuse_server, &server) == 0);

    char endpoint[64];
    snprintf(endpoint, sizeof(endpoint), "127.0.0.1:%u", server.port);
    const char *endpoints[] = {endpoint, endpoint};
    vemb_v16_client_t *client = vemb_v16_client_create(endpoints, 2, 1,
                                                         1000);
    assert(client != NULL);

    const float vector[] = {1.0f};
    assert(vemb_v16_client_vadd(client, "set", "entry", vector, 1) == 0);
    vemb_v16_client_destroy(client);
    assert(pthread_join(thread, NULL) == 0);
}

typedef struct topology_failover_server {
    int failed_seed_listen_fd;
    int healthy_seed_listen_fd;
    uint16_t healthy_seed_port;
} topology_failover_server_t;

static void *topology_failover_server(void *arg)
{
    topology_failover_server_t *server = arg;
    int fd = accept_connection(server->failed_seed_listen_fd);

    read_topology_get(fd);
    close(fd);
    close(server->failed_seed_listen_fd);

    fd = accept_connection(server->healthy_seed_listen_fd);
    read_topology_get(fd);
    write_topology_response(fd, 17, server->healthy_seed_port);
    close(fd);

    fd = accept_connection(server->healthy_seed_listen_fd);
    read_hello(fd);
    write_welcome(fd, 0x300u);

    vemb_v16_req_t request;
    read_request(fd, &request);
    assert(request.op == VEMB_V16_OP_VADD);
    write_status_response(fd, 0x300u, &request, VEMB_V16_STATUS_OK);
    read_close_channel(fd);
    close(fd);
    close(server->healthy_seed_listen_fd);
    return NULL;
}

static void test_topology_fetch_fails_over_to_next_seed(void)
{
    topology_failover_server_t server = {
        .failed_seed_listen_fd = vemb_v16_net_listen("127.0.0.1", 0, 1),
        .healthy_seed_listen_fd = vemb_v16_net_listen("127.0.0.1", 0, 2),
    };
    assert(server.failed_seed_listen_fd >= 0);
    assert(server.healthy_seed_listen_fd >= 0);
    server.healthy_seed_port = listener_port(server.healthy_seed_listen_fd);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, topology_failover_server, &server) == 0);

    char failed_seed[64];
    char healthy_seed[64];
    snprintf(failed_seed, sizeof(failed_seed), "127.0.0.1:%u",
             listener_port(server.failed_seed_listen_fd));
    snprintf(healthy_seed, sizeof(healthy_seed), "127.0.0.1:%u",
             server.healthy_seed_port);
    const char *seeds[] = {failed_seed, healthy_seed};
    vemb_v16_client_t *client = vemb_v16_client_create(seeds, 2, 1, 1000);
    assert(client != NULL);

    const float vector[] = {1.0f};
    assert(vemb_v16_client_vadd(client, "set", "failover", vector, 1) == 0);
    vemb_v16_client_destroy(client);
    assert(pthread_join(thread, NULL) == 0);
}

typedef struct tcp_handle_reject_server {
    int listen_fd;
    uint16_t port;
} tcp_handle_reject_server_t;

static void *tcp_handle_reject_server(void *arg)
{
    tcp_handle_reject_server_t *server = arg;
    int fd = accept_connection(server->listen_fd);
    read_topology_get(fd);
    write_topology_response(fd, 23, server->port);
    close(fd);

    struct pollfd pfd = {
        .fd = server->listen_fd,
        .events = POLLIN,
    };
    assert(poll(&pfd, 1, 200) == 0);
    close(server->listen_fd);
    return NULL;
}

static void test_tcp_handle_is_rejected_before_data_channel_open(void)
{
    tcp_handle_reject_server_t server = {
        .listen_fd = vemb_v16_net_listen("127.0.0.1", 0, 2),
    };
    assert(server.listen_fd >= 0);
    server.port = listener_port(server.listen_fd);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, tcp_handle_reject_server, &server) ==
           0);

    char endpoint[64];
    snprintf(endpoint, sizeof(endpoint), "127.0.0.1:%u", server.port);
    const char *seeds[] = {endpoint};
    vemb_v16_client_t *client = vemb_v16_client_create(seeds, 1, 1, 1000);
    assert(client != NULL);

    assert(vemb_v16_client_vemb_handle(client, "set", "entry", NULL, NULL,
                                       NULL, NULL) == -1);
    const char *sets[] = {"set"};
    const char *elements[] = {"entry"};
    vemb_v16_pipeline_resp_t response;
    assert(vemb_v16_client_vemb_handle_pipeline(client, sets, elements, 1,
                                                 &response, 1) == 0);
    assert(response.status == -1);

    vemb_v16_client_destroy(client);
    assert(pthread_join(thread, NULL) == 0);
}

typedef struct tcp_inline_vector_server {
    int listen_fd;
    uint16_t port;
    uint64_t channel_id;
} tcp_inline_vector_server_t;

static void *tcp_inline_vector_server(void *arg)
{
    tcp_inline_vector_server_t *server = arg;
    int fd = accept_connection(server->listen_fd);
    read_topology_get(fd);
    write_topology_response(fd, 29, server->port);
    close(fd);

    fd = accept_connection(server->listen_fd);
    read_hello(fd);
    write_welcome(fd, server->channel_id);
    vemb_v16_req_t request;
    read_request(fd, &request);
    assert(request.op == VEMB_V16_OP_VEMB_INLINE);
    write_inline_response(fd, server->channel_id, request.req_id, 37.0f);
    read_close_channel(fd);
    close(fd);
    close(server->listen_fd);
    return NULL;
}

static void test_tcp_vemb_vector_uses_inline_without_handle_error(void)
{
    tcp_inline_vector_server_t server = {
        .listen_fd = vemb_v16_net_listen("127.0.0.1", 0, 2),
        .channel_id = 0x400u,
    };
    assert(server.listen_fd >= 0);
    server.port = listener_port(server.listen_fd);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, tcp_inline_vector_server, &server) ==
           0);

    char endpoint[64];
    snprintf(endpoint, sizeof(endpoint), "127.0.0.1:%u", server.port);
    const char *seeds[] = {endpoint};
    vemb_v16_client_t *client = vemb_v16_client_create(seeds, 1, 1, 1000);
    assert(client != NULL);

    float vector = 0.0f;
    assert(vemb_v16_client_vemb_vector(client, "set", "entry", &vector, 1,
                                       NULL) == 0);
    assert(vector == 37.0f);
    vemb_v16_logical_stats_t stats;
    vemb_v16_client_get_logical_stats(client, &stats);
    assert(stats.successes == 1);
    assert(stats.errors == 0);

    vemb_v16_client_destroy(client);
    assert(pthread_join(thread, NULL) == 0);
}

typedef struct vector_session_completion {
    uint64_t cookies[3];
    float vectors[3];
    uint32_t count;
} vector_session_completion_t;

static void record_vector_session_completion(
    void *priv, uint64_t caller_cookie,
    const vemb_v16_pipeline_resp_t *response, const float *vector)
{
    vector_session_completion_t *completion = priv;
    assert(completion->count < 3);
    assert(response->status == 0);
    assert(response->dim == 1);
    assert(vector != NULL);
    completion->cookies[completion->count] = caller_cookie;
    completion->vectors[completion->count] = vector[0];
    completion->count++;
}

static void test_tcp_vector_session_coalesces_inline_read(void)
{
    tcp_inline_vector_server_t server = {
        .listen_fd = vemb_v16_net_listen("127.0.0.1", 0, 2),
        .channel_id = 0x401u,
    };
    assert(server.listen_fd >= 0);
    server.port = listener_port(server.listen_fd);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, tcp_inline_vector_server, &server) ==
           0);

    char endpoint[64];
    snprintf(endpoint, sizeof(endpoint), "127.0.0.1:%u", server.port);
    const char *seeds[] = {endpoint};
    vemb_v16_client_t *client = vemb_v16_client_create(seeds, 1, 1, 1000);
    assert(client != NULL);
    vemb_v16_client_vector_session_options_t options = {
        .cache_mode = VEMB_V16_CLIENT_VECTOR_CACHE_IMMUTABLE_SNAPSHOT,
        .cache_entries = 4,
    };
    vemb_v16_client_vector_session_t *session =
        vemb_v16_client_vector_session_create_with_options(client, &options);
    assert(session != NULL);
    assert(vemb_v16_client_vector_session_submit(session, "set", "entry",
                                                  11) == 0);
    assert(vemb_v16_client_vector_session_submit(session, "set", "entry",
                                                  22) == 0);

    vector_session_completion_t completion = {0};
    for (uint32_t attempt = 0; attempt < 1000 && completion.count != 2;
         attempt++) {
        assert(vemb_v16_client_vector_session_poll(
                   session, record_vector_session_completion, &completion) >=
               0);
        if (completion.count != 2)
            usleep(1000);
    }
    assert(completion.count == 2);
    assert(completion.cookies[0] == 11);
    assert(completion.cookies[1] == 22);
    assert(completion.vectors[0] == 37.0f);
    assert(completion.vectors[1] == 37.0f);
    assert(vemb_v16_client_vector_session_submit(session, "set", "entry",
                                                  33) == 0);
    assert(vemb_v16_client_vector_session_poll(
               session, record_vector_session_completion, &completion) == 1);
    assert(completion.count == 3);
    assert(completion.cookies[2] == 33);
    assert(completion.vectors[2] == 37.0f);
    vemb_v16_logical_stats_t stats;
    vemb_v16_client_get_logical_stats(client, &stats);
    assert(stats.successes == 3);
    assert(stats.errors == 0);

    vemb_v16_client_vector_session_close(session, NULL, NULL);
    vemb_v16_client_destroy(client);
    assert(pthread_join(thread, NULL) == 0);
}

typedef struct tcp_vector_cache_epoch_server {
    int listen_fd;
    uint16_t port;
    uint64_t channel_id;
} tcp_vector_cache_epoch_server_t;

static void *tcp_vector_cache_epoch_server(void *arg)
{
    tcp_vector_cache_epoch_server_t *server = arg;
    int fd = accept_connection(server->listen_fd);
    read_topology_get(fd);
    write_topology_response(fd, 51, server->port);
    close(fd);

    fd = accept_connection(server->listen_fd);
    read_hello(fd);
    write_welcome(fd, server->channel_id);
    vemb_v16_req_t request;
    read_request(fd, &request);
    assert(request.op == VEMB_V16_OP_VEMB_INLINE);
    write_inline_response(fd, server->channel_id, request.req_id, 51.0f);

    int topology_fd = accept_connection(server->listen_fd);
    read_topology_get(topology_fd);
    write_topology_response(topology_fd, 52, server->port);
    close(topology_fd);

    read_request(fd, &request);
    assert(request.op == VEMB_V16_OP_VEMB_INLINE);
    write_inline_response(fd, server->channel_id, request.req_id, 52.0f);
    read_close_channel(fd);
    close(fd);
    close(server->listen_fd);
    return NULL;
}

static void test_tcp_vector_session_epoch_change_invalidates_cache(void)
{
    tcp_vector_cache_epoch_server_t server = {
        .listen_fd = vemb_v16_net_listen("127.0.0.1", 0, 2),
        .channel_id = 0x402u,
    };
    assert(server.listen_fd >= 0);
    server.port = listener_port(server.listen_fd);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, tcp_vector_cache_epoch_server,
                          &server) == 0);

    char endpoint[64];
    snprintf(endpoint, sizeof(endpoint), "127.0.0.1:%u", server.port);
    const char *seeds[] = {endpoint};
    vemb_v16_client_t *client = vemb_v16_client_create(seeds, 1, 1, 1000);
    assert(client != NULL);
    vemb_v16_client_vector_session_options_t options = {
        .cache_mode = VEMB_V16_CLIENT_VECTOR_CACHE_IMMUTABLE_SNAPSHOT,
        .cache_entries = 4,
    };
    vemb_v16_client_vector_session_t *session =
        vemb_v16_client_vector_session_create_with_options(client, &options);
    assert(session != NULL);

    vector_session_completion_t completion = {0};
    assert(vemb_v16_client_vector_session_submit(session, "set", "entry",
                                                  41) == 0);
    for (uint32_t attempt = 0; attempt < 1000 && completion.count != 1;
         attempt++) {
        assert(vemb_v16_client_vector_session_poll(
                   session, record_vector_session_completion, &completion) >=
               0);
        if (completion.count != 1)
            usleep(1000);
    }
    assert(completion.vectors[0] == 51.0f);
    assert(vemb_v16_client_topology_refresh(client) == 0);
    assert(vemb_v16_client_vector_session_submit(session, "set", "entry",
                                                  42) == 0);
    for (uint32_t attempt = 0; attempt < 1000 && completion.count != 2;
         attempt++) {
        assert(vemb_v16_client_vector_session_poll(
                   session, record_vector_session_completion, &completion) >=
               0);
        if (completion.count != 2)
            usleep(1000);
    }
    assert(completion.cookies[0] == 41);
    assert(completion.cookies[1] == 42);
    assert(completion.vectors[1] == 52.0f);

    vemb_v16_client_vector_session_close(session, NULL, NULL);
    vemb_v16_client_destroy(client);
    assert(pthread_join(thread, NULL) == 0);
}

typedef struct tcp_vector_session_repeated_read_server {
    int listen_fd;
    uint16_t port;
    uint64_t channel_id;
} tcp_vector_session_repeated_read_server_t;

static void *tcp_vector_session_repeated_read_server(void *arg)
{
    tcp_vector_session_repeated_read_server_t *server = arg;
    int fd = accept_connection(server->listen_fd);
    read_topology_get(fd);
    write_topology_response(fd, 61, server->port);
    close(fd);

    fd = accept_connection(server->listen_fd);
    read_hello(fd);
    write_welcome(fd, server->channel_id);
    for (uint32_t i = 0; i < 2; i++) {
        vemb_v16_req_t request;
        read_request(fd, &request);
        assert(request.op == VEMB_V16_OP_VEMB_INLINE);
        write_inline_response(fd, server->channel_id, request.req_id,
                              61.0f + (float)i);
    }
    read_close_channel(fd);
    close(fd);
    close(server->listen_fd);
    return NULL;
}

static void test_tcp_vector_session_default_cache_repeats_read(void)
{
    tcp_vector_session_repeated_read_server_t server = {
        .listen_fd = vemb_v16_net_listen("127.0.0.1", 0, 2),
        .channel_id = 0x403u,
    };
    assert(server.listen_fd >= 0);
    server.port = listener_port(server.listen_fd);

    pthread_t thread;
    assert(pthread_create(&thread, NULL, tcp_vector_session_repeated_read_server,
                          &server) == 0);

    char endpoint[64];
    snprintf(endpoint, sizeof(endpoint), "127.0.0.1:%u", server.port);
    const char *seeds[] = {endpoint};
    vemb_v16_client_t *client = vemb_v16_client_create(seeds, 1, 1, 1000);
    assert(client != NULL);
    vemb_v16_client_vector_session_t *session =
        vemb_v16_client_vector_session_create(client);
    assert(session != NULL);

    vector_session_completion_t completion = {0};
    assert(vemb_v16_client_vector_session_submit(session, "set", "entry",
                                                  51) == 0);
    for (uint32_t attempt = 0; attempt < 1000 && completion.count != 1;
         attempt++) {
        assert(vemb_v16_client_vector_session_poll(
                   session, record_vector_session_completion, &completion) >=
               0);
        if (completion.count != 1)
            usleep(1000);
    }
    assert(completion.vectors[0] == 61.0f);

    assert(vemb_v16_client_vector_session_submit(session, "set", "entry",
                                                  52) == 0);
    for (uint32_t attempt = 0; attempt < 1000 && completion.count != 2;
         attempt++) {
        assert(vemb_v16_client_vector_session_poll(
                   session, record_vector_session_completion, &completion) >=
               0);
        if (completion.count != 2)
            usleep(1000);
    }
    assert(completion.cookies[0] == 51);
    assert(completion.cookies[1] == 52);
    assert(completion.vectors[1] == 62.0f);

    vemb_v16_client_vector_session_close(session, NULL, NULL);
    vemb_v16_client_destroy(client);
    assert(pthread_join(thread, NULL) == 0);
}

typedef struct tcp_vector_session_redirect_server {
    int listen_fd;
    uint16_t port;
    uint64_t channel_id;
    uint8_t redirect_status;
} tcp_vector_session_redirect_server_t;

static void *tcp_vector_session_redirect_server(void *arg)
{
    tcp_vector_session_redirect_server_t *server = arg;
    int topology_fd = accept_connection(server->listen_fd);
    read_topology_get(topology_fd);
    write_vector_session_topology_response(topology_fd, 71, server->port);
    close(topology_fd);

    int owner_fd = accept_connection(server->listen_fd);
    read_hello(owner_fd);
    write_welcome(owner_fd, server->channel_id);
    vemb_v16_req_t request;
    read_request(owner_fd, &request);
    assert(request.op == VEMB_V16_OP_VEMB_INLINE);
    assert(request.flags == 0);
    assert(request.topology_epoch == 71);
    write_redirect_response(owner_fd, server->channel_id, &request,
                            server->redirect_status, 1);

    if (server->redirect_status == VEMB_V16_STATUS_ASK) {
        int ask_fd = accept_connection(server->listen_fd);
        read_hello(ask_fd);
        write_welcome(ask_fd, server->channel_id + 1);
        read_request(ask_fd, &request);
        assert(request.op == VEMB_V16_OP_VEMB_INLINE);
        assert(request.flags == VEMB_V16_REQ_F_ASK_REDIRECT);
        assert(request.topology_epoch == 71);
        write_inline_response(ask_fd, server->channel_id + 1, request.req_id,
                              72.0f);
        read_close_channel(owner_fd);
        close(owner_fd);
        read_close_channel(ask_fd);
        close(ask_fd);
    } else {
        topology_fd = accept_connection(server->listen_fd);
        read_topology_get(topology_fd);
        write_vector_session_topology_response(topology_fd, 72, server->port);
        close(topology_fd);

        read_request(owner_fd, &request);
        assert(request.op == VEMB_V16_OP_VEMB_INLINE);
        assert(request.flags == 0);
        assert(request.topology_epoch == 72);
        write_inline_response(owner_fd, server->channel_id, request.req_id,
                              72.0f);
        read_close_channel(owner_fd);
        close(owner_fd);
    }
    close(server->listen_fd);
    return NULL;
}

static void test_tcp_vector_session_redirect_retries(void)
{
    const uint8_t statuses[] = {
        VEMB_V16_STATUS_ASK,
        VEMB_V16_STATUS_MOVED,
        VEMB_V16_STATUS_STALE_TOPOLOGY,
    };

    for (uint32_t i = 0; i < sizeof(statuses); i++) {
        tcp_vector_session_redirect_server_t server = {
            .listen_fd = vemb_v16_net_listen("127.0.0.1", 0, 4),
            .channel_id = 0x410u + i * 2,
            .redirect_status = statuses[i],
        };
        assert(server.listen_fd >= 0);
        server.port = listener_port(server.listen_fd);

        pthread_t thread;
        assert(pthread_create(&thread, NULL, tcp_vector_session_redirect_server,
                              &server) == 0);

        char endpoint[64];
        snprintf(endpoint, sizeof(endpoint), "127.0.0.1:%u", server.port);
        const char *seeds[] = {endpoint};
        vemb_v16_client_t *client =
            vemb_v16_client_create(seeds, 1, 1, 1000);
        assert(client != NULL);
        vemb_v16_client_vector_session_t *session =
            vemb_v16_client_vector_session_create(client);
        assert(session != NULL);
        vector_session_completion_t completion = {0};
        assert(vemb_v16_client_vector_session_submit(session, "set", "entry",
                                                      61 + i) == 0);
        for (uint32_t attempt = 0;
             attempt < 1000 && completion.count != 1; attempt++) {
            assert(vemb_v16_client_vector_session_poll(
                       session, record_vector_session_completion,
                       &completion) >= 0);
            if (completion.count != 1)
                usleep(1000);
        }
        assert(completion.count == 1);
        assert(completion.cookies[0] == 61 + i);
        assert(completion.vectors[0] == 72.0f);
        vemb_v16_logical_stats_t stats;
        vemb_v16_client_get_logical_stats(client, &stats);
        assert(stats.successes == 1);
        assert(stats.errors == 0);
        vemb_v16_redirect_stats_t redirects;
        vemb_v16_client_get_redirect_stats(client, &redirects);
        assert(redirects.ask_redirects ==
               (statuses[i] == VEMB_V16_STATUS_ASK ? 1 : 0));
        assert(redirects.moved_redirects ==
               (statuses[i] == VEMB_V16_STATUS_MOVED ? 1 : 0));
        assert(redirects.stale_topology_responses ==
               (statuses[i] == VEMB_V16_STATUS_STALE_TOPOLOGY ? 1 : 0));

        vemb_v16_client_vector_session_close(session, NULL, NULL);
        vemb_v16_client_destroy(client);
        assert(pthread_join(thread, NULL) == 0);
    }
}

int main(void)
{
    test_pipeline_routes_out_of_order_tcp_completions();
    test_same_endpoint_survives_topology_epoch_change();
    test_topology_fetch_fails_over_to_next_seed();
    test_tcp_handle_is_rejected_before_data_channel_open();
    test_tcp_vemb_vector_uses_inline_without_handle_error();
    test_tcp_vector_session_coalesces_inline_read();
    test_tcp_vector_session_epoch_change_invalidates_cache();
    test_tcp_vector_session_default_cache_repeats_read();
    test_tcp_vector_session_redirect_retries();
    printf("vemb_v16_tcp_data_transport_ut: all tests passed\n");
    return 0;
}
