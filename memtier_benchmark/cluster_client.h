/*
 * Copyright (C) 2011-2017 Redis Labs Ltd.
 *
 * This file is part of memtier_benchmark.
 *
 * memtier_benchmark is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2.
 *
 * memtier_benchmark is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with memtier_benchmark.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef MEMTIER_BENCHMARK_CLUSTER_CLIENT_H
#define MEMTIER_BENCHMARK_CLUSTER_CLIENT_H

#include <set>
#include <string>
#include <vector>
#include "client.h"

#ifdef __cplusplus
extern "C" {
#endif
#include "vemb_v16_client_sdk.h"
#ifdef __cplusplus
}
#endif

typedef std::queue<unsigned long long> key_index_pool;

// forward decleration
class shard_connection;

class cluster_client : public client {
protected:
    std::vector<key_index_pool*> m_key_index_pools;
    unsigned int m_slot_to_shard[16384];

    virtual int connect(void);
    virtual void disconnect(void);

    shard_connection* create_shard_connection(abstract_protocol* abs_protocol);
    bool connect_shard_connection(shard_connection* sc, char* address, char* port);
    void handle_moved(unsigned int conn_id, struct timeval timestamp,
                      request *request, protocol_response *response);
    void handle_ask(unsigned int conn_id, struct timeval timestamp,
                    request *request, protocol_response *response);

public:
    cluster_client(client_group* group);
    virtual ~cluster_client();

    virtual get_key_response get_key_for_conn(unsigned int command_index, unsigned int conn_id, unsigned long long* key_index);
    virtual bool create_arbitrary_request(unsigned int command_index, struct timeval& timestamp, unsigned int conn_id);

    // client manager api's
    virtual void handle_cluster_slots(protocol_response *r);
    virtual void create_request(struct timeval timestamp, unsigned int conn_id);
    virtual bool hold_pipeline(unsigned int conn_id);
    virtual void handle_response(unsigned int conn_id, struct timeval timestamp,
                                 request *request, protocol_response *response);
};

class vemb_v16_multi_client : public client {
protected:
    std::vector<std::string> m_endpoints;
    std::vector<const char*> m_endpoint_ptrs;
    bool m_topology_valid;
    vemb_v16_client_topology_t m_topology;
    int m_owner_to_conn[VEMB_V16_TOPOLOGY_MAX_OWNERS];
    struct event *m_topology_refresh_event;
    unsigned long long m_topology_refresh_count;
    unsigned long long m_topology_stale_retry_count;
    unsigned long long m_topology_moved_retry_count;
    unsigned long long m_topology_ask_retry_count;

    virtual int connect(void);
    virtual void disconnect(void);

    shard_connection* create_shard_connection(abstract_protocol* abs_protocol);
    bool connect_shard_connection(shard_connection* sc, const char* address, unsigned short port);
    int fetch_topology(void);
    int build_topology_owner_map(void);
    int route_key_to_backend(const char *key, int *backend_idx);
    int route_owner_to_backend(uint32_t owner, int *backend_idx);
    void apply_topology_epoch(void);
    void refresh_topology(void);
    void schedule_topology_refresh(void);
    static void topology_refresh_cb(evutil_socket_t fd, short events, void *arg);
    bool retry_topology_response(unsigned int conn_id,
                                 struct timeval timestamp,
                                 request *request,
                                 protocol_response *response);
    void record_topology_retry_stats(struct timeval timestamp,
                                     request *request,
                                     protocol_response *response,
                                     uint8_t status);

public:
    vemb_v16_multi_client(client_group* group);
    virtual ~vemb_v16_multi_client();

    virtual get_key_response get_key_for_conn(unsigned int command_index, unsigned int conn_id, unsigned long long* key_index);

    // client manager api's
    virtual bool hold_pipeline(unsigned int conn_id);
    virtual void handle_response(unsigned int conn_id, struct timeval timestamp,
                                 request *request, protocol_response *response);
};


#endif //MEMTIER_BENCHMARK_CLUSTER_CLIENT_H
