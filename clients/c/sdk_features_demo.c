/*
 * SDK features demo — 端到端验证用，不属于 SDK 库本身
 *
 * 设计原则：不改 example.c / 库源码，由命令行参数 host/port/dim 控制。
 * 不同子命令覆盖报告不同章节：
 *   vemb_handle  → 3.3.2  VEMB_HANDLE (zero-copy) 路径
 *   ping_stats   → 3.6    数据面 PING 与 STATS
 *   pipeline     → 3.7    SDK Pipeline 批量操作
 *
 * 编译：
 *   gcc -I clients/c/build/include -I src \
 *       clients/c/sdk_features_demo.c \
 *       clients/c/build/libvemb_v16_client.a \
 *       -o sdk_features_demo -lpthread
 *
 * 运行（前提：redis-server 已在 host:port 启动，--vemb-v16-dim = dim）：
 *   ./sdk_features_demo <subcmd> <host> <port> <dim>
 *   ./sdk_features_demo vemb_handle  127.0.0.1 6379 <dim>
 *   ./sdk_features_demo ping_stats   127.0.0.1 6379 <dim>
 *   ./sdk_features_demo pipeline     127.0.0.1 6379 <dim>
 */

#include "vemb_v16_client_sdk.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void fill_vec(float *vec, uint32_t dim, float base)
{
    for (uint32_t i = 0; i < dim; i++) vec[i] = base + (float)i;
}

/* ----- 3.3.2 VEMB_HANDLE 路径 ----- */
static int demo_vemb_handle(vemb_v16_client_t *c, uint32_t dim)
{
    float vec[dim];
    fill_vec(vec, dim, 1.0f);

    if (vemb_v16_client_vadd(c, "myset", "elem1", vec, dim) != 0) {
        fprintf(stderr, "VADD failed\n");
        return 1;
    }
    printf("VADD myset elem1 -> OK\n");

    uint64_t offset = 0;
    uint32_t bytes = 0, dim_out = 0, region_id = 0;
    int rc = vemb_v16_client_vemb_handle(c, "myset", "elem1",
                                         &offset, &bytes, &dim_out, &region_id);
    if (rc != 0) {
        printf("vemb_handle rc=%d (1=NOT_FOUND)\n", rc);
        return 1;
    }
    printf("handle: offset=%llu bytes=%u dim=%u region=%u\n",
           (unsigned long long)offset, bytes, dim_out, region_id);

    float buf[dim];
    memset(buf, 0, sizeof(buf));
    if (vemb_v16_client_read_vector(c, offset, bytes, buf, dim) != 0) {
        fprintf(stderr, "read_vector failed\n");
        return 1;
    }

    int mismatch = 0;
    for (uint32_t i = 0; i < dim; i++) {
        if (buf[i] != vec[i]) mismatch++;
    }
    printf("read_vector: first=%f last=%f mismatch=%d (expect 0)\n",
           buf[0], buf[dim - 1], mismatch);
    return mismatch == 0 ? 0 : 1;
}

/* ----- 3.6 PING + STATS ----- */
static int demo_ping_stats(vemb_v16_client_t *c)
{
    if (vemb_v16_client_ping(c) == 0) {
        printf("PING -> OK\n");
    } else {
        printf("PING failed\n");
        return 1;
    }

    vemb_v16_stats_t stats;
    memset(&stats, 0, sizeof(stats));
    if (vemb_v16_client_stats(c, &stats) != 0) {
        printf("STATS failed\n");
        return 1;
    }
    printf("STATS: total_requests=%llu vadd=%llu vemb=%llu vsim=%llu active_channels=%llu\n",
           (unsigned long long)stats.total_requests,
           (unsigned long long)stats.vadd_requests,
           (unsigned long long)stats.vemb_requests,
           (unsigned long long)stats.vsim_requests,
           (unsigned long long)stats.active_channels);
    return (stats.total_requests > 0) ? 0 : 1;
}

/* ----- 3.7 Pipeline（vadd / vemb / vsim 三类批量） ----- */
static int demo_pipeline(vemb_v16_client_t *c, uint32_t dim)
{
    float vec[dim];
    fill_vec(vec, dim, 1.0f);

    /* 1) 批量 VADD：写 elem2 / elem3 / elem4 */
    const char *sets[3]  = {"myset", "myset", "myset"};
    const char *elems_vadd[3] = {"elem2", "elem3", "elem4"};
    const float *vecs[3] = {vec, vec, vec};
    if (vemb_v16_client_vadd_pipeline(c, sets, elems_vadd, vecs, 3, 16) != 0) {
        printf("VADD pipeline failed\n");
        return 1;
    }
    printf("VADD pipeline 3 items -> OK\n");

    /* 2) 批量 VEMB */
    const char *elems_vemb[3] = {"elem1", "elem2", "elem3"};
    float vemb_vectors[3 * dim];
    vemb_v16_pipeline_resp_t resps[3];
    memset(resps, 0, sizeof(resps));
    if (vemb_v16_client_vemb_pipeline(c, sets, elems_vemb, 3,
                                      vemb_vectors, resps, 16) != 0) {
        printf("VEMB pipeline failed\n");
        return 1;
    }
    for (int i = 0; i < 3; i++) {
        printf("vemb_pipeline resp[%d]: status=%d offset=%llu bytes=%u\n",
               i, resps[i].status,
               (unsigned long long)resps[i].offset,
               resps[i].bytes);
    }

    /* 3) 批量 VSIM（用 vec 作 query，elem1 == vec → score 应 = 1.0） */
    const char *elems_vsim[3] = {"elem1", "elem2", "elem3"};
    float scores[3] = {0};
    if (vemb_v16_client_vsim_pipeline(c, sets, elems_vsim, vec, 3, scores, 16) != 0) {
        printf("VSIM pipeline failed\n");
        return 1;
    }
    for (int i = 0; i < 3; i++) {
        printf("vsim_pipeline score[%d]: %.4f\n", i, scores[i]);
    }
    return 0;
}

int main(int argc, char **argv)
{
    if (argc < 5) {
        fprintf(stderr,
                "usage: %s <vemb_handle|ping_stats|pipeline> <host> <port> <dim>\n",
                argv[0]);
        return 2;
    }
    const char *subcmd = argv[1];
    const char *host   = argv[2];
    uint16_t port      = (uint16_t)atoi(argv[3]);
    uint32_t dim       = (uint32_t)atoi(argv[4]);
    if (port == 0 || dim == 0 || dim > VEMB_V16_MAX_DIM) {
        fprintf(stderr, "invalid port or dim (dim range: 1..%u)\n",
                VEMB_V16_MAX_DIM);
        return 2;
    }

    char seed[80];
    snprintf(seed, sizeof(seed), "%s:%u", host, port);
    const char *seeds[] = {seed};
    vemb_v16_client_t *c = vemb_v16_client_create(
        seeds, 1, dim, 0, VEMB_V16_TRANSPORT_TCP);
    if (!c) {
        fprintf(stderr, "connect failed: %s:%u\n", host, port);
        return 1;
    }
    printf("connected to %s:%u dim=%u\n", host, port, dim);

    int rc;
    if (strcmp(subcmd, "vemb_handle") == 0)      rc = demo_vemb_handle(c, dim);
    else if (strcmp(subcmd, "ping_stats") == 0)  rc = demo_ping_stats(c);
    else if (strcmp(subcmd, "pipeline") == 0)    rc = demo_pipeline(c, dim);
    else {
        fprintf(stderr, "unknown subcmd: %s\n", subcmd);
        rc = 2;
    }

    vemb_v16_client_destroy(c);
    return rc;
}
