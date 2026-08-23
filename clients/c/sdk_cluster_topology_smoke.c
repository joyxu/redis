#include "vemb_v16_client_sdk.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int wait_for_file(const char *path)
{
    for (uint32_t waited = 0; waited < 300; waited++) {
        if (access(path, F_OK) == 0)
            return 0;
        usleep(100000);
    }
    return -1;
}

int main(int argc, char **argv)
{
    if (argc != 5 && argc != 6) {
        fprintf(stderr, "usage: %s <seed0> <seed1> <dim> <count> [sync-dir]\n",
                argv[0]);
        return 2;
    }

    uint32_t dim = (uint32_t)strtoul(argv[3], NULL, 10);
    uint32_t count = (uint32_t)strtoul(argv[4], NULL, 10);
    if (dim == 0 || dim > VEMB_V16_MAX_DIM || count < 2) {
        fprintf(stderr, "invalid dim/count\n");
        return 2;
    }

    const char *seeds[] = {argv[1], argv[2]};
    vemb_v16_client_t *client =
        vemb_v16_client_create(seeds, 2, dim, 5000,
                               VEMB_V16_TRANSPORT_TCP);
    if (!client) {
        fprintf(stderr, "create failed\n");
        return 1;
    }

    float *vector = calloc(dim, sizeof(*vector));
    float *out_vectors = calloc((size_t)count * dim, sizeof(*out_vectors));
    const char **set_names = calloc(count, sizeof(*set_names));
    const char **elem_names = calloc(count, sizeof(*elem_names));
    char (*elem_storage)[32] = calloc(count, sizeof(*elem_storage));
    vemb_v16_pipeline_resp_t *responses = calloc(count, sizeof(*responses));
    if (!vector || !out_vectors || !set_names || !elem_names || !elem_storage ||
        !responses) {
        fprintf(stderr, "allocation failed\n");
        free(responses);
        free(elem_storage);
        free(elem_names);
        free(set_names);
        free(out_vectors);
        free(vector);
        vemb_v16_client_destroy(client);
        return 1;
    }

    for (uint32_t d = 0; d < dim; d++)
        vector[d] = (float)(d + 1) / (float)dim;
    for (uint32_t i = 0; i < count; i++) {
        snprintf(elem_storage[i], sizeof(elem_storage[i]), "sdk-smoke:%u", i);
        set_names[i] = "sdk-cluster";
        elem_names[i] = elem_storage[i];
        if (vemb_v16_client_vadd(client, set_names[i], elem_names[i], vector,
                                 dim) != 0) {
            fprintf(stderr, "vadd failed at %u\n", i);
            goto fail;
        }
    }

    if (vemb_v16_client_vemb_pipeline(client, set_names, elem_names, count,
                                      out_vectors, responses, 8) != 0) {
        fprintf(stderr, "vemb pipeline failed\n");
        goto fail;
    }
    for (uint32_t i = 0; i < count; i++) {
        if (responses[i].status != 0 || responses[i].dim != dim) {
            fprintf(stderr, "vemb pipeline response failed at %u status=%d dim=%u\n",
                    i, responses[i].status, responses[i].dim);
            goto fail;
        }
    }

    vemb_v16_redirect_stats_t redirect_stats;
    memset(&redirect_stats, 0, sizeof(redirect_stats));
    vemb_v16_client_get_redirect_stats(client, &redirect_stats);
    if (redirect_stats.topology_refresh_calls == 0) {
        fprintf(stderr, "topology was not fetched\n");
        goto fail;
    }

    if (argc == 6) {
        char ready_path[512];
        char fail_path[512];
        char failed_path[512];
        char recover_path[512];
        snprintf(ready_path, sizeof(ready_path), "%s/ready", argv[5]);
        snprintf(fail_path, sizeof(fail_path), "%s/fail", argv[5]);
        snprintf(failed_path, sizeof(failed_path), "%s/failed", argv[5]);
        snprintf(recover_path, sizeof(recover_path), "%s/recover", argv[5]);
        FILE *ready = fopen(ready_path, "w");
        if (!ready || fclose(ready) != 0 || wait_for_file(fail_path) != 0) {
            fprintf(stderr, "reconnect synchronization failed\n");
            goto fail;
        }
        if (vemb_v16_client_vadd(client, "sdk-cluster", "sdk-reconnect-fail",
                                 vector, dim) == 0) {
            fprintf(stderr, "expected transport failure after owner shutdown\n");
            goto fail;
        }
        FILE *failed = fopen(failed_path, "w");
        if (!failed || fclose(failed) != 0 ||
            wait_for_file(recover_path) != 0) {
            fprintf(stderr, "reconnect recovery synchronization failed\n");
            goto fail;
        }
        if (vemb_v16_client_vadd(client, "sdk-cluster", "sdk-reconnect-ok",
                                 vector, dim) != 0) {
            fprintf(stderr, "vadd did not reconnect after owner restart\n");
            goto fail;
        }
    }

    printf("sdk_cluster_topology_smoke: count=%u dim=%u topology_refresh=%llu "
           "ask=%llu moved=%llu stale=%llu\n",
           count, dim,
           (unsigned long long)redirect_stats.topology_refresh_calls,
           (unsigned long long)redirect_stats.ask_redirects,
           (unsigned long long)redirect_stats.moved_redirects,
           (unsigned long long)redirect_stats.stale_topology_responses);

    free(responses);
    free(elem_storage);
    free(elem_names);
    free(set_names);
    free(out_vectors);
    free(vector);
    vemb_v16_client_destroy(client);
    return 0;

fail:
    free(responses);
    free(elem_storage);
    free(elem_names);
    free(set_names);
    free(out_vectors);
    free(vector);
    vemb_v16_client_destroy(client);
    return 1;
}
