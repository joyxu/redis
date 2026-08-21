#ifndef __VEMB_V16_SERVER_TCP_CLIENT_H
#define __VEMB_V16_SERVER_TCP_CLIENT_H

#include "vemb_v16_protocol.h"

#include <stdint.h>

int vemb_v16_stc_init(const char *host, uint16_t port, uint32_t dim);

int vemb_v16_stc_vadd(const char *key, uint32_t key_len,
                      const float *vector, uint32_t dim,
                      vemb_v16_resp_t *resp);

/* VSIM_INLINE — compute cosine similarity between stored vector and inline query vector */
int vemb_v16_stc_vsim(const char *key, uint32_t key_len,
                      const float *query_vector, uint32_t dim,
                      vemb_v16_resp_t *resp);

void vemb_v16_stc_cleanup(void);

#endif
