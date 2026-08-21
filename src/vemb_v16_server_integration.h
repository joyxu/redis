#ifndef __VEMB_V16_SERVER_INTEGRATION_H
#define __VEMB_V16_SERVER_INTEGRATION_H

struct connection;  /* forward declaration — avoids pulling connection.h here */

int vemb_v16_server_integration_init(void);
void vemb_v16_server_integration_shutdown(void);
int vemb_v16_sniff_and_handoff(struct connection *conn);

#endif
