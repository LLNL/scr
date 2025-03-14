#ifndef AXL_SOCKET_H
#define AXL_SOCKET_H

#include <stddef.h>
#include "kvtree.h"

#if defined(__cplusplus)
extern "C" {
#endif

typedef enum {
   AXL_SOCKET_DISABLED = 0, /* Default - Not utilizing AXL service (lib only) */
   AXL_SOCKET_CLIENT = 1,   /* Using AXL service and we are the client */
   AXL_SOCKET_SERVER = 2    /* Using AXL service and we are the server */
} axl_socket_RunMode;

/*
 * Flag to state whether the AXL client/server mode of operation is enabled,
 * and if so, whether the code is running as the client or the server.
 */
extern axl_socket_RunMode axl_service_mode;

typedef enum {
   AXL_SOCKET_AXL_CONFIG_SET = 0,     /* payload is config kvtree hash buffer */
} axl_socket_request_t;

typedef struct {
   axl_socket_request_t request;
   ssize_t payload_length;
} axl_socket_Request;

typedef enum {
   AXL_SOCKET_SUCCESS =  0,
   AXL_SOCKET_FAILURE = -1,
} axl_socket_response_t;

typedef struct {
   axl_socket_response_t response;
   ssize_t payload_length;    // Optional error/status string
} axl_socket_Response;

int axl_socket_client_init(char* host, unsigned short port);

/*
 * function to perform client-side request to server for AXL_Finalize()
 */
void axl_socket_client_AXL_Finalize();

/*
 * function to perform client-side request to server for AXL_Config_Set
 */
void axl_socket_client_AXL_Config_Set(const kvtree* config);

int axl_socket_server_run(int port);

#if defined(__cplusplus)
extern "C" }
#endif

#endif /* AXL_SOCKET_H */
