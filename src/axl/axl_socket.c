#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>

#include "axl_internal.h"
#include "axl_socket.h"
#include "kvtree.h"

/*
 * Flag to state whether the AXL client/server mode of operation is enabled,
 * and if so, whether the code is running as the client or the server.
 */
axl_socket_RunMode axl_service_mode = AXL_SOCKET_DISABLED;

static int axl_socket_socket = -1;

/*
 * Client implementation
 */
int axl_socket_client_init(char* host, unsigned short port)
{
  struct sockaddr_in server;
  struct hostent *hostnm = gethostbyname(host);

  if (hostnm == (struct hostent *) 0) {
    AXL_ERR("Gethostbyname failed: (%s)", strerror(errno));
    return 0;
  }

  if ( (axl_socket_socket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    AXL_ERR("socket() failed: (%s)", strerror(errno));
    return 0;
  }

  memset(&server, 0, sizeof(server));
  server.sin_family = AF_INET;
  server.sin_port = htons(port);
  server.sin_addr.s_addr = *((unsigned long *)hostnm->h_addr);

  if ( connect(axl_socket_socket, (struct sockaddr *)&server, sizeof(server) ) < 0) {
    AXL_ERR("connect() failed: (%s)", strerror(errno));
    close(axl_socket_socket);
    return 0;
  }

  return 1;   // success
}

/*
 * function to perform client-side request to server for AXL_Finalize()
 */
void axl_socket_client_AXL_Finalize()
{
  if (axl_socket_socket >= 0)
    close(axl_socket_socket);
}

/*
 * function to perform client-side request to server for AXL_Config_Set
 */
void axl_socket_client_AXL_Config_Set(const kvtree* config)
{
  ssize_t bytecount;
  axl_socket_Request request;
  axl_socket_Response response;

  request.request = AXL_SOCKET_AXL_CONFIG_SET;
  request.payload_length = (ssize_t)kvtree_pack_size(config);

  bytecount = axl_write_attempt("AXLSVC Client --> AXL_Config_Set_1",
                                  axl_socket_socket, &request, sizeof(request));

  if (bytecount != sizeof(request)) {
    AXL_ABORT(-1, "Unexpected Write Response to server: Expected %d, Got %d",
                  sizeof(request), bytecount);
  }

  bytecount = kvtree_write_fd("AXLSVC Client --> AXL_Config_Set_2",
                                  axl_socket_socket, config);

  if (bytecount != request.payload_length) {
    AXL_ABORT(-1, "Unexpected Write Response to server: Expected %d, Got %d",
                  request.payload_length, bytecount);
  }

  bytecount = axl_read("AXLSVC Client <-- Response",
                                  axl_socket_socket, &response, sizeof(response));

  if (bytecount != sizeof(response)) {
    AXL_ABORT(-1, "Unexpected Write Response to server: Expected %d, Got %d",
                  sizeof(response), bytecount);
  }

  if (response.response != AXL_SOCKET_SUCCESS) {
    AXL_ABORT(-1, "Unexpected Response from server: %d", response.response);
  }
}

/* 
 * Server Implementation
 */

static int time_to_leave = 0;

#define AXL_SOCKET_MAX_CLIENTS 16
struct axl_socket_conn_ctx {
  int sd;                         /* Connection to our socket */
  struct axl_transfer_array xfr;  /* Pointer to client-specific xfer array */
} axl_socket_conn_ctx_array[AXL_SOCKET_MAX_CLIENTS];

#if 0
static kvtree* service_request_AXL_Config_Set(int sd)
{
  kvtree* config = kvtree_new();
  kvtree* rval;
  ssize_t bytecount;

  bytecount = kvtree_read_fd("Service_AXL_Config_Set", sd, config);

  return bytecount;
}
#endif

static ssize_t axl_socket_request_from_client(int sd)
{
  ssize_t bytecount;
  axl_socket_Request req;
  axl_socket_Response response;
  char* buffer;

  bytecount = axl_read("AXLSVC Client Reqeust", sd, &req, sizeof(req));

  if (bytecount == 0) {
    AXL_DBG(2, "Client for socket %d closed", sd);
    return bytecount;
  }

  buffer = malloc(req.payload_length);

  bytecount = axl_read("AXLSVC Reqeust Payload", sd, &buffer, req.payload_length);

  if (bytecount != req.payload_length) {
    AXL_ABORT(-1, "Unexpected Payload Length: Expected %d, Got %d", req.payload_length, bytecount);
  }

  switch (req.request) {
    case AXL_SOCKET_AXL_CONFIG_SET:
      AXL_DBG(1, "AXL_SOCKET_AXL_CONFIG_SET(kfile=%s", buffer);
      response.response = AXL_SOCKET_SUCCESS;
      response.payload_length = 0;
      bytecount = axl_write_attempt("AXLSVC Response to Client", sd, &response, sizeof(response));
      if (bytecount != sizeof(response)) {
        AXL_ABORT(-1, "Unexpected Write Response to client: Expected %d, Got %d",
                      sizeof(response), bytecount);
      }
      break;
    default:
      AXL_ABORT(-1, "AXLSVC Unknown Request Type %d", req.request);
      break;
  }

  free(buffer);
  return bytecount;
}

static void sigterm_handler(int sig, siginfo_t* info, void* ucontext)
{
  AXL_DBG(2, "SIGTERM Received");
  time_to_leave++;
}

static int use_sigterm_to_exit()
{
  struct sigaction act = {0};

  act.sa_flags = 0;
  sigemptyset(&act.sa_mask);
  act.sa_sigaction = sigterm_handler;
  if (sigaction(SIGTERM, &act, NULL) == -1) {
    perror("sigaction");
    return AXL_FAILURE;
  }

  return AXL_SUCCESS;
}

int axl_socket_server_run(int port)
{
  int server_socket;
  int opt = 1;
  struct sockaddr_in address;
  int addrlen;
  int new_socket;
  fd_set readfds;
  int activity;
  int max_sd;
  int rval = AXL_FAILURE;

  axl_service_mode = AXL_SOCKET_SERVER;
  memset(axl_socket_conn_ctx_array, 0, sizeof(axl_socket_conn_ctx_array));

  /*
   * Need to check whether calling AXL_Init() at this point is really appropriate
   */
  if ( (rval = AXL_Init()) != AXL_SUCCESS)
    return rval;

  if ((rval = use_sigterm_to_exit()) != AXL_SUCCESS)
    return rval;

  if ((server_socket = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
    AXL_ABORT(-1, "socket() failed: (%s)", strerror(errno));
  }

  if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(opt)) < 0 ) {
    AXL_ABORT(-1, "setsockopt() failed: (%s)", strerror(errno));
  }

  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(port);

  if (bind(server_socket, (struct sockaddr *)&address, sizeof(address)) < 0) {
    AXL_ABORT(-1, "bind() failed: (%s)", strerror(errno));
  }

  if (listen(server_socket, AXL_SOCKET_MAX_CLIENTS) < 0) {
    AXL_ABORT(-1, "listen() failed: (%s)", strerror(errno));
  }

  addrlen = sizeof(address);

  while (!time_to_leave) {
    FD_ZERO(&readfds);
    FD_SET(server_socket, &readfds);
    max_sd = server_socket;

    for (int i = 0 ; i < AXL_SOCKET_MAX_CLIENTS ; i++) {
      if (axl_socket_conn_ctx_array[i].sd > 0)
        FD_SET(axl_socket_conn_ctx_array[i].sd, &readfds);

      if (axl_socket_conn_ctx_array[i].sd > max_sd)
        max_sd = axl_socket_conn_ctx_array[i].sd;
    }

    activity = select(max_sd + 1 , &readfds , NULL , NULL , NULL);

    if (time_to_leave)
      break;

    if (activity < 0 && errno != EINTR) {
      AXL_ABORT(-1, "select() error: (%s)", strerror(errno));
    }

    if (FD_ISSET(server_socket, &readfds)) {
      AXL_DBG(1, "Accepting new incomming connection");
      if ((new_socket = accept(server_socket, (struct sockaddr *)&address,
                                                (socklen_t*)&addrlen)) < 0) {
        AXL_ABORT(-1, "accept() error: (%s)", strerror(errno));
      }

      for (int i = 0; i < AXL_SOCKET_MAX_CLIENTS; i++) {
        if(axl_socket_conn_ctx_array[i].sd == 0 ){
          axl_socket_conn_ctx_array[i].sd = new_socket;
          break;
        }
      }
      AXL_DBG(1, "Connection established");
    }

    for ( int i = 0; i < AXL_SOCKET_MAX_CLIENTS; i++) {
      if (FD_ISSET(axl_socket_conn_ctx_array[i].sd , &readfds)) {
        axl_xfer_list = &axl_socket_conn_ctx_array[i].xfr;

        if (axl_socket_request_from_client(axl_socket_conn_ctx_array[i].sd) == 0) {
          AXL_DBG(1, "Closing server side socket(%d) to client", axl_socket_conn_ctx_array[i].sd);
          close(axl_socket_conn_ctx_array[i].sd);
          axl_socket_conn_ctx_array[i].sd = 0;
          axl_free(&axl_xfer_list->axl_kvtrees);
          axl_xfer_list->axl_kvtrees_count = 0;
        }
      }
    }
  }
  return 0;
}

