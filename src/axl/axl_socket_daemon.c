#include <stdio.h>
#include <stdlib.h>

#include "axl_internal.h"
#include "axl_socket.h"

int main(int argc , char *argv[])
{
  int rval = AXL_FAILURE;

  if (argc == 2 && atoi(argv[1]) > 0) {
    rval = axl_socket_server_run(atoi(argv[1]));
  } else {
    fprintf(stderr, "Usage: %s <port number>\n", argv[0]);
  }

  return rval;
}
