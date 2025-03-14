#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>

#include "axl.h"

#define AXLCS_SUCCESS					0
#define AXLCS_CLIENT_INVALID			1
#define AXLCS_SERVICE_CREATION_FAILURE	1000
#define AXLCS_SERVICE_KILLED			2000
#define AXLCS_SERVICE_FAIL 				3000

extern int axl_socket_server_run(int port);

int run_service()
{
	fprintf(stdout, "Service Started!\n");
	int rval = axl_socket_server_run(2000);
	fprintf(stdout, "Service Ending!\n");
	return rval;
}

int run_client()
{
	int rval;

	if ((rval = AXL_Init()) != AXL_SUCCESS) {
		fprintf(stderr, "Call to AXL_Init failed with code: %d\n", rval);
	} else {
		if ((rval = AXL_Finalize()) != AXL_SUCCESS) {
			fprintf(stderr, "Call to AXL_Init failed with code: %d\n", rval);
		}
	}

	return rval;
}

int main(int ac, char **av)
{
	fprintf(stderr, "Just testing stderr...\n");
	if (ac != 2) {
		fprintf(stderr, "Command count (%d) incorrect:\nUsage: test_client_server --<client|server>\n", ac);
		return AXLCS_CLIENT_INVALID;
	}

	if (strcmp("--server", av[1]) == 0) {
		return run_service();
	}
	else if (strcmp("--client", av[1]) == 0) {
		return run_client();
	}

	fprintf(stderr, "Unknown Argument (%s) incorrect:\nUsage: test_client_server --<client|server>\n", av[1]);
	return AXLCS_CLIENT_INVALID;
}