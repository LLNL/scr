// mpig++ -g -O0 -I../src -o test_ckpt test_ckpt.cpp -L/usr/local/tools/scr-1.1/lib -lscr
/*
 * Usage:
 *
 *      ./test_ckpt [megabytes]
 *
 * Optionally pass the size of our checkpoint file to write, in megabytes.
 *
 * Note: This is compiled as C++ file to verify SCR works with C++.  It only
 * trivially uses C++ semantics, so that people can use it as example code for
 * C as well.
 */
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <assert.h>
#include <stdlib.h>
#include <iostream>
using namespace std;

#include "mpi.h"
#include "scr.h"
int checkpoint(int size_mb)
{
    int rank;
    char tmp[256];
    char file[SCR_MAX_FILENAME];
    char dir[SCR_MAX_FILENAME];
    char dname;
    int rc;

    /* Get our rank */
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* Inform SCR that we are starting a new checkpoint */
    SCR_Start_checkpoint();

    /* Build the filename for our checkpoint file */
    sprintf(tmp, "rank_%d", rank);
    cout << "In: " << tmp << "\n";

    /* Register our checkpoint file with SCR, and where to write it */
    SCR_Route_file(tmp, file);

    /* Write our checkpoint file */
    sprintf(tmp, "truncate -s %dM %s", size_mb, file);
    system(tmp);

    cout << "Out: " << file << "\n";

    /* Tell SCR whether this process wrote its checkpoint files successfully */
    SCR_Complete_checkpoint(1);

    return 0;
}

/*
 * test_ckpt [megabytes]
 *
 * Optionally pass the size of our checkpoint file to write, in megabytes.
 */
int main(int argc, char **argv)
{
    int size_mb = 1;

    if (argc == 2) {
        size_mb = atoi(argv[1]);
    }

    MPI_Init(NULL, NULL);

    /* Initialize the SCR library */
    SCR_Init();

    /* Ask SCR whether we need to checkpoint */
    int flag = 0;
    SCR_Need_checkpoint(&flag);
    if (flag) {
        /* Execute the checkpoint code */
        checkpoint(size_mb);
    }

    /* Shut down the SCR library */
    SCR_Finalize();

    MPI_Finalize();

    return 0;
}
