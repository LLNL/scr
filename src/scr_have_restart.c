// scr_have_restart.c
#include "scr.h"
#include "mpi.h"
#include <stdio.h>

int main(int *argc, char ***argv)
{
    char fname[SCR_MAX_FILENAME];
    int flag;
    int rank;
    int retval = SCR_FAILURE;

    // TODO: add error checking to all MPI and SCR calls
    MPI_Init(NULL, NULL);
    SCR_Init();
    retval = SCR_Have_restart(&flag, fname);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if ( (rank == 0) && (retval == SCR_SUCCESS) )
    {
        printf("%s\n", fname);
    }
}
