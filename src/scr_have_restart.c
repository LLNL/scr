/*
 * Copyright (c) 2009, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-411039.
 * All rights reserved.
 * This file is part of The Scalable Checkpoint / Restart (SCR) library.
 * For details, see https://sourceforge.net/projects/scalablecr/
 * Please also read this file: LICENSE.TXT.
*/

/* Utility to print the name of the latest restart */

#include "scr.h"
#include "mpi.h"
#include <stdio.h>

int main(int *argc, char ***argv)
{
    char fname[SCR_MAX_FILENAME];
    char *myname  = "scr_have_restart";
    int flag;
    int rank;
    int rc;

    rc = MPI_Init(NULL, NULL);
    if (rc != MPI_SUCCESS) {
        fprintf(stderr, "%s: MPI_Init failed %d", myname, rc);
    }

    rc = SCR_Init();
    if (rc != SCR_SUCCESS) {
        fprintf(stderr, "%s: SCR_Init failed %d", myname, rc);
    }

    rc = SCR_Have_restart(&flag, fname);
    if (rc != SCR_SUCCESS) {
        fprintf(stderr, "%s: SCR_Have_restart failed %d", myname, rc);
    }

    rc = MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if (rc != MPI_SUCCESS) {
        fprintf(stderr, "%s: MPI_Comm_rank failed %d", myname, rc);
    }

    if ( (rank == 0) && (flag > 0) ) {
        printf("%s\n", fname);

    }

    MPI_Finalize();
    return 0;
}
