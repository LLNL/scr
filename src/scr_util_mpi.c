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

#include "scr_globals.h"

/*
=========================================
MPI utility functions
=========================================
*/

/* returns true (non-zero) if flag on each process in scr_comm_world is true */
int scr_alltrue(int flag)
{
  int all_true = 0;
  MPI_Allreduce(&flag, &all_true, 1, MPI_INT, MPI_LAND, scr_comm_world);
  return all_true;
}

/* given a comm as input, find the left and right partner ranks and hostnames */
int scr_set_partners(
  MPI_Comm comm, int dist,
  int* lhs_rank, int* lhs_rank_world, char* lhs_hostname,
  int* rhs_rank, int* rhs_rank_world, char* rhs_hostname)
{
  /* find our position in the communicator */
  int my_rank, ranks;
  MPI_Comm_rank(comm, &my_rank);
  MPI_Comm_size(comm, &ranks);

  /* shift parter distance to a valid range */
  while (dist > ranks) {
    dist -= ranks;
  }
  while (dist < 0) {
    dist += ranks;
  }

  /* compute ranks to our left and right partners */
  int lhs = (my_rank + ranks - dist) % ranks;
  int rhs = (my_rank + ranks + dist) % ranks;
  (*lhs_rank) = lhs;
  (*rhs_rank) = rhs;

  /* fetch hostnames from my left and right partners */
  strcpy(lhs_hostname, "");
  strcpy(rhs_hostname, "");

  MPI_Request request[2];
  MPI_Status  status[2];

  /* shift hostnames to the right */
  MPI_Irecv(lhs_hostname,    sizeof(scr_my_hostname), MPI_CHAR, lhs, 0, comm, &request[0]);
  MPI_Isend(scr_my_hostname, sizeof(scr_my_hostname), MPI_CHAR, rhs, 0, comm, &request[1]);
  MPI_Waitall(2, request, status);

  /* shift hostnames to the left */
  MPI_Irecv(rhs_hostname,    sizeof(scr_my_hostname), MPI_CHAR, rhs, 0, comm, &request[0]);
  MPI_Isend(scr_my_hostname, sizeof(scr_my_hostname), MPI_CHAR, lhs, 0, comm, &request[1]);
  MPI_Waitall(2, request, status);

  /* shift rank in scr_comm_world to the right */
  MPI_Irecv(lhs_rank_world,     1, MPI_INT, lhs, 0, comm, &request[0]);
  MPI_Isend(&scr_my_rank_world, 1, MPI_INT, rhs, 0, comm, &request[1]);
  MPI_Waitall(2, request, status);

  /* shift rank in scr_comm_world to the left */
  MPI_Irecv(rhs_rank_world,     1, MPI_INT, rhs, 0, comm, &request[0]);
  MPI_Isend(&scr_my_rank_world, 1, MPI_INT, lhs, 0, comm, &request[1]);
  MPI_Waitall(2, request, status);

  return SCR_SUCCESS;
}
