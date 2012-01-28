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

#ifndef SCR_UTIL_MPI_H
#define SCR_UTIL_MPI_H

/* returns true (non-zero) if flag on each process in scr_comm_world is true */
int scr_alltrue(int flag);

/* given a comm as input, find the left and right partner ranks and hostnames */
int scr_set_partners(
  MPI_Comm comm, int dist,
  int* lhs_rank, int* lhs_rank_world, char* lhs_hostname,
  int* rhs_rank, int* rhs_rank_world, char* rhs_hostname
);

#endif
