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

/*
=========================================
Functions to send/recv strings
=========================================
*/

/* sends a NUL-terminated string to a process,
 * allocates space and recieves a NUL-terminated string from a process,
 * can specify MPI_PROC_NULL as either send or recv rank */
int scr_str_sendrecv(
  const char* send_str, int send_rank,
  char** recv_str, int recv_rank,
  MPI_Comm comm
);

/* broadcast a string from root process,
 * newly allocated string is returned in str on all non-root procs,
 * non-root procs must free string when done with it */
int scr_str_bcast(char** str, int root, MPI_Comm comm);

/* broadcast a string from root process into buffer provided
 * by caller, checks that string is n bytes or less */
int scr_strn_bcast(char* str, int n, int root, MPI_Comm comm);

/*
=========================================
MPI utility functions
=========================================
*/

/* returns true (non-zero) if flag on each process in scr_comm_world is true */
int scr_alltrue(int flag);

/* given a comm as input, find the left and right partner ranks and hostnames */
int scr_set_partners(
  MPI_Comm comm, int dist,
  int* lhs_rank, int* lhs_rank_world, char** lhs_hostname,
  int* rhs_rank, int* rhs_rank_world, char** rhs_hostname
);

#endif
