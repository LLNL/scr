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

#ifndef SCR_SPLIT_H
#define SCR_SPLIT_H

/* Given a communicator and a string, compute number of unique strings
 * across all procs in comm and compute an id for input string
 * such that the id value matches another process if and only if that
 * process specified an identical string. The groupid value will range
 * from 0 to groups-1. */
int scr_rank_str(
  MPI_Comm comm,   /* IN  - communicator of processes (handle) */
  const char* str, /* IN  - input string (pointer) */
  int* groups,     /* OUT - number of unique strings (non-negative integer) */
  int* groupid     /* OUT - id for input string (non-negative integer) */
);

#endif
