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

#ifndef SCR_PATH_MPI_H
#define SCR_PATH_MPI_H

#include "mpi.h"

#include "scr_path.h"

#if 0
/* send/recv path, recv_path should be from scr_path_new() */
int scr_path_sendrecv(
  const scr_path* send_path,
  int send_rank,
  scr_path* recv_path,
  int recv_rank,
  MPI_Comm comm
);
#endif

/* broadcast path, path should be from scr_path_new() on non-roots */
int scr_path_bcast(scr_path* path, int root, MPI_Comm comm);

#endif /* SCR_PATH_MPI_H */
