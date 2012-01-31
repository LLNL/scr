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

#include "mpi.h"
#include "scr_globals.h"

/*
=========================================
Configuration parameters
=========================================
*/

/* read parameters from config file and fill in hash (parallel) */
int scr_config_read(const char* file, scr_hash* hash)
{
  int rc = SCR_FAILURE;

  /* only rank 0 reads the file */
  if (scr_my_rank_world == 0) {
    rc = scr_config_read_common(file, hash);
  }

  /* broadcast whether rank 0 read the file ok */
  MPI_Bcast(&rc, 1, MPI_INT, 0, scr_comm_world);

  /* if rank 0 read the file, broadcast the hash */
  if (rc == SCR_SUCCESS) {
    rc = scr_hash_bcast(hash, 0, scr_comm_world);
  }

  return rc;
}
