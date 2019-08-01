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

#include "kvtree.h"
#include "kvtree_mpi.h"

#include <assert.h>

/*
=========================================
Configuration parameters
=========================================
*/

/* read parameters from config file and fill in hash (parallel) */
int scr_config_read(const char* file, kvtree* hash)
{
  int rc = SCR_FAILURE;

  /* scr_config_read is called from scr_app_hash_init which runs before
   * SCR_Init so MPI may not have been properly set up yet.
   */
  /* TODO: this is quite bad, not sure if one could first store all
   * scr_config settings on all ranks, then in scr_param_init, once MPI is
   * available, have rank 0 bcast its results. Problem is how ot read in
   * app.conf before that on only one rank, or find out how to correctly
   * merge it in afterwards such that subkeys stay deleted. */
  if (scr_comm_world == MPI_COMM_NULL) {
    MPI_Comm_dup(MPI_COMM_WORLD,  &scr_comm_world);
    MPI_Comm_rank(scr_comm_world, &scr_my_rank_world);
    MPI_Comm_size(scr_comm_world, &scr_ranks_world);
  }
  assert(scr_ranks_world > 0);
  assert(scr_comm_world != MPI_COMM_NULL);

  /* only rank 0 reads the file */
  if (scr_my_rank_world == 0) {
    rc = scr_config_read_common(file, hash);
  }

  /* broadcast whether rank 0 read the file ok */
  MPI_Bcast(&rc, 1, MPI_INT, 0, scr_comm_world);

  /* if rank 0 read the file, broadcast the hash */
  if (rc == SCR_SUCCESS) {
    int kvtree_rc = kvtree_bcast(hash, 0, scr_comm_world);
    rc = (kvtree_rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
  }

  return rc;
}
