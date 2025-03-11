/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-568372.
 * All rights reserved.
 * This file is part of the LWGRP library.
 * For details, see https://github.com/hpc/lwgrp
 * Please also read this file: LICENSE.TXT. */

#ifndef _LWGRP_COMM_SPLIT_H
#define _LWGRP_COMM_SPLIT_H

#include "mpi.h"

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/* lwgrp_comm_split_members(comm, color, key, size, members)
 *
 * IN  comm    - MPI communicator on which to perform split (handle)
 * IN  color   - color value, same meaning as MPI_COMM_SPLIT (integer)
 * IN  key     - key value, same meaning as in MPI_COMM_SPLIT (integer)
 * OUT size    - size of output group after split (non-negative integer)
 * OUT members - array of ordered members in group after split (array of
 *               non-negative integers)
 *
 * The members array must be allocated and passed in by caller.
 * It should be big enough to store ranks from largest group after split,
 * so to be safe, it should be as large as size(comm). */

int lwgrp_comm_split_members(
  MPI_Comm comm,
  int color,
  int key,
  int tag1,
  int tag2,
  int* size,
  int members[]
);

/* same as above but returns a newly created communicator via MPI_Comm_create,
 * can only use in MPI-2.2 and later and still not useful unless MPI_COMM_CREATE
 * is scalable */
int lwgrp_comm_split_create(
  MPI_Comm comm,
  int color,
  int key,
  int tag1,
  int tag2,
  MPI_Comm* newcomm
);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif /* _LWGRP_COMM_SPLIT_H */
