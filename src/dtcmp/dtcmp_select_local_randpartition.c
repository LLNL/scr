/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-557516.
 * All rights reserved.
 * This file is part of the DTCMP library.
 * For details, see https://github.com/hpc/dtcmp
 * Please also read this file: LICENSE.TXT. */

#include <stdlib.h>
#include <string.h>
#include "mpi.h"
#include "dtcmp_internal.h"

static int dtcmp_select_local_randpartition_keys(
  void* buf,
  int num,
  int k,
  void* item,
  MPI_Datatype key,
  DTCMP_Op cmp,
  DTCMP_Flags hints)
{
  /* randomly pick a pivot value */
  int pivot = rand_r(&dtcmp_rand_seed) % num;

  /* partition around this value, and determine the rank of the pivot value */
  int pivot_rank;
  DTCMP_Partition_local_dtcpy(buf, num, pivot, &pivot_rank, key, key, cmp, hints);

  /* compare the rank of the pivot to the target rank we're looking for */
  if (k < pivot_rank) {
    /* the item is smaller than the pivot, so recurse into lower half of array */
    int num_left = pivot_rank;
    int rc = dtcmp_select_local_randpartition_keys(buf, num_left, k, item, key, cmp, hints);
    return rc;
  } else if (k > pivot_rank) {
    /* the item is larger than the pivot, so recurse into upper half of array */

    /* get lower bound and extent of key */
    MPI_Aint lb, extent;
    MPI_Type_get_extent(key, &lb, &extent);

    /* adjust pointer into array, rank, and number of remaining items */
    int after_pivot = pivot_rank + 1;
    char* offset = (char*)buf + after_pivot * extent;
    int num_left = num - after_pivot;
    int new_k    = k - after_pivot;
    int rc = dtcmp_select_local_randpartition_keys(offset, num_left, new_k, item, key, cmp, hints);
    return rc;
  } else { /* k == pivot_rank */
    /* in this case, the pivot rank is the target rank we're looking for,
     * copy the pivot item into the output item and return */

    /* get lower bound and extent of key */
    MPI_Aint lb, extent;
    MPI_Type_get_extent(key, &lb, &extent);

    /* copy the pivot value into item and return */
    char* pivot_offset = (char*)buf + pivot_rank * extent;
    DTCMP_Memcpy(item, 1, key, pivot_offset, 1, key);
    return DTCMP_SUCCESS;
  }
}

int DTCMP_Select_local_randpartition(
  const void* buf,
  int num,
  int k,
  void* item,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints)
{
  int rc = DTCMP_SUCCESS;

  /* get extent of keysat datatype */
  MPI_Aint lb, extent;
  MPI_Type_get_extent(keysat, &lb, &extent);

  /* get extent of key datatype */
  MPI_Aint key_lb, key_extent;
  MPI_Type_get_extent(key, &key_lb, &key_extent);

  /* get true extent of key datatype */
  MPI_Aint key_true_lb, key_true_extent;
  MPI_Type_get_extent(key, &key_true_lb, &key_true_extent);

  /* allocate an array to hold keys */
  size_t buf_size = key_true_extent * num;
  void* scratch = dtcmp_malloc(buf_size, 0, __FILE__, __LINE__);

  /* copy keys into buffer */
  int i;
  for (i = 0; i < num; i++) {
    char* pos1 = (char*)buf + i * extent;
    char* pos2 = (char*)scratch + i * key_extent;
    DTCMP_Memcpy(pos2, 1, key, pos1, 1, key);
  }

  /* find and copy target rank into item */
  dtcmp_select_local_randpartition_keys(scratch, num, k, item, key, cmp, hints);

  /* free memory */
  dtcmp_free(&scratch);

  return rc;
}
