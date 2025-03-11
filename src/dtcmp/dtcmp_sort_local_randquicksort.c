/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-557516.
 * All rights reserved.
 * This file is part of the DTCMP library.
 * For details, see https://github.com/hpc/dtcmp
 * Please also read this file: LICENSE.TXT. */

#include <stdlib.h>
#include "mpi.h"
#include "dtcmp_internal.h"

static int DTCMP_Sort_local_randquicksort_scratch(
  void* buf,
  void* scratch,
  int num,
  size_t size,
  DTCMP_Op cmp,
  DTCMP_Flags hints)
{
  /* already sorted if we have 1 or fewer elements */
  if (num <= 1) {
    return DTCMP_SUCCESS;
  }

  /* TODO: if count is small enough, just go with something like
   * insertion sort */

  /* identify pivot value */
//  int pivot = 0;
  int pivot = rand_r(&dtcmp_rand_seed) % num;

  /* parition items in buf around pivot */
  int index = dtcmp_partition_local_memcpy(
    buf, scratch, pivot, num, size, cmp, hints
  );

  /* sort items less than pivot */
  int lowcount = index;
  DTCMP_Sort_local_randquicksort_scratch(
    buf, scratch, lowcount, size, cmp, hints
  );

  /* sort items larger than pivot */
  int highcount = num - index - 1;
  void* highbuf = (char*)buf + (index + 1) * size;
  DTCMP_Sort_local_randquicksort_scratch(
    highbuf, scratch, highcount, size, cmp, hints
  );

  return DTCMP_SUCCESS;
}

/* execute a purely local sort */
int DTCMP_Sort_local_randquicksort(
  const void* inbuf, 
  void* outbuf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints)
{
  int rc = DTCMP_SUCCESS;

  /* get true extent of keysat type */
  MPI_Aint lb, extent;
  MPI_Type_get_true_extent(keysat, &lb, &extent);

  if (count > 0 && extent > 0) {
    /* copy data to outbuf if it's not already there */
    if (inbuf != DTCMP_IN_PLACE) {
      DTCMP_Memcpy(outbuf, count, keysat, inbuf, count, keysat);
    }

    /* allocate scratch space to hold one keysat type */
    void* scratch = dtcmp_malloc(extent, 0, __FILE__, __LINE__);

    /* execute quicksort */
    size_t size = extent;
    rc = DTCMP_Sort_local_randquicksort_scratch(
      outbuf, scratch, count, size, cmp, hints
    );

    /* free scratch space */
    dtcmp_free(&scratch);
  }

  return rc;
}
