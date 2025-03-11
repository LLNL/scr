/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-557516.
 * All rights reserved.
 * This file is part of the DTCMP library.
 * For details, see https://github.com/hpc/dtcmp
 * Please also read this file: LICENSE.TXT. */

#include <string.h>
#include "mpi.h"
#include "dtcmp_internal.h"

/* execute qsort to sort local data */
static int dtcmp_sort_local_mergesort_scratch(
  void* buf,
  void* scratch,
  int count,
  size_t size,
  DTCMP_Op cmp,
  DTCMP_Flags hints)
{
  /* already sorted if we have 1 or fewer elements */
  if (count <= 1) {
    return DTCMP_SUCCESS;
  }

  /* TODO: if count is small enough, just go with something like insertion sort */

  /* divide count in two */
  int counts[2];
  counts[0] = count / 2;
  counts[1] = count - counts[0];

  /* set up pointers to our buffers */
  void* bufs[2];
  bufs[0] = (char*)buf;
  bufs[1] = (char*)buf + counts[0] * size;

  /* sort each half and merge them back together */
  dtcmp_sort_local_mergesort_scratch(bufs[0], scratch, counts[0], size, cmp, hints);
  dtcmp_sort_local_mergesort_scratch(bufs[1], scratch, counts[1], size, cmp, hints);
  dtcmp_merge_local_2way_memcpy(2, (const void**)bufs, counts, scratch, size, cmp, hints);

  /* copy data from scratch back to our buffer */
  memcpy(buf, scratch, count * size);

  return DTCMP_SUCCESS;
}

/* execute a purely local sort */
int DTCMP_Sort_local_mergesort(
  const void* inbuf, 
  void* outbuf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints)
{
  int rc = DTCMP_SUCCESS;

  MPI_Aint lb, extent;
  MPI_Type_get_true_extent(keysat, &lb, &extent);

  if (count > 0 && extent > 0) {
    /* copy data to outbuf if it's not already there */
    if (inbuf != DTCMP_IN_PLACE) {
      DTCMP_Memcpy(outbuf, count, keysat, inbuf, count, keysat);
    }

    /* allocate scratch space */
    void* scratch = dtcmp_malloc(count * extent, 0, __FILE__, __LINE__);

    /* execute our merge sort */
    size_t size = (size_t) extent;
    rc = dtcmp_sort_local_mergesort_scratch(outbuf, scratch, count, size, cmp, hints);

    /* free scratch space */
    dtcmp_free(&scratch);
  }

  return rc;
}
