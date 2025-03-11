/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-557516.
 * All rights reserved.
 * This file is part of the DTCMP library.
 * For details, see https://github.com/hpc/dtcmp
 * Please also read this file: LICENSE.TXT. */

#include "mpi.h"
#include "dtcmp_internal.h"

/* execute a purely local sort */
int DTCMP_Sort_local_insertionsort(
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
    void* scratch = dtcmp_malloc(extent, 0, __FILE__, __LINE__);

    int i;
    for (i = 1; i < count; i++) {
      /* get pointer to current item and the item that preceeds it */
      char* current  = (char*)outbuf + i * extent;
      char* previous = current - extent;

      /* compare these two items */
      int result = dtcmp_op_eval(previous, current, cmp);
      if (result > 0) {
        /* current item is larger than item that preceeds it,
         * copy current item to scratch buffer */
        DTCMP_Memcpy(scratch, 1, keysat, current, 1, keysat);

        /* now step through elements, copying them one slot to the right
         * until we find one that is less than or equal */
        while (result > 0) {
          /* copy element one position to the right */
          DTCMP_Memcpy(current, 1, keysat, previous, 1, keysat);

          /* move both points down */
          current  -= extent;
          previous -= extent;
          if (previous >= (char*)outbuf) {
            /* compare this new element to the one we have in scratch */
            result = dtcmp_op_eval(previous, scratch, cmp);
          } else {
            /* in this case, we've gone through the whole array,
             * so our item must be the first item on the list */
            break;
          }
        }

        /* now copy current item into its proper place */
        DTCMP_Memcpy(current, 1, keysat, scratch, 1, keysat);
      }
    }
  
    /* free scratch space */
    dtcmp_free(&scratch);
  }

  return rc;
}
