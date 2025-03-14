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

/* execute qsort to sort local data
 * can do this if keysat is contiguous and if compare is basic op */
int DTCMP_Sort_local_qsort(
  const void* inbuf, 
  void* outbuf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints)
{
  /* get a pointer to our comparison op struct */
  dtcmp_op_handle_t* c = (dtcmp_op_handle_t*) cmp;

  /* get extent of each item */
  MPI_Aint lb, extent;
  MPI_Type_get_extent(keysat, &lb, &extent);
  size_t width = (size_t) extent;

  /* if data is not already in place, copy it from inbuf to outbuf */
  if (inbuf != DTCMP_IN_PLACE) {
    DTCMP_Memcpy(outbuf, count, keysat, inbuf, count, keysat);
  }

  /* finally, sort the data in outbuf */
  qsort(outbuf, count, width, c->fn);

  return DTCMP_SUCCESS;
}
