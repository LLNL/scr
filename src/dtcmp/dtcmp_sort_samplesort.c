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

int DTCMP_Sort_samplesort(
  const void* inbuf,
  void* outbuf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  int rc = DTCMP_SUCCESS;

  /* use sample sort (sortw) to sort the data */
  DTCMP_Handle handle;
  void* tmpbuf;
  int tmpcount;
  DTCMP_Sortz_samplesort(inbuf, count, &tmpbuf, &tmpcount, key, keysat, cmp, hints, comm, &handle);

  /* distribute data from a sortw to sort */
  dtcmp_sortz_to_sort(tmpbuf, tmpcount, outbuf, count, key, keysat, cmp, hints, comm);

  /* free the handle */
  DTCMP_Free(&handle);

  return rc;
}
