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

/* this is only valid if k== 0 or k==num-1,
 * i.e., looking for min or max value in buf */
int DTCMP_Select_local_ends(
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

  /* get lower bound and extent of key */
  MPI_Aint lb, extent;
  MPI_Type_get_extent(keysat, &lb, &extent);

  /* initialize target to first item and current
   * to second item, scan through list to find
   * min or max item */
  char* target  = (char*)buf;
  char* current = (char*)buf + extent;
  char* last    = (char*)buf + num * extent;
  if (k == 0) {
    /* record address of minimum item */
    while (current < last) {
      int result = dtcmp_op_eval(current, target, cmp);
      if (result < 0) {
        target = current;
      }
      current += extent;
    }
  } else if (k == num-1) {
    /* record address of maximum item */
    while (current < last) {
      int result = dtcmp_op_eval(current, target, cmp);
      if (result > 0) {
        target = current;
      }
      current += extent;
    }
  } else {
    /* this function can only find values for k==0 or k==num-1 */
    return DTCMP_FAILURE;
  }

  /* copy minimum key value into item */
  DTCMP_Memcpy(item, 1, key, target, 1, key);

  return rc;
}
