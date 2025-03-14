/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-557516.
 * All rights reserved.
 * This file is part of the DTCMP library.
 * For details, see https://github.com/hpc/dtcmp
 * Please also read this file: LICENSE.TXT. */

#include <string.h>
#include "dtcmp_internal.h"

int dtcmp_merge_local_2way_memcpy(
  int num,
  const void* inbufs[],
  int counts[],
  void* outbuf,
  size_t size,
  DTCMP_Op cmp,
  DTCMP_Flags hints)
{
  /* setup a pointer to march through elements in output buffer */
  char* out = (char*)outbuf;

  /* compare the smallest element from each list,
   * and copy the smallest element to the merge list */
  const char* buf_a = (const char*) inbufs[0];
  const char* buf_b = (const char*) inbufs[1];
  const char* last_a = buf_a + counts[0] * size;
  const char* last_b = buf_b + counts[1] * size;
  while (buf_a != last_a && buf_b != last_b) {
    int result = dtcmp_op_eval(buf_a, buf_b, cmp);
    if (result <= 0) {
      memcpy(out, buf_a, size);
      buf_a += size;
    } else {
      /* only pick b if it is strictly less than a,
       * so for a stable merge, put first list in a */
      memcpy(out, buf_b, size);
      buf_b += size;
    }
    out += size;
  }

  /* at least one list is empty, copy all elements from
   * the other list (if any) into the merge list */
  if (buf_a != last_a) {
    size_t remainder = last_a - buf_a;
    memcpy(out, buf_a, remainder);
  } else if (buf_b != last_b) {
    size_t remainder = last_b - buf_b;
    memcpy(out, buf_b, remainder);
  }

  return DTCMP_SUCCESS;
}

int DTCMP_Merge_local_2way(
  int num,
  const void* inbufs[],
  int counts[],
  void* outbuf,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints)
{
  /* setup a pointer to march through elements in output buffer */
  char* out = (char*)outbuf;

  /* get size of an element so we can increase our pointer by the correct amount */
  MPI_Aint lb, extent;
  MPI_Type_get_extent(keysat, &lb, &extent);

  /* compare the smallest element from each list,
   * and copy the smallest element to the merge list */
  const char* buf_a = (const char*) inbufs[0];
  const char* buf_b = (const char*) inbufs[1];
  const char* last_a = buf_a + counts[0] * extent;
  const char* last_b = buf_b + counts[1] * extent;
  while (buf_a != last_a && buf_b != last_b) {
    int result = dtcmp_op_eval(buf_a, buf_b, cmp);
    if (result <= 0) {
      DTCMP_Memcpy(out, 1, keysat, buf_a, 1, keysat);
      buf_a += extent;
    } else {
      /* only pick b if it is strictly less than a,
       * so for a stable merge, put first list in a */
      DTCMP_Memcpy(out, 1, keysat, buf_b, 1, keysat);
      buf_b += extent;
    }
    out += extent;
  }

  /* at least one list is empty, copy all elements from
   * the other list (if any) into the merge list */
  if (buf_a != last_a) {
    int remainder = (last_a - buf_a) / extent;
    DTCMP_Memcpy(out, remainder, keysat, buf_a, remainder, keysat);
  } else if (buf_b != last_b) {
    int remainder = (last_b - buf_b) / extent;
    DTCMP_Memcpy(out, remainder, keysat, buf_b, remainder, keysat);
  }

  return DTCMP_SUCCESS;
}
