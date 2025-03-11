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

/* swap item1 and item2 using memcpy() given some scratch space
 * and the size of each item */
static void dtcmp_swap_memcpy(
  void* item1,
  void* item2,
  void* scratch,
  size_t size)
{
  memcpy(scratch, item1,   size);
  memcpy(item1,   item2,   size);
  memcpy(item2,   scratch, size);
}

/* swap item1 and item2 using DTCMP_Memcpy() given some scratch
 * space and the datatype of each item */
static void DTCMP_Swap(
  void* item1,
  void* item2,
  void* scratch,
  MPI_Datatype type)
{
  DTCMP_Memcpy(scratch, 1, type, item1,   1, type);
  DTCMP_Memcpy(item1,   1, type, item2,   1, type);
  DTCMP_Memcpy(item2,   1, type, scratch, 1, type);
}

int dtcmp_partition_local_memcpy(
  void* buf,
  void* scratch,
  int pivot,
  int num,
  size_t size,
  DTCMP_Op cmp,
  DTCMP_Flags hints)
{
  /* swap pivot element with last element */
  char* pivotbuf = (char*)buf + pivot   * size;
  char* lastbuf  = (char*)buf + (num-1) * size;
  dtcmp_swap_memcpy(pivotbuf, lastbuf, scratch, size);
  
  /* move small elements to left side and large elements to right side of array,
   * split equal elements evenly on either side of pivot */
  int jumpball_arrow = 0; /* basketball reference */
  int divide = 0;
  char* dividebuf = buf;  /* tracks element dividing small and large sides */
  char* itembuf   = buf;
  while (itembuf != lastbuf) {
    /* compare current item to pivot */
    int result = dtcmp_op_eval(itembuf, lastbuf, cmp);
    if (result < 0) {
      /* current element is smaller than pivot,
       * swap this element with the one at the divide and advance divide pointer */
      dtcmp_swap_memcpy(itembuf, dividebuf, scratch, size);
      dividebuf += size;
      divide++;
    } else if (result == 0) {
      /* we put every other equal element on left side */
      if (jumpball_arrow) {
        /* swap this element with the one at the divide and advance divide pointer */
        dtcmp_swap_memcpy(itembuf, dividebuf, scratch, size);
        dividebuf += size;
        divide++;
      }
      /* flip the jumpball arrow for the next tie */
      jumpball_arrow ^= 0x1;
    }

    /* advance pointer to next item */
    itembuf += size;
  }

  /* copy pivot to its proper place */
  dtcmp_swap_memcpy(lastbuf, dividebuf, scratch, size);

  /* return position of pivot */
  return divide;
}

int DTCMP_Partition_local_dtcpy(
  void* buf,
  int count,
  int inpivot,
  int* outpivot,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints)
{
  /* get extent of keysat datatype */
  MPI_Aint lb, extent;
  MPI_Type_get_extent(keysat, &lb, &extent);

  /* get true extent of keysat datatype */
  MPI_Aint true_lb, true_extent;
  MPI_Type_get_true_extent(keysat, &true_lb, &true_extent);

  /* allocate enough space to hold one keysat type */
  void* scratch = dtcmp_malloc(true_extent, 0, __FILE__, __LINE__);

  /* swap pivot element with last element */
  char* pivotbuf = (char*)buf + inpivot   * extent;
  char* lastbuf  = (char*)buf + (count-1) * extent;
  DTCMP_Swap(pivotbuf, lastbuf, scratch, keysat);
  
  /* move small elements to left side and large elements to right side of array,
   * split equal elements evenly on either side of pivot */
  int jumpball_arrow = 0; /* basketball reference */
  int divide = 0;
  char* dividebuf = buf;  /* tracks element dividing small and large sides */
  char* itembuf   = buf;
  while (itembuf != lastbuf) {
    /* compare current item to pivot */
    int result = dtcmp_op_eval(itembuf, lastbuf, cmp);
    if (result < 0) {
      /* current element is smaller than pivot,
       * swap this element with the one at the divide and advance divide pointer */
      DTCMP_Swap(itembuf, dividebuf, scratch, keysat);
      dividebuf += extent;
      divide++;
    } else if (result == 0) {
      /* we put every other equal element on left side */
      if (jumpball_arrow) {
        /* swap this element with the one at the divide and advance divide pointer */
        DTCMP_Swap(itembuf, dividebuf, scratch, keysat);
        dividebuf += extent;
        divide++;
      }
      /* flip the jumpball arrow for the next tie */
      jumpball_arrow ^= 0x1;
    }

    /* advance pointer to next item */
    itembuf += extent;
  }

  /* copy pivot to its proper place */
  DTCMP_Swap(lastbuf, dividebuf, scratch, keysat);

  /* set output pivot position */
  *outpivot = divide;

  /* free memory */
  dtcmp_free(&scratch);

  return DTCMP_SUCCESS;
}

int DTCMP_Partition_local_target_dtcpy(
  void* buf,
  int count,
  const void* target,
  int* outindex,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints)
{
  /* get extent of keysat datatype */
  MPI_Aint lb, extent;
  MPI_Type_get_extent(keysat, &lb, &extent);

  /* get true extent of keysat datatype */
  MPI_Aint true_lb, true_extent;
  MPI_Type_get_true_extent(keysat, &true_lb, &true_extent);

  /* allocate enough space to hold one keysat type */
  void* scratch = dtcmp_malloc(true_extent, 0, __FILE__, __LINE__);

  /* swap pivot element with last element */
  char* lastbuf = (char*)buf + count * extent;
  
  /* move small elements to left side and large elements to right side of array,
   * split equal elements evenly on either side of pivot */
  int jumpball_arrow = 0; /* basketball reference */
  int divide = 0;
  char* dividebuf = buf;  /* tracks element dividing small and large sides */
  char* itembuf   = buf;
  while (itembuf != lastbuf) {
    /* compare current item to pivot */
    int result = dtcmp_op_eval(itembuf, target, cmp);
    if (result < 0) {
      /* current element is smaller than pivot,
       * swap this element with the one at the divide and advance divide pointer */
      DTCMP_Swap(itembuf, dividebuf, scratch, keysat);
      dividebuf += extent;
      divide++;
    } else if (result == 0) {
      /* we put every other equal element on left side */
      if (jumpball_arrow) {
        /* swap this element with the one at the divide and advance divide pointer */
        DTCMP_Swap(itembuf, dividebuf, scratch, keysat);
        dividebuf += extent;
        divide++;
      }
      /* flip the jumpball arrow for the next tie */
      jumpball_arrow ^= 0x1;
    }

    /* advance pointer to next item */
    itembuf += extent;
  }

  /* set output pivot position */
  *outindex = divide;

  /* free memory */
  dtcmp_free(&scratch);

  return 0;
}

int DTCMP_Partition_local_target_list_dtcpy(
  void* buf,
  int count,
  int offset,
  int num,
  const void* targets,
  int indicies[],
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints)
{
  /* TODO: shortcut to search if buf is ordered */

  /* get extent of key datatype */
  MPI_Aint key_lb, key_extent;
  MPI_Type_get_extent(key, &key_lb, &key_extent);

  /* get extent of keysat datatype */
  MPI_Aint keysat_lb, keysat_extent;
  MPI_Type_get_extent(keysat, &keysat_lb, &keysat_extent);

  /* select middle target */
  int mid = num / 2;
  char* target = (char*)targets + mid * key_extent;

  /* partition array by middle target, returns index within
   * array where target would be after partitioning if it exists */
  int index;
  DTCMP_Partition_local_target_dtcpy(
    buf, count, target, &index,
    key, keysat, cmp, hints
  );
  indicies[mid] = offset + index;

  /* recursively partition lower array */
  if (mid > 0) {
    DTCMP_Partition_local_target_list_dtcpy(
      buf, index, offset, mid, targets, indicies,
      key, keysat, cmp, hints
    );
  }

  /* recursively partition upper array */
  char* upper_buf  = (char*)buf + index * keysat_extent;
  int upper_count  = count - index;
  int upper_offset = offset + index;
  int upper_index  = mid + 1;
  int upper_num    = num - upper_index;
  char* upper_targets = (char*)targets + upper_index * key_extent;
  if (upper_num > 0) {
    DTCMP_Partition_local_target_list_dtcpy(
      upper_buf, upper_count, upper_offset,
      upper_num, upper_targets, indicies + upper_index,
      key, keysat, cmp, hints
    );
  }
  
  return DTCMP_SUCCESS;
}
