/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-557516.
 * All rights reserved.
 * This file is part of the DTCMP library.
 * For details, see https://github.com/hpc/dtcmp
 * Please also read this file: LICENSE.TXT. */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "mpi.h"
#include "dtcmp_internal.h"

/* parallel extension of random select + partition
 *
 * algorithm:
 *   1) each process locally sorts its data
 *   2) set active range to all elements
 *   loop until correct item is identified
 *   3) each process picks median of its active range
 *   4) random broadcast of median value (bcast in which
 *      root is choosen at random with restriction that
 *      root must have a valid element)
 *   5) binary search for item in active range
 *   6) determine whether target rank of is less than,
 *      equal to, or greater than rank of current item
 *   7) adjust range and repeat loop
 *
 * on average the loop should take O(log n * log p) iterations,
 * note that initial sort takes O(n/p log n/p) time but allows
 * for binary search for item each iteration, the sort could be
 * avoided at cost of requiring a partition with each iteration,
 * not immediately clear which is fastest. */

#define LT (0)
#define EQ (1)

static int find_kth_item(
  const void* data,
  int n,
  uint64_t k,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  int* found_flag,
  void* found_key,
  MPI_Comm comm)
{
  /* TODO: check that hints say data is locally sorted */
  int sorted = (hints & (DTCMP_FLAG_SORTED | DTCMP_FLAG_SORTED_LOCAL));
  if (! sorted) {
    printf("ERROR: data must be locally sorted @ %s:%d\n",__FILE__, __LINE__);
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  /* get true extent of key so we can allocate space to copy them into */
  MPI_Aint keysat_lb, keysat_extent;
  MPI_Type_get_extent(keysat, &keysat_lb, &keysat_extent);

  /* get true extent of key so we can allocate space to copy them into */
  MPI_Aint key_true_lb, key_true_extent;
  MPI_Type_get_true_extent(key, &key_true_lb, &key_true_extent);
  size_t size_key = key_true_extent;

  /* tracks current lowest index and number of elements in our active
   * range */
  int index = 0;
  int num = n;

  /* to hold our candidate value */
  char* my_val = dtcmp_malloc(size_key, 0, __FILE__, __LINE__); 

  /* to receive globally selected candidate value */
  char* out_val = dtcmp_malloc(size_key, 0, __FILE__, __LINE__);

  /* iterate until we identify the specified rank, or determine that
   * it does not exist */
  int found_exact = 0;
  while (1) {
    /* copy our item in if we have any left */
    if (num > 0) {
      /* copy in median element */
      int median_index = (num / 2) + index;
      if (median_index < n) {
        const void* median = (char*)data + median_index * keysat_extent;
        DTCMP_Memcpy(my_val, 1, key, median, 1, key);
      }
    }

    /* pick a random item from all tasks */
    int flag;
    dtcmp_randbcast(my_val, num, out_val, &flag, 1, key, comm);

    /* this shouldn't happen */
    if (flag == 0) {
      printf("ERROR: random bcast failed @ %s:%d\n",__FILE__, __LINE__);
      MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* if we don't have any active elements, set counts to 0 */
    uint64_t counts[2];
    if (num == 0) {
      counts[LT] = 0;
      counts[EQ] = 0;
    } else {
      /* get proposed split point */
      void* target = out_val;

      /* use binary search to determine indicies denoting elements
       * less-than and greater than M */
      int start_index = index;
      int end_index   = index + num - 1;
      int flag, lowest, highest;
      DTCMP_Search_low_local(target,  data, start_index, end_index, key, keysat, cmp, hints, &flag, &lowest);
      DTCMP_Search_high_local(target, data, lowest,      end_index, key, keysat, cmp, hints, &flag, &highest);
      counts[LT] = (uint64_t) (lowest - start_index);
      counts[EQ] = (uint64_t) ((highest + 1) - lowest);
    }

    /* now get global counts across all procs */
    uint64_t all_counts[2];
    MPI_Allreduce(counts, all_counts, 2, MPI_UINT64_T, MPI_SUM, comm);

    /* based on current median, chop down our active range */
    if (k <= all_counts[LT]) {
      /* the target is in the lower portion,
       * exclude all entries equal to or greater than */
      num = (int) counts[LT];
    } else if (k > (all_counts[LT] + all_counts[EQ])){
      /* the target is in the higher portion,
       * exclude all entries equal to or less than */
      int num_lte = (int) (counts[LT] + counts[EQ]);
      index += num_lte;
      num   -= num_lte;
      k = k - (all_counts[LT] + all_counts[EQ]);
    } else { /* all_counts[LT] < k && k <= (all_counts[LT] + all_counts[EQ]) */
      /* found our target exactly, we're done */
      found_exact = 1;
      break;
    }
  }

  /* determine whether we found the specified rank,
   * and copy its key into the output buffer */
  *found_flag = found_exact;
  if (found_exact != 0) {
    DTCMP_Memcpy(found_key, 1, key, out_val, 1, key);
  }

  /* free off our scratch space */
  dtcmp_free(&out_val);
  dtcmp_free(&my_val);

  return DTCMP_SUCCESS;
}

int DTCMP_Selectv_rand(
  const void* buf,
  int num,
  uint64_t k,
  void* item,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  int rc = DTCMP_SUCCESS;

  /* get extent of keysat datatype */
  MPI_Aint keysat_lb, keysat_extent;
  MPI_Type_get_extent(keysat, &keysat_lb, &keysat_extent);

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
    char* pos1 = (char*)buf + i * keysat_extent;
    char* pos2 = (char*)scratch + i * key_extent;
    DTCMP_Memcpy(pos2, 1, key, pos1, 1, key);
  }

  /* sort keys locally */
  DTCMP_Sort_local(DTCMP_IN_PLACE, scratch, num, key, key, cmp, hints);
  hints |= DTCMP_FLAG_SORTED_LOCAL;

  /* find and copy target rank into item */
  int found;
  find_kth_item(scratch, num, k, key, key, cmp, hints, &found, item, comm);
  if (! found) {
    printf("ERROR: could not find rank=%llu @ %s:%d\n", (unsigned long long) k, __FILE__, __LINE__);
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  /* free memory */
  dtcmp_free(&scratch);

  return rc;
}
