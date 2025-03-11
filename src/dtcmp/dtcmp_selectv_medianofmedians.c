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

/* rank 0 gathers one median from each process and finds the weighted
 * median of medians, then an allreduce is issued to count number
 * of items 1) less than and 2) equal to this value, O(log n)
 * iterations required to narrow in on specified rank.
 *
 * communication: O(p * log n) communication
 * computation:   O((n/p * log n/p) + (p * log p + log n/p) * log n)
 * memory:        O(p) */

#define LT (0)
#define EQ (1)

/* when we compute the weighted median, we may get some median values
 * that have a zero count.  We should avoid calling the compare function
 * for these medians, as the actual value may be garbage, which may crash
 * the comparator function, so we check the count before the median. */
static int uint64t_with_key_cmp_fn(const void* a, const void* b)
{
  /* check that the counts for both elements are not zero,
   * if a count is zero, consider that element to be higher
   * (throws zeros to back of list) */
  uint64_t count_a = *(uint64_t*) a;
  uint64_t count_b = *(uint64_t*) b;
  if (count_a != 0 && count_b != 0) {
    /* both elements have non-zero counts,
     * so compare the median values to each other */
    return 0;
  } else if (count_b != 0) {
    /* count of first element is zero (but not the second),
     * so say the first element is larger */
    return 1;
  } else if (count_a != 0) {
    /* count of second element is zero (but not the first),
     * so say the second element is larger */
    return -1;
  } else {
    /* counts of both elements are zero, they are equivalent */
    return 0;
  }
}

/* each process computes weighted median for different splitter */
static void compute_weighted_median(
  const void* my_num_with_median,
  void* out_num_with_median,
  void* scratch,
  MPI_Datatype key,
  DTCMP_Op keycmp,
  MPI_Datatype type_uint64t_with_key,
  DTCMP_Op cmp_uint64t_with_key,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  int i;

  /* get rank within communicator */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* get true extent of key */
  MPI_Aint key_true_lb, key_true_extent;
  MPI_Type_get_true_extent(key, &key_true_lb, &key_true_extent);
  size_t size_uint64t_with_key = sizeof(uint64_t) + key_true_extent;

  /* gather medians with counts to rank 0 */
  char* all_num_with_median = (char*)scratch;
  MPI_Gather(
    (void*)my_num_with_median, 1, type_uint64t_with_key,
    all_num_with_median, 1, type_uint64t_with_key, 0, comm
  );

  /* rank 0 determines the median of medians */
  if (rank == 0) {
    /* sort by medians value (ensuring that count is non-zero) */
    DTCMP_Sort_local(
      DTCMP_IN_PLACE, all_num_with_median, ranks,
      type_uint64t_with_key, type_uint64t_with_key, cmp_uint64t_with_key, hints
    );

    /* compute total number of elements */
    uint64_t N = 0;
    for (i = 0; i < ranks; i++) {
      uint64_t cnt = *(uint64_t*) (all_num_with_median + i * size_uint64t_with_key);
      N += cnt;
    }

    /* identify the weighted median */
    i = 0;
    uint64_t before_weight = 0;
    uint64_t half_weight   = N / 2;
    char* ptr = all_num_with_median;
    void* target = (void*) (ptr + sizeof(uint64_t));
    while(i < ranks) {
      /* set our target to the current median
       * and initialize our current weight */
      target = (void*) (ptr + sizeof(uint64_t));
      uint64_t current_weight = *(uint64_t*) ptr;
      i++;
      ptr += size_uint64t_with_key;

      /* add weights for any elements which equal this current median */
      if (i < ranks) {
        int result;
        uint64_t next_weight = *(uint64_t*) ptr;
        if (next_weight > 0) {
          void* next_target = (void*) (ptr + sizeof(uint64_t));
          result = dtcmp_op_eval(target, next_target, keycmp);
        } else {
          result = 0;
        }
        while (i < ranks && result == 0) {
          /* current item is equal to target, add its weight */
          current_weight += next_weight;
          i++;
          ptr += size_uint64t_with_key;

          /* get weight and comparison result of next item if one exists */
          if (i < ranks) {
            next_weight = *(uint64_t*) ptr;
            if (next_weight > 0) {
              void* next_target = (void*) (ptr + sizeof(uint64_t));
              result = dtcmp_op_eval(target, next_target, keycmp);
            } else {
              result = 0;
            }
          }
        }
      }

      /* determine if the weight before and after this value are
       * each less than or equal to half */
      uint64_t after_weight = N - before_weight - current_weight;
      if (before_weight <= half_weight && after_weight <= half_weight) {
        break;
      }

      /* after was too heavy, so add current weight to before
       * and go to next value */
      before_weight += current_weight;
    }

    /* set total number of active elements,
     * and copy the median to our allgather send buffer */
    uint64_t* num = (uint64_t*) out_num_with_median;
    *num = N;
    memcpy((char*)out_num_with_median + sizeof(uint64_t), target, key_true_extent);
  }

  /* broadcast medians */
  MPI_Bcast(out_num_with_median, 1, type_uint64t_with_key, 0, comm);

  return;
}

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
  /* get number of ranks in our communicator */
  int ranks;
  MPI_Comm_size(comm, &ranks);

  /* get extent of keysat type so we can compute offsets into data buffer */
  MPI_Aint keysat_lb, keysat_extent;
  MPI_Type_get_extent(keysat, &keysat_lb, &keysat_extent);

  /* get true extent of key so we can allocate space to copy them into */
  MPI_Aint key_true_lb, key_true_extent;
  MPI_Type_get_true_extent(key, &key_true_lb, &key_true_extent);
  size_t size_uint64t_with_key = sizeof(uint64_t) + key_true_extent;

  /* tracks current lowest index of each of active range */
  int index = 0;

  /* tracks number of items in each active range */
  int num = n;

  /* candidate median for each splitter with count from local rank */
  char* my_num_with_median = dtcmp_malloc(size_uint64t_with_key, 0, __FILE__, __LINE__); 

  /* candidate splitters after each median-of-medians round */
  char* out_num_with_median = dtcmp_malloc(size_uint64t_with_key, 0, __FILE__, __LINE__);

  /* scratch space used to compute median-of-medians */
  void* weighted_median_scratch = dtcmp_malloc(ranks * size_uint64t_with_key, 0, __FILE__, __LINE__);

  /* create new comparison operation to compare count, then key,
   * don't compare key if either count is zero, since key may be garbage */
  DTCMP_Op cmp_count_nonzero, cmp_uint64t_with_key;
  DTCMP_Op_create(MPI_UINT64_T, uint64t_with_key_cmp_fn, &cmp_count_nonzero);
  DTCMP_Op_create_series2(cmp_count_nonzero, cmp, &cmp_uint64t_with_key);

  /* create and commit datatype to represent concatenation of int with key */
  MPI_Datatype type_uint64t_with_key;
  dtcmp_type_concat2(MPI_UINT64_T, key, &type_uint64t_with_key);

  /* iterate until we identify the specified rank, or determine that
   * it does not exist */
  int found_exact = 0;
  while (1) {
    /* record number of active elements for this range */
    char* ptr = my_num_with_median;
    uint64_t num64 = (uint64_t) num;
    memcpy(ptr, &num64, sizeof(uint64_t));
    if (num > 0) {
      /* if there is at least one element, copy in median element */
      int median_index = (num / 2) + index;
      if (median_index < n) {
        const void* median = (char*)data + median_index * keysat_extent;
        DTCMP_Memcpy(ptr + sizeof(uint64_t), 1, key, median, 1, key);
      }
    }

    /* compute the weighted median, M, and total number
     * of active elements, N */
    compute_weighted_median(
      (const void*)my_num_with_median, (void*)out_num_with_median, weighted_median_scratch,
      key, cmp, type_uint64t_with_key, cmp_uint64t_with_key, hints, comm
    );

    uint64_t N = *(uint64_t*)out_num_with_median;
    if (k > N) {
      break;
    }

    /* if we don't have any active elements, set counts to 0 */
    uint64_t counts[2];
    if (num == 0) {
      counts[LT] = 0;
      counts[EQ] = 0;
    } else {
      /* get proposed split point */
      void* target = out_num_with_median + sizeof(uint64_t);

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

  /* free our comparison ops and type */
  DTCMP_Op_free(&cmp_count_nonzero);
  DTCMP_Op_free(&cmp_uint64t_with_key);
  MPI_Type_free(&type_uint64t_with_key);

  /* determine whether we found the specified rank,
   * and copy its key into the output buffer */
  *found_flag = found_exact;
  if (found_exact != 0) {
    DTCMP_Memcpy(found_key, 1, key, out_num_with_median + sizeof(uint64_t), 1, key);
  }

  /* free off our scratch space */
  dtcmp_free(&my_num_with_median);
  dtcmp_free(&out_num_with_median);
  dtcmp_free(&weighted_median_scratch);

  return DTCMP_SUCCESS;
}

int DTCMP_Selectv_medianofmedians(
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
