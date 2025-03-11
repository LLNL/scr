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

/* malloc with some checks on size and the returned pointer, along with future support for alignment,
 * if size <= 0, malloc is not called and NULL pointer is returned
 * error is printed with file name and line number if size > 0 and malloc returns NULL pointer */
void* dtcmp_malloc(size_t size, size_t align, const char* file, int line)
{
  void* ptr = NULL;
  if (size > 0) {
/*
    if (align == 0) {
      ptr = malloc(size);
    } else {
      posix_memalign(&ptr, size, align);
    }
*/
    ptr = malloc(size);
    if (ptr == NULL) {
      printf("ERROR: Failed to allocate memory %lu bytes @ %s:%d\n", size, file, line);
      exit(1);
    }
  }
  return ptr;
}

/* careful to not call free if pointer is already NULL,
 * sets pointer value to NULL */
void dtcmp_free(void* p)
{
  /* we really receive a pointer to a pointer (void**),
   * but it's typed as void* so caller doesn't need to add casts all over the place */
  void** ptr = (void**) p;

  /* free associated memory and set pointer to NULL */
  if (ptr == NULL) {
    printf("ERROR: Expected address of pointer value, but got NULL instead @ %s:%d\n",
      __FILE__, __LINE__
    );
  } else if (*ptr != NULL) {
    free(*ptr);
    *ptr = NULL;
  }
}

int dtcmp_handle_alloc_single(size_t size, void** buf, DTCMP_Handle* handle)
{
  /* setup handle and buffer to merge data to return to user */
  void* ret_buf = NULL;
  size_t ret_buf_size = sizeof(dtcmp_handle_free_fn) + size;
  if (ret_buf_size > 0) {
    ret_buf = (void*) dtcmp_malloc(ret_buf_size, 0, __FILE__, __LINE__);
    if (ret_buf == NULL) {
      /* TODO: error */
    }
  }

  /* compute and allocate space to merge received data */
  char* ret_buf_tmp = (char*)ret_buf;
  if (ret_buf != NULL) {
    /* allocate and initialize function pointer as first item in handle struct */
    dtcmp_handle_free_fn* fn = (dtcmp_handle_free_fn*) ret_buf;
    *fn = dtcmp_handle_free_single;
    ret_buf_tmp += sizeof(dtcmp_handle_free_fn);

    /* record the start of the buffer */
    if (size > 0) {
      *buf = (void*) ret_buf_tmp;
    } else {
      *buf = NULL;
    }
  }

  /* set the handle value */
  *handle = ret_buf;

  return DTCMP_SUCCESS;
}

/* assumes that handle just points to one big block of memory that must be freed */
int dtcmp_handle_free_single(DTCMP_Handle* handle)
{
  if (handle != NULL && *handle != DTCMP_HANDLE_NULL) {
    free(*handle);
    *handle = DTCMP_HANDLE_NULL;
  }
  return DTCMP_SUCCESS;
}

/* execute reduction to compute min/max/sum over comm */
int dtcmp_get_uint64t_min_max_sum(int count, uint64_t* min, uint64_t* max, uint64_t* sum, MPI_Comm comm)
{
  /* initialize our input with our count value */
  uint64_t input[3];
  input[MMS_MIN] = (uint64_t) count;
  input[MMS_MAX] = (uint64_t) count;
  input[MMS_SUM] = (uint64_t) count;

  /* execute the allreduce */
  uint64_t output[3];
  MPI_Allreduce(input, output, 1, dtcmp_type_3uint64t, dtcmp_reduceop_mms_3uint64t, comm);

  /* copy result to output parameters */
  *min = output[MMS_MIN];
  *max = output[MMS_MAX];
  *sum = output[MMS_SUM];

  return DTCMP_SUCCESS;
}

int dtcmp_get_randroot(int count, int* flag, int* root, MPI_Comm comm)
{
  /* get a random value */
  int rand = rand_r(&dtcmp_rand_seed);

  /* get our rank */
  int rank;
  MPI_Comm_rank(comm, &rank);

  /* initialize our input with our count, random value, and rank */
  int input[3];
  input[RANDROOT_COUNT] = count;
  input[RANDROOT_RAND]  = rand;
  input[RANDROOT_RANK]  = rank;

  /* execute the allreduce */
  int output[3];
  MPI_Allreduce(input, output, 1, dtcmp_type_3int, dtcmp_reduceop_randroot, comm);

  /* copy result to output parameter */
  if (output[RANDROOT_COUNT] > 0) {
    *flag = 1;
    *root = output[RANDROOT_RANK];
  } else {
    *flag = 0;
    *root = MPI_PROC_NULL;
  }

  return DTCMP_SUCCESS;
}

int dtcmp_randbcast(
  const void* inbuf,
  int weight,
  void* outbuf,
  int* flag,
  int count,
  MPI_Datatype type,
  MPI_Comm comm)
{
  /* copy input data to output buffer */
  if (weight > 0 && inbuf != MPI_IN_PLACE) {
    DTCMP_Memcpy(outbuf, count, type, inbuf, count, type);
  }

  /* determine root */
  int root;
  dtcmp_get_randroot(weight, flag, &root, comm);

  /* bcast value from root */
  if (*flag) {
    MPI_Bcast(outbuf, count, type, root, comm);
  }

  return DTCMP_SUCCESS;
}

int dtcmp_get_lt_eq_gt(
  const void* target,
  const void* buf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  uint64_t* lt,
  uint64_t* eq,
  uint64_t* gt,
  MPI_Comm comm)
{
  /* get extent of keysat type */
  MPI_Aint keysat_lb, keysat_extent;
  MPI_Type_get_extent(keysat, &keysat_lb, &keysat_extent);

  /* get comparison operation */
  dtcmp_op_handle_t* c = (dtcmp_op_handle_t*) cmp;
  DTCMP_Op_fn compare = c->fn;
  
  /* get local count of less than / equal / greater than */
  uint64_t counts[3] = {0,0,0};
  if (hints & (DTCMP_FLAG_SORTED_LOCAL | DTCMP_FLAG_SORTED)) {
    /* if hint shows that local data is sorted, use binary search */
    if (count > 0) {
      int start_index = 0;
      int end_index   = count - 1;
      int flag, lowest, highest;
      DTCMP_Search_low_local(target,  buf, start_index, end_index, key, keysat, cmp, hints, &flag, &lowest);
      DTCMP_Search_high_local(target, buf, lowest,      end_index, key, keysat, cmp, hints, &flag, &highest);
      counts[0] = (uint64_t)(lowest - start_index);
      counts[1] = (uint64_t)((highest + 1) - lowest);
      counts[2] = (uint64_t)count - counts[0] - counts[1];
    }
  } else {
    /* otherwise, just march through the whole list and count */
    int i;
    const char* ptr = (const char*) buf;
    for (i = 0; i < count; i++) {
      int result = (*compare)(ptr, target);
      if (result < 0) {
        counts[0]++;
      } else if (result == 0) {
        counts[1]++;
      } else {
        counts[2]++;
      }
      ptr += keysat_extent;
    }
  }

  /* get global sum of less than / equal / greater than */
  uint64_t all_counts[3];
  MPI_Allreduce(counts, all_counts, 3, MPI_UINT64_T, MPI_SUM, comm);
  
  /* set output parameters */
  *lt = all_counts[0];
  *eq = all_counts[1];
  *gt = all_counts[2];

  return DTCMP_SUCCESS;
}

/* builds and commits a new datatype that is the concatenation of the
 * list of old types, each oldtype should have no holes */
int dtcmp_type_concat(int num, const MPI_Datatype oldtypes[], MPI_Datatype* newtype)
{
  /* if there is nothing in the list, just return a NULL handle */
  if (num <= 0) {
    *newtype = MPI_DATATYPE_NULL;
    return DTCMP_SUCCESS;
  }

  /* TODO: could use a small array to handle most calls,
   * rather than allocate an array each time */

  /* allocate memory for blocklens, displs, and types arrays for
   * MPI_Type_create_struct */
  int* blocklens      = (int*) dtcmp_malloc(num * sizeof(int), 0, __FILE__, __LINE__);
  MPI_Aint* displs    = (MPI_Aint*) dtcmp_malloc(num * sizeof(MPI_Aint), 0, __FILE__, __LINE__);
  MPI_Datatype* types = (MPI_Datatype*) dtcmp_malloc(num * sizeof(MPI_Datatype), 0, __FILE__, __LINE__);

  /* build new key type */
  int i;
  MPI_Aint disp = 0;
  for (i = 0; i < num; i++) {
    blocklens[i] = 1;
    displs[i] = disp;
    types[i] = oldtypes[i];

    /* get true extent of current type */
    MPI_Aint true_lb, true_extent;
    MPI_Type_get_true_extent(oldtypes[i], &true_lb, &true_extent);
    disp += true_extent;
  }

  /* TODO: need to replace this eventually so we can place types
   * at their proper alignment boundaries for better performance */
  /* create and commit the new type */
  MPI_Datatype aligned_type;
  MPI_Type_create_struct(num, blocklens, displs, types, &aligned_type);
  MPI_Type_create_resized(aligned_type, 0, disp, newtype);
  MPI_Type_commit(newtype);
  MPI_Type_free(&aligned_type);

  /* free memory */
  dtcmp_free(&types);
  dtcmp_free(&displs);
  dtcmp_free(&blocklens);

  return DTCMP_SUCCESS;
}

int dtcmp_type_concat2(MPI_Datatype type1, MPI_Datatype type2, MPI_Datatype* newtype)
{
  MPI_Datatype types[2];
  types[0] = type1;
  types[1] = type2;
  return dtcmp_type_concat(2, types, newtype);
}

/* when we compute the weighted median, we may get some median values
 * that have a zero count.  We should avoid calling the compare function
 * for these medians, as the actual value may be garbage, which may crash
 * the comparator function, so we check the count before the median. */
int dtcmp_count_with_key_cmp_fn(const void* a, const void* b)
{
  /* check that the counts for both elements are not zero,
   * if a count is zero, consider that element to be higher
   * (throws zeros to back of list) */
  int count_a = *(int*) a;
  int count_b = *(int*) b;
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

int dtcmp_weighted_median(
  const void* inbuf,
  void* outbuf,
  int count,
  MPI_Datatype int_with_key,
  DTCMP_Op cmp)
{
  int i;

  /* get extent of each item */
  MPI_Aint lb, extent;
  MPI_Type_get_extent(int_with_key, &lb, &extent);

  /* compute total number of elements */
  int N = 0;
  for (i = 0; i < count; i++) {
    int cnt = *(int*) ((char*)inbuf + i * extent);
    N += cnt;
  }

  /* identify the weighted median */
  i = 0;
  int before_weight = 0;
  int half_weight   = N / 2;
  const char* ptr = (const char*) inbuf;
  void* target = (void*) (ptr + sizeof(int));
  while(i < count) {
    /* set our target to the current median
     * and initialize our current weight */
    target = (void*) (ptr + sizeof(int));
    int current_weight = *(int*) ptr;
    i++;
    ptr += extent;

    /* add weights for any elements which equal this current median */
    if (i < count) {
      int result;
      int next_weight = *(int*) ptr;
      if (next_weight > 0) {
        void* next_target = (void*) (ptr + sizeof(int));
        result = dtcmp_op_eval(target, next_target, cmp);
      } else {
        result = 0;
      }
      while (i < count && result == 0) {
        /* current item is equal to target, add its weight */
        current_weight += next_weight;
        i++;
        ptr += extent;

        /* get weight and comparison result of next item if one exists */
        if (i < count) {
          next_weight = *(int*) ptr;
          if (next_weight > 0) {
            void* next_target = (void*) (ptr + sizeof(int));
            result = dtcmp_op_eval(target, next_target, cmp);
          } else {
            result = 0;
          }
        }
      }
    }

    /* determine if the weight before and after this value are
     * each less than or equal to half */
    int after_weight = N - before_weight - current_weight;
    if (before_weight <= half_weight && after_weight <= half_weight) {
      break;
    }

    /* after was too heavy, so add current weight to before
     * and go to next value */
    before_weight += current_weight;
  }

  /* set total number of active elements,
   * and copy the median to our allgather send buffer */
  int* num = (int*) outbuf;
  *num = N;
  memcpy((char*)outbuf + sizeof(int), target, extent - sizeof(int));

  return DTCMP_SUCCESS;
}

/* distribute a sorted set of elements acquired from a sortz back to
 * ranks requiring a sort */
int dtcmp_sortz_to_sort(
  const void* inbuf,
  int incount,
  void* outbuf,
  int outcount,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  int i;

  /* nothing to do if our outcount is zero */
  if (outcount == 0) {
    return DTCMP_SUCCESS;
  }

  /* get our rank and the number of ranks in comm */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* determine the starting offset of our elements */
  uint64_t scancount;
  uint64_t count = (uint64_t) incount;
  MPI_Exscan(&count, &scancount, 1, MPI_UINT64_T, MPI_SUM, comm);
  if (rank == 0) {
    scancount = 0;
  }

  /* indentify ranks and indicies to send our values */
  uint64_t items_per_rank = (uint64_t) outcount;
  uint64_t start_rank  = scancount / items_per_rank;
  uint64_t start_index = scancount - start_rank * items_per_rank;
  uint64_t end_rank    = (scancount + count) / items_per_rank;
  uint64_t end_index   = (scancount + count) - end_rank * items_per_rank;

  /* allocate space for our alltoallv */
  int* sendcounts = (int*) dtcmp_malloc(ranks * sizeof(int), 0, __FILE__, __LINE__);
  int* senddispls = (int*) dtcmp_malloc(ranks * sizeof(int), 0, __FILE__, __LINE__);
  int* recvcounts = (int*) dtcmp_malloc(ranks * sizeof(int), 0, __FILE__, __LINE__);
  int* recvdispls = (int*) dtcmp_malloc(ranks * sizeof(int), 0, __FILE__, __LINE__);

  /* initialize all send counts to 0 */
  for (i = 0; i < ranks; i++) {
    sendcounts[i] = 0;
    senddispls[i] = 0;
  }

  /* fill in non-zero send counts and displacements */
  int senddisp = 0;
  for (i = start_rank; i <= end_rank; i++) {
    /* determine start index on destination rank */
    int start = 0;
    if (i == (int) start_rank) {
      start = (int) start_index;
    }

    /* determine end index on destination rank */
    int end = (int) outcount;
    if (i == (int) end_rank) {
      end = (int) end_index;
    }

    /* fill in our send counts */
    int sendcount = end - start;
    if (i < ranks) {
      sendcounts[i] = sendcount;
      senddispls[i] = senddisp;
    }
    senddisp += sendcount;
  }

  /* alltoall to get recv counts */
  MPI_Alltoall(sendcounts, 1, MPI_INT, recvcounts, 1, MPI_INT, comm);

  /* compute recv displacements from counts */
  int recvdisp = 0;
  for (i = 0; i < ranks; i++) {
    recvdispls[i] = recvdisp;
    recvdisp += recvcounts[i];
  }

  /* alltoallv to exchange data */
  MPI_Alltoallv(
    (void*)inbuf,  sendcounts, senddispls, keysat,
    outbuf, recvcounts, recvdispls, keysat,
    comm
  );

  /* free memory */
  dtcmp_free(&recvdispls);
  dtcmp_free(&recvcounts);
  dtcmp_free(&senddispls);
  dtcmp_free(&sendcounts);

  return DTCMP_SUCCESS;
}
