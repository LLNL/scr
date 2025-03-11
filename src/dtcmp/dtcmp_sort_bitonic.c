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

/* given same number of items per process, execute Batcher's bitonic sort */

/* this version assumes each process has exactly one item */
static int DTCMP_Sort_bitonic_merge_single(
  void* value,
  void* recv,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  int rank,
  int start,
  int num,
  int direction,
  MPI_Comm comm)
{
  /* nothing to do if there is only one rank in our remaining group */
  if (num > 1) {
    /* determine largest power of two that is smaller than size of group */
    int dist = 1;
    while (dist < num) {
      dist <<= 1;
    }
    dist >>= 1;

    /* divide range into two chunks, execute bitonic half-clean step, then recursively merge each half */
    MPI_Status status[2];
    if (rank < start + dist) {
      int dst_rank = rank + dist;
      if (dst_rank < start + num) {
        /* exchange data with our partner rank */
        MPI_Sendrecv(
          value, 1, keysat, dst_rank, 0,
          recv,  1, keysat, dst_rank, 0,
          comm, status
        );

        /* select the appropriate value, depending on the sort direction */
        int res = dtcmp_op_eval(recv, value, cmp);
        if ((direction && res < 0) || (!direction && res > 0)) {
          DTCMP_Memcpy(value, 1, keysat, recv, 1, keysat);
        }
      }

      /* recursively merge our half */
      DTCMP_Sort_bitonic_merge_single(value, recv, keysat, cmp, rank, start, dist, direction, comm);
    } else {
      int dst_rank = rank - dist;
      if (dst_rank >= start) {
        /* exchange data with our partner rank */
        MPI_Sendrecv(
          value, 1, keysat, dst_rank, 0,
          recv,  1, keysat, dst_rank, 0,
          comm, status
        );

        /* select the appropriate value, depending on the sort direction */
        int res = dtcmp_op_eval(recv, value, cmp);
        if ((direction && res > 0) || (!direction && res < 0)) {
          DTCMP_Memcpy(value, 1, keysat, recv, 1, keysat);
        }
      }

      /* recursively merge our half */
      int new_start = start + dist;
      int new_num   = num - dist;
      DTCMP_Sort_bitonic_merge_single(value, recv, keysat, cmp, rank, new_start, new_num, direction, comm);
    }
  }

  return 0;
}

/* this version assumes each process has exactly one item */
static int DTCMP_Sort_bitonic_sort_single(
  void* value,
  void* recv,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  int rank,
  int start,
  int num,
  int direction,
  MPI_Comm comm)
{
  if (num > 1) {
    /* recursively divide and sort each half */
    int mid = num / 2;
    if (rank < start + mid) {
      DTCMP_Sort_bitonic_sort_single(value, recv, keysat, cmp, rank, start, mid, !direction, comm);
    } else {
      int new_start = start + mid;
      int new_num   = num - mid;
      DTCMP_Sort_bitonic_sort_single(value, recv, keysat, cmp, rank, new_start, new_num, direction, comm);
    }

    /* merge the two sorted halves */
    DTCMP_Sort_bitonic_merge_single(value, recv, keysat, cmp, rank, start, num, direction, comm);
  }

  return 0;
}

/* this version assumes all procs have the same number of items, which can be 1 or more */
static int DTCMP_Sort_bitonic_merge_multiple(
  void* value,
  void* recv,
  void* merge,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  MPI_Aint extent,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  int rank,
  int start,
  int num,
  int direction,
  MPI_Comm comm)
{
  const void* bufs[2];
  int counts[2];

  /* nothing to do if there is only one rank in our remaining group */
  if (num > 1) {
    /* determine largest power of two that is smaller than size of group */
    int dist = 1;
    while (dist < num) {
      dist <<= 1;
    }
    dist >>= 1;

    /* divide range into two chunks, execute bitonic half-clean step, then recursively merge each half */
    MPI_Status status[2];
    if (rank < start + dist) {
      int dst_rank = rank + dist;
      if (dst_rank < start + num) {
        /* exchange data with our partner rank */
        MPI_Sendrecv(
          value, count, keysat, dst_rank, 0,
          recv,  count, keysat, dst_rank, 0,
          comm, status
        );

        /* select the appropriate value, depending on the sort direction,
         * note we're careful in how we merge so that it's stable */
        bufs[0] = value;
        bufs[1] = recv;
        counts[0] = count;
        counts[1] = count;
        DTCMP_Merge_local(2, bufs, counts, merge, key, keysat, cmp, hints);
        if (direction) {
          /* if we're in the lower half of the procs and direction is increasing, take the lower half of the values */
          DTCMP_Memcpy(value, count, keysat, merge, count, keysat);
        } else {
          /* otherwise, take the upper half of the values */
          char* target = (char*)merge + count * extent;
          DTCMP_Memcpy(value, count, keysat, target, count, keysat);
        }
      }

      /* recursively merge our half */
      DTCMP_Sort_bitonic_merge_multiple(value, recv, merge, count, key, keysat, extent, cmp, hints, rank, start, dist, direction, comm);
    } else {
      int dst_rank = rank - dist;
      if (dst_rank >= start) {
        /* exchange data with our partner rank */
        MPI_Sendrecv(
          value, count, keysat, dst_rank, 0,
          recv,  count, keysat, dst_rank, 0,
          comm, status
        );

        /* select the appropriate value, depending on the sort direction,
         * note we're careful in how we merge so that it's stable */
        bufs[0] = recv;
        bufs[1] = value;
        counts[0] = count;
        counts[1] = count;
        DTCMP_Merge_local(2, bufs, counts, merge, key, keysat, cmp, hints);
        if (direction) {
          /* if we're in the upper half of the procs and direction is increasing, take the upper half of the values */
          char* target = (char*)merge + count * extent;
          DTCMP_Memcpy(value, count, keysat, target, count, keysat);
        } else {
          /* otherwise, take the lower half of the values */
          DTCMP_Memcpy(value, count, keysat, merge, count, keysat);
        }
      }
      /* recursively merge our half */
      int new_start = start + dist;
      int new_num   = num - dist;
      DTCMP_Sort_bitonic_merge_multiple(value, recv, merge, count, key, keysat, extent, cmp, hints, rank, new_start, new_num, direction, comm);
    }
  }

  return 0;
}

/* this version assumes all procs have the same number of items, which can be 1 or more */
static int DTCMP_Sort_bitonic_sort_multiple(
  void* value,
  void* recv,
  void* merge,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  MPI_Aint extent,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  int rank,
  int start,
  int num,
  int direction,
  MPI_Comm comm)
{
  if (num > 1) {
    /* recursively divide and sort each half */
    int mid = num / 2;
    if (rank < start + mid) {
      DTCMP_Sort_bitonic_sort_multiple(value, recv, merge, count, key, keysat, extent, cmp, hints, rank, start, mid, !direction, comm);
    } else {
      int new_start = start + mid;
      int new_num   = num - mid;
      DTCMP_Sort_bitonic_sort_multiple(value, recv, merge, count, key, keysat, extent, cmp, hints, rank, new_start, new_num, direction, comm);
    }

    /* merge the two sorted halves */
    DTCMP_Sort_bitonic_merge_multiple(value, recv, merge, count, key, keysat, extent, cmp, hints, rank, start, num, direction, comm);
  }

  return 0;
}

int DTCMP_Sort_bitonic(
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

  /* get true extent of keysat */
  MPI_Aint true_lb, true_extent;
  MPI_Type_get_true_extent(keysat, &true_lb, &true_extent);

  /* allocate scratch space to hold data */
  size_t buf_size = count * true_extent;
  if (buf_size > 0) {
    void* value = dtcmp_malloc(buf_size, 0, __FILE__, __LINE__);
    void* extra = dtcmp_malloc(buf_size, 0, __FILE__, __LINE__);
    void* merge = dtcmp_malloc(2 * buf_size, 0, __FILE__, __LINE__);

    /* TODO: handle lower bound */
    /* copy our input items into the value buffer */
    void* buf = (void*) inbuf;
    if (inbuf == DTCMP_IN_PLACE) {
      buf = outbuf;
    }
    DTCMP_Memcpy(value, count, keysat, buf, count, keysat);

    /* get our rank and the number of ranks in our communicator */
    int rank, ranks;
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &ranks);

    /* conduct the bitonic sort */
    if (count == 1) {
      /* sort just a single element */
      DTCMP_Sort_bitonic_sort_single(value, extra, keysat, cmp, rank, 0, ranks, 1, comm);
    } else {
      /* sort local elements first */
      DTCMP_Sort_local(DTCMP_IN_PLACE, value, count, key, keysat, cmp, hints);

      /* now sort across processes */
      DTCMP_Sort_bitonic_sort_multiple(
        value, extra, merge, count, key, keysat, true_extent, cmp, hints,
        rank, 0, ranks, 1, comm
      );
    }

    /* TODO: handle lower bound */
    /* copy our sorted items into our output buffer */
    DTCMP_Memcpy(outbuf, count, keysat, value, count, keysat);

    /* free the scratch space */
    dtcmp_free(&merge);
    dtcmp_free(&extra);
    dtcmp_free(&value);
  }

  return rc;
}
