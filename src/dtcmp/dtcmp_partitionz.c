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

/* TODO: test cases:
 *   - fewer items in one segment than ranks
 *   - multiple segments go to the same ranks */

static int compute_rank_index(
  uint64_t offset, uint64_t num_per_rank, uint64_t num_low_ranks,
  uint64_t* outrank, uint64_t* outindex)
{
  /* each low rank has num_per_rank+1 items,
   * and all higher ranks have num_per_rank items */

  /* number of items on each low rank */
  uint64_t low_per_rank = num_per_rank + 1;

  /* total number of items contained on low ranks */
  uint64_t low_total = num_low_ranks * low_per_rank;

  uint64_t rank, index;
  if (offset < low_total) {
    /* our offset falls in the low part of the elements */
    rank  = offset / low_per_rank;
    index = offset - rank * low_per_rank;
  } else {
    /* our offset falls in the high part of the elements */
    offset -= low_total;
    if (num_per_rank > 0) {
      rank = offset / num_per_rank + num_low_ranks;
    } else {
      rank = num_low_ranks;
    }
    index = offset - (rank - num_low_ranks) * num_per_rank;
  }

  /* set output params */
  *outrank  = rank;
  *outindex = index;

  return DTCMP_SUCCESS;
}

/* Given a list of items on each process, globally partition items at
 * specified item rank, and send smaller items to lower half of ranks
 * and larger items to upper half of ranks.  Item rank should be in range
 * of [1,sum(count)].  Lower items go to [0,dividerank-1] and higher
 * items go to [dividerank,ranks-1].  Evenly divides items among lower
 * and upper ranges of ranks as best as possible, and returns partitioned
 * items in newly allocate memory (outbuf, outcount, handle) */
int DTCMP_Partitionz(
  void* buf, /* TODO: change this to const */
  int count,
  uint64_t k,
  int dividerank,
  void** outbuf,
  int* outcount,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm,
  DTCMP_Handle* handle)
{
  /* get rank and number of ranks in communicator */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* get true extent of key datatype */
  MPI_Aint key_true_lb, key_true_extent;
  MPI_Type_get_true_extent(key, &key_true_lb, &key_true_extent);

  /* get true extent of keysat datatype */
  MPI_Aint keysat_true_lb, keysat_true_extent;
  MPI_Type_get_true_extent(keysat, &keysat_true_lb, &keysat_true_extent);

  /* allocate enough space to hold one key type */
  void* target = dtcmp_malloc(key_true_extent, 0, __FILE__, __LINE__);

  /* identify kth item */
  DTCMP_Selectv(buf, count, k, target, key, keysat, cmp, hints, comm);

  /* partition local array around item */
  int divide;
  DTCMP_Partition_local_target_dtcpy(buf, count, target, &divide, key, keysat, cmp, hints);

  /* compute number of items we have for each half,
   * note that we send the pivot to the upper half */
  uint64_t counts[2];
  counts[0] = divide;            /* low count */
  counts[1] = count - counts[0]; /* high count */

  /* find global offset of our low and high items */
  uint64_t scan_counts[2];
  MPI_Exscan(counts, scan_counts, 2, MPI_UINT64_T, MPI_SUM, comm);
  if (rank == 0) {
    scan_counts[0] = 0;
    scan_counts[1] = 0;
  }

  /* compute sum of elements across processes */
  uint64_t total_counts[2];
  MPI_Allreduce(counts, total_counts, 2, MPI_UINT64_T, MPI_SUM, comm);

  /* determine how many items each low rank will hold,
   * total in lower half / number of lower ranks, and give one
   * extra to each initial rank if non-zero remainder */
  uint64_t low_per_rank = 0;
  uint64_t num_low_extras = 0;
  if (dividerank > 0) {
    low_per_rank = total_counts[0] / (uint64_t) dividerank;
    num_low_extras = total_counts[0] - low_per_rank * (uint64_t) dividerank;
  }

  /* determine how many items each high rank will hold */
  uint64_t high_per_rank = 0;
  uint64_t num_high_extras = 0;
  if (dividerank < ranks) {
    high_per_rank = total_counts[1] / (uint64_t) (ranks - dividerank);
    num_high_extras = total_counts[1] - high_per_rank * (uint64_t) (ranks - dividerank);
  }

  /* determine ranks and indicies that we'll send data to */
  uint64_t low_start_rank, low_start_index, low_end_rank, low_end_index;
  compute_rank_index(
    scan_counts[0], low_per_rank, num_low_extras,
    &low_start_rank, &low_start_index
  );
  compute_rank_index(
    scan_counts[0] + counts[0], low_per_rank, num_low_extras,
    &low_end_rank, &low_end_index
  );

  uint64_t high_start_rank, high_start_index, high_end_rank, high_end_index;
  compute_rank_index(
    scan_counts[1], high_per_rank, num_high_extras,
    &high_start_rank, &high_start_index
  );
  compute_rank_index(
    scan_counts[1] + counts[1], high_per_rank, num_high_extras,
    &high_end_rank, &high_end_index
  );
  high_start_rank += (uint64_t) dividerank;
  high_end_rank   += (uint64_t) dividerank;

  /* TODO: replace alltoall/alltoallv with DSDE call */
  
  /* allocate space for our alltoallv */
  int* sendcounts = (int*) dtcmp_malloc(ranks * sizeof(int), 0, __FILE__, __LINE__);
  int* senddispls = (int*) dtcmp_malloc(ranks * sizeof(int), 0, __FILE__, __LINE__);
  int* recvcounts = (int*) dtcmp_malloc(ranks * sizeof(int), 0, __FILE__, __LINE__);
  int* recvdispls = (int*) dtcmp_malloc(ranks * sizeof(int), 0, __FILE__, __LINE__);

  /* initialize all send counts to 0 */
  int i;
  for (i = 0; i < ranks; i++) {
    sendcounts[i] = 0;
    senddispls[i] = 0;
  }

  /* fill in non-zero send counts and displacements */
  int senddisp = 0;
  for (i = low_start_rank; i <= low_end_rank; i++) {
    /* determine start index on destination rank */
    int start_index = 0;
    if (i == low_start_rank) {
      start_index = (int) low_start_index;
    }

    /* determine end index on destination rank */
    int end_index = (int) low_per_rank;
    if (i < num_low_extras) {
      end_index++;
    }
    if (i == low_end_rank) {
      end_index = low_end_index;
    }

    /* fill in our send counts */
    int sendcount = end_index - start_index;
    if (i < ranks) {
      sendcounts[i] = sendcount;
      senddispls[i] = senddisp;
    }
    senddisp += sendcount;
  }
  for (i = high_start_rank; i <= high_end_rank; i++) {
    /* determine start index on destination rank */
    int start_index = 0;
    if (i == high_start_rank) {
      start_index = (int) high_start_index;
    }

    /* determine end index on destination rank */
    int end_index = (int) high_per_rank;
    if (i < dividerank + num_high_extras) {
      end_index++;
    }
    if (i == high_end_rank) {
      end_index = high_end_index;
    }

    /* fill in our send counts */
    int sendcount = end_index - start_index;
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
  int recvtotal = recvdisp;

  /* allocate memory to receive incoming data */
  void* recvbuf;
  size_t recvbuf_bytes = recvtotal * keysat_true_extent;
  dtcmp_handle_alloc_single(recvbuf_bytes, &recvbuf, handle);

  /* alltoallv to exchange data */
  MPI_Alltoallv(
    buf,     sendcounts, senddispls, keysat,
    recvbuf, recvcounts, recvdispls, keysat,
    comm
  );

  /* set output parameters */
  *outbuf   = recvbuf;
  *outcount = recvtotal;

  /* free memory */
  dtcmp_free(&recvdispls);
  dtcmp_free(&recvcounts);
  dtcmp_free(&senddispls);
  dtcmp_free(&sendcounts);
  dtcmp_free(&target);

  return 0;
}
