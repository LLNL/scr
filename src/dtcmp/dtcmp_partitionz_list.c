/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-557516.
 * All rights reserved.
 * This file is part of the DTCMP library.
 * For details, see https://github.com/hpc/dtcmp
 * Please also read this file: LICENSE.TXT. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mpi.h"
#include "dtcmp_internal.h"

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

/* Like DTCMP_Partitionz, but splits items using a list of num item
 * ranks and divideranks. */
int DTCMP_Partitionz_list(
  void* buf,
  int count,
  int num,
  uint64_t k[],
  int divideranks[],
  void** outbuf,
  int* outcount,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm,
  DTCMP_Handle* handle)
{
  int i;

  /* get rank and number of ranks in communicator */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* can't yet handle the case where there are more segments than ranks */
  if (num >= ranks) {
    printf("ERROR: num splitters (%d) must be strictly less than number of ranks (%d) @ %s:%d\n",
      num, ranks, __FILE__, __LINE__
    );
    exit(1);
  }

  /* TODO: check that all divideranks entries are within range o to ranks-1 */

  /* get extent of key datatype */
  MPI_Aint key_lb, key_extent;
  MPI_Type_get_extent(key, &key_lb, &key_extent);

  /* get true extent of key datatype */
  MPI_Aint key_true_lb, key_true_extent;
  MPI_Type_get_true_extent(key, &key_true_lb, &key_true_extent);

  /* get true extent of keysat datatype */
  MPI_Aint keysat_true_lb, keysat_true_extent;
  MPI_Type_get_true_extent(keysat, &keysat_true_lb, &keysat_true_extent);

  /* allocate enough space to hold num keys (splitters) */
  void* targets = dtcmp_malloc(num * key_true_extent, 0, __FILE__, __LINE__);

  /* TODO: there are more efficient ways to do this */
  /* identify targets */
  char* target = (char*) targets;
  for (i = 0; i < num; i++) {
    DTCMP_Selectv(buf, count, k[i], target, key, keysat, cmp, hints, comm);
    target += key_extent;
  }

  /* partition local array based on targets */
  int* indicies = (int*) dtcmp_malloc(num * sizeof(int), 0, __FILE__, __LINE__);
  DTCMP_Partition_local_target_list_dtcpy(
    buf, count, 0, num, targets, indicies,
    key, keysat, cmp, hints
  );

  /* find number of items we have for each range */
  int numsegments = num + 1;
  uint64_t* counts = (uint64_t*) dtcmp_malloc(numsegments * sizeof(uint64_t), 0, __FILE__, __LINE__);
  counts[0] = (uint64_t) indicies[0];
  int count_sum = (int) counts[0];
  for (i = 1; i < num; i++) {
    counts[i] = (uint64_t) (indicies[i] - indicies[i-1]);
    count_sum += (int) counts[i];
  }
  counts[numsegments-1] = count - count_sum;

  /* find global offsets of our items for each range */
  uint64_t* scan_counts = (uint64_t*) dtcmp_malloc(numsegments * sizeof(uint64_t), 0, __FILE__, __LINE__);
  MPI_Exscan(counts, scan_counts, numsegments, MPI_UINT64_T, MPI_SUM, comm);
  if (rank == 0) {
    for (i = 0; i < numsegments; i++) {
      scan_counts[i] = 0;
    }
  }

  /* compute sum of elements in each segment across processes */
  uint64_t* total_counts = (uint64_t*) dtcmp_malloc(numsegments * sizeof(uint64_t), 0, __FILE__, __LINE__);
  MPI_Allreduce(counts, total_counts, numsegments, MPI_UINT64_T, MPI_SUM, comm);

  /* determine how many items each segment will hold per rank */
  uint64_t* items_per_rank = (uint64_t*) dtcmp_malloc(numsegments * sizeof(uint64_t), 0, __FILE__, __LINE__);
  uint64_t* items_extras   = (uint64_t*) dtcmp_malloc(numsegments * sizeof(uint64_t), 0, __FILE__, __LINE__);
  if (divideranks[0] > 0) {
    uint64_t num_ranks = (uint64_t) divideranks[0];
    items_per_rank[0] = total_counts[0] / num_ranks;
    items_extras[0]   = total_counts[0] - items_per_rank[0] * num_ranks;
  } else {
    /* implied that rank 0 will get this segment */
    items_per_rank[0] = total_counts[0];
    items_extras[0]   = 0;
  }
  for (i = 1; i < num; i++) {
    uint64_t num_ranks = (uint64_t) (divideranks[i] - divideranks[i-1]);
    if (num_ranks > 0) {
      items_per_rank[i] = total_counts[i] / num_ranks;
      items_extras[i]   = total_counts[i] - items_per_rank[i] * num_ranks;
    } else {
      /* implied that divideranks[i] will get all of this segment */
      items_per_rank[i] = total_counts[i];
      items_extras[i]   = 0;
    }
  }
  if (divideranks[num-1] < ranks) {
    int idx = numsegments - 1;
    uint64_t num_ranks = (uint64_t) (ranks - divideranks[num-1]);
    if (num_ranks > 0) {
      items_per_rank[idx] = total_counts[idx] / num_ranks;
      items_extras[idx]   = total_counts[idx] - items_per_rank[idx] * num_ranks;
    } else {
      items_per_rank[idx] = total_counts[idx];
      items_extras[idx]   = 0;
    }
  }

  /* determine ranks and indicies that we'll send data to */
  uint64_t* start_rank  = (uint64_t*) dtcmp_malloc(numsegments * sizeof(uint64_t), 0, __FILE__, __LINE__);
  uint64_t* start_index = (uint64_t*) dtcmp_malloc(numsegments * sizeof(uint64_t), 0, __FILE__, __LINE__);
  uint64_t* end_rank    = (uint64_t*) dtcmp_malloc(numsegments * sizeof(uint64_t), 0, __FILE__, __LINE__);
  uint64_t* end_index   = (uint64_t*) dtcmp_malloc(numsegments * sizeof(uint64_t), 0, __FILE__, __LINE__);
  int rank_offset = 0;
  for (i = 0; i < numsegments; i++) {
    compute_rank_index(
      scan_counts[i], items_per_rank[i], items_extras[i],
      &start_rank[i], &start_index[i]
    );
    compute_rank_index(
      scan_counts[i] + counts[i], items_per_rank[i], items_extras[i],
      &end_rank[i], &end_index[i]
    );
    start_rank[i] += (uint64_t) rank_offset;
    end_rank[i]   += (uint64_t) rank_offset;
    if (i < num) {
      rank_offset = divideranks[i];
    }
  }

  /* TODO: replace alltoall/alltoallv with DSDE call */
  
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
  int j;
  int senddisp = 0;
  for (j = 0; j < numsegments; j++) {
    for (i = start_rank[j]; i <= end_rank[j]; i++) {
      /* determine start index on destination rank */
      int start = 0;
      if (i == (int) start_rank[j]) {
        start = (int) start_index[j];
      }

      /* determine end index on destination rank */
      int end = (int) items_per_rank[j];
      if (i < (int) items_extras[j]) {
        end++;
      }
      if (i == (int) end_rank[j]) {
        end = (int) end_index[j];
      }

      /* fill in our send counts */
      int sendcount = end - start;
      if (i < ranks) {
        sendcounts[i] = sendcount;
        senddispls[i] = senddisp;
      }
      senddisp += sendcount;
    }
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
  dtcmp_free(&end_index);
  dtcmp_free(&end_rank);
  dtcmp_free(&start_index);
  dtcmp_free(&start_rank);
  dtcmp_free(&items_extras);
  dtcmp_free(&items_per_rank);
  dtcmp_free(&total_counts);
  dtcmp_free(&scan_counts);
  dtcmp_free(&counts);
  dtcmp_free(&indicies);
  dtcmp_free(&targets);

  return 0;
}
