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

static int find_splitters(
  void* buf,
  int count,
  int s,
  void* splitters,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  int i;

  /* get my rank and number of ranks in comm */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* get lower bound and extent of key */
  MPI_Aint key_lb, key_extent;
  MPI_Aint key_true_lb, key_true_extent;
  MPI_Type_get_extent(key, &key_lb, &key_extent);
  MPI_Type_get_true_extent(key, &key_true_lb, &key_true_extent);

  /* get lower bound and extent of keysat */
  MPI_Aint keysat_lb, keysat_extent;
  MPI_Aint keysat_true_lb, keysat_true_extent;
  MPI_Type_get_extent(keysat, &keysat_lb, &keysat_extent);
  MPI_Type_get_true_extent(keysat, &keysat_true_lb, &keysat_true_extent);

  /* pick "s" regularly spaced samples */
  size_t samples_size = s * key_true_extent;
  if (samples_size > 0) {
    char* samples = (char*) dtcmp_malloc(samples_size, 0, __FILE__, __LINE__);
    for (i = 0; i < s; i++) {
      int sample_index = (((i+1) * count) / s) - 1;
      char* src = (char*)buf + sample_index * keysat_extent;
      char* dst = samples + i * key_extent;
      DTCMP_Memcpy(dst, 1, key, src, 1, key);
    }

    /* gather samples to root */
    int num_all_samples = ranks * s;
    size_t all_samples_size = num_all_samples * key_true_extent;
    if (all_samples_size > 0) {
      char* all_samples = NULL;
      if (rank == 0) {
        all_samples = (char*) dtcmp_malloc(all_samples_size, 0, __FILE__, __LINE__);
      }
      MPI_Gather(samples, s, key, all_samples, s, key, 0, comm);

      /* TODO: we could replace this with a merge since they are already
       * ordered from each process, or replace this with a sorting
       * gather */
      /* sort samples at root */
      if (rank == 0) {
        DTCMP_Sort_local(DTCMP_IN_PLACE, all_samples, num_all_samples, key, key, cmp, hints);
      }

      /* pick ranks-1 splitters */
      int num_splitters = ranks - 1;
      if (rank == 0) {
        /* first splitter is s units in */
        for (i = 0; i < num_splitters; i++) {
          int splitter_index = (i+1) * s;
          char* src = all_samples + splitter_index * key_extent;
          char* dst = (char*)splitters + i * key_extent;
          DTCMP_Memcpy(dst, 1, key, src, 1, key);
        }
      }

      /* broadcast splitters to all procs */
      MPI_Bcast(splitters, num_splitters, key, 0, comm);

      /* free memory */
      dtcmp_free(&all_samples);
    }

    /* free memory */
    dtcmp_free(&samples);
  }

  return DTCMP_SUCCESS;
}

static int split_exchange_merge(
  void* buf,
  int count,
  void** outbuf,
  int* outcount,
  int num_splitters,
  void* splitters,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm,
  DTCMP_Handle* handle)
{
  int i;

  /* initialize output parameters */
  *outbuf   = NULL;
  *outcount = 0;
  *handle   = DTCMP_HANDLE_NULL;

  /* get my rank and number of ranks in comm */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* get lower bound and extent of keysat */
  MPI_Aint keysat_lb, keysat_extent;
  MPI_Aint keysat_true_lb, keysat_true_extent;
  MPI_Type_get_extent(keysat, &keysat_lb, &keysat_extent);
  MPI_Type_get_true_extent(keysat, &keysat_true_lb, &keysat_true_extent);

  /* search for split locations */
  size_t flags_size    = num_splitters * sizeof(int);
  size_t indicies_size = num_splitters * sizeof(int);
  int* flags    = (int*) dtcmp_malloc(flags_size, 0, __FILE__, __LINE__);
  int* indicies = (int*) dtcmp_malloc(indicies_size, 0, __FILE__, __LINE__);
  DTCMP_Search_low_list_local(num_splitters, splitters, buf, 0, count-1, key, keysat, cmp, hints, flags, indicies);

  /* allocate space to record outgoing counts and to receive incoming counts */
  size_t outgoing_counts_size = ranks * sizeof(int);
  size_t incoming_counts_size = ranks * sizeof(int);
  int* outgoing_counts = (int*) dtcmp_malloc(outgoing_counts_size, 0, __FILE__, __LINE__);
  int* incoming_counts = (int*) dtcmp_malloc(incoming_counts_size, 0, __FILE__, __LINE__);

  /* determine number of elements that we'll send to each process */
  int last_offset = 0;
  for (i = 0; i < num_splitters; i++) {
    outgoing_counts[i] = indicies[i] - last_offset;
    last_offset = indicies[i];
  }
  outgoing_counts[num_splitters] = count - last_offset;

  /* alltoall to collect counts from each processes */
  MPI_Alltoall(outgoing_counts, 1, MPI_INT, incoming_counts, 1, MPI_INT, comm);

  /* compute outgoing and incoming displacements */
  int outgoing_disps_size = ranks * sizeof(int);
  int incoming_disps_size = ranks * sizeof(int);
  int* outgoing_disps = (int*) dtcmp_malloc(outgoing_disps_size, 0, __FILE__, __LINE__);
  int* incoming_disps = (int*) dtcmp_malloc(incoming_disps_size, 0, __FILE__, __LINE__);
  int last_outgoing = 0;
  int last_incoming = 0;
  for (i = 0; i < ranks; i++) {
    outgoing_disps[i] = last_outgoing;
    last_outgoing += outgoing_counts[i];

    incoming_disps[i] = last_incoming;
    last_incoming += incoming_counts[i];
  }
  int total_incoming = last_incoming;

  /* alltoallv to collect items */
  size_t incoming_bytes = total_incoming * keysat_true_extent;
  void* incoming_buf = (void*) dtcmp_malloc(incoming_bytes, 0, __FILE__, __LINE__);
  MPI_Alltoallv(
    buf,          outgoing_counts, outgoing_disps, keysat,
    incoming_buf, incoming_counts, incoming_disps, keysat,
    comm
  );

  /* setup handle and buffer to merge data to return to user */
  void* merge_buf;
  dtcmp_handle_alloc_single(incoming_bytes, &merge_buf, handle);

  /* merge p sorted lists */
  size_t inbufs_size = ranks * sizeof(void*);
  const void** inbufs = (const void**) dtcmp_malloc(inbufs_size, 0, __FILE__, __LINE__);
  for (i = 0; i < ranks; i++) {
    inbufs[i] = (void*) ((char*)incoming_buf + incoming_disps[i] * keysat_extent);
  }
  DTCMP_Merge_local(ranks, inbufs, incoming_counts, merge_buf, key, keysat, cmp, hints);

  /* free memory */
  dtcmp_free(&inbufs);
  dtcmp_free(&incoming_buf);
  dtcmp_free(&incoming_disps);
  dtcmp_free(&outgoing_disps);
  dtcmp_free(&incoming_counts);
  dtcmp_free(&outgoing_counts);
  dtcmp_free(&indicies);
  dtcmp_free(&flags);

  /* set remaining return parameters */
  *outbuf   = merge_buf;
  *outcount = total_incoming;

  return DTCMP_SUCCESS;
}

/* gather all items to each node and sort locally */
int DTCMP_Sortz_samplesort(
  const void* inbuf,
  int incount,
  void** outbuf,
  int* outcount,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm,
  DTCMP_Handle* handle)
{
  /* TODO: as algorithm is currently written, we need to force elements
   * to be unique, otherwise, we need to put some prefix sums in place
   * to determine to which rank each rank should send its data */

  /* algorithm:
   * 1) sort local data
   * 2) pick s samples at regular interval
   * 3) gather all samples to rank 0
   * 4) merge samples into one sorted list
   * 5) pick p-1 splitters at regular interval
   * 6) broadcast splitters
   * 7) identify split points in local data
   * 8) send to final processor with alltoall/alltoallv
   * 9) merge incoming data into final sorted order */

  /* right now, this only supports calls where sum > 0 and min == max */
  uint64_t min, max, sum;
  dtcmp_get_uint64t_min_max_sum(incount, &min, &max, &sum, comm);

  /* nothing to do if the total element count is 0, just set outbuf to
   * NULL, outcount to 0, and return a handle that must still be freed */
  if (sum == 0) {
    dtcmp_handle_alloc_single(0, outbuf, handle);
    *outcount = 0;
    return DTCMP_SUCCESS;
  }

  /* can't handle case where min != max */
  if (min != max) {
    *outbuf = NULL;
    *outcount = 0;
    *handle = DTCMP_HANDLE_NULL;
    return DTCMP_FAILURE;
  }

  /* get my rank and number of ranks in comm */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* TODO: can't handle case where ranks == 1 */
  if (ranks <= 1) {
    *outbuf = NULL;
    *outcount = 0;
    *handle = DTCMP_HANDLE_NULL;
    return DTCMP_FAILURE;
  }

  /* if the input elements are not unique, we'll call a function to
   * to force them to be unique by attaching additional components,
   * this also builds new types and comparison ops */
  void* uniqbuf;
  MPI_Datatype uniqkey, uniqkeysat;
  DTCMP_Op uniqcmp;
  DTCMP_Flags uniqhints;
  DTCMP_Handle uniqhandle;

  /* determine whether input items are unique */
  int input_unique = (hints & DTCMP_FLAG_UNIQUE);
  if (input_unique) {
    /* caller specifies that items are unique, so we can just use
     * the original types and comparison op provided by caller */
    uniqkey    = key;
    uniqkeysat = keysat;
    uniqcmp    = cmp;
    uniqhints  = hints;

    /* get lower bound and extent of keysat */
    MPI_Aint keysat_true_lb, keysat_true_extent;
    MPI_Type_get_true_extent(keysat, &keysat_true_lb, &keysat_true_extent);

    /* copy input data to a temporary buffer */
    size_t uniqbuf_size = incount * keysat_true_extent;
    uniqbuf = (void*) dtcmp_malloc(uniqbuf_size, 0, __FILE__, __LINE__);
    DTCMP_Memcpy(uniqbuf, incount, keysat, inbuf, incount, keysat);
  } else {
    /* tack on rank and original index to each item and return new key
     * and keysat types along with new comparison op */
    dtcmp_uniqify(
      inbuf, incount, key, keysat, cmp, hints,
      &uniqbuf, &uniqkey, &uniqkeysat, &uniqcmp, &uniqhints,
      rank, &uniqhandle
    );
  }

  /* local sort */
  DTCMP_Sort_local(DTCMP_IN_PLACE, uniqbuf, incount, uniqkey, uniqkeysat, uniqcmp, uniqhints);

  /* TODO: compute appropriate s value */
  /* determine the number of samples to take on each process,
   * s = min(incount, 12*ceil(log(incount))) */
  int log_count = 0;
  int size = 1;
  while (size < incount) {
    size <<= 1;
    log_count++;
  }
  int s = 12 * log_count;
  if (s > incount) {
    s = incount;
  }
  if (s == 0 && incount > 0) {
    s = 1;
  }

  /* get lower bound and extent of key */
  MPI_Aint uniqkey_true_lb, uniqkey_true_extent;
  MPI_Type_get_true_extent(uniqkey, &uniqkey_true_lb, &uniqkey_true_extent);

  /* pick ranks-1 splitters */
  int num_splitters = ranks - 1;
  size_t splitters_size = num_splitters * uniqkey_true_extent;
  char* splitters = (char*) dtcmp_malloc(splitters_size, 0, __FILE__, __LINE__);
  find_splitters(uniqbuf, incount, s, splitters, uniqkey, uniqkeysat, uniqcmp, uniqhints, comm);

  /* split local data, exchange, and merge recevied data */
  void* mergebuf;
  int mergecount;
  DTCMP_Handle mergehandle;
  split_exchange_merge(
    uniqbuf, incount, &mergebuf, &mergecount, num_splitters, splitters,
    uniqkey, uniqkeysat, uniqcmp, uniqhints, comm, &mergehandle
  );

  /* free memory */
  dtcmp_free(&splitters);

  if (input_unique) {
    /* in this case, we just sorted directly with the caller's data,
     * but we need to free the copy of the input buffer, and return
     * the merge handle */
    *outbuf   = mergebuf;
    *outcount = mergecount;
    *handle   = mergehandle;

    dtcmp_free(&uniqbuf);
  } else {
    /* we had to make the keys unique, so stip off those extra
     * components here */
    dtcmp_deuniqifyz(
      mergebuf, mergecount, uniqkey, uniqkeysat,
      outbuf, key, keysat,
      &uniqhandle, handle
    );
    *outcount = mergecount;

    /* free temporary handles we created along the way */
    DTCMP_Free(&mergehandle);
  }

  return DTCMP_SUCCESS;
}
