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
#include "lwgrp.h"

#define MIN (0)
#define MAX (1)
#define SUM (2)
#define LEV (3)

/* issue a reduce to root of tree to determine total number of elements
 * each rank will receive from its children to compute total it will send
 * to its parent, then issue a sorting gather to root of tree where at
 * each step parent merges sorted list it currently has with the incoming
 * sorted lists from each of its children, so that entire list is received
 * and sorted as the last step at the root, then scatter the sorted
 * elements back to children passing down same number they passed up */

#define DTCMP_TAG_SORTV_MAX_GATHER   (1)
#define DTCMP_TAG_SORTV_MERGE_TREE   (2)
#define DTCMP_TAG_SORTV_SCATTER_TREE (3)

typedef struct {
  void* buf;
  int count;
  uint64_t* count_list;
  int iter;
  int dist;
  int senditer;
  int block_size;
  int sorter;
  int sort_rank;
  int sort_ranks;
  lwgrp_comm* sort_lwgcomm;
} gather_scatter_state_t;

static int dtcmp_sortv_max_gather(
  int count,
  uint64_t threshold,
  int*      out_max_iter,
  uint64_t* out_count_list[],
  uint64_t* out_count_max,
  int*      out_block_size,
  MPI_Comm comm)
{
  int iter, dist;

  /* determine our rank and the number of ranks in our group */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* determine the potential max number of children we'll have */
  int list_size = 0;
  dist = 1;
  while (dist < ranks) {
    list_size++;
    dist <<= 1;
  }

  /* if list_size is 0, then we just have one rank and no children */
  if (list_size == 0) {
    *out_max_iter   = 0;
    *out_count_list = NULL;
    *out_count_max  = (uint64_t) count;
    *out_block_size = 1;
    return DTCMP_SUCCESS;
  }

  /* otherwise allocate an array to hold the counts from each child */
  uint64_t* count_list = dtcmp_malloc(list_size * sizeof(uint64_t), 0, __FILE__, __LINE__);
  int count_size = 0;

  /* initialize our data for reduction */
  uint64_t reduce[4];
  reduce[MIN] = count; /* minimum count across all ranks */
  reduce[MAX] = count; /* maximum count across all ranks */
  reduce[SUM] = count; /* sum of counts across all ranks */
  reduce[LEV] = 0;     /* min iteration at which some rank has max # elems */

  /* send counts up tree to determine how many elements each
   * child will contribute, remember the iteration in which we send */
  int send_iteration = -1;
  iter = 0;
  dist = 1;
  while (dist < ranks) {
    /* if we have not already sent our data for the reduce operation,
     * check whether we need to send or receive data in this iteration */
    if (send_iteration == -1) {
      /* determine whether we should send or receive in this round */
      int send = rank & dist;
      if (send) {
        /* send reduction data to parent and
         * remember the iteration in which we send */
        int send_rank = rank - dist;
        MPI_Send(reduce, 4, MPI_UINT64_T, send_rank, DTCMP_TAG_SORTV_MAX_GATHER, comm);
        send_iteration = iter;
      } else {
        /* compute the rank we'll receive from in this round */
        int recv_rank = rank + dist;
        if (recv_rank < ranks) {
          /* receive reduction data from this child */
          uint64_t recv_reduce[4];
          MPI_Status recv_status;
          MPI_Recv(recv_reduce, 4, MPI_UINT64_T, recv_rank, DTCMP_TAG_SORTV_MAX_GATHER, comm, &recv_status);

          /* record the total number of elements from each child */
          count_list[count_size] = recv_reduce[SUM];
          count_size++;

          /* compute minimum and maximum counts across all ranks */
          if (recv_reduce[MIN] < reduce[MIN]) {
            reduce[MIN] = recv_reduce[MIN];
          }
          if (recv_reduce[MAX] > reduce[MAX]) {
            reduce[MAX] = recv_reduce[MAX];
          }

          /* compute running sum of all elements from our children */
          reduce[SUM] += recv_reduce[SUM];

          /* check whether any task has reached its threshold, and if
           * so record the lowest iteration in which this happens */
          if (reduce[LEV] == 0) {
            if (recv_reduce[LEV] != 0) {
              /* if we haven't reached the threshold but our child has,
               * then set our level value to our child's */
              reduce[LEV] = recv_reduce[LEV];
            } else if (reduce[SUM] > threshold && threshold > 0) {
              /* otherwise, if we have reached the threshold,
               * record the current iteration plus one, since 0 means NULL */ 
              reduce[LEV] = iter + 1;
            }
          } else {
            /* if we have reached our threshold and our child has,
             * take the minimum value between the two */
            if (recv_reduce[LEV] != 0 && recv_reduce[LEV] < reduce[LEV]) {
              reduce[LEV] = recv_reduce[LEV];
            }
          }
        }
      }
    }

    /* go on to next step */
    dist <<= 1;
    iter++;
  }

  /* broacast totals back down the tree */
  int received = 0;
  if (rank == 0) {
    received = 1;

    /* if we never reached the threshold, set the reduce level to signal
     * that the gather will go all the way up the tree */
    if (reduce[LEV] == 0) {
      reduce[LEV] = iter + 1;
    }
  }
  while (dist > 1) {
    iter--;
    dist >>= 1;

    /* determine whether we should send or receive in this round */
    if (! received) {
      int receive = (iter == send_iteration);
      if (receive) {
        /* receive totals */
        MPI_Status recv_status;
        int recv_rank = rank - dist;
        MPI_Recv(reduce, 4, MPI_UINT64_T, recv_rank, DTCMP_TAG_SORTV_MAX_GATHER, comm, &recv_status);
        received = 1;
      }
    } else {
      /* we've received our data, now we forward it during each step */
      int send_rank = rank + dist;
      if (send_rank < ranks) {
        /* send data and update our offset for the next send */
        MPI_Send(reduce, 4, MPI_UINT64_T, send_rank, DTCMP_TAG_SORTV_MAX_GATHER, comm);
      }
    }
  }

  /* given that we'll gather items up tree for reduce[LEV] steps,
   * total up max number of elements we'll hold after receiving
   * all data from our children */
  iter = 0;
  int max_iter = reduce[LEV]-1;
  uint64_t max_count = (uint64_t) count;
  int block_size = 1;
  while (iter < max_iter) {
    if (iter < count_size) {
      max_count += (uint64_t) count_list[iter];
    }
    block_size <<= 1;
    iter++;
  }

  /* set output parameters */
  *out_max_iter   = max_iter;
  *out_count_list = count_list;
  *out_count_max  = max_count;
  *out_block_size = block_size;

  return DTCMP_SUCCESS;
}

int dtcmp_sortv_merge_tree(
  size_t max_mem,
  const void* inbuf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm,
  gather_scatter_state_t* state)
{
  /* determine our rank and the number of ranks in our group */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* get extent of keysat type */
  MPI_Aint keysat_true_lb, keysat_true_extent;
  MPI_Type_get_true_extent(keysat, &keysat_true_lb, &keysat_true_extent);

  /* given extent of datatype and maximum amount of temporary memory
   * that we can allocate, compute maximum number of elements we can hold */
  uint64_t threshold = max_mem / keysat_true_extent;

  /* determine the number of elements we'll receive in each gather step */
  int max_iter;
  uint64_t* count_list;
  uint64_t count_max;
  int block_size;
  dtcmp_sortv_max_gather(count, threshold, &max_iter, &count_list, &count_max, &block_size, comm);

  void* send_buf = NULL;
  int dist = 1;
  int iter = 0;
  int send_iteration = -1;

  /* NOTE: because we're using dtcmp_malloc/free it's ok to allocate
   * and free buffers of 0 size, other code should handle NULL pointers
   * with a 0 count though */

  /* declare pointers for temporary buffers to receive elements */
  send_buf = dtcmp_malloc(count_max * keysat_true_extent, 0, __FILE__, __LINE__);
  void* merge_buf = dtcmp_malloc(count_max * keysat_true_extent, 0, __FILE__, __LINE__);
  void* recv_buf  = dtcmp_malloc(count_max * keysat_true_extent, 0, __FILE__, __LINE__);

  /* copy our input data to send buffer buffer and sort it */
  DTCMP_Memcpy(send_buf, count, keysat, inbuf, count, keysat);
  DTCMP_Sort_local(DTCMP_IN_PLACE, send_buf, count, key, keysat, cmp, hints);

  /* gather data to sorters (and sort the data as it is gathered) */
  int send_count = count;
  while (send_iteration == -1 && iter < max_iter) {
    /* determine whether we should send or receive in this round */
    int send = rank & dist;
    if (send) {
      int send_rank = rank - dist;
      if (send_count > 0) {
        MPI_Send(send_buf, send_count, keysat, send_rank, DTCMP_TAG_SORTV_MERGE_TREE, comm);
      }
      send_iteration = iter;
    } else {
      /* compute the rank we'll receive from in this round */
      int recv_rank = rank + dist;
      if (recv_rank < ranks) {
        /* determine the number of entries we'll receive from this rank */
        int recv_count = (int)count_list[iter];
        if (recv_count > 0) {
          /* receive the data */
          MPI_Status recv_status;
          MPI_Recv(recv_buf, recv_count, keysat, recv_rank, DTCMP_TAG_SORTV_MERGE_TREE, comm, &recv_status);
  
          /* merge our send and recv buffers into merge buffer */
          const void* inbufs[2];
          inbufs[0] = send_buf;
          inbufs[1] = recv_buf;
          int counts[2];
          counts[0] = send_count;
          counts[1] = recv_count;
          DTCMP_Merge_local(2, inbufs, counts, merge_buf, key, keysat, cmp, hints);

          /* swap our send buffer with our merge buffer */
          void* tmp_buf = send_buf;
          send_buf = merge_buf;
          merge_buf = tmp_buf;

          /* add the number of received elements to our send count */
          send_count += recv_count;
        }
      }
    }

    /* go on to next step */
    dist <<= 1;
    iter++;
  }

  /* free memory */
  dtcmp_free(&recv_buf);
  dtcmp_free(&merge_buf);

  /* determine whether we are one of the ranks to do the sorting */
  int sorter = 0;
  int sort_rank = rank / block_size;
  if (sort_rank * block_size == rank) {
    sorter = 1;
  }

  /* compute the number of sorter ranks */
  int sort_ranks = ranks / block_size;
  if (sort_ranks * block_size < ranks) {
    sort_ranks++;
  }

  lwgrp_comm* sort_lwgcomm = NULL;
  if (sort_ranks > 0) {
    if (sorter) {
      /* build a group of sorters */
      int left = MPI_PROC_NULL;
      if (sort_rank > 0) {
        left = (sort_rank - 1) * block_size;
      }
      int right = MPI_PROC_NULL;
      if (sort_rank < sort_ranks - 1) {
        right = (sort_rank + 1) * block_size;
      }
      lwgrp_chain lwgchain;
      lwgrp_chain_build_from_vals(comm, left, right, sort_ranks, sort_rank, &lwgchain);
      sort_lwgcomm = dtcmp_malloc(sizeof(lwgrp_comm), 0, __FILE__, __LINE__);
      lwgrp_comm_build_from_chain(&lwgchain, sort_lwgcomm);
      lwgrp_chain_free(&lwgchain);
    }
  }

  /* set output parameters */
  state->buf        = send_buf;
  state->count      = (int) count_max;
  state->count_list = count_list;
  state->iter       = iter;
  state->dist       = dist;
  state->senditer   = send_iteration;
  state->block_size = block_size;
  state->sorter     = sorter;
  state->sort_rank  = sort_rank;
  state->sort_ranks = sort_ranks;
  state->sort_lwgcomm = sort_lwgcomm;

  return DTCMP_SUCCESS;
}

int dtcmp_sortv_scatter_tree(
  void* outbuf,
  int outcount,
  MPI_Datatype keysat,
  MPI_Comm comm,
  gather_scatter_state_t* state)
{
  /* determine our rank and the number of ranks in our group */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* get extent of keysat type */
  MPI_Aint keysat_true_lb, keysat_true_extent;
  MPI_Type_get_true_extent(keysat, &keysat_true_lb, &keysat_true_extent);

  /* rename variables pointing to our data to avoid confusion below */
  int final_count = 0; /* running total of number of elements we have already sent */

  void* buf = state->buf;
  int count = state->count;
  int iter  = state->iter;
  int dist  = state->dist;
  int send_iteration   = state->senditer;
  uint64_t* count_list = state->count_list;

  /* NOTE: we avoid sending messages during the gather if counts are 0,
   * however, we still send/recv 0 counts on the scatter in order to
   * synchronization all procs */

  /* scatter sorted data back to ranks */
  int received = 0;
  if (state->sorter) {
    received = 1;
  }
  while (dist > 1) {
    iter--;
    dist >>= 1;

    /* determine whether we should send or receive in this round */
    if (! received) {
      int receive = (iter == send_iteration);
      if (receive) {
        /* receive data */
        int recv_rank = rank - dist;
        MPI_Status recv_status;
        MPI_Recv(buf, count, keysat, recv_rank, DTCMP_TAG_SORTV_SCATTER_TREE, comm, &recv_status);
        received = 1;
      }
    } else {
      /* we've received our data, now we forward it during each step */
      int send_rank = rank + dist;
      if (send_rank < ranks) {
        /* determine the number of elements to send to this rank */
        int send_count = (int)count_list[iter];

        /* determine our offset in the send buffer */
        final_count += send_count;
        size_t final_offset = ((size_t)count - (size_t)final_count) * (size_t)keysat_true_extent;

        /* send data and update our offset for the next send */
        MPI_Send((char*)buf + final_offset, send_count, keysat, send_rank, DTCMP_TAG_SORTV_SCATTER_TREE, comm);
      }
    }
  }

  /* copy result into outbuf */
  int remainder = count - final_count;
  DTCMP_Memcpy(outbuf, remainder, keysat, buf, remainder, keysat);

  /* free our sort group */
  if (state->sort_lwgcomm != NULL) {
    lwgrp_comm_free(state->sort_lwgcomm);
    dtcmp_free(&state->sort_lwgcomm);
  }

  /* free memory */
  dtcmp_free(&state->buf);
  dtcmp_free(&state->count_list);

  return DTCMP_SUCCESS;
}

int DTCMP_Sortv_sortgather_scatter(
  const void* inbuf,
  void* outbuf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  /* determine our rank and the number of ranks in our group */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* don't bother with all of this mess if we only have a single rank */
  if (ranks < 2) {
    return DTCMP_Sort_local(inbuf, outbuf, count, key, keysat, cmp, hints);
  }

  /* get pointer to input data */
  const void* databuf = inbuf;
  if (inbuf == DTCMP_IN_PLACE) {
    databuf = outbuf;
  }

  /* gather and merge data up tree to subset of procs for sorting,
   * we have to remember a lot of values during this step which we
   * need again during the scatter phase, these values are tracked
   * in the gather_scatter_state variable */
  size_t max_mem = 100*1024*1024;
  gather_scatter_state_t state;
  dtcmp_sortv_merge_tree(max_mem, databuf, count, key, keysat, cmp, hints, comm, &state);

  /* parallel sort over subset */
  if (state.sort_ranks > 1 && state.sorter) {
    DTCMP_Sortv_cheng_lwgrp(
      DTCMP_IN_PLACE, state.buf, state.count, key, keysat, cmp, hints,
      state.sort_lwgcomm
    );
  }

  /* scatter data back to all procs */
  dtcmp_sortv_scatter_tree(outbuf, count, keysat, comm, &state);

  return DTCMP_SUCCESS;
}
